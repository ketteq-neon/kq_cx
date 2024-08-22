mod math;

use pgrx::lwlock::PgLwLock;
use pgrx::prelude::*;
use pgrx::shmem::*;
use pgrx::spi::SpiResult;
use pgrx::{pg_shmem_init, GucContext, GucFlags, GucRegistry, GucSetting, PgLwLockShareGuard};
use std::ffi::CStr;
use std::time::Duration;

pgrx::pg_module_magic!();

const MAX_CALENDARS: usize = 64;
const MAX_ENTRIES_PER_CALENDAR: usize = 8 * 1024;
const MAX_PAGES_PER_CALENDAR: usize = 512;
const CALENDAR_XUID_MAX_LEN: usize = 32;

const DEF_Q1_VALIDATION_QUERY: &CStr = cr#"
    SELECT
        COUNT(table_name) = 2
    FROM
        information_schema.tables
    WHERE
        table_schema = 'plan' AND
        (table_name = 'calendar' OR table_name = 'calendar_date')
    ;"#;

const DEF_Q2_GET_CALENDAR_IDS: &CStr = cr#"SELECT MIN(c.id), MAX(c.id) FROM plan.calendar c"#;

const DEF_Q3_GET_CAL_ENTRY_COUNT: &CStr =
    cr#"SELECT id, xuid FROM plan.calendar c ORDER BY id ASC;"#;

const DEF_Q4_GET_ENTRIES: &CStr = cr#"
    WITH
        dd AS (
            SELECT
                (date_trunc('year', date) - INTERVAL '10 Years')::date AS min_date,
                (date_trunc('year', date) + INTERVAL '12 Years')::date AS max_date
            FROM plan.data_date
        )
    SELECT
        calendar_id, "date"
    FROM
        plan.calendar_date cd
        CROSS JOIN dd
    WHERE
        cd.date >= dd.min_date AND cd.date < dd.max_date
    ORDER BY
        1, 2
    ;"#;

// Types

type GucStrSetting = GucSetting<Option<&'static CStr>>;
type EntriesVec = heapless::Vec<i32, MAX_ENTRIES_PER_CALENDAR>;
type PageMapVec = heapless::Vec<usize, MAX_PAGES_PER_CALENDAR>;
type CalendarIdMap = heapless::FnvIndexMap<i64, Calendar, MAX_CALENDARS>;
type CalendarXuidIdMap = heapless::FnvIndexMap<CalendarXuid, i64, MAX_CALENDARS>;
type CalendarXuid = heapless::String<CALENDAR_XUID_MAX_LEN>;
type PgDate = pgrx::Date;
type CalendarInfo = (
    i64,    // CalendarID
    String, // Calendar Name
    i64,    // Calendar Entries
    i32,    // Calendar Page Size
    i64,
); // Calendar PageMap Entries

// GUC Queries

static Q1_VALIDATION_QUERY: GucStrSetting = GucStrSetting::new(Some(DEF_Q1_VALIDATION_QUERY));
static Q2_GET_CALENDAR_IDS: GucStrSetting = GucStrSetting::new(Some(DEF_Q2_GET_CALENDAR_IDS));
static Q3_GET_CAL_ENTRY_COUNT: GucStrSetting = GucStrSetting::new(Some(DEF_Q3_GET_CAL_ENTRY_COUNT));
static Q4_GET_ENTRIES: GucStrSetting = GucStrSetting::new(Some(DEF_Q4_GET_ENTRIES));

// Structs

#[derive(Default, Clone, Debug)]
pub struct Calendar {
    dates: EntriesVec,
    page_size: i32,
    first_page_offset: i32,
    page_map: PageMapVec,
}

unsafe impl PGRXSharedMemory for Calendar {}

#[derive(Default, Clone, Debug)]
pub struct CalendarControl {
    calendar_count: usize,
    entry_count: usize,

    cache_filled: bool,
    cache_being_filled: bool,
}

unsafe impl PGRXSharedMemory for CalendarControl {}

// Shared Objects

static CALENDAR_ID_MAP: PgLwLock<CalendarIdMap> = PgLwLock::new();
static CALENDAR_XUID_ID_MAP: PgLwLock<CalendarXuidIdMap> = PgLwLock::new();
static CALENDAR_CONTROL: PgLwLock<CalendarControl> = PgLwLock::new();

#[pg_guard]
pub extern "C" fn _PG_init() {
    pg_shmem_init!(CALENDAR_ID_MAP);
    pg_shmem_init!(CALENDAR_XUID_ID_MAP);
    pg_shmem_init!(CALENDAR_CONTROL);
    init_gucs();

    info!("ketteQ Calendar Extension (kq_cx) Loaded");
}

#[pg_guard]
pub extern "C" fn _PG_fini() {
    info!("ketteQ Calendar Extension (kq_cx) Unloaded");
}

fn init_gucs() {
    GucRegistry::define_string_guc(
        "kq.calendar.q_schema_validation",
        "Query to validate the existence of the required schemas.",
        "",
        &Q1_VALIDATION_QUERY,
        GucContext::Suset,
        GucFlags::empty(),
    );
    GucRegistry::define_string_guc(
        "kq.calendar.q1_get_calendar_min_max_id",
        "Query to select the MIN and MAX calendars.",
        "",
        &Q2_GET_CALENDAR_IDS,
        GucContext::Suset,
        GucFlags::empty(),
    );
    GucRegistry::define_string_guc(
        "kq.calendar.q2_get_calendars_entry_count",
        "Query to select the entry count for each calendar.",
        "",
        &Q3_GET_CAL_ENTRY_COUNT,
        GucContext::Suset,
        GucFlags::empty(),
    );
    GucRegistry::define_string_guc(
        "kq.calendar.q3_get_calendar_entries",
        "Query to actually get the currencies and store it in the shared memory cache.",
        "",
        &Q4_GET_ENTRIES,
        GucContext::Suset,
        GucFlags::empty(),
    );
}

fn get_guc_string(guc: &GucStrSetting) -> String {
    let value = String::from_utf8_lossy(guc.get().expect("cannot get GUC value.").to_bytes())
        .to_string()
        .replace('\n', " ");
    debug2!("Query: {value}");
    value
}

/// The function `ensure_cache_populated` populates the cache with calendar data from the database, ensuring
/// the cache is filled and ready for use.

fn is_cache_filled() -> bool {
    if CALENDAR_CONTROL.share().cache_filled {
        return true;
    }

    if CALENDAR_CONTROL.share().cache_being_filled {
        while CALENDAR_CONTROL.share().cache_being_filled {
            std::thread::sleep(Duration::from_millis(1));
        }
        return true;
    }

    false
}

fn ensure_cache_populated() {
    if is_cache_filled() {
        return;
    }

    validate_compatible_db();

    // Lock CALENDAR_ID_MAP
    let mut calendar_id_map = CALENDAR_ID_MAP.exclusive();

    //someone else might have filled it already
    if is_cache_filled() {
        return;
    }

    CALENDAR_CONTROL.exclusive().cache_being_filled = true;

    let mut calendar_name_id_map = CALENDAR_XUID_ID_MAP.exclusive();
    // Load calendars (id, name and entry count)
    let mut calendar_count: usize = 0;
    Spi::connect(|client| {
        match client.select(&get_guc_string(&Q3_GET_CAL_ENTRY_COUNT), None, None) {
            Ok(tuple_table) => {
                for row in tuple_table {
                    let calendar_id = row[1]
                        .value::<i64>()
                        .unwrap_or_else(|err| error!("server interface error - {err}"))
                        .unwrap_or_else(|| error!("cannot get calendar_id"));

                    let xuid = row[2]
                        .value::<String>()
                        .unwrap_or_else(|err| error!("server interface error - {err}"))
                        .unwrap_or_else(|| error!("cannot get calendar xuid"));

                    let xuid_str: &str = &xuid;
                    let name_string = CalendarXuid::from(xuid_str);

                    // Create a new calendar
                    calendar_id_map
                        .insert(calendar_id, Calendar::default())
                        .unwrap();
                    calendar_name_id_map
                        .insert(name_string, calendar_id)
                        .unwrap();

                    calendar_count += 1;
                }
            }
            Err(spi_error) => {
                error!("cannot get calendars information. {}", spi_error)
            }
        };
    });

    // Fill Cache
    let mut total_entries: usize = 0;
    Spi::connect(|client| {
        let select = client.select(&get_guc_string(&Q4_GET_ENTRIES), None, None);
        match select {
            Ok(tuple_table) => {
                for row in tuple_table {
                    let calendar_id = row[1]
                        .value::<i64>()
                        .unwrap_or_else(|err| error!("server interface error - {err}"))
                        .unwrap_or_else(|| error!("cannot get calendar_id"));
                    let calendar_entry = row[2]
                        .value::<PgDate>()
                        .unwrap_or_else(|err| error!("server interface error - {err}"))
                        .unwrap_or_else(|| error!("cannot get calendar_entry"));

                    debug2!(
                        ">> got entry: {calendar_id} => {calendar_entry} ({})",
                        calendar_entry.to_pg_epoch_days()
                    );

                    if let Some(calendar) = calendar_id_map.get_mut(&calendar_id) {
                        if let Err(_) = calendar.dates.push(calendar_entry.to_pg_epoch_days()) {
                            error!("cannot add more entries to calendar_id = {calendar_id}");
                        }
                        total_entries += 1;
                    } else {
                        error!(
                            "cannot add entries: calendar_id = {} not initialized",
                            calendar_id
                        )
                    }
                }
            }
            Err(spi_error) => {
                error!("Cannot load calendar entries. {}", spi_error)
            }
        }
    });

    debug2!("{total_entries} entries loaded");
    
    // Page Size init
    calendar_id_map
        .iter_mut()
        .by_ref()
        .for_each(|(calendar_id, calendar)| {
            if calendar.dates.is_empty() {
                return;
            }

            let first_date = calendar.dates.first().expect("cannot get first_date");
            let last_date = calendar.dates.last().expect("cannot get last_date");
            let entry_count = calendar.dates.len() as i64;

            let page_size_tmp = math::calculate_page_size(*first_date, *last_date, entry_count);
            if page_size_tmp == 0 {
                error!("page size cannot be 0, cannot be calculated")
            }
            let first_page_offset = first_date / page_size_tmp;

            calendar.first_page_offset = first_page_offset;
            calendar.page_size = page_size_tmp;

            // Create page map
            calendar.page_map.push(0).unwrap();
            let mut prev_page_index = 0;
            for calendar_date_index in 0..calendar.dates.len() {
                let date: &i32 = calendar
                    .dates
                    .get(calendar_date_index)
                    .expect("cannot get date from cache");
                let page_index = (date / page_size_tmp) - first_page_offset;
                while prev_page_index < page_index {
                    prev_page_index += 1;
                    calendar
                        .page_map
                        .insert(prev_page_index as usize, calendar_date_index)
                        .unwrap();
                }
            }

            debug2!("page_map created: calendar_id = {calendar_id}, page_size = {page_size_tmp}");
        });

    *CALENDAR_CONTROL.exclusive() = CalendarControl {
        entry_count: total_entries,
        calendar_count,
        cache_filled: true,
        cache_being_filled: false
    };

    debug2!("cache ready. calendars = {calendar_count}, entries = {total_entries}")
}

/// Checks if the schema is compatible with the extension.
fn validate_compatible_db() {
    let spi_result: SpiResult<Option<bool>> = Spi::get_one(&get_guc_string(&Q1_VALIDATION_QUERY));
    match spi_result {
        Ok(found_tables_opt) => match found_tables_opt {
            None => {
                error!("The current database is not compatible with the ketteQ Calendar Extension.")
            }
            Some(valid) => {
                if !valid {
                    error!("The current database is not compatible with the ketteQ Calendar Extension.")
                }
            }
        },
        Err(spi_error) => {
            error!("Cannot validate current database. {}", spi_error)
        }
    }
}

fn get_calendar_xuid_from_id(
    shared_calendar_xuid_id_map: PgLwLockShareGuard<CalendarXuidIdMap>,
    calendar_id: &i64,
) -> String {
    shared_calendar_xuid_id_map
        .iter()
        .find(|&(_, map_calendar_id)| map_calendar_id == calendar_id)
        .map(|(m_calendar_xuid, _)| m_calendar_xuid.to_string())
        .unwrap()
}

fn get_calendars_info() -> Vec<CalendarInfo> {
    CALENDAR_ID_MAP
        .share()
        .iter()
        .map(|(calendar_id, calendar)| {
            let calendar_xuid =
                get_calendar_xuid_from_id(CALENDAR_XUID_ID_MAP.share(), calendar_id);
            (
                *calendar_id,
                calendar_xuid,
                calendar.dates.len() as i64,
                calendar.page_size,
                calendar.page_map.len() as i64,
            )
        })
        .collect()
}

#[pg_extern(parallel_safe)]
fn kq_cx_cache_info() -> TableIterator<
    'static,
    (
        name!(calendar_id, i64),
        name!(calendar_xuid, String),
        name!(entries, i64),
        name!(page_size, i32),
        name!(page_map_entries, i64),
    ),
> {
    TableIterator::new(get_calendars_info())
}

#[pg_extern(parallel_safe)]
fn kq_cx_info() -> TableIterator<'static, (name!(property, String), name!(value, String))> {
    let control = CALENDAR_CONTROL.share().clone();
    let mut data: Vec<(String, String)> = vec![];
    data.push((
        "PostgreSQL SDK Version".to_string(),
        pg_sys::PG_VERSION_NUM.to_string(),
    ));
    data.push((
        "PostgreSQL SDK Build".to_string(),
        std::str::from_utf8(pg_sys::PG_VERSION_STR)
            .unwrap()
            .to_string(),
    ));
    data.push((
        "Extension Version".to_string(),
        env!("CARGO_PKG_VERSION").to_string(),
    ));
    if cfg!(debug_assertions) {
        data.push(("Build Type".to_string(), "Debug".to_string()));
    } else {
        data.push(("Build Type".to_string(), "Release".to_string()));
    }
    data.push(("Max Calendars".to_string(), format!("{}", MAX_CALENDARS)));
    data.push((
        "Max Entries per calendar".to_string(),
        format!("{}", MAX_ENTRIES_PER_CALENDAR),
    ));
    data.push(("Cache Available".to_string(), control.cache_filled.to_string()));
    data.push((
        "Slice Cache Size (Calendar ID Count)".to_string(),
        control.calendar_count.to_string(),
    ));
    data.push((
        "Entry Cache Size (Entries)".to_string(),
        control.entry_count.to_string(),
    ));
    data.push((
        "[Q1] Get Calendar IDs".to_string(),
        get_guc_string(&Q2_GET_CALENDAR_IDS),
    ));
    data.push((
        "[Q2] Get Calendar Entry Count per Calendar ID".to_string(),
        get_guc_string(&Q3_GET_CAL_ENTRY_COUNT),
    ));
    data.push((
        "[Q3] Get Calendar Entries".to_string(),
        get_guc_string(&Q4_GET_ENTRIES),
    ));
    get_calendars_info().iter().for_each(|calendar_info| {
        data.push((
            format!("Calendar id={} xuid={}", calendar_info.0, calendar_info.1),
            "".to_string(),
        ));
        data.push((
            "    Entry Count".to_string(),
            format!("{}", calendar_info.2),
        ));
        data.push(("    Page Size".to_string(), format!("{}", calendar_info.3)));
        data.push((
            "    Page Map Entry Count".to_string(),
            format!("{}", calendar_info.4),
        ));
    });
    TableIterator::new(data)
}

#[pg_extern(parallel_safe)]
fn kq_cx_display_cache() -> TableIterator<'static, (name!(calendar, String), name!(entry, PgDate))>
{
    let mut data: Vec<(String, PgDate)> = vec![];
    CALENDAR_ID_MAP
        .share()
        .iter()
        .for_each(|(calendar_id, calendar)| {
            let calendar_name =
                get_calendar_xuid_from_id(CALENDAR_XUID_ID_MAP.share(), calendar_id);
            calendar.dates.iter().for_each(|date| {
                data.push((format!("{} ({})", calendar_id, calendar_name), unsafe {
                    PgDate::from_pg_epoch_days(*date)
                }));
            });
        });
    TableIterator::new(data)
}

#[pg_extern(parallel_safe)]
fn kq_cx_display_page_map() -> TableIterator<'static, (name!(calendar, String), name!(index, i64))>
{
    let mut data: Vec<(String, i64)> = vec![];
    CALENDAR_ID_MAP
        .share()
        .iter()
        .for_each(|(calendar_id, calendar)| {
            let calendar_name =
                get_calendar_xuid_from_id(CALENDAR_XUID_ID_MAP.share(), calendar_id);
            calendar.page_map.iter().for_each(|index| {
                data.push((
                    format!("{} ({})", calendar_id, calendar_name),
                    *index as i64,
                ));
            });
        });
    TableIterator::new(data)
}

#[pg_extern(parallel_safe)]
fn kq_cx_invalidate_cache() -> &'static str {
    debug2!("Waiting for lock...");
    let mut calendar_id_map = CALENDAR_ID_MAP.exclusive();

    CALENDAR_XUID_ID_MAP.exclusive().clear();
    *CALENDAR_CONTROL.exclusive() = CalendarControl::default();

    calendar_id_map.clear();
    "Cache invalidated."
}

#[pg_extern(parallel_safe)]
fn kq_cx_add_days(input_date: PgDate, interval: i32, calendar_id: i64) -> Option<PgDate> {
    ensure_cache_populated();
    match CALENDAR_ID_MAP.share().get(&calendar_id) {
        None => {
            warning!("calendar_id = {calendar_id} not found in cache");
            None
        }
        Some(calendar) => {
            let result_date =
                math::add_calendar_days(calendar, input_date.to_pg_epoch_days(), interval);
            let result_date = unsafe { PgDate::from_pg_epoch_days(result_date) };
            Some(result_date)
        }
    }
}

#[pg_extern(parallel_safe)]
unsafe fn kq_cx_add_days_xuid(
    input_date: Date,
    interval: i32,
    calendar_xuid: &str,
) -> Option<PgDate> {
    ensure_cache_populated();
    let calendar_xuid: CalendarXuid = heapless::String::from(calendar_xuid);
    match CALENDAR_XUID_ID_MAP.share().get(&calendar_xuid) {
        None => {
            warning!("calendar_xuid = {calendar_xuid} not found in cache");
            None
        }
        Some(calendar_id) => kq_cx_add_days(input_date, interval, *calendar_id),
    }
}

#[pg_extern(parallel_safe)]
fn kq_cx_populate_cache() -> &'static str {
    ensure_cache_populated();
    "Cache populated."
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    extension_sql_file!("../sql/test_data.sql");

    fn create_date(year: i32, month: u8, day: u8) -> crate::PgDate {
        crate::PgDate::new(year, month, day).expect("Failed to create date")
    }

    #[pg_test]
    fn test_populate_cache() {
        crate::kq_cx_cache_info();
        crate::kq_cx_populate_cache();
        crate::kq_cx_cache_info();
    }

    #[pg_test]
    fn test_add_calendar_days() {
        assert_eq!(
            crate::kq_cx_add_days(create_date(2024, 1, 1), 1, 1),
            Some(create_date(2024, 2, 1))
        )
    }

    // #[pg_test]
    // fn test_conv_pgdate_to_i32() {
    //     assert_eq!(
    //         -10957,
    //         create_date(1970, 1, 1).to_pg_epoch_days()
    //     );
    //     assert_eq!(
    //         72684,
    //         create_date(2199, 1, 1).to_pg_epoch_days()
    //     );
    // }

}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![
            "shared_preload_libraries = 'kq_cx'",
            "log_min_messages = debug2",
            "log_min_error_statement = debug2",
            "client_min_messages = debug2",
        ]
    }
}
