mod math;

use pgrx::lwlock::PgLwLock;
use pgrx::prelude::*;
use pgrx::shmem::*;
use pgrx::spi::SpiResult;
use pgrx::{pg_shmem_init, GucContext, GucFlags, GucRegistry, GucSetting, PgLwLockShareGuard};
use std::ffi::CStr;
use std::mem;

pgrx::pg_module_magic!();

const MAX_CALENDARS: usize = 128;
const MAX_ENTRIES_PER_CALENDAR: usize = 512 * 1024;
const CALENDAR_XUID_MAX_LEN: usize = 128;

const DEF_Q1_VALIDATION_QUERY: &CStr = cr#"SELECT COUNT(table_name) = 2
FROM information_schema.tables
WHERE table_schema = 'plan'
AND (table_name = 'calendar' OR table_name = 'calendar_date');"#;

const DEF_Q2_GET_CALENDAR_IDS: &CStr = cr#"SELECT MIN(c.id), MAX(c.id) FROM plan.calendar c"#;

const DEF_Q3_GET_CAL_ENTRY_COUNT: &CStr = cr#"SELECT cd.calendar_id, COUNT(*),
(SELECT LOWER(ct.xuid) FROM plan.calendar ct WHERE ct.id = cd.calendar_id) xuid
FROM plan.calendar_date cd
GROUP BY cd.calendar_id
ORDER BY cd.calendar_id ASC;"#;

const DEF_Q4_GET_ENTRIES: &CStr = cr#"SELECT cd.calendar_id, cd."date"
FROM plan.calendar_date cd
ORDER BY cd.calendar_id asc, cd."date" ASC;"#;

// Types

type GucStrSetting = GucSetting<Option<&'static CStr>>;
type EntriesVec = heapless::Vec<i32, MAX_ENTRIES_PER_CALENDAR>;
type PageMapVec = heapless::Vec<usize, MAX_ENTRIES_PER_CALENDAR>;
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
    filled: bool,
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
    info!("ketteQ In-Memory Calendar Cache Extension Loaded (kq_imcx)");
}

#[pg_guard]
pub extern "C" fn _PG_fini() {
    info!("Unloaded ketteQ In-Memory Calendar Cache Extension (kq_imcx)");
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
    // debug1!("Query: {value}");
    value
}

/// The function `ensure_cache_populated` populates the cache with calendar data from the database, ensuring
/// the cache is filled and ready for use.
fn ensure_cache_populated() {
    // debug1!("ensure_cache_populated()");
    if CALENDAR_CONTROL.share().clone().filled {
        // debug1!("Cache already filled. Skipping loading from DB.");
        return;
    }
    validate_compatible_db();
    // Load calendars (id, name and entry count)
    let mut calendar_count: usize = 0;
    let mut total_entry_count: usize = 0;
    Spi::connect(|client| {
        let mut calendar_id_map = CALENDAR_ID_MAP.exclusive();
        let mut calendar_name_id_map = CALENDAR_XUID_ID_MAP.exclusive();
        let select = client.select(&get_guc_string(&Q3_GET_CAL_ENTRY_COUNT), None, None);
        match select {
            Ok(tuple_table) => {
                for row in tuple_table {
                    let id = row[1]
                        .value::<i64>()
                        .unwrap()
                        .expect("calendar_id cannot be null");
                    let entry_count = row[2]
                        .value::<i64>()
                        .unwrap()
                        .expect("entry_count cannot be null");
                    let xuid = row[3]
                        .value::<&'static str>()
                        .unwrap()
                        .expect("calendar_xuid cannot be null");

                    // // Check entry count
                    if MAX_ENTRIES_PER_CALENDAR < entry_count as usize {
                        error!("cannot cache the calendar_id = {} (xuid = {}), it has too many entries ({}) for the current configuration. max_entries = {}",
                            id,
                            xuid,
                            entry_count,
                            MAX_ENTRIES_PER_CALENDAR);
                    }

                    let name_string: CalendarXuid = heapless::String::from(xuid);

                    // let mut new_calendar = Calendar::default();
                    // new_calendar.dates.resize_default(entry_count as usize).expect("cannot resize dates vector");

                    // Create a new calendar
                    calendar_id_map.insert(id, Calendar::default()).unwrap();
                    calendar_name_id_map.insert(name_string, id).unwrap();

                    total_entry_count += entry_count as usize;
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
        let mut calendar_id_map = CALENDAR_ID_MAP.exclusive();
        let select = client.select(&get_guc_string(&Q4_GET_ENTRIES), None, None);
        match select {
            Ok(tuple_table) => {
                let mut prev_calendar_id = -1;
                let mut calendar_entries_vec: Vec<i32> = vec![];

                for row in tuple_table {
                    let calendar_id = row[1]
                        .value::<i64>()
                        .unwrap()
                        .expect("calendar_id cannot be null");
                    let calendar_entry = row[2]
                        .value::<PgDate>()
                        .unwrap()
                        .expect("calendar_entry cannot be null");

                    debug1!(
                        ">> got entry: {calendar_id} => {calendar_entry} ({})",
                        calendar_entry.to_pg_epoch_days()
                    );

                    if prev_calendar_id == -1 {
                        // First loop
                        prev_calendar_id = calendar_id;
                    }

                    // Calendar filled, next calendar
                    if prev_calendar_id != calendar_id {
                        // Update the Calendar
                        if let Some(prev_calendar) = calendar_id_map.get_mut(&prev_calendar_id) {
                            if Err(()) == prev_calendar
                                .dates
                                .extend_from_slice(calendar_entries_vec.as_slice()) {
                                error!("cannot add entries to calendar_id = {prev_calendar_id}");
                            };
                            total_entries += prev_calendar.dates.len();
                            debug1!(
                                ">> loaded {} entries into calendar_id = {}, entries cached = {total_entries}/{total_entry_count}",
                                calendar_entries_vec.len(),
                                prev_calendar_id
                            );
                        } else {
                            error!(
                                "cannot add entries: calendar_id = {} not initialized",
                                prev_calendar_id
                            )
                        }
                        prev_calendar_id = calendar_id;
                        calendar_entries_vec.clear();
                    }

                    calendar_entries_vec.push(calendar_entry.to_pg_epoch_days());
                }

                // End reached, push last calendar entries
                if let Some(prev_calendar) = calendar_id_map.get_mut(&prev_calendar_id) {
                    if Err(()) == prev_calendar
                        .dates
                        .extend_from_slice(calendar_entries_vec.as_slice()) {
                        error!("cannot add entries to calendar_id = {prev_calendar_id}");
                    };
                    total_entries += prev_calendar.dates.len();
                    debug1!(
                        ">> loaded {} entries into calendar_id = {}, entries cached = {total_entries}/{total_entry_count} >> load complete",
                        calendar_entries_vec.len(),
                        prev_calendar_id
                    );
                } else {
                    error!(
                        "cannot add entries: calendar_id = {} not initialized",
                        prev_calendar_id
                    )
                }
            }
            Err(spi_error) => {
                error!("Cannot load calendar entries. {}", spi_error)
            }
        }
    });
    if total_entries != total_entry_count {
        error!("cannot load all counted entries, {total_entries} loaded of {total_entry_count} counted")
    }
    debug1!("{total_entries} entries loaded");
    // Page Size init
    {
        CALENDAR_ID_MAP
            .exclusive()
            .iter_mut()
            .by_ref()
            .for_each(|(calendar_id, calendar)| {
                let first_date = calendar.dates.first().expect("cannot get first_date");
                let last_date = calendar.dates.last().expect("cannot get last_date");
                let entry_count = calendar.dates.len() as i64;

                // debug1!("calculating page size (first_date: {}, last_date: {}, entry_count: {})", first_date, last_date, entry_count);

                let page_size_tmp = math::calculate_page_size(*first_date, *last_date, entry_count);

                if page_size_tmp == 0 {
                    error!("page size cannot be 0, cannot be calculated")
                }
                let first_page_offset = first_date / page_size_tmp;

                let mut prev_page_index = 0;
                let mut page_map: Vec<usize> = vec![0];

                // Create page map
                for calendar_date_index in 0..calendar.dates.len() {
                    let date = calendar.dates.get(calendar_date_index).expect("cannot get date from cache");
                    let page_index = (date / page_size_tmp) - first_page_offset;
                    while prev_page_index < page_index {
                        prev_page_index += 1;
                        page_map.insert(prev_page_index as usize, calendar_date_index);
                    }
                }

                debug1!("page_map created: calendar_id = {calendar_id}, page_size = {page_size_tmp}, page_map.len() = {}", page_map.len());

                calendar.first_page_offset = first_page_offset;
                calendar.page_size = page_size_tmp;
                // calendar.page_map.resize_default(page_map.len()).expect("cannot resize page map");
                calendar.page_map.extend_from_slice(page_map.as_slice()).expect("cannot set page_map for calendar");
            });
    }

    *CALENDAR_CONTROL.exclusive() = CalendarControl {
        entry_count: total_entry_count,
        calendar_count,
        filled: true,
    };

    debug1!("cache ready. calendars = {calendar_count}, entries = {total_entry_count}")
}

/// Checks if the schema is compatible with the extension.
fn validate_compatible_db() {
    let spi_result: SpiResult<Option<bool>> = Spi::get_one(&get_guc_string(&Q1_VALIDATION_QUERY));
    match spi_result {
        Ok(found_tables_opt) => match found_tables_opt {
            None => {
                error!("The current database is not compatible with the ketteQ In-Memory Calendar Extension.")
            }
            Some(valid) => {
                if !valid {
                    error!("The current database is not compatible with the ketteQ In-Memory Calendar Extension.")
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
    data.push(("Max Entries per calendar".to_string(), format!("{}", MAX_ENTRIES_PER_CALENDAR)));
    let mut current_memory_use = control.entry_count * control.calendar_count * mem::size_of::<i32>();
    // current_memory_use += control.calendar_count * mem::size_of::<i64>();
    CALENDAR_XUID_ID_MAP
        .share()
        .clone()
        .iter()
        .for_each(|(calendar_xuid, _)| {
            current_memory_use += calendar_xuid.len();
            current_memory_use += mem::size_of::<i64>();
        });
    CALENDAR_ID_MAP
        .share()
        .clone()
        .iter()
        .for_each(|(_, calendar)| {
            current_memory_use += calendar.page_map.len() * mem::size_of::<usize>();
            current_memory_use += mem::size_of::<i64>();
        });
    data.push(("Current Memory Usage".to_string(), format!("{} bytes", current_memory_use)));
    // let mut max_memory_usage = MAX_CALENDARS * MAX_ENTRIES_PER_CALENDAR * mem::size_of::<i32>();
    // max_memory_usage += MAX_CALENDARS * mem::size_of::<i64>();
    // max_memory_usage += MAX_CALENDARS * CALENDAR_XUID_MAX_LEN;
    // data.push(("Max Memory".to_string(), format!("{} bytes", max_memory_usage)));
    data.push(("Cache Available".to_string(), control.filled.to_string()));
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
            format!("  Calendar {} ({})", calendar_info.0, calendar_info.1),
            "".to_string(),
        ));
        data.push(("      Entries".to_string(), format!("{}", calendar_info.2)));
        data.push((
            "      Page Size".to_string(),
            format!("{}", calendar_info.3),
        ));
        data.push((
            "      Page Map Entries".to_string(),
            format!("{}", calendar_info.4),
        ));
    });

    TableIterator::new(data)
}

#[pg_extern(parallel_safe)]
unsafe fn kq_cx_display_cache() -> TableIterator<'static, (name!(calendar, String), name!(entry, PgDate))> {
    let mut data: Vec<(String, PgDate)> = vec![];
    CALENDAR_ID_MAP
        .share()
        .iter()
        .for_each(|(calendar_id, calendar)| {
            let calendar_name =
                get_calendar_xuid_from_id(CALENDAR_XUID_ID_MAP.share(), calendar_id);
            calendar.dates.iter().for_each(|date| {
                data.push((
                    format!("{} ({})", calendar_id, calendar_name),
                    PgDate::from_pg_epoch_days(*date),
                ));
            });
        });
    TableIterator::new(data)
}

#[pg_extern(parallel_safe)]
fn kq_cx_display_page_map() -> TableIterator<'static, (name!(calendar, String), name!(index, i64))> {
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
    debug1!("Waiting for lock...");
    CALENDAR_XUID_ID_MAP.exclusive().clear();
    debug1!("CALENDAR_XUID_ID_MAP cleared");
    CALENDAR_ID_MAP.exclusive().clear();
    debug1!("CALENDAR_ID_MAP cleared");
    *CALENDAR_CONTROL.exclusive() = CalendarControl::default();
    debug1!("Cache invalidated");
    "Cache invalidated."
}

#[pg_extern(parallel_safe)]
unsafe fn kq_cx_add_days_by_id(input_date: PgDate, interval: i32, calendar_id: i64) -> Option<PgDate> {
    ensure_cache_populated();
    match CALENDAR_ID_MAP.share().get(&calendar_id) {
        None => {
            warning!("calendar_id = {calendar_id} not found in cache");
            None
        }
        Some(calendar) => {
            let result_date =
                math::add_calendar_days(calendar, input_date.to_pg_epoch_days(), interval).0;
            // debug1!("result from add_calendar_days: {}", result_date);
            let result_date = PgDate::from_pg_epoch_days(result_date);
            Some(result_date)
        }
    }
}

#[pg_extern(parallel_safe)]
unsafe fn kq_cx_add_days(input_date: Date, interval: i32, calendar_xuid: &str) -> Option<PgDate> {
    ensure_cache_populated();
    let calendar_xuid: CalendarXuid = heapless::String::from(calendar_xuid);
    match CALENDAR_XUID_ID_MAP.share().get(&calendar_xuid) {
        None => {
            warning!("calendar_xuid = {calendar_xuid} not found in cache");
            None
        }
        Some(calendar_id) => {
            let calendar = CALENDAR_ID_MAP
                .share()
                .get(calendar_id)
                .expect("calendar is missing")
                .clone();
            let result_date =
                math::add_calendar_days(&calendar, input_date.to_pg_epoch_days(), interval).0;
            Some(PgDate::from_pg_epoch_days(result_date))
        }
    }
}

#[pg_extern(parallel_safe)]
fn hello_kq_fx_calendar() -> &'static str {
    ensure_cache_populated();
    "Hello, kq_imcx"
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    extension_sql_file!("../sql/test_data.sql");

    #[pg_test]
    fn test_hello_kq_fx_calendar() {
        crate::kq_cx_cache_info();
        assert_eq!("Hello, kq_imcx", crate::hello_kq_fx_calendar());
    }
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
            "shared_preload_libraries = 'kq_imcx'",
            "log_min_messages = debug1",
            "log_min_error_statement = debug1",
            "client_min_messages = debug1",
        ]
    }
}
