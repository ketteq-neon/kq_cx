mod math;

use pgrx::lwlock::PgLwLock;
use pgrx::prelude::*;
use pgrx::shmem::*;
use pgrx::{pg_shmem_init, GucContext, GucFlags, GucRegistry, GucSetting, PgLwLockShareGuard};
use std::ffi::CStr;
use pgrx::spi::SpiResult;

pgrx::pg_module_magic!();

const MAX_CALENDARS: usize = 128;
const MAX_ENTRIES_PER_CALENDAR: usize = 8 * 1024;

const DEF_Q1_VALIDATION_QUERY: &CStr = cr#"SELECT COUNT(table_name) = 2
FROM information_schema.tables
WHERE table_schema = 'plan'
AND (table_name = 'calendar' OR table_name = 'calendar_date');"#;

const DEF_Q2_GET_CALENDAR_IDS: &CStr = cr#"SELECT MIN(c.id), MAX(c.id) FROM plan.calendar c"#;

const DEF_Q3_GET_CAL_ENTRY_COUNT: &CStr =
    cr#"SELECT cd.calendar_id, COUNT(*),
(SELECT LOWER(ct."name") FROM plan.calendar ct WHERE ct.id = cd.calendar_id) "name"
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

// type CalendarsVec = heapless::Vec<Calendar, MAX_CALENDARS>;

type CalendarIdMap = heapless::FnvIndexMap<i64, Calendar, MAX_CALENDARS>;

type CalendarNameIdMap = heapless::FnvIndexMap<&'static str, i64, MAX_CALENDARS>;

// Queries

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
    calendar_id_count: usize,
    entry_count: usize,
    filled: bool
}

unsafe impl PGRXSharedMemory for CalendarControl {}

// Shared Objects

static CALENDAR_ID_MAP: PgLwLock<CalendarIdMap> = PgLwLock::new();
static CALENDAR_NAME_ID_MAP: PgLwLock<CalendarNameIdMap> = PgLwLock::new();
static CALENDAR_CONTROL: PgLwLock<CalendarControl> = PgLwLock::new();

#[pg_guard]
pub extern "C" fn _PG_init() {
    pg_shmem_init!(CALENDAR_ID_MAP);
    pg_shmem_init!(CALENDAR_NAME_ID_MAP);
    pg_shmem_init!(CALENDAR_CONTROL);
    unsafe {
        init_gucs();
    }
    info!("ketteQ In-Memory Calendar Cache Extension Loaded (kq_imcx)");
}

#[pg_guard]
pub extern "C" fn _PG_fini() {
    info!("Unloaded ketteQ In-Memory Calendar Cache Extension (kq_imcx)");
}

unsafe fn init_gucs() {
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
    let value = String::from_utf8_lossy(guc.get().expect("Cannot get GUC value.").to_bytes())
        .to_string()
        .replace('\n', " ");
    debug1!("Query: {value}");
    value
}

fn ensure_cache_populated() {
    debug1!("ensure_cache_populated()");
    if CALENDAR_CONTROL.share().clone().filled {
        debug1!("Cache already filled. Skipping loading from DB.");
        return;
    }
    validate_compatible_db();
    // Currency Min and Max ID Check
    let currency_min_max: SpiResult<(Option<i64>, Option<i64>)> =
        Spi::get_two(&get_guc_string(&Q2_GET_CALENDAR_IDS));
    let (min_id, max_id): (i64, i64) = match currency_min_max {
        Ok(values) => {
            let min_val = match values.0 {
                None => {
                    error!("Cannot get currency min value or calendar table is empty")
                }
                Some(min_val) => min_val,
            };
            let max_val = match values.1 {
                None => {
                    error!("Cannot get currency min value or calendar table is empty")
                }
                Some(max_val) => max_val,
            };
            (min_val, max_val)
        }
        Err(spi_error) => {
            error!(
                "Cannot execute min/max ID values query or there is no calendars in the table. {}",
                spi_error
            )
        }
    };
    if min_id > max_id {
        error!("Min calendar ID cannot be greater that max calendar ID. Cannot init cache.")
    }
    let calendar_count = max_id - min_id + 1;
    debug1!(
        "Min ID: {}, Max ID: {}, Calendars: {}",
        min_id,
        max_id,
        calendar_count
    );
    // Load calendars (id, name and entry count)
    let mut calendar_count: usize = 0;
    let mut total_entry_count: usize = 0;
    Spi::connect(|client| {
        let mut calendar_id_map = CALENDAR_ID_MAP.exclusive();
        let mut calendar_name_id_map = CALENDAR_NAME_ID_MAP.exclusive();
        let select = client.select(&get_guc_string(&Q3_GET_CAL_ENTRY_COUNT), None, None);
        match select {
            Ok(tuple_table) => {
                for row in tuple_table {
                    let id = row[1].value::<i64>().unwrap().unwrap();
                    let entry_count= row[2].value::<i64>().unwrap().unwrap();
                    let name = row[3].value::<&str>().unwrap().unwrap();

                    calendar_id_map.insert(id, Calendar::default()).unwrap();
                    calendar_name_id_map.insert(name, id).unwrap();

                    total_entry_count += entry_count as usize;
                    calendar_count += 1;
                }
            }
            Err(spi_error) => {
                error!("Cannot get calendars information. {}", spi_error)
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
                let mut current_calendar_entries: Vec<i32> = vec![];

                for row in tuple_table {
                    let calendar_id: i64 = row[1].value().unwrap().unwrap();
                    let calendar_entry: pgrx::Date = row[2].value().unwrap().unwrap();

                    debug1!("Got Entry: {calendar_id} => {calendar_entry} ({})", calendar_entry.to_pg_epoch_days());

                    if prev_calendar_id == -1 {
                        // First loop
                        prev_calendar_id = calendar_id;
                    }

                    // Calendar filled, next calendar
                    if prev_calendar_id != calendar_id {
                        // Update the Calendar
                        if let Some(prev_calendar) = calendar_id_map.get_mut(&prev_calendar_id) {
                            debug1!("loaded {} entries for calendar_id = {}", current_calendar_entries.len(), prev_calendar_id);
                            prev_calendar.dates.extend_from_slice(current_calendar_entries.as_slice()).expect("cannot add entries to calendar");
                            total_entries += prev_calendar.dates.len();
                        } else {
                            error!("cannot add entries: calendar {} not initialized", prev_calendar_id)
                        }
                        prev_calendar_id = calendar_id;
                        current_calendar_entries.clear();
                    }

                    current_calendar_entries.push(calendar_entry.to_pg_epoch_days());
                }

                // End reached, push last calendar entries
                if let Some(prev_calendar) = calendar_id_map.get_mut(&prev_calendar_id) {
                    debug1!("Loaded {} entries for calendar_id = {} - Load complete.", current_calendar_entries.len(), prev_calendar_id);
                    prev_calendar.dates.extend_from_slice(current_calendar_entries.as_slice()).expect("cannot add entries to calendar");
                    total_entries += prev_calendar.dates.len();
                } else {
                    error!("cannot add entries: calendar {} not initialized", prev_calendar_id)
                }
            }
            Err(spi_error) => {
                error!("Cannot load calendar entries. {}", spi_error)
            }
        }
    });
    debug1!("{total_entries} entries loaded intro cache, calculating page map...");
    // Page Size init
    {
        CALENDAR_ID_MAP
            .exclusive()
            .iter_mut()
            .by_ref()
            .for_each(|(calendar_id, calendar)| {
                let first_date = calendar.dates.first().unwrap();
                let last_date = calendar.dates.last().unwrap();
                let entry_count = calendar.dates.len() as i64;

                debug1!("Calculating page size (first_date: {}, last_date: {}, entry_count: {})", first_date, last_date, entry_count);

                let page_size_tmp = math::calculate_page_size(*first_date, *last_date, entry_count);

                if page_size_tmp == 0 {
                    error!("page size cannot be 0, cannot be calculated")
                }
                let first_page_offset = first_date / page_size_tmp;

                let mut prev_page_index = 0;
                let mut page_map: Vec<usize> = vec![0,];

                for calendar_date_index in 0..calendar.dates.len() {
                    let date = calendar.dates.get(calendar_date_index).unwrap();
                    let page_index = (date / page_size_tmp) - first_page_offset;
                    while prev_page_index < page_index {
                        prev_page_index += 1;
                        page_map.insert(prev_page_index as usize, calendar_date_index);
                    }
                }

                debug1!("Page size for calendar {calendar_id} calculated {page_size_tmp}, page_map: {} entries", page_map.len());

                calendar.first_page_offset = first_page_offset;
                calendar.page_size = page_size_tmp;
                calendar.page_map.extend_from_slice(page_map.as_slice()).expect("cannot set page_map for calendar {calendar_id}");
            });
    }



    *CALENDAR_CONTROL.exclusive() = CalendarControl {
        entry_count: total_entry_count,
        calendar_id_count: calendar_count,
        filled: true,
    };
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

fn get_calendar_name_from_id(shared_calendar_id_map: PgLwLockShareGuard<CalendarNameIdMap>, calendar_id: &i64) -> String {
    shared_calendar_id_map
        .iter()
        .find(|&(_, map_calendar_id)| {
            map_calendar_id == calendar_id
        })
        .map(|(m_calendar_name, _)| {
            m_calendar_name.to_string()
        }).unwrap()
}

#[pg_extern]
fn kq_invalidate_calendar_cache() -> &'static str {
    debug1!("Waiting for lock...");
    CALENDAR_NAME_ID_MAP.exclusive().clear();
    debug1!("CALENDAR_NAME_ID_MAP cleared");
    CALENDAR_ID_MAP.exclusive().clear();
    debug1!("CALENDAR_ID_MAP cleared");
    *CALENDAR_CONTROL.exclusive() = CalendarControl::default();
    debug1!("Cache invalidated");
    "Cache invalidated."
}

type CalendarInfo = (i64, String, i64, i32, i64);
fn get_calendars_info() -> Vec<CalendarInfo> {
    // let calendar_name_map = CALENDAR_NAME_ID_MAP.share();
    CALENDAR_ID_MAP
        .share()
        .iter()
        .map(|(calendar_id, calendar)| {
            let calendar_name = get_calendar_name_from_id(CALENDAR_NAME_ID_MAP.share(), calendar_id);
            (*calendar_id, calendar_name, calendar.dates.len() as i64, calendar.page_size, calendar.page_map.len() as i64)
        })
        .collect()
}

#[pg_extern(parallel_safe)]
fn kq_calendar_cache_info() -> TableIterator<
    'static,
    (
        name!(calendar_id, i64),
        name!(calendar_name, String),
        name!(entries, i64),
        name!(page_size, i32),
        name!(page_map_entries, i64),
    ),
> {
    TableIterator::new(get_calendars_info())
}

#[pg_extern(parallel_safe)]
fn kq_calendar_info() -> TableIterator<
    'static,
    (
        name!(property, String),
        name!(value, String),
    ),
> {
    let control = CALENDAR_CONTROL.share().clone();
    let mut data: Vec<(String, String)> = vec![];
    data.push(("PostgreSQL SDK Version".to_string(), pg_sys::PG_VERSION_NUM.to_string()));
    data.push(("PostgreSQL SDK Build".to_string(), std::str::from_utf8(pg_sys::PG_VERSION_STR).unwrap().to_string()));
    data.push(("Extension Version".to_string(), env!("CARGO_PKG_VERSION").to_string()));
    if cfg!(debug_assertions) {
        data.push(("Extension Build".to_string(), "Debug".to_string()));
    } else {
        data.push(("Extension Build".to_string(), "Release".to_string()));
    }
    data.push(("Cache Available".to_string(), control.filled.to_string()));
    data.push(("Slice Cache Size (Calendar ID Count)".to_string(), control.calendar_id_count.to_string()));
    data.push(("Entry Cache Size (Entries)".to_string(), control.entry_count.to_string()));
    data.push(("[Q1] Get Calendar IDs".to_string(), get_guc_string(&Q2_GET_CALENDAR_IDS)));
    data.push(("[Q2] Get Calendar Entry Count per Calendar ID".to_string(), get_guc_string(&Q3_GET_CAL_ENTRY_COUNT)));
    data.push(("[Q3] Get Calendar Entries".to_string(), get_guc_string(&Q4_GET_ENTRIES)));

    get_calendars_info()
        .iter()
        .for_each(|calendar_info| {
            data.push((format!("  Calendar {} ({})", calendar_info.0, calendar_info.1), "".to_string()));
            data.push(("      Entries".to_string(), format!("{}", calendar_info.2)));
            data.push(("      Page Size".to_string(), format!("{}", calendar_info.3)));
            data.push(("      Page Map Entries".to_string(), format!("{}", calendar_info.4)));
        });

    TableIterator::new(data)
}

#[pg_extern(parallel_safe)]
fn kq_calendar_display_cache() -> TableIterator<
    'static,
    (
        name!(calendar, String),
        name!(entry, pgrx::Date),
    ),
> {
    let mut data: Vec<(String, pgrx::Date)> = vec![];
    CALENDAR_ID_MAP
        .share()
        .iter()
        .for_each(|(calendar_id, calendar)| {
            let calendar_name = get_calendar_name_from_id(CALENDAR_NAME_ID_MAP.share(), calendar_id);
            calendar.dates.iter().for_each(
                |date| unsafe {
                    data.push((format!("{} ({})", calendar_id, calendar_name), pgrx::Date::from_pg_epoch_days(*date)));
                }
            );

        });
    TableIterator::new(data)
}

#[pg_extern(parallel_safe)]
fn kq_calendar_display_page_map() -> TableIterator<
    'static,
    (
        name!(calendar, String),
        name!(index, i64),
    ),
> {
    let mut data: Vec<(String, i64)> = vec![];
    CALENDAR_ID_MAP
        .share()
        .iter()
        .for_each(|(calendar_id, calendar)| {
            let calendar_name = get_calendar_name_from_id(CALENDAR_NAME_ID_MAP.share(), calendar_id);
            calendar.page_map.iter().for_each(
                |index| {
                    data.push((format!("{} ({})", calendar_id, calendar_name), *index as i64));
                }
            );

        });
    TableIterator::new(data)
}

fn add_calendar_days(calendar: &Calendar, input_date: i32, interval: i32) -> (i32, usize, usize) {
    if !CALENDAR_CONTROL.share().filled {
        error!("cannot calculate without cache")
    }
    let prev_date_index = math::get_closest_index_from_left(input_date, calendar);

    debug1!("closest index from left: {}", prev_date_index);
    let result_date_index = prev_date_index + interval;
    debug1!("closest result index: {}", result_date_index);

    return if result_date_index >= 0 {
        if prev_date_index < 0 {
            // Handle Negative OOB indices (When interval is negative)
            return (*calendar.dates.get(0).unwrap(), prev_date_index as usize, result_date_index as usize)
        }
        if result_date_index >= calendar.dates.len() as i32 {
            // Handle Positive OOB Indices (When interval is positive)
            // Returns infinity+
            return (i32::MAX, prev_date_index as usize, result_date_index as usize)
        }
        (*calendar.dates.get(result_date_index as usize).unwrap(), prev_date_index as usize, result_date_index as usize)
    } else {
        // First date of calendar
        (*calendar.dates.get(0).unwrap(), prev_date_index as usize, result_date_index as usize)
    }
}

#[pg_extern(parallel_safe)]
fn kq_add_days_by_id(input_date: pgrx::Date, interval: i32, calendar_id: i64) -> Option<pgrx::Date> {
    ensure_cache_populated();
    match CALENDAR_ID_MAP.share().get(&calendar_id) {
        None => {
            return None;
        }
        Some(calendar) => unsafe {
            let result_date = add_calendar_days(calendar, input_date.to_pg_epoch_days(), interval).0;
            debug1!("result from add_calendar_days: {}", result_date);
            let result_date = pgrx::Date::from_pg_epoch_days(result_date);
            Some(result_date)
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
        crate::kq_calendar_cache_info();
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
            "client_min_messages = debug1"
        ]
    }
}
