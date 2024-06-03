mod math;

use pgrx::lwlock::PgLwLock;
use pgrx::prelude::*;
use pgrx::shmem::*;
use pgrx::{pg_shmem_init, GucContext, GucFlags, GucRegistry, GucSetting};
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
    calendar_id: i64, // may not be necessary
    dates: EntriesVec,
    page_size: i32,
    first_page_offset: i32,
    page_map: PageMapVec,
}

unsafe impl PGRXSharedMemory for Calendar {}

#[derive(Default, Clone, Debug)]
pub struct CalendarControl {
    calendar_id_count: i64,
    entry_count: i64,
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
    let mut calendar_count: i64 = 0;
    let mut total_entry_count: i64 = 0;
    Spi::connect(|client| {
        let mut calendar_id_map = CALENDAR_ID_MAP.exclusive();
        let mut calendar_name_id_map = CALENDAR_NAME_ID_MAP.exclusive();
        let select = client.select(&get_guc_string(&Q3_GET_CAL_ENTRY_COUNT), None, None);
        match select {
            Ok(tuple_table) => {
                for row in tuple_table {
                    let id: i64 = row[1].value().unwrap().unwrap();
                    let entry_count: i64 = row[2].value().unwrap().unwrap();
                    let name: &str = row[3].value().unwrap().unwrap();

                    let new_calendar = Calendar {
                        calendar_id: id,
                        ..Calendar::default()
                    };

                    calendar_id_map.insert(id, new_calendar).unwrap();
                    calendar_name_id_map.insert(name, id).unwrap();

                    calendar_count += 1;
                    total_entry_count += entry_count;
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
                let mut current_calendar_entries: Vec<i32> = vec!();

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
                        if let Some(mut prev_calendar) = calendar_id_map.get_mut(&prev_calendar_id) {
                            debug1!("loaded {} entries for calendar_id = {}", current_calendar_entries.len(), prev_calendar_id);
                            prev_calendar.dates.extend_from_slice(&*current_calendar_entries).expect("cannot add entries to calendar");
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
                if let Some(mut prev_calendar) = calendar_id_map.get_mut(&prev_calendar_id) {
                    debug1!("Loaded {} entries for calendar_id = {} - Load complete.", current_calendar_entries.len(), prev_calendar_id);
                    prev_calendar.dates.extend_from_slice(&*current_calendar_entries).expect("cannot add entries to calendar");
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
        let calendar_id_map = CALENDAR_ID_MAP.exclusive();
        calendar_id_map
            .iter()
            .for_each(|(calendar_id, mut calendar)| {
                let first_date = *calendar.dates.first().unwrap();
                let last_date = *calendar.dates.last().unwrap();
                let entry_count = calendar.dates.len() as i64;
                let page_size_tmp = math::calculate_page_size(first_date, last_date, entry_count);
                if page_size_tmp == 0 {
                    error!("page size cannot be 0, cannot be calculated")
                }
                let first_page_offset = first_date / page_size_tmp;

                let mut prev_page_index = 0;
                let mut page_map: Vec<usize> = vec!(0,);

                for calendar_date_index in 0..calendar.dates.len() {
                    let date = calendar.dates.get(calendar_date_index).unwrap();
                    let page_index = (date / page_size_tmp) - first_page_offset;
                    while prev_page_index < page_index {
                        prev_page_index += 1;
                        page_map.insert(prev_page_index as usize, calendar_date_index);
                    }
                }

                let mut new_calendar: Calendar = Calendar {
                    page_size: page_size_tmp,
                    first_page_offset,
                    ..calendar.clone()
                };

                new_calendar.page_map.extend_from_slice(&*page_map).expect(&format!("cannot set page_map for calendar {calendar_id}"));

                debug1!("Page size for calendar {calendar_id} calculated {page_size_tmp} and map created");

                calendar = &new_calendar
            })
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

fn get_calendar_name_from_id(calendar_id: i64) -> String {
    CALENDAR_NAME_ID_MAP.share()
        .iter()
        .find(|&(_, map_calendar_id)| {
            map_calendar_id == &calendar_id
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
    CALENDAR_ID_MAP.exclusive().clear();
    debug1!("CALENDARS cleared");
    *CALENDAR_CONTROL.exclusive() = CalendarControl::default();
    debug1!("Cache invalidated");
    "Cache invalidated."
}

#[pg_extern]
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
    ensure_cache_populated();
    let calendar_name_map = CALENDAR_NAME_ID_MAP.share().clone();
    let result_vec: Vec<(_, _, _, _, _)> = CALENDAR_ID_MAP
        .share()
        .iter()
        .map(|(calendar_id, calendar)| {
            let calendar_name = calendar_name_map
                .iter()
                .find(|&(_, map_calendar_id)| {
                    map_calendar_id == calendar_id
                })
                .map(|(m_calendar_name, _)| {
                    m_calendar_name.to_string()
                }).unwrap();
            (*calendar_id, calendar_name, calendar.dates.len() as i64, calendar.page_size, calendar.page_map.len() as i64)
        })
        .collect();
    TableIterator::new(result_vec)
}

#[pg_extern]
fn kq_calendar_info() -> TableIterator<
    'static,
    (
        name!(property, &'static str),
        name!(value, String),
    ),
> {
    ensure_cache_populated();
    let control = CALENDAR_CONTROL.share().clone();
    let mut data: Vec<(&str, String)> = vec!();
    data.push(("PostgreSQL SDK Version", pg_sys::PG_VERSION_NUM.to_string()));
    data.push(("PostgreSQL SDK Build", std::str::from_utf8(pg_sys::PG_VERSION_STR).unwrap().to_string()));
    data.push(("Extension Version", env!("CARGO_PKG_VERSION").to_string()));
    if cfg!(debug_assertions) {
        data.push(("Extension Build", "Debug".to_string()));
    } else {
        data.push(("Extension Build", "Release".to_string()));
    }
    data.push(("Cache Available", control.filled.to_string()));
    data.push(("Slice Cache Size (Calendar ID Count)", control.calendar_id_count.to_string()));
    data.push(("Entry Cache Size (Entries)", control.entry_count.to_string()));
    data.push(("[Q1] Get Calendar IDs", get_guc_string(&Q2_GET_CALENDAR_IDS)));
    data.push(("[Q2] Get Calendar Entry Count per Calendar ID", get_guc_string(&Q3_GET_CAL_ENTRY_COUNT)));
    data.push(("[Q3] Get Calendar Entries", get_guc_string(&Q4_GET_ENTRIES)));



    // let result_vec: Vec<(_, _)> = CALENDAR_ID_MAP
    //     .share()
    //     .iter()
    //     .map(|(calendar_id, calendar)| {
    //         debug1!("displaying: calendar_id = {}, entries = {}", calendar_id, calendar.dates.len());
    //         (*calendar_id, calendar.dates.len() as i64)
    //     })
    //     .collect();
    TableIterator::new(data)
}

// #[pg_extern]
// fn kq_calendar_display_cache() {
//
// }

#[pg_extern]
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
