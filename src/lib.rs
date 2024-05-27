use pgrx::lwlock::PgLwLock;
use pgrx::prelude::*;
use pgrx::shmem::*;
use pgrx::{pg_shmem_init, GucContext, GucFlags, GucRegistry, GucSetting, PgAtomic};
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
    cr#"SELECT cd.calendar_id, COUNT(*), (SELECT LOWER(ct.\"name\")
FROM plan.calendar ct
WHERE ct.id = cd.calendar_id) \"name\"
FROM plan.calendar_date cd
GROUP BY cd.calendar_id
ORDER BY cd.calendar_id ASC;"#;

const DEF_Q4_GET_ENTRIES: &CStr = cr#"SELECT cd.calendar_id, cd.\"date\"
FROM plan.calendar_date cd
ORDER BY cd.calendar_id asc, cd.\"date\" ASC;"#;

// Types

type GucStrSetting = GucSetting<Option<&'static CStr>>;
type EntriesVec = heapless::Vec<i64, MAX_ENTRIES_PER_CALENDAR>;

type CalendarsVec = heapless::Vec<Calendar, MAX_CALENDARS>;

type CalendarIdIndexMap = heapless::FnvIndexMap<i64, i64, MAX_CALENDARS>;

type CalendarXuidIndexMap = heapless::FnvIndexMap<&'static str, i64, MAX_CALENDARS>;

// Queries

static Q1_VALIDATION_QUERY: GucStrSetting = GucStrSetting::new(Some(DEF_Q1_VALIDATION_QUERY));

static Q2_GET_CALENDAR_IDS: GucStrSetting = GucStrSetting::new(Some(DEF_Q2_GET_CALENDAR_IDS));

static Q3_GET_CAL_ENTRY_COUNT: GucStrSetting = GucStrSetting::new(Some(DEF_Q3_GET_CAL_ENTRY_COUNT));

static Q4_GET_ENTRIES: GucStrSetting = GucStrSetting::new(Some(DEF_Q4_GET_ENTRIES));

// Structs

pub struct Calendar {
    calendar_id: i64, // may not be necessary
    dates: EntriesVec,
    page_size: i64,
    first_page_offset: i64,
    page_map: EntriesVec,
}

unsafe impl PGRXSharedMemory for Calendar {}

// Shared Objects

static CALENDARS: PgLwLock<CalendarsVec> = PgLwLock::new();
static CALENDAR_ID_INDEX_MAP: PgLwLock<CalendarIdIndexMap> = PgLwLock::new();
static CALENDAR_XUID_INDEX_MAP: PgLwLock<CalendarXuidIndexMap> = PgLwLock::new();
static CONTROL_CACHE_FILLED: PgLwLock<bool> = PgLwLock::new();

#[pg_guard]
pub extern "C" fn _PG_init() {
    pg_shmem_init!(CALENDARS);
    pg_shmem_init!(CALENDAR_ID_INDEX_MAP);
    pg_shmem_init!(CALENDAR_XUID_INDEX_MAP);
    pg_shmem_init!(CONTROL_CACHE_FILLED);
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
        "kq.calendar.q2_get_calendars_entry_count",
        "Query to select the entry count for each calendar.",
        "",
        &Q2_GET_CALENDAR_IDS,
        GucContext::Suset,
        GucFlags::empty(),
    );
    GucRegistry::define_string_guc(
        "kq.calendar.q3_get_calendar_entries",
        "Query to select all calendar entries. This will be copied to the cache.",
        "",
        &Q3_GET_CAL_ENTRY_COUNT,
        GucContext::Suset,
        GucFlags::empty(),
    );
    GucRegistry::define_string_guc(
        "kq.currency.q4_get_currency_entries",
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
    if CONTROL_CACHE_FILLED.share().clone() {
        debug1!("Cache already filled. Skipping loading from DB.");
        return;
    }
    validate_compatible_db();
    // Currency Min and Max ID
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
        "Min ID: {}, Max ID: {}, Currencies: {}",
        min_id,
        max_id,
        calendar_count
    );
}

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

#[pg_extern]
fn kq_invalidate_calendar_cache() -> &'static str {
    debug1!("Waiting for lock...");
    CALENDAR_ID_INDEX_MAP.exclusive().clear();
    debug1!("CALENDAR_ID_INDEX_MAP cleared");
    CALENDAR_XUID_INDEX_MAP.exclusive().clear();
    debug1!("CALENDAR_XUID_INDEX_MAP cleared");
    CALENDARS.exclusive().clear();
    debug1!("CALENDARS cleared");
    *CONTROL_CACHE_FILLED.exclusive() = false;
    debug1!("Cache invalidated");
    "Cache invalidated."
}

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
        vec!["shared_preload_libraries = 'kq_imcx'"]
    }
}
