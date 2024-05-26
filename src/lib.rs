use pgrx::lwlock::PgLwLock;
use pgrx::prelude::*;
use pgrx::shmem::*;
use pgrx::{pg_shmem_init, GucContext, GucFlags, GucRegistry, GucSetting};
use std::ffi::CStr;

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

type GucStrSetting = GucSetting<Option<&'static CStr>>;
type EntriesVec = heapless::Vec<i64, MAX_ENTRIES_PER_CALENDAR>;

type CalendarsVec = heapless::Vec<Calendar, MAX_CALENDARS>;

type CalendarIdIndexMap = heapless::FnvIndexMap<i64, i64, MAX_CALENDARS>;

type CalendarXuidIndexMap = heapless::FnvIndexMap<&'static str, i64, MAX_CALENDARS>;

static Q1_VALIDATION_QUERY: GucStrSetting = GucStrSetting::new(Some(DEF_Q1_VALIDATION_QUERY));

static Q2_GET_CALENDAR_IDS: GucStrSetting = GucStrSetting::new(Some(DEF_Q2_GET_CALENDAR_IDS));

static Q3_GET_CAL_ENTRY_COUNT: GucStrSetting = GucStrSetting::new(Some(DEF_Q3_GET_CAL_ENTRY_COUNT));

static Q4_GET_ENTRIES: GucStrSetting = GucStrSetting::new(Some(DEF_Q4_GET_ENTRIES));

pub struct Calendar {
    calendar_id: i64, // may not be necessary
    dates: EntriesVec,
    page_size: i64,
    first_page_offset: i64,
    page_map: EntriesVec,
}

unsafe impl PGRXSharedMemory for Calendar {}

static CALENDARS: PgLwLock<CalendarsVec> = PgLwLock::new();
static CALENDAR_ID_INDEX_MAP: PgLwLock<CalendarIdIndexMap> = PgLwLock::new();
static CALENDAR_XUID_INDEX_MAP: PgLwLock<CalendarXuidIndexMap> = PgLwLock::new();

#[pg_guard]
pub extern "C" fn _PG_init() {
    pg_shmem_init!(CALENDARS);
    pg_shmem_init!(CALENDAR_ID_INDEX_MAP);
    pg_shmem_init!(CALENDAR_XUID_INDEX_MAP);
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

#[pg_extern]
fn hello_kq_fx_calendar() -> &'static str {
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
