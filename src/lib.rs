use pgrx::lwlock::PgLwLock;
use pgrx::shmem::*;
use pgrx::prelude::*;
use pgrx::pg_shmem_init;

pgrx::pg_module_magic!();


const MAX_CALENDARS: usize = 128;
const MAX_ENTRIES_PER_CALENDAR: usize = 8 * 1024;

pub struct Calendar {
    calendar_id: i64,
    dates: heapless::Vec<i64, MAX_ENTRIES_PER_CALENDAR>
}

unsafe impl PGRXSharedMemory for Calendar {}

static CALENDARS: PgLwLock<heapless::Vec<Calendar, MAX_CALENDARS>> = PgLwLock::new();
static CALENDAR_ID_INDEX_MAP: PgLwLock<heapless::FnvIndexMap<i64, i64, MAX_CALENDARS>> = PgLwLock::new();
static CALENDAR_XUID_INDEX_MAP: PgLwLock<heapless::FnvIndexMap<&'static str, i64, MAX_CALENDARS>> = PgLwLock::new();

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
