# KetteQ In-Memory Calendar Cache

© ketteQ, Inc.

ketteQ In-Memory Calendar Cache is a PostgreSQL server extension that caches calendar information and provides
fast math operations that are done entirely in memory. This cache is shared across the users connected to the 
schema that has the extension enabled.

Specific tables in the schema are required to run the extension.

# Installation

Build the extension using included `cargo` build flow with the release option. After the shared
library is built, install it manually or use the included `cargo pgrx install -r` script to do it automatically
in the local server installation.

This extension uses PostgreSQL shared memory features, and so it must be loaded using the
`shared_preload_libraries` in the `postgresql.conf` file. Then the server must be restarted before
executing the `CREATE EXTENSION` query.

# Compatibility

The PGRX build system allows to target different PostgreSQL version automatically adjusting the output for them.

Supported by this extension: PostgreSQL 17 (default), 16 and 15.

See the Build section to target a different PostgreSQL version, please note that if you use the automatic installer
provided by the PGRX crate it will use the correct target PostgreSQL version.

# Technology

- Rust + PGRX

# Usage

| Function                                                                               | Description                                                               |
|----------------------------------------------------------------------------------------|---------------------------------------------------------------------------|
| kq_invalidate_calendar_cache()                                                         | Invalidates the loaded cache.                                             |
| kq_add_days_by_id(`input date`, `interval int`, `slicetype-id int`)                    | Calculate the next or previous date using the calendar ID.                |
| kq_add_days(`input date`, `interval int`, `slicetype-name text`)                       | Same as the previous function but uses the calendar NAMEs instead of IDs. |

# Usage examples

Invalidating the cache will clear memory and execute again the load queries. After
the function is executed, a fresh cache is available.

```
SELECT kq_invalidate_calendar_cache();

INFO:  Cache Invalidated Successfully
 kq_invalidate_calendar_cache 
------------------------------
 
(1 row)
```

When extension is ready and slices are loaded in memory, calculation functions can
be used.

Add an interval to a date that corresponds to the quarter calendar (Slice Type), the
date must be in a PostgreSQL-supported date format.

```
SELECT kq_add_days('2008-01-15', 1, 'quarter');

 kq_add_days 
-------------
 2008-04-01
(1 row)
```

The output of this function can be used inside a normal SQL query:

```
SELECT 1 id, '2008-01-15' old_date, kq_add_days('2008-01-15', 1, 'quarter') new_date;

 id |  old_date  |  new_date  
----+------------+------------
  1 | 2008-01-15 | 2008-04-01
(1 row)
```

# Testing

Testing can be done using the included `cargo pgrx test -r` command, the command will automatically start a PostgreSQL instance, install the extension and
run the unit test functions included in the source. The output can be useful to detect early bugs.