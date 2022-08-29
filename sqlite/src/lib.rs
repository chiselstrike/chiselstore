/*
use libsqlite3_sys::{
    sqlite3_changes, sqlite3_clear_bindings, sqlite3_column_blob,
    sqlite3_column_bytes, sqlite3_column_count, sqlite3_column_database_name,
    sqlite3_column_decltype, sqlite3_column_double, sqlite3_column_int, sqlite3_column_int64,
    sqlite3_column_name, sqlite3_column_origin_name, sqlite3_column_table_name,
    sqlite3_column_type, sqlite3_column_value, sqlite3_db_handle, sqlite3_finalize, sqlite3_reset,
    sqlite3_sql, sqlite3_stmt_readonly, sqlite3_table_column_metadata,
    sqlite3_unlock_notify, sqlite3_value,
};
*/
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use chiselstore::{Config, Database};
use std::sync::Arc;

pub type sqlite3_int64 = libc::c_longlong;

pub type sqlite3_uint64 = libc::c_ulonglong;

pub const SQLITE_OK: libc::c_int = 0;
pub const SQLITE_ERROR: libc::c_int = 1;
pub const SQLITE_LOCKED: libc::c_int = 6;
pub const SQLITE_MISUSE: libc::c_int = 21;
pub const SQLITE_ROW: libc::c_int = 100;
pub const SQLITE_DONE: libc::c_int = 101;
pub const SQLITE_LOCKED_SHAREDCACHE: libc::c_int = SQLITE_LOCKED | (1 << 8);

pub const SQLITE_TRANSIENT: libc::c_int = -1;

pub const SQLITE_UTF8: libc::c_int = 1;

pub struct sqlite3 {
    _database: Database,
}

impl Drop for sqlite3 {
    fn drop(&mut self) {
        println!("Dropping sqlite3");
    }
}

pub struct sqlite3_stmt {}

/// Node authority (host and port) in the cluster.
fn node_authority(id: usize) -> (&'static str, u16) {
    let host = "127.0.0.1";
    let port = 50000 + (id as u16);
    (host, port)
}

#[no_mangle]
pub extern "C" fn ext_sqlite3_open(
    _filename: *const libc::c_char,
    db: *mut *mut sqlite3,
) -> libc::c_int {
    let config = Config {
        id: 1,
        peers: vec![],
        node_addr: Arc::new(node_authority),
    };
    let database = Database::new(config);
    unsafe {
        let ptr = Box::new(sqlite3 {
            _database: database,
        });
        *db = Box::into_raw(ptr);
    }
    SQLITE_OK
}

#[no_mangle]
pub extern "C" fn ext_sqlite3_close(db: *mut sqlite3) -> libc::c_int {
    let _ = unsafe { Box::from_raw(db) };
    SQLITE_OK
}

#[no_mangle]
pub extern "C" fn ext_sqlite3_prepare(
    _db: *mut sqlite3,
    _sql: *const libc::c_char,
    _sql_len: libc::c_int,
    _stmt: *mut *mut sqlite3_stmt,
    _remaining_sql: *mut *const libc::c_char,
) -> libc::c_int {
    println!("sqlite3_prepare");
    SQLITE_ERROR
}

#[no_mangle]
pub extern "C" fn ext_sqlite3_finalize(_stmt: *mut sqlite3_stmt) -> libc::c_int {
    println!("sqlite3_finalize");
    SQLITE_ERROR
}

#[no_mangle]
pub extern "C" fn ext_sqlite3_bind_blob(
    _stmt: *mut sqlite3_stmt,
    _idx: libc::c_int,
    _value: *const libc::c_void,
    _n: libc::c_int,
    _callback: extern "C" fn(*mut libc::c_void),
) -> libc::c_int {
    println!("sqlite3_bind_blob");
    SQLITE_ERROR
}

#[no_mangle]
pub extern "C" fn ext_sqlite3_bind_blob64(
    _stmt: *mut sqlite3_stmt,
    _idx: libc::c_int,
    _value: *const libc::c_void,
    _n: sqlite3_uint64,
    _callback: extern "C" fn(*mut libc::c_void),
) -> libc::c_int {
    println!("sqlite3_bind_blob64");
    SQLITE_ERROR
}

#[no_mangle]
pub extern "C" fn ext_sqlite3_bind_double(
    _stmt: *mut sqlite3_stmt,
    _idx: libc::c_int,
    _value: libc::c_double,
) -> libc::c_int {
    println!("sqlite3_bind_double");
    SQLITE_ERROR
}

#[no_mangle]
pub extern "C" fn ext_sqlite3_bind_int(
    _stmt: *mut sqlite3_stmt,
    _idx: libc::c_int,
    _value: libc::c_int,
) -> libc::c_int {
    println!("sqlite3_bind_int");
    SQLITE_ERROR
}

#[no_mangle]
pub extern "C" fn ext_sqlite3_bind_int64(
    _stmt: *mut sqlite3_stmt,
    _idx: libc::c_int,
    _value: sqlite3_int64,
) -> libc::c_int {
    println!("sqlite3_bind_int64");
    SQLITE_ERROR
}

#[no_mangle]
pub extern "C" fn ext_sqlite3_bind_null(
    _stmt: *mut sqlite3_stmt,
    _idx: libc::c_int,
) -> libc::c_int {
    println!("sqlite3_bind_null");
    SQLITE_ERROR
}

#[no_mangle]
pub extern "C" fn sqlite3_bind_parameter_count(_stmt: *mut sqlite3_stmt) -> libc::c_int {
    println!("sqlite3_bind_parameter_count");
    SQLITE_ERROR
}

#[no_mangle]
pub extern "C" fn sqlite3_bind_parameter_name(
    _stmt: *mut sqlite3_stmt,
    _n: libc::c_int,
) -> *const libc::c_char {
    println!("sqlite3_bind_parameter_name");
    std::ptr::null()
}

#[no_mangle]
pub extern "C" fn sqlite3_bind_text64(
    _stmt: *mut sqlite3_stmt,
    _idx: libc::c_int,
    _value: *const libc::c_char,
    _n: sqlite3_uint64,
    _callback: extern "C" fn(*mut libc::c_void),
    _encoding: libc::c_uchar,
) -> libc::c_int {
    println!("sqlite3_bind_text64");
    SQLITE_ERROR
}

#[no_mangle]
pub extern "C" fn ext_sqlite3_step(_stmt: *mut sqlite3_stmt) -> libc::c_int {
    println!("ext_sqlite3_step");
    SQLITE_ERROR
}

#[no_mangle]
pub extern "C" fn ext_sqlite3_column_int(
    _stmt: *mut sqlite3_stmt,
    _idx: libc::c_int,
) -> libc::c_int {
    println!("sqlite3_column_int");
    SQLITE_ERROR
}
