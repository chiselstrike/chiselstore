#ifndef SQLITE3_H
#define SQLITE3_H
#define SQLITE_OK 0
#define SQLITE_ERROR 1

typedef struct {
} sqlite3;

typedef struct {
} sqlite3_stmt;

#ifdef __cplusplus
extern "C" {
#endif

int ext_sqlite3_open(const char *filename, sqlite3 **db);

int ext_sqlite3_close(sqlite3 *db);

int ext_sqlite3_prepare(sqlite3 *db, const char *sql, int sql_len, sqlite3_stmt **stmt, const char **remaining_sql);

int ext_sqlite3_finalize(sqlite3_stmt *stmt);

int ext_sqlite3_bind_int(sqlite3_stmt *stmt, int idx, int value);

int ext_sqlite3_step(sqlite3_stmt *stmt);

int ext_sqlite3_column_int(sqlite3_stmt *stmt, int idx);

#ifdef __cplusplus
}
#endif

#endif /* SQLITE3_H */
