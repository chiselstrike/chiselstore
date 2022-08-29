/*
    cargo build \
       && gcc -Wall -Iinclude test.c ../target/debug/libsqlite.a -lsqlite3 \
       && ./a.out
*/

#include "sqlite.h"

int main(int argc, char *argv[])
{
	sqlite3 *db;

	ext_sqlite3_open("", &db);

	ext_sqlite3_close(db);

	return 0;
}
