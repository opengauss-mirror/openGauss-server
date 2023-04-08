/* -------------------------------------------------------------------------
 *
 * Utility routines for SQL dumping
 *	Basically this is stuff that is useful in both pg_dump and pg_dumpall.
 *	Lately it's also being used by psql and bin/scripts/ ...
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/bin/pg_dump/dumputils.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef DUMPUTILS_H
#define DUMPUTILS_H

#include "libpq/libpq-fe.h"
#include "libpq/pqexpbuffer.h"
#include "assert.h"

/*
 * Assert
 */
#undef ddr_Assert
#if PC_LINT
#define ddr_Assert(x) ((!(x)) ? abort() : (void)0)
#elif USE_ASSERT_CHECKING
#define ddr_Assert(x) assert(x)
#else
#define ddr_Assert(x) ((void)0)
#endif

#undef _
#define _(x) x

#define DIR_LOCK_FILE "dir.lock"
#define DIR_SEPARATOR "/"
#define FILE_PERMISSION 0600
#define MAX_PASSWDLEN 100

#define  A_FORMAT  "A"
#define  B_FORMAT  "B"
#define  C_FORMAT  "C"
#define  PG_FORMAT  "PG"

/* Data structures for simple lists of OIDs and strings.  The support for
 * these is very primitive compared to the backend's List facilities, but
 * it's all we need in pg_dump.
 */

typedef struct SimpleOidListCell {
    struct SimpleOidListCell* next;
    Oid val;
} SimpleOidListCell;

typedef struct SimpleOidList {
    SimpleOidListCell* head;
    SimpleOidListCell* tail;
} SimpleOidList;

typedef struct SimpleStringListCell {
    struct SimpleStringListCell* next;
    char val[1]; /* VARIABLE LENGTH FIELD */
} SimpleStringListCell;

typedef struct SimpleStringList {
    SimpleStringListCell* head;
    SimpleStringListCell* tail;
} SimpleStringList;

typedef enum /* bits returned by set_dump_section */
{ DUMP_PRE_DATA = 0x01,
    DUMP_DATA = 0x02,
    DUMP_POST_DATA = 0x04,
    DUMP_UNSECTIONED = 0xff } DumpSections;

/* dump and restore version string*/
#define GS_DUMP_RESTORE_VERSION "Gauss MPPDB Tools V100R003C00B540"
#define GS_FREE(ptr)            \
    do {                        \
        if (NULL != (ptr)) {    \
            free((char*)(ptr)); \
            ptr = NULL;         \
        }                       \
    } while (0)

typedef void (*on_exit_nicely_callback)(int code, void* arg);

extern int quote_all_identifiers;
extern const char* progname;
extern const char* dbname;
extern const char* instport;
extern const char *gdatcompatibility;
extern char* binary_upgrade_oldowner;
extern char* binary_upgrade_newowner;

extern void init_parallel_dump_utils(void);
extern const char* fmtId(const char* identifier);
extern void appendStringLiteral(PQExpBuffer buf, const char* str, int encoding, bool std_strings);
extern void appendStringLiteralConn(PQExpBuffer buf, const char* str, PGconn* conn);
extern void appendStringLiteralDQ(PQExpBuffer buf, const char* str, const char* dqprefix);
extern void appendByteaLiteral(PQExpBuffer buf, const unsigned char* str, size_t length, bool std_strings);
extern int parse_version(const char* versionString);
extern bool parsePGArray(const char* atext, char*** itemarray, int* nitems);
extern char* getRolePassword(PGconn* conn, const char* user);
extern bool buildACLCommands(const char* name, const char* subname, const char* type, const char* acls,
    const char* owner, const char* prefix, int remoteVersion, PQExpBuffer sql, PGconn* conn);
extern bool buildDefaultACLCommands(const char* type, const char* nspname, const char* acls, const char* owner,
    int remoteVersion, PQExpBuffer sql, PGconn* conn);
extern bool processSQLNamePattern(PGconn* conn, PQExpBuffer buf, const char* pattern, bool have_where,
    bool force_escape, const char* schemavar, const char* namevar, const char* altnamevar, const char* visibilityrule);
extern void buildShSecLabelQuery(PGconn* conn, const char* catalog_name, uint32 objectId, PQExpBuffer sql);
extern void emitShSecLabels(PGconn* conn, PGresult* res, PQExpBuffer buffer, const char* target, const char* objname);
extern void set_dump_section(const char* arg, int* dumpSections);
extern void write_msg(const char* modulename, const char* fmt, ...) __attribute__((format(PG_PRINTF_ATTRIBUTE, 2, 3)));
extern void vwrite_msg(const char* modulename, const char* fmt, va_list ap)
    __attribute__((format(PG_PRINTF_ATTRIBUTE, 2, 0)));
extern void exit_horribly(const char* modulename, const char* fmt, ...)
    __attribute__((format(PG_PRINTF_ATTRIBUTE, 2, 3), noreturn));
extern void on_exit_nicely(on_exit_nicely_callback function, void* arg);
extern void exit_nicely(int code) __attribute__((noreturn));

extern void simple_oid_list_append(SimpleOidList* list, Oid val);
extern void simple_string_list_append(SimpleStringList* list, const char* val);
extern bool simple_oid_list_member(SimpleOidList* list, Oid val);
extern bool simple_string_list_member(SimpleStringList* list, const char* val);
extern void replace_password(int argc, char** argv, const char* optionName);
extern int catalog_lock(const char* filePath);
extern void catalog_unlock(int code, void* arg);
extern void remove_lock_file(int code, void* arg);
extern int dump_flock(int fd, uint32 operation);
extern bool fileExists(const char* path);
extern bool IsDir(const char* dirpath);
extern char* make_absolute_path(const char* path);
extern bool isExistsSQLResult(PGconn* conn, const char* sqlCmd);
extern bool is_column_exists(PGconn* conn, Oid relid, const char* column_name);
#ifndef ENABLE_MULTIPLE_NODES
extern bool SetUppercaseAttributeNameToOff(PGconn* conn);
#endif

#endif /* DUMPUTILS_H */
