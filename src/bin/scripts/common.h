/*
 *	common.h
 *		Common support routines for bin/scripts/
 *
 *	Copyright (c) 2003-2012, PostgreSQL Global Development Group
 *
 *	src/bin/scripts/common.h
 */
#ifndef COMMON_H
#define COMMON_H

#include "libpq/libpq-fe.h"
#include "getopt_long.h"       /* pgrminclude ignore */
#include "libpq/pqexpbuffer.h" /* pgrminclude ignore */
#include "dumpmem.h"           /* pgrminclude ignore */
#include "securec.h"
#include "securec_check.h"

#define check_memcpy_s(r) securec_check_c((r), "", "")
#define check_memmove_s(r) securec_check_c((r), "", "")
#define check_memset_s(r) securec_check_c((r), "", "")
#define check_strcpy_s(r) securec_check_c((r), "", "")
#define check_strncpy_s(r) securec_check_c((r), "", "")
#define check_strcat_s(r) securec_check_c((r), "", "")
#define check_strncat_s(r) securec_check_c((r), "", "")
#define check_gets_s(r) securec_check_ss_c((r), "", "")
#define check_sprintf_s(r) securec_check_ss_c((r), "", "")
#define check_snprintf_s(r) securec_check_ss_c((r), "", "")
#define check_scanf_s(r) securec_check_ss_c((r), "", "")

enum trivalue { TRI_DEFAULT, TRI_NO, TRI_YES };

typedef void (*help_handler)(const char* progname);

extern const char* get_user_name(const char* progname);

extern void handle_help_version_opts(int argc, char* argv[], const char* fixed_progname, help_handler hlp);

extern PGconn* connectDatabase(const char* dbname, const char* pghost, const char* pgport, const char* pguser,
    enum trivalue prompt_password, const char* progname, bool fail_ok);

extern PGconn* connectMaintenanceDatabase(const char* maintenance_db, const char* pghost, const char* pgport,
    const char* pguser, enum trivalue prompt_password, const char* progname);

extern PGresult* executeQuery(PGconn* conn, const char* query, const char* progname, bool echo);

extern void executeCommand(PGconn* conn, const char* query, const char* progname, bool echo);

extern bool executeMaintenanceCommand(PGconn* conn, const char* query, bool echo);

extern bool yesno_prompt(const char* question);

extern void setup_cancel_handler(void);

extern char* pg_strdup(const char* string);
extern char* GetEnvStr(const char* env);


#endif /* COMMON_H */

