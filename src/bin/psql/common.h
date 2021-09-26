/*
 * psql - the openGauss interactive terminal
 *
 * Copyright (c) 2000-2012, PostgreSQL Global Development Group
 *
 * src/bin/psql/common.h
 */
#ifndef COMMON_H
#define COMMON_H

#include "settings.h"
#include "postgres_fe.h"
#include <setjmp.h>
#include "libpq/libpq-fe.h"

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

#ifdef USE_ASSERT_CHECKING
#include <assert.h>
#define psql_assert(p) assert(p)
#else
#define psql_assert(p)
#endif

#define atooid(x) ((Oid)strtoul((x), NULL, 10))

extern bool canAddHist;

#define MAX_STMTS 1024
#define DEFAULT_RETRY_TIMES 5
#define MAX_RETRY_TIMES 10
#define ERRCODE_LENGTH 5

#if defined(__LP64__) || defined(__64BIT__)
typedef unsigned int GS_UINT32;
#else
typedef unsigned long GS_UINT32;
#endif

/*
 * Safer versions of some standard C library functions. If an
 * out-of-memory condition occurs, these functions will bail out
 * safely; therefore, their return value is guaranteed to be non-NULL.
 */
extern char* pg_strdup(const char* string);
extern void* pg_malloc(size_t size);
extern void* pg_malloc_zero(size_t size);
extern void* pg_calloc(size_t nmemb, size_t size);
extern void* psql_realloc(void* ptr, size_t oldSize, size_t newSize);

extern bool setQFout(const char* fname);

extern void psql_error(const char* fmt, ...)
    /* This lets gcc check the format string for consistency. */
    __attribute__((format(PG_PRINTF_ATTRIBUTE, 1, 2)));

extern void NoticeProcessor(void* arg, const char* message);

extern volatile bool sigint_interrupt_enabled;

extern sigjmp_buf sigint_interrupt_jmp;

extern volatile bool cancel_pressed;

/* Note: cancel_pressed is defined in print.c, see that file for reasons */

extern void setup_cancel_handler(void);
extern void ignore_quit_signal(void);

extern void SetCancelConn(void);
extern void ResetCancelConn(void);

extern PGresult* PSQLexec(const char* query, bool start_xact);
extern void EmptyRetryErrcodesList(ErrCodes& list);
extern bool IsQueryNeedRetry(const char* sqlstate);
extern void ResetQueryRetryController();
extern bool QueryRetryController(const char* query);
extern bool SendQuery(const char* query, bool is_print = true, bool print_error = true);
extern bool MakeCopyWorker(const char* query, int nclients);

extern bool is_superuser(void);
extern bool standard_strings(void);
extern const char* session_username(void);

extern void expand_tilde(char** filename);
extern bool do_parallel_execution(int count, char** stmts);
extern char* GetEnvStr(const char* env);

#endif /* COMMON_H */
