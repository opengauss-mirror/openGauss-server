/* -----------------------------------------------------------------------
 *
 * PostgreSQL locale utilities
 *
 * src/include/utils/pg_locale.h
 *
 * Copyright (c) 2002-2012, PostgreSQL Global Development Group
 *
 * -----------------------------------------------------------------------
 */

#ifndef _PG_LOCALE_
#define _PG_LOCALE_

#include <locale.h>
#ifdef LOCALE_T_IN_XLOCALE
#include <xlocale.h>
#endif

#include "utils/guc.h"

extern bool check_locale_messages(char** newval, void** extra, GucSource source);
extern void assign_locale_messages(const char* newval, void* extra);
extern bool check_locale_monetary(char** newval, void** extra, GucSource source);
extern void assign_locale_monetary(const char* newval, void* extra);
extern bool check_locale_numeric(char** newval, void** extra, GucSource source);
extern void assign_locale_numeric(const char* newval, void* extra);
extern bool check_locale_time(char** newval, void** extra, GucSource source);
extern void assign_locale_time(const char* newval, void* extra);

extern bool check_locale(int category, const char* locale, char** canonname);
extern char* pg_perm_setlocale(int category, const char* locale);

extern bool lc_collate_is_c(Oid collation);
extern bool lc_ctype_is_c(Oid collation);

/*
 * Return the POSIX lconv struct (contains number/money formatting
 * information) with locale information for all categories.
 */
extern struct lconv* PGLC_localeconv(void);

/*
 * Free memory pointed by internal members of u_sess->lc_cxt.cur_lc_conv. These memory
 * are allocated by strdup in PGLC_localeconv.
 *
 * NOT free the memory pointed by u_sess->lc_cxt.cur_lc_conv itself. So we did not set
 * u_sess->lc_cxt.cur_lc_conv to NULL. u_sess->lc_cxt.cur_lc_conv is allocated by
 * palloc0 in knl_u_locale_init.
 */
extern void localeconv_deinitialize_session(void);

extern void cache_locale_time(void);

void freeLocaleCache(bool threadExit);

/*
 * We define our own wrapper around locale_t so we can keep the same
 * function signatures for all builds, while not having to create a
 * fake version of the standard type locale_t in the global namespace.
 * The fake version of pg_locale_t can be checked for truth; that's
 * about all it will be needed for.
 */
#ifdef HAVE_LOCALE_T
typedef locale_t pg_locale_t;
#else
typedef int pg_locale_t;
#endif

extern pg_locale_t pg_newlocale_from_collation(Oid collid);

/* These functions convert from/to libc's wchar_t, *not* pg_wchar_t */
#ifdef USE_WIDE_UPPER_LOWER
extern size_t wchar2char(char* to, const wchar_t* from, size_t tolen, pg_locale_t locale);
extern size_t char2wchar(wchar_t* to, size_t tolen, const char* from, size_t fromlen, pg_locale_t locale);
#endif

#endif /* _PG_LOCALE_ */
