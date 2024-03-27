/* -----------------------------------------------------------------------
 *
 * openGauss locale utilities
 *
 * Portions Copyright (c) 2002-2012, PostgreSQL Global Development Group
 *
 * src/backend/utils/adt/pg_locale.c
 *
 * -----------------------------------------------------------------------
 */

/* ----------
 * Here is how the locale stuff is handled: LC_COLLATE and LC_CTYPE
 * are fixed at CREATE DATABASE time, stored in pg_database, and cannot
 * be changed. Thus, the effects of strcoll(), strxfrm(), isupper(),
 * toupper(), etc. are always in the same fixed locale.
 *
 * LC_MESSAGES is settable at run time and will take effect
 * immediately.
 *
 * The other categories, LC_MONETARY, LC_NUMERIC, and LC_TIME are also
 * settable at run-time.  However, we don't actually set those locale
 * categories permanently.	This would have bizarre effects like no
 * longer accepting standard floating-point literals in some locales.
 * Instead, we only set the locales briefly when needed, cache the
 * required information obtained from localeconv(), and set them back.
 * The cached information is only used by the formatting functions
 * (to_char, etc.) and the money type.	For the user, this should all be
 * transparent.
 *
 * !!! NOW HEAR THIS !!!
 *
 * We've been bitten repeatedly by this bug, so let's try to keep it in
 * mind in future: on some platforms, the locale functions return pointers
 * to static data that will be overwritten by any later locale function.
 * Thus, for example, the obvious-looking sequence
 *			save = setlocale(category, NULL);
 *			if (!setlocale(category, value))
 *				fail = true;
 *			setlocale(category, save);
 * DOES NOT WORK RELIABLY: on some platforms the second setlocale() call
 * will change the memory save is pointing at.	To do this sort of thing
 * safely, you *must* pstrdup what setlocale returns the first time.
 *
 * FYI, The Open Group locale standard is defined here:
 *
 *	http://www.opengroup.org/onlinepubs/009695399/basedefs/xbd_chap07.html
 * ----------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include <locale.h>

#include "catalog/pg_collation.h"
#include "catalog/pg_control.h"
#include "mb/pg_wchar.h"
#include "storage/ipc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/pg_locale.h"
#include "utils/syscache.h"
#include "miscadmin.h"
#include "executor/executor.h"

#define MAX_L10N_DATA 80

/* Cache for collation-related knowledge */

typedef struct {
    Oid collid;         /* hash key: pg_collation OID */
    bool collate_is_c;  /* is collation's LC_COLLATE C? */
    bool ctype_is_c;    /* is collation's LC_CTYPE C? */
    bool flags_valid;   /* true if above flags are valid */
    pg_locale_t locale; /* locale_t struct, or 0 if not valid */
} collation_cache_entry;

#if defined(WIN32) && defined(LC_MESSAGES)
static char* IsoLocaleName(const char*); /* MSVC specific */
#endif

/*
 * pg_perm_setlocale
 *
 * This is identical to the libc function setlocale(), with the addition
 * that if the operation is successful, the corresponding LC_XXX environment
 * variable is set to match.  By setting the environment variable, we ensure
 * that any subsequent use of setlocale(..., "") will preserve the settings
 * made through this routine.  Of course, LC_ALL must also be unset to fully
 * ensure that, but that has to be done elsewhere after all the individual
 * LC_XXX variables have been set correctly.  (Thank you Perl for making this
 * kluge necessary.)
 */
char* pg_perm_setlocale(int category, const char* locale)
{
    char* result = NULL;
    const char* envvar = NULL;
    char* envbuf = NULL;

#ifndef WIN32
    AutoMutexLock localeLock(&gLocaleMutex);
    localeLock.lock();

    result = gs_setlocale_r(category, locale);

    if (IsUnderPostmaster) {
        localeLock.unLock();
        return result;
    }
    localeLock.unLock();
#else

    /*
     * On Windows, setlocale(LC_MESSAGES) does not work, so just assume that
     * the given value is good and set it in the environment variables. We
     * must ignore attempts to set to "", which means "keep using the old
     * environment value".
     */
#ifdef LC_MESSAGES
    if (category == LC_MESSAGES) {
        result = (char*)locale;
        if (locale == NULL || locale[0] == '\0')
            return result;
    } else
#endif
        result = setlocale(category, locale);
#endif /* WIN32 */

    if (result == NULL) {
        return result; /* fall out immediately on failure */
    }

    switch (category) {
        case LC_COLLATE:
            envvar = "LC_COLLATE";
            envbuf = t_thrd.lc_cxt.lc_collate_envbuf;
            break;
        case LC_CTYPE:
            envvar = "LC_CTYPE";
            envbuf = t_thrd.lc_cxt.lc_ctype_envbuf;
            break;
#ifdef WIN32
            result = IsoLocaleName(locale);
            if (result == NULL)
                result = (char*)locale;
#endif /* WIN32 */
        case LC_MONETARY:
            envvar = "LC_MONETARY";
            envbuf = t_thrd.lc_cxt.lc_monetary_envbuf;
            break;
        case LC_NUMERIC:
            envvar = "LC_NUMERIC";
            envbuf = t_thrd.lc_cxt.lc_numeric_envbuf;
            break;
        case LC_TIME:
            envvar = "LC_TIME";
            envbuf = t_thrd.lc_cxt.lc_time_envbuf;
            break;
        case LC_MESSAGES:
            envvar = "LC_MESSAGES";
            envbuf = t_thrd.lc_cxt.lc_messages_envbuf;
            break;
        default:
            ereport(FATAL, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized LC category: %d", category)));
            envvar = NULL; /* keep compiler quiet */
            envbuf = NULL;
            return NULL;
    }

    errno_t ss_rc = snprintf_s(envbuf, LC_ENV_BUFSIZE, LC_ENV_BUFSIZE - 1, "%s=%s", envvar, result);
    if (ss_rc > LC_ENV_BUFSIZE - 1)
        return NULL;
    securec_check_ss(ss_rc, "\0", "\0");

    if (gs_putenv_r(envbuf))
        return NULL;

    return result;
}

/*
 * Is the locale name valid for the locale category?
 *
 * If successful, and canonname isn't NULL, a palloc'd copy of the locale's
 * canonical name is stored there.	This is especially useful for figuring out
 * what locale name "" means (ie, the server environment value).  (Actually,
 * it seems that on most implementations that's the only thing it's good for;
 * we could wish that setlocale gave back a canonically spelled version of
 * the locale name, but typically it doesn't.)
 */
bool check_locale(int category, const char* locale, char** canonname)
{
    char* save = NULL;
    char* res = NULL;

    if (canonname != NULL) {
        *canonname = NULL; /* in case of failure */
    }

    AutoMutexLock localeLock(&gLocaleMutex);
    localeLock.lock();
    save = gs_setlocale_r(category, NULL);
    if (save == NULL) {
        localeLock.unLock();
        return false; /* won't happen, we hope */
    }

    /* save may be pointing at a modifiable scratch variable, see above. */
    save = pstrdup(save);

    /* set the locale with setlocale, to see if it accepts it. */
    res = gs_setlocale_r(category, locale);

    /* save canonical name if requested. */
    if ((res != NULL) && (canonname != NULL))
        *canonname = pstrdup(res);

    /* restore old value. */
    if (!gs_setlocale_r(category, save))
        elog(WARNING, "failed to restore old locale \"%s\"", save);

    pfree_ext(save);
    localeLock.unLock();

    return (res != NULL);
}

/*
 * GUC check/assign hooks
 *
 * For most locale categories, the assign hook doesn't actually set the locale
 * permanently, just reset flags so that the next use will cache the
 * appropriate values.	(See explanation at the top of this file.)
 *
 * Note: we accept value = "" as selecting the postmaster's environment
 * value, whatever it was (so long as the environment setting is legal).
 * This will have been locked down by an earlier call to pg_perm_setlocale.
 */
bool check_locale_monetary(char** newval, void** extra, GucSource source)
{
    return check_locale(LC_MONETARY, *newval, NULL);
}

void assign_locale_monetary(const char* newval, void* extra)
{
    if (u_sess->attr.attr_common.locale_monetary != NULL) {
        errno_t rc = strncpy_s(NameStr(t_thrd.port_cxt.cur_monetary),
            NAMEDATALEN,
            u_sess->attr.attr_common.locale_monetary,
            NAMEDATALEN - 1);
        securec_check(rc, "\0", "\0");
    }
    u_sess->lc_cxt.cur_lc_conv_valid = false;
}

bool check_locale_numeric(char** newval, void** extra, GucSource source)
{
    return check_locale(LC_NUMERIC, *newval, NULL);
}

void assign_locale_numeric(const char* newval, void* extra)
{
    if (u_sess->attr.attr_common.locale_numeric) {
        errno_t rc = strncpy_s(NameStr(t_thrd.port_cxt.cur_numeric),
            NAMEDATALEN,
            u_sess->attr.attr_common.locale_numeric,
            NAMEDATALEN - 1);
        securec_check(rc, "\0", "\0");
    }
    u_sess->lc_cxt.cur_lc_conv_valid = false;
}

bool check_locale_time(char** newval, void** extra, GucSource source)
{
    return check_locale(LC_TIME, *newval, NULL);
}

void assign_locale_time(const char* newval, void* extra)
{
    u_sess->lc_cxt.cur_lc_time_valid = false;
}

/*
 * We allow LC_MESSAGES to actually be set globally.
 *
 * Note: we normally disallow value = "" because it wouldn't have consistent
 * semantics (it'd effectively just use the previous value).  However, this
 * is the value passed for PGC_S_DEFAULT, so don't complain in that case,
 * not even if the attempted setting fails due to invalid environment value.
 * The idea there is just to accept the environment setting *if possible*
 * during startup, until we can read the proper value from postgresql.conf.
 */
bool check_locale_messages(char** newval, void** extra, GucSource source)
{
    if (**newval == '\0') {
        if (source == PGC_S_DEFAULT)
            return true;
        else
            return false;
    }

    /*
     * LC_MESSAGES category does not exist everywhere, but accept it anyway
     *
     * On Windows, we can't even check the value, so accept blindly
     */
#if defined(LC_MESSAGES) && !defined(WIN32)
    return check_locale(LC_MESSAGES, *newval, NULL);
#else
    return true;
#endif
}

void assign_locale_messages(const char* newval, void* extra)
{
    /*
     * LC_MESSAGES category does not exist everywhere, but accept it anyway.
     * We ignore failure, as per comment above.
     */
#if defined(ENABLE_NLS) && defined(LC_MESSAGES)
    (void) pg_perm_setlocale(LC_MESSAGES, newval);
#endif
}

/*
 * Frees the malloced content of a struct lconv.  (But not the struct
 * itself.)
 */
static void free_struct_lconv(struct lconv* s)
{
    if (s == NULL) {
        return;
    }

    if (s->currency_symbol != NULL) {
        pfree(s->currency_symbol);
        s->currency_symbol = NULL;
    }
    if (s->decimal_point != NULL) {
        pfree(s->decimal_point);
        s->decimal_point = NULL;
    }
    if (s->grouping != NULL) {
        pfree(s->grouping);
        s->grouping = NULL;
    }
    if (s->thousands_sep != NULL) {
        pfree(s->thousands_sep);
        s->thousands_sep = NULL;
    }
    if (s->int_curr_symbol != NULL) {
        pfree(s->int_curr_symbol);
        s->int_curr_symbol = NULL;
    }
    if (s->mon_decimal_point != NULL) {
        pfree(s->mon_decimal_point);
        s->mon_decimal_point = NULL;
    }
    if (s->mon_grouping != NULL) {
        pfree(s->mon_grouping);
        s->mon_grouping = NULL;
    }
    if (s->mon_thousands_sep != NULL) {
        pfree(s->mon_thousands_sep);
        s->mon_thousands_sep = NULL;
    }
    if (s->negative_sign != NULL) {
        pfree(s->negative_sign);
        s->negative_sign = NULL;
    }
    if (s->positive_sign != NULL) {
        pfree(s->positive_sign);
        s->positive_sign = NULL;
    }

    s = NULL;
}

/*
 * Return a strdup'ed string converted from the specified encoding to the
 * database encoding.
 */
static char* db_encoding_strdup(int encoding, const char* str)
{
    char* pstr = NULL;
    char* mstr = NULL;

    /* convert the string to the database encoding */
    pstr = (char*)pg_do_encoding_conversion((unsigned char*)str, strlen(str), encoding, GetDatabaseEncoding());
    mstr = pstrdup(pstr);
    if (pstr != str)
        pfree_ext(pstr);

    return mstr;
}

/*
 * Return the POSIX lconv struct (contains number/money formatting
 * information) with locale information for all categories.
 */
struct lconv* PGLC_localeconv(void)
{
    struct lconv* curlconv = u_sess->lc_cxt.cur_lc_conv;
    struct lconv* extlconv = NULL;
    char* save_lc_monetary = NULL;
    char* save_lc_numeric = NULL;
    char* decimal_point = NULL;
    char* grouping = NULL;
    char* thousands_sep = NULL;
    int encoding;
    MemoryContext old_context = NULL;
    errno_t rc;

#ifdef WIN32
    char* save_lc_ctype = NULL;
#endif

    /**
     * If LC_MONETARY or LC_NUMERIC is different between thread and session context,
     * we should mark it is not valid, and should be reloaded.
     */
    if ((strcmp(u_sess->attr.attr_common.locale_monetary, NameStr(t_thrd.port_cxt.cur_monetary)) != 0) ||
        (strcmp(u_sess->attr.attr_common.locale_numeric, NameStr(t_thrd.port_cxt.cur_numeric)) != 0)) {
        u_sess->lc_cxt.cur_lc_conv_valid = false;
    }

    /* Did we do it already? */
    if (u_sess->lc_cxt.cur_lc_conv_valid)
        return curlconv;

    free_struct_lconv(curlconv);

    AutoMutexLock localeLock(&gLocaleMutex);
    localeLock.lock();

    /* Save user's values of monetary and numeric locales */
    save_lc_monetary = gs_setlocale_r(LC_MONETARY, NULL);
    if (save_lc_monetary != NULL)
        save_lc_monetary = pstrdup(save_lc_monetary);

    save_lc_numeric = gs_setlocale_r(LC_NUMERIC, NULL);
    if (save_lc_numeric != NULL)
        save_lc_numeric = pstrdup(save_lc_numeric);

#ifdef WIN32

    /*
     * Ideally, monetary and numeric local symbols could be returned in any
     * server encoding.  Unfortunately, the WIN32 API does not allow
     * setlocale() to return values in a codepage/CTYPE that uses more than
     * two bytes per character, like UTF-8:
     *
     * http://msdn.microsoft.com/en-us/library/x99tb11d.aspx
     *
     * Evidently, LC_CTYPE allows us to control the encoding used for strings
     * returned by localeconv().  The Open Group standard, mentioned at the
     * top of this C file, doesn't explicitly state this.
     *
     * Therefore, we set LC_CTYPE to match LC_NUMERIC or LC_MONETARY (which
     * cannot be UTF8), call localeconv(), and then convert from the
     * numeric/monitary LC_CTYPE to the server encoding.  One example use of
     * this is for the Euro symbol.
     *
     * Perhaps someday we will use GetLocaleInfoW() which returns values in
     * UTF16 and convert from that.
     */

    /* save user's value of ctype locale */
    save_lc_ctype = setlocale(LC_CTYPE, NULL);
    if (save_lc_ctype != NULL)
        save_lc_ctype = pstrdup(save_lc_ctype);

    /* use numeric to set the ctype */
    setlocale(LC_CTYPE, u_sess->attr.attr_common.locale_numeric);
#endif

    /* Get formatting information for numeric */
    gs_setlocale_r(LC_NUMERIC, u_sess->attr.attr_common.locale_numeric);
    extlconv = localeconv();

    /*
     * If first time in this function and exit because of an error(like encoding
     * conversion error), because CurrentLocaleConv = *extlconv, CurrentLocaleConv
     * holds some ptrs point to memory alloced in localeconv(), which is not managed
     * by us.
     * In second time in this function, we try to free ptrs in CurrentLocaleConv,
     * but the memory it pointed may be invalid already.
     * To fix this bug, we set these ptrs to NULL after struct assign.
     */
    PG_TRY();
    {
        old_context = MemoryContextSwitchTo(SelfMemoryContext);

        encoding = pg_get_encoding_from_locale(u_sess->attr.attr_common.locale_numeric, true);

        decimal_point = db_encoding_strdup(encoding, extlconv->decimal_point);
        thousands_sep = db_encoding_strdup(encoding, extlconv->thousands_sep);

        grouping = pstrdup(extlconv->grouping);

#ifdef WIN32
        /* use monetary to set the ctype */
        setlocale(LC_CTYPE, u_sess->attr.attr_common.locale_monetary);
#endif

        /* Get formatting information for monetary */
        gs_setlocale_r(LC_MONETARY, u_sess->attr.attr_common.locale_monetary);
        extlconv = localeconv();
        encoding = pg_get_encoding_from_locale(u_sess->attr.attr_common.locale_monetary, true);

        /*
         * Must copy all values since restoring internal settings may overwrite
         * localeconv()'s results.
         */
        *curlconv = *extlconv;
        curlconv->int_curr_symbol = NULL;
        curlconv->currency_symbol = NULL;
        curlconv->mon_decimal_point = NULL;
        curlconv->mon_grouping = NULL;
        curlconv->mon_thousands_sep = NULL;
        curlconv->negative_sign = NULL;
        curlconv->positive_sign = NULL;

        curlconv->decimal_point = decimal_point;
        curlconv->grouping = grouping;
        curlconv->thousands_sep = thousands_sep;
        curlconv->int_curr_symbol = db_encoding_strdup(encoding, extlconv->int_curr_symbol);
        curlconv->currency_symbol = db_encoding_strdup(encoding, extlconv->currency_symbol);
        curlconv->mon_decimal_point = db_encoding_strdup(encoding, extlconv->mon_decimal_point);

        curlconv->mon_grouping = pstrdup(extlconv->mon_grouping);
        curlconv->mon_thousands_sep = db_encoding_strdup(encoding, extlconv->mon_thousands_sep);
        curlconv->negative_sign = db_encoding_strdup(encoding, extlconv->negative_sign);
        curlconv->positive_sign = db_encoding_strdup(encoding, extlconv->positive_sign);

        MemoryContextSwitchTo(old_context);
    }
    PG_CATCH();
    {
        /* Try to restore internal settings */
        if (save_lc_monetary != NULL) {
            if (!gs_setlocale_r(LC_MONETARY, save_lc_monetary)) {
                elog(WARNING, "failed to restore old locale");
            }
            pfree_ext(save_lc_monetary);
        }

        if (save_lc_numeric != NULL) {
            if (!gs_setlocale_r(LC_NUMERIC, save_lc_numeric)) {
                elog(WARNING, "failed to restore old locale");
            }
            pfree_ext(save_lc_numeric);
        }

#ifdef WIN32
        /* Try to restore internal ctype settings */
        if (save_lc_ctype != NULL) {
            if (!gs_setlocale_r(LC_CTYPE, save_lc_ctype)) {
                elog(WARNING, "failed to restore old locale");
            }
            pfree_ext(save_lc_ctype);
        }
#endif

        free_struct_lconv(curlconv);

        MemoryContextSwitchTo(old_context);

        localeLock.unLock();

        PG_RE_THROW();
    }
    PG_END_TRY();

    /* Try to restore internal settings */
    if (save_lc_monetary != NULL) {
        if (!gs_setlocale_r(LC_MONETARY, save_lc_monetary)) {
            elog(WARNING, "failed to restore old locale");
        }
        pfree_ext(save_lc_monetary);
    }

    if (save_lc_numeric != NULL) {
        if (!gs_setlocale_r(LC_NUMERIC, save_lc_numeric)) {
            elog(WARNING, "failed to restore old locale");
        }
        pfree_ext(save_lc_numeric);
    }

#ifdef WIN32
    /* Try to restore internal ctype settings */
    if (save_lc_ctype != NULL) {
        if (!setlocale(LC_CTYPE, save_lc_ctype)) {
            elog(WARNING, "failed to restore old locale");
        }
        pfree_ext(save_lc_ctype);
    }
#endif

    localeLock.unLock();

    /* reload LC_MONETARY and LC_NUMERIC from session context to thread context */
    rc = strncpy_s(
        NameStr(t_thrd.port_cxt.cur_monetary), NAMEDATALEN, u_sess->attr.attr_common.locale_monetary, NAMEDATALEN - 1);
    securec_check(rc, "\0", "\0");
    rc = strncpy_s(
        NameStr(t_thrd.port_cxt.cur_numeric), NAMEDATALEN, u_sess->attr.attr_common.locale_numeric, NAMEDATALEN - 1);
    securec_check(rc, "\0", "\0");

    u_sess->lc_cxt.cur_lc_conv_valid = true;
    return curlconv;
}

#ifdef WIN32
/*
 * On WIN32, strftime() returns the encoding in CP_ACP (the default
 * operating system codpage for that computer), which is likely different
 * from SERVER_ENCODING.  This is especially important in Japanese versions
 * of Windows which will use SJIS encoding, which we don't support as a
 * server encoding.
 *
 * So, instead of using strftime(), use wcsftime() to return the value in
 * wide characters (internally UTF16) and then convert it to the appropriate
 * database encoding.
 *
 * Note that this only affects the calls to strftime() in this file, which are
 * used to get the locale-aware strings. Other parts of the backend use
 * pg_strftime(), which isn't locale-aware and does not need to be replaced.
 */
static size_t strftime_win32(char* dst, size_t dstlen, const wchar_t* format, const struct tm* tm)
{
    size_t len;
    wchar_t wformat[8]; /* formats used below need 3 bytes */
    wchar_t wbuf[MAX_L10N_DATA];
    int encoding;

    encoding = GetDatabaseEncoding();

    /* get a wchar_t version of the format string */
    len = MultiByteToWideChar(CP_UTF8, 0, format, -1, wformat, lengthof(wformat));
    if (len == 0)
        ereport(ERROR,
            (errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),
                errmsg("could not convert format string from UTF-8: error code %lu", GetLastError())));

    len = wcsftime(wbuf, MAX_L10N_DATA, wformat, tm);
    if (len == 0)

        /*
         * strftime failed, possibly because the result would not fit in
         * MAX_L10N_DATA.  Return 0 with the contents of dst unspecified.
         */
        return 0;

    len = WideCharToMultiByte(CP_UTF8, 0, wbuf, len, dst, dstlen - 1, NULL, NULL);
    if (len == 0)
        ereport(ERROR,
            (errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),
                errmsg("could not convert string to UTF-8: error code %lu", GetLastError())));

    dst[len] = '\0';
    if (encoding != PG_UTF8) {
        char* convstr = (char*)pg_do_encoding_conversion((unsigned char*)dst, len, PG_UTF8, encoding);

        if (dst != convstr) {
            strlcpy(dst, convstr, dstlen);
            len = strlen(dst);
        }
    }

    return len;
}

/* redefine strftime() */
#define strftime(a, b, c, d) strftime_win32(a, b, c, d)
#endif /* WIN32 */

/*
 * Free memory pointed by internal members of u_sess->lc_cxt.cur_lc_conv. These memory
 * are allocated by strdup in PGLC_localeconv.
 *
 * NOT free the memory pointed by u_sess->lc_cxt.cur_lc_conv itself. So we did not set
 * u_sess->lc_cxt.cur_lc_conv to NULL. u_sess->lc_cxt.cur_lc_conv is allocated by
 * palloc0 in knl_u_locale_init.
 */
void localeconv_deinitialize_session(void)
{
    if (u_sess->lc_cxt.cur_lc_conv_valid) {
        struct lconv* curlconv = u_sess->lc_cxt.cur_lc_conv;
        free_struct_lconv(curlconv);
        u_sess->lc_cxt.cur_lc_conv_valid = false;
    }
}

/* Subroutine for cache_locale_time(). */
static void cache_single_time(char** dst, const char* format, const struct tm* tm)
{
    char buf[MAX_L10N_DATA];
    char* ptr = NULL;

    /*
     * MAX_L10N_DATA is sufficient buffer space for every known locale, and
     * POSIX defines no strftime() errors.  (Buffer space exhaustion is not an
     * error.)  An implementation might report errors (e.g. ENOMEM) by
     * returning 0 (or, less plausibly, a negative value) and setting errno.
     * Report errno just in case the implementation did that, but clear it in
     * advance of the call so we don't emit a stale, unrelated errno.
     */
    errno = 0;
    if (strftime(buf, MAX_L10N_DATA, format, tm) <= 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("strftime(%s) failed: %m", format)));

    ptr = MemoryContextStrdup(u_sess->cache_mem_cxt, buf);
    if (*dst)
        pfree_ext(*dst);
    *dst = ptr;
}

/*
 * Update the lc_time localization cache variables if needed.
 */
void cache_locale_time(void)
{
    char* save_lc_time = NULL;
    time_t timenow;
    struct tm timeinfo;
    int i;

#ifdef WIN32
    char* save_lc_ctype = NULL;
#endif

    /* did we do this already? */
    if (u_sess->lc_cxt.cur_lc_time_valid)
        return;

    elog(DEBUG3, "cache_locale_time() executed; locale: \"%s\"", u_sess->attr.attr_common.locale_time);

    AutoMutexLock localeLock(&gLocaleMutex);
    localeLock.lock();

    /* save user's value of time locale */
    save_lc_time = gs_setlocale_r(LC_TIME, NULL);
    if (save_lc_time != NULL)
        save_lc_time = pstrdup(save_lc_time);

#ifdef WIN32

    /*
     * On WIN32, there is no way to get locale-specific time values in a
     * specified locale, like we do for monetary/numeric.  We can only get
     * CP_ACP (see strftime_win32) or UTF16.  Therefore, we get UTF16 and
     * convert it to the database locale.  However, wcsftime() internally uses
     * LC_CTYPE, so we set it here.  See the WIN32 comment near the top of
     * PGLC_localeconv().
     */

    /* save user's value of ctype locale */
    save_lc_ctype = setlocale(LC_CTYPE, NULL);
    if (save_lc_ctype != NULL)
        save_lc_ctype = pstrdup(save_lc_ctype);

    /* use lc_time to set the ctype */
    setlocale(LC_CTYPE, u_sess->attr.attr_common.locale_time);
#endif

    gs_setlocale_r(LC_TIME, u_sess->attr.attr_common.locale_time);

    timenow = time(NULL);
    localtime_r(&timenow, &timeinfo);

    /* localized days */
    for (i = 0; i < 7; i++) {
        timeinfo.tm_wday = i;
        cache_single_time(&u_sess->lc_cxt.localized_abbrev_days[i], "%a", &timeinfo);
        cache_single_time(&u_sess->lc_cxt.localized_full_days[i], "%A", &timeinfo);
    }

    /* localized months */
    for (i = 0; i < 12; i++) {
        timeinfo.tm_mon = i;
        timeinfo.tm_mday = 1; /* make sure we don't have invalid date */
        cache_single_time(&u_sess->lc_cxt.localized_abbrev_months[i], "%b", &timeinfo);
        cache_single_time(&u_sess->lc_cxt.localized_full_months[i], "%B", &timeinfo);
    }

    /* try to restore internal settings */
    if (save_lc_time != NULL) {
        if (!gs_setlocale_r(LC_TIME, save_lc_time))
            elog(WARNING, "failed to restore old locale");
        pfree_ext(save_lc_time);
    }

#ifdef WIN32
    /* try to restore internal ctype settings */
    if (save_lc_ctype != NULL) {
        if (!setlocale(LC_CTYPE, save_lc_ctype))
            elog(WARNING, "failed to restore old locale");
        pfree_ext(save_lc_ctype);
    }
#endif

    localeLock.unLock();
    u_sess->lc_cxt.cur_lc_time_valid = true;
}

#if defined(WIN32) && defined(LC_MESSAGES)
/*
 * Convert a Windows setlocale() argument to a Unix-style one.
 *
 * Regardless of platform, we install message catalogs under a Unix-style
 * LL[_CC][.ENCODING][@VARIANT] naming convention.  Only LC_MESSAGES settings
 * following that style will elicit localized interface strings.
 *
 * Before Visual Studio 2012 (msvcr110.dll), Windows setlocale() accepted "C"
 * (but not "c") and strings of the form <Language>[_<Country>][.<CodePage>],
 * case-insensitive.  setlocale() returns the fully-qualified form; for
 * example, setlocale("thaI") returns "Thai_Thailand.874".  Internally,
 * setlocale() and _create_locale() select a "locale identifier"[1] and store
 * it in an undocumented _locale_t field.  From that LCID, we can retrieve the
 * ISO 639 language and the ISO 3166 country.  Character encoding does not
 * matter, because the server and client encodings govern that.
 *
 * Windows Vista introduced the "locale name" concept[2], closely following
 * RFC 4646.  Locale identifiers are now deprecated.  Starting with Visual
 * Studio 2012, setlocale() accepts locale names in addition to the strings it
 * accepted historically.  It does not standardize them; setlocale("Th-tH")
 * returns "Th-tH".  setlocale(category, "") still returns a traditional
 * string.  Furthermore, msvcr110.dll changed the undocumented _locale_t
 * content to carry locale names instead of locale identifiers.
 *
 * MinGW headers declare _create_locale(), but msvcrt.dll lacks that symbol.
 * IsoLocaleName() always fails in a MinGW-built postgres.exe, so only
 * Unix-style values of the lc_messages GUC can elicit localized messages.  In
 * particular, every lc_messages setting that initdb can select automatically
 * will yield only C-locale messages.  XXX This could be fixed by running the
 * fully-qualified locale name through a lookup table.
 *
 * This function returns a pointer to a static buffer bearing the converted
 * name or NULL if conversion fails.
 *
 * [1] http://msdn.microsoft.com/en-us/library/windows/desktop/dd373763.aspx
 * [2] http://msdn.microsoft.com/en-us/library/windows/desktop/dd373814.aspx
 */
static char* IsoLocaleName(const char* winlocname)
{
#if (_MSC_VER >= 1400) /* VC8.0 or later */
    static char iso_lc_messages[32];
    _locale_t loct = NULL;
    errno_t ss_rc;

    if (pg_strcasecmp("c", winlocname) == 0 || pg_strcasecmp("posix", winlocname) == 0) {
        ss_rc = strcpy_s(iso_lc_messages, sizeof(iso_lc_messages), "C");
        securec_check(ss_rc, "\0", "\0");
        return iso_lc_messages;
    }

    loct = _create_locale(LC_CTYPE, winlocname);
    if (loct != NULL) {
#if (_MSC_VER >= 1700) /* Visual Studio 2012 or later */
        size_t rc;
        char* hyphen = NULL;

        /* Locale names use only ASCII, any conversion locale suffices. */
        rc = wchar2char(iso_lc_messages, loct->locinfo->locale_name[LC_CTYPE], sizeof(iso_lc_messages), NULL);
        _free_locale(loct);
        if (rc == -1 || rc == sizeof(iso_lc_messages))
            return NULL;

        /*
         * Since the message catalogs sit on a case-insensitive filesystem, we
         * need not standardize letter case here.  So long as we do not ship
         * message catalogs for which it would matter, we also need not
         * translate the script/variant portion, e.g. uz-Cyrl-UZ to
         * uz_UZ@cyrillic.  Simply replace the hyphen with an underscore.
         *
         * Note that the locale name can be less-specific than the value we
         * would derive under earlier Visual Studio releases.  For example,
         * French_France.1252 yields just "fr".  This does not affect any of
         * the country-specific message catalogs available as of this writing
         * (pt_BR, zh_CN, zh_TW).
         */
        hyphen = strchr(iso_lc_messages, '-');
        if (hyphen)
            *hyphen = '_';
#else
        char isolang[32], isocrty[32];
        LCID lcid;

        lcid = loct->locinfo->lc_handle[LC_CTYPE];
        if (lcid == 0)
            lcid = MAKELCID(MAKELANGID(LANG_ENGLISH, SUBLANG_ENGLISH_US), SORT_DEFAULT);
        _free_locale(loct);

        if (!GetLocaleInfoA(lcid, LOCALE_SISO639LANGNAME, isolang, sizeof(isolang)))
            return NULL;
        if (!GetLocaleInfoA(lcid, LOCALE_SISO3166CTRYNAME, isocrty, sizeof(isocrty)))
            return NULL;
        ss_rc = snprintf_s(
            iso_lc_messages, sizeof(iso_lc_messages), sizeof(iso_lc_messages) - 1, "%s_%s", isolang, isocrty);
        if (ss_rc > sizeof(iso_lc_messages) - 1)
            return NULL;
        securec_check_ss(ss_rc, "\0", "\0");
#endif
        return iso_lc_messages;
    }
    return NULL;
#else
    return NULL; /* Not supported on this version of msvc/mingw */
#endif /* _MSC_VER >= 1400 */
}
#endif /* WIN32 && LC_MESSAGES */

/*
 * Cache mechanism for collation information.
 *
 * We cache two flags: whether the collation's LC_COLLATE or LC_CTYPE is C
 * (or POSIX), so we can optimize a few code paths in various places.
 * For the built-in C and POSIX collations, we can know that without even
 * doing a cache lookup, but we want to support aliases for C/POSIX too.
 * For the "default" collation, there are separate static cache variables,
 * since consulting the pg_collation catalog doesn't tell us what we need.
 *
 * Also, if a pg_locale_t has been requested for a collation, we cache that
 * for the life of a backend.
 *
 * Note that some code relies on the flags not reporting false negatives
 * (that is, saying it's not C when it is).  For example, char2wchar()
 * could fail if the locale is C, so str_tolower() shouldn't call it
 * in that case.
 *
 * Note that we currently lack any way to flush the cache.	Since we don't
 * support ALTER COLLATION, this is OK.  The worst case is that someone
 * drops a collation, and a useless cache entry hangs around in existing
 * backends.
 */

static collation_cache_entry* lookup_collation_cache(Oid collation, bool set_flags)
{
    collation_cache_entry* cache_entry = NULL;
    bool found = false;

    Assert(OidIsValid(collation));
    Assert(collation != DEFAULT_COLLATION_OID);

    if (u_sess->lc_cxt.collation_cache == NULL) {
        /* First time through, initialize the hash table */
        HASHCTL ctl;

        errno_t rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
        securec_check(rc, "\0", "\0");
        ctl.keysize = sizeof(Oid);
        ctl.entrysize = sizeof(collation_cache_entry);
        ctl.hash = oid_hash;
        ctl.hcxt = u_sess->cache_mem_cxt;
        u_sess->lc_cxt.collation_cache =
            hash_create("Collation cache", 100, &ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
    }

    cache_entry = (collation_cache_entry*)hash_search(u_sess->lc_cxt.collation_cache, &collation, HASH_ENTER, &found);
    if (!found) {
        /*
         * Make sure cache entry is marked invalid, in case we fail before
         * setting things.
         */
        cache_entry->flags_valid = false;
        cache_entry->locale = 0;
    }

    if (set_flags && !cache_entry->flags_valid) {
        /* Attempt to set the flags */
        HeapTuple tp;
        Form_pg_collation collform;
        const char* collcollate = NULL;
        const char* collctype = NULL;

        tp = SearchSysCache1(COLLOID, ObjectIdGetDatum(collation));
        if (!HeapTupleIsValid(tp))
            ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for collation %u", collation)));
        collform = (Form_pg_collation)GETSTRUCT(tp);

        collcollate = NameStr(collform->collcollate);
        collctype = NameStr(collform->collctype);

        cache_entry->collate_is_c = ((strcmp(collcollate, "C") == 0) || (strcmp(collcollate, "POSIX") == 0));
        cache_entry->ctype_is_c = ((strcmp(collctype, "C") == 0) || (strcmp(collctype, "POSIX") == 0));

        cache_entry->flags_valid = true;

        ReleaseSysCache(tp);
    }

    return cache_entry;
}

/*
 * Detect whether collation's LC_COLLATE property is C
 */
bool lc_collate_is_c(Oid collation)
{
    /*
     * If we're asked about "collation 0", return false, so that the code will
     * go into the non-C path and report that the collation is bogus.
     */
    if (!OidIsValid(collation))
        return false;

    /*
     * If we're asked about the default collation, we have to inquire of the C
     * library.  Cache the result so we only have to compute it once.
     */
    if (collation == DEFAULT_COLLATION_OID) {
        char* localeptr = NULL;

        if (u_sess->lc_cxt.lc_collate_result >= 0)
            return (bool)u_sess->lc_cxt.lc_collate_result;

        AutoMutexLock localeLock(&gLocaleMutex);
        localeLock.lock();
        localeptr = gs_setlocale_r(LC_COLLATE, NULL);
        if (localeptr == NULL) {
            localeLock.unLock();
            ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION), errmsg("invalid LC_COLLATE setting")));
        }

        if (strcmp(localeptr, "C") == 0)
            u_sess->lc_cxt.lc_collate_result = true;
        else if (strcmp(localeptr, "POSIX") == 0)
            u_sess->lc_cxt.lc_collate_result = true;
        else
            u_sess->lc_cxt.lc_collate_result = false;

        localeLock.unLock();
        return (bool)u_sess->lc_cxt.lc_collate_result;
    }

    /*
     * If we're asked about the built-in C/POSIX collations, we know that.
     */
    if (collation == C_COLLATION_OID || collation == POSIX_COLLATION_OID)
        return true;

    /*
     * Otherwise, we have to consult pg_collation, but we cache that.
     */
    return (lookup_collation_cache(collation, true))->collate_is_c;
}

/*
 * Detect whether collation's LC_CTYPE property is C
 */
bool lc_ctype_is_c(Oid collation)
{
    /*
     * If we're asked about "collation 0", return false, so that the code will
     * go into the non-C path and report that the collation is bogus.
     */
    if (!OidIsValid(collation))
        return false;

    /*
     * If we're asked about the default collation, we have to inquire of the C
     * library.  Cache the result so we only have to compute it once.
     */
    if (collation == DEFAULT_COLLATION_OID) {
        char* localeptr = NULL;

        if (u_sess->lc_cxt.lc_ctype_result >= 0)
            return (bool)u_sess->lc_cxt.lc_ctype_result;

        AutoMutexLock localeLock(&gLocaleMutex);
        localeLock.lock();
        localeptr = gs_setlocale_r(LC_CTYPE, NULL);
        if (localeptr == NULL) {
            localeLock.unLock();
            ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION), errmsg("invalid LC_CTYPE setting")));
        }

        if (strcmp(localeptr, "C") == 0)
            u_sess->lc_cxt.lc_ctype_result = true;
        else if (strcmp(localeptr, "POSIX") == 0)
            u_sess->lc_cxt.lc_ctype_result = true;
        else
            u_sess->lc_cxt.lc_ctype_result = false;

        localeLock.unLock();
        return (bool)u_sess->lc_cxt.lc_ctype_result;
    }

    /*
     * If we're asked about the built-in C/POSIX collations, we know that.
     */
    if (collation == C_COLLATION_OID || collation == POSIX_COLLATION_OID)
        return true;

    /*
     * Otherwise, we have to consult pg_collation, but we cache that.
     */
    return (lookup_collation_cache(collation, true))->ctype_is_c;
}

/* simple subroutine for reporting errors from newlocale() */
#ifdef HAVE_LOCALE_T
static void report_newlocale_failure(const char* localename)
{
    /* copy errno in case one of the ereport auxiliary functions changes it */
    int save_errno = errno;

    /*
     * ENOENT means "no such locale", not "no such file", so clarify that
     * errno with an errdetail message.
     */
    ereport(ERROR,
        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("could not create locale \"%s\": %m", localename),
            ((save_errno == ENOENT)
                    ? errdetail(
                          "The operating system could not find any locale data for the locale name \"%s\".", localename)
                    : 0)));
}
#endif /* HAVE_LOCALE_T */

/*
 * Create a locale_t from a collation OID.	Results are cached for the
 * lifetime of the backend.  Thus, do not free the result with freelocale().
 *
 * As a special optimization, the default/database collation returns 0.
 * Callers should then revert to the non-locale_t-enabled code path.
 * In fact, they shouldn't call this function at all when they are dealing
 * with the default locale.  That can save quite a bit in hotspots.
 * Also, callers should avoid calling this before going down a C/POSIX
 * fastpath, because such a fastpath should work even on platforms without
 * locale_t support in the C library.
 *
 * For simplicity, we always generate COLLATE + CTYPE even though we
 * might only need one of them.  Since this is called only once per session,
 * it shouldn't cost much.
 */
pg_locale_t pg_newlocale_from_collation(Oid collid)
{
    collation_cache_entry* cache_entry = NULL;

    /* Callers must pass a valid OID */
    Assert(OidIsValid(collid));

    /* Return 0 for "default" collation, just in case caller forgets */
    if (collid == DEFAULT_COLLATION_OID)
        return (pg_locale_t)0;

    cache_entry = lookup_collation_cache(collid, false);

    if (cache_entry->locale == 0) {
        /* We haven't computed this yet in this session, so do it */
#ifdef HAVE_LOCALE_T
        HeapTuple tp;
        Form_pg_collation collform;
        const char* collcollate = NULL;
        const char* collctype = NULL;
        locale_t result;

        tp = SearchSysCache1(COLLOID, ObjectIdGetDatum(collid));
        if (!HeapTupleIsValid(tp))
            ereport(
                ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for collation %u", collid)));
        collform = (Form_pg_collation)GETSTRUCT(tp);

        collcollate = NameStr(collform->collcollate);
        collctype = NameStr(collform->collctype);

        if (strcmp(collcollate, collctype) == 0) {
            /* Normal case where they're the same */
#ifndef WIN32
            result = newlocale(LC_COLLATE_MASK | LC_CTYPE_MASK, collcollate, NULL);
#else
            result = _create_locale(LC_ALL, collcollate);
#endif
            if (!result)
                report_newlocale_failure(collcollate);
        } else {
#ifndef WIN32
            /* We need two newlocale() steps */
            locale_t loc1;

            loc1 = newlocale(LC_COLLATE_MASK, collcollate, NULL);
            if (!loc1)
                report_newlocale_failure(collcollate);
            result = newlocale(LC_CTYPE_MASK, collctype, loc1);
            if (!result)
                report_newlocale_failure(collctype);
#else

            /*
             * XXX The _create_locale() API doesn't appear to support this.
             * Could perhaps be worked around by changing pg_locale_t to
             * contain two separate fields.
             */
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("collations with different collate and ctype values are not supported on this platform")));
#endif
        }

        cache_entry->locale = result;

        ReleaseSysCache(tp);
#else  /* not HAVE_LOCALE_T */

        /*
         * For platforms that don't support locale_t, we can't do anything
         * with non-default collations.
         */
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("nondefault collations are not supported on this platform")));
#endif /* not HAVE_LOCALE_T */
    }

    return cache_entry->locale;
}

/*
 * These functions convert from/to libc's wchar_t, *not* pg_wchar_t.
 * Therefore we keep them here rather than with the mbutils code.
 */

#ifdef USE_WIDE_UPPER_LOWER

/*
 * wchar2char --- convert wide characters to multibyte format
 *
 * This has the same API as the standard wcstombs_l() function; in particular,
 * tolen is the maximum number of bytes to store at *to, and *from must be
 * zero-terminated.  The output will be zero-terminated iff there is room.
 */
size_t wchar2char(char* to, const wchar_t* from, size_t tolen, pg_locale_t locale)
{
    size_t result;

    if (tolen == 0)
        return 0;

#ifdef WIN32

    /*
     * On Windows, the "Unicode" locales assume UTF16 not UTF8 encoding, and
     * for some reason mbstowcs and wcstombs won't do this for us, so we use
     * MultiByteToWideChar().
     */
    if (GetDatabaseEncoding() == PG_UTF8) {
        result = WideCharToMultiByte(CP_UTF8, 0, from, -1, to, tolen, NULL, NULL);
        /* A zero return is failure */
        if (result <= 0)
            result = -1;
        else {
            Assert(result <= tolen);
            /* Microsoft counts the zero terminator in the result */
            result--;
        }
    } else
#endif /* WIN32 */
        if (locale == (pg_locale_t)0) {
        /* Use wcstombs directly for the default locale */
        result = wcstombs(to, from, tolen);
    } else {
#ifdef HAVE_LOCALE_T
#ifdef HAVE_WCSTOMBS_L
        /* Use wcstombs_l for nondefault locales */
        result = wcstombs_l(to, from, tolen, locale);
#else  /* !HAVE_WCSTOMBS_L */
        /* We have to temporarily set the locale as current ... ugh */
        locale_t save_locale = uselocale(locale);

        result = wcstombs(to, from, tolen);

        uselocale(save_locale);
#endif /* HAVE_WCSTOMBS_L */
#else  /* !HAVE_LOCALE_T */
        /* Can't have locale != 0 without HAVE_LOCALE_T */
        ereport(ERROR, (errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE), errmsg("wcstombs_l is not available")));
        result = 0; /* keep compiler quiet */
#endif /* HAVE_LOCALE_T */
    }

    return result;
}

/*
 * char2wchar --- convert multibyte characters to wide characters
 *
 * This has almost the API of mbstowcs_l(), except that *from need not be
 * null-terminated; instead, the number of input bytes is specified as
 * fromlen.  Also, we ereport() rather than returning -1 for invalid
 * input encoding.	tolen is the maximum number of wchar_t's to store at *to.
 * The output will be zero-terminated iff there is room.
 */
size_t char2wchar(wchar_t* to, size_t tolen, const char* from, size_t fromlen, pg_locale_t locale)
{
    size_t result;

    if (tolen == 0)
        return 0;

#ifdef WIN32
    /* See WIN32 "Unicode" comment above */
    if (GetDatabaseEncoding() == PG_UTF8) {
        /* Win32 API does not work for zero-length input */
        if (fromlen == 0)
            result = 0;
        else {
            result = MultiByteToWideChar(CP_UTF8, 0, from, fromlen, to, tolen - 1);
            /* A zero return is failure */
            if (result == 0)
                result = -1;
        }

        if (result != -1) {
            Assert(result < tolen);
            /* Append trailing null wchar (MultiByteToWideChar() does not) */
            to[result] = 0;
        }
    } else
#endif /* WIN32 */
    {
        /* mbstowcs requires ending '\0' */
        char* str = pnstrdup(from, fromlen);

        if (locale == (pg_locale_t)0) {
            /* Use mbstowcs directly for the default locale */
            result = mbstowcs(to, str, tolen);
        } else {
#ifdef HAVE_LOCALE_T
#ifdef HAVE_MBSTOWCS_L
            /* Use mbstowcs_l for nondefault locales */
            result = mbstowcs_l(to, str, tolen, locale);
#else  /* !HAVE_MBSTOWCS_L */
            /* We have to temporarily set the locale as current ... ugh */
            locale_t save_locale = uselocale(locale);

            result = mbstowcs(to, str, tolen);

            uselocale(save_locale);
#endif /* HAVE_MBSTOWCS_L */
#else  /* !HAVE_LOCALE_T */
            /* Can't have locale != 0 without HAVE_LOCALE_T */
            ereport(ERROR, (errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE), errmsg("mbstowcs_l is not available")));
            result = 0; /* keep compiler quiet */
#endif /* HAVE_LOCALE_T */
        }

        pfree_ext(str);
    }

    if ((int)result == -1) {
        /*
         * Invalid multibyte character encountered.  We try to give a useful
         * error message by letting pg_verifymbstr check the string.  But it's
         * possible that the string is OK to us, and not OK to mbstowcs ---
         * this suggests that the LC_CTYPE locale is different from the
         * database encoding.  Give a generic error message if verifymbstr
         * can't find anything wrong.
         */
        pg_verifymbstr(from, fromlen, false); /* might not return */
        /* but if it does ... */
        ereport(ERROR,
            (errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),
                errmsg("invalid multibyte character for locale"),
                errhint("The server's LC_CTYPE locale is probably incompatible with the database encoding.")));
    }

    return result;
}

#endif /* USE_WIDE_UPPER_LOWER */

/* for locale memory leak in multithreading */
void freeLocaleCache(bool threadExit)
{
    HASH_SEQ_STATUS hash_seq;
    collation_cache_entry* cache_entry = NULL;

    if (threadExit &&
        t_thrd.port_cxt.save_locale_r != (pg_locale_t)0) {
        freelocale(t_thrd.port_cxt.save_locale_r);
        t_thrd.port_cxt.save_locale_r = (pg_locale_t)0;
    }

    if (u_sess->lc_cxt.collation_cache == NULL)
        return;

    hash_seq_init(&hash_seq, u_sess->lc_cxt.collation_cache);
    while ((cache_entry = (collation_cache_entry*)hash_seq_search(&hash_seq)) != NULL) {
        if (cache_entry->locale != 0) {
#ifndef WIN32
            freelocale(cache_entry->locale);
#else
            _free_locale(cache_entry->locale);
#endif
            cache_entry->locale = 0;
        }
    }
}
