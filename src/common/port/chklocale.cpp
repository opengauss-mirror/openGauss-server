/* -------------------------------------------------------------------------
 *
 * chklocale.cpp
 *		Functions for handling locale-related info
 *
 *
 * Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/common/port/chklocale.cpp
 *
 * -------------------------------------------------------------------------
 */

#ifndef FRONTEND
#include "postgres.h"
#include "knl/knl_variable.h"
#else
#include "postgres_fe.h"
#endif

#include <locale.h>
#ifdef HAVE_LANGINFO_H
#include <langinfo.h>
#endif

#include "mb/pg_wchar.h"

#ifndef WIN32
extern THR_LOCAL PGDLLIMPORT bool IsUnderPostmaster;
#else
extern THR_LOCAL bool IsUnderPostmaster;
#endif
/*
 * This table needs to recognize all the CODESET spellings for supported
 * backend encodings, as well as frontend-only encodings where possible
 * (the latter case is currently only needed for initdb to recognize
 * error situations).  On Windows, we rely on entries for codepage
 * numbers (CPnnn).
 *
 * Note that we search the table with pg_strcasecmp(), so variant
 * capitalizations don't need their own entries.
 */
struct encoding_match {
    enum pg_enc pg_enc_code;
    const char* system_enc_name;
};

static const struct encoding_match encoding_match_list[] = {
    {PG_EUC_JP, "EUC-JP"},
    {PG_EUC_JP, "eucJP"},
    {PG_EUC_JP, "IBM-eucJP"},
    {PG_EUC_JP, "sdeckanji"},
    {PG_EUC_JP, "CP20932"},

    {PG_EUC_CN, "EUC-CN"},
    {PG_EUC_CN, "eucCN"},
    {PG_EUC_CN, "IBM-eucCN"},
    {PG_EUC_CN, "GB2312"},
    {PG_EUC_CN, "dechanzi"},
    {PG_EUC_CN, "CP20936"},

    {PG_EUC_KR, "EUC-KR"},
    {PG_EUC_KR, "eucKR"},
    {PG_EUC_KR, "IBM-eucKR"},
    {PG_EUC_KR, "deckorean"},
    {PG_EUC_KR, "5601"},
    {PG_EUC_KR, "CP51949"},

    {PG_EUC_TW, "EUC-TW"},
    {PG_EUC_TW, "eucTW"},
    {PG_EUC_TW, "IBM-eucTW"},
    {PG_EUC_TW, "cns11643"},
    /* No codepage for EUC-TW ? */

    {PG_UTF8, "UTF-8"},
    {PG_UTF8, "utf8"},
    {PG_UTF8, "CP65001"},

    {PG_LATIN1, "ISO-8859-1"},
    {PG_LATIN1, "ISO8859-1"},
    {PG_LATIN1, "iso88591"},
    {PG_LATIN1, "CP28591"},

    {PG_LATIN2, "ISO-8859-2"},
    {PG_LATIN2, "ISO8859-2"},
    {PG_LATIN2, "iso88592"},
    {PG_LATIN2, "CP28592"},

    {PG_LATIN3, "ISO-8859-3"},
    {PG_LATIN3, "ISO8859-3"},
    {PG_LATIN3, "iso88593"},
    {PG_LATIN3, "CP28593"},

    {PG_LATIN4, "ISO-8859-4"},
    {PG_LATIN4, "ISO8859-4"},
    {PG_LATIN4, "iso88594"},
    {PG_LATIN4, "CP28594"},

    {PG_LATIN5, "ISO-8859-9"},
    {PG_LATIN5, "ISO8859-9"},
    {PG_LATIN5, "iso88599"},
    {PG_LATIN5, "CP28599"},

    {PG_LATIN6, "ISO-8859-10"},
    {PG_LATIN6, "ISO8859-10"},
    {PG_LATIN6, "iso885910"},

    {PG_LATIN7, "ISO-8859-13"},
    {PG_LATIN7, "ISO8859-13"},
    {PG_LATIN7, "iso885913"},

    {PG_LATIN8, "ISO-8859-14"},
    {PG_LATIN8, "ISO8859-14"},
    {PG_LATIN8, "iso885914"},

    {PG_LATIN9, "ISO-8859-15"},
    {PG_LATIN9, "ISO8859-15"},
    {PG_LATIN9, "iso885915"},
    {PG_LATIN9, "CP28605"},

    {PG_LATIN10, "ISO-8859-16"},
    {PG_LATIN10, "ISO8859-16"},
    {PG_LATIN10, "iso885916"},

    {PG_KOI8R, "KOI8-R"},
    {PG_KOI8R, "CP20866"},

    {PG_KOI8U, "KOI8-U"},
    {PG_KOI8U, "CP21866"},

    {PG_WIN866, "CP866"},
    {PG_WIN874, "CP874"},
    {PG_WIN1250, "CP1250"},
    {PG_WIN1251, "CP1251"},
    {PG_WIN1251, "ansi-1251"},
    {PG_WIN1252, "CP1252"},
    {PG_WIN1253, "CP1253"},
    {PG_WIN1254, "CP1254"},
    {PG_WIN1255, "CP1255"},
    {PG_WIN1256, "CP1256"},
    {PG_WIN1257, "CP1257"},
    {PG_WIN1258, "CP1258"},

    {PG_ISO_8859_5, "ISO-8859-5"},
    {PG_ISO_8859_5, "ISO8859-5"},
    {PG_ISO_8859_5, "iso88595"},
    {PG_ISO_8859_5, "CP28595"},

    {PG_ISO_8859_6, "ISO-8859-6"},
    {PG_ISO_8859_6, "ISO8859-6"},
    {PG_ISO_8859_6, "iso88596"},
    {PG_ISO_8859_6, "CP28596"},

    {PG_ISO_8859_7, "ISO-8859-7"},
    {PG_ISO_8859_7, "ISO8859-7"},
    {PG_ISO_8859_7, "iso88597"},
    {PG_ISO_8859_7, "CP28597"},

    {PG_ISO_8859_8, "ISO-8859-8"},
    {PG_ISO_8859_8, "ISO8859-8"},
    {PG_ISO_8859_8, "iso88598"},
    {PG_ISO_8859_8, "CP28598"},

    {PG_SJIS, "SJIS"},
    {PG_SJIS, "PCK"},
    {PG_SJIS, "CP932"},
    {PG_SJIS, "SHIFT_JIS"},

    {PG_BIG5, "BIG5"},
    {PG_BIG5, "BIG5HKSCS"},
    {PG_BIG5, "Big5-HKSCS"},
    {PG_BIG5, "CP950"},

    {PG_GBK, "GBK"},
    {PG_GBK, "CP936"},

    {PG_UHC, "UHC"},
    {PG_UHC, "CP949"},

    {PG_JOHAB, "JOHAB"},
    {PG_JOHAB, "CP1361"},

    {PG_GB18030, "GB18030"},
    {PG_GB18030, "CP54936"},

    {PG_SHIFT_JIS_2004, "SJIS_2004"},

    {PG_SQL_ASCII, "US-ASCII"},

    {PG_SQL_ASCII, NULL} /* end marker */
};

#ifdef WIN32
/*
 * On Windows, use CP<code page number> instead of the nl_langinfo() result
 *
 * Visual Studio 2012 expanded the set of valid LC_CTYPE values, so have its
 * locale machinery determine the code page.  See comments at IsoLocaleName().
 * For other compilers, follow the locale's predictable format.
 *
 * Returns a malloc()'d string for the caller to free.
 */
static char* win32_langinfo(const char* ctype)
{
    char* r = NULL;

#if (_MSC_VER >= 1700)
    _locale_t loct = _create_locale(LC_CTYPE, ctype);
    if (loct != NULL) {
        r = malloc(16); /* excess */
        if (r != NULL) {
            errno_t rc = sprintf_s(r, 16, "CP%u", loct->locinfo->lc_codepage);
            securec_check_ss_c(rc, "\0", "\0");
        }
        _free_locale(loct);
    }
#else
#ifdef WIN32
    const char* codepage = NULL;
#else
    char* codepage = NULL;
#endif
    /*
     * Locale format on Win32 is <Language>_<Country>.<CodePage> . For
     * example, English_United States.1252.
     */
    codepage = strrchr(ctype, '.');
    if (codepage != NULL) {
        int ln;

        codepage++;
        ln = strlen(codepage);
#ifdef WIN32
        r = (char*)malloc(ln + 3);
#else
        r = malloc(ln + 3);
#endif
        if (r != NULL) {
            errno_t rc = sprintf_s(r, ln + 3, "CP%s", codepage);
            securec_check_ss_c(rc, "\0", "\0");
        }
    }
#endif

    return r;
}
#endif /* WIN32 */

#if (defined(HAVE_LANGINFO_H) && defined(CODESET)) || defined(WIN32)

/*
 * Given a setting for LC_CTYPE, return the openGauss ID of the associated
 * encoding, if we can determine it.  Return -1 if we can't determine it.
 *
 * Pass in NULL to get the encoding for the current locale setting.
 * Pass "" to get the encoding selected by the server's environment.
 *
 * If the result is PG_SQL_ASCII, callers should treat it as being compatible
 * with any desired encoding.
 */
int pg_get_encoding_from_locale(const char* ctype, bool write_message)
{
    char* sys = NULL;
    int i;

    /* Get the CODESET property, and also LC_CTYPE if not passed in */
    if (ctype != NULL) {
        char* save = NULL;
        char* name = NULL;

        /* If locale is C or POSIX, we can allow all encodings */
        if (pg_strcasecmp(ctype, "C") == 0 || pg_strcasecmp(ctype, "POSIX") == 0) {
            return PG_SQL_ASCII;
        }

        save = gs_setlocale_r(LC_CTYPE, NULL);
        if (save == NULL) {
            return -1; /* setlocale() broken, must copy result, or it might change after setlocale */
        }
#ifdef FRONTEND
        save = strdup(save);
#else
        save = MemoryContextStrdup(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), save);
#endif
        if (save == NULL) {
            return -1; /* out of memory; unlikely */
        }

        name = gs_setlocale_r(LC_CTYPE, ctype);
        if (name == NULL) {
#ifdef FRONTEND
            free(save);
#else
            pfree(save);
#endif
            return -1; /* bogus ctype passed in? */
        }
#ifndef WIN32
        sys = gs_nl_langinfo_r(CODESET);
        if (sys != NULL) {
#ifdef FRONTEND
            sys = strdup(sys);
#else
            sys = MemoryContextStrdup(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), sys);
#endif
        }
#else
        sys = win32_langinfo(name);
#endif

        (void)gs_setlocale_r(LC_CTYPE, save);
#ifdef FRONTEND
        free(save);
#else
        pfree(save);
#endif
    } else {
        /* much easier... */
        ctype = gs_setlocale_r(LC_CTYPE, NULL);
        if (ctype == NULL) {
            return -1; /* setlocale() broken? */
        }

        /* If locale is C or POSIX, we can allow all encodings */
        if (pg_strcasecmp(ctype, "C") == 0 || pg_strcasecmp(ctype, "POSIX") == 0) {
            return PG_SQL_ASCII;
        }
#ifndef WIN32
        sys = gs_nl_langinfo_r(CODESET);
        if (sys != NULL) {
#ifdef FRONTEND
            sys = strdup(sys);
#else
            sys = MemoryContextStrdup(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), sys);
#endif
        }
#else
        sys = win32_langinfo(ctype);
#endif
    }

    if (sys == NULL) {
        return -1; /* out of memory; unlikely */
    }

    /* Check the table */
    for (i = 0; encoding_match_list[i].system_enc_name != NULL; i++) {
        if (pg_strcasecmp(sys, encoding_match_list[i].system_enc_name) == 0) {
#ifdef FRONTEND
            free(sys);
#else
            pfree(sys);
#endif
            return encoding_match_list[i].pg_enc_code;
        }
    }

    /* Special-case kluges for particular platforms go here */
#ifdef __darwin__

    /*
     * Current OS X has many locales that report an empty string for CODESET,
     * but they all seem to actually use UTF-8.
     */
    if (strlen(sys) == 0) {
#ifdef FRONTEND
        free(sys);
#else
        pfree(sys);
#endif
        return PG_UTF8;
    }
#endif

    /*
     * We print a warning if we got a CODESET string but couldn't recognize
     * it.	This means we need another entry in the table.
     */
    if (write_message) {
#ifdef FRONTEND
        fprintf(stderr, _("could not determine encoding for locale \"%s\": codeset is \"%s\""), ctype, sys);
        /* keep newline separate so there's only one translatable string */
        fputc('\n', stderr);
#else
#if ((defined(ENABLE_MULTIPLE_NODES)) || (defined(ENABLE_PRIVATEGAUSS)))
        ereport(WARNING,
            (errmsg("could not determine encoding for locale \"%s\": codeset is \"%s\"", ctype, sys),
                errdetail("Please report this to GaussDB support.")));
#else
        ereport(WARNING,
            (errmsg("could not determine encoding for locale \"%s\": codeset is \"%s\"", ctype, sys),
                errdetail("Please report this to openGauss community by raising an issue.")));
#endif
#endif
    }
#ifdef FRONTEND
    free(sys);
#else
    pfree(sys);
#endif
    return -1;
}

#ifndef WIN32
char* gs_setlocale_r(int category, const char* locale)
{
    char* result = NULL;

#ifdef FRONTEND
    result = setlocale(category, locale);
#else

    if (!IsUnderPostmaster) {
        // query locale not modify
        result = setlocale(category, locale);
    } else {
        if (locale == NULL) {
            if (t_thrd.port_cxt.save_locale_r != (locale_t)0) {
                // get locale directly
                result = gs_nl_langinfo_r(NL_LOCALE_NAME((unsigned int)category));
            } else {
                // query locale not modify
                result = setlocale(category, NULL);
            }
        } else {
            int category_mask = 0;

            if (category == LC_ALL) {
                category_mask = LC_ALL_MASK;
            } else {
                category_mask = (1 << (unsigned int)category);
            }

            t_thrd.port_cxt.save_locale_r = newlocale(category_mask, locale, t_thrd.port_cxt.save_locale_r);
            if (t_thrd.port_cxt.save_locale_r == (locale_t)0) {
                return NULL;
            }

            locale_t last_locale = uselocale(t_thrd.port_cxt.save_locale_r);

            if (last_locale == (locale_t)0) {
                return NULL;
            }

            result = (char*)locale;
        }
    }
#endif

    return result;
}

char* gs_nl_langinfo_r(nl_item item)
{
#ifdef FRONTEND
    return nl_langinfo(item);
#else
    if (!IsUnderPostmaster) {
        return nl_langinfo(item);
    } else {
        return nl_langinfo_l(item, t_thrd.port_cxt.save_locale_r);
    }
#endif
}

#else
char* gs_setlocale_r(int category, const char* locale)
{
    return setlocale(category, locale);
}

char* gs_nl_langinfo_r(const char* ctype)
{
    return win32_langinfo(ctype);
}
#endif

#else /* (HAVE_LANGINFO_H && CODESET) || WIN32 */

/*
 * stub if no multi-language platform support
 *
 * Note: we could return -1 here, but that would have the effect of
 * forcing users to specify an encoding to initdb on such platforms.
 * It seems better to silently default to SQL_ASCII.
 */
int pg_get_encoding_from_locale(const char* ctype, bool write_message)
{
    return PG_SQL_ASCII;
}

#endif /* (HAVE_LANGINFO_H && CODESET) || WIN32 */
