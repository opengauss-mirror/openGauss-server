/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * mbutils.c
 *
 * IDENTIFICATION
 *	  src/backend/utils/mb/mbutils.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres_fe.h"
#include "datatypes.h"
#include "mb/pg_wchar.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "storage/ipc.h"
#include "fmgr.h"

static THR_LOCAL pg_enc2name *g_client_encoding = &pg_enc2name_tbl[PG_SQL_ASCII];
static THR_LOCAL pg_enc2name *g_database_encoding = &pg_enc2name_tbl[PG_SQL_ASCII];

static int cliplen(const char *str, int len, int limit);

/* convert a multibyte string to a wchar */
int pg_mb2wchar(const char *from, pg_wchar *to)
{
    return (*pg_wchar_table[g_database_encoding->encoding].mb2wchar_with_len)((const unsigned char *)from, to,
        strlen(from));
}

/* convert a multibyte string to a wchar with a limited length */
int pg_mb2wchar_with_len(const char *from, pg_wchar *to, int len)
{
    return (*pg_wchar_table[g_database_encoding->encoding].mb2wchar_with_len)((const unsigned char *)from, to, len);
}

/* same, with any encoding */
int pg_encoding_mb2wchar_with_len(int encoding, const char *from, pg_wchar *to, int len)
{
    return (*pg_wchar_table[encoding].mb2wchar_with_len)((const unsigned char *)from, to, len);
}

/* convert a wchar string to a multibyte */
int pg_wchar2mb(const pg_wchar *from, char *to)
{
    return (*pg_wchar_table[g_database_encoding->encoding].wchar2mb_with_len)(from, (unsigned char *)to,
        pg_wchar_strlen(from));
}

/* convert a wchar string to a multibyte with a limited length */
int pg_wchar2mb_with_len(const pg_wchar *from, char *to, int len)
{
    return (*pg_wchar_table[g_database_encoding->encoding].wchar2mb_with_len)(from, (unsigned char *)to, len);
}

/* same, with any encoding */
int pg_encoding_wchar2mb_with_len(int encoding, const pg_wchar *from, char *to, int len)
{
    return (*pg_wchar_table[encoding].wchar2mb_with_len)(from, (unsigned char *)to, len);
}

/* returns the byte length of a multibyte character */
int pg_mblen(const char *mbstr)
{
    return ((*pg_wchar_table[g_database_encoding->encoding].mblen)((const unsigned char *)mbstr));
}

/* returns the display length of a multibyte character */
int pg_dsplen(const char *mbstr)
{
    return ((*pg_wchar_table[g_database_encoding->encoding].dsplen)((const unsigned char *)mbstr));
}

/* returns the length (counted in wchars) of a multibyte string */
int pg_mbstrlen(const char *mbstr)
{
    int len = 0;

    /* optimization for single byte encoding */
    if (pg_database_encoding_max_length() == 1) {
        return strlen(mbstr);
    }

    while (*mbstr) {
        mbstr += pg_mblen(mbstr);
        len++;
    }
    return len;
}

/* 
 * returns the length (counted in wchars) of a multibyte string
 * (not necessarily NULL terminated)
 */
int pg_mbstrlen_with_len(const char *mbstr, int limit)
{
    int len = 0;

    /* optimization for single byte encoding */
    if (pg_database_encoding_max_length() == 1) {
        return limit;
    }

    while (limit > 0 && *mbstr) {
        int l = pg_mblen(mbstr);

        limit -= l;
        mbstr += l;
        len++;
    }
    return len;
}

/* 
 * returns the length (counted in wchars) of a multibyte string
 * with fixed encoding.
 */
int pg_mbstrlen_with_len_eml(const char *mbstr, int limit, int eml)
{
    int len = 0;

    /* optimization for single byte encoding */
    if (eml == 1) {
        return limit;
    }

    while (limit > 0 && *mbstr) {
        int l = pg_mblen(mbstr);

        limit -= l;
        mbstr += l;
        len++;
    }
    return len;
}

/*
 * returns the byte length of a multibyte string
 * (not necessarily NULL terminated)
 * that is no longer than limit.
 * this function does not break multibyte character boundary.
 */
int pg_mbcliplen(const char *mbstr, int len, int limit)
{
    return pg_encoding_mbcliplen(g_database_encoding->encoding, mbstr, len, limit);
}

/*
 * pg_mbcliplen with specified encoding
 */
int pg_encoding_mbcliplen(int encoding, const char *mbstr, int len, int limit)
{
    mblen_converter mblen_fn;
    int clen = 0;
    int l;

    /* optimization for single byte encoding */
    if (pg_encoding_max_length(encoding) == 1) {
        return cliplen(mbstr, len, limit);
    }

    mblen_fn = pg_wchar_table[encoding].mblen;

    while (len > 0 && *mbstr) {
        l = (*mblen_fn)((const unsigned char *)mbstr);
        if ((clen + l) > limit) {
            break;
        }
        clen += l;
        if (clen == limit) {
            break;
        }
        len -= l;
        mbstr += l;
    }
    return clen;
}

/*
 * Similar to pg_mbcliplen except the limit parameter specifies the
 * byte length, not the character length.
 */
int pg_mbcharcliplen(const char *mbstr, int len, int limit)
{
    int clen = 0;
    int nch = 0;
    int l;

    /* optimization for single byte encoding */
    if (pg_database_encoding_max_length() == 1) {
        return cliplen(mbstr, len, limit);
    }

    while (len > 0 && *mbstr) {
        l = pg_mblen(mbstr);
        nch += l;
        if (nch > limit) {
            break;
        }
        clen += l;
        len -= l;
        mbstr += l;
    }
    return clen;
}
/*
 * Description	: Similar to pg_mbcliplen except the limit parameter specifies
 * 				  the character length, not the byte length.
 */
int pg_mbcharcliplen_orig(const char *mbstr, int len, int limit)
{
    int clen = 0;
    int nch = 0;
    int l;

    /* optimization for single byte encoding */
    if (pg_database_encoding_max_length() == 1) {
        return cliplen(mbstr, len, limit);
    }

    while (len > 0 && *mbstr) {
        l = pg_mblen(mbstr);
        nch++;
        if (nch > limit) {
            break;
        }
        clen += l;
        len -= l;
        mbstr += l;
    }
    return clen;
}

/* mbcliplen for any single-byte encoding */
static int cliplen(const char *str, int len, int limit)
{
    int l = 0;

    len = Min(len, limit);
    while (l < len && str[l]) {
        l++;
    }
    return l;
}

void SetDatabaseEncoding(int encoding)
{
    if (!PG_VALID_BE_ENCODING(encoding)) {
        printf("invalid database encoding: %d\n", encoding);
    }
    g_database_encoding = &pg_enc2name_tbl[encoding];
    Assert(g_database_encoding->encoding == encoding);
}

/*
 * Bind gettext to the codeset equivalent with the database encoding.
 */
void pg_bind_textdomain_codeset(const char *domainname)
{
#if defined(ENABLE_NLS)
    int encoding = GetDatabaseEncoding();
    int i;

    /*
     * gettext() uses the codeset specified by LC_CTYPE by default, so if that
     * matches the database encoding we don't need to do anything. In CREATE
     * DATABASE, we enforce or trust that the locale's codeset matches
     * database encoding, except for the C locale. In C locale, we bind
     * gettext() explicitly to the right codeset.
     *
     * On Windows, though, gettext() tends to get confused so we always bind
     * it.
     */
#ifndef WIN32
    /* setlocale is thread-unsafe */
    AutoMutexLock localeLock(&gLocaleMutex);
    localeLock.lock();
    const char *ctype = gs_setlocale_r(LC_CTYPE, NULL);

    if (pg_strcasecmp(ctype, "C") != 0 && pg_strcasecmp(ctype, "POSIX") != 0) {
        return;
    }

    localeLock.unLock();
#endif

    for (i = 0; pg_enc2gettext_tbl[i].name != NULL; i++) {
        if (pg_enc2gettext_tbl[i].encoding == encoding) {
            if (bind_textdomain_codeset(domainname, pg_enc2gettext_tbl[i].name) == NULL) {
                ereport(LOG, (errmsg("bind_textdomain_codeset failed")));
            }
            break;
        }
    }
#endif
}

int GetDatabaseEncoding(void)
{
    Assert(g_database_encoding);
    return g_database_encoding->encoding;
}

const char *GetDatabaseEncodingName(void)
{
    Assert(g_database_encoding);
    return g_database_encoding->name;
}

Datum getdatabaseencoding(PG_FUNCTION_ARGS)
{
    Assert(g_database_encoding);
    return 0;
}


Datum pg_client_encoding(PG_FUNCTION_ARGS)
{
    Assert(g_client_encoding);
    return 0;
}

/*
 * returns the current client encoding
 */
int pg_get_client_encoding(void)
{
    Assert(g_client_encoding);
    return g_client_encoding->encoding;
}

/*
 * returns the current client encoding name
 */
const char *pg_get_client_encoding_name(void)
{
    Assert(g_client_encoding);
    return g_client_encoding->name;
}
#ifdef WIN32

/*
 * Result is malloc'ed null-terminated utf16 string. The character length
 * is also passed to utf16len if not null. Returns NULL iff failed.
 */
WCHAR *pgwin32_toUTF16(const char *str, int len, int *utf16len)
{
    WCHAR *utf16 = NULL;
    int dstlen;
    UINT codepage;

    codepage = pg_enc2name_tbl[GetDatabaseEncoding()].codepage;
    /*
     * Use MultiByteToWideChar directly if there is a corresponding codepage,
     * or double conversion through UTF8 if not.
     */
    if (codepage != 0) {
        utf16 = (WCHAR *)feparser_malloc(sizeof(WCHAR) * (len + 1));
        dstlen = MultiByteToWideChar(codepage, 0, str, len, utf16, len);
        utf16[dstlen] = (WCHAR)0;
    } else {
        char *utf8 = NULL;

        utf8 = (char *)pg_do_encoding_conversion((unsigned char *)str, len, GetDatabaseEncoding(), PG_UTF8);
        if (utf8 != str)
            len = strlen(utf8);

        utf16 = (WCHAR *)feparser_malloc(sizeof(WCHAR) * (len + 1));
        dstlen = MultiByteToWideChar(CP_UTF8, 0, utf8, len, utf16, len);
        utf16[dstlen] = (WCHAR)0;

        if (utf8 != str)
            feparser_free(utf8);
    }

    if (dstlen == 0 && len > 0) {
        feparser_free(utf16);
        return NULL; /* error */
    }

    if (utf16len != NULL) {
        *utf16len = dstlen;
    }
    return utf16;
}

#endif
