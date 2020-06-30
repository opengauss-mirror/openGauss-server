
/* -------------------------------------------------------------------------
 *
 *	  Utility functions for conversion procs.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/utils/mb/conv.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "mb/pg_wchar.h"

/*
 * LATINn ---> MIC when the charset's local codes map directly to MIC
 *
 * l points to the source string of length len
 * p is the output area (must be large enough!)
 * lc is the mule character set id for the local encoding
 * encoding is the PG identifier for the local encoding
 */
void latin2mic(const unsigned char* l, unsigned char* p, int len, int lc, int encoding)
{
    int c1;

    while (len > 0) {
        c1 = *l;
        if (c1 == 0) {
            report_invalid_encoding(encoding, (const char*)l, len);
        }
        if (IS_HIGHBIT_SET(c1)) {
            *p++ = lc;
        }
        *p++ = c1;
        l++;
        len--;
    }
    *p = '\0';
}

/*
 * MIC ---> LATINn when the charset's local codes map directly to MIC
 *
 * mic points to the source string of length len
 * p is the output area (must be large enough!)
 * lc is the mule character set id for the local encoding
 * encoding is the PG identifier for the local encoding
 */
void mic2latin(const unsigned char* mic, unsigned char* p, int len, int lc, int encoding)
{
    int c1;
    bool bulkload_illegal_chars_conversion = false;
    int l = 0;
    mbverifier mbverify;

    if (u_sess->cmd_cxt.bulkload_compatible_illegal_chars) {
        bulkload_illegal_chars_conversion = true;
    }

    while (len > 0) {
        c1 = *mic;
        if (c1 == 0) {
            if (bulkload_illegal_chars_conversion) {
                /*
                 * use ' ' as conversion.
                 */
                *p++ = ' ';
                mic++;
                len--;
                continue;
            } else {
                report_invalid_encoding(PG_MULE_INTERNAL, (const char*)mic, len);
            }
        }
        if (!IS_HIGHBIT_SET(c1)) {
            /* easy for ASCII */
            *p++ = c1;
            mic++;
            len--;
        } else {
            l = pg_mic_mblen(mic);
            if (len < l) {
                if (bulkload_illegal_chars_conversion) {
                    /*
                     * use '?' as conversion.
                     */
                    *p++ = '?';
                    mic++;
                    len--;
                    continue;
                } else {
                    report_invalid_encoding(PG_MULE_INTERNAL, (const char*)mic, len);
                }
            }
            if (l != 2 || c1 != lc || !IS_HIGHBIT_SET(mic[1])) {
                if (bulkload_illegal_chars_conversion) {
                    /*
                     * Here we need call  pg_mule_verifier() to be sure whether the current mic byte sequence is valid
                     * not not. If it's invalid, p to be placed a '?' and mic go to the next byte for next check. If
                     * it's valid, p to be placed a '?' and mic can safely skip l bytes for next check.
                     * fetch function pointer just once */
                    mbverify = pg_wchar_table[PG_MULE_INTERNAL].mbverify;

                    if ((*mbverify)(mic, len) < 0) {
                        /*
                         * use '?' as conversion.
                         */
                        *p++ = '?';
                        mic++;
                        len--;
                        continue;
                    } else {
                        /*
                         * use '?' as conversion.
                         */
                        *p++ = '?';
                        mic += l;
                        len -= l;
                        continue;
                    }
                } else {
                    report_untranslatable_char(PG_MULE_INTERNAL, PG_LATIN1, (const char*)mic, len);
                }
            }
            *p++ = mic[1];
            mic += 2;
            len -= 2;
        }
    }
    *p = '\0';
}

/*
 * ASCII ---> MIC
 *
 * While ordinarily SQL_ASCII encoding is forgiving of high-bit-set
 * characters, here we must take a hard line because we don't know
 * the appropriate MIC equivalent.
 */
void pg_ascii2mic(const unsigned char* l, unsigned char* p, int len)
{
    int c1;

    while (len > 0) {
        c1 = *l;
        if (c1 == 0 || IS_HIGHBIT_SET(c1))
            report_invalid_encoding(PG_SQL_ASCII, (const char*)l, len);
        *p++ = c1;
        l++;
        len--;
    }
    *p = '\0';
}

/*
 * MIC ---> ASCII
 */
void pg_mic2ascii(const unsigned char* mic, unsigned char* p, int len)
{
    int c1;

    while (len > 0) {
        c1 = *mic;
        if (c1 == 0 || IS_HIGHBIT_SET(c1))
            report_untranslatable_char(PG_MULE_INTERNAL, PG_SQL_ASCII, (const char*)mic, len);
        *p++ = c1;
        mic++;
        len--;
    }
    *p = '\0';
}

/*
 * latin2mic_with_table: a generic single byte charset encoding
 * conversion from a local charset to the mule internal code.
 *
 * l points to the source string of length len
 * p is the output area (must be large enough!)
 * lc is the mule character set id for the local encoding
 * encoding is the PG identifier for the local encoding
 * tab holds conversion entries for the local charset
 * starting from 128 (0x80). each entry in the table
 * holds the corresponding code point for the mule internal code.
 */
void latin2mic_with_table(
    const unsigned char* l, unsigned char* p, int len, int lc, int encoding, const unsigned char* tab)
{
    unsigned char c1, c2;

    while (len > 0) {
        c1 = *l;
        if (c1 == 0) {
            report_invalid_encoding(encoding, (const char*)l, len);
        }
        if (!IS_HIGHBIT_SET(c1)) {
            *p++ = c1;
        } else {
            c2 = tab[c1 - HIGHBIT];
            if (c2) {
                *p++ = lc;
                *p++ = c2;
            } else {
                report_untranslatable_char(encoding, PG_MULE_INTERNAL, (const char*)l, len);
            }
        }
        l++;
        len--;
    }
    *p = '\0';
}

/*
 * mic2latin_with_table: a generic single byte charset encoding
 * conversion from the mule internal code to a local charset.
 *
 * mic points to the source string of length len
 * p is the output area (must be large enough!)
 * lc is the mule character set id for the local encoding
 * encoding is the PG identifier for the local encoding
 * tab holds conversion entries for the mule internal code's
 * second byte, starting from 128 (0x80). each entry in the table
 * holds the corresponding code point for the local charset.
 */
void mic2latin_with_table(
    const unsigned char* mic, unsigned char* p, int len, int lc, int encoding, const unsigned char* tab)
{
    unsigned char c1, c2;

    while (len > 0) {
        c1 = *mic;
        if (c1 == 0) {
            report_invalid_encoding(PG_MULE_INTERNAL, (const char*)mic, len);
        }
        if (!IS_HIGHBIT_SET(c1)) {
            /* easy for ASCII */
            *p++ = c1;
            mic++;
            len--;
        } else {
            int l = pg_mic_mblen(mic);
            if (len < l) {
                report_invalid_encoding(PG_MULE_INTERNAL, (const char*)mic, len);
            }
            if (l != 2 || c1 != lc || !IS_HIGHBIT_SET(mic[1]) || (c2 = tab[mic[1] - HIGHBIT]) == 0) {
                report_untranslatable_char(PG_MULE_INTERNAL, encoding, (const char*)mic, len);
                break; /* keep compiler quiet */
            }
            *p++ = c2;
            mic += 2;
            len -= 2;
        }
    }
    *p = '\0';
}

/*
 * comparison routine for bsearch()
 * this routine is intended for UTF8 -> local code
 */
static int compare1(const void* p1, const void* p2)
{
    uint32 v1, v2;

    v1 = *(const uint32*)p1;
    v2 = ((const pg_utf_to_local*)p2)->utf;
    return (v1 > v2) ? 1 : ((v1 == v2) ? 0 : -1);
}

/*
 * comparison routine for bsearch()
 * this routine is intended for local code -> UTF8
 */
static int compare2(const void* p1, const void* p2)
{
    uint32 v1, v2;

    v1 = *(const uint32*)p1;
    v2 = ((const pg_local_to_utf*)p2)->code;
    return (v1 > v2) ? 1 : ((v1 == v2) ? 0 : -1);
}

/*
 * comparison routine for bsearch()
 * this routine is intended for combined UTF8 -> local code
 */
static int compare3(const void* p1, const void* p2)
{
    uint32 s1, s2, d1, d2;

    s1 = *(const uint32*)p1;
    s2 = *((const uint32*)p1 + 1);
    d1 = ((const pg_utf_to_local_combined*)p2)->utf1;
    d2 = ((const pg_utf_to_local_combined*)p2)->utf2;
    return (s1 > d1 || (s1 == d1 && s2 > d2)) ? 1 : ((s1 == d1 && s2 == d2) ? 0 : -1);
}

/*
 * comparison routine for bsearch()
 * this routine is intended for local code -> combined UTF8
 */
static int compare4(const void* p1, const void* p2)
{
    uint32 v1, v2;

    v1 = *(const uint32*)p1;
    v2 = ((const pg_local_to_utf_combined*)p2)->code;
    return (v1 > v2) ? 1 : ((v1 == v2) ? 0 : -1);
}

/*
 * convert 32bit wide character to mutibye stream pointed to by iso
 */
static unsigned char *set_iso_code(unsigned char* iso, uint32 code)
{
    if (code & 0xff000000) {
        *iso++ = code >> 24;
    }
    if (code & 0x00ff0000) {
        *iso++ = (code & 0x00ff0000) >> 16;
    }
    if (code & 0x0000ff00) {
        *iso++ = (code & 0x0000ff00) >> 8;
    }
    if (code & 0x000000ff) {
        *iso++ = code & 0x000000ff;
    }
    return iso;
}

/*
 * UTF8 ---> local code
 *
 * utf: input UTF8 string (need not be null-terminated).
 * iso: pointer to the output area (must be large enough!)
 * map: the conversion map.
 * cmap: the conversion map for combined characters.
 *		  (optional)
 * size1: the size of the conversion map.
 * size2: the size of the conversion map for combined characters
 *		  (optional)
 * encoding: the PG identifier for the local encoding.
 * len: length of input string.
 */
void UtfToLocal(const unsigned char* utf, unsigned char* iso, const pg_utf_to_local* map,
    const pg_utf_to_local_combined* cmap, int size1, int size2, int encoding, int len)
{
    uint32 iutf;
    uint32 cutf[2];
    uint32 code = 0;
    pg_utf_to_local* p = NULL;
    pg_utf_to_local_combined* cp = NULL;
    int l;
    bool bulkload_illegal_chars_conversion = false;

    if (u_sess->cmd_cxt.bulkload_compatible_illegal_chars) {
        bulkload_illegal_chars_conversion = true;
    }

    for (; len > 0; len -= l) {
        /* "break" cases all represent errors */
        if (*utf == '\0') {
            if (bulkload_illegal_chars_conversion) {
                /*
                 * use ' ' as conversion.
                 */
                *iso++ = ' ';
                utf++;
                l = 1;
                continue;
            } else {
                break;
            }
        }

        l = pg_utf_mblen(utf);
        if ((len < l) || (!pg_utf8_islegal(utf, l))) {
            if (bulkload_illegal_chars_conversion) {
                /*
                 * use '?' as conversion.
                 */
                *iso++ = '?';
                utf++;
                l = 1;
                continue;
            } else {
                break;
            }
        }

        if (l == 1) {
            /* ASCII case is easy */
            *iso++ = *utf++;
            continue;
        } else if (l == 2) {
            iutf = *utf++ << 8;
            iutf |= *utf++;
        } else if (l == 3) {
            iutf = *utf++ << 16;
            iutf |= *utf++ << 8;
            iutf |= *utf++;
        } else if (l == 4) {
            iutf = *utf++ << 24;
            iutf |= *utf++ << 16;
            iutf |= *utf++ << 8;
            iutf |= *utf++;
        }

        /*
         * first, try with combined map if possible
         */
        if (cmap != NULL && len > l) {
            const unsigned char* utf_save = utf;
            int len_save = len;
            int l_save = l;

            len -= l;

            l = pg_utf_mblen(utf);
            if ((len < l) || (!pg_utf8_islegal(utf, l))) {
                if (bulkload_illegal_chars_conversion) {
                    /*
                     * use '?' as conversion.
                     */
                    *iso++ = '?';
                    utf++;
                    l = 1;
                    continue;
                } else {
                    break;
                }
            }

            cutf[0] = iutf;

            if (l == 1) {
                if (len_save > 1) {
                    p = (pg_utf_to_local*)bsearch(&cutf[0], map, size1, sizeof(pg_utf_to_local), compare1);
                    if (p == NULL) {
                        if (bulkload_illegal_chars_conversion) {
                            /*
                             * use '?' as conversion.
                             */
                            *iso++ = '?';
                        } else {
                            report_untranslatable_char(PG_UTF8, encoding, (const char*)(utf_save - l_save), len_save);
                        }
                    } else {
                        iso = set_iso_code(iso, p->code);
                    }
                }

                /* ASCII case is easy */
                *iso++ = *utf++;
                continue;
            } else if (l == 2) {
                iutf = *utf++ << 8;
                iutf |= *utf++;
            } else if (l == 3) {
                iutf = *utf++ << 16;
                iutf |= *utf++ << 8;
                iutf |= *utf++;
            } else if (l == 4) {
                iutf = *utf++ << 24;
                iutf |= *utf++ << 16;
                iutf |= *utf++ << 8;
                iutf |= *utf++;
            }

            cutf[1] = iutf;
            cp = (pg_utf_to_local_combined*)bsearch(cutf, cmap, size2, sizeof(pg_utf_to_local_combined), compare3);
            if (cp != NULL) {
                code = cp->code;
            } else {
                /* not found in combined map. try with ordinary map */
                p = (pg_utf_to_local*)bsearch(&cutf[0], map, size1, sizeof(pg_utf_to_local), compare1);
                if (p == NULL) {
                    if (bulkload_illegal_chars_conversion) {
                        /*
                         * the first map is converted to '?', and continue the next map.
                         */
                        *iso++ = '?';
                    } else {
                        report_untranslatable_char(PG_UTF8, encoding, (const char*)(utf_save - l_save), len_save);
                    }
                } else {
                    iso = set_iso_code(iso, p->code);
                }

                p = (pg_utf_to_local*)bsearch(&cutf[1], map, size1, sizeof(pg_utf_to_local), compare1);
                if (p == NULL) {
                    if (bulkload_illegal_chars_conversion) {
                        /*
                         * the second map is converted to '?', all ordinary map check is done.
                         */
                        *iso++ = '?';
                        continue;
                    } else {
                        report_untranslatable_char(PG_UTF8, encoding, (const char*)(utf - l), len);
                    }
                } else {
                    code = p->code;
                    /* FALL THRU */
                }
            }
        } else {
            /* no cmap or no remaining data */
            p = (pg_utf_to_local*)bsearch(&iutf, map, size1, sizeof(pg_utf_to_local), compare1);
            if (p == NULL) {
                if (bulkload_illegal_chars_conversion) {
                    /*
                     * use '?' as conversion.
                     */
                    *iso++ = '?';
                    continue;
                } else {
                    report_untranslatable_char(PG_UTF8, encoding, (const char*)(utf - l), len);
                }
            } else {
                code = p->code;
                /* FALL THRU */
            }
        }
        iso = set_iso_code(iso, code);
    }

    if (len > 0) {
        report_invalid_encoding(PG_UTF8, (const char*)utf, len);
    }
    *iso = '\0';
}

/*
 * local code ---> UTF8
 *
 * iso: input local string (need not be null-terminated).
 * utf: pointer to the output area (must be large enough!)
 * map: the conversion map.
 * cmap: the conversion map for combined characters.
 *		  (optional)
 * size1: the size of the conversion map.
 * size2: the size of the conversion map for combined characters
 *		  (optional)
 * encoding: the PG identifier for the local encoding.
 * len: length of input string.
 */
void LocalToUtf(const unsigned char* iso, unsigned char* utf, const pg_local_to_utf* map,
    const pg_local_to_utf_combined* cmap, int size1, int size2, int encoding, int len)
{
    unsigned int iiso;
    int l;
    pg_local_to_utf* p = NULL;
    pg_local_to_utf_combined* cp = NULL;
    bool bulkload_illegal_chars_conversion = false;

    if (u_sess->cmd_cxt.bulkload_compatible_illegal_chars) {
        bulkload_illegal_chars_conversion = true;
    }

    if (!PG_VALID_ENCODING(encoding)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid encoding number: %d", encoding)));
    }
    for (; len > 0; len -= l) {
        /* "break" cases all represent errors */
        if (*iso == '\0') {
            if (bulkload_illegal_chars_conversion) {
                /*
                 * use ' ' as conversion.
                 */
                *utf++ = ' ';
                iso++;
                l = 1;
                continue;
            } else {
                break;
            }
        }

        if (!IS_HIGHBIT_SET(*iso)) {
            /* ASCII case is easy */
            *utf++ = *iso++;
            l = 1;
            continue;
        }

        l = pg_encoding_verifymb(encoding, (const char*)iso, len);
        if (l < 0) {
            if (bulkload_illegal_chars_conversion) {
                /*
                 * use '?' as conversion.
                 */
                *utf++ = '?';
                iso++;
                l = 1;
                continue;
            } else {
                break;
            }
        }

        if (l == 1) {
            iiso = *iso++;
        } else if (l == 2) {
            iiso = *iso++ << 8;
            iiso |= *iso++;
        } else if (l == 3) {
            iiso = *iso++ << 16;
            iiso |= *iso++ << 8;
            iiso |= *iso++;
        } else if (l == 4) {
            iiso = *iso++ << 24;
            iiso |= *iso++ << 16;
            iiso |= *iso++ << 8;
            iiso |= *iso++;
        }

        p = (pg_local_to_utf*)bsearch(&iiso, map, size1, sizeof(pg_local_to_utf), compare2);
        if (p == NULL) {
            /*
             * not found in the ordinary map. if there's a combined character
             * map, try with it
             */
            if (cmap != NULL) {
                cp = (pg_local_to_utf_combined*)bsearch(&iiso, cmap, size2, sizeof(pg_local_to_utf_combined), compare4);
                if (cp != NULL) {
                    if (cp->utf1 & 0xff000000)
                        *utf++ = cp->utf1 >> 24;
                    if (cp->utf1 & 0x00ff0000)
                        *utf++ = (cp->utf1 & 0x00ff0000) >> 16;
                    if (cp->utf1 & 0x0000ff00)
                        *utf++ = (cp->utf1 & 0x0000ff00) >> 8;
                    if (cp->utf1 & 0x000000ff)
                        *utf++ = cp->utf1 & 0x000000ff;

                    if (cp->utf2 & 0xff000000)
                        *utf++ = cp->utf2 >> 24;
                    if (cp->utf2 & 0x00ff0000)
                        *utf++ = (cp->utf2 & 0x00ff0000) >> 16;
                    if (cp->utf2 & 0x0000ff00)
                        *utf++ = (cp->utf2 & 0x0000ff00) >> 8;
                    if (cp->utf2 & 0x000000ff)
                        *utf++ = cp->utf2 & 0x000000ff;

                    continue;
                }
            }

            if (!bulkload_illegal_chars_conversion) {
                report_untranslatable_char(encoding, PG_UTF8, (const char*)(iso - l), len);
            }

            // replace untranslatable char with '?'
            if (u_sess->attr.attr_common.omit_encoding_error || bulkload_illegal_chars_conversion) {
                *utf++ = '?';
                continue;
            }
        } else {
            if (p->utf & 0xff000000)
                *utf++ = p->utf >> 24;
            if (p->utf & 0x00ff0000)
                *utf++ = (p->utf & 0x00ff0000) >> 16;
            if (p->utf & 0x0000ff00)
                *utf++ = (p->utf & 0x0000ff00) >> 8;
            if (p->utf & 0x000000ff)
                *utf++ = p->utf & 0x000000ff;
        }
    }

    if (len > 0) {
        report_invalid_encoding(encoding, (const char*)iso, len);
    }
    *utf = '\0';
}
