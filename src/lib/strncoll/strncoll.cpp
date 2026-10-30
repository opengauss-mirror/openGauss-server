/*
 * strncoll.cpp - Locale-aware string comparison bounded by explicit byte
 *                lengths.
 *
 * This file is distributed under the GNU Lesser General Public License
 * v2.1 (or later) because it contains code adapted from the GNU C Library
 * (glibc), which is LGPL-licensed.  It is compiled into a standalone shared
 * library (libstrncoll.so) that is dynamically linked by the server, so end
 * users can replace or relink it independently, as required by the LGPL.
 *
 * Copyright (C) 1995-2023 Free Software Foundation, Inc.
 *   (original glibc string/strcoll_l.c and locale/weight.h)
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2026. All rights reserved.
 *   (adaptations: length-bounded scan, glibc 2.28-2.42 private-offset
 *    handling, ASCII-digit fast path with per-locale safety check)
 *
 * The GNU C Library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <https://www.gnu.org/licenses/>.
 */

/*
 * This translation unit is intentionally self-contained: it depends only on
 * the C/C++ standard library and glibc, so it can be compiled into a
 * standalone shared library (libstrncoll.so) without the openGauss server
 * headers.  The server calls strncoll() through the declaration in
 * utils/pg_locale.h, which matches the signature below.
 */
#include "utils/strncoll_glibc.h"

#include <cstring>
#include <cstdlib>

/* pg_locale_t is locale_t when HAVE_LOCALE_T is defined; otherwise strncoll()
 * is not compiled at all (guarded by the #if below). */
#include <locale.h>

#ifdef HAVE_LOCALE_T
typedef locale_t pg_locale_t;
#endif

#ifndef Min
#define Min(a, b) ((a) < (b) ? (a) : (b))
#endif

#if defined(HAVE_LOCALE_T) && defined(__GLIBC__) && !defined(WIN32)

#include <langinfo.h>
#include <limits.h>
#include <stdint.h>
#include <locale.h>
#include <sys/types.h>
#include <gnu/libc-version.h>

/*
 * The libc implementation of strncoll() below is adapted from glibc 2.38
 * (openEuler glibc-2.38-107.oe2403sp3), files string/strcoll_l.c and
 * locale/weight.h.  The original code scans until a NUL byte; here the scan
 * is bounded by an end pointer and findidx() receives the remaining byte
 * count instead of -1, which removes the need to copy the inputs into a
 * NUL-terminated temporary buffer before comparison.
 *
 * The layout of the internal locale data (__locale_data, weight tables) is
 * glibc-version specific and is reproduced only as far as it is read here.
 * It is known to be byte-identical from glibc 2.12 through 2.42, but this is
 * a private ABI with no stability guarantee, so strncoll() must only be used
 * when the running glibc is in the supported range.  That decision is made by
 * optimize_varstr_cmp(), which routes to this implementation through the
 * locale comparator variants that end in _n.
 */

/* A byte string comparison that stops at a length bound. */
static int bounded_strcmp(const char *s1, int len1, const char *s2, int len2)
{
    int cmp = memcmp(s1, s2, Min(len1, len2));
    if (cmp != 0) {
        return cmp;
    }
    if (len1 == len2) {
        return 0;
    }
    return (len1 < len2) ? -1 : 1;
}

/*
 * Return the number of leading bytes before an embedded NUL byte, or the
 * full length if there is none.  strcoll() semantics stop at NUL, so the
 * bounded comparison has to stop there as well.
 */
static int bounded_strlen(const char *str, int len)
{
    const char *nul = NULL;

    if (len < 0) {
        return (int)strlen(str);
    }
    if (len == 0) {
        return 0;
    }
    nul = (const char *)memchr(str, '\0', (size_t)len);
    return (nul != NULL) ? (int)(nul - str) : len;
}

/* Alignment used for 32-bit integers in locale files. */
#define LOCFILE_ALIGN sizeof(int32_t)
#define LOCFILE_ALIGN_MASK (LOCFILE_ALIGN - 1)

/*
 * Each entry of glibc's extra table stores the collating element's bytes
 * twice (once for each direction of the range scan), so an element whose
 * length is nhere occupies COLL_EXTRA_STR_COPIES * nhere bytes.
 */
#define COLL_EXTRA_STR_COPIES 2

/* True when a glibc locale-file structure is aligned to int32_t. */
static inline bool locfile_aligned_p(size_t x)
{
    return (x & LOCFILE_ALIGN_MASK) == 0;
}

/* Kinds of collation sorting rules, as in glibc's locale/localeinfo.h. */
enum {
    illegal_0__,
    sort_forward,
    sort_backward,
    illegal_3__,
    sort_position,
    sort_forward_position,
    sort_backward_position,
    sort_mask
};

/* Access to the per-category tables stored in a glibc locale object. */
struct __locale_data {
    const char *name;
    const char *filedata;
    off_t filesize;
    int alloc;
    /* glibc 2.28-2.35: private is a struct with cleanup fn ptr + data ptr (16 bytes).
     * glibc 2.36+:     private is a single void* (8 bytes).
     * We define the larger (2.28) layout here and adjust the values pointer
     * at runtime via glibc_private_offset(). */
    struct {
        void (*cleanup)(struct __locale_data *);
        union {
            void *data;
        } data;
    } private_data;
    unsigned int usage_count;
    int use_translit;
    unsigned int nstrings;
    union {
        const uint32_t *wstr;
        const char *string;
        unsigned int word;
    } values[1];
};

/*
 * glibc_private_offset()
 *   Returns the byte offset to subtract from a __locale_data* before
 *   accessing ->values[], to account for the private field size difference.
 *   glibc 2.28-2.35: private = 16 bytes, struct matches, offset = 0.
 *   glibc 2.36+:     private = 8 bytes, struct is 8 too big, offset = 8.
 */
static int glibc_private_offset(void)
{
    /* glibc 2.36+ shrank the private field from 16 to 8 bytes.
     * Cached once; __LC_VERSION is a compile-time constant but the
     * runtime version is what matters.  Avoid sscanf on every call:
     * gnu_get_libc_version returns a process-lifetime string, so we
     * cache the parsed result in a static. */
    static int offset = -1;
    if (offset < 0) {
        const char *v = gnu_get_libc_version();
        /* v is "2.xx" – check v[2] directly to avoid sscanf overhead */
        if (v != NULL && v[0] == '2' && v[1] == '.') {
            int minor = atoi(v + 2);
            offset = (minor >= STRNCOLL_GLIBC_PRIVATE_SHRINK_MINOR) ? STRNCOLL_GLIBC_PRIVATE_OFFSET_NEW : 0;
        } else {
            offset = 0;
        }
    }
    return offset;
}

/*
 * locale_data_values()
 *   Returns a pointer to the values[] array of a glibc __locale_data,
 *   adjusted for the running glibc's private field size.
 */
static inline const void *locale_data_values(const struct __locale_data *current)
{
    return (const void *)((const char *)current + glibc_private_offset());
}

/*
 * Find the weight index for the next collating element.  Adapted from
 * glibc's locale/weight.h; 'len' is the number of bytes still available in
 * the input rather than the original unlimited -1.
 */
static int32_t findidx_bounded(const int32_t *table, const int32_t *indirect, const unsigned char *extra,
                                      const unsigned char **cpp, size_t len)
{
    int32_t i = table[*(*cpp)++];
    const unsigned char *cp = NULL;
    const unsigned char *usrc = NULL;

    if (i >= 0) {
        return i;
    }

    cp = &extra[-i];
    usrc = *cpp;
    --len;
    while (1) {
        size_t nhere = 0;

        i = *((const int32_t *)cp);
        cp += sizeof(int32_t);
        nhere = *cp++;

        if (i >= 0) {
            size_t cnt = 0;

            for (cnt = 0; cnt < nhere && cnt < len; ++cnt) {
                if (cp[cnt] != usrc[cnt]) {
                    break;
                }
            }

            if (cnt == nhere) {
                *cpp += nhere;
                return i;
            }

            cp += nhere;
            if (!locfile_aligned_p(1 + nhere)) {
                cp += LOCFILE_ALIGN - (1 + nhere) % LOCFILE_ALIGN;
            }
        } else {
            size_t cnt = 0;
            size_t offset = 0;

            for (cnt = 0; cnt < nhere && cnt < len; ++cnt) {
                if (cp[cnt] != usrc[cnt]) {
                    break;
                }
            }

            if (cnt != nhere) {
                if (cnt == len || cp[cnt] > usrc[cnt]) {
                    cp += COLL_EXTRA_STR_COPIES * nhere;
                    if (!locfile_aligned_p(1 + COLL_EXTRA_STR_COPIES * nhere)) {
                        cp += LOCFILE_ALIGN - (1 + COLL_EXTRA_STR_COPIES * nhere) % LOCFILE_ALIGN;
                    }
                    continue;
                }

                for (cnt = 0; cnt < nhere; ++cnt) {
                    if (cp[nhere + cnt] != usrc[cnt]) {
                        break;
                    }
                }

                if (cnt != nhere && cp[nhere + cnt] < usrc[cnt]) {
                    cp += COLL_EXTRA_STR_COPIES * nhere;
                    if (!locfile_aligned_p(1 + COLL_EXTRA_STR_COPIES * nhere)) {
                        cp += LOCFILE_ALIGN - (1 + COLL_EXTRA_STR_COPIES * nhere) % LOCFILE_ALIGN;
                    }
                    continue;
                }

                for (cnt = 0; cp[cnt] == usrc[cnt]; ++cnt) {
                    /* find the offset within the range */
                }

                do {
                    offset <<= CHAR_BIT;
                    offset += usrc[cnt] - cp[cnt];
                } while (++cnt < nhere);
            }

            *cpp += nhere;
            return indirect[-i + offset];
        }
    }

    /* NOTREACHED */
    return 0x43219876;
}

/* Track status while looking for sequences in a string. */
typedef struct {
    int len;
    size_t val;
    size_t idxmax;
    size_t idxcnt;
    size_t backw;
    size_t backw_stop;
    const unsigned char *us;
    unsigned char rule;
    int32_t idx;
    int32_t save_idx;
    const unsigned char *back_us;
} coll_seq;

/* Get the next sequence, traversing the string as required. */
static void get_next_seq(coll_seq *seq, int nrules, const unsigned char *rulesets,
                                const unsigned char *weights, const int32_t *table, const unsigned char *extra,
                                const int32_t *indirect, int pass, const unsigned char *end)
{
    size_t val = seq->val = 0;
    int len = seq->len;
    size_t backw_stop = seq->backw_stop;
    size_t backw = seq->backw;
    size_t idxcnt = seq->idxcnt;
    size_t idxmax = seq->idxmax;
    int32_t idx = seq->idx;
    const unsigned char *us = seq->us;

    while (len == 0) {
        ++val;
        if (backw_stop != ~0ul) {
            if (backw == backw_stop) {
                if (idxcnt < idxmax) {
                    idx = seq->save_idx;
                    backw_stop = ~0ul;
                } else {
                    idx = 0;
                    break;
                }
            } else {
                size_t i = backw_stop;

                us = seq->back_us;
                while (i < backw) {
                    int32_t tmp = findidx_bounded(table, indirect, extra, &us, (size_t)(end - us));
                    idx = tmp & 0xffffff;
                    i++;
                }
                --backw;
                us = seq->us;
            }
        } else {
            backw_stop = idxmax;
            int32_t prev_idx = idx;

            while (us < end) {
                int32_t tmp = findidx_bounded(table, indirect, extra, &us, (size_t)(end - us));
                unsigned char rule = tmp >> 24;

                prev_idx = idx;
                idx = tmp & 0xffffff;
                idxcnt = idxmax++;

                if (idxcnt == 0) {
                    seq->rule = rule;
                }

                if ((rulesets[rule * nrules + pass] & sort_backward) == 0) {
                    break;
                }
                ++idxcnt;
            }

            if (backw_stop >= idxcnt) {
                if (idxcnt == idxmax || backw_stop > idxcnt) {
                    break;
                }
                backw_stop = ~0ul;
            } else {
                seq->back_us = seq->us;
                seq->us = us;
                backw = idxcnt;
                if (idxmax > idxcnt) {
                    backw--;
                    seq->save_idx = idx;
                    idx = prev_idx;
                }
                if (backw > backw_stop) {
                    backw--;
                }
            }
        }

        len = weights[idx++];
        for (int i = 0; i < pass; i++) {
            idx += len;
            len = weights[idx];
            idx++;
        }
    }

    seq->val = val;
    seq->len = len;
    seq->backw_stop = backw_stop;
    seq->backw = backw;
    seq->idxcnt = idxcnt;
    seq->idxmax = idxmax;
    seq->us = us;
    seq->idx = idx;
}

/* Compare two sequences. */
static int do_compare(coll_seq *seq1, coll_seq *seq2, int position, const unsigned char *weights)
{
    int seq1len = seq1->len;
    int seq2len = seq2->len;
    size_t val1 = seq1->val;
    size_t val2 = seq2->val;
    int idx1 = seq1->idx;
    int idx2 = seq2->idx;
    int result = 0;

    if (position && val1 != val2) {
        result = val1 > val2 ? 1 : -1;
        goto out;
    }

    do {
        if (weights[idx1] != weights[idx2]) {
            result = weights[idx1] - weights[idx2];
            goto out;
        }
        ++idx1;
        ++idx2;
        --seq1len;
        --seq2len;
    } while (seq1len > 0 && seq2len > 0);

    if (position && seq1len != seq2len) {
        result = seq1len - seq2len;
    }

out:
    seq1->len = seq1len;
    seq2->len = seq2len;
    seq1->idx = idx1;
    seq2->idx = idx2;
    return result;
}

/*
 * Resolve the locale object to use.  An unset locale means the databse
 * default collation. OpenGauss sets it via uselocale call.
 */
static inline locale_t get_effective_locale(pg_locale_t locale)
{
    if (locale != (pg_locale_t)0) {
        return (locale_t)locale;
    }
    return uselocale((locale_t)0);
}

/*
 * Fast path for pure-ASCII-digit strings.
 *
 * In standard glibc locales (en_US.UTF-8, C, POSIX, zh_CN.UTF-8, etc.) the
 * collation order of strings consisting only of ASCII digits ('0'..'9') is
 * exactly their byte order.  However, a custom locale created via localedef
 * can reorder digits (e.g. place '2' before '1'), so the fast path must be
 * gated by a per-locale check: digits_standard_order() verifies that
 * strcoll_l("0","1") < strcoll_l("1","2") < ... < strcoll_l("8","9") < 0
 * and caches the result in a thread-local single-entry cache (the effective
 * locale is stable within a thread for the duration of a query).  When the
 * locale changes (rare), the cache is revalidated with 9 strcoll_l calls.
 *
 * The real workload keys (tradeid, investorid) are digit-leading, so the
 * overwhelmingly common differ case can be decided by a single byte scan
 * instead of the expensive multi-pass weight walk (findidx indirect/extended
 * weight-table path).  Returns NEED_FULL_WALK when any byte before the first
 * difference is not an ASCII digit, or when the differing byte pair is not
 * both digits -- in those cases the caller falls back to the weight walk.
 */
#define NEED_FULL_WALK (-2)

/*
 * Thread-local single-entry cache for the digit-order check.
 * A thread operates on one database = one locale at a time, so the hit rate
 * is ~100% after the first call.  When the locale changes (session switch,
 * cross-database query), the cache is revalidated with 9 strcoll_l calls.
 */
static __thread locale_t digits_checked_locale = (locale_t)0;
static __thread int digits_order_safe = -1;  /* -1 = unknown, 0 = unsafe, 1 = safe */

/*
 * Return true if ASCII digits '0'..'9' collate in byte order under locale l.
 * Uses strcoll_l() on single-character pairs to verify monotonicity.
 * Result is cached per-thread for the last locale seen.
 */
static bool digits_standard_order(locale_t l)
{
    if (l == (locale_t)0) {
        /* C/POSIX or no locale set: byte order always applies. */
        return true;
    }
    if (digits_checked_locale == l && digits_order_safe >= 0) {
        return digits_order_safe != 0;
    }

    /* Verify strcoll_l("k","k+1") < 0 for k = '0'..'8'. */
    char k[2], kn[2];
    k[1] = '\0';
    kn[1] = '\0';
    bool safe = true;
    for (char c = '0'; c < '9'; ++c) {
        k[0] = c;
        kn[0] = c + 1;
        if (strcoll_l(k, kn, l) >= 0) {
            safe = false;
            break;
        }
    }

    digits_checked_locale = l;
    digits_order_safe = safe ? 1 : 0;
    return safe;
}

static int digits_compare(const unsigned char *s1, int len1, const unsigned char *s2, int len2)
{
    int n = (len1 < len2) ? len1 : len2;
    int i;

    for (i = 0; i < n; ++i) {
        unsigned char c1 = s1[i];
        unsigned char c2 = s2[i];
        if (c1 != c2) {
            if (c1 >= '0' && c1 <= '9' && c2 >= '0' && c2 <= '9')
                return (c1 < c2) ? -1 : 1;
            return NEED_FULL_WALK;
        }
        if (c1 < '0' || c1 > '9')
            return NEED_FULL_WALK;
    }

    /* Common prefix identical; check the tails are all digits. */
    for (; i < len1; ++i) {
        unsigned char c = s1[i];
        if (c < '0' || c > '9')
            return NEED_FULL_WALK;
    }
    for (; i < len2; ++i) {
        unsigned char c = s2[i];
        if (c < '0' || c > '9')
            return NEED_FULL_WALK;
    }

    if (len1 == len2)
        return 0;
    return (len1 < len2) ? -1 : 1;
}

/* Length-bounded locale comparison driven by glibc's collation weights. */
static int strncoll_libc(const char *s1, int len1, const char *s2, int len2, pg_locale_t locale)
{
    locale_t l = get_effective_locale(locale);
    struct __locale_data *current = NULL;
    uint32_t nrules = 0;
    const unsigned char *rulesets = NULL;
    const int32_t *table = NULL;
    const unsigned char *weights = NULL;
    const unsigned char *extra = NULL;
    const int32_t *indirect = NULL;
    const unsigned char *end1 = NULL;
    const unsigned char *end2 = NULL;
    int result = 0;
    int rule = 0;
    coll_seq seq1;
    coll_seq seq2;

    if (l == (locale_t)0) {
        return bounded_strcmp(s1, len1, s2, len2);
    }
    current = ((struct __locale_struct *)l)->__locales[LC_COLLATE];
    current = (struct __locale_data *)((char *)current - glibc_private_offset());
    nrules = current->values[_NL_ITEM_INDEX(_NL_COLLATE_NRULES)].word;
    if (nrules == 0) {
        return bounded_strcmp(s1, len1, s2, len2);
    }

    if (len1 == 0 || len2 == 0) {
        return (len1 != 0) - (len2 != 0);
    }

    rulesets = (const unsigned char *)current->values[_NL_ITEM_INDEX(_NL_COLLATE_RULESETS)].string;
    table = (const int32_t *)current->values[_NL_ITEM_INDEX(_NL_COLLATE_TABLEMB)].string;
    weights = (const unsigned char *)current->values[_NL_ITEM_INDEX(_NL_COLLATE_WEIGHTMB)].string;
    extra = (const unsigned char *)current->values[_NL_ITEM_INDEX(_NL_COLLATE_EXTRAMB)].string;
    indirect = (const int32_t *)current->values[_NL_ITEM_INDEX(_NL_COLLATE_INDIRECTMB)].string;

    end1 = (const unsigned char *)s1 + len1;
    end2 = (const unsigned char *)s2 + len2;

    seq1.len = 0;
    seq1.idxmax = 0;
    seq1.rule = 0;
    seq2.len = 0;
    seq2.idxmax = 0;

    for (int pass = 0; pass < (int)nrules; ++pass) {
        seq1.idxcnt = 0;
        seq1.idx = 0;
        seq2.idx = 0;
        seq1.backw_stop = ~0ul;
        seq1.backw = ~0ul;
        seq2.idxcnt = 0;
        seq2.backw_stop = ~0ul;
        seq2.backw = ~0ul;

        seq1.us = (const unsigned char *)s1;
        seq2.us = (const unsigned char *)s2;

        int position = rulesets[rule * nrules + pass] & sort_position;

        while (1) {
            get_next_seq(&seq1, nrules, rulesets, weights, table, extra, indirect, pass, end1);
            get_next_seq(&seq2, nrules, rulesets, weights, table, extra, indirect, pass, end2);

            if (seq1.len == 0 || seq2.len == 0) {
                if (seq1.len == seq2.len) {
                    if (pass == 0 && bounded_strcmp(s1, len1, s2, len2) == 0) {
                        return result;
                    } else {
                        break;
                    }
                }
                return seq1.len == 0 ? -1 : 1;
            }

            result = do_compare(&seq1, &seq2, position, weights);
            if (result != 0) {
                return result;
            }
        }

        rule = seq1.rule;
    }

    return result;
}

/*
 * strncoll
 *        Compare two byte strings according to a collation, reading at most
 *        len1 / len2 bytes from each input.
 *
 * Unlike strcoll(), the inputs need not be NUL-terminated; an embedded NUL
 * still ends the comparison.  Returns the usual negative / zero / positive
 * result.
 */
int strncoll(const char *arg1, int len1, const char *arg2, int len2, pg_locale_t locale)
{
    /*
     * Digit fast path: the dominant real-world keys (digit-leading tradeid /
     * investorid) are decided by a single byte scan BEFORE the two
     * bounded_strlen() memchr traversals.  Byte order equals collation order
     * for pure-ASCII-digit strings in standard glibc locales; a custom locale
     * created via localedef can reorder digits, so digits_standard_order()
     * gates the fast path with a per-thread cached check.
     * digits_compare() also honors strcoll's embedded-NUL semantics: a NUL
     * byte is not a digit, so it bails out with NEED_FULL_WALK.  Fall back to
     * the weight walk for everything else.
     */
    {
        int r = digits_compare((const unsigned char *)arg1, len1, (const unsigned char *)arg2, len2);
        if (r != NEED_FULL_WALK) {
            locale_t l = get_effective_locale(locale);
            if (digits_standard_order(l)) {
                return r;
            }
            /* Locale reorders digits; fall through to full weight walk. */
        }
    }

    len1 = bounded_strlen(arg1, len1);
    len2 = bounded_strlen(arg2, len2);

    return strncoll_libc(arg1, len1, arg2, len2, locale);
}

#endif /* HAVE_LOCALE_T && __GLIBC__ && !WIN32 */
