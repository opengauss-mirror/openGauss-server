/* -----------------------------------------------------------------------
 * formatting.h
 *
 * src/include/utils/formatting.h
 *
 *
 *	 Portions Copyright (c) 1999-2012, PostgreSQL Global Development Group
 *
 *	 The openGauss routines for a DateTime/int/float/numeric formatting,
 *	 inspire with A db TO_CHAR() / TO_DATE() / TO_NUMBER() routines.
 *
 *	 Karel Zak
 *
 * -----------------------------------------------------------------------
 */

#ifndef _FORMATTING_H_
#define _FORMATTING_H_

#include "fmgr.h"

#define NUM_CACHE_SIZE 64
#define NUM_CACHE_FIELDS 16
#define DCH_CACHE_SIZE 128
#define DCH_CACHE_FIELDS 16

/* ----------
 * FromCharDateMode
 * ----------
 *
 * This value is used to nominate one of several distinct (and mutually
 * exclusive) date conventions that a keyword can belong to.
 */
typedef enum {
    FROM_CHAR_DATE_NONE = 0,  /* Value does not affect date mode. */
    FROM_CHAR_DATE_GREGORIAN, /* Gregorian (day, month, year) style date */
    FROM_CHAR_DATE_ISOWEEK    /* ISO 8601 week date */
} FromCharDateMode;

typedef struct {
    const char* name;
    int len;
    int id;
    bool is_digit;
    FromCharDateMode date_mode;
} KeyWord;

/* ----------
 * Number description struct
 * ----------
 */
typedef struct {
    int pre,           /* (count) numbers before decimal */
        post,          /* (count) numbers after decimal  */
        lsign,         /* want locales sign		  */
        flag,          /* number parameters		  */
        pre_lsign_num, /* tmp value for lsign		  */
        multi,         /* multiplier for 'V'		  */
        zero_start,    /* position of first zero	  */
        zero_end,      /* position of last zero	  */
        need_locale;   /* needs it locale		  */
} NUMDesc;

typedef struct FormatNode {
    int type;           /* node type			*/
    const KeyWord* key; /* if node type is KEYWORD	*/
    char character;     /* if node type is CHAR		*/
    int suffix;         /* keyword suffix		*/
} FormatNode;

typedef struct DCHCacheEntry {
    FormatNode format[DCH_CACHE_SIZE + 1];
    char str[DCH_CACHE_SIZE + 1];
    int age;
} DCHCacheEntry;

typedef struct NUMCacheEntry {
    FormatNode format[NUM_CACHE_SIZE + 1];
    char str[NUM_CACHE_SIZE + 1];
    int age;
    NUMDesc Num;
} NUMCacheEntry;

extern char* str_tolower(const char* buff, size_t nbytes, Oid collid);
extern char* str_toupper(const char* buff, size_t nbytes, Oid collid);
extern char* str_toupper_for_raw(const char* buff, size_t nbytes, Oid collid);
extern char* str_initcap(const char* buff, size_t nbytes, Oid collid);

extern char* asc_tolower(const char* buff, size_t nbytes);
extern char* asc_toupper(const char* buff, size_t nbytes);
extern char* asc_initcap(const char* buff, size_t nbytes);

extern Datum timestamp_to_char(PG_FUNCTION_ARGS);
extern Datum timestamptz_to_char(PG_FUNCTION_ARGS);
extern Datum interval_to_char(PG_FUNCTION_ARGS);
extern Datum to_timestamp(PG_FUNCTION_ARGS);
extern Datum to_date(PG_FUNCTION_ARGS);
extern Datum numeric_to_number(PG_FUNCTION_ARGS);
extern Datum numeric_to_char(PG_FUNCTION_ARGS);
extern Datum int4_to_char(PG_FUNCTION_ARGS);
extern Datum int8_to_char(PG_FUNCTION_ARGS);
extern Datum float4_to_char(PG_FUNCTION_ARGS);
extern Datum float8_to_char(PG_FUNCTION_ARGS);
#include "pgtime.h"
#include "datatype/timestamp.h"
extern Datum to_timestamp_default_format(PG_FUNCTION_ARGS);
typedef struct TmToChar {
    struct pg_tm tm; /* classic 'tm' struct */
    fsec_t fsec;     /* fractional seconds */
    const char* tzn; /* timezone */
} TmToChar;
#define tmtcTm(_X) (&(_X)->tm)
#define tmtcTzn(_X) ((_X)->tzn)
#define tmtcFsec(_X) ((_X)->fsec)

extern void Init_NUM_cache(void);

#define ZERO_tm(_X)                                                                                                    \
    do {                                                                                                               \
        (_X)->tm_sec = (_X)->tm_year = (_X)->tm_min = (_X)->tm_wday = (_X)->tm_hour = (_X)->tm_yday = (_X)->tm_isdst = \
            0;                                                                                                         \
        (_X)->tm_mday = (_X)->tm_mon = 1;                                                                              \
    } while (0)

#define ZERO_tmtc(_X)        \
    do {                     \
        ZERO_tm(tmtcTm(_X)); \
        tmtcFsec(_X) = 0;    \
        tmtcTzn(_X) = NULL;  \
    } while (0)
extern Datum timestamp_to_char_default_format(PG_FUNCTION_ARGS);
extern Datum timestamptz_to_char_default_format(PG_FUNCTION_ARGS);
extern text* datetime_to_char_body(TmToChar* tmtc, text* fmt, bool is_interval, Oid collid);

extern void check_datetime_format(char* fmt);
extern void* get_time_format(char* fmt_str);
extern void to_timestamp_from_format(struct pg_tm* tm, fsec_t* fsec, char* date_str, void* format);
#ifdef ENABLE_UT
extern void general_to_timestamp_from_user_format(struct pg_tm* tm, fsec_t* fsec, char* date_str, void* in_format);
extern void optimized_to_timestamp_from_user_format(struct pg_tm* tm, fsec_t* fsec, char* date_str, void* in_format);
#endif

#endif
