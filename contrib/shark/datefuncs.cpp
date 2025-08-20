/*
 * Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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
 * --------------------------------------------------------------------------------------
 *
 * datefuns.cpp
 *
 * IDENTIFICATION
 *        contrib/shark/datefuncs.cpp
 *
 * --------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <cctype>
#include <cfloat>
#include <climits>
#include <cmath>
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/numeric.h"
#include "utils/timestamp.h"
#include "utils/datetime.h"
#include "utils/memutils.h"
#include "libpq/pqformat.h"
#include "parser/scansup.h"
#include "common/int.h"
#include "miscadmin.h"
#include "access/xact.h"
#include "shark.h"

#define MAX_NANO_SECOND 1000000000
#define MAX_MICRO_SECOND 1000000
#define INTERVAL_NUM 35
#define INTERVAL_KEY_SIZE 16

static HTAB *IntervalHash = NULL;
extern "C" Datum dateaddtimestamp(PG_FUNCTION_ARGS);
extern "C" Datum dateaddtimestamptz(PG_FUNCTION_ARGS);
extern "C" Datum dateadddate(PG_FUNCTION_ARGS);
extern "C" Datum dateaddtime(PG_FUNCTION_ARGS);
extern "C" Datum dateaddtimetz(PG_FUNCTION_ARGS);
extern "C" Datum dateparttimestamp(PG_FUNCTION_ARGS);
extern "C" Datum dateparttimestamptz(PG_FUNCTION_ARGS);
extern "C" Datum datepartdate(PG_FUNCTION_ARGS);
extern "C" Datum dateparttime(PG_FUNCTION_ARGS);
extern "C" Datum dateparttimetz(PG_FUNCTION_ARGS);
extern "C" Datum getdate_internal(PG_FUNCTION_ARGS);
extern "C" Datum datenametimestamp(PG_FUNCTION_ARGS);
extern "C" Datum datenametimestamptz(PG_FUNCTION_ARGS);
extern "C" Datum datenamedate(PG_FUNCTION_ARGS);
extern "C" Datum datenametime(PG_FUNCTION_ARGS);
extern "C" Datum datenametimetz(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(dateaddtimestamp);
PG_FUNCTION_INFO_V1(dateaddtimestamptz);
PG_FUNCTION_INFO_V1(dateadddate);
PG_FUNCTION_INFO_V1(dateaddtime);
PG_FUNCTION_INFO_V1(dateaddtimetz);
PG_FUNCTION_INFO_V1(dateparttimestamp);
PG_FUNCTION_INFO_V1(dateparttimestamptz);
PG_FUNCTION_INFO_V1(datepartdate);
PG_FUNCTION_INFO_V1(dateparttime);
PG_FUNCTION_INFO_V1(dateparttimetz);
PG_FUNCTION_INFO_V1(getdate_internal);
PG_FUNCTION_INFO_V1(datenametimestamp);
PG_FUNCTION_INFO_V1(datenametimestamptz);
PG_FUNCTION_INFO_V1(datenamedate);
PG_FUNCTION_INFO_V1(datenametime);
PG_FUNCTION_INFO_V1(datenametimetz);

typedef struct DataInfo {
    unsigned long year = 0;
    unsigned long month = 0;
    unsigned long day = 0;
    unsigned long hour = 0;
    unsigned long long minute = 0;
    unsigned long long second = 0;
    unsigned long long secondPart = 0;
    bool isNegativeNumber = false;
} DataInfo;

enum IntervalExprType {
    IET_YEAR,
    IET_QUARTER,
    IET_MONTH,
    IET_WEEK,
    IET_DAY,
    IET_HOUR,
    IET_MINUTE,
    IET_SECOND,
    IET_MILLISECOND,
    IET_MICROSECOND,
    IET_NANOSECOND,
    IET_YEAR_MONTH,
    IET_DAY_HOUR,
    IET_DAY_MINUTE,
    IET_DAY_SECOND,
    IET_HOUR_MINUTE,
    IET_HOUR_SECOND,
    IET_MINUTE_SECOND,
    IET_DAY_MICROSECOND,
    IET_HOUR_MICROSECOND,
    IET_MINUTE_MICROSECOND,
    IET_SECOND_MICROSECOND
};

typedef struct {
    char key[INTERVAL_KEY_SIZE];
    IntervalExprType type;
    int yearTimes;
    int monthTimes;
    int dayTimes;
    int hourTimes;
    int minuteTimes;
    int secondTimes;
    double secondPartTimes;
} IntervalEntry;

typedef struct {
    char key[INTERVAL_KEY_SIZE];
    void *valuePointer;
} IntervalKey;

static const IntervalEntry IntervalMap[] = {
    {"yy",           IET_YEAR,         1, 0, 0, 0, 0, 0, 0},
    {"yyyy",         IET_YEAR,         1, 0, 0, 0, 0, 0, 0},
    {"year",         IET_YEAR,         1, 0, 0, 0, 0, 0, 0},
    {"q",            IET_QUARTER,      0, 3, 0, 0, 0, 0, 0},
    {"qq",           IET_QUARTER,      0, 3, 0, 0, 0, 0, 0},
    {"quarter",      IET_QUARTER,      0, 3, 0, 0, 0, 0, 0},
    {"m",            IET_MONTH,        0, 1, 0, 0, 0, 0, 0},
    {"mm",           IET_MONTH,        0, 1, 0, 0, 0, 0, 0},
    {"month",        IET_MONTH,        0, 1, 0, 0, 0, 0, 0},
    {"wk",           IET_WEEK,         0, 0, 7, 0, 0, 0, 0},
    {"ww",           IET_WEEK,         0, 0, 7, 0, 0, 0, 0},
    {"week",         IET_WEEK,         0, 0, 7, 0, 0, 0, 0},
    {"dy",           IET_DAY,          0, 0, 1, 0, 0, 0, 0},
    {"y",            IET_DAY,          0, 0, 1, 0, 0, 0, 0},
    {"dayofyear",    IET_DAY,          0, 0, 1, 0, 0, 0, 0},
    {"dd",           IET_DAY,          0, 0, 1, 0, 0, 0, 0},
    {"d",            IET_DAY,          0, 0, 1, 0, 0, 0, 0},
    {"day",          IET_DAY,          0, 0, 1, 0, 0, 0, 0},
    {"dw",           IET_DAY,          0, 0, 1, 0, 0, 0, 0},
    {"w",            IET_DAY,          0, 0, 1, 0, 0, 0, 0},
    {"weekday",      IET_DAY,          0, 0, 1, 0, 0, 0, 0},
    {"hh",           IET_HOUR,         0, 0, 0, 1, 0, 0, 0},
    {"hour",         IET_HOUR,         0, 0, 0, 1, 0, 0, 0},
    {"mi",           IET_MINUTE,       0, 0, 0, 0, 1, 0, 0},
    {"n",            IET_MINUTE,       0, 0, 0, 0, 1, 0, 0},
    {"minute",       IET_MINUTE,       0, 0, 0, 0, 1, 0, 0},
    {"ss",           IET_SECOND,       0, 0, 0, 0, 0, 1, 0},
    {"s",            IET_SECOND,       0, 0, 0, 0, 0, 1, 0},
    {"second",       IET_SECOND,       0, 0, 0, 0, 0, 1, 0},
    {"ms",           IET_MILLISECOND,  0, 0, 0, 0, 0, 0, 1000},
    {"millisecond",  IET_MILLISECOND,  0, 0, 0, 0, 0, 0, 1000},
    {"mcs",          IET_MICROSECOND,  0, 0, 0, 0, 0, 0, 1},
    {"microsecond",  IET_MICROSECOND,  0, 0, 0, 0, 0, 0, 1},
    {"ns",           IET_NANOSECOND,   0, 0, 0, 0, 0, 0, 0.001},
    {"nanosecond",   IET_NANOSECOND,   0, 0, 0, 0, 0, 0, 0.001},
};

static void InitIntervalHash(void)
{
    HASHCTL ctl;
    MemSet(&ctl, 0, sizeof(ctl));
    ctl.keysize   = INTERVAL_KEY_SIZE;
    ctl.entrysize = sizeof(IntervalKey);
    ctl.hash      = string_hash;

    IntervalHash = hash_create("IntervalKeywordHash",
                               INTERVAL_NUM,
                               &ctl,
                               HASH_ELEM | HASH_FUNCTION);
}

static void PopulateHash(void)
{
    bool found;
    for (size_t i = 0; i < sizeof(IntervalMap) / sizeof(IntervalMap[0]); ++i) {
        IntervalKey *k = (IntervalKey *)hash_search(
            IntervalHash, IntervalMap[i].key, HASH_ENTER, &found);

        if (!found) {
            strlcpy(k->key, IntervalMap[i].key, INTERVAL_KEY_SIZE);
            k->valuePointer = (void *)&IntervalMap[i];
        }
    }
}

void InitIntervalLookup(void)
{
    if (!IntervalHash) {
        InitIntervalHash();
        PopulateHash();
    }
}

static inline const IntervalEntry *FindInterval(const char *key)
{
    IntervalKey *vp = (IntervalKey *)hash_search(IntervalHash, (void *)key, HASH_FIND, NULL);
    if (!vp) {
        return NULL;
    }

    return (IntervalEntry*)(vp->valuePointer);
}

void GetIntervalTypeDateMs(char* args, const int inter, DataInfo *info, int* intervalType)
{
    char* arg = pg_strtolower(args);
    long long value = inter;
    info->year = 0;
    *intervalType = -1;

    const IntervalEntry *m = FindInterval(arg);
    if (!m) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
            errmsg("unrecognized role option \"%s\"", arg)));
    }

    *intervalType = m->type;
    info->year += value * m->yearTimes;
    info->month += value * m->monthTimes;
    info->day += value * m->dayTimes;
    info->hour += value * m->hourTimes;
    info->minute += value * m->minuteTimes;
    info->second += value * m->secondTimes;
    info->secondPart += value * m->secondPartTimes;

    if (value < 0) {
        info->isNegativeNumber = true;
    }
}

void get_datainfo_interval(int intervalType, DataInfo* info, Interval* intervalValue)
{
    switch (intervalType) {
        case IET_SECOND_MICROSECOND:
        case IET_MINUTE_MICROSECOND:
        case IET_HOUR_MICROSECOND:
        case IET_DAY_MICROSECOND:
        case IET_MICROSECOND:
        case IET_MILLISECOND:
        case IET_NANOSECOND: {
            long secs;
            long microsecs;
            intervalValue->day = info->day;
#ifdef HAVE_INT64_TIMESTAMP
            secs = (long)(info->secondPart / USECS_PER_SEC);
            microsecs = (long)(info->secondPart % USECS_PER_SEC);
            intervalValue->time =
            (((((info->hour * MINS_PER_HOUR) + info->minute) * SECS_PER_MINUTE) + info->second + secs)* USECS_PER_SEC)
            + microsecs;
#else
            secs = (long)info->secondPart;
            microsecs = (long)((info->secondPart - secs) * 1000000.0);
            intervalValue->time =
                       (((info->hour * (double)MINS_PER_HOUR) + info->minute) * (double)SECS_PER_MINUTE)
                       + info->second + secs + microsecs;
#endif
            break;
        }
        case IET_SECOND:
        case IET_MINUTE:
        case IET_HOUR:
        case IET_MINUTE_SECOND:
        case IET_HOUR_SECOND:
        case IET_HOUR_MINUTE: {
#ifdef HAVE_INT64_TIMESTAMP
            intervalValue->time =
                  (((((info->hour * MINS_PER_HOUR) + info->minute) * SECS_PER_MINUTE) + info->second) * USECS_PER_SEC);
#else
            intervalValue->time =
                  (((info->hour * (double)MINS_PER_HOUR) + info->minute) * (double)SECS_PER_MINUTE) + info->second;
#endif
            break;
        }
        case IET_DAY:
        case IET_WEEK: {
            intervalValue->day = info->day;
            break;
        }
        case IET_DAY_SECOND:
        case IET_DAY_MINUTE:
        case IET_DAY_HOUR: {
            intervalValue->day = info->day;
#ifdef HAVE_INT64_TIMESTAMP
            intervalValue->time =
                (((((info->hour * MINS_PER_HOUR) + info->minute) * SECS_PER_MINUTE) + info->second) * USECS_PER_SEC);
#else
            intervalValue->time =
                (((info->hour * (double)MINS_PER_HOUR) + info->minute) * (double)SECS_PER_MINUTE) + info->second;
#endif
            break;
        }
        case IET_YEAR:
        case IET_YEAR_MONTH:
        case IET_QUARTER:
        case IET_MONTH: {
            intervalValue->month = (info->year * MONTHS_PER_YEAR + info->month +info->secondPart);
            break;
        }
        default:
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("unrecognized role option")));
    }
    return;
}

void date_add(pg_tm *tm, fsec_t *fsec, int intervalType, DataInfo* info)
{
    long long secondPart = *fsec;
    long sign = (info->isNegativeNumber ? -1 : 1);

    switch (intervalType) {
        case IET_SECOND:
        case IET_SECOND_MICROSECOND:
        case IET_MICROSECOND:
        case IET_MINUTE:
        case IET_HOUR:
        case IET_MINUTE_MICROSECOND:
        case IET_MINUTE_SECOND:
        case IET_HOUR_SECOND:
        case IET_HOUR_MINUTE:
        case IET_HOUR_MICROSECOND:
        case IET_DAY_MICROSECOND:
        case IET_DAY_SECOND:
        case IET_DAY_MINUTE:
        case IET_DAY_HOUR:
        case IET_DAY:
        case IET_WEEK:
        case IET_MILLISECOND:
        case IET_NANOSECOND: {
            long long microseconds;
            long long extractSecond;
            long long second;
            long long days;

            microseconds = secondPart + sign * info->secondPart;
            extractSecond = microseconds / 1000000L;
            microseconds = microseconds % 1000000L;

            second = ((tm->tm_mday) * SECS_PER_HOUR * 24L + tm->tm_hour * SECS_PER_HOUR +
                tm->tm_min * SECS_PER_MINUTE + tm->tm_sec + sign * (long long)(info->day *
                    SECS_PER_HOUR * 24L + info->hour * 3600LL + info->minute * 60LL +
                    info->second)) + extractSecond;
            if (microseconds < 0) {
                microseconds += 1000000LL;
                second--;
            }
            *fsec = microseconds;
            days = second / (SECS_PER_HOUR * HOURS_PER_DAY);
            second -= days * SECS_PER_HOUR * HOURS_PER_DAY;
            if (second < 0) {
                days--;
                second += SECS_PER_HOUR * HOURS_PER_DAY;
            }
            tm->tm_mday = days;
            tm->tm_hour = second / SECS_PER_HOUR;
            tm->tm_min = second / MINS_PER_HOUR % SECS_PER_MINUTE;
            tm->tm_sec = second % SECS_PER_MINUTE;
            break;
        }
        case IET_YEAR: {
            tm->tm_year += sign * info->year;
            if (isleap(tm->tm_year) && tm->tm_mon == 2 && tm->tm_mday == 29) {
                tm->tm_mday = 28;
            }
            break;
        }
        case IET_YEAR_MONTH:
        case IET_QUARTER:
        case IET_MONTH: {
            long long months = sign * info->year * MONTHS_PER_YEAR + sign * info->month;
            tm->tm_mon += months;
            if (tm->tm_mon > MONTHS_PER_YEAR) {
                tm->tm_year += (tm->tm_mon - 1) / MONTHS_PER_YEAR;
                tm->tm_mon = ((tm->tm_mon - 1) % MONTHS_PER_YEAR) + 1;
            } else if (tm->tm_mon < 1) {
                tm->tm_year += tm->tm_mon / MONTHS_PER_YEAR - 1;
                tm->tm_mon = tm->tm_mon % MONTHS_PER_YEAR + MONTHS_PER_YEAR;
            }

            if (tm->tm_mday > day_tab[isleap(tm->tm_year)][tm->tm_mon - 1]) {
                tm->tm_mday = (day_tab[isleap(tm->tm_year)][tm->tm_mon - 1]);
            }
            break;
        }
        default:
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("unrecognized role option")));
    }
}

Interval GetDateSpan(char* args, const int inter)
{
    DataInfo info;
    int intervalType = -1;
    Interval span;
    span.day = 0;
    span.month = 0;
    span.time = 0;

    GetIntervalTypeDateMs(args, inter, &info, &intervalType);
    get_datainfo_interval(intervalType, &info, &span);
    return span;
}

Datum dateaddtimestamp(PG_FUNCTION_ARGS)
{
    Timestamp timestampVal = PG_GETARG_TIMESTAMP(2);
    char* argdataNum = PG_GETARG_CSTRING(0);
    int interval = PG_GETARG_INT32(1);
    Timestamp restimestamp;

    Interval span = GetDateSpan(argdataNum, interval);

    restimestamp = timestamp_pl_interval(timestampVal, &span);
    PG_RETURN_TIMESTAMP(restimestamp);
}

Datum dateaddtimestamptz(PG_FUNCTION_ARGS)
{
    TimestampTz timestampVal = PG_GETARG_TIMESTAMPTZ(2);
    char* argdataNum = PG_GETARG_CSTRING(0);
    int interval = PG_GETARG_INT32(1);
    TimestampTz restimestamptz;

    Interval span = GetDateSpan(argdataNum, interval);

    restimestamptz =
           DirectFunctionCall2(timestamptz_pl_interval, TimestampGetDatum(timestampVal), PointerGetDatum(&span));
    PG_RETURN_TIMESTAMP(restimestamptz);
}

Datum dateadddate(PG_FUNCTION_ARGS)
{
    DateADT dateVal = PG_GETARG_DATEADT(2);
    char* argdataNum = PG_GETARG_CSTRING(0);
    int interval = PG_GETARG_INT32(1);
    Timestamp restimestamp;
    Timestamp dateStamp = date2timestamp(dateVal);

    Interval span = GetDateSpan(argdataNum, interval);

    restimestamp = timestamp_pl_interval(dateStamp, &span);
    PG_RETURN_TIMESTAMP(restimestamp);
}

void dateaddtime_internal(char* args, const int inter, pg_tm *tm, fsec_t *fsec)
{
    tm->tm_mday = 1;
    tm->tm_mon = 1;
    tm->tm_year = 1900;

    DataInfo info;
    int intervalType = -1;
    GetIntervalTypeDateMs(args, inter, &info, &intervalType);
    date_add(tm, fsec, intervalType, &info);
}

Datum dateaddtime(PG_FUNCTION_ARGS)
{
    Timestamp timestampVal;
    TimeADT time = PG_GETARG_TIMEADT(2);
    struct pg_tm tt;
    struct pg_tm *tm = &tt;
    fsec_t fsec;
    time2tm(time, tm, &fsec);

    char* argdata = PG_GETARG_CSTRING(0);
    int interval = PG_GETARG_INT32(1);

    dateaddtime_internal(argdata, interval, tm, &fsec);

    if (tm2timestamp(tm, fsec, NULL, &timestampVal) != 0) {
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
            errmsg("timestamp out of range")));
    }
    PG_RETURN_TIMESTAMP(timestampVal);
}

Datum dateaddtimetz(PG_FUNCTION_ARGS)
{
    TimestampTz timestampVal;
    TimeTzADT* time = PG_GETARG_TIMETZADT_P(2);
    struct pg_tm tt;
    struct pg_tm *tm = &tt;
    fsec_t fsec;
    int tz = 0;
    timetz2tm(time, tm, &fsec, &tz);

    char* argdata = PG_GETARG_CSTRING(0);
    int interval = PG_GETARG_INT32(1);

    dateaddtime_internal(argdata, interval, tm, &fsec);

    if (tm2timestamp(tm, fsec, &tz, &timestampVal) != 0) {
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
            errmsg("timestamp out of range")));
    }
    PG_RETURN_TIMESTAMPTZ(timestampVal);
}

static inline bool IsYearFormatStr(const char *arg)
{
    return (strcmp(arg, "yy") == 0 || strcmp(arg, "yyyy") == 0 || strcmp(arg, "year") == 0);
}

static inline bool IsQuarterFormatStr(const char *arg)
{
    return (strcmp(arg, "qq") == 0 || strcmp(arg, "q") == 0 || strcmp(arg, "quarter") == 0);
}

static inline bool IsMonthFormatStr(const char *arg)
{
    return (strcmp(arg, "mm") == 0 || strcmp(arg, "m") == 0 || strcmp(arg, "month") == 0);
}

static inline bool IsDayofyearFormatStr(const char *arg)
{
    return (strcmp(arg, "dy") == 0 || strcmp(arg, "y") == 0 || strcmp(arg, "dayofyear") == 0);
}

static inline bool IsDayFormatStr(const char *arg)
{
    return (strcmp(arg, "dd") == 0 || strcmp(arg, "d") == 0 || strcmp(arg, "day") == 0);
}

static inline bool IsWeekdayFormatStr(const char *arg)
{
    return (strcmp(arg, "dw") == 0 || strcmp(arg, "w") == 0 || strcmp(arg, "weekday") == 0);
}

static inline bool IsWeekFormatStr(const char *arg)
{
    return (strcmp(arg, "wk") == 0 || strcmp(arg, "ww") == 0 || strcmp(arg, "week") == 0);
}

static inline bool IsHourFormatStr(const char *arg)
{
    return (strcmp(arg, "hh") == 0 || strcmp(arg, "hour") == 0);
}

static inline bool IsMinuteFormatStr(const char *arg)
{
    return (strcmp(arg, "mi") == 0 || strcmp(arg, "n") == 0 || strcmp(arg, "minute") == 0);
}

static inline bool IsSecondFormatStr(const char *arg)
{
    return (strcmp(arg, "ss") == 0 || strcmp(arg, "s") == 0 || strcmp(arg, "second") == 0);
}

static inline bool IsMillisecondFormatStr(const char *arg)
{
    return (strcmp(arg, "ms") == 0 || strcmp(arg, "millisecond") == 0);
}

static inline bool IsMicrosecondFormatStr(const char *arg)
{
    return (strcmp(arg, "mcs") == 0 || strcmp(arg, "microsecond") == 0);
}

static inline bool IsNanosecondFormatStr(const char *arg)
{
    return (strcmp(arg, "ns") == 0 || strcmp(arg, "nanosecond") == 0);
}

static inline bool IsTzoffsetFormatStr(const char *arg)
{
    return (strcmp(arg, "tz") == 0 || strcmp(arg, "tzoffset") == 0);
}

static inline bool IsIsoweekFormatStr(const char *arg)
{
    return (strcmp(arg, "isowk") == 0 || strcmp(arg, "isoww") == 0 || strcmp(arg, "iso_week") == 0);
}
void get_date_part_by_string(char* args, pg_tm *tm, int tz, fsec_t fsec, int &result, bool isTz)
{
    char* arg = pg_strtolower(args);
    if (IsYearFormatStr(arg)) {
        if (tm->tm_year > 0) {
            result = tm->tm_year;
        } else {
            result = tm->tm_year - 1;
        }
    } else if (IsQuarterFormatStr(arg)) {
        result = (tm->tm_mon - 1) / 3 + 1;
    } else if (IsMonthFormatStr(arg)) {
        result = tm->tm_mon;
    } else if (IsDayofyearFormatStr(arg)) {
        result = (date2j(tm->tm_year, tm->tm_mon, tm->tm_mday) - date2j(tm->tm_year, 1, 1) + 1);
    } else if (IsDayFormatStr(arg)) {
        result = tm->tm_mday;
    } else if (IsWeekdayFormatStr(arg)) {
        result = j2day(date2j(tm->tm_year, tm->tm_mon, tm->tm_mday)) + 1;
    } else if (IsWeekFormatStr(arg)) {
        int wd = 0;
        int yd = 0;
        wd = j2day(date2j(tm->tm_year, tm->tm_mon, tm->tm_mday));
        yd = (date2j(tm->tm_year, tm->tm_mon, tm->tm_mday) - date2j(tm->tm_year, 1, 1) + 1) - 1;
        int base = j2day(date2j(tm->tm_year, 1, 1));
        result = (base + yd) / 7 + 1;
    } else if (IsHourFormatStr(arg)) {
        result = tm->tm_hour;
    } else if (IsMinuteFormatStr(arg)) {
        result = tm->tm_min;
    } else if (IsSecondFormatStr(arg)) {
#ifdef HAVE_INT64_TIMESTAMP
        result = tm->tm_sec + fsec / 1000000.0;
#else
        result = tm->tm_sec + fsec;
#endif
    } else if (IsMillisecondFormatStr(arg)) {
#ifdef HAVE_INT64_TIMESTAMP
        result = fsec / 1000.0;
#else
        result = fsec * 1000;
#endif
    } else if (IsMicrosecondFormatStr(arg)) {
#ifdef HAVE_INT64_TIMESTAMP
        result = fsec;
#else
        result = fsec * MAX_MICRO_SECOND;
#endif
    } else if (IsNanosecondFormatStr(arg)) {
#ifdef HAVE_INT64_TIMESTAMP
        result = fsec * 1000;
#else
        result = fsec * MAX_NANO_SECOND;
#endif
    } else if (IsTzoffsetFormatStr(arg)) {
        if (!isTz) {
            result = 0;
        } else {
            result = -tz;
            result /= MINS_PER_HOUR;
        }
    } else if (IsIsoweekFormatStr(arg)) {
        result = (float8)date2isoweek(tm->tm_year, tm->tm_mon, tm->tm_mday);
    } else {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
            errmsg("unrecognized role option \"%s\"", arg)));
    }
}

Datum getdate_internal(PG_FUNCTION_ARGS)
{
    TimestampTz timestamptz = GetCurrentStmtsysTimestamp();
    Timestamp result;
    struct pg_tm tt;
    struct pg_tm *tm = &tt;
    fsec_t fsec = 0;
    int tz = 0;

    if (timestamp2tm(timestamptz, &tz, tm, &fsec, NULL, NULL) != 0) {
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
            errmsg("timestamp out of range")));
    }
#ifdef HAVE_INT64_TIMESTAMP
    fsec = fsec / 1000.0;
    int val = fsec % 10;
    if (val == 9) {
        fsec = fsec + 1;
    } else if (val == 5 || val == 6 || val == 7 || val == 8) {
        fsec = (fsec / 10) * 10 + 7;
    } else if (val == 2 || val == 3 || val == 4) {
        fsec = (fsec / 10) * 10 + 3;
    } else {
        fsec = (fsec / 10) * 10;
    }
    fsec = fsec * 1000;
#endif

    if (tm2timestamp(tm, fsec, NULL, &result) != 0) {
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
            errmsg("timestamp out of range")));
    }
    PG_RETURN_TIMESTAMP(result);
}

Datum dateparttimestamp(PG_FUNCTION_ARGS)
{
    Timestamp timestampVal = PG_GETARG_TIMESTAMP(1);
    char* args = PG_GETARG_CSTRING(0);
    struct pg_tm tt;
    struct pg_tm *tm = &tt;
    fsec_t fsec = 0;
    int res = 0;

    if (timestamp2tm(timestampVal, NULL, tm, &fsec, NULL, NULL) != 0) {
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
            errmsg("timestamp out of range")));
    }
    get_date_part_by_string(args, tm, 0, fsec, res, false);
    PG_RETURN_INT32(res);
}

Datum dateparttimestamptz(PG_FUNCTION_ARGS)
{
    TimestampTz timestampVal = PG_GETARG_TIMESTAMPTZ(1);
    char* args = PG_GETARG_CSTRING(0);
    struct pg_tm tm = {0};
    fsec_t fsec = 0;
    int tz = 0;
    int res = 0;

    if (timestamp2tm(timestampVal, &tz, &tm, &fsec, NULL, NULL) != 0) {
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
            errmsg("timestamp out of range")));
    }
    get_date_part_by_string(args, &tm, tz, fsec, res, true);
    PG_RETURN_INT32(res);
}

Datum datepartdate(PG_FUNCTION_ARGS)
{
    Timestamp timestampVal;
    DateADT dateVal = PG_GETARG_DATEADT(1);
    timestampVal = date2timestamp(dateVal);
    char* args = PG_GETARG_CSTRING(0);
    struct pg_tm tt;
    struct pg_tm *tm = &tt;
    fsec_t fsec = 0;
    int res = 0;

    timestamp2tm(timestampVal, NULL, tm, &fsec, NULL, NULL);
    if (!IS_VALID_JULIAN(tm->tm_year, tm->tm_mon, tm->tm_mday)) {
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
            errmsg("date out of range")));
    }
    get_date_part_by_string(args, tm, 0, fsec, res, false);
    PG_RETURN_INT32(res);
}

Datum dateparttime(PG_FUNCTION_ARGS)
{
    Timestamp timestampVal;
    TimeADT time = PG_GETARG_TIMEADT(1);
    struct pg_tm tt;
    struct pg_tm *tm = &tt;
    fsec_t fsec;
    time2tm(time, tm, &fsec);
    tm->tm_mday = 1;
    tm->tm_mon = 1;
    tm->tm_year = 1900;
    char* args = PG_GETARG_CSTRING(0);
    int res = 0;
 
    if (tm2timestamp(tm, fsec, NULL, &timestampVal) != 0) {
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
            errmsg("timestamp out of range")));
    }
    get_date_part_by_string(args, tm, 0, fsec, res, false);
    PG_RETURN_INT32(res);
}

Datum dateparttimetz(PG_FUNCTION_ARGS)
{
    TimestampTz timestampVal;
    TimeTzADT* time = PG_GETARG_TIMETZADT_P(1);
    struct pg_tm tt;
    struct pg_tm *tm = &tt;
    fsec_t fsec;
    int tz = 0;
    timetz2tm(time, tm, &fsec, &tz);
    tm->tm_mday = 1;
    tm->tm_mon = 1;
    tm->tm_year = 1900;
    char* args = PG_GETARG_CSTRING(0);
    int res = 0;

    if (tm2timestamp(tm, fsec, &tz, &timestampVal) != 0) {
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
            errmsg("timestamp out of range")));
    }
    get_date_part_by_string(args, tm, tz, fsec, res, true);
    PG_RETURN_INT32(res);
}

char* get_date_name_by_string(char* args, pg_tm* tm, int tz, fsec_t fsec, bool isTz)
{
    errno_t ret = 0;
    const int charLen = 16;
    char* result = (char*)palloc(charLen);
    int val = 0;
    bool isNull = true;
    char* arg = pg_strtolower(args);

    if (IsYearFormatStr(arg)) {
        if (tm->tm_year > 0) {
            val = tm->tm_year;
        } else {
            val = tm->tm_year - 1;
        }
    } else if (IsQuarterFormatStr(arg)) {
        val = (tm->tm_mon - 1)/3 + 1;
    } else if (IsMonthFormatStr(arg)) {
        isNull = false;
        val = tm->tm_mon;
        switch (val) {
            case 1:
                ret = sprintf_s(result, charLen, "%s", "January");
                break;
            case 2:
                ret = sprintf_s(result, charLen, "%s", "February");
                break;
            case 3:
                ret = sprintf_s(result, charLen, "%s", "March");
                break;
            case 4:
                ret = sprintf_s(result, charLen, "%s", "April");
                break;
            case 5:
                ret = sprintf_s(result, charLen, "%s", "May");
                break;
            case 6:
                ret = sprintf_s(result, charLen, "%s", "June");
                break;
            case 7:
                ret = sprintf_s(result, charLen, "%s", "July");
                break;
            case 8:
                ret = sprintf_s(result, charLen, "%s", "August");
                break;
            case 9:
                ret = sprintf_s(result, charLen, "%s", "September");
                break;
            case 10:
                ret = sprintf_s(result, charLen, "%s", "October");
                break;
            case 11:
                ret = sprintf_s(result, charLen, "%s", "November");
                break;
            case 12:
                ret = sprintf_s(result, charLen, "%s", "December");
                break;
            default:
                ret = sprintf_s(result, charLen, "%s", "Error");
                break;
        }
        securec_check_ss(ret, "", "");
    } else if (IsDayofyearFormatStr(arg)) {
        val = (date2j(tm->tm_year, tm->tm_mon, tm->tm_mday) - date2j(tm->tm_year, 1, 1) + 1);
    } else if (IsDayFormatStr(arg)) {
        val = tm->tm_mday;
    } else if (IsWeekdayFormatStr(arg)) {
        val = j2day(date2j(tm->tm_year, tm->tm_mon, tm->tm_mday)) + 1;
        isNull = false;
        switch (val) {
            case 1:
                ret = sprintf_s(result, charLen, "%s", "Sunday");
                break;
            case 2:
                ret = sprintf_s(result, charLen, "%s", "Monday");
                break;
            case 3:
                ret = sprintf_s(result, charLen, "%s", "Tuesday");
                break;
            case 4:
                ret = sprintf_s(result, charLen, "%s", "Wednesday");
                break;
            case 5:
                ret = sprintf_s(result, charLen, "%s", "Thursday");
                break;
            case 6:
                ret = sprintf_s(result, charLen, "%s", "Friday");
                break;
            case 7:
                ret = sprintf_s(result, charLen, "%s", "Saturday");
                break;
            default:
                ret = sprintf_s(result, charLen, "%s", "Error");
                break;
        }
        securec_check_ss(ret, "", "");
    } else if (IsWeekFormatStr(arg)) {
        int wd = 0;
        int yd = 0;
        wd = j2day(date2j(tm->tm_year, tm->tm_mon, tm->tm_mday));
        yd = (date2j(tm->tm_year, tm->tm_mon, tm->tm_mday) - date2j(tm->tm_year, 1, 1) + 1) - 1;
        int base = j2day(date2j(tm->tm_year, 1, 1));
        val = (base + yd) / 7 + 1;
    } else if (IsHourFormatStr(arg)) {
        val = tm->tm_hour;
    } else if (IsMinuteFormatStr(arg)) {
        val = tm->tm_min;
    }  else if (IsSecondFormatStr(arg)) {
#ifdef HAVE_INT64_TIMESTAMP
        val = tm->tm_sec + fsec / 1000000.0;
#else
        val = tm->tm_sec + fsec;
#endif
    } else if (IsMillisecondFormatStr(arg)) {
#ifdef HAVE_INT64_TIMESTAMP
        val = fsec / 1000.0;
#else
        val = fsec * 1000;
#endif
    } else if (IsMicrosecondFormatStr(arg)) {
#ifdef HAVE_INT64_TIMESTAMP
        val = fsec;
#else
        val = fsec * MAX_MICRO_SECOND;
#endif
    } else if (IsNanosecondFormatStr(arg)) {
#ifdef HAVE_INT64_TIMESTAMP
        val = fsec * 1000;
#else
        val = fsec * MAX_NANO_SECOND;
#endif
    } else if (IsTzoffsetFormatStr(arg)) {
        val = 0;
        isNull = false;
        if (isTz) {
            int tzVal;
            if (tz < 0) {
                tzVal = -(tz/MINS_PER_HOUR);
                ret = sprintf_s(result, charLen, "+%02d:%02d", tzVal/MINS_PER_HOUR, tzVal%MINS_PER_HOUR);
            } else {
                tzVal = tz/MINS_PER_HOUR;
                ret = sprintf_s(result, charLen, "-%02d:%02d", tzVal/MINS_PER_HOUR, tzVal%MINS_PER_HOUR);
            }
        } else {
            ret = sprintf_s(result, charLen, "%s", "+00;00");
        }
        securec_check_ss(ret, "", "");
    } else if (IsIsoweekFormatStr(arg)) {
        val = (float8)date2isoweek(tm->tm_year, tm->tm_mon, tm->tm_mday);
    } else {
        pfree(result);
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
            errmsg("unrecognized role option \"%s\"", arg)));
    }

    if (isNull) {
        ret = sprintf_s(result, charLen, "%d", val);
        securec_check_ss(ret, "", "");
    }
    return result;
}

Datum datenametimestamp(PG_FUNCTION_ARGS)
{
    Timestamp timestampVal = PG_GETARG_TIMESTAMP(1);
    VarChar* result = NULL;
    char* args = PG_GETARG_CSTRING(0);
    struct pg_tm tt;
    struct pg_tm *tm = &tt;
    fsec_t fsec = 0;
  
    if (timestamp2tm(timestampVal, NULL, tm, &fsec, NULL, NULL) != 0) {
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
            errmsg("timestamp out of range")));
    }
    char* res = get_date_name_by_string(args, tm, 0, fsec, false);
    if (res != NULL) {
        result = cstring_to_text(res);
    }
    PG_RETURN_VARCHAR_P(result);
}

Datum datenametimestamptz(PG_FUNCTION_ARGS)
{
    TimestampTz timestampVal = PG_GETARG_TIMESTAMPTZ(1);
    VarChar* result = NULL;
    char* args = PG_GETARG_CSTRING(0);
    struct pg_tm tm = {0};
    fsec_t fsec = 0;
    int tz = 0;
  
    if (timestamp2tm(timestampVal, &tz, &tm, &fsec, NULL, NULL) != 0) {
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
            errmsg("timestamp out of range")));
    }
    char* res = get_date_name_by_string(args, &tm, tz, fsec, true);
    if (res != NULL) {
        result = cstring_to_text(res);
    }
    PG_RETURN_VARCHAR_P(result);
}

Datum datenamedate(PG_FUNCTION_ARGS)
{
    Timestamp timestampVal;
    VarChar* result = NULL;
    DateADT dateVal = PG_GETARG_DATEADT(1);
    timestampVal = date2timestamp(dateVal);
    char* args = PG_GETARG_CSTRING(0);
    struct pg_tm tt;
    struct pg_tm *tm = &tt;
    fsec_t fsec = 0;
 
    timestamp2tm(timestampVal, NULL, tm, &fsec, NULL, NULL);
    if (!IS_VALID_JULIAN(tm->tm_year, tm->tm_mon, tm->tm_mday)) {
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
            errmsg("timestamp out of range")));
    }
    char* res = get_date_name_by_string(args, tm, 0, fsec, false);
    if (res != NULL) {
        result = cstring_to_text(res);
    }
    PG_RETURN_VARCHAR_P(result);
}

Datum datenametime(PG_FUNCTION_ARGS)
{
    Timestamp timestampVal;
    VarChar* result = NULL;
    TimeADT time = PG_GETARG_TIMEADT(1);
    struct pg_tm tt;
    struct pg_tm *tm = &tt;
    fsec_t fsec;
    time2tm(time, tm, &fsec);
    tm->tm_mday = 1;
    tm->tm_mon = 1;
    tm->tm_year = 1900;
    char* args = PG_GETARG_CSTRING(0);

    if (tm2timestamp(tm, fsec, NULL, &timestampVal) != 0) {
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
            errmsg("timestamp out of range")));
    }
    char *res = get_date_name_by_string(args, tm, 0, fsec, false);
    if (res != NULL) {
        result = cstring_to_text(res);
    }
    PG_RETURN_VARCHAR_P(result);
}

Datum datenametimetz(PG_FUNCTION_ARGS)
{
    TimestampTz timestampVal;
    VarChar* result = NULL;
    TimeTzADT* time = PG_GETARG_TIMETZADT_P(1);
    struct pg_tm tt;
    struct pg_tm *tm = &tt;
    fsec_t fsec;
    int tz = 0;
    timetz2tm(time, tm, &fsec, &tz);
    tm->tm_mday = 1;
    tm->tm_mon = 1;
    tm->tm_year = 1900;
    char* args = PG_GETARG_CSTRING(0);

    if (tm2timestamp(tm, fsec, &tz, &timestampVal) != 0) {
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
            errmsg("timestamp out of range")));
    }
    char *res = get_date_name_by_string(args, tm, tz, fsec, false);
    if (res != NULL) {
        result = cstring_to_text(res);
    }
    PG_RETURN_VARCHAR_P(result);
}
