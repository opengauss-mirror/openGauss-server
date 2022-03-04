/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
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
 * gs_job_calendar.cpp
 *    Calendaring syntax for dbe jobs.
 *
 * IDENTIFICATION
 *    src/gausskernel/process/job/gs_job_calendar.cpp
 *
 * -------------------------------------------------------------------------
 */

 #include "postgres.h"
 #include "miscadmin.h"
 #include "utils/builtins.h"
 #include "utils/dbe_scheduler.h"

/*
 *  repeat_interval = frequency_clause
 *  [; interval=?] [; bymonth=?] [; byweekno=?]
 *  [; byyearday=?] [; bymonthday=?] [; byday=?]
 *  [; byhour=?] [; byminute=?] [; bysecond=?]
 *
 *  frequency_clause = "FREQ" "=" frequency
 *  frequency = "YEARLY" | "MONTHLY" | "WEEKLY" | "DAILY" |
 *  "HOURLY" | "MINUTELY" | "SECONDLY"
 *
 * Note:
 *  POSIX time (tm) used in this section has following flags:
 *  int    tm_sec     Seconds [0,60]. (with leap second!!)
 *  int    tm_min     Minutes [0,59].
 *  int    tm_hour    Hour [0,23].
 *  int    tm_mday    Day of month [1,31].
 *  int    tm_mon     Month of year [0,11]. (not ISO standard!! [1,12])
 *  int    tm_year    Years since 1900.
 *  int    tm_wday    Day of week [0,6] (Sunday =0). (not ISO standard!! sun =7)
 *  int    tm_yday    Day of year [0,365]. (not ISO standard!! [1,366])
 *  int    tm_isdst   Daylight Savings flag.
 *
 */

/* Initialize calendaring fields */
static bool IsLegalIntervalStr(const char* str, bool numeric_only = false);
static char *get_calendar_clause_val(char **tokens, const char *clause, bool numeric_only = false);
static char **tokenize_str(char *src, const char *delims, int fields);
static int validate_field_names(char **toks);

/*
 * Interpreter functions
 * Interpret calendaring syntax.
 */
static bool get_calendar_freqency(Calendar calendar, char **tokens);
static void get_calendar_n_interval(Calendar calendar, char **tokens);
static char *get_calendar_bymonth_val(Calendar calendar, char **tokens);
static void get_calendar_bymonth(Calendar calendar, char **tokens);
static void get_calendar_byweekno(Calendar calendar, char **tokens); /* unsupported */
static void get_calendar_byyearday(Calendar calendar, char **tokens); /* unsupported */
static void get_calendar_bydate(Calendar calendar, char **tokens);  /* unsupported */
static char *get_calendar_bymonthday_val(Calendar calendar, char **tokens);
static void get_calendar_bymonthday(Calendar calendar, char **tokens);
static void get_calendar_byday(Calendar calendar, char **tokens); /* unsupported */
static char *get_calendar_byhour_val(Calendar calendar, char **tokens);
static void get_calendar_byhour(Calendar calendar, char **tokens);
static char *get_calendar_byminute_val(Calendar calendar, char **tokens);
static void get_calendar_byminute(Calendar calendar, char **tokens);
static char *get_calendar_bysecond_val(Calendar calendar, char **tokens);
static void get_calendar_bysecond(Calendar calendar, char **tokens);
Calendar interpret_calendar_interval(char *calendar_str); /* interpreter main */

/*
 * Evaluation functions
 * Calculate calendaring interval.
 */
static Interval *get_calendar_period(Calendar calendar, int num_of_period = 1);
static void copy_calendar_dates(TimestampTz *timeline, int nvals, int cnt);
static bool validate_calendar_monthday(int year, int month, int mday);
static void fastforward_calendar_period(Calendar calendar, TimestampTz *start_date, TimestampTz date_after);
static bool recheck_calendar_period(Calendar calendar, TimestampTz start_date, TimestampTz next_date);
static int timestamp_cmp_func(const void *dt1, const void *dt2);
static TimestampTz get_next_calendar_period(Calendar calendar, TimestampTz base_date);
static bool find_nearest_calendar_time(Calendar calendar, TimestampTz *timeline, TimestampTz start, int cnt,
                                       TimestampTz *nearest);

/* Calendaring Interval Calculator */
static void prepare_calendar_period(Calendar calendar, TimestampTz base_date, TimestampTz *timeline);
static void evaluate_calendar_bymonth(Calendar calendar, TimestampTz *timeline, int *cnt);
static void evaluate_calendar_byweekno(Calendar calendar, TimestampTz *timeline, int *cnt); /* unsupported */
static void evaluate_calendar_byyearday(Calendar calendar, TimestampTz *timeline, int *cnt); /* unsupported */
static void evaluate_calendar_bymonthday(Calendar calendar, TimestampTz *timeline, int *cnt);
static void evaluate_calendar_byhour(Calendar calendar, TimestampTz *timeline, int *cnt); /* sub_timeline */
static void evaluate_calendar_byminute(Calendar calendar, TimestampTz *timeline, int *cnt); /* sub_timeline */
static void evaluate_calendar_bysecond(Calendar calendar, TimestampTz *timeline, int *cnt); /* sub_timeline */
static bool evaluate_calendar_period(Calendar calendar, TimestampTz *timeline, TimestampTz *sub_timeline,
                                     TimestampTz start_date, TimestampTz *next_date);
static TimestampTz evaluate_calendar_interval(Calendar calendar, TimestampTz start_date); /* evaluate main */

/*
 * @brief IsLegalIntervalStr
 *  Is interval legal?
 * @param str
 * @param numeric_only
 * @return true             legal
 * @return false            illegal
 */
static bool IsLegalIntervalStr(const char* str, bool numeric_only)
{
    size_t NBytes = (unsigned int)strlen(str);
    if (NBytes > (MAX_CALENDAR_FIELD_LEN)) {
        return false;
    }

    /* numeric input recognize comma, space and minus sign */
    if (numeric_only) {
        for (size_t i = 0; i < NBytes; i++) {
            if (!isdigit(str[i]) && str[i] != ',' && str[i] != ' ' && str[i] != '-') {
                return false;
            }
        }
        return true;
    }

    for (size_t i = 0; i < NBytes; i++) {
        /* check whether the character is correct */
        if (IsIllegalIntervalCharacter(str[i])) {
            return false;
        }
    }
    return true;
}


/*
 * @brief get_calendar_clause
 *  Get calendar clause and return its value;
 * @param tokens    calendar interval tokens
 * @param clause    clause name
 * @return char*    value
 */
static char *get_calendar_clause_val(char **tokens, const char *clause, bool numeric_only)
{
    char *val = NULL;
    for (int i = 0; i < MAX_CALENDAR_FIELDS; i += 2) {
        if (tokens[i] != NULL && pg_strcasecmp(tokens[i], clause) == 0) {
            val = tokens[i + 1];    /* get clause's value */
            break;
        }
    }
    if (val == NULL) {
        return NULL;
    }

    if (!IsLegalIntervalStr(val, numeric_only)) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_OPERATE_FAILED),
                        errmsg("Fail to evaluate calendaring string."),
                        errdetail("Invalid value string for clause \'%s\'", clause), errcause("N/A"),
                        erraction("Please modify the calendaring string.")));
    }
    return val;
}

/*
 * @brief get_calendar_freqency
 *  Get frequency_clause value.
 *  frequency_clause = "FREQ" "=" ( predefined_frequency | user_defined_frequency )
 *  predefined_frequency = "YEARLY" | "MONTHLY" | "WEEKLY" | "DAILY" | "HOURLY" | "MINUTELY" | "SECONDLY"
 *  user_defined_frequency = named_schedule
 * @param calendar
 * @param tokens
 * @return true             clause exists
 * @return false            clause absent
 */
static bool get_calendar_freqency(Calendar calendar, char **tokens)
{
    char *val = get_calendar_clause_val(tokens, "freq", false);
    if (val == NULL) {
        return false;
    }

    if (pg_strcasecmp(val, "yearly") == 0) {
        calendar->frequency = YEARLY;
    } else if (pg_strcasecmp(val, "monthly") == 0) {
        calendar->frequency = MONTHLY;
    } else if (pg_strcasecmp(val, "weekly") == 0) {
        calendar->frequency = WEEKLY;
    } else if (pg_strcasecmp(val, "daily") == 0) {
        calendar->frequency = DAILY;
    } else if (pg_strcasecmp(val, "hourly") == 0) {
        calendar->frequency = HOURLY;
    } else if (pg_strcasecmp(val, "minutely") == 0) {
        calendar->frequency = MINUTELY;
    } else if (pg_strcasecmp(val, "secondly") == 0) {
        calendar->frequency = SECONDLY;
    } else {
        pfree_ext(tokens);
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_OPERATE_FAILED),
                        errmsg("Fail to evaluate calendaring string."),
                        errdetail("Invalid frequency value \'%s\'.", val), errcause("N/A"),
                        erraction("Please modify the calendaring string.")));
    }
    return true;
}

/*
 * @brief get_calendar_n_interval
 *  Get interval_clause.
 *  interval_clause = "INTERVAL" "=" intervalnum
 *  intervalnum = 1 through 99
 * @param calendar
 * @param tokens
 */
static void get_calendar_n_interval(Calendar calendar, char **tokens)
{
    calendar->interval = 1;     /* we ALWAYS set interval to 1 */
    char *val = get_calendar_clause_val(tokens, "interval", true);
    if (val == NULL) {
        return;
    }

    int num = atoi(val);
    if (num < 1 || num > MAX_CALENDAR_INTERVAL_NUM) {
        pfree_ext(tokens);
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_OPERATE_FAILED),
                        errmsg("Fail to evaluate calendaring string."),
                        errdetail("Interval \'%d\' not in range [1, 99].", num), errcause("N/A"),
                        erraction("Please modify the calendaring string.")));
    }
    calendar->interval = num;
}

static char *get_calendar_bymonth_val(Calendar calendar, char **tokens)
{
    char *val = get_calendar_clause_val(tokens, "bymonth", false);
    if (val != NULL) {
        /* apply bymonth rule if bymonth is specified */
        return val;
    }

    if (calendar->frequency > MONTHLY) {
        /* If we have higher frequency, every month is possible */
        for (int i = 0; i < MONTHS_PER_YEAR; i++) {
            calendar->bymonth[i] = i + 1;
        }
        calendar->month_len = MONTHS_PER_YEAR;
        calendar->date_depth *= calendar->month_len;
    } else if (calendar->frequency < MONTHLY) {
        /* If we have lower frquency, sync with start date value, fill it later */
        calendar->month_len = 0;
    } else {
        /* Frequency is MONTHLY, try optimize */
        int mod = (calendar->interval >= MONTHS_PER_YEAR) ? calendar->interval % MONTHS_PER_YEAR : calendar->interval;
        if (mod == 0) {
            mod = MONTHS_PER_YEAR;
        }
        /* We need the start month to figure out the ACTUAL month list, set it to negative and deal with it later */
        calendar->month_len = (MONTHS_PER_YEAR % mod == 0) ? MONTHS_PER_YEAR / mod : MONTHS_PER_YEAR;
        calendar->date_depth *= calendar->month_len;
        calendar->month_len *= -1;
        calendar->bymonth[0] = mod;
    }
    return NULL;
}

/*
 * @brief get_calendar_bymonth
 *  Get bymonth_clause.
 *  bymonth_clause = "BYMONTH" "=" monthlist
 *  monthlist = month ( "," month)*
 *  month = numeric_month | char_month
 *  numeric_month = 1 | 2 | 3 ...  12
 *  char_month = "JAN" | "FEB" | "MAR" | "APR" | "MAY" | "JUN" | "JUL" | "AUG" | "SEP" | "OCT" | "NOV" | "DEC"
 * @param calendar
 * @param tokens
 */
static void get_calendar_bymonth(Calendar calendar, char **tokens)
{
    const char *month_str[] = {"JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"};
    char *val = get_calendar_bymonth_val(calendar, tokens);
    if (val == NULL) {
        return;
    }

    char *context = NULL;
    char *tok = strtok_s(val, ",", &context);
    bool used[MONTHS_PER_YEAR] = {0};
    while (tok != NULL) {
        int month = 0;
        if (isalpha(tok[0])) {
            /* char in */
            for (int j = 0; j < MONTHS_PER_YEAR; j++) {
                if (pg_strcasecmp(val, month_str[j]) == 0) {
                    month = j + 1;
                    break;
                }
            }
            if (month == 0) {
                ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_OPERATE_FAILED),
                                errmsg("Fail to evaluate calendaring string."),
                                errdetail("Invalid month token \'%s\'.", val), errcause("N/A"),
                                erraction("Please modify the calendaring string.")));
            }
        } else {
            /* numeric in */
            month = atoi(tok);
            if (month < 1 || month > MONTHS_PER_YEAR) {
                ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_OPERATE_FAILED),
                                errmsg("Fail to evaluate calendaring string."),
                                errdetail("Interval \'%d\' not in range [1, 12].", month), errcause("N/A"),
                                erraction("Please modify the calendaring string.")));
            }
        }
        tok = strtok_s(NULL, ",", &context);

        if (used[month - 1]) {
            continue;
        }
        used[month - 1] = true;
    }
    for (int i = 0; i < MONTHS_PER_YEAR; i++) {
        if (used[i]) {
            calendar->bymonth[calendar->month_len] = i + 1;
            calendar->month_len++;
        }
    }
    calendar->date_depth *= (calendar->month_len == 0) ? 1 : calendar->month_len;
    calendar->byfields |= INTERVAL_BYMONTH;
}

/*
 * @brief get_calendar_byweekno
 *  Get byweekno_clause.
 *  byweekno_clause = "BYWEEKNO" "=" weeknumber_list
 *  weeknumber_list = weeknumber ( "," weeknumber)*
 *  weeknumber = [minus] weekno
 *  weekno = 1 through 53
 * @param calendar
 * @param tokens
 */
static void get_calendar_byweekno(Calendar calendar, char **tokens)
{
    char *val = get_calendar_clause_val(tokens, "byweekno", true);
    if (val != NULL) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_OPERATE_FAILED),
                    errmsg("Fail to evaluate calendaring string."),
                    errdetail("BYWEEKNO clause is currently unsupported."), errcause("N/A"),
                    erraction("Please modify the calendaring string.")));
    }
}

/*
 * @brief get_calendar_byyearday
 *  Get byyearday_clause.
 *  byyearday_clause = "BYYEARDAY" "=" yearday_list
 *  yearday_list = yearday ( "," yearday)*
 *  yearday = [minus] yeardaynum
 *  yeardaynum = 1 through 366
 * @param calendar
 * @param tokens
 */
static void get_calendar_byyearday(Calendar calendar, char **tokens)
{
    char *val = get_calendar_clause_val(tokens, "byyearday", true);
    if (val != NULL) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_OPERATE_FAILED),
                        errmsg("Fail to evaluate calendaring string."),
                        errdetail("BYYEARDAY clause is currently unsupported."), errcause("N/A"),
                        erraction("Please modify the calendaring string.")));
    }
}

/*
 * @brief get_calendar_bydate
 *  Get bydate_clause.
 *  bydate_clause = "BYDATE" "=" date_list
 *  date_list = date ( "," date)*
 *  date = [YYYY]MMDD [ offset | span ]
 * @param calendar
 * @param tokens
 */
static void get_calendar_bydate(Calendar calendar, char **tokens)
{
    char *val = get_calendar_clause_val(tokens, "byweekno", true);
    if (val != NULL) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_OPERATE_FAILED),
                        errmsg("Fail to evaluate calendaring string."),
                        errdetail("BYDATE clause is currently unsupported."), errcause("N/A"),
                        erraction("Please modify the calendaring string.")));
    }
}


static char *get_calendar_bymonthday_val(Calendar calendar, char **tokens)
{
    char *val = get_calendar_clause_val(tokens, "bymonthday", true);
    if (val != NULL) {
        /* apply bymonthday rule if bymonthday is specified */
        return val;
    }

    if (calendar->frequency >= DAILY || calendar->frequency == WEEKLY) {
        /* If we have higher frquency, every day is possible */
        for (int i = 0; i < DAYS_PER_MONTH + 1; i++) {
            calendar->bymonthday[i] = i + 1;
        }
        calendar->monthday_len = DAYS_PER_MONTH + 1;
        calendar->date_depth *= calendar->monthday_len;
    } else {
        /* If we have lower frquency, sync with start date value, fill it later */
        calendar->monthday_len = 0;
    }
    /* We cannot optimize any further since monthday/yearday are not perfectly periodic */
    return NULL;
}


/*
 * @brief get_calendar_bymonth
 *  Get bymonthday_clause.
 *  bymonthday_clause = "BYMONTHDAY" "=" monthday_list
 *  monthday_list = monthday ( "," monthday)*
 *  monthday = [minus] monthdaynum
 *  monthdaynum = 1 through 31
 * @param calendar
 * @param tokens
 */
static void get_calendar_bymonthday(Calendar calendar, char **tokens)
{
    char *val = get_calendar_bymonthday_val(calendar, tokens);
    if (val == NULL) {
        return;
    }

    char *context = NULL;
    char *tok = strtok_s(val, ",", &context);
    bool used_p[DAYS_PER_MONTH + 1] = {0};
    bool used_n[DAYS_PER_MONTH + 1] = {0};
    while (tok != NULL) {
        int monthday = atoi(tok);
        if (monthday < -(DAYS_PER_MONTH + 1) || monthday > (DAYS_PER_MONTH + 1) || monthday == 0) {
            ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_OPERATE_FAILED),
                            errmsg("Fail to evaluate calendaring string."),
                            errdetail("Invalid monthday \'%d\'.", monthday), errcause("N/A"),
                            erraction("Please modify the calendaring string.")));
        }
        tok = strtok_s(NULL, ",", &context);

        /* considering both positive and negative parts */
        if (monthday > 0 && used_p[monthday - 1]) {
            continue;
        }
        if (monthday < 0 && used_n[(-monthday) - 1]) {
            continue;
        }

        if (monthday > 0) {
            used_p[monthday - 1] = true;
        }
        if (monthday < 0) {
            used_n[(-monthday) - 1] = true;
        }
    }
    /* there is no point of making two seperate loops for all monthdays, may refactor later. */
    for (int i = 0; i <= DAYS_PER_MONTH; i++) {
        if (used_p[i]) {
            calendar->bymonthday[calendar->monthday_len] = i + 1;
            calendar->monthday_len++;
        }
    }
    for (int i = DAYS_PER_MONTH; i >= 0; i--) {
        if (used_n[i]) {
            calendar->bymonthday[calendar->monthday_len] = -(i + 1);
            calendar->monthday_len++;
        }
    }
    calendar->date_depth *= (calendar->monthday_len == 0) ? 1 : calendar->monthday_len;
    calendar->byfields |= INTERVAL_BYMONTHDAY;
}

/*
 * @brief get_calendar_byday
 *  Get byday_clause.
 *  byday_clause = "BYDAY" "=" byday_list
 *  byday_list = byday ( "," byday)*
 *  byday = [weekdaynum] day
 *  weekdaynum = [minus] daynum
 *  daynum = 1 through 53 -- yearly
 *  daynum = 1 through 5  -- monthly
 *  day = "MON" | "TUE" | "WED" | "THU" | "FRI" | "SAT" | "SUN"
 * @param calendar
 * @param tokens
 */
static void get_calendar_byday(Calendar calendar, char **tokens)
{
    char *val = get_calendar_clause_val(tokens, "byday", true);
    if (val != NULL) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_OPERATE_FAILED),
                        errmsg("Fail to evaluate calendaring string."),
                        errdetail("BYDAY clause is currently unsupported."), errcause("N/A"),
                        erraction("Please modify the calendaring string.")));
    }
}


static char *get_calendar_byhour_val(Calendar calendar, char **tokens)
{
    char *val = get_calendar_clause_val(tokens, "byhour", true);
    if (val != NULL) {
        /* apply byhour rule if byhour is specified */
        return val;
    }

    if (calendar->frequency > HOURLY) {
        /* If we have higher frequency, every hour is possible */
        for (int i = 0; i < HOURS_PER_DAY; i++) {
            calendar->byhour[i] = i;
        }
        calendar->hour_len = HOURS_PER_DAY;
        calendar->time_depth *= calendar->hour_len;
    } else if (calendar->frequency < HOURLY) {
        /* If we have lower frequency, sync with start date value, fill it later */
        calendar->hour_len = 0;
    } else {
        /* frequency matched, try optimize */
        int mod = (calendar->interval >= HOURS_PER_DAY) ? calendar->interval % HOURS_PER_DAY : calendar->interval;
        if (mod == 0) {
            mod = HOURS_PER_DAY;
        }
        /* We need the start hour to figure out the ACTUAL hour list, set it to negative and deal with it later */
        calendar->hour_len = (HOURS_PER_DAY % mod == 0) ? HOURS_PER_DAY / mod : HOURS_PER_DAY;
        calendar->time_depth *= calendar->hour_len;
        calendar->hour_len *= -1;
        calendar->byhour[0] = mod;
    }
    return NULL;
}

/*
 * @brief get_calendar_byhour
 *  Get byhour_clause.
 *  byhour_clause = "BYHOUR" "=" hour_list
 *  hour_list = hour ( "," hour)*
 *  hour = 0 through 23
 * @param calendar
 * @param tokens
 */
static void get_calendar_byhour(Calendar calendar, char **tokens)
{
    char *val = get_calendar_byhour_val(calendar, tokens);
    if (val == NULL) {
        return;
    }

    char *context = NULL;
    char *tok = strtok_s(val, ",", &context);
    bool used[HOURS_PER_DAY] = {0};
    while (tok != NULL) {
        int hour = atoi(tok);
        if (hour < 0 || hour >= HOURS_PER_DAY) {
            ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_OPERATE_FAILED),
                            errmsg("Fail to evaluate calendaring string."),
                            errdetail("Invalid time \'%d\' o\' clock.", hour), errcause("N/A"),
                            erraction("Please modify the calendaring string.")));
        }
        tok = strtok_s(NULL, ",", &context);

        if (used[hour]) {
            continue;
        }
        used[hour] = true;
    }
    for (int i = 0; i < HOURS_PER_DAY; i++) {
        if (used[i]) {
            calendar->byhour[calendar->hour_len] = i;
            calendar->hour_len++;
        }
    }
    calendar->time_depth *= (calendar->hour_len == 0) ? 1 : calendar->hour_len;
    calendar->byfields |= INTERVAL_BYHOUR;
}


static char *get_calendar_byminute_val(Calendar calendar, char **tokens)
{
    char *val = get_calendar_clause_val(tokens, "byminute", true);
    if (val != NULL) {
        /* apply byminute rule if byminute is specified */
        return val;
    }

    if (calendar->frequency > MINUTELY) {
        /* If we have higher frequency, every minute is possible */
        for (int i = 0; i < MINS_PER_HOUR; i++) {
            calendar->byminute[i] = i;
        }
        calendar->minute_len = MINS_PER_HOUR;
        calendar->time_depth *= calendar->minute_len;
    } else if (calendar->frequency < MINUTELY) {
        /* If we have lower frequency, sync with start date value, fill it later */
        calendar->minute_len = 0;
    } else {
        /* frequency matched, try optimize */
        int mod = (calendar->interval >= MINS_PER_HOUR) ? calendar->interval % MINS_PER_HOUR : calendar->interval;
        if (mod == 0) {
            mod = MINS_PER_HOUR;
        }
        /* We need the start minute to figure out the ACTUAL minute list, set it to negative and deal with it later */
        calendar->minute_len = (MINS_PER_HOUR % mod == 0) ? MINS_PER_HOUR / mod : MINS_PER_HOUR;
        calendar->time_depth *= calendar->minute_len;
        calendar->minute_len *= -1;
        calendar->byminute[0] = mod;
    }
    return NULL;
}

/*
 * @brief get_calendar_byminute
 *  Get byminute_clause.
 *  byminute_clause = "BYMINUTE" "=" minute_list
 *  minute_list = minute ( "," minute)*
 *  minute = 0 through 59
 * @param calendar
 * @param tokens
 */
static void get_calendar_byminute(Calendar calendar, char **tokens)
{
    char *val = get_calendar_byminute_val(calendar, tokens);
    if (val == NULL) {
        return;
    }

    char *context = NULL;
    char *tok = strtok_s(val, ",", &context);
    bool used[MINS_PER_HOUR] = {0};
    while (tok != NULL) {
        int minute = atoi(tok);
        if (minute < 0 || minute >= MINS_PER_HOUR) {
            ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_OPERATE_FAILED),
                            errmsg("Fail to evaluate calendaring string."),
                            errdetail("Invalid time \'%d\' minute [0, 59].", minute), errcause("N/A"),
                            erraction("Please modify the calendaring string.")));
        }
        tok = strtok_s(NULL, ",", &context);

        if (used[minute]) {
            continue;
        }
        used[minute] = true;
    }
    for (int i = 0; i < MINS_PER_HOUR; i++) {
        if (used[i]) {
            calendar->byminute[calendar->minute_len] = i;
            calendar->minute_len++;
        }
    }
    calendar->time_depth *= (calendar->minute_len == 0) ? 1 : calendar->minute_len;
    calendar->byfields |= INTERVAL_BYMINUTE;
}

static char *get_calendar_bysecond_val(Calendar calendar, char **tokens)
{
    char *val = get_calendar_clause_val(tokens, "bysecond", true);
    if (val != NULL) {
        /* apply bysecond rule if bysecond is specified */
        return val;
    }

    Assert(calendar->frequency <= SECONDLY);
    /* Even higher frequency is unavailable */
    if (calendar->frequency < SECONDLY) {
        /* If we have lower frequency, sync with start date value, fill it later */
        calendar->second_len = 0;
    } else {
        /* frequency matched, try optimize */
        int mod = (calendar->interval >= SECS_PER_MINUTE) ? calendar->interval % SECS_PER_MINUTE : calendar->interval;
        if (mod == 0) {
            mod = SECS_PER_MINUTE;
        }
        /* We need the start second to figure out the ACTUAL second list, set it to negative and deal with it later */
        calendar->second_len = (SECS_PER_MINUTE % mod == 0) ? SECS_PER_MINUTE / mod : SECS_PER_MINUTE;
        calendar->time_depth *= calendar->second_len;
        calendar->second_len *= -1;
        calendar->bysecond[0] = mod;
    }
    return NULL;
}

/*
 * @brief get_calendar_bysecond
 *  Get bysecond_clause.
 *  bysecond_clause = "BYSECOND" "=" second_list
 *  second_list = second ( "," second)*
 *  second = 0 through 59
 * @param interval
 * @param tokens
 */
static void get_calendar_bysecond(Calendar calendar, char **tokens)
{
    char *val = get_calendar_bysecond_val(calendar, tokens);
    if (val == NULL) {
        return;
    }

    char *context = NULL;
    char *tok = strtok_s(val, ",", &context);
    bool used[SECS_PER_MINUTE] = {0};
    while (tok != NULL) {
        int second = atoi(tok);
        if (second < 0 || second >= SECS_PER_MINUTE) {
            ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_OPERATE_FAILED),
                            errmsg("Fail to evaluate calendaring string."),
                            errdetail("Invalid time \'%d\' seconds [0, 59].", second), errcause("N/A"),
                            erraction("Please modify the calendaring string.")));
        }
        tok = strtok_s(NULL, ",", &context);

        if (used[second]) {
            continue;
        }
        used[second] = true;
    }
    for (int i = 0; i < SECS_PER_MINUTE; i++) {
        if (used[i]) {
            calendar->bysecond[calendar->second_len] = i;
            calendar->second_len++;
        }
    }
    calendar->time_depth *= (calendar->second_len == 0) ? 1 : calendar->second_len;
    calendar->byfields |= INTERVAL_BYSECOND;
}


/*
 * @brief tokenize_str
 *  Tokenize string with given delimiters.
 * @param src       source string
 * @param delims    delimiters
 * @param fields    number of tokens needed
 * @return char**   an array of tokens generated
 */
static char **tokenize_str(char *src, const char *delims, int fields)
{
    char **tokens = (char **)palloc0(sizeof(char *) * fields);
    int count = 0;
    char *context = NULL;
    char *tok = strtok_s(src, delims, &context);
    while (tok != NULL) {
        tokens[count] = tok;
        tok = strtok_s(NULL, delims, &context);
        count++;
        if (count >= fields) {
            pfree_ext(tokens);
            return NULL;
        }
    }
    return tokens;
}

static int validate_field_names(char **toks)
{
    bool valid = false;
    const int name_pos_step = 2;
    const char *supported_fields[SUPPORTED_FIELDS] = {"freq", "interval", "bymonth", "bymonthday", "byhour",
                                                      "byminute", "bysecond"};
    bool fields_used[SUPPORTED_FIELDS] = {0};
    for (int i = 0; i < MAX_CALENDAR_FIELDS; i += name_pos_step) {
        for (int j = 0; j < SUPPORTED_FIELDS; j++) {
            if (toks[i] == NULL) {
                /* This is rare, usually token is not empty in the middle */
                valid = true;
                break;
            }
            if (pg_strcasecmp(toks[i], supported_fields[j]) != 0) {
                continue;
            }
            if (fields_used[j]) {
                /* duplicate field name */
                valid = false;
            } else {
                fields_used[j] = true;
                valid = true;
            }
            break;  /* here is way pass guarding condition, break it */
        }
        if (!valid) {
            return i;
        }
        valid = false;
    }
    return -1;
}

/*
 * @brief interpret_calendar_interval
 *  The main interpreter of calendar interval.
 * @param interval_str
 * @return Calendar
 */
Calendar interpret_calendar_interval(char *calendar_str)
{
    Assert(calendar_str != NULL);
    bool field = false;

    /* Make token lists */
    char **str_toks = tokenize_str(calendar_str, " =;", MAX_CALENDAR_FIELDS);
    if (str_toks == NULL) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_OPERATE_FAILED),
                        errmsg("Fail to evaluate calendaring string."),
                        errdetail("Unable to parse calendaring string."),
                        errcause("Calendaring string is too long/invalid"),
                        erraction("Please modify the calendaring string.")));
    }

    int pos = validate_field_names(str_toks);
    if (pos >= 0) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_OPERATE_FAILED),
                        errmsg("Fail to evaluate calendaring string."),
                        errdetail("Incorrect/duplicate clause name '%s'.", str_toks[pos]), errcause("N/A"),
                        erraction("Please modify the calendaring string.")));
    }

    /* Make Calendar */
    Calendar calendar = (Calendar)palloc0(sizeof(CalendarContext));

    /* Main interpreter */
    field = get_calendar_freqency(calendar, str_toks);
    if (!field || pg_strcasecmp(str_toks[0], "freq") != 0) {
        /*
         * Return NULL if frequency clause is missing(or not the first),
         * possibly not a calendaring syntax
         */
        pfree_ext(calendar);
        return NULL;
    }
    get_calendar_n_interval(calendar, str_toks);

    /* by*** clauses */
    calendar->date_depth = 1;
    calendar->time_depth = 1;
    get_calendar_bymonth(calendar, str_toks);
    get_calendar_byweekno(calendar, str_toks);
    get_calendar_byyearday(calendar, str_toks);
    get_calendar_bydate(calendar, str_toks);
    get_calendar_bymonthday(calendar, str_toks);
    get_calendar_byday(calendar, str_toks);
    get_calendar_byhour(calendar, str_toks);
    get_calendar_byminute(calendar, str_toks);
    get_calendar_bysecond(calendar, str_toks);

    pfree_ext(str_toks);
    return calendar;
}

/*
 * @brief get_calendar_period
 *  Get the calendar period in the form of Interval.
 * @param calendar
 * @param num_of_period
 * @return Interval*
 */
static Interval *get_calendar_period(Calendar calendar, int num_of_period)
{
    const char *freq_str = NULL;
    if (calendar->frequency == YEARLY) {
        freq_str = "years";
    } else if (calendar->frequency == MONTHLY) {
        freq_str = "months";
    } else if (calendar->frequency == WEEKLY) {
        freq_str = "weeks";
    } else if (calendar->frequency == DAILY) {
        freq_str = "days";
    } else if (calendar->frequency == HOURLY) {
        freq_str = "hours";
    } else if (calendar->frequency == MINUTELY) {
        freq_str = "minutes";
    } else if (calendar->frequency == SECONDLY) {
        freq_str = "seconds";
    } else {
        /* rely on upper level checks, which will eventually reach the MAX_CALENDAR_DEPTH and stop */
        return NULL;
    }

    return calendar_construct_interval(freq_str, calendar->interval * num_of_period);
}

/*
 * @brief copy_calendar_dates
 *  copy batches of timestamps.
 * @param dates     target timestamp array
 * @param nvals     number of batches
 * @param cnt       number of existing values
 * @param date_in   optional, only valid when dates is empty
 */
static void copy_calendar_dates(TimestampTz *timeline, int nvals, int cnt)
{
    /* copy nvals number of existing timestamps */
    if (nvals <= 1) {
        return;
    }
    for (int i = 0; i < cnt; i++) {
        for (int j = 1; j < nvals; j++) {
            timeline[i + (j * cnt)] = timeline[i];
        }
    }
}

/*
 * @brief validate_calendar_monthday
 *  Check if a given monthday is valid.
 * @param year
 * @param month
 * @param mday
 * @return true
 * @return false
 */
static bool validate_calendar_monthday(int year, int month, int mday)
{
    bool month_31[MONTHS_PER_YEAR] = {1, 0, 1, 0, 1, 0, 1, 1, 0, 1, 0, 1};
    if ((!month_31[month - 1] && mday > 30) || (month_31[month - 1] && mday > 31)) {
        return false;
    }
    if ((!isleap(year) && month == 2 && mday > 29) || (isleap(year) && month == 2 && mday > 28)) {
        return false;
    }
    /* monthday can be negative */
    if (!isleap(year) && month == 2 && mday < -28) {
        return false;
    }
    if (isleap(year) && month == 2 && mday < -29) {
        return false;
    }
    if (!month_31[month - 1] && mday < -30) {
        return false;
    }
    return true;
}

/*
 * @brief evaluate_calendar_bymonth
 *  Evaluate bymonth field.
 * @param calendar
 * @param dates
 * @param cnt
 */
static void evaluate_calendar_bymonth(Calendar calendar, TimestampTz *timeline, int *cnt)
{
    Assert(*cnt <= calendar->date_depth);
    if (*cnt == 0) {
        return;
    }

    Interval *interval = NULL;
    int chunk = *cnt;
    TimestampTz ref = truncate_calendar_date(CStringGetTextDatum("month"), calendar->ref_date);

    if (calendar->month_len == 0) {
        calendar->bymonth[0] = calendar->tm.tm_mon;
        calendar->month_len = 1;
    } else if (calendar->month_len < 0) {
        int mod = calendar->bymonth[0];
        int start = (calendar->tm.tm_mon - 1) - (((calendar->tm.tm_mon - 1) / mod) * mod) + 1;
        calendar->month_len = 0;
        while (start < MONTHS_PER_YEAR) {
            calendar->bymonth[calendar->month_len] = start;
            start += mod;
            calendar->month_len++;
        }
    }

    *cnt = 0;
    copy_calendar_dates(timeline, calendar->month_len, chunk);
    for (int i = 0; i < calendar->month_len; i++) {
        interval = calendar_construct_interval("months", calendar->bymonth[i] - 1);
        for (int j = i * chunk; j < (i + 1) * chunk; j++) {
            timeline[*cnt] = DatumGetTimestampTz(timestamp_pl_interval(timeline[j], interval));
            if (timestamp_cmp_internal(timeline[*cnt], ref) >= 0) {
                (*cnt)++;
            }
        }
        pfree_ext(interval);
    }
}

static void evaluate_calendar_byweekno(Calendar calendar, TimestampTz *timeline, int *cnt)
{
    return;
}

static void evaluate_calendar_byyearday(Calendar calendar, TimestampTz *timeline, int *cnt)
{
    return;
}

/*
 * @brief evaluate_calendar_bymonthday
 *  Evaluate bymonthday field.
 * @param calendar
 * @param timeline
 * @param cnt
 */
static void evaluate_calendar_bymonthday(Calendar calendar, TimestampTz *timeline, int *cnt)
{
    Assert(*cnt <= calendar->date_depth);
    if (*cnt == 0) {
        return;
    }

    int chunk = *cnt;
    Interval *interval = NULL;
    TimestampTz ref = truncate_calendar_date(CStringGetTextDatum("day"), calendar->ref_date);

    if (calendar->monthday_len == 0) {
        calendar->bymonthday[0] = calendar->tm.tm_mday;
        calendar->monthday_len = 1;
    }

    *cnt = 0;
    int tz = 0;
    fsec_t fsec;
    struct pg_tm tt, *tm = &tt;     /* POSIX time struct, see NOTE above */
    copy_calendar_dates(timeline, calendar->monthday_len, chunk);
    for (int i = 0; i < calendar->monthday_len; i++) {
        if (calendar->bymonthday[i] < 0) {
            interval = calendar_construct_interval("months", 1);
        } else {
            interval = calendar_construct_interval("days", calendar->bymonthday[i] - 1);
        }
        for (int j = i * chunk; j < (i + 1) * chunk; j++) {
            if (timestamp2tm(timeline[j], &tz, tm, &fsec, NULL, NULL) != 0) {
                ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                                errmsg("Cannot evaluate calendar clause."), errdetail("Invalid timeline."),
                                errcause("N/A"), erraction("Please modify the calendaring string.")));
            }
            if (!validate_calendar_monthday(tm->tm_year, tm->tm_mon, calendar->bymonthday[i])) {
                /* skip if month day is invalid */
                continue;
            }
            timeline[*cnt] = DatumGetTimestampTz(timestamp_pl_interval(timeline[j], interval));
            if (calendar->bymonthday[i] <= 0) {
                pfree_ext(interval);
                interval = calendar_construct_interval("days", -(calendar->bymonthday[i]));
                timeline[*cnt] = DatumGetTimestampTz(timestamp_mi_interval(timeline[j], interval));
            }
            if (timestamp_cmp_internal(timeline[*cnt], ref) >= 0) {
                (*cnt)++;
            }
        }
        pfree_ext(interval);
    }
}

/*
 * @brief evaluate_calendar_byhour
 *  Evaluate byhour field.
 * @param calendar
 * @param timeline
 * @param cnt
 */
static void evaluate_calendar_byhour(Calendar calendar, TimestampTz *timeline, int *cnt)
{
    Assert(*cnt <= calendar->time_depth);
    if (*cnt == 0) {
        return;
    }

    Interval *interval = NULL;
    int chunk = *cnt;
    TimestampTz ref = truncate_calendar_date(CStringGetTextDatum("hour"), calendar->ref_date);

    if (calendar->hour_len == 0) {
        calendar->byhour[0] = calendar->tm.tm_hour;
        calendar->hour_len = 1;
    } else if (calendar->hour_len < 0) {
        int mod = calendar->byhour[0];
        int start = calendar->tm.tm_hour - ((calendar->tm.tm_hour / mod) * mod);
        calendar->hour_len = 0;
        while (start < HOURS_PER_DAY) {
            calendar->byhour[calendar->hour_len] = start;
            start += mod;
            calendar->hour_len++;
        }
    }

    *cnt = 0;
    copy_calendar_dates(timeline, calendar->hour_len, chunk);
    for (int i = 0; i < calendar->hour_len; i++) {
        interval = calendar_construct_interval("hours", calendar->byhour[i]);
        for (int j = i * chunk; j < (i + 1) * chunk; j++) {
            timeline[*cnt] = DatumGetTimestampTz(timestamp_pl_interval(timeline[j], interval));
            if (timestamp_cmp_internal(timeline[*cnt], ref) >= 0) {
                (*cnt)++;
            }
        }
        pfree_ext(interval);
    }
}

/*
 * @brief evaluate_calendar_byminute
 *  Evaluate byminunte field.
 * @param calendar
 * @param timeline
 * @param cnt
 */
static void evaluate_calendar_byminute(Calendar calendar, TimestampTz *timeline, int *cnt)
{
    Assert(*cnt <= calendar->time_depth);
    if (*cnt == 0) {
        return;
    }

    Interval *interval = NULL;
    int chunk = *cnt;
    TimestampTz ref = truncate_calendar_date(CStringGetTextDatum("minute"), calendar->ref_date);

    if (calendar->minute_len == 0) {
        calendar->byminute[0] = calendar->tm.tm_min;
        calendar->minute_len = 1;
    } else if (calendar->minute_len < 0) {
        int mod = calendar->byminute[0];
        int start = calendar->tm.tm_min - ((calendar->tm.tm_min / mod) * mod);
        calendar->minute_len = 0;
        while (start < MINS_PER_HOUR) {
            calendar->byminute[calendar->minute_len] = start;
            start += mod;
            calendar->minute_len++;
        }
    }

    *cnt = 0;
    copy_calendar_dates(timeline, calendar->minute_len, chunk);
    for (int i = 0; i < calendar->minute_len; i++) {
        interval = calendar_construct_interval("minutes", calendar->byminute[i]);
        for (int j = i * chunk; j < (i + 1) * chunk; j++) {
            timeline[*cnt] = DatumGetTimestampTz(timestamp_pl_interval(timeline[j], interval));
            if (timestamp_cmp_internal(timeline[*cnt], ref) >= 0) {
                (*cnt)++;
            }
        }
        pfree_ext(interval);
    }
}

/*
 * @brief evaluate_calendar_bysecond
 *  Evaluate bysecond field.
 * @param calendar
 * @param timeline
 * @param cnt
 */
static void evaluate_calendar_bysecond(Calendar calendar, TimestampTz *timeline, int *cnt)
{
    Assert(*cnt <= calendar->time_depth);
    if (*cnt == 0) {
        return;
    }

    Interval *interval = NULL;
    int chunk = *cnt;
    TimestampTz ref = truncate_calendar_date(CStringGetTextDatum("second"), calendar->ref_date);

    if (calendar->second_len == 0) {
        calendar->bysecond[0] = calendar->tm.tm_sec;
        calendar->second_len = 1;
    } else if (calendar->second_len < 0) {
        int mod = calendar->bysecond[0];
        int start = calendar->tm.tm_sec - ((calendar->tm.tm_sec / mod) * mod);
        calendar->second_len = 0;
        while (start < SECS_PER_MINUTE) {
            calendar->bysecond[calendar->second_len] = start;
            start += mod;
            calendar->second_len++;
        }
    }

    *cnt = 0;
    copy_calendar_dates(timeline, calendar->second_len, chunk);
    for (int i = 0; i < calendar->second_len; i++) {
        interval = calendar_construct_interval("seconds", calendar->bysecond[i]);
        for (int j = i * chunk; j < (i + 1) * chunk; j++) {
            timeline[*cnt] = DatumGetTimestampTz(timestamp_pl_interval(timeline[j], interval));
            if (timestamp_cmp_internal(timeline[*cnt], ref) >= 0) {
                (*cnt)++;
            }
        }
        pfree_ext(interval);
    }
}

/*
 * @brief fastforward_calendar_period
 *  Fastforward to the date right before the date after to save some computing power.
 * @param calendar
 * @param start_date
 * @param date_after
 */
static void fastforward_calendar_period(Calendar calendar, TimestampTz *start_date, TimestampTz date_after)
{
    Interval *interval = calendar_construct_interval("second", 1);
    if (timestamptz_cmp_internal(*start_date, date_after) >= 0) {
        /* No rewind, no equal */
        calendar->ref_date = DatumGetTimestampTz(timestamp_pl_interval(*start_date, interval));
        pfree_ext(interval);
        return;
    }

    TimestampTz new_start_date;
    Interval *period = get_calendar_period(calendar);
    if (period == NULL) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("Cannot evaluate calendar clause."), errdetail("Broken interval clause."),
                        errcause("N/A"), erraction("Please modify the calendaring string.")));
    }
    Datum pace_datum = DirectFunctionCall2(interval_part, CStringGetTextDatum("epoch"),
                                                          PointerGetDatum(period));
    int pace = (int)DatumGetFloat8(pace_datum);
    pfree_ext(period);

    long elapsed_sec = 0;
    int elapsed_usec = 0;
    TimestampDifference(*start_date, date_after, &elapsed_sec, &elapsed_usec);

    /* fastforward span in seconds = floor(elapsed / pace) * pace */
    int num_of_periods = (elapsed_sec / pace);
    if (num_of_periods >= 1) {
        Interval *ff_span = get_calendar_period(calendar, num_of_periods);
        if (ff_span == NULL) {
            ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("Cannot evaluate calendar clause."), errdetail("Broken interval clause."),
                    errcause("N/A"), erraction("Please modify the calendaring string.")));
        }
        new_start_date = DatumGetTimestampTz(timestamp_pl_interval(*start_date, ff_span));
        pfree_ext(ff_span);
        *start_date = new_start_date;
    }
    calendar->ref_date = DatumGetTimestampTz(timestamp_pl_interval(date_after, interval));
    pfree_ext(interval);
}

/*
 * @brief recheck_calendar_period
 *  Usually we need to double check if a given timestamp resonate with original frequency.
 * @param calendar
 * @param start_date
 * @param next_date
 * @return true         resonate
 * @return false        does not resonate
 */
static bool recheck_calendar_period(Calendar calendar, TimestampTz start_date, TimestampTz next_date)
{
    const char *field_str = NULL;
    int multiplier = 1;
    if (calendar->frequency == YEARLY) {
        field_str = "year";
    } else if (calendar->frequency == MONTHLY) {
        field_str = "month";
    } else if (calendar->frequency == WEEKLY) {
        field_str = "day";
        multiplier = DAYS_PER_WEEK;
    } else if (calendar->frequency == DAILY) {
        field_str = "day";
    } else if (calendar->frequency == HOURLY) {
        field_str = "hour";
    } else if (calendar->frequency == MINUTELY) {
        field_str = "minute";
    } else if (calendar->frequency == SECONDLY) {
        field_str = "second";
    } else {
        /* rely on upper level checks, which will eventually reach the MAX_CALENDAR_DEPTH and stop */
        return false;
    }

    start_date = truncate_calendar_date(CStringGetTextDatum(field_str), start_date);
    next_date = truncate_calendar_date(CStringGetTextDatum(field_str), next_date);

    int64 elapsed = timestamp_diff_internal(cstring_to_text(field_str), start_date, next_date, true);
    if (elapsed % (calendar->interval * multiplier) == 0) {
        return true;
    }
    return false;
}

/*
 * @brief timestamp_cmp_func
 *  Compare func for qsort
 * @param dt1
 * @param dt2
 * @return int
 */
static int timestamp_cmp_func(const void *dt1, const void *dt2)
{
    TimestampTz *t1 = (TimestampTz *)dt1;
    TimestampTz *t2 = (TimestampTz *)dt2;

    return timestamp_cmp_internal(*t1, *t2);
}

/*
 * @brief find_nearest_calendar_time
 *  Find the nearest timestamp from all valid timestamps.
 * @param calendar  context
 * @param timeline  timestamp list
 * @param start     start time
 * @param cnt       number of valid final timestamps
 * @param nearest   out value
 * @return true     found
 * @return false    not found
 */
static bool find_nearest_calendar_time(Calendar calendar, TimestampTz *timeline, TimestampTz start, int cnt,
                                       TimestampTz *nearest)
{
    /* sort all timestamp */
    qsort(timeline, cnt, sizeof(TimestampTz), timestamp_cmp_func);

    for (int i = 0; i < cnt; i++) {
        /* Must greater than ref date */
        if (timestamp_cmp_internal(timeline[i], calendar->ref_date) >= 0 &&
            recheck_calendar_period(calendar, start, timeline[i])) {
            *nearest = timeline[i];
            return true;
        }
    }
    return false;
}

/*
 * @brief evaluate_calendar_period
 *
 * @param calendar
 * @param period
 * @param start_date
 * @param next_date
 * @return true         success
 * @return false        fail
 */
static bool evaluate_calendar_period(Calendar calendar, TimestampTz *timeline, TimestampTz *sub_timeline,
                                     TimestampTz start_date, TimestampTz *next_date)
{
    int date_cnt = 1;
    evaluate_calendar_bymonth(calendar, timeline, &date_cnt);
    evaluate_calendar_byweekno(calendar, timeline, &date_cnt);
    evaluate_calendar_byyearday(calendar, timeline, &date_cnt);
    evaluate_calendar_bymonthday(calendar, timeline, &date_cnt);

    /* sort the timeline so that we can break immediately once we find any timestamps later */
    qsort(timeline, date_cnt, sizeof(TimestampTz), timestamp_cmp_func);

    /*
     * Use the sorted timeline (date part) to generate matching timestamp for each day.
     */
    int time_cnt;
    for (int i = 0; i < date_cnt; i++) {
        time_cnt = 1;
        sub_timeline[0] = timeline[i]; /* starting date */
        evaluate_calendar_byhour(calendar, sub_timeline, &time_cnt);
        evaluate_calendar_byminute(calendar, sub_timeline, &time_cnt);
        evaluate_calendar_bysecond(calendar, sub_timeline, &time_cnt);
        if (find_nearest_calendar_time(calendar, sub_timeline, start_date, time_cnt, next_date)) {
            return true;
        }
    }
    return false;
}

/*
 * @brief prepare_calendar_period
 *
 * @param calendar
 * @param base_date
 * @param timeline
 */
static void prepare_calendar_period(Calendar calendar, TimestampTz base_date, TimestampTz *timeline)
{
    Assert(calendar != NULL);
    Assert(calendar->frequency <= MAX_FREQ);
    Assert(calendar->frequency >= YEARLY);
    Assert(calendar->date_depth > 0);
    Assert(calendar->time_depth > 0);
    timeline[0] = truncate_calendar_date(CStringGetTextDatum("year"), TimestampTzGetDatum(base_date));

    if (calendar->date_depth > MAX_CALENDAR_DATE_DEPTH || calendar->time_depth > SECS_PER_DAY) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("Cannot evaluate calendar clause."),
                        errdetail("The scheduler run out of attempts to find a valid date in the foreesable future."),
                        errcause("N/A"), erraction("Please modify the calendaring string.")));
    }

    fsec_t fsec;
    struct pg_tm tt, *tm = &tt;     /* POSIX time struct, see NOTE above */
    int tz;
    if (timestamp2tm(base_date, &tz, tm, &fsec, NULL, NULL) != 0) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("Cannot evaluate calendar clause."), errdetail("Fail to truncate start date."),
                        errcause("N/A"), erraction("Please modify the calendaring string.")));
    }
    calendar->tm = tt;
    calendar->fsec = fsec;

    if (timestamp_cmp_internal(timeline[0], calendar->ref_date) > 0) {
        calendar->ref_date = timeline[0];
    }
}

/*
 * @brief get_next_calendar_period
 *  Get the next calendar period. Usually a year, but can be multiple years if interval is large.
 * @param calendar
 * @param base_date
 * @return TimestampTz
 */
static TimestampTz get_next_calendar_period(Calendar calendar, TimestampTz base_date)
{
    int span_multiplier = 1;
    if (calendar->frequency == MONTHLY && calendar->interval > MONTHS_PER_YEAR) {
        span_multiplier = calendar->interval / MONTHS_PER_YEAR;
    }
    Interval *span = calendar_construct_interval("years", span_multiplier);
    base_date = DatumGetTimestampTz(timestamp_pl_interval(base_date, span));
    pfree_ext(span);
    return base_date;
}


/*
 * @brief evaluate_calendar_interval
 *  Calculate next date base on start date.
 * @param calendar
 * @param start_date
 * @return char*
 */
static TimestampTz evaluate_calendar_interval(Calendar calendar, TimestampTz start_date)
{
    if (calendar == NULL) {
        return start_date;
    }

    int try_count = 0;
    TimestampTz next_date = start_date;
    TimestampTz base_date = start_date; /* base date of each period, update with loop */
    TimestampTz *timeline = (TimestampTz *)palloc0(calendar->date_depth * sizeof(TimestampTz));
    TimestampTz *sub_timeline = (TimestampTz *)palloc0((calendar->time_depth + 1) * sizeof(TimestampTz));
    while (try_count < YEARS_PER_CENTURY) {
        prepare_calendar_period(calendar, base_date, timeline);
        if (evaluate_calendar_period(calendar, timeline, sub_timeline, start_date, &next_date)) {
            break;
        }
        base_date = get_next_calendar_period(calendar, base_date);
        CHECK_FOR_INTERRUPTS();
        try_count++;
    }

    pfree_ext(timeline);
    pfree_ext(sub_timeline);
    if (try_count >= YEARS_PER_CENTURY) {
        ereport(ERROR, (errmodule(MOD_JOB), errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("Cannot evaluate calendar clause."), errdetail("Calender clause too deep."),
                        errcause("N/A"), erraction("Please modify the calendaring string.")));
    }
    return next_date;
}

/*
 * @brief evaluate_repeat_interval
 *  Calculate next date base on start date and calendar string.
 * @param calendar_in
 * @param base_time
 * @return Datum
 */
Datum evaluate_repeat_interval(Datum calendar_in, Datum start_date, Datum date_after)
{
    char *calendar_str = text_to_cstring(DatumGetTextP(calendar_in));
    if (strlen(calendar_str) > (size_t)MAX_CALENDAR_STR_LEN) {
        pfree_ext(calendar_str);
        return calendar_in;
    }

    TimestampTz start_date_raw = DatumGetTimestampTz(start_date);
    Calendar calendar = interpret_calendar_interval(calendar_str);

    /* remove fsec part */
    start_date_raw = truncate_calendar_date(CStringGetTextDatum("second"), TimestampTzGetDatum(start_date_raw));

    /* fastforward start date if date_after is specified */
    if (calendar != NULL) {
        /* remove fsec part */
        date_after = truncate_calendar_date(CStringGetTextDatum("second"), TimestampTzGetDatum(date_after));
        fastforward_calendar_period(calendar, &start_date_raw, DatumGetTimestampTz(date_after));
    }
    TimestampTz next_date = evaluate_calendar_interval(calendar, start_date_raw);

    pfree_ext(calendar_str);
    if (calendar == NULL) {
        /* return if not a calendar interval expression, `calendar` is already freed */
        return calendar_in;
    } else {
        pfree_ext(calendar);
    }
    return TimestampTzGetDatum(next_date);
}

/*
 * @brief evaluate_calendar_string_internal
 *  eval_calendar_string interface.
 * @return Datum
 */
Datum evaluate_calendar_string_internal(PG_FUNCTION_ARGS)
{
    Datum string = PG_GETARG_DATUM(0);      /* calendar string */
    Datum start_date = PG_GETARG_DATUM(1);  /* start date */
    Datum date_after = PG_GETARG_DATUM(2);  /* return date after */
    Datum new_next_date = evaluate_repeat_interval(string, start_date, date_after);
    PG_RETURN_DATUM(new_next_date);
}