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
 * ---------------------------------------------------------------------------------------
 * 
 * dbe_scheduler.h
 *        definition of DBE_SCHEDULER internal realizations which called by interfaces.
 *        internal methods are located in gs_job_attribute.cpp and gs_job_argument.cpp
 * 
 * 
 * IDENTIFICATION
 *        src/include/utils/dbe_scheduler.h
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"
#include "utils/datetime.h"
#include "utils/timestamp.h"

/* =========================================================
 *          CALENDARING INTERVAL RELATED
 * =========================================================
 */

#define MAX_CALENDAR_FIELDS             34      /* max number of by-fields, max 17 possible clauses, 2 for each batch */
#define SUPPORTED_FIELDS                7
#define CALENDAR_INTERVAL_FIELDS_MASK   0x1FF   /* 9 interval clauses */
#define MAX_CALENDAR_STR_LEN            2048    /* max calendar interval string length */
#define MAX_CALENDAR_FIELD_LEN          256     /* max calendar clause string length */
#define MAX_CALENDAR_CLAUSE_NUM         64      /* max number of clause values */
#define MAX_CALENDAR_INTERVAL_NUM       99      /* max number of intervals(calendar->interval) */
#define MAX_CALENDAR_DATE_DEPTH         744     /* every single day in two years 31*12*2 */

/* Extra date time macros */
#define MAX_WEEKS_PER_YEAR              53
#define YEARS_PER_CENTURY               100
#define DAYS_PER_WEEK                   7

/* Macros for input checks */
#define IsIllegalIntervalCharacter(c) (!isdigit((c)) && !isalpha((c)) && \
                                       (c) != ',' && (c) != '/' && (c) != '-' && (c) != ' ')

/* Frequency of the interval */
enum CalendarFrequency {
    YEARLY = 0,
    MONTHLY,
    WEEKLY,
    DAILY,
    HOURLY,
    MINUTELY,
    SECONDLY,
    MAX_FREQ
};

/* Interval constraint fields */
enum {
    INTERVAL_BYMONTH = 1,
    INTERVAL_BYWEEKNO = 2,
    INTERVAL_BYYEARDAY = 4,
    INTERVAL_BYDATE = 8,
    INTERVAL_BYMONTHDAY = 16,
    INTERVAL_BYDAY = 32,
    INTERVAL_BYHOUR = 64,
    INTERVAL_BYMINUTE = 128,
    INTERVAL_BYSECOND = 256
};

/* Calendaring interval context */
typedef struct CalendarContext {
    /* frequency clause */
    CalendarFrequency frequency;

    /* interval clauses */
    short interval;                                 /* 1 - 99 */
    short bymonth[MONTHS_PER_YEAR];                 /* 1 - 12 or JAN - DEC */
    short byweekno[MAX_WEEKS_PER_YEAR * 2];         /* (-)1 - 53 */
    short byyearday[DAYS_PER_NYEAR * 2 + 2];        /* (-)1 - 365(366) */
    TimestampTz bydate[DAYS_PER_NYEAR + 1];         /* [yyyy]mmdd */
    short bymonthday[DAYS_PER_MONTH * 2 + 2];       /* (-)1 - 31 */
    short byday_day[DAYS_PER_WEEK];                 /* MON - SUN */
    short byday_num[MAX_WEEKS_PER_YEAR * 2];        /* (-)1 - 53 for yearly/(-)1 - 5 for weekly */
    short byhour[HOURS_PER_DAY];                    /* 0 - 23 */
    short byminute[MINS_PER_HOUR];                  /* 0 - 59 */
    short bysecond[SECS_PER_MINUTE];                /* 0 - 59 */

    /* clause length */
    short month_len;
    short weekno_len;
    short yearday_len;
    short date_len;
    short monthday_len;
    short day_len;
    short hour_len;
    short minute_len;
    short second_len;

    /* max depth */
    int date_depth;
    int time_depth;

    /* mask && other info */
    unsigned short byfields;    /* 9 valid bit mask from bymonth(L) to bysecond(H) */
    struct pg_tm tm;            /* start time POSIX time struct */
    fsec_t fsec;
    TimestampTz ref_date;
} CalendarContext;

/* For ease of use */
typedef CalendarContext* Calendar;

/* Calendaring interval interpreter */
extern Datum evaluate_repeat_interval(Datum calendar_in, Datum start_date, Datum date_after);

/* Inlined method */
inline static Interval *calendar_construct_interval(const char *field_str, int val)
{
    errno_t rc = EOK;
    const int len = 64;
    char str[len] = {0};

    rc = snprintf_s(str, len, len - 1, "%d %s", val, field_str);
    securec_check_ss_c(rc, "\0", "\0");

    return char_to_interval(str, -1);
}

inline static TimestampTz truncate_calendar_date(Datum field, Datum date)
{
    return DatumGetTimestampTz(DirectFunctionCall2(timestamptz_trunc, field, date));
}


/* =========================================================
 *          DBE_SCHEUDLER INTERFACES
 * =========================================================
 */

/*
 * @brief grant user certain privileges
 * DBE_SCHEDULER.grant_user_authorization
 *      username                IN text
 *      privilege               IN text
 */
extern void grant_user_authorization_internal(PG_FUNCTION_ARGS);

/*
 * @brief revoke certain privileges
 * DBE_SCHEDULER.revoke_user_authorization
 *      username                IN text
 *      privilege               IN text
 */
extern void revoke_user_authorization_internal(PG_FUNCTION_ARGS);

/*
 * @brief Drop a credential object
 * DBE_SCHEDULER.drop_credential
 *      credential_name         IN text
 */
extern void drop_credential_internal(PG_FUNCTION_ARGS);

/*
 * @brief Create a credential object
 * DBE_SCHEDULER.create_credential
 *      credential_name         IN text
 *      username                IN text
 *      password                IN text
 *      database_role           IN text  default NULL
 *      windows_domain          IN text  default NULL
 *      comments                IN text  default NULL
 */
extern void create_credential_internal(PG_FUNCTION_ARGS);

/*
 * @brief Create a program internal object
 * DBE_SCHEDULER.create_program
 *      program_name             IN TEXT
 *      program_type             IN TEXT
 *      program_action           IN TEXT
 *      number_of_arguments      IN INTEGER DEFAULT 0
 *      enabled                  IN BOOLEAN DEFAULT FALSE
 *      comments                 IN TEXT DEFAULT NULL
 * @return Datum 
 */
extern void create_program_internal(PG_FUNCTION_ARGS, bool is_inline_program);

/*
 * @brief Drop a program internal object
 * DBE_SCHEDULER.drop_single_program
 *      program_name            IN TEXT
 *      force                   IN BOOLEAN DEFAULT FALSE
 */
extern void drop_single_program_internal(PG_FUNCTION_ARGS);

/*
 * @brief Create a schedule internal object
 * DBE_SCHEDULER.create_schedule
 *      schedule_name          IN TEXT
 *      start_date             IN TIMESTAMP WITH TIMEZONE DEFAULT NULL
 *      repeat_interval        IN TEXT
 *      end_date               IN TIMESTAMP WITH TIMEZONE DEFAULT NULL
 *      comments               IN TEXT DEFAULT NULL
 */
extern void create_schedule_internal(PG_FUNCTION_ARGS);

/*
 * @brief Drop a schedule internal object
 * DBE_SCHEDULER.drop_single_schedule
 *      schedule_name    IN TEXT
 *      force            IN BOOLEAN DEFAULT FALSE
 */
extern void drop_single_schedule_internal(PG_FUNCTION_ARGS);

/*
 * @brief Create a job 1 internal object
 * DBE_SCHEDULER.create_job
 *      job_name             IN TEXT
 *      job_type             IN TEXT
 *      job_action           IN TEXT
 *      number_of_arguments  IN INTEGER              DEFAULT 0
 *      start_date           IN TIMESTAMP WITH TIME ZONE DEFAULT NULL
 *      repeat_interval      IN TEXT                 DEFAULT NULL
 *      end_date             IN TIMESTAMP WITH TIME ZONE DEFAULT NULL
 *      job_class            IN TEXT                 DEFAULT 'DEFAULT_JOB_CLASS'
 *      enabled              IN BOOLEAN              DEFAULT FALSE
 *      auto_drop            IN BOOLEAN              DEFAULT TRUE
 *      comments             IN TEXT                 DEFAULT NULL
 *      credential_name      IN TEXT                 DEFAULT NULL
 *      destination_name     IN TEXT                 DEFAULT NULL
 */
extern void create_job_1_internal(PG_FUNCTION_ARGS);

/*
 * @brief Create a job 2 internal object
 * DBE_SCHEDULER.create_job
 *      job_name                IN TEXT
 *      program_name            IN TEXT
 *      schedule_name           IN TEXT
 *      job_class               IN TEXT              DEFAULT 'DEFAULT_JOB_CLASS'
 *      enabled                 IN BOOLEAN           DEFAULT FALSE
 *      auto_drop               IN BOOLEAN           DEFAULT TRUE
 *      comments                IN TEXT              DEFAULT NULL
 *      job_style               IN TEXT              DEFAULT 'REGULAR'
 *      credential_name         IN TEXT              DEFAULT NULL,
 *      destination_name        IN TEXT              DEFAULT NULL
 */
extern void create_job_2_internal(PG_FUNCTION_ARGS);

/*
 * @brief Create a job 3 internal object
 * DBE_SCHEDULER.create_job
 *      job_name             IN TEXT
 *      program_name         IN TEXT
 *      start_date           IN TIMESTAMP WITH TIME ZONE DEFAULT NULL
 *      repeat_interval      IN TEXT                 DEFAULT NULL
 *      end_date             IN TIMESTAMP WITH TIME ZONE DEFAULT NULL
 *      job_class            IN TEXT                 DEFAULT 'DEFAULT_JOB_CLASS'
 *      enabled              IN BOOLEAN              DEFAULT FALSE
 *      auto_drop            IN BOOLEAN              DEFAULT TRUE
 *      comments             IN TEXT                 DEFAULT NULL
 *      job_style            IN TEXT                 DEFAULT 'REGULAR'
 *      credential_name      IN TEXT                 DEFAULT NULL
 *      destination_name     IN TEXT                 DEFAULT NULL
 */
extern void create_job_3_internal(PG_FUNCTION_ARGS);

/*
 * @brief Create a job 4 internal object
 * DBE_SCHEDULER.create_job
 *      job_name                IN TEXT
 *      schedule_name           IN TEXT
 *      job_type                IN TEXT
 *      job_action              IN TEXT
 *      number_of_arguments     IN INTEGER       DEFAULT 0
 *      job_class               IN TEXT          DEFAULT 'DEFAULT_JOB_CLASS'
 *      enabled                 IN BOOLEAN       DEFAULT FALSE
 *      auto_drop               IN BOOLEAN       DEFAULT TRUE
 *      comments                IN TEXT          DEFAULT NULL
 *      credential_name         IN TEXT          DEFAULT NULL
 *      destination_name        IN TEXT          DEFAULT NULL
 */
extern void create_job_4_internal(PG_FUNCTION_ARGS);

/*
 * @brief Create a job 5 internal object
 * DBE_SCHEDULER.create_job
 *      job_name                IN TEXT
 *      job_type                IN TEXT
 *      job_action              IN TEXT
 *      number_of_arguments     IN INTEGER       DEFAULT 0
 *      start_date              IN TIMESTAMP WITH TIME ZONE DEFAULT NULL
 *      event_condition         IN TEXT          DEFAULT NULL
 *      queue_spec              IN TEXT
 *      end_date                IN TIMESTAMP WITH TIME ZONE DEFAULT NULL
 *      job_class               IN TEXT          DEFAULT 'DEFAULT_JOB_CLASS'
 *      enabled                 IN BOOLEAN       DEFAULT FALSE
 *      auto_drop               IN BOOLEAN       DEFAULT TRUE
 *      comments                IN TEXT          DEFAULT NULL
 *      credential_name         IN TEXT          DEFAULT NULL
 *      destination_name        IN TEXT          DEFAULT NULL
 */
extern void create_job_5_internal(PG_FUNCTION_ARGS);

/*
 * @brief Create a job 6 internal object
 * DBE_SCHEDULER.create_job
 *      job_name                IN TEXT
 *      program_name            IN TEXT
 *      start_date              IN TIMESTAMP WITH TIME ZONE
 *      event_condition         IN TEXT
 *      queue_spec              IN TEXT
 *      end_date                IN TIMESTAMP WITH TIME ZONE
 *      job_class               IN TEXT          DEFAULT 'DEFAULT_JOB_CLASS'
 *      enabled                 IN BOOLEAN       DEFAULT FALSE
 *      auto_drop               IN BOOLEAN       DEFAULT TRUE
 *      comments                IN TEXT          DEFAULT NULL
 *      job_style               IN TEXT          DEFAULT 'REGULAR'
 *      credential_name         IN TEXT          DEFAULT NULL
 *      destination_name        IN TEXT          DEFAULT NULL
 */
extern void create_job_6_internal(PG_FUNCTION_ARGS);

/*
 * @brief Create a job class internal object
 * DBE_SCHEDULER.create_job_class
 *      job_class_name            IN TEXT
 *      resource_consumer_group   IN TEXT DEFAULT NULL
 *      service                   IN TEXT DEFAULT NULL
 *      logging_level             IN INTEGER DEFAULT DBE_SCHEDULER.LOGGING_RUNS
 *      log_history               IN INTEGER DEFAULT NULL
 *      comments                  IN TEXT DEFAULT NULL
 */
extern void create_job_class_internal(PG_FUNCTION_ARGS);

/*
 * @brief Drop a job class internal object
 * DBE_SCHEDULER.drop_single_job_class
 *      job_class_name          IN TEXT
 *      force                   IN BOOLEAN DEFAULT FALSE
 */
extern void drop_single_job_class_internal(PG_FUNCTION_ARGS);

/*
 * @brief Set the attribute internal object
 * DBE_SCHEDULER.set_attribute
 *      name           IN TEXT
 *      attribute      IN TEXT
 *      value          IN BOOLEAN/DATE/TIMESTAMP/TIMESTAMP WITH TIME ZONE
 */
extern void set_attribute_1_internal(PG_FUNCTION_ARGS, Oid type);

/*
 * @brief Set the attribute internal object
 * DBE_SCHEDULER.set_attribute
 *      name           IN TEXT
 *      attribute      IN TEXT
 *      value          IN DATE
 */
extern void set_attribute_2_internal(PG_FUNCTION_ARGS);

/*
 * @brief Define one program's argument w/o default
 * DBE_SCHEDULER.define_program_argument
 *      program_name            IN TEXT
 *      argument_position       IN INTEGER
 *      argument_name           IN TEXT DEFAULT NULL
 *      argument_type           IN TEXT
 *      out_argument            IN BOOLEAN DEFAULT FALSE
 */
extern void define_program_argument_1_internal(PG_FUNCTION_ARGS);

/*
 * @brief Define one program's argument w/ default
 * DBE_SCHEDULER.define_program_argument
 *      program_name            IN TEXT
 *      argument_position       IN INTEGER
 *      argument_name           IN TEXT DEFAULT NULL
 *      argument_type           IN TEXT
 *      default_value           IN TEXT
 *      out_argument            IN BOOLEAN DEFAULT FALSE
 */
extern void define_program_argument_2_internal(PG_FUNCTION_ARGS);

/*
 * @brief Set the job argument value internal object
 * DBE_SCHEDULER.set_job_argument_value
 *      job_name                IN TEXT
 *      argument_position       IN INTEGER
 *      argument_value          IN TEXT
 */
extern void set_job_argument_value_1_internal(PG_FUNCTION_ARGS);

/*
 * @brief Set the job argument value internal object
 * DBE_SCHEDULER.set_job_argument_value
 *      job_name                IN TEXT
 *      argument_name           IN TEXT
 *      argument_value          IN TEXT
 */
extern void set_job_argument_value_2_internal(PG_FUNCTION_ARGS);

/*
 * @brief General enable method for scheduler objects
 * DBE_SCHEDULER.enable
 *      name              IN TEXT
 */
extern void enable_single_internal(PG_FUNCTION_ARGS);

/*
 * @brief General disable method for scheduler objects
 * DBE_SCHEDULER.disable
 *      name              IN TEXT
 *      force             IN BOOLEAN DEFAULT FALSE
 */
extern void disable_single_internal(PG_FUNCTION_ARGS);

/*
 * @brief Run a job
 * DBE_SCHEDULER.run_backend_job_internal
 *      job_name                IN TEXT
 */
extern void run_backend_job_internal(PG_FUNCTION_ARGS);

/*
 * @brief Run a job
 * DBE_SCHEDULER.run_foreground_job_internal
 *      job_name                IN TEXT
 */
extern Datum run_foreground_job_internal(PG_FUNCTION_ARGS);

/*
 * @brief Stop a job
 * DBE_SCHEDULER.stop_job
 *      job_name         IN TEXT
 *      force            IN BOOLEAN DEFAULT FALSE
 */
extern void stop_single_job_internal(PG_FUNCTION_ARGS);

/*
 * @brief Drop a job internal object
 * DBE_SCHEDULER.drop_single_job
 *      job_name                IN TEXT
 *      force                   IN BOOLEAN DEFAULT FALSE
 *      defer                   IN BOOLEAN DEFAULT FALSE
 */
extern void drop_single_job_internal(PG_FUNCTION_ARGS);

/*
 * @brief evaluate calender string and return a timestamp base on the start_date
 * DBE_SCHEDULER.evaluate_calendar_string
 *      calendar_string         IN TEXT
 *      start_date              IN TIMESTAMP WITH TIME ZONE
  *     return_date_after       IN TIMESTAMP WITH TIME ZONE
 */
extern Datum evaluate_calendar_string_internal(PG_FUNCTION_ARGS);
