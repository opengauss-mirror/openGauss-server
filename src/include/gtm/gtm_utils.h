/* -------------------------------------------------------------------------
 *
 * gtm_utils.h
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/gtm/gtm_utils.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef GTM_UTILS_H
#define GTM_UTILS_H

#include "gtm/utils/libpq-int.h"
#include "gtm/gtm_msg.h"

#ifndef WIN32

#include <sys/time.h>

/* GTMS --> GTM Server */
typedef struct timeval gtms_time;

#define GTMS_TIME_IS_ZERO(t) ((t).tv_usec == 0 && (t).tv_sec == 0)

#define GTMS_TIME_IS_INTMAX(t) ((t).tv_usec == LONG_MAX && (t).tv_sec == LONG_MAX)

#define GTMS_TIME_INITIAL_MIN(t) ((t).tv_usec = LONG_MAX, (t).tv_sec = LONG_MAX)

#define GTMS_TIME_IS_BIGGER(x, y) ((x).tv_sec >= (y).tv_sec && (x).tv_usec >= (y).tv_usec)

#define GTMS_TIME_SET_ZERO(t) ((t).tv_sec = 0, (t).tv_usec = 0)

#define GTMS_TIME_SET_CURRENT(t) gettimeofday(&(t), NULL)

#define GTMS_TIME_ADD(x, y)              \
    do {                                 \
        (x).tv_sec += (y).tv_sec;        \
        (x).tv_usec += (y).tv_usec;      \
        /* Normalize */                  \
        while ((x).tv_usec >= 1000000) { \
            (x).tv_usec -= 1000000;      \
            (x).tv_sec++;                \
        }                                \
    } while (0)

#define GTMS_TIME_SUBTRACT(x, y)    \
    do {                            \
        (x).tv_sec -= (y).tv_sec;   \
        (x).tv_usec -= (y).tv_usec; \
        /* Normalize */             \
        while ((x).tv_usec < 0) {   \
            (x).tv_usec += 1000000; \
            (x).tv_sec--;           \
        }                           \
    } while (0)

#define GTMS_TIME_ACCUM_DIFF(x, y, z)                                         \
    do {                                                                      \
        (x).tv_sec += (y).tv_sec - (z).tv_sec;                                \
        (x).tv_usec += (y).tv_usec - (z).tv_usec;                             \
        /* Normalize after each add to avoid overflow/underflow of tv_usec */ \
        while ((x).tv_usec < 0) {                                             \
            (x).tv_usec += 1000000;                                           \
            (x).tv_sec--;                                                     \
        }                                                                     \
        while ((x).tv_usec >= 1000000) {                                      \
            (x).tv_usec -= 1000000;                                           \
            (x).tv_sec++;                                                     \
        }                                                                     \
    } while (0)

#define GTMS_TIME_GET_DOUBLE(t) (((double)(t).tv_sec) + ((double)(t).tv_usec) / 1000000.0)

#define GTMS_TIME_GET_MILLISEC(t) (((double)(t).tv_sec * 1000.0) + ((double)(t).tv_usec) / 1000.0)

#define GTMS_TIME_GET_MICROSEC(t) (((uint64)(t).tv_sec * (uint64)1000000) + (uint64)(t).tv_usec)

#endif /* WIN32 */

void gtm_util_init_nametabs(void);
char* gtm_util_message_name(GTM_MessageType type);
char* gtm_util_result_name(GTM_ResultType type);

extern void gtm_usleep(long microsec);
#endif /* GTM_UTILS_H */
