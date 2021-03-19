/* -------------------------------------------------------------------------
 *
 * pgsleep.c
 *	   Portable delay handling.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 * src/port/pgsleep.c
 *
 * -------------------------------------------------------------------------
 */
#include "c.h"

#include <sys/time.h>

/*
 * In a Windows backend, we don't use this implementation, but rather
 * the signal-aware version in src/backend/port/win32/signal.c.
 */
#if defined(FRONTEND) || !defined(WIN32)

/*
 * pg_usleep --- delay the specified number of microseconds.
 *
 * NOTE: although the delay is specified in microseconds, the effective
 * resolution is only 1/HZ, or 10 milliseconds, on most Unixen.  Expect
 * the requested delay to be rounded up to the next resolution boundary.
 *
 * On machines where "long" is 32 bits, the maximum delay is ~2000 seconds.
 */
void pg_usleep(long microsec)
{
    if (microsec > 0) {
#ifndef WIN32
        struct timeval delay;

        delay.tv_sec = microsec / 1000000L;
        delay.tv_usec = microsec % 1000000L;
        (void)select(0, NULL, NULL, NULL, &delay);
#else
        SleepEx(((microsec < 500) ? 1 : ((microsec + 500) / 1000)), FALSE);  // 500ms add to 1000s
#endif
    }
}

void pg_usleep_retry(long microsec, int retry_times = 0)
{
    if (microsec > 0) {
#ifndef WIN32
        if (retry_times > 10000) {  // just a retry times count
            return;
        }
        struct timeval delay, tv1, tv2;
        delay.tv_sec = microsec / 1000000L;
        delay.tv_usec = microsec % 1000000L;
        struct timezone tz;
        gettimeofday(&tv1, &tz);
        int ret = select(0, NULL, NULL, NULL, &delay);
        if (errno == EINTR && ret != 0) {
            errno = 0;
            gettimeofday(&tv2, &tz);
            long rest_time = (tv2.tv_sec - tv1.tv_sec) * 1000000L + tv2.tv_usec - tv1.tv_usec;
            return pg_usleep_retry(microsec - rest_time, retry_times++);
        }

#else
        SleepEx(((microsec < 500) ? 1 : ((microsec + 500) / 1000)), FALSE);  // 500ms add to 1000s
#endif
    }
}

#endif /* defined(FRONTEND) || !defined(WIN32) */
