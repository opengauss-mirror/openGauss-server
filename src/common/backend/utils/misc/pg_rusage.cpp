/* -------------------------------------------------------------------------
 *
 * pg_rusage.c
 *	  Resource usage measurement support routines.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/misc/pg_rusage.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"


#include "utils/pg_rusage.h"

/*
 * Initialize usage snapshot.
 */
void pg_rusage_init(PGRUsage* ru0)
{
    getrusage(RUSAGE_THREAD, &ru0->ru);
    gettimeofday(&ru0->tv, NULL);
}

/*
 * Compute elapsed time since ru0 usage snapshot, and format into
 * a displayable string.  Result is in a static string, which is
 * tacky, but no one ever claimed that the openGauss backend is
 * threadable...
 */
const char* pg_rusage_show(const PGRUsage* ru0)
{
    char* result = t_thrd.buf_cxt.pg_rusage_show_buf;
    int size = sizeof(t_thrd.buf_cxt.pg_rusage_show_buf);
    PGRUsage ru1;
    int nRet = 0;

    pg_rusage_init(&ru1);

    if (ru1.tv.tv_usec < ru0->tv.tv_usec) {
        ru1.tv.tv_sec--;
        ru1.tv.tv_usec += 1000000;
    }
    if (ru1.ru.ru_stime.tv_usec < ru0->ru.ru_stime.tv_usec) {
        ru1.ru.ru_stime.tv_sec--;
        ru1.ru.ru_stime.tv_usec += 1000000;
    }
    if (ru1.ru.ru_utime.tv_usec < ru0->ru.ru_utime.tv_usec) {
        ru1.ru.ru_utime.tv_sec--;
        ru1.ru.ru_utime.tv_usec += 1000000;
    }

    nRet = snprintf_s(result,
        size,
        size - 1,
        "CPU %d.%02ds/%d.%02du sec elapsed %d.%02d sec",
        (int)(ru1.ru.ru_stime.tv_sec - ru0->ru.ru_stime.tv_sec),
        (int)(ru1.ru.ru_stime.tv_usec - ru0->ru.ru_stime.tv_usec) / 10000,
        (int)(ru1.ru.ru_utime.tv_sec - ru0->ru.ru_utime.tv_sec),
        (int)(ru1.ru.ru_utime.tv_usec - ru0->ru.ru_utime.tv_usec) / 10000,
        (int)(ru1.tv.tv_sec - ru0->tv.tv_sec),
        (int)(ru1.tv.tv_usec - ru0->tv.tv_usec) / 10000);
    securec_check_ss_c(nRet, "\0", "\0");
    return result;
}
