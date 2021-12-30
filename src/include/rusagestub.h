/* -------------------------------------------------------------------------
 *
 * rusagestub.h
 *	  Stubs for getrusage(3).
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/rusagestub.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef RUSAGESTUB_H
#define RUSAGESTUB_H

#include <sys/time.h> /* for struct timeval */
#ifndef WIN32
#include <sys/times.h> /* for struct tms */
#endif
#include <limits.h> /* for CLK_TCK */

#ifndef RUSAGE_SELF
#define RUSAGE_SELF 0
#endif

#ifndef RUSAGE_CHILDREN
#define RUSAGE_CHILDREN (-1)
#endif

namespace rusagestub {
    struct rusage {
        struct timeval ru_utime; /* user time used */
        struct timeval ru_stime; /* system time used */
    };
}
using namespace rusagestub;

extern int getrusage(int who, struct rusagestub::rusage* rusage);

#ifndef WIN32
extern List* get_operator_name(Oid operid, Oid arg1, Oid arg2);
#endif

#endif /* RUSAGESTUB_H */
