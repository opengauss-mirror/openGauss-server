/* src/port/strerror.c */

/*
 * strerror - map error number to descriptive string
 *
 * This version is obviously somewhat Unix-specific.
 *
 * based on code by Henry Spencer
 * modified for ANSI by D'Arcy J.M. Cain
 */

#include "c.h"
#include "securec.h"

extern const char* const sys_errlist[];
extern int sys_nerr;

const char* strerror(int errnum)
{
    if (errnum < 0 || errnum > sys_nerr) {
        errno_t rc = sprintf_s(t_thrd.port_cxt.buf, sizeof(t_thrd.port_cxt.buf), _("unrecognized error %d"), errnum);
        securec_check_ss_c(rc, "\0", "\0");
        return t_thrd.port_cxt.buf;
    }

    return sys_errlist[errnum];
}
