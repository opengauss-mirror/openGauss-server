/* -------------------------------------------------------------------------
 *
 * kill.cpp
 *	  kill
 *
 * Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 *	This is a replacement version of kill for Win32 which sends
 *	signals that the backend can recognize.
 *
 * IDENTIFICATION
 *	  src/port/kill.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "c.h"
#include "securec.h"
#include "utils/elog.h"

#ifdef WIN32
/* signal sending */
int pgkill(int pid, int sig)
{
    char pipename[128];
    BYTE sigData = sig;
    BYTE sigRet = 0;
    DWORD bytes;

    /* we allow signal 0 here, but it will be ignored in pg_queue_signal */
    if (sig >= PG_SIGNAL_COUNT || sig < 0) {
        errno = EINVAL;
        return -1;
    }
    if (pid <= 0) {
        /* No support for process groups */
        errno = EINVAL;
        return -1;
    }
    int rc = snprintf_s(pipename, sizeof(pipename), sizeof(pipename) - 1, "\\\\.\\pipe\\pgsignal_%u", pid);
    securec_check_ss(rc, "\0", "\0");
    if (CallNamedPipe(pipename, &sigData, 1, &sigRet, 1, &bytes, 1000)) {
        if (bytes != 1 || sigRet != sig) {
            errno = ESRCH;
            return -1;
        }
        return 0;
    }

    if (GetLastError() == ERROR_FILE_NOT_FOUND)
        errno = ESRCH;
    else if (GetLastError() == ERROR_ACCESS_DENIED)
        errno = EPERM;
    else
        errno = EINVAL;
    return -1;
}

#endif
