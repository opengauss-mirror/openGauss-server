/* -------------------------------------------------------------------------
 *
 * unsetenv.cpp
 *	  unsetenv() emulation for machines without it
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common.port/unsetenv.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "c.h"
#include "securec.h"

/* We add extra space to store "s=" or "=" */
#define ENV_PLACEHOLDER 2

void unsetenv(const char* name)
{
    char* envstr = NULL;

    if (gs_getenv_r(name) == NULL) {
        return; /* no work */
    }

    /*
         * The technique embodied here works if libc follows the Single Unix Spec
         * and actually uses the storage passed to putenv() to hold the environ
         * entry.  When we clobber the entry in the second step we are ensuring
         * that we zap the actual environ member.  However, there are some libc
         * implementations (notably recent BSDs) that do not obey SUS but copy the
         * presented string.  This method fails on such platforms.	Hopefully all
         * such platforms have unsetenv() and thus won't be using this hack. See:
         * http://www.greenend.org.uk/rjk/2008/putenv.html
         *
         * Note that repeatedly setting and unsetting a var using this code will
         * leak memory.
         */

#ifdef FRONTEND
    envstr = (char*)malloc(strlen(name) + ENV_PLACEHOLDER);
#else
    envstr = (char*)MemoryContextAlloc(
        SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), strlen(name) + ENV_PLACEHOLDER);
#endif
    if (envstr == NULL) { /* not much we can do if no memory */
        return;
    }

    /* Override the existing setting by forcibly defining the var */
    errno_t rc = sprintf_s(envstr, strlen(name) + ENV_PLACEHOLDER, "%s=", name);
    securec_check_ss_c(rc, "\0", "\0");
    gs_putenv_r(envstr);

    /* Now we can clobber the variable definition this way: */
    errno_t rc = strcpy_s(envstr, strlen(name) + ENV_PLACEHOLDER, "=");
    securec_check_c(rc, "\0", "\0");

    /*
     * This last putenv cleans up if we have multiple zero-length names as a
     * result of unsetting multiple things.
     */
    gs_putenv_r(envstr);
}
