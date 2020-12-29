/*
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1990 The Regents of the University of California.
 * All rights reserved.
 *
 * src/common/backend/port/dynloader/openbsd.c
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *	  notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *	  notice, this list of conditions and the following disclaimer in the
 *	  documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the University nor the names of its contributors
 *	  may be used to endorse or promote products derived from this software
 *	  without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.	IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#if defined(LIBC_SCCS) && !defined(lint)
static char sccsid[] = "@(#)dl.c	5.4 (Berkeley) 2/23/91";
#endif /* LIBC_SCCS and not lint */

#include "postgres.h"
#include "knl/knl_variable.h"

#include <nlist.h>
#include <link.h>

#include "dynloader.h"
#include "securec.h"

static char error_message[BUFSIZ];

char* BSD44_derived_dlerror(void)
{
    static char ret[BUFSIZ];

    errno_t rc = strcpy_s(ret, sizeof(ret), error_message);
    securec_check_c(rc, "\0", "\0");

    error_message[0] = 0;
    return (ret[0] == 0) ? NULL : ret;
}

void* BSD44_derived_dlopen(const char* file, int num)
{
#if !defined(HAVE_DLOPEN)
    errno_t rc = snprintf_truncated_s(error_message, sizeof(error_message), "dlopen (%s) not supported", file);
    securec_check_c(rc, "\0", "\0");
    return NULL;
#else
    void* vp = NULL;

    if ((vp = dlopen((char*)file, num)) == NULL) {
        errno_t rc =
            snprintf_truncated_s(error_message, sizeof(error_message), "dlopen (%s) failed: %s", file, dlerror());
        securec_check_c(rc, "\0", "\0");
    }
    return vp;
#endif
}

void* BSD44_derived_dlsym(void* handle, const char* name)
{
#if !defined(HAVE_DLOPEN)
    errno_t rc = snprintf_truncated_s(error_message, sizeof(error_message), "dlsym (%s) failed", name);
    securec_check_c(rc, "\0", "\0");
    return NULL;
#else
    void* vp = NULL;

#ifndef __ELF__
    char buf[BUFSIZ];

    if (*name != '_') {
        errno_t rc = snprintf_truncated_s(buf, sizeof(buf), "_%s", name);
        securec_check_c(rc, "\0", "\0");
        name = buf;
    }
#endif /* !__ELF__ */
    if ((vp = dlsym(handle, (char*)name)) == NULL) {
        errno_t rc = snprintf_truncated_s(error_message, sizeof(error_message), "dlsym (%s) failed", name);
        securec_check_c(rc, "\0", "\0");
    }
    return vp;
#endif
}

void BSD44_derived_dlclose(void* handle)
{
#if defined(HAVE_DLOPEN)
    dlclose(handle);
#endif
}
