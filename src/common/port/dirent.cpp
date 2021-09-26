/* -------------------------------------------------------------------------
 *
 * dirent.cpp
 *	  opendir/readdir/closedir for win32/msvc
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/port/dirent.cpp
 *
 * -------------------------------------------------------------------------
 */

#ifndef FRONTEND
#include "postgres.h"
#include "knl/knl_variable.h"
#else
#include "postgres_fe.h"
#endif

#include <dirent.h>
#ifdef WIN32
#include "securec.h"
#include "securec_check.h"
#endif

#if defined(WIN32) && !defined(_MINGW32)
struct DIR {
    char* dirname;
    struct dirent ret; /* Used to return to caller */
    HANDLE handle;
};

DIR* opendir(const char* dirname)
{
    DWORD attr;
    DIR* d = NULL;
    errno_t rc;

    /* Make sure it is a directory */
    attr = GetFileAttributes(dirname);
    if (attr == INVALID_FILE_ATTRIBUTES) {
        errno = ENOENT;
        return NULL;
    }
    if ((attr & FILE_ATTRIBUTE_DIRECTORY) != FILE_ATTRIBUTE_DIRECTORY) {
        errno = ENOTDIR;
        return NULL;
    }

#ifdef FRONTEND
#ifdef WIN32
    d = (DIR*)malloc(sizeof(DIR));
#else
    d = malloc(sizeof(DIR));
#endif /* WIN32 */
#else
    d = MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), sizeof(DIR));
#endif
    if (d == NULL) {
        errno = ENOMEM;
        return NULL;
    }

#ifdef FRONTEND
#ifdef WIN32
    d->dirname = (char*)malloc(strlen(dirname) + 4);
#else
    d->dirname = malloc(strlen(dirname) + 4);
#endif /*WIN32*/
#else
    d->dirname = MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), strlen(dirname) + 4);
#endif
    if (d->dirname == NULL) {
        errno = ENOMEM;
#ifdef FRONTEND
        free(d);
#else
        pfree(d);
#endif
        return NULL;
    }
    rc = strcpy_s(d->dirname, strlen(dirname) + 1, dirname);
    securec_check_c(rc, "\0", "\0");
    if (d->dirname[strlen(d->dirname) - 1] != '/' && d->dirname[strlen(d->dirname) - 1] != '\\') {
        /* Append backslash if not already there */
        rc = strcat_s(d->dirname, strlen(dirname) + 4, "\\");
#ifdef FRONTEND
        securec_check_c(rc, "", "");
#else
        securec_check(rc, "\0", "\0");
#endif
    }
    rc = strcat_s(d->dirname, strlen(dirname) + 4, "*"); /* Search for entries named anything */
    securec_check_c(rc, "\0", "\0");
    d->handle = INVALID_HANDLE_VALUE;
    d->ret.d_ino = 0;    /* no inodes on win32 */
    d->ret.d_reclen = 0; /* not used on win32 */

    return d;
}

struct dirent* readdir(DIR* d)
{
    WIN32_FIND_DATA fd;

    if (d->handle == INVALID_HANDLE_VALUE) {
        d->handle = FindFirstFile(d->dirname, &fd);
        if (d->handle == INVALID_HANDLE_VALUE) {
            errno = ENOENT;
            return NULL;
        }
    } else {
        if (!FindNextFile(d->handle, &fd)) {
            if (GetLastError() == ERROR_NO_MORE_FILES) {
                /* No more files, force errno=0 (unlike mingw) */
                errno = 0;
                return NULL;
            }
            _dosmaperr(GetLastError());
            return NULL;
        }
    }
    /* Both strings are MAX_PATH long */
    errno_t rc = strcpy_s(d->ret.d_name, MAX_PATH, fd.cFileName);
    securec_check_c(rc, "\0", "\0");
    d->ret.d_namlen = strlen(d->ret.d_name);
    return &d->ret;
}

int closedir(DIR* d)
{
    if (d->handle != INVALID_HANDLE_VALUE) {
        FindClose(d->handle);
    }
#ifdef FRONTEND
    free(d->dirname);
    free(d);
#else
    pfree(d->dirname);
    pfree(d);
#endif
    return 0;
}
#endif
