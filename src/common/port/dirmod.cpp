/* -------------------------------------------------------------------------
 *
 * dirmod.cpp
 *	  directory handling functions
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *	This includes replacement versions of functions that work on
 *	Win32 (NT4 and newer).
 *
 * IDENTIFICATION
 *	  src/common/port/dirmod.cpp
 *
 * -------------------------------------------------------------------------
 */

#ifndef FRONTEND
#include "postgres.h"
#include "knl/knl_variable.h"
#else
#include "postgres_fe.h"
#endif

/* Don't modify declarations in system headers */
#if defined(WIN32) || defined(__CYGWIN__)
#undef rename
#undef unlink
#endif

#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>

#if defined(WIN32) || defined(__CYGWIN__)
#ifndef __CYGWIN__
#include <winioctl.h>
#else
#define _WINSOCKAPI_
#include <windows.h>
#include <w32api/winioctl.h>
#endif
#endif

#include "storage/file/fio_device.h"
#include "securec_check.h"

#ifndef FRONTEND

/*
 *	On Windows, call non-macro versions of palloc; we can't reference
 *	CurrentMemoryContext in this file because of PGDLLIMPORT conflict.
 */
#if defined(WIN32) || defined(__CYGWIN__)
#undef palloc
#undef pstrdup
#define palloc(sz) pgport_palloc(sz)
#define pstrdup(str) pgport_pstrdup(str)
#endif
#else /* FRONTEND */

/*
 *	In frontend, fake palloc behavior with these
 */
#undef palloc
#undef pstrdup
#define palloc(sz) fe_palloc(sz)
#define pstrdup(str) fe_pstrdup(str)
#define repalloc(pointer, sz) fe_repalloc(pointer, sz)
#define pfree(pointer) free(pointer)

static void* fe_palloc(Size size)
{
    void* res = NULL;
#ifdef FRONTEND
    res = malloc(size);
#else
    res = MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), size);
#endif
    if (res == NULL) {
        fprintf(stderr, _("out of memory\n"));
        exit(1);
    }
    return res;
}

static char* fe_pstrdup(const char* string)
{
    char* res = NULL;

#ifdef FRONTEND
    res = strdup(string);
#else
    res = MemoryContextStrdup(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), string);
#endif
    if (res == NULL) {
        fprintf(stderr, _("out of memory\n"));
        exit(1);
    }
    return res;
}

static void* fe_repalloc(void* pointer, Size size)
{
    void* res = NULL;
#ifdef FRONTEND
    res = realloc(pointer, size);
#else
    res = repalloc(pointer, size);
#endif
    if (res == NULL) {
        fprintf(stderr, _("out of memory\n"));
        exit(1);
    }
    return res;
}
#endif /* FRONTEND */

#if defined(WIN32) || defined(__CYGWIN__)

/*
 *	pgrename
 */
int pgrename(const char* from, const char* to)
{
    int loops = 0;

    /*
     * We need to loop because even though openGauss uses flags that allow
     * rename while the file is open, other applications might have the file
     * open without those flags.  However, we won't wait indefinitely for
     * someone else to close the file, as the caller might be holding locks
     * and blocking other backends.
     */
#if defined(WIN32) && !defined(__CYGWIN__)
    while (!MoveFileEx(from, to, MOVEFILE_REPLACE_EXISTING))
#else
    while (rename(from, to) < 0)
#endif
    {
#if defined(WIN32) && !defined(__CYGWIN__)
        DWORD err = GetLastError();

        _dosmaperr(err);

        /*
         * Modern NT-based Windows versions return ERROR_SHARING_VIOLATION if
         * another process has the file open without FILE_SHARE_DELETE.
         * ERROR_LOCK_VIOLATION has also been seen with some anti-virus
         * software. This used to check for just ERROR_ACCESS_DENIED, so
         * presumably you can get that too with some OS versions. We don't
         * expect real permission errors where we currently use rename().
         */
        if (err != ERROR_ACCESS_DENIED && err != ERROR_SHARING_VIOLATION && err != ERROR_LOCK_VIOLATION) {
            return -1;
        }
#else
        if (errno != EACCES) {
            return -1;
        }
#endif

        if (++loops > 100) { /* time out after 10 sec */
            return -1;
        }
        pg_usleep(100000); /* us */
    }
    return 0;
}

/*
 *	pgunlink
 */
int pgunlink(const char* path)
{
    int loops = 0;

    /*
     * We need to loop because even though openGauss uses flags that allow
     * unlink while the file is open, other applications might have the file
     * open without those flags.  However, we won't wait indefinitely for
     * someone else to close the file, as the caller might be holding locks
     * and blocking other backends.
     */
    while (unlink(path)) {
        if (errno != EACCES) {
            return -1;
        }
        if (++loops > 100) {
            return -1; /* time out after 10 sec */
        }
        pg_usleep(100000); /* us */
    }
    return 0;
}

/* We undefined these above; now redefine for possible use below */
#define rename(from, to) pgrename(from, to)
#define unlink(path) pgunlink(path)
#endif /* defined(WIN32) || defined(__CYGWIN__) */

#if defined(WIN32) && !defined(__CYGWIN__) /* Cygwin has its own symlinks */

/*
 *	pgsymlink support:
 *
 *	This struct is a replacement for REPARSE_DATA_BUFFER which is defined in VC6 winnt.h
 *	but omitted in later SDK functions.
 *	We only need the SymbolicLinkReparseBuffer part of the original struct's union.
 */
typedef struct {
    DWORD ReparseTag;
    WORD ReparseDataLength;
    WORD Reserved;
    /* SymbolicLinkReparseBuffer */
    WORD SubstituteNameOffset;
    WORD SubstituteNameLength;
    WORD PrintNameOffset;
    WORD PrintNameLength;
    WCHAR PathBuffer[1];
} REPARSE_JUNCTION_DATA_BUFFER;

#define REPARSE_JUNCTION_DATA_BUFFER_HEADER_SIZE FIELD_OFFSET(REPARSE_JUNCTION_DATA_BUFFER, SubstituteNameOffset)

/*
 *	pgreadlink - uses Win32 junction points
 */
int pgreadlink(const char* path, char* buf, size_t size)
{
    DWORD attr;
    HANDLE h = NULL;
    char buffer[MAX_PATH * sizeof(WCHAR) + sizeof(REPARSE_JUNCTION_DATA_BUFFER)];
    REPARSE_JUNCTION_DATA_BUFFER* reparseBuf = (REPARSE_JUNCTION_DATA_BUFFER*)buffer;
    DWORD len;
    int r;

    attr = GetFileAttributes(path);
    if (attr == INVALID_FILE_ATTRIBUTES) {
        _dosmaperr(GetLastError());
        return -1;
    }
    if ((attr & FILE_ATTRIBUTE_REPARSE_POINT) == 0) {
        errno = EINVAL;
        return -1;
    }

    h = CreateFile(path,
        GENERIC_READ,
        FILE_SHARE_READ | FILE_SHARE_WRITE,
        NULL,
        OPEN_EXISTING,
        FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS,
        0);
    if (h == INVALID_HANDLE_VALUE) {
        _dosmaperr(GetLastError());
        return -1;
    }

    if (!DeviceIoControl(h, FSCTL_GET_REPARSE_POINT, NULL, 0, (LPVOID)reparseBuf, sizeof(buffer), &len, NULL)) {
        LPSTR msg = NULL;
        errno = 0;
        FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM,
            NULL,
            GetLastError(),
            MAKELANGID(LANG_ENGLISH, SUBLANG_DEFAULT),
            (LPSTR)&msg,
            0,
            NULL);
#ifndef FRONTEND
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not get junction for \"%s\": %s", path, msg)));
#else
        fprintf(stderr, _("could not get junction for \"%s\": %s\n"), path, msg);
#endif
        LocalFree(msg);
        CloseHandle(h);
        errno = EINVAL;
        return -1;
    }
    CloseHandle(h);

    /* Got it, let's get some results from this */
    if (reparseBuf->ReparseTag != IO_REPARSE_TAG_MOUNT_POINT) {
        errno = EINVAL;
        return -1;
    }

    r = WideCharToMultiByte(CP_ACP, 0, reparseBuf->PathBuffer, -1, buf, size, NULL, NULL);

    if (r <= 0) {
        errno = EINVAL;
        return -1;
    }

    /*
     * If the path starts with "\??\", which it will do in most (all?) cases,
     * strip those out.
     */
    if (r > 4 && strncmp(buf, "\\??\\", 4) == 0) {
        errno_t rc = memmove_s(buf, strlen(buf + 4) + 5, buf + 4, strlen(buf + 4) + 1);
        securec_check_c(rc, "", "");
        r -= 4;
    }
    return r;
}

/*
 * Assumes the file exists, so will return false if it doesn't
 * (since a nonexistant file is not a junction)
 */
bool pgwin32_is_junction(char* path)
{
    DWORD attr = GetFileAttributes(path);

    if (attr == INVALID_FILE_ATTRIBUTES) {
        _dosmaperr(GetLastError());
        return false;
    }
    return ((attr & FILE_ATTRIBUTE_REPARSE_POINT) == FILE_ATTRIBUTE_REPARSE_POINT);
}
#endif /* defined(WIN32) && !defined(__CYGWIN__) */

/*
 * pgfnames
 *
 * return a list of the names of objects in the argument directory.  Caller
 * must call pgfnames_cleanup later to free the memory allocated by this
 * function.
 */
char** pgfnames(const char* path)
{
    DIR* dir = NULL;
    struct dirent* file = NULL;
    char** filenames = NULL;
    int numnames = 0;
    int fnsize = 200; /* enough for many small dbs */

    dir = opendir(path);
    if (dir == NULL) {
#ifndef FRONTEND
        ereport(WARNING, (errmsg("could not open directory \"%s\": %m", path)));
#else
        fprintf(stderr, _("could not open directory \"%s\": %s\n"), path, gs_strerror(errno));
#endif
        return NULL;
    }

    filenames = (char**)palloc(fnsize * sizeof(char*));
    if (filenames == NULL) {
        closedir(dir);

#ifndef FRONTEND
        ereport(WARNING, (errmsg("alloc memory failed for \"%s\": %m", path)));
#else
        fprintf(stderr, _("alloc memory failed \"%s\": %s\n"), path, gs_strerror(errno));
#endif

        return NULL;
    }

    errno = 0;
    while ((file = gs_readdir(dir)) != NULL) {
        if (strcmp(file->d_name, ".") != 0 && strcmp(file->d_name, "..") != 0) {
            if (numnames + 1 >= fnsize) {
                fnsize *= 2;
                filenames = (char**)repalloc(filenames, fnsize * sizeof(char*));
            }
            filenames[numnames++] = pstrdup(file->d_name);
        }
        errno = 0;
    }
#ifdef WIN32

    /*
     * This fix is in mingw cvs (runtime/mingwex/dirent.c rev 1.4), but not in
     * released version
     */
    if (GetLastError() == ERROR_NO_MORE_FILES) {
        errno = 0;
    }
#endif
    if (errno) {
#ifndef FRONTEND
        ereport(WARNING, (errmsg("could not read directory \"%s\": %m", path)));
#else
        fprintf(stderr, _("could not read directory \"%s\": %s\n"), path, gs_strerror(errno));
#endif
    }

    filenames[numnames] = NULL;
    closedir(dir);
    return filenames;
}

/*
 *	pgfnames_cleanup
 *
 *	deallocate memory used for filenames
 */
void pgfnames_cleanup(char** filenames)
{
    char** fn;

    for (fn = filenames; *fn != NULL; fn++) {
        pfree(*fn);
    }

    pfree(filenames);
}

/*
 *	rmtree
 *
 *	Delete a directory tree recursively.
 *	Assumes path points to a valid directory.
 *	Deletes everything under path.
 *	If rmtopdir is true deletes the directory too.
 *	Returns true if successful, false if there was any problem.
 *	(The details of the problem are reported already, so caller
 *	doesn't really have to say anything more, but most do.)
 */
bool rmtree(const char* path, bool rmtopdir, bool noent_ok)
{
    bool result = true;
    char pathbuf[MAXPGPATH];
    char** filenames;
    char** filename;
    struct stat statbuf;

    /*
     * we copy all the names out of the directory before we start modifying
     * it.
     */
    filenames = pgfnames(path);

    if (filenames == NULL) {
        /* treat no entry as ok */
        if (noent_ok) {
            return true;
        } else {
            return false;
        }
    }

    /* now we have the names we can start removing things */
    for (filename = filenames; *filename != NULL; filename++) {
        errno_t rc = snprintf_s(pathbuf, MAXPGPATH, MAXPGPATH - 1, "%s/%s", path, *filename);
        if (rc == -1) {
#ifndef FRONTEND
            ereport(WARNING, (errmsg("pathbuf is not enough. path is %s, and file name is %s.\n", path, *filename)));
#else
            fprintf(stderr, _("pathbuf is not enough. path is %s, and file name is %s.\n"), path, *filename);
#endif
        }
        /*
         * It's ok if the file is not there anymore; we were just about to
         * delete it anyway.
         *
         * This is not an academic possibility. One scenario where this
         * happens is when bgwriter has a pending unlink request for a file in
         * a database that's being dropped. In dropdb(), we call
         * ForgetDatabaseSyncRequests() to flush out any such pending unlink
         * requests, but because that's asynchronous, it's not guaranteed that
         * the bgwriter receives the message in time.
         */
        if (lstat(pathbuf, &statbuf) != 0) {
            if (!is_file_delete(errno)) {
#ifndef FRONTEND
                ereport(WARNING, (errmsg("could not stat file or directory \"%s\": %m", pathbuf)));
#else
                fprintf(stderr, _("could not stat file or directory \"%s\": %s\n"), pathbuf, gs_strerror(errno));
#endif
                result = false;
            }
            continue;
        }

        if (S_ISDIR(statbuf.st_mode)) {
            /* call ourselves recursively for a directory */
            if (!rmtree(pathbuf, true)) {
                /* we already reported the error */
                result = false;
            }
        } else {
            if (unlink(pathbuf) != 0) {
                if (errno != ENOENT) {
#ifndef FRONTEND
                    ereport(WARNING, (errmsg("could not remove file or directory \"%s\": %m", pathbuf)));
#else
                    fprintf(stderr, _("could not remove file or directory \"%s\": %s\n"), pathbuf, gs_strerror(errno));
#endif
                    result = false;
                }
            }
        }
    }

    if (rmtopdir) {
        if (rmdir(path) != 0) {
#ifndef FRONTEND
            ereport(WARNING, (errmsg("could not remove file or directory \"%s\": %m", path)));
#else
            fprintf(stderr, _("could not remove file or directory \"%s\": %s\n"), path, gs_strerror(errno));
#endif
            result = false;
        }
    }

    pgfnames_cleanup(filenames);

    return result;
}

#if defined(WIN32) && !defined(__CYGWIN__)

#undef stat

/*
 * The stat() function in win32 is not guaranteed to update the st_size
 * field when run. So we define our own version that uses the Win32 API
 * to update this field.
 */
int pgwin32_safestat(const char* path, struct stat* buf)
{
    int r;
    WIN32_FILE_ATTRIBUTE_DATA attr;

    r = stat(path, buf);
    if (r < 0) {
        return r;
    }

    if (!GetFileAttributesEx(path, GetFileExInfoStandard, &attr)) {
        _dosmaperr(GetLastError());
        return -1;
    }

    /*
     * XXX no support for large files here, but we don't do that in general on
     * Win32 yet.
     */
    buf->st_size = attr.nFileSizeLow;

    return 0;
}

#endif

