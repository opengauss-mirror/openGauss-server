/* -------------------------------------------------------------------------
 *
 * copydir.cpp
 *	  copies a directory
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *	While "xcopy /e /i /q" works fine for copying directories, on Windows XP
 *	it requires a Window handle which prevents it from working when invoked
 *	as a service.
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/file/copydir.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include "storage/copydir.h"
#include "storage/smgr/fd.h"
#include "storage/smgr/segment.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/file/fio_device.h"

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

static int fsync_fname_ext(const char* fname, bool isdir, bool ignore_perm, int elevel);
static int fsync_parent_path(const char* fname, int elevel);

/*
 * copydir: copy a directory
 *
 * If recurse is false, subdirectories are ignored.  Anything that's not
 * a directory or a regular file is ignored.
 */
bool copydir(char* fromdir, char* todir, bool recurse, int elevel)
{
    DIR* xldir = NULL;
    struct dirent* xlde = NULL;
    char fromfile[MAXPGPATH];
    char tofile[MAXPGPATH];

    if (mkdir(todir, S_IRWXU) != 0  &&
        !(FILE_ALREADY_EXIST(errno) && IsRoachRestore())) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not create directory \"%s\": %m", todir)));
    }

    xldir = AllocateDir(fromdir);
    if (xldir == NULL) {
        ereport(elevel, (errcode_for_file_access(), errmsg("could not open directory \"%s\": %m", fromdir)));

        return false;
    }

    while ((xlde = ReadDir(xldir, fromdir)) != NULL) {
        struct stat fst;
        errno_t rc = EOK;
        /* If we got a cancel signal during the copy of the directory, quit */
        CHECK_FOR_INTERRUPTS();

        if (strcmp(xlde->d_name, ".") == 0 || strcmp(xlde->d_name, "..") == 0)
            continue;

        rc = snprintf_s(fromfile, MAXPGPATH, MAXPGPATH - 1, "%s/%s", fromdir, xlde->d_name);
        securec_check_ss(rc, "\0", "\0");

        rc = snprintf_s(tofile, MAXPGPATH, MAXPGPATH - 1, "%s/%s", todir, xlde->d_name);
        securec_check_ss(rc, "\0", "\0");

        if (lstat(fromfile, &fst) < 0)
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat file \"%s\": %m", fromfile)));

        if (S_ISDIR(fst.st_mode)) {
            /* recurse to handle subdirectories */
            if (recurse)
                (void)copydir(fromfile, tofile, true, elevel);
        } else if (S_ISREG(fst.st_mode))
            copy_file(fromfile, tofile);
    }
    (void)FreeDir(xldir);

    /*
     * Be paranoid here and fsync all files to ensure the copy is really done.
     * But if fsync is disabled, we're done.
     */
    if (!u_sess->attr.attr_storage.enableFsync) {
        return true;
    }

    xldir = AllocateDir(todir);
    if (xldir == NULL) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not open directory \"%s\": %m", todir)));
    }

    while ((xlde = ReadDir(xldir, todir)) != NULL) {
        struct stat fst;
        errno_t rc = EOK;

        if (strcmp(xlde->d_name, ".") == 0 || strcmp(xlde->d_name, "..") == 0) {
            continue;
        }

        rc = snprintf_s(tofile, MAXPGPATH, MAXPGPATH - 1, "%s/%s", todir, xlde->d_name);
        securec_check_ss(rc, "\0", "\0");
        /*
         * We don't need to sync subdirectories here since the recursive
         * copydir will do it before it returns
         */
        if (lstat(tofile, &fst) < 0)
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat file \"%s\": %m", tofile)));

        if (S_ISREG(fst.st_mode)) {
            fsync_fname(tofile, false);
        }
    }
    (void)FreeDir(xldir);

    /*
     * It's important to fsync the destination directory itself as individual
     * file fsyncs don't guarantee that the directory entry for the file is
     * synced. Recent versions of ext4 have made the window much wider but
     * it's been true for ext3 and other filesystems in the past.
     */
    fsync_fname(todir, true);

    return true;
}

/*
 * copy one file
 */
void copy_file_internal(char* fromfile, char* tofile, bool trunc_file)
{
    char* buffer = NULL;
    int srcfd;
    int dstfd;
    int dstflag;
    int nbytes;
    off_t offset;
    int buf_size;
    char* unalign_buffer = NULL;
    int align_nbytes;

    /* Use palloc to ensure we get a maxaligned buffer */
#define COPY_BUF_SIZE (8 * BLCKSZ)
    /* DSS needs large buffer to speed up */
#define COPY_BUF_SIZE_FOR_DSS (2 * 1024 * 1024)

    /* add extern BLCKSZ for protect memory overstep the boundary */
    buf_size = COPY_BUF_SIZE + BLCKSZ;

    if (ENABLE_DSS) {
        buf_size = COPY_BUF_SIZE_FOR_DSS + BLCKSZ;
        buf_size += ALIGNOF_BUFFER;
    }
    unalign_buffer = (char*)palloc0(buf_size);
 
    if (ENABLE_DSS) {
        buffer = (char*)BUFFERALIGN(unalign_buffer);
    } else {
        buffer = unalign_buffer;
    }

    /*
     * Open the files
     */
    srcfd = BasicOpenFile(fromfile, O_RDONLY | PG_BINARY, 0);
    if (srcfd < 0) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not open file \"%s\": %m", fromfile)));
    }

    dstflag = (trunc_file ? (O_RDWR | O_CREAT | O_TRUNC | PG_BINARY) : (O_RDWR | O_CREAT | O_EXCL | PG_BINARY));

    dstfd = BasicOpenFile(tofile, dstflag, S_IRUSR | S_IWUSR);
    if (dstfd < 0) {
        (void)close(srcfd);
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not create file \"%s\": %m", tofile)));
    }

    /*
     * Do the data copying.
     */
    int copy_step_size = ENABLE_DSS ? COPY_BUF_SIZE_FOR_DSS : COPY_BUF_SIZE;
    struct stat stat_buf;
    (void)stat(fromfile, &stat_buf);
    for (offset = 0;offset < stat_buf.st_size; offset += nbytes) {
        /* If we got a cancel signal during the copy of the file, quit */
        CHECK_FOR_INTERRUPTS();

        pgstat_report_waitevent(WAIT_EVENT_COPY_FILE_READ);
        nbytes = read(srcfd, buffer, copy_step_size);
        pgstat_report_waitevent(WAIT_EVENT_END);
        if (nbytes < 0) {
            (void)close(srcfd);
            (void)close(dstfd);
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not read file \"%s\": %m", fromfile)));
        }
        if (nbytes == 0) {
            break;
        }
        errno = 0;

        align_nbytes = nbytes;
        if (ENABLE_DSS && ((nbytes % ALIGNOF_BUFFER) != 0)) {
            align_nbytes = (int)BUFFERALIGN(nbytes);
        }

        pgstat_report_waitevent(WAIT_EVENT_COPY_FILE_WRITE);
        if ((int)write(dstfd, buffer, align_nbytes) != align_nbytes) {
            pgstat_report_waitevent(WAIT_EVENT_END);
            (void)close(srcfd);
            (void)close(dstfd);
            /* if write didn't set errno, assume problem is no disk space */
            if (errno == 0) {
                errno = ENOSPC;
            }
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not write to file \"%s\": %m", tofile)));
        }
        pgstat_report_waitevent(WAIT_EVENT_END);

        /*
         * We fsync the files later but first flush them to avoid spamming the
         * cache and hopefully get the kernel to start writing them out before
         * the fsync comes.
         */
        pg_flush_data(dstfd, offset, nbytes);
    }

    if (close(dstfd)) {
        (void)close(srcfd);
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not close file \"%s\": %m", tofile)));
    }

    (void)close(srcfd);

    pfree(unalign_buffer);
}

void copy_file(char* fromfile, char* tofile)
{
    copy_file_internal(fromfile, tofile, IsRoachRestore());
}

/*
 * fsync a file
 *
 * Try to fsync directories but ignore errors that indicate the OS
 * just doesn't allow/require fsyncing directories.
 */
void fsync_fname(const char* fname, bool isdir)
{
    (void)fsync_fname_ext(fname, isdir, false, data_sync_elevel(ERROR));
}

/*
 * durable_rename -- rename(2) wrapper, issuing fsyncs required for durability
 *
 * This routine ensures that, after returning, the effect of renaming file
 * persists in case of a crash. A crash while this routine is running will
 * leave you with either the pre-existing or the moved file in place of the
 * new file; no mixed state or truncated files are possible.
 *
 * It does so by using fsync on the old filename and the possibly existing
 * target filename before the rename, and the target file and directory after.
 *
 * Note that rename() cannot be used across arbitrary directories, as they
 * might not be on the same filesystem. Therefore this routine does not
 * support renaming across directories.
 *
 * Log errors with the caller specified severity.
 *
 * Returns 0 if the operation succeeded, -1 otherwise. Note that errno is not
 * valid upon return.
 */
int durable_rename(const char* oldfile, const char* newfile, int elevel)
{
    int fd;

    /*
     * First fsync the old and target path (if it exists), to ensure that they
     * are properly persistent on disk. Syncing the target file is not
     * strictly necessary, but it makes it easier to reason about crashes;
     * because it's then guaranteed that either source or target file exists
     * after a crash.
     */
    if (fsync_fname_ext(oldfile, false, false, elevel) != 0) {
        return -1;
    }

    errno = 0;
    fd = BasicOpenFile((char*)newfile, PG_BINARY | O_RDWR, 0);
    if (fd < 0) {
        if (!FILE_POSSIBLY_DELETED(errno)) {
            ereport(elevel, (errcode_for_file_access(), errmsg("could not open file \"%s\": %m", newfile)));
            return -1;
        }
    } else {
        if (pg_fsync(fd) != 0) {
            int save_errno;

            /* close file upon error, might not be in transaction context */
            save_errno = errno;
            (void)close(fd);
            errno = save_errno;

            ereport(elevel, (errcode_for_file_access(), errmsg("could not fsync file \"%s\": %m", newfile)));
            return -1;
        }
        (void)close(fd);
    }

    /* Time to do the real deal... */
    if (rename(oldfile, newfile) < 0) {
        ereport(elevel,
                (errcode_for_file_access(), errmsg("could not rename file \"%s\" to \"%s\": %m", oldfile, newfile)));
        return -1;
    }

    /*
     * To guarantee renaming the file is persistent, fsync the file with its
     * new name, and its containing directory.
     */
    if (fsync_fname_ext(newfile, false, false, elevel) != 0) {
        return -1;
    }

    if (fsync_parent_path(newfile, elevel) != 0) {
        return -1;
    }

    return 0;
}

/*
 * durable_link_or_rename -- rename a file in a durable manner.
 *
 * Similar to durable_rename(), except that this routine tries (but does not
 * guarantee) not to overwrite the target file.
 *
 * Note that a crash in an unfortunate moment can leave you with two links to
 * the target file.
 *
 * Log errors with the caller specified severity.
 *
 * Returns 0 if the operation succeeded, -1 otherwise. Note that errno is not
 * valid upon return.
 */
int durable_link_or_rename(const char* oldfile, const char* newfile, int elevel)
{
    /*
     * Ensure that, if we crash directly after the rename/link, a file with
     * valid contents is moved into place.
     */
    if (fsync_fname_ext(oldfile, false, false, elevel) != 0) {
        return -1;
    }

#if HAVE_WORKING_LINK
    if (link(oldfile, newfile) < 0) {
        ereport(
            elevel, (errcode_for_file_access(), errmsg("could not link file \"%s\" to \"%s\": %m", oldfile, newfile)));
        return -1;
    }
    (void)unlink(oldfile);
#else
    /* XXX: Add racy file existence check? */
    if (rename(oldfile, newfile) < 0) {
        ereport(elevel,
                (errcode_for_file_access(), errmsg("could not rename file \"%s\" to \"%s\": %m", oldfile, newfile)));
        return -1;
    }
#endif

    /*
     * Make change persistent in case of an OS crash, both the new entry and
     * its parent directory need to be flushed.
     */
    if (fsync_fname_ext(newfile, false, false, elevel) != 0) {
        return -1;
    }

    /* Same for parent directory */
    if (fsync_parent_path(newfile, elevel) != 0) {
        return -1;
    }

    return 0;
}

/*
 * fsync_fname_ext -- Try to fsync a file or directory
 *
 * If ignore_perm is true, ignore errors upon trying to open unreadable
 * files. Logs other errors at a caller-specified level.
 *
 * Returns 0 if the operation succeeded, -1 otherwise.
 */
static int fsync_fname_ext(const char* fname, bool isdir, bool ignore_perm, int elevel)
{
    int fd;
    int flags;
    int returncode;

    if (is_dss_file(fname)) {
        return 0;
    }
    /*
     * Some OSs require directories to be opened read-only whereas other
     * systems don't allow us to fsync files opened read-only; so we need both
     * cases here.  Using O_RDWR will cause us to fail to fsync files that are
     * not writable by our userid, but we assume that's OK.
     */
    flags = PG_BINARY;
    if (!isdir) {
        flags |= O_RDWR;
    } else {
        flags |= O_RDONLY;
    }

    errno = 0;
    fd = BasicOpenFile((char*)fname, flags, 0);
    /*
     * Some OSs don't allow us to open directories at all (Windows returns
     * EACCES), just ignore the error in that case.  If desired also silently
     * ignoring errors about unreadable files. Log others.
     */
    if (fd < 0 && isdir && (errno == EISDIR || errno == EACCES)) {
        return 0;
    } else if (fd < 0 && ignore_perm && errno == EACCES) {
        return 0;
    } else if (fd < 0) {
        ereport(elevel, (errcode_for_file_access(), errmsg("could not open file \"%s\": %m", fname)));
        return -1;
    }

    returncode = pg_fsync(fd);
    /*
     * Some OSes don't allow us to fsync directories at all, so we can ignore
     * those errors. Anything else needs to be logged.
     */
    if (returncode != 0 && !(isdir && errno == EBADF)) {
        int save_errno;

        /* close file upon error, might not be in transaction context */
        save_errno = errno;
        (void)close(fd);
        errno = save_errno;

        ereport(elevel, (errcode_for_file_access(), errmsg("could not fsync file \"%s\": %m", fname)));
        return -1;
    }

    (void)close(fd);

    return 0;
}

/*
 * fsync_parent_path -- fsync the parent path of a file or directory
 *
 * This is aimed at making file operations persistent on disk in case of
 * an OS crash or power failure.
 */
static int fsync_parent_path(const char* fname, int elevel)
{
    char parentpath[MAXPGPATH];
    errno_t retcode;

    if (is_dss_file(fname)) {
        return 0;
    }
    
    retcode = strncpy_s(parentpath, MAXPGPATH, fname, strlen(fname));
    securec_check(retcode, "\0", "\0");
    get_parent_directory(parentpath);

    /*
     * get_parent_directory() returns an empty string if the input argument is
     * just a file name (see comments in path.c), so handle that as being the
     * current directory.
     */
    if (strlen(parentpath) == 0) {
        retcode = strncpy_s(parentpath, MAXPGPATH, ".", strlen("."));
        securec_check(retcode, "\0", "\0");
    }

    if (fsync_fname_ext(parentpath, true, false, elevel) != 0) {
        return -1;
    }

    return 0;
}
