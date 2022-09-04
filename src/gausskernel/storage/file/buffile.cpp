/* -------------------------------------------------------------------------
 *
 * buffile.cpp
 *	  Management of large buffered files, primarily temporary files.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/file/buffile.cpp
 *
 * NOTES:
 *
 * BufFiles provide a very incomplete emulation of stdio atop virtual Files
 * (as managed by fd.c).  Currently, we only support the buffered-I/O
 * aspect of stdio: a read or write of the low-level File occurs only
 * when the buffer is filled or emptied.  This is an even bigger win
 * for virtual Files than for ordinary kernel files, since reducing the
 * frequency with which a virtual File is touched reduces "thrashing"
 * of opening/closing file descriptors.
 *
 * Note that BufFile structs are allocated with palloc(), and therefore
 * will go away automatically at transaction end.  If the underlying
 * virtual File is made with OpenTemporaryFile, then all resources for
 * the file are certain to be cleaned up even if processing is aborted
 * by ereport(ERROR).  The data structures required are made in the
 * palloc context that was current when the BufFile was created, and
 * any external resources such as temp files are owned by the ResourceOwner
 * that was current at that time.
 *
 * BufFile also supports temporary files that exceed the OS file size limit
 * (by opening multiple fd.c temporary files).	This is an essential feature
 * for sorts and hashjoins on large amounts of data.
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "executor/instrument.h"
#include "pgstat.h"
#include "storage/smgr/fd.h"
#include "storage/buf/buffile.h"
#include "storage/buf/buf_internals.h"
#include "utils/aiomem.h"
#include "utils/resowner.h"
#include "miscadmin.h"
/*
 * We break BufFiles into gigabyte-sized segments, regardless of RELSEG_SIZE.
 * The reason is that we'd like large temporary BufFiles to be spread across
 * multiple tablespaces when available.
 */
#define MAX_PHYSICAL_FILESIZE 0x40000000
#define BUFFILE_SEG_SIZE (MAX_PHYSICAL_FILESIZE / BLCKSZ)

/*
 * This data structure represents a buffered file that consists of one or
 * more physical files (each accessed through a virtual file descriptor
 * managed by fd.c).
 */
struct BufFile {
    int numFiles; /* number of physical files in set */
    /* all files except the last have length exactly MAX_PHYSICAL_FILESIZE */
    File* files;    /* palloc'd array with numFiles entries */
    off_t* offsets; /* palloc'd array with numFiles entries */

    /*
     * offsets[i] is the current seek position of files[i].  We use this to
     * avoid making redundant FileSeek calls.
     */
    bool isTemp;      /* can only add files if this is TRUE */
    bool isInterXact; /* keep open over transactions? */
    bool dirty;       /* does buffer need to be written? */

    bool readOnly;            /* has the file been set to read only? */
    SharedFileSet *fileset;   /* space for segment files if shared */
    const char *name;         /* name of this BufFile if shared */

    /*
     * resowner is the ResourceOwner to use for underlying temp files.	(We
     * don't need to remember the memory context we're using explicitly,
     * because after creation we only repalloc our arrays larger.)
     */
    ResourceOwner resowner;

    /*
     * "current pos" is position of start of buffer within the logical file.
     * Position as seen by user of BufFile is (curFile, curOffset + pos).
     */
    int curFile;     /* file index (0..n) part of current pos */
    off_t curOffset; /* offset part of current pos */
    int pos;         /* next read/write position in buffer */
    int nbytes;      /* total # of valid bytes in buffer */
    char* buffer;    /* adio need pointer align */

    char pad; /* extra 1 byte, just a workaround for the memory issue of pread */
};

static BufFile* makeBufFile(File firstfile);
static void extendBufFile(BufFile* file);
static void BufFileLoadBuffer(BufFile* file);
static void BufFileDumpBuffer(BufFile* file);
static int BufFileFlush(BufFile* file);
static File MakeNewSharedSegment(BufFile *file, int segment);

/*
 * Create a BufFile given the first underlying physical file.
 * NOTE: caller must set isTemp and isInterXact if appropriate.
 */
static BufFile* makeBufFile(File firstfile)
{
    BufFile* file = NULL;
    /*
     * In ADIO scene, the pointer file->buffer must BLCKSZ byte align, so we need to palloc another BLOCK.
     * AlignMemoryContext will be reset when the transaction aborts, we should alloc the buffile in
     * CurrentMemoryContext rather than AlignMemoryContext, because the buffile may live in different transactions.
     */
    ADIO_RUN()
    {
        file = (BufFile*)palloc0(sizeof(BufFile) + BLCKSZ + BLCKSZ);
        file->buffer = ((char*)file) + sizeof(BufFile);
        file->buffer = (char*)TYPEALIGN(BLCKSZ, file->buffer);
    }
    ADIO_ELSE()
    {
        file = (BufFile*)palloc0(sizeof(BufFile) + BLCKSZ);
        file->buffer = ((char*)file) + sizeof(BufFile);
    }
    ADIO_END();

    file->numFiles = 1;
    file->files = (File*)palloc(sizeof(File));
    file->files[0] = firstfile;
    file->offsets = (off_t*)palloc(sizeof(off_t));
    file->offsets[0] = 0L;
    file->isTemp = false;
    file->isInterXact = false;
    file->dirty = false;
    file->resowner = t_thrd.utils_cxt.CurrentResourceOwner;
    file->curFile = 0;
    file->curOffset = 0L;
    file->pos = 0;
    file->nbytes = 0;
    file->pad = '\0';
    file->readOnly = false;
    file->fileset = NULL;
    file->name = NULL;

    return file;
}

/*
 * Add another component temp file.
 */
static void extendBufFile(BufFile* file)
{
    File pfile;
    ResourceOwner oldowner;

    /* Be sure to associate the file with the BufFile's resource owner */
    oldowner = t_thrd.utils_cxt.CurrentResourceOwner;
    t_thrd.utils_cxt.CurrentResourceOwner = file->resowner;

    Assert(file->isTemp);
    if (file->fileset == NULL)
        pfile = OpenTemporaryFile(file->isInterXact);
    else
        pfile = MakeNewSharedSegment(file, file->numFiles);
    Assert(pfile >= 0);
    t_thrd.utils_cxt.CurrentResourceOwner = oldowner;

    file->files = (File*)repalloc(file->files, (file->numFiles + 1) * sizeof(File));
    file->offsets = (off_t*)repalloc(file->offsets, (file->numFiles + 1) * sizeof(off_t));
    file->files[file->numFiles] = pfile;
    file->offsets[file->numFiles] = 0L;
    file->numFiles++;
}

/*
 * Create a BufFile for a new temporary file (which will expand to become
 * multiple temporary files if more than MAX_PHYSICAL_FILESIZE bytes are
 * written to it).
 *
 * If interXact is true, the temp file will not be automatically deleted
 * at end of transaction.
 *
 * Note: if interXact is true, the caller had better be calling us in a
 * memory context, and with a resource owner, that will survive across
 * transaction boundaries.
 */
static BufFile* CreateTempBufFile(bool interXact)
{
    BufFile* file = NULL;
    File pfile;

    pfile = OpenTemporaryFile(interXact);
    Assert(pfile >= 0);

    file = makeBufFile(pfile);
    file->isTemp = true;
    file->isInterXact = interXact;

    return file;
}

BufFile* BufFileCreateTemp(bool inter_xact)
{
    /* create main temp buffer file */
    BufFile* main_buf_file = CreateTempBufFile(inter_xact);

    return main_buf_file;
}

/*
 * Build the name for a given segment of a given BufFile.
 */
static void SharedSegmentName(char *name, const char *buffile_name, int segment)
{
    errno_t err_rc = snprintf_s(name, MAXPGPATH, MAXPGPATH - 1, "%s.%d", buffile_name, segment);
    securec_check_ss(err_rc, "", "");
}

/*
 * Create a new segment file backing a shared BufFile.
 */
static File MakeNewSharedSegment(BufFile *buffile, int segment)
{
    char name[MAXPGPATH];
    File file;

    SharedSegmentName(name, buffile->name, segment);
    file = SharedFileSetCreate(buffile->fileset, name);

    /* SharedFileSetCreate would've errored out */
    Assert(file > 0);

    return file;
}

/*
 * Create a BufFile that can be discovered and opened read-only by other
 * backends that are attached to the same SharedFileSet using the same name.
 *
 * The naming scheme for shared BufFiles is left up to the calling code.  The
 * name will appear as part of one or more filenames on disk, and might
 * provide clues to administrators about which subsystem is generating
 * temporary file data.  Since each SharedFileSet object is backed by one or
 * more uniquely named temporary directory, names don't conflict with
 * unrelated SharedFileSet objects.
 */
BufFile *BufFileCreateShared(SharedFileSet *fileset, const char *name)
{
    BufFile *file;

    file = (BufFile *)palloc(sizeof(BufFile) + BLCKSZ);
    file->buffer = ((char *)file) + sizeof(BufFile);
    file->fileset = fileset;
    file->name = pstrdup(name);
    file->numFiles = 1;
    file->files = (File *)palloc(sizeof(File));
    file->files[0] = MakeNewSharedSegment(file, 0);
    file->offsets = (off_t *)palloc(sizeof(off_t));
    file->offsets[0] = 0L;
    file->isInterXact = false;
    file->dirty = false;
    file->resowner = t_thrd.utils_cxt.CurrentResourceOwner;
    file->curFile = 0;
    file->curOffset = 0L;
    file->pos = 0;
    file->nbytes = 0;
    file->readOnly = false;
    file->isTemp = true;

    return file;
}

/*
 * Open a file that was previously created in another backend (or this one)
 * with BufFileCreateShared in the same SharedFileSet using the same name.
 * The backend that created the file must have called BufFileClose() or
 * BufFileExport() to make sure that it is ready to be opened by other
 * backends and render it read-only.
 */
BufFile *BufFileOpenShared(SharedFileSet *fileset, const char *name)
{
    BufFile *file = (BufFile *)palloc(sizeof(BufFile));
    char segment_name[MAXPGPATH];
    Size capacity = 16;
    File *files = (File *)palloc(sizeof(File) * capacity);
    int nfiles = 0;

    file = (BufFile *)palloc(sizeof(BufFile) + BLCKSZ);
    files = (File *)palloc(sizeof(File) * capacity);

    /*
     * We don't know how many segments there are, so we'll probe the
     * filesystem to find out.
     */
    for (;;) {
        /* See if we need to expand our file segment array. */
        if ((uint32)nfiles + 1 > capacity) {
            capacity *= 2;
            files = (File *)repalloc(files, sizeof(File) * capacity);
        }
        /* Try to load a segment. */
        SharedSegmentName(segment_name, name, nfiles);
        files[nfiles] = SharedFileSetOpen(fileset, segment_name);
        if (files[nfiles] <= 0)
            break;
        ++nfiles;

        CHECK_FOR_INTERRUPTS();
    }

    /*
     * If we didn't find any files at all, then no BufFile exists with this
     * name.
     */
    if (nfiles == 0)
        return NULL;

    file->buffer = ((char *)file) + sizeof(BufFile);
    file->numFiles = nfiles;
    file->files = files;
    file->offsets = (off_t *)palloc0(sizeof(off_t) * nfiles);
    file->isInterXact = false;
    file->dirty = false;
    file->resowner = t_thrd.utils_cxt.CurrentResourceOwner; /* Unused, can't extend */
    file->curFile = 0;
    file->curOffset = 0L;
    file->pos = 0;
    file->nbytes = 0;
    file->readOnly = true; /* Can't write to files opened this way */
    file->fileset = fileset;
    file->name = pstrdup(name);
    file->isTemp = true;

    return file;
}

/*
 * Delete a BufFile that was created by BufFileCreateShared in the given
 * SharedFileSet using the given name.
 *
 * It is not necessary to delete files explicitly with this function.  It is
 * provided only as a way to delete files proactively, rather than waiting for
 * the SharedFileSet to be cleaned up.
 *
 * Only one backend should attempt to delete a given name, and should know
 * that it exists and has been exported or closed.
 */
void BufFileDeleteShared(SharedFileSet *fileset, const char *name)
{
    char segment_name[MAXPGPATH];
    int segment = 0;
    bool found = false;

    /*
     * We don't know how many segments the file has.  We'll keep deleting
     * until we run out.  If we don't manage to find even an initial segment,
     * raise an error.
     */
    for (;;) {
        SharedSegmentName(segment_name, name, segment);
        if (!SharedFileSetDelete(fileset, segment_name, true))
            break;
        found = true;
        ++segment;

        CHECK_FOR_INTERRUPTS();
    }

    if (!found)
        elog(ERROR, "could not delete unknown shared BufFile \"%s\"", name);
}

/*
 * BufFileExportShared --- flush and make read-only, in preparation for sharing.
 */
void BufFileExportShared(BufFile *file)
{
    /* Must be a file belonging to a SharedFileSet. */
    Assert(file->fileset != NULL);

    /* It's probably a bug if someone calls this twice. */
    Assert(!file->readOnly);

    BufFileFlush(file);
    file->readOnly = true;
}

/*
 * Close a BufFile
 *
 * Like fclose(), this also implicitly FileCloses the underlying File.
 */
static void CloseTempBufFile(BufFile* file)
{
    int i;

    /* flush any unwritten data */
    (void)BufFileFlush(file);
    /* close the underlying file(s) (with delete if it's a temp file) */
    for (i = 0; i < file->numFiles; i++) {
        FileClose(file->files[i]);
    }
    /* release the buffer space */
    pfree(file->files);
    pfree(file->offsets);
    pfree(file);
}

void BufFileClose(BufFile* file)
{
    /* close main temp buf file */
    CloseTempBufFile(file);
}

/*
 * BufFileLoadBuffer
 *
 * Load some data into buffer, if possible, starting from curOffset.
 * At call, must have dirty = false, pos and nbytes = 0.
 * On exit, nbytes is number of bytes loaded.
 */
static void BufFileLoadBuffer(BufFile* file)
{
    File thisfile;

    /*
     * Advance to next component file if necessary and possible.
     *
     * This path can only be taken if there is more than one component, so it
     * won't interfere with reading a non-temp file that is over
     * MAX_PHYSICAL_FILESIZE.
     */
    if (file->curOffset >= MAX_PHYSICAL_FILESIZE && file->curFile + 1 < file->numFiles) {
        file->curFile++;
        file->curOffset = 0L;
    }

    /*
     * May need to reposition physical file.
     */
    thisfile = file->files[file->curFile];
    if (file->curOffset != file->offsets[file->curFile]) {
        file->offsets[file->curFile] = file->curOffset;
    }

    /*
     * Read whatever we can get, up to a full bufferload.
     */
    file->nbytes = FilePRead(thisfile, file->buffer, BLCKSZ, file->curOffset, WAIT_EVENT_BUFFILE_READ);

#ifdef MEMORY_CONTEXT_CHECKING
#ifndef ENABLE_MEMORY_CHECK
    AllocSetCheckPointer(file);
#endif
#endif

    if (file->nbytes < 0) {
        file->nbytes = 0;
    }

    file->offsets[file->curFile] += file->nbytes;
    /* we choose not to advance curOffset here */
    u_sess->instr_cxt.pg_buffer_usage->temp_blks_read++;
}

/*
 * Dump buffer contents starting at curOffset.
 * At call, should have dirty = true, nbytes > 0.
 * On exit, dirty is cleared if successful write, and curOffset is advanced.
 */
static void BufFileDumpBuffer(BufFile* file)
{
    int wpos = 0;
    int bytestowrite;
    File thisfile;

    /*
     * Unlike BufFileLoadBuffer, we must dump the whole buffer even if it
     * crosses a component-file boundary; so we need a loop.
     */
    while (wpos < file->nbytes) {
        /*
         * Advance to next component file if necessary and possible.
         */
        if (file->curOffset >= MAX_PHYSICAL_FILESIZE && file->isTemp) {
            while (file->curFile + 1 >= file->numFiles) {
                extendBufFile(file);
            }
            file->curFile++;
            file->curOffset = 0L;
        }

        /*
         * Enforce per-file size limit only for temp files, else just try to
         * write as much as asked...
         */
        bytestowrite = file->nbytes - wpos;
        if (file->isTemp) {
            off_t availbytes = MAX_PHYSICAL_FILESIZE - file->curOffset;

            if ((off_t)bytestowrite > availbytes) {
                bytestowrite = (int)availbytes;
            }
        }

        /*
         * May need to reposition physical file.
         */
        thisfile = file->files[file->curFile];
        if (file->curOffset != file->offsets[file->curFile]) {
            file->offsets[file->curFile] = file->curOffset;
        }
        bytestowrite =
            FilePWrite(thisfile, file->buffer + wpos, bytestowrite, file->curOffset, (uint32)WAIT_EVENT_BUFFILE_WRITE);
        if (bytestowrite <= 0) {
            return; /* failed to write */
        }

        file->offsets[file->curFile] += bytestowrite;
        file->curOffset += bytestowrite;
        wpos += bytestowrite;

        u_sess->instr_cxt.pg_buffer_usage->temp_blks_written++;
    }
    file->dirty = false;

    /*
     * At this point, curOffset has been advanced to the end of the buffer,
     * ie, its original value + nbytes.  We need to make it point to the
     * logical file position, ie, original value + pos, in case that is less
     * (as could happen due to a small backwards seek in a dirty buffer!)
     */
    file->curOffset -= (file->nbytes - file->pos);
    if (file->curOffset < 0) { /* handle possible segment crossing */
        file->curFile--;
        Assert(file->curFile >= 0);
        file->curOffset += MAX_PHYSICAL_FILESIZE;
    }

    /*
     * Now we can set the buffer empty without changing the logical position
     */
    file->pos = 0;
    file->nbytes = 0;
}

/*
 * BufFileRead
 *
 * Like fread() except we assume 1-byte element size.
 * NOTE: We change return value(0->EOF) when BufFileFlush failed,
 * becase some caller can not distinguish normal return or error return.
 * and in this case, param size can not set to (size_t)(-1), becasue
 * we treat it as error condition.
 */
size_t BufFileRead(BufFile* file, void* ptr, size_t size)
{
    size_t nread = 0;
    size_t nthistime;

    if (file->dirty) {
        if (BufFileFlush(file) != 0) {
            return (size_t)EOF; /* could not flush... */
        }
        Assert(!file->dirty);
    }

    while (size > 0) {
        if (file->pos >= file->nbytes) {
            /* Try to load more data into buffer. */
            file->curOffset += file->pos;
            file->pos = 0;
            file->nbytes = 0;
            BufFileLoadBuffer(file);
            if (file->nbytes <= 0) {
                break; /* no more data available */
            }
        }

        nthistime = file->nbytes - file->pos;
        if (nthistime > size) {
            nthistime = size;
        }
        Assert(nthistime > 0);

        errno_t rc = memcpy_s(ptr, nthistime, file->buffer + file->pos, nthistime);
        securec_check(rc, "\0", "\0");

        file->pos += nthistime;
        ptr = (void*)((char*)ptr + nthistime);
        size -= nthistime;
        nread += nthistime;
    }

    return nread;
}

/*
 * BufFileWrite
 *
 * Like fwrite() except we assume 1-byte element size.
 */
size_t BufFileWrite(BufFile* file, void* ptr, size_t size)
{
    size_t nwritten = 0;
    size_t nthistime;
    errno_t rc = EOK;

    Assert(!file->readOnly);

    while (size > 0) {
        if (file->pos >= BLCKSZ) {
            /* Buffer full, dump it out */
            if (file->dirty) {
                BufFileDumpBuffer(file);
                if (file->dirty) {
                    break; /* I/O error */
                }
            } else {
                /* Hmm, went directly from reading to writing? */
                file->curOffset += file->pos;
                file->pos = 0;
                file->nbytes = 0;
            }
        }

        nthistime = BLCKSZ - file->pos;
        if (nthistime > size) {
            nthistime = size;
        }
        Assert(nthistime > 0);

        rc = memcpy_s(file->buffer + file->pos, nthistime, ptr, nthistime);
        securec_check(rc, "", "");
#ifdef MEMORY_CONTEXT_CHECKING
#ifndef ENABLE_MEMORY_CHECK
        AllocSetCheckPointer(file);
#endif
#endif

        file->dirty = true;
        file->pos += nthistime;
        if (file->nbytes < file->pos) {
            file->nbytes = file->pos;
        }
        ptr = (void*)((char*)ptr + nthistime);
        size -= nthistime;
        nwritten += nthistime;
    }

    return nwritten;
}

/*
 * BufFileFlush
 *
 * Like fflush()
 */
static int BufFileFlush(BufFile* file)
{
    if (file->dirty) {
        BufFileDumpBuffer(file);
        if (file->dirty) {
            return EOF;
        }
    }

    return 0;
}

/*
 * BufFileSeek
 *
 * Like fseek(), except that target position needs two values in order to
 * work when logical filesize exceeds maximum value representable by long.
 * We do not support relative seeks across more than LONG_MAX, however.
 *
 * Result is 0 if OK, EOF if not.  Logical position is not moved if an
 * impossible seek is attempted.
 */
int BufFileSeek(BufFile* file, int fileno, off_t offset, int whence)
{
    int new_file;
    off_t new_offset;

    switch (whence) {
        case SEEK_SET:
            if (fileno < 0) {
                return EOF;
            }
            new_file = fileno;
            new_offset = offset;
            break;
        case SEEK_CUR:

            /*
             * Relative seek considers only the signed offset, ignoring
             * fileno. Note that large offsets (> 1 gig) risk overflow in this
             * add, unless we have 64-bit off_t.
             */
            new_file = file->curFile;
            new_offset = (file->curOffset + file->pos) + offset;
            break;
#ifdef NOT_USED
        case SEEK_END:
            /* could be implemented, not needed currently */
            break;
#endif
        default:
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_PARAMETER), errmsg("invalid whence: %d", whence)));
            return EOF;
    }
    while (new_offset < 0) {
        if (--new_file < 0) {
            return EOF;
        }
        new_offset += MAX_PHYSICAL_FILESIZE;
    }
    if (new_file == file->curFile && new_offset >= file->curOffset && new_offset <= file->curOffset + file->nbytes) {
        /*
         * Seek is to a point within existing buffer; we can just adjust
         * pos-within-buffer, without flushing buffer.	Note this is OK
         * whether reading or writing, but buffer remains dirty if we were
         * writing.
         */
        file->pos = (int)(new_offset - file->curOffset);
        return 0;
    }
    /* Otherwise, must reposition buffer, so flush any dirty data */
    if (BufFileFlush(file) != 0) {
        return EOF;
    }

    /*
     * At this point and no sooner, check for seek past last segment. The
     * above flush could have created a new segment, so checking sooner would
     * not work (at least not with this code).
     */
    if (file->isTemp) {
        /* convert seek to "start of next seg" to "end of last seg" */
        if (new_file == file->numFiles && new_offset == 0) {
            new_file--;
            new_offset = MAX_PHYSICAL_FILESIZE;
        }
        while (new_offset > MAX_PHYSICAL_FILESIZE) {
            if (++new_file >= file->numFiles) {
                return EOF;
            }
            new_offset -= MAX_PHYSICAL_FILESIZE;
        }
    }
    if (new_file >= file->numFiles) {
        return EOF;
    }
    /* Seek is OK! */
    file->curFile = new_file;
    file->curOffset = new_offset;
    file->pos = 0;
    file->nbytes = 0;

    return 0;
}

void BufFileTell(BufFile* file, int* fileno, off_t* offset)
{
    *fileno = file->curFile;
    *offset = file->curOffset + file->pos;
}

/*
 * BufFileSeekBlock --- block-oriented seek
 *
 * Performs absolute seek to the start of the n'th BLCKSZ-sized block of
 * the file.  Note that users of this interface will fail if their files
 * exceed BLCKSZ * LONG_MAX bytes, but that is quite a lot; we don't work
 * with tables bigger than that, either...
 *
 * Result is 0 if OK, EOF if not.  Logical position is not moved if an
 * impossible seek is attempted.
 */
int BufFileSeekBlock(BufFile* file, long blknum)
{
    return BufFileSeek(file, (int)(blknum / BUFFILE_SEG_SIZE), (off_t)(blknum % BUFFILE_SEG_SIZE) * BLCKSZ, SEEK_SET);
}

#ifdef NOT_USED
/*
 * BufFileTellBlock --- block-oriented tell
 *
 * Any fractional part of a block in the current seek position is ignored.
 */
long BufFileTellBlock(BufFile* file)
{
    long blknum;

    blknum = (file->curOffset + file->pos) / BLCKSZ;
    blknum += file->curFile * BUFFILE_SEG_SIZE;
    return blknum;
}

#endif

/*
 * Return the current shared BufFile size.
 *
 * Counts any holes left behind by BufFileAppend as part of the size.
 * ereport()s on failure.
 */
int64 BufFileSize(BufFile *file)
{
    int64 lastFileSize;

    Assert(file->fileset != NULL);

    /* Get the size of the last physical file by seeking to end. */
    lastFileSize = FileSeek(file->files[file->numFiles - 1], 0, SEEK_END);
    if (lastFileSize < 0) {
        ereport(ERROR, (errcode_for_file_access(),
            errmsg("could not determine size of temporary file \"%s\" from BufFile \"%s\": %m",
            FilePathName(file->files[file->numFiles - 1]), file->name)));
    }
    file->offsets[file->numFiles - 1] = lastFileSize;

    return ((file->numFiles - 1) * (int64)MAX_PHYSICAL_FILESIZE) + lastFileSize;
}

/*
 * Append the contents of source file (managed within shared fileset) to
 * end of target file (managed within same shared fileset).
 *
 * Note that operation subsumes ownership of underlying resources from
 * "source".  Caller should never call BufFileClose against source having
 * called here first.  Resource owners for source and target must match,
 * too.
 *
 * This operation works by manipulating lists of segment files, so the
 * file content is always appended at a MAX_PHYSICAL_FILESIZE-aligned
 * boundary, typically creating empty holes before the boundary.  These
 * areas do not contain any interesting data, and cannot be read from by
 * caller.
 *
 * Returns the block number within target where the contents of source
 * begins.  Caller should apply this as an offset when working off block
 * positions that are in terms of the original BufFile space.
 */
long BufFileAppend(BufFile *target, BufFile *source)
{
    long startBlock = target->numFiles * BUFFILE_SEG_SIZE;
    int newNumFiles = target->numFiles + source->numFiles;
    int i;

    Assert(target->fileset != NULL);
    Assert(source->readOnly);
    Assert(!source->dirty);
    Assert(source->fileset != NULL);

    if (target->resowner != source->resowner)
        elog(ERROR, "could not append BufFile with non-matching resource owner");

    target->files = (File *)repalloc(target->files, sizeof(File) * newNumFiles);
    target->offsets = (off_t *)repalloc(target->offsets, sizeof(off_t) * newNumFiles);
    for (i = target->numFiles; i < newNumFiles; i++) {
        target->files[i] = source->files[i - target->numFiles];
        target->offsets[i] = source->offsets[i - target->numFiles];
    }
    target->numFiles = newNumFiles;

    return startBlock;
}

