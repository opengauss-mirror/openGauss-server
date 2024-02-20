/* -------------------------------------------------------------------------
 *
 * fd.cpp
 *	  Virtual file descriptor code.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/file/fd.cpp
 *
 * NOTES:
 *
 * This code manages a cache of 'virtual' file descriptors (VFDs).
 * The server opens many file descriptors for a variety of reasons,
 * including base tables, scratch files (e.g., sort and hash spool
 * files), and random calls to C library routines like system(3); it
 * is quite easy to exceed system limits on the number of open files a
 * single process can have.  (This is around 256 on many modern
 * operating systems, but can be as low as 32 on others.)
 *
 * VFDs are managed as an LRU pool, with actual OS file descriptors
 * being opened and closed as needed.  Obviously, if a routine is
 * opened using these interfaces, all subsequent operations must also
 * be through these interfaces (the File type is not a real file
 * descriptor).
 *
 * For this scheme to work, most (if not all) routines throughout the
 * server should use these interfaces instead of calling the C library
 * routines (e.g., open(2) and fopen(3)) themselves.  Otherwise, we
 * may find ourselves short of real file descriptors anyway.
 * INTERFACE ROUTINES
 *
 * PathNameOpenFile and OpenTemporaryFile are used to open virtual files.
 * A File opened with OpenTemporaryFile is automatically deleted when the
 * File is closed, either explicitly or implicitly at end of transaction or
 * process exit. PathNameOpenFile is intended for files that are held open
 * for a long time, like relation files. It is the caller's responsibility
 * to close them, there is no automatic mechanism in fd.c for that.
 *
 * AllocateFile, AllocateDir and OpenTransientFile are wrappers around
 * fopen(3), opendir(3), and open(2), respectively. They behave like the
 * corresponding native functions, except that the handle is registered with
 * the current subtransaction, and will be automatically closed at abort.
 * These are intended for short operations like reading a configuration file.
 * and there is a fixed limit on the number files that can be open using these
 * functions at any one time.
 *
 * Finally, BasicOpenFile is a just thin wrapper around open() that can
 * release file descriptors in use by the virtual file descriptors if
 * necessary. There is no automatic cleanup of file descriptors returned by
 * BasicOpenFile, it is solely the caller's responsibility to close the file
 * descriptor by calling close(2).
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <arpa/inet.h>
#include <sys/file.h>
#include <sys/ioctl.h>
#include <sys/param.h>
#ifndef WIN32
    #include <sys/mman.h>
#endif
#ifdef HAVE_SYS_RESOURCE_H
    #include <sys/resource.h> /* for getrlimit */
#endif
#include <poll.h>

#include "miscadmin.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/catalog.h"
#include "catalog/pg_tablespace.h"
#include "catalog/namespace.h"
#include "distributelayer/streamCore.h"
#include "executor/executor.h"
#include "pgstat.h"
#include "storage/smgr/fd.h"
#include "storage/vfd.h"
#include "storage/ipc.h"
#include "storage/shmem.h"
#include "storage/file/fio_device.h"
#include "threadpool/threadpool.h"
#include "utils/guc.h"
#include "utils/plog.h"
#include "utils/resowner.h"
#include "pgstat.h"
#ifdef PGXC
    #include "pgxc/pgxc.h"
#endif
#include "libpq/ip.h"
#include "postmaster/aiocompleter.h"
#include "storage/lock/lwlock.h"
#include "pgxc/globalStatistic.h"

#include <linux/falloc.h>

/*
 * We must leave some file descriptors free for system(), the dynamic loader,
 * and other code that tries to open files without consulting fd.c.  This
 * is the number left free.  (While we can be pretty sure we won't get
 * EMFILE, there's never any guarantee that we won't get ENFILE due to
 * other processes chewing up FDs.	So it's a bad idea to try to open files
 * without consulting fd.c.  Nonetheless we cannot control all code.)
 *
 * Because this is just a fixed setting, we are effectively assuming that
 * no such code will leave FDs open over the long term; otherwise the slop
 * is likely to be insufficient.  Note in particular that we expect that
 * loading a shared library does not result in any permanent increase in
 * the number of open files.  (This appears to be true on most if not
 * all platforms as of Feb 2004.)
 */
#define NUM_RESERVED_FDS 10

/*
 * If we have fewer than this many usable FDs after allowing for the reserved
 * ones, choke.
 */
#define FD_MINFREE 10

/* connection timeout, sec. */
#define CONNECTION_TIME_OUT (5 * 60)

#ifdef FDDEBUG
#define DO_DB(A)                       \
    do {                               \
        int _do_db_save_errno = errno; \
        A;                             \
        errno = _do_db_save_errno;     \
    } while (0)
#else
#define DO_DB(A) ((void)0)
#endif

#define VFD_CLOSED (-1)

#define FileIsValid(file)                                            \
    ((file) > 0 && (file) < (int)GetSizeVfdCache() && \
     GetVfdCache()[file].fileName != NULL)

#define FileIsNotOpen(file) (GetVfdCache()[file].fd == VFD_CLOSED)

#define FileUnknownPos ((off_t)-1)

/* these are the assigned bits in fdstate below: */
#define FD_DELETE_AT_CLOSE  (1 << 0)    /* T = delete when closed */
#define FD_CLOSE_AT_EOXACT  (1 << 1)    /* T = close at eoXact */
#define FD_TEMP_FILE_LIMIT  (1 << 2)    /* T = respect temp_file_limit */
#define FD_ERRTBL_LOG       (1 << 3)    /* T = caching log file for error table */
#define FD_ERRTBL_LOG_OWNER (1 << 4)    /* T = owner of caching log file for error table */

#define RETRY_LIMIT 3 /* alloc socket retry limit */

static THR_LOCAL Vfd* save_VfdCache = NULL;
static THR_LOCAL Size save_SizeVfdCache = 0;

#ifdef USE_ASSERT_CHECKING
static DIR* g_gauss_pid_dir = NULL;
static char g_gauss_pid_dir_path[MAXPGPATH + 1] = {0};
#endif

// invalid filenode used in PathNameOpenFile
static RelFileNodeForkNum g_invalid_file_node;

typedef enum { AllocateDescFile, AllocateDescDir, AllocateDescRawFD, AllocateDescSocket } AllocateDescKind;

typedef struct AllocateDesc {
    AllocateDescKind kind;
    union {
        FILE* file;
        DIR* dir;
        int sock;
        int fd;
    } desc;
    SubTransactionId create_subid;
} AllocateDesc;

/* Number of partions the Vfd hashtable */
#define NUM_VFD_PARTITIONS 1024
static pthread_mutex_t VFDLockArray[NUM_VFD_PARTITIONS];

/*
 * The VFD mapping table is partitioned to reduce contention.
 * To determine which partition lock a given tag requires, compute the tag's
 * hash code with VFDTableHashCode(), then apply VFDMappingPartitionLock().
 */
#define VFDTableHashPartition(hashcode) ((hashcode) % NUM_VFD_PARTITIONS)
#define VFDMappingPartitionLock(hashcode) \
    (&VFDLockArray[VFDTableHashPartition(hashcode)])

/* --------------------
 *
 * Private Routines
 *
 * Delete		   - delete a file from the Lru ring
 * LruDelete	   - remove a file from the Lru ring and close its FD
 * Insert		   - put a file at the front of the Lru ring
 * LruInsert	   - put a file at the front of the Lru ring and open it
 * ReleaseLruFile  - Release an fd by closing the last entry in the Lru ring
 * ReleaseLruFiles - Release fd(s) until we're under the max_safe_fds limit
 * AllocateVfd	   - grab a free (or new) file record (from VfdArray)
 * FreeVfd		   - free a file record
 *
 * The Least Recently Used ring is a doubly linked list that begins and
 * ends on element zero.  Element zero is special -- it doesn't represent
 * a file and its "fd" field always == VFD_CLOSED.	Element zero is just an
 * anchor that shows us the beginning/end of the ring.
 * Only VFD elements that are currently really open (have an FD assigned) are
 * in the Lru ring.  Elements that are "virtually" open can be recognized
 * by having a non-null fileName field.
 *
 * example:
 *
 *	   /--less----\				   /---------\
 *	   v		   \			  v			  \
 *	 #0 --more---> LeastRecentlyUsed --more-\ \
 *	  ^\									| |
 *	   \\less--> MostRecentlyUsedFile	<---/ |
 *		\more---/					 \--less--/
 *
 * --------------------
 */
static void Delete(File file);
static void LruDelete(File file);
static void Insert(File file);
static int LruInsert(File file);
static bool ReleaseLruFile(void);
static File AllocateVfd(void);
static void FreeVfd(File file);

static int FileAccess(File file);
static File OpenTemporaryFileInTablespaceOrDir(Oid tblspcOid, bool rejectError);
static void CleanupTempFiles(bool isProcExit);
static void RemovePgTempFilesInDir(const char* tmpdirname, bool unlinkAll);
static void RemovePgTempRelationFiles(const char* tsdirname);
static void RemovePgTempRelationFilesInDbspace(const char* dbspacedirname);
static bool looks_like_temp_rel_name(const char* name);
static void DataFileIdCloseFile(Vfd* vfdP);
static File PathNameOpenFile_internal(FileName fileName, int fileFlags, int fileMode, bool useFileCache,
                                      const RelFileNodeForkNum& fileNode, File file = FILE_INVALID);
static void ReleaseLruFiles(void);
void AtProcExit_Files(int code, Datum arg);
static uint32 VFDTableHashCode(const void* tagPtr);
static void UnlinkIfExistsFname(const char *fname, bool isdir, int elevel);
static void Walkdir(const char *path,
        void (*action) (const char *fname, bool isdir, int elevel),
        bool process_symlinks,
        int elevel);

void ReportAlarmInsuffDataInstFileDesc()
{
    Alarm alarmItem[1];
    AlarmAdditionalParam tempAdditionalParam;

    // Initialize the alarm item
    AlarmItemInitialize(alarmItem, ALM_AI_InsufficientDataInstFileDesc, ALM_AS_Reported, NULL);
    // fill the alarm message
    WriteAlarmAdditionalInfo(&tempAdditionalParam,
                             g_instance.attr.attr_common.PGXCNodeName,
                             "",
                             "",
                             alarmItem,
                             ALM_AT_Fault,
                             g_instance.attr.attr_common.PGXCNodeName);
    // report the alarm
    AlarmReporter(alarmItem, ALM_AT_Fault, &tempAdditionalParam);
}

void ReportResumeInsuffDataInstFileDesc()
{
    Alarm alarmItem[1];
    AlarmAdditionalParam tempAdditionalParam;

    // Initialize the alarm item
    AlarmItemInitialize(alarmItem, ALM_AI_InsufficientDataInstFileDesc, ALM_AS_Normal, NULL);
    // fill the alarm message
    WriteAlarmAdditionalInfo(
        &tempAdditionalParam, g_instance.attr.attr_common.PGXCNodeName, "", "", alarmItem, ALM_AT_Resume);
    // report the alarm
    AlarmReporter(alarmItem, ALM_AT_Resume, &tempAdditionalParam);
}

/*
 * Estimate space needed for mapping hashtable
 */
Size DataFileIdCacheSize(void)
{
    t_thrd.storage_cxt.max_userdatafiles =
        Max(g_instance.attr.attr_common.max_files_per_process, t_thrd.storage_cxt.max_userdatafiles);

    return hash_estimate_size(t_thrd.storage_cxt.max_userdatafiles, sizeof(DataFileIdCacheEntry));
}

/*
 * DataFileIdCacheCreate
 *
 * Create file id cache spaces.
 */
void InitDataFileIdCache(void)
{
    HASHCTL ctl;

    /* hash accessed by database file id */
    errno_t rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "", "");
    ctl.keysize = sizeof(RelFileNodeForkNum);
    ctl.entrysize = sizeof(DataFileIdCacheEntry);
    ctl.hash = tag_hash;
    ctl.num_partitions = NUM_VFD_PARTITIONS;
    t_thrd.storage_cxt.DataFileIdCache = HeapMemInitHash(
                                             "Shared FileId hash by request", 256, Max(g_instance.attr.attr_common.max_files_per_process, t_thrd.storage_cxt.max_userdatafiles), &ctl,
                                             HASH_ELEM | HASH_FUNCTION | HASH_PARTITION);
    if (!t_thrd.storage_cxt.DataFileIdCache)
        ereport(FATAL, (errmsg("could not initialize shared file id hash table")));
}

RelFileNodeForkNum RelFileNodeForkNumFill(const RelFileNodeBackend& rnode, ForkNumber forknum, BlockNumber segno)
{
    RelFileNodeForkNum node;

    errno_t rc = memset_s(&node, sizeof(RelFileNodeForkNum), 0, sizeof(RelFileNodeForkNum));
    securec_check(rc, "", "");
    node.rnode = rnode;
    node.forknumber = forknum;
    node.segno = segno;
    node.storage = ROW_STORE;

    return node;
}

RelFileNodeForkNum RelFileNodeForkNumFill(RelFileNode* rnode,
    BackendId backend, ForkNumber forknum, BlockNumber segno)
{
    RelFileNodeForkNum filenode;

    errno_t rc = memset_s(&filenode, sizeof(RelFileNodeForkNum), 0, sizeof(RelFileNodeForkNum));
    securec_check(rc, "", "");

    if (rnode != NULL) {
        filenode.rnode.node.relNode = rnode->relNode;
        filenode.rnode.node.spcNode = rnode->spcNode;
        filenode.rnode.node.dbNode = rnode->dbNode;
        filenode.rnode.node.bucketNode = rnode->bucketNode;
        filenode.rnode.node.opt = rnode->opt;
    } else {
        filenode.rnode.node.relNode = InvalidOid;
        filenode.rnode.node.spcNode = InvalidOid;
        filenode.rnode.node.dbNode = InvalidOid;
        filenode.rnode.node.bucketNode = InvalidBktId;
        filenode.rnode.node.opt = 0;
    }

    filenode.rnode.backend = backend;
    filenode.forknumber = forknum;
    filenode.segno = segno;
    filenode.storage = ROW_STORE;

    return filenode;
}


/*
 * pg_fsync --- do fsync with or without writethrough
 */
int pg_fsync(int fd)
{
    if (is_dss_fd(fd)) {
        return 0;
    }
    /* #if is to skip the sync_method test if there's no need for it */
#if defined(HAVE_FSYNC_WRITETHROUGH) && !defined(FSYNC_WRITETHROUGH_IS_FSYNC)
    if (u_sess->attr.attr_storage.sync_method == SYNC_METHOD_FSYNC_WRITETHROUGH)
        return pg_fsync_writethrough(fd);
    else
#endif
    return pg_fsync_no_writethrough(fd);
}

/*
 * pg_fsync_no_writethrough --- same as fsync except does nothing if
 *	enableFsync is off
 */
int pg_fsync_no_writethrough(int fd)
{
    if (u_sess->attr.attr_storage.enableFsync)
        return fsync(fd);
    else
        return 0;
}

/*
 * pg_fsync_writethrough
 */
int pg_fsync_writethrough(int fd)
{
    if (u_sess->attr.attr_storage.enableFsync) {
#ifdef WIN32
        return _commit(fd);
#elif defined(F_FULLFSYNC)
        return (fcntl(fd, F_FULLFSYNC, 0) == -1) ? -1 : 0;
#else
        errno = ENOSYS;
        return -1;
#endif
    } else
        return 0;
}

/*
 * pg_fdatasync --- same as fdatasync except does nothing if enableFsync is off
 *
 * Not all platforms have fdatasync; treat as fsync if not available.
 */
int pg_fdatasync(int fd)
{
    if (u_sess->attr.attr_storage.enableFsync) {
#ifdef HAVE_FDATASYNC
        return fdatasync(fd);
#else
        return fsync(fd);
#endif
    } else
        return 0;
}

/*
 * pg_flush_data --- advise OS that the described dirty data should be flushed
 *
 * offset of 0 with nbytes 0 means that the entire file should be flushed;
 * in this case, this function may have side-effects on the file's
 * seek position!
 */
void pg_flush_data(int fd, off_t offset, off_t nbytes)
{
    if (is_dss_fd(fd)) {
        return;
    }
    /*
     * Right now file flushing is primarily used to avoid making later
     * fsync()/fdatasync() calls have less impact. Thus don't trigger flushes
     * if fsyncs are disabled - that's a decision we might want to make
     * configurable at some point.
     */
    if (!u_sess->attr.attr_storage.enableFsync)
        return;

    /*
     * We compile all alternatives that are supported on the current platform,
     * to find portability problems more easily.
     */
#if defined(HAVE_SYNC_FILE_RANGE)
    {
        int rc;

        /*
         * sync_file_range(SYNC_FILE_RANGE_WRITE), currently linux specific,
         * tells the OS that writeback for the specified blocks should be
         * started, but that we don't want to wait for completion.  Note that
         * this call might block if too much dirty data exists in the range.
         * This is the preferable method on OSs supporting it, as it works
         * reliably when available (contrast to msync()) and doesn't flush out
         * clean data (like FADV_DONTNEED).
         */
        rc = sync_file_range(fd, offset, nbytes, SYNC_FILE_RANGE_WRITE);
        /* don't error out, this is just a performance optimization */
        if (rc != 0) {
            ereport(data_sync_elevel(WARNING), (errcode_for_file_access(), errmsg("could not flush dirty data: %m")));
        }

        return;
    }
#else
#if !defined(WIN32) && defined(MS_ASYNC)
    {
        void* p = NULL;
        static int pagesize = 0;

        /*
         * On several OSs msync(MS_ASYNC) on a mmap'ed file triggers
         * writeback. On linux it only does so if MS_SYNC is specified, but
         * then it does the writeback synchronously. Luckily all common linux
         * systems have sync_file_range().  This is preferable over
         * FADV_DONTNEED because it doesn't flush out clean data.
         *
         * We map the file (mmap()), tell the kernel to sync back the contents
         * (msync()), and then remove the mapping again (munmap()).
         *
         * mmap() needs actual length if we want to map whole file
         */
        if (offset == 0 && nbytes == 0) {
            nbytes = lseek(fd, 0, SEEK_END);
            if (nbytes < 0) {
                ereport(WARNING, (errcode_for_file_access(), errmsg("could not determine dirty data size: %m")));
                return;
            }
        }

        /*
         * Some platforms reject partial-page mmap() attempts.  To deal with
         * that, just truncate the request to a page boundary.  If any extra
         * bytes don't get flushed, well, it's only a hint anyway.
         *
         * fetch pagesize only once
         */
        if (pagesize == 0)
            pagesize = sysconf(_SC_PAGESIZE);

        /* align length to pagesize, dropping any fractional page */
        if (pagesize > 0)
            nbytes = (nbytes / pagesize) * pagesize;

        /* fractional-page request is a no-op */
        if (nbytes <= 0)
            return;

        /*
         * mmap could well fail, particularly on 32-bit platforms where there
         * may simply not be enough address space.  If so, silently fall
         * through to the next implementation.
         */
        if (nbytes <= (off_t)SSIZE_MAX)
            p = mmap(NULL, nbytes, PROT_READ, MAP_SHARED, fd, offset);
        else
            p = MAP_FAILED;

        if (p != MAP_FAILED) {
            int rc;

            rc = msync(p, (size_t)nbytes, MS_ASYNC);
            if (rc != 0) {
                ereport(
                    data_sync_elevel(WARNING), (errcode_for_file_access(), errmsg("could not flush dirty data: %m")));
                /* NB: need to fall through to munmap()! */
            }

            rc = munmap(p, (size_t)nbytes);
            if (rc != 0) {
                /* FATAL error because mapping would remain */
                ereport(FATAL, (errcode_for_file_access(), errmsg("could not munmap() while flushing data: %m")));
            }

            return;
        }
    }
#endif
#if defined(USE_POSIX_FADVISE) && defined(POSIX_FADV_DONTNEED)
    {
        int rc;

        /*
         * Signal the kernel that the passed in range should not be cached
         * anymore. This has the, desired, side effect of writing out dirty
         * data, and the, undesired, side effect of likely discarding useful
         * clean cached blocks.  For the latter reason this is the least
         * preferable method.
         */
        rc = posix_fadvise(fd, offset, nbytes, POSIX_FADV_DONTNEED);
        if (rc != 0) {
            /* don't error out, this is just a performance optimization */
            ereport(WARNING, (errcode_for_file_access(), errmsg("could not flush dirty data: %m")));
        }

        return;
    }
#endif
#endif
}

/*
 * InitFileAccess --- initialize this module during backend startup
 *
 * This is called during either normal or standalone backend start.
 * It is *not* called in the postmaster.
 */
void InitFileAccess(void)
{
    if (EnableLocalSysCache()) {
        if(t_thrd.lsc_cxt.lsc->VfdCache != NULL) {
            return;
        }
        /* initialize cache header entry */
        t_thrd.lsc_cxt.lsc->VfdCache =
            (Vfd*)MemoryContextAlloc(LocalSmgrStorageMemoryCxt(), sizeof(vfd));
        if (t_thrd.lsc_cxt.lsc->VfdCache == NULL)
            ereport(FATAL, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
        errno_t ret = memset_s((char*)&(t_thrd.lsc_cxt.lsc->VfdCache[0]), sizeof(Vfd), 0, sizeof(Vfd));
        securec_check(ret, "\0", "\0");
        t_thrd.lsc_cxt.lsc->VfdCache->fd = VFD_CLOSED;
        t_thrd.lsc_cxt.lsc->SizeVfdCache = 1;
        /* register proc-exit hook to ensure temp files are dropped at exit */

        on_proc_exit(AtProcExit_Files, 0);
    } else {
        Assert(u_sess->storage_cxt.SizeVfdCache == 0); /* call me only once */
        /* initialize cache header entry */
        u_sess->storage_cxt.VfdCache =
            (Vfd*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), sizeof(vfd));
        if (u_sess->storage_cxt.VfdCache == NULL)
            ereport(FATAL, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
        errno_t ret = memset_s((char*)&(u_sess->storage_cxt.VfdCache[0]), sizeof(Vfd), 0, sizeof(Vfd));
        securec_check(ret, "\0", "\0");
        u_sess->storage_cxt.VfdCache->fd = VFD_CLOSED;
        u_sess->storage_cxt.SizeVfdCache = 1;
        /* register proc-exit hook to ensure temp files are dropped at exit */
        if (!IS_THREAD_POOL_SESSION)
            on_proc_exit(AtProcExit_Files, 0);
    }
}

/*
 * count_usable_fds --- count how many FDs the system will let us open,
 *		and estimate how many are already open.
 *
 * We stop counting if usable_fds reaches max_to_probe.  Note: a small
 * value of max_to_probe might result in an underestimate of already_open;
 * we must fill in any "gaps" in the set of used FDs before the calculation
 * of already_open will give the right answer.	In practice, max_to_probe
 * of a couple of dozen should be enough to ensure good results.
 *
 * We assume stdin (FD 0) is available for dup'ing
 */
static void count_usable_fds(int max_to_probe, int* usable_fds, int* already_open)
{
    int* fd = NULL;
    int size;
    int used = 0;
    int highestfd = 0;
    int j;

#ifdef HAVE_GETRLIMIT
    struct rlimit rlim;
    int getrlimit_status;
#endif

    size = 1024;
    fd = (int*)palloc(size * sizeof(int));

#ifdef HAVE_GETRLIMIT
#ifdef RLIMIT_NOFILE /* most platforms use RLIMIT_NOFILE */
    getrlimit_status = getrlimit(RLIMIT_NOFILE, &rlim);
#else  /* but BSD doesn't ... */
    getrlimit_status = getrlimit(RLIMIT_OFILE, &rlim);
#endif /* RLIMIT_NOFILE */
    if (getrlimit_status != 0)
        ereport(WARNING, (errmsg("getrlimit failed: %m")));
#endif /* HAVE_GETRLIMIT */

    /* dup until failure or probe limit reached */
    for (;;) {
        int thisfd = -1;

#ifdef HAVE_GETRLIMIT

        /*
         * don't go beyond RLIMIT_NOFILE; causes irritating kernel logs on
         * some platforms
         */
        if (getrlimit_status == 0 && (unsigned int)(highestfd) >= rlim.rlim_cur - 1)
            break;
#endif

        thisfd = dup(0);
        if (thisfd < 0) {
            /* Expect EMFILE or ENFILE, else it's fishy */
            if (errno != EMFILE && errno != ENFILE) {
                ereport(WARNING, (errmsg("dup(0) failed after %d successes: %s", used, TRANSLATE_ERRNO)));
            }
            break;
        }

        if (used >= size) {
            size *= 2;
            fd = (int*)repalloc(fd, size * sizeof(int));
        }
        fd[used++] = thisfd;

        if (highestfd < thisfd) {
            highestfd = thisfd;
        }

        if (used >= max_to_probe) {
            break;
        }
    }

    /* release the files we opened */
    for (j = 0; j < used; j++) {
        (void)close(fd[j]);
    }

    pfree(fd);

    /*
     * Return results.	usable_fds is just the number of successful dups. We
     * assume that the system limit is highestfd+1 (remember 0 is a legal FD
     * number) and so already_open is highestfd+1 - usable_fds.
     */
    *usable_fds = used;
    *already_open = highestfd + 1 - used;
}

/*
 * set_max_safe_fds
 *		Determine number of filedescriptors that fd.c is allowed to use
 */
void set_max_safe_fds(void)
{
    int usable_fds;
    int already_open;

    /* ----------
     * We want to set max_safe_fds to MIN(usable_fds, max_files_per_process - already_open)
     * less the slop factor for files that are opened without consulting
     * fd.c.  This ensures that we won't exceed either max_files_per_process
     * or the experimentally-determined EMFILE limit.
     * ----------
     */
    count_usable_fds(g_instance.attr.attr_common.max_files_per_process, &usable_fds, &already_open);

    t_thrd.storage_cxt.max_safe_fds = Min(usable_fds, g_instance.attr.attr_common.max_files_per_process - already_open);

    /*
     * Take off the FDs reserved for system() etc.
     */
    t_thrd.storage_cxt.max_safe_fds -= NUM_RESERVED_FDS;

    /*
     * Make sure we still have enough to get by.
     */
    if (t_thrd.storage_cxt.max_safe_fds < FD_MINFREE) {
        ReportAlarmInsuffDataInstFileDesc();

        ereport(FATAL,
                (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                 errmsg("insufficient file descriptors available to start server process"),
                 errdetail("System allows %d, we need at least %d.",
                           t_thrd.storage_cxt.max_safe_fds + NUM_RESERVED_FDS,
                           FD_MINFREE + NUM_RESERVED_FDS)));
    }
    ReportResumeInsuffDataInstFileDesc();
    ereport(LOG,
            (errmsg("max_safe_fds = %d, usable_fds = %d, already_open = %d",
                    t_thrd.storage_cxt.max_safe_fds,
                    usable_fds,
                    already_open)));

    /* get /proc/pid/fd  directory file handle */
#ifdef USE_ASSERT_CHECKING
    if (!IsInitdb) {
        int rc = snprintf_s(g_gauss_pid_dir_path, sizeof(g_gauss_pid_dir_path), MAXPGPATH, "/proc/%d/fd", getpid());
        securec_check_ss(rc, "", "");
        g_gauss_pid_dir_path[MAXPGPATH] = '\0';

        g_gauss_pid_dir = AllocateDir(g_gauss_pid_dir_path);
        if (g_gauss_pid_dir == NULL) {
            ereport(ERROR,
                    (errcode_for_file_access(), errmsg("unable to open process pid directory \"%s\"", g_gauss_pid_dir_path)));
        }
    }
#endif
}

void CloseGaussPidDir(void)
{
#ifdef USE_ASSERT_CHECKING
    if (g_gauss_pid_dir != NULL) {
        (void)FreeDir(g_gauss_pid_dir);
    }
#endif
}

#ifdef USE_ASSERT_CHECKING
static void DumpOpenFiles(DIR* dir)
{
    struct dirent* dent = NULL;
    char tmppath[MAXPGPATH + 1] = {0};
    char linkpath[MAXPGPATH + 1] = {0};

    while ((dent = ReadDir(dir, g_gauss_pid_dir_path)) != NULL) {
        /* symbolic link */
        if (dent->d_type != DT_LNK) {
            continue;
        }

        int rc = snprintf_s(tmppath, sizeof(tmppath), MAXPGPATH, "%s/%s", g_gauss_pid_dir_path, dent->d_name);
        securec_check_ss(rc, "", "");
        tmppath[MAXPGPATH] = '\0';

        /* get file status,  must be symbolic link */
        struct stat st;
        if (lstat(tmppath, &st) < 0 || !S_ISLNK(st.st_mode)) {
            continue;
        }

        int rllen = readlink(tmppath, linkpath, sizeof(linkpath));
        if (rllen < 0) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not read symbolic link \"%s\": %m", tmppath)));
        }
        if (rllen >= (int)sizeof(linkpath)) {
            ereport(ERROR,
                    (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
                     errmsg("symbolic link \"%s\" target is too long", tmppath)));
        }
        linkpath[rllen] = '\0';

        ereport(LOG, (errmsg("[FD] \"%s\" ", linkpath)));
    }

    return;
}
#endif

/*
 * BasicOpenFile --- same as open(2) except can free other FDs if needed
 *
 * This is exported for use by places that really want a plain kernel FD,
 * but need to be proof against running out of FDs.  Once an FD has been
 * successfully returned, it is the caller's responsibility to ensure that
 * it will not be leaked on ereport()!	Most users should *not* call this
 * routine directly, but instead use the VFD abstraction level, which
 * provides protection against descriptor leaks as well as management of
 * files that need to be open for more than a short period of time.
 *
 * Ideally this should be the *only* direct call of open() in the backend.
 * In practice, the postmaster calls open() directly, and there are some
 * direct open() calls done early in backend startup.  Those are OK since
 * this module wouldn't have any open files to close at that point anyway.
 */
int BasicOpenFile(FileName fileName, int fileFlags, int fileMode)
{
    int fd = -1;

tryAgain:
    fd = open(fileName, fileFlags, fileMode);

    if (fd >= 0) {
        return fd; /* success! */
    }

    if (errno == EMFILE || errno == ENFILE) {
        int save_errno = errno;

#ifdef USE_ASSERT_CHECKING
        if (save_errno == ENFILE) {
            DumpOpenFiles(g_gauss_pid_dir);
            pg_usleep(10000000L);
            ereport(PANIC,
                    (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("out of file descriptors: %m; release and retry")));
        } else {
            ereport(LOG,
                    (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("out of file descriptors: %m; release and retry")));
        }
#else
        ereport(
            LOG, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("out of file descriptors: %m; release and retry")));
#endif

        errno = 0;
        if (ReleaseLruFile()) {
            goto tryAgain;
        }
        errno = save_errno;
    }

    return -1; /* failure */
}


/* 
* When SS_DORADO_CLUSTER enabled, current xlog dictionary may be not the correct dictionary,
* because all xlog dictionaries are in the same LUN, we need loop over other dictionaries.
*/
int SSErgodicOpenXlogFile(XLogSegNo segno, int fileFlags, int fileMode)
{
    char xlog_file_name[MAXPGPATH];
    char xlog_file_full_path[MAXPGPATH];
    char *dssdir = g_instance.attr.attr_storage.dss_attr.ss_dss_vg_name;
    DIR* dir;
    int fd;
    struct dirent *entry;
    errno_t errorno = EOK;

    errorno = snprintf_s(xlog_file_name, MAXPGPATH, MAXPGPATH - 1, "%08X%08X%08X", t_thrd.xlog_cxt.ThisTimeLineID,
                         (uint32)((segno) / XLogSegmentsPerXLogId), (uint32)((segno) % XLogSegmentsPerXLogId));
    securec_check_ss(errorno, "", "");

    dir = opendir(dssdir);
    if (dir == NULL) {
        ereport(PANIC, (errcode_for_file_access(), errmsg("Error opening dssdir %s", dssdir)));                                                  
    }

    while ((entry = readdir(dir)) != NULL) {
        if (strncmp(entry->d_name, "pg_xlog", strlen("pg_xlog")) == 0) {
            errorno = snprintf_s(xlog_file_full_path, MAXPGPATH, MAXPGPATH - 1, "%s/%s/%s", dssdir, entry->d_name, xlog_file_name);
            securec_check_ss(errorno, "", "");

            fd = BasicOpenFile(xlog_file_full_path, fileFlags, fileMode);
            if (fd >= 0) {
                return fd;
            }
        }
    }

    if (fd < 0) {
        ereport(PANIC, (errcode_for_file_access(), errmsg("could not open xlog file \"%s\" (log segment %s): %m", xlog_file_name,
                                                          XLogFileNameP(t_thrd.xlog_cxt.ThisTimeLineID, segno))));
    }

    return fd;
}

#if defined(FDDEBUG)

static void _dump_lru(void)
{
    vfd *vfdcache = GetVfdCache();
    int mru = vfdcache[0].lruLessRecently;
    Vfd* vfdP = &vfdcache[mru];

    char buf[2048];
    errno_t rc = EOK;

    rc = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1, "LRU: MOST %d ", mru);
    securec_check_ss(rc, "", "");

    while (mru != 0) {
        mru = vfdP->lruLessRecently;
        vfdP = &vfdcache[mru];
        rc = snprintf_s(buf + strlen(buf), sizeof(buf) - strlen(buf), sizeof(buf) - strlen(buf) - 1, "%d ", mru);
        securec_check_ss(rc, "", "");
    }
    rc = snprintf_s(buf + strlen(buf), sizeof(buf) - strlen(buf), sizeof(buf) - strlen(buf) - 1, "LEAST");
    securec_check_ss(rc, "", "");
    ereport(LOG, (errmsg("%s", buf)));
}
#endif /* FDDEBUG */

static void Delete(File file)
{
    Vfd* vfdP = NULL;

    Assert(file != 0);
    vfd *vfdcache = GetVfdCache();

    DO_DB(ereport(LOG, (errmsg("Delete %d (%s)", file, vfdcache[file].fileName))));
    DO_DB(_dump_lru());

    vfdP = &vfdcache[file];

    vfdcache[vfdP->lruLessRecently].lruMoreRecently = vfdP->lruMoreRecently;
    vfdcache[vfdP->lruMoreRecently].lruLessRecently = vfdP->lruLessRecently;

    DO_DB(_dump_lru());
}

static void LruDelete(File file)
{
    Vfd* vfdP = NULL;

    Assert(file != 0);
    vfd *vfdcache = GetVfdCache();

    DO_DB(ereport(LOG, (errmsg("LruDelete %d (%s)", file, vfdcache[file].fileName))));

    vfdP = &vfdcache[file];

    /* delete the vfd record from the LRU ring */
    Delete(file);

    /* close the file */
    DataFileIdCloseFile(vfdP);

    AddVfdNfile(-1);
    vfdP->fd = VFD_CLOSED;
}

static void Insert(File file)
{
    Vfd* vfdP = NULL;

    Assert(file != 0);
    vfd *vfdcache = GetVfdCache();

    DO_DB(ereport(LOG, (errmsg("Insert %d (%s)", file, vfdcache[file].fileName))));
    DO_DB(_dump_lru());

    vfdP = &vfdcache[file];

    vfdP->lruMoreRecently = 0;
    vfdP->lruLessRecently = vfdcache[0].lruLessRecently;
    vfdcache[0].lruLessRecently = file;
    vfdcache[vfdP->lruLessRecently].lruMoreRecently = file;

    DO_DB(_dump_lru());
}

/* returns 0 on success, -1 on re-open failure (with errno set) */
static int LruInsert(File file)
{
    Vfd* vfdP = NULL;
    File rfile;

    Assert(file != 0);
    vfd *vfdcache = GetVfdCache();

    DO_DB(ereport(LOG, (errmsg("LruInsert %d (%s)", file, vfdcache[file].fileName))));

    vfdP = &vfdcache[file];

    if (FileIsNotOpen(file)) {
        rfile = DataFileIdOpenFile(vfdP->fileName, vfdP->fileNode, vfdP->fileFlags, vfdP->fileMode, file);
        if (rfile > 0) {
            Assert(rfile == file);
            return 0;
        }

        return -1;
    }

    /*
     * put it at the head of the Lru ring
     */
    Insert(file);

    return 0;
}

/*
 * Release one kernel FD by closing the least-recently-used VFD.
 */
static bool ReleaseLruFile(void)
{
    DO_DB(ereport(LOG, (errmsg("ReleaseLruFile. Opened %d", GetVfdNfile()))));

    if (GetVfdNfile() > 0) {
        /*
         * There are opened files and so there should be at least one used vfd
         * in the ring.
         */
        vfd *vfdcache = GetVfdCache();
        Assert(vfdcache[0].lruMoreRecently != 0);
        LruDelete(vfdcache[0].lruMoreRecently);
        return true; /* freed a file */
    }
    return false; /* no files available to free */
}

/*
 * Release kernel FDs as needed to get under the max_safe_fds limit.
 * After calling this, it's OK to try to open another file.
 */
static void ReleaseLruFiles(void)
{
    while (GetVfdNfile() + u_sess->storage_cxt.numAllocatedDescs >= t_thrd.storage_cxt.max_safe_fds) {
        if (!ReleaseLruFile())
            break;
    }
}

/* Be careful not to clobber VfdCache ptr if realloc fails. */
static void ReallocVfdCache(Size newCacheSize)
{
    Vfd* newVfdCache = NULL;

#ifdef ENABLE_MULTIPLE_NODES
    bool process_recursive = IsThreadProcessStreamRecursive();
    if (unlikely(process_recursive)) {
        /*
         * If current thread is under streaming recursive processing, we may have
         * the vfdcache pointer passed to another stream thread(Producer THR in
         * recursive), where it reads data from tuplestore, in this case we need
         * exclude the repalloc() of vfdcache to void trash-pointer access.
         */
        pthread_mutex_t* recursive_mutex = u_sess->stream_cxt.global_obj->GetRecursiveMutex();
        AutoMutexLock recursive_mutext_lock(recursive_mutex);
        recursive_mutext_lock.lock();
        {
            newVfdCache = (Vfd*)repalloc(GetVfdCache(), sizeof(Vfd) * newCacheSize);
            if (newVfdCache == NULL) {
                recursive_mutext_lock.unLock();
                ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
            }
            SetVfdCache(newVfdCache);
        }
        recursive_mutext_lock.unLock();
        return;
    }
#endif

    newVfdCache = (Vfd*)repalloc(GetVfdCache(), sizeof(Vfd) * newCacheSize);
    if (newVfdCache == NULL) {
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
    }
    SetVfdCache(newVfdCache);
}

static File AllocateVfd(void)
{
    Index i;
    File file;

    DO_DB(ereport(LOG, (errmsg("AllocateVfd. Size %lu", (unsigned long)GetSizeVfdCache()))));

    Assert(GetSizeVfdCache() > 0); /* InitFileAccess not called? */

    vfd *vfdcache = GetVfdCache();
    if (vfdcache[0].nextFree == 0) {
        Size SizeVfdCache = GetSizeVfdCache();
        /*
         * The free list is empty so it is time to increase the size of the
         * array.  We choose to double it each time this happens. However,
         * there's not much point in starting *real* small.
         */
        Size newCacheSize = SizeVfdCache * 2;
        Size maxCacheSize = MaxAllocSize / sizeof(Vfd);

        if (newCacheSize < 32) {
            newCacheSize = 32;
        }

        if (newCacheSize >= maxCacheSize) {
            /* sizeof(Vfd) = 96, if cache great than max cache size, change extend strategy to fix a number size 96M */
            uint32 extendCacheCount = 1024 * 1024;
            newCacheSize = SizeVfdCache + extendCacheCount;
        }

        ReallocVfdCache(newCacheSize);
        vfdcache = GetVfdCache();

        /* Initialize the new entries and link them into the free list. */
        for (i = (Index)SizeVfdCache; i < newCacheSize; i++) {
            errno_t ret = memset_s((char*)&(vfdcache[i]), sizeof(Vfd), 0, sizeof(Vfd));
            securec_check(ret, "\0", "\0");
            vfdcache[i].nextFree = (File)(i + 1);
            vfdcache[i].fd = VFD_CLOSED;
        }
        vfdcache[newCacheSize - 1].nextFree = 0;
        vfdcache[0].nextFree = (File)SizeVfdCache;
        SetSizeVfdCache(newCacheSize); /* Record the new size */
    }
    file = vfdcache[0].nextFree;
    vfdcache[0].nextFree = vfdcache[file].nextFree;

    return file;
}

static void FreeVfd(File file)
{
    vfd *vfdcache = GetVfdCache();
    Vfd* vfdP = &vfdcache[file];

    DO_DB(ereport(LOG, (errmsg("FreeVfd: %d (%s)", file, vfdP->fileName ? vfdP->fileName : ""))));

    /*
     * If the vfd has been freed, recieve a signal and FATAL, thread_exit callback
     * will DestroyAllVfds() and free the vfd again, cause heap-use-after-free.
     * So hold interrupts here.
     */
    HOLD_INTERRUPTS();

    if (vfdP->fileName != NULL) {
        pfree(vfdP->fileName);
        vfdP->fileName = NULL;
    }
    vfdP->fdstate = 0x0;

    vfdP->nextFree = vfdcache[0].nextFree;
    vfdcache[0].nextFree = file;
    RESUME_INTERRUPTS();
}

/* returns 0 on success, -1 on re-open failure (with errno set) */
static int FileAccess(File file)
{
    int returnValue;
    vfd *vfdcache = GetVfdCache();
    DO_DB(ereport(LOG, (errmsg("FileAccess %d (%s)", file, vfdcache[file].fileName))));

    /*
     * Is the file open?  If not, open it and put it at the head of the LRU
     * ring (possibly closing the least recently used file to get an FD).
     */
    if (FileIsNotOpen(file)) {
        returnValue = LruInsert(file);
        if (returnValue != 0) {
            return returnValue;
        }
    } else if (vfdcache[0].lruLessRecently != file) {
        /*
         * We now know that the file is open and that it is not the last one
         * accessed, so we need to move it to the head of the Lru ring.
         */
        Delete(file);
        Insert(file);
    }

    return 0;
}

/*
 * Called whenever a temporary file is deleted to report its size.
 */
static void
ReportTemporaryFileUsage(const char *path, off_t size)
{
    pgstat_report_tempfile(size);

    if (u_sess->attr.attr_common.log_temp_files >= 0) {
        if ((size / 1024) >= u_sess->attr.attr_common.log_temp_files)
            ereport(LOG,
                    (errmsg("temporary file: path \"%s\", size %lu",
                            path, (unsigned long) size)));
    }
}

/*
 * Called to register a temporary file for automatic close.
 * ResourceOwnerEnlargeFiles(CurrentResourceOwner) must have been called
 * before the file was opened.
 */
static void
RegisterTemporaryFile(File file)
{
    vfd *vfdcache = GetVfdCache();
    ResourceOwnerRememberFile(t_thrd.utils_cxt.CurrentResourceOwner, file);
    vfdcache[file].resowner = t_thrd.utils_cxt.CurrentResourceOwner;

    /* Backup mechanism for closing at end of xact. */
    vfdcache[file].fdstate |= FD_CLOSE_AT_EOXACT;
    u_sess->storage_cxt.have_xact_temporary_files = true;
}

#ifdef NOT_USED
/* Called when we get a shared invalidation message on some relation. */
void FileInvalidate(File file)
{
    Assert(FileIsValid(file));
    if (!FileIsNotOpen(file)) {
        LruDelete(file);
    }
}
#endif

// Return file descriptor of the specified fileid
// Note: this is a quite freqently called operation,
//    we search the shared DataFileCache and get it.
// File:
// Open file with a new vfd when the default value *FILE_INVALID* passed,
// or reopen the file on vfdCache when the 'file' is specified.
File DataFileIdOpenFile(FileName fileName, const RelFileNodeForkNum& fileNode, int fileFlags, int fileMode, File file)
{
    /*
     * Do not use thread-share fd cache when:
     * 1. datafile in temp namespace;
     * 2. datafile that backend-private;
     */
    if (isTempNamespace(fileNode.rnode.node.spcNode) || (InvalidBackendId != fileNode.rnode.backend)) {
        return PathNameOpenFile(fileName, fileFlags, fileMode, file);
    } else {
        return PathNameOpenFile_internal(fileName, fileFlags, fileMode, true, fileNode, file);
    }
}

/* if failed to close temp file during aborting transaction, use WARNING log level;
 * otherwise, use ERROR log level.
 */
#define LogLevelOfCloseFileFailed(_vfdP_)                                                                     \
    ((t_thrd.xact_cxt.bInAbortTransaction && (((_vfdP_)->fdstate & FD_TEMP_FILE_LIMIT) != 0)) \
    ? WARNING                                                                                         \
    : data_sync_elevel(ERROR))

/* Return 0 if no error, else errno */
static void DataFileIdCloseFile(Vfd* vfdP)
{
    DataFileIdCacheEntry* entry = NULL;

    /*
     * Do not use thread-share fd cache when:
     *  1. vfd is not in fd cache;
     */
    if (!vfdP->infdCache) {
        if (close(vfdP->fd) < 0) {
            ereport(LogLevelOfCloseFileFailed(vfdP),
                    (errcode_for_file_access(),
                     errmsg("[Local] File(%s) fd(%d) have been closed, %s", vfdP->fileName, vfdP->fd, TRANSLATE_ERRNO)));
        }
        return;
    }

    // lock to prevent conflicts with other threads
    // do not use LWLock, because when thread exit t_thrd.proc = NULL
    uint32 newHash = VFDTableHashCode((void *)&vfdP->fileNode);
    AutoMutexLock vfdLock(VFDMappingPartitionLock(newHash));
    vfdLock.lock();

    // find the opened file
    entry =
        (DataFileIdCacheEntry*)hash_search_with_hash_value(t_thrd.storage_cxt.DataFileIdCache,
                                                           (void*)&vfdP->fileNode, newHash, HASH_FIND, NULL);
    if (entry == NULL) {
        vfdLock.unLock();
        ereport(PANIC, (errmsg("file cache corrupted, file %s not opened with handle: %d", vfdP->fileName, vfdP->fd)));
    }

    Assert(entry->fd >= 0 && (entry->fd == vfdP->fd || (entry->repaired_fd >= 0 && entry->repaired_fd == vfdP->fd)));

    // decrease reference count
    entry->refcount--;
    vfdP->infdCache = false;

    // need to close and remove from cache
    if (entry->refcount == 0) {
        int fd = entry->fd;
        int repaired_fd = entry->repaired_fd;
        entry = (DataFileIdCacheEntry*)hash_search_with_hash_value(
                    t_thrd.storage_cxt.DataFileIdCache, (void*)&vfdP->fileNode, newHash, HASH_REMOVE, NULL);
        Assert(entry);
        vfdLock.unLock();

        if (close(fd) < 0) {
            ereport(LogLevelOfCloseFileFailed(vfdP),
                    (errcode_for_file_access(),
                     errmsg("[Global] File(%s) fd(%d) have been closed, %s", vfdP->fileName, fd, TRANSLATE_ERRNO)));
        }
        if (repaired_fd >= 0 && close(repaired_fd) < 0) {
            ereport(LogLevelOfCloseFileFailed(vfdP),
                    (errcode_for_file_access(),
                     errmsg("[Global] File(%s) reapired_fd(%d) have been closed, %s", vfdP->fileName, repaired_fd, TRANSLATE_ERRNO)));
        }
        return;
    }

    vfdLock.unLock();
}

/*
 * open a file in an arbitrary directory
 *
 * NB: if the passed pathname is relative (which it usually is),
 * it will be interpreted relative to the process' working directory
 * (which should always be $PGDATA when this code is running).
 */
File PathNameOpenFile(FileName fileName, int fileFlags, int fileMode, File file)
{
    return PathNameOpenFile_internal(fileName, fileFlags, fileMode, false, g_invalid_file_node, file);
}

// PathNameOpenFile_internal
// if use fd cache, find DataFileIdCache first, this can decrease
// the number of open files in the whole process, when found in the
// cache, refcount++.
// NB: must pass in valid `fileNode' when use fd cache
// the default value of arg 'file' is FILE_INVALID, it will allocate and return
// a new VFD file in vfdCache by default.
static File PathNameOpenFile_internal(
    FileName fileName, int fileFlags, int fileMode, bool useFileCache, const RelFileNodeForkNum& fileNode, File file)
{
    char* fnamecopy = NULL;
    Vfd* vfdP = NULL;
    bool newVfd = false;
    DataFileIdCacheEntry* entry = NULL;

    Assert(file != 0);
    DO_DB(ereport(LOG, (errmsg("PathNameOpenFile: %s %x %o", fileName, fileFlags, fileMode))));

    // Allocate a new VFD file if FILE_INVALID passed, or reopen it on the 'file'
    if (file == FILE_INVALID) {
        fnamecopy = MemoryContextStrdup(LocalSmgrStorageMemoryCxt(), fileName);
        if (fnamecopy == NULL)
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));

        file = AllocateVfd();
        newVfd = true;
    }

    vfdP = &GetVfdCache()[file];

    /* Close excess kernel FDs. */
    ReleaseLruFiles();

    uint32 newHash = 0;
    // Search in fd cache to avoid consuming file handles.
    if (useFileCache) {
        // lock to prevent conflicts with other threads
        newHash = VFDTableHashCode((void*)&fileNode);
        AutoMutexLock vfdLock(VFDMappingPartitionLock(newHash));
        vfdLock.lock();

        // find the opened file
        entry =
            (DataFileIdCacheEntry*)hash_search_with_hash_value(t_thrd.storage_cxt.DataFileIdCache,
                                                               (void*)&fileNode, newHash, HASH_FIND, NULL);
        if (entry != NULL) {
            Assert(entry->fd >= 0);
            entry->refcount++;
        }
        vfdLock.unLock();
    }

    // found in file id cache
    if (entry != NULL) {
        vfdP->fd = entry->fd;
    } else {
        Assert(FileIsNotOpen(file));
        vfdP->fd = BasicOpenFile(fileName, fileFlags, fileMode);
    }

    if (vfdP->fd < 0) {
        if (newVfd) {
            FreeVfd(file);
            pfree(fnamecopy);
        }
        return -1;
    }
    AddVfdNfile(1);
    DO_DB(ereport(LOG, (errmsg("PathNameOpenFile: success %d", vfdP->fd))));

    Insert(file);

    if (newVfd) {
        pfree_ext(vfdP->fileName);
        vfdP->fileName = fnamecopy;
        // Saved flags are adjusted to be OK for re-opening file
        vfdP->fileFlags = fileFlags & ~(O_CREAT | O_TRUNC | O_EXCL);
        vfdP->fileMode = fileMode;
        vfdP->seekPos = 0;
        vfdP->fileSize = 0;
        vfdP->fdstate = 0x0;
        vfdP->resowner = NULL;
        vfdP->fileNode = fileNode;
    }

    // Not found in fd cache, then enter it into cache
    if (useFileCache && (entry == NULL)) {
        bool found = false;
        START_CRIT_SECTION();
        AutoMutexLock vfdLock(VFDMappingPartitionLock(newHash));
        vfdLock.lock();

        // find place to enter
        entry = (DataFileIdCacheEntry*)hash_search_with_hash_value(
                    t_thrd.storage_cxt.DataFileIdCache, (void*)&fileNode, newHash, HASH_ENTER, &found);
        if (found) {
            // already opened, close my open
            Assert(entry->fd >= 0);
            entry->refcount++;
            vfdLock.unLock();
            (void)close(vfdP->fd);
            vfdP->fd = entry->fd;
        } else {
            Assert(vfdP->fd >= 0);
            entry->fd = vfdP->fd;
            entry->repaired_fd = -1;
            entry->refcount = 1;
            vfdLock.unLock();
        }
        END_CRIT_SECTION();
    }

    vfdP->infdCache = useFileCache;

    return file;
}

/*
 * Create directory 'directory'.  If necessary, create 'basedir', which must
 * be the directory above it.  This is designed for creating the top-level
 * temporary directory on demand before creating a directory underneath it.
 * Do nothing if the directory already exists.
 *
 * Directories created within the top-level temporary directory should begin
 * with PG_TEMP_FILE_PREFIX, so that they can be identified as temporary and
 * deleted at startup by RemovePgTempFiles().  Further subdirectories below
 * that do not need any particular prefix.
*/
void PathNameCreateTemporaryDir(const char *basedir, const char *directory)
{
    if (mkdir(directory, S_IRWXU) < 0) {
        if (errno == EEXIST)
            return;

        /*
         * Failed.  Try to create basedir first in case it's missing. Tolerate
         * EEXIST to close a race against another process following the same
         * algorithm.
         */
        if (mkdir(basedir, S_IRWXU) < 0 && errno != EEXIST)
            ereport(ERROR,
                (errcode_for_file_access(), errmsg("cannot create temporary directory \"%s\": %m", basedir)));

        /* Try again. */
        if (mkdir(directory, S_IRWXU) < 0 && errno != EEXIST)
            ereport(ERROR,
                (errcode_for_file_access(), errmsg("cannot create temporary subdirectory \"%s\": %m", directory)));
    }
}

/*
 * Delete a directory and everything in it, if it exists.
 */
void
PathNameDeleteTemporaryDir(const char *dirname)
{
    struct stat statbuf;

    /* Silently ignore missing directory. */
    if (stat(dirname, &statbuf) != 0 && FILE_POSSIBLY_DELETED(errno))
        return;

    /*
     * Currently, walkdir doesn't offer a way for our passed in function to
     * maintain state.  Perhaps it should, so that we could tell the caller
     * whether this operation succeeded or failed.  Since this operation is
     * used in a cleanup path, we wouldn't actually behave differently: we'll
     * just log failures.
     */
    Walkdir(dirname, UnlinkIfExistsFname, false, LOG);
}

/*
 * Open a temporary file that will disappear when we close it.
 *
 * This routine takes care of generating an appropriate tempfile name.
 * There's no need to pass in fileFlags or fileMode either, since only
 * one setting makes any sense for a temp file.
 *
 * Unless interXact is true, the file is remembered by CurrentResourceOwner
 * to ensure it's closed and deleted when it's no longer needed, typically at
 * the end-of-transaction. In most cases, you don't want temporary files to
 * outlive the transaction that created them, so this should be false -- but
 * if you need "somewhat" temporary storage, this might be useful. In either
 * case, the file is removed when the File is explicitly closed.
 */
File OpenTemporaryFile(bool interXact)
{
    File file = 0;

    /*
     * Make sure the current resource owner has space for this File before we
     * open it, if we'll be registering it below.
     */
    if (!interXact)
        ResourceOwnerEnlargeFiles(t_thrd.utils_cxt.CurrentResourceOwner);
    
    if (ENABLE_DSS) {
        file = OpenTemporaryFileInTablespaceOrDir(InvalidOid, true);
    } else {
        /*
        * If some temp tablespace(s) have been given to us, try to use the next
        * one.  If a given tablespace can't be found, we silently fall back to
        * the database's default tablespace.
        *
        * BUT: if the temp file is slated to outlive the current transaction,
        * force it into the database's default tablespace, so that it will not
        * pose a threat to possible tablespace drop attempts.
        */
        if (u_sess->storage_cxt.numTempTableSpaces > 0 && !interXact) {
            Oid tblspcOid = GetNextTempTableSpace();
            if (OidIsValid(tblspcOid))
                file = OpenTemporaryFileInTablespaceOrDir(tblspcOid, false);
        }

        /*
        * If not, or if tablespace is bad, create in database's default
        * tablespace.	u_sess->proc_cxt.MyDatabaseTableSpace should normally be set before we get
        * here, but just in case it isn't, fall back to pg_default tablespace.
        */
        if (file <= 0)
            file = OpenTemporaryFileInTablespaceOrDir(
                    u_sess->proc_cxt.MyDatabaseTableSpace ? u_sess->proc_cxt.MyDatabaseTableSpace :
                    DEFAULTTABLESPACE_OID, true);
    }

    vfd *vfdcache = GetVfdCache();
    /* Mark it for deletion at close and temporary file size limit */
    vfdcache[file].fdstate |= FD_DELETE_AT_CLOSE | FD_TEMP_FILE_LIMIT;

    /* Register it with the current resource owner */
    if (!interXact) {
        RegisterTemporaryFile(file);
    }

    return file;
}

/*
 * Return the path of the temp directory in a given tablespace.
 */
void TempTablespacePath(char *path, Oid tablespace)
{
    errno_t err_rc;
    /*
     * Identify the tempfile directory for this tablespace.
     *
     * If someone tries to specify pg_global, use pg_default instead.
     */
    if (tablespace == DEFAULTTABLESPACE_OID || tablespace == GLOBALTABLESPACE_OID) {
        err_rc = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%s", DEFTBSDIR, PG_TEMP_FILES_DIR);
    } else if (ENABLE_DSS && tablespace == InvalidOid) {
        err_rc = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s", SS_PG_TEMP_FILES_DIR);
    } else {
        /* All other tablespaces are accessed via symlinks */
        if (ENABLE_DSS) {
            err_rc = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%u/%s/%s", TBLSPCDIR, tablespace,
                TABLESPACE_VERSION_DIRECTORY, PG_TEMP_FILES_DIR);
        } else {
            err_rc = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%u/%s_%s/%s", TBLSPCDIR, tablespace,
                TABLESPACE_VERSION_DIRECTORY, g_instance.attr.attr_common.PGXCNodeName, PG_TEMP_FILES_DIR);
        }
    }
    securec_check_ss(err_rc, "", "");
}

/*
 * @Description: open given local cache file and return its vfd to use.
 * @IN pathname: local cache file name
 * @IN unlink_owner: owner has responsibility to unlink this file,
 *                   and the others only write or read this file.
 * @Return: vfd about this cache file.
 * @See also:
 */
File OpenCacheFile(const char* pathname, bool unlink_owner)
{
    File file = 0;
    char tmppath[MAXPGPATH];

    /* place all cache files under pg_errorinfo directory */
    int rc = snprintf_s(tmppath, sizeof(tmppath), sizeof(tmppath) - 1, "pg_errorinfo/%s", pathname);
    securec_check_ss(rc, "", "");
    tmppath[MAXPGPATH - 1] = '\0';

    /*
     * keep O_TRUNC flag alive to avoid some problems.
     * every time writer will overwrite data from position 0 of this file, and
     * reader will read data also from position 0 until EOF is hit. if the second
     * writed data' length is smaller than the first, extra dirty data may be read
     * and some problems happen.
     */
    file = PathNameOpenFile(tmppath, O_RDWR | O_CREAT | O_TRUNC | PG_BINARY, 0600);
    if (file <= 0) {
        ereport(
            ERROR, (errcode_for_file_access(), errmsg("could not create temporary cache file \"%s\": %m", tmppath)));
    }

    vfd *vfdcache = GetVfdCache();
    /* mark the special flag for cache file */
    vfdcache[file].fdstate |= FD_ERRTBL_LOG;
    if (unlink_owner) {
        vfdcache[file].fdstate |= FD_ERRTBL_LOG_OWNER;
    }

    /* ensure cleanup happens at eoxact */
    u_sess->storage_cxt.have_xact_temporary_files = true;

    return file;
}

/*
 * @Description: unlink given local cache file. if failed,
 *               output a warning log messages.
 * @IN pathname: local cache file name
 * @See also:
 */
void UnlinkCacheFile(const char* pathname)
{
    char tmppath[MAXPGPATH];

    /* place all cache files under pg_errorinfo directory */
    int rc = snprintf_s(tmppath, MAXPGPATH, MAXPGPATH - 1, "pg_errorinfo/%s", pathname);
    securec_check_ss(rc, "", "");
    tmppath[MAXPGPATH - 1] = '\0';
    if (unlink(tmppath)) {
        ereport(WARNING, (errmsg("[ErrorTable/Normal]Unlink cache file failed: %s", tmppath)));
    }
}

/*
 * Open a temporary file in a specific tablespace or dirctory.
 * Subroutine for OpenTemporaryFile, which see for details.
 */
static File OpenTemporaryFileInTablespaceOrDir(Oid tblspcOid, bool rejectError)
{
    char tempdirpath[MAXPGPATH];
    char tempfilepath[MAXPGPATH];
    File file;
    int rc = EOK;

    /*
     * Identify the tempfile directory for this tablespace.
     *
     * If someone tries to specify pg_global, use pg_default instead.
     */
    if (tblspcOid == DEFAULTTABLESPACE_OID || tblspcOid == GLOBALTABLESPACE_OID) {
        /* The default tablespace is {datadir}/base */
        rc = snprintf_s(tempdirpath, sizeof(tempdirpath), sizeof(tempdirpath) - 1,
                        "%s/%s", DEFTBSDIR, PG_TEMP_FILES_DIR);
    } else if (ENABLE_DSS && tblspcOid == InvalidOid) {
        rc = snprintf_s(tempdirpath, sizeof(tempdirpath), sizeof(tempdirpath) - 1, "%s", SS_PG_TEMP_FILES_DIR);
    } else {
        /* All other tablespaces are accessed via symlinks */
#ifdef PGXC
        /* Postgres-XC tablespaces include node name in path */
        if (ENABLE_DSS) {
            rc = snprintf_s(tempdirpath,
                            sizeof(tempdirpath),
                            sizeof(tempdirpath) - 1,
                            "%s/%u/%s/%s",
                            TBLSPCDIR,
                            tblspcOid,
                            TABLESPACE_VERSION_DIRECTORY,
                            PG_TEMP_FILES_DIR);
        } else {
            rc = snprintf_s(tempdirpath,
                            sizeof(tempdirpath),
                            sizeof(tempdirpath) - 1,
                            "%s/%u/%s_%s/%s",
                            TBLSPCDIR,
                            tblspcOid,
                            TABLESPACE_VERSION_DIRECTORY,
                            g_instance.attr.attr_common.PGXCNodeName,
                            PG_TEMP_FILES_DIR);
        }
#else
        rc = snprintf_s(tempdirpath,
                        sizeof(tempdirpath),
                        sizeof(tempdirpath) - 1,
                        "%s/%u/%s/%s",
                        TBLSPCDIR,
                        tblspcOid,
                        TABLESPACE_VERSION_DIRECTORY,
                        PG_TEMP_FILES_DIR);
#endif
    }
    securec_check_ss(rc, "", "");

    /*
     * Generate a tempfile name that should be unique within the current
     * database instance.
     */
    rc = snprintf_s(tempfilepath,
                    sizeof(tempfilepath),
                    sizeof(tempfilepath) - 1,
                    "%s/%s%lu.%ld",
                    tempdirpath,
                    PG_TEMP_FILE_PREFIX,
                    t_thrd.proc_cxt.MyProcPid,
                    u_sess->storage_cxt.tempFileCounter++);
    securec_check_ss(rc, "", "");

    /*
     * Open the file.  Note: we don't use O_EXCL, in case there is an orphaned
     * temp file that can be reused.
     */
    file = PathNameOpenFile(tempfilepath, O_RDWR | O_CREAT | O_TRUNC | PG_BINARY, 0600);
    if (file <= 0) {
        /*
         * We might need to create the tablespace's tempfile directory, if no
         * one has yet done so.
         *
         * Don't check for error from mkdir; it could fail if someone else
         * just did the same thing.  If it doesn't work then we'll bomb out on
         * the second create attempt, instead.
         */
        (void)mkdir(tempdirpath, S_IRWXU);

        file = PathNameOpenFile(tempfilepath, O_RDWR | O_CREAT | O_TRUNC | PG_BINARY, 0600);
        if (file <= 0 && rejectError)
            ereport(
                ERROR, (errcode_for_file_access(), errmsg("could not create temporary file \"%s\": %m", tempfilepath)));
    }

    return file;
}

/*
 * close this file after the thief has close the real fd.
 * see also FileClose().
 */
void FileCloseWithThief(File file)
{
    Vfd* vfdP = &GetVfdCache()[file];
    if (!FileIsNotOpen(file)) {
        /* remove the file from the lru ring */
        Delete(file);
        /* the thief has close the real fd */
        Assert(!vfdP->infdCache);
        AddVfdNfile(-1);
        /* clean up fd flag */
        vfdP->fd = VFD_CLOSED;
    }

    Assert(!(vfdP->fdstate & FD_TEMP_FILE_LIMIT));
    if (vfdP->resowner) {
        ResourceOwnerForgetFile(vfdP->resowner, file);
    }
    FreeVfd(file);
}

/*
 * Create a new file.  The directory containing it must already exist.  Files
 * created this way are subject to temp_file_limit and are automatically
 * closed at end of transaction, but are not automatically deleted on close
 * because they are intended to be shared between cooperating backends.
 *
 * If the file is inside the top-level temporary directory, its name should
 * begin with PG_TEMP_FILE_PREFIX so that it can be identified as temporary
 * and deleted at startup by RemovePgTempFiles().  Alternatively, it can be
 * inside a directory created with PathnameCreateTemporaryDir(), in which case
 * the prefix isn't needed.
 */
File PathNameCreateTemporaryFile(char *path, bool error_on_failure)
{
    File file;

    ResourceOwnerEnlargeFiles(t_thrd.utils_cxt.CurrentResourceOwner);

    /*
     * Open the file.  Note: we don't use O_EXCL, in case there is an orphaned
     * temp file that can be reused.
     */
    file = PathNameOpenFile(path, O_RDWR | O_CREAT | O_TRUNC | PG_BINARY, 0600);
    if (file <= 0) {
        if (error_on_failure)
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not create temporary file \"%s\": %m", path)));
        else
            return file;
    }

    /* Mark it for temp_file_limit accounting. */
    GetVfdCache()[file].fdstate |= FD_TEMP_FILE_LIMIT;

    /* Register it for automatic close. */
    RegisterTemporaryFile(file);

    return file;
}

/*
 * Open a file that was created with PathNameCreateTemporaryFile, possibly in
 * another backend.  Files opened this way don't count against the
 * temp_file_limit of the caller, are read-only and are automatically closed
 * at the end of the transaction but are not deleted on close.
 */
File PathNameOpenTemporaryFile(char *path)
{
    File        file;

    ResourceOwnerEnlargeFiles(t_thrd.utils_cxt.CurrentResourceOwner);

    /* We open the file read-only. */
    file = PathNameOpenFile(path, O_RDONLY | PG_BINARY, 0600);
    /* If no such file, then we don't raise an error. */
    if (file <= 0 && errno != ENOENT)
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("could not open temporary file \"%s\": %m",
                        path)));

    if (file > 0) {
        /* Register it for automatic close. */
        RegisterTemporaryFile(file);
    }

    return file;
}

/*
 * Delete a file by pathname.  Return true if the file existed, false if
 * didn't.
 */
bool PathNameDeleteTemporaryFile(const char *path, bool error_on_failure)
{
    struct stat filestats;
    int stat_errno;

    /* Get the final size for pgstat reporting. */
    if (stat(path, &filestats) != 0)
        stat_errno = errno;
    else
        stat_errno = 0;

    /*
     * Unlike FileClose's automatic file deletion code, we tolerate
     * non-existence to support BufFileDeleteShared which doesn't know how
     * many segments it has to delete until it runs out.
     */
    if (FILE_POSSIBLY_DELETED(stat_errno))
        return false;

    if (unlink(path) < 0) {
        if (!FILE_POSSIBLY_DELETED(errno))
            ereport(error_on_failure ? ERROR : LOG,
                (errcode_for_file_access(), errmsg("cannot unlink temporary file \"%s\": %m", path)));
        return false;
    }

    if (stat_errno == 0)
        ReportTemporaryFileUsage(path, filestats.st_size);
    else {
        errno = stat_errno;
        ereport(LOG, (errcode_for_file_access(), errmsg("could not stat file \"%s\": %m", path)));
    }

    return true;
}

/*
 * close a file when done with it
 */
void FileClose(File file)
{
    Vfd* vfdP = NULL;

    Assert(FileIsValid(file));
    vfd *vfdcache = GetVfdCache();

    DO_DB(ereport(LOG, (errmsg("FileClose: %d (%s)", file, vfdcache[file].fileName))));

    vfdP = &vfdcache[file];

    if (!FileIsNotOpen(file)) {
        /* remove the file from the lru ring */
        Delete(file);

        /*
         * If the fd has been closed but the flag has't been set to
         * VFD_CLOSED, recieve a signal and FATAL, thread_exit callback
         * will try to close the same fd again and then will fail, cause PANIC.
         * So hold interrupts here.
         */
        HOLD_INTERRUPTS();
        /* close the file */
        DataFileIdCloseFile(vfdP);

        AddVfdNfile(-1);
        vfdP->fd = VFD_CLOSED;
        RESUME_INTERRUPTS();
    }

    if (vfdP->fdstate & FD_TEMP_FILE_LIMIT) {
        /* Subtract its size from current usage (do first in case of error) */
        u_sess->storage_cxt.temporary_files_size -= vfdP->fileSize;
        perm_space_decrease(GetUserId(), (uint64)vfdP->fileSize, SP_SPILL);		
        vfdP->fileSize = 0;
    }

    /*
     * Delete the file if it was temporary, and make a log entry if wanted
     */
    if (vfdP->fdstate & FD_DELETE_AT_CLOSE) {
        struct stat filestats;
        int stat_errno;

        /*
         * If we get an error, as could happen within the ereport/elog calls,
         * we'll come right back here during transaction abort.  Reset the
         * flag to ensure that we can't get into an infinite loop.  This code
         * is arranged to ensure that the worst-case consequence is failing to
         * emit log message(s), not failing to attempt the unlink.
         */
        vfdP->fdstate &= ~FD_DELETE_AT_CLOSE;

        /* first try the stat() */
        if (stat(vfdP->fileName, &filestats))
            stat_errno = errno;
        else
            stat_errno = 0;

        /* in any case do the unlink */
        if (unlink(vfdP->fileName))
            ereport(LOG, (errmsg("could not unlink file \"%s\": %m", vfdP->fileName)));

        /* and last report the stat results */
        if (stat_errno == 0) {
            ReportTemporaryFileUsage(vfdP->fileName, filestats.st_size);
        } else {
            errno = stat_errno;
            ereport(LOG, (errmsg("could not stat file \"%s\": %m", vfdP->fileName)));
        }
    }

    /* Unregister it from the resource owner */
    if (vfdP->resowner)
        ResourceOwnerForgetFile(vfdP->resowner, file);

    /*
     * Return the Vfd slot to the free list
     */
    FreeVfd(file);
}

/*
 * FilePrefetch - initiate asynchronous read of a given range of the file.
 * The logical seek position is unaffected.
 *
 * Currently the only implementation of this function is using posix_fadvise
 * which is the simplest standardized interface that accomplishes this.
 * We could add an implementation using libaio in the future; but note that
 * this API is inappropriate for libaio, which wants to have a buffer provided
 * to read into.
 */
int FilePrefetch(File file, off_t offset, int amount, uint32 wait_event_info)
{
#if defined(USE_POSIX_FADVISE) && defined(POSIX_FADV_WILLNEED)
    int returnCode;

    Assert(FileIsValid(file));
    vfd *vfdcache = GetVfdCache();

    if (is_dss_file(vfdcache[file].fileName)) {
        return 0;
    }

    DO_DB(ereport(LOG,
                  (errmsg("FilePrefetch: %d (%s) " INT64_FORMAT " %d",
                          file,
                          vfdcache[file].fileName,
                          (int64)offset,
                          amount))));

    returnCode = FileAccess(file);
    if (returnCode < 0)
        return returnCode;

    pgstat_report_waitevent(wait_event_info);
    returnCode = posix_fadvise(vfdcache[file].fd, offset, amount, POSIX_FADV_WILLNEED);
    pgstat_report_waitevent(WAIT_EVENT_END);

    return returnCode;
#else
    Assert(FileIsValid(file));
    return 0;
#endif
}

void FileWriteback(File file, off_t offset, off_t nbytes)
{
    int returnCode;

    Assert(FileIsValid(file));
    vfd *vfdcache = GetVfdCache();
    DO_DB(ereport(LOG,
                  (errmsg("FileWriteback: %d (%s) " INT64_FORMAT " " INT64_FORMAT,
                          file,
                          vfdcache[file].fileName,
                          (int64)offset,
                          (int64)nbytes))));

    /*
     * Caution: do not call pg_flush_data with nbytes = 0, it could trash the
     * file's seek position.  We prefer to define that as a no-op here.
     */
    if (nbytes <= 0)
        return;

    returnCode = FileAccess(file);
    if (returnCode < 0)
        return;

    pg_flush_data(vfdcache[file].fd, offset, nbytes);
}

// FilePRead
// 		Read from a file at a given offset , using pread() for multithreading safe
// 		NOTE: The file offset is not changed.
int FilePRead(File file, char* buffer, int amount, off_t offset, uint32 wait_event_info)
{
    int returnCode;
    int count = 0;

    Assert(FileIsValid(file));
    vfd *vfdcache = GetVfdCache();
    DO_DB(ereport(LOG,
                  (errmsg("FilePRead: %d (%s) " INT64_FORMAT " %d",
                          file,
                          vfdcache[file].fileName,
                          (int64)vfdcache[file].seekPos,
                          amount))));

    returnCode = FileAccess(file);
    if (returnCode < 0)
        return returnCode;

    /* collect io info for statistics */
    if (u_sess->attr.attr_resource.use_workload_manager && u_sess->attr.attr_resource.enable_logical_io_statistics)
        IOStatistics(IO_TYPE_READ, 1, amount);

retry:

    PROFILING_MDIO_START();
    pgstat_report_waitevent(wait_event_info);
    PGSTAT_INIT_TIME_RECORD();
    PGSTAT_START_TIME_RECORD();
    returnCode = pread(vfdcache[file].fd, buffer, (size_t)amount, offset);
    PGSTAT_END_TIME_RECORD(DATA_IO_TIME);
    pgstat_report_waitevent(WAIT_EVENT_END);
    PROFILING_MDIO_END_READ((uint32)amount, returnCode);

    if (returnCode < 0) {
        /*
         * Windows may run out of kernel buffers and return "Insufficient
         * system resources" error.  Wait a bit and retry to solve it.
         *
         * It is rumored that EINTR is also possible on some Unix filesystems,
         * in which case immediate retry is indicated.
         */
#ifdef WIN32
        DWORD error = GetLastError();

        switch (error) {
            case ERROR_NO_SYSTEM_RESOURCES:
                pg_usleep(1000L);
                errno = EINTR;
                break;
            default:
                _dosmaperr(error);
                break;
        }
#endif
        /* OK to retry if interrupted */
        if (errno == EINTR)
            goto retry;
        if (errno == EIO) {
            if (count < EIO_RETRY_TIMES) {
                count++;
                ereport(WARNING, (errmsg("FilePRead: %d (%s) " INT64_FORMAT " %d \
                    failed, then retry: Input/Output ERROR",
                        file,
                        vfdcache[file].fileName,
                        (int64)vfdcache[file].seekPos,
                        amount)));
                goto retry;
            }
        }
    }

    return returnCode;
}

int FileWrite(File file, const char* buffer, int amount, off_t offset, int fastExtendSize)
{
    Assert(fastExtendSize >= 0);
    int returnCode;
    /*
     * A zero fill via fallocate() is specified by omitting the buffer.
     * we did not consider dio here because fallocate has no relation with dio.
     * if buffer == NULL, means can use fallocate, caller will check it
     */
    vfd *vfdcache = GetVfdCache();
    if (buffer == NULL) {
        if (fastExtendSize == 0) {
            /* fast allocate large disk space for this heap file each 8MB */
            fastExtendSize = (int)(u_sess->attr.attr_storage.fast_extend_file_size * 1024LL);
        }

        /* fast allocate large disk space for this file each 8MB */
        if ((offset % fastExtendSize) == 0) {
            returnCode = fallocate(vfdcache[file].fd, FALLOC_FL_KEEP_SIZE, offset, fastExtendSize);
            if (returnCode != 0) {
                ereport(ERROR,
                        (errcode_for_file_access(),
                         errmsg("fallocate(fd=%d, amount=%d, offset=%ld),write count(%d), errno(%d), "
                                "maybe you use adio without XFS filesystem, if you really want do this,"
                                "please turn off GUC parameter enable_fast_allocate",
                                vfdcache[file].fd,
                                fastExtendSize,
                                (int64)offset,
                                returnCode,
                                errno)));
            }
        }

        /* write all zeros into a new page */
        returnCode = fallocate(vfdcache[file].fd, 0, offset, amount);
        if (returnCode != 0) {
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("fallocate(fd=%d, amount=%d, offset=%ld),write count(%d), errno(%d), "
                            "maybe you use adio without XFS filesystem, if you really want do this,"
                            "please turn off GUC parameter enable_fast_allocate",
                            vfdcache[file].fd,
                            amount,
                            (int64)offset,
                            returnCode,
                            errno)));
        }

        /* assign returnCode with buffer size */
        returnCode = amount;
    } else {
        PROFILING_MDIO_START();
        PGSTAT_INIT_TIME_RECORD();
        PGSTAT_START_TIME_RECORD();
        returnCode = pwrite(vfdcache[file].fd, buffer, (size_t)amount, offset);
        PGSTAT_END_TIME_RECORD(DATA_IO_TIME);
        PROFILING_MDIO_END_WRITE((uint32)amount, returnCode);
    }
    return returnCode;
}

// FilePWrite
// 		Write to a file at a given offset, using pwrite() for multithreading safe
// 		NOTE: The file offset is not changed.
int FilePWrite(File file, const char* buffer, int amount, off_t offset, uint32 wait_event_info, int fastExtendSize)
{
    int returnCode;
    int count = 0;

    Assert(FileIsValid(file));
    vfd *vfdcache = GetVfdCache();
    DO_DB(ereport(LOG,
                  (errmsg("FilePWrite: %d (%s) " INT64_FORMAT " %d",
                          file,
                          vfdcache[file].fileName,
                          (int64)vfdcache[file].seekPos,
                          amount))));

    returnCode = FileAccess(file);
    if (returnCode < 0)
        return returnCode;

    /* collect io info for statistics */
    if (u_sess->attr.attr_resource.use_workload_manager && u_sess->attr.attr_resource.enable_logical_io_statistics)
        IOStatistics(IO_TYPE_WRITE, 1, amount);

    /*
     * If enforcing temp_file_limit and it's a temp file, check to see if the
     * write would overrun temp_file_limit, and throw error if so.	Note: it's
     * really a modularity violation to throw error here; we should set errno
     * and return -1.  However, there's no way to report a suitable error
     * message if we do that.  All current callers would just throw error
     * immediately anyway, so this is safe at present.
     */
    if (vfdcache[file].fdstate & FD_TEMP_FILE_LIMIT) {
        off_t past_write = offset + amount;

        if (past_write > vfdcache[file].fileSize) {
            uint64 newTotal = u_sess->storage_cxt.temporary_files_size;
            uint64 incSize = (uint64)(past_write - vfdcache[file].fileSize);
            WLMGeneralParam* g_wlm_params = &u_sess->wlm_cxt->wlm_params;
            unsigned char state = g_wlm_params->iostate;
            g_wlm_params->iostate = IOSTATE_WRITE;
            perm_space_increase(GetUserId(), incSize, SP_SPILL);
            g_wlm_params->iostate = state;

            if (u_sess->attr.attr_sql.temp_file_limit >= 0) {
                newTotal += incSize;
                if (newTotal > (uint64)(uint32)u_sess->attr.attr_sql.temp_file_limit * (uint64)1024)
                    ereport(ERROR,
                            (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
                             errmsg("temporary file size exceeds temp_file_limit (%dkB)",
                                    u_sess->attr.attr_sql.temp_file_limit)));
            }
        }
    }

retry:
    errno = 0;

    pgstat_report_waitevent(wait_event_info);
    returnCode = FileWrite(file, buffer, amount, offset, fastExtendSize);
    pgstat_report_waitevent(WAIT_EVENT_END);

    /* if write didn't set errno, assume problem is no disk space */
    if (returnCode != amount && errno == 0)
        errno = ENOSPC;

    if (returnCode >= 0) {
        /* maintain fileSize and temporary_files_size if it's a temp file */
        if (vfdcache[file].fdstate & FD_TEMP_FILE_LIMIT) {
            off_t past_write = offset + amount;

            if (past_write > vfdcache[file].fileSize) {
                u_sess->storage_cxt.temporary_files_size += past_write - vfdcache[file].fileSize;
                vfdcache[file].fileSize = past_write;
            }
        }
    } else {
        /*
         * See comments in FileRead()
         */
#ifdef WIN32
        DWORD error = GetLastError();

        switch (error) {
            case ERROR_NO_SYSTEM_RESOURCES:
                pg_usleep(1000L);
                errno = EINTR;
                break;
            default:
                _dosmaperr(error);
                break;
        }
#endif
        /* OK to retry if interrupted */
        if (errno == EINTR)
            goto retry;
        if (errno == EIO) {
            if (count < EIO_RETRY_TIMES) {
                count++;
                ereport(WARNING, (errmsg("FilePWrite: %d (%s) " INT64_FORMAT " %d \
                    failed, then retry: Input/Output ERROR",
                        file,
                        vfdcache[file].fileName,
                        (int64)vfdcache[file].seekPos,
                        amount)));
                goto retry;
            }
        }
    }

    return returnCode;
}

#ifndef ENABLE_LITE_MODE
template <typename dlistType>
static int FileAsyncSubmitIO(io_context_t aio_context, dlistType dList, int dListCount)
{
    int retCount = 0;
    int submitCount = 0;
    int tryTimes = 0;
    int insufficientTimes = 0;

    u_sess->storage_cxt.AsyncSubmitIOCount = 0;
    do {
        Assert(dListCount > submitCount);
        retCount =
            io_submit(aio_context, (long)(dListCount - submitCount), (struct iocb**)((dlistType)dList + submitCount));
        if (retCount == -EAGAIN) {
            /* Insufficient resources, try again */
            insufficientTimes++;
            pg_usleep(1000);
            ereport(LOG,
                    (errmsg(
                         "insufficient resources, async submit io count(%d), times(%d) ", submitCount, insufficientTimes)));
            continue;
        }

        if (retCount < 0) {
            /* submit error */
            ereport(LOG, (errmsg("async submit io return code(%d)", retCount)));
            return retCount;
        }

        submitCount += retCount;
        u_sess->storage_cxt.AsyncSubmitIOCount = submitCount;
        if (submitCount >= dListCount) {
            /* all IO requests have been submitted. */
            u_sess->storage_cxt.AsyncSubmitIOCount = 0;
            break;
        }

        tryTimes++;
        ereport(LOG, (errmsg("async submit io count(%d), times(%d) ", submitCount, tryTimes)));
        pg_usleep(1000);
    } while (tryTimes <= 100 && insufficientTimes <= 1000);

    return submitCount;
}

/*
 * @Description: row store async read api
 * @Param[IN] dList:aio desc list
 * @Param[IN] dn: aio desc list count
 * @Return: 0 succeed, others error happend
 * @See also:
 */
int FileAsyncRead(AioDispatchDesc_t** dList, int32 dn)
{
    int returnCode = 0;
    vfd *vfdcache = GetVfdCache();
    for (int i = 0; i < dn; i++) {
        File file = dList[i]->aiocb.aio_fildes;

        Assert(FileIsValid(file));
        DO_DB(ereport(LOG,
                      (errmsg("FileAsyncRead: fd(%d), filename(%s), seekpos(%ld)",
                              file,
                              vfdcache[file].fileName,
                              (int64)vfdcache[file].seekPos))));

        // jeh FileAccess may block opening file
        returnCode = FileAccess(file);
        if (returnCode < 0) {
            // aio debug error
            ereport(ERROR, (errcode_for_file_access(), errmsg("FileAsyncRead, file access failed %d", returnCode)));
            return returnCode;
        }

        /* replace the virtual fd with the real one */
        dList[i]->aiocb.aio_fildes = vfdcache[file].fd;
    }

    /*
     * Dispatch the dList
     * If there are too many on the system queue then retry.
     * Alternatives- abort the requests, or have another thread do
     * the blocking...
     *
     * Took a shortcut here and sent all the requests to a single context
     * If the number of requests is too great, and there are more threads
     * than request types it makes sense to spread them around.
     */
    io_context_t aio_context = CompltrContext(dList[0]->blockDesc.reqType, 0);

    returnCode = FileAsyncSubmitIO<AioDispatchDesc_t**>(aio_context, dList, dn);
    if (returnCode != dn) {
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("io_submit() async read failed %d, dispatch count(%d)", returnCode, dn)));
    }

    return returnCode;
}

/*
 * @Description:  row store async write api
 * @Param[IN] dList: aio desc list
 * @Param[IN] dn: aio desc list count
 * @Return: 0 succeed, others error happend
 * @See also:
 */
int FileAsyncWrite(AioDispatchDesc_t** dList, int32 dn)
{
    int returnCode = 0;
    vfd *vfdcache = GetVfdCache();
    for (int i = 0; i < dn; i++) {
        File file = dList[i]->aiocb.aio_fildes;

        Assert(FileIsValid(file));
        DO_DB(ereport(LOG,
                      (errmsg("FileAsyncRead: fd(%d), filename(%s), seekpos(%ld)",
                              file,
                              vfdcache[file].fileName,
                              (int64)vfdcache[file].seekPos))));

        if ((returnCode = FileAccess(file)) < 0) {
            // aio debug error
            ereport(ERROR, (errcode_for_file_access(), errmsg("FileAsyncWrite, file access failed %d", returnCode)));
            return returnCode;
        }

        /* replace the virtual fd with the real one */
        dList[i]->aiocb.aio_fildes = vfdcache[file].fd;
    }

    io_context_t aio_context = CompltrContext(dList[0]->blockDesc.reqType, 0);

    returnCode = FileAsyncSubmitIO<AioDispatchDesc_t**>(aio_context, dList, dn);
    if (returnCode != dn) {
        ereport(PANIC, (errmsg("io_submit() async write failed %d, dispatch count(%d)", returnCode, dn)));
    }

    return returnCode;
}

/*
 * @Description: column store close fd  for adio
 * @IN vfdList: aio desc vfd list
 * @IN vfdnum: aio desc vfd list count
 * @See also:
 */
void FileAsyncCUClose(File* vfdList, int32 vfdnum)
{
    for (int i = 0; i < vfdnum; i++) {
        File file = vfdList[i];

        if (FileIsValid(file)) {
            FileClose(file);
            vfdList[i] = VFD_CLOSED;
        } else {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("FileAsyncCUClose : invalid vfd(%d), SizeVfdCache(%lu)",
                            file,
                            GetSizeVfdCache())));
        }
    }
    return;
}

/*
 * @Description: column store async read api
 * @Param[IN] dList:aio desc list
 * @Param[IN] dn: aio desc list count
 * @Return: 0 succeed, others error happend
 * @See also:
 */
int FileAsyncCURead(AioDispatchCUDesc_t** dList, int32 dn)
{
    int returnCode = 0;
    File file;
    vfd *vfdcache = GetVfdCache();
    for (int i = 0; i < dn; i++) {
        file = dList[i]->aiocb.aio_fildes;
        Assert(FileIsValid(file));
        DO_DB(ereport(LOG,
                      (errmsg("FileAsyncRead: fd(%d), filename(%s), seekpos(%ld)",
                              file,
                              vfdcache[file].fileName,
                              (int64)vfdcache[file].seekPos))));

        returnCode = FileAccess(file);
        if (returnCode < 0) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("FileAccess() FAILED %d", returnCode)));
            return returnCode;
        }
        /* replace the virtual fd with the real one */
        dList[i]->aiocb.aio_fildes = vfdcache[file].fd;
    }

    io_context_t aio_context = CompltrContext(dList[0]->cuDesc.reqType, 0);

    returnCode = FileAsyncSubmitIO<AioDispatchCUDesc_t**>(aio_context, dList, dn);
    if (returnCode != dn) {
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("io_submit() async cu read failed %d, dispatch count(%d)", returnCode, dn)));
    }

    return returnCode;
}

/*
 * @Description: column store async wite api
 * @Param[IN] dList:aio desc list
 * @Param[IN] dn: aio desc list count
 * @Return: 0 succeed, others error happend
 * @See also:
 */
int FileAsyncCUWrite(AioDispatchCUDesc_t** dList, int32 dn)
{
    int returnCode = 0;
    vfd *vfdcache = GetVfdCache();
    Assert(dn > 0);
    for (int i = 0; i < dn; i++) {
        File file = dList[i]->aiocb.aio_fildes;

        Assert(FileIsValid(file));

        DO_DB(ereport(LOG,
                      (errmsg("FileAsyncCUWrite: fd(%d), filename(%s), seekpos(%ld)",
                              file,
                              vfdcache[file].fileName,
                              (int64)vfdcache[file].seekPos))));

        returnCode = FileAccess(file);
        if (returnCode < 0) {
            // adio debug error
            ereport(ERROR, (errcode_for_file_access(), errmsg("FileAccess() FAILED %d", returnCode)));
            return returnCode;
        }
        /* replace the virtual fd with the real one */
        dList[i]->aiocb.aio_fildes = vfdcache[file].fd;
    }

    io_context_t aio_context = CompltrContext(dList[0]->cuDesc.reqType, 0);

    returnCode = FileAsyncSubmitIO<AioDispatchCUDesc_t**>(aio_context, dList, dn);
    if (returnCode != dn) {
        ereport(PANIC, (errmsg("io_submit() async cu write failed %d, dispatch count(%d)", returnCode, dn)));
    }

    return returnCode;
}
#endif

void FileFastExtendFile(File file, uint32 offset, uint32 size, bool keep_size)
{
    int returnCode;
    int mode = 0;

    Assert(FileIsValid(file));
    returnCode = FileAccess(file);
    if (returnCode < 0) {
        // adio debug error
        ereport(ERROR, (errcode_for_file_access(), errmsg("FileAccess() extend file failed %d", returnCode)));
        return;
    }

    if (keep_size) {
        mode = FALLOC_FL_KEEP_SIZE;
    }
    vfd *vfdcache = GetVfdCache();
    returnCode = fallocate(vfdcache[file].fd, mode, offset, size);
    if (returnCode != 0) {
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("fallocate(fd=%d, amount=%u, offset=%u),write count(%d), errno(%d), "
                        "maybe you use adio without XFS filesystem, if you really want do this,"
                        "please turn off GUC parameter enable_fast_allocate",
                        vfdcache[file].fd,
                        size,
                        offset,
                        returnCode,
                        errno)));
    }

    return;
}

int FileSync(File file, uint32 wait_event_info)
{
    int returnCode;

    Assert(FileIsValid(file));
    vfd *vfdcache = GetVfdCache();

    DO_DB(ereport(LOG, (errmsg("FileSync: %d (%s)", file, vfdcache[file].fileName))));

    returnCode = FileAccess(file);
    if (returnCode < 0)
        return returnCode;

    pgstat_report_waitevent(wait_event_info);
    PGSTAT_INIT_TIME_RECORD();
    PGSTAT_START_TIME_RECORD();
    returnCode = pg_fsync(vfdcache[file].fd);
    PGSTAT_END_TIME_RECORD(DATA_IO_TIME);
    pgstat_report_waitevent(WAIT_EVENT_END);

    return returnCode;
}

// FileSeek
// 		parameter *whence* only supports SEEK_END for multithreading backends.
// 		FilePRead() and FilePWrite() have the offset parameter, so do NOT use FileSeek to
// 		set the file offset before read and write.
off_t FileSeek(File file, off_t offset, int whence)
{
    int returnCode;

    Assert(FileIsValid(file));
    /* only SEEK_END is valid for multithreading backends */
    Assert(whence == SEEK_END && offset == 0);
    vfd *vfdcache = GetVfdCache();
    DO_DB(ereport(LOG,
                  (errmsg("FileSeek: %d (%s) " INT64_FORMAT " " INT64_FORMAT " %d",
                          file,
                          vfdcache[file].fileName,
                          (int64)vfdcache[file].seekPos,
                          (int64)offset,
                          whence))));

    if (FileIsNotOpen(file)) {
        returnCode = FileAccess(file);
        if (returnCode < 0)
            return returnCode;
    }

    vfdcache[file].seekPos = lseek(vfdcache[file].fd, offset, whence);
    return vfdcache[file].seekPos;
}

/*
 * XXX not actually used but here for completeness
 */
#ifdef NOT_USED
off_t FileTell(File file)
{
    Assert(FileIsValid(file));
    vfd *vfdcache = GetVfdCache();
    DO_DB(ereport(LOG, (errmsg("FileTell %d (%s)", file, vfdcache[file].fileName))));
    return vfdcache[file].seekPos;
}
#endif

int FileTruncate(File file, off_t offset, uint32 wait_event_info)
{
    int returnCode;

    Assert(FileIsValid(file));
    vfd *vfdcache = GetVfdCache();

    DO_DB(ereport(LOG, (errmsg("FileTruncate %d (%s)", file, vfdcache[file].fileName))));

    returnCode = FileAccess(file);
    if (returnCode < 0)
        return returnCode;

    pgstat_report_waitevent(wait_event_info);
    returnCode = ftruncate(vfdcache[file].fd, offset);
    pgstat_report_waitevent(WAIT_EVENT_END);

    if (returnCode == 0 && vfdcache[file].fileSize > offset) {
        /* adjust our state for truncation of a temp file */
        Assert(vfdcache[file].fdstate & FD_TEMP_FILE_LIMIT);
        uint64 descSize = (uint64)(vfdcache[file].fileSize - offset);
        perm_space_decrease(GetUserId(), descSize, SP_SPILL);
        u_sess->storage_cxt.temporary_files_size -= descSize;
        vfdcache[file].fileSize = offset;
    }

    return returnCode;
}

/*
 * Return the pathname associated with an open file.
 *
 * The returned string points to an internal buffer, which is valid until
 * the file is closed.
 */
char* FilePathName(File file)
{
    Assert(FileIsValid(file));
    vfd *vfdcache = GetVfdCache();

    return vfdcache[file].fileName;
}

/*
 * @Description:  Return the fd associated with an open file.
 * @in file -  file descriptor
 * @return -  The returned fd is valid until the file is closed.
 */
int FileFd(File file)
{
    Assert(FileIsValid(file));
    vfd *vfdcache = GetVfdCache();
    return vfdcache[file].fd;
}

/*
 * Make room for another allocatedDescs[] array entry if needed and possible.
 * Returns true if an array element is available.
 */
static bool ReserveAllocatedDesc(void)
{
    AllocateDesc* newDescs = NULL;
    int newMax;

    /* Quick out if array already has a free slot. */
    if (u_sess->storage_cxt.numAllocatedDescs < u_sess->storage_cxt.maxAllocatedDescs)
        return true;

    /*
     * If the array hasn't yet been created in the current process, initialize
     * it with FD_MINFREE / 2 elements.  In many scenarios this is as many as
     * we will ever need, anyway.  We don't want to look at max_safe_fds
     * immediately because set_max_safe_fds() may not have run yet.
     */
    if (u_sess->storage_cxt.allocatedDescs == NULL) {
        newMax = FD_MINFREE / 2;
        newDescs = (AllocateDesc*)MemoryContextAlloc(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), newMax * sizeof(AllocateDesc));
        /* Out of memory already?  Treat as fatal error. */
        if (newDescs == NULL)
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
        u_sess->storage_cxt.allocatedDescs = newDescs;
        u_sess->storage_cxt.maxAllocatedDescs = newMax;
        return true;
    }

    /*
     * Consider enlarging the array beyond the initial allocation used above.
     * By the time this happens, max_safe_fds should be known accurately.
     *
     * We mustn't let allocated descriptors hog all the available FDs, and in
     * practice we'd better leave a reasonable number of FDs for VFD use.  So
     * set the maximum to max_safe_fds / 2.  (This should certainly be at
     * least as large as the initial size, FD_MINFREE / 2.)
     */
    newMax = t_thrd.storage_cxt.max_safe_fds / 2;
    if (newMax > u_sess->storage_cxt.maxAllocatedDescs) {
        newDescs = (AllocateDesc*)repalloc(u_sess->storage_cxt.allocatedDescs, newMax * sizeof(AllocateDesc));
        /* Treat out-of-memory as a non-fatal error. */
        if (newDescs == NULL)
            return false;
        u_sess->storage_cxt.allocatedDescs = newDescs;
        u_sess->storage_cxt.maxAllocatedDescs = newMax;
        return true;
    }

    /* Can't enlarge allocatedDescs[] any more. */
    return false;
}

int AllocateSocket(const char* ipaddr, int port)
{
    struct addrinfo* addr = NULL;
    struct addrinfo* naddr = NULL;
    struct addrinfo hint;
    char servname[64];
    int sockfd = -1;
    bool is_connected = false;
    errno_t rc = EOK;
    int retrynum = 0;

    DO_DB(ereport(LOG, (errmsg("AllocateFile: Allocated %d", u_sess->storage_cxt.numAllocatedDescs))));

    Assert(ipaddr != NULL);
restart:
    /* Can we allocate another non-virtual FD? */
    if (!ReserveAllocatedDesc())
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                 errmsg("exceeded maxAllocatedDescs (%d) while trying to open file \"%s:%d\"",
                        u_sess->storage_cxt.maxAllocatedDescs,
                        ipaddr,
                        port)));

    /* Close excess kernel FDs. */
    ReleaseLruFiles();

    rc = memset_s(&hint, sizeof(hint), 0, sizeof(hint));
    securec_check(rc, "", "");
    hint.ai_socktype = (int)SOCK_STREAM;
    hint.ai_family = AF_UNSPEC;
    rc = sprintf_s(servname, sizeof(servname), "%d", port);
    securec_check_ss(rc, "", "");
    (void)pg_getaddrinfo_all(ipaddr, servname, &hint, &addr);

    for (naddr = addr; naddr != NULL; naddr = naddr->ai_next) {
        SockAddr saddr;
        rc = memcpy_s(&saddr.addr, sizeof(saddr.addr), naddr->ai_addr, naddr->ai_addrlen);
        securec_check(rc, "", "");
        saddr.salen = naddr->ai_addrlen;
TryAgain:
        if (sockfd < 0 && (sockfd = socket(saddr.addr.ss_family, SOCK_STREAM, 0)) < 0) {
            if (errno == EMFILE || errno == ENFILE) {
                int save_errno = errno;
#ifdef USE_ASSERT_CHECKING
                if (save_errno == ENFILE) {
                    DumpOpenFiles(g_gauss_pid_dir);
                    pg_usleep(10000000L);
                    if (addr != NULL)
                        pg_freeaddrinfo_all(hint.ai_family, addr);
                    ereport(PANIC,
                            (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                             errmsg("out of file descriptors: %m; release and retry")));
                } else {
                    ereport(LOG,
                            (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                             errmsg("out of file descriptors: %m; release and retry")));
                }
#else
                ereport(LOG,
                        (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                         errmsg("out of file descriptors: %m; release and retry")));
#endif

                errno = 0;
                if (ReleaseLruFile())
                    goto TryAgain;
                errno = save_errno;
            } else
                continue;
        }

        /* setting socket to non-blocking mode */
        unsigned long ul = 1;
        (void)ioctl(sockfd, FIONBIO, &ul);

        /* create the connection */
        if (connect(sockfd, (struct sockaddr*)&saddr.addr, saddr.salen) == -1) {
            if (errno == EINPROGRESS) {
                /* connection timeout */
                struct pollfd pollfds;
                pollfds.fd = sockfd;
                pollfds.events = POLLOUT | POLLWRBAND;
retry:
                t_thrd.int_cxt.ImmediateInterruptOK = true;
                int ret = poll(&pollfds, 1, CONNECTION_TIME_OUT * 1000);
                t_thrd.int_cxt.ImmediateInterruptOK = false;
                if (ret == -1) {
                    if (errno == EINTR)
                        goto retry;

                    if (addr != NULL)
                        pg_freeaddrinfo_all(hint.ai_family, addr);
                    ereport(ERROR,
                            (errcode(errcode_for_socket_access()),
                             errmsg("Invalid socket fd \"%d\" for poll():%m", sockfd)));
                } else if (ret == 0) {
                    /* timeout or can't connect server */
                    ereport(LOG, (errmsg("connect timeout socket fd \"%d\" for poll():%m", sockfd)));
                    continue;
                } else {
                    socklen_t len = sizeof(ret);
                    (void)getsockopt(sockfd, SOL_SOCKET, SO_ERROR, &ret, (socklen_t*)&len);
                    if (ret != 0) {
                        ereport(LOG, (errmsg("connect error fd \"%d\" for poll() SO_ERROR:%d", sockfd, ret)));
                        continue;
                    }
                }
            } else {
                ereport(LOG, (errcode(errcode_for_socket_access()), errmsg("connect error fd \"%d\":%m", sockfd)));
                continue;
            }
        }

        /* setting socket to blocking mode */
        ul = 0;
        (void)ioctl(sockfd, FIONBIO, &ul);

        is_connected = true;
        break;
    }

    if (addr != NULL)
        pg_freeaddrinfo_all(hint.ai_family, addr);

    if (is_connected) {
        /* set SO_KEEPALIVE on */
        int on = 1;
        if (setsockopt(sockfd, SOL_SOCKET, SO_KEEPALIVE, (char*)&on, sizeof(on)) < 0) {
            ereport(LOG, (errmsg("set SO_KEEPALIVE failed: %m")));
        }

        AllocateDesc* desc = &u_sess->storage_cxt.allocatedDescs[u_sess->storage_cxt.numAllocatedDescs];

        desc->kind = AllocateDescSocket;
        desc->desc.sock = sockfd;
        desc->create_subid = GetCurrentSubTransactionId();

        struct sockaddr_in sin;
        socklen_t slen = sizeof(sockaddr_in);
        rc = memset_s(&sin, slen, 0, slen);
        securec_check(rc, "", "");
        if (getsockname(sockfd, (struct sockaddr*)&sin, &slen) == 0) {
            ereport(LOG,
                    (errmsg("open socket(fd=%d) \"%s:%d\" at allocatedDescs[%d] local \"%s:%d\"",
                            sockfd,
                            ipaddr,
                            port,
                            u_sess->storage_cxt.numAllocatedDescs,
                            inet_ntoa(sin.sin_addr),
                            ntohs(sin.sin_port))));
        }

        u_sess->storage_cxt.numAllocatedDescs++;
        return desc->desc.sock;
    } else {
        if (sockfd >= 0) {
            (void)close(sockfd);
            sockfd = -1;
        }
        if (++retrynum > RETRY_LIMIT)
            ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE), errmsg("\"%s:%d\" connect failed", ipaddr, port)));
        else
            goto restart;
    }
    return -1;
}

/*
 * Routines that want to use stdio (ie, FILE*) should use AllocateFile
 * rather than plain fopen().  This lets fd.c deal with freeing FDs if
 * necessary to open the file.	When done, call FreeFile rather than fclose.
 *
 * Note that files that will be open for any significant length of time
 * should NOT be handled this way, since they cannot share kernel file
 * descriptors with other files; there is grave risk of running out of FDs
 * if anyone locks down too many FDs.  Most callers of this routine are
 * simply reading a config file that they will read and close immediately.
 *
 * fd.c will automatically close all files opened with AllocateFile at
 * transaction commit or abort; this prevents FD leakage if a routine
 * that calls AllocateFile is terminated prematurely by ereport(ERROR).
 *
 * Ideally this should be the *only* direct call of fopen() in the backend.
 */
FILE* AllocateFile(const char* name, const char* mode)
{
    FILE* file = NULL;

    DO_DB(ereport(LOG, (errmsg("AllocateFile: Allocated %d (%s)", u_sess->storage_cxt.numAllocatedDescs, name))));

    /* Can we allocate another non-virtual FD? */
    if (!ReserveAllocatedDesc())
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                 errmsg("exceeded maxAllocatedDescs (%d) while trying to open file \"%s\"",
                        u_sess->storage_cxt.maxAllocatedDescs,
                        name)));

    /* Close excess kernel FDs. */
    ReleaseLruFiles();

TryAgain:
    if ((file = fopen(name, mode)) != NULL) {
        AllocateDesc* desc = &u_sess->storage_cxt.allocatedDescs[u_sess->storage_cxt.numAllocatedDescs];

        desc->kind = AllocateDescFile;
        desc->desc.file = file;
        desc->create_subid = GetCurrentSubTransactionId();
        u_sess->storage_cxt.numAllocatedDescs++;
        return desc->desc.file;
    }

    if (errno == EMFILE || errno == ENFILE) {
        int save_errno = errno;
#ifdef USE_ASSERT_CHECKING
        if (save_errno == ENFILE) {
            DumpOpenFiles(g_gauss_pid_dir);
            pg_usleep(10000000L);
            ereport(PANIC,
                    (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("out of file descriptors: %m; release and retry")));
        } else {
            ereport(LOG,
                    (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("out of file descriptors: %m; release and retry")));
        }
#else
        ereport(
            LOG, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("out of file descriptors: %m; release and retry")));
#endif

        errno = 0;
        if (ReleaseLruFile())
            goto TryAgain;
        errno = save_errno;
    }

    return NULL;
}

/*
 * Like AllocateFile, but returns an unbuffered fd like open(2)
 */
int OpenTransientFile(FileName fileName, int fileFlags, int fileMode)
{
    int fd;
    DO_DB(ereport(
              LOG, (errmsg("OpenTransientFile: Allocated %d (%s)", u_sess->storage_cxt.numAllocatedDescs, fileName))));

    if (!ReserveAllocatedDesc()) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
            errmsg("exceeded maxAllocatedDescs (%d) while trying to open file \"%s\"",
                u_sess->storage_cxt.maxAllocatedDescs, fileName)));
    }
    ReleaseLruFiles();
    /*
     * The test against MAX_ALLOCATED_DESCS prevents us from overflowing
     * allocatedFiles[]; the test against max_safe_fds prevents BasicOpenFile
     * from hogging every one of the available FDs, which'd lead to infinite
     * looping.
     */
    if (u_sess->storage_cxt.numAllocatedDescs >= u_sess->storage_cxt.maxAllocatedDescs ||
        u_sess->storage_cxt.numAllocatedDescs >= t_thrd.storage_cxt.max_safe_fds - 1)
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("exceeded MAX_ALLOCATED_DESCS while trying to open file \"%s\"", fileName)));

    fd = BasicOpenFile(fileName, fileFlags, fileMode);
    if (fd >= 0) {
        AllocateDesc* desc = &u_sess->storage_cxt.allocatedDescs[u_sess->storage_cxt.numAllocatedDescs];

        desc->kind = AllocateDescRawFD;
        desc->desc.fd = fd;
        desc->create_subid = GetCurrentSubTransactionId();
        u_sess->storage_cxt.numAllocatedDescs++;

        return fd;
    }

    return -1; /* failure */
}

/*
 * Free an AllocateDesc of any type.
 *
 * The argument *must* point into the allocatedDescs[] array.
 */
static int FreeDesc(AllocateDesc* desc)
{
    int result;

    /* Close the underlying object */
    switch (desc->kind) {
        case AllocateDescFile:
            result = fclose(desc->desc.file);
            break;
        case AllocateDescDir:
            result = closedir(desc->desc.dir);
            break;
        case AllocateDescRawFD:
            result = close(desc->desc.fd);
            break;
        case AllocateDescSocket:
            result = closesocket(desc->desc.sock);
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("AllocateDesc kind not recognized")));
            result = 0; /* keep compiler quiet */
            break;
    }

    /* Compact storage in the allocatedDescs array */
    u_sess->storage_cxt.numAllocatedDescs--;
    *desc = u_sess->storage_cxt.allocatedDescs[u_sess->storage_cxt.numAllocatedDescs];

    return result;
}

int FreeSocket(int sockfd)
{
    int i;

    DO_DB(ereport(LOG, (errmsg("FreeFile: Allocated %d", u_sess->storage_cxt.numAllocatedDescs))));

    /* Remove file from list of allocated files, if it's present */
    for (i = u_sess->storage_cxt.numAllocatedDescs; --i >= 0;) {
        AllocateDesc* desc = &u_sess->storage_cxt.allocatedDescs[i];

        if (desc->kind == AllocateDescSocket && desc->desc.sock == sockfd)
            return FreeDesc(desc);
    }

    /* Only get here if someone passes us a file not in allocatedDescs */
    ereport(WARNING, (errmsg("socket fd passed to FreeSocket was not obtained from AllocateSocket")));

    return closesocket(sockfd);
}

/*
 * Close a file returned by AllocateFile.
 *
 * Note we do not check fclose's return value --- it is up to the caller
 * to handle close errors.
 */
int FreeFile(FILE* file)
{
    int i;

    DO_DB(ereport(LOG, (errmsg("FreeFile: Allocated %d", u_sess->storage_cxt.numAllocatedDescs))));

    /* Remove file from list of allocated files, if it's present */
    for (i = u_sess->storage_cxt.numAllocatedDescs; --i >= 0;) {
        AllocateDesc* desc = &u_sess->storage_cxt.allocatedDescs[i];

        if (desc->kind == AllocateDescFile && desc->desc.file == file)
            return FreeDesc(desc);
    }

    /* Only get here if someone passes us a file not in allocatedDescs */
    ereport(WARNING, (errmsg("file passed to FreeFile was not obtained from AllocateFile")));

    return fclose(file);
}

void GlobalStatsCleanupFiles()
{
    if (IsGlobalStatsTrackerProcess()) {
        while (u_sess->storage_cxt.numAllocatedDescs > 0) {
            (void)FreeDesc(&u_sess->storage_cxt.allocatedDescs[0]);
        }
    }
}

/*
 * Close a file returned by OpenTransientFile.
 *
 * Note we do not check close's return value --- it is up to the caller
 * to handle close errors.
 */
int CloseTransientFile(int fd)
{
    int i;

    DO_DB(ereport(LOG, (errmsg("CloseTransientFile: Allocated %d", u_sess->storage_cxt.numAllocatedDescs))));

    /* Remove fd from list of allocated files, if it's present */
    for (i = u_sess->storage_cxt.numAllocatedDescs; --i >= 0;) {
        AllocateDesc* desc = &u_sess->storage_cxt.allocatedDescs[i];

        if (desc->kind == AllocateDescRawFD && desc->desc.fd == fd)
            return FreeDesc(desc);
    }

    /* Only get here if someone passes us a file not in allocatedDescs */
    ereport(WARNING, (errmsg("fd passed to CloseTransientFile was not obtained from OpenTransientFile")));

    return close(fd);
}

/*
 * Routines that want to use <dirent.h> (ie, DIR*) should use AllocateDir
 * rather than plain opendir().  This lets fd.c deal with freeing FDs if
 * necessary to open the directory, and with closing it after an elog.
 * When done, call FreeDir rather than closedir.
 *
 * Ideally this should be the *only* direct call of opendir() in the backend.
 */
DIR* AllocateDir(const char* dirname)
{
    DIR* dir = NULL;

    DO_DB(ereport(LOG, (errmsg("AllocateDir: Allocated %d (%s)", u_sess->storage_cxt.numAllocatedDescs, dirname))));

    /* Can we allocate another non-virtual FD? */
    if (!ReserveAllocatedDesc())
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                 errmsg("exceeded maxAllocatedDescs (%d) while trying to open directory \"%s\"",
                        u_sess->storage_cxt.maxAllocatedDescs,
                        dirname)));

    /* Close excess kernel FDs. */
    ReleaseLruFiles();

TryAgain:
    if ((dir = opendir(dirname)) != NULL) {
        AllocateDesc* desc = &u_sess->storage_cxt.allocatedDescs[u_sess->storage_cxt.numAllocatedDescs];

        desc->kind = AllocateDescDir;
        desc->desc.dir = dir;
        desc->create_subid = GetCurrentSubTransactionId();
        u_sess->storage_cxt.numAllocatedDescs++;
        return desc->desc.dir;
    }

    if (errno == EMFILE || errno == ENFILE) {
        int save_errno = errno;
#ifdef USE_ASSERT_CHECKING
        if (save_errno == ENFILE) {
            DumpOpenFiles(g_gauss_pid_dir);
            pg_usleep(10000000L);
            ereport(PANIC,
                    (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("out of file descriptors: %m; release and retry")));
        } else {
            ereport(LOG,
                    (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("out of file descriptors: %m; release and retry")));
        }
#else
        ereport(
            LOG, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("out of file descriptors: %m; release and retry")));
#endif

        errno = 0;
        if (ReleaseLruFile())
            goto TryAgain;
        errno = save_errno;
    }

    return NULL;
}

/*
 * Read a directory opened with AllocateDir, ereport'ing any error.
 *
 * This is easier to use than raw readdir() since it takes care of some
 * otherwise rather tedious and error-prone manipulation of errno.	Also,
 * if you are happy with a generic error message for AllocateDir failure,
 * you can just do
 *
 *		example code: dir = AllocateDir(path);
 *		example code: while ((dirent = ReadDir(dir, path)) != NULL)
 *		example code: 	process dirent;
 *		example code: FreeDir(dir);
 *
 * since a NULL dir parameter is taken as indicating AllocateDir failed.
 * (Make sure errno hasn't been changed since AllocateDir if you use this
 * shortcut.)
 *
 * The pathname passed to AllocateDir must be passed to this routine too,
 * but it is only used for error reporting.
 */
struct dirent* ReadDir(DIR* dir, const char* dirname)
{
    struct dirent* dent = NULL;

    /* Give a generic message for AllocateDir failure, if caller didn't */
    if (dir == NULL) {
        char realPath[PATH_MAX] = {0};
        (void)realpath(dirname, realPath);
        ereport(WARNING, ((errmsg("open directory failed \"%s\"", realPath))));
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not open directory \"%s\": %m", dirname)));
    }

    errno = 0;
    if ((dent = gs_readdir(dir)) != NULL)
        return dent;

#ifdef WIN32

    /*
     * This fix is in mingw cvs (runtime/mingwex/dirent.c rev 1.4), but not in
     * released version
     */
    if (GetLastError() == ERROR_NO_MORE_FILES)
        errno = 0;
#endif

    if (errno) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not read directory \"%s\": %m", dirname)));
    }
    return NULL;
}

/*
 * Close a directory opened with AllocateDir.
 *
 * Note we do not check closedir's return value --- it is up to the caller
 * to handle close errors.
 */
int FreeDir(DIR* dir)
{
    Assert(dir != NULL);

    int i;

    DO_DB(ereport(LOG, (errmsg("FreeDir: Allocated %d", u_sess->storage_cxt.numAllocatedDescs))));

    /* Remove dir from list of allocated dirs, if it's present */
    for (i = u_sess->storage_cxt.numAllocatedDescs; --i >= 0;) {
        AllocateDesc* desc = &u_sess->storage_cxt.allocatedDescs[i];

        if (desc->kind == AllocateDescDir && desc->desc.dir == dir)
            return FreeDesc(desc);
    }

    /* Only get here if someone passes us a dir not in allocatedDescs */
    ereport(WARNING, (errmsg("dir passed to FreeDir was not obtained from AllocateDir")));

    return closedir(dir);
}

/*
 * closeAllVfds
 *
 * Force all VFDs into the physically-closed state, so that the fewest
 * possible number of kernel file descriptors are in use.  There is no
 * change in the logical state of the VFDs.
 */
void closeAllVfds(void)
{
    Index i;
    Size size = 0;
    size = GetSizeVfdCache();
    if (size > 0) {
        Assert(FileIsNotOpen(0)); /* Make sure ring not corrupted */
        for (i = 1; i < size; i++) {
            if (!FileIsNotOpen(i))
                LruDelete((File)i);
        }
    }
}

/*
 * DestroyAllVfds
 *
 * Force all VFDs into the physically-closed state. Release all VFD resources
 */
void DestroyAllVfds(void)
{
    Index i;
    ereport(DEBUG5, (errmsg("Thread \"%lu\" trace: closeAllVfds", (unsigned long int)t_thrd.proc_cxt.MyProcPid)));
    if (GetSizeVfdCache() > 0) {
        Assert(FileIsNotOpen(0)); /* Make sure ring not corrupted */
        for (i = 1; i < GetSizeVfdCache(); i++) {
            if (FileIsValid((int)i))
                FileClose((File)i);
        }
        vfd *vfdcache = GetVfdCache();
        if (vfdcache != NULL) {
            pfree(vfdcache);
        }
    }
    SetVfdCache(NULL);
    SetSizeVfdCache(0);
}

/*
 * SetTempTablespaces
 *
 * Define a list (actually an array) of OIDs of tablespaces to use for
 * temporary files.  This list will be used until end of transaction,
 * unless this function is called again before then.  It is caller's
 * responsibility that the passed-in array has adequate lifespan (typically
 * it'd be allocated in u_sess->top_transaction_mem_cxt).
 */
void SetTempTablespaces(Oid* tableSpaces, int numSpaces)
{
    Assert(numSpaces >= 0);
    u_sess->storage_cxt.tempTableSpaces = tableSpaces;
    u_sess->storage_cxt.numTempTableSpaces = numSpaces;

    /*
     * Select a random starting point in the list.	This is to minimize
     * conflicts between backends that are most likely sharing the same list
     * of temp tablespaces.  Note that if we create multiple temp files in the
     * same transaction, we'll advance circularly through the list --- this
     * ensures that large temporary sort files are nicely spread across all
     * available tablespaces.
     */
    if (numSpaces > 1)
        u_sess->storage_cxt.nextTempTableSpace = (int)(gs_random() % numSpaces);
    else
        u_sess->storage_cxt.nextTempTableSpace = 0;
}

/*
 * TempTablespacesAreSet
 *
 * Returns TRUE if SetTempTablespaces has been called in current transaction.
 * (This is just so that tablespaces.c doesn't need its own per-transaction
 * state.)
 */
bool TempTablespacesAreSet(void)
{
    return (u_sess->storage_cxt.numTempTableSpaces >= 0);
}

/*
 * GetTempTablespaces
 *
 * Populate an array with the OIDs of the tablespaces that should be used for
 * temporary files.  Return the number that were copied into the output array.
 */
int
GetTempTablespaces(Oid *tableSpaces, int numSpaces)
{
    int         i;

    Assert(TempTablespacesAreSet());
    for (i = 0; i < u_sess->storage_cxt.numTempTableSpaces && i < numSpaces; ++i)
        tableSpaces[i] = u_sess->storage_cxt.tempTableSpaces[i];

    return i;
}

/*
 * GetNextTempTableSpace
 *
 * Select the next temp tablespace to use.	A result of InvalidOid means
 * to use the current database's default tablespace.
 */
Oid GetNextTempTableSpace(void)
{
    if (u_sess->storage_cxt.numTempTableSpaces > 0) {
        /* Advance nextTempTableSpace counter with wraparound */
        if (++u_sess->storage_cxt.nextTempTableSpace >= u_sess->storage_cxt.numTempTableSpaces)
            u_sess->storage_cxt.nextTempTableSpace = 0;
        return u_sess->storage_cxt.tempTableSpaces[u_sess->storage_cxt.nextTempTableSpace];
    }
    return InvalidOid;
}

/*
 * AtEOSubXact_Files
 *
 * Take care of subtransaction commit/abort.  At abort, we close temp files
 * that the subtransaction may have opened.  At commit, we reassign the
 * files that were opened to the parent subtransaction.
 */
void AtEOSubXact_Files(bool isCommit, SubTransactionId mySubid, SubTransactionId parentSubid)
{
    Index i;

    for (i = 0; i < (unsigned int)(u_sess->storage_cxt.numAllocatedDescs); i++) {
        if (u_sess->storage_cxt.allocatedDescs[i].create_subid == mySubid) {
            if (isCommit)
                u_sess->storage_cxt.allocatedDescs[i].create_subid = parentSubid;
            else {
                /* have to recheck the item after FreeDesc (ugly) */
                (void)FreeDesc(&u_sess->storage_cxt.allocatedDescs[i--]);
            }
        }
    }
}

/*
 * AtEOXact_Files
 *
 * This routine is called during transaction commit or abort (it doesn't
 * particularly care which).  All still-open per-transaction temporary file
 * VFDs are closed, which also causes the underlying files to be deleted
 * (although they should've been closed already by the ResourceOwner
 * cleanup). Furthermore, all "allocated" stdio files are closed. We also
 * forget any transaction-local temp tablespace list.
 */
void AtEOXact_Files(void)
{
    CleanupTempFiles(false);
    u_sess->storage_cxt.tempTableSpaces = NULL;
    u_sess->storage_cxt.numTempTableSpaces = -1;
}

/*
 * AtProcExit_Files
 *
 * on_proc_exit hook to clean up temp files during backend shutdown.
 * Here, we want to clean up *all* temp files including interXact ones.
 */
void AtProcExit_Files(int code, Datum arg)
{
    DestroyAllVfds();
}

/*
 * Close temporary files and delete their underlying files.
 *
 * isProcExit: if true, this is being called as the backend process is
 * exiting. If that's the case, we should remove all temporary files; if
 * that's not the case, we are being called for transaction commit/abort
 * and should only remove transaction-local temp files.  In either case,
 * also clean up "allocated" stdio files, dirs and fds.
 */
static void CleanupTempFiles(bool isProcExit)
{
    Index i;

    /*
     * Careful here: at proc_exit we need extra cleanup, not just
     * xact_temporary files.
     */
    vfd *vfdcache = GetVfdCache();
    if (isProcExit || u_sess->storage_cxt.have_xact_temporary_files) {
        Assert(FileIsNotOpen(0)); /* Make sure ring not corrupted */
        for (i = 1; i < GetSizeVfdCache(); i++) {
            unsigned short fdstate = vfdcache[i].fdstate;

            if (((fdstate & FD_DELETE_AT_CLOSE) || (fdstate & FD_CLOSE_AT_EOXACT)) && 
                vfdcache[i].fileName != NULL) {
                /*
                 * If we're in the process of exiting a backend process, close
                 * all temporary files. Otherwise, only close temporary files
                 * local to the current transaction. They should be closed by
                 * the ResourceOwner mechanism already, so this is just a
                 * debugging cross-check.
                 */
                if (isProcExit)
                    FileClose((File)i);
                else if (fdstate & FD_CLOSE_AT_EOXACT) {
                    ereport(WARNING,
                            (errmsg("temporary file %s not closed at end-of-transaction",
                                    vfdcache[i].fileName)));
                    FileClose((File)i);
                }
            } else if ((fdstate & FD_ERRTBL_LOG) && vfdcache[i].fileName != NULL) {
                /* first unlink the physical file because FileClose() will destroy filename. */
                if ((fdstate & FD_ERRTBL_LOG_OWNER) && unlink(vfdcache[i].fileName)) {
                    ereport(WARNING,
                            (errmsg("[ErrorTable/Abort]Unlink cache file failed: %s",
                                    vfdcache[i].fileName)));
                }
                /* close this cache file about error table and clean vfd's flag */
                FileClose((File)i);
            }
        }

        u_sess->storage_cxt.have_xact_temporary_files = false;
    }

    /* Clean up "allocated" stdio files, dirs and fds. */
    while (u_sess->storage_cxt.numAllocatedDescs > 0)
        (void)FreeDesc(&u_sess->storage_cxt.allocatedDescs[0]);
}

/*
 * Remove temporary and temporary relation files left over from a prior
 * postmaster session
 *
 * This should be called during postmaster startup.  It will forcibly
 * remove any leftover files created by OpenTemporaryFile and any leftover
 * temporary relation files created by mdcreate.
 *
 * NOTE: we could, but don't, call this during a post-backend-crash restart
 * cycle.  The argument for not doing it is that someone might want to examine
 * the temp files for debugging purposes.  This does however mean that
 * OpenTemporaryFile had better allow for collision with an existing temp
 * file name.
 */
void RemovePgTempFiles(void)
{
    char temp_path[MAXPGPATH];
    DIR* spc_dir = NULL;
    struct dirent* spc_de = NULL;
    errno_t rc = EOK;

    /*
     * First process temp files in pg_default ($PGDATA/base)
     */
    rc = snprintf_s(temp_path, sizeof(temp_path), sizeof(temp_path) - 1, "%s/%s", DEFTBSDIR, PG_TEMP_FILES_DIR);
    securec_check_ss(rc, "", "");
    RemovePgTempFilesInDir(temp_path, false);
    RemovePgTempRelationFiles(DEFTBSDIR);

    /*
     * Cycle through temp directories for all non-default tablespaces.
     */
    spc_dir = AllocateDir(TBLSPCDIR);
    if (spc_dir == NULL) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("Allocate dir failed.")));
    }

    while ((spc_de = ReadDir(spc_dir, "pg_tblspc")) != NULL) {
        if (strcmp(spc_de->d_name, ".") == 0 || strcmp(spc_de->d_name, "..") == 0)
            continue;

        /*
         * subDir returned by ReadDir will be overwritten by the next invoking.
         * therefore, the result needs to be saved.
         */
        char curSubDir[MAXPGPATH] = {0};
        rc = strncpy_s(curSubDir, MAXPGPATH, spc_de->d_name, strlen(spc_de->d_name));
        securec_check(rc, "", "");
#ifdef PGXC
        /* Postgres-XC tablespaces include node name in path */
        if (ENABLE_DSS) {
            rc = snprintf_s(temp_path,
                            sizeof(temp_path),
                            sizeof(temp_path) - 1,
                            "%s/%s/%s/%s",
                            TBLSPCDIR,
                            spc_de->d_name,
                            TABLESPACE_VERSION_DIRECTORY,
                            PG_TEMP_FILES_DIR);
            securec_check_ss(rc, "", "");
        } else {
            rc = snprintf_s(temp_path,
                            sizeof(temp_path),
                            sizeof(temp_path) - 1,
                            "%s/%s/%s_%s/%s",
                            TBLSPCDIR,
                            spc_de->d_name,
                            TABLESPACE_VERSION_DIRECTORY,
                            g_instance.attr.attr_common.PGXCNodeName,
                            PG_TEMP_FILES_DIR);
            securec_check_ss(rc, "", "");
        }
#else
        rc = snprintf_s(temp_path,
                        sizeof(temp_path),
                        sizeof(temp_path) - 1,
                        "%s/%s/%s/%s",
                        TBLSPCDIR,
                        curSubDir,
                        TABLESPACE_VERSION_DIRECTORY,
                        PG_TEMP_FILES_DIR);
        securec_check_ss(rc, "", "");
#endif
        RemovePgTempFilesInDir(temp_path, false);

#ifdef PGXC
        /* Postgres-XC tablespaces include node name in path */
        if (ENABLE_DSS) {
            rc = snprintf_s(temp_path,
                            sizeof(temp_path),
                            sizeof(temp_path) - 1,
                            "%s/%s/%s",
                            TBLSPCDIR,
                            spc_de->d_name,
                            TABLESPACE_VERSION_DIRECTORY);
            securec_check_ss(rc, "", "");
        } else {
            rc = snprintf_s(temp_path,
                            sizeof(temp_path),
                            sizeof(temp_path) - 1,
                            "%s/%s/%s_%s",
                            TBLSPCDIR,
                            spc_de->d_name,
                            TABLESPACE_VERSION_DIRECTORY,
                            g_instance.attr.attr_common.PGXCNodeName);
            securec_check_ss(rc, "", "");
        }
#else
        rc = snprintf_s(temp_path,
                        sizeof(temp_path),
                        sizeof(temp_path) - 1,
                        "%s/%s/%s",
                        TBLSPCDIR,
                        curSubDir,
                        TABLESPACE_VERSION_DIRECTORY);
        securec_check_ss(rc, "\0", "\0");
#endif
        RemovePgTempRelationFiles(temp_path);
    }

    (void)FreeDir(spc_dir);

    /*
     * In EXEC_BACKEND case there is a pgsql_tmp directory at the top level of
     * t_thrd.proc_cxt.DataDir as well.
     */
#ifdef EXEC_BACKEND
    if (ENABLE_DSS) {
        RemovePgTempFilesInDir(SS_PG_TEMP_FILES_DIR, false);
    } else {
        RemovePgTempFilesInDir(PG_TEMP_FILES_DIR, false);
    }
#endif
}

/* Process one pgsql_tmp directory for RemovePgTempFiles */
static void RemovePgTempFilesInDir(const char* tmpdirname, bool unlinkAll)
{
    DIR* temp_dir = NULL;
    struct dirent* temp_de = NULL;
    char rm_path[MAXPGPATH];
    errno_t rc = EOK;

    temp_dir = AllocateDir(tmpdirname);
    if (temp_dir == NULL) {
        /* anything except ENOENT is fishy */
        if (!FILE_POSSIBLY_DELETED(errno)) {
            ereport(LOG, (errmsg("could not open temporary-files directory \"%s\": %m", tmpdirname)));
        }
        return;
    }

    while ((temp_de = ReadDir(temp_dir, tmpdirname)) != NULL) {
        if (strcmp(temp_de->d_name, ".") == 0 || strcmp(temp_de->d_name, "..") == 0)
            continue;

        rc = snprintf_s(rm_path, sizeof(rm_path), sizeof(rm_path) - 1, "%s/%s", tmpdirname, temp_de->d_name);
        securec_check_ss(rc, "", "");

        if (unlinkAll || strncmp(temp_de->d_name, PG_TEMP_FILE_PREFIX, strlen(PG_TEMP_FILE_PREFIX)) == 0) {
            struct stat statbuf;

            /* note that we ignore any error here and below */
            if (lstat(rm_path, &statbuf) < 0) {
                continue;
            }

            if (S_ISDIR(statbuf.st_mode)) {
                RemovePgTempFilesInDir(rm_path, true);
                rmdir(rm_path);
            } else {
                unlink(rm_path);
            }
        } else {
            ereport(LOG, (errmsg("unexpected file found in temporary-files directory: \"%s\"", rm_path)));
        }
    }

    (void)FreeDir(temp_dir);
}

/* Process one tablespace directory, look for per-DB subdirectories */
static void RemovePgTempRelationFiles(const char* tsdirname)
{
    DIR* ts_dir = NULL;
    struct dirent* de = NULL;
    char dbspace_path[MAXPGPATH];

    ts_dir = AllocateDir(tsdirname);
    if (ts_dir == NULL) {
        /* anything except ENOENT is fishy */
        if (errno != ENOENT)
            ereport(LOG, (errmsg("could not open tablespace directory \"%s\": %m", tsdirname)));
        return;
    }

    while ((de = ReadDir(ts_dir, tsdirname)) != NULL) {
        int i = 0;
        errno_t rc = EOK;
        /*
         * We're only interested in the per-database directories, which have
         * numeric names.  Note that this code will also (properly) ignore "."
         * and "..".
         */
        while (isdigit((unsigned char)de->d_name[i]))
            ++i;
        if (de->d_name[i] != '\0' || i == 0)
            continue;

        rc = snprintf_s(dbspace_path, sizeof(dbspace_path), sizeof(dbspace_path) - 1, "%s/%s", tsdirname, de->d_name);
        securec_check_ss(rc, "", "");
        RemovePgTempRelationFilesInDbspace(dbspace_path);
    }

    (void)FreeDir(ts_dir);
}

/* Process one per-dbspace directory for RemovePgTempRelationFiles */
static void RemovePgTempRelationFilesInDbspace(const char* dbspacedirname)
{
    DIR* dbspace_dir = NULL;
    struct dirent* de = NULL;
    char rm_path[MAXPGPATH];
    errno_t rc = EOK;

    dbspace_dir = AllocateDir(dbspacedirname);
    if (dbspace_dir == NULL) {
        /* we just saw this directory, so it really ought to be there */
        ereport(LOG, (errmsg("could not open dbspace directory \"%s\": %m", dbspacedirname)));
        return;
    }

    while ((de = ReadDir(dbspace_dir, dbspacedirname)) != NULL) {
        if (!looks_like_temp_rel_name(de->d_name))
            continue;

        rc = snprintf_s(rm_path, sizeof(rm_path), sizeof(rm_path) - 1, "%s/%s", dbspacedirname, de->d_name);
        securec_check_ss(rc, "", "");

        (void)unlink(rm_path); /* note we ignore any error */
    }

    (void)FreeDir(dbspace_dir);
}

/* t<digits>_<digits>, or t<digits>_<digits>_<forkname> */
static bool looks_like_temp_rel_name(const char *name)
{
    int pos;
    int savepos;

    /* Must start with "t". */
    if (name[0] != 't') {
        return false;
    }

    /* Followed by a non-empty string of digits and then an underscore. */
    for (pos = 1; isdigit((unsigned char)name[pos]); ++pos) {
        ;
    }
    if (pos == 1 || name[pos] != '_') {
        return false;
    }

    /* Followed by another nonempty string of digits. */
    savepos = ++pos;
    while (isdigit((unsigned char)name[pos])) {
        ++pos;
    }
    if (savepos == pos) {
        return false;
    }

    /* We might have _forkname or .segment or both. */
    if (name[pos] == '_') {
        int forkchar = forkname_chars(&name[pos + 1], NULL);
        if (forkchar <= 0) {
            return false;
        }
        pos += forkchar + 1;
    }
    if (name[pos] == '.') {
        int segchar;

        for (segchar = 1; isdigit((unsigned char)name[pos + segchar]); ++segchar) {
            ;
        }
        if (segchar <= 1) {
            return false;
        }
        pos += segchar;
    }

    /* Now we should be at the end. */
    if (name[pos] != '\0') {
        return false;
    }
    return true;
}

void RemoveErrorCacheFiles()
{
    DIR* temp_dir = NULL;
    struct dirent* temp_de = NULL;
    char rm_path[MAXPGPATH];
    char* tmpdirname = "pg_errorinfo";
    errno_t rc = EOK;

    temp_dir = AllocateDir("pg_errorinfo");
    if (temp_dir == NULL) {
        /* anything except ENOENT is fishy */
        if (errno != ENOENT)
            ereport(LOG, (errmsg("could not open error cache directory \"%s\": %m", tmpdirname)));
        return;
    }

    while ((temp_de = ReadDir(temp_dir, tmpdirname)) != NULL) {
        if (strcmp(temp_de->d_name, ".") == 0 || strcmp(temp_de->d_name, "..") == 0)
            continue;

        rc = snprintf_s(rm_path, sizeof(rm_path), sizeof(rm_path) - 1, "%s/%s", tmpdirname, temp_de->d_name);
        securec_check_ss(rc, "", "");

        (void)unlink(rm_path); /* note we ignore any error */
    }

    (void)FreeDir(temp_dir);
}

void FreeAllAllocatedDescs(void)
{
    while (u_sess->storage_cxt.numAllocatedDescs > 0)
        (void)FreeDesc(&u_sess->storage_cxt.allocatedDescs[0]);
}

void GetFdGlobalVariables(void*** global_VfdCache, Size** global_SizeVfdCache)
{
    save_VfdCache = NULL;
    save_SizeVfdCache = 0;

    *global_VfdCache = (void**)GetVfdCachePtr();
    *global_SizeVfdCache = GetSizeVfdCachePtr();
}

void SwitchToGlobalVfdCache(void** vfd, Size* vfd_size)
{
    save_VfdCache = GetVfdCache();
    save_SizeVfdCache = GetSizeVfdCache();

    SetVfdCache((Vfd *)*vfd);
    SetSizeVfdCache(*vfd_size);
}

/* Set the THR_LOCAL VFD information from y */
void ResetToLocalVfdCache()
{
    if (save_VfdCache != NULL) {
        SetVfdCache(save_VfdCache);
        save_VfdCache = NULL;
    }
    if (save_SizeVfdCache != 0) {
        SetSizeVfdCache(save_SizeVfdCache);
        save_SizeVfdCache = 0;
    }
}

/*
 * Return the passed-in error level, or PANIC if g_instance.attr.attr_common.data_sync_retry is off.
 *
 * Failure to fsync any data file is cause for immediate panic, unless
 * g_instance.attr.attr_common.data_sync_retry is enabled.  Data may have been written to the operating
 * system and removed from our buffer pool already, and if we are running on
 * an operating system that forgets dirty data on write-back failure, there
 * may be only one copy of the data remaining: in the WAL.  A later attempt to
 * fsync again might falsely report success.  Therefore we must not allow any
 * further checkpoints to be attempted.  g_instance.attr.attr_common.data_sync_retry can in theory be
 * enabled on systems known not to drop dirty buffered data on write-back
 * failure (with the likely outcome that checkpoints will continue to fail
 * until the underlying problem is fixed).
 *
 * Any code that reports a failure from fsync() or related functions should
 * filter the error level with this function.
 */
int data_sync_elevel(int elevel)
{
    return g_instance.attr.attr_common.data_sync_retry ? elevel : PANIC;
}

/*
 * check whether the refcount of fd is zero.
 */
bool FdRefcntIsZero(SMgrRelation reln, ForkNumber forkNum)
{
    DataFileIdCacheEntry* entry = NULL;
    RelFileNodeForkNum fileNode = RelFileNodeForkNumFill(reln->smgr_rnode, forkNum, 0);

    uint32 newHash = VFDTableHashCode((void*)&fileNode);
    AutoMutexLock vfdLock(VFDMappingPartitionLock(newHash));
    vfdLock.lock();

    entry = (DataFileIdCacheEntry*)hash_search_with_hash_value(t_thrd.storage_cxt.DataFileIdCache,
                                                               (void*)&fileNode, newHash, HASH_FIND, NULL);
    if (entry != NULL) {
        Assert(entry->fd >= 0);
        Assert(entry->refcount > 0);

        vfdLock.unLock();
        return false;
    }
    vfdLock.unLock();

    return true;
}

bool repair_deleted_file_check(RelFileNodeForkNum fileNode, int fd)
{
    bool result = true;
    uint32 newHash = VFDTableHashCode((void*)&fileNode);
    AutoMutexLock vfdLock(VFDMappingPartitionLock(newHash));
    vfdLock.lock();
    DataFileIdCacheEntry* entry =
        (DataFileIdCacheEntry*)hash_search_with_hash_value(t_thrd.storage_cxt.DataFileIdCache,
        (void*)&fileNode, newHash, HASH_FIND, NULL);
    if (fd < 0) {
        if (entry != NULL && entry->repaired_fd >= 0) {
            result = false;
        }
    } else {
        if (entry != NULL && entry->fd >= 0) {
            entry->repaired_fd = entry->fd;
            entry->fd = fd;
        } else {
            result = false;
        }
    }
    vfdLock.unLock();
    return result;
}

/*
 * check whether the file is existed.
 */
FileExistStatus CheckFileExists(const char* path)
{
    struct stat statbuff;

    if (stat(path, &statbuff) == -1) {
        if (errno == ENOENT) {
            return FILE_NOT_EXIST;
        } else {
            return FILE_EXIST;
        }
    } else {
        if (!S_ISREG(statbuff.st_mode)) {
            return FILE_NOT_REG;
        } else {
            return FILE_EXIST;
        }
    }
}

/*
 * Compute hash code of RelFileNodeForkNum.
 */
static uint32 VFDTableHashCode(const void* tagPtr)
{
    return tag_hash(tagPtr, sizeof(RelFileNodeForkNum));
}

/*
 * Initialize all vfd locks.
 */
void InitializeVFDLocks(void)
{
    for (int i = 0; i < NUM_VFD_PARTITIONS; i++) {
        pthread_mutex_init(&VFDLockArray[i], NULL);
    }
}


/*
 * Alternate version that allows caller to specify the elevel for any
 * error report.  If elevel < ERROR, returns NULL on any error.
 */
struct dirent *ReadDirExtended(DIR *dir, const char *dirname, int elevel)
{
    struct dirent *dent;

    /* Give a generic message for AllocateDir failure, if caller didn't */
    if (dir == NULL) {
        ereport(elevel, (errcode_for_file_access(), errmsg("could not open directory \"%s\": %m", dirname)));
        return NULL;
    }

    errno = 0;
    if ((dent = readdir(dir)) != NULL)
        return dent;

    if (errno)
        ereport(elevel, (errcode_for_file_access(), errmsg("could not read directory \"%s\": %m", dirname)));
    return NULL;
}

/*
 * walkdir: recursively walk a directory, applying the action to each
 * regular file and directory (including the named directory itself).
 *
 * If process_symlinks is true, the action and recursion are also applied
 * to regular files and directories that are pointed to by symlinks in the
 * given directory; otherwise symlinks are ignored.  Symlinks are always
 * ignored in subdirectories, ie we intentionally don't pass down the
 * process_symlinks flag to recursive calls.
 *
 * Errors are reported at level elevel, which might be ERROR or less.
 *
 * See also walkdir in initdb.c, which is a frontend version of this logic.
 */
static void Walkdir(const char *path, void (*action)(const char *fname, bool isdir, int elevel), bool process_symlinks,
    int elevel)
{
    DIR *dir;
    struct dirent *de;

    dir = AllocateDir(path);
    if (dir == NULL) {
        ereport(elevel, (errcode_for_file_access(), errmsg("could not open directory \"%s\": %m", path)));
        return;
    }

    while ((de = ReadDirExtended(dir, path, elevel)) != NULL) {
        char subpath[MAXPGPATH * 2];
        struct stat fst;
        int sret;

        CHECK_FOR_INTERRUPTS();

        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
            continue;

        errno_t err_rc = snprintf_s(subpath, sizeof(subpath), sizeof(subpath) - 1, "%s/%s", path, de->d_name);
        securec_check_ss(err_rc, "", "");

        if (process_symlinks)
            sret = stat(subpath, &fst);
        else
            sret = lstat(subpath, &fst);

        if (sret < 0) {
            ereport(elevel, (errcode_for_file_access(), errmsg("could not stat file \"%s\": %m", subpath)));
            continue;
        }

        if (S_ISREG(fst.st_mode))
            (*action)(subpath, false, elevel);
        else if (S_ISDIR(fst.st_mode))
            Walkdir(subpath, action, false, elevel);
    }

    FreeDir(dir); /* we ignore any error here */

    /*
     * It's important to fsync the destination directory itself as individual
     * file fsyncs don't guarantee that the directory entry for the file is
     * synced.
     */
    (*action)(path, true, elevel);
}

static void UnlinkIfExistsFname(const char *fname, bool isdir, int elevel)
{
    if (isdir) {
        if (rmdir(fname) != 0 && !FILE_POSSIBLY_DELETED(errno)) {
            ereport(elevel, (errcode_for_file_access(), errmsg("could not rmdir directory \"%s\": %m", fname)));
        }
    } else {
        /* Use PathNameDeleteTemporaryFile to report filesize */
        PathNameDeleteTemporaryFile(fname, false);
    }
}

void FileAllocate(File file, uint32 offset, uint32 size)
{
    vfd *vfdcache = GetVfdCache();
    if (fallocate(vfdcache[file].fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, offset, size) < 0) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("fallocate failed on relation: \"%s\": ", FilePathName(file))));
    }
}

void FileAllocateDirectly(int fd, char* path, uint32 offset, uint32 size)
{
    if (fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, offset, size) < 0) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("fallocate failed on relation: \"%s\"", path)));
    }
}
