/* -------------------------------------------------------------------------
 *
 * md.cpp
 *	  This code manages relations that reside on magnetic disk.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/smgr/md.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <unistd.h>
#include <fcntl.h>
#include <sys/file.h>
#include <dirent.h>
#include <sys/types.h>

#include "miscadmin.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "access/extreme_rto/standby_read/standby_read_base.h"
#include "catalog/catalog.h"
#include "portability/instr_time.h"
#include "postmaster/bgwriter.h"
#include "postmaster/pagewriter.h"
#include "postmaster/pagerepair.h"
#include "storage/smgr/fd.h"
#include "storage/buf/bufmgr.h"
#include "storage/smgr/relfilenode.h"
#include "storage/copydir.h"
#include "storage/page_compression.h"
#include "storage/smgr/knl_usync.h"
#include "storage/smgr/smgr.h"
#include "storage/file/fio_device.h"
#include "utils/aiomem.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "pg_trace.h"
#include "pgstat.h"

#include "storage/cfs/cfs.h"
#include "storage/cfs/cfs_converter.h"
#include "storage/cfs/cfs_md.h"

constexpr mode_t FILE_RW_PERMISSION = 0600;

/* Populate a file tag describing an md.cpp segment file. */
#define INIT_MD_FILETAG(tag, rNode, forkNum, segNo)                         \
    do                                                                      \
    {                                                                       \
        errno_t errorno = EOK;                                              \
        errorno = memset_s(&(tag), sizeof(FileTag), (0), sizeof(FileTag));  \
        securec_check(errorno, "\0", "\0");                                 \
        (tag).handler = SYNC_HANDLER_MD;                                    \
        (tag).forknum = (forkNum);                                          \
        (tag).rnode = (rNode);                                              \
        (tag).segno = (segNo);                                              \
    } while (false);

inline static uint4 PageCompressChunkSize(SMgrRelation reln)
{
    return CHUNK_SIZE_LIST[GET_COMPRESS_CHUNK_SIZE((reln)->smgr_rnode.node.opt)];
}

/*
 *  The magnetic disk storage manager keeps track of open file
 *  descriptors in its own descriptor pool.  This is done to make it
 *  easier to support relations that are larger than the operating
 *  system's file size limit (often 2GBytes).  In order to do that,
 *  we break relations up into "segment" files that are each shorter than
 *  the OS file size limit.  The segment size is set by the RELSEG_SIZE
 *  configuration constant in pg_config.h.
 *
 *  On disk, a relation must consist of consecutively numbered segment
 *  files in the pattern
 *      -- Zero or more full segments of exactly RELSEG_SIZE blocks each
 *      -- Exactly one partial segment of size 0 <= size < RELSEG_SIZE blocks
 *      -- Optionally, any number of inactive segments of size 0 blocks.
 *  The full and partial segments are collectively the "active" segments.
 *  Inactive segments are those that once contained data but are currently
 *  not needed because of an mdtruncate() operation.  The reason for leaving
 *  them present at size zero, rather than unlinking them, is that other
 *  backends and/or the checkpointer might be holding open file references to
 *  such segments.	If the relation expands again after mdtruncate(), such
 *  that a deactivated segment becomes active again, it is important that
 *  such file references still be valid --- else data might get written
 *  out to an unlinked old copy of a segment file that will eventually
 *  disappear.
 *
 *  The file descriptor pointer (md_fd field) stored in the SMgrRelation
 *  cache is, therefore, just the head of a list of MdfdVec objects, one
 *  per segment.  But note the md_fd pointer can be NULL, indicating
 *  relation not open.
 *
 *  Also note that mdfd_chain == NULL does not necessarily mean the relation
 *  doesn't have another segment after this one; we may just not have
 *  opened the next segment yet.  (We could not have "all segments are
 *  in the chain" as an invariant anyway, since another backend could
 *  extend the relation when we weren't looking.)  We do not make chain
 *  entries for inactive segments, however; as soon as we find a partial
 *  segment, we assume that any subsequent segments are inactive.
 *
 *  All MdfdVec objects are palloc'd in the MdCxt memory context.
 */

/* local routines */
static void mdunlinkfork(const RelFileNodeBackend &rnode, ForkNumber forkNum, bool isRedo);
static MdfdVec *mdopen(SMgrRelation reln, ForkNumber forknum, ExtensionBehavior behavior);
static MdfdVec *_fdvec_alloc(void);
static char *_mdfd_segpath(const SMgrRelation reln, ForkNumber forknum, BlockNumber segno);
static MdfdVec *_mdfd_openseg(SMgrRelation reln, ForkNumber forkno, BlockNumber segno, int oflags);
static BlockNumber _mdnblocks(SMgrRelation reln, ForkNumber forknum, const MdfdVec *seg);
static void register_dirty_segment(SMgrRelation reln, ForkNumber forknum, const MdfdVec *seg);
static void register_unlink_segment(RelFileNodeBackend rnode, ForkNumber forknum, BlockNumber segno);


bool check_unlink_rel_hashtbl(RelFileNode rnode, ForkNumber forknum)
{
    HTAB* relfilenode_hashtbl = g_instance.bgwriter_cxt.unlink_rel_hashtbl;
    HTAB* relfilenode_fork_hashtbl = g_instance.bgwriter_cxt.unlink_rel_fork_hashtbl;
    ForkRelFileNode entry_key;
    bool found = false;

    LWLockAcquire(g_instance.bgwriter_cxt.rel_hashtbl_lock, LW_SHARED);
    (void)hash_search(relfilenode_hashtbl, &(rnode), HASH_FIND, &found);
    LWLockRelease(g_instance.bgwriter_cxt.rel_hashtbl_lock);

    if (!found) {
        entry_key.rnode = rnode;
        entry_key.forkNum = forknum;
        LWLockAcquire(g_instance.bgwriter_cxt.rel_one_fork_hashtbl_lock, LW_SHARED);
        (void)hash_search(relfilenode_fork_hashtbl, &(entry_key), HASH_FIND, &found);
        LWLockRelease(g_instance.bgwriter_cxt.rel_one_fork_hashtbl_lock);
    }
    return found;
}

/*
 * register_dirty_segment() -- Mark a relation segment as needing fsync
 *
 * If there is a local pending-ops table, just make an entry in it for
 * ProcessSyncRequests to process later.  Otherwise, try to pass off the
 * fsync request to the checkpointer process.  If that fails, just do the
 * fsync locally before returning (we hope this will not happen often
 * enough to be a performance problem).
 */
static void register_dirty_segment(SMgrRelation reln, ForkNumber forknum, const MdfdVec *seg)
{
    FileTag tag;

    INIT_MD_FILETAG(tag, reln->smgr_rnode.node, forknum, seg->mdfd_segno);

    /* Temp relations should never be fsync'd */
    Assert(!SmgrIsTemp(reln));

    if (!RegisterSyncRequest(&tag, SYNC_REQUEST, false /* retryOnError */)) {
        ereport(DEBUG1, (errmsg("could not forward fsync request because request queue is full")));
        if (FileSync(seg->mdfd_vfd, WAIT_EVENT_DATA_FILE_SYNC) < 0) {
            if (check_unlink_rel_hashtbl(reln->smgr_rnode.node, forknum)) {
                ereport(DEBUG1, (errmsg("could not fsync file \"%s\": %m", FilePathName(seg->mdfd_vfd))));
                return;
            }
        }
    }
}

/*
 * register_unlink_segment() -- Schedule a file to be deleted after next checkpoint
 */
static void register_unlink_segment(RelFileNodeBackend rnode, ForkNumber forknum, BlockNumber segno)
{
    FileTag tag;

    INIT_MD_FILETAG(tag, rnode.node, forknum, segno);

    /* Should never be used with temp relations */
    Assert(!RelFileNodeBackendIsTemp(rnode));

    RegisterSyncRequest(&tag, SYNC_UNLINK_REQUEST, true /* retryOnError */);
}

/*
 * md_register_forget_request() -- forget any fsyncs for a relation fork's segment
 */
void md_register_forget_request(RelFileNode rnode, ForkNumber forknum, BlockNumber segno)
{
    FileTag tag;
    INIT_MD_FILETAG(tag, rnode, forknum, segno);

    RegisterSyncRequest(&tag, SYNC_FORGET_REQUEST, true /* retryOnError */);
}

int openrepairfile(char* path, RelFileNodeForkNum filenode)
{
    int fd = -1;
    const int TEMPLEN = 8;
    volatile uint32 repair_flags = O_RDWR | PG_BINARY;
    char *temppath = (char *)palloc(strlen(path) + TEMPLEN);
    errno_t rc = sprintf_s(temppath, strlen(path) + TEMPLEN, "%s.repair", path);
    securec_check_ss(rc, "", "");
    ADIO_RUN()
    {
        repair_flags |= O_DIRECT;
    }
    ADIO_END();
    fd = DataFileIdOpenFile(temppath, filenode, (int)repair_flags, 0600);
    if (fd < 0) {
        ereport(WARNING, (errmsg("[file repair] could not open repair file %s: %s", temppath, TRANSLATE_ERRNO)));
    }
    pfree(temppath);
    return fd;
}

/*
 *	mdinit() -- Initialize private state for magnetic disk storage manager.
 */
void mdinit(void)
{
    if (EnableLocalSysCache()) {
        return;
    }
    u_sess->storage_cxt.MdCxt = AllocSetContextCreate(u_sess->top_mem_cxt, "MdSmgr", ALLOCSET_DEFAULT_MINSIZE,
                                                      ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
}

/*
 * mdexists() -- Does the physical file exist?
 *
 * Note: this will return true for lingering files, with pending deletions
 */
bool mdexists(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum)
{
    /*
     * Close it first, to ensure that we notice if the fork has been unlinked
     * since we opened it.
     */
    mdclose(reln, forkNum, blockNum);

    return (mdopen(reln, forkNum, EXTENSION_RETURN_NULL) != NULL);
}

static int RetryDataFileIdOpenFile(bool isRedo, char* path, const RelFileNodeForkNum &filenode, uint32 flags)
{
    int save_errno = errno;
    int fd = -1;
    /*
     * During bootstrap, there are cases where a system relation will be
     * accessed (by internal backend processes) before the bootstrap
     * script nominally creates it.  Therefore, allow the file to exist
     * already, even if isRedo is not set.  (See also mdopen)
     *
     * During inplace upgrade, the physical catalog files may be present
     * due to previous failure and rollback. Since the relfilenodes of these
     * new catalogs can by no means be used by other relations, we simply
     * truncate them.
     */
    if (isRedo || IsBootstrapProcessingMode() ||
        (u_sess->attr.attr_common.IsInplaceUpgrade && filenode.rnode.node.relNode < FirstNormalObjectId)) {
        ADIO_RUN()
        {
            flags = O_RDWR | PG_BINARY | O_DIRECT | (u_sess->attr.attr_common.IsInplaceUpgrade ? O_TRUNC : 0);
        }
        ADIO_ELSE()
        {
            flags = O_RDWR | PG_BINARY | (u_sess->attr.attr_common.IsInplaceUpgrade ? O_TRUNC : 0);
        }
        ADIO_END();

        fd = DataFileIdOpenFile(path, filenode, (int)flags, (int)FILE_RW_PERMISSION);
    }

    if (fd < 0) {
        /* be sure to report the error reported by create, not open */
        errno = save_errno;
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not create file \"%s\": %m", path)));
    }
    return fd;
}

/*
 *  mdcreate() -- Create a new relation on magnetic disk.
 *
 * If isRedo is true, it's okay for the relation to exist already.
 */
void mdcreate(SMgrRelation reln, ForkNumber forkNum, bool isRedo)
{
    char* path = NULL;
    File fd;
    RelFileNodeForkNum filenode;
    volatile uint32 flags = O_RDWR | O_CREAT | O_EXCL | PG_BINARY;

    if (isRedo && reln->md_fd[forkNum] != NULL)
        return; /* created and opened already... */

    Assert(reln->md_fd[forkNum] == NULL);

    path = relpath(reln->smgr_rnode, forkNum);

    filenode = RelFileNodeForkNumFill(reln->smgr_rnode, forkNum, 0);

    ADIO_RUN()
    {
        flags |= O_DIRECT;
    }
    ADIO_END();

    char* openFilePath = path;
    char dst[MAXPGPATH];
    if (IS_COMPRESSED_MAINFORK(reln, forkNum)) {
        CopyCompressedPath(dst, path);
        openFilePath = dst;
    }

    if (isRedo && (AmStartupProcess() || AmPageRedoWorker() || AmPageWriterProcess() || AmCheckpointerProcess()) &&
        CheckFileRepairHashTbl(reln->smgr_rnode.node, forkNum, 0)) {
        fd = openrepairfile(openFilePath, filenode);
        if (fd >= 0) {
            ereport(LOG, (errmsg("[file repair] open repair file %s.repair", openFilePath)));
        }
    } else {
        fd = DataFileIdOpenFile(openFilePath, filenode, flags, 0600);
    }

    if (fd < 0) {
        int save_errno = errno;

        /*
         * During bootstrap, there are cases where a system relation will be
         * accessed (by internal backend processes) before the bootstrap
         * script nominally creates it.  Therefore, allow the file to exist
         * already, even if isRedo is not set.	(See also mdopen)
         *
         * During inplace upgrade, the physical catalog files may be present
         * due to previous failure and rollback. Since the relfilenodes of these
         * new catalogs can by no means be used by other relations, we simply
         * truncate them.
         */
        if (isRedo || IsBootstrapProcessingMode() ||
            (u_sess->attr.attr_common.IsInplaceUpgrade && filenode.rnode.node.relNode < FirstNormalObjectId)) {
            ADIO_RUN() {
                flags = O_RDWR | PG_BINARY | O_DIRECT | (u_sess->attr.attr_common.IsInplaceUpgrade ? O_TRUNC : 0);
            }
            ADIO_ELSE() {
                flags = O_RDWR | PG_BINARY | (u_sess->attr.attr_common.IsInplaceUpgrade ? O_TRUNC : 0);
            } ADIO_END();

            fd = DataFileIdOpenFile(openFilePath, filenode, flags, 0600);
        }

        if (fd < 0) {
            /* be sure to report the error reported by create, not open */
            errno = save_errno;
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not create file \"%s\": %m", openFilePath)));
        }
    }

    if (fd < 0) {
        fd = RetryDataFileIdOpenFile(isRedo, openFilePath, filenode, flags);
    }
    pfree(path);

    reln->md_fd[forkNum] = _fdvec_alloc();

    reln->md_fd[forkNum]->mdfd_vfd = fd;
    reln->md_fd[forkNum]->mdfd_segno = 0;
    reln->md_fd[forkNum]->mdfd_chain = NULL;
}

/*
 *  mdunlink() -- Unlink a relation.
 *
 * Note that we're passed a RelFileNodeBackend --- by the time this is called,
 * there won't be an SMgrRelation hashtable entry anymore.
 *
 * forkNum can be a fork number to delete a specific fork, or InvalidForkNumber
 * to delete all forks.
 *
 * For regular relations, we don't unlink the first segment file of the rel,
 * but just truncate it to zero length, and record a request to unlink it after
 * the next checkpoint.  Additional segments can be unlinked immediately,
 * however.  Leaving the empty file in place prevents that relfilenode
 * number from being reused.  The scenario this protects us from is:
 * 1. We delete a relation (and commit, and actually remove its file).
 * 2. We create a new relation, which by chance gets the same relfilenode as
 *    the just-deleted one (OIDs must've wrapped around for that to happen).
 * 3. We crash before another checkpoint occurs.
 * During replay, we would delete the file and then recreate it, which is fine
 * if the contents of the file were repopulated by subsequent WAL entries.
 * But if we didn't WAL-log insertions, but instead relied on fsyncing the
 * file after populating it (as for instance CLUSTER and CREATE INDEX do),
 * the contents of the file would be lost forever.	By leaving the empty file
 * until after the next checkpoint, we prevent reassignment of the relfilenode
 * number until it's safe, because relfilenode assignment skips over any
 * existing file.
 *
 * We do not need to go through this dance for temp relations, though, because
 * we never make WAL entries for temp rels, and so a temp rel poses no threat
 * to the health of a regular rel that has taken over its relfilenode number.
 * The fact that temp rels and regular rels have different file naming
 * patterns provides additional safety.
 *
 * All the above applies only to the relation's main fork; other forks can
 * just be removed immediately, since they are not needed to prevent the
 * relfilenode number from being recycled.	Also, we do not carefully
 * track whether other forks have been created or not, but just attempt to
 * unlink them unconditionally; so we should never complain about ENOENT.
 *
 * If isRedo is true, it's unsurprising for the relation to be already gone.
 * Also, we should remove the file immediately instead of queuing a request
 * for later, since during redo there's no possibility of creating a
 * conflicting relation.
 *
 * Note: any failure should be reported as WARNING not ERROR, because
 * we are usually not in a transaction anymore when this is called.
 */
void mdunlink(const RelFileNodeBackend& rnode, ForkNumber forkNum, bool isRedo, uint32 segno)
{
    Assert(segno == InvalidBlockNumber);

    /* Now do the per-fork work */
    if (forkNum == InvalidForkNumber) {
        int fork_num = (int)forkNum;
        for (fork_num = 0; fork_num <= (int)MAX_FORKNUM; fork_num++)
            mdunlinkfork(rnode, (ForkNumber)fork_num, isRedo);
    } else {
        mdunlinkfork(rnode, forkNum, isRedo);
    }
}

void set_max_segno_delrel(int max_segno, RelFileNode rnode, ForkNumber forknum)
{
    HTAB *relfilenode_hashtbl = g_instance.bgwriter_cxt.unlink_rel_hashtbl;
    HTAB* relfilenode_fork_hashtbl = g_instance.bgwriter_cxt.unlink_rel_fork_hashtbl;
    DelFileTag *entry = NULL;
    ForkRelFileNode entry_key;
    DelForkFileTag *fork_entry = NULL;
    bool found = false;

    LWLockAcquire(g_instance.bgwriter_cxt.rel_hashtbl_lock, LW_SHARED);
    entry = (DelFileTag*)hash_search(relfilenode_hashtbl, &(rnode), HASH_FIND, &found);
    if (found) {
        if (max_segno > entry->maxSegNo) {
            entry->maxSegNo = max_segno;
        }
    }
    LWLockRelease(g_instance.bgwriter_cxt.rel_hashtbl_lock);

    if (!found) {
        entry_key.rnode = rnode;
        entry_key.forkNum = forknum;
        LWLockAcquire(g_instance.bgwriter_cxt.rel_one_fork_hashtbl_lock, LW_SHARED);
        fork_entry = (DelForkFileTag*)hash_search(relfilenode_fork_hashtbl, &(entry_key), HASH_FIND, &found);
        if (found) {
            if (max_segno > fork_entry->maxSegNo) {
                fork_entry->maxSegNo = max_segno;
            }
        }
        LWLockRelease(g_instance.bgwriter_cxt.rel_one_fork_hashtbl_lock);
    }
    return;
}

static void mdcleanrepairfile(char *segpath)
{
    const int TEMPLEN = 8;
    struct stat statBuf;

    char *temppath = (char *)palloc(strlen(segpath) + TEMPLEN);
    errno_t rc = sprintf_s(temppath, strlen(segpath) + TEMPLEN, "%s.repair", segpath);
    securec_check_ss(rc, "", "");
    if (stat(temppath, &statBuf) >= 0) {
        (void)unlink(temppath);
        ereport(LOG, (errcode_for_file_access(),
            errmsg("remove repair file \"%s\"", temppath)));
    }
    pfree(temppath);
}

/*
 * Truncate a file to release disk space.
 */
static int
do_truncate(char *path)
{
    int save_errno;
    int ret;
    int fd;

    /* truncate(2) would be easier here, but Windows hasn't got it */
    fd = BasicOpenFile(path, O_RDWR | PG_BINARY, 0);
    if (fd >= 0) {
        ret = ftruncate(fd, 0);
        save_errno = errno;
        (void)close(fd);
        errno = save_errno;
    } else
        ret = -1;

    /* Log a warning here to avoid repetition in callers. */
    if (ret < 0 && !FILE_POSSIBLY_DELETED(errno)) {
        save_errno = errno;
        ereport(WARNING,
                (errcode_for_file_access(),
                 errmsg("could not truncate file \"%s\": %m", path)));
        errno = save_errno;
    }

    return ret;
}

static void mdunlinkfork(const RelFileNodeBackend& rnode, ForkNumber forkNum, bool isRedo)
{
    char* path = NULL;
    int ret;

    path = relpath(rnode, forkNum);

    char* openFilePath = path;
    char dst[MAXPGPATH] = {0};
    if (IS_COMPRESSED_RNODE(rnode.node, forkNum)) {
        CopyCompressedPath(dst, path);
        openFilePath = dst;
    }

    /*
     * Delete or truncate the first segment.
     */
    Assert(IsHeapFileNode(rnode.node));
    if (isRedo || u_sess->attr.attr_common.IsInplaceUpgrade || forkNum != MAIN_FORKNUM ||
        RelFileNodeBackendIsTemp(rnode) || ENABLE_DMS) {
        if (!RelFileNodeBackendIsTemp(rnode)) {
            /* Prevent other backends' fds from holding on to the disk space */
            (void)do_truncate(openFilePath);

            /* Forget any pending sync requests for the first segment */
            md_register_forget_request(rnode.node, forkNum, 0 /* first segment */);
        }

        /* Next unlink the file, unless it was already found to be missing */
        ret = unlink(openFilePath);
        if (ret < 0 && !FILE_POSSIBLY_DELETED(errno)) {
            ereport(WARNING, (errcode_for_file_access(), errmsg("could not remove file \"%s\": ", openFilePath)));
        }
        if (isRedo) {
            mdcleanrepairfile(openFilePath);
        }
    } else {
        /* Prevent other backends' fds from holding on to the disk space */
        ret = do_truncate(openFilePath);

        /* Register request to unlink first segment later */
        register_unlink_segment(rnode, forkNum, 0);
    }

    /*
     * Delete any additional segments.
     */
    if (ret >= 0) {
        char *segpath = (char *)palloc(strlen(path) + 12 + strlen(COMPRESS_STR));
        BlockNumber segno;
        BlockNumber nSegments;
        errno_t rc = EOK;

        /*
         * Because mdsyncfiletag() uses _mdfd_getseg() to open segment files,
         * we have to send SYNC_FORGET_REQUEST for all segments before we
         * begin unlinking any of them.  Since it opens segments other than
         * the one you asked for, it might try to open a file that we've
         * already unlinked if we don't do this first.
         */
        for (segno = 1;; segno++) {
            struct stat statBuf;

            rc = sprintf_s(segpath, strlen(path) + 12 + strlen(COMPRESS_STR),
                           IS_COMPRESSED_RNODE(rnode.node, forkNum) ? "%s.%u" COMPRESS_STR : "%s.%u", path, segno);
            securec_check_ss(rc, "", "");

            if (stat(segpath, &statBuf) < 0) {
                /* ENOENT is expected after the last segment... */
                if (errno != ENOENT) {
                    ereport(WARNING, (errcode_for_file_access(),
                        errmsg("could not stat file \"%s\" before removing: %m", segpath)));
                }
                break;
            }
            if (!RelFileNodeBackendIsTemp(rnode)) {
                /*
                 * Prevent other backends' fds from holding on to the disk
                 * space.
                 */
                if (do_truncate(segpath) < 0 && errno == ENOENT)
                    break;
                
                /*
                 * Forget any pending sync requests for this segment before we
                 * try to unlink.
                 */
                md_register_forget_request(rnode.node, forkNum, segno);
            }
        }

        nSegments = segno;
        set_max_segno_delrel(nSegments, rnode.node, forkNum);

        /*
         * Now that we've canceled requests for all segments up to nsegments,
         * it is safe to remove the files.
         */
        for (segno = 1; segno < nSegments; segno++) {
            rc = sprintf_s(segpath, strlen(path) + 12 + strlen(COMPRESS_STR),
                           IS_COMPRESSED_RNODE(rnode.node, forkNum) ? "%s.%u" COMPRESS_STR :"%s.%u", path, segno);
            securec_check_ss(rc, "", "");
            if (unlink(segpath) < 0) {
                ereport(WARNING, (errcode_for_file_access(),
                    errmsg("could not remove file \"%s\": %m", openFilePath)));
            }
            /* try clean the repair file if exists */
            if (isRedo) {
                mdcleanrepairfile(segpath);
            }
        }
        pfree(segpath);
    }

    pfree(path);
}

/*
 *  mdextend() -- Add a block to the specified relation.
 *
 *      The semantics are nearly the same as mdwrite(): write at the
 *      specified position.  However, this is to be used for the case of
 *      extending a relation (i.e., blocknum is at or beyond the current
 *      EOF).  Note that we assume writing a block beyond current EOF
 *      causes intervening file space to become filled with zeroes.
 */
void mdextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
              char *buffer, bool skipFsync)
{
    off_t seekpos;
    int nbytes;
    MdfdVec *v = NULL;

    /* This assert is too expensive to have on normally ... */
#ifdef CHECK_WRITE_VS_EXTEND
    Assert(blocknum >= mdnblocks(reln, forknum));
#endif

    /*
     * If a relation manages to grow to 2^32-1 blocks, refuse to extend it any
     * more --- we mustn't create a block whose number actually is
     * InvalidBlockNumber.
     */
    if (blocknum == InvalidBlockNumber) {
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                        errmsg("cannot extend file \"%s\" beyond %u blocks", relpath(reln->smgr_rnode, forknum),
                               InvalidBlockNumber)));
    }

    v = _mdfd_getseg(reln, forknum, blocknum, skipFsync, EXTENSION_CREATE);
    if (v == NULL) {
        return;
    }

    if (unlikely((IS_COMPRESSED_MAINFORK(reln, forknum)))) {
        CfsExtendExtent(reln, forknum, blocknum, buffer, COMMON_STORAGE);
    } else {
        seekpos = (off_t)BLCKSZ * (blocknum % ((BlockNumber)RELSEG_SIZE));

        /*
        * Note: because caller usually obtained blocknum by calling mdnblocks,
        * which did a seek(SEEK_END), this seek is often redundant and will be
        * optimized away by fd.c.	It's not redundant, however, if there is a
        * partial page at the end of the file. In that case we want to try to
        * overwrite the partial page with a full page.  It's also not redundant
        * if bufmgr.c had to dump another buffer of the same file to make room
        * for the new page's buffer.
        */
        if ((nbytes = FilePWrite(v->mdfd_vfd, buffer, BLCKSZ, seekpos,
                                 (uint32)WAIT_EVENT_DATA_FILE_EXTEND)) != BLCKSZ) {
            if (check_unlink_rel_hashtbl(reln->smgr_rnode.node, forknum)) {
                ereport(DEBUG1,
                        (errmsg("could not extend file \"%s\": %m, this relation has been removed",
                        FilePathName(v->mdfd_vfd))));
            } else {
                if (nbytes < 0) {
                    ereport(ERROR,
                            (errcode_for_file_access(), errmsg("could not extend file \"%s\": %m", FilePathName(v->mdfd_vfd)),
                            errhint("Check free disk space.")));
                }
                /* short write: complain appropriately */
                ereport(ERROR, (errcode(ERRCODE_DISK_FULL),
                            errmsg("could not extend file \"%s\": wrote only %d of %d bytes at block %u",
                                FilePathName(v->mdfd_vfd), nbytes, BLCKSZ, blocknum),
                            errhint("Check free disk space.")));
            }
        }
    }
    if (!skipFsync && !SmgrIsTemp(reln)) {
        register_dirty_segment(reln, forknum, v);
    }
    Assert(_mdnblocks(reln, forknum, v) <= ((BlockNumber)RELSEG_SIZE));
}

static File mdopenagain(SMgrRelation reln, ForkNumber forknum, ExtensionBehavior behavior, char *path)
{
    uint32 flags = O_RDWR | PG_BINARY;
    File fd = -1;
    RelFileNodeForkNum filenode = RelFileNodeForkNumFill(reln->smgr_rnode, forknum, 0);

    ADIO_RUN()
    {
        flags |= O_DIRECT;
    }
    ADIO_END();

    /*
     * During bootstrap, there are cases where a system relation will be
     * accessed (by internal backend processes) before the bootstrap
     * script nominally creates it.  Therefore, accept mdopen() as a
     * substitute for mdcreate() in bootstrap mode only. (See mdcreate)
     */
    if (IsBootstrapProcessingMode()) {
        flags |= (O_CREAT | O_EXCL);
        fd = DataFileIdOpenFile(path, filenode, (int)flags, 0600);
    }

    if (fd < 0) {
        if (behavior == EXTENSION_RETURN_NULL && FILE_POSSIBLY_DELETED(errno)) {
            return fd;
        }
        if (check_unlink_rel_hashtbl(reln->smgr_rnode.node, forknum)) {
            ereport(DEBUG1, (errmsg("\"%s\": %m, this relation has been removed", path)));
            return fd;
        }
        if ((AmStartupProcess() || AmPageRedoWorker() || AmPageWriterProcess() || AmCheckpointerProcess()) &&
            CheckFileRepairHashTbl(reln->smgr_rnode.node, forknum, 0)) {
            fd = openrepairfile(path, filenode);
            if (fd < 0) {
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not open file %s.repair: %s", path, TRANSLATE_ERRNO)));
            } else {
                ereport(LOG, (errmsg("[file repair] open repair file %s.repair", path)));
            }
        } else {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not open file \"%s\": %m", path)));
        }
    }

    return fd;
}

/*
 *  mdopen() -- Open the specified relation.
 *
 * Note we only open the first segment, when there are multiple segments.
 *
 * If first segment is not present, either ereport or return NULL according
 * to "behavior".  We treat EXTENSION_CREATE the same as EXTENSION_FAIL;
 * EXTENSION_CREATE means it's OK to extend an existing relation, not to
 * invent one out of whole cloth.
 */
static MdfdVec *mdopen(SMgrRelation reln, ForkNumber forknum, ExtensionBehavior behavior)
{
    MdfdVec *mdfd = NULL;
    char *path = NULL;
    File fd;
    RelFileNodeForkNum filenode;
    uint32 flags = O_RDWR | PG_BINARY;

    /* No work if already open */
    if (reln->md_fd[forknum]) {
        return reln->md_fd[forknum];
    }

    path = relpath(reln->smgr_rnode, forknum);

    filenode = RelFileNodeForkNumFill(reln->smgr_rnode, forknum, 0);

    ADIO_RUN()
    {
        flags |= O_DIRECT;
    }
    ADIO_END();

    char* openFilePath = path;
    char dst[MAXPGPATH];
    if (IS_COMPRESSED_MAINFORK(reln, forknum)) {
        CopyCompressedPath(dst, path);
        openFilePath = dst;
    }

    fd = DataFileIdOpenFile(openFilePath, filenode, (int)flags, 0600);
    if (fd < 0) {
        fd = mdopenagain(reln, forknum, behavior, openFilePath);
        if (fd < 0) {
            pfree(path);
            return NULL;
        }
    }

    pfree(path);

    reln->md_fd[forknum] = mdfd = _fdvec_alloc();

    mdfd->mdfd_vfd = fd;
    mdfd->mdfd_segno = 0;
    mdfd->mdfd_chain = NULL;
    Assert(_mdnblocks(reln, forknum, mdfd) <= ((BlockNumber)RELSEG_SIZE));

    return mdfd;
}

/*
 *	mdclose() -- Close the specified relation, if it isn't closed already.
 */
void mdclose(SMgrRelation reln, ForkNumber forknum, BlockNumber blockNum)
{
    /* unused blocknum */
    MdfdVec *v = reln->md_fd[forknum];

    /* No work if already closed */
    if (v == NULL) {
        return;
    }

    reln->md_fd[forknum] = NULL; /* prevent dangling pointer after error */

    while (v != NULL) {
        MdfdVec *ov = v;

        /* if not closed already */
        if (v->mdfd_vfd >= 0) {
            FileClose(v->mdfd_vfd);
        }

        /* Now free vector */
        v = v->mdfd_chain;
        pfree(ov);
    }
}

/*
 *	mdprefetch() -- Initiate asynchronous read of the specified block of a relation
 */
void mdprefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
#ifdef USE_PREFETCH
    if (IS_COMPRESSED_MAINFORK(reln, forknum)) {
        CfsMdPrefetch(reln, forknum, blocknum, false, COMMON_STORAGE);
        return;
    }
    off_t seekpos;
    MdfdVec *v = NULL;

    v = _mdfd_getseg(reln, forknum, blocknum, false, EXTENSION_FAIL);
    if (v == NULL) {
        return;
    }

    seekpos = (off_t)BLCKSZ * (blocknum % ((BlockNumber)RELSEG_SIZE));

    Assert(seekpos < (off_t)BLCKSZ * RELSEG_SIZE);

    (void)FilePrefetch(v->mdfd_vfd, seekpos, BLCKSZ, WAIT_EVENT_DATA_FILE_PREFETCH);
#endif /* USE_PREFETCH */
}
/*
 * mdwriteback() -- Tell the kernel to write pages back to storage.
 *
 * This accepts a range of blocks because flushing several pages at once is
 * considerably more efficient than doing so individually.
 */
void mdwriteback(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, BlockNumber nblocks)
{
    if (IS_COMPRESSED_MAINFORK(reln, forknum)) {
        CfsWriteBack(reln, forknum, blocknum, nblocks, COMMON_STORAGE);
        return;
    }
    /*
     * Issue flush requests in as few requests as possible; have to split at
     * segment boundaries though, since those are actually separate files.
     */
    while (nblocks > 0) {
        BlockNumber nflush = nblocks;
        off_t seekpos;
        MdfdVec *v = NULL;
        unsigned int segnum_start, segnum_end;

        v = _mdfd_getseg(reln, forknum, blocknum, true /* not used */, EXTENSION_RETURN_NULL);
        /*
         * We might be flushing buffers of already removed relations, that's
         * ok, just ignore that case.
         */
        if (v == NULL) {
            return;
        }

        /* compute offset inside the current segment */
        segnum_start = blocknum / RELSEG_SIZE;

        /* compute number of desired writes within the current segment */
        segnum_end = (blocknum + nblocks - 1) / RELSEG_SIZE;
        if (segnum_start != segnum_end) {
            nflush = RELSEG_SIZE - (blocknum % ((BlockNumber)RELSEG_SIZE));
        }

        Assert(nflush >= 1);
        Assert(nflush <= nblocks);

        seekpos = (off_t)BLCKSZ * (blocknum % ((BlockNumber)RELSEG_SIZE));

        FileWriteback(v->mdfd_vfd, seekpos, (off_t)BLCKSZ * nflush);

        nblocks -= nflush;
        blocknum += nflush;
    }
}

/*
 * @Description: mdasyncread() -- Read the specified block from a relation.
 * On entry, io_in_progress_lock is held on all the buffers,
 * and they are pinned.  Once this function is done, it disowns
 * the buffers leaving the lock held and pin for the
 * ADIO completer in place.  Once the I/O is done the ADIO completer
 * function will release the locks and unpin the buffers.

 * @Param[IN] dList:aio desc list
 * @Param[IN] dn: aio desc list count
 * @Param[IN] forkNum: fork Num
 * @Param[IN] reln: relation
 * @See also:
 */
void mdasyncread(SMgrRelation reln, ForkNumber forkNum, AioDispatchDesc_t **dList, int32 dn)
{
#ifndef ENABLE_LITE_MODE
    if (IS_COMPRESSED_MAINFORK(reln, forkNum)) {
        return;
    }
    
    if (ENABLE_DSS) {
        return;
    }

    for (int i = 0; i < dn; i++) {
        off_t offset;
        MdfdVec *v = NULL;
        BlockNumber block_num;

        block_num = dList[i]->blockDesc.blockNum;

        /* Generate tracepoint
         *
         * mdasyncread: Need tracepoint for SMGR_MD_ASYNC_RD_START jeh
         *
         * Get the vfd and destination segment
         * and calculate the I/O offset into the segment.
         */
        v = _mdfd_getseg(dList[i]->blockDesc.smgrReln, dList[i]->blockDesc.forkNum, block_num, false, EXTENSION_FAIL);
        if (v == NULL) {
            return;
        }

        offset = (off_t)BLCKSZ * (block_num % ((BlockNumber)RELSEG_SIZE));

        /* Setup the I/O control block
         * The vfd "virtual fd" is passed here, FileAsyncRead
         * will translate it into an actual fd, to do the I/O.
         */
        io_prep_pread((struct iocb *)dList[i], v->mdfd_vfd, dList[i]->blockDesc.buffer,
                      (size_t)dList[i]->blockDesc.blockSize, offset);
        dList[i]->aiocb.aio_reqprio = CompltrPriority(dList[i]->blockDesc.reqType);

        START_CRIT_SECTION();
        /*
         * Disown the io_in_progress_lock, we will not be
         * waiting for the i/o to complete.
         */
        LWLockDisown(dList[i]->blockDesc.bufHdr->io_in_progress_lock);

        /* Pin the buffer on behalf of the ADIO Completer */
        AsyncCompltrPinBuffer((volatile void *)dList[i]->blockDesc.bufHdr);

        /* Unpin the buffer and drop the buffer ownership */
        AsyncUnpinBuffer((volatile void *)dList[i]->blockDesc.bufHdr, true);
        END_CRIT_SECTION();
    }

    /* Dispatch the I/O */
    (void)FileAsyncRead(dList, dn);
#endif
}

/*
 * @Description: Complete the async read from the ADIO completer thread.
 * @Param[IN] aioDesc: completed request from the dispatch list.
 * @Param[IN] res: the result of the read operation
 * @Return:should always succeed
 * @See also:
 */
int CompltrReadReq(void *aioDesc, long res)
{
    AioDispatchDesc_t *desc = (AioDispatchDesc_t *)aioDesc;

    START_CRIT_SECTION();
    Assert(desc->blockDesc.descType == AioRead);
    /* Take ownership of the io_in_progress_lock */
    LWLockOwn(desc->blockDesc.bufHdr->io_in_progress_lock);

    if (res != desc->blockDesc.blockSize) {
        /* io error */
        Assert(0);
        /* If there was an error handle the i/o accordingly */
        AsyncAbortBufferIO((void *)desc->blockDesc.bufHdr, true);
    } else {
        /* Make the buffer available again */
        AsyncTerminateBufferIO((void *)desc->blockDesc.bufHdr, false, BM_VALID);
    }

    /* Unpin the buffer on behalf of the ADIO Completer */
    AsyncCompltrUnpinBuffer((volatile void *)desc->blockDesc.bufHdr);

    END_CRIT_SECTION();

    /* Deallocate the AIO control block and I/O descriptor */
    adio_share_free(desc);

    return 0;
}

/*
 * @Description: Write the specified block in a relation.
 *  On entry, the io_in_progress_lock and the content_lock are held on all
 *  the buffers, and they are pinned.  Once this function is done, it
 *  disowns the locks and the buffers, leaving the locks held and a pin for the
 *  ADIO completer in place.  Once the I/O is done he ADIO completer function
 *  will release the locks and unpin the buffers.
 *
 * @Param[IN] dList:aio desc list
 * @Param[IN] dn: aio desc list count
 * @Param[IN] forkNum: fork Num
 * @Param[IN] reln: relation
 * @See also:
 */
void mdasyncwrite(SMgrRelation reln, ForkNumber forkNumber, AioDispatchDesc_t **dList, int32 dn)
{
#ifndef ENABLE_LITE_MODE
    if (IS_COMPRESSED_MAINFORK(reln, forkNumber)) {
        return;
    }

    if (ENABLE_DSS) {
        return;
    }

    for (int i = 0; i < dn; i++) {
        off_t offset;
        MdfdVec *v = NULL;
        SMgrRelation smgr_rel;
        ForkNumber fork_num;
        BlockNumber block_num;

        smgr_rel = dList[i]->blockDesc.smgrReln;
        fork_num = dList[i]->blockDesc.forkNum;
        block_num = dList[i]->blockDesc.blockNum;

        /* Generate tracepoint
         *
         * mdasyncwrite: Need tracepoint for SMGR_MD_ASYNC_WR_START jeh
         *
         * Get the vfd and destination segment
         * and calculate the I/O offset into the segment.
         */
        v = _mdfd_getseg(smgr_rel, fork_num, block_num, false, EXTENSION_FAIL);
        if (v == NULL) {
            return;
        }

        offset = (off_t)BLCKSZ * (block_num % ((BlockNumber)RELSEG_SIZE));

        off_t offset_true = FileSeek(v->mdfd_vfd, 0L, SEEK_END);
        if (offset > offset_true) {
            /* debug error */
            ereport(PANIC, (errmsg("md async write error,write offset(%ld), file size(%ld)", (int64)offset,
                                   (int64)offset_true)));
        }

        if (dList[i]->blockDesc.descType == AioWrite) {
            Assert(smgr_rel->smgr_rnode.node.spcNode == dList[i]->blockDesc.bufHdr->tag.rnode.spcNode);
            Assert(smgr_rel->smgr_rnode.node.dbNode == dList[i]->blockDesc.bufHdr->tag.rnode.dbNode);
            Assert(smgr_rel->smgr_rnode.node.relNode == dList[i]->blockDesc.bufHdr->tag.rnode.relNode);
        }

        /* Setup the I/O control block
         * The vfd "virtual fd" is passed here, FileAsyncRead
         * will translate it into an actual fd, to do the I/O.
         */
        io_prep_pwrite((struct iocb *)dList[i], v->mdfd_vfd, dList[i]->blockDesc.buffer,
                       (size_t)dList[i]->blockDesc.blockSize, offset);
        dList[i]->aiocb.aio_reqprio = CompltrPriority(dList[i]->blockDesc.reqType);

        START_CRIT_SECTION();
        if (dList[i]->blockDesc.descType == AioWrite) {
            /*
             * Disown the io_in_progress_lock and content_lock, we will not be
             * waiting for the i/o to complete.
             */
            LWLockDisown(dList[i]->blockDesc.bufHdr->io_in_progress_lock);
            LWLockDisown(dList[i]->blockDesc.bufHdr->content_lock);

            /* Pin the buffer on behalf of the ADIO Completer */
            AsyncCompltrPinBuffer((volatile void *)dList[i]->blockDesc.bufHdr);

            /* Unpin the buffer and drop the buffer ownership */
            AsyncUnpinBuffer((volatile void *)dList[i]->blockDesc.bufHdr, true);
        } else {
            Assert(dList[i]->blockDesc.descType == AioVacummFull);
            if (dList[i]->blockDesc.descType != AioVacummFull) {
                ereport(PANIC, (errmsg("md async write error")));
            }
        }
        END_CRIT_SECTION();
    }

    /* Dispatch the I/O */
    (void)FileAsyncWrite(dList, dn);
#endif
}

/*
 * @Description:  Complete the async write from the ADIO completer thread.
 * @Param[IN] aioDesc:  result of the read operation
 * @Param[IN] res: completed request from the dispatch list.
 * @Return: should always succeed
 * @See also:
 */
int CompltrWriteReq(void *aioDesc, long res)
{
    AioDispatchDesc_t *desc = (AioDispatchDesc_t *)aioDesc;

    START_CRIT_SECTION();
    if (desc->blockDesc.descType == AioWrite) {
        /* Take ownership of the content_lock and io_in_progress_lock */
        LWLockOwn(desc->blockDesc.bufHdr->content_lock);
        LWLockOwn(desc->blockDesc.bufHdr->io_in_progress_lock);

        if (res != desc->blockDesc.blockSize) {
            ereport(PANIC, (errmsg("async write failed, write_count(%ld), require_count(%d)", res,
                                   desc->blockDesc.blockSize)));
            /* If there was an error handle the i/o accordingly */
            AsyncAbortBufferIO((void *)desc->blockDesc.bufHdr, false);
        } else {
            /* Make the buffer available again */
            AsyncTerminateBufferIO((void *)desc->blockDesc.bufHdr, true, 0);
        }

        /* Release the content lock */
        LWLockRelease(desc->blockDesc.bufHdr->content_lock);

        /* Unpin the buffer and wake waiters, on behalf of the ADIO Completer */
        AsyncCompltrUnpinBuffer((volatile void *)desc->blockDesc.bufHdr);
    } else {
        Assert(desc->blockDesc.descType == AioVacummFull);

        if (res != desc->blockDesc.blockSize) {
            AsyncAbortBufferIOByVacuum((void *)desc->blockDesc.bufHdr);
            ereport(WARNING, (errmsg("vacuum full async write failed, write_count(%ld), require_count(%d)", res,
                                     desc->blockDesc.blockSize)));
        } else {
            AsyncTerminateBufferIOByVacuum((void *)desc->blockDesc.bufHdr);
        }
    }
    END_CRIT_SECTION();

    /* Deallocate the AIO control block and I/O descriptor */
    adio_share_free(desc);

    return 0;
}

const int FILE_NAME_LEN = 128;
static void check_file_stat(char *file_name)
{
    int rc;
    struct stat stat_buf;
    char file_path[MAX_PATH_LEN] = {0};
    char strfbuf[FILE_NAME_LEN];

    if (t_thrd.proc_cxt.DataDir == NULL || file_name == NULL || is_dss_file(file_name)) {
        return;
    }
    rc = snprintf_s(file_path, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/%s", t_thrd.proc_cxt.DataDir, file_name);
    securec_check_ss(rc, "", "");
    if (stat(file_path, &stat_buf) == 0) {
        pg_time_t stamp_time = (pg_time_t)stat_buf.st_mtime;
        if (log_timezone != NULL) {
            struct pg_tm *tm = pg_localtime(&stamp_time, log_timezone);
            if (tm != NULL) {
                (void)pg_strftime(strfbuf, sizeof(strfbuf), "%Y-%m-%d %H:%M:%S %Z", tm);
                ereport(LOG, (errmsg("file \"%s\" size is %ld bytes, last modify time is %s.", file_name,
                                     stat_buf.st_size, strfbuf)));
            } else {
                ereport(LOG, (errmsg("file \"%s\" size is %ld bytes.", file_name, stat_buf.st_size)));
            }
        } else {
            ereport(LOG, (errmsg("file \"%s\" size is %ld bytes.", file_name, stat_buf.st_size)));
        }
    } else {
        ereport(LOG, (errmsg("could not stat the file : \"%s\".", file_name)));
    }
}

#define CONTINUOUS_ASSIGN_2(a, b, value) do { \
    (a) = (value);                   \
    (b) = (value);                   \
} while (0)

#define CONTINUOUS_ASSIGN_3(a, b, c, value) do { \
    (a) = (value);                      \
    (b) = (value);                      \
    (c) = (value);                      \
} while (0)

/*
 *	mdread() -- Read the specified block from a relation.
 */
SMGR_READ_STATUS mdread(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, char *buffer)
{
    off_t seekpos;
    int nbytes;
    MdfdVec *v = NULL;

    instr_time startTime;
    instr_time endTime;
    PgStat_Counter timeDiff = 0;
    static THR_LOCAL PgStat_Counter msgCount = 0;
    static THR_LOCAL PgStat_Counter sumPage = 0;
    static THR_LOCAL PgStat_Counter sumTime = 0;
    static THR_LOCAL PgStat_Counter lstTime = 0;
    static THR_LOCAL PgStat_Counter minTime = 0;
    static THR_LOCAL PgStat_Counter maxTime = 0;
    static THR_LOCAL Oid lstFile = InvalidOid;
    static THR_LOCAL Oid lstDb = InvalidOid;
    static THR_LOCAL Oid lstSpc = InvalidOid;

    (void)INSTR_TIME_SET_CURRENT(startTime);

    TRACE_POSTGRESQL_SMGR_MD_READ_START(forknum, blocknum, reln->smgr_rnode.node.spcNode, reln->smgr_rnode.node.dbNode,
                                        reln->smgr_rnode.node.relNode, reln->smgr_rnode.backend);

    v = _mdfd_getseg(reln, forknum, blocknum, false, EXTENSION_FAIL);
    if (v == NULL) {
        return SMGR_RD_NO_BLOCK;
    }

    if (IS_COMPRESSED_MAINFORK(reln, forknum)) {
        nbytes = CfsReadPage(reln, forknum, blocknum, buffer, COMMON_STORAGE);
        if (nbytes < 0) {
            return SMGR_RD_CRC_ERROR;
        }
    } else {
        seekpos = (off_t)BLCKSZ * (blocknum % ((BlockNumber)RELSEG_SIZE));

        nbytes = FilePRead(v->mdfd_vfd, buffer, BLCKSZ, seekpos, WAIT_EVENT_DATA_FILE_READ);
    }
    TRACE_POSTGRESQL_SMGR_MD_READ_DONE(forknum, blocknum, reln->smgr_rnode.node.spcNode, reln->smgr_rnode.node.dbNode,
                                       reln->smgr_rnode.node.relNode, reln->smgr_rnode.backend, nbytes, BLCKSZ);

    (void)INSTR_TIME_SET_CURRENT(endTime);
    INSTR_TIME_SUBTRACT(endTime, startTime);
    timeDiff = INSTR_TIME_GET_MICROSEC(endTime);
    if (msgCount == 0) {
        lstFile = reln->smgr_rnode.node.relNode;
        lstDb = reln->smgr_rnode.node.dbNode;
        lstSpc = reln->smgr_rnode.node.spcNode;
        CONTINUOUS_ASSIGN_2(msgCount, sumPage, 1);
        CONTINUOUS_ASSIGN_3(sumTime, minTime, maxTime, timeDiff);
    } else if (msgCount % STAT_MSG_BATCH == 0 || lstFile != reln->smgr_rnode.node.relNode) {
        PgStat_MsgFile msg;
        errno_t rc;

        msg.dbid = lstDb;
        msg.spcid = lstSpc;
        msg.fn = lstFile;
        msg.rw = 'r';
        msg.cnt = msgCount;
        msg.blks = sumPage;
        msg.tim = sumTime;
        msg.lsttim = lstTime;
        msg.mintim = minTime;
        msg.maxtim = maxTime;
        reportFileStat(&msg);

        rc = memset_s(&msg, sizeof(PgStat_MsgFile), 0, sizeof(PgStat_MsgFile));
        securec_check(rc, "", "");

        CONTINUOUS_ASSIGN_2(msgCount, sumPage, 1);
        sumTime = timeDiff;
        if (lstFile != reln->smgr_rnode.node.relNode) {
            lstFile = reln->smgr_rnode.node.relNode;
            lstDb = reln->smgr_rnode.node.dbNode;
            lstSpc = reln->smgr_rnode.node.spcNode;
            CONTINUOUS_ASSIGN_3(sumTime, minTime, maxTime, timeDiff);
        }
    } else {
        msgCount++;
        sumPage++;
        sumTime += timeDiff;
    }
    lstTime = timeDiff;
    if (minTime > timeDiff) {
        minTime = timeDiff;
    }
    if (maxTime < timeDiff) {
        maxTime = timeDiff;
    }

    if (nbytes != BLCKSZ) {
        if (nbytes < 0) {
            ereport(ERROR, (errcode_for_file_access(),
                            errmsg("could not read block %u in file \"%s\": %m", blocknum, FilePathName(v->mdfd_vfd))));
        }
        /*
         * Short read: we are at or past EOF, or we read a partial block at
         * EOF.  Normally this is an error; upper levels should never try to
         * read a nonexistent block.  However, if zero_damaged_pages is ON or
         * we are InRecovery, we should instead return zeroes without
         * complaining.  This allows, for example, the case of trying to
         * update a block that was later truncated away.
         */
        if (u_sess->attr.attr_security.zero_damaged_pages || t_thrd.xlog_cxt.InRecovery) {
            MemSet(buffer, 0, BLCKSZ);
        } else {
            check_file_stat(FilePathName(v->mdfd_vfd));
            force_backtrace_messages = true;
            extreme_rto_standby_read::dump_error_all_info(reln->smgr_rnode.node, forknum, blocknum);
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                            errmsg("could not read block %u in file \"%s\": read only %d of %d bytes", blocknum,
                                   FilePathName(v->mdfd_vfd), nbytes, BLCKSZ)));
        }
    }
#ifndef ENABLE_MULTIPLE_NODES
    if (u_sess->attr.attr_common.enable_seqscan_fusion) {
        return SMGR_RD_OK;
    }
#endif

    if (PageIsVerified((Page) buffer, blocknum)) {
        return SMGR_RD_OK;
    } else {
        return SMGR_RD_CRC_ERROR;
    }
}

/*
 *  mdreadbatch() -- It will bulk read many pages;
 *  It will not return any error code, because it will check in page devided.
 */
void mdreadbatch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, int blockCount,char *buffer)
{
    off_t seekpos;
    int nbytes;
    MdfdVec *v = NULL;
    int amount = blockCount * BLCKSZ;
    v = _mdfd_getseg(reln, forknum, blocknum, false, EXTENSION_FAIL);

    seekpos = (off_t)BLCKSZ * (blocknum % ((BlockNumber)RELSEG_SIZE));

    Assert(seekpos < (off_t) BLCKSZ * RELSEG_SIZE);
    Assert(seekpos + (off_t) amount <= (off_t) BLCKSZ * RELSEG_SIZE);

    nbytes = FilePRead(v->mdfd_vfd, buffer, amount, seekpos, WAIT_EVENT_DATA_FILE_READ);

    if (nbytes != amount) {
        if (nbytes < 0) {
            ereport(ERROR, (errcode_for_file_access(),
                            errmsg("could not read block %u in file \"%s\": %m", blocknum, FilePathName(v->mdfd_vfd))));
        }
        /*
         * Short read: we are at or past EOF, or we read a partial block at
         * EOF.  Normally this is an error; upper levels should never try to
         * read a nonexistent block.  However, if zero_damaged_pages is ON or
         * we are InRecovery, we should instead return zeroes without
         * complaining.  This allows, for example, the case of trying to
         * update a block that was later truncated away.
         */
        if (u_sess->attr.attr_security.zero_damaged_pages || t_thrd.xlog_cxt.InRecovery) {
            int damaged_pages_start_offset = nbytes - nbytes % BLCKSZ;
            MemSet((char*)buffer + damaged_pages_start_offset, 0, amount - damaged_pages_start_offset);
        } else {
            check_file_stat(FilePathName(v->mdfd_vfd));
            force_backtrace_messages = true;
            extreme_rto_standby_read::dump_error_all_info(reln->smgr_rnode.node, forknum, blocknum);//standby节点
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                            errmsg("could not read block %u in file \"%s\": read only %d of %d bytes", blocknum,
                                   FilePathName(v->mdfd_vfd), nbytes, BLCKSZ)));
        }
    }
}

/*
 *  mdwrite() -- Write the supplied block at the appropriate location.
 *
 *      This is to be used only for updating already-existing blocks of a
 *      relation (ie, those before the current EOF).  To extend a relation,
 *      use mdextend().
 */
void mdwrite(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, const char *buffer, bool skipFsync)
{
    off_t seekpos;
    int nbytes;
    MdfdVec *v = NULL;

    PgStat_Counter time_diff = 0;
    static THR_LOCAL PgStat_Counter msg_count = 1;
    static THR_LOCAL PgStat_Counter sum_page = 0;
    static THR_LOCAL PgStat_Counter sum_time = 0;
    static THR_LOCAL PgStat_Counter lst_time = 0;
    static THR_LOCAL PgStat_Counter min_time = 0;
    static THR_LOCAL PgStat_Counter max_time = 0;
    static THR_LOCAL Oid lst_file = InvalidOid;
    static THR_LOCAL Oid lst_db = InvalidOid;
    static THR_LOCAL Oid lst_spc = InvalidOid;

#ifndef ENABLE_LITE_MODE
    instr_time start_time;
    (void)INSTR_TIME_SET_CURRENT(start_time);
#endif

    /* This assert is too expensive to have on normally ... */
#ifdef CHECK_WRITE_VS_EXTEND
    if (!check_unlink_rel_hashtbl(reln->smgr_rnode.node, forknum)) {
        Assert(blocknum < mdnblocks(reln, forknum));
    }
#endif

    TRACE_POSTGRESQL_SMGR_MD_WRITE_START(forknum, blocknum, reln->smgr_rnode.node.spcNode, reln->smgr_rnode.node.dbNode,
                                         reln->smgr_rnode.node.relNode, reln->smgr_rnode.backend);

    v = _mdfd_getseg(reln, forknum, blocknum, skipFsync, EXTENSION_FAIL);
    if (v == NULL) {
        return;
    }

    seekpos = (off_t)BLCKSZ * (blocknum % ((BlockNumber)RELSEG_SIZE));
    bool compressed = IS_COMPRESSED_MAINFORK(reln, forknum);
    if (compressed) {
        nbytes = (int)CfsWritePage(reln, forknum, blocknum, buffer, skipFsync, COMMON_STORAGE);
    } else {
        seekpos = (off_t)BLCKSZ * (blocknum % ((BlockNumber)RELSEG_SIZE));

        Assert(seekpos < (off_t)BLCKSZ * RELSEG_SIZE);

        nbytes = FilePWrite(v->mdfd_vfd, buffer, BLCKSZ, seekpos, (uint32)WAIT_EVENT_DATA_FILE_WRITE);
    }

    TRACE_POSTGRESQL_SMGR_MD_WRITE_DONE(forknum, blocknum, reln->smgr_rnode.node.spcNode, reln->smgr_rnode.node.dbNode,
                                        reln->smgr_rnode.node.relNode, reln->smgr_rnode.backend, nbytes, BLCKSZ);

#ifndef ENABLE_LITE_MODE
    instr_time end_time;
    (void)INSTR_TIME_SET_CURRENT(end_time);
    INSTR_TIME_SUBTRACT(end_time, start_time);
    time_diff = (PgStat_Counter)INSTR_TIME_GET_MICROSEC(end_time);
#endif

    if (msg_count == 0) {
        lst_file = reln->smgr_rnode.node.relNode;
        lst_db = reln->smgr_rnode.node.dbNode;
        lst_spc = reln->smgr_rnode.node.spcNode;
        CONTINUOUS_ASSIGN_2(msg_count, sum_page, 1);
        CONTINUOUS_ASSIGN_3(sum_time, min_time, max_time, time_diff);
    } else if (lst_file != reln->smgr_rnode.node.relNode || msg_count % STAT_MSG_BATCH) {
        PgStat_MsgFile msg;
        errno_t rc = memset_s(&msg, sizeof(msg), 0, sizeof(msg));
        securec_check(rc, "", "");

        msg.dbid = lst_db;
        msg.spcid = lst_spc;
        msg.fn = lst_file;
        msg.rw = 'w';
        msg.cnt = msg_count;
        msg.blks = sum_page;
        msg.tim = sum_time;
        msg.lsttim = lst_time;
        msg.mintim = min_time;
        msg.maxtim = max_time;
        reportFileStat(&msg);

        CONTINUOUS_ASSIGN_2(msg_count, sum_page, 1);
        sum_time = time_diff;
        if (lst_file != reln->smgr_rnode.node.relNode) {
            lst_file = reln->smgr_rnode.node.relNode;
            lst_db = reln->smgr_rnode.node.dbNode;
            lst_spc = reln->smgr_rnode.node.spcNode;
            CONTINUOUS_ASSIGN_3(sum_time, min_time, max_time, time_diff);
        }
    } else {
        msg_count++;
        sum_page++;
        sum_time += time_diff;
    }
    lst_time = time_diff;
    if (min_time > time_diff) {
        min_time = time_diff;
    }
    if (max_time < time_diff) {
        max_time = time_diff;
    }

    if (nbytes != BLCKSZ) {
        if (check_unlink_rel_hashtbl(reln->smgr_rnode.node, forknum)) {
            ereport(DEBUG1, (errmsg("could not write block %u in file \"%s\": %m, this relation has been removed",
                    blocknum, FilePathName(v->mdfd_vfd))));
            /* this file need skip sync */
            skipFsync = true;
        } else {
            if (nbytes < 0) {
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not write block %u in file \"%s\": %m", blocknum,
                                                                  FilePathName(v->mdfd_vfd))));
            }
            /* short write: complain appropriately */
            ereport(ERROR, (errcode(ERRCODE_DISK_FULL),
                        errmsg("could not write block %u in file \"%s\": wrote only %d of %d bytes", blocknum,
                               FilePathName(v->mdfd_vfd), nbytes, BLCKSZ),
                        errhint("Check free disk space.")));
        }
    }

    if (!skipFsync && !SmgrIsTemp(reln)) {
        register_dirty_segment(reln, forknum, v);
    }
}

/*
 *  mdnblocks() -- Get the number of blocks stored in a relation.
 *
 *      Important side effect: all active segments of the relation are opened
 *      and added to the mdfd_chain list.  If this routine has not been
 *      called, then only segments up to the last one actually touched
 *      are present in the chain.
 */
BlockNumber mdnblocks(SMgrRelation reln, ForkNumber forknum)
{
    MdfdVec *v = mdopen(reln, forknum, EXTENSION_FAIL);
    BlockNumber nblocks;
    BlockNumber segno = 0;

    if (v == NULL) {
        return 0;
    }
    /*
     * Skip through any segments that aren't the last one, to avoid redundant
     * seeks on them.  We have previously verified that these segments are
     * exactly RELSEG_SIZE long, and it's useless to recheck that each time.
     *
     * NOTE: this assumption could only be wrong if another backend has
     * truncated the relation.	We rely on higher code levels to handle that
     * scenario by closing and re-opening the md fd, which is handled via
     * relcache flush.	(Since the checkpointer doesn't participate in
     * relcache flush, it could have segment chain entries for inactive
     * segments; that's OK because the checkpointer never needs to compute
     * relation size.)
     */
    while (v->mdfd_chain != NULL) {
        segno++;
        v = v->mdfd_chain;
    }
    BlockNumber relSegSize = IS_COMPRESSED_MAINFORK(reln, forknum) ? CFS_LOGIC_BLOCKS_PER_FILE: RELSEG_SIZE;
    for (;;) {
        nblocks = _mdnblocks(reln, forknum, v);
        if (nblocks > relSegSize) {
            ereport(FATAL, (errmsg("segment too big")));
        }

        if (nblocks < relSegSize) {
            return (segno * relSegSize) + nblocks;
        }

        /*
         * If segment is exactly RELSEG_SIZE, advance to next one.
         */
        segno++;

        if (v->mdfd_chain == NULL) {
            /*
             * Because we pass O_CREAT, we will create the next segment (with
             * zero length) immediately, if the last segment is of length
             * RELSEG_SIZE.  While perhaps not strictly necessary, this keeps
             * the logic simple.
             */
            v->mdfd_chain = _mdfd_openseg(reln, forknum, segno, O_CREAT);
            if (v->mdfd_chain == NULL) {
                if (check_unlink_rel_hashtbl(reln->smgr_rnode.node, forknum)) {
                    ereport(DEBUG1, (errmsg("\"%s\": %m, this relation has been removed",
                                _mdfd_segpath(reln, forknum, segno))));
                    return 0;
                }
                ereport(ERROR, (errcode_for_file_access(),
                                errmsg("could not open file \"%s\": %m", _mdfd_segpath(reln, forknum, segno))));
            }
        }

        v = v->mdfd_chain;
    }
}


/*
 *	mdtruncate() -- Truncate relation to specified number of blocks.
 */
void mdtruncate(SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks)
{
    MdfdVec *v = NULL;
    BlockNumber curnblk;
    BlockNumber prior_blocks;

    /*
     * NOTE: mdnblocks makes sure we have opened all active segments, so that
     * truncation loop will get them all!
     */
    curnblk = mdnblocks(reln, forknum);
    if (curnblk == 0) {
         return;
    }
    if (nblocks > curnblk) {
        /* Bogus request ... but no complaint if InRecovery */
        if (t_thrd.xlog_cxt.InRecovery) {
            return;
        }
        ereport(ERROR, (errcode_for_file_access(),
                        errmsg("could not truncate file \"%s\" to %u blocks: it's only %u blocks now",
                               relpath(reln->smgr_rnode, forknum), nblocks, curnblk)));
    }
    if (nblocks == curnblk) {
        return; /* no work */
    }

    v = mdopen(reln, forknum, EXTENSION_FAIL);
    BlockNumber relSegSize = IS_COMPRESSED_MAINFORK(reln, forknum) ? CFS_LOGIC_BLOCKS_PER_FILE: RELSEG_SIZE;
    prior_blocks = 0;
    while (v != NULL) {
        MdfdVec *ov = v;
        if (ENABLE_DMS && !RelFileNodeBackendIsTemp(reln->smgr_rnode)) {
            md_register_forget_request(reln->smgr_rnode.node, forknum, 0 /* first segment */);
        }

        if (prior_blocks > nblocks) {
            /*
             * This segment is no longer active (and has already been unlinked
             * from the mdfd_chain). We truncate the file, but do not delete
             * it, for reasons explained in the header comments.
             */
            if (FileTruncate(v->mdfd_vfd, 0, WAIT_EVENT_DATA_FILE_TRUNCATE) < 0) {
                if (check_unlink_rel_hashtbl(reln->smgr_rnode.node, forknum)) {
                    ereport(DEBUG1, (errmsg("could not truncate file \"%s\": %m, this relation has been removed",
                                FilePathName(v->mdfd_vfd))));
                    FileClose(ov->mdfd_vfd);
                    pfree(ov);
                    break;
                }
                ereport(ERROR, (errcode_for_file_access(),
                                errmsg("could not truncate file \"%s\": %m", FilePathName(v->mdfd_vfd))));
            }

            if (!SmgrIsTemp(reln)) {
                register_dirty_segment(reln, forknum, v);
            }

            v = v->mdfd_chain;
            Assert(ov != reln->md_fd[forknum]); /* we never drop the 1st segment */
            FileClose(ov->mdfd_vfd);
            pfree(ov);
        } else if (prior_blocks + ((BlockNumber)relSegSize) > nblocks) {
            /*
             * This is the last segment we want to keep. Truncate the file to
             * the right length, and clear chain link that points to any
             * remaining segments (which we shall zap). NOTE: if nblocks is
             * exactly a multiple K of RELSEG_SIZE, we will truncate the K+1st
             * segment to 0 length but keep it. This adheres to the invariant
             * given in the header comments.
             */
            BlockNumber last_seg_blocks = nblocks - prior_blocks;

            auto truncateOffset = (off_t)last_seg_blocks * BLCKSZ;
            if (IS_COMPRESSED_MAINFORK(reln, forknum)) {
                truncateOffset = CfsMdTruncate(reln, forknum, nblocks, false, COMMON_STORAGE);
            }

            if (FileTruncate(v->mdfd_vfd, truncateOffset, WAIT_EVENT_DATA_FILE_TRUNCATE) < 0) {
                if (check_unlink_rel_hashtbl(reln->smgr_rnode.node, forknum)) {
                    ereport(DEBUG1, (errmsg("could not truncate file \"%s\": %m, this relation has been removed",
                                FilePathName(v->mdfd_vfd))));
                    break;
                }
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not truncate file \"%s\" to %u blocks: %m",
                                                                  FilePathName(v->mdfd_vfd), nblocks)));
            }

            if (!SmgrIsTemp(reln)) {
                register_dirty_segment(reln, forknum, v);
            }
            v = v->mdfd_chain;
            ov->mdfd_chain = NULL;
        } else {
            /*
             * We still need this segment and 0 or more blocks beyond it, so
             * nothing to do here.
             */
            v = v->mdfd_chain;
        }
        prior_blocks += relSegSize;
    }
}

/*
 *	mdimmedsync() -- Immediately sync a relation to stable storage.
 *
 * Note that only writes already issued are synced; this routine knows
 * nothing of dirty buffers that may exist inside the buffer manager.
 */
void mdimmedsync(SMgrRelation reln, ForkNumber forknum)
{
    MdfdVec *v = NULL;

    /*
     * NOTE: mdnblocks makes sure we have opened all active segments, so that
     * fsync loop will get them all!
     */
    (void)mdnblocks(reln, forknum);

    v = mdopen(reln, forknum, EXTENSION_FAIL);

    while (v != NULL) {
        if (FileSync(v->mdfd_vfd, WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC) < 0) {
            if (check_unlink_rel_hashtbl(reln->smgr_rnode.node, forknum)) {
                ereport(DEBUG1,
                    (errmsg("could not fsync file \"%s\": %m, this relation has been removed",
                    FilePathName(v->mdfd_vfd))));
                break;
            }
            ereport(data_sync_elevel(ERROR),
                    (errcode_for_file_access(), errmsg("could not fsync file \"%s\": %m", FilePathName(v->mdfd_vfd))));
        }
        v = v->mdfd_chain;
    }
}
/*
 * mdForgetDatabaseFsyncRequests -- forget any fsyncs and unlinks for a DB
 */
void mdForgetDatabaseFsyncRequests(Oid dbid)
{
    RelFileNode rnode;

    rnode.dbNode = dbid;
    rnode.spcNode = 0;
    rnode.relNode = 0;
    rnode.opt = 0;
    rnode.bucketNode = InvalidBktId;

    FileTag tag;
    INIT_MD_FILETAG(tag, rnode, InvalidForkNumber, InvalidBlockNumber);
    RegisterSyncRequest(&tag, SYNC_FILTER_REQUEST, true /* retry on error */);
}

/*
 *  _fdvec_alloc() -- Make a MdfdVec object.
 */
static MdfdVec *_fdvec_alloc(void)
{
    MemoryContext current;
    if (EnableLocalSysCache()) {
        current = t_thrd.lsc_cxt.lsc->lsc_mydb_memcxt;
    } else {
        current = u_sess->storage_cxt.MdCxt;
    }
    return (MdfdVec *)MemoryContextAlloc(current, sizeof(MdfdVec));
}

/* Get Path from RelFileNode */
char *mdsegpath(const RelFileNode &rnode, ForkNumber forknum, BlockNumber blkno)
{
    char *path = NULL;
    char *fullpath = NULL;
    BlockNumber segno;
    RelFileNodeBackend smgr_rnode;
    int nRet = 0;

    smgr_rnode.node = rnode;
    smgr_rnode.backend = InvalidBackendId;

    segno = blkno / ((BlockNumber)RELSEG_SIZE);

    path = relpath(smgr_rnode, forknum);

    if (segno > 0) {
        /* be sure we have enough space for the '.segno' */
        fullpath = (char *)palloc(strlen(path) + 12);
        nRet = snprintf_s(fullpath, strlen(path) + 12, strlen(path) + 11, "%s.%u", path, segno);
        securec_check_ss(nRet, "", "");
        pfree(path);
    } else {
        fullpath = path;
    }

    return fullpath;
}

/*
 * Return the filename for the specified segment of the relation. The
 * returned string is palloc'd.
 */
static char *_mdfd_segpath(const SMgrRelation reln, ForkNumber forknum, BlockNumber segno)
{
    char *path = NULL;
    char *fullpath = NULL;
    int nRet = 0;
    path = relpath(reln->smgr_rnode, forknum);

    if (segno > 0) {
        /* be sure we have enough space for the '.segno' */
        fullpath = (char *)palloc(strlen(path) + 12);
        nRet = snprintf_s(fullpath, strlen(path) + 12, strlen(path) + 11, "%s.%u", path, segno);
        securec_check_ss(nRet, "", "");
        pfree(path);
    } else {
        fullpath = path;
    }

    return fullpath;
}

/*
 * Open the specified segment of the relation,
 * and make a MdfdVec object for it.  Returns NULL on failure.
 */
static MdfdVec *_mdfd_openseg(SMgrRelation reln, ForkNumber forknum, BlockNumber segno, int oflags)
{
    MdfdVec *v = NULL;
    int fd;
    char *fullpath = NULL;
    RelFileNodeForkNum filenode;

    fullpath = _mdfd_segpath(reln, forknum, segno);

    filenode = RelFileNodeForkNumFill(reln->smgr_rnode, forknum, segno);

    ADIO_RUN()
    {
        oflags |= O_DIRECT;
    }
    ADIO_END();
    char* openFilePath = fullpath;
    char dst[MAXPGPATH];
    if (IS_COMPRESSED_MAINFORK(reln, forknum)) {
        CopyCompressedPath(dst, fullpath);
        openFilePath = dst;
    }

    /* open the file */
    if (RecoveryInProgress() && CheckFileRepairHashTbl(reln->smgr_rnode.node, forknum, segno) &&
        (AmStartupProcess() || AmPageRedoWorker() || AmPageWriterProcess() || AmCheckpointerProcess())) {
        fd = openrepairfile(openFilePath, filenode);
        if (fd < 0) {
            pfree(fullpath);
            return NULL;
        } else {
            ereport(LOG, (errmsg("[file repair] open repair file %s.repair", openFilePath)));
        }
    } else {
        fd = DataFileIdOpenFile(openFilePath, filenode, O_RDWR | PG_BINARY | oflags, 0600);
    }

    pfree(fullpath);

    if (fd < 0) {
        return NULL;
    }

    /* allocate an mdfdvec entry for it */
    v = _fdvec_alloc();

    /* fill the entry */
    v->mdfd_vfd = fd;
    v->mdfd_segno = segno;
    v->mdfd_chain = NULL;
    Assert(_mdnblocks(reln, forknum, v) <= ((BlockNumber)RELSEG_SIZE));

    /* all done */
    return v;
}

/*
 *  _mdfd_getseg() -- Find the segment of the relation holding the
 *      specified block.
 *
 * If the segment doesn't exist, we ereport, return NULL, or create the
 * segment, according to "behavior".  Note: skipFsync is only used in the
 * EXTENSION_CREATE case.
 */
MdfdVec *_mdfd_getseg(SMgrRelation reln, ForkNumber forknum, BlockNumber blkno, bool skipFsync,
                             ExtensionBehavior behavior)
{
    MdfdVec *v = mdopen(reln, forknum, behavior);
    BlockNumber targetseg;
    BlockNumber nextsegno;
    errno_t errorno = EOK;
    int elevel = ERROR;

    if (v == NULL) {
        return NULL; /* only possible if EXTENSION_RETURN_NULL */
    }
    BlockNumber relSegSize = IS_COMPRESSED_MAINFORK(reln, forknum) ? CFS_LOGIC_BLOCKS_PER_FILE: RELSEG_SIZE;
    targetseg = blkno / ((BlockNumber)relSegSize);
    for (nextsegno = 1; nextsegno <= targetseg; nextsegno++) {
        Assert(nextsegno == v->mdfd_segno + 1);

        if (v->mdfd_chain == NULL) {
            /*
             * Normally we will create new segments only if authorized by the
             * caller (i.e., we are doing mdextend()).	But when doing WAL
             * recovery, create segments anyway; this allows cases such as
             * replaying WAL data that has a write into a high-numbered
             * segment of a relation that was later deleted.  We want to go
             * ahead and create the segments so we can finish out the replay.
             *
             * We have to maintain the invariant that segments before the last
             * active segment are of size RELSEG_SIZE; therefore, pad them out
             * with zeroes if needed.  (This only matters if caller is
             * extending the relation discontiguously, but that can happen in
             * hash indexes.)
             */
            if (behavior == EXTENSION_CREATE || t_thrd.xlog_cxt.InRecovery) {
                if (_mdnblocks(reln, forknum, v) < relSegSize) {
                    char *zerobuf = NULL;
                    ADIO_RUN()
                    {
                        zerobuf = (char *)adio_align_alloc(BLCKSZ);
                        errorno = memset_s(zerobuf, BLCKSZ, 0, BLCKSZ);
                        securec_check_c(errorno, "", "");
                    }
                    ADIO_ELSE()
                    {
                        zerobuf = (char *)palloc0(BLCKSZ);
                    }
                    ADIO_END();

                    mdextend(reln, forknum, nextsegno * ((BlockNumber)relSegSize) - 1, zerobuf, skipFsync);

                    ADIO_RUN()
                    {
                        adio_align_free(zerobuf);
                    }
                    ADIO_ELSE()
                    {
                        pfree(zerobuf);
                    }
                    ADIO_END();
                }
                v->mdfd_chain = _mdfd_openseg(reln, forknum, +nextsegno, O_CREAT);
            } else {
                /* We won't create segment if not existent */
                v->mdfd_chain = _mdfd_openseg(reln, forknum, nextsegno, 0);
            }
            if (v->mdfd_chain == NULL) {
                if (behavior == EXTENSION_RETURN_NULL && FILE_POSSIBLY_DELETED(errno)) {
                    return NULL;
                }
                if (check_unlink_rel_hashtbl(reln->smgr_rnode.node, forknum)) {
                    ereport(DEBUG1, (errmsg("could not open file \"%s\" (target block %u): %m",
                                    _mdfd_segpath(reln, forknum, nextsegno), blkno)));
                    return NULL;
                }

                /*
                 * If bg writer open file failed because file is not exist,
                 * it will failed at next time, so we should PANIC to
                 * avoid repeated ERROR.
                 */
                if (FILE_POSSIBLY_DELETED(errno) && (IsBgwriterProcess() || IsPagewriterProcess())) {
                    elevel = PANIC;
                }
                ereport(elevel, (errcode_for_file_access(), errmsg("could not open file \"%s\" (target block %u): %m",
                                                                   _mdfd_segpath(reln, forknum, nextsegno), blkno)));
            }
        }
        v = v->mdfd_chain;
    }
    return v;
}

/*
 * Get number of blocks present in a single disk file
 */
static BlockNumber _mdnblocks(SMgrRelation reln, ForkNumber forknum, const MdfdVec *seg)
{
    off_t len;

    len = FileSeek(seg->mdfd_vfd, 0L, SEEK_END);
    if (len < 0) {
        if (check_unlink_rel_hashtbl(reln->smgr_rnode.node, forknum)) {
            ereport(DEBUG1, (errmsg("could not seek to end of file \"%s\": %m, this relation has been removed",
                    FilePathName(seg->mdfd_vfd))));
            return 0;
        }
        ereport(ERROR, (errcode_for_file_access(),
                        errmsg("could not seek to end of file \"%s\": %m", FilePathName(seg->mdfd_vfd))));
    }

    if (len == 0) {
        return (BlockNumber)len;
    }

    if (IS_COMPRESSED_MAINFORK(reln, forknum)) {
        return CfsNBlock(reln->smgr_rnode.node, seg->mdfd_vfd, seg->mdfd_segno, len);
    }

    /* note that this calculation will ignore any partial block at EOF */
    return (BlockNumber)(len / BLCKSZ);
}

/*
 * Sync a file to disk, given a file tag.  Write the path into an output
 * buffer so the caller can use it in error messages.
 *
 * Return 0 on success, -1 on failure, with errno set.
 */
int SyncMdFile(const FileTag *ftag, char *path)
{
    SMgrRelation reln = smgropen(ftag->rnode, InvalidBackendId, GetColumnNum(ftag->forknum));
    MdfdVec    *v;
    char       *p;
    File file;
    int result;
    int savedErrno;
    bool  needClose = false;

    /* Provide the path for informational messages. */
    p = _mdfd_segpath(reln, ftag->forknum, ftag->segno);
    strlcpy(path, p, MAXPGPATH);
    pfree(p);

    bool compressNode = IS_COMPRESSED_RNODE(ftag->rnode, ftag->forknum);
    char* openFilePath = path;
    char dst[MAXPGPATH];
    if (compressNode) {
        CopyCompressedPath(dst, path);
        openFilePath = dst;
    }
    BlockNumber relSegSize = compressNode ? CFS_LOGIC_BLOCKS_PER_FILE : RELSEG_SIZE;
    /* Try to open the requested segment. */
    v = _mdfd_getseg(reln, ftag->forknum, ftag->segno * (BlockNumber) relSegSize,
        false, EXTENSION_RETURN_NULL);
    if (v == NULL) {
        RelFileNodeForkNum filenode = RelFileNodeForkNumFill(reln->smgr_rnode,
            ftag->forknum, ftag->segno);
        uint32 flags = O_RDWR | PG_BINARY;
        file = DataFileIdOpenFile(openFilePath, filenode, (int)flags, S_IRUSR | S_IWUSR);
        if (file < 0 &&
            (AmStartupProcess() || AmPageRedoWorker() || AmPageWriterProcess() || AmCheckpointerProcess()) &&
            CheckFileRepairHashTbl(reln->smgr_rnode.node, ftag->forknum, ftag->segno)) {
            const int TEMPLEN = 8;
            char *temppath = (char *)palloc(strlen(openFilePath) + TEMPLEN);
            errno_t rc = sprintf_s(temppath, strlen(openFilePath) + TEMPLEN, "%s.repair", path);
            securec_check_ss(rc, "", "");
            file = DataFileIdOpenFile(temppath, filenode, (int)flags, S_IRUSR | S_IWUSR);
            if (file < 0) {
                pfree(temppath);
                return -1;
            }
            pfree(temppath);
        } else if (file < 0) {
            return -1;
        }
        needClose = true;
    } else {
        file = v->mdfd_vfd;
    }

    /* Try to fsync the file. */
    result = FileSync(file, WAIT_EVENT_DATA_FILE_SYNC);
    savedErrno = errno;
    if (needClose) {
        FileClose(file);
    }
    errno = savedErrno;
    return result;
}

/*
 * Unlink a file, given a file tag.  Write the path into an output
 * buffer so the caller can use it in error messages.
 *
 * Return 0 on success, -1 on failure, with errno set.
 */
int UnlinkMdFile(const FileTag *ftag, char *path)
{
    char *p;

    /* Compute the path. */
    p = relpathperm(ftag->rnode, MAIN_FORKNUM);
    strlcpy(path, p, MAXPGPATH);
    pfree(p);

    /* Try to unlink the file. */
    char* openFilePath = path;
    char dst[MAXPGPATH];
    if (IS_COMPRESSED_RNODE(ftag->rnode, MAIN_FORKNUM)) {
        CopyCompressedPath(dst, path);
        openFilePath = dst;
    }
    return unlink(openFilePath);
}

/*
 * Check if a given candidate request matches a given tag, when processing
 * a SYNC_FILTER_REQUEST request.  This will be called for all pending
 * requests to find out whether to forget them.
 */
bool MatchMdFileTag(const FileTag *ftag, const FileTag *candidate)
{
    /*
     * For now we only use filter requests as a way to drop all scheduled
     * callbacks relating to a given database, when dropping the database.
     * We'll return true for all candidates that have the same database OID as
     * the ftag from the SYNC_FILTER_REQUEST request, so they're forgotten.
     */
    return ftag->rnode.dbNode == candidate->rnode.dbNode;
}

ExtentLocation StorageConvert(SMgrRelation sRel, ForkNumber forknum, BlockNumber logicBlockNumber, bool skipSync,
                              int type)
{
    RelFileCompressOption option;
    TransCompressOptions(sRel->smgr_rnode.node, &option);

    BlockNumber extentNumber = logicBlockNumber / CFS_LOGIC_BLOCKS_PER_EXTENT;
    BlockNumber extentOffset = logicBlockNumber % CFS_LOGIC_BLOCKS_PER_EXTENT;
    BlockNumber extentStart = (extentNumber * CFS_EXTENT_SIZE) % CFS_MAX_BLOCK_PER_FILE;  // 0   129      129*2 129*3
    BlockNumber extentHeader = extentStart + CFS_LOGIC_BLOCKS_PER_EXTENT;  //              128 129+128  129*2+128
    MdfdVec *v = NULL;
    int fd = -1;
    if (type == EXTENT_OPEN_FILE) {
        v = _mdfd_getseg(sRel, forknum, logicBlockNumber, skipSync, EXTENSION_FAIL);
    } else if (type == WRITE_BACK_OPEN_FILE) {
        v = _mdfd_getseg(sRel, forknum, logicBlockNumber, skipSync, EXTENSION_RETURN_NULL);
    } else if (type == EXTENT_CREATE_FILE) {
        v = _mdfd_getseg(sRel, forknum, logicBlockNumber, skipSync, EXTENSION_CREATE);
    }
    if (v != NULL) {
        fd = v->mdfd_vfd;
    } else {
        RelFileNode node = sRel->smgr_rnode.node;
        ereport(ERROR, (errmsg("could not find valid location: [%u/%u/%u/%u]", node.spcNode, node.dbNode, node.relNode,
                               logicBlockNumber)));
    }
    return {.fd = fd,
            .relFileNode = sRel->smgr_rnode.node,
            .extentNumber = extentNumber,
            .extentStart = extentStart,
            .extentOffset = extentOffset,
            .headerNum = extentHeader,
            .chrunk_size = (uint16)CHUNK_SIZE_LIST[option.compressChunkSize],
            .algorithm = (uint8)option.compressAlgorithm
    };
}

MdfdVec *CfsMdOpenReln(SMgrRelation reln, ForkNumber forknum, ExtensionBehavior behavior)
{
    return mdopen(reln, forknum, behavior);
}

BlockNumber CfsGetBlocks(SMgrRelation reln, ForkNumber forknum, const MdfdVec *seg)
{
    return _mdnblocks(reln, forknum, seg);
}
