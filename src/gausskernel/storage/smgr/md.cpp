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
#include "catalog/catalog.h"
#include "portability/instr_time.h"
#include "postmaster/bgwriter.h"
#include "postmaster/pagewriter.h"
#include "storage/fd.h"
#include "storage/buf/bufmgr.h"
#include "storage/relfilenode.h"
#include "storage/copydir.h"
#include "storage/smgr.h"
#include "utils/aiomem.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "pg_trace.h"
#include "pgstat.h"

/* intervals for calling AbsorbFsyncRequests in mdsync and mdpostckpt */
#define FSYNCS_PER_ABSORB 10
#define UNLINKS_PER_ABSORB 10

/*
 * Special values for the segno arg to RememberFsyncRequest.
 *
 * Note that CompactCheckpointerRequestQueue assumes that it's OK to remove an
 * fsync request from the queue if an identical, subsequent request is found.
 * See comments there before making changes here.
 */
#define FORGET_RELATION_FSYNC (InvalidBlockNumber)
#define FORGET_DATABASE_FSYNC (InvalidBlockNumber - 1)
#define UNLINK_RELATION_REQUEST (InvalidBlockNumber - 2)
#define FORGET_BUCKETREL_REQUEST (InvalidBlockNumber - 3)

/*
 * On Windows, we have to interpret EACCES as possibly meaning the same as
 * ENOENT, because if a file is unlinked-but-not-yet-gone on that platform,
 * that's what you get.  Ugh.  This code is designed so that we don't
 * actually believe these cases are okay without further evidence (namely,
 * a pending fsync request getting canceled ... see mdsync).
 */
#ifndef WIN32
#define FILE_POSSIBLY_DELETED(err) ((err) == ENOENT)
#else
#define FILE_POSSIBLY_DELETED(err) ((err) == ENOENT || (err) == EACCES)
#endif

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
typedef struct _MdfdVec {
    File mdfd_vfd;               /* fd number in fd.c's pool */
    BlockNumber mdfd_segno;      /* segment number, from 0 */
    struct _MdfdVec *mdfd_chain; /* next segment, or NULL */
} MdfdVec;

/*
 * In some contexts (currently, standalone backends and the checkpointer)
 * we keep track of pending fsync operations: we need to remember all relation
 * segments that have been written since the last checkpoint, so that we can
 * fsync them down to disk before completing the next checkpoint.  This hash
 * table remembers the pending operations.	We use a hash table mostly as
 * a convenient way of merging duplicate requests.
 *
 * We use a similar mechanism to remember no-longer-needed files that can
 * be deleted after the next checkpoint, but we use a linked list instead of
 * a hash table, because we don't expect there to be any duplicate requests.
 *
 * These mechanisms are only used for non-temp relations; we never fsync
 * temp rels, nor do we need to postpone their deletion (see comments in
 * mdunlink).
 *
 * (Regular backends do not track pending operations locally, but forward
 * them to the checkpointer.)
 */
typedef uint16 CycleCtr; /* can be any convenient integer size */

typedef struct {
    RelFileNode rnode;  /* hash table key (must be first!) */
    CycleCtr cycle_ctr; /* mdsync_cycle_ctr of oldest request */

    int max_requests;
    /* requests[f] has bit n set if we need to fsync segment n of fork f */
    Bitmapset **requests;
    /* canceled[f] is true if we canceled fsyncs for fork "recently" */
    bool *canceled;
} PendingOperationEntry;

typedef struct {
    RelFileNode rnode;  /* the dead relation to delete */
    CycleCtr cycle_ctr; /* mdckpt_cycle_ctr when request was made */
} PendingUnlinkEntry;

typedef enum {                        /* behavior for mdopen & _mdfd_getseg */
               EXTENSION_FAIL,        /* ereport if segment not present */
               EXTENSION_RETURN_NULL, /* return NULL if not present */
               EXTENSION_CREATE       /* create new segments as needed */
} ExtensionBehavior;

/* local routines */
static void mdunlinkfork(const RelFileNodeBackend &rnode, ForkNumber forkNum, bool isRedo);
static MdfdVec *mdopen(SMgrRelation reln, ForkNumber forknum, ExtensionBehavior behavior);
static void register_dirty_segment(SMgrRelation reln, ForkNumber forknum, const MdfdVec *seg);
static void register_unlink(const RelFileNodeBackend &rnode);
static MdfdVec *_fdvec_alloc(void);
static char *_mdfd_segpath(const SMgrRelation reln, ForkNumber forknum, BlockNumber segno);
static MdfdVec *_mdfd_openseg(SMgrRelation reln, ForkNumber forkno, BlockNumber segno, int oflags);
static MdfdVec *_mdfd_getseg(SMgrRelation reln, ForkNumber forkno, BlockNumber blkno, bool skipFsync,
                             ExtensionBehavior behavior);
static BlockNumber _mdnblocks(SMgrRelation reln, ForkNumber forknum, const MdfdVec *seg);
static MdfdVec *_mdcreate(ForkNumber forkNum, bool isRedo, const RelFileNodeBackend &rnode);
static void _mdcreatebucket(SMgrRelation reln, ForkNumber forkNum, bool isRedo);

/*
 *	mdinit() -- Initialize private state for magnetic disk storage manager.
 */
void mdinit(void)
{
    u_sess->storage_cxt.MdCxt = AllocSetContextCreate(u_sess->top_mem_cxt, "MdSmgr", ALLOCSET_DEFAULT_MINSIZE,
                                                      ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

    /*
     * Create pending-operations hashtable if we need it.  Currently, we need
     * it if we are standalone (not under a postmaster) or if we are a startup
     * or checkpointer auxiliary process.
     */
    if (!IsUnderPostmaster || AmStartupProcess() || AmCheckpointerProcess()) {
        HASHCTL hash_ctl;
        errno_t rc;

        rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
        securec_check(rc, "", "");
        hash_ctl.keysize = sizeof(RelFileNode);
        hash_ctl.entrysize = sizeof(PendingOperationEntry);
        hash_ctl.hash = tag_hash;
        hash_ctl.hcxt = u_sess->storage_cxt.MdCxt;
        u_sess->storage_cxt.pendingOpsTable = hash_create("Pending Ops Table", 100L, &hash_ctl,
                                                          HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
        u_sess->storage_cxt.pendingUnlinks = NIL;
    }
}

/*
 * In archive recovery, we rely on checkpointer to do fsyncs, but we will have
 * already created the pendingOpsTable during initialization of the startup
 * process.  Calling this function drops the local pendingOpsTable so that
 * subsequent requests will be forwarded to checkpointer.
 */
void SetForwardFsyncRequests(void)
{
    /* Perform any pending fsyncs we may have queued up, then drop table */
    if (u_sess->storage_cxt.pendingOpsTable) {
        mdsync();
        hash_destroy(u_sess->storage_cxt.pendingOpsTable);
    }
    u_sess->storage_cxt.pendingOpsTable = NULL;

    /*
     * We should not have any pending unlink requests, since mdunlink doesn't
     * queue unlink requests when isRedo.
     */
    Assert(u_sess->storage_cxt.pendingUnlinks == NIL);
}

/*
 * mdexists() -- Does the physical file exist?
 *      Currently don't check the bucket dir exists.
 *
 * Note: this will return true for lingering files, with pending deletions
 */
bool mdexists(SMgrRelation reln, ForkNumber forkNum)
{
    /*
     * Close it first, to ensure that we notice if the fork has been unlinked
     * since we opened it.
     */
    Assert(reln->smgr_rnode.node.bucketNode != DIR_BUCKET_ID);
    mdclose(reln, forkNum);

    return (mdopen(reln, forkNum, EXTENSION_RETURN_NULL) != NULL);
}

static void _mdcreate_dir(char *file_name, bool isRedo)
{
    struct stat path_st;
    if (stat(file_name, &path_st) < 0) {
        if (errno != ENOENT) {
            ereport(ERROR,
                    (errcode_for_file_access(), errmsg("could not stat bucket directory \"%s\": %m", file_name)));
        } else {
            if (mkdir(file_name, S_IRWXU) < 0 && errno != EEXIST && isRedo == false) {
                ereport(ERROR,
                        (errcode_for_file_access(), errmsg("could not create bucket directory \"%s\": %m", file_name)));
            }
            fsync_fname(file_name, true);
            ereport(INFO, (errmsg("Create bucket dir \"%s\"", file_name)));
        }
    } else if (!S_ISDIR(path_st.st_mode)) {
        ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("\"%s\" exists but is not a bucket dir", file_name)));
    } else if (!isRedo) {
        ereport(ERROR, (errmsg("Bucket dir \"%s\" exists when creating bucket dir under not-redo", file_name)));
    }
}

/*
 *  mdcreate() -- Create a new relation on magnetic disk.
 *
 * If isRedo is true, it's okay for the relation to exist already.
 */
void mdcreate(SMgrRelation reln, ForkNumber forkNum, bool isRedo)
{
    if (reln->smgr_rnode.node.bucketNode != InvalidBktId) {
        return _mdcreatebucket(reln, forkNum, isRedo);
    }
    if (isRedo && reln->md_fd[forkNum] != NULL)
        return; /* created and opened already... */

    Assert(reln->md_fd[forkNum] == NULL);
    reln->md_fd[forkNum] = _mdcreate(forkNum, isRedo, reln->smgr_rnode);
}
/*
 *	mdcreate() -- Create a new relation on magnetic disk.
 *
 * If isRedo is true, it's okay for the relation to exist already.
 */
static void _mdcreatebucket(SMgrRelation reln, ForkNumber forkNum, bool isRedo)
{
    if (reln->smgr_rnode.node.bucketNode == DIR_BUCKET_ID) {
        Assert(reln->bucketnodes_smgrhash != NULL);
        char *path = NULL;
        path = relpath(reln->smgr_rnode, forkNum);
        _mdcreate_dir(path, isRedo);
        pfree(path);
        return;
    }

    Assert(reln->bucketnodes_smgrhash == NULL);
    if (isRedo && reln->md_fd[forkNum] != NULL) {
        return; /* created and opened already... */
    }

    Assert(reln->md_fd[forkNum] == NULL);
    reln->md_fd[forkNum] = _mdcreate(forkNum, isRedo, reln->smgr_rnode);
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
void mdunlink(const RelFileNodeBackend &rnode, ForkNumber forkNum, bool isRedo)
{
    /*
     * We have to clean out any pending fsync requests for the doomed
     * relation, else the next mdsync() will fail.	There can't be any such
     * requests for a temp relation, though.  We can send just one request
     * even when deleting multiple forks, since the fsync queuing code accepts
     * the "InvalidForkNumber = all forks" convention.
     */
    if (!RelFileNodeBackendIsTemp(rnode)) {
        ForgetRelationFsyncRequests(rnode.node, forkNum);
    }

    /* Now do the per-fork work */
    if (forkNum == InvalidForkNumber) {
        int fork_num = (int)forkNum;
        for (fork_num = 0; fork_num <= (int)MAX_FORKNUM; fork_num++)
            mdunlinkfork(rnode, (ForkNumber)fork_num, isRedo);
    } else {
        mdunlinkfork(rnode, forkNum, isRedo);
    }
}

static void _mdunlinkfork_bucket_dir(const RelFileNodeBackend &rnode, bool isRedo)
{
    char *path = relpath(rnode, MAIN_FORKNUM);
    if (isRedo || u_sess->attr.attr_common.IsInplaceUpgrade) {  // Remove bucker dir and files under bucker dir
        if (!rmtree(path, true, true)) {
            ereport(WARNING, (errcode_for_file_access(), errmsg("could not remove bucket dir \"%s\": %m", path)));
        } else {
            ereport(LOG, (errmsg("remove both bucket files and dir \"%s\": %m", path)));
        }
    } else {  // Remove files under bucket dir, but keep dir stay
        if (!rmtree(path, false, true)) {
            ereport(WARNING, (errcode_for_file_access(), errmsg("could not remove bucket dir \"%s\": %m", path)));
        } else {
            ereport(LOG, (errmsg("Only remove bucket files but leave dir \"%s\": %m", path)));
        }
        /* Register request to unlink first segment later */
        register_unlink(rnode);
    }

    pfree(path);
}

static void mdunlinkfork(const RelFileNodeBackend &rnode, ForkNumber forkNum, bool isRedo)
{
    if (BUCKET_ID_IS_DIR(rnode.node.bucketNode)) {
        _mdunlinkfork_bucket_dir(rnode, isRedo);
        return;
    }

    char *path = NULL;
    int ret;

    path = relpath(rnode, forkNum);

    /*
     * Delete or truncate the first segment.
     */
    if (isRedo || u_sess->attr.attr_common.IsInplaceUpgrade || forkNum != MAIN_FORKNUM ||
        RelFileNodeBackendIsTemp(rnode) || rnode.node.bucketNode != InvalidBktId) {
        ret = unlink(path);
        if (ret < 0 && errno != ENOENT)
            ereport(WARNING, (errcode_for_file_access(), errmsg("could not remove file \"%s\": %m", path)));
    } else {
        /* truncate(2) would be easier here, but Windows hasn't got it */
        int fd;

        fd = BasicOpenFile(path, O_RDWR | PG_BINARY, 0);
        if (fd >= 0) {
            int save_errno;

            ret = ftruncate(fd, 0);
            save_errno = errno;
            (void)close(fd);
            errno = save_errno;
        } else {
            ret = -1;
        }
        if (ret < 0 && errno != ENOENT) {
            ereport(WARNING, (errcode_for_file_access(), errmsg("could not truncate file \"%s\": %m", path)));
        }

        /* Register request to unlink first segment later */
        register_unlink(rnode);
    }

    /*
     * Delete any additional segments.
     */
    if (ret >= 0) {
        char *segpath = (char *)palloc(strlen(path) + 12);
        BlockNumber segno;
        errno_t rc = EOK;

        /*
         * Note that because we loop until getting ENOENT, we will correctly
         * remove all inactive segments as well as active ones.
         */
        for (segno = 1;; segno++) {
            rc = sprintf_s(segpath, strlen(path) + 12, "%s.%u", path, segno);
            securec_check_ss(rc, "", "");
            if (unlink(segpath) < 0) {
                /* ENOENT is expected after the last segment... */
                if (errno != ENOENT) {
                    ereport(WARNING, (errcode_for_file_access(), errmsg("could not remove file \"%s\": %m", segpath)));
                }
                break;
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
void mdextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, const char *buffer, bool skipFsync)
{
    off_t seekpos;
    int nbytes;
    MdfdVec *v = NULL;

    /* This assert is too expensive to have on normally ... */
#ifdef CHECK_WRITE_VS_EXTEND
    Assert(blocknum >= mdnblocks(reln, forknum));
#endif

    Assert(reln->smgr_rnode.node.bucketNode != DIR_BUCKET_ID);

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
    if ((nbytes = FilePWrite(v->mdfd_vfd, buffer, BLCKSZ, seekpos, WAIT_EVENT_DATA_FILE_EXTEND)) != BLCKSZ) {
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

    if (!skipFsync && !SmgrIsTemp(reln)) {
        register_dirty_segment(reln, forknum, v);
    }
    Assert(_mdnblocks(reln, forknum, v) <= ((BlockNumber)RELSEG_SIZE));
}

/*
 *  mdopen() -- Open the specified relation.
 *
 * Note we only open the first segment, when there are multiple segments.
 *      And, bucket dir can only be created but not be opened.
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

    Assert(reln->smgr_rnode.node.bucketNode != DIR_BUCKET_ID);

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

    fd = DataFileIdOpenFile(path, filenode, (int)flags, 0600);
    if (fd < 0) {
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
                pfree(path);
                return NULL;
            }
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not open file \"%s\": %m", path)));
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
 *  mdclose() -- Close the specified relation, if it isn't closed already.
 *      Currently, we don't have closing bucket dir case.
 */
void mdclose(SMgrRelation reln, ForkNumber forknum)
{
    Assert(reln->smgr_rnode.node.bucketNode != DIR_BUCKET_ID);
	if (reln->md_fd == NULL) {
        return;
    }
    MdfdVec* v = reln->md_fd[forknum];

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
 *      Currently, don't prefetch a bucket dir.
 */
void mdprefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
#ifdef USE_PREFETCH
    off_t seekpos;
    MdfdVec *v = NULL;

    Assert(reln->smgr_rnode.node.bucketNode != DIR_BUCKET_ID);

    v = _mdfd_getseg(reln, forknum, blocknum, false, EXTENSION_FAIL);

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
    /*
     * Issue flush requests in as few requests as possible; have to split at
     * segment boundaries though, since those are actually separate files.
     */

    Assert(reln->smgr_rnode.node.bucketNode != DIR_BUCKET_ID);
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
    if (t_thrd.proc_cxt.DataDir == NULL || file_name == NULL) {
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
 *      Now, we don't read from a bucket dir smgr.
 */
void mdread(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, char *buffer)
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

    Assert(reln->smgr_rnode.node.bucketNode != DIR_BUCKET_ID);

    (void)INSTR_TIME_SET_CURRENT(startTime);

    TRACE_POSTGRESQL_SMGR_MD_READ_START(forknum, blocknum, reln->smgr_rnode.node.spcNode, reln->smgr_rnode.node.dbNode,
                                        reln->smgr_rnode.node.relNode, reln->smgr_rnode.backend);

    v = _mdfd_getseg(reln, forknum, blocknum, false, EXTENSION_FAIL);

    seekpos = (off_t)BLCKSZ * (blocknum % ((BlockNumber)RELSEG_SIZE));

    nbytes = FilePRead(v->mdfd_vfd, buffer, BLCKSZ, seekpos, WAIT_EVENT_DATA_FILE_READ);

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

            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                            errmsg("could not read block %u in file \"%s\": read only %d of %d bytes", blocknum,
                                   FilePathName(v->mdfd_vfd), nbytes, BLCKSZ)));
        }
    }
}

/*
 *  mdwrite() -- Write the supplied block at the appropriate location.
 *      Now, we don't write into a bucket dir relation.
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

    instr_time start_time;
    instr_time end_time;
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

    Assert(reln->smgr_rnode.node.bucketNode != DIR_BUCKET_ID);

    (void)INSTR_TIME_SET_CURRENT(start_time);

    /* This assert is too expensive to have on normally ... */
#ifdef CHECK_WRITE_VS_EXTEND
    Assert(blocknum < mdnblocks(reln, forknum));
#endif

    TRACE_POSTGRESQL_SMGR_MD_WRITE_START(forknum, blocknum, reln->smgr_rnode.node.spcNode, reln->smgr_rnode.node.dbNode,
                                         reln->smgr_rnode.node.relNode, reln->smgr_rnode.backend);

    v = _mdfd_getseg(reln, forknum, blocknum, skipFsync, EXTENSION_FAIL);

    seekpos = (off_t)BLCKSZ * (blocknum % ((BlockNumber)RELSEG_SIZE));

    Assert(seekpos < (off_t)BLCKSZ * RELSEG_SIZE);

    nbytes = FilePWrite(v->mdfd_vfd, buffer, BLCKSZ, seekpos, WAIT_EVENT_DATA_FILE_WRITE);

    TRACE_POSTGRESQL_SMGR_MD_WRITE_DONE(forknum, blocknum, reln->smgr_rnode.node.spcNode, reln->smgr_rnode.node.dbNode,
                                        reln->smgr_rnode.node.relNode, reln->smgr_rnode.backend, nbytes, BLCKSZ);

    (void)INSTR_TIME_SET_CURRENT(end_time);
    INSTR_TIME_SUBTRACT(end_time, start_time);
    time_diff = (PgStat_Counter)INSTR_TIME_GET_MICROSEC(end_time);
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

    for (;;) {
        nblocks = _mdnblocks(reln, forknum, v);
        if (nblocks > ((BlockNumber)RELSEG_SIZE)) {
            ereport(FATAL, (errmsg("segment too big")));
        }

        if (nblocks < ((BlockNumber)RELSEG_SIZE)) {
            return (segno * ((BlockNumber)RELSEG_SIZE)) + nblocks;
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
                ereport(ERROR, (errcode_for_file_access(),
                                errmsg("could not open file \"%s\": %m", _mdfd_segpath(reln, forknum, segno))));
            }
        }

        v = v->mdfd_chain;
    }
}

/*
 *	mdtruncate() -- Truncate relation to specified number of blocks.
 *      Now, we don't truncate a bucket dir directly.
 */
void mdtruncate(SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks)
{
    MdfdVec *v = NULL;
    BlockNumber curnblk;
    BlockNumber prior_blocks;

    Assert(reln->smgr_rnode.node.bucketNode != DIR_BUCKET_ID);

    /*
     * NOTE: mdnblocks makes sure we have opened all active segments, so that
     * truncation loop will get them all!
     */
    curnblk = mdnblocks(reln, forknum);
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

    prior_blocks = 0;
    while (v != NULL) {
        MdfdVec *ov = v;

        if (prior_blocks > nblocks) {
            /*
             * This segment is no longer active (and has already been unlinked
             * from the mdfd_chain). We truncate the file, but do not delete
             * it, for reasons explained in the header comments.
             */
            if (FileTruncate(v->mdfd_vfd, 0, WAIT_EVENT_DATA_FILE_TRUNCATE) < 0) {
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
        } else if (prior_blocks + ((BlockNumber)RELSEG_SIZE) > nblocks) {
            /*
             * This is the last segment we want to keep. Truncate the file to
             * the right length, and clear chain link that points to any
             * remaining segments (which we shall zap). NOTE: if nblocks is
             * exactly a multiple K of RELSEG_SIZE, we will truncate the K+1st
             * segment to 0 length but keep it. This adheres to the invariant
             * given in the header comments.
             */
            BlockNumber last_seg_blocks = nblocks - prior_blocks;

            if (FileTruncate(v->mdfd_vfd, (off_t)last_seg_blocks * BLCKSZ, WAIT_EVENT_DATA_FILE_TRUNCATE) < 0) {
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
        prior_blocks += RELSEG_SIZE;
    }
}

/*
 *  mdimmedsync() -- Immediately sync a relation to stable storage.
 *      Now, just sync a underlying physical file but not a bucket dir.
 *
 * Note that only writes already issued are synced; this routine knows
 * nothing of dirty buffers that may exist inside the buffer manager.
 */
void mdimmedsync(SMgrRelation reln, ForkNumber forknum)
{
    MdfdVec *v = NULL;

    Assert(reln->smgr_rnode.node.bucketNode != DIR_BUCKET_ID);

    /*
     * NOTE: mdnblocks makes sure we have opened all active segments, so that
     * fsync loop will get them all!
     */
    (void)mdnblocks(reln, forknum);

    v = mdopen(reln, forknum, EXTENSION_FAIL);

    while (v != NULL) {
        if (FileSync(v->mdfd_vfd, WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC) < 0) {
            ereport(data_sync_elevel(ERROR),
                    (errcode_for_file_access(), errmsg("could not fsync file \"%s\": %m", FilePathName(v->mdfd_vfd))));
        }
        v = v->mdfd_chain;
    }
}

/*
 *  mdsync() -- Sync previous writes to stable storage.
 */
void mdsync(void)
{
    HASH_SEQ_STATUS hstat;
    PendingOperationEntry *entry = NULL;
    int absorb_counter;

    /* Statistics on sync times */
    int processed = 0;
    instr_time sync_start, sync_end, sync_diff;
    uint64 elapsed;
    uint64 longest = 0;
    uint64 total_elapsed = 0;

    /*
     * This is only called during checkpoints, and checkpoints should only
     * occur in processes that have created a pendingOpsTable.
     */
    if (!u_sess->storage_cxt.pendingOpsTable) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("cannot sync without a pendingOpsTable")));
    }

    /*
     * If we are in the checkpointer, the sync had better include all fsync
     * requests that were queued by backends up to this point.	The tightest
     * race condition that could occur is that a buffer that must be written
     * and fsync'd for the checkpoint could have been dumped by a backend just
     * before it was visited by BufferSync().  We know the backend will have
     * queued an fsync request before clearing the buffer's dirtybit, so we
     * are safe as long as we do an Absorb after completing BufferSync().
     */
    AbsorbFsyncRequests();

    /*
     * To avoid excess fsync'ing (in the worst case, maybe a never-terminating
     * checkpoint), we want to ignore fsync requests that are entered into the
     * hashtable after this point --- they should be processed next time,
     * instead.  We use mdsync_cycle_ctr to tell old entries apart from new
     * ones: new ones will have cycle_ctr equal to the incremented value of
     * mdsync_cycle_ctr.
     *
     * In normal circumstances, all entries present in the table at this point
     * will have cycle_ctr exactly equal to the current (about to be old)
     * value of mdsync_cycle_ctr.  However, if we fail partway through the
     * fsync'ing loop, then older values of cycle_ctr might remain when we
     * come back here to try again.  Repeated checkpoint failures would
     * eventually wrap the counter around to the point where an old entry
     * might appear new, causing us to skip it, possibly allowing a checkpoint
     * to succeed that should not have.  To forestall wraparound, any time the
     * previous mdsync() failed to complete, run through the table and
     * forcibly set cycle_ctr = mdsync_cycle_ctr.
     *
     * Think not to merge this loop with the main loop, as the problem is
     * exactly that that loop may fail before having visited all the entries.
     * From a performance point of view it doesn't matter anyway, as this path
     * will never be taken in a system that's functioning normally.
     */
    if (u_sess->storage_cxt.mdsync_in_progress) {
        /* prior try failed, so update any stale cycle_ctr values */
        hash_seq_init(&hstat, u_sess->storage_cxt.pendingOpsTable);
        while ((entry = (PendingOperationEntry *)hash_seq_search(&hstat)) != NULL) {
            entry->cycle_ctr = u_sess->storage_cxt.mdsync_cycle_ctr;
        }
    }

    /* Advance counter so that new hashtable entries are distinguishable */
    u_sess->storage_cxt.mdsync_cycle_ctr++;

    /* Set flag to detect failure if we don't reach the end of the loop */
    u_sess->storage_cxt.mdsync_in_progress = true;

    /* Now scan the hashtable for fsync requests to process */
    absorb_counter = FSYNCS_PER_ABSORB;
    hash_seq_init(&hstat, u_sess->storage_cxt.pendingOpsTable);
    while ((entry = (PendingOperationEntry *)hash_seq_search(&hstat)) != NULL) {
        int forknum;

        /*
         * If the entry is new then don't process it this time; it might
         * contain multiple fsync-request bits, but they are all new.  Note
         * "continue" bypasses the hash-remove call at the bottom of the loop.
         */
        if (entry->cycle_ctr == u_sess->storage_cxt.mdsync_cycle_ctr) {
            continue;
        }

        /* Else assert we haven't missed it */
        Assert((CycleCtr)(entry->cycle_ctr + 1) == u_sess->storage_cxt.mdsync_cycle_ctr);

        /*
         * Scan over the forks and segments represented by the entry.
         *
         * The bitmap manipulations are slightly tricky, because we can call
         * AbsorbFsyncRequests() inside the loop and that could result in
         * bms_add_member() modifying and even re-palloc'ing the bitmapsets.
         * This is okay because we unlink each bitmapset from the hashtable
         * entry before scanning it.  That means that any incoming fsync
         * requests will be processed now if they reach the table before we
         * begin to scan their fork.
         */
        for (forknum = 0; forknum < (int)(entry->max_requests); forknum++) {
            Bitmapset *requests = entry->requests[forknum];
            int segno;

            entry->requests[forknum] = NULL;
            entry->canceled[forknum] = false;

            while ((segno = bms_first_member(requests)) >= 0) {
                int failures;

                /*
                 * If fsync is off then we don't have to bother opening the
                 * file at all.  (We delay checking until this point so that
                 * changing fsync on the fly behaves sensibly.)
                 */
                if (!u_sess->attr.attr_storage.enableFsync) {
                    continue;
                }

                /*
                 * If in checkpointer, we want to absorb pending requests
                 * every so often to prevent overflow of the fsync request
                 * queue.  It is unspecified whether newly-added entries will
                 * be visited by hash_seq_search, but we don't care since we
                 * don't need to process them anyway.
                 */
                if (--absorb_counter <= 0) {
                    AbsorbFsyncRequests();
                    absorb_counter = FSYNCS_PER_ABSORB;
                }

                /*
                 * The fsync table could contain requests to fsync segments
                 * that have been deleted (unlinked) by the time we get to
                 * them. Rather than just hoping an ENOENT (or EACCES on
                 * Windows) error can be ignored, what we do on error is
                 * absorb pending requests and then retry.	Since mdunlink()
                 * queues a "cancel" message before actually unlinking, the
                 * fsync request is guaranteed to be marked canceled after the
                 * absorb if it really was this case. DROP DATABASE likewise
                 * has to tell us to forget fsync requests before it starts
                 * deletions.
                 */
                for (failures = 0;; failures++) { /* loop exits at "break" */
                    SMgrRelation reln;
                    MdfdVec *seg = NULL;
                    char *path = NULL;
                    int save_errno;

                    /*
                     * Find or create an smgr hash entry for this relation.
                     * This may seem a bit unclean -- md calling smgr?	But
                     * it's really the best solution.  It ensures that the
                     * open file reference isn't permanently leaked if we get
                     * an error here. (You may say "but an unreferenced
                     * SMgrRelation is still a leak!" Not really, because the
                     * only case in which a checkpoint is done by a process
                     * that isn't about to shut down is in the checkpointer,
                     * and it will periodically do smgrcloseall(). This fact
                     * justifies our not closing the reln in the success path
                     * either, which is a good thing since in non-checkpointer
                     * cases we couldn't safely do that.)
                     */
                    reln = smgropen(entry->rnode, InvalidBackendId, GetColumnNum(forknum));

                    /* Attempt to open and fsync the target segment */
                    seg = _mdfd_getseg(reln, (ForkNumber)forknum, (BlockNumber)segno * (BlockNumber)RELSEG_SIZE, false,
                                       EXTENSION_RETURN_NULL);

                    INSTR_TIME_SET_CURRENT(sync_start);

                    if (seg != NULL && FileSync(seg->mdfd_vfd, WAIT_EVENT_DATA_FILE_SYNC) >= 0) {
                        /* Success; update statistics about sync timing */
                        INSTR_TIME_SET_CURRENT(sync_end);
                        sync_diff = sync_end;
                        INSTR_TIME_SUBTRACT(sync_diff, sync_start);
                        elapsed = INSTR_TIME_GET_MICROSEC(sync_diff);
                        if (elapsed > longest) {
                            longest = elapsed;
                        }
                        total_elapsed += elapsed;
                        processed++;
                        if (u_sess->attr.attr_common.log_checkpoints) {
                            ereport(DEBUG1, (errmsg("checkpoint sync: number=%d file=%s time=%.3f msec", processed,
                                                    FilePathName(seg->mdfd_vfd), (double)elapsed / 1000)));
                        }
                        break; /* out of retry loop */
                    }

                    /* Compute file name for use in message */
                    save_errno = errno;
                    path = _mdfd_segpath(reln, (ForkNumber)forknum, (BlockNumber)segno);
                    errno = save_errno;

                    /*
                     * It is possible that the relation has been dropped or
                     * truncated since the fsync request was entered.
                     * Therefore, allow ENOENT, but only if we didn't fail
                     * already on this file.  This applies both for
                     * _mdfd_getseg() and for FileSync, since fd.c might have
                     * closed the file behind our back.
                     *
                     * XXX is there any point in allowing more than one retry?
                     * Don't see one at the moment, but easy to change the
                     * test here if so.
                     */
                    if (!FILE_POSSIBLY_DELETED(errno) || failures > 0) {
                        ereport(data_sync_elevel(ERROR),
                                (errcode_for_file_access(), errmsg("could not fsync file \"%s\": %m", path)));
                    } else {
                        ereport(DEBUG1, (errcode_for_file_access(),
                                         errmsg("could not fsync file \"%s\" but retrying: %m", path)));
                    }
                    pfree(path);

                    /*
                     * Absorb incoming requests and check to see if a cancel
                     * arrived for this relation fork.
                     */
                    AbsorbFsyncRequests();
                    absorb_counter = FSYNCS_PER_ABSORB; /* might as well... */

                    if (entry->canceled[forknum]) {
                        break;
                    }
                } /* end retry loop */
            }
            bms_free(requests);
        }

        /*
         * We've finished everything that was requested before we started to
         * scan the entry.	If no new requests have been inserted meanwhile,
         * remove the entry.  Otherwise, update its cycle counter, as all the
         * requests now in it must have arrived during this cycle.
         */
        for (forknum = 0; forknum < entry->max_requests; forknum++) {
            if (entry->requests[forknum] != NULL) {
                break;
            }
        }

        if (forknum < entry->max_requests) {
            entry->cycle_ctr = u_sess->storage_cxt.mdsync_cycle_ctr;
        } else {
            /* Okay to remove it */
            if (hash_search(u_sess->storage_cxt.pendingOpsTable, &entry->rnode, HASH_REMOVE, NULL) == NULL) {
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("pendingOpsTable corrupted")));
            } else {
                pfree(entry->requests);
                pfree(entry->canceled);
                entry->requests = NULL;
                entry->canceled = NULL;
            }
        }
    } /* end loop over hashtable entries */

    /* Return sync performance metrics for report at checkpoint end */
    t_thrd.xlog_cxt.CheckpointStats->ckpt_sync_rels = processed;
    t_thrd.xlog_cxt.CheckpointStats->ckpt_longest_sync = longest;
    t_thrd.xlog_cxt.CheckpointStats->ckpt_agg_sync_time = total_elapsed;

    /* Flag successful completion of mdsync */
    u_sess->storage_cxt.mdsync_in_progress = false;
}

/*
 * mdpreckpt() -- Do pre-checkpoint work
 *
 * To distinguish unlink requests that arrived before this checkpoint
 * started from those that arrived during the checkpoint, we use a cycle
 * counter similar to the one we use for fsync requests. That cycle
 * counter is incremented here.
 *
 * This must be called *before* the checkpoint REDO point is determined.
 * That ensures that we won't delete files too soon.
 *
 * Note that we can't do anything here that depends on the assumption
 * that the checkpoint will be completed.
 */
void mdpreckpt(void)
{
    /*
     * Any unlink requests arriving after this point will be assigned the next
     * cycle counter, and won't be unlinked until next checkpoint.
     */
    u_sess->storage_cxt.mdckpt_cycle_ctr++;
}

/*
 * mdpostckpt() -- Do post-checkpoint work
 *
 * Remove any lingering files that can now be safely removed.
 */
void mdpostckpt(void)
{
    int absorb_counter;

    absorb_counter = UNLINKS_PER_ABSORB;
    while (u_sess->storage_cxt.pendingUnlinks != NIL) {
        PendingUnlinkEntry *entry = (PendingUnlinkEntry *)linitial(u_sess->storage_cxt.pendingUnlinks);
        char *path = NULL;

        /*
         * New entries are appended to the end, so if the entry is new we've
         * reached the end of old entries.
         *
         * Note: if just the right number of consecutive checkpoints fail, we
         * could be fooled here by cycle_ctr wraparound.  However, the only
         * consequence is that we'd delay unlinking for one more checkpoint,
         * which is perfectly tolerable.
         */
        if (entry->cycle_ctr == u_sess->storage_cxt.mdckpt_cycle_ctr) {
            break;
        }

        /* Unlink the file */
        path = relpathperm(entry->rnode, MAIN_FORKNUM);
        if (BUCKET_ID_IS_DIR(entry->rnode.bucketNode)) {
            if (!rmtree(path, true, true)) {
                ereport(WARNING, (errcode_for_file_access(),
                                  errmsg("could not remove bucket dir after checkpoint \"%s\": %m", path)));
            } else {
                ereport(LOG, (errmsg("Remove bucket dir after checkpoint \"%s\": %m", path)));
            }
        } else {
            if (unlink(path) < 0) {
                /*
                 * There's a race condition, when the database is dropped at the
                 * same time that we process the pending unlink requests. If the
                 * DROP DATABASE deletes the file before we do, we will get ENOENT
                 * here. rmtree() also has to ignore ENOENT errors, to deal with
                 * the possibility that we delete the file first.
                 */
                if (errno != ENOENT) {
                    ereport(WARNING, (errcode_for_file_access(), errmsg("could not remove file \"%s\": %m", path)));
                }
            }
        }
        pfree(path);

        /* And remove the list entry */
        u_sess->storage_cxt.pendingUnlinks = list_delete_first(u_sess->storage_cxt.pendingUnlinks);
        pfree(entry);

        /*
         * As in mdsync, we don't want to stop absorbing fsync requests for a
         * long time when there are many deletions to be done.	We can safely
         * call AbsorbFsyncRequests() at this point in the loop (note it might
         * try to delete list entries).
         */
        if (--absorb_counter <= 0) {
            AbsorbFsyncRequests();
            absorb_counter = UNLINKS_PER_ABSORB;
        }
    }
}

/*
 * register_dirty_segment() -- Mark a relation segment as needing fsync
 *
 * If there is a local pending-ops table, just make an entry in it for
 * mdsync to process later.  Otherwise, try to pass off the fsync request
 * to the checkpointer process.  If that fails, just do the fsync
 * locally before returning (we hope this will not happen often enough
 * to be a performance problem).
 */
static void register_dirty_segment(SMgrRelation reln, ForkNumber forknum, const MdfdVec *seg)
{
    /* Temp relations should never be fsync'd */
    Assert(!SmgrIsTemp(reln));

    if (u_sess->storage_cxt.pendingOpsTable) {
        /* push it into local pending-ops table */
        RememberFsyncRequest(reln->smgr_rnode.node, forknum, seg->mdfd_segno);
    } else {
        if (ForwardFsyncRequest(reln->smgr_rnode.node, forknum, seg->mdfd_segno)) {
            return; /* passed it off successfully */
        }

        ereport(DEBUG1, (errmsg("could not forward fsync request because request queue is full")));

        if (FileSync(seg->mdfd_vfd, WAIT_EVENT_DATA_FILE_SYNC) < 0) {
            ereport(data_sync_elevel(ERROR), (errcode_for_file_access(),
                                              errmsg("could not fsync file \"%s\": %m", FilePathName(seg->mdfd_vfd))));
        }
    }
}

/*
 * register_unlink() -- Schedule a file to be deleted after next checkpoint
 *
 * We don't bother passing in the fork number, because this is only used
 * with main forks.
 *
 * As with register_dirty_segment, this could involve either a local or
 * a remote pending-ops table.
 */
static void register_unlink(const RelFileNodeBackend &rnode)
{
    /* Should never be used with temp relations */
    Assert(!RelFileNodeBackendIsTemp(rnode));

    if (u_sess->storage_cxt.pendingOpsTable) {
        /* push it into local pending-ops table */
        RememberFsyncRequest(rnode.node, MAIN_FORKNUM, UNLINK_RELATION_REQUEST);
    } else {
        /*
         * Notify the checkpointer about it.  If we fail to queue the request
         * message, we have to sleep and try again, because we can't simply
         * delete the file now.  Ugly, but hopefully won't happen often.
         *
         * XXX should we just leave the file orphaned instead?
         */
        Assert(IsUnderPostmaster);
        while (!ForwardFsyncRequest(rnode.node, MAIN_FORKNUM, UNLINK_RELATION_REQUEST)) {
            pg_usleep(10000L); /* 10 msec seems a good number */
        }
    }
}

/*
 * RememberFsyncRequest() -- callback from checkpointer side of fsync request
 *
 * We stuff fsync requests into the local hash table for execution
 * during the checkpointer's next checkpoint.  UNLINK requests go into a
 * separate linked list, however, because they get processed separately.
 *
 * The range of possible segment numbers is way less than the range of
 * BlockNumber, so we can reserve high values of segno for special purposes.
 * We define three:
 * - FORGET_RELATION_FSYNC means to cancel pending fsyncs for a relation,
 *	 either for one fork, or all forks if forknum is InvalidForkNumber
 * - FORGET_DATABASE_FSYNC means to cancel pending fsyncs for a whole database
 * - UNLINK_RELATION_REQUEST is a request to delete the file after the next
 *	 checkpoint.
 * Note also that we're assuming real segment numbers don't exceed INT_MAX.
 *
 * (Handling FORGET_DATABASE_FSYNC requests is a tad slow because the hash
 * table has to be searched linearly, but dropping a database is a pretty
 * heavyweight operation anyhow, so we'll live with it.)
 */
void RememberFsyncRequest(const RelFileNode &rnode, ForkNumber forknum, BlockNumber segno)
{
    Assert(u_sess->storage_cxt.pendingOpsTable);

    int intforknum = (int)forknum;

    if (segno == FORGET_RELATION_FSYNC) {
        if (rnode.bucketNode != DIR_BUCKET_ID) {
            /* Remove any pending requests for the relation (one or all forks) */
            PendingOperationEntry *entry = NULL;

            entry = (PendingOperationEntry *)hash_search(u_sess->storage_cxt.pendingOpsTable, &rnode, HASH_FIND, NULL);
            if (entry != NULL) {
                /*
                 * We can't just delete the entry since mdsync could have an
                 * active hashtable scan.  Instead we delete the bitmapsets; this
                 * is safe because of the way mdsync is coded.	We also set the
                 * "canceled" flags so that mdsync can tell that a cancel arrived
                 * for the fork(s).
                 */
                if (forknum == InvalidForkNumber) {
                    /* remove requests for all forks */
                    for (intforknum = 0; intforknum < entry->max_requests; intforknum++) {
                        bms_free(entry->requests[intforknum]);
                        entry->requests[intforknum] = NULL;
                        entry->canceled[intforknum] = true;
                    }
                } else if (intforknum < entry->max_requests) {
                    /* remove requests for single fork */
                    bms_free(entry->requests[intforknum]);
                    entry->requests[intforknum] = NULL;
                    entry->canceled[intforknum] = true;
                }
            }
        } else {
            /* Remove any pending requests for the entire bucket dir */
            HASH_SEQ_STATUS hstat;
            PendingOperationEntry *entry = NULL;
            ListCell *cell = NULL;
            ListCell *prev = NULL;
            ListCell *next = NULL;

            /* Remove fsync requests */
            hash_seq_init(&hstat, u_sess->storage_cxt.pendingOpsTable);
            while ((entry = (PendingOperationEntry *)hash_seq_search(&hstat)) != NULL) {
                if (RelFileNodeRelEquals(entry->rnode, rnode)) {
                    /* remove requests for all forks */
                    for (intforknum = 0; intforknum < entry->max_requests; intforknum++) {
                        bms_free(entry->requests[intforknum]);
                        entry->requests[intforknum] = NULL;
                        entry->canceled[intforknum] = true;
                    }
                }
            }
            /* Remove unlink requests */
            for (cell = list_head(u_sess->storage_cxt.pendingUnlinks); cell; cell = next) {
                PendingUnlinkEntry *unlink_entry = (PendingUnlinkEntry *)lfirst(cell);
                next = lnext(cell);
                if (RelFileNodeRelEquals(unlink_entry->rnode, rnode)) {
                    u_sess->storage_cxt.pendingUnlinks = list_delete_cell(u_sess->storage_cxt.pendingUnlinks, cell,
                                                                          prev);
                    pfree(unlink_entry);
                } else {
                    prev = cell;
                }
            }
        }
    } else if (segno == FORGET_DATABASE_FSYNC) {
        /* Remove any pending requests for the entire database */
        HASH_SEQ_STATUS hstat;
        PendingOperationEntry *entry = NULL;
        ListCell *cell = NULL;
        ListCell *prev = NULL;
        ListCell *next = NULL;

        /* Remove fsync requests */
        hash_seq_init(&hstat, u_sess->storage_cxt.pendingOpsTable);
        while ((entry = (PendingOperationEntry *)hash_seq_search(&hstat)) != NULL) {
            if (entry->rnode.dbNode == rnode.dbNode) {
                /* remove requests for all forks */
                for (intforknum = 0; intforknum < entry->max_requests; intforknum++) {
                    bms_free(entry->requests[intforknum]);
                    entry->requests[intforknum] = NULL;
                    entry->canceled[intforknum] = true;
                }
            }
        }

        /* Remove unlink requests */
        for (cell = list_head(u_sess->storage_cxt.pendingUnlinks); cell; cell = next) {
            PendingUnlinkEntry *unlink_entry = (PendingUnlinkEntry *)lfirst(cell);

            next = lnext(cell);
            if (unlink_entry->rnode.dbNode == rnode.dbNode) {
                u_sess->storage_cxt.pendingUnlinks = list_delete_cell(u_sess->storage_cxt.pendingUnlinks, cell, prev);
                pfree(unlink_entry);
            } else {
                prev = cell;
            }
        }
    } else if (segno == UNLINK_RELATION_REQUEST) {
        /* Unlink request: put it in the linked list */
        MemoryContext oldcxt = MemoryContextSwitchTo(u_sess->storage_cxt.MdCxt);
        PendingUnlinkEntry *entry = NULL;

        /* PendingUnlinkEntry doesn't store forknum, since it's always MAIN */
        Assert(forknum == MAIN_FORKNUM);

        entry = (PendingUnlinkEntry *)palloc(sizeof(PendingUnlinkEntry));
        entry->rnode = rnode;
        entry->cycle_ctr = u_sess->storage_cxt.mdckpt_cycle_ctr;

        u_sess->storage_cxt.pendingUnlinks = lappend(u_sess->storage_cxt.pendingUnlinks, entry);

        MemoryContextSwitchTo(oldcxt);
    } else {
        /* Normal case: enter a request to fsync this segment */
        MemoryContext oldcxt = MemoryContextSwitchTo(u_sess->storage_cxt.MdCxt);
        PendingOperationEntry *entry = NULL;
        bool found = false;

        entry = (PendingOperationEntry *)hash_search(u_sess->storage_cxt.pendingOpsTable, &rnode, HASH_ENTER, &found);
        /* if new entry, initialize it */
        if (!found) {
            entry->cycle_ctr = u_sess->storage_cxt.mdsync_cycle_ctr;
            entry->max_requests = 100;
            entry->requests = (Bitmapset **)palloc0((size_t)entry->max_requests * sizeof(Bitmapset *));
            entry->canceled = (bool *)palloc0((size_t)entry->max_requests * sizeof(bool));
        }

        if (intforknum >= (entry->max_requests)) {
            unsigned int old_max_requests = (unsigned int)entry->max_requests;
            unsigned int added_max_requests;

            entry->max_requests *= 2;
            entry->max_requests = Max(entry->max_requests, intforknum + 1);
            added_max_requests = entry->max_requests - old_max_requests;
            entry->requests = (Bitmapset **)repalloc((void *)(entry->requests),
                                                     entry->max_requests * sizeof(Bitmapset *));
            entry->canceled = (bool *)repalloc((void *)(entry->canceled), (size_t)entry->max_requests * sizeof(bool));
            errno_t ret = memset_s(entry->requests + old_max_requests, added_max_requests * sizeof(Bitmapset *), 0,
                                   added_max_requests * sizeof(Bitmapset *));
            securec_check(ret, "", "");
            ret = memset_s(entry->canceled + old_max_requests, added_max_requests * sizeof(bool), 0,
                           added_max_requests * sizeof(bool));
            securec_check(ret, "", "");
        }

        /*
         * NB: it's intentional that we don't change cycle_ctr if the entry
         * already exists.	The cycle_ctr must represent the oldest fsync
         * request that could be in the entry.
         */
        entry->requests[intforknum] = bms_add_member(entry->requests[intforknum], (int)segno);

        (void)MemoryContextSwitchTo(oldcxt);
    }
}

/*
 * ForgetRelationFsyncRequests -- forget any fsyncs for a relation fork
 *
 * forknum == InvalidForkNumber means all forks, although this code doesn't
 * actually know that, since it's just forwarding the request elsewhere.
 */
void ForgetRelationFsyncRequests(const RelFileNode &rnode, ForkNumber forknum)
{
    if (u_sess->storage_cxt.pendingOpsTable) {
        /* standalone backend or startup process: fsync state is local */
        RememberFsyncRequest(rnode, forknum, FORGET_RELATION_FSYNC);
    } else if (IsUnderPostmaster) {
        /*
         * Notify the checkpointer about it.  If we fail to queue the cancel
         * message, we have to sleep and try again ... ugly, but hopefully
         * won't happen often.
         *
         * XXX should we CHECK_FOR_INTERRUPTS in this loop?  Escaping with an
         * error would leave the no-longer-used file still present on disk,
         * which would be bad, so I'm inclined to assume that the checkpointer
         * will always empty the queue soon.
         */
        while (!ForwardFsyncRequest(rnode, forknum, FORGET_RELATION_FSYNC)) {
            pg_usleep(10000L); /* 10 msec seems a good number */
        }

        /*
         * Note we don't wait for the checkpointer to actually absorb the
         * cancel message; see mdsync() for the implications.
         */
    }
}

/*
 * ForgetDatabaseFsyncRequests -- forget any fsyncs and unlinks for a DB
 */
void ForgetDatabaseFsyncRequests(Oid dbid)
{
    RelFileNode rnode;

    rnode.dbNode = dbid;
    rnode.spcNode = 0;
    rnode.relNode = 0;
    rnode.bucketNode = InvalidBktId;

    if (u_sess->storage_cxt.pendingOpsTable) {
        /* standalone backend or startup process: fsync state is local */
        RememberFsyncRequest(rnode, InvalidForkNumber, FORGET_DATABASE_FSYNC);
    } else if (IsUnderPostmaster) {
        /* see notes in ForgetRelationFsyncRequests */
        while (!ForwardFsyncRequest(rnode, InvalidForkNumber, FORGET_DATABASE_FSYNC)) {
            pg_usleep(10000L); /* 10 msec seems a good number */
        }
    }
}

/*
 *  _fdvec_alloc() -- Make a MdfdVec object.
 */
static MdfdVec *_fdvec_alloc(void)
{
    return (MdfdVec *)MemoryContextAlloc(u_sess->storage_cxt.MdCxt, sizeof(MdfdVec));
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

    /* open the file */
    fd = DataFileIdOpenFile(fullpath, filenode, O_RDWR | PG_BINARY | oflags, 0600);

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
static MdfdVec *_mdfd_getseg(SMgrRelation reln, ForkNumber forknum, BlockNumber blkno, bool skipFsync,
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

    targetseg = blkno / ((BlockNumber)RELSEG_SIZE);
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
                if (_mdnblocks(reln, forknum, v) < RELSEG_SIZE) {
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

                    mdextend(reln, forknum, nextsegno * ((BlockNumber)RELSEG_SIZE) - 1, zerobuf, skipFsync);

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
        ereport(ERROR, (errcode_for_file_access(),
                        errmsg("could not seek to end of file \"%s\": %m", FilePathName(seg->mdfd_vfd))));
    }

    /* note that this calculation will ignore any partial block at EOF */
    return (BlockNumber)(len / BLCKSZ);
}

static MdfdVec *_mdcreate(ForkNumber forkNum, bool isRedo, const RelFileNodeBackend &rnode)
{
    File fd = -1;
    MdfdVec *ret = NULL;
    RelFileNodeForkNum filenode;
    volatile uint32 flags = O_RDWR | O_CREAT | O_EXCL | PG_BINARY;
    char *path = NULL;
    path = relpath(rnode, forkNum);
    filenode = RelFileNodeForkNumFill(rnode, forkNum, 0);
    ADIO_RUN()
    {
        flags |= O_DIRECT;
    }
    ADIO_END();

    fd = DataFileIdOpenFile(path, filenode, flags, 0600);

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
            ADIO_RUN()
            {
                flags = O_RDWR | PG_BINARY | O_DIRECT | (u_sess->attr.attr_common.IsInplaceUpgrade ? O_TRUNC : 0);
            }
            ADIO_ELSE()
            {
                flags = O_RDWR | PG_BINARY | (u_sess->attr.attr_common.IsInplaceUpgrade ? O_TRUNC : 0);
            }
            ADIO_END();

            fd = DataFileIdOpenFile(path, filenode, flags, 0600);
        }

        if (fd < 0) {
            /* be sure to report the error reported by create, not open */
            errno = save_errno;
            ereport(WARNING, (errcode_for_file_access(), errmsg("could not create file \"%s\": %m", path)));
            Assert(0);
        }
    }
    pfree(path);
    ret = _fdvec_alloc();
    ret->mdfd_vfd = fd;
    ret->mdfd_segno = 0;
    ret->mdfd_chain = NULL;
    return ret;
}
