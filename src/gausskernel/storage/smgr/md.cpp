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
#include "postmaster/pagerepair.h"
#include "storage/smgr/fd.h"
#include "storage/buf/bufmgr.h"
#include "storage/smgr/relfilenode.h"
#include "storage/copydir.h"
#include "storage/page_compression.h"
#include "storage/smgr/knl_usync.h"
#include "storage/smgr/smgr.h"
#include "utils/aiomem.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "pg_trace.h"
#include "pgstat.h"

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

constexpr mode_t FILE_RW_PERMISSION = 0600;

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
typedef struct _MdfdVec {
    File mdfd_vfd;               /* fd number in fd.c's pool */
    File mdfd_vfd_pca;    /* page compression address file 's fd number in fd.cpp's pool */
    File mdfd_vfd_pcd;    /* page compression data file 's fd number in fd.cpp's pool */
    BlockNumber mdfd_segno;      /* segment number, from 0 */
    struct _MdfdVec *mdfd_chain; /* next segment, or NULL */
} MdfdVec;

/* local routines */
static void mdunlinkfork(const RelFileNodeBackend &rnode, ForkNumber forkNum, bool isRedo);
static MdfdVec *mdopen(SMgrRelation reln, ForkNumber forknum, ExtensionBehavior behavior);
static MdfdVec *_fdvec_alloc(void);
static char *_mdfd_segpath(const SMgrRelation reln, ForkNumber forknum, BlockNumber segno);
static MdfdVec *_mdfd_openseg(SMgrRelation reln, ForkNumber forkno, BlockNumber segno, int oflags);
static MdfdVec *_mdfd_getseg(SMgrRelation reln, ForkNumber forkno, BlockNumber blkno, bool skipFsync, ExtensionBehavior behavior);
static BlockNumber _mdnblocks(SMgrRelation reln, ForkNumber forknum, const MdfdVec *seg);
static void register_dirty_segment(SMgrRelation reln, ForkNumber forknum, const MdfdVec *seg);
static void register_unlink_segment(RelFileNodeBackend rnode, ForkNumber forknum, BlockNumber segno);

/* function of compressed table */
static int sync_pcmap(PageCompressHeader *pcMap, uint32 wait_event_info);

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

static int OpenPcaFile(const char *path, const RelFileNodeBackend &node, const ForkNumber &forkNum, const uint32 &segNo, int oflags = 0)
{
    Assert(node.node.opt != 0 && forkNum == MAIN_FORKNUM);
    char dst[MAXPGPATH];
    CopyCompressedPath(dst, path, COMPRESSED_TABLE_PCA_FILE);
    uint32 flags = O_RDWR | PG_BINARY | oflags;
    return DataFileIdOpenFile(dst, RelFileNodeForkNumFill(node, PCA_FORKNUM, segNo), (int)flags, S_IRUSR | S_IWUSR);
}

static int OpenPcdFile(const char *path, const RelFileNodeBackend &node, const ForkNumber &forkNum, const uint32 &segNo, int oflags = 0)
{
    Assert(node.node.opt != 0 && forkNum == MAIN_FORKNUM);
    char dst[MAXPGPATH];
    CopyCompressedPath(dst, path, COMPRESSED_TABLE_PCD_FILE);
    uint32 flags = O_RDWR | PG_BINARY | oflags;
    return DataFileIdOpenFile(dst, RelFileNodeForkNumFill(node, PCD_FORKNUM, segNo), (int)flags, S_IRUSR | S_IWUSR);
}

static void RegisterCompressDirtySegment(SMgrRelation reln, ForkNumber forknum, const MdfdVec *seg)
{
    PageCompressHeader *pcMap = GetPageCompressMemoryMap(seg->mdfd_vfd_pca, PageCompressChunkSize(reln));
    if (sync_pcmap(pcMap, WAIT_EVENT_COMPRESS_ADDRESS_FILE_SYNC) != 0) {
        if (check_unlink_rel_hashtbl(reln->smgr_rnode.node, forknum)) {
            ereport(DEBUG1, (errmsg("could not fsync file \"%s\": %m", FilePathName(seg->mdfd_vfd))));
            return;
        }
        ereport(data_sync_elevel(ERROR), (errcode_for_file_access(), errmsg("could not msync file \"%s\": %m",
                                                                            FilePathName(seg->mdfd_vfd_pca))));
    }
    if (FileSync(seg->mdfd_vfd_pcd, WAIT_EVENT_DATA_FILE_SYNC) < 0) {
        if (check_unlink_rel_hashtbl(reln->smgr_rnode.node, forknum)) {
            ereport(DEBUG1, (errmsg("could not fsync file \"%s\": %m", FilePathName(seg->mdfd_vfd))));
            return;
        }
        ereport(data_sync_elevel(ERROR), (errcode_for_file_access(), errmsg("could not fsync file \"%s\": %m",
                                                                            FilePathName(seg->mdfd_vfd_pcd))));
    }
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

        if (IS_COMPRESSED_MAINFORK(reln, forknum)) {
            RegisterCompressDirtySegment(reln, forknum, seg);
        } else {
            if (FileSync(seg->mdfd_vfd, WAIT_EVENT_DATA_FILE_SYNC) < 0) {
                if (check_unlink_rel_hashtbl(reln->smgr_rnode.node, forknum)) {
                    ereport(DEBUG1, (errmsg("could not fsync file \"%s\": %m", FilePathName(seg->mdfd_vfd))));
                    return;
                }
                ereport(data_sync_elevel(ERROR), (errcode_for_file_access(),
                                                  errmsg("could not fsync file \"%s\": %m", FilePathName(seg->mdfd_vfd))));
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

static void allocate_chunk_check(PageCompressAddr *pcAddr, uint32 chunk_size, BlockNumber blocknum, MdfdVec *v)
{
    /* check allocated chunk number */
    Assert(chunk_size == BLCKSZ / 2 || chunk_size == BLCKSZ / 4 || chunk_size == BLCKSZ / 8 ||
           chunk_size == BLCKSZ / 16);
    if (pcAddr->allocated_chunks > BLCKSZ / chunk_size) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("invalid chunks %u of block %u in file \"%s\"",
                                                                pcAddr->allocated_chunks, blocknum,
                                                                FilePathName(v->mdfd_vfd_pca))));
    }

    auto maxChunkNumbers = MAX_CHUNK_NUMBER(chunk_size);
    for (auto i = 0; i < pcAddr->allocated_chunks; i++) {
        if (pcAddr->chunknos[i] <= 0 || pcAddr->chunknos[i] > maxChunkNumbers) {
            ereport(ERROR,
                    (errcode(ERRCODE_DATA_CORRUPTED), errmsg("invalid chunk number %u of block %u in file \"%s\"",
                                                             pcAddr->chunknos[i], blocknum,
                                                             FilePathName(v->mdfd_vfd_pca))));
        }
    }
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
        ereport(WARNING, (errmsg("[file repair] could not open repair file %s: %m", temppath)));
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

        fd = DataFileIdOpenFile(path, filenode, flags, FILE_RW_PERMISSION);
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

    if (isRedo && (AmStartupProcess() || AmPageRedoWorker() || AmPageWriterProcess() || AmCheckpointerProcess()) &&
        CheckFileRepairHashTbl(reln->smgr_rnode.node, forkNum, 0)) {
        fd = openrepairfile(path, filenode);
        if (fd >= 0) {
            ereport(LOG, (errmsg("[file repair] open repair file %s.repair", path)));
        }
    } else {
        fd = DataFileIdOpenFile(path, filenode, flags, 0600);
    }

    if (fd < 0) {
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
        fd = RetryDataFileIdOpenFile(isRedo, path, filenode, flags);
    }

    File fd_pca = -1;
    File fd_pcd = -1;
    if (unlikely(IS_COMPRESSED_MAINFORK(reln, forkNum))) {
        // close main fork file
        FileClose(fd);
        fd = -1;

        /* open page compress address file */
        char pcfile_path[MAXPGPATH];
        errno_t rc = snprintf_s(pcfile_path, MAXPGPATH, MAXPGPATH - 1, PCA_SUFFIX, path);
        securec_check_ss(rc, "\0", "\0");
        RelFileNodeForkNum pcaFienode = RelFileNodeForkNumFill(reln->smgr_rnode, PCA_FORKNUM, 0);
        fd_pca = DataFileIdOpenFile(pcfile_path, pcaFienode, flags, FILE_RW_PERMISSION);

        if (fd_pca < 0) {
            fd_pca = RetryDataFileIdOpenFile(isRedo, pcfile_path, pcaFienode, flags);
        }

        rc = snprintf_s(pcfile_path, MAXPGPATH, MAXPGPATH - 1, PCD_SUFFIX, path);
        securec_check_ss(rc, "\0", "\0");
        RelFileNodeForkNum pcdFileNode = RelFileNodeForkNumFill(reln->smgr_rnode, PCD_FORKNUM, 0);
        fd_pcd = DataFileIdOpenFile(pcfile_path, pcdFileNode, flags, FILE_RW_PERMISSION);
        if (fd_pcd < 0) {
            fd_pcd = RetryDataFileIdOpenFile(isRedo, pcfile_path, pcdFileNode, flags);
        }
        SetupPageCompressMemoryMap(fd_pca, reln->smgr_rnode.node, filenode);
    }

    pfree(path);

    reln->md_fd[forkNum] = _fdvec_alloc();

    reln->md_fd[forkNum]->mdfd_vfd_pca = fd_pca;
    reln->md_fd[forkNum] ->mdfd_vfd_pcd = fd_pcd;
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

/**
 * set all zero  to pca file
 * @param fd pca file fd 
 * @param chunkSize chunkSize
 * @param path for errport
 * @return 0 for success and other for failed
 */
static int ResetPcaFileInner(int fd, int chunkSize, char *path)
{
    bool ret = 0;
    int mapRealSize = SIZE_OF_PAGE_COMPRESS_ADDR_FILE(chunkSize);
    PageCompressHeader *map = pc_mmap_real_size(fd, mapRealSize, false);
    if (map == MAP_FAILED) {
        ereport(WARNING, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("Failed to mmap %s: %m", path)));
        ret = -1;
    } else {
        pg_atomic_write_u32(&map->nblocks, 0);
        pg_atomic_write_u32(&map->allocated_chunks, 0);
        error_t rc =
            memset_s((char *)map + SIZE_OF_PAGE_COMPRESS_HEADER_DATA, mapRealSize - SIZE_OF_PAGE_COMPRESS_HEADER_DATA,
                     0, SIZE_OF_PAGE_COMPRESS_ADDR_FILE(chunkSize) - SIZE_OF_PAGE_COMPRESS_HEADER_DATA);
        securec_check_c(rc, "\0", "\0");
        map->sync = false;
        if (sync_pcmap(map, WAIT_EVENT_COMPRESS_ADDRESS_FILE_SYNC) != 0) {
            ret = -1;
            ereport(WARNING, (errcode_for_file_access(), errmsg("could not msync file \"%s\": %m", path)));
        }
        /* if mmap is called then pc_munmap must be called too */
        if (pc_munmap(map) != 0) {
            ret = -1;
            ereport(WARNING, (errcode_for_file_access(), errmsg("could not munmap file \"%s\": %m", path)));
        }
    }
    return ret;
}

/**
 * 
 * @param fd pca_file fd
 * @param chunkSize chunkSize destination
 * @param path for ereport
 * @return success or not
 */
static bool ReadChunkSizeFromFile(int fd, uint16 *chunkSize, char *path)
{
    off_t chunkSizeOffset = (off_t)offsetof(PageCompressHeader, chunk_size);
    if (lseek(fd, chunkSizeOffset, SEEK_SET) != chunkSizeOffset && errno != ENOENT) {
        ereport(WARNING, (errcode_for_file_access(), errmsg("could not lseek file \"%s\": %m", path)));
        return false;
    }
    if (read(fd, chunkSize, sizeof(*chunkSize)) != sizeof(*chunkSize) && errno != ENOENT) {
        ereport(WARNING, (errcode_for_file_access(), errmsg("could not read file \"%s\": %m", path)));
        return false;
    }
    return true;
}

static int ResetPcMap(char *path, const RelFileNodeBackend &rnode)
{
    int ret = 0;
    char pcfile_path[MAXPGPATH];
    int rc = snprintf_s(pcfile_path, MAXPGPATH, MAXPGPATH - 1, PCA_SUFFIX, path);
    securec_check_ss(rc, "\0", "\0");
    int fd_pca = BasicOpenFile(pcfile_path, O_RDWR | PG_BINARY, 0);
    if (fd_pca < 0) {
        return -1;
    }
    uint16 chunkSize;
    if (ReadChunkSizeFromFile(fd_pca, &chunkSize, pcfile_path)) {
        ret = ResetPcaFileInner(fd_pca, chunkSize, pcfile_path);
    } else {
        ret = -1;
    }
    int save_errno = errno;
    (void)close(fd_pca);
    errno = save_errno;
    
    return ret;
}

static void UnlinkCompressedFile(const RelFileNode& node, ForkNumber forkNum, char* path)
{
    if (!IS_COMPRESSED_RNODE(node, forkNum)) {
        return;
    }
    /* remove pca */
    char pcfile_path[MAXPGPATH];
    errno_t rc = snprintf_s(pcfile_path, MAXPGPATH, MAXPGPATH - 1, PCA_SUFFIX, path);
    securec_check_ss(rc, "\0", "\0");
    int ret = unlink(pcfile_path);
    if (ret < 0 && errno != ENOENT) {
        ereport(WARNING, (errcode_for_file_access(), errmsg("could not remove file \"%s\": %m", pcfile_path)));
    }
    /* remove pcd */
    rc = snprintf_s(pcfile_path, MAXPGPATH, MAXPGPATH - 1, PCD_SUFFIX, path);
    securec_check_ss(rc, "\0", "\0");
    ret = unlink(pcfile_path);
    if (ret < 0 && errno != ENOENT) {
        ereport(WARNING, (errcode_for_file_access(), errmsg("could not remove file \"%s\": %m", pcfile_path)));
    }
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

static void mdunlinkfork(const RelFileNodeBackend& rnode, ForkNumber forkNum, bool isRedo)
{
    char* path = NULL;
    int ret;

    path = relpath(rnode, forkNum);
    
    /*
     * Delete or truncate the first segment.
     */
    Assert(IsHeapFileNode(rnode.node));
    if (isRedo || u_sess->attr.attr_common.IsInplaceUpgrade || forkNum != MAIN_FORKNUM ||
        RelFileNodeBackendIsTemp(rnode)) {
        /* First, forget any pending sync requests for the first segment */
        if (!RelFileNodeBackendIsTemp(rnode)) {
            md_register_forget_request(rnode.node, forkNum, 0 /* first segment */);
        }

        /* Next unlink the file */
        ret = unlink(path);
        if (ret < 0 && errno != ENOENT) {
            ereport(WARNING, (errcode_for_file_access(), errmsg("could not remove file \"%s\": %m", path)));
        }
        if (isRedo) {
            mdcleanrepairfile(path);
        }
        UnlinkCompressedFile(rnode.node, forkNum, path);
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
        if (IS_COMPRESSED_RNODE(rnode.node, forkNum)) {
            // dont truncate pca! pca may be occupied by other threads by mmap
            ret = ResetPcMap(path, rnode);

            // remove pcd
            char dataPath[MAXPGPATH];
            int rc = snprintf_s(dataPath, MAXPGPATH, MAXPGPATH - 1, PCD_SUFFIX, path);
            securec_check_ss(rc, "\0", "\0");
            int fd_pcd = BasicOpenFile(dataPath, O_RDWR | PG_BINARY, 0);
            if (fd_pcd >= 0) {
                int save_errno;
                ret = ftruncate(fd_pcd, 0);
                save_errno = errno;
                (void)close(fd_pcd);
                errno = save_errno;
            } else {
                ret = -1;
            }
            if (ret < 0 && errno != ENOENT) {
                ereport(WARNING, (errcode_for_file_access(), errmsg("could not truncate file \"%s\": %m", dataPath)));
            }
        }

        /* Register request to unlink first segment later */
        register_unlink_segment(rnode, forkNum, 0);
    }

    /*
     * Delete any additional segments.
     */
    if (ret >= 0) {
        char *segpath = (char *)palloc(strlen(path) + 12);
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

            rc = sprintf_s(segpath, strlen(path) + 12, "%s.%u", path, segno);
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
            rc = sprintf_s(segpath, strlen(path) + 12, "%s.%u", path, segno);
            securec_check_ss(rc, "", "");
            if (unlink(segpath) < 0) {
                ereport(WARNING, (errcode_for_file_access(),
                    errmsg("could not remove file \"%s\": %m", segpath)));
            }
            if (IS_COMPRESSED_RNODE(rnode.node, forkNum)) {
                char pcfile_segpath[MAXPGPATH];
                errno_t rc = snprintf_s(pcfile_segpath, MAXPGPATH, MAXPGPATH - 1, PCA_SUFFIX, segpath);
                securec_check_ss(rc, "\0", "\0");
                if (unlink(pcfile_segpath) < 0) {
                    ereport(WARNING,
                            (errcode_for_file_access(), errmsg("could not remove file \"%s\": %m", pcfile_segpath)));
                }

                rc = snprintf_s(pcfile_segpath, MAXPGPATH, MAXPGPATH - 1, PCD_SUFFIX, segpath);
                securec_check_ss(rc, "\0", "\0");
                if (unlink(pcfile_segpath) < 0) {
                    ereport(WARNING,
                            (errcode_for_file_access(), errmsg("could not remove file \"%s\": %m", pcfile_segpath)));
                }
            }
            /* try clean the repair file if exists */
            if (isRedo) {
                mdcleanrepairfile(path);
            }
        }
        pfree(segpath);
    }

    pfree(path);
}

static inline void ExtendChunksOfBlock(PageCompressHeader* pcMap, PageCompressAddr* pcAddr, int needChunks, MdfdVec* v)
{
    if (pcAddr->allocated_chunks < needChunks) {
        auto allocateNumber = needChunks - pcAddr->allocated_chunks;
        int chunkno = (pc_chunk_number_t)pg_atomic_fetch_add_u32(&pcMap->allocated_chunks, allocateNumber) + 1;
        for (int i = pcAddr->allocated_chunks; i < needChunks; ++i, ++chunkno) {
            pcAddr->chunknos[i] = chunkno;
        }
        pcAddr->allocated_chunks = needChunks;

        if (pg_atomic_read_u32(&pcMap->allocated_chunks) - pg_atomic_read_u32(&pcMap->last_synced_allocated_chunks) >
            COMPRESS_ADDRESS_FLUSH_CHUNKS) {
            pcMap->sync = false;
            if (sync_pcmap(pcMap, WAIT_EVENT_COMPRESS_ADDRESS_FILE_FLUSH) != 0) {
                ereport(data_sync_elevel(ERROR), (errcode_for_file_access(), errmsg("could not msync file \"%s\": %m",
                                                                                    FilePathName(v->mdfd_vfd_pca))));
            }
        }
    }
}

/*
 *	mdextend_pc() -- Add a block to the specified page compressed relation.
 *
 */
static void mdextend_pc(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, const char* buffer, bool skipFsync)
{
#ifdef CHECK_WRITE_VS_EXTEND
    Assert(blocknum >= mdnblocks(reln, forknum));
#endif
    Assert(IS_COMPRESSED_MAINFORK(reln, forknum));

    MdfdVec* v = _mdfd_getseg(reln, MAIN_FORKNUM, blocknum, skipFsync, EXTENSION_CREATE);
    RelFileCompressOption option;
    TransCompressOptions(reln->smgr_rnode.node, &option);
    uint32 chunk_size = CHUNK_SIZE_LIST[option.compressChunkSize];
    uint8 algorithm = option.compressAlgorithm;
    uint8 prealloc_chunk = option.compressPreallocChunks;
    PageCompressHeader *pcMap = GetPageCompressMemoryMap(v->mdfd_vfd_pca, chunk_size);
    Assert(blocknum % RELSEG_SIZE >= pg_atomic_read_u32(&pcMap->nblocks));

    uint32 maxAllocChunkNum = (uint32)(BLCKSZ / chunk_size - 1);
    PageCompressAddr* pcAddr = GET_PAGE_COMPRESS_ADDR(pcMap, chunk_size, blocknum);

    prealloc_chunk = (prealloc_chunk > maxAllocChunkNum) ? maxAllocChunkNum : prealloc_chunk;

    /* check allocated chunk number */
    if (pcAddr->allocated_chunks > BLCKSZ / chunk_size) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("invalid chunks %u of block %u in file \"%s\"",
                                                                pcAddr->allocated_chunks, blocknum,
                                                                FilePathName(v->mdfd_vfd_pca))));
    }

    for (int i = 0; i < pcAddr->allocated_chunks; ++i) {
        if (pcAddr->chunknos[i] <= 0 || pcAddr->chunknos[i] > (BLCKSZ / chunk_size) * RELSEG_SIZE) {
            ereport(ERROR,
                    (errcode(ERRCODE_DATA_CORRUPTED), errmsg("invalid chunk number %u of block %u in file \"%s\"",
                                                             pcAddr->chunknos[i], blocknum,
                                                             FilePathName(v->mdfd_vfd_pca))));
        }
    }

    /* compress page only for initialized page */
    char *work_buffer = NULL;
    int nchunks = 0;
    if (!PageIsNew(buffer)) {
        int work_buffer_size = CompressPageBufferBound(buffer, algorithm);
        if (work_buffer_size < 0) {
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("mdextend_pc unrecognized compression algorithm %d", algorithm)));
        }
        work_buffer = (char *) palloc(work_buffer_size);
        int compressed_page_size = CompressPage(buffer, work_buffer, work_buffer_size, option);
        if (compressed_page_size < 0) {
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("mdextend_pc unrecognized compression algorithm %d", algorithm)));
        }
        nchunks = (compressed_page_size - 1) / chunk_size + 1;
        if (nchunks * chunk_size >= BLCKSZ) {
            pfree(work_buffer);
            work_buffer = (char *) buffer;
            nchunks = BLCKSZ / chunk_size;
        } else {
            /* fill zero in the last chunk */
            int storageSize = chunk_size * nchunks;
            if (compressed_page_size < storageSize) {
                error_t rc = memset_s(work_buffer + compressed_page_size, work_buffer_size - compressed_page_size, 0,
                                      storageSize - compressed_page_size);
                securec_check_c(rc, "\0", "\0");
            }
        }
    }

    int need_chunks = prealloc_chunk > nchunks ? prealloc_chunk : nchunks;
    ExtendChunksOfBlock(pcMap, pcAddr, need_chunks, v);

    /* write chunks of compressed page
     * worker_buffer = NULL -> nchunks = 0
     */
    for (int i = 0; i < nchunks; i++) {
        char* buffer_pos = work_buffer + chunk_size * i;
        off_t seekpos = (off_t) OFFSET_OF_PAGE_COMPRESS_CHUNK(chunk_size, pcAddr->chunknos[i]);
        // write continuous chunks
        int range = 1;
        while (i < nchunks - 1 && pcAddr->chunknos[i + 1] == pcAddr->chunknos[i] + 1) {
            range++;
            i++;
        }
        int write_amount = chunk_size * range;
        int nbytes;
        if ((nbytes = FileWrite(v->mdfd_vfd_pcd, buffer_pos, write_amount, seekpos)) != write_amount) {
            if (nbytes < 0) {
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not extend file \"%s\": %m",
                                                                  FilePathName(v->mdfd_vfd_pcd)), errhint(
                    "Check free disk space.")));
            }
            /* short write: complain appropriately */
            ereport(ERROR, (errcode(ERRCODE_DISK_FULL), errmsg(
                "could not extend file \"%s\": wrote only %d of %d bytes at block %u", FilePathName(v->mdfd_vfd_pcd),
                nbytes, write_amount, blocknum), errhint("Check free disk space.")));
        }
    }

    /* finally update size of this page and global nblocks */
    if (pcAddr->nchunks != nchunks) {
        pcAddr->nchunks = nchunks;
    }

    /* write checksum */
    pcAddr->checksum = AddrChecksum32(blocknum, pcAddr, chunk_size);


    if (pg_atomic_read_u32(&pcMap->nblocks) < blocknum % RELSEG_SIZE + 1) {
        pg_atomic_write_u32(&pcMap->nblocks, blocknum % RELSEG_SIZE + 1);
    }

    pcMap->sync = false;
    if (work_buffer != NULL && work_buffer != buffer) {
        pfree(work_buffer);
    }

    if (!skipFsync && !SmgrIsTemp(reln)) {
        register_dirty_segment(reln, forknum, v);
    }

    Assert(_mdnblocks(reln, forknum, v) <= ((BlockNumber) RELSEG_SIZE));
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
    if (IS_COMPRESSED_MAINFORK(reln, forknum)) {
        mdextend_pc(reln, forknum, blocknum, buffer, skipFsync);
        return;
    }

    v = _mdfd_getseg(reln, forknum, blocknum, skipFsync, EXTENSION_CREATE);
    if (v == NULL) {
        return;
    }

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
            pfree(path);
            return fd;
        }
        if (check_unlink_rel_hashtbl(reln->smgr_rnode.node, forknum)) {
            ereport(DEBUG1, (errmsg("\"%s\": %m, this relation has been removed", path)));
            pfree(path);
            return fd;
        }
        if ((AmStartupProcess() || AmPageRedoWorker() || AmPageWriterProcess() || AmCheckpointerProcess()) &&
            CheckFileRepairHashTbl(reln->smgr_rnode.node, forknum, 0)) {
            fd = openrepairfile(path, filenode);
            if (fd < 0) {
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not open file %s.repair: %m", path)));
            } else {
                ereport(LOG, (errmsg("[file repair] open repair file %s.repair", path)));
            }
        } else {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not open file \"%s\": %m", path)));
        }
    }

    return fd;
}
static int MdOpenRetryOpenFile(char* path, const RelFileNodeForkNum &filenode, ExtensionBehavior behavior, uint32 flags)
{
    int fd = -1;
    /*
    * During bootstrap, there are cases where a system relation will be
    * accessed (by internal backend processes) before the bootstrap
    * script nominally creates it.  Therefore, accept mdopen() as a
    * substitute for mdcreate() in bootstrap mode only. (See mdcreate)
    */
    if (IsBootstrapProcessingMode()) {
        flags |= (O_CREAT | O_EXCL);
        fd = DataFileIdOpenFile(path, filenode, (int)flags, FILE_RW_PERMISSION);
    }

    if (fd < 0) {
        if (behavior == EXTENSION_RETURN_NULL && FILE_POSSIBLY_DELETED(errno)) {
            return -1;
        }
        if (check_unlink_rel_hashtbl(filenode.rnode.node, filenode.forknumber)) {
            ereport(DEBUG1, (errmsg("\"%s\": %m, this relation has been removed", path)));
            return -1;
        }
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not open file \"%s\": %m", path)));
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
    File fd = -1;
    RelFileNodeForkNum filenode;
    uint32 flags = O_RDWR | PG_BINARY;

    /* No work if already open */
    if (reln->md_fd[forknum]) {
        return reln->md_fd[forknum];
    }

    path = relpath(reln->smgr_rnode, forknum);

    filenode = RelFileNodeForkNumFill(reln->smgr_rnode, forknum, 0);

    File fd_pca = -1;
    File fd_pcd = -1;
    if (IS_COMPRESSED_MAINFORK(reln, forknum)) {
        /* open page compression address file */
        char pcfile_path[MAXPGPATH];
        errno_t rc = snprintf_s(pcfile_path, MAXPGPATH, MAXPGPATH - 1, PCA_SUFFIX, path);
        securec_check_ss(rc, "\0", "\0");
        RelFileNodeForkNum pcaRelFileNode = RelFileNodeForkNumFill(reln->smgr_rnode, PCA_FORKNUM, 0);
        fd_pca = DataFileIdOpenFile(pcfile_path, pcaRelFileNode, flags, FILE_RW_PERMISSION);
        if (fd_pca < 0) {
            fd_pca = MdOpenRetryOpenFile(pcfile_path, pcaRelFileNode, behavior, flags);
            if (fd_pca < 0) {
                pfree(path);
                return NULL;
            }
        }
        /* open page compression data file */
        rc = snprintf_s(pcfile_path, MAXPGPATH, MAXPGPATH - 1, PCD_SUFFIX, path);
        securec_check_ss(rc, "\0", "\0");
        RelFileNodeForkNum pcdRelFileNode = RelFileNodeForkNumFill(reln->smgr_rnode, PCD_FORKNUM, 0);
        fd_pcd = DataFileIdOpenFile(pcfile_path, pcdRelFileNode, flags, FILE_RW_PERMISSION);
        if (fd_pcd < 0) {
            fd_pcd = MdOpenRetryOpenFile(pcfile_path, pcaRelFileNode, behavior, flags);
            if (fd_pca < 0) {
                pfree(path);
                return NULL;
            }
        }
        SetupPageCompressMemoryMap(fd_pca, reln->smgr_rnode.node, filenode);
    } else {

        ADIO_RUN()
        {
            flags |= O_DIRECT;
        }
        ADIO_END();

        fd = DataFileIdOpenFile(path, filenode, (int)flags, 0600);
        if (fd < 0) {
            fd = mdopenagain(reln, forknum, behavior, path);
            if (fd < 0) {
                return NULL;
            }
        }
    }

    pfree(path);

    reln->md_fd[forknum] = mdfd = _fdvec_alloc();

    mdfd->mdfd_vfd = fd;
    mdfd->mdfd_segno = 0;
    mdfd->mdfd_vfd_pca = fd_pca;
    mdfd->mdfd_vfd_pcd = fd_pcd;
    mdfd->mdfd_chain = NULL;
    Assert(_mdnblocks(reln, forknum, mdfd) <= ((BlockNumber)RELSEG_SIZE));

    return mdfd;
}

/*
 *	mdclose() -- Close the specified relation, if it isn't closed already.
 */
void mdclose(SMgrRelation reln, ForkNumber forknum, BlockNumber blockNum)
{
    MdfdVec *v = reln->md_fd[forknum];

    /* No work if already closed */
    if (v == NULL) {
        return;
    }

    reln->md_fd[forknum] = NULL; /* prevent dangling pointer after error */

    while (v != NULL) {
        MdfdVec *ov = v;

        /* if not closed already */
        if (IS_COMPRESSED_MAINFORK(reln, forknum)) {
            if (v->mdfd_vfd_pca >= 0) {
                FileClose(v->mdfd_vfd_pca);
            }
            if (v->mdfd_vfd_pcd >= 0) {
                FileClose(v->mdfd_vfd_pcd);
            }
        } else {
            if (v->mdfd_vfd >= 0) {
                FileClose(v->mdfd_vfd);
            }
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
    off_t seekpos;
    MdfdVec *v = NULL;

    v = _mdfd_getseg(reln, forknum, blocknum, false, EXTENSION_FAIL);
    if (v == NULL) {
        return;
    }

    if (IS_COMPRESSED_MAINFORK(reln, forknum)) {
        int chunk_size = PageCompressChunkSize(reln);
        PageCompressHeader *pcMap = GetPageCompressMemoryMap(v->mdfd_vfd_pca, chunk_size);
        PageCompressAddr *pcAddr = GET_PAGE_COMPRESS_ADDR(pcMap, chunk_size, blocknum);
        /* check chunk number */
        if (pcAddr->nchunks < 0 || pcAddr->nchunks > BLCKSZ / chunk_size) {
            if (u_sess->attr.attr_security.zero_damaged_pages || t_thrd.xlog_cxt.InRecovery) {
                return;
            } else {
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                                errmsg("invalid chunks %u of block %u in file \"%s\"", pcAddr->nchunks, blocknum,
                                       FilePathName(v->mdfd_vfd_pca))));
            }
        }

        for (uint8 i = 0; i < pcAddr->nchunks; i++) {
            if (pcAddr->chunknos[i] <= 0 || pcAddr->chunknos[i] > (uint32)(BLCKSZ / chunk_size) * RELSEG_SIZE) {
                if (u_sess->attr.attr_security.zero_damaged_pages || t_thrd.xlog_cxt.InRecovery) {
                    return;
                } else {
                    ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                                    errmsg("invalid chunk number %u of block %u in file \"%s\"", pcAddr->chunknos[i],
                                           blocknum, FilePathName(v->mdfd_vfd_pca))));
                }
            }
            seekpos = (off_t)OFFSET_OF_PAGE_COMPRESS_CHUNK(chunk_size, pcAddr->chunknos[i]);
            int range = 1;
            while (i < pcAddr->nchunks - 1 && pcAddr->chunknos[i + 1] == pcAddr->chunknos[i] + 1) {
                range++;
                i++;
            }
            (void)FilePrefetch(v->mdfd_vfd_pcd, seekpos, chunk_size * range, WAIT_EVENT_DATA_FILE_PREFETCH);
        }
    } else {
        seekpos = (off_t)BLCKSZ * (blocknum % ((BlockNumber)RELSEG_SIZE));

        Assert(seekpos < (off_t)BLCKSZ * RELSEG_SIZE);

        (void)FilePrefetch(v->mdfd_vfd, seekpos, BLCKSZ, WAIT_EVENT_DATA_FILE_PREFETCH);
    }
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

        if (IS_COMPRESSED_MAINFORK(reln, forknum)) {
            uint32 chunk_size = PageCompressChunkSize(reln);
            PageCompressHeader *pcMap = GetPageCompressMemoryMap(v->mdfd_vfd_pca, chunk_size);
            pc_chunk_number_t seekpos_chunk;
            pc_chunk_number_t last_chunk;
            bool firstEnter = true;
            for (BlockNumber iblock = 0; iblock < nflush; ++iblock) {
                PageCompressAddr *pcAddr = GET_PAGE_COMPRESS_ADDR(pcMap, chunk_size, blocknum + iblock);
                // find continue chunk to write back
                for (uint8 i = 0; i < pcAddr->nchunks; ++i) {
                    if (firstEnter) {
                        seekpos_chunk = pcAddr->chunknos[i];
                        last_chunk = seekpos_chunk;
                        firstEnter = false;
                    } else if (pcAddr->chunknos[i] == last_chunk + 1) {
                        last_chunk++;
                    } else {
                        seekpos = (off_t)OFFSET_OF_PAGE_COMPRESS_CHUNK(chunk_size, seekpos_chunk);
                        pc_chunk_number_t nchunks = last_chunk - seekpos_chunk + 1;
                        FileWriteback(v->mdfd_vfd_pcd, seekpos, (off_t)nchunks * chunk_size);
                        seekpos_chunk = pcAddr->chunknos[i];
                        last_chunk = seekpos_chunk;
                    }
                }
            }
            /* flush the rest chunks */
            if (!firstEnter) {
                seekpos = (off_t)OFFSET_OF_PAGE_COMPRESS_CHUNK(chunk_size, seekpos_chunk);
                pc_chunk_number_t nchunks = last_chunk - seekpos_chunk + 1;
                FileWriteback(v->mdfd_vfd_pcd, seekpos, (off_t)nchunks * chunk_size);
            }
        } else {
            seekpos = (off_t) BLCKSZ * (blocknum % ((BlockNumber) RELSEG_SIZE));
            FileWriteback(v->mdfd_vfd, seekpos, (off_t) BLCKSZ * nflush);
        }
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
 *	mdread_pc() -- Read the specified block from a page compressed relation.
 */
bool mdread_pc(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, char *buffer)
{
    Assert(IS_COMPRESSED_MAINFORK(reln, forknum));

    MdfdVec *v = _mdfd_getseg(reln, forknum, blocknum, false, EXTENSION_FAIL);

    RelFileCompressOption option;
    TransCompressOptions(reln->smgr_rnode.node, &option);
    uint32 chunk_size = CHUNK_SIZE_LIST[option.compressChunkSize];
    uint8 algorithm = option.compressAlgorithm;
    PageCompressHeader *pcMap = GetPageCompressMemoryMap(v->mdfd_vfd_pca, chunk_size);
    PageCompressAddr *pcAddr = GET_PAGE_COMPRESS_ADDR(pcMap, chunk_size, blocknum);
    uint8 nchunks = pcAddr->nchunks;
    if (nchunks == 0) {
        MemSet(buffer, 0, BLCKSZ);
        return true;
    }

    if (nchunks > BLCKSZ / chunk_size) {
        if (u_sess->attr.attr_security.zero_damaged_pages || t_thrd.xlog_cxt.InRecovery) {
            MemSet(buffer, 0, BLCKSZ);
            return true;
        } else {
#ifndef ENABLE_MULTIPLE_NODES
            if (RecoveryInProgress()) {
                return false;
            }
#endif
            ereport(ERROR,
                    (errcode(ERRCODE_DATA_CORRUPTED), errmsg("invalid chunks %u of block %u in file \"%s\"", nchunks,
                                                             blocknum, FilePathName(v->mdfd_vfd_pca))));
        }
    }

    for (auto i = 0; i < nchunks; ++i) {
        if (pcAddr->chunknos[i] <= 0 || pcAddr->chunknos[i] > MAX_CHUNK_NUMBER(chunk_size)) {
            if (u_sess->attr.attr_security.zero_damaged_pages || t_thrd.xlog_cxt.InRecovery) {
                MemSet(buffer, 0, BLCKSZ);
                return true;
            } else {
                check_file_stat(FilePathName(v->mdfd_vfd_pcd));
                force_backtrace_messages = true;
#ifndef ENABLE_MULTIPLE_NODES
                if (RecoveryInProgress()) {
                    return false;
                }
#endif
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("invalid chunks %u of block %u in file \"%s\"",
                                                                        nchunks, blocknum,
                                                                        FilePathName(v->mdfd_vfd_pca))));
            }
        }
    }

    // read chunk data
    char *buffer_pos = NULL;
    uint8 start;
    int read_amount;
    char *compress_buffer = (char*)palloc(chunk_size * nchunks);
    for (uint8 i = 0; i < nchunks; ++i) {
        buffer_pos = compress_buffer + chunk_size * i;
        off_t seekpos = (off_t) OFFSET_OF_PAGE_COMPRESS_CHUNK(chunk_size, pcAddr->chunknos[i]);
        start = i;
        while (i < nchunks - 1 && pcAddr->chunknos[i + 1] == pcAddr->chunknos[i] + 1) {
            i++;
        }
        read_amount = chunk_size * (i - start + 1);
        TRACE_POSTGRESQL_SMGR_MD_READ_START(forknum, blocknum, reln->smgr_rnode.node.spcNode,
                                            reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode,
                                            reln->smgr_rnode.backend);
        int nbytes = FilePRead(v->mdfd_vfd_pcd, buffer_pos, read_amount, seekpos, WAIT_EVENT_DATA_FILE_READ);
        TRACE_POSTGRESQL_SMGR_MD_READ_DONE(forknum, blocknum, reln->smgr_rnode.node.spcNode,
                                           reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode,
                                           reln->smgr_rnode.backend, nbytes, BLCKSZ);

        if (nbytes != read_amount) {
            if (nbytes < 0) {
#ifndef ENABLE_MULTIPLE_NODES
                if (RecoveryInProgress()) {
                    return false;
                }
#endif
                ereport(ERROR,
                        (errcode_for_file_access(), errmsg("could not read block %u in file \"%s\": %m", blocknum,
                                                           FilePathName(v->mdfd_vfd_pcd))));
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
                return true;
            } else {
                check_file_stat(FilePathName(v->mdfd_vfd_pcd));
                force_backtrace_messages = true;

#ifndef ENABLE_MULTIPLE_NODES
                if (RecoveryInProgress()) {
                    return false;
                }
#endif
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg(
                    "could not read block %u in file \"%s\": read only %d of %d bytes", blocknum,
                    FilePathName(v->mdfd_vfd_pcd), nbytes, read_amount)));
            }
        }
    }

    /* decompress chunk data */
    int nbytes;
    if (pcAddr->nchunks == BLCKSZ / chunk_size) {
        error_t rc = memcpy_s(buffer, BLCKSZ, compress_buffer, BLCKSZ);
        securec_check(rc, "", "");
    } else {
        nbytes = DecompressPage(compress_buffer, buffer, algorithm);
        if (nbytes != BLCKSZ) {
            if (nbytes == -2) {
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg(
                    "could not recognized compression algorithm %d for file \"%s\"", algorithm,
                    FilePathName(v->mdfd_vfd_pcd))));
            }
            if (u_sess->attr.attr_security.zero_damaged_pages || t_thrd.xlog_cxt.InRecovery) {
                pfree(compress_buffer);
                MemSet(buffer, 0, BLCKSZ);
                return true;
            } else {
                check_file_stat(FilePathName(v->mdfd_vfd_pcd));
                force_backtrace_messages = true;

#ifndef ENABLE_MULTIPLE_NODES
                if (RecoveryInProgress()) {
                    return false;
                }
#endif
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg(
                    "could not decompress block %u in file \"%s\": decompress %d of %d bytes", blocknum,
                    FilePathName(v->mdfd_vfd_pcd), nbytes, BLCKSZ)));
            }
        }
    }
    pfree(compress_buffer);
    return true;
}

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

    if (IS_COMPRESSED_MAINFORK(reln, forknum)) {
        bool success = mdread_pc(reln, forknum, blocknum, buffer);
        if (success && PageIsVerified((Page)buffer, blocknum)) {
            return SMGR_RD_OK;
        } else {
            return SMGR_RD_CRC_ERROR;
        }
    }

    (void)INSTR_TIME_SET_CURRENT(startTime);

    TRACE_POSTGRESQL_SMGR_MD_READ_START(forknum, blocknum, reln->smgr_rnode.node.spcNode, reln->smgr_rnode.node.dbNode,
                                        reln->smgr_rnode.node.relNode, reln->smgr_rnode.backend);

    v = _mdfd_getseg(reln, forknum, blocknum, false, EXTENSION_FAIL);
    if (v == NULL) {
        return SMGR_RD_NO_BLOCK;
    }

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

    if (PageIsVerified((Page) buffer, blocknum)) {
        return SMGR_RD_OK;
    } else {
        return SMGR_RD_CRC_ERROR;
    }
}

/*
 *	mdwrite_pc() -- Write the supplied block at the appropriate location for page compressed relation.
 *
 *		This is to be used only for updating already-existing blocks of a
 *		relation (ie, those before the current EOF).  To extend a relation,
 *		use mdextend().
 */
static void mdwrite_pc(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, const char *buffer, bool skipFsync)
{
    /* This assert is too expensive to have on normally ... */
#ifdef CHECK_WRITE_VS_EXTEND
    Assert(blocknum < mdnblocks(reln, forknum));
#endif
    Assert(IS_COMPRESSED_MAINFORK(reln, forknum));
    bool mmapSync = false;
    MdfdVec *v = _mdfd_getseg(reln, forknum, blocknum, skipFsync, EXTENSION_FAIL);


    if (check_unlink_rel_hashtbl(reln->smgr_rnode.node, forknum)) {
        ereport(DEBUG1, (errmsg("could not write block %u in file \"%s\": %m, this relation has been removed",
            blocknum, FilePathName(v->mdfd_vfd))));
        /* this file need skip sync */
        return;
    }

    RelFileCompressOption option;
    TransCompressOptions(reln->smgr_rnode.node, &option);
    uint32 chunk_size = CHUNK_SIZE_LIST[option.compressChunkSize];
    uint8 algorithm = option.compressAlgorithm;
    int8 level = option.compressLevelSymbol ? option.compressLevel : -option.compressLevel;
    uint8 prealloc_chunk = option.compressPreallocChunks;

    PageCompressHeader *pcMap = GetPageCompressMemoryMap(v->mdfd_vfd_pca, chunk_size);
    PageCompressAddr *pcAddr = GET_PAGE_COMPRESS_ADDR(pcMap, chunk_size, blocknum);
    Assert(blocknum % RELSEG_SIZE < pg_atomic_read_u32(&pcMap->nblocks));
    auto maxChunkSize = BLCKSZ / chunk_size - 1;
    if (prealloc_chunk > maxChunkSize) {
        prealloc_chunk = maxChunkSize;
    }

    allocate_chunk_check(pcAddr, chunk_size, blocknum, v);

    /* compress page */
    auto work_buffer_size = CompressPageBufferBound(buffer, algorithm);
    if (work_buffer_size < 0) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg(
            "mdwrite_pc unrecognized compression algorithm %d,chunk_size:%ud,level:%d,prealloc_chunk:%ud", algorithm,
            chunk_size, level, prealloc_chunk)));
    }
    char *work_buffer = (char *) palloc(work_buffer_size);
    auto compress_buffer_size = CompressPage(buffer, work_buffer, work_buffer_size, option);
    if (compress_buffer_size < 0) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg(
            "mdwrite_pc unrecognized compression algorithm %d,chunk_size:%ud,level:%d,prealloc_chunk:%ud", algorithm,
            chunk_size, level, prealloc_chunk)));
    }

    uint8 nchunks = (compress_buffer_size - 1) / chunk_size + 1;
    auto bufferSize = chunk_size * nchunks;
    if (bufferSize >= BLCKSZ) {
        /* store original page if can not save space? */
        pfree(work_buffer);
        work_buffer = (char *) buffer;
        nchunks = BLCKSZ / chunk_size;
    } else {
        /* fill zero in the last chunk */
        if ((uint32) compress_buffer_size < bufferSize) {
            auto leftSize = bufferSize - compress_buffer_size;
            errno_t rc = memset_s(work_buffer + compress_buffer_size, leftSize, 0, leftSize);
            securec_check(rc, "", "");
        }
    }

    uint8 need_chunks = prealloc_chunk > nchunks ? prealloc_chunk : nchunks;
    ExtendChunksOfBlock(pcMap, pcAddr, need_chunks, v);

    // write chunks of compressed page
    for (auto i = 0; i < nchunks; ++i) {
        auto buffer_pos = work_buffer + chunk_size * i;
        off_t seekpos = (off_t) OFFSET_OF_PAGE_COMPRESS_CHUNK(chunk_size, pcAddr->chunknos[i]);
        auto start = i;
        while (i < nchunks - 1 && pcAddr->chunknos[i + 1] == pcAddr->chunknos[i] + 1) {
            i++;
        }
        int write_amount = chunk_size * (i - start + 1);

        TRACE_POSTGRESQL_SMGR_MD_WRITE_START(forknum, blocknum, reln->smgr_rnode.node.spcNode,
                                             reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode,
                                             reln->smgr_rnode.backend);
        int nbytes = FilePWrite(v->mdfd_vfd_pcd, buffer_pos, write_amount, seekpos, WAIT_EVENT_DATA_FILE_WRITE);
        TRACE_POSTGRESQL_SMGR_MD_WRITE_DONE(forknum, blocknum, reln->smgr_rnode.node.spcNode,
                                            reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode,
                                            reln->smgr_rnode.backend, nbytes, BLCKSZ);

        if (nbytes != write_amount) {
            if (nbytes < 0) {
                ereport(ERROR,
                        (errcode_for_file_access(), errmsg("could not write block %u in file \"%s\": %m", blocknum,
                                                           FilePathName(v->mdfd_vfd_pcd))));
            }
            /* short write: complain appropriately */
            ereport(ERROR, (errcode(ERRCODE_DISK_FULL), errmsg(
                "could not write block %u in file \"%s\": wrote only %d of %d bytes", blocknum,
                FilePathName(v->mdfd_vfd_pcd), nbytes, BLCKSZ), errhint("Check free disk space.")));
        }
    }

    /* finally update size of this page and global nblocks */
    if (pcAddr->nchunks != nchunks) {
        mmapSync = true;
        pcAddr->nchunks = nchunks;
    }

    /* write checksum */
    if (mmapSync) {
        pcMap->sync = false;
        pcAddr->checksum = AddrChecksum32(blocknum, pcAddr, chunk_size);
    }

    if (work_buffer != buffer) {
        pfree(work_buffer);
    }


    if (!skipFsync && !SmgrIsTemp(reln)) {
        register_dirty_segment(reln, forknum, v);
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

    (void)INSTR_TIME_SET_CURRENT(start_time);

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

    bool compressed = IS_COMPRESSED_MAINFORK(reln, forknum);
    if (compressed) {
        mdwrite_pc(reln, forknum, blocknum, buffer, skipFsync);
    } else {
        seekpos = (off_t)BLCKSZ * (blocknum % ((BlockNumber)RELSEG_SIZE));

        Assert(seekpos < (off_t)BLCKSZ * RELSEG_SIZE);

        nbytes = FilePWrite(v->mdfd_vfd, buffer, BLCKSZ, seekpos, WAIT_EVENT_DATA_FILE_WRITE);

        TRACE_POSTGRESQL_SMGR_MD_WRITE_DONE(forknum, blocknum, reln->smgr_rnode.node.spcNode,
                                            reln->smgr_rnode.node.dbNode, reln->smgr_rnode.node.relNode,
                                            reln->smgr_rnode.backend, nbytes, BLCKSZ);
    }
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
    if (compressed) {
        return;
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
    int chunk_size, i;
    PageCompressHeader *pcMap = NULL;
    PageCompressAddr *pcAddr = NULL;

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

    prior_blocks = 0;
    while (v != NULL) {
        MdfdVec *ov = v;

        if (prior_blocks > nblocks) {
            /*
             * This segment is no longer active (and has already been unlinked
             * from the mdfd_chain). We truncate the file, but do not delete
             * it, for reasons explained in the header comments.
             */
             if (IS_COMPRESSED_MAINFORK(reln, forknum)) {
                chunk_size = PageCompressChunkSize(reln);
                pcMap = GetPageCompressMemoryMap(v->mdfd_vfd_pca, chunk_size);
                pg_atomic_write_u32(&pcMap->nblocks, 0);
                pg_atomic_write_u32(&pcMap->allocated_chunks, 0);
                MemSet((char *)pcMap + SIZE_OF_PAGE_COMPRESS_HEADER_DATA, 0,
                       SIZE_OF_PAGE_COMPRESS_ADDR_FILE(chunk_size) - SIZE_OF_PAGE_COMPRESS_HEADER_DATA);
                pcMap->sync = false;
                if (sync_pcmap(pcMap, WAIT_EVENT_COMPRESS_ADDRESS_FILE_SYNC) != 0) {
                    ereport(data_sync_elevel(ERROR),
                            (errcode_for_file_access(),
                             errmsg("could not msync file \"%s\": %m", FilePathName(v->mdfd_vfd_pca))));
                }
                if (FileTruncate(v->mdfd_vfd_pcd, 0, WAIT_EVENT_DATA_FILE_TRUNCATE) < 0) {
                    if (check_unlink_rel_hashtbl(reln->smgr_rnode.node, forknum)) {
                        ereport(DEBUG1, (errmsg("could not truncate file \"%s\": %m, this relation has been removed",
                                                FilePathName(v->mdfd_vfd_pcd))));
                        FileClose(ov->mdfd_vfd_pcd);
                        pfree(ov);
                        break;
                    }
                    ereport(ERROR, (errcode_for_file_access(),
                                    errmsg("could not truncate file \"%s\": %m", FilePathName(v->mdfd_vfd_pcd))));
                }
            } else {
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
             }

            if (!SmgrIsTemp(reln)) {
                register_dirty_segment(reln, forknum, v);
            }

            v = v->mdfd_chain;
            Assert(ov != reln->md_fd[forknum]); /* we never drop the 1st segment */
            if (IS_COMPRESSED_MAINFORK(reln, forknum)) {
                FileClose(ov->mdfd_vfd_pca);
                FileClose(ov->mdfd_vfd_pcd);
            } else {
                FileClose(ov->mdfd_vfd);
            }
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
            if (IS_COMPRESSED_MAINFORK(reln, forknum)) {
                pc_chunk_number_t max_used_chunkno = (pc_chunk_number_t) 0;
                uint32 allocated_chunks;
                chunk_size = PageCompressChunkSize(reln);
                pcMap = GetPageCompressMemoryMap(v->mdfd_vfd_pca, chunk_size);

                for (BlockNumber blk = last_seg_blocks; blk < RELSEG_SIZE; ++blk) {
                    pcAddr = GET_PAGE_COMPRESS_ADDR(pcMap, chunk_size, blk);
                    pcAddr->nchunks = 0;
                    pcAddr->checksum = AddrChecksum32(blk, pcAddr, chunk_size);
                }
                pg_atomic_write_u32(&pcMap->nblocks, last_seg_blocks);
                pcMap->sync = false;
                if (sync_pcmap(pcMap, WAIT_EVENT_COMPRESS_ADDRESS_FILE_SYNC) != 0) {
                    ereport(data_sync_elevel(ERROR),
                        (errcode_for_file_access(), errmsg("could not msync file \"%s\": %m",
                            FilePathName(v->mdfd_vfd_pca))));
                }
                allocated_chunks = pg_atomic_read_u32(&pcMap->allocated_chunks);
                /* find the max used chunkno */
                for (BlockNumber blk = (BlockNumber) 0; blk < (BlockNumber) last_seg_blocks; blk++) {
                    pcAddr = GET_PAGE_COMPRESS_ADDR(pcMap, chunk_size, blk);
                    /* check allocated_chunks for one page */
                    if (pcAddr->allocated_chunks > BLCKSZ / chunk_size) {
                        ereport(ERROR,
                            (errcode(ERRCODE_DATA_CORRUPTED), errmsg("invalid chunks %u of block %u in file \"%s\"",
                                pcAddr->allocated_chunks, blk,
                                FilePathName(v->mdfd_vfd_pca))));
                    }

                    /* check chunknos for one page */
                    for (i = 0; i < pcAddr->allocated_chunks; i++) {
                        if (pcAddr->chunknos[i] == 0 || pcAddr->chunknos[i] > allocated_chunks) {
                            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg(
                                "invalid chunk number %u of block %u in file \"%s\"", pcAddr->chunknos[i], blk,
                                FilePathName(v->mdfd_vfd_pca))));
                        }

                        if (pcAddr->chunknos[i] > max_used_chunkno) {
                            max_used_chunkno = pcAddr->chunknos[i];
                        }
                    }
                }
                off_t compressedOffset = (off_t)max_used_chunkno * chunk_size;
                if (FileTruncate(v->mdfd_vfd_pcd, compressedOffset, WAIT_EVENT_DATA_FILE_TRUNCATE) < 0) {
                    if (check_unlink_rel_hashtbl(reln->smgr_rnode.node, forknum)) {
                        ereport(DEBUG1, (errmsg("could not truncate file \"%s\": %m, this relation has been removed",
                                                FilePathName(v->mdfd_vfd))));
                        break;
                    }
                    ereport(ERROR, (errcode_for_file_access(), errmsg("could not truncate file \"%s\" to %u blocks: %m",
                                                                      FilePathName(v->mdfd_vfd), nblocks)));
                }
            } else {
                if (FileTruncate(v->mdfd_vfd, (off_t)last_seg_blocks * BLCKSZ, WAIT_EVENT_DATA_FILE_TRUNCATE) < 0) {
                    if (check_unlink_rel_hashtbl(reln->smgr_rnode.node, forknum)) {
                        ereport(DEBUG1, (errmsg("could not truncate file \"%s\": %m, this relation has been removed",
                                                FilePathName(v->mdfd_vfd))));
                        break;
                    }
                    ereport(ERROR, (errcode_for_file_access(), errmsg("could not truncate file \"%s\" to %u blocks: %m",
                                                                      FilePathName(v->mdfd_vfd), nblocks)));
                }
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

static bool CompressMdImmediateSync(SMgrRelation reln, ForkNumber forknum, MdfdVec* v)
{
    PageCompressHeader* pcMap = GetPageCompressMemoryMap(v->mdfd_vfd_pca, PageCompressChunkSize(reln));
    if (sync_pcmap(pcMap, WAIT_EVENT_COMPRESS_ADDRESS_FILE_SYNC) != 0) {
        if (check_unlink_rel_hashtbl(reln->smgr_rnode.node, forknum)) {
            ereport(DEBUG1, (errmsg("could not fsync file \"%s\": %m, this relation has been removed",
                                    FilePathName(v->mdfd_vfd_pca))));
            return false;
        }
        ereport(data_sync_elevel(ERROR),
                (errcode_for_file_access(), errmsg("could not msync file \"%s\": %m", FilePathName(v->mdfd_vfd_pca))));
    }
    if (FileSync(v->mdfd_vfd_pcd, WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC) < 0) {
        if (check_unlink_rel_hashtbl(reln->smgr_rnode.node, forknum)) {
            ereport(DEBUG1, (errmsg("could not fsync file \"%s\": %m, this relation has been removed",
                                    FilePathName(v->mdfd_vfd_pcd))));
            return false;
        }
        ereport(data_sync_elevel(ERROR),
                (errcode_for_file_access(), errmsg("could not fsync file \"%s\": %m", FilePathName(v->mdfd_vfd_pcd))));
    }
    return true;
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
       if (IS_COMPRESSED_MAINFORK(reln, forknum)) {
            if (!CompressMdImmediateSync(reln, forknum, v)) {
                break;
            }
        } else {
           if (FileSync(v->mdfd_vfd, WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC) < 0) {
               if (check_unlink_rel_hashtbl(reln->smgr_rnode.node, forknum)) {
                   ereport(DEBUG1, (errmsg("could not fsync file \"%s\": %m, this relation has been removed",
                                           FilePathName(v->mdfd_vfd))));
                   break;
               }
               ereport(data_sync_elevel(ERROR), (errcode_for_file_access(),
                                                 errmsg("could not fsync file \"%s\": %m", FilePathName(v->mdfd_vfd))));
           }
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

    /* open the file */
    if (RecoveryInProgress() && CheckFileRepairHashTbl(reln->smgr_rnode.node, forknum, segno) &&
        (AmStartupProcess() || AmPageRedoWorker() || AmPageWriterProcess() || AmCheckpointerProcess())) {
        fd = openrepairfile(fullpath, filenode);
        if (fd < 0) {
            pfree(fullpath);
            return NULL;
        } else {
            ereport(LOG, (errmsg("[file repair] open repair file %s.repair", fullpath)));
        }
    } else {
        fd = DataFileIdOpenFile(fullpath, filenode, O_RDWR | PG_BINARY | oflags, 0600);
    }

    if (fd < 0) {
        pfree(fullpath);
        return NULL;
    }
    
    int fd_pca = -1;
    int fd_pcd = -1;
    if (IS_COMPRESSED_MAINFORK(reln, forknum)) {
        FileClose(fd);
        fd = -1;
        fd_pca = OpenPcaFile(fullpath, reln->smgr_rnode, MAIN_FORKNUM, segno, oflags);
        if (fd_pca < 0) {
            pfree(fullpath);
            return NULL;
        }
        fd_pcd = OpenPcdFile(fullpath, reln->smgr_rnode, MAIN_FORKNUM, segno, oflags);
        if (fd_pcd < 0) {
            pfree(fullpath);
            return NULL;
        }
        SetupPageCompressMemoryMap(fd_pca, reln->smgr_rnode.node, filenode);
    }

    pfree(fullpath);

    /* allocate an mdfdvec entry for it */
    v = _fdvec_alloc();

    /* fill the entry */
    v->mdfd_vfd = fd;
    v->mdfd_vfd_pca = fd_pca;
    v->mdfd_vfd_pcd = fd_pcd;
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
    if (IS_COMPRESSED_MAINFORK(reln, forknum)) {
        PageCompressHeader *pcMap = GetPageCompressMemoryMap(seg->mdfd_vfd_pca, PageCompressChunkSize(reln));
        return (BlockNumber) pg_atomic_read_u32(&pcMap->nblocks);
    }
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
    File file = -1;
    File pcaFd = -1;
    File pcdFd = -1;
    int result;
    int savedErrno;
    bool  needClose = false;

    /* Provide the path for informational messages. */
    p = _mdfd_segpath(reln, ftag->forknum, ftag->segno);
    strlcpy(path, p, MAXPGPATH);
    pfree(p);

    /* Try to open the requested segment. */
    v = _mdfd_getseg(reln, ftag->forknum, ftag->segno * (BlockNumber) RELSEG_SIZE,
        false, EXTENSION_RETURN_NULL);
    if (IS_COMPRESSED_RNODE(ftag->rnode, ftag->forknum)) {
        if (v == NULL) {
            pcaFd = OpenPcaFile(path, reln->smgr_rnode, ftag->forknum, ftag->segno);
            if (pcaFd < 0) {
                return -1;
            }
            pcdFd = OpenPcdFile(path, reln->smgr_rnode, ftag->forknum, ftag->segno);
            if (pcdFd < 0) {
                savedErrno = errno;
                FileClose(pcaFd);
                errno = savedErrno;
                return -1;
            }
            needClose = true;
        } else {
            pcaFd = v->mdfd_vfd_pca;
            pcdFd = v->mdfd_vfd_pcd;
        }

        PageCompressHeader *map = GetPageCompressMemoryMap(pcaFd, PageCompressChunkSize(reln));
        result = sync_pcmap(map, WAIT_EVENT_COMPRESS_ADDRESS_FILE_SYNC);
        if (result == 0) {
            result = FileSync(pcdFd, WAIT_EVENT_DATA_FILE_SYNC);
        } else {
            ereport(data_sync_elevel(WARNING),
                    (errcode_for_file_access(), errmsg("could not fsync pcmap \"%s\": %m", path)));
        }
    } else {
        if (v == NULL) {
            RelFileNodeForkNum filenode = RelFileNodeForkNumFill(reln->smgr_rnode, ftag->forknum, ftag->segno);
            uint32 flags = O_RDWR | PG_BINARY;
            file = DataFileIdOpenFile(path, filenode, (int)flags, S_IRUSR | S_IWUSR);
            if (file < 0 &&
                (AmStartupProcess() || AmPageRedoWorker() || AmPageWriterProcess() || AmCheckpointerProcess()) &&
                CheckFileRepairHashTbl(reln->smgr_rnode.node, ftag->forknum, ftag->segno)) {
                const int TEMPLEN = 8;
                char *temppath = (char *)palloc(strlen(path) + TEMPLEN);
                errno_t rc = sprintf_s(temppath, strlen(path) + TEMPLEN, "%s.repair", path);
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
        result = FileSync(file, WAIT_EVENT_DATA_FILE_SYNC);
    }

    /* Try to fsync the file. */
    savedErrno = errno;
    if (needClose) {
        if (IS_COMPRESSED_RNODE(ftag->rnode, ftag->forknum)) {
            FileClose(pcaFd);
            FileClose(pcdFd);
        } else {
            FileClose(file);
        }
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
    char       *p;

    /* Compute the path. */
    p = relpathperm(ftag->rnode, MAIN_FORKNUM);
    strlcpy(path, p, MAXPGPATH);
    pfree(p);

    /* Try to unlink the file. */
    UnlinkCompressedFile(ftag->rnode, MAIN_FORKNUM, path);
    return unlink(path);
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

static int sync_pcmap(PageCompressHeader *pcMap, uint32 wait_event_info)
{
    if (pg_atomic_read_u32(&pcMap->sync) == true) {
        return 0;
    }
    int returnCode;
    uint32 nblocks, allocated_chunks, last_synced_nblocks, last_synced_allocated_chunks;
    nblocks = pg_atomic_read_u32(&pcMap->nblocks);
    allocated_chunks = pg_atomic_read_u32(&pcMap->allocated_chunks);
    last_synced_nblocks = pg_atomic_read_u32(&pcMap->last_synced_nblocks);
    last_synced_allocated_chunks = pg_atomic_read_u32(&pcMap->last_synced_allocated_chunks);
    returnCode = pc_msync(pcMap);
    if (returnCode == 0) {
        if (last_synced_nblocks != nblocks) {
            pg_atomic_write_u32(&pcMap->last_synced_nblocks, nblocks);
        }

        if (last_synced_allocated_chunks != allocated_chunks) {
            pg_atomic_write_u32(&pcMap->last_synced_allocated_chunks, allocated_chunks);
        }
    }
    pcMap->sync = true;
    return returnCode;
}