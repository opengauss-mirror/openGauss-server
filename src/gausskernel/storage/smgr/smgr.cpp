/* -------------------------------------------------------------------------
 *
 * smgr.cpp
 *    public interface routines to storage manager switch.
 *
 *    All file system operations in openGauss dispatch through these
 *    routines.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/smgr/smgr.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "catalog/storage.h"
#include "commands/tablespace.h"
#include "lib/ilist.h"
#include "replication/dataqueue.h"
#include "storage/buf/bufmgr.h"
#include "storage/ipc.h"
#include "storage/smgr/smgr.h"
#include "storage/smgr/segment.h"
#include "threadpool/threadpool.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "postmaster/aiocompleter.h"

/*
 * This struct of function pointers defines the API between smgr.c and
 * any individual storage manager module.  Note that smgr subfunctions are
 * generally expected to report problems via elog(ERROR).  An exception is
 * that smgr_unlink should use elog(WARNING), rather than erroring out,
 * because we normally unlink relations during post-commit/abort cleanup,
 * and so it's too late to raise an error.  Also, various conditions that
 * would normally be errors should be allowed during bootstrap and/or WAL
 * recovery --- see comments in md.c for details.
 */
typedef struct f_smgr {
    void (*smgr_init)(void);     /* may be NULL */
    void (*smgr_shutdown)(void); /* may be NULL */
    void (*smgr_close)(SMgrRelation reln, ForkNumber forknum, BlockNumber blockNum);
    void (*smgr_create)(SMgrRelation reln, ForkNumber forknum, bool isRedo);
    bool (*smgr_exists)(SMgrRelation reln, ForkNumber forknum, BlockNumber blockNum);
    void (*smgr_unlink)(const RelFileNodeBackend &rnode, ForkNumber forknum, bool isRedo, BlockNumber blockNum);
    void (*smgr_extend)(SMgrRelation reln, ForkNumber forknum, BlockNumber blockNum, char *buffer, bool skipFsync);
    void (*smgr_prefetch)(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum);
    SMGR_READ_STATUS (*smgr_read)(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, char *buffer);
    void (*smgr_write)(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, const char *buffer, bool skipFsync);
    void (*smgr_writeback)(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, BlockNumber nblocks);
    BlockNumber (*smgr_nblocks)(SMgrRelation reln, ForkNumber forknum);
    void (*smgr_truncate)(SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks);
    void (*smgr_immedsync)(SMgrRelation reln, ForkNumber forknum);
    void (*smgr_async_read)(SMgrRelation reln, ForkNumber forknum, AioDispatchDesc_t **dList, int32 dn);
    void (*smgr_async_write)(SMgrRelation reln, ForkNumber forknum, AioDispatchDesc_t **dList, int32 dn);
    void (*smgr_move_buckets)(const RelFileNodeBackend &dest, const RelFileNodeBackend &src, List *bList);
} f_smgr;

static const f_smgr smgrsw[] = {
    /* magnetic disk */
    { mdinit,
      NULL,
      mdclose,
      mdcreate,
      mdexists,
      mdunlink,
      mdextend,
      mdprefetch,
      mdread,
      mdwrite,
      mdwriteback,
      mdnblocks,
      mdtruncate,
      mdimmedsync,
      mdasyncread,
      mdasyncwrite,
      NULL
    },

    /* undo file */
    {
        InitUndoFile,
        NULL,
        CloseUndoFile,
        CreateUndoFile,
        CheckUndoFileExists,
        UnlinkUndoFile,
        ExtendUndoFile,
        PrefetchUndoFile,
        ReadUndoFile,
        WriteUndoFile,
        WritebackUndoFile,
        GetUndoFileNblocks,
        NULL,
        NULL
    }, 

    /* segment-page */
    {
        seg_init,
        seg_shutdown,
        seg_close,
        seg_create,
        seg_exists,
        seg_unlink,
        seg_extend,
        seg_prefetch,
        seg_read,
        seg_write,
        seg_writeback,
        seg_nblocks,
        seg_truncate,
        seg_immedsync,
        seg_async_read,
        seg_async_write,
        seg_move_buckets
    },
};

static const int NSmgr = lengthof(smgrsw);
static void push_unlink_rel_one_fork_to_hashtbl(RelFileNode node, ForkNumber forkNum);

static inline int ChooseSmgrManager(RelFileNode rnode)
{
    if (rnode.dbNode == UNDO_DB_OID || rnode.dbNode == UNDO_SLOT_DB_OID) {
        return UNDO_MANAGER;
    } else if (IsSegmentFileNode(rnode)) {
        return SEGMENT_MANAGER;
    } else {
        return MD_MANAGER;
    }
    return MD_MANAGER;
}


/*
 *	smgrinit(), smgrshutdown() -- Initialize or shut down storage
 *								  managers.
 *
 * Note: smgrinit is called during backend startup (normal or standalone
 * case), *not* during postmaster start.  Therefore, any resources created
 * here or destroyed in smgrshutdown are backend-local.
 */
void smgrinit(void)
{
    int i;

    for (i = 0; i < NSmgr; i++) {
        if (smgrsw[i].smgr_init) {
            (*(smgrsw[i].smgr_init))();
        }
    }

    /* register the shutdown proc */
    if (!IS_THREAD_POOL_SESSION || EnableLocalSysCache()) {
        on_proc_exit(smgrshutdown, 0);
    }

    InitSync();
}

/*
 * on_proc_exit hook for smgr cleanup during backend shutdown
 */
void smgrshutdown(int code, Datum arg)
{
    int i;

    for (i = 0; i < NSmgr; i++) {
        if (smgrsw[i].smgr_shutdown) {
            (*(smgrsw[i].smgr_shutdown))();
        }
    }
}

/*
 *  smgropen() -- Return an SMgrRelation object, creating it if need be.
 *
 *      This does not attempt to actually open the underlying file.
 */
SMgrRelation smgropen(const RelFileNode& rnode, BackendId backend, int col /* = 0 */)
{
    RelFileNodeBackend brnode;
    SMgrRelation reln;
    bool found = false;
    int temp = 0;

    /* at least *fdNeeded* is *MAX_FORKNUM* plus 1.
     * for column table also include all the columns.
     */
    Assert(col >= 0);
    int fdNeeded = 1 + MAX_FORKNUM + col;

    if (GetSMgrRelationHash() == NULL) {
        /* First time through: initialize the hash table */
        HASHCTL hashCtrl;

        errno_t rc = memset_s(&hashCtrl, sizeof(hashCtrl), 0, sizeof(hashCtrl));
        securec_check(rc, "", "");
        hashCtrl.keysize = sizeof(RelFileNodeBackend);
        hashCtrl.entrysize = sizeof(SMgrRelationData);
        hashCtrl.hash = tag_hash;
        if (EnableLocalSysCache()) {
            hashCtrl.hcxt = t_thrd.lsc_cxt.lsc->lsc_mydb_memcxt;
            t_thrd.lsc_cxt.lsc->SMgrRelationHash = hash_create("smgr relation table", 400,
                &hashCtrl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
            dlist_init(&t_thrd.lsc_cxt.lsc->unowned_reln);
        } else {
            hashCtrl.hcxt = u_sess->cache_mem_cxt;
            u_sess->storage_cxt.SMgrRelationHash = hash_create("smgr relation table", 400,
                &hashCtrl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
            dlist_init(&u_sess->storage_cxt.unowned_reln);
        }
        
    }

    START_CRIT_SECTION();
    {
        /* Look up or create an entry */
        brnode.node = rnode;
        brnode.backend = backend;
        reln = (SMgrRelation)hash_search(GetSMgrRelationHash(), (void*)&brnode, HASH_ENTER, &found);

        /* Initialize it if not present before */
        if (!found) {
            int colnum;

            /* hash_search already filled in the lookup key */
            reln->smgr_owner = NULL;
            reln->smgr_targblock = InvalidBlockNumber;
            reln->smgr_prevtargblock = InvalidBlockNumber;
            reln->smgr_fsm_nblocks = InvalidBlockNumber;
            reln->smgr_vm_nblocks = InvalidBlockNumber;
            reln->encrypt = false;
            temp = col + 1;
            reln->smgr_bcm_nblocks = (BlockNumber*)MemoryContextAllocZero(
                LocalSmgrStorageMemoryCxt(), temp * sizeof(BlockNumber));
            reln->smgr_bcmarry_size = temp;
            for (colnum = 0; colnum < reln->smgr_bcmarry_size; colnum++) {
                reln->smgr_bcm_nblocks[colnum] = InvalidBlockNumber;
            }

            reln->smgr_which = ChooseSmgrManager(rnode);
            /* used for undofile.c */
            reln->fileState = NULL;

            /* mark it not open */
            reln->seg_space = NULL;
            if (reln->smgr_which == SEGMENT_MANAGER) {
                reln->md_fdarray_size = fdNeeded;
                reln->seg_desc = (struct SegmentDesc **)MemoryContextAllocZero(
                    LocalSmgrStorageMemoryCxt(), fdNeeded * sizeof(struct SegmentDesc *));
                reln->md_fd = NULL;
            } else if (reln->smgr_which == MD_MANAGER) {
                reln->md_fdarray_size = fdNeeded;
                reln->md_fd = (struct _MdfdVec**)MemoryContextAllocZero(
                                    LocalSmgrStorageMemoryCxt(), fdNeeded * sizeof(struct _MdfdVec*));
                reln->seg_desc = NULL;
            } else {
                reln->md_fdarray_size = 1;
                reln->md_fd = NULL;
                reln->seg_desc = NULL;
            }

            /* it has no owner yet */
            dlist_push_tail(getUnownedReln(), &reln->node);

            /* If it is a bucket smgr node, it should be linked in its parent smgr node */
            dlist_init(&reln->bucket_smgr_head);
            if (IsBucketFileNode(rnode)) {
                RelFileNode parent_rnode = rnode;
                parent_rnode.bucketNode = SegmentBktId;
                SMgrRelation parent_smgr = smgropen(parent_rnode, backend);
                dlist_push_tail(&parent_smgr->bucket_smgr_head, &reln->bucket_smgr_node);
            }
        }

        if (reln->smgr_bcmarry_size < col + 1) {
            int old_bcmarry_size = reln->smgr_bcmarry_size;
            temp = reln->smgr_bcmarry_size * 2;
            temp = Max(temp, (col + 1));
            reln->smgr_bcm_nblocks = (BlockNumber *)repalloc((void *)reln->smgr_bcm_nblocks, temp * sizeof(BlockNumber));
            reln->smgr_bcmarry_size = temp;

            for (int colnum = old_bcmarry_size; colnum < reln->smgr_bcmarry_size; colnum++)
                reln->smgr_bcm_nblocks[colnum] = InvalidBlockNumber;
        }

        if (reln->smgr_which == UNDO_MANAGER) {
            fdNeeded = 1;
        }

        if (reln->md_fdarray_size < fdNeeded) {
            Assert(rnode.bucketNode == InvalidBktId);
            int old_fdarray_size = reln->md_fdarray_size;
            temp = reln->md_fdarray_size * 2;
            temp = Max(temp, fdNeeded);
            reln->md_fd = (struct _MdfdVec**)repalloc((void*)reln->md_fd, temp * sizeof(struct _MdfdVec*));
            reln->md_fdarray_size = temp;

            for (int forknum = old_fdarray_size; forknum < reln->md_fdarray_size; forknum++)
                reln->md_fd[forknum] = NULL;
        }
    }
    END_CRIT_SECTION();
    return reln;
}

/*
 * smgrsetowner() -- Establish a long-lived reference to an SMgrRelation object
 *
 * There can be only one owner at a time; this is sufficient since currently
 * the only such owners exist in the relcache.
 */
void smgrsetowner(SMgrRelation *owner, SMgrRelation reln)
{
    /* We don't support "disowning" an SMgrRelation here, use smgrclearowner */
    Assert(owner != NULL);

    /*
     * First, unhook any old owner.  (Normally there shouldn't be any, but it
     * seems possible that this can happen during swap_relation_files()
     * depending on the order of processing.  It's ok to close the old
     * relcache entry early in that case.)
     *
     * If there isn't an old owner, then the reln should be in the unowned
     * list, and we need to remove it.
     */
    if (reln->smgr_owner) {
        *(reln->smgr_owner) = NULL;
    } else {
        dlist_delete(&reln->node);
        DListNodeInit(&reln->node);
    }

    /* Now establish the ownership relationship. */
    reln->smgr_owner = owner;
    *owner = reln;
}

/*
 * smgrclearowner() -- Remove long-lived reference to an SMgrRelation object if one exists
 */
void smgrclearowner(SMgrRelation *owner, SMgrRelation reln)
{
    /* Do nothing if the SMgrRelation object is not owned by the owner */
    if (reln->smgr_owner != owner) {
        return;
    }

    /* unset the owner's reference */
    *owner = NULL;

    /* unset our reference to the owner */
    reln->smgr_owner = NULL;

    dlist_push_tail(getUnownedReln(), &reln->node);
}

/*
 *	smgrexists() -- Does the underlying file for a fork exist?
 */
bool smgrexists(SMgrRelation reln, ForkNumber forknum, BlockNumber blockNum)
{
    if (reln == NULL) {
        return false;
    }
    return (*(smgrsw[reln->smgr_which].smgr_exists))(reln, forknum, blockNum);
}

/*
 * Hashbucket table's SMgrRelation may have children as smgr nodes of each bucket
 */
static bool smgrhaschildern(SMgrRelation reln)
{
    return IsSegmentFileNode(reln->smgr_rnode.node) && !dlist_is_empty(&reln->bucket_smgr_head);
}

/*
 *  smgrclose() -- Close and delete an SMgrRelation object.
 */
void smgrclose(SMgrRelation reln, BlockNumber blockNum)
{
    ereport(DEBUG5, (errmsg("smgr close %p", reln)));
    SMgrRelation* owner = NULL;
    int forknum;

    for (forknum = 0; forknum < (int)(reln->md_fdarray_size); forknum++) {
        (*(smgrsw[reln->smgr_which].smgr_close))(reln, (ForkNumber)forknum, blockNum);
    }
    owner = reln->smgr_owner;

    if (owner == NULL) {
        dlist_delete(&reln->node);
        DListNodeInit(&reln->node);
    }

    if (IsBucketFileNode(reln->smgr_rnode.node)) {
        dlist_delete(&reln->bucket_smgr_node);
    }

    if (smgrhaschildern(reln)) {
        /* If this smgr node is a bucket relation node, it should close all its childern. */
        dlist_mutable_iter iter;
        dlist_foreach_modify(iter, &reln->bucket_smgr_head)
        {
            SMgrRelation rel = dlist_container(SMgrRelationData, bucket_smgr_node, iter.cur);
            smgrclose(rel);
        }
    }

    if (hash_search(GetSMgrRelationHash(), (void *)&(reln->smgr_rnode), HASH_REMOVE, NULL) == NULL) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("SMgrRelation hashtable corrupted")));
    } else {
        pfree_ext(reln->smgr_bcm_nblocks);
        pfree_ext(reln->seg_desc);
        pfree_ext(reln->md_fd);
    }

    /*
     * Unhook the owner pointer, if any.  We do this last since in the remote
     * possibility of failure above, the SMgrRelation object will still exist.
     */
    if (owner != NULL) {
        *owner = NULL;
    }
}

/*
 *  smgrcloseall() -- Close all existing SMgrRelation objects.
 */
void smgrcloseall(void)
{   
    HASH_SEQ_STATUS status;
    SMgrRelation reln;

    /* Nothing to do if hashtable not set up */
    if (GetSMgrRelationHash() == NULL) {
        return;
    }

    bool smgr_has_children = false;
    hash_seq_init(&status, GetSMgrRelationHash());

    while ((reln = (SMgrRelation)hash_seq_search(&status)) != NULL) {
        if (smgrhaschildern(reln)) {
            /* 
             * if the smgr node has children, it may incur close of all its children.
             * we can not close it in the first loop, otherwise the hashtable iterator is broken.
             */
            smgr_has_children = true;
        } else {
            smgrclose(reln);
        }
    }

    if (smgr_has_children) {
        hash_seq_init(&status, GetSMgrRelationHash());

        while ((reln = (SMgrRelation)hash_seq_search(&status)) != NULL) {
            /* In the second loop, there are only hash bucket parent nodes existing in the hashtable. */
            Assert(IsSegmentFileNode(reln->smgr_rnode.node));
            smgrclose(reln);
        }
    }
}
static void smgrcleanbolcknum(SMgrRelation reln)
{
    reln->smgr_targblock = InvalidBlockNumber;
}

void smgrcleanblocknumall(void)
{
    HASH_SEQ_STATUS status;
    SMgrRelation reln;

    /* Nothing to do if hashtable not set up */
    if (GetSMgrRelationHash() == NULL) {
        return;
    }

    hash_seq_init(&status, GetSMgrRelationHash());

    while ((reln = (SMgrRelation)hash_seq_search(&status)) != NULL) {
        smgrcleanbolcknum(reln);
    }
}
/*
 *	smgrclosenode() -- Close SMgrRelation object for given RelFileNode,
 *					   if one exists.
 *
 * This has the same effects as smgrclose(smgropen(rnode)), but it avoids
 * uselessly creating a hashtable entry only to drop it again when no
 * such entry exists already.
 */
void smgrclosenode(const RelFileNodeBackend &rnode)
{
    SMgrRelation reln;

    /* Nothing to do if hashtable not set up */
    if (GetSMgrRelationHash() == NULL) {
        return;
    }

    reln = (SMgrRelation)hash_search(GetSMgrRelationHash(), (void *)&rnode, HASH_FIND, NULL);
    if (reln != NULL) {
        smgrclose(reln);
    }
}

/*
 *  smgrcreate() -- Create a new relation.
 *
 *      Given an already-created (but presumably unused) SMgrRelation,
 *      cause the underlying disk file or other storage for the fork
 *      to be created.
 *
 *      If isRedo is true, it is okay for the underlying file to exist
 *      already because we are in a WAL replay sequence.
 */
void smgrcreate(SMgrRelation reln, ForkNumber forknum, bool isRedo)
{
    /*
     * Exit quickly in WAL replay mode if we've already opened the file. If
     * it's open, it surely must exist.
     */
    if (isRedo && reln->md_fd[forknum] != NULL)
        return;

    /*
     * We may be using the target table space for the first time in this
     * database, so create a per-database subdirectory if needed.
     *
     * XXX this is a fairly ugly violation of module layering, but this seems
     * to be the best place to put the check.  Maybe TablespaceCreateDbspace
     * should be here and not in commands/tablespace.c?  But that would imply
     * importing a lot of stuff that smgr.c oughtn't know, either.
     */
    TablespaceCreateDbspace(reln->smgr_rnode.node.spcNode, 
                            reln->smgr_rnode.node.dbNode,
                            isRedo);

    (*(smgrsw[reln->smgr_which].smgr_create))(reln, forknum, isRedo);
}

/*
 *  smgrdounlink() -- Immediately unlink all forks of a relation.
 *
 *      All forks of the relation are removed from the store. This should
 *      not be used during transactional operations, since it can't be undone.
 *
 *      If isRedo is true, it is okay for the underlying file(s) to be gone
 *      already.
 *
 *      This is equivalent to calling smgrdounlinkfork for each fork, but
 *      it's significantly quicker so should be preferred when possible.
 */
void smgrdounlink(SMgrRelation reln, bool isRedo, BlockNumber blockNum)
{
    RelFileNodeBackend rnode = reln->smgr_rnode;
    int which = reln->smgr_which;
    int forknum;

    /* Close the forks at smgr level */
    for (forknum = 0; forknum < (int)(reln->md_fdarray_size); forknum++) {
        (*(smgrsw[which].smgr_close))(reln, (ForkNumber)forknum, blockNum);
    }

    if (which == UNDO_MANAGER) {
        /* Drop undo buffer is excuted in ReleaseUndoSpace. */
        goto unlink_file;
    }

    /*
     * Get rid of any remaining buffers for the relation.  bufmgr will just
     * drop them without bothering to write the contents.
     */
    DropTempRelFileNodeAllBuffers(rnode);

    /*
     * It'd be nice to tell the stats collector to forget it immediately, too.
     * But we can't because we don't know the OID (and in cases involving
     * relfilenode swaps, it's not always clear which table OID to forget,
     * anyway).
     *
     *
     * Send a shared-inval message to force other backends to close any
     * dangling smgr references they may have for this rel.  We should do this
     * before starting the actual unlinking, in case we fail partway through
     * that step.  Note that the sinval message will eventually come back to
     * this backend, too, and thereby provide a backstop that we closed our
     * own smgr rel.
     */
    CacheInvalidateSmgr(rnode);

    /*
     * Delete the physical file(s).
     *
     * Note: smgr_unlink must treat deletion failure as a WARNING, not an
     * ERROR, because we've already decided to commit or abort the current
     * xact.
     */
unlink_file:
    (*(smgrsw[which].smgr_unlink))(rnode, InvalidForkNumber, isRedo, blockNum);
}

/*
 *	smgrdounlinkfork() -- Immediately unlink one fork of a relation.
 *
 *		The specified fork of the relation is removed from the store.  This
 *		should not be used during transactional operations, since it can't be
 *		undone.
 *
 *		If isRedo is true, it is okay for the underlying file to be gone
 *		already.
 */
void smgrdounlinkfork(SMgrRelation reln, ForkNumber forknum, bool isRedo)
{
    RelFileNodeBackend rnode = reln->smgr_rnode;
    int which = reln->smgr_which;

    /* Close the fork at smgr level */
    (*(smgrsw[which].smgr_close))(reln, forknum, InvalidBlockNumber);

    /*
     * Get rid of any remaining buffers for the fork.  bufmgr will just drop
     * them without bothering to write the contents.
     */
    push_unlink_rel_one_fork_to_hashtbl(rnode.node, forknum);

    /*
     * It'd be nice to tell the stats collector to forget it immediately, too.
     * But we can't because we don't know the OID (and in cases involving
     * relfilenode swaps, it's not always clear which table OID to forget,
     * anyway).
     */
    /*
     * Send a shared-inval message to force other backends to close any
     * dangling smgr references they may have for this rel.  We should do this
     * before starting the actual unlinking, in case we fail partway through
     * that step.  Note that the sinval message will eventually come back to
     * this backend, too, and thereby provide a backstop that we closed our
     * own smgr rel.
     */
    CacheInvalidateSmgr(rnode);

    /*
     * Delete the physical file(s).
     *
     * Note: smgr_unlink must treat deletion failure as a WARNING, not an
     * ERROR, because we've already decided to commit or abort the current
     * xact.
     */
    (*(smgrsw[which].smgr_unlink))(rnode, forknum, isRedo, InvalidBlockNumber);
}

/*
 *	smgrextend() -- Add a new block to a file.
 *
 *		The semantics are nearly the same as smgrwrite(): write at the
 *		specified position.  However, this is to be used for the case of
 *		extending a relation (i.e., blocknum is at or beyond the current
 *		EOF).  Note that we assume writing a block beyond current EOF
 *		causes intervening file space to become filled with zeroes.
 */
void smgrextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
                char *buffer, bool skipFsync)
{
    (*(smgrsw[reln->smgr_which].smgr_extend))(reln, forknum, blocknum, buffer, skipFsync);
}

/*
 *	smgrprefetch() -- Initiate asynchronous read of the specified block of a relation.
 */
void smgrprefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
    (*(smgrsw[reln->smgr_which].smgr_prefetch))(reln, forknum, blocknum);
}

/*
 *	smgrasyncread() -- Initiate asynchronous read of the specified blocks
 *	of a relation.
 */
void smgrasyncread(SMgrRelation reln, ForkNumber forknum, AioDispatchDesc_t **dList, int32 dn)
{
    (*(smgrsw[reln->smgr_which].smgr_async_read))(reln, forknum, dList, dn);
}

/*
 *	smgrasyncwrite() -- Initiate asynchronous write of the specified blocks
 *	of a relation.
 */
void smgrasyncwrite(SMgrRelation reln, ForkNumber forknum, AioDispatchDesc_t **dList, int32 dn)
{
    (*(smgrsw[reln->smgr_which].smgr_async_write))(reln, forknum, dList, dn);
}

/*
 * smgrread() -- read a particular block from a relation into the supplied buffer.
 *
 * This routine is called from the buffer manager in order to
 * instantiate pages in the shared buffer cache.  All storage managers
 * return pages in the format that openGauss expects.
 */
SMGR_READ_STATUS smgrread(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, char *buffer)
{
    return (*(smgrsw[reln->smgr_which].smgr_read))(reln, forknum, blocknum, buffer);
}

/*
 *	smgrwrite() -- Write the supplied buffer out.
 *
 *		This is to be used only for updating already-existing blocks of a
 *		relation (ie, those before the current EOF).  To extend a relation,
 *		use smgrextend().
 *
 *		This is not a synchronous write -- the block is not necessarily
 *		on disk at return, only dumped out to the kernel.  However,
 *		provisions will be made to fsync the write before the next checkpoint.
 *
 *		skipFsync indicates that the caller will make other provisions to
 *		fsync the relation, so we needn't bother.  Temporary relations also
 *		do not require fsync.
 */
void smgrwrite(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, const char *buffer, bool skipFsync)
{
    (*(smgrsw[reln->smgr_which].smgr_write))(reln, forknum, blocknum, buffer, skipFsync);
}

/*
 *	smgrwriteback() -- Trigger kernel writeback for the supplied range of
 *					   blocks.
 */
void smgrwriteback(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, BlockNumber nblocks)
{
    (*(smgrsw[reln->smgr_which].smgr_writeback))(reln, forknum, blocknum, nblocks);
}

/*
 *	smgrnblocks() -- Calculate the number of blocks in the
 *					 supplied relation.
 */
BlockNumber smgrnblocks(SMgrRelation reln, ForkNumber forknum)
{
    BlockNumber result = InvalidBlockNumber;

    result = (*(smgrsw[reln->smgr_which].smgr_nblocks))(reln, forknum);
    if (forknum == MAIN_FORKNUM) {
        reln->smgr_cached_nblocks = result;
    }

    return result;
}

/*
 * smgrnblocks_cached() -- Get the cached number of blocks in the supplied
 *                          relation.
 *
 * Returns an InvalidBlockNumber when not in recovery and when the relation
 * fork size is not cached. Now, we only support cache main fork.
 */
BlockNumber
smgrnblocks_cached(SMgrRelation reln, ForkNumber forknum)
{
    /*
     * For now, we only use cached values in recovery due to lack of a shared
     * invalidation mechanism for changes in file size.
     */
    if (RecoveryInProgress() && forknum == MAIN_FORKNUM &&
        reln->smgr_cached_nblocks != InvalidBlockNumber) {
        return reln->smgr_cached_nblocks;
    }

    return InvalidBlockNumber;
}

BlockNumber smgrtotalblocks(SMgrRelation reln, ForkNumber forknum)
{
    Assert(IsSegmentSmgrRelation(reln));
    if (forknum == INIT_FORKNUM) {
        return 0;
    }
    return seg_totalblocks(reln, forknum);
}

void smgrtruncatefunc(SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks)
{
    CacheInvalidateSmgr(reln->smgr_rnode);

    (*(smgrsw[reln->smgr_which].smgr_truncate))(reln, forknum, nblocks);
}

/*
 *	smgrtruncate() -- Truncate supplied relation to the specified number
 *					  of blocks
 *
 * The truncation is done immediately, so this can't be rolled back.
 */
void smgrtruncate(SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks)
{
    /*
     * Get rid of any buffers for the about-to-be-deleted blocks. bufmgr will
     * just drop them without bothering to write the contents.
     */
    DropRelFileNodeBuffers(reln->smgr_rnode, forknum, nblocks);

    /*
     * This relfilenode will be truncated, so we should invaild the blocks at
     * the bcm element array.
     */
    if (forknum == BCM_FORKNUM)
        BCMArrayDropAllBlocks(reln->smgr_rnode.node);

    /* Make the cached size is invalid if we encounter an error. */
    reln->smgr_cached_nblocks = InvalidBlockNumber;

    /*
     * Send a shared-inval message to force other backends to close any smgr
     * references they may have for this rel.  This is useful because they
     * might have open file pointers to segments that got removed, and/or
     * smgr_targblock variables pointing past the new rel end.	(The inval
     * message will come back to our backend, too, causing a
     * probably-unnecessary local smgr flush.  But we don't expect that this
     * is a performance-critical path.)  As in the unlink code, we want to be
     * sure the message is sent before we start changing things on-disk.
     */
    smgrtruncatefunc(reln, forknum, nblocks);
}

/*
 *	smgrimmedsync() -- Force the specified relation to stable storage.
 *
 *		Synchronously force all previous writes to the specified relation
 *		down to disk.
 *
 *		This is useful for building completely new relations (eg, new
 *		indexes).  Instead of incrementally WAL-logging the index build
 *		steps, we can just write completed index pages to disk with smgrwrite
 *		or smgrextend, and then fsync the completed index file before
 *		committing the transaction.  (This is sufficient for purposes of
 *		crash recovery, since it effectively duplicates forcing a checkpoint
 *		for the completed index.  But it is *not* sufficient if one wishes
 *		to use the WAL log for PITR or replication purposes: in that case
 *		we have to make WAL entries as well.)
 *
 *		The preceding writes should specify skipFsync = true to avoid
 *		duplicative fsyncs.
 *
 *		Note that you need to do FlushRelationBuffers() first if there is
 *		any possibility that there are dirty buffers for the relation;
 *		otherwise the sync is not very meaningful.
 */
void smgrimmedsync(SMgrRelation reln, ForkNumber forknum)
{
    (*(smgrsw[reln->smgr_which].smgr_immedsync))(reln, forknum);
}


/*
 *	smgrmovebuckets() -- Move buckets between two relation.
 */
void smgrmovebuckets(SMgrRelation reln1, SMgrRelation reln2, List *bList)
{
    (*(smgrsw[reln1->smgr_which].smgr_move_buckets))(reln1->smgr_rnode, reln2->smgr_rnode, bList);
}

void partition_create_new_storage(Relation rel, Partition part, const RelFileNodeBackend &filenode)
{
    if (RelationIsCrossBucketIndex(rel) || IsCreatingCrossBucketIndex(part)) {
        RelationData dummyrel;
        dummyrel.rd_id = InvalidOid;
        dummyrel.newcbi = true;
        RelationCreateStorage(filenode.node, rel->rd_rel->relpersistence, rel->rd_rel->relowner, rel->rd_bucketoid,
                              &dummyrel);
    } else {
        RelationCreateStorage(filenode.node, rel->rd_rel->relpersistence, rel->rd_rel->relowner, rel->rd_bucketoid);
    }
    smgrclosenode(filenode);

    /*
     * Schedule unlinking of the old storage at transaction commit.
     */
    if (!u_sess->attr.attr_storage.enable_recyclebin) {
        PartitionDropStorage(rel, part);
    }
}

/*
 * AtEOXact_SMgr
 *
 * This routine is called during transaction commit or abort (it doesn't
 * particularly care which).  All transient SMgrRelation objects are closed.
 *
 * We do this as a compromise between wanting transient SMgrRelations to
 * live awhile (to amortize the costs of blind writes of multiple blocks)
 * and needing them to not live forever (since we're probably holding open
 * a kernel file descriptor for the underlying file, and we need to ensure
 * that gets closed reasonably soon if the file gets deleted).
 */
void AtEOXact_SMgr(void)
{
    /*
     * We rely on smgrclose() to remove each one from the list.
     *
     * We do not use dlist_foreach_modify because closing hash bucket parent nodes
     * incurs closing all its children node. dlist_foreach_modify does not allow
     * removing the adjacent nodes.
     */
    dlist_head *unowned_reln = getUnownedReln();
    while (!dlist_is_empty(unowned_reln)) {
        dlist_node *cur = unowned_reln->head.next;
        SMgrRelation rel = dlist_container(SMgrRelationData, node, cur);
        Assert(rel->smgr_owner == NULL);
        smgrclose(rel);
    }
}

ScalarToDatum GetTransferFuncByTypeOid(Oid attTypeOid)
{
    switch (attTypeOid) {
        case BPCHAROID:
        case TEXTOID:
        case VARCHAROID: {
            return convertScalarToDatumT<VARCHAROID>;
        }
        case TIMETZOID:
        case TINTERVALOID:
        case INTERVALOID:
        case NAMEOID: {
            return convertScalarToDatumT<TIMETZOID>;
        }
        case UNKNOWNOID:
        case CSTRINGOID: {
            return convertScalarToDatumT<UNKNOWNOID>;
        }
        default: {
            return convertScalarToDatumT<-2>;
        }
    }
}

static void push_unlink_rel_one_fork_to_hashtbl(RelFileNode node, ForkNumber forkNum)
{
    HTAB *relfilenode_hashtbl = g_instance.bgwriter_cxt.unlink_rel_fork_hashtbl;
    bool found = false;
    ForkRelFileNode key;
    DelForkFileTag *entry = NULL;
    uint del_rel_num = 0;

    key.rnode = node;
    key.forkNum = forkNum;

    LWLockAcquire(g_instance.bgwriter_cxt.rel_one_fork_hashtbl_lock, LW_EXCLUSIVE);
    entry = (DelForkFileTag*)hash_search(relfilenode_hashtbl, &(key), HASH_ENTER, &found);
    if (!found) {
        entry->forkrnode.rnode.spcNode = key.rnode.spcNode;
        entry->forkrnode.rnode.dbNode = key.rnode.dbNode;
        entry->forkrnode.rnode.relNode = key.rnode.relNode;
        entry->forkrnode.rnode.bucketNode = key.rnode.bucketNode;
        entry->forkrnode.rnode.opt = key.rnode.opt;
        entry->forkrnode.forkNum = key.forkNum;
        entry->maxSegNo = -1;
        del_rel_num++;
    }
    LWLockRelease(g_instance.bgwriter_cxt.rel_one_fork_hashtbl_lock);

    if (del_rel_num > 0 && g_instance.bgwriter_cxt.invalid_buf_proc_latch != NULL) {
        SetLatch(g_instance.bgwriter_cxt.invalid_buf_proc_latch);
    }
    return;
}

