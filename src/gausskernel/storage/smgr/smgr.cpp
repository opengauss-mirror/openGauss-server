/* -------------------------------------------------------------------------
 *
 * smgr.cpp
 *    public interface routines to storage manager switch.
 *
 *    All file system operations in POSTGRES dispatch through these
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
#include "storage/smgr.h"
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
    void (*smgr_close)(SMgrRelation reln, ForkNumber forknum);
    void (*smgr_create)(SMgrRelation reln, ForkNumber forknum, bool isRedo);
    bool (*smgr_exists)(SMgrRelation reln, ForkNumber forknum);
    void (*smgr_unlink)(const RelFileNodeBackend &rnode, ForkNumber forknum, bool isRedo);
    void (*smgr_extend)(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, const char *buffer,
                        bool skipFsync);
    void (*smgr_prefetch)(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum);
    void (*smgr_read)(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, char* buffer);
    void (*smgr_write)(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, const char *buffer, bool skipFsync);
    void (*smgr_writeback)(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, BlockNumber nblocks);
    BlockNumber (*smgr_nblocks)(SMgrRelation reln, ForkNumber forknum);
    void (*smgr_truncate)(SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks);
    void (*smgr_immedsync)(SMgrRelation reln, ForkNumber forknum);
    void (*smgr_pre_ckpt)(void);  /* may be NULL */
    void (*smgr_sync)(void);      /* may be NULL */
    void (*smgr_post_ckpt)(void); /* may be NULL */
    void (*smgr_async_read)(SMgrRelation reln, ForkNumber forknum, AioDispatchDesc_t **dList, int32 dn);
    void (*smgr_async_write)(SMgrRelation reln, ForkNumber forknum, AioDispatchDesc_t **dList, int32 dn);
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
      mdpreckpt,
      mdsync,
      mdpostckpt,
      mdasyncread,
      mdasyncwrite }
};

static const int NSmgr = lengthof(smgrsw);

/* local function prototypes */
static void smgrshutdown(int code, Datum arg);

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
    if (!IS_THREAD_POOL_SESSION) {
        on_proc_exit(smgrshutdown, 0);
    }
}

/*
 * on_proc_exit hook for smgr cleanup during backend shutdown
 */
static void smgrshutdown(int code, Datum arg)
{
    int i;

    for (i = 0; i < NSmgr; i++) {
        if (smgrsw[i].smgr_shutdown) {
            (*(smgrsw[i].smgr_shutdown))();
        }
    }
}

static inline HTAB *_smgr_hashtbl_create(const char *tabname, long nelem)
{
    HASHCTL hashCtrl;

    errno_t rc = memset_s(&hashCtrl, sizeof(hashCtrl), 0, sizeof(hashCtrl));
    securec_check(rc, "", "");
    hashCtrl.keysize = sizeof(RelFileNodeBackend);
    hashCtrl.entrysize = sizeof(SMgrRelationData);
    hashCtrl.hash = tag_hash;
    hashCtrl.hcxt = (MemoryContext)u_sess->cache_mem_cxt;

    return hash_create(tabname, nelem, &hashCtrl, (HASH_CONTEXT | HASH_FUNCTION | HASH_ELEM));
}

static void _smgr_init(SMgrRelation reln, int col = 0)
{
    /* hash_search already filled in the lookup key */
    reln->smgr_owner = NULL;
    reln->smgr_targblock = InvalidBlockNumber;
    reln->smgr_fsm_nblocks = InvalidBlockNumber;
    reln->smgr_vm_nblocks = InvalidBlockNumber;
    reln->smgr_cached_nblocks = InvalidBlockNumber;

    reln->smgr_which = 0; /* we only have md.c at present */

    if (reln->smgr_rnode.node.bucketNode != DIR_BUCKET_ID) {
        int bcmarray_size = col + 1;
        reln->smgr_bcm_nblocks = (BlockNumber *)MemoryContextAllocZero(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), bcmarray_size * sizeof(BlockNumber));
        reln->smgr_bcmarry_size = bcmarray_size;

        for (int colnum = 0; colnum < reln->smgr_bcmarry_size; colnum++)
            reln->smgr_bcm_nblocks[colnum] = InvalidBlockNumber;

        /* mark it not open */
        int fd_needed_cnt = 1 + MAX_FORKNUM + col;
        reln->md_fdarray_size = fd_needed_cnt;
        reln->md_fd = (struct _MdfdVec **)MemoryContextAllocZero(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), fd_needed_cnt * sizeof(struct _MdfdVec *));
        for (int forknum = 0; forknum < reln->md_fdarray_size; forknum++) {
            reln->md_fd[forknum] = NULL;
        }

        reln->bucketnodes_smgrhash = NULL;
    } else {
        reln->smgr_bcmarry_size = 0;
        reln->smgr_bcm_nblocks = NULL;

        reln->md_fdarray_size = 0;
        reln->md_fd = NULL;

        /* initialize the bucket nodes smgr hash table for bucket dir node */
        reln->bucketnodes_smgrhash = _smgr_hashtbl_create("smgr bucketnodes hashtbl", 400);
    }
}

static inline SMgrRelation _smgr_get_upper_reln(const RelFileNode &rnode, BackendId backend, int col = 0)
{
    RelFileNodeBackend brnode;
    bool found = false;

    brnode.node = rnode;
    brnode.backend = backend;

    /* change hashbucket node for find smgr in global hash table */
    if (brnode.node.bucketNode > InvalidBktId) {
        brnode.node.bucketNode = DIR_BUCKET_ID;
    }

    SMgrRelation upper_reln = (SMgrRelation)hash_search(u_sess->storage_cxt.SMgrRelationHash, (void *)&brnode,
                                                        HASH_ENTER, &found);

    /* Initialize it if not present before */
    if (!found) {
        _smgr_init(upper_reln, col);
        /* Open the upper relation, so it has no owner yet */
        dlist_push_tail(&u_sess->storage_cxt.unowned_reln, &upper_reln->node);
    }

    return upper_reln;
}

static void _smgr_insert_bucketnodes_table(SMgrRelation upper_reln, const oidvector *bucketlist = NULL)
{
    if (upper_reln->smgr_rnode.node.bucketNode == InvalidBktId) {
        Assert(bucketlist == NULL);
        return;
    }

    Assert(upper_reln->smgr_rnode.node.bucketNode == DIR_BUCKET_ID);
    Assert(upper_reln->bucketnodes_smgrhash != NULL);
    if (bucketlist == NULL) {
        return;
    }

    RelFileNodeBackend rnode_tmp = upper_reln->smgr_rnode;
    for (int i = 0; i < bucketlist->dim1; i++) {
        bool found_inner = false;
        rnode_tmp.node.bucketNode = bucketlist->values[i];
        SMgrRelation bktnode_smgr = (SMgrRelation)hash_search(upper_reln->bucketnodes_smgrhash, (void *)&rnode_tmp,
                                                              HASH_ENTER, &found_inner);
        if (found_inner == true) {
            continue;
        }

        _smgr_init(bktnode_smgr);
        Assert(bktnode_smgr->bucketnodes_smgrhash == NULL);
    }
}

/*
 *  smgropen() -- Return an SMgrRelation object, creating it if need be.
 *
 *      This does not attempt to actually open the underlying file.
 */
SMgrRelation smgropen(const RelFileNode &rnode, BackendId backend, int col /* = 0 */,
                      const oidvector *bucketlist /* = null */)
{
    SMgrRelation upper_reln = NULL;
    SMgrRelation reln = NULL;
    int temp = 0;
    bool found = false;

    /* at least *fdNeeded* is *MAX_FORKNUM* plus 1.
     * for column table also include all the columns.
     */
    Assert(col >= 0);
    int fdNeeded = 1 + MAX_FORKNUM + col;

    if (u_sess->storage_cxt.SMgrRelationHash == NULL) {
        /* First time through: initialize the hash table */
        u_sess->storage_cxt.SMgrRelationHash = _smgr_hashtbl_create("smgr relation table", 400);
        dlist_init(&u_sess->storage_cxt.unowned_reln);
    }

    START_CRIT_SECTION();
    {
        /* Look up or create an smgr entry. Guranted that the smgr has been initialized */
        upper_reln = _smgr_get_upper_reln(rnode, backend, col);

        _smgr_insert_bucketnodes_table(upper_reln, bucketlist);

        if (rnode.bucketNode > InvalidBktId) {
            /* get the smgr of this specific bucket node from bucketnodes table in the upper bucket dir smgr */
            RelFileNodeBackend bkt_brnode;
            bkt_brnode.node = rnode;
            bkt_brnode.backend = backend;
            reln = (SMgrRelation)hash_search(upper_reln->bucketnodes_smgrhash, (void *)&bkt_brnode, HASH_ENTER, &found);
            if (found == false) {
                _smgr_init(reln, col);
                Assert(reln->bucketnodes_smgrhash == NULL);
            }
        } else {
            reln = upper_reln;
        }

        /* Bucket Dir Smgr need not handle this */
        if (reln->smgr_bcmarry_size < col + 1 && !BUCKET_ID_IS_DIR(rnode.bucketNode)) {
            Assert(rnode.bucketNode == InvalidBktId);
            int old_bcmarry_size = reln->smgr_bcmarry_size;
            temp = reln->smgr_bcmarry_size * 2;
            temp = Max(temp, (col + 1));
            reln->smgr_bcmarry_size = temp;
            reln->smgr_bcm_nblocks = (BlockNumber *)repalloc((void *)reln->smgr_bcm_nblocks,
                                                             temp * sizeof(BlockNumber));
            for (int colnum = old_bcmarry_size; colnum < reln->smgr_bcmarry_size; colnum++)
                reln->smgr_bcm_nblocks[colnum] = InvalidBlockNumber;
        }

        if (reln->md_fdarray_size < fdNeeded && !BUCKET_ID_IS_DIR(rnode.bucketNode)) {
            Assert(rnode.bucketNode == InvalidBktId);
            int old_fdarray_size = reln->md_fdarray_size;
            temp = reln->md_fdarray_size * 2;
            temp = Max(temp, fdNeeded);
            reln->md_fdarray_size = temp;
            reln->md_fd = (struct _MdfdVec **)repalloc((void *)reln->md_fd, temp * sizeof(struct _MdfdVec *));
            for (int forknum = old_fdarray_size; forknum < reln->md_fdarray_size; forknum++) {
                reln->md_fd[forknum] = NULL;
            }
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
    } else if (reln->smgr_rnode.node.bucketNode == DIR_BUCKET_ID || reln->smgr_rnode.node.bucketNode == InvalidBktId) {
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
    if (reln->smgr_rnode.node.bucketNode == DIR_BUCKET_ID || reln->smgr_rnode.node.bucketNode == InvalidBktId) {
        dlist_push_tail(&u_sess->storage_cxt.unowned_reln, &reln->node);
    }
}

/*
 *	smgrexists() -- Does the underlying file for a fork exist?
 */
bool smgrexists(SMgrRelation reln, ForkNumber forknum)
{
    if (reln == NULL) {
        return false;
    }
    return (*(smgrsw[reln->smgr_which].smgr_exists))(reln, forknum);
}

static inline void _smgr_close_normal_table_smgr(SMgrRelation reln)
{
    Assert(reln->smgr_rnode.node.bucketNode == InvalidBktId);
    Assert(reln->bucketnodes_smgrhash == NULL);

    SMgrRelation *owner = NULL;
    owner = reln->smgr_owner;

    for (int forknum = 0; forknum < (int)(reln->md_fdarray_size); forknum++) {
        (*(smgrsw[reln->smgr_which].smgr_close))(reln, (ForkNumber)forknum);
    }

    if (owner == NULL) {
        dlist_delete(&reln->node);
        DListNodeInit(&reln->node);
    }

    if (hash_search(u_sess->storage_cxt.SMgrRelationHash, (void *)&(reln->smgr_rnode), HASH_REMOVE, NULL) == NULL) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("SMgrRelation hashtable corrupted")));
    }

    pfree_ext(reln->smgr_bcm_nblocks);
    pfree_ext(reln->md_fd);

    /*
     * Unhook the owner pointer, if any.  We do this last since in the remote
     * possibility of failure above, the SMgrRelation object will still exist.
     */
    if (owner != NULL) {
        *owner = NULL;
    }
}

static void _smgr_close_bktnode_smgr(SMgrRelation bktnode_smgr)
{
    Assert(bktnode_smgr->smgr_rnode.node.bucketNode > InvalidBktId);
    Assert(bktnode_smgr->bucketnodes_smgrhash == NULL);

    SMgrRelation *owner = NULL;
    owner = bktnode_smgr->smgr_owner;

    RelFileNodeBackend relnode = bktnode_smgr->smgr_rnode;
    relnode.node.bucketNode = DIR_BUCKET_ID;

    SMgrRelation upper_reln = (SMgrRelation)hash_search(u_sess->storage_cxt.SMgrRelationHash, (void *)&(relnode),
                                                        HASH_FIND, NULL);
    if (upper_reln == NULL) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("SMgrRelation hashtable not find")));
    }

    for (int forknum = 0; forknum < (int)(bktnode_smgr->md_fdarray_size); forknum++) {
        (*(smgrsw[bktnode_smgr->smgr_which].smgr_close))(bktnode_smgr, (ForkNumber)forknum);
    }

    Assert(upper_reln->bucketnodes_smgrhash != NULL);
    SMgrRelation match_reln = (SMgrRelation)hash_search(upper_reln->bucketnodes_smgrhash,
                                                        (void *)&(bktnode_smgr->smgr_rnode), HASH_REMOVE, NULL);
    if (match_reln == NULL || match_reln != bktnode_smgr) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                        errmsg("Bktnode smgr not found in bktnodes hastbl of upper relation")));
    }

    pfree_ext(bktnode_smgr->smgr_bcm_nblocks);
    pfree_ext(bktnode_smgr->md_fd);

    /*
     * Unhook the owner pointer, if any.  We do this last since in the remote
     * possibility of failure above, the SMgrRelation object will still exist.
     */
    if (owner != NULL) {
        *owner = NULL;
    }
}

static void _smgr_close_btkdir_smgr(SMgrRelation reln)
{
    Assert(reln->smgr_rnode.node.bucketNode == DIR_BUCKET_ID);
    Assert(reln->bucketnodes_smgrhash != NULL);
    Assert(reln->md_fd == NULL);

    SMgrRelation *owner = NULL;
    owner = reln->smgr_owner;

    HASH_SEQ_STATUS status;
    SMgrRelation item;
    hash_seq_init(&status, reln->bucketnodes_smgrhash);
    while ((item = (SMgrRelation)hash_seq_search(&status)) != NULL) {
        Assert(item->smgr_rnode.node.bucketNode > InvalidBktId);
        Assert(item->bucketnodes_smgrhash == NULL);
        _smgr_close_bktnode_smgr(item);
    }

    if (owner == NULL) {
        dlist_delete(&reln->node);
        DListNodeInit(&reln->node);
    }

    /* move dir smgr at last */
    if (hash_search(u_sess->storage_cxt.SMgrRelationHash, (void *)&(reln->smgr_rnode), HASH_REMOVE, NULL) == NULL)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("SMgrRelation hashtable corrupted")));

    hash_destroy(reln->bucketnodes_smgrhash);
    reln->bucketnodes_smgrhash = NULL;

    /*
     * Unhook the owner pointer, if any.  We do this last since in the remote
     * possibility of failure above, the SMgrRelation object will still exist.
     */
    if (owner != NULL) {
        *owner = NULL;
    }
}

/*
 *  smgrclose() -- Close and delete an SMgrRelation object.
 */
void smgrclose(SMgrRelation reln)
{
    if (reln == nullptr) {
        return;
    }

    if(reln->smgr_rnode.node.bucketNode == InvalidBktId) {
        _smgr_close_normal_table_smgr(reln);
    } else if (reln->smgr_rnode.node.bucketNode > InvalidBktId) {
        _smgr_close_bktnode_smgr(reln);
    } else if (reln->smgr_rnode.node.bucketNode == DIR_BUCKET_ID) {
        _smgr_close_btkdir_smgr(reln);
    } else {
        ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED), errmsg("unknow bucketnode %d.", reln->smgr_rnode.node.bucketNode)));
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
    if (u_sess->storage_cxt.SMgrRelationHash == NULL) {
        return;
    }

    hash_seq_init(&status, u_sess->storage_cxt.SMgrRelationHash);

    while ((reln = (SMgrRelation)hash_seq_search(&status)) != NULL) {
        Assert(reln->smgr_rnode.node.bucketNode <= InvalidBktId);
        smgrclose(reln);
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
    if (u_sess->storage_cxt.SMgrRelationHash == NULL) {
        return;
    }

    reln = (SMgrRelation)hash_search(u_sess->storage_cxt.SMgrRelationHash, (void *)&rnode, HASH_FIND, NULL);
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
    int which = reln->smgr_which;
    /*
     * We may be using the target table space for the first time in this
     * database, so create a per-database subdirectory if needed.
     *
     * XXX this is a fairly ugly violation of module layering, but this seems
     * to be the best place to put the check.  Maybe TablespaceCreateDbspace
     * should be here and not in commands/tablespace.c?  But that would imply
     * importing a lot of stuff that smgr.c oughtn't know, either.
     */
    TablespaceCreateDbspace(reln->smgr_rnode.node, isRedo);

    (*(smgrsw[which].smgr_create))(reln, (ForkNumber)forknum, isRedo);
}

void smgrcreatebuckets(SMgrRelation reln, ForkNumber forknum, bool isRedo)
{
    int which = reln->smgr_which;
    Assert(reln->smgr_rnode.node.bucketNode == DIR_BUCKET_ID);
    Assert(reln->bucketnodes_smgrhash != NULL);
    Assert(reln->md_fd == NULL);

    HASH_SEQ_STATUS status;
    SMgrRelation item = NULL;
    hash_seq_init(&status, reln->bucketnodes_smgrhash);
    while ((item = (SMgrRelation)hash_seq_search(&status)) != NULL) {
        Assert(item->smgr_rnode.node.bucketNode > InvalidBktId);
        (*(smgrsw[which].smgr_create))(item, (ForkNumber)forknum, isRedo);
    }
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
void smgrdounlink(SMgrRelation reln, bool isRedo)
{
    RelFileNodeBackend rnode = reln->smgr_rnode;
    int which = reln->smgr_which;
    int forknum;

    /* Close the forks at smgr level */
    if (rnode.node.bucketNode == DIR_BUCKET_ID) {
        Assert(reln->md_fd == NULL);
        Assert(reln->bucketnodes_smgrhash != NULL);
        HASH_SEQ_STATUS status;
        SMgrRelation item = NULL;
        hash_seq_init(&status, reln->bucketnodes_smgrhash);
        while ((item = (SMgrRelation)hash_seq_search(&status)) != NULL) {
            Assert(item->smgr_rnode.node.bucketNode > InvalidBktId);
            for (forknum = 0; forknum < (int)(item->md_fdarray_size); forknum++) {
                (*(smgrsw[which].smgr_close))(item, (ForkNumber)forknum);
            }
        }
    } else {
        for (forknum = 0; forknum < (int)(reln->md_fdarray_size); forknum++) {
            (*(smgrsw[which].smgr_close))(reln, (ForkNumber)forknum);
        }
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
    if (rnode.node.bucketNode == DIR_BUCKET_ID) {
        /* rm bucket dir directly */
        (*(smgrsw[which].smgr_unlink))(rnode, MAIN_FORKNUM, isRedo);
    } else {
        (*(smgrsw[which].smgr_unlink))(rnode, InvalidForkNumber, isRedo);
    }
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
    (*(smgrsw[which].smgr_close))(reln, forknum);

    /*
     * Get rid of any remaining buffers for the fork.  bufmgr will just drop
     * them without bothering to write the contents.
     */
    DropRelFileNodeBuffers(rnode, forknum, 0);

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
    (*(smgrsw[which].smgr_unlink))(rnode, forknum, isRedo);
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
void smgrextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, const char *buffer, bool skipFsync)
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
 * return pages in the format that POSTGRES expects.
 */
void smgrread(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, char* buffer)
{
    (*(smgrsw[reln->smgr_which].smgr_read))(reln, forknum, blocknum, buffer);
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
 *	smgrpreckpt() -- Prepare for checkpoint.
 */
void smgrpreckpt(void)
{
    int i;

    for (i = 0; i < NSmgr; i++) {
        if (smgrsw[i].smgr_pre_ckpt) {
            (*(smgrsw[i].smgr_pre_ckpt))();
        }
    }
}

/*
 *	smgrsync() -- Sync files to disk during checkpoint.
 */
void smgrsync(void)
{
    int i;

    for (i = 0; i < NSmgr; i++) {
        if (smgrsw[i].smgr_sync) {
            (*(smgrsw[i].smgr_sync))();
        }
    }
}

/*
 *	smgrpostckpt() -- Post-checkpoint cleanup.
 */
void smgrpostckpt(void)
{
    int i;

    for (i = 0; i < NSmgr; i++) {
        if (smgrsw[i].smgr_post_ckpt) {
            (*(smgrsw[i].smgr_post_ckpt))();
        }
    }
}

void partition_create_new_storage(Relation rel, Partition part, const RelFileNodeBackend &filenode)
{
    RelationCreateStorage(filenode.node, rel->rd_rel->relpersistence, rel->rd_rel->relowner, rel->rd_bucketoid);
    smgrclosenode(filenode);

    /*
     * Schedule unlinking of the old storage at transaction commit.
     */
    PartitionDropStorage(rel, part);
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
    dlist_mutable_iter iter;

    /*
     * Zap all unowned SMgrRelations.  We rely on smgrclose() to remove each
     * one from the list.
     */
    dlist_foreach_modify(iter, &u_sess->storage_cxt.unowned_reln)
    {
        SMgrRelation rel = dlist_container(SMgrRelationData, node, iter.cur);
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
