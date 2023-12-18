/* -------------------------------------------------------------------------
 *
 * localbuf.cpp
 *    local buffer manager. Fast buffer manager for temporary tables,
 *    which never need to be WAL-logged or checkpointed, etc.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/buffer/localbuf.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "catalog/catalog.h"
#include "access/double_write.h"
#include "executor/instrument.h"
#include "storage/buf/buf_internals.h"
#include "storage/buf/bufmgr.h"
#include "storage/smgr/segment.h"
#include "storage/smgr/relfilenode_hash.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "pgstat.h"

/* entry for buffer lookup hashtable */
typedef struct {
    BufferTag key; /* Tag of a disk page */
    int id;        /* Associated local buffer's index */
} LocalBufferLookupEnt;

/* Note: this macro only works on local buffers, not shared ones! */
#define LocalBufHdrGetBlock(bufHdr) u_sess->storage_cxt.LocalBufferBlockPointers[-((bufHdr)->buf_id + 2)]

static void InitLocalBuffers(void);
static Block GetLocalBufferStorage(void);

/*
 * LocalPrefetchBuffer -
 *	  initiate asynchronous read of a block of a relation
 *
 * Do PrefetchBuffer's work for temporary relations.
 * No-op if prefetching isn't compiled in.
 */
void LocalPrefetchBuffer(SMgrRelation smgr, ForkNumber forkNum, BlockNumber blockNum)
{
#ifdef USE_PREFETCH
    BufferTag new_tag; /* identity of requested block */
    LocalBufferLookupEnt *hresult = NULL;

    INIT_BUFFERTAG(new_tag, smgr->smgr_rnode.node, forkNum, blockNum);

    /* Initialize local buffers if first request in this session */
    if (u_sess->storage_cxt.LocalBufHash == NULL)
        InitLocalBuffers();

    /* See if the desired buffer already exists */
    hresult = (LocalBufferLookupEnt*)hash_search(u_sess->storage_cxt.LocalBufHash, (void*)&new_tag, HASH_FIND, NULL);
    if (hresult != NULL) {
        /* Yes, so nothing to do */
        return;
    }

    /* Not in buffers, so initiate prefetch */
    smgrprefetch(smgr, forkNum, blockNum);
#endif /* USE_PREFETCH */
}

void LocalBufferWrite(BufferDesc *bufHdr)
{
    SMgrRelation oreln;
    Page localpage = (char *)LocalBufHdrGetBlock(bufHdr);
    char *bufToWrite = NULL;

    /* Find smgr relation for buffer */
    oreln = smgropen(bufHdr->tag.rnode, BackendIdForTempRelations);
    /* data encrypt */
    bufToWrite = PageDataEncryptForBuffer(localpage, bufHdr);

    PageSetChecksumInplace((Page)bufToWrite, bufHdr->tag.blockNum);

    /* And write... */
    smgrwrite(oreln, bufHdr->tag.forkNum, bufHdr->tag.blockNum, bufToWrite, false);
}

void LocalBufferFlushForExtremRTO(BufferDesc *bufHdr)
{
    if (dw_enabled()) {
        /* double write */
    }
    FlushBuffer(bufHdr, NULL, WITH_LOCAL_CACHE);
}

void LocalBufferFlushAllBuffer()
{
    int i;

    for (i = 0; i < u_sess->storage_cxt.NLocBuffer; i++) {
        BufferDesc *bufHdr = &u_sess->storage_cxt.LocalBufferDescriptors[i].bufferdesc;
        uint64 buf_state;

        buf_state = pg_atomic_read_u64(&bufHdr->state);
        Assert(u_sess->storage_cxt.LocalRefCount[i] == 0);

        if ((buf_state & BM_VALID) && (buf_state & BM_DIRTY)) {
            LocalBufferFlushForExtremRTO(bufHdr);

            buf_state &= ~BM_DIRTY;
            pg_atomic_write_u32(((volatile uint32 *)&bufHdr->state) + 1, buf_state >> 32);

            u_sess->instr_cxt.pg_buffer_usage->local_blks_written++;
        }
    }
}

static void LocalBufferSanityCheck(BufferTag tag1, BufferTag tag2)
{
    if (!BUFFERTAGS_EQUAL(tag1, tag2)) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                    (errmsg("local buffer hash tag mismatch."))));
    }
}

/*
 * LocalBufferAlloc -
 *    Find or create a local buffer for the given page of the given relation.
 *
 * API is similar to bufmgr.c's BufferAlloc, except that we do not need
 * to do any locking since this is all local.   Also, IO_IN_PROGRESS
 * does not get set.  Lastly, we support only default access strategy
 * (hence, usage_count is always advanced).
 */
BufferDesc *LocalBufferAlloc(SMgrRelation smgr, ForkNumber forkNum, BlockNumber blockNum, bool *foundPtr)
{
    BufferTag new_tag; /* identity of requested block */
    LocalBufferLookupEnt *hresult = NULL;
    BufferDesc *buf_desc = NULL;
    int b;
    int try_counter;
    bool found = false;
    uint64 buf_state;

    INIT_BUFFERTAG(new_tag, smgr->smgr_rnode.node, forkNum, blockNum);

    /* Initialize local buffers if first request in this session */
    if (u_sess->storage_cxt.LocalBufHash == NULL)
        InitLocalBuffers();

    /* See if the desired buffer already exists */
    hresult = (LocalBufferLookupEnt*)hash_search(u_sess->storage_cxt.LocalBufHash, (void*)&new_tag, HASH_FIND, NULL);
    if (hresult != NULL) {
        b = hresult->id;
        buf_desc = &u_sess->storage_cxt.LocalBufferDescriptors[b].bufferdesc;
        LocalBufferSanityCheck(buf_desc->tag, new_tag);
#ifdef LBDEBUG
        fprintf(stderr, "LB ALLOC (%u,%d,%d) %d\n", smgr->smgr_rnode.node.relNode, forkNum, blockNum, -b - 1);
#endif
        buf_state = pg_atomic_read_u64(&buf_desc->state);

        /* this part is equivalent to PinBuffer for a shared buffer */
        if (u_sess->storage_cxt.LocalRefCount[b] == 0) {
            if (BUF_STATE_GET_USAGECOUNT(buf_state) < BM_MAX_USAGE_COUNT) {
                buf_state += BUF_USAGECOUNT_ONE;
                pg_atomic_write_u32(((volatile uint32 *)&buf_desc->state) + 1, buf_state >> 32);
            }
        }
        u_sess->storage_cxt.LocalRefCount[b]++;
        ResourceOwnerRememberBuffer(t_thrd.utils_cxt.CurrentResourceOwner, BufferDescriptorGetBuffer(buf_desc));
        *foundPtr = (buf_state & BM_VALID) ? TRUE : FALSE; /* If previous read attempt have failed; try again */
        if (*foundPtr == FALSE) {
            if (u_sess->storage_cxt.bulk_io_is_in_progress) {   
                /* If not found, we record this buf */
                u_sess->storage_cxt.bulk_io_in_progress_buf[u_sess->storage_cxt.bulk_io_in_progress_count] = buf_desc;
                u_sess->storage_cxt.bulk_io_in_progress_count++;
            }
        }
#ifdef EXTREME_RTO_DEBUG
        ereport(LOG, (errmsg("LocalBufferAlloc %u/%u/%u %u %u find in local buf %u/%u/%u %u %u id %d state %lu, lsn %lu",
                             smgr->smgr_rnode.node.spcNode, smgr->smgr_rnode.node.dbNode, smgr->smgr_rnode.node.relNode,
                             forkNum, blockNum, hresult->key.rnode.spcNode, hresult->key.rnode.dbNode,
                             hresult->key.rnode.relNode, hresult->key.forkNum, hresult->key.blockNum, hresult->id,
                             buf_state, LocalBufGetLSN(buf_desc))));
#endif
        return buf_desc;
    }

#ifdef LBDEBUG
    fprintf(stderr, "LB ALLOC (%u,%d,%d) %d\n", smgr->smgr_rnode.node.relNode, forkNum, blockNum,
            -t_thrd.storage_cxt.nextFreeLocalBuf - 1);
#endif

    /*
     * Need to get a new buffer.  We use a clock sweep algorithm (essentially
     * the same as what freelist.c does now...)
     */
    try_counter = u_sess->storage_cxt.NLocBuffer;
    for (;;) {
        b = u_sess->storage_cxt.nextFreeLocalBuf;

        if (++u_sess->storage_cxt.nextFreeLocalBuf >= u_sess->storage_cxt.NLocBuffer)
            u_sess->storage_cxt.nextFreeLocalBuf = 0;

        buf_desc = &u_sess->storage_cxt.LocalBufferDescriptors[b].bufferdesc;

        if (u_sess->storage_cxt.LocalRefCount[b] == 0) {
            buf_state = pg_atomic_read_u64(&buf_desc->state);

            if (BUF_STATE_GET_USAGECOUNT(buf_state) > 0) {
                buf_state -= BUF_USAGECOUNT_ONE;
                pg_atomic_write_u32(((volatile uint32 *)&buf_desc->state) + 1, buf_state >> 32);
                try_counter = u_sess->storage_cxt.NLocBuffer;
            } else {
                /* Found a usable buffer */
                u_sess->storage_cxt.LocalRefCount[b]++;
                ResourceOwnerRememberBuffer(t_thrd.utils_cxt.CurrentResourceOwner, BufferDescriptorGetBuffer(buf_desc));
                break;
            }
        } else if (--try_counter == 0)
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("no empty local buffer available")));
    }

    /*
     * this buffer is not referenced but it might still be dirty. if that's
     * the case, write it out before reusing it!
     */
    if (buf_state & BM_DIRTY) {
        if (AmPageRedoProcess()) {
            LocalBufferFlushForExtremRTO(buf_desc);
        } else {
            LocalBufferWrite(buf_desc);
        }

        /* Mark not-dirty now in case we error out below */
        buf_state &= ~BM_DIRTY;
        pg_atomic_write_u32(((volatile uint32 *)&buf_desc->state) + 1, buf_state >> 32);

        u_sess->instr_cxt.pg_buffer_usage->local_blks_written++;
    }

    /*
     * lazy memory allocation: allocate space on first use of a buffer.
     */
    if (LocalBufHdrGetBlock(buf_desc) == NULL) {
        /* Set pointer for use by BufferGetBlock() macro */
        LocalBufHdrGetBlock(buf_desc) = GetLocalBufferStorage();
    }

    /*
     * Update the hash table: remove old entry, if any, and make new one.
     */
    if (buf_state & BM_TAG_VALID) {
        hresult = (LocalBufferLookupEnt *)hash_search(u_sess->storage_cxt.LocalBufHash, (void *)&buf_desc->tag,
                                                      HASH_REMOVE, NULL);
        if (hresult == NULL) /* shouldn't happen */
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), (errmsg("local buffer hash table corrupted."))));
        /* mark buffer invalid just in case hash insert fails */
        CLEAR_BUFFERTAG(buf_desc->tag);
        buf_state &= ~(BM_VALID | BM_TAG_VALID);
        pg_atomic_write_u32(((volatile uint32 *)&buf_desc->state) + 1, buf_state >> 32);
    }

    hresult = (LocalBufferLookupEnt *)hash_search(u_sess->storage_cxt.LocalBufHash, (void *)&new_tag, HASH_ENTER,
                                                  &found);
    if (found) /* shouldn't happen */
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), (errmsg("local buffer hash table corrupted."))));
    hresult->id = b;

    /*
     * it's all ours now.
     */
    buf_desc->tag = new_tag;
    buf_desc->extra->encrypt = smgr->encrypt ? true : false; /* set tde flag */
    buf_state &= ~(BM_VALID | BM_DIRTY | BM_JUST_DIRTIED | BM_IO_ERROR);
    buf_state |= BM_TAG_VALID;
    buf_state &= ~BUF_USAGECOUNT_MASK;
    buf_state += BUF_USAGECOUNT_ONE;
    pg_atomic_write_u32(((volatile uint32 *)&buf_desc->state) + 1, buf_state >> 32);

    buf_desc->extra->seg_fileno = EXTENT_INVALID;

    *foundPtr = FALSE;
    if (u_sess->storage_cxt.bulk_io_is_in_progress) {   
       /* If not found, we record this buf */
        u_sess->storage_cxt.bulk_io_in_progress_buf[u_sess->storage_cxt.bulk_io_in_progress_count] = buf_desc;
        u_sess->storage_cxt.bulk_io_in_progress_count++;
    }
    return buf_desc;
}

/*
 * MarkLocalBufferDirty -
 *    mark a local buffer dirty
 */
void MarkLocalBufferDirty(Buffer buffer)
{
    int buf_id;
    BufferDesc *buf_desc = NULL;
    uint64 buf_state;

    Assert(BufferIsLocal(buffer));

#ifdef LBDEBUG
    fprintf(stderr, "LB DIRTY %d\n", buffer);
#endif

    buf_id = -(buffer + 1);

    Assert(u_sess->storage_cxt.LocalRefCount[buf_id] > 0);

    buf_desc = &u_sess->storage_cxt.LocalBufferDescriptors[buf_id].bufferdesc;

    buf_state = pg_atomic_fetch_or_u64(&buf_desc->state, BM_DIRTY);
    if (!(buf_state & BM_DIRTY)) {
        u_sess->instr_cxt.pg_buffer_usage->local_blks_dirtied++;
        pgstatCountLocalBlocksDirtied4SessionLevel();
    }
}

/*
 * DropRelFileNodeLocalBuffers
 *		This function removes from the buffer pool all the pages of the
 *		specified relation that have block numbers >= firstDelBlock.
 *		(In particular, with firstDelBlock = 0, all pages are removed.)
 *		Dirty pages are simply dropped, without bothering to write them
 *		out first.	Therefore, this is NOT rollback-able, and so should be
 *		used only with extreme caution!
 *
 *		See DropRelFileNodeBuffers in bufmgr.c for more notes.
 */
void DropRelFileNodeLocalBuffers(const RelFileNode &rnode, ForkNumber forkNum, BlockNumber firstDelBlock)
{
    int i;

    for (i = 0; i < u_sess->storage_cxt.NLocBuffer; i++) {
        BufferDesc* buf_desc = &u_sess->storage_cxt.LocalBufferDescriptors[i].bufferdesc;
        LocalBufferLookupEnt* hresult = NULL;
        uint64 buf_state;

        buf_state = pg_atomic_read_u64(&buf_desc->state);

        if ((buf_state & BM_TAG_VALID) && RelFileNodeEquals(rnode, buf_desc->tag.rnode) &&
            buf_desc->tag.forkNum == forkNum && buf_desc->tag.blockNum >= firstDelBlock) {
            if (u_sess->storage_cxt.LocalRefCount[i] != 0) {
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_BUFFER_REFERENCE),
                        (errmsg("block %u of %s is still referenced (local %d)",
                            buf_desc->tag.blockNum,
                            relpathbackend(buf_desc->tag.rnode, BackendIdForTempRelations, buf_desc->tag.forkNum),
                            u_sess->storage_cxt.LocalRefCount[i]))));
            }
            /* Remove entry from hashtable */
            hresult = (LocalBufferLookupEnt*)hash_search(
                u_sess->storage_cxt.LocalBufHash, (void*)&buf_desc->tag, HASH_REMOVE, NULL);
            if (hresult == NULL) /* shouldn't happen */
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), (errmsg("local buffer hash table corrupted."))));
            /* Mark buffer invalid */
            CLEAR_BUFFERTAG(buf_desc->tag);
            buf_state &= ~BUF_FLAG_MASK;
            buf_state &= ~BUF_USAGECOUNT_MASK;
            pg_atomic_write_u32(((volatile uint32 *)&buf_desc->state) + 1, buf_state >> 32);
        }
    }
}

/*
 * DropRelFileNodeAllLocalBuffers
 *      This function removes from the buffer pool all pages of all forks
 *      of the specified relation.
 *
 *      See DropRelFileNodeAllBuffers in bufmgr.c for more notes.
 */
void DropRelFileNodeAllLocalBuffers(const RelFileNode &rnode)
{
    int i;

    for (i = 0; i < u_sess->storage_cxt.NLocBuffer; i++) {
        BufferDesc* buf_desc = &u_sess->storage_cxt.LocalBufferDescriptors[i].bufferdesc;
        LocalBufferLookupEnt* hresult = NULL;
        uint64 buf_state;

        buf_state = pg_atomic_read_u64(&buf_desc->state);

        if ((buf_state & BM_TAG_VALID) && RelFileNodeEquals(rnode, buf_desc->tag.rnode)) {
            if (u_sess->storage_cxt.LocalRefCount[i] != 0) {
                if (buf_desc->tag.forkNum < 0) {
                    ereport(ERROR, (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                                    errmsg("fork number should not be less than zero")));
                }
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_BUFFER_REFERENCE),
                        (errmsg("block %u of %s is still referenced (local %d)",
                            buf_desc->tag.blockNum,
                            relpathbackend(buf_desc->tag.rnode, BackendIdForTempRelations, buf_desc->tag.forkNum),
                            u_sess->storage_cxt.LocalRefCount[i]))));
            }
            /* Remove entry from hashtable */
            hresult = (LocalBufferLookupEnt*)hash_search(
                u_sess->storage_cxt.LocalBufHash, (void*)&buf_desc->tag, HASH_REMOVE, NULL);
            if (hresult == NULL) /* shouldn't happen */
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), (errmsg("local buffer hash table corrupted."))));
            /* Mark buffer invalid */
            CLEAR_BUFFERTAG(buf_desc->tag);
            buf_state &= ~BUF_FLAG_MASK;
            buf_state &= ~BUF_USAGECOUNT_MASK;
            pg_atomic_write_u32(((volatile uint32 *)&buf_desc->state) + 1, buf_state >> 32);
        }
    }
}

/*
 * InitLocalBuffers -
 *    init the local buffer cache. Since most queries (esp. multi-user ones)
 *    don't involve local buffers, we delay allocating actual memory for the
 *    buffers until we need them; just make the buffer headers here.
 */
static void InitLocalBuffers(void)
{
    int nbufs = u_sess->attr.attr_storage.num_temp_buffers;
    HASHCTL info;
    int i;
    BufferDescExtra* extra;
    /* Allocate and zero buffer headers and auxiliary arrays */
    u_sess->storage_cxt.LocalBufferDescriptors = (BufferDescPadded*)MemoryContextAllocZero(
        SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), (unsigned int)nbufs * sizeof(BufferDescPadded));
    u_sess->storage_cxt.LocalBufferBlockPointers = (Block*)MemoryContextAllocZero(
        SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), (unsigned int)nbufs * sizeof(Block));
    u_sess->storage_cxt.LocalRefCount = (int32*)MemoryContextAllocZero(
        SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), (unsigned int)nbufs * sizeof(int32));
    extra = (BufferDescExtra *)MemoryContextAllocZero(
        SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), (unsigned int)nbufs * sizeof(BufferDescExtra));
    if (!u_sess->storage_cxt.LocalBufferDescriptors || !u_sess->storage_cxt.LocalBufferBlockPointers ||
        !u_sess->storage_cxt.LocalRefCount  || !extra)
        ereport(FATAL, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));

    u_sess->storage_cxt.nextFreeLocalBuf = 0;

    /* initialize fields that need to start off nonzero */
    for (i = 0; i < nbufs; i++) {
        BufferDesc* buf = &u_sess->storage_cxt.LocalBufferDescriptors[i].bufferdesc;

        /*
         * negative to indicate local buffer. This is tricky: shared buffers
         * start with 0. We have to start with -2. (Note that the routine
         * BufferDescriptorGetBuffer adds 1 to buf_id so our first buffer id
         * is -1.)
         */
        buf->buf_id = -i - 2;
        buf->extra = &extra[i];
    }

    /* Create the lookup hash table */
    errno_t ret = memset_s(&info, sizeof(info), 0, sizeof(info));
    securec_check(ret, "\0", "\0");
    info.keysize = sizeof(BufferTag);
    info.entrysize = sizeof(LocalBufferLookupEnt);
    info.match = BufTagMatchWithoutOpt;
    info.hash = BufTagHashWithoutOpt;
    info.hcxt = SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE);

    u_sess->storage_cxt.LocalBufHash = hash_create(
            "Local Buffer Lookup Table", nbufs, &info, HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION | HASH_COMPARE);

    if (!u_sess->storage_cxt.LocalBufHash) {
        ereport(ERROR, (errcode(ERRCODE_INITIALIZE_FAILED),
                        (errmsg("could not initialize local buffer hash table."))));
    }

    /* Initialization done, mark buffers allocated */
    u_sess->storage_cxt.NLocBuffer = nbufs;
}

/*
 * GetLocalBufferStorage - allocate memory for a local buffer
 *
 * The idea of this function is to aggregate our requests for storage
 * so that the memory manager doesn't see a whole lot of relatively small
 * requests.  Since we'll never give back a local buffer once it's created
 * within a particular process, no point in burdening memmgr with separately
 * managed chunks.
 */
static Block GetLocalBufferStorage(void)
{
    char *this_buf = NULL;

    Assert(u_sess->storage_cxt.total_bufs_allocated < u_sess->storage_cxt.NLocBuffer);

    if (u_sess->storage_cxt.next_buf_in_block >= u_sess->storage_cxt.num_bufs_in_block) {
        /* Need to make a new request to memmgr */
        int num_bufs;

        /*
         * We allocate local buffers in a context of their own, so that the
         * space eaten for them is easily recognizable in MemoryContextStats
         * output.	Create the context on first use.
         */
        if (u_sess->storage_cxt.LocalBufferContext == NULL)
            u_sess->storage_cxt.LocalBufferContext = AllocSetContextCreate(u_sess->top_mem_cxt,
                "LocalBufferContext",
                ALLOCSET_DEFAULT_MINSIZE,
                ALLOCSET_DEFAULT_INITSIZE,
                ALLOCSET_DEFAULT_MAXSIZE);

        /* Start with a 16-buffer request; subsequent ones double each time */
        num_bufs = Max(u_sess->storage_cxt.num_bufs_in_block * 2, 16);
        /* But not more than what we need for all remaining local bufs */
        num_bufs = Min(num_bufs, u_sess->storage_cxt.NLocBuffer - u_sess->storage_cxt.total_bufs_allocated);
        /* And don't overflow MaxAllocSize, either */
        num_bufs = Min((unsigned int)(num_bufs), MaxAllocSize / BLCKSZ);

        u_sess->storage_cxt.cur_block =
            (char*)BUFFERALIGN(MemoryContextAlloc(u_sess->storage_cxt.LocalBufferContext,
            num_bufs * BLCKSZ + ALIGNOF_BUFFER));
        u_sess->storage_cxt.next_buf_in_block = 0;
        u_sess->storage_cxt.num_bufs_in_block = num_bufs;
    }

    /* Allocate next buffer in current memory block */
    this_buf = u_sess->storage_cxt.cur_block + u_sess->storage_cxt.next_buf_in_block * BLCKSZ;
    u_sess->storage_cxt.next_buf_in_block++;
    u_sess->storage_cxt.total_bufs_allocated++;

    return (Block)this_buf;
}

/*
 * AtEOXact_LocalBuffers - clean up at end of transaction.
 *
 * This is just like AtEOXact_Buffers, but for local buffers.
 */
void AtEOXact_LocalBuffers(bool isCommit)
{
#ifdef USE_ASSERT_CHECKING
    if (assert_enabled) {
        int i;

        for (i = 0; i < u_sess->storage_cxt.NLocBuffer; i++) {
            Assert(u_sess->storage_cxt.LocalRefCount[i] == 0);
        }
    }
#endif
}

/*
 * AtProcExit_LocalBuffers - ensure we have dropped pins during backend exit.
 *
 * This is just like AtProcExit_Buffers, but for local buffers.  We shouldn't
 * be holding any remaining pins; if we are, and assertions aren't enabled,
 * we'll fail later in DropRelFileNodeBuffers while trying to drop the temp
 * rels.
 */
void AtProcExit_LocalBuffers(void)
{
#ifdef USE_ASSERT_CHECKING
    if (assert_enabled && u_sess->storage_cxt.LocalRefCount) {
        int i;

        for (i = 0; i < u_sess->storage_cxt.NLocBuffer; i++) {
            Assert(u_sess->storage_cxt.LocalRefCount[i] == 0);
        }
    }
#endif
}

/*
 * ForgetLocalBuffer - drop a buffer from local buffers
 *
 * This is similar to bufmgr.c's ForgetBuffer, except that we do not need
 * to do any locking since this is all local.  As with that function, this
 * must be used very carefully, since we'll cheerfully throw away dirty
 * buffers without any attempt to write them.
 */
void ForgetLocalBuffer(RelFileNode rnode, ForkNumber forkNum, BlockNumber blockNum)
{
    SMgrRelation smgr = smgropen(rnode, t_thrd.proc_cxt.MyBackendId);
    BufferTag   tag;            /* identity of target block */
    LocalBufferLookupEnt *hresult;
    BufferDesc *bufHdr;
    uint64      bufState;

    /*
     * If somehow this is the first request in the session, there's nothing to
     * do.  (This probably shouldn't happen, though.)
     */
    if (t_thrd.storage_cxt.LocalBufHash == NULL) {
        return;
    }

    /* create a tag so we can lookup the buffer */
    INIT_BUFFERTAG(tag, smgr->smgr_rnode.node, forkNum, blockNum);

    /* see if the block is in the local buffer pool */
    hresult = (LocalBufferLookupEnt *)
        hash_search(t_thrd.storage_cxt.LocalBufHash, (void *) &tag, HASH_REMOVE, NULL);

    /* didn't find it, so nothing to do */
    if (!hresult) {
        return;
    }

    /* mark buffer invalid */
    bufHdr = GetLocalBufferDescriptor(hresult->id);
    CLEAR_BUFFERTAG(bufHdr->tag);
    bufState = pg_atomic_read_u64(&bufHdr->state);
    bufState &= ~(BM_VALID | BM_TAG_VALID | BM_DIRTY);
}
