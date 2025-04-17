/* -------------------------------------------------------------------------
 *
 * crbuf.cpp
 *    cr pool buffer manager. Fast buffer manager for UHeap/UBtree CR pages,
 *    which never need to be WAL-logged or checkpointed, etc.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/buffer/crbuf.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "storage/buf/bufmgr.h"
#include "storage/buf/buf_internals.h"
#include "utils/dynahash.h"
#include "gstrace/gstrace_infra.h"
#include "gstrace/storage_gstrace.h"

#include "access/hash.h"
#include "access/nbtree.h"
#include "access/ubtreepcr.h"
#include "knl/knl_instance.h"
#include "storage/buf/crbuf.h"
#include "storage/checksum.h"
#include "storage/smgr/relfilenode_hash.h"
#include "tsan_annotation.h"
#include "utils/elog.h"
#include "utils/memutils.h"

#define CR_BUFFER_NUM 12800
#define INVALID_BUFF 0
#define PCRP_RESERVED_PAGE_COUNT 6
#define CRPageIsNew(page) (((PageHeader)(page))->pd_upper == 0)

extern uint32 hashquickany(uint32 seed, register const unsigned char *data, register int len);

/*
* Lock buffer header - set CRBM_LOCKED in buffer state.
*/
uint64 LockCRBufHdr(CRBufferDesc *desc)
{
#ifndef ENABLE_THREAD_CHECK
    SpinDelayStatus delayStatus = init_spin_delay(desc);
#endif
    uint64 old_buf_state;

    while (true) {
        /* set CRBM_LOCKED flag */
        old_buf_state = pg_atomic_fetch_or_u64(&desc->state, CRBM_LOCKED);
        /* if it wasn't set before we're OK */
        if (!(old_buf_state & CRBM_LOCKED)) {
            break;
        }
#ifndef ENABLE_THREAD_CHECK
        perform_spin_delay(&delayStatus);
#endif
    }
#ifndef ENABLE_THREAD_CHECK
    finish_spin_delay(&delayStatus);
#endif

    TsAnnotateHappensAfter(&desc->state);
    return old_buf_state | CRBM_LOCKED;
}

uint64 WaitCRBufHdrUnlocked(CRBufferDesc *buf)
{
#ifndef ENABLE_THREAD_CHECK
    SpinDelayStatus delay_status = init_spin_delay(buf);
#endif
    uint64 buf_state;

    buf_state = pg_atomic_read_u64(&buf->state);

    while (buf_state & CRBM_LOCKED) {
#ifndef ENABLE_THREAD_CHECK
        perform_spin_delay(&delay_status);
#endif
        buf_state = pg_atomic_read_u64(&buf->state);
    }

#ifndef ENABLE_THREAD_CHECK
    finish_spin_delay(&delay_status);
#endif

    TsAnnotateHappensAfter(&buf->state);

    return buf_state;
}

bool PinCRBuffer(CRBufferDesc *buf)
{
    uint64 buf_state;

    for (;;) {
        buf_state = __sync_add_and_fetch(&buf->state, 1);
        if (buf_state & CRBM_LOCKED) {
            buf_state = __sync_fetch_and_add(&buf->state, -1);
            WaitCRBufHdrUnlocked(buf);
            continue;
        }
        break;
    }

    return (buf_state & CRBM_VALID) != 0;
}

void UnpinCRBuffer(CRBufferDesc *buf)
{
    uint64 buf_state;

    for (;;) {
        buf_state = __sync_add_and_fetch(&buf->state, -1);
        if (buf_state & CRBM_LOCKED) {
            buf_state = __sync_add_and_fetch(&buf->state, 1);
            WaitCRBufHdrUnlocked(buf);
            continue;
        }
        break;
    }
}

void ReleaseCRBuffer(Buffer buffer)
{
    UnpinCRBuffer(GetCRBufferDescriptor(buffer));
#ifdef CRDEBUG
    CRPoolScan();
#endif
}

void InitCRBufTable(int size)
{
    HASHCTL info;

    info.keysize = sizeof(BufferTag);
    info.entrysize = sizeof(CRBufEntry);
    info.hash = tag_hash;
    info.num_partitions = NUM_BUFFER_PARTITIONS;

    t_thrd.storage_cxt.CRSharedBufHash = ShmemInitHash("CR Shared Buffer Lookup Table", size, size, &info,
                                                        HASH_ELEM | HASH_FUNCTION | HASH_PARTITION);
}

void CRBufListInsert(CRBufferDesc *buf_desc_head, CRBufferDesc *new_buf_desc)
{
    CRBufferDesc *buf_desc_ptr = buf_desc_head;

    while (buf_desc_ptr->cr_buf_next != InvalidBuffer) {
        buf_desc_ptr = GetCRBufferDescriptor(buf_desc_ptr->cr_buf_next);
    }
    buf_desc_ptr->cr_buf_next = new_buf_desc->buf_id;
    new_buf_desc->cr_buf_prev = buf_desc_ptr->buf_id;
    new_buf_desc->cr_buf_next = InvalidBuffer;
}

void CRBufListRemove(CRBufferDesc *buf_desc_head, Buffer removing_buff_id, CRBufferDesc **removed_buf_desc,
                     bool *remove_head, Buffer *new_head_buff_id)
{
    CRBufferDesc *buf_desc_ptr = buf_desc_head;
    CRBufferDesc *buf_prev = NULL;
    CRBufferDesc *buf_next = NULL;

    *remove_head = false;
    for (;;) {
        if (buf_desc_ptr->buf_id == removing_buff_id) {
            if (buf_desc_ptr->cr_buf_prev != InvalidBuffer) {
                buf_prev = GetCRBufferDescriptor(buf_desc_ptr->cr_buf_prev);
                buf_prev->cr_buf_next = buf_desc_ptr->cr_buf_next;
            }

            if (buf_desc_ptr->cr_buf_next != InvalidBuffer) {
                buf_next = GetCRBufferDescriptor(buf_desc_ptr->cr_buf_next);
                buf_next->cr_buf_prev = buf_desc_ptr->cr_buf_prev;
                /* if delete head, update head pointer */
                if (buf_desc_ptr->cr_buf_prev == InvalidBuffer) {
                    buf_desc_head = buf_next;
                    *remove_head = true;
                    *new_head_buff_id = buf_desc_head->buf_id;
                }
            } else if (buf_desc_ptr->cr_buf_prev == InvalidBuffer) {
                /* delete only head, set buf_id to Invalid */
                buf_desc_head = buf_desc_ptr;
                *remove_head = true;
                *new_head_buff_id = InvalidBuffer;
            }
            break;
        }
        if (buf_desc_ptr->cr_buf_next != InvalidBuffer) {
            buf_desc_ptr = GetCRBufferDescriptor(buf_desc_ptr->cr_buf_next);
        } else {
            *removed_buf_desc = NULL;
            return;
        }
    }

    *removed_buf_desc = buf_desc_ptr;
}

CRBufferDesc *CRBufListFetch(int buf_id, CommitSeqNo query_csn, CommandId query_cid, uint64 rsid)
{
    CRBufferDesc *buf_desc_ptr = NULL;

    while (buf_id != InvalidBuffer) {
        buf_desc_ptr = GetCRBufferDescriptor(buf_id);
        if (buf_desc_ptr->usable && query_csn == buf_desc_ptr->csn && query_cid == buf_desc_ptr->cid
            && rsid == buf_desc_ptr->rsid) {
            ereport(DEBUG3, (errmodule(MOD_PCR),
                    (errmsg("CRBufListFetch: fetch from cr buffer list found! buf_id %d, csn %ld, cid %d, rsid %ld",
                            buf_id, query_csn, query_cid, rsid))));
            return buf_desc_ptr;
        }
        buf_id = buf_desc_ptr->cr_buf_next;
    }
    ereport(DEBUG3, (errmodule(MOD_PCR),
            (errmsg("CRBufListFetch: fetch from cr buffer list notfound! buf_id: %d, csn: %ld, rsid: %ld",
                    buf_id, query_csn, rsid))));
    return NULL;
}

/*
 * CRBufTableHashCode
 *      Compute the hash code associated with a BufferTag
 */
uint32 CRBufTableHashCode(BufferTag *tagPtr)
{
    BufferTag tag = *tagPtr;
    tag.rnode.opt = DefaultFileNodeOpt;
    return hashquickany(0xFFFFFFFF, (unsigned char *)&tag, sizeof(BufferTag));
}

/*
 * CRBufTableLookup
 *       Lookup the given BufferTag; return buffer ID, or -1 if not found
 * 
 * Caller must hold at least share lock on BufMappingLock for tag's partition
 */
int CRBufTableLookup(BufferTag *tag, uint32 hashcode)
{
    CRBufEntry *result = NULL;

    result = (CRBufEntry *)buf_hash_operate<HASH_FIND>(t_thrd.storage_cxt.CRSharedBufHash, tag, hashcode, NULL);

    if (SECUREC_UNLIKELY(result == NULL)) {
        return -1;
    }

    return result->id;
}

/* CRBufTableInsert
 *      Insert a hashtable entry for given tag and buffer ID,
 *      unless an entry already exists for that tag
 * 
 * Returns -1 on successful insertion. If a conflicting entry exists
 * already, returns the buffer ID in that entry.
 */
int CRBufTableInsert(BufferTag *tag, uint32 hashcode, int bufId)
{
    CRBufEntry *result = NULL;
    bool found = false;

    Assert(bufId >= 0);            /* -1 is reserved for not-in-table */

    result = (CRBufEntry *)buf_hash_operate<HASH_ENTER>(t_thrd.storage_cxt.CRSharedBufHash, tag, hashcode, &found);

    if (found) {
        return result->id;
    }

    result->id = bufId;
    return -1;
}

/*
 * CRBufTableRemove
 *      Delete the hashtable entry for given tag (which must exist)
 * 
 * Caller must hold exclusive lock on BufMappingLock for tag's partition
 */
void CRBufTableRemove(BufferTag *tag, uint32 hashcode)
{
    CRBufEntry *result = NULL;

    result = (CRBufEntry *)buf_hash_operate<HASH_REMOVE>(t_thrd.storage_cxt.CRSharedBufHash, tag, hashcode, NULL);

    if (result == NULL) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmodule(MOD_PCR), (errmsg("shared buffer hash table corrupted."))));
    }
}

void CRBufTableInsertBuffDesc(BufferTag *tag, uint32 hashcode, CRBufferDesc *buff_desc)
{
    CRBufEntry *result = NULL;
    bool found = false;

    Assert(buff_desc->buf_id >= 0);                 /* -1 is reserved for not-in-table */

    result  = (CRBufEntry *)buf_hash_operate<HASH_ENTER>(t_thrd.storage_cxt.CRSharedBufHash, tag, hashcode, &found);

    if (found) {
        CRBufferDesc *buff_desc_head = GetCRBufferDescriptor(result->id);
        CRBufListInsert(buff_desc_head, buff_desc);
        return;
    }

    result->id = buff_desc->buf_id;
}

void CRBufTableAndListRemove(BufferTag *tag, uint32 hashcode, Buffer removing_buff_id, CRBufferDesc **removed_buff_desc)
{
    Buffer buff_id;
    Buffer new_head_buff_id = InvalidBuffer;
    CRBufferDesc *buff_desc_head = NULL;
    bool remove_head = false;

    buff_id = CRBufTableLookup(tag, hashcode);
    if (buff_id < 0) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmodule(MOD_PCR), (errmsg("shared buffer hash table corrupted."))));
        return;
    }

    buff_desc_head = GetCRBufferDescriptor(buff_id);

    CRBufListRemove(buff_desc_head, removing_buff_id, removed_buff_desc, &remove_head, &new_head_buff_id);

    if (remove_head) {
        CRBufTableRemove(tag, hashcode);
        if (new_head_buff_id != InvalidBuffer)
            CRBufTableInsert(tag, hashcode, new_head_buff_id);
    }
}

void InitCRBufPoolAccess(void)
{
    uint64 refcount_size;
    bool found_cr_refcounts = false;

    refcount_size = CR_BUFFER_NUM * sizeof(int32);
    t_thrd.storage_cxt.CRBufferRefCount = (int32*)ShmemInitStruct("CR Ref Counts", refcount_size, &found_cr_refcounts);
}

void InitCRBufPool(void)
{
    bool found_cr_descs = false;
    bool found_cr_bufs = false;
    uint64 buffer_size;

    t_thrd.storage_cxt.CRBufferDescriptors = (CRBufferDescPadded *)CACHELINEALIGN(
        ShmemInitStruct("CR Buffer Descriptors", CR_BUFFER_NUM * sizeof(CRBufferDescPadded) + PG_CACHE_LINE_SIZE,
        &found_cr_descs));

#ifdef __aarch64__
    buffer_size = CR_BUFFER_NUM * (Size)BLCKSZ + PG_CACHE_LINE_SIZE;
    t_thrd.storage_cxt.CRBufferBlocks =
        (char *)CACHELINEALIGN(ShmemInitStruct("CR Buffer Blocks", buffer_size, &found_cr_bufs));
#else
    buffer_size = CR_BUFFER_NUM * (Size)BLCKSZ;
    t_thrd.storage_cxt.CRBufferBlocks = (char *)ShmemInitStruct("CR Buffer Blocks", buffer_size, &found_cr_bufs);
#endif

    if (found_cr_descs || found_cr_bufs) {
        Assert(found_cr_descs && found_cr_bufs);
    } else {
        int i;

        for (i = 0; i < CR_BUFFER_NUM; i++) {
            CRBufferDesc *buf = GetCRBufferDescriptor(i);
            buf->buf_id = i;
            CLEAR_BUFFERTAG(buf->tag);
            buf->csn = InvalidCommitSeqNo;
            buf->cid = InvalidCommandId;
            buf->usable = false;
            pg_atomic_init_u64(&buf->state, 0);
            buf->cr_buf_next = INVALID_BUFF;
            buf->cr_buf_prev = INVALID_BUFF;
            buf->lru_next = INVALID_BUFF;
            buf->lru_prev = INVALID_BUFF;
        }
        g_instance.crbuf_cxt.cr_buf_assign_lock = LWLockAssign(LWTRANCHE_CR_BUF_ASSIGN);
        g_instance.crbuf_cxt.cr_buf_lru_lock = LWLockAssign(LWTRANCHE_CR_BUF_LRU);
        g_instance.crbuf_cxt.cr_lru_first = INVALID_BUFF;
        g_instance.crbuf_cxt.cr_lru_last = INVALID_BUFF;
        g_instance.crbuf_cxt.hwm = 1;
        g_instance.crbuf_cxt.capacity = CR_BUFFER_NUM;
    }
    InitCRBufTable(CR_BUFFER_NUM);
}

CRBufferDesc *CRBufferAllocHwm(void)
{
    CRBufferDesc *buffDesc = NULL;
    int id;

    if (g_instance.crbuf_cxt.hwm >= g_instance.crbuf_cxt.capacity) {
        return NULL;
    }

    (void)LWLockAcquire(g_instance.crbuf_cxt.cr_buf_assign_lock, LW_EXCLUSIVE);
    if (g_instance.crbuf_cxt.hwm >= g_instance.crbuf_cxt.capacity) {
        LWLockRelease(g_instance.crbuf_cxt.cr_buf_assign_lock);
        return NULL;
    }
    id = g_instance.crbuf_cxt.hwm;
    g_instance.crbuf_cxt.hwm++;
    buffDesc = GetCRBufferDescriptor(id);
    LWLockRelease(g_instance.crbuf_cxt.cr_buf_assign_lock);

    return buffDesc;
}

CRBufferDesc *AllocCRBufferFromBucket(CRBufferDesc *buf_desc_head)
{
    CRBufferDesc *buf_desc_ptr = buf_desc_head;
    CRBufferDesc *oldestCRBuf = buf_desc_head;
    CommitSeqNo minCsn = buf_desc_head->csn;
    CommandId minCid = buf_desc_head->cid;
    int pageCount = 1;

    while (buf_desc_ptr->cr_buf_next != InvalidBuffer) {
        buf_desc_ptr = GetCRBufferDescriptor(buf_desc_ptr->cr_buf_next);
        if (buf_desc_ptr->csn < minCsn) {
            minCsn = buf_desc_ptr->csn;
            oldestCRBuf = buf_desc_ptr;
        } else if (buf_desc_ptr->csn == minCsn && buf_desc_ptr->cid < minCid) {
            minCid = buf_desc_ptr->cid;
            oldestCRBuf = buf_desc_ptr;
        }
        pageCount++;
    }
    if ((pageCount >= PCRP_RESERVED_PAGE_COUNT) && (CRBUF_STATE_GET_REFCOUNT(oldestCRBuf->state) == 0)) {
        return oldestCRBuf;
    }
    return NULL;
}

CRBufferDesc *ReadCRBuffer(Relation reln, BlockNumber block_num, CommitSeqNo csn, CommandId cid)
{
    BufferTag new_tag;
    CRBufferDesc *buf_desc = NULL;
    CRBufferDesc *buf_desc_res = NULL;
    int buf_id;
    LWLock *hashbucket_lock = NULL;
    uint32 hashcode;
    bool valid = false;

    RelationOpenSmgr(reln);
    UndoPersistence persistence = UndoPersistenceForRelation(reln);
    uint64 rsid = t_thrd.proc->sessMemorySessionid;
    INIT_BUFFERTAG(new_tag, reln->rd_smgr->smgr_rnode.node, MAIN_FORKNUM, block_num);
    Assert(t_thrd.storage_cxt.CRSharedBufHash != NULL);
    hashcode = CRBufTableHashCode(&new_tag);
    hashbucket_lock = CRBufMappingPartitionLock(hashcode);
    
    LWLockAcquire(hashbucket_lock, LW_SHARED);
    buf_id = CRBufTableLookup(&new_tag, hashcode);
    if (buf_id >= 0) {
        buf_desc = GetCRBufferDescriptor(buf_id);
        ereport(DEBUG3, (errmodule(MOD_PCR),
                (errmsg("ReadCRBuffer: get from cr buffer hashtable found! (%u, %d)"
                         "buf_id %d, csn %ld, cid %d, rsid: %ld",
                        reln->rd_smgr->smgr_rnode.node.relNode, block_num, buf_id, buf_desc->csn,
                        buf_desc->cid, rsid))));
        buf_desc_res = CRBufListFetch(buf_id, csn, cid, rsid);
        if (buf_desc_res != NULL) {
            valid = PinCRBuffer(buf_desc_res);

            LWLockRelease(hashbucket_lock);
            return buf_desc_res;
        }
    }
    LWLockRelease(hashbucket_lock);

    return NULL;
}

void InitCRBufDesc(CRBufferDesc *buf_desc, BufferTag new_tag, CommitSeqNo csn, CommandId cid,
                   uint64 rsid, bool reset_chain)
{
    buf_desc->tag = new_tag;
    buf_desc->csn = csn;
    buf_desc->cid = cid;
    buf_desc->rsid = rsid;
    buf_desc->usable = true;
    pg_atomic_init_u64(&buf_desc->state, 0);
    if (reset_chain) {
        buf_desc->cr_buf_next = INVALID_BUFF;
        buf_desc->cr_buf_prev = INVALID_BUFF;
        buf_desc->lru_next = INVALID_BUFF;
        buf_desc->lru_prev = INVALID_BUFF;
    }
}

void CRBufLruAddHead(CRBufferDesc *buff_desc)
{
    CRBufferDesc *lru_first_buff_desc = NULL;

    buff_desc->lru_prev = INVALID_BUFF;
    buff_desc->lru_next = g_instance.crbuf_cxt.cr_lru_first;

    if (g_instance.crbuf_cxt.cr_lru_first != INVALID_BUFF) {
        lru_first_buff_desc = GetCRBufferDescriptor(g_instance.crbuf_cxt.cr_lru_first);
        lru_first_buff_desc->lru_prev = buff_desc->buf_id;
    }

    g_instance.crbuf_cxt.cr_lru_first = buff_desc->buf_id;
    if (g_instance.crbuf_cxt.cr_lru_last == INVALID_BUFF) {
        g_instance.crbuf_cxt.cr_lru_last = buff_desc->buf_id;
    }
}

void CRBufLruAddTail(CRBufferDesc *buff_desc)
{
    CRBufferDesc *lru_last_buff_desc = NULL;

    buff_desc->lru_next = INVALID_BUFF;
    buff_desc->lru_prev = g_instance.crbuf_cxt.cr_lru_last;

    if (g_instance.crbuf_cxt.cr_lru_last != INVALID_BUFF) {
        lru_last_buff_desc = GetCRBufferDescriptor(g_instance.crbuf_cxt.cr_lru_last);
        lru_last_buff_desc->lru_next = buff_desc->buf_id;
    }

    g_instance.crbuf_cxt.cr_lru_last = buff_desc->buf_id;
    if (g_instance.crbuf_cxt.cr_lru_first == INVALID_BUFF) {
        g_instance.crbuf_cxt.cr_lru_first = buff_desc->buf_id;
    }
}

void CRBufLruRemove(CRBufferDesc *buff_desc)
{
    CRBufferDesc *lru_prev_buf_desc = NULL;
    CRBufferDesc *lru_next_buf_desc = NULL;
    lru_prev_buf_desc = GetCRBufferDescriptor(buff_desc->lru_prev);
    lru_next_buf_desc = GetCRBufferDescriptor(buff_desc->lru_next);

    if (buff_desc->lru_prev) {
        lru_prev_buf_desc->lru_next = buff_desc->lru_next;
    }

    if (buff_desc->lru_next) {
        lru_next_buf_desc->lru_prev = buff_desc->lru_prev;
    }

    if (g_instance.crbuf_cxt.cr_lru_last == buff_desc->buf_id) {
        g_instance.crbuf_cxt.cr_lru_last = buff_desc->lru_prev;
    }

    if (g_instance.crbuf_cxt.cr_lru_first == buff_desc->buf_id) {
        g_instance.crbuf_cxt.cr_lru_first = buff_desc->lru_next;
    }

    buff_desc->lru_prev = INVALID_BUFF;
    buff_desc->lru_next = INVALID_BUFF;
}

void CRBufLruRefresh(CRBufferDesc *buff_desc)
{
    CRBufLruRemove(buff_desc);
    CRBufLruAddHead(buff_desc);
}

CRBufferDesc *CRBufferRecycle()
{
    Buffer lru_last = InvalidBuffer;
    CRBufferDesc *buff_desc = NULL;
    CRBufferDesc *removed_buff_desc = NULL;

    LWLockAcquire(g_instance.crbuf_cxt.cr_buf_lru_lock, LW_EXCLUSIVE);
    lru_last = g_instance.crbuf_cxt.cr_lru_last;
    while (lru_last != InvalidBuffer) {
        buff_desc = GetCRBufferDescriptor(lru_last);
        if (CRBUF_STATE_GET_REFCOUNT(buff_desc->state) > 0) {
            lru_last = buff_desc->lru_prev;
            CRBufLruRefresh(buff_desc);
            continue;
        }

        uint32 hashcode = CRBufTableHashCode(&buff_desc->tag);
        LWLock *hashbucket_lock = CRBufMappingPartitionLock(hashcode);
        LWLockAcquire(hashbucket_lock, LW_EXCLUSIVE);

        if (CRBUF_STATE_GET_REFCOUNT(buff_desc->state) > 0) {
            LWLockRelease(hashbucket_lock);
            lru_last = buff_desc->lru_prev;
            continue;
        }

        ereport(DEBUG3, (errmodule(MOD_PCR),
                (errmsg("CRBufTableAndListRemove: (%u, %d) buf_id %d, buf_csn %ld, buf_cid %d, buf_rsid %ld",
                        (buff_desc->tag).rnode.relNode, (buff_desc->tag).blockNum, buff_desc->buf_id, buff_desc->csn,
                        buff_desc->cid, buff_desc->rsid))));
        /* remove buff_desc from BufTable */
        CRBufTableAndListRemove(&buff_desc->tag, hashcode, buff_desc->buf_id, &removed_buff_desc);
        LWLockRelease(hashbucket_lock);
        break;
    }

    if (lru_last == InvalidBuffer) {
        LWLockRelease(g_instance.crbuf_cxt.cr_buf_lru_lock);
        return NULL;
    }

    CRBufLruRemove(removed_buff_desc);
    LWLockRelease(g_instance.crbuf_cxt.cr_buf_lru_lock);
    return removed_buff_desc;
}

CRBufferDesc *CRBufferAllocOrRecycle(void)
{
    CRBufferDesc *buff_desc = NULL;

    for (;;) {
        buff_desc = CRBufferAllocHwm();
        if (buff_desc != NULL) {
            break;
        }

        buff_desc = CRBufferRecycle();
        if (buff_desc != NULL) {
            break;
        }
        SPIN_DELAY();
    }
    return buff_desc;
}

CRBufferDesc *AllocCRBuffer(Relation reln, ForkNumber forkNum, BlockNumber blockNum, CommitSeqNo csn, CommandId cid)
{
    BufferTag new_tag;
    CRBufferDesc *buf_desc_head = NULL;
    CRBufferDesc *buf_desc = NULL;
    int buf_id;
    LWLock *hashbucket_lock = NULL;
    uint32 hashcode;
    bool valid;
    RelFileNode rel_file_node = reln->rd_node;
    UndoPersistence persistence = UndoPersistenceForRelation(reln);
    uint64 rsid = t_thrd.proc->sessMemorySessionid;
    uint64 buf_state;

    INIT_BUFFERTAG(new_tag, rel_file_node, forkNum, blockNum);

    Assert(t_thrd.storage_cxt.CRSharedBufHash != NULL);
    hashcode = CRBufTableHashCode(&new_tag);
    hashbucket_lock = CRBufMappingPartitionLock(hashcode);

    LWLockAcquire(hashbucket_lock, LW_EXCLUSIVE);
    buf_id = CRBufTableLookup(&new_tag, hashcode);
    if (buf_id >= 0) {
        buf_desc_head = GetCRBufferDescriptor(buf_id);

        /* Try alloc CR buffer from bucket */
        buf_desc = AllocCRBufferFromBucket(buf_desc_head);
        if (buf_desc != NULL) {
            Assert(CRBUF_STATE_GET_REFCOUNT(buf_desc->state) ==0);
            ereport(DEBUG3, (errmodule(MOD_PCR),
                    (errmsg("AllocCRBuffer: alloc from bucket! (%u,%d,%d) buf_id %d, csn %ld, cid %d, rsid %ld",
                            rel_file_node.relNode, forkNum, blockNum, buf_desc->buf_id,
                            buf_desc->csn, buf_desc->cid, rsid))));
            InitCRBufDesc(buf_desc, new_tag, csn, cid, rsid, false);
            ereport(DEBUG3, (errmodule(MOD_PCR),
                    (errmsg("InitBufDesc: (%u, %d) buf_id %d, csn %ld, cid %d, rsid %ld",
                            rel_file_node.relNode, blockNum, buf_desc->buf_id, buf_desc->csn,
                            buf_desc->cid, buf_desc->rsid))));
            Assert(CRBUF_STATE_GET_REFCOUNT(buf_desc->state) == 0);
            valid = PinCRBuffer(buf_desc);
            buf_state = LockCRBufHdr(buf_desc);

            LWLockRelease(hashbucket_lock);
            LWLockAcquire(g_instance.crbuf_cxt.cr_buf_lru_lock, LW_EXCLUSIVE);
            CRBufLruRefresh(buf_desc);
            LWLockRelease(g_instance.crbuf_cxt.cr_buf_lru_lock);

            return buf_desc;
        }
    }
    LWLockRelease(hashbucket_lock);
    /* Alloc CR buffer from SHM or recycle */
    CRBufferDesc *new_buf_desc = CRBufferAllocOrRecycle();
    Assert(new_buf_desc != NULL);
    Assert(CRBUF_STATE_GET_REFCOUNT(new_buf_desc->state) == 0);

    ereport(DEBUG3, (errmodule(MOD_PCR),
            (errmsg("AllocCRBuffer: alloc from shm or recycle! (%u, %d) buf_id %d, csn %ld, cid %d, rsid %ld",
            (new_buf_desc->tag).rnode.relNode, (new_buf_desc->tag).blockNum,
            new_buf_desc->buf_id, new_buf_desc->csn,
            new_buf_desc->cid, rsid))));

    InitCRBufDesc(new_buf_desc, new_tag, csn, cid, rsid, true);
    valid = PinCRBuffer(new_buf_desc);
    buf_state = LockCRBufHdr(new_buf_desc);

    LWLockAcquire(hashbucket_lock, LW_EXCLUSIVE);
    CRBufTableInsertBuffDesc(&new_tag, hashcode, new_buf_desc);
    LWLockRelease(hashbucket_lock);

    LWLockAcquire(g_instance.crbuf_cxt.cr_buf_lru_lock, LW_EXCLUSIVE);
    CRBufLruAddHead(new_buf_desc);
    LWLockRelease(g_instance.crbuf_cxt.cr_buf_lru_lock);

    return new_buf_desc;
}

CRBufferDesc *CRReadOrAllocBuffer(Relation reln, BlockNumber block_num, CommitSeqNo csn, CommandId cid)
{
    CRBufferDesc *buf_desc = NULL;
    buf_desc = ReadCRBuffer(reln, block_num, csn, cid);
    if (buf_desc == NULL) {
        buf_desc = AllocCRBuffer(reln, MAIN_FORKNUM, block_num, csn, cid);
    }
    return buf_desc;
}

void CRBufferUnused(Buffer base_buffer)
{
    BufferTag buftag;
    BufferDesc *base_buf_desc = NULL;
    int buf_id;
    uint64 buf_state;
    LWLock *hashbucket_lock = NULL;
    uint32 hashcode;

    Assert(t_thrd.storage_cxt.CRSharedBufHash != NULL);

    base_buf_desc = GetBufferDescriptor(base_buffer -1);
    buftag = base_buf_desc->tag;

    hashcode = CRBufTableHashCode(&buftag);
    hashbucket_lock = CRBufMappingPartitionLock(hashcode);
    LWLockAcquire(hashbucket_lock, LW_SHARED);
    buf_id = CRBufTableLookup(&buftag, hashcode);
    if (buf_id >= 0) {
        CRBufferDesc *buf_desc = NULL;
        while (buf_id != InvalidBuffer) {
            buf_desc = GetCRBufferDescriptor(buf_id);

            buf_state = LockCRBufHdr(buf_desc);
            ereport(DEBUG3, (errmodule(MOD_PCR),
                    (errmsg("CRBufferUnused: buf_id %d, page (%u,%d), csn %ld, cid %d, rsid %ld",
                            buf_desc->buf_id, (buf_desc->tag).rnode.relNode, (buf_desc->tag).blockNum,
                            buf_desc->csn, buf_desc->cid, buf_desc->rsid))));
            if (buf_desc->usable == true) {
                buf_desc->usable = false;
            }
            UnlockCRBufHdr(buf_desc, buf_state);
            buf_id = buf_desc->cr_buf_next;
        }
    }

    LWLockRelease(hashbucket_lock);
}

void PrintCRDesc(CRBufferDesc *buf)
{
    if (buf->csn != InvalidCommitSeqNo) {
        ereport(DEBUG5, (errmodule(MOD_PCR),
                (errmsg("page (%u, %d), buf_id %d, page (%u,%d), csn %ld, cid %d, usable: %d, "
                        "rsid %ld, cr_buf_next %d, cr_buf_prev %d, lru_next %d, lru_prev %d",
                        (buf->tag).rnode.relNode, (buf->tag).blockNum, buf->buf_id, buf->csn,
                        buf->cid, buf->usable, buf->rsid, buf->cr_buf_next, buf->cr_buf_prev,
                        buf->lru_next, buf->lru_prev))));
    }
}

void CRPoolScan()
{
    CRBufferDesc *buf = NULL;
    int i;

    ereport(DEBUG5, (errmodule(MOD_PCR), (errmsg("HashTable Info:"))));
    for (i = 1; i < CR_BUFFER_NUM; i++) {
        buf = GetCRBufferDescriptor(i);
        if (buf->cr_buf_prev != InvalidBuffer) {
            continue;
        }
        PrintCRDesc(buf);
        while (buf->cr_buf_next != InvalidBuffer) {
            buf = GetCRBufferDescriptor(buf->cr_buf_next);
            PrintCRDesc(buf);
        }
    }

    ereport(DEBUG5, (errmodule(MOD_PCR), (errmsg("LRU Info:"))));
    Buffer lru_cursor = g_instance.crbuf_cxt.cr_lru_last;
    while (lru_cursor != InvalidBuffer) {
        buf = GetCRBufferDescriptor(lru_cursor);
        ereport(DEBUG5, (errmodule(MOD_PCR),
                (errmsg("page (%u, %d), buf_id %d, csn %ld, cid %d, usable %d,"
                        "rsid %ld, cr_buf_next %d, cr_buf_prev %d, lru_next %d, lru_prev %d",
                        (buf->tag).rnode.relNode, (buf->tag).blockNum, buf->buf_id, buf->csn, buf->cid, buf->usable,
                        buf->rsid, buf->cr_buf_next, buf->cr_buf_prev, buf->lru_next, buf->lru_prev))));
        lru_cursor = buf->lru_prev;
    }
}

static void ParseUBtreeCRPageHeader(const PageHeader page, int blkno)
{
    bool checkSumMatched = false;
    uint64 freeSpace = 0;
    if (CheckPageZeroCases(page)) {
        uint16 checksum = pg_checksum_page((char*)page, (BlockNumber)blkno);
        checkSumMatched = (checksum == page->pd_checksum);
    }
    ereport(DEBUG5, (errmodule(MOD_PCR),
            (errmsg("page information of block %d, pd_lsn: %X/%X,  pd_checksum: 0x%X, verify %s",
                    blkno, (uint32)(PageGetLSN(page) >> 32), (uint32)PageGetLSN(page), page->pd_checksum,
                    checkSumMatched ? "success" : "fail"))));
    ereport(DEBUG5, (errmodule(MOD_PCR),
            (errmsg("pd_flags: PD_HAS_FREE_LINES %d, PD_PAGE_FULL %d, PD_ALL_VISIBLE %d, "
                    "PD_COMPRESSED_PAGE %d, PD_LOGICAL_PAGE %d, PD_ENCRYPT_PAGE %d",
                    PageHasFreeLinePointers(page), PageIsFull(page), PageIsAllVisible(page),
                    PageIsCompressed(page), PageIsLogical(page), PageIsEncrypt(page)))));
    ereport(DEBUG5, (errmodule(MOD_PCR),
            (errmsg("pd_lower: %u, %s", page->pd_lower, PageIsEmpty(page) ? "empty" : "non-empty"))));
    ereport(DEBUG5, (errmodule(MOD_PCR),
            (errmsg("pd_upper: %u, %s", page->pd_upper, PageIsNew(page) ? "new" : "old"))));
    ereport(DEBUG5, (errmodule(MOD_PCR),
            (errmsg("pd_special: %u, size %u", page->pd_special, PageGetSpecialSize(page)))));
    ereport(DEBUG5, (errmodule(MOD_PCR),
            (errmsg("Page size & version: %u, %u",
            (uint16)PageGetPageSize(page), (uint16)PageGetPageLayoutVersion(page)))));
    ereport(DEBUG5, (errmodule(MOD_PCR),
            (errmsg("pd_xid_base: %lu, pd_multi_base: %lu",
                    ((HeapPageHeader)(page))->pd_xid_base, ((HeapPageHeader)(page))->pd_multi_base))));
    ereport(DEBUG5, (errmodule(MOD_PCR),
            (errmsg("pd_prune_xid: %lu",
            ((HeapPageHeader)(page))->pd_prune_xid + ((HeapPageHeader)(page))->pd_xid_base))));
}

static void ParseUbtreeCRTdslot(const char *page)
{
    unsigned short tdCount;
    PageHeaderData *ubtreePCRPage = (PageHeaderData *)page;
    tdCount = UBTreePageGetTDSlotCount(ubtreePCRPage);
    ereport(DEBUG5, (errmodule(MOD_PCR), (errmsg("UBtree CR Page TD information, nTDSlots = %hu", tdCount))));
    for (int i = 0; i < tdCount; i++) {
        UBTreeTD thisTrans = UBTreePCRGetTD(page, i + 1);
        ereport(DEBUG5, (errmodule(MOD_PCR),
                (errmsg("TD Slot #%d, xid:%ld, urp:%ld, TD_FROZEN:%d, TD_ACTIVE:%d, "
                "TD_DELETE:%d, TD_COMMITED:%d, TD_CSN:%d",
                i + 1, thisTrans->xactid, thisTrans->undoRecPtr, UBTreePCRTDIsFrozen(thisTrans),
                UBTreePCRTDIsActive(thisTrans), UBTreePCRTDIsDelete(thisTrans), UBTreePCRTDIsCommited(thisTrans),
                UBTreePCRTDHasCsn(thisTrans)))));
    }
}

void ParsePageHeader(const PageHeader page, int blkno)
{
    ParseUBtreeCRPageHeader(page, blkno);
}

static void ParseSpecialData(const char* buffer)
{
    PageHeader page = (PageHeader)buffer;

    UBTPCRPageOpaque uPCRopaque;
    
    uPCRopaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);

    ereport(DEBUG5, (errmodule(MOD_PCR),
            (errmsg("ubtree PCR index special information:\nleft sibling: %u\n right sibling: %u\n",
                    uPCRopaque->btpo_prev, uPCRopaque->btpo_next))));
    if (!P_ISDELETED(uPCRopaque)) {
        ereport(DEBUG5, (errmodule(MOD_PCR),
                (errmsg("ubtree tree level: %u", uPCRopaque->btpo.level))));
    } else {
        ereport(DEBUG5, (errmodule(MOD_PCR),
                (errmsg("next txid (deleted): %u", ((UBTPCRPageOpaque)uPCRopaque)->btpo.xact_old))));
    }
    ereport(DEBUG5, (errmodule(MOD_PCR),
            (errmsg("ubtree crpage flag, BTP_LEAF: %u, BTP_INTERNAL: %u, BTP_ROOT: %u, BTP_META: %u, BTP_DELETED: %u, "
                    "BTP_HALF_DEAD: %u, BTP_HAS_GARBAGE: %u",
                    P_ISLEAF(uPCRopaque), !P_ISLEAF(uPCRopaque), P_ISROOT(uPCRopaque), P_ISMETA(uPCRopaque),
                    P_ISDELETED(uPCRopaque), P_ISHALFDEAD(uPCRopaque), P_HAS_GARBAGE(uPCRopaque)))));
    ereport(DEBUG5, (errmodule(MOD_PCR),
            (errmsg("cycle ID: %u, last delete xid: %lu, last commit xid: %lu, td_count: %u, active tuples: %u",
                    uPCRopaque->btpo_cycleid, uPCRopaque->last_delete_xid, uPCRopaque->last_commit_xid,
                    uPCRopaque->td_count, uPCRopaque->activeTupleCount))));
}


static void ParseUBTreeDataPage(const char* buffer, int blkno)
{
    const PageHeader page = (const PageHeader)buffer;
    int i;
    int nline;
    UBTreeItemId lp;
    Item item;
    RowPtr *rowptr;

    if (blkno == 0) {
        BTMetaPageData* metad = BTPageGetMeta(page);
        ereport(DEBUG5, (errmodule(MOD_PCR),
                (errmsg("Meta information, magic number: %u, PCR version: %u, Block Number of root page: %u, "
                        "Level of root page: %u, Blknum of fast root page: %u, Level of fast root page: %u",
                        metad->btm_magic, metad->btm_version, metad->btm_root, metad->btm_level,
                        metad->btm_fastroot, metad->btm_fastlevel))));
        return;
    }

    nline = UBTPCRPageGetMaxOffsetNumber((Page)page); 
    for (i = FirstOffsetNumber; i <= nline; i++) {
        lp = UBTreePCRGetRowPtr(page, i);
        if (ItemIdIsUsed(lp)) {
            ereport(DEBUG5, (errmodule(MOD_PCR),
                    (errmsg("Tuple #%d is used, IsNormal: %u, IsFrozen: %u, IsDead: %u, IsRedirect: %u",
                            i, ItemIdIsNormal(lp), IndexItemIdIsFrozen(lp),
                            ItemIdIsDead(lp), ItemIdIsRedirected(lp)))));
        } else {
            ereport(DEBUG5, (errmodule(MOD_PCR), (errmsg("Tuple #%d is unused", i))));
        }
    }

    /* compress meta data or index special data */
    ParseSpecialData(buffer);

    return;
}

void ParseCRPage(const char* crpage, int blkno)
{
    const PageHeader page = (const PageHeader)crpage;
    uint16 headersize;
    if (CRPageIsNew(page)) {
        ereport(DEBUG5, (errmodule(MOD_PCR),
                (errmsg("CR page is new page, blkno(%d)", blkno))));
        ParsePageHeader(page, blkno);
        return;
    }

    headersize = GetPageHeaderSize(page);
    if (page->pd_lower < headersize || page->pd_lower > page->pd_upper || page->pd_upper > page->pd_special ||
        page->pd_special > BLCKSZ || page->pd_special != MAXALIGN(page->pd_special)) {
        ereport(DEBUG5, (errmodule(MOD_PCR),
                (errmsg("The page data is corrupted, corrupted page pointers: lower = %u, upper = %u, special = %u",
                        page->pd_lower, page->pd_upper, page->pd_special))));
        return;
    }

    ParsePageHeader(page, blkno);

    ParseUbtreeCRTdslot(crpage);

    ParseUBTreeDataPage(crpage, blkno);

    return;
}
