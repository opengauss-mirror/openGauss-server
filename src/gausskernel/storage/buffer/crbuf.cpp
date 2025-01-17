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

#include "storage/buf/crbuf.h"
#include "knl/knl_instance.h"
#include "utils/memutils.h"
#include "utils/elog.h"
#include "access/hash.h"
#include "storage/smgr/relfilenode_hash.h"
#include "tsan_annotation.h"

#define CR_BUFFER_NUM 12800
#define INVALID_BUFF 0
#define PCRP_RESERVED_PAGE_COUNT 6

extern uint32 hashquickany(uint32 seed, register const unsigned char *data, register int len);

#define UnlockCRBufHdr(desc, s)                                                 \
    do {                                                                        \
        TsAnnotateHappensBefore(&desc->state);                                  \
        pg_write_barrier();                                                     \ 
        pg_atomic_write_u32((((volatile uint32 *)&(desc)->state) + 1), (((s) & (~CRBM_LOCKED)) >> 32)); \
    } while (0)

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
        if (!(old_buf_state & CRBM_LOCKED))
            break;
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

void CRBufListRemove(CRBufferDesc *buf_desc_head, CommitSeqNo csn, CommandId cid, CRBufferDesc **removed_buf_desc,
                    bool *remove_head, Buffer *new_head_buff_id)
{
    CRBufferDesc *buf_desc_ptr = buf_desc_head;
    CRBufferDesc *buf_prev = NULL;
    CRBufferDesc *buf_next = NULL;

    *remove_head = false;
    for (;;) {
        if (buf_desc_ptr->csn == csn && buf_desc_ptr->cid == cid) {
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

CRBufferDesc *CRBufListFetch(int buf_id, CommitSeqNo query_csn, CommandId query_cid, int rsid)
{
    CRBufferDesc *buf_desc_ptr = NULL;

    while (buf_id != InvalidBuffer) {
        buf_desc_ptr = GetCRBufferDescriptor(buf_id);

        if (buf_desc_ptr->usable && query_csn == buf_desc_ptr->csn && query_cid == buf_desc_ptr->cid && rsid == buf_desc_ptr->rsid) {
            ereport(DEBUG3, (errmodule(MOD_PCR), (errmsg("CRBufListFetch: fetch from cr buffer list found! buf_id %d, csn %ld, cid %d\n",
                    buf_id, query_csn, query_cid))));
            return buf_desc_ptr;
        }
        buf_id = buf_desc_ptr->cr_buf_next;
    }
    ereport(DEBUG3, (errmodule(MOD_PCR), (errmsg("CRBufListFetch: fetch from cr buffer list notfound! buf_id: %d, csn: %ld\n", buf_id, query_csn))));
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
int CRBufTableInsert(BufferTag *tag, uint32 hashcode, int buf_id)
{
    CRBufEntry *result = NULL;
    bool found = false;

    Assert(buf_id >= 0);            /* -1 is reserved for not-in-table */

    result = (CRBufEntry *)buf_hash_operate<HASH_ENTER>(t_thrd.storage_cxt.CRSharedBufHash, tag, hashcode, &found);

    if (found) {
        return result->id;
    }

    result->id = buf_id;
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
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmodule(MOD_PCR), (errmsg("shared buffer hash table corrupted."))));
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

void CRBufTableRemoveWithCsn(BufferTag *tag, uint32 hashcode, CommitSeqNo csn, CommandId cid, CRBufferDesc **removed_buff_desc)
{
    CRBufEntry *result = NULL;
    Buffer buff_id;
    Buffer new_head_buff_id = InvalidBuffer;
    CRBufferDesc *buff_desc_head = NULL;
    bool remove_head = false;

    buff_id = CRBufTableLookup(tag, hashcode);
    if (buff_id < 0) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmodule(MOD_PCR), (errmsg("shared buffer hash table corrupted."))));
        return;
    }

    buff_desc_head = GetCRBufferDescriptor(buff_id);

    CRBufListRemove(buff_desc_head, csn, cid, removed_buff_desc, &remove_head, &new_head_buff_id);

    if (remove_head == true) {
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
        ShmemInitStruct("CR Buffer Descriptors",CR_BUFFER_NUM * sizeof(CRBufferDescPadded) + PG_CACHE_LINE_SIZE,
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
    int page_count = 1;

    while (buf_desc_ptr->cr_buf_next != InvalidBuffer) {
        buf_desc_ptr = GetCRBufferDescriptor(buf_desc_ptr->cr_buf_next);
        if (buf_desc_ptr->csn < minCsn) {
            minCsn = buf_desc_ptr->csn;
            oldestCRBuf = buf_desc_ptr;
        } else if (buf_desc_ptr->csn == minCsn && buf_desc_ptr->cid < minCid) {
            minCid = buf_desc_ptr->cid;
            oldestCRBuf = buf_desc_ptr;
        }
        page_count++;
    }
    if ((page_count >= PCRP_RESERVED_PAGE_COUNT) && (CRBUF_STATE_GET_REFCOUNT(oldestCRBuf->state) ==0)) {
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
    uint64 buf_state;
    LWLock *hashbucket_lock = NULL;
    uint32 hashcode;
    bool valid = false;

    RelationOpenSmgr(reln);
    UndoPersistence persistence = UndoPersistenceForRelation(reln);
    int rsid = t_thrd.undo_cxt.zids[persistence];

    INIT_BUFFERTAG(new_tag, reln->rd_smgr->smgr_rnode.node, MAIN_FORKNUM, block_num);

    Assert(t_thrd.storage_cxt.CRSharedBufHash != NULL);

    hashcode = CRBufTableHashCode(&new_tag);
    hashbucket_lock = CRBufMappingPartitionLock(hashcode);

    LWLockAcquire(hashbucket_lock, LW_SHARED);

    buf_id = CRBufTableLookup(&new_tag, hashcode);

    if (buf_id >= 0) {
        buf_desc = GetCRBufferDescriptor(buf_id);
        ereport(DEBUG3, (errmodule(MOD_PCR), (errmsg("ReadCRBuffer: get from cr buffer hashtable found! (%u, %d) buf_id %d, csn %ld, cid %d\n",
                    reln->rd_smgr->smgr_rnode.node.relNode, block_num, buf_id, buf_desc->csn, buf_desc->cid))));
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

void InitCRBufDesc(CRBufferDesc *buf_desc, BufferTag new_tag, CommitSeqNo csn, CommandId cid, int rsid)
{
    uint64 buf_state;

    buf_desc->tag = new_tag;
    buf_desc->csn = csn;
    buf_desc->cid = cid;
    buf_desc->rsid = rsid;
    buf_desc->usable = true;
	buf_state = pg_atomic_read_u64(&buf_desc->state);
    buf_state &= ~(CRBM_VALID);
    Assert(!(buf_state & CRBM_LOCKED));
    buf_state &= ~(CRBM_LOCKED);
    buf_state |= CRBM_TAG_VALID;
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

    if (buff_desc->lru_prev != NULL) {
        lru_prev_buf_desc->lru_next = buff_desc->lru_next;
    }

    if (buff_desc->lru_next != NULL) {
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
    Buffer lru_last = g_instance.crbuf_cxt.cr_lru_last;
    CRBufferDesc *buff_desc = NULL;
    uint64 buff_state;
    CRBufferDesc *removed_buff_desc = NULL;

    LWLockAcquire(g_instance.crbuf_cxt.cr_buf_lru_lock, LW_EXCLUSIVE);
    while (lru_last != InvalidBuffer) {
        buff_desc = GetCRBufferDescriptor(lru_last);
        if (CRBUF_STATE_GET_REFCOUNT(buff_desc->state) == 0) {
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

            /* remove buff_desc from BufTable & LRU */
            CRBufTableRemoveWithCsn(&buff_desc->tag, hashcode, buff_desc->csn, buff_desc->cid, &removed_buff_desc);
            CRBufLruRemove(removed_buff_desc);
            LWLockRelease(hashbucket_lock);

            CLEAR_BUFFERTAG(removed_buff_desc->tag);
            removed_buff_desc->csn = InvalidCommitSeqNo;
            removed_buff_desc->usable = false;
			pg_atomic_init_u64(&removed_buff_desc->state, 0);
            removed_buff_desc->cr_buf_next = InvalidBuffer;
            removed_buff_desc->cr_buf_prev = InvalidBuffer;
            break;
        }
        lru_last = buff_desc->lru_prev;
    }

    if (lru_last == InvalidBuffer) {
        LWLockRelease(g_instance.crbuf_cxt.cr_buf_lru_lock);
        return NULL;
    }

    LWLockRelease(g_instance.crbuf_cxt.cr_buf_lru_lock);
    return removed_buff_desc;
}

CRBufferDesc *CRBufferAllocOrRecycle(void)
{
    CRBufferDesc *buff_desc = NULL;

    for(;;) {
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
    bool found = false;
    LWLock *hashbucket_lock = NULL;
    uint32 hashcode;
    bool valid;
    RelFileNode rel_file_node = reln->rd_node;
    UndoPersistence persistence = UndoPersistenceForRelation(reln);
    int rsid = t_thrd.undo_cxt.zids[persistence];

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
            ereport(DEBUG3, (errmodule(MOD_PCR), (errmsg("AllocCRBuffer: alloc from bucket! (%u,%d,%d) buf_id %d, csn %ld, cid %d\n",
                    rel_file_node.relNode, forkNum, blockNum, buf_id, buf_desc->csn, buf_desc->cid))));
            InitCRBufDesc(buf_desc, new_tag, csn, cid, rsid);
            valid = PinCRBuffer(buf_desc);

            LWLockAcquire(g_instance.crbuf_cxt.cr_buf_lru_lock, LW_EXCLUSIVE);
            CRBufLruRefresh(buf_desc);
            LWLockRelease(g_instance.crbuf_cxt.cr_buf_lru_lock);

            LWLockRelease(hashbucket_lock);
            return buf_desc;
        }
    }
    LWLockRelease(hashbucket_lock);
    /* Alloc CR buffer from SHM or recycle */
    CRBufferDesc *new_buf_desc = CRBufferAllocOrRecycle();
    Assert(new_buf_desc);

    ereport(DEBUG3, (errmodule(MOD_PCR), (errmsg("AllocCRBuffer: alloc from shm or recycle! (%u,%d,%d) buf_id %d, csn %ld, cid %d\n",
            rel_file_node.relNode, forkNum, blockNum, new_buf_desc->buf_id, new_buf_desc->csn, new_buf_desc->cid))));

    InitCRBufDesc(new_buf_desc, new_tag, csn, cid, rsid);
    valid = PinCRBuffer(new_buf_desc);

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
    bool foundPtr = FALSE;
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
	uint64 old_buf_state;
	LWLock *hashbucket_lock = NULL;
	uint32 hashcode;
	bool valid = false;

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
            ereport(DEBUG3, (errmodule(MOD_PCR), (errmsg("CRBufferUnused: buf_id %d, page (%u,%d), csn %ld, cid %d\n",
                buf_desc->buf_id, (buf_desc->tag).rnode.relNode, (buf_desc->tag).blockNum, buf_desc->csn, buf_desc->cid))));

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
		fprintf(stderr, "buf_id %d, page (%u,%d), csn %ld, cid %d, cr_buf_next %d\n",
    	        buf->buf_id, (buf->tag).rnode.relNode, (buf->tag).blockNum, buf->csn, buf->cid, buf->cr_buf_next);
	}
}

void CRPoolScan()
{
    CRBufferDesc *buf = NULL;
    int i;

    fprintf(stderr, "HashTable Info:\n");
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

    fprintf(stderr, "LRU Info:\n");
    Buffer lru_cursor = g_instance.crbuf_cxt.cr_lru_first;
    while (lru_cursor != InvalidBuffer) {
        buf = GetCRBufferDescriptor(lru_cursor);
		fprintf(stderr, "%d ", lru_cursor);
        lru_cursor = buf->lru_next;
    }
	fprintf(stderr, "\n\n");
}
