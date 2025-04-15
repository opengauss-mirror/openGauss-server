/* -------------------------------------------------------------------------
 *
 * crbuf.h
 *	  Interface of CR buffer pool.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/buf/crbuf.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CRBUF_H
#define CRBUF_H

#include "utils/hsearch.h"
#include "storage/buf/buf_internals.h"
#include "knl/knl_thread.h"

#define CRBUF_REFCOUNT_ONE 1LU
#define CRBUF_REFCOUNT_MASK ((1LU << 17) - 1)
#define CRBUF_FLAG_MASK 0xFFC0000000000000LU

/* Get refcount from buffer state */
#define CRBUF_STATE_GET_REFCOUNT(state) (((state) & 0xFFFFFFFF))

/*
 * Flags for cr buffer descriptors
 *
 * Note: TAG_VALID essentially means that there is a buffer hashtable
 * entry associated with the buffer's tag.
*/
#define CRBM_LOCKED (1LU << 22 << 32)           /* buffer header is locked */
#define CRBM_VALID (1LU << 23 << 32)            /* data is valid */
#define CRBM_TAG_VALID (1LU << 24 << 32)        /* tag is assigned */
#define CRBM_PIN_COUNT_WAITER (1LU << 25 << 32) /* have waiter for sole pin */

#define CR_BUFFER_NUM 128000

#define GetCRBufferDescriptor(id) (&t_thrd.storage_cxt.CRBufferDescriptors[(id)].crbuffdesc)

/*
 * CRBuferGetBlock
 *      Returns a reference to a buffer page image associated with a buff id.
 * Note:
 *      Assumes bufer is valid.
*/
#define CRBufferGetBlock(id) (Block)(t_thrd.storage_cxt.CRBufferBlocks + ((Size)((uint)id)) *BLCKSZ)

/*
 * BufferGetPage
 *      Returns the page associated with a buffer.
*/
#define CRBufferGetPage(id) ((Page)CRBufferGetBlock(id))

#define CRBufTableHashPartition(hashcode) ((hashcode) % NUM_BUFFER_PARTITIONS)
#define CRBufMappingPartitionLock(hashcode) \
    (&t_thrd.shemem_ptr_cxt.mainLWLockArray[FirstCRBufMappingLock + CRBufTableHashPartition(hashcode)].lock)
#define CRBufMappingPartitionLockByIndex(i) \
    (&t_thrd.shemem_ptr_cxt.mainLWLockArray[FirstCRBufMappingLock + (i)].lock)

typedef struct {
    BufferTag key;
    int id;
} CRBufEntry;

typedef struct CRBufferDesc {
    BufferTag tag;
    int buf_id;
    CommitSeqNo csn;
    CommandId cid;
    uint64 rsid;
    bool usable;
    pg_atomic_uint64 state;
    int cr_buf_next;
    int cr_buf_prev;
    int lru_next;
    int lru_prev;
} CRBufferDesc;

typedef union CRBufferDescPadded {
    CRBufferDesc crbuffdesc;
    char pad[BUFFERDESC_PAD_TO_SIZE];
} CRBufferDescPadded;

inline void UnlockCRBufHdr(CRBufferDesc *desc, uint64 s)
{
    TsAnnotateHappensBefore(&desc->state);
    pg_write_barrier();
    pg_atomic_write_u32((((volatile uint32 *)&(desc)->state) + 1), (((s) & (~CRBM_LOCKED)) >> 32));
}

void InitCRBufPool(void);
void InitCRBufPoolAccess(void);

CRBufferDesc *ReadCRBuffer(Relation reln, BlockNumber block_num, CommitSeqNo csn, CommandId cid);
CRBufferDesc *AllocCRBuffer(Relation reln, ForkNumber forkNum, BlockNumber blockNum, CommitSeqNo csn, CommandId cid);
CRBufferDesc *CRReadOrAllocBuffer(Relation reln, BlockNumber block_num, CommitSeqNo csn, CommandId cid);
void ReleaseCRBuffer(Buffer buffer);
void CRPoolScan();
void CRBufferUnused(Buffer base_buffer);
void ParseCRPage(const char *crpage, int blkno);
#endif
