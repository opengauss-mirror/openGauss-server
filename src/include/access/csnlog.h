/* ---------------------------------------------------------------------------------------
 * 
 * csnlog.h
 *        Commit-Sequence-Number log.
 * 
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * 
 * IDENTIFICATION
 *        src/include/access/csnlog.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef CSNLOG_H
#define CSNLOG_H

#include "access/xlog.h"
#include <atomic>

#define CSNBufHashPartition(hashcode) ((hashcode) % NUM_CSNLOG_PARTITIONS)

#define CSNLOGDIR (g_instance.datadir_cxt.csnlogDir)

extern void CSNLogSetCommitSeqNo(TransactionId xid, int nsubxids, TransactionId* subxids, CommitSeqNo csn);
extern CommitSeqNo CSNLogGetCommitSeqNo(TransactionId xid);
extern CommitSeqNo CSNLogGetNestCommitSeqNo(TransactionId xid);
extern CommitSeqNo CSNLogGetDRCommitSeqNo(TransactionId xid);

extern Size CSNLOGShmemBuffers(void);
extern Size CSNLOGShmemSize(void);
extern void CSNLOGShmemInit(void);
extern void BootStrapCSNLOG(void);
extern void StartupCSNLOG();
extern void ShutdownCSNLOG(void);
extern void CheckPointCSNLOG(void);
extern void ExtendCSNLOG(TransactionId newestXact);
extern void TruncateCSNLOG(TransactionId oldestXact);
void SSCSNLOGShmemClear(void);

/* USE_UB_TXN_CACHE - BEGIN */

#define UB_CSNLOG_SLOT_BITS 27
#define UB_CSNLOG_TIMELINE_BITS (64 - UB_CSNLOG_SLOT_BITS)
#define UB_CSNLOG_BUFFER_SLOTS (1ULL << UB_CSNLOG_SLOT_BITS)
#define UB_CSNLOG_MAX_TIMELINE ((1ULL << UB_CSNLOG_TIMELINE_BITS) - 1)
#define UB_CSNLOG_HALF_BITS 64

typedef std::atomic<__uint128_t> UBCSNLogBufferSlot;

static inline uint64 UBCSNLogCalSlotIndex(TransactionId xid)
{
    return (uint64)(xid % UB_CSNLOG_BUFFER_SLOTS);
}

static inline uint64 UBCSNLogExpectedTimeline(TransactionId xid)
{
    return (uint64)(xid / UB_CSNLOG_BUFFER_SLOTS);
}

static inline __uint128_t UBCSNLogPackSlot(uint64 timelineid, uint64 csn)
{
    return ((__uint128_t)timelineid << UB_CSNLOG_HALF_BITS) | ((__uint128_t)csn);
}

static inline uint64 UBCSNLogUnpackTimelineId(__uint128_t slot_val)
{
    return (uint64)(slot_val >> UB_CSNLOG_HALF_BITS);
}

static inline uint64 UBCSNLogUnpackCSN(__uint128_t slot_val)
{
    return (uint64)(slot_val & (((__uint128_t)1 << UB_CSNLOG_HALF_BITS) - 1));
}

static inline bool UBCSNLogIsValidXid(TransactionId xid)
{
    return UBCSNLogExpectedTimeline(xid) <= UB_CSNLOG_MAX_TIMELINE;
}

typedef struct {
    UBCSNLogBufferSlot slots[UB_CSNLOG_BUFFER_SLOTS];
} UBCSNLogBuffer;

extern void UBCSNLogBufferInit(UBCSNLogBuffer *buf);
extern void UBCSNLogBufferSetSlot(UBCSNLogBuffer *buf, TransactionId xid, uint64 csn);
extern size_t UBCSNLogBufferSize(void);
extern void UBCSNLogShmemInit(void);
extern bool UBGetCSNFromPrimary(TransactionId xid, uint64 *csn);

/* USE_UB_TXN_CACHE - END */

#endif /* CSNLOG_H */
