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

#define CSNBufHashPartition(hashcode) ((hashcode) % NUM_CSNLOG_PARTITIONS)
#define CSNBufMappingPartitionLock(hashcode) (&t_thrd.shemem_ptr_cxt.mainLWLockArray[FirstCSNBufMappingLock + CSNBufHashPartition(hashcode)].lock)
#define CSNBufMappingPartitionLockByIndex(i) (&t_thrd.shemem_ptr_cxt.mainLWLockArray[FirstCSNBufMappingLock + i].lock)

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

#endif /* CSNLOG_H */
