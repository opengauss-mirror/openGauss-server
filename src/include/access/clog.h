/*
 * clog.h
 *
 * openGauss transaction-commit-log manager
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/clog.h
 */
#ifndef CLOG_H
#define CLOG_H

#include "access/xlogreader.h"
#include "lib/stringinfo.h"

/*
 * Defines for CLOG page sizes.  A page is the same BLCKSZ as is used
 * everywhere else in openGauss.
 *
 * Note: because TransactionIds are 32 bits and wrap around at 0xFFFFFFFF,
 * CLOG page numbering also wraps around at 0xFFFFFFFF/CLOG_XACTS_PER_PAGE,
 * and CLOG segment numbering at
 * 0xFFFFFFFF/CLOG_XACTS_PER_PAGE/SLRU_PAGES_PER_SEGMENT.  We need take no
 * explicit notice of that fact in this module, except when comparing segment
 * and page numbers in TruncateCLOG (see CLOGPagePrecedes).
 */

/* We need two bits per xact, so four xacts fit in a byte */
#define CLOG_BITS_PER_XACT 2
#define CLOG_XACTS_PER_BYTE 4
#define CLOG_XACTS_PER_PAGE (BLCKSZ * CLOG_XACTS_PER_BYTE)
#define CLOG_XACT_BITMASK ((1 << CLOG_BITS_PER_XACT) - 1)

#define TransactionIdToPage(xid) ((xid) / (TransactionId)CLOG_XACTS_PER_PAGE)
#define TransactionIdToPgIndex(xid) ((xid) % (TransactionId)CLOG_XACTS_PER_PAGE)
#define TransactionIdToByte(xid) (TransactionIdToPgIndex(xid) / CLOG_XACTS_PER_BYTE)
#define TransactionIdToBIndex(xid) ((xid) % (TransactionId)CLOG_XACTS_PER_BYTE)
#define PAGE_TO_TRANSACTION_ID(pageno) ((pageno) * (TransactionId)CLOG_XACTS_PER_PAGE)

#define CLogPageNoToStartXactId(pageno) ((pageno > 0)?((pageno -1) * CLOG_XACTS_PER_PAGE): 0)
/* CLog lwlock partition*/
#define CBufHashPartition(hashcode) \
    ((hashcode) % NUM_CLOG_PARTITIONS)
#define CBufMappingPartitionLock(hashcode) \
    (&t_thrd.shemem_ptr_cxt.mainLWLockArray[FirstCBufMappingLock + CBufHashPartition(hashcode)].lock)
#define CBufMappingPartitionLockByIndex(i) \
    (&t_thrd.shemem_ptr_cxt.mainLWLockArray[FirstCBufMappingLock + i].lock)

/*
 * Possible transaction statuses --- note that all-zeroes is the initial
 * state.
 *
 */
typedef int CLogXidStatus;

#define CLOG_XID_STATUS_IN_PROGRESS 0x00
#define CLOG_XID_STATUS_COMMITTED 0x01
#define CLOG_XID_STATUS_ABORTED 0x02

#define CLOGDIR (g_instance.datadir_cxt.clogDir)

/*
 * A "subcommitted" transaction is a committed subtransaction whose parent
 * hasn't committed or aborted yet.
 */
#define CLOG_XID_STATUS_SUB_COMMITTED 0x03

extern void CLogSetTreeStatus(
    TransactionId xid, int nsubxids, TransactionId* subxids, CLogXidStatus status, XLogRecPtr lsn);
extern CLogXidStatus CLogGetStatus(TransactionId xid, XLogRecPtr* lsn);
extern Size CLOGShmemBuffers(void);
extern Size CLOGShmemSize(void);
extern void CLOGShmemInit(void);
extern void BootStrapCLOG(void);
extern void StartupCLOG(void);
extern void TrimCLOG(void);
extern void ShutdownCLOG(void);
extern void CheckPointCLOG(void);
extern void ExtendCLOG(TransactionId newestXact, bool allowXlog = true);
extern void TruncateCLOG(TransactionId oldestXact);
extern bool IsCLogTruncate(XLogReaderState* record);

/* XLOG stuff */
#define CLOG_ZEROPAGE 0x00
#define CLOG_TRUNCATE 0x10

extern void clog_redo(XLogReaderState* record);
extern void clog_desc(StringInfo buf, XLogReaderState* record);
extern const char *clog_type_name(uint8 subtype);

#ifdef USE_ASSERT_CHECKING

typedef enum {
    FIT_CLOG_EXTEND_PAGE = 0,
    FIT_CLOG_READ_PAGE,
    FIT_LWLOCK_DEADLOCK,
    FIT_SEGSPC,
    FIT_SEGSTORE,
    FIT_MAX_TYPE
} FIT_type;

#endif /* FAULT_INJECTION_TEST */

#include "fmgr.h"
extern Datum gs_fault_inject(PG_FUNCTION_ARGS);
extern void SSCLOGShmemClear(void);

#endif /* CLOG_H */

#ifdef ENABLE_UT
extern void set_status_by_pages(int nsubxids, TransactionId* subxids, CLogXidStatus status, XLogRecPtr lsn);

extern void CLogSetPageStatus(TransactionId xid, int nsubxids, TransactionId* subxids, CLogXidStatus status,
    XLogRecPtr lsn, int64 pageno, bool all_xact_same_page);
extern int FIT_lw_deadlock(int n_edges);
#endif
