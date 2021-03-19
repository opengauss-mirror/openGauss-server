/*
 * multixact.h
 *
 * PostgreSQL multi-transaction-log manager
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/multixact.h
 */
#ifndef MULTIXACT_H
#define MULTIXACT_H

#include "access/xlogreader.h"
#include "lib/stringinfo.h"

#define InvalidMultiXactId UINT64CONST(0)
#define FirstMultiXactId UINT64CONST(1)
#define MaxMultiXactId UINT64CONST(0xFFFFFFFFFFFFFFFF)

#define MultiXactIdIsValid(multi) ((multi) != InvalidMultiXactId)

#define MaxMultiXactOffset UINT64CONST(0xFFFFFFFFFFFFFFFF)

/* Number of SLRU buffers to use for multixact */
#define NUM_MXACTOFFSET_BUFFERS 8
#define NUM_MXACTMEMBER_BUFFERS 16

/* ----------------
 *		multixact-related XLOG entries
 * ----------------
 */

#define XLOG_MULTIXACT_ZERO_OFF_PAGE 0x00
#define XLOG_MULTIXACT_ZERO_MEM_PAGE 0x10
#define XLOG_MULTIXACT_CREATE_ID 0x20

#define XLOG_MULTIXACT_MASK 0x70
#define XLOG_MULTIXACT_INT64_PAGENO 0x80


typedef struct xl_multixact_create {
    MultiXactId mid;                           /* new MultiXact's ID */
    MultiXactOffset moff;                      /* its starting offset in members file */
    int32 nxids;                               /* number of member XIDs */
    TransactionId xids[FLEXIBLE_ARRAY_MEMBER]; /* VARIABLE LENGTH ARRAY */
} xl_multixact_create;

#define MinSizeOfMultiXactCreate offsetof(xl_multixact_create, xids)

extern MultiXactId MultiXactIdCreate(TransactionId xid1, TransactionId xid2);
extern MultiXactId MultiXactIdExpand(MultiXactId multi, TransactionId xid);
extern bool MultiXactIdIsRunning(MultiXactId multi);
extern bool MultiXactIdIsCurrent(MultiXactId multi);
extern MultiXactId ReadNextMultiXactId(void);
extern void MultiXactIdWait(MultiXactId multi, bool allow_con_update = false);
extern bool ConditionalMultiXactIdWait(MultiXactId multi);
extern void MultiXactIdSetOldestMember(void);
extern int GetMultiXactIdMembers(MultiXactId multi, TransactionId** xids);

extern void AtEOXact_MultiXact(void);
extern void AtPrepare_MultiXact(void);
extern void PostPrepare_MultiXact(TransactionId xid);

extern Size MultiXactShmemSize(void);
extern void MultiXactShmemInit(void);
extern void BootStrapMultiXact(void);
extern void StartupMultiXact(void);
extern void ShutdownMultiXact(void);
extern void MultiXactGetCheckptMulti(bool is_shutdown, MultiXactId* nextMulti, MultiXactOffset* nextMultiOffset);
extern void CheckPointMultiXact(void);
extern void MultiXactSetNextMXact(MultiXactId nextMulti, MultiXactOffset nextMultiOffset);
extern void MultiXactAdvanceNextMXact(MultiXactId minMulti, MultiXactOffset minMultiOffset);

extern void multixact_twophase_recover(TransactionId xid, uint16 info, void* recdata, uint32 len);
extern void multixact_twophase_postcommit(TransactionId xid, uint16 info, void* recdata, uint32 len);
extern void multixact_twophase_postabort(TransactionId xid, uint16 info, void* recdata, uint32 len);

extern void multixact_redo(XLogReaderState* record);
extern void multixact_desc(StringInfo buf, XLogReaderState* record);

#endif /* MULTIXACT_H */
