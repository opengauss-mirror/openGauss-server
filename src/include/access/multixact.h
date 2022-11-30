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

#define MULTIXACTDIR (g_instance.datadir_cxt.multixactDir)

/* Number of SLRU buffers to use for multixact */
#define NUM_MXACTOFFSET_BUFFERS 8
#define NUM_MXACTMEMBER_BUFFERS 16

/*
 * Possible multixact lock modes ("status").  The first four modes are for
 * tuple locks (FOR KEY SHARE, FOR SHARE, FOR NO KEY UPDATE, FOR UPDATE); the
 * next two are used for update and delete modes.
 */
typedef enum {
        MultiXactStatusForShare = 0x00,  /* set FOR_SHARE = 0 here for compatibility */
        MultiXactStatusForKeyShare = 0x01,
        MultiXactStatusForNoKeyUpdate = 0x02,
        MultiXactStatusForUpdate = 0x03,
        /* an update that doesn't touch "key" columns */
        MultiXactStatusNoKeyUpdate = 0x04,
        /* other updates, and delete */
        MultiXactStatusUpdate = 0x05
} MultiXactStatus;

#define MaxMultiXactStatus MultiXactStatusUpdate

/* does a status value correspond to a tuple update? */
#define ISUPDATE_from_mxstatus(status) \
                        ((status) > MultiXactStatusForUpdate)

typedef struct MultiXactMember {
        TransactionId xid;
        MultiXactStatus status;
} MultiXactMember;

/*
 * In htup.h, we define MaxTransactionId, and first four bits are reserved.
 * We use low 60 bits to record member xid and high 3 bits to record member status.
 */
#define MULTIXACT_MEMBER_XID_MASK UINT64CONST((UINT64CONST(1) << 60) - 1)
#define GET_MEMBER_XID_FROM_SLRU_XID(xid) ((xid) & MULTIXACT_MEMBER_XID_MASK)
#define GET_MEMBER_STATUS_FROM_SLRU_XID(xid) (MultiXactStatus((xid) >> 61))
#define GET_SLRU_XID_FROM_MULTIXACT_MEMBER(member) \
    (((TransactionId)((member)->status) << 61) | (((member)->xid) & MULTIXACT_MEMBER_XID_MASK))

/*
 * Defines for MultiXactOffset page sizes.	A page is the same BLCKSZ as is
 * used everywhere else in openGauss.
 *
 * We need four bytes per offset and also four bytes per member
 */
#define MULTIXACT_OFFSETS_PER_PAGE (BLCKSZ / sizeof(MultiXactOffset))
#define MULTIXACT_MEMBERS_PER_PAGE (BLCKSZ / sizeof(TransactionId))

#define MultiXactIdToOffsetPage(xid) ((xid) / (MultiXactOffset)MULTIXACT_OFFSETS_PER_PAGE)
#define MultiXactIdToOffsetEntry(xid) ((xid) % (MultiXactOffset)MULTIXACT_OFFSETS_PER_PAGE)

#define MXOffsetToMemberPage(xid) ((xid) / (TransactionId)MULTIXACT_MEMBERS_PER_PAGE)
#define MXOffsetToMemberEntry(xid) ((xid) % (TransactionId)MULTIXACT_MEMBERS_PER_PAGE)


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
    TransactionId xids[FLEXIBLE_ARRAY_MEMBER]; /* low 60 bits record member xid, high 3 bits record member status */
} xl_multixact_create;

#define MinSizeOfMultiXactCreate offsetof(xl_multixact_create, xids)

MultiXactId MultiXactIdCreate(TransactionId xid1, MultiXactStatus status1,
    TransactionId xid2, MultiXactStatus status2);
extern MultiXactId MultiXactIdExpand(MultiXactId multi, TransactionId xid, MultiXactStatus status);
extern bool MultiXactIdIsRunning(MultiXactId multi);
extern bool MultiXactIdIsCurrent(MultiXactId multi);
extern MultiXactId ReadNextMultiXactId(void);
extern bool DoMultiXactIdWait(MultiXactId multi, MultiXactStatus status, int *remaining, bool nowait, int waitSec = 0);
extern void MultiXactIdWait(MultiXactId multi, MultiXactStatus status, int *remaining, int waitSec = 0);
extern bool ConditionalMultiXactIdWait(MultiXactId multi, MultiXactStatus status, int *remaining);
extern void MultiXactIdSetOldestMember(void);
extern int GetMultiXactIdMembers(MultiXactId multi, MultiXactMember** members);

extern void AtEOXact_MultiXact(void);
extern void AtPrepare_MultiXact(void);
extern void PostPrepare_MultiXact(TransactionId xid);

extern Size MultiXactShmemSize(void);
extern void MultiXactShmemInit(void);
extern void BootStrapMultiXact(void);
extern void StartupMultiXact(void);
extern void ShutdownMultiXact(void);
extern void SetMultiXactIdLimit(MultiXactId oldest_datminmxid, Oid oldest_datoid);
extern void MultiXactGetCheckptMulti(bool is_shutdown, MultiXactId* nextMulti, MultiXactOffset* nextMultiOffset);
extern void CheckPointMultiXact(void);
extern MultiXactId GetOldestMultiXactId(void);
extern void TruncateMultiXact(MultiXactId cutoff_multi = InvalidMultiXactId);
extern void MultiXactSetNextMXact(MultiXactId nextMulti, MultiXactOffset nextMultiOffset);
extern void MultiXactAdvanceNextMXact(MultiXactId minMulti, MultiXactOffset minMultiOffset);
extern void MultiXactAdvanceOldest(MultiXactId oldestMulti, Oid oldestMultiDB);

extern void multixact_twophase_recover(TransactionId xid, uint16 info, void* recdata, uint32 len);
extern void multixact_twophase_postcommit(TransactionId xid, uint16 info, void* recdata, uint32 len);
extern void multixact_twophase_postabort(TransactionId xid, uint16 info, void* recdata, uint32 len);

extern void multixact_redo(XLogReaderState* record);
extern void multixact_desc(StringInfo buf, XLogReaderState* record);
extern const char* multixact_type_name(uint8 subtype);
extern void get_multixact_pageno(uint8 info, int64 *pageno, XLogReaderState *record);
extern void SSMultiXactShmemClear(void);

#endif /* MULTIXACT_H */
