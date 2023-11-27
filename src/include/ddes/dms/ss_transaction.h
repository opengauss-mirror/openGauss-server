/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 * 
 * ss_transaction.h
 *  ss_transaction
 * 
 * 
 * IDENTIFICATION
 *        src/include/ddes/dms/ss_transaction.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef SRC_INCLUDE_DDES_SS_TRANSACTION_H
#define SRC_INCLUDE_DDES_SS_TRANSACTION_H

#include "ddes/dms/ss_common_attr.h"
#include "access/transam.h"
#include "storage/sinval.h"

#define DMS_NO_RUNNING_BACKENDS (DMS_SUCCESS)
#define DMS_EXIST_RUNNING_BACKENDS (DMS_ERROR)

typedef struct SSBroadcastXmin {
    SSBroadcastOp type; // must be first
    TransactionId xmin;
} SSBroadcastXmin;

typedef struct SSBroadcastXminAck {
    SSBroadcastOpAck type; // must be first
    TransactionId xmin;
} SSBroadcastXminAck;

typedef struct SSBroadcastSnapshot {
    SSBroadcastOp type; // must be first
    TransactionId xmin;
    TransactionId xmax;
    CommitSeqNo csn;
} SSBroadcastSnapshot;

typedef struct SSBroadcastSI {
    SSBroadcastOp type; // must be first
    Oid tablespaceid;
    SharedInvalidationMessage msg;
} SSBroadcastSI;

typedef struct SSBroadcastSegDropTL {
    SSBroadcastOp type; // must be first
    uint32 seg_drop_timeline;
} SSBroadcastSegDropTL;

typedef struct SSBroadcastDropRelAllBuffer {
    SSBroadcastOp type; // must be first
    int32 size;
    RelFileNode rnodes[FLEXIBLE_ARRAY_MEMBER];
} SSBroadcastDropRelAllBuffer;

typedef struct SSBroadcastDropRelRangeBuffer {
    SSBroadcastOp type; // must be first
    RelFileNode node;
    ForkNumber forkNum;
    BlockNumber firstDelBlock;
} SSBroadcastDropRelRangeBuffer;

typedef struct SSBroadcastDropDBAllBuffer {
    SSBroadcastOp type; // must be first
    Oid dbid;
} SSBroadcastDropDBAllBuffer;

typedef struct SSBroadcastDropSegSpace {
    SSBroadcastOp type; // must be first
    Oid spcNode;
    Oid dbNode;
} SSBroadcastDropSegSpace;

typedef struct SSBroadcasDbBackends {
    SSBroadcastOp type; // must be first
    Oid dbid;
} SSBroadcastDbBackends;

typedef struct SSBroadcasDbBackendsAck {
    SSBroadcastOpAck type; // must be first
    int count;
} SSBroadcastDbBackendsAck;

Snapshot SSGetSnapshotData(Snapshot snapshot);
CommitSeqNo SSTransactionIdGetCommitSeqNo(TransactionId transactionId, bool isCommit, bool isMvcc, bool isNest,
    Snapshot snapshot, bool* sync);
void SSTransactionIdDidCommit(TransactionId transactionId, bool *ret_did_commit);
void SSTransactionIdIsInProgress(TransactionId transactionId, bool *in_progress);
TransactionId SSMultiXactIdGetUpdateXid(TransactionId xmax, uint16 t_infomask, uint16 t_infomask2);
bool SSGetOldestXminFromAllStandby();
int SSGetOldestXmin(char *data, uint32 len, char *output_msg, uint32 *output_msg_len);
int SSGetOldestXminAck(SSBroadcastXminAck *ack_data);
void SSIsPageHitDms(RelFileNode& node, BlockNumber page, int pagesNum, uint64 *pageMap, int *bitCount);
void SSSendSharedInvalidMessages(const SharedInvalidationMessage* msgs, int n);
void SSBCastDropRelAllBuffer(RelFileNode *rnodes, int rnode_len);
void SSBCastDropRelRangeBuffer(RelFileNode node, ForkNumber forkNum, BlockNumber firstDelBlock);
void SSBCastDropDBAllBuffer(Oid dbid);
void SSBCastDropSegSpace(Oid spcNode, Oid dbNode);
int SSProcessSharedInvalMsg(char *data, uint32 len);
void SSUpdateSegDropTimeline(uint32 seg_drop_timeline);
int SSProcessSegDropTimeline(char *data, uint32 len);
int SSProcessDropRelAllBuffer(char *data, uint32 len);
int SSProcessDropRelRangeBuffer(char *data, uint32 len);
int SSProcessDropDBAllBuffer(char *data, uint32 len);
int SSProcessDropSegSpace(char *data, uint32 len);
int SSCheckDbBackends(char *data, uint32 len, char *output_msg, uint32 *output_msg_len);
int SSCheckDbBackendsAck(char *data, unsigned int len);
bool SSCheckDbBackendsFromAllStandby(Oid dbid);
void SSStandbyUpdateRedirectInfo();
void SSSendLatestSnapshotToStandby(TransactionId xmin, TransactionId xmax, CommitSeqNo csn);
int SSUpdateLatestSnapshotOfStandby(char *data, uint32 len);
int SSReloadReformCtrlPage(uint32 len);
void SSRequestAllStandbyReloadReformCtrlPage();
bool SSCanFetchLocalSnapshotTxnRelatedInfo();

#endif
