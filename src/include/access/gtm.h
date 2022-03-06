/* -------------------------------------------------------------------------
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * -------------------------------------------------------------------------
 *
 * gtm.h
 *    Module interfacing with GTM definitions
 *
 * IDENTIFICATION
 *    src/include/access/gtm.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef ACCESS_GTM_H
#define ACCESS_GTM_H

#include "gtm/gtm_c.h"
#include "workload/workload.h"
#include "commands/sequence.h"

extern bool IsGTMConnected(void);
extern GtmHostIndex InitGTM(bool useCache = true);
extern void CloseGTM(void);

extern bool SetDisasterClusterGTM(char *disasterCluster);
extern bool DelDisasterClusterGTM();
extern bool GetDisasterClusterGTM(char** disasterCluster);

extern GTM_TransactionKey BeginTranGTM(GTM_Timestamp *timestamp);
extern GlobalTransactionId GetGxidGTM(GTM_TransactionKey txnKey, bool is_sub_xact);
extern CommitSeqNo GetCSNGTM();
extern CommitSeqNo CommitCSNGTM(bool need_clean);
extern TransactionId GetGTMGlobalXmin();
extern GlobalTransactionId BeginTranAutovacuumGTM(void);
extern int CommitTranGTM(GlobalTransactionId gxid, TransactionId *childXids, int nChildXids);
extern int CommitTranHandleGTM(GTM_TransactionKey txnKey, GlobalTransactionId transactionId,
                               GlobalTransactionId &outgxid);
extern int RollbackTranGTM(GlobalTransactionId gxid, GlobalTransactionId *childXids, int nChildXids);
extern int RollbackTranHandleGTM(GTM_TransactionKey txnKey, GlobalTransactionId &outgxid);
extern int StartPreparedTranGTM(GlobalTransactionId gxid, const char *gid, const char *nodestring);
extern int PrepareTranGTM(GlobalTransactionId gxid);
extern int GetGIDDataGTM(const char *gid, GlobalTransactionId *gxid, GlobalTransactionId *prepared_gxid,
                         char **nodestring);
extern int CommitPreparedTranGTM(GlobalTransactionId gxid, GlobalTransactionId prepared_gxid);

extern GTM_Snapshot GetSnapshotGTM(GTM_TransactionKey txnKey, GlobalTransactionId gxid, bool canbe_grouped,
                                   bool is_vacuum);
extern GTM_Snapshot GetSnapshotGTMLite(void);
extern GTM_SnapshotStatus GetGTMSnapshotStatus(GTM_TransactionKey txnKey);
extern GTMLite_Status GetGTMLiteStatus(void);
extern GTM_Snapshot GetSnapshotGTMDR(void);

/* Sequence interface APIs with GTM */
extern GTM_UUID GetSeqUUIDGTM();
extern int CreateSequenceWithUUIDGTM(FormData_pg_sequence seq, GTM_UUID uuid);
extern GTM_Sequence GetNextValGTM(Form_pg_sequence seq, GTM_Sequence range, GTM_Sequence *rangemax, GTM_UUID uuid);
extern int SetValGTM(GTM_UUID seq_uuid, GTM_Sequence nextval, bool iscalled);

extern int AlterSequenceGTM(GTM_UUID seq_uuid, GTM_Sequence increment, GTM_Sequence minval, GTM_Sequence maxval,
                            GTM_Sequence startval, GTM_Sequence lastval, bool cycle, bool is_restart);
extern int DropSequenceGTM(GTM_UUID seq_uuid, const char *dbname = NULL);
extern int RenameSequenceGTM(char *seqname, char *newseqname, GTM_SequenceKeyType keytype = GTM_SEQ_FULL_NAME);
/* Barrier */
extern int ReportBarrierGTM(const char *barrier_id);
/* Set gtm rw timeout */
extern void SetGTMrwTimeout(int timeout);
/* Initiate GTM connection and report connected GTM host timeline to MyBEEntry */
extern void InitGTM_Reporttimeline(void);
extern void SetGTMInterruptFlag();
extern bool PingGTM(struct gtm_conn* conn);

extern bool SetConsistencyPointCSNGTM(CommitSeqNo consistencyPointCSN);

#endif /* ACCESS_GTM_H */
