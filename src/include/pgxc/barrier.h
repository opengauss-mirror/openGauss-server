/* -------------------------------------------------------------------------
 *
 * barrier.h
 *
 *	  Definitions for the PITR barrier handling
 *
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/pgxc/barrier.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef XC_BARRIER_H
#define XC_BARRIER_H

#include "access/xlogreader.h"
#include "lib/stringinfo.h"

#define CREATE_BARRIER_PREPARE 'P'
#define CREATE_SWITCHOVER_BARRIER_PREPARE 'O'
#define CREATE_BARRIER_EXECUTE 'X'
#define CREATE_SWITCHOVER_BARRIER_EXECUTE 'S'
#define CREATE_BARRIER_END 'E'
#define CREATE_BARRIER_COMMIT 'C'
#define CREATE_BARRIER_QUERY_ARCHIVE 'W'
#define BARRIER_QUERY_ARCHIVE 'Q'


#define CREATE_BARRIER_PREPARE_DONE 'p'
#define CREATE_BARRIER_EXECUTE_DONE 'x'

#define BARRIER_LSN_FILE "barrier_lsn"
#define HADR_BARRIER_ID_FILE "hadr_barrier_id"
#define HADR_FAILOVER_BARRIER_ID_FILE "hadr_stop_barrier_id"
#define HADR_SWITCHOVER_BARRIER_ID_FILE "hadr_switchover_barrier_id"
#define HADR_MASTER_CLUSTER_STAT_FILE "master_hadr_stat"
#define HADR_STANDBY_CLUSTER_STAT_FILE "slave_hadr_stat"
#define HADR_SWITCHOVER_TO_MASTER "hadr_switchover_to_master"
#define HADR_SWITCHOVER_TO_STANDBY "hadr_switchover_to_standby"
#define HADR_IN_NORMAL "hadr_normal"
#define HADR_IN_FAILOVER "hadr_promote"
#define HADR_BARRIER_ID_HEAD "hadr"
#define CSN_BARRIER_ID_HEAD "csn"
#define ROACH_BARRIER_ID_HEAD "gs_roach"
#define HADR_KEY_CN_FILE "hadr_key_cn"
#define HADR_DELETE_CN_FILE "hadr_delete_cn"
#define HADR_SWITCHOVER_BARRIER_ID "hadr_switchover_000000000_0000000000000"
#define HADR_SWITCHOVER_BARRIER_TAIL "dr_switchover"
#define BARRIER_LSN_FILE_LENGTH 17
#define BARRIER_CSN_FILE_LENGTH 39
#define MAX_BARRIER_ID_LENGTH 40
#define BARRIER_ID_WITHOUT_TIMESTAMP_LEN 26
#define BARRIER_ID_TIMESTAMP_LEN 13
#define MAX_DEFAULT_LENGTH 255
#define WAIT_ARCHIVE_TIMEOUT 6000
#define MAX_BARRIER_SQL_LENGTH 60
#define BARRIER_LSN_LENGTH 30
#define MAX_BARRIER_PREFIX_LEHGTH 25

#define XLOG_BARRIER_CREATE 0x00
#define XLOG_BARRIER_COMMIT 0x10
#define XLOG_BARRIER_SWITCHOVER 0x20

#define IS_CSN_BARRIER(id) (strncmp(id, CSN_BARRIER_ID_HEAD, strlen(CSN_BARRIER_ID_HEAD)) == 0)
#define IS_HADR_BARRIER(id) (strncmp(id, HADR_BARRIER_ID_HEAD, strlen(HADR_BARRIER_ID_HEAD)) == 0)
#define IS_ROACH_BARRIER(id) (strncmp(id, ROACH_BARRIER_ID_HEAD, strlen(ROACH_BARRIER_ID_HEAD)) == 0)

#define BARRIER_EQ(barrier1, barrier2) (strcmp((char *)barrier1, (char *)barrier2) == 0)
#define BARRIER_GT(barrier1, barrier2) (strcmp((char *)barrier1, (char *)barrier2) > 0)
#define BARRIER_LT(barrier1, barrier2) (strcmp((char *)barrier1, (char *)barrier2) < 0)
#define BARRIER_LE(barrier1, barrier2) (strcmp((char *)barrier1, (char *)barrier2) <= 0)
#define BARRIER_GE(barrier1, barrier2) (strcmp((char *)barrier1, (char *)barrier2) >= 0)

extern void ProcessCreateBarrierPrepare(const char* id, bool isSwitchoverBarrier = false);
extern void ProcessCreateBarrierEnd(const char* id);
extern void ProcessCreateBarrierExecute(const char* id, bool isSwitchoverBarrier = false);
extern void ProcessCreateBarrierCommit(const char* id);

extern void CleanupBarrierLock();
extern void RequestBarrier(char* id, char* completionTag, bool isSwitchoverBarrier = false);
extern void barrier_redo(XLogReaderState* record);
extern void barrier_desc(StringInfo buf, XLogReaderState* record);
extern const char* barrier_type_name(uint8 subtype);
extern void DisasterRecoveryRequestBarrier(const char* id, bool isSwitchoverBarrier = false);
extern void ProcessBarrierQueryArchive(char* id);
extern bool is_barrier_pausable(const char* id);

#ifndef ENABLE_MULTIPLE_NODES
extern void CreateHadrSwitchoverBarrier();
#endif
extern void UpdateXLogMaxCSN(CommitSeqNo xlogCSN);


#endif
