/*
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
 * pgFdwRemote.h
 *
 * IDENTIFICATION
 *	 src/include/pgxc/pgFdwRemote.h
 *
 * -------------------------------------------------------------------------
 */
 
#ifndef PG_FDW_REMOTE_H
#define PG_FDW_REMOTE_H

#include "c.h"
#include "nodes/nodes.h"
#include "utils/snapshot.h"

typedef enum PgFdwMessageTag {
    PGFDW_GET_TABLE_INFO = 0,
    PGFDW_ANALYZE_TABLE,
    PGFDW_QUERY_PARAM,
    PGFDW_GET_VERSION,
    PGFDW_GET_ENCODE
} PgFdwMessageTag;

typedef enum PgFdwCheckResult {
    PGFDW_CHECK_OK = 0,
    PGFDW_CHECK_TABLE_FAIL,
    PGFDW_CHECK_RELKIND_FAIL,
    PGFDW_CHECK_COLUNM_FAIL
} PgFdwCheckResult;

/*
 * the cooperation analysis version control
 * if modify the message which is sended and received, u need do such step
 * 1.enmu GcFdwVersion, need add version,
 *		e.g. GCFDW_VERSION_V1R8C10_1 = 101		GCFDW_VERSION_V1R9C00 = 200
 * 2.modify GCFDW_VERSION, the value need be set the lastest version.
 * 3.new logic, need use if conditon with new version
 *		e.g. if (gc_fdw_run_version >= GCFDW_VERSION_V1R8C10_1)
 *			 { pq_sendint64(&retbuf, u_sess->debug_query_id); }
 */
typedef enum GcFdwVersion {
    GCFDW_VERSION_V1R8C10 = 100,   /* the first version */
    GCFDW_VERSION_V1R8C10_1 = 101, /* add foreign table option : encode type */
    GCFDW_VERSION_V1R9C00 = 200
} GcFdwVersion;

#define GCFDW_VERSION GCFDW_VERSION_V1R8C10_1

typedef struct PgFdwRemoteInfo {
    NodeTag type;
    char reltype;      /* relation type */
    int datanodenum;   /* datanode num  */
    Size snapsize;     /* the really size of snapshot */
    Snapshot snapshot; /* snapshot */
} PgFdwRemoteInfo;

extern bool PgfdwGetRelAttnum(int2vector* keys, PGFDWTableAnalyze* info);
extern bool PgfdwGetRelAttnum(TupleTableSlot* slot, PGFDWTableAnalyze* info);
extern void pgfdw_send_query(PGXCNodeAllHandles* pgxc_handles, char* query, RemoteQueryState** remotestate);
extern void PgFdwReportError(PgFdwCheckResult check_result);
extern void pgfdw_node_report_error(RemoteQueryState* combiner);
extern void PgFdwSendSnapshot(StringInfo buf, Snapshot snapshot);
extern void PgFdwSendSnapshot(StringInfo buf, Snapshot snapshot, Size snap_size);
extern Snapshot PgFdwRecvSnapshot(StringInfo buf);
extern void PgFdwRemoteSender(PGXCNodeAllHandles* pgxc_handles, const char* keystr, int len, PgFdwMessageTag tag);
extern void PgFdwRemoteReply(StringInfo msg);
extern void PgFdwRemoteReceiver(PGXCNodeAllHandles* pgxc_handles, void* info, int size);
extern bool PgfdwGetTuples(int cn_conn_count, PGXCNodeHandle** pgxc_connections,
                            RemoteQueryState* remotestate, TupleTableSlot* scanSlot);
extern void FetchGlobalPgfdwStatistics(VacuumStmt* stmt, bool has_var, PGFDWTableAnalyze* info);

#endif


