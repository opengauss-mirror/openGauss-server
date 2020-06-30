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
 * execStream.cpp
 *
 * IDENTIFICATION
 *    src/common/backend/pgxc_single/stream/execStream.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#ifdef PGXC
#include "commands/trigger.h"
#endif
#include "executor/executor.h"
#include "executor/nodeRecursiveunion.h"
#include "pgxc/execRemote.h"
#include "nodes/nodes.h"
#include "access/printtup.h"
#include "pgxc/copyops.h"
#include "pgxc/nodemgr.h"
#include "pgxc/poolmgr.h"
#include "pgxc/locator.h"
#include "pgxc/pgxc.h"
#include "parser/parse_type.h"
#include "parser/parsetree.h"
#include "utils/memutils.h"
#include "commands/dbcommands.h"
#include "miscadmin.h"
#include "libpq/ip.h"
#include "libpq/libpq.h"
#include "libcomm/libcomm.h"
#include "libpq/pqformat.h"
#include <sys/poll.h>
#include "executor/execStream.h"
#include "postmaster/postmaster.h"
#include "access/transam.h"
#include "gssignal/gs_signal.h"
#include "utils/anls_opt.h"
#include "utils/distribute_test.h"
#include "utils/guc_tables.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/combocid.h"
#include "storage/procarray.h"
#include "vecexecutor/vecstream.h"
#include "vecexecutor/vectorbatch.h"
#include "access/hash.h"
#include "pgstat.h"
#include "tcop/tcopprot.h"
#include "distributelayer/streamTransportTcp.h"
#include "distributelayer/streamTransportSctp.h"
#include "optimizer/nodegroups.h"
#include "optimizer/dataskew.h"
#include "instruments/instr_unique_sql.h"

void AddCheckMessage(StringInfo msg_new, StringInfo msg_org, bool is_stream, unsigned int planNodeId)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

bool executorEarlyStop()
{
    /* Check if query already been stopped. */
    if (u_sess->exec_cxt.executor_stop_flag == true)
        return true;
    else
        return false;
}

void BuildStreamFlow(PlannedStmt* plan)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

StreamState* ExecInitStream(Stream* node, EState* estate, int eflags)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

TupleTableSlot* ExecStream(StreamState* node)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

const char* GetStreamType(Stream* node)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

void ExecEarlyDeinitConsumer(PlanState* node)
{
    ereport(DEBUG3, (errmsg("[%s()]: shouldn't run here", __FUNCTION__)));
    return;
}

void ExecReSetStream(StreamState* node)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

bool ScanStreamByLibcomm(StreamState* node)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
}

int HandleStreamResponse(PGXCNodeHandle* conn, StreamState* node)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return 0;
}

void SetupStreamRuntime(StreamState* node)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

bool ScanMemoryStream(StreamState* node)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
}

void ExecEndStream(StreamState* node)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamPrepareRequest(StreamState* node)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void HandleStreamError(StreamState* node, char* msg_body, int len)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void HandleStreamNotice(StreamState* node, char* msg_body, size_t len)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void CheckMessages(uint64 check_query_id, uint32 check_plan_node_id, char* msg, int msg_len, bool is_stream)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamReportError(StreamState* node)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

bool IsThreadProcessStreamRecursive()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
}

bool ThreadIsDummy(Plan* plan_tree)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
}

void InitStreamContext()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void AssembleDataRow(StreamState* node)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}
