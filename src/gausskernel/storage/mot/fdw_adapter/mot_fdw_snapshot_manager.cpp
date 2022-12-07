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
 * mot_fdw_snapshot_manager.cpp
 *    MOT Foreign Data Wrapper snapshot interface.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/fdw_adapter/mot_fdw_snapshot_manager.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "global.h"
#include "postgres.h"
#include "access/transam.h"
#include "storage/procarray.h"
#include "mot_fdw_snapshot_manager.h"
#include "knl/knl_session.h"
#include "knl/knl_thread.h"
#include "knl/knl_instance.h"
#include "utils/snapshot.h"
#include "utils/atomic.h"

// Need to connect with envelope
uint64_t SnapshotManager::GetNextCSN()
{
    return GetCommitCsn();
}

uint64_t SnapshotManager::GetCurrentCSN()
{
    return u_sess->utils_cxt.CurrentSnapshot->snapshotcsn;
}

uint64_t SnapshotManager::GetGcEpoch()
{
    return pg_atomic_read_u64(&g_instance.mot_cxt.shmemVariableCache->nextCommitSeqNo);
}

void SnapshotManager::SetCSN(uint64_t value)
{
    // No-op for the external snapshot manager. Envelope manages the CSN.
}