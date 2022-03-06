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
 * ---------------------------------------------------------------------------------------
 *
 * tcap.h
 *        Interfaces for Timecapsule `Version/Recyclebin-based query, restore`
 *
 * IDENTIFICATION
 *        src/include/storage/tcap.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef TCAP_H
#define TCAP_H

#include "postgres.h"

#include "catalog/dependency.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "utils/snapshot.h"
#include "postmaster/rbcleaner.h"

static inline bool TcapFeatureAvail()
{
    return t_thrd.proc->workingVersionNum >= INPLACE_UPDATE_VERSION_NUM;
}

static inline void TcapFeatureEnsure()
{
    if (!TcapFeatureAvail()) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Un-support feature"),
                    errdetail("Only support timecapsule from version %u", 
                        INPLACE_UPDATE_VERSION_NUM)));
    }
}

/*
 * Interfaces for Timecapsule `Version-based query, restore`
 */
extern void TvCheckVersionScan(RangeTblEntry *rte);
extern bool TvIsVersionScan(const ScanState *ss);
extern bool TvIsVersionPlan(const PlannedStmt *stmt);

extern Node *TvTransformVersionExpr(ParseState *pstate, TvVersionType tvtype, Node *tvver);
extern Snapshot TvChooseScanSnap(Relation relation, Scan *scan, ScanState *ss);

extern void TvDeleteDelta(Oid relid, Snapshot snap);
extern void TvInsertLost(Oid relid, Snapshot snap);
extern void TvUheapDeleteDelta(Oid relid, Snapshot snap);
extern void TvUheapInsertLost(Oid relid, Snapshot snap);

extern void TvRestoreVersion(TimeCapsuleStmt *stmt);
extern TransactionId TvFetchSnpxminRecycle(TimestampTz tz);

/*
 * Interfaces for Timecapsule `Recyclebin-based query, restore`
 */

typedef enum TrObjType {
    RB_OBJ_TABLE = 0,
    RB_OBJ_INDEX = 1,
    RB_OBJ_TOAST = 2,
    RB_OBJ_TOAST_INDEX = 3,
    RB_OBJ_SEQUENCE = 4,
    RB_OBJ_PARTITION = 5,
    RB_OBJ_GLOBAL_INDEX = 6,
    RB_OBJ_MATVIEW = 7
} TrObjType;

extern bool TrCheckRecyclebinDrop(const DropStmt *stmt, ObjectAddresses *objects);
extern void TrDrop(const DropStmt* drop, const ObjectAddresses *objects, DropBehavior behavior);

extern bool TrCheckRecyclebinTruncate(const TruncateStmt *stmt);
extern void TrTruncate(const TruncateStmt *stmt);
extern void TrRelationSetNewRelfilenode(Relation relation, TransactionId freezeXid, void *baseDesc);
extern void TrPartitionSetNewRelfilenode(Relation parent, Partition part, TransactionId freezeXid, void *baseDesc);

extern void TrPurgeObject(RangeVar *purobj, TrObjType type);
extern void TrPurgeTablespaceDML(int64 id);
extern void TrPurgeTablespace(int64 id);
extern void TrPurgeRecyclebin(int64 id);
extern void TrPurgeUser(int64 id);
extern void TrPurgeSchema(int64 id);
extern void TrPurgeAuto(int64 id);

extern void TrRestoreDrop(const TimeCapsuleStmt *stmt);
extern void TrRestoreTruncate(const TimeCapsuleStmt *stmt);

extern void TrAdjustFrozenXid64(Oid dbid, TransactionId *frozenXID);
extern bool TrIsRefRbObjectEx(Oid classid, Oid objid, const char *objname = NULL);
extern void TrForbidAccessRbDependencies(Relation depRel, const ObjectAddress *depender, 
    const ObjectAddress *referenced, int nreferenced);
extern void TrForbidAccessRbObject(Oid classid, Oid objid, const char *objname = NULL);

extern bool TrRbIsEmptyDb(Oid dbId);
extern bool TrRbIsEmptySpc(Oid spcId);
extern bool TrRbIsEmptySchema(Oid nspId);
extern bool TrRbIsEmptyUser(Oid roleId);
extern List *TrGetDbListRcy(void);
extern List *TrGetDbListAuto(void);
extern List *TrGetDbListSpc(Oid spcId);
extern List *TrGetDbListSchema(Oid schema);
extern List *TrGetDbListUser(Oid user);

#endif  // define
