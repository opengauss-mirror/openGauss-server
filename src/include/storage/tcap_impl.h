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

#include "postgres.h"

#include "catalog/dependency.h"
#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "utils/snapshot.h"
#include "postmaster/rbcleaner.h"

#include "storage/tcap.h"

#define TR_IS_BASE_OBJ(baseDesc, object) \
        ((object)->classId == RelationRelationId && \
         (object)->objectId == (baseDesc)->relid && \
         (object)->objectSubId == 0)

#define TR_IS_BASE_OBJ_EX(baseDesc, _relid) \
        ((_relid) == (baseDesc)->relid)

typedef enum TrObjOperType {
    RB_OPER_TRUNCATE = 0,
    RB_OPER_DROP = 1,
} TrObjOperType; 

typedef struct TrObjDesc {
    Oid id;
    Oid baseid;
    Oid dbid;
    Oid relid;
    char name[NAMEDATALEN];
    char originname[2 * NAMEDATALEN];
    TrObjOperType operation;
    TrObjType type;
    int64 recyclecsn;
    TimestampTz recycletime;
    int64 createcsn;
    int64 changecsn;
    Oid nspace;
    Oid owner;
    Oid tablespace;
    Oid relfilenode;
    bool canrestore;
    bool canpurge;
    ShortTransactionId frozenxid;
    TransactionId frozenxid64;
    Oid authid;
} TrObjDesc;

typedef enum TrOperMode {
    RB_OPER_PURGE = 0,
    RB_OPER_RESTORE_DROP = 1,
    RB_OPER_RESTORE_TRUNCATE = 2,
} TrOperMode;

extern char *TrGenObjName(char *rbname, Oid classId, Oid objid);
extern void TrBaseRelMatched(TrObjDesc *baseDesc);
extern void TrDoPurgeObjectTruncate(TrObjDesc *desc);
extern void TrDoPurgeObjectDrop(TrObjDesc *desc);
extern TrObjType TrGetObjType(Oid nspId, char relKind);
extern bool NeedTrComm(Oid relid);
extern void TrUpdateBaseid(const TrObjDesc *desc);
extern Oid TrDescWrite(TrObjDesc *desc);
extern void TrDescInit(Relation rel, TrObjDesc *desc, TrObjOperType operType, 
    TrObjType objType, bool canpurge, bool isBaseObj = false);
extern void TrPartDescInit(Relation rel, Partition part, TrObjDesc *desc, TrObjOperType operType,
                        TrObjType objType, bool canpurge, bool isBaseObj = false);
extern void TrFindAllRefObjs(Relation depRel, const ObjectAddress *subobj,
    ObjectAddresses *refobjs, bool ignoreObjSubId = false);
extern void TrOperFetch(const RangeVar *purobj, TrObjType objtype, TrObjDesc *desc, TrOperMode operMode);
extern void TrOperPrep(TrObjDesc *desc, TrOperMode operMode);
extern void TrSwapRelfilenode(Relation rbRel, HeapTuple rbTup, bool isPart);
extern bool TrFetchName(const char *rcyname, TrObjType type, TrObjDesc *desc, TrOperMode operMode);
