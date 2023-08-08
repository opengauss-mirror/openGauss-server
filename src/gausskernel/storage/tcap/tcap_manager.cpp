/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
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
 * tcap_manager.cpp
 *      Routines to support Timecapsule `Recyclebin-based query, restore`.
 *      We use Tr prefix to indicate it in following coding.
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/tcap/tcap_manager.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"

#include "pgstat.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "access/xlog.h"
#include "catalog/pg_database.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_collation_fn.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_conversion_fn.h"
#include "catalog/pg_conversion.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_extension_data_source.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_job.h"
#include "catalog/pg_language.h"
#include "catalog/pg_largeobject.h"
#include "catalog/pg_object.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_recyclebin.h"
#include "catalog/pg_rewrite.h"
#include "catalog/pg_rlspolicy.h"
#include "catalog/pg_synonym.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_ts_config.h"
#include "catalog/pg_ts_dict.h"
#include "catalog/pg_ts_parser.h"
#include "catalog/pg_ts_template.h"
#include "catalog/pgxc_class.h"
#include "catalog/pg_partition.h"
#include "catalog/storage.h"
#include "commands/comment.h"
#include "commands/dbcommands.h"
#include "commands/directory.h"
#include "commands/extension.h"
#include "commands/proclang.h"
#include "commands/schemacmds.h"
#include "commands/seclabel.h"
#include "commands/sec_rls_cmds.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "commands/trigger.h"
#include "commands/typecmds.h"
#include "executor/node/nodeModifyTable.h"
#include "rewrite/rewriteRemove.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/smgr/relfilenode.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#include "storage/tcap.h"
#include "storage/tcap_impl.h"

static bool TrIsRefRbObject(const ObjectAddress *obj, Relation depRel = NULL);
void TrDoPurgeObjectDrop(TrObjDesc *desc);

char *TrGenObjName(char *rbname, Oid classId, Oid objid)
{
    int rc = EOK;

    rc = snprintf_s(rbname, NAMEDATALEN, NAMEDATALEN - 1, "BIN$%X%X%X$%llX==$0",
        u_sess->proc_cxt.MyDatabaseId, classId, objid, (uint64)GetXLogInsertRecPtr());
    securec_check_ss_c(rc, "\0", "\0");

    return rbname;
}

static TransactionId TrRbGetRcyfrozenxid64(HeapTuple rbtup, Relation rbRel = NULL)
{
    Datum datum;
    bool isNull = false;
    TransactionId rcyfrozenxid64;
    bool relArgNull = rbRel == NULL;

    if (relArgNull) {
        rbRel = heap_open(RecyclebinRelationId, AccessShareLock);
    }

    datum = heap_getattr(rbtup, Anum_pg_recyclebin_rcyfrozenxid64, RelationGetDescr(rbRel), &isNull);
    Assert(!isNull);

    rcyfrozenxid64 = DatumGetTransactionId(datum);

    if (relArgNull) {
        heap_close(rbRel, AccessShareLock);
    }

    return rcyfrozenxid64;
}

void TrDescInit(Relation rel, TrObjDesc *desc, TrObjOperType operType,
    TrObjType objType, bool canpurge, bool isBaseObj)
{
    errno_t rc = EOK;

    /* Notice: desc->id, desc->baseid will be assigned by invoker later. */
    desc->dbid = u_sess->proc_cxt.MyDatabaseId;
    desc->relid = RelationGetRelid(rel);

    (void)TrGenObjName(desc->name, RelationRelationId, desc->relid);

    rc = strncpy_s(desc->originname, NAMEDATALEN, RelationGetRelationName(rel),
        strlen(RelationGetRelationName(rel)));
    securec_check(rc, "\0", "\0");

    desc->operation = operType;
    desc->type = objType;
    desc->recyclecsn = t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo;
    desc->recycletime = GetCurrentTimestamp();
    desc->createcsn = RelationGetCreatecsn(rel);
    desc->changecsn = RelationGetChangecsn(rel);
    desc->nspace = RelationGetNamespace(rel);
    desc->owner = RelationGetOwner(rel);
    desc->tablespace = RelationGetTablespace(rel);
    desc->relfilenode = RelationGetRelFileNode(rel);
    desc->frozenxid = RelationGetRelFrozenxid(rel);
    desc->frozenxid64 = RelationGetRelFrozenxid64(rel);
    desc->canrestore = objType == RB_OBJ_TABLE;
    desc->canpurge = canpurge;
}

void TrPartDescInit(Relation rel, Partition part, TrObjDesc *desc, TrObjOperType operType,
    TrObjType objType, bool canpurge, bool isBaseObj)
{
    errno_t rc = EOK;

    /* Notice: desc->id, desc->baseid will be assigned by invoker later. */
    desc->dbid = u_sess->proc_cxt.MyDatabaseId;
    desc->relid = part->pd_id;

    (void)TrGenObjName(desc->name, PartitionRelationId, desc->relid);

    rc = strncpy_s(desc->originname, NAMEDATALEN, RelationGetRelationName(rel),
        strlen(RelationGetRelationName(rel)));
    securec_check(rc, "\0", "\0");
    
    int len = strlen(PartitionGetPartitionName(part)) + strlen(RelationGetRelationName(rel)) + 1;
    rc = strcat_s(desc->originname, len, PartitionGetPartitionName(part));
    securec_check(rc, "\0", "\0");

    desc->operation = operType;
    desc->type = objType;
    desc->recyclecsn = t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo;
    desc->recycletime = GetCurrentTimestamp();
    desc->createcsn = RelationGetCreatecsn(rel);
    desc->changecsn = RelationGetChangecsn(rel);
    desc->nspace = RelationGetNamespace(rel);
    desc->owner = RelationGetOwner(rel);
    desc->tablespace = part->pd_part->reltablespace;
    desc->relfilenode = part->pd_part->relfilenode;
    desc->frozenxid = part->pd_part->relfrozenxid;
    desc->frozenxid64 = PartGetRelFrozenxid64(part);
    desc->canrestore = false;
    desc->canpurge = canpurge;
}

static void TrDescRead(TrObjDesc *desc, HeapTuple rbtup)
{
    Form_pg_recyclebin rbForm = (Form_pg_recyclebin)GETSTRUCT(rbtup);

    desc->id = HeapTupleGetOid(rbtup);
    desc->baseid = rbForm->rcybaseid;

    desc->dbid = rbForm->rcydbid;
    desc->relid = rbForm->rcyrelid;
    (void)namestrcpy((Name)desc->name, NameStr(rbForm->rcyname));
    (void)namestrcpy((Name)desc->originname, NameStr(rbForm->rcyoriginname));
    desc->operation = (rbForm->rcyoperation == 'd') ? RB_OPER_DROP : RB_OPER_TRUNCATE;
    desc->type = (TrObjType)rbForm->rcytype;
    desc->recyclecsn = rbForm->rcyrecyclecsn;
    desc->recycletime = rbForm->rcyrecycletime;
    desc->createcsn = rbForm->rcycreatecsn;
    desc->changecsn = rbForm->rcychangecsn;
    desc->nspace = rbForm->rcynamespace;
    desc->owner = rbForm->rcyowner;
    desc->tablespace = rbForm->rcytablespace;
    desc->relfilenode = rbForm->rcyrelfilenode;
    desc->canrestore = rbForm->rcycanrestore;
    desc->canpurge = rbForm->rcycanpurge;
    desc->frozenxid = rbForm->rcyfrozenxid;
    desc->frozenxid64 = TrRbGetRcyfrozenxid64(rbtup);
}

Oid TrDescWrite(TrObjDesc *desc)
{
    Relation rel;
    HeapTuple tup;
    bool nulls[Natts_pg_recyclebin] = {0};
    Datum values[Natts_pg_recyclebin];
    NameData name;
    NameData originname;
    Oid rbid;

    values[Anum_pg_recyclebin_rcydbid - 1] = ObjectIdGetDatum(desc->dbid);
    values[Anum_pg_recyclebin_rcybaseid - 1] = ObjectIdGetDatum(desc->baseid);
    values[Anum_pg_recyclebin_rcyrelid - 1] = ObjectIdGetDatum(desc->relid);
    (void)namestrcpy(&name, desc->name);
    values[Anum_pg_recyclebin_rcyname - 1] = NameGetDatum(&name);
    (void)namestrcpy(&originname, desc->originname);
    values[Anum_pg_recyclebin_rcyoriginname - 1] = NameGetDatum(&originname);
    values[Anum_pg_recyclebin_rcyoperation - 1] = (desc->operation == RB_OPER_DROP) ? 'd' : 't';
    values[Anum_pg_recyclebin_rcytype - 1] = Int32GetDatum(desc->type);
    values[Anum_pg_recyclebin_rcyrecyclecsn - 1] = Int64GetDatum(desc->recyclecsn);
    values[Anum_pg_recyclebin_rcyrecycletime - 1] = TimestampTzGetDatum(desc->recycletime);
    values[Anum_pg_recyclebin_rcycreatecsn - 1] = Int64GetDatum(desc->createcsn);
    values[Anum_pg_recyclebin_rcychangecsn - 1] = Int64GetDatum(desc->changecsn);
    values[Anum_pg_recyclebin_rcynamespace - 1] = ObjectIdGetDatum(desc->nspace);
    values[Anum_pg_recyclebin_rcyowner - 1] = ObjectIdGetDatum(desc->owner);
    values[Anum_pg_recyclebin_rcytablespace - 1] = ObjectIdGetDatum(desc->tablespace);
    values[Anum_pg_recyclebin_rcyrelfilenode - 1] = ObjectIdGetDatum(desc->relfilenode);
    values[Anum_pg_recyclebin_rcycanrestore - 1] = BoolGetDatum(desc->canrestore);
    values[Anum_pg_recyclebin_rcycanpurge - 1] = BoolGetDatum(desc->canpurge);
    values[Anum_pg_recyclebin_rcyfrozenxid - 1] = ShortTransactionIdGetDatum(desc->frozenxid);
    values[Anum_pg_recyclebin_rcyfrozenxid64 - 1] = TransactionIdGetDatum(desc->frozenxid64);

    rel = heap_open(RecyclebinRelationId, RowExclusiveLock);

    tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);

    rbid = simple_heap_insert(rel, tup);

    CatalogUpdateIndexes(rel, tup);

    heap_freetuple_ext(tup);

    heap_close(rel, RowExclusiveLock);

    CommandCounterIncrement();

    return rbid;
}

static bool TrFetchOrinameImpl(Oid nspId, const char *oriname, TrObjType type,
    TrObjDesc *desc, TrOperMode operMode)
{
    Relation rbRel;
    SysScanDesc sd;
    ScanKeyData skey[3];
    HeapTuple tup;
    bool found = false;

    if (!OidIsValid(nspId)) {
        return false;
    }

    rbRel = heap_open(RecyclebinRelationId, AccessShareLock);

    ScanKeyInit(&skey[0], Anum_pg_recyclebin_rcynamespace, BTEqualStrategyNumber,
        F_OIDEQ, ObjectIdGetDatum(nspId));
    ScanKeyInit(&skey[1], Anum_pg_recyclebin_rcydbid, BTEqualStrategyNumber,
        F_OIDEQ, ObjectIdGetDatum(u_sess->proc_cxt.MyDatabaseId));
    ScanKeyInit(&skey[2], Anum_pg_recyclebin_rcyoriginname, BTEqualStrategyNumber,
        F_NAMEEQ, CStringGetDatum(oriname));

    sd = systable_beginscan(rbRel, RecyclebinDbidNspOrinameIndexId, true, NULL, 3, skey);
    /* restore drop/truncate use the latest version, purge use the oldest version */
    ScanDirection scan_direct = (operMode == RB_OPER_PURGE) ? ForwardScanDirection : BackwardScanDirection;
    while ((tup = (HeapTuple)index_getnext(sd->iscan, scan_direct)) != NULL) {
        Form_pg_recyclebin rbForm = (Form_pg_recyclebin)GETSTRUCT(tup);
        if ((rbForm->rcytype != type && rbForm->rcytype == RB_OBJ_TABLE) ||
            (rbForm->rcytype != type && rbForm->rcytype == RB_OBJ_INDEX) ||
            (operMode == RB_OPER_RESTORE_DROP && rbForm->rcyoperation != 'd') ||
            (operMode == RB_OPER_RESTORE_TRUNCATE && rbForm->rcyoperation != 't')) {
            continue;
        }

        found = true;
        TrDescRead(desc, tup);
        break;
    }

    systable_endscan(sd);
    heap_close(rbRel, AccessShareLock);

    return found;
}

bool TrFetchName(const char *rcyname, TrObjType type, TrObjDesc *desc, TrOperMode operMode)
{
    Relation rbRel;
    SysScanDesc sd;
    ScanKeyData skey[1];
    HeapTuple tup;
    bool found = false;

    rbRel = heap_open(RecyclebinRelationId, AccessShareLock);

    ScanKeyInit(&skey[0], Anum_pg_recyclebin_rcyname, BTEqualStrategyNumber,
        F_NAMEEQ, CStringGetDatum(rcyname));

    sd = systable_beginscan(rbRel, RecyclebinNameIndexId, true, NULL, 1, skey);
    if ((tup = systable_getnext(sd)) != NULL) {
        Form_pg_recyclebin rbForm = (Form_pg_recyclebin)GETSTRUCT(tup);
        if ((rbForm->rcytype != type && rbForm->rcytype == RB_OBJ_TABLE) ||
            (rbForm->rcytype != type && rbForm->rcytype == RB_OBJ_INDEX)) {
            ereport(ERROR,
                (errmsg("The recycle object \"%s\" type mismatched.", rcyname)));
        }
        if ((operMode == RB_OPER_RESTORE_DROP && rbForm->rcyoperation != 'd') ||
            (operMode == RB_OPER_RESTORE_TRUNCATE && rbForm->rcyoperation != 't')) {
            ereport(ERROR,
                (errmsg("recycle object \"%s\" desired does not exist", rcyname)));
        }
        found = true;
        TrDescRead(desc, tup);
    }

    systable_endscan(sd);
    heap_close(rbRel, AccessShareLock);

    return found;
}

static bool TrFetchOriname(const char *schemaname, const char *relname, TrObjType type,
    TrObjDesc *desc, TrOperMode operMode)
{
    bool found = false;
    Oid nspId;

    if (schemaname) {
        nspId = get_namespace_oid(schemaname, true);
        found = TrFetchOrinameImpl(nspId, relname, type, desc, operMode);
    } else {
        List *activeSearchPath = NIL;
        ListCell *l = NULL;

        recomputeNamespacePath();
        activeSearchPath = list_copy(u_sess->catalog_cxt.activeSearchPath);
        foreach (l, activeSearchPath) {
            nspId = lfirst_oid(l);
            if (TrFetchOrinameImpl(nspId, relname, type, desc, operMode)) {
                found = true;
                break;
            }
        }
        list_free_ext(activeSearchPath);
        if (!found) {
            nspId = PG_TOAST_NAMESPACE;
            found = TrFetchOrinameImpl(nspId, relname, type, desc, operMode);
        }
    }

    return found;
}

void TrUpdateBaseid(const TrObjDesc *desc)
{
    Relation rbRel;
    SysScanDesc sd;
    ScanKeyData skey[1];
    HeapTuple tup;
    HeapTuple newtup;
    Datum values[Natts_pg_recyclebin] = { 0 };
    bool nulls[Natts_pg_recyclebin] = { false };
    bool replaces[Natts_pg_recyclebin] = { false };

    rbRel = heap_open(RecyclebinRelationId, RowExclusiveLock);

    ScanKeyInit(&skey[0], ObjectIdAttributeNumber, BTEqualStrategyNumber,
        F_OIDEQ, ObjectIdGetDatum(desc->id));

    sd = systable_beginscan(rbRel, RecyclebinIdIndexId, true, NULL, 1, skey);
    if ((tup = systable_getnext(sd)) == NULL) {
        ereport(ERROR, (errmsg("recycle object %u does not exist", desc->id)));
    }

    replaces[Anum_pg_recyclebin_rcybaseid - 1] = true;
    values[Anum_pg_recyclebin_rcybaseid - 1] = ObjectIdGetDatum(desc->baseid);

    newtup = heap_modify_tuple(tup, RelationGetDescr(rbRel), values, nulls, replaces);

    simple_heap_update(rbRel, &newtup->t_self, newtup);

    CatalogUpdateIndexes(rbRel, newtup);

    heap_freetuple_ext(newtup);

    systable_endscan(sd);
    heap_close(rbRel, RowExclusiveLock);

    return;
}

static void TrLockRelationImpl(Oid relid, TrObjType type)
{
    /*
     * Lock failed may due to concurrently purge/timecapsule/DQL
     * on recycle object, or access on normal relation.
     */
    if (!ConditionalLockRelationOid(relid, AccessExclusiveLock)) {
        ereport(ERROR,
            (errcode(ERRCODE_RBIN_LOCK_NOT_AVAILABLE),
                errmsg("could not obtain lock on relation \"%u\"", relid)));
    }

    /*
     * Now that we have the lock, probe to see if the relation
     * really exists or not.
     */
    AcceptInvalidationMessages();
    if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(relid)) && type != RB_OBJ_PARTITION) {
        /* Clean already held locks if error return. */
        UnlockRelationOid(relid, AccessExclusiveLock);
        ereport(ERROR,
            (errcode(ERRCODE_RBIN_UNDEFINED_OBJECT),
                errmsg("relation \"%u\" does not exist", relid)));
    } else if (!SearchSysCacheExists1(PARTRELID, ObjectIdGetDatum(relid)) && type == RB_OBJ_PARTITION) {
        /* Clean already held locks if error return. */
        UnlockRelationOid(relid, AccessExclusiveLock);
        ereport(ERROR,
            (errcode(ERRCODE_RBIN_UNDEFINED_OBJECT),
                errmsg("partition \"%u\" does not exist", relid)));
    }
}

static void TrLockRelation(TrObjDesc *desc)
{
    Oid heapOid = InvalidOid;

    /* Lock heap relation for index first */
    if (desc->type == RB_OBJ_INDEX) {
        heapOid = IndexGetRelation(desc->relid, true);
        if (!OidIsValid(heapOid)) {
            ereport(ERROR,
                (errcode(ERRCODE_RBIN_UNDEFINED_OBJECT),
                    errmsg("relation \"%u\" does not exist", desc->relid)));
        }
        TrLockRelationImpl(heapOid, desc->type);
    }

    /* Use TRY-CATCH block to clean locks already held if error. */
    PG_TRY();
    {
        /* Lock relation self */
        TrLockRelationImpl(desc->relid, desc->type);
    }
    PG_CATCH();
    {
        if (desc->type == RB_OBJ_INDEX) {
            UnlockRelationOid(heapOid, AccessExclusiveLock);
        }
        PG_RE_THROW();
    }
    PG_END_TRY();
}

static void TrUnlockTrItem(TrObjDesc *desc)
{
    UnlockDatabaseObject(RecyclebinRelationId, desc->id, 0,
        AccessExclusiveLock);
}

static void TrLockTrItem(TrObjDesc *desc)
{
    Relation rbRel;
    SysScanDesc sd;
    ScanKeyData skey[1];
    HeapTuple tup;

    /* 1. Try to lock rb item in AccessExclusiveLock */
    if (!ConditionalLockDatabaseObject(RecyclebinRelationId, desc->id, 0, AccessExclusiveLock)) {
        ereport(ERROR,
            (errcode(ERRCODE_RBIN_LOCK_NOT_AVAILABLE),
                errmsg("could not obtain lock on recycle object '%s'", desc->name)));
    }

    /*
     * 2. Now that we have the lock, probe to see if the rb item really
     *    exists or not.
     */
    AcceptInvalidationMessages();

    rbRel = heap_open(RecyclebinRelationId, AccessShareLock);

    ScanKeyInit(&skey[0], ObjectIdAttributeNumber, BTEqualStrategyNumber,
        F_OIDEQ, ObjectIdGetDatum(desc->id));

    sd = systable_beginscan(rbRel, RecyclebinIdIndexId, true, NULL, 1, skey);
    if ((tup = systable_getnext(sd)) == NULL) {
        UnlockDatabaseObject(RecyclebinRelationId, desc->id, 0, AccessExclusiveLock);
        systable_endscan(sd);
        heap_close(rbRel, AccessShareLock);
        ereport(ERROR,
            (errcode(ERRCODE_RBIN_UNDEFINED_OBJECT),
                errmsg("recycle object \"%s\" does not exist", desc->name)));
    }

    systable_endscan(sd);
    heap_close(rbRel, AccessShareLock);

    return;
}

static void TrOperMatch(const TrObjDesc *desc, TrOperMode operMode)
{
    switch (operMode) {
        case RB_OPER_PURGE:
            if (!desc->canpurge && desc->type != RB_OBJ_PARTITION) {
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_OPERATION),
                        errmsg("recycle object \"%s\" cannot be purged", desc->name)));
            }
            break;

        case RB_OPER_RESTORE_DROP:
            if ((!desc->canrestore && desc->type != RB_OBJ_PARTITION) || desc->operation != RB_OPER_DROP) {
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_OPERATION),
                        errmsg("recycle object \"%s\" cannot be restored", desc->name)));
            }
            break;

        case RB_OPER_RESTORE_TRUNCATE:
            if ((!desc->canrestore && desc->type != RB_OBJ_PARTITION) || desc->operation != RB_OPER_TRUNCATE) {
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_OPERATION),
                        errmsg("recycle object \"%s\" cannot be restored", desc->name)));
            }
            break;

        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized recyclebin operation: %u", operMode)));
            break;
    }
}

/*
 * Fetch object from recycle bin for rb operations - purge, restore :
 *     Prefer to fetch as original name, then recycle name.
 */
void TrOperFetch(const RangeVar *purobj, TrObjType objtype, TrObjDesc *desc, TrOperMode operMode)
{
    bool found = false;

    AcceptInvalidationMessages();

    /* Prefer to fetch as original name */
    found = TrFetchOriname(purobj->schemaname, purobj->relname, objtype, desc, operMode);
    /* if not found, then fetch as recycle name */
    if (!found) {
        found = TrFetchName(purobj->relname, objtype, desc, operMode);
    }

    /* not found, throw error */
    if (!found) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_TABLE),
                errmsg("recycle object \"%s\" desired does not exist", purobj->relname)));
    }

    TrOperMatch(desc, operMode);

    return;
}

static void TrPermRestore(TrObjDesc *desc, TrOperMode operMode)
{
    AclResult aclCreateResult;

    /* Check namespace permissions. */
    aclCreateResult = pg_namespace_aclcheck(desc->nspace, desc->authid, ACL_CREATE);
    if (aclCreateResult != ACLCHECK_OK) {
        aclcheck_error(aclCreateResult, ACL_KIND_NAMESPACE, get_namespace_name(desc->nspace));
    }

    AclResult aclUsageResult  = pg_namespace_aclcheck(desc->nspace, desc->authid, ACL_USAGE);
    if (aclUsageResult != ACLCHECK_OK) {
        aclcheck_error(aclUsageResult, ACL_KIND_NAMESPACE, get_namespace_name(desc->nspace));
    }

    /* Allow restore to either table owner or schema owner */
    if (!pg_class_ownercheck(desc->relid, desc->authid) && !pg_namespace_ownercheck(desc->nspace, desc->authid)) {
        aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_CLASS, desc->name);
        return;
    }

    if (operMode == RB_OPER_RESTORE_TRUNCATE) {
        AclResult aclTruncateResult = pg_class_aclcheck(desc->relid, desc->authid, ACL_TRUNCATE);
        if (aclTruncateResult != ACLCHECK_OK) {
            aclcheck_error(aclTruncateResult, ACL_KIND_CLASS, desc->name);
        }
    }
}

static void TrPermPurge(TrObjDesc *desc, TrOperMode operMode)
{
    AclResult result;

    result = pg_namespace_aclcheck(desc->nspace, desc->authid, ACL_USAGE);
    if (result != ACLCHECK_OK) {
        aclcheck_error(result, ACL_KIND_NAMESPACE, get_namespace_name(desc->nspace));
    }
    if (!pg_class_ownercheck(desc->relid, desc->authid) && !pg_namespace_ownercheck(desc->nspace, desc->authid)) {
        aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_CLASS, desc->name);
    }
}

/*
 * Check permission for rb operations - purge, restore
 */
static void TrPerm(TrObjDesc *desc, TrOperMode operMode)
{
    switch (operMode) {
        case RB_OPER_RESTORE_DROP:
        case RB_OPER_RESTORE_TRUNCATE:
            TrPermRestore(desc, operMode);
            break;
        case RB_OPER_PURGE:
            TrPermPurge(desc, operMode);
            break;
        default:
            /* Never reached here. */
            Assert(0);
            break;
    }
}

/*
 * Prepare for rb operations - purge, restore :
 *     check permission, lock objects
 */
void TrOperPrep(TrObjDesc *desc, TrOperMode operMode)
{
    bool needLockRelation = false;

    /*
     * 1. Check permission.
     */
    TrPerm(desc, operMode);

    /*
     * 2. Acquire lock on rb item, avoid concurrently purge, restore.
     */
    TrLockTrItem(desc);

    /*
     * 3. Acquire lock on relation, avoid concurrently DQL.
     *    Notice: ignore this step when we purge truncated relation
     *            as base relation may not exists.
     */
    needLockRelation = !(operMode == RB_OPER_PURGE && desc->operation == RB_OPER_TRUNCATE);
    if (needLockRelation) {
        /* Use TRY-CATCH block to clean locks already held if error. */
        PG_TRY();
        {
            TrLockRelation(desc);
        }
        PG_CATCH();
        {
            TrUnlockTrItem(desc);
            PG_RE_THROW();
        }
        PG_END_TRY();
    }
}

bool NeedTrComm(Oid relid)
{
    Relation rel;
    Form_pg_class classForm;

    if (/*
         *Disable Recyclebin-based-Drop/Truncate when
         */
        /* recyclebin disabled, or */
        !u_sess->attr.attr_storage.enable_recyclebin ||
        /* target db is template1, or */
        u_sess->proc_cxt.MyDatabaseId == TemplateDbOid ||
        /* in maintenance mode, or */
        u_sess->attr.attr_common.xc_maintenance_mode ||
        /* in in-place upgrade mode, or */
        t_thrd.proc->workingVersionNum < 92350 ||
        /* in non-singlenode mode, or */
        (g_instance.role != VSINGLENODE) ||
        /* in bootstrap mode. */
        IsInitdb) {
        return false;
    }

    rel = relation_open(relid, NoLock);
    classForm = rel->rd_rel;
    if (/*
         * Disable Recyclebin-based-Drop/Truncate if
         */
        /* table is non ordinary table, or */
        classForm->relkind != RELKIND_RELATION ||
        /* is non heap table, or */
        rel->rd_tam_ops == TableAmHeap ||
        /* is non regular table, or */
        classForm->relpersistence != RELPERSISTENCE_PERMANENT ||
        /* is shared table across databases, or */
        classForm->relisshared ||
        /* has derived classes, or */
        classForm->relhassubclass ||
        /* has any PARTIAL CLUSTER KEY, or */
        classForm->relhasclusterkey ||
        /* is cstore table, or */
        (rel->rd_options && StdRelOptIsColStore(rel->rd_options)) || RelationIsColStore(rel) ||
        /* is hbkt table, or */
        (RELATION_HAS_BUCKET(rel) || RELATION_OWN_BUCKET(rel)) ||
        /* is dfs table, or */
        RelationIsPAXFormat(rel) ||
        /* is resizing, or */
        RelationInClusterResizing(rel) ||
        /* is in system namespace. */
        (IsSystemNamespace(classForm->relnamespace) || IsToastNamespace(classForm->relnamespace) ||
        IsCStoreNamespace(classForm->relnamespace))) {
        relation_close(rel, NoLock);
        return false;
    }

    relation_close(rel, NoLock);

    return true;
}

TrObjType TrGetObjType(Oid nspId, char relKind)
{
    TrObjType type = RB_OBJ_TABLE;

    switch (relKind) {
        case RELKIND_INDEX:
            type = IsToastNamespace(nspId) ? RB_OBJ_TOAST_INDEX : RB_OBJ_INDEX;
            break;
        case RELKIND_RELATION:
            type = RB_OBJ_TABLE;
            break;
        case RELKIND_SEQUENCE:
        case RELKIND_LARGE_SEQUENCE:
            type = RB_OBJ_SEQUENCE;
            break;
        case RELKIND_TOASTVALUE:
            type = RB_OBJ_TOAST;
            break;
        case PARTTYPE_PARTITIONED_RELATION:
            type = RB_OBJ_PARTITION;
            break;
        case RELKIND_GLOBAL_INDEX:
            type = RB_OBJ_GLOBAL_INDEX;
            break;
        case RELKIND_MATVIEW:
            type = RB_OBJ_MATVIEW;
            break;
        default:
            /* Never reached here. */
            Assert(0);
            break;
    }

    return type;
}

static bool TrObjAddrExists(Oid classid, Oid objid, ObjectAddresses *objSet)
{
    int i;

    for (i = 0; i < objSet->numrefs; i++) {
        if (TrObjIsEqualEx(classid, objid, &objSet->refs[i])) {
            return true;
        }
    }

    return false;
}

/*
 * output: refobjs
 */
void TrFindAllRefObjs(Relation depRel, const ObjectAddress *subobj,
    ObjectAddresses *refobjs, bool ignoreObjSubId)
{
    SysScanDesc sd;
    HeapTuple tuple;
    ScanKeyData key[3];
    int nkeys;

    ScanKeyInit(&key[0], Anum_pg_depend_classid, BTEqualStrategyNumber, F_OIDEQ,
        ObjectIdGetDatum(subobj->classId));
    ScanKeyInit(&key[1], Anum_pg_depend_objid, BTEqualStrategyNumber, F_OIDEQ,
        ObjectIdGetDatum(subobj->objectId));
    nkeys = 2;
    if (!ignoreObjSubId && subobj->objectSubId != 0) {
        ScanKeyInit(&key[2], Anum_pg_depend_objsubid, BTEqualStrategyNumber, F_INT4EQ,
            Int32GetDatum(subobj->objectSubId));
        nkeys = 3;
    }

    sd = systable_beginscan(depRel, DependDependerIndexId, true, NULL, nkeys, key);
    while (HeapTupleIsValid(tuple = systable_getnext(sd))) {
        Form_pg_depend depForm = (Form_pg_depend)GETSTRUCT(tuple);
        /* Cascaded clean rb object in `DROP SCHEMA` command. */
        if (depForm->refclassid == NamespaceRelationId) {
            continue;
        }

        /* We keep `objSet` unique when `ignoreObjSubId = true` to avoid circle recursive. */
        if (!ignoreObjSubId || !TrObjAddrExists(depForm->refclassid, depForm->refobjid, refobjs)) {
            add_object_address_ext(depForm->refclassid, depForm->refobjid,
                                   depForm->refobjsubid, depForm->deptype, refobjs);
        }
    }

    systable_endscan(sd);
    return;
}

static void TrFindAllInternalObjs(Relation depRel, const ObjectAddress *refobj,
    ObjectAddresses *objSet, bool ignoreObjSubId = false)
{
    SysScanDesc sd;
    HeapTuple tuple;
    ScanKeyData key[3];
    int nkeys;

    ScanKeyInit(&key[0], Anum_pg_depend_refclassid, BTEqualStrategyNumber, F_OIDEQ,
        ObjectIdGetDatum(refobj->classId));
    ScanKeyInit(&key[1], Anum_pg_depend_refobjid, BTEqualStrategyNumber, F_OIDEQ,
        ObjectIdGetDatum(refobj->objectId));
    nkeys = 2;
    if (!ignoreObjSubId && refobj->objectSubId != 0) {
        ScanKeyInit(&key[2], Anum_pg_depend_refobjsubid, BTEqualStrategyNumber, F_INT4EQ,
            Int32GetDatum(refobj->objectSubId));
        nkeys = 3;
    }

    sd = systable_beginscan(depRel, DependReferenceIndexId, true, NULL, nkeys, key);
    while (HeapTupleIsValid(tuple = systable_getnext(sd))) {
        Form_pg_depend depForm = (Form_pg_depend)GETSTRUCT(tuple);
        if (depForm->deptype != 'i') {
            continue;
        }

        /* We keep `objSet` unique when `ignoreObjSubId = true` to avoid circle recursive. */
        if (!ignoreObjSubId || !TrObjAddrExists(depForm->classid, depForm->objid, objSet)) {
            add_object_address_ext(depForm->classid, depForm->objid,
                                   depForm->objsubid, depForm->deptype, objSet);
        }
    }

    systable_endscan(sd);
    return;
}

static void TrDoPurgeObject(TrObjDesc *desc)
{
    if (desc->operation == RB_OPER_DROP) {
        TrDoPurgeObjectDrop(desc);
    } else {
        TrDoPurgeObjectTruncate(desc);
    }
}

void TrPurgeObject(RangeVar *purobj, TrObjType type)
{
    TrObjDesc desc;

    TrOperFetch(purobj, type, &desc, RB_OPER_PURGE);

    desc.authid = GetUserId();
    TrOperPrep(&desc, RB_OPER_PURGE);

    TrDoPurgeObject(&desc);

    return;
}

const int PURGE_BATCH = 64;
const int PURGE_SINGL = 64;
typedef void (*TrFetchBeginHook)(SysScanDesc *sd, Oid objId);
typedef bool (*TrFetchMatchHook)(Relation rbRel, HeapTuple rbTup, Oid objId);

static void TrFetchBegin(TrFetchBeginHook fetchHook, SysScanDesc *sd, Oid objId)
{
    fetchHook(sd, objId);
}

// @return: true for eof
static bool TrFetchExec(TrFetchMatchHook matchHook, Oid objId, SysScanDesc sd, TrObjDesc *desc)
{
    HeapTuple tup;
    while ((tup = systable_getnext(sd)) != NULL) {
        Form_pg_recyclebin rbForm = (Form_pg_recyclebin)GETSTRUCT(tup);
        if ((rbForm->rcytype == RB_OBJ_TABLE) && matchHook(sd->heap_rel, tup, objId)) {
            Assert (rbForm->rcycanpurge);
            TrDescRead(desc, tup);
            return false;
        } else if ((rbForm->rcytype == RB_OBJ_PARTITION) && matchHook(sd->heap_rel, tup, objId)) {
            Assert (!rbForm->rcycanpurge);
            TrDescRead(desc, tup);
            return false;
        }
    }
    return true;
}

static void TrFetchEnd(SysScanDesc sd)
{
    Relation rbRel = sd->heap_rel;

    systable_endscan(sd);
    heap_close(rbRel, AccessShareLock);
}

static bool TrPurgeBatch(TrFetchBeginHook beginHook, TrFetchMatchHook matchHook,
    Oid objId, Oid roleid, uint32 maxBatch, PurgeMsgRes *localRes)
{
    SysScanDesc sd = NULL;
    TrObjDesc desc;
    uint32 count = 0;
    bool eof = false;

    RbMsgResetRes(localRes);

    StartTransactionCommand();

    TrFetchBegin(beginHook, &sd, objId);
    while (!(eof = TrFetchExec(matchHook, objId, sd, &desc))) {
        CHECK_FOR_INTERRUPTS();

        PG_TRY();
        {
            desc.authid = roleid;
            TrOperPrep(&desc, RB_OPER_PURGE);

            TrDoPurgeObject(&desc);
            localRes->purgedNum++;
        }
        PG_CATCH();
        {
            int errcode = geterrcode();
            if (errcode == ERRCODE_RBIN_LOCK_NOT_AVAILABLE) {
                errno_t rc;
                rc = strncpy_s(localRes->errMsg, RB_MAX_ERRMSG_SIZE, Geterrmsg(), RB_MAX_ERRMSG_SIZE - 1);
                securec_check(rc, "\0", "\0");
                localRes->skippedNum++;
            } else if (errcode == ERRCODE_RBIN_UNDEFINED_OBJECT) {
                localRes->undefinedNum++;
            } else {
                PG_RE_THROW();
            }
            FlushErrorState();
        }
        PG_END_TRY();

        if (++count >= maxBatch) {
            break;
        }
    }

    TrFetchEnd(sd);

    CommitTransactionCommand();

    return eof;
}

static void TrFetchBeginSpace(SysScanDesc *sd, Oid spcId)
{
    ScanKeyData skey[2];
    Relation rbRel;

    rbRel = heap_open(RecyclebinRelationId, AccessShareLock);

    ScanKeyInit(&skey[0], Anum_pg_recyclebin_rcytablespace, BTEqualStrategyNumber,
        F_OIDEQ, ObjectIdGetDatum(spcId));
    ScanKeyInit(&skey[1], Anum_pg_recyclebin_rcydbid, BTEqualStrategyNumber,
        F_OIDEQ, ObjectIdGetDatum(u_sess->proc_cxt.MyDatabaseId));

    *sd = systable_beginscan(rbRel, RecyclebinDbidSpcidRcycsnIndexId, true, NULL, 2, skey);
}

static bool TrFetchMatchSpace(Relation rbRel, HeapTuple rbTup, Oid objId)
{
    Form_pg_recyclebin rbForm = (Form_pg_recyclebin)GETSTRUCT(rbTup);
    return rbForm->rcytablespace == objId;
}

void TrPurgeTablespace(int64 id)
{
    PurgeMsgReq *req = &RbMsg(id)->req;
    PurgeMsgRes localRes;
    bool eof = false;

    do {
        eof = TrPurgeBatch(TrFetchBeginSpace, TrFetchMatchSpace, req->objId, req->authId, PURGE_BATCH, &localRes);
        RbMsgSetStatistics(id, &localRes);
    } while (!eof && localRes.skippedNum == 0);
}

void TrPurgeTablespaceDML(int64 id)
{
    PurgeMsgReq *req = &RbMsg(id)->req;
    PurgeMsgRes localRes;
    bool eof = false;

    do {
        eof = TrPurgeBatch(TrFetchBeginSpace, TrFetchMatchSpace, req->objId, req->authId, PURGE_SINGL, &localRes);
        RbMsgSetStatistics(id, &localRes);
    } while (!eof && localRes.purgedNum == 0);
}

static void TrFetchBeginRecyclebin(SysScanDesc *sd, Oid objId)
{
    ScanKeyData skey[2];
    Relation rbRel;

    rbRel = heap_open(RecyclebinRelationId, AccessShareLock);

    ScanKeyInit(&skey[0], Anum_pg_recyclebin_rcydbid, BTEqualStrategyNumber,
        F_OIDEQ, ObjectIdGetDatum(u_sess->proc_cxt.MyDatabaseId));

    *sd = systable_beginscan(rbRel, RecyclebinDbidRelidIndexId, true, NULL, 1, skey);
}

static bool TrFetchMatchRecyclebin(Relation rbRel, HeapTuple rbTup, Oid objId)
{
    return true;
}

void TrPurgeRecyclebin(int64 id)
{
    PurgeMsgReq *req = &RbMsg(id)->req;
    PurgeMsgRes localRes;
    bool eof = false;

    do {
        eof = TrPurgeBatch(TrFetchBeginRecyclebin, TrFetchMatchRecyclebin, 
            InvalidOid, req->authId, PURGE_BATCH, &localRes);
        RbMsgSetStatistics(id, &localRes);
    } while (!eof && localRes.skippedNum == 0);
}

static void TrFetchBeginSchema(SysScanDesc *sd, Oid objId)
{
    ScanKeyData skey[2];
    Relation rbRel;

    rbRel = heap_open(RecyclebinRelationId, AccessShareLock);
    ScanKeyInit(&skey[0], Anum_pg_recyclebin_rcynamespace, BTEqualStrategyNumber,
        F_OIDEQ, ObjectIdGetDatum(objId));
    ScanKeyInit(&skey[1], Anum_pg_recyclebin_rcydbid, BTEqualStrategyNumber,
        F_OIDEQ, ObjectIdGetDatum(u_sess->proc_cxt.MyDatabaseId));

    *sd = systable_beginscan(rbRel, RecyclebinDbidNspOrinameIndexId, true, NULL, 2, skey);
}

static bool TrFetchMatchSchema(Relation rbRel, HeapTuple rbTup, Oid objId)
{
    Form_pg_recyclebin rbForm = (Form_pg_recyclebin)GETSTRUCT(rbTup);
    return rbForm->rcynamespace == objId;
}

void TrPurgeSchema(int64 id)
{
    PurgeMsgReq *req = &RbMsg(id)->req;
    PurgeMsgRes localRes;
    bool eof = false;

    do {
        eof = TrPurgeBatch(TrFetchBeginSchema, TrFetchMatchSchema, req->objId, req->authId, PURGE_BATCH, &localRes);
        RbMsgSetStatistics(id, &localRes);
    } while (!eof && localRes.skippedNum == 0);
}

static void TrFetchBeginUser(SysScanDesc *sd, Oid objId)
{
    ScanKeyData skey[2];
    Relation rbRel;

    rbRel = heap_open(RecyclebinRelationId, AccessShareLock);

    ScanKeyInit(&skey[0], Anum_pg_recyclebin_rcydbid, BTEqualStrategyNumber,
        F_OIDEQ, ObjectIdGetDatum(u_sess->proc_cxt.MyDatabaseId));

    *sd = systable_beginscan(rbRel, RecyclebinDbidRelidIndexId, true, NULL, 1, skey);
}

static bool TrFetchMatchUser(Relation rbRel, HeapTuple rbTup, Oid objId)
{
    Form_pg_recyclebin rbForm = (Form_pg_recyclebin)GETSTRUCT(rbTup);
    return rbForm->rcyowner == objId;
}

void TrPurgeUser(int64 id)
{
    PurgeMsgReq *req = &RbMsg(id)->req;
    PurgeMsgRes localRes;
    bool eof = false;

    do {
        eof = TrPurgeBatch(TrFetchBeginUser, TrFetchMatchUser, req->objId, req->authId, PURGE_BATCH, &localRes);
        RbMsgSetStatistics(id, &localRes);
    } while (!eof && localRes.skippedNum == 0);
}

static void TrFetchBeginAuto(SysScanDesc *sd, Oid objId)
{
    ScanKeyData skey[2];
    Relation rbRel;

    rbRel = heap_open(RecyclebinRelationId, AccessShareLock);

    ScanKeyInit(&skey[0], Anum_pg_recyclebin_rcydbid, BTEqualStrategyNumber,
        F_OIDEQ, ObjectIdGetDatum(u_sess->proc_cxt.MyDatabaseId));

    *sd = systable_beginscan(rbRel, RecyclebinDbidRelidIndexId, true, NULL, 1, skey);
}

static bool TrFetchMatchAuto(Relation rbRel, HeapTuple rbTup, Oid objId)
{
    bool isNull = false;
    Datum datumRcyTime = heap_getattr(rbTup, Anum_pg_recyclebin_rcyrecycletime, 
        RelationGetDescr(rbRel), &isNull);

    long secs;
    int msecs;
    TimestampDifference(isNull ? 0 : DatumGetTimestampTz(datumRcyTime), 
        GetCurrentTimestamp(), &secs, &msecs);

    return secs > u_sess->attr.attr_storage.recyclebin_retention_time || secs < 0;
}

void TrPurgeAuto(int64 id)
{
    PurgeMsgReq *req = &RbMsg(id)->req;
    PurgeMsgRes localRes;
    bool eof = false;
    do {
        eof = TrPurgeBatch(TrFetchBeginAuto, TrFetchMatchAuto, InvalidOid, req->authId, PURGE_BATCH, &localRes);
        RbMsgSetStatistics(id, &localRes);
    } while (!eof);
}

void TrSwapRelfilenode(Relation rbRel, HeapTuple rbTup, bool isPart)
{
    Relation relRel;
    HeapTuple relTup;
    HeapTuple newTup;
    TrObjDesc desc;
    int maxNattr = 0;
    Datum *values = NULL;
    bool *nulls = NULL;
    bool *replaces = NULL;
    NameData name;
    errno_t rc = EOK;
    bool isNull = false;
    int relfilenoIndex = 0;
    int frozenxidIndex = 0;
    int frozenxid64Index = 0;
    bool isPartition = false;

    TrDescRead(&desc, rbTup);

    if (desc.type == RB_OBJ_PARTITION  || (desc.type == RB_OBJ_INDEX && isPart)) {
        isPartition = true;
    }
    if (isPartition) {
        maxNattr = Max(Natts_pg_partition, Natts_pg_recyclebin);
        relRel = heap_open(PartitionRelationId, RowExclusiveLock);
        relTup = SearchSysCacheCopy1(PARTRELID, ObjectIdGetDatum(desc.relid));
        relfilenoIndex = Anum_pg_partition_relfilenode;
        frozenxidIndex = Anum_pg_partition_relfrozenxid;
        frozenxid64Index = Anum_pg_partition_relfrozenxid64;
    } else {
        maxNattr = Max(Natts_pg_class, Natts_pg_recyclebin);
        relRel = heap_open(RelationRelationId, RowExclusiveLock);
        relTup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(desc.relid));
        relfilenoIndex = Anum_pg_class_relfilenode;
        frozenxidIndex = Anum_pg_class_relfrozenxid;
        frozenxid64Index = Anum_pg_class_relfrozenxid64;
    }

    /* 1. Update pg_class or pg_partition */
    if (!HeapTupleIsValid(relTup)) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                 errmsg("cache lookup failed for relation %u", desc.relid)));
    }

    values = (Datum *)palloc0(sizeof(Datum) * maxNattr);
    nulls = (bool *)palloc0(sizeof(bool) * maxNattr);
    replaces = (bool *)palloc0(sizeof(bool) * maxNattr);

    replaces[relfilenoIndex - 1] = true;
    values[relfilenoIndex - 1] = ObjectIdGetDatum(desc.relfilenode);

    replaces[frozenxidIndex - 1] = true;
    values[frozenxidIndex - 1] = ShortTransactionIdGetDatum(desc.frozenxid);

    replaces[frozenxid64Index - 1] = true;
    values[frozenxid64Index - 1] = TransactionIdGetDatum(desc.frozenxid64);

    newTup = heap_modify_tuple(relTup, RelationGetDescr(relRel), values, nulls, replaces);

    simple_heap_update(relRel, &newTup->t_self, newTup);

    CatalogUpdateIndexes(relRel, newTup);

    heap_freetuple_ext(newTup);

    /* 2. Update pg_recyclebin */
    rc = memset_s(values, sizeof(Datum) * maxNattr, 0, sizeof(Datum) * maxNattr);
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(bool) * maxNattr, false, sizeof(bool) * maxNattr);
    securec_check(rc, "\0", "\0");
    rc = memset_s(replaces, sizeof(bool) * maxNattr, false, sizeof(bool) * maxNattr);
    securec_check(rc, "\0", "\0");

    (void)TrGenObjName(NameStr(name), RelationRelationId, desc.relid);
    replaces[Anum_pg_recyclebin_rcyname - 1] = true;
    values[Anum_pg_recyclebin_rcyname - 1] = NameGetDatum(&name);

    replaces[Anum_pg_recyclebin_rcyoriginname - 1] = true;
    if (isPartition) {
        values[Anum_pg_recyclebin_rcyoriginname - 1] = NameGetDatum(&desc.originname);
    } else {
        values[Anum_pg_recyclebin_rcyoriginname - 1] = NameGetDatum(&((Form_pg_class)GETSTRUCT(relTup))->relname);
    }

    replaces[Anum_pg_recyclebin_rcyrecyclecsn - 1] = true;
    values[Anum_pg_recyclebin_rcyrecyclecsn - 1] = Int64GetDatum(t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo);

    replaces[Anum_pg_recyclebin_rcyrecycletime - 1] = true;
    values[Anum_pg_recyclebin_rcyrecycletime - 1] = TimestampTzGetDatum(GetCurrentTimestamp());

    replaces[Anum_pg_recyclebin_rcyrelfilenode - 1] = true;
    if (isPartition) {
        values[Anum_pg_recyclebin_rcyrelfilenode - 1] =
            ObjectIdGetDatum(((Form_pg_partition)GETSTRUCT(relTup))->relfilenode);
    } else {
        values[Anum_pg_recyclebin_rcyrelfilenode - 1] =
            ObjectIdGetDatum(((Form_pg_class)GETSTRUCT(relTup))->relfilenode);
    }

    replaces[Anum_pg_recyclebin_rcyfrozenxid - 1] = true;
    if (isPartition) {
        values[Anum_pg_recyclebin_rcyfrozenxid - 1] =
            ShortTransactionIdGetDatum(((Form_pg_partition)GETSTRUCT(relTup))->relfrozenxid);
    } else {
        values[Anum_pg_recyclebin_rcyfrozenxid - 1] =
            ShortTransactionIdGetDatum(((Form_pg_class)GETSTRUCT(relTup))->relfrozenxid);
    }

    replaces[Anum_pg_recyclebin_rcyfrozenxid64 - 1] = true;
    Datum xid64datum = heap_getattr(relTup, frozenxid64Index, RelationGetDescr(relRel), &isNull);
    values[Anum_pg_recyclebin_rcyfrozenxid64 - 1] = DatumGetTransactionId(xid64datum);

    newTup = heap_modify_tuple(rbTup, RelationGetDescr(rbRel), values, nulls, replaces);

    simple_heap_update(rbRel, &newTup->t_self, newTup);

    CatalogUpdateIndexes(rbRel, newTup);

    heap_freetuple_ext(newTup);

    pfree(values);
    pfree(nulls);
    pfree(replaces);

    heap_freetuple_ext(relTup);
    heap_close(relRel, RowExclusiveLock);
    return;
}

void TrBaseRelMatched(TrObjDesc *baseDesc)
{
    ObjectAddress obj = {RelationRelationId, baseDesc->relid};
    if (TrIsRefRbObject(&obj)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("relation \"%s\" does not exist", baseDesc->originname)));
    }

    Relation rel = RelationIdGetRelation(baseDesc->relid);
    Assert(RelationIsValid(rel));
    if (RelationGetCreatecsn(rel) != (CommitSeqNo)baseDesc->createcsn) {
        ereport(ERROR,
            (errmsg("The recycle object \"%s\" and relation \"%s\" mismatched.",
                baseDesc->name, RelationGetRelationName(rel))));
    }

    if (RelationGetChangecsn(rel) > (CommitSeqNo)baseDesc->changecsn) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), 
            errmsg("The table definition of \"%s\" has been changed.",
                RelationGetRelationName(rel))));
    }

    RelationClose(rel);
}

void TrAdjustFrozenXid64(Oid dbid, TransactionId *frozenXID)
{
    Relation rbRel;
    SysScanDesc sd;
    HeapTuple rbtup;

    if (!TcapFeatureAvail()) {
        return;
    }

    rbRel = heap_open(RecyclebinRelationId, AccessShareLock);

    sd = systable_beginscan(rbRel, InvalidOid, false, NULL, 0, NULL);
    while ((rbtup = systable_getnext(sd)) != NULL) {
        Form_pg_recyclebin rbForm = (Form_pg_recyclebin)GETSTRUCT(rbtup);
        TransactionId rcyfrozenxid64;

        if (rbForm->rcydbid != dbid || (rbForm->rcytype != RB_OBJ_TABLE && rbForm->rcytype != RB_OBJ_TOAST)) {
            continue;
        }

        rcyfrozenxid64 = TrRbGetRcyfrozenxid64(rbtup, rbRel);
        Assert(TransactionIdIsNormal(rcyfrozenxid64));

        if (TransactionIdPrecedes(rcyfrozenxid64, *frozenXID)) {
            *frozenXID = rcyfrozenxid64;
        }
    }

    systable_endscan(sd);
    heap_close(rbRel, AccessShareLock);

    return;
}

bool TrRbIsEmptyDb(Oid dbid)
{
    Relation rbRel;
    SysScanDesc sd;
    HeapTuple tup;
    ScanKeyData skey[1];

    ScanKeyInit(&skey[0], Anum_pg_recyclebin_rcydbid, BTEqualStrategyNumber, F_OIDEQ,
        ObjectIdGetDatum(dbid));

    rbRel = heap_open(RecyclebinRelationId, AccessShareLock);
    sd = systable_beginscan(rbRel, RecyclebinDbidRelidIndexId, true, NULL, 1, skey);
    tup = systable_getnext(sd);
    systable_endscan(sd);
    heap_close(rbRel, AccessShareLock);

    return tup == NULL;
}

bool TrRbIsEmptySpc(Oid spcId)
{
    Relation rbRel;
    SysScanDesc sd;
    ScanKeyData skey[1];
    HeapTuple tup;

    rbRel = heap_open(RecyclebinRelationId, AccessShareLock);

    ScanKeyInit(&skey[0], Anum_pg_recyclebin_rcytablespace, BTEqualStrategyNumber,
        F_OIDEQ, ObjectIdGetDatum(spcId));

    sd = systable_beginscan(rbRel, RecyclebinDbidSpcidRcycsnIndexId, true, NULL, 1, skey);
    tup = systable_getnext(sd);
    systable_endscan(sd);
    heap_close(rbRel, AccessShareLock);

    return tup == NULL;
}

bool TrRbIsEmptySchema(Oid nspId)
{
    Relation rbRel;
    SysScanDesc sd;
    ScanKeyData skey[2];
    HeapTuple tup;

    rbRel = heap_open(RecyclebinRelationId, AccessShareLock);

    ScanKeyInit(&skey[0], Anum_pg_recyclebin_rcynamespace, BTEqualStrategyNumber,
        F_OIDEQ, ObjectIdGetDatum(nspId));
    ScanKeyInit(&skey[1], Anum_pg_recyclebin_rcydbid, BTEqualStrategyNumber,
        F_OIDEQ, ObjectIdGetDatum(u_sess->proc_cxt.MyDatabaseId));

    sd = systable_beginscan(rbRel, RecyclebinDbidNspOrinameIndexId, true, NULL, 2, skey);
    tup = systable_getnext(sd);

    systable_endscan(sd);
    heap_close(rbRel, AccessShareLock);

    return tup == NULL;
}

bool TrRbIsEmptyUser(Oid roleId)
{
    Relation rbRel;
    SysScanDesc sd;
    HeapTuple tup;
    bool found = false;

    rbRel = heap_open(RecyclebinRelationId, AccessShareLock);

    sd = systable_beginscan(rbRel, InvalidOid, false, NULL, 0, NULL);

    while ((tup = systable_getnext(sd)) != NULL) {
        Form_pg_recyclebin rbForm = (Form_pg_recyclebin)GETSTRUCT(tup);
        if ((TrObjType)rbForm->rcytype != RB_OBJ_TABLE || rbForm->rcyowner != roleId) {
            continue;
        }

        found = true;
        break;
    }

    systable_endscan(sd);
    heap_close(rbRel, AccessShareLock);

    return !found;
}

static bool TrOidExists(const List *lOid, Oid oid)
{
    ListCell *cell = NULL;
    if (lOid == NULL) {
        return false;
    }

    foreach (cell, lOid) {
        if (oid == (*(Oid *)lfirst(cell))) {
            return true;
        }
    }
    return false;
}

List *TrGetDbListRcy(void)
{
    Relation rbRel;
    SysScanDesc sd;
    HeapTuple tup;

    List *lName = NIL;
    List *lOid = NIL;
    char *dbname = NULL;

    rbRel = heap_open(RecyclebinRelationId, AccessShareLock);

    sd = systable_beginscan(rbRel, InvalidOid, false, NULL, 0, NULL);

    while ((tup = systable_getnext(sd)) != NULL) {
        Form_pg_recyclebin rbForm = (Form_pg_recyclebin)GETSTRUCT(tup);
        if (TrOidExists(lOid, rbForm->rcydbid)) {
            continue;
        }
        Oid *oid = (Oid *)palloc0(sizeof(Oid));
        *oid = rbForm->rcydbid;
        lOid = lappend(lOid, oid);

        dbname = get_database_name(rbForm->rcydbid);
        if (dbname == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_DATABASE),
                    errmsg("database \"%u\" does not exist", rbForm->rcydbid)));
        }
        lName = lappend(lName, dbname);
    }

    list_free_deep(lOid);

    systable_endscan(sd);
    heap_close(rbRel, AccessShareLock);

    return lName;
}

List *TrGetDbListSpc(Oid spcId)
{
    List *lName = NIL;
    List *lOid = NIL;
    char *dbname = NULL;

    Relation rbRel;
    SysScanDesc sd;
    ScanKeyData skey[1];
    HeapTuple tup;

    rbRel = heap_open(RecyclebinRelationId, AccessShareLock);

    ScanKeyInit(&skey[0], Anum_pg_recyclebin_rcytablespace, BTEqualStrategyNumber,
        F_OIDEQ, ObjectIdGetDatum(spcId));

    sd = systable_beginscan(rbRel, RecyclebinDbidSpcidRcycsnIndexId, true, NULL, 1, skey);

    while ((tup = systable_getnext(sd)) != NULL) {
        Form_pg_recyclebin rbForm = (Form_pg_recyclebin)GETSTRUCT(tup);
        if (TrOidExists(lOid, rbForm->rcydbid)) {
            continue;
        }
        Oid *oid = (Oid *)palloc0(sizeof(Oid));
        *oid = rbForm->rcydbid;
        lOid = lappend(lOid, oid);

        dbname = get_database_name(rbForm->rcydbid);
        if (dbname == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_DATABASE),
                    errmsg("database \"%u\" does not exist", rbForm->rcydbid)));
        }
        lName = lappend(lName, dbname);
    }

    list_free_deep(lOid);

    systable_endscan(sd);
    heap_close(rbRel, AccessShareLock);

    return lName;
}

List *TrGetDbListSchema(Oid nspId)
{
    Relation rbRel;
    SysScanDesc sd;
    ScanKeyData skey[1];
    HeapTuple tup;

    List *lName = NIL;
    List *lOid = NIL;
    char *dbname = NULL;

    rbRel = heap_open(RecyclebinRelationId, AccessShareLock);

    ScanKeyInit(&skey[0], Anum_pg_recyclebin_rcynamespace, BTEqualStrategyNumber,
        F_OIDEQ, ObjectIdGetDatum(nspId));

    sd = systable_beginscan(rbRel, RecyclebinDbidNspOrinameIndexId, true, NULL, 1, skey);

    while ((tup = systable_getnext(sd)) != NULL) {
        Form_pg_recyclebin rbForm = (Form_pg_recyclebin)GETSTRUCT(tup);
        if (TrOidExists(lOid, rbForm->rcydbid)) {
            continue;
        }
        Oid *oid = (Oid *)palloc0(sizeof(Oid));
        *oid = rbForm->rcydbid;
        lOid = lappend(lOid, oid);

        dbname = get_database_name(rbForm->rcydbid);
        if (dbname == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_DATABASE),
                    errmsg("database \"%u\" does not exist", rbForm->rcydbid)));
        }
        lName = lappend(lName, dbname);
    }

    list_free_deep(lOid);

    systable_endscan(sd);
    heap_close(rbRel, AccessShareLock);

    return lName;
}

List *TrGetDbListUser(Oid roleId)
{
    Relation rbRel;
    SysScanDesc sd;
    HeapTuple tup;

    List *lName = NIL;
    List *lOid = NIL;
    char *dbname = NULL;

    rbRel = heap_open(RecyclebinRelationId, AccessShareLock);

    sd = systable_beginscan(rbRel, InvalidOid, false, NULL, 0, NULL);

    while ((tup = systable_getnext(sd)) != NULL) {
        Form_pg_recyclebin rbForm = (Form_pg_recyclebin)GETSTRUCT(tup);
        if ((TrObjType)rbForm->rcytype != RB_OBJ_TABLE || rbForm->rcyowner != roleId) {
            continue;
        }
        if (TrOidExists(lOid, rbForm->rcydbid)) {
            continue;
        }
        Oid *oid = (Oid *)palloc0(sizeof(Oid));
        *oid = rbForm->rcydbid;
        lOid = lappend(lOid, oid);

        dbname = get_database_name(rbForm->rcydbid);
        if (dbname == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_DATABASE),
                    errmsg("database \"%u\" does not exist", rbForm->rcydbid)));
        }

        lName = lappend(lName, dbname);
    }

    list_free_deep(lOid);

    systable_endscan(sd);
    heap_close(rbRel, AccessShareLock);

    return lName;
}

/*
 * TrGetDatabaseList
 *		Return a list of all databases found in pg_database.
 */
List *TrGetDbListAuto(void)
{
    List* dblist = NIL;
    Relation rel;
    SysScanDesc sd;
    HeapTuple tup;

    rel = heap_open(DatabaseRelationId, AccessShareLock);
    sd = systable_beginscan(rel, InvalidOid, false, NULL, 0, NULL);

    while ((tup = systable_getnext(sd)) != NULL) {
        Form_pg_database pgdatabase = (Form_pg_database)GETSTRUCT(tup);
        if (strcmp(NameStr(pgdatabase->datname), "template0") == 0 || 
            strcmp(NameStr(pgdatabase->datname), "template1") == 0) {
            continue;
        }
        dblist = lappend(dblist, pstrdup(NameStr(pgdatabase->datname)));
    }

    systable_endscan(sd);
    heap_close(rel, AccessShareLock);

    return dblist;
}

static bool TrObjInRecyclebin(const ObjectAddress *obj)
{
    Relation rbRel;
    SysScanDesc sd;
    HeapTuple tup;
    ScanKeyData skey[2];
    bool found = false;

    if (getObjectClass(obj) != OCLASS_CLASS) {
        return false;
    }

    ScanKeyInit(&skey[0], Anum_pg_recyclebin_rcydbid, BTEqualStrategyNumber, F_OIDEQ,
        ObjectIdGetDatum(u_sess->proc_cxt.MyDatabaseId));
    ScanKeyInit(&skey[1], Anum_pg_recyclebin_rcyrelid, BTEqualStrategyNumber, F_OIDEQ,
        ObjectIdGetDatum(obj->objectId));

    rbRel = heap_open(RecyclebinRelationId, AccessShareLock);
    sd = systable_beginscan(rbRel, RecyclebinDbidRelidIndexId, true, NULL, 2, skey);
    while ((tup = systable_getnext(sd)) != NULL) {
        Form_pg_recyclebin rbForm = (Form_pg_recyclebin)GETSTRUCT(tup);
        if (rbForm->rcyoperation == 'd') {
            found = true;
            break;
        }
    }
    systable_endscan(sd);
    heap_close(rbRel, AccessShareLock);

    return found;
}

/*
 * May this object be a recyclebin object?
 *    true: with "BIN$" prefix, or not a Relation\Type\Trigger\Constraint\Rule
 *    false: without "BIN$" prefix, or not exists
 */
static bool TrMaybeRbObject(Oid classid, Oid objid, const char *objname = NULL)
{
    HeapTuple tup;

    /* Note: we preserve rule origin name when RbDrop. */
    if (classid != RewriteRelationId && objname) {
        return strncmp(objname, "BIN$", 4) == 0;
    }

    switch (classid) {
        case RelationRelationId:
            tup = SearchSysCache1(RELOID, ObjectIdGetDatum(objid));
            if (tup != NULL) {
                objname = NameStr(((Form_pg_class)GETSTRUCT(tup))->relname);
                ReleaseSysCache(tup);
            }
            break;
        case TypeRelationId:
            tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(objid));
            if (tup != NULL) {
                objname = NameStr(((Form_pg_type)GETSTRUCT(tup))->typname);
                ReleaseSysCache(tup);
            }
            break;
        case TriggerRelationId: {
            Relation relTrig;
            ScanKeyData skey[1];
            SysScanDesc sd;

            relTrig = heap_open(TriggerRelationId, AccessShareLock);
            ScanKeyInit(&skey[0], ObjectIdAttributeNumber, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(objid));
            sd = systable_beginscan(relTrig, TriggerOidIndexId, true, NULL, 1, skey);
            if ((tup = systable_getnext(sd)) != NULL) {
                objname = NameStr(((Form_pg_trigger)GETSTRUCT(tup))->tgname);
            }
            systable_endscan(sd);
            heap_close(relTrig, AccessShareLock);
            break;
        }
        case ConstraintRelationId:
            tup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(objid));
            if (tup != NULL) {
                objname = NameStr(((Form_pg_constraint)GETSTRUCT(tup))->conname);
                ReleaseSysCache(tup);
            }
            break;
        case NamespaceRelationId:
            /* Treate Namespace as non-recyclebin object. */
            return false;
        default:
            /* May be a recyclebin object. */
            return true;
    }

    if (objname) {
        return strncmp(objname, "BIN$", 4) == 0;
    }

    return false;
}

static bool TrIsRefRbObjectImpl(Relation depRel, const ObjectAddress *obj, ObjectAddresses *objSet)
{
    int startIdx;

    if (TrObjInRecyclebin(obj)) {
        return true;
    }

    startIdx = objSet->numrefs;

    if (!TrMaybeRbObject(obj->classId, obj->objectId)) {
        return false;
    }

    TrFindAllRefObjs(depRel, obj, objSet, true);
    TrFindAllInternalObjs(depRel, obj, objSet, true);

    for (int i = startIdx; i < objSet->numrefs; i++) {
        if (TrIsRefRbObjectImpl(depRel, &objSet->refs[i], objSet)) {
            return true;
        }
    }

    return false;
}

/* object is a rb object, or reference to a rb object. */
static bool TrIsRefRbObject(const ObjectAddress *obj, Relation depRel)
{
    ObjectAddresses *objSet = new_object_addresses();
    bool relArgNull = depRel == NULL;
    bool result = false;

    if (relArgNull) {
        depRel = heap_open(DependRelationId, AccessShareLock);
    }

    /* Note: we not care obj->deptype here. */
    add_object_address_ext1(obj, objSet);

    result = TrIsRefRbObjectImpl(depRel, obj, objSet);

    free_object_addresses(objSet);

    if (relArgNull) {
        heap_close(depRel, AccessShareLock);
    }

    return result;
}

bool TrIsRefRbObjectEx(Oid classid, Oid objid, const char *objname)
{
    if (!TcapFeatureAvail()) {
        return false;
    }

    if (classid != RewriteRelationId && objname && strncmp(objname, "BIN$", 4) != 0) {
        return false;
    }

    /* Note: we preserve rule origin name when RbDrop. */
    if (TrRbIsEmptyDb(u_sess->proc_cxt.MyDatabaseId)) {
        return false;
    }

    ObjectAddress obj = {classid, objid};

    return TrIsRefRbObject(&obj);
}

void TrForbidAccessRbDependencies(Relation depRel, const ObjectAddress *depender,
    const ObjectAddress *referenced, int nreferenced)
{
    if (!TcapFeatureAvail()) {
        return;
    }

    if (IsInitdb || TrRbIsEmptyDb(u_sess->proc_cxt.MyDatabaseId)) {
        return;
    }

    if (TrIsRefRbObject(depender, depRel)) {
        elog (ERROR, "can not access recycle object.");
    }

    for (int i = 0; i < nreferenced; i++, referenced++) {
        if (TrIsRefRbObject(referenced, depRel)) {
            elog (ERROR, "can not access recycle object.");
        }
    }

    return;
}

void TrForbidAccessRbObject(Oid classid, Oid objid, const char *objname)
{
    if (!TcapFeatureAvail()) {
        return;
    }

    if (!TrMaybeRbObject(classid, objid, objname) || TrRbIsEmptyDb(u_sess->proc_cxt.MyDatabaseId)) {
        return;
    }

    ObjectAddress obj = {classid, objid};
    if (TrIsRefRbObject(&obj)) {
        elog (ERROR, "can not access recycle object.");
    }

    return;
}

Datum gs_is_recycle_object(PG_FUNCTION_ARGS)
{
    int classid = PG_GETARG_INT32(0);
    int objid = PG_GETARG_INT32(1);
    Name objname = PG_GETARG_NAME(2);
    bool result = false;
    result = TrIsRefRbObjectEx(classid, objid, NameStr(*objname));
    PG_RETURN_BOOL(result);
}

Datum gs_is_recycle_obj(PG_FUNCTION_ARGS)
{
    Oid classid = PG_GETARG_OID(0);
    Oid objid = PG_GETARG_OID(1);
    Name objname = PG_GETARG_NAME(2);
    bool result = false;
    result = TrIsRefRbObjectEx(classid, objid, NameStr(*objname));
    PG_RETURN_BOOL(result);
}
