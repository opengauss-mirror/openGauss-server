/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
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
 * pg_object.cpp
 *
 * IDENTIFICATION
 *    src/common/backend/catalog/pg_object.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "access/heapam.h"
#include "access/xact.h"
#include "access/transam.h"
#include "catalog/indexing.h"
#include "utils/inval.h"
#include "catalog/pg_object.h"
#include "catalog/pg_class.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/rel.h"
#include "miscadmin.h"
#include "catalog/index.h"
#include "utils/knl_relcache.h"

/*
 * @Description: Insert a new record to pg_object.
 * @in objectOid - object oid.
 * @in objectType - object type.
 * @in creator - object creator.
 * @in ctime - object create time.
 * @in mtime - object modify time.
 * @in createcsn - the commit sequence number when object create.
 * @in changecsn - the commit sequence number when the old version expires. 
 * @returns - void
 */
void CreatePgObject(Oid objectOid, PgObjectType objectType, Oid creator, const PgObjectOption objectOpt)
{
    Datum values[Natts_pg_object];
    bool nulls[Natts_pg_object];
    bool replaces[Natts_pg_object];
    HeapTuple tuple = NULL;
    HeapTuple oldtuple = NULL;
    Relation rel = NULL;
    errno_t rc = 0;

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check_c(rc, "\0", "\0");
    rc = memset_s(replaces, sizeof(replaces), true, sizeof(replaces));
    securec_check_c(rc, "\0", "\0");

    if (IsInitdb) {
        return;
    }
    Datum nowtime = TimeGetDatum(GetCurrentTransactionStartTimestamp());
    CommitSeqNo csn = t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo;
    if (nowtime == (Datum)NULL) {
        return;
    }

    values[Anum_pg_object_oid - 1] = ObjectIdGetDatum(objectOid);
    values[Anum_pg_object_type - 1] = CharGetDatum(objectType);
    if (OidIsValid(creator)) {
        values[Anum_pg_object_creator - 1] = ObjectIdGetDatum(creator);
    } else {
        nulls[Anum_pg_object_creator - 1] = true;
    }

    if (objectOpt.hasCtime) {
        values[Anum_pg_object_ctime - 1] = nowtime;
    } else {
        nulls[Anum_pg_object_ctime - 1] = true;
    }

    if (objectOpt.hasMtime) {
        values[Anum_pg_object_mtime - 1] = nowtime;
    } else {
        nulls[Anum_pg_object_mtime - 1] = true;
    }

    if (objectOpt.hasCreatecsn && (t_thrd.proc->workingVersionNum >= INPLACE_UPDATE_VERSION_NUM)) {
        values[Anum_pg_object_createcsn - 1] = UInt64GetDatum(csn);
    } else {
        nulls[Anum_pg_object_createcsn - 1] = true;
    }

    if (objectOpt.hasChangecsn && (t_thrd.proc->workingVersionNum >= INPLACE_UPDATE_VERSION_NUM)) {
        values[Anum_pg_object_changecsn - 1] = UInt64GetDatum(csn);
    } else {
        nulls[Anum_pg_object_changecsn - 1] = true;
    }
    rel = heap_open(PgObjectRelationId, RowExclusiveLock);

    oldtuple = SearchSysCache2(PGOBJECTID, ObjectIdGetDatum(objectOid), CharGetDatum(objectType));
    if (HeapTupleIsValid(oldtuple)) {
        replaces[Anum_pg_object_oid - 1] = false;
        replaces[Anum_pg_object_type - 1] = false;
        tuple = heap_modify_tuple(oldtuple, RelationGetDescr(rel), values, nulls, replaces);
        simple_heap_update(rel, &tuple->t_self, tuple);
        ReleaseSysCache(oldtuple);
    } else {
        tuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);
        (void)simple_heap_insert(rel, tuple);
    }
    CatalogUpdateIndexes(rel, tuple);
    heap_freetuple_ext(tuple);
    heap_close(rel, RowExclusiveLock);
}

/*
 * Description: Delete object from pg_object_proc.
 *
 * Parameters:
 * 	@in objectOid: object id.
 * 	@in objectType: object type.
 * Returns: void
 */
void DeletePgObject(Oid objectOid, PgObjectType objectType)
{
    Relation relation = NULL;
    HeapTuple tup = NULL;

    if (!IsNormalProcessingMode() || u_sess->attr.attr_common.upgrade_mode != 0 || IsInitdb) {
        return;
    }

    relation = heap_open(PgObjectRelationId, RowExclusiveLock);

    tup = SearchSysCache2(PGOBJECTID, ObjectIdGetDatum(objectOid), CharGetDatum(objectType));
    if (HeapTupleIsValid(tup)) {
        simple_heap_delete(relation, &tup->t_self);
        ReleaseSysCache(tup);
    }
    heap_close(relation, RowExclusiveLock);
}

/*
 * Description: If an index is defined on the table, the changecsn of that table should
 * be the maximum value of the changecsn of the table and index.
 *
 * Parameters:
 * 	@in indexIds: the index list define on table.
 * 	@in relationChangecsn: the changecsn of table recorded in pg_object.
 * Returns: CommitSeqNo
 */
static CommitSeqNo GetMaxChangecsn(Relation rel, List* indexIds, CommitSeqNo relationChangecsn)
{
    HeapTuple tup = NULL;
    TupleDesc tupdesc = NULL;
    ListCell* indexId = NULL;
    CommitSeqNo changecsn = InvalidCommitSeqNo;
    CommitSeqNo maxChangecsn = relationChangecsn;
    bool changecsnIsnull = false;
    Datum valueChangecsn;
    Relation objectRel = heap_open(PgObjectRelationId, AccessShareLock);
    tupdesc = RelationGetDescr(objectRel);
    foreach(indexId, indexIds) {
        Oid indexOid = lfirst_oid(indexId);
        tup = SearchSysCache2(PGOBJECTID, ObjectIdGetDatum(indexOid), CharGetDatum(OBJECT_TYPE_INDEX));
        if (!HeapTupleIsValid(tup)) { 
            changecsn = InvalidCommitSeqNo;
        } else {
            valueChangecsn = heap_getattr(tup, Anum_pg_object_changecsn, tupdesc, &changecsnIsnull);
            if (!changecsnIsnull) {
                changecsn = UInt64GetDatum(valueChangecsn);
            }
            ReleaseSysCache(tup);
        }
        if (changecsn > maxChangecsn) {
            maxChangecsn = changecsn;
        }
    }
    list_free_ext(indexIds);
    heap_close(objectRel, AccessShareLock);
    return maxChangecsn;
}

/*
 * Description: Get createcsn and changecsn from pg_object.
 *
 * Parameters:
 * 	@in objectOid: object id.
 * 	@in objectType: object type.
 * 	@in csnInfo: the createcsn and changecsn of object.
 * Returns: void
 */
void GetObjectCSN(Oid objectOid, Relation userRel, PgObjectType objectType, ObjectCSN * const csnInfo)
{
    HeapTuple tup = NULL;
    TupleDesc tupdesc = NULL;
    bool createcsnIsnull = false;
    bool changecsnIsnull = false;
    Relation relation = NULL;
    Datum valueChangecsn;
    Datum valueCreatecsn;
    /* 
       The pg_object system table does not hold objects generated during the database init 
       process,so the object's createcsn and changecsn is zero during the process.
    */
    if (IsInitdb || !(t_thrd.proc->workingVersionNum >= INPLACE_UPDATE_VERSION_NUM)) {
        return;
    }
    /* 
       The pg_object records mtime and changecsn when the DDL command occurs.However,unlike 
       the normal table creation process, no user can directly operate on the system 
       table after the initialization process. So we record createcsn and changecsn of the 
       system table as zero.
    */
    if (objectOid < FirstNormalObjectId) {
        return;
    }
    relation = heap_open(PgObjectRelationId, RowExclusiveLock);
    tupdesc = RelationGetDescr(relation);
    tup = SearchSysCache2(PGOBJECTID, ObjectIdGetDatum(objectOid), CharGetDatum(objectType));
    if (!HeapTupleIsValid(tup)) { 
        heap_close(relation, RowExclusiveLock);
        return;
    } else {
        valueChangecsn = heap_getattr(tup, Anum_pg_object_changecsn, tupdesc, &changecsnIsnull);
        valueCreatecsn = heap_getattr(tup, Anum_pg_object_createcsn, tupdesc, &createcsnIsnull);
        if (!changecsnIsnull) {
            csnInfo->changecsn = UInt64GetDatum(valueChangecsn);
        }
        if (!createcsnIsnull) {
            csnInfo->createcsn = UInt64GetDatum(valueCreatecsn);
        }
        /* 
            When an  index is created or modified, the changecsn of table defining
            index should be updated to the maximum changecsn of the index and table.
        */
        if (objectType == OBJECT_TYPE_RELATION) {
            bool hasindex = userRel->rd_rel->relhasindex;
            if (hasindex) {
                List* indexIds = RelationGetIndexList(userRel);
                csnInfo->changecsn = GetMaxChangecsn(userRel, indexIds, csnInfo->changecsn);
            }
        }
        ReleaseSysCache(tup);
        heap_close(relation, RowExclusiveLock);
    }
    return;
}

/*
 * Description: Check object whether exist.
 *
 * Parameters:
 * 	@in objectOid: object id.
 * 	@in objectType: object type.
 * Returns: bool
 */
bool CheckObjectExist(Oid objectOid, PgObjectType objectType)
{
    HeapTuple tup = NULL;
    switch (objectType) {
        case OBJECT_TYPE_RELATION:
        case OBJECT_TYPE_INDEX:
        case OBJECT_TYPE_SEQUENCE:
        case OBJECT_TYPE_VIEW:
        case OBJECT_TYPE_CONTQUERY:
        case OBJECT_TYPE_FOREIGN_TABLE:
        case OBJECT_TYPE_STREAM:
            tup = SearchSysCache1(RELOID, ObjectIdGetDatum(objectOid));
            break;
        case OBJECT_TYPE_PROC:
            tup = SearchSysCache1(PROCOID, ObjectIdGetDatum(objectOid));
            break;
        case OBJECT_TYPE_PKGSPEC:
            tup = SearchSysCache1(PACKAGEOID, ObjectIdGetDatum(objectOid));
            break;
        default:
            return false;
    }
    if (HeapTupleIsValid(tup)) {
        ReleaseSysCache(tup);
        return true;
    } else {
        return false;
    }
}

/*
 * Description: Update object info to pg_object when object changed.
 *
 * Parameters:
 * 	@in objectOid: object id.
 * 	@in objectType: object type.
 * 	@in updateChangecsn: If true, update changecSN.
 * Returns: void
 */
void UpdatePgObjectMtime(Oid objectOid, PgObjectType objectType)
{
    if (objectType == OBJECT_TYPE_INVALID) {
        return;
    }
    Relation relation = NULL;
    HeapTuple tup = NULL;
    CommitSeqNo csn = t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo;
    Datum nowtime = TimeGetDatum(GetCurrentTransactionStartTimestamp());
    if (IsInitdb || !IsNormalProcessingMode() || nowtime == (Datum)NULL || !CheckObjectExist(objectOid, objectType)) {
        return;
    }
    relation = heap_open(PgObjectRelationId, RowExclusiveLock);
    tup = SearchSysCache2(PGOBJECTID, ObjectIdGetDatum(objectOid), CharGetDatum(objectType));
    if (!HeapTupleIsValid(tup)) {
        PgObjectOption objectOpt = {false, true, false, true};
        CreatePgObject(objectOid, objectType, InvalidOid, objectOpt);
    } else {
        Datum values[Natts_pg_object];
        bool nulls[Natts_pg_object];
        bool replaces[Natts_pg_object];
        errno_t rc = EOK;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
        securec_check(rc, "\0", "\0");
        replaces[Anum_pg_object_mtime - 1] = true;
        values[Anum_pg_object_mtime - 1] = nowtime;
        if (t_thrd.proc->workingVersionNum >= INPLACE_UPDATE_VERSION_NUM) {
            if (!u_sess->exec_cxt.isExecTrunc) {
                replaces[Anum_pg_object_changecsn - 1] = true;
                values[Anum_pg_object_changecsn - 1] = UInt64GetDatum(csn);
            }
        }
        HeapTuple newtuple = heap_modify_tuple(tup, RelationGetDescr(relation), values, nulls, replaces);
        simple_heap_update(relation, &newtuple->t_self, newtuple);
        CatalogUpdateIndexes(relation, newtuple);
        ReleaseSysCache(tup);
        heap_freetuple_ext(newtuple);
        if (t_thrd.proc->workingVersionNum >= INPLACE_UPDATE_VERSION_NUM) {
            if (!u_sess->exec_cxt.isExecTrunc &&
                (objectType == OBJECT_TYPE_RELATION || objectType == OBJECT_TYPE_INDEX)) {
                Relation userRel = RelationIdGetRelation(objectOid);
                CacheInvalidateRelcache(userRel);
                RelationClose(userRel);
            }
        }
    }
    heap_close(relation, RowExclusiveLock);
}

/*
 * Description: Update changecsn in pg_object when object changed.
 *
 * Parameters:
 * 	@in objectOid: object id.
 * 	@in objectType: object type.
 * Returns: void
 */
void UpdatePgObjectChangecsn(Oid objectOid, PgObjectType objectType)
{
    Relation relation = NULL;
    HeapTuple tup = NULL;
    CommitSeqNo csn = t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo;
    if (!(t_thrd.proc->workingVersionNum >= INPLACE_UPDATE_VERSION_NUM) || IsInitdb) {
        return;
    }
    if (!CheckObjectExist(objectOid, objectType)) {
        return;
    }
    if (u_sess->exec_cxt.isExecTrunc) {
        return;
    }
    relation = heap_open(PgObjectRelationId, RowExclusiveLock);
    tup = SearchSysCache2(PGOBJECTID, ObjectIdGetDatum(objectOid), CharGetDatum(objectType));
    if (!HeapTupleIsValid(tup)) {
        PgObjectOption objectOpt = {false, true, false, true};
        CreatePgObject(objectOid, objectType, InvalidOid, objectOpt);
    } else {
        Datum values[Natts_pg_object] = {0};
        bool nulls[Natts_pg_object] = {0};
        bool replaces[Natts_pg_object] = {0};
        replaces[Anum_pg_object_changecsn - 1] = true;
        values[Anum_pg_object_changecsn - 1] = UInt64GetDatum(csn);
        HeapTuple newtuple = heap_modify_tuple(tup, RelationGetDescr(relation), values, nulls, replaces);
        simple_heap_update(relation, &newtuple->t_self, newtuple);
        CatalogUpdateIndexes(relation, newtuple);
        ReleaseSysCache(tup);
        heap_freetuple_ext(newtuple);
        Relation userRel = RelationIdGetRelation(objectOid);
        CacheInvalidateRelcache(userRel);
        RelationClose(userRel);
    }
    heap_close(relation, RowExclusiveLock);
}

/*
 * Description: Get the type of object which in pg_class.
 *
 * Parameters:
 * 	@in relkind: relkind in pg_class.
 * Returns: PgObjectType
 */
PgObjectType GetPgObjectTypePgClass(char relkind)
{
    PgObjectType objectType = OBJECT_TYPE_INVALID;
    switch (relkind) {
        case RELKIND_RELATION:
            objectType = OBJECT_TYPE_RELATION;
            break;
        case RELKIND_FOREIGN_TABLE:
            objectType = OBJECT_TYPE_FOREIGN_TABLE;
            break;
        case RELKIND_STREAM:
            objectType = OBJECT_TYPE_STREAM;
            break;
        case RELKIND_VIEW:
            objectType = OBJECT_TYPE_VIEW;
            break;
        case RELKIND_CONTQUERY:
            objectType = OBJECT_TYPE_CONTQUERY;
            break;
        case RELKIND_SEQUENCE:
            objectType = OBJECT_TYPE_SEQUENCE;
            break;
        case RELKIND_LARGE_SEQUENCE:
            objectType = OBJECT_TYPE_LARGE_SEQUENCE;
            break;
        case RELKIND_INDEX:
        case RELKIND_GLOBAL_INDEX:
            objectType = OBJECT_TYPE_INDEX;
            break;
        default:
            objectType = OBJECT_TYPE_INVALID;
            break;
    }
    return objectType;
}

/*
 * Description: Record modify time of a relation.
 *
 * Parameters:
 * 	@in relOid: relid
 * 	@in relkind: relkind of pg_class
 *
 */
void recordRelationMTime(Oid relOid, char relkind)
{
    PgObjectType objectType = GetPgObjectTypePgClass(relkind);
    if (objectType != OBJECT_TYPE_INVALID) {
        UpdatePgObjectMtime(relOid, objectType);
    }
}

/*
 * Description: Record time of comment object.
 *
 * Parameters:
 * 	@in addr: object address
 * 	@in objType: object type.
 * 	@in rel: the comment relation
 *
 */
void recordCommentObjectTime(ObjectAddress addr, Relation rel, ObjectType objType)
{
    switch (objType) {
        case OBJECT_INDEX:
        case OBJECT_SEQUENCE:
        case OBJECT_LARGE_SEQUENCE:
        case OBJECT_TABLE:
        case OBJECT_VIEW:
        case OBJECT_CONTQUERY:
        case OBJECT_FOREIGN_TABLE:
        case OBJECT_STREAM:
        case OBJECT_COLUMN:
            if (rel != NULL) {
                recordRelationMTime(rel->rd_id, rel->rd_rel->relkind);
            }
            break;
        case OBJECT_FUNCTION:
            if (addr.objectId != InvalidOid) {
                UpdatePgObjectMtime(addr.objectId, OBJECT_TYPE_PROC);
            }
            break;
        default:
            break;
    }
}
