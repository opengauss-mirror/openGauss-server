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
#include "catalog/indexing.h"
#include "catalog/pg_object.h"
#include "catalog/pg_class.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/rel.h"
#include "miscadmin.h"

/*
 * @Description: Insert a new record to pg_object.
 * @in objectOid - object oid.
 * @in objectType - object type.
 * @in creator - object creator.
 * @in ctime - object create time.
 * @in mtime - object modify time.
 * @returns - void
 */
void CreatePgObject(Oid objectOid, PgObjectType objectType, Oid creator, bool hasCtime, bool hasMtime)
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

    if (!IsNormalProcessingMode() || u_sess->attr.attr_common.upgrade_mode != 0 || IsInitdb) {
        return;
    }
    Datum nowtime = TimeGetDatum(GetCurrentTransactionStartTimestamp());
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

    if (hasCtime) {
        values[Anum_pg_object_ctime - 1] = nowtime;
    } else {
        nulls[Anum_pg_object_ctime - 1] = true;
    }

    if (hasMtime) {
        values[Anum_pg_object_mtime - 1] = nowtime;
    } else {
        nulls[Anum_pg_object_mtime - 1] = true;
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
 * Returns: void
 */
void UpdatePgObjectMtime(Oid objectOid, PgObjectType objectType)
{
    if (objectType == OBJECT_TYPE_INVALID) {
        return;
    }
    Relation relation = NULL;
    HeapTuple tup = NULL;
    Datum nowtime = TimeGetDatum(GetCurrentTransactionStartTimestamp());
    if (!IsNormalProcessingMode() || u_sess->attr.attr_common.upgrade_mode != 0 || IsInitdb) {
        return;
    }
    if (nowtime == (Datum)NULL) {
        return;
    }
    if (!CheckObjectExist(objectOid, objectType)) {
        return;
    }

    relation = heap_open(PgObjectRelationId, RowExclusiveLock);
    tup = SearchSysCache2(PGOBJECTID, ObjectIdGetDatum(objectOid), CharGetDatum(objectType));
    if (!HeapTupleIsValid(tup)) {
        CreatePgObject(objectOid, objectType, InvalidOid, false, true);
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

        HeapTuple newtuple = heap_modify_tuple(tup, RelationGetDescr(relation), values, nulls, replaces);
        simple_heap_update(relation, &newtuple->t_self, newtuple);
        CatalogUpdateIndexes(relation, newtuple);
        ReleaseSysCache(tup);
        heap_freetuple_ext(newtuple);
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
