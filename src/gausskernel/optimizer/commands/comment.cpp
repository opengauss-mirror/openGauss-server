/* -------------------------------------------------------------------------
 *
 * comment.cpp
 *
 * openGauss object comments utility code.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/commands/comment.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/genam.h"
#include "access/heapam.h"

#include "access/tableam.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_description.h"
#include "catalog/pg_object.h"
#include "catalog/pg_shdescription.h"
#include "commands/comment.h"
#include "commands/dbcommands.h"
#include "miscadmin.h"
#include "storage/tcap.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/snapmgr.h"

/*
 * @Description: check whether it's a share object.
 * @in objtype : the type of the object.
 * @return : return true if it's a share object like databases, tablespaces, roles or users.
 */
static bool CheckShareObject(ObjectType objtype)
{
    if (objtype == OBJECT_DATABASE || objtype == OBJECT_TABLESPACE || objtype == OBJECT_ROLE || objtype == OBJECT_USER)
        return true;
    else
        return false;
}

static AclResult CheckObjectCommentPrivilege(const CommentStmt* stmt, Oid objectId, const Relation relation)
{
    AclResult aclresult = ACLCHECK_NO_PRIV;
    switch (stmt->objtype) {
        case OBJECT_SEQUENCE:
        case OBJECT_LARGE_SEQUENCE:
        case OBJECT_TABLE:
        case OBJECT_VIEW:
        case OBJECT_MATVIEW:
        case OBJECT_FOREIGN_TABLE:
            aclresult = pg_class_aclcheck(objectId, GetUserId(), ACL_COMMENT);
            break;
        case OBJECT_INDEX:
            aclresult = pg_class_aclcheck(IndexGetRelation(objectId, false), GetUserId(), ACL_INDEX);
            break;
        case OBJECT_COLUMN:
            if (relation != NULL) {
                Oid rel_id = RelationGetRelid(relation);
                aclresult = pg_class_aclcheck(rel_id, GetUserId(), ACL_COMMENT);
                if (aclresult != ACLCHECK_OK) {
                    char* attname = strVal(lfirst(list_tail(stmt->objname)));
                    AttrNumber attnum = get_attnum(rel_id, attname);
                    aclresult = pg_attribute_aclcheck(rel_id, attnum, GetUserId(), ACL_COMMENT);
                }
            }
            break;
        case OBJECT_TYPE:
            aclresult = pg_type_aclcheck(objectId, GetUserId(), ACL_COMMENT);
            break;
        case OBJECT_DATABASE:
            aclresult = pg_database_aclcheck(objectId, GetUserId(), ACL_COMMENT);
            break;
        case OBJECT_FUNCTION:
            aclresult = pg_proc_aclcheck(objectId, GetUserId(), ACL_COMMENT);
            break;
        case OBJECT_SCHEMA:
            aclresult = pg_namespace_aclcheck(objectId, GetUserId(), ACL_COMMENT);
            break;
        case OBJECT_FOREIGN_SERVER:
            aclresult = pg_foreign_server_aclcheck(objectId, GetUserId(), ACL_COMMENT);
            break;
        case OBJECT_TABLESPACE:
            aclresult = pg_tablespace_aclcheck(objectId, GetUserId(), ACL_COMMENT);
            break;
        default:
            aclresult = ACLCHECK_NO_PRIV;
            break;
    }
    return aclresult;
}
/*
 * CommentObject --
 *
 * This routine is used to add the associated comment into
 * pg_description for the object specified by the given SQL command.
 */
ObjectAddress CommentObject(CommentStmt* stmt)
{
    Relation relation;
    ObjectAddress address = InvalidObjectAddress;

    /*
     * When loading a dump, we may see a COMMENT ON DATABASE for the old name
     * of the database.  Erroring out would prevent pg_restore from completing
     * (which is really pg_restore's fault, but for now we will work around
     * the problem here).  Consensus is that the best fix is to treat wrong
     * database name as a WARNING not an ERROR; hence, the following special
     * case.  (If the length of stmt->objname is not 1, get_object_address
     * will throw an error below; that's OK.)
     */
    if (stmt->objtype == OBJECT_DATABASE && list_length(stmt->objname) == 1) {
        char* database = strVal(linitial(stmt->objname));

        if (!OidIsValid(get_database_oid(database, true))) {
            ereport(WARNING, (errcode(ERRCODE_UNDEFINED_DATABASE), errmsg("database \"%s\" does not exist", database)));
            return address;
        }
    }

    /*
     * Translate the parser representation that identifies this object into an
     * ObjectAddress.  get_object_address() will throw an error if the object
     * does not exist, and will also acquire a lock on the target to guard
     * against concurrent DROP operations.
     */
    address =
        get_object_address(stmt->objtype, stmt->objname, stmt->objargs, &relation, ShareUpdateExclusiveLock, false);

    TrForbidAccessRbObject(address.classId, address.objectId);

    /* Require ownership or comment privilege of the target object. */
    AclResult aclresult = CheckObjectCommentPrivilege(stmt, address.objectId, relation);
    if (aclresult != ACLCHECK_OK) {
        check_object_ownership(GetUserId(), stmt->objtype, address, stmt->objname, stmt->objargs, relation);
    }

    /* Perform other integrity checks as needed. */
    switch (stmt->objtype) {
        case OBJECT_COLUMN:

            /*
             * Allow comments only on columns of tables, views, materialized
             * views, composite types, and foreign tables (which are the only
             * relkinds for which pg_dump will dump per-column comments).  In
             * particular we wish to disallow comments on index columns,
             * because the naming of an index's columns may change across PG
             * versions, so dumping per-column comments could create reload
             * failures.
             */
            if (relation != NULL) {
                if (relation->rd_rel->relkind != RELKIND_RELATION && 
                    relation->rd_rel->relkind != RELKIND_VIEW &&
                    relation->rd_rel->relkind != RELKIND_CONTQUERY &&
                    relation->rd_rel->relkind != RELKIND_MATVIEW &&
                    relation->rd_rel->relkind != RELKIND_COMPOSITE_TYPE &&
                    relation->rd_rel->relkind != RELKIND_STREAM &&
                    relation->rd_rel->relkind != RELKIND_FOREIGN_TABLE)
                    ereport(ERROR,
                       (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                            errmsg("\"%s\" is not a table, view, composite type, or foreign table",
                               RelationGetRelationName(relation))));
            }
            break;
        default:
            break;
    }

    /*
     * Databases, tablespaces, roles and users are cluster-wide objects, so any
     * comments on those objects are recorded in the shared pg_shdescription
     * catalog.  Comments on all other objects are recorded in pg_description.
     */
    if (CheckShareObject(stmt->objtype))
        CreateSharedComments(address.objectId, address.classId, stmt->comment);
    else
        CreateComments(address.objectId, address.classId, address.objectSubId, stmt->comment);

    /* Recode time of comment object. */
    recordCommentObjectTime(address, relation, stmt->objtype);

    /*
     * If get_object_address() opened the relation for us, we close it to keep
     * the reference count correct - but we retain any locks acquired by
     * get_object_address() until commit time, to guard against concurrent
     * activity.
     */
    if (relation != NULL)
        relation_close(relation, NoLock);

    return address;
}

/*
 * CreateComments --
 *
 * Create a comment for the specified object descriptor.  Inserts a new
 * pg_description tuple, or replaces an existing one with the same key.
 *
 * If the comment given is null or an empty string, instead delete any
 * existing comment for the specified key.
 */
void CreateComments(Oid oid, Oid classoid, int32 subid, const char* comment)
{
    Relation description;
    ScanKeyData skey[3];
    SysScanDesc sd;
    HeapTuple oldtuple;
    HeapTuple newtuple = NULL;
    Datum values[Natts_pg_description];
    bool nulls[Natts_pg_description];
    bool replaces[Natts_pg_description];
    int i;

    /* Reduce empty-string to NULL case */
    if (comment != NULL && strlen(comment) == 0)
        comment = NULL;

    /* Prepare to form or update a tuple, if necessary */
    if (comment != NULL) {
        for (i = 0; i < Natts_pg_description; i++) {
            nulls[i] = false;
            replaces[i] = true;
        }
        values[Anum_pg_description_objoid - 1] = ObjectIdGetDatum(oid);
        values[Anum_pg_description_classoid - 1] = ObjectIdGetDatum(classoid);
        values[Anum_pg_description_objsubid - 1] = Int32GetDatum(subid);
        values[Anum_pg_description_description - 1] = CStringGetTextDatum(comment);
    }

    /* Use the index to search for a matching old tuple */
    ScanKeyInit(&skey[0], Anum_pg_description_objoid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(oid));
    ScanKeyInit(&skey[1], Anum_pg_description_classoid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(classoid));
    ScanKeyInit(&skey[2], Anum_pg_description_objsubid, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(subid));

    description = heap_open(DescriptionRelationId, RowExclusiveLock);

    sd = systable_beginscan(description, DescriptionObjIndexId, true, NULL, 3, skey);
    if ((oldtuple = systable_getnext(sd)) != NULL) {
        /* Found the old tuple, so delete or update it */
        if (comment == NULL) {
            simple_heap_delete(description, &oldtuple->t_self);
        } else {
            newtuple = (HeapTuple) tableam_tops_modify_tuple(oldtuple, RelationGetDescr(description), values, nulls, replaces);
            simple_heap_update(description, &oldtuple->t_self, newtuple);
        }
    } /* Assume there can be only one match */

    systable_endscan(sd);

    /* If we didn't find an old tuple, insert a new one */
    if (newtuple == NULL && comment != NULL) {
        newtuple = heap_form_tuple(RelationGetDescr(description), values, nulls);
        (void)simple_heap_insert(description, newtuple);
    }

    /* Update indexes, if necessary */
    if (newtuple != NULL) {
        CatalogUpdateIndexes(description, newtuple);
        tableam_tops_free_tuple(newtuple);
    }

    /* Done */
    heap_close(description, NoLock);
}

/*
 * CreateSharedComments --
 *
 * Create a comment for the specified shared object descriptor.  Inserts a
 * new pg_shdescription tuple, or replaces an existing one with the same key.
 *
 * If the comment given is null or an empty string, instead delete any
 * existing comment for the specified key.
 */
void CreateSharedComments(Oid oid, Oid classoid, const char* comment)
{
    Relation shdescription;
    ScanKeyData skey[2];
    SysScanDesc sd;
    HeapTuple oldtuple;
    HeapTuple newtuple = NULL;
    Datum values[Natts_pg_shdescription];
    bool nulls[Natts_pg_shdescription];
    bool replaces[Natts_pg_shdescription];
    int i;

    /* Reduce empty-string to NULL case */
    if (comment != NULL && strlen(comment) == 0)
        comment = NULL;

    /* Prepare to form or update a tuple, if necessary */
    if (comment != NULL) {
        for (i = 0; i < Natts_pg_shdescription; i++) {
            nulls[i] = false;
            replaces[i] = true;
        }
        values[Anum_pg_shdescription_objoid - 1] = ObjectIdGetDatum(oid);
        values[Anum_pg_shdescription_classoid - 1] = ObjectIdGetDatum(classoid);
        values[Anum_pg_shdescription_description - 1] = CStringGetTextDatum(comment);
    }

    /* Use the index to search for a matching old tuple */
    ScanKeyInit(&skey[0], Anum_pg_shdescription_objoid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(oid));
    ScanKeyInit(&skey[1], Anum_pg_shdescription_classoid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(classoid));

    shdescription = heap_open(SharedDescriptionRelationId, RowExclusiveLock);

    sd = systable_beginscan(shdescription, SharedDescriptionObjIndexId, true, NULL, 2, skey);
    if ((oldtuple = systable_getnext(sd)) != NULL) {
        /* Found the old tuple, so delete or update it */
        if (comment == NULL)
            simple_heap_delete(shdescription, &oldtuple->t_self);
        else {
            newtuple = (HeapTuple) tableam_tops_modify_tuple(oldtuple, RelationGetDescr(shdescription), values, nulls, replaces);
            simple_heap_update(shdescription, &oldtuple->t_self, newtuple);
        }
    } /* Assume there can be only one match */

    systable_endscan(sd);

    /* If we didn't find an old tuple, insert a new one */
    if (newtuple == NULL && comment != NULL) {
        newtuple = heap_form_tuple(RelationGetDescr(shdescription), values, nulls);
        (void)simple_heap_insert(shdescription, newtuple);
    }

    /* Update indexes, if necessary */
    if (newtuple != NULL) {
        CatalogUpdateIndexes(shdescription, newtuple);
        tableam_tops_free_tuple(newtuple);
    }

    /* Done */
    heap_close(shdescription, NoLock);
}

/*
 * DeleteComments -- remove comments for an object
 *
 * If subid is nonzero then only comments matching it will be removed.
 * If subid is zero, all comments matching the oid/classoid will be removed
 * (this corresponds to deleting a whole object).
 */
void DeleteComments(Oid oid, Oid classoid, int32 subid)
{
    Relation description;
    ScanKeyData skey[3];
    int nkeys;
    SysScanDesc sd;
    HeapTuple oldtuple;

    /* Use the index to search for all matching old tuples */
    ScanKeyInit(&skey[0], Anum_pg_description_objoid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(oid));
    ScanKeyInit(&skey[1], Anum_pg_description_classoid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(classoid));

    if (subid != 0) {
        ScanKeyInit(&skey[2], Anum_pg_description_objsubid, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(subid));
        nkeys = 3;
    } else
        nkeys = 2;

    description = heap_open(DescriptionRelationId, RowExclusiveLock);

    sd = systable_beginscan(description, DescriptionObjIndexId, true, NULL, nkeys, skey);

    while ((oldtuple = systable_getnext(sd)) != NULL)
        simple_heap_delete(description, &oldtuple->t_self);

    /* Done */
    systable_endscan(sd);
    heap_close(description, RowExclusiveLock);
}

/*
 * DeleteSharedComments -- remove comments for a shared object
 */
void DeleteSharedComments(Oid oid, Oid classoid)
{
    Relation shdescription;
    ScanKeyData skey[2];
    SysScanDesc sd;
    HeapTuple oldtuple;

    /* Use the index to search for all matching old tuples */
    ScanKeyInit(&skey[0], Anum_pg_shdescription_objoid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(oid));
    ScanKeyInit(&skey[1], Anum_pg_shdescription_classoid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(classoid));

    shdescription = heap_open(SharedDescriptionRelationId, RowExclusiveLock);

    sd = systable_beginscan(shdescription, SharedDescriptionObjIndexId, true, NULL, 2, skey);

    while ((oldtuple = systable_getnext(sd)) != NULL)
        simple_heap_delete(shdescription, &oldtuple->t_self);

    /* Done */
    systable_endscan(sd);
    heap_close(shdescription, RowExclusiveLock);
}

/*
 * GetComment -- get the comment for an object, or null if not found.
 */
char* GetComment(Oid oid, Oid classoid, int32 subid)
{
    Relation description;
    ScanKeyData skey[3];
    SysScanDesc sd;
    TupleDesc tupdesc;
    HeapTuple tuple;
    char* comment = NULL;

    /* Use the index to search for a matching old tuple */
    ScanKeyInit(&skey[0], Anum_pg_description_objoid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(oid));
    ScanKeyInit(&skey[1], Anum_pg_description_classoid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(classoid));
    ScanKeyInit(&skey[2], Anum_pg_description_objsubid, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(subid));

    description = heap_open(DescriptionRelationId, AccessShareLock);
    tupdesc = RelationGetDescr(description);

    sd = systable_beginscan(description, DescriptionObjIndexId, true, NULL, 3, skey);

    comment = NULL;
    if ((tuple = systable_getnext(sd)) != NULL) {
        Datum value;
        bool isnull = false;

        /* Found the tuple, get description field */
        value = tableam_tops_tuple_getattr(tuple, Anum_pg_description_description, tupdesc, &isnull);
        if (!isnull)
            comment = TextDatumGetCString(value);
    } /* Assume there can be only one match */

    systable_endscan(sd);

    /* Done */
    heap_close(description, AccessShareLock);

    return comment;
}

void CreateNonColumnComment(Oid oid, List *options, Oid objectType)
{
    ListCell *cell = NULL;
    foreach (cell, options) {
        void *pointer = lfirst(cell);
        if (IsA(pointer, CommentStmt)) {
            CommentStmt *commentStmt = (CommentStmt *)pointer;
            CreateComments(oid, objectType, 0, commentStmt->comment);
            break;
        }
    }
}