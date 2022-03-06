/* -------------------------------------------------------------------------
 *
 * gs_matview.cpp
 *    Routines to support inter-object dependencies.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * IDENTIFICATION
 *    src/backend/catalog/gs_matview.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/heap.h"
#include "catalog/namespace.h"
#include "catalog/gs_matview.h"
#include "catalog/gs_matview_dependency.h"
#include "catalog/objectaddress.h"
#include "catalog/dependency.h"
#include "commands/matview.h"
#include "commands/sec_rls_cmds.h"
#include "commands/tablecmds.h"
#include "executor/executor.h"
#include "gs_policy/gs_policy_masking.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/snapmgr.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/inval.h"

void create_matview_tuple(Oid matviewOid, Oid matmapid, bool isIncremental)
{
    errno_t rc;
    Relation matview_relation;
    HeapTuple gs_matview_htup;
    bool gs_matview_nulls[Natts_gs_matview];
    Datum gs_matview_values[Natts_gs_matview];

    matview_relation = heap_open(MatviewRelationId, RowExclusiveLock);

    rc = memset_s(gs_matview_values, sizeof(gs_matview_values), 0, sizeof(gs_matview_values));
    securec_check(rc, "\0", "\0");
    rc =  memset_s(gs_matview_nulls, sizeof(gs_matview_nulls), false, sizeof(gs_matview_nulls));
    securec_check(rc, "\0", "\0");

    gs_matview_values[Anum_gs_matview_matviewid - 1] = ObjectIdGetDatum(matviewOid);
    gs_matview_values[Anum_gs_matview_mapid - 1] = ObjectIdGetDatum(matmapid);
    gs_matview_values[Anum_gs_matview_ivm - 1] = BoolGetDatum(isIncremental);

    gs_matview_nulls[Anum_gs_matview_refreshtime - 1] = true;
    gs_matview_nulls[Anum_gs_matview_needrefresh - 1] = true;

    gs_matview_htup = heap_form_tuple(matview_relation->rd_att, gs_matview_values, gs_matview_nulls);

    /* Do the insertion */
    (void)simple_heap_insert(matview_relation, gs_matview_htup);

    CatalogUpdateIndexes(matview_relation, gs_matview_htup);

    /* Make the changes visible */
    CommandCounterIncrement();

    heap_close(matview_relation, NoLock);
    return;
}

void update_matview_tuple(Oid matviewOid, bool needrefresh, Datum curtime)
{
    HeapTuple tup;
    HeapTuple newtuple;
    TableScanDesc scan;
    Form_gs_matview matviewForm;
    Relation matview_relation;
    errno_t rc = 0;

    /* preapre tuple for update. */
    Datum values[Natts_gs_matview];
    bool nulls[Natts_gs_matview];
    bool replaces[Natts_gs_matview];

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check_c(rc, "\0", "\0");
    rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
    securec_check_c(rc, "\0", "\0");

    /*
     * handle found.
     */
    matview_relation = heap_open(MatviewRelationId, RowExclusiveLock);
    scan = tableam_scan_begin(matview_relation, SnapshotNow, 0, NULL); /* for Q2 but same reason of Q1 */
    tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);

    while(tup != NULL) {
        matviewForm = (Form_gs_matview)GETSTRUCT(tup);

        if (matviewForm->matviewid == matviewOid) {
            /* ok found tuple */
            break;
        }
        tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);
    }

    if (tup == NULL || !HeapTupleIsValid(tup)) {
        heap_endscan(scan);
        heap_close(matview_relation, NoLock);
        return;
    }

    /* here update/modify tuple. */
    if (needrefresh) {
        values[Anum_gs_matview_refreshtime - 1] = curtime;
        nulls[Anum_gs_matview_refreshtime - 1] = false;
        replaces[Anum_gs_matview_refreshtime - 1] = true;
    }

    newtuple = heap_modify_tuple(tup, RelationGetDescr(matview_relation),
                                values, nulls, replaces);

    simple_heap_update(matview_relation, &newtuple->t_self, newtuple);

    CatalogUpdateIndexes(matview_relation, newtuple);

    /* Make the changes visible */
    CommandCounterIncrement();

    tableam_scan_end(scan);
    heap_close(matview_relation, NoLock);

    return;
}

void delete_matview_tuple(Oid matviewOid)
{
    Oid matmapid = InvalidOid;
    Relation relation = NULL;
    TableScanDesc scan;
    Form_gs_matview matviewForm;
    HeapTuple tup = NULL;

    relation = heap_open(MatviewRelationId, RowExclusiveLock);
    scan = tableam_scan_begin(relation, SnapshotNow, 0, NULL);
    tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);

    while(tup != NULL) {
        matviewForm = (Form_gs_matview)GETSTRUCT(tup);

        if (matviewForm->matviewid == matviewOid) {
            /* ok found tuple */
            matmapid = matviewForm->mapid;
            break;
        }
        tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);
    }

    if (HeapTupleIsValid(tup)) {
        simple_heap_delete(relation, &tup->t_self);

        /* clear up correlative matmap-table */
        if (HeapTupleIsValid(ScanPgRelation(matmapid, true, false))) {
            ObjectAddress matmapobject;

            matmapobject.classId = RelationRelationId;
            matmapobject.objectId = matmapid;
            matmapobject.objectSubId = 0;

            performDeletion(&matmapobject, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);
        } else {
            ereport(DEBUG2,
                    (errmodule(MOD_OPT), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Matviewmap %d relation is invalid when delete it.", (int)matmapid)));
        }
    }

    tableam_scan_end(scan);
    heap_close(relation, NoLock);

    /* Make the changes visible */
    CommandCounterIncrement();

    return;
}

void insert_matviewdep_tuple(Oid matviewOid, Oid relid, Oid mlogid)
{
    errno_t     rc;
    Relation materview_dep;
    HeapTuple   matview_dep_htup;
    bool gs_matviewdep_nulls[Natts_gs_matview_dependency];
    Datum gs_matviewdep_values[Natts_gs_matview_dependency];

    materview_dep = heap_open(MatviewDependencyId, RowExclusiveLock);

    rc = memset_s(gs_matviewdep_values, sizeof(gs_matviewdep_values), 0, sizeof(gs_matviewdep_values));
    securec_check(rc, "\0", "\0");
    rc =  memset_s(gs_matviewdep_nulls, sizeof(gs_matviewdep_nulls), false, sizeof(gs_matviewdep_nulls));
    securec_check(rc, "\0", "\0");

    /* fill up tuple with input. */
    gs_matviewdep_values[Anum_gs_matview_dep_matviewid - 1] = ObjectIdGetDatum(matviewOid);
    gs_matviewdep_values[Anum_gs_matview_dep_relid - 1] = ObjectIdGetDatum(relid);
    gs_matviewdep_values[Anum_gs_matview_dep_mlogid - 1] = ObjectIdGetDatum(mlogid);

    gs_matviewdep_nulls[Anum_gs_matview_dep_mxmin - 1] = true;

    matview_dep_htup = heap_form_tuple(materview_dep->rd_att, gs_matviewdep_values, gs_matviewdep_nulls);

    /* Do the insertion */
    (void)simple_heap_insert(materview_dep, matview_dep_htup);

    CatalogUpdateIndexes(materview_dep, matview_dep_htup);

    heap_freetuple(matview_dep_htup);
    heap_close(materview_dep, NoLock);

    return;
}

static void
try_delete_mlog_table(Relation matviewdep, Oid mlogid)
{
    TableScanDesc scan;
    HeapTuple tup = NULL;
    ScanKeyData scanKey;
    int refCount = 0;
    Oid relid = InvalidOid;

    if (!HeapTupleIsValid(ScanPgRelation(mlogid, true, false))) {
        return;
    }

    ScanKeyInit(&scanKey,
           Anum_gs_matview_dep_mlogid,
           BTEqualStrategyNumber,
           F_OIDEQ,
           ObjectIdGetDatum(mlogid));
    scan = tableam_scan_begin(matviewdep, SnapshotNow, 1, &scanKey);

    while ((tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        refCount++;

        if (refCount > 1) {
            break;
        }
        Form_gs_matview_dependency dep = (Form_gs_matview_dependency)GETSTRUCT(tup);
        relid = dep->relid;
    }

    if (refCount == 1) {
        ObjectAddress mlogobject;
        mlogobject.classId = RelationRelationId;
        mlogobject.objectId = mlogid;
        mlogobject.objectSubId = 0;
        performDeletion(&mlogobject, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);
    }

    tableam_scan_end(scan);

    if (relid != InvalidOid && refCount == 1) {
        CacheInvalidateRelcacheByRelid(relid);
    }

    return;
}

void delete_matviewdep_tuple(Oid matviewOid)
{
    Relation relation = NULL;
    TableScanDesc scan;
    ScanKeyData scanKey;
    Form_gs_matview_dependency matviewDepForm;
    HeapTuple tup = NULL;

    ScanKeyInit(&scanKey,
            Anum_gs_matview_dep_matviewid,
            BTEqualStrategyNumber,
            F_OIDEQ,
            ObjectIdGetDatum(matviewOid));
    relation = heap_open(MatviewDependencyId, RowExclusiveLock);
    scan = tableam_scan_begin(relation, SnapshotNow, 1, &scanKey);

    while((tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        matviewDepForm = (Form_gs_matview_dependency)GETSTRUCT(tup);
        try_delete_mlog_table(relation, matviewDepForm->mlogid);
        simple_heap_delete(relation, &tup->t_self);
    }

    tableam_scan_end(scan);
    heap_close(relation, NoLock);

    CommandCounterIncrement();

    return;
}

/*
 * Delete tables related to mlog-table
 */
void delete_matdep_table(Oid mlogid)
{
    Relation relation = NULL;
    TableScanDesc scan;
    ScanKeyData scanKey;
    Form_gs_matview_dependency matviewDepForm;
    HeapTuple tup = NULL;

    ScanKeyInit(&scanKey,
            Anum_gs_matview_dep_mlogid,
            BTEqualStrategyNumber,
            F_OIDEQ,
            ObjectIdGetDatum(mlogid));
    relation = heap_open(MatviewDependencyId, RowExclusiveLock);
    scan = tableam_scan_begin(relation, SnapshotNow, 1, &scanKey);

    while((tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        simple_heap_delete(relation, &tup->t_self);
        try_delete_mlog_table(relation, mlogid);
        matviewDepForm = (Form_gs_matview_dependency)GETSTRUCT(tup);
        delete_matview_tuple(matviewDepForm->matviewid);
    }

    tableam_scan_end(scan);
    heap_close(relation, NoLock);

    /* Make the changes visible */
    CommandCounterIncrement();

    return;
}

Datum get_matview_refreshtime(Oid matviewOid, bool *isNULL)
{
    Relation relation = NULL;
    TableScanDesc scan;
    HeapTuple tuple = NULL;
    Datum refreshTime = 0;
    Datum matviewid;

    relation = heap_open(MatviewRelationId, AccessShareLock);
    scan = tableam_scan_begin(relation, SnapshotNow, 0, NULL);

    while ((tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        matviewid = heap_getattr(tuple,
                            Anum_gs_matview_matviewid,
                            RelationGetDescr(relation),
                            isNULL);
        Assert(!(*isNULL));

        if (matviewid == matviewOid) {
            refreshTime = heap_getattr(tuple,
                            Anum_gs_matview_refreshtime,
                            RelationGetDescr(relation),
                            isNULL);
            break;
        }
    }

    tableam_scan_end(scan);
    heap_close(relation, NoLock);
    return refreshTime;
}

Datum get_matview_mapid(Oid matviewOid)
{
    Relation relation = NULL;
    TableScanDesc scan;
    HeapTuple tuple = NULL;
    Datum mapid = 0;
    Datum matviewid;
    bool isNULL = false;

    relation = heap_open(MatviewRelationId, AccessShareLock);
    scan = tableam_scan_begin(relation, SnapshotNow, 0, NULL);  /* Q1 use SnapshotAny? */

    while ((tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        matviewid = heap_getattr(tuple,
                            Anum_gs_matview_matviewid,
                            RelationGetDescr(relation),
                            &isNULL);
        Assert(!isNULL);

        if (matviewid == matviewOid) {
            mapid = heap_getattr(tuple,
                            Anum_gs_matview_mapid,
                            RelationGetDescr(relation),
                            &isNULL);
            break;
        }
    }

    tableam_scan_end(scan);
    heap_close(relation, NoLock);
    return mapid;
}

bool is_incremental_matview(Oid oid)
{
    Relation relation = NULL;
    TableScanDesc scan;
    HeapTuple tuple = NULL;
    bool isNull = false;
    Datum ivm = 0;
    Datum matviewid;

    relation = heap_open(MatviewRelationId, AccessShareLock);
    scan = tableam_scan_begin(relation, SnapshotNow, 0, NULL);

    while ((tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        matviewid = heap_getattr(tuple,
                            Anum_gs_matview_matviewid,
                            RelationGetDescr(relation),
                            &isNull);
        Assert(!isNull);

        if (matviewid == oid) {
            ivm = heap_getattr(tuple,
                            Anum_gs_matview_ivm,
                            RelationGetDescr(relation),
                            &isNull);
            break;
        }
    }

    tableam_scan_end(scan);
    heap_close(relation, NoLock);

    return DatumGetBool(ivm);
}

/*
 * Check if it's a map table or mlog table by relOid
 */
bool IsMatviewRelationbyOid(Oid relOid, MvRelationType *matviewRelationType)
{
    /* MATVIEW_NOT means it is an oridinary table */
    *matviewRelationType = MATVIEW_NOT;

    Relation rel = relation_open(relOid, AccessShareLock);
    if (ISMATMAP(rel->rd_rel->relname.data)) {
        *matviewRelationType = MATVIEW_MAP;
    } else if (ISMLOG(rel->rd_rel->relname.data)) {
        *matviewRelationType = MATVIEW_LOG;
    }
    relation_close(rel, AccessShareLock);

    return *matviewRelationType != MATVIEW_NOT;
}

/*
 * If it's a map table, get the matview oid; else if it's a mlog table, get the basetable oid
 */
Oid MatviewRelationGetBaseid(Oid relOid, MvRelationType matviewRelationType)
{
    if (matviewRelationType == MATVIEW_NOT) {
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("Not a matview related table")));
    }

    int mvHeadLen;
    Relation rel;
    char* tableOidStr = NULL;
    Oid tableOid;

    mvHeadLen = (matviewRelationType == MATVIEW_MAP) ? MATMAPLEN : MLOGLEN;
    rel = relation_open(relOid, AccessShareLock);
    tableOidStr = TextDatumGetCString(
        DirectFunctionCall2(text_substr_no_len_orclcompat,
            CStringGetTextDatum(RelationGetRelationName(rel)), Int32GetDatum(mvHeadLen + 1)));
    relation_close(rel, AccessShareLock);

    tableOid = DatumGetInt32(DirectFunctionCall1(int4in, CStringGetDatum(tableOidStr)));
    return tableOid;
}

/*
 * get_matview_query - get the Query from a matview's _RETURN rule.
 */
Query *get_matview_query(Relation matviewRel)
{
    RewriteRule *rule = NULL;
    List *actions = NIL;
    /*
     * Check that everything is correct for a refresh. Problems at this point
     * are internal errors, so elog is sufficient.
     */
    if (matviewRel->rd_rel->relhasrules == false ||
        matviewRel->rd_rules->numLocks < 1)
        elog(ERROR,
             "materialized view \"%s\" is missing rewrite information",
             RelationGetRelationName(matviewRel));
    if (matviewRel->rd_rules->numLocks > 1)
        elog(ERROR,
             "materialized view \"%s\" has too many rules",
             RelationGetRelationName(matviewRel));
    rule = matviewRel->rd_rules->rules[0];
    if (rule->event != CMD_SELECT || !(rule->isInstead))
        elog(ERROR,
             "the rule for materialized view \"%s\" is not a SELECT INSTEAD OF rule",
             RelationGetRelationName(matviewRel));
    actions = rule->actions;
    if (list_length(actions) != 1)
        elog(ERROR,
             "the rule for materialized view \"%s\" is not a single action",
             RelationGetRelationName(matviewRel));
    /*
     * The stored query was rewritten at the time of the MV definition, but
     * has not been scribbled on by the planner.
     */
    return (Query *)linitial(actions);
}

/*
 * Check if matview query contains quals
 */
bool CheckMatviewQuals(Query *query)
{
    ListCell *lc = NULL;

    if (query->setOperations == NULL) {
        if (query->jointree->quals != NULL) {
            return true;
        } else {
            return false;
        }
    }

    foreach (lc, query->rtable) {
        RangeTblEntry *rte = (RangeTblEntry *)lfirst(lc);
        Query *subquery = rte->subquery;

        if (subquery != NULL && subquery->jointree->quals != NULL) {
            return true;
        }
    }

    return false;
}

/*
 * Check if the matview has privilege to refresh, for advanced and basic privilege
 */
void CheckRefreshMatview(Relation matviewRel, bool isIncremental)
{
    Assert(!IsSystemRelation(matviewRel));
    Assert(!matviewRel->rd_rel->relhasoids);
    Query *query = get_matview_query(matviewRel);
    Assert(IsA(query, Query));
    /* Check RLS/sensitive policy/priviate user */
    check_basetable(query, false, isIncremental);
    /* Check select privilege */
    (void)ExecCheckRTPerms(query->rtable, true);

    return;
}

/*
 * Check if the basetable has privilege to execute select
 */
bool CheckPermissionForBasetable(const RangeTblEntry *rte)
{
    Oid relid = rte->relid;
    if (is_masked_relation(relid)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Permission denied cause sensitive policy on basetable")));
    }
    if (RelationHasRlspolicy(relid)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Permission denied cause RLS on basetable")));
    }
    if (is_role_independent(FindRoleid(relid))) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Permission denied from an independent role")));
    }

    return true;
}

/*
 * Get owner by relid
 */
Oid FindRoleid(Oid relid)
{
    Oid roleid;
    HeapTuple tuple = NULL;
    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("relation with OID %u does not exist", relid)));
    roleid = ((Form_pg_class)GETSTRUCT(tuple))->relowner;
    ReleaseSysCache(tuple);
    return roleid;
}

/*
 * acquire locks of matview's tables
 */
void acquire_mativew_tables_lock(Query *query, bool incremental)
{
    Relation rel;
    ListCell *lc = NULL;
    List *relids = NIL;
    LOCKMODE lockmode = incremental ?  ShareUpdateExclusiveLock : ExclusiveLock;

    relids = pull_up_rels_recursive((Node *)query);
    foreach (lc, relids) {
        Oid relid = (Oid)lfirst_oid(lc);
        rel = heap_open(relid, lockmode);
        heap_close(rel, NoLock);
    }

    return;
}
