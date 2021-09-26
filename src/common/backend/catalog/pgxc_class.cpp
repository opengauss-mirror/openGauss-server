/* -------------------------------------------------------------------------
 *
 * pgxc_class.cpp
 *	routines to support manipulation of the pgxc_class relation
 *
 * Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/heapam.h"
#include "access/relscan.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "catalog/pgxc_class.h"
#include "catalog/pgxc_group.h"
#include "optimizer/planner.h"
#include "pgxc/locator.h"
#include "pgxc/pgxc.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"

/*
 * PgxcClassCreate
 *		Create a pgxc_class entry
 */
static void PgxcClassCreateInner(bool *nulls, Datum *values)
{
    Relation pgxcclassrel;
    HeapTuple htup;
    /* Open the relation for insertion */
    pgxcclassrel = heap_open(PgxcClassRelationId, RowExclusiveLock);

    htup = heap_form_tuple(pgxcclassrel->rd_att, values, nulls);

    (void)simple_heap_insert(pgxcclassrel, htup);

    CatalogUpdateIndexes(pgxcclassrel, htup);
    heap_freetuple_ext(htup);

    heap_close(pgxcclassrel, RowExclusiveLock);
}

/*
 * PgxcClassCreate
 *		Create a pgxc_class entry
 */
void PgxcClassCreate(Oid pcrelid, char pclocatortype, int2* pcattnum, int pchashalgorithm, int pchashbuckets,
    int numnodes, Oid* nodes, int distributeNum, const char* groupname)
{
    bool nulls[Natts_pgxc_class];
    Datum values[Natts_pgxc_class];
    int i;
    oidvector* nodes_array = NULL;
    int2vector* attnum_array = NULL;
    Relation pgxcgrouprel;
    HeapTuple ghtup;
    TableScanDesc scan;
    Form_pgxc_group groupForm;
    int scan_group;
    oidvector* gmember = NULL;
    Datum gmember_datum = 0;
    bool isNull = false;

    /* Build array of Oids to be inserted */
    attnum_array = buildint2vector(pcattnum, distributeNum);

    /* Iterate through attributes initializing nulls and values */
    for (i = 0; i < Natts_pgxc_class; i++) {
        nulls[i] = false;
        values[i] = (Datum)0;
    }

    /* Reserve column */
    nulls[Anum_pgxc_class_option - 1] = true;

    /* should not happen */
    if (pcrelid == InvalidOid) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("pgxc class relid invalid.")));
        return;
    }

    pgxcgrouprel = heap_open(PgxcGroupRelationId, ShareLock);
    scan = heap_beginscan(pgxcgrouprel, SnapshotNow, 0, NULL);
    scan_group = 1;

    while (scan_group == 1) {
        ghtup = heap_getnext(scan, ForwardScanDirection);
        /* pgxc_group should not be empty, one default group record should be inserted when catalog is created */
        if (ghtup == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_DATA_EXCEPTION), errmsg("pgxc_group should have at least one default node group")));
            // create a node group record or cluster manager needs to create one after system starts
            values[Anum_pgxc_class_pgroup - 1] =
                DirectFunctionCall1(namein, CStringGetDatum(""));  // empty group name for relation
            break;
        } else {
            groupForm = (Form_pgxc_group)GETSTRUCT(ghtup);

            /* Continue until finding target node group */
            if (strcmp(groupname, groupForm->group_name.data) != 0) {
                continue;
            }

            gmember_datum = heap_getattr(ghtup, Anum_pgxc_group_members, RelationGetDescr(pgxcgrouprel), &isNull);
            gmember = (oidvector*)PG_DETOAST_DATUM(gmember_datum);

            // we should compare nodes_array with group_members of each node group record,
            // then choose the matched one if we support subcluster clause and multiple node groups some day.
            if (isRestoreMode) {
                if (numnodes == gmember->dim1) {
                    values[Anum_pgxc_class_pgroup - 1] = DirectFunctionCall1(namein, CStringGetDatum(groupname));
                    nodes_array = buildoidvector(gmember->values, gmember->dim1);
                    scan_group = 0;
                }
            } else {
                values[Anum_pgxc_class_pgroup - 1] = DirectFunctionCall1(namein, CStringGetDatum(groupname));
                nodes_array = buildoidvector(gmember->values, gmember->dim1);
                scan_group = 0;
            }

            if (gmember != (oidvector*)DatumGetPointer(gmember_datum))
                pfree_ext(gmember);

            /*
             * We have found target nodegroup and build node oidvector so just done with
             * the loop
             */
            break;
        }
    }
    heap_endscan(scan);
    heap_close(pgxcgrouprel, ShareLock);

    values[Anum_pgxc_class_redistributed - 1] =
        CharGetDatum('n');                           // newly created table is always not in re-distribution
    values[Anum_pgxc_class_redis_order - 1] = 1024;  // default value
    values[Anum_pgxc_class_pcrelid - 1] = ObjectIdGetDatum(pcrelid);
    values[Anum_pgxc_class_pclocatortype - 1] = CharGetDatum(pclocatortype);

    if (pclocatortype == LOCATOR_TYPE_HASH || pclocatortype == LOCATOR_TYPE_MODULO) {
        values[Anum_pgxc_class_pchashalgorithm - 1] = UInt16GetDatum(pchashalgorithm);
        values[Anum_pgxc_class_pchashbuckets - 1] = UInt16GetDatum(pchashbuckets);
    }

    /* Node information */
    values[Anum_pgxc_class_pcattnum - 1] = PointerGetDatum(attnum_array);
    values[Anum_pgxc_class_nodes - 1] = PointerGetDatum(nodes_array);
    PgxcClassCreateInner(nulls, values);
}

/*
 * PgxcClassCreateForReloption
 *      Create a pgxc_class entry, only for merge list
 */
void PgxcClassCreateForReloption(Oid pcrelid, const char* reloptionstr)
{
    bool nulls[Natts_pgxc_class];
    Datum values[Natts_pgxc_class];
    ObjectAddress myself, referenced;
    int i;

    /* Iterate through attributes initializing nulls and values */
    for (i = 0; i < Natts_pgxc_class; i++) {
        nulls[i] = true;
        values[i] = (Datum)0;
    }

    /* should not happen */
    if (pcrelid == InvalidOid) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("pgxc class relid invalid.")));
        return;
    }
    values[Anum_pgxc_class_pcrelid - 1] = ObjectIdGetDatum(pcrelid);
    values[Anum_pgxc_class_option - 1] = CStringGetTextDatum(reloptionstr);
    nulls[Anum_pgxc_class_pcrelid - 1] = false;
    nulls[Anum_pgxc_class_option - 1] = false;
    PgxcClassCreateInner(nulls, values);
    
    Oid relid = partid_get_parentid(pcrelid);
    /* Make dependency entries */
    myself.classId = PgxcClassRelationId;
    myself.objectId = pcrelid;
    myself.objectSubId = 0;

    /* Dependency on relation */
    referenced.classId = RelationRelationId;
    referenced.objectId = ((relid == InvalidOid) ? pcrelid : relid);
    referenced.objectSubId = 0;
    recordDependencyOn(&myself, &referenced, DEPENDENCY_INTERNAL);
}

/*
 * PgxcClassAlter
 *		Modify a pgxc_class entry with given data
 */
static void PgxcClassAlterInner(Relation rel, Oid pcrelid, bool *new_record_nulls, Datum *new_record, bool* new_record_repl)
{
    HeapTuple oldtup, newtup;
    Assert(RelationIsValid(rel));
    Assert(OidIsValid(pcrelid));
    oldtup = SearchSysCacheCopy1(PGXCCLASSRELID, ObjectIdGetDatum(pcrelid));

    /* Update relation */
    newtup = heap_modify_tuple(oldtup, RelationGetDescr(rel), new_record, new_record_nulls, new_record_repl);
    simple_heap_update(rel, &oldtup->t_self, newtup);
    CatalogUpdateIndexes(rel, newtup);
    heap_freetuple_ext(newtup);
    heap_freetuple_ext(oldtup);
}

/*
 * PgxcClassAlter
 *		Modify a pgxc_class entry with given data
 */
void PgxcClassAlter(Oid pcrelid, char pclocatortype, int2* pcattnum, int numpcattnum, int pchashalgorithm,
    int pchashbuckets, int numnodes, Oid* nodes, char ch_redis, PgxcClassAlterType type, const char* groupname)
{
    Relation rel;
    HeapTuple oldtup;
    oidvector* nodes_array = NULL;
    int2vector* pcattnum_array = NULL;
    Datum new_record[Natts_pgxc_class];
    bool new_record_nulls[Natts_pgxc_class];
    bool new_record_repl[Natts_pgxc_class];

    Assert(OidIsValid(pcrelid));

    rel = heap_open(PgxcClassRelationId, RowExclusiveLock);
    oldtup = SearchSysCacheCopy1(PGXCCLASSRELID, ObjectIdGetDatum(pcrelid));

    if (!HeapTupleIsValid(oldtup)) /* should not happen */
        ereport(
            ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for pgxc_class %u", pcrelid)));

    pcattnum_array = buildint2vector(pcattnum, numpcattnum);

    /* Initialize fields */
    errno_t errorno = EOK;
    errorno = memset_s(new_record, sizeof(new_record), 0, sizeof(new_record));
    securec_check(errorno, "\0", "\0");
    errorno = memset_s(new_record_nulls, sizeof(new_record_nulls), false, sizeof(new_record_nulls));
    securec_check(errorno, "\0", "\0");
    errorno = memset_s(new_record_repl, sizeof(new_record_repl), false, sizeof(new_record_repl));
    securec_check(errorno, "\0", "\0");

    /* Reserve column */
    new_record_nulls[Anum_pgxc_class_option - 1] = true;

    /* Fields are updated depending on operation type */
    switch (type) {
        case PGXC_CLASS_ALTER_DISTRIBUTION:
            new_record_repl[Anum_pgxc_class_pclocatortype - 1] = true;
            new_record_repl[Anum_pgxc_class_pcattnum - 1] = true;
            new_record_repl[Anum_pgxc_class_pchashalgorithm - 1] = true;
            new_record_repl[Anum_pgxc_class_pchashbuckets - 1] = true;
            break;
        case PGXC_CLASS_ALTER_NODES:
            new_record_repl[Anum_pgxc_class_nodes - 1] = true;
            new_record_repl[Anum_pgxc_class_redistributed - 1] = true;
            new_record_repl[Anum_pgxc_class_pgroup - 1] = true;
            break;
        case PGXC_CLASS_ALTER_ALL:
        default:
            new_record_repl[Anum_pgxc_class_pcrelid - 1] = true;
            new_record_repl[Anum_pgxc_class_pclocatortype - 1] = true;
            new_record_repl[Anum_pgxc_class_pcattnum - 1] = true;
            new_record_repl[Anum_pgxc_class_pchashalgorithm - 1] = true;
            new_record_repl[Anum_pgxc_class_pchashbuckets - 1] = true;
            new_record_repl[Anum_pgxc_class_nodes - 1] = true;
            break;
    }

    /* Set up new fields */
    /* Relation Oid */
    if (new_record_repl[Anum_pgxc_class_pcrelid - 1])
        new_record[Anum_pgxc_class_pcrelid - 1] = ObjectIdGetDatum(pcrelid);

    /* Locator type */
    if (new_record_repl[Anum_pgxc_class_pclocatortype - 1])
        new_record[Anum_pgxc_class_pclocatortype - 1] = CharGetDatum(pclocatortype);

    /* Attribute number of distribution column */
    if (new_record_repl[Anum_pgxc_class_pcattnum - 1])
        new_record[Anum_pgxc_class_pcattnum - 1] = PointerGetDatum(pcattnum_array);

    /* Hash algorithm type */
    if (new_record_repl[Anum_pgxc_class_pchashalgorithm - 1])
        new_record[Anum_pgxc_class_pchashalgorithm - 1] = UInt16GetDatum(pchashalgorithm);

    /* Hash buckets */
    if (new_record_repl[Anum_pgxc_class_pchashbuckets - 1])
        new_record[Anum_pgxc_class_pchashbuckets - 1] = UInt16GetDatum(pchashbuckets);

    if (groupname == NULL) {
        const char* installationgroupname = PgxcGroupGetInstallationGroup();
        if (installationgroupname == NULL) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("can't find installation-group.")));
        }

        /* check add node */
        Oid* members = NULL;
        int nmembers = get_pgxc_groupmembers(get_pgxc_groupoid(installationgroupname), &members);
        if (nmembers != numnodes) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("AlTER TABLE ADD/DELETE NODE can only be altered up-to installation group.")));
        }
        SortRelationDistributionNodes(members, nmembers);
        for (int i = 0; i < nmembers; i++) {
            if (!PgxcNodeCheckDnMatric(nodes[i], members[i])) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("AlTER TABLE ADD/DELETE NODE can only be altered up-to installation group.")));
            }
        }

        groupname = installationgroupname;
    }

    nodes_array = buildoidvector(nodes, numnodes);
    new_record[Anum_pgxc_class_redistributed - 1] = CharGetDatum(ch_redis);
    new_record[Anum_pgxc_class_pgroup - 1] = DirectFunctionCall1(namein, CStringGetDatum(groupname));
    new_record[Anum_pgxc_class_nodes - 1] = PointerGetDatum(nodes_array);

    PgxcClassAlterInner(rel, pcrelid, new_record_nulls, new_record, new_record_repl);
    pfree_ext(nodes_array);
    heap_freetuple_ext(oldtup);
    heap_close(rel, RowExclusiveLock);
}

/*
 * PgxcClassAlterForReloption
 *      Modify a pgxc_class entry with given data
 */
void PgxcClassAlterForReloption(Oid pcrelid, const char* reloptionstr)
{
    Relation rel;
    HeapTuple oldtup;
    Datum new_record[Natts_pgxc_class];
    bool new_record_nulls[Natts_pgxc_class];
    bool new_record_repl[Natts_pgxc_class];

    Assert(OidIsValid(pcrelid));

    rel = heap_open(PgxcClassRelationId, RowExclusiveLock);
    oldtup = SearchSysCacheCopy1(PGXCCLASSRELID, ObjectIdGetDatum(pcrelid));

    if (!HeapTupleIsValid(oldtup)) /* should not happen */
        ereport(
            ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for pgxc_class %u", pcrelid)));

    /* Initialize fields */
    errno_t errorno = EOK;
    errorno = memset_s(new_record, sizeof(new_record), 0, sizeof(new_record));
    securec_check(errorno, "\0", "\0");
    errorno = memset_s(new_record_nulls, sizeof(new_record_nulls), true, sizeof(new_record_nulls));
    securec_check(errorno, "\0", "\0");
    errorno = memset_s(new_record_repl, sizeof(new_record_repl), false, sizeof(new_record_repl));
    securec_check(errorno, "\0", "\0");

    /* Reserve column */
    new_record_nulls[Anum_pgxc_class_option - 1] = false;
    new_record_repl[Anum_pgxc_class_option - 1] = true;
    new_record[Anum_pgxc_class_option - 1] = CStringGetTextDatum(reloptionstr);
    PgxcClassAlterInner(rel, pcrelid, new_record_nulls, new_record, new_record_repl);
    heap_freetuple_ext(oldtup);
    heap_close(rel, RowExclusiveLock);
}

/*
 * RemovePGXCClass():
 *		Remove extended PGXC information
 */
void RemovePgxcClass(Oid pcrelid, bool canmiss)
{
    Relation relation;
    HeapTuple tup;

    /*
     * Delete the pgxc_class tuple.
     */
    relation = heap_open(PgxcClassRelationId, RowExclusiveLock);
    tup = SearchSysCache(PGXCCLASSRELID, ObjectIdGetDatum(pcrelid), 0, 0, 0);

    if (!HeapTupleIsValid(tup) && !canmiss) /* should not happen */
        ereport(
            ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for pgxc_class %u", pcrelid)));
    if (HeapTupleIsValid(tup)) {
        simple_heap_delete(relation, &tup->t_self);
        ReleaseSysCache(tup);
    }
    heap_close(relation, RowExclusiveLock);
}

/* @Online expansion:
 * Check if the relation is in process of redistribution
 * return true : the relation is in redistribution
 * return false: the relation is not in redistribution
 */
bool RelationRedisCheck(Relation rel)
{
    Relation pcrel = NULL;
    ScanKeyData skey;
    SysScanDesc pcscan = NULL;
    HeapTuple htup = NULL;
    Form_pgxc_class pgxc_class = NULL;
    char redistribute = 'n';
    bool isInRedis = false;

    ScanKeyInit(
        &skey, Anum_pgxc_class_pcrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(RelationGetRelid(rel)));

    pcrel = heap_open(PgxcClassRelationId, AccessShareLock);
    pcscan = systable_beginscan(pcrel, PgxcClassPgxcRelIdIndexId, true, NULL, 1, &skey);
    htup = systable_getnext(pcscan);
    if (!HeapTupleIsValid(htup)) {
        /* Assume local relation only */
        systable_endscan(pcscan);
        heap_close(pcrel, AccessShareLock);
        return isInRedis;
    }

    pgxc_class = (Form_pgxc_class)GETSTRUCT(htup);

    redistribute = pgxc_class->redistributed;

    if (redistribute == 'i') {
        isInRedis = true;
    }

    systable_endscan(pcscan);
    heap_close(pcrel, AccessShareLock);
    return isInRedis;
}
