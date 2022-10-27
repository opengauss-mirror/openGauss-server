/* -------------------------------------------------------------------------
 *
 * rewriteSupport.cpp
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/rewrite/rewriteSupport.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "securec_check.h"
#include "knl/knl_variable.h"
#include "access/sysattr.h"
#include "access/heapam.h"
#include "access/tableam.h"
#include "catalog/indexing.h"
#include "catalog/pg_rewrite.h"
#include "rewrite/rewriteSupport.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"

/*
 * Is there a rule by the given name?
 */
bool IsDefinedRewriteRule(Oid owningRel, const char* ruleName)
{
    return SearchSysCacheExists2(RULERELNAME, ObjectIdGetDatum(owningRel), PointerGetDatum(ruleName));
}

/*
 * SetRelationRuleStatus
 *		Set the value of the relation's relhasrules field in pg_class;
 *		if the relation is becoming a view, also adjust its relkind.
 *
 * NOTE: caller must be holding an appropriate lock on the relation.
 *
 * NOTE: an important side-effect of this operation is that an SI invalidation
 * message is sent out to all backends --- including me --- causing relcache
 * entries to be flushed or updated with the new set of rules for the table.
 * This must happen even if we find that no change is needed in the pg_class
 * row.
 */
void SetRelationRuleStatus(Oid relationId, bool relHasRules, bool relIsBecomingView)
{
    Relation relationRelation;
    HeapTuple tuple;
    Form_pg_class classForm;

    /*
     * Find the tuple to update in pg_class, using syscache for the lookup.
     */
    relationRelation = heap_open(RelationRelationId, RowExclusiveLock);
    tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relationId));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR,
            (errmodule(MOD_OPT_REWRITE),
                errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for relation %u", relationId)));
    classForm = (Form_pg_class)GETSTRUCT(tuple);

    if (classForm->relhasrules != relHasRules || (relIsBecomingView && classForm->relkind != RELKIND_VIEW
        && classForm->relkind != RELKIND_CONTQUERY)) {
        /* Do the update */
        classForm->relhasrules = relHasRules;
        if (relIsBecomingView)
            classForm->relkind = RELKIND_VIEW;

        simple_heap_update(relationRelation, &tuple->t_self, tuple);

        /* Keep the catalog indexes up to date */
        CatalogUpdateIndexes(relationRelation, tuple);
    } else {
        /* no need to change tuple, but force relcache rebuild anyway */
        CacheInvalidateRelcacheByTuple(tuple);
    }

    tableam_tops_free_tuple(tuple);
    heap_close(relationRelation, RowExclusiveLock);
}

/*
 * Find rule oid.
 *
 * If missing_ok is false, throw an error if rule name not found.  If
 * true, just return InvalidOid.
 */
Oid get_rewrite_oid(Oid relid, const char* rulename, bool missing_ok)
{
    HeapTuple tuple;
    Oid ruleoid;

    /* Find the rule's pg_rewrite tuple, get its OID */
    tuple = SearchSysCache2(RULERELNAME, ObjectIdGetDatum(relid), PointerGetDatum(rulename));
    if (!HeapTupleIsValid(tuple)) {
        if (missing_ok)
            return InvalidOid;
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("rule \"%s\" for relation \"%s\" does not exist", rulename, get_rel_name(relid))));
    }
    AssertEreport(relid == ((Form_pg_rewrite)GETSTRUCT(tuple))->ev_class, MOD_OPT, "");
    ruleoid = HeapTupleGetOid(tuple);
    ReleaseSysCache(tuple);
    return ruleoid;
}

char* get_rewrite_rulename(Oid ruleid, bool missing_ok)
{
    ScanKeyData entry;
    SysScanDesc scan;
    HeapTuple rewrite_tup;
    char* rulename = NULL;
    errno_t rc;
    Relation rewrite_rel = heap_open(RewriteRelationId, AccessShareLock);
    ScanKeyInit(&entry, ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(ruleid));
    scan = systable_beginscan(rewrite_rel, RewriteOidIndexId, true, NULL, 1, &entry);
    rewrite_tup = systable_getnext(scan);
    if (!HeapTupleIsValid(rewrite_tup)) {
        if (missing_ok) {
            heap_close(rewrite_rel, AccessShareLock);
            return NULL;
        }
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("rule \"%u\" does not exist", ruleid)));
    }
    Form_pg_rewrite pg_rewrite = (Form_pg_rewrite)GETSTRUCT(rewrite_tup);
    rulename = (char*)palloc0(NAMEDATALEN);
    rc = strncpy_s(rulename, NAMEDATALEN, NameStr(pg_rewrite->rulename), NAMEDATALEN - 1);
    securec_check_c(rc, "\0", "\0");
    systable_endscan(scan);

    heap_close(rewrite_rel, AccessShareLock);
    return rulename;   
}

/*
 * input parameter relid is ev_class, ev_type is pg_rewrite->ev_type, CMD_UTILITY means notify, copy, alter rule,
 * latter two is only used in timeseries table redistribution
 * ev_type is CmdType, transfer it to char beacuse it is char in system catalog pg_rewrite
 */
bool rel_has_rule(Oid relid, char ev_type)
{
    bool has_rule = false;
    ScanKeyData entry;
    SysScanDesc scan;
    HeapTuple rewrite_tup;
    Relation rewrite_rel = heap_open(RewriteRelationId, AccessShareLock);
    ScanKeyInit(&entry, Anum_pg_rewrite_ev_class, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));
    scan = systable_beginscan(rewrite_rel, RewriteRelRulenameIndexId, true, NULL, 1, &entry);
    while (HeapTupleIsValid((rewrite_tup = systable_getnext(scan)))) {
        Form_pg_rewrite pg_rewrite = (Form_pg_rewrite)GETSTRUCT(rewrite_tup);
        if (pg_rewrite->ev_type == ev_type) {            
            has_rule = true;
            break;
        }
    }    
    systable_endscan(scan);
    heap_close(rewrite_rel, AccessShareLock);
    return has_rule;   
}

/*
 * Find rule oid, given only a rule name but no rel OID.
 *
 * If there's more than one, it's an error.  If there aren't any, that's an
 * error, too.	In general, this should be avoided - it is provided to support
 * syntax that is compatible with pre-7.3 versions of PG, where rule names
 * were unique across the entire database.
 */
Oid get_rewrite_oid_without_relid(const char* rulename, Oid* reloid, bool missing_ok)
{
    Relation RewriteRelation;
    TableScanDesc scanDesc;
    ScanKeyData scanKeyData;
    HeapTuple htup;
    Oid ruleoid;

    /* Search pg_rewrite for such a rule */
    ScanKeyInit(&scanKeyData, Anum_pg_rewrite_rulename, BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(rulename));

    RewriteRelation = heap_open(RewriteRelationId, AccessShareLock);
    scanDesc = tableam_scan_begin(RewriteRelation, SnapshotNow, 1, &scanKeyData);

    htup = (HeapTuple) tableam_scan_getnexttuple(scanDesc, ForwardScanDirection);
    if (!HeapTupleIsValid(htup)) {
        if (!missing_ok)
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("rule \"%s\" does not exist", rulename)));
        ruleoid = InvalidOid;
    } else {
        ruleoid = HeapTupleGetOid(htup);
        if (reloid != NULL)
            *reloid = ((Form_pg_rewrite)GETSTRUCT(htup))->ev_class;

        htup = (HeapTuple) tableam_scan_getnexttuple(scanDesc, ForwardScanDirection);
        if (HeapTupleIsValid(htup))
            ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_OBJECT),
                    errmsg("there are multiple rules named \"%s\"", rulename),
                    errhint("Specify a relation name as well as a rule name.")));
    }
    tableam_scan_end(scanDesc);
    heap_close(RewriteRelation, AccessShareLock);

    return ruleoid;
}

Oid get_rewrite_relid(Oid ruleid, bool missing_ok)
{
    Oid relid;
    ScanKeyData entry;
    SysScanDesc scan;
    HeapTuple rewrite_tup;
    Relation rewrite_rel = heap_open(RewriteRelationId, AccessShareLock);

    ScanKeyInit(&entry, ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(ruleid));
    scan = systable_beginscan(rewrite_rel, RewriteOidIndexId, true, NULL, 1, &entry);
    rewrite_tup = systable_getnext(scan);
    if (!HeapTupleIsValid(rewrite_tup)) {
        if (missing_ok) {
            systable_endscan(scan);
            heap_close(rewrite_rel, AccessShareLock);
            return InvalidOid;
        }
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("rule \"%u\" does not exist", ruleid)));
    }
    Form_pg_rewrite pg_rewrite = (Form_pg_rewrite)GETSTRUCT(rewrite_tup);
    relid = pg_rewrite->ev_class;
    systable_endscan(scan);
    heap_close(rewrite_rel, AccessShareLock);
    return relid;
}