/* -------------------------------------------------------------------------
 *
 * trigger.cpp
 *	  PostgreSQL TRIGGERs support code.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/commands/trigger.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "access/tableam.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/gs_db_privilege.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_object.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "commands/proclang.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "executor/node/nodeModifyTable.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "nodes/bitmapset.h"
#include "nodes/makefuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/var.h"
#include "parser/parse_clause.h"
#include "parser/parse_collate.h"
#include "parser/parse_func.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "pgstat.h"
#include "rewrite/rewriteManip.h"
#include "storage/buf/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/tcap.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/bytea.h"
#ifdef PGXC
#include "utils/datum.h"
#endif
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "access/heapam.h"
#include "access/tableam.h"
#ifdef PGXC
#include "pgxc/pgxc.h"
#include "optimizer/pgxcship.h"
#endif
#include "utils/knl_relcache.h"

/*
 * Note that similar macros also exist in executor/execMain.c.  There does not
 * appear to be any good header to put them into, given the structures that
 * they use, so we let them be duplicated.  Be sure to update all if one needs
 * to be changed, however.
 */
#define GET_ALL_UPDATED_COLUMNS(relinfo, estate)                                     \
    (bms_union(exec_rt_fetch((relinfo)->ri_RangeTableIndex, estate)->updatedCols, \
        exec_rt_fetch((relinfo)->ri_RangeTableIndex, estate)->extraUpdatedCols))

#define B_TRIGGER_INLINE_STR "inlinefunc"
#define B_TRIGGER_INLINE_LENGTH 10
#define B_TRIGGER_ID_LENGTH 8 /* for 65535 max = 5 chars, 3 '_' length total 8 */
#define B_TRIGGER_ID_MAX 65530 /* max id search time we will report error */

/* Local function prototypes */
static void ConvertTriggerToFK(CreateTrigStmt* stmt, Oid funcoid);
static void SetTriggerFlags(TriggerDesc* trigdesc, const Trigger* trigger);
static void ReleaseFakeRelation(Relation relation, Partition part, Relation* fakeRelation);
static bool TriggerEnabled(EState* estate, ResultRelInfo* relinfo, Trigger* trigger, TriggerEvent event,
    const Bitmapset* modifiedCols, HeapTuple oldtup, HeapTuple newtup);
static HeapTuple ExecCallTriggerFunc(
    TriggerData* trigdata, int tgindx, FmgrInfo* finfo, Instrumentation* instr, MemoryContext per_tuple_context);
static void AfterTriggerSaveEvent(EState* estate, ResultRelInfo* relinfo, uint32 event, bool row_trigger,
    Oid oldPartitionOid, Oid newPartitionOid, int2 bucketid, HeapTuple oldtup, HeapTuple newtup, List* recheckIndexes,
    Bitmapset* modifiedCols);
static char* rebuild_funcname_for_b_trigger(char* trigname, char* relname);
#ifdef PGXC
static bool pgxc_should_exec_triggers(
    Relation rel, uint16 tgtype_event, int16 tgtype_level, int16 tgtype_timing, EState* estate = NULL);
static bool pgxc_is_trigger_firable(Relation rel, Trigger* trigger, bool exec_all_triggers);
static bool pgxc_is_internal_trig_firable(Relation rel, const Trigger* trigger);
static HeapTuple pgxc_get_trigger_tuple(HeapTupleHeader tuphead);
static bool pgxc_should_exec_trigship(Relation rel, const EState* estate);
#endif
#ifdef ENABLE_MULTIPLE_NODES
inline bool IsReplicatedRelationWithoutPK(Relation rel, AttrNumber* indexed_col, int16 index_col_count)
{
    return IsRelationReplicated(RelationGetLocInfo(rel)) && (indexed_col == NULL || index_col_count == 0);
}
#endif

extern HeapTuple SearchUserHostName(const char* userName, Oid* oid);

/*
 * Create a trigger.  Returns the OID of the created trigger.
 *
 * queryString is the source text of the CREATE TRIGGER command.
 * This must be supplied if a whenClause is specified, else it can be NULL.
 *
 * constraintOid, if nonzero, says that this trigger is being created
 * internally to implement that constraint.  A suitable pg_depend entry will
 * be made to link the trigger to that constraint.	constraintOid is zero when
 * executing a user-entered CREATE TRIGGER command.  (For CREATE CONSTRAINT
 * TRIGGER, we build a pg_constraint entry internally.)
 *
 * indexOid, if nonzero, is the OID of an index associated with the constraint.
 * We do nothing with this except store it into pg_trigger.tgconstrindid.
 *
 * If isInternal is true then this is an internally-generated trigger.
 * This argument sets the tgisinternal field of the pg_trigger entry, and
 * if TRUE causes us to modify the given trigger name to ensure uniqueness.
 *
 * When isInternal is not true we require ACL_TRIGGER permissions on the
 * relation, as well as ACL_EXECUTE on the trigger function.  For internal
 * triggers the caller must apply any required permission checks.
 *
 * Note: can return  InvalidObjectAddress if we decided to not create a trigger at all,
 * but a foreign-key constraint.  This is a kluge for backwards compatibility.
 */
ObjectAddress CreateTrigger(CreateTrigStmt* stmt, const char* queryString, Oid relOid, Oid refRelOid, Oid constraintOid,
    Oid indexOid, bool isInternal)
{
    uint16 tgtype;
    int ncolumns;
    int2* columns = NULL;
    int2vector* tgattr = NULL;
    Node* whenClause = NULL;
    List* whenRtable = NIL;
    char* qual = NULL;
    Datum values[Natts_pg_trigger];
    bool nulls[Natts_pg_trigger];
    Relation rel;
    AclResult aclresult;
    Relation tgrel;
    SysScanDesc tgscan;
    ScanKeyData key;
    Relation pgrel;
    HeapTuple tuple;
    Oid fargtypes[1]; /* dummy */
    Oid funcoid;
    Oid funcrettype;
    Oid trigoid;
    char internaltrigname[NAMEDATALEN];
    char* trigname = NULL;
    Oid constrrelid = InvalidOid;
    ObjectAddress myself, referenced;
    Oid tg_owner = 0;
    Oid proownerid = InvalidOid;
    bool is_inline_procedural_func = false;
    int ret = 0;

    if (OidIsValid(relOid))
        rel = heap_open(relOid, ShareRowExclusiveLock);
    else
        rel = HeapOpenrvExtended(stmt->relation, ShareRowExclusiveLock, false, true);

    /*
     * Triggers must be on tables or views, and there are additional
     * relation-type-specific restrictions.
     */
    if (rel->rd_rel->relkind == RELKIND_RELATION) {
        /* Only support create trigger on normal row table. */
        if (!isInternal && !(pg_strcasecmp(RelationGetOrientation(rel), ORIENTATION_ROW) == 0 &&
                               rel->rd_rel->relpersistence == RELPERSISTENCE_PERMANENT))
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Only support CREATE TRIGGER on regular row table.")));

        /* Don't support create trigger on table which is undert cluster resizing. */
        if (RelationInClusterResizing(rel))
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Unsupport CREATE TRIGGER on table which is under cluster resizing.")));

        /* Tables can't have INSTEAD OF triggers */
        if (stmt->timing != TRIGGER_TYPE_BEFORE && stmt->timing != TRIGGER_TYPE_AFTER)
            ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("\"%s\" is a table", RelationGetRelationName(rel)),
                    errdetail("Tables cannot have INSTEAD OF triggers.")));
        
#ifdef ENABLE_MULTIPLE_NODES
        /* Triggers must be created on replicated table with primary key or unique index. */
        AttrNumber* indexed_col = NULL;
        int16 index_col_count = 0;

        if (IS_PGXC_COORDINATOR && !u_sess->attr.attr_sql.enable_trigger_shipping && !isInternal) {
            index_col_count = pgxc_find_primarykey(RelationGetRelid(rel), &indexed_col, true);
            if (IsReplicatedRelationWithoutPK(rel, indexed_col, index_col_count)) {
                ereport(WARNING,
                    (errmsg("Triggers must be created on replicated table with primary key or unique index.")));
            }
        }
#endif
    } else if (rel->rd_rel->relkind == RELKIND_VIEW || rel->rd_rel->relkind == RELKIND_CONTQUERY) {
        /*
         * Views can have INSTEAD OF triggers (which we check below are
         * row-level), or statement-level BEFORE/AFTER triggers.
         */
        if (stmt->timing != TRIGGER_TYPE_INSTEAD && stmt->row)
            ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("\"%s\" is a view", RelationGetRelationName(rel)),
                    errdetail("Views cannot have row-level BEFORE or AFTER triggers.")));
        /* Disallow TRUNCATE triggers on VIEWs */
        if (TRIGGER_FOR_TRUNCATE(stmt->events))
            ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("\"%s\" is a view", RelationGetRelationName(rel)),
                    errdetail("Views cannot have TRUNCATE triggers.")));
    } else
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("\"%s\" is not a table or view", RelationGetRelationName(rel))));

    if (!g_instance.attr.attr_common.allowSystemTableMods && !u_sess->attr.attr_common.IsInplaceUpgrade &&
        IsSystemRelation(rel))
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied: \"%s\" is a system catalog", RelationGetRelationName(rel))));

    if (stmt->isconstraint) {
        /*
         * We must take a lock on the target relation to protect against
         * concurrent drop.  It's not clear that AccessShareLock is strong
         * enough, but we certainly need at least that much... otherwise, we
         * might end up creating a pg_constraint entry referencing a
         * nonexistent table.
         */
        if (OidIsValid(refRelOid)) {
            LockRelationOid(refRelOid, AccessShareLock);
            constrrelid = refRelOid;
        } else if (stmt->constrrel != NULL) {
            constrrelid = RangeVarGetRelid(stmt->constrrel, AccessShareLock, false);
        }
    }

    if (u_sess->attr.attr_sql.sql_compatibility == B_FORMAT || u_sess->attr.attr_common.enable_dump_trigger_definer) {
        Oid curuser = GetUserId();
        if (stmt->definer) {
            HeapTuple roletuple = SearchUserHostName(stmt->definer, NULL);
            if (!HeapTupleIsValid(roletuple)) {
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("role \"%s\" is not exists", stmt->definer)));
            }
            proownerid = HeapTupleGetOid(roletuple);
            ReleaseSysCache(roletuple);
        }
        else {
            proownerid = curuser;
        }
        if (!systemDBA_arg(curuser) && proownerid != curuser)
            ereport (ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), 
                    errmsg("only system administrator can user definer other user ,others user definer self")));

        if (stmt->schemaname != NULL) {
            Oid relNamespaceId = RelationGetNamespace(rel);
            if (relNamespaceId != get_namespace_oid(stmt->schemaname, false)) {
                ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("trigger in wrong schema: \"%s\".\"%s\"", stmt->schemaname, stmt->trigname)));
            }
        }
    }
    /* permission checks */
    if (!isInternal) {
        bool anyResult = false;
        if (!IsSysSchema(RelationGetNamespace(rel))) {
            anyResult = HasSpecAnyPriv(GetUserId(), CREATE_ANY_TRIGGER, false);
        }
        aclresult = pg_class_aclcheck(RelationGetRelid(rel), GetUserId(), ACL_TRIGGER);
        if (aclresult != ACLCHECK_OK && anyResult != true)
            aclcheck_error(aclresult, ACL_KIND_CLASS, RelationGetRelationName(rel));

        if (OidIsValid(constrrelid)) {
            aclresult = pg_class_aclcheck(constrrelid, GetUserId(), ACL_TRIGGER);
            if (aclresult != ACLCHECK_OK && anyResult != true)
                aclcheck_error(aclresult, ACL_KIND_CLASS, get_rel_name(constrrelid));
        }
    }

    /* Compute tgtype */
    TRIGGER_CLEAR_TYPE(tgtype);
    if (stmt->row)
        TRIGGER_SETT_ROW(tgtype);
    tgtype |= (uint16)stmt->timing;
    tgtype |= (uint16)stmt->events;

    /* Disallow ROW-level TRUNCATE triggers */
    if (TRIGGER_FOR_ROW(tgtype) && TRIGGER_FOR_TRUNCATE(tgtype))
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("TRUNCATE FOR EACH ROW triggers are not supported")));

    /* INSTEAD triggers must be row-level, and can't have WHEN or columns */
    if (TRIGGER_FOR_INSTEAD(tgtype)) {
        if (!TRIGGER_FOR_ROW(tgtype))
            ereport(
                ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("INSTEAD OF triggers must be FOR EACH ROW")));
        if (stmt->whenClause)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("INSTEAD OF triggers cannot have WHEN conditions")));
        if (stmt->columns != NIL)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("INSTEAD OF triggers cannot have column lists")));
    }

    /*
     * Parse the WHEN clause, if any
     */
    if (stmt->whenClause) {
        ParseState* pstate = NULL;
        RangeTblEntry* rte = NULL;
        List* varList = NIL;
        ListCell* lc = NULL;

        /* Set up a pstate to parse with */
        pstate = make_parsestate(NULL);
        pstate->p_sourcetext = queryString;

        /*
         * Set up RTEs for OLD and NEW references.
         *
         * 'OLD' must always have varno equal to 1 and 'NEW' equal to 2.
         */
        rte = addRangeTableEntryForRelation(pstate, rel, makeAlias("old", NIL), false, false);
        addRTEtoQuery(pstate, rte, false, true, true);
        rte = addRangeTableEntryForRelation(pstate, rel, makeAlias("new", NIL), false, false);
        addRTEtoQuery(pstate, rte, false, true, true);

        /* Transform expression.  Copy to be sure we don't modify original */
        whenClause = transformWhereClause(pstate, (Node*)copyObject(stmt->whenClause), EXPR_KIND_TRIGGER_WHEN, "WHEN");
        /* we have to fix its collations too */
        assign_expr_collations(pstate, whenClause);

        /*
         * No subplans or aggregates, please
         */
        if (pstate->p_hasSubLinks)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot use subquery in trigger WHEN condition")));
        if (pstate->p_hasAggs)
            ereport(ERROR,
                (errcode(ERRCODE_GROUPING_ERROR), errmsg("cannot use aggregate function in trigger WHEN condition")));
        if (pstate->p_hasWindowFuncs)
            ereport(ERROR,
                (errcode(ERRCODE_WINDOWING_ERROR), errmsg("cannot use window function in trigger WHEN condition")));

        /*
         * Check for disallowed references to OLD/NEW.
         *
         * NB: pull_var_clause is okay here only because we don't allow
         * subselects in WHEN clauses; it would fail to examine the contents
         * of subselects.
         */
        varList = pull_var_clause(whenClause, PVC_REJECT_AGGREGATES, PVC_REJECT_PLACEHOLDERS);
        foreach (lc, varList) {
            Var* var = (Var*)lfirst(lc);

            switch (var->varno) {
                case PRS2_OLD_VARNO:
                    if (!TRIGGER_FOR_ROW(tgtype))
                        ereport(ERROR,
                            (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                                errmsg("statement trigger's WHEN condition cannot reference column values"),
                                parser_errposition(pstate, var->location)));
                    if (TRIGGER_FOR_INSERT(tgtype))
                        ereport(ERROR,
                            (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                                errmsg("INSERT trigger's WHEN condition cannot reference OLD values"),
                                parser_errposition(pstate, var->location)));
                    /* system columns are okay here */
                    break;
                case PRS2_NEW_VARNO:
                    if (!TRIGGER_FOR_ROW(tgtype))
                        ereport(ERROR,
                            (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                                errmsg("statement trigger's WHEN condition cannot reference column values"),
                                parser_errposition(pstate, var->location)));
                    if (TRIGGER_FOR_DELETE(tgtype))
                        ereport(ERROR,
                            (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                                errmsg("DELETE trigger's WHEN condition cannot reference NEW values"),
                                parser_errposition(pstate, var->location)));
                    if (var->varattno < 0 && TRIGGER_FOR_BEFORE(tgtype))
                        ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("BEFORE trigger's WHEN condition cannot reference NEW system columns"),
                                parser_errposition(pstate, var->location)));
                    if (TRIGGER_FOR_BEFORE(tgtype) && var->varattno == 0 && RelationGetDescr(rel)->constr &&
                        RelationGetDescr(rel)->constr->has_generated_stored)
                        ereport(ERROR, (errmodule(MOD_GEN_COL), errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                            errmsg("BEFORE trigger's WHEN condition cannot reference NEW generated columns"),
                            errdetail("A whole-row reference is used and the table contains generated columns."),
                            parser_errposition(pstate, var->location)));
                    if (TRIGGER_FOR_BEFORE(tgtype) && var->varattno > 0 &&
                        ISGENERATEDCOL(RelationGetDescr(rel), var->varattno - 1))
                        ereport(ERROR, (errmodule(MOD_GEN_COL), errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                            errmsg("BEFORE trigger's WHEN condition cannot reference NEW generated columns"),
                            errdetail("Column \"%s\" is a generated column.",
                            NameStr(TupleDescAttr(RelationGetDescr(rel), var->varattno - 1)->attname)),
                            parser_errposition(pstate, var->location)));
                    break;
                default:
                    /* can't happen without add_missing_from, so just elog */
                    ereport(ERROR,
                        (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                            errmsg("trigger WHEN condition cannot contain references to other relations")));
                    break;
            }
        }

        /* we'll need the rtable for recordDependencyOnExpr */
        whenRtable = pstate->p_rtable;

        qual = nodeToString(whenClause);

        free_parsestate(pstate);
    } else {
        whenClause = NULL;
        whenRtable = NIL;
        qual = NULL;
    }

    /*
     * Generate the trigger's OID now, so that we can use it in the name if
     * needed.
     */
    tgrel = heap_open(TriggerRelationId, RowExclusiveLock);

    trigoid = GetNewOid(tgrel);

    /*
     * If trigger is internally generated, modify the provided trigger name to
     * ensure uniqueness by appending the trigger OID.	(Callers will usually
     * supply a simple constant trigger name in these cases.)
     */
    if (isInternal) {
        errno_t rc = EOK;
        rc = snprintf_s(
            internaltrigname, sizeof(internaltrigname), sizeof(internaltrigname) - 1, "%s_%u", stmt->trigname, trigoid);
        securec_check_ss(rc, "\0", "\0");
        trigname = internaltrigname;
    } else {
        /* user-defined trigger; use the specified trigger name as-is */
        trigname = stmt->trigname;
    }

    /*
     * Scan pg_trigger for existing triggers on relation.  We do this only to
     * give a nice error message if there's already a trigger of the same
     * name.  (The unique index on tgrelid/tgname would complain anyway.) We
     * can skip this for internally generated triggers, since the name
     * modification above should be sufficient.
     *
     * NOTE that this is cool only because we have AccessExclusiveLock on the
     * relation, so the trigger set won't be changing underneath us.
     */
    if (!isInternal) {
        if (stmt->funcSource != NULL && u_sess->attr.attr_sql.sql_compatibility == B_FORMAT) {
            ScanKeyInit(
                &key, Anum_pg_trigger_tgname, BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(trigname));
            tgscan = systable_beginscan(tgrel, TriggerNameIndexId, true, NULL, 1, &key);
            if (HeapTupleIsValid(tuple = systable_getnext(tgscan))) {
                if (stmt->if_not_exists) {
                    ereport(NOTICE,
                        (errmsg("trigger \"%s\" already exists, skipping",
                            trigname)));
                    systable_endscan(tgscan);
                    heap_close(tgrel, RowExclusiveLock);
                    heap_close(rel, NoLock);
                    myself.classId = TriggerRelationId;
                    myself.objectId = trigoid;
                    myself.objectSubId = 0;                    
                    return myself;
                }
                else {
                    systable_endscan(tgscan);
                    heap_close(tgrel, RowExclusiveLock);
                    heap_close(rel, NoLock);
                    ereport(ERROR,
                        (errcode(ERRCODE_DUPLICATE_OBJECT),
                        errmsg("trigger \"%s\" already exists",
                            trigname)));
                }
            }
            systable_endscan(tgscan);
        } else {
            ScanKeyInit(
            &key, Anum_pg_trigger_tgrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(RelationGetRelid(rel)));
            tgscan = systable_beginscan(tgrel, TriggerRelidNameIndexId, true, NULL, 1, &key);
            while (HeapTupleIsValid(tuple = systable_getnext(tgscan))) {
                Form_pg_trigger pg_trigger = (Form_pg_trigger)GETSTRUCT(tuple);
                if (namestrcmp(&(pg_trigger->tgname), trigname) == 0) {
                    systable_endscan(tgscan);
                    heap_close(tgrel, RowExclusiveLock);
                    heap_close(rel, NoLock);
                    ereport(ERROR,
                        (errcode(ERRCODE_DUPLICATE_OBJECT),
                        errmsg("trigger \"%s\" for relation \"%s\" already exists",
                            trigname,
                            RelationGetRelationName(rel))));
                }
            }
            systable_endscan(tgscan);
        }
    }

    /*
     * Build the new pg_trigger tuple.
     */
    errno_t rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "", "");
    
    if (stmt->funcSource != NULL && u_sess->attr.attr_sql.sql_compatibility == B_FORMAT) {
        values[Anum_pg_trigger_tgfbody -1] = CStringGetTextDatum(stmt->funcSource->bodySrc);
        CreateFunctionStmt* n = makeNode(CreateFunctionStmt);
        n->isOraStyle = false;
        n->isPrivate = false;
        n->replace = false;
        n->definer = stmt->definer;
        char* trigname = pstrdup(stmt->trigname);
        char* relname = pstrdup(stmt->relation->relname);
        /* means we need to rebuild the function name */
        char* funcNameTmp = rebuild_funcname_for_b_trigger(trigname, relname);
        n->funcname = list_make1(makeString(funcNameTmp));
        n->parameters = NULL;
        n->returnType = makeTypeName("trigger");
        const char* inlineProcessDesc = " return NEW;end";
        size_t originBodyLen = strlen(stmt->funcSource->bodySrc);
        size_t bodySrcTempSize = originBodyLen + strlen(inlineProcessDesc) + 1;
        char* bodySrcTemp = (char*)palloc(bodySrcTempSize);
        int last_end = -1;
        for (int i = originBodyLen - 3; i > 0; i--) {
            if (pg_strncasecmp(stmt->funcSource->bodySrc + i, "end", strlen("end")) == 0) {
                last_end = i;
                break;
            }
        }
        if (last_end < 1){
            ereport(ERROR, (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                    errmsg("trigger function body has syntax error")));
        }
        ret = memcpy_s(bodySrcTemp, bodySrcTempSize, stmt->funcSource->bodySrc, last_end);
        securec_check(ret, "\0", "\0");
        bodySrcTemp[last_end] = '\0';
        ret = strcat_s(bodySrcTemp, bodySrcTempSize, inlineProcessDesc);
        securec_check(ret, "\0", "\0");
        n->options = lappend(n->options, makeDefElem("as", (Node*)list_make1(makeString(bodySrcTemp))));
        n->options = lappend(n->options, makeDefElem("language", (Node*)makeString("plpgsql")));
        n->withClause = NIL;
        n->isProcedure = false;
        size_t queryStringLen = strlen(bodySrcTemp) + strlen(funcNameTmp) + strlen("create function  returns trigger ") + 1;
        char *queryString = (char*)palloc(queryStringLen);
        ret = snprintf_s(queryString, queryStringLen, queryStringLen - 1, "create function %s returns trigger %s", bodySrcTemp,funcNameTmp);
        securec_check_ss(ret, "\0", "\0");
        CreateFunction(n, queryString, InvalidOid);
        stmt->funcname = n->funcname;
        is_inline_procedural_func = true;
    } else {
        nulls[Anum_pg_trigger_tgfbody - 1] = true;
    }
    /*
     * Find and validate the trigger function.
     */
    funcoid = LookupFuncName(stmt->funcname, 0, fargtypes, false);
    if (!isInternal) {
        if (get_func_lang(funcoid) != get_language_oid("plpgsql", true) &&
            get_func_lang(funcoid) != get_language_oid("pltsql", true)) {
            ereport(LOG,
                (errmsg("Trigger function with non-plpgsql or non-pltsql type is not recommended."),
                 errdetail("Non-plpgsql or non-pltsql trigger function are not shippable by default."),
                 errhint("Unshippable trigger may lead to bad performance.")));
        }
        aclresult = pg_proc_aclcheck(funcoid, GetUserId(), ACL_EXECUTE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error(aclresult, ACL_KIND_PROC, NameListToString(stmt->funcname));
    }
    funcrettype = get_func_rettype(funcoid);
    if (funcrettype != TRIGGEROID) {
        /*
         * We allow OPAQUE just so we can load old dump files.	When we see a
         * trigger function declared OPAQUE, change it to TRIGGER.
         */
        if (funcrettype == OPAQUEOID) {
            ereport(WARNING,
                (errmsg("changing return type of function %s from \"opaque\" to \"trigger\"",
                    NameListToString(stmt->funcname))));
            SetFunctionReturnType(funcoid, TRIGGEROID);
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                    errmsg("function %s must return type \"trigger\"", NameListToString(stmt->funcname))));
        }
    }

    /*
     * If the command is a user-entered CREATE CONSTRAINT TRIGGER command that
     * references one of the built-in RI_FKey trigger functions, assume it is
     * from a dump of a pre-7.3 foreign key constraint, and take steps to
     * convert this legacy representation into a regular foreign key
     * constraint.	Ugly, but necessary for loading old dump files.
     */
    int stmt_args_num = 6;
    int stmt_args_divided = 2;
    if (stmt->isconstraint && !isInternal && list_length(stmt->args) >= stmt_args_num && (list_length(stmt->args) % stmt_args_divided) == 0 &&
        RI_FKey_trigger_type(funcoid) != RI_TRIGGER_NONE) {
        /* Keep lock on target rel until end of xact */
        heap_close(rel, NoLock);

        ConvertTriggerToFK(stmt, funcoid);

        return InvalidObjectAddress;
    }

    /*
     * If it's a user-entered CREATE CONSTRAINT TRIGGER command, make a
     * corresponding pg_constraint entry.
     */
    if (stmt->isconstraint && !OidIsValid(constraintOid)) {
        /* Internal callers should have made their own constraints */
        Assert(!isInternal);
        constraintOid = CreateConstraintEntry(stmt->trigname,
            RelationGetNamespace(rel),
            CONSTRAINT_TRIGGER,
            stmt->deferrable,
            stmt->initdeferred,
            true,
            RelationGetRelid(rel),
            NULL, /* no conkey */
            0,
            0,
            InvalidOid, /* no domain */
            InvalidOid, /* no index */
            InvalidOid, /* no foreign key */
            NULL,
            NULL,
            NULL,
            NULL,
            0,
            ' ',
            ' ',
            ' ',
            NULL, /* no exclusion */
            NULL, /* no check constraint */
            NULL,
            NULL,
            true,  /* islocal */
            0,     /* inhcount */
            true,  /* isnoinherit */
            NULL); /* @hdfs informational constraint */
    }

    values[Anum_pg_trigger_tgrelid - 1] = ObjectIdGetDatum(RelationGetRelid(rel));
    values[Anum_pg_trigger_tgname - 1] = DirectFunctionCall1(namein, CStringGetDatum(trigname));
    values[Anum_pg_trigger_tgfoid - 1] = ObjectIdGetDatum(funcoid);
    values[Anum_pg_trigger_tgtype - 1] = UInt16GetDatum(tgtype);
    values[Anum_pg_trigger_tgenabled - 1] = CharGetDatum(TRIGGER_FIRES_ON_ORIGIN);
    values[Anum_pg_trigger_tgisinternal - 1] = BoolGetDatum(isInternal);
    values[Anum_pg_trigger_tgconstrrelid - 1] = ObjectIdGetDatum(constrrelid);
    values[Anum_pg_trigger_tgconstrindid - 1] = ObjectIdGetDatum(indexOid);
    values[Anum_pg_trigger_tgconstraint - 1] = ObjectIdGetDatum(constraintOid);
    values[Anum_pg_trigger_tgdeferrable - 1] = BoolGetDatum(stmt->deferrable);
    values[Anum_pg_trigger_tginitdeferred - 1] = BoolGetDatum(stmt->initdeferred);

    if (u_sess->attr.attr_sql.sql_compatibility == B_FORMAT) {
        struct pg_tm tm;
        fsec_t fsec;
        GetCurrentTimeUsec(&tm, &fsec, NULL);
        size_t timeLen = 21;
        char* timeNum = (char*)palloc(timeLen);
        ret = snprintf_s(timeNum, timeLen, timeLen - 1,"%d%02d%02d%02d%02d%02d%06d",
                tm.tm_year, tm.tm_mon, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, fsec);
        securec_check_ss(ret, "\0", "\0");
        char * needTestName = stmt->trgordername;
        values[Anum_pg_trigger_tgtime - 1] = DirectFunctionCall1(namein, CStringGetDatum(timeNum));
        if(needTestName != NULL) {
            size_t trigger_orderLen = strlen("precedes") + 1;
            char* trigger_order = (char*)palloc(trigger_orderLen);
            values[Anum_pg_trigger_tgordername - 1] = DirectFunctionCall1(namein, CStringGetDatum(stmt->trgordername));
            if (stmt->is_follows){
                ret = snprintf_s(trigger_order,trigger_orderLen,trigger_orderLen - 1,"follows");
                securec_check_ss(ret, "\0", "\0");
                values[Anum_pg_trigger_tgorder - 1] = DirectFunctionCall1(namein, CStringGetDatum(trigger_order));
            }else if(stmt->trgordername != NULL){
                ret = snprintf_s(trigger_order,trigger_orderLen,trigger_orderLen - 1, "precedes");
                securec_check_ss(ret, "\0", "\0");
                values[Anum_pg_trigger_tgorder - 1] = DirectFunctionCall1(namein, CStringGetDatum(trigger_order));
            }
            bool is_find = false;
            if (!isInternal) {
                ScanKeyInit(
                    &key, Anum_pg_trigger_tgrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(RelationGetRelid(rel)));
                tgscan = systable_beginscan(tgrel, TriggerRelidNameIndexId, true, NULL, 1, &key);
                while (HeapTupleIsValid(tuple = systable_getnext(tgscan))) {
                    Form_pg_trigger pg_trigger = (Form_pg_trigger)GETSTRUCT(tuple);
                    if (namestrcmp(&(pg_trigger->tgname), needTestName) == 0) {
                        if (pg_trigger->tgtype == tgtype) {
                            is_find = true;
                            break;
                        } else {
                            ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT),
                                errmsg("trigger \"%s\" type is not same as current trigger",
                                    needTestName)));
                        }
                    }
                }
                systable_endscan(tgscan);
                if (!is_find) {
                    ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT),
                        errmsg("trigger \"%s\" for table \"%s\" is not exists",
                            needTestName,RelationGetRelationName(rel))));
                }
            }
        } else {
            nulls[Anum_pg_trigger_tgordername - 1] = true;
            nulls[Anum_pg_trigger_tgorder - 1] = true;
        }
    } else {
        nulls[Anum_pg_trigger_tgordername - 1] = true;
        nulls[Anum_pg_trigger_tgorder - 1] = true;
        nulls[Anum_pg_trigger_tgtime - 1] = true;
    }
    if (stmt->args) {
        ListCell* le = NULL;
        char* args = NULL;
        int16 nargs = list_length(stmt->args);
        int len = 0;
        int added_len = 4;

        foreach (le, stmt->args) {
            char* ar = strVal(lfirst(le));

            len += strlen(ar) + added_len;
            for (; *ar; ar++) {
                if (*ar == '\\')
                    len++;
            }
        }
        args = (char*)palloc(len + 1);
        args[0] = '\0';
        foreach (le, stmt->args) {
            char* s = strVal(lfirst(le));
            char* d = args + strlen(args);
            rc = EOK;

            while (*s) {
                if (*s == '\\')
                    *d++ = '\\';
                *d++ = *s++;
            }
            rc = strcpy_s(d, sizeof("\\000"), "\\000");
            securec_check(rc, "\0", "\0");
        }
        values[Anum_pg_trigger_tgnargs - 1] = Int16GetDatum(nargs);
        values[Anum_pg_trigger_tgargs - 1] = DirectFunctionCall1(byteain, CStringGetDatum(args));
    } else {
        values[Anum_pg_trigger_tgnargs - 1] = Int16GetDatum(0);
        values[Anum_pg_trigger_tgargs - 1] = DirectFunctionCall1(byteain, CStringGetDatum(""));
    }

    /* build column number array if it's a column-specific trigger */
    ncolumns = list_length(stmt->columns);
    if (ncolumns == 0) {
        columns = NULL;
    } else {
        ListCell* cell = NULL;
        int i = 0;

        columns = (int2*)palloc(ncolumns * sizeof(int2));
        foreach (cell, stmt->columns) {
            char* name = strVal(lfirst(cell));
            int2 attnum;
            int j;

            /* Lookup column name.	System columns are not allowed */
            attnum = attnameAttNum(rel, name, false);
            if (attnum == InvalidAttrNumber)
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_COLUMN),
                        errmsg("column \"%s\" of relation \"%s\" does not exist", name, RelationGetRelationName(rel))));

            /* Check for duplicates */
            for (j = i - 1; j >= 0; j--) {
                if (columns[j] == attnum)
                    ereport(ERROR,
                        (errcode(ERRCODE_DUPLICATE_COLUMN), errmsg("column \"%s\" specified more than once", name)));
            }

            columns[i++] = attnum;
        }
    }
    tgattr = buildint2vector(columns, ncolumns);
    values[Anum_pg_trigger_tgattr - 1] = PointerGetDatum(tgattr);

    /* set tgqual if trigger has WHEN clause */
    if (qual != NULL)
        values[Anum_pg_trigger_tgqual - 1] = CStringGetTextDatum(qual);
    else
        nulls[Anum_pg_trigger_tgqual - 1] = true;

    /* set trigger owner */
    if (u_sess->attr.attr_sql.sql_compatibility == B_FORMAT || u_sess->attr.attr_common.enable_dump_trigger_definer) {
        tg_owner = proownerid;
    }
    else {
        tg_owner = GetUserId();
    }
    if (OidIsValid(tg_owner)) {
        values[Anum_pg_trigger_tgowner - 1] = ObjectIdGetDatum(tg_owner);
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("invalid current user oid for trigger")));
    }

    tuple = heap_form_tuple(tgrel->rd_att, values, nulls);

    /* force tuple to have the desired OID */
    HeapTupleSetOid(tuple, trigoid);

    /*
     * Insert tuple into pg_trigger.
     */
    (void)simple_heap_insert(tgrel, tuple);

    CatalogUpdateIndexes(tgrel, tuple);

    tableam_tops_free_tuple(tuple);
    heap_close(tgrel, RowExclusiveLock);

    pfree(DatumGetPointer(values[Anum_pg_trigger_tgname - 1]));
    pfree(DatumGetPointer(values[Anum_pg_trigger_tgargs - 1]));
    pfree(DatumGetPointer(values[Anum_pg_trigger_tgattr - 1]));

    /*
     * Update relation's pg_class entry.  Crucial side-effect: other backends
     * (and this one too!) are sent SI message to make them rebuild relcache
     * entries.
     */
    pgrel = heap_open(RelationRelationId, RowExclusiveLock);
    tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(RelationGetRelid(rel)));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for relation %u", RelationGetRelid(rel))));
    }

    ((Form_pg_class)GETSTRUCT(tuple))->relhastriggers = true;

    simple_heap_update(pgrel, &tuple->t_self, tuple);

    CatalogUpdateIndexes(pgrel, tuple);

    tableam_tops_free_tuple(tuple);
    heap_close(pgrel, RowExclusiveLock);

    /*
     * We used to try to update the rel's relcache entry here, but that's
     * fairly pointless since it will happen as a byproduct of the upcoming
     * CommandCounterIncrement...
     */
    /*
     * Record dependencies for trigger.  Always place a normal dependency on
     * the function.
     */
    myself.classId = TriggerRelationId;
    myself.objectId = trigoid;
    myself.objectSubId = 0;

    referenced.classId = ProcedureRelationId;
    referenced.objectId = funcoid;
    referenced.objectSubId = 0;
    recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

    if (is_inline_procedural_func) {
        recordDependencyOn(&referenced, &myself, DEPENDENCY_AUTO);
    }
    else {
        recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
    }

    if (isInternal && OidIsValid(constraintOid)) {
        /*
         * Internally-generated trigger for a constraint, so make it an
         * internal dependency of the constraint.  We can skip depending on
         * the relation(s), as there'll be an indirect dependency via the
         * constraint.
         */
        referenced.classId = ConstraintRelationId;
        referenced.objectId = constraintOid;
        referenced.objectSubId = 0;
        recordDependencyOn(&myself, &referenced, DEPENDENCY_INTERNAL);
    } else {
        /*
         * User CREATE TRIGGER, so place dependencies.	We make trigger be
         * auto-dropped if its relation is dropped or if the FK relation is
         * dropped.  (Auto drop is compatible with our pre-7.3 behavior.)
         */
        referenced.classId = RelationRelationId;
        referenced.objectId = RelationGetRelid(rel);
        referenced.objectSubId = 0;
        recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);
        if (OidIsValid(constrrelid)) {
            referenced.classId = RelationRelationId;
            referenced.objectId = constrrelid;
            referenced.objectSubId = 0;
            recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);
        }
        /* Not possible to have an index dependency in this case */
        Assert(!OidIsValid(indexOid));

        /*
         * If it's a user-specified constraint trigger, make the constraint
         * internally dependent on the trigger instead of vice versa.
         */
        if (OidIsValid(constraintOid)) {
            referenced.classId = ConstraintRelationId;
            referenced.objectId = constraintOid;
            referenced.objectSubId = 0;
            recordDependencyOn(&referenced, &myself, DEPENDENCY_INTERNAL);
        }
    }

    /* If column-specific trigger, add normal dependencies on columns */
    if (columns != NULL) {
        int i;

        referenced.classId = RelationRelationId;
        referenced.objectId = RelationGetRelid(rel);
        for (i = 0; i < ncolumns; i++) {
            referenced.objectSubId = columns[i];
            recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
        }
    }

    /*
     * If it has a WHEN clause, add dependencies on objects mentioned in the
     * expression (eg, functions, as well as any columns used).
     */
    if (whenClause != NULL)
        recordDependencyOnExpr(&myself, whenClause, whenRtable, DEPENDENCY_NORMAL);

    /* Post creation hook for new trigger */
    InvokeObjectAccessHook(OAT_POST_CREATE, TriggerRelationId, trigoid, 0, NULL);

    if (!isInternal) {
        UpdatePgObjectChangecsn(rel->rd_id, rel->rd_rel->relkind);
    }

    /* Keep lock on target rel until end of xact */
    heap_close(rel, NoLock);

    return myself;
}

/*
 * Convert legacy (pre-7.3) CREATE CONSTRAINT TRIGGER commands into
 * full-fledged foreign key constraints.
 *
 * The conversion is complex because a pre-7.3 foreign key involved three
 * separate triggers, which were reported separately in dumps.	While the
 * single trigger on the referencing table adds no new information, we need
 * to know the trigger functions of both of the triggers on the referenced
 * table to build the constraint declaration.  Also, due to lack of proper
 * dependency checking pre-7.3, it is possible that the source database had
 * an incomplete set of triggers resulting in an only partially enforced
 * FK constraint.  (This would happen if one of the tables had been dropped
 * and re-created, but only if the DB had been affected by a 7.0 pg_dump bug
 * that caused loss of tgconstrrelid information.)	We choose to translate to
 * an FK constraint only when we've seen all three triggers of a set.  This is
 * implemented by storing unmatched items in a list in t_thrd.top_mem_cxt.
 * We match triggers together by comparing the trigger arguments (which
 * include constraint name, table and column names, so should be good enough).
 */
typedef struct {
    List* args;      /* list of (T_String) Values or NIL */
    Oid funcoids[3]; /* OIDs of trigger functions */
                     /* The three function OIDs are stored in the order update, delete, child */
} OldTriggerInfo;

static void ConvertTriggerToFK(CreateTrigStmt* stmt, Oid funcoid)
{
    static const char* const funcdescr[3] = {gettext_noop("Found referenced table's UPDATE trigger."),
        gettext_noop("Found referenced table's DELETE trigger."),
        gettext_noop("Found referencing table's trigger.")};

    char* constr_name = NULL;
    char* fk_table_name = NULL;
    char* pk_table_name = NULL;
    char fk_matchtype = FKCONSTR_MATCH_UNSPECIFIED;
    List* fk_attrs = NIL;
    List* pk_attrs = NIL;
    StringInfoData buf;
    int funcnum;
    OldTriggerInfo* info = NULL;
    ListCell* l = NULL;
    int i;

    /* Parse out the trigger arguments */
    constr_name = strVal(linitial(stmt->args));
    fk_table_name = strVal(lsecond(stmt->args));
    pk_table_name = strVal(lthird(stmt->args));
    i = 0;
    int constraint_args = 4;
    int divided_num = 2;
    foreach (l, stmt->args) {
        Value* arg = (Value*)lfirst(l);

        i++;
        if (i < constraint_args) {/* skip constraint and table names */
            continue;
        }
        if (i == constraint_args) {
            /* handle match type */
            if (strcmp(strVal(arg), "FULL") == 0)
                fk_matchtype = FKCONSTR_MATCH_FULL;
            else
                fk_matchtype = FKCONSTR_MATCH_UNSPECIFIED;
            continue;
        }
        if (i % divided_num) {
            fk_attrs = lappend(fk_attrs, arg); 
        } else {
            pk_attrs = lappend(pk_attrs, arg);
        }
    }

    /* Prepare description of constraint for use in messages */
    initStringInfo(&buf);
    appendStringInfo(&buf, "FOREIGN KEY %s(", quote_identifier(fk_table_name));
    i = 0;
    foreach (l, fk_attrs) {
        Value* arg = (Value*)lfirst(l);

        if (i++ > 0)
            appendStringInfoChar(&buf, ',');
        appendStringInfoString(&buf, quote_identifier(strVal(arg)));
    }
    appendStringInfo(&buf, ") REFERENCES %s(", quote_identifier(pk_table_name));
    i = 0;
    foreach (l, pk_attrs) {
        Value* arg = (Value*)lfirst(l);

        if (i++ > 0)
            appendStringInfoChar(&buf, ',');
        appendStringInfoString(&buf, quote_identifier(strVal(arg)));
    }
    appendStringInfoChar(&buf, ')');

    /* Identify class of trigger --- update, delete, or referencing-table */
    switch (funcoid) {
        case F_RI_FKEY_CASCADE_UPD:
        case F_RI_FKEY_RESTRICT_UPD:
        case F_RI_FKEY_SETNULL_UPD:
        case F_RI_FKEY_SETDEFAULT_UPD:
        case F_RI_FKEY_NOACTION_UPD:
            funcnum = 0;
            break;

        case F_RI_FKEY_CASCADE_DEL:
        case F_RI_FKEY_RESTRICT_DEL:
        case F_RI_FKEY_SETNULL_DEL:
        case F_RI_FKEY_SETDEFAULT_DEL:
        case F_RI_FKEY_NOACTION_DEL:
            funcnum = 1;
            break;

        default:
            funcnum = 2;
            break;
    }

    /* See if we have a match to this trigger */
    foreach (l, u_sess->tri_cxt.info_list) {
        info = (OldTriggerInfo*)lfirst(l);
        if (info->funcoids[funcnum] == InvalidOid && equal(info->args, stmt->args)) {
            info->funcoids[funcnum] = funcoid;
            break;
        }
    }

    if (l == NULL) {
        /* First trigger of set, so create a new list entry */
        MemoryContext oldContext;

        ereport(NOTICE,
            (errmsg("ignoring incomplete trigger group for constraint \"%s\" %s", constr_name, buf.data),
                errdetail_internal("%s", _(funcdescr[funcnum]))));
        oldContext = MemoryContextSwitchTo(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER));
        info = (OldTriggerInfo*)palloc0(sizeof(OldTriggerInfo));
        info->args = (List*)copyObject(stmt->args);
        info->funcoids[funcnum] = funcoid;
        u_sess->tri_cxt.info_list = lappend(u_sess->tri_cxt.info_list, info);
        (void)MemoryContextSwitchTo(oldContext);
    } else if (info->funcoids[0] == InvalidOid || info->funcoids[1] == InvalidOid || info->funcoids[2] == InvalidOid) {
        /* Second trigger of set */
        ereport(NOTICE,
            (errmsg("ignoring incomplete trigger group for constraint \"%s\" %s", constr_name, buf.data),
                errdetail_internal("%s", _(funcdescr[funcnum]))));
    } else {
        /* OK, we have a set, so make the FK constraint ALTER TABLE cmd */
        AlterTableStmt* atstmt = makeNode(AlterTableStmt);
        AlterTableCmd* atcmd = makeNode(AlterTableCmd);
        Constraint* fkcon = makeNode(Constraint);

        ereport(NOTICE,
            (errmsg("converting trigger group into constraint \"%s\" %s", constr_name, buf.data),
                errdetail_internal("%s", _(funcdescr[funcnum]))));
        fkcon->contype = CONSTR_FOREIGN;
        fkcon->location = -1;
        int fk_tal_idx = 2;
        if (funcnum == fk_tal_idx) {
            /* This trigger is on the FK table */
            atstmt->relation = stmt->relation;
            if (stmt->constrrel)
                fkcon->pktable = stmt->constrrel;
            else {
                /* Work around ancient pg_dump bug that omitted constrrel */
                fkcon->pktable = makeRangeVar(NULL, pk_table_name, -1);
            }
        } else {
            /* This trigger is on the PK table */
            fkcon->pktable = stmt->relation;
            if (stmt->constrrel)
                atstmt->relation = stmt->constrrel;
            else {
                /* Work around ancient pg_dump bug that omitted constrrel */
                atstmt->relation = makeRangeVar(NULL, fk_table_name, -1);
            }
        }
        atstmt->cmds = list_make1(atcmd);
        atstmt->relkind = OBJECT_TABLE;
        atcmd->subtype = AT_AddConstraint;
        atcmd->def = (Node*)fkcon;
        if (strcmp(constr_name, "<unnamed>") == 0)
            fkcon->conname = NULL;
        else
            fkcon->conname = constr_name;
        fkcon->fk_attrs = fk_attrs;
        fkcon->pk_attrs = pk_attrs;
        fkcon->fk_matchtype = fk_matchtype;
        switch (info->funcoids[0]) {
            case F_RI_FKEY_NOACTION_UPD:
                fkcon->fk_upd_action = FKCONSTR_ACTION_NOACTION;
                break;
            case F_RI_FKEY_CASCADE_UPD:
                fkcon->fk_upd_action = FKCONSTR_ACTION_CASCADE;
                break;
            case F_RI_FKEY_RESTRICT_UPD:
                fkcon->fk_upd_action = FKCONSTR_ACTION_RESTRICT;
                break;
            case F_RI_FKEY_SETNULL_UPD:
                fkcon->fk_upd_action = FKCONSTR_ACTION_SETNULL;
                break;
            case F_RI_FKEY_SETDEFAULT_UPD:
                fkcon->fk_upd_action = FKCONSTR_ACTION_SETDEFAULT;
                break;
            default:
                /* can't get here because of earlier checks */
                ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("confused about RI update function")));
        }
        switch (info->funcoids[1]) {
            case F_RI_FKEY_NOACTION_DEL:
                fkcon->fk_del_action = FKCONSTR_ACTION_NOACTION;
                break;
            case F_RI_FKEY_CASCADE_DEL:
                fkcon->fk_del_action = FKCONSTR_ACTION_CASCADE;
                break;
            case F_RI_FKEY_RESTRICT_DEL:
                fkcon->fk_del_action = FKCONSTR_ACTION_RESTRICT;
                break;
            case F_RI_FKEY_SETNULL_DEL:
                fkcon->fk_del_action = FKCONSTR_ACTION_SETNULL;
                break;
            case F_RI_FKEY_SETDEFAULT_DEL:
                fkcon->fk_del_action = FKCONSTR_ACTION_SETDEFAULT;
                break;
            default:
                /* can't get here because of earlier checks */
                ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("confused about RI delete function")));
        }
        fkcon->deferrable = stmt->deferrable;
        fkcon->initdeferred = stmt->initdeferred;
        fkcon->skip_validation = false;
        fkcon->initially_valid = true;
        fkcon->isdisable = false;

        /* ... and execute it */
        processutility_context proutility_cxt;
        proutility_cxt.parse_tree = (Node*)atstmt;
        proutility_cxt.query_string = "(generated ALTER TABLE ADD FOREIGN KEY command)";
        proutility_cxt.readOnlyTree = false;
        proutility_cxt.params = NULL;
        proutility_cxt.is_top_level = false;
        ProcessUtility(&proutility_cxt,
            None_Receiver,
#ifdef PGXC
            false,
#endif /* PGXC */
            NULL,
            PROCESS_UTILITY_GENERATED);

        /* Remove the matched item from the list */
        u_sess->tri_cxt.info_list = list_delete_ptr(u_sess->tri_cxt.info_list, info);
        pfree_ext(info);
        /* We leak the copied args ... not worth worrying about */
    }
}

/*
 * Guts of trigger deletion.
 */
void RemoveTriggerById(Oid trigOid)
{
    Relation tgrel;
    SysScanDesc tgscan;
    ScanKeyData skey[1];
    HeapTuple tup;
    Oid relid;
    Relation rel;

    tgrel = heap_open(TriggerRelationId, RowExclusiveLock);

    /*
     * Find the trigger to delete.
     */
    ScanKeyInit(&skey[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(trigOid));

    tgscan = systable_beginscan(tgrel, TriggerOidIndexId, true, NULL, 1, skey);

    tup = systable_getnext(tgscan);
    if (!HeapTupleIsValid(tup))
        ereport(
            ERROR, (errcode(ERRCODE_TRIGGERED_INVALID_TUPLE), errmsg("could not find tuple for trigger %u", trigOid)));

    /*
     * Open and exclusive-lock the relation the trigger belongs to.
     */
    relid = ((Form_pg_trigger)GETSTRUCT(tup))->tgrelid;

    rel = heap_open(relid, AccessExclusiveLock);
    if (rel->rd_rel->relkind != RELKIND_RELATION && rel->rd_rel->relkind != RELKIND_VIEW && 
        rel->rd_rel->relkind != RELKIND_CONTQUERY)
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("\"%s\" is not a table or view", RelationGetRelationName(rel))));

    if (!g_instance.attr.attr_common.allowSystemTableMods && !u_sess->attr.attr_common.IsInplaceUpgrade &&
        IsSystemRelation(rel))
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied: \"%s\" is a system catalog", RelationGetRelationName(rel))));

    /*
     * Delete the pg_trigger tuple.
     */
    simple_heap_delete(tgrel, &tup->t_self);

    systable_endscan(tgscan);
    heap_close(tgrel, RowExclusiveLock);

    /*
     * We do not bother to try to determine whether any other triggers remain,
     * which would be needed in order to decide whether it's safe to clear the
     * relation's relhastriggers.  (In any case, there might be a concurrent
     * process adding new triggers.)  Instead, just force a relcache inval to
     * make other backends (and this one too!) rebuild their relcache entries.
     * There's no great harm in leaving relhastriggers true even if there are
     * no triggers left.
     */
    CacheInvalidateRelcache(rel);

    /* Record the changecsn of table the trigger belongs to */
    UpdatePgObjectChangecsn(relid, rel->rd_rel->relkind);

    /* Keep lock on trigger's rel until end of xact */
    heap_close(rel, NoLock);
}

/*
 * get_trigger_oid - Look up a trigger by name to find its OID.
 *
 * If missing_ok is false, throw an error if trigger not found.  If
 * true, just return InvalidOid.
 */
Oid get_trigger_oid(Oid relid, const char* trigname, bool missing_ok)
{
    Relation tgrel;
    ScanKeyData skey[2];
    SysScanDesc tgscan;
    HeapTuple tup;
    Oid oid;

    /*
     * Find the trigger, verify permissions, set up object address
     */
    tgrel = heap_open(TriggerRelationId, AccessShareLock);

    ScanKeyInit(&skey[0], Anum_pg_trigger_tgrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));
    ScanKeyInit(&skey[1], Anum_pg_trigger_tgname, BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(trigname));

    tgscan = systable_beginscan(tgrel, TriggerRelidNameIndexId, true, NULL, 2, skey);

    tup = systable_getnext(tgscan);
    if (!HeapTupleIsValid(tup)) {
        if (!missing_ok)
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("trigger \"%s\" for table \"%s\" does not exist", trigname, get_rel_name(relid))));
        oid = InvalidOid;
    } else {
        oid = HeapTupleGetOid(tup);
    }

    systable_endscan(tgscan);
    heap_close(tgrel, AccessShareLock);
    return oid;
}

Oid get_trigger_oid_b(const char* trigname, Oid* reloid, bool missing_ok)
{
    Relation tgrel;
    ScanKeyData skey;
    SysScanDesc tgscan;
    HeapTuple tup;
    Oid oid;
    int count = 0;

    /*
     * Find the trigger, verify permissions, set up object address
     */
    tgrel = heap_open(TriggerRelationId, AccessShareLock);

    ScanKeyInit(&skey, Anum_pg_trigger_tgname, BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(trigname));

    tgscan = systable_beginscan(tgrel, TriggerNameIndexId, true, NULL, 1, &skey);

    while (HeapTupleIsValid(tup = systable_getnext(tgscan))) {
        count ++;
        if (count == 1) {
            Form_pg_trigger pg_trigger = (Form_pg_trigger)GETSTRUCT(tup);
            *reloid = pg_trigger->tgrelid;
            oid = HeapTupleGetOid(tup);
        } else if (count > 1) {
            systable_endscan(tgscan);
            heap_close(tgrel, AccessShareLock);
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("trigger named \"%s\" has more than one trigger, please use drop trigger on syntax", trigname)));
        }
    }
    if (count == 0) {
        if (!missing_ok) {
            systable_endscan(tgscan);
            heap_close(tgrel, AccessShareLock);
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("trigger \"%s\" does not exist", trigname)));
        }
        oid = InvalidOid;
    }

    systable_endscan(tgscan);
    heap_close(tgrel, AccessShareLock);
    return oid;
}
/*
 * Perform permissions and integrity checks before acquiring a relation lock.
 */
static void RangeVarCallbackForRenameTrigger(
    const RangeVar* rv, Oid relid, Oid oldrelid, bool target_is_partition, void* arg)
{
    HeapTuple tuple;
    Form_pg_class form;

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tuple))
        return; /* concurrently dropped */
    form = (Form_pg_class)GETSTRUCT(tuple);
    /* only tables and views can have triggers */
    if (form->relkind != RELKIND_RELATION && form->relkind != RELKIND_VIEW && 
        form->relkind != RELKIND_CONTQUERY)
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("\"%s\" is not a table or view", rv->relname)));

    /* you must own the table or have ALTER ANY TRIGGER to rename one of its triggers */
    bool ownerResult = pg_class_ownercheck(relid, GetUserId());
    bool anyResult = false;
    if (!ownerResult && !IsSysSchema(form->relnamespace)) {
        anyResult = HasSpecAnyPriv(GetUserId(), ALTER_ANY_TRIGGER, false);
    }
    if (!ownerResult && !anyResult) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_CLASS, rv->relname);
    }
    if (!g_instance.attr.attr_common.allowSystemTableMods && !u_sess->attr.attr_common.IsInplaceUpgrade &&
        IsSystemClass(form))
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied: \"%s\" is a system catalog", rv->relname)));

    ReleaseSysCache(tuple);
}

/*
 *		renametrig		- changes the name of a trigger on a relation
 *
 *		trigger name is changed in trigger catalog.
 *		No record of the previous name is kept.
 *
 *		get proper relrelation from relation catalog (if not arg)
 *		scan trigger catalog
 *				for name conflict (within rel)
 *				for original trigger (if not arg)
 *		modify tgname in trigger tuple
 *		update row in catalog
 */
ObjectAddress renametrig(RenameStmt* stmt)
{
    Relation targetrel;
    Relation tgrel;
    HeapTuple tuple;
    SysScanDesc tgscan;
    ScanKeyData key[2];
    Oid relid;
    ObjectAddress address;
    Oid      tgoid = InvalidOid;    
    /*
     * Look up name, check permissions, and acquire lock (which we will NOT
     * release until end of transaction).
     */
    relid = RangeVarGetRelidExtended(
        stmt->relation, AccessExclusiveLock, false, false, false, false, RangeVarCallbackForRenameTrigger, NULL);

    TrForbidAccessRbObject(RelationRelationId, relid, stmt->relation->relname);

    /* Have lock already, so just need to build relcache entry. */
    targetrel = relation_open(relid, NoLock);

    if (u_sess->attr.attr_sql.sql_compatibility == B_FORMAT) {
        RangeVar* trigname = (RangeVar*)lfirst(list_head(stmt->renameTargetList));
        if (trigname->schemaname != NULL) {
            Oid relNamespaceId = RelationGetNamespace(targetrel);
            if (relNamespaceId != get_namespace_oid(trigname->schemaname, false)) {
                ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("trigger in wrong schema: \"%s\".\"%s\"", trigname->schemaname, stmt->subname)));
            }
        }
    }

    /*
     * Scan pg_trigger twice for existing triggers on relation.  We do this in
     * order to ensure a trigger does not exist with newname (The unique index
     * on tgrelid/tgname would complain anyway) and to ensure a trigger does
     * exist with oldname.
     *
     * NOTE that this is cool only because we have AccessExclusiveLock on the
     * relation, so the trigger set won't be changing underneath us.
     */
    tgrel = heap_open(TriggerRelationId, RowExclusiveLock);

    /*
     * First pass -- look for name conflict
     */
    ScanKeyInit(&key[0], Anum_pg_trigger_tgrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));
    ScanKeyInit(&key[1], Anum_pg_trigger_tgname, BTEqualStrategyNumber, F_NAMEEQ, PointerGetDatum(stmt->newname));
    tgscan = systable_beginscan(tgrel, TriggerRelidNameIndexId, true, NULL, 2, key);
    if (HeapTupleIsValid(tuple = systable_getnext(tgscan)))
        ereport(ERROR,
            (errcode(ERRCODE_DUPLICATE_OBJECT),
                errmsg("trigger \"%s\" for relation \"%s\" already exists",
                    stmt->newname,
                    RelationGetRelationName(targetrel))));
    systable_endscan(tgscan);

    /*
     * Second pass -- look for trigger existing with oldname and update
     */
    ScanKeyInit(&key[0], Anum_pg_trigger_tgrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));
    ScanKeyInit(&key[1], Anum_pg_trigger_tgname, BTEqualStrategyNumber, F_NAMEEQ, PointerGetDatum(stmt->subname));
    tgscan = systable_beginscan(tgrel, TriggerRelidNameIndexId, true, NULL, 2, key);
    if (HeapTupleIsValid(tuple = systable_getnext(tgscan))) {
        /*
         * Update pg_trigger tuple with new tgname.
         */
        tuple = (HeapTuple)tableam_tops_copy_tuple(tuple); /* need a modifiable copy */
        tgoid = HeapTupleGetOid(tuple);
        (void)namestrcpy(&((Form_pg_trigger)GETSTRUCT(tuple))->tgname, stmt->newname);

        simple_heap_update(tgrel, &tuple->t_self, tuple);

        /* keep system catalog indexes current */
        CatalogUpdateIndexes(tgrel, tuple);

        /*
         * Invalidate relation's relcache entry so that other backends (and
         * this one too!) are sent SI message to make them rebuild relcache
         * entries.  (Ideally this should happen automatically...)
         */
        CacheInvalidateRelcache(targetrel);
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("trigger \"%s\" for table \"%s\" does not exist",
                    stmt->subname,
                    RelationGetRelationName(targetrel))));
    }

    ObjectAddressSet(address, TriggerRelationId, tgoid);
    systable_endscan(tgscan);

    heap_close(tgrel, RowExclusiveLock);

    /*
     * Close rel, but keep exclusive lock!
     */
    relation_close(targetrel, NoLock);
    return address;
}

/*
 * EnableDisableTrigger
 *
 *	Called by ALTER TABLE ENABLE/DISABLE [ REPLICA | ALWAYS ] TRIGGER
 *	to change 'tgenabled' field for the specified trigger(s)
 *
 * rel: relation to process (caller must hold suitable lock on it)
 * tgname: trigger to process, or NULL to scan all triggers
 * fires_when: new value for tgenabled field. In addition to generic
 *			   enablement/disablement, this also defines when the trigger
 *			   should be fired in session replication roles.
 * skip_system: if true, skip "system" triggers (constraint triggers)
 *
 * Caller should have checked permissions for the table; here we also
 * enforce that superuser privilege is required to alter the state of
 * system triggers
 */
void EnableDisableTrigger(Relation rel, const char* tgname, char fires_when, bool skip_system)
{
    Relation tgrel;
    int nkeys;
    ScanKeyData keys[2];
    SysScanDesc tgscan;
    HeapTuple tuple;
    bool found = false;
    bool changed = false;

    /* Scan the relevant entries in pg_triggers */
    tgrel = heap_open(TriggerRelationId, RowExclusiveLock);

    ScanKeyInit(
        &keys[0], Anum_pg_trigger_tgrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(RelationGetRelid(rel)));
    if (tgname != NULL) {
        ScanKeyInit(&keys[1], Anum_pg_trigger_tgname, BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(tgname));
        nkeys = 2;
    } else
        nkeys = 1;

    tgscan = systable_beginscan(tgrel, TriggerRelidNameIndexId, true, NULL, nkeys, keys);

    found = changed = false;

    while (HeapTupleIsValid(tuple = systable_getnext(tgscan))) {
        Form_pg_trigger oldtrig = (Form_pg_trigger)GETSTRUCT(tuple);
        if (oldtrig->tgisinternal) {
            /* system trigger ... ok to process? */
            if (skip_system)
                continue;
            if (!superuser())
                ereport(ERROR,
                    (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        errmsg("permission denied: \"%s\" is a system trigger", NameStr(oldtrig->tgname))));
        }

        found = true;

        if (oldtrig->tgenabled != fires_when) {
            /* need to change this one ... make a copy to scribble on */
            HeapTuple newtup = (HeapTuple)tableam_tops_copy_tuple(tuple);
            Form_pg_trigger newtrig = (Form_pg_trigger)GETSTRUCT(newtup);

            newtrig->tgenabled = fires_when;

            simple_heap_update(tgrel, &newtup->t_self, newtup);

            /* Keep catalog indexes current */
            CatalogUpdateIndexes(tgrel, newtup);

            tableam_tops_free_tuple(newtup);

            changed = true;
        }
    }

    systable_endscan(tgscan);

    heap_close(tgrel, RowExclusiveLock);

    if (tgname && !found)
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("trigger \"%s\" for table \"%s\" does not exist", tgname, RelationGetRelationName(rel))));

    /*
     * If we changed anything, broadcast a SI inval message to force each
     * backend (including our own!) to rebuild relation's relcache entry.
     * Otherwise they will fail to apply the change promptly.
     */
    if (changed)
        CacheInvalidateRelcache(rel);
}

/*
 * Build trigger data to attach to the given relcache entry.
 *
 * Note that trigger data attached to a relcache entry must be stored in
 * u_sess->cache_mem_cxt to ensure it survives as long as the relcache entry.
 * But we should be running in a less long-lived working context.  To avoid
 * leaking cache memory if this routine fails partway through, we build a
 * temporary TriggerDesc in working memory and then copy the completed
 * structure into cache memory.
 */
void RelationBuildTriggers(Relation relation)
{
    TriggerDesc* trigdesc = NULL;
    int numtrigs;
    int maxtrigs = 16;
    Trigger* triggers = NULL;
    Relation tgrel;
    ScanKeyData skey;
    SysScanDesc tgscan;
    HeapTuple htup;
    MemoryContext oldContext;
    int i;

    /*
     * Allocate a working array to hold the triggers (the array is extended if
     * necessary)
     */
    triggers = (Trigger*)palloc(maxtrigs * sizeof(Trigger));
    numtrigs = 0;

    /*
     * Note: since we scan the triggers using TriggerRelidNameIndexId, we will
     * be reading the triggers in name order, except possibly during
     * emergency-recovery operations (ie, u_sess->attr.attr_common.IgnoreSystemIndexes). This in turn
     * ensures that triggers will be fired in name order.
     */
    ScanKeyInit(
        &skey, Anum_pg_trigger_tgrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(RelationGetRelid(relation)));

    tgrel = heap_open(TriggerRelationId, AccessShareLock);
    tgscan = systable_beginscan(tgrel, TriggerRelidNameIndexId, true, NULL, 1, &skey);

    struct TriggerNameInfo {
        char* follows_name;
        char* precedes_name;
        char* trg_name;
        char* time; 
    };
    TriggerNameInfo* tgNameInfos = NULL;
    bool is_has_follows = false;
    if (u_sess->attr.attr_sql.sql_compatibility == B_FORMAT) {
        tgNameInfos = (TriggerNameInfo*)palloc(maxtrigs*sizeof(TriggerNameInfo));
    }

    while (HeapTupleIsValid(htup = systable_getnext(tgscan))) {
        Form_pg_trigger pg_trigger = (Form_pg_trigger)GETSTRUCT(htup);
        Trigger* build = NULL;
        Datum datum;
        bool isnull = false;

        if (numtrigs >= maxtrigs) {
            maxtrigs *= 2;
            triggers = (Trigger*)repalloc(triggers, maxtrigs * sizeof(Trigger));
            if (DB_IS_CMPT(B_FORMAT)) {
                tgNameInfos = (TriggerNameInfo *)repalloc(tgNameInfos, maxtrigs * sizeof(TriggerNameInfo));
            }
        }
        if (u_sess->attr.attr_sql.sql_compatibility == B_FORMAT) {
            TriggerNameInfo *tgNameInfo = &(tgNameInfos[numtrigs]);

            char* trigname = DatumGetCString(DirectFunctionCall1(nameout, NameGetDatum(&pg_trigger->tgname)));
            char* tgordername = DatumGetCString(fastgetattr(htup, Anum_pg_trigger_tgordername, tgrel->rd_att, &isnull));
            char* tgorder = DatumGetCString(fastgetattr(htup, Anum_pg_trigger_tgorder, tgrel->rd_att, &isnull));
            if (!isnull && strcmp(tgorder, "follows") == 0) {
                tgNameInfo->follows_name = tgordername;
                tgNameInfo->precedes_name = NULL;
                is_has_follows = true;
            }
            else if (!isnull && strcmp(tgorder, "precedes") == 0) {
                tgNameInfo->follows_name = NULL;
                tgNameInfo->precedes_name = tgordername;
                is_has_follows = true;
            }
            else {
                tgNameInfo->follows_name = NULL;
                tgNameInfo->precedes_name = NULL;
            }
            char* tgtime = DatumGetCString(fastgetattr(htup, Anum_pg_trigger_tgtime, tgrel->rd_att, &isnull));
            tgNameInfo->trg_name = trigname;
            tgNameInfo->time = isnull ? NULL:tgtime;
        }
        build = &(triggers[numtrigs]);
        build->tgoid = HeapTupleGetOid(htup);
        build->tgname = DatumGetCString(DirectFunctionCall1(nameout, NameGetDatum(&pg_trigger->tgname)));
        build->tgfoid = pg_trigger->tgfoid;
        build->tgtype = pg_trigger->tgtype;
        build->tgenabled = pg_trigger->tgenabled;
        build->tgisinternal = pg_trigger->tgisinternal;
        build->tgconstrrelid = pg_trigger->tgconstrrelid;
        build->tgconstrindid = pg_trigger->tgconstrindid;
        build->tgconstraint = pg_trigger->tgconstraint;
        build->tgdeferrable = pg_trigger->tgdeferrable;
        build->tginitdeferred = pg_trigger->tginitdeferred;
        build->tgnargs = pg_trigger->tgnargs;
        /* tgattr is first var-width field, so OK to access directly */
        build->tgnattr = pg_trigger->tgattr.dim1;
        if (build->tgnattr > 0) {
            errno_t rc = EOK;
            build->tgattr = (int2*)palloc(build->tgnattr * sizeof(int2));
            rc = memcpy_s(build->tgattr,
                build->tgnattr * sizeof(int2),
                &(pg_trigger->tgattr.values),
                build->tgnattr * sizeof(int2));
            securec_check(rc, "", "");
        } else
            build->tgattr = NULL;
        if (build->tgnargs > 0) {
            bytea* val = NULL;
            char* p = NULL;

            val = DatumGetByteaP(fastgetattr(htup, Anum_pg_trigger_tgargs, tgrel->rd_att, &isnull));
            if (isnull)
                ereport(ERROR,
                    (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmsg("tgargs is null in trigger for relation \"%s\"", RelationGetRelationName(relation))));
            p = (char*)VARDATA(val);
            build->tgargs = (char**)palloc(build->tgnargs * sizeof(char*));
            for (i = 0; i < build->tgnargs; i++) {
                build->tgargs[i] = pstrdup(p);
                p += strlen(p) + 1;
            }
        } else
            build->tgargs = NULL;
        datum = fastgetattr(htup, Anum_pg_trigger_tgqual, tgrel->rd_att, &isnull);
        if (!isnull)
            build->tgqual = TextDatumGetCString(datum);
        else
            build->tgqual = NULL;

        datum = fastgetattr(htup, Anum_pg_trigger_tgowner, tgrel->rd_att, &isnull);
        if (!isnull) {
            build->tgowner = DatumGetObjectId(datum);
        } else {
            build->tgowner = InvalidTgOwnerId;
        }

        numtrigs++;
    }

    if (u_sess->attr.attr_sql.sql_compatibility == B_FORMAT) {
        int n = numtrigs;
        int gap = n;
        while (gap > 1) {
            gap = gap/2;
            int i;
            for (i = 0;i < n - gap; i++) {
                int end = i;
                char* tmp = tgNameInfos[end+gap].time;
                TriggerNameInfo temp = tgNameInfos[end+gap];
                Trigger tgtmp = triggers[end+gap];
                while (end >= 0) {
                    if (tmp != NULL && tgNameInfos[end].time != NULL && strcmp(tmp, tgNameInfos[end].time)<0) {
                        tgNameInfos[end+gap] = tgNameInfos[end];
                        triggers[end+gap] = triggers[end];
                        end -= gap;
                    }
                    else 
                        break;
                }
                tgNameInfos[end+gap] = temp;
                triggers[end+gap] = tgtmp;
            }
        }
        if (is_has_follows) {
            for (int i = 0; i < numtrigs -1;i++) {
                int end = i;
                TriggerNameInfo temp = tgNameInfos[end+1];
                Trigger tgtmp = triggers[end+1];
                char* find_name = NULL;
                bool is_follows = false;
                if (temp.follows_name != NULL) {
                    is_follows = true;
                    find_name = temp.follows_name;
                }
                else if (temp.precedes_name != NULL) {
                    find_name = temp.precedes_name;
                }
                else
                    continue;
                while (end >= 0) {
                    if (strcmp(tgNameInfos[end].trg_name, find_name)!=0) {
                        tgNameInfos[end+1] = tgNameInfos[end];
                        triggers[end+1] = triggers[end];
                        end--;
                    }
                    else {
                        if(is_follows == false)
                        {
                            tgNameInfos[end+1] = tgNameInfos[end];
                            triggers[end+1] = triggers[end];
                            end--;
                        }
                        break;
                    }
                }
                tgNameInfos[end+1] = temp;
                triggers[end+1] = tgtmp;
            }
        }
    }
    if (tgNameInfos != NULL)
    {
        pfree_ext(tgNameInfos);
    }
    systable_endscan(tgscan);
    heap_close(tgrel, AccessShareLock);

    /* There might not be any triggers */
    if (numtrigs == 0) {
        pfree_ext(triggers);
        return;
    }

    /* Build trigdesc */
    trigdesc = (TriggerDesc*)palloc0(sizeof(TriggerDesc));
    trigdesc->triggers = triggers;
    trigdesc->numtriggers = numtrigs;
    for (i = 0; i < numtrigs; i++)
        SetTriggerFlags(trigdesc, &(triggers[i]));

    /* Copy completed trigdesc into cache storage */
    oldContext = MemoryContextSwitchTo(LocalMyDBCacheMemCxt());
    relation->trigdesc = CopyTriggerDesc(trigdesc);
    (void)MemoryContextSwitchTo(oldContext);

    /* Release working memory */
    FreeTriggerDesc(trigdesc);
}

/*
 * Update the TriggerDesc's hint flags to include the specified trigger
 */
static void SetTriggerFlags(TriggerDesc* trigdesc, const Trigger* trigger)
{
    uint16 tgtype = trigger->tgtype;

    if (TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_ROW, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_INSERT))
        trigdesc->trig_insert_before_row = true;
    if (TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_ROW, TRIGGER_TYPE_AFTER, TRIGGER_TYPE_INSERT))
        trigdesc->trig_insert_after_row = true;
    if (TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_ROW, TRIGGER_TYPE_INSTEAD, TRIGGER_TYPE_INSERT))
        trigdesc->trig_insert_instead_row = true;
    if (TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_INSERT))
        trigdesc->trig_insert_before_statement = true;
    if (TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_AFTER, TRIGGER_TYPE_INSERT))
        trigdesc->trig_insert_after_statement = true;
    if (TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_ROW, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_UPDATE))
        trigdesc->trig_update_before_row = true;
    if (TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_ROW, TRIGGER_TYPE_AFTER, TRIGGER_TYPE_UPDATE))
        trigdesc->trig_update_after_row = true;
    if (TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_ROW, TRIGGER_TYPE_INSTEAD, TRIGGER_TYPE_UPDATE))
        trigdesc->trig_update_instead_row = true;
    if (TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_UPDATE))
        trigdesc->trig_update_before_statement = true;
    if (TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_AFTER, TRIGGER_TYPE_UPDATE))
        trigdesc->trig_update_after_statement = true;
    if (TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_ROW, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_DELETE))
        trigdesc->trig_delete_before_row = true;
    if (TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_ROW, TRIGGER_TYPE_AFTER, TRIGGER_TYPE_DELETE))
        trigdesc->trig_delete_after_row = true;
    if (TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_ROW, TRIGGER_TYPE_INSTEAD, TRIGGER_TYPE_DELETE))
        trigdesc->trig_delete_instead_row = true;
    if (TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_DELETE))
        trigdesc->trig_delete_before_statement = true;
    if (TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_AFTER, TRIGGER_TYPE_DELETE))
        trigdesc->trig_delete_after_statement = true;
    /* there are no row-level truncate triggers */
    if (TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_TRUNCATE))
        trigdesc->trig_truncate_before_statement = true;
    if (TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_AFTER, TRIGGER_TYPE_TRUNCATE))
        trigdesc->trig_truncate_after_statement = true;
}

/*
 * Copy a TriggerDesc data structure.
 *
 * The copy is allocated in the current memory context.
 */
TriggerDesc* CopyTriggerDesc(TriggerDesc* trigdesc)
{
    TriggerDesc* newdesc = NULL;
    Trigger* trigger = NULL;
    int i;

    if (trigdesc == NULL || trigdesc->numtriggers <= 0)
        return NULL;

    newdesc = (TriggerDesc*)palloc(sizeof(TriggerDesc));
    errno_t rc = memcpy_s(newdesc, sizeof(TriggerDesc), trigdesc, sizeof(TriggerDesc));
    securec_check(rc, "", "");

    trigger = (Trigger*)palloc(trigdesc->numtriggers * sizeof(Trigger));
    rc = memcpy_s(
        trigger, trigdesc->numtriggers * sizeof(Trigger), trigdesc->triggers, trigdesc->numtriggers * sizeof(Trigger));
    securec_check(rc, "", "");
    newdesc->triggers = trigger;

    for (i = 0; i < trigdesc->numtriggers; i++) {
        trigger->tgname = pstrdup(trigger->tgname);
        if (trigger->tgnattr > 0) {
            int2* newattr = NULL;

            newattr = (int2*)palloc(trigger->tgnattr * sizeof(int2));
            rc = memcpy_s(newattr, trigger->tgnattr * sizeof(int2), trigger->tgattr, trigger->tgnattr * sizeof(int2));
            securec_check(rc, "", "");
            trigger->tgattr = newattr;
        }
        if (trigger->tgnargs > 0) {
            char** newargs;
            int16 j;

            newargs = (char**)palloc(trigger->tgnargs * sizeof(char*));
            for (j = 0; j < trigger->tgnargs; j++)
                newargs[j] = pstrdup(trigger->tgargs[j]);
            trigger->tgargs = newargs;
        }
        if (trigger->tgqual)
            trigger->tgqual = pstrdup(trigger->tgqual);
        trigger++;
    }

    return newdesc;
}

/*
 * Free a TriggerDesc data structure.
 */
void FreeTriggerDesc(TriggerDesc* trigdesc)
{
    Trigger* trigger = NULL;
    int i;

    if (trigdesc == NULL)
        return;

    trigger = trigdesc->triggers;
    for (i = 0; i < trigdesc->numtriggers; i++) {
        pfree_ext(trigger->tgname);
        if (trigger->tgnattr > 0)
            pfree_ext(trigger->tgattr);
        if (trigger->tgnargs > 0) {
            while (--(trigger->tgnargs) >= 0)
                pfree_ext(trigger->tgargs[trigger->tgnargs]);
            pfree_ext(trigger->tgargs);
        }
        if (trigger->tgqual)
            pfree_ext(trigger->tgqual);
        trigger++;
    }
    pfree_ext(trigdesc->triggers);
    pfree_ext(trigdesc);
}

/*
 * Compare two TriggerDesc structures for logical equality.
 */
#ifdef NOT_USED
bool equalTriggerDescs(TriggerDesc* trigdesc1, TriggerDesc* trigdesc2)
{
    int i, j;

    /*
     * We need not examine the hint flags, just the trigger array itself; if
     * we have the same triggers with the same types, the flags should match.
     *
     * As of 7.3 we assume trigger set ordering is significant in the
     * comparison; so we just compare corresponding slots of the two sets.
     *
     * Note: comparing the stringToNode forms of the WHEN clauses means that
     * parse column locations will affect the result.  This is okay as long as
     * this function is only used for detecting exact equality, as for example
     * in checking for staleness of a cache entry.
     */
    if (trigdesc1 != NULL) {
        if (trigdesc2 == NULL)
            return false;
        if (trigdesc1->numtriggers != trigdesc2->numtriggers)
            return false;
        for (i = 0; i < trigdesc1->numtriggers; i++) {
            Trigger* trig1 = trigdesc1->triggers + i;
            Trigger* trig2 = trigdesc2->triggers + i;

            if (trig1->tgoid != trig2->tgoid)
                return false;
            if (strcmp(trig1->tgname, trig2->tgname) != 0)
                return false;
            if (trig1->tgfoid != trig2->tgfoid)
                return false;
            if (trig1->tgtype != trig2->tgtype)
                return false;
            if (trig1->tgenabled != trig2->tgenabled)
                return false;
            if (trig1->tgisinternal != trig2->tgisinternal)
                return false;
            if (trig1->tgconstrrelid != trig2->tgconstrrelid)
                return false;
            if (trig1->tgconstrindid != trig2->tgconstrindid)
                return false;
            if (trig1->tgconstraint != trig2->tgconstraint)
                return false;
            if (trig1->tgdeferrable != trig2->tgdeferrable)
                return false;
            if (trig1->tginitdeferred != trig2->tginitdeferred)
                return false;
            if (trig1->tgnargs != trig2->tgnargs)
                return false;
            if (trig1->tgnattr != trig2->tgnattr)
                return false;
            if (trig1->tgnattr > 0 && memcmp(trig1->tgattr, trig2->tgattr, trig1->tgnattr * sizeof(int2)) != 0)
                return false;
            for (j = 0; j < trig1->tgnargs; j++)
                if (strcmp(trig1->tgargs[j], trig2->tgargs[j]) != 0)
                    return false;
            if (trig1->tgqual == NULL && trig2->tgqual == NULL)
                /* ok */;
            else if (trig1->tgqual == NULL || trig2->tgqual == NULL)
                return false;
            else if (strcmp(trig1->tgqual, trig2->tgqual) != 0)
                return false;
        }
    } else if (trigdesc2 != NULL)
        return false;
    return true;
}
#endif /* NOT_USED */

/*
 * Call a trigger function.
 *
 *		trigdata: trigger descriptor.
 *		tgindx: trigger's index in finfo and instr arrays.
 *		finfo: array of cached trigger function call information.
 *		instr: optional array of EXPLAIN ANALYZE instrumentation state.
 *		per_tuple_context: memory context to execute the function in.
 *
 * Returns the tuple (or NULL) as returned by the function.
 */
static HeapTuple ExecCallTriggerFunc(
    TriggerData* trigdata, int tgindx, FmgrInfo* finfo, Instrumentation* instr, MemoryContext per_tuple_context)
{
    FunctionCallInfoData fcinfo;
    PgStat_FunctionCallUsage fcusage;
    Datum result;
    Oid old_uesr = 0;
    Oid save_old_uesr = 0;
    int save_sec_context = 0;
    bool saved_is_allow_commit_rollback = false;
    bool need_reset_err_msg;

    /* Guard against stack overflow due to infinite loop firing trigger. */
    check_stack_depth();

    finfo += tgindx;

    /*
     * We cache fmgr lookup info, to avoid making the lookup again on each
     * call.
     */
    if (finfo->fn_oid == InvalidOid)
        fmgr_info(trigdata->tg_trigger->tgfoid, finfo);

    Assert(finfo->fn_oid == trigdata->tg_trigger->tgfoid);

    stp_set_commit_rollback_err_msg(STP_XACT_IN_TRIGGER);

    need_reset_err_msg = stp_disable_xact_and_set_err_msg(&saved_is_allow_commit_rollback, STP_XACT_IN_TRIGGER);

    /*
     * If doing EXPLAIN ANALYZE, start charging time to this trigger.
     */
    if (instr != NULL)
        InstrStartNode(instr + tgindx);

    /*
     * Do the function evaluation in the per-tuple memory context, so that
     * leaked memory will be reclaimed once per tuple. Note in particular that
     * any new tuple created by the trigger function will live till the end of
     * the tuple cycle.
     */
    MemoryContext oldContext = MemoryContextSwitchTo(per_tuple_context);
    save_old_uesr = GetOldUserId(false);

    /* get trigger owner and make sure current user is trigger owner when execute trigger-func */
    GetUserIdAndSecContext(&old_uesr, &save_sec_context);
    Oid trigger_owner = trigdata->tg_trigger->tgowner;
    if (trigger_owner == InvalidTgOwnerId) {
        ereport(LOG, (errmsg("old system table pg_trigger does not have tgowner column, use old default permission")));
    } else {
        SetOldUserId(old_uesr, false);
        SetUserIdAndSecContext(trigger_owner,
            (int)((uint32)save_sec_context | SECURITY_LOCAL_USERID_CHANGE | SENDER_LOCAL_USERID_CHANGE));
        u_sess->exec_cxt.is_exec_trigger_func = true;
    }
    /*
     * Call the function, passing no arguments but setting a context.
     */
    InitFunctionCallInfoData(fcinfo, finfo, 0, InvalidOid, (Node*)trigdata, NULL);

    pgstat_init_function_usage(&fcinfo, &fcusage);

    u_sess->tri_cxt.MyTriggerDepth++;
    PG_TRY();
    {
        result = FunctionCallInvoke(&fcinfo);
    }
    PG_CATCH();
    {
        stp_reset_xact_state_and_err_msg(saved_is_allow_commit_rollback, need_reset_err_msg);
        u_sess->tri_cxt.MyTriggerDepth--;

        SetOldUserId(save_old_uesr, false);
        /* reset current user */
        SetUserIdAndSecContext(old_uesr, save_sec_context);
        PG_RE_THROW();
    }
    PG_END_TRY();

    stp_reset_xact_state_and_err_msg(saved_is_allow_commit_rollback, need_reset_err_msg);
    u_sess->tri_cxt.MyTriggerDepth--;

    pgstat_end_function_usage(&fcusage, true);

    SetOldUserId(save_old_uesr, false);
    /* reset current user */
    SetUserIdAndSecContext(old_uesr, save_sec_context);
    u_sess->exec_cxt.is_exec_trigger_func = false;
    (void)MemoryContextSwitchTo(oldContext);

    /*
     * Trigger protocol allows function to return a null pointer, but NOT to
     * set the isnull result flag.
     */
    if (fcinfo.isnull)
        ereport(ERROR, (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
            errmsg("trigger function %u returned null value", fcinfo.flinfo->fn_oid)));

    /*
     * If doing EXPLAIN ANALYZE, stop charging time to this trigger, and count
     * one "tuple returned" (really the number of firings).
     */
    if (instr != NULL)
        InstrStopNode(instr + tgindx, 1);

    return (HeapTuple)DatumGetPointer(result);
}

void ExecBSInsertTriggers(EState* estate, ResultRelInfo* relinfo)
{
    TriggerDesc* trigdesc = NULL;
    int i;
    TriggerData LocTriggerData;

#ifdef PGXC
    /* Know whether we should fire triggers on this node. */
    if (!pgxc_should_exec_triggers(
            relinfo->ri_RelationDesc, TRIGGER_TYPE_INSERT, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_BEFORE))
        return;
#endif

    trigdesc = relinfo->ri_TrigDesc;

    if (trigdesc == NULL)
        return;
    if (!trigdesc->trig_insert_before_statement)
        return;

    LocTriggerData.type = T_TriggerData;
    LocTriggerData.tg_event = TRIGGER_EVENT_INSERT | TRIGGER_EVENT_BEFORE;
    LocTriggerData.tg_relation = relinfo->ri_RelationDesc;
    LocTriggerData.tg_trigtuple = NULL;
    LocTriggerData.tg_newtuple = NULL;
    LocTriggerData.tg_trigtuplebuf = InvalidBuffer;
    LocTriggerData.tg_newtuplebuf = InvalidBuffer;
    for (i = 0; i < trigdesc->numtriggers; i++) {
        Trigger* trigger = &trigdesc->triggers[i];
        HeapTuple newtuple;

        if (!TRIGGER_TYPE_MATCHES(trigger->tgtype, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_INSERT))
            continue;
        if (!TriggerEnabled(estate, relinfo, trigger, LocTriggerData.tg_event, NULL, NULL, NULL))
            continue;

        LocTriggerData.tg_trigger = trigger;
        newtuple = ExecCallTriggerFunc(&LocTriggerData,
            i,
            relinfo->ri_TrigFunctions,
            relinfo->ri_TrigInstrument,
            GetPerTupleMemoryContext(estate));
        if (newtuple)
            ereport(ERROR,
                (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
                    errmsg("BEFORE STATEMENT trigger cannot return a value")));
    }
}

void ExecASInsertTriggers(EState* estate, ResultRelInfo* relinfo)
{
    TriggerDesc* trigdesc = relinfo->ri_TrigDesc;

    if (trigdesc != NULL && trigdesc->trig_insert_after_statement)
        AfterTriggerSaveEvent(
            estate, relinfo, TRIGGER_EVENT_INSERT, false, InvalidOid, InvalidOid, InvalidBktId, NULL, NULL, NIL, NULL);
}

static inline TriggerDesc* get_and_check_trigdesc_value(TriggerDesc* trigdesc)
{
    if (unlikely(trigdesc == NULL)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("trigdesc should not be null")));
    }
    return trigdesc;
}

TupleTableSlot* ExecBRInsertTriggers(EState* estate, ResultRelInfo* relinfo, TupleTableSlot* slot)
{
    TriggerDesc* trigdesc = get_and_check_trigdesc_value(relinfo->ri_TrigDesc);
    HeapTuple slottuple = ExecMaterializeSlot(slot);
    HeapTuple newtuple = slottuple;
    HeapTuple oldtuple;
    TriggerData LocTriggerData;
    int i;
#ifdef PGXC
    bool exec_all_triggers = false;

    /*
     * Fire triggers only at the node where we are supposed to fire them.
     * Note: the special requirement for BR triggers is that we should fire
     * them on coordinator even when we have shippable BR and a non-shippable AR
     * trigger. For details see the comments in the function definition.
     */
    exec_all_triggers = pgxc_should_exec_br_trigger(relinfo->ri_RelationDesc, TRIGGER_TYPE_INSERT, estate);
#endif

    LocTriggerData.type = T_TriggerData;
    LocTriggerData.tg_event = TRIGGER_EVENT_INSERT | TRIGGER_EVENT_ROW | TRIGGER_EVENT_BEFORE;
    LocTriggerData.tg_relation = relinfo->ri_RelationDesc;
    LocTriggerData.tg_newtuple = NULL;
    LocTriggerData.tg_newtuplebuf = InvalidBuffer;
    for (i = 0; i < trigdesc->numtriggers; i++) {
        Trigger* trigger = &trigdesc->triggers[i];

#ifdef PGXC
        if (!pgxc_is_trigger_firable(relinfo->ri_RelationDesc, trigger, exec_all_triggers))
            continue;
#endif
        if (!TRIGGER_TYPE_MATCHES(trigger->tgtype, TRIGGER_TYPE_ROW, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_INSERT))
            continue;
        if (!TriggerEnabled(estate, relinfo, trigger, LocTriggerData.tg_event, NULL, NULL, newtuple))
            continue;

        LocTriggerData.tg_trigtuple = oldtuple = newtuple;
        LocTriggerData.tg_trigtuplebuf = InvalidBuffer;
        LocTriggerData.tg_trigger = trigger;
        newtuple = ExecCallTriggerFunc(&LocTriggerData,
            i,
            relinfo->ri_TrigFunctions,
            relinfo->ri_TrigInstrument,
            GetPerTupleMemoryContext(estate));
        if (oldtuple != newtuple && oldtuple != slottuple)
            tableam_tops_free_tuple(oldtuple);
        if (newtuple == NULL)
            return NULL; /* "do nothing" */
    }

    if (newtuple != slottuple) {
        /*
         * Return the modified tuple using the es_trig_tuple_slot.	We assume
         * the tuple was allocated in per-tuple memory context, and therefore
         * will go away by itself. The tuple table slot should not try to
         * clear it.
         */
        TupleTableSlot* newslot = estate->es_trig_tuple_slot;
        TupleDesc tupdesc = RelationGetDescr(relinfo->ri_RelationDesc);
        if (newslot->tts_tupleDescriptor != tupdesc)
            ExecSetSlotDescriptor(newslot, tupdesc);
        (void)ExecStoreTuple(newtuple, newslot, InvalidBuffer, false);
        slot = newslot;
    }
    return slot;
}

void ExecARInsertTriggers(EState* estate, ResultRelInfo* relinfo, Oid insertPartition, int2 bucketid,
    HeapTuple trigtuple, List* recheckIndexes)
{
    TriggerDesc* trigdesc = relinfo->ri_TrigDesc;

    if (trigdesc != NULL && trigdesc->trig_insert_after_row)
        AfterTriggerSaveEvent(estate,
            relinfo,
            TRIGGER_EVENT_INSERT,
            true,
            InvalidOid,
            insertPartition,
            bucketid,
            NULL,
            trigtuple,
            recheckIndexes,
            NULL);
}

TupleTableSlot* ExecIRInsertTriggers(EState* estate, ResultRelInfo* relinfo, TupleTableSlot* slot)
{
    TriggerDesc* trigdesc = get_and_check_trigdesc_value(relinfo->ri_TrigDesc);
    HeapTuple slottuple = ExecMaterializeSlot(slot);
    HeapTuple newtuple = slottuple;
    HeapTuple oldtuple;
    TriggerData LocTriggerData;
    int i;
#ifdef PGXC
    bool exec_all_triggers = false;

    /*
     * Know whether we should fire triggers on this node. But since internal
     * triggers are an exception, we cannot bail out here.
     */
    exec_all_triggers = pgxc_should_exec_triggers(
        relinfo->ri_RelationDesc, TRIGGER_TYPE_INSERT, TRIGGER_TYPE_ROW, TRIGGER_TYPE_INSTEAD);
#endif

    LocTriggerData.type = T_TriggerData;
    LocTriggerData.tg_event = TRIGGER_EVENT_INSERT | TRIGGER_EVENT_ROW | TRIGGER_EVENT_INSTEAD;
    LocTriggerData.tg_relation = relinfo->ri_RelationDesc;
    LocTriggerData.tg_newtuple = NULL;
    LocTriggerData.tg_newtuplebuf = InvalidBuffer;
    for (i = 0; i < trigdesc->numtriggers; i++) {
        Trigger* trigger = &trigdesc->triggers[i];

#ifdef PGXC
        if (!pgxc_is_trigger_firable(relinfo->ri_RelationDesc, trigger, exec_all_triggers))
            continue;
#endif
        if (!TRIGGER_TYPE_MATCHES(trigger->tgtype, TRIGGER_TYPE_ROW, TRIGGER_TYPE_INSTEAD, TRIGGER_TYPE_INSERT))
            continue;
        if (!TriggerEnabled(estate, relinfo, trigger, LocTriggerData.tg_event, NULL, NULL, newtuple))
            continue;

        LocTriggerData.tg_trigtuple = oldtuple = newtuple;
        LocTriggerData.tg_trigtuplebuf = InvalidBuffer;
        LocTriggerData.tg_trigger = trigger;
        newtuple = ExecCallTriggerFunc(&LocTriggerData,
            i,
            relinfo->ri_TrigFunctions,
            relinfo->ri_TrigInstrument,
            GetPerTupleMemoryContext(estate));
        if (oldtuple != newtuple && oldtuple != slottuple)
            tableam_tops_free_tuple(oldtuple);
        if (newtuple == NULL)
            return NULL; /* "do nothing" */
    }

    if (newtuple != slottuple) {
        /*
         * Return the modified tuple using the es_trig_tuple_slot.	We assume
         * the tuple was allocated in per-tuple memory context, and therefore
         * will go away by itself. The tuple table slot should not try to
         * clear it.
         */
        TupleTableSlot* newslot = estate->es_trig_tuple_slot;
        TupleDesc tupdesc = RelationGetDescr(relinfo->ri_RelationDesc);

        if (newslot->tts_tupleDescriptor != tupdesc)
            ExecSetSlotDescriptor(newslot, tupdesc);
        (void)ExecStoreTuple(newtuple, newslot, InvalidBuffer, false);
        slot = newslot;
    }
    return slot;
}

void ExecBSDeleteTriggers(EState* estate, ResultRelInfo* relinfo)
{
    TriggerDesc* trigdesc = NULL;
    int i;
    TriggerData LocTriggerData;

#ifdef PGXC
    /* Know whether we should fire these type of triggers on this node */
    if (!pgxc_should_exec_triggers(
            relinfo->ri_RelationDesc, TRIGGER_TYPE_DELETE, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_BEFORE))
        return;
#endif

    trigdesc = relinfo->ri_TrigDesc;

    if (trigdesc == NULL)
        return;
    if (!trigdesc->trig_delete_before_statement)
        return;

    LocTriggerData.type = T_TriggerData;
    LocTriggerData.tg_event = TRIGGER_EVENT_DELETE | TRIGGER_EVENT_BEFORE;
    LocTriggerData.tg_relation = relinfo->ri_RelationDesc;
    LocTriggerData.tg_trigtuple = NULL;
    LocTriggerData.tg_newtuple = NULL;
    LocTriggerData.tg_trigtuplebuf = InvalidBuffer;
    LocTriggerData.tg_newtuplebuf = InvalidBuffer;
    for (i = 0; i < trigdesc->numtriggers; i++) {
        Trigger* trigger = &trigdesc->triggers[i];
        HeapTuple newtuple;

        if (!TRIGGER_TYPE_MATCHES(trigger->tgtype, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_DELETE))
            continue;
        if (!TriggerEnabled(estate, relinfo, trigger, LocTriggerData.tg_event, NULL, NULL, NULL))
            continue;

        LocTriggerData.tg_trigger = trigger;
        newtuple = ExecCallTriggerFunc(&LocTriggerData,
            i,
            relinfo->ri_TrigFunctions,
            relinfo->ri_TrigInstrument,
            GetPerTupleMemoryContext(estate));
        if (newtuple)
            ereport(ERROR,
                (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
                    errmsg("BEFORE STATEMENT trigger cannot return a value")));
    }
}

void ExecASDeleteTriggers(EState* estate, ResultRelInfo* relinfo)
{
    TriggerDesc* trigdesc = relinfo->ri_TrigDesc;

    if (trigdesc != NULL && trigdesc->trig_delete_after_statement)
        AfterTriggerSaveEvent(
            estate, relinfo, TRIGGER_EVENT_DELETE, false, InvalidOid, InvalidOid, InvalidBktId, NULL, NULL, NIL, NULL);
}

bool ExecBRDeleteTriggers(EState* estate, EPQState* epqstate, ResultRelInfo* relinfo, Oid deletePartitionOid,
    int2 bucketid,
#ifdef PGXC
    HeapTupleHeader datanode_tuphead,
#endif
    ItemPointer tupleid)
{
    TriggerDesc* trigdesc = get_and_check_trigdesc_value(relinfo->ri_TrigDesc);
    bool result = true;
    TriggerData LocTriggerData;
    HeapTuple trigtuple;
    HeapTuple newtuple;
    TupleTableSlot* newSlot = NULL;
    int i;
#ifdef PGXC
    bool exec_all_triggers = false;

    /*
     * Fire triggers only at the node where we are supposed to fire them.
     * Note: the special requirement for BR triggers is that we should fire
     * them on coordinator even when we have shippable BR and a non-shippable AR
     * trigger. For details see the comments in the function definition.
     */
    exec_all_triggers = pgxc_should_exec_br_trigger(relinfo->ri_RelationDesc, TRIGGER_TYPE_DELETE, estate);

    /*
     * GetTupleForTrigger() acquires an exclusive row lock on the tuple.
     * So while the trigger function is being executed, no one else writes
     * into this row. For PGXC, we need to do similar thing by:
     * 1. Either explicitly LOCK the row and fetch the latest value by doing:
     *    SELECT * FROM tab WHERE ctid = ctid_value FOR UPDATE
     * OR:
     * 2. Add FOR UPDATE in the SELECT statement in the subplan itself.
     */
    if (IS_PGXC_COORDINATOR && RelationGetLocInfo(relinfo->ri_RelationDesc)) {
        /* No OLD tuple means triggers are to be run on datanode */
        if (!datanode_tuphead)
            return true;
        trigtuple = pgxc_get_trigger_tuple(datanode_tuphead);
    } else {
        /* On datanode, do the usual way */
#endif
        trigtuple = GetTupleForTrigger(estate, epqstate, relinfo, deletePartitionOid,
            bucketid, tupleid, LockTupleExclusive, &newSlot);
#ifdef PGXC
    }
#endif

    if (trigtuple == NULL)
        return false;

    LocTriggerData.type = T_TriggerData;
    LocTriggerData.tg_event = TRIGGER_EVENT_DELETE | TRIGGER_EVENT_ROW | TRIGGER_EVENT_BEFORE;
    LocTriggerData.tg_relation = relinfo->ri_RelationDesc;
    LocTriggerData.tg_newtuple = NULL;
    LocTriggerData.tg_newtuplebuf = InvalidBuffer;
    for (i = 0; i < trigdesc->numtriggers; i++) {
        Trigger* trigger = &trigdesc->triggers[i];
        Assert(trigger);

#ifdef PGXC
        if (!pgxc_is_trigger_firable(relinfo->ri_RelationDesc, trigger, exec_all_triggers))
            continue;
#endif

        if (!TRIGGER_TYPE_MATCHES(trigger->tgtype, TRIGGER_TYPE_ROW, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_DELETE))
            continue;
        if (!TriggerEnabled(estate, relinfo, trigger, LocTriggerData.tg_event, NULL, trigtuple, NULL))
            continue;

        LocTriggerData.tg_trigtuple = trigtuple;
        LocTriggerData.tg_trigtuplebuf = InvalidBuffer;
        LocTriggerData.tg_trigger = trigger;
        newtuple = ExecCallTriggerFunc(&LocTriggerData,
            i,
            relinfo->ri_TrigFunctions,
            relinfo->ri_TrigInstrument,
            GetPerTupleMemoryContext(estate));
        if (newtuple == NULL) {
            result = false; /* tell caller to suppress delete */
            break;
        }
        if (newtuple != trigtuple)
            tableam_tops_free_tuple(newtuple);
    }
    tableam_tops_free_tuple(trigtuple);

    return result;
}

void ExecARDeleteTriggers(EState* estate, ResultRelInfo* relinfo, Oid deletePartitionOid, int2 bucketid,
#ifdef PGXC
    HeapTupleHeader trigtuphead,
#endif
    ItemPointer tupleid)
{
    TriggerDesc* trigdesc = relinfo->ri_TrigDesc;

    if (trigdesc != NULL && trigdesc->trig_delete_after_row) {
#ifdef PGXC
        HeapTuple trigtuple;

        if (IS_PGXC_COORDINATOR && RelationGetLocInfo(relinfo->ri_RelationDesc)) {
            /* No OLD tuple means triggers are to be run on datanode */
            if (!trigtuphead)
                return;
            trigtuple = pgxc_get_trigger_tuple(trigtuphead);
        } else {
            /* Do the usual PG-way for datanode */
#endif
            trigtuple = GetTupleForTrigger(estate, NULL, relinfo, deletePartitionOid,
                bucketid, tupleid, LockTupleExclusive, NULL);
#ifdef PGXC
        }
#endif

        AfterTriggerSaveEvent(estate,
            relinfo,
            TRIGGER_EVENT_DELETE,
            true,
            deletePartitionOid,
            InvalidOid,
            bucketid,
            trigtuple,
            NULL,
            NIL,
            NULL);
        tableam_tops_free_tuple(trigtuple);
    }
}

bool ExecIRDeleteTriggers(EState* estate, ResultRelInfo* relinfo, HeapTuple trigtuple)
{
    TriggerDesc* trigdesc = get_and_check_trigdesc_value(relinfo->ri_TrigDesc);
    TriggerData LocTriggerData;
    HeapTuple rettuple;
    int i;
#ifdef PGXC
    bool exec_all_triggers = false;
    /*
     * Know whether we should fire triggers on this node. But since internal
     * triggers are an exception, we cannot bail out here.
     */
    exec_all_triggers = pgxc_should_exec_triggers(
        relinfo->ri_RelationDesc, TRIGGER_TYPE_DELETE, TRIGGER_TYPE_ROW, TRIGGER_TYPE_INSTEAD);
#endif
    LocTriggerData.type = T_TriggerData;
    LocTriggerData.tg_event = TRIGGER_EVENT_DELETE | TRIGGER_EVENT_ROW | TRIGGER_EVENT_INSTEAD;
    LocTriggerData.tg_relation = relinfo->ri_RelationDesc;
    LocTriggerData.tg_newtuple = NULL;
    LocTriggerData.tg_newtuplebuf = InvalidBuffer;
    for (i = 0; i < trigdesc->numtriggers; i++) {
        Trigger* trigger = &trigdesc->triggers[i];

#ifdef PGXC
        if (!pgxc_is_trigger_firable(relinfo->ri_RelationDesc, trigger, exec_all_triggers))
            continue;
#endif
        if (!TRIGGER_TYPE_MATCHES(trigger->tgtype, TRIGGER_TYPE_ROW, TRIGGER_TYPE_INSTEAD, TRIGGER_TYPE_DELETE))
            continue;
        if (!TriggerEnabled(estate, relinfo, trigger, LocTriggerData.tg_event, NULL, trigtuple, NULL))
            continue;

        LocTriggerData.tg_trigtuple = trigtuple;
        LocTriggerData.tg_trigtuplebuf = InvalidBuffer;
        LocTriggerData.tg_trigger = trigger;
        rettuple = ExecCallTriggerFunc(&LocTriggerData,
            i,
            relinfo->ri_TrigFunctions,
            relinfo->ri_TrigInstrument,
            GetPerTupleMemoryContext(estate));
        if (rettuple == NULL)
            return false; /* Delete was suppressed */
        if (rettuple != trigtuple)
            tableam_tops_free_tuple(rettuple);
    }
    return true;
}

void ExecBSUpdateTriggers(EState* estate, ResultRelInfo* relinfo)
{
    TriggerDesc* trigdesc = NULL;
    int i;
    TriggerData LocTriggerData;
    Bitmapset* updatedCols = NULL;
#ifdef PGXC
    /* Know whether we should fire these type of triggers on this node */
    if (!pgxc_should_exec_triggers(
            relinfo->ri_RelationDesc, TRIGGER_TYPE_UPDATE, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_BEFORE))
        return;
#endif

    trigdesc = relinfo->ri_TrigDesc;

    if (trigdesc == NULL)
        return;
    if (!trigdesc->trig_update_before_statement)
        return;

    updatedCols = GET_ALL_UPDATED_COLUMNS(relinfo, estate);

    LocTriggerData.type = T_TriggerData;
    LocTriggerData.tg_event = TRIGGER_EVENT_UPDATE | TRIGGER_EVENT_BEFORE;
    LocTriggerData.tg_relation = relinfo->ri_RelationDesc;
    LocTriggerData.tg_trigtuple = NULL;
    LocTriggerData.tg_newtuple = NULL;
    LocTriggerData.tg_trigtuplebuf = InvalidBuffer;
    LocTriggerData.tg_newtuplebuf = InvalidBuffer;
    for (i = 0; i < trigdesc->numtriggers; i++) {
        Trigger* trigger = &trigdesc->triggers[i];
        HeapTuple newtuple;

        if (!TRIGGER_TYPE_MATCHES(trigger->tgtype, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_UPDATE))
            continue;
        if (!TriggerEnabled(estate, relinfo, trigger, LocTriggerData.tg_event, updatedCols, NULL, NULL))
            continue;

        LocTriggerData.tg_trigger = trigger;
        newtuple = ExecCallTriggerFunc(&LocTriggerData,
            i,
            relinfo->ri_TrigFunctions,
            relinfo->ri_TrigInstrument,
            GetPerTupleMemoryContext(estate));

        if (newtuple)
            ereport(ERROR,
                (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
                    errmsg("BEFORE STATEMENT trigger cannot return a value")));
    }
}

void ExecASUpdateTriggers(EState* estate, ResultRelInfo* relinfo)
{
    TriggerDesc* trigdesc = relinfo->ri_TrigDesc;

    if (trigdesc != NULL && trigdesc->trig_update_after_statement)
        AfterTriggerSaveEvent(estate,
            relinfo,
            TRIGGER_EVENT_UPDATE,
            false,
            InvalidOid,
            InvalidOid,
            InvalidBktId,
            NULL,
            NULL,
            NIL,
            GET_ALL_UPDATED_COLUMNS(relinfo, estate));
}

TupleTableSlot* ExecBRUpdateTriggers(EState* estate, EPQState* epqstate, ResultRelInfo* relinfo, Oid oldPartitionOid,
    int2 bucketid,
#ifdef PGXC
    HeapTupleHeader datanode_tuphead,
#endif
    ItemPointer tupleid, TupleTableSlot* slot, TM_Result* result, TM_FailureData* tmfd)
{
    TriggerDesc* trigdesc = get_and_check_trigdesc_value(relinfo->ri_TrigDesc);
    HeapTuple slottuple = ExecMaterializeSlot(slot);
    HeapTuple newtuple = slottuple;
    TriggerData LocTriggerData;
    HeapTuple trigtuple;
    HeapTuple oldtuple;
    TupleTableSlot* newSlot = NULL;
    int i;
    Bitmapset* updatedCols = NULL;
    Bitmapset* keyCols = NULL;
    LockTupleMode lockmode;

    /*
     * Compute lock mode to use.  If columns that are part of the key have not
     * been modified, then we can use a weaker lock, allowing for better
     * concurrency.
     */
    updatedCols = GET_ALL_UPDATED_COLUMNS(relinfo, estate);
    keyCols = RelationGetIndexAttrBitmap(relinfo->ri_RelationDesc, INDEX_ATTR_BITMAP_KEY);
#ifndef ENABLE_MULTIPLE_NODES
    if (!bms_overlap(keyCols, updatedCols)) {
        lockmode = LockTupleNoKeyExclusive;
    } else
#endif
    {
        lockmode = LockTupleExclusive;
    }


#ifdef PGXC
    bool exec_all_triggers = false;

    /*
     * Know whether we should fire triggers on this node. But since internal
     * triggers are an exception, we cannot bail out here.
     * Note: the special requirement for BR triggers is that we should fire
     * them on coordinator even when we have shippable BR and a non-shippable AR
     * trigger. For details see the comments in the function definition.
     */
    exec_all_triggers = pgxc_should_exec_br_trigger(relinfo->ri_RelationDesc, TRIGGER_TYPE_UPDATE, estate);

    /*
     * GetTupleForTrigger() acquires an exclusive row lock on the tuple.
     * So while the trigger function is being executed, no one else writes
     * into this row. For PGXC, we need to do similar thing by:
     * 1. Either explicitly LOCK the row and fetch the latest value by doing:
     *    SELECT * FROM tab WHERE ctid = ctid_value FOR UPDATE
     * OR:
     * 2. Add FOR UPDATE in the SELECT statement in the subplan itself.
     */
    if (IS_PGXC_COORDINATOR && RelationGetLocInfo(relinfo->ri_RelationDesc)) {
        /* No OLD tuple means triggers are to be run on datanode */
        if (!datanode_tuphead)
            return slot;
        trigtuple = pgxc_get_trigger_tuple(datanode_tuphead);
    } else {
        /* On datanode, do the usual way */
#endif
        /* get a copy of the on-disk tuple we are planning to update */
        trigtuple = GetTupleForTrigger(estate, epqstate, relinfo, oldPartitionOid,
            bucketid, tupleid, lockmode, &newSlot, result, tmfd);
        if (trigtuple == NULL)
            return NULL; /* cancel the update action */

        /*
         * In READ COMMITTED isolation level it's possible that target tuple was
         * changed due to concurrent update.  In that case we have a raw subplan
         * output tuple in newSlot, and need to run it through the junk filter to
         * produce an insertable tuple.
         *
         * Caution: more than likely, the passed-in slot is the same as the
         * junkfilter's output slot, so we are clobbering the original value of
         * slottuple by doing the filtering.  This is OK since neither we nor our
         * caller have any more interest in the prior contents of that slot.
         */
        if (newSlot != NULL) {
            slot = ExecFilterJunk(relinfo->ri_junkFilter, newSlot);
            slottuple = ExecMaterializeSlot(slot);
            newtuple = slottuple;
        }
#ifdef PGXC
    }
#endif

    LocTriggerData.type = T_TriggerData;
    LocTriggerData.tg_event = TRIGGER_EVENT_UPDATE | TRIGGER_EVENT_ROW | TRIGGER_EVENT_BEFORE;
    LocTriggerData.tg_relation = relinfo->ri_RelationDesc;
    for (i = 0; i < trigdesc->numtriggers; i++) {
        Trigger* trigger = &trigdesc->triggers[i];
#ifdef PGXC
        if (!pgxc_is_trigger_firable(relinfo->ri_RelationDesc, trigger, exec_all_triggers))
            continue;
#endif

        if (!TRIGGER_TYPE_MATCHES(trigger->tgtype, TRIGGER_TYPE_ROW, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_UPDATE))
            continue;
        if (!TriggerEnabled(estate, relinfo, trigger, LocTriggerData.tg_event, updatedCols, trigtuple, newtuple))
            continue;

        LocTriggerData.tg_trigtuple = trigtuple;
        LocTriggerData.tg_newtuple = oldtuple = newtuple;
        LocTriggerData.tg_trigtuplebuf = InvalidBuffer;
        LocTriggerData.tg_newtuplebuf = InvalidBuffer;
        LocTriggerData.tg_trigger = trigger;
        newtuple = ExecCallTriggerFunc(&LocTriggerData,
            i,
            relinfo->ri_TrigFunctions,
            relinfo->ri_TrigInstrument,
            GetPerTupleMemoryContext(estate));
        if (oldtuple != newtuple && oldtuple != slottuple)
            tableam_tops_free_tuple(oldtuple);
        if (newtuple == NULL) {
            tableam_tops_free_tuple(trigtuple);
            return NULL; /* "do nothing" */
        }
    }
    tableam_tops_free_tuple(trigtuple);

    if (newtuple != slottuple) {
        /*
         * Return the modified tuple using the es_trig_tuple_slot.	We assume
         * the tuple was allocated in per-tuple memory context, and therefore
         * will go away by itself. The tuple table slot should not try to
         * clear it.
         */
        TupleTableSlot* newslot = estate->es_trig_tuple_slot;
        TupleDesc tupdesc = RelationGetDescr(relinfo->ri_RelationDesc);

        if (newslot->tts_tupleDescriptor != tupdesc) {
            ExecSetSlotDescriptor(newslot, tupdesc);
        }
        (void)ExecStoreTuple(newtuple, newslot, InvalidBuffer, false);
        slot = newslot;
    }
    return slot;
}

void ExecARUpdateTriggers(EState* estate, ResultRelInfo* relinfo, Oid oldPartitionOid, int2 bucketid,
    Oid newPartitionOid, ItemPointer tupleid, HeapTuple newtuple,
#ifdef PGXC
    HeapTupleHeader trigtuphead,
#endif
    List* recheckIndexes)
{
    TriggerDesc* trigdesc = relinfo->ri_TrigDesc;

    if (trigdesc != NULL && trigdesc->trig_update_after_row) {
#ifdef PGXC
        HeapTuple trigtuple;
        if (IS_PGXC_COORDINATOR && RelationGetLocInfo(relinfo->ri_RelationDesc)) {
            /* No OLD tuple means triggers are to be run on datanode */
            if (!trigtuphead)
                return;
            trigtuple = pgxc_get_trigger_tuple(trigtuphead);
        } else {
            /* Do the usual PG-way for datanode */
#endif
            trigtuple = GetTupleForTrigger(estate, NULL, relinfo, oldPartitionOid,
                bucketid, tupleid, LockTupleExclusive, NULL);
#ifdef PGXC
        }
#endif
        /* If we get there, trigtuple must be astore type, but newtuple could be ustore type.
        To avoid potential errors in subsequent column value resolution, 
        we convert newtuple to astore type.*/
        if (newtuple->tupTableType == UHEAP_TUPLE) {
            Relation rel = relinfo->ri_RelationDesc;
            TupleDesc tupdesc = RelationGetDescr(rel);
            newtuple = (HeapTuple)UHeapToHeap(tupdesc, (UHeapTuple)newtuple);
        }
        AfterTriggerSaveEvent(estate,
            relinfo,
            TRIGGER_EVENT_UPDATE,
            true,
            oldPartitionOid,
            newPartitionOid,
            bucketid,
            trigtuple,
            newtuple,
            recheckIndexes,
            GET_ALL_UPDATED_COLUMNS(relinfo, estate));
        tableam_tops_free_tuple(trigtuple);
    }
}

TupleTableSlot* ExecIRUpdateTriggers(EState* estate, ResultRelInfo* relinfo, HeapTuple trigtuple, TupleTableSlot* slot)
{
    TriggerDesc* trigdesc = get_and_check_trigdesc_value(relinfo->ri_TrigDesc);
    HeapTuple slottuple = ExecMaterializeSlot(slot);
    HeapTuple newtuple = slottuple;
    TriggerData LocTriggerData;
    HeapTuple oldtuple;
    int i;
#ifdef PGXC
    bool exec_all_triggers = false;
    /*
     * Know whether we should fire triggers on this node. But since internal
     * triggers are an exception, we cannot bail out here.
     */
    exec_all_triggers = pgxc_should_exec_triggers(
        relinfo->ri_RelationDesc, TRIGGER_TYPE_UPDATE, TRIGGER_TYPE_ROW, TRIGGER_TYPE_INSTEAD);
#endif

    LocTriggerData.type = T_TriggerData;
    LocTriggerData.tg_event = TRIGGER_EVENT_UPDATE | TRIGGER_EVENT_ROW | TRIGGER_EVENT_INSTEAD;
    LocTriggerData.tg_relation = relinfo->ri_RelationDesc;
    for (i = 0; i < trigdesc->numtriggers; i++) {
        Trigger* trigger = &trigdesc->triggers[i];
#ifdef PGXC
        if (!pgxc_is_trigger_firable(relinfo->ri_RelationDesc, trigger, exec_all_triggers))
            continue;
#endif

        if (!TRIGGER_TYPE_MATCHES(trigger->tgtype, TRIGGER_TYPE_ROW, TRIGGER_TYPE_INSTEAD, TRIGGER_TYPE_UPDATE))
            continue;
        if (!TriggerEnabled(estate, relinfo, trigger, LocTriggerData.tg_event, NULL, trigtuple, newtuple))
            continue;

        LocTriggerData.tg_trigtuple = trigtuple;
        LocTriggerData.tg_newtuple = oldtuple = newtuple;
        LocTriggerData.tg_trigtuplebuf = InvalidBuffer;
        LocTriggerData.tg_newtuplebuf = InvalidBuffer;
        LocTriggerData.tg_trigger = trigger;
        newtuple = ExecCallTriggerFunc(&LocTriggerData,
            i,
            relinfo->ri_TrigFunctions,
            relinfo->ri_TrigInstrument,
            GetPerTupleMemoryContext(estate));
        if (oldtuple != newtuple && oldtuple != slottuple)
            tableam_tops_free_tuple(oldtuple);
        if (newtuple == NULL)
            return NULL; /* "do nothing" */
    }

    if (newtuple != slottuple) {
        /*
         * Return the modified tuple using the es_trig_tuple_slot.	We assume
         * the tuple was allocated in per-tuple memory context, and therefore
         * will go away by itself. The tuple table slot should not try to
         * clear it.
         */
        TupleTableSlot* newslot = estate->es_trig_tuple_slot;
        TupleDesc tupdesc = RelationGetDescr(relinfo->ri_RelationDesc);
        if (newslot->tts_tupleDescriptor != tupdesc) {
            ExecSetSlotDescriptor(newslot, tupdesc);
        }
        (void)ExecStoreTuple(newtuple, newslot, InvalidBuffer, false);
        slot = newslot;
    }
    return slot;
}

void ExecBSTruncateTriggers(EState* estate, ResultRelInfo* relinfo)
{
    TriggerDesc* trigdesc = NULL;
    int i;
    TriggerData LocTriggerData;
#ifdef PGXC
    /* Know whether we should fire these type of triggers on this node */
    if (!pgxc_should_exec_triggers(
            relinfo->ri_RelationDesc, TRIGGER_TYPE_TRUNCATE, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_BEFORE))
        return;
#endif

    trigdesc = relinfo->ri_TrigDesc;

    if (trigdesc == NULL)
        return;
    if (!trigdesc->trig_truncate_before_statement)
        return;

    LocTriggerData.type = T_TriggerData;
    LocTriggerData.tg_event = TRIGGER_EVENT_TRUNCATE | TRIGGER_EVENT_BEFORE;
    LocTriggerData.tg_relation = relinfo->ri_RelationDesc;
    LocTriggerData.tg_trigtuple = NULL;
    LocTriggerData.tg_newtuple = NULL;
    LocTriggerData.tg_trigtuplebuf = InvalidBuffer;
    LocTriggerData.tg_newtuplebuf = InvalidBuffer;
    for (i = 0; i < trigdesc->numtriggers; i++) {
        Trigger* trigger = &trigdesc->triggers[i];
        HeapTuple newtuple;

        if (!TRIGGER_TYPE_MATCHES(trigger->tgtype, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_TRUNCATE))
            continue;
        if (!TriggerEnabled(estate, relinfo, trigger, LocTriggerData.tg_event, NULL, NULL, NULL))
            continue;

        LocTriggerData.tg_trigger = trigger;
        newtuple = ExecCallTriggerFunc(&LocTriggerData,
            i,
            relinfo->ri_TrigFunctions,
            relinfo->ri_TrigInstrument,
            GetPerTupleMemoryContext(estate));
        if (newtuple)
            ereport(ERROR,
                (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
                    errmsg("BEFORE STATEMENT trigger cannot return a value")));
    }
}

void ExecASTruncateTriggers(EState* estate, ResultRelInfo* relinfo)
{
    TriggerDesc* trigdesc = relinfo->ri_TrigDesc;

    if (trigdesc != NULL && trigdesc->trig_truncate_after_statement)
        AfterTriggerSaveEvent(estate,
            relinfo,
            TRIGGER_EVENT_TRUNCATE,
            false,
            InvalidOid,
            InvalidOid,
            InvalidBktId,
            NULL,
            NULL,
            NIL,
            NULL);
}

HeapTuple GetTupleForTrigger(EState* estate, EPQState* epqstate, ResultRelInfo* relinfo, Oid targetPartitionOid,
    int2 bucketid, ItemPointer tid, LockTupleMode lockmode, TupleTableSlot** newSlot, TM_Result* tmresultp, TM_FailureData* tmfdp)
{
    Relation relation = relinfo->ri_RelationDesc;
    HeapTupleData tuple;
    HeapTuple result = NULL;
    Buffer buffer;

    Partition part = NULL;
    Relation fakeRelation = relinfo->ri_RelationDesc;

    if (RELATION_IS_PARTITIONED(relation)) {
        Assert(OidIsValid(targetPartitionOid));
        Assert(!RELATION_OWN_BUCKET(relation) || bucketid != InvalidBktId);
        part = partitionOpen(relation, targetPartitionOid, NoLock, bucketid);
        fakeRelation = partitionGetRelation(relation, part);
    } else if (RELATION_OWN_BUCKET(relation)) {
        fakeRelation = bucketGetRelation(relation, NULL, bucketid);
    }

    if (RelationIsUstoreFormat(relation)) {
        TupleTableSlot *slot = MakeSingleTupleTableSlot(relation->rd_att, false, TableAmUstore);
        UHeapTuple utuple;

        UHeapTupleData uheaptupdata;
        utuple = &uheaptupdata;
        union {
            UHeapDiskTupleData hdr;
            char data[MaxPossibleUHeapTupleSize + sizeof(UHeapDiskTupleData)];
        } tbuf;

        errno_t errorNo = EOK;
        errorNo = memset_s(&tbuf, sizeof(tbuf), 0, sizeof(tbuf));
        securec_check(errorNo, "\0", "\0");

        utuple->disk_tuple = &tbuf.hdr;
        utuple->ctid = *tid;


        ExecSetSlotDescriptor(slot, relation->rd_att);
        if (newSlot != NULL) {
            TM_Result inplacetest;
            TM_FailureData tmfd;

            *newSlot = NULL;

            /* caller must pass an epqstate if EvalPlanQual is possible */
            Assert(epqstate != NULL);

            /*
             * lock inplacetuple for update
             */
            inplacetest = tableam_tuple_lock(RELATION_IS_PARTITIONED(relation) ? fakeRelation : relation, utuple,
                &buffer, estate->es_output_cid, LockTupleExclusive, LockWaitBlock, &tmfd, false, false, false,
                estate->es_snapshot, tid, false);

            switch (inplacetest) {
                case TM_SelfUpdated:
                case TM_SelfModified:

                    /*
                     * The target tuple was already updated or deleted by the
                     * current command, or by a later command in the current
                     * transaction.  We ignore the tuple in the former case, and
                     * throw error in the latter case, for the same reasons
                     * enumerated in ExecUpdate and ExecDelete in
                     * nodeModifyTable.c.
                     */
                    if (tmfd.cmax != estate->es_output_cid)
                        ereport(ERROR, (errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
                            errmsg("tuple to be updated was already modified by an operation triggered by the current "
                                   "command"),
                            errhint("Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes "
                                    "to other rows.")));

                    /* treat it as deleted; do not process */
                    return NULL;

                case TM_Ok:
                    *newSlot = NULL;
                    ExecStoreTuple(utuple, slot, InvalidBuffer, false);
                    result = ExecCopySlotTuple(slot);
                    ReleaseBuffer(buffer);

                    break;

                case TM_Updated:
                    if (IsolationUsesXactSnapshot())
                        ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                            errmsg("could not serialize access due to concurrent update")));
                    elog(ERROR, "unexpected table_tuple_lock status: %u", inplacetest);
                    break;

                case TM_Deleted:
                    if (IsolationUsesXactSnapshot())
                        ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                            errmsg("could not serialize access due to concurrent delete")));
                    /* tuple was deleted */
                    return NULL;

                case TM_Invisible:
                    elog(ERROR, "attempted to lock invisible tuple");
                    break;

                default:
                    elog(ERROR, "unrecognized table_tuple_lock status: %u", inplacetest);
                    return NULL; /* keep compiler quiet */
            }
        } else {
            Page page;
            buffer =
                ReadBuffer(RELATION_IS_PARTITIONED(relation) ? fakeRelation : relation, ItemPointerGetBlockNumber(tid));

            /*
            * Although we already know this tuple is valid, we must lock the
            * buffer to ensure that no one has a buffer cleanup lock; otherwise
            * they might move the tuple while we try to copy it.  But we can
            * release the lock before actually doing the heap_copytuple call,
            * since holding pin is sufficient to prevent anyone from getting a
            * cleanup lock they don't already hold.
            */
            LockBuffer(buffer, BUFFER_LOCK_SHARE);

            page = BufferGetPage(buffer);
            RowPtr *rp = UPageGetRowPtr(page, ItemPointerGetOffsetNumber(tid));
            UHeapDiskTuple diskTuple = NULL;

            Assert(RowPtrIsUsed(rp));

            diskTuple = (UHeapDiskTuple)UPageGetRowData(page, rp);
            uheaptupdata.disk_tuple_size = rp->len;
            errorNo = memcpy_s((char*)uheaptupdata.disk_tuple, rp->len, (char*)diskTuple, rp->len);
            securec_check(errorNo, "\0", "\0");
            uheaptupdata.ctid = *tid;
            uheaptupdata.table_oid = RelationGetRelid(RELATION_IS_PARTITIONED(relation) ? fakeRelation : relation);
            uheaptupdata.xc_node_id = u_sess->pgxc_cxt.PGXCNodeIdentifier;
            uheaptupdata.t_xid_base = ((UHeapPageHeaderData*)page)->pd_xid_base;

            LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
            ExecStoreTuple(&uheaptupdata, slot, buffer, false);
            result = ExecCopySlotTuple(slot);
            ReleaseBuffer(buffer);
        }

        if (RELATION_IS_PARTITIONED(relation)) {
            partitionClose(relation, part, NoLock);
            releaseDummyRelation(&fakeRelation);
        }

        ExecDropSingleTupleTableSlot(slot);
    } else {
        if (newSlot != NULL) {
            TM_Result test;
            TM_FailureData tmfd;

            *newSlot = NULL;

            /* caller must pass an epqstate if EvalPlanQual is possible */
            Assert(epqstate != NULL);

            /*
             * lock tuple for update
             */
ltrmark:
            tuple.t_self = *tid;
            test = tableam_tuple_lock(fakeRelation,
                &tuple,
                &buffer,
                estate->es_output_cid,
                lockmode,
                LockWaitBlock,
                &tmfd,
                true,       // fake params below are for uheap implementation
                false, false, NULL, NULL, false);

            /* Let the caller know about the status of this operation */
            if (tmresultp)
                *tmresultp = test;
            if (tmfdp)
                *tmfdp = tmfd;

            switch (test) {
                case TM_SelfUpdated:
                case TM_SelfModified:
                    if (tmfd.cmax != estate->es_output_cid)
                        ereport(ERROR,
                                (errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
                                 errmsg("tuple to be updated was already modified by an operation triggered by the current command"),
                                 errhint("Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes to other rows.")));

                    /* treat it as deleted; do not process */
                    ReleaseBuffer(buffer);
                    ReleaseFakeRelation(relation, part, &fakeRelation);
                    return NULL;
                case TM_SelfCreated:
                    ReleaseBuffer(buffer);
                    ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                        errmsg("attempted to lock invisible tuple")));
                    break;
                case TM_Ok:
                    break;

                case TM_Updated: {
                    ReleaseBuffer(buffer);
                    if (IsolationUsesXactSnapshot())
                        ereport(ERROR,
                            (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                                errmsg("could not serialize access due to concurrent update")));
                    Assert(!ItemPointerEquals(&tmfd.ctid, &tuple.t_self));

                    /*
                     * Recheck the tuple using EPQ. For MERGE, we leave this
                     * to the caller (it must do additional rechecking, and
                     * might end up executing a different action entirely).
                     */
                    if (tmresultp && estate->es_plannedstmt->commandType == CMD_MERGE) {
                        ReleaseFakeRelation(relation, part, &fakeRelation);
                        return NULL;
                    }

                    /* it was updated, so look at the updated version */
                    TupleTableSlot* epqslot = NULL;

                    epqslot = EvalPlanQual(
                        estate, epqstate, fakeRelation, relinfo->ri_RangeTableIndex,
                        lockmode, &tmfd.ctid, tmfd.xmax, false);
                    if (!TupIsNull(epqslot)) {
                        *tid = tmfd.ctid;
                        *newSlot = epqslot;

                        /*
                         * EvalPlanQual already locked the tuple, but we
                         * re-call heap_lock_tuple anyway as an easy way of
                         * re-fetching the correct tuple.  Speed is hardly a
                         * criterion in this path anyhow.
                         */
                        goto ltrmark;
                    }

                    /*
                     * if tuple was deleted or PlanQual failed for updated tuple -
                     * we must not process this tuple!
                     */
                    ReleaseFakeRelation(relation, part, &fakeRelation);
                    return NULL;
                }

                case TM_Deleted:
                    ReleaseBuffer(buffer);
                    if (IsolationUsesXactSnapshot())
                        ereport(ERROR,
                            (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                                errmsg("could not serialize access due to concurrent update")));
                    Assert(ItemPointerEquals(&tmfd.ctid, &tuple.t_self));
                    /*
                     * if tuple was deleted or PlanQual failed for updated tuple -
                     * we must not process this tuple!
                     */
                    ReleaseFakeRelation(relation, part, &fakeRelation);
                    return NULL;

                default:
                    ReleaseBuffer(buffer);
                    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("unrecognized heap_lock_tuple status: %u", test)));
                    return NULL; /* keep compiler quiet */
            }
        } else {
            Page page;
            ItemId lp;

            buffer = ReadBuffer(fakeRelation, ItemPointerGetBlockNumber(tid));

            /*
             * Although we already know this tuple is valid, we must lock the
             * buffer to ensure that no one has a buffer cleanup lock; otherwise
             * they might move the tuple while we try to copy it.  But we can
             * release the lock before actually doing the (HeapTuple)tableam_tops_copy_tuple call,
             * since holding pin is sufficient to prevent anyone from getting a
             * cleanup lock they don't already hold.
             */
            LockBuffer(buffer, BUFFER_LOCK_SHARE);

            page = BufferGetPage(buffer);
            lp = PageGetItemId(page, ItemPointerGetOffsetNumber(tid));

            Assert(ItemIdIsNormal(lp));

            tuple.t_data = (HeapTupleHeader)PageGetItem(page, lp);
            tuple.t_len = ItemIdGetLength(lp);
            tuple.t_self = *tid;
            tuple.t_tableOid = RelationGetRelid(fakeRelation);
            tuple.t_bucketId = RelationGetBktid(fakeRelation);
            HeapTupleCopyBaseFromPage(&tuple, page);
#ifdef PGXC
            tuple.t_xc_node_id = u_sess->pgxc_cxt.PGXCNodeIdentifier;
#endif

            LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        }

        result = heapCopyTuple(&tuple, RelationGetDescr(relation), BufferGetPage(buffer));
        ReleaseBuffer(buffer);
        ReleaseFakeRelation(relation, part, &fakeRelation);
    }
    return result;
}

static void ReleaseFakeRelation(Relation relation, Partition part, Relation* fakeRelation)
{
    if (RELATION_IS_PARTITIONED(relation)) {
        partitionClose(relation, part, NoLock);
        releaseDummyRelation(fakeRelation);
    } else if (RELATION_OWN_BUCKET(relation)) {
        releaseDummyRelation(fakeRelation);
    }
	return;
}

static bool TriggerCheckReplicationDependent(Trigger* trigger)
{
    if (u_sess->attr.attr_common.SessionReplicationRole == SESSION_REPLICATION_ROLE_REPLICA) {
        if (trigger->tgenabled == TRIGGER_FIRES_ON_ORIGIN || trigger->tgenabled == TRIGGER_DISABLED)
            return false;
    } else {
        /* ORIGIN or LOCAL role */
        if (trigger->tgenabled == TRIGGER_FIRES_ON_REPLICA || trigger->tgenabled == TRIGGER_DISABLED)
            return false;
    }
    return true;
}

/*
 * Is trigger enabled to fire?
 */
static bool TriggerEnabled(EState* estate, ResultRelInfo* relinfo, Trigger* trigger, TriggerEvent event,
    const Bitmapset* modifiedCols, HeapTuple oldtup, HeapTuple newtup)
{
    /* Check replication-role-dependent enable state */
    if (TriggerCheckReplicationDependent(trigger) == false) {
        return false;
    }

    /*
     * Check for column-specific trigger (only possible for UPDATE, and in
     * fact we *must* ignore tgattr for other event types)
     */
    if (trigger->tgnattr > 0 && TRIGGER_FIRED_BY_UPDATE(event)) {
        int i;
        bool modified = false;

        modified = false;
        for (i = 0; i < trigger->tgnattr; i++) {
            if (bms_is_member(trigger->tgattr[i] - FirstLowInvalidHeapAttributeNumber, modifiedCols)) {
                modified = true;
                break;
            }
        }
        if (!modified) {
            return false;
        }
    }

    /* Check for WHEN clause */
    if (trigger->tgqual) {
        TupleDesc tupdesc = RelationGetDescr(relinfo->ri_RelationDesc);
        List** predicate;
        ExprContext* econtext = NULL;
        TupleTableSlot* oldslot = NULL;
        TupleTableSlot* newslot = NULL;
        MemoryContext oldContext;
        int i;

        Assert(estate != NULL);

        /*
         * trigger is an element of relinfo->ri_TrigDesc->triggers[]; find the
         * matching element of relinfo->ri_TrigWhenExprs[]
         */
        i = trigger - relinfo->ri_TrigDesc->triggers;
        predicate = &relinfo->ri_TrigWhenExprs[i];

        /*
         * If first time through for this WHEN expression, build expression
         * nodetrees for it.  Keep them in the per-query memory context so
         * they'll survive throughout the query.
         */
        if (*predicate == NIL) {
            Node* tgqual = NULL;

            oldContext = MemoryContextSwitchTo(estate->es_query_cxt);
            tgqual = (Node*)stringToNode(trigger->tgqual);
            /* Change references to OLD and NEW to INNER_VAR and OUTER_VAR */
            ChangeVarNodes(tgqual, PRS2_OLD_VARNO, INNER_VAR, 0);
            ChangeVarNodes(tgqual, PRS2_NEW_VARNO, OUTER_VAR, 0);
            /* ExecQual wants implicit-AND form */
            tgqual = (Node*)make_ands_implicit((Expr*)tgqual);
            if (estate->es_is_flt_frame){
                *predicate = (List*)ExecPrepareQualByFlatten((List*)tgqual, estate);
            } else {
                *predicate = (List*)ExecPrepareExpr((Expr*)tgqual, estate);
            }
            (void)MemoryContextSwitchTo(oldContext);
            pfree_ext(tgqual);
        }

        /*
         * We will use the EState's per-tuple context for evaluating WHEN
         * expressions (creating it if it's not already there).
         */
        econtext = GetPerTupleExprContext(estate);

        /*
         * Put OLD and NEW tuples into tupleslots for expression evaluation.
         * These slots can be shared across the whole estate, but be careful
         * that they have the current resultrel's tupdesc.
         */
        if (HeapTupleIsValid(oldtup)) {
            if (estate->es_trig_oldtup_slot == NULL) {
                oldContext = MemoryContextSwitchTo(estate->es_query_cxt);
                estate->es_trig_oldtup_slot = ExecInitExtraTupleSlot(estate);
                (void)MemoryContextSwitchTo(oldContext);
            }
            oldslot = estate->es_trig_oldtup_slot;
            if (oldslot->tts_tupleDescriptor != tupdesc) {
                ExecSetSlotDescriptor(oldslot, tupdesc);
            }
            (void)ExecStoreTuple(oldtup, oldslot, InvalidBuffer, false);
        }
        if (HeapTupleIsValid(newtup)) {
            if (estate->es_trig_newtup_slot == NULL) {
                oldContext = MemoryContextSwitchTo(estate->es_query_cxt);
                estate->es_trig_newtup_slot = ExecInitExtraTupleSlot(estate);
                (void)MemoryContextSwitchTo(oldContext);
            }
            newslot = estate->es_trig_newtup_slot;
            if (newslot->tts_tupleDescriptor != tupdesc) {
                ExecSetSlotDescriptor(newslot, tupdesc);
            }
            (void)ExecStoreTuple(newtup, newslot, InvalidBuffer, false);
        }

        /*
         * Finally evaluate the expression, making the old and/or new tuples
         * available as INNER_VAR/OUTER_VAR respectively.
         */
        econtext->ecxt_innertuple = oldslot;
        econtext->ecxt_outertuple = newslot;
        if (!ExecQual(*predicate, econtext))
            return false;
    }

    return true;
}

/* ----------
 * After-trigger stuff
 *
 * The AfterTriggersData struct holds data about pending AFTER trigger events
 * during the current transaction tree.  (BEFORE triggers are fired
 * immediately so we don't need any persistent state about them.)  The struct
 * and most of its subsidiary data are kept in u_sess->top_transaction_mem_cxt; however
 * the individual event records are kept in a separate sub-context.  This is
 * done mainly so that it's easy to tell from a memory context dump how much
 * space is being eaten by trigger events.
 *
 * Because the list of pending events can grow large, we go to some
 * considerable effort to minimize per-event memory consumption.  The event
 * records are grouped into chunks and common data for similar events in the
 * same chunk is only stored once.
 *
 * XXX We need to be able to save the per-event data in a file if it grows too
 * large.
 * ----------
 */
/* Per-trigger SET CONSTRAINT status */
typedef struct SetConstraintTriggerData {
    Oid sct_tgoid;
    bool sct_tgisdeferred;
} SetConstraintTriggerData;

/*
 * SET CONSTRAINT intra-transaction status.
 *
 * We make this a single palloc'd object so it can be copied and freed easily.
 *
 * all_isset and all_isdeferred are used to keep track
 * of SET CONSTRAINTS ALL {DEFERRED, IMMEDIATE}.
 *
 * trigstates[] stores per-trigger tgisdeferred settings.
 */
typedef struct SetConstraintStateData {
    bool all_isset;
    bool all_isdeferred;
    int numstates;                          /* number of trigstates[] entries in use */
    int numalloc;                           /* allocated size of trigstates[] */
    SetConstraintTriggerData trigstates[1]; /* VARIABLE LENGTH ARRAY */
} SetConstraintStateData;

typedef SetConstraintStateData* SetConstraintState;

/*
 * Per-trigger-event data
 *
 * The actual per-event data, AfterTriggerEventData, includes DONE/IN_PROGRESS
 * status bits and one or two tuple CTIDs.	Each event record also has an
 * associated AfterTriggerSharedData that is shared across all instances
 * of similar events within a "chunk".
 *
 * We arrange not to waste storage on ate_ctid2 for non-update events.
 * We could go further and not store either ctid for statement-level triggers,
 * but that seems unlikely to be worth the trouble.
 *
 * Note: ats_firing_id is initially zero and is set to something else when
 * AFTER_TRIGGER_IN_PROGRESS is set.  It indicates which trigger firing
 * cycle the trigger will be fired in (or was fired in, if DONE is set).
 * Although this is mutable state, we can keep it in AfterTriggerSharedData
 * because all instances of the same type of event in a given event list will
 * be fired at the same time, if they were queued between the same firing
 * cycles.	So we need only ensure that ats_firing_id is zero when attaching
 * a new event to an existing AfterTriggerSharedData record.
 */
typedef uint32 TriggerFlags;

#define AFTER_TRIGGER_OFFSET        \
    0x0FFFFFFF /* must be low-order \
                * bits */
#define AFTER_TRIGGER_2CTIDS 0x10000000
#define AFTER_TRIGGER_DONE 0x20000000
#define AFTER_TRIGGER_IN_PROGRESS 0x40000000

typedef struct AfterTriggerSharedData* AfterTriggerShared;

typedef struct AfterTriggerSharedData {
    TriggerEvent ats_event; /* event type indicator, see trigger.h */
    Oid ats_tgoid;          /* the trigger's ID */
    Oid ats_relid;          /* the relation it's on */
    Oid ats_oldPartid;
    Oid ats_newPartid;
    int2 ats_bucketid;
    CommandId ats_firing_id; /* ID for firing cycle */
} AfterTriggerSharedData;

typedef struct AfterTriggerEventData* AfterTriggerEvent;

#ifdef PGXC

/*
 * TsOffset:
 * Used for values representing offset of the OLD/NEW row position in rowstore.
 * Intentionally not unsigned, to check for invalid values.
 */
typedef int32 TsOffset;

/*
 * ARTupInfo: Wrapper around tuplestore. The only reason we have this wrapper
 * is because we need to keep track of the position of tuplestore readptr which
 * is not directly accessible by the tuplestore interface. Hence the field
 * ti_curpos.
 */
typedef struct {
    Tuplestorestate* tupstate;

    /*
     * ti_curpos: first position is 0. ti_curpos either points to a valid record,
     * or one position more than the last record when the tuplestore is at eof.
     * So if there are 3 tuples, and currently tuplestore is at eof, its value
     * will be 4. If tuplestore is empty, it's value will be 0. If the tupstate
     * is not allocated, its value is -1.
     */
    TsOffset ti_curpos;
} ARTupInfo;

const int ARTUP_NUM = 2;
/* Abstract row store type, that stores OLD and NEW rows. */
typedef struct {
    /*
     * rs_tupinfo[0] contains the trigtuple in case of INSERT/DELETE,
     * rs_tupinfo[1] contains the new tuple for UPDATE.
     */
    ARTupInfo rs_tupinfo[ARTUP_NUM];

    /* If one or more rows belong to a deferred trigger, this is set to true. */
    bool rs_has_deferred;
} ARRowStore;

/*
 * For coordinator, we don't use ctids, because the rows do not belong to the
 * local coordinator. Instead we keep in tuplestore the remotely fetched OLD and
 * NEW rows, and save their tuplestore position in the event list.
 * But since both coordinator and datanode would access the same
 * structure, we device a union that would be used for accessing
 * rowstore position on coordinator or for accessing ctid1 on datanode.
 * On coordinator, we leave ctid2 space unused. We can't move it into
 * the union because datanode needs a ctid1-only part of that structure which
 * is only possible if we keep ctid2 outside of the union.
 * The OLD and NEW row have the same rowstore position so we require only one
 * position value to be stored.
 */
/*
 * RowPointerData is like a pointer to the OLD/NEW row position in the rowstore.
 */
typedef struct {
    ARRowStore* rp_rsid; /* Pointer to the actual rowstore. */
    TsOffset rp_posid;   /* Offset of the row in that rowstore */
} RowPointerData;

typedef union {
    ItemPointerData cor_ctid;
    RowPointerData cor_rpid;
} CtidOrRpid;

#define IsRowPointerValid(rp) ((rp)->rp_rsid != NULL && (rp)->rp_posid >= 0)
#define RowPointerSetInvalid(rp) (((rp)->rp_rsid = NULL), ((rp)->rp_posid = -1))

#endif

typedef struct AfterTriggerEventData {
    TriggerFlags ate_flags; /* status bits and offset to shared data */
#ifdef PGXC
    CtidOrRpid xc_ate_cor;
#else
    ItemPointerData ate_ctid1; /* inserted, deleted, or old updated tuple */
#endif
    ItemPointerData ate_ctid2; /* new updated tuple */
} AfterTriggerEventData;

/* This struct must exactly match the one above except for not having ctid2 */
typedef struct AfterTriggerEventDataOneCtid {
    TriggerFlags ate_flags; /* status bits and offset to shared data */
#ifdef PGXC
    CtidOrRpid xc_ate_cor;
#else
    ItemPointerData ate_ctid1; /* inserted, deleted, or old updated tuple */
#endif
} AfterTriggerEventDataOneCtid;

#ifdef PGXC
#define ate_ctid1 xc_ate_cor.cor_ctid
#define xc_ate_row xc_ate_cor.cor_rpid
#endif

#define SizeofTriggerEvent(evt) \
    (((evt)->ate_flags & AFTER_TRIGGER_2CTIDS) ? sizeof(AfterTriggerEventData) : sizeof(AfterTriggerEventDataOneCtid))

#define GetTriggerSharedData(evt) ((AfterTriggerShared)((char*)(evt) + ((evt)->ate_flags & AFTER_TRIGGER_OFFSET)))

/*
 * To avoid palloc overhead, we keep trigger events in arrays in successively-
 * larger chunks (a slightly more sophisticated version of an expansible
 * array).	The space between CHUNK_DATA_START and freeptr is occupied by
 * AfterTriggerEventData records; the space between endfree and endptr is
 * occupied by AfterTriggerSharedData records.
 */
typedef struct AfterTriggerEventChunk {
    struct AfterTriggerEventChunk* next; /* list link */
    char* freeptr;                       /* start of free space in chunk */
    char* endfree;                       /* end of free space in chunk */
    char* endptr;                        /* end of chunk */
                                         /* event data follows here */
} AfterTriggerEventChunk;

#define CHUNK_DATA_START(cptr) ((char*)(cptr) + MAXALIGN(sizeof(AfterTriggerEventChunk)))

/* A list of events */
typedef struct AfterTriggerEventList {
    AfterTriggerEventChunk* head;
    AfterTriggerEventChunk* tail;
    char* tailfree; /* freeptr of tail chunk */
} AfterTriggerEventList;

/* Macros to help in iterating over a list of events */
#define for_each_chunk(cptr, evtlist) for ((cptr) = (evtlist).head; (cptr) != NULL; (cptr) = (cptr)->next)
#define for_each_event(eptr, cptr)                                                          \
    for (eptr = (AfterTriggerEvent)CHUNK_DATA_START(cptr); (char*)(eptr) < (cptr)->freeptr; \
         eptr = (AfterTriggerEvent)(((char*)(eptr)) + SizeofTriggerEvent(eptr)))
/* Use this if no special per-chunk processing is needed */
#define for_each_event_chunk(eptr, cptr, evtlist) for_each_chunk(cptr, evtlist) for_each_event(eptr, cptr)

/*
 * All per-transaction data for the AFTER TRIGGERS module.
 *
 * AfterTriggersData has the following fields:
 *
 * firing_counter is incremented for each call of afterTriggerInvokeEvents.
 * We mark firable events with the current firing cycle's ID so that we can
 * tell which ones to work on.	This ensures sane behavior if a trigger
 * function chooses to do SET CONSTRAINTS: the inner SET CONSTRAINTS will
 * only fire those events that weren't already scheduled for firing.
 *
 * state keeps track of the transaction-local effects of SET CONSTRAINTS.
 * This is saved and restored across failed subtransactions.
 *
 * events is the current list of deferred events.  This is global across
 * all subtransactions of the current transaction.	In a subtransaction
 * abort, we know that the events added by the subtransaction are at the
 * end of the list, so it is relatively easy to discard them.  The event
 * list chunks themselves are stored in event_cxt.
 *
 * query_depth is the current depth of nested AfterTriggerBeginQuery calls
 * (-1 when the stack is empty).
 *
 * query_stack[query_depth] is a list of AFTER trigger events queued by the
 * current query (and the query_stack entries below it are lists of trigger
 * events queued by calling queries).  None of these are valid until the
 * matching AfterTriggerEndQuery call occurs.  At that point we fire
 * immediate-mode triggers, and append any deferred events to the main events
 * list.
 *
 * maxquerydepth is just the allocated length of query_stack.
 *
 * state_stack is a stack of pointers to saved copies of the SET CONSTRAINTS
 * state data; each subtransaction level that modifies that state first
 * saves a copy, which we use to restore the state if we abort.
 *
 * events_stack is a stack of copies of the events head/tail pointers,
 * which we use to restore those values during subtransaction abort.
 *
 * depth_stack is a stack of copies of subtransaction-start-time query_depth,
 * which we similarly use to clean up at subtransaction abort.
 *
 * firing_stack is a stack of copies of subtransaction-start-time
 * firing_counter.	We use this to recognize which deferred triggers were
 * fired (or marked for firing) within an aborted subtransaction.
 *
 * We use GetCurrentTransactionNestLevel() to determine the correct array
 * index in these stacks.  maxtransdepth is the number of allocated entries in
 * each stack.	(By not keeping our own stack pointer, we can avoid trouble
 * in cases where errors during subxact abort cause multiple invocations
 * of AfterTriggerEndSubXact() at the same nesting depth.)
 */
typedef struct AfterTriggersData {
    CommandId firing_counter;           /* next firing ID to assign */
    SetConstraintState state;           /* the active S C state */
    AfterTriggerEventList events;       /* deferred-event list */
    int query_depth;                    /* current query list index */
    AfterTriggerEventList* query_stack; /* events pending from each query */
    int maxquerydepth;                  /* allocated len of above array */
    MemoryContext event_cxt;            /* memory context for events, if any */

#ifdef PGXC
    MemoryContext xc_rs_cxt;   /* memory context to store OLD and NEW rows */
    ARRowStore** xc_rowstores; /* Array of per-query row triggers. */
    int xc_max_rowstores;      /* Allocated length of above array */
#endif

    /* these fields are just for resetting at subtrans abort: */
    SetConstraintState* state_stack;     /* stacked S C states */
    AfterTriggerEventList* events_stack; /* stacked list pointers */
    int* depth_stack;                    /* stacked query_depths */
    CommandId* firing_stack;             /* stacked firing_counters */
    int maxtransdepth;                   /* allocated len of above arrays */
} AfterTriggersData;

typedef AfterTriggersData* AfterTriggers;

static void AfterTriggerExecute(AfterTriggerEvent event, Relation rel, Oid oldPartOid, Oid newPartOid, int2 bucketid,
    TriggerDesc* trigdesc, FmgrInfo* finfo, Instrumentation* instr, MemoryContext per_tuple_context);
static SetConstraintState SetConstraintStateCreate(int numalloc);
static SetConstraintState SetConstraintStateCopy(const SetConstraintState state);
static SetConstraintState SetConstraintStateAddItem(SetConstraintState state, Oid tgoid, bool tgisdeferred);
#ifdef PGXC
static void pgxc_ar_init_rowstore(void);
static void pgxc_ARFetchRow(Relation rel, RowPointerData* rpid, HeapTuple* rs_tuple1, HeapTuple* rs_tuple2);
static void pgxc_ar_dofetch(Relation rel, ARTupInfo* rs_tupinfo, int fetchpos, HeapTuple* rs_tuple);
static int pgxc_ar_goto_end(ARTupInfo* rs_tupinfo);
static void pgxc_ARNextNewRowpos(RowPointerData* rpid);
static void pgxc_ar_doadd(HeapTuple tuple, ARTupInfo* rs_tupinfo);
static void pgxc_ARAddRow(HeapTuple oldtup, HeapTuple newtup, bool is_deferred);
static void pgxc_ar_init_tupinfo(ARTupInfo* rs_tupinfo);
static void pgxc_ARFreeRowStoreForQuery(int query_index);
static void pgxc_ARMarkAllDeferred(void);
static ARRowStore* pgxc_ar_alloc_rsentry(void);
#endif

/* ----------
 * afterTriggerCheckState
 *
 *	Returns true if the trigger event is actually in state DEFERRED.
 * ----------
 */
static bool afterTriggerCheckState(const AfterTriggerShared evtshared)
{
    Oid tgoid = evtshared->ats_tgoid;
    SetConstraintState state = u_sess->tri_cxt.afterTriggers->state;
    int i;

    /*
     * For not-deferrable triggers (i.e. normal AFTER ROW triggers and
     * constraints declared NOT DEFERRABLE), the state is always false.
     */
    if ((evtshared->ats_event & AFTER_TRIGGER_DEFERRABLE) == 0)
        return false;

    /*
     * Check if SET CONSTRAINTS has been executed for this specific trigger.
     */
    for (i = 0; i < state->numstates; i++) {
        if (state->trigstates[i].sct_tgoid == tgoid)
            return state->trigstates[i].sct_tgisdeferred;
    }

    /*
     * Check if SET CONSTRAINTS ALL has been executed; if so use that.
     */
    if (state->all_isset) {
        return state->all_isdeferred;
    }

    /*
     * Otherwise return the default state for the trigger.
     */
    return ((evtshared->ats_event & AFTER_TRIGGER_INITDEFERRED) != 0);
}

/* ----------
 * afterTriggerAddEvent
 *
 *	Add a new trigger event to the specified queue.
 *	The passed-in event data is copied.
 * ----------
 */
static void afterTriggerAddEvent(AfterTriggerEventList* events, const AfterTriggerEvent event, AfterTriggerShared evtshared)
{
    Size eventsize = SizeofTriggerEvent(event);
    Size needed = eventsize + sizeof(AfterTriggerSharedData);
    AfterTriggerEventChunk* chunk = NULL;
    AfterTriggerShared newshared = NULL;

    /*
     * If empty list or not enough room in the tail chunk, make a new chunk.
     * We assume here that a new shared record will always be needed.
     */
    chunk = events->tail;
    if (chunk == NULL || (Size)(chunk->endfree - chunk->freeptr) < needed) {
        Size chunksize;

        /* Create event context if we didn't already */
        if (u_sess->tri_cxt.afterTriggers->event_cxt == NULL)
            u_sess->tri_cxt.afterTriggers->event_cxt = AllocSetContextCreate(u_sess->top_transaction_mem_cxt,
                "AfterTriggerEvents",
                ALLOCSET_DEFAULT_MINSIZE,
                ALLOCSET_DEFAULT_INITSIZE,
                ALLOCSET_DEFAULT_MAXSIZE);

            /*
             * Chunk size starts at 1KB and is allowed to increase up to 1MB.
             * These numbers are fairly arbitrary, though there is a hard limit at
             * AFTER_TRIGGER_OFFSET; else we couldn't link event records to their
             * shared records using the available space in ate_flags.  Another
             * constraint is that if the chunk size gets too huge, the search loop
             * below would get slow given a (not too common) usage pattern with
             * many distinct event types in a chunk.  Therefore, we double the
             * preceding chunk size only if there weren't too many shared records
             * in the preceding chunk; otherwise we halve it.  This gives us some
             * ability to adapt to the actual usage pattern of the current query
             * while still having large chunk sizes in typical usage.  All chunk
             * sizes used should be MAXALIGN multiples, to ensure that the shared
             * records will be aligned safely.
             */
#define MIN_CHUNK_SIZE 1024
#define MAX_CHUNK_SIZE (1024 * 1024)

#if MAX_CHUNK_SIZE > (AFTER_TRIGGER_OFFSET + 1)
#error MAX_CHUNK_SIZE must not exceed AFTER_TRIGGER_OFFSET
#endif

        if (chunk == NULL)
            chunksize = MIN_CHUNK_SIZE;
        else {
            /* preceding chunk size... */
            chunksize = chunk->endptr - (char*)chunk;
            /* check number of shared records in preceding chunk */
            const int chunk_num = 100;
            if ((Size)(chunk->endptr - chunk->endfree) <= (chunk_num * sizeof(AfterTriggerSharedData)))
                chunksize *= 2; /* okay, double it */
            else
                chunksize /= 2; /* too many shared records */
            chunksize = Min(chunksize, MAX_CHUNK_SIZE);
        }
        chunk = (AfterTriggerEventChunk*)MemoryContextAlloc(u_sess->tri_cxt.afterTriggers->event_cxt, chunksize);
        chunk->next = NULL;
        chunk->freeptr = CHUNK_DATA_START(chunk);
        chunk->endptr = chunk->endfree = (char*)chunk + chunksize;
        Assert((Size)(chunk->endfree - chunk->freeptr) >= needed);

        if (events->head == NULL)
            events->head = chunk;
        else
            events->tail->next = chunk;
        events->tail = chunk;
        /* events->tailfree is now out of sync, but we'll fix it below */
    }

    /*
     * Try to locate a matching shared-data record already in the chunk. If
     * none, make a new one.
     */
    for (newshared = ((AfterTriggerShared)chunk->endptr) - 1; (char*)newshared >= chunk->endfree; newshared--) {
        if (newshared->ats_tgoid == evtshared->ats_tgoid && newshared->ats_relid == evtshared->ats_relid &&
            newshared->ats_oldPartid == evtshared->ats_oldPartid &&
            newshared->ats_newPartid == evtshared->ats_newPartid &&
            newshared->ats_bucketid == evtshared->ats_bucketid && newshared->ats_event == evtshared->ats_event &&
            newshared->ats_firing_id == 0)
            break;
    }
    if ((char*)newshared < chunk->endfree) {
        *newshared = *evtshared;
        newshared->ats_firing_id = 0; /* just to be sure */
        chunk->endfree = (char*)newshared;
    }

    /* Insert the data */
    AfterTriggerEvent newevent = (AfterTriggerEvent)chunk->freeptr;
    errno_t rc = memcpy_s(newevent, eventsize, event, eventsize);
    securec_check(rc, "", "");
    /* ... and link the new event to its shared record */
    newevent->ate_flags &= ~AFTER_TRIGGER_OFFSET;
    newevent->ate_flags |= (uint32)((char*)newshared - (char*)newevent);

    chunk->freeptr += eventsize;
    events->tailfree = chunk->freeptr;
}

/* ----------
 * afterTriggerFreeEventList
 *
 *	Free all the event storage in the given list.
 * ----------
 */
static void afterTriggerFreeEventList(AfterTriggerEventList* events)
{
    AfterTriggerEventChunk* chunk = NULL;
    AfterTriggerEventChunk* next_chunk = NULL;

    for (chunk = events->head; chunk != NULL; chunk = next_chunk) {
        next_chunk = chunk->next;
        pfree_ext(chunk);
    }
    events->head = NULL;
    events->tail = NULL;
    events->tailfree = NULL;
}

/* ----------
 * afterTriggerRestoreEventList
 *
 *	Restore an event list to its prior length, removing all the events
 *	added since it had the value old_events.
 * ----------
 */
static void afterTriggerRestoreEventList(AfterTriggerEventList* events, const AfterTriggerEventList* old_events)
{
    AfterTriggerEventChunk* chunk = NULL;
    AfterTriggerEventChunk* next_chunk = NULL;

    if (old_events->tail == NULL) {
        /* restoring to a completely empty state, so free everything */
        afterTriggerFreeEventList(events);
    } else {
        *events = *old_events;
        /* free any chunks after the last one we want to keep */
        for (chunk = events->tail->next; chunk != NULL; chunk = next_chunk) {
            next_chunk = chunk->next;
            pfree_ext(chunk);
        }
        /* and clean up the tail chunk to be the right length */
        events->tail->next = NULL;
        events->tail->freeptr = events->tailfree;

        /*
         * We don't make any effort to remove now-unused shared data records.
         * They might still be useful, anyway.
         */
    }
}

/* ----------
 * AfterTriggerExecute
 *
 *	Fetch the required tuples back from the heap and fire one
 *	single trigger function.
 *
 *	Frequently, this will be fired many times in a row for triggers of
 *	a single relation.	Therefore, we cache the open relation and provide
 *	fmgr lookup cache space at the caller level.  (For triggers fired at
 *	the end of a query, we can even piggyback on the executor's state.)
 *
 *	event: event currently being fired.
 *	rel: open relation for event.
 *	trigdesc: working copy of rel's trigger info.
 *	finfo: array of fmgr lookup cache entries (one per trigger in trigdesc).
 *	instr: array of EXPLAIN ANALYZE instrumentation nodes (one per trigger),
 *		or NULL if no instrumentation is wanted.
 *	per_tuple_context: memory context to call trigger function in.
 * ----------
 */
static void AfterTriggerExecute(AfterTriggerEvent event, Relation rel, Oid oldPartOid, Oid newPartOid, int2 bucketid,
    TriggerDesc* trigdesc, FmgrInfo* finfo, Instrumentation* instr, MemoryContext per_tuple_context)
{
    AfterTriggerShared evtshared = GetTriggerSharedData(event);
    Oid tgoid = evtshared->ats_tgoid;
    TriggerData LocTriggerData;
    HeapTupleData tuple1;
    HeapTupleData tuple2;
    HeapTuple rettuple;
    Buffer buffer1 = InvalidBuffer;
    Buffer buffer2 = InvalidBuffer;
    int tgindx;
    Partition oldPart = NULL;
    Partition newPart = NULL;
    Relation oldPartRel = NULL;
    Relation newPartRel = NULL;
    Partition oldPartForSubPart = NULL;
    Partition newPartForSubPart = NULL;
    Relation oldPartRelForSubPart = NULL;
    Relation newPartRelForSubPart = NULL;
#ifdef PGXC
    HeapTuple rs_tuple1 = NULL;
    HeapTuple rs_tuple2 = NULL;
    bool is_remote_relation = (RelationGetLocInfo(rel) != NULL);
#endif
    union {
        HeapTupleHeaderData hdr;
        char data[MaxHeapTupleSize + sizeof(HeapTupleHeaderData)];
    } tbuf1 = {0};
    union {
        HeapTupleHeaderData hdr;
        char data[MaxHeapTupleSize + sizeof(HeapTupleHeaderData)];
    } tbuf2 = {0};

    /*
     * Locate trigger in trigdesc.
     */
    LocTriggerData.tg_trigger = NULL;
    for (tgindx = 0; tgindx < trigdesc->numtriggers; tgindx++) {
        if (trigdesc->triggers[tgindx].tgoid == tgoid) {
            LocTriggerData.tg_trigger = &(trigdesc->triggers[tgindx]);
            break;
        }
    }
    if (LocTriggerData.tg_trigger == NULL)
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("could not find trigger %u", tgoid)));

    /*
     * If doing EXPLAIN ANALYZE, start charging time to this trigger. We want
     * to include time spent re-fetching tuples in the trigger cost.
     */
    if (instr != NULL)
        InstrStartNode(instr + tgindx);

#ifdef PGXC
    /*
     * If this table contains locator info, then the events may be having
     * tuplestore positions of saved rows instead of ctids. So fetch them.
     */
    if (is_remote_relation) {
        RowPointerData* rpid = &event->xc_ate_row;
        if (IsRowPointerValid(rpid)) {
            pgxc_ARFetchRow(rel, &(event->xc_ate_row), &rs_tuple1, &rs_tuple2);
            LocTriggerData.tg_trigtuple = rs_tuple1;
            LocTriggerData.tg_newtuple = rs_tuple2;
        } else {
            LocTriggerData.tg_trigtuple = NULL;
            LocTriggerData.tg_newtuple = NULL;
        }
        /* Buffers are only meant for local table tuples */
        LocTriggerData.tg_trigtuplebuf = InvalidBuffer;
        LocTriggerData.tg_newtuplebuf = InvalidBuffer;
    } else {
#endif

        /*
         * Fetch the required tuple(s).
         */
        if (ItemPointerIsValid(&(event->ate_ctid1))) {
            if (RELATION_IS_PARTITIONED(rel)) {
                Assert(OidIsValid(oldPartOid) || OidIsValid(newPartOid));
                Assert(!RELATION_OWN_BUCKET(rel) || bucketid != InvalidBktId);
                if (RelationIsSubPartitioned(rel)) {
                    Oid subPartOid = OidIsValid(oldPartOid) ? oldPartOid : newPartOid;
                    Oid partOid = partid_get_parentid(subPartOid);
                    Assert(OidIsValid(partOid));
                    oldPartForSubPart = partitionOpen(rel, partOid, NoLock, bucketid);
                    oldPartRelForSubPart = partitionGetRelation(rel, oldPartForSubPart);
                    oldPart = partitionOpen(oldPartRelForSubPart, subPartOid, NoLock, bucketid);
                    oldPartRel = partitionGetRelation(oldPartRelForSubPart, oldPart);
                } else {
                    oldPart = partitionOpen(rel, OidIsValid(oldPartOid) ? oldPartOid : newPartOid, NoLock, bucketid);

                    oldPartRel = partitionGetRelation(rel, oldPart);
                }
            } else if (RELATION_OWN_BUCKET(rel)) {
                Assert(bucketid != InvalidBktId);
                oldPartRel = bucketGetRelation(rel, NULL, bucketid);
            }

            ItemPointerCopy(&(event->ate_ctid1), &(tuple1.t_self));
            /* Must set a private data buffer for heap_fetch */
            tuple1.t_data = &tbuf1.hdr;
            if (!tableam_tuple_fetch(PointerIsValid(oldPartRel) ? oldPartRel : rel, SnapshotAny, &tuple1, &buffer1, false, NULL))
                ereport(ERROR,
                    (errcode(ERRCODE_TRIGGERED_INVALID_TUPLE), errmsg("failed to fetch tuple1 for AFTER trigger")));
            LocTriggerData.tg_trigtuple = &tuple1;
            LocTriggerData.tg_trigtuplebuf = buffer1;
        } else {
            LocTriggerData.tg_trigtuple = NULL;
            LocTriggerData.tg_trigtuplebuf = InvalidBuffer;
        }

        /* don't touch ctid2 if not there */
        if ((event->ate_flags & AFTER_TRIGGER_2CTIDS) && ItemPointerIsValid(&(event->ate_ctid2))) {
            if (RELATION_IS_PARTITIONED(rel) && (newPartOid != oldPartOid)) {
                Assert(OidIsValid(newPartOid));
                Assert(!RELATION_OWN_BUCKET(rel) || bucketid != InvalidBktId);
                if (RelationIsSubPartitioned(rel)) {
                    Oid subPartOid = newPartOid;
                    Oid partOid = partid_get_parentid(subPartOid);
                    Assert(OidIsValid(partOid));
                    newPartForSubPart = partitionOpen(rel, partOid, NoLock, bucketid);
                    newPartRelForSubPart = partitionGetRelation(rel, newPartForSubPart);
                    newPart = partitionOpen(newPartRelForSubPart, subPartOid, NoLock, bucketid);
                    newPartRel = partitionGetRelation(newPartRelForSubPart, newPart);
                } else {
                    newPart = partitionOpen(rel, newPartOid, NoLock, bucketid);

                    newPartRel = partitionGetRelation(rel, newPart);
                }
            } else {
                Assert(PointerIsValid(oldPartRel) || bucketid == InvalidBktId);
                newPartRel = oldPartRel;
            }

            ItemPointerCopy(&(event->ate_ctid2), &(tuple2.t_self));
            /* Must set a private data buffer for heap_fetch */
            tuple2.t_data = &tbuf2.hdr;
            if (!tableam_tuple_fetch(PointerIsValid(newPartRel) ? newPartRel : rel, SnapshotAny, &tuple2, &buffer2, false, NULL))
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("failed to fetch tuple2 for AFTER trigger")));
            LocTriggerData.tg_newtuple = &tuple2;
            LocTriggerData.tg_newtuplebuf = buffer2;
        } else {
            LocTriggerData.tg_newtuple = NULL;
            LocTriggerData.tg_newtuplebuf = InvalidBuffer;
        }

#ifdef PGXC
    }
#endif

    /*
     * Setup the remaining trigger information
     */
    LocTriggerData.type = T_TriggerData;
    LocTriggerData.tg_event = evtshared->ats_event & (TRIGGER_EVENT_OPMASK | TRIGGER_EVENT_ROW);
    LocTriggerData.tg_relation = rel;

    MemoryContextReset(per_tuple_context);

    /*
     * Call the trigger and throw away any possibly returned updated tuple.
     * (Don't let ExecCallTriggerFunc measure EXPLAIN time.)
     */
    rettuple = ExecCallTriggerFunc(&LocTriggerData, tgindx, finfo, NULL, per_tuple_context);
#ifdef PGXC
    /*
     * For remote relations, the tuple pointers passed to triggers are
     * copies of tuplestore tuples, so we need to free them.
     */
    if (is_remote_relation) {
        if (rettuple != NULL && rettuple != rs_tuple1 && rettuple != rs_tuple2)
            tableam_tops_free_tuple(rettuple);
        if (rs_tuple1)
            tableam_tops_free_tuple(rs_tuple1);
        if (rs_tuple2)
            tableam_tops_free_tuple(rs_tuple2);
    } else
#endif
        if (rettuple != NULL && rettuple != &tuple1 && rettuple != &tuple2) {
            tableam_tops_free_tuple(rettuple);
        }
    /*
     * Release buffers
     */
    if (buffer1 != InvalidBuffer)
        ReleaseBuffer(buffer1);
    if (buffer2 != InvalidBuffer)
        ReleaseBuffer(buffer2);

    /*
     * If doing EXPLAIN ANALYZE, stop charging time to this trigger, and count
     * one "tuple returned" (really the number of firings).
     */
    if (instr != NULL)
        InstrStopNode(instr + tgindx, 1);

    if (PointerIsValid(oldPartRel)) {
        if (RelationIsSubPartitioned(rel)) {
            releaseDummyRelation(&oldPartRel);
            if (PointerIsValid(oldPart)) {
                partitionClose(oldPartRelForSubPart, oldPart, NoLock);
            }
            releaseDummyRelation(&oldPartRelForSubPart);
            if (PointerIsValid(oldPartForSubPart)) {
                partitionClose(rel, oldPartForSubPart, NoLock);
            }
        } else {
            if (PointerIsValid(oldPart)) {
                partitionClose(rel, oldPart, NoLock);
            }
            releaseDummyRelation(&oldPartRel);
        }
    }
    if (PointerIsValid(newPartRel) && (oldPartOid != newPartOid)) {
        if (RelationIsSubPartitioned(rel)) {
            releaseDummyRelation(&newPartRel);
            if (PointerIsValid(newPart)) {
                partitionClose(newPartRelForSubPart, newPart, NoLock);
            }
            releaseDummyRelation(&newPartRelForSubPart);
            if (PointerIsValid(newPartForSubPart)) {
                partitionClose(rel, newPartForSubPart, NoLock);
            }
        } else {
            if (PointerIsValid(newPart)) {
                partitionClose(rel, newPart, NoLock);
            }
            releaseDummyRelation(&newPartRel);
        }
    }
}

/*
 * afterTriggerMarkEvents
 *
 *	Scan the given event list for not yet invoked events.  Mark the ones
 *	that can be invoked now with the current firing ID.
 *
 *	If move_list isn't NULL, events that are not to be invoked now are
 *	transferred to move_list.
 *
 *	When immediate_only is TRUE, do not invoke currently-deferred triggers.
 *	(This will be FALSE only at main transaction exit.)
 *
 *	Returns TRUE if any invokable events were found.
 */
static bool afterTriggerMarkEvents(AfterTriggerEventList* events, AfterTriggerEventList* move_list, bool immediate_only)
{
    bool found = false;
    AfterTriggerEvent event = NULL;
    AfterTriggerEventChunk* chunk = NULL;

    for_each_event_chunk (event, chunk, *events) {
        AfterTriggerShared evtshared = GetTriggerSharedData(event);
        bool defer_it = false;

        if (!(event->ate_flags & (AFTER_TRIGGER_DONE | AFTER_TRIGGER_IN_PROGRESS))) {
            /*
             * This trigger hasn't been called or scheduled yet. Check if we
             * should call it now.
             */
            if (immediate_only && afterTriggerCheckState(evtshared)) {
                defer_it = true;
            } else {
                /*
                 * Mark it as to be fired in this firing cycle.
                 */
                evtshared->ats_firing_id = u_sess->tri_cxt.afterTriggers->firing_counter;
                event->ate_flags |= AFTER_TRIGGER_IN_PROGRESS;
                found = true;
            }
        }

        /*
         * If it's deferred, move it to move_list, if requested.
         */
        if (defer_it && move_list != NULL) {
            /* add it to move_list */
            afterTriggerAddEvent(move_list, event, evtshared);
            /* mark original copy "done" so we don't do it again */
            event->ate_flags |= AFTER_TRIGGER_DONE;
        }
    }

    return found;
}

/*
 * afterTriggerInvokeEvents
 *
 *	Scan the given event list for events that are marked as to be fired
 *	in the current firing cycle, and fire them.
 *
 *	If estate isn't NULL, we use its result relation info to avoid repeated
 *	openings and closing of trigger target relations.  If it is NULL, we
 *	make one locally to cache the info in case there are multiple trigger
 *	events per rel.
 *
 *	When delete_ok is TRUE, it's safe to delete fully-processed events.
 *	(We are not very tense about that: we simply reset a chunk to be empty
 *	if all its events got fired.  The objective here is just to avoid useless
 *	rescanning of events when a trigger queues new events during transaction
 *	end, so it's not necessary to worry much about the case where only
 *	some events are fired.)
 *
 *	Returns TRUE if no unfired events remain in the list (this allows us
 *	to avoid repeating afterTriggerMarkEvents).
 */
static bool afterTriggerInvokeEvents(AfterTriggerEventList* events, CommandId firing_id, EState* estate, bool delete_ok)
{
    bool all_fired = true;
    AfterTriggerEventChunk* chunk = NULL;
    MemoryContext per_tuple_context;
    bool local_estate = false;
    Relation rel = NULL;
    TriggerDesc* trigdesc = NULL;
    FmgrInfo* finfo = NULL;
    Instrumentation* instr = NULL;

    /* Make a local EState if need be */
    if (estate == NULL) {
        estate = CreateExecutorState();
        local_estate = true;
    }

    /* Make a per-tuple memory context for trigger function calls */
    per_tuple_context = AllocSetContextCreate(CurrentMemoryContext,
        "AfterTriggerTupleContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    for_each_chunk (chunk, *events) {
        AfterTriggerEvent event = NULL;
        bool all_fired_in_chunk = true;

        for_each_event (event, chunk) {
            AfterTriggerShared evtshared = GetTriggerSharedData(event);

            /*
             * Is it one for me to fire?
             */
            if ((event->ate_flags & AFTER_TRIGGER_IN_PROGRESS) && evtshared->ats_firing_id == firing_id) {
                /*
                 * So let's fire it... but first, find the correct relation if
                 * this is not the same relation as before.
                 */
                if (rel == NULL || RelationGetRelid(rel) != evtshared->ats_relid) {
                    ResultRelInfo* rInfo = NULL;

                    rInfo = ExecGetTriggerResultRel(estate, evtshared->ats_relid);
                    rel = rInfo->ri_RelationDesc;
                    trigdesc = rInfo->ri_TrigDesc;
                    finfo = rInfo->ri_TrigFunctions;
                    instr = rInfo->ri_TrigInstrument;
                    if (trigdesc == NULL) /* should not happen */
                        ereport(ERROR,
                            (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                                errmsg("relation %u has no triggers", evtshared->ats_relid)));
                }

                /*
                 * Fire it.  Note that the AFTER_TRIGGER_IN_PROGRESS flag is
                 * still set, so recursive examinations of the event list
                 * won't try to re-fire it.
                 */
                AfterTriggerExecute(event,
                    rel,
                    evtshared->ats_oldPartid,
                    evtshared->ats_newPartid,
                    evtshared->ats_bucketid,
                    trigdesc,
                    finfo,
                    instr,
                    per_tuple_context);

                /*
                 * Mark the event as done.
                 */
                event->ate_flags &= ~AFTER_TRIGGER_IN_PROGRESS;
                event->ate_flags |= AFTER_TRIGGER_DONE;
            } else if (!(event->ate_flags & AFTER_TRIGGER_DONE)) {
                /* something remains to be done */
                all_fired = all_fired_in_chunk = false;
            }
        }

        /* Clear the chunk if delete_ok and nothing left of interest */
        if (delete_ok && all_fired_in_chunk) {
            chunk->freeptr = CHUNK_DATA_START(chunk);
            chunk->endfree = chunk->endptr;

            /*
             * If it's last chunk, must sync event list's tailfree too.  Note
             * that delete_ok must NOT be passed as true if there could be
             * stacked AfterTriggerEventList values pointing at this event
             * list, since we'd fail to fix their copies of tailfree.
             */
            if (chunk == events->tail)
                events->tailfree = chunk->freeptr;
        }
    }

    /* Release working resources */
    MemoryContextDelete(per_tuple_context);

    if (local_estate) {
        ListCell* l = NULL;

        foreach (l, estate->es_trig_target_relations) {
            ResultRelInfo* resultRelInfo = (ResultRelInfo*)lfirst(l);

            /* Close indices and then the relation itself */
            ExecCloseIndices(resultRelInfo);
            heap_close(resultRelInfo->ri_RelationDesc, NoLock);
        }
        FreeExecutorState(estate);
    }

    return all_fired;
}
/* ----------
 * AfterTriggerBeginXact
 *
 *	Called at transaction start (either BEGIN or implicit for single
 *	statement outside of transaction block).
 * ----------
 */
void AfterTriggerBeginXact(void)
{
    Assert(u_sess->tri_cxt.afterTriggers == NULL);
    int max_query_depth = 8;
    /*
     * Build empty after-trigger state structure
     */
    u_sess->tri_cxt.afterTriggers =
        (AfterTriggers)MemoryContextAlloc(u_sess->top_transaction_mem_cxt, sizeof(AfterTriggersData));

    u_sess->tri_cxt.afterTriggers->firing_counter = (CommandId)1; /* mustn't be 0 */

    u_sess->tri_cxt.afterTriggers->events.head = NULL;
    u_sess->tri_cxt.afterTriggers->events.tail = NULL;
    u_sess->tri_cxt.afterTriggers->events.tailfree = NULL;
    u_sess->tri_cxt.afterTriggers->query_depth = -1;
    
    u_sess->tri_cxt.afterTriggers->maxquerydepth = max_query_depth;

    /* Context for events is created only when needed */
    u_sess->tri_cxt.afterTriggers->event_cxt = NULL;

    /* Subtransaction stack is empty until/unless needed */
    u_sess->tri_cxt.afterTriggers->state_stack = NULL;
    u_sess->tri_cxt.afterTriggers->events_stack = NULL;
    u_sess->tri_cxt.afterTriggers->depth_stack = NULL;
    u_sess->tri_cxt.afterTriggers->firing_stack = NULL;
    u_sess->tri_cxt.afterTriggers->maxtransdepth = 0;

#ifdef PGXC
    /*
     * Even though these are only used on coordinator, better nullify them
     * always.
     */
    u_sess->tri_cxt.afterTriggers->xc_rowstores = NULL;
    u_sess->tri_cxt.afterTriggers->xc_max_rowstores = 0;
    u_sess->tri_cxt.afterTriggers->xc_rs_cxt = NULL;
#endif

    /*
     * All memory allocation should be tail of this function. Because if memory is not enough,
     * it will longjmp function AfterTriggerEndXact. We must initilize afterTriggers before here.
     */
    u_sess->tri_cxt.afterTriggers->query_stack = NULL;
    u_sess->tri_cxt.afterTriggers->state = NULL;
    u_sess->tri_cxt.afterTriggers->state = SetConstraintStateCreate(max_query_depth);
    /* We initialize the query stack to a reasonable size */
    u_sess->tri_cxt.afterTriggers->query_stack =
        (AfterTriggerEventList*)MemoryContextAlloc(u_sess->top_transaction_mem_cxt, max_query_depth * sizeof(AfterTriggerEventList));
}

/* ----------
 * AfterTriggerBeginQuery
 *
 *	Called just before we start processing a single query within a
 *	transaction (or subtransaction).  Set up to record AFTER trigger
 *	events queued by the query.  Note that it is allowed to have
 *	nested queries within a (sub)transaction.
 * ----------
 */
void AfterTriggerBeginQuery(void)
{
    AfterTriggerEventList* events = NULL;

    /* Must be inside a transaction */
    Assert(u_sess->tri_cxt.afterTriggers != NULL);

    /* Increase the query stack depth */
    u_sess->tri_cxt.afterTriggers->query_depth++;

    /*
     * Allocate more space in the query stack if needed.
     */
    if (u_sess->tri_cxt.afterTriggers->query_depth >= u_sess->tri_cxt.afterTriggers->maxquerydepth) {
        /* repalloc will keep the stack in the same context */
        int new_alloc = u_sess->tri_cxt.afterTriggers->maxquerydepth * 2;

        u_sess->tri_cxt.afterTriggers->query_stack = (AfterTriggerEventList*)repalloc(
            u_sess->tri_cxt.afterTriggers->query_stack, new_alloc * sizeof(AfterTriggerEventList));
        u_sess->tri_cxt.afterTriggers->maxquerydepth = new_alloc;
    }

    /* Initialize this query's list to empty */
    events = &u_sess->tri_cxt.afterTriggers->query_stack[u_sess->tri_cxt.afterTriggers->query_depth];
    events->head = NULL;
    events->tail = NULL;
    events->tailfree = NULL;

#ifdef PGXC
    /*
     * Cleanup the row store if left allocated by some other query at the
     * same query level. AfterTriggerEndQuery() should have cleaned it up, but
     * an aborted sub-transaction might leave some rowstores belonging to
     * queries called from inside the sub-transaction. For such queries,
     * possibly AfterTriggerEndQuery() might not have been called.
     */
    if (IS_PGXC_COORDINATOR)
        pgxc_ARFreeRowStoreForQuery(u_sess->tri_cxt.afterTriggers->query_depth);
#endif
}

/* ----------
 * IsAfterTriggerBegin
 *
 * Check if the query is begin.
 * This is used to prevent transaction in store procedure.
 * ----------
 */
bool IsAfterTriggerBegin(void)
{
    return ((u_sess->tri_cxt.afterTriggers == NULL || u_sess->tri_cxt.afterTriggers->query_depth == -1) ? false : true);
}

#ifdef PGXC

/* ----------
 * IsAnyAfterTriggerDeferred
 *
 * Check if there is any deferred trigger to fire.
 * This is used to preserve snapshot data in case an
 * error occurred in a transaction block.
 * ----------
 */
bool IsAnyAfterTriggerDeferred(void)
{
    AfterTriggerEventList* events = NULL;

    if (u_sess->tri_cxt.afterTriggers == NULL)
        return false;

    /* Is there are any deferred trigger to fire */
    events = &u_sess->tri_cxt.afterTriggers->events;
    if (events->head != NULL)
        return true;

    return false;
}
#endif

/* ----------
 * AfterTriggerEndQuery
 *
 *	Called after one query has been completely processed. At this time
 *	we invoke all AFTER IMMEDIATE trigger events queued by the query, and
 *	transfer deferred trigger events to the global deferred-trigger list.
 *
 *	Note that this must be called BEFORE closing down the executor
 *	with ExecutorEnd, because we make use of the EState's info about
 *	target relations.  Normally it is called from ExecutorFinish.
 * ----------
 */
void AfterTriggerEndQuery(EState* estate)
{
    AfterTriggerEventList* events = NULL;

    /* Must be inside a transaction */
    Assert(u_sess->tri_cxt.afterTriggers != NULL);

    /* Must be inside a query, too */
    Assert(u_sess->tri_cxt.afterTriggers->query_depth >= 0);

    /*
     * Process all immediate-mode triggers queued by the query, and move the
     * deferred ones to the main list of deferred events.
     *
     * Notice that we decide which ones will be fired, and put the deferred
     * ones on the main list, before anything is actually fired.  This ensures
     * reasonably sane behavior if a trigger function does SET CONSTRAINTS ...
     * IMMEDIATE: all events we have decided to defer will be available for it
     * to fire.
     *
     * We loop in case a trigger queues more events at the same query level
     * (is that even possible?).  Be careful here: firing a trigger could
     * result in query_stack being repalloc'd, so we can't save its address
     * across afterTriggerInvokeEvents calls.
     *
     * If we find no firable events, we don't have to increment
     * firing_counter.
     */
    for (;;) {
        events = &u_sess->tri_cxt.afterTriggers->query_stack[u_sess->tri_cxt.afterTriggers->query_depth];
        if (afterTriggerMarkEvents(events, &u_sess->tri_cxt.afterTriggers->events, true)) {
            CommandId firing_id = u_sess->tri_cxt.afterTriggers->firing_counter++;

            /* OK to delete the immediate events after processing them */
            if (afterTriggerInvokeEvents(events, firing_id, estate, true))
                break; /* all fired */
        } else
            break;
    }

    /* Release query-local storage for events */
    afterTriggerFreeEventList(&u_sess->tri_cxt.afterTriggers->query_stack[u_sess->tri_cxt.afterTriggers->query_depth]);

#ifdef PGXC
    /* Cleanup the row store if created for this query */
    if (IS_PGXC_COORDINATOR)
        pgxc_ARFreeRowStoreForQuery(u_sess->tri_cxt.afterTriggers->query_depth);
#endif

    u_sess->tri_cxt.afterTriggers->query_depth--;
}

/* ----------
 * AfterTriggerFireDeferred
 *
 *	Called just before the current transaction is committed. At this
 *	time we invoke all pending DEFERRED triggers.
 *
 *	It is possible for other modules to queue additional deferred triggers
 *	during pre-commit processing; therefore xact.c may have to call this
 *	multiple times.
 * ----------
 */
void AfterTriggerFireDeferred(void)
{
    AfterTriggerEventList* events = NULL;
    bool snap_pushed = false;

    /* Must be inside a transaction */
    Assert(u_sess->tri_cxt.afterTriggers != NULL);

    /* ... but not inside a query */
    Assert(u_sess->tri_cxt.afterTriggers->query_depth == -1);

    /*
     * If there are any triggers to fire, make sure we have set a snapshot for
     * them to use.  (Since PortalRunUtility doesn't set a snap for COMMIT, we
     * can't assume ActiveSnapshot is valid on entry.)
     */
    events = &u_sess->tri_cxt.afterTriggers->events;
    if (events->head != NULL) {
        PushActiveSnapshot(GetTransactionSnapshot());
        snap_pushed = true;
    }

    /*
     * Run all the remaining triggers.	Loop until they are all gone, in case
     * some trigger queues more for us to do.
     */
    while (afterTriggerMarkEvents(events, NULL, false)) {
        CommandId firing_id = u_sess->tri_cxt.afterTriggers->firing_counter++;

        if (afterTriggerInvokeEvents(events, firing_id, NULL, true))
            break; /* all fired */
    }

    /*
     * We don't bother freeing the event list, since it will go away anyway
     * (and more efficiently than via pfree) in AfterTriggerEndXact.
     */
    if (snap_pushed)
        PopActiveSnapshot();
}

/* ----------
 * AfterTriggerEndXact
 *
 *	The current transaction is finishing.
 *
 *	Any unfired triggers are canceled so we simply throw
 *	away anything we know.
 *
 *	Note: it is possible for this to be called repeatedly in case of
 *	error during transaction abort; therefore, do not complain if
 *	already closed down.
 * ----------
 */
void AfterTriggerEndXact(bool isCommit)
{
    /*
     * Forget everything we know about AFTER triggers.
     *
     * Since all the info is in u_sess->top_transaction_mem_cxt or children thereof, we
     * don't really need to do anything to reclaim memory.  However, the
     * pending-events list could be large, and so it's useful to discard it as
     * soon as possible --- especially if we are aborting because we ran out
     * of memory for the list!
     */
    if (u_sess->tri_cxt.afterTriggers && u_sess->tri_cxt.afterTriggers->event_cxt)
        MemoryContextDelete(u_sess->tri_cxt.afterTriggers->event_cxt);

#ifdef PGXC
    /* On similar lines, discard the rowstore memory. */
    if (IS_PGXC_COORDINATOR && u_sess->tri_cxt.afterTriggers && u_sess->tri_cxt.afterTriggers->xc_rs_cxt)
        MemoryContextDelete(u_sess->tri_cxt.afterTriggers->xc_rs_cxt);
#endif

    u_sess->tri_cxt.afterTriggers = NULL;
}

/*
 * AfterTriggerBeginSubXact
 *
 *	Start a subtransaction.
 */
void AfterTriggerBeginSubXact(void)
{
    int my_level = GetCurrentTransactionNestLevel();

    /*
     * Ignore call if the transaction is in aborted state.	(Probably
     * shouldn't happen?)
     */
    if (u_sess->tri_cxt.afterTriggers == NULL)
        return;

    /*
     * Allocate more space in the stacks if needed.  (Note: because the
     * minimum nest level of a subtransaction is 2, we waste the first couple
     * entries of each array; not worth the notational effort to avoid it.)
     */
    while (my_level >= u_sess->tri_cxt.afterTriggers->maxtransdepth) {
        int actual_space;
        int init_start_index = 0;
        if (u_sess->tri_cxt.afterTriggers->maxtransdepth == 0) {
            MemoryContext old_cxt;

            old_cxt = MemoryContextSwitchTo(u_sess->top_transaction_mem_cxt);

#define DEFTRIG_INITALLOC 8
            actual_space = DEFTRIG_INITALLOC + 1;
            u_sess->tri_cxt.afterTriggers->state_stack =
                (SetConstraintState*)palloc(actual_space * sizeof(SetConstraintState));
            u_sess->tri_cxt.afterTriggers->events_stack =
                (AfterTriggerEventList*)palloc(actual_space * sizeof(AfterTriggerEventList));
            u_sess->tri_cxt.afterTriggers->depth_stack = (int*)palloc(actual_space * sizeof(int));
            u_sess->tri_cxt.afterTriggers->firing_stack = (CommandId*)palloc(actual_space * sizeof(CommandId));
            u_sess->tri_cxt.afterTriggers->maxtransdepth = actual_space - 1;

            MemoryContextSwitchTo(old_cxt);
        } else {

            /* repalloc will keep the stacks in the same context */
            init_start_index = u_sess->tri_cxt.afterTriggers->maxtransdepth;
            actual_space = init_start_index * 2 + 1;

            u_sess->tri_cxt.afterTriggers->state_stack = (SetConstraintState*)repalloc(
                u_sess->tri_cxt.afterTriggers->state_stack, actual_space * sizeof(SetConstraintState));
            u_sess->tri_cxt.afterTriggers->events_stack = (AfterTriggerEventList*)repalloc(
                u_sess->tri_cxt.afterTriggers->events_stack, actual_space * sizeof(AfterTriggerEventList));
            u_sess->tri_cxt.afterTriggers->depth_stack =
                (int*)repalloc(u_sess->tri_cxt.afterTriggers->depth_stack, actual_space * sizeof(int));
            u_sess->tri_cxt.afterTriggers->firing_stack =
                (CommandId*)repalloc(u_sess->tri_cxt.afterTriggers->firing_stack, actual_space * sizeof(CommandId));
            u_sess->tri_cxt.afterTriggers->maxtransdepth = actual_space - 1;
        }

        for (int i = init_start_index + 1; i < actual_space; ++i) {
            u_sess->tri_cxt.afterTriggers->state_stack[i] = NULL;
            u_sess->tri_cxt.afterTriggers->events_stack[i].head = NULL;
            u_sess->tri_cxt.afterTriggers->events_stack[i].tail = NULL;
            u_sess->tri_cxt.afterTriggers->events_stack[i].tailfree = NULL;
            u_sess->tri_cxt.afterTriggers->depth_stack[i] = -1;
            u_sess->tri_cxt.afterTriggers->firing_stack[i] = 0;
        }
    }

    /*
     * Push the current information into the stack.  The SET CONSTRAINTS state
     * is not saved until/unless changed.  Likewise, we don't make a
     * per-subtransaction event context until needed.
     */
    u_sess->tri_cxt.afterTriggers->state_stack[my_level] = NULL;
    u_sess->tri_cxt.afterTriggers->events_stack[my_level] = u_sess->tri_cxt.afterTriggers->events;
    u_sess->tri_cxt.afterTriggers->depth_stack[my_level] = u_sess->tri_cxt.afterTriggers->query_depth;
    u_sess->tri_cxt.afterTriggers->firing_stack[my_level] = u_sess->tri_cxt.afterTriggers->firing_counter;
}

/*
 * AfterTriggerEndSubXact
 *
 *	The current subtransaction is ending.
 */
void AfterTriggerEndSubXact(bool isCommit)
{
    int my_level = GetCurrentTransactionNestLevel();
    SetConstraintState state = NULL;
    AfterTriggerEvent event = NULL;
    AfterTriggerEventChunk* chunk = NULL;
    CommandId subxact_firing_id;

    /*
     * Ignore call if the transaction is in aborted state.	(Probably
     * unneeded)
     */
    if (u_sess->tri_cxt.afterTriggers == NULL)
        return;

    /*
     * Pop the prior state if needed.
     */
    if (isCommit) {
        Assert(my_level < u_sess->tri_cxt.afterTriggers->maxtransdepth);
        /* If we saved a prior state, we don't need it anymore */
        state = u_sess->tri_cxt.afterTriggers->state_stack[my_level];
        if (state != NULL)
            pfree_ext(state);
        /* this avoids double pfree if error later: */
        u_sess->tri_cxt.afterTriggers->state_stack[my_level] = NULL;
        Assert(u_sess->tri_cxt.afterTriggers->query_depth == u_sess->tri_cxt.afterTriggers->depth_stack[my_level]);
    } else {
        /*
         * Aborting.  It is possible subxact start failed before calling
         * AfterTriggerBeginSubXact, in which case we mustn't risk touching
         * stack levels that aren't there.
         */
        if (my_level >= u_sess->tri_cxt.afterTriggers->maxtransdepth)
            return;

        /*
         * Release any event lists from queries being aborted, and restore
         * query_depth to its pre-subxact value.
         */
        while (u_sess->tri_cxt.afterTriggers->query_depth > u_sess->tri_cxt.afterTriggers->depth_stack[my_level]) {
            afterTriggerFreeEventList(
                &u_sess->tri_cxt.afterTriggers->query_stack[u_sess->tri_cxt.afterTriggers->query_depth]);
            u_sess->tri_cxt.afterTriggers->query_depth--;
        }
        Assert(u_sess->tri_cxt.afterTriggers->query_depth == u_sess->tri_cxt.afterTriggers->depth_stack[my_level]);

        /*
         * Restore the global deferred-event list to its former length,
         * discarding any events queued by the subxact.
         */
        afterTriggerRestoreEventList(
            &u_sess->tri_cxt.afterTriggers->events, &u_sess->tri_cxt.afterTriggers->events_stack[my_level]);

        /*
         * Restore the trigger state.  If the saved state is NULL, then this
         * subxact didn't save it, so it doesn't need restoring.
         */
        state = u_sess->tri_cxt.afterTriggers->state_stack[my_level];
        if (state != NULL) {
            pfree_ext(u_sess->tri_cxt.afterTriggers->state);
            u_sess->tri_cxt.afterTriggers->state = state;
        }
        /* this avoids double pfree if error later: */
        u_sess->tri_cxt.afterTriggers->state_stack[my_level] = NULL;

        /*
         * Scan for any remaining deferred events that were marked DONE or IN
         * PROGRESS by this subxact or a child, and un-mark them. We can
         * recognize such events because they have a firing ID greater than or
         * equal to the firing_counter value we saved at subtransaction start.
         * (This essentially assumes that the current subxact includes all
         * subxacts started after it.)
         */
        subxact_firing_id = u_sess->tri_cxt.afterTriggers->firing_stack[my_level];
        for_each_event_chunk (event, chunk, u_sess->tri_cxt.afterTriggers->events) {
            AfterTriggerShared evtshared = GetTriggerSharedData(event);

            if (event->ate_flags & (AFTER_TRIGGER_DONE | AFTER_TRIGGER_IN_PROGRESS)) {
                if (evtshared->ats_firing_id >= subxact_firing_id)
                    event->ate_flags &= ~(AFTER_TRIGGER_DONE | AFTER_TRIGGER_IN_PROGRESS);
            }
        }
    }
}

/*
 * Create an empty SetConstraintState with room for numalloc trigstates
 */
static SetConstraintState SetConstraintStateCreate(int numalloc)
{
    /* Behave sanely with numalloc == 0 */
    if (numalloc <= 0) {
        numalloc = 1;
    }

    /*
     * We assume that zeroing will correctly initialize the state values.
     */
    SetConstraintState state = (SetConstraintState)MemoryContextAllocZero(u_sess->top_transaction_mem_cxt,
        sizeof(SetConstraintStateData) + (numalloc - 1) * sizeof(SetConstraintTriggerData));

    state->numalloc = numalloc;

    return state;
}

/*
 * Copy a SetConstraintState
 */
static SetConstraintState SetConstraintStateCopy(const SetConstraintState origstate)
{
    SetConstraintState state;

    state = SetConstraintStateCreate(origstate->numstates);

    state->all_isset = origstate->all_isset;
    state->all_isdeferred = origstate->all_isdeferred;
    state->numstates = origstate->numstates;

    errno_t rc = EOK;
    if (origstate->numstates > 0) {
        rc = memcpy_s(state->trigstates,
            origstate->numstates * sizeof(SetConstraintTriggerData),
            origstate->trigstates,
            origstate->numstates * sizeof(SetConstraintTriggerData));
        securec_check(rc, "", "");
    }

    return state;
}

/*
 * Add a per-trigger item to a SetConstraintState.	Returns possibly-changed
 * pointer to the state object (it will change if we have to repalloc).
 */
static SetConstraintState SetConstraintStateAddItem(SetConstraintState state, Oid tgoid, bool tgisdeferred)
{
    const int max_newalloc = 8;
    if (state->numstates >= state->numalloc) {
        int newalloc = state->numalloc * 2;

        newalloc = Max(newalloc, max_newalloc); /* in case original has size 0 */
        state = (SetConstraintState)repalloc(
            state, sizeof(SetConstraintStateData) + (newalloc - 1) * sizeof(SetConstraintTriggerData));
        state->numalloc = newalloc;
        Assert(state->numstates < state->numalloc);
    }

    state->trigstates[state->numstates].sct_tgoid = tgoid;
    state->trigstates[state->numstates].sct_tgisdeferred = tgisdeferred;
    state->numstates++;

    return state;
}

/* ----------
 * AfterTriggerSetState
 *
 *	Execute the SET CONSTRAINTS ... utility command.
 * ----------
 */
void AfterTriggerSetState(ConstraintsSetStmt* stmt)
{
    int my_level = GetCurrentTransactionNestLevel();

    /*
     * Ignore call if we aren't in a transaction.  (Shouldn't happen?)
     */
    if (u_sess->tri_cxt.afterTriggers == NULL)
        return;

#ifdef PGXC
    /*
     * If there are any row stores allocated for AR triggers, we should mark
     * all of them deferred, so they don't get deallocated at the end of query.
     * We know that row store is only used for coordinator.
     */
    if (IS_PGXC_COORDINATOR && stmt->deferred)
        pgxc_ARMarkAllDeferred();
#endif

    /*
     * If in a subtransaction, and we didn't save the current state already,
     * save it so it can be restored if the subtransaction aborts.
     */
    if (my_level > 1 && u_sess->tri_cxt.afterTriggers->state_stack[my_level] == NULL) {
        u_sess->tri_cxt.afterTriggers->state_stack[my_level] =
            SetConstraintStateCopy(u_sess->tri_cxt.afterTriggers->state);
    }

    /*
     * Handle SET CONSTRAINTS ALL ...
     */
    if (stmt->constraints == NIL) {
        /*
         * Forget any previous SET CONSTRAINTS commands in this transaction.
         */
        u_sess->tri_cxt.afterTriggers->state->numstates = 0;

        /*
         * Set the per-transaction ALL state to known.
         */
        u_sess->tri_cxt.afterTriggers->state->all_isset = true;
        u_sess->tri_cxt.afterTriggers->state->all_isdeferred = stmt->deferred;
    } else {
        Relation conrel;
        Relation tgrel;
        List* conoidlist = NIL;
        List* tgoidlist = NIL;
        ListCell* lc = NULL;

        /*
         * Handle SET CONSTRAINTS constraint-name [, ...]
         *
         * First, identify all the named constraints and make a list of their
         * OIDs.  Since, unlike the SQL spec, we allow multiple constraints of
         * the same name within a schema, the specifications are not
         * necessarily unique.	Our strategy is to target all matching
         * constraints within the first search-path schema that has any
         * matches, but disregard matches in schemas beyond the first match.
         * (This is a bit odd but it's the historical behavior.)
         */
        conrel = heap_open(ConstraintRelationId, AccessShareLock);

        foreach (lc, stmt->constraints) {
            RangeVar* constraint = (RangeVar*)lfirst(lc);
            bool found = false;
            List* namespacelist = NIL;
            ListCell* nslc = NULL;

            if (constraint->catalogname) {
                if (strcmp(constraint->catalogname, get_and_check_db_name(u_sess->proc_cxt.MyDatabaseId, true)) != 0)
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("cross-database references are not implemented: \"%s.%s.%s\"",
                                constraint->catalogname,
                                constraint->schemaname,
                                constraint->relname)));
            }

            /*
             * If we're given the schema name with the constraint, look only
             * in that schema.	If given a bare constraint name, use the
             * search path to find the first matching constraint.
             */
            if (constraint->schemaname) {
                Oid namespaceId = LookupExplicitNamespace(constraint->schemaname);

                namespacelist = list_make1_oid(namespaceId);
            } else {
                namespacelist = fetch_search_path(true);
            }

            found = false;
            foreach (nslc, namespacelist) {
                Oid namespaceId = lfirst_oid(nslc);
                SysScanDesc conscan;
                ScanKeyData skey[2];
                HeapTuple tup;

                ScanKeyInit(&skey[0],
                    Anum_pg_constraint_conname,
                    BTEqualStrategyNumber,
                    F_NAMEEQ,
                    CStringGetDatum(constraint->relname));
                ScanKeyInit(&skey[1],
                    Anum_pg_constraint_connamespace,
                    BTEqualStrategyNumber,
                    F_OIDEQ,
                    ObjectIdGetDatum(namespaceId));

                conscan = systable_beginscan(conrel, ConstraintNameNspIndexId, true, NULL, 2, skey);

                while (HeapTupleIsValid(tup = systable_getnext(conscan))) {
                    Form_pg_constraint con = (Form_pg_constraint)GETSTRUCT(tup);
                    if (con->condeferrable) {
                        if (con->contype != CONSTRAINT_CHECK) {
                            TrForbidAccessRbObject(ConstraintRelationId, HeapTupleGetOid(tup), constraint->relname);
                            conoidlist = lappend_oid(conoidlist, HeapTupleGetOid(tup));
                        } else {
                            Relation rel;
                            rel = relation_open(con->conrelid, NoLock);
                            CacheInvalidateRelcache(rel);
                            HeapTuple copyTuple;
                            Form_pg_constraint copy_con;
                            copyTuple = (HeapTuple) tableam_tops_copy_tuple(tup);
                            copy_con = (Form_pg_constraint)GETSTRUCT(copyTuple);
                            copy_con->condeferred = stmt->deferred;
                            simple_heap_update(conrel, &copyTuple->t_self, copyTuple);
                            CatalogUpdateIndexes(conrel, copyTuple);
                            tableam_tops_free_tuple(copyTuple);
                            relation_close(rel, NoLock);
                        }
                    } else if (stmt->deferred) {
                        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                            errmsg("constraint \"%s\" is not deferrable", constraint->relname)));
                    }
                    found = true;
                }

                systable_endscan(conscan);

                /*
                 * Once we've found a matching constraint we do not search
                 * later parts of the search path.
                 */
                if (found)
                    break;
            }

            list_free_ext(namespacelist);

            /*
             * Not found ?
             */
            if (!found)
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("constraint \"%s\" does not exist", constraint->relname)));
        }

        heap_close(conrel, AccessShareLock);

        /*
         * Now, locate the trigger(s) implementing each of these constraints,
         * and make a list of their OIDs.
         */
        tgrel = heap_open(TriggerRelationId, AccessShareLock);

        foreach (lc, conoidlist) {
            Oid conoid = lfirst_oid(lc);
            bool found = false;
            ScanKeyData skey;
            SysScanDesc tgscan;
            HeapTuple htup;

            found = false;

            ScanKeyInit(&skey, Anum_pg_trigger_tgconstraint, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(conoid));

            tgscan = systable_beginscan(tgrel, TriggerConstraintIndexId, true, NULL, 1, &skey);

            while (HeapTupleIsValid(htup = systable_getnext(tgscan))) {
                Form_pg_trigger pg_trigger = (Form_pg_trigger)GETSTRUCT(htup);

                /*
                 * Silently skip triggers that are marked as non-deferrable in
                 * pg_trigger.	This is not an error condition, since a
                 * deferrable RI constraint may have some non-deferrable
                 * actions.
                 */
                if (pg_trigger->tgdeferrable)
                    tgoidlist = lappend_oid(tgoidlist, HeapTupleGetOid(htup));

                found = true;
            }

            systable_endscan(tgscan);
            /* Safety check: a deferrable constraint should have triggers */
            if (!found)
                ereport(ERROR,
                    (errcode(ERRCODE_TRIGGERED_ACTION_EXCEPTION),
                        errmsg("no triggers found for constraint with OID %u", conoid)));
        }

        heap_close(tgrel, AccessShareLock);

        /*
         * Now we can set the trigger states of individual triggers for this
         * xact.
         */
        foreach (lc, tgoidlist) {
            Oid tgoid = lfirst_oid(lc);
            SetConstraintState state = u_sess->tri_cxt.afterTriggers->state;
            bool found = false;
            int i;

            for (i = 0; i < state->numstates; i++) {
                if (state->trigstates[i].sct_tgoid == tgoid) {
                    state->trigstates[i].sct_tgisdeferred = stmt->deferred;
                    found = true;
                    break;
                }
            }
            if (!found) {
                u_sess->tri_cxt.afterTriggers->state = SetConstraintStateAddItem(state, tgoid, stmt->deferred);
            }
        }
    }

    /*
     * SQL99 requires that when a constraint is set to IMMEDIATE, any deferred
     * checks against that constraint must be made when the SET CONSTRAINTS
     * command is executed -- i.e. the effects of the SET CONSTRAINTS command
     * apply retroactively.  We've updated the constraints state, so scan the
     * list of previously deferred events to fire any that have now become
     * immediate.
     *
     * Obviously, if this was SET ... DEFERRED then it can't have converted
     * any unfired events to immediate, so we need do nothing in that case.
     */
    if (!stmt->deferred) {
        AfterTriggerEventList* events = &u_sess->tri_cxt.afterTriggers->events;
        bool snapshot_set = false;

        while (afterTriggerMarkEvents(events, NULL, true)) {
            CommandId firing_id = u_sess->tri_cxt.afterTriggers->firing_counter++;

            /*
             * Make sure a snapshot has been established in case trigger
             * functions need one.	Note that we avoid setting a snapshot if
             * we don't find at least one trigger that has to be fired now.
             * This is so that BEGIN; SET CONSTRAINTS ...; SET TRANSACTION
             * ISOLATION LEVEL SERIALIZABLE; ... works properly.  (If we are
             * at the start of a transaction it's not possible for any trigger
             * events to be queued yet.)
             */
            if (!snapshot_set) {
                PushActiveSnapshot(GetTransactionSnapshot());
                snapshot_set = true;
            }

            /*
             * We can delete fired events if we are at top transaction level,
             * but we'd better not if inside a subtransaction, since the
             * subtransaction could later get rolled back.
             */
            if (afterTriggerInvokeEvents(events, firing_id, NULL, !IsSubTransaction()))
                break; /* all fired */
        }

        if (snapshot_set)
            PopActiveSnapshot();
    }
}

/* ----------
 * AfterTriggerPendingOnRel
 *
 * Test to see if there are any pending after-trigger events for rel.
 *
 * This is used by TRUNCATE, CLUSTER, ALTER TABLE, etc to detect whether
 * it is unsafe to perform major surgery on a relation.  Note that only
 * local pending events are examined.  We assume that having exclusive lock
 * on a rel guarantees there are no unserviced events in other backends ---
 * but having a lock does not prevent there being such events in our own.
 *
 * In some scenarios it'd be reasonable to remove pending events (more
 * specifically, mark them DONE by the current subxact) but without a lot
 * of knowledge of the trigger semantics we can't do this in general.
 * ----------
 */
bool AfterTriggerPendingOnRel(Oid relid)
{
    AfterTriggerEvent event = NULL;
    AfterTriggerEventChunk* chunk = NULL;
    int depth;

    /* No-op if we aren't in a transaction.  (Shouldn't happen?) */
    if (u_sess->tri_cxt.afterTriggers == NULL)
        return false;

    /* Scan queued events */
    for_each_event_chunk (event, chunk, u_sess->tri_cxt.afterTriggers->events) {
        AfterTriggerShared evtshared = GetTriggerSharedData(event);

        /*
         * We can ignore completed events.	(Even if a DONE flag is rolled
         * back by subxact abort, it's OK because the effects of the TRUNCATE
         * or whatever must get rolled back too.)
         */
        if (event->ate_flags & AFTER_TRIGGER_DONE)
            continue;

        if (evtshared->ats_relid == relid)
            return true;
    }

    /*
     * Also scan events queued by incomplete queries.  This could only matter
     * if TRUNCATE/etc is executed by a function or trigger within an updating
     * query on the same relation, which is pretty perverse, but let's check.
     */
    for (depth = 0; depth <= u_sess->tri_cxt.afterTriggers->query_depth; depth++) {
        for_each_event_chunk (event, chunk, u_sess->tri_cxt.afterTriggers->query_stack[depth]) {
            AfterTriggerShared evtshared = GetTriggerSharedData(event);

            if (event->ate_flags & AFTER_TRIGGER_DONE)
                continue;

            if (evtshared->ats_relid == relid)
                return true;
        }
    }

    return false;
}

/* ----------
 * AfterTriggerSaveEvent
 *
 *	Called by ExecA[RS]...Triggers() to queue up the triggers that should
 *	be fired for an event.
 *
 *	NOTE: this is called whenever there are any triggers associated with
 *	the event (even if they are disabled).	This function decides which
 *	triggers actually need to be queued.
 * ----------
 */
static void AfterTriggerSaveEvent(EState* estate, ResultRelInfo* relinfo, uint32 event, bool row_trigger,
    Oid oldPartitionOid, Oid newPartitionOid, int2 bucketid, HeapTuple oldtup, HeapTuple newtup, List* recheckIndexes,
    Bitmapset* modifiedCols)
{
    Relation rel = relinfo->ri_RelationDesc;
    TriggerDesc* trigdesc = get_and_check_trigdesc_value(relinfo->ri_TrigDesc);
    AfterTriggerEventData new_event;
    AfterTriggerSharedData new_shared;
    int tgtype_event;
    int tgtype_level;
    int i;
#ifdef PGXC
    bool is_deferred = false;
    bool event_added = false;
    bool is_remote_relation = (RelationGetLocInfo(rel) != NULL);
    bool exec_all_triggers = false;

#endif

    /*
     * Check state.  We use normal tests not Asserts because it is possible to
     * reach here in the wrong state given misconfigured RI triggers, in
     * particular deferring a cascade action trigger.
     */
    if (u_sess->tri_cxt.afterTriggers == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_TRIGGERED_ACTION_EXCEPTION),
                errmsg("AfterTriggerSaveEvent() called outside of transaction")));
    }
    if (u_sess->tri_cxt.afterTriggers->query_depth < 0)
        ereport(ERROR,
            (errcode(ERRCODE_TRIGGERED_ACTION_EXCEPTION), errmsg("AfterTriggerSaveEvent() called outside of query")));

        /*
         * Validate the event code and collect the associated tuple CTIDs.
         *
         * The event code will be used both as a bitmask and an array offset, so
         * validation is important to make sure we don't walk off the edge of our
         * arrays.
         */
#ifdef PGXC
        /*
         * PGXC: For coordinator, oldtup or newtup can have invalid ctid because the
         * ctid is not present. But we still keep the ItemPointerCopy() functions
         * below as-is so as to keep the code untouched and thus prevent any PG
         * merge conflicts. We anyways set the tuplestore row position subsequently.
         */
#endif
    new_event.ate_flags = 0;
    switch (event) {
        case TRIGGER_EVENT_INSERT:
            tgtype_event = TRIGGER_TYPE_INSERT;
            if (row_trigger) {
                Assert(oldtup == NULL);
                Assert(newtup != NULL);
                ItemPointerCopy(&(newtup->t_self), &(new_event.ate_ctid1));
                ItemPointerSetInvalid(&(new_event.ate_ctid2));
            } else {
                Assert(oldtup == NULL);
                Assert(newtup == NULL);
                ItemPointerSetInvalid(&(new_event.ate_ctid1));
                ItemPointerSetInvalid(&(new_event.ate_ctid2));
            }
            break;
        case TRIGGER_EVENT_DELETE:
            tgtype_event = TRIGGER_TYPE_DELETE;
            if (row_trigger) {
                Assert(oldtup != NULL);
                Assert(newtup == NULL);
                ItemPointerCopy(&(oldtup->t_self), &(new_event.ate_ctid1));
                ItemPointerSetInvalid(&(new_event.ate_ctid2));
            } else {
                Assert(oldtup == NULL);
                Assert(newtup == NULL);
                ItemPointerSetInvalid(&(new_event.ate_ctid1));
                ItemPointerSetInvalid(&(new_event.ate_ctid2));
            }
            break;
        case TRIGGER_EVENT_UPDATE:
            tgtype_event = TRIGGER_TYPE_UPDATE;
            if (row_trigger) {
                Assert(oldtup != NULL);
                Assert(newtup != NULL);
                ItemPointerCopy(&(oldtup->t_self), &(new_event.ate_ctid1));
                ItemPointerCopy(&(newtup->t_self), &(new_event.ate_ctid2));
                new_event.ate_flags |= AFTER_TRIGGER_2CTIDS;
            } else {
                Assert(oldtup == NULL);
                Assert(newtup == NULL);
                ItemPointerSetInvalid(&(new_event.ate_ctid1));
                ItemPointerSetInvalid(&(new_event.ate_ctid2));
            }
            break;
        case TRIGGER_EVENT_TRUNCATE:
            tgtype_event = TRIGGER_TYPE_TRUNCATE;
            Assert(oldtup == NULL);
            Assert(newtup == NULL);
            ItemPointerSetInvalid(&(new_event.ate_ctid1));
            ItemPointerSetInvalid(&(new_event.ate_ctid2));
            break;
        default:
            ereport(ERROR, ((errcode(ERRCODE_CASE_NOT_FOUND), errmsg("invalid after-trigger event code: %u", event))));
            tgtype_event = 0; /* keep compiler quiet */
            break;
    }

    tgtype_level = (row_trigger ? TRIGGER_TYPE_ROW : TRIGGER_TYPE_STATEMENT);

#ifdef PGXC
    /*
     * Know whether we should fire triggers on this node. But since internal
     * triggers are an exception, we cannot bail out here.
     */
    exec_all_triggers =
        pgxc_should_exec_triggers(relinfo->ri_RelationDesc, tgtype_event, tgtype_level, TRIGGER_TYPE_AFTER, estate);

    /*
     * Just save the position where the row would go *if* it gets inserted.
     * We are not sure whether it needs to be inserted because possibly in
     * the below loop, none of the trigger events will be inserted, in
     * which case we don't want to save the row; so don't yet add the row
     * into the rowstore. But we do want to know the row position beforehand
     * because the row position needs to be saved in each of the events that
     * get inserted below.
     */
    if (is_remote_relation && exec_all_triggers) {
        if (row_trigger) {
            pgxc_ARNextNewRowpos(&new_event.xc_ate_row);
        } else
            RowPointerSetInvalid(&(new_event.xc_ate_row));
        /* We never use ctid2 field */
        ItemPointerSetInvalid(&(new_event.ate_ctid2));
    }
#endif

    for (i = 0; i < trigdesc->numtriggers; i++) {
        Trigger* trigger = &trigdesc->triggers[i];

#ifdef PGXC
        if (!pgxc_is_trigger_firable(rel, trigger, exec_all_triggers))
            continue;
#endif

        if (!TRIGGER_TYPE_MATCHES(trigger->tgtype, tgtype_level, TRIGGER_TYPE_AFTER, tgtype_event))
            continue;
        if (!TriggerEnabled(estate, relinfo, trigger, event, modifiedCols, oldtup, newtup))
            continue;

        /*
         * If this is an UPDATE of a PK table or FK table that does not change
         * the PK or FK respectively, we can skip queuing the event: there is
         * no need to fire the trigger.
         */
        if (TRIGGER_FIRED_BY_UPDATE(event)) {
            switch (RI_FKey_trigger_type(trigger->tgfoid)) {
                case RI_TRIGGER_PK:
                    /* Update on PK table */
                    if (RI_FKey_keyequal_upd_pk(trigger, rel, oldtup, newtup)) {
                        /* key unchanged, so skip queuing this event */
                        continue;
                    }
                    break;

                case RI_TRIGGER_FK:

                    /*
                     * Update on FK table
                     *
                     * There is one exception when updating FK tables: if the
                     * updated row was inserted by our own transaction and the
                     * FK is deferred, we still need to fire the trigger. This
                     * is because our UPDATE will invalidate the INSERT so the
                     * end-of-transaction INSERT RI trigger will not do
                     * anything, so we have to do the check for the UPDATE
                     * anyway.
                     */
                    if (!TransactionIdIsCurrentTransactionId(HeapTupleGetRawXmin(oldtup)) &&
                        RI_FKey_keyequal_upd_fk(trigger, rel, oldtup, newtup)) {
                        continue;
                    }
                    break;

                case RI_TRIGGER_NONE:
                    /* Not an FK trigger */
                    break;
                default:
                    ereport(ERROR,
                        (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmsg("unrecognized RI_FKey_trigger_type: %d", RI_FKey_trigger_type(trigger->tgfoid))));             
            }
        }

        /*
         * If the trigger is a deferred unique constraint check trigger, only
         * queue it if the unique constraint was potentially violated, which
         * we know from index insertion time.
         */
        if (trigger->tgfoid == F_UNIQUE_KEY_RECHECK) {
            if (!list_member_oid(recheckIndexes, trigger->tgconstrindid))
                continue; /* Uniqueness definitely not violated */
        }

        /*
         * Fill in event structure and add it to the current query's queue.
         */
        new_shared.ats_event = (event & TRIGGER_EVENT_OPMASK) | (row_trigger ? TRIGGER_EVENT_ROW : 0) |
                               (trigger->tgdeferrable ? AFTER_TRIGGER_DEFERRABLE : 0) |
                               (trigger->tginitdeferred ? AFTER_TRIGGER_INITDEFERRED : 0);
        new_shared.ats_tgoid = trigger->tgoid;
        new_shared.ats_relid = RelationGetRelid(rel);
        new_shared.ats_oldPartid = oldPartitionOid;
        new_shared.ats_newPartid = newPartitionOid;
        new_shared.ats_bucketid = bucketid;
        new_shared.ats_firing_id = 0;

#ifdef PGXC
        if (is_remote_relation && IsRowPointerValid(&new_event.xc_ate_row)) {
            event_added = true;
            if (trigger->tginitdeferred)
                is_deferred = true;
        }
#endif

        afterTriggerAddEvent(&u_sess->tri_cxt.afterTriggers->query_stack[u_sess->tri_cxt.afterTriggers->query_depth],
            &new_event,
            &new_shared);
    }

#ifdef PGXC
    /*
     * If we have saved at least one row trigger event, save the
     * OLD and NEW row into the tuplestore.
     */
    if (is_remote_relation && event_added)
        pgxc_ARAddRow(oldtup, newtup, is_deferred);
#endif
}

Datum pg_trigger_depth(PG_FUNCTION_ARGS)
{
    PG_RETURN_INT32(u_sess->tri_cxt.MyTriggerDepth);
}

#ifdef PGXC

/*
 * Allocate a new row store entry in the row store array.
 */
static ARRowStore* pgxc_ar_alloc_rsentry(void)
{
    ARRowStore* rowstore =
        (ARRowStore*)MemoryContextAllocZero(u_sess->tri_cxt.afterTriggers->xc_rs_cxt, sizeof(ARRowStore));

    /* When actual tuplestore is not allocated, the current position is -1. */
    rowstore->rs_tupinfo[0].ti_curpos = -1;
    rowstore->rs_tupinfo[1].ti_curpos = -1;

    return rowstore;
}

/*
 * If rowstore is not yet initialized, allocate the rowstore context and the
 * rowstore array of size equal to the maxquerydepth. This function is implicitly
 * called when a row is to be added into the rowstore.
 */
static void pgxc_ar_init_rowstore(void)
{
    int new_numstores = u_sess->tri_cxt.afterTriggers->maxquerydepth;
    int query_index = u_sess->tri_cxt.afterTriggers->query_depth;
    MemoryContext oldContext;

    if (u_sess->tri_cxt.afterTriggers->xc_rs_cxt == NULL)
        u_sess->tri_cxt.afterTriggers->xc_rs_cxt = AllocSetContextCreate(u_sess->top_transaction_mem_cxt,
            "XC AR Trigger RowStore",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE);

    oldContext = MemoryContextSwitchTo(u_sess->tri_cxt.afterTriggers->xc_rs_cxt);

    /*
     * Expand the rowstore array upto the maxquerydepth value, so as to ensure
     * it can accommodate all the query levels.
     */
    if (new_numstores > u_sess->tri_cxt.afterTriggers->xc_max_rowstores) {
        if (!u_sess->tri_cxt.afterTriggers->xc_rowstores)
            u_sess->tri_cxt.afterTriggers->xc_rowstores = (ARRowStore**)palloc0(new_numstores * sizeof(ARRowStore*));
        else {
            u_sess->tri_cxt.afterTriggers->xc_rowstores = (ARRowStore**)repalloc(
                u_sess->tri_cxt.afterTriggers->xc_rowstores, new_numstores * sizeof(ARRowStore*));

            /* Set the new elements to NULL. */
            errno_t rc = EOK;
            rc = memset_s(&u_sess->tri_cxt.afterTriggers->xc_rowstores[u_sess->tri_cxt.afterTriggers->xc_max_rowstores],
                (new_numstores - u_sess->tri_cxt.afterTriggers->xc_max_rowstores) * sizeof(ARRowStore*),
                0,
                (new_numstores - u_sess->tri_cxt.afterTriggers->xc_max_rowstores) * sizeof(ARRowStore*));
            securec_check(rc, "\0", "\0");
        }
        u_sess->tri_cxt.afterTriggers->xc_max_rowstores = new_numstores;
    }

    /* Allocate the rowstore entry for the current query if not already */
    if (u_sess->tri_cxt.afterTriggers->xc_rowstores[query_index] == NULL)
        u_sess->tri_cxt.afterTriggers->xc_rowstores[query_index] = pgxc_ar_alloc_rsentry();

    (void)MemoryContextSwitchTo(oldContext);
}

/*
 * pgxc_ARFetchRow:
 * Given a rowstore location rpid, fetch the OLD and NEW row from the row store.
 */
static void pgxc_ARFetchRow(Relation rel, RowPointerData* rpid, HeapTuple* rs_tuple1, HeapTuple* rs_tuple2)
{
    ARRowStore* rs = rpid->rp_rsid;

    pgxc_ar_dofetch(rel, &rs->rs_tupinfo[0], rpid->rp_posid, rs_tuple1);
    pgxc_ar_dofetch(rel, &rs->rs_tupinfo[1], rpid->rp_posid, rs_tuple2);

    /* At least one out of OLD and NEW row should be present in the rowstore */
    Assert(*rs_tuple1 || *rs_tuple2);
}

/*
 * pgxc_ar_dofetch:
 * Retrieve the tuple at position fetchpos of the tuplestore.
 */
static void pgxc_ar_dofetch(Relation rel, ARTupInfo* rs_tupinfo, TsOffset fetchpos, HeapTuple* rs_tuple)
{
    TsOffset abs_pos;
    TsOffset relative_pos;
    TsOffset advance_by;
    bool forward = false;
    TupleTableSlot* slot = NULL;

    if (rs_tupinfo->tupstate == NULL) {
        /*
         * Empty tuplestore, this must the second tuplestore. Second one is used
         * only when both NEW and OLD row are present.
         */
        *rs_tuple = NULL;
        return;
    }

    Assert(fetchpos >= 0);

    /*
     * Is the position to be fetched closer to the start of the tuplestore, or
     * is it closer to the current readptr? Decide from where to scan depending
     * upon which one is closer to the fetchpos.
     */
    abs_pos = fetchpos;
    relative_pos = fetchpos - rs_tupinfo->ti_curpos;
    if (abs_pos < abs(relative_pos)) {
        /* Search from the tuplestore start */
        tuplestore_rescan(rs_tupinfo->tupstate);
        rs_tupinfo->ti_curpos = 0;
        advance_by = abs_pos;
        forward = true;
    } else {
        /* Search from the current position */
        advance_by = abs(relative_pos);
        forward = (relative_pos >= 0);
    }

    /*
     * A backward tuplestore_advance() when at eof does not actually shift back
     * the readptr, it only makes the eof status false; so we need to shift it
     * back ourselves.
     */
    if (!forward && tuplestore_ateof(rs_tupinfo->tupstate))
        (void)tuplestore_advance(rs_tupinfo->tupstate, false);

    for (; advance_by > 0; advance_by--) {
        if (!tuplestore_advance(rs_tupinfo->tupstate, forward)) {
            /* Should not happen */
            ereport(ERROR,
                (errcode(ERRCODE_TRIGGERED_ACTION_EXCEPTION),
                    errmsg("XC: Could not find the required row position %d for "
                           "AFTER ROW trigger",
                        fetchpos)));
        }

        /*
         * We need to increment the curpos counter incrementally alongwith
         * tuplestore_advance(). Otherwise if we throw an exception above,
         * the ti_curpos would be out-of-sync with the actual tupstore readptr.
         * If this is a sub-transaction, these global structures might continue
         * to be used in the outer transaction.
         */
        rs_tupinfo->ti_curpos += (forward ? 1 : -1);
    }

    Assert(rs_tupinfo->ti_curpos >= 0);

    /* Build table slot for this relation */
    slot = MakeSingleTupleTableSlot(RelationGetDescr(rel));

    if (!tuplestore_gettupleslot(rs_tupinfo->tupstate, true /* forward */, false /* copy */, slot)) {
        /* Should not happen */
        ereport(ERROR,
            (errcode(ERRCODE_TRIGGERED_ACTION_EXCEPTION),
                errmsg("XC: Could not find the required row position %d for"
                       "AFTER ROW trigger",
                    fetchpos)));
    }
    /* gettuple() implicitly advances to the next position */
    rs_tupinfo->ti_curpos++;

    /* Return a complete tuple. Tuplestore has fetched us a minimal tuple. */
    *rs_tuple = ExecCopySlotTuple(slot);

    ExecDropSingleTupleTableSlot(slot);
}

/*
 * pgxc_ar_goto_end:
 * Advance until tuplestore eof. This is used to retrieve the next new position
 * of a new tuple being appended. Typically in a series of event inserts, the
 * tuplestore stays at eof, so this call effectively has negligible cost. But
 * it is a must-have to ensure we get the correct new position if the current
 * position happens to be somewhere else in the tuplestore.
 * If tuplestore is not yet allocated, return -1.
 */
static int pgxc_ar_goto_end(ARTupInfo* rs_tupinfo)
{
    if (rs_tupinfo == NULL || rs_tupinfo->tupstate == NULL)
        return -1;

    while (tuplestore_advance(rs_tupinfo->tupstate, true /* forward */))
        rs_tupinfo->ti_curpos++;

    return rs_tupinfo->ti_curpos;
}

/*
 * Populate rpid with the next new tuple position in the row store.
 */
static void pgxc_ARNextNewRowpos(RowPointerData* rpid)
{
    int query_index = u_sess->tri_cxt.afterTriggers->query_depth;
    ARRowStore* rowstore = NULL;
    TsOffset rowpos1;
    TsOffset rowpos2;

    /* Initialize the array of rowstores if not already */
    pgxc_ar_init_rowstore();

    rowstore = u_sess->tri_cxt.afterTriggers->xc_rowstores[query_index];
    rpid->rp_rsid = rowstore;

    /*
     * New tuples always get appended at the end of the tuplestore. So we want
     * to go and get the last tuple position.
     */
    rowpos1 = pgxc_ar_goto_end(&rowstore->rs_tupinfo[0]);
    rowpos2 = pgxc_ar_goto_end(&rowstore->rs_tupinfo[1]);

    if (rowpos1 >= 0) {
        /* At least of the first tuplestore has rows */
        rpid->rp_posid = rowpos1;
        if (rowpos2 >= 0) {
            /* Both OLD and NEW are present 
             *
             * Both of them should have the same current row position.
             */
            Assert(rowpos1 == rowpos2);
        }
    } else {
        /* If rs_tupinfo[0] is not valid, it means the tuplestores don't yet
         * contain rows. The very first record will always be at position 0.
         */
        rpid->rp_posid = 0;

        /* rs_tupinfo[1] cannot have anything if rs_tupinfo[0] is empty */
        Assert(rowpos2 < 0);
    }
}

/* Allocate and initialize the actual tuplestore */
static void pgxc_ar_init_tupinfo(ARTupInfo* rs_tupinfo)
{
    if (rs_tupinfo->tupstate == NULL) {
        MemoryContext oldcxt = MemoryContextSwitchTo(u_sess->tri_cxt.afterTriggers->xc_rs_cxt);
        rs_tupinfo->tupstate = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
        (void)MemoryContextSwitchTo(oldcxt);

        rs_tupinfo->ti_curpos = 0;
    }
}

/*
 * Append the given tuple into the given tuplestore and if required update the
 * current position.
 */
static void pgxc_ar_doadd(HeapTuple tuple, ARTupInfo* rs_tupinfo)
{
    if (rs_tupinfo->tupstate == NULL)
        pgxc_ar_init_tupinfo(rs_tupinfo);

    tuplestore_puttuple(rs_tupinfo->tupstate, tuple);

    /*
     * If tuplestore is at eof, the readptr gets implicitly incremented on new
     * tuple addition, and thus stays at eof. So we want to accordingly
     * increment our curpos.
     */
    if (tuplestore_ateof(rs_tupinfo->tupstate))
        rs_tupinfo->ti_curpos++;
}

/*
 * Place the OLD and/or NEW row into the row store.
 * If is_deferred is true, it means at least one the events using this row
 * belong to a deferred trigger. If it is true, mark the row store deferred,
 * so that it won't get cleaned up at query end.
 */
static void pgxc_ARAddRow(HeapTuple oldtup, HeapTuple newtup, bool is_deferred)
{
    int query_index = u_sess->tri_cxt.afterTriggers->query_depth;
    ARRowStore* rowstore = NULL;
    HeapTuple firsttup;
    HeapTuple secondtup;

    /* Initialize the array of rowstores if not already */
    pgxc_ar_init_rowstore();

    rowstore = u_sess->tri_cxt.afterTriggers->xc_rowstores[query_index];
    if (is_deferred)
        rowstore->rs_has_deferred = true;

    Assert(oldtup || newtup);

    /*
     * When only one of the oldtuple and newtuple is present, (INSERT or DELETE)
     * add it to the first entry, else if both are present, add the newtup into
     * the 2nd entry. This goes in line with PG where LocTriggerData.tg_trigtuple
     * is always allocated and LocTriggerData.tg_newtuple is allocated only if
     * both the tuples are present (UPDATE).
     */
    if (oldtup == NULL || newtup == NULL) {
        firsttup = (oldtup ? oldtup : newtup);
        secondtup = NULL;
    } else {
        firsttup = oldtup;
        secondtup = newtup;
    }

    if (firsttup)
        pgxc_ar_doadd(firsttup, &rowstore->rs_tupinfo[0]);
    if (secondtup)
        pgxc_ar_doadd(secondtup, &rowstore->rs_tupinfo[1]);
}

/* pgxc_ARFreeRowStoreForQuery:
 * Cleanup the row store memory allocated for the given query, unless it is
 * marked deferred. But always set the row store array entry to NULL so that
 * subsequent queries at the same query level will be able to allocate their
 * own tuplestores.
 */
static void pgxc_ARFreeRowStoreForQuery(int query_index)
{
    ARRowStore** rowstores = u_sess->tri_cxt.afterTriggers->xc_rowstores;
    Assert(query_index >= 0);

    /*
     * If there were no AR triggers queued for this query, we would not have
     * any rows stored for this query.
     */
    if (u_sess->tri_cxt.afterTriggers->xc_max_rowstores < query_index + 1 || rowstores == NULL ||
        rowstores[query_index] == NULL)
        return;

    /*
     * If the particular row store has one or more rows that are used for
     * deferred triggers, we want to retain this row store until the end of
     * transaction, so cleanup the tuplestore entry but not the actual
     * tuplestore. Since the tuplestore is allocated in afterTrigger context,
     * it will automatically go away in AfterTriggerEndXact(). Note that the
     * handle to this tuplestore is not lost; it is saved in event->xc_ate_row.
     *
     * Note: Typically in a given tuplestore, either all rows belong to deferred
     * triggers or all rows belong to immediate triggers, except when
     * set-constraints-deferred is fired for a specified trigger, in which case
     * all tuplestores are marked deferred even though only one trigger is
     * deferred. We do not go to the extent of finding the corresponding row
     * and moving it from its tuplestore to some deferred-only tuplestore, one of
     * the reasons being that the row position would change for such row, so the
     * events associated with this row won't be able to access the row.
     */
    if (rowstores[query_index]->rs_has_deferred == false) {
        int i;
        /* Free OLD and NEW row tuplestores. */
        for (i = 0; i < ARTUP_NUM; i++) {
            Tuplestorestate* tupstate = rowstores[query_index]->rs_tupinfo[i].tupstate;
            if (tupstate != NULL)
                tuplestore_end(tupstate);
        }
        pfree_ext(rowstores[query_index]);
    }
    /*
     * But in any case, we do want to nullify the rowstore entry so that the next
     * query in the transaction would be able to allocate its own rowstore in this
     * entry.
     */
    rowstores[query_index] = NULL;
}

/*
 * pgxc_ARMarkAllDeferred:
 * Mark all the row stores as having deferred trigger rows. This ensures that
 * all of them will stay until the end of transaction so that the deferred
 * trigger events will be able to access the rows from the rowstore.
 * This function is called whenever set-constraints-deferred is executed.
 */
static void pgxc_ARMarkAllDeferred(void)
{
    int rs_index;
    ARRowStore* rowstore = NULL;

    if (u_sess->tri_cxt.afterTriggers == NULL)
        return;

    for (rs_index = 0; rs_index < u_sess->tri_cxt.afterTriggers->xc_max_rowstores; rs_index++) {
        rowstore = u_sess->tri_cxt.afterTriggers->xc_rowstores[rs_index];

        if (rowstore != NULL)
            rowstore->rs_has_deferred = true;
    }
}

/*
 * pgxc_has_trigger_for_event: Return true if it can be determined without
 * peeking into each of the trigger that there is a trigger present for
 * the given event.
 */
bool pgxc_has_trigger_for_event(int16 tg_event, TriggerDesc* trigdesc)
{
#define ANY_TRIGGER_MATCHES(trigdesc, event)                                                     \
    ((trigdesc)->trig_##event##_before_row || (trigdesc)->trig_##event##_after_row ||            \
        (trigdesc)->trig_##event##_instead_row || (trigdesc)->trig_##event##_before_statement || \
        (trigdesc)->trig_##event##_after_statement)

    Assert(trigdesc != NULL);

    switch (tg_event) {
        case TRIGGER_TYPE_INSERT:
            return ANY_TRIGGER_MATCHES(trigdesc, insert);
        case TRIGGER_TYPE_UPDATE:
            return ANY_TRIGGER_MATCHES(trigdesc, update);
        case TRIGGER_TYPE_DELETE:
            return ANY_TRIGGER_MATCHES(trigdesc, delete);
        case TRIGGER_TYPE_TRUNCATE:
            return (trigdesc->trig_truncate_before_statement || trigdesc->trig_truncate_after_statement);
        case CMD_SELECT:
        default:
            Assert(0); /* Shouldn't come here */
    }

    /* For Compiler's sake */
    return false;
}

/* pgxc_get_trigevent: Converts the command type into a trigger event type */
int16 pgxc_get_trigevent(CmdType commandType)
{
    int16 ret = 0;

    switch (commandType) {
        case CMD_INSERT:
            ret = TRIGGER_TYPE_INSERT;
            break;
        case CMD_UPDATE:
            ret = TRIGGER_TYPE_UPDATE;
            break;
        case CMD_DELETE:
            ret = TRIGGER_TYPE_DELETE;
            break;
        case CMD_MERGE:
            break;
        case CMD_UTILITY:
            /*
             * Assume this function is called only for TRUNCATE and no other
             * utility statement.
             */
            ret = TRIGGER_TYPE_TRUNCATE;
            break;
        case CMD_SELECT:
        default:
            Assert(0); /* Shouldn't come here */
    }

    return ret;
}

inline bool CheckTriggerType(TriggerDesc* trigdesc, uint16 tgtype_event)
{
    return trigdesc != NULL && pgxc_has_trigger_for_event(tgtype_event, trigdesc);
}

/*
 * pgxc_should_exec_triggers:
 * Return true if it is determined that all of the triggers for the relation
 * should be executed here, on this node.
 * On coordinator, returns true if there is at least one non-shippable
 * trigger for the relation that matches the given event, level and timing.
 * On datanode (or for any local-only table for that matter), returns false if
 * all of the matching triggers are shippable.
 *
 * PG behaviour is such that the triggers for the same table should be executed
 * in alphabetical order. This make it essential to execute all the triggers
 * on the same node, be it coordinator or datanode. So the idea used is: if all
 * matching triggers are shippable, they should be executed for local tables
 * (i.e. for datanodes). Even if there is a single trigger that is not
 * shippable, all the triggers should be fired on remote tables (i.e. on
 * coordinator) . This ensures that either all the triggers are executed on
 * coordinator, or all are executed on datanodes.
 */
static bool pgxc_should_exec_triggers(
    Relation rel, uint16 tgtype_event, int16 tgtype_level, int16 tgtype_timing, EState* estate)
{
    bool has_nonshippable = false;

    /* Don't need check exec node for trigger in single_node mode. */
    if (IS_SINGLE_NODE)
        return true;

    /*
     * First rule out the INSTEAD trigger case. INSTEAD triggers should always
     * be executed on coordinator because they are defined only for views and
     * views are defined only on coordinator.
     */
    if (TRIGGER_FOR_INSTEAD(tgtype_timing))
        return (IS_PGXC_COORDINATOR);

    /*
     * On datanode, it is not possible to know if the query we are executing is
     * actually an FQS. Also, for non-FQS queries, statement triggers should
     * anyway be executed on coordinator only because the non-FQS query executes
     * for each of the rows processed so these would cause stmt triggers to
     * be fired multiple times if we choose to fire them on datanode. So the
     * safest bet is to *always* fire stmt triggers on coordinator. For FQS'ed
     * query, these get explicitly fired during RemoteQuery execution on
     * coordinator.
     */
    if (tgtype_level == TRIGGER_TYPE_STATEMENT)
        return (IS_PGXC_COORDINATOR);

    /* check whether exec trigger when u_sess->attr.attr_sql.enable_trigger_shipping is on. */
    if (u_sess->attr.attr_sql.enable_trigger_shipping && estate != NULL)
        return pgxc_should_exec_trigship(rel, estate);

    /* Do we have any non-shippable trigger for the given event and timing ? */
    has_nonshippable = pgxc_find_nonshippable_row_trig(rel, tgtype_event, tgtype_timing, false);

    if (IS_PGXC_COORDINATOR) {
        /* If enable_trigger_shipping is off, directly return true on CN. */
        if (!u_sess->attr.attr_sql.enable_trigger_shipping && CheckTriggerType(rel->trigdesc, tgtype_event))
            return true;

        if (RelationGetLocInfo(rel)) {
            /*
             * So this is a typical coordinator table that has locator info.
             * This means the query would execute on datanodes as well. So fire
             * all of them on coordinator if they have a non-shippable trigger.
             */
            if (has_nonshippable)
                return true;
        } else {
            /*
             * For a local-only table, we know for sure that this query won't reach
             * datanode. So ensure we execute all triggers here at the coordinator.
             */
            return true;
        }
    } else {
        /* Directly return false on DN. */
        if (!u_sess->attr.attr_sql.enable_trigger_shipping && CheckTriggerType(rel->trigdesc, tgtype_event))
            return false;

        /*
         * On datanode, it is straightforward; just execute if all are
         * shippable. Coordinator would have skipped such triggers.
         */
        if (!has_nonshippable)
            return true;
    }

    /* In all other cases, this is not the correct node to fire triggers */
    return false;
}

/*
 * pgxc_should_exec_br_trigger:
 * Returns true if the BR trigger if present should be executed here on this
 * node. If BR triggers are not present, always returns false.
 * The BR trigger should be fired on coordinator if either of BR or AR trigger
 * is non-shippable. Even if there is a AR non-shippable trigger and shippable
 * BR trigger, we should still execute the BR trigger on coordinator. Once we
 * know that the AR triggers are going to be executed on coordinator, there's
 * no point in executing BR trigger on datanode and then fetching the updated
 * OLD row back to the coordinator so that AR trigger can use them. Also, we
 * would have needed to fetch the BR trigger tuple by using RETURNING, which
 * means additional changes to handle this.
 */
bool pgxc_should_exec_br_trigger(Relation rel, int16 trigevent, EState* estate)
{
    bool has_nonshippable_row_triggers = false;

    /* Don't need check exec node for trigger in single_node mode. */
    if (IS_SINGLE_NODE)
        return true;

    /* check whether exec trigger when u_sess->attr.attr_sql.enable_trigger_shipping is on. */
    if (u_sess->attr.attr_sql.enable_trigger_shipping && estate != NULL)
        return pgxc_should_exec_trigship(rel, estate);

    /*
     * If we don't have BR triggers in the first place, then presence of AR
     * triggers should not matter; we should always return false.
     */
    if (!rel->trigdesc || (TRIGGER_FOR_UPDATE(trigevent) && !rel->trigdesc->trig_update_before_row) ||
        (TRIGGER_FOR_INSERT(trigevent) && !rel->trigdesc->trig_insert_before_row) ||
        (TRIGGER_FOR_DELETE(trigevent) && !rel->trigdesc->trig_delete_before_row))
        return false;

    /* Check presence of AR or BR triggers that are non-shippable */
    has_nonshippable_row_triggers = pgxc_find_nonshippable_row_trig(rel, trigevent, TRIGGER_TYPE_BEFORE, false) ||
                                    pgxc_find_nonshippable_row_trig(rel, trigevent, TRIGGER_TYPE_AFTER, false);

    if (RelationGetLocInfo(rel) && has_nonshippable_row_triggers)
        return true;
    if (!RelationGetLocInfo(rel) && !has_nonshippable_row_triggers)
        return true;

    return false;
}

/*
 * pgxc_trig_oldrow_reqd:
 * To handle triggers from coordinator, we require OLD row to be fetched from the
 * source plan if we know there are triggers that are going to be executed on
 * coordinator.
 * This function is to be called only in case of non-local tables.
 * For local tables, the OLD row is required in PG for views (INSTEAD triggers)
 * which is already handled in PG.
 */
bool pgxc_trig_oldrow_reqd(Relation rel, CmdType commandType)
{
    int16 trigevent = pgxc_get_trigevent(commandType);

    /* Should be called only for remote tables */
    Assert(RelationGetLocInfo(rel));

    /*
     * We require OLD row if we are going to execute BR triggers on coordinator,
     * and also when we are going to execute AR triggers on coordinator.
     */
    if (pgxc_should_exec_br_trigger(rel, trigevent) ||
        pgxc_should_exec_triggers(rel, trigevent, TRIGGER_TYPE_ROW, TRIGGER_TYPE_AFTER))
        return true;

    return false;
}

/*
 * pgxc_is_trigger_firable:
 * This function is defined only to handle the special case if the trigger is
 * an internal trigger. Once we support global constraints, we should not
 * handle this as a special case: global constraint triggers would be executed
 * just like normal triggers. Internal triggers are internally created
 * triggers for constraints such as foreign key or unique constraints.
 * Currently we always execute an internal trigger on datanode, assuming that
 * the constraint trigger function is always shippable to datanodes. We can
 * safely assume so because we disallow constraint creation for scenarios where
 * the constraint needs access to records on other nodes.
 */
static bool pgxc_is_trigger_firable(Relation rel, Trigger* trigger, bool exec_all_triggers)
{
    if (trigger->tgisinternal)
        return pgxc_is_internal_trig_firable(rel, trigger);
    else
        return exec_all_triggers;
}

/* Is this internal trigger firable here on this node ? */
static bool pgxc_is_internal_trig_firable(Relation rel, const Trigger* trigger)
{
    Assert(trigger->tgisinternal);

    /*
     * View (INSTEAD) triggers are defined on coordinator. Currently there is no
     * internal trigger defined for views, but if it is ever defined, we have
     * no choice but to execute on coordinator because it will never get a
     * chance to execute on datanode since views are not present there.
     */
    if (TRIGGER_FOR_INSTEAD(trigger->tgtype))
        return true;

    /*
     * Otherwise, execute internal triggers only on datanode or local-only
     * tables
     */
    return !RelationGetLocInfo(rel);
}

/*
 * Convenience function to form a heaptuple out of a heaptuple header.
 * PGXCTO: Though this is a convenience function now, it would possibly serve the
 * purpose of GetTupleForTrigger() when we fix the GetTupleForTrigger() related
 * issue. If we don't end up in doing anything trigger specific, we will rename
 * and move this function to somewhere else.
 */
static HeapTuple pgxc_get_trigger_tuple(HeapTupleHeader tuphead)
{
    HeapTupleData tuple;

    if (!tuphead)
        return NULL;

    tuple.t_data = tuphead;
    tuple.t_len = (tuphead ? HeapTupleHeaderGetDatumLength(tuphead) : 0);
    ItemPointerSetInvalid(&tuple.t_self);
    tuple.t_tableOid = InvalidOid;
    tuple.t_bucketId = InvalidBktId;
    tuple.t_xc_node_id = 0;
    HeapTupleSetZeroBase(&tuple);

    return (HeapTuple)tableam_tops_copy_tuple(&tuple);
}

/*
 * @Description : check whether local node can execute trigger.
 * @in rel : the relation info of the table which the trigger belong to.
 * @in estate : the estate of the IUD query.
 * @return : true for we should execute trigger here.
 */
static bool pgxc_should_exec_trigship(Relation rel, const EState* estate)
{
    if (IS_PGXC_DATANODE) {
        if (u_sess->tri_cxt.exec_row_trigger_on_datanode)
            return true;
        else
            return false;
    } else {
        if (RelationGetLocInfo(rel)) {
            if (!estate->isRowTriggerShippable)
                return true;
            else
                return false;
        } else {
            return true;
        }
    }
}

#endif

void InvalidRelcacheForTriggerFunction(Oid funcoid, Oid returnType)
{
    /* Only handle function that returns trigger */
    if (returnType != TRIGGEROID) {
        return;
    }

    SysScanDesc sscan = NULL;
    HeapTuple tuple;
    Relation trigRel = heap_open(TriggerRelationId, AccessShareLock);

    /*
     * Find associated trigger with given function, and send invalid message to
     * the relation holding these triggers.
     */
    sscan = systable_beginscan(trigRel, InvalidOid, false, NULL, 0, NULL);
    while (HeapTupleIsValid(tuple = systable_getnext(sscan))) {
        Form_pg_trigger trigger = (Form_pg_trigger)GETSTRUCT(tuple);
        if (trigger->tgfoid == funcoid) {
            Relation rel = heap_open(trigger->tgrelid, AccessShareLock);
            CacheInvalidateRelcache(rel);
            heap_close(rel, AccessShareLock);
        }
    }
    systable_endscan(sscan);
    heap_close(trigRel, AccessShareLock);
}

/* reset row trigger shipping flag before exiting */
void ResetTrigShipFlag()
{
    if (u_sess->tri_cxt.afterTriggers->query_depth == 0) {
        u_sess->tri_cxt.exec_row_trigger_on_datanode = false;
    }

}

ObjectAddress AlterTrigger(AlterTriggerStmt* stmt)
{
    ObjectAddress address;
    Oid trigoid;
    Oid reloid;
    trigoid = get_trigger_oid_b(stmt->trigname, &reloid, false);
    if (OidIsValid(trigoid) && OidIsValid(reloid)) {
        Relation rel;
        rel = relation_open(reloid, AccessExclusiveLock);
        EnableDisableTrigger(rel, stmt->trigname, stmt->tgenabled, false);
        relation_close(rel, NoLock);
        ObjectAddressSet(address, TriggerRelationId, trigoid);
    }
    return address;
}

/* build a function name for b format trigger */
static char* rebuild_funcname_for_b_trigger(char* trigname, char* relname)
{
    uint16 uid = 0;
    char* funcname = NULL;
    int ret = 0;
    size_t funcNameLen = strlen(trigname) + strlen(relname) + B_TRIGGER_INLINE_LENGTH + B_TRIGGER_ID_LENGTH + 1;
    size_t halfPos = (NAMEDATALEN - B_TRIGGER_INLINE_LENGTH - B_TRIGGER_ID_LENGTH) / 2 - 1;
    /* reset overflow length */
    if (funcNameLen >= NAMEDATALEN) {
        /* we have to reduce name string for oversize */
        if (strlen(trigname) > halfPos) {
            trigname[halfPos - 1] = '\0';
        }

        if (strlen(relname) > halfPos) {
            relname[halfPos - 1] = '\0';
        }
        funcNameLen = strlen(trigname) + strlen(relname) + B_TRIGGER_INLINE_LENGTH + B_TRIGGER_ID_LENGTH + 1;
    }
    funcname = (char*)palloc0(sizeof(char) * funcNameLen);
    CatCList* catlist = NULL;
    do {
        if (uid++ > B_TRIGGER_ID_MAX) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), 
                    errmsg("You cannot create the trigger function because there are too many name conflicts."),
                        errdetail("We need to build a function internal, please rename the trigger.")));
        }
        ret = snprintf_s(funcname, funcNameLen, funcNameLen - 1, "%s_%s_%s_%hu", trigname, relname, B_TRIGGER_INLINE_STR, uid);
        securec_check_ss(ret, "\0", "\0");
        if (t_thrd.proc->workingVersionNum < 92470) {
            catlist = SearchSysCacheList1(PROCNAMEARGSNSP, CStringGetDatum(funcname));
        } else {
            catlist = SearchSysCacheList1(PROCALLARGS, CStringGetDatum(funcname));
        }
        if (catlist != NULL) {
            if (catlist->n_members > 0) 
                ReleaseSysCacheList(catlist);
            else {
                ReleaseSysCacheList(catlist);
                break;
            }
        } else {
            break;
        }

    } while (true);

    return funcname;
}
