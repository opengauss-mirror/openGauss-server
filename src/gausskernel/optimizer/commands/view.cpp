/* -------------------------------------------------------------------------
 *
 * view.cpp
 *	  use rewrite rules to construct views
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/commands/view.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/heapam.h"
#include "access/xact.h"
#include "catalog/gs_matview.h"
#include "catalog/gs_matview_dependency.h"
#include "catalog/namespace.h"
#include "catalog/gs_column_keys.h"
#include "catalog/gs_encrypted_columns.h"
#include "catalog/pg_rewrite.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_collation.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/catalog.h"
#include "client_logic/cache.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "commands/view.h"
#include "miscadmin.h"
#include "gs_policy/policy_common.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/analyze.h"
#include "parser/parser.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteDefine.h"
#include "rewrite/rewriteManip.h"
#include "rewrite/rewriteHandler.h"
#include "rewrite/rewriteSupport.h"
#include "optimizer/nodegroups.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "foreign/foreign.h"
#include "parser/parse_type.h"
#ifdef PGXC
#include "pgxc/execRemote.h"
#include "tcop/utility.h"
#endif
static void checkViewTupleDesc(TupleDesc newdesc, TupleDesc olddesc);

InSideView query_from_view_hook = NULL;

#define INITIAL_USER_ID 10

/*---------------------------------------------------------------------
 * Validator for "check_option" reloption on views. The allowed values
 * are "local" and "cascaded".
 */
void validateWithCheckOption(const char *value)
{
    if (value == NULL || (pg_strcasecmp(value, "local") != 0 && pg_strcasecmp(value, "cascaded") != 0)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("invalid value for \"check_option\" option"),
                errdetail("Valid values are \"local\", and \"cascaded\".")));
    }
}

/*---------------------------------------------------------------------
 * Validator for "view_sql_security" reloption on views. The allowed values
 * are "local" and "cascaded".
 */
void validateViewSecurityOption(const char *value)
{
    if (value == NULL || (pg_strcasecmp(value, "definer") != 0 && pg_strcasecmp(value, "invoker") != 0)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("invalid value for \"view_sql_security\" option"),
                errdetail("Valid values are \"INVOKER\", and \"DEFINER\".")));
    }
}
static void setEncryptedColumnRef(ColumnDef *def, TargetEntry *tle)
{
    def->clientLogicColumnRef = (ClientLogicColumnRef*)palloc(sizeof(ClientLogicColumnRef));
    HeapTuple tup = SearchSysCache2(CERELIDCOUMNNAME, ObjectIdGetDatum(tle->resorigtbl), CStringGetDatum(def->colname));
    if (!HeapTupleIsValid(tup)) {
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_COLUMN), errmsg("client encrypted column \"%s\" does not exist", def->colname)));
    }
    Form_gs_encrypted_columns ce_form = (Form_gs_encrypted_columns) GETSTRUCT(tup);
    HeapTuple setting_tup  =  SearchSysCache1(COLUMNSETTINGOID, ObjectIdGetDatum(ce_form->column_key_id));
    if (!HeapTupleIsValid(setting_tup)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
                errmsg("client encrypted column key %u does not exist", ce_form->column_key_id)));
    }
    Form_gs_column_keys setting_form = (Form_gs_column_keys) GETSTRUCT(setting_tup);
    def->clientLogicColumnRef->column_key_name =  list_make1(makeString(NameStr(setting_form->column_key_name)));
    ReleaseSysCache(setting_tup);
    ReleaseSysCache(tup);
    def->clientLogicColumnRef->orig_typname =
            makeTypeNameFromOid(exprTypmod((Node*)tle->expr), -1);
    def->clientLogicColumnRef->dest_typname =
            makeTypeNameFromOid(exprType((Node*)tle->expr), exprTypmod((Node*)tle->expr));
}

/* ---------------------------------------------------------------------
 * DefineVirtualRelation
 *
 * Create a view relation and use the rules system to store the query
 * for the view.
 * ---------------------------------------------------------------------
 */
/*
 * UpdateForceViewColumnDependency
 *
 * When an invalid force view becomes valid, its placeholder first column
 * (dummy_force_col) is rewritten in pg_attribute to the real first column.
 * We must keep pg_depend consistent: drop the old column's datatype/collation
 * dependency records and record the dependency on the new datatype/collation.
 * Otherwise dropping a user-defined type used by the new column would not be
 * blocked, leaving a dangling reference.
 */
static void UpdateForceViewColumnDependency(Oid viewOid, AttrNumber attnum, Oid oldTypid,
                                            Oid oldCollid, Form_pg_attribute newAttr)
{
    Oid newTypid = newAttr->atttypid;
    Oid newCollid = newAttr->attcollation;
    Relation depRel = heap_open(DependRelationId, RowExclusiveLock);
    ScanKeyData key[3];
    SysScanDesc scan;
    HeapTuple depTup;

    ScanKeyInit(&key[0], Anum_pg_depend_classid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(RelationRelationId));
    ScanKeyInit(&key[1], Anum_pg_depend_objid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(viewOid));
    ScanKeyInit(&key[2], Anum_pg_depend_objsubid, BTEqualStrategyNumber, F_INT4EQ,
                Int32GetDatum((int32)attnum));

    scan = systable_beginscan(depRel, DependDependerIndexId, true, NULL, 3, key);
    while (HeapTupleIsValid(depTup = systable_getnext(scan))) {
        Form_pg_depend foundDep = (Form_pg_depend)GETSTRUCT(depTup);
        /* Only remove the old datatype/collation dependency of this column. */
        if ((foundDep->refclassid == TypeRelationId && foundDep->refobjid == oldTypid) ||
            (foundDep->refclassid == CollationRelationId && foundDep->refobjid == oldCollid)) {
            simple_heap_delete(depRel, &depTup->t_self);
        }
    }
    systable_endscan(scan);
    heap_close(depRel, RowExclusiveLock);

    /* Record dependency on the new datatype. */
    ObjectAddress myself;
    ObjectAddress referenced;
    myself.classId = RelationRelationId;
    myself.objectId = viewOid;
    myself.objectSubId = attnum;
    referenced.classId = TypeRelationId;
    referenced.objectId = newTypid;
    referenced.objectSubId = 0;
    recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

    /* Record dependency on the new collation (default collation is pinned). */
    if (OidIsValid(newCollid) && newCollid != DEFAULT_COLLATION_OID) {
        referenced.classId = CollationRelationId;
        referenced.objectId = newCollid;
        referenced.objectSubId = 0;
        recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
    }
}

/*
 * IsForceViewInvalid
 *      Check if the given view relation is currently an invalid force view.
 */
static inline bool IsForceViewInvalid(Relation rel)
{
    if (rel == NULL || rel->rd_rules == NULL) {
        return false;
    }

    for (int i = 0; i < rel->rd_rules->numLocks; i++) {
        if (rel->rd_rules->rules[i]->enabled == RULE_INVALID_FORCE_VIEW) {
            return true;
        }
    }

    return false;
}

/*
 * CheckViewDefinerPermissions
 *      Verify that the definer role has CREATE permissions on the namespace.
 */
static void CheckViewDefinerPermissions(RangeVar* relation, ObjectType relkind, const char* definer, bool is_alter)
{
    if (definer == NULL) {
        return;
    }

    /* Get owner to check the permissions. */
    Oid ownerOid = get_role_oid(definer, false);
    bool isOwnerChange = false;
    if (!OidIsValid(ownerOid)) {
        ownerOid = GetUserId();
    } else if (ownerOid != GetUserId()) {
        isOwnerChange = true;
    }
    
    if (isOwnerChange && !is_alter) {
        /* Check namespace permissions. */
        AclResult aclresult;
        Oid namespaceId = RangeVarGetAndCheckCreationNamespace(relation, NoLock, NULL, relkind);
        aclresult = pg_namespace_aclcheck(namespaceId, ownerOid, ACL_CREATE);
        bool anyResult = false;
        if (aclresult != ACLCHECK_OK && !IsSysSchema(namespaceId)) {
            anyResult = CheckRelationCreateAnyPrivilege(ownerOid, relkind);
        }
        if (aclresult != ACLCHECK_OK && !anyResult) {
            aclcheck_error(aclresult, ACL_KIND_NAMESPACE, get_namespace_name(namespaceId));
        }
    }
}

/*
 * BuildViewColumnAlterCmds
 *      Build AlterTableCmd list to add or drop columns for the view during replacement.
 */
static List* BuildViewColumnAlterCmds(Relation rel, List* attrList, bool oldViewForceInvalid, bool flag)
{
    List* atcmds = NIL;
    AlterTableCmd* atcmd = NULL;

    if (oldViewForceInvalid) {
        ListCell* lc = NULL;
        int skip = 1;

        foreach (lc, attrList) {
            if (skip > 0) {
                skip--;
                continue;
            }
            atcmd = makeNode(AlterTableCmd);
            atcmd->subtype = AT_AddColumnToView;
            atcmd->def = (Node*)lfirst(lc);
            atcmds = lappend(atcmds, atcmd);
        }
    } else {
        if (list_length(attrList) > rel->rd_att->natts) {
            ListCell* c = NULL;
            int skip = rel->rd_att->natts;

            foreach (c, attrList) {
                if (skip > 0) {
                    skip--;
                    continue;
                }
                atcmd = makeNode(AlterTableCmd);
                atcmd->subtype = AT_AddColumnToView;
                atcmd->def = (Node*)lfirst(c);
                atcmds = lappend(atcmds, atcmd);
            }
        } else if (flag) {
            for (int dropcolno = rel->rd_att->natts - 1; dropcolno >= list_length(attrList); dropcolno--) {
                atcmd = makeNode(AlterTableCmd);
                atcmd->subtype = AT_DropColumn;
                atcmd->name = rel->rd_att->attrs[dropcolno].attname.data;
                atcmd->behavior = DROP_RESTRICT;
                atcmd->missing_ok = true;
                atcmds = lappend(atcmds, atcmd);
            }
        }
    }

    return atcmds;
}

static ObjectAddress DefineVirtualRelation(RangeVar* relation, List* tlist, bool replace, List* options, ObjectType relkind,
                                    ViewStmt* stmt, Query* viewParse)
{
    Oid viewOid;
    LOCKMODE lockmode;
    CreateStmt* createStmt = makeNode(CreateStmt);
    List* attrList = NIL;
    ListCell* t = NULL;
    char* definer = stmt->definer;
    bool is_alter = stmt->is_alter;

    /*
     * create a list of ColumnDef nodes based on the names and types of the
     * (non-junk) targetlist items from the view's SELECT list.
     */
    foreach (t, tlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(t);

        if (!tle->resjunk) {
            ColumnDef* def = makeNode(ColumnDef);

            def->colname = pstrdup(tle->resname);
            def->typname = makeTypeNameFromOid(exprType((Node*)tle->expr), exprTypmod((Node*)tle->expr));
            def->inhcount = 0;
            def->is_local = true;
            def->is_not_null = false;
            def->is_from_type = false;
            def->storage = 0;
            def->cmprs_mode = ATT_CMPR_NOCOMPRESS; /* dont compress */
            def->raw_default = NULL;
            def->update_default = NULL;
            def->cooked_default = NULL;
            def->collClause = NULL;
            def->collOid = exprCollation((Node*)tle->expr);
            
            if IsClientLogicType(exprType((Node*)tle->expr)) {
                setEncryptedColumnRef(def, tle);
            }
            /*
             * It's possible that the column is of a collatable type but the
             * collation could not be resolved, so double-check.
             */
            if (type_is_collatable(exprType((Node*)tle->expr))) {
                if (!OidIsValid(def->collOid))
                    ereport(ERROR,
                        (errcode(ERRCODE_INDETERMINATE_COLLATION),
                            errmsg("could not determine which collation to use for view column \"%s\"", def->colname),
                            errhint("Use the COLLATE clause to set the collation explicitly.")));
            } else if (!(DB_IS_CMPT(B_FORMAT) && ENABLE_MULTI_CHARSET && IsBinaryType(exprType((Node*)tle->expr)))) {
                Assert(!OidIsValid(def->collOid));
            }
            def->constraints = NIL;

            attrList = lappend(attrList, def);
        }
    }

    if (attrList == NIL)
        ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION), errmsg("view must have at least one column")));

    /*
     * Look up, check permissions on, and lock the creation namespace; also
     * check for a preexisting view with the same name.  This will also set
     * relation->relpersistence to RELPERSISTENCE_TEMP if the selected
     * namespace is temporary.
     */
    lockmode = replace ? AccessExclusiveLock : NoLock;
    (void)RangeVarGetAndCheckCreationNamespace(relation, lockmode, &viewOid, RELKIND_VIEW);
    
    if (!OidIsValid(viewOid) && is_alter) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("view \"%s\" does not exist", relation->relname)));
    }

    bool flag = OidIsValid(viewOid) && replace;
    if (flag) {
        Relation rel;
        TupleDesc descriptor;
        List* atcmds = NIL;
        AlterTableCmd* atcmd = NULL;
        ObjectAddress address;

        /*
         * During inplace upgrade, if we are doing rolling back, the old-versioned
         * relcache would cause problems. So update them anyway.
         */
        if (u_sess->attr.attr_common.IsInplaceUpgrade)
            RelationCacheInvalidateEntry(viewOid);

        /* Relation is already locked, but we must build a relcache entry. */
        rel = relation_open(viewOid, NoLock);
        /* Make sure it *is* a view or contquery. */
        flag = rel->rd_rel->relkind != RELKIND_VIEW && rel->rd_rel->relkind != RELKIND_CONTQUERY;
        if (flag)
            ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("\"%s\" is not a view", RelationGetRelationName(rel))));

        /* Also check it's not in use already */
        CheckTableNotInUse(rel, "CREATE OR REPLACE VIEW");

        /*
         * Due to the namespace visibility rules for temporary objects, we
         * should only end up replacing a temporary view with another
         * temporary view, and similarly for permanent views.
         */
        Assert(relation->relpersistence == rel->rd_rel->relpersistence);

        /*
         * Create a tuple descriptor to compare against the existing view, and
         * verify that the old column list is an initial prefix of the new
         * column list.
         */
        descriptor = BuildDescForRelation(attrList, (Node*)makeString(ORIENTATION_ROW));
        bool oldViewForceInvalid = IsForceViewInvalid(rel);
        if (oldViewForceInvalid) {
            Relation attrRel = heap_open(AttributeRelationId, RowExclusiveLock);
            HeapTuple oldTup = SearchSysCache2(ATTNUM, ObjectIdGetDatum(viewOid), Int16GetDatum(1));
            if (HeapTupleIsValid(oldTup)) {
                HeapTuple newTup = heap_copytuple(oldTup);
                Form_pg_attribute attStruct = (Form_pg_attribute)GETSTRUCT(newTup);
                /* Get first attr for new view. */
                Form_pg_attribute newStruct = &descriptor->attrs[0];

                /* Remember the placeholder column's old type/collation for pg_depend cleanup. */
                Oid oldTypid = attStruct->atttypid;
                Oid oldCollid = attStruct->attcollation;
                namestrcpy(&(attStruct->attname), NameStr(newStruct->attname));
                attStruct->atttypid = newStruct->atttypid;
                attStruct->atttypmod = newStruct->atttypmod;
                attStruct->attcollation = newStruct->attcollation;
                attStruct->attlen = newStruct->attlen;
                attStruct->attbyval = newStruct->attbyval;
                attStruct->attalign = newStruct->attalign;
                attStruct->attstorage = newStruct->attstorage;
                attStruct->attndims = newStruct->attndims;
                simple_heap_update(attrRel, &newTup->t_self, newTup);
                CatalogUpdateIndexes(attrRel, newTup);

                /* Keep pg_depend consistent for the rewritten first column. */
                UpdateForceViewColumnDependency(viewOid, 1, oldTypid, oldCollid, newStruct);
                heap_freetuple(newTup);
                ReleaseSysCache(oldTup);
            }
            heap_close(attrRel, RowExclusiveLock);
            CommandCounterIncrement();
            RelationCacheInvalidateEntry(viewOid);
        } else {
            /*
             * During inplace or online upgrade, we may need to drop newly-added
             * view columns to perform rollback. Since these columns should have
             * not been visible to users, we just skip the safety check.
             */
            if (!u_sess->attr.attr_common.IsInplaceUpgrade) {
                PG_TRY();
                {
                    checkViewTupleDesc(descriptor, rel->rd_att);
                }
                PG_CATCH();
                {
                    relation_close(rel, NoLock);
                    PG_RE_THROW();
                }
                PG_END_TRY();
            }

            /*
             * set definer by AlterTameCmd
             */
            if (definer != NULL) {
                CheckViewDefinerPermissions(relation, relkind, definer, is_alter);
                atcmd = makeNode(AlterTableCmd);
                atcmd->subtype = AT_ChangeOwner;
                atcmd->name = definer;
                atcmds = lappend(atcmds, atcmd);
            }

            /*
            * If new attributes have been added, we must add pg_attribute entries
            * for them.  It is convenient (although overkill) to use the ALTER
            * TABLE ADD COLUMN infrastructure for this.
            *
            * When we roll back upgrade by dropping view columns, we use the
            * ALTER TABLE DROP COLUMN infrastructure.
            */
            flag = (list_length(attrList) < rel->rd_att->natts) && u_sess->attr.attr_common.IsInplaceUpgrade;
        }
        List* colCmds = BuildViewColumnAlterCmds(rel, attrList, oldViewForceInvalid, flag);
        atcmds = list_concat(atcmds, colCmds);
        if (atcmds) {
            AlterTableInternal(viewOid, atcmds, true);

            /* Make the new view columns visible */
            CommandCounterIncrement();
        }

        /*
         * Update the query for the view.
         *
         * Note that we must do this before updating the view options, because
         * the new options may not be compatible with the old view query (for
         * example if we attempt to add the WITH CHECK OPTION, we require that
         * the new view be automatically updatable, but the old view may not
         * have been).
         */
        StoreViewQuery(viewOid, viewParse, replace);

        /* Make the new view query visible */
        CommandCounterIncrement();

         /*
          * Finally update the view options.
          *
          * The new options list replaces the existing options list, even if
          * it's empty.
          */
        atcmd = makeNode(AlterTableCmd);
        atcmd->subtype = AT_ReplaceRelOptions;
        atcmd->def = (Node*)options;
        atcmds = list_make1(atcmd);

        /* OK, let's do it. */
        AlterTableInternal(viewOid, atcmds, true);
        /*
         * There is very little to do here to update the view's dependencies.
         * Most view-level dependency relationships, such as those on the
         * owner, schema, and associated composite type, aren't changing.
         * Because we don't allow changing type or collation of an existing
         * view column, those dependencies of the existing columns don't
         * change either, while the AT_AddColumnToView machinery took care of
         * adding such dependencies for new view columns.  The dependencies of
         * the view's query could have changed arbitrarily, but that was dealt
         * with inside StoreViewQuery.  What remains is only to check that
         * view replacement is allowed when we're creating an extension.
         */
        ObjectAddressSet(address, RelationRelationId, viewOid);

        recordDependencyOnCurrentExtension(&address, true);

        /*
         * Seems okay, so return the OID of the pre-existing view.
         */
        relation_close(rel, NoLock); /* keep the lock! */

        return address;
    } else {
        ObjectAddress address;   

        /*
         * now set the parameters for keys/inheritance etc. All of these are
         * uninteresting for views...
         */
        createStmt->relation = relation;
        createStmt->tableElts = attrList;
        createStmt->inhRelations = NIL;
        createStmt->constraints = NIL;
        createStmt->options = options;
        createStmt->options = lappend(options, defWithOids(false));
        createStmt->oncommit = ONCOMMIT_NOOP;
        createStmt->tablespacename = NULL;
        createStmt->if_not_exists = false;
        createStmt->charset = PG_INVALID_ENCODING;
        Oid ownerOid = InvalidOid;

        /* 
         * get oid by user name
         */
        if (definer != NULL) {
            ownerOid = get_role_oid(definer, false);
        }

        /*
         * finally create the relation (this will error out if there's an
         * existing view, so we don't need more code to complain if "replace"
         * is false).
         */
        if (relkind == OBJECT_CONTQUERY) {
            address = DefineRelation(createStmt, RELKIND_CONTQUERY, ownerOid, NULL);
        } else {
            address = DefineRelation(createStmt, RELKIND_VIEW, ownerOid, NULL);
        }
        Assert(address.objectId != InvalidOid);

        /* Make the new view relation visible */
        CommandCounterIncrement();

        /* Store the query for the view */
        StoreViewQuery(address.objectId, viewParse, replace);

        return address;
    }
}

static void StoreInvalidForceViewQuery(Oid viewOid, const char* queryString)
{
    Relation rewriteDesc;
    HeapTuple tup;
    HeapTuple oldtup;
    Datum values[Natts_pg_rewrite];
    bool nulls[Natts_pg_rewrite];
    bool replaces[Natts_pg_rewrite];
    NameData ruleName;

    errno_t rc = memset_s(values, sizeof(values), 0, sizeof (values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof (nulls));
    securec_check(rc, "\0", "\0");
    rc = memset_s(replaces, sizeof(replaces), false, sizeof (replaces));
    securec_check(rc, "\0", "\0");
    namestrcpy(&ruleName, ViewSelectRuleName);

    Node* sqlNode = (Node*)makeString(pstrdup(queryString));
    char* qualStr = nodeToString(sqlNode);
    char* actionStr = nodeToString(NIL);
    
    values[Anum_pg_rewrite_rulename - 1] = NameGetDatum(&ruleName);
    values[Anum_pg_rewrite_ev_class - 1] = ObjectIdGetDatum(viewOid);
    values[Anum_pg_rewrite_ev_attr - 1] = Int16GetDatum(-1);
    values[Anum_pg_rewrite_ev_type - 1] = CharGetDatum(CMD_SELECT + '0');
    values[Anum_pg_rewrite_ev_enabled - 1] = CharGetDatum(RULE_INVALID_FORCE_VIEW);
    values[Anum_pg_rewrite_is_instead - 1] = BoolGetDatum(true);
    values[Anum_pg_rewrite_ev_qual - 1] = CStringGetTextDatum(qualStr);
    values[Anum_pg_rewrite_ev_action - 1] = CStringGetTextDatum(actionStr);

    rewriteDesc = heap_open(RewriteRelationId, RowExclusiveLock);
    oldtup = SearchSysCache2(RULERELNAME,
                             ObjectIdGetDatum(viewOid),
                             PointerGetDatum(ViewSelectRuleName));
    if (HeapTupleIsValid(oldtup)) {
        for (int i = 0; i < Natts_pg_rewrite; i++) {
            replaces[i] = true;
        }
        tup = heap_modify_tuple(oldtup, RelationGetDescr(rewriteDesc), values, nulls, replaces);
        simple_heap_update(rewriteDesc, &tup->t_self, tup);
        CatalogUpdateIndexes(rewriteDesc, tup);
        ReleaseSysCache(oldtup);
    } else {
        tup = heap_form_tuple(RelationGetDescr(rewriteDesc), values, nulls);
        Oid ruleOid = GetNewOid(rewriteDesc);
        HeapTupleSetOid(tup, ruleOid);
        (void)simple_heap_insert(rewriteDesc, tup);
        CatalogUpdateIndexes(rewriteDesc, tup);

        ObjectAddress myself;
        ObjectAddress referenced;
        myself.classId = RewriteRelationId;
        myself.objectId = ruleOid;
        myself.objectSubId = 0;

        referenced.classId = RelationRelationId;
        referenced.objectId = viewOid;
        referenced.objectSubId = 0;
        recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);
    }

    if (tup != NULL) {
        heap_freetuple(tup);
    }
    heap_close(rewriteDesc, RowExclusiveLock);
    pfree_ext(qualStr);
    pfree_ext(actionStr);
    if (sqlNode) {
        pfree_ext(((Value*)sqlNode)->val.str);
        pfree_ext(sqlNode);
    }
    SetRelationRuleStatus(viewOid, true, false);
}

/* ---------------------------------------------------------------------
 * DefineVirtualForceRelation
 *
 * Create a view relation and set the sql text
 * for the view.
 * ---------------------------------------------------------------------
 */
static ObjectAddress DefineVirtualForceRelation(RangeVar* relation, bool replace, List* options,
                                    ObjectType relkind, ViewStmt* stmt, const char* queryString)
{
    Oid viewOid;
    LOCKMODE lockmode;
    ObjectAddress address;
    char* definer = stmt->definer;
    bool is_alter = stmt->is_alter;

    lockmode = replace ? AccessExclusiveLock : NoLock;
    (void)RangeVarGetAndCheckCreationNamespace(relation, lockmode, &viewOid, RELKIND_VIEW);

    if (OidIsValid(viewOid) && replace) {
        Relation rel;
        bool isExistInvalid = false;
        if (u_sess->attr.attr_common.IsInplaceUpgrade)
            RelationCacheInvalidateEntry(viewOid);
        rel = relation_open(viewOid, NoLock);
        if (rel->rd_rel->relkind != RELKIND_VIEW && rel->rd_rel->relkind != RELKIND_CONTQUERY) {
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("\"%s\" is not a view.", RelationGetRelationName(rel))));
        }

        CheckTableNotInUse(rel, "CREATE OR REPLACE VIEW");
        isExistInvalid = IsForceViewInvalid(rel);
        if (!isExistInvalid) {
            relation_close(rel, NoLock);
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Force replace a valid view is not supported.")));
        }

        StoreInvalidForceViewQuery(viewOid, queryString);
        CommandCounterIncrement();
        relation_close(rel, NoLock);
        ObjectAddressSet(address, RelationRelationId, viewOid);
        return address;
    } else {
        CreateStmt* createStmt = makeNode(CreateStmt);
        List* attrList = NIL;
        ColumnDef* def = makeNode(ColumnDef);

        def->colname = pstrdup("dummy_force_col");
        def->typname = makeTypeName("int4");
        def->inhcount = 0;
        def->is_local = true;
        def->is_not_null = false;
        def->storage = 0;
        def->cmprs_mode = ATT_CMPR_NOCOMPRESS;
        def->raw_default = NULL;
        def->update_default = NULL;
        def->cooked_default = NULL;
        def->collClause = NULL;
        def->collOid = InvalidOid;
        def->constraints = NIL;

        attrList = list_make1(def);

        createStmt->relation = relation;
        createStmt->tableElts = attrList;
        createStmt->inhRelations = NIL;
        createStmt->constraints = NIL;
        createStmt->options = lappend(options, defWithOids(false));
        createStmt->oncommit = ONCOMMIT_NOOP;
        createStmt->tablespacename = NULL;
        createStmt->if_not_exists = false;
        createStmt->charset = PG_INVALID_ENCODING;

        Oid ownerOid = InvalidOid;

        if (relkind == OBJECT_CONTQUERY) {
            address = DefineRelation(createStmt, RELKIND_CONTQUERY, ownerOid, NULL);
        } else {
            address = DefineRelation(createStmt, RELKIND_VIEW, ownerOid, NULL);
        }

        CommandCounterIncrement();

        StoreInvalidForceViewQuery(address.objectId, queryString);

        return address;
    }
}

/*
 * ResetForceViewRuleStatus
 *      Change the view's select rule status from RULE_INVALID_FORCE_VIEW to RULE_FIRES_ON_ORIGIN.
 */
static void ResetForceViewRuleStatus(Oid viewOid)
{
    Relation rewriteDesc = heap_open(RewriteRelationId, RowExclusiveLock);
    HeapTuple oldTup = SearchSysCache2(RULERELNAME,
                                       ObjectIdGetDatum(viewOid),
                                       PointerGetDatum(ViewSelectRuleName));
    if (HeapTupleIsValid(oldTup)) {
        Form_pg_rewrite rewriteForm = (Form_pg_rewrite)GETSTRUCT(oldTup);
        if (rewriteForm->ev_enabled == RULE_INVALID_FORCE_VIEW) {
            HeapTuple newTup = heap_copytuple(oldTup);
            ((Form_pg_rewrite)GETSTRUCT(newTup))->ev_enabled = RULE_FIRES_ON_ORIGIN;

            simple_heap_update(rewriteDesc, &newTup->t_self, newTup);
            CatalogUpdateIndexes(rewriteDesc, newTup);
            heap_freetuple(newTup);
        }
        ReleaseSysCache(oldTup);
    }
    heap_close(rewriteDesc, RowExclusiveLock);
}

/*
 * AutoRecompileForceView
 *
 * reparse the sql and add replace delete force
 * retry the create view.
 */
bool AutoRecompileForceView(Oid viewOid, const char* queryString)
{
    bool success = false;
    int save_nestlevel = 0;
    bool guc_changed = false;

    if (list_member_oid(u_sess->utils_cxt.force_view_recompile_oids, viewOid)) {
        return false;
    }
    u_sess->utils_cxt.force_view_recompile_oids = lappend_oid(u_sess->utils_cxt.force_view_recompile_oids, viewOid);

    PG_TRY();
    {
        List* raw_parsetree_list = raw_parser(queryString, NULL);

        if (raw_parsetree_list != NIL) {
            Node* stmt = (Node*)linitial(raw_parsetree_list);
            if (IsA(stmt, ViewStmt)) {
                ViewStmt* vstmt = (ViewStmt*)stmt;
                vstmt->replace = true;
                vstmt->is_force = false;
                Oid nspOid = get_rel_namespace(viewOid);
                char* schemaName = get_namespace_name(nspOid);

                if (vstmt->view != NULL && vstmt->view->schemaname == NULL) {
                    vstmt->view->schemaname = pstrdup_ext(schemaName);
                }

                save_nestlevel = NewGUCNestLevel();
                (void)set_config_option("search_path", schemaName,
                    PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SAVE, true, 0, false);
                guc_changed = true;
                processutility_context proutility_cxt;
                proutility_cxt.parse_tree = stmt;
                proutility_cxt.query_string = queryString;
                proutility_cxt.readOnlyTree = false;
                proutility_cxt.params = NULL;
                proutility_cxt.is_top_level = false;

                ProcessUtility(&proutility_cxt,
                               None_Receiver,
                               true,
                               NULL,
                               PROCESS_UTILITY_SUBCOMMAND,
                               false);

                AtEOXact_GUC(false, save_nestlevel);
                guc_changed = false;
                pfree_ext(schemaName);
                ResetForceViewRuleStatus(viewOid);
                CommandCounterIncrement();
                success = true;
            }
        }
        u_sess->utils_cxt.force_view_recompile_oids =
            list_delete_oid(u_sess->utils_cxt.force_view_recompile_oids, viewOid);
    }
    PG_CATCH();
    {
        if (guc_changed) {
            AtEOXact_GUC(false, save_nestlevel);
        }
        u_sess->utils_cxt.force_view_recompile_oids =
            list_delete_oid(u_sess->utils_cxt.force_view_recompile_oids, viewOid);
        success = false;
        PG_RE_THROW();
    }
    PG_END_TRY();

    return success;
}

/*
 * Verify that tupledesc associated with proposed new view definition
 * matches tupledesc of old view.  This is basically a cut-down version
 * of equalTupleDescs(), with code added to generate specific complaints.
 * Also, we allow the new tupledesc to have more columns than the old.
 */
static void checkViewTupleDesc(TupleDesc newdesc, TupleDesc olddesc)
{
    int i;

    if (newdesc->natts < olddesc->natts)
        ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION), errmsg("cannot drop columns from view")));
    /* we can ignore tdhasoid */
    for (i = 0; i < olddesc->natts; i++) {
        Form_pg_attribute newattr = &newdesc->attrs[i];
        Form_pg_attribute oldattr = &olddesc->attrs[i];

        /* XXX msg not right, but we don't support DROP COL on view anyway */
        if (newattr->attisdropped != oldattr->attisdropped)
            ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION), errmsg("cannot drop columns from view")));

        if (strcmp(NameStr(newattr->attname), NameStr(oldattr->attname)) != 0)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                    errmsg("cannot change name of view column \"%s\" to \"%s\"",
                        NameStr(oldattr->attname),
                        NameStr(newattr->attname))));
        /* XXX would it be safe to allow atttypmod to change?  Not sure */
        if (newattr->atttypid != oldattr->atttypid || newattr->atttypmod != oldattr->atttypmod)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                    errmsg("cannot change data type of view column \"%s\" from %s to %s",
                        NameStr(oldattr->attname),
                        format_type_with_typemod(oldattr->atttypid, oldattr->atttypmod),
                        format_type_with_typemod(newattr->atttypid, newattr->atttypmod))));
        /* We can ignore the remaining attributes of an attribute... */
    }

    /*
     * We ignore the constraint fields.  The new view desc can't have any
     * constraints, and the only ones that could be on the old view are
     * defaults, which we are happy to leave in place.
     */
}

bool is_system_view_schema_oid(Oid relnamespace)
{
    char* schema_name = get_namespace_name(relnamespace);
    if (schema_name == NULL) {
        return false;
    }
    bool result = (strcmp(schema_name, "sys") == 0 || strcmp(schema_name, "dbe_perf") == 0 ||
        strcmp(schema_name, "information_schema") == 0);
    pfree_ext(schema_name);
    return result;
}

static void DefineViewRules(Oid viewOid, Query* viewParse, bool replace)
{
    Relation view_relation = heap_open(viewOid, AccessShareLock);
    if (!g_instance.attr.attr_common.allowSystemTableMods && !u_sess->attr.attr_common.IsInplaceUpgrade &&
        (GetUserId() != INITIAL_USER_ID && is_system_view_schema_oid(RelationGetNamespace(view_relation)))) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("Not support: \"%s\" is a system catalog.", RelationGetRelationName(view_relation)),
            errcause("Not support modify the system catalog."),
            erraction("Avoid performing operation on the system catalog \"%s\".",
                RelationGetRelationName(view_relation))));
    }
    heap_close(view_relation, NoLock);

    /*
     * Set up the ON SELECT rule.  Since the query has already been through
     * parse analysis, we use DefineQueryRewrite() directly.
     */
    DefineQueryRewrite(pstrdup(ViewSelectRuleName), viewOid, NULL, CMD_SELECT, true, replace, list_make1(viewParse));

    /*
     * Someday: automatic ON INSERT, etc
     */
}

/* ---------------------------------------------------------------
 * UpdateRangeTableOfViewParse
 *
 * Update the range table of the given parsetree.
 * This update consists of adding two new entries IN THE BEGINNING
 * of the range table (otherwise the rule system will die a slow,
 * horrible and painful death, and we do not want that now, do we?)
 * one for the OLD relation and one for the NEW one (both of
 * them refer in fact to the "view" relation).
 *
 * Of course we must also increase the 'varnos' of all the Var nodes
 * by 2...
 *
 * These extra RT entries are not actually used in the query,
 * except for run-time permission checking.
 * ---------------------------------------------------------------
 */
Query* UpdateRangeTableOfViewParse(Oid viewOid, Query* viewParse)
{
    Relation viewRel;
    List* new_rt = NIL;
    RangeTblEntry *rt_entry1, *rt_entry2;

    /*
     * Make a copy of the given parsetree.	It's not so much that we don't
     * want to scribble on our input, it's that the parser has a bad habit of
     * outputting multiple links to the same subtree for constructs like
     * BETWEEN, and we mustn't have OffsetVarNodes increment the varno of a
     * Var node twice.	copyObject will expand any multiply-referenced subtree
     * into multiple copies.
     */
    viewParse = (Query*)copyObject(viewParse);

    /* need to open the rel for addRangeTableEntryForRelation */
    viewRel = relation_open(viewOid, AccessShareLock);

    /*
     * Create the 2 new range table entries and form the new range table...
     * OLD first, then NEW....
     */
    rt_entry1 = addRangeTableEntryForRelation(NULL, viewRel, makeAlias("old", NIL), false, false);
    rt_entry2 = addRangeTableEntryForRelation(NULL, viewRel, makeAlias("new", NIL), false, false);
    /* Must override addRangeTableEntry's default access-check flags */
    rt_entry1->requiredPerms = 0;
    rt_entry2->requiredPerms = 0;

    new_rt = lcons(rt_entry1, lcons(rt_entry2, viewParse->rtable));

    viewParse->rtable = new_rt;

    /*
     * Now offset all var nodes by 2, and jointree RT indexes too.
     */
    OffsetVarNodes((Node*)viewParse, 2, 0);

    relation_close(viewRel, AccessShareLock);

    return viewParse;
}

#ifdef ENABLE_MULTIPLE_NODES
static void CreateMvCommand(ViewStmt* stmt, const char* queryString)
{
    Oid groupid = 0;
    char* queryStringinfo = (char*)queryString;
    char* group_name = NULL;

    if (stmt->subcluster == NULL) {
        group_name = ng_get_installation_group_name();
    } else {
        group_name = strVal(linitial(stmt->subcluster->members));
    }

    groupid = get_pgxc_groupoid(group_name);
    if (!OidIsValid(groupid)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("Target node group \"%s\" doesn't exist", group_name)));
    }

    /* Force add Remotequery. */
    RemoteQuery* step = makeNode(RemoteQuery);
    step->combine_type = COMBINE_TYPE_NONE;
    step->sql_statement = (char*)queryString;
    step->exec_type = EXEC_ON_DATANODES;
    step->is_temp = false;

    step->exec_nodes = makeNode(ExecNodes);
    step->exec_nodes->distribution.group_oid = groupid;

    Oid* members = NULL;
    int nmembers = 0;
    nmembers = get_pgxc_groupmembers(groupid, &members);
    step->exec_nodes->nodeList = GetNodeGroupNodeList(members, nmembers);
    pfree_ext(members);

    /* Recurse for anything else */
    processutility_context proutility_cxt;
    proutility_cxt.parse_tree = (Node*)step;
    proutility_cxt.query_string = queryStringinfo;
    proutility_cxt.readOnlyTree = false;
    proutility_cxt.params = NULL;
    proutility_cxt.is_top_level = false;
    ProcessUtility(&proutility_cxt,
            None_Receiver,
            true,
            NULL);

    return;
}
#endif

/*
 * Check the base relation of view whether is a MySQL foreign table
 * for WITH CHECK OPTION.
 *
 * Return true if it is, false means that it isn't or the view could not
 * be auto-updatable.
 */
bool CheckMySQLFdwForWCO(Query* viewquery)
{
    RangeTblRef* rtr = NULL;

    if (list_length(viewquery->jointree->fromlist) != 1) {
        return false;
    }

    rtr = (RangeTblRef*)linitial(viewquery->jointree->fromlist);
    if (!IsA(rtr, RangeTblRef)) {
        return false;
    }

    RangeTblEntry* base_rte = rt_fetch(rtr->rtindex, viewquery->rtable);

    if (base_rte->relkind == RELKIND_FOREIGN_TABLE) {
        return isMysqlFDWFromTblOid(base_rte->relid);
    } else if (base_rte->relkind == RELKIND_VIEW) {
        /* recursive check for view */
        Relation base_rel = try_relation_open(base_rte->relid, AccessShareLock);
        bool res = CheckMySQLFdwForWCO(get_view_query(base_rel));
        relation_close(base_rel, AccessShareLock);
        return res;
    }

    return false;
}

/*
 * DefineView
 *		Execute a CREATE VIEW command.
 */
ObjectAddress DefineView(ViewStmt* stmt, const char* queryString, bool send_remote, bool isFirstNode)
{
    Query* viewParse = NULL;
    RangeVar* view = NULL;
    ListCell* cell = NULL;
    bool check_option;
    ObjectAddress address;
    bool isInvalidForce = false;
    MemoryContext oldcontext = CurrentMemoryContext;
    ResourceOwner oldowner = t_thrd.utils_cxt.CurrentResourceOwner;
    bool useSubxact = stmt->is_force && !u_sess->attr.attr_common.IsInplaceUpgrade;

    /*
     * Run parse analysis to convert the raw parse tree to a Query.  Note this
     * also acquires sufficient locks on the source table(s).
     *
     * For a FORCE view, parse analysis may fail (e.g. base table/column does
     * not exist).  We wrap it in an internal subtransaction so that any
     * relcache references and locks acquired before the failure are released
     * properly on rollback; otherwise FlushErrorState alone would leak them.
     */
    if (useSubxact) {
        BeginInternalSubTransaction(NULL);
        MemoryContextSwitchTo(oldcontext);
    }
    PG_TRY();
    {
        if (!IsA(stmt->query, Query)) {
            viewParse = parse_analyze(stmt->query, queryString, NULL, 0);
        } else {
            viewParse = (Query *)stmt->query;
        }
        if (useSubxact) {
            ReleaseCurrentSubTransaction();
            MemoryContextSwitchTo(oldcontext);
            t_thrd.utils_cxt.CurrentResourceOwner = oldowner;
        }
    }
    PG_CATCH();
    {
        if (stmt->is_force) {
            if (useSubxact) {
                /* Roll back the subtransaction to release relcache refs/locks. */
                RollbackAndReleaseCurrentSubTransaction();
                MemoryContextSwitchTo(oldcontext);
                t_thrd.utils_cxt.CurrentResourceOwner = oldowner;
            }
            FlushErrorState();
            isInvalidForce = true;
        } else {
            PG_RE_THROW();
        }
    }
    PG_END_TRY();

    if (isInvalidForce) {
        address = DefineVirtualForceRelation(stmt->view, stmt->replace, stmt->options,
                            stmt->relkind, stmt, queryString);
        return address;
    }
    if (viewParse->has_uservar && IsA(stmt->query, SelectStmt)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("View's SELECT contains a variable or parameter")));
    }

    /*
     * The grammar should ensure that the result is a single SELECT Query.
     * However, it doesn't forbid SELECT INTO, so we have to check for that.
     */
    if (!IsA(viewParse, Query)) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("unexpected parse analysis result")));
    }

    if (viewParse->utilityStmt != NULL && IsA(viewParse->utilityStmt, CreateTableAsStmt)) {
        ereport(
            ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("views must not contain SELECT INTO")));
    }

    if (viewParse->commandType != CMD_SELECT || viewParse->utilityStmt != NULL) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("unexpected parse analysis result")));
    }

    /*
     * Check for unsupported cases.  These tests are redundant with ones in
     * DefineQueryRewrite(), but that function will complain about a bogus ON
     * SELECT rule, and we'd rather the message complain about a view.
     */
    if (viewParse->hasModifyingCTE) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("views must not contain data-modifying statements in WITH")));
    }
#ifdef ENABLE_MULTIPLE_NODES
    validate_streaming_engine_status((Node*) stmt);
#endif

    /*
     * If the user specified the WITH CHECK OPTION, add it to the list of
     * reloptions.
     */
    if (stmt->withCheckOption == LOCAL_CHECK_OPTION)
        stmt->options = lappend(stmt->options, makeDefElem("check_option", (Node*)makeString("local")));
    else if (stmt->withCheckOption == CASCADED_CHECK_OPTION)
        stmt->options = lappend(stmt->options, makeDefElem("check_option", (Node*)makeString("cascaded")));

    /*
     * For B format sql security option ,add it to the list of reloptions.
     */
    if (stmt->viewSecurityOption == VIEW_SQL_SECURITY_DEFINER)
        stmt->options = lappend(stmt->options, makeDefElem("view_sql_security", (Node*)makeString("definer")));
    else if (stmt->viewSecurityOption == VIEW_SQL_SECURITY_INVOKER)
        stmt->options = lappend(stmt->options, makeDefElem("view_sql_security", (Node*)makeString("invoker")));

    /*
    * Check that the view is auto-updatable if WITH CHECK OPTION was
    * specified.
    */
    check_option = false;

    foreach(cell, stmt->options) {
        DefElem* defel = (DefElem*)lfirst(cell);

        if (pg_strcasecmp(defel->defname, "check_option") == 0)
            check_option = true;
    }

    /*
     * If the check option is specified, look to see if the view is
     * actually auto-updatable or not.
     */
    if (check_option) {
        const char *view_updatable_error = view_query_is_auto_updatable(viewParse, true);

        if (view_updatable_error)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("WITH CHECK OPTION is supported only on auto-updatable views"),
                    errhint("%s", view_updatable_error)));

        /* 
         * Views based on MySQL foreign table is not allowed to add check option,
         * because returning clause which check option dependend on is not supported
         * on MySQL.
         */
        if (CheckMySQLFdwForWCO(viewParse))
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("WITH CHECK OPTION is not supported on views that base on MySQL foreign table")));
    }

    /*
     * If a list of column names was given, run through and insert these into
     * the actual query tree. - thomas 2000-03-08
     */
    if (stmt->aliases != NIL) {
        ListCell* alist_item = list_head(stmt->aliases);
        ListCell* targetList = NULL;

        foreach (targetList, viewParse->targetList) {
            TargetEntry* te = (TargetEntry*)lfirst(targetList);

            Assert(IsA(te, TargetEntry));
            /* junk columns don't get aliases */
            if (te->resjunk)
                continue;
            te->resname = pstrdup(strVal(lfirst(alist_item)));
            alist_item = lnext(alist_item);
            if (alist_item == NULL)
                break; /* done assigning aliases */
        }

        if (alist_item != NULL)
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("CREATE VIEW specifies more column "
                           "names than columns")));
    }

    /* Unlogged views are not sensible. */
    if (stmt->view->relpersistence == RELPERSISTENCE_UNLOGGED)
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR), errmsg("views cannot be unlogged because they do not have storage")));

    if (stmt->view->relpersistence == RELPERSISTENCE_GLOBAL_TEMP) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR), errmsg("views cannot be global temp because they do not have storage")));
    }

    /*
     * If the user didn't explicitly ask for a temporary view, check whether
     * we need one implicitly.	We allow TEMP to be inserted automatically as
     * long as the CREATE command is consistent with that --- no explicit
     * schema name.
     */
    view = (RangeVar*)copyObject(stmt->view); /* don't corrupt original command */
    if (view->relpersistence == RELPERSISTENCE_PERMANENT && isQueryUsingTempRelation(viewParse)) {
        view->relpersistence = RELPERSISTENCE_TEMP;
        ereport(NOTICE, (errmsg("view \"%s\" will be a temporary view", view->relname)));
    }

#ifdef PGXC
    /* In case view is temporary, be sure not to use 2PC on such relations */
    if (view->relpersistence == RELPERSISTENCE_TEMP)
        ExecSetTempObjectIncluded();
#endif

     if (stmt->relkind == OBJECT_MATVIEW) {
        Oid viewOid = InvalidOid;
        /* Relation Already Created */
        (void)RangeVarGetAndCheckCreationNamespace(view, NoLock, &viewOid, RELKIND_MATVIEW);
        ObjectAddressSet(address, RelationRelationId, viewOid);
#ifdef ENABLE_MULTIPLE_NODES
        /* try to send CREATE MATERIALIZED VIEW to DNs, Only consider PGXC now. */
        if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && !send_remote) {
            CreateMvCommand(stmt, queryString);
        }
#endif
        StoreViewQuery(viewOid, viewParse, stmt->replace);
     } else {
        /*
         * Create the view relation
         *
         * NOTE: if it already exists and replace is false, the xact will be
         * aborted.
         */
        address = DefineVirtualRelation(view, viewParse->targetList, stmt->replace, stmt->options,
                                        stmt->relkind, stmt, viewParse);

        /*
         * The relation we have just created is not visible to any other commands
         * running with the same transaction & command id. So, increment the
         * command id counter (but do NOT pfree any memory!!!!)
         */
        CommandCounterIncrement();
    }

    return address;
}

bool IsViewTemp(ViewStmt* stmt, const char* queryString)
{
    Query* viewParse = NULL;
    RangeVar* view = NULL;
    view = (RangeVar*)copyObject(((ViewStmt*)stmt)->view); /* don't corrupt original command */
    viewParse = parse_analyze((Node*)copyObject(((ViewStmt*)stmt)->query), queryString, NULL, 0, false, true);
    /*
     * If the user didn't explicitly ask for a temporary view, check whether
     * we need one implicitly.	We allow TEMP to be inserted automatically as
     * long as the CREATE command is consistent with that --- no explicit
     * schema name.
     */
    if (view->relpersistence == RELPERSISTENCE_PERMANENT && isQueryUsingTempRelation(viewParse)) {
        view->relpersistence = RELPERSISTENCE_TEMP;
    }

    if (view->relpersistence == RELPERSISTENCE_TEMP)
        return true;
    return false;
}

/*
 * Use the rules system to store the query for the view.
 */
void
StoreViewQuery(Oid viewOid, Query *viewParse, bool replace)
{
    /*
     * The range table of 'viewParse' does not contain entries for the "OLD"
     * and "NEW" relations. So... add them!
     */
    viewParse = UpdateRangeTableOfViewParse(viewOid, viewParse);

    /*
     * Now create the rules associated with the view.
     */
    DefineViewRules(viewOid, viewParse, replace);
}
