/* -------------------------------------------------------------------------
 *
 * tablecmds.cpp
 *      Shark's Commands for creating and altering table structures and settings
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *      contrib/shark/src/tablecmds.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "knl/knl_session.h"
#include "access/genam.h"
#include "commands/tablecmds.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_expr.h"
#include "parser/parse_type.h"
#include "parser/parse_collate.h"
#include "catalog/heap.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_attrdef.h"

typedef struct ComputedColumnContextData
{
    Relation    rel;
    ParseState *pstate;
    List       *gen_column_list;
} ComputedColumnContextData;

typedef ComputedColumnContextData *ComputedColumnContext;

void assign_tablecmds_hook(void);
static void pltsql_PreDropColumnHook(Relation rel, AttrNumber attnum);
static void pltsql_PreAddConstraintsHook(Relation rel, ParseState *pstate, List *newColDefaults);
static void pltsql_TransformSelectForLimitHook(SelectStmt* stmt);
static void pltsql_RecomputeLimitsHook(float8 val);
static ObjectAddress GetAttrDefaultColumnAddress(Oid attrdefoid);
static bool check_nested_computed_column(Node *node, void *context);

/* Hook to tablecmds.cpp in the engine */
static void* prev_InvokePreDropColumnHook = NULL;
static void* prev_InvokePreAddConstraintsHook = NULL;
static void* prev_InvokeTransformSelectForLimitHook = NULL;
static void* prev_RecomputeLimitsHook = NULL;

void assign_tablecmds_hook(void)
{
    if (u_sess->hook_cxt.invokePreDropColumnHook) {
        prev_InvokePreDropColumnHook = u_sess->hook_cxt.invokePreDropColumnHook;
    }
    u_sess->hook_cxt.invokePreDropColumnHook = (void*)&pltsql_PreDropColumnHook;

    if (u_sess->hook_cxt.invokePreAddConstraintsHook) {
        prev_InvokePreAddConstraintsHook = u_sess->hook_cxt.invokePreAddConstraintsHook;
    }
    u_sess->hook_cxt.invokePreAddConstraintsHook = (void*)&pltsql_PreAddConstraintsHook;

    if (u_sess->hook_cxt.invokeTransformSelectForLimitHook) {
        prev_InvokeTransformSelectForLimitHook = u_sess->hook_cxt.invokeTransformSelectForLimitHook;
    }
    u_sess->hook_cxt.invokeTransformSelectForLimitHook = (void*)&pltsql_TransformSelectForLimitHook;

    if (u_sess->hook_cxt.recomputeLimitsHook) {
        prev_RecomputeLimitsHook = u_sess->hook_cxt.recomputeLimitsHook;
    }
    u_sess->hook_cxt.recomputeLimitsHook = (void*)&pltsql_RecomputeLimitsHook;
}

static void pltsql_PreDropColumnHook(Relation rel, AttrNumber attnum)
{
    if (!DB_IS_CMPT(D_FORMAT)) {
        return;
    }
    
    Relation    depRel;
    ScanKeyData key[3];
    SysScanDesc scan;
    HeapTuple    depTup, tuple;
    char        *schema_name = NULL,
                *minor_name = NULL;

    /* Call previous hook if exists */
    if (prev_InvokePreDropColumnHook) {
        ((InvokePreDropColumnHookType)(prev_InvokePreDropColumnHook))(rel, attnum);
    }

    /*
     * Find everything that depends on the column.  If we can find a
     * computed column dependent on this column, will throw an error.
     */
    depRel = relation_open(DependRelationId, AccessShareLock);

    ScanKeyInit(&key[0],
                Anum_pg_depend_refclassid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(RelationRelationId));
    ScanKeyInit(&key[1],
                Anum_pg_depend_refobjid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(RelationGetRelid(rel)));
    ScanKeyInit(&key[2],
                Anum_pg_depend_refobjsubid,
                BTEqualStrategyNumber, F_INT4EQ,
                Int32GetDatum((int32) attnum));

    scan = systable_beginscan(depRel, DependReferenceIndexId, true, NULL, 3, key);

    while (HeapTupleIsValid(depTup = systable_getnext(scan))) {
        Form_pg_depend foundDep = (Form_pg_depend) GETSTRUCT(depTup);
        ObjectAddress foundObject;

        foundObject.classId = foundDep->classid;
        foundObject.objectId = foundDep->objid;
        foundObject.objectSubId = foundDep->objsubid;

        /*
         * Below logic has been taken from backend's ATExecAlterColumnType
         * function
         */
        if (getObjectClass(&foundObject) == OCLASS_CLASS) {

            if (IsComputedColumn(foundObject.objectId, foundObject.objectSubId)  &&
                (foundObject.objectId != RelationGetRelid(rel) ||
                 foundObject.objectSubId  != attnum)) {
                Form_pg_attribute att = TupleDescAttr(rel->rd_att, attnum - 1);

                /*
                 * This must be a reference from the expression of a generated
                 * column elsewhere in the same table. Dropping the type of a
                 * column that is used by a generated column is not allowed by
                 * SQL standard.
                 */
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("cannot drop a column used by a generated column"),
                         errdetail("Column \"%s\" is used by generated column \"%s\".",
                                   NameStr(att->attname),
                                   get_attname(foundObject.objectId, foundObject.objectSubId, false))));
            }
        }
    }

    systable_endscan(scan);
    relation_close(depRel, AccessShareLock);
}

static void pltsql_PreAddConstraintsHook(Relation rel, ParseState *pstate, List *newColDefaults)
{
    if (!DB_IS_CMPT(D_FORMAT)) {
        return;
    }
    
    ListCell   *cell;
    Relation    attrelation = NULL;
    ComputedColumnContext context;

    /* Call previous hook if exists */
    if (prev_InvokePreAddConstraintsHook) {
        ((InvokePreAddConstraintsHookType)(prev_InvokePreAddConstraintsHook))(rel, pstate, newColDefaults);
    }

    /*
     * TSQL: Support for computed columns
     *
     * For a computed column, datatype is not provided by the user.  Hence,
     * we've to evaluate the computed column expression in order to determine
     * the datatype.  By now, we should've already made an entry for the
     * relatio in the catalog, which means we can execute transformExpr on the
     * computed column expression. Once we determine the datatype of the
     * column, we'll update the corresponding entry in the catalog.
     */
    context = (ComputedColumnContext)palloc0(sizeof(ComputedColumnContextData));
    context->pstate = pstate;
    context->rel = rel;
    context->gen_column_list = NIL;

    /*
     * Collect the names of all computed columns first.  We need this in order
     * to detect nested computed columns later.
     */
    foreach(cell, newColDefaults) {
        RawColumnDefault *colDef = (RawColumnDefault *)lfirst(cell);
        Form_pg_attribute atp = TupleDescAttr(rel->rd_att, colDef->attnum - 1);

        if (colDef->generatedCol != ATTRIBUTE_GENERATED_PERSISTED) {
            continue;
        }
        context->gen_column_list = lappend(context->gen_column_list,
                                           NameStr(atp->attname));
    }

    foreach(cell, newColDefaults) {
        RawColumnDefault *colDef = (RawColumnDefault *) lfirst(cell);
        Form_pg_attribute atp = TupleDescAttr(rel->rd_att, colDef->attnum - 1);
        Node       *expr;
        Oid            targettype;
        int32        targettypmod;
        HeapTuple    heapTup;
        Type        targetType;
        Form_pg_attribute attTup;
        Form_pg_type tform;

        /* skip if not a computed column */
        if (colDef->generatedCol != ATTRIBUTE_GENERATED_PERSISTED) {
            continue;
        }
            

        /*
         * Since we're using a dummy datatype for a computed column, we need
         * to check for a nested computed column usage in the expression
         * before evaluating the expression through transformExpr. N.B. When
         * we add a new column through ALTER command, it's possible that the
         * expression includes another computed column in the table.  We'll
         * not be able to detetct that case here.  That'll be handled later in
         * check_nested_generated that works on the executable expression.
         */
        Assert(context->gen_column_list != NULL);
        check_nested_computed_column(colDef->raw_default, context);

        /*
         * transform raw parsetree to executable expression.
         */
        expr = transformExpr(pstate, colDef->raw_default, EXPR_KIND_GENERATED_COLUMN);

        /* extract the type and other relevant information */
        targettype = exprType(expr);
        targettypmod = exprTypmod(expr);

        /* now update the attribute catalog entry with the correct type */
        if (!RelationIsValid(attrelation)) {
            attrelation = relation_open(AttributeRelationId, RowExclusiveLock);
        }

        /* Look up the target column */
        heapTup = SearchSysCacheCopyAttNum(RelationGetRelid(rel), colDef->attnum);
        if (!HeapTupleIsValid(heapTup)) { /* shouldn't happen */
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_COLUMN),
                     errmsg("column number %d of relation \"%s\" does not exist",
                            colDef->attnum, RelationGetRelationName(rel))));
        }

        attTup = (Form_pg_attribute) GETSTRUCT(heapTup);

        targetType = typeidType(targettype);
        tform = (Form_pg_type) GETSTRUCT(targetType);

        attTup->atttypid = targettype;
        attTup->atttypmod = targettypmod;
        attTup->attcollation = 0;

        attTup->attndims = tform->typndims;
        attTup->attlen = tform->typlen;
        attTup->attbyval = tform->typbyval;
        attTup->attalign = tform->typalign;
        attTup->attstorage = tform->typstorage;

        /*
         * Instead of invalidating and refetching the relcache entry, just
         * update the entry that we've fetched previously.  This works because
         * no one else can see our in-progress changes.  Also note that we
         * only updated the fixed part of Form_pg_attribute.
         */
        errno_t rc = memcpy_s(atp, ATTRIBUTE_FIXED_PART_SIZE, attTup, ATTRIBUTE_FIXED_PART_SIZE);
        securec_check(rc, "\0", "\0");

        CatalogTupleUpdate(attrelation, &heapTup->t_self, heapTup);
        ReleaseSysCache((HeapTuple) targetType);

        /* Cleanup */
        heap_freetuple(heapTup);
    }

    if (RelationIsValid(attrelation)) {
        relation_close(attrelation, RowExclusiveLock);
        /* Make the updated catalog row versions visible */
        CommandCounterIncrement();
    }

    list_free(context->gen_column_list);
    pfree(context);
}

static void pltsql_TransformSelectForLimitHook(SelectStmt* stmt)
{
    if (DB_IS_CMPT(D_FORMAT) && stmt->limitWithTies && stmt->sortClause == NIL) {
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                errmodule(MOD_EXECUTOR),
                errmsg("The WITH TIES clause is not allowed without a corresponding ORDER BY clause.")));
    }
}

static void pltsql_RecomputeLimitsHook(float8 val)
{
    if (DB_IS_CMPT(D_FORMAT) && (val < 0 || val > 100)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE),
                errmodule(MOD_EXECUTOR),
                errmsg("Percent values must be between 0 and 100.")));
    }
}

/*
 * Given a pg_attrdef OID, return the relation OID and column number of
 * the owning column (represented as an ObjectAddress for convenience).
 *
 * Returns InvalidObjectAddress if there is no such pg_attrdef entry.
 */
static ObjectAddress GetAttrDefaultColumnAddress(Oid attrdefoid)
{
    ObjectAddress result = InvalidObjectAddress;
    Relation    attrdef;
    ScanKeyData skey[1];
    SysScanDesc scan;
    HeapTuple    tup;

    attrdef = relation_open(AttrDefaultRelationId, AccessShareLock);
    ScanKeyInit(&skey[0],
                Anum_pg_attrdef_adrelid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(attrdefoid));
    scan = systable_beginscan(attrdef, AttrDefaultOidIndexId, true,
                              NULL, 1, skey);

    if (HeapTupleIsValid(tup = systable_getnext(scan))) {
        Form_pg_attrdef atdform = (Form_pg_attrdef) GETSTRUCT(tup);

        result.classId = RelationRelationId;
        result.objectId = atdform->adrelid;
        result.objectSubId = atdform->adnum;
    }

    systable_endscan(scan);
    relation_close(attrdef, AccessShareLock);
    return result;
}

static bool check_nested_computed_column(Node *node, void *context)
{
    if (node == NULL) {
        return false;
    } else if (IsA(node, ColumnRef)) {
        ColumnRef  *cref = (ColumnRef *)node;
        ParseState *pstate = ((ComputedColumnContext) context)->pstate;

        switch (list_length(cref->fields)) {
            case 1: {
                    Node       *field1 = (Node *)linitial(cref->fields);
                    List       *colList;
                    char       *col1name;
                    ListCell   *lc;
                    Relation    rel;

                    colList = ((ComputedColumnContext)context)->gen_column_list;
                    rel = ((ComputedColumnContext)context)->rel;

                    Assert(IsA(field1, String));
                    col1name = strVal(field1);

                    foreach(lc, colList) {
                        char       *col2name = (char *)lfirst(lc);
                        if (strcmp(col1name, col2name) == 0) {
                            ereport(ERROR,
                                    (errcode(ERRCODE_SYNTAX_ERROR),
                                     errmsg("computed column \"%s\" in table \"%s\" is not allowed to "
                                            "be used in another computed-column definition",
                                            col2name, RelationGetRelationName(rel)),
                                     parser_errposition(pstate, cref->location)));
                        }
                    }
            } break;
            default:

                /*
                 * In CREATE/ALTER TABLE command, the name of the column
                 * should have only one field.
                 */
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("improper column name in CREATE/ALTER TABLE(too many dotted names): %s",
                                NameListToString(cref->fields)),
                         parser_errposition(pstate, cref->location)));
        }
    }

    return raw_expression_tree_walker(node, (bool (*)())check_nested_computed_column, (void*)context);
}