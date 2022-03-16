/* -------------------------------------------------------------------------
 *
 * analyze.cpp
 *	  transform the raw parse tree into a query tree
 *
 * For optimizable statements, we are careful to obtain a suitable lock on
 * each referenced table, and other modules of the backend preserve or
 * re-obtain these locks before depending on the results.  It is therefore
 * okay to do significant semantic analysis of these statements.  For
 * utility commands, no locks are obtained here (and if they were, we could
 * not be sure we'd still have them at execution).  Hence the general rule
 * for utility commands is to just dump them into a Query node untransformed.
 * DECLARE CURSOR, EXPLAIN, and CREATE TABLE AS are exceptions because they
 * contain optimizable statements, which we should transform.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *	src/common/backend/parser/analyze.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/sysattr.h"
#ifdef PGXC
#include "catalog/pg_inherits.h"
#include "catalog/pg_inherits_fn.h"
#include "catalog/pg_proc.h"
#include "catalog/pgxc_class.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "utils/fmgroids.h"
#include "utils/snapmgr.h"
#endif
#include "catalog/pg_type.h"
#include "executor/node/nodeModifyTable.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/var.h"
#include "parser/analyze.h"
#include "parser/parse_agg.h"
#include "parser/parse_clause.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_cte.h"
#include "parser/parse_hint.h"
#include "parser/parse_merge.h"
#include "parser/parse_oper.h"
#include "parser/parse_param.h"
#include "parser/parse_relation.h"
#include "parser/parse_target.h"
#include "parser/parse_type.h"
#include "parser/parse_expr.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteManip.h"
#include "rewrite/rewriteRlsPolicy.h"
#ifdef PGXC
#include "miscadmin.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "access/gtm.h"
#include "utils/distribute_test.h"
#include "utils/lsyscache.h"
#include "optimizer/pgxcplan.h"
#include "tcop/tcopprot.h"
#include "nodes/nodes.h"
#include "pgxc/poolmgr.h"
#include "catalog/pgxc_node.h"
#include "access/xact.h"
#include "utils/distribute_test.h"
#include "tcop/utility.h"
#endif
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/acl.h"
#include "commands/explain.h"
#include "commands/sec_rls_cmds.h"
#include "streaming/streaming_catalog.h"
#include "instruments/instr_unique_sql.h"
#include "streaming/init.h"

#include "db4ai/aifuncs.h"
#include "db4ai/create_model.h"
#include "db4ai/hyperparameter_validation.h"

#ifndef ENABLE_MULTIPLE_NODES
#include "optimizer/clauses.h"
#endif
/* Hook for plugins to get control at end of parse analysis */
THR_LOCAL post_parse_analyze_hook_type post_parse_analyze_hook = NULL;
static const int MILLISECONDS_PER_SECONDS = 1000;

static Query* transformDeleteStmt(ParseState* pstate, DeleteStmt* stmt);
static Query* transformInsertStmt(ParseState* pstate, InsertStmt* stmt);
static void checkUpsertTargetlist(Relation targetTable, List* updateTlist);
static UpsertExpr* transformUpsertClause(ParseState* pstate, UpsertClause* upsertClause, RangeVar* relation);
static int count_rowexpr_columns(ParseState* pstate, Node* expr);
static Query* transformSelectStmt(
    ParseState* pstate, SelectStmt* stmt, bool isFirstNode = true, bool isCreateView = false);
static Query* transformValuesClause(ParseState* pstate, SelectStmt* stmt);
static Query* transformSetOperationStmt(ParseState* pstate, SelectStmt* stmt);
static Node* transformSetOperationTree(ParseState* pstate, SelectStmt* stmt, bool isTopLevel, List** targetlist);
static void determineRecursiveColTypes(ParseState* pstate, Node* larg, List* nrtargetlist);
static Query* transformUpdateStmt(ParseState* pstate, UpdateStmt* stmt);
static List* transformUpdateTargetList(ParseState* pstate, List* qryTlist, List* origTlist, RangeVar* stmtrel);
static List* transformReturningList(ParseState* pstate, List* returningList);
static Query* transformDeclareCursorStmt(ParseState* pstate, DeclareCursorStmt* stmt);
static Query* transformExplainStmt(ParseState* pstate, ExplainStmt* stmt);
static Query* transformCreateTableAsStmt(ParseState* pstate, CreateTableAsStmt* stmt);
#ifdef PGXC
static Query* transformExecDirectStmt(ParseState* pstate, ExecDirectStmt* stmt);
static bool IsExecDirectUtilityStmt(const Node* node);
static bool is_relation_child(RangeTblEntry* child_rte, List* rtable);
static bool is_rel_child_of_rel(RangeTblEntry* child_rte, RangeTblEntry* parent_rte);
#endif

static void transformLockingClause(ParseState* pstate, Query* qry, LockingClause* lc, bool pushedDown);
static bool IsFindHDFSForeignTbl(Query* qry);

static bool checkForeignTableExist(List* rte_table_list);
static void set_subquery_is_under_insert(ParseState* subParseState);
static void set_ancestor_ps_contain_foreigntbl(ParseState* subParseState);
static bool include_groupingset(Node* groupClause);
static void transformGroupConstToColumn(ParseState* pstate, Node* groupClause, List* targetList);
static bool checkAllowedTableCombination(ParseState* pstate);
#ifdef ENABLE_MULTIPLE_NODES
static bool ContainSubLinkWalker(Node* node, void* context);
static bool ContainSubLink(Node* clause);
#endif /* ENABLE_MULTIPLE_NODES */

#ifndef ENABLE_MULTIPLE_NODES
static const char* NOKEYUPDATE_KEYSHARE_ERRMSG = "/NO KEY UPDATE/KEY SHARE";
#else
static const char* NOKEYUPDATE_KEYSHARE_ERRMSG = "";
#endif

/*
 * parse_analyze
 *		Analyze a raw parse tree and transform it to Query form.
 *
 * Optionally, information about $n parameter types can be supplied.
 * References to $n indexes not defined by paramTypes[] are disallowed.
 *
 * The result is a Query node.	Optimizable statements require considerable
 * transformation, while utility-type statements are simply hung off
 * a dummy CMD_UTILITY Query node.
 */
Query* parse_analyze(
    Node* parseTree, const char* sourceText, Oid* paramTypes, int numParams, bool isFirstNode, bool isCreateView)
{
    ParseState* pstate = make_parsestate(NULL);
    Query* query = NULL;

    /* required as of 8.4 */
    AssertEreport(sourceText != NULL, MOD_OPT, "para cannot be NULL");

    pstate->p_sourcetext = sourceText;

    if (numParams > 0) {
        parse_fixed_parameters(pstate, paramTypes, numParams);
    }

    PUSH_SKIP_UNIQUE_SQL_HOOK();
    query = transformTopLevelStmt(pstate, parseTree, isFirstNode, isCreateView);
    POP_SKIP_UNIQUE_SQL_HOOK();

    /* it's unsafe to deal with plugins hooks as dynamic lib may be released */
    if (post_parse_analyze_hook && !(g_instance.status > NoShutdown)) {
        (*post_parse_analyze_hook)(pstate, query);
    }

    pfree_ext(pstate->p_ref_hook_state);
    free_parsestate(pstate);

    /* For plpy CTAS query. CTAS is a recursive call. CREATE query is the first rewrited.
     * thd 2nd rewrited query is INSERT SELECT. Without this attribute, DB will have
     * an error that has no idea about $x when INSERT SELECT query is analyzed.
     */
    query->fixed_paramTypes = paramTypes;
    query->fixed_numParams = numParams;

    return query;
}

/*
 * parse_analyze_varparams
 *
 * This variant is used when it's okay to deduce information about $n
 * symbol datatypes from context.  The passed-in paramTypes[] array can
 * be modified or enlarged (via repalloc).
 */

Query* parse_analyze_varparams(Node* parseTree, const char* sourceText, Oid** paramTypes, int* numParams)
{
    ParseState* pstate = make_parsestate(NULL);
    Query* query = NULL;

    /* required as of 8.4 */
    AssertEreport(sourceText != NULL, MOD_OPT, "para cannot be NULL");

    pstate->p_sourcetext = sourceText;

    parse_variable_parameters(pstate, paramTypes, numParams);

    query = transformTopLevelStmt(pstate, parseTree);

    /* make sure all is well with parameter types */
    check_variable_parameters(pstate, query);

    /* it's unsafe to deal with plugins hooks as dynamic lib may be released */
    if (post_parse_analyze_hook && !(g_instance.status > NoShutdown)) {
        (*post_parse_analyze_hook)(pstate, query);
    }

    pfree_ext(pstate->p_ref_hook_state);
    free_parsestate(pstate);

    return query;
}

/*
 * parse_sub_analyze
 *		Entry point for recursively analyzing a sub-statement.
 */
Query* parse_sub_analyze(Node* parseTree, ParseState* parentParseState, CommonTableExpr* parentCTE,
    bool locked_from_parent, bool resolve_unknowns)
{
    ParseState* pstate = make_parsestate(parentParseState);
    Query* query = NULL;

    pstate->p_parent_cte = parentCTE;
    pstate->p_locked_from_parent = locked_from_parent;
    pstate->p_resolve_unknowns = resolve_unknowns;
    if (u_sess->attr.attr_sql.td_compatible_truncation && u_sess->attr.attr_sql.sql_compatibility == C_FORMAT)
        set_subquery_is_under_insert(pstate); /* Set p_is_in_insert for parse state.*/

    query = transformStmt(pstate, parseTree);

    free_parsestate(pstate);

    return query;
}

/*
 * transformTopLevelStmt -
 *	  transform a Parse tree into a Query tree.
 *
 * The only thing we do here that we don't do in transformStmt() is to
 * convert SELECT ... INTO into CREATE TABLE AS.  Since utility statements
 * aren't allowed within larger statements, this is only allowed at the top
 * of the parse tree, and so we only try it before entering the recursive
 * transformStmt() processing.
 */
Query* transformTopLevelStmt(ParseState* pstate, Node* parseTree, bool isFirstNode, bool isCreateView)
{
    if (IsA(parseTree, SelectStmt)) {
        SelectStmt* stmt = (SelectStmt*)parseTree;

        /* If it's a set-operation tree, drill down to leftmost SelectStmt */
        while (stmt != NULL && stmt->op != SETOP_NONE)
            stmt = stmt->larg;
        AssertEreport(stmt && IsA(stmt, SelectStmt) && stmt->larg == NULL, MOD_OPT, "failure to check parseTree");

        if (stmt->intoClause) {
            CreateTableAsStmt* ctas = makeNode(CreateTableAsStmt);

            ctas->query = parseTree;
            ctas->into = stmt->intoClause;
            ctas->relkind = OBJECT_TABLE;
            ctas->is_select_into = true;

            /*
             * Remove the intoClause from the SelectStmt.  This makes it safe
             * for transformSelectStmt to complain if it finds intoClause set
             * (implying that the INTO appeared in a disallowed place).
             */
            stmt->intoClause = NULL;

            parseTree = (Node*)ctas;
        }
    }

    if (u_sess->hook_cxt.transformStmtHook != NULL) {
        return
            ((transformStmtFunc)(u_sess->hook_cxt.transformStmtHook))(pstate, parseTree, isFirstNode, isCreateView);
    }
    return transformStmt(pstate, parseTree, isFirstNode, isCreateView);
}


Query* transformCreateModelStmt(ParseState* pstate, CreateModelStmt* stmt)
{
    SelectStmt* select_stmt = (SelectStmt*) stmt->select_query;

    stmt->algorithm     = get_algorithm_ml(stmt->architecture);
    if (stmt->algorithm == INVALID_ALGORITHM_ML) {
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Non recognized ML model architecture definition %s", stmt->architecture)));
    }
    if (SearchSysCacheExists1(DB4AI_MODEL, CStringGetDatum(stmt->model))) {
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("The model name \"%s\" already exists in gs_model_warehouse.", stmt->model)));
    }

    // Create the projection for the AI operator in the query plan
    // If the algorithm is supervised, the target is always the first element of the list
    if (is_supervised(stmt->algorithm)) {
        if (list_length(stmt->model_features) == 0) {
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Supervised ML algorithms require FEATURES clause")));
        }else if (list_length(stmt->model_target) == 0) {
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Supervised ML algorithms require TARGET clause")));
        }else if (list_length(stmt->model_target) > 1) {
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Target clause only supports one expression")));
        }
    }else{
        if (list_length(stmt->model_target) > 0) {
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Unsupervised ML algorithms cannot have TARGET clause")));
        }
    }

    select_stmt->targetList = NULL;
    foreach_cell(it, stmt->model_target) {
        select_stmt->targetList = lappend(select_stmt->targetList, lfirst(it));
    }

    if (list_length(stmt->model_features) > 0) { // User given projection
        foreach_cell(it, stmt->model_features) {
            select_stmt->targetList = lappend(select_stmt->targetList, lfirst(it));
        }

    } else { // No projection
        ResTarget *rt = makeNode(ResTarget);
        ColumnRef *cr = makeNode(ColumnRef);

        cr->fields = list_make1(makeNode(A_Star));
        cr->location = -1;

        rt->name = NULL;
        rt->indirection = NIL;
        rt->val = (Node *)cr;
        rt->location = -1;
        select_stmt->targetList = lappend(select_stmt->targetList, rt);
    }

    // Transform the select query that we prepared for the training operator
    Query* select_query = transformStmt(pstate, (Node*) select_stmt);
    stmt->select_query  = (Node*) select_query;

    /* represent the command as a utility Query */
    Query* result = makeNode(Query);
    result->commandType = CMD_UTILITY;
    result->utilityStmt = (Node*)stmt;

    return result;
}

/*
 * transformStmt -
 *	  recursively transform a Parse tree into a Query tree.
 */
Query* transformStmt(ParseState* pstate, Node* parseTree, bool isFirstNode, bool isCreateView)
{
    Query* result = NULL;
    AnalyzerRoutine *analyzerRoutineHook = (AnalyzerRoutine*)u_sess->hook_cxt.analyzerRoutineHook;

    switch (nodeTag(parseTree)) {
            /*
             * Optimizable statements
             */
        case T_InsertStmt:
            result = transformInsertStmt(pstate, (InsertStmt*)parseTree);
            break;

        case T_DeleteStmt:
            result = transformDeleteStmt(pstate, (DeleteStmt*)parseTree);
            break;

        case T_UpdateStmt:
            result = transformUpdateStmt(pstate, (UpdateStmt*)parseTree);
            break;

        case T_MergeStmt:
            result = transformMergeStmt(pstate, (MergeStmt*)parseTree);
            break;

        case T_SelectStmt: {
            SelectStmt* n = (SelectStmt*)parseTree;
            if (n->valuesLists) {
                result = transformValuesClause(pstate, n);
            } else if (n->op == SETOP_NONE) {
                if (analyzerRoutineHook == NULL || analyzerRoutineHook->transSelect == NULL) {
                    result = transformSelectStmt(pstate, n, isFirstNode, isCreateView);
                } else {
                    result = analyzerRoutineHook->transSelect(pstate, n, isFirstNode, isCreateView);
                }
            } else {
                result = transformSetOperationStmt(pstate, n);
            }
        } break;

            /*
             * Special cases
             */
        case T_DeclareCursorStmt:
            result = transformDeclareCursorStmt(pstate, (DeclareCursorStmt*)parseTree);
            break;

        case T_ExplainStmt:
            result = transformExplainStmt(pstate, (ExplainStmt*)parseTree);
            break;

#ifdef PGXC
        case T_ExecDirectStmt:
            result = transformExecDirectStmt(pstate, (ExecDirectStmt*)parseTree);
            break;
#endif

        case T_CreateTableAsStmt:
            result = transformCreateTableAsStmt(pstate, (CreateTableAsStmt*)parseTree);
            break;

        case T_CreateModelStmt:
            result = transformCreateModelStmt(pstate, (CreateModelStmt*) parseTree);
            break;


        default:

            /*
             * other statements don't require any transformation; just return
             * the original parsetree with a Query node plastered on top.
             */
            result = makeNode(Query);
            result->commandType = CMD_UTILITY;
            result->utilityStmt = (Node*)parseTree;
            break;
    }

    /* Mark as original query until we learn differently */
    result->querySource = QSRC_ORIGINAL;
    result->canSetTag = true;

    /* Mark whether synonym object is in rtables or not. */
    result->hasSynonyms = pstate->p_hasSynonyms;

    return result;
}

/*
 * analyze_requires_snapshot
 *		Returns true if a snapshot must be set before doing parse analysis
 *		on the given raw parse tree.
 *
 * Classification here should match transformStmt(); but we also have to
 * allow a NULL input (for Parse/Bind of an empty query string).
 */
bool analyze_requires_snapshot(Node* parseTree)
{
    bool result = false;

    if (parseTree == NULL)
        return false;

    switch (nodeTag(parseTree)) {
            /*
             * Optimizable statements
             */
        case T_InsertStmt:
        case T_DeleteStmt:
        case T_UpdateStmt:
        case T_MergeStmt:
        case T_SelectStmt:
            result = true;
            break;

            /*
             * Special cases
             */
        case T_DeclareCursorStmt:
            /* yes, because it's analyzed just like SELECT */
            result = true;
            break;

        case T_ExplainStmt:
        case T_CreateTableAsStmt:
            /* yes, because we must analyze the contained statement */
            result = true;
            break;

#ifdef PGXC
        case T_ExecDirectStmt:

            /*
             * We will parse/analyze/plan inner query, which probably will
             * need a snapshot. Ensure it is set.
             */
            result = true;
            break;
#endif
        case T_RefreshMatViewStmt:
            /* yes, because the SELECT from pg_rewrite must be analyzed */
            if ((!((RefreshMatViewStmt *)parseTree)->incremental) || IS_PGXC_DATANODE) {
                result = true;
            }
            break;

        default:
            /* other utility statements don't have any real parse analysis */
            result = false;
            break;
    }

    return result;
}

/*
 * Description: check if the delete stmt is for 'plan_table'.
 * Parameters:
 * @in relname: the object name that delete from.
 * Return: ture if the object name is plan_table
 */
static bool checkDeleteStmtForPlanTable(const char* relname)
{
    OnlyDeleteFromPlanTable = false;
    const char* target_rel = V_PLAN_TABLE;
    Oid plan_table_data_oid = RelnameGetRelid(T_PLAN_TABLE_DATA);
    if (strcasecmp(relname, target_rel) == 0 && plan_table_data_oid != InvalidOid)
        return true;

    return false;
}

/*
 * Description: make SessionIdExpr for WhereClause.
 * Parameters:
 * @in cur_location: current location.
 * @out lexpr: the left expr.
 * @out rexpr: the right expr.
 * @out op_loc: operation location.
 * Return: void
 */
static void makeSessionIdExpr(Node** lexpr, Node** rexpr, int* cur_location, int* op_loc)
{
    /* make filter condition for session_id */
    char* lcolname = "session_id";
    ColumnRef* c = makeNode(ColumnRef);
    c->fields = list_make1(makeString(lcolname));
    c->location = *cur_location;

    *cur_location = *cur_location + strlen("session_id=");
    *op_loc = *cur_location - 1;

    char* s_id = (char*)palloc0(SESSION_ID_LEN);
    getSessionID(s_id,
        IS_THREAD_POOL_WORKER ? u_sess->proc_cxt.MyProcPort->SessionStartTime : t_thrd.proc_cxt.MyStartTime,
        IS_THREAD_POOL_WORKER ? u_sess->session_id : t_thrd.proc_cxt.MyProcPid);
    A_Const* n = makeNode(A_Const);
    n->val.type = T_String;
    n->val.val.str = s_id;
    n->location = *cur_location;

    *cur_location = *cur_location + strlen(s_id) + 3;

    *lexpr = (Node*)c;
    *rexpr = (Node*)n;
}

/*
 * Description: make UserIdExpr for WhereClause.
 * Parameters:
 * @in cur_location: current location.
 * @out lexpr: the left expr.
 * @out rexpr: the right expr.
 * @out op_loc: operation location.
 * Return: void
 */
static void makeUserIdExpr(Node** lexpr, Node** rexpr, int* cur_location, int* op_loc)
{
    /* for filter condition user_id */
    char* lcolname = "user_id";
    ColumnRef* c = makeNode(ColumnRef);
    c->fields = list_make1(makeString(lcolname));
    c->location = *cur_location;

    *cur_location = *cur_location + strlen("user_id=");
    *op_loc = *cur_location - 1;

    Oid user_id = GetCurrentUserId();
    A_Const* n = makeNode(A_Const);
    n->val.type = T_Integer;
    n->val.val.ival = user_id;
    n->location = *cur_location;

    *lexpr = (Node*)c;
    *rexpr = (Node*)n;
}

/*
 * Description: make WhereClause node including SessionIdExpr and UserIdExpr for plan_table detele stmt.
 * Parameters:
 * @in cur_location: current location.
 * @out whereClause: WhereClause that we made.
 * Return: void
 */
static void makeNewWhereClause(Node** whereClause, int* cur_location)
{
    Node* lexpr = NULL;
    Node* rexpr = NULL;
    A_Expr* clause_1 = NULL;
    A_Expr* clause_2 = NULL;

    int op_loc = -1;
    int and_loc = -1;

    makeSessionIdExpr(&lexpr, &rexpr, cur_location, &op_loc);
    clause_1 = makeSimpleA_Expr(AEXPR_OP, "=", lexpr, rexpr, op_loc);

    and_loc = *cur_location;
    *cur_location = *cur_location + strlen("AND ");

    makeUserIdExpr(&lexpr, &rexpr, cur_location, &op_loc);
    clause_2 = makeSimpleA_Expr(AEXPR_OP, "=", lexpr, rexpr, op_loc);

    *whereClause = (Node*)makeA_Expr(AEXPR_AND, NIL, (Node*)clause_1, (Node*)clause_2, and_loc);
}

/*
 * Description: add WhereClause for plan_table detele stmt.
 * Parameters:
 * @in pstate: ParseState.
 * @int stmt: delete stmt that user input.
 * Return: DeleteStmt that plus new WhereClause.
 */
static DeleteStmt* addWhereClauseForPlanTable(ParseState* pstate, DeleteStmt* stmt)
{
    int current_location = -1;

    DeleteStmt* new_stmt = (DeleteStmt*)copyObject(stmt);
    new_stmt->relation->relname = T_PLAN_TABLE_DATA;

    current_location = strlen("delete from plan_table_data where ");

    /* make a new WhereClause include session_id and user_id filter condition. */
    if (new_stmt->whereClause == NULL) {
        makeNewWhereClause(&new_stmt->whereClause, &current_location);
    } else {
        /* add a WhereClause include session_id and user_id filter condition, behind original WhereClause. */
        makeNewWhereClause(&new_stmt->whereClause, &current_location);

        new_stmt->whereClause = (Node*)makeA_Expr(AEXPR_AND, NIL, new_stmt->whereClause, stmt->whereClause, -1);
    }

    return new_stmt;
}

const void SendCommandIdForInsertCte(Query* qry, WithClause* withclause)
{
    ListCell* tl = NULL;

    /*
     * For a WITH query that deletes from a parent table in the
     * main query & inserts a row in the child table in the WITH query
     * we need to use command ID communication to remote nodes in order
     * to maintain global data visibility.
     * For example
     * CREATE TEMP TABLE parent ( id int, val text ) DISTRIBUTE BY REPLICATION;
     * CREATE TEMP TABLE child ( ) INHERITS ( parent ) DISTRIBUTE BY REPLICATION;
     * INSERT INTO parent VALUES ( 42, 'old' );
     * INSERT INTO child VALUES ( 42, 'older' );
     * WITH wcte AS ( INSERT INTO child VALUES ( 42, 'new' ) RETURNING id AS newid )
     * DELETE FROM parent USING wcte WHERE id = newid;
     * The last query gets translated into the following multi-statement
     * transaction on the primary datanode
     * (a) SELECT id, ctid FROM ONLY parent WHERE true
     * (b) START TRANSACTION ISOLATION LEVEL read committed READ WRITE
     * (c) INSERT INTO child (id, val) VALUES ($1, $2) RETURNING id -- (42, 'new')
     * (d) DELETE FROM ONLY parent parent WHERE (parent.ctid = $1)
     * (e) SELECT id, ctid FROM ONLY child parent WHERE true
     * (f) DELETE FROM ONLY child parent WHERE (parent.ctid = $1)
     * (g) COMMIT TRANSACTION
     * The command id of the select in step (e), should be such that
     * it does not see the insert of step (c)
     */
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        foreach (tl, withclause->ctes) {
            CommonTableExpr* cte = (CommonTableExpr*)lfirst(tl);
            if (IsA(cte->ctequery, InsertStmt)) {
                qry->has_to_save_cmd_id = true;
                SetSendCommandId(true);
                break;
            }
        }
    }
}

/*
 * CheckTsDelete - check if delete by timerange
 */
#ifdef ENABLE_MULTIPLE_NODES
/*
 * check delete by time range or not
 */
static bool check_del_timerange(const Node* qual)
{
    if (qual == NULL) {
        return false;
    }
    OpExpr* opexpr = (OpExpr*)qual;
    const int filter_param_num = 2;

    if ((nodeTag(qual) == T_OpExpr) && (list_length(opexpr->args) == filter_param_num) &&
        ((opexpr->opfuncid <= INTERVALINFUNCOID && opexpr->opfuncid >= TIMESTAMPTZOUTFUNCOID) ||
        (opexpr->opfuncid <= TIMESTAMPTZCMPTIMESTAMPFUNCOID &&
        opexpr->opfuncid >= TIMESTAMPLTTIMESTAMPTZFUNCOID) ||
        (opexpr->opfuncid <= TimeOprId::TIMESTAMP_OP_MAX &&
        opexpr->opfuncid >= TimeOprId::TIMESTAMP_OP_MIN) ||
        (opexpr->opfuncid <= TimeOprId::TIMESTAMP_DATE_OP_MAX &&
        opexpr->opfuncid >= TimeOprId::TIMESTAMP_DATE_OP_MIN))) {
        return true;
    } else if ((nodeTag(qual) == T_BoolExpr) && (((BoolExpr*)qual)->boolop == OR_EXPR)) {
        bool check_time_qual = true;
        ListCell* cell = NULL;
        /* if is or expr, we must confirm each sub-clause has time range in least 1 hour */
        for (cell = list_head(((BoolExpr*)qual)->args); cell != NULL && check_time_qual; cell = lnext(cell)) {
            check_time_qual = check_time_qual && check_del_timerange((Node*)lfirst(cell));
        }
        return check_time_qual;
    } else if ((nodeTag(qual) == T_BoolExpr) && (((BoolExpr*)qual)->boolop == AND_EXPR)) {
        bool check_time_qual = false;
        ListCell* cell = NULL;
        /* if is and expr, we can just have one sub-clause has time range in least 1 hour */
        for (cell = list_head(((BoolExpr*)qual)->args); cell != NULL; cell = lnext(cell)) {
            check_time_qual = check_time_qual || check_del_timerange((Node*)lfirst(cell));
        }
        return check_time_qual;
    }

    return false;
}

static void CheckTsDelete(const ParseState* pstate, const Query* qry)
{
    if (pstate == NULL || qry == NULL) {
        ereport(ERROR, (errmodule(MOD_TIMESERIES), errmsg("Parsing null pointer to CheckTsDelete!")));
    }
    if (g_instance.attr.attr_common.enable_tsdb && RelationIsTsStore(pstate->p_target_relation)) {
        if (qry->jointree == NULL) {
            ereport(ERROR, (errmodule(MOD_TIMESERIES), errmsg("Timeseries only supports deletion by time range!")));
        } else {
            Node* quals = qry->jointree->quals;
            if (!check_del_timerange(quals)) {
                ereport(ERROR, (errmodule(MOD_TIMESERIES), errmsg("Timeseries only supports deletion by time range!")));
            }
        }
    }
    return;    
}
#endif   /* ENABLE_MULTIPLE_NODES */

static bool IsSupportDeleteLimit(Relation rel, bool hasLimit)
{
    RelationLocInfo* relLocInfo = NULL;

    if (rel == NULL) {
        return true;
    }
    relLocInfo = rel->rd_locator_info;
    if (relLocInfo == NULL) {
        return true;
    }
    if (IsRelationReplicated(relLocInfo) && hasLimit) {
        return false;
    }
    return true;
}

/*
 * transformDeleteStmt -
 *	  transforms a Delete Statement
 */
static Query* transformDeleteStmt(ParseState* pstate, DeleteStmt* stmt)
{
    Query* qry = makeNode(Query);
    Node* qual = NULL;
    /*
     * Check the delete object name whether is 'paln_table'
     * If it is, we will make a new where clause that include session_id and user_id filter condition.
     */
    if (checkDeleteStmtForPlanTable(stmt->relation->relname)) {
        stmt = addWhereClauseForPlanTable(pstate, stmt);
        OnlyDeleteFromPlanTable = true;
    }

    qry->commandType = CMD_DELETE;
    /* set io state for backend status for the thread, we will use it to check user space */
    pgstat_set_io_state(IOSTATE_READ);
    /* process the WITH clause independently of all else */
    if (stmt->withClause) {
#ifdef PGXC
        SendCommandIdForInsertCte(qry, stmt->withClause);
#endif
        qry->hasRecursive = stmt->withClause->recursive;
        qry->cteList = transformWithClause(pstate, stmt->withClause);
        qry->hasModifyingCTE = pstate->p_hasModifyingCTE;
    }

    /* set up range table with just the result rel */
    qry->resultRelation =
        setTargetTable(pstate, stmt->relation, interpretInhOption(stmt->relation->inhOpt), true, ACL_DELETE);

    // check hdfs relation distributed by replication
    if (pstate->p_target_relation != NULL && pstate->p_target_relation->rd_locator_info &&
        IsLocatorReplicated(pstate->p_target_relation->rd_locator_info->locatorType) &&
        RelationIsPAXFormat(pstate->p_target_relation)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Un-support feature"),
                errdetail("hdfs replication table doesn't allow DELETE")));
    }

#ifdef ENABLE_MULTIPLE_NODES
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
#endif
        if (pstate->p_target_relation != NULL && RelationIsMatview(pstate->p_target_relation)) {
            ereport(ERROR,
                 (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("Unsupported feature"),
                 errdetail("Materialized view doesn't allow DELETE")));
        }
#ifdef ENABLE_MULTIPLE_NODES
    }
#endif

    /* check delete permission */
    if (pstate->p_target_relation &&
        ((unsigned int)RelationGetInternalMask(pstate->p_target_relation) & INTERNAL_MASK_DDELETE)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Un-support feature"),
                errdetail("internal relation doesn't allow DELETE")));
    }

    // @Online expansion: check if the target relation is being redistributed in read only mode
    if (!u_sess->attr.attr_sql.enable_cluster_resize && pstate->p_target_relation &&
        RelationInClusterResizingWriteErrorMode(pstate->p_target_relation)) {
        ereport(ERROR,
            (errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
                errmsg("%s is redistributing, please retry later.", pstate->p_target_relation->rd_rel->relname.data)));
    }

    qry->distinctClause = NIL;

    /*
     * The USING clause is non-standard SQL syntax, and is equivalent in
     * functionality to the FROM list that can be specified for UPDATE. The
     * USING keyword is used rather than FROM because FROM is already a
     * keyword in the DELETE syntax.
     */
    transformFromClause(pstate, stmt->usingClause);

    qual = transformWhereClause(pstate, stmt->whereClause, "WHERE");

    qry->returningList = transformReturningList(pstate, stmt->returningList);
    if (pstate->p_target_relation && qry->returningList != NIL && RelationIsColStore(pstate->p_target_relation)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Un-support feature"),
                errdetail("column stored relation doesn't support DELETE returning")));
    } else if (pstate->p_target_relation && qry->returningList != NIL && RelationIsTsStore(pstate->p_target_relation)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Un-support feature"),
                errdetail("Timeseries stored relation doesn't support DELETE returning")));
    }

    if (!checkAllowedTableCombination(pstate)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Un-supported feature"),
                errdetail("UStore relations cannot be used with other storage types in DELETE statement")));
    }

    /* done building the range table and jointree */
    qry->rtable = pstate->p_rtable;
    qry->jointree = makeFromExpr(pstate->p_joinlist, qual);

    qry->hasSubLinks = pstate->p_hasSubLinks;
    qry->hasWindowFuncs = pstate->p_hasWindowFuncs;
    if (pstate->p_hasWindowFuncs)
        parseCheckWindowFuncs(pstate, qry);
    qry->hasAggs = pstate->p_hasAggs;

    assign_query_collations(pstate, qry);

    /* this must be done after collations, for reliable comparison of exprs */
    if (pstate->p_hasAggs)
        parseCheckAggregates(pstate, qry);

    qry->hintState = stmt->hintState;
    
    qry->limitCount = transformLimitClause(pstate, stmt->limitClause, "LIMIT");
    if (!IsSupportDeleteLimit(pstate->p_target_relation, (qry->limitCount != NULL))) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Un-support feature"),
                errdetail("replication table doesn't allow DELETE LIMIT")));
    }

    if (qry->limitCount != NULL) {
        /* flag for discriminating rownum */
        Const *flag = makeNode(Const);
        flag->ismaxvalue = true;
        flag->location = -1;
        flag->consttype = INT8OID;
        flag->consttypmod = -1;
        qry->limitOffset = (Node*)flag;
    }
    
#ifdef ENABLE_MULTIPLE_NODES
    CheckTsDelete(pstate, qry);
#endif   /* ENABLE_MULTIPLE_NODES */

    return qry;
}

/* @hdfs
 * brief: Check whether there is a openGauss forein table in qry. Return true means
 *        openGauss foreign table existed, false not existed
 * input param @qry: the Query for foreign table
 */
static bool IsFindPgfdwForeignTbl(Query* qry)
{
    List* rtable = qry->rtable;
    ListCell* lc = NULL;

    foreach (lc, rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(lc);

        if (rte->relkind == RELKIND_FOREIGN_TABLE || rte->relkind == RELKIND_STREAM) {
            if (IS_POSTGRESFDW_FOREIGN_TABLE(rte->relid)) {
                return true;
            }
        }
    }

    return false;
}

/* @hdfs
 * brief: Check whether there is a HDFS forein table in qry. Return true means
 *        HDFS foreign table existed, false not existed
 * input param @qry: the Query for foreign table
 */
static bool IsFindHDFSForeignTbl(Query* qry)
{
    List* rtable = qry->rtable;
    ListCell* lc = NULL;

    foreach (lc, rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(lc);
        if (rte->relkind == RELKIND_FOREIGN_TABLE || rte->relkind == RELKIND_STREAM) {
            if (isObsOrHdfsTableFormTblOid(rte->relid) || IS_OBS_CSV_TXT_FOREIGN_TABLE(rte->relid)) {
                return true;
            }
        }
    }

    return false;
}

static Query* FindQueryContainForeignTbl(Query* qry)
{
    List* rtable = qry->rtable;
    ListCell* lc = NULL;

    foreach (lc, rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(lc);

        if (rte->relkind == RELKIND_FOREIGN_TABLE || rte->relkind == RELKIND_STREAM) {
            return qry;
        } else if (rte->rtekind == RTE_SUBQUERY) {
            return FindQueryContainForeignTbl(rte->subquery);
        }
    }

    ListCell* target = NULL;
    foreach (target, qry->targetList) {
        TargetEntry* entry = (TargetEntry*)lfirst(target);
        if (IsA(entry->expr, SubLink) && IsA(((SubLink*)entry->expr)->subselect, Query)) {
            Query* q = FindQueryContainForeignTbl((Query*)((SubLink*)entry->expr)->subselect);
            if (q != NULL)
                return q;
        }
    }
    return NULL;
}

#ifdef ENABLE_MOT
/*
 * @brief: Expression_tree_walker callback function.
 * Checks the entire RTE used by a query to identify their storage engine type (MOT or PAGE).
 */
static bool StorageEngineUsedWalker(Node* node, RTEDetectorContext* context)
{
    if (node == NULL) {
        return false;
    }

    if (IsA(node, RangeTblRef)) {
        RangeTblRef* rtr = (RangeTblRef*)node;
        Query* qry = (Query*)llast(context->queryNodes);
        RangeTblEntry* rte = rt_fetch(rtr->rtindex, qry->rtable);
        if (rte->rtekind == RTE_RELATION) {
            if (rte->relkind == RELKIND_FOREIGN_TABLE && isMOTFromTblOid(rte->relid)) {
                context->isMotTable = true;
            } else if (!is_sys_table(rte->relid)) {
                context->isPageTable = true;
            }
        }
        return false;
    }

    if (IsA(node, Query)) {
        /* Recurse into subselects */
        bool result = false;
        context->queryNodes = lappend(context->queryNodes, (Query*)node);
        context->sublevelsUp++;
        result = query_tree_walker((Query*)node, (bool (*)())StorageEngineUsedWalker, (void*)context, 0);
        context->sublevelsUp--;
        context->queryNodes = list_delete(context->queryNodes, llast(context->queryNodes));
        return result;
    }
    return expression_tree_walker(node, (bool (*)())StorageEngineUsedWalker, (void*)context);
}

/*
 * @brief: Analyze a query to check which storage engines are being used,
 * PG, MOT, mixed PG & MOT or Other type.
 * @input: Query to be analyzed.
 * @output: Type of storage engine.
 */
void CheckTablesStorageEngine(Query* qry, StorageEngineType* type)
{
    RTEDetectorContext context;
    context.queryNodes = NIL;
    context.sublevelsUp = 0;
    context.isMotTable = false;
    context.isPageTable = false;

    *type = SE_TYPE_UNSPECIFIED;

    /* check root node RTEs in case of non RangeTblRef nodes */
    List* rtable = qry->rtable;
    ListCell* lc = NULL;
    foreach (lc, rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(lc);
        if (rte->rtekind == RTE_RELATION) {
            if (rte->relkind == RELKIND_FOREIGN_TABLE && isMOTFromTblOid(rte->relid)) {
                context.isMotTable = true;
            } else if (!is_sys_table(rte->relid)) {
                context.isPageTable = true;
            }
        }
    }

    /* add root query to query stack list */
    context.queryNodes = lappend(context.queryNodes, qry);

    /* recursive walk on the query */
    (void)query_or_expression_tree_walker((Node*)qry, (bool (*)())StorageEngineUsedWalker, (void*)&context, 0);

    if (context.isMotTable && context.isPageTable) {
        *type = SE_TYPE_MIXED;
    } else if (context.isPageTable) {
        *type = SE_TYPE_PAGE_BASED;
    } else if (context.isMotTable) {
        *type = SE_TYPE_MOT;
    }
}

/*
 * @brief: Expression_tree_walker callback function.
 * Checks the query and all sub queries to identify if there is update on indexed column.
 */
static bool MotIndexedColumnUpdateWalker(Node* node, UpdateDetectorContext* context)
{
    if (node == NULL) {
        return false;
    }

    if (IsA(node, RangeTblRef)) {
        RangeTblRef* rtr = (RangeTblRef*)node;
        Query* qry = (Query*)llast(context->queryNodes);
        RangeTblEntry* rte = rt_fetch(rtr->rtindex, qry->rtable);
        if (qry->commandType == CMD_UPDATE && rte->rtekind == RTE_RELATION && rte->relkind == RELKIND_FOREIGN_TABLE &&
            isMOTFromTblOid(rte->relid)) {
            Relation rel = relation_open(rte->relid, AccessShareLock);
            Bitmapset* idx_bmps = RelationGetIndexAttrBitmap(rel, INDEX_ATTR_BITMAP_ALL);
            relation_close(rel, NoLock);
            if (idx_bmps == NULL) {
                return false;
            }
            ListCell* target = NULL;
            foreach (target, qry->targetList) {
                TargetEntry* entry = (TargetEntry*)lfirst(target);
                if (entry->resjunk) {
                    continue;
                }
                if (bms_is_member(entry->resno - FirstLowInvalidHeapAttributeNumber, idx_bmps)) {
                    context->isIndexedColumnUpdate = true;
                }
            }
            bms_free(idx_bmps);
        }
        return false;
    }

    if (IsA(node, Query)) {
        /* Recurse into subselects */
        bool result = false;
        context->queryNodes = lappend(context->queryNodes, (Query*)node);
        context->sublevelsUp++;
        result = query_tree_walker((Query*)node, (bool (*)())MotIndexedColumnUpdateWalker, (void*)context, 0);
        context->sublevelsUp--;
        context->queryNodes = list_delete(context->queryNodes, llast(context->queryNodes));
        return result;
    }

    return expression_tree_walker(node, (bool (*)())MotIndexedColumnUpdateWalker, (void*)context);
}

/*
 * @brief: Analyze a query to check if there is update on indexed column of MOT table.
 * @input: Query to be analyzed.
 * @output: True/false.
 */
bool CheckMotIndexedColumnUpdate(Query* qry)
{
    UpdateDetectorContext context;
    context.queryNodes = NIL;
    context.sublevelsUp = 0;
    context.isIndexedColumnUpdate = false;

    /* check root node RTEs in case of non RangeTblRef nodes */
    List* rtable = qry->rtable;
    ListCell* lc = NULL;
    foreach (lc, rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(lc);
        if (qry->commandType == CMD_UPDATE && rte->rtekind == RTE_RELATION && rte->relkind == RELKIND_FOREIGN_TABLE &&
            isMOTFromTblOid(rte->relid)) {
            Relation rel = relation_open(rte->relid, AccessShareLock);
            Bitmapset* idx_bmps = RelationGetIndexAttrBitmap(rel, INDEX_ATTR_BITMAP_ALL);
            relation_close(rel, NoLock);
            if (idx_bmps == NULL) {
                return false;
            }
            ListCell* target = NULL;
            foreach (target, qry->targetList) {
                TargetEntry* entry = (TargetEntry*)lfirst(target);
                /* Junk entry is temporary and won't affect the db. in addition, entry->resno might be wrong.
                 * We should ignore it.
                 * More info in src/backend/executor/execJunk.cpp
                 */
                if (entry->resjunk) {
                    continue;
                }
                if (bms_is_member(entry->resno - FirstLowInvalidHeapAttributeNumber, idx_bmps)) {
                    bms_free(idx_bmps);
                    return true;
                }
            }
            bms_free(idx_bmps);
        }
    }

    /* add root query to query stack list */
    context.queryNodes = lappend(context.queryNodes, qry);

    /* recursive walk on the query */
    (void)query_or_expression_tree_walker((Node*)qry, (bool (*)())MotIndexedColumnUpdateWalker, (void*)&context, 0);

    return context.isIndexedColumnUpdate;
}
#endif

static void CheckUnsupportInsertSelectClause(Query* query)
{
    RangeTblEntry* result = NULL;
    Query* subquery = NULL;

    if (query == NULL)
        return;
    result = rt_fetch(query->resultRelation, query->rtable);

    AssertEreport(query->commandType == CMD_INSERT, MOD_OPT, "Only deal with CMD_INSERT commondType here");
    if (result->relkind == RELKIND_FOREIGN_TABLE || result->relkind == RELKIND_STREAM) {
        if (CheckSupportedFDWType(result->relid)) {
            return;
        }
#ifdef ENABLE_MULTIPLE_NODES
         if (relid_is_stream(result->relid)) {
            if (t_thrd.streaming_cxt.loaded) {
                return;
            } else {
                ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Un-support feature"),
                        errdetail("Before insert stream object, please switch on the stream engine")));
            }
        }
#endif
        if (list_length(query->rtable) == 1)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Un-support feature"),
                    errdetail("insert statement is an INSERT INTO VALUES(...)")));

        if (list_length(query->rtable) > 2)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Un-support feature"),
                    errdetail("too complex subquery")));

        RangeTblEntry* subrte = rt_fetch(query->resultRelation == 1 ? 2 : 1, query->rtable);
        /* non-subquery is unsupported */
        if (subrte->rtekind != RTE_SUBQUERY)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Un-support feature"),
                    errdetail("only subquery is supported")));

        subquery = subrte->subquery;
        if (list_length(subquery->rtable) != 1)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Un-support feature"),
                    errdetail("subquery can not contain more than one relation")));

        if (rt_fetch(1, subquery->rtable)->relkind != RELKIND_RELATION)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Un-support feature"),
                    errdetail("range table of subquery should be a normal relation")));

        /* complex simple table query is not supported */
        if (subquery->distinctClause)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Un-support feature"),
                    errdetail("subquery contains DISTINCT clause")));

        if (subquery->hasAggs || subquery->groupClause)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Un-support feature"),
                    errdetail("subquery contains aggregation")));

        if (subquery->hasWindowFuncs)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Un-support feature"),
                    errdetail("subquery contains window function")));

        if (subquery->hasSubLinks)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Un-support feature"),
                    errdetail("subquery contains other subqueries")));

        if (subquery->limitCount || subquery->limitOffset)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Un-support feature"),
                    errdetail("subquery contains LIMIT clause")));

        if (subquery->sortClause)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Un-support feature"),
                    errdetail("subquery contains SORT clause")));
    } else if (list_length(query->rtable) == 1) {
        return;
    } else if ((subquery = FindQueryContainForeignTbl(query)) != NULL) {
        if (!IsFindHDFSForeignTbl(subquery) && !IsFindPgfdwForeignTbl(subquery)) {
            if (list_length(query->rtable) > 2) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Un-support feature"),
                        errdetail("too complex statement to foreign table")));
            }

            // check if SELECT clause contains JOIN foreign table
            if (list_length(subquery->rtable) > 1) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Un-support feature"),
                        errdetail("JOIN contains foreign table")));
            }

            RangeTblEntry* rte = rt_fetch(query->resultRelation == 1 ? 2 : 1, query->rtable);
            AssertEreport(rte->rtekind == RTE_SUBQUERY, MOD_OPT, "Only deal with subquery in FROM clause here");

            if (rte->subquery != subquery) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Un-support feature"),
                        errdetail("too complex statement to foreign table")));
            }

            // Unsupport LIMIT clause
            if (subquery->limitCount != NULL || subquery->limitOffset != NULL) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Un-support feature"),
                        errdetail("subquery contains LIMIT clause")));
            }
        }
    }
}

/*
 * transformInsertStmt -
 *	  transform an Insert Statement
 */
static Query* transformInsertStmt(ParseState* pstate, InsertStmt* stmt)
{
    Query* qry = makeNode(Query);
    SelectStmt* selectStmt = (SelectStmt*)stmt->selectStmt;
    List* exprList = NIL;
    bool isGeneralSelect = false;
    List* sub_rtable = NIL;
    List* sub_relnamespace = NIL;
    List* sub_varnamespace = NIL;
    List* icolumns = NIL;
    List* attrnos = NIL;
    RangeTblEntry* rte = NULL;
    RangeTblRef* rtr = NULL;
    ListCell* icols = NULL;
    ListCell* attnos = NULL;
    ListCell* lc = NULL;
    AclMode    targetPerms = ACL_INSERT;

    /* There can't be any outer WITH to worry about */
    AssertEreport(pstate->p_ctenamespace == NIL, MOD_OPT, "para should be NIL");

    qry->commandType = CMD_INSERT;
    pstate->p_is_insert = true;
    /* set io state for backend status for the thread, we will use it to check user space */
    pgstat_set_io_state(IOSTATE_WRITE);

    /*
     * Insert into relation pg_auth_history is not allowed.
     * We update it only when some user's password has been changed.
     */
    if (pg_strcasecmp(stmt->relation->relname, "pg_auth_history") == 0) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_OPERATION), errmsg("Not allowed to insert into relation pg_auth_history.")));
    }

    /* process the WITH clause independently of all else */
    if (stmt->withClause) {
        qry->hasRecursive = stmt->withClause->recursive;
        qry->cteList = transformWithClause(pstate, stmt->withClause);
        qry->hasModifyingCTE = pstate->p_hasModifyingCTE;
    }

    /*
     * We have three cases to deal with: DEFAULT VALUES (selectStmt == NULL),
     * VALUES list, or general SELECT input.  We special-case VALUES, both for
     * efficiency and so we can handle DEFAULT specifications.
     *
     * The grammar allows attaching ORDER BY, LIMIT, FOR UPDATE, or WITH to a
     * VALUES clause.  If we have any of those, treat it as a general SELECT;
     * so it will work, but you can't use DEFAULT items together with those.
     */
    isGeneralSelect = (selectStmt && (selectStmt->valuesLists == NIL || selectStmt->sortClause != NIL ||
                                         selectStmt->limitOffset != NULL || selectStmt->limitCount != NULL ||
                                         selectStmt->lockingClause != NIL || selectStmt->withClause != NULL));

    /*
     * If a non-nil rangetable/namespace was passed in, and we are doing
     * INSERT/SELECT, arrange to pass the rangetable/namespace down to the
     * SELECT.	This can only happen if we are inside a CREATE RULE, and in
     * that case we want the rule's OLD and NEW rtable entries to appear as
     * part of the SELECT's rtable, not as outer references for it.  (Kluge!)
     * The SELECT's joinlist is not affected however.  We must do this before
     * adding the target table to the INSERT's rtable.
     */
    if (isGeneralSelect) {
        sub_rtable = pstate->p_rtable;
        pstate->p_rtable = NIL;
        sub_relnamespace = pstate->p_relnamespace;
        pstate->p_relnamespace = NIL;
        sub_varnamespace = pstate->p_varnamespace;
        pstate->p_varnamespace = NIL;
    } else {
        sub_rtable = NIL; /* not used, but keep compiler quiet */
        sub_relnamespace = NIL;
        sub_varnamespace = NIL;
    }

    /*
     * Must get write lock on INSERT target table before scanning SELECT, else
     * we will grab the wrong kind of initial lock if the target table is also
     * mentioned in the SELECT part.  Note that the target table is not added
     * to the joinlist or namespace.
     */
    if (stmt->upsertClause != NULL && stmt->upsertClause->targetList != NIL) {
        targetPerms |= ACL_UPDATE;
    }

    qry->resultRelation = setTargetTable(pstate, stmt->relation, false, false, targetPerms);
    if (pstate->p_target_relation != NULL &&
        ((unsigned int)RelationGetInternalMask(pstate->p_target_relation) & INTERNAL_MASK_DINSERT)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Un-support feature"),
                errdetail("internal relation doesn't allow INSERT")));
    }
#ifdef ENABLE_MULTIPLE_NODES
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
#endif
        if (pstate->p_target_relation != NULL
                && RelationIsMatview(pstate->p_target_relation) && !stmt->isRewritten) {
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("Unsupported feature"),
                            errdetail("Materialized view doesn't allow INSERT")));
        }
#ifdef ENABLE_MULTIPLE_NODES
    }
#endif

    if (pstate->p_target_relation != NULL && stmt->upsertClause != NULL) {
        /* non-supported upsert cases */
        if (!u_sess->attr.attr_sql.enable_upsert_to_merge && RelationIsColumnFormat(pstate->p_target_relation)) {
            ereport(ERROR, ((errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                             errmsg("INSERT ON DUPLICATE KEY UPDATE is not supported on column orientated table."))));
        }

        if (RelationIsForeignTable(pstate->p_target_relation) || RelationIsStream(pstate->p_target_relation)) {
            ereport(ERROR, ((errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                             errmsg("INSERT ON DUPLICATE KEY UPDATE is not supported on foreign table."))));
        }

        if (RelationIsView(pstate->p_target_relation)) {
            ereport(ERROR, ((errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("INSERT ON DUPLICATE KEY UPDATE is not supported on VIEW."))));
        }

        if (RelationIsContquery(pstate->p_target_relation)) {
            ereport(ERROR, ((errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("INSERT ON DUPLICATE KEY UPDATE is not supported on CONTQUERY."))));
        }
    }

    /* data redistribution for DFS table.
     * check if the target relation is being redistributed(insert mode).
     * For example, when table A is redistributing in session 1, operating table
     * A with insert command in session 2, the table A can not be inserted data in session 2.
     *
     * We don't allow insert during online expansion when expansion is set as read only.
     * We reply on gs_redis to ganurantee DFS table to be read only during online expansion
     * so we don't need to double check if target table is DFS table here anymore.
     */
    if (!u_sess->attr.attr_sql.enable_cluster_resize && pstate->p_target_relation != NULL &&
        RelationInClusterResizingWriteErrorMode(pstate->p_target_relation)) {
        ereport(ERROR,
            (errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
                errmsg("%s is redistributing, please retry later.", pstate->p_target_relation->rd_rel->relname.data)));
    }

    /* Validate stmt->cols list, or build default list if no list given */
    icolumns = checkInsertTargets(pstate, stmt->cols, &attrnos);
    AssertEreport(list_length(icolumns) == list_length(attrnos), MOD_OPT, "list length inconsistent");

    /*
     * Determine which variant of INSERT we have.
     */
    if (selectStmt == NULL) {
        /*
         * We have INSERT ... DEFAULT VALUES.  We can handle this case by
         * emitting an empty targetlist --- all columns will be defaulted when
         * the planner expands the targetlist.
         */
        exprList = NIL;
    } else if (isGeneralSelect) {
        /*
         * We make the sub-pstate a child of the outer pstate so that it can
         * see any Param definitions supplied from above.  Since the outer
         * pstate's rtable and namespace are presently empty, there are no
         * side-effects of exposing names the sub-SELECT shouldn't be able to
         * see.
         */
        ParseState* sub_pstate = make_parsestate(pstate);
        Query* selectQuery = NULL;
#ifdef PGXC
        RangeTblEntry* target_rte = NULL;
#endif

        /*
         * Process the source SELECT.
         *
         * It is important that this be handled just like a standalone SELECT;
         * otherwise the behavior of SELECT within INSERT might be different
         * from a stand-alone SELECT. (Indeed, Postgres up through 6.5 had
         * bugs of just that nature...)
         */
        sub_pstate->p_rtable = sub_rtable;
        sub_pstate->p_joinexprs = NIL; /* sub_rtable has no joins */
        sub_pstate->p_relnamespace = sub_relnamespace;
        sub_pstate->p_varnamespace = sub_varnamespace;
        /* Set sub_query is in insert */
        sub_pstate->p_is_in_insert = true;
        sub_pstate->p_resolve_unknowns = false;

        selectQuery = transformStmt(sub_pstate, stmt->selectStmt);
        Assert(selectQuery != NULL);

        free_parsestate(sub_pstate);

        /* The grammar should have produced a SELECT */
        if (!IsA(selectQuery, Query) || selectQuery->commandType != CMD_SELECT || selectQuery->utilityStmt != NULL) {
            ereport(
                ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("unexpected non-SELECT command in INSERT ... SELECT")));
        }

        /*
         * Make the source be a subquery in the INSERT's rangetable, and add
         * it to the INSERT's joinlist.
         */
        rte = addRangeTableEntryForSubquery(pstate, selectQuery, makeAlias("*SELECT*", NIL), false, false);
#ifdef PGXC
        /*
         * For an INSERT SELECT involving INSERT on a child after scanning
         * the parent, set flag to send command ID communication to remote
         * nodes in order to maintain global data visibility.
         */
        if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
            target_rte = rt_fetch(qry->resultRelation, pstate->p_rtable);
            if (is_relation_child(target_rte, selectQuery->rtable)) {
                qry->has_to_save_cmd_id = true;
                SetSendCommandId(true);
            }
        }
#endif
        rtr = makeNode(RangeTblRef);
        /* assume new rte is at end */
        rtr->rtindex = list_length(pstate->p_rtable);
        AssertEreport(
            rte == rt_fetch(rtr->rtindex, pstate->p_rtable), MOD_OPT, "check inconsistant with rt_fetch functon");
        pstate->p_joinlist = lappend(pstate->p_joinlist, rtr);

        /* ----------
         * Generate an expression list for the INSERT that selects all the
         * non-resjunk columns from the subquery.  (INSERT's tlist must be
         * separate from the subquery's tlist because we may add columns,
         * insert datatype coercions, etc.)
         *
         * HACK: unknown-type constants and params in the SELECT's targetlist
         * are copied up as-is rather than being referenced as subquery
         * outputs.  This is to ensure that when we try to coerce them to
         * the target column's datatype, the right things happen (see
         * special cases in coerce_type).  Otherwise, this fails:
         *		INSERT INTO foo SELECT 'bar', ... FROM baz
         * ----------
         */
        int subpos = 0;
        int respos = 0;
        Bitmapset* subset = NULL;
        Bitmapset* resset = NULL;

        exprList = NIL;
        foreach (lc, selectQuery->targetList) {
            TargetEntry* tle = (TargetEntry*)lfirst(lc);
            Expr* expr = NULL;

            subpos++;
            if (tle->resjunk) {
                continue;
            }

            respos++;
            if (tle->expr && (IsA(tle->expr, Const) || IsA(tle->expr, Param)) &&
                exprType((Node*)tle->expr) == UNKNOWNOID) {
                subset = bms_add_member(subset, subpos);
                resset = bms_add_member(resset, respos);
                expr = tle->expr;
            } else {
                Var* var = makeVarFromTargetEntry(rtr->rtindex, tle);

                var->location = exprLocation((Node*)tle->expr);
                expr = (Expr*)var;
            }
            exprList = lappend(exprList, expr);
        }

        /*
         * If td_compatible_truncation equal true and no foreign table found,
         * the auto truncation funciton should be enabled.
         */
        if (u_sess->attr.attr_sql.sql_compatibility == C_FORMAT && pstate->p_target_relation != NULL &&
            !RelationIsForeignTable(pstate->p_target_relation) &&
            !RelationIsStream(pstate->p_target_relation)) {
            if (u_sess->attr.attr_sql.td_compatible_truncation) {
                pstate->p_is_td_compatible_truncation = true;
            } else {
                pstate->tdTruncCastStatus = NOT_CAST_BECAUSEOF_GUC;
            }
        }

        /* Prepare row for assignment to target table */
        exprList = transformInsertRow(pstate, exprList, stmt->cols, icolumns, attrnos);

        if (!bms_is_empty(subset)) {
            /*
             * Try to coerce unknown-type constants and params to the target
             * column's datatype.
             */
            Assert(!bms_is_empty(resset));

            while ((subpos = bms_first_member(subset)) > 0) {
                TargetEntry* tle = (TargetEntry*)list_nth(selectQuery->targetList, subpos - 1);

                respos = bms_first_member(resset);
                Assert(respos > 0);

                ListCell* rescell = list_nth_cell(exprList, respos - 1);
                tle->expr = (Expr*)copyObject(lfirst(rescell));

                /*
                 * After coercion, we need to transform those constants from
                 * the HACKY representation to a normal Var node like others.
                 *
                 * Old exprs in exprList are replaced by the newly
                 * generated Vars and then freed on each iteration.
                 */
                if ((IsA(tle->expr, Const) || IsA(tle->expr, Param)) &&
                    exprType((Node*)tle->expr) != UNKNOWNOID) {
                    lfirst(rescell) = (void*)makeVarFromTargetEntry(rtr->rtindex, tle);
                }
            }
        }
    } else if (list_length(selectStmt->valuesLists) > 1) {
        /*
         * Process INSERT ... VALUES with multiple VALUES sublists. We
         * generate a VALUES RTE holding the transformed expression lists, and
         * build up a targetlist containing Vars that reference the VALUES
         * RTE.
         */
        List* exprsLists = NIL;
        List* collations = NIL;
        int sublist_length = -1;
        int i;
        List* temp_list = NIL;

        AssertEreport(selectStmt->intoClause == NULL, MOD_OPT, "into Clause should not happen here");
        /*
         * Here, because the auto truncation feature need to be blocked,
         * once we found there is a foreign table involve. so here we change
         * orignal one foreach loop to two foreach loops.
         * first loop only transformExpressionList for all value lists and
         * save temp result to temp_list. Here we also generate the result whether
         * auto truncation is enable or not.
         * Seconde loop will continues process the temp_list that first loop generated.
         */
        foreach (lc, selectStmt->valuesLists) {
            List* sublist = (List*)lfirst(lc);

            /* Do basic expression transformation (same as a ROW() expr) */
            sublist = transformExpressionList(pstate, sublist);

            /*
             * All the sublists must be the same length, *after*
             * transformation (which might expand '*' into multiple items).
             * The VALUES RTE can't handle anything different.
             */
            if (sublist_length < 0) {
                /* Remember post-transformation length of first sublist */
                sublist_length = list_length(sublist);
            } else if (sublist_length != list_length(sublist)) {
                ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("VALUES lists must all be the same length"),
                        parser_errposition(pstate, exprLocation((Node*)sublist))));
            }

            /*
             * If td_compatible_truncation equal true and no foreign table found,
             * the auto truncation funciton should be enabled.
             */
            if (u_sess->attr.attr_sql.sql_compatibility == C_FORMAT && pstate->p_target_relation != NULL &&
                !RelationIsForeignTable(pstate->p_target_relation) &&
                !RelationIsStream(pstate->p_target_relation)) {
                if (u_sess->attr.attr_sql.td_compatible_truncation) {
                    pstate->p_is_td_compatible_truncation = true;
                } else {
                    pstate->tdTruncCastStatus = NOT_CAST_BECAUSEOF_GUC;
                }
            }

            temp_list = lappend(temp_list, sublist);
        }

        foreach (lc, temp_list) {
            List* sublist = (List*)lfirst(lc);

            /* Prepare row for assignment to target table */
            sublist = transformInsertRow(pstate, sublist, stmt->cols, icolumns, attrnos);

            /*
             * We must assign collations now because assign_query_collations
             * doesn't process rangetable entries.  We just assign all the
             * collations independently in each row, and don't worry about
             * whether they are consistent vertically.	The outer INSERT query
             * isn't going to care about the collations of the VALUES columns,
             * so it's not worth the effort to identify a common collation for
             * each one here.  (But note this does have one user-visible
             * consequence: INSERT ... VALUES won't complain about conflicting
             * explicit COLLATEs in a column, whereas the same VALUES
             * construct in another context would complain.)
             */
            assign_list_collations(pstate, sublist);

            exprsLists = lappend(exprsLists, sublist);
        }

        /*
         * Although we don't really need collation info, let's just make sure
         * we provide a correctly-sized list in the VALUES RTE.
         */
        for (i = 0; i < sublist_length; i++) {
            collations = lappend_oid(collations, InvalidOid);
        }

        if (list_length(pstate->p_rtable) != 1 && contain_vars_of_level((Node*)exprsLists, 0)) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("VALUES must not contain OLD or NEW references"),
                    errhint("Use SELECT ... UNION ALL ... instead."),
                    parser_errposition(pstate, locate_var_of_level((Node*)exprsLists, 0))));
        }

        /*
         * Generate the VALUES RTE
         */
        rte = addRangeTableEntryForValues(pstate, exprsLists, collations, NULL, true);
        rtr = makeNode(RangeTblRef);
        /* assume new rte is at end */
        rtr->rtindex = list_length(pstate->p_rtable);
        AssertEreport(
            rte == rt_fetch(rtr->rtindex, pstate->p_rtable), MOD_OPT, "check inconsistant with rt_fetch functon");
        pstate->p_joinlist = lappend(pstate->p_joinlist, rtr);

        /*
         * Generate list of Vars referencing the RTE
         */
        expandRTE(rte, rtr->rtindex, 0, -1, false, NULL, &exprList);
    } else {
        /* ----------
         * Process INSERT ... VALUES with a single VALUES sublist.
         * We treat this separately for efficiency and for historical
         * compatibility --- specifically, allowing table references,
         * such as
         *			INSERT INTO foo VALUES(bar.*)
         *
         * The sublist is just computed directly as the Query's targetlist,
         * with no VALUES RTE.	So it works just like SELECT without FROM.
         * ----------
         */
        List* valuesLists = selectStmt->valuesLists;

        AssertEreport(list_length(valuesLists) == 1, MOD_OPT, "list length is wrong");
        AssertEreport(selectStmt->intoClause == NULL, MOD_OPT, "intoClause should not happen here");

        /* Do basic expression transformation (same as a ROW() expr) */
        exprList = transformExpressionList(pstate, (List*)linitial(valuesLists));

        /*
         * If td_compatible_truncation equal true and no foreign table found,
         * the auto truncation funciton should be enabled.
         */
        if (u_sess->attr.attr_sql.sql_compatibility == C_FORMAT && pstate->p_target_relation != NULL &&
            !RelationIsForeignTable(pstate->p_target_relation) &&
            !RelationIsStream(pstate->p_target_relation)) {
            if (u_sess->attr.attr_sql.td_compatible_truncation) {
                pstate->p_is_td_compatible_truncation = true;
            } else {
                pstate->tdTruncCastStatus = NOT_CAST_BECAUSEOF_GUC;
            }
        }

        /* Prepare row for assignment to target table */
        exprList = transformInsertRow(pstate, exprList, stmt->cols, icolumns, attrnos);
    }

    /*
     * Generate query's target list using the computed list of expressions.
     * Also, mark all the target columns as needing insert permissions.
     */
    rte = pstate->p_target_rangetblentry;
    qry->targetList = NIL;
    icols = list_head(icolumns);
    attnos = list_head(attrnos);
    foreach (lc, exprList) {
        Expr* expr = (Expr*)lfirst(lc);
        ResTarget* col = NULL;
        AttrNumber attr_num;
        TargetEntry* tle = NULL;

        col = (ResTarget*)lfirst(icols);
        AssertEreport(IsA(col, ResTarget), MOD_OPT, "nodeType inconsistant");
        attr_num = (AttrNumber)lfirst_int(attnos);

        tle = makeTargetEntry(expr, attr_num, col->name, false);
        qry->targetList = lappend(qry->targetList, tle);

        rte->insertedCols = bms_add_member(rte->insertedCols, attr_num - FirstLowInvalidHeapAttributeNumber);

        icols = lnext(icols);
        attnos = lnext(attnos);
    }

    /* Process DUPLICATE KEY UPDATE, if any. */
    if (stmt->upsertClause) {
        qry->upsertClause = transformUpsertClause(pstate, stmt->upsertClause, stmt->relation);
    }
    /*
     * If we have a RETURNING clause, we need to add the target relation to
     * the query namespace before processing it, so that Var references in
     * RETURNING will work.  Also, remove any namespace entries added in a
     * sub-SELECT or VALUES list.
     */
    if (stmt->returningList) {
        pstate->p_relnamespace = NIL;
        pstate->p_varnamespace = NIL;
        addRTEtoQuery(pstate, pstate->p_target_rangetblentry, false, true, true);
        qry->returningList = transformReturningList(pstate, stmt->returningList);
    }

    /* done building the range table and jointree */
    qry->rtable = pstate->p_rtable;
    qry->jointree = makeFromExpr(pstate->p_joinlist, NULL);

    qry->hasSubLinks = pstate->p_hasSubLinks;
    /* aggregates not allowed (but subselects are okay) */
    if (pstate->p_hasAggs) {
        ereport(ERROR,
            (errcode(ERRCODE_GROUPING_ERROR),
                errmsg("cannot use aggregate function in VALUES"),
                parser_errposition(pstate, locate_agg_of_level((Node*)qry, 0))));
    }
    if (pstate->p_hasWindowFuncs) {
        ereport(ERROR,
            (errcode(ERRCODE_WINDOWING_ERROR),
                errmsg("cannot use window function in VALUES"),
                parser_errposition(pstate, locate_windowfunc((Node*)qry))));
    }

    assign_query_collations(pstate, qry);

    CheckUnsupportInsertSelectClause(qry);

    /* Set query tdTruncCastStatus to recored if auto truncation enabled or not. */
    qry->tdTruncCastStatus = pstate->tdTruncCastStatus;
    qry->hintState = stmt->hintState;

    return qry;
}

static void checkUpsertTargetlist(Relation targetTable, List* updateTlist)
{
    List* index_list = RelationGetIndexInfoList(targetTable);
    if (check_unique_constraint(index_list)) {
        ListCell* target = NULL;
        ListCell* index = NULL;
        IndexInfo* index_info = NULL;
        Bitmapset* target_attrs = NULL; /* attr bitmap according to targetlist */
        Bitmapset* index_attrs = NULL; /* attr bitmap according to index */
        TargetEntry* tle = NULL;

        foreach (target, updateTlist) {
            tle = (TargetEntry*)lfirst(target);
            target_attrs = bms_add_member(target_attrs, tle->resno);
        }

        foreach (index, index_list) {
            index_info = (IndexInfo*)lfirst(index);
            for (int i = 0; i < index_info->ii_NumIndexAttrs; i++) {
                int attrno = index_info->ii_KeyAttrNumbers[i];
                if (attrno > 0) {
                    index_attrs = bms_add_member(index_attrs, attrno);
                }
            }
        }

        if (bms_overlap(index_attrs, target_attrs)) {
            ereport(ERROR, ((errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                         errmsg("INSERT ON DUPLICATE KEY UPDATE don't allow update on primary key or unique key."))));
        }
    }
}

List* BuildExcludedTargetlist(Relation targetrel, Index exclRelIndex)
{
    List* result = NIL;
    int attno;
    Var* var = NULL;
    TargetEntry* te = NULL;

    /*
     * Note that resnos of the tlist must correspond to attnos of the
     * underlying relation, hence we need entries for dropped columns too.
     */
    for (attno = 0; attno < RelationGetNumberOfAttributes(targetrel); attno++) {
        Form_pg_attribute attr = targetrel->rd_att->attrs[attno];
        char* name = NULL;

        if (attr->attisdropped) {
            /*
             * can't use atttypid here, but it doesn't really matter what type
             * the Const claims to be.
             */
            var = (Var*)makeNullConst(INT4OID, -1, InvalidOid);
        } else {
            var = makeVar(exclRelIndex, attno + 1, attr->atttypid, attr->atttypmod,
                          attr->attcollation, 0);
            name = pstrdup(NameStr(attr->attname));
        }

        te = makeTargetEntry((Expr*)var, attno + 1, name, false);

        result = lappend(result, te);
    }

    /*
     * Add a whole-row-Var entry to support references to "EXCLUDED.*".  Like
     * the other entries in the EXCLUDED tlist, its resno must match the Var's
     * varattno, else the wrong things happen while resolving references in
     * setrefs.c.  This is against normal conventions for targetlists, but
     * it's okay since we don't use this as a real tlist.
     */
    var = makeVar(exclRelIndex, InvalidAttrNumber, targetrel->rd_rel->reltype,
                  -1, InvalidOid, 0);
    te = makeTargetEntry((Expr*)var, InvalidAttrNumber, NULL, true);
    result = lappend(result, te);

    return result;
}

static bool CheckRlsPolicyForUpsert(Relation targetrel)
{
    ListCell *item = NULL;
    RlsPolicy *policy = NULL;
    List *relRlsPolicies = targetrel->rd_rlsdesc->rlsPolicies;
    foreach (item, relRlsPolicies) {
        policy = (RlsPolicy *)lfirst(item);
        if (policy->cmdName == ACL_UPDATE_CHR || policy->cmdName == RLS_CMD_ALL_CHR) {
            return true;
        }
    }
    return false;
}

/*
 * The following check only affect distributed deployment.
 * Sublink in upsert's where clause is supported for centralized mode.
 */
#ifdef ENABLE_MULTIPLE_NODES
static bool ContainSubLinkWalker(Node* node, void* context)
{
    if (node == NULL) {
        return false;
    }

    if (IsA(node, SubLink)) {
        return true;
    }

    return expression_tree_walker(node, (bool (*)())ContainSubLinkWalker, (void*)context);
}

static bool ContainSubLink(Node* clause)
{
    return ContainSubLinkWalker(clause, NULL);
}
#endif /* ENABLE_MULTIPLE_NODES */

static UpsertExpr* transformUpsertClause(ParseState* pstate, UpsertClause* upsertClause, RangeVar* relation)
{
    UpsertExpr* result = NULL;
    List* updateTlist = NIL;
    RangeTblEntry* exclRte = NULL;
    int exclRelIndex = 0;
    List* exclRelTlist = NIL;
    Node* updateWhere = NULL;
    UpsertAction action = UPSERT_NOTHING;
    Relation targetrel = pstate->p_target_relation;

#ifdef ENABLE_MULTIPLE_NODES
    if (targetrel->rd_rel->relhastriggers) {
        ereport(WARNING,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("triggers are not supported and will be ignored by INSERT ON DUPLICATE KEY UPDATE.")));
    }
#endif

    if (RelationHasRlspolicy(targetrel->rd_id) && CheckRlsPolicyForUpsert(targetrel)) {
        ereport(ERROR,
            (errmodule(MOD_SEC_POLICY),
                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Row level security policy is not supported by INSERT ON DUPLICATE KEY UPDATE."),
                errdetail("Table %s with row level security policy.", RelationGetRelationName(targetrel)),
                errcause("Target table with row level security policy."),
                erraction("Shutdown row level security policy, Use INSERT and UPDATE instead INSERT ON DUPLICATE KEY "
                          "UPDATE.")));
    }

    if (upsertClause->targetList != NIL) {
        pstate->p_is_insert = false;
        action = UPSERT_UPDATE;
        exclRte = addRangeTableEntryForRelation(pstate, targetrel, makeAlias("excluded", NIL), false, false);
        exclRte->isexcluded = true;
        exclRelIndex = list_length(pstate->p_rtable);

        /*
         * Build a targetlist for the EXCLUDED pseudo relation. Out of
         * simplicity we do that here, because expandRelAttrs() happens to
         * nearly do the right thing; specifically it also works with views.
         * It'd be more proper to instead scan some pseudo scan node, but it
         * doesn't seem worth the amount of code required.
         *
         * The only caveat of this hack is that the permissions expandRelAttrs
         * adds have to be reset. markVarForSelectPriv() will add the exact
         * required permissions back.
         */

        exclRelTlist = BuildExcludedTargetlist(targetrel, exclRelIndex);
        exclRte->requiredPerms = 0;
        exclRte->selectedCols = NULL;

        /*
         * Add EXCLUDED and the target RTE to the namespace, so that they can
         * be used in the UPDATE statement.
         */
        addRTEtoQuery(pstate, exclRte, false, true, true);
        addRTEtoQuery(pstate, pstate->p_target_rangetblentry, false, true, true);

        updateTlist = transformTargetList(pstate, upsertClause->targetList);
        /* Done with select-like processing, move on transforming to match update set target column */
        updateTlist = transformUpdateTargetList(pstate, updateTlist, upsertClause->targetList, relation);
        updateWhere = transformWhereClause(pstate, upsertClause->whereClause, "WHERE");
#ifdef ENABLE_MULTIPLE_NODES
        /* Do not support sublinks in update where clause for now */
        if (ContainSubLink(updateWhere)) {
            ereport(ERROR,
                (errmodule(MOD_OPT_PLANNER), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Feature is not supported for INSERT ON DUPLICATE KEY UPDATE."),
                    errdetail("Do not support sublink in where clause."),
                    errcause("Unsupported syntax."),
                    erraction("Check if the query can be rewritten into MERGE INTO statement "
                              "or reduce the sublink in where clause.")));
        }
#endif /* ENABLE_MULTIPLE_NODES */
        /* We can't update primary or unique key in upsert, check it here */
#ifdef ENABLE_MULTIPLE_NODES
        if (IS_PGXC_COORDINATOR && !u_sess->attr.attr_sql.enable_upsert_to_merge) {
            checkUpsertTargetlist(pstate->p_target_relation, updateTlist);
        }
#else
        checkUpsertTargetlist(pstate->p_target_relation, updateTlist);
#endif
    }

    /* Finally, build DUPLICATE KEY UPDATE [NOTHING | ... ] expression */
    result = makeNode(UpsertExpr);
    result->updateTlist = updateTlist;
    result->exclRelIndex = exclRelIndex;
    result->exclRelTlist = exclRelTlist;
    result->upsertAction = action;
    result->upsertWhere = updateWhere;
    return result;
}

/*
 * Prepare an INSERT row for assignment to the target table.
 *
 * The row might be either a VALUES row, or variables referencing a
 * sub-SELECT output.
 */
List* transformInsertRow(ParseState* pstate, List* exprlist, List* stmtcols, List* icolumns, List* attrnos)
{
    List* result = NIL;
    ListCell* lc = NULL;
    ListCell* icols = NULL;
    ListCell* attnos = NULL;

    /*
     * Check length of expr list.  It must not have more expressions than
     * there are target columns.  We allow fewer, but only if no explicit
     * columns list was given (the remaining columns are implicitly
     * defaulted).	Note we must check this *after* transformation because
     * that could expand '*' into multiple items.
     */
    if (list_length(exprlist) > list_length(icolumns)) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("INSERT has more expressions than target columns"),
                parser_errposition(pstate, exprLocation((Node*)list_nth(exprlist, list_length(icolumns))))));
    }
    if (stmtcols != NIL && list_length(exprlist) < list_length(icolumns)) {
        /*
         * We can get here for cases like INSERT ... SELECT (a,b,c) FROM ...
         * where the user accidentally created a RowExpr instead of separate
         * columns.  Add a suitable hint if that seems to be the problem,
         * because the main error message is quite misleading for this case.
         * (If there's no stmtcols, you'll get something about data type
         * mismatch, which is less misleading so we don't worry about giving a
         * hint in that case.)
         */
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("INSERT has more target columns than expressions"),
                ((list_length(exprlist) == 1 &&
                     count_rowexpr_columns(pstate, (Node*)linitial(exprlist)) == list_length(icolumns))
                        ? errhint("The insertion source is a row expression containing the same number of columns "
                                  "expected by the INSERT. Did you accidentally use extra parentheses?")
                        : 0),
                parser_errposition(pstate, exprLocation((Node*)list_nth(icolumns, list_length(exprlist))))));
    }

    /*
     * Prepare columns for assignment to target table.
     */
    result = NIL;
    icols = list_head(icolumns);
    attnos = list_head(attrnos);
    foreach (lc, exprlist) {
        Expr* expr = (Expr*)lfirst(lc);
        ResTarget* col = NULL;
#ifndef ENABLE_MULTIPLE_NODES		
        /*
         * Rownum is not allowed in exprlist in INSERT statement.
         */
        ExcludeRownumExpr(pstate, (Node*)expr);
#endif
        col = (ResTarget*)lfirst(icols);
        AssertEreport(IsA(col, ResTarget), MOD_OPT, "nodeType inconsistant");

        expr = transformAssignedExpr(pstate, expr, col->name, lfirst_int(attnos), col->indirection, col->location);

        result = lappend(result, expr);

        icols = lnext(icols);
        attnos = lnext(attnos);
    }

    return result;
}

/*
 * count_rowexpr_columns -
 *	  get number of columns contained in a ROW() expression;
 *	  return -1 if expression isn't a RowExpr or a Var referencing one.
 *
 * This is currently used only for hint purposes, so we aren't terribly
 * tense about recognizing all possible cases.	The Var case is interesting
 * because that's what we'll get in the INSERT ... SELECT (...) case.
 */
static int count_rowexpr_columns(ParseState* pstate, Node* expr)
{
    if (expr == NULL) {
        return -1;
    }
    if (IsA(expr, RowExpr)) {
        return list_length(((RowExpr*)expr)->args);
    }
    if (IsA(expr, Var)) {
        Var* var = (Var*)expr;
        AttrNumber attnum = var->varattno;

        if (attnum > 0 && var->vartype == RECORDOID) {
            RangeTblEntry* rte = NULL;

            rte = GetRTEByRangeTablePosn(pstate, var->varno, var->varlevelsup);
            if (rte->rtekind == RTE_SUBQUERY) {
                /* Subselect-in-FROM: examine sub-select's output expr */
                TargetEntry* ste = get_tle_by_resno(rte->subquery->targetList, attnum);

                if (ste == NULL || ste->resjunk) {
                    return -1;
                }
                expr = (Node*)ste->expr;
                if (IsA(expr, RowExpr)) {
                    return list_length(((RowExpr*)expr)->args);
                }
            }
        }
    }
    return -1;
}

static char* fetchSelectStmtFromCTAS(char* source_text)
{
    char* select_pos = strcasestr(source_text, "SELECT");
    return select_pos;
}

static bool shouldTransformStartWithStmt(ParseState* pstate, SelectStmt* stmt, Query* selectQuery)
{
    /*
     * Only applicable to START WITH CONNECT BY clauses.
     */
    if (stmt->startWithClause == NULL || pstate->p_sourcetext == NULL) {
        return false;
    }

    /*
     * Back up the current select statement to be restored after query re-writing
     * for cases of START WITH CONNNECT BY under CREATE TABLE AS.
     */
    selectQuery->sql_statement = fetchSelectStmtFromCTAS((char*)pstate->p_sourcetext);

    return true;
}

/*
 * transformSelectStmt -
 *	  transforms a Select Statement
 *
 * Note: this covers only cases with no set operations and no VALUES lists;
 * see below for the other cases.
 */
static Query* transformSelectStmt(ParseState* pstate, SelectStmt* stmt, bool isFirstNode, bool isCreateView)
{
    Query* qry = makeNode(Query);
    Node* qual = NULL;
    ListCell* l = NULL;

    qry->commandType = CMD_SELECT;

    if (stmt->startWithClause != NULL) {
        pstate->p_addStartInfo = true;
        pstate->p_sw_selectstmt = stmt;
        pstate->origin_with = (WithClause *)copyObject(stmt->withClause);
    }

    /* process the WITH clause independently of all else */
    if (stmt->withClause) {
        qry->hasRecursive = stmt->withClause->recursive;
        qry->cteList = transformWithClause(pstate, stmt->withClause);
        qry->hasModifyingCTE = pstate->p_hasModifyingCTE;
    }

    /* Complain if we get called from someplace where INTO is not allowed */
    if (stmt->intoClause) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("SELECT ... INTO is not allowed here"),
                parser_errposition(pstate, exprLocation((Node*)stmt->intoClause))));
    }

    /* make FOR UPDATE/FOR SHARE info available to addRangeTableEntry */
    pstate->p_locking_clause = stmt->lockingClause;

    /* make WINDOW info available for window functions, too */
    pstate->p_windowdefs = stmt->windowClause;

    /* process the FROM clause */
    transformFromClause(pstate, stmt->fromClause, isFirstNode, isCreateView);

    /* transform START WITH...CONNECT BY clause */
    if (shouldTransformStartWithStmt(pstate, stmt, qry)) {
        transformStartWith(pstate, stmt, qry);
    }

    /* transform targetlist */
    qry->targetList = transformTargetList(pstate, stmt->targetList);

    /* Transform operator "(+)" to outer join */
    if (stmt->hasPlus && stmt->whereClause != NULL) {
        transformOperatorPlus(pstate, &stmt->whereClause);
    }

    qry->starStart = list_copy(pstate->p_star_start);
    qry->starEnd = list_copy(pstate->p_star_end);
    qry->starOnly = list_copy(pstate->p_star_only);

    /* mark column origins */
    markTargetListOrigins(pstate, qry->targetList);

    /* transform WHERE
     * Only "(+)" is valid when  it's in WhereClause of Select, set the flag to be trure
     * during transform Whereclause.
     */
    setIgnorePlusFlag(pstate, true);
    qual = transformWhereClause(pstate, stmt->whereClause, "WHERE");
    setIgnorePlusFlag(pstate, false);

    /*
     * Initial processing of HAVING clause is just like WHERE clause.
     */
    qry->havingQual = transformWhereClause(pstate, stmt->havingClause, "HAVING");

    /*
     * Transform sorting/grouping stuff.  Do ORDER BY first because both
     * transformGroupClause and transformDistinctClause need the results. Note
     * that these functions can also change the targetList, so it's passed to
     * them by reference.
     */
    qry->sortClause = transformSortClause(
        pstate, stmt->sortClause, &qry->targetList, true /* fix unknowns */, false /* allow SQL92 rules */);

    /*
     * Transform A_const to columnref type in group by clause, So that repeated group column
     * will deleted in function transformGroupClause. If not to delete repeated column, for
     * group by rollup can have error result, because we need set null to non- group column.
     *
     * select a, b, b
     *	from t1
     *	group by rollup(1, 2), 3;
     *
     * To this example, column b should not be set to null, but if not to delete repeated column
     * b will be set to null and two b value is not equal.
     */
    if (include_groupingset((Node*)stmt->groupClause)) {
        transformGroupConstToColumn(pstate, (Node*)stmt->groupClause, qry->targetList);
    }

    qry->groupClause = transformGroupClause(pstate,
        stmt->groupClause,
        &qry->groupingSets,
        &qry->targetList,
        qry->sortClause,
        false /* allow SQL92 rules */);

    if (stmt->distinctClause == NIL) {
        qry->distinctClause = NIL;
        qry->hasDistinctOn = false;
    } else if (linitial(stmt->distinctClause) == NULL) {
        /* We had SELECT DISTINCT */
        qry->distinctClause = transformDistinctClause(pstate, &qry->targetList, qry->sortClause, false);
        qry->hasDistinctOn = false;
    } else {
        /* We had SELECT DISTINCT ON */
        qry->distinctClause =
            transformDistinctOnClause(pstate, stmt->distinctClause, &qry->targetList, qry->sortClause);
        qry->hasDistinctOn = true;
    }

    /* transform LIMIT */
    qry->limitOffset = transformLimitClause(pstate, stmt->limitOffset, "OFFSET");
    qry->limitCount = transformLimitClause(pstate, stmt->limitCount, "LIMIT");

    /* transform window clauses after we have seen all window functions */
    qry->windowClause = transformWindowDefinitions(pstate, pstate->p_windowdefs, &qry->targetList);

    /* resolve any still-unresolved output columns as being type text */
    if (pstate->p_resolve_unknowns) {
        resolveTargetListUnknowns(pstate, qry->targetList);
    }

    qry->rtable = pstate->p_rtable;
    qry->jointree = makeFromExpr(pstate->p_joinlist, qual);

    qry->hasSubLinks = pstate->p_hasSubLinks;
    qry->hasWindowFuncs = pstate->p_hasWindowFuncs;
    if (pstate->p_hasWindowFuncs) {
        parseCheckWindowFuncs(pstate, qry);
    }
    qry->hasAggs = pstate->p_hasAggs;

    foreach (l, stmt->lockingClause) {
        transformLockingClause(pstate, qry, (LockingClause*)lfirst(l), false);
    }

    qry->hintState = stmt->hintState;

    /*
     * If query is under one insert statement and include a foreign table,
     * then set top level parsestate p_is_foreignTbl_exist to true.
     */
    if (u_sess->attr.attr_sql.td_compatible_truncation && u_sess->attr.attr_sql.sql_compatibility == C_FORMAT &&
        pstate->p_is_in_insert && checkForeignTableExist(pstate->p_rtable))
        set_ancestor_ps_contain_foreigntbl(pstate);

    assign_query_collations(pstate, qry);

    /* this must be done after collations, for reliable comparison of exprs */
    if (pstate->p_hasAggs || qry->groupClause || qry->groupingSets || qry->havingQual) {
        parseCheckAggregates(pstate, qry);
    }

    /*
     * If SelectStmt has been rewrite by startwith/connectby, it should
     * return as With-Recursive for upper level. So we need to fix fromClause.
     */
    if (pstate->p_addStartInfo) {
        AdaptSWSelectStmt(pstate, stmt);
    }

    return qry;
}

/*
 * transformValuesClause -
 *	  transforms a VALUES clause that's being used as a standalone SELECT
 *
 * We build a Query containing a VALUES RTE, rather as if one had written
 *			SELECT * FROM (VALUES ...) AS "*VALUES*"
 */
static Query* transformValuesClause(ParseState* pstate, SelectStmt* stmt)
{
    Query* qry = makeNode(Query);
    List* exprsLists = NIL;
    List* collations = NIL;
    List** colexprs = NULL;
    int sublist_length = -1;
    RangeTblEntry* rte = NULL;
    int rtindex;
    ListCell* lc = NULL;
    ListCell* lc2 = NULL;
    int i;

    qry->commandType = CMD_SELECT;

    /* Most SELECT stuff doesn't apply in a VALUES clause */
    AssertEreport(stmt->distinctClause == NIL, MOD_OPT, "distinctClause should not happen here");

    AssertEreport(stmt->intoClause == NULL, MOD_OPT, "intoClause should not happen here");

    AssertEreport(stmt->targetList == NIL, MOD_OPT, "targetList only accept NIL here");

    AssertEreport(stmt->fromClause == NIL, MOD_OPT, "fromClause should not happen here");

    AssertEreport(stmt->whereClause == NULL, MOD_OPT, "whereClause should not happen here");

    AssertEreport(stmt->groupClause == NIL, MOD_OPT, "groupClause should not happen here");

    AssertEreport(stmt->havingClause == NULL, MOD_OPT, "havingClause should not happen here");

    AssertEreport(stmt->windowClause == NIL, MOD_OPT, "windowClause should not happen here");

    AssertEreport(stmt->op == SETOP_NONE, MOD_OPT, "SetOperation type is inconsistant");

    /* process the WITH clause independently of all else */
    if (stmt->withClause) {
        qry->hasRecursive = stmt->withClause->recursive;
        qry->cteList = transformWithClause(pstate, stmt->withClause);
        qry->hasModifyingCTE = pstate->p_hasModifyingCTE;
    }

    /*
     * For each row of VALUES, transform the raw expressions.  This is also a
     * handy place to reject DEFAULT nodes and Rownum, which the grammar allows for
     * simplicity.
     *
     * Note that the intermediate representation we build is column-organized
     * not row-organized.  That simplifies the type and collation processing
     * below.
     */
    foreach (lc, stmt->valuesLists) {
        List* sublist = (List*)lfirst(lc);

        /* Do basic expression transformation (same as a ROW() expr) */
        sublist = transformExpressionList(pstate, sublist);

        /*
         * All the sublists must be the same length, *after* transformation
         * (which might expand '*' into multiple items).  The VALUES RTE can't
         * handle anything different.
         */
        if (sublist_length < 0) {
            /* Remember post-transformation length of first sublist */
            sublist_length = list_length(sublist);
            /* and allocate array for per-column lists */
            colexprs = (List**)palloc0(sublist_length * sizeof(List*));
        } else if (sublist_length != list_length(sublist)) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("VALUES lists must all be the same length"),
                    parser_errposition(pstate, exprLocation((Node*)sublist))));
        }

        /* Check for DEFAULT and Rownum, then build per-column expression lists */
        i = 0;
        foreach (lc2, sublist) {
            Node* col = (Node*)lfirst(lc2);

            if (IsA(col, SetToDefault)) {
                ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("DEFAULT can only appear in a VALUES list within INSERT"),
                        parser_errposition(pstate, exprLocation(col))));
            }
#ifndef ENABLE_MULTIPLE_NODES
            ExcludeRownumExpr(pstate, col);
#endif
            colexprs[i] = lappend(colexprs[i], col);
            i++;
        }

        /* Release sub-list's cells to save memory */
        list_free_ext(sublist);
    }

    /*
     * Now resolve the common types of the columns, and coerce everything to
     * those types.  Then identify the common collation, if any, of each
     * column.
     *
     * We must do collation processing now because (1) assign_query_collations
     * doesn't process rangetable entries, and (2) we need to label the VALUES
     * RTE with column collations for use in the outer query.  We don't
     * consider conflict of implicit collations to be an error here; instead
     * the column will just show InvalidOid as its collation, and you'll get a
     * failure later if that results in failure to resolve a collation.
     *
     * Note we modify the per-column expression lists in-place.
     */
    collations = NIL;
    for (i = 0; i < sublist_length; i++) {
        Oid coltype;
        Oid colcoll;

        coltype = select_common_type(pstate, colexprs[i], "VALUES", NULL);

        foreach (lc, colexprs[i]) {
            Node* col = (Node*)lfirst(lc);

            col = coerce_to_common_type(pstate, col, coltype, "VALUES");
            lfirst(lc) = (void*)col;
        }

        colcoll = select_common_collation(pstate, colexprs[i], true);

        collations = lappend_oid(collations, colcoll);
    }

    /*
     * Finally, rearrange the coerced expressions into row-organized lists.
     */
    exprsLists = NIL;
    foreach (lc, colexprs[0]) {
        Node* col = (Node*)lfirst(lc);
        List* sublist = NIL;

        sublist = list_make1(col);
        exprsLists = lappend(exprsLists, sublist);
    }
    list_free_ext(colexprs[0]);
    for (i = 1; i < sublist_length; i++) {
        forboth(lc, colexprs[i], lc2, exprsLists)
        {
            Node* col = (Node*)lfirst(lc);
            List* sublist = (List*)lfirst(lc2);

            /* sublist pointer in exprsLists won't need adjustment */
            (void)lappend(sublist, col);
        }
        list_free_ext(colexprs[i]);
    }

    /*
     * Generate the VALUES RTE
     */
    rte = addRangeTableEntryForValues(pstate, exprsLists, collations, NULL, true);
    addRTEtoQuery(pstate, rte, true, true, true);
    /* assume new rte is at end */
    rtindex = list_length(pstate->p_rtable);
    if (rte != rt_fetch(rtindex, pstate->p_rtable)) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("rte is not in p_rtable list")));
    }


    /*
     * Generate a targetlist as though expanding "*"
     */
    AssertEreport(pstate->p_next_resno == 1, MOD_OPT, "");
    qry->targetList = expandRelAttrs(pstate, rte, rtindex, 0, -1);

    /*
     * The grammar allows attaching ORDER BY, LIMIT, and FOR UPDATE to a
     * VALUES, so cope.
     */
    qry->sortClause = transformSortClause(
        pstate, stmt->sortClause, &qry->targetList, true /* fix unknowns */, false /* allow SQL92 rules */);

    qry->limitOffset = transformLimitClause(pstate, stmt->limitOffset, "OFFSET");
    qry->limitCount = transformLimitClause(pstate, stmt->limitCount, "LIMIT");

    if (stmt->lockingClause) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("SELECT FOR UPDATE/SHARE cannot be applied to VALUES")));
    }

    /*
     * There mustn't have been any table references in the expressions, else
     * strange things would happen, like Cartesian products of those tables
     * with the VALUES list.  We have to check this after parsing ORDER BY et
     * al since those could insert more junk.
     */
    if (list_length(pstate->p_joinlist) != 1) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("VALUES must not contain table references"),
                parser_errposition(pstate, locate_var_of_level((Node*)exprsLists, 0))));
    }

    if (list_length(pstate->p_rtable) != 1 && contain_vars_of_level((Node*)exprsLists, 0)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("VALUES must not contain OLD or NEW references"),
                errhint("Use SELECT ... UNION ALL ... instead."),
                parser_errposition(pstate, locate_var_of_level((Node*)exprsLists, 0))));
    }

    qry->rtable = pstate->p_rtable;
    qry->jointree = makeFromExpr(pstate->p_joinlist, NULL);

    qry->hasSubLinks = pstate->p_hasSubLinks;
    /* aggregates not allowed (but subselects are okay) */
    if (pstate->p_hasAggs) {
        ereport(ERROR,
            (errcode(ERRCODE_GROUPING_ERROR),
                errmsg("cannot use aggregate function in VALUES"),
                parser_errposition(pstate, locate_agg_of_level((Node*)exprsLists, 0))));
    }
    if (pstate->p_hasWindowFuncs) {
        ereport(ERROR,
            (errcode(ERRCODE_WINDOWING_ERROR),
                errmsg("cannot use window function in VALUES"),
                parser_errposition(pstate, locate_windowfunc((Node*)exprsLists))));
    }

    assign_query_collations(pstate, qry);

    return qry;
}

/*
 * transformSetOperationStmt -
 *	  transforms a set-operations tree
 *
 * A set-operation tree is just a SELECT, but with UNION/INTERSECT/EXCEPT
 * structure to it.  We must transform each leaf SELECT and build up a top-
 * level Query that contains the leaf SELECTs as subqueries in its rangetable.
 * The tree of set operations is converted into the setOperations field of
 * the top-level Query.
 */
static Query* transformSetOperationStmt(ParseState* pstate, SelectStmt* stmt)
{
    Query* qry = makeNode(Query);
    SelectStmt* leftmostSelect = NULL;
    int leftmostRTI;
    Query* leftmostQuery = NULL;
    SetOperationStmt* sostmt = NULL;
    List* sortClause = NIL;
    Node* limitOffset = NULL;
    Node* limitCount = NULL;
    List* lockingClause = NIL;
    WithClause* withClause = NULL;
    Node* node = NULL;
    ListCell* left_tlist = NULL;
    ListCell* lct = NULL;
    ListCell* lcm = NULL;
    ListCell* lcc = NULL;
    ListCell* l = NULL;
    List* targetvars = NIL;
    List* targetnames = NIL;
    List* sv_relnamespace = NIL;
    List* sv_varnamespace = NIL;
    int sv_rtable_length;
    RangeTblEntry* jrte = NULL;
    int tllen;

    qry->commandType = CMD_SELECT;

    /*
     * Find leftmost leaf SelectStmt.  We currently only need to do this in
     * order to deliver a suitable error message if there's an INTO clause
     * there, implying the set-op tree is in a context that doesn't allow
     * INTO.  (transformSetOperationTree would throw error anyway, but it
     * seems worth the trouble to throw a different error for non-leftmost
     * INTO, so we produce that error in transformSetOperationTree.)
     */
    leftmostSelect = stmt->larg;
    while (leftmostSelect != NULL && leftmostSelect->op != SETOP_NONE) {
        leftmostSelect = leftmostSelect->larg;
    }
    AssertEreport(leftmostSelect && IsA(leftmostSelect, SelectStmt) && leftmostSelect->larg == NULL, MOD_OPT, "");
    if (leftmostSelect->intoClause) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("SELECT ... INTO is not allowed here"),
                parser_errposition(pstate, exprLocation((Node*)leftmostSelect->intoClause))));
    }

    /*
     * We need to extract ORDER BY and other top-level clauses here and not
     * let transformSetOperationTree() see them --- else it'll just recurse
     * right back here!
     */
    sortClause = stmt->sortClause;
    limitOffset = stmt->limitOffset;
    limitCount = stmt->limitCount;
    lockingClause = stmt->lockingClause;
    withClause = stmt->withClause;

    stmt->sortClause = NIL;
    stmt->limitOffset = NULL;
    stmt->limitCount = NULL;
    stmt->lockingClause = NIL;
    stmt->withClause = NULL;

    /* We don't support FOR UPDATE/SHARE with set ops at the moment. */
    if (lockingClause != NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("SELECT FOR UPDATE/SHARE is not allowed with UNION/INTERSECT/EXCEPT")));
    }

    /* Process the WITH clause independently of all else */
    if (withClause != NULL) {
        qry->hasRecursive = withClause->recursive;
        qry->cteList = transformWithClause(pstate, withClause);
        qry->hasModifyingCTE = pstate->p_hasModifyingCTE;
    }

    /*
     * Recursively transform the components of the tree.
     */
    sostmt = (SetOperationStmt*)transformSetOperationTree(pstate, stmt, true, NULL);
    AssertEreport(
        sostmt && IsA(sostmt, SetOperationStmt), MOD_OPT, "Recursively transform the components of the tree failure");
    qry->setOperations = (Node*)sostmt;

    /*
     * Re-find leftmost SELECT (now it's a sub-query in rangetable)
     */
    node = sostmt->larg;
    while (node && IsA(node, SetOperationStmt))
        node = ((SetOperationStmt*)node)->larg;
    AssertEreport(node && IsA(node, RangeTblRef), MOD_OPT, "Node type inconsistant");
    leftmostRTI = ((RangeTblRef*)node)->rtindex;
    leftmostQuery = rt_fetch(leftmostRTI, pstate->p_rtable)->subquery;
    AssertEreport(leftmostQuery != NULL, MOD_OPT, "para should not be NULL");
    /*
     * Generate dummy targetlist for outer query using column names of
     * leftmost select and common datatypes/collations of topmost set
     * operation.  Also make lists of the dummy vars and their names for use
     * in parsing ORDER BY.
     *
     * Note: we use leftmostRTI as the varno of the dummy variables. It
     * shouldn't matter too much which RT index they have, as long as they
     * have one that corresponds to a real RT entry; else funny things may
     * happen when the tree is mashed by rule rewriting.
     */
    qry->targetList = NIL;
    targetvars = NIL;
    targetnames = NIL;
    left_tlist = list_head(leftmostQuery->targetList);

    forthree(lct, sostmt->colTypes, lcm, sostmt->colTypmods, lcc, sostmt->colCollations)
    {
        Oid colType = lfirst_oid(lct);
        int32 colTypmod = lfirst_int(lcm);
        Oid colCollation = lfirst_oid(lcc);
        TargetEntry* lefttle = (TargetEntry*)lfirst(left_tlist);
        char* colName = NULL;
        TargetEntry* tle = NULL;
        Var* var = NULL;

        AssertEreport(!lefttle->resjunk, MOD_OPT, "");
        colName = pstrdup(lefttle->resname);
        var = makeVar(leftmostRTI, lefttle->resno, colType, colTypmod, colCollation, 0);
        var->location = exprLocation((Node*)lefttle->expr);
        tle = makeTargetEntry((Expr*)var, (AttrNumber)pstate->p_next_resno++, colName, false);
        qry->targetList = lappend(qry->targetList, tle);
        targetvars = lappend(targetvars, var);
        targetnames = lappend(targetnames, makeString(colName));
        left_tlist = lnext(left_tlist);
    }

    /*
     * As a first step towards supporting sort clauses that are expressions
     * using the output columns, generate a varnamespace entry that makes the
     * output columns visible.	A Join RTE node is handy for this, since we
     * can easily control the Vars generated upon matches.
     *
     * Note: we don't yet do anything useful with such cases, but at least
     * "ORDER BY upper(foo)" will draw the right error message rather than
     * "foo not found".
     */
    sv_rtable_length = list_length(pstate->p_rtable);

    jrte = addRangeTableEntryForJoin(pstate, targetnames, JOIN_INNER, targetvars, NULL, false);

    sv_relnamespace = pstate->p_relnamespace;

    sv_varnamespace = pstate->p_varnamespace;
    pstate->p_relnamespace = NIL;
    pstate->p_varnamespace = NIL;

    /* add jrte to varnamespace only */
    addRTEtoQuery(pstate, jrte, false, false, true);

    /*
     * For now, we don't support resjunk sort clauses on the output of a
     * setOperation tree --- you can only use the SQL92-spec options of
     * selecting an output column by name or number.  Enforce by checking that
     * transformSortClause doesn't add any items to tlist.
     */
    tllen = list_length(qry->targetList);

    qry->sortClause = transformSortClause(
        pstate, sortClause, &qry->targetList, false /* no unknowns expected */, false /* allow SQL92 rules */);

    pstate->p_rtable = list_truncate(pstate->p_rtable, sv_rtable_length);
    pstate->p_relnamespace = sv_relnamespace;
    pstate->p_varnamespace = sv_varnamespace;

    if (tllen != list_length(qry->targetList)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("invalid UNION/INTERSECT/EXCEPT ORDER BY clause"),
                errdetail("Only result column names can be used, not expressions or functions."),
                errhint("Add the expression/function to every SELECT, or move the UNION into a FROM clause."),
                parser_errposition(pstate, exprLocation((const Node*)list_nth(qry->targetList, tllen)))));
    }

    qry->limitOffset = transformLimitClause(pstate, limitOffset, "OFFSET");
    qry->limitCount = transformLimitClause(pstate, limitCount, "LIMIT");

    qry->rtable = pstate->p_rtable;
    qry->jointree = makeFromExpr(pstate->p_joinlist, NULL);

    qry->hasSubLinks = pstate->p_hasSubLinks;
    qry->hasWindowFuncs = pstate->p_hasWindowFuncs;
    if (pstate->p_hasWindowFuncs) {
        parseCheckWindowFuncs(pstate, qry);
    }
    qry->hasAggs = pstate->p_hasAggs;

    foreach (l, lockingClause) {
        transformLockingClause(pstate, qry, (LockingClause*)lfirst(l), false);
    }

    assign_query_collations(pstate, qry);

    /* this must be done after collations, for reliable comparison of exprs */
    if (pstate->p_hasAggs || qry->groupClause || qry->groupingSets || qry->havingQual) {
        parseCheckAggregates(pstate, qry);
    }

    return qry;
}

/*
 * transformSetOperationTree
 *		Recursively transform leaves and internal nodes of a set-op tree
 *
 * In addition to returning the transformed node, if targetlist isn't NULL
 * then we return a list of its non-resjunk TargetEntry nodes.	For a leaf
 * set-op node these are the actual targetlist entries; otherwise they are
 * dummy entries created to carry the type, typmod, collation, and location
 * (for error messages) of each output column of the set-op node.  This info
 * is needed only during the internal recursion of this function, so outside
 * callers pass NULL for targetlist.  Note: the reason for passing the
 * actual targetlist entries of a leaf node is so that upper levels can
 * replace UNKNOWN Consts with properly-coerced constants.
 */
static Node* transformSetOperationTree(ParseState* pstate, SelectStmt* stmt, bool isTopLevel, List** targetlist)
{
    bool isLeaf = false;
    int rc = 0;

    AssertEreport(stmt && IsA(stmt, SelectStmt), MOD_OPT, "check stmt failure");

    /* Guard against stack overflow due to overly complex set-expressions */
    check_stack_depth();

    /*
     * Validity-check both leaf and internal SELECTs for disallowed ops.
     */
    if (stmt->intoClause) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("INTO is only allowed on first SELECT of UNION/INTERSECT/EXCEPT"),
                parser_errposition(pstate, exprLocation((Node*)stmt->intoClause))));
    }

    /* We don't support FOR UPDATE/SHARE with set ops at the moment. */
    if (stmt->lockingClause) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("SELECT FOR UPDATE/SHARE is not allowed with UNION/INTERSECT/EXCEPT")));
    }

    /*
     * If an internal node of a set-op tree has ORDER BY, LIMIT, FOR UPDATE,
     * or WITH clauses attached, we need to treat it like a leaf node to
     * generate an independent sub-Query tree.  Otherwise, it can be
     * represented by a SetOperationStmt node underneath the parent Query.
     */
    if (stmt->op == SETOP_NONE) {
        AssertEreport(stmt->larg == NULL && stmt->rarg == NULL, MOD_OPT, "stmt is inconsistant");
        isLeaf = true;
    } else {
        AssertEreport(stmt->larg != NULL && stmt->rarg != NULL, MOD_OPT, "");
        if (stmt->sortClause || stmt->limitOffset || stmt->limitCount || stmt->lockingClause || stmt->withClause) {
            isLeaf = true;
        } else {
            isLeaf = false;
        }
    }

    if (isLeaf) {
        /* Process leaf SELECT */
        Query* selectQuery = NULL;
        char selectName[32];
        RangeTblEntry PG_USED_FOR_ASSERTS_ONLY* rte = NULL;
        RangeTblRef* rtr = NULL;
        ListCell* tl = NULL;

        /*
         * Transform SelectStmt into a Query.
         *
         * Note: previously transformed sub-queries don't affect the parsing
         * of this sub-query, because they are not in the toplevel pstate's
         * namespace list.
         */
        selectQuery = parse_sub_analyze((Node*)stmt, pstate, NULL, false, false);

        /*
         * Check for bogus references to Vars on the current query level (but
         * upper-level references are okay). Normally this can't happen
         * because the namespace will be empty, but it could happen if we are
         * inside a rule.
         */
        if (pstate->p_relnamespace || pstate->p_varnamespace) {
            if (contain_vars_of_level((Node*)selectQuery, 1)) {
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                        errmsg("UNION/INTERSECT/EXCEPT member statement cannot refer to other relations of same query "
                               "level"),
                        parser_errposition(pstate, locate_var_of_level((Node*)selectQuery, 1))));
            }
        }

        /*
         * Extract a list of the non-junk TLEs for upper-level processing.
         */
        if (targetlist != NULL) {
            *targetlist = NIL;
            foreach (tl, selectQuery->targetList) {
                TargetEntry* tle = (TargetEntry*)lfirst(tl);

                if (!tle->resjunk) {
                    *targetlist = lappend(*targetlist, tle);
                }
            }
        }

        /*
         * Make the leaf query be a subquery in the top-level rangetable.
         */
        rc = snprintf_s(
            selectName, sizeof(selectName), sizeof(selectName) - 1, "*SELECT* %d", list_length(pstate->p_rtable) + 1);
        securec_check_ss(rc, "", "");
        rte = addRangeTableEntryForSubquery(pstate, selectQuery, makeAlias(selectName, NIL), false, false);

        /*
         * Return a RangeTblRef to replace the SelectStmt in the set-op tree.
         */
        rtr = makeNode(RangeTblRef);
        /* assume new rte is at end */
        rtr->rtindex = list_length(pstate->p_rtable);
        AssertEreport(rte == rt_fetch(rtr->rtindex, pstate->p_rtable), MOD_OPT, "check with rt_fetch failure");
        return (Node*)rtr;
    } else {
        /* Process an internal node (set operation node) */
        SetOperationStmt* op = makeNode(SetOperationStmt);
        List* ltargetlist = NIL;
        List* rtargetlist = NIL;
        ListCell* ltl = NULL;
        ListCell* rtl = NULL;
        const char* context = NULL;

        context = (stmt->op == SETOP_UNION ? "UNION" : (stmt->op == SETOP_INTERSECT ? "INTERSECT" : "EXCEPT"));

        op->op = stmt->op;
        op->all = stmt->all;

        /*
         * Recursively transform the left child node.
         */
        op->larg = transformSetOperationTree(pstate, stmt->larg, false, &ltargetlist);

        /*
         * If we are processing a recursive union query, now is the time to
         * examine the non-recursive term's output columns and mark the
         * containing CTE as having those result columns.  We should do this
         * only at the topmost setop of the CTE, of course.
         */
        if (isTopLevel && pstate->p_parent_cte && pstate->p_parent_cte->cterecursive) {
            determineRecursiveColTypes(pstate, op->larg, ltargetlist);
        }

        /*
         * Recursively transform the right child node.
         */
        op->rarg = transformSetOperationTree(pstate, stmt->rarg, false, &rtargetlist);

        /*
         * Verify that the two children have the same number of non-junk
         * columns, and determine the types of the merged output columns.
         */
        if (list_length(ltargetlist) != list_length(rtargetlist)) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("each %s query must have the same number of columns", context),
                    parser_errposition(pstate, exprLocation((Node*)rtargetlist))));
        }

        if (targetlist != NULL) {
            *targetlist = NIL;
        }
        op->colTypes = NIL;
        op->colTypmods = NIL;
        op->colCollations = NIL;
        op->groupClauses = NIL;
        forboth(ltl, ltargetlist, rtl, rtargetlist)
        {
            TargetEntry* ltle = (TargetEntry*)lfirst(ltl);
            TargetEntry* rtle = (TargetEntry*)lfirst(rtl);
            Node* lcolnode = (Node*)ltle->expr;
            Node* rcolnode = (Node*)rtle->expr;
            Oid lcoltype = exprType(lcolnode);
            Oid rcoltype = exprType(rcolnode);
            int32 lcoltypmod = exprTypmod(lcolnode);
            int32 rcoltypmod = exprTypmod(rcolnode);
            Node* bestexpr = NULL;
            int bestlocation;
            Oid rescoltype;
            int32 rescoltypmod;
            Oid rescolcoll;

            /* select common type, same as CASE et al */
            rescoltype = select_common_type(pstate, list_make2(lcolnode, rcolnode), context, &bestexpr);
            bestlocation = exprLocation(bestexpr);
            /* if same type and same typmod, use typmod; else default */
            if (lcoltype == rcoltype && lcoltypmod == rcoltypmod) {
                rescoltypmod = lcoltypmod;
            } else {
                rescoltypmod = -1;
            }

            /*
             * Verify the coercions are actually possible.	If not, we'd fail
             * later anyway, but we want to fail now while we have sufficient
             * context to produce an error cursor position.
             *
             * For all non-UNKNOWN-type cases, we verify coercibility but we
             * don't modify the child's expression, for fear of changing the
             * child query's semantics.
             *
             * If a child expression is an UNKNOWN-type Const or Param, we
             * want to replace it with the coerced expression.	This can only
             * happen when the child is a leaf set-op node.  It's safe to
             * replace the expression because if the child query's semantics
             * depended on the type of this output column, it'd have already
             * coerced the UNKNOWN to something else.  We want to do this
             * because (a) we want to verify that a Const is valid for the
             * target type, or resolve the actual type of an UNKNOWN Param,
             * and (b) we want to avoid unnecessary discrepancies between the
             * output type of the child query and the resolved target type.
             * Such a discrepancy would disable optimization in the planner.
             *
             * If it's some other UNKNOWN-type node, eg a Var, we do nothing
             * (knowing that coerce_to_common_type would fail).  The planner
             * is sometimes able to fold an UNKNOWN Var to a constant before
             * it has to coerce the type, so failing now would just break
             * cases that might work.
             */
            if (lcoltype != UNKNOWNOID) {
                lcolnode = coerce_to_common_type(pstate, lcolnode, rescoltype, context);
            } else if (IsA(lcolnode, Const) || IsA(lcolnode, Param)) {
                lcolnode = coerce_to_common_type(pstate, lcolnode, rescoltype, context);
                ltle->expr = (Expr*)lcolnode;
            }

            if (rcoltype != UNKNOWNOID) {
                rcolnode = coerce_to_common_type(pstate, rcolnode, rescoltype, context);
            } else if (IsA(rcolnode, Const) || IsA(rcolnode, Param)) {
                rcolnode = coerce_to_common_type(pstate, rcolnode, rescoltype, context);
                rtle->expr = (Expr*)rcolnode;
            }

            /*
             * Select common collation.  A common collation is required for
             * all set operators except UNION ALL; see SQL:2008 7.13 <query
             * expression> Syntax Rule 15c.  (If we fail to identify a common
             * collation for a UNION ALL column, the curCollations element
             * will be set to InvalidOid, which may result in a runtime error
             * if something at a higher query level wants to use the column's
             * collation.)
             */
            rescolcoll =
                select_common_collation(pstate, list_make2(lcolnode, rcolnode), (op->op == SETOP_UNION && op->all));

            /* emit results */
            op->colTypes = lappend_oid(op->colTypes, rescoltype);
            op->colTypmods = lappend_int(op->colTypmods, rescoltypmod);
            op->colCollations = lappend_oid(op->colCollations, rescolcoll);

            /*
             * For all cases except UNION ALL, identify the grouping operators
             * (and, if available, sorting operators) that will be used to
             * eliminate duplicates.
             */
            if (op->op != SETOP_UNION || !op->all) {
                SortGroupClause* grpcl = makeNode(SortGroupClause);
                Oid sortop;
                Oid eqop;
                bool hashable = false;
                ParseCallbackState pcbstate;

                setup_parser_errposition_callback(&pcbstate, pstate, bestlocation);

                /* determine the eqop and optional sortop */
                get_sort_group_operators(rescoltype, false, true, false, &sortop, &eqop, NULL, &hashable);

                cancel_parser_errposition_callback(&pcbstate);

                /* we don't have a tlist yet, so can't assign sortgrouprefs */
                grpcl->tleSortGroupRef = 0;
                grpcl->eqop = eqop;
                grpcl->sortop = sortop;
                grpcl->nulls_first = false; /* OK with or without sortop */
                grpcl->hashable = hashable;

                op->groupClauses = lappend(op->groupClauses, grpcl);
            }

            /*
             * Construct a dummy tlist entry to return.  We use a SetToDefault
             * node for the expression, since it carries exactly the fields
             * needed, but any other expression node type would do as well.
             */
            if (targetlist != NULL) {
                SetToDefault* rescolnode = makeNode(SetToDefault);
                TargetEntry* restle = NULL;

                rescolnode->typeId = rescoltype;
                rescolnode->typeMod = rescoltypmod;
                rescolnode->collation = rescolcoll;
                rescolnode->location = bestlocation;
                restle = makeTargetEntry((Expr*)rescolnode,
                    0, /* no need to set resno */
                    NULL,
                    false);
                *targetlist = lappend(*targetlist, restle);
            }
        }

        return (Node*)op;
    }
}

/*
 * Process the outputs of the non-recursive term of a recursive union
 * to set up the parent CTE's columns
 */
static void determineRecursiveColTypes(ParseState* pstate, Node* larg, List* nrtargetlist)
{
    Node* node = NULL;
    int leftmostRTI;
    Query* leftmostQuery = NULL;
    List* targetList = NIL;
    ListCell* left_tlist = NULL;
    ListCell* nrtl = NULL;
    int next_resno;

    /*
     * Find leftmost leaf SELECT
     */
    node = larg;
    while (node && IsA(node, SetOperationStmt)) {
        node = ((SetOperationStmt*)node)->larg;
    }
    AssertEreport(node && IsA(node, RangeTblRef), MOD_OPT, "check node type inconsistant");
    leftmostRTI = ((RangeTblRef*)node)->rtindex;
    leftmostQuery = rt_fetch(leftmostRTI, pstate->p_rtable)->subquery;
    if (unlikely(leftmostQuery == NULL)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), 
                errmsg("leftmostQuery should not be null")));
    }

    /*
     * Generate dummy targetlist using column names of leftmost select and
     * dummy result expressions of the non-recursive term.
     */
    targetList = NIL;
    left_tlist = list_head(leftmostQuery->targetList);
    next_resno = 1;

    foreach (nrtl, nrtargetlist) {
        TargetEntry* nrtle = (TargetEntry*)lfirst(nrtl);
        TargetEntry* lefttle = (TargetEntry*)lfirst(left_tlist);
        char* colName = NULL;
        TargetEntry* tle = NULL;

        AssertEreport(!lefttle->resjunk, MOD_OPT, "");
        colName = pstrdup(lefttle->resname);
        tle = makeTargetEntry(nrtle->expr, next_resno++, colName, false);
        targetList = lappend(targetList, tle);
        left_tlist = lnext(left_tlist);
    }

    /* Now build CTE's output column info using dummy targetlist */
    analyzeCTETargetList(pstate, pstate->p_parent_cte, targetList);
}

/*
 *  fixResTargetNameWithTableNameRef
 *  The goal of this function try to handle set_clause in updatestmt, and update
 *  with table name or alias prefix. Do this for compatibility A.
 */
void fixResTargetNameWithTableNameRef(Relation rd, RangeVar* rel, ResTarget* res)
{
    char* resname = NULL;
    AttrNumber attrnoIndName = InvalidAttrNumber;
    AttrNumber attrnoResName = InvalidAttrNumber;
    Oid attrtypeid = InvalidOid;
    Oid typrelid = InvalidOid;

    /* the length of indireciton can't less than 1 and the first item type must not be A_Indices */
    if ((list_length(res->indirection) < 1) || IsA(linitial(res->indirection), A_Indices)) {
        return;
    }

    /* name must relname or aliasname */
    if ((strcmp(rel->relname, res->name) != 0) &&
        ((rel->alias == NULL) || (strcmp((rel->alias)->aliasname, res->name) != 0))) {
        return;
    }

    /*
     * if meet set_clause like a.b.c = 1 then the indirection will exceed one,
     * so treat first indirection as tablename/alisename like a in this ex.
     */
    resname = pstrdup(strVal(linitial(res->indirection)));
    if (list_length(res->indirection) > 1) {
        res->name = resname;
        res->indirection = RemoveListCell(res->indirection, 1);
        return;
    }

    /*
     * Here only meet set_clause which has two elements like a.b or test.b.
     * So first keep forward-ompatibility behavior of old version, that results
     * if a is a composite type and also relname/alisename then we first handle it as
     * composite type.
     * Otherwise we handle it like a.b like relname/alisename, then a is a relname just
     * update set_clause over.
     */
    attrnoResName = attnameAttNum(rd, res->name, true);
    attrnoIndName = attnameAttNum(rd, resname, true);

    if (attrnoResName > 0) {
        attrtypeid = attnumTypeId(rd, attrnoResName);
        if (attrtypeid) {
            typrelid = typeidTypeRelid(attrtypeid);
            if (typrelid && ((get_attnum(typrelid, resname) != InvalidAttrNumber) || (attrnoIndName <= 0))) {
                ereport(
                    NOTICE, (errmsg("update field '%s' of column '%s', though it's ambiguous.", resname, res->name)));
                return;
            }
        }
    }

    res->name = resname;
    res->indirection = RemoveListCell(res->indirection, 1);
}

/*
 * fixResTargetListWithTableNameRef
 * 	  transforms an update set_clause_list with tablename or alias prefix
 */
void fixResTargetListWithTableNameRef(Relation rd, RangeVar* rel, List* clause_list)
{
    ListCell* cell = NULL;

    foreach (cell, clause_list) {
        if (IsA(lfirst(cell), ResTarget)) {
            fixResTargetNameWithTableNameRef(rd, rel, (ResTarget*)lfirst(cell));
        }
    }
}

/*
 * transformUpdateStmt -
 *	  transforms an update statement
 */
static Query* transformUpdateStmt(ParseState* pstate, UpdateStmt* stmt)
{
    Query* qry = makeNode(Query);
    Node* qual = NULL;

    qry->commandType = CMD_UPDATE;
    pstate->p_is_insert = false;

    /* set io state for backend status for the thread, we will use it to check user space */
    pgstat_set_io_state(IOSTATE_READ);

    /* process the WITH clause independently of all else */
    if (stmt->withClause) {
#ifdef PGXC
        SendCommandIdForInsertCte(qry, stmt->withClause);
#endif
        qry->hasRecursive = stmt->withClause->recursive;
        qry->cteList = transformWithClause(pstate, stmt->withClause);
        qry->hasModifyingCTE = pstate->p_hasModifyingCTE;
    }

    qry->resultRelation =
        setTargetTable(pstate, stmt->relation, interpretInhOption(stmt->relation->inhOpt), true, ACL_UPDATE);

    // check column store relation distributed by replication
    if (pstate->p_target_relation != NULL && RelationIsColStore(pstate->p_target_relation) &&
        pstate->p_target_relation->rd_locator_info &&
        IsLocatorReplicated(pstate->p_target_relation->rd_locator_info->locatorType)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Un-support feature"),
                errdetail("replicated columnar table doesn't allow UPDATE")));
    }

#ifdef ENABLE_MULTIPLE_NODES
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
#endif
        if (pstate->p_target_relation != NULL && RelationIsMatview(pstate->p_target_relation)) {
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("Unsupported feature"),
                            errdetail("Materialized view doesn't allow UPDATE")));
        }
#ifdef ENABLE_MULTIPLE_NODES
    }
#endif

    // check update permission
    if (pstate->p_target_relation != NULL &&
        ((unsigned int)RelationGetInternalMask(pstate->p_target_relation) & INTERNAL_MASK_DUPDATE)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Un-support feature"),
                errdetail("internal relation doesn't allow UPDATE")));
    }

    // check if the target relation is being redistributed in read only mode
    if (!u_sess->attr.attr_sql.enable_cluster_resize && pstate->p_target_relation != NULL &&
        RelationInClusterResizingWriteErrorMode(pstate->p_target_relation)) {
        ereport(ERROR,
            (errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
                errmsg("%s is redistributing, please retry later.", pstate->p_target_relation->rd_rel->relname.data)));
    }

    /*
     * the FROM clause is non-standard SQL syntax. We used to be able to do
     * this with REPLACE in POSTQUEL so we keep the feature.
     */
    transformFromClause(pstate, stmt->fromClause);

    qry->targetList = transformTargetList(pstate, stmt->targetList);
    qual = transformWhereClause(pstate, stmt->whereClause, "WHERE");

    qry->returningList = transformReturningList(pstate, stmt->returningList);
    if (qry->returningList != NIL && RelationIsColStore(pstate->p_target_relation)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Un-support feature"),
                errdetail("column stored relation doesn't support UPDATE returning")));
    }

    qry->rtable = pstate->p_rtable;
    qry->jointree = makeFromExpr(pstate->p_joinlist, qual);

    qry->hasSubLinks = pstate->p_hasSubLinks;

    /*
     * Top-level aggregates are simply disallowed in UPDATE, per spec. (From
     * an implementation point of view, this is forced because the implicit
     * ctid reference would otherwise be an ungrouped variable.)
     */
    if (pstate->p_hasAggs) {
        ereport(ERROR,
            (errcode(ERRCODE_GROUPING_ERROR),
                errmsg("cannot use aggregate function in UPDATE"),
                parser_errposition(pstate, locate_agg_of_level((Node*)qry, 0))));
    }
    if (pstate->p_hasWindowFuncs) {
        ereport(ERROR,
            (errcode(ERRCODE_WINDOWING_ERROR),
                errmsg("cannot use window function in UPDATE"),
                parser_errposition(pstate, locate_windowfunc((Node*)qry))));
    }

    /*
     * Now we are done with SELECT-like processing, and can get on with
     * transforming the target list to match the UPDATE target columns.
     */
    qry->targetList = transformUpdateTargetList(pstate, qry->targetList, stmt->targetList, stmt->relation);

    assign_query_collations(pstate, qry);
    qry->hintState = stmt->hintState;

    return qry;
}

/*
 * transformUpdateTargetList -
 * handle SET clause in UPDATE/INSERT ... DUPLICATE KEY UPDATE
 */
static List* transformUpdateTargetList(ParseState* pstate, List* qryTlist, List* origTlist, RangeVar* stmtrel)
{
    List* tlist = NIL;
    RangeTblEntry* target_rte = NULL;
    ListCell* orig_tl = NULL;
    ListCell* tl = NULL;

    tlist = qryTlist;

    /* Prepare to assign non-conflicting resnos to resjunk attributes */
    if (pstate->p_next_resno <= pstate->p_target_relation->rd_rel->relnatts) {
        pstate->p_next_resno = pstate->p_target_relation->rd_rel->relnatts + 1;
    }

    /* Prepare non-junk columns for assignment to target table */
    target_rte = pstate->p_target_rangetblentry;
    orig_tl = list_head(origTlist);

    foreach (tl, tlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(tl);
        ResTarget* origTarget = NULL;
        int attrno;

        if (tle->resjunk) {
            /*
             * Resjunk nodes need no additional processing, but be sure they
             * have resnos that do not match any target columns; else rewriter
             * or planner might get confused.  They don't need a resname
             * either.
             */
            tle->resno = (AttrNumber)pstate->p_next_resno++;
            tle->resname = NULL;
            continue;
        }

        if (orig_tl == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("UPDATE target count mismatch --- internal error")));
        }

        origTarget = (ResTarget*)lfirst(orig_tl);
        Assert(IsA(origTarget, ResTarget));
        if (stmtrel != NULL) {
            fixResTargetNameWithTableNameRef(pstate->p_target_relation, stmtrel, origTarget);
        }

        attrno = attnameAttNum(pstate->p_target_relation, origTarget->name, true);
        if (attrno == InvalidAttrNumber) {
            if (!origTarget->indirection) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_COLUMN),
                        errmsg("column \"%s\" of relation \"%s\" does not exist",
                            origTarget->name,
                            RelationGetRelationName(pstate->p_target_relation)),
                        parser_errposition(pstate, origTarget->location)));
            } else {
                char* resname = pstrdup((char*)(((Value*)lfirst(list_head(origTarget->indirection)))->val.str));
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_COLUMN),
                        errmsg("column \"%s.%s\" of relation \"%s\" does not exist",
                            origTarget->name,
                            resname,
                            RelationGetRelationName(pstate->p_target_relation)),
                        parser_errposition(pstate, origTarget->location)));
            }
        }
        updateTargetListEntry(pstate, tle, origTarget->name, attrno, origTarget->indirection, origTarget->location);

        /* Mark the target column as requiring update permissions */
        target_rte->updatedCols = bms_add_member(target_rte->updatedCols, attrno - FirstLowInvalidHeapAttributeNumber);

        orig_tl = lnext(orig_tl);
    }
    if (orig_tl != NULL) {
        ereport(
            ERROR, (errcode(ERRCODE_NOT_NULL_VIOLATION), errmsg("UPDATE target count mismatch --- internal error")));
    }

    setExtraUpdatedCols(pstate->p_target_rangetblentry, pstate->p_target_relation->rd_att);

    return tlist;
}

/*
 * transformReturningList -
 *	handle a RETURNING clause in INSERT/UPDATE/DELETE
 */
static List* transformReturningList(ParseState* pstate, List* returningList)
{
    List* rlist = NIL;
    int save_next_resno;
    bool save_hasAggs = false;
    bool save_hasWindowFuncs = false;

    if (returningList == NIL) {
        return NIL; /* nothing to do */
    }

    /*
     * We need to assign resnos starting at one in the RETURNING list. Save
     * and restore the main tlist's value of p_next_resno, just in case
     * someone looks at it later (probably won't happen).
     */
    save_next_resno = pstate->p_next_resno;
    pstate->p_next_resno = 1;

    /* save other state so that we can detect disallowed stuff */
    save_hasAggs = pstate->p_hasAggs;
    pstate->p_hasAggs = false;
    save_hasWindowFuncs = pstate->p_hasWindowFuncs;
    pstate->p_hasWindowFuncs = false;

    /* transform RETURNING identically to a SELECT targetlist */
    rlist = transformTargetList(pstate, returningList);

    /* check for disallowed stuff */

    /* aggregates not allowed (but subselects are okay) */
    if (pstate->p_hasAggs) {
        ereport(ERROR,
            (errcode(ERRCODE_GROUPING_ERROR),
                errmsg("cannot use aggregate function in RETURNING"),
                parser_errposition(pstate, locate_agg_of_level((Node*)rlist, 0))));
    }
    if (pstate->p_hasWindowFuncs) {
        ereport(ERROR,
            (errcode(ERRCODE_WINDOWING_ERROR),
                errmsg("cannot use window function in RETURNING"),
                parser_errposition(pstate, locate_windowfunc((Node*)rlist))));
    }

    /* mark column origins */
    markTargetListOrigins(pstate, rlist);

    /* resolve any still-unresolved output columns as being type text */
    if (pstate->p_resolve_unknowns) {
        resolveTargetListUnknowns(pstate, rlist);
    }

    /* restore state */
    pstate->p_next_resno = save_next_resno;
    pstate->p_hasAggs = save_hasAggs;
    pstate->p_hasWindowFuncs = save_hasWindowFuncs;

    return rlist;
}

/*
 * transformDeclareCursorStmt -
 *	transform a DECLARE CURSOR Statement
 *
 * DECLARE CURSOR is a hybrid case: it's an optimizable statement (in fact not
 * significantly different from a SELECT) as far as parsing/rewriting/planning
 * are concerned, but it's not passed to the executor and so in that sense is
 * a utility statement.  We transform it into a Query exactly as if it were
 * a SELECT, then stick the original DeclareCursorStmt into the utilityStmt
 * field to carry the cursor name and options.
 */
static Query* transformDeclareCursorStmt(ParseState* pstate, DeclareCursorStmt* stmt)
{
    Query* result = NULL;

    /*
     * Don't allow both SCROLL and NO SCROLL to be specified
     */
    if ((stmt->options & CURSOR_OPT_SCROLL) && (stmt->options & CURSOR_OPT_NO_SCROLL)) {
        ereport(
            ERROR, (errcode(ERRCODE_INVALID_CURSOR_DEFINITION), errmsg("cannot specify both SCROLL and NO SCROLL")));
    }

    result = transformStmt(pstate, stmt->query);

    /* Grammar should not have allowed anything but SELECT */
    if (!IsA(result, Query) || result->commandType != CMD_SELECT || result->utilityStmt != NULL) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("unexpected non-SELECT command in DECLARE CURSOR")));
    }

    /*
     * We also disallow data-modifying WITH in a cursor.  (This could be
     * allowed, but the semantics of when the updates occur might be
     * surprising.)
     */
    if (result->hasModifyingCTE) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("DECLARE CURSOR must not contain data-modifying statements in WITH")));
    }

    /* FOR UPDATE and WITH HOLD are not compatible */
    if (result->rowMarks != NIL && (stmt->options & CURSOR_OPT_HOLD)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("DECLARE CURSOR WITH HOLD ... FOR UPDATE/SHARE is not supported"),
                errdetail("Holdable cursors must be READ ONLY.")));
    }

    /* FOR UPDATE and SCROLL are not compatible */
    if (result->rowMarks != NIL && (stmt->options & CURSOR_OPT_SCROLL)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("DECLARE SCROLL CURSOR ... FOR UPDATE/SHARE is not supported"),
                errdetail("Scrollable cursors must be READ ONLY.")));
    }

    /* FOR UPDATE and INSENSITIVE are not compatible */
    if (result->rowMarks != NIL && (stmt->options & CURSOR_OPT_INSENSITIVE)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("DECLARE INSENSITIVE CURSOR ... FOR UPDATE/SHARE is not supported"),
                errdetail("Insensitive cursors must be READ ONLY.")));
    }

    /* We won't need the raw querytree any more */
    stmt->query = NULL;

    result->utilityStmt = (Node*)stmt;

    return result;
}

/*
 * transformExplainStmt -
 *	transform an EXPLAIN Statement
 *
 * EXPLAIN is like other utility statements in that we emit it as a
 * CMD_UTILITY Query node; however, we must first transform the contained
 * query.  We used to postpone that until execution, but it's really necessary
 * to do it during the normal parse analysis phase to ensure that side effects
 * of parser hooks happen at the expected time.
 */
static Query* transformExplainStmt(ParseState* pstate, ExplainStmt* stmt)
{
    Query* result = NULL;

    /* transform contained query, allowing SELECT INTO */
    stmt->query = (Node*)transformTopLevelStmt(pstate, stmt->query);

    /* represent the command as a utility Query */
    result = makeNode(Query);
    result->commandType = CMD_UTILITY;
    result->utilityStmt = (Node*)stmt;

    return result;
}

/*
 * transformCreateTableAsStmt -
 * transform a CREATE TABLE AS, SELECT ... INTO, or CREATE MATERIALIZED VIEW Statement
 *
 * As with EXPLAIN, transform the contained statement now.
 */
static Query* transformCreateTableAsStmt(ParseState* pstate, CreateTableAsStmt* stmt)
{
    Query* result = NULL;
    ListCell* lc = NULL;

    /*
     * Set relkind in IntoClause based on statement relkind.  These are
     * different types, because the parser users the ObjectType enumeration
     * and the executor uses RELKIND_* defines.
     */
    switch (stmt->relkind)
    {
        case (OBJECT_TABLE):
            stmt->into->relkind = RELKIND_RELATION;
            break;
        case (OBJECT_MATVIEW):
            stmt->into->relkind = RELKIND_MATVIEW;
            break;
        default:
            elog(ERROR, "unrecognized object relkind: %d",
                 (int) stmt->relkind);
    }

    /* transform contained query */
    stmt->query = (Node*)transformStmt(pstate, stmt->query);

    /* if result type of new relation is unknown-type, then resolve as type TEXT */
    foreach (lc, ((Query*)stmt->query)->targetList) {
        TargetEntry* tle = (TargetEntry*)lfirst(lc);
        if (exprType((Node*)tle->expr) == UNKNOWNOID) {
            tle->expr = (Expr*)coerce_type(
                pstate, (Node*)tle->expr, UNKNOWNOID, TEXTOID, -1, COERCION_IMPLICIT, COERCE_IMPLICIT_CAST, -1);
        }
    }

    /* set io state for backend status for the thread, we will use it to check user space */
    pgstat_set_io_state(IOSTATE_WRITE);

    /* represent the command as a utility Query */
    result = makeNode(Query);
    result->commandType = CMD_UTILITY;
    result->hintState = (HintState*)copyObject(((Query*)stmt->query)->hintState);
    result->utilityStmt = (Node*)stmt;

    return result;
}

#ifdef PGXC
#ifdef ENABLE_DISTRIBUTE_TEST
/*
 * Check if given GUC variable can go through EXECUTE DIRECT
 */
static bool checkExecDirectVariableSetStmt(const Node* node)
{
    if (nodeTag(node) == T_VariableSetStmt) {
        if (pg_strcasecmp(((VariableSetStmt*)node)->name, "distribute_test_param") == 0) {
            return true;
        }
    }

    return false;
}
#endif

static void get_index_and_type_from_nodename(const char* node_name, int* node_index, char* node_type)
{
    Oid node_oid;

    if (node_name == NULL || node_index == NULL || node_type == NULL) {
        return;
    }

    node_oid = get_pgxc_nodeoid(node_name);
    if (!OidIsValid(node_oid)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("PGXC Node %s: object not defined", node_name)));
    }

    *node_type = get_pgxc_nodetype(node_oid);
    *node_index = PGXCNodeGetNodeId(node_oid, get_pgxc_nodetype(node_oid));

    return;
}

RemoteQueryExecType fill_exec_type(char node_type, ExecDirectOption option)
{
    RemoteQueryExecType exec_type;

    switch (option) {
        case EXEC_DIRECT_ON_LIST:
            if (node_type == PGXC_NODE_COORDINATOR) {
                exec_type = EXEC_ON_COORDS;
            } else {
                exec_type = EXEC_ON_DATANODES;
            }
            break;
        case EXEC_DIRECT_ON_ALL_CN:
            exec_type = EXEC_ON_COORDS;
            break;
        case EXEC_DIRECT_ON_ALL_DN:
            exec_type = EXEC_ON_DATANODES;
            break;
        case EXEC_DIRECT_ON_ALL_NODES:
            exec_type = EXEC_ON_ALL_NODES;
            break;
        default:
            exec_type = EXEC_ON_NONE;
            break;
    }

    return exec_type;
}

void check_node_index(int node_index, char* node_flag, int length_node_flag)
{
    if ((node_flag != NULL) &&
        (node_index < length_node_flag) &&
        (node_index >= 0) &&
        node_flag[node_index] == 0) {
        node_flag[node_index] = 1;
    } else {
        ereport(ERROR,
                (errcode(ERRCODE_OPERATE_NOT_SUPPORTED),
                 errmsg("node name in node list is not correct")));
    }
}

static void fill_node_list_and_exec_type(const List* nodenames_list, ExecDirectOption option, RemoteQuery* query, bool* include_local)
{
    char* node_name = NULL;
    int node_index;
    char node_type = PGXC_NODE_NONE;
    ListCell* nodename_item = NULL;
    List* index_list = NULL;
    int number_of_cn = 0;
    int number_of_dn = 0;
    char* cn_flag = NULL;
    char* dn_flag = NULL;

    *include_local = false;
    if (option == EXEC_DIRECT_ON_LIST) {
        if (u_sess->pgxc_cxt.NumCoords > 0) {
            cn_flag = (char*)palloc0(u_sess->pgxc_cxt.NumCoords);
        }
        if (u_sess->pgxc_cxt.NumDataNodes > 0) {
            dn_flag = (char*)palloc0(u_sess->pgxc_cxt.NumDataNodes);
        }

        foreach(nodename_item, nodenames_list) {
            node_name = strVal(lfirst(nodename_item));
            get_index_and_type_from_nodename(node_name, &node_index, &node_type);
            if (node_type == PGXC_NODE_COORDINATOR) {
                check_node_index(node_index, cn_flag, u_sess->pgxc_cxt.NumCoords);
                number_of_cn++;
                if (node_index == u_sess->pgxc_cxt.PGXCNodeId - 1) {
                    *include_local = true;
                    continue;
                }
            } else if (node_type == PGXC_NODE_DATANODE) {
                check_node_index(node_index, dn_flag, u_sess->pgxc_cxt.NumDataNodes);
                number_of_dn++;
            } else {
                ereport(ERROR,
                        (errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                         errmsg("unsupport node type: %d", node_type)));
            }
            index_list = lappend_int(index_list, node_index);
        }

        if ((number_of_cn != 0) && (number_of_dn != 0)) {
            ereport(ERROR,
                    (errcode(ERRCODE_OPERATE_NOT_SUPPORTED),
                     errmsg("not support both coordinator and datanode in the execute list")));
        }
    }

    query->exec_type = fill_exec_type(node_type, option);
    query->exec_nodes->nodeList = index_list;
    if ((option == EXEC_DIRECT_ON_ALL_CN) || (option == EXEC_DIRECT_ON_ALL_NODES)) {
        *include_local = true;
    }

    pfree_ext(cn_flag);
    pfree_ext(dn_flag);

    return;
}

ExecDirectType set_exec_direct_type(bool is_local, CmdType command_type)
{
    ExecDirectType type = EXEC_DIRECT_NONE;

    if (is_local) {
        if (command_type == CMD_UTILITY)
            type = EXEC_DIRECT_LOCAL_UTILITY;
        else
            type = EXEC_DIRECT_LOCAL;
    } else {
        switch (command_type) {
            case CMD_UTILITY:
                type = EXEC_DIRECT_UTILITY;
                break;
            case CMD_SELECT:
                type = EXEC_DIRECT_SELECT;
                break;
            case CMD_INSERT:
                type = EXEC_DIRECT_INSERT;
                break;
            case CMD_UPDATE:
                type = EXEC_DIRECT_UPDATE;
                break;
            case CMD_DELETE:
                type = EXEC_DIRECT_DELETE;
                break;
            case CMD_UNKNOWN:
                break;
            default:
                /* CMD_MERGE and CMD_NOTHING are handled before set up EXECUTE DIRECT flag */
                ereport(ERROR,
                    (errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                        errmsg("unrecognized commandType: %d", command_type)));
                break;
        }
    }

    return type;
}

void check_command_type(CmdType command_type)
{
    if (command_type == CMD_MERGE) {
        ereport(
            ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("EXECUTE DIRECT cannot execute MERGE INTO query")));
    }

    if (command_type == CMD_NOTHING) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("EXECUTE DIRECT cannot execute CREATE RULE")));
    }
}

/*
 * Features not yet supported
 * DML can be launched without errors but this could compromise data
 * consistency, so block it.
 */
static void check_execute_direct_type_and_statement(ExecDirectType exec_direct_type, const Node* utility_statement, bool include_local)
{
    if (!u_sess->attr.attr_common.xc_maintenance_mode && !u_sess->attr.attr_common.IsInplaceUpgrade &&
        (exec_direct_type == EXEC_DIRECT_DELETE || exec_direct_type == EXEC_DIRECT_UPDATE ||
         exec_direct_type == EXEC_DIRECT_INSERT)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("EXECUTE DIRECT cannot execute DML queries")));
    } else if (exec_direct_type == EXEC_DIRECT_UTILITY) {
        if (include_local || (!IsExecDirectUtilityStmt(utility_statement) &&
#ifdef ENABLE_DISTRIBUTE_TEST
               // just enable the VariableSetStmt named distribute_test_param for distribute test.
               !checkExecDirectVariableSetStmt(utility_statement) &&
#endif
               !u_sess->attr.attr_common.xc_maintenance_mode)) {
                /* In case this statement is an utility, check if it is authorized */
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("EXECUTE DIRECT cannot execute this utility query")));
        }
    } else if (exec_direct_type == EXEC_DIRECT_LOCAL_UTILITY &&
#ifdef ENABLE_DISTRIBUTE_TEST
               // just enable the VariableSetStmt named distribute_test_param for distribute test.
               !checkExecDirectVariableSetStmt(utility_statement) &&
#endif
               !u_sess->attr.attr_common.xc_maintenance_mode) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("EXECUTE DIRECT cannot execute locally this utility query")));
    }

    return;
}

static Query* generate_query_execdirect_statement(const ExecDirectStmt* stmt)
{
    Query* result = makeNode(Query);
    char* query = NULL;
    StringInfoData strinfo;
    List* raw_parsetree_list = NIL;
    ListCell* raw_parsetree_item = NULL;

    //  When the EXECUTE DIRECT ON 'xxxxx' statement is executed,
    //  once the 'xxxxx' statement is in error, the error will dislocation,
    // So the space of several characters should be added to ensure the correctness of the error location.
    // eg. EXECUTE DIRECT ON 'select a;', then 'select a;' will be replaced by '                   select a;'
    initStringInfo(&strinfo);
    appendStringInfoSpaces(&strinfo, stmt->location + 1);
    appendStringInfoString(&strinfo, stmt->query);
    query = strinfo.data;

    /* Transform the query into a raw parse list */
    raw_parsetree_list = pg_parse_query(query);
    /* EXECUTE DIRECT can just be executed with a single query */
    if (list_length(raw_parsetree_list) > 1) {
        ereport(
            ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("EXECUTE DIRECT cannot execute multiple queries")));
    }

    /*
     * Analyze the Raw parse tree
     * EXECUTE DIRECT is restricted to one-step usage
     */
    foreach (raw_parsetree_item, raw_parsetree_list) {
        Node* parsetree = (Node*)lfirst(raw_parsetree_item);
        /* Not allow EXECUTE DIRECT ON recursively */
        if (IsA(parsetree, ExecDirectStmt)) {
            ereport(
                ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("EXECUTE DIRECT cannot execute recursively")));
        }

        result = parse_analyze(parsetree, query, NULL, 0);
    }

    pfree_ext(strinfo.data);
    return result;
}

static void init_execdirect_utility_stmt(RemoteQuery* step, const char* statement)
{
    step->cursor = NULL;
    step->base_tlist = NIL;
    step->force_autocommit = false;
    step->read_only = true;
    step->sql_statement = pstrdup(statement);
    step->combine_type = COMBINE_TYPE_SAME;
}

/*
 * find agg functions which need add finalize funcion on sql statement in deparse_query
 */
static bool check_agg_in_execute_direct_query(Node* node, void* context)
{
    if (node == NULL || IS_PGXC_DATANODE) {
        return false;
    }

    if (IsA(node, Aggref)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("EXECUTE DIRECT on multinode not support agg functions.")));
    }

    if (IsA(node, Query)) {
        if (query_tree_walker((Query*)node,
            (bool (*)())check_agg_in_execute_direct_query, (void*)node, 0)) {
            return true;
        }
    }

    return expression_tree_walker(node, (bool (*)())check_agg_in_execute_direct_query, context);
}

/*
 * transformExecDirectStmt -
 *	transform an EXECUTE DIRECT Statement
 *
 * Handling is depends if we should execute on nodes or on Coordinator.
 * To execute on nodes we return CMD_UTILITY query having one T_RemoteQuery node
 * with the inner statement as a sql_command.
 * If statement is to run on Coordinator we should parse inner statement and
 * analyze resulting query tree.
 */
static Query* transformExecDirectStmt(ParseState* pstate, ExecDirectStmt* stmt)
{
    Query* result = NULL;
    List* nodelist = stmt->node_names;
    ExecDirectOption option = stmt->exec_option;
    RemoteQuery* step = makeNode(RemoteQuery);
    bool is_local = false;
    bool include_local = false;
    char nodetype = PGXC_NODE_COORDINATOR;
    int node_index;

    /* Support not available on Datanodes  except single node */
    if (IS_PGXC_DATANODE && !IS_SINGLE_NODE)
        ereport(
            ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("EXECUTE DIRECT cannot be executed on a Datanode")));

    Assert(IS_PGXC_COORDINATOR || IS_SINGLE_NODE);

    step->exec_nodes = makeNode(ExecNodes);
    result = generate_query_execdirect_statement(stmt);
    if (list_length(nodelist) == 1) {
        get_index_and_type_from_nodename(strVal(linitial(nodelist)), &node_index, &nodetype);
        /* Check if node is requested is the self-node or not */
        if ((nodetype == PGXC_NODE_COORDINATOR && node_index == u_sess->pgxc_cxt.PGXCNodeId - 1) || IS_SINGLE_NODE)
            is_local = true;
        step->exec_type = fill_exec_type(nodetype, option);
        step->exec_nodes->nodeList = lappend_int(step->exec_nodes->nodeList, node_index);
    } else {
        check_agg_in_execute_direct_query((Node*)result, (void*)result);
        fill_node_list_and_exec_type(nodelist, option, step, &include_local);
        step->is_remote_function_query = true;
    }

    /* Needed by planner */
    result->sql_statement = pstrdup(stmt->query);
    init_execdirect_utility_stmt(step, result->sql_statement);

    check_command_type(result->commandType);
    step->exec_direct_type = set_exec_direct_type(is_local, result->commandType);
    check_execute_direct_type_and_statement(step->exec_direct_type, result->utilityStmt, include_local);

    /* Associate newly-created RemoteQuery node to the returned Query result */
    is_local = (is_local || include_local);
    result->is_local = is_local;
    if (!is_local || include_local) {
        result->utilityStmt = (Node*)step;
    }

    /* Not allow SELECT with normal table on CN (no data) */
    if ((step->exec_type == EXEC_ON_COORDS || (step->exec_type == EXEC_ON_ALL_NODES)) &&
        result->commandType == CMD_SELECT &&
        containing_ordinary_table((Node*)result)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("EXECUTE DIRECT cannot execute SELECT query with normal table on coordinator")));
    }

    return result;
}

/*
 * Check if given node is authorized to go through EXECUTE DIRECT
 */
static bool IsExecDirectUtilityStmt(const Node* node)
{
    bool res = true;

    if (node == NULL) {
        return res;
    }

    switch (nodeTag(node)) {
        /*
         * CREATE/DROP TABLESPACE are authorized to control
         * tablespace at single node level.
         */
        case T_CreateTableSpaceStmt:
        case T_DropTableSpaceStmt:
            res = true;
            break;
        default:
            res = false;
            break;
    }

    return res;
}

/*
 * Returns whether or not the rtable (and its subqueries)
 * contain any relation who is the parent of
 * the passed relation
 */
static bool is_relation_child(RangeTblEntry* child_rte, List* rtable)
{
    ListCell* item = NULL;

    if (child_rte == NULL || rtable == NULL) {
        return false;
    }

    if (child_rte->rtekind != RTE_RELATION) {
        return false;
    }

    foreach (item, rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(item);

        if (rte->rtekind == RTE_RELATION) {
            if (is_rel_child_of_rel(child_rte, rte)) {
                return true;
            }
        } else if (rte->rtekind == RTE_SUBQUERY) {
            return is_relation_child(child_rte, rte->subquery->rtable);
        }
    }
    return false;
}

/*
 * Returns whether the passed RTEs have a parent child relationship
 */
static bool is_rel_child_of_rel(RangeTblEntry* child_rte, RangeTblEntry* parent_rte)
{
    Oid parentOID;
    bool res = false;
    Relation relation;
    SysScanDesc scan;
    ScanKeyData key[1];
    HeapTuple inheritsTuple;
    Oid inhrelid;

    /* Does parent RT entry allow inheritance? */
    if (!parent_rte->inh) {
        return false;
    }

    /* Ignore any already-expanded UNION ALL nodes */
    if (parent_rte->rtekind != RTE_RELATION) {
        return false;
    }

    /* Fast path for common case of childless table */
    parentOID = parent_rte->relid;
    if (!has_subclass(parentOID)) {
        return false;
    }

    /* Assume we did not find any match */
    res = false;

    /* Scan pg_inherits and get all the subclass OIDs one by one. */
    relation = heap_open(InheritsRelationId, AccessShareLock);
    ScanKeyInit(&key[0], Anum_pg_inherits_inhparent, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(parentOID));
    scan = systable_beginscan(relation, InheritsParentIndexId, true, NULL, 1, key);

    while ((inheritsTuple = systable_getnext(scan)) != NULL) {
        inhrelid = ((Form_pg_inherits)GETSTRUCT(inheritsTuple))->inhrelid;
        /* Did we find the Oid of the passed RTE in one of the children? */
        if (child_rte->relid == inhrelid) {
            res = true;
            break;
        }
    }

    systable_endscan(scan);
    heap_close(relation, AccessShareLock);
    return res;
}

#endif

/*
 * Check for features that are not supported together with FOR [KEY] UPDATE/SHARE.
 *
 * exported so planner can check again after rewriting, query pullup, etc
 */
void CheckSelectLocking(Query* qry)
{
    if (qry->setOperations) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("SELECT FOR UPDATE/SHARE%s is not allowed with UNION/INTERSECT/EXCEPT",
                       NOKEYUPDATE_KEYSHARE_ERRMSG)));
    }
    if (qry->distinctClause != NIL) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("SELECT FOR UPDATE/SHARE%s is not allowed with DISTINCT clause", NOKEYUPDATE_KEYSHARE_ERRMSG)));
    }
    if (qry->groupClause != NIL) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("SELECT FOR UPDATE/SHARE%s is not allowed with GROUP BY clause", NOKEYUPDATE_KEYSHARE_ERRMSG)));
    }
    if (qry->havingQual != NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("SELECT FOR UPDATE/SHARE%s is not allowed with HAVING clause", NOKEYUPDATE_KEYSHARE_ERRMSG)));
    }
    if (qry->hasAggs) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("SELECT FOR UPDATE/SHARE%s is not allowed with aggregate functions",
                       NOKEYUPDATE_KEYSHARE_ERRMSG)));
    }
    if (qry->hasWindowFuncs) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("SELECT FOR UPDATE/SHARE%s is not allowed with window functions", NOKEYUPDATE_KEYSHARE_ERRMSG)));
    }
    if (expression_returns_set((Node*)qry->targetList)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("SELECT FOR UPDATE/SHARE%s is not allowed with set-returning functions in the target list",
                       NOKEYUPDATE_KEYSHARE_ERRMSG)));
    }
}

/*
 * Transform a FOR [KEY] UPDATE/SHARE clause
 *
 * This basically involves replacing names by integer relids.
 *
 * NB: if you need to change this, see also markQueryForLocking()
 * in rewriteHandler.c, and isLockedRefname() in parse_relation.c.
 */
static void transformLockingClause(ParseState* pstate, Query* qry, LockingClause* lc, bool pushedDown)
{
    List* lockedRels = lc->lockedRels;
    ListCell* l = NULL;
    ListCell* rt = NULL;
    Index i;
    LockingClause* allrels = NULL;
    Relation rel;

    CheckSelectLocking(qry);

    /* make a clause we can pass down to subqueries to select all rels */
    allrels = makeNode(LockingClause);
    allrels->lockedRels = NIL; /* indicates all rels */
    /* The strength of lc is not set at old version and distribution. Set it according to forUpdate. */
    if (t_thrd.proc->workingVersionNum < ENHANCED_TUPLE_LOCK_VERSION_NUM
#ifdef ENABLE_MULTIPLE_NODES
        || true
#endif
        ) {
        lc->strength = lc->forUpdate ? LCS_FORUPDATE : LCS_FORSHARE;
    }
    allrels->strength = lc->strength;
    allrels->noWait = lc->noWait;
    allrels->waitSec = lc->waitSec;

    /* The processing delay of the ProcSleep function is in milliseconds. Set the delay to int_max/1000. */
    /* The processing delay of the ProcSleep function is in milliseconds. Set the delay to int_max/1000. */
    if (lc->waitSec > (MAX_INT32 / MILLISECONDS_PER_SECONDS)) {
        ereport(ERROR,
            (errmodule(MOD_OPT_PLANNER), errcode(ERRCODE_INVALID_OPTION),
                errmsg("The delay ranges from 0 to 2147483."),
                errdetail("N/A"),
                errcause("Invalid input parameter."),
                erraction("Modify SQL statement according to the manual.")));
    }


    if (lockedRels == NIL) {
        /* all regular tables used in query */
        i = 0;
        foreach (rt, qry->rtable) {
            RangeTblEntry* rte = (RangeTblEntry*)lfirst(rt);

            ++i;
            switch (rte->rtekind) {
                case RTE_RELATION:
                    rel = relation_open(rte->relid, AccessShareLock);
                    if (RelationIsColStore(rel)) {
                        heap_close(rel, AccessShareLock);
                        ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("SELECT FOR UPDATE/SHARE%s cannot be used with "
                                       "column table \"%s\"", NOKEYUPDATE_KEYSHARE_ERRMSG, rte->eref->aliasname)));
                    }

                    heap_close(rel, AccessShareLock);

                    applyLockingClause(qry, i, lc->strength, lc->noWait, pushedDown, lc->waitSec);
                    rte->requiredPerms |= ACL_SELECT_FOR_UPDATE;
                    break;
                case RTE_SUBQUERY:
                    applyLockingClause(qry, i, lc->strength, lc->noWait, pushedDown, lc->waitSec);

                    /*
                     * FOR [KEY] UPDATE/SHARE of subquery is propagated to all of
                     * subquery's rels, too.  We could do this later (based on
                     * the marking of the subquery RTE) but it is convenient
                     * to have local knowledge in each query level about which
                     * rels need to be opened with RowShareLock.
                     */
                    transformLockingClause(pstate, rte->subquery, allrels, true);
                    break;
                case RTE_CTE:
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("SELECT FOR UPDATE/SHARE cannot be applied to a WITH query")));
                    break;
                default:
                    /* ignore JOIN, SPECIAL, FUNCTION, VALUES, CTE RTEs */
                    break;
            }
        }
    } else {
        /* just the named tables */
        foreach (l, lockedRels) {
            RangeVar* thisrel = (RangeVar*)lfirst(l);

            /* For simplicity we insist on unqualified alias names here */
            if (thisrel->catalogname || thisrel->schemaname) {
                ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("SELECT FOR UPDATE/SHARE%s must specify unqualified "
                               "relation names", NOKEYUPDATE_KEYSHARE_ERRMSG),
                        parser_errposition(pstate, thisrel->location)));
            }

            i = 0;
            foreach (rt, qry->rtable) {
                RangeTblEntry* rte = (RangeTblEntry*)lfirst(rt);

                ++i;
                if (strcmp(rte->eref->aliasname, thisrel->relname) == 0) {
                    switch (rte->rtekind) {
                        case RTE_RELATION:
                            rel = relation_open(rte->relid, AccessShareLock);
                            if (RelationIsColStore(rel)) {
                                heap_close(rel, AccessShareLock);
                                ereport(ERROR,
                                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                        errmsg("SELECT FOR UPDATE/SHARE%s cannot be used with column table \"%s\"",
                                               NOKEYUPDATE_KEYSHARE_ERRMSG, rte->eref->aliasname),
                                        parser_errposition(pstate, thisrel->location)));
                            }

                            heap_close(rel, AccessShareLock);
                            applyLockingClause(qry, i, lc->strength, lc->noWait, pushedDown,
                                               lc->waitSec);
                            rte->requiredPerms |= ACL_SELECT_FOR_UPDATE;
                            break;
                        case RTE_SUBQUERY:
                            applyLockingClause(qry, i, lc->strength, lc->noWait, pushedDown,
                                               lc->waitSec);
                            /* see comment above */
                            transformLockingClause(pstate, rte->subquery, allrels, true);
                            break;
                        case RTE_JOIN:
                            ereport(ERROR,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmsg("SELECT FOR UPDATE/SHARE%s cannot be applied to a join",
                                           NOKEYUPDATE_KEYSHARE_ERRMSG),
                                    parser_errposition(pstate, thisrel->location)));
                            break;
                        case RTE_FUNCTION:
                            ereport(ERROR,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmsg("SELECT FOR UPDATE/SHARE%s cannot be applied to a function",
                                           NOKEYUPDATE_KEYSHARE_ERRMSG),
                                    parser_errposition(pstate, thisrel->location)));
                            break;
                        case RTE_VALUES:
                            ereport(ERROR,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmsg("SELECT FOR UPDATE/SHARE%s cannot be applied to VALUES",
                                           NOKEYUPDATE_KEYSHARE_ERRMSG),
                                    parser_errposition(pstate, thisrel->location)));
                            break;
                        case RTE_CTE:
                            ereport(ERROR,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmsg("SELECT FOR UPDATE/SHARE%s cannot be applied to a WITH query",
                                           NOKEYUPDATE_KEYSHARE_ERRMSG),
                                    parser_errposition(pstate, thisrel->location)));
                            break;
                        default:
                            ereport(ERROR,
                                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                                    errmsg("unrecognized RTE type: %d", (int)rte->rtekind)));
                            break;
                    }
                    break; /* out of foreach loop */
                }
            }
            if (rt == NULL) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_TABLE),
                        errmsg("relation \"%s\" in FOR UPDATE/SHARE//NO KEY UPDATE/KEY SHARE clause not found "
                               "in FROM clause", thisrel->relname),
                        parser_errposition(pstate, thisrel->location)));
            }
        }
    }
}

/*
 * Record locking info for a single rangetable item
 */
void applyLockingClause(Query* qry, Index rtindex, LockClauseStrength strength, bool noWait, bool pushedDown,
                        int waitSec)
{
    RowMarkClause* rc = NULL;

    /* If it's an explicit clause, make sure hasForUpdate gets set */
    if (!pushedDown) {
        qry->hasForUpdate = true;
    }

    /* Check for pre-existing entry for same rtindex */
    if ((rc = get_parse_rowmark(qry, rtindex)) != NULL) {
        /*
         * If the same RTE is specified for more than one locking strength,
         * treat is as the strongest.  (Reasonable, since you can't take both a
         * shared and exclusive lock at the same time; it'll end up being
         * exclusive anyway.)
         *
         * We also consider that NOWAIT wins if it's specified both ways. This
         * is a bit more debatable but raising an error doesn't seem helpful.
         * (Consider for instance SELECT FOR UPDATE NOWAIT from a view that
         * internally contains a plain FOR UPDATE spec.)
         *
         * And of course pushedDown becomes false if any clause is explicit.
         */
        rc->strength = Max(rc->strength, strength);
        rc->forUpdate = rc->strength == LCS_FORUPDATE;
        rc->noWait = rc->noWait || noWait;
        rc->waitSec = Max(rc->waitSec, waitSec);
        rc->pushedDown = rc->pushedDown && pushedDown;
        return;
    }

    /* Make a new RowMarkClause */
    rc = makeNode(RowMarkClause);
    rc->rti = rtindex;
    rc->forUpdate = strength == LCS_FORUPDATE;
    rc->strength = strength;
    rc->noWait = noWait;
    rc->waitSec = waitSec;
    rc->pushedDown = pushedDown;
    qry->rowMarks = lappend(qry->rowMarks, rc);
}

/*
 * Check if there is foreign table that is included in one insert statement.
 *
 * input:
 * 		rte_table_list: the list of p_rtable of ParseState.
 *
 * output:
 * 		true: there is foreign table included in insert statement.
 */
bool checkForeignTableExist(List* rte_table_list)
{
    bool ret_val = false;
    ListCell* lc = NULL;
    RangeTblEntry* rte = NULL;

    if (rte_table_list == NULL) {
        return ret_val;
    }

    foreach (lc, rte_table_list) {
        rte = (RangeTblEntry*)lfirst(lc);
        if (rte->relkind == RELKIND_FOREIGN_TABLE || rte->relkind == RELKIND_STREAM) {
            ret_val = true;
            break;
        }
    }

    return ret_val;
}

/*
 * Function name: set_subquery_is_under_insert
 * 		set p_is_in_insert for paserstate, to mark this is sub query under one insert.
 *
 * input:
 * 		subParseState: ParseState of sub query.
 *
 *
 * output:
 * 		void
 */
void set_subquery_is_under_insert(ParseState* subParseState)
{
    ParseState* ancestor_ps = NULL;
    ParseState* temp_ps = NULL;

    if (unlikely(subParseState == NULL)) {
        ereport(ERROR, 
            (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), 
                errmsg("subParseState should not be null")));
    }

    /* If the subParseState's parentParseState is NULL then do nothing. */
    if (subParseState->parentParseState == NULL) {
        return;
    }

    /* Find top level parsestate. */
    temp_ps = subParseState;
    while (temp_ps != NULL) {
        if (temp_ps->parentParseState != NULL) {
            ancestor_ps = temp_ps->parentParseState;
            /* If this is a insert statement. then mark sub parsestate is under insert. */
            if (ancestor_ps->p_is_insert) {
                subParseState->p_is_in_insert = ancestor_ps->p_is_insert;
                break;
            }
        }
        temp_ps = temp_ps->parentParseState;
    }
}

/*
 * Function name: set_ancestor_ps_contain_foreigntbl
 * 		When sub query find a foreign table. call this function to set top level parse
 * 		state that is insert type to set p_is_foreignTbl_exist to true;
 * Input:
 * 		subParseState: the ParseState of sub query.
 * Output:
 * 		void
 */
void set_ancestor_ps_contain_foreigntbl(ParseState* subParseState)
{
    ParseState* ancestor_ps = NULL;
    ParseState* temp_ps = NULL;

    if (unlikely(subParseState == NULL)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), 
                errmsg("subParseState should not be null")));
    }

    /* If the subParseState's parentParseState is NULL then do nothing. */
    if (subParseState->parentParseState == NULL) {
        return;
    }

    /* We go back through parentParseState to find top level parseState. */
    temp_ps = subParseState;
    while (temp_ps != NULL) {
        if (temp_ps->parentParseState != NULL) {
            ancestor_ps = temp_ps->parentParseState;
            /*
             * Because there should at least one insert statement on top of
             * this select statement, we mark find foreign table in all top level
             * insert statements.
             */
            if (ancestor_ps->p_is_insert) {
                ancestor_ps->p_is_foreignTbl_exist = true;
            }
        }
        temp_ps = temp_ps->parentParseState;
    }
}

/*
 * @Description: This group clause if include groupingSet.
 * @in groupClause - group clause.
 * @return - If include return true else return false.
 */
static bool include_groupingset(Node* groupClause)
{
    if (groupClause == NULL) {
        return false;
    }

    if (IsA(groupClause, List)) {
        List* group_list = (List*)groupClause;

        ListCell* lc = NULL;

        foreach (lc, group_list) {
            Node* node = (Node*)lfirst(lc);

            if (include_groupingset(node)) {
                return true;
            }
        }
    } else if (IsA(groupClause, GroupingSet)) {
        return true;
    }

    return false;
}

/*
 * @Description: Transform const expr in groupClause to columnref struct.
 * @in groupClause - group clause.
 * @in targetList - query targetlist.
 */
static void transformGroupConstToColumn(ParseState* pstate, Node* groupClause, List* targetList)
{
    if (groupClause == NULL) {
        return;
    }

    if (IsA(groupClause, List)) {
        List* group_list = (List*)groupClause;

        ListCell* lc = NULL;

        foreach (lc, group_list) {
            Node* node = (Node*)lfirst(lc);

            /* Replace this const by expr in targetlist. */
            if (IsA(node, A_Const)) {
                Value* val = &((A_Const*)node)->val;
                int location = ((A_Const*)node)->location;

                if (!IsA(val, Integer)) {
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("non-integer constant in group clause"),
                            parser_errposition(pstate, location)));
                }

                long target_pos = intVal(val);
                if (target_pos <= list_length(targetList)) {
                    TargetEntry* tle = (TargetEntry*)list_nth(targetList, target_pos - 1);
                    lfirst(lc) = copyObject(tle->expr);

                    pfree_ext(node);
                } else {
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                            errmsg("%s position %ld is not in select list", "GROUP BY", target_pos),
                            parser_errposition(pstate, location)));
                }

            } else {
                transformGroupConstToColumn(pstate, node, targetList);
            }
        }
    } else if (IsA(groupClause, GroupingSet)) {
        GroupingSet* grouping_set = (GroupingSet*)groupClause;
        transformGroupConstToColumn(pstate, (Node*)grouping_set->content, targetList);
    }
}

static bool checkAllowedTableCombination(ParseState* pstate)
{
    RangeTblEntry* rte = NULL;
    ListCell* lc = NULL;
    bool has_ustore = false;
    bool has_else = false;

    // Check range table list for the presence of
    // relations with different storages
    foreach(lc, pstate->p_rtable) {
        rte = (RangeTblEntry*)lfirst(lc);
        if (rte && rte->rtekind == RTE_RELATION) {
            if (rte->is_ustore) {
                has_ustore = true;
            } else {
                has_else = true;
            }
        }
    }

    // Check target table for the type of storage
    rte = pstate->p_target_rangetblentry;
    if (rte && rte->rtekind == RTE_RELATION) {
        if (rte->is_ustore) {
            has_ustore = true;
        } else {
            has_else = true;
        }
    }

    Assert(has_ustore || has_else);

    return !(has_ustore && has_else);
}
