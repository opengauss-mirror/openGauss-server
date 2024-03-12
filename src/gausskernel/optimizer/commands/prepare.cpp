/* -------------------------------------------------------------------------
 *
 * prepare.cpp
 *	  Prepareable SQL statements via PREPARE, EXECUTE and DEALLOCATE
 *
 * This module also implements storage of prepared statements that are
 * accessed via the extended FE/BE query protocol.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Copyright (c) 2002-2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/commands/prepare.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h" 
#include "knl/knl_variable.h"
#include "access/printtup.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "catalog/pg_proc.h"
#include "commands/createas.h"
#include "commands/prepare.h"
#include "executor/lightProxy.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "opfusion/opfusion.h"
#include "optimizer/bucketpruning.h"
#include "parser/analyze.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_type.h"
#include "pgstat.h"
#include "rewrite/rewriteHandler.h"
#include "tcop/pquery.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/dynahash.h"
#include "utils/globalplancache.h"
#include "utils/globalplancore.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#ifdef PGXC
#include "pgxc/pgxc.h"
#include "nodes/nodes.h"
#include "pgxc/nodemgr.h"
#include "pgxc/execRemote.h"
#include "catalog/pgxc_node.h"
#endif
#include "replication/walreceiver.h"
#include "optimizer/gplanmgr.h"
#ifdef ENABLE_MOT
#include "storage/mot/jit_exec.h"
#endif

#define CLUSTER_EXPANSION_BASE 2

void InitQueryHashTable(void);
static ParamListInfo EvaluateParams(CachedPlanSource* psrc, List* params, const char* queryString, EState* estate);
static Datum build_regtype_array(const Oid* param_types, int num_params);

extern void destroy_handles();

static void CopyPlanForGPCIfNecessary(CachedPlanSource* psrc, Portal portal)
{
    MemoryContext tmpCxt = NULL;
    bool needCopy = ENABLE_GPC && psrc->gplan;
    if (needCopy) {
        portal->stmts = CopyLocalStmt(portal->cplan->stmt_list, u_sess->temp_mem_cxt, &tmpCxt);
    }
}

#ifdef ENABLE_MOT
void TryMotJitCodegenQuery(const char* queryString, CachedPlanSource* psrc, Query* query)
{
    // Try to generate LLVM jitted code - first cleanup jit of previous run.
    if (psrc->mot_jit_context != NULL) {
        if (JitExec::IsJitContextPendingCompile(psrc->mot_jit_context) ||
            JitExec::IsJitContextDoneCompile(psrc->mot_jit_context)) {
            return;
        }

        // NOTE: context is cleaned up during end of session, this should not happen,
        // maybe a warning should be issued
        Assert(false);
        ereport(WARNING, (errmsg("Cached Plan Source already has a MOT JIT Context, destroying the residual context")));
        JitExec::DestroyJitContext(psrc->mot_jit_context, true);
        psrc->mot_jit_context = NULL;
        Assert(psrc->opFusionObj == NULL);
    }

    if (query == NULL) {
        if (list_length(psrc->query_list) != 1) {
            elog(DEBUG2, "Plan source does not have exactly one query");
            return;
        }
        query = (Query*)linitial(psrc->query_list);
        if (query == NULL) {
            elog(DEBUG2, "No query object present for MOT JIT");
            return;
        }
    }

    if ((query->commandType != CMD_SELECT) && (query->commandType != CMD_INSERT) &&
        (query->commandType != CMD_UPDATE) && (query->commandType != CMD_DELETE)) {
        elog(DEBUG2, "Query is not SELECT|INSERT|UPDATE|DELETE");
        return;
    }

    if (JitExec::IsMotCodegenPrintEnabled()) {
        elog(LOG, "Attempting to generate MOT jitted code for query: %s\n", queryString);
    }

    Assert(psrc->opFusionObj == NULL && psrc->mot_jit_context == NULL);
    u_sess->mot_cxt.jit_codegen_error = 0;
    psrc->mot_jit_context = JitExec::TryJitCodegenQuery(query, queryString);
    if (psrc->mot_jit_context != NULL) {
        if (JitExec::IsJitContextValid(psrc->mot_jit_context)) {
            psrc->is_checked_opfusion = false;
        }
    } else {
        if (JitExec::IsMotCodegenPrintEnabled()) {
            elog(LOG, "Failed to generate jitted MOT function for query %s\n", queryString);
        }
        if (u_sess->mot_cxt.jit_codegen_error == ERRCODE_QUERY_CANCELED) {
            // If JIT compilation failed due to cancel request, we need to ereport. JIT source will be in error state,
            // but checkedMotJitCodegen will still be false so that the JIT compilation will be triggered on next
            // attempt.
            Assert(!psrc->checkedMotJitCodegen);
            ereport(ERROR, (errcode(ERRCODE_QUERY_CANCELED), errmsg("canceling statement due to user request")));
        }
    }
}
#endif
/*
 * User_defined variables is a string in prepareStmt.
 * Get selectStmt/insertStmt/updateStmt/deleteStmt/mergeStmt from user_defined variables by pg_parse_query.
 * Then, execute SQL: PREPARE stmt AS selectStmt/insertStmt/updateStmt/deleteStmt/mergeStmt.
 */
static void QueryRewritePrepareStmt(Node* parsetree)
{
    char *sqlstr = NULL;
    List* raw_parsetree_list = NIL;
    PrepareStmt *stmt = (PrepareStmt *)parsetree;
    ParseState* state = make_parsestate(NULL);
    UserVar *uservar = (UserVar *)transformExpr(state, (Node *)stmt->query, EXPR_KIND_EXECUTE_PARAMETER);
    free_parsestate(state);
    Const* value = (Const *)uservar->value;

    if (value->consttype != TEXTOID) {
        ereport(ERROR,
            (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("userdefined variable in prepare statement must be text type.")));
    }
    if (value->constvalue == (Datum)0) {
        ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("Query was empty")));
    }

    sqlstr = TextDatumGetCString(value->constvalue);

    raw_parsetree_list = pg_parse_query(sqlstr);
    if (raw_parsetree_list == NIL) {
        ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("Query was empty")));
    }

    if (raw_parsetree_list->length != 1) {
        ereport(ERROR,
            (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("prepare user_defined variable can contain only one SQL statement.")));
    }

    switch (nodeTag(linitial(raw_parsetree_list))) {
        case T_SelectStmt:
        case T_InsertStmt:
        case T_UpdateStmt:
        case T_DeleteStmt:
        case T_MergeStmt:
            stmt->query = (Node *)copyObject((Node *)linitial(raw_parsetree_list));
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("the statement in prepare is not supported.")));
            break;
    }

    return ;
}

/*
 * Implements the 'PREPARE' utility statement.
 */
void PrepareQuery(PrepareStmt* stmt, const char* queryString)
{
    CachedPlanSource* plansource = NULL;
    Oid* argtypes = NULL;
    int nargs;
    Query* query = NULL;
    List* query_list = NIL;
    int i;

    /*
     * Disallow empty-string statement name (conflicts with protocol-level
     * unnamed statement).
     */
    if (!stmt->name || stmt->name[0] == '\0')
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PSTATEMENT_DEFINITION), errmsg("invalid statement name: must not be empty")));

    if (IsA(stmt->query, UserVar)) {
        QueryRewritePrepareStmt((Node*)stmt);
    }
    /*
     * Create the CachedPlanSource before we do parse analysis, since it needs
     * to see the unmodified raw parse tree.
     */
    plansource = CreateCachedPlan(stmt->query,
        queryString,
#ifdef PGXC
        stmt->name,
#endif
        CreateCommandTag(stmt->query));
    t_thrd.postgres_cxt.cur_command_tag = transform_node_tag(stmt->query);

    /* Transform list of TypeNames to array of type OIDs */
    nargs = list_length(stmt->argtypes);

    if (nargs) {
        ParseState* pstate = NULL;
        ListCell* l = NULL;

        /*
         * typenameTypeId wants a ParseState to carry the source query string.
         * Is it worth refactoring its API to avoid this?
         */
        pstate = make_parsestate(NULL);
        pstate->p_sourcetext = queryString;

        argtypes = (Oid*)palloc(nargs * sizeof(Oid));
        i = 0;

        foreach (l, stmt->argtypes) {
            TypeName* tn = (TypeName*)lfirst(l);
            Oid toid = typenameTypeId(pstate, tn);

            argtypes[i++] = toid;
        }
    }

    /*
     * Analyze the statement using these parameter types (any parameters
     * passed in from above us will not be visible to it), allowing
     * information about unknown parameters to be deduced from context.
     */

    query = parse_analyze_varparams(stmt->query, queryString, &argtypes, &nargs);

#ifdef ENABLE_MOT
    /* check cross engine queries  */
    StorageEngineType storageEngineType = SE_TYPE_UNSPECIFIED;
    CheckTablesStorageEngine(query, &storageEngineType);
    SetCurrentTransactionStorageEngine(storageEngineType);
    /* set the plan's storage engine */
    plansource->storageEngineType = storageEngineType;

    /* gpc does not support MOT engine */
    if (ENABLE_CN_GPC && plansource->gpc.status.IsSharePlan() &&
        (storageEngineType == SE_TYPE_MOT || storageEngineType == SE_TYPE_MIXED)) {
        plansource->gpc.status.SetKind(GPC_UNSHARED);
    }
#endif

    if (ENABLE_CN_GPC && plansource->gpc.status.IsSharePlan() && contains_temp_tables(query->rtable)) {
        /* temp table unsupport shared */
        plansource->gpc.status.SetKind(GPC_UNSHARED);
    }

    /*
     * Check that all parameter types were determined.
     */
    for (i = 0; i < nargs; i++) {
        Oid argtype = argtypes[i];

        if (argtype == InvalidOid || argtype == UNKNOWNOID)
            ereport(ERROR,
                (errcode(ERRCODE_INDETERMINATE_DATATYPE),
                    errmsg("could not determine data type of parameter $%d", i + 1)));
    }

    /*
     * grammar only allows OptimizableStmt, so this check should be redundant
     */
    switch (query->commandType) {
        case CMD_SELECT:
        case CMD_INSERT:
        case CMD_UPDATE:
        case CMD_DELETE:
        case CMD_MERGE:
            /* OK */
            break;
        case CMD_UTILITY:
            if (IsA(query->utilityStmt, VariableMultiSetStmt) ||
                IsA(query->utilityStmt, CopyStmt)) {
                break;
            }
        default:
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PSTATEMENT_DEFINITION), errmsg("utility statements cannot be prepared")));
            break;
    }

    /* Rewrite the query. The result could be 0, 1, or many queries. */
    query_list = QueryRewrite(query);

    /* Finish filling in the CachedPlanSource */
    CompleteCachedPlan(plansource,
        query_list,
        NULL,
        argtypes,
        NULL,
        nargs,
        NULL,
        NULL,
        0,    /* default cursor options */
        true, /* fixed result */
        stmt->name);

    /*
     * Save the results.
     */
    StorePreparedStatement(stmt->name, plansource, true);

#ifdef ENABLE_MOT
    // Try MOT JIT code generation only after the plan source is saved.
    if ((plansource->storageEngineType == SE_TYPE_MOT || plansource->storageEngineType == SE_TYPE_UNSPECIFIED) &&
        !IS_PGXC_COORDINATOR && JitExec::IsMotCodegenEnabled()) {
        // MOT JIT code generation
        TryMotJitCodegenQuery(queryString, plansource, query);
    }
#endif
}

/*
 * ExecuteQuery --- implement the 'EXECUTE' utility statement.
 *
 * This code also supports CREATE TABLE ... AS EXECUTE.  That case is
 * indicated by passing a non-null intoClause.	The DestReceiver is already
 * set up correctly for CREATE TABLE AS, but we still have to make a few
 * other adjustments here.
 *
 * Note: this is one of very few places in the code that needs to deal with
 * two query strings at once.  The passed-in queryString is that of the
 * EXECUTE, which we might need for error reporting while processing the
 * parameter expressions.  The query_string that we copy from the plan
 * source is that of the original PREPARE.
 */
void ExecuteQuery(ExecuteStmt* stmt, IntoClause* intoClause, const char* queryString, ParamListInfo params,
    DestReceiver* dest, char* completionTag)
{
    PreparedStatement *entry = NULL;
    CachedPlan* cplan = NULL;
    List* plan_list = NIL;
    ParamListInfo paramLI = NULL;
    EState* estate = NULL;
    Portal portal;
    char* query_string = NULL;
    int eflags;
    long count;
    CachedPlanSource* psrc = NULL;

    /* Look it up in the hash table */
    entry = FetchPreparedStatement(stmt->name, true, true);
    psrc = entry->plansource;
    t_thrd.postgres_cxt.cur_command_tag = transform_node_tag(psrc->raw_parse_tree);

    /* Shouldn't find a non-fixed-result cached plan */
    if (!entry->plansource->fixed_result)
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("EXECUTE does not support variable-result cached plans")));

    /* Evaluate parameters, if any */
    if (entry->plansource->num_params > 0) {
        /*
         * Need an EState to evaluate parameters; must not delete it till end
         * of query, in case parameters are pass-by-reference.	Note that the
         * passed-in "params" could possibly be referenced in the parameter
         * expressions.
         */
        estate = CreateExecutorState();
        estate->es_param_list_info = params;
        paramLI = EvaluateParams(psrc, stmt->params, queryString, estate);
    }

    OpFusion::clearForCplan((OpFusion*)psrc->opFusionObj, psrc);

#ifdef ENABLE_MOT
    /*
     * MOT JIT Execution:
     * Assist in distinguishing query boundaries in case of range query when client uses batches. This allows us to
     * know a new query started, and in case a previous execution did not fetch all records (since user is working in
     * batch-mode, and can decide to quit fetching in the middle), using this information we can infer this is a new
     * scan, and old scan state should be discarded.
     */
    if (psrc->mot_jit_context != NULL) {
        JitResetScan(psrc->mot_jit_context);
    }
#endif

    if (psrc->opFusionObj != NULL) {
        Assert(psrc->cplan == NULL);
        (void)RevalidateCachedQuery(psrc);
    }

    if (psrc->opFusionObj != NULL) {
        OpFusion *opFusionObj = (OpFusion *)(psrc->opFusionObj);
        if (opFusionObj->IsGlobal()) {
            opFusionObj = (OpFusion *)OpFusion::FusionFactory(opFusionObj->m_global->m_type,
                                                              u_sess->cache_mem_cxt, psrc, NULL, paramLI);
            Assert(opFusionObj != NULL);
        }
        opFusionObj->setPreparedDestReceiver(dest);
        opFusionObj->useOuterParameter(paramLI);
        opFusionObj->setCurrentOpFusionObj(opFusionObj);

        CachedPlanSource* cps = opFusionObj->m_global->m_psrc;
        bool needBucketId = cps != NULL && cps->gplan;
        if (needBucketId) {
            setCachedPlanBucketId(cps->gplan, paramLI);
        }

        if (OpFusion::process(FUSION_EXECUTE, NULL, completionTag, false, NULL)) {
            return;
        }
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Bypass process Failed")));
    }

    /* Create a new portal to run the query in */
    portal = CreateNewPortal();
    /* Don't display the portal in pg_cursors, it is for internal use only */
    portal->visible = false;

    /* Copy the plan's saved query string into the portal's memory */
    query_string = MemoryContextStrdup(PortalGetHeapMemory(portal), entry->plansource->query_string);

    if (!intoClause) {
        psrc->cursor_options |= CURSOR_OPT_SPQ_OK;
    }

    /* Replan if needed, and increment plan refcount for portal */
    if (ENABLE_CACHEDPLAN_MGR) {
        cplan = GetWiseCachedPlan(psrc, paramLI, false);
    } else {
        cplan = GetCachedPlan(psrc, paramLI, false);
    }

    plan_list = cplan->stmt_list;

    /* 
    * Now we can define the portal.
    *
    * DO NOT put any code that could possibly throw an error between the
    * above GetCachedPlan call and here.
    */
    PortalDefineQuery(portal, NULL, query_string, entry->plansource->commandTag, plan_list, cplan);
    portal->nextval_default_expr_type = psrc->nextval_default_expr_type;

    /* incase change shared plan in execute stage */
    CopyPlanForGPCIfNecessary(entry->plansource, portal);

    /*
     * For CREATE TABLE ... AS EXECUTE, we must verify that the prepared
     * statement is one that produces tuples.  Currently we insist that it be
     * a plain old SELECT.	In future we might consider supporting other
     * things such as INSERT ... RETURNING, but there are a couple of issues
     * to be settled first, notably how WITH NO DATA should be handled in such
     * a case (do we really want to suppress execution?) and how to pass down
     * the OID-determining eflags (PortalStart won't handle them in such a
     * case, and for that matter it's not clear the executor will either).
     *
     * For CREATE TABLE ... AS EXECUTE, we also have to ensure that the proper
     * eflags and fetch count are passed to PortalStart/PortalRun.
     */
    if (intoClause != NULL) {
        PlannedStmt* pstmt = NULL;

        if (list_length(plan_list) != 1)
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("prepared statement is not a SELECT")));
        pstmt = (PlannedStmt*)linitial(plan_list);
        if (!IsA(pstmt, PlannedStmt) || pstmt->commandType != CMD_SELECT || pstmt->utilityStmt != NULL)
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("prepared statement is not a SELECT")));

        /* Set appropriate eflags */
        eflags = GetIntoRelEFlags(intoClause);

        /* And tell PortalRun whether to run to completion or not */
        if (intoClause->skipData)
            count = 0;
        else
            count = FETCH_ALL;
    } else {
        /* Plain old EXECUTE */
        eflags = 0;
        count = FETCH_ALL;
    }

    if (OpFusion::IsSqlBypass(psrc, plan_list)) {
        psrc->opFusionObj =
            OpFusion::FusionFactory(OpFusion::getFusionType(cplan, paramLI, NULL),
                                    u_sess->cache_mem_cxt, psrc, NULL, paramLI);
        psrc->is_checked_opfusion = true;
        if (psrc->opFusionObj != NULL) {
            ((OpFusion*)psrc->opFusionObj)->setPreparedDestReceiver(dest);
            ((OpFusion*)psrc->opFusionObj)->useOuterParameter(paramLI);
            ((OpFusion*)psrc->opFusionObj)->setCurrentOpFusionObj((OpFusion*)psrc->opFusionObj);

            if (OpFusion::process(FUSION_EXECUTE, NULL, completionTag, false, NULL)) {
                return;
            }
            Assert(0);
        }
    }

    /*
     * Run the portal as appropriate.
     */
    PortalStart(portal, paramLI, eflags, GetActiveSnapshot());

    (void)PortalRun(portal, count, false, dest, dest, completionTag);

    PortalDrop(portal, false);

    if (estate != NULL)
        FreeExecutorState(estate);

    /* No need to pfree other memory, MemoryContext will be reset */
}

/*
 * EvaluateParams: evaluate a list of parameters.
 *
 * pstmt: statement we are getting parameters for.
 * params: list of given parameter expressions (raw parser output!)
 * queryString: source text for error messages.
 * estate: executor state to use.
 *
 * Returns a filled-in ParamListInfo -- this can later be passed to
 * CreateQueryDesc(), which allows the executor to make use of the parameters
 * during query execution.
 */
static ParamListInfo EvaluateParams(CachedPlanSource* psrc, List* params, const char* queryString, EState* estate)
{
    Oid* param_types = psrc->param_types;
    int num_params = psrc->num_params;
    int nparams = list_length(params);
    ParseState* pstate = NULL;
    ParamListInfo paramLI;
    List* exprstates = NIL;
    ListCell* l = NULL;
    Oid param_collation;
    int param_charset;
    int i;

    if (nparams != num_params)
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("wrong number of parameters for prepared statement \"%s\"", psrc->stmt_name),
                errdetail("Expected %d parameters but got %d.", num_params, nparams)));

    /* Quick exit if no parameters */
    if (num_params == 0)
        return NULL;

    /*
     * We have to run parse analysis for the expressions.  Since the parser is
     * not cool about scribbling on its input, copy first.
     */
    params = (List*)copyObject(params);

    pstate = make_parsestate(NULL);
    pstate->p_sourcetext = queryString;

    param_collation = GetCollationConnection();
    param_charset = GetCharsetConnection();
    i = 0;
    foreach (l, params) {
        Node* expr = (Node*)lfirst(l);
        Oid expected_type_id = param_types[i];
        Oid given_type_id;

        expr = transformExpr(pstate, expr, EXPR_KIND_EXECUTE_PARAMETER);

        /* Cannot contain subselects or aggregates */
        if (pstate->p_hasSubLinks)
            ereport(
                ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot use subquery in EXECUTE parameter")));
        if (pstate->p_hasAggs)
            ereport(
                ERROR, (errcode(ERRCODE_GROUPING_ERROR), errmsg("cannot use aggregate function in EXECUTE parameter")));
        if (pstate->p_hasWindowFuncs)
            ereport(
                ERROR, (errcode(ERRCODE_WINDOWING_ERROR), errmsg("cannot use window function in EXECUTE parameter")));

        given_type_id = exprType(expr);

        expr = coerce_to_target_type(
            pstate, expr, given_type_id, expected_type_id, -1, COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST, -1);

        if (expr == NULL)
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("parameter $%d of type %s cannot be coerced to the expected type %s",
                        i + 1,
                        format_type_be(given_type_id),
                        format_type_be(expected_type_id)),
                    errhint("You will need to rewrite or cast the expression.")));

        /* Take care of collations in the finished expression. */
        assign_expr_collations(pstate, expr);

        /* Try convert expression to target parameter charset. */
        if (OidIsValid(param_collation) && IsSupportCharsetType(expected_type_id)) {
            /* convert charset only, expression will be evaluated below */
            expr = coerce_to_target_charset(expr, param_charset, expected_type_id, -1, param_collation, false);
        }

        lfirst(l) = expr;
        i++;
    }

    /* Prepare the expressions for execution */

    paramLI = (ParamListInfo)palloc(offsetof(ParamListInfoData, params) + num_params * sizeof(ParamExternData));
    /* we have static list of params, so no hooks needed */
    paramLI->paramFetch = NULL;
    paramLI->paramFetchArg = NULL;
    paramLI->parserSetup = NULL;
    paramLI->parserSetupArg = NULL;
    paramLI->params_need_process = false;
    paramLI->numParams = num_params;
    paramLI->uParamInfo = DEFUALT_INFO;
    paramLI->params_lazy_bind = false;
    bool isInsertConst = IsA(psrc->raw_parse_tree, InsertStmt);
    foreach (l, params) {
        if (!IsA(lfirst(l), Const)) {
            isInsertConst = false;
            break;
        }
    }
    i = 0;
    if (isInsertConst) {
        foreach (l, params) {
            Const* e = (Const*)lfirst(l);
            ParamExternData* prm = &paramLI->params[i];

            prm->ptype = param_types[i];
            prm->pflags = PARAM_FLAG_CONST;
            prm->value = e->constvalue;
            prm->isnull = e->constisnull;
            prm->tabInfo = NULL;
            i++;
        }
    } else {
        exprstates = ExecPrepareExprList(params, estate);
        foreach (l, exprstates) {
            ExprState* n = (ExprState*)lfirst(l);
            ParamExternData* prm = &paramLI->params[i];

            prm->ptype = param_types[i];
            prm->pflags = PARAM_FLAG_CONST;
            prm->value = ExecEvalExprSwitchContext(n, GetPerTupleExprContext(estate), &prm->isnull);
            prm->tabInfo = NULL;

            i++;
        }
    }
    

    return paramLI;
}

/*
 * Initialize query hash table upon first use.
 */
void InitQueryHashTable(void)
{
    HASHCTL hash_ctl;
    errno_t rc = 0;

    rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check(rc, "\0", "\0");

    hash_ctl.keysize = NAMEDATALEN;
    hash_ctl.entrysize = sizeof(PreparedStatement);
    hash_ctl.hcxt = u_sess->cache_mem_cxt;
    
    PG_TRY();
    {
        (void)syscalllockAcquire(&u_sess->pcache_cxt.pstmt_htbl_lock);
        u_sess->pcache_cxt.prepared_queries = hash_create("Prepared Queries", 32, &hash_ctl, HASH_ELEM | HASH_CONTEXT);
        (void)syscalllockRelease(&u_sess->pcache_cxt.pstmt_htbl_lock);
    }
    PG_CATCH();
    {
        (void)syscalllockRelease(&u_sess->pcache_cxt.pstmt_htbl_lock);
        PG_RE_THROW();
    }
    PG_END_TRY();

#ifdef PGXC
    if (IS_PGXC_COORDINATOR) {
        rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
        securec_check(rc, "\0", "\0");

        hash_ctl.keysize = NAMEDATALEN;
        hash_ctl.entrysize = sizeof(DatanodeStatement);
        hash_ctl.hcxt = u_sess->cache_mem_cxt;

        u_sess->pcache_cxt.datanode_queries = hash_create("Datanode Queries", 64, &hash_ctl, HASH_ELEM | HASH_CONTEXT);
    }
#endif
    Assert(u_sess->pcache_cxt.prepared_queries);

    if (!ENABLE_THREAD_POOL) {
        Assert(t_thrd.shemem_ptr_cxt.MyBEEntry->my_prepared_queries == NULL);
        t_thrd.shemem_ptr_cxt.MyBEEntry->my_prepared_queries = u_sess->pcache_cxt.prepared_queries;
        t_thrd.shemem_ptr_cxt.MyBEEntry->my_pstmt_htbl_lock = &u_sess->pcache_cxt.pstmt_htbl_lock;
    }
}


static void InsertIntoQueryHashTable(const char* stmt_name, CachedPlanSource* plansource, bool from_sql, bool* found)
{
    PreparedStatement* entry = NULL;
    PG_TRY();
    {
        (void)syscalllockAcquire(&u_sess->pcache_cxt.pstmt_htbl_lock);
        entry = (PreparedStatement*)hash_search(u_sess->pcache_cxt.prepared_queries, stmt_name, HASH_ENTER, found);
        (void)syscalllockRelease(&u_sess->pcache_cxt.pstmt_htbl_lock);
    }
    PG_CATCH();
    {
        (void)syscalllockRelease(&u_sess->pcache_cxt.pstmt_htbl_lock);
        PG_RE_THROW();
    }
    PG_END_TRY();

    if (!(*found)) {
        entry->plansource = plansource;
        entry->from_sql = from_sql;
        entry->prepare_time = GetCurrentStatementStartTimestamp();
        entry->has_prepare_dn_stmt = false;
    }
    Assert(entry->plansource->magic == CACHEDPLANSOURCE_MAGIC);
}

static void DropFromQueryHashTable(const char* stmt_name)
{
    PG_TRY();
    {
        (void)syscalllockAcquire(&u_sess->pcache_cxt.pstmt_htbl_lock);
        hash_search(u_sess->pcache_cxt.prepared_queries, stmt_name, HASH_REMOVE, NULL);
        (void)syscalllockRelease(&u_sess->pcache_cxt.pstmt_htbl_lock);
    }
    PG_CATCH();
    {
        (void)syscalllockRelease(&u_sess->pcache_cxt.pstmt_htbl_lock);
        PG_RE_THROW();
    }
    PG_END_TRY();
}

#ifdef PGXC

/*
 * Assign the statement name for all the RemoteQueries in the plan tree, so
 * they use Datanode statements
 */
int SetRemoteStatementName(Plan* plan, const char* stmt_name, int num_params, Oid* param_types, int n,
                                      bool isBuildingCustomPlan, bool is_plan_shared)
{
    /* If no plan simply return */
    if (plan == NULL)
        return 0;

    /* Leave if no parameters */
    if (num_params == 0 || param_types == NULL)
        return 0;

    if (IsA(plan, RemoteQuery)) {
        RemoteQuery* remotequery = (RemoteQuery*)plan;
        DatanodeStatement* entry = NULL;
        bool exists = false;
        char name[NAMEDATALEN];

        /* Nothing to do if parameters are already set for this query */
        if (remotequery->rq_num_params != 0 && !is_plan_shared)
            return 0;

        if (stmt_name != NULL) {
            errno_t rc = strncpy_s(name, NAMEDATALEN, stmt_name, NAMEDATALEN - 1);
            securec_check(rc, "\0", "\0");

            name[NAMEDATALEN - 1] = '\0';

            /*
             * Append modifier. If resulting string is going to be truncated,
             * truncate better the base string, otherwise we may enter endless
             * loop
             */
            if (n) {
                char modifier[NAMEDATALEN];
                int ss_rc = -1;
                ss_rc = sprintf_s(modifier, NAMEDATALEN, "__%d", n);
                securec_check_ss(ss_rc, "\0", "\0");
                /*
                 * if position NAMEDATALEN - strlen(modifier) - 1 is beyond the
                 * base string this is effectively noop, otherwise it truncates
                 * the base string
                 */
                name[NAMEDATALEN - strlen(modifier) - 1] = '\0';
                ss_rc = -1;
                ss_rc = strcat_s(name, NAMEDATALEN, modifier);
                securec_check_ss(ss_rc, "\0", "\0");
            }
            n++;
            hash_search(u_sess->pcache_cxt.datanode_queries, name, HASH_FIND, &exists);

            /* If it already exists, that means this plan has just been revalidated. */
            if (!exists) {
                entry = (DatanodeStatement*)hash_search(u_sess->pcache_cxt.datanode_queries, name, HASH_ENTER, NULL);
                CN_GPC_LOG("entry datanodequery", 0, name);
                entry->current_nodes_number = 0;
                entry->dns_node_indices = (int*)MemoryContextAllocZero(
                    u_sess->pcache_cxt.datanode_queries->hcxt, u_sess->pgxc_cxt.NumDataNodes * sizeof(int));
                entry->max_nodes_number = u_sess->pgxc_cxt.NumDataNodes;
            }
            if (!is_plan_shared) {
                remotequery->statement = pstrdup(name);
                remotequery->stmt_idx = n - 1;
            }
#ifdef USE_ASSERT_CHECKING
            else {
                /* check same msg */
                Assert (remotequery->stmt_idx == n - 1);
            }
#endif
        } else if (remotequery->statement)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Passing parameters in PREPARE statement is not supported")));
        if (!is_plan_shared) {
            remotequery->rq_num_params = num_params;
            remotequery->rq_param_types = param_types;
            remotequery->isCustomPlan = isBuildingCustomPlan;
        }
#ifdef USE_ASSERT_CHECKING
        else {
            /* check same param msg */
            Assert (remotequery->rq_num_params == num_params);
            for (int i = 0; i < num_params; i++) {
                Assert (remotequery->rq_param_types[i] == param_types[i]);
            }
        }
#endif
    } else if (IsA(plan, ModifyTable)) {
        ModifyTable* mt_plan = (ModifyTable*)plan;
        /* For ModifyTable plan recurse into each of the plans underneath */
        ListCell* l = NULL;
        foreach (l, mt_plan->plans) {
            Plan* temp_plan = (Plan*)lfirst(l);
            n = SetRemoteStatementName(temp_plan, stmt_name, num_params, param_types, n,
                                       isBuildingCustomPlan, is_plan_shared);
        }
    }

    if (innerPlan(plan))
        n = SetRemoteStatementName(innerPlan(plan), stmt_name, num_params, param_types, n,
                                   isBuildingCustomPlan, is_plan_shared);

    if (outerPlan(plan))
        n = SetRemoteStatementName(outerPlan(plan), stmt_name, num_params, param_types, n,
                                   isBuildingCustomPlan, is_plan_shared);

    return n;
}

DatanodeStatement* light_set_datanode_queries(const char* stmt_name)
{
    DatanodeStatement* entry = NULL;

    /* Initialize the hash table, if necessary */
    if (!u_sess->pcache_cxt.prepared_queries)
        InitQueryHashTable();
    else {
        Assert(u_sess->pcache_cxt.datanode_queries != NULL);
        entry = (DatanodeStatement*)hash_search(u_sess->pcache_cxt.datanode_queries, stmt_name, HASH_FIND, NULL);
    }

    /* if not exists, add it */
    if (entry == NULL) {
        CN_GPC_LOG("entry lp datanodequery", 0, stmt_name);
        entry = (DatanodeStatement*)hash_search(u_sess->pcache_cxt.datanode_queries, stmt_name, HASH_ENTER, NULL);
        entry->current_nodes_number = 0;
        entry->dns_node_indices = (int*)MemoryContextAllocZero(
            u_sess->pcache_cxt.datanode_queries->hcxt, u_sess->pgxc_cxt.NumDataNodes * sizeof(int));
        entry->max_nodes_number = u_sess->pgxc_cxt.NumDataNodes;
    }

    return entry;
}
#endif

void StorePreparedStatementCNGPC(const char *stmt_name, CachedPlanSource *plansource, bool from_sql, bool is_share)
{
    TimestampTz cur_ts = GetCurrentStatementStartTimestamp();
    bool found = false;

    /* Initialize the hash table, if necessary */
    if (unlikely(!u_sess->pcache_cxt.prepared_queries))
        InitQueryHashTable();

    /* Add entry to hash table */
    InsertIntoQueryHashTable(stmt_name, plansource, from_sql, &found);
    CN_GPC_LOG("entry preparedstatement", plansource, stmt_name);

    /* Shouldn't get a duplicate entry */
    if (found) {
        if (is_share) {
            Assert(plansource->gpc.status.InShareTable());
            CN_GPC_LOG("duplicate prepared statement, sub refcount", plansource, 0);
            plansource->gpc.status.SubRefCount();
        }
        ereport(ERROR,
            (errcode(ERRCODE_DUPLICATE_PSTATEMENT), errmsg("prepared statement \"%s\" already exists", stmt_name)));
    }

    /* Now it's safe to move the CachedPlanSource to permanent memory */
    if (!is_share) {
        Assert((plansource->raw_parse_tree && IsA(plansource->raw_parse_tree, TransactionStmt)) ||
               !plansource->is_support_gplan || plansource->gpc.status.IsSharePlan());
        plansource->gpc.status.SetLoc(GPC_SHARE_IN_LOCAL_SAVE_PLAN_LIST);
        SaveCachedPlan(plansource);
    }
}

/*
 * Store all the data pertaining to a query in the hash table using
 * the specified key.  The passed CachedPlanSource should be "unsaved"
 * in case we get an error here; we'll save it once we've created the hash
 * table entry.
 */
void StorePreparedStatement(const char* stmt_name, CachedPlanSource* plansource, bool from_sql)
{
    if (ENABLE_DN_GPC) {
        if (unlikely(plansource->gpc.status.InShareTable()))
            elog(PANIC, "should get shared plan in gpc when StorePreparedStatement");
        /* dn gpc don't save prepare statement on dn */
        u_sess->pcache_cxt.cur_stmt_psrc = plansource;
        plansource->gpc.status.SetLoc(GPC_SHARE_IN_LOCAL_SAVE_PLAN_LIST);
        SaveCachedPlan(plansource);
        return;
    }
    if (ENABLE_CN_GPC) {
        StorePreparedStatementCNGPC(stmt_name, plansource, from_sql, false);
        return;
    }
    bool found = false;

    /* Initialize the hash table, if necessary */
    if (unlikely(!u_sess->pcache_cxt.prepared_queries))
        InitQueryHashTable();

    /* Add entry to hash table */
    InsertIntoQueryHashTable(stmt_name, plansource, from_sql, &found);

    /* Shouldn't get a duplicate entry */
    if (found)
        ereport(ERROR,
            (errcode(ERRCODE_DUPLICATE_PSTATEMENT), errmsg("prepared statement \"%s\" already exists", stmt_name)));

    /* Now it's safe to move the CachedPlanSource to permanent memory */
    SaveCachedPlan(plansource);
}

static void FetchPreparedStatementCNGPC(PreparedStatement* entry, const char* stmt_name)
{
    Assert (entry->plansource->magic == CACHEDPLANSOURCE_MAGIC);
    bool hasGetLock = false;
    /* check if need recreate */
    if (g_instance.plan_cache->CheckRecreateCachePlan(entry->plansource, &hasGetLock)) {
        entry->has_prepare_dn_stmt = false;
        g_instance.plan_cache->RecreateCachePlan(entry->plansource, entry->stmt_name, entry, NULL, NULL, hasGetLock);
    }
#ifdef ENABLE_MULTIPLE_NODES
    Assert (entry->plansource->lightProxyObj == NULL);
    /* add datanode statment for current sess if is shared plan.
       If it's CN light plancache. We will add datanode statment in execute stage. */
    if (entry->plansource->gpc.status.InShareTable() && entry->has_prepare_dn_stmt == false) {
        bool is_named_prepare = IS_PGXC_COORDINATOR && !IsConnFromCoord() &&
                                entry->stmt_name && entry->stmt_name[0] != '\0';
        bool is_lp = entry->plansource->single_exec_node != NULL &&
                     entry->plansource->gplan == NULL && entry->plansource->cplan == NULL;
        if (is_named_prepare && !is_lp && entry->plansource->gplan) {
            int n = 0;
            ListCell* lc = NULL;
            MemoryContext old_cxt = MemoryContextSwitchTo(u_sess->cache_mem_cxt);
            foreach (lc, entry->plansource->gplan->stmt_list) {
                Node* st = NULL;
                PlannedStmt* ps = NULL;
                st = (Node*)lfirst(lc);
                if (IsA(st, PlannedStmt)) {
                    ps = (PlannedStmt*)st;
                    n = SetRemoteStatementName(ps->planTree, entry->stmt_name, entry->plansource->num_params,
                                               entry->plansource->param_types, n, false, true);
                }
            }
            CN_GPC_LOG("set datanode statment for shared plan", entry->plansource, stmt_name);
            Assert (entry->plansource->gplan->dn_stmt_num == n);
            (void)MemoryContextSwitchTo(old_cxt);
        }
        entry->has_prepare_dn_stmt = true;
    }
#endif
}

/*
 * Lookup an existing query in the hash table. If the query does not
 * actually exist, throw ereport(ERROR) or return NULL per second parameter.
 *
 * Note: this does not force the referenced plancache entry to be valid,
 * since not all callers care.
 */
PreparedStatement* FetchPreparedStatement(const char* stmt_name, bool throwError, bool need_valid)
{
    if (ENABLE_DN_GPC) {
        if (throwError)
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_PSTATEMENT),
                     errmsg("prepared statement \"%s\" does not exist on DN with GPC", stmt_name)));
        return NULL;
    }

    PreparedStatement *entry = NULL;

    /*
     * If the hash table hasn't been initialized, it can't be storing
     * anything, therefore it couldn't possibly store our plan.
     */
    if (u_sess->pcache_cxt.prepared_queries) {
        entry = (PreparedStatement*)hash_search(u_sess->pcache_cxt.prepared_queries, stmt_name, HASH_FIND, NULL);
    } else
        entry = NULL;

    if (entry == NULL && throwError)
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_PSTATEMENT), errmsg("prepared statement \"%s\" does not exist", stmt_name)));

    if (ENABLE_CN_GPC && entry != NULL && need_valid) {
        FetchPreparedStatementCNGPC(entry, stmt_name);
    }
    return entry;
}

/*
 * Before sned a plan with specified name to datanode, Check if it
 * is exist on coordinator.
 */
bool HaveActiveCoordinatorPreparedStatement(const char* stmt_name)
{
    bool found = false;

    if (u_sess->pcache_cxt.prepared_queries) {
        hash_search(u_sess->pcache_cxt.prepared_queries, stmt_name, HASH_FIND, &found);
    }

    return found;
}

/*
 * Given a prepared statement, determine the result tupledesc it will
 * produce.  Returns NULL if the execution will not return tuples.
 *
 * Note: the result is created or copied into current memory context.
 */
TupleDesc FetchPreparedStatementResultDesc(PreparedStatement *stmt)
{
    /*
     * Since we don't allow prepared statements' result tupdescs to change,
     * there's no need to worry about revalidating the cached plan here.
     */
    Assert(stmt->plansource->fixed_result);
    if (stmt->plansource->resultDesc)
        return CreateTupleDescCopy(stmt->plansource->resultDesc);
    else
        return NULL;
}

/*
 * Given a prepared statement that returns tuples, extract the query
 * targetlist.	Returns NIL if the statement doesn't have a determinable
 * targetlist.
 *
 * Note: this is pretty ugly, but since it's only used in corner cases like
 * Describe Statement on an EXECUTE command, we don't worry too much about
 * efficiency.
 */
List* FetchPreparedStatementTargetList(PreparedStatement *stmt)
{
    List* tlist = NIL;

    /* Get the plan's primary targetlist */
    tlist = CachedPlanGetTargetList(stmt->plansource);

    /* Copy into caller's context in case plan gets invalidated */
    return (List*)copyObject(tlist);
}

/*
 * Implements the 'DEALLOCATE' utility statement: deletes the
 * specified plan from storage.
 */
void DeallocateQuery(DeallocateStmt* stmt)
{
    if (stmt->name)
        DropPreparedStatement(stmt->name, true);
    else
        DropAllPreparedStatements();
}

/*
 * Internal version of DEALLOCATE
 *
 * If showError is false, dropping a nonexistent statement is a no-op.
 */
void DropPreparedStatement(const char* stmt_name, bool showError)
{
    if (ENABLE_DN_GPC) {
        /* no prepare statement on dn gpc */
        return ;
    }

    PreparedStatement *entry = NULL;

    /* Find the query's hash table entry; raise error if wanted */
    entry = FetchPreparedStatement(stmt_name, showError, false);
    ResourceOwner originalOwner = t_thrd.utils_cxt.CurrentResourceOwner;


    if (NULL == originalOwner) {
        /*
         * make sure ResourceOwner is not null, since it may acess catalog
         * when the pooler tries to create new connections
         */
        t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "DropPreparedStatement",
            THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER));
    }


    if (entry != NULL) {
        /* Release the plancache entry */
        Assert (entry->plansource->magic == CACHEDPLANSOURCE_MAGIC);
        if (ENABLE_CN_GPC)
            GPCDropLPIfNecessary(entry->stmt_name, true, true, NULL);
        if (entry->plansource->gpc.status.InShareTable()) {
            CN_GPC_LOG("prepare remove success", 0, entry->plansource->stmt_name);
#ifdef ENABLE_MULTIPLE_NODES
            if (entry->plansource->gplan)
                GPCCleanDatanodeStatement(entry->plansource->gplan->dn_stmt_num, entry->stmt_name);
#endif
            entry->plansource->gpc.status.SubRefCount();
        } else {
            CN_GPC_LOG("prepare remove private", entry->plansource, entry->stmt_name);
            DropCachedPlan(entry->plansource);
            CN_GPC_LOG("prepare remove private succ", 0, entry->stmt_name);
        }
        CN_GPC_LOG("remove prepare statment", 0, entry->stmt_name);
        /* Now we can remove the hash table entry */
        DropFromQueryHashTable(entry->stmt_name);
    }

    if (NULL == originalOwner && t_thrd.utils_cxt.CurrentResourceOwner) {
        ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, true);
        ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_LOCKS, false, true);
        ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_AFTER_LOCKS, false, true);

        ResourceOwner tempOwner = t_thrd.utils_cxt.CurrentResourceOwner;
        t_thrd.utils_cxt.CurrentResourceOwner = originalOwner;
        ResourceOwnerDelete(tempOwner);
    } 
}

/*
 * Drop all cached statements.
 */
void DropAllPreparedStatements(void)
{
    HASH_SEQ_STATUS seq;
    PreparedStatement *entry = NULL;
    ResourceOwner originalOwner = t_thrd.utils_cxt.CurrentResourceOwner;

    if (ENABLE_DN_GPC) {
        Assert (u_sess->pcache_cxt.prepared_queries == NULL);
        CleanSessGPCPtr(u_sess);
        return;
    }

    /* nothing cached */
    if (!u_sess->pcache_cxt.prepared_queries)
        return;

#define ReleaseTempResourceOwner()                                                                               \
    do {                                                                                                         \
        ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, true); \
        ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_LOCKS, false, true);        \
        ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_AFTER_LOCKS, false, true);  \
        if (NULL == originalOwner && t_thrd.utils_cxt.CurrentResourceOwner) {                                    \
            ResourceOwner tempOwner = t_thrd.utils_cxt.CurrentResourceOwner;                                     \
            t_thrd.utils_cxt.CurrentResourceOwner = originalOwner;                                               \
            ResourceOwnerDelete(tempOwner);                                                                      \
        }                                                                                                        \
    } while (0);

    if (NULL == originalOwner) {
        /*
         * make sure ResourceOwner is not null, since it may acess catalog
         * when the pooler tries to create new connections
         */
        t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "DropAllPreparedStatements",
            THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER));
    }

    bool failflag_dropcachedplan = false;
    ErrorData* edata = NULL;
    MemoryContext oldcontext = CurrentMemoryContext;
    bool isSharedPlan = false;

    /* walk over cache */
    hash_seq_init(&seq, u_sess->pcache_cxt.prepared_queries);
    while ((entry = (PreparedStatement*)hash_seq_search(&seq)) != NULL) {
        PG_TRY();
        {
            /* Release the plancache entry */
            Assert (entry->plansource->magic == CACHEDPLANSOURCE_MAGIC);
            isSharedPlan = entry->plansource->gpc.status.InShareTable();
#ifdef ENABLE_MULTIPLE_NODES
            if (ENABLE_CN_GPC)
                GPCDropLPIfNecessary(entry->stmt_name, true, true, NULL);
            /* for gpc, in case has error, only send drop preparestatement to dn here, sub refcount later */
            if (isSharedPlan && entry->plansource->gplan != NULL) {
                GPCCleanDatanodeStatement(entry->plansource->gplan->dn_stmt_num, entry->stmt_name);
            }
#endif
            if (!isSharedPlan) {
                CN_GPC_LOG("prepare remove private", entry->plansource, entry->stmt_name);
                DropCachedPlan(entry->plansource);
                CN_GPC_LOG("prepare remove private succ", 0, entry->stmt_name);
            }
        }
        PG_CATCH();
        {
            failflag_dropcachedplan = true;

            /* Must reset elog.c's state */
            MemoryContextSwitchTo(oldcontext);
            edata = CopyErrorData();
            FlushErrorState();
            ereport(LOG,
                (errmodule(MOD_EXECUTOR),
                    errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("failed to drop cached plan when drop all prepared statements: %s", edata->message)));
            FreeErrorData(edata);
        }
        PG_END_TRY();
        if (isSharedPlan) {
            CN_GPC_LOG("prepare remove ", entry->plansource, entry->plansource->stmt_name);
            /* sub refcount savely */
            entry->plansource->gpc.status.SubRefCount();
        }

        /* Now we can remove the hash table entry */
        DropFromQueryHashTable(entry->stmt_name);
    }
    ReleaseTempResourceOwner();
    CN_GPC_LOG("remove prepare statment all", 0, 0);

    if (failflag_dropcachedplan) {
        /* destory connections to other node to cleanup all cached statements */
        destroy_handles();
        ereport(
            ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_INTERNAL_ERROR), errmsg("failed to drop cached plan")));
    }
}

/*
 * When pool reloaded on CN, drop prepared statement on dn
 * and invalid cached plans.
 */
void HandlePreparedStatementsForReload(void)
{
    HASH_SEQ_STATUS seq;
    PreparedStatement *entry = NULL;
    ErrorData* edata = NULL;

    /* nothing cached */
    if (!u_sess->pcache_cxt.prepared_queries)
        return;

    if (ENABLE_CN_GPC) {
        CN_GPC_LOG("Invalid all prepared statements for pool reload", 0, 0);
    }
    MemoryContext oldcontext = CurrentMemoryContext;
    bool has_error = false;
    /* walk over cache */
    hash_seq_init(&seq, u_sess->pcache_cxt.prepared_queries);
    while ((entry = (PreparedStatement*)hash_seq_search(&seq)) != NULL) {
        /* We don't handle these plans which don't include relation */
        if (list_length(entry->plansource->relationOids) == 0)
            continue;
        PG_TRY();
        {
            /* clean CachedPlanSource */
            if (entry->plansource->gpc.status.IsSharePlan()) {
                g_instance.plan_cache->RemovePlanSource<ACTION_RELOAD>(entry->plansource, entry->stmt_name);
            } else {
                DropCachedPlanInternal(entry->plansource);
            }
            entry->has_prepare_dn_stmt = false;
        }
        PG_CATCH();
        {
            /* Must reset elog.c's state */
            MemoryContextSwitchTo(oldcontext);
            edata = CopyErrorData();
            FlushErrorState();
            ereport(LOG,
                (errmodule(MOD_EXECUTOR),
                    errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("failed to drop internal cached plan when reload prepared statements: %s", edata->message)));
            FreeErrorData(edata);
            entry->has_prepare_dn_stmt = false;
            has_error = true;
        }
        PG_END_TRY();
    }

    ereport(LOG,
        (errmodule(MOD_OPT), errcode(ERRCODE_INTERNAL_ERROR), errmsg("Invalid all prepared statements for reload")));

    /* invalid all cached plans */
    ResetPlanCache();

    /* if error occurrs, report error to log jmp and destory handles */
    if (has_error) {
        ereport(ERROR,
            (errmodule(MOD_EXECUTOR),
                errcode(ERRCODE_INTERNAL_ERROR),
                errmsg("failed to drop internal cached plan when reload prepared statements")));
    }
}

/*
 * When CN retry, clean datanode_queries and invalid cached plans.
 */
void HandlePreparedStatementsForRetry(void)
{
    /* nothing cached */
    if (u_sess->pcache_cxt.prepared_queries == NULL)
        return;

    /*
     * If we set plansource to be invalid, its light proxy (if exits) will be cleaned in next
     * RevalidateCachedQuery, and its generic plan (if exits) will be cleaned in next CheckCachedPlan,
     * and its custom plan will be cleaned automatically when generating a new generic/custom plan
     * next time.
     * Moreover, because of CN retry, prepared statements on dn will be cleaned by destroy_handles in
     * AbortTransaction later.
     *
     * We only need to set plansource invalid here.
     */
    ResetPlanCache();

    if (ENABLE_CN_GPC) {
        /* set plansource to invalid like ungpc */
        CN_GPC_LOG("Invalid all prepared statements for retry", 0, 0);
        HASH_SEQ_STATUS seq;
        PreparedStatement* entry = NULL;
        hash_seq_init(&seq, u_sess->pcache_cxt.prepared_queries);
        while ((entry = (PreparedStatement*)hash_seq_search(&seq)) != NULL) {
            if (entry->plansource->gpc.status.IsSharePlan())
                g_instance.plan_cache->RemovePlanSource<ACTION_CN_RETRY>(entry->plansource, entry->stmt_name);
        }
    }

    ereport(DEBUG2, (errmodule(MOD_OPT), errmsg("Invalid all prepared statements for retry")));
}

CachedPlanSource* GetCachedPlanSourceFromExplainExecute(const char* stmt_name)
{
    PreparedStatement *entry = NULL;
    CachedPlanSource* psrc = NULL;
    if (ENABLE_DN_GPC && IsConnFromCoord()) {
        psrc = u_sess->pcache_cxt.cur_stmt_psrc;
        if (SECUREC_UNLIKELY(psrc == NULL)) {
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_PSTATEMENT),
                errmsg("dn gpc's prepared statement does not exist")));
        }
    } else {
        /* Look it up in the hash table */
        entry = FetchPreparedStatement(stmt_name, true, true);
        psrc = entry->plansource;
    }
    Assert(psrc != NULL);

    /* Shouldn't find a non-fixed-result cached plan */
    if (!psrc->fixed_result) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("EXPLAIN EXECUTE does not support variable-result cached plans")));
    }

    return psrc;
}

/*
 * Implements the 'EXPLAIN EXECUTE' utility statement.
 *
 * "into" is NULL unless we are doing EXPLAIN CREATE TABLE AS EXECUTE,
 * in which case executing the query should result in creating that table.
 *
 * Note: the passed-in queryString is that of the EXPLAIN EXECUTE,
 * not the original PREPARE; we get the latter string from the plancache.
 */
void ExplainExecuteQuery(
    ExecuteStmt* execstmt, IntoClause* into, ExplainState* es, const char* queryString, ParamListInfo params)
{
    const char* query_string = NULL;
    CachedPlan* cplan = NULL;
    MemoryContext tmpCxt = NULL;
    List* plan_list = NIL;
    ListCell* p = NULL;
    ParamListInfo paramLI = NULL;
    EState* estate = NULL;

    CachedPlanSource* psrc = GetCachedPlanSourceFromExplainExecute(execstmt->name);

    query_string = psrc->query_string;

    /* Evaluate parameters, if any */
    if (psrc->num_params) {
        /*
         * Need an EState to evaluate parameters; must not delete it till end
         * of query, in case parameters are pass-by-reference.	Note that the
         * passed-in "params" could possibly be referenced in the parameter
         * expressions.
         */
        estate = CreateExecutorState();
        estate->es_param_list_info = params;
        paramLI = EvaluateParams(psrc, execstmt->params, queryString, estate);
    }

    /* Replan if needed, and acquire a transient refcount */
    if (u_sess->attr.attr_common.max_datanode_for_plan > 0 && IS_PGXC_COORDINATOR && !IsConnFromCoord() &&
        paramLI != NULL) {
        paramLI->params_need_process = true;
    }

    u_sess->attr.attr_sql.explain_allow_multinode = true;

    if (!into) {
        psrc->cursor_options |= CURSOR_OPT_SPQ_OK;
    }

    if (ENABLE_CACHEDPLAN_MGR) {
        cplan = GetWiseCachedPlan(psrc, paramLI, true);
    } else {
        cplan = GetCachedPlan(psrc, paramLI, true);
    }

    /* use shared plan here, add refcount */
    if (cplan->isShared())
        (void)pg_atomic_fetch_add_u32((volatile uint32*)&cplan->global_refcount, 1);

    u_sess->attr.attr_sql.explain_allow_multinode = false;

    if (ENABLE_GPC && psrc->gplan) {
        plan_list = CopyLocalStmt(cplan->stmt_list, u_sess->temp_mem_cxt, &tmpCxt);
    } else {
        plan_list = cplan->stmt_list;
    }

    es->is_explain_gplan = false;
    if (psrc->cplan == NULL)
        es->is_explain_gplan = true;

    /* Explain each query */
    foreach (p, plan_list) {
        PlannedStmt* pstmt = (PlannedStmt*)lfirst(p);
        int instrument_option = pstmt->instrument_option;

        /* get g_RemoteQueryList by reseting sql_statement. */
        if (u_sess->attr.attr_common.max_datanode_for_plan > 0 && IS_PGXC_COORDINATOR && !IsConnFromCoord() &&
            es->is_explain_gplan && psrc->gplan_is_fqs) {
            GetRemoteQuery(pstmt, queryString);
            es->isexplain_execute = true;
        }

        if (IsA(pstmt, PlannedStmt))
            ExplainOnePlan(pstmt, into, es, query_string, None_Receiver, paramLI);
        else
            ExplainOneUtility((Node*)pstmt, into, es, query_string, paramLI);

        pstmt->instrument_option = instrument_option;

        /* No need for CommandCounterIncrement, as ExplainOnePlan did it */

        /* Separate plans with an appropriate separator */
        if (lnext(p) != NULL)
            ExplainSeparatePlans(es);
    }

    if (estate != NULL)
        FreeExecutorState(estate);

    ReleaseCachedPlan(cplan, true);
}

/*
 * This set returning function reads all the prepared statements and
 * returns a set of (name, statement, prepare_time, param_types, from_sql).
 */
Datum pg_prepared_statement(PG_FUNCTION_ARGS)
{
    ReturnSetInfo* rsinfo = (ReturnSetInfo*)fcinfo->resultinfo;
    TupleDesc tupdesc;
    Tuplestorestate* tupstore = NULL;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;

    /* check to see if caller supports us returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("set-valued function called in context that cannot accept a set")));
    if (!(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("materialize mode required, but it is not "
                       "allowed in this context")));

    /* need to build tuplestore in query context */
    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);

    /*
     * build tupdesc for result tuples. This must match the definition of the
     * pg_prepared_statements view in system_views.sql
     */
    tupdesc = CreateTemplateTupleDesc(5, false);
    TupleDescInitEntry(tupdesc, (AttrNumber)1, "name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)2, "statement", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)3, "prepare_time", TIMESTAMPTZOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)4, "parameter_types", REGTYPEARRAYOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)5, "from_sql", BOOLOID, -1, 0);

    /*
     * We put all the tuples into a tuplestore in one scan of the hashtable.
     * This avoids any issue of the hashtable possibly changing between calls.
     */
    tupstore =
        tuplestore_begin_heap(rsinfo->allowedModes & SFRM_Materialize_Random, false, u_sess->attr.attr_memory.work_mem);

    /* generate junk in short-term context */
    MemoryContextSwitchTo(oldcontext);

    /* hash table might be uninitialized */
    if (u_sess->pcache_cxt.prepared_queries) {
        HASH_SEQ_STATUS hash_seq;
        PreparedStatement *prep_stmt = NULL;

        hash_seq_init(&hash_seq, u_sess->pcache_cxt.prepared_queries);
        while ((prep_stmt = (PreparedStatement*)hash_seq_search(&hash_seq)) != NULL) {
            Datum values[5];
            bool nulls[5];

            errno_t rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
            securec_check(rc, "\0", "\0");

            values[0] = CStringGetTextDatum(prep_stmt->stmt_name);
            char* maskquery = maskPassword(prep_stmt->plansource->query_string);
            const char* query = (maskquery == NULL) ? prep_stmt->plansource->query_string : maskquery;
            values[1] = CStringGetTextDatum(query);
            if (query != maskquery)
                pfree_ext(maskquery);
            values[2] = TimestampTzGetDatum(prep_stmt->prepare_time);
            values[3] = build_regtype_array(prep_stmt->plansource->param_types, prep_stmt->plansource->num_params);
            values[4] = BoolGetDatum(prep_stmt->from_sql);

            tuplestore_putvalues(tupstore, tupdesc, values, nulls);
        }
    }

    /* clean up and return the tuplestore */
    tuplestore_donestoring(tupstore);

    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;

    return (Datum)0;
}

Datum pg_prepared_statement_global(PG_FUNCTION_ARGS)
{
    if (!superuser() && !isMonitoradmin(GetUserId())) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_PROC, "pg_prepared_statements");
    }

    uint64 sessionid = (uint64)PG_GETARG_INT64(0);
    ReturnSetInfo *rsinfo = (ReturnSetInfo*)fcinfo->resultinfo;
    TupleDesc tupdesc;
    Tuplestorestate* tupstore = NULL;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;

    /* check to see if caller supports us returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("set-valued function called in context that cannot accept a set")));
    if (!(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("materialize mode required, but it is not "
                       "allowed in this context")));

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);

    /*
     * build tupdesc for result tuples. This must match the definition of the
     * pg_prepared_statements view in system_views.sql
     */
    tupdesc = CreateTemplateTupleDesc(7, false);

    TupleDescInitEntry(tupdesc, (AttrNumber)1, "sessionid", INT8OID, -1, 0 );
    TupleDescInitEntry(tupdesc, (AttrNumber)2, "username", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)3, "name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)4, "statement", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)5, "prepare_time", TIMESTAMPTZOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)6, "parameter_types", REGTYPEARRAYOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)7, "from_sql", BOOLOID, -1, 0);

     /*
     * We put all the tuples into a tuplestore in one scan of the hashtable.
     * This avoids any issue of the hashtable possibly changing between calls.
     */
    tupstore =
        tuplestore_begin_heap(rsinfo->allowedModes & SFRM_Materialize_Random, false, u_sess->attr.attr_memory.work_mem);

    /* generate junk in short-term context */
    MemoryContextSwitchTo(oldcontext);

    /* total number of tuples to be returned */
    if (ENABLE_THREAD_POOL) {
        g_threadPoolControler->GetSessionCtrl()->GetSessionPreparedStatements(tupstore, tupdesc, sessionid);
    } else {
        GetThreadPreparedStatements(tupstore, tupdesc, sessionid);
    }

    /* clean up and return the tuplestore */
    tuplestore_donestoring(tupstore);

    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;

    return (Datum)0;
}

void GetPreparedStatements(HTAB* htbl, Tuplestorestate* tupStore, TupleDesc tupDesc, uint64 sessionId, char* userName)
{
    HASH_SEQ_STATUS hash_seq;
    PreparedStatement *prep_stmt = NULL;
    hash_seq_init(&hash_seq, htbl);
    while ((prep_stmt = (PreparedStatement*)hash_seq_search(&hash_seq)) != NULL) {
        Datum values[7];
        bool nulls[7];

        errno_t rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        values[0] = UInt64GetDatum(sessionId);
        values[1] = CStringGetTextDatum(userName);
        values[2] = CStringGetTextDatum(prep_stmt->stmt_name);
        char* maskquery = maskPassword(prep_stmt->plansource->query_string);
        const char* query = (maskquery == NULL) ? prep_stmt->plansource->query_string : maskquery;
        values[3] = CStringGetTextDatum(query);
        if (query != maskquery)
            pfree_ext(maskquery);
        values[4] = TimestampTzGetDatum(prep_stmt->prepare_time);
        values[5] = build_regtype_array(prep_stmt->plansource->param_types, prep_stmt->plansource->num_params);
        values[6] = BoolGetDatum(prep_stmt->from_sql);
        
        tuplestore_putvalues(tupStore, tupDesc, values, nulls);
    }
}

void GetThreadPreparedStatements(Tuplestorestate* tupStore, TupleDesc tupDesc, uint64 sessionId)
{
    Assert(!ENABLE_THREAD_POOL);    
    PgBackendStatus *beentry = t_thrd.shemem_ptr_cxt.BackendStatusArray;
    char* userName = NULL;

    PG_TRY();
    {
        for(int i = 0; i < BackendStatusArray_size; i++){
            HTAB* htbl = beentry->my_prepared_queries;
            
	    if (beentry->my_pstmt_htbl_lock != NULL)   
              if ((beentry->st_procpid > 0 || beentry -> st_sessionid > 0) && 
                  (beentry->st_sessionid == sessionId || sessionId == 0)) {
                  Oid userid = beentry->st_userid;
                  userName = GetUserNameFromId(userid);
                  if (htbl) {
                      (void)syscalllockAcquire(beentry->my_pstmt_htbl_lock);
                       GetPreparedStatements(htbl, tupStore, tupDesc, beentry->st_sessionid, userName);
                      (void)syscalllockRelease(beentry->my_pstmt_htbl_lock);            
                } 
            }
    
              pfree_ext(userName);

              beentry++;
        }
    }
    PG_CATCH();
    {
        (void)syscalllockRelease(beentry->my_pstmt_htbl_lock);
        pfree_ext(userName);
        PG_RE_THROW();	
    }
     PG_END_TRY();
}

/*
 * This utility function takes a C array of Oids, and returns a Datum
 * pointing to a one-dimensional Postgres array of regtypes. An empty
 * array is returned as a zero-element array, not NULL.
 */
static Datum build_regtype_array(const Oid* param_types, int num_params)
{
    Datum* tmp_ary = NULL;
    ArrayType* result = NULL;
    int i;

    tmp_ary = (Datum*)palloc(num_params * sizeof(Datum));

    for (i = 0; i < num_params; i++)
        tmp_ary[i] = ObjectIdGetDatum(param_types[i]);

    /* XXX: this hardcodes assumptions about the regtype type */
    result = construct_array(tmp_ary, num_params, REGTYPEOID, 4, true, 'i');
    return PointerGetDatum(result);
}

#ifdef PGXC
DatanodeStatement* FetchDatanodeStatement(const char* stmt_name, bool throwError)
{
    DatanodeStatement* entry = NULL;

    /*
     * If the hash table hasn't been initialized, it can't be storing
     * anything, therefore it couldn't possibly store our plan.
     */
    if (u_sess->pcache_cxt.datanode_queries)
        entry = (DatanodeStatement*)hash_search(u_sess->pcache_cxt.datanode_queries, stmt_name, HASH_FIND, NULL);
    else
        entry = NULL;

    /* Report error if entry is not found */
    if (entry == NULL && throwError)
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_PSTATEMENT), errmsg("datanode statement \"%s\" does not exist", stmt_name)));

    return entry;
}

/*
 * Drop Datanode statement and close it on nodes if active
 */
void DropDatanodeStatement(const char* stmt_name)
{
    DatanodeStatement* entry = NULL;

    entry = FetchDatanodeStatement(stmt_name, false);
    if (entry != NULL) {
        int i;
        List* nodelist = NIL;

        /* make a List of integers from node numbers */
        for (i = 0; i < entry->current_nodes_number; i++) {
            nodelist = lappend_int(nodelist, entry->dns_node_indices[i]);
        }

        CN_GPC_LOG("drop datanode statment", NULL, entry->stmt_name);

        entry->current_nodes_number = 0;
        entry->max_nodes_number = 0;
        pfree_ext(entry->dns_node_indices);

        /* Okay to remove it */
        (void*)hash_search(u_sess->pcache_cxt.datanode_queries, entry->stmt_name, HASH_REMOVE, NULL);
        if (!ENABLE_CN_GPC)
            ExecCloseRemoteStatement(stmt_name, nodelist);
        list_free_ext(nodelist);
    }
}

/*
 * Mark all datanode statements as deactive.
 */
void DeActiveAllDataNodeStatements(void)
{
    int tmp_num = 0;
    errno_t errorno = EOK;

    /* nothing cached */
    if (!u_sess->pcache_cxt.datanode_queries)
        return;

    HASH_SEQ_STATUS seq;
    DatanodeStatement* entry = NULL;

    /* walk over cache */
    hash_seq_init(&seq, u_sess->pcache_cxt.datanode_queries);
    while ((entry = (DatanodeStatement*)hash_seq_search(&seq)) != NULL) {
        tmp_num = entry->current_nodes_number;
        entry->current_nodes_number = 0;
        if (tmp_num > 0) {
            Assert(tmp_num <= Max(u_sess->pgxc_cxt.NumTotalDataNodes, u_sess->pgxc_cxt.NumDataNodes));
            errorno = memset_s(entry->dns_node_indices, tmp_num * sizeof(int), 0, tmp_num * sizeof(int));
            securec_check_c(errorno, "\0", "\0");
        }
    }
}

/*
 * Return true if there is at least one active Datanode statement, so acquired
 * Datanode connections should not be released
 */
bool HaveActiveDatanodeStatements(void)
{
    HASH_SEQ_STATUS seq;
    DatanodeStatement* entry = NULL;

    /* nothing cached */
    if (!u_sess->pcache_cxt.datanode_queries)
        return false;

    /* walk over cache */
    hash_seq_init(&seq, u_sess->pcache_cxt.datanode_queries);
    while ((entry = (DatanodeStatement*)hash_seq_search(&seq)) != NULL) {
        /* Stop walking and return true */
        if (entry->current_nodes_number > 0) {
            hash_seq_term(&seq);
            return true;
        }
    }
    /* nothing found */
    return false;
}

/*
 * Mark Datanode statement as active on specified node
 * Return true if statement has already been active on the node and can be used
 * Returns false if statement has not been active on the node and should be
 * prepared on the node
 */
bool ActivateDatanodeStatementOnNode(const char* stmt_name, int nodeIdx)
{
    DatanodeStatement* entry = NULL;
    int i;

    /* find the statement in cache */
    entry = FetchDatanodeStatement(stmt_name, true);

    /* see if statement already active on the node */
    for (i = 0; i < entry->current_nodes_number; i++) {
        if (entry->dns_node_indices[i] == nodeIdx) {
            return true;
        }
    }

    /* After cluster expansion, must expand entry->dns_node_indices array too */
    if (entry->current_nodes_number == entry->max_nodes_number) {
        int* new_dns_node_indices = (int*)MemoryContextAllocZero(
            u_sess->pcache_cxt.datanode_queries->hcxt, entry->max_nodes_number * CLUSTER_EXPANSION_BASE * sizeof(int));
        errno_t errorno = EOK;
        errorno = memcpy_s(new_dns_node_indices,
            entry->max_nodes_number * CLUSTER_EXPANSION_BASE * sizeof(int),
            entry->dns_node_indices,
            entry->max_nodes_number * sizeof(int));
        securec_check(errorno, "\0", "\0");
        pfree_ext(entry->dns_node_indices);
        entry->dns_node_indices = new_dns_node_indices;
        entry->max_nodes_number = entry->max_nodes_number * CLUSTER_EXPANSION_BASE;
        elog(LOG,
            "expand node ids array for active datanode statements "
            "after cluster expansion, now array size is %d",
            entry->max_nodes_number);
    }

    /* statement is not active on the specified node append item to the list */
    entry->dns_node_indices[entry->current_nodes_number++] = nodeIdx;
    return false;
}

char* get_datanode_statement_name(const char* stmt_name, int n)
{
    char name[NAMEDATALEN];
    errno_t rc = strncpy_s(name, NAMEDATALEN, stmt_name, NAMEDATALEN - 1);
    securec_check(rc, "\0", "\0");
    if (n) {
        name[NAMEDATALEN - 1] = '\0';
        char modifier[NAMEDATALEN];
        int ss_rc = -1;
        ss_rc = sprintf_s(modifier, NAMEDATALEN, "__%d", n);
        securec_check_ss(ss_rc, "\0", "\0");
        name[NAMEDATALEN - strlen(modifier) - 1] = '\0';
        ss_rc = -1;
        ss_rc = strcat_s(name, NAMEDATALEN, modifier);
        securec_check(ss_rc, "\0", "\0");
    }
    return pstrdup(name);
}

#endif

/*
 * Function name: needRecompileQuery
 * 		Check if perpared query need to be reprepared.
 * input Parameter:
 * 		stmt: the stmt need to be checked if it need to be reprepared.
 * output result:
 * 		True : need to do rePrepare proc before executing execute stmt.
 *		False: could execute stmt directly.
 */
bool needRecompileQuery(ExecuteStmt* stmt)
{
    bool ret_val = false;
    PreparedStatement *entry = NULL;
    CachedPlanSource* plansource = NULL;

    /* Look it up in the hash table */
    entry = FetchPreparedStatement(stmt->name, true, false);

    /* Find if there is query that has been enabled auto truncation.*/
    plansource = entry->plansource;

    ret_val = checkRecompileCondition(plansource);

    return ret_val;
}

/*
 * Function name: RePrepareQuery
 * 		do re-PrepareQuery for stmt Prepare.
 * input Parameter:
 * 		stmt: the stmt need to be re-prepared.
 * output result:
 * 				void
 */
void RePrepareQuery(ExecuteStmt* stmt)
{
    PreparedStatement *entry = NULL;
    char* query_string = NULL;
    uint32 query_length;
    errno_t err;
    List* parseTree_list = NIL;
    List* queryTree_list = NIL;
    ListCell* parsetree_item = NULL;
    ListCell* stmtlist_item = NULL;

    /* Look it up in the hash table */
    entry = FetchPreparedStatement(stmt->name, true, false);

    /* copy the original query text.*/
    query_length = strlen(entry->plansource->query_string);
    query_string = (char*)palloc(query_length + 1);
    err = strcpy_s(query_string, query_length + 1, entry->plansource->query_string);

    securec_check(err, "\0", "\0");
    /* Need drop old prepared statement and then generated new one with same name. */
    DropPreparedStatement(stmt->name, true);

    /*
     * Do re prepare task. here we will do a simplified flow to get prepared
     * stmt from query_string. since we are in execute stmt's context, we do
     * not need do all the parts of exec_simple_query.
     */
    parseTree_list = pg_parse_query(query_string);

    Assert(parseTree_list != NULL && parseTree_list->length > 0);

    /*
     * Run through the raw parsetree(s) and process each one.
     */
    foreach (parsetree_item, parseTree_list) {
        Node* parsetree = (Node*)lfirst(parsetree_item);
        t_thrd.postgres_cxt.cur_command_tag = transform_node_tag(parsetree);
        List* planTree_list = NIL;

        queryTree_list = pg_analyze_and_rewrite(parsetree, query_string, NULL, 0);

        Assert(queryTree_list != NULL && queryTree_list->length > 0);

        planTree_list = pg_plan_queries(queryTree_list, 0, NULL);

        Assert(planTree_list != NULL && planTree_list->length > 0);

        foreach (stmtlist_item, planTree_list) {
            Node* stmt_node = (Node*)lfirst(stmtlist_item);
            PrepareQuery((PrepareStmt*)stmt_node, query_string);
        }
    }
}

/*
 * Function name: checkRecompileCondition
 *      determin if the stmt need to be recompiled.
 * input Parameter:
 *      plansource: the stmt need to be checked if it need to be reprepared.
 * output result:
 * There are four scenario:
 * td_compatible_truncation | Query->tdTruncCastStatus | return
 *             True            TRUNC_CAST_QUERY          False, means the insert stmt has set auto truncation
 *                                                        according, here do not need recompile.
 *             True            NOT_CAST_BECAUSEOF_GUC    True, we should recompile to make sure the char and
 *                                                        varchar truncation enabled.
 *             False           TRUNC_CAST_QUERY          True, we should recompile to make sure turn off auto
 *                                                        truncation function for char and varchar type data. 
 *             False           NOT_CAST_BECAUSEOF_GUC    False, means we did not use auto truncation function
 *                                                        before, no need to re-compile. 
 *             True/False      UNINVOLVED_QUERY          False, uninvolved query always false.
 *                                                        Don't need re-generate plan.
 */
bool checkRecompileCondition(CachedPlanSource* plansource)
{
    ListCell* l = NULL;
    foreach (l, plansource->query_list) {
        Query* q = (Query*)lfirst(l);
        Assert(IsA(q, Query));
        /* If some rte is referenced by synonym object, must recompile. */
        if (q->hasSynonyms) {
            return true;
        }

        if (q->tdTruncCastStatus == UNINVOLVED_QUERY) {
            return false;
        }

        if (u_sess->attr.attr_sql.td_compatible_truncation) {
            if (q->tdTruncCastStatus == NOT_CAST_BECAUSEOF_GUC) {
                return true;
            }
        } else {
            if (q->tdTruncCastStatus == TRUNC_CAST_QUERY) {
                return true;
            }
        }
    }
    return false;
}

typedef struct {
    int* nargs;
    Oid** args;
    List** constargs;
    bool* ret;
} substitute_const_with_parameters_context;

static Node* substitute_const_with_parameters_mutator(Node* node, substitute_const_with_parameters_context* context)
{
    if (node == NULL)
        return NULL;
    if (*context->ret) {
        return NULL;
    }
    if (IsA(node, OpExpr) && list_length(((OpExpr*)node)->args) == 2) {
        OpExpr* op_expr = (OpExpr*)node;
        Node* arg1 = (Node*)linitial(op_expr->args);
        Node* arg2 = (Node*)lsecond(op_expr->args);

        /* We only support parameter is const and operator is less than or less equal. */
        if (IsA(arg1, Const) && IsA(arg2, Const)) {
            *context->ret = true;
            return node;
        }
    }
    if (IsA(node, FuncExpr)) {
        FuncExpr* func_expr = (FuncExpr*)node;
        if (func_expr->funcid >= DB4AI_PREDICT_BY_BOOL_OID && func_expr->funcid <= DB4AI_EXPLAIN_MODEL_OID) {
            *context->ret = true;
            return NULL;
        }
    }
    if (IsA(node, UserVar)) {
        *context->ret = true;
        return NULL;
    }
    if (IsA(node, Const)) {
        Const* con = (Const*)node;
        Param* param = makeNode(Param);
        param->paramkind = PARAM_EXTERN;
        param->paramid = *context->nargs + 1;
        param->paramtype = con->consttype;
        param->paramtypmod = con->consttypmod;
        param->paramcollid = con->constcollid;
        param->location = con->location;
        param->is_bind_param = true;
        if (*context->args) {
            *context->args = (Oid*)repalloc(*context->args, param->paramid * sizeof(Oid));
        } else {
            *context->args = (Oid*)palloc(param->paramid * sizeof(Oid));
        }
        errno_t rc = memset_s(*context->args + *context->nargs, sizeof(Oid), 0, sizeof(Oid));
        securec_check(rc, "\0", "\0");
        (*context->args)[param->paramid - 1] = param->paramtype;
        *context->constargs = lappend(*context->constargs, con);
        (*context->nargs)++;
        return (Node*)param;
    }
    return expression_tree_mutator(
        node, (Node* (*)(Node*, void*)) substitute_const_with_parameters_mutator, (void*)context);
}

static Query* substitute_const_with_parameters(Query* expr, int* nargs, Oid** param_types, List** paramListInfo, bool* ret)
{
    substitute_const_with_parameters_context context;

    context.nargs = nargs;
    *context.nargs = 0;
    context.args = param_types;
    context.constargs = paramListInfo;
    context.ret = ret;
    return query_tree_mutator(expr, (Node* (*)(Node*, void*)) substitute_const_with_parameters_mutator, &context, 0);
}

static ParamListInfo PrepareParamsFromConsts(CachedPlanSource* psrc, List* params, const char* queryString)
{
    Oid* param_types = psrc->param_types;
    int num_params = psrc->num_params;
    int nparams = list_length(params);
    ParamListInfo paramLI;
    ListCell* l = NULL;
    int i = 0;

    if (nparams != num_params)
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("wrong number of parameters for prepared statement \"%s\"", psrc->stmt_name),
                errdetail("Expected %d parameters but got %d.", num_params, nparams)));

    /* Quick exit if no parameters */
    if (num_params == 0)
        return NULL;

    /*
     * We have to run parse analysis for the expressions.  Since the parser is
     * not cool about scribbling on its input, copy first.
     */
    params = (List*)copyObject(params);

    /* Prepare the expressions for execution */

    paramLI = (ParamListInfo)palloc(offsetof(ParamListInfoData, params) + num_params * sizeof(ParamExternData));
    /* we have static list of params, so no hooks needed */
    paramLI->paramFetch = NULL;
    paramLI->paramFetchArg = NULL;
    paramLI->parserSetup = NULL;
    paramLI->parserSetupArg = NULL;
    paramLI->params_need_process = false;
    paramLI->numParams = num_params;
    paramLI->uParamInfo = DEFUALT_INFO;
    paramLI->params_lazy_bind = false;
    
    foreach (l, params) {
        Const* e = (Const*)lfirst(l);
        ParamExternData* prm = &paramLI->params[i];

        prm->ptype = param_types[i];
        prm->pflags = PARAM_FLAG_CONST;
        prm->value = e->constvalue;
        prm->isnull = e->constisnull;
        prm->tabInfo = NULL;
        i++;
    }
    return paramLI;
}

bool quickPlanner(List* querytree_list, Node* parsetree, const char*queryString, CommandDest dest, char* completionTag)
{
    if (!u_sess->attr.attr_common.enable_iud_fusion) {
        return false;
    }
    if (querytree_list == NULL || querytree_list->length != 1) {
        return false;
    }
    Query* query = (Query*)linitial(querytree_list);
    if (query->hasSubLinks || (query->rtable == NULL || query->rtable->length != 1) || query->groupClause != NULL) {
        return false;
    }
    if (query->commandType != CMD_UPDATE && query->commandType != CMD_DELETE) {
        return false;
    }
    RangeTblEntry* rte = (RangeTblEntry*)linitial(query->rtable);
    if (rte == NULL || rte->ispartrel) {
        return false;
    }
    constexpr uint32 plancache_namesize = 64; 
    if (strlen(queryString) >= plancache_namesize) {
        return false;
    }
    int nargs;
    Oid* param_types = NULL;
    List* paramListInfo = NULL;
    CachedPlan* cplan = NULL;
    List* plan_list = NIL;
    ParamListInfo paramLI;
    EState* estate = NULL;
    Portal portal;
    int eflags;
    long count;
    bool ret = false;
    query = substitute_const_with_parameters(query, &nargs, &param_types, &paramListInfo, &ret);
    if (ret) {
        return false;
    }
    if (paramListInfo == NULL || paramListInfo->length == 0) {
        return false;
    }
    StringInfo select_sql  = makeStringInfo();
    deparse_query((Query*)query, select_sql, NIL, false, false);
    if (select_sql->len >= (int)plancache_namesize) {
        return false;
    }
    PreparedStatement *entry = NULL;
    entry = FetchPreparedStatement(select_sql->data, false, false);
    CachedPlanSource* psrc = NULL;
    DestReceiver* receiver = CreateDestReceiver(dest);
    /* Create a new portal to run the query in */
    portal = CreateNewPortal();
    /* Don't display the portal in pg_cursors, it is for internal use only */
    portal->visible = false;
    if (dest == DestRemote) {
        SetRemoteDestReceiverParams(receiver, portal);
    }
    MemoryContext oldcxt = MemoryContextSwitchTo(PortalGetHeapMemory(portal));
    if (entry == NULL) {
        // MemoryContext oldcxt = MemoryContextSwitchTo(u_sess->cache_mem_cxt);
        psrc = CreateCachedPlan((Node*)parsetree,
        select_sql->data,
#ifdef PGXC
        select_sql->data,
#endif
        CreateCommandTag((Node*)parsetree));
        MemoryContextSwitchTo(oldcxt);
        List* new_querytree_list = NULL;
        new_querytree_list = list_make1(query);
        CompleteCachedPlan(psrc, new_querytree_list, NULL,  param_types, NULL, nargs, NULL, NULL, 0, true, select_sql->data);
        StorePreparedStatement(select_sql->data, psrc, true);
        entry = FetchPreparedStatement(select_sql->data, false, false);
        if (entry == NULL) {
            MemoryContextSwitchTo(oldcxt);
            return false;
        }
    }
    psrc = entry->plansource;
    if (!psrc->is_valid) {
        DropPreparedStatement(entry->stmt_name, true);
        return false;
    }
    if (nargs != entry->plansource->num_params) {
        DropPreparedStatement(entry->stmt_name, true);
        return false;
    }
    for (int i = 0; i < nargs; i++) {
        if (entry->plansource->param_types[i] != param_types[i]) {
            DropPreparedStatement(entry->stmt_name, true);
            return false;
        }
    }
    if (entry->plansource->num_params > 0) {
        paramLI = PrepareParamsFromConsts(psrc, paramListInfo, queryString);
    }

    OpFusion::clearForCplan((OpFusion*)psrc->opFusionObj, psrc);

    PG_TRY();
    {
        if (psrc->opFusionObj != NULL) {
            Assert(psrc->cplan == NULL);
            (void)RevalidateCachedQuery(psrc);
        }
    }
    PG_CATCH();
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION), errmsg("Invalid Param in QuickPlanner")));
        DropPreparedStatement(entry->stmt_name, true);
        return false;
    }
    PG_END_TRY();
        if (psrc->opFusionObj != NULL) {
            OpFusion *opFusionObj = (OpFusion *)(psrc->opFusionObj);
            if (opFusionObj->IsGlobal()) {
                opFusionObj = (OpFusion *)OpFusion::FusionFactory(opFusionObj->m_global->m_type,
                                                                u_sess->cache_mem_cxt, psrc, NULL, paramLI);
                Assert(opFusionObj != NULL);
            }
            opFusionObj->setPreparedDestReceiver(receiver);
            opFusionObj->useOuterParameter(paramLI);
            opFusionObj->setCurrentOpFusionObj(opFusionObj);

            CachedPlanSource* cps = opFusionObj->m_global->m_psrc;
            bool needBucketId = cps != NULL && cps->gplan;
            if (needBucketId) {
                setCachedPlanBucketId(cps->gplan, paramLI);
            }

            if (OpFusion::process(FUSION_EXECUTE, NULL, completionTag, false, NULL)) {
                MemoryContextSwitchTo(oldcxt);
                return true;
            }
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Bypass process Failed")));
        }
    PG_TRY();
    {
        /* Copy the plan's saved query string into the portal's memory */
        char* query_string = MemoryContextStrdup(PortalGetHeapMemory(portal), entry->plansource->query_string);

        /* Replan if needed, and increment plan refcount for portal */
        if (ENABLE_CACHEDPLAN_MGR) {
            cplan = GetWiseCachedPlan(psrc, paramLI, false);
        } else {
            cplan = GetCachedPlan(psrc, paramLI, false);
        }

        plan_list = cplan->stmt_list;

        /* 
        * Now we can define the portal.
        *
        * DO NOT put any code that could possibly throw an error between the
        * above GetCachedPlan call and here.
        */
        PortalDefineQuery(portal, NULL, query_string, entry->plansource->commandTag, plan_list, cplan);
        portal->nextval_default_expr_type = psrc->nextval_default_expr_type;

        /* incase change shared plan in execute stage */
        CopyPlanForGPCIfNecessary(entry->plansource, portal);
    }
    PG_CATCH();
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION), errmsg("Invalid Param in QuickPlanner2")));
        DropPreparedStatement(entry->stmt_name, true);
        return false;
    }
    PG_END_TRY();
    /* Plain old EXECUTE */
    eflags = 0;
    count = FETCH_ALL;
    if (OpFusion::IsSqlBypass(psrc, plan_list)) {
        psrc->opFusionObj =
            OpFusion::FusionFactory(OpFusion::getFusionType(cplan, paramLI, NULL),
                                    u_sess->cache_mem_cxt, psrc, NULL, paramLI);
        psrc->is_checked_opfusion = true;
        if (psrc->opFusionObj != NULL) {
            ((OpFusion*)psrc->opFusionObj)->setPreparedDestReceiver(receiver);
            ((OpFusion*)psrc->opFusionObj)->useOuterParameter(paramLI);
            ((OpFusion*)psrc->opFusionObj)->setCurrentOpFusionObj((OpFusion*)psrc->opFusionObj);

            if (OpFusion::process(FUSION_EXECUTE, NULL, completionTag, false, NULL)) {
                MemoryContextSwitchTo(oldcxt);
                return true;
            }
            Assert(0);
        }
    }
    MemoryContextSwitchTo(oldcxt);
    /*
     * Run the portal as appropriate.
     */
    PortalStart(portal, paramLI, eflags, GetActiveSnapshot());

    (void)PortalRun(portal, count, false, receiver, receiver, completionTag);

    PortalDrop(portal, false);

    if (estate != NULL)
        FreeExecutorState(estate);
    return true;
}
