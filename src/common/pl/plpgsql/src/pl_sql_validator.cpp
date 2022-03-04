/* -------------------------------------------------------------------------
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
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
 * pl_sql_validator.cpp
 *
 * IDENTIFICATION
 * src\common\pl\plpgsql\src\pl_sql_validator.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "utils/plpgsql.h"
#include "pgstat.h"
#include "catalog/pg_proc.h"
#include "parser/parse_node.h"
#include "client_logic/client_logic_proc.h"
#ifdef STREAMPLAN
#include "optimizer/streamplan.h"
#endif

#ifdef PGXC
#include "pgxc/pgxc.h"
#endif

#ifndef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

typedef struct pl_validate_expr_info : PLpgSQL_expr {
    SQLFunctionParseInfoPtr pinfo;
} pl_validate_expr_info;


static void plpgsql_crete_proc_parser_setup(struct ParseState* pstate, pl_validate_expr_info* pl_validate_info);
static ResourceOwnerData* create_temp_resourceowner(const char* name);
static void release_temp_resourceowner(ResourceOwnerData* resource_owner, bool is_commit);
static void pl_validate_plpgsql_stmt_if(PLpgSQL_stmt* stmt, PLpgSQL_function* func, SQLFunctionParseInfoPtr pinfo,
    SPIPlanPtr* plan, List** dynexec_list);
static void pl_validate_plpgsql_stmt_case(PLpgSQL_stmt* stmt, PLpgSQL_function* func, SQLFunctionParseInfoPtr pinfo,
    SPIPlanPtr* plan, List** dynexec_list);
static void pl_validate_stmt(PLpgSQL_stmt* stmt, PLpgSQL_function* func, SQLFunctionParseInfoPtr pinfo,
    SPIPlanPtr* plan, List** dynexec_list);
static void pl_validate_stmts(List* stmts, PLpgSQL_function* func, SQLFunctionParseInfoPtr pinfo, SPIPlanPtr* plan,
    List** dynexec_list);
static void plpgsql_fn_parser_replace_param_type_for_insert(struct ParseState* pstate, int param_no,
    Oid param_new_type,  Oid relid, const char *col_name);
static int find_input_param_count(char *argmodes, int n_modes, int param_no);

static int find_input_param_count(char *argmodes, int n_modes, int param_no)
{
    int real_param_no = 0;
    Assert(argmodes[param_no - 1] == PROARGMODE_IN || argmodes[param_no - 1] == PROARGMODE_INOUT ||
        argmodes[param_no - 1] == PROARGMODE_VARIADIC);
    for (int i = 0; i < param_no && i < n_modes; i++) {
        if (argmodes[i] == PROARGMODE_IN || argmodes[i] == PROARGMODE_INOUT ||
            argmodes[i] == PROARGMODE_VARIADIC) {
            real_param_no++;
        }
    }
    return real_param_no;
}

static void plpgsql_fn_parser_replace_param_type_for_insert(struct ParseState* pstate, int param_no,
    Oid param_new_type, Oid relid, const char *col_name)
{
    int real_param_no = 0;
    /* Find real param no - skip out params in count */
    if (param_no > 1) {
        PLpgSQL_expr *expr = (PLpgSQL_expr*)pstate->p_ref_hook_state;
        HeapTuple tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(expr->func->fn_oid));
        if (!HeapTupleIsValid(tuple)) {
            return;
        }
        char *argmodes = NULL;
        bool isNull;
        Datum proargmodes = SysCacheGetAttr(PROCNAMEARGSNSP, tuple, Anum_pg_proc_proargmodes, &isNull);
        ArrayType *arr = NULL;
        if (!isNull  && proargmodes != PointerGetDatum(NULL) && (arr = DatumGetArrayTypeP(proargmodes)) != NULL) {
            int n_modes = ARR_DIMS(arr)[0];
            argmodes = (char*)ARR_DATA_PTR(arr);
            if (param_no > n_modes) {
                /* prarmeter is plpgsql valiable - nothing to do */
                return;
            }
            /* just verify than used input parameter */
            real_param_no = find_input_param_count(argmodes, n_modes, param_no);
        }
        ReleaseSysCache(tuple);
    }
    if (real_param_no == 0) {
        real_param_no = param_no;
    }
    /* call sql function */
    sql_fn_parser_replace_param_type_for_insert(pstate, real_param_no, param_new_type, relid, col_name);
}

void plpgsql_crete_proc_parser_setup(struct ParseState* pstate, pl_validate_expr_info* pl_validate_info)
{
    plpgsql_parser_setup(pstate, pl_validate_info);
    pstate->p_create_proc_operator_hook = plpgsql_create_proc_operator_ref;
    pstate->p_create_proc_insert_hook = plpgsql_fn_parser_replace_param_type_for_insert;
    pstate->p_cl_hook_state = pl_validate_info->pinfo;
}

void pl_validate_expression(PLpgSQL_expr* expr, PLpgSQL_function* func, SQLFunctionParseInfoPtr pinfo, SPIPlanPtr* plan)
{
    if (!expr) {
        return;
    }
    pl_validate_expr_info pl_validate_info;
    *(PLpgSQL_expr*)&pl_validate_info = *expr;
    pl_validate_info.func = func;
    pl_validate_info.func->pre_parse_trig = true;
    pl_validate_info.pinfo = pinfo;
    /*
     * Generate plan
     */
    *plan =
        SPI_prepare_params(expr->query, (ParserSetupHook)plpgsql_crete_proc_parser_setup, (void*)&pl_validate_info, 0);
}

static inline bool is_fn_retval_handled(Oid ret_type)
{
    Oid attr_non_ret[] = {
        0, REFCURSOROID, VOIDOID, TRIGGEROID
    };
    for (int i = 0; i < int(sizeof(attr_non_ret) / sizeof(attr_non_ret[0])); i++) {
        if (ret_type == attr_non_ret[i]) {
            return false;
        }
    }
    return true;
}

static void check_plpgsql_fn_retval(Oid func_id, Oid ret_type, List* query_tree_list, bool* modify_target_list,
    JunkFilter** junk_filter)
{
    if (is_fn_retval_handled(ret_type)) {
        check_sql_fn_retval(func_id, ret_type, query_tree_list, modify_target_list, junk_filter, true);
    }
}

void pl_validate_function_sql(PLpgSQL_function* func, bool is_replace)
{
    if (!func) {
        return;
    }
    if (func->fn_rettype == TRIGGEROID) {
        /* we cannot handle functions with client logic on the server side */
        return;
    }
     /*
     * since function with the same func_id may exist already
     * first delete old gs_cl_proc info related to the previous create function call
     */

    delete_proc_client_info(func->fn_oid);
    if (func->fn_nargs == 0 && !is_fn_retval_handled(func->fn_rettype)) {
        /* Nothiing to validate - no input, no return */
        /* if there is some info in gs_cl_proc - remove it */
        return;
    }
    bool stored_prepase_trig = func->pre_parse_trig;
    bool is_already_replaced = false;
    bool is_rethrow_exeption = false;
    SPIPlanPtr plan = NULL;
    ResourceOwnerData* old_owner = NULL;
    List* dynexec_list = NIL;
    MemoryContext oldcontext = CurrentMemoryContext;
    HeapTuple tuple = NULL;
    PG_TRY();
    {
        old_owner = create_temp_resourceowner("pl_validator");
        int rc = 0;
        SPI_STACK_LOG("connect", NULL, NULL);
        if ((rc = SPI_connect()) != SPI_OK_CONNECT) {
            /* rethrow for clean up */
            PG_RE_THROW();
        }
        SQLFunctionParseInfoPtr pinfo;
        tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(func->fn_oid));
        if (!HeapTupleIsValid(tuple)) {
            /* rethrow for clean up */
            PG_RE_THROW();
        }
        pinfo = prepare_sql_fn_parse_info(tuple, NULL, InvalidOid);
        ReleaseSysCache(tuple);
        tuple = NULL;
        PLpgSQL_stmt_block* block = func->action;
        func->pre_parse_trig = true;
        pl_validate_stmt_block_in_subtransaction(block, func, pinfo, &plan, &dynexec_list);

        /*
         * we are going to update pg_proc and cl_gs_proc
         * any failure can leave those table inconsitent
         * we shoud rethrow exception if occured
         */
        is_rethrow_exeption = true;
        is_already_replaced = sql_fn_cl_rewrite_params(func->fn_oid, pinfo, is_replace);
        if (is_already_replaced) {
            ereport(ERROR, (errcode(ERRCODE_DUPLICATE_FUNCTION),
                    errmsg("function \"%s\" already exists with same argument types", func->fn_signature)));
        }
        is_rethrow_exeption = false;
        /*
         * for dynexecute we do not have plan here
         */
        if (plan) {
            check_plpgsql_fn_retval(func->fn_oid, func->fn_rettype, _SPI_get_querylist(plan), NULL, NULL);
            SPI_freeplan(plan);
            plan = NULL;
            verify_rettype_for_out_param(func->fn_oid);
        }
        if (dynexec_list) {
            check_plpgsql_fn_retval(func->fn_oid, func->fn_rettype, dynexec_list, NULL, NULL);
            list_free_deep(dynexec_list);
            dynexec_list = NULL;
            verify_rettype_for_out_param(func->fn_oid);
        }
    }

    PG_CATCH();
    {   
        if (HeapTupleIsValid(tuple)) {
            ReleaseSysCache(tuple);
            tuple = NULL;
        }
        if (is_rethrow_exeption) {
            PG_RE_THROW();
        }
        SPI_STACK_LOG("end", NULL, plan);
        if (plan) {
            SPI_freeplan(plan);
        }
        if (dynexec_list) {
            list_free_deep(dynexec_list);
        }
        _SPI_end_call(true);
        FlushErrorState();
    }

    PG_END_TRY();

    func->pre_parse_trig = stored_prepase_trig;
    SPI_STACK_LOG("finish", NULL, NULL);
    if (SPI_finish() == SPI_ERROR_UNCONNECTED) {
        SPICleanup();
    }
    if (old_owner != NULL) {
        release_temp_resourceowner(old_owner, false);
    }
    MemoryContextSwitchTo(oldcontext);
}

void pl_validate_stmt_block(PLpgSQL_stmt_block* block, PLpgSQL_function* func, SQLFunctionParseInfoPtr pinfo,
    SPIPlanPtr* plan, List** dynexec_list)
{
    if (!block) {
        return;
    }
    List* stmts = block->body;
    pl_validate_stmts(stmts, func, pinfo, plan, dynexec_list);
}

ResourceOwnerData* create_temp_resourceowner(const char* name)
{
    ResourceOwner tmpOwner = ResourceOwnerCreate(t_thrd.utils_cxt.CurrentResourceOwner, name,
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER));
    ResourceOwner currentOwner = t_thrd.utils_cxt.CurrentResourceOwner;
    t_thrd.utils_cxt.CurrentResourceOwner = tmpOwner;
    return currentOwner;
}
void release_temp_resourceowner(ResourceOwnerData* resource_owner, bool is_commit)
{
    ResourceOwner tmpOwner = t_thrd.utils_cxt.CurrentResourceOwner;
    ResourceOwnerRelease(tmpOwner, RESOURCE_RELEASE_BEFORE_LOCKS, is_commit, true);
    ResourceOwnerRelease(tmpOwner, RESOURCE_RELEASE_LOCKS, is_commit, true);
    ResourceOwnerRelease(tmpOwner, RESOURCE_RELEASE_AFTER_LOCKS, is_commit, true);
    t_thrd.utils_cxt.CurrentResourceOwner = resource_owner;
    ResourceOwnerDelete(tmpOwner);
}

void pl_validate_plpgsql_stmt_if(PLpgSQL_stmt* stmt, PLpgSQL_function* func, SQLFunctionParseInfoPtr pinfo,
    SPIPlanPtr* plan, List** dynexec_list)
{
    ListCell* lc = NULL;
    pl_validate_expression(((PLpgSQL_stmt_if*)stmt)->cond, func, pinfo, plan);
    pl_validate_stmts(((PLpgSQL_stmt_if*)stmt)->then_body, func, pinfo, plan, dynexec_list);
    foreach (lc, ((PLpgSQL_stmt_if*)stmt)->elsif_list) {
        PLpgSQL_if_elsif* elif = (PLpgSQL_if_elsif*)lfirst(lc);
        pl_validate_expression(elif->cond, func, pinfo, plan);
        pl_validate_stmts(elif->stmts, func, pinfo, plan, dynexec_list);
    }
    pl_validate_stmts(((PLpgSQL_stmt_if*)stmt)->else_body, func, pinfo, plan, dynexec_list);
}

void pl_validate_plpgsql_stmt_case(PLpgSQL_stmt* stmt, PLpgSQL_function* func, SQLFunctionParseInfoPtr pinfo,
    SPIPlanPtr* plan, List** dynexec_list)
{
    ListCell* lc = NULL;
    pl_validate_expression(((PLpgSQL_stmt_case*)stmt)->t_expr, func, pinfo, plan);
    foreach (lc, ((PLpgSQL_stmt_case*)stmt)->case_when_list) {
        PLpgSQL_case_when* cwt = (PLpgSQL_case_when*)lfirst(lc);

        pl_validate_expression(cwt->expr, func, pinfo, plan);
        pl_validate_stmts(cwt->stmts, func, pinfo, plan, dynexec_list);
    }
    if (((PLpgSQL_stmt_case*)stmt)->have_else) {
        pl_validate_stmts(((PLpgSQL_stmt_case*)stmt)->else_stmts, func, pinfo, plan, dynexec_list);
    }
}

void pl_validate_stmt(PLpgSQL_stmt* stmt, PLpgSQL_function* func, SQLFunctionParseInfoPtr pinfo,
    SPIPlanPtr* plan, List** dynexec_list)
{
    /* If current statement is GOTO, process it here, */
    switch ((enum PLpgSQL_stmt_types)stmt->cmd_type) {
        case PLPGSQL_STMT_BLOCK:
            pl_validate_stmt_block((PLpgSQL_stmt_block*)stmt, func, pinfo, plan, dynexec_list);
            break;

        case PLPGSQL_STMT_ASSIGN:
            break;

        case PLPGSQL_STMT_PERFORM:
            pl_validate_expression(((PLpgSQL_stmt_perform*)stmt)->expr, func, pinfo, plan);
            break;

        case PLPGSQL_STMT_IF:
            pl_validate_plpgsql_stmt_if(stmt, func, pinfo, plan, dynexec_list);
            break;

        case PLPGSQL_STMT_CASE:
            pl_validate_plpgsql_stmt_case(stmt, func, pinfo, plan, dynexec_list);
            break;

        case PLPGSQL_STMT_LOOP:
            pl_validate_stmts(((PLpgSQL_stmt_loop*)stmt)->body, func, pinfo, plan, dynexec_list);
            break;

        case PLPGSQL_STMT_WHILE:
            pl_validate_expression(((PLpgSQL_stmt_while*)stmt)->cond, func, pinfo, plan);
            pl_validate_stmts(((PLpgSQL_stmt_while*)stmt)->body, func, pinfo, plan, dynexec_list);
            break;

        case PLPGSQL_STMT_FORI:
            pl_validate_expression(((PLpgSQL_stmt_fori*)stmt)->lower, func, pinfo, plan);
            pl_validate_expression(((PLpgSQL_stmt_fori*)stmt)->upper, func, pinfo, plan);
            pl_validate_expression(((PLpgSQL_stmt_fori*)stmt)->step, func, pinfo, plan);
            pl_validate_stmts(((PLpgSQL_stmt_fori*)stmt)->body, func, pinfo, plan, dynexec_list);
            break;

        case PLPGSQL_STMT_FORS:
            pl_validate_expression(((PLpgSQL_stmt_fors*)stmt)->query, func, pinfo, plan);
            pl_validate_stmts(((PLpgSQL_stmt_fors*)stmt)->body, func, pinfo, plan, dynexec_list);
            break;

        case PLPGSQL_STMT_FORC:
            pl_validate_expression(((PLpgSQL_stmt_forc*)stmt)->argquery, func, pinfo, plan);
            pl_validate_stmts(((PLpgSQL_stmt_forc*)stmt)->body, func, pinfo, plan, dynexec_list);
            break;

        case PLPGSQL_STMT_FOREACH_A:
            pl_validate_expression(((PLpgSQL_stmt_foreach_a*)stmt)->expr, func, pinfo, plan);
            pl_validate_stmts(((PLpgSQL_stmt_foreach_a*)stmt)->body, func, pinfo, plan, dynexec_list);
            break;

        case PLPGSQL_STMT_EXIT:
            pl_validate_expression(((PLpgSQL_stmt_exit*)stmt)->cond, func, pinfo, plan);
            break;

        case PLPGSQL_STMT_RETURN:
            pl_validate_expression(((PLpgSQL_stmt_return*)stmt)->expr, func, pinfo, plan);
            break;

        case PLPGSQL_STMT_RETURN_NEXT:
            pl_validate_expression(((PLpgSQL_stmt_return_next*)stmt)->expr, func, pinfo, plan);
            break;

        case PLPGSQL_STMT_RETURN_QUERY:
            pl_validate_expression(((PLpgSQL_stmt_return_query*)stmt)->query, func, pinfo, plan);
            pl_validate_expression(((PLpgSQL_stmt_return_query*)stmt)->dynquery, func, pinfo, plan);
            break;

        case PLPGSQL_STMT_EXECSQL:
            pl_validate_expression(((PLpgSQL_stmt_execsql*)stmt)->sqlstmt, func, pinfo, plan);
            break;

        case PLPGSQL_STMT_DYNEXECUTE:
            validate_stmt_dynexecute((PLpgSQL_stmt_dynexecute*)stmt, func, pinfo, dynexec_list);
            break;

        case PLPGSQL_STMT_DYNFORS:
            pl_validate_expression(((PLpgSQL_stmt_dynfors*)stmt)->query, func, pinfo, plan);
            pl_validate_stmts(((PLpgSQL_stmt_dynfors*)stmt)->body, func, pinfo, plan, dynexec_list);
            break;

        case PLPGSQL_STMT_OPEN:
            pl_validate_expression(((PLpgSQL_stmt_open*)stmt)->query, func, pinfo, plan);
            pl_validate_expression(((PLpgSQL_stmt_open*)stmt)->dynquery, func, pinfo, plan);
            pl_validate_expression(((PLpgSQL_stmt_open*)stmt)->argquery, func, pinfo, plan);
            break;

        default:
            break;
    }
}

void pl_validate_stmts(List* stmts, PLpgSQL_function* func, SQLFunctionParseInfoPtr pinfo, SPIPlanPtr* plan,
    List** dynexec_list)
{
    if (!stmts) {
        return;
    }
    int num_stmts = list_length(stmts);
    int stmtid = 0;
    for (; stmtid < num_stmts; stmtid++) {
        PLpgSQL_stmt* stmt = (PLpgSQL_stmt*)list_nth(stmts, stmtid);
        pl_validate_stmt(stmt, func, pinfo, plan, dynexec_list);
    }
}

