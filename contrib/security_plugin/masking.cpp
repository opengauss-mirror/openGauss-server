/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * ---------------------------------------------------------------------------------------
 * 
 * masking.cpp
 * 
 * IDENTIFICATION
 *        src/contrib/security_plugin/masking.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <unordered_set>
#include "postgres.h"
#include "nodes/nodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/params.h"
#include "nodes/primnodes.h"
#include "parser/parse_clause.h"
#include "parser/parse_target.h"
#include "parser/parse_expr.h"
#include "parser/parse_oper.h"
#include "parser/parse_func.h"
#include "parser/parse_utilcmd.h"
#include "parser/parse_relation.h"
#include "utils/numeric.h"
#include "catalog/pg_proc.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"
#include "parser/parse_type.h"
#include "nodes/makefuncs.h"
#include "parser/parse_coerce.h"
#include "commands/tablespace.h"
#include "masking.h"
#include "access_audit.h"
#include "gs_threadlocal.h"
#include "gs_policy_plugin.h"
#include "gs_policy/gs_vector.h"
#include "gs_policy/gs_policy_masking.h"

#define BUFFSIZE 256

void reset_node_location()
{
    t_thrd.security_policy_cxt.node_location = 0;
}

static void parse_func(Node *expr);
static void get_var_value(const List *rtable, Var *var, PolicyLabelItem *full_column, PolicyLabelItem *view_name);

static void get_fqdn_by_joinalias(const List *rtable, const RangeTblEntry *rte, const Var *var,
                                  PolicyLabelItem *full_column, PolicyLabelItem *view_name)
{
    ListCell *l = NULL;
    int joinpos = 1;
    /* Scan all joinaliasvars of JOIN RTE.
     * varattno marks its orders in joinaliasvars,
     * and we can use it to get the real var of join statement
     */
    foreach (l, rte->joinaliasvars) {
        if (joinpos != (int)var->varattno) {
            ++joinpos;
            continue;
        }
        /* Get real join var information */
        Var *joinvar = (Var *)lfirst(l);
        get_var_value(rtable, joinvar, full_column, view_name);
        break;
    }
}

static void parse_value(const List *rtable, RangeTblEntry *rte, Var *var,
                        PolicyLabelItem *full_column, PolicyLabelItem *view_name)
{
    switch (rte->rtekind) {
        case RTE_RELATION: /* relation ID of current table */
            if (OidIsValid(rte->relid)) {
                get_fqdn_by_relid(rte, full_column, var, view_name);
            }
            break;
        case RTE_JOIN: /* RTE is JOIN kind */
            get_fqdn_by_joinalias(rtable, rte, var, full_column, view_name);
            break;
        case RTE_FUNCTION: /* relation is function */
            parse_func(rte->funcexpr);
            break;
        default:
            /* Here is no need to parse subquery or CTE, because subquery or CTE will mask itself. */
            break;
    }
}

static void get_var_value(const List *rtable, Var *var, PolicyLabelItem *full_column, PolicyLabelItem *view_name)
{
    ListCell *l = NULL;
    int pos = 1;
    /* In TargetList, each TargetEntry's varno points to rtable.
     * So, the RangeTableEntry corresponding to each varno needs to be got first.
     * Additionally, parse full column name for every (var-rte) pair.
     */
    foreach (l, rtable) {
        if (pos != (int)var->varno) {
            ++pos;
            continue;
        }

        RangeTblEntry *rte = (RangeTblEntry *) lfirst(l);
        parse_value(rtable, rte, var, full_column, view_name);
        break;
    }
}

static bool mask_expr_node(ParseState *pstate, Expr*& expr,
    const policy_set *policy_ids, masking_result *result, List* rtable, bool can_mask = true);
bool handle_masking_node(ParseState *pstate, Expr*& src_expr,
    const policy_set *policy_ids, masking_result *result, List *rtable, bool can_mask = true);

static bool mask_func(ParseState *pstate, Expr*& expr,
    const policy_set *policy_ids, masking_result *result, List* rtable, bool can_mask = true)
{
    if (expr == NULL) {
        return false;
    }
    bool is_masking = false;
    if (nodeTag(expr) == T_FuncExpr) {
        FuncExpr *fe = (FuncExpr *)(expr);
        PolicyLabelItem func_value;
        if (get_function_name(fe->funcid, &func_value)) {
            set_result_set_function(func_value);
        }
        if (fe->args != NULL) {
            ListCell* temp = NULL;
            foreach(temp, fe->args) {
                Node *&item = (Node*&)lfirst(temp);
                bool expr_masked = mask_expr_node(pstate, (Expr*&)item, policy_ids, result, rtable, can_mask);
                is_masking = is_masking || expr_masked;
            }
        }
    }
    return is_masking;
}

static void parse_func(Node* expr)
{
    switch (nodeTag(expr)) {
        case T_FuncExpr:
        {
            FuncExpr *fe = (FuncExpr *)expr;
            {
                PolicyLabelItem func_value;
                if (get_function_name(fe->funcid, &func_value)) {
                    set_result_set_function(func_value);
                }
            }
            if (fe->args != NIL) {
                ListCell* temp = NULL;
                foreach(temp, fe->args) {
                    Node *item = (Node*)lfirst(temp);
                    if (IsA(item, FuncExpr)) {
                        parse_func(item);
                    }
                }
            }
        }
        break;
        default:
            break;
    }
}

static bool get_function_id(int vartype, const char* funcname, Oid *funcid, Oid *rettype,
                            Oid schemaid = SchemaNameGetSchemaOid("pg_catalog", true))
{
    CatCList   *catlist = SearchSysCacheList1(PROCNAMEARGSNSP, CStringGetDatum(funcname));
    if (catlist != NULL) {
        for (int i = 0; i < catlist->n_members; ++i) {
            HeapTuple    proctup = &catlist->members[i]->tuple;
            Form_pg_proc procform = (Form_pg_proc) GETSTRUCT(proctup);
            if (procform && (int)procform->prorettype == vartype && procform->pronamespace == schemaid) {
                (*funcid) = HeapTupleGetOid(proctup);
                (*rettype) = procform->prorettype;
                break;
            }
        }
        ReleaseSysCacheList(catlist);
        return true;
    }
    return false;
}

static Node* create_predefined_function(const char* funcname, int funcid, int rettype, Node* arg, int funccollid,
                                        CoercionForm funcformat = COERCE_EXPLICIT_CALL)
{
    FuncExpr   *funcexpr = makeNode(FuncExpr);
    funcexpr->funcid = funcid;
    funcexpr->funcresulttype = rettype;
    funcexpr->funcretset = false;
    funcexpr->funcvariadic = false;
    funcexpr->funcformat = funcformat;

    if (IsA(arg, Var)) {
        Var* var = (Var*)arg;
        var->location += t_thrd.security_policy_cxt.node_location;
        funcexpr->location = var->location;
        var->location += strlen(funcname) + 1;
        t_thrd.security_policy_cxt.node_location += (strlen(funcname) + 2);
    } else if(IsA(arg, Const)) {
        Const* const_arg = (Const*)arg;
        const_arg->location += t_thrd.security_policy_cxt.node_location;
        funcexpr->location = const_arg->location;
        const_arg->location += strlen(funcname) + 1;
        t_thrd.security_policy_cxt.node_location += (strlen(funcname) + 2);
    } else if(IsA(arg, FuncExpr)) {
        FuncExpr* fe = (FuncExpr*)arg;
        fe->location += t_thrd.security_policy_cxt.node_location;
        funcexpr->location = fe->location;
        fe->location += strlen(funcname) + 1;
        t_thrd.security_policy_cxt.node_location += (strlen(funcname) + 2);
    }

    funcexpr->args = lappend(funcexpr->args, arg);
    funcexpr->funccollid = funccollid;
    funcexpr->inputcollid = 100; /* OID of collation that function should use */

    return (Node*)funcexpr;
}

static Node* create_relabel_type(Node* func, int resulttype, int location,
                                 CoercionForm relabelformat = COERCE_EXPLICIT_CAST)
{
    if (func == NULL) {
        return NULL;
    }
    RelabelType   *typeexpr = makeNode(RelabelType);
    if (!typeexpr)return NULL;
    typeexpr->arg = (Expr*)func;
    typeexpr->resulttype = resulttype;
    typeexpr->resulttypmod = -1;
    typeexpr->resultcollid = 100; /* OID of collation */
    typeexpr->relabelformat = relabelformat;
    typeexpr->location = location;
    return (Node*)typeexpr;
}

static inline Node* create_string_node(ParseState *pstate, const char* letter, int location, int col_type = TEXTOID)
{
    Node * const_node = (Node *) make_const(pstate, makeString((char*)letter), location);
    const_node = coerce_type(pstate, const_node, ((Const*)const_node)->consttype, col_type, -1,
                             COERCION_IMPLICIT, COERCE_IMPLICIT_CAST, -1);
    return const_node;
}


Node* create_integer_node(ParseState *pstate, int value, int location, int col_type, bool make_cast)
{
    Node * const_node = (Node *) make_const(pstate, makeInteger(value), location);
    if (make_cast) {
        const_node = coerce_type(pstate, const_node, ((Const*)const_node)->consttype, col_type, -1,
                                 COERCION_IMPLICIT, COERCE_IMPLICIT_CAST, -1);
    }
    return const_node;
}

static inline Node* create_empty_node(ParseState *pstate, int location, int col_type = INT4OID, bool make_cast = true)
{
    Const* const_node = (Const*)create_integer_node(pstate, 0, location, col_type, make_cast);
    if (const_node) {
        const_node->constisnull = false;
        const_node->constbyval = true;
    }
    return (Node*)const_node;
}

static Node *create_repeat_function(ParseState *pstate, const char *letter, Node *arg)
{
    Var *var = (Var*)arg;
    Oid funcid = 0;
    Oid rettype = 0;
    if (get_function_id(TEXTOID, "repeat", &funcid, &rettype)) {
        Node *const_node = create_string_node(pstate, letter, var->location);
        Node *repeat_func = create_predefined_function("repeat", funcid, rettype, const_node, 100);
        FuncExpr *funcexpr = (FuncExpr*)repeat_func;
        if (get_function_id(INT4OID, "length", &funcid, &rettype)) {
            Node *length_func = create_predefined_function("length", funcid, rettype, arg, 0);
            funcexpr->args = lappend(funcexpr->args, (Node*)length_func);
        }
        return repeat_func;
    }
    return arg;
}

static Node *shuffle_function(ParseState *pstate, Var *var, masking_result *result, long long polid,
                              const char *full_column)
{
    bool make_cast = false;
    Node *shuffle_func_node = NULL;
    switch (var->vartype) {
        case BPCHAROID:
        case VARCHAROID:
        case NVARCHAR2OID:
            make_cast = true;
        case TEXTOID:
        {
            Oid funcid = 0;
            Oid rettype = TEXTOID;
            get_function_id(TEXTOID, "shufflemasking", &funcid, &rettype);
            if (funcid > 0) {
                shuffle_func_node = create_predefined_function("shufflemasking", funcid, TEXTOID, (Node*)var, 100);
                if (make_cast) {
                    Expr *cast_fun = (Expr*)create_relabel_type(shuffle_func_node, var->vartype, var->location);
                    if (cast_fun != NULL) { /* success */
                        shuffle_func_node = (Node*)cast_fun;
                    }
                }
                (*result)[polid][M_SHUFFLE].insert(full_column);
            }
        }
        break;
    }
    return shuffle_func_node;
}

static Node *random_function(ParseState *pstate, Var *var, masking_result *result, long long polid,
                             const char *full_column)
{
    bool cast = false;
    Node *random_func_node = NULL;
    switch (var->vartype) {
        case BPCHAROID:
        case VARCHAROID:
        case NVARCHAR2OID:
            cast = true;
        case TEXTOID:
        {
            Oid funcid = 0;
            Oid rettype = TEXTOID;
            get_function_id(TEXTOID, "randommasking", &funcid, &rettype);
            if (funcid > 0) {
                random_func_node = create_predefined_function("randommasking", funcid, TEXTOID, (Node*)var, 100);
                if (cast) {
                    Expr *cast_fun = (Expr*)create_relabel_type(random_func_node, var->vartype, var->location);
                    if (cast_fun != NULL) { /* success */
                        random_func_node = (Node*)cast_fun;
                    }
                }
                (*result)[polid][M_RANDOM].insert(full_column);
            }
        }
        break;
    }
    return random_func_node;
}

static Node *all_digits_function(ParseState *pstate, Var *var,
    masking_result *result, long long polid, const char *full_column,
    const gs_stl::gs_vector<gs_stl::gs_string> *func_params)
{
    bool make_cast_f = false;
    Node *digits_func_node = NULL;
    switch (var->vartype) {
        case BPCHAROID:
        case VARCHAROID:
        case NVARCHAR2OID:
            make_cast_f = true;
        case TEXTOID:
        {
            Oid funcid = 0;
            Oid rettype = TEXTOID;
            get_function_id(TEXTOID, "alldigitsmasking", &funcid, &rettype);
            if (funcid > 0) {
                digits_func_node = create_predefined_function("alldigitsmasking", funcid, TEXTOID, (Node*)var, 100);
                if (func_params->size()) {
                    Node *constnode = create_string_node(pstate, func_params->begin()->c_str(), var->location);
                    FuncExpr *funcexpr = (FuncExpr*)digits_func_node;
                    funcexpr->args = lappend(funcexpr->args, constnode);
                }
                if (make_cast_f) {
                    Expr *castfunc = (Expr*)create_relabel_type(digits_func_node, var->vartype, var->location);
                    if (castfunc != NULL) { /* success */
                        digits_func_node = (Node*)castfunc;
                    }
                }
                (*result)[polid][M_ALLDIGITS].insert(full_column);
            }
        }
        break;
    }
    return digits_func_node;
}

static Node *email_function(ParseState *pstate, int masking_behavious,
    Var *var, masking_result *result, long long polid, const char *full_column,
    const gs_stl::gs_vector<gs_stl::gs_string> *func_params)
{
    bool makecast = false;
    Node *email_func_node = NULL;
    switch (var->vartype) {
        case BPCHAROID:
        case VARCHAROID:
        case NVARCHAR2OID:
            makecast = true;
        case TEXTOID:
        {
            Oid funcid = 0;
            Oid rettype = TEXTOID;
            const char *funcname = (masking_behavious == M_BASICEMAIL) ? "basicemailmasking" : "fullemailmasking";
            get_function_id(TEXTOID, funcname, &funcid, &rettype);
            if (funcid > 0) {
                email_func_node = create_predefined_function(funcname, funcid, TEXTOID, (Node*)var, 100);
                if (func_params->size()) {
                    Node *const_node = create_string_node(pstate, func_params->begin()->c_str(), var->location);
                    FuncExpr *funcexpr = (FuncExpr*)email_func_node;
                    funcexpr->args = lappend(funcexpr->args, const_node);
                }
                if (makecast) {
                    Expr *cast_fun = (Expr*)create_relabel_type(email_func_node, var->vartype, var->location);
                    if (cast_fun != NULL) { /* success */
                        email_func_node = (Node*)cast_fun;
                    }
                }
                (*result)[polid][masking_behavious].insert(full_column);
            }
        }
        break;
    }
    return email_func_node;
}

static Node *maskall_function(ParseState *pstate,
    int masking_behavious, Var *var, masking_result *result, long long polid, const char *full_column,
    const gs_stl::gs_vector<gs_stl::gs_string> *func_params)
{
    char time_str[BUFFSIZE] = {0};
    int printed_size = 0;
    Node *maskall_func_node = NULL;
    switch (var->vartype) {
        case BOOLOID:
        {
            Node *const_int_node = create_integer_node(pstate, 0, var->location, var->vartype);
            if (const_int_node != NULL) { /* success */
                maskall_func_node = const_int_node;
            }
        }
        break;
        case RELTIMEOID:
            printed_size = snprintf_s(time_str, sizeof(time_str), sizeof(time_str) - 1, "1970");
            securec_check_ss(printed_size, "\0", "\0");
        case TIMEOID:
        case TIMETZOID:
        case INTERVALOID:
            if (!printed_size) {
                printed_size = snprintf_s(time_str, sizeof(time_str), sizeof(time_str) - 1, "00:00:00.0000+00");
                securec_check_ss(printed_size, "\0", "\0");
            }
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
        case SMALLDATETIMEOID:
        case ABSTIMEOID:
        {
            if (!printed_size) {
                printed_size = snprintf_s(time_str, sizeof(time_str), sizeof(time_str) - 1, "1970-01-01 00:00:00.0000");
                securec_check_ss(printed_size, "\0", "\0");
            }
            Node *const_node = create_string_node(pstate, time_str, var->location, var->vartype);
            if (const_node != NULL) { /* success */
                maskall_func_node = const_node;
            }
        }
        break;
        case TEXTOID:
        case BPCHAROID:
        case VARCHAROID:
        case NVARCHAR2OID:
        case NAMEOID:
            if (func_params->size() > 0)
                maskall_func_node = create_repeat_function(pstate, func_params->begin()->c_str(), (Node*)var);
            else
                maskall_func_node = create_repeat_function(pstate, "x", (Node*)var);
            break;
        case INT8OID:
        case INT4OID:
        case INT2OID:
        case INT1OID:
        case NUMERICOID:
        case FLOAT4OID: /* real */
        case FLOAT8OID:
        case CASHOID:
        {
            Node *const_int_node = create_integer_node(pstate, 0, var->location,
                                                        var->vartype, (var->vartype != CASHOID));
            if (const_int_node != NULL) { /* success */
                maskall_func_node = const_int_node;
            }
        }
        break;
        default: /* wrong column type */
        {
            ereport(WARNING, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("Unsupported type of column %s will not be masked by policy %lld.", full_column, polid)));
        }
        break;
    }
    (*result)[polid][M_MASKALL].insert(full_column);
    return maskall_func_node;
}

static Node *credit_card_function(ParseState *pstate,
    int masking_behavious, Var *var, masking_result *result, long long polid, const char *full_column,
    const gs_stl::gs_vector<gs_stl::gs_string> *func_params)
{
    bool make_cast = false;
    Node *credit_func_node = NULL;
    switch (var->vartype) {
        case BPCHAROID:
        case VARCHAROID:
        case NVARCHAR2OID:
            make_cast = true;
        case TEXTOID:
        {
            Oid funcid = 0;
            Oid rettype = 25;
            get_function_id(TEXTOID, "creditcardmasking", &funcid, &rettype);
            if (funcid > 0) {
                credit_func_node = create_predefined_function("creditcardmasking", funcid, TEXTOID, (Node*)var, 100);
                if (func_params->size()) {
                    Node *const_node = create_string_node(pstate, func_params->begin()->c_str(), var->location);
                    FuncExpr *fexpr = (FuncExpr*)credit_func_node;
                    fexpr->args = lappend(fexpr->args, const_node);
                }
                if (make_cast) {
                    Expr* castfun = (Expr*)create_relabel_type(credit_func_node, var->vartype, var->location);
                    if (castfun != NULL) { /* success */
                        credit_func_node = (Node*)castfun;
                    }
                }
                (*result)[polid][M_CREDIT_CARD].insert(full_column);
            }
        }
        break;
    }
    return credit_func_node;
}

static Node *mask_node_by_behavious(bool *is_masking, int masking_behavious, ParseState *pstate, Var* var,
    masking_result *result, long long polid, const char* full_column,
    const gs_stl::gs_vector<gs_stl::gs_string> *func_params)
{
    Node *masked_node = NULL;
    switch (masking_behavious) {
        case M_CREDIT_CARD:
        {
            (*is_masking) = true;
            if ((masked_node = credit_card_function(pstate, masking_behavious, var, result,
                                                    polid, full_column, func_params)) == NULL) {
                masked_node = maskall_function(pstate, masking_behavious, var,
                                               result, polid, full_column, func_params);
            }
        }
        break;
        case M_BASICEMAIL:
        case M_FULLEMAIL:
        {
            (*is_masking) = true;
            if ((masked_node = email_function(pstate, masking_behavious, var, result,
                                              polid, full_column, func_params)) == NULL) {
                masked_node = maskall_function(pstate, masking_behavious, var, result,
                                               polid, full_column, func_params);
            }
        }
        break;
        case M_ALLDIGITS:
        {
            (*is_masking) = true;
            if ((masked_node = all_digits_function(pstate, var, result, polid,
                                                   full_column, func_params)) == NULL) {
                masked_node = maskall_function(pstate, masking_behavious, var, result,
                                               polid, full_column, func_params);
            }
        }
        break;
        case M_SHUFFLE:
        {
            (*is_masking) = true;
            if ((masked_node = shuffle_function(pstate, var, result, polid, full_column)) == NULL) {
                masked_node = maskall_function(pstate, masking_behavious, var,
                                               result, polid, full_column, func_params);
            }
        }
        break;
        case M_RANDOM:
        {
            (*is_masking) = true;
            if ((masked_node = random_function(pstate, var, result, polid, full_column)) == NULL) {
                masked_node = maskall_function(pstate, masking_behavious, var, result,
                                               polid, full_column, func_params);
            }
        }
        break;
        default:
            (*is_masking) = true;
            masked_node = maskall_function(pstate, masking_behavious, var, result,
                                           polid, full_column, func_params);
            break;
    }
    if (masked_node == NULL) {
        (*is_masking) = false;
    }
    return masked_node;
}

bool handle_masking_node(ParseState *pstate, Expr*& src_expr,
    const policy_set *policy_ids, masking_result *result, List* rtable, bool can_mask)
{
    if (src_expr == NULL || policy_ids->empty()) {
        return false;
    }
    Var* var = (Var*)(src_expr);

    PolicyLabelItem full_column(0, 0, O_COLUMN), view_name;
    get_var_value(rtable, var, &full_column, &view_name); /* fqdn column name */
    /* Varattno 'zero' references the whole tuple. */
    if (full_column.m_obj_type == O_COLUMN && var->varattno == 0 &&
        OidIsValid(full_column.m_object) && is_masked_relation_enabled(full_column.m_object)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("Un-support operation for whole tuple contains masked column in table \"%s\".",
                get_rel_name(full_column.m_object))));
    }
    if (full_column.empty() && rtable != NIL) { /* sub query */
        bool is_found = false;
        audit_open_relation(rtable, var, &full_column, &is_found);
    }
    bool is_masking = false;
    int masking_behavious = 0;
    long long polid = 0;

    gs_stl::gs_vector<gs_stl::gs_string> func_params;
    if (check_masking_policy_action(policy_ids, &full_column, &view_name, &masking_behavious, &polid, &func_params)) {
        gs_stl::gs_string log_column;
        full_column.get_fqdn_value(&log_column);

        /* When mask a column which not allowed to be masked, we will report an error */
        if (!can_mask) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Un-support operation for masking column.")));
        }

        Node* masked_node = NULL;
        masked_node = mask_node_by_behavious(&is_masking, masking_behavious, pstate, var,
                                             result, polid, log_column.c_str(), &func_params);

        if (is_masking) {
            ereport(DEBUG2, (errmodule(MOD_PARSER),
                errmsg("Column %s will be masked by masking behavious %d", log_column.c_str(), masking_behavious)));
        }

        if (masked_node != NULL) {
            src_expr = (Expr*)masked_node;
        }
    }
    return is_masking;
}


static inline void parse_var(Var* var, PolicyLabelItem *full_column, PolicyLabelItem *view_name, List* rtable)
{
    get_var_value(rtable, var, full_column, view_name); /* fqdn column name */
    if (full_column->empty() && rtable != NIL) { /* sub query */
        bool is_found = false;
        audit_open_relation(rtable, var, full_column, &is_found);
    }
}

static void parse_case_expr(Node* expr, List* rtable)
{
    PolicyLabelItem view_name, str_result;
    switch (nodeTag(expr)) {
        case T_Var:
        {
            parse_var((Var*)expr, &str_result, &view_name, rtable);
        }
        break;
        case T_RelabelType:
        {
            RelabelType *relabel = (RelabelType *) expr;
            if (relabel) {
                switch (nodeTag(relabel->arg)) {
                    case T_FuncExpr:
                    {
                        parse_func((Node*)relabel->arg);
                    }
                    break;
                    case T_Var:
                    {
                        parse_var((Var*)relabel->arg, &str_result, &view_name, rtable);
                    }
                    break;
                    default:
                        break;
                }
            }
        }
        break;
        case T_CaseWhen:
        {
            CaseWhen   *when = (CaseWhen *) expr;
            switch (nodeTag(when->expr)) {
                case T_OpExpr:
                {
                    OpExpr* op_expr = (OpExpr*)when->expr;
                    List* args = op_expr->args;
                    ListCell   *l = NULL;
                    Node* n = NULL;
                    foreach(l, args)
                    {
                        n = (Node*)lfirst(l);
                        parse_case_expr(n, rtable);
                    }
                }
                break;
                case T_Var:
                {
                    parse_var((Var*)when->expr, &str_result, &view_name, rtable);
                }
                break;
                default:
                    break;
            }
            switch (nodeTag(when->result)) {
                case T_Var:
                {
                    parse_var((Var*)when->result, &str_result, &view_name, rtable);
                }
                break;
                default:
                    break;
            }
        }
        break;
        default:
            break;
    }
}

static bool mask_list_parameters(List **params, ParseState *pstate, bool *is_masking, const policy_set *policy_ids,
                                 masking_result *result, List* rtable, bool can_mask = true)
{
    List* masked_list = NIL;
    ListCell   *lc = NULL;
    foreach(lc, (*params)) {
        Node *item = (Node *) lfirst(lc);
        switch (nodeTag(item)) {
            case T_Aggref:
            {
                Aggref* agg = (Aggref *) item;
                if (agg && agg->args != NIL && list_length(agg->args) > 0) {
                    mask_list_parameters(&(agg->args), pstate, is_masking, policy_ids, result, rtable, can_mask);
                }
            }
            break;
            case T_OpExpr:
            {
                OpExpr* opexpr = (OpExpr*)item;
                if (opexpr && opexpr->args != NIL && list_length(opexpr->args) > 0) {
                    mask_list_parameters(&(opexpr->args), pstate, is_masking, policy_ids, result, rtable, can_mask);
                }
            }
            break;
            case T_Var:
            case T_RelabelType:
            case T_FuncExpr:
            {
                bool expr_masked = mask_expr_node(pstate, (Expr*&)item, policy_ids, result, rtable, can_mask);
                *is_masking = expr_masked || *is_masking;
            }
            break;
            case T_TargetEntry:
            {
                TargetEntry*& target_entry = (TargetEntry*&)item;
                bool expr_masked = parser_target_entry(pstate, target_entry, policy_ids, result, rtable, can_mask);
                *is_masking = expr_masked || *is_masking;
            }
            break;
            case T_CaseExpr:
            {
                CaseExpr   *expr = (CaseExpr *)item;
                ListCell   *lc = NULL;

                PolicyLabelItem defresult, view_name;
                foreach(lc, expr->args) /* when expr */
                {
                    Node   *when = (Node *) lfirst(lc);
                    parse_case_expr(when, rtable);
                }
                if (expr->defresult) {
                    switch (nodeTag(expr->defresult)) {
                        case T_Var:
                        {
                            parse_var((Var*)expr->defresult, &defresult, &view_name, rtable);
                        }
                        break;
                        default:
                            break;
                    }
                }
            }
            break;
            default:
                break;
        }
        masked_list = lappend(masked_list, item);
    }
    if (*is_masking)
        (*params) = masked_list;
    return *is_masking;
}

static bool mask_sublink(ParseState *pstate, Expr*& expr,
    const policy_set *policy_ids, masking_result *result, List* rtable, bool can_mask)
{
    if (expr == NULL) {
        return false;
    }
    SubLink *sublink = (SubLink *) expr;
    Query *query = (Query *) sublink->subselect;
    ListCell* temp = NULL;
    bool is_masking = false;
    foreach (temp, query->targetList) {
        TargetEntry *old_tle = (TargetEntry *) lfirst(temp);
        is_masking = parser_target_entry(pstate, old_tle, policy_ids, result, query->rtable, can_mask) || is_masking;
    }
    return is_masking;
}

static bool mask_expr_node(ParseState *pstate, Expr*& expr,
    const policy_set *policy_ids, masking_result *result, List* rtable, bool can_mask)
{
    if (expr == NULL) {
        return false;
    }
    switch (nodeTag(expr)) {
        case T_SubLink:
            return mask_sublink(pstate, expr, policy_ids, result, rtable, can_mask);
        case T_FuncExpr:
            return mask_func(pstate, expr, policy_ids, result, rtable, can_mask);
        case T_Var:
            return handle_masking_node(pstate, expr, policy_ids, result, rtable, can_mask);
        case T_RelabelType: {
            RelabelType *relabel = (RelabelType *) expr;
            if (relabel->arg != NULL) {
                return mask_expr_node(pstate, (Expr *&)relabel->arg, policy_ids, result, rtable, can_mask);
            }
            break;
        }
        case T_CoerceViaIO: {
            CoerceViaIO *coerce = (CoerceViaIO *) expr;
            if (coerce->arg != NULL) {
                return mask_expr_node(pstate, (Expr *&)coerce->arg, policy_ids, result, rtable, false);
            }
            break;
        }
        case T_Aggref: {
            Aggref *agg = (Aggref *) expr;
            if (agg->args != NIL && list_length(agg->args) > 0) {
                bool is_masking = false;
                mask_list_parameters(&(agg->args), pstate, &is_masking, policy_ids, result, rtable, can_mask);
                return is_masking;
            }
            break;
        }
        case T_OpExpr: {
            OpExpr *opexpr = (OpExpr *) expr;
            if (opexpr->args != NIL && list_length(opexpr->args) > 0) {
                bool is_masking = false;
                mask_list_parameters(&(opexpr->args), pstate, &is_masking, policy_ids, result, rtable, can_mask);
                return is_masking;
            }
            break;
        }
        default:
            break;
    }
    return false;
}

bool parser_target_entry(ParseState *pstate, TargetEntry *&old_tle,
    const policy_set *policy_ids, masking_result *result, List* rtable, bool can_mask)
{
    Node* src_expr = (Node*)old_tle->expr;
    bool is_masking = false;
    switch (nodeTag(src_expr)) {
        case T_SubLink:
        {
            SubLink    *sublink = (SubLink *) src_expr;
            Query      *query = (Query *) sublink->subselect;
            ListCell* temp = NULL;
            foreach (temp, query->targetList) {
                TargetEntry *old_tle = (TargetEntry *) lfirst(temp);
                parser_target_entry(pstate, old_tle, policy_ids, result, query->rtable, can_mask);
            }
        }
        break;
        case T_Var:
        {
            is_masking = handle_masking_node(pstate, (Expr *&)old_tle->expr, policy_ids,
                                             result, rtable, can_mask);
            if (is_masking) {
                old_tle->resorigtbl = 0;
                old_tle->resorigcol = 0;
            }
        }
        break;
        case T_Aggref:
        case T_OpExpr:
        case T_RelabelType:
        case T_FuncExpr:
        case T_CoerceViaIO:
        {
            if (mask_expr_node(pstate, (Expr *&)old_tle->expr, policy_ids, result, rtable, can_mask)) {
                old_tle->resorigtbl = 0;
                old_tle->resorigcol = 0;
                is_masking = true;
            }
        }
        break;
        default:
            break;
    }
    return is_masking;
}
