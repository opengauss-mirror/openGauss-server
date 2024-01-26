/* -------------------------------------------------------------------------
 *
 * functions.h
 *		Declarations for execution of SQL-language functions.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/functions.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef FUNCTIONS_H
#define FUNCTIONS_H

#include "nodes/execnodes.h"
#include "tcop/dest.h"

/* This struct is known only within executor/functions.c */
typedef struct SQLFunctionParseInfo* SQLFunctionParseInfoPtr;

extern Datum fmgr_sql(PG_FUNCTION_ARGS);

extern SQLFunctionParseInfoPtr prepare_sql_fn_parse_info(HeapTuple procedureTuple, Node* call_expr, Oid inputCollation);

extern void sql_fn_parser_replace_param_type(struct ParseState* pstate, int param_no, Var* var);

extern void sql_fn_parser_replace_param_type_for_insert(struct ParseState* pstate, int param_no, Oid param_new_type,
    Oid relid, const char *col_name);

extern void plpgsql_fn_parser_replace_param_type(struct ParseState* pstate, int param_no, Var* var);

extern void sql_fn_parser_setup(struct ParseState* pstate, SQLFunctionParseInfoPtr pinfo);

extern void sql_fn_replace(struct ParseState* pstate, SQLFunctionParseInfoPtr pinfo);

extern bool check_sql_fn_retval(Oid func_id, Oid rettype, List* queryTreeList, bool* modifyTargetList,
    JunkFilter** junkFilter, bool plpgsql_validation = false);
typedef bool (*checkSqlFnRetval)(Oid func_id, Oid rettype, List* queryTreeList, bool* modifyTargetList,
    JunkFilter** junkFilter, bool plpgsql_validation);

extern DestReceiver* CreateSQLFunctionDestReceiver(void);

extern bool sql_fn_cl_rewrite_params(const Oid func_id, SQLFunctionParseInfoPtr p_info, bool is_replace);

extern void sql_create_proc_parser_setup(struct ParseState* p_state, SQLFunctionParseInfoPtr p_info);

extern Node* sql_create_proc_operator_ref(ParseState* pstate, Node* left, Node* right, Oid* ltypeid, Oid* rtypeid);

extern Node* plpgsql_create_proc_operator_ref(ParseState* pstate, Node* left, Node* right, Oid* ltypeid, Oid* rtypeid);

#endif /* FUNCTIONS_H */
