/* -------------------------------------------------------------------------
 *
 * parse_coerce.h
 *	Routines for type coercion.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * src/include/parser/parse_coerce.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PARSE_COERCE_H
#define PARSE_COERCE_H

#include "parser/parse_node.h"

/* Setup error traceback support for ereport() */
#define ELOG_FIELD_NAME_START(fieldname)                      \
    ErrorContextCallback errcontext;                          \
    errcontext.callback = expression_error_callback;          \
    errcontext.arg = (void*)(fieldname);                      \
    errcontext.previous = t_thrd.log_cxt.error_context_stack; \
    t_thrd.log_cxt.error_context_stack = &errcontext;

#define ELOG_FIELD_NAME_UPDATE(fieldname) t_thrd.log_cxt.error_context_stack->arg = (void*)(fieldname);

#define ELOG_FIELD_NAME_END t_thrd.log_cxt.error_context_stack = errcontext.previous;

/* Type categories (see TYPCATEGORY_xxx symbols in catalog/pg_type.h) */
typedef char TYPCATEGORY;

/* Result codes for find_coercion_pathway */
typedef enum CoercionPathType {
    COERCION_PATH_NONE,        /* failed to find any coercion pathway */
    COERCION_PATH_FUNC,        /* apply the specified coercion function */
    COERCION_PATH_RELABELTYPE, /* binary-compatible cast, no function */
    COERCION_PATH_ARRAYCOERCE, /* need an ArrayCoerceExpr node */
    COERCION_PATH_COERCEVIAIO  /* need a CoerceViaIO node */
} CoercionPathType;

extern bool IsBinaryCoercible(Oid srctype, Oid targettype);
extern bool IsPreferredType(TYPCATEGORY category, Oid type);
extern TYPCATEGORY TypeCategory(Oid type);

extern Node* coerce_to_target_type(ParseState* pstate, Node* expr, Oid exprtype, Oid targettype, int32 targettypmod,
    CoercionContext ccontext, CoercionForm cformat, int location);
extern bool can_coerce_type(int nargs, Oid* input_typeids, Oid* target_typeids, CoercionContext ccontext);
extern Node *type_transfer(Node *node, Oid atttypid, bool isSelect);
typedef Node* (*typeTransfer)(Node *node, Oid atttypid, bool isSelect);
extern Node *const_expression_to_const(Node *node);
extern Node* coerce_type(ParseState* pstate, Node* node, Oid inputTypeId, Oid targetTypeId, int32 targetTypeMod,
    CoercionContext ccontext, CoercionForm cformat, int location);
extern Node* coerce_to_domain(Node* arg, Oid baseTypeId, int32 baseTypeMod, Oid typeId, CoercionForm cformat,
    int location, bool hideInputCoercion, bool lengthCoercionDone);

extern Node* coerce_to_boolean(ParseState* pstate, Node* node, const char* constructName);
extern Node* coerce_to_specific_type(ParseState* pstate, Node* node, Oid targetTypeId, const char* constructName);

extern int parser_coercion_errposition(ParseState* pstate, int coerce_location, Node* input_expr);

extern Oid select_common_type(ParseState* pstate, List* exprs, const char* context, Node** which_expr);
extern bool check_all_in_whitelist(List* resultexprs);
extern Node* coerce_to_common_type(ParseState* pstate, Node* node, Oid targetTypeId, const char* context);

extern Node* coerce_to_settype(ParseState* pstate, Node* expr, Oid exprtype, Oid targettype, int32 targettypmod,
    CoercionContext ccontext, CoercionForm cformat, int location, Oid collation);

extern bool check_generic_type_consistency(Oid* actual_arg_types, Oid* declared_arg_types, int nargs);
extern Oid enforce_generic_type_consistency(
    Oid* actual_arg_types, Oid* declared_arg_types, int nargs, Oid rettype, bool allow_poly);
extern Oid resolve_generic_type(Oid declared_type, Oid context_actual_type, Oid context_declared_type);

extern CoercionPathType find_coercion_pathway(
    Oid targetTypeId, Oid sourceTypeId, CoercionContext ccontext, Oid* funcid);
extern CoercionPathType find_typmod_coercion_function(Oid typeId, Oid* funcid);

extern void expression_error_callback(void* arg);
extern Node* coerce_to_target_charset(Node* expr, int target_charset, Oid target_type, int32 target_typmod, Oid target_collation,
    bool eval_const = true);

extern Node *transferConstToAconst(Node *node);

extern Const* setValueToConstExpr(SetVariableExpr* set);
#ifdef USE_SPQ
extern bool get_cast_func(Oid oidSrc, Oid oidDest, bool *is_binary_coercible, Oid *oidCastFunc, CoercionPathType *pathtype);
#endif
#endif /* PARSE_COERCE_H */
