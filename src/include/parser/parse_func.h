/* -------------------------------------------------------------------------
 *
 * parse_func.h
 *
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * src/include/parser/parse_func.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PARSER_FUNC_H
#define PARSER_FUNC_H

#include "catalog/namespace.h"
#include "parser/parse_node.h"

/*
 *	This structure is used to explore the inheritance hierarchy above
 *	nodes in the type tree in order to disambiguate among polymorphic
 *	functions.
 */
typedef struct _InhPaths {
    int nsupers;   /* number of superclasses */
    Oid self;      /* this class */
    Oid* supervec; /* vector of superclasses */
} InhPaths;

/* Result codes for func_get_detail */
typedef enum {
    FUNCDETAIL_NOTFOUND,   /* no matching function */
    FUNCDETAIL_MULTIPLE,   /* too many matching functions */
    FUNCDETAIL_NORMAL,     /* found a matching regular function */
    FUNCDETAIL_AGGREGATE,  /* found a matching aggregate function */
    FUNCDETAIL_WINDOWFUNC, /* found a matching window function */
    FUNCDETAIL_COERCION    /* it's a type coercion request */
} FuncDetailCode;

extern Node *ParseFuncOrColumn(ParseState *pstate, List *funcname, List *fargs, Node *last_srf, FuncCall *fn,
                               int location, bool call_func = false);

extern FuncDetailCode func_get_detail(List* funcname, List* fargs, List* fargnames, int nargs, Oid* argtypes,
    bool expand_variadic, bool expand_defaults, Oid* funcid, Oid* rettype, bool* retset, int* nvargs, Oid* vatype,
    Oid** true_typeids, List** argdefaults, bool call_func = false, Oid* refSynOid = NULL, int* rettype_orig = NULL);

extern int func_match_argtypes(
    int nargs, Oid* input_typeids, FuncCandidateList raw_candidates, FuncCandidateList* candidates);

extern FuncCandidateList func_select_candidate(int nargs, Oid* input_typeids, FuncCandidateList candidates);

extern void make_fn_arguments(ParseState* pstate, List* fargs, Oid* actual_arg_types, Oid* declared_arg_types);

extern const char* funcname_signature_string(const char* funcname, int nargs, List* argnames, const Oid* argtypes);
extern const char* func_signature_string(List* funcname, int nargs, List* argnames, const Oid* argtypes);

extern Oid LookupFuncName(List* funcname, int nargs, const Oid* argtypes, bool noError);
extern Oid LookupFuncNameTypeNames(List* funcname, List* argtypes, bool noError);
extern Oid LookupFuncNameOptTypeNames(List* funcname, List* argtypes, bool noError);
extern Oid LookupPackageNames(List* pkgname);
extern Oid LookupAggNameTypeNames(List* aggname, List* argtypes, bool noError);
extern Oid LookupTypeNameOid(const TypeName* typname);

extern void check_srf_call_placement(ParseState *pstate, Node *last_srf, int location);
extern void check_pg_get_expr_args(ParseState* pstate, Oid fnoid, List* args);
extern int GetPriority(Oid typeoid);
#endif /* PARSE_FUNC_H */
