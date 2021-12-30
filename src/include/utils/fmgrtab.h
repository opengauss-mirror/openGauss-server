/* -------------------------------------------------------------------------
 *
 * fmgrtab.h
 *	  The function manager's table of internal functions.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/fmgrtab.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef FMGRTAB_H
#define FMGRTAB_H

#include "fmgr.h"
#include "catalog/genbki.h"
#include "catalog/pg_proc.h"
#include "c.h"
#include "utils/fmgroids.h"

/*
 * This table stores info about all the built-in functions (ie, functions
 * that are compiled into the openGauss executable).  The table entries are
 * required to appear in Oid order, so that binary search can be used.
 */

typedef struct {
    Oid foid;             /* OID of the function */
    const char* funcName; /* C name of the function */
    short nargs;          /* 0..FUNC_MAX_ARGS, or -1 if variable count */
    bool strict;          /* T if function is "strict" */
    bool retset;          /* T if function returns a set */
    PGFunction func;      /* pointer to compiled function */
    Oid rettype;          // OID of result type
} FmgrBuiltin;

/* This table stores all info of all builtin function. It inherits from FmgrBuiltin, but
 * unlike FmgrBuiltin, its table entries are required to be ordered by funcName, so that
 * binary search on funcName can be used when accessing the function info by function name.
 * Caution: the order of argmentes in the strunct cannot be changed!*/
typedef struct {
    Oid foid;             /* [ 0] */
    const char* funcName; /* [ 1] C name of the function or procedure name */
    int2 nargs;           /* [ 2] number of arguments, 0..FUNC_MAX_ARGS, or -1 if variable count */
    bool strict;          /* [ 3] strict with respect to NULLs? */
    bool retset;          /* [ 4] returns a set? */
    PGFunction func;      /* [ 5] pointer to compiled function */
    Oid rettype;          /* [ 6] OID of result type */
    Oid pronamespace;     /* [ 7] OID of namespace containing this proc */
    Oid proowner;         /* [ 8] procedure owner */
    Oid prolang;          /* [ 9] OID of pg_language entry, 12 for internal, 14 for SQL statement */
    float4 procost;       /* [10] estimated execution cost */
    float4 prorows;       /* [11] estimated # of rows out (if proretset) */
    Oid provariadic;      /* [12] element type of variadic array, or 0 */
    regproc protransform; /* [13] transforms calls to it during planning */
    bool proisagg;        /* [14] is it an aggregate? */
    bool proiswindow;     /* [15] is it a window function? */
    bool prosecdef;       /* [16] security definer */
    bool proleakproof;    /* [17] is it a leak-proof function? */
    char provolatile;     /* [18] see PROVOLATILE_ categories below */
    int2 pronargdefaults; /* [19] number of arguments with defaults */

    /* variable-length fields start here */
    ArrayOid proargtypes; /* [20] parameter types (excludes OUT params) */

    /* nullable fields start here, they are defined by pointer, if one of them
     * is null pointer, it means it has SQL NULL value */
    ArrayOid* proallargtypes;    /* [21] all param types (NULL if IN only) */
    ArrayChar* proargmodes;      /* [22] parameter modes (NULL if IN only) */
    ArrayCStr* proargnames;      /* [23] parameter names (NULL if no names) */
    const char* proargdefaults;  /* [24] list of expression trees for argument defaults (NULL if none) */
    const char* prosrc;          /* [25] procedure source text */
    const char* probin;          /* [26] secondary procedure info (can be NULL) */
    ArrayCStr* proconfig;        /* [27] procedure-local GUC settings */
    ArrayAcl* proacl;            /* [28] access permissions */
    ArrayInt2* prodefaultargpos; /* [29] */
    bool* fencedmode;            /* [30] */
    bool* proshippable;          /* [31] if provolatile is not 'i', proshippable will determine if the func can be shipped */
    bool* propackage;            /* [32] */
    const char* descr;           /* [33] description */
    char prokind;                /* [34] f:function, p:procedure*/
    const char* proargsrc;       /* [35] procedure/function param input string */
    Oid propackageid;            /* [36] OID of package containing this proc */
    bool proisprivate;           /* [37] is a private function */

    /*
     * The following two fields are used only if the function has more than FUNC_MAX_ARGS_INROW (666) parameters,
     * otherwise these are set to NULL.
     */
    ArrayOid* proargtypesext;       /* [38] parameter types (excludes OUT params), extended proargtypes */
    ArrayInt2* prodefaultargposext; /* [39] extended prodefaultargpos */
    ArrayOid allargtypes;         /* [40] all parameter types */
    ArrayOid* allargtypesext;
} Builtin_func;

/* The function has the same names are put in one group */
typedef struct {
    const char* funcName;
    int fnums; /* number of functions in this group */
    Builtin_func* funcs;
} FuncGroup;

static_assert(sizeof(NULL) == sizeof(void*), "NULL must be a 8 byte-length pointer");

/* is null indicator? NULL pointer has size 8 */
#define isnull_indicator(ind) (sizeof(ind) == sizeof(NULL))

/* get the number of arguments from a indicator */
#define GET_ARGNUMS(ind) DatumGetInt32((Datum)ind)

#define _SetField(field, value) .field = value
#define _SetPointerField(field, ind, value) .field = (isnull_indicator(ind) ? NULL : (value))

/* The following _i(value) Macro is used to set the i-th field of Builtin_func */
#define _0(funcid) _SetField(foid, funcid)
#define _1(fname) _SetField(funcName, fname)
#define _2(fnargs) _SetField(nargs, fnargs)
#define _3(is_strict) _SetField(strict, is_strict)
#define _4(is_retset) _SetField(retset, is_retset)
#define _5(func_ptr) _SetField(func, func_ptr)
#define _6(ret_oid_type) _SetField(rettype, ret_oid_type)
#define _7(namespace_oid) _SetField(pronamespace, namespace_oid)
#define _8(owner_oid) _SetField(proowner, owner_oid)
#define _9(lang_oid) _SetField(prolang, lang_oid)
#define _10(cost) _SetField(procost, cost)
#define _11(rows) _SetField(prorows, rows)
#define _12(vari_oid) _SetField(provariadic, vari_oid)
#define _13(tansf_oid) _SetField(protransform, tansf_oid)
#define _14(is_agg) _SetField(proisagg, is_agg)
#define _15(is_window) _SetField(proiswindow, is_window)
#define _16(is_secdef) _SetField(prosecdef, is_secdef)
#define _17(is_leakproof) _SetField(proleakproof, is_leakproof)

/* the set provolatile field, 'i', 's', 'v' */
#define _18(volt_type) _SetField(provolatile, volt_type)

#define _19(nargdefaults) _SetField(pronargdefaults, nargdefaults)

/* Set the value for proargtypes, which is an array, The argument cnt of the Macro
 * means the number of elements that are used to initialize the array,
 * e.g., _20(3, oid1, oid2, oid3), means the array field hash 3 elements oid1, oid2, and oid3 */
#define _20(cnt, ...) _SetField(proargtypes, MakeArrayOid(cnt, __VA_ARGS__))

/* The following Macros are used for initializing nullable fields.
 * For an array field, its initializer is like _xx(ind_cnt, ...), e.g., _21, _22,
 * it has two ways to initialize the fields:
 * _xx(NULL): indicates the array field is NULL
 * _xx(3, e1, e2, e3): indicates the fields is a 3-elements array.
 *
 * For a single-value field, its initializer is like _xx(ind_ff), e.g., _30, _31,
 * it has also two ways to initialize the fields:
 * _xx(NULL): indicates the field is NULL
 * _xx(fvalue): means the field has the value *fvalue*
 * */
/* Set the proallargtypes field. The input must be an array of Oids, e.g., _21(4, oid1, oid2, oid3, oid4) */
#define _21(ind_cnt, ...) _SetPointerField(proallargtypes, ind_cnt, MakeArrayOidPtr(GET_ARGNUMS(ind_cnt), __VA_ARGS__))

/* Set the proargmodes field, The input must be an array of chars: 'i', 'o', 'b', 'v', 't'
 * the usage is: _22(3, 'i', 'b', 'o') */
#define _22(ind_cnt, ...) _SetPointerField(proargmodes, ind_cnt, MakeArrayCharPtr(GET_ARGNUMS(ind_cnt), __VA_ARGS__))

/* The input must be an array of c-string, e.g., _23(3, "argname1", "argname2", "argname3") */
#define _23(ind_cnt, ...) _SetPointerField(proargnames, ind_cnt, MakeArrayCStrPtr(GET_ARGNUMS(ind_cnt), __VA_ARGS__))

#define _24(arg_default_expr_str) _SetField(proargdefaults, arg_default_expr_str)
#define _25(proc_src) _SetField(prosrc, proc_src)
#define _26(bin_info) _SetField(probin, bin_info)

#define _27(ind_cnt, ...) _SetPointerField(proconfig, ind_cnt, MakeArrayCStrPtr(GET_ARGNUMS(ind_cnt), __VA_ARGS__))
#define _28(ind_cnt, ...) _SetPointerField(proacl, ind_cnt, MakeArrayInt4Ptr(GET_ARGNUMS(ind_cnt), __VA_ARGS__))
#define _29(ind_cnt, ...) \
    _SetPointerField(prodefaultargpos, ind_cnt, MakeArrayInt2Ptr(GET_ARGNUMS(ind_cnt), __VA_ARGS__))

/* .fencedmode = ((ind_fmode) == NVL ? NULL : (bool[1]) { ind_fmode })  */
#define _30(ind_fmode) _SetPointerField(fencedmode, ind_fmode, MakeSingleValuePtr(bool, ind_fmode))
#define _31(ind_shippable) _SetPointerField(proshippable, ind_shippable, MakeSingleValuePtr(bool, ind_shippable))
#define _32(ind_is_pkg) _SetPointerField(propackage, ind_is_pkg, MakeSingleValuePtr(bool, ind_is_pkg))
#define _33(desc_str) _SetField(descr, desc_str)
#define _34(c_prokind) _SetField(prokind, c_prokind)
#define _35(argSrc) _SetField(proargsrc, argSrc)
#define _36(package_oid) _SetField(propackageid, package_oid)
#define _37(is_private) _SetField(proisprivate, is_private)

/*
 * proargtypesext (_38) and prodefaultargposext (_39) are used only if the function has more than
 * FUNC_MAX_ARGS_INROW (666) parameters. For all the builtin functions, these fields will be NULL (as none of them
 * have parameters more than 666). This is checked in the code and set to NULL accordingly while generating
 * the tuple. So it is not mandatory to set these parameters to NULL explicitly for all the existing functions in
 * builtin_funcs.ini file.
 */
#define _38(ind_cnt, ...) _SetPointerField(proargtypesext, ind_cnt, MakeArrayOidPtr(GET_ARGNUMS(ind_cnt), __VA_ARGS__))
#define _39(ind_cnt, ...) \
    _SetPointerField(prodefaultargposext, ind_cnt, MakeArrayInt2Ptr(GET_ARGNUMS(ind_cnt), __VA_ARGS__))
#define _40(cnt, ...) _SetField(allargtypes, MakeArrayOid(cnt, __VA_ARGS__))
#define _41(ind_cnt, ...) _SetPointerField(allargtypesext, ind_cnt, MakeArrayOidPtr(GET_ARGNUMS(ind_cnt), __VA_ARGS__))

/* Use Marcos _index() to initialize a built-in function, the indices between 0 ~ 20 are necessary,
 * and indices between 21 ~ 32 are optional */
#define AddBuiltinFunc(...) \
    {                       \
        __VA_ARGS__         \
    }

#define AddFuncGroup(groupName, nelem, ...)                                   \
    {                                                                         \
        .funcName = groupName, .fnums = nelem, .funcs = (Builtin_func[nelem]) \
        {                                                                     \
            __VA_ARGS__                                                       \
        }                                                                     \
    }

extern const FmgrBuiltin fmgr_builtins[];
extern const FmgrBuiltin* fmgr_isbuiltin(Oid id);

extern const int fmgr_nbuiltins; /* number of entries in table */

/* Store all built-in functions ordered by function oid. Actually, it stores the
 * function pointers to g_func_groups for saving memories. */
extern const Builtin_func* g_sorted_funcs[nBuiltinFuncs];
extern const FmgrBuiltin* g_fmgr_sorted_builtins[NFMGRFUNCS];

/* Store all built-in functions, that are grouped by function name in alphabetical
 * order with lower case comparison (see pg_strcasecmp). */
extern FuncGroup g_func_groups[];

extern const int g_nfuncgroups; /* number of function groups */

void initBuiltinFuncs();

const FuncGroup* SearchBuiltinFuncByName(const char* funcname);
const Builtin_func* SearchBuiltinFuncByOid(Oid id);
#endif /* FMGRTAB_H */
