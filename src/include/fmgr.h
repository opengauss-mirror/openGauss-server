/* -------------------------------------------------------------------------
 *
 * fmgr.h
 *	  Definitions for the openGauss function manager and function-call
 *	  interface.
 *
 * This file must be included by all openGauss modules that either define
 * or call fmgr-callable functions.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/fmgr.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef FMGR_H
#define FMGR_H

#if defined(FRONTEND_PARSER)
#include "postgres_fe.h"
#else
#include "postgres.h"
#endif // FRONTEND_PARSER
#include "fmgr/fmgr_comp.h"
#include "fmgr/fmgr_core.h"
#include "lib/stringinfo.h"
#include "access/tupdesc.h"
#include "nodes/primnodes.h"

#ifndef FRONTEND_PARSER

typedef ScalarVector* (*VectorFunction)(FunctionCallInfo fcinfo);
typedef Datum (*GenericArgExtract)(Datum* data);

extern void InitFuncCallUDFInfo(FunctionCallInfoData* fcinfo, int argN, bool setFuncPtr);
extern void InitFunctionCallUDFArgs(FunctionCallInfoData* fcinfo, int argN, int batchRows);
extern void FreeFuncCallUDFInfo(FunctionCallInfoData* Fcinfo);

#define VECTOR_GENERIC_FUNCTION_PREALLOCED_ARGS 32
#define VECTOR_GENERIC_FUNCTION_INCREMENTAL_ARGS 10

struct GenericFunRuntimeArg {
    ScalarVector** arg;
    Oid argType;
    GenericArgExtract getArgFun;
};

struct GenericFunRuntime {
    /* maxium number of the arguements can be stored */
    uint16 compacity;

    /* pointer to the address of the real arg */
    GenericFunRuntimeArg* args;
    Datum* inputargs;
    bool* nulls;

    bool restrictFlag[BatchMaxSize];
    FunctionCallInfoData* internalFinfo;
};
/*
 * This struct holds the system-catalog information that must be looked up
 * before a function can be called through fmgr.  If the same function is
 * to be called multiple times, the lookup need be done only once and the
 * info struct saved for re-use.
 *
 * Note that fn_expr really is parse-time-determined information about the
 * arguments, rather than about the function itself.  But it's convenient
 * to store it here rather than in FunctionCallInfoData, where it might more
 * logically belong.
 */
typedef struct FmgrInfo {
    PGFunction fn_addr;       /* pointer to function or handler to be called */
    Oid fn_oid;               /* OID of function (NOT of handler, if any) */
    short fn_nargs;           /* 0..FUNC_MAX_ARGS, or -1 if variable arg
                               * count */
    bool fn_strict;           /* function is "strict" (NULL in => NULL out) */
    bool fn_retset;           /* function returns a set */
    unsigned char fn_stats;   /* collect stats if track_functions > this */
    void* fn_extra;           /* extra space for use by handler */
    MemoryContext fn_mcxt;    /* memory context to store fn_extra in */
    fmNodePtr fn_expr;        /* expression parse tree for call, or NULL */
    Oid fn_rettype;           // Oid of function return type
    Oid fn_rettypemod;        /* Oid of the function returnt typmod */
    char fnName[NAMEDATALEN]; /* function name */
    char* fnLibPath;          /* library path for c-udf
                               * package.class.method(args) for java-udf */
    bool fn_fenced;
    Oid fn_languageId; /* function language id*/
    char fn_volatile;  /* procvolatile */
    // Vector Function
    VectorFunction vec_fn_addr;
    VectorFunction* vec_fn_cache;
    GenericFunRuntime* genericRuntime;
} FmgrInfo;

/*
 * number of prealloced arguments to a function.
 * In deepsql (SVM and elastic_net), we cannot explicitly set all elements to false.
 */
#define FUNC_PREALLOCED_ARGS 10

typedef struct UDFInfoType {
    UDFArgsFuncType* UDFArgsHandlerPtr; /* UDF send/recv argument function */
    UDFArgsFuncType UDFResultHandlerPtr;
    StringInfo udfMsgBuf;
    char* msgReadPtr;
    int argBatchRows;
    int allocRows;
    Datum** arg;
    bool** null;
    Datum* result;
    bool* resultIsNull;
    bool valid_UDFArgsHandlerPtr; /* True if funcUDFArgs are filled ok */

    UDFInfoType()
    {
        udfMsgBuf = NULL;
        msgReadPtr = NULL;
        arg = NULL;
        null = NULL;
        result = NULL;
        resultIsNull = NULL;
        UDFArgsHandlerPtr = NULL;
        UDFResultHandlerPtr = NULL;
        valid_UDFArgsHandlerPtr = false;
        allocRows = 0;
        argBatchRows = 0;
    }
} UDFInfoType;

typedef struct RefcusorInfoData {
    Cursor_Data* argCursor;
    Cursor_Data* returnCursor;
    int return_number;
} RefcusorInfoData;

/*
 * start-with support
 *
 * Note, need revisit. Basically, we don't want feature oriented stuffs put here
 */
typedef struct StartWithFuncEvalInfo
{
    Node   *sw_econtext;
    Node   *sw_exprstate;
    bool   sw_is_flt_frame;
} StartWithFuncEvalInfo;

/*
 * This struct is the data actually passed to an fmgr-called function.
 */
typedef struct FunctionCallInfoData {
    FmgrInfo* flinfo;                            /* ptr to lookup info used for this call */
    fmNodePtr context;                           /* pass info about context of call */
    fmNodePtr resultinfo;                        /* pass or return extra info about result */
    Oid fncollation;                             /* collation for function to use */
    bool isnull;                                 /* function must set true if result is NULL */
    bool can_ignore;                             /* function can ignore overflow or underflow conditions for type transform function */
    short nargs;                                 /* # arguments actually passed */
    Datum* arg;                                  /* Arguments passed to function */
    bool* argnull;                               /* T if arg[i] is actually NULL */
    Oid* argTypes;                               /* Argument type */
    Datum prealloc_arg[FUNC_PREALLOCED_ARGS];    /* prealloced arguments.*/
    bool prealloc_argnull[FUNC_PREALLOCED_ARGS]; /* prealloced argument null flags.*/
    Oid prealloc_argTypes[FUNC_PREALLOCED_ARGS] = {InvalidOid}; /* prealloced argument type */
    ScalarVector* argVector;                     /* Scalar Vector */
    RefcusorInfoData refcursor_data;
    UDFInfoType udfInfo;
    StartWithFuncEvalInfo  swinfo;
    CoercionContext ccontext;

    FunctionCallInfoData()
    {
        flinfo = NULL;
        arg = NULL;
        argnull = NULL;
        argTypes = NULL;
        argVector = NULL;
        fncollation = 0;
        context = NULL;
        resultinfo = NULL;
        nargs = 0;
        isnull = false;
        can_ignore = false;
        ccontext = COERCION_UNKNOWN;
    }
} FunctionCallInfoData;

/*
 * List of dynamically loaded files (kept in malloc'd memory).
 */

typedef struct df_files {
    struct df_files* next; /* List link */
    dev_t device;          /* Device file is on */
#ifndef WIN32              /* ensures we never again depend on this under \
                            * win32 */
    ino_t inode;           /* Inode number of file */
#endif
    void* handle;                         /* a handle for pg_dl* functions */
    char filename[FLEXIBLE_ARRAY_MEMBER]; /* Full pathname of file */

    /*
     * we allocate the block big enough for actual length of pathname.
     * filename[] must be last item in struct!
     */
} DynamicFileList;

/*Introduced to judge whether PG_init has done or not*/
typedef struct df_files_init {
    struct df_files_init* next; /*List Link*/
    DynamicFileList* file_list; /*restore file_list in the linked list*/
} FileListInit;

extern DynamicFileList* file_list;
extern DynamicFileList* file_tail;

#define EXTRA_NARGS 3

extern bool directory_exists(const char* direct);
extern bool file_exists(const char* name);
/*
 * This routine fills a FmgrInfo struct, given the OID
 * of the function to be called.
 */
extern void fmgr_info(Oid functionId, FmgrInfo* finfo);

/*
 * Same, when the FmgrInfo struct is in a memory context longer-lived than
 * CurrentMemoryContext.  The specified context will be set as fn_mcxt
 * and used to hold all subsidiary data of finfo.
 */
extern void fmgr_info_cxt(Oid functionId, FmgrInfo* finfo, MemoryContext mcxt);

extern void InitVecFunctionCallInfoData(
    FunctionCallInfoData* Fcinfo, FmgrInfo* Flinfo, int Nargs, Oid Collation, fmNodePtr Context, fmNodePtr Resultinfo);

/* Convenience macro for setting the fn_expr field */
#define fmgr_info_set_expr(expr, finfo) ((finfo)->fn_expr = (expr))

/*
 * Copy an FmgrInfo struct
 */
extern void fmgr_info_copy(FmgrInfo* dstinfo, FmgrInfo* srcinfo, MemoryContext destcxt);

/*
 * This macro initializes all the fields of a GenericFunRuntime except
 * for the all the prealloc arrays.	Performance testing has shown that
 * the fastest way to set up static arrays for small numbers of arguments is to
 * explicitly set each required element to false, so we don't try to zero
 * out the static prealloc array in the macro.
 */

#define InitGenericFunRuntimeInfo(GenericRuntime, Nargs)                                                            \
    do {                                                                                                            \
        (GenericRuntime).compacity = VECTOR_GENERIC_FUNCTION_PREALLOCED_ARGS;                                       \
        if (unlikely((Nargs) > VECTOR_GENERIC_FUNCTION_PREALLOCED_ARGS)) {                                          \
            (GenericRuntime).compacity +=                                                                           \
                (((Nargs)-VECTOR_GENERIC_FUNCTION_PREALLOCED_ARGS - 1) / VECTOR_GENERIC_FUNCTION_INCREMENTAL_ARGS + \
                    1) *                                                                                            \
                VECTOR_GENERIC_FUNCTION_INCREMENTAL_ARGS;                                                           \
        }                                                                                                           \
        (GenericRuntime).args =                                                                                     \
            (GenericFunRuntimeArg*)palloc0(sizeof(GenericFunRuntimeArg) * (GenericRuntime).compacity);              \
        (GenericRuntime).inputargs = (Datum*)palloc0(sizeof(Datum) * (GenericRuntime).compacity);                   \
        (GenericRuntime).nulls = (bool*)palloc0(sizeof(bool) * (GenericRuntime).compacity);                         \
    } while (0)

#define FreeGenericFunRuntimeInfo(GenericRuntime)                        \
    do {                                                                 \
        pfree_ext((GenericRuntime).args);                                \
        pfree_ext((GenericRuntime).inputargs);                           \
        pfree_ext((GenericRuntime).nulls);                               \
        if (unlikely((GenericRuntime).internalFinfo != NULL))            \
            FreeFunctionCallInfoData(*((GenericRuntime).internalFinfo)); \
        (GenericRuntime).compacity = 0;                                  \
    } while (0)

/*
 * This macro initializes all the fields of a FunctionCallInfoData except
 * for the arg[] and argnull[] arrays.	Performance testing has shown that
 * the fastest way to set up argnull[] for small numbers of arguments is to
 * explicitly set each required element to false, so we don't try to zero
 * out the argnull[] array in the macro.
 */
#define InitFunctionCallInfoArgs(Fcinfo, Nargs, batchRow)             \
    do {                                                              \
        (Fcinfo).nargs = (Nargs);                                     \
        if ((Nargs) > FUNC_PREALLOCED_ARGS) {                         \
            (Fcinfo).arg = (Datum*)palloc0((Nargs) * sizeof(Datum));  \
            (Fcinfo).argnull = (bool*)palloc0(Nargs * sizeof(bool));  \
            (Fcinfo).argTypes = (Oid*)palloc0((Nargs) * sizeof(Oid)); \
        } else {                                                      \
            (Fcinfo).arg = (Fcinfo).prealloc_arg;                     \
            (Fcinfo).argnull = (Fcinfo).prealloc_argnull;             \
            (Fcinfo).argTypes = (Fcinfo).prealloc_argTypes;           \
        }                                                             \
        if (unlikely((Fcinfo).flinfo && (Fcinfo).flinfo->fn_fenced))  \
            InitFunctionCallUDFArgs(&(Fcinfo), (Nargs), (batchRow));  \
    } while (0)

#define InitFunctionCallInfoData(Fcinfo, Flinfo, Nargs, Collation, Context, Resultinfo) \
    do {                                                                                \
        (Fcinfo).flinfo = (Flinfo);                                                     \
        (Fcinfo).context = (Context);                                                   \
        (Fcinfo).resultinfo = (Resultinfo);                                             \
        (Fcinfo).fncollation = (Collation);                                             \
        (Fcinfo).isnull = false;                                                        \
        (Fcinfo).nargs = (Nargs);                                                       \
        if (unlikely((Nargs) > FUNC_PREALLOCED_ARGS)) {                                 \
            (Fcinfo).arg = (Datum*)palloc0((Nargs) * sizeof(Datum));                    \
            (Fcinfo).argnull = (bool*)palloc0((Nargs) * sizeof(bool));                  \
            (Fcinfo).argTypes = (Oid*)palloc0((Nargs) * sizeof(Oid));                   \
        } else {                                                                        \
            (Fcinfo).arg = (Fcinfo).prealloc_arg;                                       \
            (Fcinfo).argnull = (Fcinfo).prealloc_argnull;                               \
            (Fcinfo).argTypes = (Fcinfo).prealloc_argTypes;                             \
        }                                                                               \
        if (unlikely((Flinfo) != NULL && (Fcinfo).flinfo->fn_fenced))                   \
            InitFuncCallUDFInfo(&(Fcinfo), (Nargs), false);                             \
        (Fcinfo).refcursor_data.argCursor = NULL;                                       \
        (Fcinfo).refcursor_data.returnCursor = NULL;                                    \
        (Fcinfo).refcursor_data.return_number = 0;                                      \
    } while (0)

#define FreeFunctionCallInfoData(Fcinfo)             \
    do {                                             \
        if ((Fcinfo).nargs > FUNC_PREALLOCED_ARGS) { \
            pfree((Fcinfo).arg);                     \
            pfree((Fcinfo).argnull);                 \
            pfree((Fcinfo).argTypes);                \
            (Fcinfo).argTypes = NULL;                \
            (Fcinfo).arg = NULL;                     \
            (Fcinfo).argnull = NULL;                 \
        }                                            \
        FreeFuncCallUDFInfo(&(Fcinfo));              \
    } while (0)
/*
 * This macro invokes a function given a filled-in FunctionCallInfoData
 * struct.	The macro result is the returned Datum --- but note that
 * caller must still check fcinfo->isnull!	Also, if function is strict,
 * it is caller's responsibility to verify that no null arguments are present
 * before calling.
 */
#define FunctionCallInvoke(fcinfo) ((*(fcinfo)->flinfo->fn_addr)(fcinfo))

#define VecFunctionCallInvoke(fcinfo) ((*(fcinfo)->flinfo->vec_fn_addr)(fcinfo))

/* -------------------------------------------------------------------------
 *		Support for detecting call convention of dynamically-loaded functions
 *
 * Dynamically loaded functions may use either the version-1 ("new style")
 * or version-0 ("old style") calling convention.  Version 1 is the call
 * convention defined in this header file; version 0 is the old "plain C"
 * convention.	A version-1 function must be accompanied by the macro call
 *
 *		PG_FUNCTION_INFO_V1(function_name);
 *
 * Note that internal functions do not need this decoration since they are
 * assumed to be version-1.
 *
 * -------------------------------------------------------------------------
 */

typedef struct {
    int api_version; /* specifies call convention version number */
                     /* More fields may be added later, for version numbers > 1. */
} Pg_finfo_record;

/* Expected signature of an info function */
typedef const Pg_finfo_record* (*PGFInfoFunction)(void);

/*
 *	Macro to build an info function associated with the given function name.
 *	Win32 loadable functions usually link with 'dlltool --export-all', but it
 *	doesn't hurt to add PGDLLIMPORT in case they don't.
 */
#define PG_FUNCTION_INFO_V1(funcname)                                                   \
    extern "C" PGDLLEXPORT const Pg_finfo_record* CppConcat(pg_finfo_, funcname)(void); \
    const Pg_finfo_record* CppConcat(pg_finfo_, funcname)(void)                         \
    {                                                                                   \
        static const Pg_finfo_record my_finfo = {1};                                    \
        return &my_finfo;                                                               \
    }                                                                                   \
    extern int no_such_variable

/* -------------------------------------------------------------------------
 *		Support for verifying backend compatibility of loaded modules
 *
 * We require dynamically-loaded modules to include the macro call
 *		PG_MODULE_MAGIC;
 * so that we can check for obvious incompatibility, such as being compiled
 * for a different major openGauss version.
 *
 * To compile with versions of openGauss that do not support this,
 * you may put an #ifdef/#endif test around it.  Note that in a multiple-
 * source-file module, the macro call should only appear once.
 *
 * The specific items included in the magic block are intended to be ones that
 * are custom-configurable and especially likely to break dynamically loaded
 * modules if they were compiled with other values.  Also, the length field
 * can be used to detect definition changes.
 *
 * Note: we compare magic blocks with memcmp(), so there had better not be
 * any alignment pad bytes in them.
 *
 * Note: when changing the contents of magic blocks, be sure to adjust the
 * incompatible_module_error() function in dfmgr.c.
 * -------------------------------------------------------------------------
 */

/* Definition of the magic block structure */
typedef struct {
    int len;          /* sizeof(this struct) */
    int version;      /* PostgreSQL major version */
    int funcmaxargs;  /* FUNC_MAX_ARGS */
    int indexmaxkeys; /* INDEX_MAX_KEYS */
    int namedatalen;  /* NAMEDATALEN */
    int float4byval;  /* FLOAT4PASSBYVAL */
    int float8byval;  /* FLOAT8PASSBYVAL */
} Pg_magic_struct;

/* The actual data block contents */
#define PG_MODULE_MAGIC_DATA                                                                                        \
    {                                                                                                               \
        sizeof(Pg_magic_struct), PG_VERSION_NUM / 100, FUNC_MAX_ARGS, INDEX_MAX_KEYS, NAMEDATALEN, FLOAT4PASSBYVAL, \
            FLOAT8PASSBYVAL                                                                                         \
    }

/*
 * Declare the module magic function.  It needs to be a function as the dlsym
 * in the backend is only guaranteed to work on functions, not data
 */
typedef const Pg_magic_struct* (*PGModuleMagicFunction)(void);

#define PG_MAGIC_FUNCTION_NAME Pg_magic_func
#define PG_MAGIC_FUNCTION_NAME_STRING "Pg_magic_func"

#define PG_MODULE_MAGIC                                                         \
    extern "C" PGDLLEXPORT const Pg_magic_struct* PG_MAGIC_FUNCTION_NAME(void); \
    const Pg_magic_struct* PG_MAGIC_FUNCTION_NAME(void)                         \
    {                                                                           \
        static const Pg_magic_struct Pg_magic_data = PG_MODULE_MAGIC_DATA;      \
        return &Pg_magic_data;                                                  \
    }                                                                           \
    extern int no_such_variable

/* -------------------------------------------------------------------------
 *		Support routines and macros for callers of fmgr-compatible functions
 * -------------------------------------------------------------------------
 */

/* These are for invocation of a specifically named function with a
 * directly-computed parameter list.  Note that neither arguments nor result
 * are allowed to be NULL.
 */
extern Datum DirectFunctionCall1Coll(PGFunction func, Oid collation, Datum arg1, bool can_ignore = false);
extern Datum DirectFunctionCall2Coll(PGFunction func, Oid collation, Datum arg1, Datum arg2, bool can_ignore = false);
extern Datum DirectFunctionCall3Coll(PGFunction func, Oid collation, Datum arg1, Datum arg2, Datum arg3,
    bool can_ignore = false);
extern Datum DirectFunctionCall4Coll(PGFunction func, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4,
    bool can_ignore = false);
extern Datum DirectFunctionCall5Coll(
    PGFunction func, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4, Datum arg5,
    bool can_ignore = false);
extern Datum DirectFunctionCall6Coll(
    PGFunction func, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4, Datum arg5, Datum arg6,
    bool can_ignore = false);
extern Datum DirectFunctionCall7Coll(
    PGFunction func, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4, Datum arg5, Datum arg6, Datum arg7,
    bool can_ignore = false);
extern Datum DirectFunctionCall8Coll(PGFunction func, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4,
    Datum arg5, Datum arg6, Datum arg7, Datum arg8, bool can_ignore = false);
extern Datum DirectFunctionCall9Coll(PGFunction func, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4,
    Datum arg5, Datum arg6, Datum arg7, Datum arg8, Datum arg9, bool can_ignore = false);

/* These are for invocation of a previously-looked-up function with a
 * directly-computed parameter list.  Note that neither arguments nor result
 * are allowed to be NULL.
 */
extern Datum FunctionCall1Coll(FmgrInfo* flinfo, Oid collation, Datum arg1);
extern Datum FunctionCall2Coll(FmgrInfo* flinfo, Oid collation, Datum arg1, Datum arg2);
extern Datum FunctionCall3Coll(FmgrInfo* flinfo, Oid collation, Datum arg1, Datum arg2, Datum arg3);
extern Datum FunctionCall4Coll(FmgrInfo* flinfo, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4);
extern Datum FunctionCall5Coll(
    FmgrInfo* flinfo, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4, Datum arg5);
extern Datum FunctionCall6Coll(
    FmgrInfo* flinfo, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4, Datum arg5, Datum arg6);
extern Datum FunctionCall7Coll(FmgrInfo* flinfo, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4,
    Datum arg5, Datum arg6, Datum arg7);
extern Datum FunctionCall8Coll(FmgrInfo* flinfo, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4,
    Datum arg5, Datum arg6, Datum arg7, Datum arg8);
extern Datum FunctionCall9Coll(FmgrInfo* flinfo, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4,
    Datum arg5, Datum arg6, Datum arg7, Datum arg8, Datum arg9);

/* These are for invocation of a function identified by OID with a
 * directly-computed parameter list.  Note that neither arguments nor result
 * are allowed to be NULL.	These are essentially FunctionLookup() followed
 * by FunctionCallN().	If the same function is to be invoked repeatedly,
 * do the FunctionLookup() once and then use FunctionCallN().
 */
extern Datum OidFunctionCall0Coll(Oid functionId, Oid collation, bool can_ignore = false);
extern Datum OidFunctionCall1Coll(Oid functionId, Oid collation, Datum arg1, bool can_ignore = false);
extern Datum OidFunctionCall2Coll(Oid functionId, Oid collation, Datum arg1, Datum arg2, bool can_ignore = false);
extern Datum OidFunctionCall3Coll(Oid functionId, Oid collation, Datum arg1, Datum arg2, Datum arg3, 
    bool can_ignore = false);
extern Datum OidFunctionCall4Coll(Oid functionId, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4,
    bool can_ignore = false);
extern Datum OidFunctionCall5Coll(Oid functionId, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4,
    Datum arg5, bool can_ignore = false);
extern Datum OidFunctionCall6Coll(Oid functionId, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4,
    Datum arg5, Datum arg6, bool can_ignore = false);
extern Datum OidFunctionCall7Coll(Oid functionId, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4,
    Datum arg5, Datum arg6, Datum arg7, bool can_ignore = false);
extern Datum OidFunctionCall8Coll(Oid functionId, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4,
    Datum arg5, Datum arg6, Datum arg7, Datum arg8, bool can_ignore = false);
extern Datum OidFunctionCall9Coll(Oid functionId, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4,
    Datum arg5, Datum arg6, Datum arg7, Datum arg8, Datum arg9, bool can_ignore = false);

/* These macros allow the collation argument to be omitted (with a default of
 * InvalidOid, ie, no collation).  They exist mostly for backwards
 * compatibility of source code.
 */
#define DirectFunctionCall1(func, arg1) DirectFunctionCall1Coll(func, InvalidOid, arg1)
#define DirectFunctionCall2(func, arg1, arg2) DirectFunctionCall2Coll(func, InvalidOid, arg1, arg2)
#define DirectFunctionCall3(func, arg1, arg2, arg3) DirectFunctionCall3Coll(func, InvalidOid, arg1, arg2, arg3)
#define DirectFunctionCall4(func, arg1, arg2, arg3, arg4) \
    DirectFunctionCall4Coll(func, InvalidOid, arg1, arg2, arg3, arg4)
#define DirectFunctionCall5(func, arg1, arg2, arg3, arg4, arg5) \
    DirectFunctionCall5Coll(func, InvalidOid, arg1, arg2, arg3, arg4, arg5)
#define DirectFunctionCall6(func, arg1, arg2, arg3, arg4, arg5, arg6) \
    DirectFunctionCall6Coll(func, InvalidOid, arg1, arg2, arg3, arg4, arg5, arg6)
#define DirectFunctionCall7(func, arg1, arg2, arg3, arg4, arg5, arg6, arg7) \
    DirectFunctionCall7Coll(func, InvalidOid, arg1, arg2, arg3, arg4, arg5, arg6, arg7)
#define DirectFunctionCall8(func, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8) \
    DirectFunctionCall8Coll(func, InvalidOid, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8)
#define DirectFunctionCall9(func, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9) \
    DirectFunctionCall9Coll(func, InvalidOid, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9)
#define FunctionCall1(flinfo, arg1) FunctionCall1Coll(flinfo, InvalidOid, arg1)
#define FunctionCall2(flinfo, arg1, arg2) FunctionCall2Coll(flinfo, InvalidOid, arg1, arg2)
#define FunctionCall3(flinfo, arg1, arg2, arg3) FunctionCall3Coll(flinfo, InvalidOid, arg1, arg2, arg3)
#define FunctionCall4(flinfo, arg1, arg2, arg3, arg4) FunctionCall4Coll(flinfo, InvalidOid, arg1, arg2, arg3, arg4)
#define FunctionCall5(flinfo, arg1, arg2, arg3, arg4, arg5) \
    FunctionCall5Coll(flinfo, InvalidOid, arg1, arg2, arg3, arg4, arg5)
#define FunctionCall6(flinfo, arg1, arg2, arg3, arg4, arg5, arg6) \
    FunctionCall6Coll(flinfo, InvalidOid, arg1, arg2, arg3, arg4, arg5, arg6)
#define FunctionCall7(flinfo, arg1, arg2, arg3, arg4, arg5, arg6, arg7) \
    FunctionCall7Coll(flinfo, InvalidOid, arg1, arg2, arg3, arg4, arg5, arg6, arg7)
#define FunctionCall8(flinfo, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8) \
    FunctionCall8Coll(flinfo, InvalidOid, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8)
#define FunctionCall9(flinfo, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9) \
    FunctionCall9Coll(flinfo, InvalidOid, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9)
#define OidFunctionCall0(functionId) OidFunctionCall0Coll(functionId, InvalidOid)
#define OidFunctionCall1(functionId, arg1) OidFunctionCall1Coll(functionId, InvalidOid, arg1)
#define OidFunctionCall2(functionId, arg1, arg2) OidFunctionCall2Coll(functionId, InvalidOid, arg1, arg2)
#define OidFunctionCall3(functionId, arg1, arg2, arg3) OidFunctionCall3Coll(functionId, InvalidOid, arg1, arg2, arg3)
#define OidFunctionCall4(functionId, arg1, arg2, arg3, arg4) \
    OidFunctionCall4Coll(functionId, InvalidOid, arg1, arg2, arg3, arg4)
#define OidFunctionCall5(functionId, arg1, arg2, arg3, arg4, arg5) \
    OidFunctionCall5Coll(functionId, InvalidOid, arg1, arg2, arg3, arg4, arg5)
#define OidFunctionCall6(functionId, arg1, arg2, arg3, arg4, arg5, arg6) \
    OidFunctionCall6Coll(functionId, InvalidOid, arg1, arg2, arg3, arg4, arg5, arg6)
#define OidFunctionCall7(functionId, arg1, arg2, arg3, arg4, arg5, arg6, arg7) \
    OidFunctionCall7Coll(functionId, InvalidOid, arg1, arg2, arg3, arg4, arg5, arg6, arg7)
#define OidFunctionCall8(functionId, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8) \
    OidFunctionCall8Coll(functionId, InvalidOid, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8)
#define OidFunctionCall9(functionId, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9) \
    OidFunctionCall9Coll(functionId, InvalidOid, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9)

typedef struct {
    PGFunction user_fn;             /* the function's address */
    const Pg_finfo_record* inforec; /* address of its info record */
} CFunInfo;

/* Special cases for convenient invocation of datatype I/O functions. */
extern Datum InputFunctionCall(FmgrInfo* flinfo, char* str, Oid typioparam, int32 typmod, bool can_ignore = false, Oid collation = InvalidOid);
extern Datum InputFunctionCallForDateType(
    FmgrInfo* flinfo, char* str, Oid typioparam, int32 typmod, char* date_time_fmt);
extern Datum OidInputFunctionCall(Oid functionId, char* str, Oid typioparam, int32 typmod, bool can_ignore = false);
extern char* OutputFunctionCall(FmgrInfo* flinfo, Datum val);
extern char* OidOutputFunctionCall(Oid functionId, Datum val);
extern Datum ReceiveFunctionCall(FmgrInfo* flinfo, fmStringInfo buf, Oid typioparam, int32 typmod);
extern Datum OidReceiveFunctionCall(Oid functionId, fmStringInfo buf, Oid typioparam, int32 typmod);
extern bytea* SendFunctionCall(FmgrInfo* flinfo, Datum val);
extern bytea* OidSendFunctionCall(Oid functionId, Datum val);
extern Datum OidInputFunctionCallColl(Oid functionId, char* str, Oid typioparam, int32 typmod, Oid collation);

/*
 * Routines in fmgr.c
 */
extern const Pg_finfo_record* fetch_finfo_record(void* filehandle, char* funcname, bool isValidate);
extern void clear_external_function_hash(void* filehandle);
extern Oid fmgr_internal_function(const char* proname);
extern Oid get_fn_expr_rettype(FmgrInfo* flinfo);
extern Oid get_fn_expr_argtype(FmgrInfo* flinfo, int argnum);
extern Oid get_call_expr_argtype(fmNodePtr expr, int argnum);
extern bool get_fn_expr_arg_stable(FmgrInfo* flinfo, int argnum);
extern bool get_call_expr_arg_stable(fmNodePtr expr, int argnum);
extern bool get_fn_expr_variadic(FmgrInfo* flinfo);
extern bool CheckFunctionValidatorAccess(Oid validatorOid, Oid functionOid);

extern void check_external_function(const char* filepath, const char* filename, const char* funcname);
extern CFunInfo load_external_function(const char* filename, char* funcname, bool signalNotFound, bool isValidate);
extern char* expand_dynamic_library_name(const char* name);
extern PGFunction lookup_external_function(void* filehandle, const char* funcname);
extern void load_file(const char* filename, bool restricted);
extern void** find_rendezvous_variable(const char* varName);

/*
 * Support for aggregate functions
 *
 * This is actually in executor/nodeAgg.c, but we declare it here since the
 * whole point is for callers of it to not be overly friendly with nodeAgg.
 */

/* AggCheckCallContext can return one of the following codes, or 0: */
#define AGG_CONTEXT_AGGREGATE 1 /* regular aggregate */
#define AGG_CONTEXT_WINDOW 2    /* window function */

extern int AggCheckCallContext(FunctionCallInfo fcinfo, MemoryContext* aggcontext);

/*
 * We allow plugin modules to hook function entry/exit.  This is intended
 * as support for loadable security policy modules, which may want to
 * perform additional privilege checks on function entry or exit, or to do
 * other internal bookkeeping.	To make this possible, such modules must be
 * able not only to support normal function entry and exit, but also to trap
 * the case where we bail out due to an error; and they must also be able to
 * prevent inlining.
 */
typedef enum FmgrHookEventType { FHET_START, FHET_END, FHET_ABORT } FmgrHookEventType;

typedef bool (*needs_fmgr_hook_type)(Oid fn_oid);

typedef void (*fmgr_hook_type)(FmgrHookEventType event, FmgrInfo* flinfo, Datum* arg);

extern THR_LOCAL PGDLLIMPORT needs_fmgr_hook_type needs_fmgr_hook;
extern THR_LOCAL PGDLLIMPORT fmgr_hook_type fmgr_hook;

#define FmgrHookIsNeeded(fn_oid) (!needs_fmgr_hook ? false : (*needs_fmgr_hook)(fn_oid))

// Define common stuff for vectorized expression //
typedef enum SimpleOp {
    SOP_EQ,   // =
    SOP_NEQ,  // !=
    SOP_LE,   // <=
    SOP_LT,   // <
    SOP_GE,   // >=
    SOP_GT,   // >
} SimpleOp;

template <SimpleOp sop, typename Datatype>
inline bool eval_simple_op(Datatype dataVal1, Datatype dataVal2)
{
    if (sop == SOP_EQ)
        return ((dataVal1) == (dataVal2));
    if (sop == SOP_NEQ)
        return ((dataVal1) != (dataVal2));
    if (sop == SOP_LE)
        return ((dataVal1) <= (dataVal2));
    if (sop == SOP_LT)
        return ((dataVal1) < (dataVal2));
    if (sop == SOP_GE)
        return ((dataVal1) >= (dataVal2));
    if (sop == SOP_GT)
        return ((dataVal1) > (dataVal2));
    return false;
}

#endif /* !FRONTEND_PARSER */

extern void CopyCursorInfoData(Cursor_Data* target_data, Cursor_Data* source_data);

#endif /* FMGR_H */
