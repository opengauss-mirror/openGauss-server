/* -------------------------------------------------------------------------
 *
 * windowapi.h
 *	  API for window functions to extract data from their window
 *
 * A window function does not receive its arguments in the normal way
 * (and therefore the concept of strictness is irrelevant).  Instead it
 * receives a "WindowObject", which it can fetch with PG_WINDOW_OBJECT()
 * (note V1 calling convention must be used).  Correct call context can
 * be tested with WindowObjectIsValid().  Although argument values are
 * not passed, the call is correctly set up so that PG_NARGS() can be
 * used and argument type information can be obtained with
 * get_fn_expr_argtype(), get_fn_expr_arg_stable(), etc.
 *
 * Operations on the WindowObject allow the window function to find out
 * the current row number, total number of rows in the partition, etc
 * and to evaluate its argument expression(s) at various rows in the
 * window partition.  See the header comments for each WindowObject API
 * function in nodeWindowAgg.c for details.
 *
 *
 * Portions Copyright (c) 2000-2012, PostgreSQL Global Development Group
 *
 * src/include/windowapi.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef WINDOWAPI_H
#define WINDOWAPI_H

/* values of "seektype" */
#define WINDOW_SEEK_CURRENT 0
#define WINDOW_SEEK_HEAD 1
#define WINDOW_SEEK_TAIL 2
#include "nodes/execnodes.h"

typedef struct rownumber_context {
    int64 rownumber;
} rownumber_context;

/*
 * ranking process information
 */
typedef struct rank_context {
    int64 rank; /* current rank */
} rank_context;
/*
 * All the window function APIs are called with this object, which is passed
 * to window functions as fcinfo->context.
 */
typedef struct WindowObjectData {
    NodeTag type;
    WindowAggState* winstate; /* parent WindowAggState */
    List* argstates;          /* ExprState trees for fn's arguments */
    void* localmem;           /* WinGetPartitionLocalMemory's chunk */
    int markptr;              /* tuplestore mark pointer for this fn */
    int readptr;              /* tuplestore read pointer for this fn */
    int64 markpos;            /* row that markptr is positioned on */
    int64 seekpos;            /* row that readptr is positioned on */
} WindowObjectData;

/* this struct is private in nodeWindowAgg.c */
typedef struct WindowObjectData* WindowObject;
/*
 * We have one WindowStatePerFunc struct for each window function and
 * window aggregate handled by this node.
 */
typedef struct WindowStatePerFuncData {
    /* Links to WindowFunc expr and state nodes this working state is for */
    WindowFuncExprState* wfuncstate;
    WindowFunc* wfunc;

    int numArguments; /* number of arguments */

    FmgrInfo flinfo; /* fmgr lookup data for window function */

    Oid winCollation; /* collation derived for window function */

    /*
     * We need the len and byval info for the result of each function in order
     * to know how to copy/delete values.
     */
    int16 resulttypeLen;
    bool resulttypeByVal;

    bool plain_agg; /* is it just a plain aggregate function? */
    int aggno;      /* if so, index of its PerAggData */

    WindowObject winobj; /* object used in window function API */

    // Vectorization specific
    //
    AttrNumber m_resultCol;  // position in the output batch

} WindowStatePerFuncData;

/*
 * For plain aggregate window functions, we also have one of these.
 */
typedef struct WindowStatePerAggData {
    /* Oids of transfer functions */
    Oid transfn_oid;
    Oid finalfn_oid; /* may be InvalidOid */

    /*
     * fmgr lookup data for transfer functions --- only valid when
     * corresponding oid is not InvalidOid.  Note in particular that fn_strict
     * flags are kept here.
     */
    FmgrInfo transfn;
    FmgrInfo finalfn;

    /*
     * initial value from pg_aggregate entry
     */
    Datum initValue;
    bool initValueIsNull;

    /*
     * cached value for current frame boundaries
     */
    Datum resultValue;
    bool resultValueIsNull;

    /*
     * We need the len and byval info for the agg's input, result, and
     * transition data types in order to know how to copy/delete values.
     */
    int16 inputtypeLen, resulttypeLen, transtypeLen;
    bool inputtypeByVal, resulttypeByVal, transtypeByVal;

    int wfuncno; /* index of associated PerFuncData */

    /* Current transition value */
    Datum transValue; /* current transition value */
    bool transValueIsNull;

    bool noTransValue; /* true if transValue not set yet */
} WindowStatePerAggData;

#define PG_WINDOW_OBJECT() ((WindowObject)fcinfo->context)

#define WindowObjectIsValid(winobj) ((winobj) != NULL && IsA(winobj, WindowObjectData))

extern void* WinGetPartitionLocalMemory(WindowObject winobj, Size sz);

extern int64 WinGetCurrentPosition(WindowObject winobj);
extern int64 WinGetPartitionRowCount(WindowObject winobj);

extern void WinSetMarkPosition(WindowObject winobj, int64 markpos);

extern bool WinRowsArePeers(WindowObject winobj, int64 pos1, int64 pos2);

extern Datum WinGetFuncArgInPartition(
    WindowObject winobj, int argno, int relpos, int seektype, bool set_mark, bool* isnull, bool* isout);

extern Datum WinGetFuncArgInFrame(
    WindowObject winobj, int argno, int relpos, int seektype, bool set_mark, bool* isnull, bool* isout);

extern Datum WinGetFuncArgCurrent(WindowObject winobj, int argno, bool* isnull);

#endif /* WINDOWAPI_H */
