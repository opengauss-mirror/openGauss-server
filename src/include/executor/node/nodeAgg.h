/* -------------------------------------------------------------------------
 *
 * nodeAgg.h
 *	  prototypes for nodeAgg.c
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeAgg.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef NODEAGG_H
#define NODEAGG_H

#include "nodes/execnodes.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/node/nodeAgg.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/tlist.h"
#include "parser/parse_agg.h"
#include "parser/parse_coerce.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/tuplesort.h"
#include "utils/datum.h"
#include "vecexecutor/vechashtable.h"

// hash strategy
#define MEMORY_HASHAGG 0
#define DIST_HASHAGG 1

#define HASHAGG_PREPARE 0
#define HASHAGG_FETCH 1

#define HASH_MIN_FILENUMBER 48
#define HASH_MAX_FILENUMBER 512

typedef struct AggWriteFileControl {
    bool spillToDisk; /*whether data write to temp file*/
    bool finishwrite;
    int strategy;
    int runState;
    int64 useMem;   /* useful memory, in bytes */
    int64 totalMem; /*total memory, in bytes*/
    int64 inmemoryRownum;
    hashSource* m_hashAggSource;
    hashFileSource* filesource;
    int filenum;
    int curfile;
    int64 maxMem;  /* mem spread memory, in bytes */
    int spreadNum; /* dynamic spread time */
} AggWriteFileControl;

/*
 * AggStatePerAggData - per-aggregate working state for the Agg scan
 */
typedef struct AggStatePerAggData {
    /*
     * These values are set up during ExecInitAgg() and do not change
     * thereafter:
     */

    /* Links to Aggref expr and state nodes this working state is for */
    AggrefExprState* aggrefstate;
    Aggref* aggref;

    /* number of input arguments for aggregate function proper */
    int numArguments;

    /* number of inputs including ORDER BY expressions */
    int numInputs;

    bool        is_avg;

    /*
     * Number of aggregated input columns to pass to the transfn.  This
     * includes the ORDER BY columns for ordered-set aggs, but not for plain
     * aggs.  (This doesn't count the transition state value!)
     */
    int numTransInputs;

    /* Oids of transfer functions */
    Oid transfn_oid;
    Oid finalfn_oid; /* may be InvalidOid */
#ifdef PGXC
    Oid collectfn_oid; /* may be InvalidOid */
#endif                 /* PGXC */

    /*
     * fmgr lookup data for transfer functions --- only valid when
     * corresponding oid is not InvalidOid.  Note in particular that fn_strict
     * flags are kept here.
     */
    FmgrInfo transfn;
    FmgrInfo finalfn;
#ifdef PGXC
    FmgrInfo collectfn;
#endif /* PGXC */

    /* Input collation derived for aggregate */
    Oid aggCollation;

    /* number of sorting columns */
    int numSortCols;

    /* number of sorting columns to consider in DISTINCT comparisons */
    /* (this is either zero or the same as numSortCols) */
    int numDistinctCols;

    /* deconstructed sorting information (arrays of length numSortCols) */
    AttrNumber* sortColIdx;
    Oid* sortOperators;
    Oid* sortCollations;
    bool* sortNullsFirst;

    /*
     * fmgr lookup data for input columns' equality operators --- only
     * set/used when aggregate has DISTINCT flag.  Note that these are in
     * order of sort column index, not parameter index.
     */
    FmgrInfo* equalfns; /* array of length numDistinctCols */

    /*
     * initial value from pg_aggregate entry
     */
    Datum initValue;
    bool initValueIsNull;
#ifdef PGXC
    Datum initCollectValue;
    bool initCollectValueIsNull;
#endif /* PGXC */

    /*
     * We need the len and byval info for the agg's input, result, and
     * transition data types in order to know how to copy/delete values.
     *
     * Note that the info for the input type is used only when handling
     * DISTINCT aggs with just one argument, so there is only one input type.
     */
    int16 inputtypeLen, resulttypeLen, transtypeLen;
    bool inputtypeByVal, resulttypeByVal, transtypeByVal;

    /*
     * Stuff for evaluation of inputs.	We used to just use ExecEvalExpr, but
     * with the addition of ORDER BY we now need at least a slot for passing
     * data to the sort object, which requires a tupledesc, so we might as
     * well go whole hog and use ExecProject too.
     */
    TupleDesc evaldesc;       /* descriptor of input tuples */
    ProjectionInfo* evalproj; /* projection machinery */

    /*
     * Slots for holding the evaluated input arguments.  These are set up
     * during ExecInitAgg() and then used for each input row.
     */
    TupleTableSlot* evalslot; /* current input tuple */
    TupleTableSlot* uniqslot; /* used for multi-column DISTINCT */

    /*
     * These values are working state that is initialized at the start of an
     * input tuple group and updated for each input tuple.
     *
     * For a simple (non DISTINCT/ORDER BY) aggregate, we just feed the input
     * values straight to the transition function.	If it's DISTINCT or
     * requires ORDER BY, we pass the input values into a Tuplesort object;
     * then at completion of the input tuple group, we scan the sorted values,
     * eliminate duplicates if needed, and run the transition function on the
     * rest.
     */

    Tuplesortstate** sortstates; /* sort object, if DISTINCT or ORDER BY */
    Tuplesortstate* sortstate;   /* sort object, if DISTINCT or ORDER BY */

    /*
     * This field is a pre-initialized FunctionCallInfo struct used for
     * calling this aggregate's transfn.  We save a few cycles per row by not
     * re-initializing the unchanging fields; which isn't much, but it seems
     * worth the extra space consumption.
     */
    FunctionCallInfoData transfn_fcinfo;
} AggStatePerAggData;

/*
 * AggStatePerPhaseData - per-grouping-set-phase state
 *
 * Grouping sets are divided into "phases", where a single phase can be
 * processed in one pass over the input. If there is more than one phase, then
 * at the end of input from the current phase, state is reset and another pass
 * taken over the data which has been re-sorted in the mean time.
 *
 * Accordingly, each phase specifies a list of grouping sets and group clause
 * information, plus each phase after the first also has a sort order.
 */
typedef struct AggStatePerPhaseData {
    int numsets;              /* number of grouping sets (or 0) */
    int* gset_lengths;        /* lengths of grouping sets */
    Bitmapset** grouped_cols; /* column groupings for rollup */
    FmgrInfo* eqfunctions;    /* per-grouping-field equality fns */
    Agg* aggnode;             /* Agg node for phase data */
    Sort* sortnode;           /* Sort node for input ordering for phase */
} AggStatePerPhaseData;

/*
 * AggStatePerGroupData - per-aggregate-per-group working state
 *
 * These values are working state that is initialized at the start of
 * an input tuple group and updated for each input tuple.
 *
 * In AGG_PLAIN and AGG_SORTED modes, we have a single array of these
 * structs (pointed to by aggstate->pergroup); we re-use the array for
 * each input group, if it's AGG_SORTED mode.  In AGG_HASHED mode, the
 * hash table contains an array of these structs for each tuple group.
 *
 * Logically, the sortstate field belongs in this struct, but we do not
 * keep it here for space reasons: we don't support DISTINCT aggregates
 * in AGG_HASHED mode, so there's no reason to use up a pointer field
 * in every entry of the hashtable.
 */
typedef struct AggStatePerGroupData {
    Datum transValue;   /* current transition value */
    Datum collectValue; /* current collection value */
    bool transValueIsNull;

    bool noTransValue; /* true if transValue not set yet */
    bool collectValueIsNull;
    bool noCollectValue; /* true if the collectValue not set yet */

    /*
     * Note: noTransValue initially has the same value as transValueIsNull,
     * and if true both are cleared to false at the same time.	They are not
     * the same though: if transfn later returns a NULL, we want to keep that
     * NULL and not auto-replace it with a later input value. Only the first
     * non-NULL input will be auto-substituted.
     */
#ifdef PGXC
    /*collectValue, collectValueIsNull and noCollectValue are added by PGXC*/
    /*
     * We should be able to reuse the fields above, rather than having
     * separate fields here, that can be done once we get rid of different
     * collection and transition result types in pg_aggregate.h. Collection at
     * Coordinator is equivalent to the transition at non-XC PG.
     */
#endif /* PGXC */
} AggStatePerGroupData;

/*
 * To implement hashed aggregation, we need a hashtable that stores a
 * representative tuple and an array of AggStatePerGroup structs for each
 * distinct set of GROUP BY column values.	We compute the hash key from
 * the GROUP BY columns.
 */
typedef struct AggHashEntryData* AggHashEntry;

typedef struct AggHashEntryData {
    TupleHashEntryData shared; /* common header for hash table entries */
    /* per-aggregate transition status array - must be last! */
    AggStatePerGroupData pergroup[FLEXIBLE_ARRAY_MEMBER]; /* VARIABLE LENGTH ARRAY */
} AggHashEntryData;                                       /* VARIABLE LENGTH STRUCT */

extern AggState* ExecInitAgg(Agg* node, EState* estate, int eflags);
extern TupleTableSlot* ExecAgg(AggState* node);
extern void ExecEndAgg(AggState* node);
extern void ExecReScanAgg(AggState* node);
extern Datum GetAggInitVal(Datum textInitVal, Oid transtype);
extern Size hash_agg_entry_size(int numAggs);

extern Datum aggregate_dummy(PG_FUNCTION_ARGS);
extern long ExecGetMemCostAgg(Agg*);
extern void initialize_phase(AggState* aggstate, int newphase);
extern List* find_hash_columns(AggState* aggstate);
extern uint32 ComputeHashValue(TupleHashTable hashtbl);
extern int getPower2Num(int num);
extern void agg_spill_to_disk(AggWriteFileControl* TempFileControl, TupleHashTable hashtable, TupleTableSlot* hashslot,
    int numGroups, bool isAgg, int planId, int dop, Instrumentation* intrument = NULL);
extern void ExecEarlyFreeAggregation(AggState* node);
extern void ExecReSetAgg(AggState* node);

#endif /* NODEAGG_H */
