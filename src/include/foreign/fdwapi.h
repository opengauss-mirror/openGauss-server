/* -------------------------------------------------------------------------
 *
 * fdwapi.h
 *	  API for foreign-data wrappers
 *
 * Copyright (c) 2010-2012, PostgreSQL Global Development Group
 *
 * src/include/foreign/fdwapi.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef FDWAPI_H
#define FDWAPI_H

#include "nodes/execnodes.h"
#include "nodes/relation.h"
#include "vecexecutor/vectorbatch.h"
#include "vecexecutor/vecnodes.h"
#include "pgstat.h"

/* To avoid including explain.h here, reference ExplainState thus: */
struct ExplainState;

/*
 * Callback function signatures --- see fdwhandler.sgml for more info.
 */

typedef void (*GetForeignRelSize_function)(PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid);

typedef void (*GetForeignPaths_function)(PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid);

typedef ForeignScan* (*GetForeignPlan_function)(PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid,
    ForeignPath* best_path, List* tlist, List* scan_clauses);

typedef void (*BeginForeignScan_function)(ForeignScanState* node, int eflags);

typedef TupleTableSlot* (*IterateForeignScan_function)(ForeignScanState* node);

typedef void (*ReScanForeignScan_function)(ForeignScanState* node);

typedef void (*EndForeignScan_function)(ForeignScanState* node);

typedef void (*AddForeignUpdateTargets_function)(Query* parsetree, RangeTblEntry* target_rte, Relation target_relation);

typedef List* (*PlanForeignModify_function)(
    PlannerInfo* root, ModifyTable* plan, Index resultRelation, int subplan_index);

typedef void (*BeginForeignModify_function)(
    ModifyTableState* mtstate, ResultRelInfo* rinfo, List* fdw_private, int subplan_index, int eflags);

typedef TupleTableSlot* (*ExecForeignInsert_function)(
    EState* estate, ResultRelInfo* rinfo, TupleTableSlot* slot, TupleTableSlot* planSlot);

typedef TupleTableSlot* (*ExecForeignUpdate_function)(
    EState* estate, ResultRelInfo* rinfo, TupleTableSlot* slot, TupleTableSlot* planSlot);

typedef TupleTableSlot* (*ExecForeignDelete_function)(
    EState* estate, ResultRelInfo* rinfo, TupleTableSlot* slot, TupleTableSlot* planSlot);

typedef void (*EndForeignModify_function)(EState* estate, ResultRelInfo* rinfo);

typedef int (*IsForeignRelUpdatable_function)(Relation rel);

typedef void (*ExplainForeignScan_function)(ForeignScanState* node, struct ExplainState* es);

typedef void (*ExplainForeignModify_function)(
    ModifyTableState* mtstate, ResultRelInfo* rinfo, List* fdw_private, int subplan_index, struct ExplainState* es);

typedef int (*AcquireSampleRowsFunc)(Relation relation, /* relation to be sampled */
    int elevel,                                         /* log level */
    HeapTuple* rows,                                    /* store sampled data */
    int targrows,                                       /* the count of tuples we want to sample */
    double* totalrows,                                  /* the actual count of tuples we sampled */
    double* totaldeadrows,                              /* totaldeadrows is no means for DFS table */
    void* additionalData,                               /* We use this parameter to pass data */
    bool estimate_table_rownum);                        /*  wether get sample rows */

typedef bool (*AnalyzeForeignTable_function)(Relation relation, /* relation to be analzyed */
    AcquireSampleRowsFunc* func,                                /* foreign table sampling fucntion point */
    BlockNumber* totalpages,     /* total pages in relation, will be set in analzyeforeigntable_function*/
    void* additionalData,        /* pass data used by AnalyzeForeignTable function */
    bool estimate_table_rownum); /* wether get sample rows */

typedef VectorBatch* (*VecIterateForeignScan_function)(VecForeignScanState* node);

/*
 * @hdfs
 *
 * GetFdwType_function
 * This function is used to return the type of FDW.
 * Return value "hdfs_orc" for hdfs orc file.
 */
typedef int (*GetFdwType_function)();

typedef void (*ValidateTableDef_function)(Node* Obj);

typedef void (*TruncateForeignTable_function)(TruncateStmt* stmt, Relation rel);
typedef void (*VacuumForeignTable_function)(VacuumStmt* stmt, Relation rel);
typedef uint64_t (*GetForeignRelationMemSize_function)(Oid reloid, Oid ixoid);
typedef MotMemoryDetail* (*GetForeignMemSize_function)(uint32* nodeCount, bool isGlobal);
typedef MotSessionMemoryDetail* (*GetForeignSessionMemSize_function)(uint32* sessionCount);
typedef void (*NotifyForeignConfigChange_function)();

typedef enum {
    HDFS_DROP_PARTITIONED_FOREIGNTBL,
    HDFS_CREATE_PARTITIONED_FOREIGNTBL,
    HDFS_ALTER_PARTITIONED_FOREIGNTBL
} HDFS_PARTTBL_OPERATOR;
typedef void (*PartitionTblProcess_function)(Node* Obj, Oid relid, HDFS_PARTTBL_OPERATOR op);

typedef enum {
    HDFS_BLOOM_FILTER
} HDFS_RUNTIME_PREDICATE;
typedef void (*BuildRuntimePredicate_function)(
    ForeignScanState* node, void* value, int colIdx, HDFS_RUNTIME_PREDICATE type);

/*
 * FdwRoutine is the struct returned by a foreign-data wrapper's handler
 * function.  It provides pointers to the callback functions needed by the
 * planner and executor.
 *
 * More function pointers are likely to be added in the future.  Therefore
 * it's recommended that the handler initialize the struct with
 * makeNode(FdwRoutine) so that all fields are set to NULL.  This will
 * ensure that no fields are accidentally left undefined.
 */
typedef struct FdwRoutine {
    NodeTag type;

    /* Functions for scanning foreign tables */
    GetForeignRelSize_function GetForeignRelSize;
    GetForeignPaths_function GetForeignPaths;
    GetForeignPlan_function GetForeignPlan;
    BeginForeignScan_function BeginForeignScan;
    IterateForeignScan_function IterateForeignScan;
    ReScanForeignScan_function ReScanForeignScan;
    EndForeignScan_function EndForeignScan;

    /*
     * These functions are optional.  Set the pointer to NULL for any that are
     * not provided.
     */

    /* Functions for updating foreign tables */
    AddForeignUpdateTargets_function AddForeignUpdateTargets;
    PlanForeignModify_function PlanForeignModify;
    BeginForeignModify_function BeginForeignModify;
    ExecForeignInsert_function ExecForeignInsert;
    ExecForeignUpdate_function ExecForeignUpdate;
    ExecForeignDelete_function ExecForeignDelete;
    EndForeignModify_function EndForeignModify;
    IsForeignRelUpdatable_function IsForeignRelUpdatable;

    /* Support functions for EXPLAIN */
    ExplainForeignScan_function ExplainForeignScan;
    ExplainForeignModify_function ExplainForeignModify;

    /* @hdfs Support functions for ANALYZE */
    AnalyzeForeignTable_function AnalyzeForeignTable;

    /* @hdfs Support function for sampling */
    AcquireSampleRowsFunc AcquireSampleRows;

    /* @hdfs Support Vector Interface */
    VecIterateForeignScan_function VecIterateForeignScan;

    /* @hdfs This function is uesed to return the type of FDW */
    GetFdwType_function GetFdwType;

    /* @hdfs Validate table definition */
    ValidateTableDef_function ValidateTableDef;

    /* @hdfs
     * Partition foreign table process: create/drop
     */
    PartitionTblProcess_function PartitionTblProcess;

    /* @hdfs Runtime dynamic predicate push down like bloom filter. */
    BuildRuntimePredicate_function BuildRuntimePredicate;

    /* Support truncate for foreign table */
    TruncateForeignTable_function TruncateForeignTable;

    /* Support vacuum */
    VacuumForeignTable_function VacuumForeignTable;

    /* Get table/index memory size */
    GetForeignRelationMemSize_function GetForeignRelationMemSize;

    /* Get memory size */
    GetForeignMemSize_function GetForeignMemSize;

    /* Get all session memory size */
    GetForeignSessionMemSize_function GetForeignSessionMemSize;

    /* Notify engine that envelope configuration changed */
    NotifyForeignConfigChange_function NotifyForeignConfigChange;
} FdwRoutine;

/* Functions in foreign/foreign.c */
extern FdwRoutine* GetFdwRoutine(Oid fdwhandler);
extern FdwRoutine* GetFdwRoutineByRelId(Oid relid);
extern FdwRoutine* GetFdwRoutineByServerId(Oid serverid);
extern FdwRoutine* GetFdwRoutineForRelation(Relation relation, bool makecopy);

#endif /* FDWAPI_H */
