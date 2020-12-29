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
 * foreignroutine.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/bulkload/foreignroutine.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef FOREIGNROUTINE_H
#define FOREIGNROUTINE_H

#include "nodes/execnodes.h"
#include "commands/copy.h"
#include "commands/explain.h"

#define IS_SHARED_MODE(mode) ((mode) == MODE_SHARED)
#define IS_NORMAL_MODE(mode) ((mode) == MODE_NORMAL)
#define IS_PRIVATE_MODE(mode) ((mode) == MODE_PRIVATE)
#define IS_INVALID_MODE(mode) ((mode) == MODE_INVALID)

typedef struct DistImportExecutionState : public CopyStateData {
    List* source;  /* data input source */
    List* options; /* merged COPY options, excluding filename */
    int rejectLimit;
    Relation errLogRel;
    Datum beginTime;
    List* elogger;
    bool isLogRemote;
    bool needSaveError;
} DistImportExecutionState;

typedef struct DistImportPlanState {
    char* filename;
    List* source;      /* data input source */
    List* options;     /* merged  options, excluding filename */
    BlockNumber pages; /* estimate of file's physical size */
    double ntuples;    /* estimate of number of rows in file */
    int rejectLimit;
    char* errorName;
    ImportMode mode;
    bool writeOnly;
    int fileEncoding;
    bool doLogRemote;
    char* remoteName;

    // explicit constructor
    DistImportPlanState()
        : filename(NULL),
          source(NULL),
          options(NULL),
          pages(0),
          ntuples(0),
          rejectLimit(0),
          errorName(NULL),
          mode(MODE_INVALID),
          writeOnly(false),
          fileEncoding(0),
          doLogRemote(false),
          remoteName(NULL)
    {
    }
} DistImportPlanState;

extern void ProcessDistImportOptions(DistImportPlanState *planstate, List *options, bool isPropagateToFE,
                                     bool isValidate = false);

extern void distImportGetRelSize(PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid);
extern void distImportGetPaths(PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid);
extern ForeignScan* distImportGetPlan(PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid,
    ForeignPath* best_path, List* tlist, List* scan_clauses);

extern void distImportExplain(ForeignScanState* node, ExplainState* es);

extern void distImportBegin(ForeignScanState* node, int eflags);

extern TupleTableSlot* distExecImport(ForeignScanState* node);

extern void distImportEnd(ForeignScanState* node);

extern void distReImport(ForeignScanState* node);

extern void getOBSOptions(ObsCopyOptions* obs_copy_options, List* options);

#endif
