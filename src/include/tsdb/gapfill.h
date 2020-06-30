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
 * gapfill.h
 *     the strucutre of gapfill
 * 
 * IDENTIFICATION
 *        src/include/tsdb/gapfill.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef GAPFILL_H
#define GAPFILL_H

#include <postgres.h>
#include <nodes/nodeFuncs.h>
#include <optimizer/pathnode.h>
#include <optimizer/paths.h>
#include <optimizer/planner.h>

extern const char* FILL_FUNCTION;
extern const char* FILL_LAST_FUNCTION;

Node* fill_state_create(ExtensiblePlan*);

typedef enum FillFetchState {
    FETCHED_NONE,
    FETCHED_ONE,
    FETCHED_LAST,
    FETCHED_FILLING,
    FETCHED_END,
    FETCHED_NEXT_GROUP,
    FETCHED_NEXT_GROUP_LAST,
    FETCHED_NEXT_GROUP_FILLING,
    FETCHED_NEXT_GROUP_END,
} FillFetchState;

typedef enum FillColumnType { NULL_COLUMN, TIME_COLUMN, GROUP_COLUMN, FILL_LAST_COLUMN } FillColumnType;

typedef struct FillColumnStateBase {
    FillColumnType ctype;
    Oid typid;
    bool typbyval;
    int16 typlen;
} FillColumnStateBase;

typedef struct FillColumnState {
    FillColumnStateBase base;
    Datum value;
    bool isnull;
} FillColumnState;

typedef struct FillState {
    ExtensiblePlanState epstate;
    Plan* subplan;

    Oid fill_typid;
    int64 fill_start;
    int64 fill_end;
    int64 fill_period;

    int64 next_timestamp;
    int64 subslot_time; /* time of tuple in subslot */

    int time_index;          /* position of time column */
    TupleTableSlot* subslot; /* TupleTableSlot from subplan */
    TupleTableSlot* last_group_subslot;
    bool multigroup; /* multiple groupings */
    bool groups_initialized;

    int ncolumns;
    FillColumnStateBase** columns;

    ProjectionInfo* pi;
    TupleTableSlot* scanslot;
    FillFetchState state;
    HTAB* fill_htab;
} FillState;

typedef struct FillInfo {
    int64 time;
    TupleTableSlot* slot;
} FillInfo;

void fill_begin(ExtensiblePlanState* node, EState* estate, int eflags);
TupleTableSlot* fill_exec(ExtensiblePlanState* node);
void fill_end(ExtensiblePlanState* node);
Datum fill_exec_expr(FillState* state, Expr* expr, bool* isnull);
void fill_state_initialize_columns(FillState* state);

#endif /* GAPFILL_H */
