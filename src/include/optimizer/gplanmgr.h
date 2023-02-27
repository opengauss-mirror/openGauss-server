/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
 * -------------------------------------------------------------------------
 *
 * gplanmgr.h
 *    gplanmgr
 *
 * IDENTIFICATION
 *     src/gausskernel/optimizer/gplanmgr.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef GPLANMGR_H
#define GPLANMGR_H
#include "utils/plancache.h"
#include "nodes/primnodes.h"
#include "nodes/plannodes.h"
#include "nodes/nodes.h"
#include "utils/globalplancore.h"

/*
 * The data structure representing a root of statement.  This is now just
 * a thin veneer over a PlannerInfo entry --- the main addition is that of
 * a name.
 *
 * Note: all subsidiary storage lives in the referenced PlannerInfo entry.
 */
typedef struct
{
    /* dynahash.c requires key to be first field */
    char stmt_name[NAMEDATALEN];
    PlannerInfo *generic_root; /* the actual cached planner info */
    const char* query_string;
    MemoryContext context;
} StatementRoot;

/*
 * BaseRelSelec
 *      selectivity information of a baserel.
 * In PMGR, a query feature consists of selectivities of all baserels, in
 * which the ith dimension is corresponding to the ith baserel.
 *
 * Note that, a baserel can have index selectivitiies if indexes are built
 * on. This additional information helps the manager pick the corrected
 * plan when candidates use different indexes. Please find more detail by
 * refering to MatchIndexStats.
 */
typedef struct RelSelec
{
    Index relid;
    double selectivity;
} RelSelec;

/*
 * BaseRelCI
 *     confidential interval of a plan baserel.
 * This struct records the confidential interval of a plan baserel. If
 * a query matches a candidate plan, each dimension of query feature
 * should fall in the corresponding BaseRelCI of plan. The interval is
 * expanded when optimizer reports a missing match and the interval
 * should embraces the feature to enhance performance.
 *
 * Similar to BaseRelSelec, BaseRelCIs are divided into rel output CI
 * and rel indexical CI.
 */
typedef struct CondInterval
{
    NodeTag type;
    Index relid;
    double lowerbound;
    double upperbound;
} CondInterval;

typedef struct IndexCI
{
    CondInterval ci;
    Oid indexoid;
    List *indexquals;
    uint32 indexkey;
    bool is_partial;
    double init_selec; /* for debugging, selectivity at generation */
} IndexCI;


typedef struct RelCI
{
    NodeTag type;
    Index relid;
    CondInterval ci;
    double init_selec; /* for debugging, selectivity at generation */
} RelCI;

extern CachedPlan *GetDefaultGenericPlan(CachedPlanSource *plansource, ParamListInfo boundParams, List **qlist,
                                         bool *mode);
extern bool ChooseAdaptivePlan(CachedPlanSource *plansource, ParamListInfo boundParams);

extern CachedPlan *GetCustomPlan(CachedPlanSource *plansource, ParamListInfo boundParams, List **qlist);
extern PlanManager *PMGR_CreatePlanManager(MemoryContext parent_cxt, char* stmt_name, GplanSelectionMethod method);
void PMGR_ReleasePlanManager(CachedPlanSource *plansource);
CachedPlan *GetAdaptGenericPlan(CachedPlanSource *plansource,
                                        ParamListInfo boundParams,
                                        List **qlist,
                                        bool *mode);
void DropStmtRoot(const char *stmt_name);
void DropAllStmtRoot(void);
extern List *eval_const_clauses_params(PlannerInfo *root, List *clauses);
extern bool selec_gplan_by_hint(const CachedPlanSource* plansource);
extern void ReleaseCustomPlan(CachedPlanSource *plansource);

#endif
