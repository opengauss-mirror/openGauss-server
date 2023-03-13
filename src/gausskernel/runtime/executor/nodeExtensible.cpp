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
 * nodeExtensible.cpp
 * ExtensiblePlan is an extended plan node which supports various customize operations.
 * Normally we built our ExtensiblePlan on the upper level of the original plan.
 * To implement the ExtensiblePlan, we should first implement BeginExtensiblePlan() method,
 * ExecExtensiblePlan() method, EndExtensiblePlan() method ReScanExtensiblePlan() method.
 *
 * IDENTIFICATION
 *        src/gausskernel/runtime/executor/nodeExtensible.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "executor/executor.h"
#include "executor/node/nodeExtensible.h"
#include "nodes/execnodes.h"
#include "nodes/plannodes.h"
#include "parser/parsetree.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#ifdef ENABLE_MULTIPLE_NODES
#include "tsdb/optimizer/planner.h"
#endif

const int EXTNODENAME_MAX_LEN = 64;
static HTAB* g_extensible_plan_methods = NULL;
const int HASHTABLE_LENGTH = 100;
const char* EXTENSIBLE_PLAN_METHODS_LABEL = "Extensible Plan Methods";

static TupleTableSlot* ExecExtensiblePlan(PlanState* state);

typedef struct {
    char extnodename[EXTNODENAME_MAX_LEN];
    void* extnodemethods;
} ExtensibleNodeEntry;

#ifdef ENABLE_MULTIPLE_NODES
/*
 * An internal function to register a new callback structure
 */
static void RegisterExtensibleNodeEntry(HTAB* p_htable, const char* ext_node_name, void* ext_node_methods)
{
    ExtensibleNodeEntry* entry = NULL;
    bool found = true;
    if (strlen(ext_node_name) >= EXTNODENAME_MAX_LEN)
        elog(ERROR, "extensible node name is too long");

    entry = (ExtensibleNodeEntry*)hash_search(p_htable, ext_node_name, HASH_ENTER, &found);
    if (found)
        ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_OBJECT),
                 errmsg("extensible node type \"%s\" already exists",
                        ext_node_name)));

    entry->extnodemethods = ext_node_methods;
}

void InitExtensiblePlanMethodsHashTable()
{
    HASHCTL ctl;
    if (g_extensible_plan_methods == NULL) {
        errno_t rc = memset_s(&ctl, sizeof(HASHCTL), 0, sizeof(HASHCTL));
        securec_check(rc, "", "");
        ctl.keysize = EXTNODENAME_MAX_LEN;
        ctl.entrysize = sizeof(ExtensibleNodeEntry);
        ctl.hash = string_hash;
        g_extensible_plan_methods = hash_create(EXTENSIBLE_PLAN_METHODS_LABEL, HASHTABLE_LENGTH,
            &ctl, HASH_ELEM | HASH_FUNCTION);
    }
    RegisterExtensibleNodeEntry(g_extensible_plan_methods, JOIN_TS_TAG_METHOD_NAME, &join_ts_tag_plan_methods);
    RegisterExtensibleNodeEntry(g_extensible_plan_methods, JOIN_TS_DELTA_METHOD_NAME, &join_ts_delta_plan_methods);
}
#endif

ExtensiblePlanState* ExecInitExtensiblePlan(ExtensiblePlan* eplan, EState* estate, int eflags)
{
    ExtensiblePlanState* extensionPlanState;
    Relation scan_rel = NULL;
    Index scanrelid = eplan->scan.scanrelid;
    Index tlistvarno;

    /*
     * Allocate the ExtensiblePlanState object.  We let the extensible scan provider
     * do the palloc, in case it wants to make a larger object that embeds
     * ExtensiblePlanState as the first field.  It must set the node tag and the
     * methods field correctly at this time.  Other standard fields should be
     * set to zero.
     */
    extensionPlanState = (ExtensiblePlanState*)eplan->methods->CreateExtensiblePlanState(eplan);

    /* ensure flags is filled correctly */
    extensionPlanState->flags = eplan->flags;

    /* fill up fields of ScanState */
    extensionPlanState->ss.ps.plan = &eplan->scan.plan;
    extensionPlanState->ss.ps.state = estate;
    extensionPlanState->ss.ps.ExecProcNode = ExecExtensiblePlan;

    /* create expression context for node */
    ExecAssignExprContext(estate, &extensionPlanState->ss.ps);

    /* initialize child expressions */
    if (estate->es_is_flt_frame) {
        extensionPlanState->ss.ps.qual = (List*)ExecInitQualByFlatten(eplan->scan.plan.qual, (PlanState*)extensionPlanState);
    } else {
        extensionPlanState->ss.ps.targetlist =
            (List*)ExecInitExpr((Expr*)eplan->scan.plan.targetlist, (PlanState*)extensionPlanState);
    extensionPlanState->ss.ps.qual = (List*)ExecInitExprByRecursion((Expr*)eplan->scan.plan.qual, (PlanState*)extensionPlanState);
    }

    /* tuple table initialization */
    ExecInitScanTupleSlot(estate, &extensionPlanState->ss);
    ExecInitResultTupleSlot(estate, &extensionPlanState->ss.ps);

    /*
     * open the base relation, if any, and acquire an appropriate lock on it
     */
    if (scanrelid > 0) {
        scan_rel = ExecOpenScanRelation(estate, scanrelid);
        extensionPlanState->ss.ss_currentRelation = scan_rel;
    }

    /*
     * Determine the scan tuple type.  If the extensible scan provider provided a
     * targetlist describing the scan tuples, use that; else use base
     * relation's rowtype.
     */
    if (eplan->extensible_plan_tlist != NIL || scan_rel == NULL) {
        TupleDesc scan_tupdesc;

        scan_tupdesc = ExecTypeFromTL(eplan->extensible_plan_tlist, false, false);
        ExecAssignScanType(&extensionPlanState->ss, scan_tupdesc);
        /* Node's targetlist will contain Vars with varno = INDEX_VAR */
        tlistvarno = INDEX_VAR;
    } else {
        ExecAssignScanType(&extensionPlanState->ss, RelationGetDescr(scan_rel));
        /* Node's targetlist will contain Vars with varno = scanrelid */
        tlistvarno = scanrelid;
    }

    /*
     * Initialize result tuple type and projection info.
     */
    ExecAssignResultTypeFromTL(
            &extensionPlanState->ss.ps,
            extensionPlanState->ss.ss_ScanTupleSlot->tts_tupleDescriptor->td_tam_ops);
    ExecAssignScanProjectionInfoWithVarno(&extensionPlanState->ss, tlistvarno);
    Assert(extensionPlanState->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor->td_tam_ops);

    /*
     * The callback of extensible-scan provider applies the final initialization
     * of the extensible-scan-state node according to its logic.
     */
    extensionPlanState->methods->BeginExtensiblePlan(extensionPlanState, estate, eflags);

    return extensionPlanState;
}

static TupleTableSlot* ExecExtensiblePlan(PlanState* state)
{
    ExtensiblePlanState* node = castNode(ExtensiblePlanState, state);
    Assert(node->methods->ExecExtensiblePlan != NULL);
    return node->methods->ExecExtensiblePlan(node);
}

void ExecEndExtensiblePlan(ExtensiblePlanState* node)
{
    Assert(node->methods->EndExtensiblePlan != NULL);
    node->methods->EndExtensiblePlan(node);

    /* Free the exprcontext */
    ExecFreeExprContext(&node->ss.ps);

    /* Clean out the tuple table */
    (void)ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
    (void)ExecClearTuple(node->ss.ss_ScanTupleSlot);

    /* Close the heap relation */
    if (node->ss.ss_currentRelation) {
        ExecCloseScanRelation(node->ss.ss_currentRelation);
    }
}

void ExecReScanExtensiblePlan(ExtensiblePlanState* node)
{
    Assert(node->methods->ReScanExtensiblePlan != NULL);
    node->methods->ReScanExtensiblePlan(node);
}

/*
 * An internal routine to get an ExtensibleNodeEntry by the given identifier
 */
static void* GetExtensibleNodeEntry(HTAB* htable, const char* extnodename, bool missing_ok)
{
    ExtensibleNodeEntry* entry = NULL;

    if (htable != NULL) {
        entry = (ExtensibleNodeEntry*)hash_search(htable, extnodename, HASH_FIND, NULL);
    }
    if (entry == NULL) {
        if (missing_ok) {
            return NULL;
        }
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("ExtensibleNodeMethods \"%s\" was not registered", extnodename)));
    }
    if (entry != NULL) {
        return entry->extnodemethods;
    } else {
        return NULL;
    }
}

/*
 * Get the methods for a given name of ExtensiblePlanMethods
 */
ExtensiblePlanMethods* GetExtensiblePlanMethods(const char* ExtensibleName, bool missing_ok)
{
    return (ExtensiblePlanMethods*)GetExtensibleNodeEntry(g_extensible_plan_methods, ExtensibleName, missing_ok);
}
