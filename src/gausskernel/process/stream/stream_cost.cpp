/* -------------------------------------------------------------------------
 *
 * stream_cost.cpp
 *	  functions used to calculate stream plan costs.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/gaussdbkernel/porcess/stream/stream_cost.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <math.h>
#include <pthread.h>
#include "access/hash.h"
#include "optimizer/cost.h"
#include "optimizer/dataskew.h"
#include "optimizer/planner.h"

void parallel_stream_info_print(ParallelDesc* smpDesc, StreamType type)
{
    char* distri_type = NULL;

    if (NULL == smpDesc)
        return;

    /* Set stream type tag. */
    switch (smpDesc->distriType) {
        case REMOTE_DISTRIBUTE:
            distri_type = "REDISTRIBUTE";
            break;

        case REMOTE_SPLIT_DISTRIBUTE:
            distri_type = "SPLIT REDISTRIBUTE";
            break;

        case REMOTE_BROADCAST:
            distri_type = "BROADCAST";
            break;

        case REMOTE_SPLIT_BROADCAST:
            distri_type = "SPLIT BROADCAST";
            break;

        case LOCAL_DISTRIBUTE:
            distri_type = "LOCAL REDISTRIBUTE";
            break;

        case LOCAL_BROADCAST:
            distri_type = "LOCAL BROADCAST";
            break;

        case LOCAL_ROUNDROBIN:
            distri_type = "LOCAL ROUNDROBIN";
            break;

        default:
            if (type == STREAM_BROADCAST)
                distri_type = "BROADCAST";
            else
                distri_type = "REDISTRIBUTE";
            break;
    }

    /* Print log. */
    elog(DEBUG1,
        "Stream cost: SMP INFO: sendDop: %d, receiveDop: %d, distribute type: %s",
        SET_DOP(smpDesc->producerDop),
        SET_DOP(smpDesc->consumerDop),
        distri_type);
}

/*
 * cost_stream
 * 	computer cost of stream a RepOptInfo object
 */
void cost_stream(StreamPath* stream, int width, bool isJoin)
{
    const double startup_cost_broadcast = 0.0;

    AssertEreport(stream != NULL && stream->subpath != NULL, MOD_OPT, "The stream or subplan is invalid");

    stream->path.startup_cost = startup_cost_broadcast;
    stream->path.startup_cost += stream->subpath->startup_cost;
    stream->path.total_cost = stream->subpath->total_cost;
    stream->path.stream_cost = stream->subpath->startup_cost;

    unsigned int producer_num_datanodes = bms_num_members(stream->path.distribution.bms_data_nodeids);
    unsigned int consumer_num_datanodes = bms_num_members(stream->consumer_distribution.bms_data_nodeids);

    compute_stream_cost(stream->type,
        stream->subpath->locator_type,
        PATH_LOCAL_ROWS(stream->subpath),
        stream->subpath->rows,
        stream->path.multiple,
        width,
        isJoin,
        stream->path.distribute_keys,
        &stream->path.total_cost,
        &stream->path.rows,
        producer_num_datanodes,
        consumer_num_datanodes,
        stream->smpDesc,
        stream->skew_list);

    return;
}

List* get_max_cost_distkey_for_hasdistkey(PlannerInfo* root, List* subPlans, int subPlanNum,
    List** subPlanKeyArray, Cost* subPlanCostArray, Bitmapset** redistributePlanSetCopy)
{
    Cost* keyCostArray = NULL;
    int counter = 0;
    int maxCostIndex = 0;
    int subPlanIndex = 0;
    List* redistributeKeyIndex = NULL;

    /*
     * There are more than one distribute key of subplan,
     * find the max cost distribute key of subplan.
     */
    keyCostArray = (Cost*)palloc0(sizeof(Cost) * subPlanNum);

    while (counter < subPlanNum) {
        List* keyIndex = subPlanKeyArray[counter];

        if (keyIndex == NULL) {
            counter++;
            continue;
        }

        for (subPlanIndex = 0; subPlanIndex < subPlanNum; subPlanIndex++) {
            if (equal(subPlanKeyArray[subPlanIndex], keyIndex)) {
                if (subPlanIndex < counter) {
                    keyCostArray[counter] = 0;
                    break;
                } else {
                    keyCostArray[counter] += subPlanCostArray[subPlanIndex];
                }
            }
        }

        counter++;
    }

    /*
     * Get the max cost for each redistributekey.
     */
    for (counter = 0; counter < subPlanNum; counter++) {
        if (keyCostArray[maxCostIndex] < keyCostArray[counter]) {
            maxCostIndex = counter;
        }
    }

    /*
     * Set other each subplan uesing the max cost redistribute key.
     */
    redistributeKeyIndex = subPlanKeyArray[maxCostIndex];
    for (subPlanIndex = 0; subPlanIndex < subPlanNum; subPlanIndex++) {
        if (!equal(subPlanKeyArray[subPlanIndex], redistributeKeyIndex)) {
            *redistributePlanSetCopy = bms_add_member(*redistributePlanSetCopy, subPlanIndex);
        }
    }

    pfree_ext(keyCostArray);
    keyCostArray = NULL;
    return redistributeKeyIndex;
}

/*
 * We should get the max cost distribute key of subplan
 * as the final redistribute key for other subplan.
 */
List* get_max_cost_distkey_for_nulldistkey(
    PlannerInfo* root, List* subPlans, int subPlanNum, Cost* subPlanCostArray)

{
    Plan* subPlan = NULL;
    int counter = 0;
    int maxCostIndex = 0;
    List* redistributeKeyIndex = NULL;

    /*
     * Get the max cost for each redistributekey.
     */
    for (counter = 0; counter < subPlanNum; counter++) {
        if (subPlanCostArray[maxCostIndex] < subPlanCostArray[counter]) {
            maxCostIndex = counter;
        }
    }

    /* If there is no distkey, we should choose the max cost distkey. */
    subPlan = (Plan*)list_nth(subPlans, maxCostIndex);
    redistributeKeyIndex = make_distkey_for_append(root, subPlan);
    return redistributeKeyIndex;
}

/*
 * Construct distribute key index according to bias, if we cannot find distribute key.
 * If the targetlist of subplan has no relid, we should choose the first three target entry
 * of var for distribute key.
 */
List* make_distkey_for_append(PlannerInfo* root, Plan* subPlan)
{
    List* grplist = NIL;
    List* subPlanKeyArray = NIL;
    const int defaultDistkeyNum = 3;

    /* Construct group clause using targetlist. */
    grplist = make_groupcl_for_append(root, subPlan->targetlist);
    if (grplist != NIL) {
        double multiple;
        List* distkeys = NIL;

        /* Get distkeys according to bias. */
        distkeys = get_distributekey_from_tlist(root, subPlan->targetlist, grplist, subPlan->plan_rows, &multiple);
        if (distkeys != NIL)
            subPlanKeyArray = distributeKeyIndex(root, distkeys, subPlan->targetlist);

        list_free_ext(grplist);
        list_free_ext(distkeys);
    } else {
        /* Choose the first three target entry of var for distribute key. */
        int distkeynum = 0;
        ListCell* teCell = NULL;

        foreach (teCell, subPlan->targetlist) {
            TargetEntry* teEntry = (TargetEntry*)lfirst(teCell);
            Node* node = (Node*)teEntry->expr;

            if (!teEntry->resjunk && IsTypeDistributable(exprType(node))) {
                subPlanKeyArray = lappend_int(subPlanKeyArray, teEntry->resno);
                distkeynum++;

                if (distkeynum <= defaultDistkeyNum)
                    break;
            }
        }
    }

    return subPlanKeyArray;
}