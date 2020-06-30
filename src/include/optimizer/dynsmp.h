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
 * dynsmp.h
 *        functions related to dynamic smp.
 * 
 * 
 * IDENTIFICATION
 *        src/include/optimizer/dynsmp.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DYNSMP_H
#define DYNSMP_H

#include "nodes/plannodes.h"

extern void OptimizePlanDop(PlannedStmt* plannedStmt);
extern void InitDynamicSmp();
extern bool IsDynamicSmpEnabled();
extern void ChooseStartQueryDop(int hashTableCount);
extern void CheckQueryDopValue();

typedef struct GMNSDConext {
    int max_non_spill_dop;        /* the max dop we can use without spill to disk */
    bool already_spilled;         /* if the plan is already spilled to disk ? */
    bool spill_if_use_higher_dop; /* if we +1 dop and the plan will spill to disk? */
    List* mem_info_list;          /* the mem info we have to adjust later */
} GMNSDConext;

/*
 * This struct holds the resource info needed by Dynamic Smp.
 * For constant value, we only init once.
 * For volatile value, we have to refreash from query to query.
 */
typedef struct DynamicSmpInfo {
    /*-------CONSTANT-----------------------------------
     *
     * those variable does not change during one session
     *
     * --------------------------------------------------
     */
    /* how many dn installed in one physical machine */
    int num_of_dn_in_one_machine;

    /* how many cpu installed in one physical machine */
    int num_of_cpu_in_one_machine;

    /* how many cpu can be used for one dn */
    int num_of_cpu_for_one_dn;

    /*-------VOLATILE-----------------------------------
     *
     * those variable changes from query to query
     *
     * --------------------------------------------------
     */
    /* how many sql is running concurrently */
    int active_statement;

    /* the cpu util when planning this query */
    int cpu_util;

    int64 free_mem;

    int num_of_machine;
} DynamicSmpInfo;

#define DYNMSP_ALREADY_SPILLED -1
#define DYNMSP_SPILL_IF_USE_HIGHER_DOP -2

#define DS_LOG_PREFIX "[dynamic smp]"
#define UNKNOWN_NUM_OF_DN_IN_ONE_MACHINE -1
#define DEFAULT_NUM_OF_DN_IN_ONE_MACHINE 2
#define UNKNOWN_NUM_OF_CPU_IN_ONE_MACHINE -1
#define DEFAULT_NUM_OF_CPU_IN_ONE_MACHINE 4
#define UNKNOWN_NUM_OF_CPU_FOR_ONE_DN -1
#define DEFAULT_NUM_OF_CPU_FOR_ONE_DN 2
#define UNKNOWN_ACTIVE_STATEMENT -1
#define DEFAULT_ACTIVE_STATEMENT 0
#define UNKNOWN_CPU_UTIL -1
#define DEFAULT_CPU_UTIL 40
#define UNKNOWN_FREE_MEM -1
#define DEFAULT_FREE_MEM 256 * 1024 /* in kb: 256MB * 1024 */
#define UNKNOWN_NUM_OF_MACHINE -1
#define DEFAULT_NUM_OF_MACHINE 1
#define MAX_STREAM_MEM_RATIO 0.25
#define MIN_STREAM_MEM_RATIO 0.05
#define MIN_OPTIMAL_DOP 2

#endif
