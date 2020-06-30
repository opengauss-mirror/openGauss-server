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
 * memctl.h
 *     Routines related to memory quota for queries.
 * 
 * IDENTIFICATION
 *        src/include/workload/memctl.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef MEMCTL_H_
#define MEMCTL_H_

/*
 * during mem decrease procedure, we want to assign decreased mem to large mem,
 * so just through the mem gap, and after decrease, large item should be at least 20%
 * large than the small one
 */
#define DECREASED_MIN_CMP_GAP 0.1

/* Memory usage larger than 4GB is incredible */
#define MAX_OP_MEM 4 * 1024L * 1024L

/* min memory allocate for memory intensive operator for sake of huge initial mem allocation */
#define MIN_OP_MEM 16 * 1024L

extern void CalculateQueryMemMain(PlannedStmt* stmt, bool use_tenant, bool called_by_wlm);

extern bool QueryNeedPlanB(PlannedStmt* stmt);
extern void ReSetNgQueryMem(PlannedStmt* result);
extern THR_LOCAL Oid lc_replan_nodegroup;

#endif /* MEMQUOTA_H_ */
