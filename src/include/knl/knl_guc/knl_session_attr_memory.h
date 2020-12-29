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
 * knl_session_attr_memory.h
 * knl_session_attr_memory.h
 *        Data struct to store all knl_session_attr_memory GUC variables.
 *
 *   When anyone try to added variable in this file, which means add a guc
 *   variable, there are several rules needed to obey:
 *
 *   add variable to struct 'knl_@level@_attr_@group@'
 *
 *   @level@:
 *   1. instance: the level of guc variable is PGC_POSTMASTER.
 *   2. session: the other level of guc variable.
 *
 *   @group@: sql, storage, security, network, memory, resource, common
 *   select the group according to the type of guc variable.
 * 
 * 
 * IDENTIFICATION
 *        src/include/knl/knl_guc/knl_session_attr_memory.h
 *
 * ---------------------------------------------------------------------------------------
 */


#ifndef SRC_INCLUDE_KNL_KNL_SESSION_ATTR_MEMORY_H_
#define SRC_INCLUDE_KNL_KNL_SESSION_ATTR_MEMORY_H_

#include "knl/knl_guc/knl_guc_common.h"

typedef struct knl_session_attr_memory {
    bool enable_memory_context_control;
    bool disable_memory_protect;
    int work_mem;
    int maintenance_work_mem;
    char* memory_detail_tracking;
    char* uncontrolled_memory_context;
    int memory_tracking_mode;
} knl_session_attr_memory;

#endif /* SRC_INCLUDE_KNL_KNL_SESSION_ATTR_MEMORY_H_ */
