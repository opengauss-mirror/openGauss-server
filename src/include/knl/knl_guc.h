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
 * knl_guc.h
 *        Data struct to store all GUC variables.
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
 * IDENTIFICATION
 *        src/include/knl/knl_guc.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_KNL_KNL_GUC_H_
#define SRC_INCLUDE_KNL_KNL_GUC_H_

#include "knl_guc/knl_session_attr_sql.h"
#include "knl_guc/knl_instance_attr_sql.h"
#include "knl_guc/knl_session_attr_storage.h"
#include "knl_guc/knl_instance_attr_storage.h"
#include "knl_guc/knl_session_attr_security.h"
#include "knl_guc/knl_instance_attr_security.h"
#include "knl_guc/knl_session_attr_network.h"
#include "knl_guc/knl_instance_attr_network.h"
#include "knl_guc/knl_session_attr_memory.h"
#include "knl_guc/knl_instance_attr_memory.h"
#include "knl_guc/knl_session_attr_resource.h"
#include "knl_guc/knl_instance_attr_resource.h"
#include "knl_guc/knl_session_attr_common.h"
#include "knl_guc/knl_instance_attr_common.h"

#endif /* SRC_INCLUDE_KNL_KNL_GUC_H_ */
