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
 * knl_guc_common.h
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
 *
 * IDENTIFICATION
 *        src/include/knl/knl_guc/knl_guc_common.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_KNL_KNL_GUC_COMMON_H_
#define SRC_INCLUDE_KNL_KNL_GUC_COMMON_H_

#include "c.h"

/* Configuration variables */
#define MAX_GTM_HOST_NUM (8)
#define GUC_MAX_REPLNODE_NUM (9)

#ifdef WIN32
typedef unsigned __int64 GS_UINT64;
#else
typedef unsigned long long GS_UINT64;
#endif

#if defined(__LP64__) || defined(__64BIT__)
typedef unsigned int GS_UINT32;

typedef signed int GS_INT32;
#else
typedef unsigned long GS_UINT32;

typedef signed long GS_INT32;
#endif

typedef unsigned char GS_UCHAR;

const int GTMOPTION_GTM = 0;
const int GTMOPTION_GTMLITE = 1;
const int GTMOPTION_GTMFREE = 2;
const int GTM_OLD_VERSION_NUM = 92061;
const int SLOW_QUERY_VERSION = 92089;
const int STATEMENT_TRACK_VERSION = 92277;
const int DEFAULT_INDEX_KIND_NONE = 0;
const int DEFAULT_INDEX_KIND_LOCAL = 1;
const int DEFAULT_INDEX_KIND_GLOBAL = 2;
const int GLOBAL_SESSION_ID_VERSION = 92407;
#endif /* SRC_INCLUDE_KNL_KNL_GUC_COMMON_H_ */
