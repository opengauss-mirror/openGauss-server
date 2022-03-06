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
 * -------------------------------------------------------------------------
 *
 * cmkem_version_control.h
 * 
 * IDENTIFICATION
 *	  src/common/interfaces/libpq/client_logic_hooks/cmk_entity_manager_hooks/cmkem_version_control.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef CMKEM_VERSION_CONTROL_H
#define CMKEM_VERSION_CONTROL_H

#include "pg_config.h"

#if ((defined(ENABLE_MULTIPLE_NODES)) || (defined(ENABLE_PRIVATEGAUSS) && (!defined(ENABLE_LITE_MODE))))
#define ENABLE_GS_KTOOL
#define ENABLE_HUAWEI_KMS

#ifdef ENABLE_UT
#define ENABLE_LOCAL_KMS
#endif /* ENABLE_UT */

#else
#define ENABLE_LOCAL_KMS
#endif

#endif /* CMKEM_VERSION_CONTROL_H */
