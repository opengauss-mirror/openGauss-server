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
 * security_version.h
 *
 * IDENTIFICATION
 *	  src/gausskernel/security/keymgr/include/comm/security_version.h
 *
 * -------------------------------------------------------------------------
 */

#include "pg_config.h"

#if ((defined(ENABLE_MULTIPLE_NODES)) || (defined(ENABLE_PRIVATEGAUSS)))
#ifdef ENABLE_KT
#define ENABLE_GS_KTOOL
#endif

#define ENABLE_HUAWEI_KMS

#ifdef ENABLE_UT
#define ENABLE_LOCALKMS
#endif

#else
#define ENABLE_LOCALKMS
#endif