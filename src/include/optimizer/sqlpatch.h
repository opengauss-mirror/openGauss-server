/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
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
 * sqlpatch.h
 *		prototypes for sqlpatch.c.
 * 
 *
 * IDENTIFICATION
 *      src/include/commands/sqlpatch.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SQLPATCH_H
#define SQLPATCH_H

#include "postgres.h"
#include "utils/plancache.h"

extern void sql_patch_sql_register_hook();
extern bool CheckRecreateCachePlanBySqlPatch(const CachedPlanSource *plansource);
extern void RevalidateGplanBySqlPatch(CachedPlanSource *plansource);

#endif /* SQLPATCH_H */