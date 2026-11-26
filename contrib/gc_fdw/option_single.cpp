/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-server is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

/*-------------------------------------------------------------------------
 *
 * option_single.cpp
 *		  FDW option handling for gc_fdw
 *
 * IDENTIFICATION
 *		  contrib/gc_fdw/option_single.cpp
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "gc_fdw.h"

#include "access/reloptions.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "commands/defrem.h"
#include "commands/extension.h"
#include "foreign/foreign.h"
#include "gaussdb_version.h"
#include "utils/builtins.h"

PG_FUNCTION_INFO_V1(gc_fdw_validator);
Datum gc_fdw_validator(PG_FUNCTION_ARGS)
{
    PG_RETURN_VOID();
}

