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

/* -------------------------------------------------------------------------
 *
 * version.c
 *	 Returns the openGauss version string
 *
 * Copyright (c) 1998-2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *
 * src/backend/utils/adt/version.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "utils/builtins.h"

Datum pgsql_version(PG_FUNCTION_ARGS)
{
    PG_RETURN_TEXT_P(cstring_to_text(PG_VERSION_STR));
}

Datum opengauss_version(PG_FUNCTION_ARGS)
{
    PG_RETURN_TEXT_P(cstring_to_text(OPENGAUSS_VERSION_NUM_STR)); 
}

Datum gs_deployment(PG_FUNCTION_ARGS)
{
#if (defined(ENABLE_MULTIPLE_NODES))
    PG_RETURN_TEXT_P(cstring_to_text("Distribute"));
#elif (defined(ENABLE_PRIVATEGAUSS))
    PG_RETURN_TEXT_P(cstring_to_text("BusinessCentralized"));
#else
    PG_RETURN_TEXT_P(cstring_to_text("OpenSourceCentralized"));
#endif
}

