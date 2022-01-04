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
 * first_last_agg.cpp
 *     support agg function first() and last
 *
 * IDENTIFICATION
 *        src/common/backend/utils/adt/first_last_agg.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "utils/first_last_agg.h"

Datum first_transition(PG_FUNCTION_ARGS)
{
    PG_RETURN_DATUM(PG_GETARG_DATUM(0));
}

Datum last_transition(PG_FUNCTION_ARGS)
{
    PG_RETURN_DATUM(PG_GETARG_DATUM(1));
}
