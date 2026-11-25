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
 * parse_compatibility.cpp
 * handle parsing syntax compatible module
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/backend/parser/parse_compatibility.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres_fe.h"
#include <string.h>

THR_LOCAL bool stmt_contains_operator_plus = false;

bool getOperatorPlusFlag()
{
    return stmt_contains_operator_plus;
}

void resetOperatorPlusFlag()
{
    stmt_contains_operator_plus = false;
    return;
}
