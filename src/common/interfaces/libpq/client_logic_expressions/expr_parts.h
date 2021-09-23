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
 * expr_parts.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_expressions\expr_parts.h
 *
 * -------------------------------------------------------------------------
 */
 
#ifndef EXPR_PARTS_H
#define EXPR_PARTS_H

#include "nodes/parsenodes_common.h"

typedef struct ExprParts {
    ExprParts()
        : original_size(-1),
          column_ref(NULL),
          column_refl(NULL),
          column_refr(NULL),
          operators(NULL),
          aconst(NULL),
          param_ref(NULL),
          cl_column_oid(0),
          is_empty_repeat(false)
    {}

    int original_size; /* when using in funtions, it will containt the size that should be replaced in query */
    const ColumnRef *column_ref;
    const ColumnRef *column_refl;
    const ColumnRef *column_refr;
    const List *operators;
    const A_Const *aconst;
    const ParamRef *param_ref;
    Oid cl_column_oid;
    bool is_empty_repeat;
} ExprParts;

#endif