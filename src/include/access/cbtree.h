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
 * cbtree.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/access/cbtree.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef CTREE_H
#define CTREE_H

#include "fmgr.h"

Datum cbtreebuild(PG_FUNCTION_ARGS);
Datum cbtreeoptions(PG_FUNCTION_ARGS);
Datum cbtreegettuple(PG_FUNCTION_ARGS);
Datum cbtreecanreturn(PG_FUNCTION_ARGS);

#endif /* CBTREE_H */
