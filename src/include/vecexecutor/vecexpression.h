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
 * vecexpression.h
 *     Vectorized Expression Engine
 * 
 * IDENTIFICATION
 *        src/include/vecexecutor/vecexpression.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef VECEXPRESSION_H_
#define VECEXPRESSION_H_

#include "fmgr.h"
#include "vecexecutor/vectorbatch.h"

#define VectorExprEngine(expr, econtext, selVector, inputVector, isDone)                        \
    ((expr)->vecExprFun ? (*(expr)->vecExprFun)(expr, econtext, selVector, inputVector, isDone) \
                        : (elog(ERROR, "Unsupported expressions for vector engine"), ((ScalarVector*)NULL)))

GenericArgExtract ChooseExtractFun(Oid Dtype, Oid fn_oid = 0);

#endif /* VECEXPRESSION_H_ */
