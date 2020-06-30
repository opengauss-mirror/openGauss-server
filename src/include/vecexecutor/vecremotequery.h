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
 * vecremotequery.h
 * 
 * 
 * IDENTIFICATION
 *        src/include/vecexecutor/vecremotequery.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef VECREMOTEQUERY_H_
#define VECREMOTEQUERY_H_

extern VectorBatch* ExecVecRemoteQuery(VecRemoteQueryState* node);
extern VecRemoteQueryState* ExecInitVecRemoteQuery(VecRemoteQuery* node, EState* estate, int eflags);
extern void ExecEndVecRemoteQuery(VecRemoteQueryState* node);
extern void ExecVecRemoteQueryReScan(VecRemoteQueryState* node, ExprContext* exprCtxt);

#endif /* VECREMOTEQUERY_H_ */
