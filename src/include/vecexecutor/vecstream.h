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
 * vecstream.h
 * 
 * 
 * IDENTIFICATION
 *        src/include/vecexecutor/vecstream.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef VECNODE_H
#define VECNODE_H

#include "vecexecutor/vecnodes.h"

// Extern functions
//
extern void HandleStreamBatch(VecStreamState* node, char* msg, int msg_len);
extern VecStreamState* ExecInitVecStream(Stream* node, EState* estate, int eflags);
extern VectorBatch* ExecVecStream(VecStreamState* node);
extern void ExecEndVecStream(VecStreamState* node);
extern void redistributeStreamInitType(TupleDesc desc, uint32* colsType);

#endif /* VECNODE_H */