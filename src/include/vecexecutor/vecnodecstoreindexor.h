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
 * vecnodecstoreindexor.h
 * 
 * 
 * IDENTIFICATION
 *        src/include/vecexecutor/vecnodecstoreindexor.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef VECCSTOREINDEXOR_H
#define VECCSTOREINDEXOR_H

#include "vecexecutor/vecnodes.h"

extern CStoreIndexOrState* ExecInitCstoreIndexOr(CStoreIndexOr* node, EState* estate, int eflags);
extern VectorBatch* ExecCstoreIndexOr(CStoreIndexOrState* node);
extern void ExecEndCstoreIndexOr(CStoreIndexOrState* node);
extern void ExecReScanCstoreIndexOr(CStoreIndexOrState* node);

#endif /* VECCSTOREINDEXOR_H */
