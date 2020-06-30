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
 * vecmaterial.h
 * 
 * 
 * IDENTIFICATION
 *        src/include/vecexecutor/vecmaterial.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef VECMATERIAL_H
#define VECMATERIAL_H

#include "vecexecutor/vecnodes.h"

extern VecMaterialState* ExecInitVecMaterial(VecMaterial* node, EState* estate, int eflags);
extern VectorBatch* ExecVecMaterial(VecMaterialState* node);
extern void ExecEndVecMaterial(VecMaterialState* node);
extern void ExecVecMaterialMarkPos(VecMaterialState* node);
extern void ExecVecMaterialRestrPos(VecMaterialState* node);
extern void ExecVecReScanMaterial(VecMaterialState* node);
extern void ExecEarlyFreeVecMaterial(VecMaterialState* node);

#endif /* VECMATERIAL_H */
