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
*---------------------------------------------------------------------------------------
*
*  nodeTrainModel.h
*
* IDENTIFICATION
*        src/include/executor/nodeTrainModel.h
*
* ---------------------------------------------------------------------------------------
*/

#ifndef NODE_TRAIN_MODEL_H
#define NODE_TRAIN_MODEL_H

#include "nodes/execnodes.h"

extern TrainModelState* ExecInitTrainModel(TrainModel* node, EState* estate, int eflags);
extern void ExecEndTrainModel(TrainModelState* state);

#endif /* NODE_TRAIN_MODEL_H */
