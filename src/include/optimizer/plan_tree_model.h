/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * plan_tree_model.h
 *
 * IDENTIFICATION
 * src/include/optimizer/ml_model.h
 *
 * DESCRIPTION
 * Declaration of externel APIs of Code/src/backend/utils/learn/plan_tree_model.cpp
 *
 * -------------------------------------------------------------------------
 */
#ifndef PLAN_TREE_MODEL_H
#define PLAN_TREE_MODEL_H

#include "postgres.h"

extern char* TreeModelTrain(Form_gs_opt_model modelinfo, char* labels);
extern char* TreeModelPredict(const char* modelName, char* filepath, const char* ip, int port);

#define ParseConfigBuf(buf, conninfo, msg_str)                                                            \
    do {                                                                                                  \
        pfree_ext(buf);                                                                                   \
        DestroyConnInfo(conninfo);                                                                        \
        ereport(ERROR, (errmodule(MOD_OPT_AI), errcode(ERRCODE_UNEXPECTED_NODE_STATE), errmsg(msg_str))); \
    } while (0)

#define ParseResBuf(buf, filename, msg_str)                                                               \
    do {                                                                                                  \
        pfree_ext(buf);                                                                                   \
        if ((filename) != NULL) {                                                                         \
            Unlinkfile(filename);                                                                         \
        }                                                                                                 \
        ereport(ERROR, (errmodule(MOD_OPT_AI), errcode(ERRCODE_UNEXPECTED_NODE_STATE), errmsg(msg_str))); \
    } while (0)

#endif /* PLAN_TREE_MODEL_H */