/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
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
 * subpartitionpruning.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/optimizer/subpartitionpruning.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SUBPARTPRUNING_H_
#define SUBPARTPRUNING_H_

PruningResult* getFullPruningResult(Relation relation);
SubPartitionPruningResult* PreGetSubPartitionFullPruningResult(Relation relation, Oid partitionid);

#endif /* SUBPARTPRUNING_H_ */
