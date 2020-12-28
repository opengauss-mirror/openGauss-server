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
 * roach_adpter.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/bulkload/roach_adpter.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef __ROACH_ADPTER_H__
#define __ROACH_ADPTER_H__

#include "bulkload/roach_api.h"
#include "commands/copy.h"

template <bool import>
extern bool getNextRoach(CopyState cstate);

extern int copyGetRoachData(CopyState cstate, void* databuf, int minread, int maxread);

template <bool import>
extern void initRoachState(CopyState cstate, const char* filename, List* totalTask);

extern void endRoachBulkLoad(CopyState cstate);

extern void exportRoach(CopyState cstate);

extern void exportRoachFlushOut(CopyState cstate, bool isWholeLineAtEnd);

extern RoachRoutine* initRoachRoutine();

#endif
