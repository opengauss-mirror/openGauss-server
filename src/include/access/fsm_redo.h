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
 * fsm_redo.h
 *
 *
 * IDENTIFICATION
 *        src/include/access/fsm_redo.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef FSM_REDO_H
#define FSM_REDO_H

#include "access/sdir.h"
#include "access/skey.h"
#include "access/xlogrecord.h"

#include "nodes/primnodes.h"
#include "storage/lock.h"
#include "storage/pagecompress.h"
#include "storage/relfilenode.h"
#include "storage/fsm_internals.h"

#ifdef SAL_DFV_STORE

#define XLOG_FSM_FULL_PAGE_WRITE 0x00
#define XLOG_FSM_TRUNCATE 0x10
#define XLOG_FSM_UPDATE 0x20
#define XLOG_FSM_INIT_PAGE 0x80

typedef struct FsmValue {
    uint32 nodeNo;
    uint32 value;
} FsmValue;

typedef struct FsmUpdateInfo {
    uint32 valueNum;
    FsmValue fsmInfo[NodesPerPage];
} FsmUpdateInfo;

#define SIZE_OF_UPDATE_VALUE(_fsmValue) (offsetof(FsmUpdateInfo, fsmInfo) + ((_fsmValue).valueNum * sizeof(FsmValue)))

typedef struct fsm_truncate {
    BlockNumber blkno;
    RelFileNode rnode;
} fsm_truncate;

extern void FsmXlogRedo(XLogReaderState* record);

#endif
#endif