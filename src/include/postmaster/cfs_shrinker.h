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
 * -------------------------------------------------------------------------
 * 
 * IDENTIFICATION
 *        src/include/postmaster/cfs_shrinker.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef CFS_SHRINKER_H
#define CFS_SHRINKER_H

#include "postgres.h"
#include "knl/knl_variable.h"
#include "storage/smgr/relfilenode.h"
#include "storage/pmsignal.h"
#include "storage/procarray.h"
#include "storage/proc.h"
#include "storage/procsignal.h"

#define CFS_SHRINKER_ITEM_MAX_COUNT (1024)

struct CfsShrinkItem {
    RelFileNode node;
    ForkNumber forknum;
    char parttype;
};

typedef struct CfsShrinkerShmemStruct {
    uint16 head; // index from zero
    uint16 tail; // index from zero

    // only nowait item can be here.
    CfsShrinkItem nodes[CFS_SHRINKER_ITEM_MAX_COUNT];  // recircled list, FIFO.
    slock_t spinlck;          /* protects all the fields above    SpinLockAcquire(&spinlck) SpinLockRelease(&spinlck); */
} CfsShrinkerShmemStruct;

#define CfsShrinkListIsEmpty(ctx) (ctx->head == ctx->tail)
#define CfsShrinkListIsFull(ctx)  (((ctx->tail + 1) % CFS_SHRINKER_ITEM_MAX_COUNT) == ctx->head)
#define CFS_POP_WAIT_TIMES (60)

void CfsShrinkerShmemListPush(const RelFileNode &rnode, ForkNumber forknum, char parttype);
CfsShrinkItem* CfsShrinkerShmemListPop();

extern Size CfsShrinkerShmemSize();
extern void CfsShrinkerShmemInit(void);

extern ThreadId StartCfsShrinkerCapturer(void);
extern bool IsCfsShrinkerProcess(void);

extern NON_EXEC_STATIC void CfsShrinkerMain();

#endif
