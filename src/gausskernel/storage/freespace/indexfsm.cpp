/* -------------------------------------------------------------------------
 *
 * indexfsm.cpp
 *	  openGauss free space map for quickly finding free pages in relations
 *
 *
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-server is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/freespace/indexfsm.cpp
 *
 *
 * NOTES:
 *
 *	This is similar to the FSM used for heap, in freespace.c, but instead
 *	of tracking the amount of free space on pages, we only track whether
 *	pages are completely free or in-use. We use the same FSM implementation
 *	as for heaps, using BLCKSZ - 1 to denote used pages, and 0 for unused.
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "storage/freespace.h"
#include "storage/indexfsm.h"

/*
 * Exported routines
 *
 * GetFreeIndexPage - return a free page from the FSM
 *
 * As a side effect, the page is marked as used in the FSM.
 */
BlockNumber GetFreeIndexPage(Relation rel)
{
    BlockNumber blkno = GetPageWithFreeSpace(rel, BLCKSZ / 2);
    if (blkno != InvalidBlockNumber)
        RecordUsedIndexPage(rel, blkno);

    return blkno;
}

/*
 * RecordFreeIndexPage - mark a page as free in the FSM
 */
void RecordFreeIndexPage(Relation rel, BlockNumber freeBlock)
{
    RecordPageWithFreeSpace(rel, freeBlock, BLCKSZ - 1);
}

/*
 * RecordUsedIndexPage - mark a page as used in the FSM
 */
void RecordUsedIndexPage(Relation rel, BlockNumber usedBlock)
{
    RecordPageWithFreeSpace(rel, usedBlock, 0);
}

/*
 * IndexFreeSpaceMapVacuum - scan and fix any inconsistencies in the FSM
 */
void IndexFreeSpaceMapVacuum(Relation rel)
{
    FreeSpaceMapVacuum(rel);
}

