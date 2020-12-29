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
 * pruneheap.cpp
 *    common function for heap page
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/redo/pruneheap.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/heapam.h"
#include "access/redo_common.h"

/*
 * Perform the actual page changes needed by heap_page_prune.
 * It is expected that the caller has suitable pin and lock on the
 * buffer, and is inside a critical section.
 *
 * This is split out because it is also used by heap_xlog_clean()
 * to replay the WAL record when needed after a crash.	Note that the
 * arguments are identical to those of log_heap_clean().
 */
void heap_page_prune_execute(Page page, OffsetNumber *redirected, int nredirected, OffsetNumber *nowdead, int ndead,
                             OffsetNumber *nowunused, int nunused, bool repairFragmentation)
{
    OffsetNumber *offnum = NULL;
    int i;

    /* Update all redirected line pointers */
    offnum = redirected;
    for (i = 0; i < nredirected; i++) {
        OffsetNumber fromoff = *offnum++;
        OffsetNumber tooff = *offnum++;
        ItemId fromlp = PageGetItemId(page, fromoff);

        ItemIdSetRedirect(fromlp, tooff);
    }

    /* Update all now-dead line pointers */
    offnum = nowdead;
    for (i = 0; i < ndead; i++) {
        OffsetNumber off = *offnum++;
        ItemId lp = PageGetItemId(page, off);

        ItemIdSetDead(lp);
    }

    /* Update all now-unused line pointers */
    offnum = nowunused;
    for (i = 0; i < nunused; i++) {
        OffsetNumber off = *offnum++;
        ItemId lp = PageGetItemId(page, off);

        ItemIdSetUnused(lp);
    }

    /*
     * Finally, repair any fragmentation, and update the page's hint bit about
     * whether it has free pointers.
     */
    if (repairFragmentation)
        PageRepairFragmentation(page);
}
