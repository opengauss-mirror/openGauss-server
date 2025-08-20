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
 * blvacuum.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/bloom/blvacuum.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/generic_xlog.h"
#include "access/genam.h"
#include "catalog/storage.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "postmaster/autovacuum.h"
#include "storage/buf/bufmgr.h"
#include "storage/indexfsm.h"
#include "storage/lmgr.h"

#include "access/bloom.h"

/*
 * Bulk deletion of all index entries pointing to a set of heap tuples.
 * The set of target tuples is specified via a callback routine that tells
 * whether any given heap tuple (identified by ItemPointer) is being deleted.
 *
 * Result: a palloc'd struct containing statistical info for VACUUM displays.
 */
IndexBulkDeleteResult *blbulkdelete_internal(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
    IndexBulkDeleteCallback callback, const void *callback_state)
{
    Relation    index = info->index;
    BlockNumber blkno;
    BlockNumber npages;
    FreeBlockNumberArray notFullPage;
    int            countPage = 0;
    BloomState    state;
    Buffer        buffer;
    Page        page;
    BloomMetaPageData *metaData;
    GenericXLogState *gxlogState;
    errno_t     rc;

    if (stats == NULL)
        stats = (IndexBulkDeleteResult *)palloc0(sizeof(IndexBulkDeleteResult));

    initBloomState(&state, index);

    /*
     * Interate over the pages. We don't care about concurrently added pages,
     * they can't contain tuples to delete.
     */
    npages = RelationGetNumberOfBlocks(index);
    for (blkno = BLOOM_HEAD_BLKNO; blkno < npages; blkno++) {
        BloomTuple *itup;
        BloomTuple *itupPtr;
        BloomTuple *itupEnd;

        vacuum_delay_point();

        buffer = ReadBufferExtended(index, MAIN_FORKNUM, blkno,
                                    RBM_NORMAL, info->strategy);
        LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
        gxlogState = GenericXLogStart(index);
        page = GenericXLogRegisterBuffer(gxlogState, buffer, 0);
        /* Ignore empty/deleted pages until blvacuumcleanup() */
        if (PageIsNew(page) || BloomPageIsDeleted(page)) {
            UnlockReleaseBuffer(buffer);
            GenericXLogAbort(gxlogState);
            continue;
        }

        /*
         * Iterate over the tuples.  itup points to current tuple being
         * scanned, itupPtr points to where to save next non-deleted tuple.
         */
        itup = itupPtr = BloomPageGetTuple(&state, page, FirstOffsetNumber);
        itupEnd = BloomPageGetTuple(&state, page,
                                    OffsetNumberNext(BloomPageGetMaxOffset(page)));
        while (itup < itupEnd) {
            /* Do we have to delete this tuple? */
            if (callback(&itup->heapPtr, (void *)callback_state, 0, 0)) {
                /* Yes; adjust count of tuples that will be left on page */
                stats->tuples_removed += 1;
                BloomPageGetOpaque(page)->maxoff--;
            } else {
                if (itupPtr != itup) {
                    /*
                     * If we already delete something before, we have to move
                     * this tuple backward.
                     */
                    rc = memmove_s((Pointer) itupPtr, state.sizeOfBloomTuple,
                        (Pointer) itup, state.sizeOfBloomTuple);
                    securec_check_c(rc, "\0", "\0");
                }
                stats->num_index_tuples++;
                itupPtr = BloomPageGetNextTuple(&state, itupPtr);
            }

            itup = BloomPageGetNextTuple(&state, itup);
        }

        Assert(itupPtr == BloomPageGetTuple(&state, page,
                                            OffsetNumberNext(BloomPageGetMaxOffset(page))));

        /*
         * Add page to notFullPage list if we will not mark page as deleted and
         * there is a free space on it
         */
        if (BloomPageGetMaxOffset(page) != 0 &&
            BloomPageGetFreeSpace(&state, page) >= state.sizeOfBloomTuple &&
            countPage < BLOOM_META_BLOCK_N)
            notFullPage[countPage++] = blkno;

        /* Did we delete something? */
        if (itupPtr != itup) {
            /* Is it empty page now? */
            if (BloomPageGetMaxOffset(page) == 0)
                BloomPageSetDeleted(page);
            /* Adjust pg_lower */
            ((PageHeader) page)->pd_lower = (Pointer) itupPtr - page;
            /* Finish WAL-logging */
            GenericXLogFinish(gxlogState);
        } else {
            /* Didn't change anything: abort WAL-logging */
            GenericXLogAbort(gxlogState);
        }
        UnlockReleaseBuffer(buffer);
    }

    /*
     * Update the metapage's notFullPage list with whatever we found.  Our
     * info could already be out of date at this point, but blinsert() will
     * cope if so.
     */
    if (countPage > 0) {
        BloomMetaPageData *metaData;

        buffer = ReadBuffer(index, BLOOM_METAPAGE_BLKNO);
        LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

        gxlogState = GenericXLogStart(index);
        page = GenericXLogRegisterBuffer(gxlogState, buffer, 0);

        metaData = BloomPageGetMeta(page);
        rc = memcpy_s(metaData->notFullPage, sizeof(BlockNumber) * countPage,
            notFullPage, sizeof(BlockNumber) * countPage);
        securec_check(rc, "\0", "\0");
        metaData->nStart = 0;
        metaData->nEnd = countPage;

        GenericXLogFinish(gxlogState);
        UnlockReleaseBuffer(buffer);
    }

    return stats;
}

/*
 * Post-VACUUM cleanup.
 *
 * Result: a palloc'd struct containing statistical info for VACUUM displays.
 */
IndexBulkDeleteResult *blvacuumcleanup_internal(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
    Relation    index = info->index;
    BlockNumber npages;
    BlockNumber blkno;

    if (info->analyze_only)
        return stats;

    if (stats == NULL)
        stats = (IndexBulkDeleteResult *)palloc0(sizeof(IndexBulkDeleteResult));

    /*
     * Iterate over the pages: insert deleted pages into FSM and collect
     * statistics.
     */
    npages = RelationGetNumberOfBlocks(index);
    stats->num_pages = npages;
    stats->pages_free = 0;
    stats->num_index_tuples = 0;
    for (blkno = BLOOM_HEAD_BLKNO; blkno < npages; blkno++) {
        Buffer      buffer;
        Page        page;

        vacuum_delay_point();

        buffer = ReadBufferExtended(index, MAIN_FORKNUM, blkno,
                                    RBM_NORMAL, info->strategy);
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        page = (Page) BufferGetPage(buffer);
        if (PageIsNew(page) || BloomPageIsDeleted(page)) {
            RecordFreeIndexPage(index, blkno);
            stats->pages_free++;
        } else {
            stats->num_index_tuples += BloomPageGetMaxOffset(page);
        }

        UnlockReleaseBuffer(buffer);
    }

    IndexFreeSpaceMapVacuum(info->index);

    return stats;
}
