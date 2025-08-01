/*
 * Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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
 * ivfvacuum.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/ivfvacuum.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/generic_xlog.h"
#include "commands/vacuum.h"
#include "access/datavec/ivfflat.h"
#include "storage/buf/bufmgr.h"

/*
 * Bulk delete tuples from the index
 */
IndexBulkDeleteResult *ivfflatbulkdelete_internal(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
                                                  IndexBulkDeleteCallback callback, void *callbackState)
{
    uint16 pqTableNblk;
    uint32 pqDisTableNblk;
    Relation index = info->index;
    IvfGetPQInfoFromMetaPage(index, &pqTableNblk, NULL, &pqDisTableNblk, NULL);
    BlockNumber blkno = IVFPQTABLE_START_BLKNO + pqTableNblk + pqDisTableNblk;
    BufferAccessStrategy bas = GetAccessStrategy(BAS_BULKREAD);

    if (stats == NULL)
        stats = (IndexBulkDeleteResult *)palloc0(sizeof(IndexBulkDeleteResult));

    /* Iterate over list pages */
    while (BlockNumberIsValid(blkno)) {
        Buffer cbuf;
        Page cpage;
        OffsetNumber coffno;
        OffsetNumber cmaxoffno;
        BlockNumber startPages[MaxOffsetNumber];
        ListInfo listInfo;

        cbuf = ReadBuffer(index, blkno);
        LockBuffer(cbuf, BUFFER_LOCK_SHARE);
        cpage = BufferGetPage(cbuf);

        cmaxoffno = PageGetMaxOffsetNumber(cpage);

        /* Iterate over lists */
        for (coffno = FirstOffsetNumber; coffno <= cmaxoffno; coffno = OffsetNumberNext(coffno)) {
            IvfflatList list = (IvfflatList)PageGetItem(cpage, PageGetItemId(cpage, coffno));

            startPages[coffno - FirstOffsetNumber] = list->startPage;
        }

        listInfo.blkno = blkno;
        blkno = IvfflatPageGetOpaque(cpage)->nextblkno;

        UnlockReleaseBuffer(cbuf);

        for (coffno = FirstOffsetNumber; coffno <= cmaxoffno; coffno = OffsetNumberNext(coffno)) {
            BlockNumber searchPage = startPages[coffno - FirstOffsetNumber];
            BlockNumber insertPage = InvalidBlockNumber;
            int delTuplePerList = 0;

            /* Iterate over entry pages */
            while (BlockNumberIsValid(searchPage)) {
                Buffer buf;
                Page page;
                GenericXLogState *state;
                OffsetNumber offno;
                OffsetNumber maxoffno;
                OffsetNumber deletable[MaxOffsetNumber];
                int ndeletable;

                vacuum_delay_point();

                buf = ReadBufferExtended(index, MAIN_FORKNUM, searchPage, RBM_NORMAL, bas);

                /*
                 * ambulkdelete cannot delete entries from pages that are
                 * pinned by other backends
                 *
                 * https://www.postgresql.org/docs/current/index-locking.html
                 */
                LockBufferForCleanup(buf);

                state = GenericXLogStart(index);
                page = GenericXLogRegisterBuffer(state, buf, 0);

                maxoffno = PageGetMaxOffsetNumber(page);
                ndeletable = 0;

                /* Find deleted tuples */
                for (offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno)) {
                    IndexTuple itup = (IndexTuple)PageGetItem(page, PageGetItemId(page, offno));
                    ItemPointer htup = &(itup->t_tid);

                    if (callback(htup, callbackState, InvalidOid, InvalidBktId)) {
                        deletable[ndeletable++] = offno;
                        stats->tuples_removed++;
                    } else
                        stats->num_index_tuples++;
                }

                /* Set to first free page */
                /* Must be set before searchPage is updated */
                if (!BlockNumberIsValid(insertPage) && ndeletable > 0)
                    insertPage = searchPage;

                searchPage = IvfflatPageGetOpaque(page)->nextblkno;

                if (ndeletable > 0) {
                    /* Delete tuples */
                    PageIndexMultiDelete(page, deletable, ndeletable);
                    GenericXLogFinish(state);
                } else
                    GenericXLogAbort(state);

                UnlockReleaseBuffer(buf);

                delTuplePerList += ndeletable;
            }

            /*
             * Update after all tuples deleted.
             *
             * We don't add or delete items from lists pages, so offset won't
             * change.
             */
            if (BlockNumberIsValid(insertPage)) {
                listInfo.offno = coffno;
                IvfflatUpdateList(index, listInfo, insertPage, InvalidBlockNumber, InvalidBlockNumber, MAIN_FORKNUM,
                    -delTuplePerList);
            }
        }
    }

    FreeAccessStrategy(bas);

    return stats;
}

/*
 * Clean up after a VACUUM operation
 */
IndexBulkDeleteResult *ivfflatvacuumcleanup_internal(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
    Relation rel = info->index;

    if (info->analyze_only)
        return stats;

    /* stats is NULL if ambulkdelete not called */
    /* OK to return NULL if index not changed */
    if (stats == NULL)
        return NULL;

    stats->num_pages = RelationGetNumberOfBlocks(rel);

    return stats;
}
