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
#include "access/datavec/vector_storage.h"
#include "storage/buf/bufmgr.h"

/*
 * Bulk delete tuples from the index
 */
IndexBulkDeleteResult *ivfflatbulkdelete_internal(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
                                                  IndexBulkDeleteCallback callback, void *callbackState)
{
    uint16 pqTableNblk;
    uint32 pqDisTableNblk;
    uint16 matrixNblk;
    uint16 otherNblk;

    Relation index = info->index;
    IvfGetPQInfoFromMetaPage(index, &pqTableNblk, NULL, &pqDisTableNblk, NULL);
    IvfflatGetRbqInfoFromMetaPage(index, NULL, NULL, NULL, NULL, &matrixNblk,
                            NULL, &otherNblk, NULL, NULL, NULL);
    BlockNumber blkno = IVFFLAT_CHUNK_START_BLKNO + pqTableNblk + pqDisTableNblk + matrixNblk + otherNblk;
    BufferAccessStrategy bas = GetAccessStrategy(BAS_BULKREAD);
    bool vectorStorage = IvfflatRelationHasVectorPayloadStorage(index, NULL, NULL);

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
                ItemPointerData retiredOwners[MaxOffsetNumber];
                int ndeletable;
                int nnewlyDead;
                int ndeadPayloads = 0;
                BlockNumber pageBlk;

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
                nnewlyDead = 0;
                pageBlk = searchPage;

                /*
                 * Keep the list tuple until the payload tid is on the free
                 * list (same as HNSW). Mark LP_DEAD first so scans skip it; a
                 * crash before VecPayloadRecycle is repaired by the next VACUUM.
                 */
                for (offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno)) {
                    ItemId itemid = PageGetItemId(page, offno);
                    IndexTuple itup;
                    ItemPointer htup;
                    bool alreadyDead;
                    bool heapDead;

                    if (!ItemIdIsUsed(itemid)) {
                        continue;
                    }

                    itup = (IndexTuple)PageGetItem(page, itemid);
                    htup = &(itup->t_tid);
                    alreadyDead = ItemIdIsDead(itemid);
                    heapDead = alreadyDead || callback(htup, callbackState, InvalidOid, InvalidBktId);
                    if (!heapDead) {
                        stats->num_index_tuples++;
                        continue;
                    }

                    if (vectorStorage) {
                        ItemPointerSet(&retiredOwners[ndeadPayloads], pageBlk, offno);
                        ndeadPayloads++;
                    }
                    deletable[ndeletable++] = offno;
                    if (!alreadyDead) {
                        ItemIdMarkDead(itemid);
                        stats->tuples_removed++;
                        nnewlyDead++;
                    }
                }

                /* Set to first free page */
                /* Must be set before searchPage is updated */
                if (!BlockNumberIsValid(insertPage) && nnewlyDead > 0)
                    insertPage = searchPage;

                searchPage = IvfflatPageGetOpaque(page)->nextblkno;

                if (nnewlyDead > 0) {
                    GenericXLogFinish(state);
                } else
                    GenericXLogAbort(state);

                UnlockReleaseBuffer(buf);

                for (int i = 0; i < ndeadPayloads; i++) {
                    VecPayloadRecycle(index, MAIN_FORKNUM, &retiredOwners[i]);
                }

                /*
                 * Payload is durable on the free list (or already was). Drop
                 * the leftover LP_DEAD tuples so the page can take inserts.
                 */
                if (ndeletable > 0) {
                    Buffer pbuf;
                    Page ppage;
                    GenericXLogState *pstate;
                    OffsetNumber stillDead[MaxOffsetNumber];
                    int nstill = 0;
                    OffsetNumber pmax;
                    int i;

                    pbuf = ReadBufferExtended(index, MAIN_FORKNUM, pageBlk, RBM_NORMAL, bas);
                    LockBufferForCleanup(pbuf);
                    pstate = GenericXLogStart(index);
                    ppage = GenericXLogRegisterBuffer(pstate, pbuf, 0);
                    pmax = PageGetMaxOffsetNumber(ppage);
                    for (i = 0; i < ndeletable; i++) {
                        OffsetNumber poff = deletable[i];
                        ItemId pitem;

                        if (poff > pmax) {
                            continue;
                        }
                        pitem = PageGetItemId(ppage, poff);
                        if (ItemIdIsUsed(pitem) && ItemIdIsDead(pitem)) {
                            stillDead[nstill++] = poff;
                        }
                    }
                    if (nstill > 0) {
                        PageIndexMultiDelete(ppage, stillDead, nstill);
                        GenericXLogFinish(pstate);
                    } else {
                        GenericXLogAbort(pstate);
                    }
                    UnlockReleaseBuffer(pbuf);
                }

                delTuplePerList += nnewlyDead;
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
