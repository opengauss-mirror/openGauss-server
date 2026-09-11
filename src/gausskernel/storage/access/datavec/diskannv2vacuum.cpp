/*
 * Copyright (c) 2026 Huawei Technologies Co.,Ltd.
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
 * diskannv2vacuum.cpp
 *
 *        DiskANN RaBitQ format (version 2) VACUUM (ambulkdelete). DELETE /
 *        UPDATE never touch the index; graph pages are swept one exclusive
 *        page lock at a time, dead TIDs dropped and the slot's list compacted,
 *        WAL-logged only when the page changed. A node left without TIDs is a
 *        tombstone that keeps routing traversals; REINDEX reclaims slots.
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/diskannv2vacuum.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/generic_xlog.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "storage/buf/bufmgr.h"
#include "utils/rel.h"
#include "access/datavec/diskannv2.h"

/* one bulkdelete sweep */
typedef struct DiskAnnV2VacCtx {
    const DiskAnnV2Meta* meta;
    IndexBulkDeleteCallback callback;
    void* callbackState;
    double removed;
    double remaining;
} DiskAnnV2VacCtx;

/* graph slots [firstSlot, firstSlot + slotsHere) of one page */
typedef struct DiskAnnV2VacPage {
    Page page;
    uint32 firstSlot;
    uint32 slotsHere;
} DiskAnnV2VacPage;

static inline DiskAnnV2GraphSlot* GraphSlotAt(Page page, const DiskAnnV2Meta* meta, uint32 slotNo)
{
    return (DiskAnnV2GraphSlot*)((char*)page + DiskAnnV2SlotOffset(meta->graphSlotSize, slotNo));
}

/* does any TID on this page have to go? */
static bool VacPageHasDead(const DiskAnnV2VacCtx* vc, const DiskAnnV2VacPage* vp)
{
    for (uint32 s = 0; s < vp->slotsHere; s++) {
        const DiskAnnV2GraphSlot* slot = GraphSlotAt(vp->page, vc->meta, vp->firstSlot + s);
        int tids = Min((int)slot->tidCount, DISKANN_HEAPTIDS);
        for (int t = 0; t < tids; t++) {
            if (vc->callback(const_cast<ItemPointerData*>(&slot->heaptids[t]), vc->callbackState, InvalidOid,
                             InvalidBktId)) {
                return true;
            }
        }
    }
    return false;
}

/* drop the dead TIDs of every slot on a registered page and compact the lists */
static void VacCompactPage(DiskAnnV2VacCtx* vc, const DiskAnnV2VacPage* vp)
{
    for (uint32 s = 0; s < vp->slotsHere; s++) {
        DiskAnnV2GraphSlot* slot = GraphSlotAt(vp->page, vc->meta, vp->firstSlot + s);
        int tids = Min((int)slot->tidCount, DISKANN_HEAPTIDS);
        uint8 keep = 0;
        for (int t = 0; t < tids; t++) {
            if (vc->callback(&slot->heaptids[t], vc->callbackState, InvalidOid, InvalidBktId)) {
                vc->removed += 1;
                continue;
            }
            slot->heaptids[keep] = slot->heaptids[t];
            keep++;
        }
        for (int t = keep; t < tids; t++) {
            ItemPointerSetInvalid(&slot->heaptids[t]);
        }
        slot->tidCount = keep;
        vc->remaining += keep;
    }
}

IndexBulkDeleteResult* DiskAnnV2BulkDelete(IndexVacuumInfo* info, IndexBulkDeleteResult* stats,
                                           IndexBulkDeleteCallback callback, void* callbackState)
{
    Relation index = info->index;
    if (stats == NULL) {
        stats = (IndexBulkDeleteResult*)palloc0(sizeof(IndexBulkDeleteResult));
    }

    DiskAnnV2Meta meta;
    DiskAnnV2GetMetaSnapshot(index, &meta);
    DiskAnnV2VacCtx vc;
    vc.meta = &meta;
    vc.callback = callback;
    vc.callbackState = callbackState;
    vc.removed = 0;
    vc.remaining = 0;

    /* nodes allocated but not yet addressable (a concurrent INSERT is still growing the tail) are skipped */
    uint32 n = (uint32)Min((uint64)meta.nextNodeId, DiskAnnV2NodeCapacity(&meta));
    uint32 nodeId = 0;
    while (nodeId < n) {
        BlockNumber blkno;
        uint16 firstSlot;
        DiskAnnV2ResolveGraphSlot(&meta, nodeId, &blkno, &firstSlot);
        DiskAnnV2VacPage vp;
        vp.firstSlot = firstSlot;
        vp.slotsHere = Min(DiskAnnV2GraphSlotsOnPage(&meta, nodeId), n - nodeId);

        Buffer buf = ReadBuffer(index, blkno);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        vp.page = BufferGetPage(buf);
        if (VacPageHasDead(&vc, &vp)) {
            GenericXLogState* state = GenericXLogStart(index);
            vp.page = GenericXLogRegisterBuffer(state, buf, 0);
            VacCompactPage(&vc, &vp);
            GenericXLogFinish(state);
        } else {
            /* nothing dead here: just count the live TIDs */
            for (uint32 s = 0; s < vp.slotsHere; s++) {
                vc.remaining += GraphSlotAt(vp.page, &meta, vp.firstSlot + s)->tidCount;
            }
        }
        UnlockReleaseBuffer(buf);

        nodeId += vp.slotsHere;
        vacuum_delay_point();
    }

    stats->tuples_removed += vc.removed;
    stats->num_index_tuples = vc.remaining;
    stats->estimated_count = false;
    return stats;
}
