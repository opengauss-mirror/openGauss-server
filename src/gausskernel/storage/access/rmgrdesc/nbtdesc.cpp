/* -------------------------------------------------------------------------
 *
 * nbtdesc.cpp
 *	  rmgr descriptor routines for access/nbtree/nbtxlog.cpp
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/rmgrdesc/nbtdesc.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/nbtree.h"
#include "access/ubtree.h"
#include "access/ubtreepcr.h"

const char* btree_type_name(uint8 subtype)
{
    uint8 info = subtype & ~XLR_INFO_MASK;

    switch (info) {
        case XLOG_BTREE_INSERT_LEAF: {
            return "bt_insert_leaf";
            break;
        }
        case XLOG_BTREE_INSERT_UPPER: {
            return "bt_insert_upper";
            break;
        }
        case XLOG_BTREE_INSERT_META: {
            return "bt_insert_meta";
            break;
        }
        case XLOG_BTREE_SPLIT_L: {
            return "bt_split_left";
            break;
        }
        case XLOG_BTREE_SPLIT_R: {
            return "bt_split_right";
            break;
        }
        case XLOG_BTREE_SPLIT_L_ROOT: {
            return "bt_split_l_root";
            break;
        }
        case XLOG_BTREE_SPLIT_R_ROOT: {
            return "bt_split_r_root";
            break;
        }
        case XLOG_BTREE_VACUUM: {
            return "bt_vacuum";
            break;
        }
        case XLOG_BTREE_DELETE: {
            return "bt_delete";
            break;
        }
        case XLOG_BTREE_UNLINK_PAGE: {
            return "bt_unlink_page";
            break;
        }
        case XLOG_BTREE_UNLINK_PAGE_META: {
            return "bt_unlink_page_meta";
            break;
        }
        case XLOG_BTREE_MARK_PAGE_HALFDEAD: {
            return "bt_mark_page_halfdead";
            break;
        }
        case XLOG_BTREE_NEWROOT: {
            return "bt_newroot";
            break;
        }
        case XLOG_BTREE_REUSE_PAGE: {
            return "bt_reuse_page";
            break;
        }
        case XLOG_BTREE_INSERT_POST: {
            return "bt_insert_post";
            break;
        }
        case XLOG_BTREE_DEDUP: {
            return "bt_dedup";
            break;
        }
        default:
            return "unkown_type";
            break;
    }
}

void btree_desc(StringInfo buf, XLogReaderState *record)
{
    char *rec = XLogRecGetData(record);
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    bool is_dedup_upgrade = (XLogRecGetInfo(record) & BTREE_DEDUPLICATION_FLAG) != 0;

    switch (info) {
        case XLOG_BTREE_INSERT_LEAF: {
            xl_btree_insert *xlrec = (xl_btree_insert *)rec;

            appendStringInfo(buf, "insert leaf: ");
            appendStringInfo(buf, "off %u", (uint32)xlrec->offnum);
            break;
        }
        case XLOG_BTREE_INSERT_UPPER: {
            xl_btree_insert *xlrec = (xl_btree_insert *)rec;

            appendStringInfo(buf, "insert upper: ");
            appendStringInfo(buf, "off %u", (uint32)xlrec->offnum);
            break;
        }
        case XLOG_BTREE_INSERT_META: {
            xl_btree_insert *xlrec = (xl_btree_insert *)rec;
            appendStringInfo(buf, "insert meta: ");
            appendStringInfo(buf, "off %u", (uint32)xlrec->offnum);
            break;
        }
        case XLOG_BTREE_SPLIT_L: {
            if (!is_dedup_upgrade) {
                xl_btree_split *xlrec = (xl_btree_split *)rec;
                appendStringInfo(buf, "split left: ");
                appendStringInfo(buf, "level %u; firstright %u; new off %u", xlrec->level, (uint32)xlrec->firstright,
                                 (uint32)xlrec->newitemoff);
            } else {
                xl_btree_split_posting *xlrec = (xl_btree_split_posting *)rec;
                appendStringInfo(buf, "split left: ");
                appendStringInfo(buf, "level %u; firstright %u; new off %u; posting off %u", xlrec->level,
                                 (uint32)xlrec->firstright, (uint32)xlrec->newitemoff, (uint32)xlrec->posting_off);
            }
            break;
        }
        case XLOG_BTREE_SPLIT_R: {
            if (!is_dedup_upgrade) {
                xl_btree_split *xlrec = (xl_btree_split *)rec;
                appendStringInfo(buf, "split right: ");

                appendStringInfo(buf, "level %u; firstright %u; new off %u", xlrec->level, (uint32)xlrec->firstright,
                                 (uint32)xlrec->newitemoff);
            } else {
                xl_btree_split_posting *xlrec = (xl_btree_split_posting *)rec;
                appendStringInfo(buf, "split right: ");
                appendStringInfo(buf, "level %u; firstright %u; new off %u; posting off %u", xlrec->level,
                                 (uint32)xlrec->firstright, (uint32)xlrec->newitemoff, (uint32)xlrec->posting_off);
            }
            break;
        }
        case XLOG_BTREE_SPLIT_L_ROOT: {
            xl_btree_split *xlrec = (xl_btree_split *)rec;

            appendStringInfo(buf, "split left root: ");

            appendStringInfo(buf, "level %u; firstright %u; new off %u", xlrec->level, (uint32)xlrec->firstright,
                             (uint32)xlrec->newitemoff);

            break;
        }
        case XLOG_BTREE_SPLIT_R_ROOT: {
            xl_btree_split *xlrec = (xl_btree_split *)rec;

            appendStringInfo(buf, "split right root: ");
            appendStringInfo(buf, "level %u; firstright %u; new off %u", xlrec->level, (uint32)xlrec->firstright,
                             (uint32)xlrec->newitemoff);
            break;
        }
        case XLOG_BTREE_VACUUM: {
            if (!is_dedup_upgrade) {
                xl_btree_vacuum *xlrec = (xl_btree_vacuum *)rec;

                appendStringInfo(buf, "vacuum: lastBlockVacuumed %u ", xlrec->lastBlockVacuumed);
            } else {
                xl_btree_vacuum_posting *xlrec = (xl_btree_vacuum_posting *)rec;

                appendStringInfo(buf, "vacuum: lastBlockVacuumed %u; num_deleted: %hu; num_updated: %hu",
                                 xlrec->lastBlockVacuumed, xlrec->num_deleted, xlrec->num_updated);
            }
            break;
        }
        case XLOG_BTREE_DELETE: {
            xl_btree_delete *xlrec = (xl_btree_delete *)rec;
            int bucket_id = XLogRecGetBucketId(record);
            if (bucket_id == -1) {
                appendStringInfo(buf, "delete: %d items; heap %u/%u/%u", xlrec->nitems, xlrec->hnode.spcNode,
                                 xlrec->hnode.dbNode, xlrec->hnode.relNode);
            } else {
                appendStringInfo(buf, "delete: %d items; heap %u/%u/%d/%u", xlrec->nitems, xlrec->hnode.spcNode,
                                 xlrec->hnode.dbNode, bucket_id, xlrec->hnode.relNode);
            }
            break;
        }
        case XLOG_BTREE_UNLINK_PAGE:
        case XLOG_BTREE_UNLINK_PAGE_META: {
            xl_btree_unlink_page *xlrec = (xl_btree_unlink_page *)rec;

            if (info == XLOG_BTREE_UNLINK_PAGE) {
                appendStringInfo(buf, "unlink page: ");
            } else {
                appendStringInfo(buf, "unlink page meta: ");
            }
            appendStringInfo(
                buf, "leftsib %u; rightsib %u; leafleftsib %u; leafrightsib %u; topparent %u; btpo_xact " XID_FMT "",
                xlrec->leftsib, xlrec->rightsib, xlrec->leafleftsib, xlrec->leafrightsib, xlrec->topparent,
                xlrec->btpo_xact);
            break;
        }
        case XLOG_BTREE_MARK_PAGE_HALFDEAD: {
            xl_btree_mark_page_halfdead *xlrec = (xl_btree_mark_page_halfdead *)rec;

            appendStringInfo(buf, "mark page halfdead: ");
            appendStringInfo(buf, "leaf %u; left %u; right %u; parent %u; parent off %u", xlrec->leafblk,
                             xlrec->leftblk, xlrec->rightblk, xlrec->topparent, (uint32)xlrec->poffset);
            break;
        }
        case XLOG_BTREE_NEWROOT: {
            xl_btree_newroot *xlrec = (xl_btree_newroot *)rec;

            appendStringInfo(buf, "lev %u", xlrec->level);
            break;
        }
        case XLOG_BTREE_REUSE_PAGE: {
            xl_btree_reuse_page *xlrec = (xl_btree_reuse_page *)rec;
            int bucket_id = XLogRecGetBucketId(record);
            if (bucket_id == -1) {
                appendStringInfo(buf, "reuse_page: rel %u/%u/%u; latestRemovedXid " XID_FMT, xlrec->node.spcNode,
                                 xlrec->node.dbNode, xlrec->node.relNode, xlrec->latestRemovedXid);
            } else {
                appendStringInfo(buf, "reuse_page: rel %u/%u/%u/%d; latestRemovedXid " XID_FMT, xlrec->node.spcNode,
                                 xlrec->node.dbNode, xlrec->node.relNode, bucket_id, xlrec->latestRemovedXid);
            }
            break;
        }
        case XLOG_BTREE_INSERT_POST: {
            xl_btree_insert *xlrec = (xl_btree_insert *)rec;

            appendStringInfo(buf, "insert leaf posting: ");
            appendStringInfo(buf, "off %u", (uint32)xlrec->offnum);
            break;
        }
        case XLOG_BTREE_DEDUP:{
            xl_btree_dedup *xlrec = (xl_btree_dedup *) rec;

            appendStringInfo(buf, "btree dedup: ");
			appendStringInfo(buf, "num_intervals %u", xlrec->num_intervals);
			break;
        }
        default:
            appendStringInfo(buf, "UNKNOWN");
            break;
    }
}

const char* ubtree_type_name(uint8 subtype)
{
    uint8 info = subtype & ~XLR_INFO_MASK;
    switch (info) {
        case XLOG_UBTREE_INSERT_LEAF: {
            return "ubt_insert_leaf";
            break;
        }
        case XLOG_UBTREE_INSERT_UPPER: {
            return "ubt_insert_upper";
            break;
        }
        case XLOG_UBTREE_INSERT_META: {
            return "ubt_insert_meta";
            break;
        }
        case XLOG_UBTREE_SPLIT_L: {
            return "ubt_split_l";
            break;
        }
        case XLOG_UBTREE_SPLIT_R: {
            return "ubt_split_r";
            break;
        }
        case XLOG_UBTREE_SPLIT_L_ROOT: {
            return "ubt_split_l_root";
            break;
        }
        case XLOG_UBTREE_SPLIT_R_ROOT: {
            return "ubt_split_r_root";
            break;
        }
        case XLOG_UBTREE_VACUUM: {
            return "ubt_vacuum";
            break;
        }
        case XLOG_UBTREE_DELETE: {
            return "ubt_delete";
            break;
        }
        case XLOG_UBTREE_UNLINK_PAGE: {
            return "ubt_unlink_page";
            break;
        }
        case XLOG_UBTREE_UNLINK_PAGE_META: {
            return "ubt_unlink_page_meta";
            break;
        }
        case XLOG_UBTREE_MARK_PAGE_HALFDEAD: {
            return "ubt_mark_page_halfdead";
            break;
        }
        case XLOG_UBTREE_NEWROOT: {
            return "ubt_newroot";
            break;
        }
        case XLOG_UBTREE_REUSE_PAGE: {
            return "ubt_reuse_page";
            break;
        }
        case XLOG_UBTREE_MARK_DELETE: {
            return "ubt_mark_delete";
            break;
        }
        case XLOG_UBTREE_PRUNE_PAGE: {
            return "ubt_prune_page";
            break;
        }
        default:
            return "unknown_type";
            break;
    }
}


void UBTreeDesc(StringInfo buf, XLogReaderState *record)
{
    char *rec = XLogRecGetData(record);
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    switch (info) {
        case XLOG_UBTREE_INSERT_LEAF: {
            xl_btree_insert *xlrec = (xl_btree_insert *)rec;

            appendStringInfo(buf, "insert leaf: ");
            appendStringInfo(buf, "off %u", (uint32)xlrec->offnum);
            break;
        }
        case XLOG_UBTREE_INSERT_UPPER: {
            xl_btree_insert *xlrec = (xl_btree_insert *)rec;

            appendStringInfo(buf, "insert upper: ");
            appendStringInfo(buf, "off %u", (uint32)xlrec->offnum);
            break;
        }
        case XLOG_UBTREE_INSERT_META: {
            xl_btree_insert *xlrec = (xl_btree_insert *)rec;
            appendStringInfo(buf, "insert meta: ");
            appendStringInfo(buf, "off %u", (uint32)xlrec->offnum);
            break;
        }
        case XLOG_UBTREE_SPLIT_L: {
            xl_btree_split *xlrec = (xl_btree_split *)rec;

            appendStringInfo(buf, "split left: ");
            appendStringInfo(buf, "level %u; firstright %u; new off %u", xlrec->level, (uint32)xlrec->firstright,
                             (uint32)xlrec->newitemoff);
            break;
        }
        case XLOG_UBTREE_SPLIT_R: {
            xl_btree_split *xlrec = (xl_btree_split *)rec;

            appendStringInfo(buf, "split right: ");

            appendStringInfo(buf, "level %u; firstright %u; new off %u", xlrec->level, (uint32)xlrec->firstright,
                             (uint32)xlrec->newitemoff);

            break;
        }
        case XLOG_UBTREE_SPLIT_L_ROOT: {
            xl_btree_split *xlrec = (xl_btree_split *)rec;

            appendStringInfo(buf, "split left root: ");

            appendStringInfo(buf, "level %u; firstright %u; new off %u", xlrec->level, (uint32)xlrec->firstright,
                             (uint32)xlrec->newitemoff);

            break;
        }
        case XLOG_UBTREE_SPLIT_R_ROOT: {
            xl_btree_split *xlrec = (xl_btree_split *)rec;

            appendStringInfo(buf, "split right root: ");
            appendStringInfo(buf, "level %u; firstright %u; new off %u", xlrec->level, (uint32)xlrec->firstright,
                             (uint32)xlrec->newitemoff);
            break;
        }
        case XLOG_UBTREE_VACUUM: {
            xl_btree_vacuum *xlrec = (xl_btree_vacuum *)rec;

            appendStringInfo(buf, "vacuum: lastBlockVacuumed %u ", xlrec->lastBlockVacuumed);
            break;
        }
        case XLOG_UBTREE_DELETE: {
            xl_btree_delete *xlrec = (xl_btree_delete *)rec;
            int bucket_id = XLogRecGetBucketId(record);
            if (bucket_id == -1) {
                appendStringInfo(buf, "delete: %d items; heap %u/%u/%u", xlrec->nitems, xlrec->hnode.spcNode,
                                 xlrec->hnode.dbNode, xlrec->hnode.relNode);
            } else {
                appendStringInfo(buf, "delete: %d items; heap %u/%u/%d/%u", xlrec->nitems, xlrec->hnode.spcNode,
                                 xlrec->hnode.dbNode, bucket_id, xlrec->hnode.relNode);
            }
            break;
        }
        case XLOG_UBTREE_UNLINK_PAGE:
        case XLOG_UBTREE_UNLINK_PAGE_META: {
            xl_btree_unlink_page *xlrec = (xl_btree_unlink_page *)rec;

            if (info == XLOG_UBTREE_UNLINK_PAGE) {
                appendStringInfo(buf, "unlink page: ");
            } else {
                appendStringInfo(buf, "unlink page meta: ");
            }
            appendStringInfo(buf,
                "leftsib %u; rightsib %u; leafleftsib %u; leafrightsib %u; topparent %u; btpo_xact " XID_FMT "",
                xlrec->leftsib, xlrec->rightsib, xlrec->leafleftsib, xlrec->leafrightsib, xlrec->topparent,
                xlrec->btpo_xact);
            break;
        }
        case XLOG_UBTREE_MARK_PAGE_HALFDEAD: {
            xl_btree_mark_page_halfdead *xlrec = (xl_btree_mark_page_halfdead *)rec;

            appendStringInfo(buf, "mark page halfdead: ");
            appendStringInfo(buf, "leaf %u; left %u; right %u; parent %u; parent off %u", xlrec->leafblk,
                             xlrec->leftblk, xlrec->rightblk, xlrec->topparent, (uint32)xlrec->poffset);
            break;
        }
        case XLOG_UBTREE_NEWROOT: {
            xl_btree_newroot *xlrec = (xl_btree_newroot *)rec;

            appendStringInfo(buf, "lev %u", xlrec->level);
            break;
        }
        case XLOG_UBTREE_REUSE_PAGE: {
            xl_btree_reuse_page *xlrec = (xl_btree_reuse_page *)rec;
            int bucket_id = XLogRecGetBucketId(record);
            if (bucket_id == -1) {
                appendStringInfo(buf, "reuse_page: rel %u/%u/%u; latestRemovedXid " XID_FMT, xlrec->node.spcNode,
                        xlrec->node.dbNode, xlrec->node.relNode, xlrec->latestRemovedXid);
            } else {
                appendStringInfo(buf, "reuse_page: rel %u/%u/%u/%d; latestRemovedXid " XID_FMT, xlrec->node.spcNode,
                        xlrec->node.dbNode, xlrec->node.relNode, bucket_id, xlrec->latestRemovedXid);
            }
            break;
        }
        case XLOG_UBTREE_MARK_DELETE: {
            xl_ubtree_mark_delete* xlrec = (xl_ubtree_mark_delete*)rec;
            appendStringInfo(buf,
                             "mark delete: off %d xmax " XID_FMT,
                    xlrec->offset,
                    xlrec->xid);
            break;
        }
        case XLOG_UBTREE_PRUNE_PAGE: {
            xl_ubtree_prune_page* xlrec = (xl_ubtree_prune_page*)rec;

            appendStringInfo(buf, "count %d, new_prune_xid %lu, latestRemovedXid %lu", xlrec->count,
                             xlrec->new_prune_xid, xlrec->latestRemovedXid);
            break;
        }
        default:
            appendStringInfo(buf, "UNKNOWN");
            break;
    }
}

void UBTree2Desc(StringInfo buf, XLogReaderState* record)
{
    char* rec = XLogRecGetData(record);
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    switch (info) {
        case XLOG_UBTREE2_SHIFT_BASE: {
            xl_ubtree2_shift_base *xlrec = (xl_ubtree2_shift_base *)rec;
            appendStringInfo(buf, "shift base: delta %ld", xlrec->delta);
            break;
        }
        case XLOG_UBTREE2_RECYCLE_QUEUE_INIT_PAGE: {
            xl_ubtree2_recycle_queue_init_page *xlrec = (xl_ubtree2_recycle_queue_init_page *)rec;
            appendStringInfo(buf, "recycle queue init page: inserting %s, prev blkno %u, curr blkno %u next blkno %u",
                (xlrec->insertingNewPage ? "yes" : "no"), xlrec->prevBlkno, xlrec->currBlkno, xlrec->nextBlkno);
            break;
        }
        case XLOG_UBTREE2_RECYCLE_QUEUE_ENDPOINT: {
            xl_ubtree2_recycle_queue_endpoint *xlrec = (xl_ubtree2_recycle_queue_endpoint *)rec;
            appendStringInfo(buf, "recycle queue change endpoint: isHead %s, left blkno %u, right blkno %u",
                (xlrec->isHead ? "yes" : "no"), xlrec->leftBlkno, xlrec->rightBlkno);
            break;
        }
        case XLOG_UBTREE2_RECYCLE_QUEUE_MODIFY: {
            xl_ubtree2_recycle_queue_modify *xlrec = (xl_ubtree2_recycle_queue_modify *)rec;
            appendStringInfo(buf, "recycle queue modify: isInsert %s, blkno %u, offset %u",
                (xlrec->isInsert ? "yes" : "no"), xlrec->blkno, xlrec->offset);
            break;
        }
        case XLOG_UBTREE2_FREEZE: {
            xl_ubtree2_freeze *xlrec = (xl_ubtree2_freeze *)rec;
            OffsetNumber *offsets = (OffsetNumber *)(((char *)xlrec) + SizeOfUBTree2Freeze);
            appendStringInfo(buf, "freeze page: blkno %u, nfrozen: %d, latestRemovedXid: %lu",
                xlrec->blkno, xlrec->nfrozen, xlrec->oldestXmin);
            if (xlrec->nfrozen > 0) {
                appendStringInfo(buf, ", offsets info: ");
                int32 offsetIdx = 0;
                while (offsetIdx < xlrec->nfrozen) {
                    appendStringInfo(buf, "(%d : %u) ", offsetIdx, offsets[offsetIdx]);
                    offsetIdx++;
                }
            }
            break;
        }
        default:
            appendStringInfo(buf, "UNKNOWN");
            break;
    }
}

const char* ubtree2_type_name(uint8 subtype)
{
    uint8 info = subtype & ~XLR_INFO_MASK;

    switch (info) {
        case XLOG_UBTREE2_SHIFT_BASE: {
            return "ubt2_shift_base";
            break;
        }
        case XLOG_UBTREE2_RECYCLE_QUEUE_INIT_PAGE: {
            return "ubt2_recycle_init_page";
            break;
        }
        case XLOG_UBTREE2_RECYCLE_QUEUE_ENDPOINT: {
            return "ubt2_recycle_endpoint";
            break;
        }
        case XLOG_UBTREE2_RECYCLE_QUEUE_MODIFY: {
            return "ubt2_recycle_modify";
            break;
        }
        case XLOG_UBTREE2_FREEZE: {
            return "ubt2_freeze";
            break;
        }
        default:
            return "unknown_type";
            break;
    }
}


void UBTree3PrunePagePcrDesc(StringInfo buf, XLogReaderState* record)
{
    xl_ubtree3_prune_page *xlrec = (xl_ubtree3_prune_page *)XLogRecGetData(record);
    appendStringInfo(buf, "XLOG_UBTREE3_PRUNE_PAGE_PCR : latestRemovedXid: %lu, latestFrozenXid: %lu, ",
                     xlrec->latestRemovedXid, xlrec->latestFrozenXid);

    const int32 pruneTupleCount = xlrec->pruneTupleCount;
    int deadTupleOff = SizeOfUBtree3Prunepage;
    OffsetNumber *deadOffs = (OffsetNumber *)(((char *)xlrec) + deadTupleOff);

    appendStringInfo(buf, "pruneTupleCount: %d, tuple offsets: [", xlrec->activeTupleCount);
    for (int i =0; i < pruneTupleCount; i++) {
        appendStringInfo(buf, "%hu, ", deadOffs[i]);
    }
    appendStringInfo(buf, "], activeTupleCount: %d, canCompactTdCount: %hhu, ",
                     xlrec->activeTupleCount, xlrec->canCompactTdCount);
    bool* frozenTdMap = (bool*)(((char*)xlrec) + deadTupleOff + pruneTupleCount * sizeof(OffsetNumber));
    appendStringInfo(buf, "the head 4 frozen map: [");
    for (int i = 0; i < UBTREE_DEFAULT_TD_COUNT; i++) {
        appendStringInfo(buf, "%s, ", frozenTdMap[i] ? "true" : "false");
    }
    appendStringInfo(buf, "].");
}

void UBTree3FreezeTdSlotDesc(StringInfo buf, XLogReaderState* record)
{
    xl_ubtree3_freeze_td_slot *xlrec = (xl_ubtree3_freeze_td_slot *)XLogRecGetData(record);
    uint16* frozenSlots = (uint16*)(((char*)xlrec) + SizeOfUbtree3FreezeTDSlot);
    appendStringInfo(buf, "XLOG_UBTREE3_FREEZE_TD_SLOT: latestFrozenXid: %lu, nFrozen: %d, frozen slots: [",
         xlrec->latestFrozenXid, xlrec->nFrozen);
    for (uint16 i = 0; i < xlrec->nFrozen; i ++) {
        appendStringInfo(buf, "%d, ", frozenSlots[i]);
    }
    appendStringInfo(buf, "].");
}

void UBTree3ExtendTdSlotsDesc(StringInfo buf, XLogReaderState* record)
{
    xl_ubtree3_extend_td_slots *xlrec = (xl_ubtree3_extend_td_slots *)XLogRecGetData(record);
    appendStringInfo(buf, "XLOG_UBTRE3_EXTEND_TD_SLOT: nExtended: %lu, nPrevSlots: %d.",
        xlrec->nExtended, xlrec->nPrevSlots);
}

void UBTree3InsertOrDeleteSplit(StringInfo buf, char* rec, TransactionId xid)
{
    xl_ubtree3_insert_or_delete *xlrec = (xl_ubtree3_insert_or_delete *)rec;
    rec += SizeOfUbtree3InsertOrDelete;
    IndexTuple itup = (IndexTuple)rec;
    rec += IndexTupleSize(itup);
    appendStringInfo(buf, "curXid: %lu, prevXidOfTuple: %lu, offNum: %hu, tdId: %hhu",
        xlrec->curXid, xlrec->prevXidOfTuple, xlrec->offNum, xlrec->tdId);
    appendStringInfo(buf, "IndexTuple: t_tid(blk: %u, off: %hu), t_info: 0x%hx ",
        BlockIdGetBlockNumber(&itup->t_tid.ip_blkid), itup->t_tid.ip_posid, itup->t_info);
    
    XlUndoHeader *xlundohdr = (XlUndoHeader *)rec;
    rec += SizeOfXLUndoHeader;
    UBTreeUndoInfo undoInfo = (UBTreeUndoInfo)rec;
    rec += SizeOfUBTreeUndoInfoData;
    appendStringInfo(buf, "UBTreeUndoInfo: prevTdId: %hhu", undoInfo->prev_td_id);

    UndoRecPtr blkPrev = INVALID_UNDO_REC_PTR;
    UndoRecPtr prevUrp = INVALID_UNDO_REC_PTR;
    Oid partitionOid = InvalidOid ;
    if (xlundohdr->flag & UBTREE_XLOG_HAS_BLK_PREV != 0) {
        blkPrev = *((UndoRecPtr *)rec);
        rec += sizeof(UndoRecPtr);
    }
    if (xlundohdr->flag & UBTREE_XLOG_HAS_XACT_PREV != 0) {
        prevUrp = *((UndoRecPtr *)rec);
        rec += sizeof(UndoRecPtr);
    }
    if (xlundohdr->flag & UBTREE_XLOG_HAS_PARTITION_OID != 0) {
        partitionOid = *((Oid *)rec);
        rec += sizeof(Oid);
    }
    appendStringInfo(buf, "UndoHeader: flag: %hhu, %urecptr: %lu, blkPrev: %lu, prevUrp: %lu, partitionOid: %u",
        xlundohdr->flag, xlundohdr->urecptr, blkPrev, prevUrp, partitionOid);
    
    undo::XlogUndoMeta *undoMeta = (undo::XlogUndoMeta *)rec;
    Oid dbId = (undoMeta->info & XLOG_UNDOMETA_INFO_SLOT) == 0 ? undoMeta->dbid : 0;
    appendStringInfo(buf, "UndoMeta: dbId: %u, xid: %lu, lastRecordSize: %hu, info: %hhu. ",
        dbId, xid, undoMeta->lastRecordSize, undoMeta->info);
}

void UBTree3SplitDesc(StringInfo buf, XLogReaderState* record, const char *desc)
{
    char *rec = XLogRecGetData(record);
    TransactionId xid = XLogRecGetXid(record);

    xl_ubtree3_split *xlrec = (xl_ubtree3_split *)rec;
    rec += SizeOfUbtree3Split;

    appendStringInfo(buf, "%s, ", desc);
    UBTPCRPageOpaque lo = &xlrec->letfPcrOpq;
    if (lo->td_count > 0) {
        UBTree3InsertOrDeleteSplit(buf, rec, xid);
    }

    appendStringInfo(buf, "UBtree3Split: level: %u, firstRight: %lu, newItemOff: %lu, rightLower: %hu, slotNo: %hhu, "
        "fxid: %lu, urp: %lu. ",
        xlrec->level, xlrec->firstRight, xlrec->newItemOff, xlrec->rightLower, xlrec->slotNo, xlrec->fxid, xlrec->urp);
    appendStringInfo(buf, "UBtree3Split: UBTPCRPageOpaque[btpo_prev: %u, btpo_next: %u, btpo(level: %u, xact_old: %u), "
        "btpo_flags: %hu, btpo_cycleid: %hu, xact: %lu, last_delete_xid: %lu, last_commit_xid: %lu, td_count: %hhu, "
        "activeTupleCount: %hu, flags: %lu]. ", xlrec->letfPcrOpq.btpo_prev, xlrec->letfPcrOpq.btpo_next,
        xlrec->letfPcrOpq.btpo.level, xlrec->letfPcrOpq.btpo.xact_old, xlrec->letfPcrOpq.btpo_flags, 
        xlrec->letfPcrOpq.btpo_cycleid, xlrec->letfPcrOpq.xact, xlrec->letfPcrOpq.last_delete_xid,
        xlrec->letfPcrOpq.last_commit_xid, xlrec->letfPcrOpq.td_count);
    appendStringInfo(buf, "UBTPCRPageOpaque:btpo_prev: %u, btpo_next: %u, btpo(level: %u, xact_old: %u), "
        "btpo_flags: %hu, btpo_cycleid: %hu, xact: %lu, last_delete_xid: %lu, last_commit_xid: %lu, td_count: %hhu, "
        "activeTupleCount: %hu, flags: %lu.", lo->btpo_prev, lo->btpo_next,
        lo->btpo.level, lo->btpo.xact_old, lo->btpo_flags, 
        lo->btpo_cycleid, lo->xact, lo->last_delete_xid,
        lo->last_commit_xid, lo->td_count);
}

void UBTree3InsertOrDeletePcrDesc(StringInfo buf, XLogReaderState* record, const char *desc)
{
    char *rec = XLogRecGetData(record);
    TransactionId xid = XLogRecGetXid(record);
    appendStringInfo(buf, "%s, ", desc);
    UBTree3InsertOrDeleteSplit(buf, rec, xid);
}

void UBTree3InsertPcrInternalOrMetaDesc(StringInfo buf, XLogReaderState* record, bool ismeta)
{
    char *rec = XLogRecGetData(record);
    if (!ismeta) {
        xl_btree_insert *xlrec = (xl_btree_insert *)rec;
        appendStringInfo(buf, "UBtree3InsertInternal: offnum %hu", xlrec->offnum);
    } else {
        Size dataLen;
        char *metadata = XLogRecGetBlockData(record, UBTREE3_INSERT_PCR_META_BLOCK_NUM, &dataLen);
        xl_btree_metadata *xlrec = (xl_btree_metadata *)metadata;
        appendStringInfo(buf, "UBtree3InsertMeta: fastlevel %u, fastroot %u, level: %u, root: %u, version: %u",
            xlrec->fastlevel, xlrec->fastroot, xlrec->level, xlrec->root, xlrec->version);
    }
}

void UBTree3NewRootDesc(StringInfo buf, XLogReaderState* record)
{
    char *rec = XLogRecGetData(record);
    xl_btree_newroot *xlrecroot = (xl_btree_newroot *)rec;
    appendStringInfo(buf, "UBtree3NewRoot: rootblk %u, level %u", xlrecroot->rootblk, xlrecroot->level);
    Size dataLen;
    char *matadata = XLogRecGetBlockData(record, BTREE_NEWROOT_META_BLOCK_NUM, &dataLen);
    xl_btree_metadata *xlrecmeta = (xl_btree_metadata *)matadata;
    appendStringInfo(buf, "UBtree3InsertMeta: fastlevel %u, fastroot %u, level: %u, root: %u, version: %u",
        xlrecmeta->fastlevel, xlrecmeta->fastroot, xlrecmeta->level, xlrecmeta->root, xlrecmeta->version);
}

void UBTree3RollbackTxnDesc(StringInfo buf, XLogReaderState* record)
{
    xl_ubtree3_rollback_txn *xlrec = (xl_ubtree3_rollback_txn *)XLogRecGetData(record);
    UBTreeTD xltd = (UBTreeTD)(((char *)xlrec) + sizeOfUbtree3RollbackTxn);
    appendStringInfo(buf, "XLOG_UBTREE3_ROLLBACK_PCR: xid %lu, td_id %u, td(xid: %lu, urecptr:%lu, status:%d), "
        "n_rollback %u", xlrec->xid, xlrec->td_id, xltd->xactid, xltd->undoRecPtr, xltd->tdStatus, xlrec->n_rollback);
    if (xlrec->n_rollback > 0) {
        appendStringInfo(buf, ", rollback_items:(");
        UBTreeRedoRollbackItem items = 
            (UBTreeRedoRollbackItem)(((char *)xlrec) + sizeOfUbtree3RollbackTxn + sizeof(UBTreeTDData));
        for (int i = 0; i < xlrec->n_rollback; i++) {
            appendStringInfo(buf, "[off:%u, lp_off:%u, lp_flag:%u, lp_len:%u, "
                "tdSlot:%u, slotIsInvalid:%u, isDeleted:%u]", 
                items[i].offnum, items[i].iid.lp_off, items[i].iid.lp_flags, items[i].iid.lp_len,
                items[i].trx.tdSlot, items[i].trx.slotIsInvalid, items[i].trx.isDeleted);
            if (i != xlrec->n_rollback - 1) {
                appendStringInfo(buf, ",");
            }
        }
        appendStringInfo(buf, ")");
    }
}

void UBTree3Desc(StringInfo buf, XLogReaderState* record)
{
   uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
   info &= XLOG_UBTREE_PCR_OP_MASK;

   switch (info) {
    case XLOG_UBTREE3_INSERT_PCR_INTERNAL: {
        UBTree3InsertPcrInternalOrMetaDesc(buf, record, false);
        break;
    }
    case XLOG_UBTREE3_PRUNE_PAGE_PCR: {
        UBTree3PrunePagePcrDesc(buf, record);
        break;
    }
    case XLOG_UBTREE3_ROLLBACK_TXN: {
        UBTree3RollbackTxnDesc(buf, record);
        break;
    }
    case XLOG_UBTREE3_FREEZE_TD_SLOT: {
        UBTree3FreezeTdSlotDesc(buf, record);
        break;
    }
    case XLOG_UBTREE3_EXTEND_TD_SLOTS: {
        UBTree3ExtendTdSlotsDesc(buf, record);
        break;
    }
    case XLOG_UBTREE3_SPLIT_L: {
        UBTree3SplitDesc(buf, record, "XLOG_UBTREE3_SPLIT_L");
        break;
    }
    case XLOG_UBTREE3_SPLIT_R: {
        UBTree3SplitDesc(buf, record, "XLOG_UBTREE3_SPLIT_R");
        break;
    }
    case XLOG_UBTREE3_SPLIT_L_ROOT: {
        UBTree3SplitDesc(buf, record, "XLOG_UBTREE3_SPLIT_L_ROOT");
        break;
    }
    case XLOG_UBTREE3_SPLIT_R_ROOT: {
        UBTree3SplitDesc(buf, record, "XLOG_UBTREE3_SPLIT_R_ROOT");
        break;
    }
    case XLOG_UBTREE3_INSERT_PCR: {
        UBTree3InsertOrDeletePcrDesc(buf, record, "XLOG_UBTREE3_INSERT_PCR");
        break;
    }
    case XLOG_UBTREE3_DUP_INSERT: {
        UBTree3InsertOrDeletePcrDesc(buf, record, "XLOG_UBTREE3_DUP_INSERT");
        break;
    }
    case XLOG_UBTREE3_DELETE_PCR: {
        UBTree3InsertOrDeletePcrDesc(buf, record, "XLOG_UBTREE3_DELETE_PCR");
        break;
    }
    case XLOG_UBTREE3_INSERT_PCR_META: {
        UBTree3InsertPcrInternalOrMetaDesc(buf, record, true);
        break;
    }
    case XLOG_UBTREE3_NEW_ROOT: {
        UBTree3NewRootDesc(buf, record);
        break;
    }
    /*
    case XLOG_UBTREE3_REUSE_TD_SLOT: {
        UBTree3ReuseTdSlotDesc(buf, record);
        break;
    }
    */
    default:
        appendStringInfo(buf, "UBTree3Redo: unknown op code %hhu", info);
        break;
   }
   return;
}



const char* ubtree3_type_name(uint8 subtype)
{
    uint8 info = subtype & ~XLR_INFO_MASK;
    info &= XLOG_UBTREE_PCR_OP_MASK;

    switch (info) {
        case XLOG_UBTREE3_INSERT_PCR_INTERNAL: {
            return "ubt3_insert_pcr_internal";
            break;
        }
        case XLOG_UBTREE3_PRUNE_PAGE_PCR: {
            return "ubt3_prune_page_pcr";
            break;
        }
        case XLOG_UBTREE3_DELETE_PCR: {
            return "ubt3_delete_pcr";
            break;
        }
        case XLOG_UBTREE3_INSERT_PCR: {
            return "ubt3_insert_pcr";
            break;
        }
        case XLOG_UBTREE3_DUP_INSERT: {
            return "ubt3_duplicate_pcr";
            break;
        }
        case XLOG_UBTREE3_ROLLBACK_TXN: {
            return "ubt3_rollback_transaction";
            break;
        }
        case XLOG_UBTREE3_FREEZE_TD_SLOT: {
            return "ubt3_freeze_td_slot";
            break;
        }
        case XLOG_UBTREE3_REUSE_TD_SLOT: {
            return "ubt3_reuse_td_slot";
            break;
        }
        case XLOG_UBTREE3_EXTEND_TD_SLOTS: {
            return "ubt3_extend_td_slots";
            break;
        }
        case XLOG_UBTREE3_SPLIT_L: {
            return "ubt3_split_left";
            break;
        }
        case XLOG_UBTREE3_SPLIT_R: {
            return "ubt3_split_right";
            break;
        }
        case XLOG_UBTREE3_SPLIT_L_ROOT: {
            return "ubt3_split_root_left";
            break;
        }
        case XLOG_UBTREE3_SPLIT_R_ROOT: {
            return "ubt3_split_root_right";
            break;
        }
        case XLOG_UBTREE3_NEW_ROOT: {
            return "ubt3_new_root";
            break;
        }
        case XLOG_UBTREE3_INSERT_PCR_META: {
            return "ubt3_insert_pcr_meta";
            break;
        }
        default:
            return "unknown_type";
            break;
    }
}

static void UBTree4UnlinkPageDesc(StringInfo buf, XLogReaderState* record, bool isMeta)
{
    char *rec = XLogRecGetData(record);
    xl_btree_unlink_page *xlrec = (xl_btree_unlink_page *)rec;
    if (isMeta) {
        appendStringInfo(buf, "XLOG_UBTREE4_UNLINK_PAGE_META: ");
        Size metaDataLen;
        char *metaData = XLogRecGetBlockData(record, BTREE_UNLINK_PAGE_META_NUM, &metaDataLen);
        xl_btree_metadata *metaRec = (xl_btree_metadata *)metaData;
        appendStringInfo(buf, "meta info: [root blk no: %u, level %u, fast root blk no: %u, fast level: %u, version: %u]",
            metaRec->root, metaRec->level, metaRec->fastroot, metaRec->fastlevel, metaRec->version);
    } else {
        appendStringInfo(buf, "XLOG_UBTREE4_UNLINK_PAGE: ");
    }

    appendStringInfo(buf,
                "leftsib %u; rightsib %u; leafleftsib %u; leafrightsib %u; topparent %u; btpo_xact " XID_FMT "",
                xlrec->leftsib, xlrec->rightsib, xlrec->leafleftsib, xlrec->leafrightsib, xlrec->topparent,
                xlrec->btpo_xact);
}

static void UBTree4MarkPageHalfDeadDesc(StringInfo buf, XLogReaderState* record)
{
    char *rec = XLogRecGetData(record);
    xl_btree_mark_page_halfdead *xlrec = (xl_btree_mark_page_halfdead *)rec;
    appendStringInfo(buf, "XLOG_UBTREE4_MARK_PAGE_HALFDEAD: ");
    appendStringInfo(buf, "leaf %u; left %u; right %u; parent %u; parent off %u", xlrec->leafblk,
                             xlrec->leftblk, xlrec->rightblk, xlrec->topparent, (uint32)xlrec->poffset);
}

void UBTree4Desc(StringInfo buf, XLogReaderState* record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    info &= XLOG_UBTREE_PCR_OP_MASK;

    switch (info) {
        case XLOG_UBTREE4_UNLINK_PAGE: {
            UBTree4UnlinkPageDesc(buf, record, false);
            break;
        }
        case XLOG_UBTREE4_UNLINK_PAGE_META: {
            UBTree4UnlinkPageDesc(buf, record, true);
            break;
        }
        case XLOG_UBTREE4_MARK_PAGE_HALFDEAD: {
            UBTree4MarkPageHalfDeadDesc(buf, record);
            break;
        }
        default:
        appendStringInfo(buf, "UBTree4Redo: unknown op code %hhu", info);
        break;
   }
   return;
}

const char* ubtree4_type_name(uint8 subtype)
{
    uint8 info = subtype & ~XLR_INFO_MASK;
    info &= XLOG_UBTREE_PCR_OP_MASK;

    switch (info) {
        case XLOG_UBTREE4_UNLINK_PAGE: {
            return "ubt4_unlink_page";
            break;
        }
        case XLOG_UBTREE4_UNLINK_PAGE_META: {
            return "ubt4_unlink_page_meta";
            break;
        }
        case XLOG_UBTREE4_MARK_PAGE_HALFDEAD: {
            return "ubt4_mark_page_halfdead";
            break;
        }
        default:
            return "unknown_type";
            break;
    }
}