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

