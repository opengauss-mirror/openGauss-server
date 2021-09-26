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

void btree_desc(StringInfo buf, XLogReaderState *record)
{
    char *rec = XLogRecGetData(record);
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

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
            xl_btree_split *xlrec = (xl_btree_split *)rec;

            appendStringInfo(buf, "split left: ");
            appendStringInfo(buf, "level %u; firstright %u; new off %u", xlrec->level, (uint32)xlrec->firstright,
                             (uint32)xlrec->newitemoff);
            break;
        }
        case XLOG_BTREE_SPLIT_R: {
            xl_btree_split *xlrec = (xl_btree_split *)rec;

            appendStringInfo(buf, "split right: ");

            appendStringInfo(buf, "level %u; firstright %u; new off %u", xlrec->level, (uint32)xlrec->firstright,
                             (uint32)xlrec->newitemoff);

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
            xl_btree_vacuum *xlrec = (xl_btree_vacuum *)rec;

            appendStringInfo(buf, "vacuum: lastBlockVacuumed %u ", xlrec->lastBlockVacuumed);
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
        default:
            appendStringInfo(buf, "UNKNOWN");
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
            appendStringInfo(buf, "freeze page: blkno %u, nfrozen: %d, latestRemovedXid: %lu",
                xlrec->blkno, xlrec->nfrozen, xlrec->latestRemovedXid);
        }
        default:
            appendStringInfo(buf, "UNKNOWN");
            break;
    }
}

