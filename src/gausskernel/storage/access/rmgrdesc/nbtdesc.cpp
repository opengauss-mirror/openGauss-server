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
