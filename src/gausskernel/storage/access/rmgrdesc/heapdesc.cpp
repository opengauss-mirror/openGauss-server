/* -------------------------------------------------------------------------
 *
 * heapdesc.cpp
 *   rmgr descriptor routines for access/heap/heapam.cpp
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/access/rmgrdesc/heapdesc.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/htup.h"
#include "access/xlog.h"


void heap_add_lock_info(StringInfo buf, xl_heap_lock *xlrec)
{
    if (xlrec->shared_lock) {
        appendStringInfo(buf, "shared_lock: ");
    } else {
        appendStringInfo(buf, "exclusive_lock: ");
    }
    if (xlrec->xid_is_mxact) {
        appendStringInfo(buf, "mxid ");
    } else {
        appendStringInfo(buf, "xid ");
    }
    appendStringInfo(buf, "%lu ", xlrec->locking_xid);
    appendStringInfo(buf, "off %u XLOG_HEAP_LOCK", (uint32)xlrec->offnum);
}


static void OutInfobits(StringInfo buf, uint8 infobits)
{
    if (infobits & XLHL_XMAX_IS_MULTI) {
        appendStringInfo(buf, "IS_MULTI ");
    }
    if (infobits & XLHL_XMAX_LOCK_ONLY) {
        appendStringInfo(buf, "LOCK_ONLY ");
    }
    if (infobits & XLHL_XMAX_EXCL_LOCK) {
        appendStringInfo(buf, "EXCL_LOCK ");
    }
    if (infobits & XLHL_XMAX_KEYSHR_LOCK) {
        appendStringInfo(buf, "KEYSHR_LOCK ");
    }
    if (infobits & XLHL_KEYS_UPDATED) {
        appendStringInfo(buf, "KEYS_UPDATED ");
    }
}

void heap3_new_cid(StringInfo buf, int bucket_id, xl_heap_new_cid *xlrec)
{
    if (bucket_id == -1) {
        appendStringInfo(buf, "rel %u/%u/%u; tid %u/%hu", xlrec->target_node.spcNode, xlrec->target_node.dbNode,
                         xlrec->target_node.relNode, ItemPointerGetBlockNumber(&(xlrec->target_tid)),
                         ItemPointerGetOffsetNumber(&(xlrec->target_tid)));
    } else {
        appendStringInfo(buf, "rel %u/%u/%u/%d; tid %u/%hu", xlrec->target_node.spcNode, xlrec->target_node.dbNode,
                         xlrec->target_node.relNode, bucket_id, ItemPointerGetBlockNumber(&(xlrec->target_tid)),
                         ItemPointerGetOffsetNumber(&(xlrec->target_tid)));
    }

    appendStringInfo(buf, "; cmin: %u, cmax: %u, combo: %u", xlrec->cmin, xlrec->cmax, xlrec->combocid);
}

const char* heap_type_name(uint8 subtype)
{
    uint8 info = subtype & ~XLR_INFO_MASK;
    info &= XLOG_HEAP_OPMASK;
    if (info == XLOG_HEAP_INSERT) {
        return "heap_insert";
    } else if (info == XLOG_HEAP_DELETE) {
        return "heap_delete";
    } else if (info == XLOG_HEAP_UPDATE) {
        return "heap_update";
    } else if (info == XLOG_HEAP_HOT_UPDATE) {
        return "heap_hot_update";
    } else if (info == XLOG_HEAP_NEWPAGE) {
        return "heap_newpage";
    } else if (info == XLOG_HEAP_LOCK) {
        return "heap_lock";
    } else if (info == XLOG_HEAP_INPLACE) {
        return "heap_inplace";
    } else if (info == XLOG_HEAP_BASE_SHIFT) {
        return "base_shift";
    }
    return "unkown_type";
}

void heap_desc(StringInfo buf, XLogReaderState *record)
{
    char *rec = XLogRecGetData(record);
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    bool isTupleLockUPgrade = (XLogRecGetInfo(record) & XLOG_TUPLE_LOCK_UPGRADE_FLAG) != 0;

    info &= XLOG_HEAP_OPMASK;
    if (info == XLOG_HEAP_INSERT) {
        xl_heap_insert *xlrec = (xl_heap_insert *)rec;

        if (XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE)
            appendStringInfo(buf, "XLOG_HEAP_INSERT insert(init): ");
        else
            appendStringInfo(buf, "XLOG_HEAP_INSERT insert: ");
        appendStringInfo(buf, "off %u", (uint32)xlrec->offnum);
    } else if (info == XLOG_HEAP_DELETE) {
        xl_heap_delete *xlrec = (xl_heap_delete *)rec;

        appendStringInfo(buf, "delete: ");
        appendStringInfo(buf, "off %u", (uint32)xlrec->offnum);
        if (isTupleLockUPgrade) {
            appendStringInfoChar(buf, ' ');
            OutInfobits(buf, xlrec->infobits_set);
        }
    } else if (info == XLOG_HEAP_UPDATE) {
        xl_heap_update *xlrec = (xl_heap_update *)rec;

        if (XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE)
            appendStringInfo(buf, "XLOG_HEAP_UPDATE update(init): ");
        else
            appendStringInfo(buf, "XLOG_HEAP_UPDATE update: ");
        appendStringInfo(buf, "off %u new off %u", (uint32)xlrec->old_offnum, (uint32)xlrec->new_offnum);
        if (isTupleLockUPgrade) {
            appendStringInfoChar(buf, ' ');
            OutInfobits(buf, xlrec->old_infobits_set);
        }
    } else if (info == XLOG_HEAP_HOT_UPDATE) {
        xl_heap_update *xlrec = (xl_heap_update *)rec;

        if (XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE) /* can this case happen? */
            appendStringInfo(buf, "XLOG_HEAP_HOT_UPDATE hot_update(init): ");
        else
            appendStringInfo(buf, "XLOG_HEAP_HOT_UPDATE hot_update: ");
        appendStringInfo(buf, "off %u new off %u", (uint32)xlrec->old_offnum, (uint32)xlrec->new_offnum);
        if (isTupleLockUPgrade) {
            appendStringInfoChar(buf, ' ');
            OutInfobits(buf, xlrec->old_infobits_set);
        }
    } else if (info == XLOG_HEAP_NEWPAGE) {
        appendStringInfo(buf, "new page");
        /* no further information */
    } else if (info == XLOG_HEAP_LOCK) {
        xl_heap_lock *xlrec = (xl_heap_lock *)rec;

        heap_add_lock_info(buf, xlrec);
        if (isTupleLockUPgrade) {
            appendStringInfoChar(buf, ' ');
            OutInfobits(buf, xlrec->infobits_set);
        }
    } else if (info == XLOG_HEAP_INPLACE) {
        xl_heap_inplace *xlrec = (xl_heap_inplace *)rec;

        appendStringInfo(buf, "inplace: ");
        appendStringInfo(buf, "off %u", (uint32)xlrec->offnum);
    } else if (info == XLOG_HEAP3_NEW_CID) {
        xl_heap_new_cid *xlrec = (xl_heap_new_cid *)rec;
        int bucket_id = XLogRecGetBucketId(record);
        heap3_new_cid(buf, bucket_id, xlrec);
    } else if (info == XLOG_HEAP_BASE_SHIFT) {
        xl_heap_base_shift *xlrec = (xl_heap_base_shift *)rec;

        appendStringInfo(buf, "base_shift delta %ld multi %d", xlrec->delta, xlrec->multi);
    } else
        appendStringInfo(buf, "UNKNOWN");
}

const char* heap2_type_name(uint8 subtype)
{
    uint8 info = subtype & ~XLR_INFO_MASK;
    info &= XLOG_HEAP_OPMASK;
    if (info == XLOG_HEAP2_FREEZE) {
        return "heap2_freeze";
    } else if (info == XLOG_HEAP2_CLEAN) {
        return "heap2_clean";
    } else if (info == XLOG_HEAP2_PAGE_UPGRADE) {
        return "heap2_page_udpate";  // not used
    } else if (info == XLOG_HEAP2_CLEANUP_INFO) {
        return "heap2_cleanup_info";
    } else if (info == XLOG_HEAP2_VISIBLE) {
        return "heap2_visible";
    } else if (info == XLOG_HEAP2_BCM) {
        return "heap2_bcm";
    } else if (info == XLOG_HEAP2_MULTI_INSERT) {
        return "heap2_multi_insert";
    } else if (info == XLOG_HEAP2_LOGICAL_NEWPAGE) {
        return "heap2_logical_newpage";
    } else {
        return "unkown_type";
    }
}

void heap2_desc(StringInfo buf, XLogReaderState *record)
{
    char *rec = XLogRecGetData(record);
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    info &= XLOG_HEAP_OPMASK;
    if (info == XLOG_HEAP2_FREEZE) {
        xl_heap_freeze *xlrec = (xl_heap_freeze *)rec;

        appendStringInfo(buf, "freeze: cutoff xid %lu", xlrec->cutoff_xid);

        if (!XLogRecHasBlockImage(record, 0)) {
            Size datalen;
            OffsetNumber *offsets = NULL;
            OffsetNumber *offsets_end = NULL;

            offsets = (OffsetNumber *)XLogRecGetBlockData(record, 0, &datalen);

            if (datalen > 0) {
                offsets_end = (OffsetNumber *)((char *)offsets + datalen);

                appendStringInfo(buf, " offsets: [");
                while (offsets < offsets_end) {
                    appendStringInfo(buf, " %d ", *offsets);
                    offsets++;
                }
                appendStringInfo(buf, "]");
            }
        }
    } else if (info == XLOG_HEAP2_CLEAN) {
        xl_heap_clean *xlrec = (xl_heap_clean *)rec;

        appendStringInfo(buf, "clean: remxid %lu", xlrec->latestRemovedXid);

        if (!XLogRecHasBlockImage(record, 0)) {
            OffsetNumber *end = NULL;
            OffsetNumber *redirected = NULL;
            OffsetNumber *nowdead = NULL;
            OffsetNumber *nowunused = NULL;
            int nredirected;
            int ndead;
            int nunused;
            Size datalen;
            int i = 0;

            redirected = (OffsetNumber *)XLogRecGetBlockData(record, 0, &datalen);

            nredirected = xlrec->nredirected;
            ndead = xlrec->ndead;
            end = (OffsetNumber *)((char *)redirected + datalen);
            nowdead = redirected + (nredirected * 2);
            nowunused = nowdead + ndead;
            nunused = (end - nowunused);
            if ((XLogRecGetInfo(record) & XLOG_HEAP2_NO_REPAIR_PAGE) != 0) {
                appendStringInfo(buf, "don't need PageRepairFragmentation;");
            }

            if (nredirected > 0) {
                appendStringInfo(buf, " redirected: [");
                while (i < nredirected) {
                    appendStringInfo(buf, " %d ", redirected[i]);
                    i++;
                }
                appendStringInfo(buf, "]");
            }

            if (ndead > 0) {
                i = 0;
                appendStringInfo(buf, " dead: [");
                while (i < ndead) {
                    appendStringInfo(buf, " %d ", nowdead[i]);
                    i++;
                }
                appendStringInfo(buf, "]");
            }

            if (nunused > 0) {
                i = 0;
                appendStringInfo(buf, " unused: [");
                while (i < nunused) {
                    appendStringInfo(buf, " %d ", nowunused[i]);
                    i++;
                }
                appendStringInfo(buf, "]");
            }
        }
    } else if (info == XLOG_HEAP3_REWRITE) {
        appendStringInfoString(buf, "heap rewrite:");
    } else if (info == XLOG_HEAP2_CLEANUP_INFO) {
        xl_heap_cleanup_info *xlrec = (xl_heap_cleanup_info *)rec;

        appendStringInfo(buf, "cleanup info: remxid %lu", xlrec->latestRemovedXid);
    } else if (info == XLOG_HEAP2_VISIBLE) {
        xl_heap_visible *xlrec = (xl_heap_visible *)rec;

        appendStringInfo(buf, "visible: cutoff xid %lu", xlrec->cutoff_xid);
    } else if (info == XLOG_HEAP2_BCM) {
        xl_heap_bcm *xlrec = (xl_heap_bcm *)rec;
        int bucket_id = XLogRecGetBucketId(record);
        if (bucket_id == -1) {
            appendStringInfo(buf, "bcm: rel %u/%u/%u; blk %lu status: %d, count: %d, col: %u", xlrec->node.spcNode,
                             xlrec->node.dbNode, xlrec->node.relNode, xlrec->block, xlrec->status, xlrec->count,
                             xlrec->col);
        } else {
            appendStringInfo(buf, "bcm: rel %u/%u/%u/%d; blk %lu status: %d, count: %d, col: %u", xlrec->node.spcNode,
                             xlrec->node.dbNode, xlrec->node.relNode, bucket_id, xlrec->block, xlrec->status,
                             xlrec->count, xlrec->col);
        }
    } else if (info == XLOG_HEAP2_MULTI_INSERT) {
        xl_heap_multi_insert *xlrec = NULL;
        if (XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE) {
            xlrec = (xl_heap_multi_insert *)(rec + sizeof(TransactionId));
            appendStringInfo(buf, "XLOG_HEAP2_MULTI_INSERT multi-insert (init): ");
        } else {
            xlrec = (xl_heap_multi_insert *)rec;
            appendStringInfo(buf, "XLOG_HEAP2_MULTI_INSERT multi-insert: ");
        }
        appendStringInfo(buf, "%d tuples compressed(%d)", xlrec->ntuples, xlrec->isCompressed);
    } else if (info == XLOG_HEAP2_LOGICAL_NEWPAGE) {
        xl_heap_logical_newpage *xlrec = (xl_heap_logical_newpage *)rec;

        int bucket_id = XLogRecGetBucketId(record);
        if (bucket_id == -1) {
            appendStringInfo(buf, "logical newpage rel %u/%u/%u; blk %u", xlrec->node.spcNode, xlrec->node.dbNode,
                             xlrec->node.relNode, xlrec->blkno);
        } else {
            appendStringInfo(buf, "logical newpage rel %u/%u/%u/%d; blk %u", xlrec->node.spcNode, xlrec->node.dbNode,
                             xlrec->node.relNode, bucket_id, xlrec->blkno);
        }
    } else
        appendStringInfo(buf, "UNKNOWN");
}

const char* heap3_type_name(uint8 subtype)
{
    uint8 info = subtype & ~XLR_INFO_MASK;
    info &= XLOG_HEAP_OPMASK;
    if (info == XLOG_HEAP3_NEW_CID) {
        return "heap3_new_cid";
    } else if (info == XLOG_HEAP3_REWRITE) {
        return "heap3_rewrite";
    } else if (info == XLOG_HEAP3_INVALID) {
        return "heap3_invalid";
    } else {
        return "unkown_type";
    }
}

void heap3_desc(StringInfo buf, XLogReaderState *record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    info &= XLOG_HEAP_OPMASK;
    if (info == XLOG_HEAP3_NEW_CID) {
        appendStringInfo(buf, "XLOG_HEAP_NEW_CID");
    } else if (info == XLOG_HEAP3_REWRITE) {
        appendStringInfo(buf, "XLOG_HEAP2_REWRITE");
    } else if (info == XLOG_HEAP3_INVALID) {
        xl_heap_invalid *xlrecInvalid = (xl_heap_invalid *)XLogRecGetData(record);

        appendStringInfo(buf, "invalid: cutoff xid %lu", xlrecInvalid->cutoff_xid);

        if (!XLogRecHasBlockImage(record, 0)) {
            Size datalen;
            OffsetNumber *offsets = (OffsetNumber *)XLogRecGetBlockData(record, 0, &datalen);
            if (datalen > 0) {
                OffsetNumber *offsets_end = (OffsetNumber *)((char *)offsets + datalen);

                appendStringInfo(buf, " offsets: [");
                while (offsets < offsets_end) {
                    appendStringInfo(buf, " %d ", *offsets);
                    offsets++;
                }
                appendStringInfo(buf, "]");
            }
        }
    } else {
        appendStringInfo(buf, "UNKNOWN");
    }
}
