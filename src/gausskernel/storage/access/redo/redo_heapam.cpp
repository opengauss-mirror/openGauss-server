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
 * heapam.cpp
 *    parse heap xlog
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/redo/heapam.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/heapam.h"
#include "access/hio.h"
#include "access/multixact.h"
#include "access/relscan.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "access/tuptoaster.h"
#include "access/valid.h"
#include "access/visibilitymap.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "access/xlogproc.h"
#include "access/multi_redo_api.h"
#include "catalog/catalog.h"
#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "commands/dbcommands.h"
#include "executor/node/nodeModifyTable.h"
#include "replication/dataqueue.h"
#include "replication/datasender.h"
#include "replication/walsender.h"
#include "storage/buf/bufmgr.h"
#include "storage/smgr/fd.h"
#include "storage/freespace.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/smgr/smgr.h"
#include "storage/standby.h"
#include "utils/datum.h"
#include "utils/inval.h"
#include "utils/relcache.h"
#include "utils/partcache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "access/heapam.h"
#include "utils/guc.h"
#include "vecexecutor/vectorbatch.h"
#include "access/multi_redo_api.h"
#include "gstrace/gstrace_infra.h"
#include "gstrace/access_gstrace.h"

#ifdef PGXC
#include "pgxc/pgxc.h"
#include "pgxc/redistrib.h"
#include "replication/bcm.h"
#endif

static void HeapPageShiftBase(Page page, bool multi, int64 delta);

void HeapXlogCleanOperatorPage(RedoBufferInfo *buffer, void *recorddata, void *blkdata, Size datalen, Size *freespace,
                               bool repairFragmentation)
{
    xl_heap_clean *xlrec = (xl_heap_clean *)recorddata;
    Page page = buffer->pageinfo.page;
    OffsetNumber *end = NULL;
    OffsetNumber *redirected = NULL;
    OffsetNumber *nowdead = NULL;
    OffsetNumber *nowunused = NULL;
    int nredirected;
    int ndead;
    int nunused;

    redirected = (OffsetNumber *)blkdata;

    nredirected = xlrec->nredirected;
    ndead = xlrec->ndead;
    end = (OffsetNumber *)((char *)redirected + datalen);
    nowdead = redirected + (nredirected * 2);
    nowunused = nowdead + ndead;
    nunused = (end - nowunused);
    Assert(nunused >= 0);

    /* for performance better not dump log */
    if (module_logging_is_on(MOD_REDO)) {
        DumpPageInfo(page, 0);
    }
    /* Update all item pointers per the record, and repair fragmentation */
    heap_page_prune_execute(page, redirected, nredirected, nowdead, ndead, nowunused, nunused, repairFragmentation);
    if (freespace != NULL) {
        *freespace = PageGetHeapFreeSpace(page); /* needed to update FSM below */
    }
    /* for performance better not dump log */
    if (module_logging_is_on(MOD_REDO)) {
        DumpPageInfo(page, buffer->lsn);
    }

    /*
     * Note: we don't worry about updating the page's prunability hints. At
     * worst this will cause an extra prune cycle to occur soon.
     */
    PageSetLSN(page, buffer->lsn);
}

void HeapXlogFreezeOperatorPage(RedoBufferInfo *buffer, void *recorddata, void *blkdata, Size datalen,
    bool isTupleLockUpgrade)
{
    xl_heap_freeze *xlrec = (xl_heap_freeze *)recorddata;
    Page page = buffer->pageinfo.page;
    TransactionId cutoff_xid = xlrec->cutoff_xid;
    OffsetNumber *offsets = (OffsetNumber *)blkdata;
    OffsetNumber *offsets_end = NULL;
    HeapTupleData tuple;

    if (datalen > 0) {
        offsets_end = (OffsetNumber *)((char *)offsets + datalen);

        while (offsets < offsets_end) {
            /* offsets[] entries are one-based */
            ItemId lp = PageGetItemId(page, *offsets);

            tuple.t_data = (HeapTupleHeader)PageGetItem(page, lp);
            tuple.t_len = ItemIdGetLength(lp);
            HeapTupleCopyBaseFromPage(&tuple, page);
            ItemPointerSet(&(tuple.t_self), buffer->blockinfo.blkno, *offsets);

            (void)heap_freeze_tuple(&tuple, cutoff_xid, isTupleLockUpgrade ? xlrec->cutoff_multi : InvalidMultiXactId);

            offsets++;
        }
    }

    PageSetLSN(page, buffer->lsn);
}

void HeapXlogInvalidOperatorPage(RedoBufferInfo *buffer, void *blkdata, Size datalen)
{
    Page page = buffer->pageinfo.page;
    if (datalen > 0) {
        OffsetNumber *offsets = (OffsetNumber *)blkdata;
        OffsetNumber *offsets_end = (OffsetNumber *)((char *)offsets + datalen);
        HeapTupleData tuple;

        while (offsets < offsets_end) {
            /* offsets[] entries are one-based */
            ItemId lp = PageGetItemId(page, *offsets);

            tuple.t_data = (HeapTupleHeader)PageGetItem(page, lp);
            tuple.t_len = ItemIdGetLength(lp);
            HeapTupleCopyBaseFromPage(&tuple, page);
            ItemPointerSet(&(tuple.t_self), buffer->blockinfo.blkno, *offsets);

            heap_invalid_invisible_tuple(&tuple);

            offsets++;
        }
    }

    PageSetLSN(page, buffer->lsn);
}

void HeapXlogVisibleOperatorPage(RedoBufferInfo *buffer, void *recorddata)
{
    xl_heap_visible *xlrec = (xl_heap_visible *)recorddata;
    Page page = buffer->pageinfo.page;
    /*
     * We don't bump the LSN of the heap page when setting the visibility
     * map bit, because that would generate an unworkable volume of
     * full-page writes.  This exposes us to torn page hazards, but since
     * we're not inspecting the existing page contents in any way, we
     * don't care.
     *
     * However, all operations that clear the visibility map bit *do* bump
     * the LSN, and those operations will only be replayed if the XLOG LSN
     * follows the page LSN.  Thus, if the page LSN has advanced past our
     * XLOG record's LSN, we mustn't mark the page all-visible, because
     * the subsequent update won't be replayed to clear the flag.
     */

    PageSetAllVisible(page);
    if (IsSegmentFileNode(buffer->blockinfo.rnode)) {
        PageSetLSN(page, buffer->lsn);
    }

    if (xlrec->free_dict && PageIsCompressed(page)) {
        (void)PageFreeDict(page);
    }
}

void HeapXlogVisibleOperatorVmpage(RedoBufferInfo *vmbuffer, void *recorddata)
{
    xl_heap_visible *xlrec = (xl_heap_visible *)recorddata;
    Page vmpage = vmbuffer->pageinfo.page;
    Relation reln;

    /*
     * In log_heap_visible,	block 0 is vm_buffer, block 1 is heap_buffer.
     * the vm and heap must have same relfilenode. so whether use block 0 or 1 is correct for relfilenode
     */

    /* initialize the page if it was read as zeros */
    if (PageIsNew(vmpage))
        PageInit(vmpage, BLCKSZ, 0);

    /*
     * XLogReadBufferForRedoExtended locked the buffer. But
     * visibilitymap_set will handle locking itself.
     */
    LockBuffer(vmbuffer->buf, BUFFER_LOCK_UNLOCK);

    reln = CreateFakeRelcacheEntry(vmbuffer->blockinfo.rnode);

    Assert(vmbuffer->blockinfo.blkno == HEAPBLK_TO_MAPBLOCK(xlrec->block));
    visibilitymap_pin(reln, xlrec->block, &(vmbuffer->buf));

    /*
     * Don't set the bit if replay has already passed this point.
     *
     * It might be safe to do this unconditionally; if replay has passed
     * this point, we'll replay at least as far this time as we did
     * before, and if this bit needs to be cleared, the record responsible
     * for doing so should be again replayed, and clear it.  For right
     * now, out of an abundance of conservatism, we use the same test here
     * we did for the heap page.  If this results in a dropped bit, no
     * real harm is done; and the next VACUUM will fix it.
     */
    if (!XLByteLE(vmbuffer->lsn, PageGetLSN(vmpage)))
        visibilitymap_set(reln, xlrec->block, InvalidBuffer, vmbuffer->lsn, vmbuffer->buf, xlrec->cutoff_xid, false);

    ReleaseBuffer(vmbuffer->buf);
    FreeFakeRelcacheEntry(reln);
}

inline static void HeapXlogVisibleOperatorVmbuffer(RedoBufferInfo *vmbuffer, void *recorddata)
{
    xl_heap_visible *xlrec = (xl_heap_visible *)recorddata;
    Page vmpage = vmbuffer->pageinfo.page;

    /* initialize the page if it was read as zeros */
    if (PageIsNew(vmpage))
        PageInit(vmpage, BLCKSZ, 0);

    Assert(vmbuffer->blockinfo.blkno == HEAPBLK_TO_MAPBLOCK(xlrec->block));

    if (!XLByteLE(vmbuffer->lsn, PageGetLSN(vmpage))) {
        if (visibilitymap_set_page(vmpage, xlrec->block)) {
            PageSetLSN(vmpage, vmbuffer->lsn);
            MakeRedoBufferDirty(vmbuffer);
        }
    }
}

void HeapXlogDeleteOperatorPage(RedoBufferInfo *buffer, void *recorddata, TransactionId recordxid,
    bool isTupleLockUpgrade)
{
    xl_heap_delete *xlrec = (xl_heap_delete *)recorddata;
    Page page = buffer->pageinfo.page;
    ItemId lp = NULL;
    HeapTupleHeader htup;
    ItemPointerData target_tid;

    ItemPointerSetBlockNumber(&target_tid, buffer->blockinfo.blkno);
    ItemPointerSetOffsetNumber(&target_tid, xlrec->offnum);

    OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
    if (maxoff >= xlrec->offnum)
        lp = PageGetItemId(page, xlrec->offnum);

    if (maxoff < xlrec->offnum || !ItemIdIsNormal(lp))
        ereport(PANIC, (errmsg("heap_delete_redo: invalid lp")));

    htup = (HeapTupleHeader)PageGetItem(page, lp);

    htup->t_infomask &= ~HEAP_XMAX_BITS;
    htup->t_infomask2 &= ~(HEAP_XMAX_LOCK_ONLY | HEAP_KEYS_UPDATED);
    HeapTupleHeaderClearHotUpdated(htup);

    if (isTupleLockUpgrade) {
        FixInfomaskFromInfobits(xlrec->infobits_set, &htup->t_infomask, &htup->t_infomask2);
        HeapTupleHeaderSetXmax(page, htup, xlrec->xmax);
    } else {
        htup->t_infomask2 |= HEAP_KEYS_UPDATED;
        HeapTupleHeaderSetXmax(page, htup, recordxid);
    }

    if (!(xlrec->flags & XLH_DELETE_IS_SUPER)) {
        if (isTupleLockUpgrade) {
            HeapTupleHeaderSetXmax(page, htup, xlrec->xmax);
        } else {
            HeapTupleHeaderSetXmax(page, htup, recordxid);
        }
    } else {
        HeapTupleHeaderSetXmin(page, htup, FrozenTransactionId);
        HeapTupleHeaderSetXmax(page, htup, FrozenTransactionId);
    }
    HeapTupleHeaderSetCmax(htup, FirstCommandId, false);

    /* Mark the page as a candidate for pruning */
    PageSetPrunable(page, recordxid);

    if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
        PageClearAllVisible(page);

    /* Make sure there is no forward chain link in t_ctid */
    htup->t_ctid = target_tid;
    PageSetLSN(page, buffer->lsn);
}

void HeapXlogInsertOperatorPage(RedoBufferInfo *buffer, void *recorddata, bool isinit, void *blkdata, Size datalen,
                                TransactionId recxid, Size *freespace, bool tde)
{
    Pointer rec_data = (Pointer)recorddata;
    char *data = (char *)blkdata;
    Page page = buffer->pageinfo.page;
    TransactionId pd_xid_base = InvalidTransactionId;
    xl_heap_insert *xlrec = NULL;
    ItemPointerData target_tid;
    errno_t rc = EOK;
    uint32 newlen;
    HeapTupleHeader htup;
    xl_heap_header xlhdr;
    union {
        HeapTupleHeaderData hdr;
        char data[MaxHeapTupleSize + sizeof(HeapTupleHeaderData)];
    } tbuf;

    if (isinit) {
        HeapPageHeader phdr;

        pd_xid_base = *((TransactionId *)rec_data);
        PageInit(page, buffer->pageinfo.pagesize, 0, true);
        phdr = (HeapPageHeader)page;
        phdr->pd_xid_base = pd_xid_base;
        phdr->pd_multi_base = 0;

        rec_data += sizeof(TransactionId);
        /* 
         * When it comes to the TDE record, we prefer to remake the init page in TDE format.
         * And set TDE flag which on the PAGE to the enabled state.
         */
        if (tde) {
            phdr->pd_upper -= sizeof(TdePageInfo);
            phdr->pd_special -= sizeof(TdePageInfo);
            PageSetTDE(page);
        }
    }
    xlrec = (xl_heap_insert *)rec_data;

    ItemPointerSetBlockNumber(&target_tid, buffer->blockinfo.blkno);
    ItemPointerSetOffsetNumber(&target_tid, xlrec->offnum);
    rc = memset_s(&tbuf, sizeof(tbuf), 0, sizeof(tbuf));
    securec_check(rc, "\0", "\0");

    OffsetNumber maxoff = PageGetMaxOffsetNumber(page);

    if (maxoff + 1 < xlrec->offnum)
        ereport(PANIC, (errmsg("heap_insert_redo: invalid max offset number")));

    newlen = datalen - SizeOfHeapHeader;
    Assert(datalen > SizeOfHeapHeader && newlen <= MaxHeapTupleSize);
    rc = memcpy_s((char *)&xlhdr, SizeOfHeapHeader, data, SizeOfHeapHeader);
    securec_check(rc, "", "");
    data += SizeOfHeapHeader;

    htup = &tbuf.hdr;
    rc = memset_s((char *)htup, sizeof(HeapTupleHeaderData), 0, sizeof(HeapTupleHeaderData));
    securec_check(rc, "\0", "\0");
    /* PG73FORMAT: get bitmap [+ padding] [+ oid] + data */
    rc = memcpy_s((char *)htup + offsetof(HeapTupleHeaderData, t_bits), newlen, data, newlen);
    securec_check(rc, "\0", "\0");
    newlen += offsetof(HeapTupleHeaderData, t_bits);
    htup->t_infomask2 = xlhdr.t_infomask2;
    htup->t_infomask = xlhdr.t_infomask;
    htup->t_hoff = xlhdr.t_hoff;
    HeapTupleHeaderSetXmin(page, htup, recxid);
    HeapTupleHeaderSetCmin(htup, FirstCommandId);
    htup->t_ctid = target_tid;

    if (PageAddItem(page, (Item)htup, newlen, xlrec->offnum, true, true) == InvalidOffsetNumber)
        ereport(PANIC, (errmsg("heap_insert_redo: failed to add tuple")));

    if (freespace != NULL) {
        *freespace = PageGetHeapFreeSpace(page);
    }

    PageSetLSN(page, buffer->lsn);

    if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
        PageClearAllVisible(page);
}

void HeapXlogMultiInsertOperatorPage(RedoBufferInfo *buffer, const void *recoreddata,
                                     bool isinit, const void *blkdata, Size len,
                                     TransactionId recordxid, Size *freespace, bool tde)
{
    Pointer rec_data = (Pointer)recoreddata;
    Page page = buffer->pageinfo.page;
    BlockNumber blkno = buffer->blockinfo.blkno;
    TransactionId pd_xid_base = InvalidTransactionId;

    union {
        HeapTupleHeaderData hdr;
        char data[MaxHeapTupleSize + sizeof(HeapTupleHeaderData)];
    } tbuf;

    errno_t rc = memset_s(&tbuf, sizeof(tbuf), 0, sizeof(tbuf));
    securec_check(rc, "\0", "\0");

    if (isinit) {
        pd_xid_base = *((TransactionId *)rec_data);
        PageInit(page, buffer->pageinfo.pagesize, 0, true);
        HeapPageHeader phdr = (HeapPageHeader)page;
        phdr->pd_xid_base = pd_xid_base;
        phdr->pd_multi_base = 0;

        rec_data += sizeof(TransactionId);
        /* 
         * When it comes to the TDE record, we prefer to remake the init page in TDE format.
         * And set TDE flag which on the PAGE to the enabled state.
         */
        if (tde) {
            phdr->pd_upper -= sizeof(TdePageInfo);
            phdr->pd_special -= sizeof(TdePageInfo);
            PageSetTDE(page);
        }
    }
    xl_heap_multi_insert *xlrec = (xl_heap_multi_insert *)rec_data;

    char* newblkdata = (char*)palloc(len);
    rc = memcpy_s(newblkdata, len, blkdata, len);
    securec_check(rc, "\0", "\0");
    /* Tuples are stored as block data */
    char *tupdata = newblkdata;
    char *endptr = tupdata + len;

    if (xlrec->isCompressed) {
        char *cmprsData = (char *)SHORTALIGN(tupdata);
        Size cmprSize = *((int16 *)cmprsData);

        cmprsData += sizeof(int16);
        Assert(isinit);
        PageReinitWithDict(page, cmprSize);
        rc = memcpy_s((char *)getPageDict(page), (Size)PageGetSpecialSize(page), cmprsData, cmprSize);
        securec_check(rc, "\0", "\0");
        tupdata = cmprsData + cmprSize;
    }

    for (uint32 i = 0; i < xlrec->ntuples; i++) {
        OffsetNumber offnum, maxoff;
        xl_multi_insert_tuple *xlhdr = NULL;

        if (isinit)
            offnum = FirstOffsetNumber + i;
        else
            offnum = xlrec->offsets[i];

        maxoff = PageGetMaxOffsetNumber(page);
        if (maxoff + 1 < offnum)
            ereport(PANIC, (errmsg("heap_multi_insert_redo: invalid max offset number")));

        xlhdr = (xl_multi_insert_tuple *)SHORTALIGN(tupdata);
        tupdata = ((char *)xlhdr) + SizeOfMultiInsertTuple;

        uint32 newlen = xlhdr->datalen;
        Assert(newlen <= MaxHeapTupleSize);
        HeapTupleHeader htup = &tbuf.hdr;
        rc = memset_s((char *)htup, sizeof(HeapTupleHeaderData), 0, sizeof(HeapTupleHeaderData));
        securec_check_c(rc, "\0", "\0");
        /* PG73FORMAT: get bitmap [+ padding] [+ oid] + data */
        rc = memcpy_s((char *)htup + offsetof(HeapTupleHeaderData, t_bits), newlen, (char *)tupdata, newlen);
        securec_check(rc, "\0", "\0");
        tupdata += newlen;

        newlen += offsetof(HeapTupleHeaderData, t_bits);
        htup->t_infomask2 = xlhdr->t_infomask2;
        htup->t_infomask = xlhdr->t_infomask;
        htup->t_hoff = xlhdr->t_hoff;
        HeapTupleHeaderSetXmin(page, htup, recordxid);
        HeapTupleHeaderSetCmin(htup, FirstCommandId);
        ItemPointerSetBlockNumber(&htup->t_ctid, blkno);
        ItemPointerSetOffsetNumber(&htup->t_ctid, offnum);

        offnum = PageAddItem(page, (Item)htup, newlen, offnum, true, true);
        if (offnum == InvalidOffsetNumber)
            ereport(PANIC, (errmsg("heap_multi_insert_redo: failed to add tuple")));
    }
    if (tupdata != endptr)
        ereport(PANIC, (errmsg("heap_multi_insert_redo: total tuple length mismatch")));
    if (freespace != NULL) {
        *freespace = PageGetHeapFreeSpace(page); /* needed to update FSM below */
    }

    PageSetLSN(page, buffer->lsn);

    if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
        PageClearAllVisible(page);

    if (newblkdata != NULL) {
        pfree(newblkdata);
    }
}

void HeapXlogUpdateOperatorOldpage(RedoBufferInfo *buffer, void *recoreddata, bool hot_update, bool isnewinit,
                                   BlockNumber newblk, TransactionId recordxid, bool isTupleLockUpgrade)
{
    Page page = buffer->pageinfo.page;
    Pointer rec_data = (Pointer)recoreddata;
    xl_heap_update *xlrec = NULL;
    ItemId lp = NULL;
    HeapTupleHeader htup;
    ItemPointerData newtid;

    if (isnewinit) {
        rec_data += sizeof(TransactionId);
    }

    xlrec = (xl_heap_update *)rec_data;

    ItemPointerSet(&newtid, newblk, xlrec->new_offnum);

    OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
    if (maxoff >= xlrec->old_offnum)
        lp = PageGetItemId(page, xlrec->old_offnum);

    if (maxoff < xlrec->old_offnum || !ItemIdIsNormal(lp))
        ereport(PANIC, (errmsg("heap_update_redo: invalid lp")));

    htup = (HeapTupleHeader)PageGetItem(page, lp);

    htup->t_infomask &= ~HEAP_XMAX_BITS;
    htup->t_infomask2 &= ~(HEAP_XMAX_LOCK_ONLY | HEAP_KEYS_UPDATED);
    if (hot_update)
        HeapTupleHeaderSetHotUpdated(htup);
    else
        HeapTupleHeaderClearHotUpdated(htup);
    if (isTupleLockUpgrade) {
        FixInfomaskFromInfobits(xlrec->old_infobits_set, &htup->t_infomask, &htup->t_infomask2);
        HeapTupleHeaderSetXmax(page, htup, xlrec->old_xmax);
    } else {
        htup->t_infomask2 |= HEAP_KEYS_UPDATED;
        HeapTupleHeaderSetXmax(page, htup, recordxid);
    }
    HeapTupleHeaderSetCmax(htup, FirstCommandId, false);
    /* Set forward chain link in t_ctid */
    htup->t_ctid = newtid;

    /* Mark the page as a candidate for pruning */
    PageSetPrunable(page, recordxid);

    if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
        PageClearAllVisible(page);

    PageHeader oldPhdr = (PageHeader)page;
    // too much log may slow down the speed of xlog, so only write log
    // when log level belows DEBUG4
    if (module_logging_is_on(MOD_REDO)) {
        ereport(DEBUG4, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                         errmsg("[REDO_LOG_TRACE]heap_xlog_update:,"
                                "oldPageOldLsn:%lu,OldPageNewLsn:%lu,oldpd_lower:%u, oldpd_upper:%u, "
                                "oldpd_special:%u,OldPageOffset:%u",
                                PageGetLSN(page), buffer->lsn, oldPhdr->pd_lower, oldPhdr->pd_upper,
                                oldPhdr->pd_special, PageGetMaxOffsetNumber(page))));
    }
    PageSetLSN(page, buffer->lsn);
}

void HeapXlogUpdateOperatorNewpage(RedoBufferInfo *buffer, void *recorddata, bool isinit, void *blkdata, Size datalen,
                                   TransactionId recordxid, Size *freespace, bool isTupleLockUpgrade, bool tde)
{
    Page page = buffer->pageinfo.page;
    Pointer rec_data = (Pointer)recorddata;
    BlockNumber newblk = buffer->blockinfo.blkno;
    xl_heap_update *xlrec = NULL;
    ItemPointerData newtid;
    char *recblkdata = (char *)blkdata;
    char *recblkdata_end = NULL;
    Size tuplen;
    errno_t rc = EOK;
    OffsetNumber maxoff;
    xl_heap_header xlhdr;
    TransactionId pd_xid_base = InvalidTransactionId;
    union {
        HeapTupleHeaderData hdr;
        char data[MaxHeapTupleSize + sizeof(HeapTupleHeaderData)];
    } tbuf;
    HeapTupleHeader htup;
    uint32 newlen;

    rc = memset_s(&tbuf, sizeof(tbuf), 0, sizeof(tbuf));
    securec_check(rc, "\0", "\0");

    if (isinit) {
        HeapPageHeader phdr;

        pd_xid_base = *((TransactionId *)rec_data);
        PageInit(page, buffer->pageinfo.pagesize, 0, true);
        phdr = (HeapPageHeader)page;
        phdr->pd_xid_base = pd_xid_base;
        phdr->pd_multi_base = 0;

        rec_data += sizeof(TransactionId);
        /* 
         * When it comes to the TDE record, we prefer to remake the init page in TDE format.
         * And set TDE flag which on the PAGE to the enabled state.
         */
        if (tde) {
            phdr->pd_upper -= sizeof(TdePageInfo);
            phdr->pd_special -= sizeof(TdePageInfo);
            PageSetTDE(page);
        }
    }
    xlrec = (xl_heap_update *)rec_data;

    ItemPointerSet(&newtid, newblk, xlrec->new_offnum);

    recblkdata_end = recblkdata + datalen;

    maxoff = PageGetMaxOffsetNumber(page);

    rc = memcpy_s((char *)&xlhdr, SizeOfHeapHeader, recblkdata, SizeOfHeapHeader);
    securec_check(rc, "", "");
    recblkdata += SizeOfHeapHeader;

    tuplen = recblkdata_end - recblkdata;

    // too much log may slow down the speed of xlog, so only write log
    // when log level belows DEBUG4
    PageHeader newphdr = (PageHeader)page;
    if (module_logging_is_on(MOD_REDO)) {
        ereport(DEBUG4, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                         errmsg("[REDO_LOG_TRACE]heap_xlog_update:,"
                                "newPageOldLsn:%lu,newPageNewLsn:%lu,newpd_lower:%u, newpd_upper:%u, "
                                "newpd_special:%u,new_offnum:%u,newPageOffset:%u",
                                PageGetLSN(page), buffer->lsn, newphdr->pd_lower, newphdr->pd_upper,
                                newphdr->pd_special, xlrec->new_offnum, PageGetMaxOffsetNumber(page))));
    }

    if (maxoff + 1 < xlrec->new_offnum)
        ereport(PANIC, (errmsg("heap_update_redo: invalid max offset number")));

    Assert(tuplen <= MaxHeapTupleSize);

    htup = &tbuf.hdr;
    rc = memset_s((char *)htup, sizeof(HeapTupleHeaderData), 0, sizeof(HeapTupleHeaderData));
    securec_check(rc, "\0", "\0");
    /* PG73FORMAT: get bitmap [+ padding] [+ oid] + data */
    rc = memcpy_s((char *)htup + offsetof(HeapTupleHeaderData, t_bits), tuplen, recblkdata, tuplen);
    securec_check(rc, "\0", "\0");
    newlen = offsetof(HeapTupleHeaderData, t_bits) + tuplen;
    htup->t_infomask2 = xlhdr.t_infomask2;
    htup->t_infomask = xlhdr.t_infomask;
    htup->t_hoff = xlhdr.t_hoff;

    HeapTupleHeaderSetXmin(page, htup, recordxid);
    HeapTupleHeaderSetCmin(htup, FirstCommandId);
    if (isTupleLockUpgrade) {
        HeapTupleHeaderSetXmax(page, htup, xlrec->new_xmax);
    }
    /* Make sure there is no forward chain link in t_ctid */
    htup->t_ctid = newtid;

    if (PageAddItem(page, (Item)htup, newlen, xlrec->new_offnum, true, true) == InvalidOffsetNumber)
        ereport(PANIC, (errmsg("heap_update_redo: failed to add tuple")));

    if (xlrec->flags & XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED)
        PageClearAllVisible(page);
    if (freespace != NULL) {
        *freespace = PageGetHeapFreeSpace(page);
    }
    PageSetLSN(page, buffer->lsn);
}

void HeapXlogLockOperatorPage(RedoBufferInfo *buffer, void *recorddata, bool isTupleLockUpgrade)
{
    xl_heap_lock *xlrec = (xl_heap_lock *)recorddata;
    Page page = buffer->pageinfo.page;
    ItemId lp = NULL;
    HeapTupleHeader htup;

    OffsetNumber maxoff = PageGetMaxOffsetNumber(page);

    if (maxoff >= xlrec->offnum)
        lp = PageGetItemId(page, xlrec->offnum);

    if (maxoff < xlrec->offnum || !ItemIdIsNormal(lp))
        ereport(PANIC, (errmsg("heap_lock_redo: invalid lp")));

    htup = (HeapTupleHeader)PageGetItem(page, lp);

    htup->t_infomask &= ~HEAP_XMAX_BITS;
    htup->t_infomask2 &= ~(HEAP_XMAX_LOCK_ONLY | HEAP_KEYS_UPDATED);

    if (isTupleLockUpgrade) {
        FixInfomaskFromInfobits(xlrec->infobits_set, &htup->t_infomask, &htup->t_infomask2);
        if (xlrec->lock_updated) {
            HeapTupleHeaderSetXmax(page, htup, xlrec->locking_xid);
            PageSetLSN(page, buffer->lsn);
            return;
        }
    } else {
        if (xlrec->xid_is_mxact)
            htup->t_infomask |= HEAP_XMAX_IS_MULTI;
        if (xlrec->shared_lock)
            htup->t_infomask |= HEAP_XMAX_SHARED_LOCK;
        else {
            htup->t_infomask |= HEAP_XMAX_EXCL_LOCK;
            htup->t_infomask2 |= HEAP_KEYS_UPDATED;
        }
    }
    /*
     * Clear relevant update flags, but only if the modified infomask says
     * there's no update.
     */
    if (HEAP_XMAX_IS_LOCKED_ONLY(htup->t_infomask, htup->t_infomask2)) {
        HeapTupleHeaderClearHotUpdated(htup);
        /* Make sure there is no forward chain link in t_ctid */
        ItemPointerSet(&htup->t_ctid, buffer->blockinfo.blkno, xlrec->offnum);
    }
    HeapTupleHeaderSetXmax(page, htup, xlrec->locking_xid);
    HeapTupleHeaderSetCmax(htup, FirstCommandId, false);

    PageSetLSN(page, buffer->lsn);
}

void HeapXlogInplaceOperatorPage(RedoBufferInfo *buffer, void *recorddata, void *blkdata, Size newlen)
{
    xl_heap_inplace *xlrec = (xl_heap_inplace *)recorddata;
    Page page = buffer->pageinfo.page;
    ItemId lp = NULL;
    HeapTupleHeader htup;
    uint32 oldlen;
    errno_t rc = EOK;
    OffsetNumber maxoff;

    char *newtup = (char *)blkdata;
    if (newtup == NULL)
        ereport(PANIC, (errmsg("heap_inplace_redo: no tuple data")));
    maxoff = PageGetMaxOffsetNumber(page);

    if (maxoff >= xlrec->offnum)
        lp = PageGetItemId(page, xlrec->offnum);

    if (maxoff < xlrec->offnum || !ItemIdIsNormal(lp))
        ereport(PANIC, (errmsg("heap_inplace_redo: invalid lp")));

    htup = (HeapTupleHeader)PageGetItem(page, lp);

    oldlen = ItemIdGetLength(lp) - htup->t_hoff;
    if (oldlen != newlen)
        ereport(PANIC, (errmsg("heap_inplace_redo: wrong tuple length")));

    rc = memcpy_s((char *)htup + htup->t_hoff, newlen, newtup, newlen);
    securec_check(rc, "\0", "\0");
    PageSetLSN(page, buffer->lsn);
}

/**
 * @Description: Shift xid base in the page.
 * @in: page, heap page
 * @in: multi,
 * @in: delta, size of change about xid base
 */
static void HeapPageShiftBase(Page page, bool multi, int64 delta)
{
    HeapPageHeader phdr = (HeapPageHeader)page;
    OffsetNumber offnum, maxoff;

    /* base left shift, mininum is 0 */
    if (delta < 0) {
        if (!multi) {
            if ((int64)(phdr->pd_xid_base + delta) < 0)
                delta = -(int64)(phdr->pd_xid_base);
        } else {
            if ((int64)(phdr->pd_multi_base + delta) < 0)
                delta = -(int64)(phdr->pd_multi_base);
        }
    }

    /* Iterate over page items */
    maxoff = PageGetMaxOffsetNumber(page);
    for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum = OffsetNumberNext(offnum)) {
        ItemId itemid;
        HeapTupleHeader htup;

        itemid = PageGetItemId(page, offnum);

        if (!ItemIdIsNormal(itemid))
            continue;

        htup = (HeapTupleHeader)PageGetItem(page, itemid);

        /* Apply xid shift to heap tuple */
        if (!multi) {
            if (!HeapTupleHeaderXminFrozen(htup) && TransactionIdIsNormal(htup->t_choice.t_heap.t_xmin)) {
                Assert((uint32)(htup->t_choice.t_heap.t_xmin - delta) >= FirstNormalTransactionId);
                Assert((uint32)(htup->t_choice.t_heap.t_xmin - delta) <= MaxShortTransactionId);
                htup->t_choice.t_heap.t_xmin -= delta;
            }

            if (TransactionIdIsNormal(htup->t_choice.t_heap.t_xmax) && !(htup->t_infomask & HEAP_XMAX_IS_MULTI)) {
                Assert((uint32)(htup->t_choice.t_heap.t_xmax - delta) >= FirstNormalTransactionId);
                Assert((uint32)(htup->t_choice.t_heap.t_xmax - delta) <= MaxShortTransactionId);
                htup->t_choice.t_heap.t_xmax -= delta;
            }
        } else {
            if (TransactionIdIsNormal(htup->t_choice.t_heap.t_xmax) && (htup->t_infomask & HEAP_XMAX_IS_MULTI)) {
                Assert((uint32)(htup->t_choice.t_heap.t_xmax - delta) >= FirstNormalTransactionId);
                Assert((uint32)(htup->t_choice.t_heap.t_xmax - delta) <= MaxShortTransactionId);
                htup->t_choice.t_heap.t_xmax -= delta;
            }
        }
    }

    /* Apply xid shift to base as well */
    if (!multi)
        phdr->pd_xid_base += delta;

    else
        phdr->pd_multi_base += delta;

    ereport(DEBUG1, (errmsg("The page xid_base has changed to %lu ", phdr->pd_xid_base)));
}

void HeapXlogBaseShiftOperatorPage(RedoBufferInfo *buffer, void *recorddata)
{
    xl_heap_base_shift *xlrec = (xl_heap_base_shift *)recorddata;
    Page page = buffer->pageinfo.page;

    HeapPageShiftBase(page, xlrec->multi, xlrec->delta);
    PageSetLSN(page, buffer->lsn);
}
/*  redo record parse and dispatch begin */

static XLogRecParseState *HeapXlogInsertParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    xl_heap_insert *xlrec = NULL;
    bool isinit = (XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE) != 0;
    Pointer rec_data;
    XLogRecParseState *recordstatehead = NULL;
    XLogRecParseState *blockstate = NULL;

    *blocknum = 1;

    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }

    XLogRecSetBlockDataState(record, HEAP_INSERT_ORIG_BLOCK_NUM, recordstatehead);

    rec_data = (Pointer)XLogRecGetData(record);
    if (isinit) {
        rec_data += sizeof(TransactionId);
    }
    xlrec = (xl_heap_insert *)rec_data;

    if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED) {
        (*blocknum)++;
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        if (blockstate == NULL) {
            return NULL;
        }
        XLogRecSetVmBlockState(record, HEAP_INSERT_ORIG_BLOCK_NUM, blockstate);
    }

    return recordstatehead;
}

static XLogRecParseState *HeapXlogDeleteParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    xl_heap_delete *xlrec = (xl_heap_delete *)XLogRecGetData(record);
    XLogRecParseState *recordstatehead = NULL;
    XLogRecParseState *blockstate = NULL;

    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }

    XLogRecSetBlockDataState(record, HEAP_DELETE_ORIG_BLOCK_NUM, recordstatehead);

    if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED) {
        (*blocknum)++;
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        if (blockstate == NULL) {
            return NULL;
        }

        XLogRecSetVmBlockState(record, HEAP_DELETE_ORIG_BLOCK_NUM, blockstate);
    }

    return recordstatehead;
}

/*
 * Handles UPDATE and HOT_UPDATE
 */
static XLogRecParseState *HeapXlogUpdateParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    xl_heap_update *xlrec = NULL;
    bool isinit = (XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE) != 0;
    Pointer rec_data;
    XLogRecParseState *recordstatehead = NULL;
    XLogRecParseState *blockstate = NULL;
    BlockNumber newblk, oldblk;

    XLogRecGetBlockTag(record, HEAP_UPDATE_NEW_BLOCK_NUM, NULL, NULL, &newblk);

    if (!XLogRecGetBlockTag(record, HEAP_UPDATE_OLD_BLOCK_NUM, NULL, NULL, &oldblk)) {
        oldblk = newblk;
    }

    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, HEAP_UPDATE_NEW_BLOCK_NUM, recordstatehead);
    XLogRecSetAuxiBlkNumState(&recordstatehead->blockparse.extra_rec.blockdatarec, oldblk, InvalidForkNumber);
    // NEW BLOCK

    rec_data = (Pointer)XLogRecGetData(record);
    if (isinit) {
        rec_data += sizeof(TransactionId);
    }
    xlrec = (xl_heap_update *)rec_data;

    if (oldblk != newblk) {
        Assert(!(((XLogRecGetInfo(record) & ~XLR_INFO_MASK) & XLOG_HEAP_OPMASK) == XLOG_HEAP_HOT_UPDATE));

        (*blocknum)++;
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        if (blockstate == NULL) {
            return NULL;
        }
        XLogRecSetBlockDataState(record, HEAP_UPDATE_OLD_BLOCK_NUM, blockstate);
        XLogRecSetAuxiBlkNumState(&blockstate->blockparse.extra_rec.blockdatarec, newblk, InvalidForkNumber);
        // OLD BLOCK

        if (xlrec->flags & XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED) {
            (*blocknum)++;
            XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
            if (blockstate == NULL) {
                return NULL;
            }

            XLogRecSetVmBlockState(record, HEAP_UPDATE_OLD_BLOCK_NUM, blockstate);
            // OLD BLOCK VM
        }
    }

    if ((xlrec->flags & XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED) ||
        ((oldblk == newblk) && (xlrec->flags & XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED))) {
        (*blocknum)++;
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        if (blockstate == NULL) {
            return NULL;
        }

        XLogRecSetVmBlockState(record, HEAP_UPDATE_NEW_BLOCK_NUM, blockstate);
        // NEW BLOCK VM
    }
    return recordstatehead;
}

static XLogRecParseState *HeapXlogBaseShiftParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;

    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, HEAP_BASESHIFT_ORIG_BLOCK_NUM, recordstatehead, BLOCK_DATA_MAIN_DATA_TYPE, true);
    return recordstatehead;
}

static XLogRecParseState *HeapXlogNewpageParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;

    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, HEAP_NEWPAGE_ORIG_BLOCK_NUM, recordstatehead);

    return recordstatehead;
}

static XLogRecParseState *HeapXlogLockParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;

    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, HEAP_LOCK_ORIG_BLOCK_NUM, recordstatehead, BLOCK_DATA_MAIN_DATA_TYPE, true);

    return recordstatehead;
}

static XLogRecParseState *HeapXlogInplaceParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;

    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, HEAP_INPLACE_ORIG_BLOCK_NUM, recordstatehead);

    return recordstatehead;
}

XLogRecParseState *HeapRedoParseToBlock(XLogReaderState *record, uint32 *blocknum)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    XLogRecParseState *recordblockstate = NULL;

    *blocknum = 0;
    /*
     * These operations don't overwrite MVCC data so no conflict processing is
     * required. The ones in heap2 rmgr do.
     */

    switch (info & XLOG_HEAP_OPMASK) {
        case XLOG_HEAP_INSERT:
            recordblockstate = HeapXlogInsertParseBlock(record, blocknum);
            break;
        case XLOG_HEAP_DELETE:
            recordblockstate = HeapXlogDeleteParseBlock(record, blocknum);
            break;
        case XLOG_HEAP_UPDATE:
        case XLOG_HEAP_HOT_UPDATE:
            recordblockstate = HeapXlogUpdateParseBlock(record, blocknum);
            break;
        case XLOG_HEAP_BASE_SHIFT:
            recordblockstate = HeapXlogBaseShiftParseBlock(record, blocknum);
            break;
        case XLOG_HEAP_NEWPAGE:
            recordblockstate = HeapXlogNewpageParseBlock(record, blocknum);
            break;
        case XLOG_HEAP_LOCK:
            recordblockstate = HeapXlogLockParseBlock(record, blocknum);
            break;
        case XLOG_HEAP_INPLACE:
            recordblockstate = HeapXlogInplaceParseBlock(record, blocknum);
            break;
        default:
            ereport(PANIC, (errmsg("HeapRedoParseToBlock: unknown op code %u", info)));
    }
    return recordblockstate;
}

static XLogRecParseState *HeapXlogFreezeParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;

    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, HEAP_FREEZE_ORIG_BLOCK_NUM, recordstatehead, BLOCK_DATA_MAIN_DATA_TYPE, true);

    return recordstatehead;
}

static XLogRecParseState *HeapXlogInvalidParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    *blocknum = 1;
    XLogRecParseState *recordstatehead = NULL;

    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, HEAP_FREEZE_ORIG_BLOCK_NUM, recordstatehead, BLOCK_DATA_MAIN_DATA_TYPE, true);

    return recordstatehead;
}

static XLogRecParseState *HeapXlogCleanParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;

    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, HEAP_CLEAN_ORIG_BLOCK_NUM, recordstatehead, BLOCK_DATA_MAIN_DATA_TYPE, true);

    return recordstatehead;
}

static XLogRecParseState *HeapXlogCleanupInfoParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    RelFileNodeOld *rnode = NULL;
    ForkNumber forknum = MAIN_FORKNUM;
    BlockNumber blkno = InvalidBlockNumber;

    (*blocknum)++;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);

    xl_heap_cleanup_info *xlrec = (xl_heap_cleanup_info *)XLogRecGetData(record);
    rnode = &(xlrec->node);
    forknum = MAIN_FORKNUM;
    RelFileNode tmp_node;
    RelFileNodeCopy(tmp_node, *rnode, (int2)XLogRecGetBucketId(record));
    tmp_node.opt = 0;
    RelFileNodeForkNum filenode = RelFileNodeForkNumFill(&tmp_node, InvalidBackendId, forknum, blkno);
    XLogRecSetBlockCommonState(record, BLOCK_DATA_CLEANUP_TYPE, filenode, recordstatehead);

    wal_rec_set_clean_up_info_state(&(recordstatehead->blockparse.extra_rec.clean_up_info), xlrec->latestRemovedXid);

    return recordstatehead;
}

static XLogRecParseState *HeapXlogVisibleParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    XLogRecParseState *blockstate = NULL;

    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, HEAP_VISIBLE_VM_BLOCK_NUM, recordstatehead, BLOCK_DATA_MAIN_DATA_TYPE, true);

    if (XLogRecHasBlockRef(record, HEAP_VISIBLE_DATA_BLOCK_NUM)) {
        (*blocknum)++;
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        if (blockstate == NULL) {
            return NULL;
        }
        XLogRecSetBlockDataState(record, HEAP_VISIBLE_DATA_BLOCK_NUM, blockstate, BLOCK_DATA_MAIN_DATA_TYPE, true);
    }
    
    return recordstatehead;
}

static XLogRecParseState *HeapXlogBcmParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    xl_heap_bcm *xlrec = (xl_heap_bcm *)XLogRecGetData(record);
    int col = xlrec->col;
    XLogRecParseState *recordstatehead = NULL;
    XLogRecParseState *blockstate = NULL;
    BlockNumber curBcmBlock;

    if (SUPPORT_COLUMN_BATCH) {
        *blocknum = 1;
        XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
        if (recordstatehead == NULL) {
            return NULL;
        }
        blockstate = recordstatehead;
        curBcmBlock = HEAPBLK_TO_BCMBLOCK(xlrec->block);
        RelFileNode rnode;
        RelFileNodeCopy(rnode, xlrec->node, XLogRecGetBucketId(record));

        RelFileNodeForkNum filenode = RelFileNodeForkNumFill(&rnode, InvalidBackendId, col, curBcmBlock);
        XLogRecSetBlockCommonState(record, BLOCK_DATA_BCM_TYPE, filenode, blockstate);
        XLogRecSetNewCuState(&recordstatehead->blockparse.extra_rec.blocknewcu, XLogRecGetData(record),
                             XLogRecGetDataLen(record));
    }
    return recordstatehead;
}

static XLogRecParseState *HeapXlogMultiInsertParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    xl_heap_multi_insert *xlrec = NULL;
    bool isinit = (XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE) != 0;
    Pointer rec_data;
    XLogRecParseState *recordstatehead = NULL;
    XLogRecParseState *blockstate = NULL;

    *blocknum = 1;
    ;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }

    XLogRecSetBlockDataState(record, HEAP_MULTI_INSERT_ORIG_BLOCK_NUM, recordstatehead);

    rec_data = (Pointer)XLogRecGetData(record);
    if (isinit) {
        rec_data += sizeof(TransactionId);
    }
    xlrec = (xl_heap_multi_insert *)rec_data;

    if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED) {
        (*blocknum)++;
        XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
        if (blockstate == NULL) {
            return NULL;
        }

        XLogRecSetVmBlockState(record, HEAP_MULTI_INSERT_ORIG_BLOCK_NUM, blockstate);
    }

    return recordstatehead;
}

static XLogRecParseState *HeapXlogLogicalNewPageParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    *blocknum = 0;

    if (SUPPORT_COLUMN_BATCH) {
        *blocknum = 1;
        XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
        if (recordstatehead == NULL) {
            return NULL;
        }
        xl_heap_logical_newpage *xlrec = (xl_heap_logical_newpage *)XLogRecGetData(record);

        RelFileNode rnode;
        RelFileNodeCopy(rnode, xlrec->node, XLogRecGetBucketId(record));

        RelFileNodeForkNum filenode = RelFileNodeForkNumFill(&rnode, InvalidBackendId, xlrec->blkno, xlrec->attid);
        XLogRecSetBlockCommonState(record, BLOCK_DATA_NEWCU_TYPE, filenode, recordstatehead);
        XLogRecSetNewCuState(&recordstatehead->blockparse.extra_rec.blocknewcu, XLogRecGetData(record),
                             XLogRecGetDataLen(record));
    }

    return recordstatehead;
}

XLogRecParseState *Heap2RedoParseIoBlock(XLogReaderState *record, uint32 *blocknum)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    XLogRecParseState *recordblockstate = NULL;

    *blocknum = 0;
    switch (info & XLOG_HEAP_OPMASK) {
        case XLOG_HEAP2_FREEZE:
            recordblockstate = HeapXlogFreezeParseBlock(record, blocknum);
            break;
        case XLOG_HEAP2_CLEAN:
            recordblockstate = HeapXlogCleanParseBlock(record, blocknum);
            break;
        case XLOG_HEAP2_CLEANUP_INFO:
            recordblockstate = HeapXlogCleanupInfoParseBlock(record, blocknum);
            break;
        case XLOG_HEAP2_VISIBLE:
            recordblockstate = HeapXlogVisibleParseBlock(record, blocknum);
            break;
        case XLOG_HEAP2_BCM:
            recordblockstate = HeapXlogBcmParseBlock(record, blocknum);
            break;
        case XLOG_HEAP2_MULTI_INSERT:
            recordblockstate = HeapXlogMultiInsertParseBlock(record, blocknum);
            break;
        case XLOG_HEAP2_LOGICAL_NEWPAGE:
            recordblockstate = HeapXlogLogicalNewPageParseBlock(record, blocknum);
            break;
        default:
            ereport(PANIC, (errmsg("Heap2RedoParseIoBlock: unknown op code %u", info)));
    }

    return recordblockstate;
}

static void HeapXlogInsertBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    bool isinit = (XLogBlockHeadGetInfo(blockhead) & XLOG_HEAP_INIT_PAGE) != 0;
    bool tde = ((blockdatarec->blockhead.cur_block_id) & BKID_HAS_TDE_PAGE) != 0;
    TransactionId recordxid = XLogBlockHeadGetXid(blockhead);
    XLogBlockDataParse *datadecode = blockdatarec;
    XLogRedoAction action;
    action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action == BLK_NEEDS_REDO) {
        Size blkdatalen;
        char *blkdata = NULL;
        char *maindata = XLogBlockDataGetMainData(datadecode, NULL);

        blkdata = XLogBlockDataGetBlockData(datadecode, &blkdatalen);
        Assert(blkdata != NULL);
        HeapXlogInsertOperatorPage(bufferinfo, maindata, isinit, (void *)blkdata, blkdatalen, recordxid, NULL, tde);
        MakeRedoBufferDirty(bufferinfo);
    }
}

static void HeapXlogClearVmBlock(XLogBlockVmParse *blockvm, RedoBufferInfo *bufferinfo)
{
    if (PageIsNew(bufferinfo->pageinfo.page)) {
        PageInit(bufferinfo->pageinfo.page, BLCKSZ, 0);
    }
    visibilitymap_clear_buffer(bufferinfo, blockvm->heapBlk);
}

static void HeapXlogDeleteBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    TransactionId recordxid = XLogBlockHeadGetXid(blockhead);
    XLogBlockDataParse *datadecode = blockdatarec;
    XLogRedoAction action;

    action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action == BLK_NEEDS_REDO) {
        char *maindata = XLogBlockDataGetMainData(datadecode, NULL);
        bool isTupleLockUpgrad = (XLogBlockHeadGetInfo(blockhead) & XLOG_TUPLE_LOCK_UPGRADE_FLAG) != 0;

        HeapXlogDeleteOperatorPage(bufferinfo, (void *)maindata, recordxid, isTupleLockUpgrad);
        MakeRedoBufferDirty(bufferinfo);
    }
}

static void HeapXlogUpdateBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    bool isinit = (XLogBlockHeadGetInfo(blockhead) & XLOG_HEAP_INIT_PAGE) != 0;
    bool hot_update = (((XLogBlockHeadGetInfo(blockhead) & ~XLR_INFO_MASK) & XLOG_HEAP_OPMASK) == XLOG_HEAP_HOT_UPDATE);
    bool tde = ((blockdatarec->blockhead.cur_block_id) & BKID_HAS_TDE_PAGE) != 0;
    TransactionId recordxid = XLogBlockHeadGetXid(blockhead);
    XLogBlockDataParse *datadecode = blockdatarec;
    bool isTupleLockUpgrade = (XLogBlockHeadGetInfo(blockhead) & XLOG_TUPLE_LOCK_UPGRADE_FLAG) != 0;

    XLogRedoAction action;

    action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);

    if (action == BLK_NEEDS_REDO) {
        char *maindata = XLogBlockDataGetMainData(datadecode, NULL);

        if (XLogBlockDataGetBlockId(datadecode) == HEAP_UPDATE_NEW_BLOCK_NUM) {
            Size blkdatalen;
            char *blkdata = NULL;

            BlockNumber oldblk = XLogBlockDataGetAuxiBlock1(datadecode);

            if (oldblk == bufferinfo->blockinfo.blkno) {
                HeapXlogUpdateOperatorOldpage(bufferinfo, (void *)maindata, hot_update, isinit, oldblk,
                                              recordxid, isTupleLockUpgrade); /* old tuple */
            }

            blkdata = XLogBlockDataGetBlockData(datadecode, &blkdatalen);
            Assert(blkdata != NULL);

            /* new block */
            HeapXlogUpdateOperatorNewpage(bufferinfo, (void *)maindata, isinit, (void *)blkdata, blkdatalen, recordxid,
                                          NULL, isTupleLockUpgrade, tde);
        } else {
            BlockNumber newblk = XLogBlockDataGetAuxiBlock1(datadecode);

            /* old block */
            HeapXlogUpdateOperatorOldpage(bufferinfo, (void *)maindata, hot_update, isinit, newblk, recordxid,
                                          isTupleLockUpgrade);
        }
        MakeRedoBufferDirty(bufferinfo);
    }
}

static void HeapXlogBaseShiftBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec,
                                   RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;
    XLogRedoAction action;

    action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action == BLK_NEEDS_REDO) {
        char *maindata = XLogBlockDataGetMainData(datadecode, NULL);
        HeapXlogBaseShiftOperatorPage(bufferinfo, (void *)maindata);
        MakeRedoBufferDirty(bufferinfo);
    }
}

static void HeapXlogNewpageBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;
    XLogRedoAction action;

    action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action != BLK_RESTORED)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                        errmsg("HeapXlogNewpageBlock unexpected result when restoring backup block")));
}

static void HeapXlogLockBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;
    XLogRedoAction action;

    action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action == BLK_NEEDS_REDO) {
        char *maindata = XLogBlockDataGetMainData(datadecode, NULL);
        bool isTupleLockUpgrade = (XLogBlockHeadGetInfo(blockhead) & XLOG_TUPLE_LOCK_UPGRADE_FLAG) != 0;

        HeapXlogLockOperatorPage(bufferinfo, (void *)maindata, isTupleLockUpgrade);
        MakeRedoBufferDirty(bufferinfo);
    }
}

static void HeapXlogInplaceBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;
    XLogRedoAction action;

    action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action == BLK_NEEDS_REDO) {
        Size blkdatalen;
        char *blkdata = XLogBlockDataGetBlockData(datadecode, &blkdatalen);
        char *maindata = XLogBlockDataGetMainData(datadecode, NULL);

        Assert(blkdata != NULL);
        HeapXlogInplaceOperatorPage(bufferinfo, (void *)maindata, (void *)blkdata, blkdatalen);
        MakeRedoBufferDirty(bufferinfo);
    }
}

void HeapRedoDataBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    uint8 info = XLogBlockHeadGetInfo(blockhead) & ~XLR_INFO_MASK;

    switch (info & XLOG_HEAP_OPMASK) {
        case XLOG_HEAP_INSERT:
            HeapXlogInsertBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_HEAP_DELETE:
            HeapXlogDeleteBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_HEAP_UPDATE:
        case XLOG_HEAP_HOT_UPDATE:
            HeapXlogUpdateBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_HEAP_BASE_SHIFT:
            HeapXlogBaseShiftBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_HEAP_NEWPAGE:
            HeapXlogNewpageBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_HEAP_LOCK:
            HeapXlogLockBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_HEAP_INPLACE:
            HeapXlogInplaceBlock(blockhead, blockdatarec, bufferinfo);
            break;
        default:
            ereport(PANIC, (errmsg("HeapRedoDataBlock: unknown op code %u", info)));
    }
}

void HeapRedoVmBlock(XLogBlockHead *blockhead, XLogBlockVmParse *blockvmrec, RedoBufferInfo *bufferinfo)
{
    uint8 info = XLogBlockHeadGetInfo(blockhead) & ~XLR_INFO_MASK;

    XLogBlockVmParse *blockvm = blockvmrec;
    switch (info & XLOG_HEAP_OPMASK) {
        case XLOG_HEAP_INSERT:
        case XLOG_HEAP_DELETE:
        case XLOG_HEAP_UPDATE:
        case XLOG_HEAP_HOT_UPDATE:
            HeapXlogClearVmBlock(blockvm, bufferinfo);
            break;
        default:
            ereport(PANIC, (errmsg("HeapRedoVmBlock: unknown op code %u", info)));
    }
}

static void HeapXlogFreezeBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;
    XLogRedoAction action;

    action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);

    if (action == BLK_NEEDS_REDO) {
        Size blkdatalen;
        char *blkdata = NULL;
        char *maindata = XLogBlockDataGetMainData(datadecode, NULL);

        blkdata = XLogBlockDataGetBlockData(datadecode, &blkdatalen);
        Assert(blkdata != NULL);
        HeapXlogFreezeOperatorPage(bufferinfo, (void *)maindata, (void *)blkdata, blkdatalen, false);
        MakeRedoBufferDirty(bufferinfo);
    }
}

static void HeapXlogInvalidBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;
    XLogRedoAction action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action == BLK_NEEDS_REDO) {
        Size blkdatalen;
        char *blkdata = XLogBlockDataGetBlockData(datadecode, &blkdatalen);
        Assert(blkdata != NULL);
        HeapXlogInvalidOperatorPage(bufferinfo, (void *)blkdata, blkdatalen);
        MakeRedoBufferDirty(bufferinfo);
    }
}

static void HeapXlogCleanBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;
    XLogRedoAction action;
    bool repairFragmentation = true;

    action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);

    if ((XLogBlockHeadGetInfo(blockhead) & XLOG_HEAP2_NO_REPAIR_PAGE) != 0) {
        repairFragmentation = false;
    }

    if (action == BLK_NEEDS_REDO) {
        Size blkdatalen;
        char *blkdata = NULL;
        char *maindata = XLogBlockDataGetMainData(datadecode, NULL);

        blkdata = XLogBlockDataGetBlockData(datadecode, &blkdatalen);
        Assert(blkdata != NULL);
        HeapXlogCleanOperatorPage(bufferinfo, (void *)maindata, (void *)blkdata, blkdatalen, NULL, repairFragmentation);
        MakeRedoBufferDirty(bufferinfo);
    }
}

static void HeapXlogVisibleBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;
    XLogRedoAction action;

    action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action == BLK_NEEDS_REDO) {
        char *maindata = XLogBlockDataGetMainData(datadecode, NULL);
        if (XLogBlockDataGetBlockId(datadecode) == HEAP_VISIBLE_VM_BLOCK_NUM) {
            HeapXlogVisibleOperatorVmbuffer(bufferinfo, (void *)maindata);
        } else {
            HeapXlogVisibleOperatorPage(bufferinfo, (void *)maindata);
        }
        MakeRedoBufferDirty(bufferinfo);
    }
}

static void HeapXlogMultiInsertBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec,
                                     RedoBufferInfo *bufferinfo)
{
    bool isinit = (XLogBlockHeadGetInfo(blockhead) & XLOG_HEAP_INIT_PAGE) != 0;
    bool tde = ((blockdatarec->blockhead.cur_block_id) & BKID_HAS_TDE_PAGE) != 0;
    TransactionId recordxid = XLogBlockHeadGetXid(blockhead);
    XLogBlockDataParse *datadecode = blockdatarec;

    XLogRedoAction action;
    action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);

    if (action == BLK_NEEDS_REDO) {
        char *maindata = XLogBlockDataGetMainData(datadecode, NULL);
        Size blkdatalen;
        char *blkdata = NULL;

        blkdata = XLogBlockDataGetBlockData(datadecode, &blkdatalen);
        Assert(blkdata != NULL);
        HeapXlogMultiInsertOperatorPage(bufferinfo, (void *)maindata, isinit, (void *)blkdata, blkdatalen, recordxid,
                                        NULL, tde);
        MakeRedoBufferDirty(bufferinfo);
    }
}

void Heap2RedoDataBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    uint8 info = XLogBlockHeadGetInfo(blockhead) & ~XLR_INFO_MASK;

    switch (info & XLOG_HEAP_OPMASK) {
        case XLOG_HEAP2_FREEZE:
            HeapXlogFreezeBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_HEAP2_CLEAN:
            HeapXlogCleanBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_HEAP2_VISIBLE:
            HeapXlogVisibleBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_HEAP2_MULTI_INSERT:
            HeapXlogMultiInsertBlock(blockhead, blockdatarec, bufferinfo);
            break;
        default:
            ereport(PANIC, (errmsg("heap2_redo_block: unknown op code %u", info)));
    }
}

void Heap2RedoVmBlock(XLogBlockHead *blockhead, XLogBlockVmParse *blockvmrec, RedoBufferInfo *bufferinfo)
{
    uint8 info = XLogBlockHeadGetInfo(blockhead) & ~XLR_INFO_MASK;

    XLogBlockVmParse *blockvm = blockvmrec;
    switch (info & XLOG_HEAP_OPMASK) {
        case XLOG_HEAP2_MULTI_INSERT:
            HeapXlogClearVmBlock(blockvm, bufferinfo);
            break;
        default:
            ereport(PANIC, (errmsg("Heap2RedoVmBlock: unknown op code %u", info)));
    }
}

XLogRecParseState *Heap3RedoParseToBlock(XLogReaderState *record, uint32 *blocknum)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    XLogRecParseState *recordblockstate = NULL;

    *blocknum = 0;
    switch (info & XLOG_HEAP_OPMASK) {
        case XLOG_HEAP3_NEW_CID:
            break;
        case XLOG_HEAP3_REWRITE:
            break;
        case XLOG_HEAP3_INVALID:
            recordblockstate = HeapXlogInvalidParseBlock(record, blocknum);
            break;
        default:
            ereport(PANIC, (errmsg("Heap3RedoParseToBlock: unknown op code %u", info)));
    }

    return recordblockstate;
}

void Heap3RedoDataBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    uint8 info = XLogBlockHeadGetInfo(blockhead) & ~XLR_INFO_MASK;

    switch (info & XLOG_HEAP_OPMASK) {
        case XLOG_HEAP3_NEW_CID:
            break;
        case XLOG_HEAP3_REWRITE:
            break;
        case XLOG_HEAP3_INVALID:
            HeapXlogInvalidBlock(blockhead, blockdatarec, bufferinfo);
            break;
        default:
            ereport(PANIC, (errmsg("heap3_redo_block: unknown op code %u", info)));
    }
}
