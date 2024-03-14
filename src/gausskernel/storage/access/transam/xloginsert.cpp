/* -------------------------------------------------------------------------
 *
 * xloginsert.cpp
 *        Functions for constructing WAL records
 *
 * Constructing a WAL record begins with a call to XLogBeginInsert,
 * followed by a number of XLogRegister* calls. The registered data is
 * collected in private working memory, and finally assembled into a chain
 * of XLogRecData structs by a call to XLogRecordAssemble(). See
 * access/transam/README for details.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/transam/xloginsert.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/visibilitymap.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xloginsert.h"
#include "catalog/pg_control.h"
#include "miscadmin.h"
#include "storage/buf/bufmgr.h"
#include "storage/proc.h"
#include "storage/smgr/segment.h"
#include "utils/memutils.h"
#include "utils/guc.h"
#include "pg_trace.h"
#include "replication/logical.h"
#include "replication/ss_disaster_cluster.h"
#include "pgstat.h"
#include "access/ustore/knl_upage.h"

/*
 * For each block reference registered with XLogRegisterBuffer, we fill in
 * a registered_buffer struct.
 */
typedef struct registered_buffer {
    bool in_use;       /* is this slot in use? */
    uint8 flags;       /* REGBUF_* flags */
    RelFileNode rnode; /* identifies the relation and block */
    ForkNumber forkno;
    BlockNumber block;
    uint8 seg_file_no;  /* segment-page file number (extent type) */
    BlockNumber seg_block;  /* block number in the segment-page file */
    Page page;               /* page content */
    uint32 rdata_len;        /* total length of data in rdata chain */
    XLogRecData *rdata_head; /* head of the chain of data registered with this block */
    XLogRecData *rdata_tail; /* last entry in the chain, or &rdata_head if empty */
    XLogRecPtr lastLsn;
    uint16 extra_flag;
    XLogRecData bkp_rdatas[2]; /* temporary rdatas used to hold references to
                                * backup block data in XLogRecordAssemble() */
    TdeInfo* tdeinfo;
    bool encrypt;
} registered_buffer;

#define SizeOfXlogOrigin (sizeof(RepOriginId) + sizeof(char))

#define HEADER_SCRATCH_SIZE \
    (SizeOfXLogRecord + MaxSizeOfXLogRecordBlockHeader * (XLR_MAX_BLOCK_ID + 1) + \
    SizeOfXLogRecordDataHeaderLong + SizeOfXlogOrigin)

static XLogRecData *XLogRecordAssemble(RmgrId rmid, uint8 info, XLogFPWInfo fpw_info, XLogRecPtr *fpw_lsn,
                                       int bucket_id = -1, bool istoast = false);
static void XLogResetLogicalPage(void);

/*
 * Begin constructing a WAL record. This must be called before the
 * XLogRegister* functions and XLogInsert().
 */
void XLogBeginInsert(void)
{
    if (SS_PERFORMING_SWITCHOVER) {
        XLogResetInsertion();
    }
    Assert(t_thrd.xlog_cxt.max_registered_block_id == 0);
    Assert(t_thrd.xlog_cxt.mainrdata_last == (XLogRecData *)&t_thrd.xlog_cxt.mainrdata_head);
    Assert(t_thrd.xlog_cxt.mainrdata_len == 0);
    Assert(!(SS_CLUSTER_ONDEMAND_RECOVERY && SS_STANDBY_MODE));

    /* cross-check on whether we should be here or not */
    if (!XLogInsertAllowed())
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("recovery is in progress"),
                        errhint("cannot make new WAL entries during recovery")));

    if (SECUREC_UNLIKELY(t_thrd.xlog_cxt.begininsert_called))
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("XLogBeginInsert was already called")));

    if (SS_DORADO_STANDBY_CLUSTER && !g_instance.dms_cxt.SSRecoveryInfo.disaster_cluster_promoting) {
        ereport(LOG, (errmsg("SS dorado standby cluster cannot insert XLOG entries")));
        return;
    }

    t_thrd.xlog_cxt.begininsert_called = true;
}

/*
 * Ensure that there are enough buffer and data slots in the working area,
 * for subsequent XLogRegisterBuffer, XLogRegisterData and XLogRegisterBufData
 * calls.
 *
 * There is always space for a small number of buffers and data chunks, enough
 * for most record types. This function is for the exceptional cases that need
 * more.
 */
void XLogEnsureRecordSpace(int max_block_id, int ndatas)
{
    int nbuffers;
    errno_t rc = EOK;

    /*
     * This must be called before entering a critical section, because
     * allocating memory inside a critical section can fail. repalloc() will
     * check the same, but better to check it here too so that we fail
     * consistently even if the arrays happen to be large enough already.
     */
    Assert(t_thrd.int_cxt.CritSectionCount == 0);

    /* the minimum values can't be decreased */
    if (max_block_id < XLR_NORMAL_MAX_BLOCK_ID)
        max_block_id = XLR_NORMAL_MAX_BLOCK_ID;
    if (ndatas < XLR_NORMAL_RDATAS)
        ndatas = XLR_NORMAL_RDATAS;

    if (SECUREC_UNLIKELY(max_block_id > XLR_MAX_BLOCK_ID))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("maximum number of WAL record block references exceeded")));

    nbuffers = max_block_id + 1;

    if (nbuffers > t_thrd.xlog_cxt.max_registered_buffers) {
        t_thrd.xlog_cxt.registered_buffers = (registered_buffer *)repalloc(t_thrd.xlog_cxt.registered_buffers,
                                                                           sizeof(registered_buffer) * nbuffers);

        /*
         * At least the padding bytes in the structs must be zeroed, because
         * they are included in WAL data, but initialize it all for tidiness.
         */
        rc = memset_s(&t_thrd.xlog_cxt.registered_buffers[t_thrd.xlog_cxt.max_registered_buffers],
                      (nbuffers - t_thrd.xlog_cxt.max_registered_buffers) * sizeof(registered_buffer), 0,
                      (nbuffers - t_thrd.xlog_cxt.max_registered_buffers) * sizeof(registered_buffer));
        securec_check(rc, "", "");
        t_thrd.xlog_cxt.max_registered_buffers = nbuffers;
    }

    if (ndatas > t_thrd.xlog_cxt.max_rdatas) {
        t_thrd.xlog_cxt.rdatas = (XLogRecData *)repalloc(t_thrd.xlog_cxt.rdatas, sizeof(XLogRecData) * ndatas);
        t_thrd.xlog_cxt.max_rdatas = ndatas;
    }
}

/*
 * Reset page logical flag if any
 *
 * The insertation caller should have held the buffer lock and dirtied the buffer
 * already, so it's safe here to reset the page flag directly.
 */
static void XLogResetLogicalPage(void)
{
    int i;

    Assert(t_thrd.xlog_cxt.begininsert_called);

    for (i = 0; i < t_thrd.xlog_cxt.max_registered_block_id; i++) {
        registered_buffer *regbuf = &t_thrd.xlog_cxt.registered_buffers[i];

        if (!regbuf->in_use)
            continue;

        if (PageIsLogical(regbuf->page))
            PageClearLogical(regbuf->page);

        regbuf->in_use = false;
        regbuf->seg_block = 0;
        regbuf->seg_file_no = 0;
    }

    t_thrd.xlog_cxt.num_rdatas = 0;
    t_thrd.xlog_cxt.max_registered_block_id = 0;
    t_thrd.xlog_cxt.mainrdata_len = 0;
    t_thrd.xlog_cxt.mainrdata_last = (XLogRecData *)&t_thrd.xlog_cxt.mainrdata_head;
    t_thrd.xlog_cxt.include_origin = false;
    t_thrd.xlog_cxt.begininsert_called = false;
}

/*
 * Reset WAL record construction buffers.
 */
void XLogResetInsertion(void)
{
    int i;

    for (i = 0; i < t_thrd.xlog_cxt.max_registered_block_id; i++) {
        registered_buffer *rbuf = &t_thrd.xlog_cxt.registered_buffers[i];
        rbuf->in_use = false;
        rbuf->seg_block = 0;
        rbuf->seg_file_no = 0;
    }

    t_thrd.xlog_cxt.num_rdatas = 0;
    t_thrd.xlog_cxt.max_registered_block_id = 0;
    t_thrd.xlog_cxt.mainrdata_len = 0;
    t_thrd.xlog_cxt.mainrdata_last = (XLogRecData *)&t_thrd.xlog_cxt.mainrdata_head;
    t_thrd.xlog_cxt.include_origin = false;
    t_thrd.xlog_cxt.begininsert_called = false;
}

/*
 * Register a reference to a buffer with the WAL record being constructed.
 * This must be called for every page that the WAL-logged operation modifies.
 */
void XLogRegisterBuffer(uint8 block_id, Buffer buffer, uint8 flags, TdeInfo* tdeinfo)
{
    registered_buffer *regbuf = NULL;

    /* NO_IMAGE doesn't make sense with FORCE_IMAGE */
    Assert(!((flags & REGBUF_FORCE_IMAGE) && (flags & (REGBUF_NO_IMAGE))));
    Assert(t_thrd.xlog_cxt.begininsert_called);

    if (block_id >= t_thrd.xlog_cxt.max_registered_block_id) {
        if (block_id >= t_thrd.xlog_cxt.max_registered_buffers)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("too many registered buffers")));
        t_thrd.xlog_cxt.max_registered_block_id = block_id + 1;
    }

    regbuf = &t_thrd.xlog_cxt.registered_buffers[block_id];

    BufferGetTag(buffer, &regbuf->rnode, &regbuf->forkno, &regbuf->block);
    regbuf->page = BufferGetPage(buffer);
    regbuf->flags = flags;
    regbuf->rdata_tail = (XLogRecData *)&regbuf->rdata_head;
    regbuf->rdata_len = 0;
    regbuf->lastLsn = PageGetLSN(regbuf->page);
    if (PageIsJustAfterFullPageWrite(regbuf->page)) {
        regbuf->lastLsn = InvalidXLogRecPtr;
        PageClearJustAfterFullPageWrite(regbuf->page);
    }
    regbuf->extra_flag = 0;
    regbuf->encrypt = false;

    if (PageIsTDE(regbuf->page) && tdeinfo && tdeinfo->algo != 0) {
        TdeInfo* owntdeinfo = (TdeInfo*)palloc(sizeof(TdeInfo));
        errno_t rc = EOK;
        rc = memcpy_s((void*)owntdeinfo, sizeof(TdeInfo), (void*)tdeinfo, sizeof(TdeInfo));
        securec_check(rc, "", "");
        regbuf->encrypt = true;
        regbuf->tdeinfo = owntdeinfo;
    }

    if (XLOG_NEED_PHYSICAL_LOCATION(regbuf->rnode)) {
        if (!BufferIsValid(buffer) || BufferIsLocal(buffer)) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("buffer is not valid")));
            return;
        }
        BufferDesc *bufDesc = GetBufferDescriptor(buffer - 1);
        if (bufDesc->extra->seg_fileno != EXTENT_INVALID) {  
            // buffer descriptor contains the physical location
            SegmentCheck(bufDesc->extra->seg_fileno <= EXTENT_TYPES);
            regbuf->seg_file_no = bufDesc->extra->seg_fileno;
            regbuf->seg_block = bufDesc->extra->seg_blockno;
        } else {
            SegPageLocation loc = seg_get_physical_location(regbuf->rnode, regbuf->forkno, regbuf->block);
            SegmentCheck(loc.blocknum != InvalidBlockNumber);

            regbuf->seg_file_no = (uint8) EXTENT_SIZE_TO_TYPE(loc.extent_size);
            regbuf->seg_block = loc.blocknum;
        }
    }
    /*
     * Check that this page hasn't already been registered with some other
     * block_id.
     */
#ifdef USE_ASSERT_CHECKING
    {
        int i;

        for (i = 0; i < t_thrd.xlog_cxt.max_registered_block_id; i++) {
            registered_buffer *regbuf_old = &t_thrd.xlog_cxt.registered_buffers[i];

            if (i == block_id || !regbuf_old->in_use)
                continue;

            Assert(!RelFileNodeEquals(regbuf_old->rnode, regbuf->rnode) || regbuf_old->forkno != regbuf->forkno ||
                   regbuf_old->block != regbuf->block);
        }
    }
#endif

    regbuf->in_use = true;
}

/*
 * Like XLogRegisterBuffer, but for registering a block that's not in the
 * shared buffer pool (i.e. when you don't have a Buffer for it).
 */
void XLogRegisterBlock(uint8 block_id, RelFileNode *rnode, ForkNumber forknum, BlockNumber blknum, Page page,
                       uint8 flags, const XLogPhyBlock *pblk, TdeInfo* tdeinfo)
{
    registered_buffer *regbuf = NULL;

    /* This is currently only used to WAL-log a full-page image of a page */
    Assert(flags & REGBUF_FORCE_IMAGE);
    Assert(t_thrd.xlog_cxt.begininsert_called);

    if (block_id >= t_thrd.xlog_cxt.max_registered_block_id)
        t_thrd.xlog_cxt.max_registered_block_id = block_id + 1;

    if (block_id >= t_thrd.xlog_cxt.max_registered_buffers)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("too many registered buffers")));

    regbuf = &t_thrd.xlog_cxt.registered_buffers[block_id];

    regbuf->rnode = *rnode;
    regbuf->forkno = forknum;
    regbuf->block = blknum;
    regbuf->page = page;
    regbuf->flags = flags;
    regbuf->rdata_tail = (XLogRecData *)&regbuf->rdata_head;
    regbuf->rdata_len = 0;
    regbuf->lastLsn = PageGetLSN(regbuf->page);
    if (PageIsJustAfterFullPageWrite(regbuf->page)) {
        regbuf->lastLsn = InvalidXLogRecPtr;
        PageClearJustAfterFullPageWrite(regbuf->page);
    }
    regbuf->extra_flag = 0;
    regbuf->encrypt = false;
    if ((tdeinfo && tdeinfo->algo != 0)) {
        regbuf->encrypt = true;
        regbuf->tdeinfo = tdeinfo;
    }

    if (XLOG_NEED_PHYSICAL_LOCATION(*rnode)) {
        if (pblk != NULL) {
            regbuf->seg_file_no = pblk->relNode;
            regbuf->seg_block = pblk->block;
        } else {
            SegPageLocation loc = seg_get_physical_location(*rnode, forknum, blknum);
            SegmentCheck(loc.blocknum != InvalidBlockNumber);

            regbuf->seg_file_no = (uint8)EXTENT_SIZE_TO_TYPE(loc.extent_size);
            regbuf->seg_block = loc.blocknum;
        }
    }
    /*
     * Check that this page hasn't already been registered with some other
     * block_id.
     */
#ifdef USE_ASSERT_CHECKING
    {
        int i;

        for (i = 0; i < t_thrd.xlog_cxt.max_registered_block_id; i++) {
            registered_buffer *regbuf_old = &t_thrd.xlog_cxt.registered_buffers[i];

            if (i == block_id || !regbuf_old->in_use)
                continue;

            Assert(!RelFileNodeEquals(regbuf_old->rnode, regbuf->rnode) || regbuf_old->forkno != regbuf->forkno ||
                   regbuf_old->block != regbuf->block);
        }
    }
#endif

    regbuf->in_use = true;
}

/*
 * Add data to the WAL record that's being constructed.
 *
 * The data is appended to the "main chunk",
 * available at replay with XLogRecGetData().
 */
void XLogRegisterData(char *data, int len)
{
    XLogRecData *rdata = NULL;

    Assert(t_thrd.xlog_cxt.begininsert_called);

    if (t_thrd.xlog_cxt.num_rdatas >= t_thrd.xlog_cxt.max_rdatas)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("too much WAL data")));
    rdata = &t_thrd.xlog_cxt.rdatas[t_thrd.xlog_cxt.num_rdatas++];

    rdata->data = data;
    rdata->len = len;

    /*
     * we use the mainrdata_last pointer to track the end of the chain, so no
     * need to clear 'next' here.
     */
    t_thrd.xlog_cxt.mainrdata_last->next = rdata;
    t_thrd.xlog_cxt.mainrdata_last = rdata;

    t_thrd.xlog_cxt.mainrdata_len += len;
}

/*
 * Should this record include the replication origin if one is set up?
 */
void XLogIncludeOrigin(void)
{
    Assert(t_thrd.xlog_cxt.begininsert_called);
    t_thrd.xlog_cxt.include_origin = true;
}

/*
 * Add buffer-specific data to the WAL record that's being constructed.
 *
 * Block_id must reference a block previously registered with
 * XLogRegisterBuffer(). If this is called more than once for the same
 * block_id, the data is appended.
 *
 * The maximum amount of data that can be registered per block is 65535
 * bytes. That should be plenty; if you need more than BLCKSZ bytes to
 * reconstruct the changes to the page, you might as well just log a full
 * copy of it. (the "main data" that's not associated with a block is not
 * limited)
 */
void XLogRegisterBufData(uint8 block_id, char *data, int len)
{
    registered_buffer *regbuf = NULL;
    XLogRecData *rdata = NULL;

    Assert(t_thrd.xlog_cxt.begininsert_called);

    /* find the registered buffer struct */
    regbuf = &t_thrd.xlog_cxt.registered_buffers[block_id];
    if (!regbuf->in_use)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("no block with id %d registered with WAL insertion", block_id)));

    if (t_thrd.xlog_cxt.num_rdatas >= t_thrd.xlog_cxt.max_rdatas)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("too much WAL data")));

    rdata = &t_thrd.xlog_cxt.rdatas[t_thrd.xlog_cxt.num_rdatas++];

    rdata->data = data;
    rdata->len = len;

    regbuf->rdata_tail->next = rdata;
    regbuf->rdata_tail = rdata;
    regbuf->rdata_len += len;
}

void XLogInsertTrace(RmgrId rmid, uint8 info, XLogRecPtr EndPos)
{
    int block_id;

    ereport(DEBUG4,
            (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
             errmsg("[REDO_LOG_TRACE]XLogInsert: ProcLastRecPtr:%lu,XactLastRecEnd:%lu,"
                    "rmid:%hhu,info:%hhu,newPageLsn:%lu,",
                    t_thrd.xlog_cxt.ProcLastRecPtr, t_thrd.xlog_cxt.XactLastRecEnd, rmid, info, EndPos)));
    /* 'block_id<3'seems to be enough for problem location! */
    for (block_id = 0; block_id < t_thrd.xlog_cxt.max_registered_block_id && block_id < 3; block_id++) {
        registered_buffer *regbuf = &t_thrd.xlog_cxt.registered_buffers[block_id];
        Page page = regbuf->page;
        if (!regbuf->in_use)
            continue;
        ereport(DEBUG4,
                (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                 errmsg("[REDO_LOG_TRACE]XLogInsert: block_id:%d,oldPageLsn:%lu,"
                        "relRnode(spcNode:%u, dbNode:%u, relNode:%u),block:%u,"
                        "pd_lower:%hu, pd_upper:%hu",
                        block_id, PageGetLSN(page), regbuf->rnode.spcNode, regbuf->rnode.dbNode, regbuf->rnode.relNode,
                        regbuf->block, ((PageHeader)page)->pd_lower, ((PageHeader)page)->pd_upper)));
    }
}

void XlogInsertSleep(void)
{
    if (g_instance.streaming_dr_cxt.rpoSleepTime > 0) {
        pgstat_report_waitevent(WAIT_EVENT_LOGCTRL_SLEEP);
        if (g_instance.streaming_dr_cxt.rpoSleepTime < MAX_RPO_SLEEP_TIME) {
            pg_usleep_retry(g_instance.streaming_dr_cxt.rpoSleepTime, 0);
        } else {
            pg_usleep_retry(MAX_RPO_SLEEP_TIME, 0);
        }
        pgstat_report_waitevent(WAIT_EVENT_END);
    }
}

/*
 * Insert an XLOG record having the specified RMID and info bytes,
 * with the body of the record being the data chunk(s) described by
 * the rdata chain (see xloginsert.h for notes about rdata).
 *
 * Returns XLOG pointer to end of record (beginning of next record).
 * This can be used as LSN for data pages affected by the logged action.
 * (LSN is the XLOG point up to which the XLOG must be flushed to disk
 * before the data page can be written out.  This implements the basic
 * WAL rule "write the log before the data".)
 *
 * NB: this routine feels free to scribble on the XLogRecData structs,
 * though not on the data they reference.  This is OK since the XLogRecData
 * structs are always just temporaries in the calling code.
 */
XLogRecPtr XLogInsert(RmgrId rmid, uint8 info, int bucket_id, bool istoast)
{
    XLogRecPtr EndPos;
    bool isSwitchoverBarrier = ((rmid == RM_BARRIER_ID) && (info == XLOG_BARRIER_SWITCHOVER));
    if (g_instance.archive_obs_cxt.in_switchover == true || 
        g_instance.streaming_dr_cxt.isInSwitchover == true) {
        LWLockAcquire(HadrSwitchoverLock, LW_EXCLUSIVE);
        if (isSwitchoverBarrier)
            t_thrd.storage_cxt.isSwitchoverLockHolder = true;
    }

    XlogInsertSleep();

    /*
     * The caller can set rmgr bits and XLR_SPECIAL_REL_UPDATE; the rest are
     * reserved for use by me.
     */
    if ((info & ~(XLR_RMGR_INFO_MASK | XLR_SPECIAL_REL_UPDATE |
         XLR_BTREE_UPGRADE_FLAG | XLR_REL_COMPRESS | XLR_IS_TOAST)) != 0) {
        ereport(PANIC, (errmsg("invalid xlog info mask %hhx", info)));
    }

    TRACE_POSTGRESQL_XLOG_INSERT(rmid, info);

    /*
     * In bootstrap mode, we don't actually log anything but XLOG resources;
     * return a phony record pointer.
     */
    if (IsBootstrapProcessingMode() && rmid != RM_XLOG_ID) {
        XLogResetInsertion();
        EndPos = SizeOfXLogLongPHD; /* start of 1st chkpt record */
        return EndPos;
    }

    do {
        XLogRecPtr fpw_lsn;
        XLogFPWInfo fpw_info;
        XLogRecData *rdt = NULL;

        /*
         * Get values needed to decide whether to do full-page writes. Since we
         * don't yet have an insertion lock, these could change under us, but
         * XLogInsertRecord will recheck them once it has a lock.
         */
        GetFullPageWriteInfo(&fpw_info);

        rdt = XLogRecordAssemble(rmid, info, fpw_info, &fpw_lsn, bucket_id, istoast);

        EndPos = XLogInsertRecord(rdt, fpw_lsn);
    } while (XLByteEQ(EndPos, InvalidXLogRecPtr));

    /*
     * too much log may slow down the speed of xlog, so only write log
     * when log level belows DEBUG4
     */
    if (module_logging_is_on(MOD_REDO)) {
        XLogInsertTrace(rmid, info, EndPos);
    }

    /*
     * Great! We have inserted all the request information into WAL. The last
     * thing needs to do is that we should cleanup the logical flag of the
     * backuped page if any. Caller must hold the buffer lock.
     */
    XLogResetLogicalPage();
    
    /* Switchover Barrier log will not release the lock */
    if ((g_instance.archive_obs_cxt.in_switchover == true || 
        g_instance.streaming_dr_cxt.isInSwitchover == true)) {
        if (!isSwitchoverBarrier) {
            LWLockRelease(HadrSwitchoverLock);
        } else {
            t_thrd.xlog_cxt.LocalXLogInsertAllowed = 0;
        }
    }

    return EndPos;
}

/*
 * This marco is used to get correct XLogRecData of heap Update/Delete/Insert, only used in function
 * XLogNeedVMPhysicalLocation. DONNOT change variable name "rdata" in XLogNeedVMPhysicalLocation!!!
 */
#define XLOG_GET_UDI_RDATA(type) \
            Assert(t_thrd.xlog_cxt.mainrdata_len != 0); \
            if (isinit) { \
                Assert(rdata->next != NULL); \
                rdata = rdata->next; \
            } \
            Assert(rdata->len > 0); \
            type *xlrec = (type *)(rdata->data) \

/*
 * Clearing a visibility map bit is WAL-logged together with heap_insert/heap_update/heap_delete.
 * Tools like Roach can calculate modified VM block according to heap block in heap-disk, which is
 * not possible in segment-page storage. Thus, we need to add additional VM physical location if
 * necessary 
 */
static bool XLogNeedVMPhysicalLocation(RmgrId rmi, uint8 info, int blockId)
{
    bool isinit = info & XLOG_HEAP_INIT_PAGE;
    XLogRecData *rdata = t_thrd.xlog_cxt.mainrdata_head;
    
    if (rmi == RM_HEAP_ID) {
        switch (info & XLOG_HEAP_OPMASK) {
            case XLOG_HEAP_INSERT: {
                XLOG_GET_UDI_RDATA(xl_heap_insert);
                Assert(blockId == 0);

                return (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED) != 0;
                break;
            }
            case XLOG_HEAP_DELETE: {
                XLOG_GET_UDI_RDATA(xl_heap_delete);
                Assert(blockId == 0);

                return (xlrec->flags & XLH_DELETE_ALL_VISIBLE_CLEARED) != 0;
                break;
            }
            case XLOG_HEAP_UPDATE:
            case XLOG_HEAP_HOT_UPDATE: {
                XLOG_GET_UDI_RDATA(xl_heap_update);

                if (blockId == 0) {
                    if (t_thrd.xlog_cxt.registered_buffers[1].in_use) {
                        return (xlrec->flags & XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED) != 0;
                    } else {
                        return (xlrec->flags & XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED) != 0;
                    }
                } else if (blockId == 1) {
                    return (xlrec->flags & XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED) != 0;
                } else {
                    Assert(0);
                }
                break;
            }
            default:
                break;
        };
    } else if (rmi == RM_HEAP2_ID) {
        if ((info & XLOG_HEAP_OPMASK) == XLOG_HEAP2_MULTI_INSERT) {
            XLOG_GET_UDI_RDATA(xl_heap_multi_insert);
            
            Assert(blockId == 0);
            return (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED) != 0;
        }
    }
    return false;
}

/* This macro can be only used in XLogRecordAssemble to assemble on variable into xlog */
#define XLOG_ASSEMBLE_ONE_ITEM(scratch, size, src, remained_size) \
    do { \
        rc = memcpy_s(scratch, remained_size, src, size); \
        securec_check(rc, "", ""); \
        (scratch) += (size); \
        (remained_size) -= (size); \
    } while (0)

/*
 * Assemble a WAL record from the registered data and buffers into an
 * XLogRecData chain, ready for insertion with XLogInsertRecord().
 *
 * The record header fields are filled in, except for the xl_prev field. The
 * calculated CRC does not include the record header yet.
 *
 * If there are any registered buffers, and a full-page image was not taken
 * of all of them, *fpw_lsn is set to the lowest LSN among such pages. This
 * signals that the assembled record is only good for insertion on the
 * assumption that the RedoRecPtr and doPageWrites values were up-to-date.
 * 
 * After introducing segment-page storage, BlockNumber and RelFileNode in xlog
 * are logical block number and relation file node. In some scenario, e.g.,
 * incremental buiild and incremental backup (Roach), these tools need to parse
 * xlog directly to get the physical locations of modified  blocks.
 * However, the block map tree (BMT) of related segment may be recycled at the
 * moment these tools parsing the xlog. Thus, we add physical locaiton info
 * (file + blocknumber, 5 Bytes in total) for each block in a xlog.
 * 
 */
static XLogRecData *XLogRecordAssemble(RmgrId rmid, uint8 info, XLogFPWInfo fpw_info, XLogRecPtr *fpw_lsn,
    int bucket_id, bool istoast)
{
    XLogRecData *rdt = NULL;
    uint32 total_len = 0;
    int block_id;
    pg_crc32c rdata_crc; /* pg_crc32c is same as pg_crc32 */
    registered_buffer *prev_regbuf = NULL;
    XLogRecData *rdt_datas_last = NULL;
    XLogRecord *rechdr = NULL;
    char *scratch = t_thrd.xlog_cxt.hdr_scratch;
    ssize_t remained_size = HEADER_SCRATCH_SIZE;
    errno_t rc = EOK;
    bool hashbucket_flag = false;
    bool no_hashbucket_flag = false;
    /*
     * Note: this function can be called multiple times for the same record.
     * All the modifications we do to the rdata chains below must handle that.
     *
     * The record begins with the fixed-size header
     */
    rechdr = (XLogRecord *)scratch;
    scratch += SizeOfXLogRecord;

    t_thrd.xlog_cxt.ptr_hdr_rdt->next = NULL;
    rdt_datas_last = t_thrd.xlog_cxt.ptr_hdr_rdt;
    t_thrd.xlog_cxt.ptr_hdr_rdt->data = t_thrd.xlog_cxt.hdr_scratch;

    /*
     * Make an rdata chain containing all the data portions of all block
     * references. This includes the data for full-page images. Also append
     * the headers for the block references in the scratch buffer.
     */
    *fpw_lsn = InvalidXLogRecPtr;
    for (block_id = 0; block_id < t_thrd.xlog_cxt.max_registered_block_id; block_id++) {
        registered_buffer *regbuf = &t_thrd.xlog_cxt.registered_buffers[block_id];
        bool needs_backup = false;
        bool needs_data = false;
        XLogRecordBlockHeader bkpb;
        XLogRecordBlockImageHeader bimg;
        bool page_logical = false;
        bool samerel = false;
        bool tde = false;

        // must be uncompressed table during upgrade
        bool isCompressedTable = regbuf->rnode.opt != 0;
        if (t_thrd.proc->workingVersionNum < PAGE_COMPRESSION_VERSION) {
            Assert(!isCompressedTable);
        }

        if (!regbuf->in_use)
            continue;

        if (regbuf->encrypt) {
            tde = true;
        }

        /*
         * Determine if this block needs to be backed up. In redo routine, we treate
         * a REBUF_WILL_INIT buffer as a buffer needs redo. So do not take force
         * page write in this scenario.
         */
        page_logical = PageIsLogical(regbuf->page);
        fpw_info.forcePageWrites = fpw_info.forcePageWrites || page_logical;

        if (fpw_info.forcePageWrites && !(regbuf->flags & REGBUF_NO_IMAGE))
            needs_backup = true;
        else if (regbuf->flags & REGBUF_FORCE_IMAGE)
            needs_backup = true;
        else if (regbuf->flags & REGBUF_NO_IMAGE)
            needs_backup = false;
        else if (!fpw_info.doPageWrites)
            needs_backup = false;
        else {
            /*
             * We assume page LSN is first data on *every* page that can be
             * passed to XLogInsert, whether it has the standard page layout
             * or not.
             */
            XLogRecPtr page_lsn = PageGetLSN(regbuf->page);

            needs_backup = XLByteLE(page_lsn, fpw_info.redoRecPtr);
            if (!needs_backup) {
                if (XLByteEQ(*fpw_lsn, InvalidXLogRecPtr) || XLByteLT(page_lsn, *fpw_lsn))
                    *fpw_lsn = page_lsn;
            }
        }

        /* Determine if the buffer data needs to included */
        if (regbuf->rdata_len == 0)
            needs_data = false;
        else if ((regbuf->flags & REGBUF_KEEP_DATA) != 0)
            needs_data = true;
        else
            needs_data = !needs_backup;

        bkpb.id = block_id;
        bkpb.fork_flags = regbuf->forkno;
        bkpb.data_length = 0;

        if ((regbuf->flags & REGBUF_WILL_INIT) == REGBUF_WILL_INIT)
            bkpb.fork_flags |= BKPBLOCK_WILL_INIT;

        if (needs_backup) {
            Page page = regbuf->page;

            /* check for garbage data. for memory check, It means data modify error in memory. */
            /* Note: rmid check is not sufficient to determine the page layout.
             *       eg: MarkBufferDirtyHint() uses rmid RM_XLOG_ID regardless of the Page version.
             *       The problem with using PageGetPageLayoutVersion() is it is not self maintaining.
             *       If a new layout eg PG_UHEAP_PAGE_LAYOUT_VERSION_2 is added then the statement
             *       below also needs to be updated.
             */
            bool isPageValid = (PageGetPageLayoutVersion(page) == PG_UHEAP_PAGE_LAYOUT_VERSION) ?
                                UPageHeaderIsValid((UHeapPageHeaderData *) page) :
                                PageHeaderIsValid((PageHeader)page);
            if (!isPageValid) {
                ereport(PANIC,
                        (errmsg("invalid page header in block %u, spc oid %u db oid %u relfilenode %u", regbuf->block,
                                regbuf->rnode.spcNode, regbuf->rnode.dbNode, regbuf->rnode.relNode)));
            }

            /*
             * The page needs to be backed up, so set up *bimg
             */
            if (regbuf->flags & REGBUF_STANDARD) {
                /* Assume we can omit data between pd_lower and pd_upper */
                uint16 lower;
                uint16 upper;
                if (rmid == RM_UHEAP_ID) {
                    lower = ((UHeapPageHeaderData *)page)->pd_lower;
                    upper = ((UHeapPageHeaderData *)page)->pd_upper;
                } else {
                    lower = ((PageHeader)page)->pd_lower;
                    upper = ((PageHeader)page)->pd_upper;
                }

                if (lower >= GetPageHeaderSize(page) && upper > lower && upper <= BLCKSZ) {
                    bimg.hole_offset = lower;
                    bimg.hole_length = upper - lower;
                } else {
                    /* No "hole" to compress out */
                    bimg.hole_offset = 0;
                    bimg.hole_length = 0;
                }
            } else {
                /* Not a standard page header, don't try to eliminate "hole" */
                bimg.hole_offset = 0;
                bimg.hole_length = 0;
            }

            /* Fill in the remaining fields in the XLogRecordBlockData struct */
            bkpb.fork_flags |= BKPBLOCK_HAS_IMAGE;

            total_len += BLCKSZ - bimg.hole_length;

            /*
             * Construct XLogRecData entries for the page content.
             */
            rdt_datas_last->next = &regbuf->bkp_rdatas[0];
            rdt_datas_last = rdt_datas_last->next;
            if (bimg.hole_length == 0) {
                rdt_datas_last->data = page;
                rdt_datas_last->len = BLCKSZ;
            } else {
                /* must skip the hole */
                rdt_datas_last->data = page;
                rdt_datas_last->len = bimg.hole_offset;

                rdt_datas_last->next = &regbuf->bkp_rdatas[1];
                rdt_datas_last = rdt_datas_last->next;

                rdt_datas_last->data = page + (bimg.hole_offset + bimg.hole_length);
                rdt_datas_last->len = BLCKSZ - (bimg.hole_offset + bimg.hole_length);
            }
        }

        if (needs_data) {
            /*
             * Link the caller-supplied rdata chain for this buffer to the
             * overall list.
             */
            bkpb.fork_flags |= BKPBLOCK_HAS_DATA;
            bkpb.data_length = regbuf->rdata_len;
            total_len += regbuf->rdata_len;

            rdt_datas_last->next = regbuf->rdata_head;
            rdt_datas_last = regbuf->rdata_tail;
        }

        if (prev_regbuf && RelFileNodeEquals(regbuf->rnode, prev_regbuf->rnode)) {
            samerel = true;
            bkpb.fork_flags |= BKPBLOCK_SAME_REL;
        } else
            samerel = false;
        prev_regbuf = regbuf;

        if (!samerel && (IsSegmentFileNode(regbuf->rnode) || isCompressedTable)) {
            Assert(bkpb.id <= XLR_MAX_BLOCK_ID);
            bkpb.id += BKID_HAS_BUCKET_OR_SEGPAGE;
        }

        if (!samerel && tde) {
            bkpb.id += BKID_HAS_TDE_PAGE;
        }

        /* Ok, copy the header to the scratch buffer */
        XLOG_ASSEMBLE_ONE_ITEM(scratch, SizeOfXLogRecordBlockHeader, &bkpb, remained_size);
        if (needs_backup) {
            XLOG_ASSEMBLE_ONE_ITEM(scratch, SizeOfXLogRecordBlockImageHeader, &bimg, remained_size);
        }

        if (!samerel) {
            if (IsSegmentFileNode(regbuf->rnode) || isCompressedTable) {
                if (IsSegmentFileNode(regbuf->rnode)) {
                    XLOG_ASSEMBLE_ONE_ITEM(scratch, sizeof(RelFileNode), &regbuf->rnode, remained_size);
                    hashbucket_flag = true;
                } else if (t_thrd.proc->workingVersionNum < PAGE_COMPRESSION_VERSION) {
                    Assert(!isCompressedTable);
                    RelFileNodeV2 relFileNodeV2;
                    RelFileNodeV2Copy(relFileNodeV2, regbuf->rnode);
                    XLOG_ASSEMBLE_ONE_ITEM(scratch, sizeof(RelFileNodeV2), &regbuf->rnode, remained_size);
                } else {
                    info |= XLR_REL_COMPRESS;
                    XLOG_ASSEMBLE_ONE_ITEM(scratch, sizeof(RelFileNode), &regbuf->rnode, remained_size);
                }
            } else {
                XLOG_ASSEMBLE_ONE_ITEM(scratch, sizeof(RelFileNodeOld), &regbuf->rnode, remained_size);
                no_hashbucket_flag = true;
            }

            if (tde) {
                XLOG_ASSEMBLE_ONE_ITEM(scratch, sizeof(TdeInfo), regbuf->tdeinfo, remained_size);
            }
            XLOG_ASSEMBLE_ONE_ITEM(scratch, sizeof(uint16), &regbuf->extra_flag, remained_size);
        }

        XLOG_ASSEMBLE_ONE_ITEM(scratch, sizeof(BlockNumber), &regbuf->block, remained_size);
        /* Record physical block number for segment-page storage */
        if (XLOG_NEED_PHYSICAL_LOCATION(regbuf->rnode)) {
            SegmentCheck(regbuf->seg_file_no != EXTENT_INVALID);

            bool vm_loc = XLogNeedVMPhysicalLocation(rmid, info, block_id);
            if (vm_loc) {
                regbuf->seg_file_no |= BKPBLOCK_HAS_VM_LOC;
            }

            XLOG_ASSEMBLE_ONE_ITEM(scratch, sizeof(uint8), &regbuf->seg_file_no, remained_size);
            XLOG_ASSEMBLE_ONE_ITEM(scratch, sizeof(BlockNumber), &regbuf->seg_block, remained_size);

            if (vm_loc) {
                // logic block number of visibility map block
                BlockNumber vm_block = HEAPBLK_TO_MAPBLOCK(regbuf->block);  
                SegPageLocation loc = seg_get_physical_location(regbuf->rnode, VISIBILITYMAP_FORKNUM, vm_block);
                uint8 vm_segfileno = (uint8) EXTENT_SIZE_TO_TYPE(loc.extent_size);

                XLOG_ASSEMBLE_ONE_ITEM(scratch, sizeof(uint8), &vm_segfileno, remained_size);
                XLOG_ASSEMBLE_ONE_ITEM(scratch, sizeof(BlockNumber), &loc.blocknum, remained_size);
            }
        }
        XLOG_ASSEMBLE_ONE_ITEM(scratch, sizeof(XLogRecPtr), &regbuf->lastLsn, remained_size);
    }

    int m_session_id = u_sess->reporigin_cxt.originId != InvalidRepOriginId ?
        u_sess->reporigin_cxt.originId :
        u_sess->attr.attr_storage.replorigin_sesssion_origin;
    bool m_include_origin = t_thrd.xlog_cxt.include_origin;

    /* followed by the record's origin, if any */
    if (m_session_id == InvalidRepOriginId &&
        (u_sess->attr.attr_sql.enable_cluster_resize || t_thrd.role == AUTOVACUUM_WORKER)) {
        m_include_origin = true;
        m_session_id = 1;
    }

    /* followed by the record's origin, if any */
    if (m_include_origin && (m_session_id != InvalidRepOriginId || istoast)) {
        Assert(remained_size > 0);
        *(scratch++) = XLR_BLOCK_ID_ORIGIN;
        remained_size--;
        if (istoast && t_thrd.proc->workingVersionNum >= PARALLEL_DECODE_VERSION_NUM && XLogLogicalInfoActive()) {
            m_session_id = (int)((uint32)(m_session_id) | TOAST_FLAG);
        }
        XLOG_ASSEMBLE_ONE_ITEM(scratch, sizeof(m_session_id), &m_session_id, remained_size);
    }

    /* followed by main data, if any */
    if (t_thrd.xlog_cxt.mainrdata_len > 0) {
        if (t_thrd.xlog_cxt.mainrdata_len > 255) {
            Assert(remained_size > 0);
            *(scratch++) = XLR_BLOCK_ID_DATA_LONG;
            remained_size--;
            XLOG_ASSEMBLE_ONE_ITEM(scratch, sizeof(uint32), &t_thrd.xlog_cxt.mainrdata_len, remained_size);
        } else {
            Assert(remained_size >= 2);
            *(scratch++) = XLR_BLOCK_ID_DATA_SHORT;
            *(scratch++) = (uint8)t_thrd.xlog_cxt.mainrdata_len;
            remained_size--;
        }
        rdt_datas_last->next = t_thrd.xlog_cxt.mainrdata_head;
        rdt_datas_last = t_thrd.xlog_cxt.mainrdata_last;
        total_len += t_thrd.xlog_cxt.mainrdata_len;
    }
    rdt_datas_last->next = NULL;

    t_thrd.xlog_cxt.ptr_hdr_rdt->len = (scratch - t_thrd.xlog_cxt.hdr_scratch);
    total_len += t_thrd.xlog_cxt.ptr_hdr_rdt->len;

    /*
     * When read record with randAccess mode, we don't kown the record's total_len
     * is valid or not, so do a rough check, it can not beyond XLogSegSize.
     */
    if (total_len >= XLogRecordMaxSize)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("xlog record length is %u, more than XLogRecordMaxSize %u", total_len, XLogRecordMaxSize)));

    /*
     * Calculate CRC of the data
     *
     * Note that the record header isn't added into the CRC initially since we
     * don't know the prev-link yet.  Thus, the CRC will represent the CRC of
     * the whole record in the order: rdata, then backup blocks, then record
     * header.
     */

    /* using CRC32C */
    INIT_CRC32C(rdata_crc);
    COMP_CRC32C(rdata_crc, t_thrd.xlog_cxt.hdr_scratch + SizeOfXLogRecord,
                t_thrd.xlog_cxt.ptr_hdr_rdt->len - SizeOfXLogRecord);
    for (rdt = t_thrd.xlog_cxt.ptr_hdr_rdt->next; rdt != NULL; rdt = rdt->next)
        COMP_CRC32C(rdata_crc, rdt->data, rdt->len);

    /*
     * Fill in the fields in the record header. Prev-link is filled in later,
     * once we know where in the WAL the record will be inserted. The CRC does
     * not include the record header yet.
     */
    bool isUHeap = (rmid >= RM_UHEAP_ID) && (rmid <= RM_UHEAPUNDO_ID);

    rechdr->xl_xid = (isUHeap) ? GetTopTransactionIdIfAny() : GetCurrentTransactionIdIfAny();
    rechdr->xl_tot_len = total_len;
    rechdr->xl_info = info;
    rechdr->xl_rmid = rmid;
    rechdr->xl_prev = InvalidXLogRecPtr;
    rechdr->xl_crc = rdata_crc;
    Assert(hashbucket_flag == false || no_hashbucket_flag == false);
    rechdr->xl_term = Max(g_instance.comm_cxt.localinfo_cxt.term_from_file,
                          g_instance.comm_cxt.localinfo_cxt.term_from_xlog);
    if ((rechdr->xl_term & XLOG_CONTAIN_CSN) != 0) {
        ereport(PANIC, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_LOG),
            errmsg("Term has exceeded the limit %u", XLOG_MASK_TERM),
            errdetail("The highest bit of term is occupied for logical decoding"),
            errcause("Failover or switchover occurs too frequently"),
            erraction("Make sure the cluster is under stable state.")));
    }
    if (t_thrd.proc->workingVersionNum >= PARALLEL_DECODE_VERSION_NUM && XLogLogicalInfoActive()) {
        rechdr->xl_term |= XLOG_CONTAIN_CSN;
    }
    rechdr->xl_bucket_id = (uint2)(bucket_id + 1);

    return t_thrd.xlog_cxt.ptr_hdr_rdt;
}

/*
 * Determine whether the buffer referenced has to be backed up.
 *
 * Since we don't yet have the insert lock, fullPageWrites and forcePageWrites
 * could change later, so the result should be used for optimization purposes
 * only.
 */
bool
XLogCheckBufferNeedsBackup(Buffer buffer)
{
    XLogRecPtr      redoRecPtr;
    bool            doPageWrites = false;
    Page            page;

    XLogFPWInfo fpwInfo;
    GetFullPageWriteInfo(&fpwInfo);
    doPageWrites = fpwInfo.doPageWrites;
    redoRecPtr = fpwInfo.redoRecPtr;

    page = BufferGetPage(buffer);
    if (doPageWrites && PageGetLSN(page) <= redoRecPtr)
        return true;                    /* buffer requires backup */

    return false;                           /* buffer does not need to be backed up */
}

/*
 * Write a backup block if needed when we are setting a hint. Note that
 * this may be called for a variety of page types, not just heaps.
 *
 * Callable while holding just share lock on the buffer content.
 *
 * We can't use the plain backup block mechanism since that relies on the
 * Buffer being exclusively locked. Since some modifications (setting LSN, hint
 * bits) are allowed in a sharelocked buffer that can lead to wal checksum
 * failures. So instead we copy the page and insert the copied data as normal
 * record data.
 *
 * We only need to do something if page has not yet been full page written in
 * this checkpoint round. The LSN of the inserted wal record is returned if we
 * had to write, InvalidXLogRecPtr otherwise.
 *
 * It is possible that multiple concurrent backends could attempt to write WAL
 * records. In that case, multiple copies of the same block would be recorded
 * in separate WAL records by different backends, though that is still OK from
 * a correctness perspective.
 */
XLogRecPtr XLogSaveBufferForHint(Buffer buffer, bool buffer_std)
{
    XLogRecPtr recptr = InvalidXLogRecPtr;
    XLogRecPtr lsn;
    XLogRecPtr RedoRecPtr;
    errno_t rc = EOK;
    uint8 pageHintType;

    /*
     * Ensure no checkpoint can change our view of RedoRecPtr.
     */
    Assert(t_thrd.pgxact->delayChkpt);

    /*
     * Update RedoRecPtr so that we can make the right decision
     */
    RedoRecPtr = GetRedoRecPtr();

    /*
     * We assume page LSN is first data on *every* page that can be passed to
     * XLogInsert, whether it has the standard page layout or not. Since we're
     * only holding a share-lock on the page, we must take the buffer header
     * lock when we look at the LSN.
     */
    lsn = BufferGetLSNAtomic(buffer);
    if (XLByteLE(lsn, RedoRecPtr)) {
        unsigned int flags;
        char copied_buffer[BLCKSZ];
        char *origdata = (char *)BufferGetBlock(buffer);
        RelFileNode rnode;
        ForkNumber forkno;
        BlockNumber blkno;

        /*
         * Copy buffer so we don't have to worry about concurrent hint bit or
         * lsn updates. We assume pd_lower/upper cannot be changed without an
         * exclusive lock, so the contents bkp are not racy.
         */
        if (buffer_std) {
            /* Assume we can omit data between pd_lower and pd_upper */
            Page page = BufferGetPage(buffer);
            uint16 lower = ((PageHeader)page)->pd_lower;
            uint16 upper = ((PageHeader)page)->pd_upper;
            Assert(upper <= BLCKSZ);
            pageHintType = (PG_UHEAP_PAGE_LAYOUT_VERSION == (uint16)PageGetPageLayoutVersion(page)) ?
                XLOG_FPI_FOR_HINT_UHEAP : XLOG_FPI_FOR_HINT_HEAP;

            rc = memset_s(copied_buffer, BLCKSZ, 0, BLCKSZ);
            securec_check(rc, "", "");

            rc = memcpy_s(copied_buffer, BLCKSZ, origdata, lower);
            securec_check(rc, "", "");

            if (BLCKSZ > upper) {
                rc = memcpy_s(copied_buffer + upper, BLCKSZ - upper, origdata + upper, BLCKSZ - upper);
                securec_check(rc, "", "");
            }
        } else {
            rc = memcpy_s(copied_buffer, BLCKSZ, origdata, BLCKSZ);
            securec_check(rc, "", "");
            pageHintType = XLOG_FPI_FOR_HINT_HEAP;
        }

        XLogBeginInsert();

        flags = REGBUF_FORCE_IMAGE;
        if (buffer_std)
            flags |= REGBUF_STANDARD;

        BufferGetTag(buffer, &rnode, &forkno, &blkno);
        XLogRegisterBlock(0, &rnode, forkno, blkno, copied_buffer, flags, NULL);
        XLogRegisterData((char *) &pageHintType, sizeof(uint8));

        recptr = XLogInsert(RM_XLOG_ID, XLOG_FPI_FOR_HINT);
    }

    return recptr;
}

/*
 * Allocate working buffers needed for WAL record construction.
 */
void InitXLogInsert(void)
{
    /* Initialize the working areas */
    if (t_thrd.xlog_cxt.xloginsert_cxt == NULL) {
        t_thrd.xlog_cxt.xloginsert_cxt = AllocSetContextCreate(t_thrd.top_mem_cxt, "WAL record construction",
                                                               ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE,
                                                               ALLOCSET_DEFAULT_MAXSIZE);
    }

    if (t_thrd.xlog_cxt.registered_buffers == NULL) {
        t_thrd.xlog_cxt.registered_buffers =
            (registered_buffer *)MemoryContextAllocZero(t_thrd.xlog_cxt.xloginsert_cxt,
                                                        sizeof(registered_buffer) * (XLR_NORMAL_MAX_BLOCK_ID + 1));
        t_thrd.xlog_cxt.max_registered_buffers = XLR_NORMAL_MAX_BLOCK_ID + 1;
    }
    if (t_thrd.xlog_cxt.rdatas == NULL) {
        t_thrd.xlog_cxt.rdatas = (XLogRecData *)MemoryContextAlloc(t_thrd.xlog_cxt.xloginsert_cxt,
                                                                   sizeof(XLogRecData) * XLR_NORMAL_RDATAS);
        t_thrd.xlog_cxt.max_rdatas = XLR_NORMAL_RDATAS;
    }

    /*
     * Allocate a buffer to hold the header information for a WAL record.
     */
    if (t_thrd.xlog_cxt.hdr_scratch == NULL)
        t_thrd.xlog_cxt.hdr_scratch = (char *)MemoryContextAllocZero(t_thrd.xlog_cxt.xloginsert_cxt,
                                                                     HEADER_SCRATCH_SIZE);

    /*
     * Set WAL record main data chain.
     */
    if (t_thrd.xlog_cxt.mainrdata_last == NULL)
        t_thrd.xlog_cxt.mainrdata_last = (XLogRecData *)&t_thrd.xlog_cxt.mainrdata_head;
}
