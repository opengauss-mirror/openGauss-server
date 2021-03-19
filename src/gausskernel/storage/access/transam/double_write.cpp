/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * ---------------------------------------------------------------------------------------
 *
 *  double_write.cpp
 *        Before flush dirty pages to data file, flush them to double write file,
 *        in case of half-flushed pages. Recover those half-flushed data file pages
 *        before replaying xlog when starting.
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/transam/double_write.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include <unistd.h>
#include "miscadmin.h"
#include "utils/elog.h"
#include "utils/builtins.h"
#include "access/double_write.h"
#include "storage/smgr.h"
#include "pgstat.h"
#include "utils/palloc.h"
#include "gstrace/gstrace_infra.h"
#include "gstrace/access_gstrace.h"
#include "gs_bbox.h"
#include "postmaster/bgwriter.h"

#ifdef ENABLE_UT
#define static
#endif

Datum dw_get_node_name()
{
    if (g_instance.attr.attr_common.PGXCNodeName == NULL || g_instance.attr.attr_common.PGXCNodeName[0] == '\0') {
        return CStringGetTextDatum("not define");
    } else {
        return CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
    }
}

Datum dw_get_single_flush_dwn()
{
    if (dw_enabled()) {
        return UInt32GetDatum((uint32)g_instance.dw_single_cxt.file_head->head.dwn);
    }

    return UInt32GetDatum(0);
}

Datum dw_get_single_flush_start()
{
    if (dw_enabled()) {
        return UInt32GetDatum((uint32)g_instance.dw_single_cxt.file_head->start);
    }

    return UInt32GetDatum(0);
}

Datum dw_get_single_flush_trunc_num()
{
    return UInt64GetDatum(g_instance.dw_single_cxt.single_stat_info.file_trunc_num);
}

Datum dw_get_single_flush_reset_num()
{
    return UInt64GetDatum(g_instance.dw_single_cxt.single_stat_info.file_reset_num);
}

Datum dw_get_single_flush_total_writes()
{
    return UInt64GetDatum(g_instance.dw_single_cxt.single_stat_info.total_writes);
}

Datum dw_get_dw_number()
{
    if (dw_enabled()) {
        return UInt64GetDatum((uint64)g_instance.dw_batch_cxt.file_head->head.dwn);
    }

    return UInt64GetDatum(0);
}

Datum dw_get_start_page()
{
    if (dw_enabled()) {
        return UInt64GetDatum((uint64)g_instance.dw_batch_cxt.file_head->start);
    }

    return UInt64GetDatum(0);
}

Datum dw_get_file_trunc_num()
{
    return UInt64GetDatum(g_instance.dw_batch_cxt.batch_stat_info.file_trunc_num);
}

Datum dw_get_file_reset_num()
{
    return UInt64GetDatum(g_instance.dw_batch_cxt.batch_stat_info.file_reset_num);
}

Datum dw_get_total_writes()
{
    return UInt64GetDatum(g_instance.dw_batch_cxt.batch_stat_info.total_writes);
}

Datum dw_get_low_threshold_writes()
{
    return UInt64GetDatum(g_instance.dw_batch_cxt.batch_stat_info.low_threshold_writes);
}

Datum dw_get_high_threshold_writes()
{
    return UInt64GetDatum(g_instance.dw_batch_cxt.batch_stat_info.high_threshold_writes);
}

Datum dw_get_total_pages()
{
    return UInt64GetDatum(g_instance.dw_batch_cxt.batch_stat_info.total_pages);
}

Datum dw_get_low_threshold_pages()
{
    return UInt64GetDatum(g_instance.dw_batch_cxt.batch_stat_info.low_threshold_pages);
}

Datum dw_get_high_threshold_pages()
{
    return UInt64GetDatum(g_instance.dw_batch_cxt.batch_stat_info.high_threshold_pages);
}

/* double write statistic view */
const dw_view_col_t g_dw_view_col_arr[DW_VIEW_COL_NUM] = {
    { "node_name", TEXTOID, dw_get_node_name },
    { "curr_dwn", INT8OID, dw_get_dw_number },
    { "curr_start_page", INT8OID, dw_get_start_page },
    { "file_trunc_num", INT8OID, dw_get_file_trunc_num },
    { "file_reset_num", INT8OID, dw_get_file_reset_num },
    { "total_writes", INT8OID, dw_get_total_writes },
    { "low_threshold_writes", INT8OID, dw_get_low_threshold_writes },
    { "high_threshold_writes", INT8OID, dw_get_high_threshold_writes },
    { "total_pages", INT8OID, dw_get_total_pages },
    { "low_threshold_pages", INT8OID, dw_get_low_threshold_pages },
    { "high_threshold_pages", INT8OID, dw_get_high_threshold_pages }
};

const dw_view_col_t g_dw_single_view[DW_SINGLE_VIEW_COL_NUM] = {
    {"node_name", TEXTOID, dw_get_node_name},
    {"curr_dwn", INT4OID, dw_get_single_flush_dwn},
    {"curr_start_page", INT4OID, dw_get_single_flush_start},
    {"total_writes", INT8OID, dw_get_single_flush_total_writes},
    {"file_trunc_num", INT8OID, dw_get_single_flush_trunc_num},
    {"file_reset_num", INT8OID, dw_get_single_flush_reset_num}
};

static void dw_generate_batch_file();
static void dw_generate_single_file();
static void dw_recovery_partial_write_single();


void dw_pread_file(int fd, void *buf, int size, int64 offset)
{
    int32 curr_size, total_size;
    total_size = 0;
    do {
        curr_size = pread64(fd, ((char *)buf + total_size), (size - total_size), offset);
        if (curr_size == -1) {
            ereport(PANIC, (errcode_for_file_access(), errmodule(MOD_DW), errmsg("Read file error")));
        }

        total_size += curr_size;
        offset += curr_size;
    } while (curr_size > 0);

    if (total_size != size) {
        ereport(PANIC, (errcode_for_file_access(), errmodule(MOD_DW),
                        errmsg("Read file size mismatch: expected %d, read %d", size, total_size)));
    }
}

void dw_pwrite_file(int fd, const void *buf, int size, int64 offset)
{
    int write_size = 0;
    uint32 try_times = 0;

    while (try_times < DW_TRY_WRITE_TIMES) {
        write_size = pwrite64(fd, buf, size, offset);
        if (write_size == 0) {
            try_times++;
            pg_usleep(DW_SLEEP_US);
            continue;
        } else if (write_size < 0) {
            ereport(PANIC, (errcode_for_file_access(), errmodule(MOD_DW), errmsg("Write file error")));
        } else {
            break;
        }
    }
    if (write_size != size) {
        ereport(PANIC, (errcode_for_file_access(), errmodule(MOD_DW),
                        errmsg("Write file size mismatch: expected %d, written %d", size, write_size)));
    }
}

int64 dw_seek_file(int fd, int64 offset, int32 origin)
{
    return (int64)lseek64(fd, (off64_t)offset, origin);
}

static void dw_extend_file(int fd, const void *buf, int buf_size, int64 size, bool single)
{
    int64 offset = 0;
    int64 remain_size;
    int64 file_size = 0;

    offset = dw_seek_file(fd, 0, SEEK_END);
    if (offset == -1) {
        ereport(PANIC, (errcode_for_file_access(), errmodule(MOD_DW), errmsg("Seek file error")));
    }

    if (single) {
        file_size = DW_SINGLE_FILE_SIZE;
    } else {
        file_size = DW_FILE_SIZE;
    }
    if ((offset + size) > file_size) {
        ereport(PANIC,
            (errmodule(MOD_DW), 
                errmsg("DW extend file failed, expected_file_size %ld, offset %ld, extend_size %ld",
                file_size, offset, size)));
    }

    remain_size = size;
    while (remain_size > 0) {
        size = (remain_size > buf_size) ? buf_size : remain_size;
        dw_pwrite_file(fd, buf, size, offset);
        offset += size;
        remain_size -= size;
    }
}

static void dw_set_pg_checksum(char *page, BlockNumber blockNum)
{
    if (PageIsNew(page)) {
        return;
    }

    /* set page->pd_flags mark using FNV1A for checksum */
    PageSetChecksumByFNV1A(page);
    ((PageHeader)page)->pd_checksum = pg_checksum_page(page, blockNum);
}

static bool dw_verify_pg_checksum(PageHeader page_header, BlockNumber blockNum)
{
    /* new page donot have crc and lsn, we donot recovery it */
    if (PageIsNew(page_header)) {
        ereport(WARNING, (errmodule(MOD_DW), errmsg("DW verify checksum: new page")));
        return false;
    }
    if (PageIsChecksumByFNV1A(page_header)) {
        uint16 checksum = pg_checksum_page((char *)page_header, blockNum);
        return checksum == page_header->pd_checksum;
    } else {
        ereport(WARNING, (errmodule(MOD_DW), errmsg("DW verify checksum: page checksum flag is wrong")));
        return false;
    }
}

inline void dw_prepare_page(dw_batch_t *batch, uint16 page_num, uint16 page_id, uint16 dwn)
{
    if (g_instance.dw_batch_cxt.contain_hashbucket == true) {
        if (t_thrd.proc->workingVersionNum < DW_SUPPORT_SINGLE_FLUSH_VERSION) {
            page_num = page_num | IS_HASH_BKT_MASK;
        }
        batch->buftag_ver = HASHBUCKET_TAG;
    } else {
        batch->buftag_ver = ORIGIN_TAG;
    }
    batch->page_num = page_num;
    
    batch->head.page_id = page_id;
    batch->head.dwn = dwn;
    DW_PAGE_TAIL(batch)->dwn = dwn;
    dw_calc_batch_checksum(batch);
}

static void dw_prepare_file_head(char *file_head, uint16 start, uint16 dwn)
{
    uint32 i;
    uint32 id;
    dw_file_head_t *curr_head = NULL;
    for (i = 0; i < DW_FILE_HEAD_ID_NUM; i++) {
        id = g_dw_file_head_ids[i];
        curr_head = (dw_file_head_t *)(file_head + sizeof(dw_file_head_t) * id);
        curr_head->head.page_id = 0;
        curr_head->head.dwn = dwn;
        curr_head->start = start;
        curr_head->buftag_version = HASHBUCKET_TAG;
        curr_head->tail.dwn = dwn;
        dw_calc_file_head_checksum(curr_head);
    }
}

static void dw_recover_file_head(knl_g_dw_context *cxt, bool single)
{
    uint32 i;
    uint16 id;
    errno_t rc;
    int64 file_size;
    dw_file_head_t *curr_head = NULL;
    dw_file_head_t *working_head = NULL;
    char *file_head = (char *)cxt->file_head;

    pgstat_report_waitevent(WAIT_EVENT_DW_READ);
    dw_pread_file(cxt->fd, cxt->file_head, BLCKSZ, 0);
    pgstat_report_waitevent(WAIT_EVENT_END);

    int64 offset = dw_seek_file(cxt->fd, 0, SEEK_END);
    if (single) {
        file_size = DW_SINGLE_FILE_SIZE;
    } else {
        file_size = DW_FILE_SIZE;
    }
    if (offset != file_size) {
        ereport(PANIC, (errmodule(MOD_DW),
                        errmsg("DW check file size failed, expected_size %ld, actual_size %ld", DW_FILE_SIZE, offset)));
    }

    for (i = 0; i < DW_FILE_HEAD_ID_NUM; i++) {
        id = g_dw_file_head_ids[i];
        curr_head = (dw_file_head_t *)(file_head + sizeof(dw_file_head_t) * id);
        if (dw_verify_file_head(curr_head)) {
            working_head = curr_head;
            break;
        }
    }

    if (working_head == NULL) {
        ereport(FATAL, (errcode_for_file_access(), errmodule(MOD_DW), errmsg("File header is broken")));
        /* we should not get here, since FATAL will do abort. But for ut, return is needed */
        return;
    }

    ereport(LOG, (errmodule(MOD_DW), errmsg("Found a valid file header: id %hu, file_head[dwn %hu, start %hu]", id,
                                            working_head->head.dwn, working_head->start)));

    for (i = 0; i < DW_FILE_HEAD_ID_NUM; i++) {
        id = g_dw_file_head_ids[i];
        curr_head = (dw_file_head_t *)(file_head + sizeof(dw_file_head_t) * id);
        if (curr_head != working_head) {
            rc = memcpy_s(curr_head, sizeof(dw_file_head_t), working_head, sizeof(dw_file_head_t));
            securec_check(rc, "\0", "\0");
        }
    }

    pgstat_report_waitevent(WAIT_EVENT_DW_WRITE);
    dw_pwrite_file(cxt->fd, file_head, BLCKSZ, 0);
    pgstat_report_waitevent(WAIT_EVENT_END);
}

static inline void dw_log_page_header(PageHeader page)
{
    ereport(DW_LOG_LEVEL,
            (errmodule(MOD_DW),
             errmsg("Page header info: pd_lsn %lu, pd_checksum %hu, "
                    "pd_lower %hu(%s), pd_upper %hu(%s), max_offset %hu",
                    PageGetLSN(page), page->pd_checksum, page->pd_lower, PageIsEmpty(page) ? "empty" : "non-empty",
                    page->pd_upper, PageIsNew(page) ? "new" : "old", PageGetMaxOffsetNumber((char *)page))));
}

template <typename T>
static inline void dw_log_data_page(int elevel, const char *state, T *buf_tag)
{
    ereport(elevel, (errmodule(MOD_DW),
                     errmsg("%s: buf_tag[rel %u/%u/%u blk %u fork %d]", state, buf_tag->rnode.spcNode,
                            buf_tag->rnode.dbNode, buf_tag->rnode.relNode, buf_tag->blockNum, buf_tag->forkNum)));
}

template <typename T1, typename T2>
static void dw_recover_pages(T1 *batch, T2 *buf_tag, PageHeader data_page, BufTagVer tag_ver)
{
    uint16 i;
    PageHeader dw_page;
    SMgrRelation relation;
    BlockNumber blk_num;
    RelFileNode relnode;
    for (i = 0; i < GET_REL_PGAENUM(batch->page_num); i++) {
        buf_tag = &batch->buf_tag[i];
        if (tag_ver == HASHBUCKET_TAG) {
            relnode.dbNode = buf_tag->rnode.dbNode;
            relnode.spcNode = buf_tag->rnode.spcNode;
            relnode.relNode = buf_tag->rnode.relNode;
            relnode.bucketNode = ((BufferTag *)buf_tag)->rnode.bucketNode;
        } else {
            relnode.dbNode = buf_tag->rnode.dbNode;
            relnode.spcNode = buf_tag->rnode.spcNode;
            relnode.relNode = buf_tag->rnode.relNode;
            relnode.bucketNode = InvalidBktId;
        }
        relation = smgropen(relnode, InvalidBackendId, GetColumnNum(buf_tag->forkNum));
        if (!smgrexists(relation, buf_tag->forkNum)) {
            dw_log_data_page(WARNING, "Data file deleted", buf_tag);
            continue;
        }
        blk_num = smgrnblocks(relation, buf_tag->forkNum);
        if (blk_num <= buf_tag->blockNum) {
            dw_log_data_page(WARNING, "Data page deleted", buf_tag);
            continue;
        }
        smgrread(relation, buf_tag->forkNum, buf_tag->blockNum, (char *)data_page);

        dw_page = (PageHeader)((char *)batch + (i + 1) * BLCKSZ);
        if (!dw_verify_pg_checksum(dw_page, buf_tag->blockNum)) {
            dw_log_data_page(WARNING, "DW page broken", buf_tag);
            dw_log_page_header(dw_page);
            continue;
        }

        dw_log_data_page(DW_LOG_LEVEL, "DW page fine", buf_tag);
        dw_log_page_header(dw_page);
        if (!dw_verify_pg_checksum(data_page, buf_tag->blockNum) ||
            XLByteLT(PageGetLSN(data_page), PageGetLSN(dw_page))) {
            smgrwrite(relation, buf_tag->forkNum, buf_tag->blockNum, (const char *)dw_page, false);
            dw_log_data_page(LOG, "Date page recovered", buf_tag);
            dw_log_page_header(data_page);
        }
    }
}

void wait_all_dw_page_finish_flush()
{
    if (g_instance.bgwriter_cxt.bgwriter_procs != NULL) {
        for (int i = 0; i < g_instance.bgwriter_cxt.bgwriter_num;) {
            if (g_instance.bgwriter_cxt.bgwriter_procs[i].thrd_dw_cxt.dw_page_idx == -1) {
                i++;
                continue;
            } else {
                (void)sched_yield();
            }
        }
    }
    if (g_instance.ckpt_cxt_ctl->page_writer_procs.writer_proc != NULL) {
        while (g_instance.ckpt_cxt_ctl->page_writer_procs.thrd_dw_cxt.dw_page_idx != -1) {
            (void)sched_yield();
        }
    }
    return;
}

int get_dw_page_min_idx()
{
    uint16 min_idx = 0;
    int dw_page_idx;

    if (g_instance.bgwriter_cxt.bgwriter_procs != NULL) {
        for (int i = 0; i < g_instance.bgwriter_cxt.bgwriter_num; i++) {
            dw_page_idx = g_instance.bgwriter_cxt.bgwriter_procs[i].thrd_dw_cxt.dw_page_idx;
            if (dw_page_idx != -1) {
                if (min_idx == 0 || (uint16)dw_page_idx < min_idx) {
                    min_idx = dw_page_idx;
                }
            }
        }
    }
    if (g_instance.ckpt_cxt_ctl->page_writer_procs.writer_proc != NULL) {
        dw_page_idx = g_instance.ckpt_cxt_ctl->page_writer_procs.thrd_dw_cxt.dw_page_idx;
        if (dw_page_idx != -1) {
            if (min_idx == 0 || (uint16)dw_page_idx < min_idx) {
                min_idx = dw_page_idx;
            }
        }
    }
	
    return min_idx;
}

/*
 * Basically, dw_reset_if_need calls smgrsync and then reuse dw file to some extent:
 * 1. truncate dw file start position to last flush postition, before which all dirty buffers are garanteed
 * to be smgr-synced, in order to avoid redundant dw file check during crash recovery.
 * 2. fully recycle dw file and set dw file start position to the first page, when dw file is out of space.
 *
 * On entry, caller should hold dw flush lock. For truncate purpose, which is currently considered as
 * an rto optimization, dw flush lock is released during performing smgrsync and is only conditionally re-acquired.
 * Callers for dw truncate, i.e. checkpointer and startup, should take care of lock failure.
 *
 * We do not allow dw truncate and full recycle at the same time. In fact, full dw recycle, which blocks
 * all concurrent pagewriters, should be removed for higher performance in the future, as long as dw file
 * is reused as a ring file while file sync and dw truncate jobs are solely taken care of by checkpointer.
 *
 * Return FALSE if we can not grab conditional dw flush lock after smgrsync for truncate.
 */
static bool dw_reset_if_need(knl_g_dw_context *cxt, uint16 pages_to_write, bool trunc_file)
{
    bool file_full = false;
    uint16 min_idx;
    dw_file_head_t *file_head = cxt->file_head;
    volatile uint16 org_start = file_head->start;
    volatile uint16 org_dwn = file_head->head.dwn;
    uint16 last_flush_page;

    file_full = (file_head->start + cxt->flush_page + pages_to_write >= DW_FILE_PAGE);
    
    Assert(!(file_full && trunc_file));
    if (!file_full && !trunc_file) {
        return true;
    }

    if (trunc_file) {
        Assert(AmStartupProcess() || AmCheckpointerProcess() || AmBootstrapProcess() || !IsUnderPostmaster);
        /*
         * Record min flush position for truncate because flush lock is not held during smgrsync.
         */
        min_idx = get_dw_page_min_idx();
        LWLockRelease(cxt->flush_lock);
    } else {
        Assert(AmStartupProcess() || AmPageWriterProcess() || AmMulitBackgroundWriterProcess());
        /* reset start position and flush page num for full recycle */
        file_head->start = DW_BATCH_FILE_START;
        cxt->flush_page = 0;
        wait_all_dw_page_finish_flush();
    }

    smgrsync_for_dw();

    if (trunc_file) {
        if (!LWLockConditionalAcquire(cxt->flush_lock, LW_EXCLUSIVE)) {
            ereport(LOG, (errmodule(MOD_DW),
                          errmsg("Can not get dw flush lock and skip dw truncate after sync for this time")));
            return false;
        } else if (org_start != file_head->start || org_dwn != file_head->head.dwn) {
            /*
             * Even if there are concurrent dw truncate/reset during the above smgrsync,
             * the possibility of same start and dwn value should be small enough.
             */
            ereport(LOG, (errmodule(MOD_DW), errmsg("Skip dw truncate after sync due to concurrent dw truncate/reset, "
                                                    "original[dwn %hu, start %hu], current[dwn %hu, start %hu]",
                                                    org_dwn, org_start, file_head->head.dwn, file_head->start)));
            return true;
        }
    }

    ereport(DW_LOG_LEVEL, (errmodule(MOD_DW), errmsg("Reset DW file: file_head[dwn %hu, start %hu], total_pages %hu, "
                                                     "file_full %d, trunc_file %d, pages_to_write %hu",
                                                     file_head->head.dwn, file_head->start, cxt->flush_page, file_full,
                                                     trunc_file, pages_to_write)));

    /*
     * if truncate file and flush_page is not 0, the dwn can not plus,
     * otherwise verify will failed when recovery the data form dw file.
     */
    if (trunc_file) {
        if (min_idx == 0) {
            file_head->start += cxt->flush_page;
            cxt->flush_page = 0;
        } else {
            last_flush_page = min_idx - file_head->start;
            file_head->start = min_idx;
            cxt->flush_page = cxt->flush_page - last_flush_page;
        }
        dw_prepare_file_head((char *)file_head, file_head->start, file_head->head.dwn);
    } else {
        dw_prepare_file_head((char *)file_head, file_head->start, file_head->head.dwn + 1);
    }

    Assert(file_head->head.dwn == file_head->tail.dwn);
    pgstat_report_waitevent(WAIT_EVENT_DW_WRITE);
    dw_pwrite_file(cxt->fd, file_head, BLCKSZ, 0);
    pgstat_report_waitevent(WAIT_EVENT_END);

    pg_atomic_add_fetch_u64(&cxt->batch_stat_info.file_trunc_num, 1);
    if (file_full) {
        pg_atomic_add_fetch_u64(&cxt->batch_stat_info.file_reset_num, 1);
    }
    return true;
}

static void dw_read_pages(dw_read_asst_t *read_asst, uint16 reading_pages)
{
    if (reading_pages > 0) {
        Assert(read_asst->buf_end + reading_pages <= read_asst->buf_capacity);
        Assert(read_asst->file_start + reading_pages <= read_asst->file_capacity);
        pgstat_report_waitevent(WAIT_EVENT_DW_READ);
        dw_pread_file(read_asst->fd, (read_asst->buf + read_asst->buf_end * BLCKSZ), (reading_pages * BLCKSZ),
                      (read_asst->file_start * BLCKSZ));
        pgstat_report_waitevent(WAIT_EVENT_END);
        read_asst->buf_end += reading_pages;
        read_asst->file_start += reading_pages;
    }

    Assert(read_asst->buf_end >= read_asst->buf_start);
}

static inline void dw_discard_pages(dw_read_asst_t *read_asst, uint16 page_num)
{
    read_asst->buf_start += page_num;
    Assert(read_asst->buf_end >= read_asst->buf_start);
}

static uint16 dw_calc_reading_pages(dw_read_asst_t *read_asst)
{
    dw_batch_t *curr_head;
    uint16 remain_pages, batch_pages, reading_pages;
    errno_t rc;

    remain_pages = read_asst->buf_end - read_asst->buf_start;
    curr_head = (dw_batch_t *)(read_asst->buf + read_asst->buf_start * BLCKSZ);
    batch_pages = (GET_REL_PGAENUM(curr_head->page_num) + DW_EXTRA_FOR_ONE_BATCH);
    /* batch is already im memory, no need read */
    if (remain_pages >= batch_pages) {
        reading_pages = 0;
    } else {
        /* if buf size not enough for the batch, move the read pages to the start */
        /* all 3 variable less than DW_BUF_MAX, sum not overflow uint32 */
        if (read_asst->buf_start + batch_pages >= read_asst->buf_capacity) {
            rc = memmove_s(read_asst->buf, (remain_pages * BLCKSZ), curr_head, (remain_pages * BLCKSZ));
            securec_check(rc, "\0", "\0");
            read_asst->buf_start = 0;
            read_asst->buf_end = remain_pages;
            curr_head = (dw_batch_t *)read_asst->buf;
        }
        reading_pages = batch_pages - remain_pages;
    }

    Assert((char *)curr_head + (remain_pages + reading_pages) * BLCKSZ <
           read_asst->buf + read_asst->buf_capacity * BLCKSZ);
    Assert(read_asst->file_start + reading_pages <= DW_FILE_PAGE);
    return reading_pages;
}

static void dw_recover_batch_head(knl_g_dw_context *cxt, dw_batch_t *curr_head)
{
    errno_t rc;
    rc = memset_s(curr_head, BLCKSZ, 0, BLCKSZ);
    securec_check(rc, "\0", "\0");
    dw_prepare_page(curr_head, 0, cxt->file_head->start, cxt->file_head->head.dwn);
    pgstat_report_waitevent(WAIT_EVENT_DW_WRITE);
    dw_pwrite_file(cxt->fd, curr_head, BLCKSZ, (curr_head->head.page_id * BLCKSZ));
    pgstat_report_waitevent(WAIT_EVENT_END);
}

static inline void dw_log_recover_state(knl_g_dw_context *cxt, int elevel, const char *state, dw_batch_t *batch)
{
    ereport(elevel,
            (errmodule(MOD_DW),
             errmsg("DW recovery state: \"%s\", file start page[dwn %hu, start %hu], now access page %hu, "
                    "current [page_id %hu, dwn %hu, checksum verify res is %d, page_num orig %hu, page_num fixed %hu]",
                    state, cxt->file_head->head.dwn, cxt->file_head->start, cxt->flush_page, batch->head.page_id,
                    batch->head.dwn, dw_verify_batch_checksum(batch), batch->page_num,
                    GET_REL_PGAENUM(batch->page_num))));
}

static bool dw_batch_head_broken(knl_g_dw_context *cxt, dw_batch_t *curr_head)
{
    bool broken = false;
    dw_batch_t *curr_tail = dw_batch_tail_page(curr_head);
    if (dw_verify_page(curr_head)) {
        if (GET_REL_PGAENUM(curr_head->page_num) == 0) {
            dw_log_recover_state(cxt, LOG, "Empty", curr_head);
        } else if (curr_head->head.page_id == DW_BATCH_FILE_START) {
            dw_log_recover_state(cxt, LOG, "File reset", curr_head);
        } else {
            dw_log_recover_state(cxt, WARNING, "Head info", curr_head);
            dw_log_recover_state(cxt, WARNING, "Tail broken", curr_tail);
            dw_batch_t* tail_page = dw_batch_tail_page(curr_head);
            ereport(WARNING,
                (errmodule(MOD_DW),
                errmsg("file head page[dwn %hu], "
                    "current batch head [page_id %hu, dwn %hu], checksum verify res is %d, "
                    "batch tail [page_id %hu, dwn %hu], checksum verify res is %d, batch page num is %d",
                    cxt->file_head->head.dwn, curr_head->head.page_id, curr_head->head.dwn, dw_verify_page(curr_head),
                    tail_page->head.page_id, tail_page->head.dwn, dw_verify_page(tail_page),
                    GET_REL_PGAENUM(curr_head->page_num))));
            broken = true;
        }
    } else {
        dw_log_recover_state(cxt, WARNING, "Head broken", curr_head);
        dw_log_recover_state(cxt, WARNING, "Tail unknown", curr_tail);
        broken = true;
    }
    return broken;
}

static void dw_recover_partial_write(knl_g_dw_context *cxt)
{
    dw_read_asst_t read_asst;
    dw_batch_t *curr_head = NULL;
    uint16 reading_pages;
    uint16 remain_pages;
    bool dw_file_broken = false;
    char *data_page = NULL;

    read_asst.fd = cxt->fd;
    read_asst.file_start = cxt->file_head->start;
    read_asst.file_capacity = DW_FILE_PAGE;
    read_asst.buf_start = 0;
    read_asst.buf_end = 0;
    read_asst.buf_capacity = DW_BUF_MAX;
    read_asst.buf = cxt->buf;
    reading_pages = Min(DW_BATCH_MAX_FOR_NOHBK, (DW_FILE_PAGE - cxt->file_head->start));

    data_page = (char *)palloc0(BLCKSZ);

    for (;;) {
        dw_read_pages(&read_asst, reading_pages);
        curr_head = (dw_batch_t *)(read_asst.buf + (read_asst.buf_start * BLCKSZ));

        if (!dw_verify_batch(curr_head, cxt->file_head->head.dwn)) {
            dw_file_broken = dw_batch_head_broken(cxt, curr_head);
            break;
        }
        if (t_thrd.proc->workingVersionNum < DW_SUPPORT_SINGLE_FLUSH_VERSION) {
            bool is_hashbucket = ((curr_head->page_num & IS_HASH_BKT_MASK) != 0);
            curr_head->buftag_ver = is_hashbucket ? HASHBUCKET_TAG : ORIGIN_TAG;
        }

        remain_pages = read_asst.buf_end - read_asst.buf_start;
        Assert(curr_head->head.page_id + remain_pages == read_asst.file_start);
        Assert(remain_pages >= GET_REL_PGAENUM(curr_head->page_num) + DW_EXTRA_FOR_ONE_BATCH);

        dw_log_recover_state(cxt, DW_LOG_LEVEL, "Batch fine", curr_head);
        if (curr_head->buftag_ver == ORIGIN_TAG) {
            BufferTagFirstVer *tmp = NULL;
            dw_recover_pages<dw_batch_first_ver, BufferTagFirstVer>((dw_batch_first_ver *)curr_head, tmp,
                (PageHeader)data_page, (BufTagVer)curr_head->buftag_ver);
        } else {
            BufferTag *tmp = NULL;
            dw_recover_pages<dw_batch_t, BufferTag>(curr_head, tmp, (PageHeader)data_page,
                (BufTagVer)curr_head->buftag_ver);
        }

        /* discard the first batch. including head page and data pages */
        cxt->flush_page += (1 + GET_REL_PGAENUM(curr_head->page_num));
        dw_discard_pages(&read_asst, (1 + GET_REL_PGAENUM(curr_head->page_num)));
        curr_head = dw_batch_tail_page(curr_head);
        if (GET_REL_PGAENUM(curr_head->page_num) == 0) {
            dw_log_recover_state(cxt, LOG, "Batch end", curr_head);
            break;
        }

        reading_pages = dw_calc_reading_pages(&read_asst);
    }

    /* if free space not enough for one batch, reuse file. Otherwise, just do a truncate */
    if ((cxt->file_head->start + cxt->flush_page + DW_BUF_MAX) >= DW_FILE_PAGE) {
        (void)dw_reset_if_need(cxt, DW_BUF_MAX, false);
    } else if (cxt->flush_page > 0) {
        if (!dw_reset_if_need(cxt, 0, true)) {
            ereport(PANIC, (errcode_for_file_access(), errmodule(MOD_DW),
                            errmsg("Could not truncate dw file during startup!")));
        }
    }
    if (dw_file_broken) {
        dw_recover_batch_head(cxt, curr_head);
    }
    dw_log_recover_state(cxt, LOG, "Finish", curr_head);
    pfree(data_page);
}

void dw_bootstrap()
{
    dw_generate_batch_file();
    dw_generate_single_file();
}

static void dw_generate_batch_file()
{
    char *file_head = NULL;
    dw_batch_t *batch_head = NULL;
    int64 remain_size;
    int fd = -1;
    char *unaligned_buf = NULL;

    if (file_exists(DW_FILE_NAME)) {
        ereport(PANIC, (errcode_for_file_access(), errmodule(MOD_DW), "DW batch flush file already exists"));
    }

    ereport(LOG, (errmodule(MOD_DW), errmsg("DW bootstrap batch flush file")));

    /* create dw batch flush file */
    fd = open(DW_FILE_NAME, (DW_FILE_FLAG | O_CREAT), DW_FILE_PERM);
    if (fd == -1) {
        ereport(PANIC,
                (errcode_for_file_access(), errmodule(MOD_DW), errmsg("Could not create file \"%s\"", DW_FILE_NAME)));
    }

    /* Open file with O_SYNC, to make sure the data and file system control info on file after block writing. */
    unaligned_buf = (char *)palloc0(DW_FILE_EXTEND_SIZE + BLCKSZ);
    file_head = (char *)TYPEALIGN(BLCKSZ, unaligned_buf);

    /* file head and first batch head will be writen */
    remain_size = (DW_FILE_PAGE * BLCKSZ) - BLCKSZ - BLCKSZ;
    dw_prepare_file_head(file_head, DW_BATCH_FILE_START, 0);
    batch_head = (dw_batch_t *)(file_head + BLCKSZ);
    batch_head->head.page_id = DW_BATCH_FILE_START;
    dw_calc_batch_checksum(batch_head);
    pgstat_report_waitevent(WAIT_EVENT_DW_WRITE);
    dw_pwrite_file(fd, file_head, (BLCKSZ + BLCKSZ), 0);
    dw_extend_file(fd, file_head, DW_FILE_EXTEND_SIZE, remain_size, false);
    pgstat_report_waitevent(WAIT_EVENT_END);
    ereport(LOG, (errmodule(MOD_DW), errmsg("Double write batch flush file created successfully")));

    (void)close(fd);
    fd = -1;
    pfree(unaligned_buf);
    return;
}

static void dw_free_resource(knl_g_dw_context *cxt)
{
    int rc = close(cxt->fd);
    if (rc == -1) {
        ereport(ERROR, (errcode_for_file_access(), errmodule(MOD_DW), errmsg("DW file close failed")));
    }
    cxt->fd = -1;
    cxt->flush_lock = NULL;
    cxt->buf = NULL;
    if (cxt->single_flush_state != NULL) {
        pfree(cxt->single_flush_state);
        cxt->single_flush_state = NULL;
    }
    if (cxt->single_flush_pos != NULL) {
        pfree(cxt->single_flush_pos);
        cxt->single_flush_pos = NULL;
    }

    if (cxt->unaligned_buf != NULL) {
        pfree(cxt->unaligned_buf);
        cxt->unaligned_buf = NULL;
    }
    cxt->closed = 1;
}

void dw_file_check_and_rebuild()
{
    if (file_exists(DW_BUILD_FILE_NAME)) {
        ereport(LOG, (errmodule(MOD_DW), errmsg("Double write initializing after build")));

        if (file_exists(DW_FILE_NAME)) {
            /*
             * Probably the gaussdb was killed during the first time startup after build, resulting in a half-written
             * DW file. So, log a warning message and remove the residual DW file.
             */
            ereport(WARNING, (errcode_for_file_access(), errmodule(MOD_DW), "batch flush DW file exists, deleting it"));

            if (unlink(DW_FILE_NAME) != 0) {
                ereport(PANIC, (errcode_for_file_access(), errmodule(MOD_DW),
                                errmsg("Could not remove the residual batch flush DW single flush file")));
            }
        }
        
        if (file_exists(SINGLE_DW_FILE_NAME)) {
            /*
             * Probably the gaussdb was killed during the first time startup after build, resulting in a half-written
             * DW file. So, log a warning message and remove the residual DW file.
             */
            ereport(WARNING, (errcode_for_file_access(), errmodule(MOD_DW), "single flush DW file exists, deleting it"));

            if (unlink(SINGLE_DW_FILE_NAME) != 0) {
                ereport(PANIC, (errcode_for_file_access(), errmodule(MOD_DW),
                                errmsg("Could not remove the residual single flush DW single flush file")));
            }
        }

        /* Create the DW file. */
        dw_bootstrap();

        /* Remove the DW build file. */
        if (unlink(DW_BUILD_FILE_NAME) != 0) {
            ereport(PANIC,
                    (errcode_for_file_access(), errmodule(MOD_DW), errmsg("Could not remove the DW build file")));
        }
    }

    if (!file_exists(DW_FILE_NAME)) {
        ereport(PANIC, (errcode_for_file_access(), errmodule(MOD_DW), errmsg("batch flush DW file does not exist")));
    }

    if (t_thrd.proc->workingVersionNum >= DW_SUPPORT_SINGLE_FLUSH_VERSION) {
        if (!file_exists(SINGLE_DW_FILE_NAME)) {
            ereport(PANIC, (errcode_for_file_access(), 
                errmodule(MOD_DW), errmsg("single flush DW file does not exist")));
        }
    } else {
        if (!file_exists(SINGLE_DW_FILE_NAME)) {
            ereport(LOG, (errmodule(MOD_DW), 
                errmsg("first upgrade to DW_SUPPORT_SINGLE_FLUSH_VERSION, need init the single file")));
            dw_generate_single_file();
        } else {
            (void)unlink(SINGLE_DW_FILE_NAME);
            dw_generate_single_file();
        }
    }

}

const void init_dw_single_flush_map()
{
    uint16 i = 0;
    uint16 m = 0;
    uint16 j = 0;
    uint16 pos;
    knl_g_dw_context *dw_cxt = &g_instance.dw_single_cxt;
    uint16 batch_size = SINGLE_BLOCK_TAG_NUM;
    uint16 batch_num = (DW_SINGLE_DIRTY_PAGE_NUM / batch_size) + (DW_SINGLE_DIRTY_PAGE_NUM % batch_size == 0 ? 0 : 1);
    LWLock *lock = NULL;

    for (i = 0; i < batch_num; i++) {
        lock = LWLockAssign(LWTRANCHE_DW_SINGLE_WRITE);
        for (m = 0, j = i * batch_size;
            j < (i + 1) * batch_size && j < DW_SINGLE_DIRTY_PAGE_NUM;
            j++, m++) {
            pos = i + m * batch_num;
            dw_cxt->single_flush_pos[pos].write_lock = lock;
            dw_cxt->single_flush_pos[pos].actual_pos = j;
            dw_cxt->single_flush_state[pos].data_flush = false;
        }
    }
}

void dw_cxt_init_batch()
{
    uint32 buf_size;
    char *buf = NULL;
    knl_g_dw_context *batch_cxt = &g_instance.dw_batch_cxt;

    Assert(batch_cxt->flush_lock == NULL);
    batch_cxt->flush_lock = LWLockAssign(LWTRANCHE_DOUBLE_WRITE);

    /* double write file disk space pre-allocated, O_DSYNC for less IO */
    batch_cxt->fd = open(DW_FILE_NAME, DW_FILE_FLAG, DW_FILE_PERM);
    if (batch_cxt->fd == -1) {
        ereport(PANIC,
            (errcode_for_file_access(), errmodule(MOD_DW), errmsg("Could not open file \"%s\"", DW_FILE_NAME)));
    }

    buf_size = DW_MEM_CTX_MAX_BLOCK_SIZE_FOR_NOHBK;

    batch_cxt->unaligned_buf = (char *)palloc0(buf_size); /* one more BLCKSZ for alignment */
    buf = (char *)TYPEALIGN(BLCKSZ, batch_cxt->unaligned_buf);

    batch_cxt->file_head = (dw_file_head_t *)buf;
    buf += BLCKSZ;

    batch_cxt->buf = buf;
    if (BBOX_BLACKLIST_DW_BUFFER) {
        bbox_blacklist_add(DW_BUFFER, buf, buf_size - BLCKSZ - BLCKSZ);
    }
    batch_cxt->closed = 0;
    batch_cxt->write_pos = 0;
    batch_cxt->flush_page = 0;
}

void dw_cxt_init_single()
{
    char *buf = NULL;
    knl_g_dw_context *single_cxt = &g_instance.dw_single_cxt;
    uint16 blk_num = (DW_SINGLE_DIRTY_PAGE_NUM / SINGLE_BLOCK_TAG_NUM) +
        (DW_SINGLE_DIRTY_PAGE_NUM % SINGLE_BLOCK_TAG_NUM == 0 ? 0 : 1);

    Assert(single_cxt->flush_lock == NULL);
    single_cxt->flush_lock = LWLockAssign(LWTRANCHE_DW_SINGLE_POS);

    single_cxt->fd = open(SINGLE_DW_FILE_NAME, DW_FILE_FLAG, DW_FILE_PERM);
    if (single_cxt->fd == -1) {
        ereport(PANIC,
            (errcode_for_file_access(), errmodule(MOD_DW), errmsg("Could not open file \"%s\"", DW_FILE_NAME)));
    }

    /* one more BLCKSZ for alignment, one for file head */
    single_cxt->unaligned_buf = (char *)palloc0((blk_num + 1 + 1) * BLCKSZ); 
    buf = (char *)TYPEALIGN(BLCKSZ, single_cxt->unaligned_buf);

    single_cxt->file_head = (dw_file_head_t *)buf;
    buf += BLCKSZ;
    single_cxt->buf = buf;

    single_cxt->single_flush_pos = (single_slot_pos*)palloc0(sizeof(single_slot_pos) * DW_SINGLE_DIRTY_PAGE_NUM);
    single_cxt->single_flush_state = (single_slot_state*)palloc0(sizeof(single_slot_state) * DW_SINGLE_DIRTY_PAGE_NUM);
    init_dw_single_flush_map();

    single_cxt->closed = 0;
    single_cxt->write_pos = 0;
    single_cxt->flush_page = 0;
}


void dw_init(bool shut_down)
{
    MemoryContext old_mem_cxt;
    knl_g_dw_context *batch_cxt = &g_instance.dw_batch_cxt;
    knl_g_dw_context *single_cxt = &g_instance.dw_single_cxt;

    MemoryContext mem_cxt = AllocSetContextCreate(
            INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE),
            "DoubleWriteContext",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE,
            SHARED_CONTEXT);

    g_instance.dw_batch_cxt.mem_cxt = mem_cxt;
    g_instance.dw_single_cxt.mem_cxt = mem_cxt;

    old_mem_cxt = MemoryContextSwitchTo(mem_cxt);

    dw_file_check_and_rebuild();
    ereport(LOG, (errmodule(MOD_DW), errmsg("Double Write init")));

    dw_cxt_init_batch();
    dw_cxt_init_single();

    /* recovery batch flush dw file */
    (void)LWLockAcquire(batch_cxt->flush_lock, LW_EXCLUSIVE);
    dw_recover_file_head(batch_cxt, false);

    dw_recover_partial_write(batch_cxt);
    LWLockRelease(batch_cxt->flush_lock);

    /* recovery single flush dw file */
    (void)LWLockAcquire(single_cxt->flush_lock, LW_EXCLUSIVE);
    dw_recover_file_head(single_cxt, true);
    if (!shut_down) {
        dw_recovery_partial_write_single();
    }
    /* reset the file after the recovery is complete */
    dw_force_reset_single_file();
    LWLockRelease(single_cxt->flush_lock);

    /*
     * After recovering partially written pages (if any), we will un-initialize, if the double write is disabled.
     */
    if (!dw_enabled()) {
        dw_free_resource(batch_cxt);
        dw_free_resource(single_cxt);
        (void)MemoryContextSwitchTo(old_mem_cxt);
        MemoryContextDelete(g_instance.dw_batch_cxt.mem_cxt);
        ereport(LOG, (errmodule(MOD_DW), errmsg("Double write exit after recovering partial write")));
    } else {
        (void)MemoryContextSwitchTo(old_mem_cxt);
    }

    smgrcloseall();
}

static void dw_encrypt_page(char *dest_addr)
{
    size_t plain_len, cipher_len;
    if (isEncryptedCluster() && !PageIsNew(dest_addr)) {
        plain_len = BLCKSZ - GetPageHeaderSize(dest_addr);
        encryptBlockOrCUData(PageGetContents(dest_addr), plain_len, PageGetContents(dest_addr), &cipher_len);
        Assert(plain_len == cipher_len);
        PageSetEncrypt((Page)dest_addr);
    }
}

static XLogRecPtr dw_copy_page(ThrdDwCxt* thrd_dw_cxt, int buf_desc_id, bool* is_skipped)
{
    /* we write different struct depend on hashbucket */
    dw_batch_t *batch = NULL;
    dw_batch_first_ver *batch_nohbkt = NULL;
    char *dest_addr = NULL;
    BufferDesc *buf_desc = NULL;
    XLogRecPtr page_lsn = InvalidXLogRecPtr;
    Block block;
    uint16 page_num;
    uint32 buf_state;
    errno_t rc;
    *is_skipped = true;

    buf_desc = GetBufferDescriptor(buf_desc_id);
    buf_state = LockBufHdr(buf_desc);
    if (!dw_buf_ckpt_needed(buf_state)) {
        buf_state &= (~BM_CHECKPOINT_NEEDED);
        UnlockBufHdr(buf_desc, buf_state);
        return page_lsn;
    }

    PinBuffer_Locked(buf_desc);

    /* We must use a conditional lock acquisition here to avoid deadlock. If
     * page_writer and double_write are enabled, only page_writer is allowed to
     * flush the buffers. So the backends (BufferAlloc, FlushRelationBuffers,
     * FlushDatabaseBuffers) are not allowed to flush the buffers, instead they
     * will just wait for page_writer to flush the required buffer. In some cases
     * (for example, btree split, heap_multi_insert), BufferAlloc will be called
     * with holding exclusive lock on another buffer. So if we try to acquire
     * the shared lock directly here (page_writer), it will block unconditionally
     * and the backends will be blocked on the page_writer to flush the buffer,
     * resulting in deadlock.
     */
    if (!LWLockConditionalAcquire(buf_desc->content_lock, LW_SHARED)) {
        UnpinBuffer(buf_desc, true);
        return page_lsn;
    }
    *is_skipped = false;
    thrd_dw_cxt->write_pos++;
    if (thrd_dw_cxt->write_pos <= GET_DW_BATCH_DATA_PAGE_MAX(thrd_dw_cxt->contain_hashbucket)) {
        batch = (dw_batch_t*)thrd_dw_cxt->dw_buf;
        page_num = thrd_dw_cxt->write_pos;
    } else {
        batch = (dw_batch_t*)(thrd_dw_cxt->dw_buf + 
            (GET_DW_BATCH_DATA_PAGE_MAX(thrd_dw_cxt->contain_hashbucket) + 1) * BLCKSZ);
        page_num = thrd_dw_cxt->write_pos - GET_DW_BATCH_DATA_PAGE_MAX(thrd_dw_cxt->contain_hashbucket);
    }
    if (thrd_dw_cxt->contain_hashbucket) {
        batch->buf_tag[page_num - 1] = buf_desc->tag;
    } else {
        /* change struct to nohash bucket */
        batch_nohbkt = (dw_batch_first_ver *)batch;
        batch_nohbkt->buf_tag[page_num - 1].blockNum = buf_desc->tag.blockNum;
        batch_nohbkt->buf_tag[page_num - 1].forkNum = buf_desc->tag.forkNum;
        batch_nohbkt->buf_tag[page_num - 1].rnode.dbNode = buf_desc->tag.rnode.dbNode;
        batch_nohbkt->buf_tag[page_num - 1].rnode.spcNode = buf_desc->tag.rnode.spcNode;
        batch_nohbkt->buf_tag[page_num - 1].rnode.relNode = buf_desc->tag.rnode.relNode;
    }
    dest_addr = (char *)batch + page_num * BLCKSZ;
    block = BufHdrGetBlock(buf_desc);
    rc = memcpy_s(dest_addr, BLCKSZ, block, BLCKSZ);
    securec_check(rc, "\0", "\0");

    LWLockRelease(buf_desc->content_lock);
    UnpinBuffer(buf_desc, true);

    page_lsn = PageGetLSN(dest_addr);
    dw_encrypt_page(dest_addr);

    dw_set_pg_checksum(dest_addr, buf_desc->tag.blockNum);
    return page_lsn;
}

inline uint16 dw_batch_add_extra(uint16 page_num)
{
    bool contain_hashbucket = g_instance.dw_batch_cxt.contain_hashbucket;
    Assert(page_num <= GET_DW_DIRTY_PAGE_MAX(contain_hashbucket));
    if (page_num <= GET_DW_BATCH_DATA_PAGE_MAX(contain_hashbucket)) {
        return page_num + DW_EXTRA_FOR_ONE_BATCH;
    } else {
        return page_num + DW_EXTRA_FOR_TWO_BATCH;
    }
}

static void dw_assemble_batch(knl_g_dw_context *dw_cxt, uint16 page_id, uint16 dwn)
{
    dw_batch_t *batch = NULL;
    uint16 first_batch_pages;
    uint16 second_batch_pages;

    if (dw_cxt->write_pos > GET_DW_BATCH_DATA_PAGE_MAX(dw_cxt->contain_hashbucket)) {
        first_batch_pages = GET_DW_BATCH_DATA_PAGE_MAX(dw_cxt->contain_hashbucket);
        second_batch_pages = dw_cxt->write_pos - GET_DW_BATCH_DATA_PAGE_MAX(dw_cxt->contain_hashbucket);
    } else {
        first_batch_pages = dw_cxt->write_pos;
        second_batch_pages = 0;
    }

    batch = (dw_batch_t *)dw_cxt->buf;
    dw_prepare_page(batch, first_batch_pages, page_id, dwn);

    /* tail of the first batch */
    page_id = page_id + 1 + GET_REL_PGAENUM(batch->page_num);
    batch = dw_batch_tail_page(batch);
    dw_prepare_page(batch, second_batch_pages, page_id, dwn);

    if (second_batch_pages == 0) {
        return;
    }
    /* also head of the second batch, if second batch not empty, prepare its tail */
    page_id = page_id + 1 + GET_REL_PGAENUM(batch->page_num);
    batch = dw_batch_tail_page(batch);
    dw_prepare_page(batch, 0, page_id, dwn);
}

static inline void dw_stat_batch_flush(dw_stat_info_batch *stat_info, uint32 page_to_write)
{
    (void)pg_atomic_add_fetch_u64(&stat_info->total_writes, 1);
    (void)pg_atomic_add_fetch_u64(&stat_info->total_pages, page_to_write);
    if (page_to_write < DW_WRITE_STAT_LOWER_LIMIT) {
        (void)pg_atomic_add_fetch_u64(&stat_info->low_threshold_writes, 1);
        (void)pg_atomic_add_fetch_u64(&stat_info->low_threshold_pages, page_to_write);
    } else if (page_to_write > GET_DW_BATCH_MAX(g_instance.dw_batch_cxt.contain_hashbucket)) {
        (void)pg_atomic_add_fetch_u64(&stat_info->high_threshold_writes, 1);
        (void)pg_atomic_add_fetch_u64(&stat_info->high_threshold_pages, page_to_write);
    }
}

/**
 * flush the copied page in the buffer into dw file, allocate the token for outside data file flushing
 * @param dw_cxt double write context
 * @param latest_lsn the latest lsn in the copied pages
 */
static void dw_batch_flush(knl_g_dw_context* dw_cxt, XLogRecPtr latest_lsn, ThrdDwCxt* thrd_dw_cxt)
{
    uint16 offset_page;
    uint16 pages_to_write = 0;
    dw_file_head_t* file_head = NULL;
    errno_t rc;

    if (!XLogRecPtrIsInvalid(latest_lsn)) {
        XLogWaitFlush(latest_lsn);
        g_instance.ckpt_cxt_ctl->page_writer_xlog_flush_loc = latest_lsn;
    }

    (void)LWLockAcquire(dw_cxt->flush_lock, LW_EXCLUSIVE);

    if (thrd_dw_cxt->contain_hashbucket) {
        dw_cxt->contain_hashbucket = true;
    }

    dw_cxt->write_pos = thrd_dw_cxt->write_pos;
    Assert(dw_cxt->write_pos > 0);

    file_head = dw_cxt->file_head;
    pages_to_write = dw_batch_add_extra(dw_cxt->write_pos);
    rc = memcpy_s(dw_cxt->buf, pages_to_write * BLCKSZ, thrd_dw_cxt->dw_buf, pages_to_write * BLCKSZ);
    securec_check(rc, "\0", "\0");
    (void)dw_reset_if_need(dw_cxt, pages_to_write, false);

    /* calculate it after checking file space, in case of updated by sync */
    offset_page = file_head->start + dw_cxt->flush_page;

    dw_assemble_batch(dw_cxt, offset_page, file_head->head.dwn);

    pgstat_report_waitevent(WAIT_EVENT_DW_WRITE);
    dw_pwrite_file(dw_cxt->fd, dw_cxt->buf, (pages_to_write * BLCKSZ), (offset_page * BLCKSZ));
    pgstat_report_waitevent(WAIT_EVENT_END);

    dw_stat_batch_flush(&dw_cxt->batch_stat_info, pages_to_write);
    /* the tail of this flushed batch is the head of the next batch */
    dw_cxt->flush_page += (pages_to_write - 1);
    dw_cxt->write_pos = 0;
    dw_cxt->contain_hashbucket = false;
    thrd_dw_cxt->dw_page_idx = offset_page;
    LWLockRelease(dw_cxt->flush_lock);

    ereport(DW_LOG_LEVEL,
            (errmodule(MOD_DW),
             errmsg("[batch flush] file_head[dwn %hu, start %hu], total_pages %hu, data_pages %hu, flushed_pages %hu",
                    dw_cxt->file_head->head.dwn, dw_cxt->file_head->start, dw_cxt->flush_page, dw_cxt->write_pos,
                    pages_to_write)));
}

void dw_perform_batch_flush(uint32 size, CkptSortItem *dirty_buf_list, ThrdDwCxt* thrd_dw_cxt)
{
    uint16 batch_size;
    knl_g_dw_context *dw_cxt = &g_instance.dw_batch_cxt;
    XLogRecPtr latest_lsn = InvalidXLogRecPtr;
    XLogRecPtr page_lsn;

    if (!dw_enabled()) {
        /* Double write is not enabled, nothing to do. */
        return;
    }

    if (SECUREC_UNLIKELY(pg_atomic_read_u32(&dw_cxt->closed))) {
        ereport(ERROR, (errmodule(MOD_DW), errmsg("[batch flush] Double write already closed")));
    }

    Assert(size > 0 && size <= GET_DW_DIRTY_PAGE_MAX(thrd_dw_cxt->contain_hashbucket));
    batch_size = (uint16)size;
    thrd_dw_cxt->write_pos = 0;

    for (uint16 i = 0; i < batch_size; i++) {
        bool is_skipped = false;
        if (dirty_buf_list == NULL) {
            page_lsn = dw_copy_page(thrd_dw_cxt, g_instance.ckpt_cxt_ctl->CkptBufferIds[i].buf_id, &is_skipped);
        } else {
            page_lsn = dw_copy_page(thrd_dw_cxt, dirty_buf_list[i].buf_id, &is_skipped);
        }
        if (is_skipped) {
            /*
             * We couldn't acquire conditional lock on the buffer content_lock.
             * So we mark it in buf_id_arr.
             */
            BufferDesc *buf_desc = NULL;
            if (dirty_buf_list == NULL) {
                buf_desc = GetBufferDescriptor(g_instance.ckpt_cxt_ctl->CkptBufferIds[i].buf_id);
                g_instance.ckpt_cxt_ctl->CkptBufferIds[i].buf_id = DW_INVALID_BUFFER_ID;
            } else {
                buf_desc = GetBufferDescriptor(dirty_buf_list[i].buf_id);
                dirty_buf_list[i].buf_id = DW_INVALID_BUFFER_ID;
            }
            clean_buf_need_flush_flag(buf_desc);
            continue;
        }

        if (XLByteLT(latest_lsn, page_lsn)) {
            latest_lsn = page_lsn;
        }
    }
    if (force_finish_enabled()) {
        update_max_page_flush_lsn(latest_lsn, t_thrd.proc_cxt.MyProcPid, false);
    }
    if (thrd_dw_cxt->write_pos > 0) {
        dw_batch_flush(dw_cxt, latest_lsn, thrd_dw_cxt);
    }
}
void dw_truncate_batch_file()
{
    knl_g_dw_context *cxt = &g_instance.dw_batch_cxt;

    ereport(DW_LOG_LEVEL,
        (errmodule(MOD_DW),
            errmsg("[batch flush] DW truncate start: file_head[dwn %hu, start %hu], total_pages %hu",
                cxt->file_head->head.dwn, cxt->file_head->start, cxt->flush_page)));
    /*
     * If we can grab dw flush lock, truncate dw file for faster recovery.
     *
     * Note: This is only for recovery optimization. we can not block on
     * dw flush lock, because, if we are checkpointer, pagewriter may be
     * waiting for us to finish smgrsync before it can do a full recycle of dw file.
     */
    if (!LWLockConditionalAcquire(cxt->flush_lock, LW_EXCLUSIVE)) {
        ereport(LOG, (errmodule(MOD_DW),
            errmsg("[batch flush] Can not get dw flush lock and skip dw truncate for this time")));
        return;
    }
    if (dw_reset_if_need(cxt, 0, true)) {
        LWLockRelease(cxt->flush_lock);
    }

    ereport(LOG, (errmodule(MOD_DW),
        errmsg("[batch flush] DW truncate end: file_head[dwn %hu, start %hu], total_pages %hu",
            cxt->file_head->head.dwn, cxt->file_head->start, cxt->flush_page)));
}

void dw_truncate_single_file()
{
    knl_g_dw_context *cxt = &g_instance.dw_single_cxt;
   
    if (!LWLockConditionalAcquire(cxt->flush_lock, LW_EXCLUSIVE)) {
        ereport(LOG, (errmodule(MOD_DW),
            errmsg("[single flush] can not get dw flush lock and skip dw truncate for this time")));
        return;
    }
    if (dw_single_file_recycle(true)) {
        LWLockRelease(cxt->flush_lock);
    }
    ereport(LOG, (errmodule(MOD_DW),
        errmsg("[single flush] DW truncate end: file_head[dwn %hu, start %hu], total_pages %hu",
            cxt->file_head->head.dwn, cxt->file_head->start, cxt->flush_page)));
}

void dw_truncate()
{
    if (!dw_enabled()) {
        /* Double write is not enabled, nothing to do. */
        return;
    }

    gstrace_entry(GS_TRC_ID_dw_truncate);
    dw_truncate_batch_file();
    dw_truncate_single_file();
    gstrace_exit(GS_TRC_ID_dw_truncate);
}

void dw_exit(bool single)
{
    knl_g_dw_context *dw_cxt = NULL;
    uint32 expected = 0;

    if (!dw_enabled()) {
        /* Double write is not enabled, nothing to do. */
        return;
    }

    if (single) {
        dw_cxt = &g_instance.dw_single_cxt;
    } else {
        dw_cxt = &g_instance.dw_batch_cxt;
    }

    if (!pg_atomic_compare_exchange_u32(&dw_cxt->closed, &expected, 1)) {
        ereport(WARNING, (errmodule(MOD_DW), errmsg("Double write already closed")));
        return;
    }

    Assert(pg_atomic_read_u32(&g_instance.ckpt_cxt_ctl->current_page_writer_count) == 0);

    ereport(LOG, (errmodule(MOD_DW), errmsg("Double write exit")));

    /* Do a final truncate before free resource. */
    if (single) {
        dw_truncate_single_file();
    } else {
        dw_truncate_batch_file();
    }

    dw_free_resource(dw_cxt);

}

static void dw_generate_single_file()
{
    char *file_head = NULL;
    int64 remain_size;
    int fd = -1;
    errno_t rc;
    char *unaligned_buf = NULL;

    if (file_exists(SINGLE_DW_FILE_NAME)) {
        ereport(PANIC, (errcode_for_file_access(), errmodule(MOD_DW), "DW single flush file already exists"));
    }

    ereport(LOG, (errmodule(MOD_DW), errmsg("DW bootstrap single flush file")));

    fd = open(SINGLE_DW_FILE_NAME, (DW_FILE_FLAG | O_CREAT), DW_FILE_PERM);
    if (fd == -1) {
        ereport(PANIC,
                (errcode_for_file_access(), errmodule(MOD_DW), errmsg("Could not create file \"%s\"",
                    SINGLE_DW_FILE_NAME)));
    }
    
    unaligned_buf = (char *)palloc0(DW_FILE_EXTEND_SIZE + BLCKSZ); /* one more BLCKSZ for alignment */

    file_head = (char *)TYPEALIGN(BLCKSZ, unaligned_buf);
 
    /* file head and first batch head will be writen */
    remain_size = (DW_SINGLE_DIRTY_PAGE_NUM + DW_SINGLE_BUFTAG_PAGE_NUM) * BLCKSZ;
    dw_prepare_file_head(file_head, 0, 0);
    pgstat_report_waitevent(WAIT_EVENT_DW_WRITE);
    dw_pwrite_file(fd, file_head, BLCKSZ, 0);
    rc = memset_s(file_head, BLCKSZ, 0, BLCKSZ);
    securec_check(rc, "\0", "\0");
    dw_extend_file(fd, file_head, DW_FILE_EXTEND_SIZE, remain_size, true);
    pgstat_report_waitevent(WAIT_EVENT_END);
    ereport(LOG, (errmodule(MOD_DW), errmsg("Double write single flush file created successfully")));

    (void)close(fd);
    fd = -1;
    pfree(unaligned_buf);
    return;
}

uint16 dw_single_flush(BufferDesc *buf_desc)
{
    errno_t rc;
    uint32 buf_state;
    XLogRecPtr page_lsn;
    uint16 map_pos;
    uint16 actual_pos;
    uint32 page_write_offset;
    uint32 tag_write_offset;
    uint16 block_offset;
    dw_single_flush_item item;
    knl_g_dw_context* dw_single_cxt = &g_instance.dw_single_cxt;
    dw_file_head_t *file_head = dw_single_cxt->file_head;
    char *buf = t_thrd.proc->dw_buf;
    Block block = BufHdrGetBlock(buf_desc);

    /* first step, copy buffer to dw buf, than flush page lsn, the buffer content lock  is already held */
    rc = memcpy_s(buf, BLCKSZ, block, BLCKSZ);
    securec_check(rc, "\0", "\0");

    buf_state = LockBufHdr(buf_desc);
    page_lsn = BufferGetLSN(buf_desc);
    UnlockBufHdr(buf_desc, buf_state);
    XLogWaitFlush(page_lsn);
    dw_encrypt_page(buf);
    dw_set_pg_checksum(buf, buf_desc->tag.blockNum);

    pgstat_report_waitevent(WAIT_EVENT_DW_SINGLE_POS);
    (void)LWLockAcquire(dw_single_cxt->flush_lock, LW_EXCLUSIVE);
    (void)dw_single_file_recycle(false);
    map_pos = dw_single_cxt->write_pos;
    dw_single_cxt->write_pos = (dw_single_cxt->write_pos + 1) % DW_SINGLE_DIRTY_PAGE_NUM;
    LWLockRelease(dw_single_cxt->flush_lock);
    pgstat_report_waitevent(WAIT_EVENT_END);

    /* data page need skip head page and bufferTag page, bufferTag page need skip head page */
    actual_pos = dw_single_cxt->single_flush_pos[map_pos].actual_pos;
    page_write_offset = (actual_pos + DW_SINGLE_BUFTAG_PAGE_NUM + 1) * BLCKSZ;

    item.data_page_idx = actual_pos;
    item.dwn = file_head->head.dwn;
    item.buf_tag = buf_desc->tag;

    /* Contents are protected with a CRC */
    INIT_CRC32C(item.crc);
    COMP_CRC32C(item.crc, (char*)&item, offsetof(dw_single_flush_item, crc));
    FIN_CRC32C(item.crc);

    /*
     * Write the buffer tag information to dw file:
     * 1. write the data file to dw file.
     * 2. Hold the block exckusive lock based on pos.
     * 3. read the whole block content, write the buffer tage item to corresponding pos.
     * 4. write the whole block to dw file, release lock
     */
    pgstat_report_waitevent(WAIT_EVENT_DW_SINGLE_WRITE);
    dw_pwrite_file(dw_single_cxt->fd, buf, BLCKSZ, page_write_offset);
    pgstat_report_waitevent(WAIT_EVENT_END);
    (void)pg_atomic_add_fetch_u64(&dw_single_cxt->single_stat_info.total_writes, 1);

    tag_write_offset = BLCKSZ + (actual_pos / SINGLE_BLOCK_TAG_NUM) * BLCKSZ;
    block_offset = (actual_pos % SINGLE_BLOCK_TAG_NUM) * sizeof(dw_single_flush_item);
    Assert(block_offset <= BLCKSZ - sizeof(dw_single_flush_item));

    pgstat_report_waitevent(WAIT_EVENT_DW_SINGLE_WRITE);
    (void)LWLockAcquire(dw_single_cxt->single_flush_pos[map_pos].write_lock, LW_EXCLUSIVE);
    dw_pread_file(dw_single_cxt->fd, buf, BLCKSZ, tag_write_offset);
    rc = memcpy_s(buf + block_offset, BLCKSZ - block_offset, &item, sizeof(dw_single_flush_item));
    securec_check(rc, "\0", "\0");
    dw_pwrite_file(dw_single_cxt->fd, buf, BLCKSZ, tag_write_offset);
    LWLockRelease(dw_single_cxt->single_flush_pos[map_pos].write_lock);
    pgstat_report_waitevent(WAIT_EVENT_END);

    (void)pg_atomic_add_fetch_u64(&dw_single_cxt->single_stat_info.total_writes, 1);

    return map_pos;
}

int page_idx_compare(const void* arg1, const void* arg2)
{
    uint16 idx1 = *(const uint16*)arg1;
    uint16 idx2 = *(const uint16*)arg2;

    if (idx1 > idx2)
        return 1;
    if (idx1 < idx2)
        return -1;
    return 0;
}

void dw_empty_buftag_page(uint16 org_start, uint16 max_idx)
{
    knl_g_dw_context* dw_single_cxt = &g_instance.dw_single_cxt;
    MemoryContext old_mem_cxt = MemoryContextSwitchTo(dw_single_cxt->mem_cxt);
    uint16 *page_idx = (uint16*)palloc0(DW_SINGLE_DIRTY_PAGE_NUM * sizeof(uint16));
    char *unaligned_buf = (char *)palloc0(BLCKSZ + BLCKSZ); /* one more BLCKSZ for alignment */
    char *buf = (char *)TYPEALIGN(BLCKSZ, unaligned_buf);
    errno_t rc;
    uint16 empty_num = 0;

    /* get the page actual pos which buffer tag item need empty */
    if (org_start < max_idx) {
        for (int i = org_start; i < max_idx; i++) {
            page_idx[empty_num++] = dw_single_cxt->single_flush_pos[i].actual_pos;
        }
    } else {
        for (int i = org_start; i < DW_SINGLE_DIRTY_PAGE_NUM; i++) {
            page_idx[empty_num++] = dw_single_cxt->single_flush_pos[i].actual_pos;
        }

        for (int i = 0; i < max_idx; i++) {
            page_idx[empty_num++] = dw_single_cxt->single_flush_pos[i].actual_pos;
        }
    }

    qsort(page_idx, empty_num, sizeof(uint16), page_idx_compare);

    uint16 batch = page_idx[0] / SINGLE_BLOCK_TAG_NUM; /* calculate begin batch */
    uint16 batch_begin = page_idx[0];
    uint32 start_offset;
    uint32 set_size;
    uint16 batch_num = (DW_SINGLE_DIRTY_PAGE_NUM / SINGLE_BLOCK_TAG_NUM) +
        (DW_SINGLE_DIRTY_PAGE_NUM % SINGLE_BLOCK_TAG_NUM == 0 ? 0 : 1);

    for (int i = 0; i < empty_num; i++) {
        if (page_idx[i] >= SINGLE_BLOCK_TAG_NUM * (batch + 1)) {
            LWLock *pos_lock = dw_single_cxt->single_flush_pos[batch].write_lock;
            uint32 tag_offset = (batch + 1) * BLCKSZ; /* need skip file head */
            (void)LWLockAcquire(pos_lock, LW_EXCLUSIVE);
            dw_pread_file(dw_single_cxt->fd, buf, BLCKSZ, tag_offset);

            start_offset = (batch_begin % SINGLE_BLOCK_TAG_NUM) * sizeof(dw_single_flush_item);
            set_size = (page_idx[i - 1] - batch_begin + 1) * sizeof(dw_single_flush_item);

            rc = memset_s(buf + start_offset, set_size, 0, set_size);
            securec_check(rc, "\0", "\0");
            dw_pwrite_file(dw_single_cxt->fd, buf, BLCKSZ, tag_offset);
            LWLockRelease(pos_lock);
            batch++;
            batch_begin = page_idx[i];
        }

        /* empty the last batch */
        if (batch >= batch_num - 1) {
            LWLock *pos_lock = dw_single_cxt->single_flush_pos[batch].write_lock;
            uint32 tag_offset = (batch + 1) * BLCKSZ; /* need skip file head */
            (void)LWLockAcquire(pos_lock, LW_EXCLUSIVE);
            dw_pread_file(dw_single_cxt->fd, buf, BLCKSZ, tag_offset);
            start_offset = (batch_begin % SINGLE_BLOCK_TAG_NUM) * sizeof(dw_single_flush_item);
            set_size = (page_idx[empty_num - 1] - batch_begin + 1) * sizeof(dw_single_flush_item);

            rc = memset_s(buf + start_offset, set_size, 0, set_size);
            securec_check(rc, "\0", "\0");
            dw_pwrite_file(dw_single_cxt->fd, buf, BLCKSZ, tag_offset);
            LWLockRelease(pos_lock);
            break;
        }
    }

    pfree(page_idx);
    pfree(unaligned_buf);
    MemoryContextSwitchTo(old_mem_cxt);
    return;
}

void dw_force_reset_single_file()
{
    knl_g_dw_context* single_cxt = &g_instance.dw_single_cxt;
    dw_file_head_t *file_head = single_cxt->file_head;

    smgrsync_for_dw();

    dw_prepare_file_head((char *)file_head, 0, file_head->head.dwn + 1);
    Assert(file_head->head.dwn == file_head->tail.dwn);
    pgstat_report_waitevent(WAIT_EVENT_DW_WRITE);
    dw_pwrite_file(single_cxt->fd, file_head, BLCKSZ, 0);
    pgstat_report_waitevent(WAIT_EVENT_END);

    (void)pg_atomic_add_fetch_u64(&single_cxt->single_stat_info.file_reset_num, 1);
    ereport(LOG, (errmodule(MOD_DW),
        errmsg("DW single flush finish recovery, reset the file head[dwn %hu, start %hu].",
            file_head->head.dwn, file_head->start)));
    return;
}

uint16 get_max_single_write_pos()
{
    uint16 max_idx = 0;
    uint16 i = 0;
    knl_g_dw_context* dw_single_cxt = &g_instance.dw_single_cxt;
    dw_file_head_t *file_head = dw_single_cxt->file_head;

    if (file_head->start <= dw_single_cxt->write_pos) {
        for (i = file_head->start; i < dw_single_cxt->write_pos; i++) {
            if (dw_single_cxt->single_flush_state[i].data_flush == false) {
                break;
            }
        }
        max_idx = i;
    } else {
        /* first from start to last page */
        for (i = file_head->start; i < DW_SINGLE_DIRTY_PAGE_NUM; i++) {
            if (dw_single_cxt->single_flush_state[i].data_flush == false) {
                break;
            }
        }
        if (i == DW_SINGLE_DIRTY_PAGE_NUM) {
            for (i = 0; i < dw_single_cxt->write_pos; i++) {
                if (dw_single_cxt->single_flush_state[i].data_flush == false) {
                    break;
                }
            }
            
        }
        max_idx = i;
    }
	
    return max_idx;
}

void wait_all_single_dw_finish_flush()
{
    knl_g_dw_context* dw_single_cxt = &g_instance.dw_single_cxt;
    dw_file_head_t *file_head = dw_single_cxt->file_head;

    if (file_head->start <= dw_single_cxt->write_pos) {
        for (int i = file_head->start; i < dw_single_cxt->write_pos; ) {
            if (dw_single_cxt->single_flush_state[i].data_flush != false) {
                i++;
                continue;
            } else {
                (void)sched_yield();
            }
        }
    } else {
        for (int i = file_head->start; i < DW_SINGLE_DIRTY_PAGE_NUM; ) {
            if (dw_single_cxt->single_flush_state[i].data_flush != false) {
                i++;
                continue;
            } else {
                (void)sched_yield();
            }
        }

        for (int i = 0; i < dw_single_cxt->write_pos; ) {
            if (dw_single_cxt->single_flush_state[i].data_flush != false) {
                i++;
                continue;
            } else {
                (void)sched_yield();
            }
        }
    }
    return;
}

bool dw_single_file_recycle(bool trunc_file)
{
    bool file_full = false;
    uint16 max_idx = 0;
    knl_g_dw_context* single_cxt = &g_instance.dw_single_cxt;
    dw_file_head_t *file_head = single_cxt->file_head;
    volatile uint16 org_start = file_head->start;
    volatile uint16 org_dwn = file_head->head.dwn;
    uint16 end = single_cxt->write_pos;
    errno_t rc;

    if (file_head->start != 0) {
        if (end < file_head->start) {
            file_full = end + 1 >= file_head->start;
        } else {
            file_full = false;
        }
    } else {
        file_full = end + 1 >= DW_SINGLE_DIRTY_PAGE_NUM;
    }

    if (!file_full && !trunc_file) {
        return true;
    }

    if (trunc_file) {
        max_idx = get_max_single_write_pos();
        if (max_idx == file_head->start) {
            LWLockRelease(single_cxt->flush_lock);
            return false;
        }
        LWLockRelease(single_cxt->flush_lock);
    } else {
        /* reset start position and flush page num for full recycle */
        wait_all_single_dw_finish_flush();
        file_head->start = 0;
        single_cxt->write_pos = 0;
        single_cxt->flush_page = 0;
        rc = memset_s(single_cxt->single_flush_state, sizeof(single_slot_state) * DW_SINGLE_DIRTY_PAGE_NUM,
            0, sizeof(single_slot_state) * DW_SINGLE_DIRTY_PAGE_NUM);
        securec_check(rc, "\0", "\0");
    }

    smgrsync_for_dw();

    if (trunc_file) {
        dw_empty_buftag_page(org_start, max_idx);
        if (!LWLockConditionalAcquire(single_cxt->flush_lock, LW_EXCLUSIVE)) {
            ereport(LOG, (errmodule(MOD_DW),
                errmsg("[single flush] can not get dw flush lock and skip dw truncate after sync for this time")));
            return false;
        } else if (org_start != file_head->start || org_dwn != file_head->head.dwn) {
            ereport(DW_LOG_LEVEL, (errmodule(MOD_DW),
                errmsg("[single flush] Skip dw truncate after sync due to concurrent dw truncate/reset, "
                    "original[dwn %hu, start %hu], current[dwn %hu, start %hu]",
                    org_dwn, org_start, file_head->head.dwn, file_head->start)));
            return true;
        }
    }

    /*
     * if truncate file and flush_page is not 0, the dwn can not plus,
     * otherwise verify will failed when recovery the data form dw file.
     */
    if (trunc_file) {
        file_head->start = max_idx;
        if (org_start < file_head->start) {
            rc = memset_s(single_cxt->single_flush_state + org_start, 
            sizeof(single_slot_state) * (file_head->start - org_start),
            0, sizeof(single_slot_state) * (file_head->start - org_start));
            securec_check(rc, "\0", "\0");
        } else {
            rc = memset_s(single_cxt->single_flush_state + org_start, 
                sizeof(single_slot_state) * (DW_SINGLE_DIRTY_PAGE_NUM - org_start),
                0, sizeof(single_slot_state) * (DW_SINGLE_DIRTY_PAGE_NUM - org_start));
            securec_check(rc, "\0", "\0");

            rc = memset_s(single_cxt->single_flush_state, sizeof(single_slot_state) * file_head->start,
                0, sizeof(single_slot_state) * file_head->start);
            securec_check(rc, "\0", "\0");
        }

        dw_prepare_file_head((char *)file_head, file_head->start, file_head->head.dwn);
    } else {
        dw_prepare_file_head((char *)file_head, file_head->start, file_head->head.dwn + 1);
    }

    Assert(file_head->head.dwn == file_head->tail.dwn);
    pgstat_report_waitevent(WAIT_EVENT_DW_SINGLE_WRITE);
    dw_pwrite_file(single_cxt->fd, file_head, BLCKSZ, 0);
    pgstat_report_waitevent(WAIT_EVENT_END);

    ereport(LOG, (errmodule(MOD_DW), errmsg("Reset single flush DW file: file_head[dwn %hu, start %hu], "
        "writer pos is %hu", file_head->head.dwn, file_head->start, single_cxt->write_pos)));

    if (trunc_file) {
        (void)pg_atomic_add_fetch_u64(&single_cxt->single_stat_info.file_trunc_num, 1);
    } else {
        (void)pg_atomic_add_fetch_u64(&single_cxt->single_stat_info.file_reset_num, 1);
    }
    return true;
}

bool dw_verify_item(const dw_single_flush_item* item, uint16 dwn)
{
    if (item->data_page_idx == 0 || item->dwn != dwn) {
        return false;
    }
    pg_crc32c crc;
    /* Contents are protected with a CRC */
    INIT_CRC32C(crc);
    COMP_CRC32C(crc, (char*)item, offsetof(dw_single_flush_item, crc));
    FIN_CRC32C(crc);

    if (EQ_CRC32C(crc, item->crc)) {
        return true;
    } else {
        return false;
    }
}

int buftag_compare(const void *pa, const void *pb)
{
    const dw_single_flush_item *a = (dw_single_flush_item *)pa;
    const dw_single_flush_item *b = (dw_single_flush_item *)pb;

    /* compare tablespace */
    if (a->buf_tag.rnode.spcNode < b->buf_tag.rnode.spcNode) {
        return -1;
    } else if (a->buf_tag.rnode.spcNode > b->buf_tag.rnode.spcNode) {
        return 1;
    }

    /* compare relation */
    if (a->buf_tag.rnode.relNode < b->buf_tag.rnode.relNode) {
        return -1;
    } else if (a->buf_tag.rnode.relNode > b->buf_tag.rnode.relNode) {
        return 1;
    }

    if (a->buf_tag.rnode.bucketNode < b->buf_tag.rnode.bucketNode) {
        return -1;
    } else if (a->buf_tag.rnode.bucketNode > b->buf_tag.rnode.bucketNode) {
        return 1;
    } else if (a->buf_tag.forkNum < b->buf_tag.forkNum) { /* compare fork */
        return -1;
    } else if (a->buf_tag.forkNum > b->buf_tag.forkNum) {
        return 1;
        /* compare block number */
    } else if (a->buf_tag.blockNum < b->buf_tag.blockNum) {
        return -1;
    } else {
        return 1;
    }

    if (a->data_page_idx < b->data_page_idx) {
        return -1;
    } else {
        return 1;
    }
}

static inline void dw_log_recovery_page(int elevel, const char *state, BufferTag buf_tag)
{
    ereport(elevel, (errmodule(MOD_DW),
        errmsg("[single flush] recovery, %s: buf_tag[rel %u/%u/%u blk %u fork %d]",
            state, buf_tag.rnode.spcNode, buf_tag.rnode.dbNode, buf_tag.rnode.relNode, buf_tag.blockNum,
            buf_tag.forkNum)));
}

void dw_recovery_page_single(const dw_single_flush_item *item, uint16 item_num)
{
    SMgrRelation relation;
    BlockNumber blk_num;
    uint32 offset = 0;
    BufferTag buf_tag;
    knl_g_dw_context* single_cxt = &g_instance.dw_single_cxt;
    char *unaligned_buf = (char *)palloc0(BLCKSZ + BLCKSZ); /* one more BLCKSZ for alignment */
    char *dw_block = (char *)TYPEALIGN(BLCKSZ, unaligned_buf);
    char *data_block = (char *)palloc0(BLCKSZ);

    for (uint16 i = 0; i < item_num; i++) {
        buf_tag = item[i].buf_tag;
        relation = smgropen(buf_tag.rnode, InvalidBackendId, GetColumnNum(buf_tag.forkNum));
        if (!smgrexists(relation, buf_tag.forkNum)) {
            dw_log_recovery_page(WARNING, "Data file deleted", buf_tag);
            continue;
        }
        blk_num = smgrnblocks(relation, buf_tag.forkNum);
        if (blk_num <= buf_tag.blockNum) {
            dw_log_recovery_page(WARNING, "Data page deleted", buf_tag);
            continue;
        }

        /* read dw file page */
        offset = (DW_SINGLE_BUFTAG_PAGE_NUM + 1 + item[i].data_page_idx) * BLCKSZ;
        dw_pread_file(single_cxt->fd, dw_block, BLCKSZ, offset);

        if (!dw_verify_pg_checksum((PageHeader)dw_block, buf_tag.blockNum)) {
            dw_log_recovery_page(WARNING, "DW page broken", buf_tag);
            dw_log_page_header((PageHeader)dw_block);
            continue;
        }

        dw_log_recovery_page(DW_LOG_LEVEL, "DW page fine", buf_tag);

        /* read data page */
        smgrread(relation, buf_tag.forkNum, buf_tag.blockNum, data_block);

        dw_log_page_header((PageHeader)data_block);
        if (!dw_verify_pg_checksum((PageHeader)data_block, buf_tag.blockNum) ||
            XLByteLT(PageGetLSN(data_block), PageGetLSN(dw_block))) {
            smgrwrite(relation, buf_tag.forkNum, buf_tag.blockNum, (const char *)dw_block, false);
            dw_log_recovery_page(LOG, "Date page recovered", buf_tag);
            dw_log_page_header((PageHeader)data_block);
        }
    }

    pfree(unaligned_buf);
    pfree(data_block);
    return;
}

static void dw_recovery_partial_write_single()
{
    dw_single_flush_item *item = (dw_single_flush_item*)palloc0(sizeof(dw_single_flush_item) *
        DW_SINGLE_DIRTY_PAGE_NUM);
    knl_g_dw_context* single_cxt = &g_instance.dw_single_cxt;
    uint16 batch_size = SINGLE_BLOCK_TAG_NUM;
    dw_file_head_t *file_head = single_cxt->file_head;
    uint16 blk_num = (DW_SINGLE_DIRTY_PAGE_NUM / batch_size) + (DW_SINGLE_DIRTY_PAGE_NUM % batch_size == 0 ? 0 : 1);
    char *buf = single_cxt->buf;

    /* read all buffer tag item, need skip head page */
    dw_pread_file(single_cxt->fd, buf, blk_num * BLCKSZ, BLCKSZ);
    uint16 rec_num = 0;
    int offset = 0;
    dw_single_flush_item *temp = NULL;
    for (int i = 0; i < blk_num; i++) {
        for (int j = i * batch_size; j < (i + 1) * batch_size; j++) {
            offset = BLCKSZ * i + j % SINGLE_BLOCK_TAG_NUM * sizeof(dw_single_flush_item);
            temp = (dw_single_flush_item*)((char*)buf + offset);
            if (dw_verify_item(temp, file_head->head.dwn)) {
                item[rec_num].data_page_idx = temp->data_page_idx;
                item[rec_num].dwn = temp->dwn;
                item[rec_num].buf_tag = temp->buf_tag;
                item[rec_num].crc = temp->crc;
                rec_num++;
            }
        }
    }
    ereport(LOG, (errmodule(MOD_DW), errmsg("DW single flush file valid page num is %d.", rec_num)));

    if (rec_num > 0) {
        qsort(item, rec_num, sizeof(dw_single_flush_item), buftag_compare);
        ereport(LOG, (errmodule(MOD_DW), errmsg("DW single flush file valid page qsort finish.")));
        dw_recovery_page_single(item, rec_num);
    }
    ereport(LOG, (errmodule(MOD_DW), errmsg("DW single flush file recovery finish.")));

    pfree(item);
    return;
}

/*
 * If the dw is enable, and the pagewriter thread is running, indicates that the device is not in the initialization
 * phase, when the version num smaller than DW_SUPPORT_SINGLE_FLUSH_VERSION, not support the 
 * backend thread flush dirty page.
 */
bool backend_can_flush_dirty_page()
{
    if (dw_enabled() && pg_atomic_read_u32(&g_instance.ckpt_cxt_ctl->current_page_writer_count) > 0 &&
        (t_thrd.proc->workingVersionNum < DW_SUPPORT_SINGLE_FLUSH_VERSION)) {
        Assert(g_instance.dw_single_cxt.closed == 0);
        return false;
    }
    return true;
}


void init_proc_dw_buf()
{
    MemoryContext oldContext = MemoryContextSwitchTo(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));
    t_thrd.proc->dw_unaligned_buf = (char*)palloc0(BLCKSZ + BLCKSZ);
    t_thrd.proc->dw_buf = (char *)TYPEALIGN(BLCKSZ, t_thrd.proc->dw_unaligned_buf);
    t_thrd.proc->dw_pos = -1;
    (void)MemoryContextSwitchTo(oldContext);
}

void reset_dw_pos_flag()
{
    if (t_thrd.proc->dw_pos != -1) {
        uint16 pos = t_thrd.proc->dw_pos;
        Assert(pos >= 0 && pos < DW_SINGLE_DIRTY_PAGE_NUM);
        g_instance.dw_single_cxt.single_flush_state[pos].data_flush = true;
        t_thrd.proc->dw_pos = -1;
    }
}

void clean_proc_dw_buf()
{
    if (t_thrd.proc != NULL && t_thrd.proc->dw_unaligned_buf != NULL) {
        pfree(t_thrd.proc->dw_unaligned_buf);
        t_thrd.proc->dw_unaligned_buf = NULL;
        t_thrd.proc->dw_buf = NULL;
        reset_dw_pos_flag();
    }
}
