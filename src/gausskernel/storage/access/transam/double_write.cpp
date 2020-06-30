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
#include "utils/elog.h"
#include "utils/builtins.h"
#include "access/double_write.h"
#include "pgstat.h"
#include "utils/palloc.h"
#include "gstrace/gstrace_infra.h"
#include "gstrace/access_gstrace.h"

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

Datum dw_get_dw_number()
{
    if (dw_enabled()) {
        return UInt64GetDatum((uint64)g_instance.dw_cxt.file_head->head.dwn);
    }

    return UInt64GetDatum(0);
}

Datum dw_get_start_page()
{
    if (dw_enabled()) {
        return UInt64GetDatum((uint64)g_instance.dw_cxt.file_head->start);
    }

    return UInt64GetDatum(0);
}

Datum dw_get_file_trunc_num()
{
    return UInt64GetDatum(g_instance.dw_cxt.stat_info.file_trunc_num);
}

Datum dw_get_file_reset_num()
{
    return UInt64GetDatum(g_instance.dw_cxt.stat_info.file_reset_num);
}

Datum dw_get_total_writes()
{
    return UInt64GetDatum(g_instance.dw_cxt.stat_info.total_writes);
}

Datum dw_get_low_threshold_writes()
{
    return UInt64GetDatum(g_instance.dw_cxt.stat_info.low_threshold_writes);
}

Datum dw_get_high_threshold_writes()
{
    return UInt64GetDatum(g_instance.dw_cxt.stat_info.high_threshold_writes);
}

Datum dw_get_total_pages()
{
    return UInt64GetDatum(g_instance.dw_cxt.stat_info.total_pages);
}

Datum dw_get_low_threshold_pages()
{
    return UInt64GetDatum(g_instance.dw_cxt.stat_info.low_threshold_pages);
}

Datum dw_get_high_threshold_pages()
{
    return UInt64GetDatum(g_instance.dw_cxt.stat_info.high_threshold_pages);
}

/* double write statistic view */
const dw_view_col_t g_dw_view_col_arr[DW_VIEW_COL_NUM] = {
    {"node_name", TEXTOID, dw_get_node_name},
    {"curr_dwn", INT8OID, dw_get_dw_number},
    {"curr_start_page", INT8OID, dw_get_start_page},
    {"file_trunc_num", INT8OID, dw_get_file_trunc_num},
    {"file_reset_num", INT8OID, dw_get_file_reset_num},
    {"total_writes", INT8OID, dw_get_total_writes},
    {"low_threshold_writes", INT8OID, dw_get_low_threshold_writes},
    {"high_threshold_writes", INT8OID, dw_get_high_threshold_writes},
    {"total_pages", INT8OID, dw_get_total_pages},
    {"low_threshold_pages", INT8OID, dw_get_low_threshold_pages},
    {"high_threshold_pages", INT8OID, dw_get_high_threshold_pages}
};

void dw_pread_file(int fd, void* buf, int size, int64 offset)
{
    int32 curr_size, total_size;
    total_size = 0;
    do {
        curr_size = pread64(fd, ((char*)buf + total_size), (size - total_size), offset);
        if (curr_size == -1) {
            ereport(PANIC, (errcode_for_file_access(), errmodule(MOD_DW), errmsg("Read file error")));
        }

        total_size += curr_size;
        offset += curr_size;
    } while (curr_size > 0);

    if (total_size != size) {
        ereport(PANIC,
            (errcode_for_file_access(),
                errmodule(MOD_DW),
                errmsg("Read file size mismatch: expected %d, read %d", size, total_size)));
    }
}

void dw_pwrite_file(int fd, const void* buf, int size, int64 offset)
{
    int write_size = 0;
    uint32 try_times = 0;

    if ((offset + size) > DW_FILE_SIZE) {
        ereport(PANIC,
            (errmodule(MOD_DW),
                errmsg(
                    "DW write file failed, dw_file_size %ld, offset %ld, write_size %d", DW_FILE_SIZE, offset, size)));
    }

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
        ereport(PANIC,
            (errcode_for_file_access(),
                errmodule(MOD_DW),
                errmsg("Write file size mismatch: expected %d, written %d", size, write_size)));
    }
}

int64 dw_seek_file(int fd, int64 offset, int32 origin)
{
    return (int64)lseek64(fd, (off64_t)offset, origin);
}

static void dw_extend_file(int fd, const void* buf, int buf_size, int64 size)
{
    int64 offset, remain_size;

    offset = dw_seek_file(fd, 0, SEEK_END);
    if (offset == -1) {
        ereport(PANIC, (errcode_for_file_access(), errmodule(MOD_DW), errmsg("Seek file error")));
    }

    if ((offset + size) > DW_FILE_SIZE) {
        ereport(PANIC,
            (errmodule(MOD_DW),
                errmsg("DW extend file failed, expected_file_size %ld, offset %ld, extend_size %ld",
                    DW_FILE_SIZE,
                    offset,
                    size)));
    }

    remain_size = size;
    while (remain_size > 0) {
        size = (remain_size > buf_size) ? buf_size : remain_size;
        dw_pwrite_file(fd, buf, size, offset);
        offset += size;
        remain_size -= size;
    }
}

static void dw_set_pg_checksum(char* page, BlockNumber blockNum)
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
    if (PageIsNew(page_header)) {
        return true;
    }
    if (PageIsChecksumByFNV1A(page_header)) {
        uint16 checksum = pg_checksum_page((char*)page_header, blockNum);
        return checksum == page_header->pd_checksum;
    } else {
        ereport(FATAL, (errmodule(MOD_DW), errmsg("DW verify checksum: page checksum flag is wrong")));
        return false;
    }
}

inline void dw_prepare_page(dw_batch_t* batch, uint16 page_num, uint16 page_id, uint16 dwn)
{
    if (g_instance.ckpt_cxt_ctl->buffers_contain_hashbucket == true) {
        page_num = page_num | IS_HASH_BKT_MASK;
    }
    batch->page_num = page_num;
    batch->head.page_id = page_id;
    batch->head.dwn = dwn;
    DW_PAGE_TAIL(batch)->dwn = dwn;
    dw_calc_batch_checksum(batch);
}

static void dw_prepare_file_head(char* file_head, uint16 start, uint16 dwn)
{
    uint32 i;
    uint32 id;
    dw_file_head_t* curr_head = NULL;
    for (i = 0; i < DW_FILE_HEAD_ID_NUM; i++) {
        id = g_dw_file_head_ids[i];
        curr_head = (dw_file_head_t*)(file_head + sizeof(dw_file_head_t) * id);
        curr_head->head.page_id = 0;
        curr_head->head.dwn = dwn;
        curr_head->start = start;
        curr_head->tail.dwn = dwn;
        dw_calc_file_head_checksum(curr_head);
    }
}

static void dw_generate_file(int fd, char* file_head, int buf_size)
{
    dw_batch_t* batch_head = NULL;
    /* file head and first batch head will be writen */
    int64 extend_size = (DW_FILE_PAGE * BLCKSZ) - BLCKSZ - BLCKSZ;
    dw_prepare_file_head(file_head, DW_BATCH_FILE_START, 0);
    batch_head = (dw_batch_t*)(file_head + BLCKSZ);
    batch_head->head.page_id = DW_BATCH_FILE_START;
    dw_calc_batch_checksum(batch_head);
    pgstat_report_waitevent(WAIT_EVENT_DW_WRITE);
    dw_pwrite_file(fd, file_head, (BLCKSZ + BLCKSZ), 0);
    dw_extend_file(fd, file_head, buf_size, extend_size);
    pgstat_report_waitevent(WAIT_EVENT_END);
    ereport(LOG, (errmodule(MOD_DW), errmsg("Double write file created successfully")));
}

static void dw_recover_file_head(dw_context_t* ctx)
{
    uint32 i;
    uint16 id = DW_FILE_HEAD_ID_MAX;
    errno_t rc;
    dw_file_head_t* curr_head = NULL;
    dw_file_head_t* working_head = NULL;
    char* file_head = (char*)ctx->file_head;

    pgstat_report_waitevent(WAIT_EVENT_DW_READ);
    dw_pread_file(ctx->fd, ctx->file_head, BLCKSZ, 0);
    pgstat_report_waitevent(WAIT_EVENT_END);

    int64 offset = dw_seek_file(ctx->fd, 0, SEEK_END);
    if (offset != DW_FILE_SIZE) {
        ereport(PANIC,
            (errmodule(MOD_DW),
                errmsg("DW check file size failed, expected_size %ld, actual_size %ld", DW_FILE_SIZE, offset)));
    }

    for (i = 0; i < DW_FILE_HEAD_ID_NUM; i++) {
        id = g_dw_file_head_ids[i];
        curr_head = (dw_file_head_t*)(file_head + sizeof(dw_file_head_t) * id);
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

    ereport(LOG,
        (errmodule(MOD_DW),
            errmsg("Found a valid file header: id %hu, file_head[dwn %hu, start %hu]",
                id,
                working_head->head.dwn,
                working_head->start)));

    for (i = 0; i < DW_FILE_HEAD_ID_NUM; i++) {
        id = g_dw_file_head_ids[i];
        curr_head = (dw_file_head_t*)(file_head + sizeof(dw_file_head_t) * id);
        if (curr_head != working_head) {
            rc = memcpy_s(curr_head, sizeof(dw_file_head_t), working_head, sizeof(dw_file_head_t));
            securec_check(rc, "\0", "\0");
        }
    }

    pgstat_report_waitevent(WAIT_EVENT_DW_WRITE);
    dw_pwrite_file(ctx->fd, file_head, BLCKSZ, 0);
    pgstat_report_waitevent(WAIT_EVENT_END);
}

static inline void dw_log_page_header(PageHeader page)
{
    ereport(DW_LOG_LEVEL,
        (errmodule(MOD_DW),
            errmsg("Page header info: pd_lsn %lu, pd_checksum %hu, "
                   "pd_lower %hu(%s), pd_upper %hu(%s), max_offset %hu",
                PageGetLSN(page),
                page->pd_checksum,
                page->pd_lower,
                PageIsEmpty(page) ? "empty" : "non-empty",
                page->pd_upper,
                PageIsNew(page) ? "new" : "old",
                PageGetMaxOffsetNumber((char*)page))));
}

template <typename T>
static inline void dw_log_data_page(int elevel, const char* state, T* buf_tag)
{
    ereport(elevel,
        (errmodule(MOD_DW),
            errmsg("%s: buf_tag[rel %u/%u/%u blk %u fork %d]",
                state,
                buf_tag->rnode.spcNode,
                buf_tag->rnode.dbNode,
                buf_tag->rnode.relNode,
                buf_tag->blockNum,
                buf_tag->forkNum)));
}

template <typename T1, typename T2>
static void dw_recover_pages(T1* batch, T2* buf_tag, PageHeader data_page, bool is_hashbucket)
{
    uint16 i;
    PageHeader dw_page;
    SMgrRelation relation;
    BlockNumber blk_num;
    RelFileNode relnode;
    for (i = 0; i < GET_REL_PGAENUM(batch->page_num); i++) {
        buf_tag = &batch->buf_tag[i];
        if (is_hashbucket) {
            relnode.dbNode = buf_tag->rnode.dbNode;
            relnode.spcNode = buf_tag->rnode.spcNode;
            relnode.relNode = buf_tag->rnode.relNode;
            relnode.bucketNode = ((BufferTag*)buf_tag)->rnode.bucketNode;
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
        smgrread(relation, buf_tag->forkNum, buf_tag->blockNum, (char*)data_page);

        dw_page = (PageHeader)((char*)batch + (i + 1) * BLCKSZ);
        if (!dw_verify_pg_checksum(dw_page, buf_tag->blockNum)) {
            dw_log_data_page(WARNING, "DW page broken", buf_tag);
            dw_log_page_header(dw_page);
            continue;
        }

        dw_log_data_page(DW_LOG_LEVEL, "DW page fine", buf_tag);
        dw_log_page_header(dw_page);
        if (!dw_verify_pg_checksum(data_page, buf_tag->blockNum) ||
            XLByteLT(PageGetLSN(data_page), PageGetLSN(dw_page))) {
            smgrwrite(relation, buf_tag->forkNum, buf_tag->blockNum, (const char*)dw_page, false);
            dw_log_data_page(LOG, "Date page recovered", buf_tag);
            dw_log_page_header(data_page);
        }
    }
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
static bool dw_reset_if_need(dw_context_t* ctx, uint16 pages_to_write, bool trunc_file)
{
    bool file_full = false;
    dw_file_head_t* file_head = ctx->file_head;
    volatile uint16 org_start = file_head->start;
    volatile uint16 org_dwn = file_head->head.dwn;
    uint16 last_flush_page;

    file_full = (file_head->start + ctx->flush_page + pages_to_write >= DW_FILE_PAGE);
    Assert(!(file_full && trunc_file));
    if (!file_full && !trunc_file) {
        return true;
    }

    if (trunc_file) {
        /* record last flush position for truncate because flush lock is not held during smgrsync */
        last_flush_page = ctx->last_flush_page;
        LWLockRelease(ctx->flush_lock);
    } else {
        /* no need to reserve last flush position for full recycle */
        last_flush_page = ctx->flush_page;
    }

    smgrsync_for_dw();

    if (trunc_file) {
        if (!LWLockConditionalAcquire(ctx->flush_lock, LW_EXCLUSIVE)) {
            ereport(LOG,
                (errmodule(MOD_DW), errmsg("Can not get dw flush lock and skip dw truncate after sync for this time")));
            return false;
        } else if (org_start != file_head->start || org_dwn != file_head->head.dwn) {
            /*
             * Even if there are concurrent dw truncate/reset during the above smgrsync,
             * the possibility of same start and dwn value should be small enough.
             */
            ereport(LOG,
                (errmodule(MOD_DW),
                    errmsg("Skip dw truncate after sync due to concurrent dw truncate/reset, "
                           "original[dwn %hu, start %hu], current[dwn %hu, start %hu]",
                        org_dwn,
                        org_start,
                        file_head->head.dwn,
                        file_head->start)));
            return true;
        }
    }

    ereport(DW_LOG_LEVEL,
        (errmodule(MOD_DW),
            errmsg("Reset DW file: file_head[dwn %hu, start %hu], total_pages %hu, "
                   "file_full %d, trunc_file %d, pages_to_write %hu",
                file_head->head.dwn,
                file_head->start,
                ctx->flush_page,
                file_full,
                trunc_file,
                pages_to_write)));

    if (file_full) {
        Assert(AmStartupProcess() || AmPageWriterProcess());
        file_head->start = DW_BATCH_FILE_START;
        ctx->last_flush_page = ctx->flush_page;
    } else {
        Assert(AmStartupProcess() || AmCheckpointerProcess() || AmBootstrapProcess() || !IsUnderPostmaster);
        /*
         * we can only discard all the batches till last dw flush.
         * For pages recorded in current dw flush, they may be concurrently
         * flushed by pagewriters and we might have not absorbed their fsync request.
         */
        file_head->start += last_flush_page;
    }

    ctx->flush_page -= last_flush_page;
    ctx->last_flush_page -= last_flush_page;

    /*
     * if truncate file and flush_page is not 0, the dwn can not plus,
     * otherwise verify will failed when recovery the data form dw file.
     */
    if (ctx->flush_page > 0) {
        Assert(trunc_file);
        dw_prepare_file_head((char*)file_head, file_head->start, file_head->head.dwn);
    } else {
        dw_prepare_file_head((char*)file_head, file_head->start, file_head->head.dwn + 1);
    }

    Assert(file_head->head.dwn == file_head->tail.dwn);
    pgstat_report_waitevent(WAIT_EVENT_DW_WRITE);
    dw_pwrite_file(ctx->fd, file_head, BLCKSZ, 0);
    pgstat_report_waitevent(WAIT_EVENT_END);

    pg_atomic_add_fetch_u64(&ctx->stat_info.file_trunc_num, 1);
    if (file_full) {
        pg_atomic_add_fetch_u64(&ctx->stat_info.file_reset_num, 1);
    }
    return true;
}

static void dw_read_pages(dw_read_asst_t* read_asst, uint16 reading_pages)
{
    if (reading_pages > 0) {
        Assert(read_asst->buf_end + reading_pages <= read_asst->buf_capacity);
        Assert(read_asst->file_start + reading_pages <= read_asst->file_capacity);
        pgstat_report_waitevent(WAIT_EVENT_DW_READ);
        dw_pread_file(read_asst->fd,
            (read_asst->buf + read_asst->buf_end * BLCKSZ),
            (reading_pages * BLCKSZ),
            (read_asst->file_start * BLCKSZ));
        pgstat_report_waitevent(WAIT_EVENT_END);
        read_asst->buf_end += reading_pages;
        read_asst->file_start += reading_pages;
    }

    Assert(read_asst->buf_end >= read_asst->buf_start);
}

static inline void dw_discard_pages(dw_read_asst_t* read_asst, uint16 page_num)
{
    read_asst->buf_start += page_num;
    Assert(read_asst->buf_end >= read_asst->buf_start);
}

static uint16 dw_calc_reading_pages(dw_read_asst_t* read_asst)
{
    dw_batch_t* curr_head;
    uint16 remain_pages, batch_pages, reading_pages;
    errno_t rc;

    remain_pages = read_asst->buf_end - read_asst->buf_start;
    curr_head = (dw_batch_t*)(read_asst->buf + read_asst->buf_start * BLCKSZ);
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
            curr_head = (dw_batch_t*)read_asst->buf;
        }
        reading_pages = batch_pages - remain_pages;
    }

    Assert(
        (char*)curr_head + (remain_pages + reading_pages) * BLCKSZ < read_asst->buf + read_asst->buf_capacity * BLCKSZ);
    Assert(read_asst->file_start + reading_pages <= DW_FILE_PAGE);
    return reading_pages;
}

static void dw_recover_batch_head(dw_context_t* ctx, dw_batch_t* curr_head)
{
    errno_t rc;
    rc = memset_s(curr_head, BLCKSZ, 0, BLCKSZ);
    securec_check(rc, "\0", "\0");
    dw_prepare_page(curr_head, 0, ctx->file_head->start, ctx->file_head->head.dwn);
    pgstat_report_waitevent(WAIT_EVENT_DW_WRITE);
    dw_pwrite_file(ctx->fd, curr_head, BLCKSZ, (curr_head->head.page_id * BLCKSZ));
    pgstat_report_waitevent(WAIT_EVENT_END);
}

static inline void dw_log_recover_state(dw_context_t* ctx, int elevel, const char* state, dw_batch_t* batch)
{
    ereport(elevel,
        (errmodule(MOD_DW),
            errmsg("DW recovery state: \"%s\", file start page[dwn %hu, start %hu], now access page %hu, "
                   "current [page_id %hu, dwn %hu, checksum verify res is %d, page_num orig %hu, page_num fixed %hu]",
                state,
                ctx->file_head->head.dwn,
                ctx->file_head->start,
                ctx->flush_page,
                batch->head.page_id,
                batch->head.dwn,
                dw_verify_batch_checksum(batch),
                batch->page_num,
                GET_REL_PGAENUM(batch->page_num))));
}

static bool dw_batch_head_broken(dw_context_t* ctx, dw_batch_t* curr_head)
{
    bool broken = false;
    dw_batch_t* curr_tail = dw_batch_tail_page(curr_head);
    if (dw_verify_page(curr_head)) {
        if (GET_REL_PGAENUM(curr_head->page_num) == 0) {
            dw_log_recover_state(ctx, LOG, "Empty", curr_head);
        } else if (curr_head->head.page_id == DW_BATCH_FILE_START) {
            dw_log_recover_state(ctx, LOG, "File reset", curr_head);
        } else {
            dw_log_recover_state(ctx, WARNING, "Head info", curr_head);
            dw_log_recover_state(ctx, WARNING, "Tail broken", curr_tail);
            broken = true;
        }
    } else {
        dw_log_recover_state(ctx, WARNING, "Head broken", curr_head);
        dw_log_recover_state(ctx, WARNING, "Tail unknown", curr_tail);
        broken = true;
    }
    return broken;
}

static void dw_recover_partial_write(dw_context_t* ctx)
{
    dw_read_asst_t read_asst;
    dw_batch_t* curr_head = NULL;
    uint16 reading_pages;
    uint16 remain_pages;
    bool dw_file_broken = false;
    char* data_page = NULL;
    MemoryContext old_mem_ctx;

    read_asst.fd = ctx->fd;
    read_asst.file_start = ctx->file_head->start;
    read_asst.file_capacity = DW_FILE_PAGE;
    read_asst.buf_start = 0;
    read_asst.buf_end = 0;
    read_asst.buf_capacity = GET_DW_BUF_MAX;
    read_asst.buf = ctx->buf;
    reading_pages = Min(GET_DW_BATCH_MAX, (DW_FILE_PAGE - ctx->file_head->start));

    old_mem_ctx = MemoryContextSwitchTo(ctx->mem_ctx);
    data_page = (char*)palloc0(BLCKSZ);

    for (;;) {
        dw_read_pages(&read_asst, reading_pages);
        curr_head = (dw_batch_t*)(read_asst.buf + (read_asst.buf_start * BLCKSZ));
        bool is_hashbucket = ((curr_head->page_num & IS_HASH_BKT_MASK) != 0);

        if (!dw_verify_batch(curr_head, ctx->file_head->head.dwn)) {
            dw_file_broken = dw_batch_head_broken(ctx, curr_head);
            break;
        }
        remain_pages = read_asst.buf_end - read_asst.buf_start;
        Assert(curr_head->head.page_id + remain_pages == read_asst.file_start);
        Assert(remain_pages >= GET_REL_PGAENUM(curr_head->page_num) + DW_EXTRA_FOR_ONE_BATCH);

        dw_log_recover_state(ctx, DW_LOG_LEVEL, "Batch fine", curr_head);
        if (is_hashbucket) {
            BufferTag* tmp = NULL;
            dw_recover_pages<dw_batch_t, BufferTag>(curr_head, tmp, (PageHeader)data_page, is_hashbucket);
        } else {
            BufferTagNoHBkt* tmp = NULL;
            dw_recover_pages<dw_batch_nohbkt_t, BufferTagNoHBkt>(
                (dw_batch_nohbkt_t*)curr_head, tmp, (PageHeader)data_page, is_hashbucket);
        }

        /* discard the first batch. including head page and data pages */
        ctx->flush_page += (1 + GET_REL_PGAENUM(curr_head->page_num));
        dw_discard_pages(&read_asst, (1 + GET_REL_PGAENUM(curr_head->page_num)));
        curr_head = dw_batch_tail_page(curr_head);
        if (GET_REL_PGAENUM(curr_head->page_num) == 0) {
            dw_log_recover_state(ctx, LOG, "Batch end", curr_head);
            break;
        }

        reading_pages = dw_calc_reading_pages(&read_asst);
    }

    /* Truncate to all flushed page is safe since there is no concurrent flush-buffer at this stage */
    ctx->last_flush_page = ctx->flush_page;
    /* if free space not enough for one batch, reuse file. Otherwise, just do a truncate */
    if ((ctx->file_head->start + ctx->flush_page + GET_DW_BUF_MAX) >= DW_FILE_PAGE) {
        (void)dw_reset_if_need(ctx, GET_DW_BUF_MAX, false);
    } else if (ctx->flush_page > 0) {
        if (!dw_reset_if_need(ctx, 0, true)) {
            ereport(PANIC,
                (errcode_for_file_access(), errmodule(MOD_DW), errmsg("Could not truncate dw file during startup!")));
        }
    }
    if (dw_file_broken) {
        dw_recover_batch_head(ctx, curr_head);
    }
    dw_log_recover_state(ctx, LOG, "Finish", curr_head);
    pfree(data_page);
    MemoryContextSwitchTo(old_mem_ctx);
}

void dw_bootstrap()
{
    char* unaligned_buf = NULL;
    char* file_head = NULL;
    int fd = -1;                                        /* resource fd should be initialized any way */
    int extend_buf_size = DW_FILE_EXTEND_SIZE + BLCKSZ; /* one more BLCKSZ for alignment */

    if (file_exists(DW_FILE_NAME)) {
        ereport(PANIC, (errcode_for_file_access(), errmodule(MOD_DW), "DW file already exists"));
    }

    ereport(LOG, (errmodule(MOD_DW), errmsg("Double write bootstrap")));

    /* Open file with O_SYNC, to make sure the data and file system control info on file after block writing. */
    fd = open(DW_FILE_NAME, (DW_FILE_FLAG | O_CREAT), DW_FILE_PERM);
    if (fd == -1) {
        ereport(PANIC,
            (errcode_for_file_access(), errmodule(MOD_DW), errmsg("Could not create file \"%s\"", DW_FILE_NAME)));
    }

    unaligned_buf = (char*)palloc0(extend_buf_size);

    /* alignment for O_DIRECT */
    file_head = (char*)TYPEALIGN(BLCKSZ, unaligned_buf);

    dw_generate_file(fd, file_head, DW_FILE_EXTEND_SIZE);

    (void)close(fd);
    pfree(unaligned_buf);
}

static void dw_init_memory(dw_context_t* ctx)
{
    uint32 buf_size;
    char* buf = NULL;
    MemoryContext old_mem_ctx;

    buf_size = DW_MEM_CTX_MAX_BLOCK_SIZE_FOR_NOHBK;

    ctx->mem_ctx = AllocSetContextCreate(
        g_instance.instance_context, "double write", buf_size, buf_size, buf_size, SHARED_CONTEXT);
    old_mem_ctx = MemoryContextSwitchTo(ctx->mem_ctx);
    /* one BLCKSZ left for partial write recovering's data file reading buffer */
    ctx->unaligned_buf = (char*)palloc0(buf_size - BLCKSZ);

    /* alignment for O_DIRECT */
    buf = (char*)TYPEALIGN(BLCKSZ, ctx->unaligned_buf);

    ctx->file_head = (dw_file_head_t*)buf;
    buf += BLCKSZ;

    ctx->buf = buf;
    ctx->write_pos = 0;
    ctx->flush_page = 0;
    ctx->last_flush_page = 0;

    (void)MemoryContextSwitchTo(old_mem_ctx);
}

static void dw_free_resource(dw_context_t* ctx)
{
    int rc = close(ctx->fd);
    if (rc == -1) {
        ereport(ERROR, (errcode_for_file_access(), errmodule(MOD_DW), errmsg("DW file close failed")));
    }

    pfree(ctx->unaligned_buf);
    ctx->unaligned_buf = NULL;
    ctx->file_head = NULL;
    ctx->buf = NULL;
    MemoryContextDelete(ctx->mem_ctx);
}

void dw_shmem_init()
{
    /* LWLock Should be reset when postmaster inits shmem. */
    if (!IsUnderPostmaster) {
        g_instance.dw_cxt.flush_lock = NULL;
    }
}

void dw_init()
{
    dw_context_t* ctx = &g_instance.dw_cxt;

#ifndef ENABLE_THREAD_CHECK
    if (TAS(&ctx->initialized)) {
#else
    if (__sync_lock_test_and_set(&ctx->initialized, 1)) {
#endif
        ereport(WARNING, (errmodule(MOD_DW), errmsg("Double write already initialized")));
        return;
    }

    ereport(LOG, (errmodule(MOD_DW), errmsg("Double write init")));
    ctx->closed = 0;

    if (file_exists(DW_BUILD_FILE_NAME)) {
        ereport(LOG, (errmodule(MOD_DW), errmsg("Double write initializing after build")));

        if (file_exists(DW_FILE_NAME)) {
            /*
             * Probably the gaussdb was killed during the first time startup after build, resulting in a half-written
             * DW file. So, log a warning message and remove the residual DW file.
             */
            ereport(WARNING, (errcode_for_file_access(), errmodule(MOD_DW), "Residual DW file exists, deleting it"));

            if (unlink(DW_FILE_NAME) != 0) {
                ereport(PANIC,
                    (errcode_for_file_access(), errmodule(MOD_DW), errmsg("Could not remove the residual DW file")));
            }
        }

        /* Create the DW file. */
        dw_bootstrap();

        /* Remove the DW build file. */
        if (unlink(DW_BUILD_FILE_NAME) != 0) {
            ereport(
                PANIC, (errcode_for_file_access(), errmodule(MOD_DW), errmsg("Could not remove the DW build file")));
        }
    }

    if (!file_exists(DW_FILE_NAME)) {
        ereport(PANIC, (errcode_for_file_access(), errmodule(MOD_DW), errmsg("DW file does not exist")));
    }

    /* double write file disk space pre-allocated, O_DSYNC for less IO */
    ctx->fd = open(DW_FILE_NAME, DW_FILE_FLAG, DW_FILE_PERM);
    if (ctx->fd == -1) {
        ereport(
            PANIC, (errcode_for_file_access(), errmodule(MOD_DW), errmsg("Could not open file \"%s\"", DW_FILE_NAME)));
    }

    /* LWLock has no free method, so only assign once when first init */
    /* fail_over and switch_over will dw_exit and dw_init multiple times */
    if (ctx->flush_lock == NULL) {
        ctx->flush_lock = LWLockAssign(LWTRANCHE_DOUBLE_WRITE);
    }

    LWLockAcquire(ctx->flush_lock, LW_EXCLUSIVE);

    ctx->flush_page = 0;

    dw_init_memory(ctx);

    dw_recover_file_head(ctx);

    dw_recover_partial_write(ctx);
    LWLockRelease(ctx->flush_lock);

    /*
     * After recovering partially written pages (if any), we will un-initialize, if the double write is disabled.
     */
    if (!dw_enabled()) {
        dw_free_resource(ctx);
        ctx->initialized = 0;

        ereport(LOG, (errmodule(MOD_DW), errmsg("Double write exit after recovering partial write")));
    }
}

static void dw_encrypt_page(char* dest_addr)
{
    size_t plain_len, cipher_len;
    if (isEncryptedCluster() && !PageIsNew(dest_addr)) {
        plain_len = BLCKSZ - GetPageHeaderSize(dest_addr);
        encryptBlockOrCUData(PageGetContents(dest_addr), plain_len, PageGetContents(dest_addr), &cipher_len);
        Assert(plain_len == cipher_len);
        PageSetEncrypt((Page)dest_addr);
    }
}

static XLogRecPtr dw_copy_page(dw_context_t* dw_ctx, int buf_desc_id, bool* is_skipped)
{
    /* we write different struct depend on hashbucket */
    dw_batch_t* batch = NULL;
    dw_batch_nohbkt_t* batch_nohbkt = NULL;
    char* dest_addr = NULL;
    BufferDesc* buf_desc = NULL;
    XLogRecPtr page_lsn = InvalidXLogRecPtr;
    Block block;
    uint16 page_num;
    uint32 buf_state;
    errno_t rc;
    *is_skipped = false;

    buf_desc = GetBufferDescriptor(buf_desc_id);
    buf_state = LockBufHdr(buf_desc);
    if (!dw_buf_ckpt_needed(buf_state)) {
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
        *is_skipped = true;
        return page_lsn;
    }
    dw_ctx->write_pos++;
    if (dw_ctx->write_pos <= GET_DW_BATCH_DATA_PAGE_MAX) {
        batch = (dw_batch_t*)dw_ctx->buf;
        page_num = dw_ctx->write_pos;
    } else {
        batch = (dw_batch_t*)(dw_ctx->buf + (GET_DW_BATCH_DATA_PAGE_MAX + 1) * BLCKSZ);
        page_num = dw_ctx->write_pos - GET_DW_BATCH_DATA_PAGE_MAX;
    }
    if (g_instance.ckpt_cxt_ctl->buffers_contain_hashbucket) {
        batch->buf_tag[page_num - 1] = buf_desc->tag;
    } else {
        /* change struct to nohash bucket */
        batch_nohbkt = (dw_batch_nohbkt_t*)batch;
        batch_nohbkt->buf_tag[page_num - 1].blockNum = buf_desc->tag.blockNum;
        batch_nohbkt->buf_tag[page_num - 1].forkNum = buf_desc->tag.forkNum;
        batch_nohbkt->buf_tag[page_num - 1].rnode.dbNode = buf_desc->tag.rnode.dbNode;
        batch_nohbkt->buf_tag[page_num - 1].rnode.spcNode = buf_desc->tag.rnode.spcNode;
        batch_nohbkt->buf_tag[page_num - 1].rnode.relNode = buf_desc->tag.rnode.relNode;
    }
    dest_addr = (char*)batch + page_num * BLCKSZ;
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

    Assert(page_num <= GET_DW_DIRTY_PAGE_MAX);
    if (page_num <= GET_DW_BATCH_DATA_PAGE_MAX) {
        return page_num + DW_EXTRA_FOR_ONE_BATCH;
    } else {
        return page_num + DW_EXTRA_FOR_TWO_BATCH;
    }
}

static void dw_assemble_batch(dw_context_t* dw_ctx, uint16 page_id, uint16 dwn)
{
    dw_batch_t* batch = NULL;
    uint16 first_batch_pages;
    uint16 second_batch_pages;

    if (dw_ctx->write_pos > GET_DW_BATCH_DATA_PAGE_MAX) {
        first_batch_pages = GET_DW_BATCH_DATA_PAGE_MAX;
        second_batch_pages = dw_ctx->write_pos - GET_DW_BATCH_DATA_PAGE_MAX;
    } else {
        first_batch_pages = dw_ctx->write_pos;
        second_batch_pages = 0;
    }

    batch = (dw_batch_t*)dw_ctx->buf;
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

static inline void dw_stat_flush(dw_stat_info* stat_info, uint32 page_to_write)
{
    (void)pg_atomic_add_fetch_u64(&stat_info->total_writes, 1);
    (void)pg_atomic_add_fetch_u64(&stat_info->total_pages, page_to_write);
    if (page_to_write < DW_WRITE_STAT_LOWER_LIMIT) {
        (void)pg_atomic_add_fetch_u64(&stat_info->low_threshold_writes, 1);
        (void)pg_atomic_add_fetch_u64(&stat_info->low_threshold_pages, page_to_write);
    } else if (page_to_write > GET_DW_BATCH_MAX) {
        (void)pg_atomic_add_fetch_u64(&stat_info->high_threshold_writes, 1);
        (void)pg_atomic_add_fetch_u64(&stat_info->high_threshold_pages, page_to_write);
    }
}

static inline void dw_log_perform(dw_context_t* ctx, const char* phase, uint32 write_id, uint16 size)
{
    ereport(DW_LOG_LEVEL,
        (errmodule(MOD_DW),
            errmsg("DW perform %s: write_id %u, file_head[dwn %hu, start %hu], total_pages %hu, size %hu",
                phase,
                write_id,
                ctx->file_head->head.dwn,
                ctx->file_head->start,
                ctx->flush_page,
                size)));
}

/**
 * flush the copied page in the buffer into dw file, allocate the token for outside data file flushing
 * @param dw_ctx double write context
 * @param latest_lsn the latest lsn in the copied pages
 */
static void dw_flush(dw_context_t* dw_ctx, XLogRecPtr latest_lsn)
{
    uint16 offset_page;
    uint16 pages_to_write = 0;
    uint16 write_pos = 0;
    dw_file_head_t* file_head = NULL;

    (void)LWLockAcquire(dw_ctx->flush_lock, LW_EXCLUSIVE);

    Assert(dw_ctx->write_pos > 0);

    file_head = dw_ctx->file_head;
    pages_to_write = dw_batch_add_extra(dw_ctx->write_pos);
    (void)dw_reset_if_need(dw_ctx, pages_to_write, false);

    /* calculate it after checking file space, in case of updated by sync */
    offset_page = file_head->start + dw_ctx->flush_page;

    dw_assemble_batch(dw_ctx, offset_page, file_head->head.dwn);
    if (!XLogRecPtrIsInvalid(latest_lsn)) {
        XLogFlush(latest_lsn);
    }
    pgstat_report_waitevent(WAIT_EVENT_DW_WRITE);
    dw_pwrite_file(dw_ctx->fd, dw_ctx->buf, (pages_to_write * BLCKSZ), (offset_page * BLCKSZ));
    pgstat_report_waitevent(WAIT_EVENT_END);

    dw_stat_flush(&dw_ctx->stat_info, pages_to_write);

    dw_ctx->last_flush_page = dw_ctx->flush_page;
    /* the tail of this flushed batch is the head of the next batch */
    dw_ctx->flush_page += (pages_to_write - 1);
    write_pos = dw_ctx->write_pos;
    dw_ctx->write_pos = 0;

    LWLockRelease(dw_ctx->flush_lock);

    ereport(DW_LOG_LEVEL,
        (errmodule(MOD_DW),
            errmsg("DW flush: file_head[dwn %hu, start %hu], total_pages %hu, data_pages %hu, flushed_pages %hu",
                dw_ctx->file_head->head.dwn,
                dw_ctx->file_head->start,
                dw_ctx->flush_page,
                write_pos,
                pages_to_write)));
}

void dw_perform(uint32 size)
{
    uint16 batch_size;
    dw_context_t* dw_ctx = &g_instance.dw_cxt;
    XLogRecPtr latest_lsn = InvalidXLogRecPtr;
    XLogRecPtr page_lsn;
    uint32 write_id;

    if (!dw_enabled()) {
        /* Double write is not enabled, nothing to do. */
        return;
    }

    if (SECUREC_UNLIKELY(!dw_ctx->initialized)) {
        ereport(PANIC, (errmodule(MOD_DW), errmsg("Double write not initialized")));
    }

    if (SECUREC_UNLIKELY(dw_ctx->closed)) {
        ereport(ERROR, (errmodule(MOD_DW), errmsg("Double write already closed")));
    }

    Assert(size > 0 && size <= GET_DW_DIRTY_PAGE_MAX);
    batch_size = (uint16)size;

    write_id = dw_ctx->stat_info.total_writes;

    dw_log_perform(dw_ctx, "start", write_id, batch_size);

    if (dw_ctx->write_pos != 0) {
        ereport(WARNING, (errmodule(MOD_DW), errmsg("Double write exception ignored")));
    }
    dw_ctx->write_pos = 0;

    for (uint16 i = 0; i < batch_size; i++) {
        bool is_skipped = false;
        page_lsn = dw_copy_page(dw_ctx, g_instance.ckpt_cxt_ctl->CkptBufferIds[i].buf_id, &is_skipped);
        if (is_skipped) {
            /*
             * We couldn't acquire conditional lock on the buffer content_lock.
             * So we mark it in buf_id_arr.
             */
            g_instance.ckpt_cxt_ctl->CkptBufferIds[i].buf_id = DW_INVALID_BUFFER_ID;
            continue;
        }

        if (XLByteLT(latest_lsn, page_lsn)) {
            latest_lsn = page_lsn;
        }
    }

    if (dw_ctx->write_pos > 0) {
        dw_flush(dw_ctx, latest_lsn);
    }

    dw_log_perform(dw_ctx, "end", write_id, batch_size);
}

void dw_truncate()
{
    dw_context_t* ctx = &g_instance.dw_cxt;

    if (!dw_enabled()) {
        /* Double write is not enabled, nothing to do. */
        return;
    }

    gstrace_entry(GS_TRC_ID_dw_truncate);
    ereport(DW_LOG_LEVEL,
        (errmodule(MOD_DW),
            errmsg("DW truncate start: file_head[dwn %hu, start %hu], total_pages %hu",
                ctx->file_head->head.dwn,
                ctx->file_head->start,
                ctx->flush_page)));

    /*
     * If we can grab dw flush lock, truncate dw file for faster recovery.
     *
     * Note: This is only for recovery optimization. we can not block on
     * dw flush lock, because, if we are checkpointer, pagewriter may be
     * waiting for us to finish smgrsync before it can do a full recycle of dw file.
     */
    if (!LWLockConditionalAcquire(ctx->flush_lock, LW_EXCLUSIVE)) {
        ereport(LOG, (errmodule(MOD_DW), errmsg("Can not get dw flush lock and skip dw truncate for this time")));
        return;
    }
    if (dw_reset_if_need(ctx, 0, true)) {
        LWLockRelease(ctx->flush_lock);
    }

    gstrace_exit(GS_TRC_ID_dw_truncate);
    ereport(LOG,
        (errmodule(MOD_DW),
            errmsg("DW truncate end: file_head[dwn %hu, start %hu], total_pages %hu",
                ctx->file_head->head.dwn,
                ctx->file_head->start,
                ctx->flush_page)));
}

void dw_exit()
{
    dw_context_t* ctx = &g_instance.dw_cxt;

    if (!dw_enabled()) {
        /* Double write is not enabled, nothing to do. */
        return;
    }

    if (SECUREC_UNLIKELY(!ctx->initialized)) {
        ereport(WARNING, (errmodule(MOD_DW), errmsg("Double write not initialized")));
        return;
    }

    Assert(pg_atomic_read_u32(&g_instance.ckpt_cxt_ctl->current_page_writer_count) == 0);

    if (TAS(&ctx->closed)) {
        ereport(WARNING, (errmodule(MOD_DW), errmsg("Double write already closed")));
        return;
    }

    ereport(LOG, (errmodule(MOD_DW), errmsg("Double write exit")));

    /* Do a final truncate before free resource. */
    dw_truncate();

    dw_free_resource(ctx);

    ctx->initialized = 0;
}
