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
#include "storage/smgr/smgr.h"
#include "storage/smgr/segment.h"
#include "pgstat.h"
#include "utils/palloc.h"
#include "gstrace/gstrace_infra.h"
#include "gstrace/access_gstrace.h"
#include "gs_bbox.h"
#include "postmaster/bgwriter.h"
#include "knl/knl_thread.h"
#include "tde_key_management/tde_key_storage.h"

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

Datum dw_get_file_id()
{
    return UInt64GetDatum((int64)g_stat_file_id);
}

Datum dw_get_dw_number()
{
    dw_batch_file_context *batch_file_cxt;

    if (dw_enabled()) {
        batch_file_cxt = &g_instance.dw_batch_cxt.batch_file_cxts[g_stat_file_id];
        return UInt64GetDatum((uint64)batch_file_cxt->file_head->head.dwn);
    }

    return UInt64GetDatum(0);
}

Datum dw_get_start_page()
{
    dw_batch_file_context *batch_file_cxt;

    if (dw_enabled()) {
        batch_file_cxt = &g_instance.dw_batch_cxt.batch_file_cxts[g_stat_file_id];
        return UInt64GetDatum((uint64)batch_file_cxt->file_head->start);
    }

    return UInt64GetDatum(0);
}

Datum dw_get_file_trunc_num()
{
    dw_batch_file_context *batch_file_cxt;

    if (dw_enabled()) {
        batch_file_cxt = &g_instance.dw_batch_cxt.batch_file_cxts[g_stat_file_id];
        return UInt64GetDatum(batch_file_cxt->batch_stat_info.file_trunc_num);
    }

    return UInt64GetDatum(0);
}

Datum dw_get_file_reset_num()
{
    dw_batch_file_context *batch_file_cxt;

    if (dw_enabled()) {
        batch_file_cxt = &g_instance.dw_batch_cxt.batch_file_cxts[g_stat_file_id];
        return UInt64GetDatum(batch_file_cxt->batch_stat_info.file_reset_num);
    }

    return UInt64GetDatum(0);
}

Datum dw_get_total_writes()
{
    dw_batch_file_context *batch_file_cxt;

    if (dw_enabled()) {
        batch_file_cxt = &g_instance.dw_batch_cxt.batch_file_cxts[g_stat_file_id];
        return UInt64GetDatum(batch_file_cxt->batch_stat_info.total_writes);
    }

    return UInt64GetDatum(0);
}

Datum dw_get_low_threshold_writes()
{
    dw_batch_file_context *batch_file_cxt;

    if (dw_enabled()) {
        batch_file_cxt = &g_instance.dw_batch_cxt.batch_file_cxts[g_stat_file_id];
        return UInt64GetDatum(batch_file_cxt->batch_stat_info.low_threshold_writes);
    }

    return UInt64GetDatum(0);
}

Datum dw_get_high_threshold_writes()
{
    dw_batch_file_context *batch_file_cxt;

    if (dw_enabled()) {
        batch_file_cxt = &g_instance.dw_batch_cxt.batch_file_cxts[g_stat_file_id];
        return UInt64GetDatum(batch_file_cxt->batch_stat_info.high_threshold_writes);
    }

    return UInt64GetDatum(0);
}

Datum dw_get_total_pages()
{
    dw_batch_file_context *batch_file_cxt;

    if (dw_enabled()) {
        batch_file_cxt = &g_instance.dw_batch_cxt.batch_file_cxts[g_stat_file_id];
        return UInt64GetDatum(batch_file_cxt->batch_stat_info.total_pages);
    }

    return UInt64GetDatum(0);
}

Datum dw_get_low_threshold_pages()
{
    dw_batch_file_context *batch_file_cxt;


    if (dw_enabled()) {
        batch_file_cxt = &g_instance.dw_batch_cxt.batch_file_cxts[g_stat_file_id];
        return UInt64GetDatum(batch_file_cxt->batch_stat_info.low_threshold_pages);
    }

    return UInt64GetDatum(0);
}

Datum dw_get_high_threshold_pages()
{
    dw_batch_file_context *batch_file_cxt;

    if (dw_enabled()) {
        batch_file_cxt = &g_instance.dw_batch_cxt.batch_file_cxts[g_stat_file_id];
        return UInt64GetDatum(batch_file_cxt->batch_stat_info.high_threshold_pages);
    }

    return UInt64GetDatum(0);
}

/* double write statistic view */
const dw_view_col_t g_dw_view_col_arr[DW_VIEW_COL_NUM] = {
    { "node_name", TEXTOID, dw_get_node_name},
    { "curr_dwn", INT8OID, dw_get_dw_number},
    { "curr_start_page", INT8OID, dw_get_start_page},
    { "file_trunc_num", INT8OID, dw_get_file_trunc_num},
    { "file_reset_num", INT8OID, dw_get_file_reset_num},
    { "total_writes", INT8OID, dw_get_total_writes},
    { "low_threshold_writes", INT8OID, dw_get_low_threshold_writes},
    { "high_threshold_writes", INT8OID, dw_get_high_threshold_writes},
    { "total_pages", INT8OID, dw_get_total_pages},
    { "low_threshold_pages", INT8OID, dw_get_low_threshold_pages},
    { "high_threshold_pages", INT8OID, dw_get_high_threshold_pages},
    { "file_id", INT8OID, dw_get_file_id}
};

static int dw_fetch_file_id(int thread_id);
static void dw_fetch_thread_ids(int file_id, int &size, int *thread_ids);
static void dw_remove_batch_file(int dw_file_num);
static void dw_remove_batch_meta_file();
static void dw_recover_partial_write_batch(dw_batch_file_context *cxt);
static void dw_write_meta_file(int fd, dw_batch_meta_file *batch_meta_file);
static int dw_create_file(const char* file_name);
static void dw_generate_batch_file(int file_id, uint64 dw_file_size);
void dw_cxt_init_batch();


void dw_remove_file(const char* file_name)
{
    if (file_exists(file_name)) {
        ereport(LOG, (errcode_for_file_access(), errmodule(MOD_DW), errmsg("File: %s exists, deleting it", file_name)));

        if (unlink(file_name) != 0) {
            ereport(PANIC, (errcode_for_file_access(), errmodule(MOD_DW),
                errmsg("Could not remove the file: %s.", file_name)));
        }
    }
}

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

void dw_pwrite_file(int fd, const void *buf, int size, int64 offset, const char* fileName)
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
            ereport(PANIC, (errcode_for_file_access(), errmodule(MOD_DW),
                errmsg("Write file \"%s\" error: %m", fileName)));
        } else {
            break;
        }
    }
    if (write_size != size) {
        ereport(PANIC, (errcode_for_file_access(), errmodule(MOD_DW),
            errmsg("Write file \"%s\" size mismatch: expected %d, written %d", fileName, size, write_size)));
    }
}

int64 dw_seek_file(int fd, int64 offset, int32 origin)
{
    int64 seek_offset = lseek64(fd, (off64_t)offset, origin);
    if (seek_offset == -1) {
        ereport(PANIC, (errcode_for_file_access(), errmodule(MOD_DW),
            errmsg("Seek dw file error, seek offset is %ld, origin is %d, error: %m", offset, origin)));
    }
    return seek_offset;
}

void dw_extend_file(int fd, const void *buf, int buf_size, int64 size,
    int64 file_expect_size, bool single, char* file_name)
{
    int64 offset = 0;
    int64 remain_size;

    offset = dw_seek_file(fd, 0, SEEK_END);

    if ((offset + size) > file_expect_size) {
        ereport(PANIC,
            (errmodule(MOD_DW),
                errmsg("DW extend file failed, expected_file_size %ld, offset %ld, extend_size %ld",
                file_expect_size, offset, size)));
    }

    remain_size = size;
    while (remain_size > 0) {
        size = (remain_size > buf_size) ? buf_size : remain_size;
        dw_pwrite_file(fd, buf, size, offset, (single ? SINGLE_DW_FILE_NAME : file_name));
        offset += size;
        remain_size -= size;
    }
}

void dw_set_pg_checksum(char *page, BlockNumber blockNum)
{
    if (!CheckPageZeroCases((PageHeader)page)) {
        return;
    }

    /* set page->pd_flags mark using FNV1A for checksum */
    PageSetChecksumByFNV1A(page);
    ((PageHeader)page)->pd_checksum = pg_checksum_page(page, blockNum);
}

bool dw_verify_pg_checksum(PageHeader page_header, BlockNumber blockNum, bool dw_file)
{
    /* new page donot have crc and lsn, we donot recovery it */
    if (!CheckPageZeroCases(page_header)) {
        if (!dw_file) {
            ereport(WARNING, (errmodule(MOD_DW), errmsg("during dw recovery, verify checksum: new data page")));
        }
        return false;
    }
    uint16 checksum = pg_checksum_page((char *)page_header, blockNum);
    return checksum == page_header->pd_checksum;
}

static void dw_prepare_page(dw_batch_t *batch, uint16 page_num, uint16 page_id, uint16 dwn, bool is_new_relfilenode)
{
    if (is_new_relfilenode == true) {
        if (t_thrd.proc->workingVersionNum < DW_SUPPORT_SINGLE_FLUSH_VERSION) {
            page_num = page_num | IS_HASH_BKT_SEGPAGE_MASK;
        }
        if (t_thrd.proc->workingVersionNum < PAGE_COMPRESSION_VERSION) {
            batch->buftag_ver = HASHBUCKET_TAG;
        } else {
            batch->buftag_ver = PAGE_COMPRESS_TAG;
        }
    } else {
        batch->buftag_ver = ORIGIN_TAG;
    }
    batch->page_num = page_num;

    batch->head.page_id = page_id;
    batch->head.dwn = dwn;
    DW_PAGE_TAIL(batch)->dwn = dwn;
    dw_calc_batch_checksum(batch);
}

void dw_prepare_file_head(char *file_head, uint16 start, uint16 dwn, int32 dw_version)
{
    uint32 i;
    uint32 id;
    dw_file_head_t *curr_head = NULL;
    dw_version = (dw_version == -1 ? pg_atomic_read_u32(&g_instance.dw_single_cxt.dw_version) : dw_version);
    for (i = 0; i < DW_FILE_HEAD_ID_NUM; i++) {
        id = g_dw_file_head_ids[i];
        curr_head = (dw_file_head_t *)(file_head + sizeof(dw_file_head_t) * id);
        curr_head->head.page_id = 0;
        curr_head->head.dwn = dwn;
        curr_head->start = start;
        curr_head->buftag_version = PAGE_COMPRESS_TAG;
        curr_head->tail.dwn = dwn;
        curr_head->dw_version = dw_version;
        dw_calc_file_head_checksum(curr_head);
    }
}

static uint32 dw_recover_batch_file_head(dw_batch_file_context *batch_file_cxt)
{
    uint32 i;
    uint16 id;
    errno_t rc;
    int64 file_size;
    int64 offset;

    dw_file_head_t *curr_head = NULL;
    dw_file_head_t *working_head = NULL;
    uint64 head_offset = 0;
    uint32 dw_version = 0;
    char* file_head = (char *)batch_file_cxt->file_head;
    char* file_name = batch_file_cxt->file_name;
    int fd = batch_file_cxt->fd;

    dw_pread_file(fd, file_head, BLCKSZ, head_offset);

    for (i = 0; i < DW_FILE_HEAD_ID_NUM; i++) {
        id = g_dw_file_head_ids[i];
        curr_head = (dw_file_head_t *)(file_head + sizeof(dw_file_head_t) * id);
        if (dw_verify_file_head(curr_head)) {
            working_head = curr_head;
            break;
        }
    }

    if (working_head == NULL) {
        ereport(FATAL, (errcode_for_file_access(), errmodule(MOD_DW), errmsg("Batch file header is broken")));
        /* we should not get here, since FATAL will do abort. But for ut, return is needed */
        return dw_version;
    }

    ereport(LOG, (errmodule(MOD_DW), errmsg("Found a valid batch file header: id %hu, file_head[dwn %hu, start %hu]",
        id, working_head->head.dwn, working_head->start)));

    for (i = 0; i < DW_FILE_HEAD_ID_NUM; i++) {
        id = g_dw_file_head_ids[i];
        curr_head = (dw_file_head_t *)(file_head + sizeof(dw_file_head_t) * id);
        if (curr_head != working_head) {
            rc = memcpy_s(curr_head, sizeof(dw_file_head_t), working_head, sizeof(dw_file_head_t));
            securec_check(rc, "\0", "\0");
        }
    }

    offset = dw_seek_file(fd, 0, SEEK_END);
    file_size = batch_file_cxt->file_size;

    if (offset != file_size) {
        ereport(PANIC, (errmodule(MOD_DW),
            errmsg("DW check file size failed, expected_size %ld, actual_size %ld",
            batch_file_cxt->file_size, offset)));
    }

    dw_pwrite_file(fd, file_head, BLCKSZ, head_offset, file_name);
    return dw_version;
}

void dw_log_page_header(PageHeader page)
{
    ereport(DW_LOG_LEVEL,
            (errmodule(MOD_DW),
             errmsg("Page header info: pd_lsn %lu, pd_checksum %hu, "
                    "pd_lower %hu(%s), pd_upper %hu(%s), max_offset %hu",
                    PageGetLSN(page), page->pd_checksum, page->pd_lower, PageIsEmpty(page) ? "empty" : "non-empty",
                    page->pd_upper, PageIsNew(page) ? "new" : "old", PageGetMaxOffsetNumber((char *)page))));
}

template <typename T>
static inline void dw_log_data_page(int elevel, const char* state, T* buf_tag)
{
    ereport(elevel, (errmodule(MOD_DW),
                     errmsg("%s: buf_tag[rel %u/%u/%u blk %u fork %d]", state, buf_tag->rnode.spcNode,
                            buf_tag->rnode.dbNode, buf_tag->rnode.relNode, buf_tag->blockNum, buf_tag->forkNum)));
}

template <typename T>
static SMGR_READ_STATUS dw_recover_read_page(SMgrRelation relation, RelFileNode relnode, char *data_page, T *buf_tag)
{
    if (IsSegmentPhysicalRelNode(relnode)) {
        SMgrOpenSpace(relation);
        if (relation->seg_space == NULL) {
            dw_log_data_page(WARNING, "Segment data file deleted", buf_tag);
            return SMGR_READ_STATUS::SMGR_RD_NO_BLOCK;
        }
        if (spc_size(relation->seg_space, relnode.relNode, buf_tag->forkNum) <= buf_tag->blockNum) {
            dw_log_data_page(WARNING, "Segment data page deleted", buf_tag);
            return SMGR_READ_STATUS::SMGR_RD_NO_BLOCK;
        }
        seg_physical_read(relation->seg_space, relnode, buf_tag->forkNum, buf_tag->blockNum, data_page);
        if (!PageIsVerified((Page)data_page, buf_tag->blockNum)) {
            return SMGR_READ_STATUS::SMGR_RD_CRC_ERROR;
        } else {
            return SMGR_READ_STATUS::SMGR_RD_OK;
        }
    } else {
        if (!smgrexists(relation, buf_tag->forkNum, buf_tag->blockNum)) {
            dw_log_data_page(WARNING, "Data file deleted", buf_tag);
            return SMGR_READ_STATUS::SMGR_RD_NO_BLOCK;
        }
        BlockNumber blk_num = smgrnblocks(relation, buf_tag->forkNum);
        if (blk_num <= buf_tag->blockNum) {
            dw_log_data_page(WARNING, "Data page deleted", buf_tag);
            return SMGR_READ_STATUS::SMGR_RD_NO_BLOCK;
        }
        SMGR_READ_STATUS rdStatus = smgrread(relation, buf_tag->forkNum, buf_tag->blockNum, data_page);
        return rdStatus;
    }
}

template <typename T1, typename T2>
static void dw_recover_pages(T1 *batch, T2 *buf_tag, PageHeader data_page, BufTagVer tag_ver)
{
    uint16 i;
    PageHeader dw_page;
    SMgrRelation relation;
    RelFileNode relnode;
    bool pageCorrupted = false;

    for (i = 0; i < GET_REL_PGAENUM(batch->page_num); i++) {
        buf_tag = &batch->buf_tag[i];
        relnode.dbNode = buf_tag->rnode.dbNode;
        relnode.spcNode = buf_tag->rnode.spcNode;
        relnode.relNode = buf_tag->rnode.relNode;
        if (tag_ver == HASHBUCKET_TAG) {
            relnode.opt = 0;
            // 2 bytes are used for bucketNode.
            relnode.bucketNode = (int2)((BufferTagSecondVer *)buf_tag)->rnode.bucketNode;
        } else if (tag_ver == PAGE_COMPRESS_TAG) {
            relnode.opt = ((BufferTag *)buf_tag)->rnode.opt;
            relnode.bucketNode = ((BufferTag *)buf_tag)->rnode.bucketNode;
        } else {
            relnode.dbNode = buf_tag->rnode.dbNode;
            relnode.spcNode = buf_tag->rnode.spcNode;
            relnode.relNode = buf_tag->rnode.relNode;
            relnode.opt = 0;
            relnode.bucketNode = InvalidBktId;
        }
        dw_page = (PageHeader)((char *)batch + (i + 1) * BLCKSZ);
        if (!dw_verify_pg_checksum(dw_page, buf_tag->blockNum, true)) {
            dw_log_data_page(WARNING, "DW batch page broken", buf_tag);
            dw_log_page_header(dw_page);
            continue;
        }
        dw_log_data_page(DW_LOG_LEVEL, "DW page fine", buf_tag);
        dw_log_page_header(dw_page);

        relation = smgropen(relnode, InvalidBackendId, GetColumnNum(buf_tag->forkNum));

        SMGR_READ_STATUS rdStatus = dw_recover_read_page(relation, relnode, (char *)data_page, buf_tag);
        if (rdStatus == SMGR_READ_STATUS::SMGR_RD_NO_BLOCK) {
            continue;
        }
        pageCorrupted = (rdStatus == SMGR_READ_STATUS::SMGR_RD_CRC_ERROR);

        if (pageCorrupted || XLByteLT(PageGetLSN(data_page), PageGetLSN(dw_page))) {
            if (IsSegmentPhysicalRelNode(relnode)) {
                // seg_space must be initialized before.
                seg_physical_write(relation->seg_space, relnode, buf_tag->forkNum, buf_tag->blockNum,
                                   (const char *)dw_page, false);
            } else {
                smgrwrite(relation, buf_tag->forkNum, buf_tag->blockNum, (const char *)dw_page, false);
            }

            dw_log_data_page(LOG, "Date page recovered", buf_tag);
            dw_log_page_header(data_page);
        }
    }
}


void wait_all_dw_page_finish_flush()
{
    if (g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc != NULL) {
        for (int i = 0; i < g_instance.ckpt_cxt_ctl->pgwr_procs.num;) {
            if (g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc[i].thrd_dw_cxt.dw_page_idx == -1) {
                i++;
                continue;
            } else {
                (void)sched_yield();
            }
        }
    }
    return;
}


void wait_dw_page_finish_flush(int file_id)
{
    int i;
    int size;
    int thread_num;
    int thread_id;
    int *thread_ids;

    if (g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc != NULL) {
        thread_num = g_instance.ckpt_cxt_ctl->pgwr_procs.num;
        thread_ids = (int *)palloc0(thread_num * sizeof(int));
        dw_fetch_thread_ids(file_id, size, thread_ids);

        for (i = 0; i < size;) {
            thread_id = thread_ids[i];
            if (g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc[thread_id].thrd_dw_cxt.dw_page_idx == -1) {
                i++;
                continue;
            } else {
                (void)sched_yield();
            }
        }

        pfree(thread_ids);
    }
}

int get_dw_page_min_idx(int file_id)
{
    uint16 min_idx = 0;
    int dw_page_idx;
    int size;
    int thread_id;
    int* thread_ids;
    int thread_num;

    if (g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc != NULL) {
        thread_num = g_instance.ckpt_cxt_ctl->pgwr_procs.num;
        thread_ids = (int *)palloc0(thread_num * sizeof(int));

        dw_fetch_thread_ids(file_id, size, thread_ids);

        for (int i = 0; i < size; i++) {
            thread_id = thread_ids[i];
            dw_page_idx = g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc[thread_id].thrd_dw_cxt.dw_page_idx;
            if (dw_page_idx != -1) {
                if (min_idx == 0 || (uint16)dw_page_idx < min_idx) {
                    min_idx = dw_page_idx;
                }
            }
        }

        pfree(thread_ids);
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
static bool dw_batch_file_recycle(dw_batch_file_context *cxt, uint16 pages_to_write, bool trunc_file)
{
    bool file_full = false;
    uint16 min_idx = 0;
    dw_file_head_t *file_head = cxt->file_head;
    volatile uint16 org_start = file_head->start;
    volatile uint16 org_dwn = file_head->head.dwn;
    uint16 last_flush_page;
    uint16 dw_batch_page_num;

    dw_batch_page_num = (uint16)(cxt->file_size / BLCKSZ);
    file_full = (file_head->start + cxt->flush_page + pages_to_write >= dw_batch_page_num);

    Assert(!(file_full && trunc_file));
    if (!file_full && !trunc_file) {
        return true;
    }

    if (trunc_file) {
        Assert(AmStartupProcess() || AmCheckpointerProcess() || AmBootstrapProcess() || !IsUnderPostmaster);
        /*
         * Record min flush position for truncate because flush lock is not held during smgrsync.
         */
        min_idx = get_dw_page_min_idx(cxt->id);
        LWLockRelease(cxt->flush_lock);
    } else {
        Assert(AmStartupProcess() || AmPageWriterProcess());
        /* reset start position and flush page num for full recycle */
        file_head->start = DW_BATCH_FILE_START;
        cxt->flush_page = 0;
        wait_dw_page_finish_flush(cxt->id);
    }

    PageWriterSync();

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
    dw_pwrite_file(cxt->fd, file_head, BLCKSZ, 0, cxt->file_name);
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

static uint16 dw_calc_reading_pages(dw_read_asst_t *read_asst, uint64 file_size)
{
    dw_batch_t *curr_head;
    uint16 remain_pages, batch_pages, reading_pages;
    errno_t rc;
    uint16 dw_batch_page_num;

    dw_batch_page_num = (uint16) (file_size / BLCKSZ);
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
    Assert(read_asst->file_start + reading_pages <= dw_batch_page_num);
    return reading_pages;
}

static void dw_recover_batch_head(dw_batch_file_context *cxt, dw_batch_t *curr_head, bool is_new_relfilenode)
{
    errno_t rc;

    rc = memset_s(curr_head, BLCKSZ, 0, BLCKSZ);
    securec_check(rc, "\0", "\0");
    dw_prepare_page(curr_head, 0, cxt->file_head->start, cxt->file_head->head.dwn, is_new_relfilenode);
    pgstat_report_waitevent(WAIT_EVENT_DW_WRITE);
    dw_pwrite_file(cxt->fd, curr_head, BLCKSZ, (curr_head->head.page_id * BLCKSZ), cxt->file_name);
    pgstat_report_waitevent(WAIT_EVENT_END);
}

static inline void dw_log_recover_state(dw_batch_file_context *cxt, int elevel, const char *state, dw_batch_t *batch)
{
    ereport(elevel,
            (errmodule(MOD_DW),
             errmsg("DW recovery state: \"%s\", file start page[dwn %hu, start %hu], now access page %hu, "
                    "current [page_id %hu, dwn %hu, checksum verify res is %d, page_num orig %hu, page_num fixed %hu]",
                    state, cxt->file_head->head.dwn, cxt->file_head->start, cxt->flush_page, batch->head.page_id,
                    batch->head.dwn, dw_verify_batch_checksum(batch), batch->page_num,
                    GET_REL_PGAENUM(batch->page_num))));
}

static bool dw_batch_head_broken(dw_batch_file_context *cxt, dw_batch_t *curr_head)
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

static void dw_check_batch_parameter_change(knl_g_dw_context *batch_cxt)
{
    int g_dw_file_num;
    int g_dw_file_size;
    int dw_file_num;
    int dw_file_size;
    dw_batch_meta_file batch_meta_file;

    dw_file_num = batch_cxt->batch_meta_file.dw_file_num;
    dw_file_size = batch_cxt->batch_meta_file.dw_file_size;

    g_dw_file_num = g_instance.attr.attr_storage.dw_file_num;
    g_dw_file_size = g_instance.attr.attr_storage.dw_file_size;

    if (g_dw_file_num != dw_file_num || g_dw_file_size != dw_file_size) {
        ereport(LOG, (errmodule(MOD_DW),
            errmsg("old batch parameter: dw_file_num [%d], dw_file_size [%d] MB \
                it is changed to dw_file_num [%d], dw_file_size [%d] MB", dw_file_num, dw_file_size, g_dw_file_num,  g_dw_file_size)));

        /* free batch cxt resources, close file and reset state. */
        dw_exit(false);

        /* remove all meta and batch files. */
        dw_remove_batch_file(dw_file_num);
        dw_remove_batch_meta_file();

        /* generate new meta and batch files. */
        dw_generate_meta_file(&batch_meta_file);
        dw_generate_batch_files(g_dw_file_num, DW_FILE_SIZE_UNIT * g_dw_file_size);

        /* init batch cxt */
        dw_cxt_init_batch();
    }
}

static void dw_recover_all_partial_write_batch(knl_g_dw_context *batch_cxt)
{
    int i;
    int dw_file_num;
    dw_batch_file_context* batch_file_cxt;

    dw_file_num = batch_cxt->batch_meta_file.dw_file_num;

    for (i = 0; i < dw_file_num; i++) {
        batch_file_cxt = &batch_cxt->batch_file_cxts[i];
        (void)LWLockAcquire(batch_file_cxt->flush_lock, LW_EXCLUSIVE);
        dw_recover_partial_write_batch(batch_file_cxt);
        LWLockRelease(batch_file_cxt->flush_lock);
    }
}

static void dw_recover_partial_write_batch(dw_batch_file_context *cxt)
{
    dw_read_asst_t read_asst;
    dw_batch_t *curr_head = NULL;
    uint16 reading_pages;
    uint16 remain_pages;
    bool dw_file_broken = false;
    bool is_new_relfilenode;
    char *data_page = NULL;
    uint16 dw_batch_page_num = (uint16) (cxt->file_size / BLCKSZ);

    read_asst.fd = cxt->fd;
    read_asst.file_start = cxt->file_head->start;
    read_asst.file_capacity = dw_batch_page_num;
    read_asst.buf_start = 0;
    read_asst.buf_end = 0;
    read_asst.buf_capacity = DW_BUF_MAX;
    read_asst.buf = cxt->buf;
    reading_pages = Min(DW_BATCH_MAX_FOR_NOHBK, (dw_batch_page_num - cxt->file_head->start));

    data_page = (char *)palloc0(BLCKSZ);

    for (;;) {
        dw_read_pages(&read_asst, reading_pages);
        curr_head = (dw_batch_t *)(read_asst.buf + (read_asst.buf_start * BLCKSZ));

        if (!dw_verify_batch(curr_head, cxt->file_head->head.dwn)) {
            dw_file_broken = dw_batch_head_broken(cxt, curr_head);
            break;
        }

        if (t_thrd.proc->workingVersionNum < DW_SUPPORT_SINGLE_FLUSH_VERSION) {
            bool is_hashbucket = ((curr_head->page_num & IS_HASH_BKT_SEGPAGE_MASK) != 0);
            curr_head->buftag_ver = is_hashbucket ?
                                    (t_thrd.proc->workingVersionNum < PAGE_COMPRESSION_VERSION ? HASHBUCKET_TAG
                                                                                               : PAGE_COMPRESS_TAG)
                                                  : ORIGIN_TAG;
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

        reading_pages = dw_calc_reading_pages(&read_asst, cxt->file_size);
    }

    /* if free space not enough for one batch, reuse file. Otherwise, just do a truncate */
    if ((cxt->file_head->start + cxt->flush_page + DW_BUF_MAX) >= dw_batch_page_num) {
        (void)dw_batch_file_recycle(cxt, DW_BUF_MAX, false);
    } else if (cxt->flush_page > 0) {
        if (!dw_batch_file_recycle(cxt, 0, true)) {
            ereport(PANIC, (errcode_for_file_access(), errmodule(MOD_DW),
                            errmsg("Could not truncate dw file during startup!")));
        }
    }

    /* judge whether the buftag are hashbucket tag or origin tag. */
    is_new_relfilenode = ((curr_head->page_num & IS_HASH_BKT_SEGPAGE_MASK) != 0);
    if (dw_file_broken) {
        dw_recover_batch_head(cxt, curr_head, is_new_relfilenode);
    }
    dw_log_recover_state(cxt, LOG, "Finish", curr_head);
    pfree(data_page);
}

void dw_check_file_num()
{
    int old_num = g_instance.attr.attr_storage.dw_file_num;
    if (g_instance.attr.attr_storage.dw_file_num > g_instance.attr.attr_storage.pagewriter_thread_num) {
        g_instance.attr.attr_storage.dw_file_num = g_instance.attr.attr_storage.pagewriter_thread_num;

        ereport(LOG, (errmodule(MOD_DW),
            errmsg("dw_file_num no more than pagewriter_thread_num, so it is changed from [%d] to [%d]",
            old_num,  g_instance.attr.attr_storage.dw_file_num)));
    }
}

/* only for init db */
void dw_bootstrap()
{
    dw_batch_meta_file* batch_meta_file = &g_instance.dw_batch_cxt.batch_meta_file;

    /* when double write is disabled, generate pg_dw_meta file with dw_file_num = 0 */
    if (!dw_enabled()) {
        g_instance.attr.attr_storage.dw_file_num = 0;
        dw_generate_meta_file(batch_meta_file);
        return;
    }

    ereport(LOG, (errmodule(MOD_DW), errmsg("dw_bootstrap run start")));

    dw_check_file_num();
    dw_generate_meta_file(batch_meta_file);
    dw_generate_batch_files(batch_meta_file->dw_file_num, DW_FILE_SIZE_UNIT * batch_meta_file->dw_file_size);
    dw_generate_new_single_file();

    ereport(LOG, (errmodule(MOD_DW), errmsg("dw_bootstrap run end")));
}

static void dw_prepare_meta_info_old(dw_batch_meta_file *batch_meta_file)
{
    errno_t rc;

    rc = memset_s(batch_meta_file, sizeof(dw_batch_meta_file), 0, sizeof(dw_batch_meta_file));
    securec_check(rc, "\0", "\0");

    batch_meta_file->dw_file_num = 1;
    batch_meta_file->dw_file_size = MAX_DW_FILE_SIZE_MB;
    pg_atomic_write_u32(&batch_meta_file->dw_version, 0);
    batch_meta_file->checksum = 0;
}

static void dw_prepare_meta_info(dw_batch_meta_file *batch_meta_file)
{
    errno_t rc;

    rc = memset_s(batch_meta_file, sizeof(dw_batch_meta_file), 0, sizeof(dw_batch_meta_file));
    securec_check(rc, "\0", "\0");

    batch_meta_file->dw_file_num = g_instance.attr.attr_storage.dw_file_num;
    batch_meta_file->dw_file_size = g_instance.attr.attr_storage.dw_file_size;

    if (!ENABLE_INCRE_CKPT) {
        batch_meta_file->record_state |= DW_FULL_CKPT;
    }
    pg_atomic_write_u32(&batch_meta_file->dw_version, DW_SUPPORT_MULTIFILE_FLUSH);
    batch_meta_file->checksum = 0;
}

void dw_write_meta_file(int fd, dw_batch_meta_file *batch_meta_file)
{
    uint32 i;
    int buf_size;
    char* buf;
    char* unaligned_buf;
    errno_t rc;
    dw_batch_meta_file *tmp_batch_meta = NULL;

    buf_size = DW_META_FILE_BLOCK_NUM * BLCKSZ;
    unaligned_buf = (char *)palloc0(buf_size + BLCKSZ);
    buf = (char *)TYPEALIGN(BLCKSZ, unaligned_buf);

    dw_calc_meta_checksum(batch_meta_file);

    for (i = 0; i < DW_META_FILE_BLOCK_NUM; i++) {
        tmp_batch_meta = (dw_batch_meta_file *)(buf + i * BLCKSZ);
        rc = memmove_s(tmp_batch_meta, sizeof(dw_batch_meta_file), batch_meta_file, sizeof(dw_batch_meta_file));
        securec_check(rc, "\0", "\0");
    }

    dw_pwrite_file(fd, buf, buf_size, 0, DW_META_FILE);
    pfree(unaligned_buf);
}

void dw_generate_meta_file(dw_batch_meta_file* batch_meta_file)
{
    int fd;

    dw_prepare_meta_info(batch_meta_file);
    fd = dw_create_file(DW_META_FILE);

    dw_write_meta_file(fd, batch_meta_file);
    (void)close(fd);
}

void dw_generate_batch_files(int batch_file_num, uint64 dw_file_size)
{
    for (int i = 0; i < batch_file_num; i++) {
        dw_generate_batch_file(i, dw_file_size);
    }
}

static void dw_generate_batch_file(int file_id, uint64 dw_file_size)
{
    int64 remain_size;
    char* file_head = NULL;
    dw_batch_t* batch_head = NULL;
    int fd = -1;
    char* unaligned_buf = NULL;
    char batch_file_name[PATH_MAX];

    dw_fetch_batch_file_name(file_id, batch_file_name);

    /* create dw batch flush file */
    fd = open(batch_file_name, (DW_FILE_FLAG | O_CREAT), DW_FILE_PERM);
    if (fd == -1) {
        ereport(PANIC,
            (errcode_for_file_access(), errmodule(MOD_DW), errmsg("Could not create file \"%s\"", batch_file_name)));
    }

    /* Open file with O_SYNC, to make sure the data and file system control info on file after block writing. */
    unaligned_buf = (char *)palloc0(DW_FILE_EXTEND_SIZE + BLCKSZ);
    file_head = (char *)TYPEALIGN(BLCKSZ, unaligned_buf);

    /* file head and first batch head will be writen */
    remain_size = dw_file_size - BLCKSZ - BLCKSZ;
    dw_prepare_file_head(file_head, DW_BATCH_FILE_START, 0);
    batch_head = (dw_batch_t *)(file_head + BLCKSZ);
    batch_head->head.page_id = DW_BATCH_FILE_START;
    dw_calc_batch_checksum(batch_head);
    dw_pwrite_file(fd, file_head, (BLCKSZ + BLCKSZ), 0, batch_file_name);
    dw_extend_file(fd, file_head, DW_FILE_EXTEND_SIZE, remain_size, dw_file_size, false, batch_file_name);
    ereport(LOG, (errmodule(MOD_DW), errmsg("Double write batch flush file created successfully")));

    (void)close(fd);
    fd = -1;
    pfree(unaligned_buf);
    return;
}

static void dw_free_batch_file_resource(dw_batch_file_context *cxt)
{
    int rc = close(cxt->fd);
    if (rc == -1) {
        ereport(ERROR, (errcode_for_file_access(), errmodule(MOD_DW), errmsg("DW file close failed")));
    }

    cxt->fd = -1;
    cxt->flush_lock = NULL;
    cxt->buf = NULL;

    if (cxt->unaligned_buf != NULL) {
        pfree(cxt->unaligned_buf);
        cxt->unaligned_buf = NULL;
    }
}

static void dw_free_resource(knl_g_dw_context *cxt, bool single)
{
    int rc;
    int dw_file_num = cxt->batch_meta_file.dw_file_num;

    if (!single) {
        for (int i = 0; i < dw_file_num; i++) {
            dw_free_batch_file_resource(&cxt->batch_file_cxts[i]);
        }

        pfree(cxt->batch_file_cxts);
    }

    if (cxt->fd > 0) {
        rc = close(cxt->fd);
        if (rc == -1) {
            ereport(ERROR, (errcode_for_file_access(), errmodule(MOD_DW), errmsg("DW file close failed")));
        }
    }

    cxt->fd = -1;
    cxt->flush_lock = NULL;
    cxt->buf = NULL;
    if (cxt->single_flush_state != NULL) {
        pfree(cxt->single_flush_state);
        cxt->single_flush_state = NULL;
    }

    if (cxt->unaligned_buf != NULL) {
        pfree(cxt->unaligned_buf);
        cxt->unaligned_buf = NULL;
    }

    if (cxt->recovery_buf.unaligned_buf != NULL) {
        pfree(cxt->recovery_buf.unaligned_buf);
        cxt->recovery_buf.unaligned_buf = NULL;
    }

    if (cxt->recovery_buf.single_flush_state != NULL) {
        pfree(cxt->recovery_buf.single_flush_state);
        cxt->recovery_buf.single_flush_state = NULL;
    }
    cxt->closed = 1;
}

void dw_file_check_rebuild()
{
    int fd;
    dw_batch_meta_file batch_meta_file;

    if (!file_exists(DW_BUILD_FILE_NAME)) {
        return;
    }

    ereport(LOG, (errmodule(MOD_DW), errmsg("Double write initializing after build")));

    if (file_exists(OLD_DW_FILE_NAME)) {
        /*
         * Probably the gaussdb was killed during the first time startup after build, resulting in a half-written
         * DW file. So, log a warning message and remove the residual DW file.
         */
        ereport(WARNING, (errcode_for_file_access(), errmodule(MOD_DW),
            errmsg("batch flush DW file exists, deleting it")));

        if (unlink(OLD_DW_FILE_NAME) != 0) {
            ereport(PANIC, (errcode_for_file_access(), errmodule(MOD_DW),
                errmsg("Could not remove the residual batch flush DW single flush file")));
        }
    }

    if (file_exists(SINGLE_DW_FILE_NAME)) {
        /*
         * Probably the gaussdb was killed during the first time startup after build, resulting in a half-written
         * DW file. So, log a warning message and remove the residual DW file.
         */
        ereport(WARNING, (errcode_for_file_access(), errmodule(MOD_DW),
            errmsg("single flush DW file exists, deleting it")));

        if (unlink(SINGLE_DW_FILE_NAME) != 0) {
            ereport(PANIC, (errcode_for_file_access(), errmodule(MOD_DW),
                errmsg("Could not remove the residual single flush DW single flush file")));
        }
    }

    /* read meta file and remove batch file then remove meta file */
    if (file_exists(DW_META_FILE)) {
        ereport(WARNING, (errcode_for_file_access(), errmodule(MOD_DW), errmsg("batch meta file exists, deleting it")));
        fd = dw_open_file(DW_META_FILE);
        dw_recover_batch_meta_file(fd, &batch_meta_file);
        close(fd);

        dw_remove_batch_file(batch_meta_file.dw_file_num);

        dw_remove_batch_meta_file();
    }

    /* Create the DW file. */
    if (t_thrd.proc->workingVersionNum >= DW_SUPPORT_MULTIFILE_FLUSH) {
        dw_generate_meta_file(&batch_meta_file);
        dw_generate_batch_files(batch_meta_file.dw_file_num, DW_FILE_SIZE_UNIT * batch_meta_file.dw_file_size);
    } else {
        g_instance.dw_batch_cxt.old_batch_version = true;
        dw_generate_batch_file(-1, DW_FILE_SIZE);
    }

    /* during C20 upgrade to R2C00 */
    if (t_thrd.proc->workingVersionNum >= DW_SUPPORT_SINGLE_FLUSH_VERSION &&
        t_thrd.proc->workingVersionNum < DW_SUPPORT_NEW_SINGLE_FLUSH) {
        dw_generate_single_file();
    } else {
        /* during C10 upgrade to R2C00 or now is R2C00, need check dw upgrade file */
        dw_generate_new_single_file();
        if (file_exists(DW_UPGRADE_FILE_NAME) && unlink(DW_UPGRADE_FILE_NAME) != 0) {
            ereport(PANIC,
                (errcode_for_file_access(), errmodule(MOD_DW), errmsg("Could not remove the DW upgrade file")));
        }
    }

    /* Remove the DW build file. */
    if (unlink(DW_BUILD_FILE_NAME) != 0) {
        ereport(PANIC,
            (errcode_for_file_access(), errmodule(MOD_DW), errmsg("Could not remove the DW build file")));
    }

    return;
}

static bool dw_batch_upgrade_check()
{
    int fd;
    uint64 dw_file_size;
    dw_batch_meta_file* batch_meta_file;
    bool old_batch_version = false;

    batch_meta_file = &g_instance.dw_batch_cxt.batch_meta_file;

    if (t_thrd.proc->workingVersionNum < DW_SUPPORT_MULTIFILE_FLUSH) {
        return true;
    }

    if (!file_exists(DW_BATCH_UPGRADE_META_FILE_NAME) && !file_exists(DW_BATCH_UPGRADE_BATCH_FILE_NAME)) {
        if (file_exists(DW_META_FILE)) {
            /* dw_batch_upgrade succesfully */
            dw_remove_file(OLD_DW_FILE_NAME);
        } else {
            /* stop before dw_batch_upgrade or upgrade in place */
            if (file_exists(OLD_DW_FILE_NAME)) {
                old_batch_version = true;
            } else {
                /* initdb */
                dw_generate_meta_file(batch_meta_file);

                dw_file_size = DW_FILE_SIZE_UNIT * batch_meta_file->dw_file_size;
                dw_generate_batch_files(batch_meta_file->dw_file_num, dw_file_size);
            }
        }
    } else if (file_exists(DW_BATCH_UPGRADE_META_FILE_NAME)) {
        /* stop in the process of meta file generation */
        dw_remove_file(DW_META_FILE);
        dw_remove_file(DW_BATCH_UPGRADE_META_FILE_NAME);

        old_batch_version = true;
    } else if (file_exists(DW_BATCH_UPGRADE_BATCH_FILE_NAME)) {
        fd = dw_open_file(DW_META_FILE);
        dw_recover_batch_meta_file(fd, batch_meta_file);
        close(fd);

        dw_remove_batch_file(batch_meta_file->dw_file_num);
        dw_remove_batch_meta_file();
        dw_remove_file(DW_BATCH_UPGRADE_BATCH_FILE_NAME);

        old_batch_version = true;
    }

    return old_batch_version;
}

void dw_file_check()
{
    dw_batch_meta_file* batch_meta_file;

    dw_file_check_rebuild();

    batch_meta_file = &g_instance.dw_batch_cxt.batch_meta_file;
    g_instance.dw_batch_cxt.old_batch_version = dw_batch_upgrade_check();

    if (g_instance.dw_batch_cxt.old_batch_version == true) {
        g_instance.dw_batch_cxt.recovery_dw_file_num = g_instance.attr.attr_storage.dw_file_num;
        g_instance.dw_batch_cxt.recovery_dw_file_size = g_instance.attr.attr_storage.dw_file_size;

        dw_prepare_meta_info_old(batch_meta_file);

        g_instance.attr.attr_storage.dw_file_num = batch_meta_file->dw_file_num;
        g_instance.attr.attr_storage.dw_file_size = batch_meta_file->dw_file_size;
    }

    /* C20 or R2C00 version, the system must contain either dw single file or dw upgrade file. */
    if (t_thrd.proc->workingVersionNum >= DW_SUPPORT_SINGLE_FLUSH_VERSION) {
        if (!file_exists(SINGLE_DW_FILE_NAME) && !file_exists(DW_UPGRADE_FILE_NAME)) {
            ereport(PANIC, (errcode_for_file_access(),
                errmodule(MOD_DW), errmsg("single flush DW file does not exist and dw_upgrade file does not exist")));
        }
    }

    /* during C10 upgrade to R2C00, need generate the dw single file */
    if (!file_exists(SINGLE_DW_FILE_NAME)) {
        /* create dw batch flush file */
        int fd = open(DW_UPGRADE_FILE_NAME, (DW_FILE_FLAG | O_CREAT), DW_FILE_PERM);
        if (fd == -1) {
            ereport(PANIC,
                    (errcode_for_file_access(), errmodule(MOD_DW),
                        errmsg("Could not create file \"%s\"", DW_UPGRADE_FILE_NAME)));
        }
        ereport(LOG, (errmodule(MOD_DW),
                errmsg("first upgrade to DW_SUPPORT_NEW_SINGLE_FLUSH, need init the single file")));
        dw_generate_new_single_file();
        if (close(fd) != 0 || unlink(DW_UPGRADE_FILE_NAME) != 0) {
            ereport(PANIC,
                    (errcode_for_file_access(), errmodule(MOD_DW), errmsg("Could not remove the DW upgrade file")));
        }
    } else {
        /* during C20 upgrade to R2C00, if dw upgrade file exists, need generate new single file */
        if (file_exists(DW_UPGRADE_FILE_NAME)) {
            if (unlink(SINGLE_DW_FILE_NAME) != 0) {
                ereport(PANIC,
                        (errcode_for_file_access(), errmodule(MOD_DW), errmsg("Could not remove the DW single file")));
            }
            dw_generate_new_single_file();
            if (unlink(DW_UPGRADE_FILE_NAME) != 0) {
                ereport(PANIC,
                        (errcode_for_file_access(), errmodule(MOD_DW), errmsg("Could not remove the DW upgrade file")));
            }
        }
    }
}

void dw_fetch_batch_file_name(int file_id, char* buf)
{
    errno_t rc = EOK;

    /* in upgrade process, when upragde is not submitted, all write is flushed to OLD_DW_FILE_NAME */
    if (g_instance.dw_batch_cxt.old_batch_version) {
        rc = memmove_s(buf, PATH_MAX, OLD_DW_FILE_NAME, sizeof(OLD_DW_FILE_NAME));
        securec_check_c(rc, "\0", "\0");
        return;
    }

    rc = memmove_s(buf, PATH_MAX, DW_FILE_NAME_PREFIX, sizeof(DW_FILE_NAME_PREFIX));
    securec_check_c(rc, "\0", "\0");

    char* str_buf = (char *)palloc0(PATH_MAX);
    rc = sprintf_s(str_buf, PATH_MAX, "%d", file_id);
    securec_check_ss(rc, "", "");

    rc = strcat_s(buf, PATH_MAX, str_buf);
    securec_check_c(rc, "\0", "\0");

    pfree(str_buf);
}

static void dw_file_cxt_init_batch(int id, dw_batch_file_context *batch_file_cxt, uint64 file_size)
{
    uint32 buf_size;
    char *buf = NULL;

    Assert(batch_file_cxt->flush_lock == NULL);
    batch_file_cxt->flush_lock = LWLockAssign(LWTRANCHE_DOUBLE_WRITE);

    batch_file_cxt->id = id;
    dw_fetch_batch_file_name(batch_file_cxt->id, batch_file_cxt->file_name);
    batch_file_cxt->file_size = file_size;

    /* double write file disk space pre-allocated, O_DSYNC for less IO */
    batch_file_cxt->fd = open(batch_file_cxt->file_name, DW_FILE_FLAG, DW_FILE_PERM);
    if (batch_file_cxt->fd == -1) {
        ereport(PANIC,
            (errcode_for_file_access(), errmodule(MOD_DW),
                errmsg("Could not open file \"%s\"", batch_file_cxt->file_name)));
    }

    buf_size = DW_MEM_CTX_MAX_BLOCK_SIZE_FOR_NOHBK;
    batch_file_cxt->unaligned_buf = (char *)palloc0(buf_size); /* one more BLCKSZ for alignment */
    buf = (char *)TYPEALIGN(BLCKSZ, batch_file_cxt->unaligned_buf);

    batch_file_cxt->file_head = (dw_file_head_t *)buf;
    buf += BLCKSZ;

    (void)dw_recover_batch_file_head(batch_file_cxt);

    batch_file_cxt->buf = buf;
    if (BBOX_BLACKLIST_DW_BUFFER) {
        bbox_blacklist_add(DW_BUFFER, buf, buf_size - BLCKSZ - BLCKSZ);
    }

    batch_file_cxt->write_pos = 0;
    batch_file_cxt->flush_page = 0;
}

int dw_open_file(const char* file_name)
{
    int fd = open(file_name, DW_FILE_FLAG, DW_FILE_PERM);
    if (fd == -1) {
        ereport(PANIC,
            (errcode_for_file_access(), errmodule(MOD_DW), errmsg("Could not open file \"%s\"", file_name)));
    }
    return fd;
}

static int dw_create_file(const char* file_name)
{
    int fd = -1;
    fd = open(file_name, (DW_FILE_FLAG | O_CREAT), DW_FILE_PERM);
    if (fd == -1) {
        ereport(PANIC,
                (errcode_for_file_access(), errmodule(MOD_DW), errmsg("Could not create file \"%s\"", file_name)));
    }

    return fd;
}

bool dw_verify_meta_info(dw_batch_meta_file *batch_meta_file)
{
    uint32 checksum;
    uint16 org_cks = batch_meta_file->checksum;

    batch_meta_file->checksum = 0;
    checksum = pg_checksum_block((char*)batch_meta_file, sizeof(dw_batch_meta_file));
    batch_meta_file->checksum = org_cks;

    return (org_cks == REDUCE_CKS2UINT16(checksum));
}

void dw_recover_batch_meta_file(int fd, dw_batch_meta_file *batch_meta_file)
{
    uint32 i;
    char* buf;
    char* unaligned_buf;
    int buf_size;
    errno_t rc;
    dw_batch_meta_file *valid_batch_meta = NULL;
    dw_batch_meta_file *tmp_batch_meta = NULL;

    buf_size = DW_META_FILE_BLOCK_NUM * BLCKSZ;
    unaligned_buf = (char *)palloc0(buf_size + BLCKSZ);
    buf = (char *)TYPEALIGN(BLCKSZ, unaligned_buf);

    dw_pread_file(fd, buf, buf_size, 0);

    for (i = 0; i < DW_META_FILE_BLOCK_NUM; i++) {
        tmp_batch_meta = (dw_batch_meta_file *)(buf + i * BLCKSZ);
        if (dw_verify_meta_info(tmp_batch_meta)) {
            valid_batch_meta = tmp_batch_meta;
            break;
        }
    }

    if (valid_batch_meta == NULL) {
        ereport(FATAL, (errcode_for_file_access(), errmodule(MOD_DW), errmsg("Meta File is broken")));
        return;
    }

    ereport(LOG, (errmodule(MOD_DW),
        errmsg("Found a valid batch meta file info: dw_file_num [%d], dw_file_size [%d] MB, dw_version [%d]",
        valid_batch_meta->dw_file_num, valid_batch_meta->dw_file_size, valid_batch_meta->dw_version)));

    for (i = 0; i < DW_META_FILE_BLOCK_NUM; i++) {
        tmp_batch_meta = (dw_batch_meta_file *)(buf + i * BLCKSZ);
        if (tmp_batch_meta != valid_batch_meta) {
            rc = memcpy_s(tmp_batch_meta, sizeof(dw_batch_meta_file), valid_batch_meta, sizeof(dw_batch_meta_file));
            securec_check(rc, "\0", "\0");
        }
    }

    rc = memcpy_s(batch_meta_file, sizeof(dw_batch_meta_file), valid_batch_meta, sizeof(dw_batch_meta_file));
    securec_check(rc, "\0", "\0");

    dw_pwrite_file(fd, buf, buf_size, 0, DW_META_FILE);

    pfree(unaligned_buf);
}

void dw_remove_batch_meta_file()
{
    ereport(LOG, (errmodule(MOD_DW), errmsg("start remove dw_batch_meta_file.")));
    dw_remove_file(DW_META_FILE);
}

void dw_remove_batch_file(int dw_file_num)
{
    int i;
    char batch_file_name[PATH_MAX];

    ereport(LOG, (errmodule(MOD_DW), errmsg("start remove dw_batch_files.")));

    for (i = 0; i < dw_file_num; i++) {
        dw_fetch_batch_file_name(i, batch_file_name);
        dw_remove_file(batch_file_name);
    }
}

void dw_cxt_init_batch()
{
    int fd;
    int i;
    int dw_file_num;
    uint dw_version;
    dw_batch_file_context *batch_file_cxt;
    knl_g_dw_context *dw_batch_cxt = &g_instance.dw_batch_cxt;
    dw_batch_meta_file *batch_meta_file = &dw_batch_cxt->batch_meta_file;

    if (dw_batch_cxt->old_batch_version) {
        dw_batch_cxt->fd = -1;
        dw_version = 0;
    } else {
        fd = dw_open_file(DW_META_FILE);
        dw_batch_cxt->fd = fd;

        /* init global batch meta file info */
        (void)dw_recover_batch_meta_file(fd, batch_meta_file);
        dw_version = batch_meta_file->dw_version;
    }

    if (dw_batch_cxt->flush_lock == NULL) {
        dw_batch_cxt->flush_lock = LWLockAssign(LWTRANCHE_DOUBLE_WRITE);
    }

    dw_file_num = batch_meta_file->dw_file_num;
    dw_batch_cxt->batch_file_cxts = (dw_batch_file_context *)palloc0(dw_file_num * sizeof(dw_batch_file_context));

    /* init global batch double write file head info */
    for (i = 0; i < dw_file_num; i++) {
        batch_file_cxt = &dw_batch_cxt->batch_file_cxts[i];
        dw_file_cxt_init_batch(i, batch_file_cxt, DW_FILE_SIZE_UNIT * batch_meta_file->dw_file_size);
    }

    pg_atomic_write_u32(&dw_batch_cxt->dw_version, dw_version);


    /* they are used by single flush, and setted NULL here. */
    dw_batch_cxt->single_flush_state = NULL;
    dw_batch_cxt->unaligned_buf = NULL;
    dw_batch_cxt->recovery_buf.unaligned_buf = NULL;
    dw_batch_cxt->recovery_buf.single_flush_state = NULL;

    dw_batch_cxt->closed = 0;
}

static void dw_check_meta_file()
{
    int fd;
    dw_batch_meta_file *batch_meta_file;

    if (file_exists(DW_META_FILE)) {
        batch_meta_file = &g_instance.dw_batch_cxt.batch_meta_file;
        fd = dw_open_file(DW_META_FILE);
        dw_recover_batch_meta_file(fd, batch_meta_file);
        close(fd);

        /* Notice the last time the database was started with full checkpoint or incremental checkpoint mode. */
        if (batch_meta_file->dw_file_num == 0) {
            if ((batch_meta_file->record_state & DW_FULL_CKPT) > 0) {
                ereport(LOG, (errmodule(MOD_DW), errmsg("The last time database run in full checkpoint mode.")));
            } else {
                ereport(LOG, (errmodule(MOD_DW), errmsg("The last time database run in incremental checkpoint mode.")));
            }

            fd = dw_create_file(DW_BUILD_FILE_NAME);
            close(fd);
        }
    }
}

void dw_upgrade_renable_double_write()
{
    if (g_instance.attr.attr_storage.enable_double_write) {
        return;
    }

    ereport(LOG, (errmodule(MOD_DW), errmsg("support renable dw upgrade start")));

    /* generate the pg_dw_meta with dw_file_num = 0 */
    g_instance.attr.attr_storage.dw_file_num = 0;
    dw_generate_meta_file(&g_instance.dw_batch_cxt.batch_meta_file);

    /* old version dw files were not deleted when double write is disabled, delete here */
    dw_remove_file(OLD_DW_FILE_NAME);
    dw_remove_file(SINGLE_DW_FILE_NAME);

    pg_atomic_write_u32(&g_instance.dw_batch_cxt.dw_version, DW_SUPPORT_REABLE_DOUBLE_WRITE);

    ereport(LOG, (errmodule(MOD_DW), errmsg("support renable dw upgrade end")));
}

static void dw_record_ckpt_state()
{
    int fd;
    dw_batch_meta_file *batch_meta_file = &g_instance.dw_batch_cxt.batch_meta_file;

    if (file_exists(DW_META_FILE)) {
        if (ENABLE_INCRE_CKPT) {
            batch_meta_file->record_state &= (~DW_FULL_CKPT);
        } else {
            batch_meta_file->record_state |= DW_FULL_CKPT;
        }

        fd = dw_open_file(DW_META_FILE);
        dw_write_meta_file(fd, batch_meta_file);
        close(fd);
    }
}

/* when double write and incremental checkpoint are enabled, init dw files here */
void dw_enable_init()
{
    knl_g_dw_context *batch_cxt = &g_instance.dw_batch_cxt;
    knl_g_dw_context *single_cxt = &g_instance.dw_single_cxt;

    dw_check_meta_file();
    dw_check_file_num();
    dw_file_check();

    /* init dw batch and dw single */
    dw_cxt_init_batch();
    dw_cxt_init_single();

    /* recovery batch flush dw file */
    dw_recover_all_partial_write_batch(batch_cxt);

    if (!batch_cxt->old_batch_version) {
        /* Check batch flush whether there is a change in dw_file_num and dw_file_size */
        dw_check_batch_parameter_change(batch_cxt);
    }

    /* recovery single flush dw file */
    (void)LWLockAcquire(single_cxt->flush_lock, LW_EXCLUSIVE);
    dw_recovery_partial_write_single();
    LWLockRelease(single_cxt->flush_lock);
}

/* when double write is disabled, init dw files here */
bool dw_disable_init()
{
    int fd;
    bool disable_dw_first_init = false;
    knl_g_dw_context *batch_cxt = &g_instance.dw_batch_cxt;
    knl_g_dw_context *single_cxt = &g_instance.dw_single_cxt;

    if (t_thrd.proc->workingVersionNum >= DW_SUPPORT_REABLE_DOUBLE_WRITE) {
        /* perform build process when double write is diabled */
        if (file_exists(DW_BUILD_FILE_NAME)) {
            g_instance.attr.attr_storage.dw_file_num = 0;
            dw_generate_meta_file(&batch_cxt->batch_meta_file);
            dw_remove_file(DW_BUILD_FILE_NAME);
        }

        /* normal initialization */
        if (file_exists(DW_META_FILE)) {
            fd = dw_open_file(DW_META_FILE);
            dw_recover_batch_meta_file(fd, &batch_cxt->batch_meta_file);
            close(fd);

            /* run for the first time after double write was disabled */
            if (batch_cxt->batch_meta_file.dw_file_num > 0) {
                /* init dw batch and dw single */
                dw_cxt_init_batch();
                dw_cxt_init_single();

                /* recovery batch flush dw file */
                dw_recover_all_partial_write_batch(batch_cxt);

                /* set dw_file_num to 0, remove all the dw batch files and keep the meta file */
                g_instance.attr.attr_storage.dw_file_num = 0;
                dw_check_batch_parameter_change(batch_cxt);

                /* recovery single flush dw file */
                (void)LWLockAcquire(single_cxt->flush_lock, LW_EXCLUSIVE);
                dw_recovery_partial_write_single();
                LWLockRelease(single_cxt->flush_lock);

                disable_dw_first_init = true;
            }
            pg_atomic_write_u32(&batch_cxt->dw_version, t_thrd.proc->workingVersionNum);
        } else {
            /* for upgrade process */
            pg_atomic_write_u32(&batch_cxt->dw_version, 0);
        }
    } else {
        /*
         * Between DW_SUPPORT_BCM_VERSION and DW_SUPPORT_REABLE_DOUBLE_WRITE,
         * double write files are deleted when double write is disabled. When
         * perform build in upgrade process, it will do nothing.
         */
        if (t_thrd.proc->workingVersionNum >= DW_SUPPORT_BCM_VERSION) {
            if (file_exists(DW_BUILD_FILE_NAME)) {
                dw_remove_file(DW_BUILD_FILE_NAME);
            }
        } else {
            /*
             * In the other situtations including C00, C20 and C10 before DW_SUPPORT_BCM_VERSION,
             * when double write is disabled, double write files are left. When perform build
             * in the upgrade process, it will generate double write files again.
             */
            dw_file_check_rebuild();
        }
        pg_atomic_write_u32(&batch_cxt->dw_version, t_thrd.proc->workingVersionNum);
    }

    return disable_dw_first_init;
}

void dw_init(bool shut_down)
{
    MemoryContext old_mem_cxt;
    bool disable_dw_first_init = false;
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
    pg_atomic_write_u32(&g_instance.dw_single_cxt.dw_version, 0);
    pg_atomic_write_u32(&g_instance.dw_batch_cxt.dw_version, 0);

    old_mem_cxt = MemoryContextSwitchTo(mem_cxt);
    ereport(LOG, (errmodule(MOD_DW), errmsg("Double Write init")));

    /* when double write is enabled, increamental checkpoint must be enabled too */
    if (dw_enabled()) {
        dw_enable_init();
    } else {
        /* Notice: double write can be disabled alone when the read/write page unit is atomic */
        disable_dw_first_init = dw_disable_init();
    }

    dw_record_ckpt_state();

    /*
     * After recovering partially written pages (if any), we will un-initialize, if the double write is disabled.
     */
    if (disable_dw_first_init) {
        dw_free_resource(batch_cxt, false);
        dw_free_resource(single_cxt, true);
        (void)MemoryContextSwitchTo(old_mem_cxt);
        MemoryContextDelete(g_instance.dw_batch_cxt.mem_cxt);

        if (file_exists(OLD_DW_FILE_NAME)) {
            /* If the double write is disabled, log a warning message and remove the residual DW file. */
            ereport(WARNING, (errcode_for_file_access(), errmodule(MOD_DW),
                errmsg("batch flush DW file exists, deleting it when the double write is disabled")));

            if (unlink(OLD_DW_FILE_NAME) != 0) {
                ereport(PANIC, (errcode_for_file_access(), errmodule(MOD_DW),
                    errmsg("Could not remove the residual batch flush DW single flush file")));
            }
        }

        if (file_exists(SINGLE_DW_FILE_NAME)) {
            /* If the double write is disabled, log a warning message and remove the single DW file. */
            ereport(WARNING, (errcode_for_file_access(), errmodule(MOD_DW),
                errmsg("single flush DW file exists, deleting it when the double write is disabled")));

            if (unlink(SINGLE_DW_FILE_NAME) != 0) {
                ereport(PANIC, (errcode_for_file_access(), errmodule(MOD_DW),
                    errmsg("Could not remove the residual single flush DW single flush file")));
            }
        }

        ereport(LOG, (errmodule(MOD_DW), errmsg("Double write exit after recovering partial write")));
    } else {
        if (pg_atomic_read_u32(&g_instance.dw_single_cxt.dw_version) != 0 &&
            g_instance.dw_single_cxt.recovery_buf.unaligned_buf != NULL) {
            pfree(g_instance.dw_single_cxt.recovery_buf.unaligned_buf);
            g_instance.dw_single_cxt.recovery_buf.unaligned_buf = NULL;
        }
        (void)MemoryContextSwitchTo(old_mem_cxt);
    }

    smgrcloseall();
}

void dw_transfer_phybuffer_addr(const BufferDesc *buf_desc, BufferTag *buf_tag)
{
    if (XLOG_NEED_PHYSICAL_LOCATION(buf_desc->tag.rnode)) {
        if (buf_desc->seg_fileno != EXTENT_INVALID) {
            // buffer descriptor contains the physical location
            Assert(buf_desc->seg_fileno <= EXTENT_TYPES && buf_desc->seg_fileno > EXTENT_INVALID);
            buf_tag->rnode.relNode = buf_desc->seg_fileno;
            buf_tag->blockNum = buf_desc->seg_blockno;
        } else {
            SegPageLocation loc =
                seg_get_physical_location(buf_desc->tag.rnode, buf_desc->tag.forkNum, buf_desc->tag.blockNum);
            Assert(loc.blocknum != InvalidBlockNumber);
            buf_tag->rnode.relNode = (uint8) EXTENT_SIZE_TO_TYPE(loc.extent_size);
            buf_tag->blockNum = loc.blocknum;
        }
        if (buf_tag->blockNum == InvalidBlockNumber || buf_tag->rnode.relNode == EXTENT_INVALID) {
            Assert(0);
            ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
                errmsg("dw_transfer_phybuffer_addr failed !blockNum is %u, relNode is %d.\n", buf_tag->blockNum,
                buf_tag->rnode.relNode)));
        }
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
    if (thrd_dw_cxt->write_pos <= GET_DW_BATCH_DATA_PAGE_MAX(thrd_dw_cxt->is_new_relfilenode)) {
        batch = (dw_batch_t*)thrd_dw_cxt->dw_buf;
        page_num = thrd_dw_cxt->write_pos;
    } else {
        batch = (dw_batch_t*)(thrd_dw_cxt->dw_buf +
            (GET_DW_BATCH_DATA_PAGE_MAX(thrd_dw_cxt->is_new_relfilenode) + 1) * BLCKSZ);
        page_num = thrd_dw_cxt->write_pos - GET_DW_BATCH_DATA_PAGE_MAX(thrd_dw_cxt->is_new_relfilenode);
    }

    BlockNumber phyBlock = buf_desc->tag.blockNum;

    if (thrd_dw_cxt->is_new_relfilenode) {
        batch->buf_tag[page_num - 1] = buf_desc->tag;
        dw_transfer_phybuffer_addr(buf_desc, &batch->buf_tag[page_num - 1]);

        /* physical block may be changed */
        phyBlock = batch->buf_tag[page_num - 1].blockNum;
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
    if (buf_desc->encrypt) {
        dw_encrypt_page(buf_desc->tag, dest_addr);
    }

    dw_set_pg_checksum(dest_addr, phyBlock);
    return page_lsn;
}

inline uint16 dw_batch_add_extra(uint16 page_num, bool is_new_relfilenode)
{
    Assert(page_num <= GET_DW_DIRTY_PAGE_MAX(is_new_relfilenode));
    if (page_num <= GET_DW_BATCH_DATA_PAGE_MAX(is_new_relfilenode)) {
        return page_num + DW_EXTRA_FOR_ONE_BATCH;
    } else {
        return page_num + DW_EXTRA_FOR_TWO_BATCH;
    }
}

static void dw_assemble_batch(dw_batch_file_context *dw_cxt, uint16 page_id, uint16 dwn, bool is_new_relfilenode)
{
    dw_batch_t *batch = NULL;
    uint16 first_batch_pages;
    uint16 second_batch_pages;

    if (dw_cxt->write_pos > GET_DW_BATCH_DATA_PAGE_MAX(is_new_relfilenode)) {
        first_batch_pages = GET_DW_BATCH_DATA_PAGE_MAX(is_new_relfilenode);
        second_batch_pages = dw_cxt->write_pos - GET_DW_BATCH_DATA_PAGE_MAX(is_new_relfilenode);
    } else {
        first_batch_pages = dw_cxt->write_pos;
        second_batch_pages = 0;
    }

    batch = (dw_batch_t *)dw_cxt->buf;
    dw_prepare_page(batch, first_batch_pages, page_id, dwn, is_new_relfilenode);

    /* tail of the first batch */
    page_id = page_id + 1 + GET_REL_PGAENUM(batch->page_num);
    batch = dw_batch_tail_page(batch);
    dw_prepare_page(batch, second_batch_pages, page_id, dwn, is_new_relfilenode);

    if (second_batch_pages == 0) {
        return;
    }
    /* also head of the second batch, if second batch not empty, prepare its tail */
    page_id = page_id + 1 + GET_REL_PGAENUM(batch->page_num);
    batch = dw_batch_tail_page(batch);
    dw_prepare_page(batch, 0, page_id, dwn, is_new_relfilenode);
}

static inline void dw_stat_batch_flush(dw_stat_info_batch *stat_info, uint32 page_to_write, bool is_new_relfilenode)
{
    (void)pg_atomic_add_fetch_u64(&stat_info->total_writes, 1);
    (void)pg_atomic_add_fetch_u64(&stat_info->total_pages, page_to_write);
    if (page_to_write < DW_WRITE_STAT_LOWER_LIMIT) {
        (void)pg_atomic_add_fetch_u64(&stat_info->low_threshold_writes, 1);
        (void)pg_atomic_add_fetch_u64(&stat_info->low_threshold_pages, page_to_write);
    } else if (page_to_write > GET_DW_BATCH_MAX(is_new_relfilenode)) {
        (void)pg_atomic_add_fetch_u64(&stat_info->high_threshold_writes, 1);
        (void)pg_atomic_add_fetch_u64(&stat_info->high_threshold_pages, page_to_write);
    }
}

/**
 * flush the copied page in the buffer into dw file, allocate the token for outside data file flushing
 * @param dw_cxt double write context
 * @param latest_lsn the latest lsn in the copied pages
 */
static void dw_batch_flush(dw_batch_file_context *dw_cxt, XLogRecPtr latest_lsn, ThrdDwCxt* thrd_dw_cxt)
{
    uint16 offset_page;
    bool is_new_relfilenode;
    uint16 pages_to_write = 0;
    dw_file_head_t* file_head = NULL;
    errno_t rc;

    if (!XLogRecPtrIsInvalid(latest_lsn)) {
        XLogWaitFlush(latest_lsn);
    }

    (void)LWLockAcquire(dw_cxt->flush_lock, LW_EXCLUSIVE);

    is_new_relfilenode = thrd_dw_cxt->is_new_relfilenode;
    dw_cxt->write_pos = thrd_dw_cxt->write_pos;
    Assert(dw_cxt->write_pos > 0);

    file_head = dw_cxt->file_head;
    pages_to_write = dw_batch_add_extra(dw_cxt->write_pos, is_new_relfilenode);
    rc = memcpy_s(dw_cxt->buf, pages_to_write * BLCKSZ, thrd_dw_cxt->dw_buf, pages_to_write * BLCKSZ);
    securec_check(rc, "\0", "\0");
    (void)dw_batch_file_recycle(dw_cxt, pages_to_write, false);

    /* calculate it after checking file space, in case of updated by sync */
    offset_page = file_head->start + dw_cxt->flush_page;

    dw_assemble_batch(dw_cxt, offset_page, file_head->head.dwn, is_new_relfilenode);

    pgstat_report_waitevent(WAIT_EVENT_DW_WRITE);
    dw_pwrite_file(dw_cxt->fd, dw_cxt->buf, (pages_to_write * BLCKSZ), (offset_page * BLCKSZ), dw_cxt->file_name);
    pgstat_report_waitevent(WAIT_EVENT_END);

    dw_stat_batch_flush(&dw_cxt->batch_stat_info, pages_to_write, is_new_relfilenode);
    /* the tail of this flushed batch is the head of the next batch */
    dw_cxt->flush_page += (pages_to_write - 1);
    dw_cxt->write_pos = 0;
    thrd_dw_cxt->dw_page_idx = offset_page;
    LWLockRelease(dw_cxt->flush_lock);

    ereport(DW_LOG_LEVEL,
            (errmodule(MOD_DW),
             errmsg("[batch flush] file_head[dwn %hu, start %hu], total_pages %hu, data_pages %hu, flushed_pages %hu",
                    dw_cxt->file_head->head.dwn, dw_cxt->file_head->start, dw_cxt->flush_page, dw_cxt->write_pos,
                    pages_to_write)));
}

void dw_perform_batch_flush(uint32 size, CkptSortItem *dirty_buf_list, int thread_id, ThrdDwCxt* thrd_dw_cxt)
{
    uint16 batch_size;
    int file_id;
    XLogRecPtr latest_lsn = InvalidXLogRecPtr;
    XLogRecPtr page_lsn;

    if (!dw_enabled()) {
        /* Double write is not enabled, nothing to do. */
        return;
    }

    file_id = dw_fetch_file_id(thread_id);
    dw_batch_file_context *dw_cxt = &g_instance.dw_batch_cxt.batch_file_cxts[file_id];

    if (SECUREC_UNLIKELY(pg_atomic_read_u32(&g_instance.dw_batch_cxt.closed))) {
        ereport(ERROR, (errmodule(MOD_DW), errmsg("[batch flush] Double write already closed")));
    }

    Assert(size > 0 && size <= GET_DW_DIRTY_PAGE_MAX(thrd_dw_cxt->is_new_relfilenode));
    batch_size = (uint16)size;
    thrd_dw_cxt->write_pos = 0;

    for (uint16 i = 0; i < batch_size; i++) {
        bool is_skipped = false;
        page_lsn = dw_copy_page(thrd_dw_cxt, dirty_buf_list[i].buf_id, &is_skipped);
        if (is_skipped) {
            /*
             * We couldn't acquire conditional lock on the buffer content_lock.
             * So we mark it in buf_id_arr.
             */
            BufferDesc *buf_desc = NULL;
            buf_desc = GetBufferDescriptor(dirty_buf_list[i].buf_id);
            dirty_buf_list[i].buf_id = DW_INVALID_BUFFER_ID;
            continue;
        }

        if (XLByteLT(latest_lsn, page_lsn)) {
            latest_lsn = page_lsn;
        }
    }
    if (FORCE_FINISH_ENABLED) {
        update_max_page_flush_lsn(latest_lsn, t_thrd.proc_cxt.MyProcPid, false);
    }
    if (thrd_dw_cxt->write_pos > 0) {
        dw_batch_flush(dw_cxt, latest_lsn, thrd_dw_cxt);
    }
}


static void dw_batch_file_truncate(dw_batch_file_context *cxt)
{
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

    if (dw_batch_file_recycle(cxt, 0, true)) {
        LWLockRelease(cxt->flush_lock);
    }

    ereport(LOG, (errmodule(MOD_DW),
        errmsg("[batch flush] DW truncate end: file_head[dwn %hu, start %hu], total_pages %hu",
        cxt->file_head->head.dwn, cxt->file_head->start, cxt->flush_page)));
}

void dw_batch_file_truncate()
{
    int i;
    int dw_file_num = g_instance.dw_batch_cxt.batch_meta_file.dw_file_num;
    knl_g_dw_context *cxt = &g_instance.dw_batch_cxt;

    if (!LWLockConditionalAcquire(cxt->flush_lock, LW_SHARED)) {
        ereport(LOG, (errmodule(MOD_DW),
            errmsg("[batch flush] Can not get dw flush lock and skip dw truncate for this time")));
        return;
    }

    for (i = 0; i < dw_file_num; i++) {
        dw_batch_file_truncate(&cxt->batch_file_cxts[i]);
    }

    LWLockRelease(cxt->flush_lock);
}

void dw_truncate()
{
    if (!dw_enabled()) {
        /* Double write is not enabled, nothing to do. */
        return;
    }

    gstrace_entry(GS_TRC_ID_dw_truncate);
    dw_batch_file_truncate();
    if (pg_atomic_read_u32(&g_instance.dw_single_cxt.dw_version) == DW_SUPPORT_NEW_SINGLE_FLUSH) {
        dw_single_file_truncate(true);
        dw_single_file_truncate(false);
    } else {
        dw_single_old_file_truncate();
    }
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

    dw_free_resource(dw_cxt, single);
}

void dw_encrypt_page(BufferTag tag, char* buf)
{
    TdeInfo tde_info = {0};
    TDE::TDEBufferCache::get_instance().search_cache(tag.rnode, &tde_info);
    if (strlen(tde_info.dek_cipher) == 0) {
        ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
            errmsg("double write copy page get TDE buffer cache entry failed, RelFileNode is %u/%u/%u/%u",
                   tag.rnode.spcNode, tag.rnode.dbNode, tag.rnode.relNode,
                   tag.rnode.bucketNode),
            errdetail("N/A"),
            errcause("TDE cache miss this key"),
            erraction("check cache status")));
    }
    PageDataEncryptIfNeed(buf, &tde_info, false);
    return;
}

bool free_space_enough(int buf_id)
{
    Page page = BufferGetPage(buf_id + 1);
    PageHeader pghr = (PageHeader)page;
    if(pghr->pd_upper - pghr->pd_lower >= (int)sizeof(dw_first_flush_item)) {
        return true;
    }
    return false;
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

void dw_log_recovery_page(int elevel, const char *state, BufferTag buf_tag)
{
    ereport(elevel, (errmodule(MOD_DW),
        errmsg("[single flush] recovery, %s: buf_tag[rel %u/%u/%u blk %u fork %d], compress: %u",
            state, buf_tag.rnode.spcNode, buf_tag.rnode.dbNode, buf_tag.rnode.relNode, buf_tag.blockNum,
            buf_tag.forkNum, buf_tag.rnode.opt)));
}

bool dw_read_data_page(BufferTag buf_tag, SMgrRelation reln, char* data_block)
{
    BlockNumber blk_num;

    if (IsSegmentPhysicalRelNode(buf_tag.rnode)) {
        SMgrOpenSpace(reln);
        if (reln->seg_space == NULL) {
            dw_log_recovery_page(WARNING, "Segment data file deleted", buf_tag);
            return false;
        }
        if (spc_size(reln->seg_space, buf_tag.rnode.relNode, buf_tag.forkNum) <= buf_tag.blockNum) {
            dw_log_recovery_page(WARNING, "Segment data page deleted", buf_tag);
            return false;
        }
        seg_physical_read(reln->seg_space, buf_tag.rnode, buf_tag.forkNum, buf_tag.blockNum,
                          (char *)data_block);
    } else {
        if (!smgrexists(reln, buf_tag.forkNum, buf_tag.blockNum)) {
            dw_log_recovery_page(WARNING, "Data file deleted", buf_tag);
            return false;
        }
        blk_num = smgrnblocks(reln, buf_tag.forkNum);
        if (blk_num <= buf_tag.blockNum) {
            dw_log_recovery_page(WARNING, "Data page deleted", buf_tag);
            return false;
        }
        /* read data page */
        smgrread(reln, buf_tag.forkNum, buf_tag.blockNum, data_block);
    }
    return true;
}

/*
 * If the dw is enable, and the pagewriter thread is running, indicates that the device is not in the initialization
 * phase, when the version num smaller than DW_SUPPORT_NEW_SINGLE_FLUSH, not support the
 * backend thread flush dirty page.
 */
bool backend_can_flush_dirty_page()
{
    if (dw_enabled() && pg_atomic_read_u32(&g_instance.ckpt_cxt_ctl->current_page_writer_count) > 0 &&
        (t_thrd.proc->workingVersionNum < DW_SUPPORT_NEW_SINGLE_FLUSH ||
        pg_atomic_read_u32(&g_instance.dw_single_cxt.dw_version) < DW_SUPPORT_NEW_SINGLE_FLUSH)) {
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
    t_thrd.proc->flush_new_dw = true;
    t_thrd.proc->dw_pos = -1;
    (void)MemoryContextSwitchTo(oldContext);
}

void reset_dw_pos_flag()
{
    if (t_thrd.proc->dw_pos != -1) {
        uint16 pos = t_thrd.proc->dw_pos;
        if (t_thrd.proc->flush_new_dw) {
            Assert(pos >= 0 && pos < DW_FIRST_DATA_PAGE_NUM + DW_SECOND_DATA_PAGE_NUM);
            g_instance.dw_single_cxt.single_flush_state[pos] = true;
        } else {
            Assert(pos >= 0 && pos < DW_SINGLE_DIRTY_PAGE_NUM);
            g_instance.dw_single_cxt.recovery_buf.single_flush_state[pos] = true;
        }
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

static int dw_fetch_file_id(int thread_id)
{
    int file_num = g_instance.attr.attr_storage.dw_file_num;
    return thread_id % file_num;
}

static void dw_fetch_thread_ids(int file_id, int &size, int *thread_ids)
{
    int thread_num = g_instance.attr.attr_storage.pagewriter_thread_num + 1;

    size = 0;
    for (int thread_id = 0; thread_id < thread_num; thread_id++) {
        if (dw_fetch_file_id(thread_id) == file_id) {
            thread_ids[size] = thread_id;
            size++;
        }
    }
}
