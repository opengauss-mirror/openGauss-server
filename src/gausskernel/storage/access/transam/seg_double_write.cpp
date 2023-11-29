/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
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
 *        src/gausskernel/storage/access/transam/seg_double_write.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include <unistd.h>
#include "utils/elog.h"
#include "access/double_write.h"
#include "utils/palloc.h"

#ifdef ENABLE_UT
#define static
#endif

static void dw_empty_buftag_page_old(uint16 org_start, uint16 max_idx)
{
    errno_t rc;
    uint16 batch = org_start / SINGLE_BLOCK_TAG_NUM;  /* calculate begin batch */
    uint16 batch_begin = org_start;
    uint32 start_offset;
    uint32 set_size;
    uint16 max_batch_num = DW_SINGLE_DIRTY_PAGE_NUM / SINGLE_BLOCK_TAG_NUM;
    knl_g_dw_context* dw_single_cxt = &g_instance.dw_single_cxt;
    MemoryContext old_mem_cxt = MemoryContextSwitchTo(dw_single_cxt->mem_cxt);
    char *unaligned_buf = (char *)palloc0(BLCKSZ + BLCKSZ); /* one more BLCKSZ for alignment */
    char *buf = (char *)TYPEALIGN(BLCKSZ, unaligned_buf);

    for (int i = org_start; i < max_idx - org_start; i++) {
        if (i >= SINGLE_BLOCK_TAG_NUM * (batch + 1)) {
            uint32 tag_offset = (batch + 1) * BLCKSZ; /* need skip file head */
            dw_pread_file(dw_single_cxt->fd, buf, BLCKSZ, tag_offset);
            start_offset = (batch_begin % SINGLE_BLOCK_TAG_NUM) * sizeof(dw_single_flush_item);
            set_size = (i - batch_begin) * sizeof(dw_single_flush_item);
            rc = memset_s(buf + start_offset, set_size, 0, set_size);
            securec_check(rc, "\0", "\0");
            dw_pwrite_file(dw_single_cxt->fd, buf, BLCKSZ, tag_offset, SINGLE_DW_FILE_NAME);
            batch++;
            batch_begin = i;
        }

        /* empty the last batch */
        if (batch >= max_batch_num - 1) {
            uint32 tag_offset = (batch + 1) * BLCKSZ; /* need skip file head */
            dw_pread_file(dw_single_cxt->fd, buf, BLCKSZ, tag_offset);
            start_offset = (batch_begin % SINGLE_BLOCK_TAG_NUM) * sizeof(dw_single_flush_item);
            set_size = (max_idx - batch_begin) * sizeof(dw_single_flush_item);
            rc = memset_s(buf + start_offset, set_size, 0, set_size);
            securec_check(rc, "\0", "\0");
            dw_pwrite_file(dw_single_cxt->fd, buf, BLCKSZ, tag_offset, SINGLE_DW_FILE_NAME);
            break;
        }
    }

    pfree(unaligned_buf);
    MemoryContextSwitchTo(old_mem_cxt);
    return;
}

static uint16 get_max_single_write_pos_old()
{
    uint16 max_idx = 0;
    uint16 i = 0;
    knl_g_dw_context* single_cxt = &g_instance.dw_single_cxt;
    dw_file_head_t *file_head = single_cxt->recovery_buf.file_head;

    for (i = file_head->start; i < single_cxt->recovery_buf.write_pos; i++) {
        if (single_cxt->recovery_buf.single_flush_state[i] == false) {
            break;
        }
    }
    max_idx = i;

    return max_idx;
}

void wait_all_single_dw_finish_flush_old()
{
    knl_g_dw_context* dw_single_cxt = &g_instance.dw_single_cxt;
    dw_file_head_t *file_head = dw_single_cxt->recovery_buf.file_head;

    for (int i = file_head->start; i < dw_single_cxt->recovery_buf.write_pos; ) {
        if (dw_single_cxt->recovery_buf.single_flush_state[i]) {
            i++;
            continue;
        } else {
            (void)sched_yield();
        }
    }
    return;
}

bool dw_single_file_recycle_old(bool trunc_file)
{
    bool file_full = false;
    uint16 max_idx = 0;
    knl_g_dw_context* single_cxt = &g_instance.dw_single_cxt;
    dw_file_head_t *file_head = single_cxt->recovery_buf.file_head;
    volatile uint16 org_start = file_head->start;
    volatile uint16 org_dwn = file_head->head.dwn;
    uint16 end = single_cxt->recovery_buf.write_pos;
    errno_t rc;

    Assert(g_instance.dw_single_cxt.dw_version == 0);
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
        max_idx = get_max_single_write_pos_old();
        if (max_idx == file_head->start) {
            LWLockRelease(single_cxt->second_flush_lock);
            return false;
        }
        LWLockRelease(single_cxt->second_flush_lock);
    } else {
        /* reset start position and flush page num for full recycle */
        wait_all_single_dw_finish_flush_old();
        file_head->start = 0;
        single_cxt->recovery_buf.write_pos = 0;
        rc = memset_s(single_cxt->recovery_buf.single_flush_state, sizeof(bool) * DW_SINGLE_DIRTY_PAGE_NUM,
            0, sizeof(bool) * DW_SINGLE_DIRTY_PAGE_NUM);
        securec_check(rc, "\0", "\0");
    }

    PageWriterSync();

    if (trunc_file) {
        if (!LWLockConditionalAcquire(single_cxt->second_flush_lock, LW_EXCLUSIVE)) {
            ereport(LOG, (errmodule(MOD_DW),
                errmsg("[single flush] can not get dw flush lock and skip dw truncate after sync for this time")));
            return false;
        } else if (pg_atomic_read_u32(&g_instance.dw_single_cxt.dw_version) != 0 ||
            org_start != file_head->start || org_dwn != file_head->head.dwn) {
            ereport(DW_LOG_LEVEL, (errmodule(MOD_DW),
                errmsg("[single flush] Skip dw truncate after sync due to concurrent dw truncate/reset, or dw upgrade "
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
        dw_empty_buftag_page_old(org_start, max_idx);
        file_head->start = max_idx;
        dw_prepare_file_head((char *)file_head, file_head->start, file_head->head.dwn);
    } else {
        dw_prepare_file_head((char *)file_head, file_head->start, file_head->head.dwn + 1);
    }

    Assert(file_head->head.dwn == file_head->tail.dwn);
    dw_pwrite_file(single_cxt->fd, file_head, BLCKSZ, 0, SINGLE_DW_FILE_NAME);

    ereport(LOG, (errmodule(MOD_DW), errmsg("Reset single flush DW file: file_head[dwn %hu, start %hu], "
        "writer pos is %hu", file_head->head.dwn, file_head->start, single_cxt->write_pos)));

    if (trunc_file) {
        (void)pg_atomic_add_fetch_u64(&single_cxt->single_stat_info.file_trunc_num, 1);
    } else {
        (void)pg_atomic_add_fetch_u64(&single_cxt->single_stat_info.file_reset_num, 1);
    }
    return true;
}

void dw_single_old_file_truncate()
{
    knl_g_dw_context *cxt = &g_instance.dw_single_cxt;

    /*
     * If we can grab dw flush lock, truncate dw file for faster recovery.
     *
     * Note: This is only for recovery optimization. we can not block on
     * dw flush lock, because, if we are checkpointer, pagewriter may be
     * waiting for us to finish smgrsync before it can do a full recycle of dw file.
     */
    if (!LWLockConditionalAcquire(cxt->second_flush_lock, LW_EXCLUSIVE)) {
        ereport(LOG, (errmodule(MOD_DW),
            errmsg("[single flush] Can not get dw flush lock and skip dw truncate for this time")));
        return;
    }

    /* hold the second version dw flush lock, check the dw version again */
    if (pg_atomic_read_u32(&g_instance.dw_single_cxt.dw_version) == 0) {
        if (dw_single_file_recycle_old(true)) {
            LWLockRelease(cxt->second_flush_lock);
        }
    } else {
        LWLockRelease(cxt->second_flush_lock);
    }
}

uint16 dw_single_flush_internal_old(BufferTag tag, Block block, XLogRecPtr page_lsn,
    BufferTag phy_tag, bool *dw_flush)
{
    errno_t rc;
    uint16 actual_pos;
    uint32 page_write_offset;
    uint32 tag_write_offset;
    uint16 block_offset;
    dw_single_flush_item item;
    knl_g_dw_context* dw_single_cxt = &g_instance.dw_single_cxt;
    dw_file_head_t *file_head = dw_single_cxt->recovery_buf.file_head;
    char *buf = t_thrd.proc->dw_buf;

    /* first step, copy buffer to dw buf, than flush page lsn, the buffer content lock  is already held */
    rc = memcpy_s(buf, BLCKSZ, block, BLCKSZ);
    securec_check(rc, "\0", "\0");

    XLogWaitFlush(page_lsn);

    (void)LWLockAcquire(dw_single_cxt->second_flush_lock, LW_EXCLUSIVE);
    /* recheck dw_version */
    if (pg_atomic_read_u32(&g_instance.dw_single_cxt.dw_version) == DW_SUPPORT_NEW_SINGLE_FLUSH) {
        LWLockRelease(dw_single_cxt->second_flush_lock);
        *dw_flush = false;
        return 0;
    }

    Assert(g_instance.dw_single_cxt.dw_version == 0);
    (void)dw_single_file_recycle_old(false);
    actual_pos = dw_single_cxt->recovery_buf.write_pos;
    dw_single_cxt->recovery_buf.write_pos = (dw_single_cxt->recovery_buf.write_pos + 1) % DW_SINGLE_DIRTY_PAGE_NUM;

    /* data page need skip head page and bufferTag page, bufferTag page need skip head page */
    page_write_offset = (actual_pos + DW_SINGLE_BUFTAG_PAGE_NUM + 1) * BLCKSZ;

    item.data_page_idx = actual_pos;
    item.dwn = file_head->head.dwn;
    item.buf_tag = phy_tag;
    dw_set_pg_checksum(buf, item.buf_tag.blockNum);

    /* Contents are protected with a CRC */
    INIT_CRC32C(item.crc);
    COMP_CRC32C(item.crc, (char*)&item, offsetof(dw_single_flush_item, crc));
    FIN_CRC32C(item.crc);

    dw_pwrite_file(dw_single_cxt->fd, buf, BLCKSZ, page_write_offset, SINGLE_DW_FILE_NAME);
    (void)pg_atomic_add_fetch_u64(&dw_single_cxt->single_stat_info.total_writes, 1);

    tag_write_offset = BLCKSZ + (actual_pos / SINGLE_BLOCK_TAG_NUM) * BLCKSZ;
    block_offset = (actual_pos % SINGLE_BLOCK_TAG_NUM) * sizeof(dw_single_flush_item);
    Assert(block_offset <= BLCKSZ - sizeof(dw_single_flush_item));

    dw_pread_file(dw_single_cxt->fd, buf, BLCKSZ, tag_write_offset);
    rc = memcpy_s(buf + block_offset, BLCKSZ - block_offset, &item, sizeof(dw_single_flush_item));
    securec_check(rc, "\0", "\0");
    dw_pwrite_file(dw_single_cxt->fd, buf, BLCKSZ, tag_write_offset, SINGLE_DW_FILE_NAME);
    LWLockRelease(dw_single_cxt->second_flush_lock);

    (void)pg_atomic_add_fetch_u64(&dw_single_cxt->single_stat_info.total_writes, 1);
    *dw_flush = true;
    return actual_pos;
}

/*
 * Current, this function is only used by segment-page, which does not support TDE. If we want to support TDE,
 * we could add additional parameter.
 */
uint16 seg_dw_single_flush_without_buffer(BufferTag tag, Block block, bool* flush_old_file)
{
    bool dw_flush = false;
    uint16 pos = 0;

    if (pg_atomic_read_u32(&g_instance.dw_single_cxt.dw_version) == 0) {
        pos = dw_single_flush_internal_old(tag, block, PageGetLSN(block), tag, &dw_flush);
        *flush_old_file = true;
    }

    if (!dw_flush) {
        *flush_old_file = false;
        pos = second_version_dw_single_flush(tag, block, PageGetLSN(block), false, tag);
    }
    return pos;
}

uint16 seg_dw_single_flush(BufferDesc *buf_desc, bool* flush_old_file)
{
    bool dw_flush = false;
    uint16 pos = 0;
    BufferTag phy_tag = buf_desc->tag;
    Block block = BufHdrGetBlock(buf_desc);

    uint64 buf_state = LockBufHdr(buf_desc);
    XLogRecPtr page_lsn = BufferGetLSN(buf_desc);
    UnlockBufHdr(buf_desc, buf_state);

    dw_transfer_phybuffer_addr(buf_desc, &phy_tag);

    if (pg_atomic_read_u32(&g_instance.dw_single_cxt.dw_version) == 0) {
        pos = dw_single_flush_internal_old(buf_desc->tag, block, page_lsn, phy_tag, &dw_flush);
        *flush_old_file = true;
    }

    if (!dw_flush) {
        *flush_old_file = false;
        pos = second_version_dw_single_flush(buf_desc->tag, block, page_lsn, buf_desc->extra->encrypt, phy_tag);
    }
    return pos;
}
