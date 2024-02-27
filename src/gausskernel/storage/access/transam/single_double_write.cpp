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
 * -------------------------------------------------------------------------
 *
 *  single_double_write.cpp
 *        Before flush dirty pages to data file, flush them to double write file,
 *        in case of half-flushed pages. Recover those half-flushed data file pages
 *        before replaying xlog when starting. This file is for single flush double
 *        write.
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/transam/single_double_write.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "access/double_write.h"
#include "storage/smgr/segment.h"
#include "utils/builtins.h"

const uint16 RESULT_LEN = 256;
Datum dw_get_single_flush_dwn()
{
    if (dw_enabled()) {
        char dwn[RESULT_LEN] = {0};
        errno_t rc;
        if (g_instance.dw_single_cxt.dw_version == DW_SUPPORT_NEW_SINGLE_FLUSH) {
            uint32 dwn_first = (uint32)g_instance.dw_single_cxt.file_head->head.dwn;
            uint32 dwn_second = (uint32)g_instance.dw_single_cxt.second_file_head->head.dwn;
            rc = snprintf_s(dwn, RESULT_LEN, RESULT_LEN - 1, "%u/%u", dwn_first, dwn_second);
            securec_check_ss(rc, "\0", "\0");
        } else {
            uint32 dwn_old = (uint32)g_instance.dw_single_cxt.recovery_buf.file_head->head.dwn;
            rc = snprintf_s(dwn, RESULT_LEN, RESULT_LEN - 1, "%u/0", dwn_old);
            securec_check_ss(rc, "\0", "\0");
        }
        return CStringGetTextDatum(dwn);
    }
    return CStringGetTextDatum("0/0");
}

Datum dw_get_single_flush_start()
{
    if (dw_enabled()) {
        char start[RESULT_LEN] = {0};
        errno_t rc;
        if (g_instance.dw_single_cxt.dw_version == DW_SUPPORT_NEW_SINGLE_FLUSH) {
            uint32 start_first = (uint32)g_instance.dw_single_cxt.file_head->start;
            uint32 start_second = (uint32)g_instance.dw_single_cxt.second_file_head->start;
            rc = snprintf_s(start, RESULT_LEN, RESULT_LEN - 1, "%u/%u", start_first, start_second);
            securec_check_ss(rc, "\0", "\0");
        } else {
            uint32 start_old = (uint32)g_instance.dw_single_cxt.recovery_buf.file_head->start;
            rc = snprintf_s(start, RESULT_LEN, RESULT_LEN - 1, "%u/0", start_old);
            securec_check_ss(rc, "\0", "\0");
        }
        return CStringGetTextDatum(start);
    }

    return CStringGetTextDatum("0/0");
}

Datum dw_get_single_flush_trunc_num()
{
    char trunc_num[RESULT_LEN] = {0};
    errno_t rc;
    if (g_instance.dw_single_cxt.dw_version == DW_SUPPORT_NEW_SINGLE_FLUSH) {
        uint32 trunc_num_first = (uint32)g_instance.dw_single_cxt.single_stat_info.file_trunc_num;
        uint32 trunc_num_second = (uint32)g_instance.dw_single_cxt.single_stat_info.second_file_trunc_num;
        rc = snprintf_s(trunc_num, RESULT_LEN, RESULT_LEN - 1, "%u/%u", trunc_num_first, trunc_num_second);
        securec_check_ss(rc, "\0", "\0");
    } else {
        uint32 trunc_num_old = (uint32)g_instance.dw_single_cxt.single_stat_info.file_trunc_num;
        rc = snprintf_s(trunc_num, RESULT_LEN, RESULT_LEN - 1, "%u/0", trunc_num_old);
        securec_check_ss(rc, "\0", "\0");
    }
    return CStringGetTextDatum(trunc_num);
}

Datum dw_get_single_flush_reset_num()
{
    char reset_num[RESULT_LEN] = {0};
    errno_t rc;
    if (g_instance.dw_single_cxt.dw_version == DW_SUPPORT_NEW_SINGLE_FLUSH) {
        uint32 reset_num_first = (uint32)g_instance.dw_single_cxt.single_stat_info.file_reset_num;
        uint32 reset_num_second = (uint32)g_instance.dw_single_cxt.single_stat_info.second_file_reset_num;
        rc = snprintf_s(reset_num, RESULT_LEN, RESULT_LEN - 1, "%u/%u", reset_num_first, reset_num_second);
        securec_check_ss(rc, "\0", "\0");
    } else {
        uint32 reset_num_old = (uint32)g_instance.dw_single_cxt.single_stat_info.file_reset_num;
        rc = snprintf_s(reset_num, RESULT_LEN, RESULT_LEN - 1, "%u/0", reset_num_old);
        securec_check_ss(rc, "\0", "\0");
    }
    return CStringGetTextDatum(reset_num);
}

Datum dw_get_single_flush_total_writes()
{
    char total_writes[RESULT_LEN] = {0};
    errno_t rc;
    if (g_instance.dw_single_cxt.dw_version == DW_SUPPORT_NEW_SINGLE_FLUSH) {
        uint32 total_writes_first = (uint32)g_instance.dw_single_cxt.single_stat_info.total_writes;
        uint32 total_writes_second = (uint32)g_instance.dw_single_cxt.single_stat_info.second_total_writes;
        rc = snprintf_s(total_writes, RESULT_LEN, RESULT_LEN - 1, "%u/%u", total_writes_first, total_writes_second);
        securec_check_ss(rc, "\0", "\0");
    } else {
        uint32 total_writes_old = (uint32)g_instance.dw_single_cxt.single_stat_info.total_writes;
        rc = snprintf_s(total_writes, RESULT_LEN, RESULT_LEN - 1, "%u/0", total_writes_old);
        securec_check_ss(rc, "\0", "\0");
    }
    return CStringGetTextDatum(total_writes);
}

const dw_view_col_t g_dw_single_view[DW_SINGLE_VIEW_COL_NUM] = {
    {"node_name", TEXTOID, dw_get_node_name},
    {"curr_dwn", TEXTOID, dw_get_single_flush_dwn},
    {"curr_start_page", TEXTOID, dw_get_single_flush_start},
    {"total_writes", TEXTOID, dw_get_single_flush_total_writes},
    {"file_trunc_num", TEXTOID, dw_get_single_flush_trunc_num},
    {"file_reset_num", TEXTOID, dw_get_single_flush_reset_num}
};

static bool dw_verify_item(const dw_single_flush_item* item, uint16 dwn);
static uint16 get_max_single_write_pos(bool is_first);
static uint16 atomic_get_dw_write_pos(bool is_first);

static void dw_recovery_first_version_page();
static void dw_recovery_old_single_dw_page();
static void dw_recovery_second_version_page();
static void dw_recovery_single_page(const dw_single_flush_item *item, uint16 item_num);

void dw_generate_single_file()
{
    char *file_head = NULL;
    int64 remain_size;
    int fd;
    errno_t rc;
    char *unaligned_buf = NULL;

    if (file_exists(SINGLE_DW_FILE_NAME)) {
        ereport(PANIC, (errcode_for_file_access(), errmodule(MOD_DW), errmsg("DW single flush file already exists")));
    }

    ereport(LOG, (errmodule(MOD_DW), errmsg("DW bootstrap single flush file")));

    fd = dw_create_file(SINGLE_DW_FILE_NAME);

    unaligned_buf = (char *)palloc0(DW_FILE_EXTEND_SIZE + BLCKSZ); /* one more BLCKSZ for alignment */

    file_head = (char *)TYPEALIGN(BLCKSZ, unaligned_buf);

    /* file head and first batch head will be writen */
    remain_size = (DW_SINGLE_DIRTY_PAGE_NUM + DW_SINGLE_BUFTAG_PAGE_NUM) * BLCKSZ;
    dw_prepare_file_head(file_head, 0, 0, 0);
    dw_pwrite_file(fd, file_head, BLCKSZ, 0, SINGLE_DW_FILE_NAME);
    rc = memset_s(file_head, BLCKSZ, 0, BLCKSZ);
    securec_check(rc, "\0", "\0");
    dw_extend_file(fd, file_head, DW_FILE_EXTEND_SIZE, remain_size, DW_SINGLE_FILE_SIZE, true, NULL);
    ereport(LOG, (errmodule(MOD_DW), errmsg("Double write single flush file created successfully")));

    (void)close(fd);
    fd = -1;
    pfree(unaligned_buf);
    return;
}

static bool dw_is_pca_need_recover_single(BufferTag *lst_buf_tag, BufferTag *buf_tag,
                                          SMgrRelation relnode, bool *isFirst)
{
    /* in uncompressed table:
     * opt will be 0xff if the previous version does not support compresssion
     * opt will be 0 if the previous version supports compresssion
     */
    if (relnode->smgr_rnode.node.opt == 0xffff || relnode->smgr_rnode.node.opt == 0 ||
        buf_tag->forkNum != MAIN_FORKNUM) {
        return false;
    }

    /*  in segment_extstore, page will not be compressed if it belong to ext which size is 8 pages */
    if (IsSegmentPhysicalRelNode(relnode->smgr_rnode.node) && buf_tag->blockNum < DW_EXT_LOGIC_PAGE_NUM) {
        return false;
    }

    /* first buffer */
    if (isFirst) {
        *isFirst = false;
        *lst_buf_tag = *buf_tag;
        return true;
    }

    /* different rnode or different fork,  of coures different pca */
    errno_t rc = memcmp(&(lst_buf_tag->rnode), &(buf_tag->rnode), offsetof(BufferTag, blockNum));
    if (rc != 0) {
        *lst_buf_tag = *buf_tag;
        return true;
    }

    /* different ext means different pca */
    BlockNumber ext_num = buf_tag->blockNum / DW_EXT_LOGIC_PAGE_NUM;
    if (ext_num != lst_buf_tag->blockNum / DW_EXT_LOGIC_PAGE_NUM) {
        *lst_buf_tag = *buf_tag;
        return true;
    }

    return false;
}

static void dw_recovery_first_version_page()
{
    knl_g_dw_context* single_cxt = &g_instance.dw_single_cxt;
    dw_file_head_t *file_head = single_cxt->file_head;
    errno_t rc = 0;
    uint64 offset = 0;
    PageHeader pghr = NULL;
    SMgrRelation reln = NULL;
    char *unaligned_buf = (char *)palloc0(BLCKSZ + BLCKSZ); /* one more BLCKSZ for alignment */
    char *dw_block = (char *)TYPEALIGN(BLCKSZ, unaligned_buf);
    char *data_block = (char *)palloc0(BLCKSZ);
    dw_first_flush_item flush_item;
    bool needPcaCheck = false;
    bool isFisrt = true;
    BufferTag preBufferTag;

    for (uint16 i = file_head->start; i < DW_FIRST_DATA_PAGE_NUM; i++) {
        offset = (i + 1) * BLCKSZ;  /* need skip the file head */
        dw_pread_file(single_cxt->fd, dw_block, BLCKSZ, offset);
        pghr = (PageHeader)dw_block;

        rc = memcpy_s(&flush_item, sizeof(dw_first_flush_item), dw_block + pghr->pd_lower, sizeof(dw_first_flush_item));
        securec_check(rc, "\0", "\0");
        if (unlikely((long)(t_thrd.proc->workingVersionNum < PAGE_COMPRESSION_VERSION))) {
            BufferTagSecondVer *old_buf_tag = (BufferTagSecondVer *)(void *)&flush_item.buf_tag;
            flush_item.buf_tag.rnode.bucketNode = (int2)old_buf_tag->rnode.bucketNode;
            flush_item.buf_tag.rnode.opt = 0;
        }

        if (!dw_verify_pg_checksum((PageHeader)dw_block, flush_item.buf_tag.blockNum, true)) {
            if (PageIsNew(dw_block)) {
                Assert(flush_item.buf_tag.rnode.relNode == 0);
                dw_log_recovery_page(LOG, "[first version] dw page is new, break this recovery", flush_item.buf_tag);
                break;
            }
            dw_log_recovery_page(WARNING, "DW single page broken", flush_item.buf_tag);
            dw_log_page_header((PageHeader)dw_block);
            continue;
        }
        dw_log_recovery_page(DW_LOG_LEVEL, "DW page fine", flush_item.buf_tag);

        reln = smgropen(flush_item.buf_tag.rnode, InvalidBackendId, GetColumnNum(flush_item.buf_tag.forkNum));
        /* read data page */
        if (!dw_read_data_page(flush_item.buf_tag, reln, data_block)) {
            continue;
        }
        dw_log_page_header((PageHeader)data_block);
        if (!dw_verify_pg_checksum((PageHeader)data_block, flush_item.buf_tag.blockNum, false) ||
            XLByteLT(PageGetLSN(data_block), PageGetLSN(dw_block))) {
            memset_s(dw_block + pghr->pd_lower, sizeof(dw_first_flush_item), 0, sizeof(dw_first_flush_item));
            securec_check(rc, "\0", "\0");
            dw_set_pg_checksum(dw_block, flush_item.buf_tag.blockNum);

            needPcaCheck = dw_is_pca_need_recover_single(&preBufferTag, &(flush_item.buf_tag), reln, &isFisrt);
            if (IsSegmentPhysicalRelNode(flush_item.buf_tag.rnode)) {
                // seg_space must be initialized before.
                seg_physical_write(reln->seg_space, flush_item.buf_tag.rnode, flush_item.buf_tag.forkNum,
                                   flush_item.buf_tag.blockNum, (const char *)dw_block, false);
            } else {
                SmgrRecoveryPca(reln, flush_item.buf_tag.forkNum, flush_item.buf_tag.blockNum, needPcaCheck, false);
                smgrwrite(reln, flush_item.buf_tag.forkNum, flush_item.buf_tag.blockNum,
                    (const char *)dw_block, false);
            }
            dw_log_recovery_page(LOG, "Date page recovered", flush_item.buf_tag);
            dw_log_page_header((PageHeader)data_block);
        }
    }

    pfree(unaligned_buf);
    pfree(data_block);
}

static bool dw_verify_item(const dw_single_flush_item* item, uint16 dwn)
{
    if (item->dwn != dwn) {
        return false;
    }

    if (item->buf_tag.forkNum == InvalidForkNumber || item->buf_tag.blockNum == InvalidBlockNumber ||
        item->buf_tag.rnode.relNode == InvalidOid) {
        if (item->dwn != 0 || item->data_page_idx != 0) {
            ereport(WARNING,
                (errmsg("dw recovery, find invalid item [page_idx %hu dwn %hu] skip this item,"
                "buf_tag[rel %u/%u/%u blk %u fork %d]", item->data_page_idx, item->dwn,
                item->buf_tag.rnode.spcNode, item->buf_tag.rnode.dbNode, item->buf_tag.rnode.relNode,
                item->buf_tag.blockNum, item->buf_tag.forkNum)));
        }
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

static void dw_recovery_single_page(const dw_single_flush_item *item, uint16 item_num)
{
    SMgrRelation reln;
    uint32 offset = 0;
    BufferTag buf_tag;
    knl_g_dw_context* single_cxt = &g_instance.dw_single_cxt;
    char *unaligned_buf = (char *)palloc0(BLCKSZ + BLCKSZ); /* one more BLCKSZ for alignment */
    char *dw_block = (char *)TYPEALIGN(BLCKSZ, unaligned_buf);
    char *data_block = (char *)palloc0(BLCKSZ);
    uint64 base_offset = 0;
    bool needPcaCheck = false;
    bool isFisrt = true;
    BufferTag preBufferTag;

    if (single_cxt->file_head->dw_version == DW_SUPPORT_NEW_SINGLE_FLUSH) {
        base_offset = 1 + DW_FIRST_DATA_PAGE_NUM + 1 + DW_SECOND_BUFTAG_PAGE_NUM;
    } else {
        base_offset = DW_SINGLE_BUFTAG_PAGE_NUM + 1;
    }

    for (uint16 i = 0; i < item_num; i++) {
        buf_tag = item[i].buf_tag;

        /* read dw file page */
        offset = (base_offset + item[i].data_page_idx) * BLCKSZ;
        dw_pread_file(single_cxt->fd, dw_block, BLCKSZ, offset);

        if (!dw_verify_pg_checksum((PageHeader)dw_block, buf_tag.blockNum, true)) {
            dw_log_recovery_page(WARNING, "DW single page broken", buf_tag);
            dw_log_page_header((PageHeader)dw_block);
            continue;
        }
        dw_log_recovery_page(DW_LOG_LEVEL, "DW page fine", buf_tag);

        reln = smgropen(buf_tag.rnode, InvalidBackendId, GetColumnNum(buf_tag.forkNum));
        /* read data page */
        if (!dw_read_data_page(buf_tag, reln, data_block)) {
            continue;
        }
        dw_log_page_header((PageHeader)data_block);
        if (!dw_verify_pg_checksum((PageHeader)data_block, buf_tag.blockNum, false) ||
            XLByteLT(PageGetLSN(data_block), PageGetLSN(dw_block))) {
            needPcaCheck = dw_is_pca_need_recover_single(&preBufferTag, &buf_tag, reln, &isFisrt);
            if (IsSegmentPhysicalRelNode(buf_tag.rnode)) {
                // seg_space must be initialized before.
                seg_physical_write(reln->seg_space, buf_tag.rnode, buf_tag.forkNum, buf_tag.blockNum,
                                   (const char *)dw_block, false);
            } else {
                SmgrRecoveryPca(reln, buf_tag.forkNum, buf_tag.blockNum, needPcaCheck, false);
                smgrwrite(reln, buf_tag.forkNum, buf_tag.blockNum, (const char *)dw_block, false);
            }
            dw_log_recovery_page(LOG, "Date page recovered", buf_tag);
            dw_log_page_header((PageHeader)data_block);
        }
    }

    pfree(unaligned_buf);
    pfree(data_block);
    return;
}

static void dw_recovery_second_version_page()
{
    knl_g_dw_context* single_cxt = &g_instance.dw_single_cxt;
    dw_single_flush_item *item = NULL;
    uint16 rec_num = 0;
    dw_file_head_t *file_head = single_cxt->second_file_head;
    char *buf = single_cxt->buf;
    uint64 second_offset = DW_SECOND_BUFTAG_START_IDX * BLCKSZ;

    item = (dw_single_flush_item*)palloc0(sizeof(dw_single_flush_item) * DW_SECOND_DATA_PAGE_NUM);

    /* read all buffer tag item, need skip head page */
    dw_pread_file(single_cxt->fd, buf, DW_SECOND_BUFTAG_PAGE_NUM * BLCKSZ, second_offset);

    uint64 offset = 0;
    dw_single_flush_item *temp = NULL;
    for (uint16 i = single_cxt->second_file_head->start; i < DW_SECOND_DATA_PAGE_NUM; i++) {
        offset = i * sizeof(dw_single_flush_item);
        temp = (dw_single_flush_item*)((char*)buf + offset);
        if (dw_verify_item(temp, file_head->head.dwn)) {
            item[rec_num].data_page_idx = temp->data_page_idx;
            item[rec_num].dwn = temp->dwn;
            item[rec_num].buf_tag = temp->buf_tag;
            item[rec_num].crc = temp->crc;
            rec_num++;
        }
    }
    ereport(LOG, (errmodule(MOD_DW), errmsg("[second version] DW single flush file valid item num is %d.", rec_num)));
    if (rec_num > 0) {
        qsort(item, rec_num, sizeof(dw_single_flush_item), buftag_compare);
        ereport(LOG, (errmodule(MOD_DW),
            errmsg("[second version] DW single flush file valid buftag item qsort finish.")));
        dw_recovery_single_page(item, rec_num);
    }

    pfree(item);
}

static void dw_recovery_old_single_dw_page()
{
    knl_g_dw_context* single_cxt = &g_instance.dw_single_cxt;
    dw_single_flush_item *item = NULL;
    dw_file_head_t *file_head = single_cxt->recovery_buf.file_head;
    uint16 blk_num = DW_SINGLE_DIRTY_PAGE_NUM / SINGLE_BLOCK_TAG_NUM;
    char *buf = single_cxt->recovery_buf.buf;
    uint16 rec_num = 0;

    item = (dw_single_flush_item*)palloc0(sizeof(dw_single_flush_item) * DW_SINGLE_DIRTY_PAGE_NUM);

    /* read all buffer tag item, need skip head page */
    dw_pread_file(single_cxt->fd, buf, blk_num * BLCKSZ, BLCKSZ);
    int offset = 0;
    dw_single_flush_item *temp = NULL;
    for (int i = 0; i < DW_SINGLE_DIRTY_PAGE_NUM; i++) {
        offset = i * sizeof(dw_single_flush_item);
        temp = (dw_single_flush_item*)((char*)buf + offset);
        if (dw_verify_item(temp, file_head->head.dwn)) {
            item[rec_num].data_page_idx = temp->data_page_idx;
            item[rec_num].buf_tag = temp->buf_tag;
            item[rec_num].dwn = temp->dwn;
            item[rec_num].crc = temp->crc;
            rec_num++;
        }
    }

    ereport(LOG, (errmodule(MOD_DW), errmsg("[old version] DW single flush file valid item num is %d.", rec_num)));
    if (rec_num > 0) {
        qsort(item, rec_num, sizeof(dw_single_flush_item), buftag_compare);
        ereport(LOG, (errmodule(MOD_DW), errmsg("DW single flush file valid buftag item qsort finish.")));
        dw_recovery_single_page(item, rec_num);
    }

    pfree(item);
}

void dw_force_reset_single_file(uint32 dw_version)
{
    knl_g_dw_context* single_cxt = &g_instance.dw_single_cxt;
    dw_file_head_t *file_head = single_cxt->file_head;

    if (USE_CKPT_THREAD_SYNC) {
        ProcessSyncRequests();
    } else {
        PageWriterSync();
    }

    dw_prepare_file_head((char *)file_head, 0, file_head->head.dwn + 1);
    dw_pwrite_file(single_cxt->fd, file_head, BLCKSZ, 0, SINGLE_DW_FILE_NAME);
    (void)pg_atomic_add_fetch_u64(&single_cxt->single_stat_info.file_reset_num, 1);

    ereport(LOG, (errmodule(MOD_DW),
            errmsg("DW single flush finish recovery, reset the file head[dwn %hu, start %hu].",
                file_head->head.dwn, file_head->start)));

    if (dw_version == DW_SUPPORT_NEW_SINGLE_FLUSH) {
        file_head = single_cxt->second_file_head;
        dw_prepare_file_head((char *)file_head, 0, file_head->head.dwn + 1);
        dw_pwrite_file(single_cxt->fd, file_head, BLCKSZ, (1 + DW_FIRST_DATA_PAGE_NUM) * BLCKSZ, SINGLE_DW_FILE_NAME);
        (void)pg_atomic_add_fetch_u64(&single_cxt->single_stat_info.second_file_reset_num, 1);
        ereport(LOG, (errmodule(MOD_DW),
            errmsg("DW single flush finish recovery [second version], reset the file head[dwn %hu, start %hu].",
                file_head->head.dwn, file_head->start)));
    }

    return;
}

void dw_recovery_partial_write_single()
{
    if (SS_REFORM_REFORMER) {
        return;
    }
    knl_g_dw_context* single_cxt = &g_instance.dw_single_cxt;

    ereport(LOG, (errmodule(MOD_DW), errmsg("DW single flush file recovery start.")));
    if (single_cxt->file_head->dw_version == DW_SUPPORT_NEW_SINGLE_FLUSH) {
        dw_recovery_first_version_page();
        dw_recovery_second_version_page();
    } else {
        dw_recovery_old_single_dw_page();
    }

    ereport(LOG, (errmodule(MOD_DW), errmsg("DW single flush file recovery finish.")));

    /* reset the file after the recovery is complete */
    dw_force_reset_single_file(single_cxt->file_head->dw_version);
    return;
}

void dw_single_file_truncate(bool is_first)
{
    uint16 max_idx = 0;
    knl_g_dw_context* single_cxt = &g_instance.dw_single_cxt;
    dw_file_head_t *file_head = NULL;
    volatile uint16 org_start = 0;
    volatile uint16 org_dwn = 0;
    LWLock* flush_lock = NULL;
    uint64 head_offset = 0;

    if (is_first) {
        file_head = single_cxt->file_head;
        flush_lock = single_cxt->flush_lock;
        head_offset = 0;
    } else {
        file_head = single_cxt->second_file_head;
        flush_lock = single_cxt->second_flush_lock;
        head_offset = (1 + DW_FIRST_DATA_PAGE_NUM) * BLCKSZ;
    }

    if (!LWLockConditionalAcquire(flush_lock, LW_EXCLUSIVE)) {
        ereport(LOG, (errmodule(MOD_DW),
            errmsg("[single flush] can not get dw flush lock and skip dw truncate for this time")));
        return;
    }

    org_start = file_head->start;
    org_dwn = file_head->head.dwn;
    max_idx = get_max_single_write_pos(is_first);
    if (max_idx == file_head->start) {
        LWLockRelease(flush_lock);
        return;
    }
    LWLockRelease(flush_lock);

    PageWriterSync();

    if (!LWLockConditionalAcquire(flush_lock, LW_EXCLUSIVE)) {
        ereport(LOG, (errmodule(MOD_DW),
            errmsg("[single flush] can not get dw flush lock and skip dw truncate after sync for this time")));
        return;
    } else if (org_start != file_head->start || org_dwn != file_head->head.dwn) {
        LWLockRelease(flush_lock);
        return;
    }

    file_head->start = max_idx;
    dw_prepare_file_head((char *)file_head, file_head->start, file_head->head.dwn);

    Assert(file_head->head.dwn == file_head->tail.dwn);
    dw_pwrite_file(single_cxt->fd, file_head, BLCKSZ, head_offset, SINGLE_DW_FILE_NAME);
    LWLockRelease(flush_lock);

    ereport(LOG, (errmodule(MOD_DW),
        errmsg("[single flush][%s] DW truncate end: file_head[dwn %hu, start %hu], write_pos %hu",
            is_first ? "first version" : "second_version",
            file_head->head.dwn, file_head->start, is_first ? single_cxt->write_pos : single_cxt->second_write_pos)));

    if (is_first) {
        (void)pg_atomic_add_fetch_u64(&single_cxt->single_stat_info.file_trunc_num, 1);
    } else {
        (void)pg_atomic_add_fetch_u64(&single_cxt->single_stat_info.second_file_trunc_num, 1);
    }
    return;
}

static uint16 get_max_single_write_pos(bool is_first)
{
    uint16 max_idx = 0;
    uint16 i = 0;
    uint16 start = 0;
    knl_g_dw_context* dw_single_cxt = &g_instance.dw_single_cxt;
    uint16 end = 0;
    dw_file_head_t *file_head = NULL;

    /* single_flush_state, first */
    if (!is_first) {
        file_head = dw_single_cxt->second_file_head;
        start = file_head->start + DW_FIRST_DATA_PAGE_NUM;
        end = pg_atomic_read_u32(&dw_single_cxt->second_write_pos) + DW_FIRST_DATA_PAGE_NUM;
    } else {
        file_head = dw_single_cxt->file_head;
        start = file_head->start;
        end = pg_atomic_read_u32(&dw_single_cxt->write_pos);
    }

    for (i = start; i < end; i++) {
        if (dw_single_cxt->single_flush_state[i] == false) {
            break;
        }
    }
    max_idx = i;

    if (!is_first) {
        max_idx = max_idx - DW_FIRST_DATA_PAGE_NUM;
    }

    return max_idx;
}

void dw_generate_new_single_file()
{
    char *file_head = NULL;
    int64 extend_size;
    int fd;
    errno_t rc;
    char *unaligned_buf = NULL;

    if (file_exists(SINGLE_DW_FILE_NAME)) {
        ereport(PANIC, (errcode_for_file_access(), errmodule(MOD_DW),
            errmsg("DW single flush file already exists")));
    }

    ereport(LOG, (errmodule(MOD_DW), errmsg("DW bootstrap new single flush file")));

    fd = dw_create_file(SINGLE_DW_FILE_NAME);

    /* NO EREPORT(ERROR) from here till changes are logged */
    START_CRIT_SECTION();
    unaligned_buf = (char *)palloc0(DW_FILE_EXTEND_SIZE + BLCKSZ); /* one more BLCKSZ for alignment */

    file_head = (char *)TYPEALIGN(BLCKSZ, unaligned_buf);

    /* first version page int */
    extend_size = DW_FIRST_DATA_PAGE_NUM * BLCKSZ;
    dw_prepare_file_head(file_head, 0, 0, DW_SUPPORT_NEW_SINGLE_FLUSH);
    dw_pwrite_file(fd, file_head, BLCKSZ, 0, SINGLE_DW_FILE_NAME);

    rc = memset_s(file_head, BLCKSZ, 0, BLCKSZ);
    securec_check(rc, "\0", "\0");
    dw_extend_file(fd, file_head, DW_FILE_EXTEND_SIZE, extend_size, DW_NEW_SINGLE_FILE_SIZE, true, NULL);

    /* second version page init */
    extend_size = (DW_SECOND_BUFTAG_PAGE_NUM + DW_SECOND_DATA_PAGE_NUM) * BLCKSZ;
    dw_prepare_file_head(file_head, 0, 0, DW_SUPPORT_NEW_SINGLE_FLUSH);
    dw_pwrite_file(fd, file_head, BLCKSZ, (1 + DW_FIRST_DATA_PAGE_NUM) * BLCKSZ, SINGLE_DW_FILE_NAME);

    rc = memset_s(file_head, BLCKSZ, 0, BLCKSZ);
    securec_check(rc, "\0", "\0");
    dw_extend_file(fd, file_head, DW_FILE_EXTEND_SIZE, extend_size, DW_NEW_SINGLE_FILE_SIZE, true, NULL);
    END_CRIT_SECTION();

    ereport(LOG, (errmodule(MOD_DW), errmsg("Double write single flush file created successfully")));

    (void)close(fd);
    fd = -1;
    pfree(unaligned_buf);
    return;
}

uint16 first_version_dw_single_flush(BufferDesc *buf_desc)
{
    errno_t rc;
    char *buf = t_thrd.proc->dw_buf;
    knl_g_dw_context* dw_single_cxt = &g_instance.dw_single_cxt;
    dw_file_head_t *file_head = dw_single_cxt->file_head;
    uint16 actual_pos;
    uint64 page_write_offset;
    dw_first_flush_item item;
    PageHeader pghr = NULL;
    BufferTag phy_tag;

    /* used to block the io for snapshot feature */
    (void)LWLockAcquire(g_instance.ckpt_cxt_ctl->snapshotBlockLock, LW_SHARED);
    LWLockRelease(g_instance.ckpt_cxt_ctl->snapshotBlockLock);

    uint64 buf_state = LockBufHdr(buf_desc);
    Block block = BufHdrGetBlock(buf_desc);
    XLogRecPtr page_lsn = BufferGetLSN(buf_desc);
    UnlockBufHdr(buf_desc, buf_state);

    phy_tag = buf_desc->tag;
    dw_transfer_phybuffer_addr(buf_desc, &phy_tag);

    Assert(buf_desc->buf_id < SegmentBufferStartID);
    Assert(free_space_enough(buf_desc->buf_id));

    /* first step, copy buffer to dw buf, than flush page lsn, the buffer content lock  is already held */
    rc = memcpy_s(buf, BLCKSZ, block, BLCKSZ);
    securec_check(rc, "\0", "\0");

    XLogWaitFlush(page_lsn);
    if (buf_desc->extra->encrypt) {
        dw_encrypt_page(buf_desc->tag, buf);
    }

    actual_pos = atomic_get_dw_write_pos(true);

    item.dwn = file_head->head.dwn;
    item.buf_tag = phy_tag;
    if (unlikely((long)(t_thrd.proc->workingVersionNum < PAGE_COMPRESSION_VERSION))) {
        BufferTagSecondVer *old_buf_tag = (BufferTagSecondVer *)(void *)&item.buf_tag;
        old_buf_tag->rnode.bucketNode = phy_tag.rnode.bucketNode;
    }

    pghr = (PageHeader)buf;

    rc = memcpy_s(buf + pghr->pd_lower, sizeof(dw_first_flush_item), &item, sizeof(dw_first_flush_item));
    securec_check(rc, "\0", "\0");

    dw_set_pg_checksum(buf, item.buf_tag.blockNum);
    page_write_offset = (1 + actual_pos) * BLCKSZ;
    Assert(actual_pos < DW_FIRST_DATA_PAGE_NUM);
    dw_pwrite_file(dw_single_cxt->fd, buf, BLCKSZ, page_write_offset, SINGLE_DW_FILE_NAME);

    (void)pg_atomic_add_fetch_u64(&dw_single_cxt->single_stat_info.total_writes, 1);

    return actual_pos;
}

uint16 second_version_dw_single_flush(BufferTag tag, Block block, XLogRecPtr page_lsn,
    bool encrypt, BufferTag phy_tag)
{
    errno_t rc;
    uint16 actual_pos;
    uint64 page_write_offset;
    uint64 tag_write_offset;
    uint16 block_offset;
    dw_single_flush_item item;
    knl_g_dw_context* dw_single_cxt = &g_instance.dw_single_cxt;
    dw_file_head_t *file_head = dw_single_cxt->second_file_head;
    char *buf = t_thrd.proc->dw_buf;

    /* used to block the io for snapshot feature */
    (void)LWLockAcquire(g_instance.ckpt_cxt_ctl->snapshotBlockLock, LW_SHARED);
    LWLockRelease(g_instance.ckpt_cxt_ctl->snapshotBlockLock);

    /* first step, copy buffer to dw buf, than flush page lsn, the buffer content lock  is already held */
    rc = memcpy_s(buf, BLCKSZ, block, BLCKSZ);
    securec_check(rc, "\0", "\0");

    XLogWaitFlush(page_lsn);
    if (encrypt) {
        dw_encrypt_page(tag, buf);
    }
    dw_set_pg_checksum(buf, phy_tag.blockNum);

    actual_pos = atomic_get_dw_write_pos(false);

    /* data page need skip head page and bufferTag page, bufferTag page need skip head page and first version page */
    page_write_offset = (actual_pos + DW_SECOND_DATA_START_IDX) * BLCKSZ;
    tag_write_offset = DW_SECOND_BUFTAG_START_IDX * BLCKSZ  + (actual_pos / SINGLE_BLOCK_TAG_NUM) * BLCKSZ;
    block_offset = (actual_pos % SINGLE_BLOCK_TAG_NUM) * sizeof(dw_single_flush_item);
    Assert(block_offset <= BLCKSZ - sizeof(dw_single_flush_item));
    Assert(actual_pos < DW_SECOND_DATA_PAGE_NUM);
    Assert(page_write_offset < DW_NEW_SINGLE_FILE_SIZE && tag_write_offset < DW_SECOND_DATA_START_IDX * BLCKSZ);

    /* write the data page to dw file */
    dw_pwrite_file(dw_single_cxt->fd, buf, BLCKSZ, page_write_offset, SINGLE_DW_FILE_NAME);

    item.data_page_idx = actual_pos;
    item.dwn = file_head->head.dwn;
    item.buf_tag = phy_tag;

    /* Contents are protected with a CRC */
    INIT_CRC32C(item.crc);
    COMP_CRC32C(item.crc, (char*)&item, offsetof(dw_single_flush_item, crc));
    FIN_CRC32C(item.crc);

    /* write the buffer tag item to dw file */
    (void)LWLockAcquire(dw_single_cxt->second_buftag_lock, LW_EXCLUSIVE);
    dw_pread_file(dw_single_cxt->fd, buf, BLCKSZ, tag_write_offset);
    rc = memcpy_s(buf + block_offset, BLCKSZ - block_offset, &item, sizeof(dw_single_flush_item));
    securec_check(rc, "\0", "\0");
    dw_pwrite_file(dw_single_cxt->fd, buf, BLCKSZ, tag_write_offset, SINGLE_DW_FILE_NAME);

    LWLockRelease(dw_single_cxt->second_buftag_lock);
    (void)pg_atomic_add_fetch_u64(&dw_single_cxt->single_stat_info.second_total_writes, 1);

    return (actual_pos + DW_FIRST_DATA_PAGE_NUM);
}

void wait_all_single_dw_finish_flush(bool is_first)
{
    uint16 start = 0;
    uint16 end = 0;
    dw_file_head_t *file_head = NULL;
    knl_g_dw_context* dw_single_cxt = &g_instance.dw_single_cxt;

    /* single_flush_state, first */
    if (is_first) {
        file_head = dw_single_cxt->file_head;
        start = file_head->start;
        end = pg_atomic_read_u32(&dw_single_cxt->write_pos);
    } else {
        file_head = dw_single_cxt->second_file_head;
        start = file_head->start + DW_FIRST_DATA_PAGE_NUM;
        end = pg_atomic_read_u32(&dw_single_cxt->second_write_pos) + DW_FIRST_DATA_PAGE_NUM;
    }

    for (uint i = start; i < end;) {
        if (dw_single_cxt->single_flush_state[i] != false) {
            i++;
            continue;
        } else {
            (void)sched_yield();
        }
    }
    return;
}

void dw_single_file_recycle(bool is_first)
{
    bool file_full = false;
    knl_g_dw_context* single_cxt = &g_instance.dw_single_cxt;
    dw_file_head_t *file_head = NULL;
    uint16 end = 0;
    errno_t rc;
    uint64 head_offset = 0;
    uint16 flush_state_start = 0;
    uint16 page_num = 0;

    if (is_first) {
        file_head = single_cxt->file_head;
        end = single_cxt->write_pos;
        flush_state_start = 0;
        page_num = DW_FIRST_DATA_PAGE_NUM;
        head_offset = 0;
    } else {
        file_head = single_cxt->second_file_head;
        end = single_cxt->second_write_pos;
        flush_state_start = DW_FIRST_DATA_PAGE_NUM;
        page_num = DW_SECOND_DATA_PAGE_NUM;
        head_offset = (1 + DW_FIRST_DATA_PAGE_NUM) * BLCKSZ;
    }

    file_full = end + 1 >= page_num;

    if (!file_full) {
        return;
    }

    /* reset start position and flush page num for full recycle */
    wait_all_single_dw_finish_flush(is_first);

    PageWriterSync();

    rc = memset_s(single_cxt->single_flush_state + (flush_state_start * sizeof(bool)),
        sizeof(bool) * page_num, 0, sizeof(bool) * page_num);
    securec_check(rc, "\0", "\0");

    dw_prepare_file_head((char *)file_head, 0, file_head->head.dwn + 1);
    dw_pwrite_file(single_cxt->fd, file_head, BLCKSZ, head_offset, SINGLE_DW_FILE_NAME);

    /* The start and write_pos must be reset at the end. */
    file_head->start = 0;
    if (is_first) {
        single_cxt->write_pos = 0;
    } else {
        single_cxt->second_write_pos = 0;
    }

    if (is_first) {
        (void)pg_atomic_add_fetch_u64(&single_cxt->single_stat_info.file_reset_num, 1);
    } else {
        (void)pg_atomic_add_fetch_u64(&single_cxt->single_stat_info.second_file_reset_num, 1);
    }
    ereport(LOG, (errmodule(MOD_DW), errmsg("[single flush] [%s] Reset DW file: file_head[dwn %hu, start %hu], "
        "writer pos is %hu", is_first ? "first version" : "second_version", file_head->head.dwn, file_head->start,
        is_first ? single_cxt->write_pos : single_cxt->second_write_pos)));
    return;
}

static uint16 atomic_get_dw_write_pos(bool is_first)
{
    knl_g_dw_context* dw_single_cxt = &g_instance.dw_single_cxt;
    uint16 page_num = is_first ? DW_FIRST_DATA_PAGE_NUM : DW_SECOND_DATA_PAGE_NUM;
    uint32 write_pos;
    LWLock *lock = is_first ? dw_single_cxt->flush_lock : dw_single_cxt->second_flush_lock;

    pg_memory_barrier();
    write_pos = is_first ? pg_atomic_read_u32(&dw_single_cxt->write_pos) :
        pg_atomic_read_u32(&dw_single_cxt->second_write_pos);

    while (true) {
        if ((write_pos + 1 >= page_num)) {
            (void)LWLockAcquire(lock, LW_EXCLUSIVE);
            dw_single_file_recycle(is_first);
            LWLockRelease(lock);
            write_pos = is_first ? pg_atomic_read_u32(&dw_single_cxt->write_pos) :
                pg_atomic_read_u32(&dw_single_cxt->second_write_pos);
            /* fetch write_pos, we need to check write_pos + 1 again */
            continue;
        }

        if (is_first) {
            if (pg_atomic_compare_exchange_u32(&dw_single_cxt->write_pos, &write_pos, write_pos + 1)) {
                return write_pos;
            }
        } else {
            if (pg_atomic_compare_exchange_u32(&dw_single_cxt->second_write_pos, &write_pos, write_pos + 1)) {
                return write_pos;
            }
        }
    }

    return write_pos;
}

static uint32 dw_recover_file_head(knl_g_dw_context *cxt, bool single, bool first)
{
    uint32 i;
    uint16 id;
    errno_t rc;
    int64 file_size;
    dw_file_head_t *curr_head = NULL;
    dw_file_head_t *working_head = NULL;
    char *file_head = (char *)cxt->file_head;
    uint32 dw_version = 0;
    uint64 head_offset = 0;
    int64 offset;

    if (single && !first) {
        file_head = (char *)cxt->second_file_head;
        head_offset = (1 + DW_FIRST_DATA_PAGE_NUM) * BLCKSZ;
    }
    dw_pread_file(cxt->fd, file_head, BLCKSZ, head_offset);

    for (i = 0; i < DW_FILE_HEAD_ID_NUM; i++) {
        id = g_dw_file_head_ids[i];
        curr_head = (dw_file_head_t *)(file_head + sizeof(dw_file_head_t) * id);
        if (dw_verify_file_head(curr_head)) {
            working_head = curr_head;
            break;
        }
    }

    if (working_head == NULL) {
        ereport(FATAL, (errcode_for_file_access(), errmodule(MOD_DW), errmsg("Single file header is broken")));
        /* we should not get here, since FATAL will do abort. But for ut, return is needed */
        return dw_version;
    }

    ereport(LOG, (errmodule(MOD_DW), errmsg("Found a valid single file header: id %hu, file_head[dwn %hu, start %hu]",
        id, working_head->head.dwn, working_head->start)));

    for (i = 0; i < DW_FILE_HEAD_ID_NUM; i++) {
        id = g_dw_file_head_ids[i];
        curr_head = (dw_file_head_t *)(file_head + sizeof(dw_file_head_t) * id);
        if (curr_head != working_head) {
            rc = memcpy_s(curr_head, sizeof(dw_file_head_t), working_head, sizeof(dw_file_head_t));
            securec_check(rc, "\0", "\0");
        }
    }

    offset = dw_seek_file(cxt->fd, 0, SEEK_END);
    if (single) {
        dw_version = ((dw_file_head_t *)file_head)->dw_version;
        file_size = (dw_version == DW_SUPPORT_NEW_SINGLE_FLUSH ? DW_NEW_SINGLE_FILE_SIZE : DW_SINGLE_FILE_SIZE);
    } else {
        file_size = DW_FILE_SIZE;
    }

    if (offset != file_size) {
        ereport(PANIC, (errmodule(MOD_DW),
            errmsg("DW check file size failed, expected_size %ld, actual_size %ld", DW_FILE_SIZE, offset)));
    }

    dw_pwrite_file(cxt->fd, file_head, BLCKSZ, head_offset, single ? SINGLE_DW_FILE_NAME : OLD_DW_FILE_NAME);
    return dw_version;
}

void dw_cxt_init_single()
{
    char *buf = NULL;
    knl_g_dw_context *single_cxt = &g_instance.dw_single_cxt;
    uint32 dw_version = 0;
    uint16 data_page_num = 0;
    uint64 second_start_offset = 0;

    Assert(single_cxt->flush_lock == NULL);
    single_cxt->flush_lock = LWLockAssign(LWTRANCHE_DW_SINGLE_FIRST);
    single_cxt->second_flush_lock = LWLockAssign(LWTRANCHE_DW_SINGLE_SECOND);
    single_cxt->second_buftag_lock = LWLockAssign(LWTRANCHE_DW_SINGLE_SECOND_BUFTAG);

    single_cxt->fd = dw_open_file(SINGLE_DW_FILE_NAME);

    data_page_num = DW_FIRST_DATA_PAGE_NUM + DW_SECOND_DATA_PAGE_NUM;

    /* two file head plus one for alignment */
    single_cxt->unaligned_buf = (char *)palloc0((DW_SECOND_BUFTAG_PAGE_NUM + 1 + 1 + 1) * BLCKSZ);
    buf = (char *)TYPEALIGN(BLCKSZ, single_cxt->unaligned_buf);
    single_cxt->file_head = (dw_file_head_t *)buf;
    buf += BLCKSZ;
    single_cxt->second_file_head = (dw_file_head_t *)buf;
    buf += BLCKSZ;
    single_cxt->buf = buf;
    single_cxt->single_flush_state = (bool*)palloc0(sizeof(bool) * data_page_num);

    dw_version = dw_recover_file_head(single_cxt, true, true);
    if (dw_version == DW_SUPPORT_NEW_SINGLE_FLUSH) {
        dw_pread_file(single_cxt->fd, single_cxt->file_head, BLCKSZ, 0);
        second_start_offset = (1 + DW_FIRST_DATA_PAGE_NUM) * BLCKSZ;
        dw_recover_file_head(single_cxt, true, false);
        dw_pread_file(single_cxt->fd, single_cxt->second_file_head, BLCKSZ, second_start_offset);
    } else {
        Assert(dw_version == 0);
        /* one file head plus one for alignment */
        single_cxt->recovery_buf.unaligned_buf =
            (char *)palloc0((DW_SINGLE_DIRTY_PAGE_NUM / SINGLE_BLOCK_TAG_NUM + 1 + 1) * BLCKSZ);
        buf = (char *)TYPEALIGN(BLCKSZ, single_cxt->recovery_buf.unaligned_buf);
        single_cxt->recovery_buf.file_head = (dw_file_head_t *)buf;
        buf += BLCKSZ;
        single_cxt->recovery_buf.buf = buf;
        dw_pread_file(single_cxt->fd, single_cxt->recovery_buf.file_head, BLCKSZ, 0);
        single_cxt->recovery_buf.single_flush_state =
            (bool*)palloc0(sizeof(bool) * DW_SINGLE_DIRTY_PAGE_NUM);
        single_cxt->recovery_buf.write_pos = 0;
    }

    pg_atomic_write_u32(&g_instance.dw_single_cxt.dw_version, dw_version);
    single_cxt->closed = 0;
    single_cxt->write_pos = 0;
    single_cxt->second_write_pos = 0;
    single_cxt->flush_page = 0;
}
