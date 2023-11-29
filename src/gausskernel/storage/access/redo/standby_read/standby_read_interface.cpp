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
 * standby_read_interface.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/redo/standby_read/standby_read_interface.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <vector>
#include <cassert>
#include "access/extreme_rto/page_redo.h"
#include "access/extreme_rto/standby_read/block_info_meta.h"
#include "access/extreme_rto/standby_read/lsn_info_meta.h"
#include "access/extreme_rto/standby_read/standby_read_delay_ddl.h"
#include "access/multi_redo_api.h"
#include "pgstat.h"
#include "storage/smgr/relfilenode.h"
#include "storage/buf/buf_internals.h"
#include "storage/buf/bufmgr.h"
#include "../../../page/pageparse.h"
#include "storage/smgr/segment.h"
#include "utils/rel.h"
#include "utils/palloc.h"
#include "access/extreme_rto/dispatcher.h"
#include "funcapi.h"


const char* EXRTO_BASE_PAGE_SUB_DIR = "base_page";
const char* EXRTO_LSN_INFO_SUB_DIR = "lsn_info_meta";
const char* EXRTO_BLOCK_INFO_SUB_DIR = "block_info_meta";
const char* EXRTO_FILE_SUB_DIR[] = {
    EXRTO_BASE_PAGE_SUB_DIR, EXRTO_LSN_INFO_SUB_DIR, EXRTO_BLOCK_INFO_SUB_DIR};
const uint32 EXRTO_FILE_PATH_LEN = 1024;
const uint32 XID_THIRTY_TWO = 32;

void make_standby_read_node(XLogRecPtr read_lsn, RelFileNode &read_node, bool is_start_lsn, Oid relnode)
{
    read_node.spcNode = (Oid)(read_lsn >> 32);
    read_node.dbNode = (Oid)(read_lsn);
    read_node.relNode = relnode;
    read_node.opt = 0;
    if (is_start_lsn) {
        /* means read_lsn is the start ptr of xlog */
        read_node.bucketNode = ExrtoReadStartLSNBktId;
    } else {
        /* means read_lsn is the end ptr of xlog */
        read_node.bucketNode = ExrtoReadEndLSNBktId;
    }
}

BufferDesc *alloc_standby_read_buf(const BufferTag &buf_tag, BufferAccessStrategy strategy, bool &found,
                                   XLogRecPtr read_lsn, bool is_start_lsn)
{
    RelFileNode read_node;
    make_standby_read_node(read_lsn, read_node, is_start_lsn, buf_tag.rnode.relNode);
    BufferDesc *buf_desc = BufferAlloc(read_node, 0, buf_tag.forkNum, buf_tag.blockNum, strategy, &found, NULL);

    return buf_desc;
}

Buffer get_newest_page_for_read(Relation reln, ForkNumber fork_num, BlockNumber block_num, ReadBufferMode mode,
    BufferAccessStrategy strategy, XLogRecPtr read_lsn)
{
    bool hit = false;

    Buffer newest_buf =
        ReadBuffer_common(reln->rd_smgr, reln->rd_rel->relpersistence, fork_num, block_num, mode, strategy, &hit, NULL);
    if (BufferIsInvalid(newest_buf)) {
        return InvalidBuffer;
    }

    LockBuffer(newest_buf, BUFFER_LOCK_SHARE);
    Page newest_page = BufferGetPage(newest_buf);
    XLogRecPtr page_lsn = PageGetLSN(newest_page);
    if (XLByteLT(read_lsn, page_lsn)) {
        UnlockReleaseBuffer(newest_buf);
        return InvalidBuffer;
    }

    BufferTag buf_tag = {
        .rnode = reln->rd_smgr->smgr_rnode.node,
        .forkNum = fork_num,
        .blockNum = block_num,
    };

    ResourceOwnerEnlargeBuffers(t_thrd.utils_cxt.CurrentResourceOwner);
    BufferDesc *buf_desc = alloc_standby_read_buf(buf_tag, strategy, hit, page_lsn, false);

    if (hit) {
        UnlockReleaseBuffer(newest_buf);
        return BufferDescriptorGetBuffer(buf_desc);
    }
    Page read_page = (Page)BufHdrGetBlock(buf_desc);

    errno_t rc = memcpy_s(read_page, BLCKSZ, newest_page, BLCKSZ);
    securec_check(rc, "\0", "\0");

    UnlockReleaseBuffer(newest_buf);
    buf_desc->extra->lsn_on_disk = PageGetLSN(read_page);
#ifdef USE_ASSERT_CHECKING
    buf_desc->lsn_dirty = InvalidXLogRecPtr;
#endif

    TerminateBufferIO(buf_desc, false, (BM_VALID | BM_IS_TMP_BUF));
    return BufferDescriptorGetBuffer(buf_desc);
}

Buffer get_newest_page_for_read_new(
    Relation reln, ForkNumber fork_num, BlockNumber block_num, ReadBufferMode mode, BufferAccessStrategy strategy)
{
    bool hit = false;

    Buffer newest_buf =
        ReadBuffer_common(reln->rd_smgr, reln->rd_rel->relpersistence, fork_num, block_num, mode, strategy, &hit, NULL);
    if (BufferIsInvalid(newest_buf)) {
        return InvalidBuffer;
    }

    LockBuffer(newest_buf, BUFFER_LOCK_SHARE);
    Page newest_page = BufferGetPage(newest_buf);

    BufferTag buf_tag = {
        .rnode = reln->rd_smgr->smgr_rnode.node,
        .forkNum = fork_num,
        .blockNum = block_num,
    };

    ResourceOwnerEnlargeBuffers(t_thrd.utils_cxt.CurrentResourceOwner);
    BufferDesc *buf_desc = alloc_standby_read_buf(buf_tag, strategy, hit, PageGetLSN(newest_page), false);

    if (hit) {
        UnlockReleaseBuffer(newest_buf);
        return BufferDescriptorGetBuffer(buf_desc);
    }
    Page read_page = (Page)BufHdrGetBlock(buf_desc);

    errno_t rc = memcpy_s(read_page, BLCKSZ, newest_page, BLCKSZ);
    securec_check(rc, "\0", "\0");

    UnlockReleaseBuffer(newest_buf);

    buf_desc->extra->lsn_on_disk = PageGetLSN(read_page);
#ifdef USE_ASSERT_CHECKING
    buf_desc->lsn_dirty = InvalidXLogRecPtr;
#endif

    TerminateBufferIO(buf_desc, false, (BM_VALID | BM_IS_TMP_BUF));
    return BufferDescriptorGetBuffer(buf_desc);
}

Buffer standby_read_buf(
    Relation reln, ForkNumber fork_num, BlockNumber block_num, ReadBufferMode mode, BufferAccessStrategy strategy)
{
    if (g_instance.attr.attr_storage.enable_exrto_standby_read_opt) {
        return extreme_rto_standby_read::standby_read_buf_new(reln, fork_num, block_num, mode, strategy);
    }
    /* Open it at the smgr level */
    RelationOpenSmgr(reln);  // need or not ?????
    pgstat_count_buffer_read(reln);
    pgstatCountBlocksFetched4SessionLevel();

    if (RelationisEncryptEnable(reln)) {
        reln->rd_smgr->encrypt = true;
    }

    bool hit = false;
    BufferTag buf_tag = {
        .rnode = reln->rd_smgr->smgr_rnode.node,
        .forkNum = fork_num,
        .blockNum = block_num,
    };

    XLogRecPtr read_lsn = MAX_XLOG_REC_PTR;
    if (u_sess->utils_cxt.CurrentSnapshot != NULL && XLogRecPtrIsValid(u_sess->utils_cxt.CurrentSnapshot->read_lsn)) {
        read_lsn = u_sess->utils_cxt.CurrentSnapshot->read_lsn;
    } else if (XLogRecPtrIsValid(t_thrd.proc->exrto_read_lsn)) {
        read_lsn = t_thrd.proc->exrto_read_lsn;
    }

    Buffer read_buf = get_newest_page_for_read(reln, fork_num, block_num, mode, strategy, read_lsn);

    if (read_buf != InvalidBuffer) {
        // newest page's lsn smaller than read lsn
        return read_buf;
    }
    ResourceOwnerEnlargeBuffers(t_thrd.utils_cxt.CurrentResourceOwner);
    // read lsn info
    StandbyReadLsnInfoArray *lsn_info = &t_thrd.exrto_recycle_cxt.lsn_info;
    bool result = extreme_rto_standby_read::get_page_lsn_info(buf_tag, strategy, read_lsn, lsn_info);
    if (!result) {
        ereport(
            ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
             (errmsg("standby_read_buf couldnot found buf %u/%u/%u %d %u read lsn %08X/%08X current_time: %ld "
                     "gen_snaptime:%ld thread_read_lsn:%08X/%08X",
                     buf_tag.rnode.spcNode, buf_tag.rnode.dbNode, buf_tag.rnode.relNode, buf_tag.forkNum,
                     buf_tag.blockNum, (uint32)(read_lsn >> XID_THIRTY_TWO), (uint32)read_lsn, GetCurrentTimestamp(),
                     g_instance.comm_cxt.predo_cxt.exrto_snapshot->gen_snap_time,
                     (uint32)(t_thrd.proc->exrto_read_lsn >> XID_THIRTY_TWO), (uint32)t_thrd.proc->exrto_read_lsn))));
        return InvalidBuffer;
    }

    // read lsn info
    XLogRecPtr expected_lsn = InvalidXLogRecPtr;
    bool is_start_lsn = true;
    if (lsn_info->lsn_num == 0) {
        expected_lsn = lsn_info->base_page_lsn;
        is_start_lsn = false;
    } else {
        Assert(lsn_info->lsn_array[lsn_info->lsn_num - 1] > 0);
        Assert(lsn_info->lsn_array[lsn_info->lsn_num - 1] < read_lsn);
        Assert(lsn_info->lsn_array[lsn_info->lsn_num - 1] >= lsn_info->base_page_lsn);
        expected_lsn = lsn_info->lsn_array[lsn_info->lsn_num - 1];
    }

    BufferDesc* buf_desc = alloc_standby_read_buf(buf_tag, strategy, hit, expected_lsn, is_start_lsn);

    if (hit) {
        return BufferDescriptorGetBuffer(buf_desc);
    }
    buffer_in_progress_pop();
    // read_base_page
    extreme_rto_standby_read::read_base_page(buf_tag, lsn_info->base_page_pos, buf_desc);
    if (lsn_info->lsn_num > 0) {
        redo_target_page(buf_tag, lsn_info, BufferDescriptorGetBuffer(buf_desc));
    }
    Page page = BufferGetPage(BufferDescriptorGetBuffer(buf_desc));
    buf_desc->extra->lsn_on_disk = PageGetLSN(page);
#ifdef USE_ASSERT_CHECKING
    buf_desc->lsn_dirty = InvalidXLogRecPtr;
#endif
    buffer_in_progress_push();
    TerminateBufferIO(buf_desc, false, (BM_VALID | BM_IS_TMP_BUF));

    return BufferDescriptorGetBuffer(buf_desc);
}

void make_exrto_file_directory()
{
    if (!IS_EXRTO_READ) {
        return;
    }
    if (mkdir(EXRTO_FILE_DIR, S_IRWXU) < 0 && errno != EEXIST) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not create directory \"%s\": %m", EXRTO_FILE_DIR)));
    }

    char sub_dir[EXRTO_FILE_PATH_LEN];
    errno_t rc = EOK;
    for (ExRTOFileType type = BASE_PAGE; type <= BLOCK_INFO_META; type = static_cast<ExRTOFileType>(type + 1)) {
        rc = snprintf_s(sub_dir, EXRTO_FILE_PATH_LEN, EXRTO_FILE_PATH_LEN - 1, "%s/%s", EXRTO_FILE_DIR,
                        EXRTO_FILE_SUB_DIR[type]);
        securec_check_ss(rc, "\0", "\0");
        if (mkdir(sub_dir, S_IRWXU) < 0 && errno != EEXIST) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not create directory \"%s\": %m", sub_dir)));
        }
    }
}

void exrto_clean_dir(void)
{
    int ret = 0;
    ereport(LOG, (errmsg("exrto_clean_dir: start to clean dir.")));
    if (!isDirExist(EXRTO_FILE_DIR)) {
        return;
    }

    if (!isDirExist(EXRTO_OLD_FILE_DIR)) {
        ereport(LOG, (errmsg("exrto_clean_dir: rename standby_read to standby_read_old.")));
        ret = rename(EXRTO_FILE_DIR, EXRTO_OLD_FILE_DIR);
        if (ret != 0) {
            ereport(ERROR, (errcode_for_file_access(),
                            errmsg("failed to rename exrto standby_read dir: %s\n", EXRTO_FILE_DIR)));
            return;
        }
    } else {
        ereport(LOG, (errmsg("exrto_clean_dir: remove standby_read.")));
        if (!rmtree(EXRTO_FILE_DIR, true)) {
            ereport(WARNING, (errcode_for_file_access(),
                              errmsg("could not remove exrto standby_read dir: %s\n", EXRTO_FILE_DIR)));
        }
    }
}

/* This function will be attached to the recycle thread */
void exrto_recycle_old_dir(void)
{
    if (!rmtree(EXRTO_OLD_FILE_DIR, true)) {
        ereport(WARNING, (errcode_for_file_access(),
                          errmsg("could not remove exrto standby_read_old dir: %s\n", EXRTO_OLD_FILE_DIR)));
    }
}

void exrto_standby_read_init()
{
    exrto_clean_dir();
    if (IS_EXRTO_READ) {
        make_exrto_file_directory();
    }
    init_delay_ddl_file();
}

bool check_need_drop_buffer(StandbyReadMetaInfo *meta_info, const BufferTag tag)
{
    Assert(meta_info != NULL);
    if (!IS_EXRTO_RELFILENODE(tag.rnode)) {
        return false;
    }

    ExRTOFileType type = exrto_file_type(tag.rnode.spcNode);
    if (type == BLOCK_INFO_META) {
        return false;
    }

    uint32 batch_id = tag.rnode.dbNode >> LOW_WORKERID_BITS;
    uint32 worker_id = tag.rnode.dbNode & LOW_WORKERID_MASK;
    if (batch_id == meta_info->batch_id && worker_id == meta_info->redo_id) {
        uint64 total_block_num =
            get_total_block_num(type, tag.rnode.relNode, tag.blockNum);
        uint64 recycle_pos = ((type == BASE_PAGE) ? meta_info->base_page_recyle_position
                                                    : meta_info->lsn_table_recyle_position);
        return (total_block_num < (recycle_pos / BLCKSZ));
    }

    return false;
}

void buffer_drop_exrto_standby_read_buffers(StandbyReadMetaInfo *meta_info)
{
    bool drop_all = (meta_info == NULL);
    if (drop_all) {
        ereport(LOG, (errmsg("buffer_drop_exrto_standby_read_buffers: start to drop buffers.")));
    }

    int i = 0;
    while (i < TOTAL_BUFFER_NUM) {
        BufferDesc *buf_desc = GetBufferDescriptor(i);
        uint64 buf_state;
        bool need_drop = false;
        /*
         * Some safe unlocked checks can be done to reduce the number of cycle.
         */
        if (!IS_EXRTO_RELFILENODE(buf_desc->tag.rnode)) {
            i++;
            continue;
        }

        buf_state = LockBufHdr(buf_desc);
        if (drop_all) {
            need_drop = IS_EXRTO_RELFILENODE(buf_desc->tag.rnode);
        } else {
            /* only drop base page and lsn info buffers */
            need_drop = check_need_drop_buffer(meta_info, buf_desc->tag);
        }
        if (need_drop) {
            InvalidateBuffer(buf_desc); /* with buffer head lock released */
        } else {
            UnlockBufHdr(buf_desc, buf_state);
        }
        i++;
    }
}

Datum gs_hot_standby_space_info(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_LITE_MODE    
#define EXRTO_HOT_STANDBY_SPACE_INFO_INFONUM 6
    Datum values[EXRTO_HOT_STANDBY_SPACE_INFO_INFONUM];
    errno_t rc;
    bool nulls[EXRTO_HOT_STANDBY_SPACE_INFO_INFONUM];
    HeapTuple tuple = NULL;
    TupleDesc tupdesc = NULL;
    uint64 lsn_file_size = 0;
    uint64 lsn_file_num = 0;
    uint64 basepage_file_size = 0;
    uint64 basepage_file_num = 0;
    uint64 block_meta_file_size = 0;
    uint64 block_meta_file_num = 0;
    uint32 worker_nums;

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");

    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    tupdesc = CreateTemplateTupleDesc(EXRTO_HOT_STANDBY_SPACE_INFO_INFONUM, false);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_1, "base_page_file_num", XIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_2, "base_page_total_size", XIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_3, "lsn_info_meta_file_num", XIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_4, "lsn_info_meta_total_size", XIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_5, "block_info_meta_file_num", XIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)ARG_6, "block_info_meta_total_size", XIDOID, -1, 0);

    tupdesc = BlessTupleDesc(tupdesc);

    SpinLockAcquire(&(g_instance.comm_cxt.predo_cxt.destroy_lock));
    if (extreme_rto::g_dispatcher == NULL) {
        worker_nums = 0;
    } else {
        worker_nums = extreme_rto::g_dispatcher->allWorkersCnt;
    }

    for (uint32 i = 0; i < worker_nums; ++i) {
        extreme_rto::PageRedoWorker *page_redo_worker = extreme_rto::g_dispatcher->allWorkers[i];
        if (page_redo_worker->role != extreme_rto::REDO_PAGE_WORKER || (page_redo_worker->isUndoSpaceWorker)) {
            continue;
        }
        StandbyReadMetaInfo meta_info = page_redo_worker->standby_read_meta_info;

        uint64 lsn_file_size_per_thread = 0;
        if (meta_info.lsn_table_next_position > meta_info.lsn_table_recyle_position) {
            lsn_file_size_per_thread = meta_info.lsn_table_next_position - meta_info.lsn_table_recyle_position;
            /* in 0~lsn_table_recyle_position No data is stored,
               means the size of one lsn info file does not reach maxsize
               eg:0~100KB(lsn_table_recyle_position), 100KB~(16M+100KB)(lsn_table_next_position), filenum:2, size:16M */
            lsn_file_num += meta_info.lsn_table_next_position / EXRTO_LSN_INFO_FILE_MAXSIZE +
                            ((meta_info.lsn_table_next_position % EXRTO_LSN_INFO_FILE_MAXSIZE) > 0 ? 1 : 0) -
                            (meta_info.lsn_table_recyle_position / EXRTO_LSN_INFO_FILE_MAXSIZE);
        }
        lsn_file_size += lsn_file_size_per_thread;

        uint64 basepage_file_size_per_thread = 0;
        if (meta_info.base_page_next_position > meta_info.base_page_recyle_position) {
            basepage_file_size_per_thread = meta_info.base_page_next_position - meta_info.base_page_recyle_position;
            basepage_file_num += meta_info.base_page_next_position / EXRTO_BASE_PAGE_FILE_MAXSIZE +
                                ((meta_info.base_page_next_position % EXRTO_BASE_PAGE_FILE_MAXSIZE) > 0 ? 1 : 0) -
                                 (meta_info.base_page_recyle_position / EXRTO_BASE_PAGE_FILE_MAXSIZE);
        }
        basepage_file_size += basepage_file_size_per_thread;
    }
    SpinLockRelease(&(g_instance.comm_cxt.predo_cxt.destroy_lock));

    char block_meta_file_dir[EXRTO_FILE_PATH_LEN];
    char block_meta_file_name[EXRTO_FILE_PATH_LEN];
    struct dirent *de = nullptr;
    struct stat st;

    rc = snprintf_s(block_meta_file_dir, EXRTO_FILE_PATH_LEN, EXRTO_FILE_PATH_LEN - 1, "./%s/%s",
                    EXRTO_FILE_DIR, EXRTO_BLOCK_INFO_SUB_DIR);
    securec_check_ss(rc, "\0", "\0");

    DIR *dir = opendir(block_meta_file_dir);
    while ((dir != NULL) && (de = gs_readdir(dir)) != NULL) {
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) {
            continue;
        }
        rc = snprintf_s(block_meta_file_name, EXRTO_FILE_PATH_LEN, EXRTO_FILE_PATH_LEN - 1, "%s/%s",
                        block_meta_file_dir, de->d_name);
        securec_check_ss(rc, "\0", "\0");
        if (lstat(block_meta_file_name, &st) != 0) {
            continue;
        }
        block_meta_file_num++;
        block_meta_file_size = block_meta_file_size + (uint64)st.st_size;
    }

    values[ARG_0] = TransactionIdGetDatum(basepage_file_num);
    values[ARG_1] = TransactionIdGetDatum(basepage_file_size);
    values[ARG_2] = TransactionIdGetDatum(lsn_file_num);
    values[ARG_3] = TransactionIdGetDatum(lsn_file_size);
    values[ARG_4] = TransactionIdGetDatum(block_meta_file_num);
    values[ARG_5] = TransactionIdGetDatum(block_meta_file_size);

    tuple = heap_form_tuple(tupdesc, values, nulls);
    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
#else
    FEATURE_ON_LITE_MODE_NOT_SUPPORTED();
    PG_RETURN_TEXT_P(NULL);
#endif    
}

namespace extreme_rto_standby_read {
typedef struct _DumpLsnInfo {
    XLogRecPtr lsn_info_end_lsn;
    XLogRecPtr base_page_info_cur_lsn;
    std::vector<XLogRecPtr> lsn_info_vec;
} DumpLsnInfo;
XLogRecPtr acquire_max_lsn()
{
    if (RecoveryInProgress()) {
        ereport(LOG, (errmsg("Can't get local max LSN during recovery in dump.")));
    }
    XLogRecPtr current_recptr = GetXLogWriteRecPtr();
    return current_recptr;
}

void exrto_xlog_dump_err_rep(XLogReaderState *xlogreader_state, char *error_msg)
{
    if (xlogreader_state == NULL) {
        ereport(LOG, (errmsg("could not read WAL record, xlogreader_state is invalid in dump.")));
        return;
    }

    if (error_msg != nullptr) {
        ereport(LOG, (errmsg("could not read WAL record in dump at %X/%X: %s",
            (uint32)(xlogreader_state->ReadRecPtr >> XIDTHIRTYTWO),
            (uint32)xlogreader_state->ReadRecPtr, error_msg)));
    } else {
        ereport(LOG, (errmsg("could not read WAL record in dump at %X/%X",
            (uint32)(xlogreader_state->ReadRecPtr >> XIDTHIRTYTWO),
            (uint32)xlogreader_state->ReadRecPtr)));
    }
}

void exrto_xlog_dump(char *dump_filename, DumpLsnInfo dump_lsn_info_stru)
{
    XLogRecPtr start_lsn = dump_lsn_info_stru.base_page_info_cur_lsn;
    XLogRecPtr end_lsn = dump_lsn_info_stru.lsn_info_end_lsn;
    ereport(LOG, (errmsg("start_lsn in dump at %X/%X. end_lsn in dump at %X/%X ", (uint32)(start_lsn >> XIDTHIRTYTWO),
                         (uint32)start_lsn, (uint32)(end_lsn >> XIDTHIRTYTWO), (uint32)end_lsn)));
    /* start reading */
    errno_t rc = EOK;
    WalPrivate read_private;
    rc = memset_s(&read_private, sizeof(WalPrivate), 0, sizeof(WalPrivate));
    securec_check(rc, "\0", "\0");
    read_private.data_dir = t_thrd.proc_cxt.DataDir;
    read_private.tli = 1;

    XLogRecPtr min_lsn = (XLogGetLastRemovedSegno() + 1) * XLogSegSize;
    XLogRecPtr max_lsn = acquire_max_lsn();
    if (XLByteLT(start_lsn, min_lsn)) {
        start_lsn = min_lsn;
    }
    if ((max_lsn > start_lsn) && (XLByteLT(max_lsn, end_lsn) || end_lsn == PG_UINT64_MAX)) {
        end_lsn = max_lsn;
    }

    XLogReaderState *xlogreader_state = XLogReaderAllocate(&SimpleXLogPageRead, &read_private);
    if (!xlogreader_state) {
        ereport(LOG, (errmsg("memory is temporarily unavailable in dump while allocate xlog reader")));
    }
    /* get the first valid xlog record location */
    XLogRecPtr first_record = XLogFindNextRecord(xlogreader_state, start_lsn);
    /* if we are recycling or removing log files concurrently, we can't find the next record right after.
     * Hence, we need to update the min_lsn */
    if (XLByteEQ(first_record, InvalidXLogRecPtr)) {
        ereport(LOG, (errmsg("XLogFindNextRecord in dump: could not find a valid record after %X/%X. Retry.",
                             (uint32)(start_lsn >> XIDTHIRTYTWO), (uint32)start_lsn)));
        bool found = false;
        first_record = UpdateNextLSN(start_lsn, end_lsn, xlogreader_state, &found);
        if (!found)
            ereport(LOG, (errmsg("XLogFindNextRecord in dump: could not find a valid record between %X/%X and %X/%X.",
                                 (uint32)(start_lsn >> XIDTHIRTYTWO), (uint32)start_lsn,
                                 (uint32)(end_lsn >> XIDTHIRTYTWO), (uint32)end_lsn)));
    }
    XLogRecPtr valid_start_lsn = first_record;
    XLogRecPtr valid_end_lsn = valid_start_lsn;

    FILE *output_file = fopen(dump_filename, "a");
    CheckOpenFile(output_file, dump_filename);
    char *str_output = (char *)palloc0(MAXOUTPUTLEN * sizeof(char));

    /* valid first record is not the given one */
    if (!XLByteEQ(first_record, start_lsn) && (start_lsn % XLogSegSize) != 0) {
        rc = snprintf_s(str_output + (int)strlen(str_output), MAXOUTPUTLEN, MAXOUTPUTLEN - 1,
                        "first record is after %X/%X, at %X/%X, skipping over %lu bytes\n",
                        (uint32)(start_lsn >> XIDTHIRTYTWO), (uint32)start_lsn,
                        (uint32)(first_record >> XIDTHIRTYTWO), (uint32)first_record,
                        XLByteDifference(first_record, start_lsn));
        securec_check_ss(rc, "\0", "\0");
    }
    CheckWriteFile(output_file, dump_filename, str_output);
    pfree_ext(str_output);

    size_t count = 0;
    char *error_msg = nullptr;
    XLogRecord *record = NULL;
    while (xlogreader_state && XLByteLE(xlogreader_state->EndRecPtr, end_lsn)) {
        CHECK_FOR_INTERRUPTS(); /* Allow cancel/die interrupts */
        record = XLogReadRecord(xlogreader_state, first_record, &error_msg);
        valid_end_lsn = xlogreader_state->EndRecPtr;
        if (!record && XLByteLE(valid_end_lsn, end_lsn)) {
            /* if we are recycling or removing log files concurrently, and we can't find the next record right
             * after. In this case, we try to read from the current oldest xlog file. */
            bool found = false;
            XLogRecPtr temp_start_lsn = Max(xlogreader_state->EndRecPtr, start_lsn);
            first_record = UpdateNextLSN(temp_start_lsn, end_lsn, xlogreader_state, &found);
            if (found) {
                ereport(LOG, (errmsg("We cannot read %X/%X. After retried, we jump to read the next available %X/%X. "
                                     "The missing part might be recycled or removed.",
                                     (uint32)(temp_start_lsn >> XIDTHIRTYTWO), (uint32)temp_start_lsn,
                                     (uint32)(first_record >> XIDTHIRTYTWO), (uint32)first_record)));
                continue;
            }
            exrto_xlog_dump_err_rep(xlogreader_state, error_msg);
            break;
        } else if (!record && !XLByteLT(valid_end_lsn, end_lsn)) {
            error_msg = nullptr;
            exrto_xlog_dump_err_rep(xlogreader_state, error_msg);
            break;
        }

        str_output = (char *)palloc(MAXOUTPUTLEN * sizeof(char));
        rc = memset_s(str_output, MAXOUTPUTLEN, 0, MAXOUTPUTLEN);
        securec_check(rc, "\0", "\0");
        XLogDumpDisplayRecord(xlogreader_state, str_output);
        count++;

        CheckWriteFile(output_file, dump_filename, str_output);
        pfree_ext(str_output);
        if (count >= dump_lsn_info_stru.lsn_info_vec.size()) {
            break;
        }
        first_record = dump_lsn_info_stru.lsn_info_vec.at(count);
    }
    XLogReaderFree(xlogreader_state);
    str_output = (char *)palloc(MAXOUTPUTLEN * sizeof(char));
    rc = memset_s(str_output, MAXOUTPUTLEN, 0, MAXOUTPUTLEN);
    securec_check(rc, "\0", "\0");

    /* Summary(xx total): valid start_lsn: xxx, valid end_lsn: xxx */
    rc = snprintf_s(str_output + (int)strlen(str_output), MAXOUTPUTLEN, MAXOUTPUTLEN - 1,
                    "\nSummary (%zu total): valid start_lsn: %X/%X, valid end_lsn: %X/%X\n", count,
                    (uint32)(valid_start_lsn >> XIDTHIRTYTWO), (uint32)(valid_start_lsn),
                    (uint32)(valid_end_lsn >> XIDTHIRTYTWO), (uint32)(valid_end_lsn));
    securec_check_ss(rc, "\0", "\0");
    CheckWriteFile(output_file, dump_filename, str_output);
    CheckCloseFile(output_file, dump_filename, true);
    pfree_ext(str_output);
    CloseXlogFile();
}

void dump_base_page_info(char *str_output, BasePageInfo base_page_info)
{
    errno_t rc = EOK;
    rc = snprintf_s(str_output + (int)strlen(str_output), MAXOUTPUTLEN, MAXOUTPUTLEN - 1,
                    "Base_page_info: cur_page_lsn=%lu, read_lsn=%lu, spcNode=%u, dbNode=%u, relNode=%u, bucketnode=%d, "
                    "fork_num=%d, block_num=%u, next_base_page_lsn=%lu, base_page_position=%lu, pre=%lu, next=%lu\n",
                    base_page_info->cur_page_lsn, t_thrd.proc->exrto_read_lsn, base_page_info->relfilenode.spcNode,
                    base_page_info->relfilenode.dbNode, base_page_info->relfilenode.relNode,
                    base_page_info->relfilenode.bucketNode, base_page_info->fork_num, base_page_info->block_num,
                    base_page_info->next_base_page_lsn, base_page_info->base_page_position,
                    base_page_info->base_page_list.prev, base_page_info->base_page_list.next);
    securec_check_ss(rc, "\0", "\0");
}

void dump_lsn_info(char *str_output, const BasePageInfo base_page_info, DumpLsnInfo &dump_lsn_info_stru,
                   const BufferTag& buf_tag, Buffer buffer)
{
    LsnInfo lsn_info = &base_page_info->lsn_info_node;
    LsnInfoPosition next_lsn_info_pos;
    bool find_front = false;
    dump_lsn_info_stru.base_page_info_cur_lsn = base_page_info->cur_page_lsn;
    dump_lsn_info_stru.lsn_info_end_lsn = dump_lsn_info_stru.base_page_info_cur_lsn;
 
    uint32 batch_id;
    uint32 worker_id;
 
    extreme_rto::RedoItemTag redo_item_tag;
    INIT_REDO_ITEM_TAG(redo_item_tag, buf_tag.rnode, buf_tag.forkNum, buf_tag.blockNum);
    batch_id = extreme_rto::GetSlotId(buf_tag.rnode, 0, 0, (uint32)extreme_rto::get_batch_redo_num()) + 1;
    worker_id = extreme_rto::GetWorkerId(&redo_item_tag, extreme_rto::get_page_redo_worker_num_per_manager()) + 1;
 
    do {
        errno_t rc = EOK;
        rc = snprintf_s(str_output + (int)strlen(str_output), MAXOUTPUTLEN, MAXOUTPUTLEN - 1,
                        "Lsn_info: pre=%lu, next=%lu, flags=%u, type=%u, used=%u\n",
                        lsn_info->lsn_list.prev, lsn_info->lsn_list.next, lsn_info->flags, lsn_info->type,
                        lsn_info->used);
        securec_check_ss(rc, "\0", "\0");
        for (uint16 i = 0; i < lsn_info->used; ++i) {
            rc = snprintf_s(str_output + (int)strlen(str_output), MAXOUTPUTLEN, MAXOUTPUTLEN - 1,
                            "lsn_num is %lu\n", lsn_info->lsn[i]);
            securec_check_ss(rc, "\0", "\0");
            dump_lsn_info_stru.lsn_info_vec.push_back(lsn_info->lsn[i]);
            if (!find_front) {
                dump_lsn_info_stru.base_page_info_cur_lsn = lsn_info->lsn[i];
                find_front = true;
            }
            dump_lsn_info_stru.lsn_info_end_lsn = lsn_info->lsn[i];
        }
        next_lsn_info_pos = lsn_info->lsn_list.next;
        UnlockReleaseBuffer(buffer);
        /* reach the end of the list */
        if (next_lsn_info_pos == LSN_INFO_LIST_HEAD) {
            break;
        }

        Page page = get_lsn_info_page(batch_id, worker_id, next_lsn_info_pos, RBM_NORMAL, &buffer);
        if (unlikely(page == NULL || buffer == InvalidBuffer)) {
            ereport(LOG, (errmsg(EXRTOFORMAT("get_lsn_info_page failed, batch_id: %u, redo_id: %u, pos: %lu"), batch_id,
                                 worker_id, next_lsn_info_pos)));
            break;
        }
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
 
        uint32 offset = lsn_info_postion_to_offset(next_lsn_info_pos);
        lsn_info = (LsnInfo)(page + offset);
    } while (true);
}
 
// dump all version of basepage info lsn info
void dump_base_page_info_lsn_info(const BufferTag &buf_tag, LsnInfoPosition head_lsn_base_page_pos, char *str_output,
                                  DumpLsnInfo &dump_lsn_info_stru)
{
    uint32 batch_id;
    uint32 worker_id;
    BasePageInfo base_page_info = NULL;
    Buffer buffer;
    const int max_dump_item = 10000;
    int cnt = 0;

    extreme_rto::RedoItemTag redo_item_tag;
    INIT_REDO_ITEM_TAG(redo_item_tag, buf_tag.rnode, buf_tag.forkNum, buf_tag.blockNum);
    batch_id = extreme_rto::GetSlotId(buf_tag.rnode, 0, 0, (uint32)extreme_rto::get_batch_redo_num()) + 1;
    worker_id = extreme_rto::GetWorkerId(&redo_item_tag, extreme_rto::get_page_redo_worker_num_per_manager()) + 1;

    /* find fisrt base page whose lsn less than read lsn form tail to head */
    do {
        if (cnt > max_dump_item) {
            break;
        }
        /* reach the end of the list */
        if (INFO_POSITION_IS_INVALID(head_lsn_base_page_pos)) {
            ereport(LOG, (errmsg("can not find base page, block is %u/%u/%u %d %u, batch_id: %u, redo_worker_id: %u",
                                 buf_tag.rnode.spcNode, buf_tag.rnode.dbNode, buf_tag.rnode.relNode, buf_tag.forkNum,
                                 buf_tag.blockNum, batch_id, worker_id)));
            break;
        }
        buffer = InvalidBuffer;
        Page page = get_lsn_info_page(batch_id, worker_id, head_lsn_base_page_pos, RBM_NORMAL, &buffer);
        if (unlikely(page == NULL || buffer == InvalidBuffer)) {
            ereport(LOG, (errmsg(EXRTOFORMAT("get_lsn_info_page failed, batch_id: %u, redo_id: %u, pos: %lu"), batch_id,
                                 worker_id, head_lsn_base_page_pos)));
            break;
        }
        LockBuffer(buffer, BUFFER_LOCK_SHARE);

        uint32 offset = lsn_info_postion_to_offset(head_lsn_base_page_pos);
        base_page_info = (BasePageInfo)(page + offset);

        if (is_base_page_type(base_page_info->lsn_info_node.type) == false) {
            UnlockReleaseBuffer(buffer);
            break;
        }

        Buffer base_page_datum_buffer =
            buffer_read_base_page(batch_id, worker_id, base_page_info->base_page_position, RBM_NORMAL);
        LockBuffer(base_page_datum_buffer, BUFFER_LOCK_SHARE);

        UnlockReleaseBuffer(base_page_datum_buffer);

        dump_base_page_info(str_output, base_page_info); // print info
        dump_lsn_info(str_output, base_page_info, dump_lsn_info_stru, buf_tag, buffer);
        head_lsn_base_page_pos = base_page_info->base_page_list.prev;
    } while (true);
}

void dump_current_lsn(char *str_output)
{
    errno_t rc = snprintf_s(str_output + (int)strlen(str_output), MAXOUTPUTLEN, MAXOUTPUTLEN - 1,
                            "current redo position %lu, cache lsn: %lu\n",
                            g_instance.comm_cxt.predo_cxt.redoPf.last_replayed_end_ptr, t_thrd.proc->exrto_read_lsn);
    securec_check_ss(rc, "\0", "\0");
}

void dump_one_block_info(char *str_output, BlockMetaInfo* block_meta_info)
{
    errno_t rc = snprintf_s(str_output + (int)strlen(str_output), MAXOUTPUTLEN, MAXOUTPUTLEN - 1,
                            "Block info meta info: timeline=%u, record_num=%u, min_lsn=%lu, max_lsn=%lu, "
                            "lsn_info_list prev=%lu, next=%lu, base_page_info_list prev=%lu, next=%lu\n",
                            block_meta_info->timeline, block_meta_info->record_num, block_meta_info->min_lsn,
                            block_meta_info->max_lsn, block_meta_info->lsn_info_list.prev,
                            block_meta_info->lsn_info_list.next, block_meta_info->base_page_info_list.prev,
                            block_meta_info->base_page_info_list.next);
    securec_check_ss(rc, "\0", "\0");
}

void dump_error_all_info(const RelFileNode &rnode, ForkNumber forknum, BlockNumber blocknum)
{
    if (!IS_EXRTO_STANDBY_READ) {
        return;
    }
    buffer_in_progress_pop();
    BufferTag buf_tag;
    INIT_BUFFERTAG(buf_tag, rnode, forknum, blocknum);

    Buffer buf;
    BlockMetaInfo *block_meta_info = get_block_meta_info_by_relfilenode(buf_tag, NULL, RBM_NORMAL, &buf, true);
    if (block_meta_info == NULL) {
        ereport(LOG,
                (errmsg("can not find block meta info by given buftag. rnode is %u/%u/%u %d %u", buf_tag.rnode.spcNode,
                        buf_tag.rnode.dbNode, buf_tag.rnode.relNode, buf_tag.forkNum, buf_tag.blockNum)));
        buffer_in_progress_push();
        return;  // it's more likely we cannot get block info than cannot alloc file, so put this first for performance.
    }

    char *str_output = (char *)palloc0(MAXOUTPUTLEN * sizeof(char));
    char *dump_filename = (char *)palloc0(MAXFILENAME * sizeof(char));
    errno_t rc = snprintf_s(dump_filename + (int)strlen(dump_filename), MAXFILENAME, MAXFILENAME - 1,
                            "%s/%u_%u_%u_%d_%d.lsnblockinfo_dump", u_sess->attr.attr_common.Log_directory,
                            rnode.spcNode, rnode.dbNode, rnode.relNode, forknum, blocknum);
    securec_check_ss(rc, "\0", "\0");
    struct stat file_stat;
    if (stat(dump_filename, &file_stat) == 0) {
        /* file exists */
        pfree_ext(str_output);
        pfree_ext(dump_filename);
        buffer_in_progress_push();
        return;
    }

    FILE *dump_file = AllocateFile(dump_filename, PG_BINARY_W);
    if (dump_file == NULL) {
        ereport(LOG, (errmsg("can not alloc file. rnode is %u/%u/%u %d %u", buf_tag.rnode.spcNode,
                              buf_tag.rnode.dbNode, buf_tag.rnode.relNode, buf_tag.forkNum, buf_tag.blockNum)));
        pfree_ext(str_output);
        pfree_ext(dump_filename);
        buffer_in_progress_push();
        return;
    }

    dump_current_lsn(str_output);
    dump_one_block_info(str_output, block_meta_info);

    DumpLsnInfo dump_lsn_info_stru;
    dump_lsn_info_stru.lsn_info_end_lsn = PG_UINT64_MAX;
    dump_base_page_info_lsn_info(buf_tag, block_meta_info->base_page_info_list.prev, str_output, dump_lsn_info_stru);
    UnlockReleaseBuffer(buf);  // buf was automatically locked by getting block meta info, so we need release

    uint result = fwrite(str_output, 1, strlen(str_output), dump_file);
    if (result == strlen(str_output)) {
        (void)fsync(fileno(dump_file));
        exrto_xlog_dump(dump_filename, dump_lsn_info_stru);
    } else {
        pfree_ext(str_output);
        pfree_ext(dump_filename);
        (void)FreeFile(dump_file);
        buffer_in_progress_push();

        ereport(ERROR, (errcode(ERRCODE_FILE_WRITE_FAILED),
                        errmsg("Cannot write into file %s/%u_%u_%u_%d_%u.lsnblockinfo_dump!",
                               u_sess->attr.attr_common.Log_directory, rnode.spcNode, rnode.dbNode, rnode.relNode,
                               forknum, blocknum)));
    }
    pfree_ext(str_output);
    pfree_ext(dump_filename);
    (void)FreeFile(dump_file);
    buffer_in_progress_push();
}

Buffer standby_read_buf_new(
    Relation reln, ForkNumber fork_num, BlockNumber block_num, ReadBufferMode mode, BufferAccessStrategy strategy)
{
    /* Open it at the smgr level */
    RelationOpenSmgr(reln);  // need or not ?????
    pgstat_count_buffer_read(reln);
    pgstatCountBlocksFetched4SessionLevel();

    if (RelationisEncryptEnable(reln)) {
        reln->rd_smgr->encrypt = true;
    }

    XLogRecPtr read_lsn = MAX_XLOG_REC_PTR;
    if (u_sess->utils_cxt.CurrentSnapshot != NULL && XLogRecPtrIsValid(u_sess->utils_cxt.CurrentSnapshot->read_lsn)) {
        read_lsn = u_sess->utils_cxt.CurrentSnapshot->read_lsn;
    } else if (XLogRecPtrIsValid(t_thrd.proc->exrto_read_lsn)) {
        read_lsn = t_thrd.proc->exrto_read_lsn;
    }


    Buffer read_buf = get_newest_page_for_read_new(reln, fork_num, block_num, mode, strategy);
    if (unlikely(read_buf == InvalidBuffer)) {
        ereport(DEBUG1,
            (errmsg("couldnot get newest page buf %u/%u/%u %d %u read lsn %08X/%08X current_time: %ld "
                    "gen_snaptime:%ld thread_read_lsn:%08X/%08X",
                reln->rd_smgr->smgr_rnode.node.spcNode,
                reln->rd_smgr->smgr_rnode.node.dbNode,
                reln->rd_smgr->smgr_rnode.node.relNode,
                fork_num,
                block_num,
                (uint32)(read_lsn >> XID_THIRTY_TWO),
                (uint32)read_lsn,
                GetCurrentTimestamp(),
                g_instance.comm_cxt.predo_cxt.exrto_snapshot->gen_snap_time,
                (uint32)(t_thrd.proc->exrto_read_lsn >> XID_THIRTY_TWO),
                (uint32)t_thrd.proc->exrto_read_lsn)));
        return InvalidBuffer;
    }

    if (XLByteLT(PageGetLSN(BufferGetPage(read_buf)), read_lsn)) {
        return read_buf;
    }

    BufferTag buf_tag = {
        .rnode = reln->rd_smgr->smgr_rnode.node,
        .forkNum = fork_num,
        .blockNum = block_num,
    };

    Buffer block_info_buf;
    // just lock this buffer ,so that redo worker could not modify this block info
    BlockMetaInfo *block_info =
        get_block_meta_info_by_relfilenode(buf_tag, NULL, RBM_ZERO_ON_ERROR, &block_info_buf, true);
    if (unlikely(block_info == NULL || block_info_buf == InvalidBuffer)) {
        ereport(PANIC,
            (errmsg("standby_read_buf_new read block invalid %u/%u/%u/%hd/%hu %d %u",
                buf_tag.rnode.spcNode,
                buf_tag.rnode.dbNode,
                buf_tag.rnode.relNode,
                buf_tag.rnode.bucketNode,
                buf_tag.rnode.opt,
                buf_tag.forkNum,
                buf_tag.blockNum)));
    }

    if (!is_block_meta_info_valid(block_info)) {
        UnlockReleaseBuffer(block_info_buf);
        return read_buf;
    }

    if (block_info->max_lsn < read_lsn) {
        UnlockReleaseBuffer(block_info_buf);
        return read_buf;
    }

    // find nearest base page
    LsnInfoPosition base_page_pos = get_nearest_base_page_pos(buf_tag, block_info->base_page_info_list, read_lsn);
    if (base_page_pos == LSN_INFO_LIST_HEAD) {
        UnlockReleaseBuffer(block_info_buf);
        return read_buf;
    }
    ReleaseBuffer(read_buf);

    extreme_rto::RedoItemTag redo_item_tag;
    INIT_REDO_ITEM_TAG(redo_item_tag, buf_tag.rnode, buf_tag.forkNum, buf_tag.blockNum);
    const uint32 worker_num_per_mng = (uint32)extreme_rto::get_page_redo_worker_num_per_manager();
    /* batch id and worker id start from 1 when reading a page */
    uint32 batch_id = extreme_rto::GetSlotId(buf_tag.rnode, 0, 0, (uint32)extreme_rto::get_batch_redo_num()) + 1;
    uint32 redo_worker_id = extreme_rto::GetWorkerId(&redo_item_tag, worker_num_per_mng) + 1;

    Buffer base_page_buffer = buffer_read_base_page(batch_id, redo_worker_id, base_page_pos, RBM_NORMAL);
    bool hit = false;
    LockBuffer(base_page_buffer, BUFFER_LOCK_SHARE);
    ResourceOwnerEnlargeBuffers(t_thrd.utils_cxt.CurrentResourceOwner);

    Page base_page = BufferGetPage(base_page_buffer);
    XLogRecPtr base_page_lsn = PageGetLSN(base_page);
    BufferDesc *buf_desc = alloc_standby_read_buf(buf_tag, strategy, hit, base_page_lsn, false);

    if (hit) {
        UnlockReleaseBuffer(block_info_buf);
        UnlockReleaseBuffer(base_page_buffer);
        return BufferDescriptorGetBuffer(buf_desc);
    }

    Page read_page = (Page)BufHdrGetBlock(buf_desc);
    errno_t rc = memcpy_s(read_page, BLCKSZ, base_page, BLCKSZ);
    securec_check(rc, "\0", "\0");

    buf_desc->extra->lsn_on_disk = PageGetLSN(read_page);
#ifdef USE_ASSERT_CHECKING
    buf_desc->lsn_dirty = InvalidXLogRecPtr;
#endif

    TerminateBufferIO(buf_desc, false, (BM_VALID | BM_IS_TMP_BUF));
    UnlockReleaseBuffer(block_info_buf);
    UnlockReleaseBuffer(base_page_buffer);
    return BufferDescriptorGetBuffer(buf_desc);
}
}  // namespace extreme_rto_standby_read
