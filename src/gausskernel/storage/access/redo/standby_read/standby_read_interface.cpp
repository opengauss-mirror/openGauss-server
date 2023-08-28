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

#include <cassert>
#include "access/extreme_rto/page_redo.h"
#include "access/extreme_rto/standby_read/block_info_meta.h"
#include "access/extreme_rto/standby_read/lsn_info_meta.h"
#include "access/multi_redo_api.h"
#include "pgstat.h"
#include "storage/smgr/relfilenode.h"
#include "storage/buf/buf_internals.h"
#include "storage/buf/bufmgr.h"
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

void make_standby_read_node(XLogRecPtr read_lsn, RelFileNode& read_node)
{
    read_node.spcNode = (Oid)(read_lsn >> 32);
    read_node.dbNode = (Oid)(read_lsn);
    read_node.relNode = InvalidOid;  // make sure it can be InvalidOid or not 
    read_node.opt = 0;
    read_node.bucketNode = InvalidBktId;
}

BufferDesc* alloc_standby_read_buf(
    const BufferTag& buf_tag, BufferAccessStrategy strategy, bool& found, XLogRecPtr read_lsn)
{
    RelFileNode read_node;
    make_standby_read_node(read_lsn, read_node);
    BufferDesc* buf_desc = BufferAlloc(read_node, 0, buf_tag.forkNum, buf_tag.blockNum, strategy, &found, NULL);

    return buf_desc;
}

Buffer get_newest_page_for_read(Relation reln, ForkNumber fork_num, BlockNumber block_num, ReadBufferMode mode,
    BufferAccessStrategy strategy, XLogRecPtr read_lsn)
{
    bool hit = false;

    Buffer newest_buf = ReadBuffer_common(
        reln->rd_smgr, reln->rd_rel->relpersistence, fork_num, block_num, mode, strategy, &hit, NULL);
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
    BufferDesc* buf_desc = alloc_standby_read_buf(buf_tag, strategy, hit, page_lsn);

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
    XLogRecPtr read_lsn = t_thrd.proc->exrto_read_lsn;
    if (read_lsn == InvalidXLogRecPtr) {
        Assert(IsSystemRelation(reln));
        read_lsn = MAX_XLOG_REC_PTR;
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
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 (errmsg("standby_read_buf couldnot found buf %u/%u/%u %d %u read lsn %lu", buf_tag.rnode.spcNode,
                         buf_tag.rnode.dbNode, buf_tag.rnode.relNode, buf_tag.forkNum, buf_tag.blockNum, read_lsn))));
        return InvalidBuffer;
    }

    // read lsn info
    XLogRecPtr expected_lsn = InvalidXLogRecPtr;
    if (lsn_info->lsn_num == 0) {
        expected_lsn = lsn_info->base_page_lsn;
    } else {
        Assert(lsn_info->lsn_array[lsn_info->lsn_num - 1] > 0);
        Assert(lsn_info->lsn_array[lsn_info->lsn_num - 1] < read_lsn);
        Assert(lsn_info->lsn_array[lsn_info->lsn_num - 1] >= lsn_info->base_page_lsn);
        expected_lsn = lsn_info->lsn_array[lsn_info->lsn_num - 1];
    }

    BufferDesc* buf_desc = alloc_standby_read_buf(buf_tag, strategy, hit, expected_lsn);

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
}

Datum gs_hot_standby_space_info(PG_FUNCTION_ARGS)
{
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
        extreme_rto::PageRedoWorker* page_redo_worker = extreme_rto::g_dispatcher->allWorkers[i];
        if (page_redo_worker->role != extreme_rto::REDO_PAGE_WORKER) {
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
    struct dirent *de = NULL;
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
}

