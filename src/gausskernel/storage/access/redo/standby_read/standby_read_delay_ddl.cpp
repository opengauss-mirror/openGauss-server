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
 * standby_read_delay_ddl.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/redo/standby_read/standby_read_delay_ddl.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <cassert>
#include <sys/types.h>
#include <unistd.h>
#include "access/extreme_rto/standby_read.h"
#include "access/extreme_rto/standby_read/standby_read_delay_ddl.h"
#include "access/extreme_rto/standby_read/block_info_meta.h"
#include "access/multi_redo_api.h"
#include "commands/dbcommands.h"
#include "access/slru.h"
#include "access/twophase.h"
#include "storage/procarray.h"

#define DELAY_DDL_FILE_DIR "delay_ddl"
#define DELAY_DDL_FILE_NAME "delay_ddl/delay_delete_info_file"

typedef enum {
    DROP_DB_TYPE = 1,
    DROP_TABLE_TYPE,
    TRUNCATE_CLOG,
} DropDdlType;

#define ClogCtl(n) (&t_thrd.shemem_ptr_cxt.ClogCtl[CBufHashPartition(n)])
 
const static uint32 MAX_NUM_PER_FILE = 0x10000;

typedef struct {
    uint8 type;
    uint8 len;
    uint16 resvd1;
    uint32 resvd2;
    union {
        ColFileNode file_info;
        int64 pageno;
    } node_info;
    XLogRecPtr lsn;
    pg_crc32 crc;
} DelayDdlInfo;

void init_delay_ddl_file()
{
    if (isDirExist(DELAY_DDL_FILE_DIR)) {
        return;
    }

    if (!IS_EXRTO_READ) {
        return;
    }

    if (mkdir(DELAY_DDL_FILE_DIR, S_IRWXU) < 0 && errno != EEXIST) {
        ereport(ERROR,
            (errcode_for_file_access(), errmsg("could not create directory \"%s\": %m", DELAY_DDL_FILE_DIR)));
    }
}

int read_delay_ddl_info(int fd, void* buf, size_t size, off_t off)
{
    int count = 0;
RETRY:
    errno = 0;
    int return_code = pread(fd, buf, size, off);
    if (return_code < 0) {
        /* OK to retry if interrupted */
        if (errno == EINTR) {
            goto RETRY;
        }

        if (errno == EIO) {
            if (count < EIO_RETRY_TIMES) {
                count++;
                ereport(WARNING,
                    (errmsg("delete_by_lsn: failed (read len %lu, offset %ld), retry:Input/Output ERROR", size, off)));
                goto RETRY;
            }
        }
    }

    return return_code;
}

bool write_delay_ddl_info(char* file_path, void* buf, size_t size, off_t off)
{
    int fd = BasicOpenFile(file_path, O_CREAT | O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        ereport(WARNING,
            (errcode_for_file_access(), errmsg("write_delay_ddl_info:could not open file %s : %s", file_path, TRANSLATE_ERRNO)));
        return false;
    }
    int count = 0;
RETRY:
    errno = 0;
    int return_code = pwrite(fd, buf, size, off);
    if (return_code != (int)size && errno == 0) {
        errno = ENOSPC;
    }

    /* OK to retry if interrupted */
    if (errno == EINTR) {
        goto RETRY;
    }
    if (errno == EIO) {
        if (count < EIO_RETRY_TIMES) {
            count++;
            ereport(WARNING,
                (errmsg("write_delay_ddl_info: write %s failed, then retry: Input/Output ERROR", file_path)));
            goto RETRY;
        }
    }

    if (return_code != (int)size) {
        ereport(WARNING, (errcode_for_file_access(),
            errmsg("write_delay_ddl_info:write maybe failed %s ,write %d, need %lu, offset %ld: %m",
            file_path, return_code, size, off)));
        close(fd);
        return false;
    }

    if (fsync(fd) != 0) {
        ereport(WARNING,
            (errcode_for_file_access(), errmsg("write_delay_ddl_info:could not fsync file %s: %s", file_path, TRANSLATE_ERRNO)));
        close(fd);
        return false;
    }

    close(fd);
    return true;
}


static void enter_state(uint32 *stat)
{
    uint32 expected = 0;
    while (!pg_atomic_compare_exchange_u32(stat, &expected, 1)) {
        expected = 0;
        RedoInterruptCallBack();
    }
}

static void exit_state(uint32 *stat)
{
    (void)pg_atomic_sub_fetch_u32(stat, 1);
}

void update_delay_ddl_db(Oid db_id, Oid tablespace_id, XLogRecPtr lsn)
{
    StandbyReadDelayDdlState *stat = &g_instance.comm_cxt.predo_cxt.standby_read_delay_ddl_stat;
    enter_state(&stat->insert_stat);
    uint64 insert_start = pg_atomic_read_u64(&stat->next_index_can_insert);

    char path[MAXPGPATH];
    errno_t errorno = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, DELAY_DDL_FILE_NAME "_%08X_%lX",
        t_thrd.shemem_ptr_cxt.ControlFile->timeline, insert_start / MAX_NUM_PER_FILE);
    securec_check_ss(errorno, "", "");

    off_t off_set = (off_t)(insert_start % MAX_NUM_PER_FILE * sizeof(DelayDdlInfo));

    DelayDdlInfo tmp_info = {
        .type = DROP_DB_TYPE,
        .len = sizeof(DelayDdlInfo),
        .resvd1 = 0,
        .resvd2 = 0,
    };

    tmp_info.node_info.file_info.filenode.dbNode  = db_id;
    tmp_info.node_info.file_info.filenode.spcNode  = tablespace_id;
    tmp_info.lsn = lsn;
    INIT_CRC32C(tmp_info.crc);
    COMP_CRC32C(tmp_info.crc, (char*)&tmp_info, offsetof(DelayDdlInfo, crc));
    FIN_CRC32C(tmp_info.crc);

    if (write_delay_ddl_info(path, &tmp_info, sizeof(DelayDdlInfo), off_set)) {
        pg_atomic_write_u64(&stat->next_index_can_insert, insert_start + 1);
    }
    exit_state(&stat->insert_stat);
}

void update_delay_ddl_files(ColFileNode* xnodes, int nrels, XLogRecPtr lsn)
{
    DelayDdlInfo* info_list = (DelayDdlInfo*)palloc(sizeof(DelayDdlInfo) * (uint32)nrels);
    for (int i = 0; i < nrels; ++i) {
        info_list[i].type = DROP_TABLE_TYPE;
        info_list[i].len = sizeof(DelayDdlInfo);
        info_list[i].resvd1 = 0;
        info_list[i].resvd2 = 0;
        info_list[i].node_info.file_info = xnodes[i];
        info_list[i].lsn = lsn;
        INIT_CRC32C(info_list[i].crc);
        COMP_CRC32C(info_list[i].crc, (char*)&info_list[i], offsetof(DelayDdlInfo, crc));
        FIN_CRC32C(info_list[i].crc);
    }
    uint32 remains = (uint32)nrels;
    StandbyReadDelayDdlState *stat = &g_instance.comm_cxt.predo_cxt.standby_read_delay_ddl_stat;
    enter_state(&stat->insert_stat);
    uint64 insert_start = pg_atomic_read_u64(&stat->next_index_can_insert);

    DelayDdlInfo *start = info_list;
    while (remains > 0) {
        uint32 left_size = MAX_NUM_PER_FILE - insert_start % MAX_NUM_PER_FILE;
        uint32 copys = remains;
        if (remains > left_size) {
            copys = (uint32)left_size;
        }

        char path[MAXPGPATH];
        errno_t errorno = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, DELAY_DDL_FILE_NAME "_%08X_%lX",
        t_thrd.shemem_ptr_cxt.ControlFile->timeline, insert_start / MAX_NUM_PER_FILE);
        securec_check_ss(errorno, "", "");

        off_t off_set = (off_t)(insert_start % MAX_NUM_PER_FILE * sizeof(DelayDdlInfo));

        if (write_delay_ddl_info(path, start, copys * sizeof(DelayDdlInfo), off_set)) {
            remains -= copys;
            start += copys;
            insert_start += copys;
        } else {
            break;
        }
        RedoInterruptCallBack();
    }
    pfree(info_list);

    pg_atomic_write_u64(&stat->next_index_can_insert, insert_start);
    exit_state(&stat->insert_stat);
}

void update_delay_ddl_file_truncate_clog(XLogRecPtr lsn, int64 pageno)
{
    StandbyReadDelayDdlState *stat = &g_instance.comm_cxt.predo_cxt.standby_read_delay_ddl_stat;
    enter_state(&stat->insert_stat);
    uint64 insert_start = pg_atomic_read_u64(&stat->next_index_can_insert);
 
    char path[MAXPGPATH];
    errno_t errorno = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, DELAY_DDL_FILE_NAME "_%08X_%lX",
        t_thrd.shemem_ptr_cxt.ControlFile->timeline, insert_start / MAX_NUM_PER_FILE);
    securec_check_ss(errorno, "", "");
 
    off_t off_set = (off_t)(insert_start % MAX_NUM_PER_FILE * sizeof(DelayDdlInfo));
 
    DelayDdlInfo truncate_clog_info = {0};
    truncate_clog_info.type = TRUNCATE_CLOG;
    truncate_clog_info.len = sizeof(DelayDdlInfo);
    truncate_clog_info.node_info.pageno = pageno;
    truncate_clog_info.lsn = lsn;
    INIT_CRC32C(truncate_clog_info.crc);
    COMP_CRC32C(truncate_clog_info.crc, (char*)&truncate_clog_info, offsetof(DelayDdlInfo, crc));
    FIN_CRC32C(truncate_clog_info.crc);
 
    if (write_delay_ddl_info(path, &truncate_clog_info, sizeof(DelayDdlInfo), off_set)) {
        pg_atomic_write_u64(&stat->next_index_can_insert, insert_start + 1);
    }
    exit_state(&stat->insert_stat);
}

void clog_truncate_cancel_conflicting_proc(TransactionId latest_removed_xid, XLogRecPtr lsn)
{
    const int max_check_times = 1000;
    int check_times = 0;
    bool conflict = true;
    bool reach_max_check_times = false;
    while (conflict && check_times < max_check_times) {
        RedoInterruptCallBack();
        check_times++;
        reach_max_check_times = (check_times == max_check_times);
        conflict = proc_array_cancel_conflicting_proc(latest_removed_xid, lsn, reach_max_check_times);
    }
}

void do_truncate_clog(int64 pageno)
{
    ClogCtl(pageno)->shared->latest_page_number = pageno;

    TransactionId truncate_xid = (TransactionId)PAGE_TO_TRANSACTION_ID(pageno);
    clog_truncate_cancel_conflicting_proc(truncate_xid, InvalidXLogRecPtr);
    if (TransactionIdPrecedes(g_instance.undo_cxt.hotStandbyRecycleXid, truncate_xid)) {
        pg_atomic_write_u64(&g_instance.undo_cxt.hotStandbyRecycleXid, truncate_xid);
    }

    SimpleLruTruncate(ClogCtl(0), pageno, NUM_CLOG_PARTITIONS);
    DeleteObsoleteTwoPhaseFile(pageno);
}

void do_delay_ddl(DelayDdlInfo *info, bool is_old_delay_ddl = false)
{
    pg_crc32c crc_check;
    INIT_CRC32C(crc_check);
    COMP_CRC32C(crc_check, (char*)info, offsetof(DelayDdlInfo, crc));
    FIN_CRC32C(crc_check);

    if (!EQ_CRC32C(crc_check, info->crc)) {
        ereport(WARNING, (errcode_for_file_access(),
            errmsg("delay ddl ,crc(%u:%u) check error, maybe is type:%u, info %u/%u/%u lsn:%lu", crc_check, info->crc,
            (uint32)info->type, info->node_info.file_info.filenode.spcNode, info->node_info.file_info.filenode.dbNode,
            info->node_info.file_info.filenode.relNode, info->lsn)));
        return;
    }

    if (info->type == DROP_TABLE_TYPE) {
        ereport(DEBUG2,
            (errmodule(MOD_STANDBY_READ),
                errmsg("delay ddl for table, type:%u, info %u/%u/%u lsn:%lu",
                    (uint32)info->type,
                    info->node_info.file_info.filenode.spcNode,
                    info->node_info.file_info.filenode.dbNode,
                    info->node_info.file_info.filenode.relNode,
                    info->lsn)));
        unlink_relfiles(&info->node_info.file_info, 1, is_old_delay_ddl);
        xact_redo_log_drop_segs(&info->node_info.file_info, 1, info->lsn);
    } else if (info->type == DROP_DB_TYPE) {
        ereport(DEBUG2,
            (errmodule(MOD_STANDBY_READ),
                errmsg("delay ddl for database, type:%u, info %u/%u lsn:%lu",
                    (uint32)info->type,
                    info->node_info.file_info.filenode.spcNode,
                    info->node_info.file_info.filenode.dbNode,
                    info->lsn)));
        do_db_drop(info->node_info.file_info.filenode.dbNode, info->node_info.file_info.filenode.spcNode);
    } else if (info->type == TRUNCATE_CLOG) {
        ereport(LOG,
            (errmodule(MOD_STANDBY_READ), errmsg("delay ddl for truncate clog, pageno: %ld", info->node_info.pageno)));
        UpdateMinRecoveryPoint(info->lsn, false);
        do_truncate_clog(info->node_info.pageno);
    } else {
        ereport(WARNING,
            (errcode_for_file_access(),
                errmsg("delay ddl ,type error, maybe is type:%u, info %u/%u/%u lsn:%lu",
                    (uint32)info->type,
                    info->node_info.file_info.filenode.spcNode,
                    info->node_info.file_info.filenode.dbNode,
                    info->node_info.file_info.filenode.relNode,
                    info->lsn)));
    }
}

void delete_by_lsn(XLogRecPtr lsn)
{
    StandbyReadDelayDdlState *stat = &g_instance.comm_cxt.predo_cxt.standby_read_delay_ddl_stat;
    enter_state(&stat->delete_stat);

    uint64 next_delete = pg_atomic_read_u64(&stat->next_index_need_unlink);
    uint64 next_insert = pg_atomic_read_u64(&stat->next_index_can_insert);
    DelayDdlInfo* info_list = (DelayDdlInfo*)palloc(sizeof(DelayDdlInfo) * MAX_NUM_PER_FILE);
    bool go_on_delete = true;
    uint64 deleted_total = 0;
    while (next_delete < next_insert && go_on_delete) {
        uint32 cur_deleted = MAX_NUM_PER_FILE;

        /* same file */
        if ((next_delete / MAX_NUM_PER_FILE) == (next_insert / MAX_NUM_PER_FILE)) {
            cur_deleted = next_insert - next_delete;
        } else { /* different file */
            cur_deleted = MAX_NUM_PER_FILE - next_delete % MAX_NUM_PER_FILE;
        }

        uint64 offset = next_delete % MAX_NUM_PER_FILE * sizeof(DelayDdlInfo);

        char path[MAXPGPATH];
        errno_t errorno = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, DELAY_DDL_FILE_NAME "_%08X_%lX",
        t_thrd.shemem_ptr_cxt.ControlFile->timeline, next_delete / MAX_NUM_PER_FILE);
        securec_check_ss(errorno, "", "");

        int fd = BasicOpenFile(path, O_RDONLY | PG_BINARY, S_IRUSR | S_IWUSR);
        if (fd < 0) {
            exit_state(&stat->delete_stat);
            return;
        }
        int count = read_delay_ddl_info(fd, info_list, cur_deleted * sizeof(DelayDdlInfo), (off_t)offset);
        close(fd);
        if (count <= 0) {
            ereport(WARNING,
                (errmsg("delete_by_lsn: file %s nothing deleted_total", path)));
            go_on_delete = false;
            continue;
        }
        if ((uint32)count / sizeof(DelayDdlInfo) < cur_deleted) {
            cur_deleted = (uint32)count / sizeof(DelayDdlInfo);
            ereport(WARNING,
                    (errmsg("delete_by_lsn: the info in file %s is less than expected, cur_deleted: %u, next_delete: "
                            "%lu, next_insert: %lu",
                            path, cur_deleted, next_delete, next_insert)));
        }

        for (uint32 i = 0; i < cur_deleted; ++i) {
            if (info_list[i].lsn <= lsn) {
                do_delay_ddl(&info_list[i]);
            } else {
                cur_deleted = i;
                go_on_delete = false;
                break;
            }
            RedoInterruptCallBack();
        }

        next_delete += cur_deleted;
        deleted_total += cur_deleted;
        if (next_delete % MAX_NUM_PER_FILE == 0) {
            (void)unlink(path);
            ereport(LOG, (errmsg("delete delay ddl file end [%s:%d:%s]", __FUNCTION__, __LINE__, path)));
        }
        RedoInterruptCallBack();
    }
    ereport(LOG, (errmsg("delete_by_lsn: unlink files number: %lu", deleted_total)));
    pfree(info_list);
    pg_atomic_write_u64(&stat->next_index_need_unlink, next_delete);
    exit_state(&stat->delete_stat);
}

void delete_by_table_space(Oid tablespace_id)
{
    StandbyReadDelayDdlState *stat = &g_instance.comm_cxt.predo_cxt.standby_read_delay_ddl_stat;
    enter_state(&stat->delete_stat);
    uint64 next_delete = pg_atomic_read_u64(&stat->next_index_need_unlink);
    uint64 next_insert = pg_atomic_read_u64(&stat->next_index_can_insert);

    ereport(LOG, (errmsg("delete_by_table_space start")));

    DelayDdlInfo* info_list = (DelayDdlInfo*)palloc0(sizeof(DelayDdlInfo) * MAX_NUM_PER_FILE);
    while (next_delete < next_insert) {
        uint32 copys = MAX_NUM_PER_FILE;

        // same file
        if ((next_delete / MAX_NUM_PER_FILE) == (next_insert / MAX_NUM_PER_FILE)) {
            copys = next_insert - next_delete;
        } else { /* different file */
            copys = MAX_NUM_PER_FILE - next_delete % MAX_NUM_PER_FILE;
        }

        uint64 offset = next_delete % MAX_NUM_PER_FILE * sizeof(DelayDdlInfo);
        char path[MAXPGPATH];
        errno_t errorno = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, DELAY_DDL_FILE_NAME "_%08X_%lX",
        t_thrd.shemem_ptr_cxt.ControlFile->timeline, next_delete / MAX_NUM_PER_FILE);
        securec_check_ss(errorno, "", "");

        int fd = BasicOpenFile(path, O_RDONLY | PG_BINARY, S_IRUSR | S_IWUSR);
        if (fd < 0) {
            ereport(WARNING,
                (errmsg("delete_by_table_space: file %s could not open:%s", path, TRANSLATE_ERRNO)));
            exit_state(&stat->delete_stat);
            return;
        }

        int count = read_delay_ddl_info(fd, info_list, copys * sizeof(DelayDdlInfo), (off_t)offset);
        if (count <= 0) {
            ereport(WARNING,
                (errmsg("delete_by_table_space: file %s nothing  deleted", path)));
            exit_state(&stat->delete_stat);
            return;
        }
        close(fd);

        if ((uint32)count / sizeof(DelayDdlInfo) < copys) {
            copys = (uint32)count / sizeof(DelayDdlInfo);
            ereport(
                WARNING,
                (errmsg("delete_by_table_space: the info in file %s is less than expected, copys: %u, next_delete: "
                        "%lu, next_insert: %lu",
                        path, copys, next_delete, next_insert)));
        }

        for (uint32 i = 0; i < copys; ++i) {
            if (info_list[i].node_info.file_info.filenode.spcNode == tablespace_id) {
                do_delay_ddl(&info_list[i]);
            }
            RedoInterruptCallBack();
        }
        next_delete += copys;
        RedoInterruptCallBack();
    }
    pfree(info_list);
    exit_state(&stat->delete_stat);
    ereport(LOG, (errmsg("delete_by_table_space end")));
}

void do_all_old_delay_ddl()
{
    DIR* file_dir = AllocateDir(DELAY_DDL_FILE_DIR);
    struct dirent* file_dirent = NULL;
    uint32 timeline = 0;
    uint64 segment = 0;
    while ((file_dirent = ReadDir(file_dir, DELAY_DDL_FILE_DIR)) != NULL) {
        int nmatch = sscanf_s(file_dirent->d_name, "delay_delete_info_file_%08X_%lX", &timeline, &segment);
        if (nmatch != 2) {
            continue;
        }
        if (timeline >= t_thrd.shemem_ptr_cxt.ControlFile->timeline && RecoveryInProgress()) {
            continue;
        }
        DelayDdlInfo* info_list = (DelayDdlInfo*)palloc0(sizeof(DelayDdlInfo) * MAX_NUM_PER_FILE);
        char path[MAXPGPATH];
        errno_t errorno = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, DELAY_DDL_FILE_DIR "/%s", file_dirent->d_name);
        securec_check_ss(errorno, "", "");

        int fd = BasicOpenFile(path, O_RDONLY | PG_BINARY, S_IRUSR | S_IWUSR);
        if (fd < 0) {
            ereport(WARNING,
                (errcode_for_file_access(), errmsg("open_delay_store_file:could not open file \"%s\": %m", path)));
        }

        int count = read_delay_ddl_info(fd, info_list, MAX_NUM_PER_FILE * sizeof(DelayDdlInfo), 0);
        close(fd);
        if (count <= 0) {
            return;
        }

        for (uint32 i = 0; i < (uint32)(count / sizeof(DelayDdlInfo)); ++i) {
            if (XLByteLE(info_list[i].lsn, t_thrd.shemem_ptr_cxt.ControlFile->checkPointCopy.redo)) {
                do_delay_ddl(&info_list[i]);
            }
            RedoInterruptCallBack();
        }

        (void)unlink(path);
        ereport(LOG, (errmsg("delete delay ddl file end [%s:%d:%s]", __FUNCTION__, __LINE__, path)));
        pfree(info_list);
        RedoInterruptCallBack();
    }

    if (!RecoveryInProgress()) {
        g_instance.comm_cxt.predo_cxt.standby_read_delay_ddl_stat.next_index_can_insert = 0;
        g_instance.comm_cxt.predo_cxt.standby_read_delay_ddl_stat.next_index_need_unlink = 0;
        g_instance.comm_cxt.predo_cxt.standby_read_delay_ddl_stat.delete_stat = 0;
        g_instance.comm_cxt.predo_cxt.standby_read_delay_ddl_stat.insert_stat = 0;
    }
}