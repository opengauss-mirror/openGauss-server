/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
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
 * pagerepair.cpp
 *		Working mode of pagerepair thread, copy the data page from the primary.
 *
 * IDENTIFICATION
 *      src/gausskernel/process/postmaster/pagerepair.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "access/xlog_basic.h"
#include "access/xlog_internal.h"
#include "access/multi_redo_api.h"
#include "access/extreme_rto/page_redo.h"
#include "access/parallel_recovery/page_redo.h"
#include "access/parallel_recovery/dispatcher.h"
#include "catalog/catalog.h"
#include "gssignal/gs_signal.h"
#include "knl/knl_instance.h"
#include "service/remote_read_client.h"
#include "storage/ipc.h"
#include "storage/copydir.h"
#include "storage/lmgr.h"
#include "storage/remote_read.h"
#include "storage/smgr/fd.h"
#include "pgstat.h"
#include "postmaster/pagerepair.h"
#include "utils/plog.h"
#include "utils/plog.h"
#include "utils/inval.h"

const int MAX_THREAD_NAME_LEN = 64;
const int XLOG_LSN_SWAP = 32;
const int TEN_MILLISECOND = 10;
#define MAX(A, B) ((B) > (A) ? (B) : (A))
#define FILE_REPAIR_LOCK g_instance.repair_cxt.file_repair_hashtbl_lock

typedef struct XLogPageReadPrivate {
    int emode;
    bool fetching_ckpt; /* are we fetching a checkpoint record? */
    bool randAccess;
} XLogPageReadPrivate;

/* --------------------------------
 *		signal handler routines
 * --------------------------------
 */
static void SetupPageRepairSignalHook(void);
static void PageRepairSigHupHandler(SIGNAL_ARGS);
static void PageRepairSigUsr1Handler(SIGNAL_ARGS);
static void PageRepairSigUsr2Handler(SIGNAL_ARGS);
static void PageRepairShutDownHandler(SIGNAL_ARGS);
static void PageRepairQuickDie(SIGNAL_ARGS);
static void PageRepairHandleInterrupts(void);

static void SeqRemoteReadPage();
static void SeqRemoteReadFile();
static void checkOtherFile(RepairFileKey key, uint32 max_segno, uint64 size);
static void PushBadFileToRemoteHashTbl(RepairFileKey key);

#define COMPARE_REPAIR_PAGE_KEY(key1, key2)                                                  \
    ((key1).relfilenode.relNode == (key2).relfilenode.relNode &&                             \
     (key1).relfilenode.dbNode == (key2).relfilenode.dbNode &&                               \
     (key1).relfilenode.spcNode == (key2).relfilenode.spcNode &&                             \
     (key1).relfilenode.bucketNode == (key2).relfilenode.bucketNode &&                       \
     (key1).relfilenode.opt == (key2).relfilenode.opt && (key1).forknum == (key2).forknum && \
     (key1).blocknum == (key2).blocknum)

#define NOT_SUPPORT_PAGE_REPAIR \
    (g_instance.attr.attr_common.cluster_run_mode == RUN_MODE_STANDBY ||        \
     g_instance.attr.attr_common.stream_cluster_run_mode == RUN_MODE_STANDBY || \
     t_thrd.xlog_cxt.is_hadr_main_standby || t_thrd.xlog_cxt.is_cascade_standby)

int CheckBlockLsn(XLogReaderState *xlogreader, RepairBlockKey key, XLogRecPtr page_old_lsn, XLogRecPtr *last_lsn)
{
    RepairBlockKey temp_key = {0};
    bool page_found = false;
    bool getlsn = false;

    for (int block_id = 0; block_id <= xlogreader->max_block_id; block_id++) {
        XLogRecGetBlockTag(xlogreader, block_id, &temp_key.relfilenode, &temp_key.forknum, &temp_key.blocknum);
        if (COMPARE_REPAIR_PAGE_KEY(key, temp_key)) {
            page_found = true;
            getlsn = XLogRecGetBlockLastLsn(xlogreader, block_id, last_lsn);
            Assert(getlsn);
            if (XLogRecPtrIsInvalid(*last_lsn)) {
                ereport(LOG,
                    (errmsg("check the repair page successfully, last_lsn is 0,"
                        "the page %u/%u/%u bucketnode %d, forknum is %u, blocknum is %u",
                            key.relfilenode.spcNode, key.relfilenode.dbNode, key.relfilenode.relNode,
                            key.relfilenode.bucketNode, key.forknum, key.blocknum)));
                return CHECK_SUCCESS;
            }
            /* if the xlog record last_lsn equal the current standby page lsn, means found a complete xlog chain */
            if (*last_lsn == page_old_lsn) {
                ereport(LOG,
                    (errmsg("check the repair page successfully, the page %u/%u/%u bucketnode %d, "
                        "forknum is %u, blocknum is %u",
                            key.relfilenode.spcNode, key.relfilenode.dbNode, key.relfilenode.relNode,
                            key.relfilenode.bucketNode, key.forknum, key.blocknum)));
                return CHECK_SUCCESS;
            }
            /* if the xlog record last lsn */
            if (*last_lsn < page_old_lsn) {
                ereport(WARNING,
                    (errmsg("check the repair page, lsn not match, page_old_lsn is %X/%X, last_lsn is %X/%X, "
                        "could not repair the page %u/%u/%u bucketnode %d, forknum is %u, blocknum is %u",
                        (uint32)(page_old_lsn >> XLOG_LSN_SWAP), (uint32)page_old_lsn,
                        (uint32)(*last_lsn >> XLOG_LSN_SWAP), (uint32)*last_lsn,
                        key.relfilenode.spcNode, key.relfilenode.dbNode, key.relfilenode.relNode,
                        key.relfilenode.bucketNode, key.forknum, key.blocknum)));
                return CHECK_FAIL;
            }
        }
    }

    if (!page_found) {
        ereport(WARNING,
            (errmsg("check the repair page, not get page info, page_old_lsn is %X/%X, last_lsn is %X/%X, "
                "could not repair the page %u/%u/%u bucketnode %d, forknum is %u, blocknum is %u",
                (uint32)(page_old_lsn >> XLOG_LSN_SWAP), (uint32)page_old_lsn,
                (uint32)(*last_lsn >> XLOG_LSN_SWAP), (uint32)*last_lsn,
                key.relfilenode.spcNode, key.relfilenode.dbNode, key.relfilenode.relNode,
                key.relfilenode.bucketNode, key.forknum, key.blocknum)));
        return CHECK_FAIL;
    }
    return NEED_CONTINUE_CHECK;
}

/* CheckPrimaryPageLSN
 *          Check whether the data page of the primary DN forms a complete xlog chain with the page of the standby DN.
 */
bool CheckPrimaryPageLSN(XLogRecPtr page_old_lsn, XLogRecPtr page_new_lsn, RepairBlockKey key)
{
    XLogRecPtr prev_lsn = InvalidXLogRecPtr;
    XLogRecPtr last_lsn = page_new_lsn;
    XLogRecord *record = NULL;
    char *errormsg = NULL;
    XLogReaderState *xlogreader = NULL;
    XLogPageReadPrivate readprivate;
    errno_t rc;
    int ret_code;

    rc = memset_s(&readprivate, sizeof(XLogPageReadPrivate), 0, sizeof(XLogPageReadPrivate));
    securec_check(rc, "", "");

    xlogreader = XLogReaderAllocate(&XLogPageRead, &readprivate);
    if (xlogreader == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory"),
                errdetail("Failed while allocating an XLog reading processor for pagerepair thread")));
    }

    xlogreader->system_identifier = t_thrd.shemem_ptr_cxt.ControlFile->system_identifier;
    t_thrd.xlog_cxt.recoveryTargetTLI = t_thrd.shemem_ptr_cxt.ControlFile->checkPointCopy.ThisTimeLineID;
    t_thrd.xlog_cxt.expectedTLIs = readTimeLineHistory(t_thrd.xlog_cxt.recoveryTargetTLI);

    /* page lsn is the xlog EndRecPtr, so need parse the next record, get the prev lsn */
    while (true) {
        if (0 == last_lsn % XLogSegSize) {
            XLByteAdvance(last_lsn, SizeOfXLogLongPHD);
        } else if (0 == last_lsn % XLOG_BLCKSZ) {
            XLByteAdvance(last_lsn, SizeOfXLogShortPHD);
        }
        record = XLogReadRecord(xlogreader, last_lsn, &errormsg);
        if (record == NULL) {
            ereport(WARNING,
                    (errmsg("check the repair page, page_old_lsn is %X/%X, could not get the xlog %X/%X "
                        "could not repair the page %u/%u/%u bucketnode %d, forknum is %u, blocknum is %u",
                        (uint32)(page_old_lsn >> XLOG_LSN_SWAP), (uint32)page_old_lsn,
                        (uint32)(last_lsn >> XLOG_LSN_SWAP), (uint32)last_lsn,
                        key.relfilenode.spcNode, key.relfilenode.dbNode, key.relfilenode.relNode,
                        key.relfilenode.bucketNode, key.forknum, key.blocknum)));
            return false;
        }

        prev_lsn = record->xl_prev;
        record = XLogReadRecord(xlogreader, prev_lsn, &errormsg);

        ret_code = CheckBlockLsn(xlogreader, key, page_old_lsn, &last_lsn);
        if (ret_code == CHECK_SUCCESS) {
            return true;
        } else if (ret_code == CHECK_FAIL) {
            return false;
        }
    }

    ereport(WARNING,
        (errmsg("check the repair page, could not found the page info from the xlog "
            "could not repair the page, page old lsn is %X/%X, last lsn is %X/%X, page new lsn is %X/%X"
            "page info is %u/%u/%u bucketnode %d, forknum is %u, blocknum is %u",
            (uint32)(page_old_lsn >> XLOG_LSN_SWAP), (uint32)page_old_lsn,
            (uint32)(last_lsn >> XLOG_LSN_SWAP), (uint32)last_lsn,
            (uint32)(page_new_lsn >> XLOG_LSN_SWAP), (uint32)page_new_lsn,
            key.relfilenode.spcNode, key.relfilenode.dbNode, key.relfilenode.relNode,
            key.relfilenode.bucketNode, key.forknum, key.blocknum)));

    return false;
}

void PageRepairHashTblInit(void)
{
    HASHCTL ctl;

    if (g_instance.repair_cxt.page_repair_hashtbl_lock == NULL) {
        g_instance.repair_cxt.page_repair_hashtbl_lock = LWLockAssign(LWTRANCHE_PAGE_REPAIR);
    }

    if (g_instance.repair_cxt.page_repair_hashtbl == NULL) {
        /* hash accessed by database file id */
        errno_t rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
        securec_check(rc, "", "");
        ctl.keysize = sizeof(RepairBlockKey);
        ctl.entrysize = sizeof(RepairBlockEntry);
        ctl.hash = tag_hash;
        ctl.hcxt = INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE);
        g_instance.repair_cxt.page_repair_hashtbl = hash_create("Page Repair Hash Table", MAX_REPAIR_PAGE_NUM, &ctl,
            HASH_ELEM | HASH_FUNCTION |HASH_CONTEXT);

        if (!g_instance.repair_cxt.page_repair_hashtbl)
            ereport(FATAL, (errmsg("could not initialize page repair Hash table")));
    }

    return;
}

void ClearPageRepairTheadMem(void)
{
    if (g_instance.repair_cxt.page_repair_hashtbl != NULL) {
        hash_destroy(g_instance.repair_cxt.page_repair_hashtbl);
        g_instance.repair_cxt.page_repair_hashtbl = NULL;
    }

    if (g_instance.repair_cxt.file_repair_hashtbl != NULL) {
        hash_destroy(g_instance.repair_cxt.file_repair_hashtbl);
        g_instance.repair_cxt.file_repair_hashtbl = NULL;
    }

    return;
}

/* CopyPageToRepairHashTbl
 *                After remote read, copy the page to hash table and update the page_new_lsn
 * of page repair hash table, means that the page is correct.
 */
void CopyPageToRepairHashTbl(RepairBlockEntry *entry, char *page_content)
{
    XLogRecPtr page_lsn = PageGetLSN(page_content);
    errno_t rc = 0;

    memcpy_s(entry->page_content, BLCKSZ, page_content, BLCKSZ);
    securec_check(rc, "", "");

    if (entry->error_type == CRC_CHECK_FAIL) {
        entry->page_new_lsn = page_lsn;
        entry->page_state = WAIT_REPAIR;
    } else {
        entry->page_state = WAIT_LSN_CHECK;
    }
    return;
}

void CheckPageLSN(RepairBlockKey key)
{
    HTAB *repair_hash = g_instance.repair_cxt.page_repair_hashtbl;
    XLogRecPtr standby_flush_lsn;
    RepairBlockEntry *entry = NULL;
    bool found = false;
    XLogRecPtr page_lsn;
    XLogRecPtr page_old_lsn;

    LWLockAcquire(g_instance.repair_cxt.page_repair_hashtbl_lock, LW_EXCLUSIVE);

    entry = (RepairBlockEntry*)hash_search(repair_hash, &(key), HASH_FIND, &found);
    if (found) {
        page_old_lsn = entry->page_old_lsn;
        page_lsn = PageGetLSN(entry->page_content);

        LWLockRelease(g_instance.repair_cxt.page_repair_hashtbl_lock);
        standby_flush_lsn = GetStandbyFlushRecPtr(NULL);
        if (XLByteLE(standby_flush_lsn, page_lsn)) {
            return;
        }
        /* after release the lock, check the page lsn where the error_type is LSN_CHECK_FAIL */
        bool check = CheckPrimaryPageLSN(page_old_lsn, page_lsn, key);
        if (check) {
            LWLockAcquire(g_instance.repair_cxt.page_repair_hashtbl_lock, LW_EXCLUSIVE);
            entry = (RepairBlockEntry*)hash_search(repair_hash, &(key), HASH_FIND, &found);
            if (found) {
                entry->page_new_lsn = page_lsn;
                entry->page_state = WAIT_REPAIR;
                (void)gs_signal_send(entry->recovery_tid, SIGUSR1);
            }
            LWLockRelease(g_instance.repair_cxt.page_repair_hashtbl_lock);
        } else {
            ereport(PANIC,
                (errmsg("check the repair page lsn failed, could not repair the page, "
                    "page old lsn is %X/%X, primary lsn is %X/%X, "
                    "page info is %u/%u/%u bucketnode %d, forknum is %u, blocknum is %u",
                    (uint32)(page_old_lsn >> XLOG_LSN_SWAP), (uint32)page_old_lsn,
                    (uint32)(page_lsn >> XLOG_LSN_SWAP), (uint32)page_lsn,
                    key.relfilenode.spcNode, key.relfilenode.dbNode, key.relfilenode.relNode,
                    key.relfilenode.bucketNode, key.forknum, key.blocknum)));
        }
    } else {
        LWLockRelease(g_instance.repair_cxt.page_repair_hashtbl_lock);
    }
    return;
}

int RemoteReadFileSizeNoError(RepairFileKey *key, int64 *size)
{
    /* get remote address */
    char remote_address1[MAXPGPATH] = {0}; /* remote_address1[0] = '\0'; */
    char remote_address2[MAXPGPATH] = {0}; /* remote_address2[0] = '\0'; */
    int timeout = 120;

    GetRemoteReadAddress(remote_address1, remote_address2, MAXPGPATH);
    char *remote_address = remote_address1;
    if (remote_address[0] == '\0' || remote_address[0] == ':') {
        ereport(DEBUG1, (errmodule(MOD_REMOTE), errmsg("remote not available")));
        return REMOTE_READ_IP_NOT_EXIST;
    }
    ereport(LOG, (errmodule(MOD_REMOTE), errmsg("remote read file size, file %s from %s",
                                                relpathperm(key->relfilenode, key->forknum),
                                                remote_address)));

    RemoteReadFileKey read_key;
    read_key.relfilenode = key->relfilenode;
    read_key.forknum = key->forknum;
    read_key.blockstart = 0;

    PROFILING_REMOTE_START();
    int retCode = RemoteGetFileSize(remote_address, &read_key, InvalidXLogRecPtr, size, timeout);
    /* return file size + primary lsn */
    PROFILING_REMOTE_END_READ(sizeof(uint64) + sizeof(uint64), (retCode == REMOTE_READ_OK));
    return retCode;
}

/* RemoteReadFile
 *             standby dn use this function repair file.
 */
int RemoteReadFileNoError(RemoteReadFileKey *key, char *buf, XLogRecPtr lsn, uint32 size,
    XLogRecPtr *remote_lsn, uint32 *remote_size)
{
    /* get remote address */
    char remote_address1[MAXPGPATH] = {0}; /* remote_address1[0] = '\0'; */
    char remote_address2[MAXPGPATH] = {0}; /* remote_address2[0] = '\0'; */
    char *remote_address = NULL;

    GetRemoteReadAddress(remote_address1, remote_address2, MAXPGPATH);
    remote_address = remote_address1;
    int timeout = 0;

    if (remote_address[0] == '\0' || remote_address[0] == ':') {
        ereport(WARNING, (errcode(ERRCODE_IO_ERROR), errmodule(MOD_REMOTE), errmsg("remote not available")));
        return REMOTE_READ_IP_NOT_EXIST;
    }
    ereport(LOG, (errmodule(MOD_REMOTE),
        errmsg("remote read file, file %s  from %s, block start is %u",
            relpathperm(key->relfilenode, key->forknum), remote_address, key->blockstart)));

    PROFILING_REMOTE_START();
    int retCode = RemoteGetFile(remote_address, key, lsn, size, buf, remote_lsn, remote_size, timeout);
    PROFILING_REMOTE_END_READ(size, (retCode == REMOTE_READ_OK));
    return retCode;
}

int RemoteReadBlockNoError(RepairBlockKey *key, char *buf, XLogRecPtr lsn, const XLogPhyBlock *pblk)
{
    /* get remote address */
    char remote_address1[MAXPGPATH] = {0}; /* remote_address1[0] = '\0'; */
    char remote_address2[MAXPGPATH] = {0}; /* remote_address2[0] = '\0'; */

    GetRemoteReadAddress(remote_address1, remote_address2, MAXPGPATH);
    char *remote_address = remote_address1;
    if (remote_address[0] == '\0' || remote_address[0] == ':') {
        ereport(DEBUG1, (errmodule(MOD_REMOTE), errmsg("remote not available")));
        return REMOTE_READ_IP_NOT_EXIST;
    }
    if (pblk != NULL) {
        ereport(LOG, (errmodule(MOD_REMOTE), errmsg("remote read page, file %s block %u (pblk %u/%d) from %s",
            relpathperm(key->relfilenode, key->forknum), key->blocknum, pblk->relNode, pblk->block, remote_address)));
    } else {
        ereport(LOG, (errmodule(MOD_REMOTE), errmsg("remote read page, file %s block %u from %s",
            relpathperm(key->relfilenode, key->forknum), key->blocknum, remote_address)));
    }

    const int TIMEOUT = 60;
    PROFILING_REMOTE_START();
    int retCode = RemoteGetPage(remote_address, key, BLCKSZ, lsn, buf, pblk, TIMEOUT);
    PROFILING_REMOTE_END_READ(BLCKSZ, (retCode == REMOTE_READ_OK));
    return retCode;
}

static void RepairPage(RepairBlockEntry *entry, char *page)
{
    int retCode = 0;

    if (entry->pblk.relNode != InvalidOid) {
        retCode = RemoteReadBlockNoError(&entry->key, page, entry->page_old_lsn, &entry->pblk);
    } else {
        retCode = RemoteReadBlockNoError(&entry->key, page, entry->page_old_lsn, NULL);
    }
    if (retCode == REMOTE_READ_OK) {
        CopyPageToRepairHashTbl(entry, page);
    }

    return;
}

const int MAX_CHECK_LSN_NUM = 100;
static void SeqRemoteReadPage()
{
    HTAB *repair_hash = g_instance.repair_cxt.page_repair_hashtbl;
    RepairBlockEntry *entry = NULL;
    HASH_SEQ_STATUS status;
    RepairBlockKey lsncheck[MAX_CHECK_LSN_NUM];
    int check_lsn_num = 0;
    int need_repair_num = 0;
    int repair_num = 0;
    char page[BLCKSZ] = {0};

    LWLockAcquire(g_instance.repair_cxt.page_repair_hashtbl_lock, LW_EXCLUSIVE);

    hash_seq_init(&status, repair_hash);
    while ((entry = (RepairBlockEntry *)hash_seq_search(&status)) != NULL) {
        need_repair_num++;
        switch (entry->page_state) {
            case WAIT_REMOTE_READ:
                RepairPage(entry, page);
                if (entry->error_type == LSN_CHECK_FAIL && entry->page_state == WAIT_LSN_CHECK &&
                    check_lsn_num < MAX_CHECK_LSN_NUM) {
                    lsncheck[check_lsn_num] = entry->key;
                    check_lsn_num++;
                }
                if (entry->page_state == WAIT_REPAIR) {
                    (void)gs_signal_send(entry->recovery_tid, SIGUSR1);
                    repair_num++;
                }
                break;
            case WAIT_LSN_CHECK:
                if (check_lsn_num < MAX_CHECK_LSN_NUM) {
                    lsncheck[check_lsn_num] = entry->key;
                    check_lsn_num++;
                }
                break;
            case WAIT_REPAIR:
                repair_num++;
                (void)gs_signal_send(entry->recovery_tid, SIGUSR1);
                break;
            default:
                ereport(ERROR, (errmsg("error page state during remote read")));
        }
    }

    LWLockRelease(g_instance.repair_cxt.page_repair_hashtbl_lock);

    for (int i = 0; i < check_lsn_num; i++) {
        RepairBlockKey temp = lsncheck[i];
        CheckPageLSN(temp);
    }
    if (need_repair_num == repair_num) {
        t_thrd.pagerepair_cxt.page_repair_requested = false;
    }
    return;
}

static void PageRepairHandleInterrupts(void)
{
    if (t_thrd.pagerepair_cxt.got_SIGHUP) {
        t_thrd.pagerepair_cxt.got_SIGHUP = false;
        ProcessConfigFile(PGC_SIGHUP);
    }

    if (t_thrd.pagerepair_cxt.shutdown_requested && g_instance.pid_cxt.StartupPID == 0) {
        ereport(LOG, (errmodule(MOD_REDO), errmsg("pagerepair thread shut down")));

        u_sess->attr.attr_common.ExitOnAnyError = true;
        proc_exit(0);
    }
}

void PageRepairMain(void)
{
    MemoryContext pagerepair_context;
    char name[MAX_THREAD_NAME_LEN] = {0};
    uint32 rc = 0;

    t_thrd.role = PAGEREPAIR_THREAD;

    SetupPageRepairSignalHook();

    /* We allow SIGQUIT (quickdie) at all times */
    (void)sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGQUIT);

    ereport(LOG, (errmodule(MOD_REDO), errmsg("pagerepair started")));

    /*
     * Create a resource owner to keep track of our resources (currently only
     * buffer pins).
     */
    errno_t err_rc = snprintf_s(
        name, MAX_THREAD_NAME_LEN, MAX_THREAD_NAME_LEN - 1, "%s", "PageRepair");
    securec_check_ss(err_rc, "", "");

    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, name,
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));

    /*
     * Create a memory context that we will do all our work in.  We do this so
     * that we can reset the context during error recovery and thereby avoid
     * possible memory leaks.  Formerly this code just ran in
     * TopMemoryContext, but resetting that would be a really bad idea.
     */
    pagerepair_context = AllocSetContextCreate(
        TopMemoryContext, name, ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    (void)MemoryContextSwitchTo(pagerepair_context);

    /*
     * Unblock signals (they were blocked when the postmaster forked us)
     */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    pgstat_report_appname("PageRepair");
    pgstat_report_activity(STATE_IDLE, NULL);

    /*
     * Loop forever
     */
    for (;;) {
        PageRepairHandleInterrupts();
        pgstat_report_activity(STATE_IDLE, NULL);
        rc = WaitLatch(&t_thrd.proc->procLatch, WL_TIMEOUT | WL_POSTMASTER_DEATH, (long)TEN_MILLISECOND);
        if (rc & WL_POSTMASTER_DEATH) {
            gs_thread_exit(1);
        }

        ResetLatch(&t_thrd.proc->procLatch);
        pgstat_report_activity(STATE_RUNNING, NULL);
        if (!t_thrd.pagerepair_cxt.shutdown_requested) {
            SeqRemoteReadPage();
            SeqRemoteReadFile();
        }
    }
}

static void SetupPageRepairSignalHook(void)
{
    /*
     * Reset some signals that are accepted by postmaster but not here
     */
    (void)gspqsignal(SIGHUP, PageRepairSigHupHandler);
    (void)gspqsignal(SIGINT, SIG_IGN);
    (void)gspqsignal(SIGTERM, PageRepairShutDownHandler);
    (void)gspqsignal(SIGQUIT, PageRepairQuickDie); /* hard crash time */
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, PageRepairSigUsr1Handler);
    (void)gspqsignal(SIGUSR2, PageRepairSigUsr2Handler);

    /*
     * Reset some signals that are accepted by postmaster but not here
     */
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);
}

static void PageRepairSigUsr1Handler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.pagerepair_cxt.page_repair_requested = true;
    if (t_thrd.proc) {
        SetLatch(&t_thrd.proc->procLatch);
    }

    errno = save_errno;
}

static void PageRepairSigUsr2Handler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.pagerepair_cxt.file_repair_requested = true;
    if (t_thrd.proc) {
        SetLatch(&t_thrd.proc->procLatch);
    }

    errno = save_errno;
}

static void PageRepairSigHupHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.pagerepair_cxt.got_SIGHUP = true;
    if (t_thrd.proc) {
        SetLatch(&t_thrd.proc->procLatch);
    }

    errno = save_errno;
}

static void PageRepairShutDownHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.pagerepair_cxt.shutdown_requested = true;
    if (t_thrd.proc) {
        SetLatch(&t_thrd.proc->procLatch);
    }

    errno = save_errno;
}

static void PageRepairQuickDie(SIGNAL_ARGS)
{
    gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL);

    /*
     * We DO NOT want to run proc_exit() callbacks -- we're here because
     * shared memory may be corrupted, so we don't want to try to clean up our
     * transaction.  Just nail the windows shut and get out of town.  Now that
     * there's an atexit callback to prevent third-party code from breaking
     * things by calling exit() directly, we have to reset the callbacks
     * explicitly to make this work as intended.
     */
    on_exit_reset();

    /*
     * Note we do exit(2) not exit(0).    This is to force the postmaster into a
     * system reset cycle if some idiot DBA sends a manual SIGQUIT to a random
     * backend.  This is necessary precisely because we don't clean up our
     * shared memory state.  (The "dead man switch" mechanism in pmsignal.c
     * should ensure the postmaster sees this as a crash, too, but no harm in
     * being doubly sure.)
     */
    gs_thread_exit(2);
}

/* recovery thread function */

bool PushBadPageToRemoteHashTbl(RepairBlockKey key, PageErrorType error_type, XLogRecPtr old_lsn,
    XLogPhyBlock pblk, ThreadId tid)
{
    HTAB *repair_hash = g_instance.repair_cxt.page_repair_hashtbl;
    bool found = false;

    Assert(repair_hash != NULL);

    LWLockAcquire(g_instance.repair_cxt.page_repair_hashtbl_lock, LW_EXCLUSIVE);
    RepairBlockEntry *entry = (RepairBlockEntry*)hash_search(repair_hash, &(key), HASH_ENTER, &found);
    if (!found) {
        entry->key = key;
        entry->recovery_tid = tid;
        entry->error_type = error_type;
        entry->page_state = WAIT_REMOTE_READ;
        entry->page_old_lsn = old_lsn;
        entry->page_new_lsn = InvalidXLogRecPtr;
        entry->pblk = pblk;
    }
    LWLockRelease(g_instance.repair_cxt.page_repair_hashtbl_lock);

    if (!found) {
        /* need add array and wakeup the page repair thread */
        ThreadId PageRepairPID = g_instance.pid_cxt.PageRepairPID;
        if (PageRepairPID != 0) {
            (void)gs_signal_send(PageRepairPID, SIGUSR1);
        }
        if (g_instance.repair_cxt.repair_proc_latch != NULL) {
            SetLatch(g_instance.repair_cxt.repair_proc_latch);
        }
    }

    return found;
}

bool BlockNodeMatch(RepairBlockKey key, XLogPhyBlock pblk, RelFileNode node,
    ForkNumber forknum, BlockNumber minblkno, bool segment_shrink)
{
    if (segment_shrink) {
        RelFileNode rnode = key.relfilenode;
        rnode.relNode = pblk.relNode;
        bool node_equal = RelFileNodeRelEquals(node, rnode);
        return node_equal && key.forknum == forknum && pblk.block >= minblkno;
    } else {
        bool node_equal = IsBucketFileNode(node) ? RelFileNodeEquals(node, key.relfilenode)
                                                 : RelFileNodeRelEquals(node, key.relfilenode);
        return node_equal && key.forknum == forknum && key.blocknum >= minblkno;
    }
}

bool dbNodeandSpcNodeMatch(RelFileNode *rnode, Oid spcNode, Oid dbNode)
{
    if (OidIsValid(spcNode) && rnode->spcNode != spcNode) {
        return false;
    }
    if (OidIsValid(dbNode) && rnode->dbNode != dbNode) {
        return false;
    }
    return true;
}

/* BatchClearPageRepairHashTbl
 *         drop database, or drop segmentspace, need clear the page repair hashTbl,
 * if the repair page key dbNode match and spcNode match, need remove.
 */
void BatchClearPageRepairHashTbl(Oid spcNode, Oid dbNode)
{
    HTAB *repair_hash = g_instance.repair_cxt.page_repair_hashtbl;
    bool found = false;
    RepairBlockEntry *entry = NULL;
    HASH_SEQ_STATUS status;

    LWLockAcquire(g_instance.repair_cxt.page_repair_hashtbl_lock, LW_EXCLUSIVE);

    hash_seq_init(&status, repair_hash);
    while ((entry = (RepairBlockEntry *)hash_seq_search(&status)) != NULL) {
        if (dbNodeandSpcNodeMatch(&(entry->key.relfilenode), spcNode, dbNode)) {
            if (hash_search(repair_hash, &(entry->key), HASH_REMOVE, &found) == NULL) {
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("page repair hash table corrupted")));
            }
        }
    }

    LWLockRelease(g_instance.repair_cxt.page_repair_hashtbl_lock);

    return;
}

/* ClearPageRepairHashTbl
 *         drop table, or truncate table, need clear the page repair hashTbl, if the
 * repair page Filenode match  need remove.
 */
void ClearPageRepairHashTbl(const RelFileNode &node, ForkNumber forknum, BlockNumber minblkno,
    bool segment_shrink)
{
    HTAB *repair_hash = g_instance.repair_cxt.page_repair_hashtbl;
    bool found = false;
    RepairBlockEntry *entry = NULL;
    HASH_SEQ_STATUS status;

    LWLockAcquire(g_instance.repair_cxt.page_repair_hashtbl_lock, LW_EXCLUSIVE);

    hash_seq_init(&status, repair_hash);
    while ((entry = (RepairBlockEntry *)hash_seq_search(&status)) != NULL) {
        if (BlockNodeMatch(entry->key, entry->pblk, node, forknum, minblkno, segment_shrink)) {
            if (hash_search(repair_hash, &(entry->key), HASH_REMOVE, &found) == NULL) {
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("page repair hash table corrupted")));
            }
        }
    }

    LWLockRelease(g_instance.repair_cxt.page_repair_hashtbl_lock);

    return;
}

/* ClearSpecificsPageRepairHashTbl
 *         If the page repair finish, need clear the page repair hashTbl.
 */
void ClearSpecificsPageRepairHashTbl(RepairBlockKey key)
{
    bool found = false;
    HTAB *repair_hash = g_instance.repair_cxt.page_repair_hashtbl;

    LWLockAcquire(g_instance.repair_cxt.page_repair_hashtbl_lock, LW_EXCLUSIVE);

    if ((RepairBlockEntry*)hash_search(repair_hash, &(key), HASH_REMOVE, &found) == NULL) {
        ereport(WARNING,
            (errmsg("the %u/%u/%u bucketnode %d forknum %u, blknum %u, remove form repair hashtbl, not found",
                key.relfilenode.spcNode, key.relfilenode.dbNode, key.relfilenode.relNode, key.relfilenode.bucketNode,
                key.forknum, key.blocknum)));
    }

    LWLockRelease(g_instance.repair_cxt.page_repair_hashtbl_lock);
    return;
}

/* CheckRepairPage
 *               recovery thread check the primary page lsn (page_new_lsn) is in the range
 * from record_min_lsn to record_max_lsn,
 */
bool CheckRepairPage(RepairBlockKey key, XLogRecPtr min_lsn, XLogRecPtr max_lsn, char *page)
{
    bool found = false;
    bool can_recovery = false;
    RepairBlockEntry *entry = NULL;
    HTAB *repair_hash = g_instance.repair_cxt.page_repair_hashtbl;

    LWLockAcquire(g_instance.repair_cxt.page_repair_hashtbl_lock, LW_EXCLUSIVE);

    entry = (RepairBlockEntry*)hash_search(repair_hash, &(key), HASH_FIND, &found);
    if (entry == NULL) {
        ereport(ERROR, (errmsg("the page repair hash table corrupted ")));
    }

    /* the page_new_lsn is in the range from record_min_lsn to record_max_lsn */
    if (entry->page_state == WAIT_REPAIR && entry->page_new_lsn <= max_lsn && entry->page_new_lsn >= min_lsn) {
        errno_t rc;
        can_recovery = true;

        rc = memcpy_s(page, BLCKSZ, entry->page_content, BLCKSZ);
        securec_check(rc, "", "");
    }
    LWLockRelease(g_instance.repair_cxt.page_repair_hashtbl_lock);

    return can_recovery;
}

void WaitRepalyFinish()
{
    /* file repair finish, need clean the invalid page */
    if (IsExtremeRedo()) {
        extreme_rto::WaitAllReplayWorkerIdle();
    } else if (IsParallelRedo()) {
        parallel_recovery::WaitAllPageWorkersQueueEmpty();
    } else {
        XLogRecPtr standby_replay_lsn = GetXLogReplayRecPtr(NULL, NULL);
        XLogRecPtr suspend_lsn = pg_atomic_read_u64(&g_instance.startup_cxt.suspend_lsn);
        /* if suspend_lsn > standby_replay_lsn then need wait */
        while (!XLByteLE(suspend_lsn, standby_replay_lsn)) {
            /* sleep 1s */
            PageRepairHandleInterrupts();
            pg_usleep(1000000L);
            /* get current replay lsn again */
            (void)GetXLogReplayRecPtr(NULL, &standby_replay_lsn);
        }
    }
}

const int MAX_REPAIR_FILE_NUM = 20;
void FileRepairHashTblInit(void)
{
    HASHCTL ctl;

    if (g_instance.repair_cxt.file_repair_hashtbl_lock == NULL) {
        g_instance.repair_cxt.file_repair_hashtbl_lock = LWLockAssign(LWTRANCHE_FILE_REPAIR);
    }

    if (g_instance.repair_cxt.file_repair_hashtbl == NULL) {
        /* hash accessed by database file id */
        errno_t rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
        securec_check(rc, "", "");
        ctl.keysize = sizeof(RepairFileKey);
        ctl.entrysize = sizeof(RepairFileEntry);
        ctl.hash = tag_hash;
        ctl.hcxt = INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE);
        g_instance.repair_cxt.file_repair_hashtbl = hash_create("File Repair Hash Table", MAX_REPAIR_FILE_NUM, &ctl,
            HASH_ELEM | HASH_FUNCTION |HASH_CONTEXT);

        if (!g_instance.repair_cxt.file_repair_hashtbl)
            ereport(FATAL, (errmsg("could not initialize file repair Hash table")));
    }

    return;
}

bool CheckFileRepairHashTbl(RelFileNode rnode, ForkNumber forknum, uint32 segno)
{
    HTAB* file_hashtbl = g_instance.repair_cxt.file_repair_hashtbl;
    RepairFileKey key;
    RepairFileEntry *entry = NULL;
    bool found = false;

    key.relfilenode = rnode;
    key.forknum = forknum;
    key.segno = segno;

    if (file_hashtbl == NULL) {
        return found;
    }
    LWLockAcquire(FILE_REPAIR_LOCK, LW_SHARED);
    entry = (RepairFileEntry*)hash_search(file_hashtbl, &(key), HASH_FIND, &found);
    if (found) {
        if (entry->file_state == WAIT_FILE_REPAIR || entry->file_state == WAIT_RENAME) {
            found = true;
        } else {
            found = false;
        }
    }
    LWLockRelease(FILE_REPAIR_LOCK);

    return found;
}

void CheckNeedRecordBadFile(RepairFileKey key, uint32 nblock, uint32 blocknum, const XLogPhyBlock *pblk)
{
    if (CheckVerionSupportRepair() && (nblock == 0 || blocknum / RELSEG_SIZE > nblock / RELSEG_SIZE) &&
        IsPrimaryClusterStandbyDN() && g_instance.repair_cxt.support_repair) {
        if (pblk != NULL) {
            key.relfilenode.relNode = pblk->relNode;
            key.segno = pblk->block / RELSEG_SIZE;
        }
        if (IsSegmentFileNode(key.relfilenode)) {
            key.relfilenode.bucketNode = SegmentBktId;
        }
        PushBadFileToRemoteHashTbl(key);
    }
}

static void PushBadFileToRemoteHashTbl(RepairFileKey key)
{
    HTAB *file_hash = g_instance.repair_cxt.file_repair_hashtbl;
    RepairFileEntry *entry = NULL;
    bool found = false;
    XLogRecPtr min_recovery_point;

    LWLockAcquire(ControlFileLock, LW_SHARED);
    min_recovery_point = t_thrd.shemem_ptr_cxt.ControlFile->minRecoveryPoint;
    LWLockRelease(ControlFileLock);

    LWLockAcquire(FILE_REPAIR_LOCK, LW_EXCLUSIVE);
    entry = (RepairFileEntry*)hash_search(file_hash, &(key), HASH_ENTER, &found);
    if (!found) {
        entry->key.relfilenode.relNode = key.relfilenode.relNode;
        entry->key.relfilenode.dbNode = key.relfilenode.dbNode;
        entry->key.relfilenode.spcNode = key.relfilenode.spcNode;
        entry->key.relfilenode.bucketNode = key.relfilenode.bucketNode;
        entry->key.relfilenode.opt = key.relfilenode.opt;
        entry->key.forknum = key.forknum;
        entry->key.segno = key.segno;
        entry->min_recovery_point = min_recovery_point;
        entry->file_state = WAIT_FILE_CHECK_REPAIR;
        entry->primary_file_lsn = InvalidXLogRecPtr;

        ereport(LOG, (errmodule(MOD_REDO),
                    errmsg("[file repair] push to file repair hashtbl, path is %s segno is %u",
                    relpathperm(entry->key.relfilenode, entry->key.forknum), entry->key.segno)));
    }
    LWLockRelease(FILE_REPAIR_LOCK);
    return;
}

bool FileNodeMatch(RepairFileKey key, RelFileNode node, ForkNumber forknum, uint32 segno)
{
    bool node_equal = RelFileNodeRelEquals(node, key.relfilenode);

    return node_equal && key.forknum == forknum && key.segno >= segno;
}

void ClearBadFileHashTbl(const RelFileNode &node, ForkNumber forknum, uint32 segno)
{
    HTAB *file_hash = g_instance.repair_cxt.file_repair_hashtbl;
    RepairFileEntry *entry = NULL;
    bool found = false;
    HASH_SEQ_STATUS status;

    LWLockAcquire(FILE_REPAIR_LOCK, LW_EXCLUSIVE);

    hash_seq_init(&status, file_hash);
    while ((entry = (RepairFileEntry *)hash_seq_search(&status)) != NULL) {
        if (FileNodeMatch(entry->key, node, forknum, segno)) {
            if (hash_search(file_hash, &(entry->key), HASH_REMOVE, &found) == NULL) {
                LWLockRelease(FILE_REPAIR_LOCK);
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("file repair hash table corrupted")));
            } else {
                ereport(LOG, (errmodule(MOD_REDO),
                    errmsg("[file repair] file %s segno is %u entry remove when drop table or truncate table",
                    relpathperm(entry->key.relfilenode, entry->key.forknum), entry->key.segno)));
            }
        }
    }

    LWLockRelease(FILE_REPAIR_LOCK);
    return;
}


void BatchClearBadFileHashTbl(Oid spcNode, Oid dbNode)
{
    HTAB *file_hash = g_instance.repair_cxt.file_repair_hashtbl;
    RepairFileEntry *entry = NULL;
    bool found = false;
    HASH_SEQ_STATUS status;

    LWLockAcquire(FILE_REPAIR_LOCK, LW_EXCLUSIVE);

    hash_seq_init(&status, file_hash);
    while ((entry = (RepairFileEntry *)hash_seq_search(&status)) != NULL) {
        if (dbNodeandSpcNodeMatch(&(entry->key.relfilenode), spcNode, dbNode)) {
            if (hash_search(file_hash, &(entry->key), HASH_REMOVE, &found) == NULL) {
                LWLockRelease(FILE_REPAIR_LOCK);
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("file repair hash table corrupted")));
            } else {
                ereport(LOG, (errmodule(MOD_REDO),
                    errmsg("[file repair] file %s segno is %u entry remove when drop database or segment space",
                    relpathperm(entry->key.relfilenode, entry->key.forknum), entry->key.segno)));
            }
        }
    }

    LWLockRelease(FILE_REPAIR_LOCK);

    return;
}

void RenameRepairFile(RepairFileKey *key, bool clear_entry)
{
    errno_t rc;
    bool found = false;
    HTAB *file_hash = g_instance.repair_cxt.file_repair_hashtbl;
    char *path = relpathperm(key->relfilenode, key->forknum);
    char *tempsegpath = (char *)palloc(strlen(path) + SEGLEN);
    char *segpath = (char *)palloc(strlen(path) + SEGLEN);

    /* wait all dirty page flush */
    RequestCheckpoint(CHECKPOINT_FLUSH_DIRTY|CHECKPOINT_WAIT);

    if (key->segno == 0) {
        rc = sprintf_s(segpath, strlen(path) + SEGLEN, "%s", path);
        securec_check_ss(rc, "", "")
        rc = sprintf_s(tempsegpath, strlen(path) + SEGLEN, "%s.repair", path);
        securec_check_ss(rc, "", "")
    } else {
        rc = sprintf_s(segpath, strlen(path) + SEGLEN, "%s.%u", path, key->segno);
        securec_check_ss(rc, "", "")
        rc = sprintf_s(tempsegpath, strlen(path) + SEGLEN, "%s.%u.repair", path, key->segno);
        securec_check_ss(rc, "", "")
    }

    rc = durable_rename(tempsegpath, segpath, WARNING);
    if (rc == 0) {
        ereport(LOG, (errmodule(MOD_REDO),
            errmsg("[file repair] file rename from %s to %s finish", tempsegpath, segpath)));

        /* file repair finish, need clean the invalid page */
        if (IsExtremeRedo()) {
            extreme_rto::DispatchCleanInvalidPageMarkToAllRedoWorker(*key);
            extreme_rto::DispatchClosefdMarkToAllRedoWorker();
            extreme_rto::WaitAllReplayWorkerIdle();
        } else if (IsParallelRedo()) {
            if (AmStartupProcess()) {
                ProcTxnWorkLoad(true);
            }
            parallel_recovery::SendCleanInvalidPageMarkToAllWorkers(*key);
            parallel_recovery::SendClosefdMarkToAllWorkers();
            parallel_recovery::WaitAllPageWorkersQueueEmpty();
        } else {
        }
        forget_range_invalid_pages((void*)key);
        smgrcloseall();

        LWLockAcquire(FILE_REPAIR_LOCK, LW_EXCLUSIVE);

        if (clear_entry) {
            if (hash_search(file_hash, key, HASH_REMOVE, &found) == NULL) {
                pfree(path);
                pfree(segpath);
                pfree(tempsegpath);
                LWLockRelease(FILE_REPAIR_LOCK);
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("file repair hash table corrupted")));
            } else {
                ereport(LOG, (errmodule(MOD_REDO),
                    errmsg("[file repair] file %s repair finish, remove the entry", segpath)));
            }
        }
        LWLockRelease(FILE_REPAIR_LOCK);
    }
    pfree(path);
    pfree(segpath);
    pfree(tempsegpath);

    return;
}

void CheckNeedRenameFile()
{
    HASH_SEQ_STATUS status;
    RepairFileEntry *entry = NULL;
    HTAB *file_hash = g_instance.repair_cxt.file_repair_hashtbl;
    uint32 need_repair_num = 0;
    uint32 need_rename_num = 0;
    RepairFileKey *rename_key = NULL;
    errno_t rc = 0;
    uint32 i = 0;

    LWLockAcquire(FILE_REPAIR_LOCK, LW_EXCLUSIVE);
    hash_seq_init(&status, file_hash);
    while ((entry = (RepairFileEntry *)hash_seq_search(&status)) != NULL) {
        if (entry->file_state == WAIT_RENAME) {
            need_rename_num++;
        }
    }
    if (need_rename_num > 0) {
        rename_key = (RepairFileKey*)palloc0(sizeof(RepairFileKey) * need_rename_num);
    }

    hash_seq_init(&status, file_hash);

    while ((entry = (RepairFileEntry *)hash_seq_search(&status)) != NULL) {
        switch (entry->file_state) {
            case WAIT_RENAME:
                Assert(XLByteLE(entry->primary_file_lsn, GetXLogReplayRecPtr(NULL, NULL)));
                Assert(!IsSegmentFileNode(entry->key.relfilenode));
                rc = memcpy_s(&rename_key[i], sizeof(RepairFileKey), &(entry->key), sizeof(RepairFileKey));
                securec_check(rc, "", "");
                i++;
                break;
            case WAIT_FILE_REMOTE_READ:
            case WAIT_FILE_REPAIR_SEGMENT:
                need_repair_num++;
                break;
            case WAIT_FOREGT_INVALID_PAGE:
                {
                    forget_range_invalid_pages((void*)&entry->key);
                    bool found = false;
                    if (hash_search(file_hash, &(entry->key), HASH_REMOVE, &found) == NULL) {
                        LWLockRelease(FILE_REPAIR_LOCK);
                        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("file repair hash table corrupted")));
                    } else {
                        ereport(LOG, (errmodule(MOD_REDO),
                            errmsg("[file repair] file %s seg is %d, repair finish, remove the entry",
                                relpathperm(entry->key.relfilenode, entry->key.forknum), entry->key.segno)));
                    }
                }
                break;
            default:
                break;
        }
    }
    LWLockRelease(FILE_REPAIR_LOCK);

    for (i = 0; i < need_rename_num; i++) {
        RepairFileKey *key = &rename_key[i];
        RenameRepairFile(key, true);
    }
    if (need_rename_num > 0) {
        pfree(rename_key);
        rename_key = NULL;
    }
    if (need_repair_num == 0) {
        SetRecoverySuspend(false);
        ereport(LOG, (errmodule(MOD_REDO),
            errmsg("set recovery suspend to false, the need repair num is zero")));
    }
}

void CheckIsStopRecovery(void)
{
    uint32 need_repair_num = 0;
    uint32 need_rename_num = 0;
    HASH_SEQ_STATUS status;
    RepairFileEntry *entry = NULL;
    HTAB *file_hash = g_instance.repair_cxt.file_repair_hashtbl;
    XLogRecPtr repaly = GetXLogReplayRecPtr(NULL, NULL);
    XLogRecPtr flush = GetStandbyFlushRecPtr(NULL);

    if (file_hash == NULL) {
        return;
    }

    if (LWLockConditionalAcquire(FILE_REPAIR_LOCK, LW_EXCLUSIVE)) {
        hash_seq_init(&status, file_hash);
        while ((entry = (RepairFileEntry *)hash_seq_search(&status)) != NULL) {
            flush = GetStandbyFlushRecPtr(NULL);
            if (!XLogRecPtrIsInvalid(entry->min_recovery_point) && XLByteLT(entry->min_recovery_point, repaly)
                && entry->file_state == WAIT_FILE_CHECK_REPAIR) {
                entry->file_state = WAIT_FILE_REMOTE_READ;
            }

            if (entry->file_state == WAIT_FILE_REMOTE_READ || entry->file_state == WAIT_FILE_REPAIR_SEGMENT) {
                need_repair_num++;
                ereport(LOG, (errmodule(MOD_REDO),
                    errmsg("[file repair] need remote read or segment file wait rename, file path %s segno is %u",
                        relpathperm(entry->key.relfilenode, entry->key.forknum), entry->key.segno)));
            }

            if ((entry->file_state == WAIT_FILE_REPAIR && !IsSegmentFileNode(entry->key.relfilenode) &&
                XLByteLT(entry->primary_file_lsn, repaly)) || entry->file_state == WAIT_RENAME) {
                entry->file_state = WAIT_RENAME;
                need_rename_num++;
                ereport(LOG, (errmodule(MOD_REDO),
                    errmsg("[file repair] need rename, file path %s segno is %u",
                        relpathperm(entry->key.relfilenode, entry->key.forknum), entry->key.segno)));
            }
        }

        LWLockRelease(FILE_REPAIR_LOCK);

        if (need_repair_num > 0 || need_rename_num > 0) {
            load_server_mode();
            if (NOT_SUPPORT_PAGE_REPAIR) {
                return;
            }
            if (t_thrd.xlog_cxt.server_mode == STANDBY_MODE && g_instance.repair_cxt.support_repair) {
                SetRecoverySuspend(true);
                ereport(LOG, (errmodule(MOD_REDO),
                    errmsg("set recovery suspend to true, the need repair num is %d, need rename num is %d",
                        need_repair_num, need_rename_num)));
            }
        }
    }

    return;
}

const int REPAIR_LEN = 8;
int CreateRepairFile(char *path)
{
    int fd = -1;
    int retry_times = 0;
    const int MAX_RETRY_TIME = 2;
    errno_t rc;
    char *reapirpath = (char *)palloc(strlen(path) + REPAIR_LEN);

    rc = sprintf_s(reapirpath, strlen(path) + REPAIR_LEN, "%s.repair", path);
    securec_check_ss(rc, "", "");

RETRY:
    fd = BasicOpenFile((char*)reapirpath, O_CREAT | O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);
    retry_times++;
    if (fd < 0) {
        if (retry_times < MAX_RETRY_TIME) {
            goto RETRY;
        }
        if (errno != ENOENT) {
            ereport(WARNING, (errcode_for_file_access(),
                errmsg("[file repair] could not open file \"%s\": %m", reapirpath)));
            pfree(reapirpath);
            return -1;
        }
    }

    pfree(reapirpath);
    return fd;
}

int WriteRepairFile(int fd, char* path, char *buf, uint32 offset, uint32 size)
{
    errno_t rc = 0;
    char *reapirpath = (char *)palloc(strlen(path) + REPAIR_LEN);

    rc = sprintf_s(reapirpath, strlen(path) + REPAIR_LEN, "%s.repair", path);
    securec_check_ss(rc, "", "");

    if (lseek(fd, offset, SEEK_SET) < 0) {
        ereport(WARNING, (errcode_for_file_access(), errmsg("[file repair] could not seek reapir file %s : %m",
            reapirpath)));
        pfree(reapirpath);
        return -1;
    }

    if (write(fd, buf, size) != size) {
        /* if write didn't set errno, assume problem is no disk space */
        if (errno == 0) {
            errno = ENOSPC;
        }
        ereport(WARNING, (errcode_for_file_access(), errmsg("[file repair] could not write to temp file %s : %m",
            reapirpath)));
        pfree(reapirpath);
        return -1;
    }
    if (fsync(fd) != 0) {
        ereport(WARNING, (errcode_for_file_access(), errmsg("[file repair] could not fsync temp file %s : %m",
            reapirpath)));
        pfree(reapirpath);
        return -1;
    }

    pfree(reapirpath);
    return 0;
}

void UnlinkOldBadFile(char *path, RepairFileKey key)
{
    /* wait the xlog repaly finish */
    if (IsExtremeRedo()) {
        extreme_rto::DispatchClosefdMarkToAllRedoWorker();
        extreme_rto::WaitAllReplayWorkerIdle();
    } else if (IsParallelRedo()) {
        parallel_recovery::SendClosefdMarkToAllWorkers();
        parallel_recovery::WaitAllPageWorkersQueueEmpty();
    } else {
        XLogRecPtr standby_replay_lsn = GetXLogReplayRecPtr(NULL, NULL);
        XLogRecPtr suspend_lsn = pg_atomic_read_u64(&g_instance.startup_cxt.suspend_lsn);
        /* if suspend_lsn > standby_replay_lsn then need wait */
        while (!XLByteLE(suspend_lsn, standby_replay_lsn)) {
            /* sleep 1s */
            PageRepairHandleInterrupts();
            pg_usleep(1000000L);
            /* get current replay lsn again */
            (void)GetXLogReplayRecPtr(NULL, &standby_replay_lsn);
        }
    }
    /* wait all dirty page flush */
    RequestCheckpoint(CHECKPOINT_FLUSH_DIRTY|CHECKPOINT_WAIT);

    /* handle the backend thread */
    RelFileNodeBackend rnode;
    rnode.node = key.relfilenode;
    rnode.backend = InvalidBackendId;
    CacheInvalidateSmgr(rnode);
    int ret = unlink(path);
    if (ret < 0 && errno != ENOENT) {
        ereport(WARNING, (errcode_for_file_access(), errmsg("[file repair] could not remove file \"%s\": %m", path)));
    }
    if (ret >= 0) {
        ereport(LOG, (errcode_for_file_access(), errmsg("[file repair] remove file \"%s\": %m", path)));
    }
    CacheInvalidateSmgr(rnode);
    /* invalidate shared buffer about this seg file */
    LockRelFileNode(key.relfilenode, AccessExclusiveLock);
    RangeForgetBuffer(key.relfilenode, key.forknum, key.segno * RELSEG_SIZE, (key.segno + 1) * RELSEG_SIZE);
    UnlockRelFileNode(key.relfilenode, AccessExclusiveLock);
    return;
}

static void RepairSegFile(RepairFileKey key, char *segpath, uint32 seg_no, uint32 max_segno, uint64 size)
{
    char *buf = 0;
    int ret_code = REMOTE_READ_NEED_WAIT;
    int fd = -1;
    errno_t rc = 0;
    struct stat statBuf;
    uint32 seg_size = 0;
    uint32 remote_size = 0;
    RemoteReadFileKey read_key;
    XLogRecPtr remote_lsn = InvalidXLogRecPtr;
    XLogRecPtr standby_flush_lsn = InvalidXLogRecPtr;
    bool found = false;

    fd = CreateRepairFile(segpath);
    if (fd < 0) {
        ereport(WARNING, (errcode_for_file_access(),
                errmsg("[file repair] could not create repair file \"%s\", segno is %d",
                       relpathperm(key.relfilenode, key.forknum), seg_no)));
        return;
    }
    read_key.relfilenode = key.relfilenode;
    read_key.forknum = key.forknum;
    read_key.blockstart = seg_no * RELSEG_SIZE;

    buf = (char*)palloc(MAX_BATCH_READ_BLOCKNUM * BLCKSZ);
    seg_size = (seg_no < max_segno ? (RELSEG_SIZE * BLCKSZ) : (size % (RELSEG_SIZE * BLCKSZ)));
    int batch_size = MAX_BATCH_READ_BLOCKNUM * BLCKSZ;
    int max_times = seg_size % batch_size == 0 ? seg_size / batch_size : (seg_size / batch_size + 1);

    for (int j = 0; j < max_times; j++) {
        int read_size = 0;
        if (seg_size % batch_size != 0) {
            read_size = (j == max_times - 1 ? seg_size % batch_size : batch_size);
        } else {
            read_size = batch_size;
        }

        read_key.blockstart = seg_no * RELSEG_SIZE + j * MAX_BATCH_READ_BLOCKNUM;
        ret_code = RemoteReadFileNoError(&read_key, buf, InvalidXLogRecPtr, read_size, &remote_lsn, &remote_size);
        if (ret_code == REMOTE_READ_OK) {
            rc = WriteRepairFile(fd, segpath, buf, j * batch_size, read_size);
            if (rc != 0) {
                ereport(WARNING, (errcode_for_file_access(),
                errmsg("[file repair] could not write repair file \"%s\", segno is %d",
                       relpathperm(key.relfilenode, key.forknum), seg_no)));
                pfree(buf);
                (void)close(fd);
                return;
            }
        } else {
            ereport(WARNING, (errcode_for_file_access(),
                errmsg("[file repair] remote read file failed \"%s\", segno is %d, block start %u",
                       relpathperm(key.relfilenode, key.forknum), seg_no, read_key.blockstart)));
            pfree(buf);
            (void)close(fd);
            return;
        }
    }
    pfree(buf);
    (void)close(fd);

    if (ret_code == REMOTE_READ_OK) {
        standby_flush_lsn = GetStandbyFlushRecPtr(NULL);
        while (!XLByteLT(remote_lsn, standby_flush_lsn)) {
            PageRepairHandleInterrupts();
            /* sleep 10ms */
            pg_usleep(10000L);
            /* get current replay lsn again */
            standby_flush_lsn = GetStandbyFlushRecPtr(NULL);
        }
        ereport(LOG, (errmsg("[file repair] wait lsn flush, remote lsn is %X/%X",
            (uint32)(remote_lsn >> XLOG_LSN_SWAP), (uint32)remote_lsn)));
    } else {
        return;
    }

    /* wait xlog repaly */
    if (!IsSegmentFileNode(key.relfilenode)) {
        if (stat(segpath, &statBuf) < 0) {
            if (errno != ENOENT) {
                ereport(WARNING, (errcode_for_file_access(),
                    errmsg("[file repair] could not stat file \"%s\" before repair: %m", segpath)));
                UnlinkOldBadFile(segpath, key);
            }
        } else {
            UnlinkOldBadFile(segpath, key);
        }
    }

    /* wait xlog repaly finish, need get lock */
    LWLockAcquire(FILE_REPAIR_LOCK, LW_EXCLUSIVE);
    RepairFileEntry *temp_entry = (RepairFileEntry*)hash_search(g_instance.repair_cxt.file_repair_hashtbl, &(key),
        HASH_FIND, &found);
    if (found) {
        temp_entry->file_state = IsSegmentFileNode(key.relfilenode) ? WAIT_FILE_REPAIR_SEGMENT : WAIT_FILE_REPAIR;
        temp_entry->primary_file_lsn = remote_lsn;
    }
    LWLockRelease(FILE_REPAIR_LOCK);
    return;
}

bool CheckAllSegmentFileRepair(RepairFileKey key, uint32 max_segno)
{
    uint32 repair_num = 0;

    /* check all slicno file remote read finish */
    LWLockAcquire(FILE_REPAIR_LOCK, LW_EXCLUSIVE);
    for (uint i = 0; i <= max_segno; i++) {
        RepairFileKey temp_key;
        bool found = false;
        temp_key.relfilenode.relNode = key.relfilenode.relNode;
        temp_key.relfilenode.dbNode = key.relfilenode.dbNode;
        temp_key.relfilenode.spcNode = key.relfilenode.spcNode;
        temp_key.relfilenode.bucketNode = key.relfilenode.bucketNode;
        temp_key.relfilenode.bucketNode = key.relfilenode.opt;
        temp_key.forknum = key.forknum;
        temp_key.segno = i;

        RepairFileEntry *entry = (RepairFileEntry*)hash_search(g_instance.repair_cxt.file_repair_hashtbl,
            &(temp_key), HASH_FIND, &found);
        Assert(found);
        if (found && entry->file_state == WAIT_FILE_REPAIR_SEGMENT) {
            repair_num++;
        }
    }
    LWLockRelease(FILE_REPAIR_LOCK);

    if (repair_num == max_segno + 1) {
        /* 1. rename all file */
        RepairFileKey rename_key = {0};
        for (uint i = 0; i <= max_segno; i++) {
            bool found = false;

            rename_key.relfilenode.relNode = key.relfilenode.relNode;
            rename_key.relfilenode.dbNode = key.relfilenode.dbNode;
            rename_key.relfilenode.spcNode = key.relfilenode.spcNode;
            rename_key.relfilenode.bucketNode = key.relfilenode.bucketNode;
            rename_key.relfilenode.opt = key.relfilenode.opt;
            rename_key.forknum = key.forknum;
            rename_key.segno = i;

            LWLockAcquire(FILE_REPAIR_LOCK, LW_EXCLUSIVE);
            (void*)hash_search(g_instance.repair_cxt.file_repair_hashtbl, &(rename_key), HASH_FIND, &found);
            Assert(found);
            LWLockRelease(FILE_REPAIR_LOCK);
            if (found) {
                RenameRepairFile(&rename_key, false);
            }
        }

        /* 2. open all file */
        df_open_all_file(rename_key, max_segno);

        /* 3. change file state */
        LWLockAcquire(FILE_REPAIR_LOCK, LW_EXCLUSIVE);
        RepairFileKey change_key;
        for (uint i = 0; i <= max_segno; i++) {
            bool found = false;
            change_key.relfilenode.relNode = key.relfilenode.relNode;
            change_key.relfilenode.dbNode = key.relfilenode.dbNode;
            change_key.relfilenode.spcNode = key.relfilenode.spcNode;
            change_key.relfilenode.bucketNode = key.relfilenode.bucketNode;
            change_key.relfilenode.opt = key.relfilenode.opt;
            change_key.forknum = key.forknum;
            change_key.segno = i;

            RepairFileEntry *entry = (RepairFileEntry*)hash_search(g_instance.repair_cxt.file_repair_hashtbl,
                &(change_key), HASH_FIND, &found);
            Assert(found);
            if (found) {
                entry->file_state = WAIT_FOREGT_INVALID_PAGE;
            }
        }
        LWLockRelease(FILE_REPAIR_LOCK);
        return true;
    }

    return false;
}

void StandbyRemoteReadFile(RepairFileKey key)
{
    int ret_code;
    int64 size = 0;
    errno_t rc;
    bool found = false;
    char *path = relpathperm(key.relfilenode, key.forknum);
    char *segpath = (char *)palloc(strlen(path) + SEGLEN);

RETYR:
    ret_code = RemoteReadFileSizeNoError(&key, &size);
    if (ret_code == REMOTE_READ_OK) {
        uint32 max_segno = 0;

        if (size <= 0) {
            pfree(path);
            pfree(segpath);
            LWLockAcquire(FILE_REPAIR_LOCK, LW_EXCLUSIVE);
            RepairFileEntry *temp_entry = (RepairFileEntry*)hash_search(g_instance.repair_cxt.file_repair_hashtbl,
                &(key), HASH_FIND, &found);
            if (found) {
                temp_entry->file_state = WAIT_CLEAN;
            }
            LWLockRelease(FILE_REPAIR_LOCK);
            return;
        }

        max_segno = size / (RELSEG_SIZE * BLCKSZ); /* max_segno start from 0 */
        if (key.segno > max_segno) {
            ereport(WARNING, (errcode_for_file_access(),
                errmsg("[file repair] primary this file %s , segno is %d also not exist, can not repair, wait clean",
                relpathperm(key.relfilenode, key.forknum), key.segno)));

            LWLockAcquire(FILE_REPAIR_LOCK, LW_EXCLUSIVE);
            RepairFileEntry *temp_entry = (RepairFileEntry*)hash_search(g_instance.repair_cxt.file_repair_hashtbl,
                &(key), HASH_FIND, &found);
            if (found) {
                temp_entry->file_state = WAIT_CLEAN;
            }
            LWLockRelease(FILE_REPAIR_LOCK);

            pfree(path);
            pfree(segpath);
            return;
        }

        if (IsSegmentFileNode(key.relfilenode)) {
            /* wait all dirty page flush */
            WaitRepalyFinish();
            /* wait all dirty page flush */
            RequestCheckpoint(CHECKPOINT_FLUSH_DIRTY|CHECKPOINT_WAIT);
            df_clear_and_close_all_file(key, max_segno);
        }

        if (key.segno == 0) {
            rc = sprintf_s(segpath, strlen(path) + SEGLEN, "%s", path);
        } else {
            rc = sprintf_s(segpath, strlen(path) + SEGLEN, "%s.%u", path, key.segno);
        }
        securec_check_ss(rc, "", "");
        RepairSegFile(key, segpath, key.segno, max_segno, size);
        checkOtherFile(key, max_segno, size);
        if (IsSegmentFileNode(key.relfilenode) && !CheckAllSegmentFileRepair(key, max_segno)) {
            goto RETYR;
        }
    }
    pfree(path);
    pfree(segpath);
    return;
}

static void checkOtherFile(RepairFileKey key, uint32 max_segno, uint64 size)
{
    errno_t rc;
    bool found = false;
    struct stat statBuf;
    RepairFileKey temp_key;
    RepairFileEntry *temp_entry = NULL;
    char *path = relpathperm(key.relfilenode, key.forknum);
    char *segpath = (char *)palloc(strlen(path) + SEGLEN);
    HTAB *file_hash = g_instance.repair_cxt.file_repair_hashtbl;

    for (uint i = 0; i <= max_segno; i++) {
        if (i == 0) {
            rc = sprintf_s(segpath, strlen(path) + SEGLEN, "%s", path);
        } else {
            rc = sprintf_s(segpath, strlen(path) + SEGLEN, "%s.%u", path, i);
        }
        securec_check_ss(rc, "", "");

        if (i == key.segno) {
            continue;
        }

        /* Check whether other segment files exist in the hashtable. */
        temp_key.relfilenode.relNode = key.relfilenode.relNode;
        temp_key.relfilenode.dbNode = key.relfilenode.dbNode;
        temp_key.relfilenode.spcNode = key.relfilenode.spcNode;
        temp_key.relfilenode.bucketNode = key.relfilenode.bucketNode;
        temp_key.relfilenode.opt = key.relfilenode.opt;
        temp_key.forknum = key.forknum;
        temp_key.segno = i;

        LWLockAcquire(FILE_REPAIR_LOCK, LW_EXCLUSIVE);
        temp_entry = (RepairFileEntry*)hash_search(file_hash, &(temp_key), HASH_FIND, &found);
        if (found && temp_entry->file_state == WAIT_FILE_REMOTE_READ) {
            LWLockRelease(FILE_REPAIR_LOCK);
            RepairSegFile(temp_key, segpath, i, max_segno, size);
            continue;
        }

        LWLockRelease(FILE_REPAIR_LOCK);

        /* Check whether other segment files exist */
        if (stat(segpath, &statBuf) == 0) {
            continue;
        }
        if (stat(segpath, &statBuf) < 0 && errno != ENOENT) {
            continue;
        }

        LWLockAcquire(FILE_REPAIR_LOCK, LW_EXCLUSIVE);

        temp_entry = (RepairFileEntry*)hash_search(file_hash, &(temp_key), HASH_ENTER, &found);
        if (!found) {
            LWLockAcquire(ControlFileLock, LW_SHARED);
            XLogRecPtr min_recovery_point = t_thrd.shemem_ptr_cxt.ControlFile->minRecoveryPoint;
            LWLockRelease(ControlFileLock);
            temp_entry->key = temp_key;
            temp_entry->min_recovery_point = min_recovery_point;
            temp_entry->file_state = WAIT_FILE_REMOTE_READ;
            temp_entry->primary_file_lsn = InvalidXLogRecPtr;

            ereport(LOG, (errmodule(MOD_REDO),
                        errmsg("[file repair] check other seg file push to file repair hashtbl, path is %s segno is %u",
                        relpathperm(key.relfilenode, key.forknum), i)));
            LWLockRelease(FILE_REPAIR_LOCK);
            RepairSegFile(temp_key, segpath, i, max_segno, size);
        } else {
            LWLockRelease(FILE_REPAIR_LOCK);
        }
    }
    pfree(path);
    pfree(segpath);
    return;
}

const int MAX_FILE_REPAIR_NUM = 10;
static void SeqRemoteReadFile()
{
    HTAB *repair_hash = g_instance.repair_cxt.file_repair_hashtbl;
    RepairFileEntry *entry = NULL;
    HASH_SEQ_STATUS status;
    uint32 need_repair_num = 0;
    errno_t rc = 0;
    RepairFileKey remote_read[MAX_FILE_REPAIR_NUM] = {0};

    pg_memory_barrier();
    if (!RecoveryIsSuspend() || XLogRecPtrIsInvalid(g_instance.startup_cxt.suspend_lsn)) {
        return;
    }

    rc = memset_s(remote_read, sizeof(RepairFileEntry) * MAX_FILE_REPAIR_NUM, 0,
        sizeof(RepairFileEntry) * MAX_FILE_REPAIR_NUM);
    securec_check(rc, "", "");
    /* wait the xlog repaly finish */
    WaitRepalyFinish();

    LWLockAcquire(FILE_REPAIR_LOCK, LW_EXCLUSIVE);

    hash_seq_init(&status, repair_hash);
    while ((entry = (RepairFileEntry *)hash_seq_search(&status)) != NULL) {
        switch (entry->file_state) {
            /* page repair thread only handle need remote read file */
            case WAIT_FILE_CHECK_REPAIR:
            case WAIT_FILE_REPAIR:
            case WAIT_FOREGT_INVALID_PAGE:
            case WAIT_CLEAN:
            case WAIT_RENAME:
                break;
            case WAIT_FILE_REPAIR_SEGMENT:
            case WAIT_FILE_REMOTE_READ:
                entry->file_state = WAIT_FILE_REMOTE_READ;
                if (need_repair_num >= MAX_FILE_REPAIR_NUM) {
                    break;
                } else {
                    rc = memcpy_s(&remote_read[need_repair_num], sizeof(RepairFileEntry),
                        entry, sizeof(RepairFileEntry));
                    securec_check(rc, "", "");
                }
                break;
            default:
                LWLockRelease(FILE_REPAIR_LOCK);
                ereport(ERROR, (errmsg("[file repair] error file state during remote read")));
                break;
        }
        
    }
    LWLockRelease(FILE_REPAIR_LOCK);
    for (uint32 i = 0; i < need_repair_num; i++) {
        RepairFileKey temp = remote_read[i];
        StandbyRemoteReadFile(temp);
    }

    LWLockAcquire(FILE_REPAIR_LOCK, LW_EXCLUSIVE);
    hash_seq_init(&status, repair_hash);
    while ((entry = (RepairFileEntry *)hash_seq_search(&status)) != NULL) {
        if (entry->file_state == WAIT_FILE_REPAIR_SEGMENT || entry->file_state == WAIT_FILE_REMOTE_READ ||
            entry->file_state == WAIT_RENAME) {
            need_repair_num++;
        }
    }
    LWLockRelease(FILE_REPAIR_LOCK);
    if (need_repair_num == 0) {
        SetRecoverySuspend(false);
        ereport(LOG, (errmodule(MOD_REDO),
            errmsg("pagerepair thread set recovery suspend to false, the need repair num is zero")));
    }
    return;
}
