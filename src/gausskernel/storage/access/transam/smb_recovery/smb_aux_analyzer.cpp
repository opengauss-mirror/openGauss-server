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
 * smb_aux_analyzer.cpp
 * Parallel recovery has a centralized log dispatcher which runs inside
 * the StartupProcess.  The dispatcher is responsible for managing the
 * life cycle of PageRedoWorkers and the TxnRedoWorker, analyzing log
 * records and dispatching them to workers for processing.
 *
 * IDENTIFICATION
 * src/gausskernel/storage/access/transam/smb_recovery/smb_aux_analyzer.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "pgstat.h"
#include "access/smb.h"
#include "storage/ipc.h"
#include "storage/buf/buf_internals.h"

#ifdef WIN32
#define SMB_ALY_MAX_BUCKET_PER_FILE 1
#else
#define SMB_ALY_MAX_BUCKET_PER_FILE (uint64)SIZE_K(200)  // can save 50G tpcc log
#endif

#define SMB_ALY_MAX_FILE            (uint32)10
#define SMB_ALY_BUCKET_AVERAGE_NUM  (uint32)5
#define SMB_ALY_MAX_ITEM            (SMB_ALY_MAX_FILE * SMB_ALY_MAX_BUCKET_PER_FILE * SMB_ALY_BUCKET_AVERAGE_NUM)
#define SMB_ALY_MAX_ITEM_SIZE       (SMB_ALY_MAX_ITEM * sizeof(smb_recovery::SMBAnalyseItem))
#define SMB_ALY_MAX_BUCKET_SIZE \
    (SMB_ALY_MAX_FILE * SMB_ALY_MAX_BUCKET_PER_FILE * sizeof(smb_recovery::SMBAnalyseBucket))

namespace smb_recovery {

static const uint64 THOUSAND_MICROSECOND = 1000;

void SMBAlyMemInit()
{
    SMBAnalyseMem *mgr = NULL;
    uint64 buf_size = SMB_ALY_MAX_ITEM_SIZE + SMB_ALY_MAX_BUCKET_SIZE;
    int32 ret;

    if (g_instance.smb_cxt.SMBAlyMem == NULL) {
        g_instance.smb_cxt.SMBAlyMem = palloc0(buf_size);
        mgr = (SMBAnalyseMem *)palloc0(sizeof(SMBAnalyseMem));
        mgr->smbAlyItems = (SMBAnalyseItem *)g_instance.smb_cxt.SMBAlyMem;
        mgr->smbAlyBuckets =
            (SMBAnalyseBucket *)(g_instance.smb_cxt.SMBAlyMem + SMB_ALY_MAX_ITEM_SIZE);
    }

    if (g_instance.smb_cxt.SMBAlyPageQueue == nullptr) {
        g_instance.smb_cxt.SMBAlyPageQueue = (SMBAnalysePageQueue*)palloc0(sizeof(SMBAnalysePageQueue));
        g_instance.smb_cxt.SMBAlyPageQueue->size = SMB_ALY_MAX_ITEM;
        Size queueMemSize = g_instance.smb_cxt.SMBAlyPageQueue->size * sizeof(SMBAnalysePage);
        g_instance.smb_cxt.SMBAlyPageQueue->pages =
            (SMBAnalysePage *)palloc_huge(CurrentMemoryContext, queueMemSize);
        MemSet((char*)g_instance.smb_cxt.SMBAlyPageQueue->pages, 0, queueMemSize);
    }

    ret = memset_sp(mgr->smbAlyItems, SMB_ALY_MAX_ITEM_SIZE, 0, SMB_ALY_MAX_ITEM_SIZE);
    securec_check(ret, "", "");
    ret = memset_sp(mgr->smbAlyBuckets, SMB_ALY_MAX_BUCKET_SIZE, 0, SMB_ALY_MAX_BUCKET_SIZE);
    securec_check(ret, "", "");

    mgr->freelist.count = SMB_ALY_MAX_ITEM;
    mgr->freelist.first = &mgr->smbAlyItems[0];
    for (uint32 i = 0; i < SMB_ALY_MAX_ITEM - 1; i++) {
        mgr->smbAlyItems[i].next = &mgr->smbAlyItems[i + 1];
    }
    g_instance.smb_cxt.SMBMgr = mgr;
}

static void SMBShutdownHandler(SIGNAL_ARGS)
{
    g_instance.smb_cxt.shutdownSMBAly = true;
}

static void SetupSignalHandlers()
{
    (void)gspqsignal(SIGHUP, SIG_IGN);
    (void)gspqsignal(SIGINT, SMBShutdownHandler);
    (void)gspqsignal(SIGTERM, SMBShutdownHandler);
    (void)gspqsignal(SIGQUIT, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, SIG_IGN);
    (void)gspqsignal(SIGUSR2, SIG_IGN);
    (void)gspqsignal(SIGCHLD, SIG_IGN);
    (void)gspqsignal(SIGTTIN, SIG_IGN);
    (void)gspqsignal(SIGTTOU, SIG_IGN);
    (void)gspqsignal(SIGCONT, SIG_IGN);
    (void)gspqsignal(SIGWINCH, SIG_IGN);
    (void)gspqsignal(SIGURG, print_stack);
    (void)gspqsignal(SIGALRM, SIG_IGN);
    
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();
}

static SMBAnalyseItem *SMBAlyPopFree()
{
    SMBAnalyseMem *mgr = g_instance.smb_cxt.SMBMgr;
    SMBAnalyseItem *item = NULL;

    if (mgr->freelist.first == NULL) {
        return NULL;
    }

    item = mgr->freelist.first;
    mgr->freelist.first = item->next;
    mgr->freelist.count--;
    item->next = NULL;

    return item;
}

static void SMBAlyRecycle(SMBAnalyseBucket *bucket)
{
    SMBAnalyseMem *mgr = g_instance.smb_cxt.SMBMgr;
    SMBAnalyseItem *item = NULL;
    SMBAnalyseItem *prev = NULL;
    SMBAnalyseItem *next = NULL;

    GetRedoRecPtr();
    item = bucket->first;
    while (item != NULL) {
        next = item->next;
        if ((uint64)item->lsn < t_thrd.xlog_cxt.RedoRecPtr) {
            if (prev == NULL) {
                bucket->first = next;
            } else {
                prev->next = next;
            }
            item->is_verified = 0;
            item->lsn = 0;

            item->next = mgr->freelist.first;
            mgr->freelist.first = item;
            mgr->freelist.count++;
            bucket->count--;
        } else {
            prev = item;
        }

        item = next;
    }
}

static void SMBAlyDoRecycle(SMBAnalyseItem **new_item)
{
    SMBAnalyseMem *mgr = g_instance.smb_cxt.SMBMgr;
    for (uint32 i = 0; i < SMB_ALY_MAX_FILE * SMB_ALY_MAX_BUCKET_PER_FILE; i++) {
        SMBAlyRecycle(&mgr->smbAlyBuckets[i]);
    }
    *new_item = SMBAlyPopFree();
}

void SMBSetPageLsn(BufferTag tag, XLogPhyBlock pblk, XLogRecPtr lsn)
{
    SMBAnalyseItem *item = NULL;
    SMBAnalyseItem *reuse_item = NULL;
    SMBAnalyseItem *new_item = NULL;
    SMBAnalyseMem *mgr = g_instance.smb_cxt.SMBMgr;
    uint32 file_hash = (tag.rnode.dbNode + tag.rnode.relNode) % SMB_ALY_MAX_FILE;
    uint32 page_hash = tag.blockNum % SMB_ALY_MAX_BUCKET_PER_FILE;
    SMBAnalyseBucket *bucket = &mgr->smbAlyBuckets[file_hash * SMB_ALY_MAX_BUCKET_PER_FILE + page_hash];

    GetRedoRecPtr();
    item = bucket->first;
    while (item != NULL) {
        if (BUFFERTAGS_EQUAL(tag, item->tag)) {
            item->lsn = lsn;
            item->pblk = pblk;
            return;
        }

        if (reuse_item == NULL && (uint64)item->lsn < t_thrd.xlog_cxt.RedoRecPtr) {
            reuse_item = item;
        }
        item = item->next;
    }

    /* if same page id item is not found, try reuse one item */
    if (reuse_item != NULL) {
        reuse_item->lsn = lsn;
        reuse_item->pblk = pblk;
        reuse_item->tag = tag;
        return;
    }

    /* if same page id item or reuse item is not found, add one free item */
    new_item = SMBAlyPopFree();
    if (new_item == NULL) {
        SMBAlyDoRecycle(&new_item);
    }

    if (new_item != NULL) {
        new_item->next = bucket->first;
        bucket->first = new_item;
        bucket->count++;
        new_item->lsn = lsn;
        new_item->pblk = pblk;
        new_item->tag = tag;
        return;
    }
    // something bad happen :(
    g_instance.smb_cxt.shutdownSMBAly = true;
}

SMBAnalyseItem *SMBAlyGetPageItem(BufferTag tag)
{
    SMBAnalyseItem *item = NULL;
    SMBAnalyseMem *mgr = g_instance.smb_cxt.SMBMgr;
    uint32 file_hash = (tag.rnode.dbNode + tag.rnode.relNode) % SMB_ALY_MAX_FILE;
    uint32 page_hash = tag.blockNum % SMB_ALY_MAX_BUCKET_PER_FILE;
    SMBAnalyseBucket *bucket = &mgr->smbAlyBuckets[file_hash * SMB_ALY_MAX_BUCKET_PER_FILE + page_hash];

    item = bucket->first;
    while (item != NULL) {
        if (BUFFERTAGS_EQUAL(tag, item->tag)) {
            return item;
        }

        item = item->next;
    }

    return NULL;
}

SMBAnalyseBucket *SMBAlyGetBucket(BufferTag tag)
{
    SMBAnalyseMem *mgr = g_instance.smb_cxt.SMBMgr;
    uint32 file_hash = (tag.rnode.dbNode + tag.rnode.relNode) % SMB_ALY_MAX_FILE;
    uint32 page_hash = tag.blockNum % SMB_ALY_MAX_BUCKET_PER_FILE;
    SMBAnalyseBucket *bucket = &mgr->smbAlyBuckets[file_hash * SMB_ALY_MAX_BUCKET_PER_FILE + page_hash];
    return bucket;
}

static void SMBSetPageLsnBatch()
{
    SMBAnalysePage *item = nullptr;
    uint64 tempLoc;
    uint64 head = pg_atomic_read_u64(&g_instance.smb_cxt.SMBAlyPageQueue->head);
    uint64 newLoc = pg_atomic_barrier_read_u64(&g_instance.smb_cxt.SMBAlyPageQueue->tail);
    uint64 expectedFlushNum = newLoc - head;

    for (uint64 i = 0; i < expectedFlushNum; i++) {
        tempLoc = (head + i) % g_instance.smb_cxt.SMBAlyPageQueue->size;
        item = &g_instance.smb_cxt.SMBAlyPageQueue->pages[tempLoc];
        SMBSetPageLsn(item->tag, item->pblk, item->lsn);
    }
    (void)pg_atomic_fetch_add_u64(&g_instance.smb_cxt.SMBAlyPageQueue->head, expectedFlushNum);
}

void SMBAnalysisAuxiliaryMain(void)
{
    ereport(LOG, (errmsg("SMB Analysis Auxiliary Start.")));
    pgstat_report_appname("SMB Analysis Auxiliary");
    pgstat_report_activity(STATE_IDLE, NULL);
    t_thrd.role = SMB_ALY_AUXILIARY_THREAD;
    SetupSignalHandlers();
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "SMBAnalyzeAuxiliaryThread",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));
    MemoryContext ctx = AllocSetContextCreate(g_instance.instance_context, "SMBAnalyzeAuxiliary",
        ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE, SHARED_CONTEXT);
    (void)MemoryContextSwitchTo(ctx);

    for (;;) {
        if (!g_instance.smb_cxt.aly_mem_init_flag) {
            pg_usleep(10000L);
            continue;
        }
        instr_time start;
        INSTR_TIME_SET_CURRENT(start);
        if (g_instance.smb_cxt.shutdownSMBAly) {
            break;
        }
        if (g_instance.smb_cxt.analyze_end_flag) {
            SMBSetPageLsnBatch();
            g_instance.smb_cxt.analyze_aux_end_flag = true;
            break;
        }
        SMBSetPageLsnBatch();
        instr_time tmp;
        INSTR_TIME_SET_CURRENT(tmp);
        INSTR_TIME_SUBTRACT(tmp, start);
        uint64 totalTime = INSTR_TIME_GET_MICROSEC(tmp);
        if (totalTime < THOUSAND_MICROSECOND) {
            pg_usleep(THOUSAND_MICROSECOND - totalTime);
        }
    }

    ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, true);
    g_instance.smb_cxt.SMBAlyAuxPID = 0;
    MemoryContextDelete(ctx);
    ereport(LOG, (errmsg("SMB Analysis Auxiliary End.")));
    proc_exit(0);
}

} // namespace smb_recovery