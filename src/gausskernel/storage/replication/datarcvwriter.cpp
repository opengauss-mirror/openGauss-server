/* -------------------------------------------------------------------------
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * datarcvwriter.cpp
 *	 functions for data receive management
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/replication/datarcvwriter.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/xlogutils.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "catalog/catalog.h"
#include "catalog/pg_tablespace.h"
#include "replication/dataqueue.h"
#include "replication/datareceiver.h"
#include "replication/datasender.h"
#include "replication/walsender.h"
#include "storage/buf/bufmgr.h"
#include "storage/cu.h"
#include "storage/custorage.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/lmgr.h"
#include "storage/smgr/smgr.h"
#include "storage/smgr/relfilenode_hash.h"
#include "postmaster/alarmchecker.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/resowner.h"
#include "instruments/gs_stack.h"
#include "gssignal/gs_signal.h"

#define MAX_DUMMY_DATA_FILE (RELSEG_SIZE * BLCKSZ)

typedef enum {
    NONEEXISTDATABASEDIR = 0,
    EXISTDATABASEDIR,
    DATABASEDIRCREATECANCEL
} DataBaseDirState;

/* max dummy data write file (default: 1GB) */
static int dummy_data_writer_file_fd = -1;
static uint32 dummy_data_writer_file_offset = 0;
bool dummy_data_writer_use_file = false;

/* Signal handlers */
static void DataRcvWriterSigHupHandler(SIGNAL_ARGS);
static void DataRcvWriterQuickDie(SIGNAL_ARGS);
static void ReqShutdownHandler(SIGNAL_ARGS);

static void SetDataRcvWriterLatch(void);
static void EmptyDataRcvWriterLatch(void);

static void DummyStandbyDoDataWrite(char *buf, uint32 nbytes);

static void DataWriterHashCreate(void);
static bool DataWriterHashSearch(const RelFileNode &node, int attid, ForkNumber forkno, StorageEngine type,
                                 HASHACTION action, Relation &reln, CUStorage *&cuStorage);
static void DataWriterHashRemove(bool flushdata);
static bool DatabaseHashSearch(Oid spcoid, Oid dboid);
static DataBaseDirState CheckDatabaseReady(Oid spcNode, Oid dbNode);
static void DummyStandbyDataRcvWrite(char *buf, uint32 nbytes);
static bool CanWriteBuffer(SMgrRelation smgr, ForkNumber forkNum, const char *path);

/*
 * Called when the DataRcvWriterMain is ending.
 */
static void ShutdownDataRcvWriter(int code, Datum arg)
{
    /* If waiting, get off wait queue (should only be needed after error),clear WriterPid */
    LockErrorCleanup();
    LockReleaseCurrentOwner();
    EmptyDataRcvWriterLatch();
}

void DataRcvWriterMain(void)
{
    sigjmp_buf localSigjmpBuf;
    MemoryContext datarcvWriterContext;
    t_thrd.xlog_cxt.InRecovery = true;

    ereport(LOG, (errmsg("datarcvwriter thread started")));
    /*
     * Reset some signals that are accepted by postmaster but not here
     */
    (void)gspqsignal(SIGHUP, DataRcvWriterSigHupHandler);
    (void)gspqsignal(SIGINT, SIG_IGN);
    (void)gspqsignal(SIGTERM, ReqShutdownHandler);
    (void)gspqsignal(SIGQUIT, DataRcvWriterQuickDie); /* hard crash time */
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, SIG_IGN);
    (void)gspqsignal(SIGUSR2, SIG_IGN);
    (void)gspqsignal(SIGURG, print_stack);
    /*
     * Reset some signals that are accepted by postmaster but not here
     */
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);

    /* We allow SIGQUIT (quickdie) at all times */
    (void)sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGQUIT);

    on_shmem_exit(ShutdownDataRcvWriter, 0);

    /*
     * Create a resource owner to keep track of our resources (currently only
     * buffer pins).
     */
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "DataReceive Writer",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));

    /*
     * Create a memory context that we will do all our work in.  We do this so
     * that we can reset the context during error recovery and thereby avoid
     * possible memory leaks.  Formerly this code just ran in
     * t_thrd.top_mem_cxt, but resetting that would be a really bad idea.
     */
    datarcvWriterContext = AllocSetContextCreate(t_thrd.top_mem_cxt, "DataReceive Writer", ALLOCSET_DEFAULT_MINSIZE,
                                                 ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    (void)MemoryContextSwitchTo(datarcvWriterContext);

    /* init the dummy standby data num to write */
    if (dummyStandbyMode) {
        InitDummyDataNum();
        t_thrd.datarcvwriter_cxt.dummy_data_writer_file_num++;
        dummy_data_writer_file_offset = 0;
    }

    /*
     * If an exception is encountered, processing resumes here.
     *
     * See notes in postgres.c about the design of this coding.
     */
    int curTryCounter = 0;
    int *oldTryCounter = NULL;
    if (sigsetjmp(localSigjmpBuf, 1) != 0) {
        gstrace_tryblock_exit(true, oldTryCounter);

        /* Since not using PG_TRY, must reset error stack by hand */
        t_thrd.log_cxt.error_context_stack = NULL;

        /* Prevent interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /* Report the error to the server log */
        EmitErrorReport();

        /* abort async io, must before LWlock release */
        AbortAsyncListIO();

        /* release resource held by lsc */
        AtEOXact_SysDBCache(false);

        /*
         * These operations are really just a minimal subset of
         * AbortTransaction().  We don't have very many resources to worry
         * about in pagewriter, but we do have LWLocks, buffers, and temp files.
         */
        LWLockReleaseAll();
        AbortBufferIO();
        UnlockBuffers();
        /* buffer pins are released here: */
        ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, true);
        /* we needn't bother with the other ResourceOwnerRelease phases */
        AtEOXact_Buffers(false);

        /* If waiting, get off wait queue (should only be needed after error) */
        LockErrorCleanup();
        LockReleaseCurrentOwner();

        AtEOXact_Files();
        AtEOXact_HashTables(false);

        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */
        (void)MemoryContextSwitchTo(datarcvWriterContext);
        FlushErrorState();

        /* Close fd in data writer hash table if any. */
        DataWriterHashRemove(false);

        /* Flush any leaked data in the top-level context */
        MemoryContextResetAndDeleteChildren(datarcvWriterContext);

        /* Now we can allow interrupts again */
        RESUME_INTERRUPTS();

        /*
         * Sleep at least 1 second after any error.  A write error is likely
         * to be repeated, and we don't want to be filling the error logs as
         * fast as we can.
         */
        pg_usleep(1000000L);

        /*
         * Close all open files after any error.  This is helpful on Windows,
         * where holding deleted files open causes various strange errors.
         * It's not clear we need it elsewhere, but shouldn't hurt.
         */
        smgrcloseall();
    }
    oldTryCounter = gstrace_tryblock_entry(&curTryCounter);

    /* We can now handle ereport(ERROR) */
    t_thrd.log_cxt.PG_exception_stack = &localSigjmpBuf;

    /*
     * Unblock signals (they were blocked when the postmaster forked us)
     */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    /*
     * Use the recovery target timeline ID during recovery
     */
    if (!RecoveryInProgress())
        ereport(FATAL, (errmsg("cannot continue Data streaming, recovery has already ended")));

    t_thrd.xlog_cxt.ThisTimeLineID = GetRecoveryTargetTLI();

    /*
     * register procLatch to datarcv shared memory
     */
    SetDataRcvWriterLatch();

    /*
     * init a hash table to store the rel file node fd.
     */
    DataWriterHashCreate();

    pgstat_report_appname("Data Receive Writer");
    pgstat_report_activity(STATE_IDLE, NULL);

    /*
     * Loop forever
     */
    for (;;) {
        int rc;

        /* Clear any already-pending wakeups */
        ResetLatch(&t_thrd.proc->procLatch);

        if (t_thrd.datarcvwriter_cxt.gotSIGHUP) {
            t_thrd.datarcvwriter_cxt.gotSIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        while (!t_thrd.datarcvwriter_cxt.shutdownRequested && DataRcvWrite() > 0)
            ;

        if (t_thrd.datarcvwriter_cxt.shutdownRequested) {
            ereport(LOG, (errmsg("datarcvwriter thread shut down")));
            /*
             * From here on, elog(ERROR) should end with exit(1), not send
             * control back to the sigsetjmp block above
             */
            u_sess->attr.attr_common.ExitOnAnyError = true;
            /* Normal exit from the pagewriter is here */
            proc_exit(0); /* done */
        }

        rc = WaitLatch(&t_thrd.proc->procLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, (long)1000 /* ms */);
        if (rc & WL_POSTMASTER_DEATH) {
            ereport(LOG, (errmsg("datarcvwriter thread shut down with exit code 1")));
            gs_thread_exit(1);
        }
    }
}

static void DataRcvWriterSigHupHandler(SIGNAL_ARGS)
{
    int saveErrno = errno;

    t_thrd.datarcvwriter_cxt.gotSIGHUP = true;
    if (t_thrd.proc) {
        SetLatch(&t_thrd.proc->procLatch);
    }

    errno = saveErrno;
}

static void DataRcvWriterQuickDie(SIGNAL_ARGS)
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

static void ReqShutdownHandler(SIGNAL_ARGS)
{
    int saveErrno = errno;

    t_thrd.datarcvwriter_cxt.shutdownRequested = true;

    /* cancel the wait for database directory */
    t_thrd.int_cxt.ProcDiePending = true;
    if (t_thrd.proc) {
        SetLatch(&t_thrd.proc->procLatch);
    }
    errno = saveErrno;
}

void SetDataRcvWriterPID(ThreadId tid)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile DataRcvData *datarcv = t_thrd.datareceiver_cxt.DataRcv;
    SpinLockAcquire(&datarcv->mutex);
    datarcv->writerPid = tid;
    SpinLockRelease(&datarcv->mutex);
}

static void SetDataRcvWriterLatch(void)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile DataRcvData *datarcv = t_thrd.datareceiver_cxt.DataRcv;
    SpinLockAcquire(&datarcv->mutex);
    datarcv->datarcvWriterLatch = &t_thrd.proc->procLatch;
    datarcv->writerPid = t_thrd.proc_cxt.MyProcPid;
    SpinLockRelease(&datarcv->mutex);
}

static void EmptyDataRcvWriterLatch(void)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile DataRcvData *datarcv = t_thrd.datareceiver_cxt.DataRcv;
    SpinLockAcquire(&datarcv->mutex);
    datarcv->datarcvWriterLatch = NULL;
    datarcv->writerPid = 0;
    SpinLockRelease(&datarcv->mutex);
}

bool DataRcvWriterInProgress(void)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile DataRcvData *datarcv = t_thrd.datareceiver_cxt.DataRcv;

    SpinLockAcquire(&datarcv->mutex);
    if (datarcv->writerPid == 0) {
        SpinLockRelease(&datarcv->mutex);
        return false;
    }
    SpinLockRelease(&datarcv->mutex);
    return true;
}

/*
 * Write data to disk of DummyStandby.
 */
static void DummyStandbyDoDataWrite(char *buf, uint32 nbytes)
{
    uint32 sum = 0;
    uint32 currentlen = 0;
    char *caculatebuf = buf;
    dummy_data_writer_use_file = true;
    errno_t rc = 0;
    while (nbytes > 0) {
        rc = memcpy_s((void *)&currentlen, sizeof(uint32), caculatebuf, sizeof(uint32));
        securec_check(rc, "", "");
        if ((sum + currentlen) > (uint32)g_instance.attr.attr_storage.MaxSendSize * 1024) {
            DummyStandbyDataRcvWrite(buf, (uint32)sum);
            buf = caculatebuf;
            sum = 0;
        } else {
            caculatebuf += currentlen;
            sum += currentlen;
            nbytes -= currentlen;
        }
    }
    DummyStandbyDataRcvWrite(buf, (uint32)sum);
    dummy_data_writer_use_file = false;
}

/*
 * Write data to disk, the data is acquire by GetFromDataQueue.
 * 1. When enable_mix_replication is off, and Data sender/DataRcv started,
 *     we periodly Write old-format ROW_STORE and COLUMN_STORE data to disk
 * 2. When enable_mix_replication is on, the data is from WalRcv, and written to
 *     disk by walrcvwriter, for 'd'message. In this case, we have skip the new-format
 *     data when call DoDataWrite
 */
uint32 DoDataWrite(char *buf, uint32 nbytes)
{
#define InvalidRelFileNode ((RelFileNode){ 0, 0, 0, -1})

    RelFileNode curnode = InvalidRelFileNode;
    RelFileNode prevnode = InvalidRelFileNode;
    int curattid = -1;
    int prevattid = -1;

    DataElementHeaderData datahdr;
    DataQueuePtr lastqueueoffset = { 0, 0 };
    /* buf unit */
    uint32 currentlen = 0;
    int headerlen = sizeof(DataElementHeaderData);
    Relation reln = NULL;
    CUStorage *cuStorage = NULL;
    errno_t errorno = EOK;

#ifdef DATA_DEBUG
    pg_crc32 crc;
#endif

    while (nbytes > 0) {
        if (!g_instance.attr.attr_storage.enable_mix_replication) {
            /* parse total_len for this slice of data */
            errorno = memcpy_s(&currentlen, sizeof(uint32), buf, sizeof(uint32));
            securec_check(errorno, "", "");
            buf += sizeof(uint32);
        }

        /* parse data element header, and skip the header to parse payload below */
        errorno = memcpy_s((void *)&datahdr, headerlen, buf, headerlen);
        securec_check(errorno, "", "");
        RelFileNodeCopy(curnode, datahdr.rnode, GETBUCKETID(datahdr.attid));
        curattid = (int)GETATTID((uint32)datahdr.attid);
        buf += headerlen;

        if (!g_instance.attr.attr_storage.enable_mix_replication) {
            Assert(currentlen == (sizeof(uint32) + (uint32)headerlen + datahdr.data_size));
            /* for release version, if data is invalid, longjmp */
            if (currentlen != (sizeof(uint32) + (uint32)headerlen + datahdr.data_size)) {
                ereport(PANIC, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("corrupt wal data write len %u bytes, "
                                                                               "the expected write data_size %u",
                                                                               currentlen, datahdr.data_size)));
            }
        }
        ereport(DEBUG5, (errmsg("DoDataWrite write page: rnode[%u,%u,%u], blocknum[%u], "
                                "pageoffset[%lu], size[%u], queueoffset[%u/%u], attid[%d]",
                                datahdr.rnode.spcNode, datahdr.rnode.dbNode, datahdr.rnode.relNode,
                                datahdr.blocknum, datahdr.offset, datahdr.data_size, 
                                datahdr.queue_offset.queueid, datahdr.queue_offset.queueoff,
                                (int)GETATTID((uint32)datahdr.attid))));

#ifdef DATA_DEBUG
        INIT_CRC32(crc);
        COMP_CRC32(crc, buf, datahdr.data_size);
        FIN_CRC32(crc);

        if (!EQ_CRC32(datahdr.data_crc, crc)) {
            ereport(PANIC, (errmsg("writing incorrect data page checksum at: "
                                   "rnode[%u,%u,%u], blocknum[%u], "
                                   "pageoffset[%u], size[%u], queueoffset[%u/%u]",
                                   datahdr.rnode.spcNode, datahdr.rnode.dbNode, datahdr.rnode.relNode,
                                   datahdr.blocknum, datahdr.offset, datahdr.data_size,
                                   datahdr.queue_offset.queueid, datahdr.queue_offset.queueoff)));
        }
#endif
        /* when enable_mix_replication is on, the ROW_STORE type is not supported now! */
        if (g_instance.attr.attr_storage.enable_mix_replication && ROW_STORE == datahdr.type) {
            Assert(false);
            ereport(PANIC, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("The Row Store Heap Log SHOULD NOT BE synchronized in the WAL Streaming. "
                                   "Tracking the data header info: "
                                   "rnode[%u/%u/%u] blocknum[%u] pageoffset[%lu] "
                                   "size[%u] queueoffset[%u/%u].",
                                   datahdr.rnode.spcNode, datahdr.rnode.dbNode, datahdr.rnode.relNode,
                                   datahdr.blocknum, datahdr.offset, datahdr.data_size,
                                   datahdr.queue_offset.queueid, datahdr.queue_offset.queueoff)));
            return nbytes;
        }

        if ((memcmp(&curnode, &prevnode, sizeof(RelFileNode)) != 0) || (curattid != prevattid)) {
            prevnode = curnode;
            prevattid = curattid;

            if (!DataWriterHashSearch(curnode, curattid, MAIN_FORKNUM, datahdr.type, HASH_FIND, reln, cuStorage)) {
                /*
                 * Before we open the smgr, we should insure that the database
                 * directory of current relation is ready based on previous write.
                 * If it's not ready, wait for recovery to create.
                 */
                if ((curnode.spcNode != GLOBALTABLESPACE_OID) && !DatabaseHashSearch(curnode.spcNode, curnode.dbNode)) {
                    DataBaseDirState databaseDirState = CheckDatabaseReady(curnode.spcNode, curnode.dbNode);
                    if (databaseDirState == DATABASEDIRCREATECANCEL) {
                        /*
                         * when enable_mix_replcation is off,
                         * Cancel has been requested, give up the following write.
                         * when enable_mix_replcation is on, ERROR
                         */
                        if (!g_instance.attr.attr_storage.enable_mix_replication) {
                            WakeupDataRecovery();
                        } else {
                            ereport(ERROR, (errmsg("Failed to write the wal data: the database path %s doesn't exist.",
                                                   GetDatabasePath(curnode.dbNode, curnode.spcNode))));
                        }
                        break;
                    } else if (databaseDirState == NONEEXISTDATABASEDIR) {
                        /* xlog is already redo where we dropped the database, so we skip this record */
                        buf += datahdr.data_size;
                        nbytes -= currentlen;
                        continue;
                    } else {
                        /* database dir is already been created, go ahead */
                        ;
                    }
                }

                if (ROW_STORE == datahdr.type) {
                    reln = CreateFakeRelcacheEntry(curnode);

                    /* Open rel at the smgr level if not already done */
                    RelationOpenSmgr(reln);

                    /* insert it to the hash table */
                    DataWriterHashSearch(curnode, curattid, MAIN_FORKNUM, ROW_STORE, HASH_ENTER, reln, cuStorage);
                } else if (COLUMN_STORE == datahdr.type) {
                    CFileNode cFileNode(curnode);
                    cFileNode.m_forkNum = MAIN_FORKNUM;
                    cFileNode.m_attid = curattid;

                    cuStorage = New(CurrentMemoryContext) CUStorage(cFileNode);

                    /* insert it to the hash table */
                    DataWriterHashSearch(curnode, curattid, MAIN_FORKNUM, COLUMN_STORE, HASH_ENTER, reln, cuStorage);
                }
            }
        }

        /*
         * Row store write data page by Buffer Manager API
         *
         * Lock relation for extension is nessesary for write received pages.
         * We use this lock in reader, this doesn't comply with the principle
         * of lock relation for extension, then read buffer, then lock buffer,
         * then release relation extension. Cause standby server doesn't vacuum
         * the pages, it's ok.
         */
        if (ROW_STORE == datahdr.type) {
            Buffer buffer;
            Page page;
            SMgrRelation smgr;
            char* path = NULL;

#ifdef ENABLE_MULTIPLE_NODES
            LockRelFileNode(curnode, AccessShareLock);
#endif
            smgr = smgropen(curnode, InvalidBackendId);
            path = relpath(smgr->smgr_rnode, MAIN_FORKNUM);

        retry:
            /* Do not dirty the buffer any more if relation file was unlinked after we open it */
            if (CanWriteBuffer(smgr, MAIN_FORKNUM, path) == true) {
                /*
                 * We use RBM_ZERO_ON_ERROR rather than RBM_ZERO_AND_LOCK here
                 * 'cause if the page has not been loaded into buffer, buffer
                 * manager would just allocate a zero buffer rather than read the
                 * page from smgr. See more in ReadBuffer_common() comments inside.
                 */
                buffer = XLogReadBufferExtended(curnode, MAIN_FORKNUM, datahdr.blocknum, RBM_ZERO_ON_ERROR, NULL);
                Assert(BufferIsValid(buffer));
                LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
                page = (Page)BufferGetPage(buffer);

                /* Ignore pages with small LSN */
                XLogRecPtr local_lsn;
                XLogRecPtr peer_lsn;
                local_lsn = PageGetLSN(page);
                peer_lsn = PageGetLSN((Page)buf);
                /*
                 * In data page replicated scenes, maybe appear an old page to cover
                 * an new page. Explame for two scenes:
                 * 1: when standby is crash, page A will be copyed to dummystandby,
                 *    then dummystandby is crash, stanby is ok, and primary update
                 *    A to A', standby will get A', then primary is crash, stanby
                 *    will failover, it will get page A to cover the page A'.
                 * 2: when session1 is copying page A to table t1; session2 is update
                 *    page A to A'; standby redo the A' first, then write the A, and
                 *    A will cover A'.
                 */
                if (XLByteLT(peer_lsn, local_lsn))
                    ; /* do nothing */
                else {
                    errno_t rc = 0;
                    rc = memcpy_s(page, BLCKSZ, buf, datahdr.data_size);
                    securec_check(rc, "", "");
                    MarkBufferDirty(buffer);
                }
                UnlockReleaseBuffer(buffer);
            } else {
                /* check again */
                if (CheckFileExists(path) == FILE_EXIST) {
                    goto retry;
                }
                ereport(WARNING, (errmsg("HA-DoDataWrite: No File Write(file not exists), rnode %u/%u/%u, blockno %u ",
                                         curnode.spcNode, curnode.dbNode, curnode.relNode,
                                         datahdr.blocknum)));
            }
#ifdef ENABLE_MULTIPLE_NODES
            UnlockRelFileNode(curnode, AccessShareLock);
#endif
            pfree_ext(path);

            if (u_sess->attr.attr_storage.HaModuleDebug) {
                ereport(LOG, (errmsg("HA-DoDataWrite: rnode %u/%u/%u, blockno %u ", curnode.spcNode, curnode.dbNode,
                                     curnode.relNode, datahdr.blocknum)));
            }
        } else if (COLUMN_STORE == datahdr.type) {
            Assert(cuStorage->m_cnode.m_attid == GETATTID(datahdr.attid));
            if (u_sess->attr.attr_storage.HaModuleDebug) {
                int align_size = CUAlignUtils::GetCuAlignSizeColumnId(datahdr.attid);
                check_cu_block(buf, datahdr.data_size, align_size);
                ereport(LOG, (errmsg("HA-DoDataWrite: rnode %u/%u/%u, col %u, "
                                     "blockno %lu, cuUnitCount %u",
                                     datahdr.rnode.spcNode, datahdr.rnode.dbNode, datahdr.rnode.relNode,
                                     GETATTID((uint)datahdr.attid), datahdr.offset / align_size,
                                     datahdr.data_size / align_size)));
            }
#ifdef ENABLE_MULTIPLE_NODES
            LockRelFileNode(curnode, AccessShareLock);
#endif
            /* direct write the data file when column store */
            cuStorage->SaveCU(buf, datahdr.offset, datahdr.data_size, false);

            UnlockRelFileNode(curnode, AccessShareLock);
        }

        buf += datahdr.data_size;

        if (!g_instance.attr.attr_storage.enable_mix_replication) {
            nbytes -= currentlen;
            lastqueueoffset = datahdr.queue_offset;

            /* Use volatile pointer to prevent code rearrangement */
            volatile DataRcvData *datarcv = t_thrd.datareceiver_cxt.DataRcv;

            ereport(DEBUG5, (errmsg("recwriter: receive: %u/%u, local: %u/%u, offset %u/%u",
                                    datarcv->receivePosition.queueid, datarcv->receivePosition.queueoff,
                                    datarcv->localWritePosition.queueid, datarcv->localWritePosition.queueoff,
                                    datahdr.queue_offset.queueid, datahdr.queue_offset.queueoff)));

            WakeupDataRecovery();
        } else
            nbytes -= ((uint32)headerlen + datahdr.data_size);
    }

    /* Sync all relations to the disk and then clear the hash table */
    DataWriterHashRemove(true);

    /* Update shared-memory status when datareceiver is started */
    if (!g_instance.attr.attr_storage.enable_mix_replication) {
        volatile DataRcvData *datarcv = t_thrd.datareceiver_cxt.DataRcv;

        SpinLockAcquire(&datarcv->mutex);
        datarcv->receivePosition.queueid = lastqueueoffset.queueid;
        datarcv->receivePosition.queueoff = lastqueueoffset.queueoff;
        SpinLockRelease(&datarcv->mutex);
    }

    return nbytes;
}

/*
 * Write data to disk.
 */
int DataRcvWrite(void)
{
    /* Use volatile pointer to prevent code rearrangement */
    volatile DataRcvData *datarcv = t_thrd.datareceiver_cxt.DataRcv;
    char *writeBuf = NULL;
    uint32 nbytes = 0;
    uint32 remainbytes = 0;
    DataQueuePtr curstartpos;
    DataQueuePtr curendpos;

    /* avoid concurrent WalDataRcvWrite by WalRcvWriteLock */
    START_CRIT_SECTION();
    LWLockAcquire(RcvWriteLock, LW_EXCLUSIVE);

    SpinLockAcquire(&datarcv->mutex);
    curstartpos.queueid = datarcv->localWritePosition.queueid;
    curstartpos.queueoff = datarcv->localWritePosition.queueoff;
    SpinLockRelease(&datarcv->mutex);

    nbytes = GetFromDataQueue(writeBuf, g_instance.attr.attr_storage.DataQueueBufSize * 1024, curstartpos, curendpos,
                              true, t_thrd.dataqueue_cxt.DataWriterQueue);
    if (nbytes == 0) {
        LWLockRelease(RcvWriteLock);
        END_CRIT_SECTION();
        return 0;
    }

    /* Write data. */
    if (dummyStandbyMode)
        DummyStandbyDoDataWrite(writeBuf, nbytes);
    else
        remainbytes = DoDataWrite(writeBuf, nbytes);

    /* Close all open files */
    smgrcloseall();

    /* Update share memory from the the next operation. */
    nbytes -= remainbytes;
    SpinLockAcquire(&datarcv->mutex);
    datarcv->localWritePosition.queueid = curstartpos.queueid;
    datarcv->localWritePosition.queueoff = curstartpos.queueoff;
    DQByteAdvance(datarcv->localWritePosition, nbytes);
    SpinLockRelease(&datarcv->mutex);

    PopFromDataQueue(((DataRcvData *)datarcv)->localWritePosition, t_thrd.dataqueue_cxt.DataWriterQueue);

    LWLockRelease(RcvWriteLock);
    END_CRIT_SECTION();

    return nbytes;
}

static void DataWriterHashCreate(void)
{
    /* if no the hash table we will create it */
    if (t_thrd.datarcvwriter_cxt.data_writer_rel_tab == NULL) {
        HASHCTL ctl;
        errno_t rc = 0;
        rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
        securec_check(rc, "", "");
        ctl.keysize = sizeof(data_writer_rel_key);
        ctl.entrysize = sizeof(data_writer_rel);
        ctl.hash = DataWriterRelKeyHash;
        ctl.match = DataWriterRelKeyMatch;
        t_thrd.datarcvwriter_cxt.data_writer_rel_tab = hash_create("data writer rel table", 100, &ctl,
                                                                   HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);
    } else
        return;
}

static bool DataWriterHashSearch(const RelFileNode &node, int attid, ForkNumber forkno, StorageEngine type,
                                 HASHACTION action, Relation &reln, CUStorage *&cuStorage)
{
    data_writer_rel_key key;
    data_writer_rel *hentry = NULL;
    bool found = false;
    errno_t rc = 0;

    rc = memset_s(&key, sizeof(data_writer_rel_key), 0, sizeof(data_writer_rel_key));
    securec_check(rc, "", "");
    key.node = node;
    key.forkno = forkno;
    key.attid = attid;
    key.type = type;

    if (t_thrd.datarcvwriter_cxt.data_writer_rel_tab == NULL) {
        DataWriterHashCreate();
    }

    hentry = (data_writer_rel *)hash_search(t_thrd.datarcvwriter_cxt.data_writer_rel_tab, (void *)&key, action, &found);

    if (action == HASH_FIND) {
        /* if the action is find, we will get the fd  */
        if (found) {
            reln = hentry->reln;
            cuStorage = hentry->cuStorage;
        }
    }

    if (action == HASH_ENTER) {
        /* if the action is enter, we will insert the fd  */
        Assert(!found);
        if (!found) {
            hentry->reln = reln;
            hentry->cuStorage = cuStorage;
        }
    }

    return found;
}

static bool DatabaseHashSearch(Oid spcoid, Oid dboid)
{
    HASH_SEQ_STATUS status;
    data_writer_rel *hentry = NULL;

    if (t_thrd.datarcvwriter_cxt.data_writer_rel_tab == NULL)
        return false; /* nothing to do */

    hash_seq_init(&status, t_thrd.datarcvwriter_cxt.data_writer_rel_tab);

    while ((hentry = (data_writer_rel *)hash_seq_search(&status)) != NULL) {
        if (hentry->key.node.spcNode == spcoid && hentry->key.node.dbNode == dboid) {
            hash_seq_term(&status);
            return true;
        }
    }

    return false;
}

/*
 * Remove relations in current data writer hash table, flush data if requested.
 * We use the hash table to cach the relation info we are handling to reduce the
 * overhead of IO.
 * Call the function once, after we handle all the data in data writer queue.
 */
static void DataWriterHashRemove(bool flushdata)
{
#define ERRORDATA_FLUSH_NUM 5

    HASH_SEQ_STATUS status;
    data_writer_rel *hentry = NULL;
    Relation reln;
    CUStorage *cuStorage = NULL;

    if (t_thrd.datarcvwriter_cxt.data_writer_rel_tab == NULL)
        return;

    /*
     * After flush data failed five times, maybe the storage has damage,
     * In order to prevent operation hang up, the process will PANIC.
     */
    if (t_thrd.datarcvwriter_cxt.dataRcvWriterFlushPageErrorCount++ >= ERRORDATA_FLUSH_NUM)
        ereport(PANIC, (errmsg_internal("ERRORDATA_FLUSH_NUM exceeded")));

    hash_seq_init(&status, t_thrd.datarcvwriter_cxt.data_writer_rel_tab);
    while ((hentry = (data_writer_rel *)hash_seq_search(&status)) != NULL) {
        if (hentry->key.type == ROW_STORE && !g_instance.attr.attr_storage.enable_mix_replication) {
            reln = hentry->reln;

            if (flushdata) {
                char* path = relpath(reln->rd_smgr->smgr_rnode, MAIN_FORKNUM);
#ifdef ENABLE_MULTIPLE_NODES
                LockRelFileNode(reln->rd_node, AccessExclusiveLock);
#endif
                /* do not sync the file if it not exists any more */
                if (smgrexists(reln->rd_smgr, MAIN_FORKNUM) && CheckFileExists(path) == FILE_EXIST) {
                    FlushRelationBuffers(reln);
                    smgrimmedsync(reln->rd_smgr, MAIN_FORKNUM);
                } else {
                    ereport(WARNING, (errmsg("HA-DataWriterHashRemove: No File SYNC, rnode %u/%u/%u dose not exists",
                                             reln->rd_node.spcNode, reln->rd_node.dbNode, reln->rd_node.relNode)));
                }
#ifdef ENABLE_MULTIPLE_NODES
                UnlockRelFileNode(reln->rd_node, AccessExclusiveLock);
#endif
                pfree_ext(path);
            }
            RelationCloseSmgr(reln);
            FreeFakeRelcacheEntry(reln);
        } else if (hentry->key.type == COLUMN_STORE) {
            cuStorage = hentry->cuStorage;
            if (cuStorage != NULL) {
                if (flushdata) {
#ifdef ENABLE_MULTIPLE_NODES
                    LockRelFileNode(hentry->key.node, AccessShareLock);
#endif
                    /*
                     * unlink/truncate the cu file begin at 0, so we just check
                     * whether the xxx_Cx.0 is exist.
                     */
                    if (cuStorage->IsDataFileExist(0)) {
                        cuStorage->FlushDataFile();
                    }
#ifdef ENABLE_MULTIPLE_NODES
                    UnlockRelFileNode(hentry->key.node, AccessShareLock);
#endif
                    if (u_sess->attr.attr_storage.HaModuleDebug)
                        ereport(LOG,
                                (errmsg("HA-DataWriterHashRemove: rnode %u/%u/%u, col %d", hentry->key.node.spcNode,
                                        hentry->key.node.dbNode, hentry->key.node.relNode, hentry->key.attid)));
                }
                DELETE_EX(cuStorage);
            }
        }
        hash_search(t_thrd.datarcvwriter_cxt.data_writer_rel_tab, (void *)&hentry->key, HASH_REMOVE, NULL);
    }

    /* Reset flush page error num after flush page successfully */
    if (flushdata)
        t_thrd.datarcvwriter_cxt.dataRcvWriterFlushPageErrorCount = 0;
}

/*
 * We only handle path in pg_tblspc/. Wait until the tablepsace been redo
 * If the spcNode we got from the tablespace path does not exist. If the spcNode
 * is not a link, throw a error here.
 */
static DataBaseDirState CheckDatabaseReady(Oid spcNode, Oid dbNode)
{
    char *dbpath = NULL;
    struct stat st;
    int nRet = 0;
    DataBaseDirState dbDirState = NONEEXISTDATABASEDIR;

    dbpath = GetDatabasePath(dbNode, spcNode);
    pg_memory_barrier();

    Assert(OidIsValid(spcNode));
retry:
    if (stat(dbpath, &st) < 0) {
        pg_memory_barrier();
        /* xlog already redo where we dropped the database, so we skip this record */
        if (t_thrd.xlog_cxt.RedoDone) {
            dbDirState = NONEEXISTDATABASEDIR;
            ereport(LOG, (errmsg("skip the dropped database directory")));
            return dbDirState;
        }
        /* Directory does not exist? */
        if (errno == ENOENT) {
#if defined(HAVE_READLINK) || defined(WIN32)
            char tbpath[64] = {0};
            char linkpath[MAXPGPATH] = {0};
            int rllen = 0;

            /* wait if it's under global and base */
            if (spcNode == GLOBALTABLESPACE_OID || spcNode == DEFAULTTABLESPACE_OID)
                goto invalid_handle;

            nRet = snprintf_s(tbpath, sizeof(tbpath), 63, "pg_tblspc/%u", spcNode);
            securec_check_ss(nRet, "", "");

            rllen = readlink(tbpath, linkpath, sizeof(linkpath));
            bool b_einval = rllen < 0 && (errno == EINVAL || errno == ENOTDIR);
            bool b_enoent = rllen < 0 && errno == ENOENT;
            if (b_einval) {
                pfree(dbpath);
                dbpath = NULL;
                ereport(PANIC, (errmsg("could not read symbolic link \"%s\": %m", tbpath)));
            } else if (b_enoent) {
                ereport(DEBUG3, (errmsg("sleep a while waiting for tablespace \"%s\" ready", tbpath)));
                goto invalid_handle;
            } else if (rllen < 0) {
                pfree(dbpath);
                dbpath = NULL;
                ereport(ERROR, (errcode_for_file_access(), errmsg("invalid tablespace link %s: %s", tbpath, TRANSLATE_ERRNO)));
            } else if (rllen >= (int)sizeof(linkpath)) {
                pfree(dbpath);
                dbpath = NULL;
                ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                                errmsg("symbolic link \"%s\" target is too long", tbpath)));
            } else {
                linkpath[rllen] = '\0';
                if (stat(linkpath, &st) < 0 || !S_ISDIR(st.st_mode))
                    ereport(PANIC, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                                    errmsg("invalid tablespace directory %s: %s", tbpath, TRANSLATE_ERRNO)));
                goto invalid_handle;
            }
#else
            /*
             * If the platform does not have symbolic links, it should not be
             * possible to have tablespaces - clearly somebody else created
             * them. Warn about it and ignore.
             */
            pfree(dbpath);
            dbpath = NULL;
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("tablespaces are not supported on this platform")));
#endif
        } else {
            pfree(dbpath);
            dbpath = NULL;
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                            errmsg("\"database %u/%u\" invalid directory : %m", spcNode, dbNode)));
        }
    } else {
        pfree(dbpath);
        dbpath = NULL;
        /* Is it not a directory? */
        if (!S_ISDIR(st.st_mode))
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                            errmsg("\"database %u/%u\" exists but is not a directory", spcNode, dbNode)));
        dbDirState = EXISTDATABASEDIR;
        return dbDirState;
    }

invalid_handle:
    if ((t_thrd.int_cxt.ProcDiePending || t_thrd.proc_cxt.proc_exit_inprogress) &&
        !t_thrd.datarcvwriter_cxt.AmDataReceiverForDummyStandby) {
        ereport(WARNING, (errcode(ERRCODE_ADMIN_SHUTDOWN), errmsg("canceling the wait for database directory \"%s\" "
                                                                  "being created in recovery",
                                                                  dbpath)));
        pfree(dbpath);
        dbpath = NULL;
        dbDirState = DATABASEDIRCREATECANCEL;
        return dbDirState;
    }
    ereport(LOG, (errmsg("waiting for database directory \"%s\" being created in recovery", dbpath)));
    pg_usleep(5000000L);
    goto retry;
}

static void DummyStandbyDataRcvWrite(char *buf, uint32 nbytes)
{
    ssize_t byteswritten;
    char path[MAXPGPATH] = {0};
    volatile DataRcvData *datarcv = t_thrd.datareceiver_cxt.DataRcv;

    /* buf unit */
    DataElementHeaderData dataehdr;
    uint32 currentlen = 0;
    int headerlen = sizeof(DataElementHeaderData);
    uint32 caculatebytes = nbytes;
    char *caculatebuf = buf;
    int nRet = 0;

    /*
     * get  the file num to store:
     *  1. If MAX_DUMMY_DATA_FILE <= dummy_data_writer_file_offset, open
     *     the next file to store
     *  2. If the current file can not store the nbytes, discard the current file space,
     *     open the next file to store.
     */
    if (dummy_data_writer_file_offset >= MAX_DUMMY_DATA_FILE ||
        (MAX_DUMMY_DATA_FILE - dummy_data_writer_file_offset < (uint32)(nbytes + sizeof(nbytes)))) {
        ereport(DEBUG2, (errmsg("data file num %u", t_thrd.datarcvwriter_cxt.dummy_data_writer_file_num)));
        t_thrd.datarcvwriter_cxt.dummy_data_writer_file_num++;
        dummy_data_writer_file_offset = 0;

        if (dummy_data_writer_file_fd >= 0) {
            close(dummy_data_writer_file_fd);
            dummy_data_writer_file_fd = -1;
        }
    }

    /* open the file */
    if (dummy_data_writer_file_fd < 0) {
        /* get the file path */
        nRet = snprintf_s(path, sizeof(path), MAXPGPATH - 1, "base/dummy_standby/%u",
                          t_thrd.datarcvwriter_cxt.dummy_data_writer_file_num);
        securec_check_ss_c(nRet, "", "");

        dummy_data_writer_file_fd = open(path, O_RDWR | O_CREAT | PG_BINARY, S_IRUSR | S_IWUSR);
        if (dummy_data_writer_file_fd < 0)
            ereport(PANIC, (errcode_for_file_access(),
                            errmsg("could not create data file \"%s\", dummy_data_writer_file_fd=%d: %m", path,
                                   dummy_data_writer_file_fd)));
    }

    while (caculatebytes > 0) {
        errno_t rc = 0;
        rc = memcpy_s((void *)&currentlen, sizeof(uint32), caculatebuf, sizeof(uint32));
        securec_check(rc, "", "");
        caculatebuf += sizeof(uint32);

        rc = memcpy_s((void *)&dataehdr, headerlen, caculatebuf, headerlen);
        securec_check(rc, "", "");
        caculatebuf += headerlen;
        caculatebuf += dataehdr.data_size;
        caculatebytes -= currentlen;
    }

    errno = 0;
    /* OK to write the data */
    byteswritten = write(dummy_data_writer_file_fd, &nbytes, sizeof(uint32));
    if (byteswritten < (ssize_t)sizeof(uint32)) {
        /* if write didn't set errno, assume no disk space */
        if (errno == 0) {
            errno = ENOSPC;
        }
        ereport(PANIC, (errcode_for_file_access(),
                        errmsg("could not write to data file %s buffer len %u, length %u: %s", path, nbytes, nbytes, TRANSLATE_ERRNO)));
    }

    errno = 0;
    byteswritten = write(dummy_data_writer_file_fd, buf, nbytes);
    if (byteswritten < nbytes) {
        /* if write didn't set errno, assume no disk space */
        if (errno == 0)
            errno = ENOSPC;
        ereport(PANIC, (errcode_for_file_access(), errmsg("could not write to data file %s "
                                                          "at offset %u, length %u: %m",
                                                          path, (uint32)dummy_data_writer_file_offset, nbytes)));
    }

    dummy_data_writer_file_offset = dummy_data_writer_file_offset + nbytes + sizeof(nbytes);

    if (u_sess->attr.attr_storage.HaModuleDebug) {
        ereport(LOG,
                (errmsg("HA-DummyStandbyDataRcvWrite: fileno %u, nbytes %u, queueoffset %u/%u,"
                        " dummy_data_writer_file_offset %u, dummy_data_writer_file_fd %d",
                        t_thrd.datarcvwriter_cxt.dummy_data_writer_file_num, nbytes, dataehdr.queue_offset.queueid,
                        dataehdr.queue_offset.queueoff, dummy_data_writer_file_offset, dummy_data_writer_file_fd)));
    }

    /* use fdatasync to make sure the received data to flush the disk */
    if (pg_fdatasync(dummy_data_writer_file_fd) != 0)
        ereport(PANIC, (errcode_for_file_access(),
                        errmsg("could not fdatasync data file num %u, fd %d: %m",
                               t_thrd.datarcvwriter_cxt.dummy_data_writer_file_num, dummy_data_writer_file_fd)));

    /* Update shared-memory status */
    SpinLockAcquire(&datarcv->mutex);
    datarcv->receivePosition.queueid = dataehdr.queue_offset.queueid;
    datarcv->receivePosition.queueoff = dataehdr.queue_offset.queueoff;
    SpinLockRelease(&datarcv->mutex);
}

/*
 * close data fd for rm data file
 */
extern void CloseDataFile(void)
{
    close(dummy_data_writer_file_fd);
    dummy_data_writer_file_fd = -1;
    dummy_data_writer_file_offset = 0;
}

void InitDummyDataNum(void)
{
    DIR *dir = NULL;
    struct dirent *de = NULL;
    int max_num_file = 0;
    int min_num_file = 0;
    char *dirpath = DUMMY_STANDBY_DATADIR;

    /* open the dir of base/dummy_standby */
    errno = 0;
    dir = AllocateDir(dirpath);
    if (dir == NULL && errno == ENOENT) {
        if (mkdir(dirpath, S_IRWXU) < 0) {
            /* Failure other than not exists */
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not create directory \"%s\": %m", dirpath)));
        }
        dir = AllocateDir(dirpath);
    }
    if (dir == NULL) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not open directory \"%s\": %m", dirpath)));
        return;
    }

    /* loop read the file name of base/dummy_standby */
    while ((de = ReadDir(dir, dirpath)) != NULL) {
        /* Skip special stuff */
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
            continue;

        min_num_file = atoi(de->d_name);
        break;
    }

    FreeDir(dir);
    dir = AllocateDir(dirpath);

    /* loop read the file name of base/dummy_standby */
    while ((de = ReadDir(dir, dirpath)) != NULL) {
        /* Skip special stuff */
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
            continue;

        /* search the max num file */
        max_num_file = Max(atoi(de->d_name), max_num_file);
        min_num_file = Min(atoi(de->d_name), min_num_file);
        ereport(DEBUG5, (errmsg("InitDummyDataNum de->d_name=%s;   max_num_path=%d; min_num_file=%d.", de->d_name,
                                max_num_file, min_num_file)));
    }

    t_thrd.datarcvwriter_cxt.dummy_data_writer_file_num = max_num_file;
    t_thrd.datasender_cxt.dummy_data_read_file_num = min_num_file;
    FreeDir(dir);
}

/* check whether the data writer thread can write data through shared buffer. */
static bool CanWriteBuffer(SMgrRelation smgr, ForkNumber forkNum, const char *path)
{
    if (CheckFileExists(path) == FILE_EXIST) {
        return true;
    } else {
        /*
         * In the case that the physical file is unlink and the fd of file is still held by other thread, the datawriter
         * thead can not write data to shared buffer. Thus, it may dirty the shared buffer and the pendingOpsTable hash
         * table, and it may cause coredump when doing checkpoint due to unlinked file.
         */
        if ((smgr->md_fd[forkNum] == NULL) && (FdRefcntIsZero(smgr, forkNum))) {
            return true;
        } else {
            return false;
        }
    }
}
