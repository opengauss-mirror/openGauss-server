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
 * dataqueue.cpp
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/replication/dataqueue.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/heapam.h"
#include "access/xact.h"
#include "access/xlogutils.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "replication/catchup.h"
#include "replication/dataqueue.h"
#include "replication/datareceiver.h"
#include "replication/datasender_private.h"
#include "replication/datasender.h"
#include "replication/syncrep.h"
#include "replication/walsender.h"
#include "replication/walreceiver.h"
#include "storage/lock/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/buf/bufmgr.h"
#include "pgxc/pgxc.h"
#include "gs_bbox.h"

#define BCMElementArrayLen 8192
#define BCMElementArrayLenHalf (BCMElementArrayLen / 2)
#define InvalidRelFileNode ((RelFileNode){ 0, 0, 0, -1})
#define IsDataReplInterruptted()                                                                 \
    (InterruptPending && (t_thrd.int_cxt.QueryCancelPending || t_thrd.int_cxt.ProcDiePending) && \
     !DataSndInProgress(SNDROLE_PRIMARY_STANDBY | SNDROLE_PRIMARY_DUMMYSTANDBY))

static void UpdateDataSendPosition(void);
static void UpdateDataWritePosition(void);
static void UpdateWalDataWritePosition(void);
static void PushToBCMElementArray(const RelFileNode &rnode, BlockNumber blockNum, StorageEngine type, uint32 data_len,
                                  int attid, uint64 offset, const DataQueuePtr &queueoffset);
static void ClearBCMStatus(uint32 first, uint32 end);
static void BCMArrayDropBlock(uint32 first, uint32 end, const RelFileNode &dropnode);

void PallocBCMBCMElementArray(void);
/*
 * Initialization of shared memory for DataQueueBuffer
 */
Size DataQueueShmemSize(void)
{
    Size size = 0;
    Assert(g_instance.attr.attr_storage.DataQueueBufSize > 0);
    /* DataQueue */
    size = sizeof(DataQueueData);
    /* extra alignment padding for DataQueue I/O buffers */
    size = add_size(size, ALIGNOF_BUFFER);
    /* and the buffers themselves */
    size = add_size(size, g_instance.attr.attr_storage.DataQueueBufSize * 1024);
    return size;
}

void DataSenderQueueShmemInit(void)
{
    /* The number of buffers in a page of DataSenderQueue is 1024. */
    const int buffernum = 1024;
    /* Allocate SHM in non-single node or dummy standby mode */
    if (!IS_SINGLE_NODE || IS_DN_DUMMY_STANDYS_MODE()) {
        bool foundDataQueue = false;
        char *allocptr = NULL;
        errno_t rc = 0;

        t_thrd.dataqueue_cxt.DataSenderQueue = (DataQueueData *)ShmemInitStruct("Data Sender Queue",
                                                                                DataQueueShmemSize(), &foundDataQueue);

        if (foundDataQueue) {
            return;
        }

        rc = memset_s(t_thrd.dataqueue_cxt.DataSenderQueue, sizeof(DataQueueData), 0, sizeof(DataQueueData));
        securec_check_c(rc, "", "");

        allocptr = ((char *)t_thrd.dataqueue_cxt.DataSenderQueue) + sizeof(DataQueueData);

        /*
         * Align the start of the page buffers to an ALIGNOF_XLOG_BUFFER boundary.
         */
        allocptr = (char *)TYPEALIGN(ALIGNOF_BUFFER, allocptr);
        t_thrd.dataqueue_cxt.DataSenderQueue->pages = allocptr;
        rc = memset_s(t_thrd.dataqueue_cxt.DataSenderQueue->pages,
                      (INT2SIZET(g_instance.attr.attr_storage.DataQueueBufSize)) * buffernum, 0,
                      (INT2SIZET(g_instance.attr.attr_storage.DataQueueBufSize)) * buffernum);
        securec_check_c(rc, "", "");

        /*
         * Do basic initialization of DataQueue shared data.
         */
        t_thrd.dataqueue_cxt.DataSenderQueue->size = g_instance.attr.attr_storage.DataQueueBufSize *
                                                     buffernum; /* unit: Bytes */
        SpinLockInit(&t_thrd.dataqueue_cxt.DataSenderQueue->use_mutex);
    }
}

void DataWriterQueueShmemInit(void)
{
    /* Allocate SHM in non-single node or dummy standby mode */
    if (!IS_SINGLE_NODE || IS_DN_DUMMY_STANDYS_MODE()) {
        bool foundDataQueue = false;
        char *allocptr = NULL;
        errno_t rc = 0;

        t_thrd.dataqueue_cxt.DataWriterQueue = (DataQueueData *)ShmemInitStruct("Data Writer Queue",
                                                                                DataQueueShmemSize(), &foundDataQueue);

        if (foundDataQueue) {
            return;
        }

        if (BBOX_BLACKLIST_DATA_WRITER_QUEUE) {
            bbox_blacklist_add(DATA_WRITER_QUEUE, t_thrd.dataqueue_cxt.DataWriterQueue, DataQueueShmemSize());
        }

        rc = memset_s(t_thrd.dataqueue_cxt.DataWriterQueue, sizeof(DataQueueData), 0, sizeof(DataQueueData));
        securec_check_c(rc, "", "");

        allocptr = ((char *)t_thrd.dataqueue_cxt.DataWriterQueue) + sizeof(DataQueueData);

        /*
         * Align the start of the page buffers to an ALIGNOF_XLOG_BUFFER boundary.
         */
        allocptr = (char *)TYPEALIGN(ALIGNOF_BUFFER, allocptr);
        t_thrd.dataqueue_cxt.DataWriterQueue->pages = allocptr;
        rc = memset_s(t_thrd.dataqueue_cxt.DataWriterQueue->pages,
                      INT2SIZET(g_instance.attr.attr_storage.DataQueueBufSize) * 1024, 0,
                      INT2SIZET(g_instance.attr.attr_storage.DataQueueBufSize) * 1024);
        securec_check_c(rc, "", "");

        /*
         * Do basic initialization of DataQueue shared data.
         */
        t_thrd.dataqueue_cxt.DataWriterQueue->size = g_instance.attr.attr_storage.DataQueueBufSize * 1024; /* unit:
                                                                                                              Bytes */
        SpinLockInit(&t_thrd.dataqueue_cxt.DataWriterQueue->use_mutex);
    }
}

/*
 * Reset the given data queue data.
 */
void ResetDataQueue(DataQueueData *data_queue)
{
    DataQueuePtr invalidptr = (DataQueuePtr){ 0, 0 };
    DataQueuePtr queue_tail1 = (DataQueuePtr){ 0, 0 };
    DataQueuePtr queue_head2 = (DataQueuePtr){ 0, 0 };
    DataQueuePtr queue_tail2 = (DataQueuePtr){ 0, 0 };

    if (data_queue == NULL)
        return; /* nothing to do */

    /* Keep the queue size and page start location. */
    SpinLockAcquire(&data_queue->use_mutex);
    queue_tail1 = data_queue->use_tail1;
    queue_head2 = data_queue->use_head2;
    queue_tail2 = data_queue->use_tail2;
    data_queue->use_tail1 = invalidptr;
    data_queue->use_head2 = invalidptr;
    data_queue->use_tail2 = invalidptr;
    SpinLockRelease(&data_queue->use_mutex);

    if (!DataQueuePtrIsInvalid(queue_tail1) || !DQByteEQ(queue_head2, queue_tail2))
        ereport(WARNING, (errmsg("data remained in reset data queue: tail1:%u/%u,head2:%u/%u,tail2:%u/%u",
                                 queue_tail1.queueid, queue_tail1.queueoff, queue_head2.queueid, queue_head2.queueoff,
                                 queue_tail2.queueid, queue_tail2.queueoff)));
}

/*
 * Is the given data queue data already empty or not?
 */
bool DataQueueIsEmpty(DataQueueData *data_queue)
{
    DataQueuePtr queue_tail1 = (DataQueuePtr){ 0, 0 };
    DataQueuePtr queue_head2 = (DataQueuePtr){ 0, 0 };
    DataQueuePtr queue_tail2 = (DataQueuePtr){ 0, 0 };
    bool isEmpty = false;

    if (data_queue == NULL)
        return true; /* nothing to do */

    /* Keep the queue size and page start location. */
    SpinLockAcquire(&data_queue->use_mutex);
    queue_tail1 = data_queue->use_tail1;
    queue_head2 = data_queue->use_head2;
    queue_tail2 = data_queue->use_tail2;
    SpinLockRelease(&data_queue->use_mutex);

    if (!DataQueuePtrIsInvalid(queue_tail1) || !DQByteEQ(queue_head2, queue_tail2))
        isEmpty = false;
    else
        isEmpty = true;

    return isEmpty;
}

DataQueuePtr PushToSenderQueue(const RelFileNode &rnode, BlockNumber blockNum, StorageEngine type, const char *mem,
                               uint32 data_len, int attid, uint64 offset)
{
    uint32 total_len;
    uint32 buffer_size = g_instance.attr.attr_storage.DataQueueBufSize * 1024; /* unit: Bytes */
    uint32 freespace_head = 0;
    uint32 data_header_len = sizeof(DataElementHeaderData);
    uint32 time_count = 1;
    DataQueuePtr invalidPtr = (DataQueuePtr){ 0, 0 };
    DataElementHeaderData data_header;
    errno_t errorno = EOK;

    if (!u_sess->attr.attr_storage.enable_stream_replication ||
        t_thrd.postmaster_cxt.HaShmData->current_mode == NORMAL_MODE)
        return invalidPtr;

    /* the array is empty, Initialize the save_send_dummy_count */
    if (t_thrd.dataqueue_cxt.BCMElementArrayIndex1 == 0 &&
        t_thrd.dataqueue_cxt.BCMElementArrayIndex2 == BCMElementArrayLenHalf) {
        pg_memory_barrier();
        t_thrd.dataqueue_cxt.save_send_dummy_count = send_dummy_count;
    }

    Assert(mem != NULL && data_len > 0);

    /* structure the data_header. */
    RelFileNodeRelCopy(data_header.rnode, rnode);
    data_header.blocknum = blockNum;
    data_header.attid = (int)((uint32)attid | ((uint32)(rnode.bucketNode + 1) << 16));
    data_header.type = type;
    data_header.data_size = data_len;

    if (type == ROW_STORE)
        data_header.offset = BLCKSZ * (blockNum % ((BlockNumber)RELSEG_SIZE));
    else if (type == COLUMN_STORE)
        data_header.offset = offset;

    data_header.ref_rec_ptr = InvalidXLogRecPtr;

    /* total_len + header_len + data_len */
    total_len = sizeof(uint32) + data_header_len + data_len;

#ifdef DATA_DEBUG
    /* caculate the data page crc */
    INIT_CRC32(data_header.data_crc);
    COMP_CRC32(data_header.data_crc, mem, data_len);
    FIN_CRC32(data_header.data_crc);
#endif

    /* get enough buffer */
    do {
        LWLockAcquire(DataSyncRepLock, LW_SHARED);

        SpinLockAcquire(&t_thrd.dataqueue_cxt.DataSenderQueue->use_mutex);

        if (buffer_size - t_thrd.dataqueue_cxt.DataSenderQueue->use_tail2.queueoff > total_len &&
            DataQueuePtrIsInvalid(t_thrd.dataqueue_cxt.DataSenderQueue->use_tail1)) {
            /* update use_tail2 */
            freespace_head = t_thrd.dataqueue_cxt.DataSenderQueue->use_tail2.queueoff;
            DQByteAdvance(t_thrd.dataqueue_cxt.DataSenderQueue->use_tail2, total_len);
            data_header.queue_offset = t_thrd.dataqueue_cxt.DataSenderQueue->use_tail2;
            SpinLockRelease(&t_thrd.dataqueue_cxt.DataSenderQueue->use_mutex);
            break;
        } else if ((t_thrd.dataqueue_cxt.DataSenderQueue->use_head2.queueoff -
                    t_thrd.dataqueue_cxt.DataSenderQueue->use_tail1.queueoff) > total_len) {
            /* update use_tail1 */
            freespace_head = t_thrd.dataqueue_cxt.DataSenderQueue->use_tail1.queueoff;
            if (DataQueuePtrIsInvalid(t_thrd.dataqueue_cxt.DataSenderQueue->use_tail1))
                t_thrd.dataqueue_cxt.DataSenderQueue->use_tail1.queueid++;
            DQByteAdvance(t_thrd.dataqueue_cxt.DataSenderQueue->use_tail1, total_len);
            data_header.queue_offset = t_thrd.dataqueue_cxt.DataSenderQueue->use_tail1;
            SpinLockRelease(&t_thrd.dataqueue_cxt.DataSenderQueue->use_mutex);
            break;
        }

        SpinLockRelease(&t_thrd.dataqueue_cxt.DataSenderQueue->use_mutex);

        /* print the log every 10s */
        if (time_count % 10000 == 0)
            ereport(LOG, (errmsg("can not get enough freespace for page to be sent")));
        LWLockRelease(DataSyncRepLock);

        if (g_instance.attr.attr_storage.max_wal_senders > 0) {
            if (t_thrd.walsender_cxt.WalSndCtl->sync_master_standalone && !IS_SHARED_STORAGE_MODE) {
                ereport(
                    LOG,
                    (errmsg("failed to push rnode %u/%u/%u blockno %u into data-queue becuase sync_master_standalone "
                            "is false."
                            " attid %d, pageoffset2blockno %lu, size %u",
                            rnode.spcNode, rnode.dbNode, rnode.relNode, blockNum, attid, offset / BLCKSZ, data_len)));
                return invalidPtr;
            } else
                DataSndWakeup();
        }

        /*
         * Check interrupts before we get in sleep, and we can not use
         * CHECK_FOR_INTERRUPTS because we have hold the page lwlock.
         */
        if (IsDataReplInterruptted()) {
            ereport(
                LOG,
                (errmsg("failed to push rnode %u/%u/%u blockno %u into data-queue becuase InterruptPending is true and"
                        " datasender is not in progress. attid %d, pageoffset2blockno %lu, size %u",
                        rnode.spcNode, rnode.dbNode, rnode.relNode, blockNum, attid, offset / BLCKSZ, data_len)));
            return invalidPtr;
        }

        /* wait for 1ms */
        CatchupShutdownIfNoDataSender();
        WaitState oldStatus = pgstat_report_waitstatus(STATE_WAIT_DATASYNC_QUEUE);
        pg_usleep(1000L);
        time_count++;
        (void)pgstat_report_waitstatus(oldStatus);
    } while (true);

    // copy data to buffer
    // 0. total_len
    // 1. data header
    // 2. data
    errorno = memcpy_s(t_thrd.dataqueue_cxt.DataSenderQueue->pages + freespace_head,
                       g_instance.attr.attr_storage.DataQueueBufSize * 1024 - freespace_head, &total_len,
                       sizeof(uint32));
    securec_check(errorno, "", "");
    freespace_head += sizeof(uint32);

    errorno = memcpy_s(t_thrd.dataqueue_cxt.DataSenderQueue->pages + freespace_head,
                       g_instance.attr.attr_storage.DataQueueBufSize * 1024 - freespace_head, &data_header,
                       data_header_len);
    securec_check(errorno, "", "");
    freespace_head += data_header_len;

    errorno = memcpy_s(t_thrd.dataqueue_cxt.DataSenderQueue->pages + freespace_head,
                       g_instance.attr.attr_storage.DataQueueBufSize * 1024 - freespace_head, mem, data_len);
    securec_check(errorno, "", "");

    LWLockRelease(DataSyncRepLock);

    /*
     * Put the header of page to the array, if the array will be full, to clear
     * a part of array
     */
    PushToBCMElementArray(rnode, blockNum, type, data_len, attid, offset, data_header.queue_offset);

    Assert(!DataQueuePtrIsInvalid(data_header.queue_offset));

    /* return the offset of DataSenderQueue */
    return data_header.queue_offset;
}

DataQueuePtr PushToWriterQueue(const char *mem, uint32 mem_len)
{
    uint32 buffer_size = g_instance.attr.attr_storage.DataQueueBufSize * 1024; /* unit: Bytes */
    uint32 freespace_head = 0;
    DataQueuePtr current_offset = (DataQueuePtr){ 0, 0 };
    uint32 time_count = 1;
    ThreadId writerPid = 0;
    errno_t errorno = EOK;

    /* get enough buffer */
    do {
        LWLockAcquire(DataSyncRepLock, LW_SHARED);

        SpinLockAcquire(&t_thrd.dataqueue_cxt.DataWriterQueue->use_mutex);

        if (buffer_size - t_thrd.dataqueue_cxt.DataWriterQueue->use_tail2.queueoff >= mem_len &&
            DataQueuePtrIsInvalid(t_thrd.dataqueue_cxt.DataWriterQueue->use_tail1)) {
            /* update use_tail2 */
            freespace_head = t_thrd.dataqueue_cxt.DataWriterQueue->use_tail2.queueoff;
            DQByteAdvance(t_thrd.dataqueue_cxt.DataWriterQueue->use_tail2, mem_len);
            current_offset = t_thrd.dataqueue_cxt.DataWriterQueue->use_tail2;
            SpinLockRelease(&t_thrd.dataqueue_cxt.DataWriterQueue->use_mutex);
            break;
        } else if ((t_thrd.dataqueue_cxt.DataWriterQueue->use_head2.queueoff -
                    t_thrd.dataqueue_cxt.DataWriterQueue->use_tail1.queueoff) >= mem_len) {
            /* update use_tail1 */
            freespace_head = t_thrd.dataqueue_cxt.DataWriterQueue->use_tail1.queueoff;
            if (DataQueuePtrIsInvalid(t_thrd.dataqueue_cxt.DataWriterQueue->use_tail1))
                t_thrd.dataqueue_cxt.DataWriterQueue->use_tail1.queueid++;
            DQByteAdvance(t_thrd.dataqueue_cxt.DataWriterQueue->use_tail1, mem_len);
            current_offset = t_thrd.dataqueue_cxt.DataWriterQueue->use_tail1;
            SpinLockRelease(&t_thrd.dataqueue_cxt.DataWriterQueue->use_mutex);
            break;
        }

        SpinLockRelease(&t_thrd.dataqueue_cxt.DataWriterQueue->use_mutex);
        LWLockRelease(DataSyncRepLock);

        /* print the log every 10s */
        if (time_count % 1000 == 0)
            ereport(LOG, (errmsg("can not get enough freespace for page to be write")));

        /* when datawriter and datareceiver is started, wake up DataRcvWriter */
        if (!g_instance.attr.attr_storage.enable_mix_replication) {
            volatile DataRcvData *datarcv = t_thrd.datareceiver_cxt.DataRcv;
            ProcessDataRcvInterrupts();
            SpinLockAcquire(&datarcv->mutex);
            writerPid = datarcv->writerPid;
            SpinLockRelease(&datarcv->mutex);
            if (writerPid != 0) {
                WakeupDataRcvWriter();
                /* Keepalived with primary when waiting flush wal data */
                DataRcvSendReply(false, false);
                /* sleep 10ms */
                pg_usleep(10000L);
                time_count++;
            } else
                DataRcvDataCleanup();
        } else { /* when there is only walrcv and walwriter, wake up WalRcvWriter */
            volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
            ProcessWalRcvInterrupts();
            SpinLockAcquire(&walrcv->mutex);
            writerPid = walrcv->writerPid;
            SpinLockRelease(&walrcv->mutex);
            if (writerPid != 0) {
                wakeupWalRcvWriter();
                /* Keepalived with primary when waiting flush wal data */
                XLogWalRcvSendReply(false, false);
                /* sleep 10ms */
                pg_usleep(10000L);
                time_count++;
            } else
                walRcvDataCleanup();
        }
    } while (true);

    /* copy data to buffer */
    errorno = memcpy_s(t_thrd.dataqueue_cxt.DataWriterQueue->pages + freespace_head,
                       g_instance.attr.attr_storage.DataQueueBufSize * 1024 - freespace_head, mem, mem_len);
    securec_check(errorno, "", "");
    LWLockRelease(DataSyncRepLock);

    if (u_sess->attr.attr_storage.HaModuleDebug) {
        ereport(LOG, (errmsg("HA-PushToWriterQueue done: data size %u, from %u to %u/%u", mem_len, freespace_head,
                             current_offset.queueid, current_offset.queueoff)));
    }

    /* Return the offset of  data queue */
    return current_offset;
}

/*
 * retrun copied size, used in three situations below:
 * 1. walsender send data, between use_head2 and use_tail2, controlled by WalSndCtl
 *     when multi walsenders exist;
 * 2. when wal and data is written by walwriter and datawriter separately, used by datawriter to fetch what
 *     is pushed into DataWriterQueue by datareceiver
 * 3. when enable_mix_replication is on, fetch both wal log and data from DataWriterQueue
 */
uint32 GetFromDataQueue(char *&buf, int bufsize, DataQueuePtr &startptr, DataQueuePtr &endptr, bool amIWriter,
                        DataQueueData *data_queue)
{
    /* get the total_len of the send data */
    uint32 page_len = 0;
    uint32 mem_len = 0;
    int buf_max_len = bufsize;
    bool ChangeFromRightToLeft = false;
    DataQueuePtr current_tail2 = { 0, 0 };

    /* when enable_mix_replication open, only amIWriter == true is allowed */
    if (amIWriter == false && g_instance.attr.attr_storage.enable_mix_replication) {
        ereport(ERROR, (errcode(ERRCODE_AMBIGUOUS_PARAMETER),
                        errmsg("we should be a writer when enable_mix_replication is on")));
    }

    SpinLockAcquire(&data_queue->use_mutex);

    /* When DataSender restart, its sendposition = 0 */
    if (DataQueuePtrIsInvalid(startptr)) {
        startptr = data_queue->use_head2;
    }

    /* Right side sent done, turn to Left side */
    if (DQByteEQ(data_queue->use_head2, data_queue->use_tail2)) {
        /* I am the slowest Data Sender to trigger the ChangeFromRightToLeft */
        if (DataQueuePtrIsInvalid(data_queue->use_tail1)) {
            /* Sender Queue is empty */
            SpinLockRelease(&data_queue->use_mutex);
            return 0;
        } else {
            ChangeFromRightToLeft = true;
        }
    } else {
        Assert(DQByteLT(data_queue->use_head2, data_queue->use_tail2));

        if (DQByteLE(data_queue->use_tail2, startptr)) {
            /* I am a faster Data Sender */
            SpinLockRelease(&data_queue->use_mutex);
            return 0;
        }
    }

    if (ChangeFromRightToLeft) {
        /* Change header2/tail2 pointer to Left side */
        data_queue->use_head2.queueid++;
        data_queue->use_head2.queueoff = 0;
        data_queue->use_tail2 = data_queue->use_tail1;
        data_queue->use_tail1.queueoff = 0;
        Assert(data_queue->use_head2.queueid == data_queue->use_tail2.queueid);

        SpinLockRelease(&data_queue->use_mutex);

        /*
         * when datareceiver or datasender is started, update the position in DataRcv or DataSndCtl
         * when enable_mix_replication is on, only update local_write_pos in WalRcv
         */
        if (!g_instance.attr.attr_storage.enable_mix_replication) {
            if (amIWriter) {
                UpdateDataWritePosition();
            } else {
                UpdateDataSendPosition();
            }
        } else {
            UpdateWalDataWritePosition();
        }

        startptr = data_queue->use_head2;

        SpinLockAcquire(&data_queue->use_mutex);
    }

    endptr = startptr;
    current_tail2 = data_queue->use_tail2;
    SpinLockRelease(&data_queue->use_mutex);

    LWLockAcquire(DataSyncRepLock, LW_EXCLUSIVE);

    Assert(current_tail2.queueoff > startptr.queueoff);

    /* if dataqueue < bufsize , copy all dataqueue to bufsize */
    if (current_tail2.queueoff - startptr.queueoff <= (uint32)bufsize) {
        endptr = current_tail2;
    } else {
        while (bufsize >= 0) {
            page_len = *(uint32 *)(data_queue->pages + endptr.queueoff);
            endptr.queueoff += page_len;
            bufsize -= page_len;
        }
        endptr.queueoff -= page_len;

        Assert(DQByteLE(endptr, current_tail2) && DQByteLT(startptr, endptr));
    }

    mem_len = endptr.queueoff - startptr.queueoff;

    if (amIWriter) {
        /* get the position to be written. */
        buf = data_queue->pages + startptr.queueoff;
    } else {
        errno_t errorno = EOK;
        /* copy data to buf for sending to datareceiver */
        errorno = memcpy_s(buf, buf_max_len, data_queue->pages + startptr.queueoff, mem_len);
        securec_check(errorno, "", "");
    }

    LWLockRelease(DataSyncRepLock);

    if (u_sess->attr.attr_storage.HaModuleDebug) {
        ereport(LOG, (errmsg("HA-GetFromDataQueue: start %u/%u, end %u/%u, head2 %u/%u, tail1 %u/%u, tail2 %u/%u",
                             startptr.queueid, startptr.queueoff, endptr.queueid, endptr.queueoff,
                             data_queue->use_head2.queueid, data_queue->use_head2.queueoff,
                             data_queue->use_tail1.queueid, data_queue->use_tail1.queueoff,
                             data_queue->use_tail2.queueid, data_queue->use_tail2.queueoff)));
    }

    return mem_len;
}

void PopFromDataQueue(const DataQueuePtr &position, DataQueueData *data_queue)
{
    SpinLockAcquire(&data_queue->use_mutex);
    if ((position.queueid == data_queue->use_tail2.queueid) && DQByteLE(position, data_queue->use_tail2)) {
        data_queue->use_head2.queueoff = position.queueoff;
    }
    SpinLockRelease(&data_queue->use_mutex);

    return;
}

static void UpdateDataSendPosition(void)
{
    int i = 0;

    for (i = 0; i < g_instance.attr.attr_storage.max_wal_senders; i++) {
        /* use volatile pointer to prevent code rearrangement */
        volatile DataSnd *datasnd = &t_thrd.datasender_cxt.DataSndCtl->datasnds[i];

        SpinLockAcquire(&datasnd->mutex);

        if (datasnd->pid != 0) {
            datasnd->sendPosition.queueid++;
            datasnd->sendPosition.queueoff = 0;
        }

        SpinLockRelease(&datasnd->mutex);
    }
}

static void UpdateWalDataWritePosition(void)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;

    SpinLockAcquire(&walrcv->mutex);

    if (walrcv->pid != 0) {
        walrcv->local_write_pos.queueid++;
        walrcv->local_write_pos.queueoff = 0;
    }

    SpinLockRelease(&walrcv->mutex);
}

static void UpdateDataWritePosition(void)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile DataRcvData *datarcv = t_thrd.datareceiver_cxt.DataRcv;

    SpinLockAcquire(&datarcv->mutex);

    if (datarcv->pid != 0) {
        datarcv->localWritePosition.queueid++;
        datarcv->localWritePosition.queueoff = 0;
    }

    SpinLockRelease(&datarcv->mutex);
}

void PushCUToDataQueue(Relation rel, int col, const char *mem, _in_ uint64 offset, _in_ int size, bool setbcm)
{
    int align_size = CUAlignUtils::GetCuAlignSizeColumnId(col);
    Buffer bcmbuffer = InvalidBuffer;
    uint64 cuSliceOffset = offset;
    uint64 cuBlock = 0;
    int i = 0;
    int cuUnitCount = (size / align_size);

#define SLICE_SIZE (512 * 1024)

    if (NORMAL_MODE == t_thrd.postmaster_cxt.HaShmData->current_mode || IS_DN_WITHOUT_STANDBYS_MODE())
        return;

    CHECK_FOR_INTERRUPTS();

    /* Set BCM status */
    if (setbcm) {
        BlockNumber curBcmBlock = 0;
        BlockNumber nextBcmBlock = 0;

        /* read current bcm block */
        cuSliceOffset = offset;
        uint64 cu_align_size = (uint64)(uint32)CUAlignUtils::GetCuAlignSizeColumnId(col);
        curBcmBlock = (BlockNumber)cstore_offset_to_bcmblock(cuSliceOffset, cu_align_size);
        nextBcmBlock = curBcmBlock;
        BCM_CStore_pin(rel, col, cuSliceOffset, &bcmbuffer);
        LockBuffer(bcmbuffer, BUFFER_LOCK_EXCLUSIVE);

        do {
            /* deal with bcm block switch */
            if (nextBcmBlock != curBcmBlock) {
                curBcmBlock = nextBcmBlock;

                /* release last bcm block and read in the next one */
                UnlockReleaseBuffer(bcmbuffer);

                BCM_CStore_pin(rel, col, cuSliceOffset, &bcmbuffer);
                LockBuffer(bcmbuffer, BUFFER_LOCK_EXCLUSIVE);
            }
            cuBlock = cstore_offset_to_cstoreblock(cuSliceOffset, cu_align_size);
            BCMSetStatusBit(rel, cuBlock, bcmbuffer, NOTSYNCED, col);

            cuSliceOffset += cu_align_size;
            nextBcmBlock = (BlockNumber)cstore_offset_to_bcmblock(cuSliceOffset, cu_align_size);
        } while (cuSliceOffset < offset + size);

        UnlockReleaseBuffer(bcmbuffer);

        /* record one log_cu_bcm xlog */
        BCMLogCU(rel, offset, col, NOTSYNCED, cuUnitCount);
    }

    if (size <= Min(g_instance.attr.attr_storage.DataQueueBufSize, g_instance.attr.attr_storage.MaxSendSize) * 256) {
        t_thrd.proc->waitDataSyncPoint = PushToSenderQueue(rel->rd_node, 0, COLUMN_STORE, mem, size, col, offset);
    } else {
        uint32 remain = 0;
        /* 512KB per cu slice */
        for (i = 0; i < (size / SLICE_SIZE); i++) {
            /*
             * For all the intermediate CU_Data slices we MUST SET the latest_ref_xlog to InvalidXLogRecPtr to prevend
             * the synchronization of the replication data on the standby node.
             */
            t_thrd.proc->waitDataSyncPoint = PushToSenderQueue(rel->rd_node, 0, COLUMN_STORE, mem + (i * SLICE_SIZE),
                                                               SLICE_SIZE, col, offset + (i * SLICE_SIZE));
        }
        /* the remaining data less than 512KB */
        remain = (uint32)(size % SLICE_SIZE);
        if (remain > 0) {
            t_thrd.proc->waitDataSyncPoint = PushToSenderQueue(rel->rd_node, 0, COLUMN_STORE, mem + (i * SLICE_SIZE),
                                                               remain, col, offset + (i * SLICE_SIZE));
        }
    }

    if (u_sess->attr.attr_storage.HaModuleDebug) {
        ereport(LOG, (errmsg("HA-PushToSenderQueue done: rnode %u/%u/%u, blockno %lu,\
						   cuUnitCount %d, attid %d, waitpoint %u/%u",
                             rel->rd_node.spcNode, rel->rd_node.dbNode, rel->rd_node.relNode, offset / align_size,
                             cuUnitCount, col, t_thrd.proc->waitDataSyncPoint.queueid,
                             t_thrd.proc->waitDataSyncPoint.queueoff)));
    }

    /* Wake up all datasenders to send Page if replication is enabled */
    if (g_instance.attr.attr_storage.max_wal_senders > 0)
        DataSndWakeup();
}

static void PushToBCMElementArray(const RelFileNode &rnode, BlockNumber blockNum, StorageEngine type, uint32 data_len,
                                  int attid, uint64 offset, const DataQueuePtr &queueoffset)
{
    uint32 array_index = 0;
    uint32 &index1 = t_thrd.dataqueue_cxt.BCMElementArrayIndex1;
    uint32 &index2 = t_thrd.dataqueue_cxt.BCMElementArrayIndex2;
    volatile DataSndCtlData *datasndctl = t_thrd.datasender_cxt.DataSndCtl;
    errno_t rc = EOK;

    /* Initialize BCMElementArray */
    if (t_thrd.dataqueue_cxt.BCMElementArray == NULL) {
        MemoryContext oldcxt = NULL;

        oldcxt = MemoryContextSwitchTo(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));
        t_thrd.dataqueue_cxt.BCMElementArray = (BCMElement)palloc0(BCMElementArrayLen * sizeof(BCMElementData));
        MemoryContextSwitchTo(oldcxt);
    }

    /*
     * First to push to the first half of array, or push to the second
     * half of array
     */
    if (index1 < BCMElementArrayLenHalf) {
        array_index = index1++;
        rc = memcpy_s(t_thrd.dataqueue_cxt.BCMElementArrayOffset1, sizeof(DataQueuePtr), &queueoffset,
                      sizeof(DataQueuePtr));
        securec_check(rc, "", "");

        /*
         * when the array is full,
         * if primary send data to dummystandby, the send_dummy_count will increase,
         * then the save_send_dummy_count != send_dummy_count, we will not clear
         * the bcm file, and set array to empty.
         * if save_send_dummy_count == send_dummy_count, we will clear the second half of array,
         * and set the index2 = BCMElementArrayLenHalf.
         */
        if (index1 == BCMElementArrayLenHalf && index2 == BCMElementArrayLen) {
            /*
             * we should wait the second half data has been send to the standby,
             * then clear the BCMArray.
             */
            while (DQByteLT(datasndctl->queue_offset, *t_thrd.dataqueue_cxt.BCMElementArrayOffset2)) {
                /*
                 * Check interrupts before we get in sleep, and we can not use
                 * CHECK_FOR_INTERRUPTS because we have hold the page lwlock.
                 */
                if (IsDataReplInterruptted())
                    ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("Insert BCM Info to be interrupted.")));

                CatchupShutdownIfNoDataSender();
                pg_usleep(1000L); /* 1ms */
            }

            pg_memory_barrier();
            if (t_thrd.dataqueue_cxt.save_send_dummy_count == send_dummy_count) {
                ClearBCMStatus(BCMElementArrayLenHalf, BCMElementArrayLen);
            } else {
                index1 = 0;
            }
            index2 = BCMElementArrayLenHalf;
        }
    } else {
        Assert(index1 == BCMElementArrayLenHalf);

        array_index = index2++;
        rc = memcpy_s(t_thrd.dataqueue_cxt.BCMElementArrayOffset2, sizeof(DataQueuePtr), &queueoffset,
                      sizeof(DataQueuePtr));
        securec_check(rc, "", "");

        /* array is full */
        if (index2 == BCMElementArrayLen) {
            /*
             * we should wait the first half data has been send to the standby,
             * then clear the BCMArray.
             */
            while (DQByteLT(datasndctl->queue_offset, *t_thrd.dataqueue_cxt.BCMElementArrayOffset1)) {
                if (IsDataReplInterruptted())
                    ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("Insert BCM Info to be interrupted.")));

                CatchupShutdownIfNoDataSender();
                pg_usleep(1000L); /* 1ms */
            }

            if (t_thrd.dataqueue_cxt.save_send_dummy_count == send_dummy_count)
                ClearBCMStatus(0, BCMElementArrayLenHalf);
            else
                index2 = BCMElementArrayLenHalf;
            index1 = 0;
        }
    }

    Assert(index2 >= BCMElementArrayLenHalf);
    Assert(array_index < BCMElementArrayLen);

    if ((index2 < BCMElementArrayLenHalf) || (array_index >= BCMElementArrayLen))
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                        errmsg("The got BCM Array index is corrupt: index1 %u index2 %u array_index %u "
                               "BCMElementArrayOffset1 %X/%X BCMElementArrayOffset2 %X/%X",
                               index1, index2, array_index, t_thrd.dataqueue_cxt.BCMElementArrayOffset1->queueid,
                               t_thrd.dataqueue_cxt.BCMElementArrayOffset1->queueoff,
                               t_thrd.dataqueue_cxt.BCMElementArrayOffset2->queueid,
                               t_thrd.dataqueue_cxt.BCMElementArrayOffset2->queueoff)));

    RelFileNodeRelCopy(t_thrd.dataqueue_cxt.BCMElementArray[array_index].rnode, rnode);

    t_thrd.dataqueue_cxt.BCMElementArray[array_index].blocknum = blockNum;
    t_thrd.dataqueue_cxt.BCMElementArray[array_index].attid =
        (int)((uint32)attid | ((uint32)(rnode.bucketNode + 1) << 16));
    t_thrd.dataqueue_cxt.BCMElementArray[array_index].type = type;
    t_thrd.dataqueue_cxt.BCMElementArray[array_index].offset = offset;
    t_thrd.dataqueue_cxt.BCMElementArray[array_index].data_size = data_len;
    t_thrd.dataqueue_cxt.BCMElementArray[array_index].is_vaild = true;
}

bool BCMArrayIsEmpty(void)
{
    return (t_thrd.dataqueue_cxt.BCMElementArrayIndex1 == 0 &&
            t_thrd.dataqueue_cxt.BCMElementArrayIndex2 == BCMElementArrayLenHalf);
}

void ResetBCMArray(void)
{
    t_thrd.dataqueue_cxt.BCMElementArrayIndex1 = 0;
    t_thrd.dataqueue_cxt.BCMElementArrayIndex2 = BCMElementArrayLenHalf;
}

void ClearBCMArray(void)
{
    pg_memory_barrier();
    /* If no data send to dummystandby, we will clear the BCM status */
    if (t_thrd.dataqueue_cxt.save_send_dummy_count == send_dummy_count) {
        ClearBCMStatus(0, t_thrd.dataqueue_cxt.BCMElementArrayIndex1);
        ClearBCMStatus(BCMElementArrayLenHalf, t_thrd.dataqueue_cxt.BCMElementArrayIndex2);
    }

    /* After clear, set the array to empty. */
    ResetBCMArray();
}

static void ClearBCMStatus(uint32 first, uint32 end)
{
    RelFileNode curnode = InvalidRelFileNode;
    RelFileNode prevnode = InvalidRelFileNode;
    BlockNumber blockNum;
    Buffer bcmbuffer;
    Relation relation = NULL;
    BCMElementData bcmhdr;

    while (first < end) {
        bcmhdr = t_thrd.dataqueue_cxt.BCMElementArray[first];
        RelFileNodeCopy(curnode, bcmhdr.rnode, GETBUCKETID(bcmhdr.attid));
        blockNum = bcmhdr.blocknum;

        if (!bcmhdr.is_vaild) {
            first++;
            continue;
        }

        if (memcmp(&prevnode, &curnode, sizeof(RelFileNode)) != 0) {
            prevnode = curnode;

            if (relation) {
                FreeFakeRelcacheEntry(relation);
                relation = NULL;
            }
            relation = CreateFakeRelcacheEntry(prevnode);
        }
        Assert(relation != NULL);

        if (relation == NULL) {
            ereport(ERROR, (errmsg("Invalid relation while clearing BCM status: rnode[%u,%u,%u], blocknum[%u], "
                                   "pageoffset[%lu], size[%u], attid[%d]",
                                   bcmhdr.rnode.spcNode, bcmhdr.rnode.dbNode, 
                                   bcmhdr.rnode.relNode, bcmhdr.blocknum,
                                   bcmhdr.offset, bcmhdr.data_size, (int)GETATTID((uint32)bcmhdr.attid))));
        }

        ereport(DEBUG5, (errmsg("clear BCM status: rnode[%u,%u,%u], blocknum[%u], "
                                "pageoffset[%lu], size[%u], attid[%d]",
                                bcmhdr.rnode.spcNode, bcmhdr.rnode.dbNode, bcmhdr.rnode.relNode, 
                                bcmhdr.blocknum, bcmhdr.offset, bcmhdr.data_size, 
                                (int)GETATTID((uint32)bcmhdr.attid))));

        if (bcmhdr.type == ROW_STORE) {
            Buffer buffer;
            Page page;

            /* Set BCM status */
            BCM_pin(relation, blockNum, &bcmbuffer);
            LockBuffer(bcmbuffer, BUFFER_LOCK_EXCLUSIVE);
            BCMSetStatusBit(relation, blockNum, bcmbuffer, SYNCED);
            UnlockReleaseBuffer(bcmbuffer);

            /* clear the logical page flag */
            buffer = ReadBuffer(relation, blockNum);
            if (!BufferIsValid(buffer)) {
                ereport(ERROR,
                        (errcode(ERRCODE_DATA_EXCEPTION), errmsg("buffer should be valid, but now is %d", buffer)));
            }
            LockBuffer(buffer, BUFFER_LOCK_SHARE);
            page = (Page)BufferGetPage(buffer);
            if (PageIsLogical(page)) {
                PageClearLogical(page);
                MarkBufferDirty(buffer);
            }
            UnlockReleaseBuffer(buffer);
        } else {
            BlockNumber curBcmBlock = 0;
            BlockNumber nextBcmBlock = 0;
            uint64 cuSliceOffset = 0;
            uint64 cuBlock = 0;
            uint64 align_size = (uint64)(uint32)CUAlignUtils::GetCuAlignSizeColumnId(bcmhdr.attid);
            int cuUnitCount = (bcmhdr.data_size / align_size);

            /* read current bcm block */
            cuSliceOffset = bcmhdr.offset;
            curBcmBlock = cstore_offset_to_bcmblock(cuSliceOffset, align_size);
            nextBcmBlock = curBcmBlock;
            BCM_CStore_pin(relation, (int)GETATTID((uint32)bcmhdr.attid), cuSliceOffset, &bcmbuffer);
            LockBuffer(bcmbuffer, BUFFER_LOCK_EXCLUSIVE);

            do {
                /* deal with bcm block switch */
                if (nextBcmBlock != curBcmBlock) {
                    curBcmBlock = nextBcmBlock;

                    /* release last bcm block and read in the next one */
                    UnlockReleaseBuffer(bcmbuffer);

                    BCM_CStore_pin(relation, (int)GETATTID((uint32)bcmhdr.attid), cuSliceOffset, &bcmbuffer);
                    LockBuffer(bcmbuffer, BUFFER_LOCK_EXCLUSIVE);
                }

                cuBlock = cstore_offset_to_cstoreblock(cuSliceOffset, align_size);
                BCMSetStatusBit(relation, cuBlock, bcmbuffer, SYNCED, (int)GETATTID((uint32)bcmhdr.attid));

                cuSliceOffset += align_size;
                nextBcmBlock = cstore_offset_to_bcmblock(cuSliceOffset, align_size);
            } while (cuSliceOffset < bcmhdr.offset + (uint64)bcmhdr.data_size);

            UnlockReleaseBuffer(bcmbuffer);

            /* record one log_cu_bcm xlog */
            BCMLogCU(relation, bcmhdr.offset, (int)GETATTID((uint32)bcmhdr.attid), SYNCED, cuUnitCount);

            if (u_sess->attr.attr_storage.HaModuleDebug) {
                ereport(LOG, (errmsg("HA-ClearBCMStatus: rnode %u/%u/%u, col %u, blockno %lu "
                                     "cuUnitCount %u, status  %u",
                                     relation->rd_node.spcNode, relation->rd_node.dbNode, relation->rd_node.relNode,
                                     GETATTID((uint)bcmhdr.attid), bcmhdr.offset / align_size,
                                     bcmhdr.data_size / (uint32)align_size, SYNCED)));
            }
        }
        first++;
    }

    if (relation)
        FreeFakeRelcacheEntry(relation);
}

/*
 * Invaild the dropnode relfilenode block in the bcm element array
 */
static void BCMArrayDropBlock(uint32 first, uint32 end, const RelFileNode &dropnode)
{
    while (first < end) {
        if (!t_thrd.dataqueue_cxt.BCMElementArray[first].is_vaild) {
            first++;
            continue;
        }

        RelFileNode tmp_node;
        int bucket_id = GETBUCKETID(t_thrd.dataqueue_cxt.BCMElementArray[first].attid);
        RelFileNodeCopy(tmp_node, t_thrd.dataqueue_cxt.BCMElementArray[first].rnode, bucket_id);

        if (RelFileNodeEquals(dropnode, tmp_node))
            t_thrd.dataqueue_cxt.BCMElementArray[first].is_vaild = false;

        first++;
    }
}

/*
 * Drop all invaild dropnode block at bcm element array.
 * First drop the first half part, then drop the end half part.
 */
void BCMArrayDropAllBlocks(const RelFileNode &dropnode)
{
    BCMArrayDropBlock(0, t_thrd.dataqueue_cxt.BCMElementArrayIndex1, dropnode);
    BCMArrayDropBlock(BCMElementArrayLenHalf, t_thrd.dataqueue_cxt.BCMElementArrayIndex2, dropnode);
}

/*
 * @Description: if the table need WAL, log and copy cu data.
 * @Param[IN] attrId: which attribute of column relation.
 * @Param[IN] cuData: CU data
 * @Param[IN] cuFileOffset: CU data offset
 * @Param[IN] cuSize: CU data size
 * @Param[IN] rel: relation for CU data replication
 * @See also:
 */
void CStoreCUReplication(_in_ Relation rel, _in_ int attrId, _in_ char *cuData, _in_ int cuSize,
                         _in_ uint64 cuFileOffset)
{
    if (RelationNeedsWAL(rel)) {
        /* one primary muti standby and enable_mix_replication is off need reocord cu xlog. */
        if ((IS_DN_MULTI_STANDYS_MODE() && !g_instance.attr.attr_storage.enable_mix_replication)) {
            log_logical_newcu(&rel->rd_node, MAIN_FORKNUM, attrId, cuFileOffset, cuSize, cuData);
        } else {
            if (g_instance.attr.attr_common.enable_tsdb && RelationIsTsStore(rel)) {
                /* timeseries table use xlog to sync data */
                log_logical_newcu(&rel->rd_node, MAIN_FORKNUM, attrId, cuFileOffset, cuSize, cuData);
            } else {
                log_logical_newcu(&rel->rd_node, MAIN_FORKNUM, attrId, cuFileOffset, cuSize, NULL);
                if (!g_instance.attr.attr_storage.enable_mix_replication) {
                    /* Push CU to replicate queue */
                    PushCUToDataQueue(rel, attrId, cuData, cuFileOffset, cuSize, true);
                }
            }
        }
    }

    return;
}

static void HeapSyncHashCreate(void)
{
    /* if no the hash table we will create it */
    if (t_thrd.dataqueue_cxt.heap_sync_rel_tab == NULL) {
        HASHCTL ctl;
        errno_t rc = 0;
        rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
        securec_check(rc, "", "");
        ctl.keysize = sizeof(heap_sync_rel_key);
        ctl.entrysize = sizeof(heap_sync_rel);
        ctl.hash = tag_hash;
        t_thrd.dataqueue_cxt.heap_sync_rel_tab = hash_create("heap sync rel table", 100, &ctl,
                                                             HASH_ELEM | HASH_FUNCTION);
    } else
        return;
}

void HeapSyncHashSearch(Oid rd_id, HASHACTION action)
{
    heap_sync_rel_key key;
    errno_t rc = 0;
    rc = memset_s(&key, sizeof(heap_sync_rel_key), 0, sizeof(heap_sync_rel_key));
    securec_check(rc, "", "");
    key.rd_id = rd_id;

    if (t_thrd.dataqueue_cxt.heap_sync_rel_tab == NULL)
        HeapSyncHashCreate();

    hash_search(t_thrd.dataqueue_cxt.heap_sync_rel_tab, (void *)&key, action, NULL);
}

void AtAbort_RelationSync(void)
{
    HASH_SEQ_STATUS status;
    heap_sync_rel *hentry = NULL;
    Relation rel;

    if (t_thrd.dataqueue_cxt.heap_sync_rel_tab != NULL) {
        hash_seq_init(&status, t_thrd.dataqueue_cxt.heap_sync_rel_tab);
        while ((hentry = (heap_sync_rel *)hash_seq_search(&status)) != NULL) {
            rel = relation_open(hentry->key.rd_id, NoLock);
            /*
             * Here we don't try to lock related partition, partition would not be deleted during aborting
             * because we still hold RowExclusiveLock. If we try to lock again, something bad may happen.
             * Scene for example:
             *      1. error when 'copy from' : errept
             *      2. AbortTransaction
             *      3. we try to open partition with AccessShareLock
             *      4. fail to palloc when we build locallock->owner during calling LockAcquire
             *      5. errept and AbortTransaction again
             *      6. we try to access locallock->owner but it's NULL, so we get Segmentation fault.
             */
            heap_sync(rel, NoLock);
            relation_close(rel, NoLock);

            hash_search(t_thrd.dataqueue_cxt.heap_sync_rel_tab, (void *)&hentry->key, HASH_REMOVE, NULL);
        }
    }
}

void AtCommit_RelationSync(void)
{
    if (t_thrd.dataqueue_cxt.heap_sync_rel_tab != NULL &&
        hash_get_num_entries(t_thrd.dataqueue_cxt.heap_sync_rel_tab) > 0) {
        ereport(PANIC, (errmsg("heap sync hash table not cleaned, num of entries:%ld",
                               hash_get_num_entries(t_thrd.dataqueue_cxt.heap_sync_rel_tab))));
    }
}

void PallocBCMBCMElementArray(void)
{
    if (t_thrd.dataqueue_cxt.BCMElementArray == NULL) {
        MemoryContext oldcxt = NULL;

        oldcxt = MemoryContextSwitchTo(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));
        t_thrd.dataqueue_cxt.BCMElementArray = (BCMElement)palloc0(BCMElementArrayLen * sizeof(BCMElementData));
        MemoryContextSwitchTo(oldcxt);
    }
}
