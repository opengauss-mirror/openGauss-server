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
 * datareceiver.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/replication/datareceiver.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef _DATARECEIVER_H
#define _DATARECEIVER_H

#include "storage/latch.h"
#include "storage/spin.h"
#include "storage/custorage.h"
#include "pgtime.h"
#include "postgres.h"
#include "knl/knl_variable.h"
#include "replication/bcm.h"
#include "replication/dataqueuedefs.h"
#include "replication/walreceiver.h"

/*
 * MAXCONNINFO: maximum size of a connection string.
 *
 * XXX: Should this move to pg_config_manual.h?
 */
#define MAXCONNINFO 1024
#define DUMMY_STANDBY_DATADIR "base/dummy_standby"

/*
 * Values for DataRcv->dataRcvState.
 */
typedef enum {
    DATARCV_STOPPED,  /* stopped and mustn't start up again */
    DATARCV_STARTING, /* launched, but the process hasn't
                       * initialized yet */
    DATARCV_RUNNING,  /* datareceiver is running */
    DATARCV_STOPPING  /* requested to stop, but still running */
} DataRcvState;

/* Shared memory area for management of datareceiver process */
typedef struct DataRcvData {
    /*
     * PID of currently active datareceiver process, its current state and
     * start time (actually, the time at which it was requested to be
     * started).
     */
    ThreadId pid;
    ThreadId writerPid;
    int lwpId;
    DataRcvState dataRcvState;
    pg_time_t startTime;

    bool isRuning;

    /*
     * Time of send and receive of any message received.
     */
    TimestampTz lastMsgSendTime;
    TimestampTz lastMsgReceiptTime;

    /*
     * Latest reported end of Data on the sender
     */
    TimestampTz latestDataEndTime;

    DataQueuePtr sendPosition;       /* sender sent queue position (remote queue) */
    DataQueuePtr receivePosition;    /* recvwriter write queue position (remote queue) */
    DataQueuePtr localWritePosition; /* recvwriter write queue position (local queue) */

    int dummyStandbySyncPercent;

    /*
     * connection string; is used for datareceiver to connect with the primary.
     */
    char conninfo[MAXCONNINFO];
    ReplConnTarget conn_target;

    Latch* datarcvWriterLatch;

    slock_t mutex; /* locks shared variables shown above */
} DataRcvData;

typedef struct data_writer_rel_key {
    RelFileNode node;  /* the relation */
    ForkNumber forkno; /* the fork number */
    int attid;         /* colum id */
    StorageEngine type;
} data_writer_rel_key;

typedef struct data_writer_rel {
    data_writer_rel_key key; /* hash key ... must be first */
    Relation reln;
    CUStorage* cuStorage;
} data_writer_rel;

extern bool dummy_data_writer_use_file;

/* prototypes for functions in datareceiver.cpp */
extern void DataReceiverMain(void);

extern Size DataRcvShmemSize(void);
extern void DataRcvShmemInit(void);

extern bool DataRcvInProgress(void);
extern void ShutdownDataRcv(void);
extern void StartupDataStreaming(void);
extern void RequestDataStreaming(const char* conninfo, ReplConnTarget conn_target);
extern void InitDummyDataNum(void);

extern void WakeupDataRcvWriter(void);
extern void wakeupWalRcvWriter(void);

/* Receive writer */
extern bool DataRcvWriterInProgress(void);
extern void DataRcvDataCleanup(void);
extern void walRcvDataCleanup(void);

/* prototypes for functions in datarcvreceiver.cpp */
extern void DataRcvWriterMain(void);
extern int DataRcvWrite(void);
extern void DataRcvSendReply(bool force, bool requestReply);

extern void SetDataRcvDummyStandbySyncPercent(int percent);
extern int GetDataRcvDummyStandbySyncPercent(void);
extern void SetDataRcvWriterPID(ThreadId tid);
extern void ProcessDataRcvInterrupts(void);
extern void CloseDataFile(void);

/* write data to disk */
extern uint32 DoDataWrite(char* buf, uint32 nbytes);

#endif /* _DATARECEIVER_H */
