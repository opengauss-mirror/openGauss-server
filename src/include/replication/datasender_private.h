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
 * datasender_private.h
 *        Private definitions from replication/datasender.cpp.
 * 
 * 
 * IDENTIFICATION
 *        src/include/replication/datasender_private.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef _DATASENDER_PRIVATE_H
#define _DATASENDER_PRIVATE_H

#include "nodes/nodes.h"
#include "replication/syncrep.h"
#include "replication/walsender_private.h"
#include "replication/repl_gramparse.h"
#include "storage/latch.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "postgres.h"
#include "knl/knl_variable.h"

typedef enum DataSndState { DATASNDSTATE_STARTUP = 0, DATASNDSTATE_CATCHUP, DATASNDSTATE_STREAMING } DataSndState;

/*
 * Each datasender has a DataSnd struct in shared memory.
 */
typedef struct DataSnd {
    ThreadId pid; /* this datasender's process id, or 0 */
    int lwpId;
    DataSndState state;         /* this datasender's state */
    TimestampTz catchupTime[2]; /* time stamp of this datasender's catchup */
    SndRole sendRole;           /* role of sender */
    bool sendKeepalive;         /* do we send keepalives on this connection? */
    bool sending;               /* true if sending to standby/dummystandby */

    DataQueuePtr sendPosition;    /* data in queue has been sent up to this position */
    DataQueuePtr receivePosition; /* receiver received queue position */

    /* Protects shared variables shown above. */
    slock_t mutex;

    /*
     * Latch used by backends to wake up this datasender when it has work to
     * do.
     */
    Latch latch;
} DataSnd;

/* There is one DataSndCtl struct for the whole database cluster */
typedef struct DataSndCtlData {
    /*
     * Synchronous replication queue with one queue per request type.
     * Protected by SyncRepLock.
     */
    SHM_QUEUE SyncRepQueue;

    /* the queue_offset is to release the waiting queue and transaction  */
    DataQueuePtr queue_offset;

    /* Protects shared variables of all datalsnd */
    slock_t mutex;

    DataSnd datasnds[FLEXIBLE_ARRAY_MEMBER]; /* VARIABLE LENGTH ARRAY */
} DataSndCtlData;

extern void DataSndSetState(DataSndState state);

#endif /* _DATASENDER_PRIVATE_H */
