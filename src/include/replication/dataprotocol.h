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
 * dataprotocol.h
 *        Definitions relevant to the streaming DATA transmission protocol.
 * 
 * 
 * IDENTIFICATION
 *        src/include/replication/dataprotocol.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef _DATAPROTOCOL_H
#define _DATAPROTOCOL_H

#include "access/xlogdefs.h"
#include "replication/dataqueuedefs.h"
#include "replication/replicainternal.h"
#include "datatype/timestamp.h"

/*
 * All messages from DataSender must contain these fields to allow us to
 * correctly calculate the replication delay.
 */
typedef struct {
    DataQueuePtr sendPosition;

    /* Sender's system clock at the time of transmission */
    TimestampTz sendTime;

    /*
     * If replyRequested is set, the client should reply immediately to this
     * message, to avoid a timeout disconnect.
     */
    bool replyRequested;
    bool catchup;
} DataSndMessage;

/*
 * Header for a data replication message (message type 'd').  This is wrapped within
 * a CopyData message at the FE/BE protocol level.
 *
 * The header is followed by actual data page.  Note that the data length is
 * not specified in the header --- it's just whatever remains in the message.
 *
 * walEnd and sendTime are not essential data, but are provided in case
 * the receiver wants to adjust its behavior depending on how far behind
 * it is.
 */
typedef struct {
    /* data start location of the data queue included in this message */
    DataQueuePtr dataStart;

    /* data end location of the data queue included in this message */
    DataQueuePtr dataEnd;

    /* Sender's system clock at the time of transmission */
    TimestampTz sendTime;

    bool catchup;
} DataPageMessageHeader;

/*
 * Keepalive message from primary (message type 'k'). (lowercase k)
 * This is wrapped within a CopyData message at the FE/BE protocol level.
 *
 * Note that the data length is not specified here.
 */
typedef DataSndMessage DataSndKeepaliveMessage;

/*Notify message structure for dummy scan incremental files for catchup */
typedef struct {
    /* Sender's system clock at the time of transmission */
    TimestampTz sendTime;
} NotifyDummyCatchupMessage;

/*
 * Refence :PrimaryKeepaliveMessage
 */
typedef struct EndDataMessage {
    /* Current end of WAL on the sender */
    ServerMode peer_role;
    DbState peer_state;

    /* Sender's system clock at the time of transmission */
    TimestampTz sendTime;

    int percent;
} EndDataMessage;

/*
 * Refence :PrimaryKeepaliveMessage
 * All messages from WalSender must contain these fields to allow us to
 * correctly calculate the replication delay.
 */
typedef struct RmDataMessage {
    /* Current end of WAL on the sender */
    ServerMode peer_role;
    DbState peer_state;

    /* Sender's system clock at the time of transmission */
    TimestampTz sendTime;

    /*
     * If replyRequested is set, the client should reply immediately to this
     * message, to avoid a timeout disconnect.
     */
    bool replyRequested;
} RmDataMessage;

/*
 * Reply message from standby (message type 'r').  This is wrapped within
 * a CopyData message at the FE/BE protocol level.
 *
 * Note that the data length is not specified here.
 */
typedef struct StandbyDataReplyMessage {
    DataQueuePtr receivePosition;

    /* Sender's system clock at the time of transmission */
    TimestampTz sendTime;

    /*
     * If replyRequested is set, the server should reply immediately to this
     * message, to avoid a timeout disconnect.
     */
    bool replyRequested;
} StandbyDataReplyMessage;

/*
 * Maximum data payload in data message.	Must be >= XLOG_BLCKSZ.
 *
 * We don't have a good idea of what a good value would be; there's some
 * overhead per message in both walsender and walreceiver, but on the other
 * hand sending large batches makes walsender less responsive to signals
 * because signals are checked only between messages.  128kB (with
 * default 8k blocks) seems like a reasonable guess for now.
 */
#define MAX_SEND_SIZE (XLOG_BLCKSZ * 16)

#endif /* _DATAPROTOCOL_H */
