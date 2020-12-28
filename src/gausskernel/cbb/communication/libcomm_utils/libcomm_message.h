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
 * sctp_message.h
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/communication/sctp_utils/sctp_message.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef _UTILS_MESSAGE_H_
#define _UTILS_MESSAGE_H_
#include "libcomm.h"

#define NAMEDATALEN 64
// SCTP message related data structures
// the state of stream, stream could send and receive only in MAIL_RUN state.
typedef enum {
    CTRL_UNKNOWN,
    CTRL_CONN_REGIST,
    CTRL_CONN_REGIST_CN,
    CTRL_CONN_REJECT,
    CTRL_CONN_DUAL,
    CTRL_CONN_CANCEL,
    CTRL_CONN_REQUEST,
    CTRL_CONN_ACCEPT,
    CTRL_ADD_QUOTA,
    CTRL_CLOSED,
    CTRL_PEER_TID,
    CTRL_ASSERT_FAIL,
    CTRL_PEER_CHANGED,
    CTRL_STOP_QUERY,
    CTRL_MAX_TYPE
} CtrlMsgType;

typedef enum {
    MAIL_UNKNOWN,
    MAIL_READY,
    MAIL_RUN,
    MAIL_HOLD,
    MAIL_CLOSED,
    MAIL_TO_CLOSE,
    MAIL_MAX_TYPE
} SctpMailboxStatus;

// structure of control message transmitted by control tcp connection
struct FCMSG_T {
    uint16 type;      // message type: INIT,READY,RESUME,CLOSED
    uint16 node_idx;  // local node id in remote
    uint16 streamid;  // stream index
    uint16 version;
    TcpStreamKey stream_key;    // stream key
    uint64 query_id;             // query index (debug_query_id)
    long streamcap;              // quota size
    unsigned long extra_info;    // more information, like thread id/remote version
    char nodename[NAMEDATALEN];  // node name
};

#endif  //_UTILS_MESSAGE_H_
