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
 * gs_warn_common.h
 *        Warning module common header file.
 * 
 * 
 * IDENTIFICATION
 *        src/include/alarm/gs_warn_common.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef __GT_WARN_COMMON_H__
#define __GT_WARN_COMMON_H__

#include "c.h"
#include "alarm/GaussAlarm_client.h"

#define GT_FALSE 0
#define GT_TRUE 1
#define GT_NULL_POINTER NULL
#define GT_SOCKET int32
#define MAX_PORT_CHAR_SIZE 6
#define LOCALHOST "127.0.0.1"
#define SOCKET_OPT_IS_REUSE_ADDR 1
#define SOCKET_OPT_IS_KEEP_ALIVE 1
#define GT_MAGIC_NUMBER 0xabcdef87
#define GT_PROTOCOL_VERSION 0x0001
#define MAX_ALARM_PARAM_LEN 1024  // Alarm params length
#define MAX_SEND_RETRY_COUNT 3
#define MAX_RECV_RETRY_COUNT 3

#define GT_NETWORK_BYTE_ORDER 0
#define GT_NON_NETWORK_BYTE_ORDER 1
#define SYS_BYTE_ORDER \
    ((0x01020304 == *(int32*)(void*)"\x1\x2\x3\x4") ? GT_NETWORK_BYTE_ORDER : GT_NON_NETWORK_BYTE_ORDER)

/* Packet header size */
#define GT_PACKET_HEADER_SIZE sizeof(GT_PACKET_HEAD)
#define GT_PACKET_MAX_PAYLOAD_SIZE (GT_SEND_BUFFER_SIZE - GT_PACKET_HEADER_SIZE)

#define GT_SEND_BUFFER_SIZE 4096 /* 4K */
#define GT_RECV_BUFFER_SIZE 4096 /* 4K */
/* The max length of address consisting of IP */
#define MAX_LEN_SERVADDR 32

/* Mapping system function to own functions, to enable future changes */
#define GT_MALLOC malloc
#define GT_FREE(mem)              \
    if (GT_NULL_POINTER != mem) { \
        free(mem);                \
    }
#define GTLOG(...) printf(__VA_ARGS__)
#define GTCONSOLELOG(...) printf(__VA_ARGS__)

/* Error codes - to be integrated with Gauss */
typedef enum tagWarnErrorCodes {
    GT_SUCCESS = 0,
    GTERR_END_OF_WARNINGS,

    GTERR_MEMORY_ALLOC_FAILED = 0x00001100,
    GTERR_THREAD_INIT_FAILED,
    GTERR_THREAD_CREATE_FAILED,
    GTERR_THREAD_DESTROY_FAILED,
    GTERR_THREAD_ACQUIRE_LOCK_FAILED,
    GTERR_THREAD_RELEASE_LOCK_FAILED,

    GTERR_SOCKET_CONNECT_FAILED,
    GTERR_SOCKET_WRITE_FAILED,
    GTERR_SOCKET_READ_FAILED,
    GTERR_SOCKET_CLOSED,

    GTERR_INTEGER_OVERFLOW,
    GTERR_INVALID_VALUE,

    GTERR_LISTENER_START_FAILED,
    GTERR_INSUFFICIENT_CONFIGS,
    GTERR_INVALID_CONFIG_VALUE,

    GTERR_SHUTDOWN_INPROGRESS,
    GTERR_CONNECT_TO_WMP_FAILED,
    GTERR_INVALID_PACKET,
    GTERR_COMMAND_SEND_TIMEDOUT,
    GTERR_RESEND_WARNING,

    GTERR_GAUSS_CONN_FAILED,
    GTERR_GAUSS_CONNECTION_LOST,
    GTERR_GAUSS_DATA_FETCH_FAILED,

    GTERR_BUTT = 0xffffffff
} WARNERRCODE;

/***
 * List of commands understood by Client - Server network layer.
 ***/
typedef enum tagGTPacketHeadCmds {
    GT_PKT_WARNING = 0x00022001,
    GT_PKT_END_OF_WARNINGS,
    GT_PKT_SERVER_ERROR,
    GT_PKT_GET_ALL_WARNINGS,
    GT_PKT_ACK,

    GT_PKT_BUTT = 0xffffffff
} GT_PACKET_HEAD_CMDS;

/***
 * Packet header for Client-Server communication.
 *
 ***/
typedef struct tagGtPacketHead {
    uint32 ulMagicNum;        /* Magic number used for packet validation */
    int32 packetCmd;          /* Type of command being executed */
    uint16 usProtocolVersion; /* Protocol version */
    int8 rsvd1[2];            /*Alignment */
    uint32 uiSize;            /* Size of the packet including header */
    int32 uiErrorCode;        /* Error code. 0 for success */
    uint32 uiTotDataSize;     /* Size of the complete Data, valid for 1st packet */
    int8 cByteOrder;          /* Byte order */
    bool bIsPendingData;      /* Is the data partial */
    int8 rsvd2[2];
    int32 cOptions; /* Placeholder holder for future use */
} GT_PACKET_HEAD, *LPGT_PACKET_HEAD;

/***
 * Client Server Packet overall structure
 ***/
typedef struct tagGtPacket {
    GT_PACKET_HEAD packetHeader;
    unsigned char* packetPayload;
} GT_PACKET, *LPGT_PACKET;

#endif /* __GT_WARN_COMMON_H__ */
