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
 * libpq-be.h
 *	  This file contains definitions for structures and externs used
 *	  by the cm server during client coonect.
 *
 *	  Note that this is backend-internal and is NOT exported to clients.
 *	  Structs that need to be client-visible are in pqcomm.h.
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * 
 * 
 * IDENTIFICATION
 *        src/include/replication/heartbeat/libpq/libpq-be.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef CM_LIBPQ_BE_H
#define CM_LIBPQ_BE_H

#include "replication/heartbeat/libpq/pqcomm.h"

#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif
#ifdef HAVE_NETINET_TCP_H
#include <netinet/tcp.h>
#endif
namespace PureLibpq {

typedef enum PortLastCall {
    LastCall_NONE = 0,
    LastCall_SEND,
    LastCall_RECV,
} PortLastCall;

/*
 * The Port structure maintains the tcp channel information,
 * and is kept in palloc'd memory.
 * TCP keepalive settings such as keepalives_idle,keepalives_interval
 * and keepalives_count art use the default values of system.
 */
typedef struct Port {
    int sock;               /* File descriptor */
    SockAddr laddr;         /* local addr */
    SockAddr raddr;         /* remote addr */
    PortLastCall last_call; /* Last syscall to this port */
    int last_errno;         /* Last errno. zero if the last call succeeds */
#define PQ_BUFFER_SIZE (16 * 1024)

    char PqSendBuffer[PQ_BUFFER_SIZE];
    int PqSendPointer; /* Next index to store a byte in PqSendBuffer */

    char PqRecvBuffer[PQ_BUFFER_SIZE];
    int PqRecvPointer; /* Next index to read a byte from PqRecvBuffer */
    int PqRecvLength;  /* End of data available in PqRecvBuffer */
} Port;

extern int StreamServerPort(int family, char* hostName, unsigned short portNumber, int ListenSocket[], int MaxListen);
extern int StreamConnection(int server_fd, Port* port);
extern void StreamClose(int sock);
extern int pq_getbyte(Port* myport);
extern int pq_putmessage(Port* myport, char msgtype, const char* s, size_t len);
extern int pq_flush(Port* myport);
extern int SetSocketNoBlock(int isocketId);
extern void CloseAndFreePort(Port* port);

}  // namespace PureLibpq

#endif /* LIBPQ_BE_H */
