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
 * mc_sctp.h
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/communication/sctp_core/mc_sctp.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef _CORE_MC_SCTP_H_
#define _CORE_MC_SCTP_H_

#include <ctype.h>
#include <sys/types.h>
#include <stdint.h>
#include <arpa/inet.h>
#include <sys/resource.h>

#include "sctp_common.h"
#include "sctp_utils/sctp_util.h"

#define SCTP_LISTENQ 2048

// init a sctp sockaddr_storage with host and port
//
int mc_sctp_addr_init(const char* host, int port, struct sockaddr_storage* ss, int* in_len);

// init a sctp server with socket address storage
//
int mc_sctp_server_init(struct sockaddr_storage* s_loc, int sin_len);

// init a sctp server with socket address storage
//
int mc_sctp_client_init(struct sockaddr_storage* s_loc, int sin_len);

// connect to server
//
int mc_sctp_connect(int sock, struct sockaddr_storage* s_loc, int sin_len);

// accetp connect request
//
int mc_sctp_accept(int sock, struct sockaddr* sa, socklen_t* salenptr);
// send message to sctp socket with given stream
//
int mc_sctp_send(int sock, int stream, int order, int timetolive, int flags, struct sockaddr_storage* s_rem,
    char* message, unsigned long ppid, int s_len);

// send ack message
//
int mc_sctp_send_ack(int sock, char* msg, int msg_len);

// recv ack message
//
int mc_sctp_recv_ack(int sock, char* msg, int msg_len);

// receive message from SCTP socket
//
int mc_sctp_recv(int sock, struct msghdr* inmessage, int flags);

// close SCTP socket
//
int mc_sctp_sock_close(int sock);

// set socket to NON-BLOCKING to epoll on it
//
int mc_sctp_set_nonblock(int sock);

// set sctp socket option
//
int mc_sctp_setsockopt(int sock, int level, int optname, const void* optval, socklen_t optlen);

// get sctp socket option
//
int mc_sctp_getsockopt(int sock, int optname, void* optval, socklen_t* optlen);

// check if sctp socket is connected
//
int mc_sctp_check_socket(int sock);

// receive message on block mode
int mc_sctp_recv_block_mode(int fd, char* ack_msg, int msg_len);

// send message on block mode
int mc_sctp_send_block_mode(int fd, const void* data, int size);

unsigned int mc_sctp_get_ppid(struct msghdr* msg);

unsigned int mc_sctp_get_sid(struct msghdr* msg);

void mc_sctp_print_message(struct msghdr* msg, size_t msg_len);

#endif  //_CORE_MC_SCTP_H_

