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
 * mc_tcp.h
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/communication/sctp_core/mc_tcp.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef _CORE_MC_TCP_H_
#define _CORE_MC_TCP_H_

#include <sys/stat.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <netinet/in.h>
#include <unistd.h>
#include <netinet/tcp.h>

#define TCP_LISTENQ 1024

void mc_tcp_set_keepalive_param(int idle, int intvl, int count);

void mc_tcp_set_timeout_param(int conn_timeout, int send_timeout);

int mc_tcp_get_connect_timeout();

int mc_tcp_listen(const char* host, int port, socklen_t* addrlenp);

int mc_tcp_accept(int fd, struct sockaddr* sa, socklen_t* salenptr);

int mc_tcp_connect(const char* host, int port);

int mc_tcp_get_peer_name(int fd, char* host, int* port);

int mc_tcp_write_block(int fd, const void* data, int size);

int mc_tcp_write_noblock(int fd, const void* data, int size);

int mc_tcp_read_block(int fd, void* data, int size, int flags);

int mc_tcp_read_nonblock(int fd, void* data, int size, int flags);

int mc_tcp_check_socket(int sock);

void mc_tcp_close(int fd);

#endif  //_CORE_MC_TCP_H_

