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
 *---------------------------------------------------------------------------------------
 *
 *  gds_utils.h
 *
 * IDENTIFICATION
 *        src/bin/gds/gds_utils.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef GDS_UTILS_H
#define GDS_UTILS_H

#ifndef WIN32
#include <sys/socket.h>
#else
#include <WinSock2.h>
#endif

typedef struct {
    struct sockaddr_storage mask;
    struct sockaddr_storage addr;
} listen_addr;

bool check_ip(struct sockaddr_storage* raddr, struct sockaddr* addr, struct sockaddr* mask);

int sockaddr_cidr_mask(struct sockaddr_storage* mask, const char* numbits, int family);
void TrimDirectory(char* path);
void CanonicalizePath(char* path);
int PGMkdirP(char* path, int omode);
#endif
