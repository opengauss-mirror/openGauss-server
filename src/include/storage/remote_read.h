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
 * remote_read.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/storage/remote_read.h
 *
 * NOTE
 *     Don't include any of RPC or PG header file
 *     Just using simple C API interface
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef REMOTE_READ_H
#define REMOTE_READ_H

#include "c.h"

typedef enum {
    REMOTE_READ_OFF = 0,  /* not allow reomte read */
    REMOTE_READ_NON_AUTH, /* remote read no ssl */
    REMOTE_READ_AUTH      /* remote read with SSL */
} remote_read_param;

#define REMOTE_READ_OK 0
#define REMOTE_READ_NEED_WAIT 1
#define REMOTE_READ_CRC_ERROR 2
#define REMOTE_READ_RPC_ERROR 3
#define REMOTE_READ_SIZE_ERROR 4
#define REMOTE_READ_IO_ERROR 5
#define REMOTE_READ_RPC_TIMEOUT 6
#define REMOTE_READ_BLCKSZ_NOT_SAME 7
#define REMOTE_READ_MEMCPY_ERROR 8
#define REMOTE_READ_IP_NOT_EXIST 9
#define REMOTE_READ_CONN_ERROR 10


#define MAX_PATH_LEN 1024
#define MAX_IPADDR_LEN 64

const int MAX_BATCH_READ_BLOCKNUM = 16 * 1024 * 1024 / BLCKSZ;  /* 16MB file */

extern const char* RemoteReadErrMsg(int error_code);

extern void GetRemoteReadAddress(char* first_address, char* second_address, size_t address_len);

extern void GetIPAndPort(char* address, char* ip, char* port, size_t len);

extern bool CanRemoteRead();

extern bool IsRemoteReadModeOn();
extern int SetRemoteReadModeOffAndGetOldMode();
extern void SetRemoteReadMode(int mode);

#endif /* REMOTE_READ_H */
