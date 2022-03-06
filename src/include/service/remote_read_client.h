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
 * remote_read_client.h
 *     Don't include any of RPC or PG header file
 *     Just using simple C API interface
 * 
 * IDENTIFICATION
 *        src/include/service/remote_read_client.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef REMOTE_READ_CLIENT_H
#define REMOTE_READ_CLIENT_H

#include "c.h"

#include "storage/remote_adapter.h"
#include "storage/smgr/relfilenode.h"
#include "postmaster/pagerepair.h"
#include "access/xlog_basic.h"

typedef uint64 XLogRecPtr;
extern int RemoteGetCU(char* remote_address, uint32 spcnode, uint32 dbnode, uint32 relnode, int32 colid,
    uint64 offset, int32 size, uint64 lsn, char* cu_data);

extern int RemoteGetPage(char* remote_address, RepairBlockKey *key, uint32 blocksize, uint64 lsn,
    char* page_data, const XLogPhyBlock *pblk, int timeout);

extern int RemoteGetFile(char* remoteAddress, RemoteReadFileKey *key, uint64 lsn, uint32 size, char* pageData,
    XLogRecPtr *remote_lsn, uint32 *remote_size, int timeout);
extern int RemoteGetFileSize(char* remoteAddress, RemoteReadFileKey *key, uint64 lsn, int64 *size, int timeout);


#endif /* REMOTE_READ_CLIENT_H */
