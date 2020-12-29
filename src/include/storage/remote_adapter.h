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
 * remote_adapter.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/storage/remote_adapter.h
 *
 * NOTE
 *   Don't include any of RPC or PG header file
 *   Just using simple C API interface
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef REMOTE_ADAPTER_H
#define REMOTE_ADAPTER_H

#include "c.h"

#include "storage/remote_read.h"

#define Free(x)        \
    do {               \
        if ((x))       \
            free((x)); \
        (x) = NULL;    \
    } while (0)

typedef struct TlsCertPath {
    char caPath[MAX_PATH_LEN];
    char keyPath[MAX_PATH_LEN];
    char certPath[MAX_PATH_LEN];
} TlsCertPath;

typedef struct RemoteReadContext RemoteReadContext;

extern RemoteReadContext* InitRemoteReadContext();

extern void ReleaseRemoteReadContext(RemoteReadContext* context);

extern int StandbyReadCUforPrimary(uint32 spcnode, uint32 dbnode, uint32 relnode, int32 colid, uint64 offset,
    int32 size, uint64 lsn, RemoteReadContext* context, char** cudata);

extern int StandbyReadPageforPrimary(uint32 spcnode, uint32 dbnode, uint32 relnode, int16 bucketnode, int32 forknum, uint32 blocknum,
    uint32 blocksize, uint64 lsn, RemoteReadContext* context, char** pagedata);

extern void CleanWorkEnv();

extern bool GetCertEnv(const char* envName, char* outputEnvStr, size_t envValueLen);

extern char* GetCertStr(char* path, int* len);

extern int GetRemoteReadMode();

extern void OutputMsgforRPC(int elevel, const char* fmt, ...) __attribute__((format(PG_PRINTF_ATTRIBUTE, 2, 3)));

#endif /* REMOTE_ADAPTER_H */
