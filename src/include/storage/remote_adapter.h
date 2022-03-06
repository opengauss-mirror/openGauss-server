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
#include "storage/smgr/relfilenode.h"
#include "access/xlogdefs.h"
#include "access/xlog_basic.h"
#include "storage/smgr/segment_internal.h"
#include "funcapi.h"

extern int StandbyReadCUforPrimary(RepairBlockKey key, uint64 offset, int32 size, uint64 lsn, int timeout,
    bytea** cudata);
extern int StandbyReadPageforPrimary(RepairBlockKey key, uint32 blocksize, uint64 lsn, bytea** pagedata,
    int timeout, const XLogPhyBlock *pblk);

extern int ReadFileSizeForRemote(RelFileNode rnode, int32 forknum, XLogRecPtr lsn, int64* res, int timeout);

Datum gs_read_file_from_remote(PG_FUNCTION_ARGS);
Datum gs_read_file_size_from_remote(PG_FUNCTION_ARGS);

#endif /* REMOTE_ADAPTER_H */
