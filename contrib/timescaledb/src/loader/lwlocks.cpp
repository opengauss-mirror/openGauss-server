/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-server is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * Copyright (c) 2017-2022 Timescale, Inc. All Rights Reserved.
 * Copyright (c) 2016-2017 440 Labs Inc. dba Timescale. All Rights Reserved
 */

/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include <fmgr.h>
#include <miscadmin.h>
#include <storage/lock/lwlock.h>
#include <storage/shmem.h>

#include "loader/lwlocks.h"
#include "compat.h"
#define TS_LWLOCKS_SHMEM_NAME "ts_lwlocks_shmem"
#define CHUNK_APPEND_LWLOCK_TRANCHE_NAME "ts_chunk_append_lwlock_tranche"

/*
 * since shared memory can only be setup in a library loaded as
 * shared_preload_libraries we have to setup this struct here
 */
typedef struct TSLWLocks
{
	LWLock *chunk_append;
} TSLWLocks;

static TSLWLocks *ts_lwlocks = NULL;

void
ts_lwlocks_shmem_startup()
{
	bool found;
	LWLock **lock_pointer;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	ts_lwlocks =(TSLWLocks *) ShmemInitStruct(TS_LWLOCKS_SHMEM_NAME, sizeof(TSLWLocks), &found);
	if (!found)
	{
		memset(ts_lwlocks, 0, sizeof(TSLWLocks));
	}
	LWLockRelease(AddinShmemInitLock);

	/*
	 * We use a lock specific rendezvous variable to decouple the struct
	 * from the individual lock users to have no constraints on the struct
	 * across timescaledb versions.
	 */
	lock_pointer = (LWLock **) find_rendezvous_variable(RENDEZVOUS_CHUNK_APPEND_LWLOCK);
	*lock_pointer = ts_lwlocks->chunk_append;
}

void
ts_lwlocks_shmem_alloc()
{
	RequestAddinShmemSpace(sizeof(TSLWLocks));
}
