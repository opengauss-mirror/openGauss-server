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
 * knl_usync.h
 *        File synchronization management code.
 *
 * IDENTIFICATION
 *        opengauss_server/src/include/storage/knl_usync.h
 * -------------------------------------------------------------------------
 */

#ifndef SYNC_H
#define SYNC_H

#include "storage/smgr/relfilenode.h"

/*
 * Type of sync request.  These are used to manage the set of pending
 * requests to call a sync handler's sync or unlink functions at the next
 * checkpoint.
 */
typedef enum SyncRequestType {
    SYNC_REQUEST,        /* schedule a call of sync function */
    SYNC_UNLINK_REQUEST, /* schedule a call of unlink function */
    SYNC_FORGET_REQUEST, /* forget all calls for a tag */
    SYNC_FILTER_REQUEST  /* forget all calls satisfying match fn */
} SyncRequestType;

/*
 * Which set of functions to use to handle a given request.  The values of
 * the enumerators must match the indexes of the function table in sync.c.
 */
typedef enum SyncRequestHandler {
    SYNC_HANDLER_MD = 0,  /* md smgr */
    SYNC_HANDLER_UNDO = 1, /* undo smgr */
    SYNC_HANDLER_SEGMENT = 2, /* segment-page smgr */
} SyncRequestHandler;

/*
 * A tag identifying a file.  Currently it has the members required for md.c's
 * usage, but sync.c has no knowledge of the internal structure, and it is
 * liable to change as required by future handlers.
 */
typedef struct FileTag {
    int16 handler;      /* SyncRequstHandler value, saving space */
    ForkNumber forknum; /* ForkNumber, saving space */
    RelFileNode rnode;
    uint32 segno;
} FileTag;

extern void InitSync(void);
extern void SyncPreCheckpoint(void);
extern void SyncPostCheckpoint(void);
extern void ProcessSyncRequests(void);
extern void RememberSyncRequest(const FileTag *ftag, SyncRequestType type);
extern void EnableSyncRequestForwarding(void);
extern bool RegisterSyncRequest(const FileTag *ftag, SyncRequestType type, bool retryOnError);

#endif              /* SYNC_H */

