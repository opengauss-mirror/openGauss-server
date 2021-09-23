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
 * dataqueue.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/replication/dataqueue.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef _DATAQUEUE_H
#define _DATAQUEUE_H

#include "c.h"
#include "gs_threadlocal.h"
#include "replication/dataqueuedefs.h"
#include "replication/bcm.h"
#include "storage/lock/s_lock.h"
#include "utils/hsearch.h"
#include "utils/pg_crc.h"

#define WS_MAX_DATA_QUEUE_SIZE (uint32)(g_instance.attr.attr_storage.DataQueueBufSize * 1024)

#define GETATTID(attid) ((attid) & 0xffff)
#define GETBUCKETID(attid) (((uint)(attid) >> 16) - 1)

/*
 *------------------------------------------------------
 * |****************|          |**************|			|
 * -----------------------------------------------------
 * 0  			use_tail1       use_head2    use_tail2   tail
 *
 * From use_tail1 to use_head2 and from use_tail2 to tail are freespace
 */
typedef struct DataQueueData {
    uint32 size; /* memory size */

    DataQueuePtr use_tail1;
    DataQueuePtr use_head2; /* from use_head2 to use_tail2 are elements */
    DataQueuePtr use_tail2;
    slock_t use_mutex; /* protect above variables */

    char* pages;
} DataQueueData, *DataQueue;

typedef struct DataElementHeaderData {
    RelFileNodeOld rnode;
    BlockNumber blocknum; /* Upper 2 bit store StorageType */
    int attid; /* column storage id */
    StorageEngine type;

    uint64 offset;             /* offset the element to be written */
    uint32 data_size;          /* element size */
    DataQueuePtr queue_offset; /* current page offset in queue */

    XLogRecPtr ref_rec_ptr; /* the associated ref xlog ptr */

#ifdef DATA_DEBUG
    pg_crc32 data_crc; /* CRC for current page */
#endif
} DataElementHeaderData, *DataElementHeader;

typedef struct BCMElementData {
    RelFileNodeOld rnode;
    BlockNumber blocknum; /* Upper 2 bit store StorageType */
    int attid; /* column storage id */
    StorageEngine type;
    uint64 offset;    /* offset the element to be written */
    uint32 data_size; /* element size */
    bool is_vaild;    /* is this block vaild ? */
} BCMElementData, *BCMElement;

typedef struct heap_sync_rel_key {
    Oid rd_id; /* the relation */
} heap_sync_rel_key;

typedef struct heap_sync_rel {
    heap_sync_rel_key key; /* hash key ... must be first */
} heap_sync_rel;

extern Size DataQueueShmemSize(void);
extern void DataSenderQueueShmemInit(void);
extern void DataWriterQueueShmemInit(void);

extern DataQueuePtr PushToSenderQueue(const RelFileNode& rnode, BlockNumber blockNum, StorageEngine type,
    const char* mem, uint32 data_len, int attid, uint64 offset);
extern DataQueuePtr PushToWriterQueue(const char* mem, uint32 mem_len);

extern uint32 GetFromDataQueue(
    char*& buf, int bufsize, DataQueuePtr& startptr, DataQueuePtr& endptr, bool amIWriter, DataQueueData* data_queue);
extern void PopFromDataQueue(const DataQueuePtr& position, DataQueueData* data_queue);
extern void PushCUToDataQueue(Relation rel, int col, const char* mem, _in_ uint64 offset, _in_ int size, bool setbcm);
extern void ResetDataQueue(DataQueueData* data_queue);
extern bool DataQueueIsEmpty(DataQueueData* data_queue);
extern void CStoreCUReplication(
    _in_ Relation rel, _in_ int attrId, _in_ char* cuData, _in_ int cuSize, _in_ uint64 cuFileOffset);
extern bool BCMArrayIsEmpty(void);
extern void ResetBCMArray(void);
extern void ClearBCMArray(void);
extern void BCMArrayDropAllBlocks(const RelFileNode& dropnode);
extern void HeapSyncHashSearch(Oid rd_id, HASHACTION action);
extern void AtAbort_RelationSync(void);
extern void AtCommit_RelationSync(void);
extern void PallocBCMBCMElementArray(void);
#endif
