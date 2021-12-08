/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
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
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/smgr/mmap_shared.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#include "catalog/pg_type.h"
#include "utils/datum.h"
#include "utils/relcache.h"

#include "utils/memutils.h"
#include "utils/memprot.h"

#include "storage/page_compression.h"
#include "executor/executor.h"
#include "storage/vfd.h"

struct MmapEntry {
    RelFileNodeForkNum relFileNodeForkNum;
    /*
     * the following are setting sin runtime
     */
    size_t reference = 0;
    PageCompressHeader *pcmap = NULL;
};

constexpr size_t LOCK_ARRAY_SIZE = 1024;
static pthread_mutex_t mmapLockArray[LOCK_ARRAY_SIZE];

static inline uint32 MmapTableHashCode(const RelFileNodeForkNum &relFileNodeForkNum)
{
    return tag_hash((void *)&relFileNodeForkNum, sizeof(RelFileNodeForkNum));
}

static inline pthread_mutex_t *MmapPartitionLock(size_t hashCode)
{
    return &mmapLockArray[hashCode % LOCK_ARRAY_SIZE];
}

static inline PageCompressHeader *MmapSharedMapFile(Vfd *vfdP, int chunkSize, bool readonly)
{
    size_t pcMapSize = SIZE_OF_PAGE_COMPRESS_ADDR_FILE(chunkSize);
    PageCompressHeader *map = pc_mmap_real_size(vfdP->fd, pcMapSize, false);
    if (map == MAP_FAILED) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                        errmsg("Failed to mmap page compression address file %s: %m", vfdP->fileName)));
    }
    return map;
}

void RealInitialMMapLockArray()
{
    for (size_t i = 0; i < LOCK_ARRAY_SIZE; ++i) {
        pthread_mutex_init(&mmapLockArray[i], NULL);
    }

    HASHCTL ctl;
    /* hash accessed by database file id */
    errno_t rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "", "");

    ctl.keysize = sizeof(RelFileNodeForkNum);
    ctl.entrysize = sizeof(MmapEntry);
    ctl.hash = tag_hash;
    ctl.num_partitions = LOCK_ARRAY_SIZE;
    const size_t initLen = 256;
    g_instance.mmapCache = HeapMemInitHash(
        "mmap hash", initLen,
        (Max(g_instance.attr.attr_common.max_files_per_process, t_thrd.storage_cxt.max_userdatafiles)) / 2, &ctl,
        HASH_ELEM | HASH_FUNCTION | HASH_PARTITION);
}

PageCompressHeader *GetPageCompressHeader(void *vfd, int chunkSize, const RelFileNodeForkNum &relFileNodeForkNum)
{
    if (IsInitdb && g_instance.mmapCache == NULL) {
        RealInitialMMapLockArray();
    }
    Vfd *currentVfd = (Vfd *)vfd;
    uint32 hashCode = MmapTableHashCode(relFileNodeForkNum);
    AutoMutexLock mmapLock(MmapPartitionLock(hashCode));

    mmapLock.lock();
    bool find = false;
    MmapEntry *mmapEntry = (MmapEntry *)hash_search_with_hash_value(g_instance.mmapCache, (void *)&relFileNodeForkNum,
                                                                    hashCode, HASH_ENTER, &find);
    if (!find) {
        mmapEntry->pcmap = NULL;
        mmapEntry->reference = 0;
    }
    if (mmapEntry->pcmap == NULL) {
        mmapEntry->pcmap = MmapSharedMapFile(currentVfd, chunkSize, false);
    }
    ++mmapEntry->reference;
    mmapLock.unLock();
    return mmapEntry->pcmap;
}

void UnReferenceAddrFile(void *vfd)
{
    Vfd *currentVfd = (Vfd *)vfd;
    RelFileNodeForkNum relFileNodeForkNum = currentVfd->fileNode;
    uint32 hashCode = MmapTableHashCode(relFileNodeForkNum);
    AutoMutexLock mmapLock(MmapPartitionLock(hashCode));
    mmapLock.lock();

    MmapEntry *mmapEntry = (MmapEntry *)hash_search_with_hash_value(g_instance.mmapCache, (void *)&relFileNodeForkNum,
                                                                    hashCode, HASH_FIND, NULL);
    if (mmapEntry == NULL) {
        ereport(ERROR, (errcode_for_file_access(),
                        errmsg("UnReferenceAddrFile failed! mmap not found, filePath: %s", currentVfd->fileName)));
    }
    --mmapEntry->reference;
    if (mmapEntry->reference == 0) {
        if (pc_munmap(mmapEntry->pcmap) != 0) {
            ereport(ERROR,
                    (errcode_for_file_access(), errmsg("could not munmap file \"%s\": %m", currentVfd->fileName)));
        }
        if (hash_search_with_hash_value(g_instance.mmapCache, (void *)&relFileNodeForkNum, hashCode, HASH_REMOVE,
                                        NULL) == NULL) {
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("UnReferenceAddrFile failed! remove hash key failed, filePath: %s", currentVfd->fileName)));
        }
    } else if (mmapEntry->reference < 0) {
        ereport(FATAL, (errcode_for_file_access(), errmsg("could not munmap file \"%s\": %m", currentVfd->fileName)));
    }
    mmapLock.unLock();
}