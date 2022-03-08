/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
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
 * knl_partcache.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/utils/knl_partcache.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef KNL_PARTCACHE_H
#define KNL_PARTCACHE_H
#include "catalog/pg_partition.h"
#include "utils/knl_localpartdefcache.h"
#include "utils/knl_localsysdbcache.h"
#include "knl/knl_thread.h"
#include "utils/relcache.h"
#include "utils/snapshot.h"

/* part 1:macro definitions, global virables, and typedefs */
typedef struct partidcacheent {
    Oid partoid;
    Partition partdesc;
} PartIdCacheEnt;

/* part 2: functions used only in this gsc source file */
extern void PartitionDestroyPartition(Partition partition);
extern void PartitionClearPartition(Partition partition, bool rebuild);
extern void PartitionInitPhysicalAddr(Partition partition);
extern void PartitionReloadIndexInfo(Partition part);
extern Partition PartitionBuildDesc(Oid targetPartId, StorageType storage_type, bool insertIt);


#define PartitionIdCacheInsertIntoLocal(PARTITION)                                                           \
    do {                                                                                                     \
        if (EnableLocalSysCache()) {                                                                         \
            t_thrd.lsc_cxt.lsc->partdefcache.InsertPartitionIntoLocal((PARTITION));                     \
        } else {                                                                                             \
            PartIdCacheEnt *idhentry;                                                                        \
            bool found = true;                                                                               \
            idhentry = (PartIdCacheEnt *)hash_search(                                                        \
                u_sess->cache_cxt.PartitionIdCache, (void *)&((PARTITION)->pd_id), HASH_ENTER, &found);      \
            /* used to give notice if found -- now just keep quiet */                                        \
            idhentry->partdesc = PARTITION;                                                                  \
        }                                                                                                    \
    } while (0)

#define PartitionIdCacheLookup(ID, PARTITION)                                                                      \
    do {                                                                                                           \
        if (EnableLocalSysCache()) {                                                                               \
            (PARTITION) = t_thrd.lsc_cxt.lsc->partdefcache.SearchPartition((ID));                             \
        } else {                                                                                                   \
            PartIdCacheEnt *hentry;                                                                                \
            hentry =                                                                                               \
                (PartIdCacheEnt *)hash_search(u_sess->cache_cxt.PartitionIdCache, (void *)&(ID), HASH_FIND, NULL); \
            if (hentry != NULL)                                                                                    \
                (PARTITION) = hentry->partdesc;                                                                    \
            else                                                                                                   \
                (PARTITION) = NULL;                                                                                \
        }                                                                                                          \
    } while (0)

#define PartitionIdCacheDeleteLocal(PARTITION)                                                              \
    do {                                                                                               \
        if (EnableLocalSysCache()) {                                                                   \
            t_thrd.lsc_cxt.lsc->partdefcache.RemovePartition((PARTITION));                        \
        } else {                                                                                       \
            PartIdCacheEnt *idhentry;                                                                  \
            idhentry = (PartIdCacheEnt *)hash_search(                                                  \
                u_sess->cache_cxt.PartitionIdCache, (void *)&((PARTITION)->pd_id), HASH_REMOVE, NULL); \
            if (idhentry == NULL)                                                                      \
                ereport(WARNING,                                                                       \
                    (errcode(ERRCODE_UNDEFINED_TABLE),                                                 \
                        errmsg("trying to delete a rd_id partdesc that does not exist")));             \
        }                                                                                              \
    } while (0)

#define PartitionIdCacheLookupOnlyLocal(ID, PARTITION)                                                             \
    do {                                                                                                           \
        if (EnableLocalSysCache()) {                                                                               \
            (PARTITION) = t_thrd.lsc_cxt.lsc->partdefcache.SearchPartitionFromLocal((ID));                    \
        } else {                                                                                                   \
            PartIdCacheEnt *hentry;                                                                                \
            hentry =                                                                                               \
                (PartIdCacheEnt *)hash_search(u_sess->cache_cxt.PartitionIdCache, (void *)&(ID), HASH_FIND, NULL); \
            if (hentry != NULL)                                                                                    \
                (PARTITION) = hentry->partdesc;                                                                    \
            else                                                                                                   \
                (PARTITION) = NULL;                                                                                \
        }                                                                                                          \
    } while (0)

inline bool GetPartCacheNeedEOXActWork()
{
    if (EnableLocalSysCache()) {
        return t_thrd.lsc_cxt.lsc->partdefcache.PartCacheNeedEOXActWork;
    } else {
        return u_sess->cache_cxt.PartCacheNeedEOXActWork;
    }
}

inline void SetPartCacheNeedEOXActWork(bool value)
{
    if (EnableLocalSysCache()) {
        t_thrd.lsc_cxt.lsc->partdefcache.PartCacheNeedEOXActWork = value;
    } else {
        u_sess->cache_cxt.PartCacheNeedEOXActWork = value;
    }
}
#endif