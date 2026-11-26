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
 *
 * -------------------------------------------------------------------------
 *
 * common_parallel.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/common_parallel.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "access/datavec/common_parallel.h"
#include "access/datavec/ivfflat.h"

CommonShared *CommonInitShared(const std::function<void(int, int)> &func, int parallelWorkers, int dataSize)
{
    CommonShared *shared = (CommonShared *)MemoryContextAllocZero(
        INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), sizeof(CommonShared));
    pg_atomic_init_u32(&shared->cuThreadId, 0);

    shared->func = func;
    shared->parallelWorkers = parallelWorkers;
    shared->dataSize = dataSize;

    return shared;
}

/*
 * Perform work within a launched parallel process
 */
void CommonParallelMain(const BgWorkerContext *bwc)
{
    CommonShared *shared = (CommonShared *)bwc->bgshared;
    uint32 cuThreadId = pg_atomic_add_fetch_u32(&shared->cuThreadId, 1);
    Assert(cuThreadId > 0);

    int size = shared->dataSize;
    int batch = std::max((int)(size / shared->parallelWorkers), 1);
    int begin = batch * (cuThreadId - 1);
    int end = begin + batch;
    if (cuThreadId == shared->parallelWorkers) {
        end = size;
    }

    shared->func(begin, end);
}

/*
 * End parallel build
 */
void EndCommonParallel()
{
    BgworkerListSyncQuit();
}

/*
 * Begin parallel build
 */
void BeginCommonParallel(const std::function<void(int, int)> &func, int parallelWorkers, int dataSize)
{
    CommonShared* shared = CommonInitShared(func, parallelWorkers, dataSize);
    int nworkers = shared->parallelWorkers;

    int successWorkers = LaunchBackgroundWorkers(nworkers, shared, CommonParallelMain, NULL);
    if (successWorkers == 0) {
        pfree_ext(shared);
        ereport(DEBUG1, (errmsg("Parallel populate error, all workers failed.")));
        return;
    }

    /* Log participants */
    ereport(DEBUG1, (errmsg("using parallel workers")));

    PG_TRY();
    {
        BgworkerListWaitFinish(&successWorkers);
    }
    PG_CATCH();
    {
        EndCommonParallel();
        PG_RE_THROW();
    }
    PG_END_TRY();

    EndCommonParallel();
}