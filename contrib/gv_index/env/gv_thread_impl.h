/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
 * --------------------------------------------------------------------------------------
 *
 * gv_thread_impl.h
 *
 * Portions Copyright (c) 2026, Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2020, AWS
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *        contrib/gv_index/env/gv_thread_impl.h
 *
 * --------------------------------------------------------------------------------------
 */

#ifndef GVGRAPH_THREAD_IMPL_H
#define GVGRAPH_THREAD_IMPL_H

#include <atomic>
#include <thread>

#include "c.h"
#include "catalog/index.h"
#include "postmaster/bgworker.h"
#include "utils/elog.h"
#include "utils/rel.h"
#include "gv_compute_thread.h"
#include "gv_page_storage.h"
#include "lite/index/light_env/compute_task_runner.h"

namespace gs_vector {

struct GVThreadPoolImpl : public annlite::light_env::ComputeTaskRunner {
    using Function = GVWorkerArgs::Function;
    using Args = GVWorkerArgs::Args;
    using Task = GVWorkerArgs::Task;

    GVThreadPoolImpl()
        : m_max_threads(0), m_env(nullptr), m_init(false), m_index_oid(InvalidOid)
    {}
    GVThreadPoolImpl(size_t threads, annlite::LightEnv *_env)
        : m_max_threads(threads), m_env(_env), m_init(false), m_index_oid(InvalidOid)
    {}

    void set_index_oid(Oid oid) { m_index_oid = oid; }

    static void* worker(void *void_args)
    {
        GVWorkerArgs *args = static_cast<GVWorkerArgs*>(void_args);
        size_t id = args->id_acquire();
        args->worker_start();

        while (!args->is_terminated()) {
            Task *task = args->get_task(id);
            if (task == nullptr) {
                snap();
                continue;
            }
            (*task)();
            args->set_task(id, nullptr);
        }
        args->worker_stop();
        return nullptr;
    }

    static void bgworker_main(const BgWorkerContext *bwc)
    {
        GVWorkerArgs** args_ptr = static_cast<GVWorkerArgs**>(bwc->bgshared);
        if (args_ptr != nullptr && *args_ptr != nullptr) {
            GVWorkerArgs* args = *args_ptr;
            if (OidIsValid(args->index_oid)) {
                Relation index_rel = index_open(args->index_oid, AccessShareLock);
                GVPageStorage::set_worker_index(index_rel);
            }
            (void)worker(args);

            Relation index_rel = GVPageStorage::get_worker_index();
            if (index_rel != nullptr) {
                index_close(index_rel, AccessShareLock);
                GVPageStorage::set_worker_index(nullptr);
            }
        }
    }

    static void bgworker_cleanup(const BgWorkerContext *bwc)
    {
        (void)bwc;
    }

    virtual ~GVThreadPoolImpl()
    {
        destroy();
    }

    virtual size_t max_threads() const override
    {
        return m_max_threads;
    }

    virtual void init(size_t threads = 0) override
    {
        if (m_max_threads <= 1) {
            return;
        }
        if (threads == 0) {
            threads = m_max_threads;
        }
        m_max_threads = threads;

        if (m_init) {
            return;
        }

        m_workers_arg = GVWorkerArgs(m_max_threads);
        m_workers_arg.set_terminated(false);
        m_workers_arg.index_oid = m_index_oid;

        GVWorkerArgs **pointer = (GVWorkerArgs **)palloc(sizeof(GVWorkerArgs *));
        *pointer = &m_workers_arg;
        int num_workers = LaunchBackgroundWorkers(m_max_threads, pointer, bgworker_main, bgworker_cleanup);

        if (num_workers > 0) {
            m_max_threads = (size_t)num_workers;
            m_init = true;
            ereport(LOG, (errmsg("GVThreadPoolImpl: started %d bgworkers", num_workers)));
        } else {
            // bgworker 启动失败，回退到单线程
            ereport(WARNING, (errmsg("GVThreadPoolImpl: failed to start bgworkers, falling back to single thread")));
            m_max_threads = 1;
        }
    }

    virtual void destroy() override
    {
        if (!m_init) {
            return;
        }
        if (!m_workers_arg.is_terminated()) {
            m_workers_arg.shutdown();

            // 等待 worker 结束
            int workers = m_max_threads;
            BgworkerListWaitFinish(&workers);
            BgworkerListSyncQuit();
        }

        m_init = false;
    }

    virtual void launch(Function func, void *args, size_t sizeof_argtype, size_t taskcount) override
    {
        if (max_threads() <= 1 || !m_init) {
            // 顺序执行
            for (size_t i = 0; i < taskcount; ++i) {
                void *targs = static_cast<void *>(static_cast<char *>(args) + sizeof_argtype * i);
                func(targs);
            }
            return;
        }

        // 使用 bgworker 线程池执行
        Task tasks[taskcount];

        for (size_t i = 0; i < taskcount; ++i) {
            void *targs = static_cast<void *>(static_cast<char *>(args) + sizeof_argtype * i);
            new (&tasks[i]) Task(func, targs);
            m_workers_arg.set_task(i, &tasks[i]);
        }

        // 等待所有任务完成
        for (size_t i = 0; i < taskcount; ++i) {
            while (m_workers_arg.get_task(i) != nullptr) {
                snap();
            }
        }
    }

    virtual void set_max_threads(size_t threads) override
    {
        m_max_threads = threads;
    }

private:
    static void snap()
    {
        GVWorkerArgs::snap();
    }
    size_t m_max_threads;
    annlite::LightEnv *m_env;
    GVWorkerArgs m_workers_arg;
    bool m_init;
    Oid m_index_oid;
};
} /* namespace gs_vector */

#endif /* GV_THREAD_IMPL_H */
