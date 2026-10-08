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
 * gs_compute_thread.h
 *
 * Portions Copyright (c) 2026, Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2020, AWS
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *        contrib/gv_index/env/gs_compute_thread.h
 *
 * --------------------------------------------------------------------------------------
 */

#ifndef GVGRAPH_COMPUTE_THREAD_H
#define GVGRAPH_COMPUTE_THREAD_H

#include <atomic>
#include <thread>
#include "c.h"
#include "port.h"

namespace gs_vector {

struct GVWorkerArgs {
    using Function = void *(*)(void *);
    using Args = void*;
    using Result = void*;

    struct Task {
        Task() : func(nullptr), args(nullptr) {}
        Task(Function _func, Args _args) : func(_func), args(_args) {}
        void* operator()() { return func(args); }
        Function func;
        Args args;
    };

    static constexpr size_t supported_max_threads = 128;

    GVWorkerArgs()
        : max_worker(0), next_worker_id(0), total_workers(0), terminated(true), index_oid(InvalidOid)
    {}

    GVWorkerArgs(size_t _max_worker)
        : max_worker(_max_worker), next_worker_id(0), total_workers(0), terminated(true), index_oid(InvalidOid)
    {
        for (size_t i = 0; i < supported_max_threads; ++i) {
            tasks[i] = nullptr;
        }
    }

    size_t id_acquire()
    {
        std::atomic<size_t>* id_next = reinterpret_cast<std::atomic<size_t>*>(&next_worker_id);
        size_t result = id_next->fetch_add(1, std::memory_order_seq_cst);
        return result;
    }

    void worker_start()
    {
        (reinterpret_cast<std::atomic<size_t>*>(&total_workers))->fetch_add(1, std::memory_order_seq_cst);
    }

    void worker_stop()
    {
        (reinterpret_cast<std::atomic<size_t>*>(&total_workers))->fetch_sub(1, std::memory_order_seq_cst);
    }

    size_t workers()
    {
        return (reinterpret_cast<std::atomic<size_t>*>(&total_workers))->load(std::memory_order_seq_cst);
    }

    Task* get_task(size_t worker_id)
    {
        if (worker_id >= supported_max_threads) {
            return nullptr;
        }
        std::atomic<Task*>* taskslot = reinterpret_cast<std::atomic<Task*>*>(&tasks[worker_id]);
        return taskslot->load(std::memory_order_seq_cst);
    }

    void set_task(size_t worker_id, Task* task)
    {
        if (worker_id >= supported_max_threads) {
            return;
        }
        std::atomic<Task*>* taskslot = reinterpret_cast<std::atomic<Task*>*>(&tasks[worker_id]);
        taskslot->store(task, std::memory_order_seq_cst);
    }

    void set_terminated(bool val)
    {
        reinterpret_cast<std::atomic<uint32_t>*>(&terminated)->
            store(val, std::memory_order_seq_cst);
    }

    bool is_terminated() const
    {
        return terminated;
    }

    void shutdown()
    {
        if (is_terminated()) { return; }
        set_terminated(true);
        while (workers() > 0) { snap(); }
    }

    static void snap()
    {
        pg_usleep(100);
    }
    
    Task* tasks[supported_max_threads];
    size_t max_worker;
    size_t next_worker_id;
    size_t total_workers;
    bool terminated;
    Oid index_oid;
};

}  // namespace gs_vector

#endif
