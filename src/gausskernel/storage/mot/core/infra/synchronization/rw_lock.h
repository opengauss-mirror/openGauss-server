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
 * rw_lock.h
 *    Implements a reader/writer lock using spinlock.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/synchronization/rw_lock.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef RW_LOCK_H
#define RW_LOCK_H

namespace MOT {
typedef unsigned Spinlock;

/**
 * @class RwLock
 * @brief this class implements a reader/writer lock using spinlock
 */
class RwLock {
public:
    RwLock();

    RwLock(const RwLock& orig) = delete;

    virtual ~RwLock()
    {}

    void WrLock();

    void WrUnlock();

    int WrTryLock();

    void RdLock();

    void RdUnlock();

    int RdTryLock();

    int RdUpgradeLock();

private:
    Spinlock m_lock;

    unsigned m_readers;

    unsigned LockXchg32(void* ptr, unsigned x);

    void SpinLock();

    void SpinUnlock();

    int SpinTryLock();
};
}  // namespace MOT

#endif /* RW_LOCK_H */