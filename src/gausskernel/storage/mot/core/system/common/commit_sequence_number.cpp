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
 * commit_sequence_number.cpp
 *    Encapsulates the logic of a global unique auto incremental number.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/common/commit_sequence_number.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <list>
#include "commit_sequence_number.h"
#include "recovery_manager.h"
#include "utilities.h"
#include "mot_engine.h"

namespace MOT {
DECLARE_LOGGER(CSNManager, System);

CSNManager::CSNManager() : m_csn(INITIAL_CSN)
{}

CSNManager::~CSNManager()
{}

RC CSNManager::SetCSN(uint64_t value)
{
    if (!MOTEngine::GetInstance()->IsRecovering()) {
        MOT_LOG_ERROR("Set CSN is supported only during recovery");
        return RC_ERROR;
    } else {
        m_csn = value;
        return RC_OK;
    }
}

uint64_t CSNManager::GetNextCSN()
{
    if (MOTEngine::GetInstance() && MOTEngine::GetInstance()->IsRecovering()) {
        // Read queries on standby
        MOT_LOG_DEBUG("CSN increment is not supported during recovery");
        return 0;
    } else {
        uint64_t current = 0;
        uint64_t next = 0;
        do {
            current = m_csn;
            next = current + 1;
        } while (!m_csn.compare_exchange_strong(current, next, std::memory_order_acq_rel));
        return next;
    }
}
}  // namespace MOT
