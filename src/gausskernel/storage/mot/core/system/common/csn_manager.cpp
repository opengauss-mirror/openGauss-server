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
#include "csn_manager.h"
#include "utilities.h"
#include "mot_engine.h"

namespace MOT {
DECLARE_LOGGER(CSNManager, System);

CSNManager::CSNManager() : m_csn(INITIAL_CSN)
{}

void CSNManager::SetCSN(uint64_t value)
{
    if (!MOTEngine::GetInstance()->IsRecovering()) {
        MOT_LOG_ERROR("Set CSN is supported only during recovery");
        MOT_ASSERT(false);
    } else {
        // CAS is not needed as it used only by a single thread always.
        if (m_csn.load() <= value) {
            m_csn = value + 1;  // GetNextCSN is fetch and then increment.
        }
    }
}
}  // namespace MOT
