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
 * surrogate_key_manager.cpp
 *    Manages the array of surrogate key generators for all possible connections.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/storage/index/surrogate_key_manager.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "surrogate_key_manager.h"
#include "utilities.h"

DECLARE_LOGGER(SurrogateKeyManager, System)

namespace MOT {

bool SurrogateKeyManager::Initialize(uint16_t maxConnectionCount)
{
    for (uint64_t i = 0; i < maxConnectionCount; i++) {
        m_surrogateArray.push_back(SurrogateKeyGenerator());
    }
    return true;
}

void SurrogateKeyManager::Destroy()
{
    m_surrogateArray.clear();
}

void SurrogateKeyManager::PrintSurrogateMap()
{
    for (int i = 0; i < (int)m_surrogateArray.size(); i++) {
        MOT_LOG_INFO("ThreadID:%d InsertCounter = %lu\n", i, m_surrogateArray[i].GetCurrentCount());
    }
}

}  // namespace MOT
