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
 * config_loader.cpp
 *    The base interface for all configuration loaders.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/config/config_loader.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "global.h"
#include "config_loader.h"

#include <cstring>

namespace MOT {

ConfigLoader::ConfigLoader(const char* name, uint32_t priority) : m_priority(priority), m_configTree(nullptr)
{
    errno_t erc = strncpy_s(m_name, MAX_CONFIG_LOADER_NAME, name, strlen(name));
    securec_check(erc, "\0", "\0");
    m_name[MAX_CONFIG_LOADER_NAME - 1] = 0;
}

}  // namespace MOT
