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
 * config_item.cpp
 *    The base interface for all configuration item types.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/config/config_item.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "config_item.h"

namespace MOT {
IMPLEMENT_CLASS_LOGGER(ConfigItem, Configuration)

constexpr const char ConfigItem::PATH_SEP;
constexpr const char* ConfigItem::PATH_SEP_STR;
constexpr size_t ConfigItem::CFG_MAX_PATH_LEN;
constexpr size_t ConfigItem::CFG_MAX_NAME_LEN;
constexpr size_t ConfigItem::CFG_MAX_FULL_PATH_LEN;

ConfigItem::ConfigItem(ConfigItemClass itemClass)
    : m_path(CFG_MAX_PATH_LEN),
      m_name(CFG_MAX_NAME_LEN),
      m_fullPathName(CFG_MAX_FULL_PATH_LEN),
      m_class(itemClass),
      m_depth(0)
{}

bool ConfigItem::Initialize(const char* path, const char* name)
{
    bool result = false;

    if ((path[0] == 0) || (path[0] == PATH_SEP)) {  // either empty or has leading slash
        result = m_path.assign(path);
    } else {  // non-empty and missing leading slash
        result = m_path.format("%s%s", PATH_SEP_STR, path);
    }

    if (!result) {
        MOT_REPORT_ERROR(
            MOT_ERROR_INTERNAL, "Load Configuration", "Failed to initialize configuration path to: %s", path);
    } else {
        // safe copy without buffer overrun
        result = m_name.assign(name);
        if (!result) {
            MOT_REPORT_ERROR(
                MOT_ERROR_INTERNAL, "Load Configuration", "Failed to initialize configuration name to: %s", name);
        } else {
            // safe format without buffer overrun
            result = m_fullPathName.format("%s%s%s", m_path.c_str(), PATH_SEP_STR, m_name.c_str());
            if (!result) {
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                    "Load Configuration",
                    "Failed to initialize full configuration path to: %s%s%s",
                    name,
                    m_path.c_str(),
                    PATH_SEP_STR,
                    m_name.c_str());
            } else {
                m_depth = ComputeDepth();
            }
        }
    }

    return result;
}

uint32_t ConfigItem::ComputeDepth() const
{
    uint32_t result = 0;
    if (m_fullPathName.compare(PATH_SEP_STR) != 0) {
        result = m_fullPathName.count(PATH_SEP);
    }
    MOT_LOG_DEBUG("Computed depth in full path %s (@%p), path %s (@%p) and name %s (@%p): %u",
        m_fullPathName.c_str(),
        m_fullPathName.c_str(),
        m_path.c_str(),
        m_path.c_str(),
        m_name.c_str(),
        m_name.c_str(),
        result);
    return result;
}
}  // namespace MOT
