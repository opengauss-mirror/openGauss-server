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
 * config_array.cpp
 *    Configuration item array implementation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/config/config_array.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "config_array.h"

namespace MOT {
ConfigArray::ConfigArray() : ConfigItem(ConfigItemClass::CONFIG_ITEM_ARRAY)
{}

ConfigArray::~ConfigArray()
{}

void ConfigArray::Print(LogLevel logLevel, bool fullPrint) const
{
    // print indented section name
    if (!fullPrint) {
        MOT_LOG(logLevel, "%*s%s", GetDepth(), "", GetName());
    }

    // print values
    for (uint32_t i = 0; i < mItemArray.size(); ++i) {
        if (!fullPrint) {
            MOT_LOG(logLevel, "%*s%s[%u]", GetDepth(), "", GetName(), i);
        }
        mItemArray[i]->Print(logLevel, fullPrint);
    }
}

void ConfigArray::ForEach(ConfigItemVisitor& visitor) const
{
    visitor.OnConfigItem(this);
    for (uint32_t i = 0; i < mItemArray.size(); ++i) {
        const ConfigItem* configItem = mItemArray[i];
        switch (configItem->GetClass()) {
            case ConfigItemClass::CONFIG_ITEM_SECTION:
                static_cast<const ConfigSection*>(configItem)->ForEach(visitor);
                break;

            case ConfigItemClass::CONFIG_ITEM_ARRAY:
                static_cast<const ConfigArray*>(configItem)->ForEach(visitor);
                break;

            default:
                visitor.OnConfigItem(configItem);
                break;
        }
    }
}
}  // namespace MOT
