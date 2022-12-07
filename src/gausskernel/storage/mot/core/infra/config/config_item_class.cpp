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
 * config_item_class.cpp
 *    The class of configuration item.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/config/config_item_class.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "config_item_class.h"
#include <cstring>

namespace MOT {
static const char* CONFIG_ITEM_SECTION_STR = "section";
static const char* CONFIG_ITEM_VALUE_STR = "value";
static const char* CONFIG_ITEM_ARRAY_STR = "array";
static const char* CONFIG_ITEM_UNDEFINED_STR = "N/A";

static const char* ITEM_CLASS_NAMES[] = {
    CONFIG_ITEM_SECTION_STR, CONFIG_ITEM_VALUE_STR, CONFIG_ITEM_ARRAY_STR, CONFIG_ITEM_UNDEFINED_STR};

extern ConfigItemClass ConfigItemClassFromString(const char* itemClassStr)
{
    ConfigItemClass result = ConfigItemClass::CONFIG_ITEM_UNDEFINED;

    if (strcmp(itemClassStr, CONFIG_ITEM_SECTION_STR) == 0) {
        result = ConfigItemClass::CONFIG_ITEM_SECTION;
    } else if (strcmp(itemClassStr, CONFIG_ITEM_VALUE_STR) == 0) {
        result = ConfigItemClass::CONFIG_ITEM_VALUE;
    } else if (strcmp(itemClassStr, CONFIG_ITEM_ARRAY_STR) == 0) {
        result = ConfigItemClass::CONFIG_ITEM_ARRAY;
    }

    return result;
}

extern const char* ConfigItemClassToString(ConfigItemClass configItemClass)
{
    if (configItemClass < ConfigItemClass::CONFIG_ITEM_UNDEFINED) {
        return ITEM_CLASS_NAMES[(uint32_t)configItemClass];
    }
    return CONFIG_ITEM_UNDEFINED_STR;
}
}  // namespace MOT
