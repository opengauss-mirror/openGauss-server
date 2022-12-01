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
 * config_value_type.cpp
 *    The type of configuration value.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/config/config_value_type.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "config_value_type.h"
#include <cstring>

namespace MOT {
static const char* CONFIG_VALUE_INT64_STR = "int64";
static const char* CONFIG_VALUE_INT32_STR = "int32";
static const char* CONFIG_VALUE_INT16_STR = "int16";
static const char* CONFIG_VALUE_INT8_STR = "int8";
static const char* CONFIG_VALUE_UINT64_STR = "uint64";
static const char* CONFIG_VALUE_UINT32_STR = "uint32";
static const char* CONFIG_VALUE_UINT16_STR = "uint16";
static const char* CONFIG_VALUE_UINT8_STR = "uint8";
static const char* CONFIG_VALUE_DOUBLE_STR = "double";
static const char* CONFIG_VALUE_BOOL_STR = "bool";
static const char* CONFIG_VALUE_STRING_STR = "string";
static const char* CONFIG_VALUE_UNDEFINED_STR = "N/A";

static const char* VALUE_TYPE_NAMES[] = {CONFIG_VALUE_INT64_STR,
    CONFIG_VALUE_INT32_STR,
    CONFIG_VALUE_INT16_STR,
    CONFIG_VALUE_INT8_STR,
    CONFIG_VALUE_UINT64_STR,
    CONFIG_VALUE_UINT32_STR,
    CONFIG_VALUE_UINT16_STR,
    CONFIG_VALUE_UINT8_STR,
    CONFIG_VALUE_DOUBLE_STR,
    CONFIG_VALUE_BOOL_STR,
    CONFIG_VALUE_STRING_STR,
    CONFIG_VALUE_UNDEFINED_STR};

extern ConfigValueType ConfigValueTypeFromString(const char* valueTypeStr)
{
    ConfigValueType result = ConfigValueType::CONFIG_VALUE_UNDEFINED;

    if (strcmp(valueTypeStr, CONFIG_VALUE_INT64_STR) == 0) {
        result = ConfigValueType::CONFIG_VALUE_INT64;
    } else if (strcmp(valueTypeStr, CONFIG_VALUE_INT32_STR) == 0) {
        result = ConfigValueType::CONFIG_VALUE_INT32;
    } else if (strcmp(valueTypeStr, CONFIG_VALUE_INT16_STR) == 0) {
        result = ConfigValueType::CONFIG_VALUE_INT16;
    } else if (strcmp(valueTypeStr, CONFIG_VALUE_INT8_STR) == 0) {
        result = ConfigValueType::CONFIG_VALUE_INT8;
    } else if (strcmp(valueTypeStr, CONFIG_VALUE_UINT64_STR) == 0) {
        result = ConfigValueType::CONFIG_VALUE_UINT64;
    } else if (strcmp(valueTypeStr, CONFIG_VALUE_UINT32_STR) == 0) {
        result = ConfigValueType::CONFIG_VALUE_UINT32;
    } else if (strcmp(valueTypeStr, CONFIG_VALUE_UINT16_STR) == 0) {
        result = ConfigValueType::CONFIG_VALUE_UINT16;
    } else if (strcmp(valueTypeStr, CONFIG_VALUE_UINT8_STR) == 0) {
        result = ConfigValueType::CONFIG_VALUE_UINT8;
    } else if (strcmp(valueTypeStr, CONFIG_VALUE_DOUBLE_STR) == 0) {
        result = ConfigValueType::CONFIG_VALUE_DOUBLE;
    } else if (strcmp(valueTypeStr, CONFIG_VALUE_BOOL_STR) == 0) {
        result = ConfigValueType::CONFIG_VALUE_BOOL;
    } else if (strcmp(valueTypeStr, CONFIG_VALUE_STRING_STR) == 0) {
        result = ConfigValueType::CONFIG_VALUE_STRING;
    }

    return result;
}

extern const char* ConfigValueTypeToString(ConfigValueType valueType)
{
    if (valueType < ConfigValueType::CONFIG_VALUE_UNDEFINED) {
        return VALUE_TYPE_NAMES[(uint32_t)valueType];
    }
    return CONFIG_VALUE_UNDEFINED_STR;
}

extern bool IsConfigValueIntegral(ConfigValueType valueType)
{
    bool result = false;
    if ((valueType == ConfigValueType::CONFIG_VALUE_INT64) || (valueType == ConfigValueType::CONFIG_VALUE_INT32) ||
        (valueType == ConfigValueType::CONFIG_VALUE_INT16) || (valueType == ConfigValueType::CONFIG_VALUE_INT8) ||
        (valueType == ConfigValueType::CONFIG_VALUE_UINT64) || (valueType == ConfigValueType::CONFIG_VALUE_UINT32) ||
        (valueType == ConfigValueType::CONFIG_VALUE_UINT16) || (valueType == ConfigValueType::CONFIG_VALUE_UINT8)) {
        result = true;
    }
    return result;
}
}  // namespace MOT
