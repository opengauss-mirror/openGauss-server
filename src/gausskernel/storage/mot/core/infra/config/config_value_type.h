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
 * config_value_type.h
 *    The type of configuration value.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/config/config_value_type.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CONFIG_VALUE_TYPE_H
#define CONFIG_VALUE_TYPE_H

#include <stdint.h>

namespace MOT {
/**
 * @enum ConfigValueType
 * @brief The type of configuration value.
 */
enum class ConfigValueType : uint32_t {
    /** @var The configuration value is of type int64_t. */
    CONFIG_VALUE_INT64,

    /** @var The configuration value is of type int32_t. */
    CONFIG_VALUE_INT32,

    /** @var The configuration value is of type int16_t. */
    CONFIG_VALUE_INT16,

    /** @var The configuration value is of type int8_t. */
    CONFIG_VALUE_INT8,

    /** @var The configuration value is of type uint64_t. */
    CONFIG_VALUE_UINT64,

    /** @var The configuration value is of type uint32_t. */
    CONFIG_VALUE_UINT32,

    /** @var The configuration value is of type uint16_t. */
    CONFIG_VALUE_UINT16,

    /** @var The configuration value is of type uint8_t. */
    CONFIG_VALUE_UINT8,

    /** @var The configuration value is of type double. */
    CONFIG_VALUE_DOUBLE,

    /** @var The configuration value is of type bool. */
    CONFIG_VALUE_BOOL,

    /** @var The configuration value is of type MOT::mot_string. */
    CONFIG_VALUE_STRING,

    /** @var The configuration value is undefined (parse error). */
    CONFIG_VALUE_UNDEFINED
};

/**
 * @brief Converts a value type string to enumeration.
 * @param valueTypeStr The value type string.
 * @return The resulting enumeration.
 */
extern ConfigValueType ConfigValueTypeFromString(const char* valueTypeStr);

/**
 * @brief Converts a configuration value type enumeration to string.
 * @param valueType The value type enumeration.
 * @return The resulting configuration value type string.
 */
extern const char* ConfigValueTypeToString(ConfigValueType valueType);

/** @brief Queries whether a value type is an integral type. */
extern bool IsConfigValueIntegral(ConfigValueType valueType);
}  // namespace MOT

#endif /* CONFIG_VALUE_TYPE_H */
