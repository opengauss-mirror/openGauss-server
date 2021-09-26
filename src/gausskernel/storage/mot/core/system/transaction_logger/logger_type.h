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
 * logger_type.h
 *    Constants for configuration and logger factory.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction_logger/logger_type.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef LOGGER_TYPE_H
#define LOGGER_TYPE_H

#include "type_formatter.h"

namespace MOT {
/**
 * @enum LoggerType
 * @brief Constants for configuration and logger factory.
 */
enum class LoggerType : uint32_t { /** @var Denotes ExternalLogger provided by envelope. */
    EXTERNAL_LOGGER = 0,

    /** @var Invalid logger value. */
    INVALID_LOGGER
};

/**
 * Converts a logger type string to enumeration value.
 * @param logger_type_name The logger type string.
 * @return The logger type enumeration value.
 */
extern LoggerType LoggerTypeFromString(const char* loggerTypeName);

/**
 * Converts a logger type enumeration value to string.
 * @param logger_type The logger type enumeration.
 * @return The logger type string.
 */
extern const char* LoggerTypeToString(const LoggerType& loggerType);

/**
 * @class TypeFormatter<LoggerType>
 * @brief Specialization of TypeFormatter<T> with [ T = LoggerType ].
 */
template <>
class TypeFormatter<LoggerType> {
public:
    /**
     * @brief Converts a value to string.
     * @param value The value to convert.
     * @param[out] stringValue The resulting string.
     */
    static inline const char* ToString(const LoggerType& value, mot_string& stringValue)
    {
        stringValue = LoggerTypeToString(value);
        return stringValue.c_str();
    }

    /**
     * @brief Converts a string to a value.
     * @param The string to convert.
     * @param[out] The resulting value.
     * @return Boolean value denoting whether the conversion succeeded or not.
     */
    static inline bool FromString(const char* stringValue, LoggerType& value)
    {
        value = LoggerTypeFromString(stringValue);
        return value != LoggerType::INVALID_LOGGER;
    }
};
}  // namespace MOT

#endif /* LOGGER_TYPE_H */
