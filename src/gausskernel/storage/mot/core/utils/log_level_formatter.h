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
 * log_level_formatter.h
 *    Log printing level formatter.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/utils/log_level_formatter.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef LOG_LEVEL_FORMATTER_H
#define LOG_LEVEL_FORMATTER_H

#include "log_level.h"
#include "type_formatter.h"

// we put this specialization in a separate file in order to break cyclic dependency
// of Logger, LogLevel and mot_string
namespace MOT {
/**
 * @brief Specialization of TypeFormatter<T> with [ T = LogLevel ].
 */
template <>
class TypeFormatter<LogLevel> {
public:
    /**
     * @brief Converts a value to string.
     * @param value The value to convert.
     * @param[out] stringValue The resulting string.
     */
    static inline const char* ToString(const LogLevel& value, mot_string& stringValue)
    {
        stringValue = LogLevelToString(value);
        return stringValue.c_str();
    }

    /**
     * @brief Converts a string to a value.
     * @param The string to convert.
     * @param[out] The resulting value.
     * @return Boolean value denoting whether the conversion succeeded or not.
     */
    static inline bool FromString(const char* stringValue, LogLevel& value)
    {
        value = LogLevelFromString(stringValue);
        return value != LogLevel::LL_INVALID;
    }
};
}  // namespace MOT

#endif /* LOG_LEVEL_FORMATTER_H */
