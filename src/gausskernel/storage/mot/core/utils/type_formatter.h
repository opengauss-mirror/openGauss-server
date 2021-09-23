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
 * type_formatter.h
 *    Helper template class used in printing user defined types.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/utils/type_formatter.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef TYPE_FORMATTER_H
#define TYPE_FORMATTER_H

#include "mot_string.h"

namespace MOT {
/**
 * @class TypeFormatter
 * @brief Helper template class used in printing user defined types.
 * @tparam T The type for which string conversions are required.
 * @details This class should be specialized for each user defined type.
 */
template <typename T>
class TypeFormatter {
public:
    /**
     * @brief Converts a value to string.
     * @param value The value to convert.
     * @param[out] stringValue The resulting string.
     */
    static inline const char* ToString(const T& value, mot_string& stringValue)
    {
        // by default empty implementation
        return stringValue.c_str();
    }

    /**
     * @brief Converts a string to a value.
     * @param The string to convert.
     * @param[out] The resulting value.
     * @return Boolean value denoting whether the conversion succeeded or not.
     */
    static inline bool FromString(const char* stringValue, T& value)
    {
        // by default not implemented
        return false;
    }
};
}  // namespace MOT

#endif /* TYPE_FORMATTER_H */
