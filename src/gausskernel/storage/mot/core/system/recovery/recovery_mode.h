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
 * recovery_mode.h
 *    Recovery mode types.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/recovery/recovery_mode.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef RECOVERY_MODE_H
#define RECOVERY_MODE_H

#include <cstdint>
#include "type_formatter.h"

namespace MOT {
enum class RecoveryMode : uint32_t {
    /** @var Use mtls (low footprint) multiple threads. */
    MTLS,

    /** @var Constant value designating invalid recovery mode (indicates error in configuration loading). */
    RECOVERY_INVALID
};

/**
 * @brief Converts a redo log handler type string to enumeration value.
 * @param redoLogHandlerType The redo log handler type string.
 * @return The redo log handler enumeration value.
 */
extern RecoveryMode RecoveryModeFromString(const char* recoveryMode);

/**
 * @brief Converts a redo log handler type enumeration value to string.
 * @param redoLogHandlerType The redo log handler type enumeration.
 * @return The redo log handler type string.
 */
extern const char* RecoveryModeToString(const RecoveryMode& recoveryMode);

/**
 * @class TypeFormatter<RecoveryMode>
 * @brief Specialization of TypeFormatter<T> with [ T = RecoveryMode ].
 */
template <>
class TypeFormatter<RecoveryMode> {
public:
    /**
     * @brief Converts a value to string.
     * @param value The value to convert.
     * @param[out] stringValue The resulting string.
     */
    static inline const char* ToString(const RecoveryMode& value, mot_string& stringValue)
    {
        stringValue = RecoveryModeToString(value);
        return stringValue.c_str();
    }

    /**
     * @brief Converts a string to a value.
     * @param The string to convert.
     * @param[out] The resulting value.
     * @return Boolean value denoting whether the conversion succeeded or not.
     */
    static inline bool FromString(const char* stringValue, RecoveryMode& value)
    {
        value = RecoveryModeFromString(stringValue);
        return value != RecoveryMode::RECOVERY_INVALID;
    }
};
}  // namespace MOT

#endif /* RECOVERY_MODE_H */
