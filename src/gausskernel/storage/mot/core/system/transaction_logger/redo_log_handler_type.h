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
 * redo_log_handler_type.h
 *    Redo log handler types.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction_logger/redo_log_handler_type.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef REDO_LOG_HANDLER_TYPE_H
#define REDO_LOG_HANDLER_TYPE_H

#include <cstdint>
#include "type_formatter.h"

namespace MOT {
enum class RedoLogHandlerType : uint32_t {
    /** @var Denotes no redo log. */
    NONE_REDO_LOG_HANDLER = 0,

    /** @var Denotes SynchronousRedoLogHandler. */
    SYNC_REDO_LOG_HANDLER,

    /** @var Denotes SegmentedGroupSyncRedoLogHandler. */
    SEGMENTED_GROUP_SYNC_REDO_LOG_HANDLER,

    /** @var Denotes invalid handler type. */
    INVALID_REDO_LOG_HANDLER
};

/**
 * @brief Converts a redo log handler type string to enumeration value.
 * @param redoLogHandlerType The redo log handler type string.
 * @return The redo log handler enumeration value.
 */
extern RedoLogHandlerType RedoLogHandlerTypeFromString(const char* redoLogHandlerType);

/**
 * @brief Converts a redo log handler type enumeration value to string.
 * @param redoLogHandlerType The redo log handler type enumeration.
 * @return The redo log handler type string.
 */
extern const char* RedoLogHandlerTypeToString(const RedoLogHandlerType& redoLogHandlerType);

/**
 * @class TypeFormatter<RedoLogHandlerType>
 * @brief Specialization of TypeFormatter<T> with [ T = RedoLogHandlerType ].
 */
template <>
class TypeFormatter<RedoLogHandlerType> {
public:
    /**
     * @brief Converts a value to string.
     * @param value The value to convert.
     * @param[out] stringValue The resulting string.
     */
    static inline const char* ToString(const RedoLogHandlerType& value, mot_string& stringValue)
    {
        stringValue = RedoLogHandlerTypeToString(value);
        return stringValue.c_str();
    }

    /**
     * @brief Converts a string to a value.
     * @param The string to convert.
     * @param[out] The resulting value.
     * @return Boolean value denoting whether the conversion succeeded or not.
     */
    static inline bool FromString(const char* stringValue, RedoLogHandlerType& value)
    {
        value = RedoLogHandlerTypeFromString(stringValue);
        return value != RedoLogHandlerType::INVALID_REDO_LOG_HANDLER;
    }
};
}  // namespace MOT

#endif /* REDO_LOG_HANDLER_TYPE_H */
