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
 * redo_log_handler_type.cpp
 *    Redo log handler types.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction_logger/redo_log_handler_type.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "redo_log_handler_type.h"
#include "utilities.h"

#include <string.h>

namespace MOT {
DECLARE_LOGGER(RedoLogHandlerType, RedoLog)

static const char* NONE_STR = "none";
static const char* SYNC_REDO_LOG_HANDLER_STR = "synchronous";
static const char* SEGMENTED_GROUP_SYNC_REDO_LOG_HANDLER_STR = "segmented_group_synchronous";
static const char* INVALID_REDO_LOG_HANDLER_STR = "INVALID";

static const char* redoLogHandlerTypeNames[] = {
    NONE_STR, SYNC_REDO_LOG_HANDLER_STR, SEGMENTED_GROUP_SYNC_REDO_LOG_HANDLER_STR};

RedoLogHandlerType RedoLogHandlerTypeFromString(const char* redoLogHandlerType)
{
    RedoLogHandlerType handlerType = RedoLogHandlerType::INVALID_REDO_LOG_HANDLER;

    if (strcmp(redoLogHandlerType, NONE_STR) == 0) {
        handlerType = RedoLogHandlerType::NONE;
    } else if (strcmp(redoLogHandlerType, SYNC_REDO_LOG_HANDLER_STR) == 0) {
        handlerType = RedoLogHandlerType::SYNC_REDO_LOG_HANDLER;
    } else if (strcmp(redoLogHandlerType, SEGMENTED_GROUP_SYNC_REDO_LOG_HANDLER_STR) == 0) {
        handlerType = RedoLogHandlerType::SEGMENTED_GROUP_SYNC_REDO_LOG_HANDLER;
    } else {
        MOT_LOG_ERROR("Invalid redo log handler type: %s", redoLogHandlerType);
    }

    return handlerType;
}

extern const char* RedoLogHandlerTypeToString(const RedoLogHandlerType& redoLogHandlerType)
{
    if (redoLogHandlerType < RedoLogHandlerType::INVALID_REDO_LOG_HANDLER) {
        return redoLogHandlerTypeNames[(uint32_t)redoLogHandlerType];
    } else {
        return INVALID_REDO_LOG_HANDLER_STR;
    }
}
}  // namespace MOT
