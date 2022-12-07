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
 * recovery_mode.cpp
 *    Recovery mode types.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/recovery/recovery_mode.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "recovery_mode.h"
#include "utilities.h"

#include <cstring>

namespace MOT {
DECLARE_LOGGER(RecoveryMode, Recovery)

static const char* RECOVERY_MTLS_STR = "mtls";
static const char* RECOVERY_INVALID_STR = "INVALID";

static const char* recoveryModeNames[] = {RECOVERY_MTLS_STR, RECOVERY_INVALID_STR};

RecoveryMode RecoveryModeFromString(const char* recoveryMode)
{
    RecoveryMode handlerType = RecoveryMode::RECOVERY_INVALID;

    if (strcmp(recoveryMode, RECOVERY_MTLS_STR) == 0) {
        handlerType = RecoveryMode::MTLS;
    } else {
        MOT_LOG_ERROR("Invalid recovery mode: %s", recoveryMode);
    }

    return handlerType;
}

extern const char* RecoveryModeToString(const RecoveryMode& recoveryMode)
{
    if (recoveryMode < RecoveryMode::RECOVERY_INVALID) {
        return recoveryModeNames[(uint32_t)recoveryMode];
    } else {
        return RECOVERY_INVALID_STR;
    }
}
}  // namespace MOT
