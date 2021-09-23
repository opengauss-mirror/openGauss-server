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
 * infra.cpp
 *    Basic infrastructure helper functions.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/infra.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "infra.h"
#include "mot_error.h"
#include "logger.h"

#include <stdarg.h>

namespace MOT {
DECLARE_LOGGER(Infra, Infra)

extern void mot_infra_report_error(int errorCode, const char* errorContext, const char* format, ...)
{
    va_list args;
    va_start(args, format);

    MOT_REPORT_ERROR_V(errorCode, errorContext, format, args);

    va_end(args);
}

extern void mot_infra_assert(const char* invariant, const char* file, int line)
{
    MOT_LOG_PANIC("Assertion '%s' failed\nat: %s:%d", invariant, file, line);
    MOT_ASSERT(0);
}
}  // namespace MOT
