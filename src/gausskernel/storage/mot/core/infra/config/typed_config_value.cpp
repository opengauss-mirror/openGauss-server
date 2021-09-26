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
 * typed_config_value.cpp
 *    Helper class for printing configuration values.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/config/typed_config_value.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "typed_config_value.h"

namespace MOT {
// prepare template type definitions for macro usage
typedef ConfigValueTypePrinter<uint64_t> UInt64ConfigValueTypePrinter;
typedef ConfigValueTypePrinter<uint32_t> UInt32ConfigValueTypePrinter;
typedef ConfigValueTypePrinter<uint16_t> UInt16ConfigValueTypePrinter;
typedef ConfigValueTypePrinter<uint8_t> UInt8ConfigValueTypePrinter;
typedef ConfigValueTypePrinter<int64_t> Int64ConfigValueTypePrinter;
typedef ConfigValueTypePrinter<int32_t> Int32ConfigValueTypePrinter;
typedef ConfigValueTypePrinter<int16_t> Int16ConfigValueTypePrinter;
typedef ConfigValueTypePrinter<int8_t> Int8ConfigValueTypePrinter;
typedef ConfigValueTypePrinter<double> DoubleConfigValueTypePrinter;
typedef ConfigValueTypePrinter<mot_string> StringConfigValueTypePrinter;
typedef ConfigValueTypePrinter<bool> BoolConfigValueTypePrinter;

IMPLEMENT_TEMPLATE_LOGGER(UInt64ConfigValueTypePrinter, Configuration)
IMPLEMENT_TEMPLATE_LOGGER(UInt32ConfigValueTypePrinter, Configuration)
IMPLEMENT_TEMPLATE_LOGGER(UInt16ConfigValueTypePrinter, Configuration)
IMPLEMENT_TEMPLATE_LOGGER(UInt8ConfigValueTypePrinter, Configuration)
IMPLEMENT_TEMPLATE_LOGGER(Int64ConfigValueTypePrinter, Configuration)
IMPLEMENT_TEMPLATE_LOGGER(Int32ConfigValueTypePrinter, Configuration)
IMPLEMENT_TEMPLATE_LOGGER(Int16ConfigValueTypePrinter, Configuration)
IMPLEMENT_TEMPLATE_LOGGER(Int8ConfigValueTypePrinter, Configuration)
IMPLEMENT_TEMPLATE_LOGGER(DoubleConfigValueTypePrinter, Configuration)
IMPLEMENT_TEMPLATE_LOGGER(StringConfigValueTypePrinter, Configuration)
IMPLEMENT_TEMPLATE_LOGGER(BoolConfigValueTypePrinter, Configuration)
}  // namespace MOT
