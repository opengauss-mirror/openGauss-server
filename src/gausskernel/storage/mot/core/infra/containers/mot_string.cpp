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
 * mot_string.cpp
 *    An STL-like string with safe memory management and bounded size.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/containers/mot_string.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "mot_string.h"
#include "infra_util.h"

#include <cstdarg>
#include <cstdio>
#include <cctype>

namespace MOT {
template <>
constexpr uint32_t mot_string::npos;
template <>
constexpr uint32_t mot_string::INIT_CAPACITY;
template <>
constexpr uint32_t mot_string::ALIGNMENT;
template <>
constexpr uint32_t mot_string::DEFAULT_SIZE_LIMIT;
template <>
constexpr uint32_t mot_string::INVALID_CAPACITY;
}  // namespace MOT
