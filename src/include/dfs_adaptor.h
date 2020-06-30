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
 * ---------------------------------------------------------------------------------------
 *
 * dfs_adaptor.h
 *        Define macros of the same function for different platform.
 *
 *
 * IDENTIFICATION
 *        src/include/dfs_adaptor.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef ADAPTER_HH
#define ADAPTER_HH

#include "dfs_config.h"

#ifndef DFS_CXX_HAS_NOEXCEPT
#define noexcept DFS_NOEXCEPT
#endif

#ifndef DFS_CXX_HAS_OVERRIDE
#define override DFS_OVERRIDE
#endif

#ifdef HAS_DIAGNOSTIC_PUSH
#ifdef __clang__
#define DIAGNOSTIC_PUSH _Pragma("clang diagnostic push")
#define DIAGNOSTIC_POP _Pragma("clang diagnostic pop")
#elif defined(__GNUC__)
#define DIAGNOSTIC_PUSH _Pragma("GCC diagnostic push")
#define DIAGNOSTIC_POP _Pragma("GCC diagnostic pop")
#else
#error("Unknown compiler")
#endif
#else
#define DIAGNOSTIC_PUSH
#define DIAGNOSTIC_POP
#endif

#define PRAGMA(TXT) _Pragma(#TXT)

#ifdef __clang__
#define DIAGNOSTIC_IGNORE(XXX) PRAGMA(clang diagnostic ignored XXX)
#elif defined(__GNUC__)
#define DIAGNOSTIC_IGNORE(XXX) PRAGMA(GCC diagnostic ignored XXX)
#else
#define DIAGNOSTIC_IGNORE(XXX)
#endif

#ifndef DFS_CXX_HAS_UNIQUE_PTR
#define unique_ptr auto_ptr
#endif

#ifndef UINT32_MAX
#define UINT32_MAX 0xffffffff
#endif

#define GTEST_LANG_CXX11 0

#endif /* ADAPTER_H */
