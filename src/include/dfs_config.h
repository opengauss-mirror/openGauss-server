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
 * dfs_config.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/dfs_config.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DFS_UNIQUE_H
#define DFS_UNIQUE_H

#define DFS_CXX_HAS_CSTDINT
#define DFS_CXX_HAS_INITIALIZER_LIST
#define DFS_CXX_HAS_NOEXCEPT
#define DFS_CXX_HAS_NULLPTR
#define DFS_CXX_HAS_OVERRIDE
#define DFS_CXX_HAS_UNIQUE_PTR

#ifdef DFS_CXX_HAS_CSTDINT
#include <cstdint>
#else
#include <stdint.h>
#endif

#ifdef DFS_CXX_HAS_NOEXCEPT
#define DFS_NOEXCEPT noexcept
#else
#define DFS_NOEXCEPT throw()
#endif

#ifdef DFS_CXX_HAS_NULLPTR
#define DFS_NULLPTR nullptr
#else
namespace dfs {
class nullptr_t {
public:
    template <class T>
    operator T *() const
    {
        return 0;
    }

    template <class C, class T>
    operator T C::*() const
    {
        return 0;
    }

private:
    void operator&() const;  // whose address can't be taken
};
const nullptr_t nullPtr = {};
}  // namespace dfs
#define DFS_NULLPTR dfs::nullPtr
#endif

#ifdef DFS_CXX_HAS_OVERRIDE
#define DFS_OVERRIDE override
#else
#define DFS_OVERRIDE
#endif

#ifdef DFS_CXX_HAS_UNIQUE_PTR
#define DFS_UNIQUE_PTR std::unique_ptr
#else
#define DFS_UNIQUE_PTR std::auto_ptr
namespace dfs {
template <typename T>
inline T move(T &x)
{
    return x;
}
}  // namespace dfs
#endif

#endif
