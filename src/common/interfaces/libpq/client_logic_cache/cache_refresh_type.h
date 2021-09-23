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
 * cache_refresh_type.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_cache\cache_refresh_type.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CACHE_REFRESH_TYPE_H
#define CACHE_REFRESH_TYPE_H
#include <type_traits>

enum class CacheRefreshType {
    CACHE_NONE = 0x00,
    GLOBAL_SETTING = 0x01,
    COLUMN_SETTING = 0x02,
    COLUMNS = 0x04,
    SEARCH_PATH = 0x08,
    COMPATIBILITY = 0x10,
    PROCEDURES = 0x20,
    CACHE_ALL = GLOBAL_SETTING | COLUMN_SETTING | COLUMNS | SEARCH_PATH | COMPATIBILITY | PROCEDURES
};

inline CacheRefreshType operator |(CacheRefreshType lhs, CacheRefreshType rhs)
{
    using T = std::underlying_type<CacheRefreshType>::type;
    return static_cast<CacheRefreshType>(static_cast<T>(lhs) | static_cast<T>(rhs));
}

inline CacheRefreshType operator&(CacheRefreshType lhs, CacheRefreshType rhs)
{
    using T = std::underlying_type<CacheRefreshType>::type;
    return static_cast<CacheRefreshType>(static_cast<T>(lhs) & static_cast<T>(rhs));
}

inline CacheRefreshType &operator |=(CacheRefreshType &lhs, CacheRefreshType rhs)
{
    lhs = lhs | rhs;
    return lhs;
}

inline CacheRefreshType &operator &=(CacheRefreshType &lhs, CacheRefreshType rhs)
{
    lhs = lhs & rhs;
    return lhs;
}

inline CacheRefreshType &operator ^=(CacheRefreshType &lhs, CacheRefreshType rhs)
{
    lhs = CacheRefreshType((int)lhs ^ (int)rhs);
    return lhs;
}

#endif