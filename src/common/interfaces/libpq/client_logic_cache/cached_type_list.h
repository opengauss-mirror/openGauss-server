/* -------------------------------------------------------------------------
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
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
 * cached_type_list.h
 *
 * IDENTIFICATION
 *   src\common\interfaces\libpq\client_logic_cache\cached_type_list.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CACHED_TYPE_LIST_H
#define CACHED_TYPE_LIST_H

#include "c.h"
#include "libpq-fe.h"
#include "cached_type.h"

class CachedTypeList {
public:
    CachedTypeList();
    ~CachedTypeList();
    void clear();
    bool add(CachedType* cached_type);
    const CachedType* get_by_oid(Oid type_oid) const;
    bool empty() const;
    const CachedType* at(size_t i) const;

private:
    CachedType** m_type_list;
    size_t m_type_list_size;
};

#endif