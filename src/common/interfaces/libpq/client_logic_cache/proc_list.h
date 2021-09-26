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
 * proc_list.h
 *
 * IDENTIFICATION
 * src\common\interfaces\libpq\client_logic_cache\proc_list.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef PROC_LIST_H
#define PROC_LIST_H

#include "c.h"
#include "libpq/libpq-fe.h"
#include "cached_proc.h"

class ProcList {
public:
    ProcList();
    ~ProcList();
    void clear();
    bool add(CachedProc* cached_proc);
    const CachedProc* get_by_oid(Oid function_oid) const;
    CachedProc* get_by_details(const char* db_name, const char* schema_name, const char* function_name) const;
    bool empty() const;
    const CachedProc* at(size_t i) const;
    CachedProc** m_proc_list;

private:
    size_t m_proc_list_size;
};
#endif
