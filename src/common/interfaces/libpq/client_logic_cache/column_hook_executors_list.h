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
 * column_hook_executors_list.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_cache\column_hook_executors_list.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef COLUMN_HOOK_EXECUTORS_LIST_H
#define COLUMN_HOOK_EXECUTORS_LIST_H

#include "c.h"

class ColumnHookExecutor;
class ColumnHookExecutorsList {
public:
    ColumnHookExecutorsList();
    ~ColumnHookExecutorsList();
    bool add(ColumnHookExecutor *column_hook_executor);
    size_t size() const;
    ColumnHookExecutor *at(size_t position) const;
    ColumnHookExecutor **m_column_hook_executors;
    size_t m_column_hook_executors_size;
};

#endif