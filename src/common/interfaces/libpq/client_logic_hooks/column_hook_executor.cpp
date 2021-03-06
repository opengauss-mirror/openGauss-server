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
 * column_hook_executor.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_hooks\column_hook_executor.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <string.h>
#include <iostream>
#include "column_hook_executor.h"
#include "global_hook_executor.h"

ColumnHookExecutor::ColumnHookExecutor(GlobalHookExecutor *global_hook_executor, Oid oid, const char *function_name)
    : AbstractHookExecutor(function_name)
{
    m_global_hook_executor = global_hook_executor;
    m_global_hook_executor->inc_ref_count();
    m_oid = oid;
}

ColumnHookExecutor::~ColumnHookExecutor()
{
    m_global_hook_executor->dec_ref_count();
    if (m_global_hook_executor->safe_to_remove()) {
        delete m_global_hook_executor;
    }
}
const Oid ColumnHookExecutor::getOid() const
{
    return m_oid;
}

bool ColumnHookExecutor::is_set_operation_allowed(const ICachedColumn *cached_column) const
{
    return true;
}

bool ColumnHookExecutor::is_operator_allowed(const ICachedColumn *ce, const char * const op) const
{
    return true;
}

int ColumnHookExecutor::get_estimated_processed_data_size(int data_size) const
{
    return sizeof(Oid) + get_estimated_processed_data_size_impl(data_size);
}

int ColumnHookExecutor::process_data(const ICachedColumn *cached_column,
    const unsigned char *data, int data_size, unsigned char *processed_data)
{
    if (!m_global_hook_executor->process(this)) {
        fprintf(stderr, "global executor failed to process column excecutor\n");
        return -1;
    }

    errno_t rc = EOK;
    rc = memcpy_s(processed_data, sizeof(Oid), &m_oid, sizeof(Oid));
    securec_check_c(rc, "\0", "\0");
    return process_data_impl(cached_column, data, data_size, processed_data + sizeof(Oid));
}

DecryptDataRes ColumnHookExecutor::deprocess_data(const unsigned char *processed_data, int processed_data_size,
    unsigned char **data, int *data_plain_size)
{
    if (!m_global_hook_executor->process(this)) {
        fprintf(stderr, "global executor failed to deprocess column excecutor\n");
        return DECRYPT_CEK_ERR;
    }
    return deprocess_data_impl(processed_data, processed_data_size, data, data_plain_size);
}


bool ColumnHookExecutor::pre_create(PGClientLogic &client_logic, const StringArgs &args, StringArgs &new_args)
{
    return true;
}

bool ColumnHookExecutor::set_deletion_expected()
{
    return true;
}
void ColumnHookExecutor::dec_ref_count()
{
    AbstractHookExecutor::dec_ref_count();
    m_global_hook_executor->dec_ref_count();
}
void ColumnHookExecutor::inc_ref_count()
{
    AbstractHookExecutor::inc_ref_count();
    m_global_hook_executor->inc_ref_count();
}

GlobalHookExecutor *ColumnHookExecutor::get_global_hook_executor() const
{
    return m_global_hook_executor;
}
