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
 * global_hook_executor.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_hooks\global_hook_executor.cpp
 *
 * -------------------------------------------------------------------------
 */
 
#include "global_hook_executor.h"
#include "cl_state.h"
#include <zlib.h>

void GlobalHookExecutor::set_client_logic(PGClientLogic &client_logic)
{
    m_clientLogic = client_logic;
}

PGClientLogic &GlobalHookExecutor::get_client_logic()
{
    return m_clientLogic;
}

bool GlobalHookExecutor::pre_create(const StringArgs &args, const GlobalHookExecutor **existing_global_hook_executors,
    size_t existing_global_hook_executors_size)
{
    return true;
}

bool GlobalHookExecutor::set_deletion_expected()
{
    return true;
}

#if ((!defined(ENABLE_MULTIPLE_NODES)) && (!defined(ENABLE_PRIVATEGAUSS)))
bool GlobalHookExecutor::get_key_path_by_cmk_name(char *key_path_buf, size_t buf_len)
{
    return true;
}
#endif
