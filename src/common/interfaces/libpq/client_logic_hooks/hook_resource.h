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
 * hook_resource.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_hooks\hook_resource.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef HOOK_RESOURCE_H
#define HOOK_RESOURCE_H

#include <string.h>
#include "libpq-int.h"

class HookResource {
public:
    HookResource(const char *function_name, const char *resource_name)
    {
        init(function_name, resource_name);
    }
    void init(const char *function_name, const char *resource_name)
    {
        const char *functionName = (function_name != NULL) ? function_name : "";
        const char *resouceName = (resource_name != NULL) ? resource_name : "";
        check_strncpy_s(strncpy_s(m_function_name, sizeof(m_function_name), functionName, strlen(functionName)));
        check_strncpy_s(strncpy_s(m_resource_name, sizeof(m_resource_name), resouceName, strlen(resouceName)));
    }
    virtual ~HookResource() {}
    const char *get_family_name()
    {
        return m_function_name;
    }

private:
    static const int m_FUNCTION_NAME_SIZE = 256;
    static const int m_RESOURCE_NAME_SIZE = 256;

private:
    char m_function_name[m_FUNCTION_NAME_SIZE];
    char m_resource_name[m_RESOURCE_NAME_SIZE];
};

#endif