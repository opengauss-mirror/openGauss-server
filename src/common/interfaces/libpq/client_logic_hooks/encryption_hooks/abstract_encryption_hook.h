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
 * abstract_encryption_hook.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_hooks\encryption_hooks\abstract_encryption_hook.h
 *
 * -------------------------------------------------------------------------
 */
 
#ifndef ABSTRACT_ENCRYPTION_HOOK_H
#define ABSTRACT_ENCRYPTION_HOOK_H

#include <iostream>
#include <cstring>
#include "libpq-int.h"

class AbstractEncryptionHook {
public:
    const char *getAlgorithm()
    {
        return m_algorithm;
    }
    void setAlgorithm(const char *algorithm)
    {
        if (algorithm) {
            check_strncpy_s(strncpy_s(m_algorithm, sizeof(m_algorithm), algorithm, strlen(algorithm)));
        }
    }

protected:
    char m_algorithm[256];
};

#endif