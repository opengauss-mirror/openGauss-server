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
 * pg_client_logic_params.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_common\pg_client_logic_params.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "client_logic_common/pg_client_logic_params.h"
#include <cstdlib>

PGClientLogicParams::~PGClientLogicParams()
{
    for (size_t i = 0; i < nParams; i++) {
        if (copy_sizes && copy_sizes[i]) {
            libpq_free(new_param_values[i]);
        }
    }
    libpq_free(new_param_values);
    libpq_free(copy_sizes);
    libpq_free(adjusted_paramTypes);
    libpq_free(adjusted_param_values);
    libpq_free(adjusted_param_lengths);
    libpq_free(new_query);
}
