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
 * pg_client_logic_params.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_common\pg_client_logic_params.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef PG_CLIENT_LOGIC_PARAMS_H
#define PG_CLIENT_LOGIC_PARAMS_H
#include "libpq-fe.h"

typedef struct PGClientLogicParams {
    PGClientLogicParams()
        : new_query(NULL),
          new_query_size(0),
          new_param_values(NULL),
          nParams(0),
          adjusted_query(NULL),
          adjusted_query_size(0),
          adjusted_paramTypes(NULL),
          adjusted_param_values(NULL),
          adjusted_param_lengths(NULL),
          copy_sizes(NULL) {};
    PGClientLogicParams(const PGClientLogicParams &other) = delete;
    ~PGClientLogicParams();
    char *new_query;
    size_t new_query_size;
    unsigned char **new_param_values;
    size_t nParams;
    const char *adjusted_query;
    size_t adjusted_query_size;
    Oid *adjusted_paramTypes;
    const char **adjusted_param_values;
    int *adjusted_param_lengths;
    size_t *copy_sizes;
} PGClientLogicParams;
#endif /* PG_CLIENT_LOGIC_PARAMS_H */
