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

#include "libpq-int.h"
#include "client_logic_common/pg_client_logic_params.h"
#include <cstdlib>

PGClientLogicParams::PGClientLogicParams(const PGClientLogicParams &other)
{
    init(other);
}

void PGClientLogicParams::init(const PGClientLogicParams &other)
{
    nParams = other.nParams;
    new_param_values = NULL;
    adjusted_query = other.adjusted_query;
    adjusted_query_size = other.adjusted_query_size;
    adjusted_paramTypes = NULL;
    copy_sizes = NULL;
    adjusted_param_lengths = NULL;
    adjusted_param_values = NULL;
    new_query = NULL;
    new_query_size = other.new_query_size;
    if (other.new_query != NULL && other.new_query_size > 0) {
        new_query = (char *)malloc(new_query_size + 1);
        if (new_query == NULL) {
            printf("out of memory\n");
            exit(EXIT_FAILURE);
        }
        check_strncpy_s(strncpy_s(new_query, new_query_size + 1, other.new_query, other.new_query_size));
        new_query[new_query_size] = '\0';
    }
    if (other.nParams && other.copy_sizes) {
        copy_sizes = (size_t *)malloc(other.nParams * sizeof(size_t));
        if (copy_sizes == NULL) {
            printf("out of memory\n");
            exit(EXIT_FAILURE);
        }
        check_memcpy_s(
            memcpy_s(copy_sizes, nParams * sizeof(size_t), other.copy_sizes, other.nParams * sizeof(size_t)));
    }
    if (other.new_param_values) {
        new_param_values = (unsigned char **)malloc(other.nParams * sizeof(unsigned char *));
        if (new_param_values == NULL) {
            printf("out of memory\n");
            exit(EXIT_FAILURE);
        }
        for (size_t i = 0; i < other.nParams; ++i) {
            if (copy_sizes != NULL && copy_sizes[i]) {
                new_param_values[i] = (unsigned char *)malloc(other.copy_sizes[i] * sizeof(unsigned char));
                if (new_param_values[i] == NULL) {
                    printf("out of memory\n");
                    exit(EXIT_FAILURE);
                }
                check_memcpy_s(memcpy_s(new_param_values[i], copy_sizes[i] * sizeof(unsigned char),
                    other.new_param_values[i], other.copy_sizes[i]));
            } else {
                new_param_values[i] = NULL;
            }
        }
    }
    if (other.adjusted_paramTypes) {
        adjusted_paramTypes = (Oid *)malloc(other.nParams * sizeof(Oid));
        if (adjusted_paramTypes == NULL) {
            printf("out of memory\n");
            exit(EXIT_FAILURE);
        }
        check_memcpy_s(memcpy_s(adjusted_paramTypes, nParams * sizeof(Oid), other.adjusted_paramTypes,
            other.nParams * sizeof(Oid)));
    }
    if (other.adjusted_param_lengths) {
        adjusted_param_lengths = (int *)malloc(other.nParams * sizeof(int));
        if (adjusted_param_lengths == NULL) {
            printf("out of memory\n");
            exit(EXIT_FAILURE);
        }
        check_memcpy_s(memcpy_s(adjusted_param_lengths, nParams * sizeof(int), other.adjusted_param_lengths,
            other.nParams * sizeof(int)));
    }
}


PGClientLogicParams::~PGClientLogicParams()
{
    for (size_t i = 0; i < nParams; i++) {
        if (copy_sizes[i]) {
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