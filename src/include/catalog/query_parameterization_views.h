/*
* Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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
 * ---------------------------------------------------------------------------------------
 *
 * query_parameterization_views.h
 *      A catlog view that stores information about parameterized queries
 *
 * IDENTIFICATION
 *        src/include/catalog/query_parameterization_views.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef QUERY_PARAMETERIZATION_VIEWS_H
#define QUERY_PARAMETERIZATION_VIEWS_H

#include "utils/date.h"
#define Natts_parameterization_views 6

#define Anum_parameterization_views_reloid 1
#define Anum_parameterization_views_query_type 2
#define Anum_parameterization_views_is_bypass 3
#define Anum_parameterization_views_types 4
#define Anum_parameterization_views_param_nums 5
#define Anum_parameterization_views_parameterized_query 6

struct ParamView {
    Oid relOid;
    const char* queryType;
    bool isBypass;
    int2vector* paramTypes;
    int paramNums;
    const char* parameterizedQuery;
};

extern Datum query_parameterization_views(PG_FUNCTION_ARGS);

extern ParamView* GetAllParamQueries(uint32 *num);
#endif // QUERY_PARAMETERIZATION_VIEWS_H