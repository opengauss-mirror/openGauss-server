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
 * ---------------------------------------------------------------------------------------
 *
 * first_last_agg.h
 *     support agg function first() and last
 *
 * IDENTIFICATION
 *        src/include/utils/first_last_agg.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef FIRST_LAST_AGG_H
#define FIRST_LAST_AGG_H

#include "fmgr.h"

extern Datum first_transition(PG_FUNCTION_ARGS);
extern Datum last_transition(PG_FUNCTION_ARGS);

#endif /* FIRST_LAST_AGG_H */