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
 * psort.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/access/psort.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef PSORT_H
#define PSORT_H

#include "fmgr.h"

Datum psortbuild(PG_FUNCTION_ARGS);
Datum psortoptions(PG_FUNCTION_ARGS);
Datum psortgettuple(PG_FUNCTION_ARGS);
Datum psortgetbitmap(PG_FUNCTION_ARGS);
Datum psortcanreturn(PG_FUNCTION_ARGS);

#endif /* PSORT_H */
