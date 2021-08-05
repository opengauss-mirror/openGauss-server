/**
Copyright (c) 2021 Huawei Technologies Co.,Ltd.

openGauss is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:

  http://license.coscl.org.cn/MulanPSL2

THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details.
---------------------------------------------------------------------------------------

fp_ops.h
       Robust floating point operations

IDENTIFICATION
    src/include/dbmind/db4ai/executor/fp_ops.h

---------------------------------------------------------------------------------------
**/

#ifndef DB4AI_FP_OPS_H
#define DB4AI_FP_OPS_H

/*
 * High precision sum: a + b = *sum + *e
 */
extern void twoSum(double a, double b, double* sum, double* e);

/*
 * The equivalent subtraction a - b = *sub + *e
 */
extern void twoDiff(double a, double b, double* sub, double* e);

/*
 * High precision product a * b = *mult + *e
 */
extern void twoMult(double a, double b, double* mult, double* e);

/*
 * High precision square a * a = *square + *e (faster than twoMult(a, a,..))
 */
extern void square(double a, double* square, double* e);

/*
 * High precision division a / b = *div + *e
 */
extern void twoDiv(double a, double b, double* div, double* e);

#endif //DB4AI_FP_OPS_H
