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
 * timestampcodegen.h
 *        Definitions of code generation for timestamp operation
 * 
 * 
 * IDENTIFICATION
 *        src/include/codegen/timestampcodegen.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef LLVM_TIMESTAMP_H
#define LLVM_TIMESTAMP_H
#include "codegen/gscodegen.h"

namespace dorado {
llvm::Function* timestamp_eq_codegen();
llvm::Function* timestamp_ne_codegen();
llvm::Function* timestamp_lt_codegen();
llvm::Function* timestamp_le_codegen();
llvm::Function* timestamp_gt_codegen();
llvm::Function* timestamp_ge_codegen();
}  // namespace dorado
#endif