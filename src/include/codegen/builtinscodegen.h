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
 * builtinscodegen.h
 *        Declarations of code generation for operations on built-in types.
 * 
 * 
 * IDENTIFICATION
 *        src/include/codegen/builtinscodegen.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef LLVM_BUILTINS_H
#define LLVM_BUILTINS_H
#include "catalog/pg_type.h"
#include "codegen/gscodegen.h"
#include "fmgr.h"
#include "nodes/execnodes.h"
#include "nodes/primnodes.h"
#include "utils/biginteger.h"

namespace dorado {
/* Get the byte length of a multibyte character. */
#define MB_LEN(mlen, string)           \
    if ((*string & 0x80) == 0)         \
        mlen = 1;                      \
    else if ((*string & 0xe0) == 0xc0) \
        mlen = 2;                      \
    else if ((*string & 0xf0) == 0xe0) \
        mlen = 3;                      \
    else if ((*string & 0xf8) == 0xf0) \
        mlen = 4;                      \
    else                               \
        mlen = 1;

/*
 * int_codegen.inl
 * template function for simple op('='/'!='/'<='/'>='/'<'/'>') of int type.
 */
template <SimpleOp op>
llvm::Function* int2_sop_codegen();
template <SimpleOp op>
llvm::Function* int4_sop_codegen();
template <SimpleOp op>
llvm::Function* int8_sop_codegen();
template <SimpleOp op, int ltype>
llvm::Function* int2_int4_sop_codegen();
template <SimpleOp op, int ltype>
llvm::Function* int4_int8_sop_codegen();

/*
 * numeric_codegen.inl
 * template function for simple op('='/'!='/'<='/'>='/'<'/'>') of numeric type.
 * Since most cases the data can be converted to BI integer, we especially
 * codegen the bi64 operations. For orginal numeric data, just call original
 * row functions.
 */
template <biop op>
llvm::Function* numeric_sop_codegen();
template <biop op>
llvm::Function* bi64cmp64_codegen();
template <biop op>
llvm::Function* fast_numericbi_sop_codegen();
template <biop op>
llvm::Value* WrapnumericFuncCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* arg);

/* boolcodegen.cpp */
llvm::Function* booleq_codegen();

/* intcodegen.cpp */
llvm::Function* int4pl_codegen();
llvm::Function* int4mi_codegen();
llvm::Function* int4mul_codegen();
llvm::Function* int4div_codegen();
llvm::Function* int8pl_codegen();
llvm::Function* int8mi_codegen();
llvm::Function* int8mul_codegen();
llvm::Function* int8div_codegen();
llvm::Function* int48pl_codegen();
llvm::Function* int84pl_codegen();
llvm::Function* int48mi_codegen();
llvm::Function* int84mi_codegen();
llvm::Function* int48mul_codegen();
llvm::Function* int84mul_codegen();
llvm::Function* int48div_codegen();
llvm::Function* int84div_codegen();

/* floatcodegen.cpp */
llvm::Function* float8eq_codegen();
llvm::Function* float8ne_codegen();
llvm::Function* float8lt_codegen();
llvm::Function* float8le_codegen();
llvm::Function* float8gt_codegen();
llvm::Function* float8ge_codegen();

/* numericcodegen.cpp */
llvm::Function* int8_sum_codegen(Aggref* aggref);
llvm::Function* int8_avg_codegen(Aggref* aggref);
llvm::Function* numeric_sum_codegen(Aggref* aggref);
llvm::Function* vsnumeric_sum_codegen(Aggref* aggref);
llvm::Function* numeric_avg_codegen(Aggref* aggref);
llvm::Function* vsnumeric_avg_codegen(Aggref* aggref);
llvm::Function* vec_count_codegen(Aggref* aggref);
llvm::Function* vsonic_count_codegen(Aggref* aggref);

/* assistant function needed by numeric agg functions */
llvm::Value* DatumGetBINumericCodeGenShort(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* pval);
llvm::Value* DatumGetBINumericCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* pval);
llvm::Value* WrapaddVariableCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* ccontext, llvm::Value* val);
llvm::Value* WrapreplVariableCodeGen(
    GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* ccontext, llvm::Value* oldval, llvm::Value* val);
llvm::Value* WrapmakeNumeric64CodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* data, llvm::Value* scale);
llvm::Value* WrapmakeNumeric128CodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* data, llvm::Value* scale);
void WrapBiAggAddMatrixCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* arg1, llvm::Value* arg2,
    llvm::Value* leftarg, llvm::Value* rightarg, llvm::Value* ctl);
llvm::Value* WrapBiFunMatrixCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* op, llvm::Value* arg1,
    llvm::Value* arg2, llvm::Value* leftarg, llvm::Value* rightarg);
void WrapDeconstructArrayCodeGen(
    GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* pval, llvm::Value* datumarray, llvm::Value* ndatum);
void WrapcallnumericaddCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* cell, llvm::Value* mct,
    llvm::Value* larg, llvm::Value* rarg, llvm::Value* nidx);
void WrapcallscnumaddCodeGen(
    GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* data, llvm::Value* larg, llvm::Value* rarg, llvm::Value* loc);

/* bigintegercodegen.cpp */
llvm::Function* bi64add64_codegen(bool use_ctl);
llvm::Function* bi64sub64_codegen();
llvm::Function* bi64mul64_codegen();
llvm::Function* bi64div64_codegen();

/* assistant function needed by biginteger functions */
llvm::Value* GetInt64MulOutofBoundCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* deltascale);
llvm::Value* ScaleMultiCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* deltascale);
llvm::Value* GetScaleMultiCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* deltascale);
void WrapReplVarWithOldCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* ctl, llvm::Value* Val);
llvm::Value* Wrapbi64div64CodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* larg, llvm::Value* rarg);

/* varcharcodegen.cpp */
llvm::Function* bpchareq_codegen();
llvm::Function* bpcharne_codegen();
llvm::Function* bpcharlen_codegen(int current_encoding);

/* varlenacodegen.cpp */
llvm::Function* texteq_codegen();
llvm::Function* textlt_codegen();
llvm::Function* textgt_codegen();
llvm::Function* substr_codegen();
llvm::Function* rtrim1_codegen();
llvm::Function* btrim1_codegen();
llvm::Function* textlike_codegen();
llvm::Function* textnlike_codegen();

llvm::Function* VarlenaCvtCodeGen();
llvm::Value* VarlenaGetDatumCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* len, llvm::Value* data);
}  // namespace dorado

/* Include template function in int_codegen.inl */
#include "../../gausskernel/runtime/codegen/codegenutil/int_codegen.inl"
#include "../../gausskernel/runtime/codegen/codegenutil/numeric_codegen.inl"

#endif
