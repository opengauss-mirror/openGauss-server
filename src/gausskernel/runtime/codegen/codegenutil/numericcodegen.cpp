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
 * numericcodegen.cpp
 *     codegeneration of aggeregation operations with numeric data.
 *
 * IDENTIFICATION
 *        src/gausskernel/runtime/codegen/codegenutil/numericcodegen.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "codegen/gscodegen.h"
#include "codegen/codegendebuger.h"
#include "codegen/builtinscodegen.h"

#include "catalog/pg_aggregate.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "pgxc/pgxc.h"
#include "vecexecutor/vecexecutor.h"

void WrapBiAggAddMatrix(int arg1, int arg2, Numeric leftarg, Numeric rightarg, bictl* ctl);
Datum WrapBiFunMatrix(biop op, int arg1, int arg2, Numeric leftarg, Numeric rightarg);
void WrapDeconstructArray(ScalarValue pval, Datum** datumarray, int* ndatum);
void Wrapcallnumericadd(hashCell* cell, MemoryContext mcontext, Numeric leftarg, Numeric rightarg, int idx);
void Wrapcallsonicnumericadd(SonicEncodingDatumArray* data, Numeric leftarg, Numeric rightarg, uint32 loc);

namespace dorado {
#define DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, func_type, name, arg1, arg1Type, arg2, arg2Type) \
    GsCodeGen::FnPrototype func_type(llvmCodeGen, name, int64Type);                                 \
    (func_type).addArgument(GsCodeGen::NamedVariable(arg1, arg1Type));                              \
    (func_type).addArgument(GsCodeGen::NamedVariable(arg2, arg2Type));                              \
    jitted_funptr = (func_type).generatePrototype(&builder, &llvmargs[0]);

/**
 * @Description	: Codegeneration of func vint8_sum.
 * @in aggref	: Aggref information about each agg function, which is
 *				  used to get the isTransition info.
 * @return		: The LLVM function that respresents the vint8_sum
 *				  function in LLVM assemble with respect to one row.
 */
llvm::Function* int8_sum_codegen(Aggref* aggref)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    /* Define the datatype and variables that needed */
    DEFINE_CG_VOIDTYPE(voidType);
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int16Type, INT2OID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_TYPE(bictlType, "struct.bictl");
    DEFINE_CG_PTRTYPE(hashCellPtrType, "struct.hashCell");
    DEFINE_CG_PTRTYPE(numericPtrType, "struct.NumericData");
    DEFINE_CG_PTRTYPE(MemCxtDataType, "struct.MemoryContextData");

    DEFINE_CGVAR_INT8(int8_0, 0);
    DEFINE_CGVAR_INT8(int8_1, 1);
    DEFINE_CGVAR_INT16(val_bimask, NUMERIC_BI_MASK);
    DEFINE_CGVAR_INT16(val_numeric128, NUMERIC_128);
    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT64(int64_0, 0);
    DEFINE_CGVAR_INT32(int32_pos_hcell_mval, pos_hcell_mval);

    llvm::Value* llvmargs[4] = {NULL, NULL, NULL, NULL};
    llvm::Value* tmpval = NULL;
    llvm::Value* cellval = NULL;
    llvm::Value* cellflag = NULL;
    llvm::Value* leftarg = NULL;
    llvm::Value* rightarg = NULL;
    llvm::Value* argop1 = NULL;
    llvm::Value* argop2 = NULL;

    /* Define Const arrays */
    llvm::Value* Vals[2] = {int64_0, int32_0};
    llvm::Value* Vals4[4] = {int64_0, int32_0, int32_0, int32_0};

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_int8sum", voidType);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("hashcell", hashCellPtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("hcxt", MemCxtDataType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("aggidx", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("pval", int64Type));
    llvm::Function* jitted_int8sum = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* cell = llvmargs[0];
    llvm::Value* hcxt = llvmargs[1];
    llvm::Value* aggidx = llvmargs[2];
    llvm::Value* pval = llvmargs[3];

    llvm::BasicBlock* entry = &jitted_int8sum->getEntryBlock();
    DEFINE_BLOCK(agg_then, jitted_int8sum);
    DEFINE_BLOCK(agg_else, jitted_int8sum);
    DEFINE_BLOCK(agg_end, jitted_int8sum);
    DEFINE_BLOCK(bi64_add_bi64, jitted_int8sum);
    DEFINE_BLOCK(bi128_add_bi128, jitted_int8sum);

    builder.SetInsertPoint(entry);
    /* do ctl.context = context */
    llvm::Value* bictlval = builder.CreateAlloca(bictlType);
    Vals[0] = int64_0;
    Vals[1] = int32_1;
    tmpval = builder.CreateInBoundsGEP(bictlval, Vals);
    builder.CreateStore(hcxt, tmpval);

    /* get the hash val : hashCell->m_val[aggidx].val */
    Vals4[0] = int64_0;
    Vals4[1] = int32_pos_hcell_mval;
    Vals4[2] = aggidx;
    Vals4[3] = int32_0;
    cellval = builder.CreateInBoundsGEP(cell, Vals4);

    /* get the flag of the hash cell and check if it is NULL */
    Vals4[0] = int64_0;
    Vals4[1] = int32_pos_hcell_mval;
    Vals4[2] = aggidx;
    Vals4[3] = int32_1;
    cellflag = builder.CreateInBoundsGEP(cell, Vals4);
    tmpval = builder.CreateLoad(int8Type, cellflag, "cellflag");

    tmpval = builder.CreateAnd(tmpval, int8_1);
    tmpval = builder.CreateICmpEQ(tmpval, int8_0);
    builder.CreateCondBr(tmpval, agg_else, agg_then);

    /* cell be null, do CopyVarP to cell */
    builder.SetInsertPoint(agg_then);
    if (aggref->aggstage == 0) {
        /* store the first value to bi64 */
        tmpval = WrapmakeNumeric64CodeGen(&builder, pval, int8_0);
        tmpval = WrapaddVariableCodeGen(&builder, hcxt, tmpval);
    } else {
        /* copy the sum data to hash table */
        tmpval = WrapaddVariableCodeGen(&builder, hcxt, pval);
    }
    builder.CreateStore(tmpval, cellval);
    builder.CreateStore(int8_0, cellflag);
    builder.CreateBr(agg_end);

    /* cell be not null, do aggregation */
    builder.SetInsertPoint(agg_else);
    if (aggref->aggstage == 0) {
        /* load the hash cell value from the pointer to hashcell */
        llvm::Value* real_cellval = builder.CreateLoad(int64Type, cellval, "cellval");
        /* leftarg maybe bi64 or bi128 */
        leftarg = builder.CreateIntToPtr(real_cellval, numericPtrType);
        /* store pVal[i](int8) to bi64 */
        rightarg = WrapmakeNumeric64CodeGen(&builder, pval, int8_0);
        rightarg = builder.CreateIntToPtr(rightarg, numericPtrType);

        /* ctl.store_pos = cell->m_val[idx].val */
        Vals[0] = int64_0;
        Vals[1] = int32_0;
        llvm::Value* store_pos_val = builder.CreateInBoundsGEP(bictlval, Vals);
        builder.CreateStore(real_cellval, store_pos_val);

        /* get leftarg->choice.n_header */
        Vals4[0] = int64_0;
        Vals4[1] = int32_1;
        Vals4[2] = int32_0;
        Vals4[3] = int32_0;
        tmpval = builder.CreateInBoundsGEP(leftarg, Vals4);
        tmpval = builder.CreateLoad(int16Type, tmpval, "header");

        /* check numeric is bi128 or not */
        llvm::Value* tmp_is_bi128 = builder.CreateAnd(tmpval, val_bimask);
        llvm::Value* is_bi128 = builder.CreateICmpEQ(tmp_is_bi128, val_numeric128);

        llvm::Value* arg1 = builder.CreateZExt(is_bi128, int32Type);
        llvm::Value* cmpval = builder.CreateICmpEQ(arg1, int32_0);
        builder.CreateCondBr(cmpval, bi64_add_bi64, bi128_add_bi128);

        /* only do bi64addbi64 */
        builder.SetInsertPoint(bi64_add_bi64);
        llvm::Function* func_bi64addbi64 = llvmCodeGen->module()->getFunction("Jitted_bi64add64");
        if (NULL == func_bi64addbi64) {
            func_bi64addbi64 = bi64add64_codegen(true);
        }
        builder.CreateCall(func_bi64addbi64, {leftarg, rightarg, bictlval});
        tmpval = builder.CreateLoad(int64Type, store_pos_val);
        builder.CreateStore(tmpval, cellval);
        builder.CreateBr(agg_end);

        /* with one argument be bi128 */
        builder.SetInsertPoint(bi128_add_bi128);
        WrapBiAggAddMatrixCodeGen(&builder, arg1, int32_0, leftarg, rightarg, bictlval);
        tmpval = builder.CreateLoad(int64Type, store_pos_val);
        builder.CreateStore(tmpval, cellval);
        builder.CreateBr(agg_end);
    } else {
        /* load the hash cell value from the pointer to hashcell */
        llvm::Value* real_cellval = builder.CreateLoad(int64Type, cellval, "cellval");
        /* leftarg maybe bi64 or bi128 */
        leftarg = builder.CreateIntToPtr(real_cellval, numericPtrType);
        /* rightarg maybe bi64 or bi128 */
        rightarg = builder.CreateIntToPtr(pval, numericPtrType);

        /* ctl.store_pos = cell->m_val[idx].val */
        Vals[0] = int64_0;
        Vals[1] = int32_0;
        llvm::Value* store_pos_val = builder.CreateInBoundsGEP(bictlval, Vals);
        builder.CreateStore(real_cellval, store_pos_val);

        /* get leftarg->choice.n_header */
        Vals4[0] = int64_0;
        Vals4[1] = int32_1;
        Vals4[2] = int32_0;
        Vals4[3] = int32_0;
        tmpval = builder.CreateInBoundsGEP(leftarg, Vals4);
        tmpval = builder.CreateLoad(int16Type, tmpval, "header");

        /* check numeric is bi128 or not */
        argop1 = builder.CreateAnd(tmpval, val_bimask);
        argop1 = builder.CreateICmpEQ(argop1, val_numeric128);

        tmpval = builder.CreateInBoundsGEP(rightarg, Vals4);
        tmpval = builder.CreateLoad(int16Type, tmpval, "header");
        argop2 = builder.CreateAnd(tmpval, val_bimask);
        argop2 = builder.CreateICmpEQ(argop2, val_numeric128);

        llvm::Value* cmpand = builder.CreateAnd(argop1, argop2, "cmpand");
        builder.CreateCondBr(cmpand, bi64_add_bi64, bi128_add_bi128);

        builder.SetInsertPoint(bi64_add_bi64);
        llvm::Function* func_bi64addbi64 = llvmCodeGen->module()->getFunction("Jitted_bi64add64");
        if (NULL == func_bi64addbi64) {
            func_bi64addbi64 = bi64add64_codegen(true);
        }
        builder.CreateCall(func_bi64addbi64, {leftarg, rightarg, bictlval});
        tmpval = builder.CreateLoad(int64Type, store_pos_val);
        builder.CreateStore(tmpval, cellval);
        builder.CreateBr(agg_end);

        builder.SetInsertPoint(bi128_add_bi128);
        argop1 = builder.CreateZExt(argop1, int32Type);
        argop2 = builder.CreateZExt(argop2, int32Type);
        WrapBiAggAddMatrixCodeGen(&builder, argop1, argop2, leftarg, rightarg, bictlval);
        tmpval = builder.CreateLoad(int64Type, store_pos_val);
        builder.CreateStore(tmpval, cellval);
        builder.CreateBr(agg_end);
    }

    builder.SetInsertPoint(agg_end);
    builder.CreateRetVoid();

    llvmCodeGen->FinalizeFunction(jitted_int8sum);

    return jitted_int8sum;
}

/**
 * @Description	: Codegeneration of func vint8_avg.
 * @in aggref	: Aggref information about each agg function, which is
 *				  used to get the isTransition info.
 * @return		: The LLVM function that respresents the vint8_avg
 *				  function in LLVM assemble with respect to one row.
 */
llvm::Function* int8_avg_codegen(Aggref* aggref)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    /* Define the datatype and variables that needed */
    DEFINE_CG_VOIDTYPE(voidType);
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int16Type, INT2OID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_TYPE(bictlType, "struct.bictl");
    DEFINE_CG_PTRTYPE(int64PtrType, INT8OID);
    DEFINE_CG_PTRTYPE(hashCellPtrType, "struct.hashCell");
    DEFINE_CG_PTRTYPE(numericPtrType, "struct.NumericData");
    DEFINE_CG_PTRTYPE(MemCxtDataType, "struct.MemoryContextData");

    DEFINE_CGVAR_INT8(int8_0, 0);
    DEFINE_CGVAR_INT8(int8_1, 1);
    DEFINE_CGVAR_INT16(val_bimask, NUMERIC_BI_MASK);
    DEFINE_CGVAR_INT16(val_numeric128, NUMERIC_128);
    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT64(int64_0, 0);
    DEFINE_CGVAR_INT64(int64_1, 1);
    DEFINE_CGVAR_INT32(int32_pos_hcell_mval, pos_hcell_mval);

    llvm::Value* llvmargs[4] = {NULL, NULL, NULL, NULL};
    llvm::Value* tmpval = NULL;
    llvm::Value* cellval = NULL;
    llvm::Value* cellval2 = NULL;
    llvm::Value* cellflag = NULL;
    llvm::Value* cellflag2 = NULL;
    llvm::Value* real_cellval = NULL;
    llvm::Value* leftarg = NULL;
    llvm::Value* rightarg = NULL;
    llvm::Value* argop1 = NULL;
    llvm::Value* argop2 = NULL;
    llvm::Value* store_pos_val = NULL;
    llvm::Function* func_bi64addbi64 = NULL;

    /* Define Const arrays */
    llvm::Value* Vals[2] = {int64_0, int32_0};
    llvm::Value* Vals4[4] = {int64_0, int32_0, int32_0, int32_0};

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_int8avg", voidType);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("hashcell", hashCellPtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("mcontext", MemCxtDataType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("aggidx", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("pval", int64Type));
    llvm::Function* jitted_int8avg = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* cell = llvmargs[0];
    llvm::Value* mcontext = llvmargs[1];
    llvm::Value* aggidx = llvmargs[2];
    llvm::Value* pval = llvmargs[3];

    llvm::BasicBlock* entry = &jitted_int8avg->getEntryBlock();
    DEFINE_BLOCK(agg_then, jitted_int8avg);
    DEFINE_BLOCK(agg_else, jitted_int8avg);
    DEFINE_BLOCK(agg_end, jitted_int8avg);
    DEFINE_BLOCK(bi64_add_bi64, jitted_int8avg);
    DEFINE_BLOCK(bi128_add_bi128, jitted_int8avg);
    DEFINE_BLOCK(add_count, jitted_int8avg);

    /* define Datum *datumarray and int ndatum */
    llvm::Value* datumarr = builder.CreateAlloca(int64PtrType);
    llvm::Value* ndatum = builder.CreateAlloca(int32Type);

    builder.SetInsertPoint(entry);
    /* do ctl.context = context */
    llvm::Value* bictlval = builder.CreateAlloca(bictlType);
    Vals[0] = int64_0;
    Vals[1] = int32_1;
    tmpval = builder.CreateInBoundsGEP(bictlval, Vals);
    builder.CreateStore(mcontext, tmpval);

    /* get the hash val : hashCell->m_val[aggidx].val */
    Vals4[0] = int64_0;
    Vals4[1] = int32_pos_hcell_mval;
    Vals4[2] = aggidx;
    Vals4[3] = int32_0;
    cellval = builder.CreateInBoundsGEP(cell, Vals4);

    /* get the count of hash val : hashCell->m_val[aggidx + 1].val */
    Vals4[2] = builder.CreateAdd(aggidx, int64_1, "val_plus");
    cellval2 = builder.CreateInBoundsGEP(cell, Vals4);

    /* get the flag of the hash cell and check if it is NULL */
    Vals4[0] = int64_0;
    Vals4[1] = int32_pos_hcell_mval;
    Vals4[2] = aggidx;
    Vals4[3] = int32_1;
    cellflag = builder.CreateInBoundsGEP(cell, Vals4);

    /* get the flag of cell->m_val[idx + 1].flag */
    Vals4[2] = builder.CreateAdd(aggidx, int64_1, "flag_plus");
    cellflag2 = builder.CreateInBoundsGEP(cell, Vals4);

    /* load the cell flag and check it*/
    tmpval = builder.CreateLoad(int8Type, cellflag, "cellflag");

    tmpval = builder.CreateAnd(tmpval, int8_1);
    tmpval = builder.CreateICmpEQ(tmpval, int8_0);
    builder.CreateCondBr(tmpval, agg_else, agg_then);

    /* cell be null, do CopyVarP to cell */
    builder.SetInsertPoint(agg_then);
    if (aggref->aggstage == 0) {
        /* store the first value to bi64 */
        tmpval = WrapmakeNumeric64CodeGen(&builder, pval, int8_0);
        tmpval = WrapaddVariableCodeGen(&builder, mcontext, tmpval);
        builder.CreateStore(tmpval, cellval);

        /* count set to be one */
        builder.CreateStore(int64_1, cellval2);
    } else {
        /*
         * construct an array, with the first element be the count
         * and the second element be the real value.
         */
        WrapDeconstructArrayCodeGen(&builder, pval, datumarr, ndatum);

        /* do cell->m_val[idx].val = addVariable(datumarray[1]) */
        llvm::Value* tmpdarr = builder.CreateLoad(int64PtrType, datumarr, "datumarr");
        tmpval = builder.CreateInBoundsGEP(tmpdarr, int64_1);
        tmpval = builder.CreateLoad(int64Type, tmpval, "datumarr1");
        tmpval = WrapaddVariableCodeGen(&builder, mcontext, tmpval);
        builder.CreateStore(tmpval, cellval);

        /* do cell->m_val[idx + 1].val = addVariable(datumarray[0]) */
        tmpval = builder.CreateLoad(int64Type, tmpdarr, "datumarr0");
        tmpval = WrapaddVariableCodeGen(&builder, mcontext, tmpval);
        builder.CreateStore(tmpval, cellval2);
    }

    builder.CreateStore(int8_0, cellflag);
    builder.CreateStore(int8_0, cellflag2);
    builder.CreateBr(agg_end);

    /* cell be not null, do aggregation */
    builder.SetInsertPoint(agg_else);
    if (aggref->aggstage == 0) {
        /* load the hash cell value from the pointer to hashcell */
        real_cellval = builder.CreateLoad(int64Type, cellval, "cellval");
        /* leftarg is bi64 or bi128 */
        leftarg = builder.CreateIntToPtr(real_cellval, numericPtrType);
        /* store pVal[i](int8) to bi64 */
        rightarg = WrapmakeNumeric64CodeGen(&builder, pval, int8_0);
        rightarg = builder.CreateIntToPtr(rightarg, numericPtrType);

        /* ctl.store_pos = cell->m_val[idx].val */
        Vals[0] = int64_0;
        Vals[1] = int32_0;
        store_pos_val = builder.CreateInBoundsGEP(bictlval, Vals);
        builder.CreateStore(real_cellval, store_pos_val);

        /* get leftarg->choice.n_header */
        Vals4[0] = int64_0;
        Vals4[1] = int32_1;
        Vals4[2] = int32_0;
        Vals4[3] = int32_0;
        tmpval = builder.CreateInBoundsGEP(leftarg, Vals4);
        tmpval = builder.CreateLoad(int16Type, tmpval, "lheader");

        tmpval = builder.CreateAnd(tmpval, val_bimask);
        argop1 = builder.CreateICmpEQ(tmpval, val_numeric128);
        builder.CreateCondBr(argop1, bi128_add_bi128, bi64_add_bi64);

        /* only do bi64addbi64 */
        builder.SetInsertPoint(bi64_add_bi64);
        func_bi64addbi64 = llvmCodeGen->module()->getFunction("Jitted_bi64add64");
        if (NULL == func_bi64addbi64) {
            func_bi64addbi64 = bi64add64_codegen(true);
        }
        builder.CreateCall(func_bi64addbi64, {leftarg, rightarg, bictlval});
        tmpval = builder.CreateLoad(int64Type, store_pos_val);
        builder.CreateStore(tmpval, cellval);
        builder.CreateBr(add_count);

        /* with one argument be bi128 */
        builder.SetInsertPoint(bi128_add_bi128);
        argop1 = builder.CreateZExt(argop1, int32Type);
        WrapBiAggAddMatrixCodeGen(&builder, argop1, int32_0, leftarg, rightarg, bictlval);
        tmpval = builder.CreateLoad(int64Type, store_pos_val);
        builder.CreateStore(tmpval, cellval);
        builder.CreateBr(add_count);

        builder.SetInsertPoint(add_count);
        /* count++ : cell->m_val[idx + 1].val++ */
        tmpval = builder.CreateLoad(int64Type, cellval2, "count");
        tmpval = builder.CreateAdd(tmpval, int64_1);
        builder.CreateStore(tmpval, cellval2);
        builder.CreateBr(agg_end);
    } else {
        /*
         * construct an array, with the first element be the count
         * and the second element be the real value.
         */
        WrapDeconstructArrayCodeGen(&builder, pval, datumarr, ndatum);

        /* corresponding to leftarg = (Numeric)(cell->m_val[idx].val) */
        real_cellval = builder.CreateLoad(int64Type, cellval, "cell_val");
        leftarg = builder.CreateIntToPtr(real_cellval, numericPtrType);

        /* corresponding to rightarg = (Numeric)(datumarray[1]) */
        llvm::Value* tmpdarr = builder.CreateLoad(int64PtrType, datumarr, "datumarr");
        tmpval = builder.CreateInBoundsGEP(tmpdarr, int64_1);
        rightarg = builder.CreateLoad(int64Type, tmpval, "datumarr1");

        /* ctl.store_pos = cell->m_val[idx].val */
        Vals[0] = int64_0;
        Vals[1] = int32_0;
        store_pos_val = builder.CreateInBoundsGEP(bictlval, Vals);
        builder.CreateStore(real_cellval, store_pos_val);

        /* check the leftarg or rightarg is BI128 or not */
        /* get arg->choice.n_header */
        Vals4[0] = int64_0;
        Vals4[1] = int32_1;
        Vals4[2] = int32_0;
        Vals4[3] = int32_0;
        tmpval = builder.CreateInBoundsGEP(leftarg, Vals4);
        tmpval = builder.CreateLoad(int16Type, tmpval, "lheader");
        tmpval = builder.CreateAnd(tmpval, val_bimask);
        argop1 = builder.CreateICmpEQ(tmpval, val_numeric128);

        tmpval = builder.CreateInBoundsGEP(rightarg, Vals4);
        tmpval = builder.CreateLoad(int16Type, tmpval, "rheader");
        tmpval = builder.CreateAnd(tmpval, val_bimask);
        argop2 = builder.CreateICmpEQ(tmpval, val_numeric128);

        llvm::Value* cmpor = builder.CreateAnd(argop1, argop2);
        builder.CreateCondBr(cmpor, bi128_add_bi128, bi64_add_bi64);

        /* only do bi64addbi64 */
        builder.SetInsertPoint(bi64_add_bi64);
        func_bi64addbi64 = llvmCodeGen->module()->getFunction("Jitted_bi64add64");
        if (NULL == func_bi64addbi64) {
            func_bi64addbi64 = bi64add64_codegen(true);
        }
        builder.CreateCall(func_bi64addbi64, {leftarg, rightarg, bictlval});
        tmpval = builder.CreateLoad(int64Type, store_pos_val);
        builder.CreateStore(tmpval, cellval);
        builder.CreateBr(add_count);

        /* with one argument be bi128 */
        builder.SetInsertPoint(bi128_add_bi128);
        argop1 = builder.CreateZExt(argop1, int32Type);
        argop2 = builder.CreateZExt(argop2, int32Type);
        WrapBiAggAddMatrixCodeGen(&builder, argop1, argop2, leftarg, rightarg, bictlval);
        tmpval = builder.CreateLoad(int64Type, store_pos_val);
        builder.CreateStore(tmpval, cellval);
        builder.CreateBr(add_count);

        builder.SetInsertPoint(add_count);
        /* calculate count: count = oldcount + count
         * the count num can be stored by int64, call bi64add64 directly.
         */
        llvm::Value* real_cellval2 = builder.CreateLoad(int64Type, cellval2, "cellval2");
        builder.CreateStore(real_cellval2, store_pos_val);
        func_bi64addbi64 = llvmCodeGen->module()->getFunction("Jitted_bi64add64");
        if (NULL == func_bi64addbi64) {
            func_bi64addbi64 = bi64add64_codegen(true);
        }

        /* extract ctl.store_pos and datumarray[0] */
        llvm::Value* tmplarg = builder.CreateIntToPtr(real_cellval2, numericPtrType);

        tmpval = builder.CreateLoad(int64Type, tmpdarr, "datumarr0");
        llvm::Value* tmprarg = builder.CreateIntToPtr(tmpval, numericPtrType);
        builder.CreateCall(func_bi64addbi64, {tmplarg, tmprarg, bictlval});
        builder.CreateBr(agg_end);
    }

    builder.SetInsertPoint(agg_end);
    builder.CreateRetVoid();

    llvmCodeGen->FinalizeFunction(jitted_int8avg);

    return jitted_int8avg;
}

/**
 * @Description	: Codegeneration of func vnumeric_sum.
 * @in aggref	: Aggref information about each agg function, which is
 *				  used to get the isTransition info.
 * @return		: The LLVM function that respresents the vnumeric_sum
 *				  function in LLVM assemble with respect to one row.
 */
llvm::Function* numeric_sum_codegen(Aggref* aggref)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    /* Define the datatype and variables that needed */
    DEFINE_CG_VOIDTYPE(voidType);
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int16Type, INT2OID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_TYPE(bictlType, "struct.bictl");
    DEFINE_CG_PTRTYPE(numericPtrType, "struct.NumericData");
    DEFINE_CG_PTRTYPE(hashCellPtrType, "struct.hashCell");
    DEFINE_CG_PTRTYPE(MemCxtDataType, "struct.MemoryContextData");

    DEFINE_CGVAR_INT8(int8_0, 0);
    DEFINE_CGVAR_INT8(int8_1, 1);
    DEFINE_CGVAR_INT16(val_mask, NUMERIC_BI_MASK);
    DEFINE_CGVAR_INT16(val_nan, NUMERIC_NAN);
    DEFINE_CGVAR_INT16(val_binum128, NUMERIC_128);
    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT64(int64_0, 0);
    DEFINE_CGVAR_INT32(int32_pos_hcell_mval, pos_hcell_mval);

    llvm::Value* llvmargs[4] = {NULL, NULL, NULL, NULL};
    llvm::Value* tmpval = NULL;
    llvm::Value* cellval = NULL;
    llvm::Value* cellflag = NULL;
    llvm::Value* oparg1 = NULL;
    llvm::Value* oparg2 = NULL;

    /* Define Const arrays */
    llvm::Value* Vals[2] = {int64_0, int32_0};
    llvm::Value* Vals4[4] = {int64_0, int32_0, int32_0, int32_0};

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_numericsum", voidType);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("hashcell", hashCellPtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("mcontext", MemCxtDataType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("aggidx", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("pval", int64Type));
    llvm::Function* jitted_numericsum = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* cell = llvmargs[0];
    llvm::Value* mcontext = llvmargs[1];
    llvm::Value* aggidx = llvmargs[2];
    llvm::Value* pval = llvmargs[3];

    llvm::BasicBlock* entry = &jitted_numericsum->getEntryBlock();
    DEFINE_BLOCK(agg_then, jitted_numericsum);
    DEFINE_BLOCK(agg_else, jitted_numericsum);
    DEFINE_BLOCK(agg_end, jitted_numericsum);
    DEFINE_BLOCK(bi_numeric_sum, jitted_numericsum);
    DEFINE_BLOCK(org_numeric_sum, jitted_numericsum);
    DEFINE_BLOCK(bi64_add_bi64, jitted_numericsum);
    DEFINE_BLOCK(bi128_add_bi128, jitted_numericsum);

    builder.SetInsertPoint(entry);

    /* get the hash val : hashCell->m_val[aggidx].val */
    Vals4[0] = int64_0;
    Vals4[1] = int32_pos_hcell_mval;
    Vals4[2] = aggidx;
    Vals4[3] = int32_0;
    cellval = builder.CreateInBoundsGEP(cell, Vals4);

    /* get the flag of the hash cell and check if it is NULL */
    Vals4[0] = int64_0;
    Vals4[1] = int32_pos_hcell_mval;
    Vals4[2] = aggidx;
    Vals4[3] = int32_1;
    cellflag = builder.CreateInBoundsGEP(cell, Vals4);
    aggidx = builder.CreateTrunc(aggidx, int32Type);
    tmpval = builder.CreateLoad(int8Type, cellflag, "cellflag");

    tmpval = builder.CreateAnd(tmpval, int8_1);
    tmpval = builder.CreateICmpEQ(tmpval, int8_0);
    builder.CreateCondBr(tmpval, agg_else, agg_then);

    /* cell be null, do CopyVarP to cell */
    builder.SetInsertPoint(agg_then);
    /* corresponding to DatumGetBINumeric(pval) */
    tmpval = DatumGetBINumericCodeGen(&builder, pval);
    /* corresponding to NumericGetDatum(leftarg) */
    tmpval = builder.CreatePtrToInt(tmpval, int64Type);
    /* corresponding to addVariable(context, NumericGetDatum(leftarg)) */
    tmpval = WrapaddVariableCodeGen(&builder, mcontext, tmpval);

    builder.CreateStore(tmpval, cellval);
    builder.CreateStore(int8_0, cellflag);
    builder.CreateBr(agg_end);

    /* cell be not null, do aggregation */
    builder.SetInsertPoint(agg_else);
    /* corresponding to leftarg = (Numeric)(cell->m_val[idx].val) */
    llvm::Value* real_cellval = builder.CreateLoad(int64Type, cellval, "cell_val");
    llvm::Value* leftarg = builder.CreateIntToPtr(real_cellval, numericPtrType);

    /* get BI numeric val from datum */
    llvm::Value* rightarg = DatumGetBINumericCodeGen(&builder, pval);

    /* corresponding to ctl.context = context */
    llvm::Value* bictlval = builder.CreateAlloca(bictlType);
    Vals[0] = int64_0;
    Vals[1] = int32_1;
    tmpval = builder.CreateInBoundsGEP(bictlval, Vals);
    builder.CreateStore(mcontext, tmpval);

    /* ctl.store_pos = cell->m_val[idx].val */
    Vals[1] = int32_0;
    llvm::Value* store_pos_val = builder.CreateInBoundsGEP(bictlval, Vals);
    builder.CreateStore(real_cellval, store_pos_val);

    /* get numFlags : NUMERIC_NB_FLAGBITS(arg) */
    Vals4[0] = int64_0;
    Vals4[1] = int32_1;
    Vals4[2] = int32_0;
    Vals4[3] = int32_0;
    tmpval = builder.CreateInBoundsGEP(leftarg, Vals4);
    tmpval = builder.CreateLoad(int16Type, tmpval, "lheader");
    llvm::Value* lnumFlags = builder.CreateAnd(tmpval, val_mask);

    tmpval = builder.CreateInBoundsGEP(rightarg, Vals4);
    tmpval = builder.CreateLoad(int16Type, tmpval, "rheader");
    llvm::Value* rnumFlags = builder.CreateAnd(tmpval, val_mask);

    /* check : NUMERIC_FLAG_IS_BI(numFlags) */
    llvm::Value* cmp1 = builder.CreateICmpUGT(lnumFlags, val_nan);
    llvm::Value* cmp2 = builder.CreateICmpUGT(rnumFlags, val_nan);
    llvm::Value* cmpand = builder.CreateAnd(cmp1, cmp2);
    builder.CreateCondBr(cmpand, bi_numeric_sum, org_numeric_sum);

    builder.SetInsertPoint(bi_numeric_sum);
    oparg1 = builder.CreateICmpEQ(lnumFlags, val_binum128);
    oparg2 = builder.CreateICmpEQ(rnumFlags, val_binum128);
    llvm::Value* cmpor = builder.CreateOr(oparg1, oparg2);
    builder.CreateCondBr(cmpor, bi128_add_bi128, bi64_add_bi64);

    builder.SetInsertPoint(bi64_add_bi64);
    llvm::Function* func_bi64addbi64 = llvmCodeGen->module()->getFunction("Jitted_bi64add64");
    if (NULL == func_bi64addbi64) {
        func_bi64addbi64 = bi64add64_codegen(true);
    }
    builder.CreateCall(func_bi64addbi64, {leftarg, rightarg, bictlval});
    tmpval = builder.CreateLoad(int64Type, store_pos_val);
    builder.CreateStore(tmpval, cellval);
    builder.CreateBr(agg_end);

    builder.SetInsertPoint(bi128_add_bi128);
    oparg1 = builder.CreateZExt(oparg1, int32Type);
    oparg2 = builder.CreateZExt(oparg2, int32Type);
    WrapBiAggAddMatrixCodeGen(&builder, oparg1, oparg2, leftarg, rightarg, bictlval);
    tmpval = builder.CreateLoad(int64Type, store_pos_val);
    builder.CreateStore(tmpval, cellval);
    builder.CreateBr(agg_end);

    /* call numeric add */
    builder.SetInsertPoint(org_numeric_sum);
    WrapcallnumericaddCodeGen(&builder, cell, mcontext, leftarg, rightarg, aggidx);
    builder.CreateBr(agg_end);

    builder.SetInsertPoint(agg_end);
    builder.CreateRetVoid();

    llvmCodeGen->FinalizeFunction(jitted_numericsum);

    return jitted_numericsum;
}

/**
 * @Description	: Codegeneration of func vsnumeric_sum, which is used only in sonic
 *				 hashagg case.
 * @in aggref		: Aggref information about each agg function, which is
 *				  used to get the isTransition info.
 * @return		: The LLVM function that respresents the vsnumeric_sum
 *				  function in LLVM assemble with respect to one row.
 */
llvm::Function* vsnumeric_sum_codegen(Aggref* aggref)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    /* Define the datatype and variables that needed */
    DEFINE_CG_VOIDTYPE(voidType);
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int16Type, INT2OID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_TYPE(bictlType, "struct.bictl");
    DEFINE_CG_PTRTYPE(int64PtrType, INT8OID);
    DEFINE_CG_PTRTYPE(sonicEncodingDatumArrayPtrType, "class.SonicEncodingDatumArray");
    DEFINE_CG_PTRTYPE(memCxtPtrType, "struct.MemoryContextData");
    DEFINE_CG_PTRTYPE(atomPtrType, "struct.atom");
    DEFINE_CG_PTRPTRTYPE(atomPtrPtrType, "struct.atom");
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);

    DEFINE_CGVAR_INT8(int8_0, 0);
    DEFINE_CGVAR_INT8(int8_1, 1);
    DEFINE_CGVAR_INT16(val_mask, NUMERIC_BI_MASK);
    DEFINE_CGVAR_INT16(val_nan, NUMERIC_NAN);
    DEFINE_CGVAR_INT16(val_binum128, NUMERIC_128);
    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT32(int32_pos_sdarray_cxt, pos_sdarray_cxt);
    DEFINE_CGVAR_INT32(int32_pos_sdarray_nbit, pos_sdarray_nbit);
    DEFINE_CGVAR_INT32(int32_pos_sdarray_atomsize, pos_sdarray_atomsize);
    DEFINE_CGVAR_INT32(int32_pos_sdarray_arr, pos_sdarray_arr);
    DEFINE_CGVAR_INT32(int32_pos_atom_data, pos_atom_data);
    DEFINE_CGVAR_INT32(int32_pos_atom_nullflag, pos_atom_nullflag);
    DEFINE_CGVAR_INT64(int64_0, 0);

    llvm::Value* llvmargs[3] = {NULL, NULL, NULL};
    llvm::Value* tmpval = NULL;
    llvm::Value* oparg1 = NULL;
    llvm::Value* oparg2 = NULL;

    /* Define Const arrays */
    llvm::Value* Vals2[2] = {int64_0, int32_0};
    llvm::Value* Vals3[3] = {int64_0, int32_0, int32_0};
    llvm::Value* Vals4[4] = {int64_0, int32_0, int32_0, int32_0};

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_sonic_numericsum", voidType);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("sonicdata", sonicEncodingDatumArrayPtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("loc", int32Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("pval", int64Type));
    llvm::Function* jitted_vsnumericsum = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* sdata = llvmargs[0];
    llvm::Value* loc = llvmargs[1];
    llvm::Value* pval = llvmargs[2];

    llvm::BasicBlock* entry = &jitted_vsnumericsum->getEntryBlock();
    DEFINE_BLOCK(agg_then, jitted_vsnumericsum);
    DEFINE_BLOCK(agg_else, jitted_vsnumericsum);
    DEFINE_BLOCK(agg_end, jitted_vsnumericsum);
    DEFINE_BLOCK(bi_numeric_sum, jitted_vsnumericsum);
    DEFINE_BLOCK(org_numeric_sum, jitted_vsnumericsum);
    DEFINE_BLOCK(bi64_add_bi64, jitted_vsnumericsum);
    DEFINE_BLOCK(bi128_add_bi128, jitted_vsnumericsum);

    builder.SetInsertPoint(entry);

    /* get data->m_cxt */
    Vals3[2] = int32_pos_sdarray_cxt;
    llvm::Value* mcxt = builder.CreateInBoundsGEP(sdata, Vals3);
    mcxt = builder.CreateLoad(memCxtPtrType, mcxt, "context");

    /* get the nth flag of sonicdata : first calculate arrIdx and atomIdx */
    Vals3[2] = int32_pos_sdarray_nbit;
    llvm::Value* nbit = builder.CreateInBoundsGEP(sdata, Vals3);
    nbit = builder.CreateLoad(int32Type, nbit, "arridx");
    llvm::Value* arrIdx = builder.CreateLShr(loc, nbit);
    arrIdx = builder.CreateSExt(arrIdx, int64Type);

    Vals3[2] = int32_pos_sdarray_atomsize;
    llvm::Value* atomsize = builder.CreateInBoundsGEP(sdata, Vals3);
    atomsize = builder.CreateLoad(int32Type, atomsize, "atomsize");
    atomsize = builder.CreateSub(atomsize, int32_1);
    llvm::Value* atomIdx = builder.CreateAnd(atomsize, loc);
    atomIdx = builder.CreateSExt(atomIdx, int64Type);

    /* sdata->m_arr[arrIdx] */
    Vals3[2] = int32_pos_sdarray_arr;
    llvm::Value* atom = builder.CreateInBoundsGEP(sdata, Vals3);
    atom = builder.CreateLoad(atomPtrPtrType, atom, "atomptr");
    atom = builder.CreateInBoundsGEP(atom, arrIdx);
    atom = builder.CreateLoad(atomPtrType, atom, "atom");

    /* get address of sdata->m_arr[arrIdx]->data[atomIdx] */
    Vals2[1] = int32_pos_atom_data;
    llvm::Value* data = builder.CreateInBoundsGEP(atom, Vals2);
    data = builder.CreateLoad(int8PtrType, data, "data");
    data = builder.CreateBitCast(data, int64PtrType);
    llvm::Value* dataaddr = builder.CreateInBoundsGEP(data, atomIdx);

    /* sdata->m_arr[arrIdx]->nullFlag[atomIdx] */
    Vals2[1] = int32_pos_atom_nullflag;
    llvm::Value* nullflag = builder.CreateInBoundsGEP(atom, Vals2);
    nullflag = builder.CreateLoad(int8PtrType, nullflag, "nullflagptr");
    nullflag = builder.CreateInBoundsGEP(nullflag, atomIdx);

    /* if the case of data->getNthNullFlag(arrIdx, atomIdx) == NULL */
    tmpval = builder.CreateLoad(int8Type, nullflag, "nullFlag");
    tmpval = builder.CreateAnd(tmpval, int8_1);
    tmpval = builder.CreateICmpEQ(tmpval, int8_0);
    builder.CreateCondBr(tmpval, agg_else, agg_then);

    /* leftflag be null, do set value */
    builder.SetInsertPoint(agg_then);

    /* corresponding to DatumGetBINumeric(pval) */
    tmpval = DatumGetBINumericCodeGen(&builder, pval);
    /* corresponding to NumericGetDatum(leftarg) */
    tmpval = builder.CreatePtrToInt(tmpval, int64Type);
    /* corresponding to addVariable(context, NumericGetDatum(leftarg)) */
    tmpval = WrapaddVariableCodeGen(&builder, mcxt, tmpval);

    builder.CreateStore(tmpval, dataaddr);
    builder.CreateStore(int8_0, nullflag);
    builder.CreateBr(agg_end);

    /* leftarg is not null, do aggregation */
    builder.SetInsertPoint(agg_else);

    /* corresponding to leftarg = (DatumGetBINumeric)(leftdata[0]) */
    llvm::Value* leftdata = builder.CreateLoad(int64Type, dataaddr, "cell_val");
    llvm::Value* leftarg = DatumGetBINumericCodeGen(&builder, leftdata);

    /* get BI numeric val from datum */
    llvm::Value* rightarg = DatumGetBINumericCodeGen(&builder, pval);

    /* corresponding to ctl.context = context */
    llvm::Value* bictlval = builder.CreateAlloca(bictlType);
    Vals2[0] = int64_0;
    Vals2[1] = int32_1;
    tmpval = builder.CreateInBoundsGEP(bictlval, Vals2);
    builder.CreateStore(mcxt, tmpval);

    /* ctl.store_pos = cell->m_val[idx].val */
    Vals2[1] = int32_0;
    llvm::Value* store_pos_val = builder.CreateInBoundsGEP(bictlval, Vals2);
    builder.CreateStore(leftdata, store_pos_val);

    /* get numFlags : NUMERIC_NB_FLAGBITS(arg) */
    Vals4[0] = int64_0;
    Vals4[1] = int32_1;
    Vals4[2] = int32_0;
    Vals4[3] = int32_0;
    tmpval = builder.CreateInBoundsGEP(leftarg, Vals4);
    tmpval = builder.CreateLoad(int16Type, tmpval, "lheader");
    llvm::Value* lnumFlags = builder.CreateAnd(tmpval, val_mask);

    tmpval = builder.CreateInBoundsGEP(rightarg, Vals4);
    tmpval = builder.CreateLoad(int16Type, tmpval, "rheader");
    llvm::Value* rnumFlags = builder.CreateAnd(tmpval, val_mask);

    /* check : NUMERIC_FLAG_IS_BI(numFlags) */
    llvm::Value* cmp1 = builder.CreateICmpUGT(lnumFlags, val_nan);
    llvm::Value* cmp2 = builder.CreateICmpUGT(rnumFlags, val_nan);
    llvm::Value* cmpand = builder.CreateAnd(cmp1, cmp2);
    builder.CreateCondBr(cmpand, bi_numeric_sum, org_numeric_sum);

    builder.SetInsertPoint(bi_numeric_sum);
    oparg1 = builder.CreateICmpEQ(lnumFlags, val_binum128);
    oparg2 = builder.CreateICmpEQ(rnumFlags, val_binum128);
    llvm::Value* cmpor = builder.CreateOr(oparg1, oparg2);
    builder.CreateCondBr(cmpor, bi128_add_bi128, bi64_add_bi64);

    builder.SetInsertPoint(bi64_add_bi64);
    llvm::Function* func_bi64addbi64 = llvmCodeGen->module()->getFunction("Jitted_bi64add64");
    if (NULL == func_bi64addbi64) {
        func_bi64addbi64 = bi64add64_codegen(true);
    }
    builder.CreateCall(func_bi64addbi64, {leftarg, rightarg, bictlval});
    tmpval = builder.CreateLoad(int64Type, store_pos_val);
    builder.CreateStore(tmpval, dataaddr);
    builder.CreateBr(agg_end);

    builder.SetInsertPoint(bi128_add_bi128);
    oparg1 = builder.CreateZExt(oparg1, int32Type);
    oparg2 = builder.CreateZExt(oparg2, int32Type);
    WrapBiAggAddMatrixCodeGen(&builder, oparg1, oparg2, leftarg, rightarg, bictlval);
    tmpval = builder.CreateLoad(int64Type, store_pos_val);
    builder.CreateStore(tmpval, dataaddr);
    builder.CreateBr(agg_end);

    /* call numeric add */
    builder.SetInsertPoint(org_numeric_sum);
    WrapcallscnumaddCodeGen(&builder, sdata, leftarg, rightarg, loc);
    builder.CreateBr(agg_end);

    builder.SetInsertPoint(agg_end);
    builder.CreateRetVoid();

    llvmCodeGen->FinalizeFunction(jitted_vsnumericsum);

    return jitted_vsnumericsum;
}

/**
 * @Description	: Codegeneration of func vnumeric_avg.
 * @in aggref		: Aggref information about each agg function, which is
 *				  used to get the isTransition info.
 * @return		: The LLVM function that respresents the vnumeric_avg
 *				  function in LLVM assemble with respect to one row.
 */
llvm::Function* numeric_avg_codegen(Aggref* aggref)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    /* Define the datatype and variables that needed */
    DEFINE_CG_VOIDTYPE(voidType);
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int16Type, INT2OID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_TYPE(bictlType, "struct.bictl");
    DEFINE_CG_PTRTYPE(int64PtrType, INT8OID);
    DEFINE_CG_PTRTYPE(hashCellPtrType, "struct.hashCell");
    DEFINE_CG_PTRTYPE(numericPtrType, "struct.NumericData");
    DEFINE_CG_PTRTYPE(MemCxtDataType, "struct.MemoryContextData");

    DEFINE_CGVAR_INT8(int8_0, 0);
    DEFINE_CGVAR_INT8(int8_1, 1);
    DEFINE_CGVAR_INT16(val_mask, NUMERIC_BI_MASK);
    DEFINE_CGVAR_INT16(val_nan, NUMERIC_NAN);
    DEFINE_CGVAR_INT16(val_binum128, NUMERIC_128);
    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT64(int64_0, 0);
    DEFINE_CGVAR_INT64(int64_1, 1);
    DEFINE_CGVAR_INT32(int32_pos_hcell_mval, pos_hcell_mval);

    llvm::Value* llvmargs[4] = {NULL, NULL, NULL, NULL};
    llvm::Value* tmpval = NULL;
    llvm::Value* cellval = NULL;
    llvm::Value* cellval2 = NULL;
    llvm::Value* cellflag = NULL;
    llvm::Value* cellflag2 = NULL;
    llvm::Value* oparg1 = NULL;
    llvm::Value* oparg2 = NULL;
    llvm::Value* lnumFlags = NULL;
    llvm::Value* rnumFlags = NULL;
    llvm::Value* real_cellval = NULL;
    llvm::Value* leftarg = NULL;
    llvm::Value* rightarg = NULL;
    llvm::Function* func_bi64addbi64 = NULL;

    /* Define Const arrays */
    llvm::Value* Vals[2] = {int64_0, int32_0};
    llvm::Value* Vals4[4] = {int64_0, int32_0, int32_0, int32_0};

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_numericavg", voidType);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("hashcell", hashCellPtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("mcontext", MemCxtDataType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("aggidx", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("pval", int64Type));
    llvm::Function* jitted_numericavg = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* cell = llvmargs[0];
    llvm::Value* mcontext = llvmargs[1];
    llvm::Value* aggidx = llvmargs[2];
    llvm::Value* pval = llvmargs[3];

    llvm::BasicBlock* entry = &jitted_numericavg->getEntryBlock();
    DEFINE_BLOCK(agg_then, jitted_numericavg);
    DEFINE_BLOCK(agg_else, jitted_numericavg);
    DEFINE_BLOCK(eval_count, jitted_numericavg);
    DEFINE_BLOCK(agg_end, jitted_numericavg);
    DEFINE_BLOCK(bi_numeric_avg, jitted_numericavg);
    DEFINE_BLOCK(org_numeric_avg, jitted_numericavg);
    DEFINE_BLOCK(bi64_add_bi64, jitted_numericavg);
    DEFINE_BLOCK(bi128_add_bi128, jitted_numericavg);

    builder.SetInsertPoint(entry);

    /* define Datum *datumarray and int ndatum */
    llvm::Value* datumarr = builder.CreateAlloca(int64PtrType);
    llvm::Value* ndatum = builder.CreateAlloca(int32Type);

    /* get the hash val : hashCell->m_val[aggidx].val */
    Vals4[0] = int64_0;
    Vals4[1] = int32_pos_hcell_mval;
    Vals4[2] = aggidx;
    Vals4[3] = int32_0;
    cellval = builder.CreateInBoundsGEP(cell, Vals4);

    /* get the count of hash val : hashCell->m_val[aggidx + 1].val */
    Vals4[2] = builder.CreateAdd(aggidx, int64_1, "val_plus");
    cellval2 = builder.CreateInBoundsGEP(cell, Vals4);

    /* get the flag of the hash cell and check if it is NULL */
    Vals4[0] = int64_0;
    Vals4[1] = int32_pos_hcell_mval;
    Vals4[2] = aggidx;
    Vals4[3] = int32_1;
    cellflag = builder.CreateInBoundsGEP(cell, Vals4);

    /* get the flag of cell->m_val[idx + 1].flag */
    Vals4[2] = builder.CreateAdd(aggidx, int64_1, "flag_plus");
    cellflag2 = builder.CreateInBoundsGEP(cell, Vals4);

    /* Now load the cell flag and check it*/
    aggidx = builder.CreateTrunc(aggidx, int32Type);
    tmpval = builder.CreateLoad(int8Type, cellflag, "cellflag");

    tmpval = builder.CreateAnd(tmpval, int8_1);
    tmpval = builder.CreateICmpEQ(tmpval, int8_0);
    builder.CreateCondBr(tmpval, agg_else, agg_then);

    /* cell be null, do CopyVarP to cell */
    builder.SetInsertPoint(agg_then);
    if (aggref->aggstage == 0) {
        /* do leftarg = DatumGetBINumeric(pVal[i]) */
        leftarg = DatumGetBINumericCodeGen(&builder, pval);
        tmpval = builder.CreatePtrToInt(leftarg, int64Type);
        /* corresponding to addVariable(context, NumericGetDatum(leftarg)) */
        tmpval = WrapaddVariableCodeGen(&builder, mcontext, tmpval);
        builder.CreateStore(tmpval, cellval);

        /* count set to be one */
        builder.CreateStore(int64_1, cellval2);
    } else {
        /*
         * construct an array, with the first element be the count
         * and the second element be the real value.
         */
        WrapDeconstructArrayCodeGen(&builder, pval, datumarr, ndatum);

        /* do cell->m_val[idx].val = tbl->CopyVarP(datumarray[1]) */
        llvm::Value* tmpdarr = builder.CreateLoad(int64PtrType, datumarr, "datumarr");
        tmpval = builder.CreateInBoundsGEP(tmpdarr, int64_1);
        tmpval = builder.CreateLoad(int64Type, tmpval, "datumarr1");
        tmpval = WrapaddVariableCodeGen(&builder, mcontext, tmpval);
        builder.CreateStore(tmpval, cellval);

        /* do cell->m_val[idx + 1].val = tbl->CopyVarP(datumarray[0]) */
        tmpval = builder.CreateLoad(int64Type, tmpdarr, "datumarr0");
        tmpval = WrapaddVariableCodeGen(&builder, mcontext, tmpval);
        builder.CreateStore(tmpval, cellval2);
    }

    builder.CreateStore(int8_0, cellflag);
    builder.CreateStore(int8_0, cellflag2);
    builder.CreateBr(agg_end);

    /* cell be not null, do aggregation */
    builder.SetInsertPoint(agg_else);

    /* corresponding to ctl.context = context */
    llvm::Value* bictlval = builder.CreateAlloca(bictlType);
    Vals[0] = int64_0;
    Vals[1] = int32_1;
    tmpval = builder.CreateInBoundsGEP(bictlval, Vals);
    builder.CreateStore(mcontext, tmpval);

    aggidx = builder.CreateTrunc(aggidx, int32Type);
    if (aggref->aggstage == 0) {
        /* corresponding to leftarg = (Numeric)(cell->m_val[idx].val) */
        real_cellval = builder.CreateLoad(int64Type, cellval, "cell_val");
        leftarg = builder.CreateIntToPtr(real_cellval, numericPtrType);

        /* get BI numeric val from datum */
        rightarg = DatumGetBINumericCodeGen(&builder, pval);

        /* get numFlags : NUMERIC_NB_FLAGBITS(arg) */
        Vals4[0] = int64_0;
        Vals4[1] = int32_1;
        Vals4[2] = int32_0;
        Vals4[3] = int32_0;
        tmpval = builder.CreateInBoundsGEP(leftarg, Vals4);
        tmpval = builder.CreateLoad(int16Type, tmpval, "lheader");
        lnumFlags = builder.CreateAnd(tmpval, val_mask);

        tmpval = builder.CreateInBoundsGEP(rightarg, Vals4);
        tmpval = builder.CreateLoad(int16Type, tmpval, "rheader");
        rnumFlags = builder.CreateAnd(tmpval, val_mask);

        /* check : NUMERIC_FLAG_IS_BI(numFlags) */
        llvm::Value* cmp1 = builder.CreateICmpUGT(lnumFlags, val_nan);
        llvm::Value* cmp2 = builder.CreateICmpUGT(rnumFlags, val_nan);
        llvm::Value* cmpand = builder.CreateAnd(cmp1, cmp2);
        builder.CreateCondBr(cmpand, bi_numeric_avg, org_numeric_avg);

        builder.SetInsertPoint(bi_numeric_avg);
        /* ctl.store_pos = cell->m_val[idx].val */
        Vals[0] = int32_0;
        Vals[1] = int32_0;
        llvm::Value* store_pos_val = builder.CreateInBoundsGEP(bictlval, Vals);
        builder.CreateStore(real_cellval, store_pos_val);
        /* check numeric is BI128 */
        oparg1 = builder.CreateICmpEQ(lnumFlags, val_binum128);
        oparg2 = builder.CreateICmpEQ(rnumFlags, val_binum128);
        llvm::Value* cmpor = builder.CreateOr(oparg1, oparg2);
        builder.CreateCondBr(cmpor, bi128_add_bi128, bi64_add_bi64);

        builder.SetInsertPoint(bi64_add_bi64);
        func_bi64addbi64 = llvmCodeGen->module()->getFunction("Jitted_bi64add64");
        if (NULL == func_bi64addbi64) {
            func_bi64addbi64 = bi64add64_codegen(true);
        }
        builder.CreateCall(func_bi64addbi64, {leftarg, rightarg, bictlval});
        tmpval = builder.CreateLoad(int64Type, store_pos_val);
        builder.CreateStore(tmpval, cellval);
        builder.CreateBr(eval_count);

        builder.SetInsertPoint(bi128_add_bi128);
        oparg1 = builder.CreateZExt(oparg1, int32Type);
        oparg2 = builder.CreateZExt(oparg2, int32Type);
        WrapBiAggAddMatrixCodeGen(&builder, oparg1, oparg2, leftarg, rightarg, bictlval);
        tmpval = builder.CreateLoad(int64Type, store_pos_val);
        builder.CreateStore(tmpval, cellval);
        builder.CreateBr(eval_count);

        /* call numeric add */
        builder.SetInsertPoint(org_numeric_avg);
        WrapcallnumericaddCodeGen(&builder, cell, mcontext, leftarg, rightarg, aggidx);
        builder.CreateBr(eval_count);

        builder.SetInsertPoint(eval_count);
        /* cell->m_val[idx+1].val++ */
        tmpval = builder.CreateLoad(int64Type, cellval2, "count");
        tmpval = builder.CreateAdd(tmpval, int64_1);
        builder.CreateStore(tmpval, cellval2);
        builder.CreateBr(agg_end);
    } else {
        /*
         * construct an array, with the first element be the count
         * and the second element be the real value.
         */
        WrapDeconstructArrayCodeGen(&builder, pval, datumarr, ndatum);

        /* calculate sum : sum = sum + num */
        /* corresponding to leftarg = (Numeric)(cell->m_val[idx].val) */
        real_cellval = builder.CreateLoad(int64Type, cellval, "cell_val");
        leftarg = builder.CreateIntToPtr(real_cellval, numericPtrType);

        /* corresponding to rightarg = DatumGetBINumeric(datumarray[1]) */
        llvm::Value* tmpdarr = builder.CreateLoad(int64PtrType, datumarr, "datumarr");
        tmpval = builder.CreateInBoundsGEP(tmpdarr, int64_1);
        tmpval = builder.CreateLoad(int64Type, tmpval, "datumarr1");
        rightarg = DatumGetBINumericCodeGen(&builder, tmpval);

        /* get numFlags : NUMERIC_NB_FLAGBITS(arg) */
        Vals4[0] = int64_0;
        Vals4[1] = int32_1;
        Vals4[2] = int32_0;
        Vals4[3] = int32_0;
        tmpval = builder.CreateInBoundsGEP(leftarg, Vals4);
        tmpval = builder.CreateLoad(int16Type, tmpval, "lheader");
        lnumFlags = builder.CreateAnd(tmpval, val_mask);

        tmpval = builder.CreateInBoundsGEP(rightarg, Vals4);
        tmpval = builder.CreateLoad(int16Type, tmpval, "rheader");
        rnumFlags = builder.CreateAnd(tmpval, val_mask);

        /* check : NUMERIC_FLAG_IS_BI(numFlags) */
        llvm::Value* cmp1 = builder.CreateICmpUGT(lnumFlags, val_nan);
        llvm::Value* cmp2 = builder.CreateICmpUGT(rnumFlags, val_nan);
        llvm::Value* cmpand = builder.CreateAnd(cmp1, cmp2);
        builder.CreateCondBr(cmpand, bi_numeric_avg, org_numeric_avg);

        builder.SetInsertPoint(bi_numeric_avg);
        /* ctl.store_pos = cell->m_val[idx].val */
        Vals[0] = int32_0;
        Vals[1] = int32_0;
        llvm::Value* store_pos_val = builder.CreateInBoundsGEP(bictlval, Vals);
        builder.CreateStore(real_cellval, store_pos_val);
        /* check numeric is BI128 */
        oparg1 = builder.CreateICmpEQ(lnumFlags, val_binum128);
        oparg2 = builder.CreateICmpEQ(rnumFlags, val_binum128);
        llvm::Value* cmpor = builder.CreateOr(oparg1, oparg2);
        builder.CreateCondBr(cmpor, bi128_add_bi128, bi64_add_bi64);

        builder.SetInsertPoint(bi64_add_bi64);
        func_bi64addbi64 = llvmCodeGen->module()->getFunction("Jitted_bi64add64");
        if (NULL == func_bi64addbi64) {
            func_bi64addbi64 = bi64add64_codegen(true);
        }
        builder.CreateCall(func_bi64addbi64, {leftarg, rightarg, bictlval});
        tmpval = builder.CreateLoad(int64Type, store_pos_val);
        builder.CreateStore(tmpval, cellval);
        builder.CreateBr(eval_count);

        builder.SetInsertPoint(bi128_add_bi128);
        oparg1 = builder.CreateZExt(oparg1, int32Type);
        oparg2 = builder.CreateZExt(oparg2, int32Type);
        WrapBiAggAddMatrixCodeGen(&builder, oparg1, oparg2, leftarg, rightarg, bictlval);
        tmpval = builder.CreateLoad(int64Type, store_pos_val);
        builder.CreateStore(tmpval, cellval);
        builder.CreateBr(eval_count);

        /* call numeric add */
        builder.SetInsertPoint(org_numeric_avg);
        WrapcallnumericaddCodeGen(&builder, cell, mcontext, leftarg, rightarg, aggidx);
        builder.CreateBr(eval_count);

        builder.SetInsertPoint(eval_count);
        /* calculate count: count = oldcount + count
         * the count num can be stored by int64, call bi64add64 directly.
         */
        llvm::Value* real_cellval2 = builder.CreateLoad(int64Type, cellval2, "cellval2");
        builder.CreateStore(real_cellval2, store_pos_val);
        func_bi64addbi64 = llvmCodeGen->module()->getFunction("Jitted_bi64add64");
        if (NULL == func_bi64addbi64) {
            func_bi64addbi64 = bi64add64_codegen(true);
        }

        /* extract ctl.store_pos and datumarray[0] */
        llvm::Value* tmplarg = builder.CreateIntToPtr(real_cellval2, numericPtrType);

        tmpval = builder.CreateLoad(int64Type, tmpdarr, "datumarr0");
        llvm::Value* tmprarg = builder.CreateIntToPtr(tmpval, numericPtrType);
        builder.CreateCall(func_bi64addbi64, {tmplarg, tmprarg, bictlval});
        builder.CreateBr(agg_end);
    }

    builder.SetInsertPoint(agg_end);
    builder.CreateRetVoid();

    llvmCodeGen->FinalizeFunction(jitted_numericavg);

    return jitted_numericavg;
}

/**
 * @Description	: Codegeneration of func vsnumeric_avg, which is used only
 *				  in sonic hash case.
 * @in aggref		: Aggref information about each agg function, which is
 *				  used to get the isTransition info.
 * @return		: The LLVM function that respresents the vsnumeric_avg
 *				  function in LLVM assemble with respect to one row.
 */
llvm::Function* vsnumeric_avg_codegen(Aggref* aggref)
{

    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    /* Define the datatype and variables that needed */
    DEFINE_CG_VOIDTYPE(voidType);
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int16Type, INT2OID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_TYPE(bictlType, "struct.bictl");
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(int64PtrType, INT8OID);
    DEFINE_CG_PTRTYPE(numericPtrType, "struct.NumericData");
    DEFINE_CG_PTRTYPE(sonicDatumArrayPtrType, "class.SonicDatumArray");
    DEFINE_CG_PTRTYPE(sonicEncodingDatumArrayPtrType, "class.SonicEncodingDatumArray");
    DEFINE_CG_PTRTYPE(memCxtPtrType, "struct.MemoryContextData");
    DEFINE_CG_PTRTYPE(atomPtrType, "struct.atom");
    DEFINE_CG_PTRPTRTYPE(atomPtrPtrType, "struct.atom");

    DEFINE_CGVAR_INT8(int8_0, 0);
    DEFINE_CGVAR_INT8(int8_1, 1);
    DEFINE_CGVAR_INT16(val_mask, NUMERIC_BI_MASK);
    DEFINE_CGVAR_INT16(val_nan, NUMERIC_NAN);
    DEFINE_CGVAR_INT16(val_binum128, NUMERIC_128);
    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT32(int32_pos_sdarray_cxt, pos_sdarray_cxt);
    DEFINE_CGVAR_INT32(int32_pos_sdarray_nbit, pos_sdarray_nbit);
    DEFINE_CGVAR_INT32(int32_pos_sdarray_atomsize, pos_sdarray_atomsize);
    DEFINE_CGVAR_INT32(int32_pos_sdarray_arr, pos_sdarray_arr);
    DEFINE_CGVAR_INT32(int32_pos_atom_data, pos_atom_data);
    DEFINE_CGVAR_INT32(int32_pos_atom_nullflag, pos_atom_nullflag);
    DEFINE_CGVAR_INT64(int64_0, 0);
    DEFINE_CGVAR_INT64(int64_1, 1);

    llvm::Value* llvmargs[4] = {NULL, NULL, NULL, NULL};
    llvm::Value* tmpval = NULL;
    llvm::Value* oparg1 = NULL;
    llvm::Value* oparg2 = NULL;
    llvm::Value* lnumFlags = NULL;
    llvm::Value* rnumFlags = NULL;
    llvm::Value* realdata = NULL;
    llvm::Value* leftarg = NULL;
    llvm::Value* rightarg = NULL;
    llvm::Function* func_bi64addbi64 = NULL;

    /* Define Const arrays */
    llvm::Value* Vals2[2] = {int64_0, int32_0};
    llvm::Value* Vals3[3] = {int64_0, int32_0, int32_0};
    llvm::Value* Vals4[4] = {int64_0, int32_0, int32_0, int32_0};

    /* Define llvm function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_sonic_numericavg", voidType);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("sonicdata", sonicEncodingDatumArrayPtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("countdata", sonicDatumArrayPtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("loc", int32Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("pval", int64Type));
    llvm::Function* jitted_vsnumericavg = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* sdata = llvmargs[0];
    llvm::Value* scount = llvmargs[1];
    llvm::Value* loc = llvmargs[2];
    llvm::Value* pval = llvmargs[3];

    llvm::BasicBlock* entry = &jitted_vsnumericavg->getEntryBlock();
    DEFINE_BLOCK(agg_then, jitted_vsnumericavg);
    DEFINE_BLOCK(agg_else, jitted_vsnumericavg);
    DEFINE_BLOCK(eval_count, jitted_vsnumericavg);
    DEFINE_BLOCK(agg_end, jitted_vsnumericavg);
    DEFINE_BLOCK(bi_numeric_avg, jitted_vsnumericavg);
    DEFINE_BLOCK(org_numeric_avg, jitted_vsnumericavg);
    DEFINE_BLOCK(bi64_add_bi64, jitted_vsnumericavg);
    DEFINE_BLOCK(bi128_add_bi128, jitted_vsnumericavg);

    builder.SetInsertPoint(entry);
    /* define Datum *datumarray and int ndatum */
    llvm::Value* datumarr = builder.CreateAlloca(int64PtrType);
    llvm::Value* ndatum = builder.CreateAlloca(int32Type);

    /* get data->m_cxt */
    Vals3[2] = int32_pos_sdarray_cxt;
    llvm::Value* mcxt = builder.CreateInBoundsGEP(sdata, Vals3);
    mcxt = builder.CreateLoad(memCxtPtrType, mcxt, "context");

    /* get sonic data according to sonic datum array and locidx */
    /* calculate arrIdx and atomIdx */
    Vals3[2] = int32_pos_sdarray_nbit;
    llvm::Value* nbit = builder.CreateInBoundsGEP(sdata, Vals3);
    nbit = builder.CreateLoad(int32Type, nbit, "arridx");
    llvm::Value* arrIdx = builder.CreateLShr(loc, nbit);
    arrIdx = builder.CreateSExt(arrIdx, int64Type);

    Vals3[2] = int32_pos_sdarray_atomsize;
    llvm::Value* atomsize = builder.CreateInBoundsGEP(sdata, Vals3);
    atomsize = builder.CreateLoad(int32Type, atomsize, "atomsize");
    atomsize = builder.CreateSub(atomsize, int32_1);
    llvm::Value* atomIdx = builder.CreateAnd(atomsize, loc);
    atomIdx = builder.CreateSExt(atomIdx, int64Type);

    /* sdata->m_arr[arrIdx] */
    Vals3[2] = int32_pos_sdarray_arr;
    llvm::Value* atom = builder.CreateInBoundsGEP(sdata, Vals3);
    atom = builder.CreateLoad(atomPtrPtrType, atom, "atomptr");
    atom = builder.CreateInBoundsGEP(atom, arrIdx);
    atom = builder.CreateLoad(atomPtrType, atom, "atom");

    /* get address of sdata->m_arr[arrIdx]->data[atomIdx] */
    Vals2[1] = int32_pos_atom_data;
    llvm::Value* data = builder.CreateInBoundsGEP(atom, Vals2);
    data = builder.CreateLoad(int8PtrType, data, "data");
    data = builder.CreateBitCast(data, int64PtrType);
    llvm::Value* dataptr = builder.CreateInBoundsGEP(data, atomIdx);

    /* get scount->m_arr[arrIdx] */
    Vals2[1] = int32_pos_sdarray_arr;
    llvm::Value* cntatom = builder.CreateInBoundsGEP(scount, Vals2);
    cntatom = builder.CreateLoad(atomPtrPtrType, cntatom, "cntatomptr");
    cntatom = builder.CreateInBoundsGEP(cntatom, arrIdx);
    cntatom = builder.CreateLoad(atomPtrType, cntatom, "cntatom");

    /* get address of scount->m_arr[arrIdx]->data[atomIdx] */
    Vals2[1] = int32_pos_atom_data;
    llvm::Value* cntdata = builder.CreateInBoundsGEP(cntatom, Vals2);
    cntdata = builder.CreateLoad(int8PtrType, cntdata, "cntdata");
    cntdata = builder.CreateBitCast(cntdata, int64PtrType);
    llvm::Value* cntptr = builder.CreateInBoundsGEP(cntdata, atomIdx);

    /* get sdata->m_arr[arrIdx]->nullFlag[atomIdx] */
    Vals2[1] = int32_pos_atom_nullflag;
    llvm::Value* dataflag = builder.CreateInBoundsGEP(atom, Vals2);
    dataflag = builder.CreateLoad(int8PtrType, dataflag, "dataflagptr");
    dataflag = builder.CreateInBoundsGEP(dataflag, atomIdx);

    /* get scount>m_arr[arrIdx]->nullFlag[atomIdx] */
    Vals2[1] = int32_pos_atom_nullflag;
    llvm::Value* cntflag = builder.CreateInBoundsGEP(cntatom, Vals2);
    cntflag = builder.CreateLoad(int8PtrType, cntflag, "cntflagptr");
    cntflag = builder.CreateInBoundsGEP(cntflag, atomIdx);

    /* Now load the data flag and check it*/
    tmpval = builder.CreateLoad(int8Type, dataflag, "cntflag");

    tmpval = builder.CreateAnd(tmpval, int8_1);
    tmpval = builder.CreateICmpEQ(tmpval, int8_0);
    builder.CreateCondBr(tmpval, agg_else, agg_then);

    /* sonic data is null, set inital value */
    builder.SetInsertPoint(agg_then);
    if (aggref->aggstage == 0) {
        /* do leftarg = DatumGetBINumeric(pVal[i]) */
        leftarg = DatumGetBINumericCodeGen(&builder, pval);
        tmpval = builder.CreatePtrToInt(leftarg, int64Type);
        /* corresponding to addVariable(context, NumericGetDatum(leftarg)) */
        tmpval = WrapaddVariableCodeGen(&builder, mcxt, tmpval);
        builder.CreateStore(tmpval, dataptr);

        /* count set to be one */
        builder.CreateStore(int64_1, cntptr);
    } else {
        /*
         * construct an array, with the first element be the count
         * and the second element be the real value.
         */
        WrapDeconstructArrayCodeGen(&builder, pval, datumarr, ndatum);

        /* do data->setValue(datumarray[1], false, arridx, atomIdx) */
        llvm::Value* tmpdarr = builder.CreateLoad(int64PtrType, datumarr, "datumarr");
        tmpval = builder.CreateInBoundsGEP(tmpdarr, int64_1);
        tmpval = builder.CreateLoad(int64Type, tmpval, "datumarr1");
        tmpval = WrapaddVariableCodeGen(&builder, mcxt, tmpval);
        builder.CreateStore(tmpval, dataptr);

        /* do cell->m_val[idx + 1].val = tbl->CopyVarP(datumarray[0]) */
        tmpval = builder.CreateLoad(int64Type, tmpdarr, "datumarr0");
        tmpval = builder.CreateIntToPtr(tmpval, numericPtrType);
        Vals4[0] = int64_0;
        Vals4[1] = int32_1;
        Vals4[2] = int32_0;
        Vals4[3] = int32_1;
        tmpval = builder.CreateInBoundsGEP(tmpval, Vals4);
        tmpval = builder.CreateBitCast(tmpval, int64PtrType);
        tmpval = builder.CreateLoad(int64Type, tmpval, "lval");
        builder.CreateStore(tmpval, cntptr);
    }

    builder.CreateStore(int8_0, dataflag);
    builder.CreateStore(int8_0, cntflag);
    builder.CreateBr(agg_end);

    /* sonic data is not null, do aggregation */
    builder.SetInsertPoint(agg_else);

    /* corresponding to ctl.context = data->m_cxt */
    llvm::Value* bictlval = builder.CreateAlloca(bictlType);
    Vals2[0] = int64_0;
    Vals2[1] = int32_1;
    tmpval = builder.CreateInBoundsGEP(bictlval, Vals2);
    builder.CreateStore(mcxt, tmpval);

    if (aggref->aggstage == 0) {
        /* corresponding to leftarg = (Numeric)(cell->m_val[idx].val) */
        realdata = builder.CreateLoad(int64Type, dataptr, "sonic_data");
        leftarg = builder.CreateIntToPtr(realdata, numericPtrType);

        /* get BI numeric val from datum */
        rightarg = DatumGetBINumericCodeGen(&builder, pval);

        /* get numFlags : NUMERIC_NB_FLAGBITS(arg) */
        Vals4[0] = int64_0;
        Vals4[1] = int32_1;
        Vals4[2] = int32_0;
        Vals4[3] = int32_0;
        tmpval = builder.CreateInBoundsGEP(leftarg, Vals4);
        tmpval = builder.CreateLoad(int16Type, tmpval, "lheader");
        lnumFlags = builder.CreateAnd(tmpval, val_mask);

        tmpval = builder.CreateInBoundsGEP(rightarg, Vals4);
        tmpval = builder.CreateLoad(int16Type, tmpval, "rheader");
        rnumFlags = builder.CreateAnd(tmpval, val_mask);

        /* check : NUMERIC_FLAG_IS_BI(numFlags) */
        llvm::Value* cmp1 = builder.CreateICmpUGT(lnumFlags, val_nan);
        llvm::Value* cmp2 = builder.CreateICmpUGT(rnumFlags, val_nan);
        llvm::Value* cmpand = builder.CreateAnd(cmp1, cmp2);
        builder.CreateCondBr(cmpand, bi_numeric_avg, org_numeric_avg);

        builder.SetInsertPoint(bi_numeric_avg);
        /* ctl.store_pos = cell->m_val[idx].val */
        Vals2[0] = int32_0;
        Vals2[1] = int32_0;
        llvm::Value* store_pos_val = builder.CreateInBoundsGEP(bictlval, Vals2);
        builder.CreateStore(realdata, store_pos_val);
        /* check numeric is BI128 */
        oparg1 = builder.CreateICmpEQ(lnumFlags, val_binum128);
        oparg2 = builder.CreateICmpEQ(rnumFlags, val_binum128);
        llvm::Value* cmpor = builder.CreateOr(oparg1, oparg2);
        builder.CreateCondBr(cmpor, bi128_add_bi128, bi64_add_bi64);

        builder.SetInsertPoint(bi64_add_bi64);
        func_bi64addbi64 = llvmCodeGen->module()->getFunction("Jitted_bi64add64");
        if (NULL == func_bi64addbi64) {
            func_bi64addbi64 = bi64add64_codegen(true);
        }
        builder.CreateCall(func_bi64addbi64, {leftarg, rightarg, bictlval});
        tmpval = builder.CreateLoad(int64Type, store_pos_val);
        builder.CreateStore(tmpval, dataptr);
        builder.CreateBr(eval_count);

        builder.SetInsertPoint(bi128_add_bi128);
        oparg1 = builder.CreateZExt(oparg1, int32Type);
        oparg2 = builder.CreateZExt(oparg2, int32Type);
        WrapBiAggAddMatrixCodeGen(&builder, oparg1, oparg2, leftarg, rightarg, bictlval);
        tmpval = builder.CreateLoad(int64Type, store_pos_val);
        builder.CreateStore(tmpval, dataptr);
        builder.CreateBr(eval_count);

        /* call numeric add */
        builder.SetInsertPoint(org_numeric_avg);
        WrapcallscnumaddCodeGen(&builder, sdata, leftarg, rightarg, loc);
        builder.CreateBr(eval_count);

        builder.SetInsertPoint(eval_count);
        /* cell->m_val[idx+1].val++ */
        tmpval = builder.CreateLoad(int64Type, cntptr, "count");
        tmpval = builder.CreateAdd(tmpval, int64_1);
        builder.CreateStore(tmpval, cntptr);
        builder.CreateBr(agg_end);
    } else {
        /*
         * construct an array, with the first element be the count
         * and the second element be the real value.
         */
        WrapDeconstructArrayCodeGen(&builder, pval, datumarr, ndatum);

        /* calculate sum : sum = sum + num */
        /* corresponding to leftarg = (Numeric)(leftdata[0]) */
        realdata = builder.CreateLoad(int64Type, dataptr, "cell_val");
        leftarg = builder.CreateIntToPtr(realdata, numericPtrType);

        /* corresponding to rightarg = DatumGetBINumeric(datumarray[1]) */
        llvm::Value* tmpdarr = builder.CreateLoad(int64PtrType, datumarr, "datumarr");
        tmpval = builder.CreateInBoundsGEP(tmpdarr, int64_1);
        tmpval = builder.CreateLoad(int64Type, tmpval, "datumarr1");
        rightarg = DatumGetBINumericCodeGen(&builder, tmpval);

        /* get numFlags : NUMERIC_NB_FLAGBITS(arg) */
        Vals4[0] = int64_0;
        Vals4[1] = int32_1;
        Vals4[2] = int32_0;
        Vals4[3] = int32_0;
        tmpval = builder.CreateInBoundsGEP(leftarg, Vals4);
        tmpval = builder.CreateLoad(int16Type, tmpval, "lheader");
        lnumFlags = builder.CreateAnd(tmpval, val_mask);

        tmpval = builder.CreateInBoundsGEP(rightarg, Vals4);
        tmpval = builder.CreateLoad(int16Type, tmpval, "rheader");
        rnumFlags = builder.CreateAnd(tmpval, val_mask);

        /* check : NUMERIC_FLAG_IS_BI(numFlags) */
        llvm::Value* cmp1 = builder.CreateICmpUGT(lnumFlags, val_nan);
        llvm::Value* cmp2 = builder.CreateICmpUGT(rnumFlags, val_nan);
        llvm::Value* cmpand = builder.CreateAnd(cmp1, cmp2);
        builder.CreateCondBr(cmpand, bi_numeric_avg, org_numeric_avg);

        builder.SetInsertPoint(bi_numeric_avg);

        Vals2[0] = int32_0;
        Vals2[1] = int32_0;
        llvm::Value* store_pos_val = builder.CreateInBoundsGEP(bictlval, Vals2);
        builder.CreateStore(realdata, store_pos_val);
        /* check numeric is BI128 */
        oparg1 = builder.CreateICmpEQ(lnumFlags, val_binum128);
        oparg2 = builder.CreateICmpEQ(rnumFlags, val_binum128);
        llvm::Value* cmpor = builder.CreateOr(oparg1, oparg2);
        builder.CreateCondBr(cmpor, bi128_add_bi128, bi64_add_bi64);

        builder.SetInsertPoint(bi64_add_bi64);
        func_bi64addbi64 = llvmCodeGen->module()->getFunction("Jitted_bi64add64");
        if (NULL == func_bi64addbi64) {
            func_bi64addbi64 = bi64add64_codegen(true);
        }
        builder.CreateCall(func_bi64addbi64, {leftarg, rightarg, bictlval});
        tmpval = builder.CreateLoad(int64Type, store_pos_val);
        builder.CreateStore(tmpval, dataptr);
        builder.CreateBr(eval_count);

        builder.SetInsertPoint(bi128_add_bi128);
        oparg1 = builder.CreateZExt(oparg1, int32Type);
        oparg2 = builder.CreateZExt(oparg2, int32Type);
        WrapBiAggAddMatrixCodeGen(&builder, oparg1, oparg2, leftarg, rightarg, bictlval);
        tmpval = builder.CreateLoad(int64Type, store_pos_val);
        builder.CreateStore(tmpval, dataptr);
        builder.CreateBr(eval_count);

        /* call numeric add */
        builder.SetInsertPoint(org_numeric_avg);
        WrapcallscnumaddCodeGen(&builder, sdata, leftarg, rightarg, loc);
        builder.CreateBr(eval_count);

        builder.SetInsertPoint(eval_count);
        /* calculate count: count = oldcount + count, the count num can be stored by int64:
         * *countdata = *countdata + NUMERIC_64VALUE((Numeric)datumarray[0])
         */
        llvm::Value* cntdata = builder.CreateLoad(int64Type, cntptr, "cntdata");

        tmpval = builder.CreateLoad(int64Type, tmpdarr, "datumarr0");
        tmpval = builder.CreateIntToPtr(tmpval, numericPtrType);
        Vals4[0] = int64_0;
        Vals4[1] = int32_1;
        Vals4[2] = int32_0;
        Vals4[3] = int32_1;
        tmpval = builder.CreateInBoundsGEP(tmpval, Vals4);
        tmpval = builder.CreateBitCast(tmpval, int64PtrType);
        llvm::Value* tmplarg = builder.CreateLoad(int64Type, tmpval, "tmp_rarg");
        cntdata = builder.CreateAdd(cntdata, tmplarg);
        builder.CreateStore(cntdata, cntptr);
        builder.CreateBr(agg_end);
    }

    builder.SetInsertPoint(agg_end);
    builder.CreateRetVoid();

    return jitted_vsnumericavg;
}

/**
 * @Description	: Codegeneration of func vector_count.
 * @in aggref	: Aggref information about each agg function, which is
 *				  used to get the isTransition info.
 * @return		: The LLVM function that respresents the vector_count
 *				  function in LLVM assemble with respect to one row.
 */
llvm::Function* vec_count_codegen(Aggref* aggref)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    /* Define the datatype and variables that needed */
    DEFINE_CG_VOIDTYPE(voidType);
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(hashCellPtrType, "struct.hashCell");

    DEFINE_CGVAR_INT8(int8_0, 0);
    DEFINE_CGVAR_INT8(int8_1, 1);
    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT64(int64_0, 0);
    DEFINE_CGVAR_INT64(int64_1, 1);

    llvm::Value* llvmargs[3];
    llvm::Value* tmpval = NULL;
    llvm::Value* cellval = NULL;
    llvm::Value* cellflag = NULL;
    char* Jittedname = NULL;

    /* Define Const arrays */
    llvm::Value* Vals4[4] = {int64_0, int32_0, int32_0, int32_0};

    /* Define llvm function */
    if (aggref->aggstage == 0)
        Jittedname = "Jitted_count_0";
    else
        Jittedname = "Jitted_count_1";

    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, Jittedname, voidType);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("hashcell", hashCellPtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("aggidx", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("pval", int64Type));
    llvm::Function* jitted_count = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* cell = llvmargs[0];
    llvm::Value* aggidx = llvmargs[1];
    llvm::Value* pval = llvmargs[2];

    llvm::BasicBlock* entry = &jitted_count->getEntryBlock();
    DEFINE_BLOCK(agg_then, jitted_count);
    DEFINE_BLOCK(agg_else, jitted_count);
    DEFINE_BLOCK(agg_end, jitted_count);

    builder.SetInsertPoint(entry);

    /* get the hash val : hashCell->m_val[aggidx].val */
    Vals4[0] = int64_0;
    Vals4[1] = int32_1;
    Vals4[2] = aggidx;
    Vals4[3] = int32_0;
    cellval = builder.CreateInBoundsGEP(cell, Vals4);

    /* get the flag of the hash cell and check if it is NULL */
    Vals4[3] = int32_1;
    cellflag = builder.CreateInBoundsGEP(cell, Vals4);

    tmpval = builder.CreateLoad(int8Type, cellflag, "cellflag");
    tmpval = builder.CreateAnd(tmpval, int8_1);
    tmpval = builder.CreateICmpEQ(tmpval, int8_1);
    builder.CreateCondBr(tmpval, agg_else, agg_then);

    /* cell flag is null */
    builder.SetInsertPoint(agg_else);
    if (aggref->aggstage == 0) {
        builder.CreateStore(int64_1, cellval);
    } else {
        builder.CreateStore(pval, cellval);
    }
    builder.CreateStore(int8_0, cellflag);
    builder.CreateBr(agg_end);

    /* cell flag is not null */
    builder.SetInsertPoint(agg_then);
    if (aggref->aggstage == 0) {
        tmpval = builder.CreateLoad(int64Type, cellval);
        tmpval = builder.CreateAdd(tmpval, int64_1);
        builder.CreateStore(tmpval, cellval);
    } else {
        tmpval = builder.CreateLoad(int64Type, cellval);
        tmpval = builder.CreateAdd(tmpval, pval);
        builder.CreateStore(tmpval, cellval);
    }
    builder.CreateBr(agg_end);

    builder.SetInsertPoint(agg_end);
    builder.CreateRetVoid();

    llvmCodeGen->FinalizeFunction(jitted_count);

    return jitted_count;
}

/**
 * @Description	: Codegeneration of func vsonic_count, which is used
 *				  for sonic hash case.
 * @in aggref		: Aggref information about each agg function, which is
 *				  used to get the isTransition info.
 * @return		: The LLVM function that respresents the vsonic_count
 *				  function in LLVM assemble with respect to one row.
 */
llvm::Function* vsonic_count_codegen(Aggref* aggref)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    /* Define the datatype and variables that needed */
    DEFINE_CG_VOIDTYPE(voidType);
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(int64PtrType, INT8OID);
    DEFINE_CG_PTRTYPE(sonicDatumArrayPtrType, "class.SonicDatumArray");
    DEFINE_CG_PTRTYPE(atomPtrType, "struct.atom");
    DEFINE_CG_PTRPTRTYPE(atomPtrPtrType, "struct.atom");

    DEFINE_CGVAR_INT8(int8_0, 0);
    DEFINE_CGVAR_INT8(int8_1, 1);
    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT32(int32_pos_sdarray_nbit, pos_sdarray_nbit);
    DEFINE_CGVAR_INT32(int32_pos_sdarray_atomsize, pos_sdarray_atomsize);
    DEFINE_CGVAR_INT32(int32_pos_sdarray_arr, pos_sdarray_arr);
    DEFINE_CGVAR_INT32(int32_pos_atom_data, pos_atom_data);
    DEFINE_CGVAR_INT32(int32_pos_atom_nullflag, pos_atom_nullflag);
    DEFINE_CGVAR_INT64(int64_0, 0);
    DEFINE_CGVAR_INT64(int64_1, 1);

    llvm::Value* llvmargs[3];
    llvm::Value* tmpval = NULL;
    char* Jittedname = NULL;

    /* Define Const arrays */
    llvm::Value* Vals2[2] = {int64_0, int32_0};

    /* Define llvm function */
    if (aggref->aggstage == 0)
        Jittedname = "Jitted_scount_0";
    else
        Jittedname = "Jitted_scount_1";

    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, Jittedname, voidType);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("sonicdata", sonicDatumArrayPtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("loc", int32Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("pval", int64Type));
    llvm::Function* jitted_scount = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* sdata = llvmargs[0];
    llvm::Value* locidx = llvmargs[1];
    llvm::Value* pval = llvmargs[2];

    llvm::BasicBlock* entry = &jitted_scount->getEntryBlock();
    DEFINE_BLOCK(agg_then, jitted_scount);
    DEFINE_BLOCK(agg_else, jitted_scount);
    DEFINE_BLOCK(agg_end, jitted_scount);

    builder.SetInsertPoint(entry);

    /* get the nth flag of sonicdata : first calculate arrIdx and atomIdx */
    Vals2[1] = int32_pos_sdarray_nbit;
    llvm::Value* nbit = builder.CreateInBoundsGEP(sdata, Vals2);
    nbit = builder.CreateLoad(int32Type, nbit, "arridx");
    llvm::Value* arrIdx = builder.CreateLShr(locidx, nbit);
    arrIdx = builder.CreateSExt(arrIdx, int64Type);

    Vals2[1] = int32_pos_sdarray_atomsize;
    llvm::Value* atomsize = builder.CreateInBoundsGEP(sdata, Vals2);
    atomsize = builder.CreateLoad(int32Type, atomsize, "atomsize");
    atomsize = builder.CreateSub(atomsize, int32_1);
    llvm::Value* atomIdx = builder.CreateAnd(atomsize, locidx);
    atomIdx = builder.CreateSExt(atomIdx, int64Type);

    /* sdata->m_arr[arrIdx] */
    Vals2[1] = int32_pos_sdarray_arr;
    llvm::Value* atom = builder.CreateInBoundsGEP(sdata, Vals2);
    atom = builder.CreateLoad(atomPtrPtrType, atom, "atomptr");
    atom = builder.CreateInBoundsGEP(atom, arrIdx);
    atom = builder.CreateLoad(atomPtrType, atom, "atom");

    /* get address of sdata->m_arr[arrIdx]->data[atomIdx] */
    Vals2[1] = int32_pos_atom_data;
    llvm::Value* data = builder.CreateInBoundsGEP(atom, Vals2);
    data = builder.CreateLoad(int8PtrType, data, "data");
    data = builder.CreateBitCast(data, int64PtrType);
    llvm::Value* dataaddr = builder.CreateInBoundsGEP(data, atomIdx);

    /* sdata->m_arr[arrIdx]->nullFlag[atomIdx] */
    Vals2[1] = int32_pos_atom_nullflag;
    llvm::Value* nullflag = builder.CreateInBoundsGEP(atom, Vals2);
    nullflag = builder.CreateLoad(int8PtrType, nullflag, "nullflagptr");
    nullflag = builder.CreateInBoundsGEP(nullflag, atomIdx);

    /* check if flag is NULL */
    tmpval = builder.CreateLoad(int8Type, nullflag, "nullflag");
    tmpval = builder.CreateAnd(tmpval, int8_1);
    tmpval = builder.CreateICmpEQ(tmpval, int8_1);
    builder.CreateCondBr(tmpval, agg_else, agg_then);

    /* sonic data flag is null */
    builder.SetInsertPoint(agg_else);
    if (aggref->aggstage == 0) {
        builder.CreateStore(int64_1, dataaddr);
    } else {
        builder.CreateStore(pval, dataaddr);
    }
    builder.CreateStore(int8_0, nullflag);
    builder.CreateBr(agg_end);

    /* sonic data flag is not null */
    builder.SetInsertPoint(agg_then);
    if (aggref->aggstage == 0) {
        tmpval = builder.CreateLoad(int64Type, dataaddr, "dataval");
        tmpval = builder.CreateAdd(tmpval, int64_1);
        builder.CreateStore(tmpval, dataaddr);
    } else {
        tmpval = builder.CreateLoad(int64Type, dataaddr, "dataval");
        tmpval = builder.CreateAdd(tmpval, pval);
        builder.CreateStore(tmpval, dataaddr);
    }
    builder.CreateBr(agg_end);

    builder.SetInsertPoint(agg_end);
    builder.CreateRetVoid();

    llvmCodeGen->FinalizeFunction(jitted_scount);

    return jitted_scount;
}

/**
 * @Description	: Codegeneration of func DatumGetBINumeric, since column
 * 				  numeric is unlikely a short-header type, we only codegen
 * 				  the upper part. If column numeric is a short-header type,
 *				  call the C-function.
 * @in ptrbuilder	: LLVM Builder associated with the current module.
 * @in pval		: Input numeric data
 */
llvm::Value* DatumGetBINumericCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* pval)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    llvm::Function* jitted_datbinum = NULL;

    jitted_datbinum = llvmCodeGen->module()->getFunction("Jitted_datbinum");
    if (NULL == jitted_datbinum) {
        DEFINE_CG_TYPE(int8Type, CHAROID);
        DEFINE_CG_TYPE(int64Type, INT8OID);
        DEFINE_CG_PTRTYPE(numericPtrType, "struct.NumericData");
        DEFINE_CG_PTRTYPE(varattrib_1bPtrType, "struct.varattrib_1b");

        DEFINE_CGVAR_INT8(val_mask, 0x01);
        DEFINE_CGVAR_INT8(int8_0, 0);
        DEFINE_CGVAR_INT32(int32_0, 0);
        DEFINE_CGVAR_INT64(int64_0, 0);
        llvm::Value* Vals[2] = {int64_0, int32_0};

        llvm::Value* llvmargs[1] = {NULL};
        llvm::Value* tmpval = NULL;
        llvm::Value* res1 = NULL;
        llvm::Value* res2 = NULL;

        /* Define llvm function */
        GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_datbinum", numericPtrType);
        fn_prototype.addArgument(GsCodeGen::NamedVariable("datum", int64Type));
        jitted_datbinum = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

        llvm::Value* arg = llvmargs[0];

        DEFINE_BLOCK(numeric_short, jitted_datbinum);
        DEFINE_BLOCK(numeric_not_short, jitted_datbinum);
        DEFINE_BLOCK(ret_bb, jitted_datbinum);

        /* convert to varatt_1b type */
        tmpval = builder.CreateIntToPtr(arg, varattrib_1bPtrType);
        tmpval = builder.CreateInBoundsGEP(tmpval, Vals);
        tmpval = builder.CreateLoad(int8Type, tmpval);
        tmpval = builder.CreateAnd(tmpval, val_mask);

        /* check VARATT_IS_SHORT(num) is true or not */
        llvm::Value* cmp = builder.CreateICmpEQ(tmpval, int8_0);
        builder.CreateCondBr(cmp, numeric_not_short, numeric_short);

        /* return (Numeric)num directly */
        builder.SetInsertPoint(numeric_not_short);
        res1 = builder.CreateIntToPtr(arg, numericPtrType);
        builder.CreateBr(ret_bb);

        /*
         * unlikely this is a short-header varlena --- convert to 4-byte header format
         */
        builder.SetInsertPoint(numeric_short);
        res2 = DatumGetBINumericCodeGenShort(&builder, arg);
        builder.CreateBr(ret_bb);

        builder.SetInsertPoint(ret_bb);
        llvm::PHINode* phi_ret = builder.CreatePHI(numericPtrType, 2);
        phi_ret->addIncoming(res1, numeric_not_short);
        phi_ret->addIncoming(res2, numeric_short);
        builder.CreateRet(phi_ret);

        llvmCodeGen->FinalizeFunction(jitted_datbinum);
    }

    llvm::Value* result = ptrbuilder->CreateCall(jitted_datbinum, pval);

    return result;
}

/**
 * @Description : The interface between LLVM assemble and C-function with respect
 *				  to DatumGetBINumericShort. They have the same input arguments
 *				  and output result
 */
llvm::Value* DatumGetBINumericCodeGenShort(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* pval)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::Value* result = NULL;

    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(numericPtrType, "struct.NumericData");

    llvm::Function* jitted_datbinums = llvmCodeGen->module()->getFunction("LLVMWrapDatGetBiNumSht");
    if (jitted_datbinums == NULL) {
        GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMWrapDatGetBiNumSht", numericPtrType);
        fn_prototype.addArgument(GsCodeGen::NamedVariable("datum", int64Type));
        jitted_datbinums = fn_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMWrapDatGetBiNumSht", (void*)DatumGetBINumericShort);
    }

    llvmCodeGen->FinalizeFunction(jitted_datbinums);

    result = ptrbuilder->CreateCall(jitted_datbinums, pval);
    return result;
}

/**
 * @Description	: The interface between LLVM assemble and C-function which
 * 				  is used to call addVariable in LLVM. They have the same
 *				  input arguments and output result.
 */
llvm::Value* WrapaddVariableCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* ccontext, llvm::Value* val)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::Value* result = NULL;

    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(MemCxtDataType, "struct.MemoryContextData");

    llvm::Function* jitted_addvariable = llvmCodeGen->module()->getFunction("LLVMWrapaddVariable");
    if (jitted_addvariable == NULL) {
        GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMWrapaddVariable", int64Type);
        fn_prototype.addArgument(GsCodeGen::NamedVariable("memorycontext", MemCxtDataType));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("value", int64Type));
        jitted_addvariable = fn_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMWrapaddVariable", (void*)addVariable);
    }

    llvmCodeGen->FinalizeFunction(jitted_addvariable);

    result = ptrbuilder->CreateCall(jitted_addvariable, {ccontext, val});
    return result;
}

/**
 * @Description	: The interface between LLVM assemble and C-function which
 * 				  is used to call replaceVariable in LLVM. They have the same
 *				  input arguments and output result.
 */
llvm::Value* WrapreplVariableCodeGen(
    GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* ccontext, llvm::Value* oldval, llvm::Value* val)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::Value* result = NULL;

    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(MemCxtDataType, "struct.MemoryContextData");

    llvm::Function* jitted_repvariable = llvmCodeGen->module()->getFunction("LLVMWrapreplVariable");

    if (jitted_repvariable == NULL) {
        GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMWrapreplVariable", int64Type);
        fn_prototype.addArgument(GsCodeGen::NamedVariable("memorycontext", MemCxtDataType));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("value", int64Type));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("value", int64Type));
        jitted_repvariable = fn_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMWrapreplVariable", (void*)replaceVariable);
    }

    llvmCodeGen->FinalizeFunction(jitted_repvariable);

    result = ptrbuilder->CreateCall(jitted_repvariable, {ccontext, oldval, val});
    return result;
}

/**
 * @Description	: The interface between LLVM assemble and C-function which
 * 				  is used to call makeNumeric64 in LLVM. They have the same
 *				  input arguments and output result.
 */
llvm::Value* WrapmakeNumeric64CodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* data, llvm::Value* scale)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::Value* result = NULL;

    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CGVAR_INT64(int64_0, 0);

    llvm::Function* jitted_make64num = llvmCodeGen->module()->getFunction("LLVMWrapMakeNumeric64");
    if (jitted_make64num == NULL) {
        GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMWrapMakeNumeric64", int64Type);
        fn_prototype.addArgument(GsCodeGen::NamedVariable("value", int64Type));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("scale", int8Type));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("arr", int64Type));
        jitted_make64num = fn_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMWrapMakeNumeric64", (void*)makeNumeric64);
    }

    llvmCodeGen->FinalizeFunction(jitted_make64num);

    result = ptrbuilder->CreateCall(jitted_make64num, {data, scale, int64_0});
    return result;
}

/**
 * @Description	: The interface between LLVM assemble and C-function which
 * 				  is used to call makeNumeric128 in LLVM. They have the same
 *				  input arguments and output result.
 */
llvm::Value* WrapmakeNumeric128CodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* data, llvm::Value* scale)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    llvm::Value* result = NULL;

    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_NINTTYP(int128Type, 128);
    DEFINE_CGVAR_INT64(int64_0, 0);

    llvm::Function* jitted_make128num = llvmCodeGen->module()->getFunction("LLVMWrapMakeNumeric128");
    if (jitted_make128num == NULL) {
        GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMWrapMakeNumeric128", int64Type);
        fn_prototype.addArgument(GsCodeGen::NamedVariable("value", int128Type));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("scale", int8Type));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("arr", int64Type));
        jitted_make128num = fn_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMWrapMakeNumeric128", (void*)makeNumeric128);
    }

    llvmCodeGen->FinalizeFunction(jitted_make128num);

    result = ptrbuilder->CreateCall(jitted_make128num, {data, scale, int64_0});
    return result;
}

/**
 * @Description	: The interface between LLVM assemble and C-function with respect
 * 				  to WrapBiAggAddMatrix. They have the same input arguments.
 */
void WrapBiAggAddMatrixCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* arg1, llvm::Value* arg2,
    llvm::Value* leftarg, llvm::Value* rightarg, llvm::Value* ctl)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_PTRTYPE(bictlPtrType, "struct.bictl");
    DEFINE_CG_PTRTYPE(numericPtrType, "struct.NumericData");
    llvm::Type* VoidType = llvm::Type::getVoidTy(context);

    llvm::Function* jitted_biaggaddfunc = llvmCodeGen->module()->getFunction("LLVMWrapBiAggAdd");
    if (jitted_biaggaddfunc == NULL) {
        GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMWrapBiAggAdd", VoidType);
        fn_prototype.addArgument(GsCodeGen::NamedVariable("arg1", int32Type));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("arg2", int32Type));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("leftarg", numericPtrType));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("rightarg", numericPtrType));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("bictl", bictlPtrType));
        jitted_biaggaddfunc = fn_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMWrapBiAggAdd", (void*)WrapBiAggAddMatrix);
    }

    llvmCodeGen->FinalizeFunction(jitted_biaggaddfunc);

    ptrbuilder->CreateCall(jitted_biaggaddfunc, {arg1, arg2, leftarg, rightarg, ctl});
}

/**
 * @Description	: The interface between LLVM assemble and C-function with respect
 * 				  to WrapBiFunMatrix. They have the same input arguments and
 *				  output result.
 */
llvm::Value* WrapBiFunMatrixCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* op, llvm::Value* arg1,
    llvm::Value* arg2, llvm::Value* leftarg, llvm::Value* rightarg)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(numericPtrType, "struct.NumericData");

    llvm::Function* jitted_bifunmat = llvmCodeGen->module()->getFunction("LLVMWrapBiFunMatrix");
    if (jitted_bifunmat == NULL) {
        GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMWrapBiFunMatrix", int64Type);
        fn_prototype.addArgument(GsCodeGen::NamedVariable("op", int32Type));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("arg1", int32Type));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("arg2", int32Type));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("leftarg", numericPtrType));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("rightarg", numericPtrType));
        jitted_bifunmat = fn_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMWrapBiFunMatrix", (void*)WrapBiFunMatrix);
    }

    llvmCodeGen->FinalizeFunction(jitted_bifunmat);

    return ptrbuilder->CreateCall(jitted_bifunmat, {op, arg1, arg2, leftarg, rightarg});
}

/**
 * @Description	: The interface between LLVM assemble and C-function with respect
 * 				  to WrapDeconstructArray. They have the same input arguments.
 */
void WrapDeconstructArrayCodeGen(
    GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* pval, llvm::Value* datumarray, llvm::Value* ndatum)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    DEFINE_CG_VOIDTYPE(voidType);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int32PtrType, INT4OID);
    DEFINE_CG_PTRTYPE(int64PtrType, INT8OID);
    llvm::Type* int64PPtrType = llvmCodeGen->getPtrType(int64PtrType);

    llvm::Function* jitted_contarray = llvmCodeGen->module()->getFunction("LLVMWrapContArray");
    if (jitted_contarray == NULL) {
        GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMWrapContArray", voidType);
        fn_prototype.addArgument(GsCodeGen::NamedVariable("pval", int64Type));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("datumarray", int64PPtrType));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("ndatum", int32PtrType));
        jitted_contarray = fn_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMWrapContArray", (void*)WrapDeconstructArray);
    }

    llvmCodeGen->FinalizeFunction(jitted_contarray);

    ptrbuilder->CreateCall(jitted_contarray, {pval, datumarray, ndatum});
}

/**
 * @Description	: The interface between LLVM assemble and C-function with respect
 * 				  to Wrapcallnumericadd. They have the same input arguments.
 */
void WrapcallnumericaddCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* cell, llvm::Value* mct,
    llvm::Value* larg, llvm::Value* rarg, llvm::Value* nidx)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    DEFINE_CG_VOIDTYPE(voidType);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_PTRTYPE(numericPtrType, "struct.NumericData");
    DEFINE_CG_PTRTYPE(hashCellPtrType, "struct.hashCell");
    DEFINE_CG_PTRTYPE(MemCxtDataType, "struct.MemoryContextData");

    llvm::Function* jitted_callnumadd = llvmCodeGen->module()->getFunction("LLVMWrapcallnumadd");
    if (jitted_callnumadd == NULL) {
        GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMWrapcallnumadd", voidType);
        fn_prototype.addArgument(GsCodeGen::NamedVariable("cell", hashCellPtrType));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("mcontext", MemCxtDataType));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("larg", numericPtrType));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("rarg", numericPtrType));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("idx", int32Type));
        jitted_callnumadd = fn_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMWrapcallnumadd", (void*)Wrapcallnumericadd);
    }

    llvmCodeGen->FinalizeFunction(jitted_callnumadd);

    ptrbuilder->CreateCall(jitted_callnumadd, {cell, mct, larg, rarg, nidx});
}

/**
 * @Description	: The interface between LLVM assemble and C-function with respect
 * 				  to Wrapcallnumericadd. They have the same input arguments.
 */
void WrapcallscnumaddCodeGen(
    GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* data, llvm::Value* larg, llvm::Value* rarg, llvm::Value* loc)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    DEFINE_CG_VOIDTYPE(voidType);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_PTRTYPE(numericPtrType, "struct.NumericData");
    DEFINE_CG_PTRTYPE(sonicEncodingDatumArrayPtrType, "class.SonicEncodingDatumArray");

    llvm::Function* jitted_callsnumadd = llvmCodeGen->module()->getFunction("LLVMWrapcallsonicnumadd");
    if (jitted_callsnumadd == NULL) {
        GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMWrapcallsonicnumadd", voidType);
        fn_prototype.addArgument(GsCodeGen::NamedVariable("data", sonicEncodingDatumArrayPtrType));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("larg", numericPtrType));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("rarg", numericPtrType));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("loc", int32Type));
        jitted_callsnumadd = fn_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMWrapcallsonicnumadd", (void*)Wrapcallsonicnumericadd);
    }

    llvmCodeGen->FinalizeFunction(jitted_callsnumadd);

    ptrbuilder->CreateCall(jitted_callsnumadd, {data, larg, rarg, loc});
}
}  // namespace dorado

/**
 * @Description	: Wrap the call of BiAggFunMatrix with the operation fixed as BI_AGG_ADD.
 * @in arg1		: flag used to mask if leftarg is bi128 or not.
 * @in arg2		: flag used to mask if rightarg is bi128 or not.
 * @in leftarg	: The left argument.
 * @in rightarg	: The right argument.
 * @in ctl		: BICTL data struct used to store the result.
 */
void WrapBiAggAddMatrix(int arg1, int arg2, Numeric leftarg, Numeric rightarg, bictl* ctl)
{
    (BiAggFunMatrix[BI_AGG_ADD][arg1][arg2])(leftarg, rightarg, ctl);
}

/**
 * @Description	: Wrap the call of BiFunMatrix with the bictl always be NULL.
 * @in op		: The exact operation that we need to call.
 * @in arg1		: flag used to mask if leftarg is bi128 or not.
 * @in arg2		: flag used to mask if rightarg is bi128 or not.
 * @in leftarg	: The left argument.
 * @in rightarg	: The right argument.
 * @return		: The result with respect to op operation.
 */
Datum WrapBiFunMatrix(biop op, int arg1, int arg2, Numeric leftarg, Numeric rightarg)
{
    Datum result;
    result = (BiFunMatrix[op][arg1][arg2])(leftarg, rightarg, NULL);

    return result;
}

/**
 * @Description	: Wrap simple method for extracting data from an array, only have
 * 				  three input parameters, because other parameters are fixed in
 *				  our case.
 * @in pval		: Value used to define the array type.
 * @in datumarray	: Array pointer used to store values.
 * @in ndatum	: Dimension of array
 */
void WrapDeconstructArray(ScalarValue pval, Datum** datumarray, int* ndatum)
{
    deconstruct_array(DatumGetArrayTypeP(pval), NUMERICOID, -1, false, 'i', datumarray, NULL, ndatum);
}

/**
 * @Description	: Evalate the sum of two original numeric data and copy the result
 * 				  the hash cell.
 * @in cell		: The hash cell needed to store the result.
 * @in tbl		: The hash table needed to call CopyVarP function.
 * @in leftarg	: The left numeric data
 * @in rightarg	: The right numeric data
 * @in idx		: The agg index.
 */
void Wrapcallnumericadd(hashCell* cell, MemoryContext mcontext, Numeric leftarg, Numeric rightarg, int idx)
{
    Datum args[2];
    Datum result;
    FunctionCallInfoData finfo;
    finfo.arg = &args[0];

    args[0] = NumericGetDatum(leftarg);
    args[1] = NumericGetDatum(rightarg);
    result = numeric_add(&finfo);
    cell->m_val[idx].val = replaceVariable(mcontext, cell->m_val[idx].val, result);
}

/**
 * @Description	: Evalate the sum of two original numeric data and put the value
 *				  in sonic datum arry.
 * @in data		: Sonic datum array information.
 * @in leftarg		: The left numeric data
 * @in rightarg	: The right numeric data
 * @in loc		: The position of value in sonic datum array data.
 */
void Wrapcallsonicnumericadd(SonicEncodingDatumArray* data, Numeric leftarg, Numeric rightarg, uint32 loc)
{
    Datum args[2];
    Datum result;
    FunctionCallInfoData finfo;
    finfo.arg = &args[0];

    int arrIdx = getArrayIndx(loc, data->m_nbit);
    int atomIdx = getArrayLoc(loc, data->m_atomSize - 1);

    Datum* leftdata = &((Datum*)data->m_arr[arrIdx]->data)[atomIdx];

    args[0] = NumericGetDatum(leftarg);
    args[1] = NumericGetDatum(rightarg);
    result = numeric_add(&finfo);
    leftdata[0] = data->replaceVariable(leftdata[0], result);
}
