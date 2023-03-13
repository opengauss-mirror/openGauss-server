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
 *  vecsortcodegen.cpp
 *        Routines to handle vector sort nodes. We only focuse on
 *          the CPU intensive part of sort operation.
 * IDENTIFICATION
 *        src/gausskernel/runtime/codegen/vecexecutor/vecsortcodegen.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
 
#include "codegen/gscodegen.h"
#include "codegen/vecsortcodegen.h"
#include "codegen/codegendebuger.h"
#include "codegen/builtinscodegen.h"

#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "vecexecutor/vecexecutor.h"
#include "pgxc/pgxc.h"
#include "executor/executor.h"
#include "utils/pg_locale.h"

struct varlena* Wrap_heap_tuple_untoast_attr(struct varlena* datum);
void Wrap_pfree(char* ptr);
int Wrap_numeric_cmp(struct NumericData* datum1, struct NumericData* datum2);

namespace dorado {
/*
 * @Description	: Check if codegeneration is allowed for CompareMultiColumn
 * 				  function of Vec Sort Node.
 */
bool VecSortCodeGen::JittableCompareMultiColumn(VecSortState* node)
{
    Sort* plannode = (Sort*)node->ss.ps.plan;
    PlanState* outerNode = NULL;
    TupleDesc tupDesc;
    int i, colIdx;

    if (!u_sess->attr.attr_sql.enable_codegen || IS_PGXC_COORDINATOR)
        return false;

    /* Get the attr column according the sortindex in the targetlist */
    outerNode = outerPlanState(node);
    tupDesc = ExecGetResultType(outerNode);

    for (i = 0; i < plannode->numCols; i++) {
        colIdx = plannode->sortColIdx[i] - 1;
        Form_pg_attribute attr = &tupDesc->attrs[colIdx];
        Oid typeOid = attr->atttypid;
        Oid opno = plannode->sortOperators[i];
        switch (typeOid) {
            case INT4OID:
            case INT8OID:
            case NUMERICOID:
                break;
            case TEXTOID:
            case VARCHAROID: {
                /* To avoid the error cased by transformation from text to bpchar, we
                 * only consider textlt or textgt for text and varchar data type.
                 */
                if (opno != TEXTLTOID && opno != TEXTGTOID)
                    return false;

                /*
                 * Only support C/POSIX collation. For other case, we need
                 * to call varstr_cmp which will be considered in the future.
                 */
                if (!lc_collate_is_c(plannode->collations[i]))
                    return false;
            } break;
            case CHAROID:
            case BPCHAROID: {
                /* To avoid the error cased by transformation from bpchar to text, we
                 * only consider bpcharlt or bpchargt for char and bpchar data type.
                 */
                if (opno != BPCHARLTOID && opno != BPCHARGTOID)
                    return false;

                /* no codegen for unknown length(len = atttypmode - VARHDRSZ) */
                if (attr->atttypmod <= VARHDRSZ)
                    return false;

                /* Only support C/POSIX collation. */
                if (!lc_collate_is_c(plannode->collations[i]))
                    return false;
            } break;
            default:
                return false;
        }
    }

    return true;
}

/*
 * @Description	:Check if codegeneration is allowed for match_key function
 * 				 of SortAgg node.
 */
bool VecSortCodeGen::JittableSortAggMatchKey(VecAggState* node)
{
    VecAgg* plannode = (VecAgg*)node->ss.ps.plan;
    int i, colIdx;

    if (!u_sess->attr.attr_sql.enable_codegen || IS_PGXC_COORDINATOR)
        return false;

    /* Only consider sortagg here */
    if (plannode->aggstrategy != AGG_SORTED)
        return false;

    /* Only support group by rollup(...) related sort aggregation */
    if (plannode->groupingSets == NULL)
        return false;

    /* Do not consider multiple phases */
    if (node->numphases != 1)
        return false;

    PlanState* outerNode = outerPlanState(node);
    TupleDesc tupDesc = ExecGetResultType(outerNode);

    for (i = 0; i < plannode->numCols; i++) {
        colIdx = plannode->grpColIdx[i] - 1;
        Form_pg_attribute attr = &tupDesc->attrs[colIdx];
        Oid opoid = plannode->grpOperators[i];
        switch (opoid) {
            case INT4EQOID:
            case INT8EQOID:
            case TEXTEQOID:
                break;
            case BPCHAREQOID: {
                /* no codegen for unknown length(len = atttypmode - VARHDRSZ) */
                if (attr->atttypmod <= VARHDRSZ)
                    return false;
            } break;
            default:
                return false;
        }
    }
    return true;
}

/*
 * @Description	: Codegen on CompareMultiColumn function of Sort operator.
 * The optimization points include:
 * (1). Inline inlineApplySortFunction function;
 * (2). Loop unrolling on multiple sort keys;
 * (3). For text/varchar key, call the generated textcmp function;
 * (4). For bpchar key, call the generated bpcharcmp function;
 * (5). For numeric key, call the generated numeric_cmp function;
 * (6). For int4 and int8, inline the comparison function;
 */
llvm::Function* VecSortCodeGen::CompareMultiColumnCodeGen(VecSortState* node, bool use_prefetch)
{
    if (!JittableCompareMultiColumn(node))
        return NULL;

    Assert(NULL != (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj);
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    /* Find and load the IR file from the installaion directory */
    llvmCodeGen->loadIRFile();

    /* Get LLVM Context and builder */
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    llvm::Value* tmpval = NULL;
    llvm::Value* llvmargs[3];
    llvm::Value* parm1 = NULL;
    llvm::Value* parm2 = NULL;
    llvm::Value* parm3 = NULL;

    int i, colIdx;
    Sort* plannode = (Sort*)node->ss.ps.plan;
    PlanState* outerNode = outerPlanState(node);
    TupleDesc tupDesc = ExecGetResultType(outerNode);

    DEFINE_CG_TYPE(int1Type, BITOID);
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(int64PtrType, INT8OID);
    DEFINE_CG_PTRTYPE(MultiColumnsPtrType, "struct.MultiColumns");
    DEFINE_CG_PTRTYPE(BatchsortstatePtrType, "class.Batchsortstate");

    DEFINE_CGVAR_INT8(int8_1, 1);
    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT32(int32_3, 3);
    DEFINE_CGVAR_INT32(int32_m1, -1);
    DEFINE_CGVAR_INT64(int64_0, 0);
    llvm::Value* Vals2[2] = {int64_0, int32_0};

    /*
     * The prototype of the jitted function for CompareMultiColumn
     * Two parameters: (1) MultiColumns *a, (2) MultiColumns *b
     */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "JittedCompareMultiColumn", int32Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("a", MultiColumnsPtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("b", MultiColumnsPtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("state", BatchsortstatePtrType));
    llvm::Function* jitted_comparecol = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    parm1 = llvmargs[0];
    parm2 = llvmargs[1];
    parm3 = llvmargs[2];

    int numKeys = plannode->numCols;
    llvm::BasicBlock** bb_keys1 = (llvm::BasicBlock**)palloc(sizeof(llvm::BasicBlock*) * numKeys);
    llvm::BasicBlock** bb_keys2 = (llvm::BasicBlock**)palloc(sizeof(llvm::BasicBlock*) * numKeys);
    llvm::BasicBlock** bb_bothnotnull = (llvm::BasicBlock**)palloc(sizeof(llvm::BasicBlock*) * numKeys);
    llvm::BasicBlock** bb_checkbothnull = (llvm::BasicBlock**)palloc(sizeof(llvm::BasicBlock*) * numKeys);
    llvm::BasicBlock** bb_eithernull = (llvm::BasicBlock**)palloc(sizeof(llvm::BasicBlock*) * numKeys);

    llvm::PHINode* Phi_res = NULL;
    llvm::Value** comp_res = (llvm::Value**)palloc(sizeof(llvm::Value*) * numKeys);

    llvm::Value* nulls1 = NULL;
    llvm::Value* nulls2 = NULL;
    llvm::Value* values1 = NULL;
    llvm::Value* values2 = NULL;
    llvm::Value* result1 = NULL;
    llvm::Value* result2 = NULL;
    llvm::Value* datum1 = NULL;
    llvm::Value* datum2 = NULL;

    for (i = 0; i < numKeys; i++) {
        bb_keys1[i] = llvm::BasicBlock::Create(context, "bb_keys1", jitted_comparecol);
        bb_keys2[i] = llvm::BasicBlock::Create(context, "bb_keys2", jitted_comparecol);
        bb_bothnotnull[i] = llvm::BasicBlock::Create(context, "bb_bothnotnull", jitted_comparecol);
        bb_checkbothnull[i] = llvm::BasicBlock::Create(context, "bb_checkbothnull", jitted_comparecol);
        bb_eithernull[i] = llvm::BasicBlock::Create(context, "bb_eithernull", jitted_comparecol);
        comp_res[i] = NULL;
    }
    DEFINE_BLOCK(bb_ret, jitted_comparecol);
    llvm::BasicBlock* entry = &jitted_comparecol->getEntryBlock();

    /* Start the codegen in entry basic block */
    builder.SetInsertPoint(entry);

    /* get MultiColumns.m_values */
    tmpval = builder.CreateInBoundsGEP(parm1, Vals2);
    values1 = builder.CreateLoad(int64PtrType, tmpval, "a_values");
    tmpval = builder.CreateInBoundsGEP(parm2, Vals2);
    values2 = builder.CreateLoad(int64PtrType, tmpval, "b_values");

    /* get MultiColumns.m_nulls */
    Vals2[1] = int32_1;
    tmpval = builder.CreateInBoundsGEP(parm1, Vals2);
    nulls1 = builder.CreateLoad(int8PtrType, tmpval, "a_nulls");
    tmpval = builder.CreateInBoundsGEP(parm2, Vals2);
    nulls2 = builder.CreateLoad(int8PtrType, tmpval, "b_nulls");
    builder.CreateBr(bb_keys1[0]);

    /* Looping over sort keys for codegeneration of each key comparison */
    for (i = 0; i < numKeys; i++) {
        bool desc = false;
        bool nullsFirst = plannode->nullsFirst[i];
        Oid opno = plannode->sortOperators[i];

        colIdx = plannode->sortColIdx[i] - 1;
        Form_pg_attribute attr = &tupDesc->attrs[colIdx];
        Oid typeOid = attr->atttypid;

        builder.SetInsertPoint(bb_keys1[i]);

        /*
         * Loading the data first for prefetch, assume most data is NOT NULL.
         * MultiColumns->m_values[colIdx]
         */
        llvm::Value* colidx = llvmCodeGen->getIntConstant(INT8OID, colIdx);
        tmpval = builder.CreateInBoundsGEP(values1, colidx);
        datum1 = builder.CreateLoad(int64Type, tmpval, "a_values_i");
        tmpval = builder.CreateInBoundsGEP(values2, colidx);
        datum2 = builder.CreateLoad(int64Type, tmpval, "b_values_i");

        /*
         * For bpchar or numeric type key, the varlena datum is a pointer
         * points to the data with header. Prefetch the pointed buffer
         * may reduce the memory stall (cache miss overhead).
         */
        if (use_prefetch) {
            /* load instrinsic prefetch function */
            llvm::Function* fn_prefetch =
                llvm::Intrinsic::getDeclaration(llvmCodeGen->module(), llvm_prefetch);
            if (fn_prefetch == NULL) {
                ereport(ERROR,
                    (errcode(ERRCODE_LOAD_INTRINSIC_FUNCTION_FAILED),
                        errmodule(MOD_LLVM),
                        errmsg("Failed to get function Intrinsic::prefetch!\n")));
            }

            /* In first iteration, prefetch the first key and next key if they are varlen data
             * In following iterations, just prefetch the next key since the current key has
             * already been prefetched.
             */
            if (i == 0) {
                if (typeOid == VARCHAROID || typeOid == TEXTOID || typeOid == BPCHAROID || typeOid == NUMERICOID) {
                    /* llvm prefetch function : Prefetches have no effect on
                     * the behavior of the program but can change its performance
                     * characteristics.
                     * llvm::prefetch(int8* ptr, int32 rw, int32 local, int32 IorD)
                     */
                    tmpval = builder.CreateIntToPtr(datum1, int8PtrType);
                    builder.CreateCall(fn_prefetch, {tmpval, int32_0, int32_3, int32_1});
                    tmpval = builder.CreateIntToPtr(datum2, int8PtrType);
                    builder.CreateCall(fn_prefetch, {tmpval, int32_0, int32_3, int32_1});
                }
            }

            if (i < numKeys - 1) {
                /* prefetch the next key if it is BPCHAR or NUMERIC data */
                int next_colIdx = plannode->sortColIdx[i + 1] - 1;
                Form_pg_attribute next_attr = &tupDesc->attrs[next_colIdx];
                if (next_attr->atttypid == VARCHAROID || next_attr->atttypid == TEXTOID ||
                    next_attr->atttypid == BPCHAROID || next_attr->atttypid == NUMERICOID) {
                    llvm::Value* next_val_colIdx = llvmCodeGen->getIntConstant(INT8OID, next_colIdx);
                    tmpval = builder.CreateInBoundsGEP(values1, next_val_colIdx);
                    llvm::Value* next_datum1 = builder.CreateLoad(int64Type, tmpval, "a_values_ip1");
                    tmpval = builder.CreateInBoundsGEP(values2, next_val_colIdx);
                    llvm::Value* next_datum2 = builder.CreateLoad(int64Type, tmpval, "b_values_ip1");

                    tmpval = builder.CreateIntToPtr(next_datum1, int8PtrType);
                    builder.CreateCall(fn_prefetch, {tmpval, int32_0, int32_3, int32_1});
                    tmpval = builder.CreateIntToPtr(next_datum2, int8PtrType);
                    builder.CreateCall(fn_prefetch, {tmpval, int32_0, int32_3, int32_1});
                }
            }
        }

        /* get MultiColumns.m_nulls[colIdx] */
        tmpval = builder.CreateInBoundsGEP(nulls1, colidx);
        llvm::Value* isNull1 = builder.CreateLoad(int8Type, tmpval, "a_nulls_i");
        tmpval = builder.CreateInBoundsGEP(nulls2, colidx);
        llvm::Value* isNull2 = builder.CreateLoad(int8Type, tmpval, "b_nulls_i");
        isNull1 = builder.CreateAnd(isNull1, int8_1);
        isNull2 = builder.CreateAnd(isNull2, int8_1);
        isNull1 = builder.CreateTrunc(isNull1, int1Type);
        isNull2 = builder.CreateTrunc(isNull2, int1Type);

        /* check if both of them are not null by 'Or' operation */
        tmpval = builder.CreateOr(isNull1, isNull2);
        builder.CreateCondBr(tmpval, bb_checkbothnull[i], bb_bothnotnull[i]);

        /* if both of them are null, turn to next key */
        builder.SetInsertPoint(bb_checkbothnull[i]);
        tmpval = builder.CreateAnd(isNull1, isNull2);
        builder.CreateCondBr(tmpval, bb_keys2[i], bb_eithernull[i]);

        builder.SetInsertPoint(bb_eithernull[i]);
        if (nullsFirst) {
            result2 = builder.CreateSelect(isNull1, int32_m1, int32_1);
        } else {
            result2 = builder.CreateSelect(isNull1, int32_1, int32_m1);
        }
        builder.CreateBr(bb_keys2[i]);

        /* when both of them are not null, call compare function */
        builder.SetInsertPoint(bb_bothnotnull[i]);
        result1 = NULL;
        switch (typeOid) {
            /*
             * if operation from opno is '>', it means we have desc in SQL.
             */
            case NUMERICOID: {
                Assert(opno == NUMERICLTOID || opno == NUMERICGTOID);
                desc = (opno == NUMERICGTOID) ? true : false;

                llvm::Function* jitted_numericcmp = NULL;
                bool fastpath = false;

                /* consider use fast path if numeric data can be represented
                 * in BI64 format (precision less than 19).
                 */
                if (attr->atttypmod != -1 && ((attr->atttypmod >> 16) & 0xFFFF) <= 18)
                    fastpath = true;

                if (fastpath) {
                    jitted_numericcmp = llvmCodeGen->module()->getFunction("LLVMIRnumericcmp_fastpath");
                    if (jitted_numericcmp == NULL) {
                        jitted_numericcmp = numericcmpCodeGen_fastpath();
                    }
                } else {
                    jitted_numericcmp = llvmCodeGen->module()->getFunction("LLVMIRnumericcmp");
                    if (jitted_numericcmp == NULL) {
                        jitted_numericcmp = numericcmpCodeGen();
                    }
                }
                result1 = builder.CreateCall(jitted_numericcmp, {datum1, datum2});
            } break;
            case TEXTOID:
            case VARCHAROID: {
                Assert(opno == TEXTLTOID || opno == TEXTGTOID);
                desc = (opno == TEXTGTOID) ? true : false;

                llvm::Function* jitted_textcmp = NULL;
                jitted_textcmp = llvmCodeGen->module()->getFunction("LLVMIRtextcmp_CMC");
                if (jitted_textcmp == NULL) {
                    jitted_textcmp = textcmpCodeGen();
                }
                result1 = builder.CreateCall(jitted_textcmp, {datum1, datum2});
            } break;
            case BPCHAROID: {
                int len = attr->atttypmod - VARHDRSZ;
                Assert(opno == BPCHARLTOID || opno == BPCHARGTOID);
                desc = (opno == BPCHARGTOID) ? true : false;

                llvm::Function* jitted_bpcharcmp = NULL;
                /*
                 * When the length of bpchar is less than 2 byte, we could compare
                 * them in fast path, so make a difference here.
                 */
                if (len > 16) {
                    jitted_bpcharcmp = llvmCodeGen->module()->getFunction("LLVMIRbpcharcmp_long");
                    if (jitted_bpcharcmp == NULL) {
                        jitted_bpcharcmp = bpcharcmpCodeGen_long();
                    }
                } else {
                    jitted_bpcharcmp = llvmCodeGen->module()->getFunction("LLVMIRbpcharcmp_short");
                    if (jitted_bpcharcmp == NULL) {
                        jitted_bpcharcmp = bpcharcmpCodeGen_short();
                    }
                }
                llvm::Value* len_value = llvmCodeGen->getIntConstant(INT4OID, len);
                result1 = builder.CreateCall(jitted_bpcharcmp, {datum1, datum2, len_value});
            } break;
            case INT4OID: {
                Assert(opno == INT4LTOID || opno == INT4GTOID);
                desc = (opno == INT4GTOID) ? true : false;

                datum1 = builder.CreateTrunc(datum1, int32Type);
                datum2 = builder.CreateTrunc(datum2, int32Type);
                tmpval = builder.CreateICmpEQ(datum1, datum2);
                result1 = builder.CreateICmpSGT(datum1, datum2);
                result1 = builder.CreateSelect(result1, int32_1, int32_m1);
                result1 = builder.CreateSelect(tmpval, int32_0, result1);
            } break;
            case INT8OID: {
                Assert(opno == INT8LTOID || opno == INT8GTOID);
                desc = (opno == INT8GTOID) ? true : false;

                tmpval = builder.CreateICmpEQ(datum1, datum2);
                result1 = builder.CreateICmpSGT(datum1, datum2);
                result1 = builder.CreateSelect(result1, int32_1, int32_m1);
                result1 = builder.CreateSelect(tmpval, int32_0, result1);
            } break;
            default:
                break;
        }

        /*
         * Descending is only applied for both NOT NULL data, it cannot be
         * applied to any NULL data. When desc is true, reversal the result.
         */
        if (desc) {
            result1 = builder.CreateSub(int32_0, result1);
        }
        builder.CreateBr(bb_keys2[i]);

        builder.SetInsertPoint(bb_keys2[i]);
        Phi_res = builder.CreatePHI(int32Type, 3);
        Phi_res->addIncoming(result1, bb_bothnotnull[i]);
        Phi_res->addIncoming(int32_0, bb_checkbothnull[i]);
        Phi_res->addIncoming(result2, bb_eithernull[i]);
        comp_res[i] = Phi_res;

        if (i == numKeys - 1)
            builder.CreateBr(bb_ret);
        else {
            /* if compare value is zero, turn to next sortkey, else end */
            tmpval = builder.CreateICmpEQ(Phi_res, int32_0);
            builder.CreateCondBr(tmpval, bb_keys1[i + 1], bb_ret);
        }
    }

    builder.SetInsertPoint(bb_ret);
    llvm::PHINode* Phi_compare = builder.CreatePHI(int32Type, numKeys);
    for (i = 0; i < numKeys; i++) {
        Phi_compare->addIncoming(comp_res[i], bb_keys2[i]);
    }
    builder.CreateRet(Phi_compare);

    pfree_ext(bb_keys1);
    pfree_ext(bb_keys2);
    pfree_ext(bb_bothnotnull);
    pfree_ext(bb_checkbothnull);
    pfree_ext(bb_eithernull);
    pfree_ext(comp_res);

    llvmCodeGen->FinalizeFunction(jitted_comparecol, node->ss.ps.plan->plan_node_id);
    return jitted_comparecol;
}

/*
 * @Description	: Codegen on CompareMultiColumn function of Sort operator
 * under LIMIT operator. During execution, when switch from quick sort to
 * heap sort, the order needs to be reversed for the execution of CompareMultiColumn.
 * The generated code will use reverse order (descending and nullsFirst),
 * all the others are the same as the CompareMultiColumnCodeGen.
 */
llvm::Function* VecSortCodeGen::CompareMultiColumnCodeGen_TOPN(VecSortState* node, bool use_prefetch)
{
    /* There is no need to do jittable on TopN version of CompareMultiColumn,
     * since it has the same condisiton as CompareMultiColumn.
     */
    Assert(NULL != (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj);
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    llvmCodeGen->loadIRFile();

    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    int i, colIdx;
    Sort* plannode = (Sort*)node->ss.ps.plan;
    PlanState* outerNode = outerPlanState(node);
    TupleDesc tupDesc = ExecGetResultType(outerNode);

    llvm::Value* tmpval = NULL;
    llvm::Value* llvmargs[3];
    llvm::Value* parm1 = NULL;
    llvm::Value* parm2 = NULL;
    llvm::Value* state = NULL;

    DEFINE_CG_TYPE(int1Type, BITOID);
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(int64PtrType, INT8OID);
    DEFINE_CG_PTRTYPE(MultiColumnsPtrType, "struct.MultiColumns");
    DEFINE_CG_PTRTYPE(BatchsortstatePtrType, "class.Batchsortstate");

    DEFINE_CGVAR_INT8(int8_1, 1);
    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT32(int32_3, 3);
    DEFINE_CGVAR_INT32(int32_m1, -1);
    DEFINE_CGVAR_INT64(int64_0, 0);
    llvm::Value* Vals2[2] = {int64_0, int32_0};

    /* the prototype of the jitted function for CompareMultiColumn
     * Two parameters: (1) MultiColumns *a, (2) MultiColumns *b
     */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "JittedCompareMultiColumn_TOPN", int32Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("a", MultiColumnsPtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("b", MultiColumnsPtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("state", BatchsortstatePtrType));
    llvm::Function* jitted_CMC = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    parm1 = llvmargs[0];
    parm2 = llvmargs[1];
    state = llvmargs[2];

    int numKeys = plannode->numCols;

    llvm::BasicBlock** bb_keys1 = (llvm::BasicBlock**)palloc(sizeof(llvm::BasicBlock*) * numKeys);
    llvm::BasicBlock** bb_keys2 = (llvm::BasicBlock**)palloc(sizeof(llvm::BasicBlock*) * numKeys);
    llvm::BasicBlock** bb_bothnotnull = (llvm::BasicBlock**)palloc(sizeof(llvm::BasicBlock*) * numKeys);
    llvm::BasicBlock** bb_checkbothnull = (llvm::BasicBlock**)palloc(sizeof(llvm::BasicBlock*) * numKeys);
    llvm::BasicBlock** bb_eithernull = (llvm::BasicBlock**)palloc(sizeof(llvm::BasicBlock*) * numKeys);

    llvm::PHINode* Phi_res = NULL;
    llvm::Value** comp_res = (llvm::Value**)palloc(sizeof(llvm::Value*) * numKeys);

    llvm::Value* nulls1 = NULL;
    llvm::Value* nulls2 = NULL;
    llvm::Value* values1 = NULL;
    llvm::Value* values2 = NULL;
    llvm::Value* result1 = NULL;
    llvm::Value* result2 = NULL;
    llvm::Value* datum1 = NULL;
    llvm::Value* datum2 = NULL;

    for (i = 0; i < numKeys; i++) {
        bb_keys1[i] = llvm::BasicBlock::Create(context, "bb_keys1", jitted_CMC);
        bb_keys2[i] = llvm::BasicBlock::Create(context, "bb_keys2", jitted_CMC);
        bb_bothnotnull[i] = llvm::BasicBlock::Create(context, "bb_bothnotnull", jitted_CMC);
        bb_checkbothnull[i] = llvm::BasicBlock::Create(context, "bb_checkbothnull", jitted_CMC);
        bb_eithernull[i] = llvm::BasicBlock::Create(context, "bb_eithernull", jitted_CMC);
        comp_res[i] = NULL;
    }
    DEFINE_BLOCK(bb_ret, jitted_CMC);

    /* Start the codegen in entry basic block */
    /* MultiColumns.m_values */
    tmpval = builder.CreateInBoundsGEP(parm1, Vals2);
    values1 = builder.CreateLoad(int64PtrType, tmpval, "a_values");
    tmpval = builder.CreateInBoundsGEP(parm2, Vals2);
    values2 = builder.CreateLoad(int64PtrType, tmpval, "b_values");

    /* MultiColumns.m_nulls */
    Vals2[1] = int32_1;
    tmpval = builder.CreateInBoundsGEP(parm1, Vals2);
    nulls1 = builder.CreateLoad(int8PtrType, tmpval, "a_nulls");
    tmpval = builder.CreateInBoundsGEP(parm2, Vals2);
    nulls2 = builder.CreateLoad(int8PtrType, tmpval, "b_nulls");
    builder.CreateBr(bb_keys1[0]);

    /* Codegen for each sort key comparison */
    for (i = 0; i < numKeys; i++) {
        bool desc = false;
        bool nullsFirst = plannode->nullsFirst[i];
        Oid opno = plannode->sortOperators[i];

        /* reverse order for TopN */
        nullsFirst = !nullsFirst;
        colIdx = plannode->sortColIdx[i] - 1;
        Form_pg_attribute attr = &tupDesc->attrs[colIdx];
        Oid typeOid = attr->atttypid;

        builder.SetInsertPoint(bb_keys1[i]);
        llvm::Value* val_colIdx = llvmCodeGen->getIntConstant(INT8OID, colIdx);

        /* loading the data first for prefetch, we assume most data is
         * NOT NULL : MultiColumns->a_values[colIdx].
         */
        tmpval = builder.CreateInBoundsGEP(values1, val_colIdx);
        datum1 = builder.CreateLoad(int64Type, tmpval, "a_values_i");
        tmpval = builder.CreateInBoundsGEP(values2, val_colIdx);
        datum2 = builder.CreateLoad(int64Type, tmpval, "b_values_i");

        /*
         * For bpchar or numeric type key, the varlena datum is a pointer
         * points to the data with header. Prefetch the pointed buffer
         * may reduce the memory stall (cache miss overhead).
         */
        if (use_prefetch) {
            /* prefetch current key if it is bpchar or numeric data */
            llvm::Function* fn_prefetch =
                llvm::Intrinsic::getDeclaration(llvmCodeGen->module(), llvm_prefetch);
            if (fn_prefetch == NULL) {
                ereport(ERROR,
                    (errcode(ERRCODE_LOAD_INTRINSIC_FUNCTION_FAILED),
                        errmodule(MOD_LLVM),
                        errmsg("Failed to get llvm function Intrinsic::prefetch!\n")));
            }

            if (i == 0) {
                if (typeOid == VARCHAROID || typeOid == TEXTOID || typeOid == BPCHAROID || typeOid == NUMERICOID) {
                    tmpval = builder.CreateIntToPtr(datum1, int8PtrType);
                    builder.CreateCall(fn_prefetch, {tmpval, int32_0, int32_3, int32_1});
                    tmpval = builder.CreateIntToPtr(datum2, int8PtrType);
                    builder.CreateCall(fn_prefetch, {tmpval, int32_0, int32_3, int32_1});
                }
            }

            /* prefetch the next key if it is bpchar or numeric data */
            if (i < numKeys - 1) {
                int next_colIdx = plannode->sortColIdx[i + 1] - 1;
                Form_pg_attribute next_attr = &tupDesc->attrs[next_colIdx];
                if (next_attr->atttypid == VARCHAROID || next_attr->atttypid == TEXTOID ||
                    next_attr->atttypid == BPCHAROID || next_attr->atttypid == NUMERICOID) {
                    llvm::Value* next_val_colIdx = llvmCodeGen->getIntConstant(INT8OID, next_colIdx);
                    tmpval = builder.CreateInBoundsGEP(values1, next_val_colIdx);
                    llvm::Value* next_datum1 = builder.CreateLoad(int64Type, tmpval, "a_values_ip1");
                    tmpval = builder.CreateInBoundsGEP(values2, next_val_colIdx);
                    llvm::Value* next_datum2 = builder.CreateLoad(int64Type, tmpval, "b_values_ip1");
                    tmpval = builder.CreateIntToPtr(next_datum1, int8PtrType);
                    builder.CreateCall(fn_prefetch, {tmpval, int32_0, int32_3, int32_1});
                    tmpval = builder.CreateIntToPtr(next_datum2, int8PtrType);
                    builder.CreateCall(fn_prefetch, {tmpval, int32_0, int32_3, int32_1});
                }
            }
        }

        /* get MultiColumns.m_nulls[colIdx] */
        tmpval = builder.CreateInBoundsGEP(nulls1, val_colIdx);
        llvm::Value* isNull1 = builder.CreateLoad(int8Type, tmpval, "a_nulls_i");
        tmpval = builder.CreateInBoundsGEP(nulls2, val_colIdx);
        llvm::Value* isNull2 = builder.CreateLoad(int8Type, tmpval, "b_nulls_i");

        /* check if both of them are not null by 'Or' operation */
        isNull1 = builder.CreateAnd(isNull1, int8_1);
        isNull2 = builder.CreateAnd(isNull2, int8_1);
        isNull1 = builder.CreateTrunc(isNull1, int1Type);
        isNull2 = builder.CreateTrunc(isNull2, int1Type);
        tmpval = builder.CreateOr(isNull1, isNull2);
        builder.CreateCondBr(tmpval, bb_checkbothnull[i], bb_bothnotnull[i]);

        builder.SetInsertPoint(bb_checkbothnull[i]);
        tmpval = builder.CreateAnd(isNull1, isNull2);
        builder.CreateCondBr(tmpval, bb_keys2[i], bb_eithernull[i]);

        builder.SetInsertPoint(bb_eithernull[i]);
        if (nullsFirst) {
            result2 = builder.CreateSelect(isNull1, int32_m1, int32_1);
        } else {
            result2 = builder.CreateSelect(isNull1, int32_1, int32_m1);
        }
        builder.CreateBr(bb_keys2[i]);

        builder.SetInsertPoint(bb_bothnotnull[i]);

        result1 = NULL;
        switch (typeOid) {
            case NUMERICOID: {
                Assert(opno == NUMERICLTOID || opno == NUMERICGTOID);
                desc = (opno == NUMERICGTOID) ? true : false;

                llvm::Function* jitted_numericcmp = NULL;
                bool fastpath = false;

                /* consider use fast path if numeric data can be represented
                 * in BI64 format (precision less than 19).
                 */
                if (attr->atttypmod != -1 && ((attr->atttypmod >> 16) & 0xFFFF) <= 18)
                    fastpath = true;

                if (fastpath) {
                    jitted_numericcmp = llvmCodeGen->module()->getFunction("LLVMIRnumericcmp_fastpath");
                    if (jitted_numericcmp == NULL) {
                        jitted_numericcmp = numericcmpCodeGen_fastpath();
                    }
                } else {
                    jitted_numericcmp = llvmCodeGen->module()->getFunction("LLVMIRnumericcmp");
                    if (jitted_numericcmp == NULL) {
                        jitted_numericcmp = numericcmpCodeGen();
                    }
                }
                result1 = builder.CreateCall(jitted_numericcmp, {datum1, datum2});
            } break;
            case TEXTOID:
            case VARCHAROID: {
                Assert(opno == TEXTLTOID || opno == TEXTGTOID);
                desc = (opno == TEXTGTOID) ? true : false;

                llvm::Function* jitted_textcmp = NULL;
                jitted_textcmp = llvmCodeGen->module()->getFunction("LLVMIRtextcmp_CMC");
                if (jitted_textcmp == NULL) {
                    jitted_textcmp = textcmpCodeGen();
                }
                result1 = builder.CreateCall(jitted_textcmp, {datum1, datum2});
            } break;
            case BPCHAROID: {
                int len = attr->atttypmod - VARHDRSZ;
                Assert(opno == BPCHARLTOID || opno == BPCHARGTOID);
                desc = (opno == BPCHARGTOID) ? true : false;

                /*
                 * When the length of bpchar is less than 2 byte, we could compare
                 * them in fast path, so make a difference here.
                 */
                llvm::Function* jitted_bpcharcmp = NULL;
                if (len > 16) {
                    jitted_bpcharcmp = llvmCodeGen->module()->getFunction("LLVMIRbpcharcmp_long");
                    if (jitted_bpcharcmp == NULL) {
                        jitted_bpcharcmp = bpcharcmpCodeGen_long();
                    }
                } else {
                    jitted_bpcharcmp = llvmCodeGen->module()->getFunction("LLVMIRbpcharcmp_short");
                    if (jitted_bpcharcmp == NULL) {
                        jitted_bpcharcmp = bpcharcmpCodeGen_short();
                    }
                }
                llvm::Value* len_value = llvmCodeGen->getIntConstant(INT4OID, len);
                result1 = builder.CreateCall(jitted_bpcharcmp, {datum1, datum2, len_value});
            } break;
            case INT4OID: {
                Assert(opno == INT4LTOID || opno == INT4GTOID);
                desc = (opno == INT4GTOID) ? true : false;

                datum1 = builder.CreateTrunc(datum1, int32Type);
                datum2 = builder.CreateTrunc(datum2, int32Type);
                tmpval = builder.CreateICmpEQ(datum1, datum2);
                result1 = builder.CreateICmpSGT(datum1, datum2);
                result1 = builder.CreateSelect(result1, int32_1, int32_m1);
                result1 = builder.CreateSelect(tmpval, int32_0, result1);
            } break;
            case INT8OID: {
                Assert(opno == INT8LTOID || opno == INT8GTOID);
                desc = (opno == INT8GTOID) ? true : false;

                tmpval = builder.CreateICmpEQ(datum1, datum2);
                result1 = builder.CreateICmpSGT(datum1, datum2);
                result1 = builder.CreateSelect(result1, int32_1, int32_m1);
                result1 = builder.CreateSelect(tmpval, int32_0, result1);
            } break;
            default:
                break;
        }

        /*
         * Descending is only applied for both NOT NULL data, it cannot be
         * applied to any NULL data. When desc is true, reversal the result.
         */
        desc = !desc;
        if (desc) {
            result1 = builder.CreateSub(int32_0, result1);
        }
        builder.CreateBr(bb_keys2[i]);

        builder.SetInsertPoint(bb_keys2[i]);
        Phi_res = builder.CreatePHI(int32Type, 3);
        Phi_res->addIncoming(result1, bb_bothnotnull[i]);
        Phi_res->addIncoming(int32_0, bb_checkbothnull[i]);
        Phi_res->addIncoming(result2, bb_eithernull[i]);
        comp_res[i] = Phi_res;

        if (i == numKeys - 1)
            builder.CreateBr(bb_ret);
        else {
            tmpval = builder.CreateICmpEQ(Phi_res, int32_0);
            builder.CreateCondBr(tmpval, bb_keys1[i + 1], bb_ret);
        }
    }

    builder.SetInsertPoint(bb_ret);
    llvm::PHINode* Phi_compare = builder.CreatePHI(int32Type, numKeys);
    for (i = 0; i < numKeys; i++) {
        Phi_compare->addIncoming(comp_res[i], bb_keys2[i]);
    }
    builder.CreateRet(Phi_compare);

    pfree_ext(bb_keys1);
    pfree_ext(bb_keys2);
    pfree_ext(bb_bothnotnull);
    pfree_ext(bb_checkbothnull);
    pfree_ext(bb_eithernull);
    pfree_ext(comp_res);

    llvmCodeGen->FinalizeFunction(jitted_CMC, node->ss.ps.plan->plan_node_id);
    return jitted_CMC;
}

/*
 * @Description	: Codegenration on numeric_cmp function. Currently we only
 * codegen the bi64cmp64 for the comparison of two int64 data without scale
 * adjustment. If the conditions are not satisfied, call the original wrapped
 * numeric_cmp.
 */
llvm::Function* VecSortCodeGen::numericcmpCodeGen()
{
    Assert(NULL != (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj);
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    llvm::Value* tmpval = NULL;
    llvm::Value* tmpval2 = NULL;
    llvm::Value* llvmargs[2];
    llvm::Value* parm1 = NULL;
    llvm::Value* parm2 = NULL;
    llvm::Value* value1 = NULL;
    llvm::Value* value2 = NULL;

    DEFINE_CG_VOIDTYPE(voidType);
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int16Type, INT2OID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(int64PtrType, INT8OID);
    DEFINE_CG_PTRTYPE(varattrib_1bPtrType, "struct.varattrib_1b");
    DEFINE_CG_PTRTYPE(varlenaPtrType, "struct.varlena");
    DEFINE_CG_PTRTYPE(NumericDataPtrType, "struct.NumericData");

    DEFINE_CGVAR_INT1(int1_1, 1);
    DEFINE_CGVAR_INT8(int8_0, 0);
    DEFINE_CGVAR_INT8(int8_3, 3);
    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT32(int32_20, 20);
    DEFINE_CGVAR_INT32(int32_m1, -1);
    DEFINE_CGVAR_INT32(val_sign_mask, NUMERIC_SIGN_MASK);
    DEFINE_CGVAR_INT32(val_bi_mask, NUMERIC_BI_MASK);
    DEFINE_CGVAR_INT32(val_numeric64, NUMERIC_64);
    DEFINE_CGVAR_INT64(int64_0, 0);

    llvm::Value* Vals2[2] = {int64_0, int32_0};
    llvm::Value* Vals4[4] = {int64_0, int32_0, int32_0, int32_0};

    /* the prototype of the jitted function for numeric_cmp
     * Two parameters: (1) datum a, (2) datum b
     */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMIRnumericcmp", int32Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("a", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("b", int64Type));
    llvm::Function* jitted_numericcmp = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    parm1 = llvmargs[0];
    parm2 = llvmargs[1];

    DEFINE_BLOCK(bb_untoast1, jitted_numericcmp);
    DEFINE_BLOCK(bb_data1, jitted_numericcmp);
    DEFINE_BLOCK(bb_nextVal, jitted_numericcmp);
    DEFINE_BLOCK(bb_untoast2, jitted_numericcmp);
    DEFINE_BLOCK(bb_data2, jitted_numericcmp);
    DEFINE_BLOCK(bb_checkNAN, jitted_numericcmp);
    DEFINE_BLOCK(bb_checkBI, jitted_numericcmp);
    DEFINE_BLOCK(bb_checkScale, jitted_numericcmp);
    DEFINE_BLOCK(bb_adjustScale, jitted_numericcmp);
    DEFINE_BLOCK(bb_checkScaleAdjust, jitted_numericcmp);
    DEFINE_BLOCK(bb_int64cmp, jitted_numericcmp);
    DEFINE_BLOCK(bb_originalcmp, jitted_numericcmp);
    DEFINE_BLOCK(bb_checkBothNAN, jitted_numericcmp);
    DEFINE_BLOCK(bb_eitherNAN, jitted_numericcmp);
    DEFINE_BLOCK(bb_ret, jitted_numericcmp);
    DEFINE_BLOCK(bb_pfree1, jitted_numericcmp);
    DEFINE_BLOCK(bb_check_pfree, jitted_numericcmp);
    DEFINE_BLOCK(bb_pfree2, jitted_numericcmp);
    DEFINE_BLOCK(bb_ret2, jitted_numericcmp);

    /*
     * call PG_GETARG_NUMERIC(arg1), which turn to call pg_detoast_datum(arg1):
     * if (VARATT_IS_EXTENDED(datum))
     * 		return heap_tuple_untoast_attr(datum);
     * else
     *		return datum;
     */
    value1 = builder.CreateIntToPtr(parm1, varattrib_1bPtrType);
    value1 = builder.CreateInBoundsGEP(value1, Vals2);
    value1 = builder.CreateLoad(int8Type, value1, "header1");
    tmpval = builder.CreateAnd(value1, int8_3);
    llvm::Value* isExtended = builder.CreateICmpEQ(tmpval, int8_0);
    builder.CreateCondBr(isExtended, bb_data1, bb_untoast1);

    /* if vartt_is_extended is true */
    builder.SetInsertPoint(bb_untoast1);
    llvm::Function* wrap_untoast = llvmCodeGen->module()->getFunction("LLVMIRWrapuntoast");
    if (wrap_untoast == NULL) {
        GsCodeGen::FnPrototype func_prototype(llvmCodeGen, "LLVMIRWrapuntoast", varlenaPtrType);
        func_prototype.addArgument(GsCodeGen::NamedVariable("datum", varlenaPtrType));
        wrap_untoast = func_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMIRWrapuntoast", (void*)Wrap_heap_tuple_untoast_attr);
    }

    /* get the numeric argument1 */
    value1 = builder.CreateIntToPtr(parm1, varlenaPtrType);
    llvm::Value* untoast_value1 = builder.CreateCall(wrap_untoast, value1);
    untoast_value1 = builder.CreateBitCast(untoast_value1, NumericDataPtrType);
    builder.CreateBr(bb_nextVal);

    builder.SetInsertPoint(bb_data1);
    value1 = builder.CreateIntToPtr(parm1, NumericDataPtrType);
    builder.CreateBr(bb_nextVal);

    /* finish get argument1, and deal with next argument */
    builder.SetInsertPoint(bb_nextVal);
    llvm::PHINode* Phi_arg1 = builder.CreatePHI(NumericDataPtrType, 2);
    Phi_arg1->addIncoming(value1, bb_data1);
    Phi_arg1->addIncoming(untoast_value1, bb_untoast1);

    value2 = builder.CreateIntToPtr(parm2, varattrib_1bPtrType);
    value2 = builder.CreateInBoundsGEP(value2, Vals2);
    value2 = builder.CreateLoad(int8Type, value2, "header2");
    tmpval = builder.CreateAnd(value2, int8_3);
    isExtended = builder.CreateICmpEQ(tmpval, int8_0);
    builder.CreateCondBr(isExtended, bb_data2, bb_untoast2);

    /* if vartt_is_extended is true */
    builder.SetInsertPoint(bb_untoast2);
    value2 = builder.CreateIntToPtr(parm2, varlenaPtrType);
    llvm::Value* untoast_value2 = builder.CreateCall(wrap_untoast, value2);
    untoast_value2 = builder.CreateBitCast(untoast_value2, NumericDataPtrType);
    builder.CreateBr(bb_checkNAN);

    builder.SetInsertPoint(bb_data2);
    value2 = builder.CreateIntToPtr(parm2, NumericDataPtrType);
    builder.CreateBr(bb_checkNAN);

    /*
     * After we get num1 and num2, consider all NANS to be equal and larger
     * than any non-NAN.
     */
    builder.SetInsertPoint(bb_checkNAN);
    llvm::PHINode* Phi_arg2 = builder.CreatePHI(NumericDataPtrType, 2);
    Phi_arg2->addIncoming(value2, bb_data2);
    Phi_arg2->addIncoming(untoast_value2, bb_untoast2);

    /* check arg1 & arg2 are NANs or not */
    /* (arg1)->choice.n_header & NUMERIC_BI_MASK == NUMERIC_NAN */
    Vals4[1] = int32_1;
    tmpval = builder.CreateInBoundsGEP(Phi_arg1, Vals4);
    llvm::Value* n_header1 = builder.CreateLoad(int16Type, tmpval, "n_header1");
    n_header1 = builder.CreateZExt(n_header1, int32Type);
    llvm::Value* n_header_mask1 = builder.CreateAnd(n_header1, val_bi_mask);
    llvm::Value* isNAN1 = builder.CreateICmpEQ(n_header_mask1, val_sign_mask);

    /* (arg2)->choice.n_header & NUMERIC_BI_MASK == NUMERIC_NAN */
    tmpval = builder.CreateInBoundsGEP(Phi_arg2, Vals4);
    llvm::Value* n_header2 = builder.CreateLoad(int16Type, tmpval, "n_header2");
    n_header2 = builder.CreateZExt(n_header2, int32Type);
    llvm::Value* n_header_mask2 = builder.CreateAnd(n_header2, val_bi_mask);
    llvm::Value* isNAN2 = builder.CreateICmpEQ(n_header_mask2, val_sign_mask);

    /* If one of args is nan, we can get the result now */
    tmpval = builder.CreateOr(isNAN1, isNAN2);
    builder.CreateCondBr(tmpval, bb_checkBothNAN, bb_checkBI);

    /* both of them are not nans, so check them are both BI64 or not */
    builder.SetInsertPoint(bb_checkBI);
    llvm::Value* is64BI1 = builder.CreateICmpEQ(n_header_mask1, val_numeric64);
    llvm::Value* is64BI2 = builder.CreateICmpEQ(n_header_mask2, val_numeric64);
    tmpval = builder.CreateAnd(is64BI1, is64BI2);
    builder.CreateCondBr(tmpval, bb_checkScale, bb_originalcmp);

    /* both of them are BI64s, then check if they have the same scale */
    builder.SetInsertPoint(bb_checkScale);
    Vals4[1] = int32_1;
    Vals4[3] = int32_1;
    tmpval = builder.CreateInBoundsGEP(Phi_arg1, Vals4);
    tmpval = builder.CreateBitCast(tmpval, int64PtrType);
    llvm::Value* int64data1 = builder.CreateLoad(int64Type, tmpval, "int64data1");
    tmpval = builder.CreateInBoundsGEP(Phi_arg2, Vals4);
    tmpval = builder.CreateBitCast(tmpval, int64PtrType);
    llvm::Value* int64data2 = builder.CreateLoad(int64Type, tmpval, "int64data2");

    /* if they have the same header, they have the same scale */
    tmpval = builder.CreateICmpEQ(n_header1, n_header2);
    builder.CreateCondBr(tmpval, bb_int64cmp, bb_adjustScale);

    /* if the scales are not the same, calculate delta_scale and do adjust */
    builder.SetInsertPoint(bb_adjustScale);
    llvm::Value* isScale1Greater = builder.CreateICmpSGT(n_header1, n_header2);
    llvm::Value* delta1 = builder.CreateSub(n_header1, n_header2);
    llvm::Value* delta2 = builder.CreateSub(n_header2, n_header1);
    llvm::Value* delta = builder.CreateSelect(isScale1Greater, delta1, delta2);

    /*
     * the max dimension of Int64MultiOutOfBound is 20, which is used to check
     * if the result of tmpval*ScaleMultipler[delta_scale] out of int64 bound.
     */
    tmpval = builder.CreateICmpSGT(delta, int32_20);
    builder.CreateCondBr(tmpval, bb_originalcmp, bb_checkScaleAdjust);

    builder.SetInsertPoint(bb_checkScaleAdjust);
    llvm::Value* data1Zero = builder.CreateICmpEQ(int64data1, int64_0);
    llvm::Value* data2Zero = builder.CreateICmpEQ(int64data2, int64_0);
    tmpval = builder.CreateAnd(isScale1Greater, data2Zero);
    llvm::Value* isScale1Less = builder.CreateXor(isScale1Greater, int1_1);
    tmpval2 = builder.CreateAnd(isScale1Less, data1Zero);
    tmpval = builder.CreateOr(tmpval, tmpval2);
    builder.CreateCondBr(tmpval, bb_int64cmp, bb_originalcmp);

    /* both of them bi64 and have the same scale, so get the result */
    builder.SetInsertPoint(bb_int64cmp);
    tmpval = builder.CreateICmpSGT(int64data1, int64data2);
    tmpval2 = builder.CreateICmpEQ(int64data1, int64data2);
    llvm::Value* result1 = builder.CreateSelect(tmpval, int32_1, int32_m1);
    result1 = builder.CreateSelect(tmpval2, int32_0, result1);
    builder.CreateBr(bb_ret);

    /* turn to orginal cmp_numerics */
    builder.SetInsertPoint(bb_originalcmp);
    llvm::Function* wrap_numericcmp = llvmCodeGen->module()->getFunction("LLVMIRwrapcmpnumerics");
    if (wrap_numericcmp == NULL) {
        GsCodeGen::FnPrototype func_prototype(llvmCodeGen, "LLVMIRwrapcmpnumerics", int32Type);
        func_prototype.addArgument(GsCodeGen::NamedVariable("datum1", NumericDataPtrType));
        func_prototype.addArgument(GsCodeGen::NamedVariable("datum2", NumericDataPtrType));
        wrap_numericcmp = func_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMIRwrapcmpnumerics", (void*)Wrap_numeric_cmp);
    }
    llvm::Value* result2 = builder.CreateCall(wrap_numericcmp, {Phi_arg1, Phi_arg2});
    builder.CreateBr(bb_ret);

    /* if one of the argument is NaN */
    builder.SetInsertPoint(bb_checkBothNAN);
    tmpval = builder.CreateAnd(isNAN1, isNAN2);
    builder.CreateCondBr(tmpval, bb_ret, bb_eitherNAN);

    builder.SetInsertPoint(bb_eitherNAN);
    llvm::Value* result3 = builder.CreateSelect(isNAN1, int32_1, int32_m1);
    builder.CreateBr(bb_ret);

    builder.SetInsertPoint(bb_ret);
    llvm::PHINode* Phi_compare = builder.CreatePHI(int32Type, 4);
    Phi_compare->addIncoming(result1, bb_int64cmp);
    Phi_compare->addIncoming(result2, bb_originalcmp);
    Phi_compare->addIncoming(result3, bb_eitherNAN);
    Phi_compare->addIncoming(int32_0, bb_checkBothNAN);

    value1 = builder.CreatePtrToInt(Phi_arg1, int64Type);
    tmpval = builder.CreateICmpEQ(parm1, value1);
    builder.CreateCondBr(tmpval, bb_check_pfree, bb_pfree1);

    /* pfree space at last */
    builder.SetInsertPoint(bb_pfree1);
    llvm::Function* wrap_pfree = llvmCodeGen->module()->getFunction("LLVMIRwrappfree");
    if (wrap_pfree == NULL) {
        GsCodeGen::FnPrototype func_prototype(llvmCodeGen, "LLVMIRwrappfree", voidType);
        func_prototype.addArgument(GsCodeGen::NamedVariable("ptr", int8PtrType));
        wrap_pfree = func_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMIRwrappfree", (void*)Wrap_pfree);
    }
    tmpval = builder.CreateBitCast(Phi_arg1, int8PtrType);
    builder.CreateCall(wrap_pfree, tmpval);
    builder.CreateBr(bb_check_pfree);

    builder.SetInsertPoint(bb_check_pfree);
    value2 = builder.CreatePtrToInt(Phi_arg2, int64Type);
    tmpval = builder.CreateICmpEQ(parm2, value2);
    builder.CreateCondBr(tmpval, bb_ret2, bb_pfree2);

    builder.SetInsertPoint(bb_pfree2);
    tmpval = builder.CreateBitCast(Phi_arg2, int8PtrType);
    builder.CreateCall(wrap_pfree, tmpval);
    builder.CreateBr(bb_ret2);

    builder.SetInsertPoint(bb_ret2);
    builder.CreateRet(Phi_compare);

    llvmCodeGen->FinalizeFunction(jitted_numericcmp);
    return jitted_numericcmp;
}

/*
 * @Description	: When the precision of numeric is less than 19, use fast
 * path to codegen numeric_cmp function.
 */
llvm::Function* VecSortCodeGen::numericcmpCodeGen_fastpath()
{
    Assert(NULL != (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj);
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    llvm::Value* tmpval = NULL;
    llvm::Value* tmpval2 = NULL;
    llvm::Value* llvmargs[2];
    llvm::Value* parm1 = NULL;
    llvm::Value* parm2 = NULL;
    llvm::Value* value1 = NULL;
    llvm::Value* value2 = NULL;

    DEFINE_CG_VOIDTYPE(voidType);
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int16Type, INT2OID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(int64PtrType, INT8OID);
    DEFINE_CG_PTRTYPE(varattrib_1bPtrType, "struct.varattrib_1b");
    DEFINE_CG_PTRTYPE(varlenaPtrType, "struct.varlena");
    DEFINE_CG_PTRTYPE(NumericDataPtrType, "struct.NumericData");

    DEFINE_CGVAR_INT8(int8_0, 0);
    DEFINE_CGVAR_INT8(int8_3, 3);
    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT32(int32_m1, -1);
    DEFINE_CGVAR_INT32(val_sign_mask, NUMERIC_SIGN_MASK);
    DEFINE_CGVAR_INT32(val_bi_mask, NUMERIC_BI_MASK);
    DEFINE_CGVAR_INT32(val_numeric64, NUMERIC_64);
    DEFINE_CGVAR_INT64(int64_0, 0);

    llvm::Value* Vals2[2] = {int64_0, int32_0};
    llvm::Value* Vals4[4] = {int64_0, int32_0, int32_0, int32_0};

    /* the prototype of the jitted function for numeric_cmp
     * Two parameters: (1) datum a, (2) datum b
     */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMIRnumericcmp_fastpath", int32Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("a", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("b", int64Type));
    llvm::Function* jitted_numericcmp = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    parm1 = llvmargs[0];
    parm2 = llvmargs[1];

    DEFINE_BLOCK(bb_untoast1, jitted_numericcmp);
    DEFINE_BLOCK(bb_data1, jitted_numericcmp);
    DEFINE_BLOCK(bb_nextVal, jitted_numericcmp);
    DEFINE_BLOCK(bb_untoast2, jitted_numericcmp);
    DEFINE_BLOCK(bb_data2, jitted_numericcmp);
    DEFINE_BLOCK(bb_checkNAN, jitted_numericcmp);
    DEFINE_BLOCK(bb_checkBI, jitted_numericcmp);
    DEFINE_BLOCK(bb_int64cmp, jitted_numericcmp);
    DEFINE_BLOCK(bb_originalcmp, jitted_numericcmp);
    DEFINE_BLOCK(bb_checkBothNAN, jitted_numericcmp);
    DEFINE_BLOCK(bb_eitherNAN, jitted_numericcmp);
    DEFINE_BLOCK(bb_ret, jitted_numericcmp);
    DEFINE_BLOCK(bb_pfree1, jitted_numericcmp);
    DEFINE_BLOCK(bb_check_pfree, jitted_numericcmp);
    DEFINE_BLOCK(bb_pfree2, jitted_numericcmp);
    DEFINE_BLOCK(bb_ret2, jitted_numericcmp);

    /*
     * call PG_GETARG_NUMERIC(arg1), which turn to call pg_detoast_datum(arg1):
     * if (VARATT_IS_EXTENDED(datum))
     * 		return heap_tuple_untoast_attr(datum);
     * else
     *		return datum;
     */
    value1 = builder.CreateIntToPtr(parm1, varattrib_1bPtrType);
    value1 = builder.CreateInBoundsGEP(value1, Vals2);
    value1 = builder.CreateLoad(int8Type, value1, "header1");
    tmpval = builder.CreateAnd(value1, int8_3);
    llvm::Value* isExtended = builder.CreateICmpEQ(tmpval, int8_0);
    builder.CreateCondBr(isExtended, bb_data1, bb_untoast1);

    builder.SetInsertPoint(bb_untoast1);
    llvm::Function* wrap_untoast = llvmCodeGen->module()->getFunction("LLVMIRwrapuntoast");
    if (wrap_untoast == NULL) {
        GsCodeGen::FnPrototype func_prototype(llvmCodeGen, "LLVMIRwrapuntoast", varlenaPtrType);
        func_prototype.addArgument(GsCodeGen::NamedVariable("datum", varlenaPtrType));
        wrap_untoast = func_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMIRwrapuntoast", (void*)Wrap_heap_tuple_untoast_attr);
    }

    /* get the numeric argument1 */
    value1 = builder.CreateIntToPtr(parm1, varlenaPtrType);
    llvm::Value* untoast_value1 = builder.CreateCall(wrap_untoast, value1);
    untoast_value1 = builder.CreateBitCast(untoast_value1, NumericDataPtrType);
    builder.CreateBr(bb_nextVal);

    builder.SetInsertPoint(bb_data1);
    value1 = builder.CreateIntToPtr(parm1, NumericDataPtrType);
    builder.CreateBr(bb_nextVal);

    /* finish get argument1, and deal with next argument */
    builder.SetInsertPoint(bb_nextVal);
    llvm::PHINode* Phi_arg1 = builder.CreatePHI(NumericDataPtrType, 2);
    Phi_arg1->addIncoming(value1, bb_data1);
    Phi_arg1->addIncoming(untoast_value1, bb_untoast1);

    value2 = builder.CreateIntToPtr(parm2, varattrib_1bPtrType);
    value2 = builder.CreateInBoundsGEP(value2, Vals2);
    value2 = builder.CreateLoad(int8Type, value2, "header2");
    tmpval = builder.CreateAnd(value2, int8_3);
    isExtended = builder.CreateICmpEQ(tmpval, int8_0);
    builder.CreateCondBr(isExtended, bb_data2, bb_untoast2);

    /* if vartt_is_extended is true */
    builder.SetInsertPoint(bb_untoast2);
    value2 = builder.CreateIntToPtr(parm2, varlenaPtrType);
    llvm::Value* untoast_value2 = builder.CreateCall(wrap_untoast, value2);
    untoast_value2 = builder.CreateBitCast(untoast_value2, NumericDataPtrType);
    builder.CreateBr(bb_checkNAN);

    builder.SetInsertPoint(bb_data2);
    value2 = builder.CreateIntToPtr(parm2, NumericDataPtrType);
    builder.CreateBr(bb_checkNAN);

    /*
     * After we get num1 and num2, consider all NANS to be equal and larger
     * than any non-NAN.
     */
    builder.SetInsertPoint(bb_checkNAN);
    llvm::PHINode* Phi_arg2 = builder.CreatePHI(NumericDataPtrType, 2);
    Phi_arg2->addIncoming(value2, bb_data2);
    Phi_arg2->addIncoming(untoast_value2, bb_untoast2);

    /* check arg1 & arg2 are NANs or not */
    /* (arg1)->choice.n_header & NUMERIC_BI_MASK == NUMERIC_NAN */
    Vals4[1] = int32_1;
    tmpval = builder.CreateInBoundsGEP(Phi_arg1, Vals4);
    llvm::Value* n_header1 = builder.CreateLoad(int16Type, tmpval, "n_header1");
    n_header1 = builder.CreateZExt(n_header1, int32Type);
    llvm::Value* n_header_mask1 = builder.CreateAnd(n_header1, val_bi_mask);
    llvm::Value* isNAN1 = builder.CreateICmpEQ(n_header_mask1, val_sign_mask);

    /* (arg2)->choice.n_header & NUMERIC_BI_MASK == NUMERIC_NAN */
    tmpval = builder.CreateInBoundsGEP(Phi_arg2, Vals4);
    llvm::Value* n_header2 = builder.CreateLoad(int16Type, tmpval, "n_header2");
    n_header2 = builder.CreateZExt(n_header2, int32Type);
    llvm::Value* n_header_mask2 = builder.CreateAnd(n_header2, val_bi_mask);
    llvm::Value* isNAN2 = builder.CreateICmpEQ(n_header_mask2, val_sign_mask);
    tmpval = builder.CreateOr(isNAN1, isNAN2);
    builder.CreateCondBr(tmpval, bb_checkBothNAN, bb_checkBI);

    /* both of them are not nans, so check them are both BI64 or not */
    builder.SetInsertPoint(bb_checkBI);
    llvm::Value* is64BI1 = builder.CreateICmpEQ(n_header_mask1, val_numeric64);
    llvm::Value* is64BI2 = builder.CreateICmpEQ(n_header_mask2, val_numeric64);
    tmpval = builder.CreateAnd(is64BI1, is64BI2);
    builder.CreateCondBr(tmpval, bb_int64cmp, bb_originalcmp);

    /*
     * In the fast numeric sort compare path, all the data have the same
     * attribute, so no need to compare scale, compare them directly.
     */
    builder.SetInsertPoint(bb_int64cmp);
    Vals4[1] = int32_1;
    Vals4[3] = int32_1;
    tmpval = builder.CreateInBoundsGEP(Phi_arg1, Vals4);
    tmpval = builder.CreateBitCast(tmpval, int64PtrType);
    llvm::Value* int64data1 = builder.CreateLoad(int64Type, tmpval, "int64data1");
    tmpval = builder.CreateInBoundsGEP(Phi_arg2, Vals4);
    tmpval = builder.CreateBitCast(tmpval, int64PtrType);
    llvm::Value* int64data2 = builder.CreateLoad(int64Type, tmpval, "int64data2");

    tmpval = builder.CreateICmpSGT(int64data1, int64data2);
    tmpval2 = builder.CreateICmpEQ(int64data1, int64data2);
    llvm::Value* result1 = builder.CreateSelect(tmpval, int32_1, int32_m1);
    result1 = builder.CreateSelect(tmpval2, int32_0, result1);
    builder.CreateBr(bb_ret);

    /* If could not compare them directly, turn to original numeric compare function. */
    builder.SetInsertPoint(bb_originalcmp);
    llvm::Function* wrap_numericcmp = llvmCodeGen->module()->getFunction("LLVMIRwrapcmpnumerics");
    if (wrap_numericcmp == NULL) {
        GsCodeGen::FnPrototype func_prototype(llvmCodeGen, "LLVMIRwrapcmpnumerics", int32Type);
        func_prototype.addArgument(GsCodeGen::NamedVariable("datum1", NumericDataPtrType));
        func_prototype.addArgument(GsCodeGen::NamedVariable("datum2", NumericDataPtrType));
        wrap_numericcmp = func_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMIRwrapcmpnumerics", (void*)Wrap_numeric_cmp);
    }
    llvm::Value* result2 = builder.CreateCall(wrap_numericcmp, {Phi_arg1, Phi_arg2});
    builder.CreateBr(bb_ret);

    /* if one of the argument is at least NaN */
    builder.SetInsertPoint(bb_checkBothNAN);
    tmpval = builder.CreateAnd(isNAN1, isNAN2);
    builder.CreateCondBr(tmpval, bb_ret, bb_eitherNAN);

    builder.SetInsertPoint(bb_eitherNAN);
    llvm::Value* result3 = builder.CreateSelect(isNAN1, int32_1, int32_m1);
    builder.CreateBr(bb_ret);

    builder.SetInsertPoint(bb_ret);
    llvm::PHINode* Phi_compare = builder.CreatePHI(int32Type, 4);
    Phi_compare->addIncoming(result1, bb_int64cmp);
    Phi_compare->addIncoming(result2, bb_originalcmp);
    Phi_compare->addIncoming(result3, bb_eitherNAN);
    Phi_compare->addIncoming(int32_0, bb_checkBothNAN);

    value1 = builder.CreatePtrToInt(Phi_arg1, int64Type);
    tmpval = builder.CreateICmpEQ(parm1, value1);
    builder.CreateCondBr(tmpval, bb_check_pfree, bb_pfree1);

    builder.SetInsertPoint(bb_pfree1);
    llvm::Function* wrap_pfree = llvmCodeGen->module()->getFunction("LLVMIRwrappfree");
    if (wrap_pfree == NULL) {
        GsCodeGen::FnPrototype func_prototype(llvmCodeGen, "LLVMIRwrappfree", voidType);
        func_prototype.addArgument(GsCodeGen::NamedVariable("ptr", int8PtrType));
        wrap_pfree = func_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMIRwrappfree", (void*)Wrap_pfree);
    }
    tmpval = builder.CreateBitCast(Phi_arg1, int8PtrType);
    builder.CreateCall(wrap_pfree, tmpval);
    builder.CreateBr(bb_ret2);

    builder.SetInsertPoint(bb_check_pfree);
    value2 = builder.CreatePtrToInt(Phi_arg2, int64Type);
    tmpval = builder.CreateICmpEQ(parm2, value2);
    builder.CreateCondBr(tmpval, bb_ret2, bb_pfree2);

    builder.SetInsertPoint(bb_pfree2);
    tmpval = builder.CreateBitCast(Phi_arg2, int8PtrType);
    builder.CreateCall(wrap_pfree, tmpval);
    builder.CreateBr(bb_ret2);

    builder.SetInsertPoint(bb_ret2);
    builder.CreateRet(Phi_compare);

    llvmCodeGen->FinalizeFunction(jitted_numericcmp);
    return jitted_numericcmp;
}

/*
 * @Description	: Codegen on memcmp function that is called by bpcharcmp function
 * The generated function LLVMIRmemcmp_CMC compares each byte until it finds
 * the difference or reach the end of the strings, and returns -1, 0, or
 * 1 according to the comparison, the codegeneration is corresponding to:
 * LLVMIRmemcmp_CMC(char *a, char *b, int length)
 * {
 * 		for (i = 0; i < length; i++)
 * 		{
 *			if (a[i] != b[i])
 *				rerturn a[i] > b[i] ? 1 : -1;
 * 		}
 * 		return 0;
 * }
 */
llvm::Function* VecSortCodeGen::LLVMIRmemcmp_CMC_CodeGen()
{
    Assert(NULL != (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj);
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    llvm::Value* tmpval = NULL;
    llvm::Value* llvmargs[3];
    llvm::Value* parm1 = NULL;
    llvm::Value* parm2 = NULL;
    llvm::Value* parm3 = NULL;
    llvm::Value* data1 = NULL;
    llvm::Value* data2 = NULL;

    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);

    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT32(int32_m1, -1);
    DEFINE_CGVAR_INT64(int64_1, 1);

    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMIRmemcmp_CMC", int32Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("a", int8PtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("b", int8PtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("length", int32Type));
    llvm::Function* jitted_memcmp = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    parm1 = llvmargs[0];
    parm2 = llvmargs[1];
    parm3 = llvmargs[2];

    llvm::BasicBlock* bb_entry = &jitted_memcmp->getEntryBlock();
    DEFINE_BLOCK(bb_loopBody, jitted_memcmp);
    DEFINE_BLOCK(bb_loopEnd, jitted_memcmp);
    DEFINE_BLOCK(bb_loopExit, jitted_memcmp);
    DEFINE_BLOCK(bb_ret, jitted_memcmp);

    /*
     * start codegen in entry basic block : once the length is zero, do not need to compare
     * them, and we could return back with result 0(equal).
     */
    tmpval = builder.CreateICmpEQ(parm3, int32_0);
    builder.CreateCondBr(tmpval, bb_ret, bb_loopBody);

    /* compare them one byte by one byte, until end or they are different */
    builder.SetInsertPoint(bb_loopBody);
    llvm::PHINode* Phi_data1 = builder.CreatePHI(int8PtrType, 2);
    llvm::PHINode* Phi_data2 = builder.CreatePHI(int8PtrType, 2);
    llvm::PHINode* Phi_len = builder.CreatePHI(int32Type, 2);
    Phi_data1->addIncoming(parm1, bb_entry);
    Phi_data2->addIncoming(parm2, bb_entry);
    Phi_len->addIncoming(parm3, bb_entry);
    data1 = builder.CreateLoad(int8Type, Phi_data1, "data1");
    data2 = builder.CreateLoad(int8Type, Phi_data2, "data2");
    tmpval = builder.CreateICmpEQ(data1, data2);
    builder.CreateCondBr(tmpval, bb_loopEnd, bb_loopExit);

    /* compare the value directly */
    builder.SetInsertPoint(bb_loopExit);
    /* Use unsigned comparison (should not use signed one) */
    tmpval = builder.CreateICmpUGT(data1, data2);
    llvm::Value* result1 = builder.CreateSelect(tmpval, int32_1, int32_m1);
    builder.CreateBr(bb_ret);

    /* get the new compare strings and length minus one */
    builder.SetInsertPoint(bb_loopEnd);
    llvm::Value* next_data1 = builder.CreateInBoundsGEP(Phi_data1, int64_1);
    llvm::Value* next_data2 = builder.CreateInBoundsGEP(Phi_data2, int64_1);
    Phi_data1->addIncoming(next_data1, bb_loopEnd);
    Phi_data2->addIncoming(next_data2, bb_loopEnd);
    llvm::Value* len_less1 = builder.CreateAdd(Phi_len, int32_m1);
    Phi_len->addIncoming(len_less1, bb_loopEnd);
    tmpval = builder.CreateICmpEQ(len_less1, int32_0);
    builder.CreateCondBr(tmpval, bb_ret, bb_loopBody);

    builder.SetInsertPoint(bb_ret);
    llvm::PHINode* Phi_result = builder.CreatePHI(int32Type, 3);
    Phi_result->addIncoming(int32_0, bb_entry);
    Phi_result->addIncoming(result1, bb_loopExit);
    Phi_result->addIncoming(int32_0, bb_loopEnd);
    builder.CreateRet(Phi_result);

    llvmCodeGen->FinalizeFunction(jitted_memcmp);
    return jitted_memcmp;
}

/* @Description	: Codegen on bpcharcmp function with long bpchar length
 * (the length is greater than 16 bytes). We inline the pg_detoast_datum_packed,
 * bcTruelen and varstr_cmp (jittable have checked lc_collate_is_c is true).
 * Only Support lc_collate_is_c is true, or need to call a wrapped function
 * of varstr_cmp, which will be fixed in the future.
 * @Notes	: Different from bpcharcmpCodeGen_short function, the inlined
 * bcTruelen function has the following optimized code:
 * bcTruelen(char *a, int len)
 * {
 *		while (len >= 8)
 *		{
 *			uint64 t = *((uint64*)(a+len-8));
 *			if ( t != 0x2020202020202020)
 *			{
 *				uint32 tt = t >> 32;
 *				if (tt == 0x20202020)
 *				{
 *					len -= 4;
 *					break;
 *				}
 *			}
 *			len -= 8;
 *		}
 *      for (int i = len - 1; i >= 0; i--)
 *      {
 *			if (a[i] != ' ')
 *				break;
 *      }
 *		return i+1;
 * }
 */
llvm::Function* VecSortCodeGen::bpcharcmpCodeGen_long()
{
    Assert(NULL != (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj);
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    llvm::Value* tmpval = NULL;
    llvm::Value* llvmargs[3];
    llvm::Value* parm1 = NULL;
    llvm::Value* parm2 = NULL;
    llvm::Value* parm3 = NULL;
    llvm::Value* value1 = NULL;
    llvm::Value* value2 = NULL;

    DEFINE_CG_VOIDTYPE(voidType);
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(int64PtrType, INT8OID);
    DEFINE_CG_PTRTYPE(varattrib_1bPtrType, "struct.varattrib_1b");
    DEFINE_CG_PTRTYPE(varlenaPtrType, "struct.varlena");
    llvm::Type* int8ArrayType = llvm::ArrayType::get(int8Type, 1);
    llvm::Type* int8ArrayPtrType = llvmCodeGen->getPtrType(int8ArrayType);

    DEFINE_CGVAR_INT8(int8_0, 0);
    DEFINE_CGVAR_INT8(int8_1, 1);
    DEFINE_CGVAR_INT8(int8_2, 2);
    DEFINE_CGVAR_INT8(int8_3, 3);
    DEFINE_CGVAR_INT8(int8_32, 32);
    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT32(int32_4, 4);
    DEFINE_CGVAR_INT32(int32_7, 7);
    DEFINE_CGVAR_INT32(int32_8, 8);
    DEFINE_CGVAR_INT32(int32_m1, -1);
    DEFINE_CGVAR_INT64(int64_0, 0);
    DEFINE_CGVAR_INT64(int64_1, 1);
    DEFINE_CGVAR_INT64(int64_8, 8);
    DEFINE_CGVAR_INT64(int64_2020202020202020X, 0x2020202020202020);
    DEFINE_CGVAR_INT64(int64_FFFFFFFF00000000X, 0xFFFFFFFF00000000);
    DEFINE_CGVAR_INT64(int64_2020202000000000X, 0x2020202000000000);

    llvm::Value* Vals2[2] = {int64_0, int32_0};
    llvm::Value* Vals3[3] = {int64_0, int32_0, int64_0};

    /* the prototype of the jitted function for bpcharcmp
     * Three parameters: (1) datum a, (2) datum b, (3) int32 length
     */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMIRbpcharcmp_long", int32Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("a", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("b", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("length", int32Type));
    llvm::Function* jitted_bpcharcmp = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    parm1 = llvmargs[0];
    parm2 = llvmargs[1];
    parm3 = llvmargs[2];

    llvm::BasicBlock* bb_entry = &jitted_bpcharcmp->getEntryBlock();
    DEFINE_BLOCK(bb_untoast1, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_untoast2, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_nextVal, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_data1, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_longHeader1, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_shortHeader1, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_data2, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_longHeader2, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_shortHeader2, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_prehead_a1, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_loop_len_a1, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_loopBody_a1, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_loopEnd_a1, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_check_int32_a, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_prehead_a2, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_loopBody_a2, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_loopEnd_a2, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_prehead_b1, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_loop_len_b1, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_loopBody_b1, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_loopEnd_b1, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_check_int32_b, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_prehead_b2, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_loopBody_b2, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_loopEnd_b2, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_varstr_cmp, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_update_result, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_ret, jitted_bpcharcmp);

    DEFINE_BLOCK(bb_pfree1, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_check_pfree, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_pfree2, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_ret2, jitted_bpcharcmp);

    /* Start codegen on entry basic block.
     * First, get the input argument by calling PG_GETARG_BPCHAR_PP.
     *
     * pg_detoast_datum_packed((struct varlena *) DatumGetPointer(datum)):
     * if (VARATT_IS_COMPRESSED(datum) || VARATT_IS_EXTERNAL(datum))
     *		return heap_tuple_untoast_attr(datum);
     * else
     *		return datum;
     */
    value1 = builder.CreateIntToPtr(parm1, varattrib_1bPtrType);
    value1 = builder.CreateInBoundsGEP(value1, Vals2);
    value1 = builder.CreateLoad(int8Type, value1, "header1");
    tmpval = builder.CreateAnd(value1, int8_3);
    llvm::Value* isCompressed = builder.CreateICmpEQ(tmpval, int8_2);
    llvm::Value* isExternal = builder.CreateICmpEQ(value1, int8_1);
    value1 = builder.CreateIntToPtr(parm1, varlenaPtrType);
    tmpval = builder.CreateOr(isCompressed, isExternal);
    builder.CreateCondBr(tmpval, bb_untoast1, bb_nextVal);

    /* call heap_tuple_untoast_attr for arg1 */
    builder.SetInsertPoint(bb_untoast1);
    llvm::Function* wrap_untoast = llvmCodeGen->module()->getFunction("LLVMIRwrapuntoast");
    if (wrap_untoast == NULL) {
        GsCodeGen::FnPrototype func_prototype(llvmCodeGen, "LLVMIRwrapuntoast", varlenaPtrType);
        func_prototype.addArgument(GsCodeGen::NamedVariable("datum", varlenaPtrType));
        wrap_untoast = func_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMIRwrapuntoast", (void*)Wrap_heap_tuple_untoast_attr);
    }
    llvm::Value* untoast_value1 = builder.CreateCall(wrap_untoast, value1);
    builder.CreateBr(bb_nextVal);

    /* get arg1 and deal with pg_detoast_datum_packed(arg2) */
    builder.SetInsertPoint(bb_nextVal);
    llvm::PHINode* Phi_arg1 = builder.CreatePHI(varlenaPtrType, 2);
    Phi_arg1->addIncoming(value1, bb_entry);
    Phi_arg1->addIncoming(untoast_value1, bb_untoast1);

    value2 = builder.CreateIntToPtr(parm2, varattrib_1bPtrType);
    value2 = builder.CreateInBoundsGEP(value2, Vals2);
    value2 = builder.CreateLoad(int8Type, value2, "header2");
    tmpval = builder.CreateAnd(value2, int8_3);
    isCompressed = builder.CreateICmpEQ(tmpval, int8_2);
    isExternal = builder.CreateICmpEQ(value2, int8_1);
    value2 = builder.CreateIntToPtr(parm2, varlenaPtrType);
    tmpval = builder.CreateOr(isCompressed, isExternal);
    builder.CreateCondBr(tmpval, bb_untoast2, bb_data1);

    /* call heap_tuple_untoast_attr for arg2 */
    builder.SetInsertPoint(bb_untoast2);
    llvm::Value* untoast_value2 = builder.CreateCall(wrap_untoast, value2);
    builder.CreateBr(bb_data1);

    /* char *s = VARDATA_ANY(arg1):
     * (VARATT_IS_1B(PTR) ? VARDATA_1B(PTR) : VARDATA_4B(PTR))
     */
    builder.SetInsertPoint(bb_data1);
    llvm::PHINode* Phi_arg2 = builder.CreatePHI(varlenaPtrType, 2);
    Phi_arg2->addIncoming(value2, bb_nextVal);
    Phi_arg2->addIncoming(untoast_value2, bb_untoast2);

    tmpval = builder.CreateInBoundsGEP(Phi_arg1, Vals3);
    tmpval = builder.CreateLoad(int8Type, tmpval, "header1");
    tmpval = builder.CreateAnd(tmpval, int8_1);
    tmpval = builder.CreateICmpEQ(tmpval, int8_0);
    builder.CreateCondBr(tmpval, bb_longHeader1, bb_shortHeader1);

    /* VARATT_IS_1B(arg1) is true */
    builder.SetInsertPoint(bb_shortHeader1);
    Vals3[2] = int64_1;
    tmpval = builder.CreateInBoundsGEP(Phi_arg1, Vals3);
    llvm::Value* data1_short = builder.CreateBitCast(tmpval, int8ArrayPtrType);
    builder.CreateBr(bb_data2);

    /* VARATT_IS_1B(arg1) is false */
    builder.SetInsertPoint(bb_longHeader1);
    Vals2[1] = int32_1;
    llvm::Value* data1_long = builder.CreateInBoundsGEP(Phi_arg1, Vals2);
    builder.CreateBr(bb_data2);

    /* finish VARDATA_ANY(arg1) and deal with VARDATA_ANY(arg2) */
    builder.SetInsertPoint(bb_data2);
    llvm::PHINode* Phi_data1 = builder.CreatePHI(int8ArrayPtrType, 2);
    Phi_data1->addIncoming(data1_short, bb_shortHeader1);
    Phi_data1->addIncoming(data1_long, bb_longHeader1);

    Vals3[2] = int64_0;
    tmpval = builder.CreateInBoundsGEP(Phi_arg2, Vals3);
    tmpval = builder.CreateLoad(int8Type, tmpval, "header2");
    tmpval = builder.CreateAnd(tmpval, int8_1);
    tmpval = builder.CreateICmpEQ(tmpval, int8_0);
    builder.CreateCondBr(tmpval, bb_longHeader2, bb_shortHeader2);

    /* VARATT_IS_1B(arg2) is true */
    builder.SetInsertPoint(bb_shortHeader2);
    Vals3[2] = int64_1;
    tmpval = builder.CreateInBoundsGEP(Phi_arg2, Vals3);
    llvm::Value* data2_short = builder.CreateBitCast(tmpval, int8ArrayPtrType);
    builder.CreateBr(bb_prehead_a1);

    /* VARATT_IS_1B(arg2) is false */
    builder.SetInsertPoint(bb_longHeader2);
    Vals2[1] = int32_1;
    llvm::Value* data2_long = builder.CreateInBoundsGEP(Phi_arg2, Vals2);
    builder.CreateBr(bb_prehead_a1);

    /*
     * Corresponding to for loop in bcTurelen:
     * for (i = len - 1; i >= 0; i--)
     *		if (s[i] != ' ') break;
     * return i + 1;
     */
    builder.SetInsertPoint(bb_prehead_a1);
    /* get VARDATA_ANY(arg2) */
    llvm::PHINode* Phi_data2 = builder.CreatePHI(int8ArrayPtrType, 2);
    Phi_data2->addIncoming(data2_short, bb_shortHeader2);
    Phi_data2->addIncoming(data2_long, bb_longHeader2);

    tmpval = builder.CreateICmpSGT(parm3, int32_7);
    builder.CreateCondBr(tmpval, bb_loop_len_a1, bb_prehead_a2);

    builder.SetInsertPoint(bb_loop_len_a1);
    llvm::Value* loopIdx = builder.CreateSExt(parm3, int64Type);
    builder.CreateBr(bb_loopBody_a1);

    /* calculate len1 */
    builder.SetInsertPoint(bb_loopBody_a1);
    llvm::PHINode* Phi_loopIdx = builder.CreatePHI(int64Type, 2);
    llvm::PHINode* Phi_len1 = builder.CreatePHI(int32Type, 2);
    Phi_loopIdx->addIncoming(loopIdx, bb_loop_len_a1);
    llvm::Value* remaining = NULL;
    remaining = builder.CreateSub(Phi_loopIdx, int64_8);
    Phi_loopIdx->addIncoming(remaining, bb_loopEnd_a1);
    Phi_len1->addIncoming(parm3, bb_loop_len_a1);
    llvm::Value* len_loopBody_a1 = NULL;
    len_loopBody_a1 = builder.CreateSub(Phi_len1, int32_8);
    Phi_len1->addIncoming(len_loopBody_a1, bb_loopEnd_a1);

    Vals2[1] = remaining;
    tmpval = builder.CreateInBoundsGEP(Phi_data1, Vals2);
    llvm::Value* val_int64 = NULL;
    tmpval = builder.CreateBitCast(tmpval, int64PtrType);
    val_int64 = builder.CreateLoad(int64Type, tmpval, "s1_int64");
    tmpval = builder.CreateICmpEQ(val_int64, int64_2020202020202020X);
    builder.CreateCondBr(tmpval, bb_loopEnd_a1, bb_check_int32_a);

    builder.SetInsertPoint(bb_loopEnd_a1);
    tmpval = builder.CreateTrunc(remaining, int32Type);
    tmpval = builder.CreateICmpSGT(tmpval, int32_7);
    builder.CreateCondBr(tmpval, bb_loopBody_a1, bb_prehead_a2);

    builder.SetInsertPoint(bb_check_int32_a);
    llvm::Value* len_check_int32_a = NULL;
    tmpval = builder.CreateAnd(val_int64, int64_FFFFFFFF00000000X);
    tmpval = builder.CreateICmpEQ(tmpval, int64_2020202000000000X);
    len_check_int32_a = builder.CreateSub(Phi_len1, int32_4);
    len_check_int32_a = builder.CreateSelect(tmpval, len_check_int32_a, Phi_len1);
    builder.CreateBr(bb_prehead_a2);

    builder.SetInsertPoint(bb_prehead_a2);
    llvm::PHINode* Phi_len2 = builder.CreatePHI(int32Type, 3);
    Phi_len2->addIncoming(parm3, bb_prehead_a1);
    Phi_len2->addIncoming(len_loopBody_a1, bb_loopEnd_a1);
    Phi_len2->addIncoming(len_check_int32_a, bb_check_int32_a);
    loopIdx = builder.CreateSExt(Phi_len2, int64Type);
    builder.CreateBr(bb_loopBody_a2);

    builder.SetInsertPoint(bb_loopBody_a2);
    Phi_loopIdx = builder.CreatePHI(int64Type, 2);
    llvm::PHINode* Phi_len_a = builder.CreatePHI(int32Type, 2);
    Phi_loopIdx->addIncoming(loopIdx, bb_prehead_a2);
    remaining = builder.CreateSub(Phi_loopIdx, int64_1);
    Phi_loopIdx->addIncoming(remaining, bb_loopEnd_a2);
    Phi_len_a->addIncoming(Phi_len2, bb_prehead_a2);
    tmpval = builder.CreateTrunc(Phi_loopIdx, int32Type);
    tmpval = builder.CreateICmpSGT(tmpval, int32_0);
    builder.CreateCondBr(tmpval, bb_loopEnd_a2, bb_prehead_b1);

    builder.SetInsertPoint(bb_loopEnd_a2);
    llvm::Value* len_less1 = builder.CreateSub(Phi_len_a, int32_1);
    Phi_len_a->addIncoming(len_less1, bb_loopEnd_a2);
    Vals2[1] = remaining;
    tmpval = builder.CreateInBoundsGEP(Phi_data1, Vals2);
    tmpval = builder.CreateLoad(int8Type, tmpval, "a_i");
    tmpval = builder.CreateICmpEQ(tmpval, int8_32);
    builder.CreateCondBr(tmpval, bb_loopBody_a2, bb_prehead_b1);

    llvm::Value* len_less4 = NULL;
    llvm::Value* len_less8 = NULL;

    /* calculate len2 = bcTruelen(arg2) */
    builder.SetInsertPoint(bb_prehead_b1);
    tmpval = builder.CreateICmpSGT(parm3, int32_7);
    builder.CreateCondBr(tmpval, bb_loop_len_b1, bb_prehead_b2);

    builder.SetInsertPoint(bb_loop_len_b1);
    loopIdx = builder.CreateSExt(parm3, int64Type);
    builder.CreateBr(bb_loopBody_b1);

    builder.SetInsertPoint(bb_loopBody_b1);
    Phi_loopIdx = builder.CreatePHI(int64Type, 2);
    Phi_len1 = builder.CreatePHI(int32Type, 2);
    Phi_loopIdx->addIncoming(loopIdx, bb_loop_len_b1);
    remaining = builder.CreateSub(Phi_loopIdx, int64_8);
    Phi_loopIdx->addIncoming(remaining, bb_loopEnd_b1);
    Phi_len1->addIncoming(parm3, bb_loop_len_b1);
    len_less8 = builder.CreateSub(Phi_len1, int32_8);
    Phi_len1->addIncoming(len_less8, bb_loopEnd_b1);

    Vals2[1] = remaining;
    tmpval = builder.CreateInBoundsGEP(Phi_data2, Vals2);
    tmpval = builder.CreateBitCast(tmpval, int64PtrType);
    val_int64 = builder.CreateLoad(int64Type, tmpval, "s2_int64");
    tmpval = builder.CreateICmpEQ(val_int64, int64_2020202020202020X);
    builder.CreateCondBr(tmpval, bb_loopEnd_b1, bb_check_int32_b);

    builder.SetInsertPoint(bb_loopEnd_b1);
    tmpval = builder.CreateTrunc(remaining, int32Type);
    tmpval = builder.CreateICmpSGT(tmpval, int32_7);
    builder.CreateCondBr(tmpval, bb_loopBody_b1, bb_prehead_b2);

    builder.SetInsertPoint(bb_check_int32_b);
    tmpval = builder.CreateAnd(val_int64, int64_FFFFFFFF00000000X);
    tmpval = builder.CreateICmpEQ(tmpval, int64_2020202000000000X);
    len_less4 = builder.CreateSub(Phi_len1, int32_4);
    len_less4 = builder.CreateSelect(tmpval, len_less4, Phi_len1);
    builder.CreateBr(bb_prehead_b2);

    builder.SetInsertPoint(bb_prehead_b2);
    Phi_len2 = builder.CreatePHI(int32Type, 3);
    Phi_len2->addIncoming(parm3, bb_prehead_b1);
    Phi_len2->addIncoming(len_less8, bb_loopEnd_b1);
    Phi_len2->addIncoming(len_less4, bb_check_int32_b);
    loopIdx = builder.CreateSExt(Phi_len2, int64Type);
    builder.CreateBr(bb_loopBody_b2);

    builder.SetInsertPoint(bb_loopBody_b2);
    Phi_loopIdx = builder.CreatePHI(int64Type, 2);
    llvm::PHINode* Phi_len_b = builder.CreatePHI(int32Type, 2);
    Phi_loopIdx->addIncoming(loopIdx, bb_prehead_b2);
    remaining = builder.CreateSub(Phi_loopIdx, int64_1);
    Phi_loopIdx->addIncoming(remaining, bb_loopEnd_b2);
    Phi_len_b->addIncoming(Phi_len2, bb_prehead_b2);
    tmpval = builder.CreateTrunc(Phi_loopIdx, int32Type);
    tmpval = builder.CreateICmpSGT(tmpval, int32_0);
    builder.CreateCondBr(tmpval, bb_loopEnd_b2, bb_varstr_cmp);

    builder.SetInsertPoint(bb_loopEnd_b2);
    len_less1 = builder.CreateSub(Phi_len_b, int32_1);
    Phi_len_b->addIncoming(len_less1, bb_loopEnd_b2);
    Vals2[1] = remaining;
    tmpval = builder.CreateInBoundsGEP(Phi_data2, Vals2);
    tmpval = builder.CreateLoad(int8Type, tmpval, "b_i");
    tmpval = builder.CreateICmpEQ(tmpval, int8_32);
    builder.CreateCondBr(tmpval, bb_loopBody_b2, bb_varstr_cmp);

    /* do compare = varstr_cmp(*s1, len1, *s2, len2, collation) */
    builder.SetInsertPoint(bb_varstr_cmp);
    Vals2[1] = int64_0;
    llvm::Value* val_data1 = builder.CreateInBoundsGEP(Phi_data1, Vals2);
    llvm::Value* val_data2 = builder.CreateInBoundsGEP(Phi_data2, Vals2);
    llvm::Value* smaller_len1 = builder.CreateICmpSLT(Phi_len_a, Phi_len_b);
    llvm::Value* min_len = builder.CreateSelect(smaller_len1, Phi_len_a, Phi_len_b);

    /* since collection is C, we only need to compare them according to the
     * smaller length by memcmp.
     */
    llvm::Function* jitted_memcmp = llvmCodeGen->module()->getFunction("LLVMIRmemcmp_CMC");
    if (jitted_memcmp == NULL) {
        jitted_memcmp = LLVMIRmemcmp_CMC_CodeGen();
    }

    llvm::Value* compare = builder.CreateCall(jitted_memcmp, {val_data1, val_data2, min_len});
    tmpval = builder.CreateICmpEQ(compare, int32_0);
    llvm::Value* tmpval2 = builder.CreateICmpNE(Phi_len_a, Phi_len_b);
    tmpval = builder.CreateAnd(tmpval, tmpval2);
    builder.CreateCondBr(tmpval, bb_update_result, bb_ret);

    /*
     * if compare == 0, the longer one win, which means:
     * if (compare == 0) && (len1 != len2), compare result is
     * 	(len1 < len2) ? -1 : 1;
     */
    builder.SetInsertPoint(bb_update_result);
    llvm::Value* updated_res = builder.CreateSelect(smaller_len1, int32_m1, int32_1);
    builder.CreateBr(bb_ret);

    builder.SetInsertPoint(bb_ret);
    llvm::PHINode* Phi_compare = builder.CreatePHI(int32Type, 2);
    Phi_compare->addIncoming(compare, bb_varstr_cmp);
    Phi_compare->addIncoming(updated_res, bb_update_result);

    value1 = builder.CreatePtrToInt(Phi_arg1, int64Type);
    tmpval = builder.CreateICmpEQ(parm1, value1);
    builder.CreateCondBr(tmpval, bb_check_pfree, bb_pfree1);

    /* PG_FREE_IF_COPY(arg1) */
    builder.SetInsertPoint(bb_pfree1);
    llvm::Function* wrap_pfree = llvmCodeGen->module()->getFunction("LLVMIRwrappfree");
    if (wrap_pfree == NULL) {
        GsCodeGen::FnPrototype func_prototype(llvmCodeGen, "LLVMIRwrappfree", voidType);
        func_prototype.addArgument(GsCodeGen::NamedVariable("ptr", int8PtrType));
        wrap_pfree = func_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMIRwrappfree", (void*)Wrap_pfree);
    }
    tmpval = builder.CreateBitCast(Phi_arg1, int8PtrType);
    builder.CreateCall(wrap_pfree, tmpval);
    builder.CreateBr(bb_ret2);

    builder.SetInsertPoint(bb_check_pfree);
    value2 = builder.CreatePtrToInt(Phi_arg2, int64Type);
    tmpval = builder.CreateICmpEQ(parm2, value2);
    builder.CreateCondBr(tmpval, bb_ret2, bb_pfree2);

    /* PG_FREE_IF_COPY(arg2) */
    builder.SetInsertPoint(bb_pfree2);
    tmpval = builder.CreateBitCast(Phi_arg2, int8PtrType);
    builder.CreateCall(wrap_pfree, tmpval);
    builder.CreateBr(bb_ret2);

    builder.SetInsertPoint(bb_ret2);
    builder.CreateRet(Phi_compare);

    llvmCodeGen->FinalizeFunction(jitted_bpcharcmp);
    return jitted_bpcharcmp;
}

/*
 * @Description	: Codegen on bpcharcmp function with short bpchar length
 * (less than or equal to 16 bytes). We inline the pg_detoast_datum_packed,
 * bcTruelen and varstr_cmp (jittable have checked lc_collate_is_c is true).
 * If lc_collate_is_c is not true, need to call a wrapped function of
 * varstr_cmp. The inlined bcTruelen is different from the long version:
 * bcTruelen(char *a, int len)
 * {
 * 		for (int i = len - 1; i >= 0; i--)
 *		{
 * 			if (s[i] != ' ')
 *				break;
 *		}
 *		return i+1;
 * }
 */
llvm::Function* VecSortCodeGen::bpcharcmpCodeGen_short()
{
    Assert(NULL != (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj);
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    llvm::Value* tmpval = NULL;
    llvm::Value* llvmargs[3];
    llvm::Value* parm1 = NULL;
    llvm::Value* parm2 = NULL;
    llvm::Value* parm3 = NULL;
    llvm::Value* value1 = NULL;
    llvm::Value* value2 = NULL;

    DEFINE_CG_VOIDTYPE(voidType);
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(varattrib_1bPtrType, "struct.varattrib_1b");
    DEFINE_CG_PTRTYPE(varlenaPtrType, "struct.varlena");
    llvm::Type* int8ArrayType = llvm::ArrayType::get(int8Type, 1);
    llvm::Type* int8ArrayPtrType = llvmCodeGen->getPtrType(int8ArrayType);

    DEFINE_CGVAR_INT8(int8_0, 0);
    DEFINE_CGVAR_INT8(int8_1, 1);
    DEFINE_CGVAR_INT8(int8_2, 2);
    DEFINE_CGVAR_INT8(int8_3, 3);
    DEFINE_CGVAR_INT8(int8_32, 32);
    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT32(int32_m1, -1);
    DEFINE_CGVAR_INT64(int64_0, 0);
    DEFINE_CGVAR_INT64(int64_1, 1);

    llvm::Value* Vals2[2] = {int64_0, int32_0};
    llvm::Value* Vals3[3] = {int64_0, int32_0, int64_0};

    /* the prototype of the jitted function for bpcharcmp
     * Three parameters: (1) datum a, (2) datum b, (3) int32 length
     */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMIRbpcharcmp_short", int32Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("a", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("b", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("length", int32Type));
    llvm::Function* jitted_bpcharcmp = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    parm1 = llvmargs[0];
    parm2 = llvmargs[1];
    parm3 = llvmargs[2];

    llvm::BasicBlock* bb_entry = &jitted_bpcharcmp->getEntryBlock();
    DEFINE_BLOCK(bb_untoast1, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_untoast2, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_nextVal, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_data1, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_longHeader1, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_shortHeader1, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_data2, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_longHeader2, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_shortHeader2, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_prehead_a, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_loopBody_a, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_loopEnd_a, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_prehead_b, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_loopBody_b, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_loopEnd_b, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_varstr_cmp, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_update_result, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_ret, jitted_bpcharcmp);

    DEFINE_BLOCK(bb_pfree1, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_check_pfree, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_pfree2, jitted_bpcharcmp);
    DEFINE_BLOCK(bb_ret2, jitted_bpcharcmp);

    /* Start codegen on entry basic block.
     * First, get the input argument by calling PG_GETARG_BPCHAR_PP.
     *
     * pg_detoast_datum_packed((struct varlena *) DatumGetPointer(datum)):
     * if (VARATT_IS_COMPRESSED(datum) || VARATT_IS_EXTERNAL(datum))
     *		return heap_tuple_untoast_attr(datum);
     * else
     *		return datum;
     */
    value1 = builder.CreateIntToPtr(parm1, varattrib_1bPtrType);
    value1 = builder.CreateInBoundsGEP(value1, Vals2);
    value1 = builder.CreateLoad(int8Type, value1, "header1");
    tmpval = builder.CreateAnd(value1, int8_3);
    llvm::Value* isCompressed = builder.CreateICmpEQ(tmpval, int8_2);
    llvm::Value* isExternal = builder.CreateICmpEQ(value1, int8_1);
    value1 = builder.CreateIntToPtr(parm1, varlenaPtrType);
    tmpval = builder.CreateOr(isCompressed, isExternal);
    builder.CreateCondBr(tmpval, bb_untoast1, bb_nextVal);

    /* call heap_tuple_untoast_attr for arg1 */
    builder.SetInsertPoint(bb_untoast1);
    llvm::Function* wrap_untoast = llvmCodeGen->module()->getFunction("LLVMIRwrapuntoast");
    if (wrap_untoast == NULL) {
        GsCodeGen::FnPrototype func_prototype(llvmCodeGen, "LLVMIRwrapuntoast", varlenaPtrType);
        func_prototype.addArgument(GsCodeGen::NamedVariable("datum", varlenaPtrType));
        wrap_untoast = func_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMIRwrapuntoast", (void*)Wrap_heap_tuple_untoast_attr);
    }
    llvm::Value* untoast_value1 = builder.CreateCall(wrap_untoast, value1);
    builder.CreateBr(bb_nextVal);

    /* get arg1 and deal with pg_detoast_datum_packed(arg2) */
    builder.SetInsertPoint(bb_nextVal);
    llvm::PHINode* Phi_arg1 = builder.CreatePHI(varlenaPtrType, 2);
    Phi_arg1->addIncoming(value1, bb_entry);
    Phi_arg1->addIncoming(untoast_value1, bb_untoast1);

    value2 = builder.CreateIntToPtr(parm2, varattrib_1bPtrType);
    value2 = builder.CreateInBoundsGEP(value2, Vals2);
    value2 = builder.CreateLoad(int8Type, value2, "header2");
    tmpval = builder.CreateAnd(value2, int8_3);
    isCompressed = builder.CreateICmpEQ(tmpval, int8_2);
    isExternal = builder.CreateICmpEQ(value2, int8_1);
    value2 = builder.CreateIntToPtr(parm2, varlenaPtrType);
    tmpval = builder.CreateOr(isCompressed, isExternal);
    builder.CreateCondBr(tmpval, bb_untoast2, bb_data1);

    /* call heap_tuple_untoast_attr for arg2 */
    builder.SetInsertPoint(bb_untoast2);
    llvm::Value* untoast_value2 = builder.CreateCall(wrap_untoast, value2);
    builder.CreateBr(bb_data1);

    /* char *s = VARDATA_ANY(arg1):
     * (VARATT_IS_1B(PTR) ? VARDATA_1B(PTR) : VARDATA_4B(PTR))
     */
    builder.SetInsertPoint(bb_data1);
    llvm::PHINode* Phi_arg2 = builder.CreatePHI(varlenaPtrType, 2);
    Phi_arg2->addIncoming(value2, bb_nextVal);
    Phi_arg2->addIncoming(untoast_value2, bb_untoast2);

    tmpval = builder.CreateInBoundsGEP(Phi_arg1, Vals3);
    tmpval = builder.CreateLoad(int8Type, tmpval, "header1");
    tmpval = builder.CreateAnd(tmpval, int8_1);
    tmpval = builder.CreateICmpEQ(tmpval, int8_0);
    builder.CreateCondBr(tmpval, bb_longHeader1, bb_shortHeader1);

    /* VARATT_IS_1B(arg1) is true */
    builder.SetInsertPoint(bb_shortHeader1);
    Vals3[2] = int64_1;
    tmpval = builder.CreateInBoundsGEP(Phi_arg1, Vals3);
    llvm::Value* data1_short = builder.CreateBitCast(tmpval, int8ArrayPtrType);
    builder.CreateBr(bb_data2);

    /* VARATT_IS_1B(arg1) is false */
    builder.SetInsertPoint(bb_longHeader1);
    Vals2[1] = int32_1;
    llvm::Value* data1_long = builder.CreateInBoundsGEP(Phi_arg1, Vals2);
    builder.CreateBr(bb_data2);

    builder.SetInsertPoint(bb_data2);
    /* finish VARDATA_ANY(arg1) */
    llvm::PHINode* Phi_data1 = builder.CreatePHI(int8ArrayPtrType, 2);
    Phi_data1->addIncoming(data1_short, bb_shortHeader1);
    Phi_data1->addIncoming(data1_long, bb_longHeader1);

    Vals3[2] = int64_0;
    tmpval = builder.CreateInBoundsGEP(Phi_arg2, Vals3);
    tmpval = builder.CreateLoad(int8Type, tmpval, "header2");
    tmpval = builder.CreateAnd(tmpval, int8_1);
    tmpval = builder.CreateICmpEQ(tmpval, int8_0);
    builder.CreateCondBr(tmpval, bb_longHeader2, bb_shortHeader2);

    /* VARATT_IS_1B(arg2) is true */
    builder.SetInsertPoint(bb_shortHeader2);
    Vals3[2] = int64_1;
    tmpval = builder.CreateInBoundsGEP(Phi_arg2, Vals3);
    llvm::Value* data2_short = builder.CreateBitCast(tmpval, int8ArrayPtrType);
    builder.CreateBr(bb_prehead_a);

    /* VARATT_IS_1B(arg2) is false */
    builder.SetInsertPoint(bb_longHeader2);
    Vals2[1] = int32_1;
    llvm::Value* data2_long = builder.CreateInBoundsGEP(Phi_arg2, Vals2);
    builder.CreateBr(bb_prehead_a);

    /* len1 = bcTruelen(data1) */
    builder.SetInsertPoint(bb_prehead_a);
    /* finish VARDATA_ANY(arg2) */
    llvm::PHINode* Phi_data2 = builder.CreatePHI(int8ArrayPtrType, 2);
    Phi_data2->addIncoming(data2_short, bb_shortHeader2);
    Phi_data2->addIncoming(data2_long, bb_longHeader2);

    llvm::Value* loopIdx = builder.CreateSExt(parm3, int64Type);
    builder.CreateBr(bb_loopBody_a);

    /*
     * Corresponding to for loop in bcTurelen:
     * for (i = len - 1; i >= 0; i--)
     *		if (s[i] != ' ') break;
     * return i + 1;
     */
    builder.SetInsertPoint(bb_loopBody_a);
    llvm::PHINode* Phi_loopIdx = builder.CreatePHI(int64Type, 2);
    llvm::PHINode* Phi_len_a = builder.CreatePHI(int32Type, 2);
    Phi_loopIdx->addIncoming(loopIdx, bb_prehead_a);
    llvm::Value* remaining = builder.CreateSub(Phi_loopIdx, int64_1);
    Phi_loopIdx->addIncoming(remaining, bb_loopEnd_a);

    Phi_len_a->addIncoming(parm3, bb_prehead_a);
    tmpval = builder.CreateTrunc(Phi_loopIdx, int32Type);
    tmpval = builder.CreateICmpSGT(tmpval, int32_0);
    builder.CreateCondBr(tmpval, bb_loopEnd_a, bb_prehead_b);

    builder.SetInsertPoint(bb_loopEnd_a);
    llvm::Value* len_less1 = builder.CreateSub(Phi_len_a, int32_1);
    Phi_len_a->addIncoming(len_less1, bb_loopEnd_a);
    Vals2[1] = remaining;
    /* if s[i] == ' ' */
    tmpval = builder.CreateInBoundsGEP(Phi_data1, Vals2);
    tmpval = builder.CreateLoad(int8Type, tmpval, "a_i");
    tmpval = builder.CreateICmpEQ(tmpval, int8_32);
    builder.CreateCondBr(tmpval, bb_loopBody_a, bb_prehead_b);

    /* len2 = bcTruelen(data2) */
    builder.SetInsertPoint(bb_prehead_b);
    builder.CreateBr(bb_loopBody_b);

    builder.SetInsertPoint(bb_loopBody_b);
    Phi_loopIdx = builder.CreatePHI(int64Type, 2);
    llvm::PHINode* Phi_len_b = builder.CreatePHI(int32Type, 2);
    Phi_loopIdx->addIncoming(loopIdx, bb_prehead_b);
    remaining = builder.CreateSub(Phi_loopIdx, int64_1);
    Phi_loopIdx->addIncoming(remaining, bb_loopEnd_b);
    Phi_len_b->addIncoming(parm3, bb_prehead_b);
    tmpval = builder.CreateTrunc(Phi_loopIdx, int32Type);
    tmpval = builder.CreateICmpSGT(tmpval, int32_0);
    builder.CreateCondBr(tmpval, bb_loopEnd_b, bb_varstr_cmp);

    builder.SetInsertPoint(bb_loopEnd_b);
    len_less1 = builder.CreateSub(Phi_len_b, int32_1);
    Phi_len_b->addIncoming(len_less1, bb_loopEnd_b);
    Vals2[1] = remaining;
    tmpval = builder.CreateInBoundsGEP(Phi_data2, Vals2);
    tmpval = builder.CreateLoad(int8Type, tmpval, "b_i");
    tmpval = builder.CreateICmpEQ(tmpval, int8_32);
    builder.CreateCondBr(tmpval, bb_loopBody_b, bb_varstr_cmp);

    /* compare = varstr_cmp(*s1, len1, *s2, len2, collation) */
    builder.SetInsertPoint(bb_varstr_cmp);
    Vals2[1] = int64_0;
    llvm::Value* val_data1 = builder.CreateInBoundsGEP(Phi_data1, Vals2);
    llvm::Value* val_data2 = builder.CreateInBoundsGEP(Phi_data2, Vals2);
    llvm::Value* smaller_len1 = builder.CreateICmpSLT(Phi_len_a, Phi_len_b);
    llvm::Value* min_len = builder.CreateSelect(smaller_len1, Phi_len_a, Phi_len_b);

    /* compare bpchar according to the smaller length */
    llvm::Function* jitted_memcmp = llvmCodeGen->module()->getFunction("LLVMIRmemcmp_CMC");
    if (jitted_memcmp == NULL) {
        jitted_memcmp = LLVMIRmemcmp_CMC_CodeGen();
    }
    llvm::Value* compare = builder.CreateCall(jitted_memcmp, {val_data1, val_data2, min_len});

    /*
     * if compare == 0, the longer one win, which means:
     * if (compare == 0) && (len1 != len2), compare result is
     * 	(len1 < len2) ? -1 : 1;
     */
    compare = builder.CreateTrunc(compare, int32Type);
    tmpval = builder.CreateICmpEQ(compare, int32_0);
    llvm::Value* tmpval2 = builder.CreateICmpNE(Phi_len_a, Phi_len_b);
    tmpval = builder.CreateAnd(tmpval, tmpval2);
    builder.CreateCondBr(tmpval, bb_update_result, bb_ret);

    builder.SetInsertPoint(bb_update_result);
    llvm::Value* updated_res = builder.CreateSelect(smaller_len1, int32_m1, int32_1);
    builder.CreateBr(bb_ret);

    /* if compare != 0, go to the end */
    builder.SetInsertPoint(bb_ret);
    llvm::PHINode* Phi_compare = builder.CreatePHI(int32Type, 2);
    Phi_compare->addIncoming(compare, bb_varstr_cmp);
    Phi_compare->addIncoming(updated_res, bb_update_result);

    value1 = builder.CreatePtrToInt(Phi_arg1, int64Type);
    tmpval = builder.CreateICmpEQ(parm1, value1);
    builder.CreateCondBr(tmpval, bb_check_pfree, bb_pfree1);

    /* PG_FREE_IF_COPY(arg1) */
    builder.SetInsertPoint(bb_pfree1);
    llvm::Function* wrap_pfree = llvmCodeGen->module()->getFunction("LLVMIRwrappfree");
    if (wrap_pfree == NULL) {
        GsCodeGen::FnPrototype func_prototype(llvmCodeGen, "LLVMIRwrappfree", voidType);
        func_prototype.addArgument(GsCodeGen::NamedVariable("ptr", int8PtrType));
        wrap_pfree = func_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMIRwrappfree", (void*)Wrap_pfree);
    }
    tmpval = builder.CreateBitCast(Phi_arg1, int8PtrType);
    builder.CreateCall(wrap_pfree, tmpval);
    builder.CreateBr(bb_ret2);

    builder.SetInsertPoint(bb_check_pfree);
    value2 = builder.CreatePtrToInt(Phi_arg2, int64Type);
    tmpval = builder.CreateICmpEQ(parm2, value2);
    builder.CreateCondBr(tmpval, bb_ret2, bb_pfree2);

    /* PG_FREE_IF_COPY(arg2) */
    builder.SetInsertPoint(bb_pfree2);
    tmpval = builder.CreateBitCast(Phi_arg2, int8PtrType);
    builder.CreateCall(wrap_pfree, tmpval);
    builder.CreateBr(bb_ret2);

    builder.SetInsertPoint(bb_ret2);
    builder.CreateRet(Phi_compare);

    llvmCodeGen->FinalizeFunction(jitted_bpcharcmp);
    return jitted_bpcharcmp;
}

/*
 * @Description	: Codegen on bttextcmp function called by CompareMultiColumn
 * 				  for text data by inlining pg_detoast_datum_packed
 */
llvm::Function* VecSortCodeGen::textcmpCodeGen()
{
    Assert(NULL != (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj);
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    llvm::Value* tmpval = NULL;
    llvm::Value* llvmargs[3];
    llvm::Value* parm1 = NULL;
    llvm::Value* parm2 = NULL;
    llvm::Value* value1 = NULL;
    llvm::Value* value2 = NULL;

    DEFINE_CG_VOIDTYPE(voidType);
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(int32PtrType, INT4OID);
    DEFINE_CG_PTRTYPE(varattrib_1bPtrType, "struct.varattrib_1b");
    DEFINE_CG_PTRTYPE(varlenaPtrType, "struct.varlena");
    llvm::Type* int8ArrayType = llvm::ArrayType::get(int8Type, 1);
    llvm::Type* int8ArrayPtrType = llvmCodeGen->getPtrType(int8ArrayType);

    DEFINE_CGVAR_INT8(int8_0, 0);
    DEFINE_CGVAR_INT8(int8_1, 1);
    DEFINE_CGVAR_INT8(int8_2, 2);
    DEFINE_CGVAR_INT8(int8_3, 3);
    DEFINE_CGVAR_INT8(int8_7FX, 0x7F);
    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT32(int32_4, 4);
    DEFINE_CGVAR_INT32(int32_m1, -1);
    DEFINE_CGVAR_INT64(int64_0, 0);
    DEFINE_CGVAR_INT64(int64_1, 1);

    llvm::Value* Vals2[2] = {int64_0, int32_0};
    llvm::Value* Vals3[3] = {int64_0, int32_0, int64_0};

    /* the prototype of the jitted function for bttextcmp
     * Three parameters: (1) datum a, (2) datum b
     */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMIRtextcmp_CMC", int32Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("a", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("b", int64Type));
    llvm::Function* jitted_textcmp = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    parm1 = llvmargs[0];
    parm2 = llvmargs[1];

    llvm::BasicBlock* bb_entry = &jitted_textcmp->getEntryBlock();
    DEFINE_BLOCK(bb_untoast1, jitted_textcmp);
    DEFINE_BLOCK(bb_untoast2, jitted_textcmp);
    DEFINE_BLOCK(bb_nextVal, jitted_textcmp);
    DEFINE_BLOCK(bb_data1, jitted_textcmp);
    DEFINE_BLOCK(bb_longHeader1, jitted_textcmp);
    DEFINE_BLOCK(bb_shortHeader1, jitted_textcmp);
    DEFINE_BLOCK(bb_data2, jitted_textcmp);
    DEFINE_BLOCK(bb_longHeader2, jitted_textcmp);
    DEFINE_BLOCK(bb_shortHeader2, jitted_textcmp);
    DEFINE_BLOCK(bb_varstr_cmp, jitted_textcmp);
    DEFINE_BLOCK(bb_update_result, jitted_textcmp);
    DEFINE_BLOCK(bb_ret, jitted_textcmp);
    DEFINE_BLOCK(bb_pfree1, jitted_textcmp);
    DEFINE_BLOCK(bb_check_pfree, jitted_textcmp);
    DEFINE_BLOCK(bb_pfree2, jitted_textcmp);
    DEFINE_BLOCK(bb_ret2, jitted_textcmp);

    /* Start codegen on entry basic block.
     * First, get the input argument by calling PG_GETARG_BPCHAR_PP.
     *
     * pg_detoast_datum_packed((struct varlena *) DatumGetPointer(datum)):
     * if (VARATT_IS_COMPRESSED(datum) || VARATT_IS_EXTERNAL(datum))
     *		return heap_tuple_untoast_attr(datum);
     * else
     *		return datum;
     */
    value1 = builder.CreateIntToPtr(parm1, varattrib_1bPtrType);
    value1 = builder.CreateInBoundsGEP(value1, Vals2);
    value1 = builder.CreateLoad(int8Type, value1, "header1");
    tmpval = builder.CreateAnd(value1, int8_3);
    llvm::Value* isCompressed = builder.CreateICmpEQ(tmpval, int8_2);
    llvm::Value* isExternal = builder.CreateICmpEQ(value1, int8_1);
    value1 = builder.CreateIntToPtr(parm1, varlenaPtrType);
    tmpval = builder.CreateOr(isCompressed, isExternal);
    builder.CreateCondBr(tmpval, bb_untoast1, bb_nextVal);

    /* call heap_tuple_untoast_attr for arg1 */
    builder.SetInsertPoint(bb_untoast1);
    llvm::Function* wrap_untoast = llvmCodeGen->module()->getFunction("LLVMIRWrapuntoast");
    if (wrap_untoast == NULL) {
        GsCodeGen::FnPrototype func_prototype(llvmCodeGen, "LLVMIRWrapuntoast", varlenaPtrType);
        func_prototype.addArgument(GsCodeGen::NamedVariable("datum", varlenaPtrType));
        wrap_untoast = func_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMIRWrapuntoast", (void*)Wrap_heap_tuple_untoast_attr);
    }
    llvm::Value* untoast_value1 = builder.CreateCall(wrap_untoast, value1);
    builder.CreateBr(bb_nextVal);

    /* turn to deal with pg_detoast_datum_packed(arg2) */
    builder.SetInsertPoint(bb_nextVal);
    llvm::PHINode* Phi_arg1 = builder.CreatePHI(varlenaPtrType, 2);
    Phi_arg1->addIncoming(value1, bb_entry);
    Phi_arg1->addIncoming(untoast_value1, bb_untoast1);

    value2 = builder.CreateIntToPtr(parm2, varattrib_1bPtrType);
    value2 = builder.CreateInBoundsGEP(value2, Vals2);
    value2 = builder.CreateLoad(int8Type, value2, "header2");
    tmpval = builder.CreateAnd(value2, int8_3);
    isCompressed = builder.CreateICmpEQ(tmpval, int8_2);
    isExternal = builder.CreateICmpEQ(value2, int8_1);
    value2 = builder.CreateIntToPtr(parm2, varlenaPtrType);
    tmpval = builder.CreateOr(isCompressed, isExternal);
    builder.CreateCondBr(tmpval, bb_untoast2, bb_data1);

    builder.SetInsertPoint(bb_untoast2);
    llvm::Value* untoast_value2 = builder.CreateCall(wrap_untoast, value2);
    builder.CreateBr(bb_data1);

    /* now enter into text_cmp, first deal with VARDATA_ANY(arg1) and
     * VARSIZE_ANY_EXHDR(arg1).
     */
    builder.SetInsertPoint(bb_data1);
    llvm::PHINode* Phi_arg2 = builder.CreatePHI(varlenaPtrType, 2);
    Phi_arg2->addIncoming(value2, bb_nextVal);
    Phi_arg2->addIncoming(untoast_value2, bb_untoast2);

    tmpval = builder.CreateInBoundsGEP(Phi_arg1, Vals3);
    llvm::Value* header1 = builder.CreateLoad(int8Type, tmpval, "header1");
    tmpval = builder.CreateAnd(header1, int8_1);
    tmpval = builder.CreateICmpEQ(tmpval, int8_0);
    builder.CreateCondBr(tmpval, bb_longHeader1, bb_shortHeader1);

    builder.SetInsertPoint(bb_shortHeader1);
    Vals3[2] = int64_1;
    tmpval = builder.CreateInBoundsGEP(Phi_arg1, Vals3);
    llvm::Value* data1_short = builder.CreateBitCast(tmpval, int8ArrayPtrType);
    llvm::Value* len1_short = builder.CreateLShr(header1, 1);
    len1_short = builder.CreateAnd(len1_short, int8_7FX);
    len1_short = builder.CreateSub(len1_short, int8_1);
    len1_short = builder.CreateZExt(len1_short, int32Type);
    builder.CreateBr(bb_data2);

    builder.SetInsertPoint(bb_longHeader1);
    Vals2[1] = int32_1;
    llvm::Value* data1_long = builder.CreateInBoundsGEP(Phi_arg1, Vals2);
    Vals2[1] = int32_0;
    llvm::Value* len1_long = builder.CreateInBoundsGEP(Phi_arg1, Vals2);
    len1_long = builder.CreateBitCast(len1_long, int32PtrType);
    len1_long = builder.CreateLoad(int32Type, len1_long);
    len1_long = builder.CreateLShr(len1_long, 2);
    len1_long = builder.CreateSub(len1_long, int32_4);
    builder.CreateBr(bb_data2);

    /* deal with VARDATA_ANY(arg1) and VARSIZE_ANY_EXHDR(arg1). */
    builder.SetInsertPoint(bb_data2);
    llvm::PHINode* Phi_data1 = builder.CreatePHI(int8ArrayPtrType, 2);
    llvm::PHINode* Phi_len1 = builder.CreatePHI(int32Type, 2);
    Phi_data1->addIncoming(data1_short, bb_shortHeader1);
    Phi_data1->addIncoming(data1_long, bb_longHeader1);
    Phi_len1->addIncoming(len1_short, bb_shortHeader1);
    Phi_len1->addIncoming(len1_long, bb_longHeader1);

    Vals3[2] = int64_0;
    tmpval = builder.CreateInBoundsGEP(Phi_arg2, Vals3);
    llvm::Value* header2 = builder.CreateLoad(int8Type, tmpval, "header2");
    tmpval = builder.CreateAnd(header2, int8_1);
    tmpval = builder.CreateICmpEQ(tmpval, int8_0);
    builder.CreateCondBr(tmpval, bb_longHeader2, bb_shortHeader2);

    builder.SetInsertPoint(bb_shortHeader2);
    Vals3[2] = int64_1;
    tmpval = builder.CreateInBoundsGEP(Phi_arg2, Vals3);
    llvm::Value* data2_short = builder.CreateBitCast(tmpval, int8ArrayPtrType);
    llvm::Value* len2_short = builder.CreateLShr(header2, 1);
    len2_short = builder.CreateAnd(len2_short, int8_7FX);
    len2_short = builder.CreateSub(len2_short, int8_1);
    len2_short = builder.CreateZExt(len2_short, int32Type);
    builder.CreateBr(bb_varstr_cmp);

    builder.SetInsertPoint(bb_longHeader2);
    Vals2[1] = int32_1;
    llvm::Value* data2_long = builder.CreateInBoundsGEP(Phi_arg2, Vals2);
    Vals2[1] = int32_0;
    llvm::Value* len2_long = builder.CreateInBoundsGEP(Phi_arg2, Vals2);
    len2_long = builder.CreateBitCast(len2_long, int32PtrType);
    len2_long = builder.CreateLoad(int32Type, len2_long);
    len2_long = builder.CreateLShr(len2_long, 2);
    len2_long = builder.CreateSub(len2_long, int32_4);
    builder.CreateBr(bb_varstr_cmp);

    /* deal with VARDATA_ANY(arg2) and VARSIZE_ANY_EXHDR(arg2). */
    builder.SetInsertPoint(bb_varstr_cmp);
    llvm::PHINode* Phi_data2 = builder.CreatePHI(int8ArrayPtrType, 2);
    llvm::PHINode* Phi_len2 = builder.CreatePHI(int32Type, 2);
    Phi_data2->addIncoming(data2_short, bb_shortHeader2);
    Phi_data2->addIncoming(data2_long, bb_longHeader2);
    Phi_len2->addIncoming(len2_short, bb_shortHeader2);
    Phi_len2->addIncoming(len2_long, bb_longHeader2);

    /* we only need to compare the smaller length part */
    Vals2[1] = int64_0;
    llvm::Value* val_data1 = builder.CreateInBoundsGEP(Phi_data1, Vals2);
    llvm::Value* val_data2 = builder.CreateInBoundsGEP(Phi_data2, Vals2);
    llvm::Value* smaller_len1 = builder.CreateICmpSLT(Phi_len1, Phi_len2);
    llvm::Value* min_len = builder.CreateSelect(smaller_len1, Phi_len1, Phi_len2);

    llvm::Function* jitted_memcmp = llvmCodeGen->module()->getFunction("LLVMIRmemcmp_CMC");
    if (jitted_memcmp == NULL) {
        jitted_memcmp = LLVMIRmemcmp_CMC_CodeGen();
    }

    llvm::Value* compare = builder.CreateCall(jitted_memcmp, {val_data1, val_data2, min_len});
    compare = builder.CreateTrunc(compare, int32Type);
    tmpval = builder.CreateICmpEQ(compare, int32_0);
    llvm::Value* tmpval2 = builder.CreateICmpNE(Phi_len1, Phi_len2);
    tmpval = builder.CreateAnd(tmpval, tmpval2);
    builder.CreateCondBr(tmpval, bb_update_result, bb_ret);

    /* if ((compare == 0) && (len1 != len2))
     *     compare = (len1 < len2) ? -1 : 1;
     */
    builder.SetInsertPoint(bb_update_result);
    llvm::Value* updated_res = builder.CreateSelect(smaller_len1, int32_m1, int32_1);
    builder.CreateBr(bb_ret);

    builder.SetInsertPoint(bb_ret);
    llvm::PHINode* Phi_compare = builder.CreatePHI(int32Type, 2);
    Phi_compare->addIncoming(compare, bb_varstr_cmp);
    Phi_compare->addIncoming(updated_res, bb_update_result);

    value1 = builder.CreatePtrToInt(Phi_arg1, int64Type);
    tmpval = builder.CreateICmpEQ(parm1, value1);
    builder.CreateCondBr(tmpval, bb_check_pfree, bb_pfree1);

    builder.SetInsertPoint(bb_pfree1);
    llvm::Function* wrap_pfree = llvmCodeGen->module()->getFunction("LLVMIRwrappfree");
    if (wrap_pfree == NULL) {
        GsCodeGen::FnPrototype func_prototype(llvmCodeGen, "LLVMIRwrappfree", voidType);
        func_prototype.addArgument(GsCodeGen::NamedVariable("ptr", int8PtrType));
        wrap_pfree = func_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMIRwrappfree", (void*)Wrap_pfree);
    }
    tmpval = builder.CreateBitCast(Phi_arg1, int8PtrType);
    builder.CreateCall(wrap_pfree, tmpval);
    builder.CreateBr(bb_ret2);

    builder.SetInsertPoint(bb_check_pfree);
    value2 = builder.CreatePtrToInt(Phi_arg2, int64Type);
    tmpval = builder.CreateICmpEQ(parm2, value2);
    builder.CreateCondBr(tmpval, bb_ret2, bb_pfree2);

    builder.SetInsertPoint(bb_pfree2);
    tmpval = builder.CreateBitCast(Phi_arg2, int8PtrType);
    builder.CreateCall(wrap_pfree, tmpval);
    builder.CreateBr(bb_ret2);

    builder.SetInsertPoint(bb_ret2);
    builder.CreateRet(Phi_compare);

    llvmCodeGen->FinalizeFunction(jitted_textcmp);
    return jitted_textcmp;
}

/* @Description	: Codegen on memcmp function that is called by bpchareq
 * 				  function in Sort Aggregation. The generated function is
 *				  optimized for the case where bpchar length is greater
 *				  than or equal to 16 bytes.
 * @Notes		: SortAggMemcmp(char *a, char *b, int length) returns true
 *				  if the two memory blocks have the same data, otherwise,
 *				  it returns false.
 *
 * The optimized memcmp code:
 * bool SortAggMemcmp_long(char *a, char *b, int length)
 * {
 * 		while (length >= 16)
 *		{
 *			int128 t1 = *((int128*)a);
 *			int128 t2 = *((int128*)b);
 *			if (t1 != t2)
 *				return false;
 *			a += 16;
 *			b += 16;
 *			length -= 16;
 * 		}
 *
 *		if (length >= 8)
 *		{
 *			uint64 t1 = *((uint64*)a);
 *			uint64 t2 = *((uint64*)b);
 *			if (t1 != t2)
 *				return false;
 *			a += 8;
 *			b += 8;
 *			length -= 8;
 *		}
 *
 *		if (length >= 4)
 *		{
 *			uint32 t1 = *((uint32*)a);
 *			uint32 t2 = *((uint32*)b);
 *			if (t1 != t2)
 *				return false;
 *			a += 4;
 *			b += 4;
 *			length -= 4;
 *		}
 *
 *		if (length >= 2)
 *		{
 *			uint16 t1 = *((uint16*)a);
 *			uint16 t2 = *((uint16*)b);
 *			if (t1 != t2)
 *				return false;
 *			a += 2;
 *			b += 2;
 *			length -= 2;
 *		}
 *
 *		if (length > 0)
 *		{
 *			if (*a != *b)
 *				return false;
 *		}
 *		return true;
 * }
 */
llvm::Function* VecSortCodeGen::SortAggMemcmpCodeGen_long()
{
    Assert(NULL != (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj);
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    llvm::Value* tmpval = NULL;
    llvm::Value* llvmargs[3];
    llvm::Value* parm1 = NULL;
    llvm::Value* parm2 = NULL;
    llvm::Value* len = NULL;
    llvm::Value* data1 = NULL;
    llvm::Value* data2 = NULL;

    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int16Type, INT2OID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_NINTTYP(int128Type, 128);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(int16PtrType, INT2OID);
    DEFINE_CG_PTRTYPE(int32PtrType, INT4OID);
    DEFINE_CG_PTRTYPE(int64PtrType, INT8OID);
    llvm::Type* int128PtrType = llvm::Type::getIntNPtrTy(context, 128, 0);

    DEFINE_CGVAR_INT8(int8_0, 0);
    DEFINE_CGVAR_INT8(int8_1, 1);
    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT32(int32_2, 2);
    DEFINE_CGVAR_INT32(int32_3, 3);
    DEFINE_CGVAR_INT32(int32_4, 4);
    DEFINE_CGVAR_INT32(int32_7, 7);
    DEFINE_CGVAR_INT32(int32_8, 8);
    DEFINE_CGVAR_INT32(int32_15, 15);
    DEFINE_CGVAR_INT32(int32_16, 16);
    DEFINE_CGVAR_INT64(int64_2, 2);
    DEFINE_CGVAR_INT64(int64_4, 4);
    DEFINE_CGVAR_INT64(int64_8, 8);
    DEFINE_CGVAR_INT64(int64_16, 16);

    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "SortAggMemcmp_long", int8Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("a", int8PtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("b", int8PtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("length", int32Type));
    llvm::Function* jitted_memcmp = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    parm1 = llvmargs[0];
    parm2 = llvmargs[1];
    len = llvmargs[2];

    llvm::BasicBlock* bb_entry = &jitted_memcmp->getEntryBlock();
    DEFINE_BLOCK(bb_loopBody, jitted_memcmp);
    DEFINE_BLOCK(bb_loopEnd, jitted_memcmp);
    DEFINE_BLOCK(bb_ret, jitted_memcmp);
    DEFINE_BLOCK(bb_tryInt64, jitted_memcmp);
    DEFINE_BLOCK(bb_cmpInt64, jitted_memcmp);
    DEFINE_BLOCK(bb_adjustInt64, jitted_memcmp);
    DEFINE_BLOCK(bb_tryInt32, jitted_memcmp);
    DEFINE_BLOCK(bb_cmpInt32, jitted_memcmp);
    DEFINE_BLOCK(bb_adjustInt32, jitted_memcmp);
    DEFINE_BLOCK(bb_tryInt16, jitted_memcmp);
    DEFINE_BLOCK(bb_cmpInt16, jitted_memcmp);
    DEFINE_BLOCK(bb_adjustInt16, jitted_memcmp);
    DEFINE_BLOCK(bb_tryInt8, jitted_memcmp);
    DEFINE_BLOCK(bb_cmpInt8, jitted_memcmp);
    DEFINE_BLOCK(bb_retTrue, jitted_memcmp);

    /* start codegen in entry basic block */
    /* check if the length is greate than 15, try int128 fisrt */
    tmpval = builder.CreateICmpSGT(len, int32_15);
    builder.CreateCondBr(tmpval, bb_loopBody, bb_tryInt64);

    /* loop on int128 comparisons : int128 t1 = *((int128*)a); */
    builder.SetInsertPoint(bb_loopBody);
    llvm::PHINode* Phi_data1 = builder.CreatePHI(int8PtrType, 2);
    llvm::PHINode* Phi_data2 = builder.CreatePHI(int8PtrType, 2);
    llvm::PHINode* Phi_len = builder.CreatePHI(int32Type, 2);
    Phi_data1->addIncoming(parm1, bb_entry);
    Phi_data2->addIncoming(parm2, bb_entry);
    Phi_len->addIncoming(len, bb_entry);
    data1 = builder.CreateBitCast(Phi_data1, int128PtrType);
    data1 = builder.CreateLoad(int128Type, data1, "data1_i128");
    data2 = builder.CreateBitCast(Phi_data2, int128PtrType);
    data2 = builder.CreateLoad(int128Type, data2, "data2_i128");
    tmpval = builder.CreateICmpEQ(data1, data2);
    builder.CreateCondBr(tmpval, bb_loopEnd, bb_ret);

    /* continue the loop or out the loop if remaining is less than 16 bytes */
    builder.SetInsertPoint(bb_loopEnd);
    llvm::Value* next_data1 = builder.CreateInBoundsGEP(Phi_data1, int64_16);
    llvm::Value* next_data2 = builder.CreateInBoundsGEP(Phi_data2, int64_16);
    Phi_data1->addIncoming(next_data1, bb_loopEnd);
    Phi_data2->addIncoming(next_data2, bb_loopEnd);
    llvm::Value* len_less16 = builder.CreateSub(Phi_len, int32_16);
    Phi_len->addIncoming(len_less16, bb_loopEnd);
    tmpval = builder.CreateICmpSGT(len_less16, int32_15);
    builder.CreateCondBr(tmpval, bb_loopBody, bb_tryInt64);

    /* if length is less than 16, try the int64 comparison for remainings */
    builder.SetInsertPoint(bb_tryInt64);
    llvm::PHINode* Phi_data1_tryInt64 = builder.CreatePHI(int8PtrType, 2);
    llvm::PHINode* Phi_data2_tryInt64 = builder.CreatePHI(int8PtrType, 2);
    llvm::PHINode* Phi_len_tryInt64 = builder.CreatePHI(int32Type, 2);
    Phi_data1_tryInt64->addIncoming(parm1, bb_entry);
    Phi_data1_tryInt64->addIncoming(next_data1, bb_loopEnd);
    Phi_data2_tryInt64->addIncoming(parm2, bb_entry);
    Phi_data2_tryInt64->addIncoming(next_data2, bb_loopEnd);
    Phi_len_tryInt64->addIncoming(len, bb_entry);
    Phi_len_tryInt64->addIncoming(len_less16, bb_loopEnd);
    tmpval = builder.CreateICmpSGT(Phi_len_tryInt64, int32_7);
    builder.CreateCondBr(tmpval, bb_cmpInt64, bb_tryInt32);

    /* int64 comparison : compare 8 byte every time */
    builder.SetInsertPoint(bb_cmpInt64);
    data1 = builder.CreateBitCast(Phi_data1_tryInt64, int64PtrType);
    data1 = builder.CreateLoad(int64Type, data1, "data1_i64");
    data2 = builder.CreateBitCast(Phi_data2_tryInt64, int64PtrType);
    data2 = builder.CreateLoad(int64Type, data2, "data2_i64");
    tmpval = builder.CreateICmpEQ(data1, data2);
    builder.CreateCondBr(tmpval, bb_adjustInt64, bb_ret);

    builder.SetInsertPoint(bb_adjustInt64);
    next_data1 = builder.CreateInBoundsGEP(Phi_data1_tryInt64, int64_8);
    next_data2 = builder.CreateInBoundsGEP(Phi_data2_tryInt64, int64_8);
    llvm::Value* len_less8 = builder.CreateSub(Phi_len_tryInt64, int32_8);
    builder.CreateBr(bb_tryInt32);

    /* try the int32 comnparsion for the remaining : uint32 t1 = *((uint32*)a) */
    builder.SetInsertPoint(bb_tryInt32);
    llvm::PHINode* Phi_data1_tryInt32 = builder.CreatePHI(int8PtrType, 2);
    llvm::PHINode* Phi_data2_tryInt32 = builder.CreatePHI(int8PtrType, 2);
    llvm::PHINode* Phi_len_tryInt32 = builder.CreatePHI(int32Type, 2);
    Phi_data1_tryInt32->addIncoming(Phi_data1_tryInt64, bb_tryInt64);
    Phi_data1_tryInt32->addIncoming(next_data1, bb_adjustInt64);
    Phi_data2_tryInt32->addIncoming(Phi_data2_tryInt64, bb_tryInt64);
    Phi_data2_tryInt32->addIncoming(next_data2, bb_adjustInt64);
    Phi_len_tryInt32->addIncoming(Phi_len_tryInt64, bb_tryInt64);
    Phi_len_tryInt32->addIncoming(len_less8, bb_adjustInt64);
    tmpval = builder.CreateICmpSGT(Phi_len_tryInt32, int32_3);
    builder.CreateCondBr(tmpval, bb_cmpInt32, bb_tryInt16);

    /* int32 comparison */
    builder.SetInsertPoint(bb_cmpInt32);
    data1 = builder.CreateBitCast(Phi_data1_tryInt32, int32PtrType);
    data1 = builder.CreateLoad(int32Type, data1, "data1_i32");
    data2 = builder.CreateBitCast(Phi_data2_tryInt32, int32PtrType);
    data2 = builder.CreateLoad(int32Type, data2, "data2_i32");
    tmpval = builder.CreateICmpEQ(data1, data2);
    builder.CreateCondBr(tmpval, bb_adjustInt32, bb_ret);

    builder.SetInsertPoint(bb_adjustInt32);
    next_data1 = builder.CreateInBoundsGEP(Phi_data1_tryInt32, int64_4);
    next_data2 = builder.CreateInBoundsGEP(Phi_data2_tryInt32, int64_4);
    llvm::Value* len_less4 = builder.CreateSub(Phi_len_tryInt32, int32_4);
    builder.CreateBr(bb_tryInt16);

    /* try int16 comparison for the remaining */
    builder.SetInsertPoint(bb_tryInt16);
    llvm::PHINode* Phi_data1_tryInt16 = builder.CreatePHI(int8PtrType, 2);
    llvm::PHINode* Phi_data2_tryInt16 = builder.CreatePHI(int8PtrType, 2);
    llvm::PHINode* Phi_len_tryInt16 = builder.CreatePHI(int32Type, 2);
    Phi_data1_tryInt16->addIncoming(Phi_data1_tryInt32, bb_tryInt32);
    Phi_data1_tryInt16->addIncoming(next_data1, bb_adjustInt32);
    Phi_data2_tryInt16->addIncoming(Phi_data2_tryInt32, bb_tryInt32);
    Phi_data2_tryInt16->addIncoming(next_data2, bb_adjustInt32);
    Phi_len_tryInt16->addIncoming(Phi_len_tryInt32, bb_tryInt32);
    Phi_len_tryInt16->addIncoming(len_less4, bb_adjustInt32);
    tmpval = builder.CreateICmpSGT(Phi_len_tryInt16, int32_1);
    builder.CreateCondBr(tmpval, bb_cmpInt16, bb_tryInt8);

    /* int16 comparison */
    builder.SetInsertPoint(bb_cmpInt16);
    data1 = builder.CreateBitCast(Phi_data1_tryInt16, int16PtrType);
    data1 = builder.CreateLoad(int16Type, data1, "data1_i16");
    data2 = builder.CreateBitCast(Phi_data2_tryInt16, int16PtrType);
    data2 = builder.CreateLoad(int16Type, data2, "data2_i16");
    tmpval = builder.CreateICmpEQ(data1, data2);
    builder.CreateCondBr(tmpval, bb_adjustInt16, bb_ret);

    builder.SetInsertPoint(bb_adjustInt16);
    next_data1 = builder.CreateInBoundsGEP(Phi_data1_tryInt16, int64_2);
    next_data2 = builder.CreateInBoundsGEP(Phi_data2_tryInt16, int64_2);
    llvm::Value* len_less2 = builder.CreateSub(Phi_len_tryInt16, int32_2);
    builder.CreateBr(bb_tryInt8);

    /* try the int8 (char) comparison for remaining */
    builder.SetInsertPoint(bb_tryInt8);
    llvm::PHINode* Phi_data1_tryInt8 = builder.CreatePHI(int8PtrType, 2);
    llvm::PHINode* Phi_data2_tryInt8 = builder.CreatePHI(int8PtrType, 2);
    llvm::PHINode* Phi_len_tryInt8 = builder.CreatePHI(int32Type, 2);
    Phi_data1_tryInt8->addIncoming(Phi_data1_tryInt16, bb_tryInt16);
    Phi_data1_tryInt8->addIncoming(next_data1, bb_adjustInt16);
    Phi_data2_tryInt8->addIncoming(Phi_data2_tryInt16, bb_tryInt16);
    Phi_data2_tryInt8->addIncoming(next_data2, bb_adjustInt16);
    Phi_len_tryInt8->addIncoming(Phi_len_tryInt16, bb_tryInt16);
    Phi_len_tryInt8->addIncoming(len_less2, bb_adjustInt16);
    tmpval = builder.CreateICmpSGT(Phi_len_tryInt8, int32_0);
    builder.CreateCondBr(tmpval, bb_cmpInt8, bb_retTrue);

    /* int8 (char) comparison */
    builder.SetInsertPoint(bb_cmpInt8);
    data1 = builder.CreateLoad(int8Type, Phi_data1_tryInt8, "data1_i8");
    data2 = builder.CreateLoad(int8Type, Phi_data2_tryInt8, "data2_i8");
    tmpval = builder.CreateICmpEQ(data1, data2);
    builder.CreateCondBr(tmpval, bb_retTrue, bb_ret);

    builder.SetInsertPoint(bb_retTrue);
    builder.CreateBr(bb_ret);

    builder.SetInsertPoint(bb_ret);
    llvm::PHINode* Phi_result = builder.CreatePHI(int8Type, 6);
    Phi_result->addIncoming(int8_1, bb_retTrue);
    Phi_result->addIncoming(int8_0, bb_loopBody);
    Phi_result->addIncoming(int8_0, bb_cmpInt64);
    Phi_result->addIncoming(int8_0, bb_cmpInt32);
    Phi_result->addIncoming(int8_0, bb_cmpInt16);
    Phi_result->addIncoming(int8_0, bb_cmpInt8);
    builder.CreateRet(Phi_result);

    llvmCodeGen->FinalizeFunction(jitted_memcmp);
    return jitted_memcmp;
}

/*
 * @Description	: Codegen on memcmp function that is called by bpchareq function
 *				  for Sort Aggregation. We make a special case here since the
 *				  bpchar length is less than 16 bytes.
 * @return		: SortAggMemcmp_short(char *a, char *b, int length) returns
 *				  true if the two memory blocks have the same data, otherwise,
 *				  it returns false.
 * @Notes		: The optimized memcmp code:
 *
 * bool SortAggMemcmp_short(char *a, char *b, int length)
 * {
 *		if (length >= 8)
 *		{
 *			uint64 t1 = *((uint64*)a);
 *			uint64 t2 = *((uint64*)b);
 *			if (t1 != t2)
 *				return false;
 *			a += 8;
 *			b += 8;
 *			length -= 8;
 *		}
 *
 *		if (length >= 4)
 *		{
 *			uint32 t1 = *((uint32*)a);
 *			uint32 t2 = *((uint32*)b);
 *			if (t1 != t2)
 *				return false;
 *			a += 4;
 *			b += 4;
 *			length -= 4;
 *		}
 *
 *		if (length >= 2)
 *		{
 *			uint16 t1 = *((uint16*)a);
 *			uint16 t2 = *((uint16*)b);
 *			if (t1 != t2)
 *				return false;
 *			a += 2;
 *			b += 2;
 *			length -= 2;
 *		}
 *
 *		if (length > 0)
 *		{
 *			if (*a != *b)
 *				return false;
 *		}
 *		return true;
 * }
 */
llvm::Function* VecSortCodeGen::SortAggMemcmpCodeGen_short()
{
    Assert(NULL != (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj);
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    llvm::Value* tmpval = NULL;
    llvm::Value* llvmargs[3];
    llvm::Value* parm1 = NULL;
    llvm::Value* parm2 = NULL;
    llvm::Value* len = NULL;
    llvm::Value* data1 = NULL;
    llvm::Value* data2 = NULL;

    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int16Type, INT2OID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(int16PtrType, INT2OID);
    DEFINE_CG_PTRTYPE(int32PtrType, INT4OID);
    DEFINE_CG_PTRTYPE(int64PtrType, INT8OID);

    DEFINE_CGVAR_INT8(int8_0, 0);
    DEFINE_CGVAR_INT8(int8_1, 1);
    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT32(int32_2, 2);
    DEFINE_CGVAR_INT32(int32_3, 3);
    DEFINE_CGVAR_INT32(int32_4, 4);
    DEFINE_CGVAR_INT32(int32_7, 7);
    DEFINE_CGVAR_INT32(int32_8, 8);
    DEFINE_CGVAR_INT64(int64_2, 2);
    DEFINE_CGVAR_INT64(int64_4, 4);
    DEFINE_CGVAR_INT64(int64_8, 8);

    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "SortAggMemcmp_short", int8Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("a", int8PtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("b", int8PtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("length", int32Type));
    llvm::Function* jitted_memcmp = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    parm1 = llvmargs[0];
    parm2 = llvmargs[1];
    len = llvmargs[2];

    llvm::BasicBlock* bb_entry = &jitted_memcmp->getEntryBlock();
    DEFINE_BLOCK(bb_ret, jitted_memcmp);
    DEFINE_BLOCK(bb_cmpInt64, jitted_memcmp);
    DEFINE_BLOCK(bb_adjustInt64, jitted_memcmp);
    DEFINE_BLOCK(bb_tryInt32, jitted_memcmp);
    DEFINE_BLOCK(bb_cmpInt32, jitted_memcmp);
    DEFINE_BLOCK(bb_adjustInt32, jitted_memcmp);
    DEFINE_BLOCK(bb_tryInt16, jitted_memcmp);
    DEFINE_BLOCK(bb_cmpInt16, jitted_memcmp);
    DEFINE_BLOCK(bb_adjustInt16, jitted_memcmp);
    DEFINE_BLOCK(bb_tryInt8, jitted_memcmp);
    DEFINE_BLOCK(bb_cmpInt8, jitted_memcmp);
    DEFINE_BLOCK(bb_retTrue, jitted_memcmp);

    /* start codegen in entry basic block */
    /* check if the length is greate than 7, try int64 fisrt, else try int32 */
    llvm::Value* parm1_int8Ptr = builder.CreateBitCast(parm1, int8PtrType);
    llvm::Value* parm2_int8Ptr = builder.CreateBitCast(parm2, int8PtrType);
    tmpval = builder.CreateICmpSGT(len, int32_7);
    builder.CreateCondBr(tmpval, bb_cmpInt64, bb_tryInt32);

    /* uint64 t1 = *((uint64*)a); */
    builder.SetInsertPoint(bb_cmpInt64);
    data1 = builder.CreateBitCast(parm1, int64PtrType);
    data1 = builder.CreateLoad(int64Type, data1, "data1_i64");
    data2 = builder.CreateBitCast(parm2, int64PtrType);
    data2 = builder.CreateLoad(int64Type, data2, "data2_i64");
    tmpval = builder.CreateICmpEQ(data1, data2);
    builder.CreateCondBr(tmpval, bb_adjustInt64, bb_ret);

    builder.SetInsertPoint(bb_adjustInt64);
    llvm::Value* next_data1 = builder.CreateInBoundsGEP(parm1_int8Ptr, int64_8);
    llvm::Value* next_data2 = builder.CreateInBoundsGEP(parm2_int8Ptr, int64_8);
    llvm::Value* len_less8 = builder.CreateSub(len, int32_8);
    builder.CreateBr(bb_tryInt32);

    builder.SetInsertPoint(bb_tryInt32);
    llvm::PHINode* Phi_data1_tryInt32 = builder.CreatePHI(int8PtrType, 2);
    llvm::PHINode* Phi_data2_tryInt32 = builder.CreatePHI(int8PtrType, 2);
    llvm::PHINode* Phi_len_tryInt32 = builder.CreatePHI(int32Type, 2);
    Phi_data1_tryInt32->addIncoming(parm1_int8Ptr, bb_entry);
    Phi_data1_tryInt32->addIncoming(next_data1, bb_adjustInt64);
    Phi_data2_tryInt32->addIncoming(parm2_int8Ptr, bb_entry);
    Phi_data2_tryInt32->addIncoming(next_data2, bb_adjustInt64);
    Phi_len_tryInt32->addIncoming(len, bb_entry);
    Phi_len_tryInt32->addIncoming(len_less8, bb_adjustInt64);
    tmpval = builder.CreateICmpSGT(Phi_len_tryInt32, int32_3);
    builder.CreateCondBr(tmpval, bb_cmpInt32, bb_tryInt16);

    /* length is less than 7 greate than 3 */
    builder.SetInsertPoint(bb_cmpInt32);
    data1 = builder.CreateBitCast(Phi_data1_tryInt32, int32PtrType);
    data1 = builder.CreateLoad(int32Type, data1, "data1_i32");
    data2 = builder.CreateBitCast(Phi_data2_tryInt32, int32PtrType);
    data2 = builder.CreateLoad(int32Type, data2, "data2_i32");
    tmpval = builder.CreateICmpEQ(data1, data2);
    builder.CreateCondBr(tmpval, bb_adjustInt32, bb_ret);

    builder.SetInsertPoint(bb_adjustInt32);
    next_data1 = builder.CreateInBoundsGEP(Phi_data1_tryInt32, int64_4);
    next_data2 = builder.CreateInBoundsGEP(Phi_data2_tryInt32, int64_4);
    llvm::Value* len_less4 = builder.CreateSub(Phi_len_tryInt32, int32_4);
    builder.CreateBr(bb_tryInt16);

    builder.SetInsertPoint(bb_tryInt16);
    llvm::PHINode* Phi_data1_tryInt16 = builder.CreatePHI(int8PtrType, 2);
    llvm::PHINode* Phi_data2_tryInt16 = builder.CreatePHI(int8PtrType, 2);
    llvm::PHINode* Phi_len_tryInt16 = builder.CreatePHI(int32Type, 2);
    Phi_data1_tryInt16->addIncoming(Phi_data1_tryInt32, bb_tryInt32);
    Phi_data1_tryInt16->addIncoming(next_data1, bb_adjustInt32);
    Phi_data2_tryInt16->addIncoming(Phi_data2_tryInt32, bb_tryInt32);
    Phi_data2_tryInt16->addIncoming(next_data2, bb_adjustInt32);
    Phi_len_tryInt16->addIncoming(Phi_len_tryInt32, bb_tryInt32);
    Phi_len_tryInt16->addIncoming(len_less4, bb_adjustInt32);
    tmpval = builder.CreateICmpSGT(Phi_len_tryInt16, int32_1);
    builder.CreateCondBr(tmpval, bb_cmpInt16, bb_tryInt8);

    builder.SetInsertPoint(bb_cmpInt16);
    data1 = builder.CreateBitCast(Phi_data1_tryInt16, int16PtrType);
    data1 = builder.CreateLoad(int16Type, data1, "data1_i16");
    data2 = builder.CreateBitCast(Phi_data2_tryInt16, int16PtrType);
    data2 = builder.CreateLoad(int16Type, data2, "data2_i16");
    tmpval = builder.CreateICmpEQ(data1, data2);
    builder.CreateCondBr(tmpval, bb_adjustInt16, bb_ret);

    builder.SetInsertPoint(bb_adjustInt16);
    next_data1 = builder.CreateInBoundsGEP(Phi_data1_tryInt16, int64_2);
    next_data2 = builder.CreateInBoundsGEP(Phi_data2_tryInt16, int64_2);
    llvm::Value* len_less2 = builder.CreateSub(Phi_len_tryInt16, int32_2);
    builder.CreateBr(bb_tryInt8);

    builder.SetInsertPoint(bb_tryInt8);
    llvm::PHINode* Phi_data1_tryInt8 = builder.CreatePHI(int8PtrType, 2);
    llvm::PHINode* Phi_data2_tryInt8 = builder.CreatePHI(int8PtrType, 2);
    llvm::PHINode* Phi_len_tryInt8 = builder.CreatePHI(int32Type, 2);
    Phi_data1_tryInt8->addIncoming(Phi_data1_tryInt16, bb_tryInt16);
    Phi_data1_tryInt8->addIncoming(next_data1, bb_adjustInt16);
    Phi_data2_tryInt8->addIncoming(Phi_data2_tryInt16, bb_tryInt16);
    Phi_data2_tryInt8->addIncoming(next_data2, bb_adjustInt16);
    Phi_len_tryInt8->addIncoming(Phi_len_tryInt16, bb_tryInt16);
    Phi_len_tryInt8->addIncoming(len_less2, bb_adjustInt16);
    tmpval = builder.CreateICmpSGT(Phi_len_tryInt8, int32_0);
    builder.CreateCondBr(tmpval, bb_cmpInt8, bb_retTrue);

    builder.SetInsertPoint(bb_cmpInt8);
    data1 = builder.CreateLoad(int8Type, Phi_data1_tryInt8, "data1_i8");
    data2 = builder.CreateLoad(int8Type, Phi_data2_tryInt8, "data2_i8");
    tmpval = builder.CreateICmpEQ(data1, data2);
    builder.CreateCondBr(tmpval, bb_retTrue, bb_ret);

    builder.SetInsertPoint(bb_retTrue);
    builder.CreateBr(bb_ret);

    builder.SetInsertPoint(bb_ret);
    llvm::PHINode* Phi_result = builder.CreatePHI(int8Type, 6);
    Phi_result->addIncoming(int8_1, bb_retTrue);
    Phi_result->addIncoming(int8_0, bb_cmpInt64);
    Phi_result->addIncoming(int8_0, bb_cmpInt32);
    Phi_result->addIncoming(int8_0, bb_cmpInt16);
    Phi_result->addIncoming(int8_0, bb_cmpInt8);
    builder.CreateRet(Phi_result);

    llvmCodeGen->FinalizeFunction(jitted_memcmp);
    return jitted_memcmp;
}

/*
 * @Description	: Codegen on bpchareq function for Sort Aggregation. We
 *				  inlined pg_detoast_datum_packed.
 * @Notes		: Call SortAggMemcmp without calling bcTruelen since the two
 *				  compared bpchar data should have the same definition length.
 */
llvm::Function* VecSortCodeGen::SortAggBpchareqCodeGen(int length)
{
    Assert(NULL != (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj);
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    llvm::Value* tmpval = NULL;
    llvm::Value* llvmargs[3];
    llvm::Value* parm1 = NULL;
    llvm::Value* parm2 = NULL;
    llvm::Value* len = NULL;
    llvm::Value* value1 = NULL;
    llvm::Value* value2 = NULL;

    DEFINE_CG_VOIDTYPE(voidType);
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(varattrib_1bPtrType, "struct.varattrib_1b");
    DEFINE_CG_PTRTYPE(varlenaPtrType, "struct.varlena");
    llvm::Type* int8ArrayType = llvm::ArrayType::get(int8Type, 1);
    llvm::Type* int8ArrayPtrType = llvmCodeGen->getPtrType(int8ArrayType);

    DEFINE_CGVAR_INT8(int8_0, 0);
    DEFINE_CGVAR_INT8(int8_1, 1);
    DEFINE_CGVAR_INT8(int8_2, 2);
    DEFINE_CGVAR_INT8(int8_3, 3);
    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT64(int64_0, 0);
    DEFINE_CGVAR_INT64(int64_1, 1);

    llvm::Value* Vals2[2] = {int64_0, int32_0};
    llvm::Value* Vals3[3] = {int64_0, int32_0, int64_0};

    /* the prototype of the jitted function for bpchareq
     * Three parameters: (1) datum a, (2) datum b, (3) int32 length
     */
    char long_len[] = "LLVMIRbpchareq_SA_long";
    char short_len[] = "LLVMIRbpchareq_SA_short";
    char* func_name = NULL;
    if (length >= 16)
        func_name = long_len;
    else
        func_name = short_len;
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, func_name, int8Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("a", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("b", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("length", int32Type));
    llvm::Function* jitted_bpchareq = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    parm1 = llvmargs[0];
    parm2 = llvmargs[1];
    len = llvmargs[2];

    llvm::BasicBlock* bb_entry = &jitted_bpchareq->getEntryBlock();
    DEFINE_BLOCK(bb_untoast1, jitted_bpchareq);
    DEFINE_BLOCK(bb_untoast2, jitted_bpchareq);
    DEFINE_BLOCK(bb_nextVal, jitted_bpchareq);
    DEFINE_BLOCK(bb_data1, jitted_bpchareq);
    DEFINE_BLOCK(bb_longHeader1, jitted_bpchareq);
    DEFINE_BLOCK(bb_shortHeader1, jitted_bpchareq);
    DEFINE_BLOCK(bb_data2, jitted_bpchareq);
    DEFINE_BLOCK(bb_longHeader2, jitted_bpchareq);
    DEFINE_BLOCK(bb_shortHeader2, jitted_bpchareq);
    DEFINE_BLOCK(bb_memcmp, jitted_bpchareq);
    DEFINE_BLOCK(bb_pfree1, jitted_bpchareq);
    DEFINE_BLOCK(bb_check_pfree, jitted_bpchareq);
    DEFINE_BLOCK(bb_pfree2, jitted_bpchareq);
    DEFINE_BLOCK(bb_ret, jitted_bpchareq);

    /* Start codegen on entry basic block.
     * First, get the input argument by calling PG_GETARG_BPCHAR_PP.
     *
     * pg_detoast_datum_packed((struct varlena *) DatumGetPointer(datum)):
     * if (VARATT_IS_COMPRESSED(datum) || VARATT_IS_EXTERNAL(datum))
     *		return heap_tuple_untoast_attr(datum);
     * else
     *		return datum;
     */
    value1 = builder.CreateIntToPtr(parm1, varattrib_1bPtrType);
    value1 = builder.CreateInBoundsGEP(value1, Vals2);
    value1 = builder.CreateLoad(int8Type, value1, "header1");
    tmpval = builder.CreateAnd(value1, int8_3);
    llvm::Value* isCompressed = builder.CreateICmpEQ(tmpval, int8_2);
    llvm::Value* isExternal = builder.CreateICmpEQ(value1, int8_1);
    value1 = builder.CreateIntToPtr(parm1, varlenaPtrType);
    tmpval = builder.CreateOr(isCompressed, isExternal);
    builder.CreateCondBr(tmpval, bb_untoast1, bb_nextVal);

    /* call heap_tuple_untoast_attr for arg1 */
    builder.SetInsertPoint(bb_untoast1);
    llvm::Function* wrap_untoast = llvmCodeGen->module()->getFunction("LLVMIRwrapuntoast");
    if (wrap_untoast == NULL) {
        GsCodeGen::FnPrototype func_prototype(llvmCodeGen, "LLVMIRwrapuntoast", varlenaPtrType);
        func_prototype.addArgument(GsCodeGen::NamedVariable("datum", varlenaPtrType));
        wrap_untoast = func_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMIRwrapuntoast", (void*)Wrap_heap_tuple_untoast_attr);
    }
    llvm::Value* untoast_value1 = builder.CreateCall(wrap_untoast, value1);
    builder.CreateBr(bb_nextVal);

    /* turn to deal with pg_detoast_datum_packed(arg2) */
    builder.SetInsertPoint(bb_nextVal);
    llvm::PHINode* Phi_arg1 = builder.CreatePHI(varlenaPtrType, 2);
    Phi_arg1->addIncoming(value1, bb_entry);
    Phi_arg1->addIncoming(untoast_value1, bb_untoast1);

    value2 = builder.CreateIntToPtr(parm2, varattrib_1bPtrType);
    value2 = builder.CreateInBoundsGEP(value2, Vals2);
    value2 = builder.CreateLoad(int8Type, value2, "header2");
    tmpval = builder.CreateAnd(value2, int8_3);
    isCompressed = builder.CreateICmpEQ(tmpval, int8_2);
    isExternal = builder.CreateICmpEQ(value2, int8_1);
    value2 = builder.CreateIntToPtr(parm2, varlenaPtrType);
    tmpval = builder.CreateOr(isCompressed, isExternal);
    builder.CreateCondBr(tmpval, bb_untoast2, bb_data1);

    /* call heap_tuple_untoast_attr for arg2 */
    builder.SetInsertPoint(bb_untoast2);
    llvm::Value* untoast_value2 = builder.CreateCall(wrap_untoast, value2);
    builder.CreateBr(bb_data1);

    /* now enter into text_cmp, first deal with VARDATA_ANY(arg1) and
     * VARSIZE_ANY_EXHDR(arg1).
     */
    builder.SetInsertPoint(bb_data1);
    llvm::PHINode* Phi_arg2 = builder.CreatePHI(varlenaPtrType, 2);
    Phi_arg2->addIncoming(value2, bb_nextVal);
    Phi_arg2->addIncoming(untoast_value2, bb_untoast2);

    tmpval = builder.CreateInBoundsGEP(Phi_arg1, Vals3);
    tmpval = builder.CreateLoad(int8Type, tmpval, "header1");
    tmpval = builder.CreateAnd(tmpval, int8_1);
    tmpval = builder.CreateICmpEQ(tmpval, int8_0);
    builder.CreateCondBr(tmpval, bb_longHeader1, bb_shortHeader1);

    builder.SetInsertPoint(bb_shortHeader1);
    Vals3[2] = int64_1;
    tmpval = builder.CreateInBoundsGEP(Phi_arg1, Vals3);
    llvm::Value* data1_short = builder.CreateBitCast(tmpval, int8ArrayPtrType);
    builder.CreateBr(bb_data2);

    builder.SetInsertPoint(bb_longHeader1);
    Vals2[1] = int32_1;
    llvm::Value* data1_long = builder.CreateInBoundsGEP(Phi_arg1, Vals2);
    builder.CreateBr(bb_data2);

    builder.SetInsertPoint(bb_data2);
    llvm::PHINode* Phi_data1 = builder.CreatePHI(int8ArrayPtrType, 2);
    Phi_data1->addIncoming(data1_short, bb_shortHeader1);
    Phi_data1->addIncoming(data1_long, bb_longHeader1);

    Vals3[2] = int64_0;
    tmpval = builder.CreateInBoundsGEP(Phi_arg2, Vals3);
    tmpval = builder.CreateLoad(int8Type, tmpval, "header2");
    tmpval = builder.CreateAnd(tmpval, int8_1);
    tmpval = builder.CreateICmpEQ(tmpval, int8_0);
    builder.CreateCondBr(tmpval, bb_longHeader2, bb_shortHeader2);

    builder.SetInsertPoint(bb_shortHeader2);
    Vals3[2] = int64_1;
    tmpval = builder.CreateInBoundsGEP(Phi_arg2, Vals3);
    llvm::Value* data2_short = builder.CreateBitCast(tmpval, int8ArrayPtrType);
    builder.CreateBr(bb_memcmp);

    builder.SetInsertPoint(bb_longHeader2);
    Vals2[1] = int32_1;
    llvm::Value* data2_long = builder.CreateInBoundsGEP(Phi_arg2, Vals2);
    builder.CreateBr(bb_memcmp);

    /* get data1 and data2, then compare them according to the len */
    builder.SetInsertPoint(bb_memcmp);
    llvm::PHINode* Phi_data2 = builder.CreatePHI(int8ArrayPtrType, 2);
    Phi_data2->addIncoming(data2_short, bb_shortHeader2);
    Phi_data2->addIncoming(data2_long, bb_longHeader2);

    Vals2[1] = int64_0;
    llvm::Value* val_data1 = builder.CreateInBoundsGEP(Phi_data1, Vals2);
    llvm::Value* val_data2 = builder.CreateInBoundsGEP(Phi_data2, Vals2);

    llvm::Function* jitted_memcmp = NULL;
    if (length >= 16) {
        jitted_memcmp = llvmCodeGen->module()->getFunction("SortAggMemcmp_long");
        if (jitted_memcmp == NULL) {
            jitted_memcmp = SortAggMemcmpCodeGen_long();
        }
    } else {
        jitted_memcmp = llvmCodeGen->module()->getFunction("SortAggMemcmp_short");
        if (jitted_memcmp == NULL) {
            jitted_memcmp = SortAggMemcmpCodeGen_short();
        }
    }
    llvm::Value* compare = builder.CreateCall(jitted_memcmp, {val_data1, val_data2, len});

    value1 = builder.CreatePtrToInt(Phi_arg1, int64Type);
    tmpval = builder.CreateICmpEQ(parm1, value1);
    builder.CreateCondBr(tmpval, bb_check_pfree, bb_pfree1);

    /* consider release source :
     * PG_FREE_IF_COPY(arg1, 0);
     * PG_FREE_IF_COPY(arg2, 1);
     */
    builder.SetInsertPoint(bb_pfree1);
    llvm::Function* wrap_pfree = llvmCodeGen->module()->getFunction("LLVMIRwrappfree");
    if (wrap_pfree == NULL) {
        GsCodeGen::FnPrototype func_prototype(llvmCodeGen, "LLVMIRwrappfree", voidType);
        func_prototype.addArgument(GsCodeGen::NamedVariable("ptr", int8PtrType));
        wrap_pfree = func_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMIRwrappfree", (void*)Wrap_pfree);
    }
    tmpval = builder.CreateBitCast(Phi_arg1, int8PtrType);
    builder.CreateCall(wrap_pfree, tmpval);
    builder.CreateBr(bb_ret);

    builder.SetInsertPoint(bb_check_pfree);
    value2 = builder.CreatePtrToInt(Phi_arg2, int64Type);
    tmpval = builder.CreateICmpEQ(parm2, value2);
    builder.CreateCondBr(tmpval, bb_ret, bb_pfree2);

    builder.SetInsertPoint(bb_pfree2);
    tmpval = builder.CreateBitCast(Phi_arg2, int8PtrType);
    builder.CreateCall(wrap_pfree, tmpval);
    builder.CreateBr(bb_ret);

    builder.SetInsertPoint(bb_ret);
    builder.CreateRet(compare);

    llvmCodeGen->FinalizeFunction(jitted_bpchareq);
    return jitted_bpchareq;
}

/*
 * @Description	: Codegeneration for texteq function in sort aggregation.
 */
llvm::Function* VecSortCodeGen::SortAggTexteqCodeGen()
{
    Assert(NULL != (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj);
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    llvm::Value* tmpval = NULL;
    llvm::Value* llvmargs[3];
    llvm::Value* parm1 = NULL;
    llvm::Value* parm2 = NULL;
    llvm::Value* value1 = NULL;
    llvm::Value* value2 = NULL;

    DEFINE_CG_VOIDTYPE(voidType);
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(int32PtrType, INT4OID);
    DEFINE_CG_PTRTYPE(varattrib_1bPtrType, "struct.varattrib_1b");
    DEFINE_CG_PTRTYPE(varlenaPtrType, "struct.varlena");
    llvm::Type* int8ArrayType = llvm::ArrayType::get(int8Type, 1);
    llvm::Type* int8ArrayPtrType = llvmCodeGen->getPtrType(int8ArrayType);

    DEFINE_CGVAR_INT8(int8_0, 0);
    DEFINE_CGVAR_INT8(int8_1, 1);
    DEFINE_CGVAR_INT8(int8_2, 2);
    DEFINE_CGVAR_INT8(int8_3, 3);
    DEFINE_CGVAR_INT8(int8_7FX, 0x7F);
    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT32(int32_4, 4);
    DEFINE_CGVAR_INT64(int64_0, 0);
    DEFINE_CGVAR_INT64(int64_1, 1);

    llvm::Value* Vals2[2] = {int64_0, int32_0};
    llvm::Value* Vals3[3] = {int64_0, int32_0, int64_0};

    /* the prototype of the jitted function for texteq
     * Three parameters: (1) datum a, (2) datum b
     */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMIRtexteq_SA", int8Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("a", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("b", int64Type));
    llvm::Function* jitted_texteq = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    parm1 = llvmargs[0];
    parm2 = llvmargs[1];

    llvm::BasicBlock* bb_entry = &jitted_texteq->getEntryBlock();
    DEFINE_BLOCK(bb_untoast1, jitted_texteq);
    DEFINE_BLOCK(bb_untoast2, jitted_texteq);
    DEFINE_BLOCK(bb_nextVal, jitted_texteq);
    DEFINE_BLOCK(bb_data1, jitted_texteq);
    DEFINE_BLOCK(bb_longHeader1, jitted_texteq);
    DEFINE_BLOCK(bb_shortHeader1, jitted_texteq);
    DEFINE_BLOCK(bb_data2, jitted_texteq);
    DEFINE_BLOCK(bb_longHeader2, jitted_texteq);
    DEFINE_BLOCK(bb_shortHeader2, jitted_texteq);
    DEFINE_BLOCK(bb_lencmp, jitted_texteq);
    DEFINE_BLOCK(bb_memcmp, jitted_texteq);
    DEFINE_BLOCK(bb_pfree1, jitted_texteq);
    DEFINE_BLOCK(bb_check_pfree, jitted_texteq);
    DEFINE_BLOCK(bb_pfree2, jitted_texteq);
    DEFINE_BLOCK(bb_ret, jitted_texteq);

    /* Start codegen on entry basic block.
     * First, get the input argument by calling PG_GETARG_BPCHAR_PP.
     *
     * pg_detoast_datum_packed((struct varlena *) DatumGetPointer(datum)):
     * if (VARATT_IS_COMPRESSED(datum) || VARATT_IS_EXTERNAL(datum))
     *		return heap_tuple_untoast_attr(datum);
     * else
     *		return datum;
     */
    value1 = builder.CreateIntToPtr(parm1, varattrib_1bPtrType);
    value1 = builder.CreateInBoundsGEP(value1, Vals2);
    value1 = builder.CreateLoad(int8Type, value1, "header1");
    tmpval = builder.CreateAnd(value1, int8_3);
    llvm::Value* isCompressed = builder.CreateICmpEQ(tmpval, int8_2);
    llvm::Value* isExternal = builder.CreateICmpEQ(value1, int8_1);
    value1 = builder.CreateIntToPtr(parm1, varlenaPtrType);
    tmpval = builder.CreateOr(isCompressed, isExternal);
    builder.CreateCondBr(tmpval, bb_untoast1, bb_nextVal);

    /* call heap_tuple_untoast_attr for first argument */
    builder.SetInsertPoint(bb_untoast1);
    llvm::Function* wrap_untoast = llvmCodeGen->module()->getFunction("LLVMIRwrapuntoast");
    if (wrap_untoast == NULL) {
        GsCodeGen::FnPrototype func_prototype(llvmCodeGen, "LLVMIRwrapuntoast", varlenaPtrType);
        func_prototype.addArgument(GsCodeGen::NamedVariable("datum", varlenaPtrType));
        wrap_untoast = func_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMIRwrapuntoast", (void*)Wrap_heap_tuple_untoast_attr);
    }
    llvm::Value* untoast_value1 = builder.CreateCall(wrap_untoast, value1);
    builder.CreateBr(bb_nextVal);

    /* begin to deal with the second argument */
    builder.SetInsertPoint(bb_nextVal);
    llvm::PHINode* Phi_arg1 = builder.CreatePHI(varlenaPtrType, 2);
    Phi_arg1->addIncoming(value1, bb_entry);
    Phi_arg1->addIncoming(untoast_value1, bb_untoast1);

    value2 = builder.CreateIntToPtr(parm2, varattrib_1bPtrType);
    value2 = builder.CreateInBoundsGEP(value2, Vals2);
    value2 = builder.CreateLoad(int8Type, value2, "header2");
    tmpval = builder.CreateAnd(value2, int8_3);
    isCompressed = builder.CreateICmpEQ(tmpval, int8_2);
    isExternal = builder.CreateICmpEQ(value2, int8_1);
    value2 = builder.CreateIntToPtr(parm2, varlenaPtrType);
    tmpval = builder.CreateOr(isCompressed, isExternal);
    builder.CreateCondBr(tmpval, bb_untoast2, bb_data1);

    /* call heap_tuple_untoast_attr for second argument */
    builder.SetInsertPoint(bb_untoast2);
    llvm::Value* untoast_value2 = builder.CreateCall(wrap_untoast, value2);
    builder.CreateBr(bb_data1);

    /* now enter into text_cmp, first deal with VARDATA_ANY(arg1) and
     * VARSIZE_ANY_EXHDR(arg1).
     */
    builder.SetInsertPoint(bb_data1);
    llvm::PHINode* Phi_arg2 = builder.CreatePHI(varlenaPtrType, 2);
    Phi_arg2->addIncoming(value2, bb_nextVal);
    Phi_arg2->addIncoming(untoast_value2, bb_untoast2);

    tmpval = builder.CreateInBoundsGEP(Phi_arg1, Vals3);
    llvm::Value* header1 = builder.CreateLoad(int8Type, tmpval, "header1");
    tmpval = builder.CreateAnd(header1, int8_1);
    tmpval = builder.CreateICmpEQ(tmpval, int8_0);
    builder.CreateCondBr(tmpval, bb_longHeader1, bb_shortHeader1);

    builder.SetInsertPoint(bb_shortHeader1);
    Vals3[2] = int64_1;
    tmpval = builder.CreateInBoundsGEP(Phi_arg1, Vals3);
    llvm::Value* data1_short = builder.CreateBitCast(tmpval, int8ArrayPtrType);
    llvm::Value* len1_short = builder.CreateLShr(header1, 1);
    len1_short = builder.CreateAnd(len1_short, int8_7FX);
    len1_short = builder.CreateSub(len1_short, int8_1);
    len1_short = builder.CreateZExt(len1_short, int32Type);
    builder.CreateBr(bb_data2);

    builder.SetInsertPoint(bb_longHeader1);
    Vals2[1] = int32_1;
    llvm::Value* data1_long = builder.CreateInBoundsGEP(Phi_arg1, Vals2);
    Vals2[1] = int32_0;
    llvm::Value* len1_long = builder.CreateInBoundsGEP(Phi_arg1, Vals2);
    len1_long = builder.CreateBitCast(len1_long, int32PtrType);
    len1_long = builder.CreateLoad(int32Type, len1_long);
    len1_long = builder.CreateLShr(len1_long, 2);
    len1_long = builder.CreateSub(len1_long, int32_4);
    builder.CreateBr(bb_data2);

    /* deal with VARDATA_ANY(arg1) and VARSIZE_ANY_EXHDR(arg1) */
    builder.SetInsertPoint(bb_data2);
    llvm::PHINode* Phi_data1 = builder.CreatePHI(int8ArrayPtrType, 2);
    llvm::PHINode* Phi_len1 = builder.CreatePHI(int32Type, 2);
    Phi_data1->addIncoming(data1_short, bb_shortHeader1);
    Phi_data1->addIncoming(data1_long, bb_longHeader1);
    Phi_len1->addIncoming(len1_short, bb_shortHeader1);
    Phi_len1->addIncoming(len1_long, bb_longHeader1);

    Vals3[2] = int64_0;
    tmpval = builder.CreateInBoundsGEP(Phi_arg2, Vals3);
    llvm::Value* header2 = builder.CreateLoad(int8Type, tmpval, "header2");
    tmpval = builder.CreateAnd(header2, int8_1);
    tmpval = builder.CreateICmpEQ(tmpval, int8_0);
    builder.CreateCondBr(tmpval, bb_longHeader2, bb_shortHeader2);

    builder.SetInsertPoint(bb_shortHeader2);
    Vals3[2] = int64_1;
    tmpval = builder.CreateInBoundsGEP(Phi_arg2, Vals3);
    llvm::Value* data2_short = builder.CreateBitCast(tmpval, int8ArrayPtrType);
    llvm::Value* len2_short = builder.CreateLShr(header2, 1);
    len2_short = builder.CreateAnd(len2_short, int8_7FX);
    len2_short = builder.CreateSub(len2_short, int8_1);
    len2_short = builder.CreateZExt(len2_short, int32Type);
    builder.CreateBr(bb_lencmp);

    builder.SetInsertPoint(bb_longHeader2);
    Vals2[1] = int32_1;
    llvm::Value* data2_long = builder.CreateInBoundsGEP(Phi_arg2, Vals2);
    Vals2[1] = int32_0;
    llvm::Value* len2_long = builder.CreateInBoundsGEP(Phi_arg2, Vals2);
    len2_long = builder.CreateBitCast(len2_long, int32PtrType);
    len2_long = builder.CreateLoad(int32Type, len2_long);
    len2_long = builder.CreateLShr(len2_long, 2);
    len2_long = builder.CreateSub(len2_long, int32_4);
    builder.CreateBr(bb_lencmp);

    /* get bcTruelen and actual data */
    builder.SetInsertPoint(bb_lencmp);
    llvm::PHINode* Phi_data2 = builder.CreatePHI(int8ArrayPtrType, 2);
    llvm::PHINode* Phi_len2 = builder.CreatePHI(int32Type, 2);
    Phi_data2->addIncoming(data2_short, bb_shortHeader2);
    Phi_data2->addIncoming(data2_long, bb_longHeader2);
    Phi_len2->addIncoming(len2_short, bb_shortHeader2);
    Phi_len2->addIncoming(len2_long, bb_longHeader2);

    tmpval = builder.CreateICmpEQ(Phi_len1, Phi_len2);
    builder.CreateCondBr(tmpval, bb_memcmp, bb_ret);

    builder.SetInsertPoint(bb_memcmp);
    Vals2[1] = int64_0;
    llvm::Value* val_data1 = builder.CreateInBoundsGEP(Phi_data1, Vals2);
    llvm::Value* val_data2 = builder.CreateInBoundsGEP(Phi_data2, Vals2);

    llvm::Function* jitted_memcmp = NULL;
    jitted_memcmp = llvmCodeGen->module()->getFunction("SortAggMemcmp_long");
    if (jitted_memcmp == NULL) {
        jitted_memcmp = SortAggMemcmpCodeGen_long();
    }
    llvm::Value* compare = builder.CreateCall(jitted_memcmp, {val_data1, val_data2, Phi_len1});

    /* free source after evaluation */
    value1 = builder.CreatePtrToInt(Phi_arg1, int64Type);
    tmpval = builder.CreateICmpEQ(parm1, value1);
    builder.CreateCondBr(tmpval, bb_check_pfree, bb_pfree1);

    builder.SetInsertPoint(bb_pfree1);
    llvm::Function* wrap_pfree = llvmCodeGen->module()->getFunction("LLVMIRwrappfree");
    if (wrap_pfree == NULL) {
        GsCodeGen::FnPrototype func_prototype(llvmCodeGen, "LLVMIRwrappfree", voidType);
        func_prototype.addArgument(GsCodeGen::NamedVariable("ptr", int8PtrType));
        wrap_pfree = func_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMIRwrappfree", (void*)Wrap_pfree);
    }
    tmpval = builder.CreateBitCast(Phi_arg1, int8PtrType);
    builder.CreateCall(wrap_pfree, tmpval);
    builder.CreateBr(bb_ret);

    builder.SetInsertPoint(bb_check_pfree);
    value2 = builder.CreatePtrToInt(Phi_arg2, int64Type);
    tmpval = builder.CreateICmpEQ(parm2, value2);
    builder.CreateCondBr(tmpval, bb_ret, bb_pfree2);

    builder.SetInsertPoint(bb_pfree2);
    tmpval = builder.CreateBitCast(Phi_arg2, int8PtrType);
    builder.CreateCall(wrap_pfree, tmpval);
    builder.CreateBr(bb_ret);

    builder.SetInsertPoint(bb_ret);
    llvm::PHINode* Phi_result = builder.CreatePHI(int8Type, 4);
    Phi_result->addIncoming(int8_0, bb_lencmp);
    Phi_result->addIncoming(compare, bb_pfree1);
    Phi_result->addIncoming(compare, bb_pfree2);
    Phi_result->addIncoming(compare, bb_check_pfree);
    builder.CreateRet(Phi_result);

    llvmCodeGen->FinalizeFunction(jitted_texteq);
    return jitted_texteq;
}

/*
 * @Description	: Codegeneration on match_key for SortAgg, only support loop
 *				  unrolling for multiple keys matching. Compared with original
 *				  match_key function, we add an extra parameter m_key for the
 *				  changed number of keys in group by rollup(...). So we just
 *				  generate one match_key function for the Sort Agg with group
 *				  by rollup(...).
 * @Notes		: Only support int4, int8, bpchar, text data type.
 */
llvm::Function* VecSortCodeGen::SortAggMatchKeyCodeGen(VecAggState* node)
{
    if (!JittableSortAggMatchKey(node))
        return NULL;

    Assert(NULL != (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj);
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    llvmCodeGen->loadIRFile();

    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    llvm::Value* tmpval = NULL;
    llvm::Value* llvmargs[4];
    llvm::Value* sAggRunner = NULL;
    llvm::Value* batch = NULL;
    llvm::Value* batchIdx = NULL;
    llvm::Value* hcell = NULL;
    llvm::Value* mkey = NULL;

    int i;
    VecAgg* plannode = (VecAgg*)node->ss.ps.plan;
    PlanState* outerNode = outerPlanState(node);
    TupleDesc tupDesc = ExecGetResultType(outerNode);

    DEFINE_CG_TYPE(int1Type, BITOID);
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(int32PtrType, INT4OID);
    DEFINE_CG_PTRTYPE(int64PtrType, INT8OID);
    DEFINE_CG_PTRTYPE(vecBatchPtrType, "class.VectorBatch");
    DEFINE_CG_PTRTYPE(scalarVecPtrType, "class.ScalarVector");
    DEFINE_CG_PTRTYPE(hashCellPtrType, "struct.hashCell");
    DEFINE_CG_PTRTYPE(sortAggRunnerPtrType, "class.SortAggRunner");

    DEFINE_CGVAR_INT8(int8_0, 0);
    DEFINE_CGVAR_INT8(int8_1, 1);
    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_pos_hval_val, pos_hval_val);
    DEFINE_CGVAR_INT32(int32_pos_hval_flag, pos_hval_flag);
    DEFINE_CGVAR_INT32(int32_pos_hcell_mval, pos_hcell_mval);
    DEFINE_CGVAR_INT32(int32_pos_batch_marr, pos_batch_marr);
    DEFINE_CGVAR_INT32(int32_pos_scalvec_flag, pos_scalvec_flag);
    DEFINE_CGVAR_INT32(int32_pos_scalvec_vals, pos_scalvec_vals);
    DEFINE_CGVAR_INT32(int32_pos_hBOper_mkey, pos_hBOper_mkey);
    DEFINE_CGVAR_INT32(int32_pos_hBOper_keyIdx, pos_hBOper_keyIdx);
    DEFINE_CGVAR_INT32(int32_pos_bAggR_keyIdxInCell, pos_bAggR_keyIdxInCell);
    DEFINE_CGVAR_INT64(int64_0, 0);
    llvm::Value* Vals2[2] = {int64_0, int32_0};
    llvm::Value* Vals3[3] = {int64_0, int32_0, int32_0};
    llvm::Value* Vals4[4] = {int64_0, int32_0, int32_0, int32_0};

    /* the prototype of the jitted function for match_key called by SortAgg
     * with four input parameters: (1) VectorBatch *batch, (2) int *batchIdx,
     * (3) hashCell *cell, (4) int m_key.
     */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "JittedSortAggMatchKey", int8Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("sortAggRunner", sortAggRunnerPtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("batch", vecBatchPtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("batchIdx", int32Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("cell", hashCellPtrType));
    llvm::Function* jitted_matchkey = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    sAggRunner = llvmargs[0];
    batch = llvmargs[1];
    batchIdx = llvmargs[2];
    hcell = llvmargs[3];

    int numKeys = plannode->numCols;

    llvm::BasicBlock** bb_keyStart = (llvm::BasicBlock**)palloc(sizeof(llvm::BasicBlock*) * numKeys);
    llvm::BasicBlock** bb_keyCmp = (llvm::BasicBlock**)palloc(sizeof(llvm::BasicBlock*) * numKeys);
    llvm::BasicBlock** bb_keyEnd = (llvm::BasicBlock**)palloc(sizeof(llvm::BasicBlock*) * numKeys);
    llvm::BasicBlock** bb_checkbothnull = (llvm::BasicBlock**)palloc(sizeof(llvm::BasicBlock*) * numKeys);

    /* define vector structures used to store key idx info. */
    llvm::Value** keyIdx = (llvm::Value**)palloc(sizeof(llvm::Value*) * numKeys);
    llvm::Value** keyIdxInCell = (llvm::Value**)palloc(sizeof(llvm::Value*) * numKeys);

    for (i = 0; i < numKeys; i++) {
        bb_keyStart[i] = llvm::BasicBlock::Create(context, "bb_keyStart", jitted_matchkey);
        bb_keyCmp[i] = llvm::BasicBlock::Create(context, "bb_keyCmp", jitted_matchkey);
        bb_keyEnd[i] = llvm::BasicBlock::Create(context, "bb_keyEnd", jitted_matchkey);
        bb_checkbothnull[i] = llvm::BasicBlock::Create(context, "bb_checkbothnull", jitted_matchkey);
    }
    DEFINE_BLOCK(bb_ret, jitted_matchkey);

    /* SortAggRunner.BaseAggRunner.hashBasedOperator.m_key */
    Vals4[3] = int32_pos_hBOper_mkey;
    tmpval = builder.CreateInBoundsGEP(sAggRunner, Vals4);
    mkey = builder.CreateLoad(int32Type, tmpval, "vec_m_key");

    /* SortAggRunner.BaseAggRunner.hashBasedOperator::m_keyIdx */
    Vals4[3] = int32_pos_hBOper_keyIdx;
    tmpval = builder.CreateInBoundsGEP(sAggRunner, Vals4);
    llvm::Value* keyIdxVal = builder.CreateLoad(int32PtrType, tmpval, "vec_m_keyIdx");

    /* sortAggRunner.BaseAggRunner::m_keyIdxInCell */
    Vals3[2] = int32_pos_bAggR_keyIdxInCell;
    tmpval = builder.CreateInBoundsGEP(sAggRunner, Vals3);
    llvm::Value* keyIdxInCellVal = builder.CreateLoad(int32PtrType, tmpval, "vec_m_keyIdxInCell");

    for (i = 0; i < numKeys; i++) {
        tmpval = llvmCodeGen->getIntConstant(INT4OID, i);
        keyIdx[i] = builder.CreateInBoundsGEP(keyIdxVal, tmpval);
        keyIdx[i] = builder.CreateLoad(int32Type, keyIdx[i], "m_key");
        tmpval = builder.CreateSExt(tmpval, int64Type);
        keyIdxInCell[i] = builder.CreateInBoundsGEP(keyIdxInCellVal, tmpval);
        keyIdxInCell[i] = builder.CreateLoad(int32Type, keyIdxInCell[i], "m_key");
    }

    /* Start the codegen in entry basic block */
    /* VectorBatch.m_arr */
    Vals2[1] = int32_pos_batch_marr;
    llvm::Value* batch_arr = builder.CreateInBoundsGEP(batch, Vals2);
    batch_arr = builder.CreateLoad(scalarVecPtrType, batch_arr, "m_arr");
    builder.CreateBr(bb_keyStart[0]);

    /* Codegen for each key comparison */
    for (i = 0; i < numKeys; i++) {
        int val_keyIdx = plannode->grpColIdx[i] - 1;
        Form_pg_attribute attr = &tupDesc->attrs[val_keyIdx];
        Oid opno = plannode->grpOperators[i];

        builder.SetInsertPoint(bb_keyStart[i]);
        /* ScalarVector.m_flag */
        llvm::Value* batch_Idx = builder.CreateZExt(batchIdx, int64Type);
        Vals2[0] = keyIdx[i];
        Vals2[1] = int32_pos_scalvec_flag;
        llvm::Value* batch_flag = builder.CreateInBoundsGEP(batch_arr, Vals2);
        batch_flag = builder.CreateLoad(int8PtrType, batch_flag, "m_flag");
        batch_flag = builder.CreateInBoundsGEP(batch_flag, batch_Idx);
        batch_flag = builder.CreateLoad(int8Type, batch_flag, "batch_flag");

        /* hshCell.m_val[kdyIdxInCell].flag */
        Vals4[0] = int64_0;
        Vals4[1] = int32_pos_hcell_mval;
        Vals4[2] = keyIdxInCell[i];
        Vals4[3] = int32_pos_hval_flag;
        llvm::Value* cell_flag = builder.CreateInBoundsGEP(hcell, Vals4);
        cell_flag = builder.CreateLoad(int8Type, cell_flag, "cell_flag");
        tmpval = builder.CreateOr(batch_flag, cell_flag);
        tmpval = builder.CreateAnd(tmpval, int8_1);
        tmpval = builder.CreateICmpEQ(tmpval, int8_0);
        builder.CreateCondBr(tmpval, bb_keyCmp[i], bb_checkbothnull[i]);

        /* Only consider compare when both are not null */
        builder.SetInsertPoint(bb_keyCmp[i]);
        Vals2[0] = keyIdx[i];
        Vals2[1] = int32_pos_scalvec_vals;
        llvm::Value* batch_val = builder.CreateInBoundsGEP(batch_arr, Vals2);
        batch_val = builder.CreateLoad(int64PtrType, batch_val, "m_vals");
        batch_val = builder.CreateInBoundsGEP(batch_val, batch_Idx);
        batch_val = builder.CreateLoad(int64Type, batch_val, "batch_val");
        Vals4[0] = int64_0;
        Vals4[1] = int32_pos_hcell_mval;
        Vals4[2] = keyIdxInCell[i];
        Vals4[3] = int32_pos_hval_val;
        llvm::Value* cell_val = builder.CreateInBoundsGEP(hcell, Vals4);
        cell_val = builder.CreateLoad(int64Type, cell_val, "cell_val");
        llvm::Value* compare = NULL;
        switch (opno) {
            case INT4EQOID: {
                batch_val = builder.CreateTrunc(batch_val, int32Type);
                cell_val = builder.CreateTrunc(cell_val, int32Type);
                compare = builder.CreateICmpEQ(batch_val, cell_val);
            } break;
            case INT8EQOID: {
                compare = builder.CreateICmpEQ(batch_val, cell_val);
            } break;
            case BPCHAREQOID: {
                int len = attr->atttypmod - VARHDRSZ;
                llvm::Value* len_value = llvmCodeGen->getIntConstant(INT4OID, len);
                llvm::Function* jitted_bpchareq = NULL;
                /* make a difference here to distinct different length cases */
                if (len > 16) {
                    jitted_bpchareq = llvmCodeGen->module()->getFunction("LLVMIRbpchareq_SA_long");
                } else {
                    jitted_bpchareq = llvmCodeGen->module()->getFunction("LLVMIRbpchareq_SA_short");
                }

                if (jitted_bpchareq == NULL) {
                    jitted_bpchareq = SortAggBpchareqCodeGen(len);
                }

                compare = builder.CreateCall(jitted_bpchareq, {batch_val, cell_val, len_value});
                compare = builder.CreateTrunc(compare, int1Type);
            } break;
            case TEXTEQOID: {
                llvm::Function* jitted_texteq = NULL;
                jitted_texteq = llvmCodeGen->module()->getFunction("LLVMIRtexteq_SA");
                if (jitted_texteq == NULL) {
                    jitted_texteq = SortAggTexteqCodeGen();
                }
                compare = builder.CreateCall(jitted_texteq, {batch_val, cell_val});
                compare = builder.CreateTrunc(compare, int1Type);
            } break;
            default:
                break;
        }

        builder.CreateCondBr(compare, bb_keyEnd[i], bb_ret);

        /* both null is equal */
        builder.SetInsertPoint(bb_checkbothnull[i]);
        tmpval = builder.CreateAnd(batch_flag, int8_1);
        tmpval = builder.CreateAnd(tmpval, cell_flag);
        tmpval = builder.CreateICmpEQ(tmpval, int8_0);
        builder.CreateCondBr(tmpval, bb_ret, bb_keyEnd[i]);

        builder.SetInsertPoint(bb_keyEnd[i]);
        if (i == numKeys - 1) {
            builder.CreateBr(bb_ret);
        } else {
            llvm::Value* val_keys = llvmCodeGen->getIntConstant(INT4OID, i + 1);
            tmpval = builder.CreateICmpEQ(mkey, val_keys);
            builder.CreateCondBr(tmpval, bb_ret, bb_keyStart[i + 1]);
        }
    }

    builder.SetInsertPoint(bb_ret);
    llvm::PHINode* Phi_compare = builder.CreatePHI(int8Type, 3 * numKeys);
    for (i = 0; i < numKeys; i++) {
        Phi_compare->addIncoming(int8_0, bb_keyCmp[i]);
        Phi_compare->addIncoming(int8_0, bb_checkbothnull[i]);
        Phi_compare->addIncoming(int8_1, bb_keyEnd[i]);
    }
    builder.CreateRet(Phi_compare);

    pfree_ext(bb_keyStart);
    pfree_ext(bb_keyCmp);
    pfree_ext(bb_keyEnd);
    pfree_ext(bb_checkbothnull);

    llvmCodeGen->FinalizeFunction(jitted_matchkey, node->ss.ps.plan->plan_node_id);
    return jitted_matchkey;
}
}  // namespace dorado

/*
 * @Description	: Wrap functions called by jitted functions.
 * @input datum	: The value need to be toasted.
 * @return		: Return back a toasted value from compression or external storage.
 */
struct varlena* Wrap_heap_tuple_untoast_attr(struct varlena* datum)
{
    return heap_tuple_untoast_attr(datum);
}

/*
 * @Description	: Wrap pfree function used to cleaning up detoasted copies of inputs.
 * @in ptr		: The varlena string that we need to free.
 */
void Wrap_pfree(char* ptr)
{
    pfree_ext(ptr);
}

/*
 * @Description	: Wrap numeric compare function called by jitted functions.
 * @in num1		: The input numeric argument.
 * @in num2		: The input numeric argument.
 * @return		: The compare result of this two numeric.
 */
int Wrap_numeric_cmp(struct NumericData* num1, struct NumericData* num2)
{

    int result = 0;

    /* use fast numeric optimization if both can be represented in BI Numeric format */
    if (NUMERIC_IS_BI(num1) && NUMERIC_IS_BI(num2)) {
        int left_type = NUMERIC_IS_BI128(num1);
        int right_type = NUMERIC_IS_BI128(num2);
        biopfun func = BiFunMatrix[BICMP][left_type][right_type];
        result = DatumGetInt32(func(num1, num2, NULL));
    } else {
        result = cmp_numerics(num1, num2);
    }

    return result;
}
