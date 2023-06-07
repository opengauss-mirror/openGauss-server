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
 *  vechashaggcodegen.cpp
 *        Routines to handle vector hashagg nodes. We only focuse on 
 *          the CPU intensive part of hashagg operation.
 * IDENTIFICATION
 *        src/gausskernel/runtime/codegen/vecexecutor/vechashaggcodegen.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "codegen/gscodegen.h"
#include "codegen/vechashaggcodegen.h"
#include "codegen/vecsortcodegen.h"
#include "codegen/codegendebuger.h"
#include "codegen/builtinscodegen.h"
#include "codegen/vecexprcodegen.h"

#include "catalog/pg_operator.h"
#include "pgxc/pgxc.h"
#include "vecexecutor/vecexecutor.h"

void WrapAllocHashSlot(HashAggRunner* haRunner, VectorBatch* batch, int idx, int keysimple);
void WrapSglTblAllocHashSlot(HashAggRunner* haRunner, VectorBatch* batch, int idx, int keysimple);
void WrapResetExprContext(ExprContext* econtext);

/* macro values used for prefetch in batchagg */
#define PREFETCH_AGGHASHING_DISTANCE1 4
#define PREFETCH_AGGHASHING_DISTANCE2 2
#define PREFETCH_BATCHAGGREGATION_DISTANCE 2
#define AGG_ECONOMY_RATION 0.001

namespace dorado {
llvm::Function* prefetchAggHashingCodeGen();
llvm::Function* prefetchBatchAggregationCodeGen();
llvm::Function* prefetchAggSglTblHashingCodeGen();

int VecHashAggCodeGen::GetAlignedScale(Expr* node)
{
    int resscale = 0;

    /*
     * Get into this function only when fast_aggref is true, which means
     * we could apply fast codegen path for this expression node.
     */
    switch (nodeTag(node)) {
        case T_TargetEntry: {
            TargetEntry* tentry = (TargetEntry*)node;
            resscale = GetAlignedScale(tentry->expr);
        } break;
        case T_OpExpr: {
            OpExpr* opexpr = (OpExpr*)node;
            Expr* lexpr = (Expr*)linitial(opexpr->args);
            Expr* rexpr = (Expr*)lsecond(opexpr->args);

            int lscale = GetAlignedScale(lexpr);
            int rscale = GetAlignedScale(rexpr);

            /*
             * Only when AggRefFastJittable is satisfied, we evaluate the scale
             * of the aggref expression. So, no need to consider NUMERICDIVOID.
             */
            if (opexpr->opno == NUMERICADDOID || opexpr->opno == NUMERICSUBOID) {
                if (lscale < rscale)
                    resscale = rscale;
                else
                    resscale = lscale;
            } else if (opexpr->opno == NUMERICMULOID)
                resscale = lscale + rscale;
            else
                ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmodule(MOD_LLVM),
                        errmsg("Unsupported operation %u in FastAgg.", opexpr->opno)));
        } break;
        case T_Var: {
            Var* var = (Var*)node;
            Assert(var->vartype == NUMERICOID);

            resscale = (var->vartypmod - VARHDRSZ) & NUMERIC_BI_SCALEMASK;
        } break;
        case T_Const: {
            Const* cst = (Const*)node;
            Assert(cst->consttype == NUMERICOID);

            /* Extract the real value of this const */
            ScalarValue val = ScalarVector::DatumToScalar(cst->constvalue, cst->consttype, cst->constisnull);
            resscale = NUMERIC_BI_SCALE((Numeric)val);
        } break;
        default:
            Assert(0);
            break;
    }

    return resscale;
}

bool VecHashAggCodeGen::AggRefJittable(ExprState* state)
{
    Expr* node = state->expr;

    switch (nodeTag(node)) {
        /*
         * there always be a targetentry in AggrefExprState, so extract
         * targetentry first.
         */
        case T_TargetEntry: {
            GenericExprState* gstate = (GenericExprState*)state;
            ExprState* tstate = gstate->arg;
            if (!AggRefJittable(tstate))
                return false;
        } break;
        case T_Var: {
            Var* var = (Var*)state->expr;
            /* If var is in sysattrlist, we do not codegen the var expr. */
            if (var->varattno < 0)
                return false;

            /* only support int8 and numeric type */
            switch (var->vartype) {
                case INT8OID:
                case NUMERICOID:
                    break;
                default:
                    return false;
            }

            if (var->vartype == NUMERICOID) {
                /*
                 * do not consider numeric data type without precision
                 * specfied, since it changed during evaluation.
                 */
                if (var->vartypmod == -1)
                    return false;
            }

            state->exprCodeGen = (exprFakeCodeGenSig)&VecExprCodeGen::VarCodeGen;
        } break;
        case T_Const: {
            Const* con = (Const*)state->expr;
            switch (con->consttype) {
                /* only support int8 and numeric type */
                case INT8OID:
                case NUMERICOID:
                    break;
                default:
                    return false;
            }
            state->exprCodeGen = (exprFakeCodeGenSig)&VecExprCodeGen::ConstCodeGen;
        } break;
        case T_OpExpr: {
            /* check if have special opexpr with one arg */
            OpExpr* opexpr = (OpExpr*)state->expr;
            FuncExprState* fstate = (FuncExprState*)state;
            if (list_length(opexpr->args) == 1)
                return false;

            switch (opexpr->opno) {
                /*
                 * only support limited math operations
                 */
                case INT8PLOID:
                case INT8MIOID:
                case INT8MULOID:
                case INT8DIVOID:
                case NUMERICADDOID:
                case NUMERICSUBOID:
                case NUMERICMULOID:
                case NUMERICDIVOID:
                    break;
                default:
                    return false;
            }

            /*
             * now consider the args of this operation expression.
             */
            ExprState* lestate = (ExprState*)linitial(fstate->args);
            ExprState* restate = (ExprState*)lsecond(fstate->args);

            if (!AggRefJittable(lestate) || !AggRefJittable(restate))
                return false;

            state->exprCodeGen = (exprFakeCodeGenSig)&VecExprCodeGen::OpCodeGen;
        } break;
        default:
            return false;
    }

    return true;
}

bool VecHashAggCodeGen::AggRefFastJittable(ExprState* state)
{
    Expr* node = state->expr;

    switch (nodeTag(node)) {
        case T_TargetEntry: {
            GenericExprState* gstate = (GenericExprState*)state;
            ExprState* tstate = (ExprState*)(gstate->arg);

            if (!AggRefFastJittable(tstate))
                return false;
        } break;
        case T_Var: {
            Var* var = (Var*)node;
            if (var->vartype != NUMERICOID || var->vartypmod == -1)
                return false;

            /* get the precision of this attribute column */
            int prec = (var->vartypmod >> 16) & 0xFFFF;
            if (prec > 18)
                return false;
        } break;
        case T_Const: {
            Const* cst = (Const*)(state->expr);
            /* only consider not null numeric type const value */
            if (cst->consttype != NUMERICOID || cst->constisnull)
                return false;

            ScalarValue val = ScalarVector::DatumToScalar(cst->constvalue, cst->consttype, cst->constisnull);

            if (!cst->constisnull) {
                if ((((NumericData*)val)->choice.n_header & NUMERIC_BI_MASK) != NUMERIC_64)
                    return false;
            }
        } break;
        case T_OpExpr: {
            /* check if have special opexpr with one arg */
            OpExpr* opexpr = (OpExpr*)state->expr;
            FuncExprState* fstate = (FuncExprState*)state;
            if (list_length(opexpr->args) == 1)
                return false;

            /* only support +, -, * operation in numeric expression */
            switch (opexpr->opno) {
                case NUMERICADDOID:
                case NUMERICSUBOID:
                case NUMERICMULOID:
                    break;
                default:
                    return false;
            }

            ExprState* lestate = (ExprState*)linitial(fstate->args);
            ExprState* restate = (ExprState*)lsecond(fstate->args);

            if (!AggRefFastJittable(lestate) || !AggRefFastJittable(restate))
                return false;
        } break;
        default:
            Assert(0);
            return false;
    }

    return true;
}

bool VecHashAggCodeGen::AgghashingJittable(VecAggState* node)
{
    VecAgg* vecagg = (VecAgg*)(node->ss.ps.plan);

    if (!u_sess->attr.attr_sql.enable_codegen || IS_PGXC_COORDINATOR)
        return false;

    /* Only support hash aggeregation */
    if (vecagg->aggstrategy != AGG_HASHED)
        return false;

    /*
     * Get the input variable from the outer plan, and we only
     * support char, varchar, text, int4, int8 and timestamp(date).
     */
    List* tlist = (outerPlan(vecagg))->targetlist;
    int numkeys = vecagg->numCols;
    AttrNumber* keyIdx = vecagg->grpColIdx;

    for (int i = 0; i < numkeys; i++) {
        AttrNumber key = keyIdx[i] - 1;
        TargetEntry* tle = (TargetEntry*)list_nth(tlist, key);

        /* only support variable expr */
        switch (nodeTag(tle->expr)) {
            case T_Var: {
                Var* var = (Var*)(tle->expr);
                switch (var->vartype) {
                    case BPCHAROID: {
                        int len = var->vartypmod - VARHDRSZ;

                        /* no codegeneration for unknown length */
                        if (len < 0)
                            return false;
                    } break;
                    case INT4OID:
                    case INT8OID:
                    case TIMESTAMPOID:
                    case DATEOID:
                    case TEXTOID:
                    case VARCHAROID:
                        break;
                    default:
                        return false;
                }
            } break;
            case T_FuncExpr: {
                FuncExpr* funcexpr = (FuncExpr*)(tle->expr);
                if (funcexpr->funcid != SUBSTRFUNCOID)
                    return false;

                /* Only support ASCII and UTF-8 encoding */
                int current_encoding = GetDatabaseEncoding();
                if (current_encoding != PG_SQL_ASCII && current_encoding != PG_UTF8)
                    return false;

                List* func_args = funcexpr->args;
                Expr* aexpr1 = (Expr*)linitial(func_args);
                Expr* aexpr2 = (Expr*)lsecond(func_args);
                Expr* aexpr3 = (Expr*)lthird(func_args);

                if (!IsA(aexpr1, RelabelType) && !IsA(aexpr1, Var))
                    return false;

                if (!IsA(aexpr2, Const) || !IsA(aexpr3, Const))
                    return false;
            } break;
            default:
                return false;
        }

        /* only codegen hashint4, hashint8, hashtext,
         * timestamp_hash, and hashbpchar functions
         */
        Oid fnoid = node->hashfunctions[i].fn_oid;
        switch (fnoid) {
            case HASHINT4OID:
            case HASHINT8OID:
            case HASHBPCHAROID:
            case HASHTEXTOID:
                break;
            case TIMESTAMPHASHOID: {
#ifdef HAVE_INT64_TIMESTAMP
#else
                return false;
#endif
            } break;
            default:
                return false;
        }
    }

    return true;
}

bool VecHashAggCodeGen::BatchAggJittable(VecAggState* node, bool isSonic)
{
    int i = 0;
    VecAgg* vecagg = (VecAgg*)(node->ss.ps.plan);
    VecAggStatePerAgg peragg = node->pervecagg;

    if (!u_sess->attr.attr_sql.enable_codegen || IS_PGXC_COORDINATOR)
        return false;

    /* only support hashagg */
    if (vecagg->aggstrategy != AGG_HASHED)
        return false;

    /* if no agg funcs exists, no codegen is needed */
    if (0 == node->numaggs)
        return false;

    for (i = 0; i < node->numaggs; i++) {
        AggrefExprState* aggexprstate = peragg[i].aggrefstate;
        Aggref* aggref = peragg[i].aggref;

        /* only support sum/avg */
        switch (aggref->aggfnoid) {
            case INT8AVGFUNCOID:
            case INT8SUMFUNCOID:
                if (isSonic)
                    return false;
                break;
            case NUMERICAVGFUNCOID:
            case NUMERICSUMFUNCOID:
            case COUNTOID:
                break;
            default:
                return false;
        }

        /* count(*) has no args */
        if (aggref->aggfnoid == COUNTOID)
            continue;

        ExprState* estate = (ExprState*)linitial(aggexprstate->args);
        /* We only support simple expression cases */
        if (!AggRefJittable(estate))
            return false;
    }

    return true;
}

void VecHashAggCodeGen::HashAggCodeGen(VecAggState* node)
{
    /*
     * Codegeneration for hashagg:
     * Since the whole HashAggRunner::BuildAggTbl has been divided into three
     * part, we should do codegeneration separately.
     */
    Assert(NULL != (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj);
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::Function* jitted_vechashing = NULL;
    llvm::Function* jitted_vecsglhashing = NULL;
    llvm::Function* jitted_vecbatchagg = NULL;
    llvm::Function* jitted_SortAggMatchKey = NULL;

    /*
     * For aggregation, if economy is too small, which means number of distinct
     * values is very large, we meet cachemiss during batch aggregation, so
     * consider prefetch in this case.
     */
    if (0) {
        jitted_vechashing = AgghashingWithPrefetchCodeGenorSglTbl<false>(node);
        jitted_vecsglhashing = AgghashingWithPrefetchCodeGenorSglTbl<true>(node);
    } else {
        jitted_vechashing = AgghashingCodeGenorSglTbl<false>(node);
        jitted_vecsglhashing = AgghashingCodeGenorSglTbl<true>(node);
    }

    if (NULL != jitted_vechashing)
        llvmCodeGen->addFunctionToMCJit(jitted_vechashing, reinterpret_cast<void**>(&(node->jitted_hashing)));

    if (NULL != jitted_vecsglhashing)
        llvmCodeGen->addFunctionToMCJit(jitted_vecsglhashing, reinterpret_cast<void**>(&(node->jitted_sglhashing)));

    /* Codegeneration for BatchAggregation in buildAggTbl */
    jitted_vecbatchagg = dorado::VecHashAggCodeGen::BatchAggregationCodeGen(node, false);
    if (NULL != jitted_vecbatchagg)
        llvmCodeGen->addFunctionToMCJit(jitted_vecbatchagg, reinterpret_cast<void**>(&(node->jitted_batchagg)));

    /* Codegeneration for sortagg */
    jitted_SortAggMatchKey = dorado::VecSortCodeGen::SortAggMatchKeyCodeGen(node);
    node->jitted_SortAggMatchKey = NULL;
    if (NULL != jitted_SortAggMatchKey)
        llvmCodeGen->addFunctionToMCJit(
            jitted_SortAggMatchKey, reinterpret_cast<void**>(&(node->jitted_SortAggMatchKey)));

    /* Codegenration for targetlist of aggregation */
    llvm::Function* jitted_vectarget = NULL;
    jitted_vectarget = dorado::VecExprCodeGen::TargetListCodeGen(node->ss.ps.targetlist, (PlanState*)node);
    if (NULL != jitted_vectarget)
        llvmCodeGen->addFunctionToMCJit(
            jitted_vectarget, reinterpret_cast<void**>(&(node->ss.ps.ps_ProjInfo->jitted_vectarget)));
}

void VecHashAggCodeGen::SonicHashAggCodeGen(VecAggState* node)
{
    Assert(NULL != (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj);
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::Function* jitted_sonicbatchagg = NULL;

    jitted_sonicbatchagg = SonicBatchAggregationCodeGen(node, false);

    if (NULL != jitted_sonicbatchagg)
        llvmCodeGen->addFunctionToMCJit(jitted_sonicbatchagg, reinterpret_cast<void**>(&(node->jitted_sonicbatchagg)));
}

/* Check if existing tuple is unique */
static void VecAggCheckUnique(VecAgg* vecagg, GsCodeGen::LlvmBuilder* builder)
{
    const char* errorstr = "more than one row returned by a subquery used as an expression";

    if (vecagg->unique_check) {
        /* If uniqueness is not checked, the result is incorrect. */
        DebugerCodeGen::CodeGenElogInfo(builder, ERROR, errorstr);
    }
}

template <bool isSglTbl>
llvm::Function* VecHashAggCodeGen::AgghashingCodeGenorSglTbl(VecAggState* node)
{
    Assert(NULL != (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj);
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    /* If the condition can not be satisfied, no need to codegen */
    if (!AgghashingJittable(node))
        return NULL;

    /* Find and load the IR file from the installaion directory */
    llvmCodeGen->loadIRFile();

    /* Extract plan information from node */
    int i = 0;
    VecAgg* vecagg = (VecAgg*)(node->ss.ps.plan);
    int numkeys = vecagg->numCols;
    AttrNumber* keyIdx = vecagg->grpColIdx;

    /* Get LLVM Context and builder */
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    llvm::Value* hAggRunner = NULL;
    llvm::Value* batch = NULL;
    llvm::Value* tmpval = NULL;
    llvm::Value* cmpval = NULL;
    llvm::Value* idx_next = NULL;
    llvm::Value* llvmargs[2];
    llvm::PHINode* phi_idx = NULL;
    llvm::Function* jitted_agghashing = NULL;

    /* Define data types and some llvm consts */
    DEFINE_CG_VOIDTYPE(voidType);
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(int32PtrType, INT4OID);
    DEFINE_CG_PTRTYPE(int64PtrType, INT8OID);
    DEFINE_CG_PTRTYPE(hashCellPtrType, "struct.hashCell");
    DEFINE_CG_PTRPTRTYPE(hashCellPtrPtrType, "struct.hashCell");
    DEFINE_CG_PTRTYPE(vectorBatchPtrType, "class.VectorBatch");
    DEFINE_CG_PTRTYPE(hashAggRunnerPtrType, "class.HashAggRunner");
    DEFINE_CG_PTRTYPE(scalarVecPtrType, "class.ScalarVector");
    DEFINE_CG_PTRTYPE(hashSegTblPtrType, "struct.HashSegTbl");

    DEFINE_CGVAR_INT32(int32_m1, -1);
    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT32(int32_pos_hBOper_cols, pos_hBOper_cols);
    DEFINE_CGVAR_INT32(int32_pos_hBOper_cacheLoc, pos_hBOper_cacheLoc);
    DEFINE_CGVAR_INT32(int32_pos_hAggR_hashVal, pos_hAggR_hashVal);
    DEFINE_CGVAR_INT32(int32_pos_hAggR_hSegTbl, pos_hAggR_hSegTbl);
    DEFINE_CGVAR_INT32(int32_pos_hAggR_hsegmax, pos_hAggR_hsegmax); /* for AgghashingCodeGen */
    DEFINE_CGVAR_INT32(int32_pos_hAggR_hashSize, pos_hAggR_hashSize);
    DEFINE_CGVAR_INT32(int32_pos_bAggR_keyIdxInCell, pos_bAggR_keyIdxInCell);
    DEFINE_CGVAR_INT32(int32_pos_bAggR_Loc, pos_bAggR_Loc);
    DEFINE_CGVAR_INT32(int32_pos_bAggR_keySimple, pos_bAggR_keySimple);
    DEFINE_CGVAR_INT32(int32_pos_hcell_mval, pos_hcell_mval);
    DEFINE_CGVAR_INT32(int32_pos_batch_marr, pos_batch_marr);
    DEFINE_CGVAR_INT32(int32_pos_scalvec_vals, pos_scalvec_vals);
    DEFINE_CGVAR_INT32(int32_pos_scalvec_flag, pos_scalvec_flag);
    DEFINE_CGVAR_INT64(Datum_0, 0);
    DEFINE_CGVAR_INT64(Datum_1, 1);

    /* llvm array values, used to represent the location of some element */
    llvm::Value* Vals[2] = {Datum_0, int32_0};
    llvm::Value* Vals3[3] = {Datum_0, int32_0, int32_0};
    llvm::Value* Vals4[4] = {Datum_0, int32_0, int32_0, int32_0};
    llvm::Value* Vals5[5] = {Datum_0, int32_0, int32_0, int32_0, int32_0};

    const char* name = NULL;
    if (isSglTbl) {
        name = "JittedSglTblAggHashing";
    } else {
        name = "JittedAggHashing";
    }
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, name, voidType);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("hashAggRunner", hashAggRunnerPtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("batch", vectorBatchPtrType));
    jitted_agghashing = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    /* start the main codegen process for hashagg */
    hAggRunner = llvmargs[0];
    batch = llvmargs[1];

    /* get the number of rows of this batch : VectorBatch.m_rows */
    tmpval = builder.CreateInBoundsGEP(batch, Vals);
    llvm::Value* nValues = builder.CreateLoad(int32Type, tmpval, "m_rows");

    /* mask = hashAggRunner.m_hashSize - 1 */
    Vals[1] = int32_pos_hAggR_hashSize;
    tmpval = builder.CreateInBoundsGEP(hAggRunner, Vals);
    llvm::Value* maskval = builder.CreateLoad(int64Type, tmpval, "m_hashSize");
    maskval = builder.CreateSub(maskval, Datum_1, "mask");

    /* HashAggRunner.BaseAggRunner.hashBasedOperator::m_cols */
    Vals4[3] = int32_pos_hBOper_cols;
    llvm::Value* m_colsVal = builder.CreateInBoundsGEP(hAggRunner, Vals4);

    /* define basic block information for batch loop */
    llvm::BasicBlock* entry = &jitted_agghashing->getEntryBlock();
    DEFINE_BLOCK(for_body, jitted_agghashing);
    DEFINE_BLOCK(for_end, jitted_agghashing);
    DEFINE_BLOCK(for_inc, jitted_agghashing);

    /* define basic block information for key-value matching */
    DEFINE_BLOCK(while_body, jitted_agghashing);
    DEFINE_BLOCK(key_match, jitted_agghashing);
    DEFINE_BLOCK(hashval_eq, jitted_agghashing);
    DEFINE_BLOCK(next_cell, jitted_agghashing);
    DEFINE_BLOCK(alloc_hashslot, jitted_agghashing);

    /* define vector structures used for store batch info. in LLVM */
    llvm::Value** keyIdxInCell = (llvm::Value**)palloc(sizeof(llvm::Value*) * numkeys);
    llvm::Value** pVector = (llvm::Value**)palloc(sizeof(llvm::Value*) * numkeys);
    llvm::Value** pFlag = (llvm::Value**)palloc(sizeof(llvm::Value*) * numkeys);

    /* HashAggRunner.BaseAggRunner::m_keyIdxInCell */
    Vals3[0] = Datum_0;
    Vals3[1] = int32_0;
    Vals3[2] = int32_pos_bAggR_keyIdxInCell;
    llvm::Value* cellkeyIdx = builder.CreateInBoundsGEP(hAggRunner, Vals3);
    cellkeyIdx = builder.CreateLoad(int32PtrType, cellkeyIdx, "keyIdxInCellArr");
    for (i = 0; i < numkeys; i++) {
        /* load keyIdx in cell */
        tmpval = llvmCodeGen->getIntConstant(INT4OID, i);
        keyIdxInCell[i] = builder.CreateInBoundsGEP(cellkeyIdx, tmpval);
        keyIdxInCell[i] = builder.CreateLoad(int32Type, keyIdxInCell[i], "m_keyIdxInCell");
    }

    /* load m_arr from batch data */
    Vals[0] = Datum_0;
    Vals[1] = int32_pos_batch_marr;
    tmpval = builder.CreateInBoundsGEP(batch, Vals);
    llvm::Value* tmparr = builder.CreateLoad(scalarVecPtrType, tmpval, "m_arr");
    for (i = 0; i < numkeys; i++) {
        /* load scalarvector from m_arr */
        AttrNumber key = keyIdx[i] - 1;
        Vals[0] = llvmCodeGen->getIntConstant(INT8OID, key);
        Vals[1] = int32_pos_scalvec_vals;
        llvm::Value* pVec = builder.CreateInBoundsGEP(tmparr, Vals);
        pVector[i] = builder.CreateLoad(int64PtrType, pVec, "pVector");

        /* load flag information from m_arr */
        Vals[1] = int32_pos_scalvec_flag;
        llvm::Value* Flag = builder.CreateInBoundsGEP(tmparr, Vals);
        pFlag[i] = builder.CreateLoad(int8PtrType, Flag, "pFlag");
    }

    /*
     * Begin to loop the whole batch to evaluation hashval and initialize
     * the hash cell by matching hashval and key
     */
    builder.SetInsertPoint(entry);
    tmpval = builder.CreateICmpSGT(nValues, int32_0);
    builder.CreateCondBr(tmpval, for_body, for_end);

    builder.SetInsertPoint(for_body);
    phi_idx = builder.CreatePHI(int64Type, 2);

    /* add one for every loop to check the index */
    idx_next = builder.CreateAdd(phi_idx, Datum_1);
    phi_idx->addIncoming(Datum_0, entry);
    phi_idx->addIncoming(idx_next, for_inc);

    /* given the initial hash value */
    llvm::Value* hash_res = int32_m1;
    bool rehash = false;

    /* evaluation the hash value for phi_idx-th tuple */
    for (i = 0; i < numkeys; i++) {
        if (i > 0)
            rehash = true;

        llvm::Function* func_hashbatch = HashBatchCodeGen(node, i, rehash);
        if (func_hashbatch == NULL) {
            ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmodule(MOD_LLVM),
                    errmsg("Failed on generating HashBatchCodeGen!\n")));
        }

        /* load the phi_idx-th value and flag of the current key */
        llvm::Value* pval = builder.CreateInBoundsGEP(pVector[i], phi_idx);
        pval = builder.CreateLoad(int64Type, pval, "pval");
        llvm::Value* pflag = builder.CreateInBoundsGEP(pFlag[i], phi_idx);
        pflag = builder.CreateLoad(int8Type, pflag, "pflag");

        hash_res = builder.CreateCall(func_hashbatch, {pval, pflag, hash_res});
    }

    /* store the hash value (hash_res) to m_hashVal of hashAggRunner */
    /* hashAggRunner.m_hashVal[phi_idx] */
    Vals3[1] = int32_pos_hAggR_hashVal;
    Vals3[2] = phi_idx;
    llvm::Value* hashValSlot = builder.CreateInBoundsGEP(hAggRunner, Vals3);
    llvm::Value* hash_res64 = builder.CreateZExt(hash_res, int64Type);
    builder.CreateStore(hash_res64, hashValSlot);

    /* corresponding to  m_cacheLoc[i] = m_hashVal[i] & mask; (uint64) */
    llvm::Value* cacheLocVal = builder.CreateAnd(hash_res64, maskval, "cacheLoc");
    /* store cacheLocVal to hashAggRunner.BaseAggRunner.hashBasedOperator.m_cacheLoc[i] */
    Vals5[3] = int32_pos_hBOper_cacheLoc;
    Vals5[4] = phi_idx;
    llvm::Value* cacheLoc_i = builder.CreateInBoundsGEP(hAggRunner, Vals5);
    builder.CreateStore(cacheLocVal, cacheLoc_i);
    llvm::Value* segmaxval = NULL;
    llvm::Value* nsegsval = NULL;
    llvm::Value* pos = NULL;
    if (!isSglTbl) {
        /* get  m_hashseg_max from hashAggRunner to calculate nsegs and pos */
        Vals[0] = Datum_0;
        Vals[1] = int32_pos_hAggR_hsegmax;
        /* segmaxval, nsegsval and pos are for AgghashingCodeGen */
        segmaxval = builder.CreateInBoundsGEP(hAggRunner, Vals);
        segmaxval = builder.CreateLoad(int32Type, segmaxval, "segmax");
        segmaxval = builder.CreateSExt(segmaxval, int64Type);
        /* nsegs  =  m_cacheLoc[i] / m_hashseg_max */
        nsegsval = builder.CreateExactUDiv(cacheLocVal, segmaxval, "nsegs");
        nsegsval = builder.CreateTrunc(nsegsval, int32Type);
        /* pos  =  m_cacheLoc[i] % m_hashseg_max */
        pos = builder.CreateSRem(cacheLocVal, segmaxval, "pos");
    }
    /* get m_hashData from hashAggRunner */
    Vals[0] = Datum_0;
    Vals[1] = int32_pos_hAggR_hSegTbl;
    llvm::Value* hashData = builder.CreateInBoundsGEP(hAggRunner, Vals);
    hashData = builder.CreateLoad(hashSegTblPtrType, hashData, "m_hashData");
    if (isSglTbl) {
        Vals[0] = int32_0;
    } else {
        Vals[0] = nsegsval;
    }
    Vals[1] = int32_1;
    llvm::Value* tbldata = builder.CreateInBoundsGEP(hashData, Vals);
    tbldata = builder.CreateLoad(hashCellPtrPtrType, tbldata, "tbl_data");
    if (isSglTbl) {
        tmpval = builder.CreateInBoundsGEP(tbldata, cacheLocVal);
    } else {
        tmpval = builder.CreateInBoundsGEP(tbldata, pos);
    }
    llvm::Value* cellval = builder.CreateLoad(hashCellPtrType, tmpval, "cell");

    /* check if cell is NULL or not */
    tmpval = builder.CreatePtrToInt(cellval, int64Type);
    if (!isSglTbl) {
        cellval = builder.CreateIntToPtr(tmpval, hashCellPtrType);
    }
    cmpval = builder.CreateICmpEQ(tmpval, Datum_0);
    builder.CreateCondBr(cmpval, alloc_hashslot, while_body);

    /* while (cell!= NULL) { compare hash value and do match_key } */
    builder.SetInsertPoint(while_body);
    llvm::PHINode* phi_cell = builder.CreatePHI(hashCellPtrType, 2);

    /* get next cell : cell = cell->flag.m_next: hashCell.flag.m_next */
    builder.SetInsertPoint(next_cell);
    Vals3[0] = Datum_0;
    Vals3[1] = int32_0;
    Vals3[2] = int32_0;
    llvm::Value* nextcellval = builder.CreateInBoundsGEP(phi_cell, Vals3);
    nextcellval = builder.CreateLoad(hashCellPtrType, nextcellval, "nextcellval");
    tmpval = builder.CreatePtrToInt(nextcellval, int64Type);
    cmpval = builder.CreateICmpEQ(tmpval, Datum_0);
    builder.CreateCondBr(cmpval, alloc_hashslot, while_body);

    /* loop over the whole hash cell chain */
    builder.SetInsertPoint(while_body);
    phi_cell->addIncoming(nextcellval, next_cell);
    phi_cell->addIncoming(cellval, for_body);

    /* get hash value from current cell : cell->m_val[m_cols].val */
    tmpval = builder.CreateLoad(int32Type, m_colsVal, "m_cols");
    tmpval = builder.CreateZExt(tmpval, int64Type);
    Vals4[0] = Datum_0;
    Vals4[1] = int32_pos_hcell_mval;
    Vals4[2] = tmpval;
    Vals4[3] = int32_0;
    tmpval = builder.CreateInBoundsGEP(phi_cell, Vals4);
    tmpval = builder.CreateLoad(int64Type, tmpval, "hashval_cell");
    llvm::Value* cmp_hashval = builder.CreateICmpEQ(hash_res64, tmpval);
    builder.CreateCondBr(cmp_hashval, hashval_eq, next_cell);

    /*
     * When hash val is equal, we only consider the result of matchkey.
     * Loop over all the keys, once the key is not matched, get the
     * next cell. If all the keys have been compared, find the next cell
     * or keymatched according to the result. The following code is the
     * codegeneration of the following code:
     *     if (true && match_key(<simple>)(batch, i, cell))
     *     {    ...; break;   }
     *     cell = cell->flag.m_next
     */
    builder.SetInsertPoint(hashval_eq);
    for (i = 0; i < numkeys; i++) {
        llvm::Function* func_matchonekey = MatchOneKeyCodeGen(node, i);
        if (NULL == func_matchonekey) {
            ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmodule(MOD_LLVM),
                    errmsg("Failed on generating MatchOneKey Function!\n")));
        }

        /* load the phi_idx-th value and flag of the current key */
        llvm::Value* pval = builder.CreateInBoundsGEP(pVector[i], phi_idx);
        pval = builder.CreateLoad(int64Type, pval, "pval");
        llvm::Value* pflag = builder.CreateInBoundsGEP(pFlag[i], phi_idx);
        pflag = builder.CreateLoad(int8Type, pflag, "pflag");
        llvm::Value* res = builder.CreateCall(func_matchonekey, {pval, pflag, phi_cell, keyIdxInCell[i]});

        cmpval = builder.CreateICmpEQ(res, Datum_0);

        if (i == numkeys - 1)
            builder.CreateCondBr(cmpval, next_cell, key_match);
        else {
            DEFINE_BLOCK(next_bb, jitted_agghashing);
            builder.CreateCondBr(cmpval, next_cell, next_bb);
            builder.SetInsertPoint(next_bb);
        }
    }

    /*
     * when both hash value and all keys are matched, remember this
     * hash cell and go to next tuple.
     */
    builder.SetInsertPoint(key_match);
    VecAggCheckUnique(vecagg, &builder);
    /* get hashAggRunner.BaseAggRunner.m_Loc[phi_idx] */
    Vals4[0] = Datum_0;
    Vals4[1] = int32_0;
    Vals4[2] = int32_pos_bAggR_Loc;
    Vals4[3] = phi_idx;
    tmpval = builder.CreateInBoundsGEP(hAggRunner, Vals4);
    builder.CreateStore(phi_cell, tmpval);
    builder.CreateBr(for_inc);

    /* if (foundMatch == false){ allocate hash slot and initilize it } */
    builder.SetInsertPoint(alloc_hashslot);
    /* HashAggRunner.BaseAggRunner.m_keySimple */
    Vals3[0] = Datum_0;
    Vals3[1] = int32_0;
    Vals3[2] = int32_pos_bAggR_keySimple;
    llvm::Value* simple_key = builder.CreateInBoundsGEP(hAggRunner, Vals3);
    simple_key = builder.CreateLoad(int32Type, simple_key, "key_simple");
    if (isSglTbl) {
        WarpSglTblAllocHashSlotCodeGen(&builder, hAggRunner, batch, phi_idx, simple_key);
    } else {
        WarpAllocHashSlotCodeGen(&builder, hAggRunner, batch, phi_idx, simple_key);
    }
    builder.CreateBr(for_inc);

    builder.SetInsertPoint(for_inc);
    tmpval = builder.CreateTrunc(idx_next, int32Type);
    tmpval = builder.CreateICmpEQ(tmpval, nValues);
    builder.CreateCondBr(tmpval, for_end, for_body);

    /* return nothing after hashing all the tuples */
    builder.SetInsertPoint(for_end);
    builder.CreateRetVoid();

    llvmCodeGen->FinalizeFunction(jitted_agghashing, vecagg->plan.plan_node_id);

    return jitted_agghashing;
}

/*
 * AgghashingWithPrefetchCodeGen
 * @Description	: Codegeneration for hashing batch and match key in
 *				  buildAggTbl. To reduce cache miss, we need to prefetch
 *				  the hash cell. Different from the original function,
 *				  we use two loops to handle the hashing and match key
 *				  separately, since cache miss mostly happens during
 *				  matching key.
 *
 * AggSglTblhashingWithPrefetchCodeGen
 * @Description	: Codegeneration for hashing batch and match key in
 *				  buildAggTbl. To reduce cache miss, we need to prefetch
 *				  the hash cell. Different from the original function,
 *				  we use two loops to handle the hashing and match key
 *				  separately, since cache miss mostly happens during
 *				  matching key.
 */
template <bool isSglTbl>
llvm::Function* VecHashAggCodeGen::AgghashingWithPrefetchCodeGenorSglTbl(VecAggState* node)
{
    Assert(NULL != (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj);
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    /* If the condition can not be satisfied, no need to codegen */
    if (!AgghashingJittable(node))
        return NULL;

    /* Find and load the IR file from the installaion directory */
    llvmCodeGen->loadIRFile();

    /* Extract plan information from node */
    int i = 0;
    VecAgg* vecagg = (VecAgg*)(node->ss.ps.plan);
    int numkeys = vecagg->numCols;
    AttrNumber* keyIdx = vecagg->grpColIdx;

    /* Get LLVM Context and builder */
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    llvm::Value* hAggRunner = NULL;
    llvm::Value* batch = NULL;
    llvm::Value* tmpval = NULL;
    llvm::Value* cmpval = NULL;
    llvm::Value* idx_next = NULL;
    llvm::Value* llvmargs[2];
    llvm::PHINode* phi_idx = NULL;
    llvm::Function* jitted_agghashing = NULL;

    /* Define data types and some llvm consts */
    DEFINE_CG_VOIDTYPE(voidType);
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(int32PtrType, INT4OID);
    DEFINE_CG_PTRTYPE(int64PtrType, INT8OID);
    DEFINE_CG_PTRTYPE(hashCellPtrType, "struct.hashCell");
    DEFINE_CG_PTRPTRTYPE(hashCellPtrPtrType, "struct.hashCell");
    DEFINE_CG_PTRTYPE(vectorBatchPtrType, "class.VectorBatch");
    DEFINE_CG_PTRTYPE(scalarVecPtrType, "class.ScalarVector");
    DEFINE_CG_PTRTYPE(hashAggRunnerPtrType, "class.HashAggRunner");
    DEFINE_CG_PTRTYPE(hashSegTblPtrType, "struct.HashSegTbl");
    
    DEFINE_CGVAR_INT32(int32_m1, -1);
    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT32(int32_pos_hBOper_cols, pos_hBOper_cols);
    DEFINE_CGVAR_INT32(int32_pos_hBOper_cacheLoc, pos_hBOper_cacheLoc);
    DEFINE_CGVAR_INT32(int32_pos_hAggR_hashVal, pos_hAggR_hashVal);
    DEFINE_CGVAR_INT32(int32_pos_hAggR_hSegTbl, pos_hAggR_hSegTbl);
    DEFINE_CGVAR_INT32(int32_pos_hAggR_hsegmax, pos_hAggR_hsegmax); /* just for AgghashingWithPrefetchCodeGen */
    DEFINE_CGVAR_INT32(int32_pos_hAggR_hashSize, pos_hAggR_hashSize);
    DEFINE_CGVAR_INT32(int32_pos_bAggR_keyIdxInCell, pos_bAggR_keyIdxInCell);
    DEFINE_CGVAR_INT32(int32_pos_bAggR_Loc, pos_bAggR_Loc);
    DEFINE_CGVAR_INT32(int32_pos_bAggR_keySimple, pos_bAggR_keySimple);
    DEFINE_CGVAR_INT32(int32_pos_hcell_mval, pos_hcell_mval);
    DEFINE_CGVAR_INT32(int32_pos_batch_marr, pos_batch_marr);
    DEFINE_CGVAR_INT32(int32_pos_scalvec_vals, pos_scalvec_vals);
    DEFINE_CGVAR_INT32(int32_pos_scalvec_flag, pos_scalvec_flag);
    DEFINE_CGVAR_INT64(Datum_0, 0);
    DEFINE_CGVAR_INT64(Datum_1, 1);

    /* llvm array values, used to represent the location of some element */
    llvm::Value* Vals[2] = {Datum_0, int32_0};
    llvm::Value* Vals3[3] = {Datum_0, int32_0, int32_0};
    llvm::Value* Vals4[4] = {Datum_0, int32_0, int32_0, int32_0};
    llvm::Value* Vals5[5] = {Datum_0, int32_0, int32_0, int32_0, int32_0};
    const char* name = NULL;
    if (isSglTbl) {
        name = "JittedSglTblAggHashingWithPreFetch";
    } else {
        name = "JittedAggHashingWithPreFetch";
    }
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, name, voidType);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("hashAggRunner", hashAggRunnerPtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("batch", vectorBatchPtrType));
    jitted_agghashing = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    /* start the main codegen process for hashagg */
    hAggRunner = llvmargs[0];
    batch = llvmargs[1];

    /* get the number of rows of this batch : VectorBatch.m_rows */
    tmpval = builder.CreateInBoundsGEP(batch, Vals);
    llvm::Value* nValues = builder.CreateLoad(int32Type, tmpval, "m_rows");

    /* mask = hashAggRunner.m_hashSize - 1  */
    Vals[1] = int32_pos_hAggR_hashSize;
    tmpval = builder.CreateInBoundsGEP(hAggRunner, Vals);
    llvm::Value* maskval = builder.CreateLoad(int64Type, tmpval, "m_hashSize");
    maskval = builder.CreateSub(maskval, Datum_1, "mask");

    /* HashAggRunner.BaseAggRunner.hashBasedOperator.m_cols */
    Vals4[3] = int32_pos_hBOper_cols;
    llvm::Value* m_colsVal = builder.CreateInBoundsGEP(hAggRunner, Vals4);

    /* define basic block information for batch loop */
    llvm::BasicBlock* entry = &jitted_agghashing->getEntryBlock();
    DEFINE_BLOCK(hashing_for_body, jitted_agghashing);
    DEFINE_BLOCK(hashing_for_end, jitted_agghashing);
    DEFINE_BLOCK(hashing_for_inc, jitted_agghashing);

    DEFINE_BLOCK(for_body, jitted_agghashing);
    DEFINE_BLOCK(for_end, jitted_agghashing);
    DEFINE_BLOCK(for_inc, jitted_agghashing);

    /* define basic block information for key-value matching */
    DEFINE_BLOCK(while_body, jitted_agghashing);
    DEFINE_BLOCK(key_match, jitted_agghashing);
    DEFINE_BLOCK(hashval_eq, jitted_agghashing);
    DEFINE_BLOCK(next_cell, jitted_agghashing);
    DEFINE_BLOCK(alloc_hashslot, jitted_agghashing);

    /* define vector structures used for store batch info. in LLVM */
    llvm::Value** keyIdxInCell = (llvm::Value**)palloc(sizeof(llvm::Value*) * numkeys);
    llvm::Value** pVector = (llvm::Value**)palloc(sizeof(llvm::Value*) * numkeys);
    llvm::Value** pFlag = (llvm::Value**)palloc(sizeof(llvm::Value*) * numkeys);

    /* HashAggRunner.BaseAggRunner.m_keyIdxInCell */
    Vals3[0] = Datum_0;
    Vals3[1] = int32_0;
    Vals3[2] = int32_pos_bAggR_keyIdxInCell;
    llvm::Value* cellkeyIdx = builder.CreateInBoundsGEP(hAggRunner, Vals3);
    cellkeyIdx = builder.CreateLoad(int32PtrType, cellkeyIdx, "keyIdxInCellArr");
    for (i = 0; i < numkeys; i++) {
        /* load keyIdx in cell */
        tmpval = llvmCodeGen->getIntConstant(INT4OID, i);
        keyIdxInCell[i] = builder.CreateInBoundsGEP(cellkeyIdx, tmpval);
        keyIdxInCell[i] = builder.CreateLoad(int32Type, keyIdxInCell[i], "m_keyIdxInCell");
    }

    /* load m_arr from batch data */
    Vals[0] = Datum_0;
    Vals[1] = int32_pos_batch_marr;
    tmpval = builder.CreateInBoundsGEP(batch, Vals);
    llvm::Value* tmparr = builder.CreateLoad(scalarVecPtrType, tmpval, "m_arr");
    for (i = 0; i < numkeys; i++) {
        /* load scalarvector from m_arr */
        AttrNumber key = keyIdx[i] - 1;
        Vals[0] = llvmCodeGen->getIntConstant(INT8OID, key);
        Vals[1] = int32_pos_scalvec_vals;
        llvm::Value* pVec = builder.CreateInBoundsGEP(tmparr, Vals);
        pVector[i] = builder.CreateLoad(int64PtrType, pVec, "pVector");

        /* load flag information from m_arr */
        Vals[1] = int32_pos_scalvec_flag;
        llvm::Value* Flag = builder.CreateInBoundsGEP(tmparr, Vals);
        pFlag[i] = builder.CreateLoad(int8PtrType, Flag, "pFlag");
    }

    /*
     * Begin to loop the whole batch to evaluation hashval
     */
    builder.SetInsertPoint(entry);
    tmpval = builder.CreateICmpSGT(nValues, int32_0);
    builder.CreateCondBr(tmpval, hashing_for_body, for_end);

    builder.SetInsertPoint(hashing_for_body);
    phi_idx = builder.CreatePHI(int64Type, 2);

    /* add one for every hashing loop to check the index */
    idx_next = builder.CreateAdd(phi_idx, Datum_1);
    phi_idx->addIncoming(Datum_0, entry);
    phi_idx->addIncoming(idx_next, hashing_for_inc);

    /* given the initial hash value */
    llvm::Value* hash_res = int32_m1;
    bool rehash = false;

    /* evaluation the hash value for phi_idx-th tuple */
    for (i = 0; i < numkeys; i++) {
        if (i > 0)
            rehash = true;

        llvm::Function* func_hashbatch = HashBatchCodeGen(node, i, rehash);
        if (func_hashbatch == NULL) {
            ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmodule(MOD_LLVM),
                    errmsg("Failed on generating HashBatchCodeGen!\n")));
        }

        /* load the phi_idx-th value and flag of the current key */
        llvm::Value* pval = builder.CreateInBoundsGEP(pVector[i], phi_idx);
        pval = builder.CreateLoad(int64Type, pval, "pval");
        llvm::Value* pflag = builder.CreateInBoundsGEP(pFlag[i], phi_idx);
        pflag = builder.CreateLoad(int8Type, pflag, "pflag");

        hash_res = builder.CreateCall(func_hashbatch, {pval, pflag, hash_res});
    }

    /* store the hash value (hash_res) to m_hashVal of hashAggRunner */
    /* hashAggRunner.m_hashVal[phi_idx] */
    Vals3[1] = int32_pos_hAggR_hashVal;
    Vals3[2] = phi_idx;
    llvm::Value* hashValSlot = builder.CreateInBoundsGEP(hAggRunner, Vals3);
    llvm::Value* hash_res64 = builder.CreateZExt(hash_res, int64Type);
    builder.CreateStore(hash_res64, hashValSlot);
    builder.CreateBr(hashing_for_inc);

    builder.SetInsertPoint(hashing_for_inc);
    tmpval = builder.CreateTrunc(idx_next, int32Type);
    tmpval = builder.CreateICmpEQ(tmpval, nValues);
    builder.CreateCondBr(tmpval, hashing_for_end, hashing_for_body);

    builder.SetInsertPoint(hashing_for_end);
    builder.CreateBr(for_body);

    /*
     * Ending the hashing loop and starting the loop for match_key.
     * Initializing hash cell according to the match key result.
     */
    builder.SetInsertPoint(for_body);
    phi_idx = builder.CreatePHI(int64Type, 2);

    /* add one for every match key loop to check the index */
    idx_next = builder.CreateAdd(phi_idx, Datum_1);
    phi_idx->addIncoming(Datum_0, hashing_for_end);
    phi_idx->addIncoming(idx_next, for_inc);

    /* Prefetch hashData[m_cacheLoc[i+2]] and &(hashData[m_cacheLoc[i+4]]) */
    if (isSglTbl) {
        llvm::Function* func_sgltblprefetch = llvmCodeGen->module()->getFunction("prefetchAggSglTblHashing");
        if (func_sgltblprefetch == NULL) {
            func_sgltblprefetch = prefetchAggSglTblHashingCodeGen();
        }
        llvm::Value* nrows = builder.CreateZExt(nValues, int64Type);
        builder.CreateCall(func_sgltblprefetch, {hAggRunner, phi_idx, nrows});
    } else {
        llvm::Function* func_prefetch = llvmCodeGen->module()->getFunction("prefetchAggHashing");
        if (func_prefetch == NULL) {
            func_prefetch = prefetchAggHashingCodeGen();
        }
        llvm::Value* nrows = builder.CreateZExt(nValues, int64Type);
        builder.CreateCall(func_prefetch, {hAggRunner, phi_idx, nrows});
    }
    /* load the hash value (hash_res) from m_hashVal of hashAggRunner */
    /* hashAggRunner.m_hashVal[phi_idx] */
    Vals3[1] = int32_pos_hAggR_hashVal;
    Vals3[2] = phi_idx;
    hashValSlot = builder.CreateInBoundsGEP(hAggRunner, Vals3);
    hash_res64 = builder.CreateLoad(int64Type, hashValSlot, "m_hashVal");
    hash_res = builder.CreateTrunc(hash_res64, int32Type);

    /* corresponding to  m_cacheLoc[i] = m_hashVal[i] & mask; (uint64) */
    llvm::Value* cacheLocVal = builder.CreateAnd(hash_res64, maskval, "cacheLoc");
    /* store cacheLocVal to hashAggRunner.BaseAggRunner.hashBasedOperator.m_cacheLoc[i] */
    Vals5[3] = int32_pos_hBOper_cacheLoc;
    Vals5[4] = phi_idx;
    llvm::Value* cacheLoc_i = builder.CreateInBoundsGEP(hAggRunner, Vals5);
    builder.CreateStore(cacheLocVal, cacheLoc_i);

    /* segmaxval, nsegsval and pos are for AgghashingCodeGen */
    llvm::Value* segmaxval = NULL;
    llvm::Value* nsegsval = NULL;
    llvm::Value* pos = NULL;
    /* get  m_hashseg_max from hashAggRunner to calculate nsegs and pos */
    if (!isSglTbl) {
        Vals[0] = Datum_0;
        Vals[1] = int32_pos_hAggR_hsegmax;
        segmaxval = builder.CreateInBoundsGEP(hAggRunner, Vals);
        segmaxval = builder.CreateLoad(int32Type, segmaxval, "segmax");
        segmaxval = builder.CreateSExt(segmaxval, int64Type);
        /* nsegs  =  m_cacheLoc[i] / m_hashseg_max */
        nsegsval = builder.CreateExactUDiv(cacheLocVal, segmaxval, "nsegs");
        nsegsval = builder.CreateTrunc(nsegsval, int32Type);
        /* pos  =  m_cacheLoc[i] % m_hashseg_max */
        pos = builder.CreateSRem(cacheLocVal, segmaxval, "pos");
    }
    /* get m_hashData from hashAggRunner */
    Vals[0] = Datum_0;
    Vals[1] = int32_pos_hAggR_hSegTbl;
    llvm::Value* hashData = builder.CreateInBoundsGEP(hAggRunner, Vals);
    hashData = builder.CreateLoad(hashSegTblPtrType, hashData, "m_hashData");
    if (isSglTbl) {
        Vals[0] = int32_0;
    } else {
        Vals[0] = nsegsval;
    }
    Vals[1] = int32_1;
    llvm::Value* tbldata = builder.CreateInBoundsGEP(hashData, Vals);
    tbldata = builder.CreateLoad(hashCellPtrPtrType, tbldata, "tbl_data");
    if (isSglTbl) {
        tmpval = builder.CreateInBoundsGEP(tbldata, cacheLocVal);
    } else {
        tmpval = builder.CreateInBoundsGEP(tbldata, pos);
    }
    llvm::Value* cellval = builder.CreateLoad(hashCellPtrType, tmpval, "cell");
    /* check if cell is NULL or not */
    tmpval = builder.CreatePtrToInt(cellval, int64Type);
    cmpval = builder.CreateICmpEQ(tmpval, Datum_0);
    builder.CreateCondBr(cmpval, alloc_hashslot, while_body);

    /* while (cell!= NULL) { compare hash value and do match_key } */
    builder.SetInsertPoint(while_body);
    llvm::PHINode* phi_cell = builder.CreatePHI(hashCellPtrType, 2);

    /* get next cell : cell = cell->flag.m_next: hashCell.flag.m_next */
    builder.SetInsertPoint(next_cell);
    Vals3[0] = Datum_0;
    Vals3[1] = int32_0;
    Vals3[2] = int32_0;
    llvm::Value* nextcellval = builder.CreateInBoundsGEP(phi_cell, Vals3);
    nextcellval = builder.CreateLoad(hashCellPtrType, nextcellval, "nextcellval");
    tmpval = builder.CreatePtrToInt(nextcellval, int64Type);
    cmpval = builder.CreateICmpEQ(tmpval, Datum_0);
    builder.CreateCondBr(cmpval, alloc_hashslot, while_body);

    /* loop over the whole hash cell chain */
    builder.SetInsertPoint(while_body);
    phi_cell->addIncoming(nextcellval, next_cell);
    phi_cell->addIncoming(cellval, for_body);

    /* get hash value from current cell : cell->m_val[m_cols].val */
    tmpval = builder.CreateLoad(int32Type, m_colsVal, "m_cols");
    tmpval = builder.CreateZExt(tmpval, int64Type);
    Vals4[0] = Datum_0;
    Vals4[1] = int32_pos_hcell_mval;
    Vals4[2] = tmpval;
    Vals4[3] = int32_0;
    tmpval = builder.CreateInBoundsGEP(phi_cell, Vals4);
    tmpval = builder.CreateLoad(int64Type, tmpval, "hashval_cell");
    llvm::Value* cmp_hashval = builder.CreateICmpEQ(hash_res64, tmpval);
    builder.CreateCondBr(cmp_hashval, hashval_eq, next_cell);

    /*
     * When hash val is equal, we only consider the result of matchkey.
     * Loop over all the keys, once the key is not matched, get the
     * next cell. If all the keys have been compared, find the next cell
     * or keymatched according to the result. The following code is the
     * codegeneration of the following code:
     *     if (true && match_key(<simple>)(batch, i, cell))
     *     {    ...; break;   }
     *     cell = cell->flag.m_next
     */
    builder.SetInsertPoint(hashval_eq);
    for (i = 0; i < numkeys; i++) {
        llvm::Function* func_matchonekey = MatchOneKeyCodeGen(node, i);
        if (NULL == func_matchonekey) {
            ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                    errmodule(MOD_LLVM),
                    errmsg("Failed on generating MatchOneKey Function!\n")));
        }

        /* load the phi_idx-th value and flag of the current key */
        llvm::Value* pval = builder.CreateInBoundsGEP(pVector[i], phi_idx);
        pval = builder.CreateLoad(int64Type, pval, "pval");
        llvm::Value* pflag = builder.CreateInBoundsGEP(pFlag[i], phi_idx);
        pflag = builder.CreateLoad(int8Type, pflag, "pflag");
        llvm::Value* res = builder.CreateCall(func_matchonekey, {pval, pflag, phi_cell, keyIdxInCell[i]});

        cmpval = builder.CreateICmpEQ(res, Datum_0);

        if (i == numkeys - 1)
            builder.CreateCondBr(cmpval, next_cell, key_match);
        else {
            DEFINE_BLOCK(next_bb, jitted_agghashing);
            builder.CreateCondBr(cmpval, next_cell, next_bb);
            builder.SetInsertPoint(next_bb);
        }
    }

    /*
     * when both hash value and all keys are matched, remember this
     * hash cell and go to next tuple.
     */
    builder.SetInsertPoint(key_match);
    VecAggCheckUnique(vecagg, &builder);
    /* get hashAggRunner.BaseAggRunner.m_Loc[phi_idx] */
    Vals4[0] = Datum_0;
    Vals4[1] = int32_0;
    Vals4[2] = int32_pos_bAggR_Loc;
    Vals4[3] = phi_idx;
    tmpval = builder.CreateInBoundsGEP(hAggRunner, Vals4);
    builder.CreateStore(phi_cell, tmpval);
    builder.CreateBr(for_inc);

    /* if (foundMatch == false){ allocate hash slot and initilize it } */
    builder.SetInsertPoint(alloc_hashslot);
    /* HashAggRunner.BaseAggRunner.m_keySimple */
    Vals3[0] = Datum_0;
    Vals3[1] = int32_0;
    Vals3[2] = int32_pos_bAggR_keySimple;
    llvm::Value* simple_key = builder.CreateInBoundsGEP(hAggRunner, Vals3);
    simple_key = builder.CreateLoad(int32Type, simple_key, "key_simple");
    WarpAllocHashSlotCodeGen(&builder, hAggRunner, batch, phi_idx, simple_key);
    builder.CreateBr(for_inc);

    builder.SetInsertPoint(for_inc);
    tmpval = builder.CreateTrunc(idx_next, int32Type);
    tmpval = builder.CreateICmpEQ(tmpval, nValues);
    builder.CreateCondBr(tmpval, for_end, for_body);

    /* return nothing after hashing all the tuples */
    builder.SetInsertPoint(for_end);
    builder.CreateRetVoid();

    llvmCodeGen->FinalizeFunction(jitted_agghashing, vecagg->plan.plan_node_id);

    return jitted_agghashing;
}

llvm::Function* VecHashAggCodeGen::BatchAggregationCodeGen(VecAggState* node, bool use_prefetch)
{
    /* First get the basic information of VecAggState */
    int numaggs = node->numaggs;
    VecAggStatePerAgg peragg = node->pervecagg;

    Assert(NULL != (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj);
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    if (!BatchAggJittable(node, false))
        return NULL;

    /* Find and load the IR file from the installaion directory */
    llvmCodeGen->loadIRFile();

    /* Get LLVM Context and builder */
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);
    llvm::Module* mod = llvmCodeGen->module();

    int i;
    int exprscale = 0;
    bool fast_aggref = false;
    ExprState* estate = NULL;
    llvm::Value* nValues = NULL;
    llvm::Value* tmpval = NULL;
    llvm::Value* idx_next = NULL;
    llvm::Value* cell = NULL;
    llvm::Value* result = NULL;
    llvm::Value* expres = NULL;
    llvm::Value* llvmargs[4];
    Aggref* aggref = NULL;
    llvm::Function* jitted_batchagg = NULL;

    /* Define data types and some llvm consts */
    DEFINE_CG_VOIDTYPE(voidType);
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int16Type, INT2OID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(int32PtrType, INT4OID);
    DEFINE_CG_PTRTYPE(int64PtrType, INT8OID);
    DEFINE_CG_PTRTYPE(hashCellPtrType, "struct.hashCell");
    DEFINE_CG_PTRPTRTYPE(hashCellPtrPtrType, "struct.hashCell");
    DEFINE_CG_PTRTYPE(exprCxtPtrType, "struct.ExprContext");
    DEFINE_CG_PTRTYPE(vecBatchPtrType, "class.VectorBatch");
    DEFINE_CG_PTRTYPE(scalarVecPtrType, "class.ScalarVector");
    DEFINE_CG_PTRTYPE(hashAggRunnerPtrType, "class.HashAggRunner");
    DEFINE_CG_PTRTYPE(numericPtrType, "struct.NumericData");
    DEFINE_CG_PTRTYPE(memCxtDataPtrType, "struct.MemoryContextData");

    /* create LLVM value with {uint16, int64} format type */
    llvm::Type* Elements[] = {int16Type, int64Type};
    llvm::Type* SiNumeric64Type = llvm::StructType::create(context, Elements, "SiNumeric64");

    DEFINE_CGVAR_INT8(int8_0, 0);
    DEFINE_CGVAR_INT8(int8_1, 1);
    DEFINE_CGVAR_INT16(val_mask, NUMERIC_BI_MASK);
    DEFINE_CGVAR_INT16(val_binum64, NUMERIC_64);
    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT64(int64_0, 0);
    DEFINE_CGVAR_INT64(int64_1, 1);
    DEFINE_CGVAR_INT64(int64_6, 6);
    DEFINE_CGVAR_INT32(int32_pos_batch_marr, pos_batch_marr);
    DEFINE_CGVAR_INT32(int32_pos_scalvec_vals, pos_scalvec_vals);
    DEFINE_CGVAR_INT32(int32_pos_scalvec_flag, pos_scalvec_flag);
    DEFINE_CGVAR_INT32(int32_pos_ecxt_pertuple, pos_ecxt_pertuple);
    DEFINE_CGVAR_INT32(int32_pos_ecxt_outerbatch, pos_ecxt_outerbatch);
    DEFINE_CGVAR_INT32(int32_pos_hBOper_hcxt, pos_hBOper_hcxt);
    DEFINE_CGVAR_INT32(int32_pos_bAggR_econtext, pos_bAggR_econtext);
    DEFINE_CGVAR_INT32(int32_pos_hcell_mval, pos_hcell_mval);

    /* llvm array values, used to represent the location of some element */
    llvm::Value* Vals[2] = {int64_0, int32_0};
    llvm::Value* Vals3[3] = {int64_0, int32_0, int32_0};
    llvm::Value* Vals4[4] = {int64_0, int32_0, int32_0, int32_0};

    llvm::Value** aggIdxList = (llvm::Value**)palloc(sizeof(llvm::Value*) * numaggs);
    llvm::Value** batch_vals = (llvm::Value**)palloc(sizeof(llvm::Value*) * numaggs);
    llvm::Value** batch_flag = (llvm::Value**)palloc(sizeof(llvm::Value*) * numaggs);
    llvm::BasicBlock** agg_bb = (llvm::BasicBlock**)palloc(sizeof(llvm::BasicBlock*) * numaggs);
    llvm::BasicBlock** flag_then = (llvm::BasicBlock**)palloc(sizeof(llvm::BasicBlock*) * numaggs);
    llvm::BasicBlock** flag_else = (llvm::BasicBlock**)palloc(sizeof(llvm::BasicBlock*) * numaggs);

    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "JittedFastBatchAgg", voidType);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("haRuner", hashAggRunnerPtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("Loc", hashCellPtrPtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("batch", vecBatchPtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("aggIdx", int32PtrType));
    jitted_batchagg = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* hAggRunner = llvmargs[0];
    llvm::Value* hashCellPP = llvmargs[1];
    llvm::Value* batch = llvmargs[2];
    llvm::Value* aggIdx = llvmargs[3];

    /* parameter used to mark if this tuple is NULL or not */
    llvm::Value* isNull = builder.CreateAlloca(int8Type);

    /* HashAggRunner.BaseAggRunner.hashBasedOperator::m_hashContext */
    Vals4[3] = int32_pos_hBOper_hcxt;
    llvm::Value* hcxt = builder.CreateInBoundsGEP(hAggRunner, Vals4);
    hcxt = builder.CreateLoad(memCxtDataPtrType, hcxt, "hashContext");

    /* get the nrows of the batch */
    tmpval = builder.CreateInBoundsGEP(batch, Vals);
    nValues = builder.CreateLoad(int32Type, tmpval, "m_rows");

    /* get vectorBatch.m_arr of the batch */
    Vals[0] = int64_0;
    Vals[1] = int32_pos_batch_marr;
    llvm::Value* argVector = builder.CreateInBoundsGEP(batch, Vals);
    argVector = builder.CreateLoad(scalarVecPtrType, argVector, "m_arr");

    /* pre-load all the expression context */
    llvm::Value** econtext = (llvm::Value**)palloc(sizeof(llvm::Value*) * numaggs);
    for (i = 0; i < numaggs; i++) {
        econtext[i] = NULL;
        VecAggStatePerAgg peraggstate = &node->pervecagg[numaggs - i - 1];
        aggref = (Aggref*)(peraggstate->aggref);
        if (peraggstate->evalproj != NULL && aggref->aggfnoid != COUNTOID) {
            ExprContext* exprcontext = peragg[numaggs - 1 - i].evalproj->pi_exprContext;
            econtext[i] = llvmCodeGen->CastPtrToLlvmPtr(exprCxtPtrType, exprcontext);
        }
    }

    /* define the basic block needed in the main process */
    llvm::BasicBlock* entry = &jitted_batchagg->getEntryBlock();
    DEFINE_BLOCK(for_body, jitted_batchagg);
    DEFINE_BLOCK(for_inc, jitted_batchagg);
    DEFINE_BLOCK(for_end, jitted_batchagg);

    /* get all addIdx of agg operators */
    for (i = 0; i < numaggs; i++) {
        llvm::Value* tmpidx = llvmCodeGen->getIntConstant(INT4OID, i);
        tmpval = builder.CreateInBoundsGEP(aggIdx, tmpidx);
        tmpval = builder.CreateLoad(int32Type, tmpval, "aggIdx");
        aggIdxList[i] = builder.CreateSExt(tmpval, int64Type);

        agg_bb[i] = llvm::BasicBlock::Create(context, "agg_bb", jitted_batchagg);
        flag_then[i] = llvm::BasicBlock::Create(context, "flag_then", jitted_batchagg);
        flag_else[i] = llvm::BasicBlock::Create(context, "flag_else", jitted_batchagg);
    }

    /*
     * Start the main process for batchaggregation, which has the following
     * pedudo code:
     * for (j = 0; j < nrows; j++){
     *	   for (i = 0; i < m_aggNum; i++){
     *		   peraggstate = peragg[numaggs - 1 - i];
     *		   pbatch = ExecVecProject (peraggstate->evalproj)
     *		   AggregationOnScalar(aggInfo[i], &pbatch->m_arr[0], aggidx[i], m_Loc)
     *	   }
     * }
     */
    builder.SetInsertPoint(entry);
    /*
     * First get the ecxt_per_tuple_memory, since we need to switch to this
     * memory context.
     */
    /* HashAggRunner.BaseAggRunner.m_econtext */
    Vals3[2] = int32_pos_bAggR_econtext;
    llvm::Value* mecontext = builder.CreateInBoundsGEP(hAggRunner, Vals3);
    mecontext = builder.CreateLoad(exprCxtPtrType, mecontext, "m_econtext");
    Vals[1] = int32_pos_ecxt_pertuple;
    llvm::Value* agg_expr_context = builder.CreateInBoundsGEP(mecontext, Vals);
    agg_expr_context = builder.CreateLoad(memCxtDataPtrType, agg_expr_context, "agg_per_tuple_memory");
    llvm::Value* agg_oldcontext = VecExprCodeGen::MemCxtSwitToCodeGen(&builder, agg_expr_context);

    /*
     * Load value and flag from batch before the batch loop when we have
     * simple vars in transition level.
     */
    for (i = 0; i < numaggs; i++) {
        int numSimpleVars = 0;
        VecAggStatePerAgg peraggstate = &node->pervecagg[numaggs - i - 1];
        aggref = (Aggref*)(peraggstate->aggref);
        ProjectionInfo* projInfo = (ProjectionInfo*)(peraggstate->evalproj);

        if (aggref->aggstage == 0 && aggref->aggfnoid != COUNTOID) {
            numSimpleVars = projInfo->pi_numSimpleVars;
            if (numSimpleVars > 0) {
                int* varNumbers = projInfo->pi_varNumbers;
                int varNumber = varNumbers[0] - 1;
                /* m_arr[varNumber].m_vals */
                Vals[0] = llvmCodeGen->getIntConstant(INT8OID, varNumber);
                Vals[1] = int32_pos_scalvec_vals;
                tmpval = builder.CreateInBoundsGEP(argVector, Vals);
                tmpval = builder.CreateLoad(int64PtrType, tmpval, "m_vals");
                batch_vals[i] = tmpval;

                /* m_arr[varNumber].m_flag */
                Vals[1] = int32_pos_scalvec_flag;
                llvm::Value* argFlag = builder.CreateInBoundsGEP(argVector, Vals);
                argFlag = builder.CreateLoad(int8PtrType, argFlag, "m_flag");
                batch_flag[i] = argFlag;
            }
        }
    }

    tmpval = builder.CreateICmpSGT(nValues, int32_0);
    builder.CreateCondBr(tmpval, for_body, for_end);

    builder.SetInsertPoint(for_body);
    llvm::PHINode* phi_idx = builder.CreatePHI(int64Type, 2);

    /* after each loop, index plus one */
    idx_next = builder.CreateAdd(phi_idx, int64_1);

    phi_idx->addIncoming(int64_0, entry);
    phi_idx->addIncoming(idx_next, for_inc);

    /* define prefetch function to prefetch loc[i+2] to avoid cache miss */
    if (use_prefetch) {
        llvm::Function* func_prefetch = llvmCodeGen->module()->getFunction("prefetchBatchAggregation");
        if (NULL == func_prefetch) {
            func_prefetch = prefetchBatchAggregationCodeGen();
        }
        llvm::Value* nrows = builder.CreateZExt(nValues, int64Type);
        builder.CreateCall(func_prefetch, {hashCellPP, phi_idx, nrows});
    }

    /*
     * get the hashcell : cell = Loc[i] (see vnumeric_sum and vint8_sum)
     * and check if it is NULL
     */
    tmpval = builder.CreateInBoundsGEP(hashCellPP, phi_idx);
    cell = builder.CreateLoad(hashCellPtrType, tmpval, "hashCell");
    tmpval = builder.CreatePtrToInt(cell, int64Type);
    tmpval = builder.CreateICmpEQ(tmpval, int64_0);
    builder.CreateCondBr(tmpval, for_inc, agg_bb[0]);

    /* loop over the numaggs */
    int numSimpleVars = 0;
    for (i = 0; i < numaggs; i++) {
        llvm::BasicBlock* bisum_bb = NULL;
        llvm::BasicBlock* numsum_bb = NULL;

        /* the inverse order */
        VecAggStatePerAgg peraggstate = &node->pervecagg[numaggs - i - 1];
        aggref = (Aggref*)(peraggstate->aggref);
        ProjectionInfo* projInfo = (ProjectionInfo*)(peraggstate->evalproj);

        /* start the codegeneration for each aggregation */
        builder.SetInsertPoint(agg_bb[i]);
        if (aggref->aggstage == 0) {
            if (aggref->aggfnoid != COUNTOID) {
                Assert(peraggstate->evalproj != NULL);
                AggrefExprState* aggexprstate = peraggstate->aggrefstate;
                /* check if current expression can be codegened in fast path or not */
                estate = (ExprState*)linitial(aggexprstate->args);
                fast_aggref = AggRefFastJittable(estate);

                /*
                 * Do not consider collection and finalization level for
                 * numeric_avg to avoid deconstruct_array.
                 */
                if (aggref->aggfnoid == NUMERICAVGFUNCOID && aggref->aggstage > 0)
                    fast_aggref = false;

                /* If the current agg expression is just a simple var,
                 * load it from the batch directly
                 */
                if (projInfo == NULL) {
                    ereport(ERROR,
                        (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                            errmodule(MOD_LLVM),
                            errmsg("Unexpected NULL project information.")));
                }
                numSimpleVars = projInfo->pi_numSimpleVars;
                if (numSimpleVars > 0) {
                    /* m_arr[varNumber].m_vals */
                    tmpval = batch_vals[i];
                    tmpval = builder.CreateInBoundsGEP(tmpval, phi_idx);
                    result = builder.CreateLoad(int64Type, tmpval, "val");

                    /* m_arr[varNumber].m_flag */
                    tmpval = batch_flag[i];
                    tmpval = builder.CreateInBoundsGEP(tmpval, phi_idx);
                    tmpval = builder.CreateLoad(int8Type, tmpval, "flag");
                    builder.CreateStore(tmpval, isNull);
                } else {
                    /* set the batch information : econtext->ecxt_outerbatch = batch */
                    Vals[0] = int64_0;
                    Vals[1] = int32_pos_ecxt_outerbatch;
                    llvm::Value* tmp_outerbatch = builder.CreateInBoundsGEP(econtext[i], Vals);
                    builder.CreateStore(batch, tmp_outerbatch);

                    /*
                     * If fast_aggref is true, we could try to evaluate the
                     * expression value by using BI64 all the way, and turn
                     * to original path once meet outofbound.
                     */
                    if (fast_aggref) {
                        llvm::BasicBlock* bb_last = builder.GetInsertBlock();
                        DEFINE_BLOCK(bb_null, jitted_batchagg);
                        DEFINE_BLOCK(bb_outofbound, jitted_batchagg);

                        if (NULL == bisum_bb) {
                            bisum_bb = llvm::BasicBlock::Create(context, "bisum_bb", jitted_batchagg);
                        }

                        if (NULL == numsum_bb) {
                            numsum_bb = llvm::BasicBlock::Create(context, "numsum_bb", jitted_batchagg);
                        }

                        /* evaluate expression result */
                        llvm::Value* tmpexpres = EvalFastExprInBatchAgg(estate,
                            builder,
                            jitted_batchagg,
                            &bb_null,
                            &bb_last,
                            &bb_outofbound,
                            econtext[i],
                            argVector,
                            phi_idx);

                        /* expres is already in {int16, int64} format */
                        builder.SetInsertPoint(bb_last);
                        builder.CreateStore(int8_0, isNull);
                        llvm::Value* tmp_scale = builder.CreateExtractValue(tmpexpres, 0);
                        llvm::Value* tmp_value = builder.CreateExtractValue(tmpexpres, 1);
                        expres = llvm::UndefValue::get(SiNumeric64Type);
                        expres = builder.CreateInsertValue(expres, tmp_scale, 0);
                        expres = builder.CreateInsertValue(expres, tmp_value, 1);
                        builder.CreateBr(bisum_bb);

                        /* construct a null value, and no need to do aggregation */
                        builder.SetInsertPoint(bb_null);
                        builder.CreateStore(int8_1, isNull);
                        if (i == numaggs - 1)
                            builder.CreateBr(for_inc);
                        else
                            builder.CreateBr(agg_bb[i + 1]);

                        /* if result can not be represented in BI64, turn to
                         * the original path
                         */
                        builder.SetInsertPoint(bb_outofbound);
                        /* Turn to per_tuple_memory to evaluate expression. */
                        Vals[0] = int64_0;
                        Vals[1] = int32_pos_ecxt_pertuple;
                        llvm::Value* curr_Context = builder.CreateInBoundsGEP(econtext[i], Vals);
                        curr_Context = builder.CreateLoad(memCxtDataPtrType, curr_Context, "per_tuple_memory");
                        llvm::Value* cg_oldContext = VecExprCodeGen::MemCxtSwitToCodeGen(&builder, curr_Context);
                        result = EvalSimpleExprInBatchAgg(estate, builder, econtext[i], phi_idx, isNull);
                        /* return back to the old memory context */
                        (void)VecExprCodeGen::MemCxtSwitToCodeGen(&builder, cg_oldContext);
                    } else {
                        /*
                         * corresponding to ExecVecProject(peraggstate->evalproj) :
                         * to evaluate expressions, we should turn to per_tuple_memory.
                         */
                        Vals[0] = int64_0;
                        Vals[1] = int32_pos_ecxt_pertuple;
                        llvm::Value* curr_Context = builder.CreateInBoundsGEP(econtext[i], Vals);
                        curr_Context = builder.CreateLoad(memCxtDataPtrType, curr_Context, "per_tuple_memory");
                        llvm::Value* cg_oldContext = VecExprCodeGen::MemCxtSwitToCodeGen(&builder, curr_Context);
                        /* corresponding to ExecVecProject(peraggstate->evalproj) */
                        result = EvalSimpleExprInBatchAgg(estate, builder, econtext[i], phi_idx, isNull);
                        /* return back to the old memory context */
                        (void)VecExprCodeGen::MemCxtSwitToCodeGen(&builder, cg_oldContext);
                    }
                }
            } else {
                /*
                 * When current stage is transaction and aggfnoid is COUNTOID, no need to
                 * load any batch information. since we only need to plus one when cell
                 * is not null.
                 */
                result = int64_0;
                builder.CreateStore(int8_0, isNull);
            }
        } else {
            /*
             * When aggref->stage is not transiction, the aggref expr is always
             * be var, so get the value from batch directly (projInfo is not null).
             */
            if (projInfo != NULL) {
                int* varNumbers = projInfo->pi_varNumbers;
                int varNumber = varNumbers[0] - 1;
                /* m_arr[varNumber].m_vals */
                Vals[0] = llvmCodeGen->getIntConstant(INT8OID, varNumber);
                Vals[1] = int32_pos_scalvec_vals;
                tmpval = builder.CreateInBoundsGEP(argVector, Vals);
                tmpval = builder.CreateLoad(int64PtrType, tmpval, "m_vals");
                tmpval = builder.CreateInBoundsGEP(tmpval, phi_idx);
                result = builder.CreateLoad(int64Type, tmpval, "val");

                /* m_arr[varNumber].m_flag */
                Vals[1] = int32_pos_scalvec_flag;
                llvm::Value* argFlag = builder.CreateInBoundsGEP(argVector, Vals);
                argFlag = builder.CreateLoad(int8PtrType, argFlag, "m_flag");
                tmpval = builder.CreateInBoundsGEP(argFlag, phi_idx);
                tmpval = builder.CreateLoad(int8Type, tmpval, "flag");
                builder.CreateStore(tmpval, isNull);
            } else {
                ereport(ERROR,
                    (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmodule(MOD_LLVM),
                        errmsg("Unexpected NULL project information.")));
            }
        }

        /* Compute Aggregation */
        if (aggref->aggfnoid == COUNTOID) {
            flag_then[i]->eraseFromParent();
            flag_else[i]->eraseFromParent();

            char* Jittedname = NULL;
            if (aggref->aggstage == 0)
                Jittedname = "Jitted_count_0";
            else
                Jittedname = "Jitted_count_1";

            llvm::Function* func_vcount = llvmCodeGen->module()->getFunction(Jittedname);
            if (NULL == func_vcount) {
                func_vcount = vec_count_codegen(aggref);
            }
            builder.CreateCall(func_vcount, {cell, aggIdxList[i], result});

            if (i == numaggs - 1)
                builder.CreateBr(for_inc);
            else
                builder.CreateBr(agg_bb[i + 1]);
        } else {
            /*
             * now we already get HashCell cell(cellval) and pVector(result), check
             * the flag and do aggregation.
             */
            /* see if IS_NULL(flag[phi_idx]) == false */
            llvm::Value* tmpnull = builder.CreateLoad(int8Type, isNull, "tmpnull");
            tmpnull = builder.CreateAnd(tmpnull, int8_1);
            llvm::Value* flag_cmp = builder.CreateICmpEQ(tmpnull, int8_0);
            builder.CreateCondBr(flag_cmp, flag_then[i], flag_else[i]);

            /* only do when not null */
            builder.SetInsertPoint(flag_then[i]);
            switch (aggref->aggfnoid) {
                case INT8SUMFUNCOID: {
                    llvm::Function* func_vint8sum = llvmCodeGen->module()->getFunction("Jitted_int8sum");
                    if (NULL == func_vint8sum) {
                        func_vint8sum = int8_sum_codegen(aggref);
                    }
                    builder.CreateCall(func_vint8sum, {cell, hcxt, aggIdxList[i], result});
                } break;
                case INT8AVGFUNCOID: {
                    llvm::Function* func_vint8avg = llvmCodeGen->module()->getFunction("Jitted_int8avg");
                    if (NULL == func_vint8avg) {
                        func_vint8avg = int8_avg_codegen(aggref);
                    }
                    builder.CreateCall(func_vint8avg, {cell, hcxt, aggIdxList[i], result});
                } break;
                case NUMERICSUMFUNCOID: {
                    /*
                     * If aggref can be evaluated in fast path and just be
                     * simple vars, use the result from batch.
                     */
                    if (fast_aggref && (numSimpleVars > 0)) {
                        DEFINE_BLOCK(agg_then, jitted_batchagg);
                        DEFINE_BLOCK(agg_else, jitted_batchagg);
                        DEFINE_BLOCK(agg_end, jitted_batchagg);
                        DEFINE_BLOCK(normal_bb, jitted_batchagg);
                        DEFINE_BLOCK(var_bisum_bb, jitted_batchagg);
                        DEFINE_BLOCK(var_numsum_bb, jitted_batchagg);

                        /* get the hash val : hashCell->m_val[aggidx].val */
                        Vals4[0] = int64_0;
                        Vals4[1] = int32_pos_hcell_mval;
                        Vals4[2] = aggIdxList[i];
                        Vals4[3] = int32_0;
                        llvm::Value* cellval = builder.CreateInBoundsGEP(cell, Vals4);

                        /* get the flag of the hash cell and check if it is NULL */
                        Vals4[3] = int32_1;
                        llvm::Value* cellflag = builder.CreateInBoundsGEP(cell, Vals4);
                        tmpval = builder.CreateLoad(int8Type, cellflag, "cellFlag");
                        tmpval = builder.CreateAnd(tmpval, int8_1);
                        tmpval = builder.CreateICmpEQ(tmpval, int8_0);
                        builder.CreateCondBr(tmpval, agg_else, agg_then);

                        /* cell be null, add Variable */
                        builder.SetInsertPoint(agg_then);
                        /*
                         * should make a new context to record the result : the
                         * following code corresponding to:
                         * 'leftarg = DatumGetBINumeric(pVal[i]);
                         * cell->m_val[idx].val = addVariable(context, NumericGetDatum(leftarg));'.
                         */
                        tmpval = DatumGetBINumericCodeGen(&builder, result);
                        tmpval = builder.CreatePtrToInt(tmpval, int64Type);
                        tmpval = WrapaddVariableCodeGen(&builder, hcxt, tmpval);

                        builder.CreateStore(tmpval, cellval);
                        builder.CreateStore(int8_0, cellflag);

                        /* turn to next basicblock or end this */
                        if (i == numaggs - 1)
                            builder.CreateBr(for_inc);
                        else
                            builder.CreateBr(agg_bb[i + 1]);

                        /* cell be not null, do aggregation */
                        builder.SetInsertPoint(agg_else);

                        /*
                         * When fast_aggref is true and numSimpleVars is greater than zero,
                         * the expr is numeric type var. Convert this numeric type data to
                         * SiNumeric data to get the value.
                         */
                        llvm::Value* bires = DatumGetBINumericCodeGen(&builder, result);

                        /* extract the header of result to check if it is BINumeric */
                        Vals4[0] = int64_0;
                        Vals4[1] = int32_1;
                        Vals4[2] = int32_0;
                        Vals4[3] = int32_0;
                        tmpval = builder.CreateInBoundsGEP(bires, Vals4);
                        tmpval = builder.CreateLoad(int16Type, tmpval, "biheader");
                        llvm::Value* rflag = builder.CreateAnd(tmpval, val_mask);

                        /* extract the header of hashcell to check if it is BINumeric */
                        llvm::Value* real_cellval = builder.CreateLoad(int64Type, cellval, "cell_val");
                        llvm::Value* cellarg = builder.CreateIntToPtr(real_cellval, numericPtrType);
                        tmpval = builder.CreateInBoundsGEP(cellarg, Vals4);
                        tmpval = builder.CreateLoad(int16Type, tmpval, "cellheader");
                        llvm::Value* lflag = builder.CreateAnd(tmpval, val_mask);

                        /* check if either of them is not BI64 */
                        llvm::Value* oparg1 = builder.CreateICmpEQ(lflag, val_binum64);
                        llvm::Value* oparg2 = builder.CreateICmpEQ(rflag, val_binum64);
                        llvm::Value* bothbi64 = builder.CreateAnd(oparg1, oparg2);

                        /* use fast path only when both args are bi64 */
                        builder.CreateCondBr(bothbi64, var_bisum_bb, var_numsum_bb);

                        builder.SetInsertPoint(var_bisum_bb);
                        /* extract the actual data of numeric only when cell is not null */
                        Vals4[0] = int64_0;
                        Vals4[1] = int32_1;
                        Vals4[2] = int32_0;
                        Vals4[3] = int32_1;
                        tmpval = builder.CreateInBoundsGEP(bires, Vals4);
                        tmpval = builder.CreateBitCast(tmpval, int64PtrType);
                        llvm::Value* resval = builder.CreateLoad(int64Type, tmpval, "value");

                        /* locate the restore value in hash cell by position */
                        llvm::Value* cell_addr = builder.CreateAdd(real_cellval, int64_6);
                        cell_addr = builder.CreateIntToPtr(cell_addr, int64PtrType);
                        llvm::Value* mid_cell_val = builder.CreateLoad(int64Type, cell_addr);

                        /* check overflow */
                        llvm::Type* Intrinsic_Tys[] = {int64Type};
                        llvm::Function* func_sadd_overflow =
                            llvm::Intrinsic::getDeclaration(mod, llvm_sadd_with_overflow, Intrinsic_Tys);
                        if (func_sadd_overflow == NULL) {
                            ereport(ERROR,
                                (errcode(ERRCODE_LOAD_INTRINSIC_FUNCTION_FAILED),
                                    errmodule(MOD_LLVM),
                                    errmsg("Cannot get the llvm::Intrinsic::sadd_with_overflow function!\n")));
                        }
                        llvm::Value* aggres = builder.CreateCall(func_sadd_overflow, {resval, mid_cell_val});
                        llvm::Value* oflag = builder.CreateExtractValue(aggres, 1);
                        builder.CreateCondBr(oflag, var_numsum_bb, normal_bb);

                        /* if meet overflow during aggregation, turn to original sum function. */
                        builder.SetInsertPoint(var_numsum_bb);
                        llvm::Function* func_vnumericsum = llvmCodeGen->module()->getFunction("Jitted_numericsum");
                        if (NULL == func_vnumericsum) {
                            func_vnumericsum = numeric_sum_codegen(aggref);
                        }
                        builder.CreateCall(func_vnumericsum, {cell, hcxt, aggIdxList[i], result});
                        builder.CreateBr(agg_end);

                        builder.SetInsertPoint(normal_bb);
                        llvm::Value* sumval = builder.CreateExtractValue(aggres, 0);
                        builder.CreateStore(sumval, cell_addr);
                        builder.CreateBr(agg_end);

                        builder.SetInsertPoint(agg_end);
                    } else if (fast_aggref) {
                        /*
                         * If aggref can be evaluated in fast path and be numeric
                         * expressions, use the result from fastexpr.
                         */
                        Assert(bisum_bb != NULL);
                        Assert(numsum_bb != NULL);
                        DEFINE_BLOCK(agg_end, jitted_batchagg);
                        DEFINE_BLOCK(agg_then, jitted_batchagg);
                        DEFINE_BLOCK(agg_else, jitted_batchagg);
                        DEFINE_BLOCK(normal_bb, jitted_batchagg);
                        DEFINE_BLOCK(expr_bisum_bb, jitted_batchagg);
                        DEFINE_BLOCK(bioverflow_bb, jitted_batchagg);

                        /*
                         * if the result of expression is outofbound, turn to
                         * original numeric path.
                         */
                        builder.CreateBr(numsum_bb);

                        /*
                         * if the result of expression is BI64, extract it.
                         */
                        builder.SetInsertPoint(bisum_bb);
                        llvm::Value* resval = builder.CreateExtractValue(expres, 1);

                        /* get the hash val : hashCell->m_val[aggidx].val */
                        Vals4[0] = int64_0;
                        Vals4[1] = int32_pos_hcell_mval;
                        Vals4[2] = aggIdxList[i];
                        Vals4[3] = int32_0;
                        llvm::Value* cellval = builder.CreateInBoundsGEP(cell, Vals4);

                        /* get the flag of the hash cell and check if it is NULL */
                        Vals4[3] = int32_1;
                        llvm::Value* cellflag = builder.CreateInBoundsGEP(cell, Vals4);
                        tmpval = builder.CreateLoad(int8Type, cellflag, "cellFlag");
                        tmpval = builder.CreateAnd(tmpval, int8_1);
                        tmpval = builder.CreateICmpEQ(tmpval, int8_0);
                        builder.CreateCondBr(tmpval, agg_else, agg_then);

                        /* cell be null, add Variable */
                        builder.SetInsertPoint(agg_then);
                        /* get the aligned scale of this expression */
                        exprscale = GetAlignedScale(estate->expr);
                        llvm::Value* alignedscale = llvmCodeGen->getIntConstant(CHAROID, exprscale);
                        /*
                         * should make a new context to record the result : the
                         * following code corresponding to:
                         * 'leftarg = DatumGetBINumeric(pVal[i]);
                         * cell->m_val[idx].val = addVariable(context, NumericGetDatum(leftarg));'.
                         */
                        tmpval = WrapmakeNumeric64CodeGen(&builder, resval, alignedscale);
                        tmpval = DatumGetBINumericCodeGen(&builder, tmpval);
                        tmpval = builder.CreatePtrToInt(tmpval, int64Type);
                        tmpval = WrapaddVariableCodeGen(&builder, hcxt, tmpval);

                        builder.CreateStore(tmpval, cellval);
                        builder.CreateStore(int8_0, cellflag);

                        /* turn to next basicblock or end this */
                        if (i == numaggs - 1)
                            builder.CreateBr(for_inc);
                        else
                            builder.CreateBr(agg_bb[i + 1]);

                        /* cell not be null, do aggregation */
                        builder.SetInsertPoint(agg_else);
                        llvm::Value* real_cellval = builder.CreateLoad(int64Type, cellval, "cell_val");

                        /* first make sure the value in cell is BI64 format */
                        Vals4[0] = int64_0;
                        Vals4[1] = int32_1;
                        Vals4[2] = int32_0;
                        Vals4[3] = int32_0;
                        llvm::Value* cellarg = builder.CreateIntToPtr(real_cellval, numericPtrType);
                        tmpval = builder.CreateInBoundsGEP(cellarg, Vals4);
                        tmpval = builder.CreateLoad(int16Type, tmpval, "cellheader");
                        llvm::Value* biflag = builder.CreateAnd(tmpval, val_mask);
                        llvm::Value* isbi64 = builder.CreateICmpEQ(biflag, val_binum64);
                        builder.CreateCondBr(isbi64, expr_bisum_bb, bioverflow_bb);

                        /*
                         * do aggregation directly only when both expr value
                         * and cell value is bi64.
                         */
                        builder.SetInsertPoint(expr_bisum_bb);
                        llvm::Value* cell_ptr = builder.CreateAdd(real_cellval, int64_6);
                        cell_ptr = builder.CreateIntToPtr(cell_ptr, int64PtrType);
                        llvm::Value* mid_cell_val = builder.CreateLoad(int64Type, cell_ptr);

                        /* check overflow */
                        llvm::Type* Intrinsic_Tys[] = {int64Type};
                        llvm::Function* func_sadd_overflow =
                            llvm::Intrinsic::getDeclaration(mod, llvm_sadd_with_overflow, Intrinsic_Tys);
                        if (func_sadd_overflow == NULL) {
                            ereport(ERROR,
                                (errcode(ERRCODE_LOAD_INTRINSIC_FUNCTION_FAILED),
                                    errmodule(MOD_LLVM),
                                    errmsg("Cannot get the llvm::Intrinsic::sadd_with_overflow function!\n")));
                        }
                        llvm::Value* aggres = builder.CreateCall(func_sadd_overflow, {resval, mid_cell_val});
                        llvm::Value* oflag = builder.CreateExtractValue(aggres, 1);
                        builder.CreateCondBr(oflag, bioverflow_bb, normal_bb);

                        builder.SetInsertPoint(bioverflow_bb);
                        exprscale = GetAlignedScale(estate->expr);
                        llvm::Value* ascale = llvmCodeGen->getIntConstant(CHAROID, exprscale);
                        llvm::Value* bioverres = WrapmakeNumeric64CodeGen(&builder, resval, ascale);
                        builder.CreateBr(numsum_bb);

                        builder.SetInsertPoint(numsum_bb);
                        llvm::PHINode* numres = builder.CreatePHI(int64Type, 2);
                        numres->addIncoming(result, flag_then[i]);
                        numres->addIncoming(bioverres, bioverflow_bb);
                        llvm::Value* evalval = (llvm::Value*)numres;

                        llvm::Function* func_vnumericsum = llvmCodeGen->module()->getFunction("Jitted_numericsum");
                        if (NULL == func_vnumericsum) {
                            func_vnumericsum = numeric_sum_codegen(aggref);
                        }
                        builder.CreateCall(func_vnumericsum, {cell, hcxt, aggIdxList[i], evalval});
                        builder.CreateBr(agg_end);

                        /* if there is no overflow, extract result directly */
                        builder.SetInsertPoint(normal_bb);
                        llvm::Value* sumval = builder.CreateExtractValue(aggres, 0);
                        builder.CreateStore(sumval, cell_ptr);
                        builder.CreateBr(agg_end);

                        builder.SetInsertPoint(agg_end);
                    } else {
                        llvm::Function* func_vnumericsum = llvmCodeGen->module()->getFunction("Jitted_numericsum");
                        if (NULL == func_vnumericsum) {
                            func_vnumericsum = numeric_sum_codegen(aggref);
                        }
                        builder.CreateCall(func_vnumericsum, {cell, hcxt, aggIdxList[i], result});
                    }
                } break;
                case NUMERICAVGFUNCOID: {
                    /*
                     * If aggref can be evaluated in fast path and just be
                     * simple vars, use the result from batch.
                     */
                    if (fast_aggref && (numSimpleVars > 0)) {
                        DEFINE_BLOCK(agg_then, jitted_batchagg);
                        DEFINE_BLOCK(agg_else, jitted_batchagg);
                        DEFINE_BLOCK(agg_end, jitted_batchagg);
                        DEFINE_BLOCK(normal_bb, jitted_batchagg);
                        DEFINE_BLOCK(bisum_bblock, jitted_batchagg);
                        DEFINE_BLOCK(numsum_bblock, jitted_batchagg);

                        /* get the hash val : hashCell->m_val[aggidx].val */
                        Vals4[0] = int64_0;
                        Vals4[1] = int32_pos_hcell_mval;
                        Vals4[2] = aggIdxList[i];
                        Vals4[3] = int32_0;
                        llvm::Value* cellval = builder.CreateInBoundsGEP(cell, Vals4);

                        /* get the count of hash val : hashCell->m_val[aggidx + 1].val */
                        Vals4[2] = builder.CreateAdd(aggIdxList[i], int64_1, "val_plus");
                        llvm::Value* cellval2 = builder.CreateInBoundsGEP(cell, Vals4);

                        /* get the flag of the hash cell and check if it is NULL */
                        Vals4[2] = aggIdxList[i];
                        Vals4[3] = int32_1;
                        llvm::Value* cellflag = builder.CreateInBoundsGEP(cell, Vals4);

                        /* get the flag of cell->m_val[idx + 1].flag */
                        Vals4[2] = builder.CreateAdd(aggIdxList[i], int64_1, "flag_plus");
                        llvm::Value* cellflag2 = builder.CreateInBoundsGEP(cell, Vals4);

                        /* Now load the cell flag and check it */
                        tmpval = builder.CreateLoad(int8Type, cellflag, "cellFlag");
                        tmpval = builder.CreateAnd(tmpval, int8_1);
                        tmpval = builder.CreateICmpEQ(tmpval, int8_0);
                        builder.CreateCondBr(tmpval, agg_else, agg_then);

                        /* cell be null, add Variable */
                        builder.SetInsertPoint(agg_then);
                        /* do leftarg = DatumGetBINumeric(pVal[i]) */
                        tmpval = DatumGetBINumericCodeGen(&builder, result);
                        tmpval = builder.CreatePtrToInt(tmpval, int64Type);
                        /* corresponding to addVariable(context, NumericGetDatum(leftarg)) */
                        tmpval = WrapaddVariableCodeGen(&builder, hcxt, tmpval);
                        builder.CreateStore(tmpval, cellval);

                        /* count set to be one */
                        builder.CreateStore(int64_1, cellval2);

                        /* set cell flag */
                        builder.CreateStore(int8_0, cellflag);
                        builder.CreateStore(int8_0, cellflag2);

                        /* turn to next basicblock or end this */
                        if (i == numaggs - 1)
                            builder.CreateBr(for_inc);
                        else
                            builder.CreateBr(agg_bb[i + 1]);

                        /* cell be not null, do aggregation */
                        builder.SetInsertPoint(agg_else);
                        /*
                         * When fast_aggref is true and numSimpleVars is greater than zero,
                         * the expr is numeric type var. Convert this numeric type data to
                         * SiNumeric data to get the value.
                         */
                        llvm::Value* bires = DatumGetBINumericCodeGen(&builder, result);

                        /* extract the header of result to check if it is BINumeric */
                        Vals4[0] = int64_0;
                        Vals4[1] = int32_1;
                        Vals4[2] = int32_0;
                        Vals4[3] = int32_0;
                        tmpval = builder.CreateInBoundsGEP(bires, Vals4);
                        tmpval = builder.CreateLoad(int16Type, tmpval, "biheader");
                        llvm::Value* rflag = builder.CreateAnd(tmpval, val_mask);

                        /* extract the header of hashcell to check if it is BINumeric */
                        llvm::Value* real_cellval = builder.CreateLoad(int64Type, cellval, "cell_val");
                        llvm::Value* cellarg = builder.CreateIntToPtr(real_cellval, numericPtrType);
                        tmpval = builder.CreateInBoundsGEP(cellarg, Vals4);
                        tmpval = builder.CreateLoad(int16Type, tmpval, "cellheader");
                        llvm::Value* lflag = builder.CreateAnd(tmpval, val_mask);

                        /* check if either of them is not BI64 */
                        llvm::Value* oparg1 = builder.CreateICmpEQ(lflag, val_binum64);
                        llvm::Value* oparg2 = builder.CreateICmpEQ(rflag, val_binum64);
                        llvm::Value* bothbi64 = builder.CreateAnd(oparg1, oparg2);

                        /* use fast path only when both args are bi64 */
                        builder.CreateCondBr(bothbi64, bisum_bblock, numsum_bblock);

                        builder.SetInsertPoint(bisum_bblock);
                        /* extract the actual data of numeric only when value is not null */
                        Vals4[0] = int64_0;
                        Vals4[1] = int32_1;
                        Vals4[2] = int32_0;
                        Vals4[3] = int32_1;
                        tmpval = builder.CreateInBoundsGEP(bires, Vals4);
                        tmpval = builder.CreateBitCast(tmpval, int64PtrType);
                        llvm::Value* resval = builder.CreateLoad(int64Type, tmpval, "value");

                        llvm::Value* cell_addr = builder.CreateAdd(real_cellval, int64_6);
                        cell_addr = builder.CreateIntToPtr(cell_addr, int64PtrType);
                        llvm::Value* mid_cell_val = builder.CreateLoad(int64Type, cell_addr);

                        /* check overflow */
                        llvm::Type* Intrinsic_Tys[] = {int64Type};
                        llvm::Function* func_sadd_overflow =
                            llvm::Intrinsic::getDeclaration(mod, llvm_sadd_with_overflow, Intrinsic_Tys);
                        if (func_sadd_overflow == NULL) {
                            ereport(ERROR,
                                (errcode(ERRCODE_LOAD_INTRINSIC_FUNCTION_FAILED),
                                    errmodule(MOD_LLVM),
                                    errmsg("Cannot get the llvm::Intrinsic::sadd_with_overflow function!\n")));
                        }
                        llvm::Value* aggres = builder.CreateCall(func_sadd_overflow, {resval, mid_cell_val});
                        llvm::Value* oflag = builder.CreateExtractValue(aggres, 1);
                        builder.CreateCondBr(oflag, numsum_bblock, normal_bb);

                        builder.SetInsertPoint(numsum_bblock);
                        llvm::Function* func_vnumericavg = llvmCodeGen->module()->getFunction("Jitted_numericavg");
                        if (NULL == func_vnumericavg) {
                            func_vnumericavg = numeric_avg_codegen(aggref);
                        }
                        builder.CreateCall(func_vnumericavg, {cell, hcxt, aggIdxList[i], result});
                        builder.CreateBr(agg_end);

                        builder.SetInsertPoint(normal_bb);
                        llvm::Value* sumval = builder.CreateExtractValue(aggres, 0);
                        builder.CreateStore(sumval, cell_addr);

                        /* cell->m_val[idx+1].val++ */
                        tmpval = builder.CreateLoad(int64Type, cellval2, "count");
                        tmpval = builder.CreateAdd(tmpval, int64_1);
                        builder.CreateStore(tmpval, cellval2);
                        builder.CreateBr(agg_end);

                        builder.SetInsertPoint(agg_end);
                    } else if (fast_aggref) {
                        /*
                         * If aggref can be evaluated in fast path and be numeric
                         * expressions, use the result from fastexpr.
                         */
                        Assert(bisum_bb != NULL);
                        Assert(numsum_bb != NULL);
                        DEFINE_BLOCK(agg_end, jitted_batchagg);
                        DEFINE_BLOCK(agg_then, jitted_batchagg);
                        DEFINE_BLOCK(agg_else, jitted_batchagg);
                        DEFINE_BLOCK(normal_bb, jitted_batchagg);
                        DEFINE_BLOCK(expr_bisum_bb, jitted_batchagg);
                        DEFINE_BLOCK(bioverflow_bb, jitted_batchagg);

                        builder.CreateBr(numsum_bb);

                        builder.SetInsertPoint(bisum_bb);
                        llvm::Value* resval = builder.CreateExtractValue(expres, 1);

                        /* get the hash val : hashCell->m_val[aggidx].val */
                        Vals4[0] = int64_0;
                        Vals4[1] = int32_pos_hcell_mval;
                        Vals4[2] = aggIdxList[i];
                        Vals4[3] = int32_0;
                        llvm::Value* cellval = builder.CreateInBoundsGEP(cell, Vals4);

                        /* get the count of hash val : hashCell->m_val[aggidx + 1].val */
                        Vals4[2] = builder.CreateAdd(aggIdxList[i], int64_1, "val_plus");
                        llvm::Value* cellval2 = builder.CreateInBoundsGEP(cell, Vals4);
                        ;

                        /* get the flag of the hash cell and check if it is NULL */
                        Vals4[2] = aggIdxList[i];
                        Vals4[3] = int32_1;
                        llvm::Value* cellflag = builder.CreateInBoundsGEP(cell, Vals4);

                        /* get the flag of cell->m_val[idx + 1].flag */
                        Vals4[2] = builder.CreateAdd(aggIdxList[i], int64_1, "flag_plus");
                        llvm::Value* cellflag2 = builder.CreateInBoundsGEP(cell, Vals4);

                        tmpval = builder.CreateLoad(int8Type, cellflag, "cellFlag");
                        tmpval = builder.CreateAnd(tmpval, int8_1);
                        tmpval = builder.CreateICmpEQ(tmpval, int8_0);
                        builder.CreateCondBr(tmpval, agg_else, agg_then);

                        /* cell be null, add Variable */
                        builder.SetInsertPoint(agg_then);
                        exprscale = GetAlignedScale(estate->expr);
                        llvm::Value* alignedscale = llvmCodeGen->getIntConstant(CHAROID, exprscale);
                        /*
                         * should make a new context to record the result : the
                         * following code corresponding to:
                         * 'leftarg = DatumGetBINumeric(pVal[i]);
                         * cell->m_val[idx].val = addVariable(context, NumericGetDatum(leftarg));'.
                         */
                        tmpval = WrapmakeNumeric64CodeGen(&builder, resval, alignedscale);
                        tmpval = DatumGetBINumericCodeGen(&builder, tmpval);
                        tmpval = builder.CreatePtrToInt(tmpval, int64Type);
                        tmpval = WrapaddVariableCodeGen(&builder, hcxt, tmpval);
                        builder.CreateStore(tmpval, cellval);
                        /* count set to be one */
                        builder.CreateStore(int64_1, cellval2);
                        /* set the flag of hashcell */
                        builder.CreateStore(int8_0, cellflag);
                        builder.CreateStore(int8_0, cellflag2);

                        /* turn to next basicblock */
                        if (i == numaggs - 1)
                            builder.CreateBr(for_inc);
                        else
                            builder.CreateBr(agg_bb[i + 1]);

                        builder.SetInsertPoint(agg_else);
                        llvm::Value* real_cellval = builder.CreateLoad(int64Type, cellval, "cell_val");

                        /* first make sure the value in cell is BI64 format */
                        Vals4[0] = int64_0;
                        Vals4[1] = int32_1;
                        Vals4[2] = int32_0;
                        Vals4[3] = int32_0;
                        llvm::Value* cellarg = builder.CreateIntToPtr(real_cellval, numericPtrType);
                        tmpval = builder.CreateInBoundsGEP(cellarg, Vals4);
                        tmpval = builder.CreateLoad(int16Type, tmpval, "cellheader");
                        llvm::Value* biflag = builder.CreateAnd(tmpval, val_mask);
                        llvm::Value* isbi64 = builder.CreateICmpEQ(biflag, val_binum64);
                        builder.CreateCondBr(isbi64, expr_bisum_bb, bioverflow_bb);

                        /*
                         * do aggregation directly only when both expr value
                         * and cell value is bi64.
                         */
                        builder.SetInsertPoint(expr_bisum_bb);
                        llvm::Value* cell_ptr = builder.CreateAdd(real_cellval, int64_6);
                        cell_ptr = builder.CreateIntToPtr(cell_ptr, int64PtrType);
                        llvm::Value* mid_cell_val = builder.CreateLoad(int64Type, cell_ptr);

                        /* check overflow */
                        llvm::Type* Intrinsic_Tys[] = {int64Type};
                        llvm::Function* func_sadd_overflow =
                            llvm::Intrinsic::getDeclaration(mod, llvm_sadd_with_overflow, Intrinsic_Tys);
                        if (func_sadd_overflow == NULL) {
                            ereport(ERROR,
                                (errcode(ERRCODE_LOAD_INTRINSIC_FUNCTION_FAILED),
                                    errmodule(MOD_LLVM),
                                    errmsg("Cannot get the llvm::Intrinsic::sadd_with_overflow function!\n")));
                        }
                        llvm::Value* aggres = builder.CreateCall(func_sadd_overflow, {resval, mid_cell_val});
                        llvm::Value* oflag = builder.CreateExtractValue(aggres, 1);
                        builder.CreateCondBr(oflag, bioverflow_bb, normal_bb);

                        /* make numeric64 when meet overflow */
                        builder.SetInsertPoint(bioverflow_bb);
                        exprscale = GetAlignedScale(estate->expr);
                        llvm::Value* ascale = llvmCodeGen->getIntConstant(CHAROID, exprscale);
                        llvm::Value* bioverres = WrapmakeNumeric64CodeGen(&builder, resval, ascale);
                        builder.CreateBr(numsum_bb);

                        builder.SetInsertPoint(numsum_bb);
                        llvm::PHINode* numres = builder.CreatePHI(int64Type, 2);
                        numres->addIncoming(result, flag_then[i]);
                        numres->addIncoming(bioverres, bioverflow_bb);
                        llvm::Value* evalval = (llvm::Value*)numres;

                        llvm::Function* func_vnumericavg = llvmCodeGen->module()->getFunction("Jitted_numericavg");
                        if (NULL == func_vnumericavg) {
                            func_vnumericavg = numeric_avg_codegen(aggref);
                        }
                        builder.CreateCall(func_vnumericavg, {cell, hcxt, aggIdxList[i], evalval});
                        builder.CreateBr(agg_end);

                        /* if there is no overflow, extract result directly */
                        builder.SetInsertPoint(normal_bb);
                        llvm::Value* sumval = builder.CreateExtractValue(aggres, 0);
                        builder.CreateStore(sumval, cell_ptr);

                        /* cell->m_val[idx+1].val++ */
                        tmpval = builder.CreateLoad(int64Type, cellval2, "count");
                        tmpval = builder.CreateAdd(tmpval, int64_1);
                        builder.CreateStore(tmpval, cellval2);
                        builder.CreateBr(agg_end);

                        builder.SetInsertPoint(agg_end);
                    } else {
                        llvm::Function* func_vnumericavg = llvmCodeGen->module()->getFunction("Jitted_numericavg");
                        if (NULL == func_vnumericavg) {
                            func_vnumericavg = numeric_avg_codegen(aggref);
                        }
                        builder.CreateCall(func_vnumericavg, {cell, hcxt, aggIdxList[i], result});
                    }
                } break;
                default:
                    ereport(ERROR,
                        (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmodule(MOD_LLVM),
                            errmsg("Unsupported agg function %u!", aggref->aggfnoid)));
                    break;
            }

            if (i == numaggs - 1)
                builder.CreateBr(for_inc);
            else
                builder.CreateBr(agg_bb[i + 1]);

            /* if the current flag is null, turn to next agg */
            builder.SetInsertPoint(flag_else[i]);
            if (i == numaggs - 1)
                builder.CreateBr(for_inc);
            else
                builder.CreateBr(agg_bb[i + 1]);
        }
    }

    /* codegen in the for_inc basic block: compare the loop index with nrows */
    builder.SetInsertPoint(for_inc);
    tmpval = builder.CreateTrunc(idx_next, int32Type);
    tmpval = builder.CreateICmpEQ(tmpval, nValues);
    builder.CreateCondBr(tmpval, for_end, for_body);

    /* codegen in for_end basic block: just return void */
    builder.SetInsertPoint(for_end);
    (void)VecExprCodeGen::MemCxtSwitToCodeGen(&builder, agg_oldcontext);
    (void)WrapResetEContextCodeGen(&builder, mecontext);
    for (i = 0; i < numaggs; i++) {
        if (econtext[i])
            (void)WrapResetEContextCodeGen(&builder, econtext[i]);
    }
    builder.CreateRetVoid();

    pfree_ext(aggIdxList);
    pfree_ext(agg_bb);
    pfree_ext(flag_then);
    pfree_ext(flag_else);
    pfree_ext(econtext);
    pfree_ext(batch_vals);
    pfree_ext(batch_flag);

    llvmCodeGen->FinalizeFunction(jitted_batchagg, node->ss.ps.plan->plan_node_id);

    return jitted_batchagg;
}

llvm::Function* VecHashAggCodeGen::HashBatchCodeGen(VecAggState* node, int idx, bool rehash)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    /* Find and load the IR file from the installaion directory */
    llvmCodeGen->loadIRFile();

    /* Extract plan information from node */
    VecAgg* vecagg = (VecAgg*)(node->ss.ps.plan);
    List* tlist = (outerPlan(vecagg))->targetlist;
    AttrNumber* keyIdx = vecagg->grpColIdx;

    AttrNumber key = keyIdx[idx] - 1;
    TargetEntry* tentry = (TargetEntry*)list_nth(tlist, key);
    int bpchar_len = 0;

    Assert(IsA(tentry->expr, Var) || IsA(tentry->expr, FuncExpr));

    /* Hash batch value just according to the return type. */
    Oid rettype = InvalidOid;
    switch (nodeTag(tentry->expr)) {
        case T_Var: {
            Var* var = (Var*)(tentry->expr);
            rettype = var->vartype;

            if (var->vartype == BPCHAROID)
                bpchar_len = var->vartypmod - VARHDRSZ;
        } break;
        case T_FuncExpr: {
            FuncExpr* funcexpr = (FuncExpr*)(tentry->expr);
            rettype = funcexpr->funcresulttype;
        } break;
        default:
            Assert(0);
            break;
    }

    /* Get LLVM Context and builder */
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    /* Define data types and some llvm consts */
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
    DEFINE_CGVAR_INT32(int32_2, 2);
    DEFINE_CGVAR_INT32(int32_4, 4);
    DEFINE_CGVAR_INT64(Datum_0, 0);

    llvm::Function* jitted_hashbatch = NULL;
    llvm::Value* llvmargs[3];
    llvm::Value* hash_res1 = NULL;
    llvm::Value* hash_res2 = NULL;
    llvm::Value* lt0_hash = NULL;
    llvm::BasicBlock* EQ0_bb = NULL;

    /* Function definition and input parameters */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "JittedHashBatch", int32Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("value", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("flag", int8Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("hash_val", int32Type));
    jitted_hashbatch = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* pval = llvmargs[0];
    llvm::Value* flag = llvmargs[1];
    llvm::Value* hash_val = llvmargs[2];

    llvm::BasicBlock* entry = &jitted_hashbatch->getEntryBlock();
    DEFINE_BLOCK(be_not_null, jitted_hashbatch);
    DEFINE_BLOCK(be_null, jitted_hashbatch);
    DEFINE_BLOCK(end_null, jitted_hashbatch);

    builder.SetInsertPoint(entry);
    /* check the current value is null or not */
    flag = builder.CreateAnd(flag, int8_1);
    llvm::Value* cmp = builder.CreateICmpEQ(flag, int8_0);
    builder.CreateCondBr(cmp, be_not_null, be_null);

    /*
     * corresponding to likely(NOT_NULL(flag[j])) branch in hashColT function.
     */
    builder.SetInsertPoint(be_not_null);
    llvm::Module* mod = llvmCodeGen->module();
    switch (rettype) {
        case INT4OID: {
            hash_res1 = hash_val;
            pval = builder.CreateTrunc(pval, int32Type);
            llvm_crc32_32_32(hash_res1, hash_res1, pval);
        } break;
        case INT8OID:
        case DATEOID:
        case TIMESTAMPOID: {
            hash_res1 = hash_val;
            llvm_crc32_32_64(hash_res1, hash_res1, pval);
        } break;
        case BPCHAROID: {
            int len = bpchar_len;
            llvm::Function* func_evalvar = llvmCodeGen->module()->getFunction("JittedEvalVarlena");
            if (func_evalvar == NULL) {
                func_evalvar = VarlenaCvtCodeGen();
            }
            llvm::Value* res = builder.CreateCall(func_evalvar, pval, "func_evalvar");
            llvm::Value* data1 = builder.CreateExtractValue(res, 1);
            data1 = builder.CreatePtrToInt(data1, int64Type);
            data1 = builder.CreateIntToPtr(data1, int8PtrType);

            hash_res1 = hash_val;
            if (len >= 8) {
                int k = 0;
                llvm::Value* big_data = builder.CreateBitCast(data1, int64PtrType);
                llvm::Value* kidx = NULL;
                while (len >= 8) {
                    kidx = llvmCodeGen->getIntConstant(INT4OID, k);
                    pval = builder.CreateInBoundsGEP(big_data, kidx);
                    pval = builder.CreateLoad(int64Type, pval, "bigdat");
                    llvm_crc32_32_64(hash_res1, hash_res1, pval);
                    len = len - 8;
                    k++;
                }
                kidx = llvmCodeGen->getIntConstant(INT4OID, k * 8);
                data1 = builder.CreateInBoundsGEP(data1, kidx);
            }

            if (len >= 4) {
                llvm::Value* data = builder.CreateBitCast(data1, int32PtrType);
                pval = builder.CreateInBoundsGEP(data, Datum_0);
                pval = builder.CreateLoad(int32Type, pval, "intdat");
                llvm_crc32_32_32(hash_res1, hash_res1, pval);
                data1 = builder.CreateInBoundsGEP(data1, int32_4);
                len = len - 4;
            }

            if (len >= 2) {
                llvm::Value* short_data = builder.CreateBitCast(data1, int16PtrType);
                pval = builder.CreateInBoundsGEP(short_data, Datum_0);
                pval = builder.CreateLoad(int16Type, pval, "shortdat");
                llvm_crc32_32_16(hash_res1, hash_res1, pval);
                data1 = builder.CreateInBoundsGEP(data1, int32_2);
                len = len - 2;
            }

            if (len == 1) {
                pval = builder.CreateLoad(int8Type, data1, "val_char");
                llvm_crc32_32_8(hash_res1, hash_res1, pval);
            }
        } break;
        case VARCHAROID:
        case TEXTOID: {
            /*
             * Different from bpchar type, we should create hash table according
             * to the actual length of varchar or text.
             */
            llvm::Value* cmpval = NULL;
            llvm::Value* data = NULL;
            llvm::Value* nxt_len = NULL;
            llvm::Value* nxt_pos = NULL;
            llvm::Value* nxt_hash = NULL;
            DEFINE_CGVAR_INT32(int32_8, 8);
            DEFINE_CGVAR_INT32(int32_4, 4);
            DEFINE_CGVAR_INT32(int32_2, 2);
            DEFINE_CGVAR_INT32(int32_1, 1);

            DEFINE_BLOCK(GE8_bb, jitted_hashbatch);
            DEFINE_BLOCK(end_GE8_bb, jitted_hashbatch);
            DEFINE_BLOCK(LT8_bb, jitted_hashbatch);
            DEFINE_BLOCK(GE4_bb, jitted_hashbatch);
            DEFINE_BLOCK(LT4_bb, jitted_hashbatch);
            DEFINE_BLOCK(GE2_bb, jitted_hashbatch);
            DEFINE_BLOCK(LT2_bb, jitted_hashbatch);
            DEFINE_BLOCK(EQ1_bb, jitted_hashbatch);

            if (NULL == EQ0_bb) {
                EQ0_bb = llvm::BasicBlock::Create(context, "EQ0_bb", jitted_hashbatch);
            }

            /* get the initial data and true length */
            llvm::Function* func_evalvar = llvmCodeGen->module()->getFunction("JittedEvalVarlena");
            if (func_evalvar == NULL) {
                func_evalvar = VarlenaCvtCodeGen();
            }

            llvm::Value* res = builder.CreateCall(func_evalvar, pval, "func_evalvar");
            llvm::Value* vlen = builder.CreateExtractValue(res, 0);
            llvm::Value* vdata = builder.CreateExtractValue(res, 1);
            vdata = builder.CreatePtrToInt(vdata, int64Type);
            vdata = builder.CreateIntToPtr(vdata, int8PtrType);
            /* set initial hash value */
            hash_res1 = hash_val;
            llvm::Value* bighash = builder.CreateZExt(hash_res1, int64Type);
            llvm::Value* bigdata = builder.CreateBitCast(vdata, int64PtrType);

            /* check if the actual length is great than 9 */
            cmpval = builder.CreateICmpSGE(vlen, int32_8, "if_ge8");
            builder.CreateCondBr(cmpval, GE8_bb, LT8_bb);

            /* loop over the length until it is less than 8 */
            builder.SetInsertPoint(GE8_bb);
            llvm::PHINode* phi_whl_len = builder.CreatePHI(int32Type, 2);
            llvm::PHINode* phi_whl_pos = builder.CreatePHI(int32Type, 2);
            llvm::PHINode* phi_whl_hash = builder.CreatePHI(int64Type, 2);

            llvm::Value* whl_len = (llvm::Value*)phi_whl_len;
            nxt_len = builder.CreateSub(whl_len, int32_8);
            phi_whl_len->addIncoming(vlen, be_not_null);
            phi_whl_len->addIncoming(nxt_len, GE8_bb);

            llvm::Value* whl_pos = (llvm::Value*)phi_whl_pos;
            nxt_pos = builder.CreateAdd(whl_pos, int32_1);
            phi_whl_pos->addIncoming(int32_0, be_not_null);
            phi_whl_pos->addIncoming(nxt_pos, GE8_bb);

            llvm::Value* whl_hash = (llvm::Value*)phi_whl_hash;
            whl_hash = builder.CreateTrunc(whl_hash, int32Type);
            /* compute hash value */
            llvm::Value* whl_data = builder.CreateInBoundsGEP(bigdata, whl_pos);
            whl_data = builder.CreateLoad(int64Type, whl_data, "whl_data");
            llvm_crc32_32_64(nxt_hash, whl_hash, whl_data);
            phi_whl_hash->addIncoming(bighash, be_not_null);
            nxt_hash = builder.CreateZExt(nxt_hash, int64Type);
            phi_whl_hash->addIncoming(nxt_hash, GE8_bb);

            /* increament pos and minimus the length */
            cmpval = builder.CreateICmpSGE(nxt_len, int32_8);
            builder.CreateCondBr(cmpval, GE8_bb, end_GE8_bb);

            builder.SetInsertPoint(end_GE8_bb);
            llvm::Value* ge8_len = nxt_len;
            llvm::Value* ge8_hash = builder.CreateTrunc(nxt_hash, int32Type);
            llvm::Value* ge8_data = builder.CreateInBoundsGEP(bigdata, nxt_pos);
            ge8_data = builder.CreateBitCast(ge8_data, int8PtrType);
            builder.CreateBr(LT8_bb);

            builder.SetInsertPoint(LT8_bb);
            llvm::PHINode* phi_lt8_len = builder.CreatePHI(int32Type, 2);
            phi_lt8_len->addIncoming(vlen, be_not_null);
            phi_lt8_len->addIncoming(ge8_len, end_GE8_bb);
            llvm::Value* lt8_len = (llvm::Value*)phi_lt8_len;

            llvm::PHINode* phi_lt8_hash = builder.CreatePHI(int32Type, 2);
            phi_lt8_hash->addIncoming(hash_res1, be_not_null);
            phi_lt8_hash->addIncoming(ge8_hash, end_GE8_bb);
            llvm::Value* lt8_hash = (llvm::Value*)phi_lt8_hash;

            llvm::PHINode* phi_lt8_data = builder.CreatePHI(int8PtrType, 2);
            phi_lt8_data->addIncoming(vdata, be_not_null);
            phi_lt8_data->addIncoming(ge8_data, end_GE8_bb);
            llvm::Value* lt8_data = (llvm::Value*)phi_lt8_data;

            cmpval = builder.CreateICmpSGE(lt8_len, int32_4);
            builder.CreateCondBr(cmpval, GE4_bb, LT4_bb);

            /* if the actual length is greater than 4 and less than 8 */
            builder.SetInsertPoint(GE4_bb);
            data = builder.CreateBitCast(lt8_data, int32PtrType);
            data = builder.CreateInBoundsGEP(data, Datum_0);
            data = builder.CreateLoad(int32Type, data, "intdat");
            llvm::Value* ge4_hash = NULL;
            llvm_crc32_32_32(ge4_hash, lt8_hash, data);
            llvm::Value* ge4_data = builder.CreateInBoundsGEP(lt8_data, int32_4);
            llvm::Value* ge4_len = builder.CreateSub(lt8_len, int32_4);
            builder.CreateBr(LT4_bb);

            /* if the actual length is less than 4 */
            builder.SetInsertPoint(LT4_bb);
            llvm::PHINode* phi_lt4_len = builder.CreatePHI(int32Type, 2);
            phi_lt4_len->addIncoming(lt8_len, LT8_bb);
            phi_lt4_len->addIncoming(ge4_len, GE4_bb);
            llvm::Value* lt4_len = (llvm::Value*)phi_lt4_len;

            llvm::PHINode* phi_lt4_hash = builder.CreatePHI(int32Type, 2);
            phi_lt4_hash->addIncoming(lt8_hash, LT8_bb);
            phi_lt4_hash->addIncoming(ge4_hash, GE4_bb);
            llvm::Value* lt4_hash = (llvm::Value*)phi_lt4_hash;

            llvm::PHINode* phi_lt4_data = builder.CreatePHI(int8PtrType, 2);
            phi_lt4_data->addIncoming(lt8_data, LT8_bb);
            phi_lt4_data->addIncoming(ge4_data, GE4_bb);
            llvm::Value* lt4_data = (llvm::Value*)phi_lt4_data;

            cmpval = builder.CreateICmpSGE(lt4_len, int32_2);
            builder.CreateCondBr(cmpval, GE2_bb, LT2_bb);

            /* if the length is greater than 2 and less than 4 */
            builder.SetInsertPoint(GE2_bb);
            data = builder.CreateBitCast(lt4_data, int16PtrType);
            data = builder.CreateInBoundsGEP(data, Datum_0);
            data = builder.CreateLoad(int16Type, data, "shortdat");
            llvm::Value* ge2_hash = NULL;
            llvm_crc32_32_16(ge2_hash, lt4_hash, data);
            llvm::Value* ge2_data = builder.CreateInBoundsGEP(lt4_data, int32_2);
            llvm::Value* ge2_len = builder.CreateSub(lt4_len, int32_2);
            builder.CreateBr(LT2_bb);

            /* if the length is less than 2 */
            builder.SetInsertPoint(LT2_bb);
            llvm::PHINode* phi_lt2_len = builder.CreatePHI(int32Type, 2);
            phi_lt2_len->addIncoming(lt4_len, LT4_bb);
            phi_lt2_len->addIncoming(ge2_len, GE2_bb);
            llvm::Value* lt2_len = (llvm::Value*)phi_lt2_len;

            llvm::PHINode* phi_lt2_hash = builder.CreatePHI(int32Type, 2);
            phi_lt2_hash->addIncoming(lt4_hash, LT4_bb);
            phi_lt2_hash->addIncoming(ge2_hash, GE2_bb);
            llvm::Value* lt2_hash = (llvm::Value*)phi_lt2_hash;

            llvm::PHINode* phi_lt2_data = builder.CreatePHI(int8PtrType, 2);
            phi_lt2_data->addIncoming(lt4_data, LT4_bb);
            phi_lt2_data->addIncoming(ge2_data, GE2_bb);
            llvm::Value* lt2_data = (llvm::Value*)phi_lt2_data;

            cmpval = builder.CreateICmpEQ(lt2_len, int32_1);
            builder.CreateCondBr(cmpval, EQ1_bb, EQ0_bb);

            builder.SetInsertPoint(EQ1_bb);
            data = builder.CreateLoad(int8Type, lt2_data, "val_char");
            llvm::Value* lt1_hash = NULL;
            llvm_crc32_32_8(lt1_hash, lt2_hash, data);
            builder.CreateBr(EQ0_bb);

            builder.SetInsertPoint(EQ0_bb);
            llvm::PHINode* phi_lt0_hash = builder.CreatePHI(int32Type, 2);
            phi_lt0_hash->addIncoming(lt2_hash, LT2_bb);
            phi_lt0_hash->addIncoming(lt1_hash, EQ1_bb);
            lt0_hash = (llvm::Value*)phi_lt0_hash;
        } break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmodule(MOD_LLVM),
                    errmsg("Type %u is not supported yet in hashBatch", rettype)));
            break;
    }
    builder.CreateBr(end_null);

    /*
     * corresponding to the else branch:
     * if (!rehash) { hashRes[j] = 0 }
     */
    builder.SetInsertPoint(be_null);
    if (!rehash)
        hash_res2 = int32_0;
    else
        hash_res2 = hash_val;
    builder.CreateBr(end_null);

    builder.SetInsertPoint(end_null);
    if (rettype != VARCHAROID && rettype != TEXTOID) {
        llvm::PHINode* Phi_hash = builder.CreatePHI(int32Type, 2);
        Phi_hash->addIncoming(hash_res1, be_not_null);
        Phi_hash->addIncoming(hash_res2, be_null);
        builder.CreateRet(Phi_hash);
    } else {
        llvm::PHINode* Phi_hash = builder.CreatePHI(int32Type, 2);
        Phi_hash->addIncoming(lt0_hash, EQ0_bb);
        Phi_hash->addIncoming(hash_res2, be_null);
        builder.CreateRet(Phi_hash);
    }

    llvmCodeGen->FinalizeFunction(jitted_hashbatch, node->ss.ps.plan->plan_node_id);

    return jitted_hashbatch;
}

llvm::Function* VecHashAggCodeGen::MatchOneKeyCodeGen(VecAggState* node, int idx)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    /* Find and load the IR file from the installaion directory */
    llvmCodeGen->loadIRFile();

    /* Extract plan information from node */
    VecAgg* vecagg = (VecAgg*)(node->ss.ps.plan);
    List* tlist = (outerPlan(vecagg))->targetlist;
    AttrNumber* keyIdx = vecagg->grpColIdx;

    AttrNumber key = keyIdx[idx] - 1;
    TargetEntry* tentry = (TargetEntry*)list_nth(tlist, key);

    Assert(IsA(tentry->expr, Var) || IsA(tentry->expr, FuncExpr));
    Oid rettype = InvalidOid;
    int bpchar_len = 0;
    switch (nodeTag(tentry->expr)) {
        case T_Var: {
            Var* var = (Var*)(tentry->expr);
            rettype = var->vartype;

            if (var->vartype == BPCHAROID)
                bpchar_len = var->vartypmod - VARHDRSZ;
        } break;
        case T_FuncExpr: {
            FuncExpr* funcexpr = (FuncExpr*)(tentry->expr);
            rettype = funcexpr->funcresulttype;
        } break;
        default:
            Assert(0);
            break;
    }
    Var* var = (Var*)(tentry->expr);

    /* Get LLVM Context and builder */
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    /* Define data types and some llvm consts */
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(hashCellPtrType, "struct.hashCell");

    DEFINE_CGVAR_INT8(int8_0, 0);
    DEFINE_CGVAR_INT8(int8_1, 1);
    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT64(Datum_0, 0);
    DEFINE_CGVAR_INT64(Datum_1, 1);
    DEFINE_CGVAR_INT32(int32_pos_hcell_mval, pos_hcell_mval);

    llvm::Function* jitted_matchonekey = NULL;
    llvm::Value* tmpval = NULL;
    llvm::Value* llvmargs[4];
    llvm::Value* Vals4[4] = {Datum_0, int32_0, int32_0, int32_0};

    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "JittedOneMatchKey", int64Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("value", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("flag", int8Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("hashcell", hashCellPtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("keycell_idx", int32Type));
    jitted_matchonekey = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* pval = llvmargs[0];
    llvm::Value* pflg = llvmargs[1];
    llvm::Value* hashcell = llvmargs[2];
    llvm::Value* keyidxincell = llvmargs[3];

    DEFINE_BLOCK(bnot_null, jitted_matchonekey);
    DEFINE_BLOCK(may_null, jitted_matchonekey);
    DEFINE_BLOCK(both_null, jitted_matchonekey);
    DEFINE_BLOCK(one_null, jitted_matchonekey);
    DEFINE_BLOCK(check_end, jitted_matchonekey);

    keyidxincell = builder.CreateSExt(keyidxincell, int64Type);
    /* hashCell.m_val[keyidxincell].val / flag */
    Vals4[0] = Datum_0;
    Vals4[1] = int32_pos_hcell_mval;
    Vals4[2] = keyidxincell;
    Vals4[3] = int32_0;
    tmpval = builder.CreateInBoundsGEP(hashcell, Vals4);
    llvm::Value* keyval = builder.CreateLoad(int64Type, tmpval, "keyincell");

    Vals4[3] = int32_1;
    tmpval = builder.CreateInBoundsGEP(hashcell, Vals4);
    llvm::Value* keyflg = builder.CreateLoad(int8Type, tmpval, "flagincell");

    llvm::Value* cmp1 = NULL;
    llvm::Value* cmp2 = NULL;
    llvm::Value* cmpand = NULL;
    llvm::Value* cmpor = NULL;
    llvm::Value* res1 = NULL;
    llvm::Value* res2 = NULL;
    llvm::Value* res3 = NULL;

    pflg = builder.CreateAnd(pflg, int8_1);
    cmp1 = builder.CreateICmpEQ(pflg, int8_0);
    keyflg = builder.CreateAnd(keyflg, int8_1);
    cmp2 = builder.CreateICmpEQ(keyflg, int8_0);
    cmpand = builder.CreateAnd(cmp1, cmp2);
    cmpor = builder.CreateOr(cmp1, cmp2);

    builder.CreateCondBr(cmpand, bnot_null, may_null);
    builder.SetInsertPoint(bnot_null);
    switch (rettype) {
        case INT4OID: {
            /*
             * should first truncate the keyval to make sure we compare the
             * right value
             */
            keyval = builder.CreateTrunc(keyval, int32Type);
            pval = builder.CreateTrunc(pval, int32Type);
            res1 = builder.CreateICmpEQ(keyval, pval);
            res1 = builder.CreateZExt(res1, int64Type);
        } break;
        case INT8OID:
        case DATEOID:
        case TIMESTAMPOID: {
            res1 = builder.CreateICmpEQ(keyval, pval);
            res1 = builder.CreateZExt(res1, int64Type);
        } break;
        case BPCHAROID: {
            /* first extract the char* value from Datum */
            llvm::Function* func_evalvar = llvmCodeGen->module()->getFunction("JittedEvalVarlena");
            if (func_evalvar == NULL) {
                func_evalvar = dorado::VarlenaCvtCodeGen();
            }

            llvm::Value* vecval = builder.CreateCall(func_evalvar, pval, "evalbatchvar");
            llvm::Value* vecdata = builder.CreateExtractValue(vecval, 1);

            llvm::Value* cellval = builder.CreateCall(func_evalvar, keyval, "evalcellvar");
            llvm::Value* celldata = builder.CreateExtractValue(cellval, 1);

            /* call simple memcmp IR function */
            llvm::Function* evalmemcmp = llvmCodeGen->module()->getFunction("LLVMIRmemcmp");
            if (evalmemcmp == NULL) {
                ereport(ERROR,
                    (errcode(ERRCODE_LOAD_IR_FUNCTION_FAILED),
                        errmodule(MOD_LLVM),
                        errmsg("Failed on getting IR function : LLVMIRmemcmp!\n")));
            }

            llvm::Value* lendat = llvmCodeGen->getIntConstant(INT4OID, bpchar_len);
            res1 = builder.CreateCall(evalmemcmp, {celldata, vecdata, lendat}, "memcmp");
        } break;
        case TEXTOID:
        case VARCHAROID: {
            llvm::Function* func_evalvar = llvmCodeGen->module()->getFunction("JittedEvalVarlena");
            if (func_evalvar == NULL) {
                func_evalvar = VarlenaCvtCodeGen();
            }

            llvm::Value* vecval = builder.CreateCall(func_evalvar, pval, "evalbatchvar");
            llvm::Value* veclen = builder.CreateExtractValue(vecval, 0);
            llvm::Value* vecdata = builder.CreateExtractValue(vecval, 1);

            llvm::Value* cellval = builder.CreateCall(func_evalvar, keyval, "evalcellvar");
            llvm::Value* celllen = builder.CreateExtractValue(cellval, 0);
            llvm::Value* celldata = builder.CreateExtractValue(cellval, 1);

            llvm::Function* func_texteq_cc = llvmCodeGen->module()->getFunction("LLVMIRtexteq");
            if (func_texteq_cc == NULL) {
                ereport(ERROR,
                    (errcode(ERRCODE_LOAD_IR_FUNCTION_FAILED),
                        errmodule(MOD_LLVM),
                        errmsg("Failed on getting IR function : LLVMIRtexteq!\n")));
            }
            res1 = builder.CreateCall(func_texteq_cc, {veclen, vecdata, celllen, celldata}, "texteq");
        } break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmodule(MOD_LLVM),
                    (errmsg("Type %u is not supported yet in match_key", var->vartype))));
            break;
    }
    builder.CreateBr(check_end);

    builder.SetInsertPoint(may_null);
    builder.CreateCondBr(cmpor, one_null, both_null);
    /* both null is equal */
    builder.SetInsertPoint(both_null);
    res2 = Datum_1;
    builder.CreateBr(check_end);

    /* null not equal to non-null */
    builder.SetInsertPoint(one_null);
    res3 = Datum_0;
    builder.CreateBr(check_end);

    builder.SetInsertPoint(check_end);
    llvm::PHINode* Phi_ret = builder.CreatePHI(int64Type, 3);
    Phi_ret->addIncoming(res1, bnot_null);
    Phi_ret->addIncoming(res2, both_null);
    Phi_ret->addIncoming(res3, one_null);
    builder.CreateRet(Phi_ret);

    llvmCodeGen->FinalizeFunction(jitted_matchonekey, node->ss.ps.plan->plan_node_id);

    return jitted_matchonekey;
}

llvm::Value* VecHashAggCodeGen::EvalFastExprInBatchAgg(ExprState* state, GsCodeGen::LlvmBuilder& builder,
    llvm::Function* jitted_func, llvm::BasicBlock** bb_null, llvm::BasicBlock** bb_last,
    llvm::BasicBlock** bb_outofbound, llvm::Value* econtext, llvm::Value* argVector, llvm::Value* phi_idx)
{
    dorado::GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::Module* mod = llvmCodeGen->module();
    llvm::LLVMContext& context = llvmCodeGen->context();

    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int16Type, INT2OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(int64PtrType, INT8OID);

    DEFINE_CGVAR_INT8(int8_0, 0);
    DEFINE_CGVAR_INT8(int8_1, 1);
    DEFINE_CGVAR_INT16(int16_0, 0);
    DEFINE_CGVAR_INT16(val_numeric64, NUMERIC_64);
    DEFINE_CGVAR_INT16(val_bimask, NUMERIC_BI_MASK);
    DEFINE_CGVAR_INT16(val_scalemask, NUMERIC_BI_SCALEMASK);
    DEFINE_CGVAR_INT32(int32_pos_scalvec_vals, pos_scalvec_vals);
    DEFINE_CGVAR_INT32(int32_pos_scalvec_flag, pos_scalvec_flag);
    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT64(int64_0, 0);

    llvm::Value* tmpval = NULL;
    llvm::Value* cmpval = NULL;
    llvm::Value* phi_val = NULL;
    llvm::Value* result = NULL;
    llvm::Value* multi_bound = NULL;
    llvm::Value* left_scaled1 = NULL;
    llvm::Value* left_scaled2 = NULL;
    llvm::Value* right_scaled1 = NULL;
    llvm::Value* right_scaled2 = NULL;
    llvm::Value* resscale1 = NULL;
    llvm::Value* resscale2 = NULL;
    llvm::Value* res1 = NULL;
    llvm::Value* lval = NULL;
    llvm::Value* rval = NULL;
    llvm::PHINode* left_scaled = NULL;
    llvm::PHINode* right_scaled = NULL;
    llvm::PHINode* resscale = NULL;

    llvm::Type* Intrinsic_Tys[] = {int64Type};
    llvm::Value* Vals[2] = {int64_0, int32_0};
    llvm::Value* Vals4[4] = {int64_0, int32_0, int32_0, int32_0};

    /* create LLVM value with {uint16, int64} format */
    llvm::Type* Elements[] = {int16Type, int64Type};
    llvm::Type* SiNumeric64Type = llvm::StructType::create(context, Elements, "SiNumeric64");

    if (*bb_last)
        builder.SetInsertPoint(*bb_last);

    switch (nodeTag(state->expr)) {
        case T_TargetEntry: {
            /* TargetEntry information is stored in GenericExprState */
            GenericExprState* gstate = (GenericExprState*)state;
            ExprState* estate = gstate->arg;

            result = EvalFastExprInBatchAgg(
                estate, builder, jitted_func, bb_null, bb_last, bb_outofbound, econtext, argVector, phi_idx);
        } break;
        case T_OpExpr: {
            /* define the basic block needed here */
            DEFINE_BLOCK(both_bi64, jitted_func);
            DEFINE_BLOCK(not_both_bi64, jitted_func);

            /* check if have special opexpr with one arg */
            OpExpr* opexpr = (OpExpr*)state->expr;
            FuncExprState* fstate = (FuncExprState*)state;
            List* op_args = fstate->args;

            ExprState* lestate = (ExprState*)linitial(op_args);
            ExprState* restate = (ExprState*)lsecond(op_args);

            lval = EvalFastExprInBatchAgg(
                lestate, builder, jitted_func, bb_null, bb_last, bb_outofbound, econtext, argVector, phi_idx);
            rval = EvalFastExprInBatchAgg(
                restate, builder, jitted_func, bb_null, bb_last, bb_outofbound, econtext, argVector, phi_idx);

            if (*bb_last)
                builder.SetInsertPoint(*bb_last);

            /* Extract the header value to get the mask and scale */
            llvm::Value* lheader = builder.CreateExtractValue(lval, 0);
            llvm::Value* rheader = builder.CreateExtractValue(rval, 0);

            llvm::Value* lvalscale = builder.CreateAnd(lheader, val_scalemask);
            llvm::Value* rvalscale = builder.CreateAnd(rheader, val_scalemask);

            llvm::Value* lvalmask = builder.CreateAnd(lheader, val_bimask);
            llvm::Value* lvalbi64 = builder.CreateICmpEQ(lvalmask, val_numeric64);
            llvm::Value* rvalmask = builder.CreateAnd(rheader, val_bimask);
            llvm::Value* rvalbi64 = builder.CreateICmpEQ(rvalmask, val_numeric64);

            llvm::Value* bebi64 = builder.CreateAnd(lvalbi64, rvalbi64);
            builder.CreateCondBr(bebi64, both_bi64, not_both_bi64);

            builder.SetInsertPoint(both_bi64);
            llvm::Value* leftval = builder.CreateExtractValue(lval, 1);
            llvm::Value* rightval = builder.CreateExtractValue(rval, 1);

            /*
             * adjust the scale and value, first define outofbound block
             */
            if (opexpr->opno == NUMERICADDOID || opexpr->opno == NUMERICSUBOID) {
                /* Define basic block that needed only by NUMERICADDOID and NUMERICSUBOID*/
                DEFINE_BLOCK(delta_large, jitted_func);
                DEFINE_BLOCK(delta_small, jitted_func);
                DEFINE_BLOCK(adjust_true, jitted_func);
                DEFINE_BLOCK(adjust_overflow_bb, jitted_func);
                DEFINE_BLOCK(adjust_normal_bb, jitted_func);

                llvm::Value* delta_scale = builder.CreateSub(lvalscale, rvalscale, "delta_scale");
                /* check delta_scale >= 0 or not */
                cmpval = builder.CreateICmpSGE(delta_scale, int16_0, "cmp_delta_scale");
                builder.CreateCondBr(cmpval, delta_large, delta_small);

                /* corresponding to delta_scale >= 0 */
                builder.SetInsertPoint(delta_large);
                /* tmpval = y < 0 ? y : -y */
                tmpval = builder.CreateSub(int64_0, rightval);
                cmpval = builder.CreateICmpSLT(rightval, int64_0, "negative_cmp");
                phi_val = builder.CreateSelect(cmpval, rightval, tmpval);

                left_scaled1 = leftval;
                llvm::Value* mulscale = ScaleMultiCodeGen(&builder, delta_scale);
                /* corresponding to y_scaled = y * ScaleMultipler[delta_scale] */
                right_scaled1 = builder.CreateMul(rightval, mulscale);

                /* result_scale = x_scale */
                resscale1 = lvalscale;
                multi_bound = GetInt64MulOutofBoundCodeGen(&builder, delta_scale);
                cmpval = builder.CreateICmpSGE(phi_val, multi_bound, "bound_check");
                builder.CreateCondBr(cmpval, adjust_true, *bb_outofbound);

                builder.SetInsertPoint(delta_small);
                /* tmpval = x < 0 ? x : -x */
                tmpval = builder.CreateSub(int64_0, leftval);
                cmpval = builder.CreateICmpSLT(leftval, int64_0, "negative_cmp");
                phi_val = builder.CreateSelect(cmpval, leftval, tmpval);

                /* corresponding to x_scaled = x * ScaleMultipler[-delta_scale] */
                llvm::Value* mdelta_scale = builder.CreateSub(int16_0, delta_scale);
                llvm::Value* mmulscale = ScaleMultiCodeGen(&builder, mdelta_scale);
                left_scaled2 = builder.CreateMul(leftval, mmulscale);
                right_scaled2 = rightval;

                /* result_scale = y_scale */
                resscale2 = rvalscale;
                multi_bound = GetInt64MulOutofBoundCodeGen(&builder, mdelta_scale);
                cmpval = builder.CreateICmpSGE(phi_val, multi_bound, "bound_check");
                builder.CreateCondBr(cmpval, adjust_true, *bb_outofbound);

                builder.SetInsertPoint(adjust_true);
                left_scaled = builder.CreatePHI(int64Type, 2);
                right_scaled = builder.CreatePHI(int64Type, 2);
                resscale = builder.CreatePHI(int16Type, 2);

                left_scaled->addIncoming(left_scaled1, delta_large);
                left_scaled->addIncoming(left_scaled2, delta_small);

                right_scaled->addIncoming(right_scaled1, delta_large);
                right_scaled->addIncoming(right_scaled2, delta_small);

                resscale->addIncoming(resscale1, delta_large);
                resscale->addIncoming(resscale2, delta_small);

                switch (opexpr->opno) {
                    case NUMERICADDOID: {
                        /* do bi64 add bi64, need to check if there is any overflow */
                        llvm::Function* func_sadd_overflow =
                            llvm::Intrinsic::getDeclaration(mod, llvm_sadd_with_overflow, Intrinsic_Tys);
                        if (func_sadd_overflow == NULL) {
                            ereport(ERROR,
                                (errcode(ERRCODE_LOAD_INTRINSIC_FUNCTION_FAILED),
                                    errmodule(MOD_LLVM),
                                    errmsg("Cannot get the llvm::Intrinsic::sadd_with_overflow function!\n")));
                        }
                        llvm::Value* res = builder.CreateCall(func_sadd_overflow, {left_scaled, right_scaled}, "sadd");
                        llvm::Value* overflow_flag = builder.CreateExtractValue(res, 1);

                        builder.CreateCondBr(overflow_flag, adjust_overflow_bb, adjust_normal_bb);

                        builder.SetInsertPoint(adjust_normal_bb);
                        res1 = builder.CreateExtractValue(res, 0);

                        /* create the result of current step */
                        result = llvm::UndefValue::get(SiNumeric64Type);
                        llvm::Value* tmphead = builder.CreateAdd(val_numeric64, resscale);
                        result = builder.CreateInsertValue(result, tmphead, 0);
                        result = builder.CreateInsertValue(result, res1, 1);
                        *bb_last = adjust_normal_bb;

                        /* adjust true without out_of_bound */
                        builder.SetInsertPoint(adjust_overflow_bb);
                        builder.CreateBr(*bb_outofbound);
                    } break;
                    case NUMERICSUBOID: {
                        /* do bi64 add bi64, need to check if there is any overflow */
                        llvm::Function* func_ssub_overflow =
                            llvm::Intrinsic::getDeclaration(mod, llvm_ssub_with_overflow, Intrinsic_Tys);
                        if (func_ssub_overflow == NULL) {
                            ereport(ERROR,
                                (errcode(ERRCODE_LOAD_INTRINSIC_FUNCTION_FAILED),
                                    errmodule(MOD_LLVM),
                                    errmsg("Cannot get the llvm::Intrinsic::sasub_with_overflow function!\n")));
                        }
                        llvm::Value* res = builder.CreateCall(func_ssub_overflow, {left_scaled, right_scaled}, "sadd");
                        llvm::Value* overflow_flag = builder.CreateExtractValue(res, 1);
                        builder.CreateCondBr(overflow_flag, adjust_overflow_bb, adjust_normal_bb);

                        builder.SetInsertPoint(adjust_normal_bb);
                        /* if not out of bound, return makeNumeric64(result, resScale) */
                        res1 = builder.CreateExtractValue(res, 0);

                        /* create the result of current step */
                        result = llvm::UndefValue::get(SiNumeric64Type);
                        llvm::Value* tmphead = builder.CreateAdd(val_numeric64, resscale);
                        result = builder.CreateInsertValue(result, tmphead, 0);
                        result = builder.CreateInsertValue(result, res1, 1);
                        *bb_last = adjust_normal_bb;

                        /* if overflow, turn to int128 type first */
                        builder.SetInsertPoint(adjust_overflow_bb);
                        builder.CreateBr(*bb_outofbound);
                    } break;
                    default:
                        Assert(0);
                        break;
                }
            } else if (opexpr->opno == NUMERICMULOID) {
                DEFINE_BLOCK(mul64, jitted_func);
                DEFINE_BLOCK(mul128, jitted_func);
                DEFINE_CGVAR_INT16(maxInt64digitsNum, MAXINT64DIGIT);

                llvm::Value* res_scale = builder.CreateAdd(lvalscale, rvalscale, "res_scale");
                /* check delta_scale >= MAX DIGITS INT64 NUMBER or not */
                cmpval = builder.CreateICmpSLE(res_scale, maxInt64digitsNum, "cmp_delta_scale");

                /* do bi64 mul bi64, need to check if there is any overflow */
                llvm::Function* func_smul_overflow =
                    llvm::Intrinsic::getDeclaration(mod, llvm_smul_with_overflow, Intrinsic_Tys);
                if (func_smul_overflow == NULL) {
                    ereport(ERROR,
                        (errcode(ERRCODE_LOAD_INTRINSIC_FUNCTION_FAILED),
                            errmodule(MOD_LLVM),
                            errmsg("Cannot get the llvm::Intrinsic::smul_with_overflow function!")));
                }
                llvm::Value* res = builder.CreateCall(func_smul_overflow, {leftval, rightval}, "smul");
                llvm::Value* overflow_flag = builder.CreateExtractValue(res, 1);
                llvm::Value* res_64 = builder.CreateExtractValue(res, 0);
                cmpval = builder.CreateAnd(cmpval, builder.CreateNot(overflow_flag));

                builder.CreateCondBr(cmpval, mul64, mul128);

                builder.SetInsertPoint(mul64);
                res1 = res_64;
                /* create the result of current step */
                result = llvm::UndefValue::get(SiNumeric64Type);
                llvm::Value* tmphead = builder.CreateAdd(val_numeric64, res_scale);
                result = builder.CreateInsertValue(result, tmphead, 0);
                result = builder.CreateInsertValue(result, res_64, 1);
                *bb_last = mul64;

                builder.SetInsertPoint(mul128);
                builder.CreateBr(*bb_outofbound);
            } else {
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmodule(MOD_LLVM),
                        errmsg("Unexpected operation %u!", opexpr->opno)));
            }

            /*
             * when one of the argument is bi128 : note that at most
             * one of the argument is bi128.
             */
            builder.SetInsertPoint(not_both_bi64);
            builder.CreateBr(*bb_outofbound);
        } break;
        case T_Var: {
            /* Extract the variable from the batch */
            Var* var = (Var*)(state->expr);
            llvm::Value* m_attno = llvmCodeGen->getIntConstant(INT8OID, var->varattno - 1);

            Vals[0] = m_attno;
            Vals[1] = int32_pos_scalvec_vals;
            tmpval = builder.CreateInBoundsGEP(argVector, Vals);
            tmpval = builder.CreateLoad(int64PtrType, tmpval, "m_vals");
            tmpval = builder.CreateInBoundsGEP(tmpval, phi_idx);
            llvm::Value* res = builder.CreateLoad(int64Type, tmpval, "val");

            Vals[1] = int32_pos_scalvec_flag;
            tmpval = builder.CreateInBoundsGEP(argVector, Vals);
            tmpval = builder.CreateLoad(int8PtrType, tmpval, "m_flag");
            tmpval = builder.CreateInBoundsGEP(tmpval, phi_idx);
            llvm::Value* flg = builder.CreateLoad(int8Type, tmpval, "flag");

            /* make sure the value is null or not */
            llvm::Value* tmpNull = builder.CreateAnd(flg, int8_1);
            tmpval = builder.CreateICmpEQ(tmpNull, int8_0);
            DEFINE_BLOCK(var_bb, jitted_func);
            builder.CreateCondBr(tmpval, var_bb, *bb_null);

            builder.SetInsertPoint(var_bb);
            Assert(NUMERICOID == var->vartype);
            /* Since var is numeric type, turn numeric data to BInumeric */
            result = llvm::UndefValue::get(SiNumeric64Type);
            llvm::Value* bires = DatumGetBINumericCodeGen(&builder, res);
            /* Extract the header data */
            Vals4[0] = int64_0;
            Vals4[1] = int32_1;
            Vals4[2] = int32_0;
            Vals4[3] = int32_0;
            tmpval = builder.CreateInBoundsGEP(bires, Vals4);
            tmpval = builder.CreateLoad(int16Type, tmpval, "header");
            result = builder.CreateInsertValue(result, tmpval, 0);

            Vals4[3] = int32_1;
            tmpval = builder.CreateInBoundsGEP(bires, Vals4);
            tmpval = builder.CreateBitCast(tmpval, int64PtrType);
            tmpval = builder.CreateLoad(int64Type, tmpval, "value");
            result = builder.CreateInsertValue(result, tmpval, 1);
            *bb_last = var_bb;

            return result;
        } break;
        case T_Const: {
            Const* cst = (Const*)(state->expr);
            ScalarValue val = ScalarVector::DatumToScalar(cst->constvalue, cst->consttype, cst->constisnull);

            Assert(NUMERICOID == cst->consttype);
            /* The const value will not be NULL */
            uint16 header;
            Datum numval;
            /* turn to {uint16, int64} format(Simple Numeric Type) */
            /* First store mark bits and scale of big integer:
             * first 4 bits to distinguish bi64 and bi128, next
             * 4 bits are not used, the last 8 bits store the scale of bit integer
             */
            Numeric arg = DatumGetBINumeric(val);
            header = arg->choice.n_header;
            numval = *(int64*)(arg->choice.n_bi.n_data);

            result = llvm::UndefValue::get(SiNumeric64Type);
            llvm::Value* hed = llvmCodeGen->getIntConstant(INT2OID, header);
            llvm::Value* res = llvmCodeGen->getIntConstant(INT8OID, numval);
            result = builder.CreateInsertValue(result, hed, 0);
            result = builder.CreateInsertValue(result, res, 1);
            return result;
        } break;
        default:
            Assert(0);
            break;
    }

    return result;
}

llvm::Value* VecHashAggCodeGen::EvalSimpleExprInBatchAgg(
    ExprState* state, GsCodeGen::LlvmBuilder& builder, llvm::Value* econtext, llvm::Value* phi_idx, llvm::Value* isNull)
{
    llvm::Value* result = NULL;
    switch (nodeTag(state->expr)) {
        case T_TargetEntry: {
            /* TargetEntry information is stored in GenericExprState */
            GenericExprState* gstate = (GenericExprState*)state;
            ExprState* estate = gstate->arg;
            result = EvalSimpleExprInBatchAgg(estate, builder, econtext, phi_idx, isNull);
        } break;
        case T_Var:
        case T_Const:
        case T_OpExpr: {
            /* define llvmargs */
            llvm::Value* llvmargs[3];
            llvmargs[0] = econtext;
            llvmargs[1] = isNull;
            llvmargs[2] = phi_idx;

            /*
             * Prepare the parameters that needed by OpCodeGen.
             */
            ExprCodeGenArgs args;
            args.exprstate = state;
            args.parent = NULL;
            args.builder = &builder;
            args.llvm_args = &llvmargs[0];
            result = dorado::VecExprCodeGen::CodeGen(&args);
        } break;
        default:
            Assert(0);
            break;
    }

    return result;
}

llvm::Function* VecHashAggCodeGen::SonicBatchAggregationCodeGen(VecAggState* node, bool use_prefetch)
{
    /* First get the basic information of VecAggState */
    int numaggs = node->numaggs;

    Assert(NULL != (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj);
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    if (!BatchAggJittable(node, true))
        return NULL;

    /* Find and load the IR file from the installaion directory */
    llvmCodeGen->loadIRFile();

    /* Get LLVM Context and builder */
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);
    llvm::Module* mod = llvmCodeGen->module();

    int i;
    int exprscale = 0;
    bool fast_aggref = false;
    ExprState* estate = NULL;
    llvm::Value* nValues = NULL;
    llvm::Value* tmpval = NULL;
    llvm::Value* idx_next = NULL;
    llvm::Value* locidx = NULL;
    llvm::Value* result = NULL;
    llvm::Value* expres = NULL;
    llvm::Value* nbit = NULL;
    llvm::Value* arrIdx = NULL;
    llvm::Value* atomIdx = NULL;
    llvm::Value* atom = NULL;
    llvm::Value* cntatom = NULL;
    llvm::Value* atomsize = NULL;
    llvm::Value* data = NULL;
    llvm::Value* cntdata = NULL;
    llvm::Value* scount = NULL;
    llvm::Value* llvmargs[3];
    Aggref* aggref = NULL;
    llvm::Function* jitted_sonicbatchagg = NULL;

    /* Define data types and some llvm consts */
    DEFINE_CG_VOIDTYPE(voidType);
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int16Type, INT2OID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(int16PtrType, INT2OID);
    DEFINE_CG_PTRTYPE(int64PtrType, INT8OID);
    DEFINE_CG_PTRTYPE(exprCxtPtrType, "struct.ExprContext");
    DEFINE_CG_PTRTYPE(vecBatchPtrType, "class.VectorBatch");
    DEFINE_CG_PTRTYPE(scalarVecPtrType, "class.ScalarVector");
    DEFINE_CG_PTRTYPE(sonicEncodingDatumArrayPtrType, "class.SonicEncodingDatumArray");
    DEFINE_CG_PTRTYPE(numericPtrType, "struct.NumericData");
    DEFINE_CG_PTRTYPE(sonicHashAggPtrType, "class.SonicHashAgg");
    DEFINE_CG_PTRTYPE(atomPtrType, "struct.atom");
    DEFINE_CG_PTRPTRTYPE(atomPtrPtrType, "struct.atom");
    DEFINE_CG_PTRTYPE(memCxtDataPtrType, "struct.MemoryContextData");
    DEFINE_CG_PTRTYPE(sonicDatumArrayPtrType, "class.SonicDatumArray");
    DEFINE_CG_PTRPTRTYPE(sonicDatumArrayPtrPtrType, "class.SonicDatumArray");

    /* create LLVM value with {uint16, int64} format type */
    llvm::Type* Elements[] = {int16Type, int64Type};
    llvm::Type* SiNumeric64Type = llvm::StructType::create(context, Elements, "SiNumeric64");

    DEFINE_CGVAR_INT8(int8_0, 0);
    DEFINE_CGVAR_INT8(int8_1, 1);
    DEFINE_CGVAR_INT16(val_mask, NUMERIC_BI_MASK);
    DEFINE_CGVAR_INT16(val_binum64, NUMERIC_64);
    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT64(int64_0, 0);
    DEFINE_CGVAR_INT64(int64_1, 1);
    DEFINE_CGVAR_INT64(int64_6, 6);
    DEFINE_CGVAR_INT32(int32_pos_batch_marr, pos_batch_marr);
    DEFINE_CGVAR_INT32(int32_pos_scalvec_vals, pos_scalvec_vals);
    DEFINE_CGVAR_INT32(int32_pos_scalvec_flag, pos_scalvec_flag);
    DEFINE_CGVAR_INT32(int32_pos_ecxt_pertuple, pos_ecxt_pertuple);
    DEFINE_CGVAR_INT32(int32_pos_ecxt_outerbatch, pos_ecxt_outerbatch);
    DEFINE_CGVAR_INT32(int32_pos_shash_data, pos_shash_data);
    DEFINE_CGVAR_INT32(int32_pos_shash_sonichmemctl, pos_shash_sonichmemctl);
    DEFINE_CGVAR_INT32(int32_pos_shash_loc, pos_shash_loc);
    DEFINE_CGVAR_INT32(int32_pos_sonichmemctl_hcxt, pos_sonichmemctl_hcxt);
    DEFINE_CGVAR_INT32(int32_pos_shashagg_ecxt, pos_shashagg_ecxt);
    DEFINE_CGVAR_INT32(int32_pos_sdarray_nbit, pos_sdarray_nbit);
    DEFINE_CGVAR_INT32(int32_pos_sdarray_atomsize, pos_sdarray_atomsize);
    DEFINE_CGVAR_INT32(int32_pos_sdarray_arr, pos_sdarray_arr);
    DEFINE_CGVAR_INT32(int32_pos_atom_data, pos_atom_data);
    DEFINE_CGVAR_INT32(int32_pos_atom_nullflag, pos_atom_nullflag);

    /* llvm array values, used to represent the location of some element */
    llvm::Value* Vals[2] = {int64_0, int32_0};
    llvm::Value* Vals3[3] = {int64_0, int32_0, int32_0};
    llvm::Value* Vals4[4] = {int64_0, int32_0, int32_0, int32_0};

    llvm::Value** aggIdxList = (llvm::Value**)palloc(sizeof(llvm::Value*) * numaggs);
    llvm::Value** batch_vals = (llvm::Value**)palloc(sizeof(llvm::Value*) * numaggs);
    llvm::Value** batch_flag = (llvm::Value**)palloc(sizeof(llvm::Value*) * numaggs);
    llvm::Value** sdata = (llvm::Value**)palloc(sizeof(llvm::Value*) * numaggs);
    llvm::BasicBlock** agg_bb = (llvm::BasicBlock**)palloc(sizeof(llvm::BasicBlock*) * numaggs);
    llvm::BasicBlock** flag_then = (llvm::BasicBlock**)palloc(sizeof(llvm::BasicBlock*) * numaggs);
    llvm::BasicBlock** flag_else = (llvm::BasicBlock**)palloc(sizeof(llvm::BasicBlock*) * numaggs);

    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "JittedSonicFastBatchAgg", voidType);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("sonicHashAgg", sonicHashAggPtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("batch", vecBatchPtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("aggIdx", int16PtrType));
    jitted_sonicbatchagg = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* sonicHashAgg = llvmargs[0];
    llvm::Value* batch = llvmargs[1];
    llvm::Value* aggIdx = llvmargs[2];

    /* parameter used to mark if this tuple is NULL or not */
    llvm::Value* isNull = builder.CreateAlloca(int8Type);

    /* SonicHashAgg.SonicHash.SonicHashMemoryControl.hashContext */
    Vals4[2] = int32_pos_shash_sonichmemctl;
    Vals4[3] = int32_pos_sonichmemctl_hcxt;
    llvm::Value* hcxt = builder.CreateInBoundsGEP(sonicHashAgg, Vals4);
    hcxt = builder.CreateLoad(memCxtDataPtrType, hcxt, "hashContext");

    /* get the nrows of the batch */
    tmpval = builder.CreateInBoundsGEP(batch, Vals);
    nValues = builder.CreateLoad(int32Type, tmpval, "m_rows");

    /* get vectorBatch.m_arr of the batch */
    Vals[0] = int64_0;
    Vals[1] = int32_pos_batch_marr;
    llvm::Value* argVector = builder.CreateInBoundsGEP(batch, Vals);
    argVector = builder.CreateLoad(scalarVecPtrType, argVector, "m_arr");

    /* pre-load all the expression context : node->ss.ps.ps_ExprContext*/
    ExprContext* exprcontext = node->ss.ps.ps_ExprContext;
    llvm::Value* econtext = llvmCodeGen->CastPtrToLlvmPtr(exprCxtPtrType, exprcontext);

    /* get sonic data array structure : SonicHashAgg.SonicHash.SonicDatumArray** */
    Vals3[2] = int32_pos_shash_data;
    llvm::Value* sdataptr = builder.CreateInBoundsGEP(sonicHashAgg, Vals3);
    sdataptr = builder.CreateLoad(sonicDatumArrayPtrPtrType, sdataptr, "m_data");

    /* get sonic data array structure: SonicHashAgg.SonicHash::m_loc */
    Vals4[2] = int32_pos_shash_loc;
    Vals4[3] = int64_0;
    llvm::Value* loc = builder.CreateInBoundsGEP(sonicHashAgg, Vals4);

    /* define the basic block needed in the main process */
    llvm::BasicBlock* entry = &jitted_sonicbatchagg->getEntryBlock();
    DEFINE_BLOCK(for_body, jitted_sonicbatchagg);
    DEFINE_BLOCK(for_inc, jitted_sonicbatchagg);
    DEFINE_BLOCK(for_end, jitted_sonicbatchagg);

    /* get all addIdx of agg operators */
    for (i = 0; i < numaggs; i++) {
        llvm::Value* tmpidx = llvmCodeGen->getIntConstant(INT4OID, i);
        tmpval = builder.CreateInBoundsGEP(aggIdx, tmpidx);
        tmpval = builder.CreateLoad(int16Type, tmpval, "aggIdx");
        aggIdxList[i] = builder.CreateSExt(tmpval, int64Type);

        tmpval = builder.CreateInBoundsGEP(sdataptr, aggIdxList[i]);
        sdata[i] = builder.CreateLoad(sonicDatumArrayPtrType, tmpval, "sonicdata");

        agg_bb[i] = llvm::BasicBlock::Create(context, "agg_bb", jitted_sonicbatchagg);
        flag_then[i] = llvm::BasicBlock::Create(context, "flag_then", jitted_sonicbatchagg);
        flag_else[i] = llvm::BasicBlock::Create(context, "flag_else", jitted_sonicbatchagg);
    }

    /*
     * Start the main process for SonicHashAgg::batchaggregation, which has the following
     * pedudo code:
     * for (j = 0; j < nrows; j++){
     *	   for (i = 0; i < m_aggNum; i++){
     *		   peraggstate = peragg[numaggs - 1 - i];
     *		   pbatch = ExecVecProject (peraggstate->evalproj)
     *		   AggregationOnScalar(aggInfo[i], &pbatch->m_arr[0], aggidx[i], m_Loc)
     *	   }
     * }
     */
    builder.SetInsertPoint(entry);
    /*
     * First get the ecxt_per_tuple_memory, since we need to switch to this
     * memory context.
     */
    /* SonicHashAgg.m_econtext */
    Vals[1] = int32_pos_shashagg_ecxt;
    llvm::Value* mecontext = builder.CreateInBoundsGEP(sonicHashAgg, Vals);
    mecontext = builder.CreateLoad(exprCxtPtrType, mecontext, "m_econtext");
    Vals[1] = int32_pos_ecxt_pertuple;
    llvm::Value* agg_expr_context = builder.CreateInBoundsGEP(mecontext, Vals);
    agg_expr_context = builder.CreateLoad(memCxtDataPtrType, agg_expr_context, "agg_per_tuple_memory");
    llvm::Value* agg_oldcontext = VecExprCodeGen::MemCxtSwitToCodeGen(&builder, agg_expr_context);

    /*
     * Load value and flag from batch before the batch loop when we have
     * simple vars in transition level.
     */
    for (i = 0; i < numaggs; i++) {
        int numSimpleVars = 0;
        VecAggStatePerAgg peraggstate = &node->pervecagg[numaggs - i - 1];
        aggref = (Aggref*)(peraggstate->aggref);
        ProjectionInfo* projInfo = (ProjectionInfo*)(peraggstate->evalproj);

        if (aggref->aggstage == 0 && aggref->aggfnoid != COUNTOID) {
            numSimpleVars = projInfo->pi_numSimpleVars;
            if (numSimpleVars > 0) {
                int* varNumbers = projInfo->pi_varNumbers;
                int varNumber = varNumbers[0] - 1;
                /* m_arr[varNumber].m_vals */
                Vals[0] = llvmCodeGen->getIntConstant(INT8OID, varNumber);
                Vals[1] = int32_pos_scalvec_vals;
                tmpval = builder.CreateInBoundsGEP(argVector, Vals);
                tmpval = builder.CreateLoad(int64PtrType, tmpval, "m_vals");
                batch_vals[i] = tmpval;

                /* m_arr[varNumber].m_flag */
                Vals[1] = int32_pos_scalvec_flag;
                llvm::Value* argFlag = builder.CreateInBoundsGEP(argVector, Vals);
                argFlag = builder.CreateLoad(int8PtrType, argFlag, "m_flag");
                batch_flag[i] = argFlag;
            } else {
                batch_vals[i] = NULL;
                batch_flag[i] = NULL;
            }
        }
    }

    tmpval = builder.CreateICmpSGT(nValues, int32_0);
    builder.CreateCondBr(tmpval, for_body, for_end);

    builder.SetInsertPoint(for_body);
    llvm::PHINode* phi_idx = builder.CreatePHI(int64Type, 2);

    /* after each loop, index plus one */
    idx_next = builder.CreateAdd(phi_idx, int64_1);

    phi_idx->addIncoming(int64_0, entry);
    phi_idx->addIncoming(idx_next, for_inc);

    /* get location : loc[i] (see vsnumeric_sum and vsint8_sum) and check if it is zero */
    tmpval = builder.CreateInBoundsGEP(loc, phi_idx);
    locidx = builder.CreateLoad(int32Type, tmpval, "loc_i");
    tmpval = builder.CreateICmpEQ(locidx, int32_0);
    builder.CreateCondBr(tmpval, for_inc, agg_bb[0]);

    /* loop over the numaggs */
    int numSimpleVars = 0;
    for (i = 0; i < numaggs; i++) {
        llvm::BasicBlock* bisum_bb = NULL;
        llvm::BasicBlock* numsum_bb = NULL;

        /* the inverse order */
        VecAggStatePerAgg peraggstate = &node->pervecagg[numaggs - i - 1];
        aggref = (Aggref*)(peraggstate->aggref);
        ProjectionInfo* projInfo = (ProjectionInfo*)(peraggstate->evalproj);

        /* start the codegeneration for each aggregation */
        builder.SetInsertPoint(agg_bb[i]);

        if (aggref->aggfnoid == NUMERICAVGFUNCOID) {
            /* get scount = sdata[aggidx + 1] */
            llvm::Value* aggplus = builder.CreateAdd(aggIdxList[i], int64_1);
            tmpval = builder.CreateInBoundsGEP(sdataptr, aggplus);
            scount = builder.CreateLoad(sonicDatumArrayPtrType, tmpval, "scount");
        }

        if (aggref->aggstage == 0) {
            if (aggref->aggfnoid != COUNTOID) {
                Assert(peraggstate->evalproj != NULL);
                AggrefExprState* aggexprstate = peraggstate->aggrefstate;
                /* check if current expression can be codegened in fast path or not */
                estate = (ExprState*)linitial(aggexprstate->args);
                fast_aggref = AggRefFastJittable(estate);

                /*
                 * Do not consider collection and finalization level for
                 * numeric_avg to avoid deconstruct_array.
                 */
                if (aggref->aggfnoid == NUMERICAVGFUNCOID && aggref->aggstage > 0)
                    fast_aggref = false;

                /* If the current agg expression is just a simple var,
                 * load it from the batch directly
                 */
                if (projInfo == NULL) {
                    ereport(ERROR,
                        (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                            errmodule(MOD_LLVM),
                            errmsg("Unexpected NULL project information.")));
                }
                numSimpleVars = projInfo->pi_numSimpleVars;
                if (numSimpleVars > 0) {
                    /* m_arr[varNumber].m_vals */
                    tmpval = batch_vals[i];
                    tmpval = builder.CreateInBoundsGEP(tmpval, phi_idx);
                    result = builder.CreateLoad(int64Type, tmpval, "val");

                    /* m_arr[varNumber].m_flag */
                    tmpval = batch_flag[i];
                    tmpval = builder.CreateInBoundsGEP(tmpval, phi_idx);
                    tmpval = builder.CreateLoad(int8Type, tmpval, "flag");
                    builder.CreateStore(tmpval, isNull);
                } else {
                    /* set the batch information : econtext->ecxt_outerbatch = batch */
                    Vals[0] = int64_0;
                    Vals[1] = int32_pos_ecxt_outerbatch;
                    llvm::Value* tmp_outerbatch = builder.CreateInBoundsGEP(econtext, Vals);
                    builder.CreateStore(batch, tmp_outerbatch);

                    /*
                     * If fast_aggref is true, we could try to evaluate the
                     * expression value by using BI64 all the way, and turn
                     * to original path once meet outofbound.
                     */
                    if (fast_aggref) {
                        llvm::BasicBlock* bb_last = builder.GetInsertBlock();
                        DEFINE_BLOCK(bb_null, jitted_sonicbatchagg);
                        DEFINE_BLOCK(bb_outofbound, jitted_sonicbatchagg);

                        if (NULL == bisum_bb) {
                            bisum_bb = llvm::BasicBlock::Create(context, "bisum_bb", jitted_sonicbatchagg);
                        }

                        if (NULL == numsum_bb) {
                            numsum_bb = llvm::BasicBlock::Create(context, "numsum_bb", jitted_sonicbatchagg);
                        }

                        /* evaluate expression result */
                        llvm::Value* tmpexpres = EvalFastExprInBatchAgg(estate,
                            builder,
                            jitted_sonicbatchagg,
                            &bb_null,
                            &bb_last,
                            &bb_outofbound,
                            econtext,
                            argVector,
                            phi_idx);

                        /* expres is already in {int16, int64} format */
                        builder.SetInsertPoint(bb_last);
                        builder.CreateStore(int8_0, isNull);
                        llvm::Value* tmp_scale = builder.CreateExtractValue(tmpexpres, 0);
                        llvm::Value* tmp_value = builder.CreateExtractValue(tmpexpres, 1);
                        expres = llvm::UndefValue::get(SiNumeric64Type);
                        expres = builder.CreateInsertValue(expres, tmp_scale, 0);
                        expres = builder.CreateInsertValue(expres, tmp_value, 1);
                        builder.CreateBr(bisum_bb);

                        /* construct a null value, and no need to do aggregation */
                        builder.SetInsertPoint(bb_null);
                        builder.CreateStore(int8_1, isNull);
                        if (i == numaggs - 1)
                            builder.CreateBr(for_inc);
                        else
                            builder.CreateBr(agg_bb[i + 1]);

                        /* if result can not be represented in BI64, turn to
                         * the original path
                         */
                        builder.SetInsertPoint(bb_outofbound);
                        /* Turn to per_tuple_memory to evaluate expression. */
                        Vals[0] = int64_0;
                        Vals[1] = int32_pos_ecxt_pertuple;
                        llvm::Value* curr_Context = builder.CreateInBoundsGEP(econtext, Vals);
                        curr_Context = builder.CreateLoad(memCxtDataPtrType, curr_Context, "per_tuple_memory");
                        llvm::Value* cg_oldContext = VecExprCodeGen::MemCxtSwitToCodeGen(&builder, curr_Context);
                        result = EvalSimpleExprInBatchAgg(estate, builder, econtext, phi_idx, isNull);
                        /* return back to the old memory context */
                        (void)VecExprCodeGen::MemCxtSwitToCodeGen(&builder, cg_oldContext);
                    } else {
                        /*
                         * corresponding to ExecVecProject(peraggstate->evalproj) :
                         * to evaluate expressions, we should turn to per_tuple_memory.
                         */
                        Vals[0] = int64_0;
                        Vals[1] = int32_pos_ecxt_pertuple;
                        llvm::Value* curr_Context = builder.CreateInBoundsGEP(econtext, Vals);
                        curr_Context = builder.CreateLoad(memCxtDataPtrType, curr_Context, "per_tuple_memory");
                        llvm::Value* cg_oldContext = VecExprCodeGen::MemCxtSwitToCodeGen(&builder, curr_Context);
                        /* corresponding to ExecVecProject(peraggstate->evalproj) */
                        result = EvalSimpleExprInBatchAgg(estate, builder, econtext, phi_idx, isNull);
                        /* return back to the old memory context */
                        (void)VecExprCodeGen::MemCxtSwitToCodeGen(&builder, cg_oldContext);
                    }
                }
            } else {
                /*
                 * When current stage is transaction and aggfnoid is COUNTOID, no need to
                 * load any batch information. since we only need to plus one when cell
                 * is not null.
                 */
                result = int64_0;
                builder.CreateStore(int8_0, isNull);
            }
        } else {
            /*
             * When aggref->stage is not transiction, the aggref expr is always
             * be var, so get the value from batch directly (projInfo is not null).
             */
            if (projInfo != NULL) {
                int* varNumbers = projInfo->pi_varNumbers;
                int varNumber = varNumbers[0] - 1;
                /* m_arr[varNumber].m_vals */
                Vals[0] = llvmCodeGen->getIntConstant(INT8OID, varNumber);
                Vals[1] = int32_pos_scalvec_vals;
                tmpval = builder.CreateInBoundsGEP(argVector, Vals);
                tmpval = builder.CreateLoad(int64PtrType, tmpval, "m_vals");
                tmpval = builder.CreateInBoundsGEP(tmpval, phi_idx);
                result = builder.CreateLoad(int64Type, tmpval, "val");

                /* m_arr[varNumber].m_flag */
                Vals[1] = int32_pos_scalvec_flag;
                llvm::Value* argFlag = builder.CreateInBoundsGEP(argVector, Vals);
                argFlag = builder.CreateLoad(int8PtrType, argFlag, "m_flag");
                tmpval = builder.CreateInBoundsGEP(argFlag, phi_idx);
                tmpval = builder.CreateLoad(int8Type, tmpval, "flag");
                builder.CreateStore(tmpval, isNull);
            } else {
                ereport(ERROR,
                    (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmodule(MOD_LLVM),
                        errmsg("Unexpected NULL project information.")));
            }
        }

        /* Compute Aggregation */
        if (aggref->aggfnoid == COUNTOID) {
            flag_then[i]->eraseFromParent();
            flag_else[i]->eraseFromParent();

            char* Jittedname = NULL;
            if (aggref->aggstage == 0)
                Jittedname = "Jitted_scount_0";
            else
                Jittedname = "Jitted_scount_1";

            llvm::Function* func_vscount = llvmCodeGen->module()->getFunction(Jittedname);
            if (NULL == func_vscount) {
                func_vscount = vsonic_count_codegen(aggref);
            }
            builder.CreateCall(func_vscount, {sdata[i], locidx, result});

            if (i == numaggs - 1)
                builder.CreateBr(for_inc);
            else
                builder.CreateBr(agg_bb[i + 1]);
        } else {
            /*
             * now we already get sonic datum (data and locidx) and pVector(result), check
             * the flag and do aggregation.
             */
            /* see if IS_NULL(flag[phi_idx]) == false */
            llvm::Value* tmpnull = builder.CreateLoad(int8Type, isNull, "tmpnull");
            tmpnull = builder.CreateAnd(tmpnull, int8_1);
            llvm::Value* flag_cmp = builder.CreateICmpEQ(tmpnull, int8_0);
            builder.CreateCondBr(flag_cmp, flag_then[i], flag_else[i]);

            /* only do when not null */
            builder.SetInsertPoint(flag_then[i]);
            switch (aggref->aggfnoid) {
                case NUMERICSUMFUNCOID: {
                    /*
                     * If aggref can be evaluated in fast path and just be
                     * simple vars, use the result from batch.
                     */
                    if (fast_aggref && (numSimpleVars > 0)) {
                        DEFINE_BLOCK(agg_then, jitted_sonicbatchagg);
                        DEFINE_BLOCK(agg_else, jitted_sonicbatchagg);
                        DEFINE_BLOCK(agg_end, jitted_sonicbatchagg);
                        DEFINE_BLOCK(normal_bb, jitted_sonicbatchagg);
                        DEFINE_BLOCK(var_bisum_bb, jitted_sonicbatchagg);
                        DEFINE_BLOCK(var_numsum_bb, jitted_sonicbatchagg);

                        /* get sonic data according to sonic datum array and locidx */
                        /* calculate arrIdx and atomIdx */
                        Vals[0] = int64_0;
                        Vals[1] = int32_pos_sdarray_nbit;
                        nbit = builder.CreateInBoundsGEP(sdata[i], Vals);
                        nbit = builder.CreateLoad(int32Type, nbit, "arridx");
                        arrIdx = builder.CreateLShr(locidx, nbit);
                        arrIdx = builder.CreateSExt(arrIdx, int64Type);

                        Vals[1] = int32_pos_sdarray_atomsize;
                        atomsize = builder.CreateInBoundsGEP(sdata[i], Vals);
                        atomsize = builder.CreateLoad(int32Type, atomsize, "atomsize");
                        atomsize = builder.CreateSub(atomsize, int32_1);
                        atomIdx = builder.CreateAnd(atomsize, locidx);
                        atomIdx = builder.CreateSExt(atomIdx, int64Type);

                        /* get sdata[i]->m_arr[arrIdx] */
                        Vals[1] = int32_pos_sdarray_arr;
                        atom = builder.CreateInBoundsGEP(sdata[i], Vals);
                        atom = builder.CreateLoad(atomPtrPtrType, atom, "atom**");
                        atom = builder.CreateInBoundsGEP(atom, arrIdx);
                        atom = builder.CreateLoad(atomPtrType, atom, "atom*");

                        /* get address of sdata->m_arr[arrIdx]->data[atomIdx] */
                        Vals[1] = int32_pos_atom_data;
                        data = builder.CreateInBoundsGEP(atom, Vals);
                        data = builder.CreateLoad(int8PtrType, data, "data");
                        data = builder.CreateBitCast(data, int64PtrType);
                        llvm::Value* dataptr = builder.CreateInBoundsGEP(data, atomIdx);

                        /* get sdata->m_arr[arrIdx]->nullFlag[atomIdx] and check if it is NULL */
                        Vals[1] = int32_pos_atom_nullflag;
                        llvm::Value* nullflag = builder.CreateInBoundsGEP(atom, Vals);
                        nullflag = builder.CreateLoad(int8PtrType, nullflag, "nullflagptr");
                        nullflag = builder.CreateInBoundsGEP(nullflag, atomIdx);

                        /* load nullflag */
                        tmpval = builder.CreateLoad(int8Type, nullflag, "nullFlag");
                        tmpval = builder.CreateAnd(tmpval, int8_1);
                        tmpval = builder.CreateICmpEQ(tmpval, int8_0);
                        builder.CreateCondBr(tmpval, agg_else, agg_then);

                        /* sonic data is null, set value */
                        builder.SetInsertPoint(agg_then);

                        /*
                         * should make a new context to record the result : the
                         * following code corresponding to:
                         * 'leftarg = DatumGetBINumeric(pVal[i]);
                         * data->setValue(NumericGetDatum(leftarg, false, arrIndx, atomIdx))'.
                         */
                        tmpval = DatumGetBINumericCodeGen(&builder, result);
                        tmpval = builder.CreatePtrToInt(tmpval, int64Type);
                        tmpval = WrapaddVariableCodeGen(&builder, hcxt, tmpval);

                        builder.CreateStore(tmpval, dataptr);
                        builder.CreateStore(int8_0, nullflag);

                        /* turn to next basicblock or end this */
                        if (i == numaggs - 1)
                            builder.CreateBr(for_inc);
                        else
                            builder.CreateBr(agg_bb[i + 1]);

                        /* cell be not null, do aggregation */
                        builder.SetInsertPoint(agg_else);

                        /*
                         * When fast_aggref is true and numSimpleVars is greater than zero,
                         * the expr is numeric type var. Convert this numeric type data to
                         * SiNumeric data to get the value.
                         */
                        // return struct.NumericData*
                        llvm::Value* bires = DatumGetBINumericCodeGen(&builder, result);

                        /* extract the header of result to check if it is BINumeric */
                        Vals4[0] = int64_0;
                        Vals4[1] = int32_1;
                        Vals4[2] = int32_0;
                        Vals4[3] = int32_0;
                        tmpval = builder.CreateInBoundsGEP(bires, Vals4);
                        tmpval = builder.CreateLoad(int16Type, tmpval, "biheader");
                        llvm::Value* rflag = builder.CreateAnd(tmpval, val_mask);

                        /* get leftdata[0] and convert to BINumeric */
                        llvm::Value* realdata = builder.CreateLoad(int64Type, dataptr, "dataval");
                        llvm::Value* leftarg = DatumGetBINumericCodeGen(&builder, realdata);
                        tmpval = builder.CreateInBoundsGEP(leftarg, Vals4);
                        tmpval = builder.CreateLoad(int16Type, tmpval, "sonicheader");
                        llvm::Value* lflag = builder.CreateAnd(tmpval, val_mask);

                        /* check if either of them is not BI64 */
                        llvm::Value* oparg1 = builder.CreateICmpEQ(lflag, val_binum64);
                        llvm::Value* oparg2 = builder.CreateICmpEQ(rflag, val_binum64);
                        llvm::Value* bothbi64 = builder.CreateAnd(oparg1, oparg2);

                        /* use fast path only when both args are bi64 */
                        builder.CreateCondBr(bothbi64, var_bisum_bb, var_numsum_bb);

                        builder.SetInsertPoint(var_bisum_bb);
                        /* extract the actual data of numeric only when sonic data is not null */
                        Vals4[0] = int64_0;
                        Vals4[1] = int32_1;
                        Vals4[2] = int32_0;
                        Vals4[3] = int32_1;
                        tmpval = builder.CreateInBoundsGEP(bires, Vals4);
                        tmpval = builder.CreateBitCast(tmpval, int64PtrType);
                        llvm::Value* resval = builder.CreateLoad(int64Type, tmpval, "bivalue");

                        /* extrac the actual data of leftdata and do sum */
                        llvm::Value* sonicptr = builder.CreateAdd(realdata, int64_6);
                        sonicptr = builder.CreateIntToPtr(sonicptr, int64PtrType);
                        llvm::Value* midval = builder.CreateLoad(int64Type, sonicptr, "intvalue");

                        /* check overflow */
                        llvm::Type* Intrinsic_Tys[] = {int64Type};
                        llvm::Function* func_sonicadd_overflow =
                            llvm::Intrinsic::getDeclaration(mod, llvm_sadd_with_overflow, Intrinsic_Tys);
                        if (func_sonicadd_overflow == NULL) {
                            ereport(ERROR,
                                (errcode(ERRCODE_LOAD_INTRINSIC_FUNCTION_FAILED),
                                    errmodule(MOD_LLVM),
                                    errmsg("Cannot get the llvm::Intrinsic::sadd_with_overflow function!\n")));
                        }
                        llvm::Value* aggres = builder.CreateCall(func_sonicadd_overflow, {resval, midval});
                        llvm::Value* oflag = builder.CreateExtractValue(aggres, 1);
                        builder.CreateCondBr(oflag, var_numsum_bb, normal_bb);

                        /* if meet overflow during aggregation, turn to original sum function. */
                        builder.SetInsertPoint(var_numsum_bb);
                        llvm::Function* func_vsnumericsum =
                            llvmCodeGen->module()->getFunction("Jitted_sonic_numericsum");
                        if (NULL == func_vsnumericsum) {
                            func_vsnumericsum = vsnumeric_sum_codegen(aggref);
                        }
                        sdata[i] = builder.CreateBitCast(sdata[i], sonicEncodingDatumArrayPtrType);
                        builder.CreateCall(func_vsnumericsum, {sdata[i], locidx, result});
                        builder.CreateBr(agg_end);

                        builder.SetInsertPoint(normal_bb);
                        llvm::Value* sumval = builder.CreateExtractValue(aggres, 0);
                        builder.CreateStore(sumval, sonicptr);
                        builder.CreateBr(agg_end);

                        builder.SetInsertPoint(agg_end);
                    } else if (fast_aggref) {
                        /*
                         * If aggref can be evaluated in fast path and be numeric
                         * expressions, use the result from fastexpr.
                         */
                        Assert(bisum_bb != NULL);
                        Assert(numsum_bb != NULL);
                        DEFINE_BLOCK(agg_end, jitted_sonicbatchagg);
                        DEFINE_BLOCK(agg_then, jitted_sonicbatchagg);
                        DEFINE_BLOCK(agg_else, jitted_sonicbatchagg);
                        DEFINE_BLOCK(normal_bb, jitted_sonicbatchagg);
                        DEFINE_BLOCK(expr_bisum_bb, jitted_sonicbatchagg);
                        DEFINE_BLOCK(bioverflow_bb, jitted_sonicbatchagg);

                        /*
                         * if the result of expression is outofbound, turn to
                         * original numeric path.
                         */
                        builder.CreateBr(numsum_bb);

                        /*
                         * if the result of expression is BI64, extract it.
                         */
                        builder.SetInsertPoint(bisum_bb);
                        llvm::Value* resval = builder.CreateExtractValue(expres, 1);

                        /* get sonic data according to sonic datum array and locidx */
                        /* calculate arrIdx and atomIdx */
                        Vals[1] = int32_pos_sdarray_nbit;
                        nbit = builder.CreateInBoundsGEP(sdata[i], Vals);
                        nbit = builder.CreateLoad(int32Type, nbit, "arridx");
                        arrIdx = builder.CreateLShr(locidx, nbit);
                        arrIdx = builder.CreateSExt(arrIdx, int64Type);

                        Vals[1] = int32_pos_sdarray_atomsize;
                        atomsize = builder.CreateInBoundsGEP(sdata[i], Vals);
                        atomsize = builder.CreateLoad(int32Type, atomsize, "atomsize");
                        atomsize = builder.CreateSub(atomsize, int32_1);
                        atomIdx = builder.CreateAnd(atomsize, locidx);
                        atomIdx = builder.CreateSExt(atomIdx, int64Type);

                        /* get sdata[aggidx]->m_arr[arrIdx] */
                        Vals[1] = int32_pos_sdarray_arr;
                        atom = builder.CreateInBoundsGEP(sdata[i], Vals);
                        atom = builder.CreateLoad(atomPtrPtrType, atom, "atom**");
                        atom = builder.CreateInBoundsGEP(atom, arrIdx);
                        atom = builder.CreateLoad(atomPtrType, atom, "atom*");

                        /* get address of sdata[aggidx]->m_arr[arrIdx]->data[atomIdx] */
                        Vals[1] = int32_pos_atom_data;
                        data = builder.CreateInBoundsGEP(atom, Vals);
                        data = builder.CreateLoad(int8PtrType, data, "data");
                        data = builder.CreateBitCast(data, int64PtrType);
                        llvm::Value* dataptr = builder.CreateInBoundsGEP(data, atomIdx);

                        /* get sdata[aggidx]->m_arr[arrIdx]->nullFlag[atomIdx] and check if it is NULL */
                        Vals[1] = int32_pos_atom_nullflag;
                        llvm::Value* nullflag = builder.CreateInBoundsGEP(atom, Vals);
                        nullflag = builder.CreateLoad(int8PtrType, nullflag, "nullflagptr");
                        nullflag = builder.CreateInBoundsGEP(nullflag, atomIdx);

                        /* load nullflag */
                        tmpval = builder.CreateLoad(int8Type, nullflag, "nullFlag");
                        tmpval = builder.CreateAnd(tmpval, int8_1);
                        tmpval = builder.CreateICmpEQ(tmpval, int8_0);
                        builder.CreateCondBr(tmpval, agg_else, agg_then);

                        /* sonic data is null, set value */
                        builder.SetInsertPoint(agg_then);

                        /* get the aligned scale of this expression */
                        exprscale = GetAlignedScale(estate->expr);
                        llvm::Value* alignedscale = llvmCodeGen->getIntConstant(CHAROID, exprscale);

                        /*
                         * should make a new context to record the result : the
                         * following code corresponding to:
                         * 'leftarg = DatumGetBINumeric(pVal[i]);
                         * cell->m_val[idx].val = addVariable(context, NumericGetDatum(leftarg));'.
                         */
                        tmpval = WrapmakeNumeric64CodeGen(&builder, resval, alignedscale);
                        tmpval = DatumGetBINumericCodeGen(&builder, tmpval);
                        tmpval = builder.CreatePtrToInt(tmpval, int64Type);
                        tmpval = WrapaddVariableCodeGen(&builder, hcxt, tmpval);

                        builder.CreateStore(tmpval, dataptr);
                        builder.CreateStore(int8_0, nullflag);

                        /* turn to next basicblock or end this */
                        if (i == numaggs - 1)
                            builder.CreateBr(for_inc);
                        else
                            builder.CreateBr(agg_bb[i + 1]);

                        /* cell not be null, do aggregation */
                        builder.SetInsertPoint(agg_else);
                        llvm::Value* realdata = builder.CreateLoad(int64Type, dataptr, "dataval");

                        /* first make sure the value in sonic datum array is BI64 format */
                        llvm::Value* leftarg = DatumGetBINumericCodeGen(&builder, realdata);
                        Vals4[0] = int64_0;
                        Vals4[1] = int32_1;
                        Vals4[2] = int32_0;
                        Vals4[3] = int32_0;
                        tmpval = builder.CreateInBoundsGEP(leftarg, Vals4);
                        tmpval = builder.CreateLoad(int16Type, tmpval, "leftheader");
                        llvm::Value* biflag = builder.CreateAnd(tmpval, val_mask);
                        llvm::Value* isbi64 = builder.CreateICmpEQ(biflag, val_binum64);
                        builder.CreateCondBr(isbi64, expr_bisum_bb, bioverflow_bb);

                        /*
                         * do aggregation directly only when both expr value
                         * and cell value is bi64.
                         */
                        builder.SetInsertPoint(expr_bisum_bb);
                        llvm::Value* sonicptr = builder.CreateAdd(realdata, int64_6);
                        sonicptr = builder.CreateIntToPtr(sonicptr, int64PtrType);
                        llvm::Value* midval = builder.CreateLoad(int64Type, sonicptr, "intvalue");

                        /* check overflow */
                        llvm::Type* Intrinsic_Tys[] = {int64Type};
                        llvm::Function* func_sonicadd_overflow =
                            llvm::Intrinsic::getDeclaration(mod, llvm_sadd_with_overflow, Intrinsic_Tys);
                        if (func_sonicadd_overflow == NULL) {
                            ereport(ERROR,
                                (errcode(ERRCODE_LOAD_INTRINSIC_FUNCTION_FAILED),
                                    errmodule(MOD_LLVM),
                                    errmsg("Cannot get the llvm::Intrinsic::sadd_with_overflow function!")));
                        }
                        llvm::Value* aggres = builder.CreateCall(func_sonicadd_overflow, {resval, midval});
                        llvm::Value* oflag = builder.CreateExtractValue(aggres, 1);
                        builder.CreateCondBr(oflag, bioverflow_bb, normal_bb);

                        builder.SetInsertPoint(bioverflow_bb);
                        exprscale = GetAlignedScale(estate->expr);
                        llvm::Value* ascale = llvmCodeGen->getIntConstant(CHAROID, exprscale);
                        llvm::Value* bioverres = WrapmakeNumeric64CodeGen(&builder, resval, ascale);
                        builder.CreateBr(numsum_bb);

                        builder.SetInsertPoint(numsum_bb);
                        llvm::PHINode* numres = builder.CreatePHI(int64Type, 2);
                        numres->addIncoming(result, flag_then[i]);
                        numres->addIncoming(bioverres, bioverflow_bb);
                        llvm::Value* evalval = (llvm::Value*)numres;

                        llvm::Function* func_vsnumericsum =
                            llvmCodeGen->module()->getFunction("Jitted_sonic_numericsum");
                        if (NULL == func_vsnumericsum) {
                            func_vsnumericsum = vsnumeric_sum_codegen(aggref);
                        }
                        sdata[i] = builder.CreateBitCast(sdata[i], sonicEncodingDatumArrayPtrType);
                        builder.CreateCall(func_vsnumericsum, {sdata[i], locidx, evalval});
                        builder.CreateBr(agg_end);

                        /* if there is no overflow, extract result directly */
                        builder.SetInsertPoint(normal_bb);
                        llvm::Value* sumval = builder.CreateExtractValue(aggres, 0);
                        builder.CreateStore(sumval, sonicptr);
                        builder.CreateBr(agg_end);

                        builder.SetInsertPoint(agg_end);
                    } else {
                        llvm::Function* func_vsnumericsum =
                            llvmCodeGen->module()->getFunction("Jitted_sonic_numericsum");
                        if (NULL == func_vsnumericsum) {
                            func_vsnumericsum = vsnumeric_sum_codegen(aggref);
                        }
                        sdata[i] = builder.CreateBitCast(sdata[i], sonicEncodingDatumArrayPtrType);
                        builder.CreateCall(func_vsnumericsum, {sdata[i], locidx, result});
                    }
                } break;
                case NUMERICAVGFUNCOID: {
                    /*
                     * If aggref can be evaluated in fast path and just be
                     * simple vars, use the result from batch.
                     */
                    if (fast_aggref && (numSimpleVars > 0)) {
                        DEFINE_BLOCK(agg_then, jitted_sonicbatchagg);
                        DEFINE_BLOCK(agg_else, jitted_sonicbatchagg);
                        DEFINE_BLOCK(agg_end, jitted_sonicbatchagg);
                        DEFINE_BLOCK(normal_bb, jitted_sonicbatchagg);
                        DEFINE_BLOCK(bisum_bblock, jitted_sonicbatchagg);
                        DEFINE_BLOCK(numsum_bblock, jitted_sonicbatchagg);

                        /* get sonic data according to sonic datum array and locidx */
                        /* calculate arrIdx and atomIdx */
                        Vals[0] = int64_0;
                        Vals[1] = int32_pos_sdarray_nbit;
                        nbit = builder.CreateInBoundsGEP(sdata[i], Vals);
                        nbit = builder.CreateLoad(int32Type, nbit, "arridx");
                        arrIdx = builder.CreateLShr(locidx, nbit);
                        arrIdx = builder.CreateSExt(arrIdx, int64Type);

                        Vals[1] = int32_pos_sdarray_atomsize;
                        atomsize = builder.CreateInBoundsGEP(sdata[i], Vals);
                        atomsize = builder.CreateLoad(int32Type, atomsize, "atomsize");
                        atomsize = builder.CreateSub(atomsize, int32_1);
                        atomIdx = builder.CreateAnd(atomsize, locidx);
                        atomIdx = builder.CreateSExt(atomIdx, int64Type);

                        /* get sdata[aggidx]->m_arr[arrIdx] */
                        Vals[1] = int32_pos_sdarray_arr;
                        atom = builder.CreateInBoundsGEP(sdata[i], Vals);
                        atom = builder.CreateLoad(atomPtrPtrType, atom, "atom**");
                        atom = builder.CreateInBoundsGEP(atom, arrIdx);
                        atom = builder.CreateLoad(atomPtrType, atom, "atom*");

                        /* get address of sdata->m_arr[arrIdx]->data[atomIdx] */
                        Vals[1] = int32_pos_atom_data;
                        data = builder.CreateInBoundsGEP(atom, Vals);
                        data = builder.CreateLoad(int8PtrType, data, "data");
                        data = builder.CreateBitCast(data, int64PtrType);
                        llvm::Value* dataptr = builder.CreateInBoundsGEP(data, atomIdx);

                        /* get scount->m_arr[arrIdx] */
                        Vals[1] = int32_pos_sdarray_arr;
                        cntatom = builder.CreateInBoundsGEP(scount, Vals);
                        cntatom = builder.CreateLoad(atomPtrPtrType, cntatom, "cntatom**");
                        cntatom = builder.CreateInBoundsGEP(cntatom, arrIdx);
                        cntatom = builder.CreateLoad(atomPtrType, cntatom, "cntatom*");

                        /* get address of scount->m_arr[arrIdx]->data[atomIdx] */
                        Vals[1] = int32_pos_atom_data;
                        cntdata = builder.CreateInBoundsGEP(cntatom, Vals);
                        cntdata = builder.CreateLoad(int8PtrType, cntdata, "cntdata");
                        cntdata = builder.CreateBitCast(cntdata, int64PtrType);
                        llvm::Value* cntptr = builder.CreateInBoundsGEP(cntdata, atomIdx);

                        /* get sdata[aggidx]->m_arr[arrIdx]->nullFlag[atomIdx] */
                        Vals[1] = int32_pos_atom_nullflag;
                        llvm::Value* dataflag = builder.CreateInBoundsGEP(atom, Vals);
                        dataflag = builder.CreateLoad(int8PtrType, dataflag, "dataflagptr");
                        dataflag = builder.CreateInBoundsGEP(dataflag, atomIdx);

                        /* get scount>m_arr[arrIdx]->nullFlag[atomIdx] */
                        Vals[1] = int32_pos_atom_nullflag;
                        llvm::Value* cntflag = builder.CreateInBoundsGEP(cntatom, Vals);
                        cntflag = builder.CreateLoad(int8PtrType, cntflag, "cntflagptr");
                        cntflag = builder.CreateInBoundsGEP(cntflag, atomIdx);

                        /* Now load the sonic data flag and check it */
                        tmpval = builder.CreateLoad(int8Type, dataflag, "dataFlag");
                        tmpval = builder.CreateAnd(tmpval, int8_1);
                        tmpval = builder.CreateICmpEQ(tmpval, int8_0);
                        builder.CreateCondBr(tmpval, agg_else, agg_then);

                        /* sonic data is null, store the value in data and count */
                        builder.SetInsertPoint(agg_then);
                        /* do leftarg = DatumGetBINumeric(pVal[i]) */
                        tmpval = DatumGetBINumericCodeGen(&builder, result);
                        tmpval = builder.CreatePtrToInt(tmpval, int64Type);
                        /* corresponding to addVariable(context, NumericGetDatum(leftarg)) */
                        tmpval = WrapaddVariableCodeGen(&builder, hcxt, tmpval);
                        builder.CreateStore(tmpval, dataptr);

                        /* count set to be one */
                        builder.CreateStore(int64_1, cntptr);

                        /* set cell flag */
                        builder.CreateStore(int8_0, dataflag);
                        builder.CreateStore(int8_0, cntflag);

                        /* turn to next basicblock or end this */
                        if (i == numaggs - 1)
                            builder.CreateBr(for_inc);
                        else
                            builder.CreateBr(agg_bb[i + 1]);

                        /* cell be not null, do aggregation */
                        builder.SetInsertPoint(agg_else);
                        /*
                         * When fast_aggref is true and numSimpleVars is greater than zero,
                         * the expr is numeric type var. Convert this numeric type data to
                         * SiNumeric data to get the value.
                         */
                        llvm::Value* bires = DatumGetBINumericCodeGen(&builder, result);

                        /* extract the header of result to check if it is BINumeric */
                        Vals4[0] = int64_0;
                        Vals4[1] = int32_1;
                        Vals4[2] = int32_0;
                        Vals4[3] = int32_0;
                        tmpval = builder.CreateInBoundsGEP(bires, Vals4);
                        tmpval = builder.CreateLoad(int16Type, tmpval, "biheader");
                        llvm::Value* rflag = builder.CreateAnd(tmpval, val_mask);

                        /* get leftdata[0] and convert to Numeric to get lflag */
                        llvm::Value* realdata = builder.CreateLoad(int64Type, dataptr, "dataval");
                        llvm::Value* leftarg = builder.CreateIntToPtr(realdata, numericPtrType);
                        tmpval = builder.CreateInBoundsGEP(leftarg, Vals4);
                        tmpval = builder.CreateLoad(int16Type, tmpval, "sonicheader");
                        llvm::Value* lflag = builder.CreateAnd(tmpval, val_mask);

                        /* check if either of them is not BI64 */
                        llvm::Value* oparg1 = builder.CreateICmpEQ(lflag, val_binum64);
                        llvm::Value* oparg2 = builder.CreateICmpEQ(rflag, val_binum64);
                        llvm::Value* bothbi64 = builder.CreateAnd(oparg1, oparg2);

                        /* use fast path only when both args are bi64 */
                        builder.CreateCondBr(bothbi64, bisum_bblock, numsum_bblock);

                        builder.SetInsertPoint(bisum_bblock);
                        /* extract the actual data of numeric only when value is not null */
                        Vals4[0] = int64_0;
                        Vals4[1] = int32_1;
                        Vals4[2] = int32_0;
                        Vals4[3] = int32_1;
                        tmpval = builder.CreateInBoundsGEP(bires, Vals4);
                        tmpval = builder.CreateBitCast(tmpval, int64PtrType);
                        llvm::Value* resval = builder.CreateLoad(int64Type, tmpval, "value");

                        llvm::Value* sonicptr = builder.CreateAdd(realdata, int64_6);
                        sonicptr = builder.CreateIntToPtr(sonicptr, int64PtrType);
                        llvm::Value* midval = builder.CreateLoad(int64Type, sonicptr);

                        /* check overflow */
                        llvm::Type* Intrinsic_Tys[] = {int64Type};
                        llvm::Function* func_sonicadd_overflow =
                            llvm::Intrinsic::getDeclaration(mod, llvm_sadd_with_overflow, Intrinsic_Tys);
                        if (func_sonicadd_overflow == NULL) {
                            ereport(ERROR,
                                (errcode(ERRCODE_LOAD_INTRINSIC_FUNCTION_FAILED),
                                    errmodule(MOD_LLVM),
                                    errmsg("Cannot get the llvm::Intrinsic::sadd_with_overflow function!\n")));
                        }
                        llvm::Value* aggres = builder.CreateCall(func_sonicadd_overflow, {resval, midval});
                        llvm::Value* oflag = builder.CreateExtractValue(aggres, 1);
                        builder.CreateCondBr(oflag, numsum_bblock, normal_bb);

                        builder.SetInsertPoint(numsum_bblock);
                        llvm::Function* func_vsnumericavg =
                            llvmCodeGen->module()->getFunction("Jitted_sonic_numericavg");
                        if (NULL == func_vsnumericavg) {
                            func_vsnumericavg = vsnumeric_avg_codegen(aggref);
                        }
                        sdata[i] = builder.CreateBitCast(sdata[i], sonicEncodingDatumArrayPtrType);
                        builder.CreateCall(func_vsnumericavg, {sdata[i], scount, locidx, result});
                        builder.CreateBr(agg_end);

                        builder.SetInsertPoint(normal_bb);
                        llvm::Value* sumval = builder.CreateExtractValue(aggres, 0);
                        builder.CreateStore(sumval, sonicptr);

                        /* cell->m_val[idx+1].val++ */
                        tmpval = builder.CreateLoad(int64Type, cntptr, "count");
                        tmpval = builder.CreateAdd(tmpval, int64_1);
                        builder.CreateStore(tmpval, cntptr);
                        builder.CreateBr(agg_end);

                        builder.SetInsertPoint(agg_end);
                    } else if (fast_aggref) {
                        /*
                         * If aggref can be evaluated in fast path and be numeric
                         * expressions, use the result from fastexpr.
                         */
                        Assert(bisum_bb != NULL);
                        Assert(numsum_bb != NULL);
                        DEFINE_BLOCK(agg_end, jitted_sonicbatchagg);
                        DEFINE_BLOCK(agg_then, jitted_sonicbatchagg);
                        DEFINE_BLOCK(agg_else, jitted_sonicbatchagg);
                        DEFINE_BLOCK(normal_bb, jitted_sonicbatchagg);
                        DEFINE_BLOCK(expr_bisum_bb, jitted_sonicbatchagg);
                        DEFINE_BLOCK(bioverflow_bb, jitted_sonicbatchagg);

                        builder.CreateBr(numsum_bb);

                        builder.SetInsertPoint(bisum_bb);
                        llvm::Value* resval = builder.CreateExtractValue(expres, 1);

                        /* get sonic data according to sonic datum array and locidx */
                        /* calculate arrIdx and atomIdx */
                        Vals[0] = int64_0;
                        Vals[1] = int32_pos_sdarray_nbit;
                        nbit = builder.CreateInBoundsGEP(sdata[i], Vals);
                        nbit = builder.CreateLoad(int32Type, nbit, "arridx");
                        arrIdx = builder.CreateLShr(locidx, nbit);
                        arrIdx = builder.CreateSExt(arrIdx, int64Type);

                        Vals[1] = int32_pos_sdarray_atomsize;
                        atomsize = builder.CreateInBoundsGEP(sdata[i], Vals);
                        atomsize = builder.CreateLoad(int32Type, atomsize, "atomsize");
                        atomsize = builder.CreateSub(atomsize, int32_1);
                        atomIdx = builder.CreateAnd(atomsize, locidx);
                        atomIdx = builder.CreateSExt(atomIdx, int64Type);

                        /* get sdata[aggidx]->m_arr[arrIdx] */
                        Vals[1] = int32_pos_sdarray_arr;
                        atom = builder.CreateInBoundsGEP(sdata[i], Vals);
                        atom = builder.CreateLoad(atomPtrPtrType, atom, "atom**");
                        atom = builder.CreateInBoundsGEP(atom, arrIdx);
                        atom = builder.CreateLoad(atomPtrType, atom, "atom*");

                        /* get address of sdata->m_arr[arrIdx]->data[atomIdx] */
                        Vals[1] = int32_pos_atom_data;
                        data = builder.CreateInBoundsGEP(atom, Vals);
                        data = builder.CreateLoad(int8PtrType, data, "data");
                        data = builder.CreateBitCast(data, int64PtrType);
                        llvm::Value* dataptr = builder.CreateInBoundsGEP(data, atomIdx);

                        /* get scount->m_arr[arrIdx] */
                        Vals[1] = int32_pos_sdarray_arr;
                        cntatom = builder.CreateInBoundsGEP(scount, Vals);
                        cntatom = builder.CreateLoad(atomPtrPtrType, cntatom, "cnt atom**");
                        cntatom = builder.CreateInBoundsGEP(cntatom, arrIdx);
                        cntatom = builder.CreateLoad(atomPtrType, cntatom, "cnt atom*");

                        /* get address of scount->m_arr[arrIdx]->data[atomIdx] */
                        Vals[1] = int32_pos_atom_data;
                        cntdata = builder.CreateInBoundsGEP(cntatom, Vals);
                        cntdata = builder.CreateLoad(int8PtrType, cntdata, "cntdata");
                        cntdata = builder.CreateBitCast(cntdata, int64PtrType);
                        llvm::Value* cntptr = builder.CreateInBoundsGEP(cntdata, atomIdx);

                        /* get sdata[aggidx]->m_arr[arrIdx]->nullFlag[atomIdx] */
                        Vals[1] = int32_pos_atom_nullflag;
                        llvm::Value* dataflag = builder.CreateInBoundsGEP(atom, Vals);
                        dataflag = builder.CreateLoad(int8PtrType, dataflag, "dataflagptr");
                        dataflag = builder.CreateInBoundsGEP(dataflag, atomIdx);

                        /* get scount>m_arr[arrIdx]->nullFlag[atomIdx] */
                        Vals[1] = int32_pos_atom_nullflag;
                        llvm::Value* cntflag = builder.CreateInBoundsGEP(cntatom, Vals);
                        cntflag = builder.CreateLoad(int8PtrType, cntflag, "cntflagptr");
                        cntflag = builder.CreateInBoundsGEP(cntflag, atomIdx);

                        /* Now load the sonic data flag and check it */
                        tmpval = builder.CreateLoad(int8Type, dataflag, "dataFlag");
                        tmpval = builder.CreateAnd(tmpval, int8_1);
                        tmpval = builder.CreateICmpEQ(tmpval, int8_0);
                        builder.CreateCondBr(tmpval, agg_else, agg_then);

                        /* cell be null, add Variable */
                        builder.SetInsertPoint(agg_then);
                        exprscale = GetAlignedScale(estate->expr);
                        llvm::Value* alignedscale = llvmCodeGen->getIntConstant(CHAROID, exprscale);
                        /*
                         * should make a new context to record the result : the
                         * following code corresponding to:
                         * 'leftarg = DatumGetBINumeric(pVal[i]);
                         * data->setValue(NumericGetDatum(leftarg), false, arrIdx, atomIdx);'.
                         */
                        tmpval = WrapmakeNumeric64CodeGen(&builder, resval, alignedscale);
                        tmpval = DatumGetBINumericCodeGen(&builder, tmpval);
                        tmpval = builder.CreatePtrToInt(tmpval, int64Type);
                        tmpval = WrapaddVariableCodeGen(&builder, hcxt, tmpval);
                        builder.CreateStore(tmpval, dataptr);
                        /* count set to be one */
                        builder.CreateStore(int64_1, cntptr);
                        /* set the flag of hashcell */
                        builder.CreateStore(int8_0, dataflag);
                        builder.CreateStore(int8_0, cntflag);

                        /* turn to next basicblock */
                        if (i == numaggs - 1)
                            builder.CreateBr(for_inc);
                        else
                            builder.CreateBr(agg_bb[i + 1]);

                        builder.SetInsertPoint(agg_else);
                        llvm::Value* realdata = builder.CreateLoad(int64Type, dataptr, "cell_val");

                        /* first make sure the value in cell is BI64 format */
                        Vals4[0] = int64_0;
                        Vals4[1] = int32_1;
                        Vals4[2] = int32_0;
                        Vals4[3] = int32_0;
                        llvm::Value* leftflag = builder.CreateIntToPtr(realdata, numericPtrType);
                        tmpval = builder.CreateInBoundsGEP(leftflag, Vals4);
                        tmpval = builder.CreateLoad(int16Type, tmpval, "leftheader");
                        llvm::Value* biflag = builder.CreateAnd(tmpval, val_mask);
                        llvm::Value* isbi64 = builder.CreateICmpEQ(biflag, val_binum64);
                        builder.CreateCondBr(isbi64, expr_bisum_bb, bioverflow_bb);

                        /*
                         * do aggregation directly only when both expr value
                         * and cell value is bi64.
                         */
                        builder.SetInsertPoint(expr_bisum_bb);
                        llvm::Value* sonicptr = builder.CreateAdd(realdata, int64_6);
                        sonicptr = builder.CreateIntToPtr(sonicptr, int64PtrType);
                        llvm::Value* midval = builder.CreateLoad(int64Type, sonicptr);

                        /* check overflow */
                        llvm::Type* Intrinsic_Tys[] = {int64Type};
                        llvm::Function* func_sonicadd_overflow =
                            llvm::Intrinsic::getDeclaration(mod, llvm_sadd_with_overflow, Intrinsic_Tys);
                        if (func_sonicadd_overflow == NULL) {
                            ereport(ERROR,
                                (errcode(ERRCODE_LOAD_INTRINSIC_FUNCTION_FAILED),
                                    errmodule(MOD_LLVM),
                                    errmsg("Cannot get the llvm::Intrinsic::sadd_with_overflow function!\n")));
                        }
                        llvm::Value* aggres = builder.CreateCall(func_sonicadd_overflow, {resval, midval});
                        llvm::Value* oflag = builder.CreateExtractValue(aggres, 1);
                        builder.CreateCondBr(oflag, bioverflow_bb, normal_bb);

                        /* make numeric64 when meet overflow */
                        builder.SetInsertPoint(bioverflow_bb);
                        exprscale = GetAlignedScale(estate->expr);
                        llvm::Value* ascale = llvmCodeGen->getIntConstant(CHAROID, exprscale);
                        llvm::Value* bioverres = WrapmakeNumeric64CodeGen(&builder, resval, ascale);
                        builder.CreateBr(numsum_bb);

                        builder.SetInsertPoint(numsum_bb);
                        llvm::PHINode* numres = builder.CreatePHI(int64Type, 2);
                        numres->addIncoming(result, flag_then[i]);
                        numres->addIncoming(bioverres, bioverflow_bb);
                        llvm::Value* evalval = (llvm::Value*)numres;

                        llvm::Function* func_vsnumericavg =
                            llvmCodeGen->module()->getFunction("Jitted_sonic_numericavg");
                        if (NULL == func_vsnumericavg) {
                            func_vsnumericavg = vsnumeric_avg_codegen(aggref);
                        }
                        sdata[i] = builder.CreateBitCast(sdata[i], sonicEncodingDatumArrayPtrType);
                        builder.CreateCall(func_vsnumericavg, {sdata[i], scount, locidx, evalval});
                        builder.CreateBr(agg_end);

                        /* if there is no overflow, extract result directly */
                        builder.SetInsertPoint(normal_bb);
                        llvm::Value* sumval = builder.CreateExtractValue(aggres, 0);
                        builder.CreateStore(sumval, sonicptr);

                        /* cell->m_val[idx+1].val++ */
                        tmpval = builder.CreateLoad(int64Type, cntptr, "count");
                        tmpval = builder.CreateAdd(tmpval, int64_1);
                        builder.CreateStore(tmpval, cntptr);
                        builder.CreateBr(agg_end);

                        builder.SetInsertPoint(agg_end);
                    } else {
                        llvm::Function* func_vsnumericavg =
                            llvmCodeGen->module()->getFunction("Jitted_sonic_numericavg");
                        if (NULL == func_vsnumericavg) {
                            func_vsnumericavg = vsnumeric_avg_codegen(aggref);
                        }
                        sdata[i] = builder.CreateBitCast(sdata[i], sonicEncodingDatumArrayPtrType);
                        builder.CreateCall(func_vsnumericavg, {sdata[i], scount, locidx, result});
                    }
                } break;
                default:
                    ereport(ERROR,
                        (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmodule(MOD_LLVM),
                            errmsg("Unsupported agg function %u!", aggref->aggfnoid)));
                    break;
            }

            if (i == numaggs - 1)
                builder.CreateBr(for_inc);
            else
                builder.CreateBr(agg_bb[i + 1]);

            /* if the current flag is null, turn to next agg */
            builder.SetInsertPoint(flag_else[i]);
            if (i == numaggs - 1)
                builder.CreateBr(for_inc);
            else
                builder.CreateBr(agg_bb[i + 1]);
        }
    }

    /* codegen in the for_inc basic block: compare the loop index with nrows */
    builder.SetInsertPoint(for_inc);
    tmpval = builder.CreateTrunc(idx_next, int32Type);
    tmpval = builder.CreateICmpEQ(tmpval, nValues);
    builder.CreateCondBr(tmpval, for_end, for_body);

    /* codegen in for_end basic block: just return void */
    builder.SetInsertPoint(for_end);
    (void)VecExprCodeGen::MemCxtSwitToCodeGen(&builder, agg_oldcontext);
    (void)WrapResetEContextCodeGen(&builder, mecontext);
    (void)WrapResetEContextCodeGen(&builder, econtext);
    builder.CreateRetVoid();

    pfree_ext(aggIdxList);
    pfree_ext(sdata);
    pfree_ext(agg_bb);
    pfree_ext(flag_then);
    pfree_ext(flag_else);
    pfree_ext(batch_vals);
    pfree_ext(batch_flag);

    llvmCodeGen->FinalizeFunction(jitted_sonicbatchagg, node->ss.ps.plan->plan_node_id);

    return jitted_sonicbatchagg;
}

void VecHashAggCodeGen::WarpAllocHashSlotCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* hAggRunner,
    llvm::Value* batch, llvm::Value* idx, llvm::Value* keysimple)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    /* Define data types and some llvm consts */
    DEFINE_CG_VOIDTYPE(voidType);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_PTRTYPE(vectorBatchPtrType, "class.VectorBatch");
    DEFINE_CG_PTRTYPE(hashAggRunnerPtrType, "class.HashAggRunner");

    llvm::Function* func_allochashslot = llvmCodeGen->module()->getFunction("IRAllocHashSlot");
    if (NULL == func_allochashslot) {
        GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "IRAllocHashSlot", voidType);
        fn_prototype.addArgument(GsCodeGen::NamedVariable("hAggRunner", hashAggRunnerPtrType));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("batch", vectorBatchPtrType));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("idx", int32Type));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("keysimple", int32Type));
        func_allochashslot = fn_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("IRAllocHashSlot", (void*)WrapAllocHashSlot);
    }

    idx = ptrbuilder->CreateTrunc(idx, int32Type);
    ptrbuilder->CreateCall(func_allochashslot, {hAggRunner, batch, idx, keysimple});

    return;
}

void VecHashAggCodeGen::WarpSglTblAllocHashSlotCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* hAggRunner,
    llvm::Value* batch, llvm::Value* idx, llvm::Value* keysimple)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    /* Define data types and some llvm consts */
    DEFINE_CG_VOIDTYPE(voidType);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_PTRTYPE(vectorBatchPtrType, "class.VectorBatch");
    DEFINE_CG_PTRTYPE(hashAggRunnerPtrType, "class.HashAggRunner");

    llvm::Function* func_sgltblallochashslot = llvmCodeGen->module()->getFunction("IRSglTblAllocHashSlot");
    if (NULL == func_sgltblallochashslot) {
        GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "IRSglTblAllocHashSlot", voidType);
        fn_prototype.addArgument(GsCodeGen::NamedVariable("hAggRunner", hashAggRunnerPtrType));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("batch", vectorBatchPtrType));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("idx", int32Type));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("keysimple", int32Type));
        func_sgltblallochashslot = fn_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("IRSglTblAllocHashSlot", (void*)WrapSglTblAllocHashSlot);
    }

    idx = ptrbuilder->CreateTrunc(idx, int32Type);
    ptrbuilder->CreateCall(func_sgltblallochashslot, {hAggRunner, batch, idx, keysimple});

    return;
}

void VecHashAggCodeGen::WrapResetEContextCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* econtext)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    /* Define data types and some llvm consts */
    DEFINE_CG_VOIDTYPE(voidType);
    DEFINE_CG_PTRTYPE(ExprContextPtrType, "struct.ExprContext");

    /* Associate with ResetExprContext */
    llvm::Function* func_resetec = llvmCodeGen->module()->getFunction("IRResetExprContext");
    if (NULL == func_resetec) {
        GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "IRResetExprContext", voidType);
        fn_prototype.addArgument(GsCodeGen::NamedVariable("econtext", ExprContextPtrType));
        func_resetec = fn_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("IRResetExprContext", (void*)WrapResetExprContext);
    }

    ptrbuilder->CreateCall(func_resetec, econtext);
    return;
}

/*
 * @Description	: Generate the function to prefetch the hash cell ahead
 * 				  of 2, which means hint fetch loc[i+2]. This function
 *				  is called by BatchAggregation.
 * @return		: return llvm prefetch function for batchaggregation.
 */
llvm::Function* prefetchBatchAggregationCodeGen()
{
    Assert(NULL != (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj);
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    /* Get LLVM Context and builder */
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    llvm::Value* tmpval = NULL;
    llvm::Value* llvmargs[3];
    llvm::Function* jitted_batchagg_prefetch = NULL;

    /* Define data types and some llvm consts */
    DEFINE_CG_VOIDTYPE(voidType);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(hashCellPtrType, "struct.hashCell");
    DEFINE_CG_PTRPTRTYPE(hashCellPtrPtrType, "struct.hashCell");

    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT32(int32_3, 3);
    DEFINE_CGVAR_INT64(int64_distance, PREFETCH_BATCHAGGREGATION_DISTANCE);

    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "prefetchBatchAggregation", voidType);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("Loc", hashCellPtrPtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("Idx", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("nrows", int64Type));
    jitted_batchagg_prefetch = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* Loc = llvmargs[0];
    llvm::Value* idx = llvmargs[1];
    llvm::Value* nrows = llvmargs[2];

    DEFINE_BLOCK(prefetch_bb, jitted_batchagg_prefetch);
    DEFINE_BLOCK(ret_bb, jitted_batchagg_prefetch);

    llvm::Value* prefetchIdx = builder.CreateAdd(idx, int64_distance);
    tmpval = builder.CreateICmpSLT(prefetchIdx, nrows);
    builder.CreateCondBr(tmpval, prefetch_bb, ret_bb);

    builder.SetInsertPoint(prefetch_bb);
    /* in llvm10 llvm::Intrinsic::prefetch is define as uint value 217 */
    llvm::Function* fn_prefetch = llvm::Intrinsic::getDeclaration(llvmCodeGen->module(), llvm_prefetch);
    if (fn_prefetch == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_LOAD_INTRINSIC_FUNCTION_FAILED),
                errmsg("Failed to get llvm function Intrinsic::prefetch!\n")));
    }
    tmpval = builder.CreateInBoundsGEP(Loc, prefetchIdx);
    tmpval = builder.CreateLoad(hashCellPtrType, tmpval, "hashCell");
    tmpval = builder.CreateBitCast(tmpval, int8PtrType);
    builder.CreateCall(fn_prefetch, {tmpval, int32_0, int32_3, int32_1});
    builder.CreateBr(ret_bb);

    builder.SetInsertPoint(ret_bb);
    builder.CreateRetVoid();

    llvmCodeGen->FinalizeFunction(jitted_batchagg_prefetch);

    return jitted_batchagg_prefetch;
}

/*
 * @Description	: Generate the function to prefetch the hash cell ahead
 * 				  of 2, which means hint fetch loc[i+2], and prefetch the
 *				  address of m_data ahead of 4, which means hint fetch
 *				  m_data[m_cacheLoc[i+4]]. This function is called in the
 *				  loop of the matching key.
 * @return		: return llvm function for building hash table in batchagg.
 */
llvm::Function* prefetchAggHashingCodeGen()
{
    Assert(NULL != (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj);
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    /* Get LLVM Context and builder */
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    llvm::Value* tmpval = NULL;
    llvm::Value* llvmargs[3];
    llvm::Function* jitted_hashing_prefetch = NULL;

    /* Define data types and some llvm consts */
    DEFINE_CG_VOIDTYPE(voidType);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(hashAggRunnerPtrType, "class.HashAggRunner");
    DEFINE_CG_PTRTYPE(hashSegTblPtrType, "struct.HashSegTbl");
    DEFINE_CG_PTRTYPE(hashCellPtrType, "struct.hashCell");
    DEFINE_CG_PTRPTRTYPE(hashCellPtrPtrType, "struct.hashCell");

    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT32(int32_3, 3);
    DEFINE_CGVAR_INT32(int32_pos_hAggR_hashVal, pos_hAggR_hashVal);
    DEFINE_CGVAR_INT32(int32_pos_hAggR_hSegTbl, pos_hAggR_hSegTbl);
    DEFINE_CGVAR_INT32(int32_pos_hAggR_hsegmax, pos_hAggR_hsegmax);
    DEFINE_CGVAR_INT32(int32_pos_hAggR_hashSize, pos_hAggR_hashSize);
    DEFINE_CGVAR_INT64(int64_0, 0);
    DEFINE_CGVAR_INT64(int64_1, 1);
    DEFINE_CGVAR_INT64(int64_distance1, PREFETCH_AGGHASHING_DISTANCE1);
    DEFINE_CGVAR_INT64(int64_distance2, PREFETCH_AGGHASHING_DISTANCE2);

    llvm::Value* Vals2[2] = {int64_0, int32_0};
    llvm::Value* Vals3[3] = {int64_0, int32_0, int32_0};

    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "prefetchAggHashing", voidType);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("hAggRunner", hashAggRunnerPtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("Idx", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("nrows", int64Type));
    jitted_hashing_prefetch = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* hAggRunner = llvmargs[0];
    llvm::Value* idx = llvmargs[1];
    llvm::Value* nrows = llvmargs[2];

    DEFINE_BLOCK(prefetch_hashData_bb, jitted_hashing_prefetch);
    DEFINE_BLOCK(prefetch_cell_bb, jitted_hashing_prefetch);
    DEFINE_BLOCK(ret_bb, jitted_hashing_prefetch);

    /*
     * try to prefetch hashData[m_cacheLoc[i+4]] : only do this when we have
     * enough rows.
     */
    llvm::Value* prefetchIdx = builder.CreateAdd(idx, int64_distance1);
    tmpval = builder.CreateICmpSLT(prefetchIdx, nrows);
    builder.CreateCondBr(tmpval, prefetch_hashData_bb, ret_bb);

    builder.SetInsertPoint(prefetch_hashData_bb);
    /* mask = hashAggRunner.m_hashSize - 1 */
    Vals2[1] = int32_pos_hAggR_hashSize;
    tmpval = builder.CreateInBoundsGEP(hAggRunner, Vals2);
    llvm::Value* maskval = builder.CreateLoad(int64Type, tmpval, "m_hashSize");
    maskval = builder.CreateSub(maskval, int64_1, "mask");

    /* calculate m_cacheLoc[i+4] = m_hashVal[i+4] & mask */
    llvm::Value* next_hashData_idx = builder.CreateAdd(idx, int64_distance1);
    Vals3[1] = int32_pos_hAggR_hashVal;
    Vals3[2] = next_hashData_idx;
    llvm::Value* hashValSlot = builder.CreateInBoundsGEP(hAggRunner, Vals3);
    llvm::Value* hashVal64 = builder.CreateLoad(int64Type, hashValSlot, "hashVal");
    llvm::Value* cacheLocVal = builder.CreateAnd(hashVal64, maskval);

    /* get hashData = hashAggRunner->m_hashData.tbl_data */
    /* calculate nsegs and pos according to m_cacheLoc[i+4] */
    Vals2[0] = int64_0;
    Vals2[1] = int32_pos_hAggR_hsegmax;
    llvm::Value* segmaxval = builder.CreateInBoundsGEP(hAggRunner, Vals2);
    segmaxval = builder.CreateLoad(int32Type, segmaxval, "segmax");
    segmaxval = builder.CreateSExt(segmaxval, int64Type);
    /* nsegs  =  m_cacheLoc[i+4] / m_hashseg_max */
    llvm::Value* nsegsval = builder.CreateExactUDiv(cacheLocVal, segmaxval, "nsegs");
    nsegsval = builder.CreateTrunc(nsegsval, int32Type);
    /* pos  =  m_cacheLoc[i+4] % m_hashseg_max */
    llvm::Value* pos = builder.CreateSRem(cacheLocVal, segmaxval, "pos");
    /* get m_hashData from hashAggRunner */
    Vals2[0] = int64_0;
    Vals2[1] = int32_pos_hAggR_hSegTbl;
    llvm::Value* hashData = builder.CreateInBoundsGEP(hAggRunner, Vals2);
    hashData = builder.CreateLoad(hashSegTblPtrType, hashData, "m_hashData");
    Vals2[0] = nsegsval;
    Vals2[1] = int32_1;
    llvm::Value* tbldata = builder.CreateInBoundsGEP(hashData, Vals2);
    tbldata = builder.CreateLoad(hashCellPtrPtrType, tbldata, "tbl_data");
    llvm::Value* next_hashData_addr = builder.CreateInBoundsGEP(tbldata, pos);

    llvm::Function* fn_prefetch = llvm::Intrinsic::getDeclaration(llvmCodeGen->module(), llvm_prefetch);
    if (fn_prefetch == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_LOAD_INTRINSIC_FUNCTION_FAILED),
                errmodule(MOD_LLVM),
                errmsg("Failed to get function Intrinsic::prefetch!\n")));
    }
    tmpval = builder.CreateBitCast(next_hashData_addr, int8PtrType);
    builder.CreateCall(fn_prefetch, {tmpval, int32_0, int32_3, int32_1});

    /* calculate m_cacheLoc[i+2] = m_hashVal[i+2] & mask */
    llvm::Value* next_cell_idx = builder.CreateAdd(idx, int64_distance2);
    Vals3[1] = int32_pos_hAggR_hashVal;
    Vals3[2] = next_cell_idx;
    hashValSlot = builder.CreateInBoundsGEP(hAggRunner, Vals3);
    hashVal64 = builder.CreateLoad(int64Type, hashValSlot, "hashVal");
    cacheLocVal = builder.CreateAnd(hashVal64, maskval);

    /* nsegs  =  m_cacheLoc[i+2] / m_hashseg_max */
    llvm::Value* nsegsval2 = builder.CreateExactUDiv(cacheLocVal, segmaxval, "nsegs");
    nsegsval2 = builder.CreateTrunc(nsegsval2, int32Type);
    /* pos  =  m_cacheLoc[i+2] % m_hashseg_max */
    llvm::Value* pos2 = builder.CreateSRem(cacheLocVal, segmaxval, "pos");
    Vals2[0] = nsegsval2;
    Vals2[1] = int32_1;
    llvm::Value* tbldata2 = builder.CreateInBoundsGEP(hashData, Vals2);
    tbldata2 = builder.CreateLoad(hashCellPtrPtrType, tbldata2, "tbl_data");
    tmpval = builder.CreateInBoundsGEP(tbldata2, pos2);
    llvm::Value* next_cell = builder.CreateLoad(hashCellPtrType, tmpval, "cell");

    tmpval = builder.CreatePtrToInt(next_cell, int64Type);
    tmpval = builder.CreateICmpEQ(tmpval, int64_0);
    builder.CreateCondBr(tmpval, ret_bb, prefetch_cell_bb);

    builder.SetInsertPoint(prefetch_cell_bb);
    next_cell = builder.CreateBitCast(next_cell, int8PtrType);
    builder.CreateCall(fn_prefetch, {next_cell, int32_0, int32_3, int32_1});
    builder.CreateBr(ret_bb);

    builder.SetInsertPoint(ret_bb);
    builder.CreateRetVoid();

    llvmCodeGen->FinalizeFunction(jitted_hashing_prefetch);

    return jitted_hashing_prefetch;
}

/*
 * @Description	: Generate the function to prefetch the hash cell ahead
 * 				  of 2, which means hint fetch loc[i+2], and prefetch the
 *				  address of m_data ahead of 4, which means hint fetch
 *				  m_data[m_cacheLoc[i+4]]. This function is called in the
 *				  loop of the matching key.
 * @return		: return llvm function for building hash table in batchagg.
 */
llvm::Function* prefetchAggSglTblHashingCodeGen()
{
    Assert(NULL != (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj);
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    /* Get LLVM Context and builder */
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    llvm::Value* tmpval = NULL;
    llvm::Value* llvmargs[3];
    llvm::Function* jitted_hashing_prefetch = NULL;

    /* Define data types and some llvm consts */
    DEFINE_CG_VOIDTYPE(voidType);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(hashAggRunnerPtrType, "class.HashAggRunner");
    DEFINE_CG_PTRTYPE(hashCellPtrType, "struct.hashCell");
    DEFINE_CG_PTRPTRTYPE(hashCellPtrPtrType, "struct.hashCell");
    DEFINE_CG_PTRTYPE(hashSegTblPtrType, "struct.HashSegTbl");

    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT32(int32_3, 3);
    DEFINE_CGVAR_INT32(int32_pos_hAggR_hashVal, pos_hAggR_hashVal);
    DEFINE_CGVAR_INT32(int32_pos_hAggR_hSegTbl, pos_hAggR_hSegTbl);
    DEFINE_CGVAR_INT32(int32_pos_hAggR_hashSize, pos_hAggR_hashSize);
    DEFINE_CGVAR_INT64(int64_0, 0);
    DEFINE_CGVAR_INT64(int64_1, 1);
    DEFINE_CGVAR_INT64(int64_distance1, PREFETCH_AGGHASHING_DISTANCE1);
    DEFINE_CGVAR_INT64(int64_distance2, PREFETCH_AGGHASHING_DISTANCE2);

    llvm::Value* Vals2[2] = {int64_0, int32_0};
    llvm::Value* Vals3[3] = {int64_0, int32_0, int32_0};

    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "prefetchAggSglTblHashing", voidType);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("hAggRunner", hashAggRunnerPtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("Idx", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("nrows", int64Type));
    jitted_hashing_prefetch = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* hAggRunner = llvmargs[0];
    llvm::Value* idx = llvmargs[1];
    llvm::Value* nrows = llvmargs[2];

    DEFINE_BLOCK(prefetch_hashData_bb, jitted_hashing_prefetch);
    DEFINE_BLOCK(prefetch_cell_bb, jitted_hashing_prefetch);
    DEFINE_BLOCK(ret_bb, jitted_hashing_prefetch);

    /*
     * try to prefetch hashData[m_cacheLoc[i+4]] : only do this when we have
     * enough rows.
     */
    llvm::Value* prefetchIdx = builder.CreateAdd(idx, int64_distance1);
    tmpval = builder.CreateICmpSLT(prefetchIdx, nrows);
    builder.CreateCondBr(tmpval, prefetch_hashData_bb, ret_bb);

    builder.SetInsertPoint(prefetch_hashData_bb);
    /* mask = hashAggRunner.m_hashSize - 1 */
    Vals2[1] = int32_pos_hAggR_hashSize;
    tmpval = builder.CreateInBoundsGEP(hAggRunner, Vals2);
    llvm::Value* maskval = builder.CreateLoad(int64Type, tmpval, "m_hashSize");
    maskval = builder.CreateSub(maskval, int64_1, "mask");

    /* calculate m_cacheLoc[i+4] = m_hashVal[i+4] & mask */
    llvm::Value* next_hashData_idx = builder.CreateAdd(idx, int64_distance1);
    Vals3[1] = int32_pos_hAggR_hashVal;
    Vals3[2] = next_hashData_idx;
    llvm::Value* hashValSlot = builder.CreateInBoundsGEP(hAggRunner, Vals3);
    llvm::Value* hashVal64 = builder.CreateLoad(int64Type, hashValSlot, "hashVal");
    llvm::Value* cacheLocVal = builder.CreateAnd(hashVal64, maskval);

    /* get m_hashData from hashAggRunner */
    Vals2[0] = int64_0;
    Vals2[1] = int32_pos_hAggR_hSegTbl;
    llvm::Value* hashData = builder.CreateInBoundsGEP(hAggRunner, Vals2);
    hashData = builder.CreateLoad(hashSegTblPtrType, hashData, "m_hashData");

    /* calculate address of m_hashData[0].tbl_data[m_cacheLoc[i+4]] */
    Vals2[0] = int64_0;
    Vals2[1] = int32_1;
    llvm::Value* tbldata = builder.CreateInBoundsGEP(hashData, Vals2);
    tbldata = builder.CreateLoad(hashCellPtrPtrType, tbldata, "tbl_data");
    llvm::Value* next_hashData_addr = builder.CreateInBoundsGEP(tbldata, cacheLocVal);

    llvm::Function* fn_prefetch = llvm::Intrinsic::getDeclaration(llvmCodeGen->module(), llvm_prefetch);
    if (fn_prefetch == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_LOAD_INTRINSIC_FUNCTION_FAILED),
                errmodule(MOD_LLVM),
                errmsg("Failed to get function Intrinsic::prefetch!\n")));
    }
    tmpval = builder.CreateBitCast(next_hashData_addr, int8PtrType);
    builder.CreateCall(fn_prefetch, {tmpval, int32_0, int32_3, int32_1});

    /* calculate m_cacheLoc[i+2] = m_hashVal[i+2] & mask */
    llvm::Value* next_cell_idx = builder.CreateAdd(idx, int64_distance2);
    Vals3[1] = int32_pos_hAggR_hashVal;
    Vals3[2] = next_cell_idx;
    hashValSlot = builder.CreateInBoundsGEP(hAggRunner, Vals3);
    hashVal64 = builder.CreateLoad(int64Type, hashValSlot, "hashVal");
    cacheLocVal = builder.CreateAnd(hashVal64, maskval);

    llvm::Value* next_cell = builder.CreateInBoundsGEP(tbldata, cacheLocVal);
    next_cell = builder.CreateLoad(hashCellPtrType, next_cell, "cell");

    tmpval = builder.CreatePtrToInt(next_cell, int64Type);
    tmpval = builder.CreateICmpEQ(tmpval, int64_0);
    builder.CreateCondBr(tmpval, ret_bb, prefetch_cell_bb);

    builder.SetInsertPoint(prefetch_cell_bb);
    next_cell = builder.CreateBitCast(next_cell, int8PtrType);
    builder.CreateCall(fn_prefetch, {next_cell, int32_0, int32_3, int32_1});
    builder.CreateBr(ret_bb);

    builder.SetInsertPoint(ret_bb);
    builder.CreateRetVoid();

    llvmCodeGen->FinalizeFunction(jitted_hashing_prefetch);

    return jitted_hashing_prefetch;
}
}  // namespace dorado

/**
 * @Description	: If current hashcell is not matched, allocate a new hash cell and
 *				  initialize the hash value.
 * @in haRunner	: HashAggRunner data structure.
 * @in batch	: Batch value used to initialize the hash cell.
 * @in idx		: The row index of current tuple in batch.
 * @in keysimple	: the match key is simple when true.
 */
void WrapAllocHashSlot(HashAggRunner* haRunner, VectorBatch* batch, int idx, int keysimple)
{
    if (keysimple)
        haRunner->AllocHashSlot<true, false>(batch, idx);
    else
        haRunner->AllocHashSlot<false, false>(batch, idx);
}

/**
 * @Description	: If current hashcell is not matched, allocate a new hash cell and
 *				  initialize the hash value with only one segment hash table case.
 * @in haRunner	: HashAggRunner data structure.
 * @in batch	: Batch value used to initialize the hash cell.
 * @in idx		: The row index of current tuple in batch.
 * @in keysimple	: the match key is simple when true.
 */
void WrapSglTblAllocHashSlot(HashAggRunner* haRunner, VectorBatch* batch, int idx, int keysimple)
{
    if (keysimple)
        haRunner->AllocHashSlot<true, true>(batch, idx);
    else
        haRunner->AllocHashSlot<false, true>(batch, idx);
}

/**
 * @Description	: Reset current expr context.
 * @econtext	: Current expr context.
 */
void WrapResetExprContext(ExprContext* econtext)
{
    if (econtext != NULL)
        ResetExprContext(econtext);
}
