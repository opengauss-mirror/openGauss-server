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
 *  vechashjoincodegen.cpp
 *        Routines to handle vector hashjoin nodes. We only focuse on 
 *          the CPU intensive part of hashjoin operation.
 * IDENTIFICATION
 *        src/gausskernel/runtime/codegen/vecexecutor/vechashjoincodegen.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "codegen/gscodegen.h"
#include "codegen/vechashjoincodegen.h"
#include "codegen/codegendebuger.h"
#include "codegen/builtinscodegen.h"

#include "access/transam.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "utils/lsyscache.h"
#include "vecexecutor/vecexecutor.h"
#include "pgxc/pgxc.h"

char* Wrappalloc(int32 size);
bool NULL_possible(PlanState* node);
void WrapposRepalloc(filter::BloomFilterImpl<int64>* bfi);

int64 Idx_cellCache_hashBasedOperator;
int64 Idx_keyMatch_hashBasedOperator;

namespace dorado {
Var* VecHashJoinCodeGen::GetSimpHashCondExpr(
    VecHashJoinState* node, ExprState* estate, bool* is_simple_var, int* enable_fast_keyMatch)
{
    Var* var = NULL;
    Assert(true == *is_simple_var);

    /*
     * Since we only support simple hash condition case, make sure the
     * expr is 'Var' or 'Relabel' type.
     */
    if (IsA(estate->expr, Var)) {
        var = (Var*)estate->expr;
    } else if (IsA(estate->expr, RelabelType)) {
        RelabelType* reltype = (RelabelType*)estate->expr;
        if (IsA(reltype->arg, Var) && ((Var*)reltype->arg)->varattno > 0) {
            var = (Var*)reltype->arg;
        } else {
            *is_simple_var = false;
            return var;
        }
    } else {
        *is_simple_var = false;
        return var;
    }

    /* Without considering codegen if hash key contains system columns */
    Assert(var != NULL);
    if (var->varoattno < 0) {
        *is_simple_var = false;
        return var;
    }

    /* Find the base relation according to the varnoold of var. */
    Oid relid = 0;
    int count = 0;
    Relation base_relation;
    FormData_pg_attribute* attr = NULL;
    if (*enable_fast_keyMatch) {
        if (var->varnoold > 0) {
            ListCell* lc = NULL;
            int base_pos = var->varnoold;
            foreach (lc, node->js.ps.state->es_range_table) {
                if ((++count) == base_pos) {
                    RangeTblEntry* rte = (RangeTblEntry*)lfirst(lc);
                    relid = rte->relid;
                    break;
                }
            }

            /* If not found, do not use fast match key path. */
            if (relid > 0) {
                base_relation = RelationIdGetRelation(relid);
                Oid namespaceOid = get_rel_namespace(relid);
                if (RelationIsValid(base_relation) && namespaceOid > FirstNormalObjectId) {
                    if (var->varoattno <= base_relation->rd_att->natts) {
                        attr = &base_relation->rd_att->attrs[var->varoattno - 1];
                        if (attr == NULL || !attr->attnotnull)
                            *enable_fast_keyMatch = 0;
                    } else
                        *enable_fast_keyMatch = 0;

                    RelationClose(base_relation);
                } else {
                    *enable_fast_keyMatch = 0;
                    if (RelationIsValid(base_relation))
                        RelationClose(base_relation);
                }
            } else
                *enable_fast_keyMatch = 0;
        } else
            *enable_fast_keyMatch = 0;
    }

    return var;
}

bool VecHashJoinCodeGen::JittableHashJoin(VecHashJoinState* node)
{
    int enable_fast_keyMatch = 1;
    if (!u_sess->attr.attr_sql.enable_codegen || IS_PGXC_COORDINATOR)
        return false;

    /*
     * Only support inner hash join.
     */
    if (node->js.jointype != JOIN_INNER)
        return false;

    /*
     * Set enable_fast_keyMatch to be 1 when hash keys are NOT_NULL columns
     * and their data types are integer.
     */
    ListCell* lc_inner = NULL;
    ListCell* lc_outer = NULL;
    Var* var_inner = NULL;
    Var* var_outer = NULL;
    ExprState* exprState = NULL;
    bool is_simple_var = false;
    forboth(lc_inner, node->hj_InnerHashKeys, lc_outer, node->hj_OuterHashKeys)
    {
        /*
         * Only support hash join with simple join key. Check inner hash key
         * and outer hash key separately
         */
        exprState = (ExprState*)lfirst(lc_inner);
        is_simple_var = true;
        var_inner = GetSimpHashCondExpr(node, exprState, &is_simple_var, &enable_fast_keyMatch);
        if (!is_simple_var)
            return false;

        /* Reset the flag */
        is_simple_var = true;
        exprState = (ExprState*)lfirst(lc_outer);
        var_outer = GetSimpHashCondExpr(node, exprState, &is_simple_var, &enable_fast_keyMatch);
        if (!is_simple_var)
            return false;

        /* Inner var and outer var should be the same data type */
        if (var_inner->vartype != var_outer->vartype)
            return false;

        /*
         * Only supported hash claus with int, float, numeric, bpchar type.
         */
        switch (var_inner->vartype) {
            case INT4OID:
            case INT8OID:
                break;
            case BPCHAROID: {
                enable_fast_keyMatch = 0;
                /*
                 * Make sure the inner key and the outer key has the same length.
                 */
                if (var_inner->vartypmod != var_outer->vartypmod)
                    return false;
            } break;
            default:
                return false;
        }
    }

    /*
     * If we can make sure the column of the base relation is masked as
     * not null, we could codegen the process without checking NULL.
     */
    if (enable_fast_keyMatch != 0 && !NULL_possible((PlanState*)node))
        enable_fast_keyMatch = 2;
    else if (list_length(node->hj_InnerHashKeys) == 1)
        enable_fast_keyMatch = 1;
    else
        enable_fast_keyMatch = 0;

    node->enable_fast_keyMatch = enable_fast_keyMatch;

    return true;
}

/*
 * @Description	: Check if codegeneration is allowed for buildHashTable
 *				  and probeHashTable function.
 * @Notes		: Do not support complicated hash condition and the hash
 *				  key should be int4/int8/bpchar type.
 */
bool VecHashJoinCodeGen::JittableHashJoin_buildandprobe(VecHashJoinState* node)
{
    if (!u_sess->attr.attr_sql.enable_codegen || IS_PGXC_COORDINATOR)
        return false;

    /* loop over all the hash keys */
    ListCell* lc_inner = NULL;
    ListCell* lc_outer = NULL;
    Var* var_inner = NULL;
    Var* var_outer = NULL;
    ExprState* exprState = NULL;
    forboth(lc_inner, node->hj_InnerHashKeys, lc_outer, node->hj_OuterHashKeys)
    {
        exprState = (ExprState*)lfirst(lc_inner);
        if (IsA(exprState->expr, Var))
            var_inner = (Var*)exprState->expr;
        else if (IsA(exprState->expr, RelabelType)) {
            RelabelType* reltype = (RelabelType*)exprState->expr;
            if (IsA(reltype->arg, Var) && ((Var*)reltype->arg)->varattno > 0)
                var_inner = (Var*)((RelabelType*)exprState->expr)->arg;
            else
                return false;
        } else
            return false;
        Assert(var_inner != NULL);

        /* without doing codegen for system column */
        if (var_inner->varoattno < 0)
            return false;

        exprState = (ExprState*)lfirst(lc_outer);
        if (IsA(exprState->expr, Var))
            var_outer = (Var*)exprState->expr;
        else if (IsA(exprState->expr, RelabelType)) {
            RelabelType* reltype = (RelabelType*)exprState->expr;
            if (IsA(reltype->arg, Var) && ((Var*)reltype->arg)->varattno > 0)
                var_outer = (Var*)((RelabelType*)exprState->expr)->arg;
            else
                return false;
        } else
            return false;
        Assert(var_outer != NULL);

        /* without doing codegen for system column */
        if (var_outer->varoattno < 0)
            return false;

        /* 2. Same data type */
        if (var_inner->vartype != var_outer->vartype)
            return false;

        /* 3. Supported data type */
        switch (var_inner->vartype) {
            case INT4OID:
            case INT8OID:
                break;
            case BPCHAROID: {
                /*
                 * Make sure the inner key and the outer key has the same length.
                 */
                if (var_inner->vartypmod != var_outer->vartypmod)
                    return false;
            } break;
            default:
                return false;
        }
    }

    return true;
}

bool VecHashJoinCodeGen::JittableHashJoin_bloomfilter(VecHashJoinState* node)
{
    if (!u_sess->attr.attr_sql.enable_codegen || IS_PGXC_COORDINATOR)
        return false;

    List* bfVarList = node->bf_runtime.bf_var_list;

    /* Without considering codegen when there is no bfVarList */
    if (bfVarList == NULL || list_length(bfVarList) == 0)
        return false;

    /*
     * If one of the hash key type is in (int2, int4, int8), allow codegeneration
     * in this function.
     */
    int int_count = 0;
    for (int i = 0; i < list_length(bfVarList); i++) {
        Var* var = (Var*)list_nth(bfVarList, i);
        Oid dataType = var->vartype;

        switch (dataType) {
            case INT2OID:
            case INT4OID:
            case INT8OID:
                int_count++;
                break;
            default:
                break;
        }
    }

    if (int_count == 0)
        return false;

    return true;
}

llvm::Function* VecHashJoinCodeGen::KeyMatchCodeGen(VecHashJoinState* node, Var* variable)
{
    bool nulleqnull = (node->js.nulleqqual != NIL);

    Assert(NULL != (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj);
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    /* Get LLVM Context and builder */
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    llvm::Function* jitted_keyMatch = NULL;
    llvm::Value* andTrue_result = NULL;
    llvm::Value* orFalse_result = NULL;
    llvm::PHINode* Phi_result = NULL;
    llvm::Value* llvmargs[5];

    /* Define data types and some llvm consts */
    DEFINE_CG_TYPE(int1Type, BITOID);
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(hashCellPtrType, "struct.hashCell");

    DEFINE_CGVAR_INT8(int8_0, 0);
    DEFINE_CGVAR_INT8(int8_1, 1);
    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT64(int64_0, 0);

    /* llvm array values, used to represent the location of some element */
    llvm::Value* Vals4[4] = {int64_0, int32_0, int32_0, int32_0};

    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "JittedkeyMatch", int8Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("cellCache", hashCellPtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("batch_mflag", int8Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("batch_mvals", int64Type));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("key_idx", int64Type));
    if (node->enable_fast_keyMatch == 0)
        fn_prototype.addArgument(GsCodeGen::NamedVariable("keyMatch", int8PtrType));

    jitted_keyMatch = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    /* define basic blocks */
    llvm::BasicBlock* bb_entry = &jitted_keyMatch->getEntryBlock();
    DEFINE_BLOCK(bb_checkNull, jitted_keyMatch);
    DEFINE_BLOCK(bb_andTrue, jitted_keyMatch);
    DEFINE_BLOCK(bb_andFalse, jitted_keyMatch);
    DEFINE_BLOCK(bb_orTrue, jitted_keyMatch);
    DEFINE_BLOCK(bb_orFalse, jitted_keyMatch);
    DEFINE_BLOCK(bb_ret, jitted_keyMatch);

    /* start the main codegen process for hash inner join */
    llvm::Value* mcellCache = llvmargs[0];
    llvm::Value* flag_batch = llvmargs[1];
    llvm::Value* vals_batch = llvmargs[2];
    llvm::Value* keyIdx = llvmargs[3];
    llvm::Value* mkeyMatch = NULL;
    if (node->enable_fast_keyMatch == 0) {
        mkeyMatch = llvmargs[4];
    }

    /* if node->enable_fast_keyMatch == 1, there is just one hash clause,
     * so no need to check the previous m_keyMatch[looxIdx]
     */
    if (node->enable_fast_keyMatch == 0) {
        /* get HashJoinTbl.hashBasedOperator.m_keyMatch[loop_idx] */
        llvm::Value* keyMatch_val = builder.CreateLoad(int8Type, mkeyMatch, "m_keyMatch_i");
        keyMatch_val = builder.CreateTrunc(keyMatch_val, int1Type);
        builder.CreateCondBr(keyMatch_val, bb_checkNull, bb_ret);
    } else {
        builder.CreateBr(bb_checkNull);
    }

    /* if m_keyMatch[i] = TRUE, start the keyMatching, first check the NULL flag */
    builder.SetInsertPoint(bb_checkNull);

    /* Get flag from m_cellCache[keyIdx] */
    Vals4[0] = int64_0;
    Vals4[1] = int32_1;
    Vals4[2] = keyIdx;
    Vals4[3] = int32_1;
    llvm::Value* flag_cell = builder.CreateInBoundsGEP(mcellCache, Vals4);
    flag_cell = builder.CreateLoad(int8Type, flag_cell, "flag");
    flag_cell = builder.CreateTrunc(flag_cell, int1Type);
    flag_batch = builder.CreateTrunc(flag_batch, int1Type);
    llvm::Value* and_flag = builder.CreateAnd(flag_cell, flag_batch);
    builder.CreateCondBr(and_flag, bb_andTrue, bb_andFalse);

    /* both flags are TRUE */
    builder.SetInsertPoint(bb_andTrue);
    if (nulleqnull)
        andTrue_result = int8_1;
    else
        andTrue_result = int8_0;
    builder.CreateBr(bb_ret);

    /* at least one flag is FALSE */
    builder.SetInsertPoint(bb_andFalse);
    llvm::Value* or_flag = builder.CreateOr(flag_cell, flag_batch);
    builder.CreateCondBr(or_flag, bb_orTrue, bb_orFalse);

    /* one flag is TRUE, and the other is FALSE */
    builder.SetInsertPoint(bb_orTrue);
    builder.CreateBr(bb_ret);

    /* both flags are FALSE, so compare the values from batch and cellCache */
    builder.SetInsertPoint(bb_orFalse);
    /* m_cellCache->m_val[keyIdx].val */
    Vals4[1] = int32_1;
    Vals4[2] = keyIdx;
    Vals4[3] = int32_0;
    llvm::Value* val_cell = builder.CreateInBoundsGEP(mcellCache, Vals4);
    val_cell = builder.CreateLoad(int64Type, val_cell, "val");

    /* According to the data type to do comparison */
    switch (variable->vartype) {
        case INT4OID: {
            val_cell = builder.CreateTrunc(val_cell, int32Type);
            vals_batch = builder.CreateTrunc(vals_batch, int32Type);
            orFalse_result = builder.CreateICmpEQ(val_cell, vals_batch, "key_int4eq");
        } break;
        case INT8OID:
            orFalse_result = builder.CreateICmpEQ(val_cell, vals_batch, "key_int8eq");
            break;
        case BPCHAROID: {
            DEFINE_CGVAR_INT32(int32_len, variable->vartypmod - VARHDRSZ);
            /* first extract the char* value from Datum */
            llvm::Function* evalvar = llvmCodeGen->module()->getFunction("JittedEvalVarlena");
            if (evalvar == NULL) {
                evalvar = VarlenaCvtCodeGen();
            }
            val_cell = builder.CreateCall(evalvar, val_cell, "evalbatchvar");
            val_cell = builder.CreateExtractValue(val_cell, 1);
            vals_batch = builder.CreateCall(evalvar, vals_batch, "evalbatchvar");
            vals_batch = builder.CreateExtractValue(vals_batch, 1);

            llvm::Function* func_memcmp = llvmCodeGen->module()->getFunction("LLVMIRmemcmp");
            if (func_memcmp == NULL) {
                ereport(ERROR,
                    (errcode(ERRCODE_LOAD_IR_FUNCTION_FAILED),
                        errmodule(MOD_LLVM),
                        errmsg("Failed on getting IR function : LLVMIRmemcmp!\n")));
            }
            orFalse_result = builder.CreateCall(func_memcmp, {val_cell, vals_batch, int32_len}, "memcmp");
            orFalse_result = builder.CreateTrunc(orFalse_result, int1Type);
        } break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmodule(MOD_LLVM),
                    errmsg("Codegen keyMatch failed: unsupported data type!\n")));
    }

    orFalse_result = builder.CreateZExt(orFalse_result, int8Type);
    builder.CreateBr(bb_ret);

    /* return result according to different cases */
    builder.SetInsertPoint(bb_ret);
    if (node->enable_fast_keyMatch == 0) {
        Phi_result = builder.CreatePHI(int8Type, 4);
        Phi_result->addIncoming(int8_0, bb_entry);
        Phi_result->addIncoming(andTrue_result, bb_andTrue);
        Phi_result->addIncoming(int8_0, bb_orTrue);
        Phi_result->addIncoming(orFalse_result, bb_orFalse);
        builder.CreateStore(Phi_result, mkeyMatch);
    } else {
        Phi_result = builder.CreatePHI(int8Type, 3);
        Phi_result->addIncoming(andTrue_result, bb_andTrue);
        Phi_result->addIncoming(int8_0, bb_orTrue);
        Phi_result->addIncoming(orFalse_result, bb_orFalse);
    }
    builder.CreateRet(Phi_result);

    /* finalize the jitted function */
    llvmCodeGen->FinalizeFunction(jitted_keyMatch, node->js.ps.plan->plan_node_id);

    return jitted_keyMatch;
}

/*
 * @Description	: Consider codegeneration on hot functions of hash join node,
 *				  including innerJoinT, buildHashTable, probeHashTable and
 *				  bloom filter function.
 * @Notes		: Different hot functions have different constraints.
 */
void VecHashJoinCodeGen::HashJoinCodeGen(VecHashJoinState* node)
{
    Assert(NULL != (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj);
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::Function* jitted_innerjoin = NULL;

    /*
     * only consider simple key-match cases. If the condition
     * is not satisfied, no need to codegen.
     */
    if (JittableHashJoin(node)) {
        if (node->enable_fast_keyMatch == 2)
            jitted_innerjoin = VecHashJoinCodeGen::HashJoinCodeGen_fastpath(node);
        else
            jitted_innerjoin = VecHashJoinCodeGen::HashJoinCodeGen_normal(node);

        if (jitted_innerjoin != NULL) {
            llvmCodeGen->addFunctionToMCJit(jitted_innerjoin, reinterpret_cast<void**>(&(node->jitted_innerjoin)));
        }
    }

    /* buildHashTable and probeHashTable should use the same hash function */
    if (JittableHashJoin_buildandprobe(node)) {
        llvm::Function* jitted_buildHashTable = VecHashJoinCodeGen::HashJoinCodeGen_buildHashTable(node);
        llvm::Function* jitted_buildHashTable_NeedCopy =
            VecHashJoinCodeGen::HashJoinCodeGen_buildHashTable_NeedCopy(node);
        llvm::Function* jitted_probeHashTable = VecHashJoinCodeGen::HashJoinCodeGen_probeHashTable(node);
        if (jitted_buildHashTable != NULL && jitted_probeHashTable != NULL) {
            llvmCodeGen->addFunctionToMCJit(
                jitted_buildHashTable, reinterpret_cast<void**>(&(node->jitted_buildHashTable)));
            llvmCodeGen->addFunctionToMCJit(
                jitted_buildHashTable_NeedCopy, reinterpret_cast<void**>(&(node->jitted_buildHashTable_NeedCopy)));
            llvmCodeGen->addFunctionToMCJit(
                jitted_probeHashTable, reinterpret_cast<void**>(&(node->jitted_probeHashTable)));
        }
    }

    /*
     * Since jitted_innerjoin IR function does not support compalicate key
     * cases, we should codegen this part independently when jitted_innerjoin
     * is null.
     */
    if (jitted_innerjoin == NULL) {
        llvm::Function* vhj_hashclauses = NULL;
        if (node->js.nulleqqual == NIL) {
            vhj_hashclauses = dorado::VecExprCodeGen::QualCodeGen(node->hashclauses, (PlanState*)node);
            if (vhj_hashclauses != NULL)
                llvmCodeGen->addFunctionToMCJit(vhj_hashclauses, reinterpret_cast<void**>(&(node->jitted_hashclause)));
        }
    }

    /* check codegeneration for hash join qual */
    {
        llvm::Function* vhj_joinqual = NULL;
        vhj_joinqual = dorado::VecExprCodeGen::QualCodeGen(node->js.joinqual, (PlanState*)node);
        if (vhj_joinqual != NULL)
            llvmCodeGen->addFunctionToMCJit(vhj_joinqual, reinterpret_cast<void**>(&(node->jitted_joinqual)));
    }

    /* consider codegeneration for bloom_filter in hashjoin node */
    if (JittableHashJoin_bloomfilter(node)) {
        llvm::Function* jitted_bf_addLong = dorado::VecHashJoinCodeGen::HashJoinCodeGen_bf_addLong(node);
        if (jitted_bf_addLong != NULL)
            llvmCodeGen->addFunctionToMCJit(
                jitted_bf_addLong, reinterpret_cast<void**>(&(node->jitted_hashjoin_bfaddLong)));

        llvm::Function* jitted_bf_incLong = dorado::VecHashJoinCodeGen::HashJoinCodeGen_bf_includeLong(node);
        if (jitted_bf_incLong != NULL)
            llvmCodeGen->addFunctionToMCJit(
                jitted_bf_incLong, reinterpret_cast<void**>(&(node->jitted_hashjoin_bfincLong)));
    }
}

llvm::Function* VecHashJoinCodeGen::HashJoinCodeGen_normal(VecHashJoinState* node)
{
    Assert(NULL != (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj);
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    /* Find and load the IR file from the installaion directory */
    llvmCodeGen->loadIRFile();

    /* Get LLVM Context and builder */
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    llvm::Value* hashJoinTbl = NULL;
    llvm::Value* batch = NULL;
    llvm::Function* jitted_hashjoin = NULL;
    llvm::Value* tmpval = NULL;
    llvm::Value* llvmargs[2];

    int mcols, j;
    llvm::Value* val = NULL;
    llvm::Value* flag = NULL;
    llvm::Value* mvals = NULL;
    llvm::Value* mflag = NULL;

    /* Define data types and some llvm consts */
    DEFINE_CG_TYPE(int1Type, BITOID);
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(int64PtrType, INT8OID);
    DEFINE_CG_PTRTYPE(vecBatchPtrType, "class.VectorBatch");
    DEFINE_CG_PTRTYPE(hashJoinTblPtrType, "class.HashJoinTbl");
    DEFINE_CG_PTRTYPE(hashCellPtrType, "struct.hashCell");
    DEFINE_CG_PTRPTRTYPE(hashCellPtrPtrType, "struct.hashCell");
    DEFINE_CG_PTRTYPE(scalarVecPtrType, "class.ScalarVector");
    
    DEFINE_CGVAR_INT8(int8_0, 0);
    DEFINE_CGVAR_INT8(int8_1, 1);
    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT32(int32_2, 2);
    DEFINE_CGVAR_INT32(int32_BatchMaxSize, BatchMaxSize);
    DEFINE_CGVAR_INT32(int32_pos_hjointbl_StateLog, pos_hjointbl_StateLog);
    DEFINE_CGVAR_INT32(int32_pos_batch_marr, pos_batch_marr);
    DEFINE_CGVAR_INT32(int32_pos_hjointbl_inbatch, pos_hjointbl_inbatch);
    DEFINE_CGVAR_INT32(int32_pos_hjointbl_outbatch, pos_hjointbl_outbatch);
    DEFINE_CGVAR_INT32(int32_pos_scalvec_vals, pos_scalvec_vals);
    DEFINE_CGVAR_INT32(int32_pos_scalvec_flag, pos_scalvec_flag);
    DEFINE_CGVAR_INT64(int64_0, 0);
    DEFINE_CGVAR_INT64(int64_1, 1);
    DEFINE_CGVAR_INT64(int64_8032, 8032);
    DEFINE_CGVAR_INT64(int64_8, 8);
    DEFINE_CGVAR_INT32(int32_6, 6);

    /* llvm array values, used to represent the location of some element */
    llvm::Value* Vals[2] = {int64_0, int32_0};
    llvm::Value* Vals3[3] = {int64_0, int32_0, int32_0};
    llvm::Value* Vals4[4] = {int64_0, int32_0, int32_0, int32_0};

    /* the prototype of the jitted function for innerHashJoinT.
     * Two parameters: (1) hashJoinTbl (this of innerHashJoinT)
     * 		   		   (2) batch of probing data
     */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "JittedHashInnerJoin", int32Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("hashJoinTbl", hashJoinTblPtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("batch", vecBatchPtrType));
    jitted_hashjoin = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    hashJoinTbl = llvmargs[0];
    batch = llvmargs[1];

    /* define basic block information for batch loop */
    llvm::BasicBlock* entry = &jitted_hashjoin->getEntryBlock();
    DEFINE_BLOCK(bb_restoreTrue, jitted_hashjoin);
    DEFINE_BLOCK(bb_restoreFalse, jitted_hashjoin);
    DEFINE_BLOCK(bb_pre_loopEntry, jitted_hashjoin);
    DEFINE_BLOCK(bb_ret, jitted_hashjoin);
    DEFINE_BLOCK(bb_loopEntry, jitted_hashjoin);
    DEFINE_BLOCK(bb_crit_edge, jitted_hashjoin);

    /* define basic block information for key-value matching */
    DEFINE_BLOCK(bb_pre_keyMatch, jitted_hashjoin);
    DEFINE_BLOCK(bb_keyMatch, jitted_hashjoin);
    DEFINE_BLOCK(bb_Matched, jitted_hashjoin);
    DEFINE_BLOCK(bb_NoMatch, jitted_hashjoin);
    DEFINE_BLOCK(bb_BatchFull, jitted_hashjoin);
    DEFINE_BLOCK(bb_TryNextCell, jitted_hashjoin);

    /* start the main codegen process for hash inner join */
    /* get the number of rows of this batch */
    tmpval = builder.CreateInBoundsGEP(batch, Vals);
    llvm::Value* nRows = builder.CreateLoad(int32Type, tmpval, "m_rows");

    /* get HashJoinTbl.m_joinStateLog.lastBuildIdx */
    Vals3[0] = int64_0;
    Vals3[1] = int32_pos_hjointbl_StateLog;
    Vals3[2] = int32_0;
    llvm::Value* lastBuildIdx_addr = builder.CreateInBoundsGEP(hashJoinTbl, Vals3);

    /* get HashJoinTbl.m_joinStateLog.lastCell */
    Vals3[2] = int32_1;
    llvm::Value* lastCell_addr = builder.CreateInBoundsGEP(hashJoinTbl, Vals3);

    /* get HashJoinTbl.m_joinStateLog.restore */
    Vals3[2] = int32_2;
    llvm::Value* joinStateLog_restore = builder.CreateInBoundsGEP(hashJoinTbl, Vals3);
    tmpval = builder.CreateLoad(int8Type, joinStateLog_restore, "restore");
    tmpval = builder.CreateICmpEQ(tmpval, int8_0);
    builder.CreateCondBr(tmpval, bb_restoreFalse, bb_restoreTrue);

    /* if restore == true, get the loop start index and cellCache from
     * m_joinStateLog, and reset the restore to false.
     * bb_restoreTrue:
     */
    builder.SetInsertPoint(bb_restoreTrue);
    /* corresponding to : m_joinStateLog.restore = false; */
    builder.CreateStore(int8_0, joinStateLog_restore);
    /* get m_joinStateLog.lastBuildIdx; */
    llvm::Value* lastBuildIdx = builder.CreateLoad(int32Type, lastBuildIdx_addr, "lastBuildIdx");
    /* get m_joinStateLog.lastCell */
    llvm::Value* lastCell = builder.CreateLoad(hashCellPtrType, lastCell_addr, "lastCell");

    /* get HashJoinTbl.hashBasedOperator.m_cellCache[lastBuildIdx] */
    tmpval = builder.CreatePtrToInt(hashJoinTbl, int64Type);
    tmpval = builder.CreateAdd(tmpval, int64_8032);
    llvm::Value* tmpval2 = builder.CreateSExt(lastBuildIdx, int64Type);
    tmpval2 = builder.CreateMul(tmpval2, int64_8);
    tmpval = builder.CreateAdd(tmpval, tmpval2);
    llvm::Value* mcellCache = builder.CreateIntToPtr(tmpval, hashCellPtrPtrType);

    builder.CreateStore(lastCell, mcellCache);
    builder.CreateBr(bb_restoreFalse);

    /* restore is false, so start the loop from 0
     *	bb_restoreFalse:
     */
    builder.SetInsertPoint(bb_restoreFalse);
    llvm::PHINode* Phi = builder.CreatePHI(int32Type, 2);
    Phi->addIncoming(int32_0, entry);
    Phi->addIncoming(lastBuildIdx, bb_restoreTrue);
    tmpval = builder.CreateICmpSLT(Phi, nRows);
    builder.CreateCondBr(tmpval, bb_pre_loopEntry, bb_ret);

    /*
     * bb_pre_loopEntry:
     * br label %bb_loopEntry
     * This basic block is the one prior to the batch loop.
     * Load all necessary ScalarVectors (batch loop constants) here.
     * innerBatch, outerBatch, inner_arr, outer_arr, inner_vals/inner_flag,
     * outer_vals/outer_flag, batch_vals/batch_flag, ...
     */
    builder.SetInsertPoint(bb_pre_loopEntry);

    /* get m_arr from batch */
    Vals[1] = int32_pos_batch_marr;
    llvm::Value* arr = builder.CreateInBoundsGEP(batch, Vals);
    llvm::Value* batch_arr = builder.CreateLoad(scalarVecPtrType, arr, "batch_arr");
    /* HashJoinTbl.m_innerBatch */
    Vals[1] = int32_pos_hjointbl_inbatch;
    llvm::Value* innerBatch = builder.CreateInBoundsGEP(hashJoinTbl, Vals);
    /* HashJoinTbl.m_outerBatch */
    Vals[1] = int32_pos_hjointbl_outbatch;
    llvm::Value* outerBatch = builder.CreateInBoundsGEP(hashJoinTbl, Vals);
    innerBatch = builder.CreateLoad(vecBatchPtrType, innerBatch, "innerBatch");
    outerBatch = builder.CreateLoad(vecBatchPtrType, outerBatch, "outerBatch");

    /* Get m_arr from innerBatch and outerBatch */
    Vals[1] = int32_pos_batch_marr;
    llvm::Value* inner_arr = builder.CreateInBoundsGEP(innerBatch, Vals);
    inner_arr = builder.CreateLoad(scalarVecPtrType, inner_arr, "inner_arr");
    llvm::Value* outer_arr = builder.CreateInBoundsGEP(outerBatch, Vals);
    outer_arr = builder.CreateLoad(scalarVecPtrType, outer_arr, "outer_arr");

    /* Extract vals and flags from m_innerBatch */
    mcols = innerPlanState(node)->ps_ResultTupleSlot->tts_tupleDescriptor->natts;
    llvm::Value** inner_vals_array = (llvm::Value**)palloc(sizeof(llvm::Value*) * mcols);
    llvm::Value** inner_flag_array = (llvm::Value**)palloc(sizeof(llvm::Value*) * mcols);
    for (j = 0; j < mcols; j++) {
        DEFINE_CGVAR_INT64(int64_jIdx, (long long)j);
        /* Get m_vals from pVector */
        Vals[0] = int64_jIdx;
        Vals[1] = int32_pos_scalvec_vals;
        tmpval = builder.CreateInBoundsGEP(inner_arr, Vals);
        tmpval = builder.CreateLoad(int64PtrType, tmpval, "inner_vals");
        inner_vals_array[j] = tmpval;
        /* Get m_flag from pVector */
        Vals[1] = int32_pos_scalvec_flag;
        tmpval = builder.CreateInBoundsGEP(inner_arr, Vals);
        tmpval = builder.CreateLoad(int8PtrType, tmpval, "inner_flag");
        inner_flag_array[j] = tmpval;
    }

    /* Extract vals and flags from m_outerBatch */
    mcols = outerPlanState(node)->ps_ResultTupleSlot->tts_tupleDescriptor->natts;
    llvm::Value** outer_vals_array = (llvm::Value**)palloc(sizeof(llvm::Value*) * mcols);
    llvm::Value** outer_flag_array = (llvm::Value**)palloc(sizeof(llvm::Value*) * mcols);
    llvm::Value** batch_vals_array = (llvm::Value**)palloc(sizeof(llvm::Value*) * mcols);
    llvm::Value** batch_flag_array = (llvm::Value**)palloc(sizeof(llvm::Value*) * mcols);
    for (j = 0; j < mcols; j++) {
        DEFINE_CGVAR_INT64(int64_jIdx, (long long)j);
        Vals[0] = int64_jIdx;
        /* Get m_vals from pVector */
        Vals[1] = int32_pos_scalvec_vals;
        tmpval = builder.CreateInBoundsGEP(outer_arr, Vals);
        tmpval = builder.CreateLoad(int64PtrType, tmpval, "outer_vals");
        outer_vals_array[j] = tmpval;

        tmpval = builder.CreateInBoundsGEP(batch_arr, Vals);
        tmpval = builder.CreateLoad(int64PtrType, tmpval, "batch_val");
        batch_vals_array[j] = tmpval;

        /* Get m_flag from pVector */
        Vals[1] = int32_pos_scalvec_flag;
        tmpval = builder.CreateInBoundsGEP(outer_arr, Vals);
        tmpval = builder.CreateLoad(int8PtrType, tmpval, "outer_flag");
        outer_flag_array[j] = tmpval;

        tmpval = builder.CreateInBoundsGEP(batch_arr, Vals);
        tmpval = builder.CreateLoad(int8PtrType, tmpval, "batch_flag");
        batch_flag_array[j] = tmpval;
    }

    ListCell* lc_inner = NULL;
    ListCell* lc_outer = NULL;
    int keyIdx_var = 0;
    int outkeyIdx_var = 0;
    int num_hashkey = list_length(node->hj_InnerHashKeys);
    Var* variable = NULL;
    ExprState* exprState = NULL;
    llvm::Value** outer_keys_flag_array = (llvm::Value**)palloc(sizeof(llvm::Value*) * num_hashkey);
    llvm::Value** outer_keys_vals_array = (llvm::Value**)palloc(sizeof(llvm::Value*) * num_hashkey);
    llvm::Value** inner_keyIdx_array = (llvm::Value**)palloc(sizeof(llvm::Value*) * num_hashkey);
    Var** inner_var = (Var**)palloc(sizeof(Var*) * num_hashkey);
    j = 0;
    forboth(lc_inner, node->hj_InnerHashKeys, lc_outer, node->hj_OuterHashKeys)
    {
        /* get inner keyIdx */
        variable = NULL;
        exprState = (ExprState*)lfirst(lc_inner);
        if (IsA(exprState->expr, Var))
            variable = (Var*)exprState->expr;
        else if (IsA(exprState->expr, RelabelType)) {
            RelabelType* reltype = (RelabelType*)exprState->expr;
            if (IsA(reltype->arg, Var) && ((Var*)reltype->arg)->varattno > 0)
                variable = (Var*)((RelabelType*)exprState->expr)->arg;
        }

        if (variable == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmodule(MOD_LLVM), errmsg("Failed to get inner hash key!")));
        }
        keyIdx_var = variable->varattno - 1;
        inner_var[j] = variable;

        /* get outer keyIdx */
        variable = NULL;
        exprState = (ExprState*)lfirst(lc_outer);
        if (IsA(exprState->expr, Var))
            variable = (Var*)exprState->expr;
        else if (IsA(exprState->expr, RelabelType)) {
            RelabelType* reltype = (RelabelType*)exprState->expr;
            if (IsA(reltype->arg, Var) && ((Var*)reltype->arg)->varattno > 0)
                variable = (Var*)((RelabelType*)exprState->expr)->arg;
        }

        if (variable == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmodule(MOD_LLVM), errmsg("Failed to get outer hash key!")));
        }
        outkeyIdx_var = variable->varattno - 1;

        /* get the ScalarVector = m_arr[outkeyIdx_var] */
        DEFINE_CGVAR_INT64(int64_outkeyIdx, (long long)outkeyIdx_var);
        DEFINE_CGVAR_INT64(int64_keyIdx, (long long)keyIdx_var);
        tmpval = builder.CreateInBoundsGEP(batch_arr, int64_outkeyIdx);  // address of ScalarVector
        Vals[0] = int64_0;
        Vals[1] = int32_pos_scalvec_vals;
        tmpval = builder.CreateInBoundsGEP(tmpval, Vals);
        tmpval = builder.CreateLoad(int64PtrType, tmpval, "outer_key_vals");
        outer_keys_vals_array[j] = tmpval;

        Vals[1] = int32_pos_scalvec_flag;
        tmpval = builder.CreateInBoundsGEP(batch_arr, int64_outkeyIdx);  // address of ScalarVector
        tmpval = builder.CreateInBoundsGEP(tmpval, Vals);
        tmpval = builder.CreateLoad(int8PtrType, tmpval, "outer_key_flag");
        outer_keys_flag_array[j] = tmpval;

        inner_keyIdx_array[j] = int64_keyIdx;
        j++;
    }

    llvm::Value* loopIdx1 = builder.CreateSExt(Phi, int64Type);
    builder.CreateBr(bb_loopEntry);

    /* bb_loopEntry */
    /* loop entry of the batch loop */
    builder.SetInsertPoint(bb_loopEntry);
    llvm::PHINode* Phi_Idx = builder.CreatePHI(int64Type, 2);
    Phi_Idx->addIncoming(loopIdx1, bb_pre_loopEntry);
    llvm::Value* loopIdx2 = NULL;
    llvm::PHINode* Phi_resultRow_06 = builder.CreatePHI(int32Type, 2);  // resultRow: number of result rows
    Phi_resultRow_06->addIncoming(int32_0, bb_pre_loopEntry);
    llvm::PHINode* Phi_resultRow_01 = NULL;

    /* get HashJoinTbl.hashBasedOperator.m_cellCache */
    tmpval = builder.CreatePtrToInt(hashJoinTbl, int64Type);
    tmpval = builder.CreateAdd(tmpval, int64_8032);
    tmpval2 = builder.CreateMul(Phi_Idx, int64_8);
    tmpval = builder.CreateAdd(tmpval, tmpval2);
    mcellCache = builder.CreateIntToPtr(tmpval, hashCellPtrPtrType);
    llvm::Value* cellCache_loopEntry = builder.CreateLoad(hashCellPtrType, mcellCache, "cellCache");
    tmpval = builder.CreatePtrToInt(cellCache_loopEntry, int64Type);
    tmpval = builder.CreateICmpEQ(tmpval, int64_0);
    builder.CreateCondBr(tmpval, bb_crit_edge, bb_pre_keyMatch);

    /*
     * bb_pre_keyMatch:    ; preds = %bb_loopEntry
     */
    llvm::Value* flag_batch = NULL;
    llvm::Value* vals_batch = NULL;

    builder.SetInsertPoint(bb_pre_keyMatch);
    /* hashJoinTbl.hashBasedOperator.m_keyMatch[Phi_idx] */
    llvm::Value* mkeyMatch = NULL;
    if (node->enable_fast_keyMatch == 0) {
        Vals4[0] = int64_0;
        Vals4[1] = int32_0;
        Vals4[2] = int32_6;
        Vals4[3] = Phi_Idx;
        mkeyMatch = builder.CreateInBoundsGEP(hashJoinTbl, Vals4);
    } else {
        /* just one hash clause */
        tmpval = builder.CreateInBoundsGEP(outer_keys_flag_array[0], Phi_Idx);
        flag_batch = builder.CreateLoad(int8Type, tmpval, "flag_outer_key");
        tmpval = builder.CreateInBoundsGEP(outer_keys_vals_array[0], Phi_Idx);
        vals_batch = builder.CreateLoad(int64Type, tmpval, "vals_outer_key");
    }
    builder.CreateBr(bb_keyMatch);

    /*
     * bb_keyMatch:    ; preds = %bb_pre_keyMatch, %TryNextCell
     */
    builder.SetInsertPoint(bb_keyMatch);
    llvm::PHINode* Phi_resultRow_keyMatch = builder.CreatePHI(int32Type, 2);
    Phi_resultRow_keyMatch->addIncoming(Phi_resultRow_06, bb_pre_keyMatch);
    llvm::PHINode* Phi_resultRow_02 = NULL;
    llvm::PHINode* Phi_cellCache_keyMatch = NULL;
    Phi_cellCache_keyMatch = builder.CreatePHI(hashCellPtrType, 2);
    Phi_cellCache_keyMatch->addIncoming(cellCache_loopEntry, bb_pre_keyMatch);

    /* if there are multiple hash clauses, initialize m_keyMatch[i] to true
     * and the match key result will be saved in m_keyMatch[i]
     */
    if (node->enable_fast_keyMatch == 0) {
        builder.CreateStore(int8_1, mkeyMatch);
    }
    /* for each key, call the matchKey function
     * matchKey(HashJoinTbl *this, ScalarVector *col, int64 loopIdx, int64 keyIdx)
     */
    llvm::Value* keyMatch_result = NULL;
    llvm::Function* jitted_matchKey = NULL;
    for (j = 0; j < num_hashkey; j++) {
        jitted_matchKey = VecHashJoinCodeGen::KeyMatchCodeGen(node, inner_var[j]);
        if (jitted_matchKey == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                    errmodule(MOD_LLVM),
                    errmsg("Codegen keyMatch failed for %d-th clause of hash join!\n", j)));
        }

        /* Have multiple hash clauses */
        if (node->enable_fast_keyMatch == 0) {
            tmpval = builder.CreateInBoundsGEP(outer_keys_flag_array[j], Phi_Idx);
            flag_batch = builder.CreateLoad(int8Type, tmpval, "flag_outer_key");
            tmpval = builder.CreateInBoundsGEP(outer_keys_vals_array[j], Phi_Idx);
            vals_batch = builder.CreateLoad(int64Type, tmpval, "vals_outer_key");
        }

        if (node->enable_fast_keyMatch == 0)
            keyMatch_result = builder.CreateCall(
                jitted_matchKey, {Phi_cellCache_keyMatch, flag_batch, vals_batch, inner_keyIdx_array[j], mkeyMatch});
        else
            keyMatch_result = builder.CreateCall(
                jitted_matchKey, {Phi_cellCache_keyMatch, flag_batch, vals_batch, inner_keyIdx_array[j]});
    }

    if (node->enable_fast_keyMatch == 0) {
        tmpval = builder.CreateLoad(int8Type, mkeyMatch, "keyMatch");
        tmpval = builder.CreateICmpEQ(tmpval, int8_0);
        builder.CreateCondBr(tmpval, bb_NoMatch, bb_Matched);
    } else {
        /* when there is only one hash clause */
        keyMatch_result = builder.CreateTrunc(keyMatch_result, int1Type);
        builder.CreateCondBr(keyMatch_result, bb_Matched, bb_NoMatch);
    }

    builder.SetInsertPoint(bb_Matched);
    llvm::Value* cellCache = Phi_cellCache_keyMatch;

    mcols = innerPlanState(node)->ps_ResultTupleSlot->tts_tupleDescriptor->natts;
    llvm::Value* resultRow = builder.CreateSExt(Phi_resultRow_keyMatch, int64Type);

    /* copy data from hash cell to innerBatch */
    llvm::Value* resultRow_i64 = builder.CreateSExt(resultRow, int64Type);
    for (j = 0; j < mcols; j++) {
        mvals = builder.CreateInBoundsGEP(inner_vals_array[j], resultRow_i64);
        mflag = builder.CreateInBoundsGEP(inner_flag_array[j], resultRow_i64);

        /* get hashVal.val from cellCache */
        DEFINE_CGVAR_INT64(int64_jIdx, (long long)j);
        Vals4[1] = int32_1;
        Vals4[2] = int64_jIdx;
        Vals4[3] = int32_0;
        val = builder.CreateInBoundsGEP(cellCache, Vals4);
        val = builder.CreateLoad(int64Type, val, "cell_val");
        /* get flag from cellCache */
        Vals4[3] = int32_1;
        flag = builder.CreateInBoundsGEP(cellCache, Vals4);
        flag = builder.CreateLoad(int8Type, flag, "cell_flag");

        /* Assign the cell val to mvals and cell flag to mflag */
        builder.CreateStore(val, mvals);
        builder.CreateStore(flag, mflag);
    }

    /* copy data from batch to outerBatch */
    mcols = outerPlanState(node)->ps_ResultTupleSlot->tts_tupleDescriptor->natts;
    for (j = 0; j < mcols; j++) {
        /* Get the address from outerBatch*/
        mvals = builder.CreateInBoundsGEP(outer_vals_array[j], resultRow_i64);
        mflag = builder.CreateInBoundsGEP(outer_flag_array[j], resultRow_i64);

        /* Get the val and flag from batch */
        val = builder.CreateInBoundsGEP(batch_vals_array[j], Phi_Idx);
        val = builder.CreateLoad(int64Type, val, "batch_val");
        flag = builder.CreateInBoundsGEP(batch_flag_array[j], Phi_Idx);
        flag = builder.CreateLoad(int8Type, flag, "batch_flag");

        /* Assign the cell val to mvals and cell flag to mflag */
        builder.CreateStore(val, mvals);
        builder.CreateStore(flag, mflag);
    }

    /* resultRow++ */
    llvm::Value* resultRow_Matched = builder.CreateAdd(Phi_resultRow_keyMatch, int32_1);
    tmpval = builder.CreateICmpEQ(resultRow_Matched, int32_BatchMaxSize);
    builder.CreateCondBr(tmpval, bb_BatchFull, bb_TryNextCell);

    builder.SetInsertPoint(bb_NoMatch);
    builder.CreateBr(bb_TryNextCell);

    builder.SetInsertPoint(bb_BatchFull);
    llvm::Value* idx_int32 = builder.CreateTrunc(Phi_Idx, int32Type);
    builder.CreateStore(idx_int32, lastBuildIdx_addr);

    // get cellCache.flag.m_next
    Vals3[0] = int64_0;
    Vals3[1] = int32_0;
    Vals3[2] = int32_0;
    tmpval = builder.CreateInBoundsGEP(cellCache, Vals3);
    llvm::Value* nextCell = builder.CreateLoad(hashCellPtrType, tmpval, "nextCell");
    builder.CreateStore(nextCell, lastCell_addr);
    builder.CreateStore(int8_1, joinStateLog_restore);
    builder.CreateBr(bb_ret);

    /* try next cell of m_cellCache[i] and update m_cellCache[i] if next cell exists */
    builder.SetInsertPoint(bb_TryNextCell);
    Phi_resultRow_02 = builder.CreatePHI(int32Type, 2);
    Phi_resultRow_02->addIncoming(resultRow_Matched, bb_Matched);
    Phi_resultRow_02->addIncoming(Phi_resultRow_keyMatch, bb_NoMatch);

    /* Phi_resultRow_keyMatch- is in bb_keyMatch */
    Phi_resultRow_keyMatch->addIncoming(Phi_resultRow_02, bb_TryNextCell);

    /* m_cellCache[i]->flag.m_next */
    Vals3[1] = int32_0;
    Vals3[2] = int32_0;
    tmpval = builder.CreateInBoundsGEP(cellCache, Vals3);
    nextCell = builder.CreateLoad(hashCellPtrType, tmpval, "nextCell");
    Phi_cellCache_keyMatch->addIncoming(nextCell, bb_TryNextCell);
    tmpval = builder.CreatePtrToInt(nextCell, int64Type);
    tmpval = builder.CreateICmpEQ(tmpval, int64_0);
    builder.CreateCondBr(tmpval, bb_crit_edge, bb_keyMatch);

    builder.SetInsertPoint(bb_crit_edge);
    Phi_resultRow_01 = builder.CreatePHI(int32Type, 2);
    Phi_resultRow_01->addIncoming(Phi_resultRow_06, bb_loopEntry);
    Phi_resultRow_01->addIncoming(Phi_resultRow_02, bb_TryNextCell);
    // Phi_resultRow_06- is in bb_loopEntry
    Phi_resultRow_06->addIncoming(Phi_resultRow_01, bb_crit_edge);
    loopIdx2 = builder.CreateAdd(Phi_Idx, int64_1);
    Phi_Idx->addIncoming(loopIdx2, bb_crit_edge);
    tmpval = builder.CreateTrunc(loopIdx2, int32Type);
    tmpval = builder.CreateICmpSLT(tmpval, nRows);
    builder.CreateCondBr(tmpval, bb_loopEntry, bb_ret);

    /* return resultRow */
    builder.SetInsertPoint(bb_ret);
    llvm::PHINode* Phi_resultRow = builder.CreatePHI(int32Type, 3);
    Phi_resultRow->addIncoming(int32_BatchMaxSize, bb_BatchFull);
    llvm::Value* resultRow_crit_edge = builder.CreateTrunc(Phi_resultRow_01, int32Type);
    Phi_resultRow->addIncoming(resultRow_crit_edge, bb_crit_edge);
    Phi_resultRow->addIncoming(int32_0, bb_restoreFalse);
    builder.CreateRet(Phi_resultRow);

    /* free all temperatory memory */
    pfree_ext(inner_vals_array);
    pfree_ext(inner_flag_array);
    pfree_ext(outer_vals_array);
    pfree_ext(outer_flag_array);
    pfree_ext(batch_vals_array);
    pfree_ext(batch_flag_array);
    pfree_ext(outer_keys_flag_array);
    pfree_ext(outer_keys_vals_array);
    pfree_ext(inner_keyIdx_array);
    pfree_ext(inner_var);

    /* finalize the jitted function */
    llvmCodeGen->FinalizeFunction(jitted_hashjoin, node->js.ps.plan->plan_node_id);

    return jitted_hashjoin;
}

/* Codegen fast path of innerJoinT with inlined match key function.
 * Condition: (1) All hash keys are NOT NULL columns from base tables
 *            (2) All hash key data types are int4 or int8
 */
llvm::Function* VecHashJoinCodeGen::HashJoinCodeGen_fastpath(VecHashJoinState* node)
{
    Assert(NULL != (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj);
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    /* Find and load the IR file from the installaion directory */
    llvmCodeGen->loadIRFile();

    /* Get LLVM Context and builder */
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    llvm::Value* hashJoinTbl = NULL;
    llvm::Value* batch = NULL;
    llvm::Function* jitted_hashjoin = NULL;
    llvm::Value* tmpval = NULL;
    llvm::Value* llvmargs[2];

    int mcols, j;
    llvm::Value* val = NULL;
    llvm::Value* flag = NULL;
    llvm::Value* mvals = NULL;
    llvm::Value* mflag = NULL;

    /* Define data types and some llvm consts */
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(int64PtrType, INT8OID);
    DEFINE_CG_PTRTYPE(vecBatchPtrType, "class.VectorBatch");
    DEFINE_CG_PTRTYPE(HashJoinTblPtrType, "class.HashJoinTbl");
    DEFINE_CG_PTRTYPE(hashCellPtrType, "struct.hashCell");
    DEFINE_CG_PTRTYPE(scalarVecPtrType, "class.ScalarVector");
    
    DEFINE_CGVAR_INT8(int8_0, 0);
    DEFINE_CGVAR_INT8(int8_1, 1);
    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT32(int32_2, 2);
    DEFINE_CGVAR_INT64(int64_0, 0);
    DEFINE_CGVAR_INT64(int64_1, 1);
    DEFINE_CGVAR_INT32(int32_BatchMaxSize, BatchMaxSize);
    DEFINE_CGVAR_INT32(int32_pos_hjointbl_StateLog, pos_hjointbl_StateLog);
    DEFINE_CGVAR_INT32(int32_pos_batch_marr, pos_batch_marr);
    DEFINE_CGVAR_INT32(int32_pos_hjointbl_inbatch, pos_hjointbl_inbatch);
    DEFINE_CGVAR_INT32(int32_pos_hjointbl_outbatch, pos_hjointbl_outbatch);
    DEFINE_CGVAR_INT32(int32_pos_scalvec_vals, pos_scalvec_vals);
    DEFINE_CGVAR_INT32(int32_pos_scalvec_flag, pos_scalvec_flag);

    /* llvm array values, used to represent the location of some element */
    llvm::Value* Vals[2] = {int64_0, int32_0};
    llvm::Value* Vals3[3] = {int64_0, int32_0, int32_0};
    llvm::Value* Vals4[4] = {int64_0, int32_0, int32_0, int32_0};

    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "JittedHashInnerJoin_fastpath", int32Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("hashJoinTbl", HashJoinTblPtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("batch", vecBatchPtrType));
    jitted_hashjoin = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    /* start the main codegen process for hash inner join */
    hashJoinTbl = llvmargs[0];
    batch = llvmargs[1];

    /* define basic block information for batch loop */
    llvm::BasicBlock* entry = &jitted_hashjoin->getEntryBlock();
    DEFINE_BLOCK(bb_restoreTrue, jitted_hashjoin);
    DEFINE_BLOCK(bb_restoreFalse, jitted_hashjoin);
    DEFINE_BLOCK(bb_pre_loopEntry, jitted_hashjoin);
    DEFINE_BLOCK(bb_ret, jitted_hashjoin);
    DEFINE_BLOCK(bb_loopEntry, jitted_hashjoin);
    DEFINE_BLOCK(bb_crit_edge, jitted_hashjoin);

    /* define basic block information for key-value matching */
    DEFINE_BLOCK(bb_keyMatch, jitted_hashjoin);
    DEFINE_BLOCK(bb_Matched, jitted_hashjoin);
    DEFINE_BLOCK(bb_NoMatch, jitted_hashjoin);
    DEFINE_BLOCK(bb_BatchFull, jitted_hashjoin);
    DEFINE_BLOCK(bb_TryNextCell, jitted_hashjoin);

    /* entry basic block */
    /* get the number of rows of this batch */
    tmpval = builder.CreateInBoundsGEP(batch, Vals);
    llvm::Value* nRows = builder.CreateLoad(int32Type, tmpval, "m_rows");

    /* get HashJoinTbl.m_joinStateLog.lastBuildIdx */
    Vals3[0] = int64_0;
    Vals3[1] = int32_pos_hjointbl_StateLog;
    Vals3[2] = int32_0;
    llvm::Value* lastBuildIdx_addr = builder.CreateInBoundsGEP(hashJoinTbl, Vals3);

    /* get HashJoinTbl.m_joinStateLog.lastCell */
    Vals3[2] = int32_1;
    llvm::Value* lastCell_addr = builder.CreateInBoundsGEP(hashJoinTbl, Vals3);

    /* get HashJoinTbl.m_joinStateLog.restore */
    Vals3[2] = int32_2;
    llvm::Value* joinStateLog_restore = builder.CreateInBoundsGEP(hashJoinTbl, Vals3);
    tmpval = builder.CreateLoad(int8Type, joinStateLog_restore, "restore");
    tmpval = builder.CreateICmpEQ(tmpval, int8_0);
    builder.CreateCondBr(tmpval, bb_restoreFalse, bb_restoreTrue);

    /* if restore == true, get the loop start index and cellCache from
     * m_joinStateLog, and reset the restore to false.
     */
    builder.SetInsertPoint(bb_restoreTrue);
    builder.CreateStore(int8_0, joinStateLog_restore);
    /* get lastBuildIdx from m_joinStateLog */
    llvm::Value* lastBuildIdx = builder.CreateLoad(int32Type, lastBuildIdx_addr, "lastBuildIdx");
    /* get m_joinStateLog.lastCell */
    llvm::Value* lastCell = builder.CreateLoad(hashCellPtrType, lastCell_addr, "lastCell");

    /* get hashJoinTbl.vechashtable.m_cellCache[lastBuildIdx] */
    DEFINE_CGVAR_INT64(int64_8032, 8032);
    DEFINE_CGVAR_INT64(int64_8, 8);
    tmpval = builder.CreatePtrToInt(hashJoinTbl, int64Type);
    tmpval = builder.CreateAdd(tmpval, int64_8032);
    llvm::Value* tmpval2 = builder.CreateSExt(lastBuildIdx, int64Type);
    tmpval2 = builder.CreateMul(tmpval2, int64_8);
    tmpval = builder.CreateAdd(tmpval, tmpval2);
    DEFINE_CG_PTRTYPE(hashCellPPtrType, hashCellPtrType);
    llvm::Value* mcellCache = builder.CreateIntToPtr(tmpval, hashCellPPtrType);

    builder.CreateStore(lastCell, mcellCache);
    builder.CreateBr(bb_restoreFalse);

    /* restore is false, so start the loop from 0
     * bb_restoreFalse:
     */
    builder.SetInsertPoint(bb_restoreFalse);
    llvm::PHINode* Phi = builder.CreatePHI(int32Type, 2);
    Phi->addIncoming(int32_0, entry);
    Phi->addIncoming(lastBuildIdx, bb_restoreTrue);
    tmpval = builder.CreateICmpSLT(Phi, nRows);
    builder.CreateCondBr(tmpval, bb_pre_loopEntry, bb_ret);

    /*
     * bb_pre_loopEntry:
     * br label %bb_loopEntry
     * This basic block is the one prior to the batch loop.
     * Load all necessary ScalarVectors (batch loop constants) here.
     * innerBatch, outerBatch, inner_arr, outer_arr, inner_vals/inner_flag,
     * outer_vals/outer_flag, batch_vals/batch_flag, ...
     */
    builder.SetInsertPoint(bb_pre_loopEntry);

    /* Get m_arr from batch */
    Vals[1] = int32_pos_batch_marr;
    llvm::Value* arr = builder.CreateInBoundsGEP(batch, Vals);
    llvm::Value* batch_arr = builder.CreateLoad(scalarVecPtrType, arr, "batch_arr");
    /* hashJoinTbl.m_innerBatch */
    Vals[1] = int32_pos_hjointbl_inbatch;
    llvm::Value* innerBatch = builder.CreateInBoundsGEP(hashJoinTbl, Vals);
    /* hashJoinTbl.m_outerBatch */
    Vals[1] = int32_pos_hjointbl_outbatch;
    llvm::Value* outerBatch = builder.CreateInBoundsGEP(hashJoinTbl, Vals);
    innerBatch = builder.CreateLoad(vecBatchPtrType, innerBatch, "innerBatch");
    outerBatch = builder.CreateLoad(vecBatchPtrType, outerBatch, "outerBatch");
    Vals[1] = int32_pos_batch_marr;
    llvm::Value* inner_arr = builder.CreateInBoundsGEP(innerBatch, Vals);
    inner_arr = builder.CreateLoad(scalarVecPtrType, inner_arr, "inner_arr");
    llvm::Value* outer_arr = builder.CreateInBoundsGEP(outerBatch, Vals);
    outer_arr = builder.CreateLoad(scalarVecPtrType, outer_arr, "outer_arr");

    mcols = innerPlanState(node)->ps_ResultTupleSlot->tts_tupleDescriptor->natts;
    llvm::Value** inner_vals_array = (llvm::Value**)palloc(sizeof(llvm::Value*) * mcols);
    llvm::Value** inner_flag_array = (llvm::Value**)palloc(sizeof(llvm::Value*) * mcols);
    for (j = 0; j < mcols; j++) {
        DEFINE_CGVAR_INT64(int64_jIdx, (long long)j);
        /* Get m_vals from pVector */
        Vals[0] = int64_jIdx;
        Vals[1] = int32_pos_scalvec_vals;
        tmpval = builder.CreateInBoundsGEP(inner_arr, Vals);
        tmpval = builder.CreateLoad(int64PtrType, tmpval, "inner_vals");
        inner_vals_array[j] = tmpval;
        /* Get m_flag from pVector */
        Vals[1] = int32_pos_scalvec_flag;
        tmpval = builder.CreateInBoundsGEP(inner_arr, Vals);
        tmpval = builder.CreateLoad(int8PtrType, tmpval, "inner_flag");
        inner_flag_array[j] = tmpval;
    }
    mcols = outerPlanState(node)->ps_ResultTupleSlot->tts_tupleDescriptor->natts;
    llvm::Value** outer_vals_array = (llvm::Value**)palloc(sizeof(llvm::Value*) * mcols);
    llvm::Value** outer_flag_array = (llvm::Value**)palloc(sizeof(llvm::Value*) * mcols);
    llvm::Value** batch_vals_array = (llvm::Value**)palloc(sizeof(llvm::Value*) * mcols);
    llvm::Value** batch_flag_array = (llvm::Value**)palloc(sizeof(llvm::Value*) * mcols);
    for (j = 0; j < mcols; j++) {
        DEFINE_CGVAR_INT64(int64_jIdx, (long long)j);
        Vals[0] = int64_jIdx;
        /* Get m_vals from pVector */
        Vals[1] = int32_pos_scalvec_vals;
        tmpval = builder.CreateInBoundsGEP(outer_arr, Vals);
        tmpval = builder.CreateLoad(int64PtrType, tmpval, "outer_vals");
        outer_vals_array[j] = tmpval;

        tmpval = builder.CreateInBoundsGEP(batch_arr, Vals);
        tmpval = builder.CreateLoad(int64PtrType, tmpval, "batch_val");
        batch_vals_array[j] = tmpval;

        /* Get m_flag from pVector */
        Vals[1] = int32_pos_scalvec_flag;
        tmpval = builder.CreateInBoundsGEP(outer_arr, Vals);
        tmpval = builder.CreateLoad(int8PtrType, tmpval, "outer_flag");
        outer_flag_array[j] = tmpval;

        tmpval = builder.CreateInBoundsGEP(batch_arr, Vals);
        tmpval = builder.CreateLoad(int8PtrType, tmpval, "batch_flag");
        batch_flag_array[j] = tmpval;
    }

    ListCell* lc_inner = NULL;
    ListCell* lc_outer = NULL;
    int keyIdx_var = 0;
    int outkeyIdx_var = 0;
    int num_hashkey = list_length(node->hj_InnerHashKeys);
    Var* variable = NULL;
    ExprState* exprState = NULL;
    llvm::Value** outer_key_mvals_array = (llvm::Value**)palloc(sizeof(llvm::Value*) * num_hashkey);
    llvm::Value** inner_keyIdx_array = (llvm::Value**)palloc(sizeof(llvm::Value*) * num_hashkey);
    Var** inner_var = (Var**)palloc(sizeof(Var*) * num_hashkey);
    j = 0;
    forboth(lc_inner, node->hj_InnerHashKeys, lc_outer, node->hj_OuterHashKeys)
    {
        /* get inner keyIdx */
        variable = NULL;
        exprState = (ExprState*)lfirst(lc_inner);
        if (IsA(exprState->expr, Var))
            variable = (Var*)exprState->expr;
        else if (IsA(exprState->expr, RelabelType)) {
            RelabelType* reltype = (RelabelType*)exprState->expr;
            if (IsA(reltype->arg, Var) && ((Var*)reltype->arg)->varattno > 0)
                variable = (Var*)((RelabelType*)exprState->expr)->arg;
        }

        if (variable == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmodule(MOD_LLVM), errmsg("Failed to get inner hash key!")));
        }
        keyIdx_var = variable->varattno - 1;
        inner_var[j] = variable;

        /* get outer keyIdx */
        variable = NULL;
        exprState = (ExprState*)lfirst(lc_outer);
        if (IsA(exprState->expr, Var))
            variable = (Var*)exprState->expr;
        else if (IsA(exprState->expr, RelabelType)) {
            RelabelType* reltype = (RelabelType*)exprState->expr;
            if (IsA(reltype->arg, Var) && ((Var*)reltype->arg)->varattno > 0)
                variable = (Var*)((RelabelType*)exprState->expr)->arg;
        }

        if (variable == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmodule(MOD_LLVM), errmsg("Failed to get outer hash key!")));
        }
        outkeyIdx_var = variable->varattno - 1;

        /* get the ScalarVector = m_arr[outkeyIdx_var] */
        DEFINE_CGVAR_INT64(int64_outkeyIdx, (long long)outkeyIdx_var);
        DEFINE_CGVAR_INT64(int64_keyIdx, (long long)keyIdx_var);
        tmpval = builder.CreateInBoundsGEP(batch_arr, int64_outkeyIdx);
        /* get val from batch->m_arr[outkeyIdx].m_vals[i] */
        Vals[0] = int64_0;
        Vals[1] = int32_pos_scalvec_vals;
        tmpval = builder.CreateInBoundsGEP(tmpval, Vals);
        tmpval = builder.CreateLoad(int64PtrType, tmpval, "batch_vals");
        inner_keyIdx_array[j] = int64_keyIdx;
        outer_key_mvals_array[j] = tmpval;
        j++;
    }

    llvm::Value* loopIdx1 = builder.CreateSExt(Phi, int64Type);
    builder.CreateBr(bb_loopEntry);

    /* bb_loopEntry */
    builder.SetInsertPoint(bb_loopEntry);
    llvm::PHINode* Phi_Idx = builder.CreatePHI(int64Type, 2);
    Phi_Idx->addIncoming(loopIdx1, bb_pre_loopEntry);
    llvm::Value* loopIdx2 = NULL;
    /* Phi_Idx->addIncoming(loopIdx2, bb_crit_edge); do this in bb_crit_edge*/
    llvm::PHINode* Phi_resultRow_06 = builder.CreatePHI(int32Type, 2);
    Phi_resultRow_06->addIncoming(int32_0, bb_pre_loopEntry);
    llvm::PHINode* Phi_resultRow_01 = NULL;

    /* get HashJoinTbl.hashBasedOperator.m_cellCache */
    tmpval = builder.CreatePtrToInt(hashJoinTbl, int64Type);
    tmpval = builder.CreateAdd(tmpval, int64_8032);
    tmpval2 = builder.CreateMul(Phi_Idx, int64_8);
    tmpval = builder.CreateAdd(tmpval, tmpval2);
    mcellCache = builder.CreateIntToPtr(tmpval, hashCellPPtrType);
    llvm::Value* cellCache_loopEntry = builder.CreateLoad(hashCellPtrType, mcellCache, "cellCache");
    tmpval = builder.CreatePtrToInt(cellCache_loopEntry, int64Type);
    tmpval = builder.CreateICmpEQ(tmpval, int64_0);
    builder.CreateCondBr(tmpval, bb_crit_edge, bb_keyMatch);

    /* bb_keyMatch */
    builder.SetInsertPoint(bb_keyMatch);
    llvm::PHINode* Phi_resultRow_keyMatch = builder.CreatePHI(int32Type, 2);
    Phi_resultRow_keyMatch->addIncoming(Phi_resultRow_06, bb_loopEntry);
    llvm::PHINode* Phi_resultRow_02 = NULL;
    llvm::PHINode* Phi_cellCache_keyMatch = NULL;

    Phi_cellCache_keyMatch = builder.CreatePHI(hashCellPtrType, 2);
    Phi_cellCache_keyMatch->addIncoming(cellCache_loopEntry, bb_loopEntry);

    Vals3[0] = int64_0;
    Vals3[1] = int32_0;
    Vals3[2] = int32_0;
    tmpval = builder.CreateInBoundsGEP(Phi_cellCache_keyMatch, Vals3);
    llvm::Value* nextCell = builder.CreateLoad(hashCellPtrType, tmpval, "nextCell");
    Phi_cellCache_keyMatch->addIncoming(nextCell, bb_TryNextCell);

    llvm::Value* fastPath_result = NULL;
    llvm::Value* oneMatch_result = NULL;

    /* fast path keyMatch
     * condition: (1) NOT NULL for both hash keys
     * 	          (2) integer hash keys
     */
    llvm::Value* cellCache = NULL;
    llvm::Value* val_cell = NULL;
    llvm::Value* val_batch = NULL;
    cellCache = Phi_cellCache_keyMatch;
    for (j = 0; j < num_hashkey; j++) {
        Vals4[0] = int64_0;
        Vals4[1] = int32_1;  // Get val from cellCache
        Vals4[2] = inner_keyIdx_array[j];
        Vals4[3] = int32_0;
        val_cell = builder.CreateInBoundsGEP(cellCache, Vals4);
        val_cell = builder.CreateLoad(int64Type, val_cell, "cell_val");
        val_batch = builder.CreateInBoundsGEP(outer_key_mvals_array[j], Phi_Idx);
        val_batch = builder.CreateLoad(int64Type, val_batch, "m_vals");
        switch (variable->vartype) {
            case INT4OID: {
                val_cell = builder.CreateTrunc(val_cell, int32Type);
                val_batch = builder.CreateTrunc(val_batch, int32Type);
                oneMatch_result = builder.CreateICmpEQ(val_cell, val_batch);
            } break;
            case INT8OID:
                oneMatch_result = builder.CreateICmpEQ(val_cell, val_batch);
                break;
            default:
                ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmodule(MOD_LLVM),
                        errmsg("Codegen fast keyMatch failed: unsupported data type!\n")));
        }

        if (fastPath_result == NULL)
            fastPath_result = oneMatch_result;
        else {
            fastPath_result = builder.CreateAnd(fastPath_result, oneMatch_result);
        }
    }
    builder.CreateCondBr(fastPath_result, bb_Matched, bb_NoMatch);

    builder.SetInsertPoint(bb_Matched);
    cellCache = Phi_cellCache_keyMatch;

    mcols = innerPlanState(node)->ps_ResultTupleSlot->tts_tupleDescriptor->natts;
    llvm::Value* resultRow = builder.CreateSExt(Phi_resultRow_keyMatch, int64Type);
    /* copy data from hash cell to innerBatch */
    llvm::Value* resultRow_i64 = builder.CreateSExt(resultRow, int64Type);
    for (j = 0; j < mcols; j++) {
        mvals = builder.CreateInBoundsGEP(inner_vals_array[j], resultRow_i64);
        mflag = builder.CreateInBoundsGEP(inner_flag_array[j], resultRow_i64);

        DEFINE_CGVAR_INT64(int64_jIdx, (long long)j);
        /* Get val from cellCache */
        Vals4[1] = int32_1;
        Vals4[2] = int64_jIdx;
        Vals4[3] = int32_0;
        val = builder.CreateInBoundsGEP(cellCache, Vals4);
        val = builder.CreateLoad(int64Type, val, "cell_val");
        /* get flag from cellCache */
        Vals4[3] = int32_1;
        flag = builder.CreateInBoundsGEP(cellCache, Vals4);
        flag = builder.CreateLoad(int8Type, flag, "cell_flag");

        /* Assign the cell val to mvals and cell flag to mflag */
        builder.CreateStore(val, mvals);
        builder.CreateStore(flag, mflag);
    }

    /* copy data from batch to outerBatch */
    mcols = outerPlanState(node)->ps_ResultTupleSlot->tts_tupleDescriptor->natts;
    for (j = 0; j < mcols; j++) {
        /* Get the address from outerBatch*/
        mvals = builder.CreateInBoundsGEP(outer_vals_array[j], resultRow_i64);
        mflag = builder.CreateInBoundsGEP(outer_flag_array[j], resultRow_i64);

        /* Get the val and flag from batch */
        val = builder.CreateInBoundsGEP(batch_vals_array[j], Phi_Idx);
        val = builder.CreateLoad(int64Type, val, "batch_val");
        flag = builder.CreateInBoundsGEP(batch_flag_array[j], Phi_Idx);
        flag = builder.CreateLoad(int8Type, flag, "batch_flag");

        /* Assign the cell val to mvals and cell flag to mflag */
        builder.CreateStore(val, mvals);
        builder.CreateStore(flag, mflag);
    }
    /* resultRow++ */
    llvm::Value* resultRow_Matched = builder.CreateAdd(Phi_resultRow_keyMatch, int32_1);
    tmpval = builder.CreateICmpEQ(resultRow_Matched, int32_BatchMaxSize);
    builder.CreateCondBr(tmpval, bb_BatchFull, bb_TryNextCell);

    builder.SetInsertPoint(bb_NoMatch);
    builder.CreateBr(bb_TryNextCell);

    builder.SetInsertPoint(bb_BatchFull);
    llvm::Value* idx_int32 = builder.CreateTrunc(Phi_Idx, int32Type);
    builder.CreateStore(idx_int32, lastBuildIdx_addr);
    /* Save m_cellCache[i]->flag.m_next to joinStateLog */
    builder.CreateStore(nextCell, lastCell_addr);
    builder.CreateStore(int8_1, joinStateLog_restore);
    builder.CreateBr(bb_ret);

    /* try next cell of m_cellCache[i] and update m_cellCache[i] if next cell exists */
    builder.SetInsertPoint(bb_TryNextCell);
    Phi_resultRow_02 = builder.CreatePHI(int32Type, 2);
    Phi_resultRow_02->addIncoming(resultRow_Matched, bb_Matched);
    Phi_resultRow_02->addIncoming(Phi_resultRow_keyMatch, bb_NoMatch);

    /* Phi_resultRow_keyMatch- is in bb_keyMatch */
    Phi_resultRow_keyMatch->addIncoming(Phi_resultRow_02, bb_TryNextCell);

    tmpval = builder.CreatePtrToInt(nextCell, int64Type);
    tmpval = builder.CreateICmpEQ(tmpval, int64_0);
    builder.CreateCondBr(tmpval, bb_crit_edge, bb_keyMatch);

    /* loop_index++ */
    /* Phi resultRow (loopEntry, TryNextCell)
     * if (loop_index < nRows) goto bb_loopEntry
     * else goto bb_ret
     */
    builder.SetInsertPoint(bb_crit_edge);
    Phi_resultRow_01 = builder.CreatePHI(int32Type, 2);
    Phi_resultRow_01->addIncoming(Phi_resultRow_06, bb_loopEntry);
    Phi_resultRow_01->addIncoming(Phi_resultRow_02, bb_TryNextCell);
    Phi_resultRow_06->addIncoming(Phi_resultRow_01, bb_crit_edge);
    loopIdx2 = builder.CreateAdd(Phi_Idx, int64_1);  // Phi_Idx is in bb_loopEntry
    Phi_Idx->addIncoming(loopIdx2, bb_crit_edge);
    tmpval = builder.CreateTrunc(loopIdx2, int32Type);
    tmpval = builder.CreateICmpSLT(tmpval, nRows);
    builder.CreateCondBr(tmpval, bb_loopEntry, bb_ret);

    /* return resultRow */
    builder.SetInsertPoint(bb_ret);
    llvm::PHINode* Phi_resultRow = builder.CreatePHI(int32Type, 3);
    Phi_resultRow->addIncoming(int32_BatchMaxSize, bb_BatchFull);
    llvm::Value* resultRow_crit_edge = builder.CreateTrunc(Phi_resultRow_01, int32Type);
    Phi_resultRow->addIncoming(resultRow_crit_edge, bb_crit_edge);
    Phi_resultRow->addIncoming(int32_0, bb_restoreFalse);
    builder.CreateRet(Phi_resultRow);

    pfree_ext(inner_vals_array);
    pfree_ext(inner_flag_array);
    pfree_ext(outer_vals_array);
    pfree_ext(outer_flag_array);
    pfree_ext(batch_vals_array);
    pfree_ext(batch_flag_array);
    pfree_ext(outer_key_mvals_array);
    pfree_ext(inner_keyIdx_array);
    pfree_ext(inner_var);

    /* finalize the jitted function */
    llvmCodeGen->FinalizeFunction(jitted_hashjoin, node->js.ps.plan->plan_node_id);

    return jitted_hashjoin;
}

/*
 * @Description	: Codegen buildHashTable for hash join. Replace the hash
 * 				  function(hashint4 + hash_uint32) by CRC32 hardware instruction.
 * @Notes		: Only Support the case with condition:
 *			      (1) int4, int8, or bpchar hash keys
 *				  (2) NeedCopy is false
 *				  (3) m_complicateJoinKey is false
 */
llvm::Function* VecHashJoinCodeGen::HashJoinCodeGen_buildHashTable(VecHashJoinState* node)
{
    Assert(NULL != (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj);
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    /* Find and load the IR file from the installaion directory */
    llvmCodeGen->loadIRFile();

    /* Get LLVM Context and builder */
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);
    llvm::Module* mod = llvmCodeGen->module();

    llvm::Function* jitted_buildHashTable = NULL;
    llvm::Value* tmpval = NULL;
    llvm::Value* llvmargs[2];
    llvm::Value* cellHead = NULL;
    llvm::Value* hashTbl = NULL;

    int j;
    llvm::Value* val = NULL;
    llvm::Value* flag = NULL;
    llvm::Value* mask = NULL;
    llvm::Value* hash_result = NULL;

    /* Define data types and some llvm consts */
    DEFINE_CG_VOIDTYPE(voidType);
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int16Type, INT2OID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(int16PtrType, INT2OID);
    DEFINE_CG_PTRTYPE(int32PtrType, INT4OID);
    DEFINE_CG_PTRTYPE(int64PtrType, INT8OID);
    DEFINE_CG_PTRTYPE(hashCellPtrType, "struct.hashCell");
    DEFINE_CG_PTRPTRTYPE(hashCellPtrPtrType, "struct.hashCell");
    DEFINE_CG_PTRTYPE(vechashtablePtrType, "class.vechashtable");

    DEFINE_CGVAR_INT8(int8_0, 0);
    DEFINE_CGVAR_INT8(int8_1, 1);
    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT64(int64_0, 0);
    DEFINE_CGVAR_INT64(int64_2, 2);
    DEFINE_CGVAR_INT64(int64_4, 4);
    DEFINE_CGVAR_INT64(int64_8, 8);

    /* llvm array values, used to represent the location of some element */
    llvm::Value* Vals2[2] = {int64_0, int32_0};
    llvm::Value* Vals3[3] = {int64_0, int32_0, int32_0};
    llvm::Value* Vals4[4] = {int64_0, int32_0, int32_0, int32_0};

    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_buildHashTable", voidType);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("cellHead", hashCellPtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("hashTbl", vechashtablePtrType));
    jitted_buildHashTable = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    /* start the main codegen process for buildHashTable in hash join */
    cellHead = llvmargs[0];
    hashTbl = llvmargs[1];

    VecHashJoin* plannode = (VecHashJoin*)node->js.ps.plan;
    Plan* innerPlan = innerPlan(plannode);

    /* add 1 column to flag match or not */
    int numCols = 0;
    if (plannode->join.jointype == JOIN_RIGHT || plannode->join.jointype == JOIN_RIGHT_ANTI_FULL ||
        plannode->join.jointype == JOIN_RIGHT_ANTI || plannode->join.jointype == JOIN_RIGHT_SEMI)
        numCols = list_length(innerPlan->targetlist) + 1;
    else
        numCols = list_length(innerPlan->targetlist);

    /*
     * Since m_complicateJoinKey is false, m_cellSize equals to
     * sizeof(hashCell) + (m_cols - 1) * sizeof(hashVal);
     */
    int cellSize_value = offsetof(hashCell, m_val) + numCols * sizeof(hashVal);
    llvm::Value* int64_cellsize = llvmCodeGen->getIntConstant(INT8OID, cellSize_value);

    /* m_key equals to list_length(node->hj_InnerHashKeys) */
    int num_hashkey = list_length(node->hj_InnerHashKeys);

    /* define basic block information for the function */
    llvm::BasicBlock** bb_checkFlag = (llvm::BasicBlock**)palloc(sizeof(llvm::BasicBlock*) * num_hashkey);
    llvm::BasicBlock** bb_hashing = (llvm::BasicBlock**)palloc(sizeof(llvm::BasicBlock*) * num_hashkey);
    for (j = 0; j < num_hashkey; j++) {
        bb_checkFlag[j] = llvm::BasicBlock::Create(context, "bb_checkFlag", jitted_buildHashTable);
        bb_hashing[j] = llvm::BasicBlock::Create(context, "bb_hashing", jitted_buildHashTable);
    }

    DEFINE_BLOCK(bb_pre_loop, jitted_buildHashTable);
    DEFINE_BLOCK(bb_loopEnd, jitted_buildHashTable);
    DEFINE_BLOCK(bb_ret, jitted_buildHashTable);

    /* entry basic block */
    /* get the number of rows of this batch : nrows = cellHead->flag.m_rows; */
    tmpval = builder.CreateBitCast(cellHead, int32PtrType);
    llvm::Value* nRows = builder.CreateLoad(int32Type, tmpval, "nrows");
    tmpval = builder.CreateICmpSGT(nRows, int32_0);
    builder.CreateCondBr(tmpval, bb_pre_loop, bb_ret);

    /*
     * bb_pre_loopEntry:
     * Get all m_keyIdx constants.
     */
    builder.SetInsertPoint(bb_pre_loop);
    /* m_hashTbl is constructed by hashSize, mask = hashSize - 1 */
    Vals2[1] = int32_0;
    mask = builder.CreateInBoundsGEP(hashTbl, Vals2);
    mask = builder.CreateLoad(int32Type, mask, "m_size");
    mask = builder.CreateSub(mask, int32_1);

    ListCell* lc_inner = NULL;
    int keyIdx_var = 0;
    Var* variable = NULL;
    ExprState* exprState = NULL;
    Var** inner_var = (Var**)palloc(sizeof(Var*) * num_hashkey);
    llvm::Value** inner_keyIdx_array = (llvm::Value**)palloc(sizeof(llvm::Value*) * num_hashkey);

    /* loop over all the right hashkeys to preload all the variables */
    j = 0;
    foreach (lc_inner, node->hj_InnerHashKeys) {
        /* get inner keyIdx */
        variable = NULL;
        exprState = (ExprState*)lfirst(lc_inner);

        if (IsA(exprState->expr, Var)) {
            variable = (Var*)exprState->expr;
        } else if (IsA(exprState->expr, RelabelType)) {
            RelabelType* reltype = (RelabelType*)exprState->expr;

            if (IsA(reltype->arg, Var) && ((Var*)reltype->arg)->varattno > 0)
                variable = (Var*)((RelabelType*)exprState->expr)->arg;
        }

        Assert(variable != NULL);

        keyIdx_var = variable->varattno - 1;
        inner_var[j] = variable;

        DEFINE_CGVAR_INT64(int64_keyIdx, (long long)keyIdx_var);
        inner_keyIdx_array[j] = int64_keyIdx;
        j++;
    }
    builder.CreateBr(bb_checkFlag[0]);

    /* Beginning of the loop */
    builder.SetInsertPoint(bb_checkFlag[0]);
    llvm::PHINode* Phi_cell = builder.CreatePHI(hashCellPtrType, 2);
    llvm::PHINode* Phi_Idx = builder.CreatePHI(int32Type, 2);
    Phi_cell->addIncoming(cellHead, bb_pre_loop);
    Phi_Idx->addIncoming(int32_0, bb_pre_loop);

    /* defined for multiple hash keys */
    llvm::PHINode* Phi_hash_result = NULL;
    llvm::Value* prev_hash_value = NULL;
    for (j = 0; j < num_hashkey; j++) {
        /* bb_checkFlag */
        builder.SetInsertPoint(bb_checkFlag[j]);
        if (j > 0) {
            Phi_hash_result = builder.CreatePHI(int32Type, 2);
            Phi_hash_result->addIncoming(prev_hash_value, bb_checkFlag[j - 1]);
            Phi_hash_result->addIncoming(hash_result, bb_hashing[j - 1]);
            prev_hash_value = Phi_hash_result;
        } else
            prev_hash_value = int32_0;

        /* get the flag of hash key : cell->m_val[keyIdx].flag */
        Vals4[1] = int32_1;
        Vals4[2] = inner_keyIdx_array[j];
        Vals4[3] = int32_1;
        flag = builder.CreateInBoundsGEP(Phi_cell, Vals4);
        flag = builder.CreateLoad(int8Type, flag, "flag");
        flag = builder.CreateAnd(flag, int8_1);
        tmpval = builder.CreateICmpEQ(flag, int8_0);

        if (j == num_hashkey - 1)
            builder.CreateCondBr(tmpval, bb_hashing[j], bb_loopEnd);
        else
            builder.CreateCondBr(tmpval, bb_hashing[j], bb_checkFlag[j + 1]);

        /* when val is not null */
        builder.SetInsertPoint(bb_hashing[j]);
        /* cell->m_val[keyIdx].val */
        Vals4[3] = int32_0;
        val = builder.CreateInBoundsGEP(Phi_cell, Vals4);
        val = builder.CreateLoad(int64Type, val, "val");

        /* get the variable type according to the preloaded variable */
        variable = inner_var[j];
        switch (variable->vartype) {
            case INT4OID: {
                val = builder.CreateTrunc(val, int32Type);
                llvm_crc32_32_32(hash_result, prev_hash_value, val);
            } break;
            case INT8OID: {
                llvm_crc32_32_64(hash_result, prev_hash_value, val);
            } break;
            case BPCHAROID: {
                int len = variable->vartypmod - VARHDRSZ;
                llvm::Function* func_evalvar = llvmCodeGen->module()->getFunction("JittedEvalVarlena");
                if (func_evalvar == NULL) {
                    func_evalvar = VarlenaCvtCodeGen();
                }
                llvm::Value* res = builder.CreateCall(func_evalvar, val, "func_evalvar");
                llvm::Value* data1 = builder.CreateExtractValue(res, 1);
                llvm::Value* data1_int64 = builder.CreatePtrToInt(data1, int64Type);

                llvm::Value* pval = NULL;
                llvm::Value* hash_res1 = prev_hash_value;
                if (len >= 8) {
                    while (len >= 8) {
                        pval = builder.CreateIntToPtr(data1_int64, int64PtrType);
                        pval = builder.CreateLoad(int64Type, pval, "int64data");
                        llvm_crc32_32_64(hash_res1, hash_res1, pval);
                        data1_int64 = builder.CreateAdd(data1_int64, int64_8);
                        len = len - 8;
                    }
                }

                if (len >= 4) {
                    pval = builder.CreateIntToPtr(data1_int64, int32PtrType);
                    pval = builder.CreateLoad(int32Type, pval, "int32data");
                    llvm_crc32_32_32(hash_res1, hash_res1, pval);
                    data1_int64 = builder.CreateAdd(data1_int64, int64_4);
                    len = len - 4;
                }

                if (len >= 2) {
                    pval = builder.CreateIntToPtr(data1_int64, int16PtrType);
                    pval = builder.CreateLoad(int16Type, pval, "int16data");
                    llvm_crc32_32_16(hash_res1, hash_res1, pval);
                    data1_int64 = builder.CreateAdd(data1_int64, int64_2);
                    len = len - 2;
                }

                if (len == 1) {
                    pval = builder.CreateIntToPtr(data1_int64, int8PtrType);
                    pval = builder.CreateLoad(int8Type, pval, "int8data");
                    llvm_crc32_32_8(hash_res1, hash_res1, pval);
                }
                hash_result = hash_res1;
            } break;
            default: {
                ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmodule(MOD_LLVM),
                        errmsg("Codegen buildHashTable failed: unsupported data type!\n")));
            }
        }

        if (j == num_hashkey - 1)
            builder.CreateBr(bb_loopEnd);
        else
            builder.CreateBr(bb_checkFlag[j + 1]);
    }

    builder.SetInsertPoint(bb_loopEnd);
    llvm::PHINode* Phi_result = builder.CreatePHI(int32Type, 2);
    Phi_result->addIncoming(hash_result, bb_hashing[num_hashkey - 1]);
    Phi_result->addIncoming(prev_hash_value, bb_checkFlag[num_hashkey - 1]);

    tmpval = builder.CreateAnd(Phi_result, mask);
    llvm::Value* bucketIdx = builder.CreateZExt(tmpval, int64Type);

    /* Get m_data from vechashtable */
    Vals2[1] = int32_1;
    llvm::Value* m_data = builder.CreateInBoundsGEP(hashTbl, Vals2);
    m_data = builder.CreateLoad(hashCellPtrPtrType, m_data, "m_data");

    llvm::Value* m_data_idx = builder.CreateInBoundsGEP(m_data, bucketIdx);
    llvm::Value* lastCell = builder.CreateLoad(hashCellPtrType, m_data_idx, "lastCell");

    builder.CreateStore(Phi_cell, m_data_idx);

    /* get cell->flag.m_next */
    Vals3[1] = int32_0;
    Vals3[2] = int32_0;
    llvm::Value* next_addr = builder.CreateInBoundsGEP(Phi_cell, Vals3);

    builder.CreateStore(lastCell, next_addr);
    tmpval = builder.CreateBitCast(Phi_cell, int8PtrType);
    tmpval = builder.CreateInBoundsGEP(tmpval, int64_cellsize);

    llvm::Value* nextCell = builder.CreateBitCast(tmpval, hashCellPtrType);
    llvm::Value* nextIdx = builder.CreateAdd(Phi_Idx, int32_1);
    Phi_cell->addIncoming(nextCell, bb_loopEnd);
    Phi_Idx->addIncoming(nextIdx, bb_loopEnd);
    tmpval = builder.CreateICmpEQ(nextIdx, nRows);
    builder.CreateCondBr(tmpval, bb_ret, bb_checkFlag[0]);

    builder.SetInsertPoint(bb_ret);
    builder.CreateRetVoid();

    pfree_ext(inner_keyIdx_array);
    pfree_ext(inner_var);
    pfree_ext(bb_checkFlag);
    pfree_ext(bb_hashing);

    /* finalize the jitted function */
    llvmCodeGen->FinalizeFunction(jitted_buildHashTable, node->js.ps.plan->plan_node_id);

    return jitted_buildHashTable;
}

/* @Description	: Codegen buildHashTable for hash join when NeedCopy is
 *				  required. Replace the hash function (hashint4 + hash_uint32)
 *				  by CRC32 hardware instruction.
 * @Notes		: Only Support the case with condition:
 *				  (1) int4, int8, or bpchar hash keys
 *				  (2) NeedCopy is true
 *				  (3) m_complicateJoinKey is false
 */
llvm::Function* VecHashJoinCodeGen::HashJoinCodeGen_buildHashTable_NeedCopy(VecHashJoinState* node)
{
    Assert(NULL != (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj);
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    /* Find and load the IR file from the installaion directory */
    llvmCodeGen->loadIRFile();

    /* Get LLVM Context and builder */
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);
    llvm::Module* mod = llvmCodeGen->module();

    llvm::Function* jitted_buildHashTable = NULL;
    llvm::Value* tmpval = NULL;
    llvm::Value* llvmargs[2];
    llvm::Value* cellHead = NULL;
    llvm::Value* hashTbl = NULL;

    int j;
    llvm::Value* val = NULL;
    llvm::Value* flag = NULL;
    llvm::Value* mask = NULL;
    llvm::Value* hash_result = NULL;

    /* Define data types and some llvm consts */
    DEFINE_CG_VOIDTYPE(voidType);
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int16Type, INT2OID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(int16PtrType, INT2OID);
    DEFINE_CG_PTRTYPE(int32PtrType, INT4OID);
    DEFINE_CG_PTRTYPE(int64PtrType, INT8OID);
    DEFINE_CG_PTRTYPE(hashCellPtrType, "struct.hashCell");
    DEFINE_CG_PTRPTRTYPE(hashCellPtrPtrType, "struct.hashCell");
    DEFINE_CG_PTRTYPE(vechashtablePtrType, "class.vechashtable");

    DEFINE_CGVAR_INT8(int8_0, 0);
    DEFINE_CGVAR_INT8(int8_1, 1);
    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT64(int64_0, 0);
    DEFINE_CGVAR_INT64(int64_2, 2);
    DEFINE_CGVAR_INT64(int64_4, 4);
    DEFINE_CGVAR_INT64(int64_8, 8);

    /* llvm array values, used to represent the location of some element */
    llvm::Value* Vals2[2] = {int64_0, int32_0};
    llvm::Value* Vals3[3] = {int64_0, int32_0, int32_0};
    llvm::Value* Vals4[4] = {int64_0, int32_0, int32_0, int32_0};

    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_buildHashTable_NeedCopy", voidType);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("cellHead", hashCellPtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("hashTbl", vechashtablePtrType));
    jitted_buildHashTable = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    /* start the main codegen process for buildHashTable in hash join */
    cellHead = llvmargs[0];
    hashTbl = llvmargs[1];

    VecHashJoin* plannode = (VecHashJoin*)node->js.ps.plan;
    Plan* innerPlan = innerPlan(plannode);

    /* add 1 column to flag match or not */
    int numCols = 0;
    if (plannode->join.jointype == JOIN_RIGHT || plannode->join.jointype == JOIN_RIGHT_ANTI_FULL ||
        plannode->join.jointype == JOIN_RIGHT_ANTI || plannode->join.jointype == JOIN_RIGHT_SEMI)
        numCols = list_length(innerPlan->targetlist) + 1;
    else
        numCols = list_length(innerPlan->targetlist);

    /*
     * Since m_complicateJoinKey is false, m_cellSize equals to
     * sizeof(hashCell) + (m_cols - 1) * sizeof(hashVal);
     */
    int cellSize_value = offsetof(hashCell, m_val) + numCols * sizeof(hashVal);
    llvm::Value* int64_cellsize = llvmCodeGen->getIntConstant(INT8OID, cellSize_value);

    /* m_key equals to list_length(node->hj_InnerHashKeys) */
    int num_hashkey = list_length(node->hj_InnerHashKeys);

    /* define basic block information for the function */
    llvm::BasicBlock** bb_checkFlag = (llvm::BasicBlock**)palloc(sizeof(llvm::BasicBlock*) * num_hashkey);
    llvm::BasicBlock** bb_hashing = (llvm::BasicBlock**)palloc(sizeof(llvm::BasicBlock*) * num_hashkey);

    for (j = 0; j < num_hashkey; j++) {
        bb_checkFlag[j] = llvm::BasicBlock::Create(context, "bb_checkFlag", jitted_buildHashTable);
        bb_hashing[j] = llvm::BasicBlock::Create(context, "bb_hashing", jitted_buildHashTable);
    }

    DEFINE_BLOCK(bb_pre_loop, jitted_buildHashTable);
    DEFINE_BLOCK(bb_loopEnd, jitted_buildHashTable);
    DEFINE_BLOCK(bb_ret, jitted_buildHashTable);

    /* entry basic block */
    /* get the number of rows of this batch : nrows = cellHead->flag.m_rows; */
    tmpval = builder.CreateBitCast(cellHead, int32PtrType);
    llvm::Value* nRows = builder.CreateLoad(int32Type, tmpval, "nrows");
    tmpval = builder.CreateICmpSGT(nRows, int32_0);
    builder.CreateCondBr(tmpval, bb_pre_loop, bb_ret);

    /*
     * bb_pre_loopEntry:
     * Get all m_keyIdx constants.
     */
    builder.SetInsertPoint(bb_pre_loop);

    /* since needcopy is true, do allocate for copyCellArray */
    llvm::Function* func_palloc = llvmCodeGen->module()->getFunction("Wrappalloc");
    if (NULL == func_palloc) {
        GsCodeGen::FnPrototype func_prototype(llvmCodeGen, "Wrappalloc", int8PtrType);
        func_prototype.addArgument(GsCodeGen::NamedVariable("size", int32Type));
        func_palloc = func_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("Wrappalloc", (void*)Wrappalloc);
    }

    llvm::Value* int32_cellsize = llvmCodeGen->getIntConstant(INT4OID, cellSize_value);
    llvm::Value* alloc_size = builder.CreateMul(nRows, int32_cellsize);
    llvm::Value* copyCellArray = builder.CreateCall(func_palloc, alloc_size);
    copyCellArray = builder.CreateBitCast(copyCellArray, hashCellPtrType);

    /* m_hashTbl is constructed by hashSize, mask = hashSize - 1 */
    Vals2[1] = int32_0;
    mask = builder.CreateInBoundsGEP(hashTbl, Vals2);
    mask = builder.CreateLoad(int32Type, mask, "m_size");
    mask = builder.CreateSub(mask, int32_1);

    ListCell* lc_inner = NULL;
    int keyIdx_var = 0;
    Var* variable = NULL;
    ExprState* exprState = NULL;
    Var** inner_var = (Var**)palloc(sizeof(Var*) * num_hashkey);
    llvm::Value** inner_keyIdx_array = (llvm::Value**)palloc(sizeof(llvm::Value*) * num_hashkey);

    /* loop over all the right hashkeys to preload all the variables */
    j = 0;
    foreach (lc_inner, node->hj_InnerHashKeys) {
        /* get inner keyIdx */
        variable = NULL;
        exprState = (ExprState*)lfirst(lc_inner);
        if (IsA(exprState->expr, Var)) {
            variable = (Var*)exprState->expr;
        } else if (IsA(exprState->expr, RelabelType)) {
            RelabelType* reltype = (RelabelType*)exprState->expr;

            if (IsA(reltype->arg, Var) && ((Var*)reltype->arg)->varattno > 0)
                variable = (Var*)((RelabelType*)exprState->expr)->arg;
        }

        Assert(variable != NULL);

        keyIdx_var = variable->varattno - 1;
        inner_var[j] = variable;

        DEFINE_CGVAR_INT64(int64_keyIdx, (long long)keyIdx_var);
        inner_keyIdx_array[j] = int64_keyIdx;
        j++;
    }
    builder.CreateBr(bb_checkFlag[0]);

    /* Beginning of the loop */
    builder.SetInsertPoint(bb_checkFlag[0]);
    llvm::PHINode* Phi_cell = builder.CreatePHI(hashCellPtrType, 2);
    llvm::PHINode* Phi_copyCell = builder.CreatePHI(hashCellPtrType, 2);
    llvm::PHINode* Phi_Idx = builder.CreatePHI(int32Type, 2);
    Phi_cell->addIncoming(cellHead, bb_pre_loop);
    Phi_copyCell->addIncoming(copyCellArray, bb_pre_loop);
    Phi_Idx->addIncoming(int32_0, bb_pre_loop);

    /* defined for multiple hash keys */
    llvm::PHINode* Phi_hash_result = NULL;
    llvm::Value* prev_hash_value = NULL;
    for (j = 0; j < num_hashkey; j++) {
        /* bb_checkFlag */
        builder.SetInsertPoint(bb_checkFlag[j]);
        if (j > 0) {
            Phi_hash_result = builder.CreatePHI(int32Type, 2);
            Phi_hash_result->addIncoming(prev_hash_value, bb_checkFlag[j - 1]);
            Phi_hash_result->addIncoming(hash_result, bb_hashing[j - 1]);
            prev_hash_value = Phi_hash_result;
        } else
            prev_hash_value = int32_0;

        /* get the flag of hash key : cell->m_val[keyIdx].flag */
        Vals4[1] = int32_1;
        Vals4[2] = inner_keyIdx_array[j];
        Vals4[3] = int32_1;
        flag = builder.CreateInBoundsGEP(Phi_cell, Vals4);
        flag = builder.CreateLoad(int8Type, flag, "flag");
        flag = builder.CreateAnd(flag, int8_1);
        tmpval = builder.CreateICmpEQ(flag, int8_0);

        if (j == num_hashkey - 1)
            builder.CreateCondBr(tmpval, bb_hashing[j], bb_loopEnd);
        else
            builder.CreateCondBr(tmpval, bb_hashing[j], bb_checkFlag[j + 1]);

        /* when val is not null, do hashing */
        builder.SetInsertPoint(bb_hashing[j]);
        Vals4[3] = int32_0;
        val = builder.CreateInBoundsGEP(Phi_cell, Vals4);
        val = builder.CreateLoad(int64Type, val, "val");

        variable = inner_var[j];
        switch (variable->vartype) {
            case INT4OID: {
                val = builder.CreateTrunc(val, int32Type);
                llvm_crc32_32_32(hash_result, prev_hash_value, val);
            } break;
            case INT8OID: {
                llvm_crc32_32_64(hash_result, prev_hash_value, val);
            } break;
            case BPCHAROID: {
                int len = variable->vartypmod - VARHDRSZ;
                llvm::Function* func_evalvar = llvmCodeGen->module()->getFunction("JittedEvalVarlena");
                if (func_evalvar == NULL) {
                    func_evalvar = VarlenaCvtCodeGen();
                }
                llvm::Value* res = builder.CreateCall(func_evalvar, val, "func_evalvar");
                llvm::Value* data1 = builder.CreateExtractValue(res, 1);
                llvm::Value* data1_int64 = builder.CreatePtrToInt(data1, int64Type);

                llvm::Value* pval = NULL;
                llvm::Value* hash_res1 = prev_hash_value;
                if (len >= 8) {
                    while (len >= 8) {
                        pval = builder.CreateIntToPtr(data1_int64, int64PtrType);
                        pval = builder.CreateLoad(int64Type, pval, "int64data");
                        llvm_crc32_32_64(hash_res1, hash_res1, pval);
                        data1_int64 = builder.CreateAdd(data1_int64, int64_8);
                        len = len - 8;
                    }
                }

                if (len >= 4) {
                    pval = builder.CreateIntToPtr(data1_int64, int32PtrType);
                    pval = builder.CreateLoad(int32Type, pval, "int32data");
                    llvm_crc32_32_32(hash_res1, hash_res1, pval);
                    data1_int64 = builder.CreateAdd(data1_int64, int64_4);
                    len = len - 4;
                }

                if (len >= 2) {
                    pval = builder.CreateIntToPtr(data1_int64, int16PtrType);
                    pval = builder.CreateLoad(int16Type, pval, "int16data");
                    llvm_crc32_32_16(hash_res1, hash_res1, pval);
                    data1_int64 = builder.CreateAdd(data1_int64, int64_2);
                    len = len - 2;
                }

                if (len == 1) {
                    pval = builder.CreateIntToPtr(data1_int64, int8PtrType);
                    pval = builder.CreateLoad(int8Type, pval, "int8data");
                    llvm_crc32_32_8(hash_res1, hash_res1, pval);
                }
                hash_result = hash_res1;
            } break;
            default: {
                ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmodule(MOD_LLVM),
                        errmsg("Codegen buildHashTable failed: unsupported data type!\n")));
            }
        }

        if (j == num_hashkey - 1)
            builder.CreateBr(bb_loopEnd);
        else
            builder.CreateBr(bb_checkFlag[j + 1]);
    }

    builder.SetInsertPoint(bb_loopEnd);
    llvm::PHINode* Phi_result = builder.CreatePHI(int32Type, 2);
    Phi_result->addIncoming(hash_result, bb_hashing[num_hashkey - 1]);
    Phi_result->addIncoming(prev_hash_value, bb_checkFlag[num_hashkey - 1]);

    tmpval = builder.CreateAnd(Phi_result, mask);
    llvm::Value* bucketIdx = builder.CreateZExt(tmpval, int64Type);

    /* Get m_data from vechashtable */
    Vals2[1] = int32_1;
    llvm::Value* m_data = builder.CreateInBoundsGEP(hashTbl, Vals2);
    m_data = builder.CreateLoad(hashCellPtrPtrType, m_data, "m_data");

    llvm::Value* m_data_idx = builder.CreateInBoundsGEP(m_data, bucketIdx);
    llvm::Value* lastCell = builder.CreateLoad(hashCellPtrType, m_data_idx, "lastCell");

    llvm::Value* target = builder.CreateBitCast(Phi_copyCell, int8PtrType);
    llvm::Value* source = builder.CreateBitCast(Phi_cell, int8PtrType);
    builder.CreateMemCpy(target, (llvm::MaybeAlign)1, source, (llvm::MaybeAlign)1, cellSize_value);

    builder.CreateStore(Phi_copyCell, m_data_idx);

    /* get copyCell->flag.m_next */
    Vals3[1] = int32_0;
    Vals3[2] = int32_0;
    llvm::Value* next_addr = builder.CreateInBoundsGEP(Phi_copyCell, Vals3);

    builder.CreateStore(lastCell, next_addr);

    /* Get next cell and next copyCell : GET_NTH_CELL(hashcell, i) */
    tmpval = builder.CreateBitCast(Phi_cell, int8PtrType);
    tmpval = builder.CreateInBoundsGEP(tmpval, int64_cellsize);
    llvm::Value* nextCell = builder.CreateBitCast(tmpval, hashCellPtrType);
    llvm::Value* nextIdx = builder.CreateAdd(Phi_Idx, int32_1);
    Phi_cell->addIncoming(nextCell, bb_loopEnd);

    tmpval = builder.CreateBitCast(Phi_copyCell, int8PtrType);
    tmpval = builder.CreateInBoundsGEP(tmpval, int64_cellsize);
    llvm::Value* nextCopyCell = builder.CreateBitCast(tmpval, hashCellPtrType);
    Phi_copyCell->addIncoming(nextCopyCell, bb_loopEnd);

    /* next idx */
    Phi_Idx->addIncoming(nextIdx, bb_loopEnd);

    tmpval = builder.CreateICmpEQ(nextIdx, nRows);
    builder.CreateCondBr(tmpval, bb_ret, bb_checkFlag[0]);

    builder.SetInsertPoint(bb_ret);
    builder.CreateRetVoid();

    pfree_ext(inner_keyIdx_array);
    pfree_ext(inner_var);
    pfree_ext(bb_checkFlag);
    pfree_ext(bb_hashing);

    /* finalize the jitted function */
    llvmCodeGen->FinalizeFunction(jitted_buildHashTable, node->js.ps.plan->plan_node_id);

    return jitted_buildHashTable;
}

/*
 * @Description	: Codegen probeHashTable for hash join. Replace the hash
 *				  function(hashint4 + hash_uint32) by CRC32 hardware instruction.
 * @Notes		: Only Support the case with condition:
 *				  (1) int4, int8, or bpchar hash keys
 *				  (2) m_complicateJoinKey is false
 */
llvm::Function* VecHashJoinCodeGen::HashJoinCodeGen_probeHashTable(VecHashJoinState* node)
{
    Assert(NULL != (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj);
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    /* Find and load the IR file from the installaion directory */
    llvmCodeGen->loadIRFile();

    /* Get LLVM Context and builder */
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);
    llvm::Module* mod = llvmCodeGen->module();

    llvm::Function* jitted_probeHashTable = NULL;
    llvm::Value* tmpval = NULL;
    llvm::Value* llvmargs[2];
    llvm::Value* hashJoinTbl = NULL;
    llvm::Value* batch = NULL;

    int j;
    llvm::Value* val = NULL;
    llvm::Value* flag = NULL;
    llvm::Value* mask = NULL;
    llvm::Value* hash_result = NULL;

    /* Define data types and some llvm consts */
    DEFINE_CG_VOIDTYPE(voidType);
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int16Type, INT2OID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(int16PtrType, INT2OID);
    DEFINE_CG_PTRTYPE(int32PtrType, INT4OID);
    DEFINE_CG_PTRTYPE(int64PtrType, INT8OID);
    DEFINE_CG_PTRTYPE(VectorBatchPtrType, "class.VectorBatch");
    DEFINE_CG_PTRTYPE(HashJoinTblPtrType, "class.HashJoinTbl");
    DEFINE_CG_PTRTYPE(vechashtablePtrType, "class.vechashtable");
    DEFINE_CG_PTRTYPE(hashBasedOperatorPtrType, "class.hashBasedOperator.base");
    DEFINE_CG_PTRTYPE(hashCellPtrType, "struct.hashCell");
    DEFINE_CG_PTRPTRTYPE(hashCellPtrPtrType, "struct.hashCell");
    DEFINE_CG_PTRTYPE(scalarVecPtrType, "class.ScalarVector");

    DEFINE_CGVAR_INT8(int8_0, 0);
    DEFINE_CGVAR_INT8(int8_1, 1);
    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT32(int32_pos_hashtbl_data, pos_hashtbl_data);
    DEFINE_CGVAR_INT32(int32_pos_hashtbl_size, pos_hashtbl_size);
    DEFINE_CGVAR_INT32(int32_pos_hBOper_htbl, pos_hBOper_htbl);
    DEFINE_CGVAR_INT32(int32_pos_scalvec_flag, pos_scalvec_flag);
    DEFINE_CGVAR_INT32(int32_pos_batch_marr, pos_batch_marr);
    DEFINE_CGVAR_INT32(int32_pos_scalvec_vals, pos_scalvec_vals);
    DEFINE_CGVAR_INT32(int32_pos_hBOper_keyMatch, pos_hBOper_keyMatch);
    DEFINE_CGVAR_INT32(int32_pos_hjointbl_match, pos_hjointbl_match);
    DEFINE_CGVAR_INT32(int32_pos_hBOper_cellCache, pos_hBOper_cellCache);
    DEFINE_CGVAR_INT32(int32_pos_hBOper_cacheLoc, pos_hBOper_cacheLoc);
    DEFINE_CGVAR_INT64(int64_0, 0);
    DEFINE_CGVAR_INT64(int64_2, 2);
    DEFINE_CGVAR_INT64(int64_4, 4);
    DEFINE_CGVAR_INT64(int64_8, 8);

    /* llvm array values, used to represent the location of some element */
    llvm::Value* Vals2[2] = {int64_0, int32_0};
    llvm::Value* Vals3[3] = {int64_0, int32_0, int32_0};

    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "Jitted_probeHashTable", voidType);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("hashJoinTbl", HashJoinTblPtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("batch", VectorBatchPtrType));
    jitted_probeHashTable = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    /* start the main codegen process for probeHashTable in hash join */
    hashJoinTbl = llvmargs[0];
    batch = llvmargs[1];

    /* m_key equals to list_length(node->hj_InnerHashKeys) */
    int num_hashkey = list_length(node->hj_InnerHashKeys);

    /* define basic block information for the function */
    llvm::BasicBlock** bb_checkFlag = (llvm::BasicBlock**)palloc(sizeof(llvm::BasicBlock*) * num_hashkey);
    llvm::BasicBlock** bb_hashing = (llvm::BasicBlock**)palloc(sizeof(llvm::BasicBlock*) * num_hashkey);
    for (j = 0; j < num_hashkey; j++) {
        bb_checkFlag[j] = llvm::BasicBlock::Create(context, "bb_checkFlag", jitted_probeHashTable);
        bb_hashing[j] = llvm::BasicBlock::Create(context, "bb_hashing", jitted_probeHashTable);
    }

    DEFINE_BLOCK(bb_pre_loop, jitted_probeHashTable);
    DEFINE_BLOCK(bb_loopEnd, jitted_probeHashTable);
    DEFINE_BLOCK(bb_ret, jitted_probeHashTable);

    /* entry basic block */
    /* get the number of rows of this batch : nrows = batch->m_rows */
    tmpval = builder.CreateInBoundsGEP(batch, Vals2);
    llvm::Value* nRows = builder.CreateLoad(int32Type, tmpval, "nrows");
    tmpval = builder.CreateICmpSGT(nRows, int32_0);
    builder.CreateCondBr(tmpval, bb_pre_loop, bb_ret);

    /* enter into bb_pre_loopEntry to get all the needed info. */
    builder.SetInsertPoint(bb_pre_loop);

    /* get hashBasedOperator from HashJoinTbl */
    Vals2[1] = int32_0;
    llvm::Value* hashBasedOperator = builder.CreateInBoundsGEP(hashJoinTbl, Vals2);
    hashBasedOperator = builder.CreateBitCast(hashBasedOperator, hashBasedOperatorPtrType);

    /* get hashBasedOperator.m_hashTbl */
    Vals2[1] = int32_pos_hBOper_htbl;
    llvm::Value* hashTbl = builder.CreateInBoundsGEP(hashBasedOperator, Vals2);
    hashTbl = builder.CreateLoad(vechashtablePtrType, hashTbl, "m_hashTbl");

    /* get m_hashTbl->m_size */
    Vals2[1] = int32_pos_hashtbl_size;
    mask = builder.CreateInBoundsGEP(hashTbl, Vals2);
    mask = builder.CreateLoad(int32Type, mask, "m_size");
    mask = builder.CreateSub(mask, int32_1);

    /* get m_arr from the batch */
    Vals2[1] = int32_pos_batch_marr;
    llvm::Value* m_arr = builder.CreateInBoundsGEP(batch, Vals2);
    m_arr = builder.CreateLoad(scalarVecPtrType, m_arr, "m_arr");

    /* get HashJoinTbl.m_match */
    Vals2[1] = int32_pos_hjointbl_match;
    llvm::Value* m_match_addr = builder.CreateInBoundsGEP(hashJoinTbl, Vals2, "m_match");
    llvm::Value* int64_nRows = builder.CreateZExt(nRows, int64Type);
    builder.CreateMemSet(m_match_addr, int8_0, int64_nRows, (llvm::MaybeAlign)1);

    /* get hashBasedOperator.m_keyMatch */
    Vals2[0] = int64_0;
    Vals2[1] = int32_pos_hBOper_keyMatch;
    llvm::Value* m_keyMatch_addr = builder.CreateInBoundsGEP(hashBasedOperator, Vals2, "m_keyMatch");
    builder.CreateMemSet(m_keyMatch_addr, int8_1, int64_nRows, (llvm::MaybeAlign)1);

    ListCell* lc_outer = NULL;
    int keyIdx_var = 0;
    Var* variable = NULL;
    ExprState* exprState = NULL;
    Var** outer_var = (Var**)palloc(sizeof(Var*) * num_hashkey);
    llvm::Value** outer_keyIdx_array = (llvm::Value**)palloc(sizeof(llvm::Value*) * num_hashkey);
    /* loop over all the left hashkeys to preload all the variables */
    j = 0;
    foreach (lc_outer, node->hj_OuterHashKeys) {
        /* get outer keyIdx */
        variable = NULL;
        exprState = (ExprState*)lfirst(lc_outer);
        if (IsA(exprState->expr, Var)) {
            variable = (Var*)exprState->expr;
        } else if (IsA(exprState->expr, RelabelType)) {
            RelabelType* reltype = (RelabelType*)exprState->expr;
            if (IsA(reltype->arg, Var) && ((Var*)reltype->arg)->varattno > 0)
                variable = (Var*)((RelabelType*)exprState->expr)->arg;
        }

        Assert(variable != NULL);

        keyIdx_var = variable->varattno - 1;
        outer_var[j] = variable;

        DEFINE_CGVAR_INT64(int64_keyIdx, (long long)keyIdx_var);
        outer_keyIdx_array[j] = int64_keyIdx;
        j++;
    }
    builder.CreateBr(bb_checkFlag[0]);

    /* Beginning of the loop */
    builder.SetInsertPoint(bb_checkFlag[0]);
    llvm::PHINode* Phi_Idx = builder.CreatePHI(int32Type, 2);
    Phi_Idx->addIncoming(int32_0, bb_pre_loop);

    /* defined for multiple hash keys */
    llvm::PHINode* Phi_hash_result = NULL;
    llvm::Value* prev_hash_value = NULL;
    for (j = 0; j < num_hashkey; j++) {
        /* bb_checkFlag */
        builder.SetInsertPoint(bb_checkFlag[j]);
        if (j > 0) {
            Phi_hash_result = builder.CreatePHI(int32Type, 2);
            Phi_hash_result->addIncoming(prev_hash_value, bb_checkFlag[j - 1]);
            Phi_hash_result->addIncoming(hash_result, bb_hashing[j - 1]);
            prev_hash_value = Phi_hash_result;
        } else
            prev_hash_value = int32_0;

        /* get the flag of hash key from the batch : pVector[m_outkeyIdx].flag */
        Vals2[0] = outer_keyIdx_array[j];
        Vals2[1] = int32_pos_scalvec_flag;
        flag = builder.CreateInBoundsGEP(m_arr, Vals2);
        flag = builder.CreateLoad(int8PtrType, flag, "flag");
        flag = builder.CreateInBoundsGEP(flag, Phi_Idx);
        flag = builder.CreateLoad(int8Type, flag, "flag_i");
        flag = builder.CreateAnd(flag, int8_1);
        tmpval = builder.CreateICmpEQ(flag, int8_0);

        if (j == num_hashkey - 1)
            builder.CreateCondBr(tmpval, bb_hashing[j], bb_loopEnd);
        else
            builder.CreateCondBr(tmpval, bb_hashing[j], bb_checkFlag[j + 1]);

        /* value is not null, do hashing */
        builder.SetInsertPoint(bb_hashing[j]);
        /* get the value of hash key from the batch : pVector[m_outkeyIdx].val */
        Vals2[1] = int32_pos_scalvec_vals;
        val = builder.CreateInBoundsGEP(m_arr, Vals2);
        val = builder.CreateLoad(int64PtrType, val, "val");
        val = builder.CreateInBoundsGEP(val, Phi_Idx);
        val = builder.CreateLoad(int64Type, val, "val_i");

        /* do hashing according to the hashkey type */
        variable = outer_var[j];
        switch (variable->vartype) {
            case INT4OID: {
                val = builder.CreateTrunc(val, int32Type);
                llvm_crc32_32_32(hash_result, prev_hash_value, val);
            } break;
            case INT8OID: {
                llvm_crc32_32_64(hash_result, prev_hash_value, val);
            } break;
            case BPCHAROID: {
                int len = variable->vartypmod - 4;
                llvm::Function* func_evalvar = llvmCodeGen->module()->getFunction("JittedEvalVarlena");
                if (func_evalvar == NULL) {
                    func_evalvar = VarlenaCvtCodeGen();
                }
                llvm::Value* res = builder.CreateCall(func_evalvar, val, "func_evalvar");
                llvm::Value* data1 = builder.CreateExtractValue(res, 1);
                llvm::Value* data1_int64 = builder.CreatePtrToInt(data1, int64Type);

                llvm::Value* pval = NULL;
                llvm::Value* hash_res1 = prev_hash_value;
                if (len >= 8) {
                    while (len >= 8) {
                        pval = builder.CreateIntToPtr(data1_int64, int64PtrType);
                        pval = builder.CreateLoad(int64Type, pval, "int64data");
                        llvm_crc32_32_64(hash_res1, hash_res1, pval);
                        data1_int64 = builder.CreateAdd(data1_int64, int64_8);
                        len = len - 8;
                    }
                }

                if (len >= 4) {
                    pval = builder.CreateIntToPtr(data1_int64, int32PtrType);
                    pval = builder.CreateLoad(int32Type, pval, "int32data");
                    llvm_crc32_32_32(hash_res1, hash_res1, pval);
                    data1_int64 = builder.CreateAdd(data1_int64, int64_4);
                    len = len - 4;
                }

                if (len >= 2) {
                    pval = builder.CreateIntToPtr(data1_int64, int16PtrType);
                    pval = builder.CreateLoad(int16Type, pval, "int16data");
                    llvm_crc32_32_16(hash_res1, hash_res1, pval);
                    data1_int64 = builder.CreateAdd(data1_int64, int64_2);
                    len = len - 2;
                }

                if (len == 1) {
                    pval = builder.CreateIntToPtr(data1_int64, int8PtrType);
                    pval = builder.CreateLoad(int8Type, pval, "int8data");
                    llvm_crc32_32_8(hash_res1, hash_res1, pval);
                }
                hash_result = hash_res1;
            } break;
            default: {
                ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmodule(MOD_LLVM),
                        errmsg("Codegen probeHashTable failed: unsupported data type!\n")));
            }
        }

        if (j == num_hashkey - 1)
            builder.CreateBr(bb_loopEnd);
        else
            builder.CreateBr(bb_checkFlag[j + 1]);
    }

    builder.SetInsertPoint(bb_loopEnd);
    llvm::PHINode* Phi_result = builder.CreatePHI(int32Type, 2);
    Phi_result->addIncoming(hash_result, bb_hashing[num_hashkey - 1]);
    Phi_result->addIncoming(prev_hash_value, bb_checkFlag[num_hashkey - 1]);

    tmpval = builder.CreateAnd(Phi_result, mask);
    llvm::Value* bucketIdx = builder.CreateZExt(tmpval, int64Type);

    /* get hashBasedOperator.m_cacheLoc[i] */
    Vals3[0] = int64_0;
    Vals3[1] = int32_pos_hBOper_cacheLoc;
    Vals3[2] = Phi_Idx;
    llvm::Value* m_cacheLoc_idx = builder.CreateInBoundsGEP(hashBasedOperator, Vals3);

    builder.CreateStore(bucketIdx, m_cacheLoc_idx);

    /* get vechashtable.m_data */
    Vals2[0] = int64_0;
    Vals2[1] = int32_pos_hashtbl_data;
    llvm::Value* m_data = builder.CreateInBoundsGEP(hashTbl, Vals2);
    m_data = builder.CreateLoad(hashCellPtrPtrType, m_data, "m_data");

    llvm::Value* m_data_idx = builder.CreateInBoundsGEP(m_data, bucketIdx);
    llvm::Value* lastCell = builder.CreateLoad(hashCellPtrType, m_data_idx, "lastCell");

    /* get hashBasedOperator.m_cellCache[i] */
    Vals3[0] = int64_0;
    Vals3[1] = int32_pos_hBOper_cellCache;
    Vals3[2] = Phi_Idx;
    llvm::Value* m_cellCache_idx = builder.CreateInBoundsGEP(hashBasedOperator, Vals3);

    builder.CreateStore(lastCell, m_cellCache_idx);

    /* increase idx */
    llvm::Value* nextIdx = builder.CreateAdd(Phi_Idx, int32_1);
    Phi_Idx->addIncoming(nextIdx, bb_loopEnd);
    tmpval = builder.CreateICmpEQ(nextIdx, nRows);
    builder.CreateCondBr(tmpval, bb_ret, bb_checkFlag[0]);

    builder.SetInsertPoint(bb_ret);
    builder.CreateRetVoid();

    pfree_ext(outer_keyIdx_array);
    pfree_ext(outer_var);
    pfree_ext(bb_checkFlag);
    pfree_ext(bb_hashing);

    /* finalize the jitted function */
    llvmCodeGen->FinalizeFunction(jitted_probeHashTable, node->js.ps.plan->plan_node_id);

    return jitted_probeHashTable;
}

llvm::Function* VecHashJoinCodeGen::HashJoinCodeGen_bf_includeLong(VecHashJoinState* node)
{
    Assert(NULL != (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj);
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    /* Find and load the IR file from the installaion directory */
    llvmCodeGen->loadIRFile();

    /* Get LLVM Context and builder */
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);
    llvm::Module* mod = llvmCodeGen->module();

    DEFINE_CG_TYPE(int1Type, BITOID);
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int64PtrType, INT8OID);
    DEFINE_CG_PTRTYPE(bFImplPtrType, "class.filter::BloomFilterImpl");
    DEFINE_CG_PTRTYPE(bitSetPtrType, "class.BitSet");

    DEFINE_CGVAR_INT8(int8_0, 0);
    DEFINE_CGVAR_INT8(int8_1, 1);
    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT32(int32_2, 2);
    DEFINE_CGVAR_INT32(int32_3, 3);
    DEFINE_CGVAR_INT32(int32_4, 4);
    DEFINE_CGVAR_INT32(int32_8, 8);
    DEFINE_CGVAR_INT32(int32_10, 10);
    DEFINE_CGVAR_INT64(int64_0, 0);
    DEFINE_CGVAR_INT64(int64_1, 1);

    llvm::Function* jitted_bf_inlLong = NULL;
    llvm::Value* llvmargs[2];
    llvm::Value* tmpval = NULL;
    llvm::Value* idx_next = NULL;
    llvm::Value* pos = int32_0;

    /* llvm array values, used to represent the location of some element */
    llvm::Value* Vals[2] = {int64_0, int32_0};

    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "JittedBloomFilterIncLong", int8Type);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("BFImpl", bFImplPtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("value", int64Type));
    jitted_bf_inlLong = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* bfimpl = llvmargs[0];
    llvm::Value* inputval = llvmargs[1];

    /* define basic block information for batch loop */
    llvm::BasicBlock* entry = &jitted_bf_inlLong->getEntryBlock();
    DEFINE_BLOCK(nohash_bb, jitted_bf_inlLong);
    DEFINE_BLOCK(evalhash_bb, jitted_bf_inlLong);
    DEFINE_BLOCK(for_inc, jitted_bf_inlLong);
    DEFINE_BLOCK(end_bb, jitted_bf_inlLong);

    builder.SetInsertPoint(entry);
    /* Get all the parameters that we needed */
    Vals[1] = int32_2;
    llvm::Value* numBits = builder.CreateInBoundsGEP(bfimpl, Vals);
    numBits = builder.CreateLoad(int64Type, numBits, "numBits");
    Vals[1] = int32_3;
    llvm::Value* numHashFunction = builder.CreateInBoundsGEP(bfimpl, Vals);
    numHashFunction = builder.CreateLoad(int64Type, numHashFunction, "numHashFunc");
    Vals[1] = int32_4;
    llvm::Value* numValues = builder.CreateInBoundsGEP(bfimpl, Vals);
    numValues = builder.CreateLoad(int64Type, numValues, "numValues");
    Vals[1] = int32_8;
    llvm::Value* minValue = builder.CreateInBoundsGEP(bfimpl, Vals);
    minValue = builder.CreateLoad(int64Type, minValue, "minValue");
    Vals[1] = int32_10;
    llvm::Value* addMinMax = builder.CreateInBoundsGEP(bfimpl, Vals);
    addMinMax = builder.CreateLoad(int8Type, addMinMax, "addMinMax");

    /* Compute addMinMax && (1 == numValues) */
    addMinMax = builder.CreateTrunc(addMinMax, int1Type);
    tmpval = builder.CreateICmpEQ(numValues, int64_1);
    tmpval = builder.CreateAnd(tmpval, addMinMax);
    builder.CreateCondBr(tmpval, nohash_bb, evalhash_bb);

    /* Corresponding to ret = (value == minValue) */
    builder.SetInsertPoint(nohash_bb);
    llvm::Value* res1 = builder.CreateICmpEQ(inputval, minValue);
    res1 = builder.CreateZExt(res1, int8Type);
    builder.CreateBr(end_bb);

    /*
     * Begin to compute includeHashInternal(getLongHash(value)),
     * defferent from the original function, we use CRC32 in LLVM
     * to replace the original Thomas Wang's integer hash function.
     */
    builder.SetInsertPoint(evalhash_bb);
    llvm::PHINode* phi_idx = builder.CreatePHI(int32Type, 2);

    idx_next = builder.CreateAdd(phi_idx, int32_1);
    phi_idx->addIncoming(int32_1, entry);
    phi_idx->addIncoming(idx_next, for_inc);

    /*
     * Compute hashval and combinedHash = hv1 + hv2, then get the pos:
     * pos = combinedHash % numBits;
     */
    llvm::Value* hashval1 = NULL;
    llvm_crc32_32_64(hashval1, phi_idx, inputval);
    llvm::Value* hashval2 = NULL;
    llvm_crc32_32_64(hashval2, hashval1, inputval);
    llvm::Value* combinedHash = builder.CreateAdd(hashval1, hashval2);
    combinedHash = builder.CreateSExt(combinedHash, int64Type);
    pos = builder.CreateURem(combinedHash, numBits, "pos");

    /* corresponding to bitSet->get(pos) */
    Vals[1] = int32_1;
    llvm::Value* bitset = builder.CreateInBoundsGEP(bfimpl, Vals);
    bitset = builder.CreateLoad(bitSetPtrType, bitset, "bitset");
    llvm::Value* data = builder.CreateInBoundsGEP(bitset, Vals);
    data = builder.CreateLoad(int64PtrType, data, "data");
    llvm::Value* index1 = builder.CreateAShr(pos, 6);
    data = builder.CreateInBoundsGEP(data, index1);
    data = builder.CreateLoad(int64Type, data, "fdata");
    llvm::Value* index2 = builder.CreateShl(int64_1, pos);
    tmpval = builder.CreateAnd(data, index2);
    tmpval = builder.CreateICmpNE(tmpval, int64_0);
    builder.CreateCondBr(tmpval, for_inc, end_bb);

    builder.SetInsertPoint(for_inc);
    numHashFunction = builder.CreateTrunc(numHashFunction, int32Type);
    tmpval = builder.CreateICmpSGT(idx_next, numHashFunction);
    builder.CreateCondBr(tmpval, end_bb, evalhash_bb);

    builder.SetInsertPoint(end_bb);
    llvm::PHINode* phi_ret = builder.CreatePHI(int8Type, 3);
    phi_ret->addIncoming(res1, nohash_bb);
    phi_ret->addIncoming(int8_0, evalhash_bb);
    phi_ret->addIncoming(int8_1, for_inc);
    builder.CreateRet(phi_ret);

    llvmCodeGen->FinalizeFunction(jitted_bf_inlLong, node->js.ps.plan->plan_node_id);

    return jitted_bf_inlLong;
}

llvm::Function* VecHashJoinCodeGen::HashJoinCodeGen_bf_addLong(VecHashJoinState* node)
{
    Assert(NULL != (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj);
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    /* Find and load the IR file from the installaion directory */
    llvmCodeGen->loadIRFile();

    /* Get LLVM Context and builder */
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);
    llvm::Module* mod = llvmCodeGen->module();

    DEFINE_CG_VOIDTYPE(voidType);
    DEFINE_CG_TYPE(int16Type, INT2OID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int64PtrType, INT8OID);
    DEFINE_CG_PTRTYPE(bFImplPtrType, "class.filter::BloomFilterImpl");
    DEFINE_CG_PTRTYPE(valBitPtrType, "struct.ValueBit");
    DEFINE_CG_PTRTYPE(bitSetPtrType, "class.BitSet");

    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(int32_1, 1);
    DEFINE_CGVAR_INT32(int32_2, 2);
    DEFINE_CGVAR_INT32(int32_3, 3);
    DEFINE_CGVAR_INT32(int32_4, 4);
    DEFINE_CGVAR_INT32(int32_5, 5);
    DEFINE_CGVAR_INT32(int32_6, 6);
    DEFINE_CGVAR_INT32(int32_7, 7);
    DEFINE_CGVAR_INT64(int64_0, 0);
    DEFINE_CGVAR_INT64(int64_1, 1);

    llvm::Function* jitted_bf_addLong = NULL;
    llvm::Value* llvmargs[2];
    llvm::Value* tmpval = NULL;
    llvm::Value* idx_next = NULL;
    llvm::Value* pos = int32_0;

    /* llvm array values, used to represent the location of some element */
    llvm::Value* Vals[2] = {int64_0, int32_0};
    llvm::Value* Vals3[3] = {int64_0, int32_0, int32_0};

    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "JittedBloomFilterAddLong", voidType);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("BFImpl", bFImplPtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("value", int64Type));
    jitted_bf_addLong = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    llvm::Value* bfimpl = llvmargs[0];
    llvm::Value* inputval = llvmargs[1];

    /* define basic block information for batch loop */
    llvm::BasicBlock* entry = &jitted_bf_addLong->getEntryBlock();
    DEFINE_BLOCK(do_error, jitted_bf_addLong);
    DEFINE_BLOCK(chk_realloc, jitted_bf_addLong);
    DEFINE_BLOCK(do_realloc, jitted_bf_addLong);
    DEFINE_BLOCK(for_body, jitted_bf_addLong);
    DEFINE_BLOCK(for_inc, jitted_bf_addLong);
    DEFINE_BLOCK(for_end, jitted_bf_addLong);

    builder.SetInsertPoint(entry);
    /* Get all the parameters that we needed */
    Vals[1] = int32_2;
    llvm::Value* numBits = builder.CreateInBoundsGEP(bfimpl, Vals);
    numBits = builder.CreateLoad(int64Type, numBits, "numBits");
    Vals[1] = int32_3;
    llvm::Value* num_HashFunction = builder.CreateInBoundsGEP(bfimpl, Vals);
    num_HashFunction = builder.CreateLoad(int64Type, num_HashFunction, "numHashFunc");
    num_HashFunction = builder.CreateTrunc(num_HashFunction, int32Type);
    Vals[1] = int32_4;
    llvm::Value* ptrnumValues = builder.CreateInBoundsGEP(bfimpl, Vals);
    llvm::Value* numValues = builder.CreateLoad(int64Type, ptrnumValues, "numValues");
    Vals[1] = int32_5;
    llvm::Value* val_numMaxValues = builder.CreateInBoundsGEP(bfimpl, Vals);
    val_numMaxValues = builder.CreateLoad(int64Type, val_numMaxValues, "maxNumValues");
    Vals[1] = int32_6;
    llvm::Value* ptrvalstartup = builder.CreateInBoundsGEP(bfimpl, Vals);
    llvm::Value* val_startup = builder.CreateLoad(int64Type, ptrvalstartup, "startupEntries");
    Vals[1] = int32_7;
    llvm::Value* valpos = builder.CreateInBoundsGEP(bfimpl, Vals);
    valpos = builder.CreateLoad(valBitPtrType, valpos, "valuePositions");

    /* Report error when numValues >= maxNumValues */
    tmpval = builder.CreateICmpSGE(numValues, val_numMaxValues);
    builder.CreateCondBr(tmpval, do_error, chk_realloc);
    builder.SetInsertPoint(do_error);
    const char* errorstr = "Add too many values to the bloom filter.";
    DebugerCodeGen::CodeGenElogInfo(&builder, ERROR, errorstr);
    builder.CreateBr(chk_realloc);

    /* Check if numValues >= startupEntries, repalloc valuePositions when true */
    builder.SetInsertPoint(chk_realloc);
    tmpval = builder.CreateICmpSGE(numValues, val_startup);
    builder.CreateCondBr(tmpval, do_realloc, for_body);

    builder.SetInsertPoint(do_realloc);
    llvm::Function* func_pos_repalloc = llvmCodeGen->module()->getFunction("WrapvalPosRepalloc");
    if (NULL == func_pos_repalloc) {
        GsCodeGen::FnPrototype func_prototype(llvmCodeGen, "WrapvalPosRepalloc", voidType);
        func_prototype.addArgument(GsCodeGen::NamedVariable("bfimpl", bFImplPtrType));
        func_pos_repalloc = func_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("WrapvalPosRepalloc", (void*)WrapposRepalloc);
    }
    builder.CreateCall(func_pos_repalloc, bfimpl);
    builder.CreateBr(for_body);

    /*
     * Defferent from the original function, we use CRC32 in LLVM
     * to replace the original Thomas Wang's integer hash function.
     */
    builder.SetInsertPoint(for_body);
    llvm::PHINode* phi_idx = builder.CreatePHI(int32Type, 3);

    idx_next = builder.CreateAdd(phi_idx, int32_1);
    phi_idx->addIncoming(int32_1, chk_realloc);
    phi_idx->addIncoming(int32_1, do_realloc);
    phi_idx->addIncoming(idx_next, for_inc);

    /*
     * Compute hashval and combinedHash = hv1 + hv2, then get the pos:
     * pos = combinedHash % numBits;
     */
    llvm::Value* hashval1 = NULL;
    llvm_crc32_32_64(hashval1, phi_idx, inputval);
    llvm::Value* hashval2 = NULL;
    llvm_crc32_32_64(hashval2, hashval1, inputval);
    llvm::Value* combinedHash = builder.CreateAdd(hashval1, hashval2);
    combinedHash = builder.CreateSExt(combinedHash, int64Type);
    pos = builder.CreateURem(combinedHash, numBits, "pos");

    /* corresponding to bitSet->set(pos) */
    Vals[1] = int32_1;
    llvm::Value* bitset = builder.CreateInBoundsGEP(bfimpl, Vals);
    bitset = builder.CreateLoad(bitSetPtrType, bitset, "bitset");
    llvm::Value* data = builder.CreateInBoundsGEP(bitset, Vals);
    data = builder.CreateLoad(int64PtrType, data, "data");
    llvm::Value* index1 = builder.CreateAShr(pos, 6);
    llvm::Value* pdata = builder.CreateInBoundsGEP(data, index1);
    llvm::Value* realdata = builder.CreateLoad(int64Type, pdata, "pdata");
    llvm::Value* index2 = builder.CreateShl(int64_1, pos);
    realdata = builder.CreateOr(realdata, index2);
    builder.CreateStore(realdata, pdata);

    /* corresponding to valuePositions[numValues].position[i - 1] = pos */
    Vals3[0] = numValues;
    Vals3[1] = int32_0;
    Vals3[2] = builder.CreateSub(phi_idx, int32_1);
    llvm::Value* position = builder.CreateInBoundsGEP(valpos, Vals3);
    llvm::Value* idx = builder.CreateTrunc(pos, int16Type);
    builder.CreateStore(idx, position);
    builder.CreateBr(for_inc);

    builder.SetInsertPoint(for_inc);
    tmpval = builder.CreateICmpSGT(idx_next, num_HashFunction);
    builder.CreateCondBr(tmpval, for_end, for_body);

    builder.SetInsertPoint(for_end);
    numValues = builder.CreateAdd(numValues, int64_1);
    builder.CreateStore(numValues, ptrnumValues);
    builder.CreateRetVoid();

    llvmCodeGen->FinalizeFunction(jitted_bf_addLong, node->js.ps.plan->plan_node_id);

    return jitted_bf_addLong;
}
}  // namespace dorado

/*
 * @Description : We could load notnull information from base relation, while if the
 *				  table is not located at the subplan of inner join, we could not
 *				  promise it.
 *
 * @in node		: Input node state.
 * @return		: The output variable cound be null when ture.
 */
bool NULL_possible(PlanState* node)
{
    bool result = false;

    if (IsA(node, VecHashJoinState) || IsA(node, VecNestLoopState) || IsA(node, VecMergeJoinState)) {
        if (((VecHashJoinState*)node)->js.jointype != JOIN_INNER)
            return true;
    }

    PlanState* inner_node = innerPlanState(node);
    PlanState* outer_node = outerPlanState(node);
    if (inner_node != NULL)
        result = NULL_possible(inner_node);
    if (result == false && outer_node != NULL)
        result = NULL_possible(outer_node);
    return result;
}

/* @Description	: Wraper function for palloc, when we do codegen on buildHashTable, if
 *				   NeedCopy is true, we need to palloc copyCellArray.
 * @in size		: The size of memory that we need.
 * @return		: The pointer point to the alloced context.
 */
char* Wrappalloc(int32 size)
{
    return (char*)palloc0(size);
}

/*
 * @Description 	: Realloc valuepos when numValues >= startupEntries.
 * @in  bfi		: The BloomFilter info struct that we used in HashJoin
 */
void WrapposRepalloc(filter::BloomFilterImpl<int64>* bfi)
{
    AutoContextSwitch memGuard(bfi->context);
    bfi->startupEntries = Min(bfi->startupEntries * 2, bfi->maxNumValues);
    bfi->valuePositions = (ValueBit*)repalloc(bfi->valuePositions, bfi->startupEntries * sizeof(ValueBit));
}
