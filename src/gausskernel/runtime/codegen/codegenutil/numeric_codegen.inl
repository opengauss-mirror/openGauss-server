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
 * numeric_codegen.inl
 *    template implementation of numeric codegen.
 *
 * IDENTIFICATION
 *    src/gausskernel/runtime/codegen/codegenutil/numeric_codegen.inl
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef NUMERICCODEGEN_INL
#define NUMERICCODEGEN_INL

#include "codegen/gscodegen.h"
#include "codegen/builtinscodegen.h"
#include "codegen/codegendebuger.h"

#include "utils/biginteger.h"

extern const int64 ScaleMultipler[20];
extern const int64 Int64MultiOutOfBound[20];

namespace dorado
{
	/* declare llvm function with two arguments */
	#define DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, func_type, name, arg1, arg1Type, arg2, arg2Type)\
			GsCodeGen::FnPrototype func_type(llvmCodeGen, name, int64Type);\
			(func_type).addArgument(GsCodeGen::NamedVariable(arg1, arg1Type));\
			(func_type).addArgument(GsCodeGen::NamedVariable(arg2, arg2Type));\
			jitted_funptr = (func_type).generatePrototype(&builder, &llvmargs[0]);\

	/**
	 * @Description : Generate IR function to codegen numeric operation, which includes
	 *				  +, - , *, /, =, !=, <, <=, >, >=. 'lhs_arg' and 'rhs_arg' are the 
	 *				  parameters used by LLVM function. Since in most cases, numeric 
	 *				  data can be stored in BI64 data type, we codegen all the operations
	 *				  for bi64 type.
	 * @return      : LLVM IR Function that point to the special codegened operation func.
	 */
	template<biop op>
	llvm::Function *numeric_sop_codegen()
	{
		GsCodeGen *llvmCodeGen = (GsCodeGen *)t_thrd.codegen_cxt.thr_codegen_obj;
		llvm::LLVMContext& context = llvmCodeGen->context();
		GsCodeGen::LlvmBuilder builder(context);

		/* Define the datatype and variables that needed */
		DEFINE_CG_TYPE(int16Type, INT2OID);
		DEFINE_CG_TYPE(int32Type, INT4OID);
		DEFINE_CG_TYPE(int64Type, INT8OID);
		DEFINE_CG_TYPE(FuncCallInfoType, "struct.FunctionCallInfoData");

		DEFINE_CGVAR_INT16(val_mask, NUMERIC_BI_MASK);
		DEFINE_CGVAR_INT16(val_nan, NUMERIC_NAN);
		DEFINE_CGVAR_INT16(val_binum128, NUMERIC_128);
		DEFINE_CGVAR_INT32(int32_0, 0);
		DEFINE_CGVAR_INT32(int32_1, 1);
		DEFINE_CGVAR_INT32(int32_6, 6);
		DEFINE_CGVAR_INT64(int64_0, 0);
		DEFINE_CGVAR_INT64(int64_1, 1);

		llvm::Value *llvmargs[2];
		llvm::Value *tmpval = NULL;
		llvm::Value *res1 = NULL, *res2 = NULL, *res3 = NULL;
		llvm::Value *oparg1 = NULL, *oparg2 = NULL;

		llvm::Value *Vals[2] = {int64_0, int64_0};
		llvm::Value *Vals4[4] = {int64_0, int32_0, int32_0, int32_0};

		/* the jitted function pointer */
		llvm::Function *jitted_numfuncptr = NULL;

		/* Define the right jitted function through op type and left arg type. */
		switch (op)
		{
			case BIADD:
			{
				DEFINE_FUNCTION_WITH_2_ARGS(jitted_numfuncptr, ADD, "Jitted_numericadd", 
										"arg1", int64Type, "arg2", int64Type);
			}
				break;
			case BISUB:
			{
				DEFINE_FUNCTION_WITH_2_ARGS(jitted_numfuncptr, SUB, "Jitted_numericsub", 
										"arg1", int64Type, "arg2", int64Type);
			}
				break;
			case BIMUL:
			{
				DEFINE_FUNCTION_WITH_2_ARGS(jitted_numfuncptr, MUL, "Jitted_numericmul", 
										"arg1", int64Type, "arg2", int64Type);
			}
				break;
			case BIDIV:
			{
				DEFINE_FUNCTION_WITH_2_ARGS(jitted_numfuncptr, DIV, "Jitted_numericdiv", 
										"arg1", int64Type, "arg2", int64Type);
			}
				break;
			case BIEQ:
			{
				DEFINE_FUNCTION_WITH_2_ARGS(jitted_numfuncptr, EQ, "Jitted_numericeq", 
										"arg1", int64Type, "arg2", int64Type);
			}
				break;
			case BINEQ:
			{
				DEFINE_FUNCTION_WITH_2_ARGS(jitted_numfuncptr, NEQ, "Jitted_numericne",
										"arg1", int64Type, "arg2", int64Type);
			}
				break;
			case BILE:
			{
				DEFINE_FUNCTION_WITH_2_ARGS(jitted_numfuncptr, LE, "Jitted_numericle", 
										"arg1", int64Type, "arg2", int64Type);
			}
				break;
			case BILT:
			{
				DEFINE_FUNCTION_WITH_2_ARGS(jitted_numfuncptr, LT, "Jitted_numericlt", 
										"arg1", int64Type, "arg2", int64Type);
			}
				break;
			case BIGE:
			{
				DEFINE_FUNCTION_WITH_2_ARGS(jitted_numfuncptr, GE, "Jitted_numericge", 
										"arg1", int64Type, "arg2", int64Type);
			}
				break;
			case BIGT:
			{
				DEFINE_FUNCTION_WITH_2_ARGS(jitted_numfuncptr, GT, "Jitted_numericgt", 
										"arg1", int64Type, "arg2", int64Type);
			}
				break;
			default:
				break;
		}

		llvm::Value *arg1 = llvmargs[0];
		llvm::Value *arg2 = llvmargs[1];

		llvm::BasicBlock *entry = &jitted_numfuncptr->getEntryBlock();
		DEFINE_BLOCK(bi_numeric_op, jitted_numfuncptr);
		DEFINE_BLOCK(org_numeric_op, jitted_numfuncptr);
		DEFINE_BLOCK(bi64_op, jitted_numfuncptr);
		DEFINE_BLOCK(bi128_op, jitted_numfuncptr);
		DEFINE_BLOCK(ret_bb, jitted_numfuncptr);

		builder.SetInsertPoint(entry);
		/* get BI numeric val from datum */
		llvm::Value *leftarg = DatumGetBINumericCodeGen(&builder, arg1);
		llvm::Value *rightarg = DatumGetBINumericCodeGen(&builder, arg2);

		/* get numFlags : NUMERIC_NB_FLAGBITS(arg) */
		Vals4[0] = int64_0;
		Vals4[1] = int32_1;
		Vals4[2] = int32_0;
		Vals4[3] = int32_0;
		tmpval = builder.CreateInBoundsGEP(leftarg, Vals4);
		tmpval = builder.CreateLoad(int16Type, tmpval, "lheader");
		llvm::Value *lnumFlags = builder.CreateAnd(tmpval, val_mask);

		tmpval = builder.CreateInBoundsGEP(rightarg, Vals4);
		tmpval = builder.CreateLoad(int16Type, tmpval, "rheader");
		llvm::Value *rnumFlags = builder.CreateAnd(tmpval, val_mask);

		/* check : NUMERIC_FLAG_IS_BI(numFlags) */
		llvm::Value *cmp1 = builder.CreateICmpUGT(lnumFlags, val_nan);
		llvm::Value *cmp2 = builder.CreateICmpUGT(rnumFlags, val_nan);
		llvm::Value *cmpand  = builder.CreateAnd(cmp1, cmp2);
		builder.CreateCondBr(cmpand, bi_numeric_op, org_numeric_op);

		/*
		 * when arguments can be respented by BI numeric, further check
		 * their if it is bi64 or bi128.
		 */
		builder.SetInsertPoint(bi_numeric_op);
		oparg1 = builder.CreateICmpEQ(lnumFlags, val_binum128);
		oparg2 = builder.CreateICmpEQ(rnumFlags, val_binum128);
		llvm::Value *cmpor = builder.CreateOr(oparg1, oparg2);
		builder.CreateCondBr(cmpor, bi128_op, bi64_op);

		builder.SetInsertPoint(bi64_op);
		llvm::Function *jitted_func = NULL;
		switch (op)
		{
			case BIADD:
			{
				jitted_func = llvmCodeGen->module()->getFunction("Jitted_bi64add64s");
				if (jitted_func == NULL)
					jitted_func = bi64add64_codegen(false);
			}
				break;
			case BISUB:
			{
				jitted_func = llvmCodeGen->module()->getFunction("Jitted_bi64sub64");
				if (jitted_func == NULL)
					jitted_func = bi64sub64_codegen();
			}
				break;
			case BIMUL:
			{
				jitted_func = llvmCodeGen->module()->getFunction("Jitted_bi64mul64");
				if (jitted_func == NULL)
					jitted_func = bi64mul64_codegen();
			}
				break;
			case BIDIV:
			{
				jitted_func = llvmCodeGen->module()->getFunction("Jitted_bi64div64");
				if (jitted_func == NULL)
					jitted_func = bi64div64_codegen();
			}
				break;
			case BIEQ:
			{
				jitted_func = llvmCodeGen->module()->getFunction("Jitted_bi64eq64");
				if (jitted_func == NULL)
					jitted_func = bi64cmp64_codegen<BIEQ>();
			}
				break;
			case BINEQ:
			{
				jitted_func = llvmCodeGen->module()->getFunction("Jitted_bi64ne64");
				if (jitted_func == NULL)
					jitted_func = bi64cmp64_codegen<BINEQ>();
			}
				break;
			case BILE:
			{
				jitted_func = llvmCodeGen->module()->getFunction("Jitted_bi64le64");
				if (jitted_func == NULL)
					jitted_func = bi64cmp64_codegen<BILE>();
			}
				break;
			case BILT:
			{
				jitted_func = llvmCodeGen->module()->getFunction("Jitted_bi64lt64");
				if (jitted_func == NULL)
					jitted_func = bi64cmp64_codegen<BILT>();
			}
				break;
			case BIGE:
			{
				jitted_func = llvmCodeGen->module()->getFunction("Jitted_bi64ge64");
				if (jitted_func == NULL)
					jitted_func = bi64cmp64_codegen<BIGE>();
			}
				break;
			case BIGT:
			{
				jitted_func = llvmCodeGen->module()->getFunction("Jitted_bi64gt64");
				if (jitted_func == NULL)
					jitted_func = bi64cmp64_codegen<BIGT>();
			}
				break;
			default:
				break;
		}
		res1 = builder.CreateCall(jitted_func, {leftarg, rightarg});
		builder.CreateBr(ret_bb);

		builder.SetInsertPoint(bi128_op);
		oparg1 = builder.CreateZExt(oparg1, int32Type);
		oparg2 = builder.CreateZExt(oparg2, int32Type);
		llvm::Value *cop = llvmCodeGen->getIntConstant(INT4OID, op);
		res2 = WrapBiFunMatrixCodeGen(&builder, cop, oparg1, oparg2, leftarg, rightarg);
		builder.CreateBr(ret_bb);

		/* call original numeric function according to the 'op' */
		builder.SetInsertPoint(org_numeric_op);
		DEFINE_CG_ARRTYPE(int64ArrType, int64Type, 2);
		llvm::Value *finfo = builder.CreateAlloca(FuncCallInfoType);
		llvm::Value *args = builder.CreateAlloca(int64ArrType);
		llvm::Value *arghead = builder.CreateInBoundsGEP(args, Vals);
		Vals[1] = int32_6;
		llvm::Value *fihead = builder.CreateInBoundsGEP(finfo, Vals);
		builder.CreateStore(arghead, fihead);
		oparg1 = builder.CreatePtrToInt(leftarg, int64Type);
		builder.CreateStore(oparg1, arghead);
		oparg2 = builder.CreatePtrToInt(rightarg, int64Type);
		Vals[1] = int64_1;
		tmpval = builder.CreateInBoundsGEP(args, Vals);
		builder.CreateStore(oparg2, tmpval);
		res3 = WrapnumericFuncCodeGen<op>(&builder, finfo);
		builder.CreateBr(ret_bb);

		builder.SetInsertPoint(ret_bb);
		llvm::PHINode *phi_ret = builder.CreatePHI(int64Type, 3);
		phi_ret->addIncoming(res1, bi64_op);
		phi_ret->addIncoming(res2, bi128_op);
		phi_ret->addIncoming(res3, org_numeric_op);

		(void)builder.CreateRet(phi_ret);
		
		llvmCodeGen->FinalizeFunction(jitted_numfuncptr);

		return jitted_numfuncptr;
	}

	/**
	 * @Description : Generate IR function to codegen bi64 operation, which includes
	 *				  =, !=, <, <=, >, >=. 'lhs_arg' and 'rhs_arg' are the 
	 *				  parameters used by LLVM function with numeric data type. 
	 * @return		: LLVM IR Function that point to the special codegened operation func.
	 */
	template<biop op>
	llvm::Function *bi64cmp64_codegen()
	{
		GsCodeGen *llvmCodeGen = (GsCodeGen *)t_thrd.codegen_cxt.thr_codegen_obj;
		llvm::LLVMContext& context = llvmCodeGen->context();
		GsCodeGen::LlvmBuilder builder(context);

		/* Define the datatype and variables that needed */
		DEFINE_CG_TYPE(int16Type, INT2OID);
		DEFINE_CG_TYPE(int32Type, INT4OID);
		DEFINE_CG_TYPE(int64Type, INT8OID);
		DEFINE_CG_NINTTYP(int128Type, 128);
		DEFINE_CG_PTRTYPE(int64PtrType, INT8OID);
		DEFINE_CG_PTRTYPE(numericPtrType, "struct.NumericData");

		DEFINE_CGVAR_INT16(val_scalemask, NUMERIC_BI_SCALEMASK);
		DEFINE_CGVAR_INT1(int1_0, 0);
		DEFINE_CGVAR_INT1(int1_1, 1);
		DEFINE_CGVAR_INT32(int32_0, 0);
		DEFINE_CGVAR_INT32(int32_1, 1);
		DEFINE_CGVAR_INT64(int64_0, 0);

		llvm::Value *llvmargs[2];
		llvm::Value *cmpval = NULL;
		llvm::Value *tmpval = NULL;
		llvm::Value *mulscale = NULL;
		llvm::Value *phi_val = NULL;
		llvm::Value *multi_bound = NULL;
		llvm::Value *left_scaled1= NULL, *left_scaled2 = NULL;
		llvm::Value *right_scaled1 = NULL, *right_scaled2 = NULL;
		llvm::PHINode *left_scaled = NULL, *right_scaled = NULL;
		llvm::Value *res1 = NULL, *res2 = NULL, *res3 = NULL;
		llvm::Value *phi_left = NULL, *phi_right = NULL;
		llvm::Value *Vals4[4] = {int64_0, int32_0, int32_0, int32_0};

		/* the jitted function pointer */
		llvm::Function *jitted_funcptr = NULL;

		/* Define the right jitted function through op type and left arg type. */
		switch (op)
		{
			case BIEQ:
			{
				DEFINE_FUNCTION_WITH_2_ARGS(jitted_funcptr, EQ, "Jitted_bi64eq64", 
										"arg1", numericPtrType, "arg2", numericPtrType);
			}
				break;
			case BINEQ:
			{
				DEFINE_FUNCTION_WITH_2_ARGS(jitted_funcptr, NEQ, "Jitted_bi64ne64",
										"arg1", numericPtrType, "arg2", numericPtrType);
			}
				break;
			case BILE:
			{
				DEFINE_FUNCTION_WITH_2_ARGS(jitted_funcptr, LE, "Jitted_bi64le64", 
										"arg1", numericPtrType, "arg2", numericPtrType);
			}
				break;
			case BILT:
			{
				DEFINE_FUNCTION_WITH_2_ARGS(jitted_funcptr, LT, "Jitted_bi64lt64", 
										"arg1", numericPtrType, "arg2", numericPtrType);
			}
				break;
			case BIGE:
			{
				DEFINE_FUNCTION_WITH_2_ARGS(jitted_funcptr, GE, "Jitted_bi64ge64", 
										"arg1", numericPtrType, "arg2", numericPtrType);	
			}
				break;
			case BIGT:
			{
				DEFINE_FUNCTION_WITH_2_ARGS(jitted_funcptr, GT, "Jitted_bi64gt64", 
										"arg1", numericPtrType, "arg2", numericPtrType);
			}
				break;
			default:
				break;
		}

		llvm::Value *larg = llvmargs[0];
		llvm::Value *rarg = llvmargs[1];

		/* get value scale : NUMERIC_BI_SCALE */
		Vals4[0] = int64_0;
		Vals4[1] = int32_1;
		Vals4[2] = int32_0;
		Vals4[3] = int32_0;
		tmpval = builder.CreateInBoundsGEP(larg, Vals4);
		tmpval = builder.CreateLoad(int16Type, tmpval, "lheader");
		llvm::Value *lvalscale = builder.CreateAnd(tmpval, val_scalemask);
		lvalscale = builder.CreateSExt(lvalscale, int32Type, "lscale");

		tmpval = builder.CreateInBoundsGEP(rarg, Vals4);
		tmpval = builder.CreateLoad(int16Type, tmpval, "rheader");
		llvm::Value *rvalscale = builder.CreateAnd(tmpval, val_scalemask);
		rvalscale = builder.CreateSExt(rvalscale, int32Type, "rscale");

		/* get value : NUMERIC_64VALUE */
		Vals4[3] = int32_1;
		tmpval = builder.CreateInBoundsGEP(larg, Vals4);
		tmpval = builder.CreateBitCast(tmpval, int64PtrType);
		llvm::Value* leftval = builder.CreateLoad(int64Type, tmpval, "lval");

		tmpval = builder.CreateInBoundsGEP(rarg, Vals4);
		tmpval = builder.CreateBitCast(tmpval, int64PtrType);
		llvm::Value* rightval = builder.CreateLoad(int64Type, tmpval, "rval");

		llvm::BasicBlock *entry = &jitted_funcptr->getEntryBlock();
		DEFINE_BLOCK(delta_large, jitted_funcptr);
		DEFINE_BLOCK(delta_small, jitted_funcptr);
		DEFINE_BLOCK(adjust_true, jitted_funcptr);
		DEFINE_BLOCK(left_out_bound, jitted_funcptr);
		DEFINE_BLOCK(right_out_bound, jitted_funcptr);
		DEFINE_BLOCK(ret_bb, jitted_funcptr);

		builder.SetInsertPoint(entry);
		/* delta_scale = lvalscale - rvalscale */
		llvm::Value* delta_scale = builder.CreateSub(lvalscale, rvalscale, "delta_scale");
		/* check delta_scale >= 0 or not */
		cmpval = builder.CreateICmpSGE(delta_scale, int32_0, "cmp_delta_scale");
		builder.CreateCondBr(cmpval, delta_large, delta_small);

		/* corresponding to delta_scale >= 0 */
		builder.SetInsertPoint(delta_large);
		/* tmpval = y < 0 ? y : -y */
		tmpval = builder.CreateSub(int64_0, rightval);
		cmpval = builder.CreateICmpSLT(rightval, int64_0, "negative_cmp");
		phi_val = builder.CreateSelect(cmpval, rightval, tmpval);

		left_scaled1 = leftval;
		mulscale = ScaleMultiCodeGen(&builder, delta_scale);
		/* corresponding to y_scaled = y * ScaleMultipler[delta_scale] */
		right_scaled1 = builder.CreateMul(rightval, mulscale);

		/* the result of tmpval * ScaleMultipler[delta_scale] 
		 * doesn't out of int64 bound.
		 */
		multi_bound = GetInt64MulOutofBoundCodeGen(&builder, delta_scale);
		cmpval = builder.CreateICmpSGE(phi_val, multi_bound, "bound_check");
		builder.CreateCondBr(cmpval, adjust_true, right_out_bound);

		builder.SetInsertPoint(delta_small);
		/* tmpval = x < 0 ? x : -x */
		tmpval = builder.CreateSub(int64_0, leftval);
		cmpval = builder.CreateICmpSLT(leftval, int64_0, "negative_cmp");
		phi_val = builder.CreateSelect(cmpval, leftval, tmpval);

		/* corresponding to x_scaled = x * ScaleMultipler[-delta_scale] */
		llvm::Value* mdelta_scale = builder.CreateSub(int32_0, delta_scale);
		llvm::Value* mmulscale = ScaleMultiCodeGen(&builder, mdelta_scale);
		left_scaled2 = builder.CreateMul(leftval, mmulscale);
		right_scaled2 = rightval;

		/* the result of tmpval * ScaleMultipler[delta_scale] 
		 * doesn't out of int64 bound.
		 */ 
		multi_bound = GetInt64MulOutofBoundCodeGen(&builder, mdelta_scale);
		cmpval = builder.CreateICmpSGE(phi_val, multi_bound, "bound_check");
		builder.CreateCondBr(cmpval, adjust_true, left_out_bound);

		builder.SetInsertPoint(adjust_true);
		left_scaled = builder.CreatePHI(int64Type, 2);
		left_scaled->addIncoming(left_scaled1, delta_large);
		left_scaled->addIncoming(left_scaled2, delta_small);
		
		right_scaled = builder.CreatePHI(int64Type, 2);
		right_scaled->addIncoming(right_scaled1, delta_large);
		right_scaled->addIncoming(right_scaled2, delta_small);
		
		/* compare leftval with rightval directly, both are int64 type */
		switch (op)
		{
			case BIEQ:
			{
				res1 = builder.CreateICmpEQ(left_scaled, right_scaled, "bi64_eq");
			}
				break;
			case BINEQ:
			{
				res1 = builder.CreateICmpNE(left_scaled, right_scaled, "bi64_ne");
			}
				break;
			case BILE:
			{
				res1 = builder.CreateICmpSLE(left_scaled, right_scaled, "bi64_le");
			}
				break;
			case BILT:
			{
				res1 = builder.CreateICmpSLT(left_scaled, right_scaled, "bi64_lt");
			}
				break;
			case BIGE:
			{
				res1 = builder.CreateICmpSGE(left_scaled, right_scaled, "bi64_ge");
			}
				break;
			case BIGT:
			{
				res1 = builder.CreateICmpSGT(left_scaled, right_scaled, "bi64_gt");
			}
				break;
			default:
				break;
		}
		res1 = builder.CreateZExt(res1, int64Type);
		builder.CreateBr(ret_bb);

		/* right_scaled must be out of int64 bound, so leftval_scaled != right_scaled */ 
		builder.SetInsertPoint(right_out_bound);
		phi_left = builder.CreateSExt(leftval, int128Type);
		tmpval = GetScaleMultiCodeGen(&builder, delta_scale);
		tmpval = builder.CreateSExt(tmpval, int128Type);
		phi_right = builder.CreateSExt(rightval, int128Type);
		phi_right = builder.CreateMul(phi_right, tmpval, "right_scale");
		switch (op)
		{
			case BIEQ:
			{
				res2 = int1_0;
			}
				break;
			case BINEQ:
			{
				res2  = int1_1;
			}
				break;
			case BILE:
			{
				res2  = builder.CreateICmpSLE(phi_left, phi_right, "bi64_le");
			}
				break;
			case BILT:
			{
				res2  = builder.CreateICmpSLT(phi_left, phi_right, "bi64_lt");
			}
				break;
			case BIGE:
			{
				res2  = builder.CreateICmpSGE(phi_left, phi_right, "bi64_ge");
			}
				break;
			case BIGT:
			{
				res2  = builder.CreateICmpSGT(phi_left, phi_right, "bi64_gt");
			}
				break;
			default:
				break;
		}
		res2 = builder.CreateZExt(res2, int64Type);
		builder.CreateBr(ret_bb);

		/* leftval_scaled must be out of int64 bound, so leftval_scaled != right_scaled */
		builder.SetInsertPoint(left_out_bound);
		tmpval = GetScaleMultiCodeGen(&builder, mdelta_scale);
		tmpval = builder.CreateSExt(tmpval, int128Type);
		phi_left = builder.CreateSExt(leftval, int128Type);
		phi_left = builder.CreateMul(phi_left, tmpval, "left_scale");
		phi_right = builder.CreateSExt(rightval, int128Type);
		switch (op)
		{
			case BIEQ:
			{
				res3 = int1_0;
			}
				break;
			case BINEQ:
			{
				res3 = int1_1;
			}
				break;
			case BILE:
			{
				res3 = builder.CreateICmpSLE(phi_left, phi_right, "bi64_le");
			}
				break;
			case BILT:
			{
				res3 = builder.CreateICmpSLT(phi_left, phi_right, "bi64_lt");
			}
				break;
			case BIGE:
			{
				res3 = builder.CreateICmpSGE(phi_left, phi_right, "bi64_ge");
			}
				break;
			case BIGT:
			{
				res3 = builder.CreateICmpSGT(phi_left, phi_right, "bi64_gt");
			}
				break;
			default:
				break;
		}
		res3 = builder.CreateZExt(res3, int64Type);
		builder.CreateBr(ret_bb);

		builder.SetInsertPoint(ret_bb);
		llvm::PHINode *phi_ret = builder.CreatePHI(int64Type, 3);
		phi_ret->addIncoming(res1, adjust_true);
		phi_ret->addIncoming(res2, right_out_bound);
		phi_ret->addIncoming(res3, left_out_bound);
		(void)builder.CreateRet(phi_ret);

		llvmCodeGen->FinalizeFunction(jitted_funcptr);
		return jitted_funcptr;
	}

	/**
	 * @Description : Generate IR function to codegen numeric operation, which includes
	 *				  =, !=, <, <=, >, >=. 'lhs_arg' and 'rhs_arg' are the 
	 *				  parameters used by LLVM function with numeric data type.  'lhs_arg'
	 *				  and 'rhs_arg' can be both represented by BI64.
	 * @return		: LLVM IR Function that point to the special codegened operation func.
	 */
	template<biop op>
	llvm::Function *fast_numericbi_sop_codegen()
	{
		dorado::GsCodeGen *llvmCodeGen = (GsCodeGen *)t_thrd.codegen_cxt.thr_codegen_obj;
		llvm::LLVMContext& context = llvmCodeGen->context();
		GsCodeGen::LlvmBuilder builder(context);
	
		DEFINE_CG_TYPE(int16Type, INT2OID);
		DEFINE_CG_TYPE(int32Type, INT4OID);
		DEFINE_CG_TYPE(int64Type, INT8OID);
		DEFINE_CG_NINTTYP(int128Type, 128);
		DEFINE_CG_PTRTYPE(int64PtrType, INT8OID);
		DEFINE_CG_TYPE(FuncCallInfoType, "struct.FunctionCallInfoData");

		DEFINE_CGVAR_INT16(val_nan, NUMERIC_NAN);
		DEFINE_CGVAR_INT16(val_mask, NUMERIC_BI_MASK);
		DEFINE_CGVAR_INT16(val_scalemask, NUMERIC_BI_SCALEMASK);
		DEFINE_CGVAR_INT1(int1_0, 0);
		DEFINE_CGVAR_INT1(int1_1, 1);
		DEFINE_CGVAR_INT32(int32_0, 0);
		DEFINE_CGVAR_INT32(int32_1, 1);
		DEFINE_CGVAR_INT32(int32_6, 6);
		DEFINE_CGVAR_INT64(int64_0, 0);
		DEFINE_CGVAR_INT64(int64_1, 1);

		llvm::Value* llvmargs[2];
		llvm::Value* cmpval = NULL;
		llvm::Value* tmpval = NULL;
		llvm::Value* phi_val = NULL;
		llvm::Value* mulscale = NULL;
		llvm::Value* multi_bound = NULL;
		llvm::Value *left_scaled1 = NULL, *left_scaled2 = NULL;
		llvm::Value *right_scaled1 = NULL, *right_scaled2 = NULL;
		llvm::PHINode *left_scaled = NULL, *right_scaled = NULL;
		llvm::Value *res1 = NULL, *res2 = NULL, *res3 = NULL, *res4 = NULL;
		llvm::Value* phi_left = NULL;
		llvm::Value* phi_right = NULL;
		llvm::Value* Vals[2] = {int64_0, int64_0};
		llvm::Value* Vals4[4] = {int64_0, int32_0, int32_0, int32_0};

		/* the jitted function pointer */
		llvm::Function *jitted_numfuncptr = NULL;

		/* Define the right jitted function through op type and left arg type. */
		switch (op)
		{
			case BIEQ:
			{
				DEFINE_FUNCTION_WITH_2_ARGS(jitted_numfuncptr, EQ, "Jitted_fast_numericeq", 
										"arg1", int64Type, "arg2", int64Type);
			}
				break;
			case BINEQ:
			{
				DEFINE_FUNCTION_WITH_2_ARGS(jitted_numfuncptr, NEQ, "Jitted_fast_numericne",
										"arg1", int64Type, "arg2", int64Type);
			}
				break;
			case BILE:
			{
				DEFINE_FUNCTION_WITH_2_ARGS(jitted_numfuncptr, LE, "Jitted_fast_numericle", 
										"arg1", int64Type, "arg2", int64Type);
			}
				break;
			case BILT:
			{
				DEFINE_FUNCTION_WITH_2_ARGS(jitted_numfuncptr, LT, "Jitted_fast_numericlt", 
										"arg1", int64Type, "arg2", int64Type);
			}
				break;
			case BIGE:
			{
				DEFINE_FUNCTION_WITH_2_ARGS(jitted_numfuncptr, GE, "Jitted_fast_numericge", 
										"arg1", int64Type, "arg2", int64Type);
			}
				break;
			case BIGT:
			{
				DEFINE_FUNCTION_WITH_2_ARGS(jitted_numfuncptr, GT, "Jitted_fast_numericgt", 
										"arg1", int64Type, "arg2", int64Type);
			}
				break;
			default:
				break;
		}

		llvm::Value* arg = llvmargs[0];
		llvm::Value* cst = llvmargs[1];

		DEFINE_BLOCK(fast_bi, jitted_numfuncptr);
		DEFINE_BLOCK(fast_numeric, jitted_numfuncptr);
		DEFINE_BLOCK(delta_large, jitted_numfuncptr);
		DEFINE_BLOCK(delta_small, jitted_numfuncptr);
		DEFINE_BLOCK(adjust_true, jitted_numfuncptr);
		DEFINE_BLOCK(left_out_bound, jitted_numfuncptr);
		DEFINE_BLOCK(right_out_bound, jitted_numfuncptr);
		DEFINE_BLOCK(ret_bb, jitted_numfuncptr);
		
		llvm::Value* varg = DatumGetBINumericCodeGen(&builder, arg);
		llvm::Value* rcst = DatumGetBINumericCodeGen(&builder, cst);
		Vals4[0] = int64_0;
		Vals4[1] = int32_1;
		Vals4[2] = int32_0;
		Vals4[3] = int32_0;
		tmpval = builder.CreateInBoundsGEP(varg, Vals4);
		tmpval = builder.CreateLoad(int16Type, tmpval, "lheader");
		llvm::Value* varnumFlags = builder.CreateAnd(tmpval, val_mask);
		llvm::Value* varisbi = builder.CreateICmpUGT(varnumFlags, val_nan);
		builder.CreateCondBr(varisbi, fast_bi, fast_numeric);

		builder.SetInsertPoint(fast_bi);
		/* parse the variable argument */
		/* get value scale : NUMERIC_BI_SCALE */
		Vals4[0] = int64_0;
		Vals4[1] = int32_1;
		Vals4[2] = int32_0;
		Vals4[3] = int32_0;
		tmpval = builder.CreateInBoundsGEP(varg, Vals4);
		tmpval = builder.CreateLoad(int16Type, tmpval, "header");
		llvm::Value* argscale = builder.CreateAnd(tmpval, val_scalemask);
		argscale = builder.CreateSExt(argscale, int32Type, "scale");

		/* get value : NUMERIC_64VALUE */
		Vals4[3] = int32_1;
		tmpval = builder.CreateInBoundsGEP(varg, Vals4);
		tmpval = builder.CreateBitCast(tmpval, int64PtrType);
		llvm::Value* argval = builder.CreateLoad(int64Type, tmpval, "val");

		/* parse the const argument */
		/* get value scale : NUMERIC_BI_SCALE */
		Vals4[0] = int64_0;
		Vals4[1] = int32_1;
		Vals4[2] = int32_0;
		Vals4[3] = int32_0;
		tmpval = builder.CreateInBoundsGEP(rcst, Vals4);
		tmpval = builder.CreateLoad(int16Type, tmpval, "header");
		llvm::Value* cstscale = builder.CreateAnd(tmpval, val_scalemask);
		cstscale = builder.CreateSExt(cstscale, int32Type, "scale");

		/* get value : NUMERIC_64VALUE */
		Vals4[3] = int32_1;
		tmpval = builder.CreateInBoundsGEP(rcst, Vals4);
		tmpval = builder.CreateBitCast(tmpval, int64PtrType);
		llvm::Value* cstval = builder.CreateLoad(int64Type, tmpval, "val");

		/* delta_scale = lvalscale - rvalscale */
		llvm::Value* delta_scale = builder.CreateSub(argscale, cstscale, "delta_scale");
		/* check delta_scale >= 0 or not */
		cmpval = builder.CreateICmpSGE(delta_scale, int32_0, "cmp_delta_scale");
		builder.CreateCondBr(cmpval, delta_large, delta_small);
		
		/* corresponding to delta_scale >= 0 */
		builder.SetInsertPoint(delta_large);

		/* tmpval = y < 0 ? y : -y */
		tmpval = builder.CreateSub(int64_0, cstval);
		cmpval = builder.CreateICmpSLT(cstval, int64_0, "negative_cmp");
		phi_val = builder.CreateSelect(cmpval, cstval, tmpval);

		left_scaled1 = argval;
		mulscale = ScaleMultiCodeGen(&builder, delta_scale);
		/* corresponding to y_scaled = y * ScaleMultipler[delta_scale] */
		right_scaled1 = builder.CreateMul(cstval, mulscale);

		/* the result of tmpval * ScaleMultipler[delta_scale] 
		 * doesn't out of int64 bound.
		 */
		multi_bound = GetInt64MulOutofBoundCodeGen(&builder, delta_scale);
		cmpval = builder.CreateICmpSGE(phi_val, multi_bound, "bound_check");
		builder.CreateCondBr(cmpval, adjust_true, right_out_bound);

		builder.SetInsertPoint(delta_small);
		/* tmpval = x < 0 ? x : -x */
		tmpval = builder.CreateSub(int64_0, argval);
		cmpval = builder.CreateICmpSLT(argval, int64_0, "negative_cmp");
		phi_val = builder.CreateSelect(cmpval, argval, tmpval);

		/* corresponding to x_scaled = x * ScaleMultipler[-delta_scale] */
		llvm::Value* mdelta_scale = builder.CreateSub(int32_0, delta_scale);
		llvm::Value* mmulscale = ScaleMultiCodeGen(&builder, mdelta_scale);
		left_scaled2 = builder.CreateMul(argval, mmulscale);
		right_scaled2 = cstval;

		/* the result of tmpval * ScaleMultipler[delta_scale] 
		 * doesn't out of int64 bound.
		 */ 
		multi_bound = GetInt64MulOutofBoundCodeGen(&builder, mdelta_scale);
		cmpval = builder.CreateICmpSGE(phi_val, multi_bound, "bound_check");
		builder.CreateCondBr(cmpval, adjust_true, left_out_bound);

		builder.SetInsertPoint(adjust_true);
		left_scaled = builder.CreatePHI(int64Type, 2);
		left_scaled->addIncoming(left_scaled1, delta_large);
		left_scaled->addIncoming(left_scaled2, delta_small);
		
		right_scaled = builder.CreatePHI(int64Type, 2);
		right_scaled->addIncoming(right_scaled1, delta_large);
		right_scaled->addIncoming(right_scaled2, delta_small);

		/* compare leftval with rightval directly, both are int64 type */
		switch (op)
		{
			case BIEQ:
			{
				res1 = builder.CreateICmpEQ(left_scaled, right_scaled, "bi64_eq");
			}
				break;
			case BINEQ:
			{
				res1 = builder.CreateICmpNE(left_scaled, right_scaled, "bi64_ne");
			}
				break;
			case BILE:
			{
				res1 = builder.CreateICmpSLE(left_scaled, right_scaled, "bi64_le");
			}
				break;
			case BILT:
			{
				res1 = builder.CreateICmpSLT(left_scaled, right_scaled, "bi64_lt");
			}
				break;
			case BIGE:
			{
				res1 = builder.CreateICmpSGE(left_scaled, right_scaled, "bi64_ge");
			}
				break;
			case BIGT:
			{
				res1 = builder.CreateICmpSGT(left_scaled, right_scaled, "bi64_gt");
			}
				break;
			default:
				break;
		}
		res1 = builder.CreateZExt(res1, int64Type);
		builder.CreateBr(ret_bb);
	
		/* right_scaled must be out of int64 bound, so leftval_scaled != right_scaled */ 
		builder.SetInsertPoint(right_out_bound);
		phi_left = builder.CreateSExt(argval, int128Type);
		tmpval = GetScaleMultiCodeGen(&builder, delta_scale);
		tmpval = builder.CreateSExt(tmpval, int128Type);
		phi_right = builder.CreateSExt(cstval, int128Type);
		phi_right = builder.CreateMul(phi_right, tmpval, "right_scale");
		switch (op)
		{
			case BIEQ:
			{
				res2 = int1_0;
			}
				break;
			case BINEQ:
			{
				res2  = int1_1;
			}
				break;
			case BILE:
			{
				res2  = builder.CreateICmpSLE(phi_left, phi_right, "bi64_le");
			}
				break;
			case BILT:
			{
				res2  = builder.CreateICmpSLT(phi_left, phi_right, "bi64_lt");
			}
				break;
			case BIGE:
			{
				res2  = builder.CreateICmpSGE(phi_left, phi_right, "bi64_ge");
			}
				break;
			case BIGT:
			{
				res2  = builder.CreateICmpSGT(phi_left, phi_right, "bi64_gt");
			}
				break;
			default:
				break;
		}
		res2 = builder.CreateZExt(res2, int64Type);
		builder.CreateBr(ret_bb);
	
		/* leftval_scaled must be out of int64 bound, so leftval_scaled != right_scaled */
		builder.SetInsertPoint(left_out_bound);
		tmpval = GetScaleMultiCodeGen(&builder, mdelta_scale);
		tmpval = builder.CreateSExt(tmpval, int128Type);
		phi_left = builder.CreateSExt(argval, int128Type);
		phi_left = builder.CreateMul(phi_left, tmpval, "left_scale");
		phi_right = builder.CreateSExt(cstval, int128Type);
		switch (op)
		{
			case BIEQ:
			{
				res3 = int1_0;
			}
				break;
			case BINEQ:
			{
				res3 = int1_1;
			}
				break;
			case BILE:
			{
				res3 = builder.CreateICmpSLE(phi_left, phi_right, "bi64_le");
			}
				break;
			case BILT:
			{
				res3 = builder.CreateICmpSLT(phi_left, phi_right, "bi64_lt");
			}
				break;
			case BIGE:
			{
				res3 = builder.CreateICmpSGE(phi_left, phi_right, "bi64_ge");
			}
				break;
			case BIGT:
			{
				res3 = builder.CreateICmpSGT(phi_left, phi_right, "bi64_gt");
			}
				break;
			default:
				break;
		}
		res3 = builder.CreateZExt(res3, int64Type);
		builder.CreateBr(ret_bb);

		builder.SetInsertPoint(fast_numeric);
		DEFINE_CG_ARRTYPE(int64ArrType, int64Type, 2);
		llvm::Value* finfo = builder.CreateAlloca(FuncCallInfoType);
		llvm::Value* args = builder.CreateAlloca(int64ArrType);
		llvm::Value* arghead = builder.CreateInBoundsGEP(args, Vals);
		Vals[1] = int32_6;
		llvm::Value* fihead = builder.CreateInBoundsGEP(finfo, Vals);
		builder.CreateStore(arghead, fihead);
		llvm::Value* oparg1 = builder.CreatePtrToInt(varg, int64Type);
		builder.CreateStore(oparg1, arghead);
		llvm::Value* oparg2 = builder.CreatePtrToInt(rcst, int64Type);
		Vals[1] = int64_1;
		tmpval = builder.CreateInBoundsGEP(args, Vals);
		builder.CreateStore(oparg2, tmpval);
		res4 = WrapnumericFuncCodeGen<op>(&builder, finfo);
		builder.CreateBr(ret_bb);

		builder.SetInsertPoint(ret_bb);
		llvm::PHINode *phi_ret = builder.CreatePHI(int64Type, 4);
		phi_ret->addIncoming(res1, adjust_true);
		phi_ret->addIncoming(res2, right_out_bound);
		phi_ret->addIncoming(res3, left_out_bound);
		phi_ret->addIncoming(res4, fast_numeric);
		(void)builder.CreateRet(phi_ret);

		llvmCodeGen->FinalizeFunction(jitted_numfuncptr);
		return jitted_numfuncptr;
	}

	/**
	 * @Description	: Call the original numeric row function with input arguments in LLVM.
	 * @in ptrbuilder	: LLVM Builder associated with the current module.
	 * @in arg		: FuncCallInfoData structure which contains the input arguments
	 *				  needed by numericFunc.
	 */
	template<biop op>
	llvm::Value* WrapnumericFuncCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* arg)
	{
		GsCodeGen *llvmCodeGen = (GsCodeGen *)t_thrd.codegen_cxt.thr_codegen_obj;
		llvm::LLVMContext& context = llvmCodeGen->context();
		GsCodeGen::LlvmBuilder builder(context);

		DEFINE_CG_TYPE(int64Type, INT8OID);
		DEFINE_CG_PTRTYPE(FuncCallInfoPtrType, "struct.FunctionCallInfoData");

		llvm::Value* result = NULL;
		llvm::Function *jitted_numfunc = NULL;

		switch (op)
		{
			case BIADD:
			{
				jitted_numfunc = llvmCodeGen->module()->getFunction("LLVMWrapnumfuncadd");
				if (jitted_numfunc == NULL)
				{
					GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMWrapnumfuncadd", int64Type);
					fn_prototype.addArgument(GsCodeGen::NamedVariable("arg", FuncCallInfoPtrType));
					jitted_numfunc = fn_prototype.generatePrototype(NULL, NULL);
					llvm::sys::DynamicLibrary::AddSymbol("LLVMWrapnumfuncadd", (void *)numeric_add);
				}
			}
				break;
			case BISUB:
			{
				jitted_numfunc = llvmCodeGen->module()->getFunction("LLVMWrapnumfuncsub");
				if (jitted_numfunc == NULL)
				{
					GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMWrapnumfuncsub", int64Type);
					fn_prototype.addArgument(GsCodeGen::NamedVariable("arg", FuncCallInfoPtrType));
					jitted_numfunc = fn_prototype.generatePrototype(NULL, NULL);
					llvm::sys::DynamicLibrary::AddSymbol("LLVMWrapnumfuncsub", (void *)numeric_sub);
				}
			}
				break;
			case BIMUL:
			{
				jitted_numfunc = llvmCodeGen->module()->getFunction("LLVMWrapnumfuncmul");
				if (jitted_numfunc == NULL)
				{
					GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMWrapnumfuncmul", int64Type);
					fn_prototype.addArgument(GsCodeGen::NamedVariable("arg", FuncCallInfoPtrType));
					jitted_numfunc = fn_prototype.generatePrototype(NULL, NULL);
					llvm::sys::DynamicLibrary::AddSymbol("LLVMWrapnumfuncmul", (void *)numeric_mul);
				}
			}
				break;
			case BIDIV:
			{
				jitted_numfunc = llvmCodeGen->module()->getFunction("LLVMWrapnumfuncdiv");
				if (jitted_numfunc == NULL)
				{
					GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMWrapnumfuncdiv", int64Type);
					fn_prototype.addArgument(GsCodeGen::NamedVariable("arg", FuncCallInfoPtrType));
					jitted_numfunc = fn_prototype.generatePrototype(NULL, NULL);
					llvm::sys::DynamicLibrary::AddSymbol("LLVMWrapnumfuncdiv", (void *)numeric_div);
				}
			}
				break;
			case BIEQ:
			{
				jitted_numfunc = llvmCodeGen->module()->getFunction("LLVMWrapnumfunceq");
				if (jitted_numfunc == NULL)
				{
					GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMWrapnumfunceq", int64Type);
					fn_prototype.addArgument(GsCodeGen::NamedVariable("arg", FuncCallInfoPtrType));
					jitted_numfunc = fn_prototype.generatePrototype(NULL, NULL);
					llvm::sys::DynamicLibrary::AddSymbol("LLVMWrapnumfunceq", (void *)numeric_eq);
				}
			}
				break;
			case BINEQ:
			{
				jitted_numfunc = llvmCodeGen->module()->getFunction("LLVMWrapnumfuncneq");
				if (jitted_numfunc == NULL)
				{
					GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMWrapnumfuncneq", int64Type);
					fn_prototype.addArgument(GsCodeGen::NamedVariable("arg", FuncCallInfoPtrType));
					jitted_numfunc = fn_prototype.generatePrototype(NULL, NULL);
					llvm::sys::DynamicLibrary::AddSymbol("LLVMWrapnumfuncneq", (void *)numeric_ne);
				}
			}
				break;
			case BILE:
			{
				jitted_numfunc = llvmCodeGen->module()->getFunction("LLVMWrapnumfuncle");
				if (jitted_numfunc == NULL)
				{
					GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMWrapnumfuncle", int64Type);
					fn_prototype.addArgument(GsCodeGen::NamedVariable("arg", FuncCallInfoPtrType));
					jitted_numfunc = fn_prototype.generatePrototype(NULL, NULL);
					llvm::sys::DynamicLibrary::AddSymbol("LLVMWrapnumfuncle", (void *)numeric_le);
				}
			}
				break;
			case BILT:
			{
				jitted_numfunc = llvmCodeGen->module()->getFunction("LLVMWrapnumfunclt");
				if (jitted_numfunc == NULL)
				{
					GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMWrapnumfunclt", int64Type);
					fn_prototype.addArgument(GsCodeGen::NamedVariable("arg", FuncCallInfoPtrType));
					jitted_numfunc = fn_prototype.generatePrototype(NULL, NULL);
					llvm::sys::DynamicLibrary::AddSymbol("LLVMWrapnumfunclt", (void *)numeric_lt);
				}
			}
				break;
			case BIGE:
			{
				jitted_numfunc = llvmCodeGen->module()->getFunction("LLVMWrapnumfuncge");
				if (jitted_numfunc == NULL)
				{
					GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMWrapnumfuncge", int64Type);
					fn_prototype.addArgument(GsCodeGen::NamedVariable("arg", FuncCallInfoPtrType));
					jitted_numfunc = fn_prototype.generatePrototype(NULL, NULL);
					llvm::sys::DynamicLibrary::AddSymbol("LLVMWrapnumfuncge", (void *)numeric_ge);
				}
			}
				break;
			case BIGT:
			{
				jitted_numfunc = llvmCodeGen->module()->getFunction("LLVMWrapnumfuncgt");
				if (jitted_numfunc == NULL)
				{
					GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMWrapnumfuncgt", int64Type);
					fn_prototype.addArgument(GsCodeGen::NamedVariable("arg", FuncCallInfoPtrType));
					jitted_numfunc = fn_prototype.generatePrototype(NULL, NULL);
					llvm::sys::DynamicLibrary::AddSymbol("LLVMWrapnumfuncgt", (void *)numeric_gt);
				}
			}
				break;
			default:
				break;
		}
		
		llvmCodeGen->FinalizeFunction(jitted_numfunc);
		
		result = ptrbuilder->CreateCall(jitted_numfunc, arg);
		return result;
	}
}
#endif
