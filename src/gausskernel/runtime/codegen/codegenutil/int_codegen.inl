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
 * int_codegen.inl
 *    template implementation of int codegen.
 *
 * IDENTIFICATION
 *    src/gausskernel/runtime/codegen/codegenutil/int_codegen.inl
 *
 * ---------------------------------------------------------------------------------------
 */
 
#ifndef INTCODEGEN_INL
#define INTCODEGEN_INL

#include "codegen/gscodegen.h"
#include "codegen/builtinscodegen.h"
#include "codegen/codegendebuger.h"

#include "fmgr.h"
#include "utils/int8.h"

namespace dorado
{
	/*
	 * @Description : Define macro to declare the LLVM IR function
	 * @in jiited_funptr: LLVM IR function pointer
	 * @in func_type	: make a difference between functions
	 * @in name		: the name of the LLVM IR function
	 * @in arg1		: the value of the first argument
	 * @in arg1Type	: the type of the first argument
	 * @in arg2		: the value of the second argument
	 * @in arg2Type	: the type of the second argument
	 */
	#define DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, func_type, name, arg1, arg1Type, arg2, arg2Type)\
			GsCodeGen::FnPrototype func_type(llvmCodeGen, name, int64Type);\
			(func_type).addArgument(GsCodeGen::NamedVariable(arg1, arg1Type));\
			(func_type).addArgument(GsCodeGen::NamedVariable(arg2, arg2Type));\
			jitted_funptr = (func_type).generatePrototype(&builder, &llvmargs[0]);\

	/**
	 * @Description : Generate IR function to codegen int2 with respect to different
	 *				  kinds of operation.
	 * @return      : LLVM codegened IR Function
	 */
	template<SimpleOp op>
	llvm::Function* int2_sop_codegen()
	{
		GsCodeGen *llvmCodeGen = (GsCodeGen *)t_thrd.codegen_cxt.thr_codegen_obj;
		llvm::LLVMContext& context = llvmCodeGen->context();
		GsCodeGen::LlvmBuilder builder(context);
	
		/* Define the datatype and variables that needed */
		DEFINE_CG_TYPE(int16Type, INT2OID);
		DEFINE_CG_TYPE(int64Type, INT8OID);
		
		llvm::Value *llvmargs[2];
		llvm::Value *result = NULL;
		
		/* the jitted function pointer */
		llvm::Function *jitted_funptr = NULL;		

		/* Define the right jitted function through op type and left arg type. */
		switch (op)
		{
			case SOP_EQ:
			{
				DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, EQ, "Jitted_int2eq", 
										"arg1", int64Type, "arg2", int64Type);
				break;
			}
			case SOP_NEQ:
			{
				DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, NEQ, "Jitted_int2ne",
										"arg1", int64Type, "arg2", int64Type);
				break;
			}
			case SOP_LE:
			{
				DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, LE, "Jitted_int2le", 
										"arg1", int64Type, "arg2", int64Type);
				break;
			}
			case SOP_LT:
			{
				DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, LT, "Jitted_int2lt", 
										"arg1", int64Type, "arg2", int64Type);
				break;
			}
			case SOP_GE:
			{
				DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, GE, "Jitted_int2ge", 
										"arg1", int64Type, "arg2", int64Type);
				break;
			}
			case SOP_GT:
			{
				DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, GT, "Jitted_int2gt", 
										"arg1", int64Type, "arg2", int64Type);
				break;
			}
			default:
				break;
		}

		llvm::Value *arg1 = llvmargs[0];
		llvm::Value *arg2 = llvmargs[1];
	
		/* Truncate the args type to confim the same type of the args. */
		arg1 = builder.CreateTrunc(arg1, int16Type);
		arg2 = builder.CreateTrunc(arg2, int16Type);

		/* Choice the Cmp funcion though different op type. */
		switch (op)
		{
			case SOP_EQ:
				result = builder.CreateICmpEQ(arg1, arg2, "int2_eq");
				break;
			case SOP_NEQ:
				result = builder.CreateICmpNE(arg1, arg2, "int2_ne");
				break;
			case SOP_LE:
				result = builder.CreateICmpSLE(arg1, arg2, "int2_le");
				break;
			case SOP_LT:
				result = builder.CreateICmpSLT(arg1, arg2, "int2_lt");
				break;
			case SOP_GE:
				result = builder.CreateICmpSGE(arg1, arg2, "int2_ge");
				break;
			case SOP_GT:
				result = builder.CreateICmpSGT(arg1, arg2, "int2_gt");
				break;
			default:
				break;
		}

		result = builder.CreateZExt(result, int64Type);
		(void)builder.CreateRet(result);
		llvmCodeGen->FinalizeFunction(jitted_funptr);
		return jitted_funptr;
	}

	/**
	 * @Description : Generate IR function to codegen int4.
	 * @return      : LLVM IR Function
	 */
	template<SimpleOp op>
	llvm::Function* int4_sop_codegen()
	{
		GsCodeGen *llvmCodeGen = (GsCodeGen *)t_thrd.codegen_cxt.thr_codegen_obj;
		llvm::LLVMContext& context = llvmCodeGen->context();
		GsCodeGen::LlvmBuilder builder(context);
	
		/* Define the datatype and variables that needed */
		DEFINE_CG_TYPE(int32Type, INT4OID);
		DEFINE_CG_TYPE(int64Type, INT8OID);
		
		llvm::Value *llvmargs[2];
		llvm::Value *result = NULL;
		
		/* the jitted function pointer.*/
		llvm::Function *jitted_funptr = NULL;

		/* Define the right jitted function through op type and left arg type. */
		switch (op)
		{
			case SOP_EQ:
			{
				DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, EQ, "Jitted_int4eq", 
										"arg1", int64Type, "arg2", int64Type);
				break;
			}
			case SOP_NEQ:
			{
				DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, NEQ, "Jitted_int4ne",
										"arg1", int64Type, "arg2", int64Type);
				break;
			}
			case SOP_LE:
			{
				DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, LE, "Jitted_int4le", 
										"arg1", int64Type, "arg2", int64Type);
				break;
			}
			case SOP_LT:
			{
				DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, LT, "Jitted_int4lt", 
										"arg1", int64Type, "arg2", int64Type);
				break;
			}
			case SOP_GE:
			{
				DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, GE, "Jitted_int4ge", 
										"arg1", int64Type, "arg2", int64Type);
				break;
			}
			case SOP_GT:
			{
				DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, GT, "Jitted_int4gt", 
										"arg1", int64Type, "arg2", int64Type);
				break;
			}
			default:
				break;
		}

		llvm::Value *arg1 = llvmargs[0];
		llvm::Value *arg2 = llvmargs[1];
	
		/* Truncate the args type to confim the same type of the args. */
		arg1 = builder.CreateTrunc(arg1, int32Type);
		arg2 = builder.CreateTrunc(arg2, int32Type);

		/* Choice the Cmp funcion though different op type. */
		switch (op)
		{
			case SOP_EQ:
				result = builder.CreateICmpEQ(arg1, arg2, "int4_eq");
				break;
			case SOP_NEQ:
				result = builder.CreateICmpNE(arg1, arg2, "int4_ne");
				break;
			case SOP_LE:
				result = builder.CreateICmpSLE(arg1, arg2, "int4_le");
				break;
			case SOP_LT:
				result = builder.CreateICmpSLT(arg1, arg2, "int4_lt");
				break;
			case SOP_GE:
				result = builder.CreateICmpSGE(arg1, arg2, "int4_ge");
				break;
			case SOP_GT:
				result = builder.CreateICmpSGT(arg1, arg2, "int4_gt");
				break;
			default:
				break;
		}

		result = builder.CreateZExt(result, int64Type);
		(void)builder.CreateRet(result);
		llvmCodeGen->FinalizeFunction(jitted_funptr);
		return jitted_funptr;

	}

	/**
	 * @Description : Generate IR function to codegen int8.
	 * @return      : LLVM IR Function
	 */
	template<SimpleOp op>
	llvm::Function* int8_sop_codegen()
	{
		GsCodeGen *llvmCodeGen = (GsCodeGen *)t_thrd.codegen_cxt.thr_codegen_obj;
		llvm::LLVMContext& context = llvmCodeGen->context();
		GsCodeGen::LlvmBuilder builder(context);
	
		/* Define the datatype and variables that needed */
		DEFINE_CG_TYPE(int64Type, INT8OID);
		
		llvm::Value *llvmargs[2];
		llvm::Value *result = NULL;
		llvm::Function *jitted_funptr = NULL;		/* the jitted function pointer */

		/* Define the right jitted function through op type and left arg type. */
		switch (op)
		{
			case SOP_EQ:
			{
				DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, EQ, "Jitted_int8eq", 
										"arg1", int64Type, "arg2", int64Type);
				break;
			}
			case SOP_NEQ:
			{
				DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, NEQ, "Jitted_int8ne", 
										"arg1", int64Type, "arg2", int64Type);
				break;
			}
			case SOP_LE:
			{
				DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, LE, "Jitted_int8le", 
										"arg1", int64Type, "arg2", int64Type);
				break;
			}
			case SOP_LT:
			{
				DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, LT, "Jitted_int8lt", 
										"arg1", int64Type, "arg2", int64Type);
				break;
			}
			case SOP_GE:
			{
				DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, GE, "Jitted_int8ge", 
										"arg1", int64Type, "arg2", int64Type);
				break;
			}
			case SOP_GT:
			{
				DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, GT, "Jitted_int8gt", 
										"arg1", int64Type, "arg2", int64Type);
				break;
			}
			default:
				break;
		}

		llvm::Value *arg1 = llvmargs[0];
		llvm::Value *arg2 = llvmargs[1];

		/* Choice the Cmp funcion though different op type. */
		switch (op)
		{
			case SOP_EQ:
				result = builder.CreateICmpEQ(arg1, arg2, "int8_eq");
				break;
			case SOP_NEQ:
				result = builder.CreateICmpNE(arg1, arg2, "int8_ne");
				break;
			case SOP_LE:
				result = builder.CreateICmpSLE(arg1, arg2, "int8_le");
				break;
			case SOP_LT:
				result = builder.CreateICmpSLT(arg1, arg2, "int8_lt");
				break;
			case SOP_GE:
				result = builder.CreateICmpSGE(arg1, arg2, "int8_ge");
				break;
			case SOP_GT:
				result = builder.CreateICmpSGT(arg1, arg2, "int8_gt");
				break;
			default:
				break;
		}

		result = builder.CreateZExt(result, int64Type);
		(void)builder.CreateRet(result);
		llvmCodeGen->FinalizeFunction(jitted_funptr);
		return jitted_funptr;

	}

	/**
	 * @Description : Generate IR function to codegen operator between int2 and int4.
	 * @return      : LLVM IR Function
	 */
	template<SimpleOp op, int ltype>
	llvm::Function* int2_int4_sop_codegen()
	{
		GsCodeGen *llvmCodeGen = (GsCodeGen *)t_thrd.codegen_cxt.thr_codegen_obj;
		llvm::LLVMContext& context = llvmCodeGen->context();
		GsCodeGen::LlvmBuilder builder(context);
	
		/* Define the datatype and variables that needed */
		DEFINE_CG_TYPE(int16Type, INT2OID);
		DEFINE_CG_TYPE(int32Type, INT4OID);
		DEFINE_CG_TYPE(int64Type, INT8OID);
		
		llvm::Value *llvmargs[2];
		llvm::Value *result = NULL;
		llvm::Function *jitted_funptr = NULL;		/* the jitted function pointer */

		/* Define the right jitted function through op type and left arg type. */
		switch (op)
		{
			case SOP_EQ:
			{
				if (ltype == INT2OID)
				{
					DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, EQ, "Jitted_int24eq", 
										"arg1", int64Type, "arg2", int64Type);
				}
				else
				{
					DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, EQ, "Jitted_int42eq", 
										"arg1", int64Type, "arg2", int64Type);
				}
				break;
			}
			case SOP_NEQ:
			{
				if (ltype == INT2OID)
				{
					DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, NEQ, "Jitted_int24ne", 
										"arg1", int64Type, "arg2", int64Type);
				}
				else
				{
					DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, NEQ, "Jitted_int42ne", 
										"arg1", int64Type, "arg2", int64Type);
				}
				break;
			}
			case SOP_LE:
			{
				if (ltype == INT2OID)
				{
					DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, LE, "Jitted_int24le", 
										"arg1", int64Type, "arg2", int64Type);
				}
				else
				{
					DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, LE, "Jitted_int42le", 
										"arg1", int64Type, "arg2", int64Type);
				}
				break;
			}
			case SOP_LT:
			{
				if (ltype == INT2OID)
				{
					DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, LT, "Jitted_int24lt", 
										"arg1", int64Type, "arg2", int64Type);
				}
				else
				{
					DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, LT, "Jitted_int42lt", 
										"arg1", int64Type, "arg2", int64Type);
				}
				break;
			}
			case SOP_GE:
			{
				if (ltype == INT2OID)
				{
					DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, GE, "Jitted_int24ge", 
										"arg1", int64Type, "arg2", int64Type);
				}
				else
				{
					DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, GE, "Jitted_int42ge", 
										"arg1", int64Type, "arg2", int64Type);
				}
				break;
			}
			case SOP_GT:
			{
				if (ltype == INT2OID)
				{
					DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, GT, "Jitted_int24gt", 
										"arg1", int64Type, "arg2", int64Type);
				}
				else
				{
					DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, GT, "Jitted_int42gt", 
										"arg1", int64Type, "arg2", int64Type);
				}
				break;
			}
			default:
				break;
		}

		llvm::Value *arg1 = llvmargs[0];
		llvm::Value *arg2 = llvmargs[1];

		/* Truncate the args type to confim the same type of the args. */
		if (ltype == INT2OID)
		{
			arg1 = builder.CreateTrunc(arg1, int16Type);
			arg1 = builder.CreateSExt(arg1, int32Type);
			arg2 = builder.CreateTrunc(arg2, int32Type);
		}
		else
		{
			arg1 = builder.CreateTrunc(arg1, int32Type);
			arg2 = builder.CreateTrunc(arg2, int16Type);
			arg2 = builder.CreateSExt(arg2, int32Type);
		}

		/* Choice the Cmp funcion though different op type. */
		switch (op)
		{
			case SOP_EQ:
				result = builder.CreateICmpEQ(arg1, arg2, "int2_int4_eq");
				break;
			case SOP_NEQ:
				result = builder.CreateICmpNE(arg1, arg2, "int2_int4_ne");
				break;
			case SOP_LE:
				result = builder.CreateICmpSLE(arg1, arg2, "int2_int4_le");
				break;
			case SOP_LT:
				result = builder.CreateICmpSLT(arg1, arg2, "int2_int4_lt");
				break;
			case SOP_GE:
				result = builder.CreateICmpSGE(arg1, arg2, "int2_int4_ge");
				break;
			case SOP_GT:
				result = builder.CreateICmpSGT(arg1, arg2, "int2_int4_gt");
				break;
			default:
				break;
		}

		result = builder.CreateZExt(result, int64Type);
		(void)builder.CreateRet(result);
		llvmCodeGen->FinalizeFunction(jitted_funptr);
		return jitted_funptr;
	}

	/**
	 * @Description : Generate IR function to codegen operator between int4 and int8.
	 * @return      : LLVM IR Function
	 */
	template<SimpleOp op, int ltype>
	llvm::Function* int4_int8_sop_codegen()
	{
		GsCodeGen *llvmCodeGen = (GsCodeGen *)t_thrd.codegen_cxt.thr_codegen_obj;
		llvm::LLVMContext& context = llvmCodeGen->context();
		GsCodeGen::LlvmBuilder builder(context);
	
		/* Define the datatype and variables that needed */
		DEFINE_CG_TYPE(int32Type, INT4OID);
		DEFINE_CG_TYPE(int64Type, INT8OID);
		
		llvm::Value *llvmargs[2];
		llvm::Value *result = NULL;
		llvm::Function *jitted_funptr = NULL;		/* the jitted function pointer */

		/* Define the right jitted function through op type and left arg type. */
		switch (op)
		{
			case SOP_EQ:
			{
				if (ltype == INT4OID)
				{
					DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, EQ, "Jitted_int48eq", 
											"arg1", int64Type, "arg2", int64Type);
				}
				else
				{
					DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, EQ, "Jitted_int84eq", 
											"arg1", int64Type, "arg2", int64Type);
				}
				break;
			}
			case SOP_NEQ:
			{
				if (ltype == INT4OID)
				{
					DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, NEQ, "Jitted_int48ne", 
											"arg1", int64Type, "arg2", int64Type);
				}
				else
				{
					DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, NEQ, "Jitted_int84ne", 
											"arg1", int64Type, "arg2", int64Type);
				}
				break;
			}
			case SOP_LE:
			{
				if (ltype == INT4OID)
				{
					DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, LE, "Jitted_int48le", 
											"arg1", int64Type, "arg2", int64Type);
				}
				else
				{
					DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, LE, "Jitted_int84le", 
											"arg1", int64Type, "arg2", int64Type);
				}
				break;
			}
			case SOP_LT:
			{
				if (ltype == INT4OID)
				{
					DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, LT, "Jitted_int48lt", 
											"arg1", int64Type, "arg2", int64Type);
				}
				else
				{
					DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, LT, "Jitted_int84lt", 
											"arg1", int64Type, "arg2", int64Type);
				}
				break;
			}
			case SOP_GE:
			{
				if (ltype == INT4OID)
				{
					DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, GE, "Jitted_int48ge", 
											"arg1", int64Type, "arg2", int64Type);
				}
				else
				{
					DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, GE, "Jitted_int84ge", 
											"arg1", int64Type, "arg2", int64Type);
				}
				break;
			}
			case SOP_GT:
			{
				if (ltype == INT4OID)
				{
					DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, GT, "Jitted_int48gt", 
											"arg1", int64Type, "arg2", int64Type);
				}
				else
				{
					DEFINE_FUNCTION_WITH_2_ARGS(jitted_funptr, GT, "Jitted_int84gt", 
											"arg1", int64Type, "arg2", int64Type);
				}
				break;
			}
			default:
				break;
		}

		llvm::Value *arg1 = llvmargs[0];
		llvm::Value *arg2 = llvmargs[1];

		/* Truncate the args type to confim the same type of the args. */
		if (ltype == INT4OID)
		{
			arg1 = builder.CreateTrunc(arg1, int32Type);
			arg1 = builder.CreateSExt(arg1, int64Type);
		}
		else
		{
			arg2 = builder.CreateTrunc(arg2, int32Type);
			arg2 = builder.CreateSExt(arg2, int64Type);
		}

		/* Choice the Cmp funcion though different op type. */
		switch (op)
		{
			case SOP_EQ:
				result = builder.CreateICmpEQ(arg1, arg2, "int4_int8_eq");
				break;
			case SOP_NEQ:
				result = builder.CreateICmpNE(arg1, arg2, "int4_int8_ne");
				break;
			case SOP_LE:
				result = builder.CreateICmpSLE(arg1, arg2, "int4_int8_le");
				break;
			case SOP_LT:
				result = builder.CreateICmpSLT(arg1, arg2, "int4_int8_lt");
				break;
			case SOP_GE:
				result = builder.CreateICmpSGE(arg1, arg2, "int4_int8_ge");
				break;
			case SOP_GT:
				result = builder.CreateICmpSGT(arg1, arg2, "int4_int8_gt");
				break;
			default:
				break;
		}

		result = builder.CreateZExt(result, int64Type);
		(void)builder.CreateRet(result);
		llvmCodeGen->FinalizeFunction(jitted_funptr);
		return jitted_funptr;
	}
}
#endif
