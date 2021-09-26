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
 * foreignscancodegen.cpp
 *     codegeneration of filter expression for HDFS tables
 *
 * IDENTIFICATION
 *     Code/src/gausskernel/runtime/codegen/executor/foreignscancodegen.cpp
 *
 * -----------------------------------------------------------------------
 */
#include "codegen/gscodegen.h"
#include "codegen/foreignscancodegen.h"
#include "catalog/pg_operator.h"

using namespace llvm;
using namespace dorado;

namespace dorado {
bool ForeignScanCodeGen::IsJittableExpr(Expr* expr)
{

    /*
     * We only support the following operators and data types.
     */
    OpExpr* op = (OpExpr*)expr;
    bool IsOperatorLLVMReady = false;

    switch (op->opno) {
        case FLOAT8EQOID:
        case FLOAT8NEOID:
        case FLOAT8LTOID:
        case FLOAT8LEOID:
        case FLOAT8GTOID:
        case FLOAT8GEOID:
        case FLOAT4EQOID:
        case FLOAT4NEOID:
        case FLOAT4LTOID:
        case FLOAT4LEOID:
        case FLOAT4GTOID:
        case FLOAT4GEOID:
        case INT2EQOID:
        case INT2NEOID:
        case INT2LTOID:
        case INT2LEOID:
        case INT2GTOID:
        case INT2GEOID:
        case INT4EQOID:
        case INT4NEOID:
        case INT4LTOID:
        case INT4LEOID:
        case INT4GTOID:
        case INT4GEOID:
        case INT8EQOID:
        case INT8NEOID:
        case INT8LTOID:
        case INT8LEOID:
        case INT8GTOID:
        case INT8GEOID:
        case INT24EQOID:
        case INT42EQOID:
        case INT24LTOID:
        case INT42LTOID:
        case INT24GTOID:
        case INT42GTOID:
        case INT24NEOID:
        case INT42NEOID:
        case INT24LEOID:
        case INT42LEOID:
        case INT24GEOID:
        case INT42GEOID:
        case INT84EQOID:
        case INT84NEOID:
        case INT84LTOID:
        case INT84GTOID:
        case INT84LEOID:
        case INT84GEOID:
        case INT48EQOID:
        case INT48NEOID:
        case INT48LTOID:
        case INT48GTOID:
        case INT48LEOID:
        case INT48GEOID:
        case INT28EQOID:
        case INT28NEOID:
        case INT28LTOID:
        case INT28GTOID:
        case INT28LEOID:
        case INT28GEOID:
        case INT82EQOID:
        case INT82NEOID:
        case INT82LTOID:
        case INT82GTOID:
        case INT82LEOID:
        case INT82GEOID:
        case FLOAT48EQOID:
        case FLOAT48NEOID:
        case FLOAT48LTOID:
        case FLOAT48GTOID:
        case FLOAT48LEOID:
        case FLOAT48GEOID:
        case FLOAT84EQOID:
        case FLOAT84NEOID:
        case FLOAT84LTOID:
        case FLOAT84GTOID:
        case FLOAT84LEOID:
        case FLOAT84GEOID: {
            IsOperatorLLVMReady = true;
            break;
        }
        default: {
            IsOperatorLLVMReady = false;
            break;
        }
    }

    return IsOperatorLLVMReady;
}

llvm::Value* ForeignScanCodeGen::buildConstValue(
    Expr* node, llvm::Value* llvm_args[], GsCodeGen::LlvmBuilder& builder, StringInfo fname)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    llvm::LLVMContext& context = llvmCodeGen->context();

    llvm::Value* result = NULL;

    if (node == NULL) {
        return NULL;
    }

    switch (nodeTag(node)) {
        case T_Const: {
            Const* c = (Const*)node;
            if (c->consttype == FLOAT8OID) {
                result = llvm::ConstantFP::get(context, llvm::APFloat((DatumGetFloat8(c->constvalue))));
            } else if (c->consttype == FLOAT4OID) {
                result = llvm::ConstantFP::get(context, llvm::APFloat((float8)(DatumGetFloat4(c->constvalue))));
            } else if (c->consttype == INT8OID || c->consttype == INT4OID || c->consttype == INT2OID) {
                int64 value = 0;
                switch (c->consttype) {
                    case INT8OID: {
                        value = DatumGetInt64(c->constvalue);
                        break;
                    }
                    case INT4OID: {
                        value = DatumGetInt32(c->constvalue);
                        break;
                    }
                    default: {
                        value = DatumGetInt16(c->constvalue);
                        break;
                    }
                }
                result = llvm::ConstantInt::get(llvmCodeGen->getType(INT8OID), value);
            }
            break;
        }
        default: {
            ereport(LOG, (errmsg("Find unsupported node type in buildConstValue.")));
        }
    }

    return result;
}

bool ForeignScanCodeGen::ScanCodeGen(Expr* expr, PlanState* parent, void** jittedFunc)
{
    bool prepare_result = false;
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    if (IsJittableExpr(expr)) {
        OpExpr* op = (OpExpr*)expr;

        /*
         * Rightop should be const. Has been checked before.
         * To avoid any risk, if the rightop is not Const, just return without doing anything.
         */
        Expr* rightop = (Expr*)get_rightop(expr);
        if (rightop == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                    errmodule(MOD_LLVM),
                    errmsg("Unexpected NULL right operator!")));
        }

        if (!IsA(rightop, Const))
            return false;

        /*
         * Get the IR function from the static IR file:
         * Make sure we use the same module for both HDFS tables and column tables.
         */
        llvmCodeGen->loadIRFile();

        llvm::LLVMContext& context = llvmCodeGen->context();
        GsCodeGen::LlvmBuilder builder(context);

        llvm::Type* int64_type = llvmCodeGen->getType(INT8OID);
        llvm::Value* llvmargs[1];
        llvm::Value* rhs_value = NULL;
        llvm::Value* result = NULL;
        llvm::Function* jitted_function = NULL;

        GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "JittedPredicate", llvmCodeGen->getType(BITOID));

        /*
         * If float8 operation, else the operation must be int4 related operation and
         * it is guarded by IsOperatorLLVMReady right now (need to change the condition
         * when we support more data type though)
         */
        List* args = NULL;
        Node* leftChild = NULL;
        Var* leftVar = NULL;

        args = op->args;
        Assert(list_length(args) == 2);

        leftChild = (Node*)linitial(args);
        Assert(nodeTag(leftChild) == T_Var);

        leftVar = (Var*)leftChild;
        Assert((leftVar->vartype == FLOAT8OID) || (leftVar->vartype == FLOAT4OID) || (leftVar->vartype == INT2OID) ||
               (leftVar->vartype == INT4OID) || (leftVar->vartype == INT8OID));

        /* int4, int8, int2 */
        if (leftVar->vartype == INT2OID || leftVar->vartype == INT4OID || leftVar->vartype == INT8OID) {
            fn_prototype.addArgument(GsCodeGen::NamedVariable("value", int64_type));
        } else {
            fn_prototype.addArgument(GsCodeGen::NamedVariable("value", llvmCodeGen->getType(FLOAT8OID)));
        }

        jitted_function = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

        if (NULL == jitted_function) {
            return false;
        }

        StringInfo fname = makeStringInfo();
        rhs_value = buildConstValue(rightop, llvmargs, builder, fname);
        if (NULL == rhs_value) {
            pfree_ext(fname->data);
            pfree_ext(fname);
            return false;
        }
        switch (op->opno) {
            case FLOAT4LTOID: /* "<" */
            case FLOAT8LTOID:
            case FLOAT48LTOID:
            case FLOAT84LTOID: {
                result = builder.CreateFCmpOLT(llvmargs[0], rhs_value, "tmp_flt");
                break;
            }
            case FLOAT4LEOID: /* "<=" */
            case FLOAT8LEOID:
            case FLOAT48LEOID:
            case FLOAT84LEOID: {
                result = builder.CreateFCmpOLE(llvmargs[0], rhs_value, "tmp_fle");
                break;
            }
            case FLOAT4EQOID: /* "=" */
            case FLOAT8EQOID:
            case FLOAT48EQOID:
            case FLOAT84EQOID: {
                result = builder.CreateFCmpOEQ(llvmargs[0], rhs_value, "tmp_feq");
                break;
            }
            case FLOAT4GEOID: /* ">=" */
            case FLOAT8GEOID:
            case FLOAT48GEOID:
            case FLOAT84GEOID: {
                result = builder.CreateFCmpOGE(llvmargs[0], rhs_value, "tmp_fge");
                break;
            }
            case FLOAT4GTOID: /* ">" */
            case FLOAT8GTOID:
            case FLOAT48GTOID:
            case FLOAT84GTOID: {
                result = builder.CreateFCmpOGT(llvmargs[0], rhs_value, "tmp_fgt");
                break;
            }
            case FLOAT4NEOID: /* "!=" and "<>" */
            case FLOAT8NEOID:
            case FLOAT48NEOID:
            case FLOAT84NEOID: {
                result = builder.CreateFCmpONE(llvmargs[0], rhs_value, "tmp_fne");
                break;
            }
            case INT2EQOID: /* "=" */
            case INT4EQOID:
            case INT8EQOID:
            case INT24EQOID:
            case INT28EQOID:
            case INT42EQOID:
            case INT48EQOID:
            case INT82EQOID:
            case INT84EQOID: {
                result = builder.CreateICmpEQ(llvmargs[0], rhs_value, "tmp_i4eq");
                break;
            }
            case INT2NEOID: /* "<>" and "!=" */
            case INT4NEOID:
            case INT8NEOID:
            case INT24NEOID:
            case INT28NEOID:
            case INT42NEOID:
            case INT48NEOID:
            case INT82NEOID:
            case INT84NEOID: {
                result = builder.CreateICmpNE(llvmargs[0], rhs_value, "tmp_i4ne");
                break;
            }
            case INT2LTOID: /* "<" */
            case INT4LTOID:
            case INT8LTOID:
            case INT24LTOID:
            case INT28LTOID:
            case INT42LTOID:
            case INT48LTOID:
            case INT82LTOID:
            case INT84LTOID: {
                result = builder.CreateICmpSLT(llvmargs[0], rhs_value, "tmp_i4lt");
                break;
            }
            case INT2GTOID: /* ">" */
            case INT4GTOID:
            case INT8GTOID:
            case INT24GTOID:
            case INT28GTOID:
            case INT42GTOID:
            case INT48GTOID:
            case INT82GTOID:
            case INT84GTOID: {
                result = builder.CreateICmpSGT(llvmargs[0], rhs_value, "tmp_i4gt");
                break;
            }
            case INT2GEOID: /* ">=" */
            case INT4GEOID:
            case INT8GEOID:
            case INT24GEOID:
            case INT28GEOID:
            case INT42GEOID:
            case INT48GEOID:
            case INT82GEOID:
            case INT84GEOID: {
                result = builder.CreateICmpSGE(llvmargs[0], rhs_value, "tmp_i4ge");
                break;
            }
            case INT2LEOID: /* "<=" */
            case INT4LEOID:
            case INT8LEOID:
            case INT24LEOID:
            case INT28LEOID:
            case INT42LEOID:
            case INT48LEOID:
            case INT82LEOID:
            case INT84LEOID: {
                result = builder.CreateICmpSLE(llvmargs[0], rhs_value, "tmp_i4le");

                break;
            }
            default: {
                elog(LOG, "Find unsupported operator %u!", op->opno);
                return false;
            }
        }

        builder.CreateRet(result);

        llvmCodeGen->FinalizeFunction(jitted_function);

        llvmCodeGen->addFunctionToMCJit(jitted_function, reinterpret_cast<void**>(&(*jittedFunc)));
        prepare_result = true;

        pfree_ext(fname->data);
        pfree_ext(fname);
    } else {
        prepare_result = false;
    }

    return prepare_result;
}
}  // namespace dorado

bool ForeignScanExprCodeGen(Expr* expr, PlanState* parent, void** jittedFunc)
{
    return ForeignScanCodeGen::ScanCodeGen(expr, parent, jittedFunc);
}
