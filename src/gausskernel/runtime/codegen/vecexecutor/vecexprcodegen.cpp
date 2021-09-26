/*
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
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
 *
 * ---------------------------------------------------------------------------------------
 *
 *  vecexprcodegen.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/runtime/codegen/vecexecutor/vecexprcodegen.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "codegen/gscodegen.h"
#include "codegen/builtinscodegen.h"
#include "codegen/codegendebuger.h"
#include "codegen/datecodegen.h"
#include "codegen/timestampcodegen.h"
#include "codegen/vecexprcodegen.h"

#include "funcapi.h"
#include "access/htup.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_language.h"
#include "optimizer/planmain.h"
#include "pgxc/pgxc.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "vecexecutor/vecfunc.h"
#include "vecexecutor/vecnodes.h"

#define MAX_ARGS_NUM 3
#define MAX_OID_LENGTH 11 /* 10 digits for unsigned int32, plus '\0' */
#define ARRAY_LIMIT_SIZE 10
#define STR_PER '%'
#define STR_UNL '_'
#define STR_SLA '\\'

/*
 * extern all the functions needed by WarpChooExtFunCodeGen.
 */
extern Datum ExtractVarType(Datum* data);
extern Datum ExtractAddrType(Datum* data);
extern Datum ExtractPlainType(Datum* data);
extern Datum ExtractFixedType(Datum* data);
extern Datum ExtractCstringType(Datum* data);
extern void initVectorFcache(Oid foid, Oid input_collation, FuncExprState* fcache, MemoryContext fcacheCxt);
extern TypeFuncClass get_expr_result_type(Node* expr, Oid* resultTypeId, TupleDesc* resultTupleDesc,
    int4* resultTypeId_orig);

namespace dorado {
/*
 * Define C Functions : all there functions are used to deal with different
 * datum types.
 */
static Datum WrapVecStrictOperFunc(FunctionCallInfo fcinfo, Datum* arg, bool* isNull);
static Datum WrapVecNonStrictOperFunc(FunctionCallInfo fcinfo, Datum* arg, const bool* argnull, bool* isNull);
static Datum WrapDatumCstringToScalar(Datum data);
static Datum WrapDatumFixLenToScalar(Datum data, Size len);
static VectorBatch* WrapGetVectorBatch(ExprContext* econtext);
static MemoryContext WrapMemoryContextSwitchTo(MemoryContext context);

#define DEFINE_FUNCTION_WITH_3_ARGS(jitted_funptr, name, arg1, arg1Type, arg2, arg2Type, arg3, arg3Type) \
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, name, int64Type);                                   \
    fn_prototype.addArgument(GsCodeGen::NamedVariable(arg1, arg1Type));                                  \
    fn_prototype.addArgument(GsCodeGen::NamedVariable(arg2, arg2Type));                                  \
    fn_prototype.addArgument(GsCodeGen::NamedVariable(arg3, arg3Type));                                  \
    llvm::Function* jitted_funptr = fn_prototype.generatePrototype(&inner_builder, &llvmargs[0]);

bool VecExprCodeGen::QualJittable(List* qual)
{
    ListCell* cell = NULL;

    if (!u_sess->attr.attr_sql.enable_codegen || IS_PGXC_COORDINATOR || qual == NULL)
        return false;

    /*
     * Iterate over all the lists, if one of them can not be llvm optimized,
     * then, the whole qual can not be llvm optimized.
     */
    foreach (cell, qual) {
        ExprState* qexpr = (ExprState*)lfirst(cell);
        if (!ExprJittable(qexpr))
            return false;
    }

    return true;
}

bool VecExprCodeGen::ExprJittable(ExprState* state)
{
    Expr* node = state->expr;

    /* codegen is only supported on Datanode */
    if (!u_sess->attr.attr_sql.enable_codegen || IS_PGXC_COORDINATOR || (node == NULL))
        return false;

    switch (nodeTag(node)) {
        case T_ScalarArrayOpExpr: {
            return ScalarArrayJittable(state);
        }
        case T_CaseExpr: {
            return CaseJittable(state);
        }
        case T_CaseTestExpr: {
            /*
             * when case_arg is not null, we have CaseTestExpr Node.
             * for each case when clause, we only need to eval the second arg.
             */
            return true;
        }
        case T_BoolExpr: {
            return BoolJittable(state);
        }
        case T_BooleanTest: {
            return BooleanTestJittable(state);
        }
        case T_RelabelType: {
            return RelabelJittable(state);
        }
        case T_FuncExpr: {
            return FuncJittable(state);
        }
        case T_OpExpr: {
            return OpJittable(state);
        }
        case T_NullIfExpr: {
            return NullIfJittable(state);
        }
        case T_Var: {
            return VarJittable(state);
        }
        case T_Const: {
            return ConstJittable(state);
        }
        case T_NullTest: {
            return NullTestJittable(state);
        }
        default:
            return false;
    }
}

bool VecExprCodeGen::ScalarArrayJittable(ExprState* state)
{
    ScalarArrayOpExprState* arrayestate = (ScalarArrayOpExprState*)state;
    ScalarArrayOpExpr* arrayexpr = (ScalarArrayOpExpr*)state->expr;

    /* Not support All(array[...]) yet */
    if (!arrayexpr->useOr)
        return false;

    /* Lists of all the supported operations */
    switch (arrayexpr->opno) {
        case INT2EQOID:
        case INT4EQOID:
        case INT8EQOID:
        case INT48EQOID:
        case INT84EQOID:
        case TEXTEQOID:
        case BPCHAREQOID:
        case DATEEQOID:
        case FLOAT4EQOID:
        case FLOAT8EQOID:
        case TIMESTAMPEQOID:
        case TIMETZEQOID:
        case INTERVALEQOID:
            break;
        default:
            return false;
    }

    /*
     * Make sure the left operand and the right operand
     * support codegen, or return false.
     */
    List* args = arrayestate->fxprstate.args;
    ExprState* lestate = (ExprState*)linitial(args);
    if (!ExprJittable(lestate))
        return false;

    ExprState* restate = (ExprState*)lsecond(args);
    if (!IsA(restate->expr, Const))
        return false;

    /* not support null value yet */
    Const* cst = (Const*)restate->expr;
    if (cst->constisnull)
        return false;

    /*
     * Avoid codegeneration when const lists is too long, since
     * it may take a long time to compile it.
     */
    if (BPCHAREQOID == arrayexpr->opno || TEXTEQOID == arrayexpr->opno) {
        ArrayType* arrtyp = DatumGetArrayTypeP(cst->constvalue);
        int nitems = ArrayGetNItems(ARR_NDIM(arrtyp), ARR_DIMS(arrtyp));
        if (nitems > ARRAY_LIMIT_SIZE)
            return false;
    }

    state->exprCodeGen = (exprFakeCodeGenSig)&VecExprCodeGen::ScalarArrayCodeGen;
    return true;
}

bool VecExprCodeGen::BooleanTestJittable(ExprState* state)
{
    GenericExprState* bstate = (GenericExprState*)state;
    ExprState* arg = bstate->arg;

    if (!ExprJittable(arg))
        return false;

    state->exprCodeGen = (exprFakeCodeGenSig)&VecExprCodeGen::BooleanTestCodeGen;
    return true;
}

bool VecExprCodeGen::NullTestJittable(ExprState* state)
{
    NullTestState* nullteststate = (NullTestState*)state;
    NullTest* ntexpr = (NullTest*)state->expr;
    ExprState* arg = nullteststate->arg;

    if (!ExprJittable(arg))
        return false;
    if (ntexpr->nulltesttype != IS_NULL && ntexpr->nulltesttype != IS_NOT_NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmodule(MOD_LLVM),
                errmsg("unrecognized nulltesttype: %d", (int)ntexpr->nulltesttype)));
        return false;
    }
    state->exprCodeGen = (exprFakeCodeGenSig)&VecExprCodeGen::NullTestCodeGen;
    return true;
}

bool VecExprCodeGen::CaseJittable(ExprState* state)
{
    CaseExprState* casestate = (CaseExprState*)state;
    CaseExpr* caseexpr = (CaseExpr*)state->expr;
    ListCell* cell = NULL;

    switch (caseexpr->casetype) {
        case BOOLOID:
        case INT1OID:
        case INT4OID:
        case INT8OID:
        case FLOAT4OID:
        case FLOAT8OID:
        case TEXTOID:
        case NUMERICOID:
        case BPCHAROID:
        case VARCHAROID:
        case DATEOID:
        case TIMEOID:
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
        case INTERVALOID:
        case OIDOID:
            break;
        default:
            return false;
    }

    /* If exists caseexpr argument, we should consider it */
    if (casestate->arg != NULL) {
        if (!ExprJittable(casestate->arg))
            return false;
    }

    /* Consider both when clause and result clause */
    foreach (cell, casestate->args) {
        CaseWhenState* wclause = (CaseWhenState*)lfirst(cell);
        if (!ExprJittable(wclause->expr) || !ExprJittable(wclause->result))
            return false;
    }

    /* Consider default caluse */
    ExprState* case_default = casestate->defresult;
    if (case_default != NULL) {
        if (!ExprJittable(case_default))
            return false;
    }

    state->exprCodeGen = (exprFakeCodeGenSig)&VecExprCodeGen::CaseCodeGen;
    return true;
}

bool VecExprCodeGen::NullIfJittable(ExprState* state)
{
    NullIfExpr* opexpr = (NullIfExpr*)state->expr;
    FuncExprState* fstate = (FuncExprState*)state;
    List* args = fstate->args;
    ExprState* lestate = NULL;
    ExprState* restate = NULL;

    if (fstate->func.fn_oid != InvalidOid)
        return false;

    switch (opexpr->opresulttype) {
        case BOOLOID:
        case INT1OID:
        case INT2OID:
        case INT4OID:
        case INT8OID:
        case FLOAT4OID:
        case FLOAT8OID:
        case NUMERICOID:
        case DATEOID:
        case TIMEOID:
        case TIMETZOID:
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
        case INTERVALOID:
        case BPCHAROID:
        case VARCHAROID:
        case TEXTOID:
        case OIDOID:
            break;
        default:
            return false;
    }

    if (list_length(args) != 2)
        return false;

    lestate = (ExprState*)linitial(args);
    restate = (ExprState*)lsecond(args);

    if (!ExprJittable(lestate) || !ExprJittable(restate))
        return false;

    state->exprCodeGen = (exprFakeCodeGenSig)&VecExprCodeGen::NullIfCodeGen;
    return true;
}

bool VecExprCodeGen::BoolJittable(ExprState* state)
{
    BoolExprState* bstate = (BoolExprState*)state;
    BoolExpr* bexpr = (BoolExpr*)state->expr;

    List* boolargs = bstate->args;
    ListCell* cell = NULL;

    /*
     * And/Or/Not Exprs are belong to Bool Expr. While we make a differentce
     * between And/Or and Not Expr.
     */
    switch (bexpr->boolop) {
        case AND_EXPR:
        case OR_EXPR: {
            foreach (cell, boolargs) {
                ExprState* cstate = (ExprState*)lfirst(cell);
                if (!ExprJittable(cstate))
                    return false;
            }
            state->exprCodeGen = (exprFakeCodeGenSig)&VecExprCodeGen::AndOrLogicCodeGen;
        } break;
        case NOT_EXPR: {
            /* There is only one argument in Not expr */
            ExprState* nexpr = (ExprState*)linitial(boolargs);
            if (!ExprJittable(nexpr))
                return false;
            state->exprCodeGen = (exprFakeCodeGenSig)&VecExprCodeGen::NotCodeGen;
        } break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmodule(MOD_LLVM),
                    errmsg("unrecognized boolop: %d", (int)bexpr->boolop)));
            break;
    }

    return true;
}

bool VecExprCodeGen::RelabelJittable(ExprState* state)
{
    /*
     * Only support the cast from varchar to text
     */
    GenericExprState* gstate = (GenericExprState*)state;
    RelabelType* RT = (RelabelType*)state->expr;
    if (IsA(gstate->arg->expr, Var)) {
        Var* var = (Var*)gstate->arg->expr;
        if (var->vartype != VARCHAROID || RT->resulttype != TEXTOID)
            return false;
        ExprState* varg = (ExprState*)(gstate->arg);
        varg->exprCodeGen = (exprFakeCodeGenSig)&VecExprCodeGen::VarCodeGen;
    } else if (IsA(gstate->arg->expr, CaseTestExpr)) {
        /*
         * When exist CaseTestExpr, we only need to get the second argument
         * (Const). Usualy, we have the Relabel Node for string type Const
         * Node.
         */
        CaseTestExpr* ctexpr = (CaseTestExpr*)gstate->arg->expr;
        if (ctexpr->typeId != TEXTOID && ctexpr->typeId != VARCHAROID)
            return false;
    } else
        return false;

    state->exprCodeGen = (exprFakeCodeGenSig)&VecExprCodeGen::RelabelCodeGen;
    return true;
}

bool VecExprCodeGen::FuncJittable(ExprState* state)
{
    FuncExprState* fstate = (FuncExprState*)state;
    FuncExpr* funcexpr = (FuncExpr*)state->expr;
    const FmgrBuiltin* fbp = NULL;
    bool isnull = false;

    /* Return false if function returns set */
    if (funcexpr->funcretset)
        return false;

    /*
     * Return false if function takes no args
     * Handle function like random().
     */
    if (!fstate->args)
        return false;

    /* Return false if number of args is large than 3 */
    int num_args = fstate->args->length;
    if (num_args > 3)
        return false;

    Oid funcid = funcexpr->funcid;

    /*
     * Without doing codegeneration when UDF is defined in fencedMode.
     */
    if ((fbp = fmgr_isbuiltin(funcid)) == NULL) {
        HeapTuple procedureTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));

        if (!HeapTupleIsValid(procedureTuple))
            ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmodule(MOD_LLVM),
                    errmsg("cache lookup failed for function %u", funcid)));

        Datum procFenced = SysCacheGetAttr(PROCOID, procedureTuple, Anum_pg_proc_fenced, &isnull);
        bool fencedMode = DatumGetBool(procFenced) && !isnull;
        /* Do not support fenced udf in codegen */
        if (fencedMode)
            return false;

        ReleaseSysCache(procedureTuple);
    }

    switch (funcid) {
        case SUBSTRFUNCOID: {
            /* Only support ASCII and UTF-8 encoding */
            int current_encoding = GetDatabaseEncoding();
            if (current_encoding != PG_SQL_ASCII && current_encoding != PG_UTF8)
                return false;

            List* func_args = fstate->args;
            ExprState* astate1 = (ExprState*)linitial(func_args);
            ExprState* astate2 = (ExprState*)lsecond(func_args);
            ExprState* astate3 = (ExprState*)lthird(func_args);

            /* Only support the first expr to be FuncExpr/RelabelType/Var */
            if (IsA(astate1->expr, FuncExpr) || IsA(astate1->expr, RelabelType) || IsA(astate1->expr, Var)) {
                if (!ExprJittable(astate1))
                    return false;
            } else
                return false;
            /* The second and third parameter should be const value */
            if (!IsA(astate2->expr, Const) || !IsA(astate3->expr, Const))
                return false;
            else {
                /*
                 * For lpad, if the length is less than zero, no need to do codegen,
                 * and for substr, if either start position or  length is less than zero, no need to do codegen
                 */
                Const* cst2 = (Const*)astate2->expr;
                Const* cst3 = (Const*)astate3->expr;
                if (cst2 != NULL) {
                    int cst_val2 = DatumGetInt32(cst2->constvalue);
                    if (cst_val2 < 0)
                        return false;
                }
                if (cst3 != NULL && funcexpr->funcid == SUBSTRFUNCOID) {
                    int cst_val3 = DatumGetInt32(cst3->constvalue);
                    if (cst_val3 < 0)
                        return false;
                }
                astate2->exprCodeGen = (exprFakeCodeGenSig)&VecExprCodeGen::ConstCodeGen;
                astate3->exprCodeGen = (exprFakeCodeGenSig)&VecExprCodeGen::ConstCodeGen;
            }
        } break;
        /*
         * For btrim1/rtrim1, only consider one argument with set fixed as ' '
         */
        case RTRIMPARAFUNCOID:
        case RTRIM1FUNCOID:
        case BTRIMFUNCOID:
        case BPLENFUNCOID: {
            /* Only support ASCII and UTF-8 encoding */
            int current_encoding = GetDatabaseEncoding();
            if (current_encoding != PG_SQL_ASCII && current_encoding != PG_UTF8)
                return false;

            List* args = fstate->args;
            ExprState* istate = (ExprState*)linitial(args);
            if (!ExprJittable(istate))
                return false;
        } break;
        default: {
            /*
             * Call original C-Function without codegen
             */
            List* func_args = fstate->args;
            ListCell* func_cell = NULL;
            ExprState* estate_arg = NULL;
            foreach (func_cell, func_args) {
                estate_arg = (ExprState*)lfirst(func_cell);
                if (!ExprJittable(estate_arg))
                    return false;
            }
        } break;
    }

    state->exprCodeGen = (exprFakeCodeGenSig)&VecExprCodeGen::FuncCodeGen;
    return true;
}

bool VecExprCodeGen::NumericOpFastJittable(ExprState* state)
{
    OpExpr* opexpr = (OpExpr*)state->expr;
    FuncExprState* fstate = (FuncExprState*)state;
    List* args = fstate->args;
    ExprState* lestate = NULL;
    ExprState* restate = NULL;
    Const* cst = NULL;

    /* construct fast path only for numeric eq/neq/lt/le/gt/ge */
    switch (opexpr->opno) {
        case NUMERICEQOID:
        case NUMERICNEOID:
        case NUMERICLTOID:
        case NUMERICLEOID:
        case NUMERICGTOID:
        case NUMERICGEOID:
            break;
        default:
            return false;
    }

    /* only consider simple case : variable vs const */
    lestate = (ExprState*)linitial(args);
    restate = (ExprState*)lsecond(args);
    if (!((IsA(lestate->expr, Var) && IsA(restate->expr, Const)) ||
            (IsA(lestate->expr, Const) && IsA(restate->expr, Var))))
        return false;

    /* if left expr is var, then the right expr is const */
    if (IsA(lestate->expr, Var)) {
        Var* var = (Var*)(lestate->expr);
        bool isNumericOid = (var->vartype != NUMERICOID || var->vartypmod == -1);
        if (isNumericOid)
            return false;

        /* get the precision of this attrbiute column */
        int prec = (var->vartypmod >> 16) & 0xFFFF;
        if (prec > 18)
            return false;

        cst = (Const*)(restate->expr);
        /* only consider not null numeric type const value */
        if (cst->consttype != NUMERICOID || cst->constisnull)
            return false;

        ScalarValue val = ScalarVector::DatumToScalar(cst->constvalue, cst->consttype, cst->constisnull);

        if ((((NumericData*)val)->choice.n_header & NUMERIC_BI_MASK) != NUMERIC_64)
            return false;
    }

    if (IsA(lestate->expr, Const)) {
        Var* var = (Var*)(restate->expr);
        if (var->vartype != NUMERICOID || var->vartypmod == -1)
            return false;

        /* get the precision of this attrbiute column */
        int prec = (var->vartypmod >> 16) & 0xFFFF;
        if (prec > 18)
            return false;

        cst = (Const*)(lestate->expr);
        /* only consider not null numeric type const value */
        if (cst->consttype != NUMERICOID || cst->constisnull)
            return false;

        ScalarValue val = ScalarVector::DatumToScalar(cst->constvalue, cst->consttype, cst->constisnull);

        if ((((NumericData*)val)->choice.n_header & NUMERIC_BI_MASK) != NUMERIC_64)
            return false;
    }

    return true;
}

bool VecExprCodeGen::OpJittable(ExprState* state)
{
    OpExpr* opexpr = (OpExpr*)state->expr;
    FuncExprState* fstate = (FuncExprState*)state;
    List* args = fstate->args;
    ExprState* lestate = NULL;
    ExprState* restate = NULL;

    switch (opexpr->opresulttype) {
        case INT1OID:
        case INT2OID:
        case INT4OID:
        case INT8OID:
        case FLOAT4OID:
        case FLOAT8OID:
        case BOOLOID:
        case DATEOID:
        case TIMEOID:
        case TIMETZOID:
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
        case NUMERICOID:
        case INTERVALOID:
        case BPCHAROID:
        case VARCHAROID:
        case TEXTOID:
        case OIDOID:
            break;
        default:
            return false;
    }

    if (list_length(args) != 2)
        return false;

    lestate = (ExprState*)linitial(args);
    restate = (ExprState*)lsecond(args);

    if (!ExprJittable(lestate) || !ExprJittable(restate))
        return false;

    switch (opexpr->opno) {
        case TEXTLTOID:
        case TEXTGTOID: {
            /* Only support ASCII and UTF-8 encoding */
            int current_encoding = GetDatabaseEncoding();
            if (current_encoding != PG_SQL_ASCII && current_encoding != PG_UTF8)
                return false;

            /* only support 'var op const' cases */
            if (!IsA(restate->expr, Const))
                return false;

            /* only support digit numbers */
            Const* cst = (Const*)restate->expr;
            if (cst != NULL) {
                text* cst_val = (text*)(cst->constvalue);
                char* data = VARDATA_ANY(cst_val);
                int len = VARSIZE_ANY_EXHDR(cst_val);
                for (int i = 0; i < len; i++) {
                    if (data[i] < '0' || data[i] > '9')
                        return false;
                }
            } else {
                ereport(ERROR,
                    (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmodule(MOD_LLVM),
                        errmsg("Failed to get the const node of the operation!\n")));
            }
        } break;
        case OID_TEXT_LIKE_OP: {
            /* Only support ASCII encoding */
            int current_encoding = GetDatabaseEncoding();
            if (current_encoding != PG_SQL_ASCII && current_encoding != PG_UTF8)
                return false;

            /* Only support 'var like const' cases */
            if (!IsA(restate->expr, Const))
                return false;

            /*
             * we only consider pattern string like 'pattern%' or '%pattern',
             * or 'pattarn' and there is no wild char in 'pattern'.
             */
            Const* cst = (Const*)restate->expr;
            text* cst_val = (text*)(cst->constvalue);
            char* data = VARDATA_ANY(cst_val);
            int len = VARSIZE_ANY_EXHDR(cst_val);

            if (len <= 1)
                return false;

            if (data[0] == STR_PER) {
                /*
                 * check whether there is a wild char '%', '\\', or ' ' in
                 * 'pattern' part in case of '%pattern' */
                for (int i = 1; i < len; i++) {
                    if (data[i] == STR_UNL || data[i] == STR_PER || data[i] == STR_SLA)
                        return false;
                }
            } else if (data[len - 1] == STR_PER) {
                for (int i = 0; i < len - 1; i++) {
                    if (data[i] == STR_UNL || data[i] == STR_PER || data[i] == STR_SLA)
                        return false;
                }
            } else {
                for (int i = 0; i < len; i++) {
                    if (data[i] == STR_UNL || data[i] == STR_PER || data[i] == STR_SLA)
                        return false;
                }
            }
        } break;
        case TEXTNOTLIKEOID: {
            /* Only support ASCII encoding */
            int current_encoding = GetDatabaseEncoding();
            if (current_encoding != PG_SQL_ASCII && current_encoding != PG_UTF8)
                return false;

            /* Only support 'var like const' cases */
            if (!IsA(restate->expr, Const))
                return false;

            /*
             * we only consider pattern string contain special character '%', without
             * considering '_' and '\\' .
             */
            Const* cst = (Const*)restate->expr;
            text* cst_val = (text*)(cst->constvalue);
            char* data = VARDATA_ANY(cst_val);
            int len = VARSIZE_ANY_EXHDR(cst_val);

            if (len <= 1)
                return false;

            /* we only consider pattern string like containing '%' */
            for (int i = 0; i < len; i++) {
                if (data[i] == STR_UNL || data[i] == STR_SLA)
                    return false;
            }
        } break;
        case TIMESTAMPEQOID:
        case TIMESTAMPNEOID:
        case TIMESTAMPLTOID:
        case TIMESTAMPLEOID:
        case TIMESTAMPGTOID:
        case TIMESTAMPGEOID: {
#ifdef HAVE_INT64_TIMESTAMP
#else
            return false;
#endif
        } break;
        default:
            /* call the original c-function */
            break;
    }

    state->exprCodeGen = (exprFakeCodeGenSig)&VecExprCodeGen::OpCodeGen;
    return true;
}

bool VecExprCodeGen::VarJittable(ExprState* state)
{
    Var* var = (Var*)state->expr;
    /* If var is in sysattrlist, we do not codegen the var expr. */
    if (var->varattno < 0)
        return false;

    switch (var->vartype) {
        case BOOLOID:
        case INT1OID:
        case INT2OID:
        case INT4OID:
        case INT8OID:
        case FLOAT4OID:
        case FLOAT8OID:
        case NUMERICOID:
        case DATEOID:
        case TIMEOID:
        case TIMETZOID:
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
        case INTERVALOID:
        case BPCHAROID:
        case VARCHAROID:
        case TEXTOID:
        case OIDOID:
            state->exprCodeGen = (exprFakeCodeGenSig)&VecExprCodeGen::VarCodeGen;
            return true;
        default:
            return false;
    }
}

bool VecExprCodeGen::ConstJittable(ExprState* state)
{
    Const* con = (Const*)state->expr;
    switch (con->consttype) {
        case BOOLOID:
        case INT1OID:
        case INT2OID:
        case INT4OID:
        case INT8OID:
        case FLOAT4OID:
        case FLOAT8OID:
        case NUMERICOID:
        case DATEOID:
        case TIMEOID:
        case TIMETZOID:
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
        case INTERVALOID:
        case BPCHAROID:
        case VARCHAROID:
        case TEXTOID:
        case OIDOID:
            state->exprCodeGen = (exprFakeCodeGenSig)&VecExprCodeGen::ConstCodeGen;
            return true;
        default:
            return false;
    }
}

bool VecExprCodeGen::TargetListJittable(List* targetlist)
{
    ListCell* cell = NULL;

    if (!u_sess->attr.attr_sql.enable_codegen || IS_PGXC_COORDINATOR || targetlist == NULL)
        return false;

    /*
     * Iterator over all the targetlists, if one of them can not be llvm
     * optimized, the whole qual can not be llvm optimized.
     */
    foreach (cell, targetlist) {
        GenericExprState* gstate = (GenericExprState*)lfirst(cell);
        ExprState* tstate = gstate->arg;
        if (!ExprJittable(tstate))
            return false;
    }

    return true;
}

/*
 * To fully use the 'data centric' thought, we exchange the position of qual
 * and batch, which means we use the following
 *         for(i = 0; i < batch->m_rows; i++){
 *             foreach(cell, qual){
 *	               expr = lfirst(cell)
 *                 ExecEvalQual(expr, batch->m_arr[attno]->m_vals[i])
 *             }
 *         }
 * instead of
 *         foreach(cell, qual){
 *		       expr = lfirst(cell)
 *             for(i = 0; i < batch->m_rows; i++){
 *                 ExecEvalQual(expr, batch->m_arr[attno]->m_vals[i])
 *             }
 *         }
 *
 * We show here an example to interpret the generated IR function with the following
 * IR API Builder Code. If we have the query:
 *       select * from test where where (a + 1) * 2 > 4;
 * then we arrived at the following IR function:
 *
 *  define %class.ScalarVector* @JittedVecQual(%struct.ExprContext* %econtext) {
 *	entry:
 *	  %0 = alloca i8
 *	  %1 = getelementptr inbounds %struct.ExprContext* %econtext, i64 0, i32 17
 *	  %scanbatch = load %class.VectorBatch** %1, align 8
 *	  %2 = getelementptr inbounds %class.VectorBatch* %scanbatch, i64 0, i32 3
 *	  %m_sel = load i8** %2, align 8
 *	  call void @llvm.memset.p0i8.i64(i8* %m_sel, i8 0, i64 1000, i32 1, i1 false)
 *	  %3 = getelementptr inbounds %class.VectorBatch* %scanbatch, i64 0, i32 0
 *	  %m_rows = load i32* %3, align 4
 *	  %4 = icmp sgt i32 %m_rows, 0
 *	  br i1 %4, label %for_body, label %ret_bb
 *
 *	for_body:                                         ; preds = %bb_end, %entry
 *	  %5 = phi i64 [ 0, %entry ], [ %7, %bb_end ]
 *	  %6 = phi i1 [ false, %entry ], [ %11, %bb_end ]
 *	  %7 = add i64 %5, 1
 *	  %EvalVecOp = call i64 @JittedVecOper(%struct.ExprContext* %econtext, i8* %0, i64 %5)
 *	  %8 = trunc i64 %EvalVecOp to i1
 *	  br label %bb_end
 *
 *	bb_end:                                           ; preds = %for_body
 *	  %9 = getelementptr inbounds i8* %m_sel, i64 %5
 *	  %10 = zext i1 %8 to i8
 *	  store i8 %10, i8* %9, align 1
 *	  %11 = or i1 %8, %6
 *	  %12 = trunc i64 %7 to i32
 *	  %13 = icmp eq i32 %12, %m_rows
 *	  br i1 %13, label %for_end, label %for_body
 *
 *	for_end:                                          ; preds = %bb_end
 *	  %14 = icmp eq i1 %11, false
 *	  br i1 %14, label %ret_bb, label %for2_begin
 *
 *	for2_begin:                                       ; preds = %for_end
 *	  %15 = icmp slt i64 %7, 1000
 *	  br i1 %15, label %for2_body, label %ret_bb
 *
 *	for2_body:                                        ; preds = %for2_body, %for2_begin
 *	  %16 = phi i64 [ %7, %for2_begin ], [ %17, %for2_body ]
 *	  %17 = add i64 %16, 1
 *	  %18 = getelementptr inbounds i8* %m_sel, i64 %16
 *	  store i8 0, i8* %18, align 1
 *	  %19 = icmp eq i64 %17, 1000
 *	  br i1 %19, label %ret_bb, label %for2_body
 *
 *	ret_bb:                                           ; preds = %for2_body, %for2_begin, %for_end, %entry
 *	  %20 = phi i64 [ 0, %entry ], [ 0, %for_end ], [ 1, %for2_begin ], [ 1, %for2_body ]
 *	  %21 = inttoptr i64 %20 to %class.ScalarVector*
 *	  ret %class.ScalarVector* %21
 *	}
 */
llvm::Function* VecExprCodeGen::QualCodeGen(List* qual, PlanState* parent, bool isReset)
{
    Assert(NULL != (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj);
    dorado::GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    /* if can not code gen qual, just return false */
    if (!QualJittable(qual))
        return NULL;

    /* Find and load the IR file from the installaion directory */
    llvmCodeGen->loadIRFile();

    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    /* Define the data type, const value, and some variables */
    DEFINE_CG_TYPE(int1Type, BITOID);
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(scalarVectorPtrType, "class.ScalarVector");

    /* Get the offset of element in data struct */
    DEFINE_CGVAR_INT1(int1_0, 0);
    DEFINE_CGVAR_INT8(int8_0, 0);
    DEFINE_CGVAR_INT8(int8_1, 1);
    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(m_seloffset, 3);
    DEFINE_CGVAR_INT32(ecxt_scanbatch_offset, 17);
    DEFINE_CGVAR_INT32(ecxt_tupmem_offset, 5);
    DEFINE_CGVAR_INT64(Datum_0, 0);
    DEFINE_CGVAR_INT64(Datum_1, 1);
    DEFINE_CGVAR_INT64(m_BatchMaxSize, BatchMaxSize);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(ExprContextPtrType, "struct.ExprContext");
    DEFINE_CG_PTRTYPE(VecBatchPtrType, "class.VectorBatch");
    DEFINE_CG_PTRTYPE(MemContextPtrType, "struct.MemoryContextData");

    llvm::BasicBlock* bb_next = NULL;
    llvm::BasicBlock** bb_list = NULL;
    llvm::PHINode* Phi_idx = NULL;
    llvm::PHINode* Phi_idx2 = NULL;
    llvm::PHINode* Phi_res = NULL;
    llvm::PHINode* Phi_ret = NULL;
    llvm::Value* idx_next = NULL;
    llvm::Value* idx_next2 = NULL;
    llvm::Value* nValues = NULL;
    llvm::Value* econtext = NULL;
    llvm::Value* pSelection = NULL;
    llvm::Value* Val = NULL;
    llvm::Value* pVal = NULL;
    llvm::Value* ecxt_batch = NULL;
    llvm::Value* Vals[] = {Datum_0, int32_0};
    llvm::Value* isNull = NULL;
    llvm::Value* result = NULL;
    llvm::Value* cg_oldContext = NULL;
    llvm::Value* curr_Context = NULL;
    llvm::Value* llvmargs[3];
    llvm::Function* jitted_vecqual = NULL;

    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "JittedVecQual", scalarVectorPtrType);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("econtext", ExprContextPtrType));
    jitted_vecqual = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    econtext = llvmargs[0];

    /*
     * To avoid load values from memory continually, we keep tuple in memory
     * as loog as possible. So the ourter loop is designed for each tuple, the
     * inner loop is designed for each qual. First codegen the loop for
     * for(i = 0; i < nValus, i++)
     */
    llvm::BasicBlock* entry = &jitted_vecqual->getEntryBlock();
    DEFINE_BLOCK(for_body, jitted_vecqual);
    DEFINE_BLOCK(bb_end, jitted_vecqual);
    DEFINE_BLOCK(for_end, jitted_vecqual);
    DEFINE_BLOCK(for2_begin, jitted_vecqual);
    DEFINE_BLOCK(for2_body, jitted_vecqual);
    DEFINE_BLOCK(ret_bb, jitted_vecqual);

    builder.SetInsertPoint(entry);
    isNull = builder.CreateAlloca(int8Type);

    /*
     * Run in short-lived per-tuple context while computing expressions
     * First get the ecxt_per_tuple_memory, since we need to switch to this
     * memory context.
     */
    Vals[0] = Datum_0;
    Vals[1] = ecxt_tupmem_offset;
    curr_Context = builder.CreateInBoundsGEP(econtext, Vals);
    curr_Context = builder.CreateLoad(MemContextPtrType, curr_Context, "per_tuple_memory");
    cg_oldContext = MemCxtSwitToCodeGen(&builder, curr_Context);

    /*
     * Start code generation on the entry of basic block.
     * With CreateLoad we could get the element of the structure
     * according to the offset.
     */
    Vals[1] = ecxt_scanbatch_offset;
    ecxt_batch = builder.CreateInBoundsGEP(econtext, Vals);
    ecxt_batch = builder.CreateLoad(VecBatchPtrType, ecxt_batch, "scanbatch");
    Vals[1] = m_seloffset;
    pSelection = builder.CreateInBoundsGEP(ecxt_batch, Vals);
    pSelection = builder.CreateLoad(int8PtrType, pSelection, "m_sel");

    /*
     * Reset selection vector to be fully loaded
     * Corresponding to 'econtext->ecxt_scanbatch->ResetSelection (true)'
     */
    if (isReset) {
        (void)builder.CreateMemSet(pSelection, int8_1, BatchMaxSize, (llvm::MaybeAlign)1);
    }

    Vals[1] = int32_0;
    Val = builder.CreateInBoundsGEP(ecxt_batch, Vals);
    nValues = builder.CreateLoad(int32Type, Val, "m_rows");

    Val = builder.CreateICmpSGT(nValues, int32_0);
    builder.CreateCondBr(Val, for_body, ret_bb);

    /* start the codegen in basic block for_body */
    builder.SetInsertPoint(for_body);
    Phi_idx = builder.CreatePHI(int64Type, 2);
    Phi_res = builder.CreatePHI(int1Type, 2);

    idx_next = builder.CreateAdd(Phi_idx, Datum_1);
    Phi_idx->addIncoming(Datum_0, entry);
    Phi_idx->addIncoming(idx_next, bb_end);
    Phi_res->addIncoming(int1_0, entry);

    /*
     * For each time, we deal with one tuple. Phi_idx should be passed as
     * 'loop index' in 'for' structure, these arguments will be used in
     * function llvmCodeGenExpr. Each tuple passes through the whole quallist.
     */
    llvmargs[1] = isNull;
    llvmargs[2] = (llvm::Value*)Phi_idx;

    int qual_len = list_length(qual);
    int qual_index = 0;
    int i;
    ListCell* cell = NULL;
    ExprState* node = NULL;

    if (qual_len > 1) {
        bb_list = (llvm::BasicBlock**)palloc(sizeof(llvm::BasicBlock*) * qual_len);
        bb_list[0] = for_body;
    }

    ExprCodeGenArgs args;
    args.parent = parent;
    args.builder = &builder;
    args.llvm_args = &llvmargs[0];

    foreach (cell, qual) {
        node = (ExprState*)lfirst(cell);

        /*
         * code generation the evaluation of each qual expression,
         * one item at a time, which corresponds to
         * qual_result = VectorExprEngine(clause, econtext,
         * 					econtext->ecxt_scanbatch->m_sel, pVector);
         */
        args.exprstate = node;
        result = CodeGen(&args);
        if (result == NULL) {
            jitted_vecqual->eraseFromParent();
            ereport(ERROR,
                (errcode(ERRCODE_CODEGEN_ERROR),
                    errmodule(MOD_LLVM),
                    errmsg("Codegen failed on the procedure of ExecVecQual!")));
        }
        result = builder.CreateTrunc(result, int1Type);

        /*
         * Since in LLVM we deal with one block at a time, we should check
         * if there exists more predicates.
         */
        if (cell->next == NULL) {
            /* no more predicates, finish this block */
            builder.CreateBr(bb_end);
        } else {
            bb_next = llvm::BasicBlock::Create(context, "bb", jitted_vecqual);
            builder.CreateCondBr(result, bb_next, bb_end);
            builder.SetInsertPoint(bb_next);
            bb_list[++qual_index] = bb_next;
        }
    }

    /* all the codegen for the qual lists is done, finish the loop*/
    builder.SetInsertPoint(bb_end);
    if (qual_len > 1) {
        llvm::PHINode* Phi = builder.CreatePHI(int1Type, qual_len);
        for (i = 0; i < qual_len - 1; i++)
            Phi->addIncoming(int1_0, bb_list[i]);

        Phi->addIncoming(result, bb_list[qual_len - 1]);
        result = (llvm::Value*)Phi;
    }

    /* restore the result to m_sel[i], fetch pSelection[Phi_idx] */
    pVal = builder.CreateInBoundsGEP(pSelection, Phi_idx);
    llvm::Value* res_i8 = builder.CreateZExt(result, int8Type);
    builder.CreateStore(res_i8, pVal);

    llvm::Value* bool_res = builder.CreateOr(result, Phi_res);
    /* make sure Phi_idx is always less than nValues */
    llvm::Value* v1 = builder.CreateTrunc(idx_next, int32Type);
    v1 = builder.CreateICmpEQ(v1, nValues);
    builder.CreateCondBr(v1, for_end, for_body);

    /* Phi_res is the PHI node for bool_res in the basic block for_body */
    Phi_res->addIncoming(bool_res, bb_end);

    /*
     * when codegen is done for the loop, Codegen the following code:
     */
    builder.SetInsertPoint(for_end);
    Val = builder.CreateICmpEQ(bool_res, int1_0);
    builder.CreateCondBr(Val, ret_bb, for2_begin);

    builder.SetInsertPoint(for2_begin);
    Val = builder.CreateICmpSLT(idx_next, m_BatchMaxSize);
    builder.CreateCondBr(Val, for2_body, ret_bb);

    builder.SetInsertPoint(for2_body);

    Phi_idx2 = builder.CreatePHI(int64Type, 2);
    idx_next2 = builder.CreateAdd(Phi_idx2, Datum_1);
    Phi_idx2->addIncoming(idx_next, for2_begin);
    Phi_idx2->addIncoming(idx_next2, for2_body);

    pVal = builder.CreateInBoundsGEP(pSelection, Phi_idx2);
    builder.CreateStore(int8_0, pVal);

    v1 = builder.CreateICmpEQ(idx_next2, m_BatchMaxSize);
    builder.CreateCondBr(v1, ret_bb, for2_body);

    /* finish the whole loop:
     * for(i = 0; i < nValues; i++){ foreach(cell, qual){...} }
     * return NULL or return econtext->qual_results
     */
    builder.SetInsertPoint(ret_bb);
    Phi_ret = builder.CreatePHI(int64Type, 4);
    Phi_ret->addIncoming(Datum_0, entry);
    Phi_ret->addIncoming(Datum_0, for_end);
    Phi_ret->addIncoming(Datum_1, for2_begin);
    Phi_ret->addIncoming(Datum_1, for2_body);

    /*
     * Phi_ret has the type int64, need to convert it to a pointer type and
     * return back to the old memory context.
     */
    llvm::Value* ret_val = builder.CreateIntToPtr(Phi_ret, scalarVectorPtrType);
    (void)MemCxtSwitToCodeGen(&builder, cg_oldContext);
    builder.CreateRet(ret_val);

    if (qual_len > 1)
        pfree_ext(bb_list);

    int plan_node_id = parent->plan->plan_node_id;
    llvmCodeGen->FinalizeFunction(jitted_vecqual, plan_node_id);

    /* dump out C-function calls in codegen */
    llvmCodeGen->dumpCFunctionCalls("Filter", plan_node_id);

    /*
     * If coden_strategy is CODEGEN_PURE and having C-function call in codegen,
     * we need to discard the generated IR function.
     */
    if (llvmCodeGen->hasCFunctionCalls()) {
        llvmCodeGen->clearCFunctionCalls();

        if (u_sess->attr.attr_sql.codegen_strategy == CODEGEN_PURE)
            return NULL;
    }

    return jitted_vecqual;
}

llvm::Value* VecExprCodeGen::ScalarArrayCodeGen(ExprCodeGenArgs* args)
{
    dorado::GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder* outer_builder = args->builder;
    GsCodeGen::LlvmBuilder inner_builder(context);

    /* data type and consts needed by LLVM */
    DEFINE_CG_TYPE(int1Type, BITOID);
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int16Type, INT2OID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(ExprContextPtrType, "struct.ExprContext");

    DEFINE_CGVAR_INT8(null_true, 1);
    DEFINE_CGVAR_INT64(Datum_0, 0);
    DEFINE_CGVAR_INT64(Datum_1, 1);

    ScalarArrayOpExprState* arrayexprstate = (ScalarArrayOpExprState*)args->exprstate;
    ScalarArrayOpExpr* arrayexpr = (ScalarArrayOpExpr*)args->exprstate->expr;
    List* arr_args = arrayexprstate->fxprstate.args;
    ExprState* cststate = (ExprState*)lsecond(arr_args);
    Const* cst = (Const*)cststate->expr;
    ArrayType* arr = DatumGetArrayTypeP(cst->constvalue);
    llvm::Value* isNull = NULL;
    llvm::Value* result = NULL;
    llvm::Value* lhs_value = NULL;
    llvm::Value* llvmargs[3];
    llvm::Value* cmp = NULL;
    llvm::Value* tmpNull = NULL;

    /* define LLVM function */
    DEFINE_FUNCTION_WITH_3_ARGS(jitted_arrayop,
        "JittedVecScalarArrayOp",
        "econtext",
        ExprContextPtrType,
        "isNull",
        int8PtrType,
        "loop_index",
        int64Type);

    isNull = llvmargs[1];

    /* define basicblock used in 'switch instrusion' */
    llvm::BasicBlock* entry = &jitted_arrayop->getEntryBlock();
    DEFINE_BLOCK(if_then, jitted_arrayop);
    DEFINE_BLOCK(if_else, jitted_arrayop);
    DEFINE_BLOCK(ret_bb, jitted_arrayop);
    llvm::BasicBlock* current_bb = NULL;
    llvm::BasicBlock* next_bb = NULL;

    inner_builder.SetInsertPoint(entry);

    /* we have to firstly prepare the lhs_value and isNull flag */
    ExprCodeGenArgs largs;
    ExprState* leftstate = (ExprState*)linitial(arr_args);
    largs = *args;
    largs.exprstate = leftstate;
    largs.builder = &inner_builder;
    largs.llvm_args = &llvmargs[0];
    lhs_value = CodeGen(&largs);

    /* if the scalar var is NULL, return NULL, no point in iterating the loop */
    tmpNull = inner_builder.CreateLoad(int8Type, isNull, "tmpNull");

    cmp = inner_builder.CreateICmpEQ(tmpNull, null_true);
    inner_builder.CreateCondBr(cmp, if_then, if_else);
    inner_builder.SetInsertPoint(if_then);
    inner_builder.CreateBr(ret_bb);

    inner_builder.SetInsertPoint(if_else);
    /*
     * Since everything is already 'known' here, we could decode the data from
     * the Const node without checking vecfunexpr:
     * look over the arryelements, and get each arry element.
     */
    int16 typlen;
    bool typbyval = false;
    char typalign;
    bits8* bitmap = NULL;
    uint32 bitmask;
    int i = 0;
    int j = 0;

    /* get array information */
    get_typlenbyvalalign(ARR_ELEMTYPE(arr), &typlen, &typbyval, &typalign);
    int nitems = ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));
    char* s = (char*)ARR_DATA_PTR(arr);
    bitmap = ARR_NULLBITMAP(arr);
    bitmask = 1;
    Datum* Args = (Datum*)palloc(sizeof(Datum) * nitems);
    bool* elemNull = (bool*)palloc(sizeof(bool) * nitems);
    bool resultNull = false;

    /* get arry element */
    for (i = 0; i < nitems; i++) {
        if (bitmap && (*bitmap & bitmask) == 0) {
            elemNull[i] = true;
            Args[i] = (Datum)0;
            /* Mark one of the element is NULL */
            resultNull = true;
        } else {
            elemNull[i] = false;
            Datum elt = fetch_att(s, typbyval, typlen);
            Args[i] = ScalarVector::DatumToScalar(elt, ARR_ELEMTYPE(arr), false);
            s = att_addlength_pointer(s, typlen, s);
            s = (char*)att_align_nominal(s, typalign);
        }

        /* advance bitmap pointer if any */
        if (bitmap != NULL) {
            bitmask <<= 1;
            if (bitmask == 0x100) {
                bitmap++;
                bitmask = 1;
            }
        }
    }

    /* remove null value cases
     */
    for (i = 0; i < nitems; i++) {
        if (elemNull[i]) {
            for (j = i; j < nitems - 1; j++) {
                Args[j] = Args[j + 1];
                elemNull[j] = elemNull[j + 1];
            }
            i--;
            nitems--;
        }
    }

    /*
     * remove duplicate constants in 'switch instruction(with the definition
     * in LLVM)'
     */
    for (i = 0; i < nitems; i++) {
        for (j = i + 1; j < nitems; j++) {
            if (Args[i] == Args[j]) {
                /* Replace Args[j] with the last one and decrement nitems */
                if (j == nitems - 1) {
                    nitems--;
                    break;
                }

                Args[j] = Args[nitems - 1];
                j--;
                nitems--;
            }
        }
    }

    /*
     * Do code generation according to data type
     */
    if (nitems > 0) {
        /*
         * When the number of items is large than zero, we should compare
         * the elements one by one.
         */
        DEFINE_BLOCK(in_bb, jitted_arrayop);
        DEFINE_BLOCK(default_bb, jitted_arrayop);
        switch (arrayexpr->opno) {
            /*
             * For varchar, text and bpchar type, we should deal with elements
             * one by one, and each element is considered in one basic block.
             */
            case TEXTEQOID: {
                current_bb = llvm::BasicBlock::Create(context, "current_bb", jitted_arrayop);

                llvm::Function* func_varlena = llvmCodeGen->module()->getFunction("JittedEvalVarlena");
                if (func_varlena == NULL) {
                    func_varlena = VarlenaCvtCodeGen();
                }

                llvm::Value* lhs_val = inner_builder.CreateCall(func_varlena, lhs_value, "lval");
                llvm::Value* var_len = inner_builder.CreateExtractValue(lhs_val, 0);
                llvm::Value* var_data = inner_builder.CreateExtractValue(lhs_val, 1);
                inner_builder.CreateBr(current_bb);

                /* Enter into the main compare loop */
                for (i = 0; i < nitems; i++) {
                    inner_builder.SetInsertPoint(current_bb);
                    if (i == nitems - 1) {
                        next_bb = default_bb;
                    } else {
                        next_bb = llvm::BasicBlock::Create(context, "next_bb", jitted_arrayop);
                    }

                    /* Since all the elements is const data type, we could parse them directly */
                    text* cst_text = DatumGetTextPP(Args[i]);
                    int val_len = VARSIZE_ANY_EXHDR(cst_text);
                    char* val_data = VARDATA_ANY(cst_text);
                    llvm::Value* cst_len = llvmCodeGen->getIntConstant(INT4OID, val_len);
                    llvm::Value* cst_data = llvmCodeGen->CastPtrToLlvmPtr(int8PtrType, (void*)val_data);

                    llvm::Function* func_texteq_cc = llvmCodeGen->module()->getFunction("LLVMIRtexteq");
                    if (func_texteq_cc == NULL) {
                        ereport(ERROR,
                            (errcode(ERRCODE_LOAD_IR_FUNCTION_FAILED),
                                errmodule(MOD_LLVM),
                                errmsg("Failed on getting IR function : LLVMIRtexteq!\n")));
                    }

                    llvm::Value* tmp =
                        inner_builder.CreateCall(func_texteq_cc, {var_len, var_data, cst_len, cst_data}, "texteq");
                    tmp = inner_builder.CreateTrunc(tmp, int1Type);
                    inner_builder.CreateCondBr(tmp, in_bb, next_bb);
                    current_bb = next_bb;
                }
            } break;
            case BPCHAREQOID: {
                current_bb = llvm::BasicBlock::Create(context, "current_bb", jitted_arrayop);

                llvm::Function* func_varlena = llvmCodeGen->module()->getFunction("JittedEvalVarlena");
                if (func_varlena == NULL) {
                    func_varlena = VarlenaCvtCodeGen();
                }

                llvm::Value* lhs_val = inner_builder.CreateCall(func_varlena, lhs_value, "lval");
                llvm::Value* var_len = inner_builder.CreateExtractValue(lhs_val, 0);
                llvm::Value* var_data = inner_builder.CreateExtractValue(lhs_val, 1);

                inner_builder.CreateBr(current_bb);

                for (i = 0; i < nitems; i++) {
                    inner_builder.SetInsertPoint(current_bb);
                    if (i == nitems - 1) {
                        next_bb = default_bb;
                    } else {
                        next_bb = llvm::BasicBlock::Create(context, "next_bb", jitted_arrayop);
                    }

                    /* Since all the elements is const data type, we could parse them directly */
                    BpChar* cst_bpchar = DatumGetBpCharPP(Args[i]);
                    int val_len = VARSIZE_ANY_EXHDR(cst_bpchar);
                    char* cst_char = VARDATA_ANY(cst_bpchar);
                    llvm::Value* cst_len = llvmCodeGen->getIntConstant(INT4OID, val_len);
                    llvm::Value* cst_data = llvmCodeGen->CastPtrToLlvmPtr(int8PtrType, (void*)cst_char);

                    llvm::Function* func_bpchareq_cc = llvmCodeGen->module()->getFunction("LLVMIRbpchareq");
                    if (func_bpchareq_cc == NULL) {
                        ereport(ERROR,
                            (errcode(ERRCODE_LOAD_IR_FUNCTION_FAILED),
                                errmodule(MOD_LLVM),
                                errmsg("Failed on getting IR function : LLVMIRbpchareq!")));
                    }

                    llvm::Value* tmp =
                        inner_builder.CreateCall(func_bpchareq_cc, {var_len, var_data, cst_len, cst_data}, "bpchareq");
                    tmp = inner_builder.CreateTrunc(tmp, int1Type);
                    inner_builder.CreateCondBr(tmp, in_bb, next_bb);
                    current_bb = next_bb;
                }
            } break;
            /*
             * Turn array operation to switch instruction operation defined in LLVM.
             * Create a switch instruction with the specified value, default dest, and
             * with a hint for the number of cases that will be added. We should also
             * pay attention to different data type, since the values stored in llvm
             * and pg may diffrent because of sign.
             */
            case INT2EQOID: {
                lhs_value = inner_builder.CreateTrunc(lhs_value, int16Type);
                llvm::SwitchInst* switchInst = inner_builder.CreateSwitch(lhs_value, default_bb, nitems);
                for (i = 0; i < nitems; i++) {
                    switchInst->addCase(llvm::ConstantInt::get(context, llvm::APInt(16, Args[i], true)), in_bb);
                }
            } break;
            case INT4EQOID:
            case FLOAT4EQOID: {
                lhs_value = inner_builder.CreateTrunc(lhs_value, int32Type);
                llvm::SwitchInst* switchInst = inner_builder.CreateSwitch(lhs_value, default_bb, nitems);
                for (i = 0; i < nitems; i++) {
                    switchInst->addCase(llvm::ConstantInt::get(context, llvm::APInt(32, Args[i], true)), in_bb);
                }
            } break;
            case INT8EQOID:
            case FLOAT8EQOID:
            case DATEEQOID:
            case TIMESTAMPEQOID: {
                llvm::SwitchInst* switchInst = inner_builder.CreateSwitch(lhs_value, default_bb, nitems);
                for (i = 0; i < nitems; i++) {
                    switchInst->addCase(llvm::ConstantInt::get(context, llvm::APInt(64, Args[i], true)), in_bb);
                }
            } break;
            case INT48EQOID: {
                lhs_value = inner_builder.CreateTrunc(lhs_value, int32Type);
                lhs_value = inner_builder.CreateSExt(lhs_value, int64Type);
                llvm::SwitchInst* switchInst = inner_builder.CreateSwitch(lhs_value, default_bb, nitems);
                for (i = 0; i < nitems; i++) {
                    switchInst->addCase(llvm::ConstantInt::get(context, llvm::APInt(64, Args[i], true)), in_bb);
                }
            } break;
            case INT84EQOID: {
                llvm::SwitchInst* switchInst = inner_builder.CreateSwitch(lhs_value, default_bb, nitems);
                for (i = 0; i < nitems; i++) {
                    switchInst->addCase(llvm::ConstantInt::get(context, llvm::APInt(64, (int)Args[i], true)), in_bb);
                }
            } break;
            default: {
                current_bb = llvm::BasicBlock::Create(context, "current_bb", jitted_arrayop);
                inner_builder.CreateBr(current_bb);

                /*
                 * By using EvalFuncResult function, we could evaluate different
                 * kind of operations defined here, say <, >, >=, <= and so on.
                 */
                FuncExprState* fcache = (FuncExprState*)args->exprstate;
                for (i = 0; i < nitems; i++) {
                    inner_builder.SetInsertPoint(current_bb);
                    if (i == nitems - 1)
                        next_bb = default_bb;
                    else
                        next_bb = llvm::BasicBlock::Create(context, "next_bb", jitted_arrayop);

                    llvm::Value* rhs_value = llvmCodeGen->getIntConstant(INT8OID, Args[i]);

                    /*
                     * Prepare the parameters usde by original c-function. For
                     * other cases, we have already remove null values and duplicate
                     * values.
                     */
                    LLVMFuncCallInfo lfcinfo;
                    llvm::Value* inargs[MAX_ARGS_NUM];
                    inargs[0] = lhs_value;
                    inargs[1] = rhs_value;
                    lfcinfo.nargs = list_length(fcache->args);
                    lfcinfo.args = &inargs[0];
                    llvm::Value* tmp = EvalFuncResultCodeGen(&inner_builder, fcache, isNull, &lfcinfo);

                    tmp = inner_builder.CreateTrunc(tmp, int1Type);
                    /* If true, put that element in the in-block */
                    inner_builder.CreateCondBr(tmp, in_bb, next_bb);
                    current_bb = next_bb;
                }
            } break;
        }

        inner_builder.SetInsertPoint(in_bb);
        inner_builder.CreateBr(ret_bb);

        inner_builder.SetInsertPoint(default_bb);
        /*
         * If current value does not match any value in array, when resultNull
         * is NULL, return NULL.
         */
        if (resultNull)
            inner_builder.CreateStore(null_true, isNull);
        inner_builder.CreateBr(ret_bb);

        /*
         * When the element is in 'in_bb' block, rerun true, else return false.
         */
        inner_builder.SetInsertPoint(ret_bb);
        llvm::PHINode* Phi = inner_builder.CreatePHI(int64Type, 3);
        Phi->addIncoming(Datum_0, if_then);
        Phi->addIncoming(Datum_1, in_bb);
        Phi->addIncoming(Datum_0, default_bb);
        inner_builder.CreateRet(Phi);
    } else {
        /* If there is no non-null elements in the array, just return Null */
        inner_builder.CreateBr(ret_bb);
        inner_builder.SetInsertPoint(ret_bb);
        llvm::PHINode* Phi = inner_builder.CreatePHI(int64Type, 3);
        Phi->addIncoming(Datum_0, if_then);
        Phi->addIncoming(Datum_0, if_else);
        inner_builder.CreateRet(Phi);
    }

    pfree_ext(Args);
    pfree_ext(elemNull);

    llvmCodeGen->FinalizeFunction(jitted_arrayop);

    /* call the generated IR function */
    result = outer_builder->CreateCall(
        jitted_arrayop, {args->llvm_args[0], args->llvm_args[1], args->llvm_args[2]}, "VecScalarArrayOp");

    return result;
}

llvm::Value* VecExprCodeGen::BooleanTestCodeGen(ExprCodeGenArgs* args)
{
    GenericExprState* bstate = (GenericExprState*)(args->exprstate);
    BooleanTest* btestexpr = (BooleanTest*)args->exprstate->expr;
    ExprState* lestate = bstate->arg;

    dorado::GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder* outer_builder = args->builder;
    GsCodeGen::LlvmBuilder inner_builder(context);

    /* define data type needed by LLVM */
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(ExprContextPtrType, "struct.ExprContext");

    /* define const value that needed */
    DEFINE_CGVAR_INT64(Datum_0, 0);
    DEFINE_CGVAR_INT64(Datum_1, 1);
    DEFINE_CGVAR_INT8(null_true, 1);
    DEFINE_CGVAR_INT8(null_false, 0);

    /* define variable that needed */
    llvm::Value* isNull = NULL;
    llvm::Value* llvmargs[3];
    llvm::Value* lhs_value = NULL;
    llvm::Value* lhs_flag = NULL;
    llvm::Value* cmp = NULL;
    llvm::Value* res0 = NULL;
    llvm::Value* res1 = NULL;
    llvm::Value* res2 = NULL;
    llvm::Value* result = NULL;

    DEFINE_FUNCTION_WITH_3_ARGS(jitted_booleantest,
        "JittedBooleanTest",
        "econtext",
        ExprContextPtrType,
        "isNull",
        int8PtrType,
        "loop_index",
        int64Type);

    isNull = llvmargs[1];

    llvm::BasicBlock* entry = &jitted_booleantest->getEntryBlock();
    /*
     * Since IS_UNKNOWN and IS_NOT_UNKNOWN do not have any conditional
     * branch, we enter into ret_bb block directly.
     */
    DEFINE_BLOCK(ret_bb, jitted_booleantest);

    inner_builder.SetInsertPoint(entry);

    /* prepare the operand value */
    ExprCodeGenArgs largs;
    largs = *args;
    largs.exprstate = lestate;
    largs.builder = &inner_builder;
    largs.llvm_args = &llvmargs[0];
    lhs_value = CodeGen(&largs);

    lhs_flag = llvmargs[1];
    lhs_flag = inner_builder.CreateLoad(int8Type, lhs_flag, "lflag");
    inner_builder.CreateStore(null_false, isNull);
    cmp = inner_builder.CreateICmpEQ(lhs_flag, null_true);

    if (btestexpr->booltesttype == IS_UNKNOWN || btestexpr->booltesttype == IS_NOT_UNKNOWN) {
        if (btestexpr->booltesttype == IS_UNKNOWN) {
            /* return true if lhs_value is NULL, else return false */
            res0 = lhs_flag;
        } else {
            /* return true if lhs_value is not NULL, else return false */
            cmp = inner_builder.CreateICmpEQ(lhs_flag, null_true);
            res0 = inner_builder.CreateSelect(cmp, Datum_0, Datum_1);
        }

        inner_builder.CreateBr(ret_bb);
        inner_builder.SetInsertPoint(ret_bb);
        res0 = inner_builder.CreateZExt(res0, int64Type);
        inner_builder.CreateRet(res0);
    } else {
        DEFINE_BLOCK(be_null, jitted_booleantest);
        DEFINE_BLOCK(bnot_null, jitted_booleantest);
        inner_builder.CreateCondBr(cmp, be_null, bnot_null);

        switch (btestexpr->booltesttype) {
            /* return false if lhs_value is NULL, else return lhs_value */
            case IS_TRUE:
                inner_builder.SetInsertPoint(be_null);
                res1 = Datum_0;
                inner_builder.CreateBr(ret_bb);

                inner_builder.SetInsertPoint(bnot_null);
                res2 = lhs_value;
                inner_builder.CreateBr(ret_bb);
                break;
            /* return true if lhs_value is NULL, else return not lhs_value */
            case IS_NOT_TRUE:
                inner_builder.SetInsertPoint(be_null);
                res1 = Datum_1;
                inner_builder.CreateBr(ret_bb);

                inner_builder.SetInsertPoint(bnot_null);
                cmp = inner_builder.CreateICmpEQ(lhs_value, Datum_1);
                res2 = inner_builder.CreateSelect(cmp, Datum_0, Datum_1);
                inner_builder.CreateBr(ret_bb);
                break;
            /* return false if lhs_value is NULL, else return not lhs_value */
            case IS_FALSE:
                inner_builder.SetInsertPoint(be_null);
                res1 = Datum_0;
                inner_builder.CreateBr(ret_bb);

                inner_builder.SetInsertPoint(bnot_null);
                cmp = inner_builder.CreateICmpEQ(lhs_value, Datum_1);
                res2 = inner_builder.CreateSelect(cmp, Datum_0, Datum_1);
                inner_builder.CreateBr(ret_bb);
                break;
            /* return true if lhs_value is NULL, else return lhs_value */
            case IS_NOT_FALSE:
                inner_builder.SetInsertPoint(be_null);
                res1 = Datum_1;
                inner_builder.CreateBr(ret_bb);

                inner_builder.SetInsertPoint(bnot_null);
                res2 = lhs_value;
                inner_builder.CreateBr(ret_bb);
                break;
            default:
                ereport(ERROR,
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmodule(MOD_LLVM),
                        errmsg("unrecognized booltexttype: %d", (int)btestexpr->booltesttype)));
                break;
        }
        inner_builder.SetInsertPoint(ret_bb);
        llvm::PHINode* Phi_ret = inner_builder.CreatePHI(int64Type, 2);
        Phi_ret->addIncoming(res1, be_null);
        Phi_ret->addIncoming(res2, bnot_null);
        inner_builder.CreateRet(Phi_ret);
    }

    llvmCodeGen->FinalizeFunction(jitted_booleantest);

    result = outer_builder->CreateCall(
        jitted_booleantest, {args->llvm_args[0], args->llvm_args[1], args->llvm_args[2]}, "EvalVecBoolenTest");
    return result;
}

llvm::Value* VecExprCodeGen::NullTestCodeGen(ExprCodeGenArgs* args)
{
    NullTestState* nteststate = (NullTestState*)(args->exprstate);
    NullTest* ntestexpr = (NullTest*)(args->exprstate->expr);
    ExprState* lestate = nteststate->arg;

    /* Start the process of createing IR function */
    dorado::GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder* outer_builder = args->builder;
    GsCodeGen::LlvmBuilder inner_builder(context);

    /* define data type needed by LLVM */
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(ExprContextPtrType, "struct.ExprContext");

    /* define const value that needed */
    DEFINE_CGVAR_INT64(Datum_0, 0);
    DEFINE_CGVAR_INT64(Datum_1, 1);
    DEFINE_CGVAR_INT8(null_false, 0);

    /* define variable that needed by LLVM*/
    llvm::Value* isNull = NULL;

    llvm::Value* llvmargs[3];
    llvm::Value* arg_flag = NULL;

    llvm::Value* cmp = NULL;
    llvm::Value* res0 = NULL;
    llvm::Value* result = NULL;

    /* Define llvm function */
    DEFINE_FUNCTION_WITH_3_ARGS(jitted_nulltest,
        "JittedNullTest",
        "econtext",
        ExprContextPtrType,
        "isNull",
        int8PtrType,
        "loop_index",
        int64Type);

    isNull = llvmargs[1];

    llvm::BasicBlock* entry = &jitted_nulltest->getEntryBlock();
    inner_builder.SetInsertPoint(entry);

    /* prepare the operand values */
    ExprCodeGenArgs largs;
    largs = *args;
    largs.exprstate = lestate;
    largs.builder = &inner_builder;
    largs.llvm_args = &llvmargs[0];
    (void)CodeGen(&largs);

    arg_flag = llvmargs[1];
    arg_flag = inner_builder.CreateLoad(int8Type, arg_flag, "argflag");
    inner_builder.CreateStore(null_false, isNull);

    if (ntestexpr->nulltesttype == IS_NULL) {
        res0 = inner_builder.CreateZExt(arg_flag, int64Type);
        inner_builder.CreateRet(res0);
    } else {
        arg_flag = inner_builder.CreateZExt(arg_flag, int64Type);
        cmp = inner_builder.CreateICmpEQ(arg_flag, Datum_1);
        res0 = inner_builder.CreateSelect(cmp, Datum_0, Datum_1);
        inner_builder.CreateRet(res0);
    }

    llvmCodeGen->FinalizeFunction(jitted_nulltest);

    result = outer_builder->CreateCall(
        jitted_nulltest, {args->llvm_args[0], args->llvm_args[1], args->llvm_args[2]}, "EvalVecNullTest");
    return result;
}

llvm::Value* VecExprCodeGen::CaseCodeGen(ExprCodeGenArgs* args)
{
    dorado::GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder* outer_builder = args->builder;
    GsCodeGen::LlvmBuilder inner_builder(context);

    /* define data type and variables needed by LLVM */
    DEFINE_CG_TYPE(int1Type, BITOID);
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(ExprContextPtrType, "struct.ExprContext");

    DEFINE_CGVAR_INT1(null_true, 1);
    DEFINE_CGVAR_INT1(int1_0, 0);
    DEFINE_CGVAR_INT64(Datum_0, 0);

    llvm::Value* llvmargs[3];
    llvm::Value* case_arg_value = NULL;
    llvm::Value* case_arg_null = NULL;
    llvm::Value* case_value = NULL;
    llvm::Value* case_null = NULL;
    llvm::Value* case_cond = NULL;
    llvm::Value* case_cond1 = NULL;
    llvm::Value* case_cond2 = NULL;
    llvm::Value* result = NULL;
    llvm::Value* tmp = NULL;
    int i = 0;
    int clause_index = 0;

    DEFINE_FUNCTION_WITH_3_ARGS(jitted_caseexpr,
        "JittedVecCase",
        "econtext",
        ExprContextPtrType,
        "isNull",
        int8PtrType,
        "loop_index",
        int64Type);

    llvm::Value* isNull = llvmargs[1];

    llvm::BasicBlock* entry = &jitted_caseexpr->getEntryBlock();
    DEFINE_BLOCK(ret_bb, jitted_caseexpr);
    llvm::BasicBlock* current_bb = NULL;
    llvm::BasicBlock* next_bb = NULL;

    /*
     * Extract caseexpr information from node, case_arg decides different
     * forms of case expr.
     */
    CaseExprState* cstate = (CaseExprState*)(args->exprstate);

    ExprState* case_arg = cstate->arg;
    ExprCodeGenArgs case_expr_arg;
    case_expr_arg = *args;
    case_expr_arg.builder = &inner_builder;
    case_expr_arg.llvm_args = &llvmargs[0];

    List* clauses = cstate->args;
    ExprCodeGenArgs case_when_arg;
    case_when_arg = *args;
    case_when_arg.builder = &inner_builder;
    case_when_arg.llvm_args = &llvmargs[0];

    ExprState* case_default = cstate->defresult;
    ExprCodeGenArgs case_default_arg;
    case_default_arg = *args;
    case_default_arg.builder = &inner_builder;
    case_default_arg.llvm_args = &llvmargs[0];

    ExprCodeGenArgs case_result_arg;
    case_result_arg = *args;
    case_result_arg.builder = &inner_builder;
    case_result_arg.llvm_args = &llvmargs[0];

    ExprState* case_arg_expr = NULL;
    ListCell* cell = NULL;

    /*
     * If there's a test expression, we have to evaluate it and save the value
     * where the CaseTestExpr placeholders can find it. We must save and
     * restore prior setting of econtext's caseValue fields, in case this node
     * is itself within a larger CASE.
     */
    if (case_arg != NULL) {
        inner_builder.SetInsertPoint(entry);
        case_expr_arg.exprstate = case_arg;
        case_arg_value = CodeGen(&case_expr_arg);
        case_arg_null = llvmargs[1];
        case_arg_null = inner_builder.CreateLoad(int8Type, case_arg_null, "case_arg_flag");
        if (case_arg_value == NULL)
            return NULL;
    }

    /*
     * we evaluate each of the WHEN clauses in turn, as soon as one is true we
     * return the corresponding result. If none are true then we return the
     * value of the default clause, or NULL if there is none.
     *
     * the following code is designed to cover foreach(clause, clauses){...}
     * Since each case... is a basicblock, we need a basicblock array
     */
    int casewhen_len = list_length(clauses);
    llvm::BasicBlock** clause_bb = (llvm::BasicBlock**)palloc(sizeof(llvm::BasicBlock*) * (casewhen_len + 1));
    llvm::BasicBlock** if_then = (llvm::BasicBlock**)palloc(sizeof(llvm::BasicBlock*) * (casewhen_len + 1));
    llvm::BasicBlock** if_else = (llvm::BasicBlock**)palloc(sizeof(llvm::BasicBlock*) * (casewhen_len + 1));
    llvm::BasicBlock** check_bb = (llvm::BasicBlock**)palloc(sizeof(llvm::BasicBlock*) * (casewhen_len + 1));
    llvm::Value** clause_result = (llvm::Value**)palloc(sizeof(llvm::Value*) * (casewhen_len + 1));
    current_bb = entry;
    foreach (cell, clauses) {
        CaseWhenState* wclause = (CaseWhenState*)lfirst(cell);
        clause_bb[clause_index] = llvm::BasicBlock::Create(context, "clause_bb", jitted_caseexpr);
        check_bb[clause_index] = llvm::BasicBlock::Create(context, "check_bb", jitted_caseexpr);
        next_bb = llvm::BasicBlock::Create(context, "next_bb", jitted_caseexpr);

        /*
         * Since we could not call every function through ExecStrictVecOpExpr, so
         * we should list all the cases we supported here, it should be
         * changed later.
         */
        inner_builder.SetInsertPoint(current_bb);
        if (case_arg == NULL) {
            case_when_arg.exprstate = (ExprState*)wclause->expr;
            case_value = CodeGen(&case_when_arg);
            if (case_value == NULL) {
                next_bb->eraseFromParent();
                return NULL;
            }

            case_cond = inner_builder.CreateTrunc(case_value, int1Type);
            inner_builder.CreateBr(check_bb[clause_index]);
            inner_builder.SetInsertPoint(check_bb[clause_index]);
        } else {

            if_then[clause_index] = llvm::BasicBlock::Create(context, "if_then", jitted_caseexpr);
            if_else[clause_index] = llvm::BasicBlock::Create(context, "if_else", jitted_caseexpr);
            if (IsA(wclause->expr->expr, CaseTestExpr)) {
                /*
                 * When there is only a CaseTextExpr here, the typeId is
                 * Boolean and we could get the result according to
                 * case_arg_value. (with 'when true' syntax)
                 */
                tmp = inner_builder.CreateTrunc(case_arg_null, int1Type);
                inner_builder.CreateCondBr(tmp, if_else[clause_index], if_then[clause_index]);
                inner_builder.SetInsertPoint(if_then[clause_index]);
                case_cond1 = case_arg_value;
            } else if (IsA(wclause->expr->expr, BoolExpr)) {
                /*
                 * When there is only a BoolExpr node with 'not' type, its
                 * argument is CastTestExpr. We could get the result accoding
                 * to not case_arg_value. (with 'when false' syntax)
                 */
                tmp = inner_builder.CreateTrunc(case_arg_null, int1Type);
                inner_builder.CreateCondBr(tmp, if_else[clause_index], if_then[clause_index]);
                inner_builder.SetInsertPoint(if_then[clause_index]);
                case_cond1 = inner_builder.CreateNot(case_arg_value);
            } else {
                /*
                 * the wclause->expr's first arg is CASETESTEXPR, its second
                 * arg is case_value different from 'case_arg==NULL',
                 * case_value should be compared with case_arg_value to see
                 * if this branch is satisfied or not.
                 */
                FuncExprState* arg_expr = (FuncExprState*)wclause->expr;
                case_arg_expr = (ExprState*)(lsecond(arg_expr->args));
                case_when_arg.exprstate = case_arg_expr;
                case_value = CodeGen(&case_when_arg);
                case_null = llvmargs[1];
                case_null = inner_builder.CreateLoad(int8Type, case_null, "case_flag");
                tmp = inner_builder.CreateOr(case_arg_null, case_null);
                tmp = inner_builder.CreateTrunc(tmp, int1Type);
                inner_builder.CreateCondBr(tmp, if_else[clause_index], if_then[clause_index]);
                inner_builder.SetInsertPoint(if_then[clause_index]);
                OpExpr* aexpr = (OpExpr*)arg_expr->xprstate.expr;

                switch (aexpr->opno) {
                    case BooleanEqualOperator: {
                        llvm::Function* func_booleq = llvmCodeGen->module()->getFunction("Jitted_booleq");
                        if (func_booleq == NULL) {
                            func_booleq = booleq_codegen();
                        }
                        case_cond1 = inner_builder.CreateCall(func_booleq, {case_arg_value, case_value});
                    } break;
                    case INT4EQOID: {
                        llvm::Function* func_int4eq = llvmCodeGen->module()->getFunction("Jitted_int4eq");
                        if (func_int4eq == NULL) {
                            func_int4eq = int4_sop_codegen<SOP_EQ>();
                        }
                        case_cond1 = inner_builder.CreateCall(func_int4eq, {case_arg_value, case_value});
                    } break;
                    case INT8EQOID: {
                        llvm::Function* func_int8eq = llvmCodeGen->module()->getFunction("Jitted_int8eq");
                        if (func_int8eq == NULL) {
                            func_int8eq = int8_sop_codegen<SOP_EQ>();
                        }
                        case_cond1 = inner_builder.CreateCall(func_int8eq, {case_arg_value, case_value});
                    } break;
                    case INT24EQOID: {
                        llvm::Function* func_int24eq = llvmCodeGen->module()->getFunction("Jitted_int24eq");
                        if (func_int24eq == NULL) {
                            func_int24eq = int2_int4_sop_codegen<SOP_EQ, INT2OID>();
                        }
                        case_cond1 = inner_builder.CreateCall(func_int24eq, {case_arg_value, case_value});
                    } break;
                    case INT42EQOID: {
                        llvm::Function* func_int42eq = llvmCodeGen->module()->getFunction("Jitted_int42eq");
                        if (func_int42eq == NULL) {
                            func_int42eq = int2_int4_sop_codegen<SOP_EQ, INT4OID>();
                        }
                        case_cond1 = inner_builder.CreateCall(func_int42eq, {case_arg_value, case_value});
                    } break;
                    case INT48EQOID: {
                        llvm::Function* func_int48eq = llvmCodeGen->module()->getFunction("Jitted_int48eq");
                        if (func_int48eq == NULL) {
                            func_int48eq = int4_int8_sop_codegen<SOP_EQ, INT4OID>();
                        }
                        case_cond1 = inner_builder.CreateCall(func_int48eq, {case_arg_value, case_value});
                    } break;
                    case INT84EQOID: {
                        llvm::Function* func_int84eq = llvmCodeGen->module()->getFunction("Jitted_int84eq");
                        if (func_int84eq == NULL) {
                            func_int84eq = int4_int8_sop_codegen<SOP_EQ, INT8OID>();
                        }
                        case_cond1 = inner_builder.CreateCall(func_int84eq, {case_arg_value, case_value});
                    } break;
                    case DATEEQOID: {
                        llvm::Function* func_dateeq = llvmCodeGen->module()->getFunction("Jitted_dateeq");
                        if (func_dateeq == NULL) {
                            func_dateeq = date_eq_codegen();
                        }
                        case_cond1 = inner_builder.CreateCall(func_dateeq, {case_arg_value, case_value});
                    } break;
                    case FLOAT8EQOID: {
                        llvm::Function* func_float8eq = llvmCodeGen->module()->getFunction("Jitted_float8eq");
                        if (func_float8eq == NULL) {
                            func_float8eq = float8eq_codegen();
                        }
                        case_cond1 = inner_builder.CreateCall(func_float8eq, {case_arg_value, case_value});
                    } break;
                    case TIMESTAMPEQOID: {
                        llvm::Function* func_timestampeq = llvmCodeGen->module()->getFunction("Jitted_timestampeq");
                        if (func_timestampeq == NULL) {
                            func_timestampeq = timestamp_eq_codegen();
                        }
                        case_cond1 = inner_builder.CreateCall(func_timestampeq, {case_arg_value, case_value});
                    } break;
                    case TEXTEQOID: {
                        llvm::Function* func_texteq = llvmCodeGen->module()->getFunction("Jitted_texteq");
                        if (func_texteq == NULL) {
                            func_texteq = texteq_codegen();
                        }
                        case_cond1 = inner_builder.CreateCall(func_texteq, {case_arg_value, case_value});
                    } break;
                    case BPCHAREQOID: {
                        llvm::Function* func_bpchareq = llvmCodeGen->module()->getFunction("Jitted_bpchareq");
                        if (func_bpchareq == NULL) {
                            func_bpchareq = bpchareq_codegen();
                        }
                        case_cond1 = inner_builder.CreateCall(func_bpchareq, {case_arg_value, case_value});
                    } break;
                    case NUMERICEQOID: {
                        llvm::Function* func_numericeq = llvmCodeGen->module()->getFunction("Jitted_numericeq");
                        if (func_numericeq == NULL) {
                            func_numericeq = numeric_sop_codegen<BIEQ>();
                        }
                        case_cond1 =
                            inner_builder.CreateCall(func_numericeq, {case_arg_value, case_value}, "numericeq");
                    } break;
                    default: {
                        /*
                         * first prepare the parameters used by EvalFuncResult,
                         * then invoke the origial c-function by WrapVecStrictOperFunc,
                         * since eq-func are all strict funcs.
                         */
                        LLVMFuncCallInfo lfcinfo;
                        llvm::Value* inargs[MAX_ARGS_NUM];
                        inargs[0] = case_arg_value;
                        inargs[1] = case_value;
                        lfcinfo.nargs = list_length(arg_expr->args);
                        lfcinfo.args = &inargs[0];
                        case_cond1 = EvalFuncResultCodeGen(&inner_builder, arg_expr, isNull, &lfcinfo);
                    } break;
                }
            }
            case_cond1 = inner_builder.CreateTrunc(case_cond1, int1Type);
            inner_builder.CreateBr(check_bb[clause_index]);
            inner_builder.SetInsertPoint(if_else[clause_index]);
            case_cond2 = int1_0;
            inner_builder.CreateBr(check_bb[clause_index]);
            inner_builder.SetInsertPoint(check_bb[clause_index]);
            llvm::PHINode* Phi_case = inner_builder.CreatePHI(int1Type, 2);
            Phi_case->addIncoming(case_cond1, if_then[clause_index]);
            Phi_case->addIncoming(case_cond2, if_else[clause_index]);
            case_cond = (llvm::Value*)Phi_case;
        }
        /*
         * if we have a true test, then we return the result, since the case
         * statement is satisfied.	A NULL result from the test is not
         * considered true.
         */
        inner_builder.CreateCondBr(case_cond, clause_bb[clause_index], next_bb);
        inner_builder.SetInsertPoint(clause_bb[clause_index]);
        case_result_arg.exprstate = wclause->result;
        clause_result[clause_index] = CodeGen(&case_result_arg);

        if (clause_result[clause_index] == NULL) {
            ret_bb->eraseFromParent();
            return NULL;
        }
        inner_builder.CreateBr(ret_bb);
        clause_index++;
        current_bb = next_bb;
    }

    /* turn to default expr */
    inner_builder.SetInsertPoint(next_bb);
    clause_bb[clause_index] = next_bb;
    if (case_default == NULL) {
        clause_result[clause_index] = Datum_0;
        inner_builder.CreateStore(null_true, isNull);
    } else {
        case_default_arg.exprstate = case_default;
        clause_result[clause_index] = CodeGen(&case_default_arg);
        if (clause_result[clause_index] == NULL)
            return NULL;
    }
    inner_builder.CreateBr(ret_bb);
    inner_builder.SetInsertPoint(ret_bb);
    llvm::PHINode* Phi_ret = inner_builder.CreatePHI(int64Type, clause_index + 1);
    for (i = 0; i < clause_index + 1; i++) {
        Phi_ret->addIncoming(clause_result[i], clause_bb[i]);
    }
    inner_builder.CreateRet(Phi_ret);

    pfree_ext(clause_bb);
    pfree_ext(clause_result);

    llvmCodeGen->FinalizeFunction(jitted_caseexpr);

    result = outer_builder->CreateCall(
        jitted_caseexpr, {args->llvm_args[0], args->llvm_args[1], args->llvm_args[2]}, "VecCase");
    return result;
}

llvm::Value* VecExprCodeGen::AndOrLogicCodeGen(ExprCodeGenArgs* args)
{
    BoolExprState* boolstate = (BoolExprState*)args->exprstate;
    BoolExpr* boolexpr = (BoolExpr*)args->exprstate->expr;
    List* boolargs = boolstate->args;

    /* Start the process of createing IR function */
    dorado::GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    GsCodeGen::LlvmBuilder* outer_builder = args->builder;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder inner_builder(context);

    llvm::Value* isNull = NULL;
    llvm::Value* result = NULL;
    llvm::Value* cvalue = NULL;
    llvm::Value* cflag = NULL;
    llvm::Value* AnyNull = NULL;
    llvm::Value* tmp = NULL;
    llvm::Value* cmp = NULL;
    llvm::Value* llvmargs[3];

    /* Define the datatype and variable that needed */
    DEFINE_CG_TYPE(int1Type, BITOID);
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(ExprContextPtrType, "struct.ExprContext");

    DEFINE_CGVAR_INT8(null_false, 0);
    DEFINE_CGVAR_INT8(null_true, 1);
    DEFINE_CGVAR_INT64(Datum_0, 0);
    DEFINE_CGVAR_INT64(Datum_1, 1);

    /* Define llvm function */
    DEFINE_FUNCTION_WITH_3_ARGS(
        jitted_vbool, "JittedVecBool", "econtext", ExprContextPtrType, "isNull", int8PtrType, "loop_index", int64Type);

    isNull = llvmargs[1];

    /* Define the basic block of if...else...end structure */
    llvm::BasicBlock* entry = &jitted_vbool->getEntryBlock();
    DEFINE_BLOCK(ret_bb, jitted_vbool);
    llvm::BasicBlock* current_bb = NULL;
    llvm::BasicBlock* next_bb = NULL;
    ListCell* cell = NULL;
    int clause_index = 0;

    /* Define ExprCodeGenArgs to calculate each cell expr */
    ExprCodeGenArgs bargs;
    bargs = *args;
    bargs.builder = &inner_builder;
    bargs.llvm_args = &llvmargs[0];
    AnyNull = inner_builder.CreateAlloca(int8Type);
    inner_builder.CreateStore(null_false, AnyNull);

    /* palloc BasicBlock : we should palloc one more for return logic */
    int bool_nargs = list_length(boolargs);
    llvm::BasicBlock** clause_bb = (llvm::BasicBlock**)palloc(sizeof(llvm::BasicBlock*) * (bool_nargs + 1));
    llvm::BasicBlock** if_then = (llvm::BasicBlock**)palloc(sizeof(llvm::BasicBlock*) * (bool_nargs + 1));
    llvm::BasicBlock** if_else = (llvm::BasicBlock**)palloc(sizeof(llvm::BasicBlock*) * (bool_nargs + 1));
    llvm::Value** clause_result = (llvm::Value**)palloc(sizeof(llvm::Value*) * (bool_nargs + 1));

    /* Begin the whole control flow to evaluate the expression */
    current_bb = entry;
    switch (boolexpr->boolop) {
        /*
         * And Expr:
         * If any of the clauses is FALSE, the AND result if FALSE regardless
         * of the states of teh rest of the caluses, so we can stop evaluating
         * and return FALSE immediately.
         */
        case AND_EXPR: {
            foreach (cell, boolargs) {
                ExprState* cexpr = (ExprState*)lfirst(cell);
                clause_bb[clause_index] = llvm::BasicBlock::Create(context, "clause_bb", jitted_vbool);
                if_then[clause_index] = llvm::BasicBlock::Create(context, "if_then", jitted_vbool);
                if_else[clause_index] = llvm::BasicBlock::Create(context, "if_else", jitted_vbool);
                next_bb = llvm::BasicBlock::Create(context, "next_bb", jitted_vbool);

                inner_builder.SetInsertPoint(current_bb);
                bargs.exprstate = cexpr;
                cvalue = CodeGen(&bargs);
                cflag = llvmargs[1];
                cflag = inner_builder.CreateLoad(int8Type, cflag, "cflag");
                tmp = inner_builder.CreateTrunc(cflag, int1Type);
                inner_builder.CreateCondBr(tmp, if_then[clause_index], if_else[clause_index]);

                /* if we have a null rsult, remember it */
                inner_builder.SetInsertPoint(if_then[clause_index]);
                inner_builder.CreateStore(null_true, AnyNull);
                inner_builder.CreateBr(next_bb);

                /* if we have a non-null false result, then return it */
                inner_builder.SetInsertPoint(if_else[clause_index]);
                cmp = inner_builder.CreateICmpNE(cvalue, Datum_0);
                cmp = inner_builder.CreateTrunc(cmp, int1Type);
                inner_builder.CreateCondBr(cmp, next_bb, clause_bb[clause_index]);
                inner_builder.SetInsertPoint(clause_bb[clause_index]);
                clause_result[clause_index] = cvalue;
                inner_builder.CreateStore(null_false, isNull);
                inner_builder.CreateBr(ret_bb);
                clause_index++;
                current_bb = next_bb;
            }

            /*
             * AnyNull is true if at least one cluase evaluated to NULL.
             * And the result is not AnyNull.
             */
            inner_builder.SetInsertPoint(next_bb);
            clause_bb[clause_index] = next_bb;
            AnyNull = inner_builder.CreateLoad(int8Type, AnyNull);
            cmp = inner_builder.CreateTrunc(AnyNull, int1Type);
            clause_result[clause_index] = inner_builder.CreateSelect(cmp, Datum_0, Datum_1);
            inner_builder.CreateStore(AnyNull, isNull);
            inner_builder.CreateBr(ret_bb);
            inner_builder.SetInsertPoint(ret_bb);
            llvm::PHINode* Phi_ret = inner_builder.CreatePHI(int64Type, clause_index + 1);
            for (int i = 0; i < clause_index + 1; i++) {
                Phi_ret->addIncoming(clause_result[i], clause_bb[i]);
            }
            inner_builder.CreateRet(Phi_ret);
        } break;
        case OR_EXPR: {
            /*
             * Or Expr:
             * If any of the clauses is TRUE, the OR result is TRUE regardless of the
             * states of the rest of the clauses, so we can stop evaluating and return
             * TRUE immediately.
             */
            foreach (cell, boolargs) {
                ExprState* cexpr = (ExprState*)lfirst(cell);
                clause_bb[clause_index] = llvm::BasicBlock::Create(context, "clause_bb", jitted_vbool);
                if_then[clause_index] = llvm::BasicBlock::Create(context, "if_then", jitted_vbool);
                if_else[clause_index] = llvm::BasicBlock::Create(context, "if_else", jitted_vbool);
                next_bb = llvm::BasicBlock::Create(context, "next_bb", jitted_vbool);

                /* evaluate current list cell */
                inner_builder.SetInsertPoint(current_bb);
                bargs.exprstate = cexpr;
                cvalue = CodeGen(&bargs);
                cflag = llvmargs[1];
                cflag = inner_builder.CreateLoad(int8Type, cflag, "cflag");
                tmp = inner_builder.CreateTrunc(cflag, int1Type);
                inner_builder.CreateCondBr(tmp, if_then[clause_index], if_else[clause_index]);

                /* remember we got a null here */
                inner_builder.SetInsertPoint(if_then[clause_index]);
                inner_builder.CreateStore(null_true, AnyNull);
                inner_builder.CreateBr(next_bb);

                /* if we have a non-null true result, then return it */
                inner_builder.SetInsertPoint(if_else[clause_index]);
                cmp = inner_builder.CreateICmpNE(cvalue, Datum_0);
                cmp = inner_builder.CreateTrunc(cmp, int1Type);
                inner_builder.CreateCondBr(cmp, clause_bb[clause_index], next_bb);
                inner_builder.SetInsertPoint(clause_bb[clause_index]);
                clause_result[clause_index] = cvalue;
                inner_builder.CreateStore(null_false, isNull);
                inner_builder.CreateBr(ret_bb);
                clause_index++;
                current_bb = next_bb;
            }

            /* If None of the clauses is satisfied, return false */
            inner_builder.SetInsertPoint(next_bb);
            clause_bb[clause_index] = next_bb;
            clause_result[clause_index] = Datum_0;
            AnyNull = inner_builder.CreateLoad(int8Type, AnyNull);
            inner_builder.CreateStore(AnyNull, isNull);
            inner_builder.CreateBr(ret_bb);
            inner_builder.SetInsertPoint(ret_bb);
            llvm::PHINode* Phi_ret = inner_builder.CreatePHI(int64Type, clause_index + 1);
            for (int i = 0; i < clause_index + 1; i++) {
                Phi_ret->addIncoming(clause_result[i], clause_bb[i]);
            }
            inner_builder.CreateRet(Phi_ret);
        } break;
        default:
            Assert(false);
            break;
    }

    pfree_ext(clause_bb);
    pfree_ext(clause_result);
    pfree_ext(if_then);
    pfree_ext(if_else);

    llvmCodeGen->FinalizeFunction(jitted_vbool);
    result = outer_builder->CreateCall(jitted_vbool, {args->llvm_args[0], args->llvm_args[1], args->llvm_args[2]});

    return result;
}

llvm::Value* VecExprCodeGen::NotCodeGen(ExprCodeGenArgs* args)
{
    /* Get the argument expr */
    BoolExprState* notclause = (BoolExprState*)args->exprstate;
    ExprState* notexpr = (ExprState*)linitial(notclause->args);

    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder* outer_builder = args->builder;
    GsCodeGen::LlvmBuilder inner_builder(context);

    /* Define the datatype and variable that needed */
    DEFINE_CG_TYPE(int1Type, BITOID);
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(ExprContextPtrType, "struct.ExprContext");

    DEFINE_CGVAR_INT8(null_false, 0);
    DEFINE_CGVAR_INT8(null_true, 1);
    DEFINE_CGVAR_INT64(Datum_0, 0);

    llvm::Value* isNull = NULL;
    llvm::Value* expr_value = NULL;
    llvm::Value* vflag = NULL;
    llvm::Value* cmp = NULL;
    llvm::Value* res1 = NULL;
    llvm::Value* res2 = NULL;
    llvm::Value* result = NULL;
    llvm::Value* llvmargs[3];

    /* Define llvm function */
    DEFINE_FUNCTION_WITH_3_ARGS(
        jitted_vnot, "JittedVecNot", "econtext", ExprContextPtrType, "isNull", int8PtrType, "loop_index", int64Type);

    isNull = llvmargs[1];

    /* Define ExprCodeGenArgs to calculate not expr argument value */
    ExprCodeGenArgs nargs;
    nargs = *args;
    nargs.builder = &inner_builder;
    nargs.llvm_args = &llvmargs[0];
    nargs.exprstate = notexpr;
    expr_value = CodeGen(&nargs);
    vflag = llvmargs[1];

    /* Define the basic block of if...else...end structure */
    llvm::BasicBlock* entry = &jitted_vnot->getEntryBlock();
    DEFINE_BLOCK(be_null, jitted_vnot);
    DEFINE_BLOCK(be_not_null, jitted_vnot);
    DEFINE_BLOCK(ret_bb, jitted_vnot);

    inner_builder.SetInsertPoint(entry);
    vflag = inner_builder.CreateLoad(int8Type, vflag, "not_flag");
    vflag = inner_builder.CreateTrunc(vflag, int1Type);
    inner_builder.CreateCondBr(vflag, be_null, be_not_null);

    /*
     * If the expression evaluates to null, just cascade the null back to
     * whoever called us.
     */
    inner_builder.SetInsertPoint(be_null);
    res1 = expr_value;
    inner_builder.CreateStore(null_true, isNull);
    inner_builder.CreateBr(ret_bb);

    /*
     * Evaluation of 'not' is simple..expr is false, then return 'true'
     * and vice versa.
     */
    inner_builder.SetInsertPoint(be_not_null);
    cmp = inner_builder.CreateICmpNE(expr_value, Datum_0);
    res2 = inner_builder.CreateNot(cmp);
    res2 = inner_builder.CreateZExt(res2, int64Type);
    inner_builder.CreateStore(null_false, isNull);
    inner_builder.CreateBr(ret_bb);

    /* Get the final result */
    inner_builder.SetInsertPoint(ret_bb);
    llvm::PHINode* Phi_ret = inner_builder.CreatePHI(int64Type, 2);
    Phi_ret->addIncoming(res1, be_null);
    Phi_ret->addIncoming(res2, be_not_null);
    inner_builder.CreateRet(Phi_ret);

    llvmCodeGen->FinalizeFunction(jitted_vnot);
    result = outer_builder->CreateCall(jitted_vnot, {args->llvm_args[0], args->llvm_args[1], args->llvm_args[2]});

    return result;
}

llvm::Value* VecExprCodeGen::NullIfCodeGen(ExprCodeGenArgs* args)
{
    FuncExprState* fcache = (FuncExprState*)args->exprstate;
    NullIfExpr* nullifexpr = (NullIfExpr*)(args->exprstate->expr);
    List* nullif_args = fcache->args;
    ExprState* lestate = NULL;
    ExprState* restate = NULL;

    /* Only support the case with 2 arguments */
    if (list_length(nullif_args) != 2)
        return NULL;

    /* Start the process of createing IR function */
    dorado::GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    GsCodeGen::LlvmBuilder* outer_builder = args->builder;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder inner_builder(context);

    llvm::Value* isNull = NULL;

    llvm::Value* result = NULL;
    llvm::Value* inargs[MAX_ARGS_NUM];
    llvm::Value* inargnulls[MAX_ARGS_NUM];
    llvm::Value* res1 = NULL;
    llvm::Value* ret1 = NULL;
    llvm::Value* ret2 = NULL;
    llvm::Value* cmp = NULL;
    llvm::Value* llvmargs[3];

    /* Define the datatype and variable that needed */
    DEFINE_CG_TYPE(int1Type, BITOID);
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(ExprContextPtrType, "struct.ExprContext");
    DEFINE_CGVAR_INT8(null_false, 0);
    DEFINE_CGVAR_INT8(null_true, 1);
    DEFINE_CGVAR_INT64(Datum_0, 0);
    DEFINE_CGVAR_INT64(Datum_1, 1);

    /* Define llvm function */
    DEFINE_FUNCTION_WITH_3_ARGS(jitted_vnullif,
        "JittedVecNullif",
        "econtext",
        ExprContextPtrType,
        "isNull",
        int8PtrType,
        "loop_index",
        int64Type);

    isNull = llvmargs[1];

    /* Define the basic block of if...else...end structure */
    llvm::BasicBlock* entry = &jitted_vnullif->getEntryBlock();
    DEFINE_BLOCK(bnot_null, jitted_vnullif);
    DEFINE_BLOCK(be_null, jitted_vnullif);
    DEFINE_BLOCK(ret_bb, jitted_vnullif);
    DEFINE_BLOCK(if_then, jitted_vnullif);
    DEFINE_BLOCK(if_else, jitted_vnullif);

    lestate = (ExprState*)linitial(nullif_args);
    restate = (ExprState*)lsecond(nullif_args);
    inner_builder.SetInsertPoint(entry);

    /* Prepare the left operand value */
    ExprCodeGenArgs largs;
    largs = *args;
    largs.exprstate = lestate;
    largs.builder = &inner_builder;
    largs.llvm_args = &llvmargs[0];
    inargs[0] = CodeGen(&largs);
    inargnulls[0] = llvmargs[1];
    inargnulls[0] = inner_builder.CreateLoad(int8Type, inargnulls[0], "lflag");

    /* Prepare the right operand value */
    ExprCodeGenArgs rargs;
    rargs = *args;
    rargs.exprstate = restate;
    rargs.builder = &inner_builder;
    rargs.llvm_args = &llvmargs[0];
    inargs[1] = CodeGen(&rargs);
    inargnulls[1] = llvmargs[1];
    inargnulls[1] = inner_builder.CreateLoad(int8Type, inargnulls[1], "rflag");

    cmp = inner_builder.CreateOr(inargnulls[0], inargnulls[1]);
    cmp = inner_builder.CreateTrunc(cmp, int1Type);
    inner_builder.CreateCondBr(cmp, be_null, bnot_null);
    inner_builder.SetInsertPoint(bnot_null);
    inner_builder.CreateStore(null_false, isNull);

    /*
     * For NullIfExpr, there is no need to consider strict attribute.
     * Only call the IR function when both the operands are not NULL.
     * Once we have generated the IR function, we will not generate
     * it again at the next time.
     */
    switch (nullifexpr->opno) {
        case INT2EQOID: {
            llvm::Function* func_int2eq = llvmCodeGen->module()->getFunction("Jitted_int2eq");
            if (func_int2eq == NULL) {
                func_int2eq = int2_sop_codegen<SOP_EQ>();
            }
            res1 = inner_builder.CreateCall(func_int2eq, {inargs[0], inargs[1]}, "int2_eq");
        } break;
        case INT24EQOID: {
            llvm::Function* func_int24eq = llvmCodeGen->module()->getFunction("Jitted_int24eq");
            if (func_int24eq == NULL) {
                func_int24eq = int2_int4_sop_codegen<SOP_EQ, INT2OID>();
            }
            res1 = inner_builder.CreateCall(func_int24eq, {inargs[0], inargs[1]}, "int24_eq");
        } break;
        case INT42EQOID: {
            llvm::Function* func_int42eq = llvmCodeGen->module()->getFunction("Jitted_int42eq");
            if (func_int42eq == NULL) {
                func_int42eq = int2_int4_sop_codegen<SOP_EQ, INT4OID>();
            }
            res1 = inner_builder.CreateCall(func_int42eq, {inargs[0], inargs[1]}, "int42_eq");
        } break;
        case INT4EQOID: {
            llvm::Function* func_int4eq = llvmCodeGen->module()->getFunction("Jitted_int4eq");
            if (func_int4eq == NULL) {
                func_int4eq = int4_sop_codegen<SOP_EQ>();
            }
            res1 = inner_builder.CreateCall(func_int4eq, {inargs[0], inargs[1]}, "int4_eq");
        } break;
        case INT48EQOID: {
            llvm::Function* func_int48eq = llvmCodeGen->module()->getFunction("Jitted_int48eq");
            if (func_int48eq == NULL) {
                func_int48eq = int4_int8_sop_codegen<SOP_EQ, INT4OID>();
            }
            res1 = inner_builder.CreateCall(func_int48eq, {inargs[0], inargs[1]}, "int48_eq");
        } break;
        case INT84EQOID: {
            llvm::Function* func_int84eq = llvmCodeGen->module()->getFunction("Jitted_int84eq");
            if (func_int84eq == NULL) {
                func_int84eq = int4_int8_sop_codegen<SOP_EQ, INT8OID>();
            }
            res1 = inner_builder.CreateCall(func_int84eq, {inargs[0], inargs[1]}, "int84_eq");
        } break;
        case INT8EQOID: {
            llvm::Function* func_int8eq = llvmCodeGen->module()->getFunction("Jitted_int8eq");
            if (func_int8eq == NULL) {
                func_int8eq = int8_sop_codegen<SOP_EQ>();
            }
            res1 = inner_builder.CreateCall(func_int8eq, {inargs[0], inargs[1]}, "int8_eq");
        } break;
        case FLOAT8EQOID: {
            llvm::Function* func_float8eq = llvmCodeGen->module()->getFunction("Jitted_float8eq");
            if (func_float8eq == NULL) {
                func_float8eq = float8eq_codegen();
            }
            res1 = inner_builder.CreateCall(func_float8eq, {inargs[0], inargs[1]}, "float8_eq");
        } break;
        case BooleanEqualOperator: {
            llvm::Function* func_booleq = llvmCodeGen->module()->getFunction("Jitted_booleq");
            if (func_booleq == NULL) {
                func_booleq = booleq_codegen();
            }
            res1 = inner_builder.CreateCall(func_booleq, {inargs[0], inargs[1]}, "booleq");
        } break;
        case DATEEQOID: {
            llvm::Function* func_dateeq = llvmCodeGen->module()->getFunction("Jitted_dateeq");
            if (func_dateeq == NULL) {
                func_dateeq = date_eq_codegen();
            }
            res1 = inner_builder.CreateCall(func_dateeq, {inargs[0], inargs[1]}, "date_eq");
        } break;
        case TIMESTAMPEQOID: {
            llvm::Function* func_timestampeq = llvmCodeGen->module()->getFunction("Jitted_timestampeq");
            if (func_timestampeq == NULL) {
                func_timestampeq = timestamp_eq_codegen();
            }
            res1 = inner_builder.CreateCall(func_timestampeq, {inargs[0], inargs[1]}, "timestamp_eq");
        } break;
        case TEXTEQOID: {
            llvm::Function* func_texteq = llvmCodeGen->module()->getFunction("Jitted_texteq");
            if (func_texteq == NULL) {
                func_texteq = texteq_codegen();
            }
            res1 = inner_builder.CreateCall(func_texteq, {inargs[0], inargs[1]}, "texteq");
        } break;
        case BPCHAREQOID: {
            llvm::Function* func_bpchareq = llvmCodeGen->module()->getFunction("Jitted_bpchareq");
            if (func_bpchareq == NULL) {
                func_bpchareq = bpchareq_codegen();
            }
            res1 = inner_builder.CreateCall(func_bpchareq, {inargs[0], inargs[1]}, "bpchareq");
        } break;
        case NUMERICEQOID: {
            llvm::Function* func_numericeq = llvmCodeGen->module()->getFunction("Jitted_numericeq");
            if (func_numericeq == NULL) {
                func_numericeq = numeric_sop_codegen<BIEQ>();
            }
            res1 = inner_builder.CreateCall(func_numericeq, {inargs[0], inargs[1]}, "numericeq");
        } break;
        default: {
            /*
             * first prepare the parameters used by EvalFuncResult,
             * then invoke the origial c-function by WrapVecStrictOperFunc.
             */
            LLVMFuncCallInfo lfcinfo;
            lfcinfo.nargs = list_length(fcache->args);
            lfcinfo.args = &inargs[0];
            lfcinfo.argnulls = &inargnulls[0];
            res1 = EvalFuncResultCodeGen(&inner_builder, fcache, isNull, &lfcinfo);
        } break;
    }

    /* Check the result and flag of expr. */
    cmp = inner_builder.CreateICmpEQ(res1, Datum_1);
    inner_builder.CreateCondBr(cmp, if_then, if_else);

    /* when expr in nullif is equal. */
    inner_builder.SetInsertPoint(if_then);
    inner_builder.CreateStore(null_true, isNull);
    ret1 = Datum_0;
    inner_builder.CreateBr(ret_bb);

    /* when expr in nullif is not equal. */
    inner_builder.SetInsertPoint(if_else);
    inner_builder.CreateBr(be_null);

    inner_builder.SetInsertPoint(be_null);
    inner_builder.CreateStore(inargnulls[0], isNull);
    ret2 = inargs[0];
    inner_builder.CreateBr(ret_bb);

    /* Achieve the return block. */
    inner_builder.SetInsertPoint(ret_bb);
    llvm::PHINode* Phi_ret = inner_builder.CreatePHI(int64Type, 3);
    Phi_ret->addIncoming(ret1, if_then);
    Phi_ret->addIncoming(ret2, be_null);
    inner_builder.CreateRet(Phi_ret);

    llvmCodeGen->FinalizeFunction(jitted_vnullif);

    result = outer_builder->CreateCall(
        jitted_vnullif, {args->llvm_args[0], args->llvm_args[1], args->llvm_args[2]}, "EvalVecNullif");
    return result;
}

llvm::Value* VecExprCodeGen::RelabelCodeGen(ExprCodeGenArgs* args)
{
    GenericExprState* gstate = (GenericExprState*)args->exprstate;
    ExprState* ert = gstate->arg;

    llvm::Value* result = NULL;

    ExprCodeGenArgs rtargs;
    rtargs = *args;
    rtargs.exprstate = ert;
    result = CodeGen(&rtargs);

    return result;
}

llvm::Value* VecExprCodeGen::FuncCodeGen(ExprCodeGenArgs* args)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder* outer_builder = args->builder;
    GsCodeGen::LlvmBuilder inner_builder(context);

    /* Define the datatype and variable that needed */
    DEFINE_CG_TYPE(int1Type, BITOID);
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(ExprContextPtrType, "struct.ExprContext");

    DEFINE_CGVAR_INT8(null_false, 0);
    DEFINE_CGVAR_INT8(null_true, 1);
    DEFINE_CGVAR_INT64(Datum_0, 0);

    llvm::Value* llvmargs[3];
    llvm::Value* result = NULL;
    llvm::Value* inargs[MAX_ARGS_NUM];
    llvm::Value* inargnulls[MAX_ARGS_NUM];
    llvm::Value* res1 = NULL;
    llvm::Value* res2 = NULL;

    /* Define llvm function */
    DEFINE_FUNCTION_WITH_3_ARGS(
        jitted_vfunc, "JittedVecFunc", "econtext", ExprContextPtrType, "isNull", int8PtrType, "loop_index", int64Type);

    llvm::Value* isNull = llvmargs[1];

    /* Define the basic block of if...else...end structure */
    llvm::BasicBlock* entry = &jitted_vfunc->getEntryBlock();

    /*
     * Different FuncExprs have different number of arguments.
     * Get the argument value according to listlength(FuncExpr->args)
     * and all the cases should not be confused.
     */
    FuncExprState* fcache = (FuncExprState*)args->exprstate;
    FuncExpr* funcexpr = (FuncExpr*)(args->exprstate->expr);
    List* func_args = fcache->args;
    int num_args;
    ExprState* func_arg1 = NULL;
    ExprState* func_arg2 = NULL;
    ExprState* func_arg3 = NULL;
    Oid funcid = funcexpr->funcid;

    const FmgrBuiltin* fbp = NULL;
    bool func_is_strict = false;
    /*
     * First check if the current func is strict or not.
     * Fast path for builtin functions: don't bother consulting pg_proc
     */
    if ((fbp = fmgr_isbuiltin(funcid)) != NULL) {
        func_is_strict = fbp->strict;
    } else {
        /* Otherwise we need the pg_proc entry */
        HeapTuple procedureTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));

        if (!HeapTupleIsValid(procedureTuple))
            ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmodule(MOD_LLVM),
                    errmsg("cache lookup failed for function %u", funcid)));

        Form_pg_proc procedureStruct = (Form_pg_proc)GETSTRUCT(procedureTuple);
        func_is_strict = procedureStruct->proisstrict;

        /* Release heap tuple */
        ReleaseSysCache(procedureTuple);
    }

    /* prepare args with respect to num_args */
    inner_builder.SetInsertPoint(entry);
    num_args = funcexpr->args->length;

    /*
     * Evaluate all the arguments of the FuncExpr and consider the combination
     * of the null flag.
     */
    llvm::Value* cmpor = NULL;
    if (num_args > 0) {
        func_arg1 = (ExprState*)linitial(func_args);
        ExprCodeGenArgs largs = *args;
        largs.exprstate = func_arg1;
        largs.builder = &inner_builder;
        largs.llvm_args = &llvmargs[0];
        inargs[0] = CodeGen(&largs);
        inargnulls[0] = llvmargs[1];
        inargnulls[0] = inner_builder.CreateLoad(int8Type, inargnulls[0], "lflag");
        cmpor = inargnulls[0];
        if (!inargs[0]) {
            ereport(ERROR,
                (errcode(ERRCODE_CODEGEN_ERROR),
                    errmodule(MOD_LLVM),
                    errmsg("Codegen failed on the first argument of FuncExpr!\n")));
        }
    }

    if (num_args > 1) {
        func_arg2 = (ExprState*)lsecond(func_args);
        ExprCodeGenArgs rargs = *args;
        rargs.exprstate = func_arg2;
        rargs.builder = &inner_builder;
        rargs.llvm_args = &llvmargs[0];
        inargs[1] = CodeGen(&rargs);
        inargnulls[1] = llvmargs[1];
        inargnulls[1] = inner_builder.CreateLoad(int8Type, inargnulls[1], "rflag");
        cmpor = inner_builder.CreateOr(cmpor, inargnulls[1]);
        if (!inargs[1]) {
            ereport(ERROR,
                (errcode(ERRCODE_CODEGEN_ERROR),
                    errmodule(MOD_LLVM),
                    errmsg("Codegen failed on the second argument of FuncExpr!\n")));
        }
    }

    if (num_args > 2) {
        func_arg3 = (ExprState*)lthird(func_args);
        ExprCodeGenArgs targs = *args;
        targs.exprstate = func_arg3;
        targs.builder = &inner_builder;
        targs.llvm_args = &llvmargs[0];
        inargs[2] = CodeGen(&targs);
        inargnulls[2] = llvmargs[1];
        inargnulls[2] = inner_builder.CreateLoad(int8Type, inargnulls[2], "tflag");
        cmpor = inner_builder.CreateOr(cmpor, inargnulls[2]);
        if (!inargs[2]) {
            ereport(ERROR,
                (errcode(ERRCODE_CODEGEN_ERROR),
                    errmodule(MOD_LLVM),
                    errmsg("Codegen failed on the third argument of FuncExpr!\n")));
        }
    }

    if (func_is_strict) {
        DEFINE_BLOCK(bnot_null, jitted_vfunc);
        DEFINE_BLOCK(be_null, jitted_vfunc);
        DEFINE_BLOCK(ret_bb, jitted_vfunc);

        cmpor = inner_builder.CreateTrunc(cmpor, int1Type);
        inner_builder.CreateCondBr(cmpor, be_null, bnot_null);
        inner_builder.SetInsertPoint(bnot_null);

        switch (funcid) {
            /* Get the actual result */
            case SUBSTRFUNCOID: {
                /* Only consider it when none of its arguments are NULL, else return NULL */
                inargs[1] = inner_builder.CreateTrunc(inargs[1], int32Type);
                inargs[2] = inner_builder.CreateTrunc(inargs[2], int32Type);

                llvm::Function* func_substr = llvmCodeGen->module()->getFunction("Jittedsubstr");
                if (func_substr == NULL) {
                    func_substr = substr_codegen();
                }

                /*
                 * here we should consider the ORC compatiblity setting,
                 * if sql_compatiblity = ORC_FORMAT, and length of result is 0 (indicates by the 'isNull' flag),
                 * then we have to set 'isNull' flag to True,
                 * and then jump to the be_null block.
                 */
                if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) {
                    res1 = inner_builder.CreateCall(func_substr, {inargs[0], inargs[1], inargs[2], isNull});
                    llvm::Value* res_flag = inner_builder.CreateLoad(int8Type, isNull, "resflag");
                    llvm::Value* cmp = inner_builder.CreateICmpEQ(res_flag, null_true, "check");
                    inner_builder.CreateCondBr(cmp, be_null, ret_bb);
                } else {
                    res1 = inner_builder.CreateCall(func_substr, {inargs[0], inargs[1], inargs[2]});
                    inner_builder.CreateStore(null_false, isNull);
                    inner_builder.CreateBr(ret_bb);
                }
            } break;
            case RTRIM1FUNCOID:
            case RTRIMPARAFUNCOID: {
                llvm::Function* func_rtrim1 = llvmCodeGen->module()->getFunction("Jittedrtrim1");
                if (func_rtrim1 == NULL) {
                    func_rtrim1 = rtrim1_codegen();
                }
                /*
                 * here we should consider the ORC compatiblity setting,
                 * if sql_compatiblity = ORC_FORMAT, and length of result is 0 (indicates by the 'isNull' flag),
                 * then we have to set 'isNull' flag to True,
                 * and then jump to the be_null block.
                 */
                if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) {
                    res1 = inner_builder.CreateCall(func_rtrim1, {inargs[0], isNull});
                    llvm::Value* res_flag = inner_builder.CreateLoad(int8Type, isNull, "resflag");
                    llvm::Value* cmp = inner_builder.CreateICmpEQ(res_flag, null_true, "check");
                    inner_builder.CreateCondBr(cmp, be_null, ret_bb);
                } else {
                    res1 = inner_builder.CreateCall(func_rtrim1, inargs[0]);
                    inner_builder.CreateStore(null_false, isNull);
                    inner_builder.CreateBr(ret_bb);
                }
            } break;
            case BTRIMFUNCOID: {
                llvm::Function* func_btrim1 = llvmCodeGen->module()->getFunction("Jittedbtrim1");
                if (func_btrim1 == NULL) {
                    func_btrim1 = btrim1_codegen();
                }
                /*
                 * here we should consider the ORC compatiblity setting,
                 * if sql_compatiblity = ORC_FORMAT, and length of result is 0 (indicates by the 'isNull' flag),
                 * then we have to set 'isNull' flag to True,
                 * and then jump to the be_null block.
                 */
                if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) {
                    res1 = inner_builder.CreateCall(func_btrim1, {inargs[0], isNull});
                    llvm::Value* res_flag = inner_builder.CreateLoad(int8Type, isNull, "resflag");
                    llvm::Value* cmp = inner_builder.CreateICmpEQ(res_flag, null_true, "check");
                    inner_builder.CreateCondBr(cmp, be_null, ret_bb);
                } else {
                    res1 = inner_builder.CreateCall(func_btrim1, inargs[0]);
                    inner_builder.CreateStore(null_false, isNull);
                    inner_builder.CreateBr(ret_bb);
                }
            } break;
            case BPLENFUNCOID: {
                int current_encoding = GetDatabaseEncoding();
                llvm::Function* func_bpcahrlen = llvmCodeGen->module()->getFunction("Jitted_bpcharlen");
                if (func_bpcahrlen == NULL) {
                    func_bpcahrlen = bpcharlen_codegen(current_encoding);
                }

                res1 = inner_builder.CreateCall(func_bpcahrlen, inargs[0]);
                inner_builder.CreateStore(null_false, isNull);
                inner_builder.CreateBr(ret_bb);
            } break;
            default: {
                /* prepare the parameters used by EvalFuncResult */
                LLVMFuncCallInfo lfcinfo;
                lfcinfo.nargs = list_length(funcexpr->args);
                lfcinfo.args = &inargs[0];
                res1 = EvalFuncResultCodeGen(&inner_builder, fcache, isNull, &lfcinfo);
                inner_builder.CreateBr(ret_bb);
            } break;
        }

        /* If one of the iput argument is NULL, return NULL */
        inner_builder.SetInsertPoint(be_null);
        res2 = Datum_0;
        inner_builder.CreateStore(null_true, isNull);
        inner_builder.CreateBr(ret_bb);

        inner_builder.SetInsertPoint(ret_bb);
        llvm::PHINode* Phi_ret = inner_builder.CreatePHI(int64Type, 2);
        Phi_ret->addIncoming(res1, bnot_null);
        Phi_ret->addIncoming(res2, be_null);
        inner_builder.CreateRet(Phi_ret);
    } else {
        /* prepare the parameters used by EvalFuncResult */
        LLVMFuncCallInfo lfcinfo;
        lfcinfo.nargs = num_args;
        lfcinfo.args = &inargs[0];
        lfcinfo.argnulls = &inargnulls[0];
        llvm::Value* res = EvalFuncResultCodeGen(&inner_builder, fcache, isNull, &lfcinfo);
        inner_builder.CreateRet(res);
    }

    llvmCodeGen->FinalizeFunction(jitted_vfunc);

    result = outer_builder->CreateCall(
        jitted_vfunc, {args->llvm_args[0], args->llvm_args[1], args->llvm_args[2]}, "EvalVecFunc");
    return result;
}

llvm::Value* VecExprCodeGen::OpCodeGen(ExprCodeGenArgs* args)
{
    FuncExprState* fcache = (FuncExprState*)args->exprstate;
    OpExpr* opexpr = (OpExpr*)(args->exprstate->expr);
    List* op_args = fcache->args;
    ExprState* lestate = NULL;
    ExprState* restate = NULL;
    int num_args = list_length(op_args);
    /* Only support the case with 2 arguments */
    if (num_args != 2)
        return NULL;

    /* Start the process of createing IR function */
    dorado::GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    GsCodeGen::LlvmBuilder* outer_builder = args->builder;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder inner_builder(context);

    llvm::Value* isNull = NULL;
    llvm::Value* inargs[MAX_ARGS_NUM];
    llvm::Value* inargnulls[MAX_ARGS_NUM];
    llvm::Value* result = NULL;
    llvm::Value* res1 = NULL;
    llvm::Value* res2 = NULL;
    llvm::Value* cmpor = NULL;
    llvm::Value* llvmargs[3];

    /* Define the datatype and variable that needed */
    DEFINE_CG_TYPE(int1Type, BITOID);
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(ExprContextPtrType, "struct.ExprContext");
    DEFINE_CGVAR_INT8(null_false, 0);
    DEFINE_CGVAR_INT8(null_true, 1);
    DEFINE_CGVAR_INT64(Datum_0, 0);
    DEFINE_CGVAR_INT64(Datum_1, 1);

    /* Define llvm function */
    DEFINE_FUNCTION_WITH_3_ARGS(
        jitted_vop, "JittedVecOper", "econtext", ExprContextPtrType, "isNull", int8PtrType, "loop_index", int64Type);

    isNull = llvmargs[1];

    /* Define the basic block of if...else...end structure */
    llvm::BasicBlock* entry = &jitted_vop->getEntryBlock();

    lestate = (ExprState*)linitial(op_args);
    restate = (ExprState*)lsecond(op_args);
    inner_builder.SetInsertPoint(entry);

    /* Prepare the first operand value */
    ExprCodeGenArgs largs;
    largs = *args;
    largs.exprstate = lestate;
    largs.builder = &inner_builder;
    largs.llvm_args = &llvmargs[0];
    inargs[0] = CodeGen(&largs);
    inargnulls[0] = llvmargs[1];
    inargnulls[0] = inner_builder.CreateLoad(int8Type, inargnulls[0], "lflag");

    /* Prepare the second operand value */
    ExprCodeGenArgs rargs;
    rargs = *args;
    rargs.exprstate = restate;
    rargs.builder = &inner_builder;
    rargs.llvm_args = &llvmargs[0];
    inargs[1] = CodeGen(&rargs);
    inargnulls[1] = llvmargs[1];
    inargnulls[1] = inner_builder.CreateLoad(int8Type, inargnulls[1], "rflag");
    if (inargs[0] == NULL || inargs[1] == NULL)
        return NULL;

    const FmgrBuiltin* fbp = NULL;
    bool func_is_strict = false;

    /*
     * Fast path for builtin functions: don't bother consulting pg_proc
     */
    if ((fbp = fmgr_isbuiltin(opexpr->opfuncid)) != NULL) {
        func_is_strict = fbp->strict;
    } else {
        /* Otherwise we need the pg_proc entry */
        HeapTuple procedureTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(opexpr->opfuncid));

        if (!HeapTupleIsValid(procedureTuple))
            ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmodule(MOD_LLVM),
                    errmsg("cache lookup failed for function %u", opexpr->opfuncid)));

        Form_pg_proc procedureStruct = (Form_pg_proc)GETSTRUCT(procedureTuple);
        func_is_strict = procedureStruct->proisstrict;

        /* Release heap tuple */
        ReleaseSysCache(procedureTuple);
    }

    /*
     * If the function is strict, we should not continue the evalution if one
     * of the parameter is NULL. While for non-strict functions, do evalation
     * directly without consider the null flag.
     */
    if (func_is_strict) {

        DEFINE_BLOCK(bnot_null, jitted_vop);
        DEFINE_BLOCK(be_null, jitted_vop);
        DEFINE_BLOCK(ret_bb, jitted_vop);

        cmpor = inner_builder.CreateOr(inargnulls[0], inargnulls[1]);
        cmpor = inner_builder.CreateTrunc(cmpor, int1Type);
        inner_builder.CreateCondBr(cmpor, be_null, bnot_null);
        inner_builder.SetInsertPoint(bnot_null);

        /*
         * Only call the IR function when both the operands are not NULL.
         * Once we have generated the IR function, we will not generate
         * it again at the next time. The following functions we realized
         * are all strict.
         */
        switch (opexpr->opno) {
            case OID_TEXT_LIKE_OP: {
                llvm::Function* func_textlike = llvmCodeGen->module()->getFunction("Jitted_textlike");
                if (func_textlike == NULL) {
                    func_textlike = textlike_codegen();
                }
                res1 = inner_builder.CreateCall(func_textlike, {inargs[0], inargs[1]}, "textlike");
            } break;
            case TEXTNOTLIKEOID: {
                llvm::Function* func_textnlike = llvmCodeGen->module()->getFunction("Jitted_textnotlike");
                if (func_textnlike == NULL) {
                    func_textnlike = textnlike_codegen();
                }
                res1 = inner_builder.CreateCall(func_textnlike, {inargs[0], inargs[1]}, "textnotlike");
            } break;
            case TEXTEQOID: {
                llvm::Function* func_texteq = llvmCodeGen->module()->getFunction("Jitted_texteq");
                if (func_texteq == NULL) {
                    func_texteq = texteq_codegen();
                }
                res1 = inner_builder.CreateCall(func_texteq, {inargs[0], inargs[1]}, "texteq");
            } break;
            case TEXTLTOID: {
                DEFINE_CGVAR_INT32(collid, (long long)opexpr->inputcollid);
                llvm::Function* func_textlt = llvmCodeGen->module()->getFunction("Jitted_textlt");
                if (func_textlt == NULL) {
                    func_textlt = textlt_codegen();
                }
                res1 = inner_builder.CreateCall(func_textlt, {inargs[0], inargs[1], collid}, "textlt");
            } break;
            case TEXTGTOID: {
                DEFINE_CGVAR_INT32(collid, (long long)opexpr->inputcollid);
                llvm::Function* func_textgt = llvmCodeGen->module()->getFunction("Jitted_textgt");
                if (func_textgt == NULL) {
                    func_textgt = textgt_codegen();
                }
                res1 = inner_builder.CreateCall(func_textgt, {inargs[0], inargs[1], collid}, "textgt");
            } break;
            case BPCHAREQOID:
            case BPCHARNEOID: {
                /* deep optimization : check if the case is 'var = const' or
                 * 'const = var'. We could generate a fast path when the var
                 * length equals to the length of the const bpchar data.
                 */
                Expr* lexpr = lestate->expr;
                Expr* rexpr = restate->expr;

                bool fast_path = false;
                Var* var = NULL;
                Const* cst = NULL;
                llvm::Value* const_data = NULL;
                llvm::Value* len_val = NULL;
                llvm::Value* var_data = NULL;

                if (IsA(lexpr, Var) && IsA(rexpr, Const)) {
                    /* simple case : var == 'CHAR' */
                    var = (Var*)lexpr;
                    cst = (Const*)rexpr;
                    var_data = inargs[0];
                } else if (IsA(rexpr, Var) && IsA(lexpr, Const)) {
                    /* simple case : 'CHAR' == var */
                    var = (Var*)rexpr;
                    cst = (Const*)lexpr;
                    var_data = inargs[1];
                }

                if (var != NULL && cst != NULL) {
                    int var_len = var->vartypmod - VARHDRSZ;
                    Datum const_val = cst->constvalue;
                    /* Get the defined bpchar length of the column */
                    BpChar* const_bpchar = DatumGetBpCharPP(const_val);
                    /* Get the length of the const bpchar */
                    int const_len = VARSIZE_ANY_EXHDR(const_bpchar);

                    if (var_len > 0 && var_len == const_len) {
                        llvm::Function* func_varlena = llvmCodeGen->module()->getFunction("JittedEvalVarlena");
                        if (func_varlena == NULL) {
                            func_varlena = VarlenaCvtCodeGen();
                        }
                        var_data = inner_builder.CreateCall(func_varlena, var_data, "var_val");

                        /* Get the bpchar data of the var */
                        var_data = inner_builder.CreateExtractValue(var_data, 1);
                        /* Get the bpchar data of the const */
                        char* const_char = VARDATA_ANY(const_bpchar);
                        const_data = llvmCodeGen->CastPtrToLlvmPtr(int8PtrType, (void*)const_char);
                        /* Get the LLVM value of the const bpchar data */
                        len_val = llvmCodeGen->getIntConstant(INT4OID, var_len);
                        fast_path = true;
                    }
                }

                if (opexpr->opno == BPCHAREQOID) {
                    if (fast_path) {
                        /* call LLVMIRmemcmp since both the var and const have
                         * the same length
                         */
                        llvm::Function* func_memcmp = llvmCodeGen->module()->getFunction("LLVMIRmemcmp");
                        if (func_memcmp == NULL) {
                            ereport(ERROR,
                                (errcode(ERRCODE_LOAD_IR_FUNCTION_FAILED),
                                    errmodule(MOD_LLVM),
                                    errmsg("Failed on getting IR function : LLVMIRmemcmp!\n")));
                        }
                        res1 = inner_builder.CreateCall(func_memcmp, {var_data, const_data, len_val}, "memcmp");
                    } else {
                        llvm::Function* func_bpchareq = llvmCodeGen->module()->getFunction("Jitted_bpchareq");
                        if (func_bpchareq == NULL) {
                            func_bpchareq = bpchareq_codegen();
                        }
                        res1 = inner_builder.CreateCall(func_bpchareq, {inargs[0], inargs[1]}, "bpchareq");
                    }
                } else {
                    if (fast_path) {
                        /* call LLVMIRmemcmp since both the var and const have
                         * the same length
                         */
                        llvm::Function* func_memcmp = llvmCodeGen->module()->getFunction("LLVMIRmemcmp");
                        if (func_memcmp == NULL) {
                            ereport(ERROR,
                                (errcode(ERRCODE_LOAD_IR_FUNCTION_FAILED),
                                    errmodule(MOD_LLVM),
                                    errmsg("Failed on getting IR function : LLVMIRmemcmp!\n")));
                        }
                        res1 = inner_builder.CreateCall(func_memcmp, {var_data, const_data, len_val}, "memcmp");
                        res1 = inner_builder.CreateICmpNE(res1, Datum_1);
                        res1 = inner_builder.CreateZExt(res1, int64Type);
                    } else {
                        llvm::Function* func_bpcharne = llvmCodeGen->module()->getFunction("Jitted_bpcharne");
                        if (func_bpcharne == NULL) {
                            func_bpcharne = bpcharne_codegen();
                        }
                        res1 = inner_builder.CreateCall(func_bpcharne, {inargs[0], inargs[1]}, "bpcharne");
                    }
                }
            } break;
            case DATEEQOID: {
                llvm::Function* func_dateeq = llvmCodeGen->module()->getFunction("Jitted_dateeq");
                if (func_dateeq == NULL) {
                    func_dateeq = date_eq_codegen();
                }
                res1 = inner_builder.CreateCall(func_dateeq, {inargs[0], inargs[1]}, "date_eq");
            } break;
            case DATENEOID: {
                llvm::Function* func_datene = llvmCodeGen->module()->getFunction("Jitted_datene");
                if (func_datene == NULL) {
                    func_datene = date_ne_codegen();
                }
                res1 = inner_builder.CreateCall(func_datene, {inargs[0], inargs[1]}, "date_ne");
            } break;
            case DATELTOID: {
                llvm::Function* func_datelt = llvmCodeGen->module()->getFunction("Jitted_datelt");
                if (func_datelt == NULL) {
                    func_datelt = date_lt_codegen();
                }
                res1 = inner_builder.CreateCall(func_datelt, {inargs[0], inargs[1]}, "date_lt");
            } break;
            case DATELEOID: {
                llvm::Function* func_datele = llvmCodeGen->module()->getFunction("Jitted_datele");
                if (func_datele == NULL) {
                    func_datele = date_le_codegen();
                }
                res1 = inner_builder.CreateCall(func_datele, {inargs[0], inargs[1]}, "date_le");
            } break;
            case DATEGTOID: {
                llvm::Function* func_dategt = llvmCodeGen->module()->getFunction("Jitted_dategt");
                if (func_dategt == NULL) {
                    func_dategt = date_gt_codegen();
                }
                res1 = inner_builder.CreateCall(func_dategt, {inargs[0], inargs[1]}, "date_gt");
            } break;
            case DATEGEOID: {
                llvm::Function* func_datege = llvmCodeGen->module()->getFunction("Jitted_datege");
                if (func_datege == NULL) {
                    func_datege = date_ge_codegen();
                }
                res1 = inner_builder.CreateCall(func_datege, {inargs[0], inargs[1]}, "date_ge");
            } break;
            case INT2EQOID: {
                llvm::Function* func_int2eq = llvmCodeGen->module()->getFunction("Jitted_int2eq");
                if (func_int2eq == NULL) {
                    func_int2eq = int2_sop_codegen<SOP_EQ>();
                }
                res1 = inner_builder.CreateCall(func_int2eq, {inargs[0], inargs[1]}, "int2_eq");
            } break;
            case INT24EQOID: {
                llvm::Function* func_int24eq = llvmCodeGen->module()->getFunction("Jitted_int24eq");
                if (func_int24eq == NULL) {
                    func_int24eq = int2_int4_sop_codegen<SOP_EQ, INT2OID>();
                }
                res1 = inner_builder.CreateCall(func_int24eq, {inargs[0], inargs[1]}, "int24_eq");
            } break;
            case INT42EQOID: {
                llvm::Function* func_int42eq = llvmCodeGen->module()->getFunction("Jitted_int42eq");
                if (func_int42eq == NULL) {
                    func_int42eq = int2_int4_sop_codegen<SOP_EQ, INT4OID>();
                }
                res1 = inner_builder.CreateCall(func_int42eq, {inargs[0], inargs[1]}, "int42_eq");
            } break;
            case INT24NEOID: {
                llvm::Function* func_int24ne = llvmCodeGen->module()->getFunction("Jitted_int24ne");
                if (func_int24ne == NULL) {
                    func_int24ne = int2_int4_sop_codegen<SOP_NEQ, INT2OID>();
                }
                res1 = inner_builder.CreateCall(func_int24ne, {inargs[0], inargs[1]}, "int24_ne");
            } break;
            case INT42NEOID: {
                llvm::Function* func_int42ne = llvmCodeGen->module()->getFunction("Jitted_int42ne");
                if (func_int42ne == NULL) {
                    func_int42ne = int2_int4_sop_codegen<SOP_NEQ, INT4OID>();
                }
                res1 = inner_builder.CreateCall(func_int42ne, {inargs[0], inargs[1]}, "int42_ne");
            } break;
            case INT24LTOID: {
                llvm::Function* func_int24lt = llvmCodeGen->module()->getFunction("Jitted_int24lt");
                if (func_int24lt == NULL) {
                    func_int24lt = int2_int4_sop_codegen<SOP_LT, INT2OID>();
                }
                res1 = inner_builder.CreateCall(func_int24lt, {inargs[0], inargs[1]}, "int24_lt");
            } break;
            case INT42LTOID: {
                llvm::Function* func_int42lt = llvmCodeGen->module()->getFunction("Jitted_int42lt");
                if (func_int42lt == NULL) {
                    func_int42lt = int2_int4_sop_codegen<SOP_LT, INT4OID>();
                }
                res1 = inner_builder.CreateCall(func_int42lt, {inargs[0], inargs[1]}, "int42_lt");
            } break;
            case INT24LEOID: {
                llvm::Function* func_int24le = llvmCodeGen->module()->getFunction("Jitted_int24le");
                if (func_int24le == NULL) {
                    func_int24le = int2_int4_sop_codegen<SOP_LE, INT2OID>();
                }
                res1 = inner_builder.CreateCall(func_int24le, {inargs[0], inargs[1]}, "int24_le");
            } break;
            case INT42LEOID: {
                llvm::Function* func_int42le = llvmCodeGen->module()->getFunction("Jitted_int42le");
                if (func_int42le == NULL) {
                    func_int42le = int2_int4_sop_codegen<SOP_LE, INT4OID>();
                }
                res1 = inner_builder.CreateCall(func_int42le, {inargs[0], inargs[1]}, "int42_le");
            } break;
            case INT24GTOID: {
                llvm::Function* func_int24gt = llvmCodeGen->module()->getFunction("Jitted_int24gt");
                if (func_int24gt == NULL) {
                    func_int24gt = int2_int4_sop_codegen<SOP_GT, INT2OID>();
                }
                res1 = inner_builder.CreateCall(func_int24gt, {inargs[0], inargs[1]}, "int24_gt");
            } break;
            case INT42GTOID: {
                llvm::Function* func_int42gt = llvmCodeGen->module()->getFunction("Jitted_int42gt");
                if (func_int42gt == NULL) {
                    func_int42gt = int2_int4_sop_codegen<SOP_GT, INT4OID>();
                }
                res1 = inner_builder.CreateCall(func_int42gt, {inargs[0], inargs[1]}, "int42_gt");
            } break;
            case INT24GEOID: {
                llvm::Function* func_int24ge = llvmCodeGen->module()->getFunction("Jitted_int24ge");
                if (func_int24ge == NULL) {
                    func_int24ge = int2_int4_sop_codegen<SOP_GE, INT2OID>();
                }
                res1 = inner_builder.CreateCall(func_int24ge, {inargs[0], inargs[1]}, "int24_ge");
            } break;
            case INT42GEOID: {
                llvm::Function* func_int42ge = llvmCodeGen->module()->getFunction("Jitted_int42ge");
                if (func_int42ge == NULL) {
                    func_int42ge = int2_int4_sop_codegen<SOP_GE, INT4OID>();
                }
                res1 = inner_builder.CreateCall(func_int42ge, {inargs[0], inargs[1]}, "int42_ge");
            } break;
            case INT48EQOID: {
                llvm::Function* func_int48eq = llvmCodeGen->module()->getFunction("Jitted_int48eq");
                if (func_int48eq == NULL) {
                    func_int48eq = int4_int8_sop_codegen<SOP_EQ, INT4OID>();
                }
                res1 = inner_builder.CreateCall(func_int48eq, {inargs[0], inargs[1]}, "int48_eq");
            } break;
            case INT4EQOID: {
                llvm::Function* func_int4eq = llvmCodeGen->module()->getFunction("Jitted_int4eq");
                if (func_int4eq == NULL) {
                    func_int4eq = int4_sop_codegen<SOP_EQ>();
                }
                res1 = inner_builder.CreateCall(func_int4eq, {inargs[0], inargs[1]}, "int4_eq");
            } break;
            case INT84EQOID: {
                llvm::Function* func_int84eq = llvmCodeGen->module()->getFunction("Jitted_int84eq");
                if (func_int84eq == NULL) {
                    func_int84eq = int4_int8_sop_codegen<SOP_EQ, INT8OID>();
                }
                res1 = inner_builder.CreateCall(func_int84eq, {inargs[0], inargs[1]}, "int84_eq");
            } break;
            case INT8EQOID: {
                llvm::Function* func_int8eq = llvmCodeGen->module()->getFunction("Jitted_int8eq");
                if (func_int8eq == NULL) {
                    func_int8eq = int8_sop_codegen<SOP_EQ>();
                }
                res1 = inner_builder.CreateCall(func_int8eq, {inargs[0], inargs[1]}, "int8_eq");
            } break;
            case INT2NEOID: {
                llvm::Function* func_int2ne = llvmCodeGen->module()->getFunction("Jitted_int2ne");
                if (func_int2ne == NULL) {
                    func_int2ne = int2_sop_codegen<SOP_NEQ>();
                }
                res1 = inner_builder.CreateCall(func_int2ne, {inargs[0], inargs[1]}, "int2_ne");
            } break;
            case INT4NEOID: {
                llvm::Function* func_int4ne = llvmCodeGen->module()->getFunction("Jitted_int4ne");
                if (func_int4ne == NULL) {
                    func_int4ne = int4_sop_codegen<SOP_NEQ>();
                }
                res1 = inner_builder.CreateCall(func_int4ne, {inargs[0], inargs[1]}, "int4_ne");
            } break;
            case INT8NEOID: {
                llvm::Function* func_int8ne = llvmCodeGen->module()->getFunction("Jitted_int8ne");
                if (func_int8ne == NULL) {
                    func_int8ne = int8_sop_codegen<SOP_NEQ>();
                }
                res1 = inner_builder.CreateCall(func_int8ne, {inargs[0], inargs[1]}, "int8_ne");
            } break;
            case INT84NEOID: {
                llvm::Function* func_int84ne = llvmCodeGen->module()->getFunction("Jitted_int84ne");
                if (func_int84ne == NULL) {
                    func_int84ne = int4_int8_sop_codegen<SOP_NEQ, INT8OID>();
                }
                res1 = inner_builder.CreateCall(func_int84ne, {inargs[0], inargs[1]}, "int84_ne");
            } break;
            case INT48NEOID: {
                llvm::Function* func_int48ne = llvmCodeGen->module()->getFunction("Jitted_int48ne");
                if (func_int48ne == NULL) {
                    func_int48ne = int4_int8_sop_codegen<SOP_NEQ, INT4OID>();
                }
                res1 = inner_builder.CreateCall(func_int48ne, {inargs[0], inargs[1]}, "int48_ne");
            } break;
            case INT2LTOID: {
                llvm::Function* func_int2lt = llvmCodeGen->module()->getFunction("Jitted_int2lt");
                if (func_int2lt == NULL) {
                    func_int2lt = int2_sop_codegen<SOP_LT>();
                }
                res1 = inner_builder.CreateCall(func_int2lt, {inargs[0], inargs[1]}, "int2_lt");
            } break;
            case INT4LTOID: {
                llvm::Function* func_int4lt = llvmCodeGen->module()->getFunction("Jitted_int4lt");
                if (func_int4lt == NULL) {
                    func_int4lt = int4_sop_codegen<SOP_LT>();
                }
                res1 = inner_builder.CreateCall(func_int4lt, {inargs[0], inargs[1]}, "int4_lt");
            } break;
            case INT48LTOID: {
                llvm::Function* func_int48lt = llvmCodeGen->module()->getFunction("Jitted_int48lt");
                if (func_int48lt == NULL) {
                    func_int48lt = int4_int8_sop_codegen<SOP_LT, INT4OID>();
                }
                res1 = inner_builder.CreateCall(func_int48lt, {inargs[0], inargs[1]}, "int48_lt");
            } break;
            case INT8LTOID: {
                llvm::Function* func_int8lt = llvmCodeGen->module()->getFunction("Jitted_int8lt");
                if (func_int8lt == NULL) {
                    func_int8lt = int8_sop_codegen<SOP_LT>();
                }
                res1 = inner_builder.CreateCall(func_int8lt, {inargs[0], inargs[1]}, "int8_lt");
            } break;
            case INT84LTOID: {
                llvm::Function* func_int84lt = llvmCodeGen->module()->getFunction("Jitted_int84lt");
                if (func_int84lt == NULL) {
                    func_int84lt = int4_int8_sop_codegen<SOP_LT, INT8OID>();
                }
                res1 = inner_builder.CreateCall(func_int84lt, {inargs[0], inargs[1]}, "int84_lt");
            } break;
            case INT2LEOID: {
                llvm::Function* func_int2le = llvmCodeGen->module()->getFunction("Jitted_int2le");
                if (func_int2le == NULL) {
                    func_int2le = int2_sop_codegen<SOP_LE>();
                }
                res1 = inner_builder.CreateCall(func_int2le, {inargs[0], inargs[1]}, "int2_le");
            } break;
            case INT4LEOID: {
                llvm::Function* func_int4le = llvmCodeGen->module()->getFunction("Jitted_int4le");
                if (func_int4le == NULL) {
                    func_int4le = int4_sop_codegen<SOP_LE>();
                }
                res1 = inner_builder.CreateCall(func_int4le, {inargs[0], inargs[1]}, "int4_le");
            } break;
            case INT8LEOID: {
                llvm::Function* func_int8le = llvmCodeGen->module()->getFunction("Jitted_int8le");
                if (func_int8le == NULL) {
                    func_int8le = int8_sop_codegen<SOP_LE>();
                }
                res1 = inner_builder.CreateCall(func_int8le, {inargs[0], inargs[1]}, "int8_le");
            } break;
            case INT84LEOID: {
                llvm::Function* func_int84le = llvmCodeGen->module()->getFunction("Jitted_int84le");
                if (func_int84le == NULL) {
                    func_int84le = int4_int8_sop_codegen<SOP_LE, INT8OID>();
                }
                res1 = inner_builder.CreateCall(func_int84le, {inargs[0], inargs[1]}, "int84_le");
            } break;
            case INT48LEOID: {
                llvm::Function* func_int48le = llvmCodeGen->module()->getFunction("Jitted_int48le");
                if (func_int48le == NULL) {
                    func_int48le = int4_int8_sop_codegen<SOP_LE, INT4OID>();
                }
                res1 = inner_builder.CreateCall(func_int48le, {inargs[0], inargs[1]}, "int48_le");
            } break;
            case INT2GTOID: {
                llvm::Function* func_int2gt = llvmCodeGen->module()->getFunction("Jitted_int2gt");
                if (func_int2gt == NULL) {
                    func_int2gt = int2_sop_codegen<SOP_GT>();
                }
                res1 = inner_builder.CreateCall(func_int2gt, {inargs[0], inargs[1]}, "int2_gt");
            } break;
            case INT4GTOID: {
                llvm::Function* func_int4gt = llvmCodeGen->module()->getFunction("Jitted_int4gt");
                if (func_int4gt == NULL) {
                    func_int4gt = int4_sop_codegen<SOP_GT>();
                }
                res1 = inner_builder.CreateCall(func_int4gt, {inargs[0], inargs[1]}, "int4_gt");
            } break;
            case INT48GTOID: {
                llvm::Function* func_int48gt = llvmCodeGen->module()->getFunction("Jitted_int48gt");
                if (func_int48gt == NULL) {
                    func_int48gt = int4_int8_sop_codegen<SOP_GT, INT4OID>();
                }
                res1 = inner_builder.CreateCall(func_int48gt, {inargs[0], inargs[1]}, "int48_gt");
            } break;
            case INT84GTOID: {
                llvm::Function* func_int84gt = llvmCodeGen->module()->getFunction("Jitted_int84gt");
                if (func_int84gt == NULL) {
                    func_int84gt = int4_int8_sop_codegen<SOP_GT, INT8OID>();
                }
                res1 = inner_builder.CreateCall(func_int84gt, {inargs[0], inargs[1]}, "int84_gt");
            } break;
            case INT8GTOID: {
                llvm::Function* func_int8gt = llvmCodeGen->module()->getFunction("Jitted_int8gt");
                if (func_int8gt == NULL) {
                    func_int8gt = int8_sop_codegen<SOP_GT>();
                }
                res1 = inner_builder.CreateCall(func_int8gt, {inargs[0], inargs[1]}, "int8_gt");
            } break;
            case INT2GEOID: {
                llvm::Function* func_int2ge = llvmCodeGen->module()->getFunction("Jitted_int2ge");
                if (func_int2ge == NULL) {
                    func_int2ge = int2_sop_codegen<SOP_GE>();
                }
                res1 = inner_builder.CreateCall(func_int2ge, {inargs[0], inargs[1]}, "int2_ge");
            } break;
            case INT4GEOID: {
                llvm::Function* func_int4ge = llvmCodeGen->module()->getFunction("Jitted_int4ge");
                if (func_int4ge == NULL) {
                    func_int4ge = int4_sop_codegen<SOP_GE>();
                }
                res1 = inner_builder.CreateCall(func_int4ge, {inargs[0], inargs[1]}, "int4_ge");
            } break;
            case INT48GEOID: {
                llvm::Function* func_int48ge = llvmCodeGen->module()->getFunction("Jitted_int48ge");
                if (func_int48ge == NULL) {
                    func_int48ge = int4_int8_sop_codegen<SOP_GE, INT4OID>();
                }
                res1 = inner_builder.CreateCall(func_int48ge, {inargs[0], inargs[1]}, "int48_ge");
            } break;
            case INT8GEOID: {
                llvm::Function* func_int8ge = llvmCodeGen->module()->getFunction("Jitted_int8ge");
                if (func_int8ge == NULL) {
                    func_int8ge = int8_sop_codegen<SOP_GE>();
                }
                res1 = inner_builder.CreateCall(func_int8ge, {inargs[0], inargs[1]}, "int8_ge");
            } break;
            case INT84GEOID: {
                llvm::Function* func_int84ge = llvmCodeGen->module()->getFunction("Jitted_int84ge");
                if (func_int84ge == NULL) {
                    func_int84ge = int4_int8_sop_codegen<SOP_GE, INT8OID>();
                }
                res1 = inner_builder.CreateCall(func_int84ge, {inargs[0], inargs[1]}, "int84_ge");
            } break;
            case INT4MULOID: {
                llvm::Function* func_int4mul = llvmCodeGen->module()->getFunction("Jitted_int4mul");
                if (func_int4mul == NULL) {
                    func_int4mul = int4mul_codegen();
                }
                res1 = inner_builder.CreateCall(func_int4mul, {inargs[0], inargs[1]}, "int4_mul");
            } break;
            case INT8MULOID: {
                llvm::Function* func_int8mul = llvmCodeGen->module()->getFunction("Jitted_int8mul");
                if (func_int8mul == NULL) {
                    func_int8mul = int8mul_codegen();
                }
                res1 = inner_builder.CreateCall(func_int8mul, {inargs[0], inargs[1]}, "int8_mul");
            } break;
            case INT48MULOID: {
                llvm::Function* func_int48mul = llvmCodeGen->module()->getFunction("Jitted_int48mul");
                if (func_int48mul == NULL) {
                    func_int48mul = int48mul_codegen();
                }
                res1 = inner_builder.CreateCall(func_int48mul, {inargs[0], inargs[1]}, "int48_mul");
            } break;
            case INT84MULOID: {
                llvm::Function* func_int84mul = llvmCodeGen->module()->getFunction("Jitted_int84mul");
                if (func_int84mul == NULL) {
                    func_int84mul = int84mul_codegen();
                }
                res1 = inner_builder.CreateCall(func_int84mul, {inargs[0], inargs[1]}, "int84_mul");
            } break;
            case INT4PLOID: {
                llvm::Function* func_int4pl = llvmCodeGen->module()->getFunction("Jitted_int4pl");
                if (func_int4pl == NULL) {
                    func_int4pl = int4pl_codegen();
                }
                res1 = inner_builder.CreateCall(func_int4pl, {inargs[0], inargs[1]}, "int4_pl");
            } break;
            case INT8PLOID: {
                llvm::Function* func_int8pl = llvmCodeGen->module()->getFunction("Jitted_int8pl");
                if (func_int8pl == NULL) {
                    func_int8pl = int8pl_codegen();
                }
                res1 = inner_builder.CreateCall(func_int8pl, {inargs[0], inargs[1]}, "int8_pl");
            } break;
            case INT48PLOID: {
                llvm::Function* func_int48pl = llvmCodeGen->module()->getFunction("Jitted_int48pl");
                if (func_int48pl == NULL) {
                    func_int48pl = int48pl_codegen();
                }
                res1 = inner_builder.CreateCall(func_int48pl, {inargs[0], inargs[1]}, "int48_pl");
            } break;
            case INT84PLOID: {
                llvm::Function* func_int84pl = llvmCodeGen->module()->getFunction("Jitted_int84pl");
                if (func_int84pl == NULL) {
                    func_int84pl = int84pl_codegen();
                }
                res1 = inner_builder.CreateCall(func_int84pl, {inargs[0], inargs[1]}, "int84_pl");
            } break;
            case INT4MIOID: {
                llvm::Function* func_int4mi = llvmCodeGen->module()->getFunction("Jitted_int4mi");
                if (func_int4mi == NULL) {
                    func_int4mi = int4mi_codegen();
                }
                res1 = inner_builder.CreateCall(func_int4mi, {inargs[0], inargs[1]}, "int4_mi");
            } break;
            case INT8MIOID: {
                llvm::Function* func_int8mi = llvmCodeGen->module()->getFunction("Jitted_int8mi");
                if (func_int8mi == NULL) {
                    func_int8mi = int8mi_codegen();
                }
                res1 = inner_builder.CreateCall(func_int8mi, {inargs[0], inargs[1]}, "int8_mi");
            } break;
            case INT48MIOID: {
                llvm::Function* func_int48mi = llvmCodeGen->module()->getFunction("Jitted_int48mi");
                if (func_int48mi == NULL) {
                    func_int48mi = int48mi_codegen();
                }
                res1 = inner_builder.CreateCall(func_int48mi, {inargs[0], inargs[1]}, "int48_mi");
            } break;
            case INT84MIOID: {
                llvm::Function* func_int84mi = llvmCodeGen->module()->getFunction("Jitted_int84mi");
                if (func_int84mi == NULL) {
                    func_int84mi = int84mi_codegen();
                }
                res1 = inner_builder.CreateCall(func_int84mi, {inargs[0], inargs[1]}, "int84_mi");
            } break;
            case INT4DIVOID: {
                llvm::Function* func_int4div = llvmCodeGen->module()->getFunction("Jitted_int4div");
                if (func_int4div == NULL) {
                    func_int4div = int4div_codegen();
                }
                res1 = inner_builder.CreateCall(func_int4div, {inargs[0], inargs[1]}, "int4_div");
            } break;
            case INT8DIVOID: {
                llvm::Function* func_int8div = llvmCodeGen->module()->getFunction("Jitted_int8div");
                if (func_int8div == NULL) {
                    func_int8div = int8div_codegen();
                }
                res1 = inner_builder.CreateCall(func_int8div, {inargs[0], inargs[1]}, "int8_div");
            } break;
            case INT48DIVOID: {
                llvm::Function* func_int48div = llvmCodeGen->module()->getFunction("Jitted_int48div");
                if (func_int48div == NULL) {
                    func_int48div = int48div_codegen();
                }
                res1 = inner_builder.CreateCall(func_int48div, {inargs[0], inargs[1]}, "int48_div");
            } break;
            case INT84DIVOID: {
                llvm::Function* func_int84div = llvmCodeGen->module()->getFunction("Jitted_int84div");
                if (func_int84div == NULL) {
                    func_int84div = int84div_codegen();
                }
                res1 = inner_builder.CreateCall(func_int84div, {inargs[0], inargs[1]}, "int84_div");
            } break;
            case FLOAT8EQOID: {
                llvm::Function* func_float8eq = llvmCodeGen->module()->getFunction("Jitted_float8eq");
                if (func_float8eq == NULL) {
                    func_float8eq = float8eq_codegen();
                }
                res1 = inner_builder.CreateCall(func_float8eq, {inargs[0], inargs[1]}, "float8_eq");
            } break;
            case FLOAT8NEOID: {
                llvm::Function* func_float8ne = llvmCodeGen->module()->getFunction("Jitted_float8ne");
                if (func_float8ne == NULL) {
                    func_float8ne = float8ne_codegen();
                }
                res1 = inner_builder.CreateCall(func_float8ne, {inargs[0], inargs[1]}, "float8_ne");
            } break;
            case FLOAT8LTOID: {
                llvm::Function* func_float8lt = llvmCodeGen->module()->getFunction("Jitted_float8lt");
                if (func_float8lt == NULL) {
                    func_float8lt = float8lt_codegen();
                }
                res1 = inner_builder.CreateCall(func_float8lt, {inargs[0], inargs[1]}, "float8_lt");
            } break;
            case FLOAT8LEOID: {
                llvm::Function* func_float8le = llvmCodeGen->module()->getFunction("Jitted_float8le");
                if (func_float8le == NULL) {
                    func_float8le = float8le_codegen();
                }
                res1 = inner_builder.CreateCall(func_float8le, {inargs[0], inargs[1]}, "float8_le");
            } break;
            case FLOAT8GTOID: {
                llvm::Function* func_float8gt = llvmCodeGen->module()->getFunction("Jitted_float8gt");
                if (func_float8gt == NULL) {
                    func_float8gt = float8gt_codegen();
                }
                res1 = inner_builder.CreateCall(func_float8gt, {inargs[0], inargs[1]}, "float8_gt");
            } break;
            case FLOAT8GEOID: {
                llvm::Function* func_float8ge = llvmCodeGen->module()->getFunction("Jitted_float8ge");
                if (func_float8ge == NULL) {
                    func_float8ge = float8ge_codegen();
                }
                res1 = inner_builder.CreateCall(func_float8ge, {inargs[0], inargs[1]}, "float8_ge");
            } break;
            case TIMESTAMPEQOID: {
                llvm::Function* func_timestampeq = llvmCodeGen->module()->getFunction("Jitted_timestampeq");
                if (func_timestampeq == NULL) {
                    func_timestampeq = timestamp_eq_codegen();
                }
                res1 = inner_builder.CreateCall(func_timestampeq, {inargs[0], inargs[1]}, "timestamp_eq");
            } break;
            case TIMESTAMPNEOID: {
                llvm::Function* func_timestampne = llvmCodeGen->module()->getFunction("Jitted_timestampne");
                if (func_timestampne == NULL) {
                    func_timestampne = timestamp_ne_codegen();
                }
                res1 = inner_builder.CreateCall(func_timestampne, {inargs[0], inargs[1]}, "timestamp_ne");
            } break;
            case TIMESTAMPLTOID: {
                llvm::Function* func_timestamplt = llvmCodeGen->module()->getFunction("Jitted_timestamplt");
                if (func_timestamplt == NULL) {
                    func_timestamplt = timestamp_lt_codegen();
                }
                res1 = inner_builder.CreateCall(func_timestamplt, {inargs[0], inargs[1]}, "timestamp_lt");
            } break;
            case TIMESTAMPLEOID: {
                llvm::Function* func_timestample = llvmCodeGen->module()->getFunction("Jitted_timestample");
                if (func_timestample == NULL) {
                    func_timestample = timestamp_le_codegen();
                }
                res1 = inner_builder.CreateCall(func_timestample, {inargs[0], inargs[1]}, "timestamp_le");
            } break;
            case TIMESTAMPGTOID: {
                llvm::Function* func_timestampgt = llvmCodeGen->module()->getFunction("Jitted_timestampgt");
                if (func_timestampgt == NULL) {
                    func_timestampgt = timestamp_gt_codegen();
                }
                res1 = inner_builder.CreateCall(func_timestampgt, {inargs[0], inargs[1]}, "timestamp_gt");
            } break;
            case TIMESTAMPGEOID: {
                llvm::Function* func_timestampge = llvmCodeGen->module()->getFunction("Jitted_timestampge");
                if (func_timestampge == NULL) {
                    func_timestampge = timestamp_ge_codegen();
                }
                res1 = inner_builder.CreateCall(func_timestampge, {inargs[0], inargs[1]}, "timestamp_ge");
            } break;
            case NUMERICADDOID: {
                llvm::Function* func_numericadd = llvmCodeGen->module()->getFunction("Jitted_numericadd");
                if (func_numericadd == NULL) {
                    func_numericadd = numeric_sop_codegen<BIADD>();
                }
                res1 = inner_builder.CreateCall(func_numericadd, {inargs[0], inargs[1]}, "numeric_add");
            } break;
            case NUMERICSUBOID: {
                llvm::Function* func_numericsub = llvmCodeGen->module()->getFunction("Jitted_numericsub");
                if (func_numericsub == NULL) {
                    func_numericsub = numeric_sop_codegen<BISUB>();
                }
                res1 = inner_builder.CreateCall(func_numericsub, {inargs[0], inargs[1]}, "numeric_sub");
            } break;
            case NUMERICMULOID: {
                llvm::Function* func_numericmul = llvmCodeGen->module()->getFunction("Jitted_numericmul");
                if (func_numericmul == NULL) {
                    func_numericmul = numeric_sop_codegen<BIMUL>();
                }
                res1 = inner_builder.CreateCall(func_numericmul, {inargs[0], inargs[1]}, "numeric_mul");
            } break;
            case NUMERICDIVOID: {
                llvm::Function* func_numericdiv = llvmCodeGen->module()->getFunction("Jitted_numericdiv");
                if (func_numericdiv == NULL) {
                    func_numericdiv = numeric_sop_codegen<BIDIV>();
                }
                res1 = inner_builder.CreateCall(func_numericdiv, {inargs[0], inargs[1]}, "numeric_div");
            } break;
            case NUMERICEQOID:
            case NUMERICNEOID:
            case NUMERICLEOID:
            case NUMERICLTOID:
            case NUMERICGTOID:
            case NUMERICGEOID: {
                bool fast_numericop = NumericOpFastJittable(args->exprstate);
                if (fast_numericop) {
                    res1 = FastNumericOpCodeGen(&inner_builder, args->exprstate, inargs[0], inargs[1]);
                } else {
                    res1 = NormalNumericOpCodeGen(&inner_builder, opexpr->opno, inargs[0], inargs[1]);
                }
            } break;
            default: {
                LLVMFuncCallInfo lfcinfo;
                lfcinfo.nargs = num_args;
                lfcinfo.args = &inargs[0];
                res1 = EvalFuncResultCodeGen(&inner_builder, fcache, isNull, &lfcinfo);
            } break;
        }
        inner_builder.CreateStore(null_false, isNull);
        inner_builder.CreateBr(ret_bb);

        inner_builder.SetInsertPoint(be_null);
        res2 = Datum_0;
        inner_builder.CreateStore(null_true, isNull);
        inner_builder.CreateBr(ret_bb);
        inner_builder.SetInsertPoint(ret_bb);
        llvm::PHINode* Phi_ret = inner_builder.CreatePHI(int64Type, 2);
        Phi_ret->addIncoming(res1, bnot_null);
        Phi_ret->addIncoming(res2, be_null);
        inner_builder.CreateRet(Phi_ret);
    } else {
        LLVMFuncCallInfo lfcinfo;
        lfcinfo.nargs = num_args;
        lfcinfo.args = &inargs[0];
        lfcinfo.argnulls = &inargnulls[0];
        llvm::Value* res = EvalFuncResultCodeGen(&inner_builder, fcache, isNull, &lfcinfo);
        inner_builder.CreateRet(res);
    }

    llvmCodeGen->FinalizeFunction(jitted_vop);

    result = outer_builder->CreateCall(
        jitted_vop, {args->llvm_args[0], args->llvm_args[1], args->llvm_args[2]}, "EvalVecOp");
    return result;
}

llvm::Value* VecExprCodeGen::FastNumericOpCodeGen(
    GsCodeGen::LlvmBuilder* ptrbuilder, ExprState* estate, llvm::Value* larg, llvm::Value* rarg)
{
    dorado::GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    FuncExprState* fcache = (FuncExprState*)estate;
    OpExpr* opexpr = (OpExpr*)(estate->expr);
    List* op_args = fcache->args;
    ExprState* lestate = (ExprState*)linitial(op_args);
    ;
    ExprState* restate = (ExprState*)lsecond(op_args);
    ;

    bool isvarconst = true;
    if (!(IsA(lestate->expr, Var) && IsA(restate->expr, Const)))
        isvarconst = false;

    /* construct fast path only for numeric eq/neq/lt/le/gt/ge */
    llvm::Value* result = NULL;
    switch (opexpr->opno) {
        case NUMERICEQOID: {
            if (isvarconst) {
                llvm::Function* func_fnumericeq = llvmCodeGen->module()->getFunction("Jitted_fast_numericeq");
                if (func_fnumericeq == NULL) {
                    func_fnumericeq = fast_numericbi_sop_codegen<BIEQ>();
                }
                result = ptrbuilder->CreateCall(func_fnumericeq, {larg, rarg}, "numeric_eq");
            } else {
                llvm::Function* func_fnumericeq = llvmCodeGen->module()->getFunction("Jitted_fast_revnumericeq");
                if (func_fnumericeq == NULL) {
                    func_fnumericeq = fast_numericbi_sop_codegen<BIEQ>();
                }
                result = ptrbuilder->CreateCall(func_fnumericeq, {rarg, larg}, "numeric_eq");
            }
        } break;
        case NUMERICNEOID: {
            if (isvarconst) {
                llvm::Function* func_fnumericne = llvmCodeGen->module()->getFunction("Jitted_fast_numericne");
                if (func_fnumericne == NULL) {
                    func_fnumericne = fast_numericbi_sop_codegen<BINEQ>();
                }
                result = ptrbuilder->CreateCall(func_fnumericne, {larg, rarg}, "numeric_ne");
            } else {
                llvm::Function* func_fnumericne = llvmCodeGen->module()->getFunction("Jitted_fast_revnumericne");
                if (func_fnumericne == NULL) {
                    func_fnumericne = fast_numericbi_sop_codegen<BINEQ>();
                }
                result = ptrbuilder->CreateCall(func_fnumericne, {rarg, larg}, "numeric_ne");
            }
        } break;
        case NUMERICLTOID: {
            if (isvarconst) {
                llvm::Function* func_fnumericlt = llvmCodeGen->module()->getFunction("Jitted_fast_numericlt");
                if (func_fnumericlt == NULL) {
                    func_fnumericlt = fast_numericbi_sop_codegen<BILT>();
                }
                result = ptrbuilder->CreateCall(func_fnumericlt, {larg, rarg}, "numeric_lt");
            } else {
                llvm::Function* func_fnumericlt = llvmCodeGen->module()->getFunction("Jitted_fast_revnumericlt");
                if (func_fnumericlt == NULL) {
                    func_fnumericlt = fast_numericbi_sop_codegen<BIGT>();
                }
                result = ptrbuilder->CreateCall(func_fnumericlt, {rarg, larg}, "reverse_numeric_gt");
            }
        } break;
        case NUMERICLEOID: {
            if (isvarconst) {
                llvm::Function* func_fnumericle = llvmCodeGen->module()->getFunction("Jitted_fast_numericle");
                if (func_fnumericle == NULL) {
                    func_fnumericle = fast_numericbi_sop_codegen<BILE>();
                }
                result = ptrbuilder->CreateCall(func_fnumericle, {larg, rarg}, "numeric_le");
            } else {
                llvm::Function* func_fnumericle = llvmCodeGen->module()->getFunction("Jitted_fast_revnumericle");
                if (func_fnumericle == NULL) {
                    func_fnumericle = fast_numericbi_sop_codegen<BIGE>();
                }
                result = ptrbuilder->CreateCall(func_fnumericle, {rarg, larg}, "reverse_numeric_ge");
            }
        } break;
        case NUMERICGTOID: {
            if (isvarconst) {
                llvm::Function* func_fnumericgt = llvmCodeGen->module()->getFunction("Jitted_fast_numericgt");
                if (func_fnumericgt == NULL) {
                    func_fnumericgt = fast_numericbi_sop_codegen<BIGT>();
                }
                result = ptrbuilder->CreateCall(func_fnumericgt, {larg, rarg}, "numeric_gt");
            } else {
                llvm::Function* func_fnumericgt = llvmCodeGen->module()->getFunction("Jitted_fast_revnumericgt");
                if (func_fnumericgt == NULL) {
                    func_fnumericgt = fast_numericbi_sop_codegen<BILT>();
                }
                result = ptrbuilder->CreateCall(func_fnumericgt, {rarg, larg}, "reverse_numeric_lt");
            }
        } break;
        case NUMERICGEOID: {
            if (isvarconst) {
                llvm::Function* func_fnumericge = llvmCodeGen->module()->getFunction("Jitted_fast_numericge");
                if (func_fnumericge == NULL) {
                    func_fnumericge = fast_numericbi_sop_codegen<BIGE>();
                }
                result = ptrbuilder->CreateCall(func_fnumericge, {larg, rarg}, "numeric_ge");
            } else {
                llvm::Function* func_fnumericge = llvmCodeGen->module()->getFunction("Jitted_fast_revnumericge");
                if (func_fnumericge == NULL) {
                    func_fnumericge = fast_numericbi_sop_codegen<BILE>();
                }
                result = ptrbuilder->CreateCall(func_fnumericge, {rarg, larg}, "reverse_numeric_le");
            }
        } break;
        default:
            Assert(0);
            break;
    }

    return result;
}

llvm::Value* VecExprCodeGen::NormalNumericOpCodeGen(
    GsCodeGen::LlvmBuilder* ptrbuilder, Oid funcoid, llvm::Value* larg, llvm::Value* rarg)
{
    llvm::Value* result = NULL;
    dorado::GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    switch (funcoid) {
        case NUMERICEQOID: {
            llvm::Function* func_numericeq = llvmCodeGen->module()->getFunction("Jitted_numericeq");
            if (func_numericeq == NULL) {
                func_numericeq = numeric_sop_codegen<BIEQ>();
            }
            result = ptrbuilder->CreateCall(func_numericeq, {larg, rarg}, "numeric_eq");
        } break;
        case NUMERICNEOID: {
            llvm::Function* func_numericne = llvmCodeGen->module()->getFunction("Jitted_numericne");
            if (func_numericne == NULL) {
                func_numericne = numeric_sop_codegen<BINEQ>();
            }
            result = ptrbuilder->CreateCall(func_numericne, {larg, rarg}, "numeric_ne");
        } break;
        case NUMERICLEOID: {
            llvm::Function* func_numericle = llvmCodeGen->module()->getFunction("Jitted_numericle");
            if (func_numericle == NULL) {
                func_numericle = numeric_sop_codegen<BILE>();
            }
            result = ptrbuilder->CreateCall(func_numericle, {larg, rarg}, "numeric_le");
        } break;
        case NUMERICLTOID: {
            llvm::Function* func_numericlt = llvmCodeGen->module()->getFunction("Jitted_numericlt");
            if (func_numericlt == NULL) {
                func_numericlt = numeric_sop_codegen<BILT>();
            }
            result = ptrbuilder->CreateCall(func_numericlt, {larg, rarg}, "numeric_lt");
        } break;
        case NUMERICGEOID: {
            llvm::Function* func_numericge = llvmCodeGen->module()->getFunction("Jitted_numericge");
            if (func_numericge == NULL) {
                func_numericge = numeric_sop_codegen<BIGE>();
            }
            result = ptrbuilder->CreateCall(func_numericge, {larg, rarg}, "numeric_ge");
        } break;
        case NUMERICGTOID: {
            llvm::Function* func_numericgt = llvmCodeGen->module()->getFunction("Jitted_numericgt");
            if (func_numericgt == NULL) {
                func_numericgt = numeric_sop_codegen<BIGT>();
            }
            result = ptrbuilder->CreateCall(func_numericgt, {larg, rarg}, "numeric_gt");
        } break;
        default:
            Assert(0);
            break;
    }

    return result;
}

llvm::Value* VecExprCodeGen::VarCodeGen(ExprCodeGenArgs* args)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder* outer_builder = args->builder;
    GsCodeGen::LlvmBuilder inner_builder(context);

    llvm::Value* econtext = NULL;
    llvm::Value* isNull = NULL;
    llvm::Value* loop_index = NULL;

    /* Define the datatype and variables that needed */
    DEFINE_CG_TYPE(int8Type, BOOLOID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(int64PtrType, INT8OID);
    DEFINE_CG_PTRTYPE(ExprContextPtrType, "struct.ExprContext");
    DEFINE_CG_PTRTYPE(VecBatchPtrType, "class.VectorBatch");
    DEFINE_CG_PTRTYPE(ScalarVectorPtrType, "class.ScalarVector");

    DEFINE_CGVAR_INT8(null_false, 0);
    DEFINE_CGVAR_INT8(null_true, 1);
    DEFINE_CGVAR_INT32(m_flagoffset, 3);
    DEFINE_CGVAR_INT32(m_arroffset, 4);
    DEFINE_CGVAR_INT32(m_valsoffset, 5);
    DEFINE_CGVAR_INT32(ecxt_scanbatch_offset, 17);
    DEFINE_CGVAR_INT32(ecxt_innerbatch_offset, 18);
    DEFINE_CGVAR_INT32(ecxt_outerbatch_offset, 19);
    DEFINE_CGVAR_INT64(val_0, 0);

    llvm::Value* llvmargs[3];
    llvm::Value* argVals[2];
    llvm::Value* ecxt_batch = NULL;
    llvm::Value* argFlag = NULL;
    llvm::Value* argVector = NULL;
    llvm::Value* result = NULL;
    llvm::Value* val1 = NULL;
    llvm::Value* val2 = NULL;

    /* Define llvm function */
    DEFINE_FUNCTION_WITH_3_ARGS(jitted_sval,
        "JittedVecScanVar",
        "econtext",
        ExprContextPtrType,
        "isNull",
        int8PtrType,
        "loop_index",
        int64Type);

    econtext = llvmargs[0];
    isNull = llvmargs[1];
    loop_index = llvmargs[2];

    /*
     * Get the VectorBatch from econtext according to varno.
     *
     * The second value of array argVals represents the ordinal number in data
     * structure 'ExprContext', say, *ecxt->innerbatch is the 18th element of
     * ExprContext. It also means if the order is changed, we should change these
     * values correspondingly.
     */
    Var* var = (Var*)((args->exprstate)->expr);
    bool is_notnull = false;
    FormData_pg_attribute* attr = NULL;
    if ((args->parent != NULL) && IsA(args->parent, CStoreScanState)) {
        CStoreScanState* scanstate = (CStoreScanState*)args->parent;
        if (!scanstate->isPartTbl && scanstate->ss_currentRelation) {
            attr = scanstate->ss_currentRelation->rd_att->attrs[var->varattno - 1];
            is_notnull = attr->attnotnull;
        }
    }
    /* extract the value from batch according to Var expr */
    argVals[0] = val_0;
    if (var->varno == INNER_VAR) {
        argVals[1] = ecxt_innerbatch_offset;
    } else if (var->varno == OUTER_VAR) {
        argVals[1] = ecxt_outerbatch_offset;
    } else {
        argVals[1] = ecxt_scanbatch_offset;
    }
    ecxt_batch = inner_builder.CreateInBoundsGEP(econtext, argVals);
    ecxt_batch = inner_builder.CreateLoad(VecBatchPtrType, ecxt_batch, "batch");

    /* get batch->m_arr from VectorBatch according to varattno */
    argVals[1] = m_arroffset;
    argVector = inner_builder.CreateInBoundsGEP(ecxt_batch, argVals);
    argVector = inner_builder.CreateLoad(ScalarVectorPtrType, argVector, "m_arr");

    /* get m_arr[var->varattno-1].m_flag */
    DEFINE_CGVAR_INT64(m_attno, var->varattno - 1);

    /*
     * if the attribute is defined with 'NOT NULL' constraint, we no need to
     * load the m_flag, else check the m_flag to see is this value is NULL.
     */
    if (!is_notnull) {
        /* Define the basic block of if...else...end structure */
        llvm::BasicBlock* entry = &jitted_sval->getEntryBlock();
        DEFINE_BLOCK(if_then, jitted_sval);
        DEFINE_BLOCK(if_else, jitted_sval);
        DEFINE_BLOCK(if_end, jitted_sval);

        argVals[0] = m_attno;
        argVals[1] = m_flagoffset;
        argFlag = inner_builder.CreateInBoundsGEP(argVector, argVals);
        argFlag = inner_builder.CreateLoad(int8PtrType, argFlag, "m_flag");

        /* get m_arr[var->varattno-1].m_flag[loop_index] */
        inner_builder.SetInsertPoint(entry);
        llvm::Value* tmpNull = inner_builder.CreateInBoundsGEP(argFlag, loop_index);
        tmpNull = inner_builder.CreateLoad(int8Type, tmpNull);

        /*
         * As we do in c-code, we should use (flag)&V_NULL_MASK to get the actual
         * flag. After this, mask isNull with null_true and null_false.
         */
        tmpNull = inner_builder.CreateAnd(tmpNull, null_true);

        llvm::Value* cmp = inner_builder.CreateICmpEQ(tmpNull, null_false);
        inner_builder.CreateCondBr(cmp, if_then, if_else);

        /* only fetch the val when m_arr[var->varattno-1].m_vals[loop_index] is not null */
        inner_builder.SetInsertPoint(if_then);
        argVals[1] = m_valsoffset;
        argVector = inner_builder.CreateInBoundsGEP(argVector, argVals);
        argVector = inner_builder.CreateLoad(int64PtrType, argVector, "m_vals");

        /* get m_arr[var->varattno-1].m_vals[loop_index] */
        argVector = inner_builder.CreateInBoundsGEP(argVector, loop_index);
        val1 = inner_builder.CreateLoad(int64Type, argVector);
        inner_builder.CreateStore(null_false, isNull);
        inner_builder.CreateBr(if_end);

        /* if null, return (Datum)0 */
        inner_builder.SetInsertPoint(if_else);
        val2 = val_0;
        inner_builder.CreateStore(null_true, isNull);
        inner_builder.CreateBr(if_end);

        inner_builder.SetInsertPoint(if_end);
        llvm::PHINode* Phi_ret = inner_builder.CreatePHI(int64Type, 2);
        Phi_ret->addIncoming(val1, if_then);
        Phi_ret->addIncoming(val2, if_else);
        inner_builder.CreateRet(Phi_ret);
    } else {
        argVals[0] = m_attno;
        argVals[1] = m_valsoffset;
        argVector = inner_builder.CreateInBoundsGEP(argVector, argVals);
        argVector = inner_builder.CreateLoad(int64PtrType, argVector, "m_vals");
        argVector = inner_builder.CreateInBoundsGEP(argVector, loop_index);
        llvm::Value* res = inner_builder.CreateLoad(int64Type, argVector);
        inner_builder.CreateStore(null_false, isNull);
        inner_builder.CreateRet(res);
    }

    llvmCodeGen->FinalizeFunction(jitted_sval);

    result = outer_builder->CreateCall(
        jitted_sval, {args->llvm_args[0], args->llvm_args[1], args->llvm_args[2]}, "VecEvalVar");

    return result;
}

llvm::Value* VecExprCodeGen::ConstCodeGen(ExprCodeGenArgs* args)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder* outer_builder = args->builder;
    GsCodeGen::LlvmBuilder inner_builder(context);

    /* Define data type and variables */
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(ExprContextPtrType, "struct.ExprContext");

    llvm::Value* llvmargs[3];
    llvm::Value* isNull = NULL;
    llvm::Value* tmpNull = NULL;
    llvm::Value* result = NULL;

    /* Define llvm function */
    DEFINE_FUNCTION_WITH_3_ARGS(
        jitted_cst, "JittedEvalConst", "econtext", ExprContextPtrType, "isNull", int8PtrType, "loop_index", int64Type);

    isNull = llvmargs[1];

    /* restore isNull flag */
    Const* cst = (Const*)((args->exprstate)->expr);
    tmpNull = llvmCodeGen->getIntConstant(CHAROID, cst->constisnull);
    inner_builder.CreateStore(tmpNull, isNull);

    ScalarValue val = ScalarVector::DatumToScalar(cst->constvalue, cst->consttype, cst->constisnull);
    result = llvmCodeGen->getIntConstant(INT8OID, val);

    inner_builder.CreateRet(result);

    llvmCodeGen->FinalizeFunction(jitted_cst);

    result = outer_builder->CreateCall(
        jitted_cst, {args->llvm_args[0], args->llvm_args[1], args->llvm_args[2]}, "VecEvalConst");
    return result;
}

/*
 * We show here an example to interpret the generated IR function with the
 * following IR API Builder Code. If we have the query:
 *          select (a + 1) * 2 from test;
 * then we arrived at the following IR function:
 * define i8 @JittedVecTargetList(%struct.ExprContext* %econtext, %class.VectorBatch* %pBatch) {
 * entry:
 *  %0 = alloca i8
 *  %1 = call %class.VectorBatch* @LLVMWrapGetVecBatch(%struct.ExprContext* %econtext)
 *  %2 = getelementptr inbounds %class.VectorBatch* %1, i64 0, i32 0
 *  %m_rows = load i32* %2, align 4
 *  %3 = getelementptr inbounds %class.VectorBatch* %pBatch, i64 0, i32 0
 *  store i32 %m_rows, i32* %3, align 4
 *  %4 = icmp sgt i32 %m_rows, 0
 *  br i1 %4, label %for_body, label %ret_bb

 * for_body:                                         ; preds = %bb_end, %entry
 *  %5 = phi i64 [ 0, %entry ], [ %6, %bb_end ]
 *  %6 = add i64 %5, 1
 *  %7 = getelementptr inbounds %class.VectorBatch* %pBatch, i64 0, i32 4
 *  %m_arr = load %class.ScalarVector** %7, align 8
 * %EvalVecOp = call i64 @JittedVecOper(%struct.ExprContext* %econtext, i8* %0, i64 %5)
 *  %8 = getelementptr inbounds %class.ScalarVector* %m_arr, i32 0, i32 5
 *  %m_vals = load i64** %8, align 8
 *  %9 = getelementptr inbounds i64* %m_vals, i64 %5
 *  store i64 %EvalVecOp, i64* %9, align 8
 *  %10 = getelementptr inbounds %class.ScalarVector* %m_arr, i32 0, i32 3
 *  %m_flag = load i8** %10, align 1
 *  %11 = getelementptr inbounds i8* %m_flag, i64 %5
 *  %12 = load i8* %0, align 1
 *  store i8 %12, i8* %11, align 1
 *  br label %bb_end

 * bb_end:                                           ; preds = %for_body
 *  %13 = trunc i64 %6 to i32
 *  %14 = icmp eq i32 %13, %m_rows
 *  br i1 %14, label %for_end, label %for_body

 * for_end:                                          ; preds = %bb_end
 *  %15 = getelementptr inbounds %class.ScalarVector* %m_arr, i32 0, i32 0
 *  store i32 %m_rows, i32* %15, align 4
 *  br label %ret_bb

 * ret_bb:                                           ; preds = %for_end, %entry
 *  ret i8 1
 * }
 * */
llvm::Function* VecExprCodeGen::TargetListCodeGen(List* targetlist, PlanState* parent)
{
    Assert(NULL != (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj);
    dorado::GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    /* If can not code gen qual, just return false */
    if (!TargetListJittable(targetlist))
        return NULL;

    /*
     * If we already have a module, we do not need to load the IR
     * file to get the module.
     */
    llvmCodeGen->loadIRFile();

    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    /* Define the data type, const value, and some variables */
    DEFINE_CG_TYPE(int8Type, CHAROID);
    DEFINE_CG_TYPE(int32Type, INT4OID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_TYPE(boolType, BOOLOID);
    DEFINE_CG_PTRTYPE(int8PtrType, CHAROID);
    DEFINE_CG_PTRTYPE(int64PtrType, INT8OID);
    DEFINE_CG_PTRTYPE(VectorBatchPtrType, "class.VectorBatch");
    DEFINE_CG_PTRTYPE(ExprContextPtrType, "struct.ExprContext");
    DEFINE_CG_PTRTYPE(ScalarVectorPtrType, "class.ScalarVector");
    DEFINE_CG_PTRTYPE(MemContextPtrType, "struct.MemoryContextData");

    /* Get the offset of element in data struct */
    DEFINE_CGVAR_INT8(int8_1, 1);
    DEFINE_CGVAR_INT32(int32_0, 0);
    DEFINE_CGVAR_INT32(m_flagoffset, 3);
    DEFINE_CGVAR_INT32(m_arroffset, 4);
    DEFINE_CGVAR_INT32(m_valsoffset, 5);
    DEFINE_CGVAR_INT32(ecxt_tupmem_offset, 5);
    DEFINE_CGVAR_INT64(Datum_0, 0);
    DEFINE_CGVAR_INT64(Datum_1, 1);

    llvm::BasicBlock* bb_next = NULL;
    llvm::BasicBlock** bb_list = NULL;
    llvm::PHINode* Phi_idx = NULL;
    llvm::Value* econtext = NULL;
    llvm::Value* pBatch = NULL;
    llvm::Value* isNull = NULL;
    llvm::Value* res = NULL;
    llvm::Value* val = NULL;
    llvm::Value* val1 = NULL;
    llvm::Value* ecxt_batch = NULL;
    llvm::Value* nsize = NULL;
    llvm::Value* idx_next = NULL;
    llvm::Value* loop_index = NULL;
    llvm::Value* argArr = NULL;
    llvm::Value* argVec = NULL;
    llvm::Value* argVal = NULL;
    llvm::Value* argFlag = NULL;
    llvm::Value* curr_Context = NULL;
    llvm::Value* cg_oldContext = NULL;
    llvm::Value* Vals[] = {Datum_0, int32_0};
    llvm::Value* llvmargs[3];
    llvm::Value* exprargs[3];
    llvm::Function* jitted_vectarget = NULL;

    /* Define IR pointer function */
    GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "JittedVecTargetList", boolType);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("econtext", ExprContextPtrType));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("pBatch", VectorBatchPtrType));
    jitted_vectarget = fn_prototype.generatePrototype(&builder, &llvmargs[0]);

    /* Get the actual context and pBatch from C Function */
    econtext = llvmargs[0];
    pBatch = llvmargs[1];
    isNull = builder.CreateAlloca(int8Type);

    /*
     * Get number of rows of this vectorbatch : since different node get their
     * data source from different sub-tree, we should get the actual batch
     * info. from econtext.
     */
    ecxt_batch = WrapGetVectorBatchCodeGen(&builder, econtext);

    /* Get the m_rows from the batch */
    Vals[0] = Datum_0;
    Vals[1] = int32_0;
    val = builder.CreateInBoundsGEP(ecxt_batch, Vals);
    nsize = builder.CreateLoad(int32Type, val, "m_rows");

    /* Restore number of rows to pBatch->m_rows */
    llvm::Value* mrows = builder.CreateInBoundsGEP(pBatch, Vals);
    builder.CreateStore(nsize, mrows);

    /* Run in short-lived per-tuple context while computing expressions */
    Vals[1] = ecxt_tupmem_offset;
    curr_Context = builder.CreateInBoundsGEP(econtext, Vals);
    curr_Context = builder.CreateLoad(MemContextPtrType, curr_Context, "per_tuple_memory");
    cg_oldContext = MemCxtSwitToCodeGen(&builder, curr_Context);

    /* Define the Basic Block structure */
    llvm::BasicBlock* entry = &jitted_vectarget->getEntryBlock();
    DEFINE_BLOCK(for_body, jitted_vectarget);
    DEFINE_BLOCK(bb_end, jitted_vectarget);
    DEFINE_BLOCK(for_end, jitted_vectarget);
    DEFINE_BLOCK(ret_bb, jitted_vectarget);

    /* Start the main loop */
    builder.SetInsertPoint(entry);
    val = builder.CreateICmpSGT(nsize, int32_0);
    builder.CreateCondBr(val, for_body, ret_bb);

    /* Start the codegen with respect to 'for(i = 0; i < nsize, i++)' */
    builder.SetInsertPoint(for_body);
    Phi_idx = builder.CreatePHI(int64Type, 2);

    idx_next = builder.CreateAdd(Phi_idx, Datum_1);
    Phi_idx->addIncoming(Datum_0, entry);
    Phi_idx->addIncoming(idx_next, bb_end);

    /*
     * For each time, we deal with one tuple. Phi_idx should be passed
     * as 'loop index' in 'for' structure.
     */
    loop_index = (llvm::Value*)Phi_idx;

    int target_len = list_length(targetlist);
    int tl_index = 0;
    ListCell* cell = NULL;

    if (target_len > 1) {
        bb_list = (llvm::BasicBlock**)palloc(sizeof(llvm::BasicBlock*) * target_len);
        bb_list[0] = for_body;
    }

    ExprCodeGenArgs args;
    args.parent = parent;
    args.builder = &builder;
    exprargs[0] = econtext;
    exprargs[1] = isNull;
    exprargs[2] = loop_index;
    args.llvm_args = &exprargs[0];

    /* First get the pointer to pBatch->m_arr */
    Vals[1] = m_arroffset;
    argArr = builder.CreateInBoundsGEP(pBatch, Vals);
    argArr = builder.CreateLoad(ScalarVectorPtrType, argArr, "m_arr");

    foreach (cell, targetlist) {
        GenericExprState* gstate = (GenericExprState*)lfirst(cell);
        ExprState* tstate = gstate->arg;
        TargetEntry* tle = (TargetEntry*)gstate->xprstate.expr;
        ;
        AttrNumber resind = tle->resno - 1;

        args.exprstate = tstate;
        res = CodeGen(&args);
        if (res == NULL) {
            jitted_vectarget->eraseFromParent();
            ereport(ERROR,
                (errcode(ERRCODE_CODEGEN_ERROR),
                    errmodule(MOD_LLVM),
                    errmsg("Codegen failed on the procedure of ExecVecTargetList!")));
        }

        /*
         * Restore the result to pBatch->m_arr[resind]->m_vals[Phi_idx]
         */
        DEFINE_CGVAR_INT32(colind, resind);
        Vals[0] = colind;
        Vals[1] = m_valsoffset;
        argVec = builder.CreateInBoundsGEP(argArr, Vals);
        argVec = builder.CreateLoad(int64PtrType, argVec, "m_vals");

        argVal = builder.CreateInBoundsGEP(argVec, loop_index);
        builder.CreateStore(res, argVal);

        /* Restore the flag to pBatch->m_arr[resind]->m_flag[Phi_idx] */
        Vals[1] = m_flagoffset;
        argFlag = builder.CreateInBoundsGEP(argArr, Vals);
        argFlag = builder.CreateLoad(int8PtrType, argFlag, "m_flag");

        llvm::Value* tmpFlag = builder.CreateInBoundsGEP(argFlag, loop_index);
        llvm::Value* tmpNull = isNull;
        tmpNull = builder.CreateLoad(int8Type, tmpNull);
        builder.CreateStore(tmpNull, tmpFlag);

        /*
         * Since in LLVM we deal with one block at a time, we should check
         * if there exists more predicates.
         */
        if (cell->next == NULL) {
            /* no more predicates, finish this block */
            builder.CreateBr(bb_end);
        } else {
            bb_next = llvm::BasicBlock::Create(context, "bb", jitted_vectarget);
            builder.CreateBr(bb_next);
            builder.SetInsertPoint(bb_next);
            bb_list[++tl_index] = bb_next;
        }
    }

    /* When all the codegen for the qual lists is done, finish the loop*/
    builder.SetInsertPoint(bb_end);

    /* Make sure Phi_idx is always less than nsize */
    llvm::Value* vtmp = builder.CreateTrunc(idx_next, int32Type);
    vtmp = builder.CreateICmpEQ(vtmp, nsize);
    builder.CreateCondBr(vtmp, for_end, for_body);

    builder.SetInsertPoint(for_end);
    foreach (cell, targetlist) {
        GenericExprState* gstate = (GenericExprState*)lfirst(cell);
        TargetEntry* tle = (TargetEntry*)gstate->xprstate.expr;
        ;
        AttrNumber resind = tle->resno - 1;

        DEFINE_CGVAR_INT32(colind, resind);
        Vals[0] = colind;
        Vals[1] = int32_0;
        val1 = builder.CreateInBoundsGEP(argArr, Vals);
        builder.CreateStore(nsize, val1);
    }
    builder.CreateBr(ret_bb);
    builder.SetInsertPoint(ret_bb);
    /* return back to the old memory context */
    (void)MemCxtSwitToCodeGen(&builder, cg_oldContext);
    builder.CreateRet(int8_1);

    if (target_len > 1) {
        pfree_ext(bb_list);
    }

    int plan_node_id = parent->plan->plan_node_id;
    llvmCodeGen->FinalizeFunction(jitted_vectarget, plan_node_id);

    /* Dump out C-function calls in codegen */
    llvmCodeGen->dumpCFunctionCalls("Targetlist", plan_node_id);

    /*
     * If coden_strategy is CODEGEN_PURE and having C-function call in codegen,
     * we need to discard the generated IR function.
     */
    if (llvmCodeGen->hasCFunctionCalls()) {
        llvmCodeGen->clearCFunctionCalls();

        if (u_sess->attr.attr_sql.codegen_strategy == CODEGEN_PURE)
            return NULL;
    }

    return jitted_vectarget;
}

/**
 * Function pointer to point to the actual LLVM assemble function.
 */
llvm::Value* VecExprCodeGen::CodeGen(ExprCodeGenArgs* args)
{
    return (*(VectorExprCodeGen)(args->exprstate->exprCodeGen))(args);
}

llvm::Value* VecExprCodeGen::WrapChooExtFunCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, Oid argtyp, llvm::Value* argval)
{
    llvm::Value* res = NULL;
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    DEFINE_CG_TYPE(int64Type, INT8OID);

    /* first convert the datum to *datum */
    llvm::Value* intmp = ptrbuilder->CreateAlloca(int64Type);
    ptrbuilder->CreateStore(argval, intmp);

    if (COL_IS_ENCODE(argtyp)) {
        switch (argtyp) {
            case MACADDROID:
            case TIMETZOID:
            case ARRAYTIMETZOID:
            case TINTERVALOID:
            case INTERVALOID:
            case ARRAYINTERVALOID:
            case ARRAYTINTERVALOID:
            case NAMEOID:
                res = WrapExtFixedTypeCodeGen(ptrbuilder, intmp);
                break;
            case UNKNOWNOID:
            case CSTRINGOID:
                res = WrapExtCStrTypeCodeGen(ptrbuilder, intmp);
                break;
            default:
                res = WrapExtVarTypeCodeGen(ptrbuilder, intmp);
                break;
        }
    } else if (argtyp == TIDOID) {
        res = WrapExtAddrTypeCodeGen(ptrbuilder, intmp);
    } else {
        res = WrapExtVarTypeCodeGen(ptrbuilder, intmp);
    }

    return res;
}

llvm::Value* VecExprCodeGen::WrapExtCStrTypeCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* data)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int64PtrType, INT8OID);
    llvm::Value* result = NULL;

    llvm::Function* jitted_wrapctcg = llvmCodeGen->module()->getFunction("LLVMWrapExtCStrTyp");
    if (jitted_wrapctcg == NULL) {
        GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMWrapExtCStrTyp", int64Type);
        fn_prototype.addArgument(GsCodeGen::NamedVariable("data", int64PtrType));
        jitted_wrapctcg = fn_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMWrapExtCStrTyp", (void*)ExtractCstringType);
    }

    llvmCodeGen->FinalizeFunction(jitted_wrapctcg);
    result = ptrbuilder->CreateCall(jitted_wrapctcg, data);
    return result;
}

llvm::Value* VecExprCodeGen::WrapExtVarTypeCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* data)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int64PtrType, INT8OID);
    llvm::Value* result = NULL;

    llvm::Function* jitted_wrapvarcg = llvmCodeGen->module()->getFunction("LLVMWrapExtVarTyp");
    if (jitted_wrapvarcg == NULL) {
        GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMWrapExtVarTyp", int64Type);
        fn_prototype.addArgument(GsCodeGen::NamedVariable("data", int64PtrType));
        jitted_wrapvarcg = fn_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMWrapExtVarTyp", (void*)ExtractVarType);
    }

    llvmCodeGen->FinalizeFunction(jitted_wrapvarcg);
    result = ptrbuilder->CreateCall(jitted_wrapvarcg, data);
    return result;
}

llvm::Value* VecExprCodeGen::WrapExtFixedTypeCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* data)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int64PtrType, INT8OID);
    llvm::Value* result = NULL;

    llvm::Function* jitted_wrapftcg = llvmCodeGen->module()->getFunction("LLVMWrapExtFixedTyp");
    if (jitted_wrapftcg == NULL) {
        GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMWrapExtFixedTyp", int64Type);
        fn_prototype.addArgument(GsCodeGen::NamedVariable("data", int64PtrType));
        jitted_wrapftcg = fn_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMWrapExtFixedTyp", (void*)ExtractFixedType);
    }

    llvmCodeGen->FinalizeFunction(jitted_wrapftcg);
    result = ptrbuilder->CreateCall(jitted_wrapftcg, data);
    return result;
}

llvm::Value* VecExprCodeGen::WrapExtAddrTypeCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* data)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    llvm::LLVMContext& context = llvmCodeGen->context();
    GsCodeGen::LlvmBuilder builder(context);

    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int64PtrType, INT8OID);
    llvm::Value* result = NULL;

    llvm::Function* jitted_wrapatcg = llvmCodeGen->module()->getFunction("LLVMWrapExtAddrTyp");
    if (jitted_wrapatcg == NULL) {
        GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMWrapExtAddrTyp", int64Type);
        fn_prototype.addArgument(GsCodeGen::NamedVariable("data", int64PtrType));
        jitted_wrapatcg = fn_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMWrapExtAddrTyp", (void*)ExtractAddrType);
    }

    llvmCodeGen->FinalizeFunction(jitted_wrapatcg);
    result = ptrbuilder->CreateCall(jitted_wrapatcg, data);
    return result;
}

llvm::Value* VecExprCodeGen::WrapDCStrToScalCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* val)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    DEFINE_CG_TYPE(int64Type, INT8OID);

    llvm::Value* result = NULL;

    llvm::Function* jitted_wrapdcsts = llvmCodeGen->module()->getFunction("LLVMWrapDatumCStrToScal");
    if (jitted_wrapdcsts == NULL) {
        GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMWrapDatumCStrToScal", int64Type);
        fn_prototype.addArgument(GsCodeGen::NamedVariable("val", int64Type));
        jitted_wrapdcsts = fn_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMWrapDatumCStrToScal", (void*)WrapDatumCstringToScalar);
    }

    llvmCodeGen->FinalizeFunction(jitted_wrapdcsts);

    result = ptrbuilder->CreateCall(jitted_wrapdcsts, val);
    return result;
}

llvm::Value* VecExprCodeGen::WrapDFixLToScalCodeGen(
    GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* val, llvm::Value* len)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_TYPE(int32Type, INT4OID);

    llvm::Value* result = NULL;

    llvm::Function* jitted_wrapdflts = llvmCodeGen->module()->getFunction("LLVMWrapDatumFixLToScal");
    if (jitted_wrapdflts == NULL) {
        GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMWrapDatumFixLToScal", int64Type);
        fn_prototype.addArgument(GsCodeGen::NamedVariable("val", int64Type));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("len", int32Type));
        jitted_wrapdflts = fn_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMWrapDatumFixLToScal", (void*)WrapDatumFixLenToScalar);
    }

    llvmCodeGen->FinalizeFunction(jitted_wrapdflts);

    result = ptrbuilder->CreateCall(jitted_wrapdflts, {val, len});
    return result;
}

llvm::Value* VecExprCodeGen::WrapGetVectorBatchCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* econtext)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    DEFINE_CG_PTRTYPE(exprContextPtrType, "struct.ExprContext");
    DEFINE_CG_PTRTYPE(vectorBatchPtrType, "class.VectorBatch");

    llvm::Value* result = NULL;

    llvm::Function* jitted_wrapgetvb = llvmCodeGen->module()->getFunction("LLVMWrapGetVecBatch");
    if (jitted_wrapgetvb == NULL) {
        GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMWrapGetVecBatch", vectorBatchPtrType);
        fn_prototype.addArgument(GsCodeGen::NamedVariable("econtext", exprContextPtrType));
        jitted_wrapgetvb = fn_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMWrapGetVecBatch", (void*)WrapGetVectorBatch);
    }

    llvmCodeGen->FinalizeFunction(jitted_wrapgetvb);

    result = ptrbuilder->CreateCall(jitted_wrapgetvb, econtext);
    return result;
}

llvm::Value* VecExprCodeGen::WrapStrictOpFuncCodeGen(
    GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* fcinfo, llvm::Value* arg, llvm::Value* isNull)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int64PtrType, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, BOOLOID);
    DEFINE_CG_PTRTYPE(FuncCallInfoPtrType, "struct.FunctionCallInfoData");

    llvm::Value* result = NULL;

    llvm::Function* jitted_wrapsopexpr = llvmCodeGen->module()->getFunction("LLVMWrapStrictOpExprFunc");
    if (jitted_wrapsopexpr == NULL) {
        GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMWrapStrictOpExprFunc", int64Type);
        fn_prototype.addArgument(GsCodeGen::NamedVariable("fcinfo", FuncCallInfoPtrType));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("arg", int64PtrType));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("isNull", int8PtrType));
        jitted_wrapsopexpr = fn_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMWrapStrictOpExprFunc", (void*)WrapVecStrictOperFunc);
    }

    llvmCodeGen->FinalizeFunction(jitted_wrapsopexpr);

    result = ptrbuilder->CreateCall(jitted_wrapsopexpr, {fcinfo, arg, isNull});
    return result;
}

llvm::Value* VecExprCodeGen::WrapNonStrictOpFuncCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* fcinfo,
    llvm::Value* arg, llvm::Value* argnull, llvm::Value* isNull)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CG_PTRTYPE(int64PtrType, INT8OID);
    DEFINE_CG_PTRTYPE(int8PtrType, BOOLOID);
    DEFINE_CG_PTRTYPE(FuncCallInfoPtrType, "struct.FunctionCallInfoData");

    llvm::Value* result = NULL;

    llvm::Function* jitted_wrapnsopexpr = llvmCodeGen->module()->getFunction("LLVMWrapNStrictOpExprFunc");
    if (jitted_wrapnsopexpr == NULL) {
        GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMWrapNStrictOpExprFunc", int64Type);
        fn_prototype.addArgument(GsCodeGen::NamedVariable("fcinfo", FuncCallInfoPtrType));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("arg", int64PtrType));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("argnull", int8PtrType));
        fn_prototype.addArgument(GsCodeGen::NamedVariable("isNull", int8PtrType));
        jitted_wrapnsopexpr = fn_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMWrapNStrictOpExprFunc", (void*)WrapVecNonStrictOperFunc);
    }

    llvmCodeGen->FinalizeFunction(jitted_wrapnsopexpr);

    result = ptrbuilder->CreateCall(jitted_wrapnsopexpr, {fcinfo, arg, argnull, isNull});
    return result;
}

llvm::Value* VecExprCodeGen::MemCxtSwitToCodeGen(GsCodeGen::LlvmBuilder* ptrbuilder, llvm::Value* context)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    DEFINE_CG_PTRTYPE(MemCxtDataType, "struct.MemoryContextData");

    llvm::Value* result = NULL;
    llvm::Function* jitted_wrapmcst = llvmCodeGen->module()->getFunction("LLVMWrapMemCxtSwitTo");
    if (jitted_wrapmcst == NULL) {
        GsCodeGen::FnPrototype fn_prototype(llvmCodeGen, "LLVMWrapMemCxtSwitTo", MemCxtDataType);
        fn_prototype.addArgument(GsCodeGen::NamedVariable("context", MemCxtDataType));
        jitted_wrapmcst = fn_prototype.generatePrototype(NULL, NULL);
        llvm::sys::DynamicLibrary::AddSymbol("LLVMWrapMemCxtSwitTo", (void*)WrapMemoryContextSwitchTo);
    }

    llvmCodeGen->FinalizeFunction(jitted_wrapmcst);

    result = ptrbuilder->CreateCall(jitted_wrapmcst, context);
    return result;
}

llvm::Value* VecExprCodeGen::EvalFuncResultCodeGen(
    GsCodeGen::LlvmBuilder* ptrbuilder, FuncExprState* fcache, llvm::Value* isNull, LLVMFuncCallInfo* lfcinfo)
{
    GsCodeGen* llvmCodeGen = (GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;

    /* Get the funcoid and collationid according to different expr type */
    Oid funcid = 0;
    Oid Collation = 0;
    Oid datumType = 0;
    ExprState* cestate = (ExprState*)fcache;
    Expr* node = cestate->expr;
    int num_args = list_length(fcache->args);
    Assert(num_args == lfcinfo->nargs);
    switch (nodeTag(node)) {
        case T_OpExpr: {
            OpExpr* opexpr = (OpExpr*)node;
            funcid = opexpr->opfuncid;
            Collation = opexpr->inputcollid;
        } break;
        case T_FuncExpr: {
            FuncExpr* funcexpr = (FuncExpr*)node;
            funcid = funcexpr->funcid;
            Collation = funcexpr->inputcollid;
        } break;
        case T_ScalarArrayOpExpr: {
            ScalarArrayOpExpr* saop = (ScalarArrayOpExpr*)node;
            funcid = saop->opfuncid;
            Collation = saop->inputcollid;
            break;
        }
        case T_NullIfExpr: {
            NullIfExpr* nullifexpr = (NullIfExpr*)node;
            funcid = nullifexpr->opfuncid;
            Collation = nullifexpr->inputcollid;
        } break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmodule(MOD_LLVM),
                    errmsg("Not supported expr node %d yet!", nodeTag(node))));
            break;
    }

    llvm::Value* res0 = NULL;
    llvm::Value* res = NULL;
    DEFINE_CG_TYPE(int8Type, BOOLOID);
    DEFINE_CG_TYPE(int64Type, INT8OID);
    DEFINE_CGVAR_INT64(Datum_0, 0);
    DEFINE_CGVAR_INT64(Datum_1, 1);
    DEFINE_CGVAR_INT64(Datum_2, 2);
    DEFINE_CG_PTRTYPE(FuncCallInfoDataPtrType, "struct.FunctionCallInfoData");

    /*
     * Fill the most elements of FunctionCallInfoData as we do in ExecEvalVecOper
     * and ExecEvalVecFunc. For the args in FunctionCallInfo, we should fill
     * these values during actual execution.
     */
    FunctionCallInfo fcinfo;
    initVectorFcache(funcid, Collation, fcache, CurrentMemoryContext);
    fcinfo = &fcache->fcinfo_data;
    Assert(fcinfo->flinfo->vec_fn_addr == NULL);

    /* Records C-function calls in codegen */
    const FmgrBuiltin* fbp = fmgr_isbuiltin(fcinfo->flinfo->fn_oid);
    char* name = NULL;

    /* If we can get function name, record it, else record function oid */
    if (fbp != NULL) {
        name = const_cast<char*>(fbp->funcName);
    } else {
        name = (char*)palloc0(MAX_OID_LENGTH);
        /* the second arg of sprintf_s should be the true length of buffer name */
        int rc = sprintf_s(name, MAX_OID_LENGTH, "%u", fcinfo->flinfo->fn_oid);
        /* check the return value of security function */
        securec_check_ss(rc, "\0", "\0");
    }

    llvmCodeGen->recordCFunctionCalls(name);

    /*
     * In the following cases, we have to make it possible to re-enter DispatchVectorFunction:
     *	(1) codegen_strategy is CODEGEN_PURE, then this expr will be execute in vector engine;
     *	(2) this expr is the innHashKey of a hashClause, then it will be execute in vector engine.
     * we have not discovery other case that need to re-enter DispatchVectorFunction until now.
     */
    if (u_sess->attr.attr_sql.codegen_strategy == CODEGEN_PURE || IsA(node, NullIfExpr)) {
        fcache->func.fn_oid = InvalidOid;
    }

    if (fcinfo->flinfo->fn_rettype == NUMERICOID) {
        if (u_sess->attr.attr_sql.enable_fast_numeric) {
            bool found = false;
            VecFuncCacheEntry* entry =
                (VecFuncCacheEntry*)hash_search(g_instance.vec_func_hash, &fcinfo->flinfo->fn_oid, HASH_FIND, &found);

            /* if found, replace it */
            if (found && entry->vec_transform_function[0] != NULL)
                fcinfo->flinfo->fn_addr = entry->vec_transform_function[0];
            /* not found, report this log */
            else {
                const FmgrBuiltin* fmgrbp = fmgr_isbuiltin(fcinfo->flinfo->fn_oid);
                /* UnSupported vector function */
                if (fmgrbp != NULL)
                    ereport(DEBUG1,
                        (errmodule(MOD_LLVM), errmsg("non-optimized big integer function: %s", fmgrbp->funcName)));
                else
                    ereport(DEBUG1,
                        (errmodule(MOD_LLVM),
                            errmsg("non-optimized big integer function :%u", fcinfo->flinfo->fn_oid)));
            }
        }
    }

    llvm::Value* cg_fcinfo = llvmCodeGen->CastPtrToLlvmPtr(FuncCallInfoDataPtrType, fcinfo);

    /*
     * Allocate an array according to the actual number of arguments
     * of Func. And we need to restore these values(llvm former) to
     * this array. We alloc one more element to record the flag of the
     * result.
     */
    DEFINE_CGVAR_INT32(nargs, num_args);
    llvm::Value* inargs = ptrbuilder->CreateAlloca(int64Type, nargs);
    llvm::Value* inargflags = ptrbuilder->CreateAlloca(int8Type, nargs);

    /* Dispatch vector function and get the argument types */
    ListCell* cell = NULL;
    int i = 0;
    Oid argtyp[MAX_ARGS_NUM] = {0};
    foreach (cell, fcache->args) {
        ExprState* argstate = (ExprState*)lfirst(cell);
        Oid funcrettype;
        TupleDesc tupdesc;

        (void)get_expr_result_type((Node*)argstate->expr, &funcrettype, &tupdesc);

        argtyp[i] = funcrettype;
        i++;
    }

    /* We should consider both OpExpr and FuncExpr */
    bool is_func_strict = fcache->func.fn_strict;
    if (num_args > 0) {
        llvm::Value* lhs_value = lfcinfo->args[0];
        llvm::Value* arg1 = ptrbuilder->CreateInBoundsGEP(inargs, Datum_0);
        lhs_value = WrapChooExtFunCodeGen(ptrbuilder, argtyp[0], lhs_value);
        ptrbuilder->CreateStore(lhs_value, arg1);

        if (!is_func_strict) {
            llvm::Value* lhs_flag = lfcinfo->argnulls[0];
            llvm::Value* argflag1 = ptrbuilder->CreateInBoundsGEP(inargflags, Datum_0);
            ptrbuilder->CreateStore(lhs_flag, argflag1);
        }
    }

    if (num_args > 1) {
        llvm::Value* rhs_value = lfcinfo->args[1];
        llvm::Value* arg2 = ptrbuilder->CreateInBoundsGEP(inargs, Datum_1);
        rhs_value = WrapChooExtFunCodeGen(ptrbuilder, argtyp[1], rhs_value);
        ptrbuilder->CreateStore(rhs_value, arg2);

        if (!is_func_strict) {
            llvm::Value* rhs_flag = lfcinfo->argnulls[1];
            llvm::Value* argflag2 = ptrbuilder->CreateInBoundsGEP(inargflags, Datum_1);
            ptrbuilder->CreateStore(rhs_flag, argflag2);
        }
    }

    if (num_args > 2) {
        llvm::Value* thd_value = lfcinfo->args[2];
        llvm::Value* arg3 = ptrbuilder->CreateInBoundsGEP(inargs, Datum_2);
        thd_value = WrapChooExtFunCodeGen(ptrbuilder, argtyp[2], thd_value);
        ptrbuilder->CreateStore(thd_value, arg3);

        if (!is_func_strict) {
            llvm::Value* thd_flag = lfcinfo->argnulls[2];
            llvm::Value* argflag3 = ptrbuilder->CreateInBoundsGEP(inargflags, Datum_2);
            ptrbuilder->CreateStore(thd_flag, argflag3);
        }
    }

    /*
     * call orginal c-function
     *
     * For most of the functions, if the input arguments are not NULL,
     * the returned value will not be NULL. While for some functions,
     * like text_substr_null, replace_text, the returned value could not
     * be NULL again. So we should record the null flag according to the
     * actual result.
     */
    if (is_func_strict) {
        res0 = WrapStrictOpFuncCodeGen(ptrbuilder, cg_fcinfo, inargs, isNull);
    } else {
        res0 = WrapNonStrictOpFuncCodeGen(ptrbuilder, cg_fcinfo, inargs, inargflags, isNull);
    }

    /*
     * get the result according to the return type not just the result type
     * of expressions.
     */
    datumType = fcinfo->flinfo->fn_rettype;
    if (COL_IS_ENCODE(datumType)) {
        switch (datumType) {
            case MACADDROID: {
                DEFINE_CGVAR_INT32(arglen, 6);
                res = WrapDFixLToScalCodeGen(ptrbuilder, res0, arglen);
            } break;
            case TIMETZOID:
            case TINTERVALOID: {
                DEFINE_CGVAR_INT32(arglen, 12);
                res = WrapDFixLToScalCodeGen(ptrbuilder, res0, arglen);
            } break;
            case INTERVALOID: {
                DEFINE_CGVAR_INT32(arglen, 16);
                res = WrapDFixLToScalCodeGen(ptrbuilder, res0, arglen);
            } break;
            case NAMEOID: {
                DEFINE_CGVAR_INT32(arglen, 64);
                res = WrapDFixLToScalCodeGen(ptrbuilder, res0, arglen);
            } break;
            case UNKNOWNOID:
            case CSTRINGOID: {
                res = WrapDCStrToScalCodeGen(ptrbuilder, res0);
            } break;
            default: {
                res = res0;
            } break;
        }
    } else {
        res = res0;
    }

    return res;
}

/*
 * @Description	: convert the cstring data type to scalar value.
 */
Datum WrapDatumCstringToScalar(Datum data)
{
    /* evaluate the length of the string */
    Size len = strlen((char*)data);

    /* call the to-scalar function defined in ScalarVector */
    Datum val = ScalarVector::DatumCstringToScalar(data, len);
    return val;
}

/*
 * @Description	: convert the fix-len data type to scalar value.
 */
Datum WrapDatumFixLenToScalar(Datum data, Size len)
{
    Datum val;

    /* call the to-scalar function defined in ScalarVector */
    val = ScalarVector::DatumFixLenToScalar(data, len);
    return val;
}

/*
 * @Description	: Get the result of the particular function.
 * @in fcinfo	: FunctionCallInfo data with respect to funcid.
 * @in arg		: arguments needed by func and the flag of the result.
 * @return		: the result of the function defined in (fcinfo)->flinfo->fn_addr
 */
Datum WrapVecStrictOperFunc(FunctionCallInfo fcinfo, Datum* arg, bool* isNull)
{
    Datum result = 0;

    /*
     * get the arguments during execution and invoke the pre-defined function
     */
    int nargs = fcinfo->nargs;
    for (int i = 0; i < nargs; i++) {
        fcinfo->arg[i] = arg[i];
    }
    /*
     * we should reset then isnull flag here since the pre-defined function use it directly
     */
    fcinfo->isnull = false;
    result = FunctionCallInvoke(fcinfo);
    *isNull = fcinfo->isnull;

    return result;
}

/*
 * @Description	: Get the result of the particular function.
 * @in fcinfo	: FunctionCallInfo data with respect to funcid.
 * @in arg		: arguments needed by func and the flag of the result.
 * @return		: the result of the function defined in (fcinfo)->flinfo->fn_addr
 * @Notes		: Only consider functions with non-strict attribute.
 */
Datum WrapVecNonStrictOperFunc(FunctionCallInfo fcinfo, Datum* arg, const bool* argnull, bool* isNull)
{
    Datum result = 0;

    /*
     * get the arguments during execution and invoke the pre-defined function
     */
    int nargs = fcinfo->nargs;
    for (int i = 0; i < nargs; i++) {
        fcinfo->arg[i] = arg[i];
        fcinfo->argnull[i] = argnull[i];
    }

    fcinfo->isnull = false;
    result = FunctionCallInvoke(fcinfo);
    *isNull = fcinfo->isnull;

    return result;
}

/*
 * @Description : Get the vectorbatch needed during projection. To Scan Operator,
 *				  we use scanbatch, while to Join Operator, we use innberbatch
 *				  and outerbatch.
 * @in econtext	: ExprContext of current Expr.
 * @return		: scanbatch needed during eval targetlist.
 */
VectorBatch* WrapGetVectorBatch(ExprContext* econtext)
{
    VectorBatch* resBatch = NULL;

    if (econtext->ecxt_scanbatch)
        resBatch = econtext->ecxt_scanbatch;
    else if (econtext->ecxt_innerbatch)
        resBatch = econtext->ecxt_innerbatch;
    else if (econtext->ecxt_outerbatch)
        resBatch = econtext->ecxt_outerbatch;
    else
        ereport(ERROR,
            (errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                errmodule(MOD_LLVM),
                errmsg("Unexpected batch information from ExprContext.")));

    return resBatch;
}

/*
 * @Description : Switch current context memory to 'context' Context
 * @in econtext	: Current memory context.
 * @return		: Old memory context.
 */
MemoryContext WrapMemoryContextSwitchTo(MemoryContext context)
{
    MemoryContext old = CurrentMemoryContext;

    CurrentMemoryContext = context;
    return old;
}
}  // namespace dorado
