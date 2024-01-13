

#include "postgres.h"
#include "knl/knl_variable.h"

#include "catalog/pg_enum.h"
#include "catalog/pg_type.h"
#include "catalog/pg_proc.h"
#include "catalog/gs_package.h"
#include "catalog/gs_collation.h"
#include "commands/dbcommands.h"
#include "commands/sequence.h"
#include "db4ai/predict_by.h"
#include "executor/node/nodeCtescan.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/var.h"
#include "optimizer/planner.h"
#include "parser/analyze.h"
#include "parser/parser.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_func.h"
#include "parser/parse_oper.h"
#include "parser/parse_relation.h"
#include "parser/parse_target.h"
#include "parser/parse_type.h"
#include "parser/parse_agg.h"
#include "parser/parse_utilcmd.h"
#include "rewrite/rewriteManip.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/plpgsql.h"
#include "utils/xml.h"
#include "funcapi.h"
#include "utils/guc.h"
#include "utils/guc_tables.h"
#include "utils/varbit.h"


// 构建列的默认值
extern Node* build_column_default(Relation rel, int attrno, bool isInsertCmd = false, bool needOnUpdate = false);

// 创建一个常量值
extern Node* makeAConst(Value* v, int location);

// 创建一个字符串值
extern Value* makeStringValue(char* str);

// 转换参数引用
static Node* transformParamRef(ParseState* pstate, ParamRef* pref);

// 转换A表达式操作
static Node* transformAExprOp(ParseState* pstate, A_Expr* a);

// 转换A表达式与
static Node* transformAExprAnd(ParseState* pstate, A_Expr* a);

// 转换A表达式或
static Node* transformAExprOr(ParseState* pstate, A_Expr* a);

// 转换A表达式非
static Node* transformAExprNot(ParseState* pstate, A_Expr* a);

// 转换A表达式操作任意
static Node* transformAExprOpAny(ParseState* pstate, A_Expr* a);

// 转换A表达式操作全部
static Node* transformAExprOpAll(ParseState* pstate, A_Expr* a);

// 转换A表达式不重复
static Node* transformAExprDistinct(ParseState* pstate, A_Expr* a);

// 转换A表达式为空
static Node* transformAExprNullIf(ParseState* pstate, A_Expr* a);

// 转换A表达式
static Node* transformAExprOf(ParseState* pstate, A_Expr* a);

// 转换A表达式
static Node* transformAExprIn(ParseState* pstate, A_Expr* a);

// 转换用户集元素
static Node* transformUserSetElem(ParseState* pstate, UserSetElem *elem);

// 转换用户变量
static Node* transformUserVar(UserVar *uservar);

// 转换函数调用
static Node* transformFuncCall(ParseState* pstate, FuncCall* fn);

// 转换CASE表达式
static Node* transformCaseExpr(ParseState* pstate, CaseExpr* c);

// 转换子链接
static Node* transformSubLink(ParseState* pstate, SubLink* sublink);

// 转换SELECT INTO变量列表
static Node* transformSelectIntoVarList(ParseState* pstate, SelectIntoVarList* sis);

// 转换数组表达式
static Node* transformArrayExpr(ParseState* pstate, A_ArrayExpr* a, Oid array_type, Oid element_type, int32 typmod);

// 转换行表达式
static Node* transformRowExpr(ParseState* pstate, RowExpr* r);

// 转换合并表达式
static Node* transformCoalesceExpr(ParseState* pstate, CoalesceExpr* c);

// 转换最小最大表达式
static Node* transformMinMaxExpr(ParseState* pstate, MinMaxExpr* m);

// 转换XML表达式
static Node* transformXmlExpr(ParseState* pstate, XmlExpr* x);

// 转换XML序列化
static Node* transformXmlSerialize(ParseState* pstate, XmlSerialize* xs);

// 转换布尔测试
static Node* transformBooleanTest(ParseState* pstate, BooleanTest* b);

// 转换当前表达式
static Node* transformCurrentOfExpr(ParseState* pstate, CurrentOfExpr* cexpr);

// 转换预测函数
static Node* transformPredictByFunction(ParseState* pstate, PredictByFunction* cexpr);

// 转换整行引用
static Node* transformWholeRowRef(ParseState* pstate, RangeTblEntry* rte, int location);

// 转换间接引用
static Node* transformIndirection(ParseState* pstate, A_Indirection* ind);

// 转换类型转换
static Node* transformTypeCast(ParseState* pstate, TypeCast* tc);

// 转换字符集子句
static Node* transformCharsetClause(ParseState* pstate, CharsetClause* c);

// 转换排序子句
static Node* transformCollateClause(ParseState* pstate, CollateClause* c);

// 转换行比较操作
static Node* make_row_comparison_op(ParseState* pstate, List* opname, List* largs, List* rargs, int location);

// 转换行不重复操作
static Node* make_row_distinct_op(ParseState* pstate, List* opname, RowExpr* lrow, RowExpr* rrow, int location);

// 转换常量值
static Node* convertStarToCRef(RangeTblEntry* rte, char* catname, char* nspname, char* relname, int location);

// 检查序列函数调用
static bool IsSequenceFuncCall(Node* filed1, Node* filed2, Node* filed3);

// 转换序列函数调用
static Node* transformSequenceFuncCall(ParseState* pstate, Node* field1, Node* field2, Node* field3, int location);

// 转换连接根函数调用
static Node* transformConnectByRootFuncCall(ParseState* pstate, Node* funcNameVal, ColumnRef *cref);

// 检查是否被中断的RTE
static bool CheckSwAbortedRTE(ParseState *pstate, char *relname);

// 查找列引用的关系名称
static char *ColumnRefFindRelname(ParseState *pstate, const char *colname);

// 转换开始为列引用
static Node *transformStartWithColumnRef(ParseState *pstate, ColumnRef *cref, char **colname);

// 尝试转换函数
static Node* tryTransformFunc(ParseState* pstate, List* fields, int location);

// 子检查出参
static void SubCheckOutParam(List* exprtargs, Oid funcid);

// 转换前缀键
static Node* transformPrefixKey(ParseState* pstate, PrefixKey* pkey);

// 判断关系表的定向
#define OrientedIsCOLorPAX(rte) ((rte)->orientation == REL_COL_ORIENTED || (rte)->orientation == REL_PAX_ORIENTED)
#define INDEX_KEY_MAX_PREFIX_LENGTH (int)2676


static inline bool IsAutoIncrementColumn(TupleDesc rdAtt, int attrNo) 
{
    
    return rdAtt->constr && rdAtt->constr->cons_autoinc && rdAtt->constr->cons_autoinc->attnum == attrNo;
}

static bool IsBaseRightRefSupportType(Oid oid)
{
    // 检查给定的Oid是否是基本右引用支持类型。
    
    switch (oid) {
        case BOOLOID:
        case BYTEAOID:
        case CHAROID:
        case NAMEOID:
        case INT8OID:
        case INT2OID:
        case INT1OID:
        case INT4OID:
        case TEXTOID:
        case INT16OID:
        case RAWOID:
        case BLOBOID:
        case CLOBOID:
        case JSONOID:
        case XMLOID:
        case POINTOID:
        case LSEGOID:
        case PATHOID:
        case BOXOID:
        case POLYGONOID:
        case FLOAT4OID:
        case FLOAT8OID:
        case ABSTIMEOID:
        case RELTIMEOID:
        case TINTERVALOID:
        case CIRCLEOID:
        case CASHOID:
        case MACADDROID:
        case INETOID:
        case CIDROID:
        case BPCHAROID:
        case VARCHAROID:
        case NVARCHAR2OID:
        case DATEOID:
        case TIMEOID:
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
        case INTERVALOID:
        case TIMETZOID:
        case BITOID:
        case VARBITOID:
        case NUMERICOID:
        case UUIDOID:
        case TSVECTOROID:
        case TSQUERYOID:
        case JSONBOID:
        case INT4RANGEOID:
        case NUMRANGEOID:
        case TSRANGEOID:
        case TSTZRANGEOID:
        case DATERANGEOID:
        case INT8RANGEOID:
        case CSTRINGOID:
        case INTERNALOID:
        case SMALLDATETIMEOID:
        case HLL_OID:
        case HASH16OID:
        case HASH32OID:
            return true;
        default: {
            return false;
        }
    }
}

// 静态函数BuildColumnBaseValue，根据表属性tup，构建常量值
static Const* BuildColumnBaseValue(Form_pg_attribute attTup)
{
    // 如果表属性是支持类型，则返回该类型的常量值
    
    if (IsBaseRightRefSupportType(attTup->atttypid)) {
        
        Datum datum = GetTypeZeroValue(attTup);
        
        return makeConst(attTup->atttypid,
                         attTup->atttypmod,
                         attTup->attcollation,
                         attTup->attlen,
                         datum,
                         false, 
                         attTup->attbyval);
    
    // 如果表属性是枚举类型，则返回枚举类型中的一个值
    } else if (type_is_enum(attTup->atttypid)) {
        
        Relation enumRel = heap_open(EnumRelationId, AccessShareLock);
        
        CatCList* items = SearchSysCacheList1(ENUMTYPOIDNAME, ObjectIdGetDatum(attTup->atttypid));
        int itemCnt = items->n_members;
        
        
        for (int eindex = 0; eindex < itemCnt; ++eindex) {
            HeapTuple enumTup = t_thrd.lsc_cxt.FetchTupleFromCatCList(items, eindex);
            Form_pg_enum item = (Form_pg_enum)GETSTRUCT(enumTup);
            if (item && item->enumsortorder == 1) {
                
                Datum datum = DirectFunctionCall2(enum_in,
                                                  CStringGetDatum(pstrdup(NameStr(item->enumlabel))),
                                                  attTup->atttypid);
                ReleaseSysCacheList(items);
                heap_close(enumRel, AccessShareLock);
                
                return makeConst(attTup->atttypid,
                                 attTup->atttypmod,
                                 attTup->attcollation,
                                 attTup->attlen,
                                 datum,
                                 false, 
                                 attTup->attbyval);
            }
        }
        ReleaseSysCacheList(items);
        heap_close(enumRel, AccessShareLock);
    
    // 如果表属性是集合类型，则返回空字符串
    } else if (type_is_set(attTup->atttypid)) {
        return makeConst(attTup->atttypid,
                         attTup->atttypmod,
                         attTup->attcollation,
                         attTup->attlen,
                         CStringGetTextDatum(""), 
                         false, 
                         attTup->attbyval);
    }

    
    return nullptr;
}
// 静态函数AddDefaultExprNode，为插入语句添加默认表达式节点
static void AddDefaultExprNode(ParseState* pstate)
{
    
    RightRefState* refState = pstate->rightRefState;
    
    // 如果表属性有右引用，则不添加默认表达式节点
    if (refState->isInsertHasRightRef) {
        return;
    }
    
    pstate->rightRefState->isInsertHasRightRef = true;
    
    
    // 获取目标表
    Relation relation = (Relation)linitial(pstate->p_target_relation);
    
    // 获取目标表的属性描述符
    TupleDesc rdAtt = relation->rd_att;
    
    // 获取目标表的属性数
    int fieldCnt = rdAtt->natts;
    
    // 初始化常量值数组
    refState->constValues = (Const**)palloc0(sizeof(Const*) * fieldCnt);
    
    
    // 初始化表达式上下文
    eval_const_expressions_context context;
    context.boundParams = nullptr;
    context.root = nullptr;
    context.active_fns = NIL;
    context.case_val = NULL;
    context.estimate = false;

    
    // 遍历目标表的属性，构建表达式节点
    for (int i = 0; i < fieldCnt; ++i) {
        FormData_pg_attribute *attTup = &rdAtt->attrs[i];
        
        // 如果属性是自增列，则返回自增列的常量值
        if (IsAutoIncrementColumn(rdAtt, i + 1)) {
            refState->constValues[i] = makeConst(attTup->atttypid, -1, attTup->attcollation,
                      attTup->attlen, (Datum)0, false, attTup->attbyval);
        
        // 如果属性是生成列，则返回生成列的常量值
        } else if (ISGENERATEDCOL(rdAtt, i)) {
            refState->constValues[i] = nullptr;
        
        // 如果属性是普通列，则根据表属性tup构建常量值
        } else {
            
            // 根据表属性tup构建表达式节点
            Node* node = build_column_default(relation, i + 1, true);
            
            // 如果表达式节点是常量，则返回该常量
            if (node == nullptr) {
                refState->constValues[i] = nullptr;
            
            } else if (IsA(node, Const)) {
                refState->constValues[i] = (Const*)node;
            
            // 如果表达式节点是函数，则根据函数参数构建表达式节点
            } else if (IsA(node, FuncExpr)) {
                FuncExpr* expr = (FuncExpr*)node;
                List* args = expr->args;
                Expr* simple = simplify_function(expr->funcid, expr->funcresulttype, exprTypmod((const Node*)expr), 
                                                    expr->funccollid, expr->inputcollid, &args, true, false, &context);
                if (simple && IsA(simple, Const)) {
                    refState->constValues[i] = (Const*)simple;
                } else {
                    refState->constValues[i] = nullptr;
                }
            
            // 如果表达式节点是其他类型，则返回null
            } else {
                refState->constValues[i] = nullptr;
            }
        }

        
        
        // 如果常量值为null且属性tup的attnotnull为true，则根据属性tup构建常量值
        if (refState->constValues[i] == nullptr && attTup && attTup->attnotnull) {
            refState->constValues[i] = BuildColumnBaseValue(attTup);
        }
    }
}


Node* transformExpr(ParseState* pstate, Node* expr, ParseExprKind exprKind)
{
    Node *result;
    ParseExprKind sv_expr_kind;

    // 保存当前表达式类型，以便在递归调用时可以恢复它
    
    Assert(exprKind != EXPR_KIND_NONE);
    sv_expr_kind = pstate->p_expr_kind;
    pstate->p_expr_kind = exprKind;

    // 递归调用transformExpr，以处理子表达式
    
    result = transformExprRecurse(pstate, expr);

    // 恢复当前表达式类型
    
    pstate->p_expr_kind = sv_expr_kind;

    return result;
}
Node *transformExprRecurse(ParseState *pstate, Node *expr)
{
    Node* result = NULL;

    // 检查表达式是否为空
    if (expr == NULL) {
        return NULL;
    }
    
    // 检查堆栈深度
    check_stack_depth();

    // 根据节点类型，进行不同的转换
    switch (nodeTag(expr)) {
        case T_ColumnRef:
            // 转换列引用
            result = transformColumnRef(pstate, (ColumnRef*)expr);
            // 如果支持右引用，并且列引用只有一个字段，则设置右引用状态，并设置是否需要检查orderby列
            if (IS_SUPPORT_RIGHT_REF(pstate->rightRefState) && list_length(((ColumnRef*)expr)->fields) == 1) {
                pstate->p_hasTargetSRFs = false;
                pstate->p_is_flt_frame = false;
                if (pstate->rightRefState->isUpsert) {
                    pstate->rightRefState->isUpsertHasRightRef = true;
                } else {
                    // 添加默认表达式节点
                    AddDefaultExprNode(pstate);
                }
            }

            // 如果需要检查orderby列，则将表达式添加到orderbyCols中
            if (pstate->shouldCheckOrderbyCol) {
                pstate->orderbyCols = lappend(pstate->orderbyCols, expr);
            }
            break;

        case T_ParamRef:
            // 转换参数引用
            result = transformParamRef(pstate, (ParamRef*)expr);
            break;

        case T_A_Const: {
            // 转换常量
            A_Const* con = (A_Const*)expr;
            Value* val = &con->val;

            // 创建常量节点
            result = (Node*)make_const(pstate, val, con->location);
            break;
        }

        case T_A_Indirection: {
            // 转换索引引用
            result = transformIndirection(pstate, (A_Indirection *)expr);
            break;
        }

        case T_A_ArrayExpr:
            // 转换数组表达式
            result = transformArrayExpr(pstate, (A_ArrayExpr*)expr, InvalidOid, InvalidOid, -1);
            break;

        case T_TypeCast: {
            // 转换类型转换
            TypeCast* tc = (TypeCast*)expr;
            
            if (IsA(tc->arg, A_ArrayExpr)) {
                Oid targetType;
                Oid elementType;
                int32 targetTypmod;

                typenameTypeIdAndMod(pstate, tc->typname, &targetType, &targetTypmod);

                
                targetType = getBaseTypeAndTypmod(targetType, &targetTypmod);
                elementType = get_element_type(targetType);
                if (OidIsValid(elementType)) {
                    tc = (TypeCast*)copyObject(tc);
                    tc->arg = transformArrayExpr(pstate, (A_ArrayExpr*)tc->arg, targetType, elementType, targetTypmod);
                }
            }

            result = transformTypeCast(pstate, tc);
            break;
        }

        case T_CollateClause:
            result = transformCollateClause(pstate, (CollateClause*)expr);
            break;

        case T_A_Expr: {
            A_Expr* a = (A_Expr*)expr;

            switch (a->kind) {
                case AEXPR_OP:
                    result = transformAExprOp(pstate, a);
                    break;
                case AEXPR_AND:
                    result = transformAExprAnd(pstate, a);
                    break;
                case AEXPR_OR:
                    result = transformAExprOr(pstate, a);
                    break;
                case AEXPR_NOT:
                    result = transformAExprNot(pstate, a);
                    break;
                case AEXPR_OP_ANY:
                    result = transformAExprOpAny(pstate, a);
                    break;
                case AEXPR_OP_ALL:
                    result = transformAExprOpAll(pstate, a);
                    break;
                case AEXPR_DISTINCT:
                    result = transformAExprDistinct(pstate, a);
                    break;
                case AEXPR_NULLIF:
                    result = transformAExprNullIf(pstate, a);
                    break;
                case AEXPR_OF:
                    result = transformAExprOf(pstate, a);
                    break;
                case AEXPR_IN:
                    result = transformAExprIn(pstate, a);
                    break;
                default:
                    ereport(ERROR,
                        (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized A_Expr kind: %d", a->kind)));
            }
            break;
        }

        case T_UserSetElem: {
            result = transformUserSetElem(pstate, (UserSetElem *)expr);
            break;
        }

        case T_UserVar: {
            result = transformUserVar((UserVar *)expr);
            break;
        }

        case T_FuncCall:
            result = transformFuncCall(pstate, (FuncCall*)expr);
            break;

        case T_NamedArgExpr: {
            NamedArgExpr* na = (NamedArgExpr*)expr;

            na->arg = (Expr*)transformExprRecurse(pstate, (Node*)na->arg);
            result = expr;
            break;
        }

        case T_SubLink:
            result = transformSubLink(pstate, (SubLink*)expr);
            break;

	case T_SelectIntoVarList:
            result = transformSelectIntoVarList(pstate, (SelectIntoVarList*)expr);
            break;

        case T_CaseExpr:
            result = transformCaseExpr(pstate, (CaseExpr*)expr);
            break;

        case T_RowExpr:
            result = transformRowExpr(pstate, (RowExpr*)expr);
            break;

        case T_CoalesceExpr:
            result = transformCoalesceExpr(pstate, (CoalesceExpr*)expr);
            break;

        case T_MinMaxExpr:
            result = transformMinMaxExpr(pstate, (MinMaxExpr*)expr);
            break;
        case T_GroupingFunc:
            result = transformGroupingFunc(pstate, (GroupingFunc*)expr);
            break;

        case T_XmlExpr:
            result = transformXmlExpr(pstate, (XmlExpr*)expr);
            break;

        case T_XmlSerialize:
            result = transformXmlSerialize(pstate, (XmlSerialize*)expr);
            break;

        case T_NullTest: {
            NullTest* n = (NullTest*)expr;

            n->arg = (Expr*)transformExprRecurse(pstate, (Node*)n->arg);
            
            n->argisrow = type_is_rowtype(exprType((Node*)n->arg));
            result = expr;
            break;
        }

        case T_BooleanTest:
            result = transformBooleanTest(pstate, (BooleanTest*)expr);
            break;

        case T_CurrentOfExpr:
            result = transformCurrentOfExpr(pstate, (CurrentOfExpr*)expr);
            break;

        case T_PredictByFunction:
            result = transformPredictByFunction(pstate, (PredictByFunction*) expr);
            break;
        case T_PrefixKey:
            result = transformPrefixKey(pstate, (PrefixKey*)expr);
            break;

        case T_SetVariableExpr:
            result = transformSetVariableExpr((SetVariableExpr*)expr);
            break;
        case T_CharsetClause:
            result = transformCharsetClause(pstate, (CharsetClause*)expr);
            break;

            
        case T_Var:
        case T_Const:
        case T_Param:
        case T_Aggref:
        case T_WindowFunc:
        case T_ArrayRef:
        case T_FuncExpr:
        case T_Rownum:
        case T_OpExpr:
        case T_DistinctExpr:
        case T_NullIfExpr:
        case T_ScalarArrayOpExpr:
        case T_BoolExpr:
        case T_FieldSelect:
        case T_FieldStore:
        case T_RelabelType:
        case T_CoerceViaIO:
        case T_ArrayCoerceExpr:
        case T_ConvertRowtypeExpr:
        case T_CollateExpr:
        case T_CaseTestExpr:
        case T_ArrayExpr:
        case T_CoerceToDomain:
        case T_CoerceToDomainValue:
        case T_SetToDefault: {
            result = (Node*)expr;
            break;
        }

        default:
            
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized node type: %d", (int)nodeTag(expr))));
            break;
    }

    return result;
}


// 静态函数unknown_attribute，用于处理未知的属性
static void unknown_attribute(ParseState* pstate, Node* relref, char* attname, int location)
{
    RangeTblEntry* rte = NULL;

    // 1. 检查relref是否为变量，如果是，则根据变量在范围表中的位置获取RangeTblEntry
    
    if (IsA(relref, Var) && ((Var*)relref)->varattno == InvalidAttrNumber) {
        
        rte = GetRTEByRangeTablePosn(pstate, ((Var*)relref)->varno, ((Var*)relref)->varlevelsup);
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_COLUMN),
                errmsg("column %s.%s does not exist", rte->eref->aliasname, attname),
                parser_errposition(pstate, location)));
    } else {
        
        // 2. 否则，检查relref的类型是否为复杂类型，如果是，则根据类型获取RangeTblEntry
        Oid relTypeId = exprType(relref);

        // 3. 如果不是复杂类型，则检查relTypeId是否为RECORDOID，如果是，则根据类型获取RangeTblEntry
        
        if (ISCOMPLEX(relTypeId)) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_COLUMN),
                    errmsg("column \"%s\" not found in data type %s", attname, format_type_be(relTypeId)),
                    parser_errposition(pstate, location)));
        } else if (relTypeId == RECORDOID) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_COLUMN),
                    errmsg("could not identify column \"%s\" in record data type", attname),
                    parser_errposition(pstate, location)));
        } else {
            // 4. 如果不是复杂类型，也不是RECORDOID，则根据类型获取RangeTblEntry
        else {
            ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("column notation .%s applied to type %s, "
                           "which is not a composite type",
                        attname,
                        format_type_be(relTypeId)),
                    parser_errposition(pstate, location)));
        }
    }
}
// 静态函数transformIndirection，用于转换索引表达式，参数为ParseState* pstate和A_Indirection* ind
static Node* transformIndirection(ParseState* pstate, A_Indirection* ind)
{
    // 获取上一次的表
    Node* last_srf = pstate->p_last_srf;
    // 递归转换表达式
    Node* result = transformExprRecurse(pstate, ind->arg);
    // 用于存储索引表达式中的索引列表
    List* subscripts = NIL;
    // 获取表达式的位置
    int location = exprLocation(result);
    // 用于遍历索引表达式中的每一个索引
    ListCell* i = NULL;

    
    // 遍历索引表达式中的每一个索引
    foreach (i, ind->indirection) {
        // 获取索引表达式中的每一个索引
        Node* n = (Node*)lfirst(i);

        // 如果索引是索引表达式，则将其添加到subscripts中
        if (IsA(n, A_Indices)) {
            subscripts = lappend(subscripts, n);
        // 如果索引是*，则抛出错误
        } else if (IsA(n, A_Star)) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("row expansion via \"*\" is not supported here"),
                    parser_errposition(pstate, location)));
        // 如果索引是字符串，则将其转换为函数或列，并将其添加到result中
        } else {
            Node* newresult = NULL;

            AssertEreport(IsA(n, String), MOD_OPT, "");

            
            // 如果subscripts不为空，则将其中的索引表达式转换为函数或列，并将其添加到result中
            if (subscripts != NIL) {
                result = (Node*)transformArraySubscripts(
                    pstate, result, exprType(result), InvalidOid, exprTypmod(result), subscripts, NULL);
            }
            // 将subscripts置为空
            subscripts = NIL;

            // 将索引表达式转换为函数或列，并将其添加到result中
            newresult = ParseFuncOrColumn(pstate, list_make1(n), list_make1(result), last_srf, NULL, location);
            // 如果转换失败，则抛出错误
            if (newresult == NULL) {
                unknown_attribute(pstate, result, strVal(n), location);
            }
            // 将转换后的结果赋值给result
            result = newresult;
        }
    }
    
    // 如果subscripts不为空，则将其中的索引表达式转换为函数或列，并将其添加到result中
    if (subscripts != NIL) {
        result = (Node*)transformArraySubscripts(
            pstate, result, exprType(result), InvalidOid, exprTypmod(result), subscripts, NULL);
    }
    // 返回转换后的结果
    return result;
}
static Node* replaceExprAliasIfNecessary(ParseState* pstate, char* colname, ColumnRef* cref)
{
    ListCell* lc = NULL;
    bool isFind = false;
    Expr* matchExpr = NULL;
    TargetEntry* tle = NULL;
    
    // 遍历pstate->p_target_list，查找与colname匹配的TargetEntry
    foreach (lc, pstate->p_target_list) {
        tle = (TargetEntry*)lfirst(lc);
        
        // 检查tle->expr是否为ArrayRef，并且refexpr是否为Param
        // 如果不是，则检查是否与colname匹配
        
        bool isArrayParam = IsA(tle->expr, ArrayRef) && ((ArrayRef*)tle->expr)->refexpr != NULL &&
                            IsA(((ArrayRef*)tle->expr)->refexpr, Param);
        
        if (tle->resname != NULL && !IsA(tle->expr, Param) && !isArrayParam &&
            strncmp(tle->resname, colname, strlen(colname) + 1) == 0) {
            
            // 检查tle->expr是否包含窗口函数，如果包含，则报错
            if (checkExprHasWindowFuncs((Node*)tle->expr)) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_COLUMN),
                        errmsg("Alias \"%s\" reference with window function included is not supported.", colname),
                        parser_errposition(pstate, cref->location)));
#ifndef ENABLE_MULTIPLE_NODES
            } else if (ContainRownumExpr((Node*)tle->expr)) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_COLUMN),
                     errmsg("Alias \"%s\" reference with ROWNUM included is invalid.", colname),
                     parser_errposition(pstate, cref->location)));
#endif					 
            } else if (contain_volatile_functions((Node*)tle->expr)) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_COLUMN),
                        errmsg("Alias \"%s\" reference with volatile function included is not supported.", colname),
                        parser_errposition(pstate, cref->location)));
            } else {
                
                // 如果isFind为false，则将tle->expr赋值给matchExpr
                if (!isFind) {
                    matchExpr = tle->expr;
                    isFind = true;
                } else {
                    ereport(ERROR,
                        (errcode(ERRCODE_UNDEFINED_COLUMN),
                            errmsg("Alias \"%s\" is ambiguous.", colname),
                            parser_errposition(pstate, cref->location)));
                    return NULL;
                }
            }
        }
    }
    
    // 返回matchExpr的副本
    return (Node*)copyObject(matchExpr);
}


static Node* ParseColumnRef(ParseState* pstate, RangeTblEntry* rte, char* colname, ColumnRef* cref)
{
    Node* node = NULL;

    // 1. Parse the column reference.
    
    // 1.1. If the column reference is a simple column reference, then
    //       parse the column reference as a column reference.
    node = scanRTEForColumn(pstate, rte, colname, cref->location);
    if (node == NULL) {
  
        // 1.2. If the column reference is not a simple column reference,
        //       then parse the column reference as a whole row reference.
        
        node = transformWholeRowRef(pstate, rte, cref->location);
        
        // 1.2.1. If the column reference is a whole row reference, then
        //       parse the column reference as a function or column reference.
        node = ParseFuncOrColumn(pstate, list_make1(makeString(colname)), list_make1(node), pstate->p_last_srf, NULL,
                                 cref->location);
    }

    return node;
}

static ColumnRef *fixSWNameSubLevel(RangeTblEntry *rte, char *rname, char **colname)
{
    ListCell *lc = NULL;

    // 遍历rte->eref->colnames，查找与colname匹配的列名
    
    foreach(lc, rte->eref->colnames) {
        Value *val = (Value *)lfirst(lc);

        // 如果val包含colname，则将其赋值给*colname，并跳出循环
        if (strstr(strVal(val), *colname)) {
            *colname = pstrdup(strVal(val));
            break;
        }
    }

    // 创建一个ColumnRef节点，并将其赋值给c
    
    ColumnRef* c = makeNode(ColumnRef);
    c->fields = list_make2((Node*)makeString(rname), (Node*)makeString(*colname));
    c->location = -1;

    return c;
}


Node* transformColumnRef(ParseState* pstate, ColumnRef* cref)
{
    Node* node = NULL;
    char* nspname = NULL;
    char* relname = NULL;
    char* colname = NULL;
    RangeTblEntry* rte = NULL;
    int levels_up;
    bool hasplus = false;
    enum { CRERR_NO_COLUMN, CRERR_NO_RTE, CRERR_WRONG_DB, CRERR_TOO_MANY } crerr = CRERR_NO_COLUMN;

    // 1. Transform the ColumnRef into a Node.
    
    // 调用pre_columnref_hook函数，如果返回值不为空，则返回该值
    if (pstate->p_pre_columnref_hook != NULL) {
        node = (*pstate->p_pre_columnref_hook)(pstate, cref);
        if (node != NULL) {
            return node;
        }
    }

    // 2. If the ColumnRef is a plus-columnref, check if it is used in a
    //    Select-Statement or Subquery. If not, transform it into a Node.
    
    // 检查是否是plus-columnref，如果是，检查是否在Select-Statement或Subquery中使用
    int fields_len = list_length(cref->fields);
    hasplus = IsColumnRefPlusOuterJoin(cref);
    if (hasplus) {
        
        // 如果是plus-columnref，并且没有忽略plus，则报错
        if (!pstate->ignoreplus) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("Operator \"(+)\" can only be used in WhereClause of Select-Statement or Subquery."),
                    parser_errposition(pstate, cref->location)));
        }

        if (!pstate->ignoreplus) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("Operator \"(+)\" can only be used in WhereClause of Select-Statement or Subquery."),
                    parser_errposition(pstate, cref->location)));
        // 如果是plus-columnref，并且忽略plus，则将fields_len减1
        
        fields_len--;
    }

        
        fields_len--;
    }
    
    // 根据字段长度，进行不同的处理
    switch (fields_len) {
        case 1: {
            // 获取第一个字段
            Node* field1 = (Node*)linitial(cref->fields);

            // 检查字段是否为字符串
            AssertEreport(IsA(field1, String), MOD_OPT, "");
            colname = strVal(field1);

            // 如果需要，则使用STARTWITH函数处理字段
            if (pstate->p_hasStartWith) {
                Node *expr = transformStartWithColumnRef(pstate, cref, &colname);

                // 如果expr不为空，则返回expr
                
                if (expr != NULL) {
                    return expr;
                }
            }

            // 获取列名对应的变量
            node = colNameToVar(pstate, colname, hasplus, cref->location, &rte);

            // 如果hasplus为真，且node为空，则报错
            if (hasplus && node == NULL) {
                ereport(ERROR,
                    (errcode(ERRCODE_AMBIGUOUS_COLUMN),
                        errmsg("column reference \"%s\" is ambiguous.", colname),
                        errhint("\"%s\" with \"(+)\" can only reference relation in current query level.", colname),
                        parser_errposition(pstate, cref->location)));
            }

            // 如果node为空，则根据rte的类型，进行不同的处理
            if (node == NULL) {
 
                // 如果pstate->p_value_substitute不为空，且colname为value，则将pstate->p_value_substitute赋值给node
                if (pstate->p_value_substitute != NULL && strcmp(colname, "value") == 0) {
                    node = (Node*)copyObject(pstate->p_value_substitute);

                    // 如果node为CoerceToDomainValue类型，则将node的location赋值为cref->location
                    if (IsA(node, CoerceToDomainValue)) {
                        ((CoerceToDomainValue*)node)->location = cref->location;
                    }
                    break;
                }

                // 否则，根据rte的类型，进行不同的处理

                // 如果rte不为空，则根据rte的类型，进行不同的处理
                rte = refnameRangeTblEntry(pstate, NULL, colname, cref->location, &levels_up);
                if (rte != NULL) {
                    // 如果rte为COLORPAX类型，或者为HDFS类型，或者为OBS类型，且rte的alias为colname，或者rte的relname为colname，则将rte的row_expr赋值给node
                    if ((OrientedIsCOLorPAX(rte) || RelIsSpecifiedFTbl(rte, HDFS) || RelIsSpecifiedFTbl(rte, OBS)) &&
                        ((rte->alias && (strcmp(rte->alias->aliasname, colname) == 0)) ||
                            (strcmp(rte->relname, colname) == 0))) {
                        Node* row_expr = convertStarToCRef(rte, NULL, NULL, colname, cref->location);
                        node = transformExprRecurse(pstate, row_expr);
                    } else {
                        // 否则，将rte的whole_row_expr赋值给node
                        node = transformWholeRowRef(pstate, rte, cref->location);
                    }
                    break;
                }

                // 如果rte为空，则将colname对应的变量赋值给node
                node = replaceExprAliasIfNecessary(pstate, colname, cref);

                // 如果node为包含子plan的节点，则报错
                if (!pstate->isAliasReplace && contain_subplans(node)) {
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg(
                                "Alias \"%s\" contains subplan, which is not supported to use in grouping() function",
                                colname)));
                }

                // 如果node为空，则根据pstate->p_bind_variable_columnref_hook和pstate->p_bind_describe_hook，进行不同的处理
                if (node == NULL) {
                    if (pstate->p_bind_variable_columnref_hook != NULL) {
                        node = (*pstate->p_bind_variable_columnref_hook)(pstate, cref);
                        if (node != NULL) {
                            return node;
                        }
                    }
                    if (pstate->p_bind_describe_hook != NULL) {
                        node = (*pstate->p_bind_describe_hook)(pstate, cref);
                        return node;
                    }
                }
            }
            break;
        }
        case 2: {
            // 获取cref的第一个和第二个字段
            Node* field1 = (Node*)linitial(cref->fields);
            Node* field2 = (Node*)lsecond(cref->fields);

            // 检查第一个字段是否为字符串
            AssertEreport(IsA(field1, String), MOD_OPT, "");
            relname = strVal(field1);

            // 检查是否有plus

            if (hasplus) {
                // 获取引用范围表条目
                rte = refnameRangeTblEntry(pstate, nspname, relname, cref->location, NULL);
            } else {
                // 获取引用范围表条目，并获取levels_up
                rte = refnameRangeTblEntry(pstate, nspname, relname, cref->location, &levels_up);
            }

            // 检查引用范围表条目是否存在
            if (rte == NULL && IsSequenceFuncCall(NULL, field1, field2)) {
                // 如果是序列函数调用，则调用transformSequenceFuncCall函数
                return transformSequenceFuncCall(pstate, NULL, field1, field2, cref->location);

            if (rte == NULL && IsSequenceFuncCall(NULL, field1, field2)) {
                return transformSequenceFuncCall(pstate, NULL, field1, field2, cref->location);
            } else if (rte == NULL) {
                // 引用范围表条目不存在
                crerr = CRERR_NO_RTE;
                break;
            }

            } else if (rte == NULL) {
                crerr = CRERR_NO_RTE;
            // 检查是否是COLORPAX或OBS表
            if (IsA(field2, A_Star)) {
                // 如果是COLORPAX或OBS表，则调用convertStarToCRef函数
                if (OrientedIsCOLorPAX(rte) || RelIsSpecifiedFTbl(rte, HDFS) || RelIsSpecifiedFTbl(rte, OBS)) {
                    Node* row_expr = convertStarToCRef(rte, NULL, NULL, relname, cref->location);
                    node = transformExprRecurse(pstate, row_expr);
                } else {
                    // 如果是其他表，则调用transformWholeRowRef函数
                    node = transformWholeRowRef(pstate, rte, cref->location);
                }
                break;
            }

            // 检查第二个字段是否为字符串
            AssertEreport(IsA(field2, String), MOD_OPT, "");
            colname = strVal(field2);

            if (IsA(field2, A_Star)) {
                if (OrientedIsCOLorPAX(rte) || RelIsSpecifiedFTbl(rte, HDFS) || RelIsSpecifiedFTbl(rte, OBS)) {
                    Node* row_expr = convertStarToCRef(rte, NULL, NULL, relname, cref->location);
                    node = transformExprRecurse(pstate, row_expr);
                } else {
                    node = transformWholeRowRef(pstate, rte, cref->location);
            // 检查是否是子查询，并且是否包含子查询
            if (rte->rtekind == RTE_SUBQUERY && rte->swSubExist) {
                // 如果是，则调用fixSWNameSubLevel函数
                cref = fixSWNameSubLevel(rte, relname, &colname);
            }

            // 检查是否是p_hasStartWith或swConverted
            if (pstate->p_hasStartWith || rte->swConverted) {
                // 如果是，则调用transformStartWithColumnRef函数
                Node *expr = transformStartWithColumnRef(pstate, cref, &colname);
case 3: {
            // 获取cref的fields中的三个元素
            Node* field1 = (Node*)linitial(cref->fields);
            Node* field2 = (Node*)lsecond(cref->fields);
            Node* field3 = (Node*)lthird(cref->fields);

            // 检查field1是否为String类型
            AssertEreport(IsA(field1, String), MOD_OPT, "");
            // 将field1转换为字符串赋值给nspname
            nspname = strVal(field1);
            // 检查field2是否为String类型
            AssertEreport(IsA(field2, String), MOD_OPT, "");
            // 将field2转换为字符串赋值给relname
            relname = strVal(field2);

            // 检查cref的fields中是否有plus
            
            if (hasplus) {
                // 调用refnameRangeTblEntry函数，传入nspname, relname, cref->location, NULL，返回rte
                rte = refnameRangeTblEntry(pstate, nspname, relname, cref->location, NULL);
            } else {
                // 调用refnameRangeTblEntry函数，传入nspname, relname, cref->location, &levels_up，返回rte
                rte = refnameRangeTblEntry(pstate, nspname, relname, cref->location, &levels_up);
            }

            
            // 检查rte是否为NULL，并且field1, field2, field3是否为SequenceFuncCall
            if (rte == NULL && IsSequenceFuncCall(field1, field2, field3)) {
                // 调用transformSequenceFuncCall函数，传入field1, field2, field3, cref->location，返回node
                return transformSequenceFuncCall(pstate, field1, field2, field3, cref->location);

            } else if (rte == NULL) {
                // 调用CRERR_NO_RTE，返回crerr
                crerr = CRERR_NO_RTE;
                break;
            }

            
            // 检查field3是否为A_Star
            if (IsA(field3, A_Star)) {
                // 检查rte是否为COLorPAX，HDFS，OBS
                if (OrientedIsCOLorPAX(rte) || RelIsSpecifiedFTbl(rte, HDFS) || RelIsSpecifiedFTbl(rte, OBS)) {
                    // 调用convertStarToCRef函数，传入rte, NULL, nspname, relname, cref->location，返回row_expr
                    Node* row_expr = convertStarToCRef(rte, NULL, nspname, relname, cref->location);
                    // 调用transformExprRecurse函数，传入row_expr，返回node
                    node = transformExprRecurse(pstate, row_expr);
                } else {
                    // 调用transformWholeRowRef函数，传入rte, cref->location，返回node
                    node = transformWholeRowRef(pstate, rte, cref->location);
                }
                break;
            }

            // 检查field3是否为String类型
            AssertEreport(IsA(field3, String), MOD_OPT, "");
            // 将field3转换为字符串赋值给colname
            colname = strVal(field3);

            // 调用ParseColumnRef函数，传入rte, colname, cref，返回node
            node = ParseColumnRef(pstate, rte, colname, cref);
            break;
        }       }

                if (strstr(colname, "@")) {
                    ListCell *lc = NULL;

                    foreach(lc, pstate->p_rtable) {
                        RangeTblEntry *tbl = (RangeTblEntry *)lfirst(lc);

                        if (tbl->relname != NULL &&
                            strcmp(tbl->relname, "tmp_reuslt") == 0) {
                            rte = tbl;
                            break;
                        }
                    }
                }
            }

            node = ParseColumnRef(pstate, rte, colname, cref);
            break;
        }
        case 3: {
            // 获取cref->fields中的三个元素
            Node* field1 = (Node*)linitial(cref->fields);
            Node* field2 = (Node*)lsecond(cref->fields);
            Node* field3 = (Node*)lthird(cref->fields);

            // 检查field1是否是字符串
            AssertEreport(IsA(field1, String), MOD_OPT, "");
            // 获取field1的值
            nspname = strVal(field1);
            // 检查field2是否是字符串
            AssertEreport(IsA(field2, String), MOD_OPT, "");
            // 获取field2的值
            relname = strVal(field2);

            // 检查cref->location是否是函数调用
            // 如果是函数调用，则调用transformSequenceFuncCall函数
            
            if (hasplus) {
                // 检查cref->location是否是函数调用
                rte = refnameRangeTblEntry(pstate, nspname, relname, cref->location, NULL);
            } else {
                // 检查cref->location是否是函数调用
                rte = refnameRangeTblEntry(pstate, nspname, relname, cref->location, &levels_up);
            }

            // 检查rte是否为空
            
            if (rte == NULL && IsSequenceFuncCall(field1, field2, field3)) {
                // 如果是函数调用，则调用transformSequenceFuncCall函数
                return transformSequenceFuncCall(pstate, field1, field2, field3, cref->location);

            } else if (rte == NULL) {
                // 检查rte是否为空
                crerr = CRERR_NO_RTE;
                break;
            }

            // 检查rte是否是COLORPAX、OBS、FTbl中的一个
            
            if (IsA(field3, A_Star)) {
                // 检查rte是否是COLORPAX、OBS、FTbl中的一个
                if (OrientedIsCOLorPAX(rte) || RelIsSpecifiedFTbl(rte, HDFS) || RelIsSpecifiedFTbl(rte, OBS)) {
                    // 调用convertStarToCRef函数，将rte转换为cref
                    Node* row_expr = convertStarToCRef(rte, NULL, nspname, relname, cref->location);
                    // 调用transformExprRecurse函数，将row_expr转换为node
                    node = transformExprRecurse(pstate, row_expr);
                } else {
                    // 调用transformWholeRowRef函数，将rte转换为node
                    node = transformWholeRowRef(pstate, rte, cref->location);
                }
                break;
            }

            // 检查field3是否是字符串
            AssertEreport(IsA(field3, String), MOD_OPT, "");
            // 获取field3的值
            colname = strVal(field3);

            // 调用ParseColumnRef函数，将rte、colname、cref转换为node
            node = ParseColumnRef(pstate, rte, colname, cref);
            break;
        }
       case 4: {
            // 获取cref的4个字段
            Node* field1 = (Node*)linitial(cref->fields);
            Node* field2 = (Node*)lsecond(cref->fields);
            Node* field3 = (Node*)lthird(cref->fields);
            Node* field4 = (Node*)lfourth(cref->fields);
            // 初始化catname
            char* catname = NULL;

            // 检查field1是否为String类型
            AssertEreport(IsA(field1, String), MOD_OPT, "");
            // 获取field1的值
            catname = strVal(field1);
            // 检查field2是否为String类型
            AssertEreport(IsA(field2, String), MOD_OPT, "");
            // 获取field2的值
            nspname = strVal(field2);
            // 检查field3是否为String类型
            AssertEreport(IsA(field3, String), MOD_OPT, "");
            // 获取field3的值
            relname = strVal(field3);

            // 检查catname是否与当前数据库名称相同
            
            if (strcmp(catname, get_and_check_db_name(u_sess->proc_cxt.MyDatabaseId, true)) != 0) {
                crerr = CRERR_WRONG_DB;
                break;
            }

            // 检查nspname是否为当前命名空间
            
            if (hasplus) {
                // 检查nspname和relname是否在当前命名空间中
                rte = refnameRangeTblEntry(pstate, nspname, relname, cref->location, NULL);
            } else {
                // 检查nspname和relname是否在当前命名空间中，并且检查levels_up是否为0
                rte = refnameRangeTblEntry(pstate, nspname, relname, cref->location, &levels_up);
            }

            // 检查rte是否为空
            
            if (rte == NULL && IsSequenceFuncCall(field2, field3, field4)) {
                // 尝试转换序列函数调用
                return transformSequenceFuncCall(pstate, field2, field3, field4, cref->location);

            } else if (rte == NULL) {
                // 检查rte是否为空
                crerr = CRERR_NO_RTE;
                break;
            }

            // 检查field4是否为A_Star类型
            
            if (IsA(field4, A_Star)) {
                // 检查rte是否为COLOR_PAX，HDFS，OBS类型
                if (OrientedIsCOLorPAX(rte) || RelIsSpecifiedFTbl(rte, HDFS) || RelIsSpecifiedFTbl(rte, OBS)) {
                    // 尝试将*转换为cref
                    Node* row_expr = convertStarToCRef(rte, catname, nspname, relname, cref->location);
                    node = transformExprRecurse(pstate, row_expr);
                } else {
                    // 尝试将whole_row转换为cref
                    node = transformWholeRowRef(pstate, rte, cref->location);
                }
                break;
            }

            // 检查field4是否为String类型
            AssertEreport(IsA(field4, String), MOD_OPT, "");
            // 获取field4的值
            colname = strVal(field4);

            // 尝试将cref转换为列引用
            node = ParseColumnRef(pstate, rte, colname, cref);
            break;
        }
        default:
            crerr = CRERR_TOO_MANY; 
            break;
    }

    // 检查pstate->p_post_columnref_hook是否为空
    if (pstate->p_post_columnref_hook != NULL) {
        Node* hookresult = NULL;

    if (pstate->p_post_columnref_hook != NULL) {
        Node* hookresult = NULL;
        // 检查node是否为空
        
        hookresult = (*pstate->p_post_columnref_hook)(pstate, cref, node);
        if (node == NULL) {
            node = hookresult;
        } else if (hookresult != NULL) {
            // 检查hookresult是否为空
            if (IS_SUPPORT_RIGHT_REF(pstate->rightRefState)) {
                node = hookresult;
            } else {
                // 检查是否是ambiguous column reference
                ereport(ERROR,
                    (errcode(ERRCODE_AMBIGUOUS_COLUMN),
                        errmsg("column reference \"%s\" is ambiguous", NameListToString(cref->fields)),
                        parser_errposition(pstate, cref->location)));
            }

        hookresult = (*pstate->p_post_columnref_hook)(pstate, cref, node);
        if (node == NULL) {
            node = hookresult;
        } else if (hookresult != NULL) {
            if (IS_SUPPORT_RIGHT_REF(pstate->rightRefState)) {
                node = hookresult;
            } else {
                ereport(ERROR,
                    (errcode(ERRCODE_AMBIGUOUS_COLUMN),
                        errmsg("column reference \"%s\" is ambiguous", NameListToString(cref->fields)),
                        parser_errposition(pstate, cref->location)));
        }
    }
    
    // 检查是否是SQL兼容模式
    if (node == NULL && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) {
        // 尝试转换FuncCall
        node = tryTransformFunc(pstate, cref->fields, cref->location);
        if (node) {
            return node;
        }
    }
    
    if (node == NULL && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) {
        node = tryTransformFunc(pstate, cref->fields, cref->location);
        if (node) {
            return node;
    // 检查是否是SQL兼容模式
    if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) {
        // 检查node是否为空
        Node* check = plpgsql_check_match_var(node, pstate, cref);
        if (check) {
            return check;
        }
    }

    if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) {
        Node* check = plpgsql_check_match_var(node, pstate, cref);
        if (check) {
            return check;
        }
    }

    
    if (node == NULL) {
        switch (crerr) {
            case CRERR_NO_COLUMN:
                errorMissingColumn(pstate, relname, colname, cref->location);
                break;
            case CRERR_NO_RTE:
                errorMissingRTE(pstate, makeRangeVar(nspname, relname, cref->location), hasplus);
                break;
            case CRERR_WRONG_DB:
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("cross-database references are not implemented: %s", NameListToString(cref->fields)),
                        parser_errposition(pstate, cref->location)));
                break;
            case CRERR_TOO_MANY:
                ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("improper qualified name (too many dotted names): %s", NameListToString(cref->fields)),
                        parser_errposition(pstate, cref->location)));
                break;
            default:
                break;
        }
    }

    
    if (pstate->p_plusjoin_rte_info != NULL && pstate->p_plusjoin_rte_info->needrecord && rte != NULL) {
        pstate->p_plusjoin_rte_info->info =
            lappend(pstate->p_plusjoin_rte_info->info, makePlusJoinRTEItem(rte, hasplus));
    }

    return node;
}

// 判断是否是列2函数
static bool isCol2Function(List* fields)
{
    // 获取函数的命名空间
    char* schemaname = NULL;
    char* pkgname = NULL;
    char* funcname = NULL;
    DeconstructQualifiedName(fields, &schemaname, &funcname, &pkgname);
    Oid npsOid = InvalidOid;
    Oid pkgOid = InvalidOid;
    // 获取命名空间Oid
    if (schemaname != NULL) {
        npsOid = get_namespace_oid(schemaname, true);
    }
    else {
        npsOid = getCurrentNamespace();
    }

    // 获取包名Oid
    if (pkgname != NULL) {
        pkgOid = PackageNameListGetOid(fields, true);
    }

    // 判断是否找到函数
    bool is_found = false;
    
    // 获取函数列表
    CatCList   *catlist = NULL;
#ifndef ENABLE_MULTIPLE_NODES
    // 如果当前版本小于92470，则使用PROCNAMEARGSNSP缓存，否则使用PROCALLARGS缓存
    if (t_thrd.proc->workingVersionNum < 92470) {
        catlist = SearchSysCacheList1(PROCNAMEARGSNSP, CStringGetDatum(funcname));
    } else {
        catlist = SearchSysCacheList1(PROCALLARGS, CStringGetDatum(funcname));
    }
#else
    // 如果当前版本大于92470，则使用PROCNAMEARGSNSP缓存
    catlist = SearchSysCacheList1(PROCNAMEARGSNSP, CStringGetDatum(funcname));
#endif
    // 遍历函数列表
    for (int i = 0; i < catlist->n_members; i++) {
        // 从缓存中获取函数元组
        HeapTuple proctup = t_thrd.lsc_cxt.FetchTupleFromCatCList(catlist, i);
        Form_pg_proc procform = (Form_pg_proc)GETSTRUCT(proctup);
        
        // 获取函数参数信息
        Oid *p_argtypes = NULL;
        char **p_argnames = NULL;
        char *p_argmodes = NULL;
        int allArgs = get_func_arg_info(proctup, &p_argtypes, &p_argnames, &p_argmodes);
        // 如果参数个数不等于默认参数个数或者函数返回类型为空，则跳过
        if ((allArgs > 0 && allArgs != procform->pronargdefaults) || !OidIsValid(procform->prorettype)) {
            continue;
        }

        // 如果函数命名空间Oid有效，则判断函数命名空间是否与npsOid相同
        if (OidIsValid(npsOid)) {
            
            if (procform->pronamespace != npsOid) {
                continue;
            }
	    }

        // 如果包名Oid有效，则判断函数包名是否与pkgOid相同
        if (OidIsValid(pkgOid)) {
            bool isNull = false;
            Datum packageid_datum = SysCacheGetAttr(PROCOID, proctup, Anum_pg_proc_packageid, &isNull);
            Oid packageid = ObjectIdGetDatum(packageid_datum);
            if (packageid != pkgOid) {
                continue;
            }
        }
        // 找到函数，设置is_found为true
        is_found = true;
    }
    // 释放函数列表缓存
    ReleaseSysCacheList(catlist);
    // 返回是否找到函数
    return is_found;
}

static Node* tryTransformFunc(ParseState* pstate, List* fields, int location)
{
    // 如果不是col2函数，则返回NULL
    if (!isCol2Function(fields)) {
        return NULL;
    }

    // 初始化结果变量
    Node* result = NULL;
    // 创建FuncCall节点
    FuncCall *fn = makeNode(FuncCall);
    // 设置函数名
    fn->funcname = fields;
    // 设置参数列表
    fn->args = NIL;
    // 设置聚合函数排序列表
    fn->agg_order = NIL;
    // 设置聚合函数是否使用*号
    fn->agg_star = FALSE;
    // 设置聚合函数是否去重
    fn->agg_distinct = FALSE;
    // 设置聚合函数是否可变参数
    fn->func_variadic = FALSE;
    // 设置聚合函数的over子句
    fn->over = NULL;
    // 设置函数位置
    fn->location = location;
    // 设置函数调用标志
    fn->call_func = false;
    
    // 初始化参数列表
    List* targs = NIL;

    
    // 调用ParseFuncOrColumn函数，解析函数或列，并将结果赋值给result
    result = ParseFuncOrColumn(pstate, fn->funcname, targs, pstate->p_last_srf, fn, fn->location, fn->call_func);

    
    // 如果函数调用标志为true，且函数名是包函数，且函数调用结果不为空，且函数调用结果是FuncExpr，则将函数调用结果的参数列表设置为函数的参数列表
    if (IsPackageFunction(fn->funcname) && result != NULL && nodeTag(result) == T_FuncExpr && fn->call_func) {
        FuncExpr* funcexpr = (FuncExpr*)result;
        int funcoid = funcexpr->funcid;
        funcexpr->args = extract_function_outarguments(funcoid, funcexpr->args, fn->funcname);
    }

    return result;
}

static Node* transformParamRef(ParseState* pstate, ParamRef* pref)
{
    // 初始化结果变量
    Node* result = NULL;

    // 调用参数引用钩子函数，如果存在，则将结果赋值给result
    
    if (pstate->p_paramref_hook != NULL) {
        result = (*pstate->p_paramref_hook)(pstate, pref);
    } else {
        result = NULL;
    }
    // 如果result为空，则报错
    if (result == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_PARAMETER),
                errmsg("there is no parameter $%d", pref->number),
                parser_errposition(pstate, pref->location)));
    }
    return result;
}


// 判断表达式是否为空常量
static bool exprIsNullConstant(Node* arg)
{
    if (arg && IsA(arg, A_Const)) {
        A_Const* con = (A_Const*)arg;

        if (con->val.type == T_Null) {
            return true;
        }
    }
    return false;
}

// 转换A_Expr操作，返回转换后的节点
static Node* transformAExprOp(ParseState* pstate, A_Expr* a)
{
    Node* lexpr = a->lexpr;
    Node* rexpr = a->rexpr;
    Node* result = NULL;

    // 检查是否是null = null，如果是，则可以替换为null test
    
    if (u_sess->attr.attr_sql.Transform_null_equals && list_length(a->name) == 1 &&
        strcmp(strVal(linitial(a->name)), "=") == 0 && (exprIsNullConstant(lexpr) || exprIsNullConstant(rexpr)) &&
        (!IsA(lexpr, CaseTestExpr) && !IsA(rexpr, CaseTestExpr))) {
        NullTest* n = makeNode(NullTest);

        n->nulltesttype = IS_NULL;

        n->arg = exprIsNullConstant(lexpr) ? (Expr *)rexpr : (Expr *)lexpr;

        result = transformExprRecurse(pstate, (Node*)n);
    } else if (lexpr && IsA(lexpr, RowExpr) && rexpr && IsA(rexpr, SubLink) &&
               ((SubLink*)rexpr)->subLinkType == EXPR_SUBLINK) {
        
        SubLink* s = (SubLink*)rexpr;

        s->subLinkType = ROWCOMPARE_SUBLINK;
        s->testexpr = lexpr;
        s->operName = a->name;
        s->location = a->location;
        result = transformExprRecurse(pstate, (Node*)s);
    } else if (lexpr && IsA(lexpr, RowExpr) && rexpr && IsA(rexpr, RowExpr)) {
        
        lexpr = transformExprRecurse(pstate, lexpr);
        rexpr = transformExprRecurse(pstate, rexpr);
        AssertEreport(IsA(lexpr, RowExpr), MOD_OPT, "");
        AssertEreport(IsA(rexpr, RowExpr), MOD_OPT, "");

        result = make_row_comparison_op(pstate, a->name, ((RowExpr*)lexpr)->args, ((RowExpr*)rexpr)->args, a->location);
    } else {
        
        Node *last_srf = parse_get_last_srf(pstate);
        lexpr = transformExprRecurse(pstate, lexpr);
        rexpr = transformExprRecurse(pstate, rexpr);

        result = (Node*)make_op(pstate, a->name, lexpr, rexpr, last_srf, a->location);
    }

    return result;
}

// 静态函数transformAExprAnd，用于转换A_Expr的AND表达式，并返回一个Node*
static Node* transformAExprAnd(ParseState* pstate, A_Expr* a)
{
    // 递归调用transformExprRecurse函数，转换a->lexpr和a->rexpr
    Node* lexpr = transformExprRecurse(pstate, a->lexpr);
    Node* rexpr = transformExprRecurse(pstate, a->rexpr);

    // 将lexpr和rexpr转换为布尔类型
    lexpr = coerce_to_boolean(pstate, lexpr, "AND");
    rexpr = coerce_to_boolean(pstate, rexpr, "AND");

    // 返回一个布尔表达式，其中包含lexpr和rexpr
    return (Node*)makeBoolExpr(AND_EXPR, list_make2(lexpr, rexpr), a->location);
}

// 静态函数transformAExprOr，用于转换A_Expr的OR表达式，并返回一个Node*
static Node* transformAExprOr(ParseState* pstate, A_Expr* a)
{
    // 递归调用transformExprRecurse函数，转换a->lexpr和a->rexpr
    Node* lexpr = transformExprRecurse(pstate, a->lexpr);
    Node* rexpr = transformExprRecurse(pstate, a->rexpr);

    // 将lexpr和rexpr转换为布尔类型
    lexpr = coerce_to_boolean(pstate, lexpr, "OR");
    rexpr = coerce_to_boolean(pstate, rexpr, "OR");

    // 返回一个布尔表达式，其中包含lexpr和rexpr
    return (Node*)makeBoolExpr(OR_EXPR, list_make2(lexpr, rexpr), a->location);
}

// 静态函数transformAExprNot，用于转换A_Expr的NOT表达式，并返回一个Node*
static Node* transformAExprNot(ParseState* pstate, A_Expr* a)
{
    // 递归调用transformExprRecurse函数，转换a->rexpr
    Node* rexpr = transformExprRecurse(pstate, a->rexpr);

    // 将rexpr转换为布尔类型
    rexpr = coerce_to_boolean(pstate, rexpr, "NOT");

    // 返回一个布尔表达式，其中包含rexpr
    return (Node*)makeBoolExpr(NOT_EXPR, list_make1(rexpr), a->location);
}

// 静态函数transformAExprOpAny，用于转换A_Expr的ANY表达式，并返回一个Node*
static Node* transformAExprOpAny(ParseState* pstate, A_Expr* a)
{
    // 递归调用transformExprRecurse函数，转换a->lexpr和a->rexpr
    Node* lexpr = transformExprRecurse(pstate, a->lexpr);
    Node* rexpr = transformExprRecurse(pstate, a->rexpr);

    // 返回一个标量数组操作符，其中包含lexpr和rexpr
    return (Node*)make_scalar_array_op(pstate, a->name, true, lexpr, rexpr, a->location);
}

// 静态函数transformAExprOpAll，用于转换A_Expr的ALL表达式，并返回一个Node*
static Node* transformAExprOpAll(ParseState* pstate, A_Expr* a)
{
    // 递归调用transformExprRecurse函数，转换a->lexpr和a->rexpr
    Node* lexpr = transformExprRecurse(pstate, a->lexpr);
    Node* rexpr = transformExprRecurse(pstate, a->rexpr);

    // 返回一个标量数组操作符，其中包含lexpr和rexpr
    return (Node*)make_scalar_array_op(pstate, a->name, false, lexpr, rexpr, a->location);
}

// 静态函数transformAExprDistinct，用于转换A_Expr的DISTINCT表达式，并返回一个Node*
static Node* transformAExprDistinct(ParseState* pstate, A_Expr* a)
{
    // 递归调用transformExprRecurse函数，转换a->lexpr和a->rexpr
    Node* lexpr = transformExprRecurse(pstate, a->lexpr);
    Node* rexpr = transformExprRecurse(pstate, a->rexpr);

    // 如果lexpr和rexpr都是RowExpr，则返回一个行去重操作符，其中包含lexpr和rexpr
    if (lexpr && IsA(lexpr, RowExpr) && rexpr && IsA(rexpr, RowExpr)) {
        
        return make_row_distinct_op(pstate, a->name, (RowExpr*)lexpr, (RowExpr*)rexpr, a->location);
    } else {
        
        // 否则，返回一个标量数组操作符，其中包含lexpr和rexpr
        return (Node*)make_distinct_op(pstate, a->name, lexpr, rexpr, a->location);
    }
}

//static Node* transformAExprNullIf(ParseState* pstate, A_Expr* a)
//功能：处理NULLIF表达式，返回一个节点
//参数：pstate：解析状态；a：表达式
//返回值：一个节点
static Node* transformAExprNullIf(ParseState* pstate, A_Expr* a)
{
    //获取最后一个SRF节点
    Node* last_srf = parse_get_last_srf(pstate);
    //递归转换表达式
    Node* lexpr = transformExprRecurse(pstate, a->lexpr);
    Node* rexpr = transformExprRecurse(pstate, a->rexpr);
    OpExpr* result = NULL;

    //创建一个操作表达式，并设置操作符为NULLIF
    result = (OpExpr*)make_op(pstate, a->name, lexpr, rexpr, last_srf, a->location);

    //检查操作表达式的结果类型是否为BOOLOID，如果不是，则报错
    
    if (result->opresulttype != BOOLOID) {
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("NULLIF requires = operator to yield boolean"),
                parser_errposition(pstate, a->location)));
    }
    //如果操作表达式返回一个集合，则报错
    if (result->opretset && pstate && pstate->p_is_flt_frame)
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                        
                        errmsg("%s must not return a set", "NULLIF"), parser_errposition(pstate, a->location)));
    
    //设置操作表达式的结果类型为表达式的第一个参数的类型
    result->opresulttype = exprType((Node*)linitial(result->args));

    
    //设置操作表达式的类型为NULLIF
    NodeSetTag(result, T_NullIfExpr);

    return (Node*)result;
}

//static Node* transformAExprOf(ParseState* pstate, A_Expr* a)
//功能：处理OF表达式，返回一个节点
//参数：pstate：解析状态；a：表达式
//返回值：一个节点
static Node* transformAExprOf(ParseState* pstate, A_Expr* a)
{
    
    //递归转换表达式
    Node* lexpr = transformExprRecurse(pstate, a->lexpr);
    Const* result = NULL;
    ListCell* telem = NULL;
    Oid ltype, rtype;
    bool matched = false;

    //检查表达式的类型是否与表达式列表中的类型匹配
    ltype = exprType(lexpr);
    foreach (telem, (List*)a->rexpr) {
        rtype = typenameTypeId(pstate, (const TypeName*)lfirst(telem));
        matched = (rtype == ltype);
        if (matched) {
            break;
        }
    }

    
    //如果表达式的类型为<>，则检查表达式列表中的类型是否与<>匹配
    if (strcmp(strVal(linitial(a->name)), "<>") == 0) {
        matched = (!matched);
    }
    //创建一个布尔常量，并设置值为匹配
    result = (Const*)makeBoolConst(matched, false);

    
    //设置布尔常量的位置为表达式的位置
    result->location = exprLocation((Node*)a);

    return (Node*)result;
}

static Node* transformAExprIn(ParseState* pstate, A_Expr* a)
{
    Node* result = NULL;
    Node* lexpr = NULL;
    List* rexprs = NIL;
    List* rvars = NIL;
    List* rnonvars = NIL;
    bool useOr = false;
    bool haveRowExpr = false;
    bool haveSetType = false;
    ListCell* l = NULL;

    
    // 检查IN表达式是否是<>
    if (strcmp(strVal(linitial(a->name)), "<>") == 0) {
        useOr = false;
    } else {
        useOr = true;
    }
    
    // 递归转换表达式
    lexpr = transformExprRecurse(pstate, a->lexpr);
    // 检查IN表达式是否是行表达式
    haveRowExpr = (lexpr && IsA(lexpr, RowExpr));
    // 检查IN表达式是否是集合类型
    haveSetType = type_is_set(exprType(lexpr));
    // 初始化表达式列表
    rexprs = rvars = rnonvars = NIL;
    // 遍历IN表达式中的表达式
    foreach (l, (List*)a->rexpr) {
        Node* rexpr = (Node*)transformExprRecurse(pstate, (Node*)lfirst(l));

        // 检查IN表达式是否是行表达式
        haveRowExpr = haveRowExpr || (rexpr && IsA(rexpr, RowExpr));
        // 检查IN表达式是否是集合类型
        haveSetType = haveSetType || type_is_set(exprType(rexpr));
        // 将表达式添加到表达式列表中
        rexprs = lappend(rexprs, rexpr);
        // 检查表达式是否是变量
        if (contain_vars_of_level(rexpr, 0)) {
            // 如果是变量，则将表达式添加到变量列表中
            rvars = lappend(rvars, rexpr);
        } else {
            // 如果不是变量，则将表达式添加到非变量列表中
            rnonvars = lappend(rnonvars, rexpr);
        }
    }

    
    // 如果IN表达式不是行表达式，并且非变量列表中有多个表达式，则将非变量列表中的表达式转换为数组表达式
    if (!haveRowExpr && list_length(rnonvars) > 1) {
        List* allexprs = NIL;
        Oid scalar_type;
        Oid array_type;
        const char *context = haveSetType ? "IN" : NULL;
        
        // 将表达式列表和非变量列表中的表达式合并
        allexprs = list_concat(list_make1(lexpr), rnonvars);
        // 选择公共类型
        scalar_type = select_common_type(pstate, allexprs, context, NULL);

        
        if (OidIsValid(scalar_type)) {
            // 如果公共类型有效，则获取数组类型
            array_type = get_array_type(scalar_type);
        } else {
            // 如果公共类型无效，则将数组类型设置为无效
            array_type = InvalidOid;
        }
        if (array_type != InvalidOid) {
            
            List* aexprs = NIL;
            ArrayExpr* newa = NULL;

            // 将非变量列表中的表达式转换为数组表达式
            aexprs = NIL;
            foreach (l, rnonvars) {
                Node* rexpr = (Node*)lfirst(l);

                // 将表达式转换为公共类型
                rexpr = coerce_to_common_type(pstate, rexpr, scalar_type, "IN");
                // 将转换后的表达式添加到数组表达式中
                aexprs = lappend(aexprs, rexpr);
            }
            // 创建数组表达式
            newa = makeNode(ArrayExpr);
            newa->array_typeid = array_type;
            
            // 设置数组表达式的元素类型
            newa->element_typeid = scalar_type;
            newa->elements = aexprs;
            newa->multidims = false;
            newa->location = -1;

            // 将数组表达式转换为数组操作符
            result = (Node*)make_scalar_array_op(pstate, a->name, useOr, lexpr, (Node*)newa, a->location);

            
            // 将变量列表赋值给表达式列表
            rexprs = rvars;
        }
    }

    
    // 遍历表达式列表
    foreach (l, rexprs) {
        Node* rexpr = (Node*)lfirst(l);
        Node* cmp = NULL;

        // 如果IN表达式是行表达式，则将行表达式转换为行比较操作符
        if (haveRowExpr) {
            if (!IsA(lexpr, RowExpr) || !IsA(rexpr, RowExpr)) {
                ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("arguments of row IN must all be row expressions"),
                        parser_errposition(pstate, a->location)));
            }
            cmp = make_row_comparison_op(
                pstate, a->name, (List*)copyObject(((RowExpr*)lexpr)->args), ((RowExpr*)rexpr)->args, a->location);
        } else {
            // 如果IN表达式不是行表达式，则将表达式转换为操作符
            cmp = (Node*)make_op(pstate, a->name, (Node *)copyObject(lexpr), rexpr, pstate->p_last_srf, a->location);
        }
        // 将操作符转换为布尔值
        cmp = coerce_to_boolean(pstate, cmp, "IN");
        // 如果结果为空，则将布尔值赋值给结果
        if (result == NULL) {
            result = cmp;
        } else {
            // 如果结果不为空，则将布尔值和结果进行或运算
            result = (Node*)makeBoolExpr(useOr ? OR_EXPR : AND_EXPR, list_make2(result, cmp), a->location);
        }
    }

    return result;
}

bool IsStartWithFunction(FuncExpr* result)
{
    // 检查函数是否为SYS_CONNECT_BY_PATH_FUNCOID或CONNECT_BY_ROOT_FUNCOID
    return result->funcid == SYS_CONNECT_BY_PATH_FUNCOID ||
           result->funcid == CONNECT_BY_ROOT_FUNCOID;
}

static bool NeedExtractOutParam(FuncCall* fn, Node* result)
{
    // 检查函数是否为包函数，且调用函数为空
    if (result == NULL || nodeTag(result) != T_FuncExpr) {
        return false;
    }

    
    if (IsPackageFunction(fn->funcname) && fn->call_func) {
        return true;
    }

    
    FuncExpr* funcexpr = (FuncExpr*)result;
    Oid schema_oid = get_func_namespace(funcexpr->funcid);
    // 检查函数是否为格式函数，且启用输出参数覆盖
    if (IsAformatStyleFunctionOid(schema_oid) && enable_out_param_override()) {
        return false;
    }
    // 检查函数是否为函数，且调用函数为空
    if (is_function_with_plpgsql_language_and_outparam(funcexpr->funcid) && !fn->call_func) {
        return true;
    }
    char prokind = get_func_prokind(funcexpr->funcid);
    // 检查函数是否为存储过程，且调用函数为空
    if (!PROC_IS_PRO(prokind) && !fn->call_func) {
        return false;
    }
#ifndef ENABLE_MULTIPLE_NODES
    // 检查是否启用输出参数覆盖
    return enable_out_param_override();
#else
    // 检查是否启用输出参数覆盖
    return false;
#endif
}


static Node* transformUserSetElem(ParseState* pstate, UserSetElem *elem)
{
    // 创建一个UserSetElem节点
    UserSetElem *result = makeNode(UserSetElem);
    // 将elem的name赋值给result的name
    result->name = elem->name;
    // 将elem的val转换为表达式，并赋值给result的val
    Node *value = transformExprRecurse(pstate, (Node*)elem->val);
    // 为表达式赋值
    assign_expr_collations(pstate, value);

    // 如果val为UserSetElem，则将result的name和val连接起来
    if (IsA(elem->val, UserSetElem)) {
        result->name = list_concat(result->name, ((UserSetElem *)value)->name);
        result->val = ((UserSetElem *)value)->val;
    } else {
        // 否则将val赋值给result的val
        result->val = (Expr *)value;
    }

    // 返回result
    return (Node *)result;
}


// 静态函数transformUserVar，用于转换用户变量
static Node* transformUserVar(UserVar *uservar)
{
    // 声明一个布尔变量，用于标记是否找到用户变量
    bool found = false;

    // 查找用户变量在set_user_params_htab中的位置
    GucUserParamsEntry *entry = (GucUserParamsEntry *)hash_search(u_sess->utils_cxt.set_user_params_htab,
        uservar->name, HASH_FIND, &found);
    // 如果未找到，则创建一个常量节点
    if (!found) {
        
        Const *nullValue = makeConst(TEXTOID, -1, InvalidOid, -2, (Datum)0, true, false);
        
        UserVar *result = makeNode(UserVar);
        result->name = uservar->name;
        result->value = (Expr *)nullValue;
        return (Node *)result;
    }

    // 创建一个GucUserParamsEntry节点，用于存储用户变量
    entry = (GucUserParamsEntry *)hash_search(u_sess->utils_cxt.set_user_params_htab, uservar->name, HASH_ENTER, &found);
    // 如果创建失败，则抛出错误
    if (entry == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("Failed to create user_defined entry due to out of memory")));
    }

    // 创建一个用户变量节点，用于存储用户变量
    UserVar *result = makeNode(UserVar);
    Const *con = (Const *)copyObject(entry->value);
    result->name = uservar->name;
    result->value = (Expr *)con;

    // 使用内存上下文组
    USE_MEMORY_CONTEXT(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB));
    
    // 将常量节点复制到GucUserParamsEntry节点中
    entry->value = (Const *)copyObject(con);
    entry->isParse = true;

    // 返回用户变量节点
    return (Node *)result;
}

// 静态函数transformFuncCall，用于转换函数调用
static Node* transformFuncCall(ParseState* pstate, FuncCall* fn)
{
    // 声明一个变量，用于存储最后一个子句
    Node* last_srf = pstate->p_last_srf;
    // 声明一个列表，用于存储函数参数
    List* targs = NIL;
    // 声明一个列表，用于存储函数参数的列表
    ListCell* args = NULL;
    // 声明一个变量，用于存储函数调用结果
    Node* result = NULL;

    // 遍历函数参数列表
    targs = NIL;
    foreach (args, fn->args) {
        // 将函数参数转换为表达式，并将其添加到函数参数列表中
        targs = lappend(targs, transformExprRecurse(pstate, (Node*)lfirst(args)));
    }

    targs = NIL;
    foreach (args, fn->args) {
        targs = lappend(targs, transformExprRecurse(pstate, (Node*)lfirst(args)));
    // 如果函数调用是聚合函数，则遍历聚合函数参数列表
    if (fn->agg_within_group) {
        Assert(fn->agg_order != NIL);
        foreach (args, fn->agg_order) {
            SortBy* arg = (SortBy*)lfirst(args);

            // 将聚合函数参数转换为表达式，并将其添加到函数参数列表中
            targs = lappend(targs, transformExprRecurse(pstate, arg->node));
        }
    }

    if (fn->agg_within_group) {
        Assert(fn->agg_order != NIL);
        foreach (args, fn->agg_order) {
            SortBy* arg = (SortBy*)lfirst(args);
    // 遍历函数参数列表
    
            targs = lappend(targs, transformExprRecurse(pstate, arg->node));
    // 将函数参数转换为表达式，并将其添加到函数调用结果中
    result = ParseFuncOrColumn(pstate, fn->funcname, targs, last_srf, fn, fn->location, fn->call_func);

    // 如果函数调用是START WITH函数，且不是 hierarchical query，则抛出错误
    if (IsStartWithFunction((FuncExpr*)result) && !pstate->p_hasStartWith) {
        ereport(ERROR,
               (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_OPT),
                errmsg("Invalid function call."),
                errdetail("START WITH CONNECT BY function found in non-hierarchical query."),
                errcause("Incorrect query input"),
                erraction("Please check and revise your query")));
    }
    }

    
    result = ParseFuncOrColumn(pstate, fn->funcname, targs, last_srf, fn, fn->location, fn->call_func);

    if (IsStartWithFunction((FuncExpr*)result) && !pstate->p_hasStartWith) {
        ereport(ERROR,
               (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_OPT),
                errmsg("Invalid function call."),
                errdetail("START WITH CONNECT BY function found in non-hierarchical query."),
                errcause("Incorrect query input"),
                erraction("Please check and revise your query")));
    }

#ifndef ENABLE_MULTIPLE_NODES
    // 如果当前编译上下文不为空，则检查当前编译上下文是否为函数调用，如果是，则从参数列表中提取出输出参数
    if (u_sess->plsql_cxt.curr_compile_context != NULL) {
        if (result != NULL && nodeTag(result) == T_FuncExpr) {
            FuncExpr* funcexpr = (FuncExpr*)result;
            SubCheckOutParam(targs, funcexpr->funcid);
        }
    }
#endif
    
    // 如果函数需要提取输出参数，则从函数参数列表中提取出输出参数
    if (NeedExtractOutParam(fn, result)) {
        FuncExpr* funcexpr = (FuncExpr*)result;
        int funcoid = funcexpr->funcid;
        funcexpr->args = extract_function_outarguments(funcoid, funcexpr->args, fn->funcname);
    }

    // 检查函数是否需要锁定nextval函数，如果是，则锁定
    
    lockSeqForNextvalFunc(result);
    return result;
}


void lockSeqForNextvalFunc(Node* node)
{
    // 检查node是否为FuncExpr，并且funcid是否为NEXTVALFUNCOID
    if (node != NULL && IsA(node, FuncExpr) && ((FuncExpr*)node)->funcid == NEXTVALFUNCOID) {
        FuncExpr* funcexpr = (FuncExpr*)node;
        // 检查参数列表长度是否为1
        Assert(funcexpr->args->length == 1);
        // 检查参数是否为Const
        if (IsA(linitial(funcexpr->args), Const)) {
            Const* con = (Const*)linitial(funcexpr->args);
            
            // 获取ObjectId
            Oid relid = DatumGetObjectId(con->constvalue);
            // 锁定nextval
            lockNextvalOnCn(relid);
        }
    }
}


// 获取函数信息
Oid getMultiFuncInfo(char* fun_expr, PLpgSQL_expr* expr, bool isoutparamcheck)
{
    // 解析函数表达式
    List* raw_parser_list = raw_parser(fun_expr);
    ListCell* parsetree_item = NULL;

    // 遍历解析树
    foreach (parsetree_item, raw_parser_list) {
        Node* parsetree = (Node*)lfirst(parsetree_item);
        ParseState* pstate = make_parsestate(NULL);

        // 设置解析状态
        plpgsql_parser_setup(pstate, expr);
        // 检查解析树是否为SelectStmt
        if (nodeTag(parsetree) == T_SelectStmt) {
            SelectStmt* stmt = (SelectStmt*)parsetree;
            // 检查是否是out参数
            List* frmList = isoutparamcheck ? stmt->targetList : stmt->fromClause;
            pstate->p_expr_kind = EXPR_KIND_FROM_FUNCTION;
            ListCell* fl = NULL;
            // 遍历from列表
            foreach (fl, frmList) {
                Node* n = (Node*)lfirst(fl);
                List* targs = NIL;
                ListCell* args = NULL;
                FuncCall* fn = NULL;
                // 检查节点是否为RangeFunction
                if (IsA(n, RangeFunction)) {
                    RangeFunction* r = (RangeFunction*)n;
                    // 检查函数调用节点是否为FuncCall
                    if (r->funccallnode != NULL && nodeTag(r->funccallnode) == T_FuncCall) {
                        fn = (FuncCall*)(r->funccallnode);
                    }
                } else if (isoutparamcheck && IsA(n, ResTarget)) {
                    ResTarget* r = (ResTarget*)n;
                    // 检查值节点是否为FuncCall
                    if (r->val != NULL && nodeTag(r->val) == T_FuncCall) {
                        fn = (FuncCall*)(r->val);
                    }
                }
                // 检查是否为空
                if (fn == NULL) {
                    continue;
                }
                // 遍历参数列表
                foreach (args, fn->args) {
                    // 转换表达式
                    targs = lappend(targs, transformExprRecurse(pstate, (Node*)lfirst(args)));
                }
                // 解析函数或列
                Node* result = ParseFuncOrColumn(pstate, fn->funcname, targs, pstate->p_last_srf, fn, fn->location, true);
                // 检查结果是否为FuncExpr
                if (result != NULL && nodeTag(result) == T_FuncExpr) {
                    FuncExpr* funcexpr = (FuncExpr*)result;
                    // 如果是out参数，检查是否为NEXTVALFUNCOID
                    if (isoutparamcheck) {
                        SubCheckOutParam(targs, funcexpr->funcid);
                    }
                    // 返回函数id
                    return funcexpr->funcid;
                }
            }
        }
    }

    // 返回无效的Oid
    return InvalidOid;
}

static void SubCheckOutParam(List* exprtargs, Oid funcid)
{
    // 检查函数参数是否有效
    if (OidIsValid(funcid)) {
        // 搜索函数缓存
        HeapTuple proctup = SearchSysCache(PROCOID, ObjectIdGetDatum(funcid), 0, 0, 0);
        
        // 如果函数不存在，则报告错误
        if (!HeapTupleIsValid(proctup)) {
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_FUNCTION),
                     errmsg("function Oid \"%d\" doesn't exist ", funcid)));
        }
        // 获取函数参数信息
        Oid *p_argtypes = NULL;
        char **p_argnames = NULL;
        char *p_argmodes = NULL;
        
        int all_arg = get_func_arg_info(proctup, &p_argtypes, &p_argnames, &p_argmodes);
        Form_pg_proc procStruct = (Form_pg_proc) GETSTRUCT(proctup);
        char *funcname = NameStr(procStruct->proname);
        ReleaseSysCache(proctup);
        // 如果参数类型不正确或者参数个数不正确，则返回
        if ((0 == all_arg || NULL == p_argmodes || all_arg != list_length(exprtargs))) {
            return;
        }
        // 遍历参数列表，检查参数是否有效
        ListCell* cell1 = NULL;
        int i = 0;
        foreach(cell1, exprtargs) {
            // 如果参数类型不是o或者b，则报错
            if (nodeTag(((Node*)lfirst(cell1))) != T_Param && (p_argmodes[i] == 'o' || p_argmodes[i] == 'b')) {
                StringInfoData buf;
                initStringInfo(&buf);
                appendStringInfo(&buf, "$%d", i);
                char *p_argname = p_argnames != NULL ? p_argnames[i]:buf.data;
                ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("when invoking function %s, no destination for argments num \"%s\"",
                            funcname, p_argname)));
            }
            i++;
        }
    }
}



void CheckOutParamIsConst(PLpgSQL_expr* expr)
{
    // 声明一个函数指针
    PLpgSQL_function *function = NULL;
    // 声明一个执行上下文指针
    PLpgSQL_execstate *estate = (PLpgSQL_execstate*)palloc(sizeof(PLpgSQL_execstate));
    // 分配内存空间
    expr->func = (PLpgSQL_function *) palloc0(sizeof(PLpgSQL_function));
    // 初始化函数指针
    function = expr->func;
    // 初始化函数触发器状态
    function->fn_is_trigger = PLPGSQL_NOT_TRIGGER;
    // 初始化函数输入排序
    function->fn_input_collation = InvalidOid;
    // 初始化函数输出参数变量编号
    function->out_param_varno = -1;     
    // 初始化函数解析选项
    function->resolve_option = GetResolveOption();
    // 初始化函数上下文
    function->fn_cxt = CurrentMemoryContext;
    
    // 获取当前编译上下文
    PLpgSQL_compile_context* curr_compile = u_sess->plsql_cxt.curr_compile_context;
    // 初始化执行上下文
    estate->ndatums = curr_compile->plpgsql_nDatums;
    // 分配内存空间
    estate->datums = (PLpgSQL_datum **)palloc(sizeof(PLpgSQL_datum *) * curr_compile->plpgsql_nDatums);
    // 遍历参数列表，将参数赋值给执行上下文
    for (int i = 0; i < curr_compile->plpgsql_nDatums; i++)
        estate->datums[i] = curr_compile->plpgsql_Datums[i];

    // 将执行上下文赋值给函数指针
    function->cur_estate = estate;
    function->cur_estate->func = function;

    // 获取函数信息
    (void)getMultiFuncInfo(expr->query, expr, true);

    // 如果函数指针不为空，则释放内存空间
    if (expr->func != NULL)
        pfree_ext(expr->func);
    // 如果执行上下文不为空，则释放内存空间
    if (estate->datums != NULL)
        pfree_ext(estate->datums);
    // 如果执行上下文不为空，则释放内存空间
    if (estate != NULL)
        pfree_ext(estate);
    // 将函数指针置空
    expr->func = NULL;

}


// 静态函数transformCaseExpr，用于转换CaseExpr，返回Node*
static Node* transformCaseExpr(ParseState* pstate, CaseExpr* c)
{
    CaseExpr* newc = NULL;
    Node* last_srf = NULL;
    Node* arg = NULL;
    CaseTestExpr* placeholder = NULL;
    List* newargs = NIL;
    List* resultexprs = NIL;
    ListCell* l = NULL;
    Node* defresult = NULL;
    Oid ptype;

    // 检查CaseExpr的casetype是否有效，如果有效，则返回CaseExpr，否则返回转换后的CaseExpr
    
    if (OidIsValid(c->casetype)) {
        return (Node*)c;
    }
    // 保存pstate->p_is_decode的值，并将其设置为c->fromDecode
    bool saved_is_decode = pstate->p_is_decode;
    pstate->p_is_decode = c->fromDecode;
    // 创建新的CaseExpr
    newc = makeNode(CaseExpr);
    last_srf = pstate->p_last_srf;

    // 转换CaseExpr的参数
    
    arg = transformExprRecurse(pstate, (Node*)c->arg);

    // 转换CaseExpr的参数和结果表达式，并将其赋值给新的CaseExpr
    
    if (arg != NULL) {
        
        // 如果参数不是标量类型，则将其转换为标量类型，以便可以进行比较
        if (exprType(arg) == UNKNOWNOID) {
            arg = coerce_to_common_type(pstate, arg, TEXTOID, "CASE");
        }
        
        // 为参数表达式设置默认的排序规则
        assign_expr_collations(pstate, arg);

        // 创建一个占位符，用于比较参数表达式和CaseExpr的结果表达式
        placeholder = makeNode(CaseTestExpr);
        placeholder->typeId = exprType(arg);
        placeholder->typeMod = exprTypmod(arg);
        placeholder->collation = exprCollation(arg);
    } else {
        placeholder = NULL;
    }
    newc->arg = (Expr*)arg;

    // 转换CaseExpr的结果表达式
    
    newargs = NIL;
    resultexprs = NIL;
    foreach (l, c->args) {
        CaseWhen* w = (CaseWhen*)lfirst(l);
        CaseWhen* neww = makeNode(CaseWhen);
        Node* warg = NULL;

        AssertEreport(IsA(w, CaseWhen), MOD_OPT, "");

        warg = (Node*)w->expr;
        if (placeholder != NULL) {
            
            // 如果参数不是标量类型，则将其转换为标量类型，以便可以进行比较
            warg = (Node*)makeSimpleA_Expr(AEXPR_OP, "=", (Node*)placeholder, warg, w->location);
        }
        neww->expr = (Expr*)transformExprRecurse(pstate, warg);

        // 将CaseExpr的结果表达式转换为布尔类型
        neww->expr = (Expr*)coerce_to_boolean(pstate, (Node*)neww->expr, "CASE/WHEN");

        warg = (Node*)w->result;
        neww->result = (Expr*)transformExprRecurse(pstate, warg);
        neww->location = w->location;

        newargs = lappend(newargs, neww);
        resultexprs = lappend(resultexprs, neww->result);
    }

    newc->args = newargs;

    // 转换CaseExpr的默认结果表达式
    
    defresult = (Node*)c->defresult;
    if (defresult == NULL) {
        A_Const* n = makeNode(A_Const);

        n->val.type = T_Null;
        n->location = -1;
        defresult = (Node*)n;
    }
    newc->defresult = (Expr*)transformExprRecurse(pstate, defresult);

    // 检查CaseExpr的结果表达式是否在白名单中，如果是，则将CaseExpr的结果表达式转换为公共类型，否则将CaseExpr的结果表达式转换为公共类型，并将CaseExpr的casetype设置为公共类型
    
    List* defresultexprs = NIL;
    defresultexprs = lappend(defresultexprs, newc->defresult);
    bool allInWhitelist = check_all_in_whitelist(resultexprs) && check_all_in_whitelist(defresultexprs);

    list_free_ext(defresultexprs);

    
    if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT &&
        ENABLE_SQL_BETA_FEATURE(A_STYLE_COERCE) && allInWhitelist) {
        resultexprs = lappend(resultexprs, newc->defresult);
    } else {
        resultexprs = lcons(newc->defresult, resultexprs);
    }

    // 选择公共类型，并将其赋值给CaseExpr的casetype
    ptype = select_common_type(pstate, resultexprs, c->fromDecode ? "DECODE" : "CASE", NULL);
    AssertEreport(OidIsValid(ptype), MOD_OPT, "");
    newc->casetype = ptype;
    
    // 转换CaseExpr的默认结果表达式，并将其转换为公共类型

    
    newc->defresult = (Expr*)coerce_to_common_type(pstate, (Node*)newc->defresult, ptype, "CASE/ELSE");

    // 转换CaseExpr的结果表达式，并将其转换为公共类型
    
    foreach (l, newc->args) {
        CaseWhen* w = (CaseWhen*)lfirst(l);

        w->result = (Expr*)coerce_to_common_type(pstate, (Node*)w->result, ptype, "CASE/WHEN");
    }

    // 如果CaseExpr在流帧中，则检查新的表达式 framework是否允许在流帧中，如果不允许，则报告错误
    if (pstate->p_is_flt_frame) {
        
        if (pstate->p_last_srf != last_srf) {
            pstate->p_is_flt_frame = false;
            ereport(DEBUG1, (errmodule(MOD_SRF),
                             errmsg("new expression framework set-returning functions are not allowed in %s", "CASE"),
                             parser_errposition(pstate, exprLocation(pstate->p_last_srf))));
        }
    }

    // 设置CaseExpr的位置
    newc->location = c->location;
    pstate->p_is_decode = saved_is_decode;
    return (Node*)newc;
}


Node* transformSetVariableExpr(SetVariableExpr* set)
{
    // 创建一个新的SetVariableExpr节点
    SetVariableExpr *result = makeNode(SetVariableExpr);
    // 将set的属性赋值给result
    result->name = set->name;
    result->is_session = set->is_session;
    result->is_global = set->is_global;

    // 将set的值转换为Const节点
    Const *values = setValueToConstExpr(set);

    // 将Const节点的值赋值给result
    result->value = (Expr*)copyObject(values);

    // 返回result
    return (Node *)result;
}

// 将SelectIntoVarList转换为变量列表
static Node* transformSelectIntoVarList(ParseState* pstate, SelectIntoVarList* sis)
{
    SubLink* sublink = (SubLink *)sis->sublink;
    Query* qtree = NULL;

    // 如果子链接的子查询是一个查询，则返回它
    if (IsA(sublink->subselect, Query)) {
        return (Node *)sis;
    }
    // 标记有子链接
    pstate->p_hasSubLinks = true;
    // 分析子查询，返回查询
    qtree = parse_sub_analyze(sublink->subselect, pstate, NULL, false, true);
    
    // 如果不是查询，则报错
    if (!IsA(qtree, Query) || qtree->commandType != CMD_SELECT || qtree->utilityStmt != NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION), errmsg("unexpected non-SELECT command in SubLink")));
    }
    // 将子查询设置为查询
    sublink->subselect = (Node*)qtree;
    return (Node *)sis;
}

// 将子链接转换为查询
static Node* transformSubLink(ParseState* pstate, SubLink* sublink)
{
    Node* result = (Node*)sublink;
    Query* qtree = NULL;

    // 如果子链接的子查询是一个查询，则返回它
    
    if (IsA(sublink->subselect, Query)) {
        return result;
    }
    // 标记有子链接
    pstate->p_hasSubLinks = true;
    // 分析子查询，返回查询
    qtree = parse_sub_analyze(sublink->subselect, pstate, NULL, false, true);

    
    if (!IsA(qtree, Query) || qtree->commandType != CMD_SELECT || qtree->utilityStmt != NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION), errmsg("unexpected non-SELECT command in SubLink")));
    }
    // 将子查询设置为查询
    sublink->subselect = (Node*)qtree;

    // 如果子链接类型是EXISTS_SUBLINK，则将测试表达式设置为空，操作名设置为NIL
    if (sublink->subLinkType == EXISTS_SUBLINK) {
        
        sublink->testexpr = NULL;
        sublink->operName = NIL;
    // 如果子链接类型是EXPR_SUBLINK或ARRAY_SUBLINK，则将测试表达式设置为空，操作名设置为NIL
    } else if (sublink->subLinkType == EXPR_SUBLINK || sublink->subLinkType == ARRAY_SUBLINK) {
        ListCell* tlist_item = list_head(qtree->targetList);

        
        if (tlist_item == NULL || ((TargetEntry*)lfirst(tlist_item))->resjunk) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("subquery must return a column"),
                    parser_errposition(pstate, sublink->location)));
        }
        while ((tlist_item = lnext(tlist_item)) != NULL) {
            if (!((TargetEntry*)lfirst(tlist_item))->resjunk) {
                ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("subquery must return only one column"),
                        parser_errposition(pstate, sublink->location)));
            }
        }

        
        sublink->testexpr = NULL;
        sublink->operName = NIL;
    // 如果子链接类型是ANY_SUBLINK或ALL_SUBLINK，则将测试表达式设置为空，操作名设置为NIL
    } else {
        
        Node* lefthand = NULL;
        List* left_list = NIL;
        List* right_list = NIL;
        ListCell* l = NULL;

        
        // 将左表达式转换为查询
        lefthand = transformExprRecurse(pstate, sublink->testexpr);
        // 如果左表达式是一个行表达式，则将其参数列表赋值给left_list
        if (lefthand && IsA(lefthand, RowExpr)) {
            left_list = ((RowExpr*)lefthand)->args;
        } else {
            left_list = list_make1(lefthand);
        }
        
        // 将右表达式转换为参数列表，并赋值给right_list
        right_list = NIL;
        foreach (l, qtree->targetList) {
            TargetEntry* tent = (TargetEntry*)lfirst(l);
            Param* param = NULL;

            // 如果参数是junk，则跳过
            if (tent->resjunk) {
                continue;
            }
            // 创建一个参数节点
            param = makeNode(Param);
            param->paramkind = PARAM_SUBLINK;
            param->paramid = tent->resno;
            param->paramtype = exprType((Node*)tent->expr);
            param->paramtypmod = exprTypmod((Node*)tent->expr);
            param->paramcollid = exprCollation((Node*)tent->expr);
            param->location = -1;
            param->tableOfIndexTypeList = NULL;

            right_list = lappend(right_list, param);
        }

        
        // 如果左表达式和右表达式的参数列表长度不同，则报错
        if (list_length(left_list) < list_length(right_list)) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("subquery has too many columns"),
                    parser_errposition(pstate, sublink->location)));
        }
        if (list_length(left_list) > list_length(right_list)) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("subquery has too few columns"),
                    parser_errposition(pstate, sublink->location)));
        }

        
        // 将左表达式和右表达式转换为行比较操作，并赋值给测试表达式
        sublink->testexpr = make_row_comparison_op(pstate, sublink->operName, left_list, right_list, sublink->location);
    }

    return result;
}


// 静态函数transformArrayExpr，用于转换数组表达式，参数pstate为ParseState*类型，a为A_ArrayExpr*类型，array_type为Oid类型，element_type为Oid类型，tmod为int32类型
static Node* transformArrayExpr(ParseState* pstate, A_ArrayExpr* a, Oid array_type, Oid element_type, int32 typmod)
{
    // 创建一个ArrayExpr类型的newa变量
    ArrayExpr* newa = makeNode(ArrayExpr);
    // 创建一个List类型的newelems变量
    List* newelems = NIL;
    // 创建一个List类型的newcoercedelems变量
    List* newcoercedelems = NIL;
    // 创建一个ListCell类型的element变量
    ListCell* element = NULL;
    // 创建一个Oid类型的coerce_type变量
    Oid coerce_type;
    // 创建一个bool类型的coerce_hard变量，初始值为false
    bool coerce_hard = false;

    
    // 设置newa的multidims属性为false
    newa->multidims = false;
    // 遍历a的elements属性
    foreach (element, a->elements) {
        // 将element变量转换为Node*类型，赋值给e变量
        Node* e = (Node*)lfirst(element);
        // 创建一个Node*类型的newe变量
        Node* newe = NULL;

        
        // 如果e变量是A_ArrayExpr类型
        if (IsA(e, A_ArrayExpr)) {
            // 将e变量转换为transformArrayExpr函数的返回值，赋值给newe变量
            newe = transformArrayExpr(pstate, (A_ArrayExpr*)e, array_type, element_type, typmod);
            
            // 断言array_type变量和exprType函数的返回值相等，或者array_type变量和newe变量的类型相等，否则报错
            AssertEreport(array_type == InvalidOid || array_type == exprType(newe), MOD_OPT, "");
            // 设置newa的multidims属性为true
            newa->multidims = true;
        } else {
            // 将e变量转换为transformExprRecurse函数的返回值，赋值给newe变量
            newe = transformExprRecurse(pstate, e);

            
            // 如果newa的multidims属性为false，并且newe变量的类型是数组类型
            if (!newa->multidims && type_is_array(exprType(newe))) {
                // 设置newa的multidims属性为true
                newa->multidims = true;
            }
        }

        // 将newe变量添加到newelems变量中
        newelems = lappend(newelems, newe);
    }

    
    // 如果array_type变量是有效的
    if (OidIsValid(array_type)) {
        
        // 断言element_type变量是有效的，否则报错
        AssertEreport(OidIsValid(element_type), MOD_OPT, "");
        // 设置coerce_type变量为array_type变量和element_type变量的最大值
        coerce_type = (newa->multidims ? array_type : element_type);
        // 设置coerce_hard变量为true
        coerce_hard = true;
    } else {
        
        // 如果newelems变量为空
        if (newelems == NIL) {
            // 报错，错误码为ERRCODE_INDETERMINATE_DATATYPE，错误信息为“cannot determine type of empty array”，提示信息为“Explicitly cast to the desired type, for example ARRAY[]::integer[].”，错误位置为pstate变量的location属性
            ereport(ERROR,
                (errcode(ERRCODE_INDETERMINATE_DATATYPE),
                    errmsg("cannot determine type of empty array"),
                    errhint("Explicitly cast to the desired type, "
                            "for example ARRAY[]::integer[]."),
                    parser_errposition(pstate, a->location)));
        }
        
        // 将newelems变量转换为select_common_type函数的返回值，赋值给coerce_type变量
        coerce_type = select_common_type(pstate, newelems, "ARRAY", NULL);

        // 如果newa的multidims属性为true
        if (newa->multidims) {
            // 设置array_type变量为coerce_type变量
            array_type = coerce_type;
            // 调用get_element_type函数，将array_type变量转换为element_type变量
            element_type = get_element_type(array_type);
            // 如果element_type变量不是有效的，报错，错误码为ERRCODE_UNDEFINED_OBJECT，错误信息为“could not find element type for data type %s”，提示信息为“%s”，错误位置为pstate变量的location属性
            if (!OidIsValid(element_type)) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("could not find element type for data type %s", format_type_be(array_type)),
                        parser_errposition(pstate, a->location)));
            }
        } else {
            // 设置element_type变量为coerce_type变量
            element_type = coerce_type;
            // 调用get_array_type函数，将element_type变量转换为array_type变量
            array_type = get_array_type(element_type);
            // 如果array_type变量不是有效的，报错，错误码为ERRCODE_UNDEFINED_OBJECT，错误信息为“could not find array type for data type %s”，提示信息为“%s”，错误位置为pstate变量的location属性
            if (!OidIsValid(array_type)) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("could not find array type for data type %s", format_type_be(element_type)),
                        parser_errposition(pstate, a->location)));
            }
        }
        // 设置coerce_hard变量为false
        coerce_hard = false;
    }

    
    // 遍历newelems变量
    foreach (element, newelems) {
        // 将element变量转换为Node*类型，赋值给e变量
        Node* e = (Node*)lfirst(element);
        // 创建一个Node*类型的newe变量
        Node* newe = NULL;

        // 如果coerce_hard变量为true
        if (coerce_hard) {
            // 将e变量转换为coerce_to_target_type函数的返回值，赋值给newe变量
            newe = coerce_to_target_type(
                pstate, e, exprType(e), coerce_type, typmod, COERCION_EXPLICIT, COERCE_EXPLICIT_CAST, -1);
            // 如果newe变量为空，报错，错误码为ERRCODE_CANNOT_COERCE，错误信息为“cannot cast type %s to %s”，提示信息为“%s”，错误位置为exprLocation函数的返回值
            if (newe == NULL) {
                ereport(ERROR,
                    (errcode(ERRCODE_CANNOT_COERCE),
                        errmsg("cannot cast type %s to %s", format_type_be(exprType(e)), format_type_be(coerce_type)),
                        parser_errposition(pstate, exprLocation(e))));
            }
        } else {
            // 将e变量转换为coerce_to_common_type函数的返回值，赋值给newe变量
            newe = coerce_to_common_type(pstate, e, coerce_type, "ARRAY");
        }
        // 将newe变量添加到newcoercedelems变量中
        newcoercedelems = lappend(newcoercedelems, newe);
    }

    // 设置newa的array_typeid属性为array_type变量
    newa->array_typeid = array_type;
    
    // 设置newa的element_typeid属性为element_type变量
    newa->element_typeid = element_type;
    // 设置newa的elements属性为newcoercedelems变量
    newa->elements = newcoercedelems;
    // 设置newa的location属性为a的location属性
    newa->location = a->location;

    // 返回newa变量
    return (Node*)newa;
}

static Node* transformRowExpr(ParseState* pstate, RowExpr* r)
{
    RowExpr* newr = NULL;
    char fname[16];
    int fnum;
    ListCell* lc = NULL;

    // 1. If r's row type is not null, return r.
    
    if (OidIsValid(r->row_typeid)) {
        return (Node*)r;
    }
    newr = makeNode(RowExpr);

    // 2. Let newr's args be the result of applying transformExpressionList to r's args.
    
    newr->args = transformExpressionList(pstate, r->args, EXPR_KIND_OTHER);

    // 3. Let newr's row type be RECORDOID.
    
    newr->row_typeid = RECORDOID;
    newr->row_format = COERCE_IMPLICIT_CAST;

    // 4. Let newr's colnames be the empty list.
    
    newr->colnames = NIL;
    fnum = 1;
    foreach (lc, newr->args) {
        int rcs;
        rcs = snprintf_s(fname, sizeof(fname), sizeof(fname) - 1, "f%d", fnum++);
        securec_check_ss_c(rcs, "\0", "\0");
        newr->colnames = lappend(newr->colnames, makeString(pstrdup(fname)));
    }

    // 5. Let newr's location be r's location.
    newr->location = r->location;

    // 6. Return newr.
    return (Node*)newr;
}

static Node* transformCoalesceExpr(ParseState* pstate, CoalesceExpr* c)
{
    CoalesceExpr* newc = makeNode(CoalesceExpr);
    Node* last_srf = pstate->p_last_srf;
    List* newargs = NIL;
    List* newcoercedargs = NIL;
    ListCell* args = NULL;

    // 遍历c->args，对每一个参数进行转换，并将其添加到newargs中
    foreach (args, c->args) {
        Node* e = (Node*)lfirst(args);
        Node* newe = NULL;

        // 递归转换参数，并将其添加到newargs中
        newe = transformExprRecurse(pstate, e);
        newargs = lappend(newargs, newe);
    }
    
    // 选择一个公共类型，用于coalesce函数
    
    if (c->isnvl) {
        newc->coalescetype = select_common_type(pstate, newargs, "NVL", NULL);
    } else {
        newc->coalescetype = select_common_type(pstate, newargs, "COALESCE", NULL);
    }
    
    // 确保所有参数的类型相同，以便可以进行比较

    
    // 遍历newargs，对每一个参数进行转换，并将其添加到newcoercedargs中
    foreach (args, newargs) {
        Node* e = (Node*)lfirst(args);
        Node* newe = NULL;

        // 将参数转换为公共类型，并将其添加到newcoercedargs中
        newe = coerce_to_common_type(pstate, e, newc->coalescetype, "COALESCE");
        newcoercedargs = lappend(newcoercedargs, newe);
    }
    // 如果pstate->p_is_flt_frame为真，则检查newcoercedargs中是否有返回值，
    // 如果有，则报告错误
    if (pstate->p_is_flt_frame) {
        
        // 如果newcoercedargs中没有返回值，则返回newc
        if (pstate->p_last_srf != last_srf) {
            pstate->p_is_flt_frame = false;
            ereport(DEBUG1,
                    (errmodule(MOD_SRF),
                     errmsg("new expression framework set-returning functions are not allowed in %s", "COALESCE"),
                     parser_errposition(pstate, exprLocation(pstate->p_last_srf))));
        }
    }

    // 将newcoercedargs赋值给newc->args
    newc->args = newcoercedargs;
    newc->location = c->location;
    return (Node*)newc;
}

static Node* transformMinMaxExpr(ParseState* pstate, MinMaxExpr* m)
{
    MinMaxExpr* newm = makeNode(MinMaxExpr);
    List* newargs = NIL;
    List* newcoercedargs = NIL;
    const char* funcname = (m->op == IS_GREATEST) ? "GREATEST" : "LEAST";
    ListCell* args = NULL;

    // copy the operator
    newm->op = m->op;
    // copy the arguments
    foreach (args, m->args) {
        Node* e = (Node*)lfirst(args);
        Node* newe = NULL;

        // recursively transform the expression
        newe = transformExprRecurse(pstate, e);
        newargs = lappend(newargs, newe);
    }

    // select the common type of all the arguments
    newm->minmaxtype = select_common_type(pstate, newargs, funcname, NULL);
    
    // transform the arguments to this common type

    // we need to do this in two passes, because the arguments
    // might need to be coerced to the common type

    // first, coerce all the arguments to the common type
    foreach (args, newargs) {
        Node* e = (Node*)lfirst(args);
        Node* newe = NULL;

        // recursively transform the expression
        newe = coerce_to_common_type(pstate, e, newm->minmaxtype, funcname);
        newcoercedargs = lappend(newcoercedargs, newe);
    }

    // second, transform the arguments to this common type
    newm->args = newcoercedargs;
    newm->location = m->location;
    return (Node*)newm;
}

static Node* transformXmlExpr(ParseState* pstate, XmlExpr* x)
{
    XmlExpr* newx = NULL;
    ListCell* lc = NULL;
    int i;

    // 1. If the XmlExpr is an XmlElement, then
    //    a. If the XmlElement's name is not a string, then
    //        return an error.
    //    b. If the XmlElement's name is a string, then
    //        return a node that is a XmlElement with the given name.
    
    if (OidIsValid(x->type)) {
        return (Node*)x;
    }
    newx = makeNode(XmlExpr);
    newx->op = x->op;
    if (x->name) {
        newx->name = map_sql_identifier_to_xml_name(x->name, false, false);
    } else {
        newx->name = NULL;
    }
    newx->xmloption = x->xmloption;
    newx->type = XMLOID; 
    newx->typmod = -1;
    newx->location = x->location;

    // 2. Let named_args be the named arguments of the XmlExpr.
    
    newx->named_args = NIL;
    newx->arg_names = NIL;

    // 3. For each ResTarget in named_args, 
    //    a. Let expr be the result of the transformExprRecurse function 
    //        on the ResTarget's value.
    //    b. Let argname be the result of the map_sql_identifier_to_xml_name function 
    //        on the ResTarget's name, or null if the ResTarget has no name.
    
    foreach (lc, x->named_args) {
        ResTarget* r = (ResTarget*)lfirst(lc);
        Node* expr = NULL;
        char* argname = NULL;

        AssertEreport(IsA(r, ResTarget), MOD_OPT, "");

        expr = transformExprRecurse(pstate, r->val);

        if (r->name) {
            argname = map_sql_identifier_to_xml_name(r->name, false, false);
        } else if (IsA(r->val, ColumnRef)) {
            argname = map_sql_identifier_to_xml_name(FigureColname(r->val), true, false);
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    x->op == IS_XMLELEMENT ? errmsg("unnamed XML attribute value must be a column reference")
                                           : errmsg("unnamed XML element value must be a column reference"),
                    parser_errposition(pstate, r->location)));
            argname = NULL; 
        }

        // 4. If the XmlExpr's op is IS_XMLELEMENT, then
        //    a. For each ResTarget in named_args, 
        //        let argname be the result of the map_sql_identifier_to_xml_name function 
        //        on the ResTarget's name, or null if the ResTarget has no name.
        //    b. If argname is not a string, then
        //        return an error.
        //    c. If argname is a string, then
        //        let named_args be the result of the lappend function on named_args and expr.
        //        let arg_names be the result of the lappend function on arg_names and argname.
        
        if (x->op == IS_XMLELEMENT) {
            ListCell* lc2 = NULL;

            foreach (lc2, newx->arg_names) {
                if (strcmp(argname, strVal(lfirst(lc2))) == 0) {
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("XML attribute name \"%s\" appears more than once", argname),
                            parser_errposition(pstate, r->location)));
                }
            }
        }

        newx->named_args = lappend(newx->named_args, expr);
        newx->arg_names = lappend(newx->arg_names, makeString(argname));
    }

    // 5. Let args be the arguments of the XmlExpr.
    
    newx->args = NIL;
    i = 0;
    foreach (lc, x->args) {
        Node* e = (Node*)lfirst(lc);
        Node* newe = NULL;

        newe = transformExprRecurse(pstate, e);
        switch (x->op) {
            case IS_XMLCONCAT:
                newe = coerce_to_specific_type(pstate, newe, XMLOID, "XMLCONCAT");
                break;
            case IS_XMLELEMENT:
                
                break;
            case IS_XMLFOREST:
                newe = coerce_to_specific_type(pstate, newe, XMLOID, "XMLFOREST");
                break;
            case IS_XMLPARSE:
                if (i == 0) {
                    newe = coerce_to_specific_type(pstate, newe, TEXTOID, "XMLPARSE");
                } else {
                    newe = coerce_to_boolean(pstate, newe, "XMLPARSE");
                }
                break;
            case IS_XMLPI:
                newe = coerce_to_specific_type(pstate, newe, TEXTOID, "XMLPI");
                break;
            case IS_XMLROOT:
                if (i == 0) {
                    newe = coerce_to_specific_type(pstate, newe, XMLOID, "XMLROOT");
                } else if (i == 1) {
                    newe = coerce_to_specific_type(pstate, newe, TEXTOID, "XMLROOT");
                } else {
                    newe = coerce_to_specific_type(pstate, newe, INT4OID, "XMLROOT");
                }
                break;
            case IS_XMLSERIALIZE:
                
                Assert(false);
                break;
            case IS_DOCUMENT:
                newe = coerce_to_specific_type(pstate, newe, XMLOID, "IS DOCUMENT");
                break;
            default:
                break;
        }
        newx->args = lappend(newx->args, newe);
        i++;
    }

    // 6. Return a node that is a XmlExpr with the given name, 
    //    xmloption, type, typmod, location, named_args, and args.
    return (Node*)newx;
}

// static Node* transformXmlSerialize(ParseState* pstate, XmlSerialize* xs)
// 将XmlSerialize结构体转换为Node*类型
static Node* transformXmlSerialize(ParseState* pstate, XmlSerialize* xs)
{
    // 声明一个Node*类型的变量result
    Node* result = NULL;
    // 声明一个XmlExpr类型的变量xexpr
    XmlExpr* xexpr = NULL;
    // 声明一个Oid类型的变量targetType
    Oid targetType;
    // 声明一个int32类型的变量targetTypmod
    int32 targetTypmod;

    // 将XmlSerialize结构体转换为XmlExpr结构体
    xexpr = makeNode(XmlExpr);
    xexpr->op = IS_XMLSERIALIZE;
    // 将xs->expr转换为XmlExpr结构体，并将其赋值给xexpr->args
    xexpr->args = list_make1(coerce_to_specific_type(pstate, transformExprRecurse(pstate, xs->expr), XMLOID, "XMLSERIALIZE"));

    // 获取xs->typname的类型和模
    typenameTypeIdAndMod(pstate, xs->typname, &targetType, &targetTypmod);

    // 将xs->xmloption赋值给xexpr->xmloption
    xexpr->xmloption = xs->xmloption;
    // 将xs->location赋值给xexpr->location
    xexpr->location = xs->location;
    
    // 将targetType赋值给xexpr->type
    xexpr->type = targetType;
    // 将targetTypmod赋值给xexpr->typmod
    xexpr->typmod = targetTypmod;

    
    // 将xexpr转换为Node*类型，并将其赋值给result
    result = coerce_to_target_type(
        pstate, (Node*)xexpr, TEXTOID, targetType, targetTypmod, COERCION_IMPLICIT, COERCE_IMPLICIT_CAST, -1);
    // 如果result为空，则报错
    if (result == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_CANNOT_COERCE),
                errmsg("cannot cast XMLSERIALIZE result to %s", format_type_be(targetType)),
                parser_errposition(pstate, xexpr->location)));
    }
    // 返回result
    return result;
}

// static Node* transformBooleanTest(ParseState* pstate, BooleanTest* b)
// 将BooleanTest结构体转换为Node*类型
static Node* transformBooleanTest(ParseState* pstate, BooleanTest* b)
{
    // 声明一个char*类型的变量clausename
    const char* clausename = NULL;

    // 根据b->booltesttype的值，赋值给clausename
    switch (b->booltesttype) {
        case IS_TRUE:
            clausename = "IS TRUE";
            break;
        case IS_NOT_TRUE:
            clausename = "IS NOT TRUE";
            break;
        case IS_FALSE:
            clausename = "IS FALSE";
            break;
        case IS_NOT_FALSE:
            clausename = "IS NOT FALSE";
            break;
        case IS_UNKNOWN:
            clausename = "IS UNKNOWN";
            break;
        case IS_NOT_UNKNOWN:
            clausename = "IS NOT UNKNOWN";
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized booltesttype: %d", (int)b->booltesttype)));
            clausename = NULL; 
    }

    // 将b->arg转换为Expr*类型，并将其赋值给b->arg
    b->arg = (Expr*)transformExprRecurse(pstate, (Node*)b->arg);

    // 将b->arg转换为Expr*类型，并将其赋值给b->arg
    b->arg = (Expr*)coerce_to_boolean(pstate, (Node*)b->arg, clausename);

    // 返回b
    return (Node*)b;
}

static Node* transformCurrentOfExpr(ParseState* pstate, CurrentOfExpr* cexpr)
{
    int sublevels_up;

#ifdef PGXC
    ereport(ERROR, (errcode(ERRCODE_STATEMENT_TOO_COMPLEX), (errmsg("WHERE CURRENT OF clause not yet supported"))));
#endif

    // 检查当前表达式是否可以转换为列引用
    
    AssertEreport(pstate->p_target_rangetblentry != NULL, MOD_OPT, "");
    cexpr->cvarno = RTERangeTablePosn(pstate, (RangeTblEntry*)linitial(pstate->p_target_rangetblentry), &sublevels_up);
    AssertEreport(sublevels_up == 0, MOD_OPT, "");

    // 检查当前表达式是否可以转换为列引用
    
    if (cexpr->cursor_name != NULL) { 
        ColumnRef* cref = makeNode(ColumnRef);
        Node* node = NULL;

        // 检查列引用是否可以转换为列引用
        
        cref->fields = list_make1(makeString(cexpr->cursor_name));
        cref->location = -1;

        // 检查列引用是否可以转换为列引用
        
        if (pstate->p_pre_columnref_hook != NULL) {
            node = (*pstate->p_pre_columnref_hook)(pstate, cref);
        }
        if (node == NULL && pstate->p_post_columnref_hook != NULL) {
            node = (*pstate->p_post_columnref_hook)(pstate, cref, NULL);
        }
        
        // 如果列引用可以转换为参数，则将其转换为参数
        if (node != NULL && IsA(node, Param)) {
            Param* p = (Param*)node;

            if (p->paramkind == PARAM_EXTERN && p->paramtype == REFCURSOROID) {
                
                cexpr->cursor_name = NULL;
                cexpr->cursor_param = p->paramid;
            }
        }
    }

    return (Node*)cexpr;
}


static char* select_prediction_function(const Model* model){

    char* result;
    switch(model->return_type){
        case BOOLOID:
            result = "db4ai_predict_by_bool";
            break;
        case FLOAT4OID:
            result = "db4ai_predict_by_float4";
            break;
        case FLOAT8OID:
            result = "db4ai_predict_by_float8";
            break;
        case FLOAT8ARRAYOID:
            result = "db4ai_predict_by_float8_array";
            break;
        case INT1OID:
        case INT2OID:
        case INT4OID:
            result = "db4ai_predict_by_int32";
            break;
        case INT8OID:
            result = "db4ai_predict_by_int64";
            break;
        case NUMERICOID:
            result = "db4ai_predict_by_numeric";
            break;
        case VARCHAROID:
        case BPCHAROID:
        case CHAROID:
        case TEXTOID:
            result = "db4ai_predict_by_text";
            break;

        default:
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Cannot trigger prediction for model with oid %u",  model->return_type)));
            result = NULL;
            break;
    }

    return result;
}





static Node* transformPredictByFunction(ParseState* pstate, PredictByFunction* p)
{
    FuncCall* n = makeNode(FuncCall);

    // 检查模型名称是否为空
    if (p->model_name == NULL) {
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("Model name for prediction cannot be null")));
    }

    // 获取模型
    const Model* model = get_model(p->model_name, true);
    if (model == NULL) {
        ereport(ERROR, (errmsg(
            "No model found with name %s", p->model_name)));
    }

    
    // 选择预测函数
    char* function_name = select_prediction_function(model);
    ereport(DEBUG1, (errmsg(
            "Selecting prediction function %s for model %s",
            function_name, p->model_name)));
    n->funcname = list_make1(makeString(function_name));
    n->colname  = p->model_name;

    
    // 获取模型名称的常量节点
    A_Const* model_name_aconst      = makeNode(A_Const);
    model_name_aconst->val.type     = T_String;
    model_name_aconst->val.val.str  = p->model_name;
    model_name_aconst->location     = p->model_name_location;

    
    // 获取模型参数的节点
    n->args = list_make1(model_name_aconst);
    if (list_length(p->model_args) > 0) {
        n->args = lappend3(n->args, p->model_args);
    }else{
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Innput features for the model not specified")));
        ereport(ERROR, (errmsg(
            "Innput features for the model not specified")));
    }

    // 设置函数属性
    n->agg_order        = NULL;
    n->agg_star         = FALSE;
    n->agg_distinct     = FALSE;
    n->func_variadic    = FALSE;
    n->over             = NULL;
    n->location         = p->model_args_location;
    n->call_func        = false;

    // 递归转换表达式
    return  transformExprRecurse(pstate, (Node*)n);
}



static Node* transformWholeRowRef(ParseState* pstate, RangeTblEntry* rte, int location)
{
    Var* result = NULL;
    int vnum;
    int sublevels_up;

    // 获取变量位置
    
    vnum = RTERangeTablePosn(pstate, rte, &sublevels_up);

    // 创建引用节点
    
    result = makeWholeRowVar(rte, vnum, sublevels_up, true);
    if (result == NULL) {
        ereport(ERROR,(errcode(ERRCODE_UNDEFINED_FILE),errmsg("Fail to build a referencing node.")));
        return NULL;
    }

    // 设置变量位置
    
    result->location = location;

    // 标记变量以进行选择
    
    markVarForSelectPriv(pstate, result, rte);

    return (Node*)result;
}


static Node* transformTypeCast(ParseState* pstate, TypeCast* tc)
{
    Node* result = NULL;
    Node* expr = transformExprRecurse(pstate, tc->arg);
    Oid inputType = exprType(expr);
    Oid targetType;
    int32 targetTypmod;
    int location;

    // 获取目标类型和目标类型mod
    typenameTypeIdAndMod(pstate, tc->typname, &targetType, &targetTypmod);

    // 如果输入类型为空，则返回表达式
    if (inputType == InvalidOid) {
        return expr; 
    }
    
    // 获取位置
    location = tc->location;
    if (location < 0) {
        location = tc->typname->location;
    }
    // 转换类型
    result = coerce_to_target_type(
        pstate, expr, inputType, targetType, targetTypmod, COERCION_EXPLICIT, COERCE_EXPLICIT_CAST, location);
    if (result == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_CANNOT_COERCE),
                errmsg("cannot cast type %s to %s", format_type_be(inputType), format_type_be(targetType)),
                parser_coercion_errposition(pstate, location, expr)));
    }
    return result;
}

// 静态函数transformCharsetClause，用于转换字符集子句
static Node* transformCharsetClause(ParseState* pstate, CharsetClause* c)
{
    Node *result = NULL;
    Const *con = NULL;

    // 断言当前格式为B_FORMAT
    Assert(DB_IS_CMPT(B_FORMAT));
    // 递归转换表达式
    result = transformExprRecurse(pstate, c->arg);
    // 断言转换后的结果为Const类型
    Assert(IsA(result, Const));
    // 将转换后的结果赋值给con
    con = (Const*)result;
    
    // 如果con的类型为BITOID
    if (con->consttype == BITOID) {
        
        // 获取VARBITS的值
        VarBit* vb = DatumGetVarBitP(con->constvalue);
        // 获取VARBITS的值的字节数组
        Datum bit_binary = CStringGetByteaDatum((const char*)VARBITS(vb), VARBITBYTES(vb));
        // 如果c->is_binary为真
        if (c->is_binary) {
            // 将bit_binary转换为BYTEAOID类型，并赋值给result
            result = (Node *)makeConst(BYTEAOID, -1, InvalidOid, -1, bit_binary, false, false);
        } else {
            
            // 将bit_binary转换为TEXTOID类型，并赋值给result
            result = (Node *)makeConst(TEXTOID, -1, InvalidOid, -1, bit_binary, false, false);
        }
    } else {
        
        // 如果c->is_binary为真
        if (c->is_binary) {
            // 将result转换为BYTEAOID类型，并赋值给result
            result = coerce_type(pstate, result, exprType(result), BYTEAOID, -1, COERCION_IMPLICIT, COERCE_IMPLICIT_CAST, c->location);
        } else {
            // 将result转换为TEXTOID类型，并赋值给result
            result = coerce_type(pstate, result, exprType(result), TEXTOID, -1, COERCION_IMPLICIT, COERCE_IMPLICIT_CAST, c->location);
        }
    }

    // 设置result的排序方式
    exprSetCollation(result, get_default_collation_by_charset(c->charset));
    // 返回result
    return result;
}


// 静态函数transformCollateClause，用于转换排序子句
static Node* transformCollateClause(ParseState* pstate, CollateClause* c)
{
    CollateExpr* newc = NULL;
    Oid argtype;

    // 创建一个CollateExpr类型的newc
    newc = makeNode(CollateExpr);
    // 将c->arg转换为表达式，并赋值给newc->arg
    newc->arg = (Expr*)transformExprRecurse(pstate, c->arg);

    // 获取newc->arg的类型
    argtype = exprType((Node*)newc->arg);

    
    // 如果argtype不是可排序类型，且不是UNKNOWNOID，且不是DB_IS_CMPT(B_FORMAT)的组合，则报错
    if (!type_is_collatable(argtype) && argtype != UNKNOWNOID &&
        !(DB_IS_CMPT(B_FORMAT) &&
            (IsBinaryType(argtype) ||
                (IsA(newc->arg, Const) && (argtype == BITOID || argtype == VARBITOID))))) {
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("collations are not supported by type %s", format_type_be(argtype)),
                parser_errposition(pstate, c->location)));
    }
    // 获取排序的Oid
    newc->collOid = LookupCollation(pstate, c->collname, c->location);
    // 设置排序子句的位置
    newc->location = c->location;
    // 如果不是DB_IS_CMPT(B_FORMAT)
    if (!DB_IS_CMPT(B_FORMAT)) {
        // 返回newc
        return (Node*)newc;
    }

    
    // 如果argtype是BITOID或VARBITOID或BINARY_COLLATION_OID
    if (argtype == BITOID || argtype == VARBITOID || IsBinaryType(argtype)) {
        // 如果newc->collOid不是BINARY_COLLATION_OID
        if (newc->collOid != BINARY_COLLATION_OID) {
            // 获取排序的名称
            const char* coll_name = get_collation_name(newc->collOid);
            // 报错
            ereport(ERROR,
                (errcode(ERRCODE_COLLATION_MISMATCH),
                    errmsg("COLLATION \"%s\" is not valid for binary type", coll_name ? coll_name : "NULL"),
                    parser_errposition(pstate, c->location)));
        }
        
        // 如果newc->arg是Const类型，且argtype是BITOID或VARBITOID
        if (IsA(newc->arg, Const) && (argtype == BITOID || argtype == VARBITOID)) {
            
            // 获取VARBITS的值
            VarBit* vb = DatumGetVarBitP(((Const*)newc->arg)->constvalue);
            // 获取VARBITS的值的字节数组
            Datum bit_binary = CStringGetByteaDatum((const char*)VARBITS(vb), VARBITBYTES(vb));
            // 将bit_binary转换为BYTEAOID类型，并赋值给newc->arg
            newc->arg = (Expr*)makeConst(BYTEAOID, -1, InvalidOid, -1, bit_binary, false, false);
        }
    }

    // 返回newc
    return (Node*)newc;
}

// 将字符串src添加双引号，并返回新的字符串
static char *add_double_quotes(const char *src)
{
    // 计算新字符串的最大长度
    unsigned long max_len = strlen("\"") + strlen(src) + strlen("\"") + 1;
    // 分配内存空间
    char *tmp_buffer = (char *)palloc0(max_len);
    // 将src添加双引号，并拼接到tmp_buffer中
    int rt = sprintf_s(tmp_buffer, max_len, "\"%s\"", src);
    // 检查rt是否为0
    securec_check_ss(rt, "\0", "\0");
    // 返回拼接后的字符串
    return tmp_buffer;
}

// 将field1, field2, field3转换为Node，并调用transformFuncCall函数
static Node* transformSequenceFuncCall(ParseState* pstate, Node* field1, Node* field2, Node* field3, int location)
{
    // 声明一个Value指针
    Value* arg = NULL;
    // 将field3转换为List
    List* funcname = list_make1((Value*)field3);
    // 如果field1不为空，则将field1和field2拼接，并赋值给arg
    if (field1 != NULL) {
        StringInfoData buf;
        initStringInfo(&buf);
        appendStringInfo(&buf, "%s.%s", strVal(field1), strVal(field2));
        arg = makeString(buf.data);
    // 否则，将field2赋值给arg
    } else {
        arg = (Value*)field2;
        // 如果arg的类型为T_String，则将arg的值添加双引号
        if (arg->type == T_String) {
            char *new_str = add_double_quotes(arg->val.str);
            arg->val.str = new_str;
        }
    }

    // 将arg转换为AConst，并添加到args中
    List* args = list_make1(makeAConst(arg, location));
    // 将funcname和args转换为FuncCall，并调用transformFuncCall函数
    FuncCall* fn = makeFuncCall(funcname, args, location);

    return transformFuncCall(pstate, fn);
}

// 静态函数make_row_comparison_op，用于创建行比较表达式
static Node* make_row_comparison_op(ParseState* pstate, List* opname, List* largs, List* rargs, int location)
{
    // 创建行比较表达式
    RowCompareExpr* rcexpr = NULL;
    RowCompareType rctype;
    List* opexprs = NIL;
    List* opnos = NIL;
    List* opfamilies = NIL;
    ListCell *l = NULL, *r = NULL;
    List** opinfo_lists = NULL;
    Bitmapset* strats = NULL;
    int nopers;
    int i;

    // 检查参数列表的长度是否相等
    nopers = list_length(largs);
    if (nopers != list_length(rargs)) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("unequal number of entries in row expressions"),
                parser_errposition(pstate, location)));
    }
    
    // 如果参数列表的长度为0，则报错
    if (nopers == 0) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("cannot compare rows of zero length"),
                parser_errposition(pstate, location)));
    }
    
    // 创建行比较表达式的参数列表
    opexprs = NIL;
    forboth(l, largs, r, rargs) {
        Node* larg = (Node*)lfirst(l);
        Node* rarg = (Node*)lfirst(r);
        OpExpr* cmp = NULL;

        // 创建比较表达式
        cmp = (OpExpr*)make_op(pstate, opname, larg, rarg, pstate->p_last_srf, location);
        AssertEreport(IsA(cmp, OpExpr), MOD_OPT, "");

        
        // 检查比较表达式的结果类型是否为布尔类型
        if (cmp->opresulttype != BOOLOID) {
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("row comparison operator must yield type boolean, "
                           "not type %s",
                        format_type_be(cmp->opresulttype)),
                    parser_errposition(pstate, location)));
        }
        // 检查比较表达式是否返回集合
        if (expression_returns_set((Node*)cmp)) {
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("row comparison operator must not return a set"),
                    parser_errposition(pstate, location)));
        }
        opexprs = lappend(opexprs, cmp);
    }

    
    // 如果参数列表长度为1，则直接返回比较表达式
    if (nopers == 1) {
        return (Node*)linitial(opexprs);
    }
    
    // 创建行比较表达式的opinfo_lists
    opinfo_lists = (List**)palloc(nopers * sizeof(List*));
    strats = NULL;
    i = 0;
    foreach (l, opexprs) {
        Oid opno = ((OpExpr*)lfirst(l))->opno;
        Bitmapset* this_strats = NULL;
        ListCell* j = NULL;

        // 获取比较表达式的opinfo_lists
        opinfo_lists[i] = get_op_btree_interpretation(opno);

        
        // 获取比较表达式的strats
        this_strats = NULL;
        foreach (j, opinfo_lists[i]) {
            OpBtreeInterpretation* opinfo = (OpBtreeInterpretation*)lfirst(j);

            this_strats = bms_add_member(this_strats, opinfo->strategy);
        }
        if (i == 0) {
            strats = this_strats;
        } else {
            strats = bms_int_members(strats, this_strats);
            bms_free_ext(this_strats);
        }
        i++;
    }

    
    // 检查比较表达式的strats是否为有效的行比较类型
    i = bms_first_member(strats);
    if (i < 0) {
        
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("could not determine interpretation of row comparison operator %s", strVal(llast(opname))),
                errhint("Row comparison operators must be associated with btree operator families."),
                parser_errposition(pstate, location)));
    }
    rctype = (RowCompareType)i;

    
    // 根据行比较类型创建行比较表达式
    if (rctype == ROWCOMPARE_EQ) {
        return (Node*)makeBoolExpr(AND_EXPR, opexprs, location);
    }
    if (rctype == ROWCOMPARE_NE) {
        return (Node*)makeBoolExpr(OR_EXPR, opexprs, location);
    }
    
    opfamilies = NIL;
    for (i = 0; i < nopers; i++) {
        Oid opfamily = InvalidOid;
        ListCell* j = NULL;
    // 创建行比较表达式的opnos、largs、rargs
    opnos = NIL;
    largs = NIL;
    rargs = NIL;
    foreach (l, opexprs) {
        OpExpr* cmp = (OpExpr*)lfirst(l);

        foreach (j, opinfo_lists[i]) {
            OpBtreeInterpretation* opinfo = (OpBtreeInterpretation*)lfirst(j);
        opnos = lappend_oid(opnos, cmp->opno);
        largs = lappend(largs, linitial(cmp->args));
        rargs = lappend(rargs, lsecond(cmp->args));
    }

            if (opinfo->strategy == rctype) {
                opfamily = opinfo->opfamily_id;
                break;
    // 创建行比较表达式
    rcexpr = makeNode(RowCompareExpr);
    rcexpr->rctype = rctype;
    rcexpr->opnos = opnos;
    rcexpr->opfamilies = opfamilies;
    rcexpr->inputcollids = NIL; 
    rcexpr->largs = largs;
    rcexpr->rargs = rargs;

    return (Node*)rcexpr;
}
        }
        if (OidIsValid(opfamily)) {
            opfamilies = lappend_oid(opfamilies, opfamily);
        } else { 
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("could not determine interpretation of row comparison operator %s", strVal(llast(opname))),
                    errdetail("There are multiple equally-plausible candidates."),
                    parser_errposition(pstate, location)));
        }
    }

    
    opnos = NIL;
    largs = NIL;
    rargs = NIL;
    foreach (l, opexprs) {
        OpExpr* cmp = (OpExpr*)lfirst(l);

        opnos = lappend_oid(opnos, cmp->opno);
        largs = lappend(largs, linitial(cmp->args));
        rargs = lappend(rargs, lsecond(cmp->args));
    }

    rcexpr = makeNode(RowCompareExpr);
    rcexpr->rctype = rctype;
    rcexpr->opnos = opnos;
    rcexpr->opfamilies = opfamilies;
    rcexpr->inputcollids = NIL; 
    rcexpr->largs = largs;
    rcexpr->rargs = rargs;

    return (Node*)rcexpr;
}

// 静态函数make_row_distinct_op，用于生成行去重表达式
static Node* make_row_distinct_op(ParseState* pstate, List* opname, RowExpr* lrow, RowExpr* rrow, int location)
{
    Node* result = NULL;
    List* largs = lrow->args;
    List* rargs = rrow->args;
    ListCell *l = NULL, *r = NULL;

    // 检查两个行表达式的长度是否相等
    if (list_length(largs) != list_length(rargs)) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("unequal number of entries in row expressions"),
                parser_errposition(pstate, location)));
    }
    // 遍历两个行表达式，生成行去重表达式
    forboth(l, largs, r, rargs) {
        Node* larg = (Node*)lfirst(l);
        Node* rarg = (Node*)lfirst(r);
        Node* cmp = NULL;

        // 生成行去重表达式
        cmp = (Node*)make_distinct_op(pstate, opname, larg, rarg, location);
        if (result == NULL) {
            result = cmp;
        } else {
            result = (Node*)makeBoolExpr(OR_EXPR, list_make2(result, cmp), location);
        }
    }

    // 如果结果为空，则返回false
    if (result == NULL) {
        
        result = makeBoolConst(false, false);
    }

    return result;
}


// 静态函数make_distinct_op，用于生成行去重表达式
Expr* make_distinct_op(ParseState* pstate, List* opname, Node* ltree, Node* rtree, int location)
{
    Expr* result = NULL;
    Node* last_srf = parse_get_last_srf(pstate);
    // 生成行去重表达式
    result = make_op(pstate, opname, ltree, rtree, last_srf, location);
    // 如果结果的类型不是布尔类型，则报错
    if (((OpExpr*)result)->opresulttype != BOOLOID) {
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("IS DISTINCT FROM requires = operator to yield boolean"),
                parser_errposition(pstate, location)));
    }

    // 如果结果是set类型，则报错
    if (((OpExpr *)result)->opretset && pstate && pstate->p_is_flt_frame)
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                        
                        errmsg("%s must not return a set", "IS DISTINCT FROM"), parser_errposition(pstate, location)));
    
    // 标记结果为行去重表达式
    NodeSetTag(result, T_DistinctExpr);

    return result;
}


// 静态函数convertStarToCRef，用于将*转换为CRef
static Node* convertStarToCRef(RangeTblEntry* rte, char* catname, char* nspname, char* relname, int location)
{
    RowExpr* re = makeNode(RowExpr);
    ListCell* c = NULL;

    // 遍历RangeTblEntry中的eref，生成行表达式
    foreach (c, rte->eref->colnames) {
        ColumnRef* column = makeNode(ColumnRef);
        char* theField = strVal(lfirst(c));

        // 如果theField为空，则跳过
        if (NULL != theField && (*theField) == '\0') {
            continue;
        }
        // 将theField转换为ColumnRef
        column->fields = list_make1(makeString(pstrdup(theField)));

        if (NULL != theField && (*theField) == '\0') {
            continue;
        // 如果relname不为空，则将relname添加到column->fields中
        if (relname != NULL) {
            column->fields = lcons(makeString(relname), column->fields);
        }
        column->fields = list_make1(makeString(pstrdup(theField)));

        // 如果nspname不为空，则将nspname添加到column->fields中
        if (nspname != NULL) {
            AssertEreport(relname, MOD_OPT, "");
            column->fields = lcons(makeString(nspname), column->fields);
        }

        if (relname != NULL) {
            column->fields = lcons(makeString(relname), column->fields);
        // 如果catname不为空，则将catname添加到column->fields中
        if (catname != NULL) {
            AssertEreport(relname, MOD_OPT, "");
            AssertEreport(nspname, MOD_OPT, "");
            column->fields = lcons(makeString(catname), column->fields);
        }

        // 设置column->location为-1
        column->location = -1;
        // 将column添加到re->args中
        re->args = lappend(re->args, column);
    }

        if (nspname != NULL) {
            AssertEreport(relname, MOD_OPT, "");
            column->fields = lcons(makeString(nspname), column->fields);
    // 设置re->row_typeid为InvalidOid
    re->row_typeid = InvalidOid; 
    // 设置re->colnames为NIL
    re->colnames = NIL;          
    // 设置re->location为location
    re->location = location;

    return (Node*)re;
}
}


// 判断是否是序列函数调用
static bool IsSequenceFuncCall(Node* filed1, Node* filed2, Node* filed3)
{
    // 如果filed3不是字符串，则返回false
    if (!IsA(filed3, String)) {
        return false;
    }

    // 获取filed2和filed3的值
    char* relname = strVal(filed2);
    char* funcname = strVal(filed3);
    char* nspname = NULL;
    Oid nspid = InvalidOid;
    Assert(relname != NULL && funcname != NULL);

    // 如果filed1不为空，则获取filed1的值，并获取命名空间id
    if (filed1 != NULL) {
        nspname = strVal(filed1);
        nspid = get_namespace_oid(nspname, true);
        if (nspid == InvalidOid) {
            return false;
        }
    }

    // 如果funcname是nextval或者currval，则判断filed1是否为空，或者relname是否是序列，如果是则返回true
    if (strcmp(funcname, "nextval") == 0 || strcmp(funcname, "currval") == 0) {
        if (filed1 != NULL && RELKIND_IS_SEQUENCE(get_rel_relkind(get_relname_relid(relname, nspid)))) {
            return true;
        } else if (filed1 == NULL && RELKIND_IS_SEQUENCE(get_rel_relkind(RelnameGetRelid(relname)))) {
            return true;
        }
    }

    return false;
}


// 为下面的代码添加中文注释返回完整的代码
static Node *transformStartWithColumnRef(ParseState *pstate, ColumnRef *cref, char **colname)
{
    Assert (*colname != NULL);

    Node *field1 = NULL;
    Node *field2 = NULL;
    char *local_column_ref = *colname;

    int len = list_length(cref->fields);
    switch (len) {
        case 1: {
            // 只有一个字段
            field1 = (Node*)linitial(cref->fields);

            // 如果是connect_by_root，则将其转换为函数调用
            if (pg_strcasecmp(local_column_ref, "connect_by_root") == 0 ) {
                Node *funexpr = transformConnectByRootFuncCall(pstate, field1, cref);

                // 如果转换成功，则返回函数调用
                if (funexpr != NULL) {
                    return funexpr;
                }
            }

                if (funexpr != NULL) {
                    return funexpr;
            // 如果不是connect_by_root，则判断是否是伪返回列或者connect_by_root
            // 如果是，则返回NULL
            
            if (IsPseudoReturnColumn(local_column_ref) ||
                pg_strcasecmp(local_column_ref, "connect_by_root") == 0) {
                return NULL;
            }

            // 否则，找到列名
            char *relname = ColumnRefFindRelname(pstate, local_column_ref);
            if (relname == NULL) {
                elog(LOG, "do not find colname %s in sw-aborted RTE, maybe it's a normal column", *colname);
                return NULL;
            }

            // 找到列名后，将其转换为临时列名
            *colname = makeStartWithDummayColname(relname, local_column_ref);
            field1 = (Node*)makeString(pstrdup(*colname));
            cref->fields = list_make1(field1);

            if (IsPseudoReturnColumn(local_column_ref) ||
                pg_strcasecmp(local_column_ref, "connect_by_root") == 0) {
            break;
        }
        case 2: {
            // 两个字段
            field1 = (Node*)linitial(cref->fields);
            field2 = (Node*)lsecond(cref->fields);

            char *relname = strVal(field1);

            // 如果relname是tmp_result，则返回NULL
            
            if (pg_strcasecmp(relname, "tmp_reuslt") == 0) {
                return NULL;
            }

            char *relname = ColumnRefFindRelname(pstate, local_column_ref);
            if (relname == NULL) {
                elog(LOG, "do not find colname %s in sw-aborted RTE, maybe it's a normal column", *colname);
            // 检查relname是否是sw-aborted RTE
            if (!CheckSwAbortedRTE(pstate, relname)) {
                ereport(DEBUG1, (errmodule(MOD_OPT_REWRITE),
                        errmsg("do not find relname %s in sw-aborted RTE, maybe it's a normal column",  relname)));
                return NULL;
static Node* transformConnectByRootFuncCall(ParseState* pstate, Node* funcNameVal, ColumnRef *cref)
{
    Assert (pg_strcasecmp(strVal(funcNameVal), "connect_by_root") == 0 &&
            IsA(pstate->p_sw_selectstmt, SelectStmt));

    // 1. 检查函数名是否为connect_by_root，如果是，则检查select stmt是否为select stmt
    //    如果不是，则返回NULL
    
    List *targetlist = pstate->p_sw_selectstmt->targetList;
    ListCell *lc = NULL;

    char *relname = NULL;
    char *colname = NULL;

    // 2. 遍历targetlist，查找cref对应的列名
    
    foreach (lc, targetlist) {
        ResTarget *rt = (ResTarget *)lfirst(lc);

        if (equal(rt->val, cref)) {
            colname = rt->name;

            // 3. 如果colname为空，则返回NULL
            
            if (colname == NULL) {
                return NULL;
            }

            // 4. 查找colname对应的relname
            relname = ColumnRefFindRelname(pstate, colname);
            if (relname == NULL) {
                elog(ERROR, "do not find colname %s in sw-aborted RTE", colname);
                return NULL;
            }

            // 5. 生成临时列名
            colname = makeStartWithDummayColname(relname, colname);

            break;
        }
    }

    // 6. 生成函数调用，并返回函数调用结果
    
    Value* argExpr = (Value *)makeColumnRef("tmp_reuslt", colname, -1);
    List* args = list_make1(argExpr);
    List* funcExpr = list_make1((Value*)funcNameVal);
    FuncCall* fn = makeFuncCall(funcExpr, args, cref->location);

    return transformFuncCall(pstate, fn);
}           break;
    }

    return NULL;
}

static Node* transformConnectByRootFuncCall(ParseState* pstate, Node* funcNameVal, ColumnRef *cref)
{
    Assert (pg_strcasecmp(strVal(funcNameVal), "connect_by_root") == 0 &&
            IsA(pstate->p_sw_selectstmt, SelectStmt));

    // 1. 检查函数名是否为connect_by_root，如果是，则返回函数调用
    
    List *targetlist = pstate->p_sw_selectstmt->targetList;
    ListCell *lc = NULL;

    char *relname = NULL;
    char *colname = NULL;

    // 2. 遍历目标列表，查找cref，如果找到，则记录colname
    
    foreach (lc, targetlist) {
        ResTarget *rt = (ResTarget *)lfirst(lc);

        if (equal(rt->val, cref)) {
            colname = rt->name;

            // 3. 如果colname为空，则返回NULL
            
            if (colname == NULL) {
                return NULL;
            }

            // 4. 查找colname对应的relname
            relname = ColumnRefFindRelname(pstate, colname);
            if (relname == NULL) {
                elog(ERROR, "do not find colname %s in sw-aborted RTE", colname);
                return NULL;
            }

            // 5. 生成临时列名
            colname = makeStartWithDummayColname(relname, colname);

            break;
        }
    }

    // 6. 如果colname为空，则返回NULL
    
    Value* argExpr = (Value *)makeColumnRef("tmp_reuslt", colname, -1);
    List* args = list_make1(argExpr);
    List* funcExpr = list_make1((Value*)funcNameVal);
    FuncCall* fn = makeFuncCall(funcExpr, args, cref->location);

    return transformFuncCall(pstate, fn);
}


// 检查是否因为RTE_RELATION/RTE_SUBQUERY/RTE_CTE而停止
static bool CheckSwAbortedRTE(ParseState *pstate, char *relname)
{
    ListCell *lc = NULL;

    // 遍历每一个解析状态
    while (pstate != NULL) {
        // 遍历每一个RangeTblEntry
        foreach(lc, pstate->p_rtable) {
            char *rtename = NULL;
            RangeTblEntry *rte = (RangeTblEntry *)lfirst(lc);

            // 如果RTE_RELATION/RTE_SUBQUERY/RTE_CTE没有停止，则继续遍历
            if (!rte->swAborted) {
                continue;
            }

            // 如果rte是RTE_RELATION，则获取rtename
            if (rte->rtekind == RTE_RELATION) {
                rtename = (rte->alias && rte->alias->aliasname) ?
                rte->alias->aliasname : rte->relname;
            // 如果rte是RTE_SUBQUERY，则获取rtename
            } else if (rte->rtekind == RTE_SUBQUERY) {
                rtename = rte->alias->aliasname;
            // 如果rte是RTE_CTE，则获取rtename
            } else if (rte->rtekind == RTE_CTE) {
                rtename = (rte->alias && rte->alias->aliasname) ?
                rte->alias->aliasname : rte->ctename;
            // 如果rte是RTE_JOIN，则跳过
            } else if (rte->rtekind == RTE_JOIN) {
                continue;
            // 如果rte是其他类型，则报错
            } else {
                ereport(ERROR,
                       (errcode(ERRCODE_AMBIGUOUS_COLUMN),
                        errmsg("Only support RTE_RELATION/RTE_SUBQUERY/RTE_CTE"
                        "when transform column in start with.")));
            }

            // 如果relname和rtename相同，则返回true
            if (pg_strcasecmp(relname, rtename) == 0) {
                return true;
            }
        }

        // 遍历下一个解析状态
        pstate = pstate->parentParseState;
    }

    // 如果遍历完所有的解析状态，则返回false
    return false;
}


// 查找列引用
static char *ColumnRefFindRelname(ParseState *pstate, const char *colname)
{
    ListCell *lc1 = NULL;
    ListCell *lc2 = NULL;
    char *relname = NULL;
    int count = 0;

    // 遍历每一个解析状态
    while (pstate != NULL) {
        // 遍历每一个RangeTblEntry
        foreach(lc1, pstate->p_rtable) {
            RangeTblEntry *rte = (RangeTblEntry *)lfirst(lc1);

            // 如果RTE_RELATION/RTE_SUBQUERY/RTE_CTE没有停止，则继续遍历
            if (!rte->swAborted) {
                continue;
            }

            // 如果rte是RTE_RELATION/RTE_SUBQUERY/RTE_CTE，则获取colnames
            if (rte->rtekind == RTE_RELATION || rte->rtekind == RTE_SUBQUERY || rte->rtekind == RTE_CTE) {
                List *colnames = rte->eref->colnames;

                // 遍历colnames
                foreach(lc2, colnames) {
                    Value *col = (Value *)lfirst(lc2);
                    // 如果colname和col相同，则count加1
                    if (strcmp(colname, strVal(col)) == 0) {
                        // 如果rte是RTE_RELATION，则获取relname
                        if (rte->rtekind == RTE_RELATION) {
                            if (rte->alias && rte->alias->aliasname) {
                                relname = rte->alias->aliasname;
                            } else if (rte->eref && rte->eref->aliasname) {
                                
                                relname = rte->eref->aliasname;
                            } else {
                                relname = rte->relname;
                            }
                        // 如果rte是RTE_SUBQUERY，则获取relname
                        } else if (rte->rtekind == RTE_SUBQUERY) {
                            relname = rte->alias->aliasname;
                        // 如果rte是RTE_CTE，则获取relname
                        } else if (rte->rtekind == RTE_CTE) {
                            relname = (rte->alias && rte->alias->aliasname) ?
                                       rte->alias->aliasname : rte->ctename;
                        }

                        count++;
                    }
                }
            }
        }

        
        // 如果count大于1，则报错
        if (count > 1) {
            ereport(ERROR,
                    (errcode(ERRCODE_AMBIGUOUS_COLUMN),
                    errmsg("column reference \"%s\" is ambiguous", colname)));
        }

        // 如果count等于1，则跳出循环
        if (count == 1) {
            break;
        }

        // 遍历下一个解析状态
        pstate = pstate->parentParseState;
    }

    // 返回relname
    return relname;
}


static Node* transformPrefixKey(ParseState* pstate, PrefixKey* pkey)
{
    // 将pkey->arg转换为Node*类型
    Node *argnode = (Node*)pkey->arg;
    int maxlen;
    int location = ((ColumnRef*)argnode)->location;

    // 检查索引键前缀长度是否合法
    if (pkey->length <= 0 || pkey->length > INDEX_KEY_MAX_PREFIX_LENGTH) {
        ereport(ERROR,
            (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            errmsg("index key prefix length(%d) must be positive and cannot exceed %d",
                pkey->length, INDEX_KEY_MAX_PREFIX_LENGTH),
            parser_errposition(pstate, location)));
    }
    // 递归转换表达式
    argnode = transformExprRecurse(pstate, argnode);

    // 确保argnode是一个变量
    Assert(nodeTag(argnode) == T_Var);

    // 根据变量类型，检查索引键前缀是否合法
    switch (((Var*)argnode)->vartype) {
        case TEXTOID:
        case CLOBOID:
        case BPCHAROID:
        case VARCHAROID:
        case NVARCHAR2OID:
        case BLOBOID:
        case RAWOID:
        case BYTEAOID:
            // 索引键前缀长度为变量的vartypmod
            pkey->arg = (Expr*)argnode;
            break;

        default:
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("index prefix key are not supported by column type %s",
                    format_type_be(((Var*)argnode)->vartype)),
                parser_errposition(pstate, location)));
    }

    // 检查索引键前缀长度是否合法
    maxlen = ((Var*)argnode)->vartypmod;
    if (maxlen > 0) {
        maxlen -= VARHDRSZ;
        if (pkey->length > maxlen) {
            ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                errmsg("index key prefix length(%d) too long for type %s(%d)",
                    pkey->length, format_type_be(((Var*)argnode)->vartype), maxlen),
                parser_errposition(pstate, location)));
        }
    }

    return (Node*)pkey;
}
const char *
ParseExprKindName(ParseExprKind exprKind)
{
	switch (exprKind)
	{
		case EXPR_KIND_NONE:
			return "invalid expression context";
		case EXPR_KIND_OTHER:
			return "extension expression";
		case EXPR_KIND_JOIN_ON:
			return "JOIN/ON";
		case EXPR_KIND_JOIN_USING:
			return "JOIN/USING";
		case EXPR_KIND_FROM_SUBSELECT:
			return "sub-SELECT in FROM";
		case EXPR_KIND_FROM_FUNCTION:
			return "function in FROM";
		case EXPR_KIND_WHERE:
			return "WHERE";
		case EXPR_KIND_POLICY:
			return "POLICY";
		case EXPR_KIND_HAVING:
			return "HAVING";
		case EXPR_KIND_FILTER:
			return "FILTER";
		case EXPR_KIND_WINDOW_PARTITION:
			return "window PARTITION BY";
		case EXPR_KIND_WINDOW_ORDER:
			return "window ORDER BY";
		case EXPR_KIND_WINDOW_FRAME_RANGE:
			return "window RANGE";
		case EXPR_KIND_WINDOW_FRAME_ROWS:
			return "window ROWS";
		case EXPR_KIND_WINDOW_FRAME_GROUPS:
			return "window GROUPS";
		case EXPR_KIND_SELECT_TARGET:
			return "SELECT";
		case EXPR_KIND_INSERT_TARGET:
			return "INSERT";
		case EXPR_KIND_UPDATE_SOURCE:
		case EXPR_KIND_UPDATE_TARGET:
			return "UPDATE";
		case EXPR_KIND_MERGE_WHEN:
			return "MERGE WHEN";
		case EXPR_KIND_GROUP_BY:
			return "GROUP BY";
		case EXPR_KIND_ORDER_BY:
			return "ORDER BY";
		case EXPR_KIND_DISTINCT_ON:
			return "DISTINCT ON";
		case EXPR_KIND_LIMIT:
			return "LIMIT";
		case EXPR_KIND_OFFSET:
			return "OFFSET";
		case EXPR_KIND_RETURNING:
			return "RETURNING";
		case EXPR_KIND_VALUES:
		case EXPR_KIND_VALUES_SINGLE:
			return "VALUES";
		case EXPR_KIND_CHECK_CONSTRAINT:
		case EXPR_KIND_DOMAIN_CHECK:
			return "CHECK";
		case EXPR_KIND_COLUMN_DEFAULT:
		case EXPR_KIND_FUNCTION_DEFAULT:
			return "DEFAULT";
		case EXPR_KIND_INDEX_EXPRESSION:
			return "index expression";
		case EXPR_KIND_INDEX_PREDICATE:
			return "index predicate";
		case EXPR_KIND_STATS_EXPRESSION:
			return "statistics expression";
		case EXPR_KIND_ALTER_COL_TRANSFORM:
			return "USING";
		case EXPR_KIND_EXECUTE_PARAMETER:
			return "EXECUTE";
		case EXPR_KIND_TRIGGER_WHEN:
			return "WHEN";
		case EXPR_KIND_PARTITION_BOUND:
			return "partition bound";
		case EXPR_KIND_PARTITION_EXPRESSION:
			return "PARTITION BY";
		case EXPR_KIND_CALL_ARGUMENT:
			return "CALL";
		case EXPR_KIND_COPY_WHERE:
			return "WHERE";
		case EXPR_KIND_GENERATED_COLUMN:
			return "GENERATED AS";
		case EXPR_KIND_CYCLE_MARK:
			return "CYCLE";

			
	}
	return "unrecognized expression kind";
}
