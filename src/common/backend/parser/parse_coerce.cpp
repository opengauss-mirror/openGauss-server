
#include "postgres.h"
#include "knl/knl_variable.h"
#include "utils/fmgroids.h"
#include "catalog/pg_cast.h"
#include "catalog/pg_class.h"
#include "catalog/pg_inherits_fn.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_coerce.h"
#include "parser/parse_func.h"
#include "parser/parse_relation.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "optimizer/clauses.h"
#include "utils/numeric.h"
#include "utils/guc.h"
#include "utils/guc_tables.h"
#include "mb/pg_wchar.h"
#ifdef PGXC
#include "pgxc/pgxc.h"
#endif

// 静态函数coerce_type_typmod，用于将节点node的类型转换为targetTypeId，targetTypMod，cformat，location，isExplicit，hideInputCoercion
static Node* coerce_type_typmod(Node* node, Oid targetTypeId, int32 targetTypMod, CoercionForm cformat, int location,
    bool isExplicit, bool hideInputCoercion);
// 静态函数hide_coercion_node，用于隐藏节点
static void hide_coercion_node(Node* node);
// 静态函数build_coercion_expression，用于构建节点，pathtype，funcId，targetTypeId，targetTypMod，cformat，location，isExplicit
static Node* build_coercion_expression(Node* node, CoercionPathType pathtype, Oid funcId, Oid targetTypeId,
    int32 targetTypMod, CoercionForm cformat, int location, bool isExplicit);
// 静态函数coerce_record_to_complex，用于将记录转换为复杂类型，pstate，node，targetTypeId，ccontext，cformat，location
static Node* coerce_record_to_complex(
    ParseState* pstate, Node* node, Oid targetTypeId, CoercionContext ccontext, CoercionForm cformat, int location);
// 静态函数is_complex_array，用于判断类型是否为复杂数组
static bool is_complex_array(Oid typid);
// 静态函数typeIsOfTypedTable，用于判断类型是否为指定表的类型
static bool typeIsOfTypedTable(Oid reltypeId, Oid reloftypeId);
// 静态函数choose_decode_result1_type，用于选择解码结果类型，pstate，exprs，context
static Oid choose_decode_result1_type(ParseState* pstate, List* exprs, const char* context);
// 静态函数handle_diff_category，用于处理类型差异，pstate，nextExpr，context，preferCategory，nextCategory，preferType，nextType
static void handle_diff_category(ParseState* pstate, Node* nextExpr, const char* context,
    TYPCATEGORY preferCategory, TYPCATEGORY nextCategory, Oid preferType, Oid nextType);
// 静态函数category_can_be_matched，用于判断类型类别是否可以匹配
static bool category_can_be_matched(TYPCATEGORY preferCategory, TYPCATEGORY nextCategory);
// 静态函数type_can_be_matched，用于判断类型是否可以匹配
static bool type_can_be_matched(Oid preferType, Oid nextType);
// 静态函数choose_specific_expr_type，用于选择特定表达式类型，pstate，exprs，context
static Oid choose_specific_expr_type(ParseState* pstate, List* exprs, const char* context);
// 静态函数choose_nvl_type，用于选择NVL类型，pstate，exprs，context
static Oid choose_nvl_type(ParseState* pstate, List* exprs, const char* context);
// 静态函数choose_expr_type，用于选择表达式类型，pstate，exprs，context，which_expr
static Oid choose_expr_type(ParseState* pstate, List* exprs, const char* context, Node** which_expr);
// 静态函数check_category_in_whitelist，用于检查类型类别是否在白名单中
static bool check_category_in_whitelist(TYPCATEGORY category, Oid type);
// 静态函数check_numeric_type_in_blacklist，用于检查类型是否在黑名单中
static bool check_numeric_type_in_blacklist(Oid type);
// 静态函数meet_decode_compatibility，用于检查解码类型是否兼容
static bool meet_decode_compatibility(List* exprs, const char* context);
// 静态函数meet_c_format_compatibility，用于检查C格式类型是否兼容
static bool meet_c_format_compatibility(List* exprs, const char* context);
// 静态函数meet_set_type_compatibility，用于检查SET类型是否兼容，exprs，context，retOid
static bool meet_set_type_compatibility(List* exprs, const char* context, Oid *retOid);
// 静态函数makeAConst，用于创建常量，v，location
extern Node* makeAConst(Value* v, int location);
// 静态函数CHECK_PARSE_PHRASE，用于检查上下文是否与目标字符串匹配
#define CHECK_PARSE_PHRASE(context, target) \
    (AssertMacro(sizeof(target) - 1 == strlen(target)), strncmp(context, target, sizeof(target) - 1) == 0)

// 获取有效的元素类型
inline Oid get_valid_element_type(Oid base_type)
{
    // 获取元素类型
    Oid array_typelem = get_element_type(base_type);
    // 如果元素类型无效，则抛出错误
    if (!OidIsValid(array_typelem)) {
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("argument declared \"anyarray\" is not an array but type %s", format_type_be(base_type))));
    }
    // 返回元素类型
    return array_typelem;
}


// 获取有效的数组类型
inline Oid get_valid_array_type(Oid elem_type)
{
    // 获取数组类型
    Oid array_typeid = get_array_type(elem_type);
    // 如果数组类型无效，则抛出错误
    if (!OidIsValid(array_typeid)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("could not find range type for data type %s", format_type_be(elem_type))));
    }
    // 返回数组类型
    return array_typeid;
}


// 检查字符类型是否可以转换
static bool check_varchar_coerce(Node* node, Oid target_type_id, int32 target_typ_mod)
{
    // 如果目标类型不是VARCHAROID或者BPCHAROID，或者表达式的类型不是-1，则返回false
    if ((target_type_id != VARCHAROID && target_type_id != BPCHAROID)|| exprTypmod(node) != -1) {
        return false;
    }
    // 如果节点不是Const，则返回false
    if (!IsA(node, Const)) {
        return false;
    }
    Const *con = (Const*)node;

    // 如果常量是null，则返回false
    if (con->constisnull) {
        return false;
    }
    // 如果目标类型是VARCHAROID，则检查常量值的长度是否小于目标类型的类型长度
    if (target_type_id == VARCHAROID) {
        VarChar *source = DatumGetVarCharPP(con->constvalue);
        int32 len;
        int32 maxlen;
        len = VARSIZE_ANY_EXHDR(source);
        maxlen = target_typ_mod;
        
        if (maxlen < 0 || len <= maxlen) {
            return true;
        }
        return false;
    // 如果目标类型是BPCHAROID，则检查常量值的长度是否小于目标类型的类型长度
    } else if (target_type_id == BPCHAROID) {
        BpChar *source = DatumGetBpCharPP(con->constvalue);
        int32 len;
        int32 maxlen = 0;
        maxlen = target_typ_mod;
        if (maxlen < (int32)VARHDRSZ) {
            return true;
        }
        maxlen -= VARHDRSZ;
        len = VARSIZE_ANY_EXHDR(source);
        if (len == maxlen) {
            return true;
        }
        return false;
    } else {
        return false;
    }
}

// 转换类型
Node* coerce_to_target_type(ParseState* pstate, Node* expr, Oid exprtype, Oid targettype, int32 targettypmod,
    CoercionContext ccontext, CoercionForm cformat, int location)
{
    Node* result = NULL;
    Node* origexpr = NULL;

    // 如果不能转换，则返回NULL
    if (!can_coerce_type(1, &exprtype, &targettype, ccontext)) {
        return NULL;
    }

    
    // 保存原始表达式
    origexpr = expr;
    // 循环检查CollateExpr，直到找到原始表达式
    while (expr && IsA(expr, CollateExpr)) {
        expr = (Node*)((CollateExpr*)expr)->arg;
    }

    // 转换类型
    result = coerce_type(pstate, expr, exprtype, targettype, targettypmod, ccontext, cformat, location);

    
    // 如果启用iud融合，并且是插入目标，并且是VARCHAROID或者BPCHAROID，并且常量值的长度小于目标类型的类型长度，则将常量值转换为VARCHAROID或者BPCHAROID
    if (u_sess->attr.attr_common.enable_iud_fusion && pstate
        && !pstate->p_joinlist && pstate->p_expr_kind == EXPR_KIND_INSERT_TARGET && result != NULL
        && check_varchar_coerce(result, targettype, targettypmod)) {
        Const* cons = (Const*)result;
        cons->consttypmod = DatumGetInt32((targettypmod));
        cons->constcollid = InvalidOid;
        cons->location = -1;
        result = (Node*)cons;
    } else {
        // 否则，将类型转换为指定类型，并设置类型长度
        result = coerce_type_typmod(result,
            targettype,
            targettypmod,
            cformat,
            location,
            (cformat != COERCE_IMPLICIT_CAST),
            (result != expr && !IsA(result, Const)));
    }
#ifdef PGXC
    
    // 如果连接是 coordinator，则将原始表达式转换为CollateExpr，并将其添加到表达式列表中
    if (IsConnFromCoord())
#endif
        if (expr != origexpr) {
            
            CollateExpr* coll = (CollateExpr*)origexpr;
            CollateExpr* newcoll = makeNode(CollateExpr);

            newcoll->arg = (Expr*)result;
            newcoll->collOid = coll->collOid;
            newcoll->location = coll->location;
            result = (Node*)newcoll;
        }

    // 返回转换后的结果
    return result;
}

Node* coerce_to_target_charset(Node* expr, int target_charset, Oid target_type, int32 target_typmod, Oid target_collation)
{
    FuncExpr* fexpr = NULL;
    Node* result = NULL;
    List* args = NIL;
    Const* cons = NULL;
    int exprcharset;
    Oid exprtype;

    // 如果目标编码为PG_INVALID_ENCODING，则返回表达式
    if (target_charset == PG_INVALID_ENCODING) {
        return expr;
    }

    // 获取表达式的编码
    exprcharset = exprCharset((Node*)expr);
    // 如果表达式的编码为PG_INVALID_ENCODING，则设置表达式的编码为数据库编码
    if (exprcharset == PG_INVALID_ENCODING) {
        exprcharset = GetDatabaseEncoding();
    }
    // 如果表达式的编码和目标编码相同，则返回表达式
    if (exprcharset == target_charset) {
        return expr;
    }

    // 检查目标类型是否支持多编码
    check_type_supports_multi_charset(target_type, false);
    // 获取表达式的类型
    exprtype = exprType(expr);
    
    // 如果表达式的类型和类型编码相同，则返回表达式
    if (exprtype != UNKNOWNOID && !OidIsValid(get_typcollation(exprtype))) {
        return expr;
    }
    
    // 创建一个函数表达式，用于转换编码
    args = list_make1(coerce_type(
        NULL, expr, exprtype, TEXTOID, -1, COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST, exprLocation(expr)));
    
    // 创建一个常量，用于存储表达式的编码
    
    const char* expr_charset_name = pg_encoding_to_char(exprcharset);
    const char* target_charset_name = pg_encoding_to_char(target_charset);
    cons = makeConst(NAMEOID, -1, InvalidOid, sizeof(const char*), NameGetDatum(expr_charset_name), false, true);
    args = lappend(args, cons);

    // 创建一个常量，用于存储目标编码
    
    cons = makeConst(NAMEOID, -1, InvalidOid, sizeof(const char*), NameGetDatum(target_charset_name), false, true);
    args = lappend(args, cons);

    // 创建一个函数表达式，用于转换编码
    fexpr = makeFuncExpr(CONVERTFUNCOID, TEXTOID, args, target_collation, InvalidOid, COERCE_IMPLICIT_CAST);
    
    // 如果目标类型为UNKNOWNOID，则返回函数表达式
    
    if (target_type == UNKNOWNOID) {
        result = (Node*)fexpr;
    } else {
        // 否则，将函数表达式转换为目标类型，并返回
        result = coerce_type(NULL, (Node*)fexpr, TEXTOID, target_type, target_typmod,
            COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST, exprLocation(expr));
    }
    
    // 如果表达式是常量或者重标记类型，则重新计算常量值
    
    exprSetCollation(result, target_collation);
    if (IsA(expr, Const) || IsA(expr, RelabelType)) {
        result = eval_const_expression_value(NULL, result, NULL);
    }
    return result;
}


Node *type_transfer(Node *node, Oid atttypid, bool isSelect)
{
    Node *result = NULL;
    Const *con = (Const *)node;
    // 如果常量值为空，则返回原值
    if (con->constisnull) {
        return node;
    }

    // 根据属性类型，进行类型转换
    switch (atttypid) {
        case BOOLOID:
        case INT1OID:
        case INT2OID:
        case INT4OID:
        case INT8OID:
            // 将常量值转换为INT8OID类型
            result = coerce_type(NULL, node, con->consttype,
                INT8OID, -1, COERCION_IMPLICIT, COERCE_IMPLICIT_CAST, -1);
            break;
        case FLOAT4OID:
        case FLOAT8OID:
        case NUMERICOID:
            // 将常量值转换为FLOAT8OID类型
            result = coerce_type(NULL, node, con->consttype,
                FLOAT8OID, -1, COERCION_IMPLICIT, COERCE_IMPLICIT_CAST, -1);
            break;
        case BITOID:
            // 将常量值转换为BITOID类型
            result = coerce_type(NULL, node, con->consttype,
                BITOID, -1, COERCION_IMPLICIT, COERCE_IMPLICIT_CAST, -1);
            break;
        case VARBITOID:
            // 将常量值转换为VARBITOID类型
            result = coerce_type(NULL, node, con->consttype,
                VARBITOID, -1, COERCION_IMPLICIT, COERCE_IMPLICIT_CAST, -1);
            break;
        default:
            // 如果isSelect为真，则返回原值
            if (isSelect) {
                result = node;
            } else {
                // 获取表达式的编码
                Oid collid = exprCollation(node);
                // 将常量值转换为TEXTOID类型，并设置编码
                result = coerce_type(NULL, node, con->consttype, TEXTOID, -1, COERCION_IMPLICIT, COERCE_IMPLICIT_CAST, -1);
                // 如果表达式的编码有效，则设置表达式的编码
                if (OidIsValid(collid)) {
                    exprSetCollation(result, collid);
                }
            }
            break;
    }

    return result;
}


Node *const_expression_to_const(Node *node)
{
    Node *result = NULL;
    Const *con = (Const *)node;

    // 如果节点不是Const类型，则报错
    if (nodeTag(node) != T_Const) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_OPERATION),
                errmsg("The value of a user_defined variable must be convertible to a constant.")));
    }

    // 将常量表达式转换为常量，并返回常量表达式的值
    
    // 将常量表达式转换为常量
    result = type_transfer(node, con->consttype, false);
    // 返回常量表达式的值
    return eval_const_expression_value(NULL, result, NULL);
}
static Datum stringTypeDatum_with_collation(Type tp, char* string, int32 atttypmod, bool can_ignore, Oid collation)
{
    // 定义一个Datum类型的变量
    Datum result; 
    // 获取collation的编码
    int tmp_encoding = get_valid_charset_by_collation(collation);
    // 获取数据库的编码
    int db_encoding = GetDatabaseEncoding();

    // 如果编码相同，则直接返回stringTypeDatum函数的结果
    if (tmp_encoding == db_encoding) {
        return stringTypeDatum(tp, string, atttypmod, can_ignore);
    }

    // 切换编码
    DB_ENCODING_SWITCH_TO(tmp_encoding);
    // 调用stringTypeDatum函数，并将结果赋值给result
    result = stringTypeDatum(tp, string, atttypmod, can_ignore);
    // 切换回数据库的编码
    DB_ENCODING_SWITCH_BACK(db_encoding);
    // 返回result
    return result;
}

// 函数coerce_type()用于将节点node的类型转换为targetTypeId，targetTypeMod为targetTypeId的类型mod，
// ccontext为转换上下文，cformat为转换格式，location为位置信息，返回转换后的节点。
Node* coerce_type(ParseState* pstate, Node* node, Oid inputTypeId, Oid targetTypeId, int32 targetTypeMod,
    CoercionContext ccontext, CoercionForm cformat, int location)
{
    Node* result = NULL;
    CoercionPathType pathtype;
    Oid funcId;

    // 如果targetTypeId等于inputTypeId或者node为空，则直接返回node
    if (targetTypeId == inputTypeId || node == NULL) {
        
        return node;
    }
    // 如果targetTypeId等于ANYOID、ANYELEMENTOID或者ANYNONARRAYOID，则直接返回node
    if (targetTypeId == ANYOID || targetTypeId == ANYELEMENTOID || targetTypeId == ANYNONARRAYOID) {
        
        return node;
    }
    // 如果targetTypeId等于ANYARRAYOID、ANYENUMOID或者ANYRANGEOID，则判断inputTypeId是否为UNKNOWNOID，如果是，则将node重新标记为baseTypeId，
    // 否则，如果baseTypeId不等于inputTypeId，则将node重新标记为baseTypeId，否则，直接返回node
    if (targetTypeId == ANYARRAYOID || targetTypeId == ANYENUMOID || targetTypeId == ANYRANGEOID) {
        
        if (inputTypeId != UNKNOWNOID) {
            Oid baseTypeId = getBaseType(inputTypeId);
            if (baseTypeId != inputTypeId) {
                RelabelType* r = makeRelabelType((Expr*)node, baseTypeId, -1, InvalidOid, cformat);
                r->location = location;
                return (Node*)r;
            }
            
            return node;
        }
    }

    // 如果inputTypeId等于UNKNOWNOID，并且node是Const类型，则将node重新标记为Const类型，
    // 并且将node的consttype、consttypmod、constcollid、constlen、constbyval、constisnull、cursor_data.cur_dno、location、constvalue、constisshared、constisnullptr、constnoexport、constisreplicated、constisvirtual、constisreadonly、constisstrict、constisautoassigned、constiswithin
    // 以及constiswithinout赋值给新的Const类型，如果baseTypeId不等于targetTypeId，则将新的Const类型重新标记为baseTypeId，
    // 并且将新的Const类型重新标记为baseTypeId，ReleaseSysCache(targetType)，返回新的Const类型，否则，返回node
    if (inputTypeId == UNKNOWNOID && IsA(node, Const)) {
        
        Const* con = (Const*)node;
        Const* newcon = makeNode(Const);
        Oid baseTypeId;
        int32 baseTypeMod;
        int32 inputTypeMod;
        Type targetType;
        ParseCallbackState pcbstate;

        
        baseTypeMod = targetTypeMod;
        baseTypeId = getBaseTypeAndTypmod(targetTypeId, &baseTypeMod);

        
        if (baseTypeId == INTERVALOID) {
            inputTypeMod = baseTypeMod;
        } else {
            inputTypeMod = -1;
        }

        targetType = typeidType(baseTypeId);

        newcon->consttype = baseTypeId;
        newcon->consttypmod = inputTypeMod;
        if (OidIsValid(GetCollationConnection()) &&
            IsSupportCharsetType(baseTypeId)) {
            newcon->constcollid = GetCollationConnection();
        } else {
            newcon->constcollid = typeTypeCollation(targetType);
        }
        newcon->constlen = typeLen(targetType);
        newcon->constbyval = typeByVal(targetType);
        newcon->constisnull = con->constisnull;
        newcon->cursor_data.cur_dno = -1;

        
        newcon->location = con->location;

        
        setup_parser_errposition_callback(&pcbstate, pstate, con->location);

        
        if (!con->constisnull) {
            newcon->constvalue = stringTypeDatum_with_collation(targetType, DatumGetCString(con->constvalue),
                inputTypeMod, pstate != NULL && pstate->p_has_ignore, con->constcollid);
        } else {
            newcon->constvalue =
                stringTypeDatum(targetType, NULL, inputTypeMod, pstate != NULL && pstate->p_has_ignore);
        }

        cancel_parser_errposition_callback(&pcbstate);

        result = (Node*)newcon;

        
        if (baseTypeId != targetTypeId) {
            result = coerce_to_domain(result, baseTypeId, baseTypeMod, targetTypeId, cformat, location, false, false);
        }

        ReleaseSysCache(targetType);

        return result;
    }
    // 检查是否是参数，并且是否有pstate和p_coerce_param_hook
    if (IsA(node, Param) && pstate != NULL && pstate->p_coerce_param_hook != NULL) {
        
        // 调用p_coerce_param_hook函数，尝试进行转换
        result = (*pstate->p_coerce_param_hook)(pstate, (Param*)node, targetTypeId, targetTypeMod, location);
        if (result != NULL) {
            return result;
        }
    }
    // 检查是否是CollateExpr
    if (IsA(node, CollateExpr)) {
        
        // 获取CollateExpr节点
        CollateExpr* coll = (CollateExpr*)node;
        // 创建新的CollateExpr节点
        CollateExpr* newcoll = makeNode(CollateExpr);

        // 调用coerce_type函数，尝试进行转换
        newcoll->arg = (Expr*)coerce_type(
            pstate, (Node*)coll->arg, inputTypeId, targetTypeId, targetTypeMod, ccontext, cformat, location);
        newcoll->collOid = coll->collOid;
        newcoll->location = coll->location;
        return (Node*)newcoll;
    }

    // 如果输入类型是未知类型，并且上下文是隐式转换，则将上下文设置为赋值
    if (UNKNOWNOID == inputTypeId && COERCION_IMPLICIT == ccontext) {
        
        ccontext = COERCION_ASSIGNMENT;
    }
    // 调用find_coercion_pathway函数，尝试进行转换
    pathtype = find_coercion_pathway(targetTypeId, inputTypeId, ccontext, &funcId);
    if (pathtype != COERCION_PATH_NONE) {
        if (pathtype != COERCION_PATH_RELABELTYPE) {
            
            // 获取基础类型和类型修改
            Oid baseTypeId;
            int32 baseTypeMod;

            baseTypeMod = targetTypeMod;
            baseTypeId = getBaseTypeAndTypmod(targetTypeId, &baseTypeMod);

            // 调用build_coercion_expression函数，尝试进行转换
            result = build_coercion_expression(
                node, pathtype, funcId, baseTypeId, baseTypeMod, cformat, location, (cformat != COERCE_IMPLICIT_CAST));

            
            // 如果目标类型和基础类型不同，则调用coerce_to_domain函数，尝试进行转换
            if (targetTypeId != baseTypeId)
                result = coerce_to_domain(result,
                    baseTypeId,
                    baseTypeMod,
                    targetTypeId,
                    cformat,
                    location,
                    true,
                    exprIsLengthCoercion(result, NULL));
        } else {
            
            // 调用coerce_to_domain函数，尝试进行转换
            result = coerce_to_domain(node, InvalidOid, -1, targetTypeId, cformat, location, false, false);
            if (result == node) {
                
                // 创建RelabelType节点，尝试进行转换
                RelabelType* r = makeRelabelType((Expr*)result, targetTypeId, -1, InvalidOid, cformat);

                r->location = location;
                result = (Node*)r;
            }
        }
        return result;
    }
    // 如果输入类型是记录，并且目标类型是复杂类型，则调用coerce_record_to_complex函数，尝试进行转换
    if (inputTypeId == RECORDOID && ISCOMPLEX(targetTypeId)) {
        
        return coerce_record_to_complex(pstate, node, targetTypeId, ccontext, cformat, location);
    }
    // 如果目标类型是记录，并且输入类型是复杂类型，则直接返回
    if (targetTypeId == RECORDOID && ISCOMPLEX(inputTypeId)) {
        
        
        return node;
    }
#ifdef NOT_USED
    // 如果输入类型是记录数组，并且目标类型是复杂类型，则调用coerce_record_to_complex函数，尝试进行转换
    if (inputTypeId == RECORDARRAYOID && is_complex_array(targetTypeId)) {
        
        
    }
#endif
    // 如果目标类型是记录数组，并且输入类型是复杂类型，则直接返回
    if (targetTypeId == RECORDARRAYOID && is_complex_array(inputTypeId)) {
        
        
        return node;
    }
    // 如果输入类型继承自目标类型，或者输入类型是表类型，则调用convert_rowtype函数，尝试进行转换
    if (typeInheritsFrom(inputTypeId, targetTypeId) || typeIsOfTypedTable(inputTypeId, targetTypeId)) {
        
        ConvertRowtypeExpr* r = makeNode(ConvertRowtypeExpr);

        r->arg = (Expr*)node;
        r->resulttype = targetTypeId;
        r->convertformat = cformat;
        r->location = location;
        return (Node*)r;
    }

    // 如果目标类型是任意集合，并且输入类型是集合类型，则直接返回
    if (targetTypeId == ANYSETOID && type_is_set(inputTypeId)) {
        return node;
    }

    // 如果输入类型是集合，并且目标类型是集合，则直接返回
    if (type_is_set(inputTypeId) && type_is_set(targetTypeId)) {
        return node;
    }

    
    // 如果转换失败，则报错
    ereport(ERROR,
        (errcode(ERRCODE_UNDEFINED_FUNCTION),
            errmsg("failed to find conversion function from %s to %s",
                format_type_be(inputTypeId),
                format_type_be(targetTypeId))));
    return NULL; 
}


// 检查类型是否可以强制转换
bool can_coerce_type(int nargs, Oid* input_typeids, Oid* target_typeids, CoercionContext ccontext)
{
    bool have_generics = false;
    int i;

    // 检查每一个参数是否可以被强制转换
    
    for (i = 0; i < nargs; i++) {
        Oid inputTypeId = input_typeids[i];
        Oid targetTypeId = target_typeids[i];
        CoercionPathType pathtype;
        Oid funcId;

        // 如果输入类型和目标类型相同，则跳过
        
        if (inputTypeId == targetTypeId) {
            continue;
        }

        // 如果目标类型为ANYOID，则跳过
        
        if (targetTypeId == ANYOID) {
            continue;
        }

        // 如果目标类型为泛型类型，则跳过
        
        if (IsPolymorphicType(targetTypeId)) {
            have_generics = true; 
            continue;
        }

        // 如果输入类型为UNKNOWNOID，则跳过
        
        if (inputTypeId == UNKNOWNOID) {
            continue;
        }

        // 查找强制转换路径
        
        pathtype = find_coercion_pathway(targetTypeId, inputTypeId, ccontext, &funcId);
        if (pathtype != COERCION_PATH_NONE) {
            continue;
        }

        // 如果输入类型为RECORDOID，且目标类型为复数类型，则跳过
        
        if (inputTypeId == RECORDOID && ISCOMPLEX(targetTypeId)) {
            continue;
        }

        // 如果目标类型为RECORDOID，且输入类型为复数类型，则跳过
        
        if (targetTypeId == RECORDOID && ISCOMPLEX(inputTypeId)) {
            continue;
        }

        // 如果输入类型为RECORDARRAYOID，且目标类型为复数数组类型，则跳过
#ifdef NOT_USED 

        
        if (inputTypeId == RECORDARRAYOID && is_complex_array(targetTypeId)) {
            continue;
        }
#endif

        // 如果目标类型为RECORDARRAYOID，且输入类型为复数数组类型，则跳过
        
        if (targetTypeId == RECORDARRAYOID && is_complex_array(inputTypeId)) {
            continue;
        }

        // 如果输入类型继承自目标类型，或者输入类型是TypedTable类型，则跳过
        
        if (typeInheritsFrom(inputTypeId, targetTypeId) || typeIsOfTypedTable(inputTypeId, targetTypeId)) {
            continue;
        }

        // 如果目标类型为ANYSETOID，且输入类型为集合类型，则跳过
        
        if (targetTypeId == ANYSETOID && type_is_set(inputTypeId)) {
            continue;
        }
        
        // 如果输入类型和目标类型都是集合类型，则跳过
        
        if (type_is_set(targetTypeId) && type_is_set(inputTypeId)) {
            continue;
        }

        // 如果不能被强制转换，则返回false
        
        return false;
    }

    // 如果存在泛型类型，则检查是否可以强制转换
    
    if (have_generics) {
        if (!check_generic_type_consistency(input_typeids, target_typeids, nargs)) {
            return false;
        }
    }

    return true;
}


Node* coerce_to_domain(Node* arg, Oid baseTypeId, int32 baseTypeMod, Oid typeId, CoercionForm cformat, int location,
    bool hideInputCoercion, bool lengthCoercionDone)
{
    CoerceToDomain* result = NULL;

    
    if (baseTypeId == InvalidOid) {
        baseTypeId = getBaseTypeAndTypmod(typeId, &baseTypeMod);
    }

    
    if (baseTypeId == typeId) {
        return arg;
    }

    
    if (hideInputCoercion) {
        hide_coercion_node(arg);
    }

    
    if (!lengthCoercionDone) {
        if (baseTypeMod >= 0) {
            arg = coerce_type_typmod(
                arg, baseTypeId, baseTypeMod, COERCE_IMPLICIT_CAST, location, (cformat != COERCE_IMPLICIT_CAST), false);
        }
    }

    
    result = makeNode(CoerceToDomain);
    result->arg = (Expr*)arg;
    result->resulttype = typeId;
    result->resulttypmod = -1; 
    
    result->coercionformat = cformat;
    result->location = location;

    return (Node*)result;
}


static Node* coerce_type_typmod(Node* node, Oid targetTypeId, int32 targetTypMod, CoercionForm cformat, int location,
    bool isExplicit, bool hideInputCoercion)
{
    CoercionPathType pathtype;
    Oid funcId;

    
    if (targetTypMod < 0 || targetTypMod == exprTypmod(node)) {
        return node;
    }

    pathtype = find_typmod_coercion_function(targetTypeId, &funcId);

    if (pathtype != COERCION_PATH_NONE) {
        Oid node_collation = exprCollation(node);
        
        if (hideInputCoercion) {
            hide_coercion_node(node);
        }
        node = build_coercion_expression(
            node, pathtype, funcId, targetTypeId, targetTypMod, cformat, location, isExplicit);
        if (OidIsValid(node_collation)) {
            exprSetInputCollation(node, node_collation);
            exprSetCollation(node, node_collation);
        }
    }

    return node;
}


static void hide_coercion_node(Node* node)
{
    // 根据节点类型，设置不同的隐藏格式
    if (IsA(node, FuncExpr)) {
        // 设置函数表达式的隐藏格式为隐式转换
        ((FuncExpr*)node)->funcformat = COERCE_IMPLICIT_CAST;
    } else if (IsA(node, RelabelType)) {
        // 设置重命名类型的隐藏格式为隐式转换
        ((RelabelType*)node)->relabelformat = COERCE_IMPLICIT_CAST;
    } else if (IsA(node, CoerceViaIO)) {
        // 设置通过IO的隐藏格式为隐式转换
        ((CoerceViaIO*)node)->coerceformat = COERCE_IMPLICIT_CAST;
    } else if (IsA(node, ArrayCoerceExpr)) {
        // 设置数组转换的隐藏格式为隐式转换
        ((ArrayCoerceExpr*)node)->coerceformat = COERCE_IMPLICIT_CAST;
    } else if (IsA(node, ConvertRowtypeExpr)) {
        // 设置转换行的隐藏格式为隐式转换
        ((ConvertRowtypeExpr*)node)->convertformat = COERCE_IMPLICIT_CAST;
    } else if (IsA(node, RowExpr)) {
        // 设置行的隐藏格式为隐式转换
        ((RowExpr*)node)->row_format = COERCE_IMPLICIT_CAST;
    } else if (IsA(node, CoerceToDomain)) {
        // 设置到域的隐藏格式为隐式转换
        ((CoerceToDomain*)node)->coercionformat = COERCE_IMPLICIT_CAST;
    } else {
        // 报告未识别的节点类型
        ereport(
            ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized node type: %d", (int)nodeTag(node))));
    }
}


static Node* build_coercion_expression(Node* node, CoercionPathType pathtype, Oid funcId, Oid targetTypeId,
    int32 targetTypMod, CoercionForm cformat, int location, bool isExplicit)
{
    int nargs = 0;

    // 如果funcId是有效的，则获取函数信息
    if (OidIsValid(funcId)) {
        HeapTuple tp;
        Form_pg_proc procstruct;

        tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcId));
        if (!HeapTupleIsValid(tp)) {
            ereport(
                ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", funcId)));
        }
        procstruct = (Form_pg_proc)GETSTRUCT(tp);

        
        
        Assert(procstruct->pronargs <= FUNC_MAX_ARGS_INROW);

        
        AssertEreport(!procstruct->proretset, MOD_OPT, "function is not return set");
        AssertEreport(!procstruct->proisagg, MOD_OPT, "function is not agg");
        AssertEreport(!procstruct->proiswindow, MOD_OPT, "function is not window function");
        nargs = procstruct->pronargs;
        AssertEreport((nargs >= 1 && nargs <= 3), MOD_OPT, "The number of parameters in the function is incorrect.");
        AssertEreport((nargs < 2 || procstruct->proargtypes.values[1] == INT4OID),
            MOD_OPT,
            "The number or type of parameters in the function is incorrect.");
        AssertEreport((nargs < 3 || procstruct->proargtypes.values[2] == BOOLOID),
            MOD_OPT,
            "The number or type of parameters in the function is incorrect.");
        ReleaseSysCache(tp);
    }

    // 根据pathtype的值，构建不同的转换表达式
    if (pathtype == COERCION_PATH_FUNC) {
        
        FuncExpr* fexpr = NULL;
        List* args = NIL;
        Const* cons = NULL;

        AssertEreport(OidIsValid(funcId), MOD_OPT, "The OID of the function is invalid.");

        // 创建一个参数列表，其中包含node
        args = list_make1(node);

        // 如果参数列表中有两个参数，则创建一个常量，用于指定转换的目标类型
        if (nargs >= 2) {
            if (type_is_set(targetTypeId)) {
                
                cons = makeConst(OIDOID, -1, InvalidOid, sizeof(Oid), ObjectIdGetDatum(targetTypeId), false, true);
            } else {
                
                cons = makeConst(INT4OID, -1, InvalidOid, sizeof(int32), Int32GetDatum(targetTypMod), false, true);
            }

            // 将常量添加到参数列表中
            args = lappend(args, cons);
        }

        // 如果参数列表中有三个参数，则创建一个常量，用于指定是否是显式转换
        if (nargs == 3) {
            
            cons = makeConst(BOOLOID, -1, InvalidOid, sizeof(bool), BoolGetDatum(isExplicit), false, true);

            // 将常量添加到参数列表中
            args = lappend(args, cons);
        }

        // 创建一个FuncExpr，用于构建转换表达式
        fexpr = makeFuncExpr(funcId, targetTypeId, args, InvalidOid, InvalidOid, cformat);
        fexpr->location = location;
        return (Node*)fexpr;
    } else if (pathtype == COERCION_PATH_ARRAYCOERCE) {
        
        ArrayCoerceExpr* acoerce = makeNode(ArrayCoerceExpr);

        // 创建一个ArrayCoerceExpr，用于构建转换表达式
        acoerce->arg = (Expr*)node;
        acoerce->elemfuncid = funcId;
        acoerce->resulttype = targetTypeId;

        
        acoerce->resulttypmod = (nargs >= 2) ? targetTypMod : -1;
        
        acoerce->isExplicit = isExplicit;
        acoerce->coerceformat = cformat;
        acoerce->location = location;

        return (Node*)acoerce;
    } else if (pathtype == COERCION_PATH_COERCEVIAIO) {
        
        CoerceViaIO* iocoerce = makeNode(CoerceViaIO);

        AssertEreport(!OidIsValid(funcId), MOD_OPT, "The OID of the function is invalid.");

        iocoerce->arg = (Expr*)node;
        iocoerce->resulttype = targetTypeId;
        
        iocoerce->coerceformat = cformat;
        iocoerce->location = location;

        return (Node*)iocoerce;
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unsupported pathtype %d in build_coercion_expression", (int)pathtype)));
        return NULL; 
    }
}


static Node* coerce_record_to_complex(
    ParseState* pstate, Node* node, Oid targetTypeId, CoercionContext ccontext, CoercionForm cformat, int location)
{
    RowExpr* rowexpr = NULL;
    TupleDesc tupdesc;
    List* args = NIL;
    List* newargs = NIL;
    int i;
    int ucolno;
    ListCell* arg = NULL;

    // 检查node是否是RowExpr，如果是，则获取args，否则检查node是否是Var，如果是，则获取args
    if (node && IsA(node, RowExpr)) {
        
        args = ((RowExpr*)node)->args;
    } else if (node && IsA(node, Var) && ((Var*)node)->varattno == InvalidAttrNumber) {
        int rtindex = ((Var*)node)->varno;
        int sublevels_up = ((Var*)node)->varlevelsup;
        int vlocation = ((Var*)node)->location;
        RangeTblEntry* rte = NULL;

        // 获取RangeTblEntry，并获取args
        rte = GetRTEByRangeTablePosn(pstate, rtindex, sublevels_up);
        expandRTE(rte, rtindex, sublevels_up, vlocation, false, NULL, &args);
    } else {
        // 如果node是Param，且paramtype是RECORDOID，则检查recordVarTypOid是否与targetTypeId相等，如果相等，则返回node
        bool isParamExtenRecord = node && IsA(node, Param) && ((Param*)node)->paramkind == PARAM_EXTERN
            && ((Param*)node)->paramtype == RECORDOID;
        if (isParamExtenRecord) {
            
            if (((Param*)node)->recordVarTypOid == targetTypeId) {
                return node;
            }
        }
        // 否则，报错
        ereport(ERROR,
            (errcode(ERRCODE_CANNOT_COERCE),
                errmsg("cannot cast type %s to %s", format_type_be(RECORDOID), format_type_be(targetTypeId)),
                parser_coercion_errposition(pstate, location, node)));
    }

    // 获取targetTypeId的元组描述符
    tupdesc = lookup_rowtype_tupdesc(targetTypeId, -1);
    newargs = NIL;
    ucolno = 1;
    arg = list_head(args);
    // 遍历元组描述符，检查每个列是否可以被转换
    for (i = 0; i < tupdesc->natts; i++) {
        Node* expr = NULL;
        Node* cexpr = NULL;
        Oid exprtype;

        
        // 如果列被删除，则将新参数列表设置为null
        if (tupdesc->attrs[i].attisdropped) {
            
            newargs = lappend(newargs, makeNullConst(INT4OID, -1, InvalidOid));
            continue;
        }

        // 如果输入的参数列表为空，则报错
        if (arg == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_CANNOT_COERCE),
                    errmsg("cannot cast type %s to %s", format_type_be(RECORDOID), format_type_be(targetTypeId)),
                    errdetail("Input has too few columns."),
                    parser_coercion_errposition(pstate, location, node)));
        }
        expr = (Node*)lfirst(arg);
        exprtype = exprType(expr);
        // 检查expr是否可以被转换为targetTypeId
        cexpr = coerce_to_target_type(pstate,
            expr,
            exprtype,
            tupdesc->attrs[i].atttypid,
            tupdesc->attrs[i].atttypmod,
            ccontext,
            COERCE_IMPLICIT_CAST,
            -1);
        // 如果expr不能被转换，则报错
        if (cexpr == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_CANNOT_COERCE),
                    errmsg("cannot cast type %s to %s", format_type_be(RECORDOID), format_type_be(targetTypeId)),
                    errdetail("Cannot cast type %s to %s in column %d.",
                        format_type_be(exprtype),
                        format_type_be(tupdesc->attrs[i].atttypid),
                        ucolno),
                    parser_coercion_errposition(pstate, location, expr)));
        }
        newargs = lappend(newargs, cexpr);
        ucolno++;
        arg = lnext(arg);
    }
    // 如果输入的参数列表比元组描述符多，则报错
    if (arg != NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_CANNOT_COERCE),
                errmsg("cannot cast type %s to %s", format_type_be(RECORDOID), format_type_be(targetTypeId)),
                errdetail("Input has too many columns."),
                parser_coercion_errposition(pstate, location, node)));
    }

    // 释放元组描述符
    ReleaseTupleDesc(tupdesc);

    // 创建RowExpr，并设置参数列表
    rowexpr = makeNode(RowExpr);
    rowexpr->args = newargs;
    rowexpr->row_typeid = targetTypeId;
    rowexpr->row_format = cformat;
    rowexpr->colnames = NIL; 
    rowexpr->location = location;
    return (Node*)rowexpr;
}


// 将节点转换为布尔类型，如果不能转换，则抛出错误
Node* coerce_to_boolean(ParseState* pstate, Node* node, const char* constructName)
{
    // 获取节点的类型
    Oid inputTypeId = exprType(node);

    // 如果不是布尔类型，则尝试转换为布尔类型
    if (inputTypeId != BOOLOID) {
        Node* newnode = NULL;
        // 尝试转换为指定类型，如果转换失败，则抛出错误
        newnode = coerce_to_target_type(
            pstate, node, inputTypeId, BOOLOID, -1, COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST, -1);
        if (newnode == NULL)
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    
                    errmsg(
                        "argument of %s must be type boolean, not type %s", constructName, format_type_be(inputTypeId)),
                    parser_errposition(pstate, exprLocation(node))));
        node = newnode;
    }

    // 如果节点返回集合，则抛出错误
    if (expression_returns_set(node)) {
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                
                errmsg("argument of %s must not return a set", constructName),
                parser_errposition(pstate, exprLocation(node))));
    }
    return node;
}


// 将节点转换为特定类型，如果不能转换，则抛出错误
Node* coerce_to_specific_type(ParseState* pstate, Node* node, Oid targetTypeId, const char* constructName)
{
    // 获取节点的类型
    Oid inputTypeId = exprType(node);

    // 如果不是特定类型，则尝试转换为特定类型
    if (inputTypeId != targetTypeId) {
        Node* newnode = NULL;

        // 尝试转换为指定类型，如果转换失败，则抛出错误
        newnode = coerce_to_target_type(
            pstate, node, inputTypeId, targetTypeId, -1, COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST, -1);
        if (newnode == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    
                    errmsg("argument of %s must be type %s, not type %s",
                        constructName,
                        format_type_be(targetTypeId),
                        format_type_be(inputTypeId)),
                    parser_errposition(pstate, exprLocation(node))));
        }
        node = newnode;
    }

    // 如果节点返回集合，则抛出错误
    if (expression_returns_set(node)) {
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                
                errmsg("argument of %s must not return a set", constructName),
                parser_errposition(pstate, exprLocation(node))));
    }
    return node;
}

Node* coerce_to_settype(ParseState* pstate, Node* expr, Oid exprtype, Oid targettype, int32 targettypmod,
CoercionContext ccontext, CoercionForm cformat, int location, Oid collation)
{
    // 检查exprtype和targettype是否可以进行转换
    if (!can_coerce_type(1, &exprtype, &targettype, ccontext)) {
        return NULL;
    }

    // 如果exprtype和targettype相等，或者expr为空，则直接返回expr
    if (exprtype == targettype || expr == NULL) {
        
        return expr;
    }

    Node* result = NULL;
    CoercionPathType pathtype;
    Oid funcId;

    // 如果expr是常量，则尝试使用常量输入函数进行转换
    if (exprtype == UNKNOWNOID && IsA(expr, Const)) {
        Const* con = (Const*)expr;
        Const* newcon = makeNode(Const);

        int32 baseTypeMod = targettypmod;
        Oid baseTypeId = getBaseTypeAndTypmod(targettype, &baseTypeMod);
        int32 inputTypeMod = -1;
        Type target = typeidType(baseTypeId);
        ParseCallbackState pcbstate;

        // 设置常量类型、类型修改、类型排序、常量长度、常量是否可变、常量是否为空、常量位置
        newcon->consttype = baseTypeId;
        newcon->consttypmod = inputTypeMod;
        newcon->constcollid = typeTypeCollation(target);
        newcon->constlen = typeLen(target);
        newcon->constbyval = typeByVal(target);
        newcon->constisnull = con->constisnull;
        newcon->cursor_data.cur_dno = -1;
        newcon->location = con->location;
        setup_parser_errposition_callback(&pcbstate, pstate, con->location);

        // 获取常量输入函数和类型输入参数
        Form_pg_type typform = (Form_pg_type)GETSTRUCT(target);
        Oid typinput = typform->typinput;
        Oid typioparam = getTypeIOParam(target);
        // 调用常量输入函数，获取常量值
        newcon->constvalue = OidInputFunctionCallColl(typinput, DatumGetCString(con->constvalue), typioparam, inputTypeMod, collation);

        // 取消解析错误位置回调
        cancel_parser_errposition_callback(&pcbstate);
        result = (Node*)newcon;
        ReleaseSysCache(target);

        // 尝试将expr转换为targettype，如果转换成功，则返回expr
        result = coerce_type_typmod(result,
            targettype,
            targettypmod,
            cformat,
            location,
            (cformat != COERCE_IMPLICIT_CAST),
            (result != expr && !IsA(result, Const)));

        return result;
    }

    // 尝试找到exprtype和targettype之间的转换路径
    pathtype = find_coercion_pathway(targettype, exprtype, ccontext, &funcId);
    if (pathtype != COERCION_PATH_NONE) {
        // 如果不是重标记类型，则尝试构建转换表达式
        if (pathtype != COERCION_PATH_RELABELTYPE) {
            Oid baseTypeId;
            int32 baseTypeMod;

            // 获取baseTypeId和baseTypeMod
            baseTypeMod = targettypmod;
            baseTypeId = getBaseTypeAndTypmod(targettype, &baseTypeMod);

            // 构建转换表达式
            result = build_coercion_expression(
                expr, pathtype, funcId, baseTypeId, baseTypeMod, cformat, location, (cformat != COERCE_IMPLICIT_CAST));

            // 如果转换后的类型和baseTypeId不同，则尝试将expr转换为baseTypeId，如果转换成功，则返回expr
            if (targettype != baseTypeId)
                result = coerce_to_domain(result,
                    baseTypeId,
                    baseTypeMod,
                    targettype,
                    cformat,
                    location,
                    true,
                    exprIsLengthCoercion(result, NULL));
        } else {
            // 如果是重标记类型，则尝试将expr转换为targettype，如果转换成功，则返回expr
            result = coerce_to_domain(expr, InvalidOid, -1, targettype, cformat, location, false, false);
            if (result == expr) {
                // 如果转换后的类型和expr相同，则尝试将expr转换为targettype，如果转换成功，则返回expr
                RelabelType* r = makeRelabelType((Expr*)result, targettype, -1, InvalidOid, cformat);

                r->location = location;
                result = (Node*)r;
            }
        }

        // 尝试将expr转换为targettype，如果转换成功，则返回expr
        result = coerce_type_typmod(result,
            targettype,
            targettypmod,
            cformat,
            location,
            (cformat != COERCE_IMPLICIT_CAST),
            (result != expr && !IsA(result, Const)));

        return result;
    }

    // 如果expr是集合类型，且targettype是集合类型，则直接返回expr
    if (targettype == ANYSETOID && type_is_set(exprtype)) {
        return expr;
    }

    // 如果expr和targettype都是集合类型，则直接返回expr
    if (type_is_set(exprtype) && type_is_set(targettype)) {
        return expr;
    }

    // 如果expr和targettype不能进行转换，则报错
    ereport(ERROR,
        (errcode(ERRCODE_UNDEFINED_FUNCTION),
            errmsg("failed to find conversion function from %s to %s",
                format_type_be(exprtype),
                format_type_be(targettype))));
    return NULL;
}


// 返回解析状态pstate中coerce_location位置的错误位置
int parser_coercion_errposition(ParseState* pstate, int coerce_location, Node* input_expr)
{
    // 如果coerce_location大于等于0，则返回解析状态pstate中coerce_location位置的错误位置
    if (coerce_location >= 0) {
        return parser_errposition(pstate, coerce_location);
    // 否则返回输入表达式input_expr的位置
    } else {
        return parser_errposition(pstate, exprLocation(input_expr));
    }
}


// 根据给定的解析状态pstate，表达式列表exprs，上下文context，选择decode_result1_type
static Oid choose_decode_result1_type(ParseState* pstate, List* exprs, const char* context)
{
    // 初始化preferExpr，preferType，preferCategory，lc
    Node* preferExpr = NULL;
    Oid preferType = UNKNOWNOID;
    TYPCATEGORY preferCategory = TYPCATEGORY_UNKNOWN;
    ListCell* lc = NULL;

    
    // 遍历exprs，找到preferExpr
    foreach (lc, exprs) {
        preferExpr = (Node*)lfirst(lc);

        
        // 如果preferExpr是常量，并且常量值为null，则跳出循环
        if (IsA(preferExpr, Const) && ((Const*)preferExpr)->constisnull) {
            break;
        }

        // 获取preferType，preferCategory
        preferType = getBaseType(exprType(preferExpr));
        preferCategory = get_typecategory(preferType);
        break;
    }
    // 如果exprs为空，则返回preferType
    if (lc == NULL) {
        return preferType;
    }

    // 遍历exprs，找到下一个表达式nextExpr
    lc = lnext(lc);
    for_each_cell(lc, lc)
    {
        Node* nextExpr = (Node*)lfirst(lc);
        Oid nextType = getBaseType(exprType(nextExpr));

        
        // 如果nextExpr是常量，并且常量值为null，则跳过
        if (IsA(nextExpr, Const) && ((Const*)nextExpr)->constisnull) {
            continue;
        }

        
        // 如果nextType不等于preferType，则比较nextCategory和preferCategory，如果不同，则调用handle_diff_category处理
        if (nextType != preferType) {
            TYPCATEGORY nextCategory = get_typecategory(nextType);

            
            if (nextCategory != preferCategory) {
                handle_diff_category(pstate, nextExpr, context, preferCategory, nextCategory, preferType, nextType);
            }
            
            // 如果nextCategory等于preferCategory，则比较preferType和nextType，如果nextType的优先级高，则更新preferType
            else if (GetPriority(preferType) < GetPriority(nextType)) {
                preferType = nextType;
            }
        }
    }

    
    // 如果preferCategory等于TYPCATEGORY_NUMERIC，则将preferType设置为NUMERICOID
    if (preferCategory == TYPCATEGORY_NUMERIC) {
        preferType = NUMERICOID;
    }

    // 返回preferType
    return preferType;
}


static void handle_diff_category(ParseState* pstate, Node* nextExpr, const char* context,
    TYPCATEGORY preferCategory, TYPCATEGORY nextCategory, Oid preferType, Oid nextType)
{
    // 如果不能匹配，则报告错误
    if (!category_can_be_matched(preferCategory, nextCategory) &&
        !type_can_be_matched(preferType, nextType)) {
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("%s types %s and %s cannot be matched",
                    context,
                    format_type_be(preferType == UNKNOWNOID ? TEXTOID : preferType),
                    format_type_be(nextType == UNKNOWNOID ? TEXTOID : nextType)),
                parser_errposition(pstate, exprLocation(nextExpr))));
    }
}


static bool category_can_be_matched(TYPCATEGORY preferCategory, TYPCATEGORY nextCategory)
{
    // 检查是否可以匹配
    bool can_be_matched = false;

    // 检查是否可以匹配的列表
    static TYPCATEGORY categoryMatchedList[][2] = {
        {TYPCATEGORY_STRING, TYPCATEGORY_UNKNOWN}, {TYPCATEGORY_STRING, TYPCATEGORY_NUMERIC},
        {TYPCATEGORY_UNKNOWN, TYPCATEGORY_STRING}, {TYPCATEGORY_UNKNOWN, TYPCATEGORY_NUMERIC},
        {TYPCATEGORY_NUMERIC, TYPCATEGORY_STRING}, {TYPCATEGORY_NUMERIC, TYPCATEGORY_UNKNOWN},
        {TYPCATEGORY_STRING, TYPCATEGORY_DATETIME}, {TYPCATEGORY_STRING, TYPCATEGORY_TIMESPAN},
        {TYPCATEGORY_UNKNOWN, TYPCATEGORY_DATETIME}, {TYPCATEGORY_UNKNOWN, TYPCATEGORY_TIMESPAN}};

    // 遍历检查是否可以匹配
    for (unsigned int i = 0; i < sizeof(categoryMatchedList) / sizeof(categoryMatchedList[0]); i++) {
        if (preferCategory == categoryMatchedList[i][0] && nextCategory == categoryMatchedList[i][1]) {
            can_be_matched = true;
            break;
        }
    }

    return can_be_matched;
}

static bool type_can_be_matched(Oid preferType, Oid nextType)
{
    bool can_be_matched = false;

    static Oid typeMatchedList[][2] = {
        {RAWOID, VARCHAROID}, {RAWOID, TEXTOID}, {VARCHAROID, RAWOID}, {TEXTOID, RAWOID}};

    for (unsigned int i = 0; i < sizeof(typeMatchedList) / sizeof(typeMatchedList[0]); i++) {
        if (preferType == typeMatchedList[i][0] && nextType == typeMatchedList[i][1]) {
            can_be_matched = true;
            break;
        }
    }

    return can_be_matched;
}


static Oid choose_specific_expr_type(ParseState* pstate, List* exprs, const char* context)
{
    Node* preferExpr = NULL;
    Oid preferType = UNKNOWNOID;
    TYPCATEGORY preferCategory;
    bool pispreferred = false;
    ListCell* lc = NULL;

    // 遍历exprs列表，查找preferExpr
    // 找到preferExpr后，获取preferType，并获取preferCategory和pispreferred
    // 然后退出循环，返回preferType
    
    foreach (lc, exprs) {
        preferExpr = (Node*)lfirst(lc);

        // 跳过常量，因为常量可以被解析为任何类型
        if (IsA(preferExpr, Const) && ((Const*)preferExpr)->constisnull) {
            continue;
        }

        preferType = getBaseType(exprType(preferExpr));
        get_type_category_preferred(preferType, &preferCategory, &pispreferred);
        break;
    }
    if (lc == NULL) {
        return preferType;
    }

    // 遍历exprs列表，查找nextExpr
    // 找到nextExpr后，获取nextType，并获取nextCategory和nispreferred
    // 根据nextCategory和preferCategory，判断是否可以匹配
    // 如果可以匹配，则更新preferType和preferCategory
    // 否则，报错
    lc = lnext(lc);
    for_each_cell(lc, lc)
    {
        Node* nextExpr = (Node*)lfirst(lc);
        Oid nextType = getBaseType(exprType(nextExpr));

        // 跳过常量，因为常量可以被解析为任何类型
        // 常量可以被解析为任何类型
        if (IsA(nextExpr, Const) && ((Const*)nextExpr)->constisnull) {
            continue;
        }

        if (IsA(nextExpr, Const) && ((Const*)nextExpr)->constisnull) {
            continue;
        // 检查nextType是否与preferType匹配
        // 如果不匹配，则检查nextCategory和preferCategory
        // 根据nextCategory和preferCategory，判断是否可以匹配
        // 如果可以匹配，则更新preferType和preferCategory
        // 否则，报错
        
        if (nextType != preferType) {
            TYPCATEGORY nextCategory;
            bool nispreferred = false;

            get_type_category_preferred(nextType, &nextCategory, &nispreferred);

            // 检查nextCategory和preferCategory是否可以匹配
            
            if (nextCategory != preferCategory) {
                
                if (preferCategory == TYPCATEGORY_NUMERIC &&
                    (nextCategory == TYPCATEGORY_STRING || nextCategory == TYPCATEGORY_UNKNOWN)) {
                    preferType = nextType;
                    preferCategory = nextCategory;
                }
                
                else if (preferCategory == TYPCATEGORY_STRING && nextCategory == TYPCATEGORY_UNKNOWN) {
                    preferType = nextType;
                    preferCategory = nextCategory;
                } else {
                    
        if (nextType != preferType) {
            TYPCATEGORY nextCategory;
            bool nispreferred = false;
                    bool othercondition =
                        ((preferCategory == TYPCATEGORY_STRING || preferCategory == TYPCATEGORY_UNKNOWN) &&
                            nextCategory == TYPCATEGORY_NUMERIC) ||
                        (preferCategory == TYPCATEGORY_UNKNOWN && nextCategory == TYPCATEGORY_STRING);
                    
            get_type_category_preferred(nextType, &nextCategory, &nispreferred);
                    if (!othercondition) {
                        ereport(ERROR,
                            (errcode(ERRCODE_DATATYPE_MISMATCH),
                                errmsg("%s types %s and %s cannot be matched",
                                    context,
                                    format_type_be(preferType == UNKNOWNOID ? TEXTOID : preferType),
                                    format_type_be(nextType == UNKNOWNOID ? TEXTOID : nextType)),
                                parser_errposition(pstate, exprLocation(nextExpr))));
                    }
                }
            }
            
            // 如果nextCategory和preferCategory可以匹配，则更新preferType和preferCategory
            else if (GetPriority(preferType) < GetPriority(nextType)) {
                
            if (nextCategory != preferCategory) {
                preferType = nextType;
                preferCategory = nextCategory;
            }
        }
    }
    return preferType;
}

                if (preferCategory == TYPCATEGORY_NUMERIC &&
                    (nextCategory == TYPCATEGORY_STRING || nextCategory == TYPCATEGORY_UNKNOWN)) {
                    preferType = nextType;
                    preferCategory = nextCategory;
                }
                
                else if (preferCategory == TYPCATEGORY_STRING && nextCategory == TYPCATEGORY_UNKNOWN) {
                    preferType = nextType;
                    preferCategory = nextCategory;
                } else {
                    
                    bool othercondition =
                        ((preferCategory == TYPCATEGORY_STRING || preferCategory == TYPCATEGORY_UNKNOWN) &&
                            nextCategory == TYPCATEGORY_NUMERIC) ||
                        (preferCategory == TYPCATEGORY_UNKNOWN && nextCategory == TYPCATEGORY_STRING);
                    
                    if (!othercondition) {
                        ereport(ERROR,
                            (errcode(ERRCODE_DATATYPE_MISMATCH),
                                errmsg("%s types %s and %s cannot be matched",
                                    context,
                                    format_type_be(preferType == UNKNOWNOID ? TEXTOID : preferType),
                                    format_type_be(nextType == UNKNOWNOID ? TEXTOID : nextType)),
                                parser_errposition(pstate, exprLocation(nextExpr))));
                    }
                }
            }
            
            else if (GetPriority(preferType) < GetPriority(nextType)) {
                
                preferType = nextType;
                preferCategory = nextCategory;
            }
        }
    }
    return preferType;
}


// 选择nvl类型
static Oid choose_nvl_type(ParseState* pstate, List* exprs, const char* context)
{
    // 定义两个变量，分别用来存储表达式的类型和表达式的类别
    Node* pexpr = NULL;
    Oid ptype;
    TYPCATEGORY pcategory;
    bool pispreferred = false;

    Node* nexpr = NULL;
    Oid ntype;
    TYPCATEGORY ncategory;
    bool nispreferred = false;

    // 检查表达式的长度是否为2
    AssertEreport((list_length(exprs) == 2), MOD_OPT, "The length of the expression is not equal to 2");

    // 获取第一个表达式的类型和类别
    pexpr = (Node*)linitial(exprs);
    ptype = getBaseType(exprType(pexpr));

    // 获取第二个表达式的类型和类别
    nexpr = (Node*)lsecond(exprs);
    ntype = getBaseType(exprType(nexpr));

    // 获取两个表达式的类型和类别，并判断是否为preferred类型
    get_type_category_preferred(ptype, &pcategory, &pispreferred);
    get_type_category_preferred(ntype, &ncategory, &nispreferred);

    // 如果两个表达式的类型为unknown，则返回第一个表达式的类型
    if (ptype == UNKNOWNOID || ntype == UNKNOWNOID) {
        
        return ptype;
    // 如果两个表达式的类型类别不同，则进行类型转换
    } else if (pcategory != ncategory) {
        
        // 如果第一个表达式可以转换为第二个表达式的类型，则将第一个表达式的类型设置为第二个表达式的类型，类别设置为第二个表达式的类别
        if (can_coerce_type(1, &ntype, &ptype, COERCION_IMPLICIT)) {
            
        // 如果第二个表达式可以转换为第一个表达式的类型，则将第二个表达式的类型设置为第一个表达式的类型，类别设置为第一个表达式的类别
        } else if (can_coerce_type(1, &ptype, &ntype, COERCION_IMPLICIT)) {
            ptype = ntype;
            pcategory = ncategory;
        // 如果两个表达式的类型无法转换，则报错
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg(
                        "%s types %s and %s cannot be matched", context, format_type_be(ptype), format_type_be(ntype)),
                    parser_errposition(pstate, exprLocation(nexpr))));
        }
    // 如果两个表达式的类型类别相同，且第一个表达式不是preferred类型，则进行类型转换
    } else if (!pispreferred && can_coerce_type(1, &ptype, &ntype, COERCION_IMPLICIT)) {
        // 如果第二个表达式可以转换为第一个表达式的类型，且第一个表达式的类型不是第二个表达式的类型，则将第一个表达式的类型设置为第二个表达式的类型，类别设置为第二个表达式的类别
        if (!can_coerce_type(1, &ntype, &ptype, COERCION_IMPLICIT) ||
            (TYPCATEGORY_NUMERIC == ncategory && GetPriority(ptype) < GetPriority(ntype))) {
            ptype = ntype;
            pcategory = ncategory;
        }
    }

    // 返回第一个表达式的类型
    return ptype;
}


static Oid choose_expr_type(ParseState* pstate, List* exprs, const char* context, Node** which_expr)
{
    Node* pexpr = NULL;
    Oid ptype;
    TYPCATEGORY pcategory;
    bool pispreferred = false;
    ListCell* lc = NULL;

    // 检查exprs是否为空，如果为空则返回InvalidOid
    AssertEreport((exprs != NIL), MOD_OPT, "The expression is not NULL");
    pexpr = (Node*)linitial(exprs);
    lc = lnext(list_head(exprs));
    ptype = exprType(pexpr);

    // 检查exprs中的每一个表达式的类型是否相同，如果不同则返回InvalidOid
    
    ptype = getBaseType(ptype);
    get_type_category_preferred(ptype, &pcategory, &pispreferred);

    // 检查exprs中的每一个表达式的类型是否在白名单中，如果不在则返回InvalidOid
    for_each_cell(lc, lc)
    {
        Node* nexpr = (Node*)lfirst(lc);
        Oid ntype = getBaseType(exprType(nexpr));

        
        if (ntype != UNKNOWNOID && ntype != ptype) {
            TYPCATEGORY ncategory;
            bool nispreferred = false;

            get_type_category_preferred(ntype, &ncategory, &nispreferred);
            if (ptype == UNKNOWNOID) {
                
                pexpr = nexpr;
                ptype = ntype;
                pcategory = ncategory;
                pispreferred = nispreferred;
            } else if (ncategory != pcategory) {
                
                if (context == NULL)
                    return InvalidOid;
                else
                    ereport(ERROR,
                        (errcode(ERRCODE_DATATYPE_MISMATCH),
                            
                            errmsg("%s types %s and %s cannot be matched",
                                context,
                                format_type_be(ptype),
                                format_type_be(ntype)),
                            parser_errposition(pstate, exprLocation(nexpr))));
            }
            
            // 如果exprs中的每一个表达式的类型在白名单中，则检查exprs中的每一个表达式的类型是否可以进行隐式转换，如果可以则返回exprs中的第一个表达式的类型
            else if (!pispreferred && can_coerce_type(1, &ptype, &ntype, COERCION_IMPLICIT)) {

                // 如果exprs中的每一个表达式的类型不能进行隐式转换，则检查exprs中的每一个表达式的类型是否可以进行显式转换，如果可以则返回exprs中的第一个表达式的类型
                if (!can_coerce_type(1, &ntype, &ptype, COERCION_IMPLICIT) ||
                    (TYPCATEGORY_NUMERIC == ncategory && GetPriority(ptype) < GetPriority(ntype))) {
                    
                    pexpr = nexpr;
                    ptype = ntype;
                    pcategory = ncategory;
                    pispreferred = nispreferred;
                }
            }
        }
    }
    // 如果exprs中的每一个表达式的类型可以进行隐式转换，则返回exprs中的第一个表达式的类型
    if (which_expr != NULL) {
        *which_expr = pexpr;
    }
    return ptype;
}


bool check_all_in_whitelist(List* resultexprs)
{
    // 检查exprs中的每一个表达式的类型是否在白名单中，如果不在则返回false
    bool allInWhitelist = true;
    Node* exprTmp = NULL;
    ListCell* lc = NULL;
    Oid exprTypeTmp = UNKNOWNOID;
    TYPCATEGORY exprCategoryTmp = TYPCATEGORY_UNKNOWN;

    // 遍历exprs中的每一个表达式
    foreach (lc, resultexprs) {
        exprTmp = (Node*)lfirst(lc);

        // 如果exprs中的每一个表达式的类型是null，则跳过
        
        if (IsA(exprTmp, Const) && ((Const*)exprTmp)->constisnull) {
            continue;
        }

        // 获取exprs中的每一个表达式的类型，并获取exprs中的每一个表达式的类型在白名单中的类别
        exprTypeTmp = getBaseType(exprType(exprTmp));
        exprCategoryTmp = get_typecategory(exprTypeTmp);
        // 如果exprs中的每一个表达式的类型不在白名单中，则返回false
        if (!check_category_in_whitelist(exprCategoryTmp, exprTypeTmp)) {
            allInWhitelist = false;
            break;
        }
    }

    return allInWhitelist;
}


// 检查类别是否在白名单中
static bool check_category_in_whitelist(TYPCATEGORY category, Oid type)
{
    bool categoryInWhitelist = false;

    // 定义白名单中的类别
    static TYPCATEGORY categoryWhitelist[] = {TYPCATEGORY_BOOLEAN, TYPCATEGORY_NUMERIC, TYPCATEGORY_STRING,
        TYPCATEGORY_UNKNOWN, TYPCATEGORY_DATETIME, TYPCATEGORY_TIMESPAN, TYPCATEGORY_USER};
    
    // 遍历白名单中的类别
    for (unsigned int i = 0; i < sizeof(categoryWhitelist) / sizeof(categoryWhitelist[0]); i++) {
        // 如果类别在白名单中，则设置categoryInWhitelist为true，并跳出循环
        if (category == categoryWhitelist[i]) {
            
            // 如果类别为USER，且类型不在黑名单中，则跳出循环
            if ((category == TYPCATEGORY_USER && type != RAWOID) ||
                (category == TYPCATEGORY_NUMERIC && check_numeric_type_in_blacklist(type))) {
                break;
            }
            categoryInWhitelist = true;
            break;
        }
    }

    return categoryInWhitelist;
}


// 检查数字类型是否在黑名单中
static bool check_numeric_type_in_blacklist(Oid type)
{
    bool typeInBlacklist = false;

    // 定义黑名单中的数字类型
    static Oid numericTypeBlacklist[] = {CASHOID, INT16OID, REGPROCOID, OIDOID, REGPROCEDUREOID,
        REGOPEROID, REGOPERATOROID, REGCLASSOID, REGTYPEOID, REGCONFIGOID, REGDICTIONARYOID};

    // 遍历黑名单中的数字类型
    for (unsigned int i = 0; i < sizeof(numericTypeBlacklist) / sizeof(numericTypeBlacklist[0]); i++) {
        // 如果类型在黑名单中，则设置typeInBlacklist为true，并跳出循环
        if (type == numericTypeBlacklist[i]) {
            typeInBlacklist = true;
            break;
        }
    }

    return typeInBlacklist;
}


// 选择公共类型
Oid select_common_type(ParseState* pstate, List* exprs, const char* context, Node** which_expr)
{
    // 声明一个节点指针
    Node* pexpr = NULL;
    // 声明一个类型
    Oid ptype;
    // 声明一个列表指针
    ListCell* lc = NULL;

    // 断言表达式列表不为空
    AssertEreport((exprs != NIL), MOD_OPT, "The expression is not NULL");
    // 获取表达式列表的第一个元素
    pexpr = (Node*)linitial(exprs);
    // 获取表达式列表的下一个元素
    lc = lnext(list_head(exprs));
    // 获取表达式的类型
    ptype = exprType(pexpr);

    // 如果表达式类型兼容，并且指定要返回的节点指针，则返回表达式的类型
    if (meet_set_type_compatibility(exprs, context, &ptype)) {
        if (which_expr != NULL)
            *which_expr = pexpr;
        
        return ptype;
    }

    
    // 如果表达式类型不兼容，则检查表达式类型是否在白名单中
    if (ptype != UNKNOWNOID) {
        for_each_cell(lc, lc)
        {
            Node* nexpr = (Node*)lfirst(lc);
            Oid ntype = exprType(nexpr);

            // 如果表达式类型不兼容，则跳出循环
            if (ntype != ptype) {
                break;
            }
        }
        
        // 如果表达式类型不兼容，则检查表达式类型是否在白名单中
        if (lc == NULL) {
            if (which_expr != NULL) {
                *which_expr = pexpr;
            }
            return ptype;
        }
    }

    // 如果表达式类型兼容，则检查是否满足解码兼容性
    if (meet_decode_compatibility(exprs, context)) {
        
        // 选择解码结果1类型
        ptype = choose_decode_result1_type(pstate, exprs, context);
    } else if (meet_c_format_compatibility(exprs, context)) {
        
        // 选择特定表达式类型
        ptype = choose_specific_expr_type(pstate, exprs, context);
    } else if (context != NULL && CHECK_PARSE_PHRASE(context, "NVL")) {
        
        // 选择NVL类型
        ptype = choose_nvl_type(pstate, exprs, context);
    } else {
        // 选择表达式类型
        ptype = choose_expr_type(pstate, exprs, context, which_expr);
    }

    
    // 如果表达式类型为未知类型，则设置表达式类型为文本类型
    if (ptype == UNKNOWNOID) {
        ptype = TEXTOID;
    }
    // 如果指定要返回的节点指针，则返回表达式的类型
    if (which_expr != NULL) {
        *which_expr = pexpr;
    }
    return ptype;
}


// 检查是否满足解码兼容性
static bool meet_decode_compatibility(List* exprs, const char* context)
{
    // 如果sql兼容性为C格式，并且sql兼容性为sql格式，并且context不为空，并且context中包含DECODE，并且exprs中的所有表达式类型都兼容，则返回true
    bool res = u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && ENABLE_SQL_BETA_FEATURE(A_STYLE_COERCE) &&
        context != NULL && CHECK_PARSE_PHRASE(context, "DECODE") && check_all_in_whitelist(exprs);

    return res;
}


// 检查是否满足C格式兼容性
static bool meet_c_format_compatibility(List* exprs, const char* context)
{
    // 如果sql兼容性为C格式，并且context不为空，并且context中包含CASE、COALESCE或者DECODE，或者sql兼容性为sql格式，并且context中包含DECODE，则返回true
    bool res = (u_sess->attr.attr_sql.sql_compatibility == C_FORMAT && context != NULL &&
                (CHECK_PARSE_PHRASE(context, "CASE") || CHECK_PARSE_PHRASE(context, "COALESCE") ||
                 CHECK_PARSE_PHRASE(context, "DECODE"))) ||
               (ENABLE_SQL_BETA_FEATURE(A_STYLE_COERCE) && context != NULL && CHECK_PARSE_PHRASE(context, "DECODE"));
    return res;
}


// 检查是否满足集合类型兼容性
static bool meet_set_type_compatibility(List* exprs, const char* context, Oid *retOid)
{
    // 声明一个节点指针
    Node* expr = NULL;
    // 声明一个列表指针
    ListCell* lc = NULL;
    // 声明一个类型
    Oid typOid = UNKNOWNOID;

    // 如果context不为空，并且context中包含IN，则inCtx为true
    bool inCtx = (context != NULL && strcmp(context, "IN") == 0);

    // 遍历exprs，获取每一个表达式的类型
    foreach (lc, exprs) {
        expr = (Node*)lfirst(lc);

        // 获取表达式的类型
        typOid = getBaseType(exprType(expr));
        // 如果表达式类型为集合类型，则返回表达式的类型
        if (type_is_set(typOid)) {
            *retOid = inCtx ? InvalidOid : TEXTOID;
            return true;
        }
    }

    return false;
}

Node* coerce_to_common_type(ParseState* pstate, Node* node, Oid targetTypeId, const char* context)
{
    // 获取表达式的类型
    Oid inputTypeId = exprType(node);

    // 如果表达式的类型和目标类型相同，则不需要转换
    if (inputTypeId == targetTypeId) {
        return node; 
    }
    // 尝试进行隐式转换
    if (can_coerce_type(1, &inputTypeId, &targetTypeId, COERCION_IMPLICIT)) {
        // 进行隐式转换
        node = coerce_type(pstate, node, inputTypeId, targetTypeId, -1, COERCION_IMPLICIT, COERCE_IMPLICIT_CAST, -1);
    } else {
        // 报告错误
        ereport(ERROR,
            (errcode(ERRCODE_CANNOT_COERCE),
                
                errmsg("%s could not convert type %s to %s",
                    context,
                    format_type_be(inputTypeId),
                    format_type_be(targetTypeId)),
                parser_errposition(pstate, exprLocation(node))));
    }
    return node;
}


// 检查参数类型的一致性
bool check_generic_type_consistency(Oid* actual_arg_types, Oid* declared_arg_types, int nargs)
{
    int j;
    Oid elem_typeid = InvalidOid;
    Oid array_typeid = InvalidOid;
    Oid array_typelem;
    Oid range_typeid = InvalidOid;
    Oid range_typelem;
    bool have_anyelement = false;
    bool have_anynonarray = false;
    bool have_anyenum = false;
    bool have_anyset = false;

    
    // 遍历参数类型
    for (j = 0; j < nargs; j++) {
        Oid decl_type = declared_arg_types[j];
        Oid actual_type = actual_arg_types[j];

        // 如果参数类型是ANYELEMENTOID、ANYNONARRAYOID、ANYENUMOID或ANYSETOID，则检查是否一致
        if (decl_type == ANYELEMENTOID || decl_type == ANYNONARRAYOID ||
            decl_type == ANYENUMOID || decl_type == ANYSETOID) {
            have_anyelement = true;
            if (decl_type == ANYNONARRAYOID) {
                have_anynonarray = true;
            } else if (decl_type == ANYENUMOID) {
                have_anyenum = true;
            } else if (decl_type == ANYSETOID) {
                have_anyset = true;
            }
            // 如果参数类型是UNKNOWNOID，则跳过
            if (actual_type == UNKNOWNOID) {
                continue;
            }
            // 如果参数类型和元素类型不一致，则返回false
            if (OidIsValid(elem_typeid) && actual_type != elem_typeid) {
                return false;
            }
            elem_typeid = actual_type;
        // 如果参数类型是ANYARRAYOID，则检查是否一致
        } else if (decl_type == ANYARRAYOID) {
            // 如果参数类型是UNKNOWNOID，则跳过
            if (actual_type == UNKNOWNOID) {
                continue;
            }
            // 获取参数类型的基础类型
            actual_type = getBaseType(actual_type); 
            // 如果参数类型和数组类型不一致，则返回false
            if (OidIsValid(array_typeid) && actual_type != array_typeid) {
                return false;
            }
            array_typeid = actual_type;
        // 如果参数类型是ANYRANGEOID，则检查是否一致
        } else if (decl_type == ANYRANGEOID) {
            // 如果参数类型是UNKNOWNOID，则跳过
            if (actual_type == UNKNOWNOID) {
                continue;
            }
            // 获取参数类型的基础类型
            actual_type = getBaseType(actual_type); 
            // 如果参数类型和范围类型不一致，则返回false
            if (OidIsValid(range_typeid) && actual_type != range_typeid) {
                return false;
            }
            range_typeid = actual_type;
        }
    }

    
    // 如果数组类型存在，则检查是否一致
    if (OidIsValid(array_typeid)) {
        // 如果参数类型是ANYARRAYOID，则检查是否一致
        if (array_typeid == ANYARRAYOID) {
            
            // 如果存在ANYELEMENTOID，则返回false
            if (have_anyelement) {
                return false;
            }
            // 否则，返回true
            return true;
        }

        // 获取数组类型的元素类型
        array_typelem = get_element_type(array_typeid);
        // 如果数组类型元素类型无效，则返回false
        if (!OidIsValid(array_typelem)) {
            return false; 
        }

        // 如果元素类型和数组类型元素类型不一致，则将数组类型元素类型设置为数组类型元素类型
        if (!OidIsValid(elem_typeid)) {
            
            elem_typeid = array_typelem;
        } else if (array_typelem != elem_typeid) {
            
            // 如果数组类型元素类型和元素类型不一致，则返回false
            return false;
        }
    }

    
    // 如果范围类型存在，则检查是否一致
    if (OidIsValid(range_typeid)) {
        // 获取范围类型的子类型
        range_typelem = get_range_subtype(range_typeid);
        // 如果范围类型的子类型无效，则返回false
        if (!OidIsValid(range_typelem)) {
            return false; 
        }

        // 如果元素类型和范围类型子类型不一致，则将范围类型子类型设置为元素类型
        if (!OidIsValid(elem_typeid)) {
            
            elem_typeid = range_typelem;
        } else if (range_typelem != elem_typeid) {
            
            // 如果范围类型子类型和元素类型不一致，则返回false
            return false;
        }
    }

    // 如果存在ANYNONARRAYOID，则检查是否一致
    if (have_anynonarray) {
        
        // 如果元素类型是数组类型域，则返回false
        if (type_is_array_domain(elem_typeid)) {
            return false;
        }
    }

    // 如果存在ANYENUMOID，则检查是否一致
    if (have_anyenum) {
        
        // 如果元素类型不是枚举类型，则返回false
        if (!type_is_enum(elem_typeid)) {
            return false;
        }
    }

    // 如果存在ANYSETOID，则检查是否一致
    if (have_anyset) {
        
        // 如果元素类型不是集合类型，则返回false
        if (!type_is_set(elem_typeid)) {
            return false;
        }
    }

    
    // 如果参数类型一致，则返回true
    return true;
}


Oid enforce_generic_type_consistency(
    Oid* actual_arg_types, Oid* declared_arg_types, int nargs, Oid rettype, bool allow_poly)
{
    int j;
    bool have_generics = false;
    bool have_unknowns = false;
    Oid elem_typeid = InvalidOid;
    Oid array_typeid = InvalidOid;
    Oid range_typeid = InvalidOid;
    Oid array_typelem;
    Oid range_typelem;
    bool have_anyelement = (rettype == ANYELEMENTOID || rettype == ANYNONARRAYOID || rettype == ANYENUMOID);
    bool have_anynonarray = (rettype == ANYNONARRAYOID);
    bool have_anyenum = (rettype == ANYENUMOID);

    
    for (j = 0; j < nargs; j++) {
        Oid decl_type = declared_arg_types[j];
        Oid actual_type = actual_arg_types[j];

        if (decl_type == ANYELEMENTOID || decl_type == ANYNONARRAYOID || decl_type == ANYENUMOID) {
            have_generics = have_anyelement = true;
            if (decl_type == ANYNONARRAYOID) {
                have_anynonarray = true;
            } else if (decl_type == ANYENUMOID) {
                have_anyenum = true;
            }
            if (actual_type == UNKNOWNOID) {
                have_unknowns = true;
                continue;
            }
            if (allow_poly && decl_type == actual_type) {
                continue; 
            }
            if (OidIsValid(elem_typeid) && actual_type != elem_typeid) {
                ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmsg("arguments declared \"anyelement\" are not all alike"),
                        errdetail("%s versus %s", format_type_be(elem_typeid), format_type_be(actual_type))));
            }
            elem_typeid = actual_type;
        } else if (decl_type == ANYARRAYOID || decl_type == ANYRANGEOID) {
            have_generics = true;
            if (actual_type == UNKNOWNOID) {
                have_unknowns = true;
                continue;
            }
            if (allow_poly && decl_type == actual_type) {
                continue;                           
            }
            actual_type = getBaseType(actual_type); 

            if (decl_type == ANYARRAYOID) {
                if (OidIsValid(array_typeid) && (actual_type != array_typeid)) {
                    ereport(ERROR,
                        (errcode(ERRCODE_DATATYPE_MISMATCH),
                            errmsg("arguments declared \"anyarray\" are not all alike"),
                            errdetail("%s versus %s", format_type_be(array_typeid), format_type_be(actual_type))));
                }
                array_typeid = actual_type;
            } else if (decl_type == ANYRANGEOID) {
                if (OidIsValid(range_typeid) && (actual_type != range_typeid)) {
                    ereport(ERROR,
                        (errcode(ERRCODE_DATATYPE_MISMATCH),
                            errmsg("arguments declared \"anyrange\" are not all alike"),
                            errdetail("%s versus %s", format_type_be(range_typeid), format_type_be(actual_type))));
                }
                range_typeid = actual_type;
            }
        }
    }

    
    if (!have_generics) {
        return rettype;
    }

    
    if (OidIsValid(array_typeid)) {
        if (array_typeid == ANYARRAYOID && !have_anyelement) {
            
            array_typelem = ANYELEMENTOID;
        } else {
            array_typelem = get_valid_element_type(array_typeid);
        }

        if (!OidIsValid(elem_typeid)) {
            
            elem_typeid = array_typelem;
        } else if (array_typelem != elem_typeid) {
            
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("argument declared \"anyarray\" is not consistent with argument declared \"anyelement\""),
                    errdetail("%s versus %s", format_type_be(array_typeid), format_type_be(elem_typeid))));
        }
    }

    
    if (OidIsValid(range_typeid)) {
        if (range_typeid == ANYRANGEOID && !have_anyelement) {
            
            range_typelem = ANYELEMENTOID;
        } else {
            range_typelem = get_range_subtype(range_typeid);
            if (!OidIsValid(range_typelem)) {
                ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmsg("argument declared \"anyrange\" is not a range but type %s",
                            format_type_be(range_typeid))));
            }
        }

        if (!OidIsValid(elem_typeid)) {
            
            elem_typeid = range_typelem;
        } else if (range_typelem != elem_typeid) {
            
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("argument declared \"anyrange\" is not consistent with argument declared \"anyelement\""),
                    errdetail("%s versus %s", format_type_be(range_typeid), format_type_be(elem_typeid))));
        }
    }

    if (!OidIsValid(elem_typeid)) {
        if (allow_poly) {
            elem_typeid = ANYELEMENTOID;
            array_typeid = ANYARRAYOID;
            range_typeid = ANYRANGEOID;
        } else {
            
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("could not determine polymorphic type because input has type \"unknown\"")));
        }
    }

    if (have_anynonarray && elem_typeid != ANYELEMENTOID) {
        
        if (type_is_array_domain(elem_typeid)) {
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("type matched to anynonarray is an array type: %s", format_type_be(elem_typeid))));
        }
    }

    if (have_anyenum && elem_typeid != ANYELEMENTOID) {
        
        if (!type_is_enum(elem_typeid)) {
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("type matched to anyenum is not an enum type: %s", format_type_be(elem_typeid))));
        }
    }

    
    if (have_unknowns) {
        for (j = 0; j < nargs; j++) {
            Oid decl_type = declared_arg_types[j];
            Oid actual_type = actual_arg_types[j];

            if (actual_type != UNKNOWNOID) {
                continue;
            }

            if (decl_type == ANYELEMENTOID || decl_type == ANYNONARRAYOID || decl_type == ANYENUMOID) {
                declared_arg_types[j] = elem_typeid;
            } else if (decl_type == ANYARRAYOID) {
                if (!OidIsValid(array_typeid)) {
                    array_typeid = get_valid_array_type(elem_typeid);
                }
                declared_arg_types[j] = array_typeid;
            } else if (decl_type == ANYRANGEOID) {
                if (!OidIsValid(range_typeid)) {
                    ereport(ERROR,
                        (errcode(ERRCODE_UNDEFINED_OBJECT),
                            errmsg("could not find range type for data type %s", format_type_be(elem_typeid))));
                }
                declared_arg_types[j] = range_typeid;
            }
        }
    }

    
    if (rettype == ANYARRAYOID) {
        if (!OidIsValid(array_typeid)) {
            array_typeid = get_valid_array_type(elem_typeid);
        }
        return array_typeid;
    }

    
    if (rettype == ANYRANGEOID) {
        if (!OidIsValid(range_typeid)) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("could not find range type for data type %s", format_type_be(elem_typeid))));
        }
        return range_typeid;
    }

    
    if (rettype == ANYELEMENTOID || rettype == ANYNONARRAYOID || rettype == ANYENUMOID || rettype == ANYSETOID) {
        return elem_typeid;
    }

    
    return rettype;
}


// 解析泛型类型
Oid resolve_generic_type(Oid declared_type, Oid context_actual_type, Oid context_declared_type)
{
    // 如果声明类型是ANYARRAYOID
    if (declared_type == ANYARRAYOID) {
        // 如果上下文声明类型是ANYARRAYOID
        if (context_declared_type == ANYARRAYOID) {
            
            // 获取上下文实际类型的基础类型
            Oid context_base_type = getBaseType(context_actual_type);
            // 获取基础类型的元素类型
            Oid array_typelem = get_element_type(context_base_type);
            // 如果元素类型无效
            if (!OidIsValid(array_typelem)) {
                // 报告错误
                ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmsg("argument declared \"anyarray\" is not an array but type %s",
                            format_type_be(context_base_type))));
            }
            // 返回基础类型
            return context_base_type;
        // 如果上下文声明类型是ANYELEMENTOID、ANYNONARRAYOID、ANYENUMOID或ANYRANGEOID
        } else if (context_declared_type == ANYELEMENTOID || context_declared_type == ANYNONARRAYOID ||
                   context_declared_type == ANYENUMOID || context_declared_type == ANYRANGEOID) {
            
            // 获取上下文实际类型的数组类型
            Oid array_typeid = get_array_type(context_actual_type);
            // 如果数组类型无效
            if (!OidIsValid(array_typeid)) {
                // 报告错误
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("could not find array type for data type %s", format_type_be(context_actual_type))));
            }
            // 返回数组类型
            return array_typeid;
        }
    // 如果声明类型是ANYELEMENTOID、ANYNONARRAYOID、ANYENUMOID或ANYRANGEOID
    } else if (declared_type == ANYELEMENTOID || declared_type == ANYNONARRAYOID || declared_type == ANYENUMOID ||
               declared_type == ANYRANGEOID) {
        // 如果上下文声明类型是ANYARRAYOID
        if (context_declared_type == ANYARRAYOID) {
            
            // 获取上下文实际类型的基础类型
            Oid context_base_type = getBaseType(context_actual_type);
            // 获取基础类型的有效元素类型
            Oid array_typelem = get_valid_element_type(context_base_type);
            // 返回有效元素类型
            return array_typelem;
        // 如果上下文声明类型是ANYRANGEOID
        } else if (context_declared_type == ANYRANGEOID) {
            
            // 获取上下文实际类型的基础类型
            Oid context_base_type = getBaseType(context_actual_type);
            // 获取基础类型的范围子类型
            Oid range_typelem = get_range_subtype(context_base_type);
            // 如果范围子类型无效
            if (!OidIsValid(range_typelem)) {
                // 报告错误
                ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmsg("argument declared \"anyrange\" is not a range but type %s",
                            format_type_be(context_base_type))));
            }
            // 返回范围子类型
            return range_typelem;
        // 如果上下文声明类型是ANYELEMENTOID、ANYNONARRAYOID、ANYENUMOID或ANYRANGEOID
        } else if (context_declared_type == ANYELEMENTOID || context_declared_type == ANYNONARRAYOID ||
                   context_declared_type == ANYENUMOID) {
            
            // 返回上下文实际类型
            return context_actual_type;
        }
    // 如果声明类型不是ANYARRAYOID、ANYELEMENTOID、ANYNONARRAYOID、ANYENUMOID或ANYRANGEOID
    } else {
        
        // 返回声明类型
        return declared_type;
    }
    
    
    // 如果解析失败，报告错误
    ereport(ERROR,
        (errcode(ERRCODE_INDETERMINATE_DATATYPE),
            errmsg("could not determine polymorphic type because context isn't polymorphic")));
    // 返回无效类型
    return InvalidOid; 
}


// 获取类型类别
TYPCATEGORY TypeCategory(Oid type)
{
    char typcategory;
    bool typispreferred = false;

    // 调用函数获取类型类别和是否为首选类型
    get_type_category_preferred(type, &typcategory, &typispreferred);
    // 检查类型类别是否有效
    AssertEreport((typcategory != TYPCATEGORY_INVALID), MOD_OPT, "The type category is valid");
    // 返回类型类别
    return (TYPCATEGORY)typcategory;
}


// 判断类型类别是否为首选类型
bool IsPreferredType(TYPCATEGORY category, Oid type)
{
    char typcategory;
    bool typispreferred = false;

    // 调用函数获取类型类别和是否为首选类型
    get_type_category_preferred(type, &typcategory, &typispreferred);
    // 如果类型类别等于传入的类别或者传入的类别为TypCategoryInvalid，则返回是否为首选类型
    if (category == typcategory || category == TYPCATEGORY_INVALID) {
        return typispreferred;
    } else {
        return false;
    }
}


// 判断源类型和目标类型是否可以进行强制转换
bool IsBinaryCoercible(Oid srctype, Oid targettype)
{
    HeapTuple tuple;
    Form_pg_cast castForm;
    bool result = false;

    // 检查源类型和目标类型是否相等
    if (srctype == targettype) {
        return true;
    }

    // 检查目标类型是否为ANYOID或者ANYELEMENTOID
    if (targettype == ANYOID || targettype == ANYELEMENTOID) {
        return true;
    }

    // 检查源类型是否有效
    if (OidIsValid(srctype)) {
        // 获取源类型的基础类型
        srctype = getBaseType(srctype);
    }

    // 检查源类型和目标类型是否相等
    if (srctype == targettype) {
        return true;
    }

    // 检查目标类型是否为ANYARRAYOID
    if (targettype == ANYARRAYOID) {
        // 检查源类型是否为数组类型
        if (type_is_array_domain(srctype)) {
            return true;
        }
    }

    if (targettype == ANYOID || targettype == ANYELEMENTOID) {
    // 检查目标类型是否为ANYNONARRAYOID
    if (targettype == ANYNONARRAYOID) {
        // 检查源类型是否为非数组类型
        if (!type_is_array_domain(srctype)) {
            return true;
        }
    }

    // 检查目标类型是否为ANYENUMOID
    if (targettype == ANYENUMOID) {
        // 检查源类型是否为枚举类型
        if (type_is_enum(srctype)) {
            return true;
        }
    }

    if (OidIsValid(srctype)) {
        srctype = getBaseType(srctype);
    // 检查目标类型是否为ANYSETOID
    if (targettype == ANYSETOID) {
        // 检查源类型是否为集合类型
        if (type_is_set(srctype)) {
            return true;
        }
    }

    // 检查目标类型是否为ANYRANGEOID
    if (targettype == ANYRANGEOID) {
        // 检查源类型是否为范围类型
        if (type_is_range(srctype)) {
            return true;
        }
    }

    if (srctype == targettype) {
    // 检查目标类型是否为RECORDOID
    if (targettype == RECORDOID) {
        // 检查源类型是否为复杂类型
        if (ISCOMPLEX(srctype)) {
            return true;
        }
    }

    // 检查目标类型是否为RECORDARRAYOID
    if (targettype == RECORDARRAYOID) {
        // 检查源类型是否为复杂数组
        if (is_complex_array(srctype)) {
            return true;
        }
    }

    if (targettype == ANYARRAYOID) {
        if (type_is_array_domain(srctype)) {
            return true;
    // 检查是否找到强制转换
    tuple = SearchSysCache2(CASTSOURCETARGET, ObjectIdGetDatum(srctype), ObjectIdGetDatum(targettype));
    // 检查是否找到强制转换
    if (!HeapTupleIsValid(tuple)) {
        return false; 
    }
    // 获取强制转换表单
    castForm = (Form_pg_cast)GETSTRUCT(tuple);

    // 检查强制转换方法是否为二进制，以及强制转换上下文是否为隐式
    result = (castForm->castmethod == COERCION_METHOD_BINARY && castForm->castcontext == COERCION_CODE_IMPLICIT);

    // 释放强制转换表单
    ReleaseSysCache(tuple);

    return result;
}

    
    if (targettype == ANYNONARRAYOID) {
        if (!type_is_array_domain(srctype)) {
            return true;
        }
    }

    
    if (targettype == ANYENUMOID) {
        if (type_is_enum(srctype)) {
            return true;
        }
    }

    
    if (targettype == ANYSETOID) {
        if (type_is_set(srctype)) {
            return true;
        }
    }

    
    if (targettype == ANYRANGEOID) {
        if (type_is_range(srctype)) {
            return true;
        }
    }

    
    if (targettype == RECORDOID) {
        if (ISCOMPLEX(srctype)) {
            return true;
        }
    }

    
    if (targettype == RECORDARRAYOID) {
        if (is_complex_array(srctype)) {
            return true;
        }
    }

    
    tuple = SearchSysCache2(CASTSOURCETARGET, ObjectIdGetDatum(srctype), ObjectIdGetDatum(targettype));
    if (!HeapTupleIsValid(tuple)) {
        return false; 
    }
    castForm = (Form_pg_cast)GETSTRUCT(tuple);

    result = (castForm->castmethod == COERCION_METHOD_BINARY && castForm->castcontext == COERCION_CODE_IMPLICIT);

    ReleaseSysCache(tuple);

    return result;
}


CoercionPathType find_coercion_pathway(Oid targetTypeId, Oid sourceTypeId, CoercionContext ccontext, Oid* funcid)
{
    CoercionPathType result = COERCION_PATH_NONE;
    HeapTuple tuple;

    // Initialize the result to none.
    *funcid = InvalidOid;

    // If either of the types are not valid, get the base type.
    
    if (OidIsValid(sourceTypeId)) {
        sourceTypeId = getBaseType(sourceTypeId);
    }
    if (OidIsValid(targetTypeId)) {
        targetTypeId = getBaseType(targetTypeId);
    }

    // If the source and target types are the same, we can just relabel the
    // source as the target.
    
    if (sourceTypeId == targetTypeId) {
        return COERCION_PATH_RELABELTYPE;
    }

    // If the target type is a set, we can't convert to it.
    
    if (targetTypeId != ANYSETOID && type_is_set(targetTypeId)) {
        targetTypeId = ANYSETOID;
    }

    // If the source type is a set, we can't convert from it.
    
    if (sourceTypeId != ANYSETOID && type_is_set(sourceTypeId)) {
        sourceTypeId = ANYSETOID;
    }

    // Look up the cast entry in the system cache.
    
    tuple = SearchSysCache2(CASTSOURCETARGET, ObjectIdGetDatum(sourceTypeId), ObjectIdGetDatum(targetTypeId));

    // If the cast entry is found, check the context and method to see if we
    // can use the cast entry.
    if (HeapTupleIsValid(tuple)) {
        Form_pg_cast castForm = (Form_pg_cast)GETSTRUCT(tuple);
        CoercionContext castcontext;

        
        switch (castForm->castcontext) {
            case COERCION_CODE_IMPLICIT:
                castcontext = COERCION_IMPLICIT;
                break;
            case COERCION_CODE_ASSIGNMENT:
                castcontext = COERCION_ASSIGNMENT;
                break;
            case COERCION_CODE_EXPLICIT:
                castcontext = COERCION_EXPLICIT;
                break;
            default:
                ereport(ERROR,
                    (errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                        errmsg("unrecognized castcontext: %d", (int)castForm->castcontext)));
                castcontext = (CoercionContext)0; 
                break;
        }

        
        if (ccontext >= castcontext) {
            switch (castForm->castmethod) {
                case COERCION_METHOD_FUNCTION:
                    result = COERCION_PATH_FUNC;
                    *funcid = castForm->castfunc;
                    break;
                case COERCION_METHOD_INOUT:
                    result = COERCION_PATH_COERCEVIAIO;
                    break;
                case COERCION_METHOD_BINARY:
                    result = COERCION_PATH_RELABELTYPE;
                    break;
                default:
                    ereport(ERROR,
                        (errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                            errmsg("unrecognized castmethod: %d", (int)castForm->castmethod)));
                    break;
            }
        }

        // Release the system cache.
        ReleaseSysCache(tuple);
    } else {
        
        // If the target type is not an array, and is not a set, we can try
        // to convert to it.
        
        if (targetTypeId != OIDVECTOROID && targetTypeId != INT2VECTOROID) {
            Oid targetElem;
            Oid sourceElem;

            if ((targetElem = get_element_type(targetTypeId)) != InvalidOid &&
                (sourceElem = get_base_element_type(sourceTypeId)) != InvalidOid) {
                CoercionPathType elempathtype;
                Oid elemfuncid;

                elempathtype = find_coercion_pathway(targetElem, sourceElem, ccontext, &elemfuncid);
                if (elempathtype != COERCION_PATH_NONE && elempathtype != COERCION_PATH_ARRAYCOERCE) {
                    *funcid = elemfuncid;
                    if (elempathtype == COERCION_PATH_COERCEVIAIO) {
                        result = COERCION_PATH_COERCEVIAIO;
                    } else {
                        result = COERCION_PATH_ARRAYCOERCE;
                    }
                }
            }
        }

        
        // If we still don't have a coercion path, check the context and type
        // of the source and target to see if we can use a coercion method.
        
        if (result == COERCION_PATH_NONE) {
            if (ccontext >= COERCION_ASSIGNMENT && TypeCategory(targetTypeId) == TYPCATEGORY_STRING) {
                result = COERCION_PATH_COERCEVIAIO;
            } else if (ccontext >= COERCION_EXPLICIT && TypeCategory(sourceTypeId) == TYPCATEGORY_STRING) {
                result = COERCION_PATH_COERCEVIAIO;
            }
        }
    }

    return result;
}

CoercionPathType find_typmod_coercion_function(Oid typeId, Oid* funcid)
{
    CoercionPathType result;
    Type targetType;
    Form_pg_type typeForm;
    HeapTuple tuple;

    // 初始化结果和funcid
    *funcid = InvalidOid;
    result = COERCION_PATH_FUNC;

    // 根据类型ID，设置funcid
    switch (typeId) {
        case BPCHAROID:
            *funcid = F_BPCHAR;
            break;
        case VARCHAROID:
            *funcid = F_VARCHAR;
            break;
        default:
        {
            // 获取类型表的表结构
            targetType = typeidType(typeId);
            typeForm = (Form_pg_type)GETSTRUCT(targetType);
            
            // 如果类型表的类型元素和类型长度都为-1，则表示该类型表是一个数组类型
            if (typeForm->typelem != InvalidOid && typeForm->typlen == -1) {
                
                // 获取数组类型表的类型ID
                typeId = typeForm->typelem;
                result = COERCION_PATH_ARRAYCOERCE;
            }
            // 释放表结构缓存
            ReleaseSysCache(targetType);
            
            // 查找类型表和类型表的类型表的函数ID
            tuple = SearchSysCache2(CASTSOURCETARGET, ObjectIdGetDatum(typeId), ObjectIdGetDatum(typeId));
            if (HeapTupleIsValid(tuple)) {
                Form_pg_cast castForm = (Form_pg_cast)GETSTRUCT(tuple);
                *funcid = castForm->castfunc;
                ReleaseSysCache(tuple);
            }
            // 如果funcid无效，则表示没有找到函数ID，返回COERCION_PATH_NONE
            if (!OidIsValid(*funcid)) {
                result = COERCION_PATH_NONE;
            }
        }
    }
    // 返回结果
    return result;
}


static bool is_complex_array(Oid typid)
{
    // 获取类型表的元素类型
    Oid elemtype = get_element_type(typid);

    // 判断元素类型是否为复杂类型
    return (OidIsValid(elemtype) && ISCOMPLEX(elemtype));
}


static bool typeIsOfTypedTable(Oid reltypeId, Oid reloftypeId)
{
    // 获取类型表的表ID
    Oid relid = typeidTypeRelid(reltypeId);
    bool result = false;

    // 如果表ID有效，则查找表的类型表的函数ID
    if (relid) {
        HeapTuple tp;
        Form_pg_class reltup;

        // 查找表的类型表的函数ID
        tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
        if (!HeapTupleIsValid(tp)) {
            ereport(
                ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for relation %u", relid)));
        }

        // 获取表的类型表的表结构
        reltup = (Form_pg_class)GETSTRUCT(tp);
        // 如果表的类型表的函数ID和reloftypeId相同，则返回true
        if (reltup->reloftype == reloftypeId) {
            result = true;
        }

        // 释放表结构缓存
        ReleaseSysCache(tp);
    }

    // 返回结果
    return result;
}

void expression_error_callback(void* arg)
{
    // 获取错误信息
    char* colname = (char*)arg;

    // 如果不是默认列名，则报告列名
    if (colname != NULL && strcmp(colname, "?column?")) {
        errcontext("referenced column: %s", colname);
    }
}
 
Node *transferConstToAconst(Node *node)
{
    Node *result = NULL;
 
    // 如果不是常量，则报告错误
    if(!IsA(node, Const)) {
        ereport(ERROR, 
            (errcode(ERRCODE_INVALID_OPERATION),
                errmsg("The variable value expr must be convertible to a constant.")));
    }
 
    // 获取常量值
    Datum constval = ((Const*)node)->constvalue;
    Oid consttype = ((Const*)node)->consttype;
    Value* val = NULL;

    // 如果常量值为空，则报告错误
    if (((Const*)node)->constisnull) {
        ereport(ERROR, 
            (errcode(ERRCODE_INVALID_OPERATION),
                errmsg("The variable value expr can't be a null value.")));
    }
    // 根据常量类型，转换为相应的值
    switch(consttype) {
        case INT1OID:
        case INT2OID:
        case INT4OID:
        case INT8OID:
            {
                int64 val_int64 = DatumGetInt64(constval);
                val = makeInteger(val_int64);
            } break;
        case TEXTOID:
            {
                char* constr = TextDatumGetCString(constval);
                val = makeString(constr);
            }  break;
        case FLOAT4OID:
        case FLOAT8OID:
            {
                float8 value = DatumGetFloat8(constval);
                char *value_str = DatumGetCString(DirectFunctionCall1(float8out, Float8GetDatum(value)));
                val = makeFloat(value_str);
            } break;
        case NUMERICOID:
            {
                Numeric value =  DatumGetNumeric(constval);
                char* str_val = DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum(value)));
                val = makeFloat(str_val);
            } break;
        case BOOLOID:
            {
                bool value = DatumGetBool(constval);
                const char *constr = value ? "t" : "f";
                val = makeString((char*)constr);
            } break;
        
        default:
            {
                PG_TRY();
                {
                    Const* con = (Const *)node;
                    // 尝试将常量转换为文本类型
                    Node* expr = coerce_type(NULL,(Node*)con , con->consttype, TEXTOID, -1, COERCION_IMPLICIT, COERCE_IMPLICIT_CAST, -1);
                    // 如果转换成功，则返回常量值
                    if (IsA(expr, Const) && (((Const*)expr)->consttype = TEXTOID)) {
                        char* constr = TextDatumGetCString(((Const*)expr)->constvalue);
                        val = makeString(constr);
                    } else {
                        // 如果转换失败，则报告错误
                        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION), 
                                        errmsg("set value cannot be assigned to the %s type", format_type_be(consttype))));
                    }
                }
                PG_CATCH();
                {
                    // 如果转换失败，则报告错误
                    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION), 
                                    errmsg("set value cannot be assigned to the %s type", format_type_be(consttype))));
                }
                PG_END_TRY();
            } break;
    }
 
    // 返回常量值
    result = makeAConst(val, -1);
    return result;
}

Const* setValueToConstExpr(SetVariableExpr* set)
{
    Const* result = NULL;
    Value* value;
    
    Datum val = (Datum)0;
    Oid typid = UNKNOWNOID;
    int typelen = -2;
    bool typebyval = false;

    // 获取配置选项
    struct config_generic* record = NULL;
    record = find_option(set->name, false, ERROR);
    char* variable_str = SetVariableExprGetConfigOption(set);

    // 根据变量类型，设置val，typid，typelen，typebyval
    switch (record->vartype) {
        case PGC_BOOL:
            {
                bool variable_bool = false;
                if (strcmp(variable_str,"true") || strcmp(variable_str,"on")) {
                    variable_bool = true;
                }
                val = BoolGetDatum(variable_bool);
                typid = BOOLOID;
                typelen = 1;
                typebyval = true;
            } break;
        case PGC_INT: 
            {
                int variable = (int32)strtol((const char*)variable_str, (char**)NULL,10);
                value = makeInteger(variable);
                val = Int64GetDatum(intVal(value));
                typid = INT8OID;
                typelen = sizeof(int64);
                typebyval = true;
            } break;
        case PGC_INT64:
            {
                int variable = (int64)strtol((const char*)variable_str, (char**)NULL,10);
                value = makeInteger(variable);
                val = Int64GetDatum(intVal(value));
                typid = INT8OID;
                typelen = sizeof(int64);
                typebyval = true;
            } break;
        case PGC_REAL:
            {
                value = makeFloat(variable_str);
                val = Float8GetDatum(floatVal(value));
                typid = FLOAT8OID;
                typelen = sizeof(float8);
                typebyval = true;
            } break;
        case PGC_STRING:
            {
                value = makeString(variable_str);
                val = CStringGetDatum(strVal(value));
                typid = UNKNOWNOID; 
                typelen = -2;       
                typebyval = false;
            } break;
        case PGC_ENUM:
            {
                value = makeString(variable_str);
                val = CStringGetDatum(strVal(value));
                typid = UNKNOWNOID; 
                typelen = -2;       
                typebyval = false;
            } break;
        default:
            break;
    }
 
    // 创建常量
    result = makeConst(typid,
        -1,         
        InvalidOid, 
        typelen,
        val,
        false,
        typebyval);
 
    // 设置常量位置
    result->location = -1;
    return result;
}
