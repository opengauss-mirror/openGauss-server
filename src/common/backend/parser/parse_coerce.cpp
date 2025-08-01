/* -------------------------------------------------------------------------
 *
 * parse_coerce.cpp
 *		handle type coercions/conversions for parser
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/common/backend/parser/parse_coerce.cpp
 *
 * -------------------------------------------------------------------------
 */
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

static Node* coerce_type_typmod(Node* node, Oid targetTypeId, int32 targetTypMod, CoercionForm cformat, char* fmtstr,
    char* nlsfmtstr, int location, bool isExplicit, bool hideInputCoercion);
static void hide_coercion_node(Node* node);
static Node* build_coercion_expression(Node* node, CoercionPathType pathtype, Oid funcId, Oid targetTypeId,
    int32 targetTypMod, CoercionForm cformat, char* fmtstr, char* nlsfmtstr, int location, bool isExplicit);
static Node* coerce_record_to_complex(
    ParseState* pstate, Node* node, Oid targetTypeId, CoercionContext ccontext, CoercionForm cformat, int location);
static bool is_complex_array(Oid typid);
static bool typeIsOfTypedTable(Oid reltypeId, Oid reloftypeId);
static Oid choose_decode_result1_type(ParseState* pstate, List* exprs, const char* context);
static void handle_diff_category(ParseState* pstate, Node* nextExpr, const char* context,
    TYPCATEGORY preferCategory, TYPCATEGORY nextCategory, Oid preferType, Oid nextType);
static bool category_can_be_matched(TYPCATEGORY preferCategory, TYPCATEGORY nextCategory);
static bool type_can_be_matched(Oid preferType, Oid nextType);
static Oid choose_specific_expr_type(ParseState* pstate, List* exprs, const char* context);
static Oid choose_nvl_type(ParseState* pstate, List* exprs, const char* context);
static Oid choose_expr_type(ParseState* pstate, List* exprs, const char* context, Node** which_expr);
static bool check_category_in_whitelist(TYPCATEGORY category, Oid type);
static bool check_numeric_type_in_blacklist(Oid type);
static bool meet_decode_compatibility(List* exprs, const char* context);
static bool meet_c_format_compatibility(List* exprs, const char* context);
static bool meet_set_type_compatibility(List* exprs, const char* context, Oid *retOid);
extern Node* makeAConst(Value* v, int location);
#define CHECK_PARSE_PHRASE(context, target) \
    (AssertMacro(sizeof(target) - 1 == strlen(target)), strncmp(context, target, sizeof(target) - 1) == 0)

/*
 * @Description: same as get_element_type() except this reports error
 * when the result is invalid.
 * @in base_type: array type oid to resolve
 * @return: valid element oid
 */
inline Oid get_valid_element_type(Oid base_type)
{
    Oid array_typelem = get_element_type(base_type);
    if (!OidIsValid(array_typelem)) {
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("argument declared \"anyarray\" is not an array but type %s", format_type_be(base_type))));
    }
    return array_typelem;
}

/*
 * @Description: same as get_array_type() except this reports error
 * when the result is invalid.
 * @in elem_type: element type oid to be searched
 * @return: valid array oid
 */
inline Oid get_valid_array_type(Oid elem_type)
{
    Oid array_typeid = get_array_type(elem_type);
    if (!OidIsValid(array_typeid)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("could not find range type for data type %s", format_type_be(elem_type))));
    }
    return array_typeid;
}
/*
 * coerce_to_target_type()
 *		Convert an expression to a target type and typmod.
 *
 * This is the general-purpose entry point for arbitrary type coercion
 * operations.	Direct use of the component operations can_coerce_type,
 * coerce_type, and coerce_type_typmod should be restricted to special
 * cases (eg, when the conversion is expected to succeed).
 *
 * Returns the possibly-transformed expression tree, or NULL if the type
 * conversion is not possible.	(We do this, rather than ereport'ing directly,
 * so that callers can generate custom error messages indicating context.)
 *
 * pstate - parse state (can be NULL, see coerce_type)
 * expr - input expression tree (already transformed by transformExpr)
 * exprtype - result type of expr
 * targettype - desired result type
 * targettypmod - desired result typmod
 * ccontext, cformat - context indicators to control coercions
 * location - parse location of the coercion request, or -1 if unknown/implicit
 */

static bool check_varchar_coerce(Node* node, Oid target_type_id, int32 target_typ_mod)
{
    if ((target_type_id != VARCHAROID && target_type_id != BPCHAROID)|| exprTypmod(node) != -1) {
        return false;
    }
    if (!IsA(node, Const)) {
        return false;
    }
    Const *con = (Const*)node;

    if (con->constisnull) {
        return false;
    }
    if (target_type_id == VARCHAROID) {
        VarChar *source = DatumGetVarCharPP(con->constvalue);
        int32 len;
        int32 maxlen;
        len = VARSIZE_ANY_EXHDR(source);
        maxlen = target_typ_mod;
        /*No work if typmod is invalid or supplied data fits it already*/
        if (maxlen < 0 || len <= maxlen) {
            return true;
        }
        return false;
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

Node* coerce_to_target_type(ParseState* pstate, Node* expr, Oid exprtype, Oid targettype, int32 targettypmod,
    CoercionContext ccontext, CoercionForm cformat, char* fmtstr, char* nlsfmtstr, int location)
{
    Node* result = NULL;
    Node* origexpr = NULL;

    if (!can_coerce_type(1, &exprtype, &targettype, ccontext)) {
        return NULL;
    }

    /*
     * If the input has a CollateExpr at the top, strip it off, perform the
     * coercion, and put a new one back on.  This is annoying since it
     * duplicates logic in coerce_type, but if we don't do this then it's too
     * hard to tell whether coerce_type actually changed anything, and we
     * *must* know that to avoid possibly calling hide_coercion_node on
     * something that wasn't generated by coerce_type.  Note that if there are
     * multiple stacked CollateExprs, we just discard all but the topmost.
     */
    origexpr = expr;
    while (expr && IsA(expr, CollateExpr)) {
        expr = (Node*)((CollateExpr*)expr)->arg;
    }

    result = coerce_type(pstate, expr, exprtype, targettype, targettypmod, ccontext, cformat,
        fmtstr, nlsfmtstr, location);

    /*
     * If the target is a fixed-length type, it may need a length coercion as
     * well as a type coercion.  If we find ourselves adding both, force the
     * inner coercion node to implicit display form.
     */
    if (u_sess->attr.attr_common.enable_iud_fusion && pstate
        && !pstate->p_joinlist && pstate->p_expr_kind == EXPR_KIND_INSERT_TARGET && result != NULL
        && check_varchar_coerce(result, targettype, targettypmod)) {
        Const* cons = (Const*)result;
        cons->consttypmod = DatumGetInt32((targettypmod));
        cons->constcollid = InvalidOid;
        cons->location = -1;
        result = (Node*)cons;
    } else {
        result = coerce_type_typmod(result,
            targettype,
            targettypmod,
            cformat,
            fmtstr,
            nlsfmtstr,
            location,
            (cformat != COERCE_IMPLICIT_CAST),
            (result != expr && !IsA(result, Const)));
    }

    if (expr != origexpr && (
#ifdef PGXC
        /* Do not need to do that on local Coordinator */
        IsConnFromCoord() ||
#endif
        type_is_collatable(targettype))) {

        /* Reinstall top CollateExpr */
        CollateExpr* coll = (CollateExpr*)origexpr;
        CollateExpr* newcoll = makeNode(CollateExpr);

        newcoll->arg = (Expr*)result;
        newcoll->collOid = coll->collOid;
        newcoll->location = coll->location;
        result = (Node*)newcoll;
    }

    return result;
}

/*
 * coerce_to_target_charset()
 *		Convert an expression to a target character set.
 *
 * pstate - parse state (can be NULL, see semtc_coerce_type)
 * expr - input expression tree (already transformed by semtc_expr)
 * target_charset - desired result character set
 * target_type - desired result type
 */
Node* coerce_to_target_charset(Node* expr, int target_charset, Oid target_type, int32 target_typmod,
    Oid target_collation, bool eval_const)
{
    FuncExpr* fexpr = NULL;
    Node* result = NULL;
    List* args = NIL;
    Const* cons = NULL;
    int exprcharset;
    Oid exprtype;

    if (target_charset == PG_INVALID_ENCODING) {
        return expr;
    }

    exprcharset = exprCharset((Node*)expr);
    if (exprcharset == PG_INVALID_ENCODING) {
        exprcharset = GetDatabaseEncoding();
    }
    if (exprcharset == target_charset) {
        return expr;
    }

    check_type_supports_multi_charset(target_type, false);
    exprtype = exprType(expr);
    /* datatype have no collation and charset */
    if (exprtype != UNKNOWNOID && !OidIsValid(get_typcollation(exprtype))) {
        return expr;
    }
    /* coerce expression to bytea or text first */
    args = list_make1(coerce_type(
        NULL, expr, exprtype, TEXTOID, -1, COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST,
        NULL, NULL, exprLocation(expr)));

    /* construct a convert FuncExpr */
    const char* expr_charset_name = pg_encoding_to_char(exprcharset);
    const char* target_charset_name = pg_encoding_to_char(target_charset);
    cons = makeConst(NAMEOID, -1, InvalidOid, -2, NameGetDatum(expr_charset_name), false, false);
    args = lappend(args, cons);

    cons = makeConst(NAMEOID, -1, InvalidOid, -2, NameGetDatum(target_charset_name), false, false);
    args = lappend(args, cons);

    fexpr = makeFuncExpr(CONVERTFUNCOID, TEXTOID, args, target_collation, InvalidOid, COERCE_IMPLICIT_CAST);

    /* coerce convert expression to original datatype */
    if (target_type == UNKNOWNOID) {
        result = (Node*)fexpr;
    } else {
        result = coerce_type(NULL, (Node*)fexpr, TEXTOID, target_type, target_typmod,
            COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST, NULL, NULL, exprLocation(expr));
    }

    /* set collation after coerce_to_target_type */
    exprSetCollation(result, target_collation);
    if (eval_const && (IsA(expr, Const) || IsA(expr, RelabelType))) {
        result = eval_const_expression_value(NULL, result, NULL);
    }
    return result;
}

/*
 * user_defined variables only store integer, float, bit, string and null,
 * therefor, we convert the constant to the corresponding type.
 * atttypid: datatype
 * isSelect: subquery flag
 */
Node *type_transfer(Node *node, Oid atttypid, bool isSelect)
{
    if (u_sess->hook_cxt.typeTransfer != NULL) {
        return ((typeTransfer)(u_sess->hook_cxt.typeTransfer))(node, atttypid, isSelect);
    }
    Node *result = NULL;
    Const *con = (Const *)node;
    if (con->constisnull) {
        return node;
    }

    switch (atttypid) {
        case BOOLOID:
        case INT1OID:
        case INT2OID:
        case INT4OID:
        case INT8OID:
            result = coerce_type(NULL, node, con->consttype,
                INT8OID, -1, COERCION_IMPLICIT, COERCE_IMPLICIT_CAST, NULL, NULL, -1);
            break;
        case FLOAT4OID:
        case FLOAT8OID:
        case NUMERICOID:
            result = coerce_type(NULL, node, con->consttype,
                FLOAT8OID, -1, COERCION_IMPLICIT, COERCE_IMPLICIT_CAST, NULL, NULL, -1);
            break;
        case BITOID:
            result = coerce_type(NULL, node, con->consttype,
                BITOID, -1, COERCION_IMPLICIT, COERCE_IMPLICIT_CAST, NULL, NULL, -1);
            break;
        case VARBITOID:
            result = coerce_type(NULL, node, con->consttype,
                VARBITOID, -1, COERCION_IMPLICIT, COERCE_IMPLICIT_CAST, NULL, NULL, -1);
            break;
        default:
            if (isSelect) {
                result = node;
            } else {
                Oid collid = exprCollation(node);
                result = coerce_type(NULL, node, con->consttype, TEXTOID, -1, COERCION_IMPLICIT, COERCE_IMPLICIT_CAST,
                    NULL, NULL, -1);
                if (OidIsValid(collid)) {
                    exprSetCollation(result, collid);
                }
            }
            break;
    }

    return result;
}

/*
 * convert expression to const.
 */
Node *const_expression_to_const(Node *node)
{
    Node *result = NULL;
    Const *con = (Const *)node;

    if (nodeTag(node) != T_Const) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_OPERATION),
                errmsg("The value of a user_defined variable must be convertible to a constant.")));
    }

    /* user_defined varibale only stores integer, float, bit, string, null. */
    result = type_transfer(node, con->consttype, false);
    return eval_const_expression_value(NULL, result, NULL);
}

static Datum stringTypeDatum_with_collation(Type tp, char* string, char*fmtstr, char* nlsfmtstr, int32 atttypmod,
    bool can_ignore, Oid collation)
{
    Datum result; 
    int tmp_encoding = get_valid_charset_by_collation(collation);
    int db_encoding = GetDatabaseEncoding();

    if (tmp_encoding == db_encoding) {
        return stringTypeDatum(tp, string, fmtstr, nlsfmtstr, atttypmod, can_ignore);
    }

    DB_ENCODING_SWITCH_TO(tmp_encoding);
    result = stringTypeDatum(tp, string, fmtstr, nlsfmtstr, atttypmod, can_ignore);
    DB_ENCODING_SWITCH_BACK(db_encoding);
    return result;
}

bool targetissqlvariant(Oid targetOid)
{
    if (!DB_IS_CMPT(D_FORMAT)) {
        return false;
    }

    Oid SvOid = get_typeoid(get_namespace_oid(SYS_NAMESPACE_NAME, true), "sql_variant");

    return OidIsValid(SvOid) && targetOid == SvOid;
}

/*
 * coerce_type()
 *		Convert an expression to a different type.
 *
 * The caller should already have determined that the coercion is possible;
 * see can_coerce_type.
 *
 * Normally, no coercion to a typmod (length) is performed here.  The caller
 * must call coerce_type_typmod as well, if a typmod constraint is wanted.
 * (But if the target type is a domain, it may internally contain a
 * typmod constraint, which will be applied inside coerce_to_domain.)
 * In some cases pg_cast specifies a type coercion function that also
 * applies length conversion, and in those cases only, the result will
 * already be properly coerced to the specified typmod.
 *
 * pstate is only used in the case that we are able to resolve the type of
 * a previously UNKNOWN Param.	It is okay to pass pstate = NULL if the
 * caller does not want type information updated for Params.
 *
 * Note: this function must not modify the given expression tree, only add
 * decoration on top of it.  See transformSetOperationTree, for example.
 */
Node* coerce_type(ParseState* pstate, Node* node, Oid inputTypeId, Oid targetTypeId, int32 targetTypeMod,
    CoercionContext ccontext, CoercionForm cformat, char* fmtstr, char* nlsfmtstr, int location)
{
    Node* result = NULL;
    CoercionPathType pathtype;
    Oid funcId;

    if (targetTypeId == inputTypeId || node == NULL) {
        /* no conversion needed */
        return node;
    }
    if (targetTypeId == ANYOID || targetTypeId == ANYELEMENTOID || targetTypeId == ANYNONARRAYOID) {
        /*
         * Assume can_coerce_type verified that implicit coercion is okay.
         *
         * Note: by returning the unmodified node here, we are saying that
         * it's OK to treat an UNKNOWN constant as a valid input for a
         * function accepting ANY, ANYELEMENT, or ANYNONARRAY.	This should be
         * all right, since an UNKNOWN value is still a perfectly valid Datum.
         *
         * NB: we do NOT want a RelabelType here: the exposed type of the
         * function argument must be its actual type, not the polymorphic
         * pseudotype.
         */
        return node;
    }
    if (targetTypeId == ANYARRAYOID || targetTypeId == ANYENUMOID || targetTypeId == ANYRANGEOID) {
        /*
         * Assume can_coerce_type verified that implicit coercion is okay.
         *
         * These cases are unlike the ones above because the exposed type of
         * the argument must be an actual array, enum, or range type.  In
         * particular the argument must *not* be an UNKNOWN constant.  If it
         * is, we just fall through; below, we'll call anyarray_in,
         * anyenum_in, or anyrange_in, which will produce an error.  Also, if
         * what we have is a domain over array, enum, or range, we have to
         * relabel it to its base type.
         *
         * Note: currently, we can't actually see a domain-over-enum here,
         * since the other functions in this file will not match such a
         * parameter to ANYENUM.  But that should get changed eventually.
         */
        if (inputTypeId != UNKNOWNOID) {
            Oid baseTypeId = getBaseType(inputTypeId);
            if (baseTypeId != inputTypeId) {
                RelabelType* r = makeRelabelType((Expr*)node, baseTypeId, -1, InvalidOid, cformat);
                r->location = location;
                return (Node*)r;
            }
            /* Not a domain type, so return it as-is */
            return node;
        }
    }

    if (inputTypeId == UNKNOWNOID && IsA(node, Const)) {
        /*
         * Input is a string constant with previously undetermined type. Apply
         * the target type's typinput function to it to produce a constant of
         * the target type.
         *
         * NOTE: this case cannot be folded together with the other
         * constant-input case, since the typinput function does not
         * necessarily behave the same as a type conversion function. For
         * example, int4's typinput function will reject "1.2", whereas
         * float-to-int type conversion will round to integer.
         *
         * XXX if the typinput function is not immutable, we really ought to
         * postpone evaluation of the function call until runtime. But there
         * is no way to represent a typinput function call as an expression
         * tree, because C-string values are not Datums. (XXX This *is*
         * possible as of 7.3, do we want to do it?)
         */
        Const* con = (Const*)node;
        Const* newcon = makeNode(Const);
        Oid baseTypeId;
        int32 baseTypeMod;
        int32 inputTypeMod;
        Type targetType;
        ParseCallbackState pcbstate;

        /*
         * If the target type is a domain, we want to call its base type's
         * input routine, not domain_in().	This is to avoid premature failure
         * when the domain applies a typmod: existing input routines follow
         * implicit-coercion semantics for length checks, which is not always
         * what we want here.  The needed check will be applied properly
         * inside coerce_to_domain().
         */
        baseTypeMod = targetTypeMod;
        baseTypeId = getBaseTypeAndTypmod(targetTypeId, &baseTypeMod);

        /*
         * For most types we pass typmod -1 to the input routine, because
         * existing input routines follow implicit-coercion semantics for
         * length checks, which is not always what we want here.  Any length
         * constraint will be applied later by our caller.	An exception
         * however is the INTERVAL type, for which we *must* pass the typmod
         * or it won't be able to obey the bizarre SQL-spec input rules. (Ugly
         * as sin, but so is this part of the spec...)
         */
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

        /*
         * We use the original literal's location regardless of the position
         * of the coercion.  This is a change from pre-9.2 behavior, meant to
         * simplify life for pg_stat_statements.
         */
        newcon->location = con->location;

        /*
         * Set up to point at the constant's text if the input routine throws
         * an error.
         */
        setup_parser_errposition_callback(&pcbstate, pstate, con->location);

        /*
        * We assume here that UNKNOWN's internal representation is the same
        * as CSTRING.
        */
        if (!con->constisnull) {
            newcon->constvalue = stringTypeDatum_with_collation(targetType, DatumGetCString(con->constvalue),
                fmtstr, nlsfmtstr, inputTypeMod, pstate != NULL && pstate->p_has_ignore,
                    con->constcollid);
        } else {
            newcon->constvalue =
                stringTypeDatum(targetType, NULL, fmtstr, nlsfmtstr, inputTypeMod,
                    pstate != NULL && pstate->p_has_ignore);
        }

        cancel_parser_errposition_callback(&pcbstate);

        result = (Node*)newcon;

        /* If target is a domain, apply constraints. */
        if (baseTypeId != targetTypeId) {
            result = coerce_to_domain(result, baseTypeId, baseTypeMod, targetTypeId, cformat,
                fmtstr, nlsfmtstr, location, false, false);
        }

        ReleaseSysCache(targetType);

        return result;
    }
    if (inputTypeId == UNKNOWNOID && IsA(node, UserVar) && IsA(((UserVar*)node)->value, Const)) {
        /*
         * Input is a string constant with previously undetermined type. Apply
         * the target type's typinput function to it to produce a constant of
         * the target type.
         *
         * NOTE: this case cannot be folded together with the other
         * constant-input case, since the typinput function does not
         * necessarily behave the same as a type conversion function. For
         * example, int4's typinput function will reject "1.2", whereas
         * float-to-int type conversion will round to integer.
         *
         * XXX if the typinput function is not immutable, we really ought to
         * postpone evaluation of the function call until runtime. But there
         * is no way to represent a typinput function call as an expression
         * tree, because C-string values are not Datums. (XXX This *is*
         * possible as of 7.3, do we want to do it?)
         */
        Const* con = (Const*)((UserVar*)node)->value;
        Const* newcon = makeNode(Const);
        Oid baseTypeId;
        int32 baseTypeMod;
        int32 inputTypeMod;
        Type targetType;
        ParseCallbackState pcbstate;

        /*
         * If the target type is a domain, we want to call its base type's
         * input routine, not domain_in().	This is to avoid premature failure
         * when the domain applies a typmod: existing input routines follow
         * implicit-coercion semantics for length checks, which is not always
         * what we want here.  The needed check will be applied properly
         * inside coerce_to_domain().
         */
        baseTypeMod = targetTypeMod;
        baseTypeId = getBaseTypeAndTypmod(targetTypeId, &baseTypeMod);

        /*
         * For most types we pass typmod -1 to the input routine, because
         * existing input routines follow implicit-coercion semantics for
         * length checks, which is not always what we want here.  Any length
         * constraint will be applied later by our caller.	An exception
         * however is the INTERVAL type, for which we *must* pass the typmod
         * or it won't be able to obey the bizarre SQL-spec input rules. (Ugly
         * as sin, but so is this part of the spec...)
         */
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

        /*
         * We use the original literal's location regardless of the position
         * of the coercion.  This is a change from pre-9.2 behavior, meant to
         * simplify life for pg_stat_statements.
         */
        newcon->location = con->location;

        /*
         * Set up to point at the constant's text if the input routine throws
         * an error.
         */
        setup_parser_errposition_callback(&pcbstate, pstate, con->location);

        /*
        * We assume here that UNKNOWN's internal representation is the same
        * as CSTRING.
        */
        if (!con->constisnull) {
            newcon->constvalue = stringTypeDatum_with_collation(targetType, DatumGetCString(con->constvalue),
                fmtstr, nlsfmtstr, inputTypeMod, pstate != NULL && pstate->p_has_ignore,
                    con->constcollid);
        } else {
            newcon->constvalue =
                stringTypeDatum(targetType, NULL, fmtstr, nlsfmtstr, inputTypeMod,
                    pstate != NULL && pstate->p_has_ignore);
        }

        cancel_parser_errposition_callback(&pcbstate);

        result = (Node*)newcon;

        /* If target is a domain, apply constraints. */
        if (baseTypeId != targetTypeId) {
            result = coerce_to_domain(result, baseTypeId, baseTypeMod, targetTypeId, cformat,
                fmtstr, nlsfmtstr, location, false, false);
        }

        ReleaseSysCache(targetType);

        UserVar *newus = makeNode(UserVar);
        newus->name = ((UserVar*)node)->name;
        newus->value = (Expr*)result;

        pathtype = find_coercion_pathway(targetTypeId, inputTypeId, ccontext, &funcId);
        switch (pathtype) {
            case COERCION_PATH_NONE:
                return (Node *)newus;
                break;
            case COERCION_PATH_RELABELTYPE: {
                result = coerce_to_domain((Node *)newus, InvalidOid, -1, targetTypeId, cformat,
                    fmtstr, nlsfmtstr, location, false, false);
                if (result == (Node *)newus) {
                    RelabelType* r = makeRelabelType((Expr*)result, targetTypeId, -1, InvalidOid, cformat);

                    r->location = location;
                    result = (Node*)r;
                }
                return result;
            } break;
            default: {
                Oid baseTypeId;
                int32 baseTypeMod;

                baseTypeMod = targetTypeMod;
                baseTypeId = getBaseTypeAndTypmod(targetTypeId, &baseTypeMod);

                result = build_coercion_expression((Node *)newus, pathtype, funcId, baseTypeId, 
                    baseTypeMod, cformat, fmtstr, nlsfmtstr, location, (cformat != COERCE_IMPLICIT_CAST));

                if (targetTypeId != baseTypeId) {
                    result = coerce_to_domain(result,
                        baseTypeId,
                        baseTypeMod,
                        targetTypeId,
                        cformat,
                        fmtstr,
                        nlsfmtstr,
                        location,
                        true,
                        exprIsLengthCoercion(result, NULL));
                }
            } break;
        }
    }
    if (IsA(node, Param) && pstate != NULL && pstate->p_coerce_param_hook != NULL) {
        /*
         * Allow the CoerceParamHook to decide what happens.  It can return a
         * transformed node (very possibly the same Param node), or return
         * NULL to indicate we should proceed with normal coercion.
         */
        result = (*pstate->p_coerce_param_hook)(pstate, (Param*)node, targetTypeId, targetTypeMod, location);
        if (result != NULL) {
            return result;
        }
    }
    if (IsA(node, CollateExpr)) {
        /*
         * If we have a COLLATE clause, we have to push the coercion
         * underneath the COLLATE.	This is really ugly, but there is little
         * choice because the above hacks on Consts and Params wouldn't happen
         * otherwise.  This kluge has consequences in coerce_to_target_type.
         */
        CollateExpr* coll = (CollateExpr*)node;
        CollateExpr* newcoll = makeNode(CollateExpr);

        newcoll->arg = (Expr*)coerce_type(
            pstate, (Node*)coll->arg, inputTypeId, targetTypeId, targetTypeMod, ccontext, cformat,
                fmtstr, nlsfmtstr, location);
        newcoll->collOid = coll->collOid;
        newcoll->location = coll->location;
        return (Node*)newcoll;
    }

    if (UNKNOWNOID == inputTypeId && COERCION_IMPLICIT == ccontext) {
        /*
         * force to convert unknown expression to target type, since there is
         * no implicit coercion method for UNKNOWNOID type, or it will throw
         * an exception
         */
        ccontext = COERCION_ASSIGNMENT;
    }
    pathtype = find_coercion_pathway(targetTypeId, inputTypeId, ccontext, &funcId);
    if (pathtype != COERCION_PATH_NONE) {
        if (pathtype != COERCION_PATH_RELABELTYPE) {
            /*
             * Generate an expression tree representing run-time application
             * of the conversion function.	If we are dealing with a domain
             * target type, the conversion function will yield the base type,
             * and we need to extract the correct typmod to use from the
             * domain's typtypmod.
             */
            Oid baseTypeId;
            int32 baseTypeMod;

            baseTypeMod = targetTypeMod;
            baseTypeId = getBaseTypeAndTypmod(targetTypeId, &baseTypeMod);

            result = build_coercion_expression(
                node, pathtype, funcId, baseTypeId, baseTypeMod, cformat, fmtstr, nlsfmtstr,
                    location, (cformat != COERCE_IMPLICIT_CAST));

            /*
             * If domain, coerce to the domain type and relabel with domain
             * type ID.  We can skip the internal length-coercion step if the
             * selected coercion function was a type-and-length coercion.
             */
            if (targetTypeId != baseTypeId)
                result = coerce_to_domain(result,
                    baseTypeId,
                    baseTypeMod,
                    targetTypeId,
                    cformat,
                    fmtstr,
                    nlsfmtstr,
                    location,
                    true,
                    exprIsLengthCoercion(result, NULL));
        } else {
            /*
             * We don't need to do a physical conversion, but we do need to
             * attach a RelabelType node so that the expression will be seen
             * to have the intended type when inspected by higher-level code.
             *
             * Also, domains may have value restrictions beyond the base type
             * that must be accounted for.	If the destination is a domain
             * then we won't need a RelabelType node.
             */
            result = coerce_to_domain(node, InvalidOid, -1, targetTypeId, cformat,
                fmtstr, nlsfmtstr, location, false, false);
            if (result == node) {
                /*
                 * XXX could we label result with exprTypmod(node) instead of
                 * default -1 typmod, to save a possible length-coercion
                 * later? Would work if both types have same interpretation of
                 * typmod, which is likely but not certain.
                 */
                RelabelType* r = makeRelabelType((Expr*)result, targetTypeId, -1, InvalidOid, cformat);

                r->location = location;
                result = (Node*)r;
            }
        }
        return result;
    }
    if (inputTypeId == RECORDOID && ISCOMPLEX(targetTypeId)) {
        /* Coerce a RECORD to a specific complex type */
        return coerce_record_to_complex(pstate, node, targetTypeId, ccontext, cformat, location);
    }
    if (targetTypeId == RECORDOID && ISCOMPLEX(inputTypeId)) {
        /* Coerce a specific complex type to RECORD */
        /* NB: we do NOT want a RelabelType here */
        return node;
    }
#ifdef NOT_USED
    if (inputTypeId == RECORDARRAYOID && is_complex_array(targetTypeId)) {
        /* Coerce record[] to a specific complex array type */
        /* not implemented yet ... */
    }
#endif
    if (targetTypeId == RECORDARRAYOID && is_complex_array(inputTypeId)) {
        /* Coerce a specific complex array type to record[] */
        /* NB: we do NOT want a RelabelType here */
        return node;
    }
    if (typeInheritsFrom(inputTypeId, targetTypeId) || typeIsOfTypedTable(inputTypeId, targetTypeId)) {
        /*
         * Input class type is a subclass of target, so generate an
         * appropriate runtime conversion (removing unneeded columns and
         * possibly rearranging the ones that are wanted).
         */
        ConvertRowtypeExpr* r = makeNode(ConvertRowtypeExpr);

        r->arg = (Expr*)node;
        r->resulttype = targetTypeId;
        r->convertformat = cformat;
        r->location = location;
        return (Node*)r;
    }

    if (targetTypeId == ANYSETOID && type_is_set(inputTypeId)) {
        return node;
    }

    if (type_is_set(inputTypeId) && type_is_set(targetTypeId)) {
        return node;
    }

    /* If we get here, caller blew it */
    ereport(ERROR,
        (errcode(ERRCODE_UNDEFINED_FUNCTION),
            errmsg("failed to find conversion function from %s to %s",
                format_type_be(inputTypeId),
                format_type_be(targetTypeId))));
    return NULL; /* keep compiler quiet */
}

/*
 * can_coerce_type()
 *		Can input_typeids be coerced to target_typeids?
 *
 * We must be told the context (CAST construct, assignment, implicit coercion)
 * as this determines the set of available casts.
 */
bool can_coerce_type(int nargs, Oid* input_typeids, Oid* target_typeids, CoercionContext ccontext)
{
    bool have_generics = false;
    int i;

    /* run through argument list... */
    for (i = 0; i < nargs; i++) {
        Oid inputTypeId = input_typeids[i];
        Oid targetTypeId = target_typeids[i];
        CoercionPathType pathtype;
        Oid funcId;

        /* no problem if same type */
        if (inputTypeId == targetTypeId) {
            continue;
        }

        /* accept if target is ANY */
        if (targetTypeId == ANYOID) {
            continue;
        }

        /* accept if target is polymorphic, for now */
        if (IsPolymorphicType(targetTypeId)) {
            have_generics = true; /* do more checking later */
            continue;
        }

        /*
         * If input is an untyped string constant, assume we can convert it to
         * anything.
         */
        if (inputTypeId == UNKNOWNOID) {
            continue;
        }

        /*
         * If pg_cast shows that we can coerce, accept.  This test now covers
         * both binary-compatible and coercion-function cases.
         */
        pathtype = find_coercion_pathway(targetTypeId, inputTypeId, ccontext, &funcId);
        if (pathtype != COERCION_PATH_NONE) {
            continue;
        }

        /*
         * If input is RECORD and target is a composite type, assume we can
         * coerce (may need tighter checking here)
         */
        if (inputTypeId == RECORDOID && ISCOMPLEX(targetTypeId)) {
            continue;
        }

        /*
         * If input is a composite type and target is RECORD, accept
         */
        if (targetTypeId == RECORDOID && ISCOMPLEX(inputTypeId)) {
            continue;
        }

#ifdef NOT_USED /* not implemented yet */

        /*
         * If input is record[] and target is a composite array type, assume
         * we can coerce (may need tighter checking here)
         */
        if (inputTypeId == RECORDARRAYOID && is_complex_array(targetTypeId)) {
            continue;
        }
#endif

        /*
         * If input is a composite array type and target is record[], accept
         */
        if (targetTypeId == RECORDARRAYOID && is_complex_array(inputTypeId)) {
            continue;
        }

        /*
         * If input is a class type that inherits from target, accept
         */
        if (typeInheritsFrom(inputTypeId, targetTypeId) || typeIsOfTypedTable(inputTypeId, targetTypeId)) {
            continue;
        }

        /*
         * If input or target type is a actual set type, accept if the other is of number or char type value.
         */
        if (targetTypeId == ANYSETOID && type_is_set(inputTypeId)) {
            continue;
        }
        /*
         * If input and target type are different actual set type, accept
         */
        if (type_is_set(targetTypeId) && type_is_set(inputTypeId)) {
            continue;
        }

        /*
         * Else, cannot coerce at this argument position
         */
        return false;
    }

    /* If we found any generic argument types, cross-check them */
    if (have_generics) {
        if (!check_generic_type_consistency(input_typeids, target_typeids, nargs)) {
            return false;
        }
    }

    return true;
}

/*
 * Create an expression tree to represent coercion to a domain type.
 *
 * 'arg': input expression
 * 'baseTypeId': base type of domain, if known (pass InvalidOid if caller
 *		has not bothered to look this up)
 * 'baseTypeMod': base type typmod of domain, if known (pass -1 if caller
 *		has not bothered to look this up)
 * 'typeId': target type to coerce to
 * 'cformat': coercion format
 * 'location': coercion request location
 * 'hideInputCoercion': if true, hide the input coercion under this one.
 * 'lengthCoercionDone': if true, caller already accounted for length,
 *		ie the input is already of baseTypMod as well as baseTypeId.
 *
 * If the target type isn't a domain, the given 'arg' is returned as-is.
 */
Node* coerce_to_domain(Node* arg, Oid baseTypeId, int32 baseTypeMod, Oid typeId, CoercionForm cformat,
    char* fmtstr, char* nlsfmtstr, int location, bool hideInputCoercion, bool lengthCoercionDone)
{
    CoerceToDomain* result = NULL;

    /* Get the base type if it hasn't been supplied */
    if (baseTypeId == InvalidOid) {
        baseTypeId = getBaseTypeAndTypmod(typeId, &baseTypeMod);
    }

    /* If it isn't a domain, return the node as it was passed in */
    if (baseTypeId == typeId) {
        return arg;
    }

    /* Suppress display of nested coercion steps */
    if (hideInputCoercion) {
        hide_coercion_node(arg);
    }

    /*
     * If the domain applies a typmod to its base type, build the appropriate
     * coercion step.  Mark it implicit for display purposes, because we don't
     * want it shown separately by ruleutils.c; but the isExplicit flag passed
     * to the conversion function depends on the manner in which the domain
     * coercion is invoked, so that the semantics of implicit and explicit
     * coercion differ.  (Is that really the behavior we want?)
     *
     * NOTE: because we apply this as part of the fixed expression structure,
     * ALTER DOMAIN cannot alter the typtypmod.  But it's unclear that that
     * would be safe to do anyway, without lots of knowledge about what the
     * base type thinks the typmod means.
     */
    if (!lengthCoercionDone) {
        if (baseTypeMod >= 0) {
            arg = coerce_type_typmod(
                arg, baseTypeId, baseTypeMod, COERCE_IMPLICIT_CAST, fmtstr, nlsfmtstr, location,
                    (cformat != COERCE_IMPLICIT_CAST), false);
        }
    }

    /*
     * Now build the domain coercion node.	This represents run-time checking
     * of any constraints currently attached to the domain.  This also ensures
     * that the expression is properly labeled as to result type.
     */
    result = makeNode(CoerceToDomain);
    result->arg = (Expr*)arg;
    result->resulttype = typeId;
    result->resulttypmod = -1; /* currently, always -1 for domains */
    /* resultcollid will be set by parse_collate.c */
    result->coercionformat = cformat;
    result->location = location;

    return (Node*)result;
}

/*
 * coerce_type_typmod()
 *		Force a value to a particular typmod, if meaningful and possible.
 *
 * This is applied to values that are going to be stored in a relation
 * (where we have an atttypmod for the column) as well as values being
 * explicitly CASTed (where the typmod comes from the target type spec).
 *
 * The caller must have already ensured that the value is of the correct
 * type, typically by applying coerce_type.
 *
 * cformat determines the display properties of the generated node (if any),
 * while isExplicit may affect semantics.  If hideInputCoercion is true
 * *and* we generate a node, the input node is forced to IMPLICIT display
 * form, so that only the typmod coercion node will be visible when
 * displaying the expression.
 *
 * NOTE: this does not need to work on domain types, because any typmod
 * coercion for a domain is considered to be part of the type coercion
 * needed to produce the domain value in the first place.  So, no getBaseType.
 */
static Node* coerce_type_typmod(Node* node, Oid targetTypeId, int32 targetTypMod, CoercionForm cformat, char* fmtstr,
    char* nlsfmtstr, int location, bool isExplicit, bool hideInputCoercion)
{
    CoercionPathType pathtype;
    Oid funcId;

    /*
     * A negative typmod is assumed to mean that no coercion is wanted. Also,
     * skip coercion if already done.
     */
    if (targetTypMod < 0 || targetTypMod == exprTypmod(node)) {
        return node;
    }

    pathtype = find_typmod_coercion_function(targetTypeId, &funcId);

    if (pathtype != COERCION_PATH_NONE) {
        Oid node_collation = exprCollation(node);
        /* Suppress display of nested coercion steps */
        if (hideInputCoercion) {
            hide_coercion_node(node);
        }
        node = build_coercion_expression(
            node, pathtype, funcId, targetTypeId, targetTypMod, cformat, fmtstr, nlsfmtstr,
                location, isExplicit);
        if (OidIsValid(node_collation)) {
            exprSetInputCollation(node, node_collation);
            exprSetCollation(node, node_collation);
        }
    }

    return node;
}

/*
 * Mark a coercion node as IMPLICIT so it will never be displayed by
 * ruleutils.c.  We use this when we generate a nest of coercion nodes
 * to implement what is logically one conversion; the inner nodes are
 * forced to IMPLICIT_CAST format.	This does not change their semantics,
 * only display behavior.
 *
 * It is caller error to call this on something that doesn't have a
 * CoercionForm field.
 */
static void hide_coercion_node(Node* node)
{
    if (IsA(node, FuncExpr)) {
        ((FuncExpr*)node)->funcformat = COERCE_IMPLICIT_CAST;
    } else if (IsA(node, RelabelType)) {
        ((RelabelType*)node)->relabelformat = COERCE_IMPLICIT_CAST;
    } else if (IsA(node, CoerceViaIO)) {
        ((CoerceViaIO*)node)->coerceformat = COERCE_IMPLICIT_CAST;
    } else if (IsA(node, ArrayCoerceExpr)) {
        ((ArrayCoerceExpr*)node)->coerceformat = COERCE_IMPLICIT_CAST;
    } else if (IsA(node, ConvertRowtypeExpr)) {
        ((ConvertRowtypeExpr*)node)->convertformat = COERCE_IMPLICIT_CAST;
    } else if (IsA(node, RowExpr)) {
        ((RowExpr*)node)->row_format = COERCE_IMPLICIT_CAST;
    } else if (IsA(node, CoerceToDomain)) {
        ((CoerceToDomain*)node)->coercionformat = COERCE_IMPLICIT_CAST;
    } else {
        ereport(
            ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized node type: %d", (int)nodeTag(node))));
    }
}

/*
 * build_coercion_expression()
 *		Construct an expression tree for applying a pg_cast entry.
 *
 * This is used for both type-coercion and length-coercion operations,
 * since there is no difference in terms of the calling convention.
 */
static Node* build_coercion_expression(Node* node, CoercionPathType pathtype, Oid funcId, Oid targetTypeId,
    int32 targetTypMod, CoercionForm cformat, char* fmtstr, char* nlsfmtstr, int location, bool isExplicit)
{
    int nargs = 0;

    if (OidIsValid(funcId)) {
        HeapTuple tp;
        Form_pg_proc procstruct;

        tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcId));
        if (!HeapTupleIsValid(tp)) {
            ereport(
                ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", funcId)));
        }
        procstruct = (Form_pg_proc)GETSTRUCT(tp);

        // No need to check Anum_pg_proc_proargtypesext attribute, because cast function
        // will not have more than FUNC_MAX_ARGS_INROW parameters
        Assert(procstruct->pronargs <= FUNC_MAX_ARGS_INROW);

        /*
         * These Asserts essentially check that function is a legal coercion
         * function.  We can't make the seemingly obvious tests on prorettype
         * and proargtypes[0], even in the COERCION_PATH_FUNC case, because of
         * various binary-compatibility cases.
         */
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

    if (pathtype == COERCION_PATH_FUNC) {
        /* We build an ordinary FuncExpr with special arguments */
        FuncExpr* fexpr = NULL;
        List* args = NIL;
        Const* cons = NULL;

        AssertEreport(OidIsValid(funcId), MOD_OPT, "The OID of the function is invalid.");

        args = list_make1(node);

        if (targetissqlvariant(targetTypeId)) {
            /* convert to sql_variant */
            cons = makeConst(INT4OID, -1, InvalidOid, sizeof(int32), Int32GetDatum(exprTypmod(node)), false, true);
            args = lappend(args, cons);
        } else {
            if (nargs >= 2) {
                if (type_is_set(targetTypeId)) {
                    /* Pass actual set id as Oid type */
                    cons = makeConst(OIDOID, -1, InvalidOid, sizeof(Oid), ObjectIdGetDatum(targetTypeId), false, true);
                } else {
                    /* Pass target typmod as an int4 constant */
                    cons = makeConst(INT4OID, -1, InvalidOid, sizeof(int32), Int32GetDatum(targetTypMod), false, true);
                }

                args = lappend(args, cons);
            }

            if (nargs == 3) {
                /* Pass it a boolean isExplicit parameter, too */
                cons = makeConst(BOOLOID, -1, InvalidOid, sizeof(bool), BoolGetDatum(isExplicit), false, true);

                args = lappend(args, cons);
            }
        }

        fexpr = makeFuncExpr(funcId, targetTypeId, args, InvalidOid, InvalidOid, cformat);
        fexpr->location = location;
        fexpr->fmtstr = fmtstr;
        fexpr->nlsfmtstr = nlsfmtstr;
        return (Node*)fexpr;
    } else if (pathtype == COERCION_PATH_ARRAYCOERCE) {
        /* We need to build an ArrayCoerceExpr */
        ArrayCoerceExpr* acoerce = makeNode(ArrayCoerceExpr);

        acoerce->arg = (Expr*)node;
        acoerce->fmtstr = fmtstr;
        acoerce->nlsfmtstr = nlsfmtstr;
        acoerce->elemfuncid = funcId;
        acoerce->resulttype = targetTypeId;

        /*
         * Label the output as having a particular typmod only if we are
         * really invoking a length-coercion function, ie one with more than
         * one argument.
         */
        acoerce->resulttypmod = (nargs >= 2) ? targetTypMod : -1;
        /* resultcollid will be set by parse_collate.c */
        acoerce->isExplicit = isExplicit;
        acoerce->coerceformat = cformat;
        acoerce->location = location;

        return (Node*)acoerce;
    } else if (pathtype == COERCION_PATH_COERCEVIAIO) {
        /* We need to build a CoerceViaIO node */
        CoerceViaIO* iocoerce = makeNode(CoerceViaIO);

        AssertEreport(!OidIsValid(funcId), MOD_OPT, "The OID of the function is invalid.");

        iocoerce->arg = (Expr*)node;
        iocoerce->fmtstr = fmtstr;
        iocoerce->nlsfmtstr = nlsfmtstr;
        iocoerce->resulttype = targetTypeId;
        /* resultcollid will be set by parse_collate.c */
        iocoerce->coerceformat = cformat;
        iocoerce->location = location;

        return (Node*)iocoerce;
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unsupported pathtype %d in build_coercion_expression", (int)pathtype)));
        return NULL; /* keep compiler quiet */
    }
}

static void release_check_tupdesc(TupleDesc src_tupdesc, TupleDesc tar_tupdesc)
{
    if (src_tupdesc) {
        ReleaseTupleDesc(src_tupdesc);
    }
    if (tar_tupdesc) {
        ReleaseTupleDesc(tar_tupdesc);
    }
}

static bool check_cast_record_type(Oid sourceType, Oid targetType)
{
    TupleDesc src_tupdesc = lookup_rowtype_tupdesc(sourceType, -1);
    TupleDesc tar_tupdesc = lookup_rowtype_tupdesc(targetType, -1);
    int satts = src_tupdesc ? src_tupdesc->natts : 0;
    int tatts = tar_tupdesc ? tar_tupdesc->natts : 0;
    int fnum = 0;
    int anum = 0;

    if (src_tupdesc == tar_tupdesc) {
        release_check_tupdesc(src_tupdesc, tar_tupdesc);
        return true;
    }
    
    if (tatts != satts) {
        release_check_tupdesc(src_tupdesc, tar_tupdesc);
        return false;
    }

    /* Walk over destination columns */
    for (fnum = 0; fnum < satts; fnum++) {
        Form_pg_attribute sattr = TupleDescAttr(src_tupdesc, fnum);
        Form_pg_attribute tattr = NULL;
        Oid reqtype, valtype;
        int32 smod, tmod;

        if (sattr->attisdropped) {
            continue;
        }

        while (anum < tatts && TupleDescAttr(tar_tupdesc, anum)->attisdropped) {
            anum++; /* skip dropped column in tuple */
        }

        if (anum < tatts) {
            Form_pg_attribute tattr = TupleDescAttr(tar_tupdesc, anum);
            reqtype = tattr->atttypid;
            tmod = tattr->atttypmod;
            smod = sattr->atttypmod;
            valtype = sattr->atttypid;
            anum++;
            if (valtype != reqtype || smod != tmod) {
                release_check_tupdesc(src_tupdesc, tar_tupdesc);
                return false;
            }
        } else {
            release_check_tupdesc(src_tupdesc, tar_tupdesc);
            return false;
        }
    }
    release_check_tupdesc(src_tupdesc, tar_tupdesc);
    return true;
}

/*
 * coerce_record_to_complex
 *		Coerce a RECORD to a specific composite type.
 *
 * Currently we only support this for inputs that are RowExprs or whole-row
 * Vars.
 */
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

    if (node && IsA(node, RowExpr)) {
        /*
         * Since the RowExpr must be of type RECORD, we needn't worry about it
         * containing any dropped columns.
         */
        args = ((RowExpr*)node)->args;
    } else if (node && IsA(node, Var) && ((Var*)node)->varattno == InvalidAttrNumber) {
        int rtindex = ((Var*)node)->varno;
        int sublevels_up = ((Var*)node)->varlevelsup;
        int vlocation = ((Var*)node)->location;
        RangeTblEntry* rte = NULL;

        rte = GetRTEByRangeTablePosn(pstate, rtindex, sublevels_up);
        expandRTE(rte, rtindex, sublevels_up, vlocation, false, NULL, &args);
    } else if( node && IsA(node, Param) && ((Param*)node)->paramkind == PARAM_EXTERN
            && ((Param*)node)->paramtype == RECORDOID && ((Param*)node)->recordVarTypOid == UNKNOWNOID) {
        args = ((Param*)node)->args;
    } else {
        bool isParamExtenRecord = node && IsA(node, Param) && ((Param*)node)->paramkind == PARAM_EXTERN
            && ((Param*)node)->paramtype == RECORDOID;
        if (isParamExtenRecord) {
            /* package record var'type same with target type, no need cast, just return */
            if (((Param*)node)->recordVarTypOid == targetTypeId) {
                return node;
            } else if (check_cast_record_type(((Param*)node)->recordVarTypOid, targetTypeId)) {
                return node;
            }
        }
        ereport(ERROR,
            (errcode(ERRCODE_CANNOT_COERCE),
                errmsg("cannot cast type %s to %s", format_type_be(RECORDOID), format_type_be(targetTypeId)),
                parser_coercion_errposition(pstate, location, node)));
    }

    tupdesc = lookup_rowtype_tupdesc(targetTypeId, -1);
    newargs = NIL;
    ucolno = 1;
    arg = list_head(args);
    for (i = 0; i < tupdesc->natts; i++) {
        Node* expr = NULL;
        Node* cexpr = NULL;
        Oid exprtype;

        /* Fill in NULLs for dropped columns in rowtype */
        if (tupdesc->attrs[i].attisdropped) {
            /*
             * can't use atttypid here, but it doesn't really matter what type
             * the Const claims to be.
             */
            newargs = lappend(newargs, makeNullConst(INT4OID, -1, InvalidOid));
            continue;
        }

        if (arg == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_CANNOT_COERCE),
                    errmsg("cannot cast type %s to %s", format_type_be(RECORDOID), format_type_be(targetTypeId)),
                    errdetail("Input has too few columns."),
                    parser_coercion_errposition(pstate, location, node)));
        }
        expr = (Node*)lfirst(arg);
        exprtype = exprType(expr);
        cexpr = coerce_to_target_type(pstate,
            expr,
            exprtype,
            tupdesc->attrs[i].atttypid,
            tupdesc->attrs[i].atttypmod,
            ccontext,
            COERCE_IMPLICIT_CAST,
            NULL,
            NULL,
            -1);
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
    if (arg != NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_CANNOT_COERCE),
                errmsg("cannot cast type %s to %s", format_type_be(RECORDOID), format_type_be(targetTypeId)),
                errdetail("Input has too many columns."),
                parser_coercion_errposition(pstate, location, node)));
    }

    ReleaseTupleDesc(tupdesc);

    rowexpr = makeNode(RowExpr);
    rowexpr->args = newargs;
    rowexpr->row_typeid = targetTypeId;
    rowexpr->row_format = cformat;
    rowexpr->colnames = NIL; /* not needed for named target type */
    rowexpr->location = location;
    return (Node*)rowexpr;
}

/*
 * coerce_to_boolean()
 *		Coerce an argument of a construct that requires boolean input
 *		(AND, OR, NOT, etc).  Also check that input is not a set.
 *
 * Returns the possibly-transformed node tree.
 *
 * As with coerce_type, pstate may be NULL if no special unknown-Param
 * processing is wanted.
 */
Node* coerce_to_boolean(ParseState* pstate, Node* node, const char* constructName)
{
    Oid inputTypeId = exprType(node);

    if (inputTypeId != BOOLOID) {
        Node* newnode = NULL;
        newnode = coerce_to_target_type(
            pstate, node, inputTypeId, BOOLOID, -1, COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST, NULL, NULL, -1);
        if (newnode == NULL)
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    /* translator: first %s is name of a SQL construct, eg WHERE */
                    errmsg(
                        "argument of %s must be type boolean, not type %s", constructName, format_type_be(inputTypeId)),
                    parser_errposition(pstate, exprLocation(node))));
        node = newnode;
    }

    if (expression_returns_set(node)) {
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                /* translator: %s is name of a SQL construct, eg WHERE */
                errmsg("argument of %s must not return a set", constructName),
                parser_errposition(pstate, exprLocation(node))));
    }
    return node;
}

/*
 * coerce_to_specific_type()
 *		Coerce an argument of a construct that requires a specific data type.
 *		Also check that input is not a set.
 *
 * Returns the possibly-transformed node tree.
 *
 * As with coerce_type, pstate may be NULL if no special unknown-Param
 * processing is wanted.
 */
Node* coerce_to_specific_type(ParseState* pstate, Node* node, Oid targetTypeId, const char* constructName)
{
    Oid inputTypeId = exprType(node);

    if (inputTypeId != targetTypeId) {
        Node* newnode = NULL;

        newnode = coerce_to_target_type(
            pstate, node, inputTypeId, targetTypeId, -1, COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST, NULL, NULL, -1);
        if (newnode == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    /* translator: first %s is name of a SQL construct, eg LIMIT */
                    errmsg("argument of %s must be type %s, not type %s",
                        constructName,
                        format_type_be(targetTypeId),
                        format_type_be(inputTypeId)),
                    parser_errposition(pstate, exprLocation(node))));
        }
        node = newnode;
    }

    if (expression_returns_set(node)) {
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                /* translator: %s is name of a SQL construct, eg LIMIT */
                errmsg("argument of %s must not return a set", constructName),
                parser_errposition(pstate, exprLocation(node))));
    }
    return node;
}

Node* coerce_to_settype(ParseState* pstate, Node* expr, Oid exprtype, Oid targettype, int32 targettypmod,
CoercionContext ccontext, CoercionForm cformat, int location, Oid collation)
{
    if (!can_coerce_type(1, &exprtype, &targettype, ccontext)) {
        return NULL;
    }

    if (exprtype == targettype || expr == NULL) {
        /* no conversion needed */
        return expr;
    }

    Node* result = NULL;
    CoercionPathType pathtype;
    Oid funcId;

    if (exprtype == UNKNOWNOID && IsA(expr, Const)) {
        Const* con = (Const*)expr;
        Const* newcon = makeNode(Const);

        int32 baseTypeMod = targettypmod;
        Oid baseTypeId = getBaseTypeAndTypmod(targettype, &baseTypeMod);
        int32 inputTypeMod = -1;
        Type target = typeidType(baseTypeId);
        ParseCallbackState pcbstate;

        newcon->consttype = baseTypeId;
        newcon->consttypmod = inputTypeMod;
        newcon->constcollid = typeTypeCollation(target);
        newcon->constlen = typeLen(target);
        newcon->constbyval = typeByVal(target);
        newcon->constisnull = con->constisnull;
        newcon->cursor_data.cur_dno = -1;
        newcon->location = con->location;
        setup_parser_errposition_callback(&pcbstate, pstate, con->location);

        Form_pg_type typform = (Form_pg_type)GETSTRUCT(target);
        Oid typinput = typform->typinput;
        Oid typioparam = getTypeIOParam(target);
        newcon->constvalue = OidInputFunctionCallColl(typinput, DatumGetCString(con->constvalue), typioparam, inputTypeMod, collation);

        cancel_parser_errposition_callback(&pcbstate);
        result = (Node*)newcon;
        ReleaseSysCache(target);

        result = coerce_type_typmod(result,
            targettype,
            targettypmod,
            cformat,
            NULL,
            NULL,
            location,
            (cformat != COERCE_IMPLICIT_CAST),
            (result != expr && !IsA(result, Const)));

        return result;
    }

    pathtype = find_coercion_pathway(targettype, exprtype, ccontext, &funcId);
    if (pathtype != COERCION_PATH_NONE) {
        if (pathtype != COERCION_PATH_RELABELTYPE) {
            Oid baseTypeId;
            int32 baseTypeMod;

            baseTypeMod = targettypmod;
            baseTypeId = getBaseTypeAndTypmod(targettype, &baseTypeMod);

            result = build_coercion_expression(
                expr, pathtype, funcId, baseTypeId, baseTypeMod, cformat, NULL, NULL,
                location, (cformat != COERCE_IMPLICIT_CAST));

            if (targettype != baseTypeId)
                result = coerce_to_domain(result,
                    baseTypeId,
                    baseTypeMod,
                    targettype,
                    cformat,
                    NULL,
                    NULL,
                    location,
                    true,
                    exprIsLengthCoercion(result, NULL));
        } else {
            result = coerce_to_domain(expr, InvalidOid, -1, targettype, cformat,
                NULL, NULL, location, false, false);
            if (result == expr) {
                RelabelType* r = makeRelabelType((Expr*)result, targettype, -1, InvalidOid, cformat);

                r->location = location;
                result = (Node*)r;
            }
        }

        result = coerce_type_typmod(result,
            targettype,
            targettypmod,
            cformat,
            NULL,
            NULL,
            location,
            (cformat != COERCE_IMPLICIT_CAST),
            (result != expr && !IsA(result, Const)));

        return result;
    }

    if (targettype == ANYSETOID && type_is_set(exprtype)) {
        return expr;
    }

    if (type_is_set(exprtype) && type_is_set(targettype)) {
        return expr;
    }

    ereport(ERROR,
        (errcode(ERRCODE_UNDEFINED_FUNCTION),
            errmsg("failed to find conversion function from %s to %s",
                format_type_be(exprtype),
                format_type_be(targettype))));
    return NULL;
}

/*
 * parser_coercion_errposition - report coercion error location, if possible
 *
 * We prefer to point at the coercion request (CAST, ::, etc) if possible;
 * but there may be no such location in the case of an implicit coercion.
 * In that case point at the input expression.
 *
 * XXX possibly this is more generally useful than coercion errors;
 * if so, should rename and place with parser_errposition.
 */
int parser_coercion_errposition(ParseState* pstate, int coerce_location, Node* input_expr)
{
    if (coerce_location >= 0) {
        return parser_errposition(pstate, coerce_location);
    } else {
        return parser_errposition(pstate, exprLocation(input_expr));
    }
}

/* choose_decode_result1_type
 * Choose case when and decode return value type in A_FORMAT.
 */
static Oid choose_decode_result1_type(ParseState* pstate, List* exprs, const char* context)
{
    Node* preferExpr = NULL;
    Oid preferType = UNKNOWNOID;
    TYPCATEGORY preferCategory = TYPCATEGORY_UNKNOWN;
    ListCell* lc = NULL;

    /* result1 is the first expr, treat result1 type (or category) as return value type */
    foreach (lc, exprs) {
        preferExpr = (Node*)lfirst(lc);

        /* if first expr is "null", treat it as unknown type */
        if (IsA(preferExpr, Const) && ((Const*)preferExpr)->constisnull) {
            break;
        }

        preferType = getBaseType(exprType(preferExpr));
        preferCategory = get_typecategory(preferType);
        break;
    }
    if (lc == NULL) {
        return preferType;
    }

    lc = lnext(lc);
    for_each_cell(lc, lc)
    {
        Node* nextExpr = (Node*)lfirst(lc);
        Oid nextType = getBaseType(exprType(nextExpr));

        /* skip "null" */
        if (IsA(nextExpr, Const) && ((Const*)nextExpr)->constisnull) {
            continue;
        }

        /* no need to check if nextType the same as preferType */
        if (nextType != preferType) {
            TYPCATEGORY nextCategory = get_typecategory(nextType);

            /*
            * Both types in different categories, we check if nextCategory/nextType can be implicitly
            * converted to preferCategory/preferType. Here we will treat unknow type as text type.
            */
            if (nextCategory != preferCategory) {
                handle_diff_category(pstate, nextExpr, context, preferCategory, nextCategory, preferType, nextType);
            }
            /* both types is same categories, we choose a priority higher. */
            else if (GetPriority(preferType) < GetPriority(nextType)) {
                preferType = nextType;
            }
        }
    }

    /*
     * If preferCategory is TYPCATEGORY_NUMERIC, choose NUMERICOID as preferType.
     * To compatible with a string representing a large number needs to be converted to a number type.
     * e.g., "select decode(1, 2, 2, '63274723794832454677432493248593478549543535453'::text);"
     */
    if (preferCategory == TYPCATEGORY_NUMERIC) {
        preferType = NUMERICOID;
    }

    return preferType;
}

/*
 * Handle the case where nextCategory and preferCategory are different.
 * Check whether nextCategory can be converted to preferCategory,
 * or nextType can be converted to preferType.
 */
static void handle_diff_category(ParseState* pstate, Node* nextExpr, const char* context,
    TYPCATEGORY preferCategory, TYPCATEGORY nextCategory, Oid preferType, Oid nextType)
{
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

/*
 * Check whether nextCategory can be converted to preferCategory.
 */
static bool category_can_be_matched(TYPCATEGORY preferCategory, TYPCATEGORY nextCategory)
{
    bool can_be_matched = false;

    static TYPCATEGORY categoryMatchedList[][2] = {
        {TYPCATEGORY_STRING, TYPCATEGORY_UNKNOWN}, {TYPCATEGORY_STRING, TYPCATEGORY_NUMERIC},
        {TYPCATEGORY_UNKNOWN, TYPCATEGORY_STRING}, {TYPCATEGORY_UNKNOWN, TYPCATEGORY_NUMERIC},
        {TYPCATEGORY_NUMERIC, TYPCATEGORY_STRING}, {TYPCATEGORY_NUMERIC, TYPCATEGORY_UNKNOWN},
        {TYPCATEGORY_STRING, TYPCATEGORY_DATETIME}, {TYPCATEGORY_STRING, TYPCATEGORY_TIMESPAN},
        {TYPCATEGORY_UNKNOWN, TYPCATEGORY_DATETIME}, {TYPCATEGORY_UNKNOWN, TYPCATEGORY_TIMESPAN},
        {TYPCATEGORY_DATETIME, TYPCATEGORY_UNKNOWN}};

    for (unsigned int i = 0; i < sizeof(categoryMatchedList) / sizeof(categoryMatchedList[0]); i++) {
        if (preferCategory == categoryMatchedList[i][0] && nextCategory == categoryMatchedList[i][1]) {
            can_be_matched = true;
            break;
        }
    }

    return can_be_matched;
}

/*
 * Check whether nextType can be converted to preferType.
 */
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

/* choose_specific_expr_type
 * Choose case when and coalesce return value type in C_FORMAT.
 */
static Oid choose_specific_expr_type(ParseState* pstate, List* exprs, const char* context)
{
    Node* preferExpr = NULL;
    Oid preferType = UNKNOWNOID;
    TYPCATEGORY preferCategory;
    bool pispreferred = false;
    ListCell* lc = NULL;

    /* locate first not "null" expr */
    foreach (lc, exprs) {
        preferExpr = (Node*)lfirst(lc);

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

    lc = lnext(lc);
    for_each_cell(lc, lc)
    {
        Node* nextExpr = (Node*)lfirst(lc);
        Oid nextType = getBaseType(exprType(nextExpr));

        /* skip "null" */
        if (IsA(nextExpr, Const) && ((Const*)nextExpr)->constisnull) {
            continue;
        }

        /* move on to next one if no new information... */
        if (nextType != preferType) {
            TYPCATEGORY nextCategory;
            bool nispreferred = false;

            get_type_category_preferred(nextType, &nextCategory, &nispreferred);

            /*
             * Both types in different categories, if they are numeric and string type
             * expr will return string type. Here we will treat unknow type as text type.
             */
            if (nextCategory != preferCategory) {
                /* Number and string mix, will return string type*/
                if (preferCategory == TYPCATEGORY_NUMERIC &&
                    (nextCategory == TYPCATEGORY_STRING || nextCategory == TYPCATEGORY_UNKNOWN)) {
                    preferType = nextType;
                    preferCategory = nextCategory;
                }
                /* Unknown and string mix, we will choose unknown type, Finally unknow will be treated as text type*/
                else if (preferCategory == TYPCATEGORY_STRING && nextCategory == TYPCATEGORY_UNKNOWN) {
                    preferType = nextType;
                    preferCategory = nextCategory;
                } else {
                    /* Number and string mix, will return string type.
                     * Unknown and string mix, we will choose unknown type, Finally unknow will be treated as text type.
                     * we nothing to do when othercondition is true, current preferCategory is prefer type.
                     */
                    bool othercondition =
                        ((preferCategory == TYPCATEGORY_STRING || preferCategory == TYPCATEGORY_UNKNOWN) &&
                            nextCategory == TYPCATEGORY_NUMERIC) ||
                        (preferCategory == TYPCATEGORY_UNKNOWN && nextCategory == TYPCATEGORY_STRING);
                    /* Not to deal with other categories mix*/
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
            /* Both types is same categories, we choose a priority higher*/
            else if (GetPriority(preferType) < GetPriority(nextType)) {
                /* take new type if can coerce to it implicitly but not the
                 * other way; but if we have a preferred type, stay on it.
                 */
                preferType = nextType;
                preferCategory = nextCategory;
            }
        }
    }
    return preferType;
}

/* choose_nvl_type
 * Get the datatype of the return value of nvl.
 */
static Oid choose_nvl_type(ParseState* pstate, List* exprs, const char* context)
{
    Node* pexpr = NULL;
    Oid ptype;
    TYPCATEGORY pcategory;
    bool pispreferred = false;

    Node* nexpr = NULL;
    Oid ntype;
    TYPCATEGORY ncategory;
    bool nispreferred = false;

    AssertEreport((list_length(exprs) == 2), MOD_OPT, "The length of the expression is not equal to 2");

    pexpr = (Node*)linitial(exprs);
    ptype = getBaseType(exprType(pexpr));

    nexpr = (Node*)lsecond(exprs);
    ntype = getBaseType(exprType(nexpr));

    get_type_category_preferred(ptype, &pcategory, &pispreferred);
    get_type_category_preferred(ntype, &ncategory, &nispreferred);

    if (ptype == UNKNOWNOID || ntype == UNKNOWNOID) {
        /* Return the datatype of first parameter */
        return ptype;
    } else if (pcategory != ncategory) {
        /* supports implicit convert */
        if (can_coerce_type(1, &ntype, &ptype, COERCION_IMPLICIT)) {
            /* nothing to do, we will return ptype */
        } else if (can_coerce_type(1, &ptype, &ntype, COERCION_IMPLICIT)) {
            ptype = ntype;
            pcategory = ncategory;
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg(
                        "%s types %s and %s cannot be matched", context, format_type_be(ptype), format_type_be(ntype)),
                    parser_errposition(pstate, exprLocation(nexpr))));
        }
    } else if (!pispreferred && can_coerce_type(1, &ptype, &ntype, COERCION_IMPLICIT)) {
        if (!can_coerce_type(1, &ntype, &ptype, COERCION_IMPLICIT) ||
            (TYPCATEGORY_NUMERIC == ncategory && GetPriority(ptype) < GetPriority(ntype))) {
            ptype = ntype;
            pcategory = ncategory;
        }
    }

    return ptype;
}

/* choose_expr_type
 * Get the datatype of union, case, and related Constructs.
 */
static Oid choose_expr_type(ParseState* pstate, List* exprs, const char* context, Node** which_expr)
{
    Node* pexpr = NULL;
    Oid ptype;
    TYPCATEGORY pcategory;
    bool pispreferred = false;
    ListCell* lc = NULL;

    AssertEreport((exprs != NIL), MOD_OPT, "The expression is not NULL");
    pexpr = (Node*)linitial(exprs);
    lc = lnext(list_head(exprs));
    ptype = exprType(pexpr);

    /*
     * Nope, so set up for the full algorithm. Note that at this point, lc
     * points to the first list item with type different from pexpr's; we need
     * not re-examine any items the previous loop advanced over.
     */
    ptype = getBaseType(ptype);
    get_type_category_preferred(ptype, &pcategory, &pispreferred);

    for_each_cell(lc, lc)
    {
        Node* nexpr = (Node*)lfirst(lc);
        Oid ntype = getBaseType(exprType(nexpr));

        /* move on to next one if no new information... */
        if (ntype != UNKNOWNOID && ntype != ptype) {
            TYPCATEGORY ncategory;
            bool nispreferred = false;

            get_type_category_preferred(ntype, &ncategory, &nispreferred);
            if (ptype == UNKNOWNOID) {
                /* so far, only unknowns so take anything... */
                pexpr = nexpr;
                ptype = ntype;
                pcategory = ncategory;
                pispreferred = nispreferred;
            } else if (ncategory != pcategory) {
                /*
                 * both types in different categories? then not much hope...
                 */
                if (context == NULL)
                    return InvalidOid;
                /*
                 * Skip the check only when the parameter is true 
                 * and the expression is WITIN GROUP
                 * This processing is to solve the aggregate function's 
                 * (cume_dist,rank,dense_rank,percent_rank)
                 * direct args and hypothetical args type is not match.
                 */
                else if (u_sess->attr.attr_sql.sql_compatibility != A_FORMAT ||
                        !(u_sess->attr.attr_common.enable_aggr_coerce_type &&
                        CHECK_PARSE_PHRASE(context, "WITHIN GROUP")))
                    ereport(ERROR,
                        (errcode(ERRCODE_DATATYPE_MISMATCH),
                            /* ------
                              translator: first %s is name of a SQL construct, eg CASE */
                            errmsg("%s types %s and %s cannot be matched",
                                context,
                                format_type_be(ptype),
                                format_type_be(ntype)),
                            parser_errposition(pstate, exprLocation(nexpr))));
            }
            /* To same categories, we choose a priority higher*/
            else if (!pispreferred && can_coerce_type(1, &ptype, &ntype, COERCION_IMPLICIT)) {

                if (!can_coerce_type(1, &ntype, &ptype, COERCION_IMPLICIT) ||
                    (TYPCATEGORY_NUMERIC == ncategory && GetPriority(ptype) < GetPriority(ntype))) {
                    /*
                     * take new type if can coerce to it implicitly but not the
                     * other way; but if we have a preferred type, stay on it.
                     */
                    pexpr = nexpr;
                    ptype = ntype;
                    pcategory = ncategory;
                    pispreferred = nispreferred;
                }
            }
        }
    }
    if (which_expr != NULL) {
        *which_expr = pexpr;
    }
    return ptype;
}

/*
 * To maintain forward compatibility, decode type conversion rules compatible with O is
 * only valid for a few specific type categories. Save these type categories in the form
 * of a whitelist. If any one is not in the whitelist, allInWhitelist is set to false.
 */
bool check_all_in_whitelist(List* resultexprs)
{
    bool allInWhitelist = true;
    Node* exprTmp = NULL;
    ListCell* lc = NULL;
    Oid exprTypeTmp = UNKNOWNOID;
    TYPCATEGORY exprCategoryTmp = TYPCATEGORY_UNKNOWN;

    foreach (lc, resultexprs) {
        exprTmp = (Node*)lfirst(lc);

        /* if exprTmp is "null", treat it as unknown type, can skip it. */
        if (IsA(exprTmp, Const) && ((Const*)exprTmp)->constisnull) {
            continue;
        }

        exprTypeTmp = getBaseType(exprType(exprTmp));
        exprCategoryTmp = get_typecategory(exprTypeTmp);
        if (!check_category_in_whitelist(exprCategoryTmp, exprTypeTmp)) {
            allInWhitelist = false;
            break;
        }
    }

    return allInWhitelist;
}

/*
 * Check whether the given category and type is in the whitelist.
 */
static bool check_category_in_whitelist(TYPCATEGORY category, Oid type)
{
    bool categoryInWhitelist = false;

    static TYPCATEGORY categoryWhitelist[] = {TYPCATEGORY_BOOLEAN, TYPCATEGORY_NUMERIC, TYPCATEGORY_STRING,
        TYPCATEGORY_UNKNOWN, TYPCATEGORY_DATETIME, TYPCATEGORY_TIMESPAN, TYPCATEGORY_USER};
    
    for (unsigned int i = 0; i < sizeof(categoryWhitelist) / sizeof(categoryWhitelist[0]); i++) {
        if (category == categoryWhitelist[i]) {
            /*
             * For TYPCATEGORY_USER, just RAW in the whitelist.
             * For TYPCATEGORY_NUMERIC, some numeric type not in the whitelist.
             */
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

/*
 * Check whether the given numeric type is in the blacklist.
 */
static bool check_numeric_type_in_blacklist(Oid type)
{
    bool typeInBlacklist = false;

    static Oid numericTypeBlacklist[] = {CASHOID, INT16OID, REGPROCOID, OIDOID, REGPROCEDUREOID,
        REGOPEROID, REGOPERATOROID, REGCLASSOID, REGTYPEOID, REGCONFIGOID, REGDICTIONARYOID};

    for (unsigned int i = 0; i < sizeof(numericTypeBlacklist) / sizeof(numericTypeBlacklist[0]); i++) {
        if (type == numericTypeBlacklist[i]) {
            typeInBlacklist = true;
            break;
        }
    }

    return typeInBlacklist;
}

/*
 * select_common_type()
 *		Determine the common supertype of a list of input expressions.
 *		This is used for determining the output type of CASE, UNION,
 *		and similar constructs.
 *
 * 'exprs' is a *nonempty* list of expressions.  Note that earlier items
 * in the list will be preferred if there is doubt.
 * 'context' is a phrase to use in the error message if we fail to select
 * a usable type.  Pass NULL to have the routine return InvalidOid
 * rather than throwing an error on failure.
 * 'which_expr': if not NULL, receives a pointer to the particular input
 * expression from which the result type was taken.
 */
Oid select_common_type(ParseState* pstate, List* exprs, const char* context, Node** which_expr)
{
    Node* pexpr = NULL;
    Oid ptype;
    ListCell* lc = NULL;

    AssertEreport((exprs != NIL), MOD_OPT, "The expression is not NULL");
    pexpr = (Node*)linitial(exprs);
    lc = lnext(list_head(exprs));
    ptype = exprType(pexpr);

    if (meet_set_type_compatibility(exprs, context, &ptype)) {
        if (which_expr != NULL)
            *which_expr = pexpr;
        /* need return textoid even all the set types are the same */
        return ptype;
    }

    /*
     * If all input types are valid and exactly the same, just pick that type.
     * This is the only way that we will resolve the result as being a domain
     * type; otherwise domains are smashed to their base types for comparison.
     */
    if (ptype != UNKNOWNOID) {
        for_each_cell(lc, lc)
        {
            Node* nexpr = (Node*)lfirst(lc);
            Oid ntype = exprType(nexpr);

            if (ntype != ptype) {
                break;
            }
        }
        /* got to the end of the list? */
        if (lc == NULL) {
            if (which_expr != NULL) {
                *which_expr = pexpr;
            }
            return ptype;
        }
    }

    if (meet_decode_compatibility(exprs, context)) {
        /*
         * For A format, result1 is considered the most significant type in determining preferred type.
         * In this function, try to choose a higher priority type of the same category as result1.
         * And check whether other parameters can be implicitly converted to the data type of result1.
         */
        ptype = choose_decode_result1_type(pstate, exprs, context);
    } else if (meet_c_format_compatibility(exprs, context)) {
        /*
         * To C format, we need handle numeric and string mix situation.
         * For A format, type should be coerced by the first case, therefore, it can accept cases like
         *          select decode(1, 2, 'a', 3);
         * where the default value 3 will be cast to the first value "a"'s type category. We temporarily fix this by
         * using C format coercion.
         */
        ptype = choose_specific_expr_type(pstate, exprs, context);
    } else if (context != NULL && CHECK_PARSE_PHRASE(context, "NVL")) {
        /* Follow A db nvl*/
        ptype = choose_nvl_type(pstate, exprs, context);
    } else {
        ptype = choose_expr_type(pstate, exprs, context, which_expr);
    }

    /*
     * If all the inputs were UNKNOWN type --- ie, unknown-type literals ---
     * then resolve as type TEXT.  This situation comes up with constructs
     * like SELECT (CASE WHEN foo THEN 'bar' ELSE 'baz' END); SELECT 'foo'
     * UNION SELECT 'bar'; It might seem desirable to leave the construct's
     * output type as UNKNOWN, but that really doesn't work, because we'd
     * probably end up needing a runtime coercion from UNKNOWN to something
     * else, and we usually won't have it.  We need to coerce the unknown
     * literals while they are still literals, so a decision has to be made
     * now.
     */
    if (ptype == UNKNOWNOID) {
        ptype = TEXTOID;
    }
    if (which_expr != NULL) {
        *which_expr = pexpr;
    }
    return ptype;
}

/*
 * Check meet the decode type conversion rules compatibility or not.
 */
static bool meet_decode_compatibility(List* exprs, const char* context)
{
    bool res = u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && ENABLE_SQL_BETA_FEATURE(A_STYLE_COERCE) &&
        context != NULL && CHECK_PARSE_PHRASE(context, "DECODE") && check_all_in_whitelist(exprs);

    return res;
}

/*
 * Check meet the c format compatibility or not.
 * For A format, some temporary are also in it.
 */
static bool meet_c_format_compatibility(List* exprs, const char* context)
{
    bool res = (u_sess->attr.attr_sql.sql_compatibility == C_FORMAT && context != NULL &&
                (CHECK_PARSE_PHRASE(context, "CASE") || CHECK_PARSE_PHRASE(context, "COALESCE") ||
                 CHECK_PARSE_PHRASE(context, "DECODE"))) ||
               (ENABLE_SQL_BETA_FEATURE(A_STYLE_COERCE) && context != NULL && CHECK_PARSE_PHRASE(context, "DECODE"));
    return res;
}

/*
 * Check the expression list contains set type or not.
 * If true, common type is text for CASE/GREATEST/LEAST/NVL/COALESCE/VALUES/DECODE,
 * but we do not need common type for IN/NOT IN expression, left expression compares
 * each in list.
 */
static bool meet_set_type_compatibility(List* exprs, const char* context, Oid *retOid)
{
    Node* expr = NULL;
    ListCell* lc = NULL;
    Oid typOid = UNKNOWNOID;

    bool inCtx = (context != NULL && strcmp(context, "IN") == 0);

    foreach (lc, exprs) {
        expr = (Node*)lfirst(lc);

        typOid = getBaseType(exprType(expr));
        if (type_is_set(typOid)) {
            *retOid = inCtx ? InvalidOid : TEXTOID;
            return true;
        }
    }

    return false;
}

/*
 * coerce_to_common_type()
 *		Coerce an expression to the given type.
 *
 * This is used following select_common_type() to coerce the individual
 * expressions to the desired type.  'context' is a phrase to use in the
 * error message if we fail to coerce.
 *
 * As with coerce_type, pstate may be NULL if no special unknown-Param
 * processing is wanted.
 */
Node* coerce_to_common_type(ParseState* pstate, Node* node, Oid targetTypeId, const char* context)
{
    Oid inputTypeId = exprType(node);

    if (inputTypeId == targetTypeId) {
        return node; /* no work */
    }
    if (can_coerce_type(1, &inputTypeId, &targetTypeId, COERCION_IMPLICIT)) {
        node = coerce_type(pstate, node, inputTypeId, targetTypeId, -1, COERCION_IMPLICIT, COERCE_IMPLICIT_CAST,
        NULL, NULL, -1);
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_CANNOT_COERCE),
                /* translator: first %s is name of a SQL construct, eg CASE */
                errmsg("%s could not convert type %s to %s",
                    context,
                    format_type_be(inputTypeId),
                    format_type_be(targetTypeId)),
                parser_errposition(pstate, exprLocation(node))));
    }
    return node;
}

/*
 * check_generic_type_consistency()
 *		Are the actual arguments potentially compatible with a
 *		polymorphic function?
 *
 * The argument consistency rules are:
 *
 * 1) All arguments declared ANYELEMENT must have the same datatype.
 * 2) All arguments declared ANYARRAY must have the same datatype,
 *	  which must be a varlena array type.
 * 3) All arguments declared ANYRANGE must have the same datatype,
 *	  which must be a range type.
 * 4) If there are arguments of both ANYELEMENT and ANYARRAY, make sure the
 *	  actual ANYELEMENT datatype is in fact the element type for the actual
 *	  ANYARRAY datatype.
 * 5) Similarly, if there are arguments of both ANYELEMENT and ANYRANGE,
 *	  make sure the actual ANYELEMENT datatype is in fact the subtype for
 *	  the actual ANYRANGE type.
 * 6) ANYENUM is treated the same as ANYELEMENT except that if it is used
 *	  (alone or in combination with plain ANYELEMENT), we add the extra
 *	  condition that the ANYELEMENT type must be an enum.
 * 7) ANYNONARRAY is treated the same as ANYELEMENT except that if it is used,
 *	  we add the extra condition that the ANYELEMENT type must not be an array.
 *	  (This is a no-op if used in combination with ANYARRAY or ANYENUM, but
 *	  is an extra restriction if not.)
 *
 * Domains over arrays match ANYARRAY, and are immediately flattened to their
 * base type.  (Thus, for example, we will consider it a match if one ANYARRAY
 * argument is a domain over int4[] while another one is just int4[].)	Also
 * notice that such a domain does *not* match ANYNONARRAY.
 *
 * Similarly, domains over ranges match ANYRANGE, and are immediately
 * flattened to their base type.
 *
 * Note that domains aren't currently considered to match ANYENUM,
 * even if their base type would match.
 *
 * If we have UNKNOWN input (ie, an untyped literal) for any polymorphic
 * argument, assume it is okay.
 *
 * If an input is of type ANYARRAY (ie, we know it's an array, but not
 * what element type), we will accept it as a match to an argument declared
 * ANYARRAY, so long as we don't have to determine an element type ---
 * that is, so long as there is no use of ANYELEMENT.  This is mostly for
 * backwards compatibility with the pre-7.4 behavior of ANYARRAY.
 *
 * We do not ereport here, but just return FALSE if a rule is violated.
 */
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

    /*
     * Loop through the arguments to see if we have any that are polymorphic.
     * If so, require the actual types to be consistent.
     */
    for (j = 0; j < nargs; j++) {
        Oid decl_type = declared_arg_types[j];
        Oid actual_type = actual_arg_types[j];

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
            if (actual_type == UNKNOWNOID) {
                continue;
            }
            if (OidIsValid(elem_typeid) && actual_type != elem_typeid) {
                return false;
            }
            elem_typeid = actual_type;
        } else if (decl_type == ANYARRAYOID) {
            if (actual_type == UNKNOWNOID) {
                continue;
            }
            actual_type = getBaseType(actual_type); /* flatten domains */
            if (OidIsValid(array_typeid) && actual_type != array_typeid) {
                return false;
            }
            array_typeid = actual_type;
        } else if (decl_type == ANYRANGEOID) {
            if (actual_type == UNKNOWNOID) {
                continue;
            }
            actual_type = getBaseType(actual_type); /* flatten domains */
            if (OidIsValid(range_typeid) && actual_type != range_typeid) {
                return false;
            }
            range_typeid = actual_type;
        }
    }

    /* Get the element type based on the array type, if we have one */
    if (OidIsValid(array_typeid)) {
        if (array_typeid == ANYARRAYOID) {
            /* Special case for ANYARRAY input: okay iff no ANYELEMENT */
            if (have_anyelement) {
                return false;
            }
            return true;
        }

        array_typelem = get_element_type(array_typeid);
        if (!OidIsValid(array_typelem)) {
            return false; /* should be an array, but isn't */
        }

        if (!OidIsValid(elem_typeid)) {
            /*
             * if we don't have an element type yet, use the one we just got
             */
            elem_typeid = array_typelem;
        } else if (array_typelem != elem_typeid) {
            /* otherwise, they better match */
            return false;
        }
    }

    /* Get the element type based on the range type, if we have one */
    if (OidIsValid(range_typeid)) {
        range_typelem = get_range_subtype(range_typeid);
        if (!OidIsValid(range_typelem)) {
            return false; /* should be a range, but isn't */
        }

        if (!OidIsValid(elem_typeid)) {
            /*
             * if we don't have an element type yet, use the one we just got
             */
            elem_typeid = range_typelem;
        } else if (range_typelem != elem_typeid) {
            /* otherwise, they better match */
            return false;
        }
    }

    if (have_anynonarray) {
        /* require the element type to not be an array or domain over array */
        if (type_is_array_domain(elem_typeid)) {
            return false;
        }
    }

    if (have_anyenum) {
        /* require the element type to be an enum */
        if (!type_is_enum(elem_typeid)) {
            return false;
        }
    }

    if (have_anyset) {
        /* require the element type to be an set */
        if (!type_is_set(elem_typeid)) {
            return false;
        }
    }

    /* Looks valid */
    return true;
}

/*
 * enforce_generic_type_consistency()
 *		Make sure a polymorphic function is legally callable, and
 *		deduce actual argument and result types.
 *
 * If any polymorphic pseudotype is used in a function's arguments or
 * return type, we make sure the actual data types are consistent with
 * each other.	The argument consistency rules are shown above for
 * check_generic_type_consistency().
 *
 * If we have UNKNOWN input (ie, an untyped literal) for any polymorphic
 * argument, we attempt to deduce the actual type it should have.  If
 * successful, we alter that position of declared_arg_types[] so that
 * make_fn_arguments will coerce the literal to the right thing.
 *
 * Rules are applied to the function's return type (possibly altering it)
 * if it is declared as a polymorphic type:
 *
 * 1) If return type is ANYARRAY, and any argument is ANYARRAY, use the
 *	  argument's actual type as the function's return type.
 * 2) Similarly, if return type is ANYRANGE, and any argument is ANYRANGE,
 *	  use the argument's actual type as the function's return type.
 * 3) If return type is ANYARRAY, no argument is ANYARRAY, but any argument is
 *	  ANYELEMENT, use the actual type of the argument to determine the
 *	  function's return type, i.e. the element type's corresponding array
 *	  type.  (Note: similar behavior does not exist for ANYRANGE, because it's
 *	  impossible to determine the range type from the subtype alone.)
 * 4) If return type is ANYARRAY, but no argument is ANYARRAY or ANYELEMENT,
 *	  generate an error.  Similarly, if return type is ANYRANGE, but no
 *	  argument is ANYRANGE, generate an error.	(These conditions are
 *	  prevented by CREATE FUNCTION and therefore are not expected here.)
 * 5) If return type is ANYELEMENT, and any argument is ANYELEMENT, use the
 *	  argument's actual type as the function's return type.
 * 6) If return type is ANYELEMENT, no argument is ANYELEMENT, but any argument
 *	  is ANYARRAY or ANYRANGE, use the actual type of the argument to determine
 *	  the function's return type, i.e. the array type's corresponding element
 *	  type or the range type's corresponding subtype (or both, in which case
 *	  they must match).
 * 7) If return type is ANYELEMENT, no argument is ANYELEMENT, ANYARRAY, or
 *	  ANYRANGE, generate an error.	(This condition is prevented by CREATE
 *	  FUNCTION and therefore is not expected here.)
 * 8) ANYENUM is treated the same as ANYELEMENT except that if it is used
 *	  (alone or in combination with plain ANYELEMENT), we add the extra
 *	  condition that the ANYELEMENT type must be an enum.
 * 9) ANYNONARRAY is treated the same as ANYELEMENT except that if it is used,
 *	  we add the extra condition that the ANYELEMENT type must not be an array.
 *	  (This is a no-op if used in combination with ANYARRAY or ANYENUM, but
 *	  is an extra restriction if not.)
 *
 * Domains over arrays or ranges match ANYARRAY or ANYRANGE arguments,
 * respectively, and are immediately flattened to their base type. (In
 * particular, if the return type is also ANYARRAY or ANYRANGE, we'll set it
 * to the base type not the domain type.)
 *
 * When allow_poly is false, we are not expecting any of the actual_arg_types
 * to be polymorphic, and we should not return a polymorphic result type
 * either.	When allow_poly is true, it is okay to have polymorphic "actual"
 * arg types, and we can return ANYARRAY, ANYRANGE, or ANYELEMENT as the
 * result.	(This case is currently used only to check compatibility of an
 * aggregate's declaration with the underlying transfn.)
 *
 * A special case is that we could see ANYARRAY as an actual_arg_type even
 * when allow_poly is false (this is possible only because pg_statistic has
 * columns shown as anyarray in the catalogs).	We allow this to match a
 * declared ANYARRAY argument, but only if there is no ANYELEMENT argument
 * or result (since we can't determine a specific element type to match to
 * ANYELEMENT).  Note this means that functions taking ANYARRAY had better
 * behave sanely if applied to the pg_statistic columns; they can't just
 * assume that successive inputs are of the same actual element type.
 */
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

    /*
     * Loop through the arguments to see if we have any that are polymorphic.
     * If so, require the actual types to be consistent.
     */
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
                continue; /* no new information here */
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
                continue;                           /* no new information here */
            }
            actual_type = getBaseType(actual_type); /* flatten domains */

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

    /*
     * Fast Track: if none of the arguments are polymorphic, return the
     * unmodified rettype.	We assume it can't be polymorphic either.
     */
    if (!have_generics) {
        return rettype;
    }

    /* Get the element type based on the array type, if we have one */
    if (OidIsValid(array_typeid)) {
        if (array_typeid == ANYARRAYOID && !have_anyelement) {
            /* Special case for ANYARRAY input: okay iff no ANYELEMENT */
            array_typelem = ANYELEMENTOID;
        } else {
            array_typelem = get_valid_element_type(array_typeid);
        }

        if (!OidIsValid(elem_typeid)) {
            /*
             * if we don't have an element type yet, use the one we just got
             */
            elem_typeid = array_typelem;
        } else if (array_typelem != elem_typeid) {
            /* otherwise, they better match */
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("argument declared \"anyarray\" is not consistent with argument declared \"anyelement\""),
                    errdetail("%s versus %s", format_type_be(array_typeid), format_type_be(elem_typeid))));
        }
    }

    /* Get the element type based on the range type, if we have one */
    if (OidIsValid(range_typeid)) {
        if (range_typeid == ANYRANGEOID && !have_anyelement) {
            /* Special case for ANYRANGE input: okay iff no ANYELEMENT */
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
            /*
             * if we don't have an element type yet, use the one we just got
             */
            elem_typeid = range_typelem;
        } else if (range_typelem != elem_typeid) {
            /* otherwise, they better match */
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
            /* Only way to get here is if all the generic args are UNKNOWN */
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("could not determine polymorphic type because input has type \"unknown\"")));
        }
    }

    if (have_anynonarray && elem_typeid != ANYELEMENTOID) {
        /* require the element type to not be an array or domain over array */
        if (type_is_array_domain(elem_typeid)) {
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("type matched to anynonarray is an array type: %s", format_type_be(elem_typeid))));
        }
    }

    if (have_anyenum && elem_typeid != ANYELEMENTOID) {
        /* require the element type to be an enum */
        if (!type_is_enum(elem_typeid)) {
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("type matched to anyenum is not an enum type: %s", format_type_be(elem_typeid))));
        }
    }

    /*
     * If we had any unknown inputs, re-scan to assign correct types
     */
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

    /* if we return ANYARRAY use the appropriate argument type */
    if (rettype == ANYARRAYOID) {
        if (!OidIsValid(array_typeid)) {
            array_typeid = get_valid_array_type(elem_typeid);
        }
        return array_typeid;
    }

    /* if we return ANYRANGE use the appropriate argument type */
    if (rettype == ANYRANGEOID) {
        if (!OidIsValid(range_typeid)) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("could not find range type for data type %s", format_type_be(elem_typeid))));
        }
        return range_typeid;
    }

    /* if we return ANYELEMENT use the appropriate argument type */
    if (rettype == ANYELEMENTOID || rettype == ANYNONARRAYOID || rettype == ANYENUMOID || rettype == ANYSETOID) {
        return elem_typeid;
    }

    /* we don't return a generic type; send back the original return type */
    return rettype;
}

/*
 * resolve_generic_type()
 *		Deduce an individual actual datatype on the assumption that
 *		the rules for polymorphic types are being followed.
 *
 * declared_type is the declared datatype we want to resolve.
 * context_actual_type is the actual input datatype to some argument
 * that has declared datatype context_declared_type.
 *
 * If declared_type isn't polymorphic, we just return it.  Otherwise,
 * context_declared_type must be polymorphic, and we deduce the correct
 * return type based on the relationship of the two polymorphic types.
 */
Oid resolve_generic_type(Oid declared_type, Oid context_actual_type, Oid context_declared_type)
{
    if (declared_type == ANYARRAYOID) {
        if (context_declared_type == ANYARRAYOID) {
            /*
             * Use actual type, but it must be an array; or if it's a domain
             * over array, use the base array type.
             */
            Oid context_base_type = getBaseType(context_actual_type);
            Oid array_typelem = get_element_type(context_base_type);
            if (!OidIsValid(array_typelem)) {
                ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmsg("argument declared \"anyarray\" is not an array but type %s",
                            format_type_be(context_base_type))));
            }
            return context_base_type;
        } else if (context_declared_type == ANYELEMENTOID || context_declared_type == ANYNONARRAYOID ||
                   context_declared_type == ANYENUMOID || context_declared_type == ANYRANGEOID) {
            /* Use the array type corresponding to actual type */
            Oid array_typeid = get_array_type(context_actual_type);
            if (!OidIsValid(array_typeid)) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("could not find array type for data type %s", format_type_be(context_actual_type))));
            }
            return array_typeid;
        }
    } else if (declared_type == ANYELEMENTOID || declared_type == ANYNONARRAYOID || declared_type == ANYENUMOID ||
               declared_type == ANYRANGEOID) {
        if (context_declared_type == ANYARRAYOID) {
            /* Use the element type corresponding to actual type */
            Oid context_base_type = getBaseType(context_actual_type);
            Oid array_typelem = get_valid_element_type(context_base_type);
            return array_typelem;
        } else if (context_declared_type == ANYRANGEOID) {
            /* Use the element type corresponding to actual type */
            Oid context_base_type = getBaseType(context_actual_type);
            Oid range_typelem = get_range_subtype(context_base_type);
            if (!OidIsValid(range_typelem)) {
                ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmsg("argument declared \"anyrange\" is not a range but type %s",
                            format_type_be(context_base_type))));
            }
            return range_typelem;
        } else if (context_declared_type == ANYELEMENTOID || context_declared_type == ANYNONARRAYOID ||
                   context_declared_type == ANYENUMOID) {
            /* Use the actual type; it doesn't matter if array or not */
            return context_actual_type;
        }
    } else {
        /* declared_type isn't polymorphic, so return it as-is */
        return declared_type;
    }
    /* If we get here, declared_type is polymorphic and context isn't */
    /* NB: this is a calling-code logic error, not a user error */
    ereport(ERROR,
        (errcode(ERRCODE_INDETERMINATE_DATATYPE),
            errmsg("could not determine polymorphic type because context isn't polymorphic")));
    return InvalidOid; /* keep compiler quiet */
}

/* TypeCategory()
 *		Assign a category to the specified type OID.
 *
 * NB: this must not return TYPCATEGORY_INVALID.
 */
TYPCATEGORY TypeCategory(Oid type)
{
    char typcategory;
    bool typispreferred = false;

    get_type_category_preferred(type, &typcategory, &typispreferred);
    AssertEreport((typcategory != TYPCATEGORY_INVALID), MOD_OPT, "The type category is valid");
    return (TYPCATEGORY)typcategory;
}

/* IsPreferredType()
 *		Check if this type is a preferred type for the given category.
 *
 * If category is TYPCATEGORY_INVALID, then we'll return TRUE for preferred
 * types of any category; otherwise, only for preferred types of that
 * category.
 */
bool IsPreferredType(TYPCATEGORY category, Oid type)
{
    char typcategory;
    bool typispreferred = false;

    get_type_category_preferred(type, &typcategory, &typispreferred);
    if (category == typcategory || category == TYPCATEGORY_INVALID) {
        return typispreferred;
    } else {
        return false;
    }
}

/* IsBinaryCoercible()
 *		Check if srctype is binary-coercible to targettype.
 *
 * This notion allows us to cheat and directly exchange values without
 * going through the trouble of calling a conversion function.	Note that
 * in general, this should only be an implementation shortcut.	Before 7.4,
 * this was also used as a heuristic for resolving overloaded functions and
 * operators, but that's basically a bad idea.
 *
 * As of 7.3, binary coercibility isn't hardwired into the code anymore.
 * We consider two types binary-coercible if there is an implicitly
 * invokable, no-function-needed pg_cast entry.  Also, a domain is always
 * binary-coercible to its base type, though *not* vice versa (in the other
 * direction, one must apply domain constraint checks before accepting the
 * value as legitimate).  We also need to special-case various polymorphic
 * types.
 *
 * This function replaces IsBinaryCompatible(), which was an inherently
 * symmetric test.	Since the pg_cast entries aren't necessarily symmetric,
 * the order of the operands is now significant.
 */
bool IsBinaryCoercible(Oid srctype, Oid targettype)
{
    HeapTuple tuple;
    Form_pg_cast castForm;
    bool result = false;

    /* Fast path if same type */
    if (srctype == targettype) {
        return true;
    }

    /* Anything is coercible to ANY or ANYELEMENT */
    if (targettype == ANYOID || targettype == ANYELEMENTOID) {
        return true;
    }

    /* If srctype is a domain, reduce to its base type */
    if (OidIsValid(srctype)) {
        srctype = getBaseType(srctype);
    }

    /* Somewhat-fast path for domain -> base type case */
    if (srctype == targettype) {
        return true;
    }

    /* Also accept any array type as coercible to ANYARRAY */
    if (targettype == ANYARRAYOID) {
        if (type_is_array_domain(srctype)) {
            return true;
        }
    }

    /* Also accept any non-array type as coercible to ANYNONARRAY */
    if (targettype == ANYNONARRAYOID) {
        if (!type_is_array_domain(srctype)) {
            return true;
        }
    }

    /* Also accept any enum type as coercible to ANYENUM */
    if (targettype == ANYENUMOID) {
        if (type_is_enum(srctype)) {
            return true;
        }
    }

    /* Also accept any set type as coercible to ANYSET */
    if (targettype == ANYSETOID) {
        if (type_is_set(srctype)) {
            return true;
        }
    }

    /* Also accept any range type as coercible to ANYRANGE */
    if (targettype == ANYRANGEOID) {
        if (type_is_range(srctype)) {
            return true;
        }
    }

    /* Also accept any composite type as coercible to RECORD */
    if (targettype == RECORDOID) {
        if (ISCOMPLEX(srctype)) {
            return true;
        }
    }

    /* Also accept any composite array type as coercible to RECORD[] */
    if (targettype == RECORDARRAYOID) {
        if (is_complex_array(srctype)) {
            return true;
        }
    }

    /* Else look in pg_cast */
    tuple = SearchSysCache2(CASTSOURCETARGET, ObjectIdGetDatum(srctype), ObjectIdGetDatum(targettype));
    if (!HeapTupleIsValid(tuple)) {
        return false; /* no cast */
    }
    castForm = (Form_pg_cast)GETSTRUCT(tuple);

    result = (castForm->castmethod == COERCION_METHOD_BINARY && castForm->castcontext == COERCION_CODE_IMPLICIT);

    ReleaseSysCache(tuple);

    return result;
}

/*
 * find_coercion_pathway
 *		Look for a coercion pathway between two types.
 *
 * Currently, this deals only with scalar-type cases; it does not consider
 * polymorphic types nor casts between composite types.  (Perhaps fold
 * those in someday?)
 *
 * ccontext determines the set of available casts.
 *
 * The possible result codes are:
 *	COERCION_PATH_NONE: failed to find any coercion pathway
 *				*funcid is set to InvalidOid
 *	COERCION_PATH_FUNC: apply the coercion function returned in *funcid
 *	COERCION_PATH_RELABELTYPE: binary-compatible cast, no function needed
 *				*funcid is set to InvalidOid
 *	COERCION_PATH_ARRAYCOERCE: need an ArrayCoerceExpr node
 *				*funcid is set to the element cast function, or InvalidOid
 *				if the array elements are binary-compatible
 *	COERCION_PATH_COERCEVIAIO: need a CoerceViaIO node
 *				*funcid is set to InvalidOid
 *
 * Note: COERCION_PATH_RELABELTYPE does not necessarily mean that no work is
 * needed to do the coercion; if the target is a domain then we may need to
 * apply domain constraint checking.  If you want to check for a zero-effort
 * conversion then use IsBinaryCoercible().
 */
CoercionPathType find_coercion_pathway(Oid targetTypeId, Oid sourceTypeId, CoercionContext ccontext, Oid* funcid)
{
    CoercionPathType result = COERCION_PATH_NONE;
    HeapTuple tuple;

    *funcid = InvalidOid;
    int32 typmode = -1;
    char sourceTypeType = TYPTYPE_INVALID;
    char targetTypeType = TYPTYPE_INVALID;

    /* Perhaps the types are domains; if so, look at their base types */
    if (OidIsValid(sourceTypeId)) {
        sourceTypeId = getBaseTypeAndOtherAttr(sourceTypeId, &typmode, &sourceTypeType);
    }
    if (OidIsValid(targetTypeId)) {
        targetTypeId = getBaseTypeAndOtherAttr(targetTypeId, &typmode, &targetTypeType);
    }

    /* Domains are always coercible to and from their base type */
    if (sourceTypeId == targetTypeId) {
        return COERCION_PATH_RELABELTYPE;
    }

    /* target is an actual set type, change it to anyset to find the path */
    if (targetTypeId != ANYSETOID && targetTypeType == TYPTYPE_SET) {
        targetTypeId = ANYSETOID;
    }

    if (sourceTypeId != ANYSETOID && sourceTypeType == TYPTYPE_SET) {
        sourceTypeId = ANYSETOID;
    }

    /* Look in pg_cast */
    tuple = SearchSysCache2(CASTSOURCETARGET, ObjectIdGetDatum(sourceTypeId), ObjectIdGetDatum(targetTypeId));

    if (HeapTupleIsValid(tuple)) {
        Form_pg_cast castForm = (Form_pg_cast)GETSTRUCT(tuple);
        CoercionContext castcontext;

        /* convert char value for castcontext to CoercionContext enum */
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
                castcontext = (CoercionContext)0; /* keep compiler quiet */
                break;
        }

        /* Rely on ordering of enum for correct behavior here */
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

        ReleaseSysCache(tuple);
    } else {
        /*
         * If there's no pg_cast entry, perhaps we are dealing with a pair of
         * array types.  If so, and if the element types have a suitable cast,
         * report that we can coerce with an ArrayCoerceExpr.
         *
         * Note that the source type can be a domain over array, but not the
         * target, because ArrayCoerceExpr won't check domain constraints.
         *
         * Hack: disallow coercions to oidvector and int2vector, which
         * otherwise tend to capture coercions that should go to "real" array
         * types.  We want those types to be considered "real" arrays for many
         * purposes, but not this one.	(Also, ArrayCoerceExpr isn't
         * guaranteed to produce an output that meets the restrictions of
         * these datatypes, such as being 1-dimensional.)
         */
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

        /*
         * If we still haven't found a possibility, consider automatic casting
         * using I/O functions.  We allow assignment casts to string types and
         * explicit casts from string types to be handled this way. (The
         * CoerceViaIO mechanism is a lot more general than that, but this is
         * all we want to allow in the absence of a pg_cast entry.) It would
         * probably be better to insist on explicit casts in both directions,
         * but this is a compromise to preserve something of the pre-8.3
         * behavior that many types had implicit (yipes!) casts to text.
         */
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

/*
 * find_typmod_coercion_function -- does the given type need length coercion?
 *
 * If the target type possesses a pg_cast function from itself to itself,
 * it must need length coercion.
 *
 * "bpchar" (ie, char(N)) and "numeric" are examples of such types.
 *
 * If the given type is a varlena array type, we do not look for a coercion
 * function associated directly with the array type, but instead look for
 * one associated with the element type.  An ArrayCoerceExpr node must be
 * used to apply such a function.
 *
 * We use the same result enum as find_coercion_pathway, but the only possible
 * result codes are:
 *	COERCION_PATH_NONE: no length coercion needed
 *	COERCION_PATH_FUNC: apply the function returned in *funcid
 *	COERCION_PATH_ARRAYCOERCE: apply the function using ArrayCoerceExpr
 */
CoercionPathType find_typmod_coercion_function(Oid typeId, Oid* funcid)
{
    CoercionPathType result;
    Type targetType;
    Form_pg_type typeForm;
    HeapTuple tuple;

    *funcid = InvalidOid;
    result = COERCION_PATH_FUNC;

    switch (typeId) {
        case BPCHAROID:
            *funcid = F_BPCHAR;
            break;
        case VARCHAROID:
            *funcid = F_VARCHAR;
            break;
        default:
        {
            targetType = typeidType(typeId);
            typeForm = (Form_pg_type)GETSTRUCT(targetType);
            /* Check for a varlena array type */
            if (typeForm->typelem != InvalidOid && typeForm->typlen == -1) {
                /* Yes, switch our attention to the element type */
                typeId = typeForm->typelem;
                result = COERCION_PATH_ARRAYCOERCE;
            }
            ReleaseSysCache(targetType);
            /* Look in pg_cast */
            tuple = SearchSysCache2(CASTSOURCETARGET, ObjectIdGetDatum(typeId), ObjectIdGetDatum(typeId));
            if (HeapTupleIsValid(tuple)) {
                Form_pg_cast castForm = (Form_pg_cast)GETSTRUCT(tuple);
                *funcid = castForm->castfunc;
                ReleaseSysCache(tuple);
            }
            if (!OidIsValid(*funcid)) {
                result = COERCION_PATH_NONE;
            }
        }
    }
    return result;
}

/*
 * is_complex_array
 *		Is this type an array of composite?
 *
 * Note: this will not return true for record[]; check for RECORDARRAYOID
 * separately if needed.
 */
static bool is_complex_array(Oid typid)
{
    Oid elemtype = get_element_type(typid);

    return (OidIsValid(elemtype) && ISCOMPLEX(elemtype));
}

/*
 * Check whether reltypeId is the row type of a typed table of type
 * reloftypeId.  (This is conceptually similar to the subtype
 * relationship checked by typeInheritsFrom().)
 */
static bool typeIsOfTypedTable(Oid reltypeId, Oid reloftypeId)
{
    Oid relid = typeidTypeRelid(reltypeId);
    bool result = false;

    if (relid) {
        HeapTuple tp;
        Form_pg_class reltup;

        tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
        if (!HeapTupleIsValid(tp)) {
            ereport(
                ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for relation %u", relid)));
        }

        reltup = (Form_pg_class)GETSTRUCT(tp);
        if (reltup->reloftype == reloftypeId) {
            result = true;
        }

        ReleaseSysCache(tp);
    }

    return result;
}

void expression_error_callback(void* arg)
{
    char* colname = (char*)arg;

    if (colname != NULL && strcmp(colname, "?column?")) {
        errcontext("referenced column: %s", colname);
    }
}
 
Node *transferConstToAconst(Node *node)
{
    Node *result = NULL;
 
    if(!IsA(node, Const)) {
        ereport(ERROR, 
            (errcode(ERRCODE_INVALID_OPERATION),
                errmsg("The variable value expr must be convertible to a constant.")));
    }
 
    Datum constval = ((Const*)node)->constvalue;
    Oid consttype = ((Const*)node)->consttype;
    Value* val = NULL;

    if (((Const*)node)->constisnull) {
        ereport(ERROR, 
            (errcode(ERRCODE_INVALID_OPERATION),
                errmsg("The variable value expr can't be a null value.")));
    }
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
        /*
        * set configuration only support types above.
        * if assigned value are not types above, will attempt to convert the type to text.
        * if the type connot be converted to text also, an error is reported.
        */
        default:
            {
                PG_TRY();
                {
                    Const* con = (Const *)node;
                    Node* expr = coerce_type(NULL, (Node*)con, con->consttype, TEXTOID, -1, COERCION_IMPLICIT,
                        COERCE_IMPLICIT_CAST, NULL, NULL, -1);
                    if (IsA(expr, Const) && (((Const*)expr)->consttype = TEXTOID)) {
                        char* constr = TextDatumGetCString(((Const*)expr)->constvalue);
                        val = makeString(constr);
                    } else {
                        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION), 
                                        errmsg("set value cannot be assigned to the %s type", format_type_be(consttype))));
                    }
                }
                PG_CATCH();
                {
                    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION), 
                                    errmsg("set value cannot be assigned to the %s type", format_type_be(consttype))));
                }
                PG_END_TRY();
            } break;
    }
 
    result = makeAConst(val, -1);
    return result;
}

Const* setValueToConstExpr(SetVariableExpr* set)
{
    Const* result = NULL;
    Value* value;
    /* initial value is null Const */
    Datum val = (Datum)0;
    Oid typid = UNKNOWNOID;
    int typelen = -2;
    bool typebyval = false;

    struct config_generic* record = NULL;
    record = find_option(set->name, false, ERROR);
    char* variable_str = SetVariableExprGetConfigOption(set);

    switch (record->vartype) {
        case PGC_BOOL:
            {
                bool variable_bool = false;
                if (strcmp(variable_str, "true") == 0 || strcmp(variable_str,"on") == 0) {
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
                typid = UNKNOWNOID; /* will be coerced later */
                typelen = -2;       /* cstring-style varwidth type */
                typebyval = false;
            } break;
        case PGC_ENUM:
            {
                value = makeString(variable_str);
                val = CStringGetDatum(strVal(value));
                typid = UNKNOWNOID; /* will be coerced later */
                typelen = -2;       /* cstring-style varwidth type */
                typebyval = false;
            } break;
        default:
            break;
    }
 
    result = makeConst(typid,
        -1,         /* typmod -1 is OK for all cases */
        InvalidOid, /* all cases are uncollatable types */
        typelen,
        val,
        false,
        typebyval);
 
    result->location = -1;
    return result;
}
