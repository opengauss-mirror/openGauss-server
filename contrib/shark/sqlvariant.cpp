/*
 * Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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
 * --------------------------------------------------------------------------------------
 *
 * sqlvariant.cpp
 *
 * IDENTIFICATION
 *        contrib/shark/sqlvariant.cpp
 *
 * --------------------------------------------------------------------------------------
 */
#include <cstdlib>
#include <cmath>

#include "postgres.h"
#include "miscadmin.h"
#include "knl/knl_instance.h"
#include "knl/knl_variable.h"

#include "access/sysattr.h"
#include "commands/extension.h"
#include "commands/dbcommands.h"
#include "nodes/execnodes.h"
#include "utils/builtins.h"
#include "utils/cash.h"
#include "utils/date.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "utils/varbit.h"
#include "utils/lsyscache.h"
#include "utils/fmgroids.h"
#include "parser/scansup.h"
#include "parser/parse_coerce.h"
#include "parser/parse_oper.h"
#include "tcop/ddldeparse.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_attrdef.h"
#include "catalog/indexing.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_trigger.h"
#include "libpq/pqformat.h"
#include "shark.h"

/* limit for sql_variant */
constexpr int MAX_SQL_VARIANT_SIZE = 8016;
constexpr int MAX_BASIC_VALUE_SIZE = 8000;

static Datum do_cast(Oid source_type, Oid target_type, Datum value, int32_t typmod, Oid coll, CoercionContext ccontext);
Datum do_compare(char *oprname, Datum basic_value1, Datum basic_value2,
                 Oid type_oid1, Oid type_oid2, int typeprio1, int typeprio2, Oid fncollation);
Datum comp_time(char *oprname, uint16_t t1, uint16_t t2);

typedef struct {
    Oid		typeOid;			/* oid is only retrievable during runtime, so
    							 * we have to init to 0 */
    const char *typeName;
    int		familyPrio;
} TypePrio;

const int TOTAL_TYPECODE_COUNT = 16;
TypePrio g_typePrios[TOTAL_TYPECODE_COUNT] = {
    {SMALLDATETIMEOID, "smalldatetime", 1},
    {DATEOID, "date", 1},
    {TIMEOID, "time", 1},
    {FLOAT8OID, "float8", 2},
    {FLOAT4OID, "float4", 2},
    {NUMERICOID, "numeric", 3},
    {CASHOID, "money", 3},
    {INT8OID, "int8", 3},
    {INT4OID, "int4", 3},
    {INT2OID, "int2", 3},
    {INT1OID, "tinyint", 3},
    {BITOID, "bit", 3},
    {NVARCHAR2OID, "nvarchar", 4},
    {VARCHAROID, "varchar", 4},
    {BPCHAROID, "bpchar", 4},
    {TEXTOID, "text", 4},
};

static int get_typeprio(Oid typoid, TYPCATEGORY tcategory)
{
    int result = 0;
    for (int i = 0; i < TOTAL_TYPECODE_COUNT; i++) {
        if (g_typePrios[i].typeOid == typoid) {
            return g_typePrios[i].familyPrio;
        }
    }

    switch (tcategory) {
        case ('D'): /* Datetime */
            result = 1;
            break;
        case ('N'): /* Numeric */
            result = 3;
            break;
        case ('S'): /* String */
            result = 4;
            break;
        default:
            result = 0;
            break;
    }
    return result;
}

PG_FUNCTION_INFO_V1(sql_variantin);
PG_FUNCTION_INFO_V1(sql_variantout);
PG_FUNCTION_INFO_V1(sql_variantrecv);
PG_FUNCTION_INFO_V1(sql_variantsend);

static Datum from_sql_variant(bytea* valena, Oid* typeoid, int32* typmod)
{
    char* data = VARDATA_ANY(valena);
    StringInfoData buf;
    Oid typreceive;
    Oid typioparam;
    Datum result;
    *typeoid = *(Oid*)data;
    *typmod = *(int32*)(data + sizeof(Oid));
    bytea* basic_value_b = (bytea*) (data + sizeof(Oid) + VARHDRSZ);
    initStringInfo(&buf);
    pq_sendbytes(&buf, VARDATA_ANY(basic_value_b), VARSIZE_ANY_EXHDR(basic_value_b));
    getTypeBinaryInputInfo(*typeoid, &typreceive, &typioparam);
    result = OidReceiveFunctionCall(typreceive, &buf, typioparam, *typmod);
    pfree(buf.data);
    PG_RETURN_DATUM(result);
}

Datum to_sql_variant_internal(Datum value, Oid basic_type, int32 typmod)
{
    Oid typsend;
    Oid typreceive;
    Oid typioparam;
    bool typisvarlena;
    bytea *result;
    bytea *val;
    int32 len;
    char* data;
    errno_t ss_rc = 0;

    /* check whether the type has a binary input function */
    getTypeBinaryInputInfo(basic_type, &typreceive, &typioparam);
    getTypeBinaryOutputInfo(basic_type, &typsend, &typisvarlena);

    val = OidSendFunctionCall(typsend, value);
    /* possible that the size of the original type value exceeds 8000 bytes */
    if (VARSIZE_ANY_EXHDR(val) > MAX_BASIC_VALUE_SIZE) {
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
                        errmsg("value of basic type must be a binary length <= 8000 byte")));
    }

    len = VARHDRSZ + sizeof(Oid) + VARHDRSZ + VARSIZE_ANY(val);
    result = (bytea*)palloc0(len);
    SET_VARSIZE(result, len);
    data = VARDATA_ANY(result);
    *(Oid*)data = basic_type; /* basictype, typmod, isnull and basictypevalue */
    data += sizeof(Oid);
    *(int32*)data = typmod;
    data += VARHDRSZ;
    ss_rc = memcpy_s(data, VARSIZE_ANY(val), val, VARSIZE_ANY(val));
    securec_check(ss_rc, "\0", "\0");

    PG_RETURN_BYTEA_P(result);
}

Datum sql_variantin(PG_FUNCTION_ARGS)
{
    char* inputText = PG_GETARG_CSTRING(0);
    Datum result = DirectFunctionCall3(varcharin,
                                       CStringGetDatum(inputText), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
    return to_sql_variant_internal(result, VARCHAROID, -1);
}

/* sql_variant convert the internal storage format to string form */
Datum sql_variantout(PG_FUNCTION_ARGS)
{
    bytea* vlena = PG_GETARG_BYTEA_PP(0);
    bool typIsVarlena;
    Oid typeoid;
    Oid typOutput;
    int32 typemod;
    Datum basic_value;

    basic_value = from_sql_variant(vlena, &typeoid, &typemod);
    getTypeOutputInfo(typeoid, &typOutput, &typIsVarlena);
    PG_RETURN_CSTRING(OidOutputFunctionCall(typOutput, basic_value));
}

Datum sql_variantrecv(PG_FUNCTION_ARGS)
{
    StringInfo buf = (StringInfo)PG_GETARG_POINTER(0);
    bytea* result = NULL;
    int nbytes;

    nbytes = buf->len - buf->cursor;

    /* size of sql_variant is limit to 8016 */
    if (nbytes + VARHDRSZ > MAX_SQL_VARIANT_SIZE) {
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
                        errmsg("size of sql_variant type must be <= 8016 byte")));
    }
    result = (bytea*)palloc(nbytes + VARHDRSZ);
    SET_VARSIZE(result, nbytes + VARHDRSZ);
    pq_copymsgbytes(buf, VARDATA(result), nbytes);
    PG_RETURN_BYTEA_P(result);
}

Datum sql_variantsend(PG_FUNCTION_ARGS)
{
    bytea* vlena = PG_GETARG_BYTEA_P_COPY(0);

    PG_RETURN_BYTEA_P(vlena);
}

extern "C" Datum sql_variantcmp(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(sql_variantcmp);

Datum sql_variantcmp(PG_FUNCTION_ARGS)
{
    bytea* vlena1 = PG_GETARG_BYTEA_PP(0);
    bytea* vlena2 = PG_GETARG_BYTEA_PP(1);

    Oid typeoid1;
    Oid typeoid2;
    int32 typemod1;
    int32 typemod2;
    int typeprio1;
    int typeprio2;
    Datum basic_value1;
    Datum basic_value2;
    TYPCATEGORY tcategory1;
    TYPCATEGORY tcategory2;
    bool typispreferred1 = false;
    bool typispreferred2 = false;
    int result = 0;

    basic_value1 = from_sql_variant(vlena1, &typeoid1, &typemod1);
    basic_value2 = from_sql_variant(vlena2, &typeoid2, &typemod2);

    get_type_category_preferred(typeoid1, &tcategory1, &typispreferred1);
    get_type_category_preferred(typeoid2, &tcategory2, &typispreferred2);

    typeprio1 = get_typeprio(typeoid1, tcategory1);
    typeprio2 = get_typeprio(typeoid2, tcategory2);
    if (typeprio1 == typeprio2) {
        char	   *opeq = "=";
        char	   *oplt = "<";
        Datum		is_eq;
        Datum		is_lt;
        
        is_lt = do_compare(oplt, basic_value1, basic_value2,
                           typeoid1, typeoid2, typeprio1, typeprio2, PG_GET_COLLATION());
        if (DatumGetBool(is_lt)) {
            result = Int32GetDatum(-1);
        } else {
            is_eq = do_compare(opeq, basic_value1, basic_value2,
                               typeoid1, typeoid2, typeprio1, typeprio2, PG_GET_COLLATION());
            result = DatumGetBool(is_eq) ? Int32GetDatum(0) : Int32GetDatum(1);
        }
    } else {
        result = (typeprio1 > typeprio2) ? Int32GetDatum(-1) : Int32GetDatum(1);
    }
    PG_RETURN_INT32(result);
}

static Datum do_cast(Oid source_type, Oid target_type, Datum value, int32_t typmod, Oid coll, CoercionContext ccontext)
{
    Oid			funcid;
    CoercionPathType path;
    Oid			typioparam;
    bool		isVarlena;
    int 		nargs;
    
    path = find_coercion_pathway(target_type, source_type, ccontext, &funcid);
    
    switch (path) {
        case COERCION_PATH_FUNC:
            nargs = get_func_nargs(funcid);
            switch (nargs) {
                case 1:
                    return OidFunctionCall1Coll(funcid, coll, value);
                case 2:
                    return OidFunctionCall2Coll(funcid, coll, value, (Datum) typmod);
                case 3:
                    return OidFunctionCall3Coll(funcid, coll, value, (Datum) typmod,
                                                (Datum) (ccontext == COERCION_EXPLICIT));
                default:
                    elog(ERROR, "Unsupported number of arguments (%d) for function %u", nargs, funcid);
            }
            break;
        case COERCION_PATH_COERCEVIAIO:
            if (TypeCategory(source_type) == TYPCATEGORY_STRING) {
                getTypeInputInfo(target_type, &funcid, &typioparam);
                return OidInputFunctionCall(funcid, TextDatumGetCString(value), typioparam, typmod);
            } else {
                getTypeOutputInfo(source_type, &funcid, &isVarlena);
                return CStringGetTextDatum(OidOutputFunctionCall(funcid, value));
            }
            break;
        case COERCION_PATH_RELABELTYPE:
            return value;
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("unable to cast from internal type %s to %s",
                        format_type_be(source_type), format_type_be(target_type))));
    }
    return value;
}

Datum comp_time(char *oprname, Oid t1, Oid t2)
{
    /*
     * Notice: THIS IS NOT A GENERATL COMPARISON FUNCTION Assumption : 1 and
     * ONLY 1 of t1,t2 is of TIME_T
     */
    if (pg_strncasecmp(oprname, "<>", 2) == 0) {
        PG_RETURN_BOOL(true);
    } else if (pg_strncasecmp(oprname, ">", 1) == 0) { /* including >= */
        PG_RETURN_BOOL(t1 != TIMEOID && t2 == TIMEOID);
    } else if (pg_strncasecmp(oprname, "<", 1) == 0) { /* including <= */
        PG_RETURN_BOOL(t1 == TIMEOID && t2 != TIMEOID);
    } else { /* (pg_strncasecmp(oprname, "=", 2) == 0) */
        PG_RETURN_BOOL(false);
    }
}

Datum do_compare(char *oprname, Datum basic_value1, Datum basic_value2,
                 Oid type_oid1, Oid type_oid2, int typeprio1, int typeprio2, Oid fncollation)
{
    Operator	operator_cmp;
    Form_pg_operator opform;
    Oid			oprcode;
    
    /* handle sql_variant specific cases */
    if (type_oid1 != type_oid2 && (type_oid1 == TIMEOID || type_oid2 == TIMEOID)) {
        return comp_time(oprname, type_oid1, type_oid2);
    }
    
    /* find direct comparisions without casting */
    operator_cmp = oper(NULL, list_make1(makeString(oprname)), type_oid1, type_oid2, false, -1);
    opform = (Form_pg_operator)GETSTRUCT(operator_cmp);
    oprcode = opform->oprcode;
    ReleaseSysCache(operator_cmp);

    if (targetissqlvariant(opform->oprleft) || targetissqlvariant(opform->oprright)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_FUNCTION),
                errmsg("operator does not exist: %s %s %s",
                    format_type_be(type_oid1), oprname, format_type_be(type_oid2)),
                errhint("No operator matches the given name and argument type(s). "
                        "You might need to add explicit type casts."),
                parser_errposition(NULL, -1)));
    }

    if (type_oid1 != opform->oprleft) {
        basic_value1 = do_cast(type_oid1, opform->oprleft, basic_value1, -1, fncollation, COERCION_IMPLICIT);
    }

    if (type_oid2 != opform->oprright) {
        basic_value2 = do_cast(type_oid2, opform->oprright, basic_value2, -1, fncollation, COERCION_IMPLICIT);
    }
    return OidFunctionCall2Coll(oprcode, fncollation, basic_value1, basic_value2);
}
extern "C" Datum sql_varianteq(PG_FUNCTION_ARGS);
extern "C" Datum sql_variantne(PG_FUNCTION_ARGS);
extern "C" Datum sql_variantlt(PG_FUNCTION_ARGS);
extern "C" Datum sql_variantle(PG_FUNCTION_ARGS);
extern "C" Datum sql_variantgt(PG_FUNCTION_ARGS);
extern "C" Datum sql_variantge(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(sql_varianteq);
PG_FUNCTION_INFO_V1(sql_variantne);
PG_FUNCTION_INFO_V1(sql_variantlt);
PG_FUNCTION_INFO_V1(sql_variantle);
PG_FUNCTION_INFO_V1(sql_variantgt);
PG_FUNCTION_INFO_V1(sql_variantge);

Datum sql_varianteq(PG_FUNCTION_ARGS)
{
    int ret = DatumGetInt32(sql_variantcmp(fcinfo));
    PG_RETURN_BOOL(ret == 0);
}

Datum sql_variantne(PG_FUNCTION_ARGS)
{
    int ret = DatumGetInt32(sql_variantcmp(fcinfo));
    PG_RETURN_BOOL(ret != 0);
}

Datum sql_variantlt(PG_FUNCTION_ARGS)
{
    int ret = DatumGetInt32(sql_variantcmp(fcinfo));
    PG_RETURN_BOOL(ret < 0);
}

Datum sql_variantle(PG_FUNCTION_ARGS)
{
    int ret = DatumGetInt32(sql_variantcmp(fcinfo));
    PG_RETURN_BOOL(ret <= 0);
}

Datum sql_variantgt(PG_FUNCTION_ARGS)
{
    int ret = DatumGetInt32(sql_variantcmp(fcinfo));
    PG_RETURN_BOOL(ret > 0);
}

Datum sql_variantge(PG_FUNCTION_ARGS)
{
    int ret = DatumGetInt32(sql_variantcmp(fcinfo));
    PG_RETURN_BOOL(ret >= 0);
}

/*
 * CAST functions to SQL_VARIANT
 */
extern "C" Datum smalldatetime2sqlvariant(PG_FUNCTION_ARGS);
extern "C" Datum date2sqlvariant(PG_FUNCTION_ARGS);
extern "C" Datum time2sqlvariant(PG_FUNCTION_ARGS);
extern "C" Datum float2sqlvariant(PG_FUNCTION_ARGS);
extern "C" Datum real2sqlvariant(PG_FUNCTION_ARGS);
extern "C" Datum numeric2sqlvariant(PG_FUNCTION_ARGS);
extern "C" Datum money2sqlvariant(PG_FUNCTION_ARGS);
extern "C" Datum bigint2sqlvariant(PG_FUNCTION_ARGS);
extern "C" Datum int2sqlvariant(PG_FUNCTION_ARGS);
extern "C" Datum smallint2sqlvariant(PG_FUNCTION_ARGS);
extern "C" Datum tinyint2sqlvariant(PG_FUNCTION_ARGS);
extern "C" Datum bit2sqlvariant(PG_FUNCTION_ARGS);
extern "C" Datum varchar2sqlvariant(PG_FUNCTION_ARGS);
extern "C" Datum nvarchar2sqlvariant(PG_FUNCTION_ARGS);
extern "C" Datum char2sqlvariant(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(smalldatetime2sqlvariant);
PG_FUNCTION_INFO_V1(date2sqlvariant);
PG_FUNCTION_INFO_V1(time2sqlvariant);
PG_FUNCTION_INFO_V1(float2sqlvariant);
PG_FUNCTION_INFO_V1(real2sqlvariant);
PG_FUNCTION_INFO_V1(numeric2sqlvariant);
PG_FUNCTION_INFO_V1(money2sqlvariant);
PG_FUNCTION_INFO_V1(bigint2sqlvariant);
PG_FUNCTION_INFO_V1(int2sqlvariant);
PG_FUNCTION_INFO_V1(smallint2sqlvariant);
PG_FUNCTION_INFO_V1(tinyint2sqlvariant);
PG_FUNCTION_INFO_V1(bit2sqlvariant);
PG_FUNCTION_INFO_V1(varchar2sqlvariant);
PG_FUNCTION_INFO_V1(nvarchar2sqlvariant);
PG_FUNCTION_INFO_V1(char2sqlvariant);

Datum smalldatetime2sqlvariant(PG_FUNCTION_ARGS)
{
    return to_sql_variant_internal(PG_GETARG_DATUM(0), SMALLDATETIMEOID, PG_GETARG_INT32(1));
}

Datum date2sqlvariant(PG_FUNCTION_ARGS)
{
    return to_sql_variant_internal(PG_GETARG_DATUM(0), DATEOID, PG_GETARG_INT32(1));
}

Datum time2sqlvariant(PG_FUNCTION_ARGS)
{
    return to_sql_variant_internal(PG_GETARG_DATUM(0), TIMEOID, PG_GETARG_INT32(1));
}

/* Approximate numerics */
Datum float2sqlvariant(PG_FUNCTION_ARGS)
{
    return to_sql_variant_internal(PG_GETARG_DATUM(0), FLOAT8OID, PG_GETARG_INT32(1));
}

Datum real2sqlvariant(PG_FUNCTION_ARGS)
{
    return to_sql_variant_internal(PG_GETARG_DATUM(0), FLOAT4OID, PG_GETARG_INT32(1));
}

Datum numeric2sqlvariant(PG_FUNCTION_ARGS)
{
    return to_sql_variant_internal(PG_GETARG_DATUM(0), NUMERICOID, PG_GETARG_INT32(1));
}

Datum money2sqlvariant(PG_FUNCTION_ARGS)
{
    return to_sql_variant_internal(PG_GETARG_DATUM(0), CASHOID, PG_GETARG_INT32(1));
}

Datum bigint2sqlvariant(PG_FUNCTION_ARGS)
{
    return to_sql_variant_internal(PG_GETARG_DATUM(0), INT8OID, PG_GETARG_INT32(1));
}

Datum int2sqlvariant(PG_FUNCTION_ARGS)
{
    return to_sql_variant_internal(PG_GETARG_DATUM(0), INT4OID, PG_GETARG_INT32(1));
}

Datum smallint2sqlvariant(PG_FUNCTION_ARGS)
{
    return to_sql_variant_internal(PG_GETARG_DATUM(0), INT2OID, PG_GETARG_INT32(1));
}

Datum tinyint2sqlvariant(PG_FUNCTION_ARGS)
{
    return to_sql_variant_internal(PG_GETARG_DATUM(0), INT1OID, PG_GETARG_INT32(1));
}

Datum bit2sqlvariant(PG_FUNCTION_ARGS)
{
    return to_sql_variant_internal(PG_GETARG_DATUM(0), BITOID, PG_GETARG_INT32(1));
}

/* Character strings */
Datum varchar2sqlvariant(PG_FUNCTION_ARGS)
{
        return to_sql_variant_internal(PG_GETARG_DATUM(0), VARCHAROID, PG_GETARG_INT32(1));
}

Datum nvarchar2sqlvariant(PG_FUNCTION_ARGS)
{
        return to_sql_variant_internal(PG_GETARG_DATUM(0), NVARCHAR2OID, PG_GETARG_INT32(1));
}

Datum char2sqlvariant(PG_FUNCTION_ARGS)
{
        return to_sql_variant_internal(PG_GETARG_DATUM(0), BPCHAROID, PG_GETARG_INT32(1));
}

/* copy from pl_exec.cpp:exec_simple_cast_datum but change a lot */
static Datum cast_value(Datum value, Oid valtype, Oid reqtype, CoercionContext context)
{
    Oid funcid = InvalidOid;
    CoercionPathType result = COERCION_PATH_NONE;

    if (valtype == reqtype) {
        return value;
    }
    if (valtype == UNKNOWNOID) {
        valtype = TEXTOID;
        value = DirectFunctionCall1(textin, value);
    }

    result = find_coercion_pathway(reqtype, valtype, context, &funcid);
    if (result == COERCION_PATH_NONE || result == COERCION_PATH_ARRAYCOERCE) {
        ereport(ERROR,
            (errcode(ERRCODE_CANNOT_COERCE),
                errmsg("cannot cast type %s to %s", format_type_be(valtype), format_type_be(reqtype))));
    }
    if (result == COERCION_PATH_FUNC) {
        value = OidFunctionCall1(funcid, value);
    } else if (result == COERCION_PATH_COERCEVIAIO) {
        Oid typoutput;
        Oid typinput;
        Oid typioparam;
        char* extval = nullptr;
        bool typIsVarlena = false;
        getTypeInputInfo(reqtype, &typinput, &typioparam);
        getTypeOutputInfo(valtype, &typoutput, &typIsVarlena);
        extval = OidOutputFunctionCall(typoutput, value);
        value = OidInputFunctionCall(typinput, extval, typioparam, -1);
    }
    return value;
}

Datum cast_from_sql_variant(bytea* valena, Oid targettype)
{
    Oid srctype;
    int32 srctypemod;
    Datum basic_value = from_sql_variant(valena, &srctype, &srctypemod);

    return cast_value(basic_value, srctype, targettype, COERCION_EXPLICIT);
}

extern "C" Datum sqlvariant2smalldatetime(PG_FUNCTION_ARGS);
extern "C" Datum sqlvariant2date(PG_FUNCTION_ARGS);
extern "C" Datum sqlvariant2time(PG_FUNCTION_ARGS);
extern "C" Datum sqlvariant2float(PG_FUNCTION_ARGS);
extern "C" Datum sqlvariant2real(PG_FUNCTION_ARGS);
extern "C" Datum sqlvariant2numeric(PG_FUNCTION_ARGS);
extern "C" Datum sqlvariant2money(PG_FUNCTION_ARGS);
extern "C" Datum sqlvariant2bigint(PG_FUNCTION_ARGS);
extern "C" Datum sqlvariant2int(PG_FUNCTION_ARGS);
extern "C" Datum sqlvariant2smallint(PG_FUNCTION_ARGS);
extern "C" Datum sqlvariant2bit(PG_FUNCTION_ARGS);
extern "C" Datum sqlvariant2varchar(PG_FUNCTION_ARGS);
extern "C" Datum sqlvariant2char(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(sqlvariant2smalldatetime);
PG_FUNCTION_INFO_V1(sqlvariant2date);
PG_FUNCTION_INFO_V1(sqlvariant2time);
PG_FUNCTION_INFO_V1(sqlvariant2float);
PG_FUNCTION_INFO_V1(sqlvariant2real);
PG_FUNCTION_INFO_V1(sqlvariant2numeric);
PG_FUNCTION_INFO_V1(sqlvariant2money);
PG_FUNCTION_INFO_V1(sqlvariant2bigint);
PG_FUNCTION_INFO_V1(sqlvariant2int);
PG_FUNCTION_INFO_V1(sqlvariant2smallint);
PG_FUNCTION_INFO_V1(sqlvariant2bit);
PG_FUNCTION_INFO_V1(sqlvariant2varchar);
PG_FUNCTION_INFO_V1(sqlvariant2char);

Datum sqlvariant2smalldatetime(PG_FUNCTION_ARGS)
{
    bytea	   *sv = PG_GETARG_BYTEA_PP(0);
    Datum result = cast_from_sql_variant(sv, SMALLDATETIMEOID);
    PG_RETURN_TIMESTAMP(DatumGetTimestamp(result));
}

Datum sqlvariant2date(PG_FUNCTION_ARGS)
{
    bytea          *sv = PG_GETARG_BYTEA_PP(0);
    Datum result = cast_from_sql_variant(sv, DATEOID);
    PG_RETURN_DATEADT(DatumGetDateADT(result));
}

Datum sqlvariant2time(PG_FUNCTION_ARGS)
{
    bytea          *sv = PG_GETARG_BYTEA_PP(0);
    Datum result = cast_from_sql_variant(sv, TIMEOID);
    PG_RETURN_TIMEADT(DatumGetTimeADT(result));
}

Datum sqlvariant2float(PG_FUNCTION_ARGS)
{
    bytea          *sv = PG_GETARG_BYTEA_PP(0);
    Datum result = cast_from_sql_variant(sv, FLOAT8OID);
    PG_RETURN_FLOAT8(DatumGetFloat8(result));
}

Datum sqlvariant2real(PG_FUNCTION_ARGS)
{
    bytea          *sv = PG_GETARG_BYTEA_PP(0);
    Datum result = cast_from_sql_variant(sv, FLOAT4OID);
    PG_RETURN_FLOAT4(DatumGetFloat4(result));
}

Datum sqlvariant2numeric(PG_FUNCTION_ARGS)
{
    bytea          *sv = PG_GETARG_BYTEA_PP(0);
    Datum result = cast_from_sql_variant(sv, NUMERICOID);
    PG_RETURN_NUMERIC(DatumGetNumeric(result));
}

Datum sqlvariant2money(PG_FUNCTION_ARGS)
{
    bytea          *sv = PG_GETARG_BYTEA_PP(0);
    Datum result = cast_from_sql_variant(sv, CASHOID);
    PG_RETURN_CASH(DatumGetInt64(result));
}

Datum sqlvariant2bigint(PG_FUNCTION_ARGS)
{
    bytea          *sv = PG_GETARG_BYTEA_PP(0);
    Datum result = cast_from_sql_variant(sv, INT8OID);
    PG_RETURN_INT64(DatumGetInt64(result));
}

Datum sqlvariant2int(PG_FUNCTION_ARGS)
{
    bytea          *sv = PG_GETARG_BYTEA_PP(0);
    Datum result = cast_from_sql_variant(sv, INT4OID);
    PG_RETURN_INT32(DatumGetInt32(result));
}

Datum sqlvariant2smallint(PG_FUNCTION_ARGS)
{
    bytea          *sv = PG_GETARG_BYTEA_PP(0);
    Datum result = cast_from_sql_variant(sv, INT2OID);
    PG_RETURN_INT16(DatumGetInt16(result));
}

Datum sqlvariant2bit(PG_FUNCTION_ARGS)
{
    bytea          *sv = PG_GETARG_BYTEA_PP(0);
    Datum result = cast_from_sql_variant(sv, BITOID);
    PG_RETURN_VARBIT_P(result);
}

Datum sqlvariant2varchar(PG_FUNCTION_ARGS)
{
    bytea          *sv = PG_GETARG_BYTEA_PP(0);
    Datum result = cast_from_sql_variant(sv, VARCHAROID);
    PG_RETURN_VARCHAR_P(DatumGetVarCharP(result));
}

Datum sqlvariant2char(PG_FUNCTION_ARGS)
{
    bytea          *sv = PG_GETARG_BYTEA_PP(0);
    Datum result = cast_from_sql_variant(sv, BPCHAROID);
    PG_RETURN_BPCHAR_P(DatumGetBpCharP(result));
}

