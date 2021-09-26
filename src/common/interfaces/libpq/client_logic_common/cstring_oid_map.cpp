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
 * cstring_oid_map.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_common\cstring_oid_map.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "cstring_oid_map.h"
#include "libpq-int.h"
#include "libpq-fe.h"
#include "libpq-int.h"
#include "../client_logic_cache/dataTypes.def"
#define RESIZE_FACTOR 4

CStringOidMap::CStringOidMap() : size(0), k_v_map(NULL) 
{
    fill_types_map();
}
CStringOidMap::~CStringOidMap()
{
    clear();
}

void CStringOidMap::fill_types_map() 
{
    set("bool", BOOLOID);
    set("bytea", BYTEAOID);
    set("char", CHAROID);
    set("name", NAMEOID);
    set("int8", INT8OID);
    set("int2", INT2OID);
    set("int1", INT1OID);
    set("int2vector", INT2VECTOROID);
    set("int4", INT4OID);
    set("regproc", REGPROCOID);
    set("text", TEXTOID);
    set("oid", OIDOID);
    set("tid", TIDOID);
    set("xid", XIDOID);
    set("shortxid", SHORTXIDOID);
    set("cid", CIDOID);
    set("vectoroid", OIDVECTOROID);
    set("vectorextendoid", OIDVECTOREXTENDOID);
    set("raw", RAWOID);
    set("blob", BLOBOID);
    set("clob", CLOBOID);
    set("json", JSONOID);
    set("xml", XMLOID);
    set("pgnodetree", PGNODETREEOID);
    set("smgr", SMGROID);
    set("point", POINTOID);
    set("lseg", LSEGOID);
    set("path", PATHOID);
    set("box", BOXOID);
    set("polygon", POLYGONOID);
    set("line", LINEOID);
    set("float4", FLOAT4OID);
    set("float8", FLOAT8OID);
    set("abstime", ABSTIMEOID);
    set("reltime", RELTIMEOID);
    set("tinterval", TINTERVALOID);
    set("unknown", UNKNOWNOID);
    set("circle", CIRCLEOID);
    set("cash", CASHOID);
    set("casharray", CASHARRAYOID);
    set("macaddr", MACADDROID);
    set("inet", INETOID);
    set("cidr", CIDROID);
    set("boolarray", BOOLARRAYOID);
    set("bytearray", BYTEARRAYOID);
    set("chararray", CHARARRAYOID);
    set("namearray", NAMEARRAYOID);
    set("int2array", INT2ARRAYOID);
    set("int1array", INT1ARRAYOID);
    set("int4array", INT4ARRAYOID);
    set("textarray", TEXTARRAYOID);
    set("bpchararray", BPCHARARRAYOID);
    set("varchararray", VARCHARARRAYOID);
    set("int8array", INT8ARRAYOID);
    set("float4array", FLOAT4ARRAYOID);
    set("float8array", FLOAT8ARRAYOID);
    set("abstimearray", ABSTIMEARRAYOID);
    set("reltimearray", RELTIMEARRAYOID);
    set("arraytinterval", ARRAYTINTERVALOID);
    set("aclitem", ACLITEMOID);
    set("aclitemarray", ACLITEMARRAYOID);
    set("inetarray", INETARRAYOID);
    set("cidrarray", CIDRARRAYOID);
    set("cstringarray", CSTRINGARRAYOID);
    set("bpchar", BPCHAROID);
    set("varchar", VARCHAROID);
    set("nvarchar2", NVARCHAR2OID);
    set("nvarchar2array", NVARCHAR2ARRAYOID);
    set("date", DATEOID);
    set("time", TIMEOID);
    set("timestamp", TIMESTAMPOID);
    set("timestamparray", TIMESTAMPARRAYOID);
    set("datearray", DATEARRAYOID);
    set("timearray", TIMEARRAYOID);
    set("timestamptz", TIMESTAMPTZOID);
    set("timestamptzarray", TIMESTAMPTZARRAYOID);
    set("interval", INTERVALOID);
    set("arrayinterval", ARRAYINTERVALOID);
    set("arraynumeric", ARRAYNUMERICOID);
    set("timetz", TIMETZOID);
    set("arraytimetz", ARRAYTIMETZOID);
    set("bit", BITOID);
    set("bitarray", BITARRAYOID);
    set("varbit", VARBITOID);
    set("varbitarray", VARBITARRAYOID);
    set("numeric", NUMERICOID);
    set("refcursor", REFCURSOROID);
    set("regprocedure", REGPROCEDUREOID);
    set("regoper", REGOPEROID);
    set("regoperator", REGOPERATOROID);
    set("regclass", REGCLASSOID);
    set("regtype", REGTYPEOID);
    set("regtypearray", REGTYPEARRAYOID);
    set("uuid", UUIDOID);
    set("tsvector", TSVECTOROID);
    set("gtsvector", GTSVECTOROID);
    set("tsquery", TSQUERYOID);
    set("regconfig", REGCONFIGOID);
    set("regdictionary", REGDICTIONARYOID);
    set("int4range", INT4RANGEOID);
    set("record", RECORDOID);
    set("recordarray", RECORDARRAYOID);
    set("cstring", CSTRINGOID);
    set("any", ANYOID);
    set("anyarray", ANYARRAYOID);
    set("void", VOIDOID);
    set("trigger", TRIGGEROID);
    set("language_handler", LANGUAGE_HANDLEROID);
    set("internal", INTERNALOID);
    set("opaque", OPAQUEOID);
    set("anyelement", ANYELEMENTOID);
    set("anynonarray", ANYNONARRAYOID);
    set("anyenum", ANYENUMOID);
    set("fdw_handler", FDW_HANDLEROID);
    set("anyrange", ANYRANGEOID);
    set("smalldatetime", SMALLDATETIMEOID);
    set("smalldatetimearray", SMALLDATETIMEARRAYOID);
    set("hll_", HLL_OID);
    set("hll_array", HLL_ARRAYOID);
    set("hll_hashval_", HLL_HASHVAL_OID);
    set("hll_hashval_array", HLL_HASHVAL_ARRAYOID);
    set("byteawithoutorderwithequalcol", BYTEAWITHOUTORDERWITHEQUALCOLOID);
    set("byteawithoutordercol", BYTEAWITHOUTORDERCOLOID);
}

void CStringOidMap::set(const char *key, const Oid value)
{
    Assert(key && value);
    size_t key_index = index(key);
    if (key_index == size) {
        if (size % RESIZE_FACTOR == 0) {
            k_v_map = (struct constKeyValueStrOid *)libpq_realloc(k_v_map, sizeof(*k_v_map) * size,
                sizeof(*k_v_map) * (size + RESIZE_FACTOR));
            if (k_v_map == NULL) {
                return;
            }
        }
        check_strncpy_s(strncpy_s(k_v_map[size].key, sizeof(k_v_map[size].key), key, strlen(key)));
        ++size;
    }
    k_v_map[key_index].value = value;
}

void CStringOidMap::clear()
{
    libpq_free(k_v_map);
    size = 0;
}

const Oid CStringOidMap::find(const char *key) const
{
    size_t i = index(key);
    if (i < size) {
        return k_v_map[i].value;
    }

    return InvalidOid;
}
const char *CStringOidMap::find_by_oid(const Oid oid) const
{
    for (size_t i = 0; i < size; ++i) {
        if (k_v_map[i].value == oid) {
            return k_v_map[i].key;
        }
    }

    return NULL;
}
const size_t CStringOidMap::index(const char *key) const
{
    size_t i = 0;
    for (; i < size; ++i) {
        if (pg_strncasecmp(k_v_map[i].key, key, NAMEDATALEN) == 0) {
            break;
        }
    }
    return i;
}
