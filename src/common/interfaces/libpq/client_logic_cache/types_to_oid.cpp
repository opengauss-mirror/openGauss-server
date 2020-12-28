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
 * types_to_oid.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_cache\types_to_oid.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "types_to_oid.h"
#include "dataTypes.def"

CStringOidMap TypesMap::typesTextToOidMap;

void TypesMap::fill_types_map()
{
    typesTextToOidMap.set("bool", BOOLOID);
    typesTextToOidMap.set("bytea", BYTEAOID);
    typesTextToOidMap.set("char", CHAROID);
    typesTextToOidMap.set("name", NAMEOID);
    typesTextToOidMap.set("int8", INT8OID);
    typesTextToOidMap.set("int2", INT2OID);
    typesTextToOidMap.set("int1", INT1OID);
    typesTextToOidMap.set("int2vector", INT2VECTOROID);
    typesTextToOidMap.set("int4", INT4OID);
    typesTextToOidMap.set("regproc", REGPROCOID);
    typesTextToOidMap.set("text", TEXTOID);
    typesTextToOidMap.set("oid", OIDOID);
    typesTextToOidMap.set("tid", TIDOID);
    typesTextToOidMap.set("xid", XIDOID);
    typesTextToOidMap.set("shortxid", SHORTXIDOID);
    typesTextToOidMap.set("cid", CIDOID);
    typesTextToOidMap.set("vectoroid", OIDVECTOROID);
    typesTextToOidMap.set("vectorextendoid", OIDVECTOREXTENDOID);
    typesTextToOidMap.set("raw", RAWOID);
    typesTextToOidMap.set("blob", BLOBOID);
    typesTextToOidMap.set("clob", CLOBOID);
    typesTextToOidMap.set("json", JSONOID);
    typesTextToOidMap.set("xml", XMLOID);
    typesTextToOidMap.set("pgnodetree", PGNODETREEOID);
    typesTextToOidMap.set("smgr", SMGROID);
    typesTextToOidMap.set("point", POINTOID);
    typesTextToOidMap.set("lseg", LSEGOID);
    typesTextToOidMap.set("path", PATHOID);
    typesTextToOidMap.set("box", BOXOID);
    typesTextToOidMap.set("polygon", POLYGONOID);
    typesTextToOidMap.set("line", LINEOID);
    typesTextToOidMap.set("float4", FLOAT4OID);
    typesTextToOidMap.set("float8", FLOAT8OID);
    typesTextToOidMap.set("abstime", ABSTIMEOID);
    typesTextToOidMap.set("reltime", RELTIMEOID);
    typesTextToOidMap.set("tinterval", TINTERVALOID);
    typesTextToOidMap.set("unknown", UNKNOWNOID);
    typesTextToOidMap.set("circle", CIRCLEOID);
    typesTextToOidMap.set("cash", CASHOID);
    typesTextToOidMap.set("casharray", CASHARRAYOID);
    typesTextToOidMap.set("macaddr", MACADDROID);
    typesTextToOidMap.set("inet", INETOID);
    typesTextToOidMap.set("cidr", CIDROID);
    typesTextToOidMap.set("boolarray", BOOLARRAYOID);
    typesTextToOidMap.set("bytearray", BYTEARRAYOID);
    typesTextToOidMap.set("chararray", CHARARRAYOID);
    typesTextToOidMap.set("namearray", NAMEARRAYOID);
    typesTextToOidMap.set("int2array", INT2ARRAYOID);
    typesTextToOidMap.set("int1array", INT1ARRAYOID);
    typesTextToOidMap.set("int4array", INT4ARRAYOID);
    typesTextToOidMap.set("textarray", TEXTARRAYOID);
    typesTextToOidMap.set("bpchararray", BPCHARARRAYOID);
    typesTextToOidMap.set("varchararray", VARCHARARRAYOID);
    typesTextToOidMap.set("int8array", INT8ARRAYOID);
    typesTextToOidMap.set("float4array", FLOAT4ARRAYOID);
    typesTextToOidMap.set("float8array", FLOAT8ARRAYOID);
    typesTextToOidMap.set("abstimearray", ABSTIMEARRAYOID);
    typesTextToOidMap.set("reltimearray", RELTIMEARRAYOID);
    typesTextToOidMap.set("arraytinterval", ARRAYTINTERVALOID);
    typesTextToOidMap.set("aclitem", ACLITEMOID);
    typesTextToOidMap.set("aclitemarray", ACLITEMARRAYOID);
    typesTextToOidMap.set("inetarray", INETARRAYOID);
    typesTextToOidMap.set("cidrarray", CIDRARRAYOID);
    typesTextToOidMap.set("cstringarray", CSTRINGARRAYOID);
    typesTextToOidMap.set("bpchar", BPCHAROID);
    typesTextToOidMap.set("varchar", VARCHAROID);
    typesTextToOidMap.set("nvarchar2", NVARCHAR2OID);
    typesTextToOidMap.set("nvarchar2array", NVARCHAR2ARRAYOID);
    typesTextToOidMap.set("date", DATEOID);
    typesTextToOidMap.set("time", TIMEOID);
    typesTextToOidMap.set("timestamp", TIMESTAMPOID);
    typesTextToOidMap.set("timestamparray", TIMESTAMPARRAYOID);
    typesTextToOidMap.set("datearray", DATEARRAYOID);
    typesTextToOidMap.set("timearray", TIMEARRAYOID);
    typesTextToOidMap.set("timestamptz", TIMESTAMPTZOID);
    typesTextToOidMap.set("timestamptzarray", TIMESTAMPTZARRAYOID);
    typesTextToOidMap.set("interval", INTERVALOID);
    typesTextToOidMap.set("arrayinterval", ARRAYINTERVALOID);
    typesTextToOidMap.set("arraynumeric", ARRAYNUMERICOID);
    typesTextToOidMap.set("timetz", TIMETZOID);
    typesTextToOidMap.set("arraytimetz", ARRAYTIMETZOID);
    typesTextToOidMap.set("bit", BITOID);
    typesTextToOidMap.set("bitarray", BITARRAYOID);
    typesTextToOidMap.set("varbit", VARBITOID);
    typesTextToOidMap.set("varbitarray", VARBITARRAYOID);
    typesTextToOidMap.set("numeric", NUMERICOID);
    typesTextToOidMap.set("refcursor", REFCURSOROID);
    typesTextToOidMap.set("regprocedure", REGPROCEDUREOID);
    typesTextToOidMap.set("regoper", REGOPEROID);
    typesTextToOidMap.set("regoperator", REGOPERATOROID);
    typesTextToOidMap.set("regclass", REGCLASSOID);
    typesTextToOidMap.set("regtype", REGTYPEOID);
    typesTextToOidMap.set("regtypearray", REGTYPEARRAYOID);
    typesTextToOidMap.set("uuid", UUIDOID);
    typesTextToOidMap.set("tsvector", TSVECTOROID);
    typesTextToOidMap.set("gtsvector", GTSVECTOROID);
    typesTextToOidMap.set("tsquery", TSQUERYOID);
    typesTextToOidMap.set("regconfig", REGCONFIGOID);
    typesTextToOidMap.set("regdictionary", REGDICTIONARYOID);
    typesTextToOidMap.set("int4range", INT4RANGEOID);
    typesTextToOidMap.set("record", RECORDOID);
    typesTextToOidMap.set("recordarray", RECORDARRAYOID);
    typesTextToOidMap.set("cstring", CSTRINGOID);
    typesTextToOidMap.set("any", ANYOID);
    typesTextToOidMap.set("anyarray", ANYARRAYOID);
    typesTextToOidMap.set("void", VOIDOID);
    typesTextToOidMap.set("trigger", TRIGGEROID);
    typesTextToOidMap.set("language_handler", LANGUAGE_HANDLEROID);
    typesTextToOidMap.set("internal", INTERNALOID);
    typesTextToOidMap.set("opaque", OPAQUEOID);
    typesTextToOidMap.set("anyelement", ANYELEMENTOID);
    typesTextToOidMap.set("anynonarray", ANYNONARRAYOID);
    typesTextToOidMap.set("anyenum", ANYENUMOID);
    typesTextToOidMap.set("fdw_handler", FDW_HANDLEROID);
    typesTextToOidMap.set("anyrange", ANYRANGEOID);
    typesTextToOidMap.set("smalldatetime", SMALLDATETIMEOID);
    typesTextToOidMap.set("smalldatetimearray", SMALLDATETIMEARRAYOID);
    typesTextToOidMap.set("hll_", HLL_OID);
    typesTextToOidMap.set("hll_array", HLL_ARRAYOID);
    typesTextToOidMap.set("hll_hashval_", HLL_HASHVAL_OID);
    typesTextToOidMap.set("hll_hashval_array", HLL_HASHVAL_ARRAYOID);
    typesTextToOidMap.set("byteawithoutorderwithequalcol", BYTEAWITHOUTORDERWITHEQUALCOLOID);
    typesTextToOidMap.set("byteawithoutordercol", BYTEAWITHOUTORDERCOLOID);
}