/* -------------------------------------------------------------------------
 *
 * parse_type.h
 *		handle type operations for parser
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * Portions Copyright (c) 2021, openGauss Contributors
 * src/include/parser/parse_type.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PARSE_TYPE_H
#define PARSE_TYPE_H

#include "access/htup.h"
#include "parser/parse_node.h"

typedef HeapTuple Type;

extern Type LookupTypeName(ParseState* pstate, const TypeName* typname, int32* typmod_p, bool print_notice = true);
extern Type LookupTypeNameExtended(ParseState* pstate, const TypeName* typname, int32* typmod_p, bool temp_ok,
                            bool print_notice = true);
extern Oid LookupPctTypeInPackage(RangeVar* rel, Oid pkgOid, const char* field);
extern Oid LookupTypeInPackage(const char* typeName, Oid pkgOid = InvalidOid, Oid namespaceId = InvalidOid);
extern Type typenameType(ParseState* pstate, const TypeName* typname, int32* typmod_p);
extern Oid typenameTypeId(ParseState* pstate, const TypeName* typname);
extern void typenameTypeIdAndMod(ParseState* pstate, const TypeName* typname, Oid* typeid_p, int32* typmod_p);

extern char* TypeNameToString(const TypeName* typname);
extern char* TypeNameListToString(List* typenames);

extern Oid LookupCollation(ParseState* pstate, List* collnames, int location);
extern Oid GetColumnDefCollation(ParseState* pstate, ColumnDef* coldef, Oid typeOid);

extern Type typeidType(Oid id);

extern Oid typeTypeId(Type tp);
extern int16 typeLen(Type t);
extern bool typeByVal(Type t);
extern char* typeTypeName(Type t);
extern Oid typeTypeRelid(Type typ);
extern Oid typeTypeCollation(Type typ);
extern Datum stringTypeDatum(Type tp, char* string, int32 atttypmod);

extern Oid typeidTypeRelid(Oid type_id);
extern bool IsTypeSupportedByCStore(_in_ Oid typeOid);
extern bool CheckTypeSupportRowToVec(List* targetlist, int errLevel);
extern bool IsTypeSupportedByORCRelation(_in_ Oid typeOid);
extern bool IsTypeSupportedByTsStore(_in_ int kvtype, _in_ Oid typeOid);
extern bool IsTypeSupportedByUStore (_in_ Oid typeOid, _in_ int32 typeMod);

extern void parseTypeString(const char* str, Oid* typeid_p, int32* typmod_p);
extern bool IsTypeTableInInstallationGroup(const Type type_tup);
extern HeapTuple FindPkgVariableType(ParseState* pstate, const TypeName* typname, int32* typmod_p);
extern char* CastPackageTypeName(const char* typName, Oid  pkgOid, bool isPackage, bool isPublic = true);
#define ISCOMPLEX(typeid) (typeidTypeRelid(typeid) != InvalidOid)

#endif /* PARSE_TYPE_H */
