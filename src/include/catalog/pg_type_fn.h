/* -------------------------------------------------------------------------
 *
 * pg_type_fn.h
 *	 prototypes for functions in catalog/pg_type.c
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_type_fn.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_TYPE_FN_H
#define PG_TYPE_FN_H

#include "catalog/objectaddress.h"
#include "nodes/nodes.h"


extern ObjectAddress TypeShellMake(const char *typname,
                         Oid typeNamespace,
                         Oid ownerId);

extern ObjectAddress TypeCreate(Oid newTypeOid,
                      const char *typname,
                      Oid typeNamespace,
                      Oid relationOid,
                      char relationKind,
                      Oid ownerId,
                      int16 internalSize,
                      char typeType,
                      char typeCategory,
                      bool typePreferred,
                      char typDelim,
                      Oid inputProcedure,
                      Oid outputProcedure,
                      Oid receiveProcedure,
                      Oid sendProcedure,
                      Oid typmodinProcedure,
                      Oid typmodoutProcedure,
                      Oid analyzeProcedure,
                      Oid elementType,
                      bool isImplicitArray,
                      Oid arrayType,
                      Oid baseType,
                      const char *defaultTypeValue,
                      char *defaultTypeBin,
                      bool passedByValue,
                      char alignment,
                      char storage,
                      int32 typeMod,
                      int32 typNDims,
                      bool typeNotNull,
                      Oid typeCollation,
                      TypeDependExtend* dependExtend = NULL);

extern void GenerateTypeDependencies(Oid typeNamespace,
                                     Oid typeObjectId,
                                     Oid relationOid,
                                     char relationKind,
                                     Oid owner,
                                     Oid inputProcedure,
                                     Oid outputProcedure,
                                     Oid receiveProcedure,
                                     Oid sendProcedure,
                                     Oid typmodinProcedure,
                                     Oid typmodoutProcedure,
                                     Oid analyzeProcedure,
                                     Oid elementType,
                                     bool isImplicitArray,
                                     Oid baseType,
                                     Oid typeCollation,
                                     Node *defaultExpr,
                                     bool rebuild,
                                     const char* typname = NULL,
                                     TypeDependExtend* dependExtend = NULL);

extern void RenameTypeInternal(Oid typeOid, 
                               const char *newTypeName,
                               Oid typeNamespace);

extern char *makeArrayTypeName(const char *typname, Oid typeNamespace);

extern bool moveArrayTypeName(Oid typeOid, 
                              const char *typname,
                              Oid typeNamespace);
extern void InstanceTypeNameDependExtend(TypeDependExtend** dependExtend);
extern void ReleaseTypeNameDependExtend(TypeDependExtend** dependExtend);
extern char* MakeTypeNamesStrForTypeOid(Oid typOid, bool* dependUndefined = NULL, StringInfo concatName = NULL);
extern void MakeTypeNamesStrForTypeOid(StringInfo concatName, Oid typOid,
                                       char** schemaName = NULL, char** pkgName = NULL, char** name = NULL);
extern Oid GetTypePackageOid(Oid typoid);
extern Oid TypeNameGetOid(const char* schemaName, const char* packageName, const char* typeName);
#endif   /* PG_TYPE_FN_H */

