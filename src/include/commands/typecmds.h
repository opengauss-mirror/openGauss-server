/* -------------------------------------------------------------------------
 *
 * typecmds.h
 *	  prototypes for typecmds.c.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/typecmds.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef TYPECMDS_H
#define TYPECMDS_H

#include "access/htup.h"
#include "catalog/dependency.h"
#include "catalog/objectaddress.h"
#include "nodes/parsenodes.h"

#define DEFAULT_TYPDELIM ','
#define TYPE_COMPOSITE_DEFAULT 0 /* Postgres composite type for default */
#define TYPE_COMPOSITE_OBJECT_TYPE 1 /* object type */
#define TYPE_COMPOSITE_OBJECT_TYPE_BODY 2 /* object type body */

extern ObjectAddress DefineType(List* names, List* parameters);
extern void RemoveTypeById(Oid typeOid);
extern ObjectAddress DefineDomain(CreateDomainStmt* stmt);
extern ObjectAddress DefineEnum(CreateEnumStmt* stmt);
extern ObjectAddress DefineRange(CreateRangeStmt* stmt);
extern ObjectAddress AlterEnum(AlterEnumStmt* stmt);
extern ObjectAddress DefineSet(CreateSetStmt* stmt);
extern ObjectAddress DefineCompositeType(RangeVar* typevar, List* coldeflist,
    bool replace = false, ObjectAddress *reladdress = NULL, bool is_object_type = false, Oid typbasetype = InvalidOid);
extern ObjectAddress DefineObjectTypeSpec(CompositeTypeStmt* stmt);
extern void DefineObjectTypeBody(CompositeTypeStmt* stmt);
extern void RemoveTypeMethod(Oid typeoid);

extern Oid AssignTypeArrayOid(void);
extern ObjectAddress DefineTableOfType(const TableOfTypeStmt* stmt, Oid typbasetype = InvalidOid);
extern ObjectAddress AlterDomainDefault(List* names, Node* defaultRaw);
extern ObjectAddress AlterDomainNotNull(List* names, bool notNull);
extern ObjectAddress AlterDomainAddConstraint(List* names, Node* constr);
extern ObjectAddress AlterDomainValidateConstraint(List* names, char* constrName);
extern ObjectAddress AlterDomainDropConstraint(List* names, const char* constrName, DropBehavior behavior, bool missing_ok);

extern void checkDomainOwner(HeapTuple tup);

extern List* GetDomainConstraints(Oid typeOid);

extern ObjectAddress RenameType(RenameStmt* stmt);
extern ObjectAddress AlterTypeOwner(List* names, Oid newOwnerId, ObjectType objecttype, bool altertype);
extern void AlterTypeOwnerInternal(Oid typeOid, Oid newOwnerId, bool hasDependEntry);
extern ObjectAddress AlterTypeNamespace(List* names, const char* newschema, ObjectType objecttype);
extern Oid AlterTypeNamespace_oid(Oid typeOid, Oid nspOid, ObjectAddresses* objsMoved);
extern Oid AlterTypeNamespaceInternal(
    Oid typeOid, Oid nspOid, bool isImplicitArray, bool errorOnTableType, ObjectAddresses* objsMoved,
    char* newTypeName = NULL);
extern void AlterTypeOwnerByPkg(Oid pkgOid, Oid newOwnerId);
extern void AlterTypeOwnerByFunc(Oid funcOid, Oid newOwnerId);

#endif /* TYPECMDS_H */
