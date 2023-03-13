/* -------------------------------------------------------------------------
 *
 * alter.h
 *	  prototypes for commands/alter.c
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/alter.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef ALTER_H
#define ALTER_H

#include "catalog/dependency.h"
#include "catalog/objectaddress.h"
#include "utils/acl.h"
#include "utils/relcache.h"

extern ObjectAddress ExecRenameStmt(RenameStmt* stmt);
extern ObjectAddress ExecAlterObjectSchemaStmt(AlterObjectSchemaStmt* stmt, ObjectAddress *oldSchemaAddr);
extern Oid AlterObjectNamespace_oid(Oid classId, Oid objid, Oid nspOid, ObjectAddresses* objsMoved);
extern Oid AlterObjectNamespace(Relation rel, int oidCacheId, int nameCacheId, Oid objid, Oid nspOid, int Anum_name,
    int Anum_namespace, int Anum_owner, AclObjectKind acl_kind);
extern ObjectAddress ExecAlterOwnerStmt(AlterOwnerStmt* stmt);
extern Oid AlterObjectNamespace_internal(Relation rel, Oid objid, Oid nspOid);

#endif /* ALTER_H */
