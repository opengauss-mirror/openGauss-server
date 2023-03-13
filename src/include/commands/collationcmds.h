/* -------------------------------------------------------------------------
 *
 * collationcmds.h
 *	  prototypes for collationcmds.c.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/collationcmds.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef COLLATIONCMDS_H
#define COLLATIONCMDS_H

#include "catalog/objectaddress.h"
#include "nodes/parsenodes.h"

extern ObjectAddress DefineCollation(List* names, List* parameters);
extern void RenameCollation(List* name, const char* newname);
extern ObjectAddress AlterCollationOwner(List* name, Oid newOwnerId);
extern void AlterCollationOwner_oid(Oid collationOid, Oid newOwnerId);
extern ObjectAddress AlterCollationNamespace(List* name, const char* newschema);
extern Oid AlterCollationNamespace_oid(Oid collOid, Oid newNspOid);
extern void IsThereCollationInNamespace(const char *collname, Oid nspOid);

#endif /* COLLATIONCMDS_H */
