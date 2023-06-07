/*-------------------------------------------------------------------------
 *
 * publicationcmds.h
 *	  prototypes for publicationcmds.cpp.
 *
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/publicationcmds.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PUBLICATIONCMDS_H
#define PUBLICATIONCMDS_H

#include "nodes/parsenodes_common.h"

extern ObjectAddress CreatePublication(CreatePublicationStmt *stmt);
extern void AlterPublication(AlterPublicationStmt *stmt);
extern void RemovePublicationById(Oid proid);
extern void RemovePublicationRelById(Oid proid);

extern ObjectAddress AlterPublicationOwner(const char *name, Oid newOwnerId);
extern void AlterPublicationOwner_oid(Oid pubid, Oid newOwnerId);

#endif   /* PUBLICATIONCMDS_H */
