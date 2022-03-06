/* -------------------------------------------------------------------------
 *
 * catalog.h
 *	  prototypes for functions in backend/catalog/catalog.c
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/catalog.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef CATALOG_H
#define CATALOG_H

/*
 *	'pgrminclude ignore' needed here because CppAsString2() does not throw
 *	an error if the symbol is not defined.
 */
#include "catalog/catversion.h" /* pgrminclude ignore */
#include "catalog/pg_class.h"
#include "storage/smgr/relfilenode.h"
#include "utils/relcache.h"

#define FORKNAMECHARS	4		/* max chars for a fork name */
#define OIDCHARS		10		/* max chars printed by %u */
#define TABLESPACE_VERSION_DIRECTORY	"PG_" PG_MAJORVERSION "_" \
									CppAsString2(CATALOG_VERSION_NO)

/* file name: Cxxxxx.0
 * the max length is up to MaxAttrNumber
 */
#define COLFILE_SUFFIX 	"C"
#define COLFILE_SUFFIX_MAXLEN  8

extern const char *forkNames[];
extern ForkNumber forkname_to_number(char *forkName, BlockNumber *segno = NULL);
extern int	forkname_chars(const char *str, ForkNumber *);

extern char *relpathbackend(RelFileNode rnode, BackendId backend, ForkNumber forknum);
extern char *GetDatabasePath(Oid dbNode, Oid spcNode);

/* First argument is a RelFileNodeBackend */
#define relpath(rnode, forknum) \
		relpathbackend((rnode).node, (rnode).backend, (forknum))

/* First argument is a RelFileNode */
#define relpathperm(rnode, forknum) \
		relpathbackend((rnode), InvalidBackendId, (forknum))

extern RelFileNodeForkNum relpath_to_filenode(char *path);

extern bool IsSystemRelation(Relation relation);
extern bool IsToastRelation(Relation relation);
extern bool IsCatalogRelation(Relation relation);

extern bool IsSysSchema(Oid namespaceId);
extern bool IsSystemClass(Form_pg_class reltuple);
extern bool IsToastClass(Form_pg_class reltuple);
extern bool IsCatalogClass(Oid relid, Form_pg_class reltuple);

extern bool IsSystemNamespace(Oid namespaceId);
extern bool IsToastNamespace(Oid namespaceId);
extern bool IsCStoreNamespace(Oid namespaceId);
extern bool IsPerformanceNamespace(Oid namespaceId);
extern bool IsSnapshotNamespace(Oid namespaceId);
extern bool IsMonitorSpace(Oid namespaceId);

extern bool IsReservedName(const char *name);

extern bool IsSharedRelation(Oid relationId);

extern Oid	GetNewOid(Relation relation);
extern Oid GetNewOidWithIndex(Relation relation, Oid indexId, AttrNumber oidcolumn);
extern Oid GetNewRelFileNode(Oid reltablespace, Relation pg_class, char relpersistence);

extern bool IsPackageSchemaOid(Oid relnamespace);
extern bool IsPackageSchemaName(const char* schemaName);



#endif   /* CATALOG_H */
