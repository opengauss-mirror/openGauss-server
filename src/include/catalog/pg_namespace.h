/* -------------------------------------------------------------------------
 *
 * pg_namespace.h
 *      definition of the system "namespace" relation (pg_namespace)
 *      along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_namespace.h
 *
 * NOTES
 *      the genbki.pl script reads this file and generates .bki
 *      information from the DATA() statements.
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_NAMESPACE_H
#define PG_NAMESPACE_H

#include "catalog/genbki.h"

/* ----------------------------------------------------------------
 *        pg_namespace definition.
 *
 *        cpp turns this into typedef struct FormData_pg_namespace
 *
 *    nspname             name of the namespace
 *    nspowner            owner (creator) of the namespace
 *    nspacl              access privilege list
 * ----------------------------------------------------------------
 */
#define NamespaceRelationId  2615
#define NamespaceRelation_Rowtype_Id 11629

CATALOG(pg_namespace,2615) BKI_SCHEMA_MACRO
{
    NameData    nspname;
    Oid         nspowner;
    int8        nsptimeline;

#ifdef CATALOG_VARLEN            /* variable-length fields start here */
    aclitem     nspacl[1];
#endif
	char		in_redistribution;
    bool		nspblockchain;
} FormData_pg_namespace;

/* ----------------
 *        Form_pg_namespace corresponds to a pointer to a tuple with
 *        the format of pg_namespace relation.
 * ----------------
 */
typedef FormData_pg_namespace *Form_pg_namespace;

/* ----------------
 *        compiler constants for pg_namespace
 * ----------------
 */

#define Natts_pg_namespace				6
#define Anum_pg_namespace_nspname         1
#define Anum_pg_namespace_nspowner        2
#define Anum_pg_namespace_nsptimeline     3
#define Anum_pg_namespace_nspacl          4
#define Anum_pg_namespace_in_redistribution 5
#define Anum_pg_namespace_nspblockchain   6


/* ----------------
 * initial contents of pg_namespace
 * ---------------
 */

DATA(insert OID = 11 ( "pg_catalog" PGUID 0 _null_ n f));
DESCR("system catalog schema");
#define PG_CATALOG_NAMESPACE 11
DATA(insert OID = 99 ( "pg_toast" PGUID 0 _null_ n f));
DESCR("reserved schema for TOAST tables");
#define PG_TOAST_NAMESPACE 99

DATA(insert OID = 100 ( "cstore" PGUID 0 _null_ n f));
DESCR("reserved schema for DELTA tables");
#define CSTORE_NAMESPACE 100

DATA(insert OID = 3988 ( "pkg_service" PGUID 0 _null_ n f));
DESCR("pkg_service schema");
#define PG_PKG_SERVICE_NAMESPACE 3988

DATA(insert OID = 2200 ( "public" PGUID 0 _null_ n f));
DESCR("standard public schema");
#define PG_PUBLIC_NAMESPACE 2200

DATA(insert OID = 4988 ( "dbe_perf" PGUID 0 _null_ n f));
DESCR("dbe_perf schema");
#define PG_DBEPERF_NAMESPACE 4988

DATA(insert OID = 4989 ( "snapshot" PGUID 0 _null_ n f));
DESCR("snapshot schema");
#define PG_SNAPSHOT_NAMESPACE 4989

DATA(insert OID = 4990 ( "blockchain" PGUID 0 _null_ n f));
DESCR("blockchain schema");
#define PG_BLOCKCHAIN_NAMESPACE 4990

DATA(insert OID = 4991 ( "db4ai" PGUID 0 _null_ n f));
DESCR("db4ai schema");
#define PG_DB4AI_NAMESPACE 4991
DATA(insert OID = 4992 ( "dbe_pldebugger" PGUID 0 _null_ n f));
DESCR("dbe_pldebugger schema");
#define PG_PLDEBUG_NAMESPACE 4992
DATA(insert OID = 7813 ( "sqladvisor" PGUID 0 _null_ n f));
DESCR("sqladvisor schema");
#define PG_SQLADVISOR_NAMESPACE 7813
#ifndef ENABLE_MULTIPLE_NODES
DATA(insert OID = 4993 ( "dbe_pldeveloper" PGUID 0 _null_ n f));
DESCR("dbe_pldeveloper schema");
#define DBE_PLDEVELOPER_NAMESPACE 4993
#endif
/*
 * prototypes for functions in pg_namespace.c
 */
extern Oid NamespaceCreate(const char *nspName, Oid ownerId, bool isTemp, bool hasBlockChain = false);
extern bool IsLedgerNameSpace(Oid nspOid);

#endif   /* PG_NAMESPACE_H */

