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
#define Natts_pg_namespace                4
#define Anum_pg_namespace_nspname         1
#define Anum_pg_namespace_nspowner        2
#define Anum_pg_namespace_nsptimeline     3
#define Anum_pg_namespace_nspacl          4


/* ----------------
 * initial contents of pg_namespace
 * ---------------
 */
DATA(insert OID = 11 ( "pg_catalog" PGUID 0 _null_ ));
DESCR("system catalog schema");
#define PG_CATALOG_NAMESPACE 11
DATA(insert OID = 99 ( "pg_toast" PGUID 0 _null_ ));
DESCR("reserved schema for TOAST tables");
#define PG_TOAST_NAMESPACE 99

DATA(insert OID = 100 ( "cstore" PGUID 0 _null_ ));
DESCR("reserved schema for DELTA tables");
#define CSTORE_NAMESPACE 100

DATA(insert OID = 2200 ( "public" PGUID 0 _null_ ));
DESCR("standard public schema");
#define PG_PUBLIC_NAMESPACE 2200

DATA(insert OID = 4988 ( "dbe_perf" PGUID 0 _null_ ));
DESCR("dbe_perf schema");
#define PG_DBMSPERF_NAMESPACE 4988

DATA(insert OID = 4989 ( "snapshot" PGUID 0 _null_ ));
DESCR("snapshot schema");
#define PG_SNAPSHOT_NAMESPACE 4989

/*
 * prototypes for functions in pg_namespace.c
 */
extern Oid NamespaceCreate(const char *nspName, Oid ownerId, bool isTemp);

#endif   /* PG_NAMESPACE_H */

