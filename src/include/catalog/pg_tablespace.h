/* -------------------------------------------------------------------------
 *
 * pg_tablespace.h
 *      definition of the system "tablespace" relation (pg_tablespace)
 *      along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_tablespace.h
 *
 * NOTES
 *      the genbki.pl script reads this file and generates .bki
 *      information from the DATA() statements.
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_TABLESPACE_H
#define PG_TABLESPACE_H

#include "catalog/genbki.h"

/* ----------------
 *        pg_tablespace definition.  cpp turns this into
 *        typedef struct FormData_pg_tablespace
 * ----------------
 */
#define TableSpaceRelationId  1213
#define TableSpaceRelation_Rowtype_Id 11633

CATALOG(pg_tablespace,1213) BKI_SHARED_RELATION BKI_SCHEMA_MACRO
{
    NameData  spcname;           /* tablespace name */
    Oid       spcowner;          /* owner of tablespace */

#ifdef CATALOG_VARLEN            /* variable-length fields start here */
    aclitem   spcacl[1];         /* access permissions */
    text      spcoptions[1];     /* per-tablespace options */
    text      spcmaxsize;        /* tablespace max size */
    bool      relative;          /* relative location */
#endif
} FormData_pg_tablespace;

/* ----------------
 *        Form_pg_tablespace corresponds to a pointer to a tuple with
 *        the format of pg_tablespace relation.
 * ----------------
 */
typedef FormData_pg_tablespace *Form_pg_tablespace;

/* ----------------
 *        compiler constants for pg_tablespace
 * ----------------
 */
#define Natts_pg_tablespace                6
#define Anum_pg_tablespace_spcname         1
#define Anum_pg_tablespace_spcowner        2
#define Anum_pg_tablespace_spcacl          3
#define Anum_pg_tablespace_spcoptions      4
#define Anum_pg_tablespace_maxsize         5
#define Anum_pg_tablespace_relative        6


DATA(insert OID = 1663 (pg_default PGUID _null_ _null_ _null_ f));
DATA(insert OID = 1664 (pg_global  PGUID _null_ _null_ _null_ f));

#define DEFAULTTABLESPACE_OID 1663
#define GLOBALTABLESPACE_OID 1664

#define MAX_TABLESPACE_LIMITED_STRING_LEN 21

#define TABLESPACE_OPTION_FILESYSTEM "filesystem"
#define TABLESPACE_OPTION_ADDRESS "address"
#define TABLESPACE_OPTION_CFGPATH "cfgpath"
#define TABLESPACE_OPTION_STOREPATH "storepath"
#define TABLESPACE_OPTION_SEQ_PAGE_COST  "seq_page_cost"
#define TABLESPACE_OPTION_RANDOM_PAGE_COST "random_page_cost"

#define DFS_TABLESPACE_SUBDIR "tablespace_secondary"
#define DFS_TABLESPACE_TEMPDIR_SUFFIX "pgsql_tmp"


#endif   /* PG_TABLESPACE_H */

