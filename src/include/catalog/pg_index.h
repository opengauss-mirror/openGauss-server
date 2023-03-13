/* -------------------------------------------------------------------------
 *
 * pg_index.h
 *      definition of the system "index" relation (pg_index)
 *      along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_index.h
 *
 * NOTES
 *      the genbki.pl script reads this file and generates .bki
 *      information from the DATA() statements.
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_INDEX_H
#define PG_INDEX_H

#include "catalog/genbki.h"

/* ----------------
 *        pg_index definition.  cpp turns this into
 *        typedef struct FormData_pg_index.
 * ----------------
 */
#define IndexRelationId  2610
#define IndexRelation_Rowtype_Id 10003

CATALOG(pg_index,2610) BKI_WITHOUT_OIDS BKI_SCHEMA_MACRO
{
    Oid         indexrelid;      /* OID of the index */
    Oid         indrelid;        /* OID of the relation it indexes */
    int2        indnatts;        /* total number of columns in index */
    bool        indisunique;     /* is this a unique index? */
    bool        indisprimary;    /* is this index for primary key? */
    bool        indisexclusion;  /* is this index for exclusion constraint? */
    bool        indimmediate;    /* is uniqueness enforced immediately? */
    bool        indisclustered;  /* is this the index last clustered by? */
    bool        indisusable;     /* is this index useable for insert and select?*/
                                 /*if yes, insert and select should ignore this index*/
    bool        indisvalid;      /* is this index valid for use by queries? */
    bool        indcheckxmin;    /* must we wait for xmin to be old? */
    bool        indisready;      /* is this index ready for inserts? */

    /* variable-length fields start here, but we allow direct access to indkey */
    int2vector  indkey;            /* column numbers of indexed cols, or 0 */

#ifdef CATALOG_VARLEN
    oidvector   indcollation;    /* collation identifiers */
    oidvector   indclass;        /* opclass identifiers */
    int2vector  indoption;      /* per-column flags (AM-specific meanings) */
    pg_node_tree indexprs;        /* expression trees for index attributes that
                                   * are not simple column references; one for
                                   * each zero entry in indkey[] */
    pg_node_tree indpred;         /* expression tree for predicate, if a partial
                                    * index; else NULL */
    bool        indisreplident; /* is this index the identity for replication? */
    int2        indnkeyatts;     /* number of key columns in index */
    bool        indisvisible;    /* is this index visible? */
#endif
} FormData_pg_index;

/* ----------------
 *        Form_pg_index corresponds to a pointer to a tuple with
 *        the format of pg_index relation.
 * ----------------
 */
typedef FormData_pg_index *Form_pg_index;

/* ----------------
 *        compiler constants for pg_index
 * ----------------
 */
#define Natts_pg_index                    21
#define Anum_pg_index_indexrelid          1
#define Anum_pg_index_indrelid            2
#define Anum_pg_index_indnatts            3
#define Anum_pg_index_indisunique         4
#define Anum_pg_index_indisprimary        5
#define Anum_pg_index_indisexclusion      6
#define Anum_pg_index_indimmediate        7
#define Anum_pg_index_indisclustered      8
#define Anum_pg_index_indisusable         9
#define Anum_pg_index_indisvalid          10
#define Anum_pg_index_indcheckxmin        11
#define Anum_pg_index_indisready          12
#define Anum_pg_index_indkey              13
#define Anum_pg_index_indcollation        14
#define Anum_pg_index_indclass            15
#define Anum_pg_index_indoption           16
#define Anum_pg_index_indexprs            17
#define Anum_pg_index_indpred             18
#define Anum_pg_index_indisreplident      19
#define Anum_pg_index_indnkeyatts         20
#define Anum_pg_index_indisvisible        21

/*
 * Index AMs that support ordered scans must support these two indoption
 * bits.  Otherwise, the content of the per-column indoption fields is
 * open for future definition.
 */
#define INDOPTION_DESC           0x0001    /* values are in reverse order */
#define INDOPTION_NULLS_FIRST    0x0002    /* NULLs are first instead of last */

/*
 * Use of these macros is recommended over direct examination of the state
 * flag columns where possible; this allows source code compatibility with
 * the less ugly representation used after 9.2.
 */
#define IndexIsUsable(indexForm) ((indexForm)->indisusable)
#define IndexIsReady(indexForm) ((indexForm)->indisready)
#define IndexIsValid(indexForm) (((indexForm)->indisvalid && (indexForm)->indisready) && ((indexForm)->indisusable))
#define IndexIsLive(indexForm)  (((indexForm)->indisready || !(indexForm)->indisvalid) && ((indexForm)->indisusable))

#endif   /* PG_INDEX_H */

