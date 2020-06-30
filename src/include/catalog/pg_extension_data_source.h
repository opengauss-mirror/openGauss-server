/* ---------------------------------------------------------------------------------------
 * 
 * pg_extension_data_source.h
 *        definition of the system "data source" relation (pg_extension_data_source)
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * 
 * 
 * IDENTIFICATION
 *        src/include/catalog/pg_extension_data_source.h
 *
 * NOTES
 *      the genbki.pl script reads this file and generates .bki
 *      information from the DATA() statements.
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef PG_EXTENSION_DATA_SOURCE_H
#define PG_EXTENSION_DATA_SOURCE_H

#include "catalog/genbki.h"

/* ----------------
 *        pg_extension_data_source definition.  cpp turns this into
 *        typedef struct FormData_pg_extension_data_source
 * ----------------
 */
#define DataSourceRelationId 4211
#define DataSourceRelation_Rowtype_Id 7177

CATALOG(pg_extension_data_source,4211) BKI_SHARED_RELATION BKI_ROWTYPE_OID(7177) BKI_SCHEMA_MACRO
{
    NameData    srcname;    /* data source name */
    Oid         srcowner;   /* source owner */

#ifdef CATALOG_VARLEN        /* variable-length fields start here */
    text        srctype;          /* source type */
    text        srcversion;       /* source version */
    aclitem     srcacl[1];        /* access permissions */
    text        srcoptions[1];    /* options */
#endif
} FormData_pg_extension_data_source;

/* ----------------
 *        Form_data_source corresponds to a pointer to a tuple with
 *        the format of pg_extension_data_source relation.
 * ----------------
 */
typedef FormData_pg_extension_data_source *Form_pg_extension_data_source;

/* ----------------
 *        compiler constants for pg_extension_data_source
 * ----------------
 */
#define Natts_pg_extension_data_source                  6
#define Anum_pg_extension_data_source_srcname           1
#define Anum_pg_extension_data_source_srcowner          2
#define Anum_pg_extension_data_source_srctype           3
#define Anum_pg_extension_data_source_srcversion        4
#define Anum_pg_extension_data_source_srcacl            5
#define Anum_pg_extension_data_source_srcoptions        6

#endif   /* PG_EXTENSION_DATA_SOURCE_H */

