/* ---------------------------------------------------------------------------------------
 * 
 * pg_obsscaninfo.h
 *        definition of the system "obsscaninfo" relation (pg_obsscaninfo)
 *        along with the relation's initial contents.
 
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * 
 * IDENTIFICATION
 *        src/include/catalog/pg_obsscaninfo.h
 *
 * NOTES
 *      the genbki.pl script reads this file and generates .bki
 *      information from the DATA() statements.
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef PG_OBSSCANINFO_H
#define PG_OBSSCANINFO_H

#include "catalog/genbki.h"

/* ----------------
 *        pg_obsscaninfo definition.cpp turns this into
 *        typedef struct FormData_pg_obsscaninfo
 * ----------------
 */
#define timestamptz Datum
#define ObsScanInfoRelationId  5679
#define ObsScanInfoRelation_Rowtype_Id 11644

CATALOG(pg_obsscaninfo,5679) BKI_WITHOUT_OIDS BKI_SCHEMA_MACRO
{
    /* These fields form the unique key for the entry: */
    int8              query_id;
#ifdef CATALOG_VARLEN
    text              user_id;
    text              table_name;
    text              file_type;
#endif
    timestamptz       time_stamp;
    float8            actual_time;
    int8              file_scanned;
    float8            data_size;
#ifdef CATALOG_VARLEN
    text              billing_info;
#endif
} FormData_pg_obsscaninfo;


/* ----------------
 *        Form_pg_obsscaninfo corresponds to a pointer to a tuple with
 *        the format of pg_obsscaninfo relation.
 * ----------------
 */
typedef FormData_pg_obsscaninfo *Form_pg_obsscaninfo;

/* ----------------
 *        compiler constants for pg_obsscaninfo
 * ----------------
 */
#define Natts_pg_obsscaninfo                   9
#define Anum_pg_obsscaninfo_query_id           1
#define Anum_pg_obsscaninfo_user_id            2
#define Anum_pg_obsscaninfo_table_name         3
#define Anum_pg_obsscaninfo_file_type          4
#define Anum_pg_obsscaninfo_time_stamp         5
#define Anum_pg_obsscaninfo_actual_time        6
#define Anum_pg_obsscaninfo_file_scanned       7
#define Anum_pg_obsscaninfo_data_size          8
#define Anum_pg_obsscaninfo_billing_info       9

#endif   /* PG_OBSSCANINFO_H */

