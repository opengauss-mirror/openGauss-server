/* -------------------------------------------------------------------------
 *
 * pg_collation.h
 *	  definition of the system "collation" relation (pg_collation)
 *	  along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/include/catalog/pg_collation.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_COLLATION_H
#define PG_COLLATION_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_collation definition.  cpp turns this into
 *		typedef struct FormData_pg_collation
 * ----------------
 */
#define CollationRelationId  3456
#define CollationRelation_Rowtype_Id 11656

CATALOG(pg_collation,3456) BKI_SCHEMA_MACRO
{
	NameData	collname;		/* collation name */
	Oid			collnamespace;	/* OID of namespace containing collation */
	Oid			collowner;		/* owner of collation */
	int4		collencoding;	/* encoding for this collation; -1 = "all" */
	NameData	collcollate;	/* LC_COLLATE setting */
	NameData	collctype;		/* LC_CTYPE setting */
#ifdef CATALOG_VARLEN
	text		collpadattr;	/* collation pad attribute */
	bool		collisdef;		/* whether the collation is the default for its collencoding */
#endif
} FormData_pg_collation;

/* ----------------
 *		Form_pg_collation corresponds to a pointer to a row with
 *		the format of pg_collation relation.
 * ----------------
 */
typedef FormData_pg_collation *Form_pg_collation;

/* ----------------
 *		compiler constants for pg_collation
 * ----------------
 */
#define Natts_pg_collation				8
#define Anum_pg_collation_collname		1
#define Anum_pg_collation_collnamespace 2
#define Anum_pg_collation_collowner		3
#define Anum_pg_collation_collencoding	4
#define Anum_pg_collation_collcollate	5
#define Anum_pg_collation_collctype		6
#define Anum_pg_collation_collpadattr   7
#define Anum_pg_collation_collisdef     8

/* ----------------
 *		initial contents of pg_collation
 * ----------------
 */

DATA(insert OID = 100 ( default		PGNSP PGUID -1 "" "" _null_ _null_));
DESCR("database's default collation");
#define DEFAULT_COLLATION_OID	100
DATA(insert OID = 950 ( C			PGNSP PGUID -1 "C" "C" _null_ _null_));
DESCR("standard C collation");
#define C_COLLATION_OID			950
DATA(insert OID = 951 ( POSIX		PGNSP PGUID -1 "POSIX" "POSIX" _null_ _null_));
DESCR("standard POSIX collation");
#define POSIX_COLLATION_OID		951

#define B_FORMAT_COLLATION_INTERVAL 256
/* collation in B format start here. */
#define B_FORMAT_COLLATION_OID_MIN 1024

/* BINARY's start with 1024 */
DATA(insert OID = 1026 (binary	PGNSP PGUID 0 "binary" "binary" "NO PAD" t));
DESCR("binary collation");
#define BINARY_COLLATION_OID		1026
/* GBK's start with 1280 */
/* UTF8's start with 1536 */
DATA(insert OID = 1537 (utf8mb4_general_ci	PGNSP PGUID 7 "utf8mb4_general_ci" "utf8mb4_general_ci" "PAD SPACE" t));
DESCR("utf8mb4_general_ci collation");
#define UTF8MB4_GENERAL_CI_COLLATION_OID		1537
DATA(insert OID = 1538 (utf8mb4_unicode_ci	PGNSP PGUID 7 "utf8mb4_unicode_ci" "utf8mb4_unicode_ci" "PAD SPACE" _null_));
DESCR("utf8mb4_unicode_ci collation");
#define UTF8MB4_UNICODE_CI_COLLATION_OID		1538
DATA(insert OID = 1539 (utf8mb4_bin			PGNSP PGUID 7 "utf8mb4_bin" "utf8mb4_bin" "PAD SPACE" _null_));
DESCR("utf8mb4_bin collation");
#define UTF8MB4_BIN_COLLATION_OID				1539

DATA(insert OID = 1551 (utf8_general_ci	PGNSP PGUID 7 "utf8_general_ci" "utf8_general_ci" "PAD SPACE" _null_));
DESCR("utf8_general_ci collation");
#define UTF8_GENERAL_CI_COLLATION_OID		1551
DATA(insert OID = 1552 (utf8_unicode_ci	PGNSP PGUID 7 "utf8_unicode_ci" "utf8_unicode_ci" "PAD SPACE" _null_));
DESCR("utf8_unicode_ci collation");
#define UTF8_UNICODE_CI_COLLATION_OID		1552
DATA(insert OID = 1553 (utf8_bin	PGNSP PGUID 7 "utf8_bin" "utf8_bin" "PAD SPACE" _null_));
DESCR("utf8_bin collation");
#define UTF8_BIN_COLLATION_OID		1553
/* GB10830's start with 1792 */

#define B_FORMAT_COLLATION_OID_MAX 10000
#define B_FORMAT_COLLATION_STR_LEN 4

#define COLLATION_IN_B_FORMAT(colloid) \
	((colloid) > B_FORMAT_COLLATION_OID_MIN && (colloid) < B_FORMAT_COLLATION_OID_MAX)

#define COLLATION_HAS_INVALID_ENCODING(colloid) \
	((colloid) < B_FORMAT_COLLATION_OID_MIN)

#endif   /* PG_COLLATION_H */
