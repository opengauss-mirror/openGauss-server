/* -------------------------------------------------------------------------
 *
 * pg_amproc.h
 *	  definition of the system "amproc" relation (pg_amproc)
 *	  along with the relation's initial contents.
 *
 * The amproc table identifies support procedures associated with index
 * operator families and classes.  These procedures can't be listed in pg_amop
 * since they are not the implementation of any indexable operator.
 *
 * The primary key for this table is <amprocfamily, amproclefttype,
 * amprocrighttype, amprocnum>.  The "default" support functions for a
 * particular opclass within the family are those with amproclefttype =
 * amprocrighttype = opclass's opcintype.  These are the ones loaded into the
 * relcache for an index and typically used for internal index operations.
 * Other support functions are typically used to handle cross-type indexable
 * operators with oprleft/oprright matching the entry's amproclefttype and
 * amprocrighttype. The exact behavior depends on the index AM, however, and
 * some don't pay attention to non-default functions at all.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_amproc.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_AMPROC_H
#define PG_AMPROC_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_amproc definition.  cpp turns this into
 *		typedef struct FormData_pg_amproc
 * ----------------
 */
#define AccessMethodProcedureRelationId  2603
#define AccessMethodProcedureRelation_Rowtype_Id 10920

CATALOG(pg_amproc,2603) BKI_SCHEMA_MACRO
{
	Oid			amprocfamily;	/* the index opfamily this entry is for */
	Oid			amproclefttype; /* procedure's left input data type */
	Oid			amprocrighttype;	/* procedure's right input data type */
	int2		amprocnum;		/* support procedure index */
	regproc		amproc;			/* OID of the proc */
} FormData_pg_amproc;

/* ----------------
 *		Form_pg_amproc corresponds to a pointer to a tuple with
 *		the format of pg_amproc relation.
 * ----------------
 */
typedef FormData_pg_amproc *Form_pg_amproc;

/* ----------------
 *		compiler constants for pg_amproc
 * ----------------
 */
#define Natts_pg_amproc					5
#define Anum_pg_amproc_amprocfamily		1
#define Anum_pg_amproc_amproclefttype	2
#define Anum_pg_amproc_amprocrighttype	3
#define Anum_pg_amproc_amprocnum		4
#define Anum_pg_amproc_amproc			5

/* ----------------
 *		initial contents of pg_amproc
 * ----------------
 */

/* btree */
DATA(insert (	397   2277 2277 1 382 ));
DATA(insert (	421   702 702 1 357 ));
DATA(insert (	423   1560 1560 1 1596 ));
DATA(insert (	423   1560 1560 3 4609 ));
DATA(insert (	424   16 16 1 1693 ));
DATA(insert (	424   16 16 3 4609 ));
DATA(insert (	426   1042 1042 1 1078 ));
DATA(insert (	426   1042 1042 2 3256 ));
DATA(insert (	426   1042 1042 3 4608 ));
DATA(insert (	428   17 17 1 1954 ));
DATA(insert (	428   17 17 2 3452 ));
DATA(insert (	428   17 17 3 4609 ));
DATA(insert (	429   18 18 1 358 ));
DATA(insert (	429   18 18 3 4609 ));
DATA(insert (	434   1082 1082 1 1092 ));
DATA(insert (	434   1082 1082 2 3136 ));
DATA(insert (	434   1082 1082 3 4609 ));
DATA(insert (	434   1082 1114 1 2344 ));
DATA(insert (	434   1082 1184 1 2357 ));
DATA(insert (	434   1114 1114 1 2045 ));
DATA(insert (	434   1114 1114 2 3137 ));
DATA(insert (	434   1114 1114 3 4609 ));
DATA(insert (	434   1114 1082 1 2370 ));
DATA(insert (	434   1114 1184 1 2526 ));
DATA(insert (	434   1184 1184 1 1314 ));
DATA(insert (	434   1184 1184 2 3137 ));
DATA(insert (	434   1184 1184 3 4609 ));
DATA(insert (	434   1184 1082 1 2383 ));
DATA(insert (	434   1184 1114 1 2533 ));
DATA(insert (	436   4402 4402 1 4418 ));
DATA(insert (	436   4402 4402 2 3452 ));
DATA(insert (	1970   700 700 1 354 ));
DATA(insert (	1970   700 700 2 3132 ));
DATA(insert (	1970   700 701 1 2194 ));
DATA(insert (	1970   701 701 1 355 ));
DATA(insert (	1970   701 701 2 3133 ));
DATA(insert (	1970   701 700 1 2195 ));
DATA(insert (	1974   869 869 1 926 ));
DATA(insert (	1974   869 869 3 4609 ));
DATA(insert (	1976   21 21 1 350 ));
DATA(insert (	1976   21 21 2 3129 ));
DATA(insert (	1976   21 21 3 4609 ));
DATA(insert (	1976   21 23 1 2190 ));
DATA(insert (	1976   21 20 1 2192 ));
DATA(insert (	1976   23 23 1 351 ));
DATA(insert (	1976   23 23 2 3130 ));
DATA(insert (	1976   23 23 3 4609 ));
DATA(insert (	1976   23 20 1 2188 ));
DATA(insert (	1976   23 21 1 2191 ));
DATA(insert (	1976   20 20 1 842 ));
DATA(insert (	1976   20 20 2 3131 ));
DATA(insert (	1976   20 20 3 4609 ));
DATA(insert (	1976   20 23 1 2189 ));
DATA(insert (	1976   20 21 1 2193 ));
DATA(insert (	1976   3272 3272 1 6544));
DATA(insert (	1976   3272 3272 2 6538));
DATA(insert (	1976   3272 23 1  6545));
DATA(insert (	1976   23 3272 1  6543));
DATA(insert (	1976   3272 20 1  6546));
DATA(insert (	1976   20 3272 1  6547));
DATA(insert (	1976   3272 21 1  6549));
DATA(insert (	1976   21 3272 1  6548));
DATA(insert (	1982   1186 1186 1 1315 ));
DATA(insert (	1982   1186 1186 3 4609 ));
DATA(insert (	1984   829 829 1 836 ));
DATA(insert (	1984   829 829 3 4609 ));
DATA(insert (	1986   19 19 1 359 ));
DATA(insert (	1986   19 19 2 3135 ));
DATA(insert (	1986   19 19 3 4608 ));
DATA(insert (	1988   1700 1700 1 1769 ));
DATA(insert (	1988   1700 1700 2 3283 ));
DATA(insert (	1989   26 26 1 356 ));
DATA(insert (	1989   26 26 2 3134 ));
DATA(insert (	1989   26 26 3 4609 ));
DATA(insert (	1991   30 30 1 404 ));
DATA(insert (	1991   30 30 3 4609 ));
DATA(insert (	2994   2249 2249 1 2987 ));
DATA(insert (	1994   25 25 1 360 ));
DATA(insert (	1994   25 25 2 3255 ));
DATA(insert (	1994   25 25 3 4608 ));
DATA(insert (	1996   1083 1083 1 1107 ));
DATA(insert (	1996   1083 1083 3 4609 ));
DATA(insert (	2000   1266 1266 1 1358 ));
DATA(insert (	2000   1266 1266 3 4609 ));
DATA(insert (	2002   1562 1562 1 1672 ));
DATA(insert (	2002   1562 1562 3 4609 ));
DATA(insert (	2095   25 25 1 2166 ));
DATA(insert (	2095   25 25 3 4609 ));
DATA(insert (	2097   1042 1042 1 2180 ));
DATA(insert (	2097   1042 1042 3 4609 ));
DATA(insert (	2099   790 790 1  377 ));
DATA(insert (	2099   790 790 3  4609 ));
DATA(insert (	2233   703 703 1  380 ));
DATA(insert (	2234   704 704 1  381 ));
DATA(insert (	2789   27 27 1 2794 ));
DATA(insert (	2789   27 27 3 4609 ));
DATA(insert (	2968   2950 2950 1 2960 ));
DATA(insert (	2968   2950 2950 3 4609 ));
DATA(insert (	3522   3500 3500 1 3514 ));

DATA(insert (	3806   86 86 1 3475 ));
DATA(insert (	3807   86 86 1 456 ));
DATA(insert (	4033   3802 3802 1 3417 ));
DATA(insert (	5535   5545 5545 1 5519 ));
DATA(insert (	5536   5545 5545 1 5520 ));

/* hash */
DATA(insert (	427   1042 1042 1 1080 ));
DATA(insert (	431   18 18 1 454 ));
DATA(insert (	435   1082 1082 1 450 ));
DATA(insert (	627   2277 2277 1 626 ));
DATA(insert (	1971   700 700 1 451 ));
DATA(insert (	1971   701 701 1 452 ));
DATA(insert (	1975   869 869 1 422 ));
DATA(insert (	1977   21 21 1 449 ));
DATA(insert (	1977   23 23 1 450 ));
DATA(insert (	1977   20 20 1 949 ));
DATA(insert (	1977   3272 3272 1 3294 ));
DATA(insert (	1983   1186 1186 1 1697 ));
DATA(insert (	1985   829 829 1 399 ));
DATA(insert (	1987   19 19 1 455 ));
DATA(insert (	1990   26 26 1 453 ));
DATA(insert (	1992   30 30 1 457 ));
DATA(insert (	1995   25 25 1 400 ));
DATA(insert (	1995   3272 3272 1 3297 ));
DATA(insert (	1997   1083 1083 1 1688 ));
DATA(insert (	1998   1700 1700 1 432 ));
DATA(insert (	1999   1184 1184 1 2039 ));
DATA(insert (	2001   1266 1266 1 1696 ));
DATA(insert (	2040   1114 1114 1 2039 ));
DATA(insert (	2222   16 16 1 454 ));
DATA(insert (	2223   17 17 1 456 ));
DATA(insert (	4470   4402 4402 1 456 ));
DATA(insert (	2224   22 22 1 398 ));
DATA(insert (	2225   28 28 1 949 ));
DATA(insert (	2226   29 29 1 450 ));
DATA(insert (	2227   702 702 1 450 ));
DATA(insert (	2228   703 703 1 450 ));
DATA(insert (	2229   25 25 1 400 ));
DATA(insert (	2231   1042 1042 1 1080 ));
DATA(insert (	2232   31 31 1 450 ));
DATA(insert (	2235   1033 1033 1 329 ));
DATA(insert (	2969   2950 2950 1 2963 ));
DATA(insert (	3523   3500 3500 1 3515 ));
DATA(insert (	4034   3802 3802 1 3430 ));
DATA(insert (	8646   3272 3272 1 3297 ));

/* gist */
DATA(insert (	2593   603 603 1 2578 ));
DATA(insert (	2593   603 603 2 2583 ));
DATA(insert (	2593   603 603 3 2579 ));
DATA(insert (	2593   603 603 4 2580 ));
DATA(insert (	2593   603 603 5 2581 ));
DATA(insert (	2593   603 603 6 2582 ));
DATA(insert (	2593   603 603 7 2584 ));
DATA(insert (	2594   604 604 1 2585 ));
DATA(insert (	2594   604 604 2 2583 ));
DATA(insert (	2594   604 604 3 2586 ));
DATA(insert (	2594   604 604 4 2580 ));
DATA(insert (	2594   604 604 5 2581 ));
DATA(insert (	2594   604 604 6 2582 ));
DATA(insert (	2594   604 604 7 2584 ));
DATA(insert (	2595   718 718 1 2591 ));
DATA(insert (	2595   718 718 2 2583 ));
DATA(insert (	2595   718 718 3 2592 ));
DATA(insert (	2595   718 718 4 2580 ));
DATA(insert (	2595   718 718 5 2581 ));
DATA(insert (	2595   718 718 6 2582 ));
DATA(insert (	2595   718 718 7 2584 ));
DATA(insert (	3655   3614 3614 1 3654 ));
DATA(insert (	3655   3614 3614 2 3651 ));
DATA(insert (	3655   3614 3614 3 3648 ));
DATA(insert (	3655   3614 3614 4 3649 ));
DATA(insert (	3655   3614 3614 5 3653 ));
DATA(insert (	3655   3614 3614 6 3650 ));
DATA(insert (	3655   3614 3614 7 3652 ));
DATA(insert (	3702   3615 3615 1 3701 ));
DATA(insert (	3702   3615 3615 2 3698 ));
DATA(insert (	3702   3615 3615 3 3695 ));
DATA(insert (	3702   3615 3615 4 3696 ));
DATA(insert (	3702   3615 3615 5 3700 ));
DATA(insert (	3702   3615 3615 6 3697 ));
DATA(insert (	3702   3615 3615 7 3699 ));
DATA(insert (	1029   600 600 1 2179 ));
DATA(insert (	1029   600 600 2 2583 ));
DATA(insert (	1029   600 600 3 1030 ));
DATA(insert (	1029   600 600 4 2580 ));
DATA(insert (	1029   600 600 5 2581 ));
DATA(insert (	1029   600 600 6 2582 ));
DATA(insert (	1029   600 600 7 2584 ));
DATA(insert (	1029   600 600 8 3064 ));


/* gin */
DATA(insert (	2745   1007 1007 1	351 ));
DATA(insert (	2745   1007 1007 2 2743 ));
DATA(insert (	2745   1007 1007 3 2774 ));
DATA(insert (	2745   1007 1007 4 2744 ));
DATA(insert (	2745   1007 1007 6 3920 ));
DATA(insert (	2745   1009 1009 1	360 ));
DATA(insert (	2745   1009 1009 2 2743 ));
DATA(insert (	2745   1009 1009 3 2774 ));
DATA(insert (	2745   1009 1009 4 2744 ));
DATA(insert (	2745   1009 1009 6 3920 ));
DATA(insert (	2745   1015 1015 1	360 ));
DATA(insert (	2745   1015 1015 2 2743 ));
DATA(insert (	2745   1015 1015 3 2774 ));
DATA(insert (	2745   1015 1015 4 2744 ));
DATA(insert (	2745   1015 1015 6 3920 ));
DATA(insert (	2745   1023 1023 1 357 ));
DATA(insert (	2745   1023 1023 2 2743 ));
DATA(insert (	2745   1023 1023 3 2774 ));
DATA(insert (	2745   1023 1023 4 2744 ));
DATA(insert (	2745   1023 1023 6 3920 ));
DATA(insert (	2745   1561 1561 1 1596 ));
DATA(insert (	2745   1561 1561 2 2743 ));
DATA(insert (	2745   1561 1561 3 2774 ));
DATA(insert (	2745   1561 1561 4 2744 ));
DATA(insert (	2745   1561 1561 6 3920 ));
DATA(insert (	2745   1000 1000 1 1693 ));
DATA(insert (	2745   1000 1000 2 2743 ));
DATA(insert (	2745   1000 1000 3 2774 ));
DATA(insert (	2745   1000 1000 4 2744 ));
DATA(insert (	2745   1000 1000 6 3920 ));
DATA(insert (	2745   1014 1014 1 1078 ));
DATA(insert (	2745   1014 1014 2 2743 ));
DATA(insert (	2745   1014 1014 3 2774 ));
DATA(insert (	2745   1014 1014 4 2744 ));
DATA(insert (	2745   1014 1014 6 3920 ));
DATA(insert (	2745   1001 1001 1 1954 ));
DATA(insert (	2745   1001 1001 2 2743 ));
DATA(insert (	2745   1001 1001 3 2774 ));
DATA(insert (	2745   1001 1001 4 2744 ));
DATA(insert (	2745   1001 1001 6 3920 ));
DATA(insert (	2745   1002 1002 1 358 ));
DATA(insert (	2745   1002 1002 2 2743 ));
DATA(insert (	2745   1002 1002 3 2774 ));
DATA(insert (	2745   1002 1002 4 2744 ));
DATA(insert (	2745   1002 1002 6 3920 ));
DATA(insert (	2745   1182 1182 1 1092 ));
DATA(insert (	2745   1182 1182 2 2743 ));
DATA(insert (	2745   1182 1182 3 2774 ));
DATA(insert (	2745   1182 1182 4 2744 ));
DATA(insert (	2745   1182 1182 6 3920 ));
DATA(insert (	2745   1021 1021 1 354 ));
DATA(insert (	2745   1021 1021 2 2743 ));
DATA(insert (	2745   1021 1021 3 2774 ));
DATA(insert (	2745   1021 1021 4 2744 ));
DATA(insert (	2745   1021 1021 6 3920 ));
DATA(insert (	2745   1022 1022 1 355 ));
DATA(insert (	2745   1022 1022 2 2743 ));
DATA(insert (	2745   1022 1022 3 2774 ));
DATA(insert (	2745   1022 1022 4 2744 ));
DATA(insert (	2745   1022 1022 6 3920 ));
DATA(insert (	2745   1041 1041 1 926 ));
DATA(insert (	2745   1041 1041 2 2743 ));
DATA(insert (	2745   1041 1041 3 2774 ));
DATA(insert (	2745   1041 1041 4 2744 ));
DATA(insert (	2745   1041 1041 6 3920 ));
DATA(insert (	2745   651 651 1 926 ));
DATA(insert (	2745   651 651 2 2743 ));
DATA(insert (	2745   651 651 3 2774 ));
DATA(insert (	2745   651 651 4 2744 ));
DATA(insert (	2745   651 651 6 3920 ));
DATA(insert (	2745   1005 1005 1 350 ));
DATA(insert (	2745   1005 1005 2 2743 ));
DATA(insert (	2745   1005 1005 3 2774 ));
DATA(insert (	2745   1005 1005 4 2744 ));
DATA(insert (	2745   1005 1005 6 3920 ));
DATA(insert (	2745   1016 1016 1 842 ));
DATA(insert (	2745   1016 1016 2 2743 ));
DATA(insert (	2745   1016 1016 3 2774 ));
DATA(insert (	2745   1016 1016 4 2744 ));
DATA(insert (	2745   1016 1016 6 3920 ));
DATA(insert (	2745   1187 1187 1 1315 ));
DATA(insert (	2745   1187 1187 2 2743 ));
DATA(insert (	2745   1187 1187 3 2774 ));
DATA(insert (	2745   1187 1187 4 2744 ));
DATA(insert (	2745   1187 1187 6 3920 ));
DATA(insert (	2745   1040 1040 1 836 ));
DATA(insert (	2745   1040 1040 2 2743 ));
DATA(insert (	2745   1040 1040 3 2774 ));
DATA(insert (	2745   1040 1040 4 2744 ));
DATA(insert (	2745   1040 1040 6 3920 ));
DATA(insert (	2745   1003 1003 1 359 ));
DATA(insert (	2745   1003 1003 2 2743 ));
DATA(insert (	2745   1003 1003 3 2774 ));
DATA(insert (	2745   1003 1003 4 2744 ));
DATA(insert (	2745   1003 1003 6 3920 ));
DATA(insert (	2745   1231 1231 1 1769 ));
DATA(insert (	2745   1231 1231 2 2743 ));
DATA(insert (	2745   1231 1231 3 2774 ));
DATA(insert (	2745   1231 1231 4 2744 ));
DATA(insert (	2745   1231 1231 6 3920 ));
DATA(insert (	2745   1028 1028 1 356 ));
DATA(insert (	2745   1028 1028 2 2743 ));
DATA(insert (	2745   1028 1028 3 2774 ));
DATA(insert (	2745   1028 1028 4 2744 ));
DATA(insert (	2745   1028 1028 6 3920 ));
DATA(insert (	2745   1013 1013 1 404 ));
DATA(insert (	2745   1013 1013 2 2743 ));
DATA(insert (	2745   1013 1013 3 2774 ));
DATA(insert (	2745   1013 1013 4 2744 ));
DATA(insert (	2745   1013 1013 6 3920 ));
DATA(insert (	2745   1183 1183 1 1107 ));
DATA(insert (	2745   1183 1183 2 2743 ));
DATA(insert (	2745   1183 1183 3 2774 ));
DATA(insert (	2745   1183 1183 4 2744 ));
DATA(insert (	2745   1183 1183 6 3920 ));
DATA(insert (	2745   1185 1185 1 1314 ));
DATA(insert (	2745   1185 1185 2 2743 ));
DATA(insert (	2745   1185 1185 3 2774 ));
DATA(insert (	2745   1185 1185 4 2744 ));
DATA(insert (	2745   1185 1185 6 3920 ));
DATA(insert (	2745   1270 1270 1 1358 ));
DATA(insert (	2745   1270 1270 2 2743 ));
DATA(insert (	2745   1270 1270 3 2774 ));
DATA(insert (	2745   1270 1270 4 2744 ));
DATA(insert (	2745   1270 1270 6 3920 ));
DATA(insert (	2745   1563 1563 1 1672 ));
DATA(insert (	2745   1563 1563 2 2743 ));
DATA(insert (	2745   1563 1563 3 2774 ));
DATA(insert (	2745   1563 1563 4 2744 ));
DATA(insert (	2745   1563 1563 6 3920 ));
DATA(insert (	2745   1115 1115 1 2045 ));
DATA(insert (	2745   1115 1115 2 2743 ));
DATA(insert (	2745   1115 1115 3 2774 ));
DATA(insert (	2745   1115 1115 4 2744 ));
DATA(insert (	2745   1115 1115 6 3920 ));
DATA(insert (	2745   791 791 1 377 ));
DATA(insert (	2745   791 791 2 2743 ));
DATA(insert (	2745   791 791 3 2774 ));
DATA(insert (	2745   791 791 4 2744 ));
DATA(insert (	2745   791 791 6 3920 ));
DATA(insert (	2745   1024 1024 1 380 ));
DATA(insert (	2745   1024 1024 2 2743 ));
DATA(insert (	2745   1024 1024 3 2774 ));
DATA(insert (	2745   1024 1024 4 2744 ));
DATA(insert (	2745   1024 1024 6 3920 ));
DATA(insert (	2745   1025 1025 1 381 ));
DATA(insert (	2745   1025 1025 2 2743 ));
DATA(insert (	2745   1025 1025 3 2774 ));
DATA(insert (	2745   1025 1025 4 2744 ));
DATA(insert (	2745   1025 1025 6 3920 ));
DATA(insert (	3659   3614 3614 1 3724 ));
DATA(insert (	3659   3614 3614 2 3656 ));
DATA(insert (	3659   3614 3614 3 3657 ));
DATA(insert (	3659   3614 3614 4 3658 ));
DATA(insert (	3659   3614 3614 5 2700 ));
DATA(insert (	3659   3614 3614 6 3921 ));
DATA(insert (	3626   3614 3614 1 3622 ));
DATA(insert (	3683   3615 3615 1 3668 ));
DATA(insert (	3901   3831 3831 1 3870 ));
DATA(insert (	3903   3831 3831 1 3902 ));
DATA(insert (	3919   3831 3831 1 3875 ));
DATA(insert (	3919   3831 3831 2 3876 ));
DATA(insert (	3919   3831 3831 3 3877 ));
DATA(insert (	3919   3831 3831 4 3878 ));
DATA(insert (	3919   3831 3831 5 3879 ));
DATA(insert (	3919   3831 3831 6 3880 ));
DATA(insert (	3919   3831 3831 7 3881 ));
DATA(insert (	4036   3802 3802 1 3498 ));
DATA(insert (	4036   3802 3802 2 3482 ));
DATA(insert (	4036   3802 3802 3 3493 ));
DATA(insert (	4036   3802 3802 4 3497 ));
DATA(insert (	4036   3802 3802 6 3494 ));
DATA(insert (	4037   3802 3802 1 351 ));
DATA(insert (	4037   3802 3802 2 3492 ));
DATA(insert (	4037   3802 3802 3 3486 ));
DATA(insert (	4037   3802 3802 4 3487 ));
DATA(insert (	4037   3802 3802 6 3495 ));
/* cgin */
DATA(insert (	4446   3614 3614 1 3724 ));
DATA(insert (	4446   3614 3614 2 3656 ));
DATA(insert (	4446   3614 3614 3 3657 ));
DATA(insert (	4446   3614 3614 4 3658 ));
DATA(insert (	4446   3614 3614 5 2700 ));
DATA(insert (	4446   3614 3614 6 3921 ));

/* sp-gist */
DATA(insert (	4015   600 600 1 4018 ));
DATA(insert (	4015   600 600 2 4019 ));
DATA(insert (	4015   600 600 3 4020 ));
DATA(insert (	4015   600 600 4 4021 ));
DATA(insert (	4015   600 600 5 4022 ));
DATA(insert (	4016   600 600 1 4023 ));
DATA(insert (	4016   600 600 2 4024 ));
DATA(insert (	4016   600 600 3 4025 ));
DATA(insert (	4016   600 600 4 4026 ));
DATA(insert (	4016   600 600 5 4022 ));
DATA(insert (	4017   25 25 1 4027 ));
DATA(insert (	4017   25 25 2 4028 ));
DATA(insert (	4017   25 25 3 4029 ));
DATA(insert (	4017   25 25 4 4030 ));
DATA(insert (	4017   25 25 5 4031 ));
DATA(insert (	5570   9003 9003 1 5586 ));
DATA(insert (	5571   9003 9003 1 5587 ));

/* psort, fake data just make index work */
DATA(insert (	4050	  21	  21	1	350));
DATA(insert (	4050	  21	  23	1	2190));
DATA(insert (	4050	  21	  20	1	2192));
DATA(insert (	4050	  23	  23	1	351));
DATA(insert (	4050	  23	  20	1	2188));
DATA(insert (	4050	  23	  21	1	2191));
DATA(insert (	4050	  20	  20	1	842));
DATA(insert (	4050	  20	  23	1	2189));
DATA(insert (	4050	  20	  21	1	2193));
DATA(insert (	4051	  26	  26	1	356));
DATA(insert (	4052	1082	1082	1	1092));
DATA(insert (	4052	1082	1114	1	2344));
DATA(insert (	4052	1082	1184	1	2357));
DATA(insert (	4052	1114	1114	1	2045));
DATA(insert (	4052	1114	1082	1	2370));
DATA(insert (	4052	1114	1184	1	2526));
DATA(insert (	4052	1184	1184	1	1314));
DATA(insert (	4052	1184	1082	1	2383));
DATA(insert (	4052	1184	1114	1	2533));
DATA(insert (	4053	 700	 700	1	354));
DATA(insert (	4053	 700	 701	1	2194));
DATA(insert (	4053	 701	 701	1	355));
DATA(insert (	4053	 701	 700	1	2195));
DATA(insert (	4054	1700	1700	1	1769));
DATA(insert (	4055	  25	  25	1	360));
DATA(insert (	4056	1042	1042	1	1078));
DATA(insert (	4057	1083	1083	1	1107));
DATA(insert (	4058	1266	1266	1	1358));
DATA(insert (	4059	 790	 790	1	377));
DATA(insert (	4060	1186	1186	1	1315));
DATA(insert (	4061	 704	 704	1	381));
DATA(insert (	4062	5545	5545	1	5519));
DATA(insert (	4063	  16	  16	1	1693));
DATA(insert (	4064	9003	9003	1	5586));

/* cbtree, fake data just make index work */
DATA(insert (	4250	  21	  21	1	350));
DATA(insert (	4250	  21	  23	1	2190));
DATA(insert (	4250	  21	  20	1	2192));
DATA(insert (	4250	  23	  23	1	351));
DATA(insert (	4250	  23	  20	1	2188));
DATA(insert (	4250	  23	  21	1	2191));
DATA(insert (	4250	  20	  20	1	842));
DATA(insert (	4250	  20	  23	1	2189));
DATA(insert (	4250	  20	  21	1	2193));
DATA(insert (	4251	  26	  26	1	356));
DATA(insert (	4252	1082	1082	1	1092));
DATA(insert (	4252	1082	1114	1	2344));
DATA(insert (	4252	1082	1184	1	2357));
DATA(insert (	4252	1114	1114	1	2045));
DATA(insert (	4252	1114	1082	1	2370));
DATA(insert (	4252	1114	1184	1	2526));
DATA(insert (	4252	1184	1184	1	1314));
DATA(insert (	4252	1184	1082	1	2383));
DATA(insert (	4252	1184	1114	1	2533));
DATA(insert (	4253	 700	 700	1	354));
DATA(insert (	4253	 700	 701	1	2194));
DATA(insert (	4253	 701	 701	1	355));
DATA(insert (	4253	 701	 700	1	2195));
DATA(insert (	4254	1700	1700	1	1769));
DATA(insert (	4255	  25	  25	1	360));
DATA(insert (	4256	1042	1042	1	1078));
DATA(insert (	4257	1083	1083	1	1107));
DATA(insert (	4258	1266	1266	1	1358));
DATA(insert (	4259	 790	 790	1	377));
DATA(insert (	4260	1186	1186	1	1315));
DATA(insert (	4261	 704	 704	1	381));
DATA(insert (	4262	5545	5545	1	5519));
DATA(insert (	4263	  16	  16	1	1693));
DATA(insert (	4264	9003	9003	1	5586));

/* ubtree */
DATA(insert (	5397   2277 2277 1 382 ));
DATA(insert (	5421   702 702 1 357 ));
DATA(insert (	5423   1560 1560 1 1596 ));
DATA(insert (	5423   1560 1560 3 4609 ));
DATA(insert (	5424   16 16 1 1693 ));
DATA(insert (	5424   16 16 3 4609 ));
DATA(insert (	5426   1042 1042 1 1078 ));
DATA(insert (	5426   1042 1042 2 3256 ));
DATA(insert (	5426   1042 1042 3 4608 ));
DATA(insert (	5428   17 17 1 1954 ));
DATA(insert (	5428   17 17 2 3452 ));
DATA(insert (	5428   17 17 3 4609 ));
DATA(insert (	5429   18 18 1 358 ));
DATA(insert (	5429   18 18 3 4609 ));
DATA(insert (	5434   1082 1082 1 1092 ));
DATA(insert (	5434   1082 1082 2 3136 ));
DATA(insert (	5434   1082 1082 3 4609 ));
DATA(insert (	5434   1082 1114 1 2344 ));
DATA(insert (	5434   1082 1184 1 2357 ));
DATA(insert (	5434   1114 1114 1 2045 ));
DATA(insert (	5434   1114 1114 2 3137 ));
DATA(insert (	5434   1114 1114 3 4609 ));
DATA(insert (	5434   1114 1082 1 2370 ));
DATA(insert (	5434   1114 1184 1 2526 ));
DATA(insert (	5434   1184 1184 1 1314 ));
DATA(insert (	5434   1184 1184 2 3137 ));
DATA(insert (	5434   1184 1184 3 4609 ));
DATA(insert (	5434   1184 1082 1 2383 ));
DATA(insert (	5434   1184 1114 1 2533 ));
DATA(insert (	5436   4402 4402 1 4418 ));
DATA(insert (	5436   4402 4402 2 3452 ));
DATA(insert (	6970   700 700 1 354 ));
DATA(insert (	6970   700 700 2 3132 ));
DATA(insert (	6970   700 701 1 2194 ));
DATA(insert (	6970   701 701 1 355 ));
DATA(insert (	6970   701 701 2 3133 ));
DATA(insert (	6970   701 700 1 2195 ));
DATA(insert (	6974   869 869 1 926 ));
DATA(insert (	6974   869 869 3 4609 ));
DATA(insert (	6976   21 21 1 350 ));
DATA(insert (	6976   21 21 2 3129 ));
DATA(insert (	6976   21 21 3 4609 ));
DATA(insert (	6976   21 23 1 2190 ));
DATA(insert (	6976   21 20 1 2192 ));
DATA(insert (	6976   23 23 1 351 ));
DATA(insert (	6976   23 23 2 3130 ));
DATA(insert (	6976   23 23 3 4609 ));
DATA(insert (	6976   23 20 1 2188 ));
DATA(insert (	6976   23 21 1 2191 ));
DATA(insert (	6976   20 20 1 842 ));
DATA(insert (	6976   20 20 2 3131 ));
DATA(insert (	6976   20 20 3 4609 ));
DATA(insert (	6976   20 23 1 2189 ));
DATA(insert (	6976   20 21 1 2193 ));
DATA(insert (	6976   3272 3272 1  6544));
DATA(insert (	6976   3272 3272 2  6538));
DATA(insert (	6976   3272 23 1  6545));
DATA(insert (	6976   23 3272 1  6543));
DATA(insert (	6976   3272 20 1  6546));
DATA(insert (	6976   20 3272 1  6547));
DATA(insert (	6976   3272 21 1  6549));
DATA(insert (	6976   21 3272 1  6548));
DATA(insert (	6982   1186 1186 1 1315 ));
DATA(insert (	6982   1186 1186 3 4609 ));
DATA(insert (	6984   829 829 1 836 ));
DATA(insert (	6984   829 829 3 4609 ));
DATA(insert (	6986   19 19 1 359 ));
DATA(insert (	6986   19 19 2 3135 ));
DATA(insert (	6986   19 19 3 4608 ));
DATA(insert (	6988   1700 1700 1 1769 ));
DATA(insert (	6988   1700 1700 2 3283 ));
DATA(insert (	6989   26 26 1 356 ));
DATA(insert (	6989   26 26 2 3134 ));
DATA(insert (	6989   26 26 3 4609 ));
DATA(insert (	6991   30 30 1 404 ));
DATA(insert (	6991   30 30 3 4609 ));
DATA(insert (	7994   2249 2249 1 2987 ));
DATA(insert (	6994   25 25 1 360 ));
DATA(insert (	6994   25 25 2 3255 ));
DATA(insert (	6994   25 25 3 4608 ));
DATA(insert (	6996   1083 1083 1 1107 ));
DATA(insert (	6996   1083 1083 3 4609 ));
DATA(insert (	7000   1266 1266 1 1358 ));
DATA(insert (	7000   1266 1266 3 4609 ));
DATA(insert (	7002   1562 1562 1 1672 ));
DATA(insert (	7002   1562 1562 3 4609 ));
DATA(insert (	7095   25 25 1 2166 ));
DATA(insert (	7095   25 25 3 4609 ));
DATA(insert (	7097   1042 1042 1 2180 ));
DATA(insert (	7097   1042 1042 3 4609 ));
DATA(insert (	7099   790 790 1  377 ));
DATA(insert (	7099   790 790 3  4609 ));
DATA(insert (	7233   703 703 1  380 ));
DATA(insert (	7234   704 704 1  381 ));
DATA(insert (	7789   27 27 1 2794 ));
DATA(insert (	7789   27 27 3 4609 ));
DATA(insert (	7968   2950 2950 1 2960 ));
DATA(insert (	7968   2950 2950 3 4609 ));
DATA(insert (	8522   3500 3500 1 3514 ));

DATA(insert (	8806   86 86 1 3475 ));
DATA(insert (	9535   5545 5545 1 5519 ));

DATA(insert (	9570   9003 9003 1 5586 ));
DATA(insert (	8901   3831 3831 1 3870 ));
DATA(insert (	8626   3614 3614 1 3622 ));
DATA(insert (	8683   3615 3615 1 3668 ));

DATA(insert OID = 8924 (	8371   8305 8305 1 8431 ));
DATA(insert OID = 8925 (	8372   8305 8305 1 8434 ));
DATA(insert OID = 8926 (	8373   8305 8305 1 8434 ));
DATA(insert OID = 8947 (	8373   8305 8305 2 8438 ));
DATA(insert OID = 8927 (	8374   8305 8305 1 8436 ));

DATA(insert OID = 8932 (	8379   1560 1560 1 8468 ));
DATA(insert OID = 8975 (	8379   1560 1560 3 8209 ));

DATA(insert OID = 8933 (	8380   1560 1560 1 8469 ));
DATA(insert OID = 8976 (	8380   1560 1560 3 8209 ));

DATA(insert OID = 8934 (	8381   8307 8307 1 8470 ));
DATA(insert OID = 8954 (	8381   8307 8307 3 8479 ));

DATA(insert OID = 8935 (	8382   8307 8307 1 8463 ));
DATA(insert OID = 8955 (	8382   8307 8307 3 8479 ));

DATA(insert OID = 8936 (	8383   8307 8307 1 8463 ));
DATA(insert OID = 8956 (	8383   8307 8307 2 8478 ));
DATA(insert OID = 8957 (	8383   8307 8307 3 8479 ));

DATA(insert OID = 8937 (	8384   8307 8307 1 8467 ));
DATA(insert OID = 8958 (	8384   8307 8307 3 8479 ));

DATA(insert OID = 8938 (	8385   8305 8305 1 8431 ));
DATA(insert OID = 8939 (	8385   8305 8305 3 8433 ));

DATA(insert OID = 8940 (	8386   8305 8305 1 8434 ));
DATA(insert OID = 8941 (	8386   8305 8305 3 8432 ));
DATA(insert OID = 8942 (	8386   8305 8305 4 8438 ));

DATA(insert OID = 8943 (	8387   8305 8305 1 8434 ));
DATA(insert OID = 8944 (	8387   8305 8305 2 8438 ));
DATA(insert OID = 8945 (	8387   8305 8305 3 8432 ));
DATA(insert OID = 8946 (	8387   8305 8305 4 8438 ));

DATA(insert OID = 8953 (	8394   1560 1560 1 8469 ));
DATA(insert OID = 8973 (	8394   1560 1560 3 8469 ));
DATA(insert OID = 8974 (	8394   1560 1560 5 8210 ));
DATA(insert OID = 8985 (	8392   8305 8305 1 8450 ));
DATA(insert OID = 8998 (	8397   8307 8307 1 8464 ));

DATA(insert OID = 8970 (	8375   8305 8305 1 8450 ));
DATA(insert OID = 8971 (	8376   8307 8307 1 8464 ));
#endif   /* PG_AMPROC_H */
