/* ---------------------------------------------------------------------------------------
 * 
 * pg_statistic_ext.h
 *        definition of the system "extended statistic" relation (pg_statistic_ext)
 *        along with the relation's initial contents.
 * 
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *        src/include/catalog/pg_statistic_ext.h
 *
 * NOTES
 *      the genbki.pl script reads this file and generates .bki
 *      information from the DATA() statements.
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef PG_STATISTIC_EXT_H
#define PG_STATISTIC_EXT_H

#include "catalog/genbki.h"

/* ----------------
 *        pg_statistic_ext definition.  cpp turns this into
 *        typedef struct FormData_pg_statistic_ext
 * ----------------
 */
#define StatisticExtRelationId  3220
#define StatisticExtRelation_Rowtype_Id 11660

CATALOG(pg_statistic_ext,3220) BKI_WITHOUT_OIDS BKI_SCHEMA_MACRO
{
    /* These fields form the unique key for the entry: */
    Oid         starelid;          /* relation containing attribute */
    char        starelkind;        /* 'c': starelid ref pg_class.oid
                                    * 'p': starelid ref pg_partition.oid */
    bool        stainherit;        /* true if inheritance children are included */

    /* the fraction of the column's entries that are NULL: */
    float4        stanullfrac;

    /*
     * stawidth is the average width in bytes of non-null entries.    For
     * fixed-width datatypes this is of course the same as the typlen, but for
     * var-width types it is more useful.  Note that this is the average width
     * of the data as actually stored, post-TOASTing (eg, for a
     * moved-out-of-line value, only the size of the pointer object is
     * counted).  This is the appropriate definition for the primary use of
     * the statistic, which is to estimate sizes of in-memory hash tables of
     * tuples.
     */
    int4        stawidth;

    /* ----------------
     * stadistinct indicates the (approximate) number of distinct non-null
     * data values in the column.  The interpretation is:
     *        0        unknown or not computed
     *        > 0        actual number of distinct values
     *        < 0        negative of multiplier for number of rows
     * The special negative case allows us to cope with columns that are
     * unique (stadistinct = -1) or nearly so (for example, a column in
     * which values appear about twice on the average could be represented
     * by stadistinct = -0.5).    Because the number-of-rows statistic in
     * pg_class may be updated more frequently than pg_statistic_ext is, it's
     * important to be able to describe such situations as a multiple of
     * the number of rows, rather than a fixed number of distinct values.
     * But in other cases a fixed number is correct (eg, a boolean column).
     * ----------------
     */
    float4        stadistinct;
    float4        stadndistinct;

    /* ----------------
     * To allow keeping statistics on different kinds of datatypes,
     * we do not hard-wire any particular meaning for the remaining
     * statistical fields.    Instead, we provide several "slots" in which
     * statistical data can be placed.    Each slot includes:
     *        kind            integer code identifying kind of data (see below)
     *        op                OID of associated operator, if needed
     *        numbers            float4 array (for statistical values)
     *        values            anyarray (for representations of data values)
     * The ID and operator fields are never NULL; they are zeroes in an
     * unused slot.  The numbers and values fields are NULL in an unused
     * slot, and might also be NULL in a used slot if the slot kind has
     * no need for one or the other.
     * ----------------
     */
    int2        stakind1;
    int2        stakind2;
    int2        stakind3;
    int2        stakind4;
    int2        stakind5;

    Oid         staop1;
    Oid         staop2;
    Oid         staop3;
    Oid         staop4;
    Oid         staop5;

    /* variable-length fields start here, but we allow direct access to stakey */
    int2vector  stakey;          /* attribute (column) stats are for */
#ifdef CATALOG_VARLEN            /* variable-length fields start here */
    float4        stanumbers1[1];
    float4        stanumbers2[1];
    float4        stanumbers3[1];
    float4        stanumbers4[1];
    float4        stanumbers5[1];

    /*
     * Values in these arrays are values of the column's data type, or of some
     * related type such as an array element type.    We presently have to cheat
     * quite a bit to allow polymorphic arrays of this kind, but perhaps
     * someday it'll be a less bogus facility.
     */
    anyarray    stavalues1;
    anyarray    stavalues2;
    anyarray    stavalues3;
    anyarray    stavalues4;
    anyarray    stavalues5;
    pg_node_tree staexprs;
#endif
} FormData_pg_statistic_ext;

#define STATISTIC_NUM_SLOTS  5
#define STATISTIC_KIND_TBLSIZE  6
#define STARELKIND_CLASS  'c'
#define STARELKIND_PARTITION  'p'

/* ----------------
 *        Form_pg_statistic_ext corresponds to a pointer to a tuple with
 *        the format of pg_statistic_ext relation.
 * ----------------
 */
typedef FormData_pg_statistic_ext *Form_pg_statistic_ext;

/* ----------------
 *        compiler constants for pg_statistic
 * ----------------
 */
#define Natts_pg_statistic_ext                29
#define Anum_pg_statistic_ext_starelid        1
#define Anum_pg_statistic_ext_starelkind    2
#define Anum_pg_statistic_ext_stainherit    3
#define Anum_pg_statistic_ext_stanullfrac    4
#define Anum_pg_statistic_ext_stawidth        5
#define Anum_pg_statistic_ext_stadistinct    6
#define Anum_pg_statistic_ext_stadndistinct 7
#define Anum_pg_statistic_ext_stakind1        8
#define Anum_pg_statistic_ext_stakind2        9
#define Anum_pg_statistic_ext_stakind3        10
#define Anum_pg_statistic_ext_stakind4        11
#define Anum_pg_statistic_ext_stakind5        12
#define Anum_pg_statistic_ext_staop1        13
#define Anum_pg_statistic_ext_staop2        14
#define Anum_pg_statistic_ext_staop3        15
#define Anum_pg_statistic_ext_staop4        16
#define Anum_pg_statistic_ext_staop5        17
#define Anum_pg_statistic_ext_stakey        18
#define Anum_pg_statistic_ext_stanumbers1    19
#define Anum_pg_statistic_ext_stanumbers2    20
#define Anum_pg_statistic_ext_stanumbers3    21
#define Anum_pg_statistic_ext_stanumbers4    22
#define Anum_pg_statistic_ext_stanumbers5    23
#define Anum_pg_statistic_ext_stavalues1    24
#define Anum_pg_statistic_ext_stavalues2    25
#define Anum_pg_statistic_ext_stavalues3    26
#define Anum_pg_statistic_ext_stavalues4    27
#define Anum_pg_statistic_ext_stavalues5    28
#define Anum_pg_statistic_ext_staexprs      29

/* statistic definition for extended statistic */
/*
 * In a "most common values" slot that contains NULL value staop is the OID
 * of the "=" operator used to decide whether values are the same or not.
 * stavalues contains the K most common values(NULLS contains ) appearing in
 * the column, and stanumbers contains their frequencies (fractions of total
 * row count).  The values shall be ordered in decreasing frequency.  Note that
 * since the arrays are variable-size, K may be chosen by the statistics
 * collector. Values should not appear in MCV unless they have been observed
 * to occur more than once; a unique column will have no MCV slot.
 */
#define STATISTIC_KIND_NULL_MCV 6

#ifndef ENABLE_MULTIPLE_NODES
/*
 * STATISTIC_EXT_DEPENDENCIES represents the functinal dependency between
 * multiple columns in a table. In a "functional dependency" slot, stavalues
 * contains the attribute numbers, and stanumbers contains theirs' functional 
 * dependency degrees which must be in 0~1 strictly.
 */
#define STATISTIC_EXT_DEPENDENCIES 7
#endif /* ENABLE_MULTIPLE_NODES */

/*
 ** A "bayes network" slot describes the joint distribution of columns.
 ** Bayes Network constructs a probability graph according to the given edges.
 ** All edges are constructed according to the function dependency of columns.
 ** A bayes network can describe: lossless distribution, distribution with 
 ** independence assumption, and distribution with conditional independence 
 ** assumption.
 **/
#define STATISTIC_KIND_BAYESNET 8

#define STATISTIC_KIND_MULTICOLUMN 10001

#endif   /* PG_STATISTIC_EXT_H */

