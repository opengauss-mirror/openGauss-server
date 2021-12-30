/* -------------------------------------------------------------------------
 *
 * pg_class.h
 *	  definition of the system "relation" relation (pg_class)
 *	  along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_class.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_CLASS_H
#define PG_CLASS_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_class definition.  cpp turns this into
 *		typedef struct FormData_pg_class
 * ----------------
 */
#define RelationRelationId 1259
#define RelationRelation_Rowtype_Id 83

CATALOG(pg_class,1259) BKI_BOOTSTRAP BKI_ROWTYPE_OID(83) BKI_SCHEMA_MACRO
{
    NameData relname; /* class name */
    Oid relnamespace; /* OID of namespace containing this class */
    Oid reltype;      /* OID of entry in pg_type for table's
                       * implicit row type */
    Oid reloftype;    /* OID of entry in pg_type for underlying
                       * composite type */
    Oid relowner;     /* class owner */
    Oid relam;        /* index access method; 0 if not an index */
    Oid relfilenode;  /* identifier of physical storage file */

    /* relfilenode == 0 means it is a "mapped" relation, see relmapper.c */
    Oid reltablespace;   /* identifier of table space for relation */
    float8 relpages;     /* # of blocks (not always up-to-date) */
    float8 reltuples;    /* # of tuples (not always up-to-date) */
    int4 relallvisible;  /* # of all-visible blocks (not always
                          * up-to-date) */
    Oid reltoastrelid;   /* OID of toast table; 0 if none */
    Oid reltoastidxid;   /* if toast table, OID of chunk_id index */
    Oid reldeltarelid;   /* if ColStore table, it is not 0 */
    Oid reldeltaidx;
    Oid relcudescrelid;  /* if ColStore table, it is not 0, if TsStore, it is partition oid */
    Oid relcudescidx;
    bool relhasindex;    /* T if has (or has had) any indexes */
    bool relisshared;    /* T if shared across databases */
    char relpersistence; /* see RELPERSISTENCE_xxx constants below */
    char relkind;        /* see RELKIND_xxx constants below */
    int2 relnatts;       /* number of user attributes */

    /*
     * Class pg_attribute must contain exactly "relnatts" user attributes
     * (with attnums ranging from 1 to relnatts) for this class.  It may also
     * contain entries with negative attnums for system attributes.
     */
    int2 relchecks;                  /* # of CHECK constraints for class */
    bool relhasoids;                 /* T if we generate OIDs for rows of rel */
    bool relhaspkey;                 /* has (or has had) PRIMARY KEY index */
    bool relhasrules;                /* has (or has had) any rules */
    bool relhastriggers;             /* has (or has had) any TRIGGERs */
    bool relhassubclass;             /* has (or has had) derived classes */
    int1 relcmprs;                   /* row compression attribution */
    bool relhasclusterkey;           /* has (or has had) any PARTIAL CLUSTER KEY */
    bool relrowmovement;             /* enable or disable rowmovement */
    char parttype;                   /* 'p' for  partitioned relation, 'n' for non-partitioned relation */
    ShortTransactionId relfrozenxid; /* all Xids < this are frozen in this rel */
#ifdef CATALOG_VARLEN                /* variable-length fields start here */
    /* NOTE: These fields are not present in a relcache entry's rd_rel field. */
    aclitem relacl[1];  /* access permissions */
    text reloptions[1]; /* access-method-specific options */
#endif
    char relreplident;            /* see REPLICA_IDENTITY_xxx constants  */
    TransactionId relfrozenxid64; /* all Xids < this are frozen in this rel */
    Oid relbucket;                /* bucket info in pg_hashbucket */
    int2vector relbucketkey;      /* Column number of hash partition */
#ifdef CATALOG_VARLEN
    TransactionId relminmxid;     /* all multixacts in this rel are >= this.
                                   * this is really a MultiXactId */
#endif
}
FormData_pg_class;

/* Size of fixed part of pg_class tuples, not counting var-length fields */
#define CLASS_TUPLE_SIZE (offsetof(FormData_pg_class, relfrozenxid) + sizeof(ShortTransactionId))

/* ----------------
 *		Form_pg_class corresponds to a pointer to a tuple with
 *		the format of pg_class relation.
 * ----------------
 */
typedef FormData_pg_class* Form_pg_class;

/* ----------------
 *		compiler constants for pg_class
 * ----------------
 */

#define Natts_pg_class 40
#define Anum_pg_class_relname 1
#define Anum_pg_class_relnamespace 2
#define Anum_pg_class_reltype 3
#define Anum_pg_class_reloftype 4
#define Anum_pg_class_relowner 5
#define Anum_pg_class_relam 6
#define Anum_pg_class_relfilenode 7
#define Anum_pg_class_reltablespace 8
#define Anum_pg_class_relpages 9
#define Anum_pg_class_reltuples 10
#define Anum_pg_class_relallvisible 11
#define Anum_pg_class_reltoastrelid 12
#define Anum_pg_class_reltoastidxid 13
#define Anum_pg_class_reldeltarelid 14
#define Anum_pg_class_reldeltaidx 15
#define Anum_pg_class_relcudescrelid 16
#define Anum_pg_class_relcudescidx 17
#define Anum_pg_class_relhasindex 18
#define Anum_pg_class_relisshared 19
#define Anum_pg_class_relpersistence 20
#define Anum_pg_class_relkind 21
#define Anum_pg_class_relnatts 22
#define Anum_pg_class_relchecks 23
#define Anum_pg_class_relhasoids 24
#define Anum_pg_class_relhaspkey 25
#define Anum_pg_class_relhasrules 26
#define Anum_pg_class_relhastriggers 27
#define Anum_pg_class_relhassubclass 28
#define ANUM_PG_CLASS_RELCMPRS 29
#define Anum_pg_class_relhasclusterkey 30
#define Anum_pg_clsss_relrowmovement 31
#define Anum_pg_class_parttype 32
#define Anum_pg_class_relfrozenxid 33
#define Anum_pg_class_relacl 34
#define Anum_pg_class_reloptions 35
#define Anum_pg_class_relreplident 36
#define Anum_pg_class_relfrozenxid64 37
#define Anum_pg_class_relbucket 38
#define Anum_pg_class_relbucketkey 39
#define Anum_pg_class_relminmxid 40

/* ----------------
 *		initial contents of pg_class
 *
 * NOTE: only "bootstrapped" relations need to be declared here.  Be sure that
 * the OIDs listed here match those given in their CATALOG macros, and that
 * the relnatts values are correct.
 * ----------------
 */

/*
 * Note: "3" in the relfrozenxid and the relfrozenxid64 column stands for FirstNormalTransactionId;
 * similarly, "1" in relminmxid stands for FirstMultiXactId.
 */
DATA(insert OID = 1247 (  pg_type       PGNSP 71 0 PGUID 0 0 0 0 0 0 0 0 0 0 0 0 f f p r 30 0 t f f f f 0 f f n 3 _null_ _null_ n 3 _null_ _null_ 1));
DESCR("");
DATA(insert OID = 1249 (  pg_attribute  PGNSP 75 0 PGUID 0 0 0 0 0 0 0 0 0 0 0 0 f f p r 24 0 f f f f f 0 f f n 3 _null_ _null_ n 3 _null_ _null_ 1));
DESCR("");
DATA(insert OID = 1255 (  pg_proc       PGNSP 81 0 PGUID 0 0 0 0 0 0 0 0 0 0 0 0 f f p r 39 0 t f f f f 0 f f n 3 _null_ _null_ n 3 _null_ _null_ 1));
DESCR("");
DATA(insert OID = 7815 (  gs_package       PGNSP 9745 0 PGUID 0 0 0 0 0 0 0 0 0 0 0 0 f f p r 8 0 t f f f f 0 f f n 3 _null_ _null_ n 3 _null_ _null_ 1));
DESCR("");
DATA(insert OID = 1259 (  pg_class      PGNSP 83 0 PGUID 0 0 0 0 0 0 0 0 0 0 0 0 f f p r 40 0 t f f f f 0 f f n 3 _null_ _null_ n 3 _null_ _null_ 1));
DESCR("");

#define RELKIND_RELATION 'r'                    /* ordinary table */
#define RELKIND_INDEX 'i'                       /* secondary index */
#define RELKIND_GLOBAL_INDEX 'I'                /* GLOBAL partitioned index */
#define RELKIND_SEQUENCE 'S'                    /* sequence object */
#define RELKIND_LARGE_SEQUENCE 'L'              /* large sequence object that support 128-bit integer */
#define RELKIND_TOASTVALUE 't'                  /* for out-of-line values */
#define RELKIND_VIEW 'v'                        /* view */
#define RELKIND_MATVIEW 'm'                     /* materialized view */
#define RELKIND_COMPOSITE_TYPE 'c'              /* composite type */
#define RELKIND_FOREIGN_TABLE 'f'               /* foreign table */
#define RELKIND_STREAM 'e'                      /* stream */
#define RELKIND_CONTQUERY 'o'                   /* contview */
#define PARTTYPE_PARTITIONED_RELATION 'p'       /* partitioned relation */
#define PARTTYPE_SUBPARTITIONED_RELATION 's'       /* subpartitioned relation */
#define PARTTYPE_VALUE_PARTITIONED_RELATION 'v' /* value partitioned relation */
#define PARTTYPE_NON_PARTITIONED_RELATION 'n'   /* non-partitioned relation */
#define RELPERSISTENCE_PERMANENT 'p'            /* regular table */
#define RELPERSISTENCE_UNLOGGED 'u'             /* unlogged permanent table */
#define RELPERSISTENCE_TEMP 't'                 /* temporary table */
#define RELPERSISTENCE_GLOBAL_TEMP 'g'          /* global temporary table */

/* default selection for replica identity (primary key or nothing) */
#define REPLICA_IDENTITY_DEFAULT 'd'
/* no replica identity is logged for this relation */
#define REPLICA_IDENTITY_NOTHING 'n'
/* all columns are loged as replica identity */
#define REPLICA_IDENTITY_FULL 'f'

/*
 * Relation kinds that have physical storage. These relations normally have
 * relfilenode set to non-zero, but it can also be zero if the relation is
 * mapped.
 */
#define RELKIND_HAS_STORAGE(relkind) \
    ((relkind) == RELKIND_RELATION || \
     (relkind) == RELKIND_INDEX || \
     (relkind) == RELKIND_SEQUENCE || \
     (relkind) == RELKIND_TOASTVALUE)

#define RELKIND_IS_SEQUENCE(relkind) \
    ((relkind) == RELKIND_SEQUENCE || (relkind) == RELKIND_LARGE_SEQUENCE)

/*
 * an explicitly chosen candidate key's columns are used as identity;
 * will still be set if the index has been dropped, in that case it
 * has the same meaning as 'd'
 */
#define REPLICA_IDENTITY_INDEX 'i'
#endif /* PG_CLASS_H */
