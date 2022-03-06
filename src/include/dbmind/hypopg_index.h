/* -------------------------------------------------------------------------
 *
 * hypopg_index.h: Implementation of hypothetical indexes for openGauss
 *
 * This file contains all includes for the internal code related to
 * hypothetical indexes support.
 *
 * This program is open source, licensed under the PostgreSQL license.
 * For license terms, see the LICENSE file.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Copyright (C) 2015-2018: Julien Rouhaud
 *
 *
 * IDENTIFICATION
 * 	  src/include/dbmind/hypopg_index.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef _HYPOPG_INDEX_H_
#define _HYPOPG_INDEX_H_

/* --------------------------------------------------------
 * Hypothetical index storage, pretty much an IndexOptInfo
 * Some dynamic informations such as pages and lines are not stored but
 * computed when the hypothetical index is used.
 */
typedef struct hypoIndex {
    Oid oid;           /* hypothetical index unique identifier */
    Oid relid;         /* related relation Oid */
    Oid reltablespace; /* tablespace of the index, if set */
    char *indexname;   /* hypothetical index name */

    BlockNumber pages; /* number of estimated disk pages for the
                        * index */
    double tuples;     /* number of estimated tuples in the index */

    /* index descriptor informations */
    int ncolumns;         /* number of columns, only 1 for now */
    int nkeycolumns;      /* number of key columns */
    short int *indexkeys; /* attnums */
    Oid *indexcollations; /* OIDs of collations of index columns */
    Oid *opfamily;        /* OIDs of operator families for columns */
    Oid *opclass;         /* OIDs of opclass data types */
    Oid *opcintype;       /* OIDs of opclass declared input data types */
    Oid *sortopfamily;    /* OIDs of btree opfamilies, if orderable */
    bool *reverse_sort;   /* is sort order descending? */
    bool *nulls_first;    /* do NULLs come first in the sort order? */
    Oid relam;            /* OID of the access method (in pg_am) */

    RegProcedure amcostestimate; /* OID of the access method's cost fcn */
    RegProcedure amcanreturn;    /* OID of the access method's canreturn fcn */

    List *indexprs; /* expressions for non-simple index columns */
    List *indpred;  /* predicate if a partial index, else NIL */

    bool predOK;    /* true if predicate matches query */
    bool unique;    /* true if a unique index */
    bool immediate; /* is uniqueness enforced immediately? */
    bool canreturn; /* can index return IndexTuples? */

    bool amcanorderbyop; /* does AM support order by operator result? */
    bool amoptionalkey;  /* can query omit key for the first column? */
    bool amsearcharray;  /* can AM handle ScalarArrayOpExpr quals? */
    bool amsearchnulls;  /* can AM search for NULL/NOT NULL entries? */
    bool amhasgettuple;  /* does AM have amgettuple interface? */
    bool amhasgetbitmap; /* does AM have amgetbitmap interface? */
    bool amcanunique;    /* does AM support UNIQUE indexes? */
    bool amcanmulticol;  /* does AM support multi-column indexes? */

    bool isGlobal;   /* true if index is global partition index */
    bool ispartitionedindex;  /* it is an partitioned index */
    /* store some informations usually saved in catalogs */
    List *options;   /* WITH clause options: a list of DefElem */
    bool amcanorder; /* does AM support order by column value? */
} hypoIndex;

void InitHypopg();
void hypopg_register_hook();

#endif