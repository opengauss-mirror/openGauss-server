/* ---------------------------------------------------------------------------------------
 *
 * func_dependency.h
 *        functional dependency statistics declarations
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *        src/include/optimizer/func_dependency.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef ENABLE_MULTIPLE_NODES

#ifndef FUNC_DEPENDENCY_H
#define FUNC_DEPENDENCY_H

#include "commands/vacuum.h"
#include "nodes/relation.h"
#include "utils/sortsupport.h"

/* Min number of attributes for functional dependency statistics */
#define FD_MIN_DIMENSIONS 2

/*
 * Max number of attributes for functional dependency statistics
 *
 * This number can be set to a larger integer, but this will cause the time consumed by ANALYZE to increase
 * exponentially when computing functional dependency statistics.
 */
#define FD_MAX_DIMENSIONS 4 


/* Multivariate functional dependencies */
#define STATS_DEPS_MAGIC 0xB4549A2C /* marks serialized bytea */
#define STATS_DEPS_TYPE_BASIC 1     /* basic dependencies type */

/*
 * Functional dependencies, tracking column-level relationships (values
 * in one column determine values in another one).
 */
typedef struct MVDependency {
    double degree;                                /* degree of validity (0-1) */
    AttrNumber nattributes;                       /* number of attributes */
    AttrNumber attributes[FLEXIBLE_ARRAY_MEMBER]; /* attribute numbers */
} MVDependency;

typedef struct MVDependencies {
    uint32 magic;                              /* magic constant marker */
    uint32 type;                               /* type of MV Dependencies (BASIC) */
    int ndeps;                              /* number of dependencies */
    MVDependency *deps[FLEXIBLE_ARRAY_MEMBER]; /* dependencies */
} MVDependencies;

/* multi-sort */
typedef struct MultiSortSupportData {
    int ndims; /* number of dimensions */
    /* sort support data for each dimension: */
    SortSupportData ssup[FLEXIBLE_ARRAY_MEMBER];
} MultiSortSupportData;

typedef MultiSortSupportData *MultiSortSupport;

typedef struct SortItem {
    Datum *values; /* value array extracted from a tuple */
    bool *isnull;  /* value array extracted from a tuple  */
    int count;
} SortItem;

/* a unified representation of the data the statistics is built on */
typedef struct StatsBuildData {
    int numrows;  /* total rows of data being analyzed */
    int nattnums; /* total number of attributes being analyzed */
    AttrNumber *attnums;   /* attribute number array being analyzed  */
    VacAttrStats *stats;   /* statistics for one attribute */
    Datum **values;        /* value matrix extracted from relation */
    bool **nulls;          /* null matrix extracted from relation */
} StatsBuildData;

extern void analyze_compute_dependencies(Relation onerel, int *slot_idx, const char *tableName,
                                         AnalyzeSampleTableSpecInfo *spec, VacAttrStats *stats);

extern int get_slot_index_dependencies(HeapTuple statstuple, int reqkind);
extern bool get_attmultistatsslot_dependencies(bool dependencies_flag, HeapTuple statstuple, int reqkind,
                                               MVDependencies **dependencies);

extern Selectivity dependencies_clauselist_selectivity(PlannerInfo *root, const List *clauses, int varRelid,
                                                       JoinType jointype, SpecialJoinInfo *sjinfo,
                                                       const RelOptInfo *rel, Bitmapset **estimatedclauses);

extern MVDependencies *statext_dependencies_build(StatsBuildData *data);
extern bytea *statext_dependencies_serialize(MVDependencies *dependencies);
extern MVDependencies *statext_dependencies_deserialize(bytea *data);

extern MultiSortSupport multi_sort_init(int ndims);
extern void multi_sort_add_dimension(MultiSortSupport mss, int sortdim, Oid oper, Oid collation);
extern int multi_sort_compare(const void *arr1, const void *arr2, void *arg);
extern int multi_sort_compare_dim(int dim, const SortItem *arr1, const SortItem *arr2, MultiSortSupport mss);
extern int multi_sort_compare_dims(int start, int end, const SortItem *arr1, const SortItem *arr2,
                                   MultiSortSupport mss);
extern SortItem *build_sorted_items(const StatsBuildData *data, int *nitems, MultiSortSupport mss, int numattrs,
                                    const AttrNumber *attnums);

#endif /* FUNC_DEPENDENCY_H */

#endif /* ENABLE_MULTIPLE_NODES */