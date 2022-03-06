/* -------------------------------------------------------------------------
 *
 * vacuum.h
 *	  header file for postgres vacuum cleaner and statistics analyzer
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/vacuum.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef VACUUM_H
#define VACUUM_H
#include "dfsdesc.h"
#include "access/htup.h"
#include "access/genam.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_type.h"
#include "nodes/parsenodes.h"
#include "storage/buf/buf.h"
#include "storage/cu.h"
#include "storage/lock/lock.h"
#include "utils/pg_rusage.h"
#include "utils/relcache.h"

typedef enum DELETE_STATS_OPTION {
    DELETE_STATS_NONE = 0x00u,
    DELETE_STATS_SINGLE = 0x01u,
    DELETE_STATS_MULTI = 0x02u,
    DELETE_STATS_ALL = 0x03u,
} DELETE_STATS_OPTION;

/* ----------
 * ANALYZE builds one of these structs for each attribute (column) that is
 * to be analyzed.	The struct and subsidiary data are in anl_context,
 * so they live until the end of the ANALYZE operation.
 *
 * The type-specific typanalyze function is passed a pointer to this struct
 * and must return TRUE to continue analysis, FALSE to skip analysis of this
 * column.	In the TRUE case it must set the compute_stats and minrows fields,
 * and can optionally set extra_data to pass additional info to compute_stats.
 * minrows is its request for the minimum number of sample rows to be gathered
 * (but note this request might not be honored, eg if there are fewer rows
 * than that in the table).
 *
 * The compute_stats routine will be called after sample rows have been
 * gathered.  Aside from this struct, it is passed:
 *		fetchfunc: a function for accessing the column values from the
 *				   sample rows
 *		samplerows: the number of sample tuples
 *		totalrows: estimated total number of rows in relation
 * The fetchfunc may be called with rownum running from 0 to samplerows-1.
 * It returns a Datum and an isNull flag.
 *
 * compute_stats should set stats_valid TRUE if it is able to compute
 * any useful statistics.  If it does, the remainder of the struct holds
 * the information to be stored in a pg_statistic row for the column.  Be
 * careful to allocate any pointed-to data in anl_context, which will NOT
 * be CurrentMemoryContext when compute_stats is called.
 *
 * Note: for the moment, all comparisons done for statistical purposes
 * should use the database's default collation (DEFAULT_COLLATION_OID).
 * This might change in some future release.
 * ----------
 */
typedef struct VacAttrStats* VacAttrStatsP;

typedef Datum (*AnalyzeAttrFetchFunc)(VacAttrStatsP stats, int rownum, bool* isNull, Relation rel);

typedef void (*AnalyzeAttrComputeStatsFunc)(
    VacAttrStatsP stats, AnalyzeAttrFetchFunc fetchfunc, int samplerows, double totalrows, Relation rel);

typedef struct VacAttrStats {
    /*
     * These fields are set up by the main ANALYZE code before invoking the
     * type-specific typanalyze function.
     *
     * Note: do not assume that the data being analyzed has the same datatype
     * shown in attr, ie do not trust attr->atttypid, attlen, etc.	This is
     * because some index opclasses store a different type than the underlying
     * column/expression.  Instead use attrtypid, attrtypmod, and attrtype for
     * information about the datatype being fed to the typanalyze function.
     */
    unsigned int num_attrs;
    Form_pg_attribute* attrs;  /* copy of pg_attribute row for columns */
    Oid* attrtypid;            /* type of data being analyzed */
    int32* attrtypmod;         /* typmod of data being analyzed */
    Form_pg_type* attrtype;    /* copy of pg_type row for attrtypid */
    MemoryContext anl_context; /* where to save long-lived data */

    /*
     * These fields must be filled in by the typanalyze routine, unless it
     * returns FALSE.
     */
    AnalyzeAttrComputeStatsFunc compute_stats; /* function pointer */
    int minrows;                               /* Minimum # of rows wanted for stats */
    void* extra_data;                          /* for extra type-specific data */

    /*
     * These fields are to be filled in by the compute_stats routine. (They
     * are initialized to zero when the struct is created.)
     */
    bool stats_valid;
    float4 stanullfrac;   /* fraction of entries that are NULL */
    int4 stawidth;        /* average width of column values */
    float4 stadistinct;   /* # distinct values */
    float4 stadndistinct; /* # distinct value of dn1*/
    int2 stakind[STATISTIC_NUM_SLOTS];
    Oid staop[STATISTIC_NUM_SLOTS];
    int numnumbers[STATISTIC_NUM_SLOTS];
    float4* stanumbers[STATISTIC_NUM_SLOTS];
    int numvalues[STATISTIC_NUM_SLOTS];
    Datum* stavalues[STATISTIC_NUM_SLOTS];
    bool* stanulls[STATISTIC_NUM_SLOTS];

    /*
     * These fields describe the stavalues[n] element types. They will be
     * initialized to match attrtypid, but a custom typanalyze function might
     * want to store an array of something other than the analyzed column's
     * elements. It should then overwrite these fields.
     */
    Oid statypid[STATISTIC_NUM_SLOTS];
    int2 statyplen[STATISTIC_NUM_SLOTS];
    bool statypbyval[STATISTIC_NUM_SLOTS];
    char statypalign[STATISTIC_NUM_SLOTS];

    /*
     * These fields are private to the main ANALYZE code and should not be
     * looked at by type-specific functions.
     */
    int tupattnum;   /* attribute number within tuples */
    HeapTuple* rows; /* access info for std fetch function */
    TupleDesc tupDesc;
    Datum* exprvals; /* access info for index fetch function */
    bool* exprnulls;
    int rowstride;
} VacAttrStats;

/*
 * flags for vacuum object
 */
typedef enum VacuumFlags {
    VACFLG_SIMPLE_HEAP = 1 << 0,          /* simple heap */
    VACFLG_SIMPLE_BTREE = 1 << 1,         /* no use, btree index on simple heap */
    VACFLG_MAIN_PARTITION = 1 << 2,       /* partitioned table */
    VACFLG_MAIN_PARTITION_BTREE = 1 << 3, /* no use, btree index on partitioned table */
    VACFLG_SUB_PARTITION = 1 << 4,        /* table partition */
    VACFLG_SUB_PARTITION_BTREE = 1 << 5,  /* no use, btree index on table partition */
    VACFLG_TOAST = 1 << 6,                /* no use*/
    VACFLG_TOAST_BTREE = 1 << 7           /* no use*/
} VacuumFlags;

typedef struct vacuum_object {
    Oid tab_oid;    /* object id for a table, index or a partition */
    Oid parent_oid; /* parent object id if it's a partition */

    /*
     * we ues following flag to skip some check
     * 1. for partitioned table , we vacuum all the partitiones when we
     *    vacuum partitioned so we just skip check all partiitons
     * 2. for main table, we vaccum toast table when we vacuum main table
     */
    bool dovacuum;
    bool dovacuum_toast; /* flags for vacuum toast table, do vacuum on toast if true */
    bool doanalyze;
    bool need_freeze;   /* flag to freeze old tuple for recycle clog */
    bool is_internal_relation; /* flag to mark if it is an internal relation */
    bool is_tsdb_deltamerge;    /* flag to mark if it is a tsdb deltamerge task */
    int flags;                 /* flags for vacuum object */
} vacuum_object;

/*
 * This struct is used to store the partitioned table's information in pg_class,
 * after the VACUUM or ANALYZE, update pg_class with it.
 */
typedef struct UpdatePartitionedTableData {
    Oid tabOid;                  /* partitioned table's oid */
    BlockNumber pages;           /* all blocks, including all its partitions */
    double tuples;               /* all tuples, including all its partitions */
    BlockNumber allVisiblePages; /* all visible pages */
    bool hasIndex;               /* true, iff table has index */
    TransactionId frozenXid;     /* frozen Xid */
} UpdatePartitionedTableData;

/* Identify create temp table for attribute or table. */
typedef enum { TempSmpleTblType_Table, TempSmpleTblType_Attrbute } TempSmpleTblType;

/* The stage for datanode send sample to coordinator under debugging. */
typedef enum { DebugStage_Begin, DebugStage_Execute, DebugStage_End } AnalyzeTempTblDebugStage;

/* Each sample of distinct value. */
typedef struct {
    Datum value; /* a sample value */
    int64 count; /* how many duplicate values */
} SampleItem;

/* Mcv list for compute statistic.*/
typedef struct {
    int stattarget; /* how many most common values we should save. */
    int64 rows_mcv; /* sum of rows of all the  most common values */
    int num_mcv;    /* num of mcv for the current saved */
} McvInfo;

/* Histgram list for compute statistic.*/
typedef struct {
    bool is_last_value; /* indentify the value is the last value. */
    int stattarget;     /* how many histgrams we should save. */
    int num_hist;       /* num of histgram for the current saved. */
    int64 rows_hist;    /* sum of rows of all the histgrams. */
    int64 bucketSize;   /* the step length for histogram bound. */
    int64 sum_count;    /* sum of the count for saved histogram value. */
    int64 cur_mcv_idx;
    int64 start_value_count; /* how many duplicate values */
    Datum start_value;       /* a sample value */
    SampleItem* histitem;    /* item of histgram */
} HistgramInfo;

/* The sample info of special attribute for compute statistic */
typedef struct {
    bool is_varwidth;       /* the width of attribute is variable-length type or not. */
    double totalrows;       /* total rows for the table. */
    int64 samplerows;       /* how many sample rows for the table. */
    double ndistinct;       /* # distinct values */
    int64 nmultiple;        /* duplicate num of distinct values more than 1. */
    int64 null_cnt;         /* count of null value */
    int64 nonnull_cnt;      /* count of non-null values for all samples. */
    McvInfo mcv_list;       /* mcv list for compute stats.*/
    HistgramInfo hist_list; /* histgram list for compute stats.*/
    char** v_alias;         /* alias for column v in temp table. */
    VacAttrStats* stats;    /* the statistics of attribute for update to pg_staitsitc. */
} AnalyzeSampleTableSpecInfo;

/*
 * data and functions for delta merge
 */
typedef struct {
    StringInfo row_count_sql;
    StringInfo merge_sql;
    StringInfo vacuum_sql;
    uint64 max_row;

    /* original info */
    Oid oid;
    StringInfo relname;
    StringInfo schemaname;
    bool is_hdfs;

} MergeInfo;

#define vacuumRelation(flag) (((flag)&VACFLG_SIMPLE_HEAP) == VACFLG_SIMPLE_HEAP)

#define vacuumMainPartition(flag) (((flag)&VACFLG_MAIN_PARTITION) == VACFLG_MAIN_PARTITION)

#define vacuumPartition(flag) (((flag)&VACFLG_SUB_PARTITION) == VACFLG_SUB_PARTITION)

#define hdfsVcuumAction(flag) (((flag)&VACOPT_HDFSDIRECTORY) || ((flag)&VACOPT_COMPACT) || ((flag)&VACOPT_MERGE))

/* We need estimate total rows on datanode only sample rate is -1. */
#define NEED_EST_TOTAL_ROWS_DN(vacstmt) \
    (IS_PGXC_DATANODE && IsConnFromCoord() && (vacstmt)->pstGlobalStatEx[(vacstmt)->tableidx].sampleRate < 0)

/*
 * remote analyze user-defined table
 * 1. for system catalog, do local analyze
 * 2. for user-define table, local coordinator broadcast statistics
 */
#define udtRemoteAnalyze(relid) (FirstNormalObjectId < (relid) && IS_PGXC_COORDINATOR && IsConnFromCoord())

typedef bool (*EqualFunc)(const void*, const void*);

typedef struct VacItemPointerData {
    ItemPointerData itemPointerData;
    int2 bktId;
} VacItemPointerData;

typedef VacItemPointerData* VacItemPointer;

#define VacItemPtrDataGetItemPtr(vacItemPtrData) (&((vacItemPtrData).itemPointerData))
#define VacItemPtrGetItemPtr(vacItemPtr) (&((vacItemPtr)->itemPointerData))

typedef struct LVRelStats {
    /* hasindex = true means two-pass strategy; false means one-pass */
    bool hasindex;
    /* Overall statistics about rel */
    BlockNumber old_rel_pages; /* previous value of pg_class.relpages */
    BlockNumber rel_pages;     /* total number of pages */
    BlockNumber scanned_pages; /* number of pages we examined */
    BlockNumber tupcount_pages; /* pages whose tuples we counted */
    BlockNumber new_visible_pages; /* number of pages marked all visible */
    double old_live_tuples;        /* previous value of pg_class.reltuples */
    double scanned_tuples;     /* counts only tuples on scanned pages */
    double old_rel_tuples;     /* previous value of pg_class.reltuples */
    double new_rel_tuples;     /* new estimated total # of tuples */
    double          new_dead_tuples;        /* new estimated total # of dead tuples */
    BlockNumber pages_removed;
    double tuples_deleted;
    BlockNumber nonempty_pages; /* actually, last nonempty page + 1 */
    /* List of TIDs of tuples we intend to delete */
    /* NB: this list is ordered by TID address */
    int num_dead_tuples;     /* current # of entries */
    int max_dead_tuples;     /* # slots allocated in array */
    VacItemPointer dead_tuples; /* array of ItemPointerData */
    int curr_heap_start;
    int num_index_scans;
    TransactionId latestRemovedXid;
    bool lock_waiter_detected;
    BlockNumber* new_idx_pages;
    double* new_idx_tuples;
    bool* idx_estimated;
    Oid currVacuumPartOid;    /* current lazy vacuum partition oid */
    int2 currVacuumBktId;     /* current lazy vacuum bucket id */
    oidvector* bucketlist;
    bool hasKeepInvisbleTuples;
} LVRelStats;

typedef struct VacRelPrintStats {
    double tupsVacuumed;
    double numTuples;
    BlockNumber scannedPages;
    BlockNumber nblocks;
    double nkeep;
    double nunused;
    BlockNumber emptyPages;
    PGRUsage ruVac;
} VACRelPrintStats;

/* GUC parameters */
extern THR_LOCAL PGDLLIMPORT int default_statistics_target; /* PGDLLIMPORT for
                                                             * PostGIS */

#define DEBUG_START_TIMER                       \
    struct timeval stStartTime;                 \
    struct timeval stStopTime;                  \
    double dTotalElapsTime;                     \
    if (log_min_messages <= DEBUG1) {           \
        (void)gettimeofday(&stStartTime, NULL); \
        dTotalElapsTime = 0;                    \
    }

#define DEBUG_RESET_TIMER                           \
    do {                                            \
        dTotalElapsTime = 0;                        \
        if (log_min_messages <= DEBUG1)             \
            (void)gettimeofday(&stStartTime, NULL); \
    } while (0)

#define DEBUG_STOP_TIMER(fmt, ...)                                                                                \
    do {                                                                                                          \
        if (log_min_messages <= DEBUG1) {                                                                         \
            StringInfoData str1;                                                                                  \
            initStringInfo(&str1);                                                                                \
            (void)gettimeofday(&stStopTime, NULL);                                                                \
            dTotalElapsTime =                                                                                     \
                (stStopTime.tv_sec - stStartTime.tv_sec) + (stStopTime.tv_usec - stStartTime.tv_usec) * 0.000001; \
            appendStringInfo(&str1, fmt, ##__VA_ARGS__);                                                          \
            elog(DEBUG1,                                                                                          \
                "%s for queryid[%lu]: %s  --- elapse time: [%9.3lfs] \n",                                         \
                g_instance.attr.attr_common.PGXCNodeName,                                                         \
                u_sess->debug_query_id,                                                                           \
                str1.data,                                                                                        \
                dTotalElapsTime);                                                                                 \
            pfree(str1.data);                                                                                     \
        }                                                                                                         \
    } while (0)

#define allow_debug(MOD) (log_min_messages <= DEBUG2 && module_logging_is_on(MOD))

#define DEBUG_MOD_START_TIMER(MOD)              \
    struct timeval stStartTime;                 \
    double dTotalElapsTime;                     \
    if (allow_debug(MOD)) {                     \
        (void)gettimeofday(&stStartTime, NULL); \
        dTotalElapsTime = 0;                    \
    }

#define DEBUG_MOD_STOP_TIMER(MOD, fmt, ...)                                                                       \
    do {                                                                                                          \
        if (allow_debug(MOD)) {                                                                                   \
            StringInfoData str1;                                                                                  \
            initStringInfo(&str1);                                                                                \
            struct timeval stStopTime;                                                                            \
            (void)gettimeofday(&stStopTime, NULL);                                                                \
            dTotalElapsTime =                                                                                     \
                (stStopTime.tv_sec - stStartTime.tv_sec) + (stStopTime.tv_usec - stStartTime.tv_usec) * 0.000001; \
            appendStringInfo(&str1, fmt, ##__VA_ARGS__);                                                          \
            ereport(DEBUG2,                                                                                       \
                (errmodule(MOD),                                                                                  \
                    errmsg("%s for queryid[%lu]: %s  --- elapse time: [%9.6lfs] \n",                              \
                        g_instance.attr.attr_common.PGXCNodeName,                                                 \
                        u_sess->debug_query_id,                                                                   \
                        str1.data,                                                                                \
                        dTotalElapsTime)));                                                                       \
            dTotalElapsTime = 0;                                                                                  \
            (void)gettimeofday(&stStartTime, NULL);                                                               \
            pfree(str1.data);                                                                                     \
        }                                                                                                         \
    } while (0)

/* Time elapse stats end */

/* in commands/vacuum.c */
extern void vacuum(VacuumStmt* vacstmt, Oid relid, bool do_toast, BufferAccessStrategy bstrategy, bool isTopLevel);
extern void vac_open_indexes(Relation relation, LOCKMODE lockmode, int* nindexes, Relation** Irel);
extern void vac_close_indexes(int nindexes, Relation* Irel, LOCKMODE lockmode);
extern double vac_estimate_reltuples(
    Relation relation, BlockNumber total_pages, BlockNumber scanned_pages, double scanned_tuples);
extern void vac_update_relstats(Relation relation, Relation classRel, RelPageType num_pages, double num_tuples,
    BlockNumber num_all_visible_pages, bool hasindex, TransactionId frozenxid,
    MultiXactId minmulti = InvalidMultiXactId);
extern void vacuum_set_xid_limits(Relation rel, int64 freeze_min_age, int64 freeze_table_age, TransactionId* oldestXmin,
    TransactionId* freezeLimit, TransactionId* freezeTableLimit, MultiXactId* multiXactFrzLimit);
extern void vac_update_datfrozenxid(void);
extern void vacuum_delay_point(void);

/* in commands/vacuumlazy.c */
extern void lazy_vacuum_rel(Relation onerel, VacuumStmt* vacstmt, BufferAccessStrategy bstrategy);

/* in commands/analyze.c */
extern void analyze_rel(Oid relid, VacuumStmt* vacstmt, BufferAccessStrategy bstrategy);
extern char* buildTempSampleTable(Oid relid, Oid mian_relid, TempSmpleTblType type,
    AnalyzeMode analyzemode = ANALYZENORMAL, bool inh = false, VacuumStmt* vacstmt = NULL,
    AnalyzeSampleTableSpecInfo* spec = NULL);
extern void dropSampleTable(const char* tableName);
extern const char* get_sample_tblname(AnalyzeMode analyzemode, List* tmpSampleTblNameList);
extern VacAttrStats* examine_attribute(Relation onerel, Bitmapset* bms_attnums, bool isLog);
extern void update_attstats(
    Oid relid, char relkind, bool inh, int natts, VacAttrStats** vacattrstats, char relpersistence);
extern void update_attstats(Oid relid, char relkind, bool inh, int natts, VacAttrStats** vacattrstats);
/* we should delete all records in pg_statistic when the data is dirty and current totalrows is null. */
extern void delete_attstats(Oid relid, char relkind, bool inh, int natts, VacAttrStats** vacattrstats,
    unsigned int delete_stats_option = DELETE_STATS_SINGLE);

/* get one relation by relid before do analyze */
extern Relation analyze_get_relation(Oid relid, VacuumStmt* vacstmt);
extern bool std_typanalyze(VacAttrStats* stats);
extern double anl_random_fract(void);
extern double anl_init_selection_state(int n);
extern double anl_get_next_S(double t, int n, double* stateptr);
extern int compute_sample_size(
    VacuumStmt* vacstmt, int num_samples, bool** require_samp, Oid relid = 0, int tableidx = 0);
extern void set_complex_sample(VacuumStmt* pStmt);
extern void delete_attstats_replication(Oid relid, VacuumStmt* stmt);
extern int compute_attr_target(Form_pg_attribute attr);

extern void vac_update_partstats(Partition part, BlockNumber num_pages, double num_tuples,
    BlockNumber num_all_visible_pages, TransactionId frozenxid, MultiXactId minmulti = InvalidMultiXactId);
extern void vac_open_part_indexes(VacuumStmt* vacstmt, LOCKMODE lockmode, int* nindexes, int* nindexesGlobal,
    Relation** Irel, Relation** indexrel, Partition** indexpart);
extern void vac_close_part_indexes(
    int nindexes, int nindexesGlobal, Relation* Irel, Relation* indexrel, Partition* indexpart, LOCKMODE lockmode);
extern void vac_update_pgclass_partitioned_table(Relation partitionRel, bool hasIndex, TransactionId newFrozenXid,
                                                 MultiXactId newMultiXid);

extern void CStoreVacUpdateNormalRelStats(Oid relid, TransactionId frozenxid, Relation pgclassRel);
extern void CStoreVacUpdatePartitionRelStats(Relation partitionRel, TransactionId newFrozenXid);
extern void CStoreVacUpdatePartitionStats(Oid relid, TransactionId frozenxid);
extern void CalculatePartitionedRelStats(_in_ Relation partitionRel, _in_ Relation pgPartitionRel,
    _out_ BlockNumber* totalPages, _out_ BlockNumber* totalVisiblePages, _out_ double* totalTuples,
    _out_ TransactionId* minFrozenXid, _out_ MultiXactId* minMultiXid);

extern bool IsToastRelationbyOid(Oid relid);
extern Oid pg_toast_get_baseid(Oid relOid, bool* isPartToast);
extern void elogVacuumInfo(Relation rel, HeapTuple tuple, char* funcName, TransactionId oldestxmin);

typedef Datum (*GetValFunc[2])(CU* cuPtr, int rowIdx);
extern void InitGetValFunc(int attlen, GetValFunc* getValFuncPtr, int col);

extern void DfsVacuumFull(Oid relid, VacuumStmt* vacstmt);
extern void RemoveGarbageFiles(Relation rel, DFSDescHandler* handler);

extern bool equal_string(const void* _str1, const void* _str2);
extern List* GetDifference(const List* list1, const List* list2, EqualFunc fn);

extern void merge_one_relation(void* _info);
extern void merge_cu_relation(void* info, VacuumStmt* stmt);

extern List* get_rel_oids(Oid relid, VacuumStmt* vacstmt);

extern char* get_nsp_relname(Oid relid);

// obs foreign table options totalrows
extern void updateTotalRows(Oid relid, double n);

extern void analyze_concurrency_process(Oid relid, int16 attnum, MemoryContext oldcontext, const char* funcname);

extern int GetOneTupleSize(VacuumStmt* stmt, Relation rel);

extern void lazy_vacuum_index(Relation indrel,
                                  IndexBulkDeleteResult **stats,
                                  const LVRelStats *vacrelstats,
                                  BufferAccessStrategy vacStrategy);
extern IndexBulkDeleteResult* lazy_cleanup_index(
    Relation indrel, IndexBulkDeleteResult* stats, LVRelStats* vacrelstats, BufferAccessStrategy vac_strategy);
extern void lazy_record_dead_tuple(LVRelStats *vacrelstats,
                                           ItemPointer itemptr);
extern void vacuum_log_cleanup_info(Relation rel, LVRelStats *vacrelstats);
extern void CBIOpenLocalCrossbucketIndex(Relation onerel, LOCKMODE lockmode, int* nindexes, Relation** iRel);

#endif /* VACUUM_H */
