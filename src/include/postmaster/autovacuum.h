/* -------------------------------------------------------------------------
 *
 * autovacuum.h
 *	  header file for integrated autovacuum daemon
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/postmaster/autovacuum.h
 *
 * -------------------------------------------------------------------------
 */
#include "utils/guc.h"

#ifndef AUTOVACUUM_H
#define AUTOVACUUM_H

#include "utils/guc.h"

#ifdef PGXC /* PGXC_DATANODE */
#define IsAutoVacuumAnalyzeWorker() (IsAutoVacuumWorkerProcess() && !(MyProc->vacuumFlags & PROC_IN_VACUUM))

#define UINT32_MASK ((uint64)((1UL << 32) - 1))

extern inline bool is_errmodule_enable(int elevel, ModuleId mod_id);
#define AUTOVAC_LOG(level, format, ...)                                                                      \
    do {                                                                                                     \
        if (module_logging_is_on(MOD_AUTOVAC)) {                                                       \
            ereport(level, (errmodule(MOD_AUTOVAC), errmsg(format, ##__VA_ARGS__), ignore_interrupt(true))); \
        }                                                                                                    \
    } while (0)

#define DEBUG_VACUUM_LOG(relid, nameSpaceOid, level, format, ...)                                            \
    do {                                                                                                     \
        if (module_logging_is_on(MOD_VACUUM) && (!IsSystemNamespace(nameSpaceOid) &&                         \
            !IsToastNamespace(nameSpaceOid) && !(relid < FirstNormalObjectId))) {                           \
            ereport(level, (errmodule(MOD_VACUUM), errmsg(format, ##__VA_ARGS__)));                           \
        }                                                                                                    \
    } while (0)

typedef enum {
    AUTOVACUUM_DO_ANALYZE,        /* just do auto-analyze when autovacuum = true */
    AUTOVACUUM_DO_VACUUM,         /* just do auto-vacuum when autovacuum = true */
    AUTOVACUUM_DO_ANALYZE_VACUUM, /* do auto-vacuum and auto-analyze when autovacuum = true */
    AUTOVACUUM_DO_NONE            /* do neither auto-vacuum or auto-analyze when autovacuum = true */
} AutoVacuumModeType;

#endif

extern const char* AUTO_VACUUM_WORKER;

/* Status inquiry functions */
extern bool AutoVacuumingActive(void);
extern bool IsAutoVacuumLauncherProcess(void);
extern bool IsAutoVacuumWorkerProcess(void);
extern bool IsFromAutoVacWoker(void);
extern AutoVacOpts* extract_autovac_opts(HeapTuple tup, TupleDesc pg_class_desc);

#define IsAnyAutoVacuumProcess() (IsAutoVacuumLauncherProcess() || IsAutoVacuumWorkerProcess())

/* Functions to start autovacuum process, called from postmaster */
extern void autovac_init(void);

/* called from postmaster when a worker could not be forked */
extern void AutoVacWorkerFailed(void);

/* autovacuum cost-delay balancer */
extern void AutoVacuumUpdateDelay(void);

#ifdef EXEC_BACKEND
extern void AutoVacLauncherMain();
extern void AutoVacWorkerMain();
extern void AutovacuumLauncherIAm(void);
#endif

/* shared memory stuff */
extern Size AutoVacuumShmemSize(void);
extern void AutoVacuumShmemInit(void);

extern bool check_autovacuum_coordinators(char** newval, void** extra, GucSource source);
extern void assign_autovacuum_coordinators(const char* newval, void* extra);
extern void relation_support_autoavac(
    HeapTuple tuple, bool* enable_analyze, bool* enable_vacuum, bool* is_internal_relation);

/* how long to keep pgstat data in the launcher, in milliseconds */
#define STATS_READ_DELAY 1000

/* the minimum allowed time between two awakenings of the launcher */
#define MIN_AUTOVAC_SLEEPTIME 100.0 /* milliseconds */

/* struct to keep track of databases in launcher */
typedef struct avl_dbase {
    Oid adl_datid; /* hash key -- must be first */
    TimestampTz adl_next_worker;
    int adl_score;
} avl_dbase;

typedef struct av_toastid_mainid {
    Oid at_toastrelid; /* hash key - must be first */
    Oid at_relid;      /* it is a partiton id if at_parentid is a valid oid, or it is an ordinary table oid */
    Oid at_parentid;   /* parent oid if at_relid is a partitioned table oid */

    /*
     * main table's autovac state
     * 1. if at_relid is an ordinary table, it is at_relid's autovac state
     * 2. if at_relid is a partition, it is partitoned table's autovac state
     */
    bool at_allowvacuum; /* main table is allowed to do autovaccum */
    bool at_dovacuum;    /* main table will do vacuum */
    bool at_doanalyze;   /* main table will do analyze */
    bool at_needfreeze;  /* main table need freeze the old tuple to recycle clog */
    bool at_internal;    /* main table is internal relation */
} av_toastid_mainid;

/* struct to keep track of tables to vacuum and/or analyze, in 1st pass */
typedef struct av_relation {
    Oid ar_relid; /* hash key - must be first */
    bool ar_hasrelopts;
    AutoVacOpts ar_reloptions; /* copy of AutoVacOpts from the main table's
                                * reloptions, or NULL if none */
} av_relation;

/* struct to keep track of tables to vacuum and/or analyze, after rechecking */
typedef struct autovac_table {
    Oid at_relid;
    int at_flags;
    bool at_dovacuum;
    bool at_doanalyze;
    bool at_needfreeze;
    bool at_sharedrel;
    int64 at_freeze_min_age;
    int64 at_freeze_table_age;
    int at_vacuum_cost_delay;
    int at_vacuum_cost_limit;
    char* at_partname;
    char* at_subpartname;
    char* at_relname;
    char* at_nspname;
    char* at_datname;
    bool at_is_toast;
    bool at_gpivacuumed;
} autovac_table;

/* partitioned table's autovac state */
typedef struct at_partitioned_table {
    Oid at_relid;        /* partitioned table's oid, it is hash-key - must be first */
    bool at_allowvacuum; /* partitioned table is allowed to do autovaccum */
    bool at_dovacuum;    /* partitioned table will do vacuum */
    bool at_doanalyze;   /* partitioned table will do analyze */
    bool at_needfreeze;  /* partitioned table need freeze old tuple to recycle clog */
    bool at_gpivacuumed; /* partitioned table has vacuumed global index */
} at_partitioned_table;

/* -------------
 * This struct holds information about a single worker's whereabouts.  We keep
 * an array of these in shared memory, sized according to
 * autovacuum_max_workers.
 *
 * wi_links		entry into free list or running list
 * wi_dboid		OID of the database this worker is supposed to work on
 * wi_tableoid	OID of the table currently being vacuumed, if any
 * wi_parentoid	OID of the paritioned table, if wi_tableoid is a partition id
 * wi_sharedrel	flag indicating whether table is marked relisshared
 * wi_proc		pointer to PGPROC of the running worker, NULL if not started
 * wi_launchtime Time at which this worker was launched
 * wi_cost_*	Vacuum cost-based delay parameters current in this worker
 *
 * All fields are protected by AutovacuumLock, except for wi_tableoid which is
 * protected by AutovacuumScheduleLock (which is read-only for everyone except
 * that worker itself).
 * -------------
 */
typedef struct WorkerInfoData {
    SHM_QUEUE wi_links;
    Oid wi_dboid;
    Oid wi_tableoid;
    Oid wi_parentoid;
    bool wi_ispartition;
    bool wi_sharedrel;
    PGPROC* wi_proc;
    TimestampTz wi_launchtime;
    int wi_cost_delay;
    int wi_cost_limit;
    int wi_cost_limit_base;
} WorkerInfoData;

typedef struct WorkerInfoData* WorkerInfo;

/*
 * Possible signals received by the launcher from remote processes.  These are
 * stored atomically in shared memory so that other processes can set them
 * without locking.
 */
typedef enum {
    AutoVacForkFailed, /* failed trying to start a worker */
    AutoVacRebalance,  /* rebalance the cost limits */
    AutoVacNumSignals  /* must be last */
} AutoVacuumSignal;

/* -------------
 * The main autovacuum shmem struct.  On shared memory we store this main
 * struct and the array of WorkerInfo structs.	This struct keeps:
 *
 * av_signal		set by other processes to indicate various conditions
 * av_launcherpid	the PID of the autovacuum launcher
 * av_freeWorkers	the WorkerInfo freelist
 * av_runningWorkers the WorkerInfo non-free queue
 * av_startingWorker pointer to WorkerInfo currently being started (cleared by
 *					the worker itself as soon as it's up and running)
 *
 * This struct is protected by AutovacuumLock, except for av_signal and parts
 * of the worker list (see above).
 * -------------
 */
typedef struct AutoVacuumShmemStruct {
    sig_atomic_t av_signal[AutoVacNumSignals];
    ThreadId av_launcherpid;
    WorkerInfo av_freeWorkers;
    SHM_QUEUE av_runningWorkers;
    WorkerInfo av_startingWorker;
} AutoVacuumShmemStruct;

#endif /* AUTOVACUUM_H */
