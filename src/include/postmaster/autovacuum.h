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
        if (is_errmodule_enable(level, MOD_AUTOVAC)) {                                                       \
            ereport(level, (errmodule(MOD_AUTOVAC), errmsg(format, ##__VA_ARGS__), ignore_interrupt(true))); \
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
extern void AutovacuumWorkerIAm(void);
extern void AutovacuumLauncherIAm(void);
#endif

/* shared memory stuff */
extern Size AutoVacuumShmemSize(void);
extern void AutoVacuumShmemInit(void);

extern bool check_autovacuum_coordinators(char** newval, void** extra, GucSource source);
extern void assign_autovacuum_coordinators(const char* newval, void* extra);
extern void relation_support_autoavac(
    HeapTuple tuple, bool* enable_analyze, bool* enable_vacuum, bool* is_internal_relation);

#endif /* AUTOVACUUM_H */
