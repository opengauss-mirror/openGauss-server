/* -------------------------------------------------------------------------
 *
 * twophase_rmgr.cpp
 *	  Two-phase-commit resource managers tables
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/transam/twophase_rmgr.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/multixact.h"
#include "access/twophase_rmgr.h"
#include "pgstat.h"
#include "storage/lock/lock.h"
#include "storage/predicate.h"

const TwoPhaseCallback g_twophase_recover_callbacks[TWOPHASE_RM_MAX_ID + 1] = {
    NULL,                          /* END ID */
    lock_twophase_recover,         /* Lock */
    NULL,                          /* pgstat */
    multixact_twophase_recover,    /* MultiXact */
    predicatelock_twophase_recover /* PredicateLock */
};

const TwoPhaseCallback g_twophase_postcommit_callbacks[TWOPHASE_RM_MAX_ID + 1] = {
    NULL,                          /* END ID */
    lock_twophase_postcommit,      /* Lock */
    pgstat_twophase_postcommit,    /* pgstat */
    multixact_twophase_postcommit, /* MultiXact */
    NULL                           /* PredicateLock */
};

const TwoPhaseCallback g_twophase_postabort_callbacks[TWOPHASE_RM_MAX_ID + 1] = {
    NULL,                         /* END ID */
    lock_twophase_postabort,      /* Lock */
    pgstat_twophase_postabort,    /* pgstat */
    multixact_twophase_postabort, /* MultiXact */
    NULL                          /* PredicateLock */
};
