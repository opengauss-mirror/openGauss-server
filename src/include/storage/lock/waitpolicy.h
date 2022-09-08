/* -------------------------------------------------------------------------
 *
 * waitpolicy.h
 *	  openGauss low-level lock wait policy
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/lock/waitpolicy.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef _LOCK_WAIT_POLICY_H_
#define _LOCK_WAIT_POLICY_H_

/*
 * This enum controls how to deal with rows being locked by FOR UPDATE/SHARE
 * clauses (i.e., it represents the NOWAIT and SKIP LOCKED options).
 * The ordering here is important, because the highest numerical value takes
 * precedence when a RTE is specified multiple ways.  See applyLockingClause.
 */
typedef enum LockWaitPolicy {
    /* Wait for the lock to become available (default behavior) */
    LockWaitBlock,
    /* Skip rows that can't be locked (SKIP LOCKED) */
    LockWaitSkip,
    /* Raise an error if a row cannot be locked (NOWAIT) */
    LockWaitError
} LockWaitPolicy;

#endif
