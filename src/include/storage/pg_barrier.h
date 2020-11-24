/* -------------------------------------------------------------------------
 *
 * pg_barrier.h
 *	  Barriers for synchronizing cooperating processes.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * src/include/storage/pg_barrier.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_BARRIER_H
#define PG_BARRIER_H

/*
 * For the header previously known as "barrier.h", please include
 * "port/atomics.h", which deals with atomics, compiler barriers and memory
 * barriers.
 */

#include "storage/spin.h"
#include "miscadmin.h"
typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
} ConditionVariable;

typedef struct Barrier {
    slock_t mutex;
    int phase;         /* phase counter */
    int participants;  /* the number of participants attached */
    int arrived;       /* the number of participants that have
                        * arrived */
    int elected;       /* highest phase elected */
    bool static_party; /* used only for assertions */
    ConditionVariable condition_variable;
} Barrier;

extern void BarrierInit(Barrier* barrier, int num_workers);
extern bool BarrierArriveAndWaitStatus(Barrier* barrier);
extern bool BarrierArriveAndDetach(Barrier* barrier);
extern int BarrierAttach(Barrier* barrier);
extern bool BarrierDetach(Barrier* barrier);
extern int BarrierPhase(Barrier* barrier);
extern int BarrierParticipants(Barrier* barrier);

extern void ConditionVariableInit(ConditionVariable*);
extern void ConditionVariableBroadcast(ConditionVariable*);

#define BarrierArriveAndWait(a, b) BarrierArriveAndWaitStatus(a)
#endif /* PG_BARRIER_H */
