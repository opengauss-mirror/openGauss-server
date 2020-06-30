/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * bbox_atomic_arm.h
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/bbox/bbox_atomic_arm.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef BBOX_ATOMIC_ARM_H
#define BBOX_ATOMIC_ARM_H

#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

typedef struct {
    volatile int counter;
} BBOX_ATOMIC_STRU;

extern pthread_mutex_t g_atomic_mutex;

#define BBOX_ATOMIC_INIT(i) \
    {                       \
        (i)                 \
    }

/*
    atomic read the value of variable.
 */
#define BBOX_AtomicRead(v) ((v)->counter)

/*
    atomic add 1 to the value of variable v.
 */
#define BBOX_AtomicSet(v, i) (((v)->counter) = (i))

/*
    atomic add 1 to the value of variable v and return new value.
 */
#define BBOX_AtomicIncReturn(v) BBOX_AtomicAddReturn(1, (v))

/*
    add 1 to the atomic variable v atomically
 */
static __inline__ void BBOX_AtomicAdd(int i, BBOX_ATOMIC_STRU* v)
{
    pthread_mutex_lock(&g_atomic_mutex);
    v->counter += i;
    pthread_mutex_unlock(&g_atomic_mutex);
}

/*
    sub 1 of the atomic variable v atomically
 */
static __inline__ void BBOX_AtomicSub(int i, BBOX_ATOMIC_STRU* v)
{
    pthread_mutex_lock(&g_atomic_mutex);
    v->counter -= i;
    pthread_mutex_unlock(&g_atomic_mutex);
}

/*
    add 1 to the atomic variable atomically
 */
#define BBOX_AtomicInc(v) BBOX_AtomicAdd(1, v)

/*
    sub 1 of the atomic variable atomically
 */
#define BBOX_AtomicDec(v) BBOX_AtomicSub(1, v)

/*
    add 1 to the atomic variable atomically and return the new value.
 */
static __inline__ int BBOX_AtomicAddReturn(int i, BBOX_ATOMIC_STRU* v)
{
    pthread_mutex_lock(&g_atomic_mutex);
    v->counter += i;
    pthread_mutex_unlock(&g_atomic_mutex);

    return v->counter;
}

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif
