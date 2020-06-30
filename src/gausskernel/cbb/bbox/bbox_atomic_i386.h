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
 * bbox_atomic_i386.h
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/bbox/bbox_atomic_i386.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef BBOX_ATOMIC_i386_H
#define BBOX_ATOMIC_i386_H

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

#define _LOCK_ "lock ; "

/*
    define the atomic variable.
 */
typedef struct {
    volatile int counter;
} BBOX_ATOMIC_STRU;

/*
    init the automic variable.
 */
#define BBOX_ATOMIC_INIT(i) \
    {                       \
        (i)                 \
    }

/*
    init value of automic variable v to i.
 */
static __inline__ void BBOX_AtomicCreate(BBOX_ATOMIC_STRU* v, int i)
{
    v->counter = i;
}

/*
    read the atomic variable atomically.
 */
#define BBOX_AtomicRead(v) ((v)->counter)

/*
    set the value of atomic variable to i atomically.
 */
#define BBOX_AtomicSet(v, i) (((v)->counter) = (i))

/*
    add i to the atomic variable atomically and return new value.
 */
#define BBOX_AtomicIncReturn(v) BBOX_AtomicAddReturn(1, (v))

/*
    add i to the atomic variable atomically.
 */
static __inline__ void BBOX_AtomicAdd(int i, BBOX_ATOMIC_STRU* v)
{
    __asm__ __volatile__(_LOCK_ "addl %1,%0" : "=m"(v->counter) : "ir"(i), "m"(v->counter));
}

/*
    sub i of the value of atomic variable v atomically.
 */
static __inline__ void BBOX_AtomicSub(int i, BBOX_ATOMIC_STRU* v)
{
    __asm__ __volatile__(_LOCK_ "subl %1,%0" : "=m"(v->counter) : "ir"(i), "m"(v->counter));
}

/*
    add 1 to the atomic variable atomically.
 */
static __inline__ void BBOX_AtomicInc(BBOX_ATOMIC_STRU* v)
{
    __asm__ __volatile__(_LOCK_ "incl %0" : "=m"(v->counter) : "m"(v->counter));
}

/*
    sub 1 of the value of atomic variable v atomically.
 */
static __inline__ void BBOX_AtomicDec(BBOX_ATOMIC_STRU* v)
{
    __asm__ __volatile__(_LOCK_ "decl %0" : "=m"(v->counter) : "m"(v->counter));
}

/*
    add i to the atomic variable atomically and return new value.
 */
static __inline__ int BBOX_AtomicAddReturn(int i, BBOX_ATOMIC_STRU* v)
{
    int __i;

    __i = i;
    __asm__ volatile(_LOCK_ "xaddl %0, %1" : "+r"(i), "+m"(v->counter) : : "memory");
    return i + __i;
}

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif
