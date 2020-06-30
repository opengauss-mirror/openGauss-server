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
 * bbox_atomic_X86_64.h
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/bbox/bbox_atomic_X86_64.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef BBOX_ATOMIC_X86_64_H
#define BBOX_ATOMIC_X86_64_H

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

#define __ASM_FORM_(x) " " #x " "

#define __ASM_SEL_(a, b) __ASM_FORM_(b)

#define _ASM_ALIGN_ __ASM_SEL_(.balign 4, .balign 8)
#define _ASM_PTR_ __ASM_SEL_(.long, .quad)

#define _LOCK_                                                                      \
    ".section .smp_locks,\"a\"\n" _ASM_ALIGN_ "\n" _ASM_PTR_ "661f\n" /* address */ \
    ".previous\n"                                                                   \
    "661:\n\tlock; "

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
    read the atomic variable atomically.
 */
static inline int BBOX_AtomicRead(const BBOX_ATOMIC_STRU* v)
{
    return v->counter;
}

/*
    set the value of atomic variable to i atomically.
 */
static inline void BBOX_AtomicSet(BBOX_ATOMIC_STRU* v, int i)
{
    v->counter = i;
}

/*
    add i to the atomic variable atomically and return new value.
 */
static inline int BBOX_AtomicAddReturn(int i, BBOX_ATOMIC_STRU* v)
{
    int __i = i;
    asm volatile(_LOCK_ "xaddl %0, %1" : "+r"(i), "+m"(v->counter) : : "memory");

    return i + __i;
}

/*
    add l to the atomic variable atomically and return new value.
 */
#define BBOX_AtomicIncReturn(v) (BBOX_AtomicAddReturn(1, v))

/*
    add i to the atomic variable atomically.
 */
static inline void BBOX_AtomicAdd(int i, BBOX_ATOMIC_STRU* v)
{
    asm volatile(_LOCK_ "addl %1,%0" : "=m"(v->counter) : "ir"(i), "m"(v->counter));
}

/*
    sub i of the value of atomic variable v atomically.
 */
static inline void BBOX_AtomicSub(int i, BBOX_ATOMIC_STRU* v)
{
    asm volatile(_LOCK_ "subl %1,%0" : "=m"(v->counter) : "ir"(i), "m"(v->counter));
}

/*
    add l to the atomic variable atomically.
 */
static inline void BBOX_AtomicInc(BBOX_ATOMIC_STRU* v)
{
    asm volatile(_LOCK_ "incl %0" : "=m"(v->counter) : "m"(v->counter));
}

/*
    sub i of the value of atomic variable v atomically.
 */
static inline void BBOX_AtomicDec(BBOX_ATOMIC_STRU* v)
{
    asm volatile(_LOCK_ "decl %0" : "=m"(v->counter) : "m"(v->counter));
}

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif
