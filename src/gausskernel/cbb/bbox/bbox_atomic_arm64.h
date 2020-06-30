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
 * bbox_atomic_arm64.h
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/bbox/bbox_atomic_arm64.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef BBOX_ATOMIC_ARM64_H
#define BBOX_ATOMIC_ARM64_H

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

#define barrier() __asm__ __volatile__("" : : : "memory")
#define smp_mb() barrier()

/*
    define the atomic variable
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
#define BBOX_AtomicRead(v) ((v)->counter)

/*
    set the value of atomic variable to i atomically.
 */
#define BBOX_AtomicSet(v, i) (((v)->counter) = (i))

/*
    add i to the atomic variable atomically.
 */
static inline void BBOX_AtomicAdd(int i, BBOX_ATOMIC_STRU* v)
{
    unsigned long tmp;
    int result;

    __asm__ __volatile__("1:	ldxr	%w0, %2\n"
                         "	add	%w0, %w0, %w3\n"
                         "	stxr	%w1, %w0, %2\n"
                         "	cbnz	%w1, 1b"
                         : "=&r"(result), "=&r"(tmp), "+Q"(v->counter)
                         : "Ir"(i));
}

/*
    add i to the atomic variable atomically and return new value.
 */
static inline int BBOX_AtomicAddReturn(int i, BBOX_ATOMIC_STRU* v)
{
    unsigned long tmp;
    int result;

    __asm__ __volatile__("1:	ldxr	%w0, %2\n"
                         "	add	%w0, %w0, %w3\n"
                         "	stlxr	%w1, %w0, %2\n"
                         "	cbnz	%w1, 1b"
                         : "=&r"(result), "=&r"(tmp), "+Q"(v->counter)
                         : "Ir"(i)
                         : "memory");

    smp_mb();
    return result;
}

#define BBOX_AtomicIncReturn(v) BBOX_AtomicAddReturn(1, (v))

/*
    sub i of the value of atomic variable atomically.
 */
static inline void BBOX_AtomicSub(int i, BBOX_ATOMIC_STRU* v)
{
    unsigned long tmp;
    int result;

    __asm__ __volatile__("1:	ldxr	%w0, %2\n"
                         "	sub	%w0, %w0, %w3\n"
                         "	stxr	%w1, %w0, %2\n"
                         "	cbnz	%w1, 1b"
                         : "=&r"(result), "=&r"(tmp), "+Q"(v->counter)
                         : "Ir"(i));
}

/*
    add l to the atomic variable atomically.
 */
#define BBOX_AtomicInc(v) BBOX_AtomicAdd(1, v)

/*
    sub l of the value of atomic variable atomically.
 */
#define BBOX_AtomicDec(v) BBOX_AtomicSub(1, v)

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif
