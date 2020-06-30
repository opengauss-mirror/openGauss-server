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
 * bbox_atomic_arm7.h
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/bbox/bbox_atomic_arm7.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef BBOX_ATOMIC_ARM_H
#define BBOX_ATOMIC_ARM_H

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

#ifdef CONFIG_SMP
#undef CONFIG_SMP
#endif

/*
    define the atomic variable
 */
typedef struct {
    volatile int counter;
} BBOX_ATOMIC_STRU;

#define barrier() __asm__ __volatile__("" : : : "memory")
#define dmb() __asm__ __volatile__("dmb" : : : "memory")

#ifndef CONFIG_SMP
#define smp_mb() barrier()
#else
#define smp_mb() dmb()
#endif

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
    unsigned long tmp_var;
    int ret;

    __asm__ __volatile__("@ BBOX_AtomicAdd\n"
                         "1: ldrex   %0, [%2]\n"
                         "   add %0, %0, %3\n"
                         "   strex   %1, %0, [%2]\n"
                         "   teq %1, #0\n"
                         "   bne 1b"
                         : "=&r"(ret), "=&r"(tmp_var)
                         : "r"(&v->counter), "Ir"(i)
                         : "cc");
}

/*
    add i to the atomic variable atomically and return new value.
 */
static inline int BBOX_AtomicAddReturn(int i, BBOX_ATOMIC_STRU* v)
{
    unsigned long tmp_var;
    int ret;

    smp_mb();

    __asm__ __volatile__("@ BBOX_AtomicAddReturn\n"
                         "1: ldrex   %0, [%2]\n"
                         "   add %0, %0, %3\n"
                         "   strex   %1, %0, [%2]\n"
                         "   teq %1, #0\n"
                         "   bne 1b"
                         : "=&r"(ret), "=&r"(tmp_var)
                         : "r"(&v->counter), "Ir"(i)
                         : "cc");

    smp_mb();

    return ret;
}

#define BBOX_AtomicIncReturn(v) BBOX_AtomicAddReturn(1, (v))

/*
    sub i of the value of atomic variable atomically.
 */
static inline void BBOX_AtomicSub(int i, BBOX_ATOMIC_STRU* v)
{
    unsigned long tmp_var;
    int ret;

    __asm__ __volatile__("@ BBOX_AtomicSub\n"
                         "1: ldrex   %0, [%2]\n"
                         "   sub %0, %0, %3\n"
                         "   strex   %1, %0, [%2]\n"
                         "   teq %1, #0\n"
                         "   bne 1b"
                         : "=&r"(ret), "=&r"(tmp_var)
                         : "r"(&v->counter), "Ir"(i)
                         : "cc");
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
