/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2018-2020. All rights reserved.
 * Description: This is assembly implementation of 128bit exclusive CAS and atomic 
 * operations leverage ARMv8.1-a large system extension(LSE), which is faster 
 * than legacy exclusive mode APIs of GNU Built-in functions.
 * Author: Peng Fengbin
 * Create: 2020-01-11
 *
 * */

#ifndef ATOMIC_LSE_H
#define ATOMIC_LSE_H
#include "c.h"
#include "elog.h"

#ifdef __aarch64__

typedef __uint128_t uint128_t;


/*
 * Exclusive load/store 2 uint64_t variables to fullfil 128bit atomic compare and swap
 * */
static inline bool __excl_compare_and_swap_u128(
    volatile uint128_t* ptr, uint64_t old_low, uint64_t old_high, uint64_t new_low, uint64_t new_high)
{
    uint64_t tmp, ret;
    asm volatile("1:     ldxp    %0, %1, %2\n"
                 "       eor     %0, %0, %3\n"
                 "       eor     %1, %1, %4\n"
                 "       orr     %1, %0, %1\n"
                 "       cbnz    %1, 2f\n"
                 "       stlxp   %w0, %5, %6, %2\n"
                 "       cbnz    %w0, 1b\n"
                 "       dmb ish\n"
                 "2:"
                 : "=&r"(tmp), "=&r"(ret), "+Q"(*(uint128_t*)ptr)
                 : "r"(old_low), "r"(old_high), "r"(new_low), "r"(new_high)
                 : "memory");

    return ret;
}


/*
 * using CASP instinct to atomically compare and swap 2 uint64_t variables to fullfil
 * 128bit atomic compare and swap
 * */
static inline bool __lse_compare_and_swap_u128(volatile uint128_t *ptr,         \
                                         uint64_t old_low,                      \
                                         uint64_t old_high,                     \
                                         uint64_t new_low,                      \
                                         uint64_t new_high)
{                                                                               \
    unsigned long oldval1 = old_low;                                            \
    unsigned long oldval2 = old_high;                                           \
    register unsigned long x0 asm ("x0") = old_low;                             \
    register unsigned long x1 asm ("x1") = old_high;                            \
    register unsigned long x2 asm ("x2") = new_low;                             \
    register unsigned long x3 asm ("x3") = new_high;                            \
    register unsigned long x4 asm ("x4") = (unsigned long)ptr;                  \
                                                                                \
    asm volatile(".arch_extension lse\n"                                        \
    "   caspal    %[old_low], %[old_high], %[new_low], %[new_high], %[v]\n"     \
    "   eor %[old_low], %[old_low], %[oldval1]\n"                               \
    "   eor %[old_high], %[old_high], %[oldval2]\n"                             \
    "   orr %[old_low], %[old_low], %[old_high]"                                \
    : [old_low] "+&r" (x0), [old_high] "+&r" (x1),                              \
      [v] "+Q" (*(ptr))                                                         \
    : [new_low] "r" (x2), [new_high] "r" (x3), [ptr] "r" (x4),                  \
      [oldval1] "r" (oldval1), [oldval2] "r" (oldval2)                          \
    : "x16", "x17", "x30", "memory");                                           \
                                                                                \
    return x0;                                                                  \
}

/*
 * alternative atomic operations of __sync_val_compare_and_swap for 32bits integers
 * */
static inline uint32 __lse_compare_and_swap_u32(volatile uint32 *ptr, uint32 oldval, uint32 newval)
{
    register unsigned long x0 asm ("x0") = (unsigned long)ptr;                  \
    register uint32 x1 asm ("x1") = oldval;                                     \
    register uint32 x2 asm ("x2") = newval;                                     \
                                                                                \
    asm volatile(".arch_extension lse\n"                                        \
        "       mov     w30, %w[oldval]\n"                                      \
        "       casal  w30, %w[newval], %[v]\n"                                 \
        "       mov    %w[ret], w30\n"                                          \
        : [ret] "+r" (x0), [v] "+Q" (*(ptr))                                    \
        : [oldval] "r" (x1), [newval] "r" (x2)                                  \
        : "x16", "x17", "x30", "memory");                                       \
    return x0;                                                                  \
}

/*
 * alternative atomic operations of __sync_val_compare_and_swap for 64bits integers
 * */
static inline uint64 __lse_compare_and_swap_u64(volatile uint64 *ptr, uint64 oldval, uint64 newval)
{
    register unsigned long x0 asm ("x0") = (unsigned long)ptr;                  \
    register uint64 x1 asm ("x1") = oldval;                                     \
    register uint64 x2 asm ("x2") = newval;                                     \
                                                                                \
    asm volatile(".arch_extension lse\n"                                        \
        "       mov     x30, %x[oldval]\n"                                      \
        "       casal  x30, %x[newval], %[v]\n"                                 \
        "       mov    %x[ret], x30"                                            \
        : [ret] "+r" (x0), [v] "+Q" (*ptr)                                      \
        : [oldval] "r" (x1), [newval] "r" (x2)                                  \
        : "x16", "x17", "x30", "memory");                                       \
    return x0;                                                                  \
}


/*
 * alternative atomic operations of __sync_fetch_and_and for 32bits integers
 * */
static inline uint32 __lse_atomic_fetch_and_u32(volatile uint32 *ptr, uint32 val)
{
    register uint32 w0 asm ("w0") = val;                                        \
    register uint32 *x1 asm ("x1") = (uint32 *)(unsigned long)ptr;              \
                                                                                \
    asm volatile(".arch_extension lse\n"                                        \
        "       mvn     %w[val], %w[val]\n"                                     \
        "       ldclral  %w[val], %w[val], %[v]\n"                              \
        : [val] "+&r" (w0), [v] "+Q" (*ptr)                                     \
        : "r" (x1)                                                              \
        : "x16", "x17", "x30", "memory");                                       \
    return w0;                                                                  \
}

/*
 * alternative atomic operations of __sync_fetch_and_and for 64bits integers
 * */
static inline uint64 __lse_atomic_fetch_and_u64(volatile uint64 *ptr, uint64 val)
{
    register uint64 x0 asm ("w0") = val;                                        \
    register uint64 *x1 asm ("x1") = (uint64 *)(unsigned long)ptr;              \
                                                                                \
    asm volatile(".arch_extension lse\n"                                        \
        "       mvn     %[val], %[val]\n"                                       \
        "       ldclral  %[val], %[val], %[v]\n"                                \
        : [val] "+&r" (x0), [v] "+Q" (*ptr)                                     \
        : "r" (x1)                                                              \
        : "x16", "x17", "x30", "memory");                                       \
    return x0;                                                                  \
}


/*
 * alternative atomic operations of __sync_fetch_and_add for 32bits integers
 * */
static inline uint32 __lse_atomic_fetch_add_u32(volatile uint32 *ptr, uint32 val)
{
    register uint32 w0 asm ("w0") = val;                                        \
    register uint32 *x1 asm ("x1") = (uint32 *)(unsigned long)ptr;              \
                                                                                \
    asm volatile(".arch_extension lse\n"                                        \
        "       ldaddal  %w[val], %w[val], %[v]\n"                              \
        : [val] "+r" (w0), [v] "+Q" (*ptr)                                      \
        : "r" (x1)                                                              \
        : "x16", "x17", "x30", "memory");                                       \
    return w0;                                                                  \
}


/*
 * alternative atomic operations of __sync_fetch_and_add for 64bits integers
 * */
static inline uint64 __lse_atomic_fetch_add_u64(volatile uint64 *ptr, uint64 val)
{
    register uint64 x0 asm ("w0") = val;                                        \
    register uint64 *x1 asm ("x1") = (uint64 *)(unsigned long)ptr;              \
                                                                                \
    asm volatile(".arch_extension lse\n"                                        \
        "       ldaddal  %[val], %[val], %[v]\n"                                \
        : [val] "+r" (x0), [v] "+Q" (*ptr)                                      \
        : "r" (x1)                                                              \
        : "x16", "x17", "x30", "memory");                                       \
    return x0;                                                                  \
}

/*
 * alternative atomic operations of __sync_fetch_and_or for 32bits integers
 * */
static inline uint32 __lse_atomic_fetch_or_u32(volatile uint32 *ptr, uint32 val)
{
    register uint32 w0 asm ("w0") = val;                                        \
    register uint32 *x1 asm ("x1") = (uint32 *)(unsigned long)ptr;              \
                                                                                \
    asm volatile(".arch_extension lse\n"                                        \
        "       ldsetal  %w[val], %w[val], %[v]\n"                              \
        : [val] "+r" (w0), [v] "+Q" (*ptr)                                      \
        : "r" (x1)                                                              \
        : "x16", "x17", "x30", "memory");                                       \
    return w0;                                                                  \
}

/*
 * alternative atomic operations of __sync_fetch_and_or for 64bits integers
 * */
static inline uint64 __lse_atomic_fetch_or_u64(volatile uint64 *ptr, uint64 val)
{
    register uint64 x0 asm ("w0") = val;                                        \
    register uint64 *x1 asm ("x1") = (uint64 *)(unsigned long)ptr;              \
                                                                                \
    asm volatile(".arch_extension lse\n"                                        \
        "       ldsetal  %[val], %[val], %[v]\n"                                \
        : [val] "+r" (x0), [v] "+Q" (*ptr)                                      \
        : "r" (x1)                                                              \
        : "x16", "x17", "x30", "memory");                                       \
    return x0;                                                                  \
}

/*
 * alternative atomic operations of __sync_fetch_and_sub for 32bits integers
 * */
static inline uint32 __lse_atomic_fetch_sub_u32(volatile uint32 *ptr, uint32 val)
{
    register uint32 w0 asm ("w0") = val;                                        \
    register uint32 *x1 asm ("x1") = (uint32 *)(unsigned long)ptr;              \
                                                                                \
    asm volatile(".arch_extension lse\n"                                        \
        "       neg  %w[val], %w[val]\n"                                        \
        "       ldaddal  %w[val], %w[val], %[v]\n"                              \
        : [val] "+&r" (w0), [v] "+Q" (*ptr)                                     \
        : "r" (x1)                                                              \
        : "x16", "x17", "x30", "memory");                                       \
    return w0;                                                                  \
}

/*
 * alternative atomic operations of __sync_fetch_and_sub for 64bits integers
 * */
static inline uint64 __lse_atomic_fetch_sub_u64(volatile uint64 *ptr, uint64 val)
{
    register uint64 x0 asm ("w0") = val;                                        \
    register uint64 *x1 asm ("x1") = (uint64 *)(unsigned long)ptr;              \
                                                                                \
    asm volatile(".arch_extension lse\n"                                        \
        "       neg  %[val], %[val]\n"                                          \
        "       ldaddal  %[val], %[val], %[v]\n"                                \
        : [val] "+&r" (x0), [v] "+Q" (*ptr)                                     \
        : "r" (x1)                                                              \
        : "x16", "x17", "x30", "memory");                                       \
    return x0;                                                                  \
}

#endif // __aarch64__

#endif
