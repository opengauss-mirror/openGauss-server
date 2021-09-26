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
 * mot_atomic_ops.h
 *    Utility macros for all atomic operations with strictest memory ordering.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/synchronization/mot_atomic_ops.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MM_ATOMIC_OPS_H
#define MM_ATOMIC_OPS_H

/** @define COMPILER_BARRIER A macro for generating a compiler barrier. */
#if defined(__x86_64__) || defined(__x86__)
#define COMPILER_BARRIER asm volatile("" : : : "memory");
#else
#define COMPILER_BARRIER __sync_synchronize();
#endif

/** @define PAUSE A macro for memory pause. */
#if defined(__x86_64__) || defined(__x86__)
#define PAUSE                                       \
    {                                               \
        __asm__ __volatile__("pause" ::: "memory"); \
    }
#else
#define PAUSE __asm__ __volatile__("" : : : "memory");
#endif

// Utility macros for all atomic operations with strictest memory ordering

/** @define Atomic load of value (imposes memory fence). */
#define MOT_ATOMIC_LOAD(v) __atomic_load_n(&v, __ATOMIC_SEQ_CST)

/** @define Atomic store of value. Performs the operation "v = new_value". */
#define MOT_ATOMIC_STORE(v, new_value) __atomic_store_n(&v, new_value, __ATOMIC_SEQ_CST)

/** @define Atomic addition. Performs the operation "v += value". */
#define MOT_ATOMIC_ADD(v, value) __atomic_add_fetch(&v, value, __ATOMIC_SEQ_CST)

/** @define Atomic subtraction. Performs the operation "v -= value". */
#define MOT_ATOMIC_SUB(v, value) __atomic_sub_fetch(&v, value, __ATOMIC_SEQ_CST)

/** @define Atomic increment. Performs the operation "++v". */
#define MOT_ATOMIC_INC(v) MOT_ATOMIC_ADD(v, 1)

/** @define Atomic decrement. Performs the operation "--v". */
#define MOT_ATOMIC_DEC(v) MOT_ATOMIC_SUB(v, 1)

/** @define Atomic compare-and-swap. Performs the operation "if (v == curr_value) then v = new_value". */
#define MOT_ATOMIC_CAS(v, curr_value, new_value) __sync_bool_compare_and_swap(&v, curr_value, new_value)

#endif /* MM_ATOMIC_OPS_H */