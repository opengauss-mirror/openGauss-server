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
 * sctp_atomics.h
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/communication/sctp_utils/sctp_atomics.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __SCTP_ATOMIC_H
#define __SCTP_ATOMIC_H

#ifdef __GNUC__
/* Atomic functions in GCC are present from version 4.1.0 on
 * http://gcc.gnu.org/onlinedocs/gcc-4.1.0/gcc/Atomic-Builtins.html
 */
#define atomic_set(ptr, newVal) __sync_lock_test_and_set(ptr, newVal)

/* @brief atomically adds count to the variable pointed by ptr
 * @return the value that had previously been in memory
 */
#define atomic_add(ptr, count) __sync_fetch_and_add(ptr, count)

/* @brief atomically substracts count from the variable pointed by ptr
 * @return the value that had previously been in memory
 */
#define atomic_sub(ptr, count) __sync_fetch_and_sub(ptr, count)

/* @brief Compare And Swap
 * If the current value of *ptr is oldVal, then write newVal into *ptr
 * @return true if the comparison is successful and newVal was written
 */
#define COMPARE_AND_SWAP(ptr, oldVal, newVal) __sync_bool_compare_and_swap(ptr, oldVal, newVal)

/* @brief Compare And Swap
 * If the current value of *ptr is oldVal, then write newVal into *ptr
 * @return the contents of *ptr before the operation
 */
#define COMPARE_AND_SWAP_VAL(ptr, oldVal, newVal) __sync_val_compare_and_swap(ptr, oldVal, newVal)

#else
#error Atomic functions such as COMPARE_AND_SWAP or AtomicAdd are not defined for your compiler.
#endif  // __GNUC__

#endif  // __SCTP_ATOMIC_H
