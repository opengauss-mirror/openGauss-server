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
 * ---------------------------------------------------------------------------------------
 * 
 * atomic.h
 *        Header file for atomic operations.
 * 
 * 
 * IDENTIFICATION
 *        src/include/gtm/atomic.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_GTM_ATOMIC_H
#define SRC_INCLUDE_GTM_ATOMIC_H

extern int32 gs_atomic_add_32(volatile int32* ptr, int32 inc);
extern int64 gs_atomic_add_64(int64* ptr, int64 inc);

extern int32 gs_lock_test_and_set(volatile int32* ptr, int32 val);
extern int64 gs_lock_test_and_set_64(volatile int64* ptr, int64 val);

extern bool gs_compare_and_swap_32(volatile int32* dest, int32 oldval, int32 newval);
extern bool gs_compare_and_swap_64(int64* dest, int64 oldval, int64 newval);

extern void pg_atomic_init_u32(volatile uint32* ptr, uint32 val);
extern uint32 pg_atomic_read_u32(volatile uint32* ptr);
extern uint32 pg_atomic_fetch_or_u32(volatile uint32* ptr, uint32 inc);
extern uint32 pg_atomic_fetch_and_u32(volatile uint32* ptr, uint32 inc);
extern uint32 pg_atomic_fetch_add_u32(volatile uint32* ptr, uint32 inc);
extern uint32 pg_atomic_add_fetch_u32(volatile uint32* ptr, uint32 inc);
extern uint32 pg_atomic_fetch_sub_u32(volatile uint32* ptr, int32 inc);
extern uint32 pg_atomic_sub_fetch_u32(volatile uint32* ptr, int32 inc);
extern bool pg_atomic_compare_exchange_u32(volatile uint32* ptr, uint32* expected, uint32 newval);
extern void pg_atomic_write_u32(volatile uint32* ptr, uint32 val);
extern uint32 pg_atomic_exchange_u32(volatile uint32* ptr, uint32 newval);

extern void pg_atomic_init_u64(volatile uint64* ptr, uint64 val);
extern uint64 pg_atomic_read_u64(volatile uint64* ptr);
extern uint64 pg_atomic_fetch_or_u64(volatile uint64* ptr, uint64 inc);
extern uint64 pg_atomic_fetch_and_u64(volatile uint64* ptr, uint64 inc);
extern uint64 pg_atomic_fetch_add_u64(volatile uint64* ptr, uint64 inc);
extern uint64 pg_atomic_add_fetch_u64(volatile uint64* ptr, uint64 inc);
extern uint64 pg_atomic_fetch_sub_u64(volatile uint64* ptr, int64 inc);
extern uint64 pg_atomic_sub_fetch_u64(volatile uint64* ptr, int64 inc);
extern bool pg_atomic_compare_exchange_u64(volatile uint64* ptr, uint64* expected, uint64 newval);
extern void pg_atomic_write_u64(volatile uint64* ptr, uint64 val);
extern uint64 pg_atomic_exchange_u64(volatile uint64* ptr, uint64 newval);

#endif  // SRC_INCLUDE_GTM_ATOMIC_H