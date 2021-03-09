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
 * atomic.h
 *
 * IDENTIFICATION
 *    src/include/utils/atomic.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef ATOMIC_H
#define ATOMIC_H

#include <stdint.h>
#include "c.h"
#include "storage/barrier.h"
#include "utils/atomic_arm.h"

typedef volatile uint32 pg_atomic_uint32;
typedef volatile uint64 pg_atomic_uint64;

#ifndef WIN32

/*
 * @Description: Atomic increment in a 32-bit address, and return the incremented value.
 * @IN ptr: int32 pointer
 * @IN inc: increase value
 * @Return: new value
 * @See also:
 */
static inline int32 gs_atomic_add_32(volatile int32* ptr, int32 inc)
{
    return __sync_fetch_and_add(ptr, inc) + inc;
}

/*
 * @Description: Atomic increment in a 64-bit address, and return the incremented value.
 * @IN ptr: int64 pointer
 * @IN inc: increase value
 * @Return: new value
 * @See also:
 */
static inline int64 gs_atomic_add_64(int64* ptr, int64 inc)
{
    return __sync_fetch_and_add(ptr, inc) + inc;
}

/*
 * @Description: Atomic set val into *ptr in a 32-bit address, and return the previous pointed by ptr
 * @IN ptr: int32 pointer
 * @IN val: value to set
 * @Return: old value
 * @See also:
 */
static inline int32 gs_lock_test_and_set(volatile int32* ptr, int32 val)
{
    return  __sync_lock_test_and_set(ptr, val);
}

/*
 * @Description: Atomic set val into *ptr in a 32-bit address, and return the previous pointed by ptr
 * @IN ptr: int64 pointer
 * @IN val: value to set
 * @Return: old value
 * @See also:
 */
static inline int64 gs_lock_test_and_set_64(volatile int64* ptr, int64 val)
{
    return  __sync_lock_test_and_set(ptr, val);
}

/*
 * @Description: Atomic compare and set val into *ptr in a 32-bit address, and return compare ok or not
 * @IN ptr: int32 pointer
 * @IN oldval: value to compare
 * @IN newval: value to set
 * @Return: true: dest is equal to the old value false: not equal
 * @See also:
 */
static inline bool gs_compare_and_swap_32(int32* dest, int32 oldval, int32 newval)
{
    if (oldval == newval)
        return true;

    volatile bool res = __sync_bool_compare_and_swap(dest, oldval, newval);

    return res;
}

/*
 * @Description: Atomic compare and set val into *ptr in a 64-bit address, and return compare ok or not
 * @IN ptr: int64 pointer
 * @IN oldval: value to compare
 * @IN newval: value to set
 * @Return: true: dest is equal to the old value false: not equal
 * @See also:
 */
static inline bool gs_compare_and_swap_64(int64* dest, int64 oldval, int64 newval)
{
    if (oldval == newval)
        return true;

    return __sync_bool_compare_and_swap(dest, oldval, newval);
}

static inline uint32 gs_compare_and_swap_u32(volatile uint32* ptr, uint32 oldval, uint32 newval)
{
    return (uint32)__sync_val_compare_and_swap(ptr, oldval, newval);
}

static inline uint64 gs_compare_and_swap_u64(volatile uint64* ptr, uint64 oldval, uint64 newval)
{
    return (uint64)__sync_val_compare_and_swap(ptr, oldval, newval);
}

/*
 * @Description: Atomic init in a 32-bit address.
 * @IN ptr: int32 pointer
 * @IN inc: int32 value
 * @Return: void
 * @See also:
 */
static inline void pg_atomic_init_u32(volatile uint32* ptr, uint32 val)
{
    *ptr = val;
}

/*
 * @Description: Atomic increment in a 32-bit address, and return the incremented value.
 * @IN ptr: int32 pointer
 * @IN inc: increase value
 * @Return: new value
 * @See also:
 */
static inline uint32 pg_atomic_read_u32(volatile uint32* ptr)
{
    return *ptr;
}

/*
 * @Description: Atomic increment in a 32-bit address, and return the old value.
 * @IN ptr: int32 pointer
 * @IN inc: increase value
 * @Return: old value
 * @See also:
 */
static inline uint32 pg_atomic_fetch_or_u32(volatile uint32* ptr, uint32 inc)
{
    return __sync_fetch_and_or(ptr, inc);
}

/*
 * @Description: Atomic and in a 32-bit address, and return the old value.
 * @IN ptr: int32 pointer
 * @IN inc: and value
 * @Return: oid value
 * @See also:
 */
static inline uint32 pg_atomic_fetch_and_u32(volatile uint32* ptr, uint32 inc)
{
    return __sync_fetch_and_and(ptr, inc);
}

/*
 * @Description: Atomic increment in a 32-bit address, and return the old value.
 * @IN ptr: int32 pointer
 * @IN inc: increase value
 * @Return: old value
 * @See also:
 */
static inline uint32 pg_atomic_fetch_add_u32(volatile uint32* ptr, uint32 inc)
{
    return __sync_fetch_and_add(ptr, inc);
}

/*
 * @Description: Atomic increment in a 32-bit address, and return the incremented value.
 * @IN ptr: int32 pointer
 * @IN inc: increase value
 * @Return: new value
 * @See also:
 */
static inline uint32 pg_atomic_add_fetch_u32(volatile uint32* ptr, uint32 inc)
{
    return __sync_fetch_and_add(ptr, inc) + inc;
}

/*
 * @Description: Atomic decrement in a 32-bit address, and return the old value.
 * @IN ptr: int32 pointer
 * @IN inc: decrease value
 * @Return: old value
 * @See also:
 */
static inline uint32 pg_atomic_fetch_sub_u32(volatile uint32* ptr, int32 inc)
{
    return __sync_fetch_and_sub(ptr, inc);
}

/*
 * @Description: Atomic decrement in a 32-bit address, and return the decremented value.
 * @IN ptr: int32 pointer
 * @IN inc: decrease value
 * @Return: new value
 * @See also:
 */
static inline uint32 pg_atomic_sub_fetch_u32(volatile uint32* ptr, int32 inc)
{
    return  __sync_fetch_and_sub(ptr, inc) - inc;
}

/*
 * @Description: Atomic change to given value newval in a 32-bit address,
 * if *ptr is equal to *expected.
 * @IN ptr: int32 pointer
 * @IN expected: int32 pointer, expected value
 * @IN newval: new value
 * @Return: true if *ptr is equal to *expected. otherwise return false.
 * @See also:
 */
static inline bool pg_atomic_compare_exchange_u32(volatile uint32* ptr, uint32* expected, uint32 newval)
{
    bool ret = false;
    uint32	current;
    current = __sync_val_compare_and_swap(ptr, *expected, newval);
    ret = current == *expected;
    *expected = current;
    return ret;
}

/*
 * @Description: Atomic write in a 32-bit address.
 * @IN ptr: int32 pointer
 * @IN inc: new value
 * @See also:
 */
static inline void pg_atomic_write_u32(volatile uint32* ptr, uint32 val)
{
    *ptr = val;
}

/*
 * @Description: Atomic change to the value newval in a 32-bit address,
 * if *ptr is equal to *expected, return old value.
 * @IN ptr: int32 pointer
 * @IN newval: new value
 * @Return: old value
 * @See also:
 */
static inline uint32 pg_atomic_exchange_u32(volatile uint32* ptr, uint32 newval)
{
    uint32 old;
    while (true) {
        old = pg_atomic_read_u32(ptr);
        if (pg_atomic_compare_exchange_u32(ptr, &old, newval))
            break;
    }
    return old;
}

/*
 * @Description: Atomic init in a 64-bit address.
 * @IN ptr: int64 pointer
 * @IN inc: int64 value
 * @Return: void
 * @See also:
 */
static inline void pg_atomic_init_u64(volatile uint64* ptr, uint64 val)
{
    *ptr = val;
}

/*
 * @Description: Atomic read in a 64-bit address, and return the int64 pointer.
 * @IN ptr: int64 pointer
 * @Return: int64 pointer
 * @See also:
 */
static inline uint64 pg_atomic_read_u64(volatile uint64* ptr)
{
    return *ptr;
}

/*
 * @Description: Atomic or in a 64-bit address, and return the old value.
 * @IN ptr: int64 pointer
 * @IN inc: or value
 * @Return: old value
 * @See also:
 */
static inline uint64 pg_atomic_fetch_or_u64(volatile uint64* ptr, uint64 inc)
{
    return __sync_fetch_and_or(ptr, inc);
}

/*
 * @Description: Atomic and in a 64-bit address, and return the old result.
 * @IN ptr: int64 pointer
 * @IN inc: and value
 * @Return: old value
 * @See also:
 */
static inline uint64 pg_atomic_fetch_and_u64(volatile uint64* ptr, uint64 inc)
{
    return __sync_fetch_and_and(ptr, inc);
}

/*
 * @Description: Atomic increment in a 64-bit address, and return the old value.
 * @IN ptr: int64 pointer
 * @IN inc: incremented value
 * @Return: old value
 * @See also:
 */
static inline uint64 pg_atomic_fetch_add_u64(volatile uint64* ptr, uint64 inc)
{
    return __sync_fetch_and_add(ptr, inc);
}

/*
 * @Description: Atomic increment in a 64-bit address, and return the incremented value.
 * @IN ptr: int64 pointer
 * @IN inc: incremented value
 * @Return: new value
 * @See also:
 */
static inline uint64 pg_atomic_add_fetch_u64(volatile uint64* ptr, uint64 inc)
{
    return __sync_fetch_and_add(ptr, inc) + inc;
}

/*
 * @Description: Atomic increment in a 64-bit address, and return the old value.
 * @IN ptr: int64 pointer
 * @IN inc: delcrease value
 * @Return: old value
 * @See also:
 */
static inline uint64 pg_atomic_fetch_sub_u64(volatile uint64* ptr, int64 inc)
{
    return __sync_fetch_and_sub(ptr, inc);
}

/*
 * @Description: Atomic increment in a 64-bit address, and return the decremented value.
 * @IN ptr: int64 pointer
 * @IN inc: decrease value
 * @Return: new value
 * @See also:
 */
static inline uint64 pg_atomic_sub_fetch_u64(volatile uint64* ptr, int64 inc)
{
    return __sync_fetch_and_sub(ptr, inc) - inc;
}

/*
 * @Description: Atomic change to the given value newval in a 64-bit address,
 * if *ptr is equal to *expected.
 * @IN ptr: int64 pointer
 * @IN expected: int64 pointer, expected value
 * @IN newval: new value
 * @Return: true if *ptr is equal to *expected. otherwise return false.
 * @See also:
 */
static inline bool pg_atomic_compare_exchange_u64(volatile uint64* ptr, uint64* expected, uint64 newval)
{
    bool ret = false;
    uint64	current;
    current = __sync_val_compare_and_swap(ptr, *expected, newval);
    ret = current == *expected;
    *expected = current;
    return ret;
}

/*
 * @Description: Atomic write in a 34-bit address.
 * @IN ptr: int64 pointer
 * @IN inc: new value
 * @See also:
 */
static inline void pg_atomic_write_u64(volatile uint64* ptr, uint64 val)
{
    *ptr = val;
}

/*
 * @Description: Atomic change to the value newval in a 64-bit address,
 * if *ptr is equal to *expected, return old value.
 * @IN ptr: int64 pointer
 * @IN newval: new value
 * @Return: old value
 * @See also:
 */
static inline uint64 pg_atomic_exchange_u64(volatile uint64* ptr, uint64 newval)
{
    uint64 old;
    while (true) {
        old = pg_atomic_read_u64(ptr);
        if (pg_atomic_compare_exchange_u64(ptr, &old, newval))
            break;
    }
    return old;
}

#ifdef __aarch64__
/*
 * This static function implements an atomic compare and exchange operation of
 * 128bit width variable. This compares the contents of *ptr with the contents of
 * *old and if equal, writes newval into *ptr. If they are not equal, the
 * current contents of *ptr is written into *old.
 * This API is an alternative implementation of __sync_val_compare_and_swap for 
 * 128bit on ARM64 platforms.
 *
 * @IN ptr: uint128_u pointer shold be 128bit(16bytes) aligned
 * @IN oldval: old value, should be 128bit(16bytes) aligned.
 * @IN newval: new value, should be 128bit(16bytes) aligned
 * @Return: old value
 */
static inline uint128_u arm_compare_and_swap_u128(volatile uint128_u* ptr, uint128_u oldval, uint128_u newval)
{
#ifdef __ARM_LSE
    return __lse_compare_and_swap_u128(ptr, oldval, newval);
#else
    return __excl_compare_and_swap_u128(ptr, oldval, newval);
#endif
}
#endif

static inline void pg_atomic_init_uintptr(volatile uintptr_t* ptr, uintptr_t val)
{
    pg_atomic_init_u64(ptr, (uint64)val);
}

static inline uintptr_t pg_atomic_read_uintptr(volatile uintptr_t* ptr)
{
    return (uintptr_t)pg_atomic_read_u64(ptr);
}

static inline bool pg_atomic_compare_exchange_uintptr(volatile uintptr_t* ptr, uintptr_t* expected, uintptr_t newval)
{
    return pg_atomic_compare_exchange_u64(ptr, (uint64*)expected, (uint64)newval);
}

/*
 * @Description: Atomic write in a uintptr_t address.
 * @IN ptr: uintptr_t pointer
 * @IN inc: new value
 * @See also:
 */
static inline void pg_atomic_write_uintptr(volatile uintptr_t* ptr, uintptr_t val)
{
    pg_atomic_write_u64(ptr, (uint64)val);
}

/*
 * @Description: Atomic change to the value newval in a uintptr_t address,
 * if *ptr is equal to *expected, return old value.
 * @IN ptr: uintptr_t pointer
 * @IN newval: new value
 * @Return: old value
 * @See also:
 */
static inline uintptr_t pg_atomic_exchange_uintptr(volatile uintptr_t* ptr, uintptr_t newval)
{
    return (uintptr_t)pg_atomic_exchange_u64(ptr, (uint64)newval);
}

/*
 * @Description: Atomic load acquire the value in a uint64 address,
 * @IN ptr: uint64 pointer
 * @Return: the value of pointer
 * @See also:
 */
static inline uint64 pg_atomic_barrier_read_u64(volatile uint64* ptr)
{
    return __atomic_load_n(ptr, __ATOMIC_ACQUIRE);
}

/*
 * @Description This API is an unified wrapper of __sync_val_compare_and_swap for 
 * 128bit on ARM64 platforms and X86.
 *
 * @IN ptr: uint128_t pointer shold be 128bit(16bytes) aligned
 * @IN oldval: old value, should be 128bit(16bytes) aligned.
 * @IN newval: new value, should be 128bit(16bytes) aligned
 * @Return: old value
 */
static inline uint128_u atomic_compare_and_swap_u128(
    volatile uint128_u* ptr, 
    uint128_u oldval = uint128_u{0}, 
    uint128_u newval = uint128_u{0})
{
#ifdef __aarch64__
    return arm_compare_and_swap_u128(ptr, oldval, newval);
#else
    uint128_u ret;
    ret.u128 = __sync_val_compare_and_swap(&ptr->u128, oldval.u128, newval.u128);
    return ret;
#endif
}
#endif

#endif /* ATOMIC_H */

