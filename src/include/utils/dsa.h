/* -------------------------------------------------------------------------
 *
 * dsa.h
 *	  Dynamic shared memory areas.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * IDENTIFICATION
 *	  src/include/utils/dsa.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef DSA_H
#define DSA_H

#include "utils/atomic.h"

/*
 * If this system only uses a 32-bit value for Size, then use the 32-bit
 * implementation of DSA.  This limits the amount of DSA that can be created
 * to something significantly less than the entire 4GB address space because
 * the DSA pointer must encode both a segment identifier and an offset, but
 * that shouldn't be a significant limitation in practice.
 *
 * If this system doesn't support atomic operations on 64-bit values, then
 * we fall back to 32-bit dsa_pointer for lack of other options.
 *
 * For testing purposes, USE_SMALL_DSA_POINTER can be defined to force the use
 * of 32-bit dsa_pointer even on systems capable of supporting a 64-bit
 * dsa_pointer.
 */
#if SIZEOF_SIZE_T == 4
#define SIZEOF_DSA_POINTER 4
#else
#define SIZEOF_DSA_POINTER 8
#endif

/*
 * The type of 'relative pointers' to memory allocated by a dynamic shared
 * area.  dsa_pointer values can be shared with other processes, but must be
 * converted to backend-local pointers before they can be dereferenced.  See
 * dsa_get_address.  Also, an atomic version and appropriately sized atomic
 * operations.
 */
#if SIZEOF_DSA_POINTER == 4
typedef uint32 dsa_pointer;
typedef pg_atomic_uint32 dsa_pointer_atomic;
#define dsa_pointer_atomic_init pg_atomic_init_u32
#define dsa_pointer_atomic_read pg_atomic_read_u32
#define dsa_pointer_atomic_write pg_atomic_write_u32
#define dsa_pointer_atomic_fetch_add pg_atomic_fetch_add_u32
#define dsa_pointer_atomic_compare_exchange pg_atomic_compare_exchange_u32
#define DSA_POINTER_FORMAT "%08x"
#else
typedef uint64 dsa_pointer;
typedef pg_atomic_uint64 dsa_pointer_atomic;
#define dsa_pointer_atomic_init pg_atomic_init_u64
#define dsa_pointer_atomic_read pg_atomic_read_u64
#define dsa_pointer_atomic_write pg_atomic_write_u64
#define dsa_pointer_atomic_fetch_add pg_atomic_fetch_add_u64
#define dsa_pointer_atomic_compare_exchange pg_atomic_compare_exchange_u64
#define DSA_POINTER_FORMAT "%016" INT64_MODIFIER "x"
#endif

/* A sentinel value for dsa_pointer used to indicate failure to allocate. */
#define InvalidDsaPointer (0)

/* Check if a dsa_pointer value is valid. */
#define DsaPointerIsValid(x) ((x) != InvalidDsaPointer)

/* Allocate uninitialized memory with error on out-of-memory. */
#define dsa_allocate(ctx, size) MemoryContextAlloc(ctx, size)

/* Allocate zero-initialized memory with error on out-of-memory. */
#define dsa_allocate0(ctx, size) MemoryContextAllocZero(ctx, size)

#define dsa_free(ctx, dp) pfree(dp)
#define dsa_get_address(a, b) (b)

#endif /* DSA_H */
