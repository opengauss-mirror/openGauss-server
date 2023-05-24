/* -------------------------------------------------------------------------
 *
 * palloc.h
 *	  openGauss memory allocator definitions.
 *
 * This file contains the basic memory allocation interface that is
 * needed by almost every backend module.  It is included directly by
 * postgres.h, so the definitions here are automatically available
 * everywhere.	Keep it lean!
 *
 * Memory allocation occurs within "contexts".	Every chunk obtained from
 * palloc()/MemoryContextAlloc() is allocated within a specific context.
 * The entire contents of a context can be freed easily and quickly by
 * resetting or deleting the context --- this is both faster and less
 * prone to memory-leakage bugs than releasing chunks individually.
 * We organize contexts into context trees to allow fine-grain control
 * over chunk lifetime while preserving the certainty that we will free
 * everything that should be freed.  See utils/mmgr/README for more info.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd. 
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * $PostgreSQL: pgsql/src/include/utils/palloc.h,v 1.40 2008/06/28 16:45:22 tgl Exp $
 *
 * -------------------------------------------------------------------------
 */
#ifndef PALLOC_H
#define PALLOC_H

/*
 * Flags for MemoryContextAllocExtended.
 */
#define MCXT_ALLOC_HUGE 0x01   /* allow huge allocation (> 1 GB) */
#define MCXT_ALLOC_NO_OOM 0x02 /* no failure if out-of-memory */
#define MCXT_ALLOC_ZERO 0x04   /* zero allocated memory */

/*
 * Type MemoryContextData is declared in nodes/memnodes.h.	Most users
 * of memory allocation should just treat it as an abstract type, so we
 * do not provide the struct contents here.
 */
typedef struct MemoryContextData* MemoryContext;

/*
 * Fundamental memory-allocation operations (more are in utils/memutils.h)
 */
extern void* MemoryContextAlloc(MemoryContext context, Size size);
extern void* MemoryContextAllocZero(MemoryContext context, Size size);
extern void* MemoryContextAllocZeroAligned(MemoryContext context, Size size);

#define palloc(sz) MemoryContextAlloc(CurrentMemoryContext, (sz))

#define palloc0(sz) MemoryContextAllocZero(CurrentMemoryContext, (sz))

/*
 * The result of palloc() is always word-aligned, so we can skip testing
 * alignment of the pointer when deciding which MemSet variant to use.
 * Note that this variant does not offer any advantage, and should not be
 * used, unless its "sz" argument is a compile-time constant; therefore, the
 * issue that it evaluates the argument multiple times isn't a problem in
 * practice.
 */
#define palloc0fast(sz)                                                          \
    (MemSetTest(0, sz) ? MemoryContextAllocZeroAligned(CurrentMemoryContext, sz) \
                       : MemoryContextAllocZero(CurrentMemoryContext, sz))

extern void pfree(void* pointer);
extern void opt_pfree(void* pointer);

extern void* repalloc(void* pointer, Size size);

/*
 * MemoryContextSwitchTo can't be a macro in standard C compilers.
 * But we can make it an inline function when using GCC.
 */

extern MemoryContext MemoryContextSwitchTo(MemoryContext context);

/*
 * These are like standard strdup() except the copied string is
 * allocated in a context, not with malloc().
 */
extern char* MemoryContextStrdup(MemoryContext context, const char* string);

#define pstrdup(str) MemoryContextStrdup(CurrentMemoryContext, (str))

extern char* pnstrdup(const char* in, Size len);

#if defined(WIN32) || defined(__CYGWIN__)
extern void* pgport_palloc(Size sz);
extern char* pgport_pstrdup(const char* str);
extern void pgport_pfree(void* pointer);
#endif

#ifdef PGXC
/*
 * The following part provides common palloc binary interface.  This
 * is needed especially for gtm_serialize.c and gtm_serialize_debug.c.
 */
#include "gen_alloc.h"
#endif

#endif /* PALLOC_H */
