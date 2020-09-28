/* ---------------------------------------------------------------------------------------
 *
 * fe_memutils.cpp
 *	  memory management support for frontend code
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/lib/pgcommon/fe_memutils.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres_fe.h"
#include "common/fe_memutils.h"

static inline void* pg_malloc_internal(size_t size, int flags)
{
    void* tmp = NULL;
    errno_t rc = EOK;

    /* Avoid unportable behavior of malloc(0) */
    if (size == 0)
        size = 1;
    tmp = malloc(size);
    if (tmp == NULL) {
        if ((flags & MCXT_ALLOC_NO_OOM) == 0) {
            fprintf(stderr, _("out of memory\n"));
            exit(EXIT_FAILURE);
        }
        return NULL;
    }

    if ((flags & MCXT_ALLOC_ZERO) != 0) {
        rc = memset_s(tmp, size, 0, size);
        securec_check_ss_c(rc, "\0", "\0");
    }
    return tmp;
}

void* pg_malloc(size_t size)
{
    return pg_malloc_internal(size, 0);
}

void *
pg_malloc0(size_t size)
{
	return pg_malloc_internal(size, MCXT_ALLOC_ZERO);
}

void* pg_realloc(void* ptr, size_t size)
{
    void* tmp = NULL;

    /* Avoid unportable behavior of realloc(NULL, 0) */
    if (ptr == NULL && size == 0)
        size = 1;
    tmp = realloc(ptr, size);
    if (NULL == tmp) {
        fprintf(stderr, _("out of memory\n"));
        exit(EXIT_FAILURE);
    }
    return tmp;
}

/*
 * "Safe" wrapper around strdup().
 */
char* pg_strdup(const char* in)
{
    char* tmp = NULL;

    if (NULL == in) {
        fprintf(stderr, _("cannot duplicate null pointer (internal error)\n"));
        exit(EXIT_FAILURE);
    }
    tmp = strdup(in);
    if (NULL == tmp) {
        fprintf(stderr, _("out of memory\n"));
        exit(EXIT_FAILURE);
    }
    return tmp;
}

void pg_free(void* ptr)
{
    if (ptr != NULL) {
        free(ptr);
        ptr = NULL;
    }
}

void* palloc_extended(Size size, int flags)
{
    return pg_malloc_internal(size, flags);
}

void* palloc(Size size)
{
    return pg_malloc_internal(size, 0);
}

void pfree(void* pointer)
{
    pg_free(pointer);
}
