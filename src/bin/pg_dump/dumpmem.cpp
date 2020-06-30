/* -------------------------------------------------------------------------
 *
 * dumpmem.c
 *	  Memory allocation routines used by pg_dump, pg_dumpall, and pg_restore
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/bin/pg_dump/dumpmem.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres_fe.h"
#include "dumputils.h"
#include "dumpmem.h"

#ifdef GAUSS_SFT_TEST
#include "gauss_sft.h"
#endif

/*
 * Safer versions of some standard C library functions. If an
 * out-of-memory condition occurs, these functions will bail out via exit();
 *therefore, their return value is guaranteed to be non-NULL.
 */
char* gs_strdup(const char* string)
{
    char* tmp = NULL;

    if (NULL == string)
        exit_horribly(NULL, "cannot duplicate null pointer\n");
    tmp = strdup(string);
    if (NULL == tmp)
        exit_horribly(NULL, "out of memory\n");
    return tmp;
}

void* pg_malloc(size_t size)
{
    void* tmp = NULL;

    /* Avoid unportable behavior of malloc(0) */
    if (size == 0)
        size = 1;
    tmp = (void*)malloc(size);
    if (NULL == tmp)
        exit_horribly(NULL, "out of memory\n");
    return tmp;
}

void* pg_calloc(size_t nmemb, size_t size)
{
    void* tmp = NULL;

    if (nmemb == 0 || size == 0)
        exit_horribly(NULL, "Invalid size or invalid block count specified\n");
    tmp = (void*)calloc(nmemb, size);
    if (NULL == tmp)
        exit_horribly(NULL, "out of memory\n");
    return tmp;
}

void* pg_realloc(void* ptr, size_t size)
{
    void* tmp = NULL;

    /* Avoid unportable behavior of realloc(NULL, 0) */
    if (ptr == NULL)  // && size == 0) /* If ptr is NULL then size will be definitely 0*/
        size = 1;
    tmp = (void*)realloc(ptr, size);
    if (NULL == tmp)
        exit_horribly(NULL, "out of memory\n");
    return tmp;
}
