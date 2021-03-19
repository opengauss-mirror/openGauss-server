/*
 * This file is in the public domain, so clarified as of
 * 2006-07-17 by Arthur David Olson.
 *
 * IDENTIFICATION
 *	  src/common/timezone/ialloc.cpp
 */

#include "postgres_fe.h"

#include "private.h"

#define nonzero(n) (((n) == 0) ? 1 : (n))

char* imalloc(int n)
{
    char* ptr = NULL;
    ptr = (char*)malloc((size_t)nonzero(n));

    return ptr;
}

char* icalloc(int nelem, int elsize)
{
    char* ptr = NULL;

    if (nelem == 0 || elsize == 0) {
        nelem = elsize = 1;
    }

    ptr = (char*)calloc((size_t)nelem, (size_t)elsize);

    return ptr;
}

void* irealloc(void* pointer, int size)
{
    void* ptr = NULL;

    if (pointer == NULL) {
        return imalloc(size);
    }

    ptr = (void*)realloc((void*)pointer, (size_t)nonzero(size));

    return ptr;
}

char* icatalloc(char* old, const char* newm)
{
    char* result = NULL;
    int oldsize, newsize;
    errno_t rc = EOK;

    newsize = (newm == NULL) ? 0 : strlen(newm);
    if (old == NULL) {
        oldsize = 0;
    } else if (newsize == 0) {
        return old;
    } else {
        oldsize = strlen(old);
    }

    if ((result = (char*)irealloc(old, oldsize + newsize + 1)) != NULL) {
        if (newm != NULL) {
            rc = strcpy_s(result + oldsize, newsize + 1, newm);
            securec_check_c(rc, "\0", "\0");
        }
    }

    return result;
}

char* icpyalloc(const char* string)
{
    return icatalloc((char*)NULL, string);
}

void ifree(char* p)
{
    if (p != NULL) {
        free(p);
    }
}

void icfree(char* p)
{
    if (p != NULL) {
        free(p);
    }
}
