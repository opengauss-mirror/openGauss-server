/*-------------------------------------------------------------------------
 *
 * parray.c: pointer array collection.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Copyright (c) 2009-2011, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include "parray.h"
#include "pgut.h"

static size_t qsort_size = 100000; /* 100000 = default size */

/* members of struct parray are hidden from client. */
struct parray
{
    void **data;    /* pointer array, expanded if necessary */
    size_t alloced; /* number of elements allocated */
    size_t used;    /* number of elements in use */
};

/*
 * Create new parray object.
 * Never returns NULL.
 */
parray *
parray_new(void)
{
    parray *a = pgut_new(parray);

    a->data = NULL;
    a->used = 0;
    a->alloced = 0;

    parray_expand(a, 1024);

    return a;
}

/*
 * Expand array pointed by data to newsize.
 * Elements in expanded area are initialized to NULL.
 * Note: never returns NULL.
 */
void
parray_expand(parray *array, size_t newsize)
{
    void **p;
    errno_t rc = 0;

    /* already allocated */
    if (newsize <= array->alloced)
        return;

    p = (void **)pgut_realloc(array->data, sizeof(void *) * array->alloced, sizeof(void *) * newsize);

    /* initialize expanded area to NULL */
    rc = memset_s(p + array->alloced, (newsize - array->alloced) * sizeof(void *),
             0, (newsize - array->alloced) * sizeof(void *));
    securec_check_c(rc, "\0", "\0");

    array->alloced = newsize;
    array->data = p;
}

void
parray_free(parray *array)
{
    if (array == NULL)
        return;
    free(array->data);
    free(array);
}

void
parray_append(parray *array, void *elem)
{
    if (array->used + 1 > array->alloced)
        parray_expand(array, array->alloced * 2);

    array->data[array->used++] = elem;
}

void
parray_insert(parray *array, size_t index, void *elem)
{
    errno_t rc = 0;
    
    if (array->used + 1 > array->alloced)
        parray_expand(array, array->alloced * 2);

    rc = memmove_s(array->data + index + 1, (array->alloced - index - 1) * sizeof(void *), array->data + index,
        (array->alloced - index - 1) * sizeof(void *));
    securec_check_c(rc, "\0", "\0");
    array->data[index] = elem;

    /* adjust used count */
    if (array->used < index + 1)
        array->used = index + 1;
    else
        array->used++;
}

/*
 * Concatenate two parray.
 * parray_concat() appends the copy of the content of src to the end of dest.
 */
parray *
parray_concat(parray *dest, const parray *src)
{
    errno_t rc = 0;
    /* expand head array */
    parray_expand(dest, dest->used + src->used);

    /* copy content of src after content of dest */
    rc = memcpy_s(dest->data + dest->used, (dest->alloced - dest->used) * sizeof(void *),
             src->data, src->used * sizeof(void *));
    securec_check_c(rc, "\0", "\0");
    dest->used += parray_num(src);

    return dest;
}

void
parray_set(parray *array, size_t index, void *elem)
{
    if (index > array->alloced - 1)
        parray_expand(array, index + 1);

    array->data[index] = elem;

    /* adjust used count */
    if (array->used < index + 1)
        array->used = index + 1;
}

void *
parray_get(const parray *array, size_t index)
{
    if (index > array->alloced - 1)
        return NULL;
    return array->data[index];
}

void *
parray_remove(parray *array, size_t index)
{
    errno_t rc = 0;
    void *val;

    /* removing unused element */
    if (index > array->used)
        return NULL;

    val = array->data[index];

    /* Do not move if the last element was removed. */
    if (index < array->alloced - 1) {
        rc = memmove_s(array->data + index, (array->alloced - index) * sizeof(void *), array->data + index + 1,
         (array->alloced - index - 1) * sizeof(void *));
        securec_check_c(rc, "\0", "\0");
    }

    /* adjust used count */
    array->used--;

    return val;
}

bool
parray_rm(parray *array, const void *key, int(*compare)(const void *, const void *))
{
    size_t i;

    for (i = 0; i < array->used; i++)
    {
        if (compare(&key, &array->data[i]) == 0)
        {
            parray_remove(array, i);
            return true;
        }
    }
    return false;
}

size_t
parray_num(const parray *array)
{
    return array->used;
}

static void HeapAdjust(void **array, size_t size, size_t index,
                       int(*compare)(const void *, const void *))
{
    size_t parent = index;
    size_t child = 2 * parent + 1; /* 2 * n + 1 :left child */
    while (child < size) {
        if (child + 1 < size && compare(&array[child + 1], &array[child]) > 0) {
            child = child + 1;
        }

        if (compare(&array[child], &array[parent]) > 0) {
            void *tmp = array[child];
            array[child] = array[parent];
            array[parent] = tmp;
        } else {
            break;
        }

        parent = child;
        child = 2 * parent + 1;  /* 2 * n + 1 :left child */
    }
}

static void HeapPop(void **array, size_t size,
                    int(*compare)(const void *, const void *))
{
    void *tmp = array[0];
    array[0] = array[size - 1];
    array[size - 1] = tmp;

    HeapAdjust(array, size - 1, 0, compare);
}

static void HeapSort(void **array, size_t size,
                     int(*compare)(const void *, const void *))
{
    for (int64 i = ((int64)size - 2) / 2; i >= 0; i--) { /* parent node:(size -2) / 2 */
        HeapAdjust(array, size, i, compare);
    }

    for (size_t i = 0; i < size; i++) {
        HeapPop(array, size - i, compare);
    }
}

void
parray_qsort(parray *array, int(*compare)(const void *, const void *))
{
    Assert(array->used < (PG_UINT64_MAX / 1024));
    if (array->used <= qsort_size) {
        qsort(array->data, array->used, sizeof(void *), compare);
    } else {
        HeapSort(array->data, array->used, compare);
    }
}

void
parray_walk(parray *array, void (*action)(void *))
{
    size_t i;
    if (array == nullptr) {
        return;
    }

    for (i = 0; i < array->used; i++)
        action(array->data[i]);
}

void *
parray_bsearch(parray *array, const void *key, int(*compare)(const void *, const void *))
{
    return bsearch(&key, array->data, array->used, sizeof(void *), compare);
}

/* checks that parray contains element */
bool parray_contains(parray *array, const void *elem)
{
    int i;

    for (i = 0; i < (int)parray_num(array); i++)
    {
        if (parray_get(array, i) == elem)
            return true;
    }
    return false;
}
