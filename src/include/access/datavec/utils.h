#ifndef UTILS_H
#define UTILS_H
#include "postgres.h"
#include "fmgr/fmgr_comp.h"
#include <vector>

typedef struct VectorArrayData {
    int length;
    int maxlen;
    int dim;
    Size itemsize;
    char *items;
} VectorArrayData;

#define VECTOR_ARRAY_SIZE(_length, _size) (sizeof(VectorArrayData) + (_length) * MAXALIGN(_size))

typedef VectorArrayData * VectorArray;

static inline Pointer VectorArrayGet(VectorArray arr, int offset)
{
    return ((char *) arr->items) + (offset * arr->itemsize);
}

static inline void VectorArraySet(VectorArray arr, int offset, Pointer val)
{
    errno_t rc = memcpy_s(VectorArrayGet(arr, offset), VARSIZE_ANY(val), val, VARSIZE_ANY(val));
    securec_check_c(rc, "\0", "\0");
}

Size VectorItemSize(int dimensions);
Size HalfvecItemSize(int dimensions);
Size BitItemSize(int dimensions);
void VectorUpdateCenter(Pointer v, int dimensions, const float *x);
void HalfvecUpdateCenter(Pointer v, int dimensions, const float *x);
void BitUpdateCenter(Pointer v, int dimensions, const float *x);
void VectorSumCenter(Pointer v, float *x);
void HalfvecSumCenter(Pointer v, float *x);
void BitSumCenter(Pointer v, float *x);
VectorArray VectorArrayInit(int maxlen, int dimensions, Size itemsize);
void VectorArrayFree(VectorArray arr);

int HNSWPQInit();
void HNSWPQUinit();
#endif