#ifndef UTILS_H
#define UTILS_H
#include "postgres.h"
#include "fmgr/fmgr_comp.h"
#include <vector>

#define GENERIC_DEFAULT_ENABLE_PQ false
#define GENERIC_DEFAULT_PQ_M 8
#define GENERIC_MIN_PQ_M 1
#define GENERIC_MAX_PQ_M HNSW_MAX_DIM
#define GENERIC_DEFAULT_PQ_KSUB 256
#define GENERIC_MIN_PQ_KSUB 1
#define GENERIC_MAX_PQ_KSUB 256

typedef struct VectorArrayData {
    int length;
    int maxlen;
    int dim;
    Size itemsize;
    char *items;
} VectorArrayData;

typedef struct PQParams {
    int pqM;
    int pqKsub;
    int funcType;
    int dim;
    int pqMode;
    size_t subItemSize;
    char *pqTable;
} PQParams;

#define VECTOR_ARRAY_SIZE(_length, _size) (sizeof(VectorArrayData) + (_length) * MAXALIGN(_size))

typedef VectorArrayData * VectorArray;

typedef struct st_pq_func {
    bool inited;
    void *handle;
    int (*ComputePQTable)(VectorArray samples, PQParams *params);
    int (*ComputeVectorPQCode)(float *vector, const PQParams *params, uint8 *pqCode);
    int (*GetPQDistanceTableSdc)(const PQParams *params, float *pqDistanceTable);
    int (*GetPQDistanceTableAdc)(float *vector, const PQParams *params, float *pqDistanceTable);
    int (*GetPQDistance)(const uint8 *basecode, const uint8 *querycode, const PQParams *params,
                         const float *pqDistanceTable, float *pqDistance);
} pq_func_t;
extern pq_func_t g_pq_func;

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

int PQInit();
void PQUinit();
#endif