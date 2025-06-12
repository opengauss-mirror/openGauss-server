#ifndef UTILS_H
#define UTILS_H
#include "postgres.h"
#include "fmgr/fmgr_comp.h"
#include "access/multi_redo_api.h"
#include <vector>

#define GENERIC_DEFAULT_ENABLE_PQ false
#define GENERIC_DEFAULT_USE_MMAP false
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

typedef struct st_npu_func {
    bool inited;
    void *handle;
    int (*InitNPU)(int *useNPUDevices, int devNum);
    int (*MatrixMulOnNPU)(float *matrixA, float *matrixB, float *resMatrix, int paramM, int paramN, int paramK,
        uint8_t **matrixACacheAddr, int DevIdx, bool cacheMatrixA);
    int (*ReleaseNPU)(void);
    void (*ReleaseNPUCache)(uint8_t **cacheAddr, int DevIdx);
} npu_func_t;
extern npu_func_t g_npu_func;

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

typedef struct MmapShmemVal {
    BlockNumber blockNum;
    bool isMmap;
    void* mptr = NULL;
} MmapShmemVal;

typedef struct MmapShmem {
    BufferTag key;
    int mfd;
    size_t totalSize;       // file size
    uint32 mmapPage;        // Page size
    BlockNumber numPerPage; // total BlockNumber per page size
    uint32 maxBlock;        // maxBlock =  totalSize / 8K
    bool isUStore;
    bool isInit =  false;
    MmapShmemVal* mMate = NULL;
    MmapShmemVal mShmem[RELSEG_SIZE];
    MmapShmem* next;
} MmapShmem;

#define MAX_MMAP_BACKENDS 128
#define NUM_MMAP_PARTITIONS (NUM_BUFFER_PARTITIONS / MAX_MMAP_BACKENDS)
#define MMAP_FILE_MAX_SIZE (RELSEG_SIZE * BLCKSZ)
#ifdef __x86_64__
#define MMAP_PAGE_SIZE 0
#else
#define MMAP_PAGE_SIZE sysconf(_SC_PAGESIZE)
#endif

extern uint32 g_mmapOff;
static inline uint32 InitMmapOff()
{
    if (unlikely(MMAP_PAGE_SIZE == 0)) {
        return 0;
    }
    uint32 ret = 0;
    if (MMAP_PAGE_SIZE < BLCKSZ) {
        ret = ((BLCKSZ % MMAP_PAGE_SIZE) == 0) ? BLCKSZ : 0;
    } else {
        ret = ((MMAP_PAGE_SIZE % BLCKSZ) == 0) ? MMAP_PAGE_SIZE : 0;
    }
    return ret;
}
extern void MmapShmemInit(void);
Size MmapShmemSize();
void InitParamsMetaPage(Relation index, PQParams* params, bool* enablePQ, bool trymmap);
void GetMMapMetaPageInfo(Relation index, int* m, void** entryPoint);
bool IsRelnodeMmapLoad(Oid relNode);
bool IsDBnodeMmapLoad(Oid dbNode);
#endif