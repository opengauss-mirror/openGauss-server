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

template <typename T>
class VectorList {
public:
    typedef T* iterator;

public:
    VectorList()
    {
        this->p = (T*)palloc0(baseCapacity * sizeof(T));
        this->myCapacity = baseCapacity;
        this->mySize = 0;
    }

    void initialize_vector()
    {
        this->p = (T*)palloc0(baseCapacity * sizeof(T));
        this->myCapacity = baseCapacity;
        this->mySize = 0;
        return;
    }

    VectorList(uint32_t mySize)
    {
        this->myCapacity = baseCapacity + mySize;
        this->mySize = mySize;
        this->p = (T*)palloc0(myCapacity * sizeof(T));
    }

    /* caller needs to call clear() to free memory */
    ~VectorList()
    {}

    VectorList(const VectorList& v)
    {
        this->myCapacity = v.myCapacity;
        this->mySize = v.mySize;
        this->p = (T*)palloc0(this->myCapacity * sizeof(T));
        for (size_t i = 0; i < this->mySize; i++) {
            this->p[i] = v[i];
        }
    }

    VectorList(VectorList&& v)
    {
        this->myCapacity = v.myCapacity;
        this->mySize = v.mySize;
        this->p = v.p;
        v.p = nullptr;
        v.mySize = 0;
        v.myCapacity = 0;
    }

    void push_back(const T& data)
    {
        if (this->mySize == this->myCapacity) {
            this->p = (T*)repalloc(this->p, (this->myCapacity * capacityExpansionMultiplier) * sizeof(T));
            this->myCapacity *= capacityExpansionMultiplier;
        }
        errno_t rc = memcpy_s(this->p + this->mySize, sizeof(T), &data, sizeof(T));
        securec_check(rc, "", "");
        this->mySize++;
    }

    void reserve(size_t new_cap)
    {
        if (new_cap <= this->myCapacity) {
            return;
        }
        this->p = (T*)repalloc(this->p, new_cap * sizeof(T));
        this->myCapacity = new_cap;
    }

    void resize(size_t new_size)
    {
        while (this->mySize > new_size) {
            pop();
        }
        while (this->mySize < new_size) {
            push_back(T());
        }
    }

    void removeIndex(size_t i)
    {
        size_t tail_size = (size_t)(this->mySize - i - 1);
        if (tail_size > 0) {
            errno_t rc = memmove_s(this->p + i, sizeof(T) * tail_size, this->p + i + 1, sizeof(T) * tail_size);
            securec_check(rc, "", "");
        }
        this->mySize--;
    }

    void pop_back()
    {
        if (this->mySize > 1) {
            this->mySize--;
        }
    }

    void pop()
    {
        this->mySize--;
    }

    T& back()
    {
        return *(end() - 1);
    }

    const T& back() const
    {
        return this->p[this->size() - 1];
    }

    void clear()
    {
        this->myCapacity = 0;
        this->mySize = 0;
        pfree_ext(this->p);

        p = nullptr;
    }

    void reset()
    {
        this->mySize = 0;
    }

    bool empty()
    {
        return this->mySize == 0;
    }

    void insert(iterator pos, T data)
    {
        int32_t istPos = pos - this->begin();
        if (pos >= this->begin() && pos <= this->end()) {
            if (this->mySize == this->myCapacity) {
                T* new_p = (T*)palloc0((this->myCapacity + capacityExpansionIncrement) * sizeof(T));
                for (size_t i = 0; i < mySize; i++) {
                    new_p[i] = this->p[i];
                }
                this->myCapacity += capacityExpansionIncrement;

                pfree(this->p);

                this->p = new_p;
            }

            int32_t curPos = 0;
            for (curPos = mySize; curPos > istPos; curPos--) {
                this->p[curPos] = this->p[curPos - 1];
            }

            this->p[curPos] = data;
            this->mySize++;
        }
    }

    const T& operator[](uint32_t index) const
    {
        return this->p[index];
    }

    const T& operator[](size_t index) const
    {
        return this->p[index];
    }

    const T& operator[](int index) const
    {
        return this->p[index];
    }

    T& operator[](uint32_t index)
    {
        return this->p[index];
    }

    T& operator[](size_t index)
    {
        return this->p[index];
    }

    T& operator[](int index)
    {
        return this->p[index];
    }

    VectorList& operator=(const VectorList& v)
    {
        this->myCapacity = v.myCapacity;
        this->mySize = v.mySize;
        if (this->p != NULL) {
            pfree(this->p);
        }
        this->p = (T*)palloc0(this->myCapacity * sizeof(T));
        for (size_t i = 0; i < this->mySize; i++) {
            this->p[i] = v[i];
        }
        return *this;
    }

    VectorList& operator=(VectorList&& v)
    {
        this->myCapacity = v.myCapacity;
        this->mySize = v.mySize;
        this->p = v.p;
        v.p = nullptr;
        v.mySize = 0;
        v.myCapacity = 0;
        return *this;
    }

    size_t find(const T& x) const
    {
        for (size_t i = 0; i < this->mySize; i++) {
            if (this->p[i] == x) {
                return i;
            }
        }
        return (size_t)-1;
    }

    bool contains(const T& x) const
    {
        for (size_t i = 0; i < this->mySize; i++) {
            if (this->p[i] == x) {
                return true;
            }
        }
        return false;
    }

    size_t size() const
    {
        return this->mySize;
    }

    iterator end()
    {
        return this->p + this->size();
    }

    const iterator end() const
    {
        return this->p + this->size();
    }

    iterator begin()
    {
        return this->p;
    }

    const iterator begin() const
    {
        return this->p;
    }

public:
    T* p{nullptr};
    uint32_t myCapacity{0};
    uint32_t mySize{0};
    static const uint32_t baseCapacity = 16U;
    static const uint32_t capacityExpansionIncrement = 256U;
    static const uint32_t capacityExpansionMultiplier = 2;
};

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
        uint8_t **matrixACacheAddr, int devIdx, bool cacheMatrixA);
    int (*ReleaseNPU)(void);
    void (*ReleaseNPUCache)(uint8_t **cacheAddr, int devIdx);
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