/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT license.
 *
 * Portions Copyright (c) 2025 Huawei Technologies Co.,Ltd.
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * diskann.h
 *
 * IDENTIFICATION
 *        src/include/access/datavec/diskann.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef DISKANN_H
#define DISKANN_H

#include "postgres.h"
#include "utils/rel.h"
#include "fmgr.h"
#include "nodes/execnodes.h"
#include "access/datavec/vector.h"
#include "access/datavec/utils.h"
#include "access/amapi.h"

#define DISKANN_FUNC_NUM 4

#define DISKANN_VERSION 1
#define DISKANN_MAGIC_NUMBER 0x14FF1A7
#define DISKANN_PAGE_ID 0xFF84

/* Support functions */
#define DISKANN_DISTANCE_PROC 1
#define DISKANN_NORM_PROC 2
#define DISKANN_TYPE_INFO_PROC 3
#define DISKANN_KMEANS_NORMAL_PROC 4

#define DISKANN_MAX_DIM 2000
#define DISKANN_DEFAULT_MAX_DEGREE 9600
#define DISKANN_DEFAULT_MAX_ALPHA 1
#define DISKANN_MIN_INDEX_SIZE 16
#define DISKANN_MAX_INDEX_SIZE 1000
#define DISKANN_DEFAULT_INDEX_SIZE 100
#define DISKANN_MAX_DEGREE 96

#define FROZEN_POINT_SIZE 1
#define DISKANN_DISTANCE_THRESHOLD (1e-9)
#define INDEXINGMAXC 500
#define DISKANN_HEAPTIDS 10
#define DISKANN_METAPAGE_BLKNO 0
#define DISKANN_HEAD_BLKNO 1                            /* first element page */
#define DISKANN_PQTABLE_START_BLKNO 1                   /* pqtable start page */

#define DISKANN_DIS_L2 1
#define DISKANN_DIS_IP 2
#define DISKANN_DIS_COSINE 3

#ifdef DISKANN_BENCH
#define DiskAnnBench(name, code)                                                                             \
    do {                                                                                                     \
        instr_time start;                                                                                    \
        instr_time duration;                                                                                 \
        INSTR_TIME_SET_CURRENT(start);                                                                       \
        double result = (code);                                                                              \
        INSTR_TIME_SET_CURRENT(duration);                                                                    \
        INSTR_TIME_SUBTRACT(duration, start);                                                                \
        elog(INFO, "%s: %.3f ms, assign tuples num: %.0f", name, INSTR_TIME_GET_MILLISEC(duration), result); \
    } while (0)
#else
#define DiskAnnBench(name, code) (code)
#endif

#define DiskAnnPageGetMeta(page) ((DiskAnnMetaPageData*)PageGetContents(page))
#define DiskAnnPageGetNode(itup) ((DiskAnnNodePage)((char*)(itup) + IndexTupleSize(itup)))
#define DiskAnnPageGetOpaque(page) ((DiskAnnPageOpaque)PageGetSpecialPointer(page))
#define DiskAnnPageGetIndexTuple(page) ((IndexTuple)PageGetItem(page, PageGetItemId(page, FirstOffsetNumber)))

enum ParallelBuildFlag { CREATE_ENTRY_PAGE = 1, LINK = 2 };

struct Neighbor {
    unsigned id;
    float distance;
    bool expanded;
    ItemPointerData heaptids[DISKANN_HEAPTIDS];
    uint8 heaptidsLength;

    Neighbor() = default;

    Neighbor(unsigned id, float distance) : id{id}, distance{distance}, expanded(false)
    {}

    inline bool operator<(const Neighbor& other) const
    {
        return distance < other.distance || (distance == other.distance && id < other.id);
    }

    inline bool operator==(const Neighbor& other) const
    {
        return (id == other.id);
    }
};

typedef struct NeighborPriorityQueue {
public:
    size_t _size;
    size_t _capacity;
    size_t _cur;
    VectorList<Neighbor> _data;

    void reserve(size_t capacity)
    {
        if (capacity + 1 > _data.size()) {
            _data.reserve(capacity + 1);
        }
        _capacity = capacity;
    }

    void insert(const Neighbor& nbr)
    {
        if (_size == _capacity && _data[_size - 1] < nbr) {
            return;
        }

        size_t lo = 0;
        size_t hi = _size;
        while (lo < hi) {
            size_t mid = (lo + hi) >> 1;
            if (nbr < _data[mid]) {
                hi = mid;
                // Make sure the same id isn't inserted into the set
            } else if (_data[mid].id == nbr.id) {
                return;
            } else {
                lo = mid + 1;
            }
        }

        if (lo < _capacity) {
            std::memmove(&_data[lo + 1], &_data[lo], (_size - lo) * sizeof(Neighbor));
        }
        _data[lo] = {nbr.id, nbr.distance};
        _data[lo].id = nbr.id;
        _data[lo].distance = nbr.distance;
        _data[lo].expanded = false;
        _data[lo].heaptidsLength = nbr.heaptidsLength;
        for (int i = 0; i < nbr.heaptidsLength; i++) {
            _data[lo].heaptids[i] = nbr.heaptids[i];
        }
        if (_size < _capacity) {
            _size++;
        }
        if (lo < _cur) {
            _cur = lo;
        }
    }

    Neighbor closest_unexpanded()
    {
        _data[_cur].expanded = true;
        size_t pre = _cur;
        while (_cur < _size && _data[_cur].expanded) {
            _cur++;
        }
        return _data[pre];
    }

    bool has_unexpanded_node() const
    {
        return _cur < _size;
    }

    size_t size() const
    {
        return _size;
    }

    size_t capacity() const
    {
        return _capacity;
    }

    void remove(size_t idx)
    {
        if (_size > idx + 1) {
            errno_t rc = memmove_s(&_data[idx], ((_size - idx) - 1) * sizeof(Neighbor), &_data[idx + 1],
                                   ((_size - idx) - 1) * sizeof(Neighbor));
            securec_check(rc, "\0", "\0");
        }
        _size--;
        if (_cur > idx) {
            _cur--;
        }
        while (_cur < _size && _data[_cur].expanded) {
            _cur++;
        }
    }

    Neighbor& operator[](size_t i)
    {
        return _data[i];
    }

    Neighbor operator[](size_t i) const
    {
        return _data[i];
    }

    void clear()
    {
        _size = 0;
        _cur = 0;
    }
} NeighborPriorityQueue;

struct QueryScratch {
    NeighborPriorityQueue* bestLNodes;
    float* alignedQuery;
    double sqrSum;
    HTAB* insertedNodeHash;
};

typedef struct DiskAnnTypeInfo {
    int maxDimensions;
    bool supportPQ;
    Size (*itemSize)(int dimensions);
    Datum (*normalize)(PG_FUNCTION_ARGS);
    void (*checkValue)(Pointer v);
} DiskAnnTypeInfo;

/* DiskAnn index options */
typedef struct DiskAnnOptions {
    StdRdOptions* rd_options;
    int indexSize;
    bool enablePQ;
    int pqM;    /* number of subquantizer */
    int pqKsub; /* number of centroids for each subquantizer */
} DiskAnnOptions;

typedef struct DiskAnnEdgePageData {
    uint8 type;
    uint16 count;
    BlockNumber nexts[DISKANN_MAX_DEGREE];
    float distance[DISKANN_MAX_DEGREE];
} DiskAnnEdgePageData;
typedef DiskAnnEdgePageData* DiskAnnEdgePage;

struct DiskAnnGraphStore : public BaseObject {
    DiskAnnGraphStore(Relation index);
    ~DiskAnnGraphStore();

    void GetVector(BlockNumber blkno, float* vec, double* sqrSum, ItemPointerData* hctid) const;
    float GetDistance(BlockNumber blk1, BlockNumber blk2) const;
    float ComputeDistance(BlockNumber blk1, float* vec, double sqrSum) const;
    void GetNeighbors(BlockNumber blkno, VectorList<Neighbor>* nbrs);
    void AddNeighbor(DiskAnnEdgePage edge, BlockNumber id, float distance) const;
    void FlushEdge(DiskAnnEdgePage edge, BlockNumber id) const;
    bool ContainsNeighbors(BlockNumber src, BlockNumber blk) const;
    void AddDuplicateNeighbor(BlockNumber src, ItemPointerData tid);
    bool NeighborExists(const DiskAnnEdgePage edge, BlockNumber id) const;
    void Clear() const;

    Relation m_rel;
    uint32 m_nodeSize;
    uint32 m_edgeSize;
    uint32 m_itemSize;
    double m_dimension;
};

class DiskAnnGraph : public BaseObject {
public:
    DiskAnnGraph(Relation rel, double dim, BlockNumber blkno, DiskAnnGraphStore* graphStore);
    ~DiskAnnGraph();
    void Link(BlockNumber blk, int indexSize);
    void IterateToFixedPoint(BlockNumber blk, const uint32 Lsize, BlockNumber frozen, VectorList<Neighbor>* pool,
                             bool search_invocatio);
    void PruneNeighbors(BlockNumber blk, VectorList<Neighbor>* pool, VectorList<Neighbor>* pruned_list);

    void OccludeList(BlockNumber location, VectorList<Neighbor>* pool, VectorList<Neighbor>* result, const float alpha);

    void InterInsert(BlockNumber blk, VectorList<Neighbor>* pruned_list);
    bool FindDuplicateNeighbor(NeighborPriorityQueue* bestLNodes, BlockNumber blk);
    void Clear();

private:
    int functype;
    DiskAnnGraphStore* graphStore = NULL;
    QueryScratch* scratch;
    BlockNumber frozen;
    bool saturateGraph = false;
};

typedef struct DiskAnnShared {
    /* Immutable state */
    Oid heaprelid;
    Oid indexrelid;
    char* pqTable;

    /* Mutex for mutable state */
    slock_t mutex;

    VectorList<BlockNumber> blocksList;

    BufferAccessStrategy parallelStrategy;

    /* Mutable state */
    int nparticipantsdone;
    double reltuples;

    int parallelWorker;
    pg_atomic_uint32 workers;
    ParallelBuildFlag flag;
    uint32 indexSize;
    BlockNumber frozen;
    uint16 dimensions;

    ParallelHeapScanDescData heapdesc;
} DiskAnnShared;

typedef struct DiskAnnLeader {
    int nparticipanttuplesorts;
    DiskAnnShared* diskannshared;
} DiskAnnLeader;

typedef struct DiskAnnBuildState {
    /* Info */
    Relation heap;
    Relation index;
    IndexInfo* indexInfo;
    const DiskAnnTypeInfo* typeInfo;

    /* Settings */
    uint16 dimensions;
    uint32 indexSize;

    /* Statistics */
    double indtuples;
    double reltuples;

    /* Parallel builds */
    DiskAnnLeader* diskannleader;
    DiskAnnShared* diskannshared;

    /* Support functions */
    FmgrInfo* procinfo;
    FmgrInfo* normprocinfo;
    FmgrInfo* kmeansnormprocinfo;
    Oid collation;

    VectorList<BlockNumber> blocksList;
    DiskAnnGraphStore* graphStore;

    uint32 nodeSize;
    uint32 edgeSize;
    uint32 itemSize;

    /* PQ info */
    bool enablePQ;
    int pqM;
    int pqKsub;
    char* pqTable;
    Size pqTableSize;
    float* pqDistanceTable;
    uint16 pqcodeSize;
    PQParams* params;
    VectorArray samples;
    BlockSamplerData bs;
    double rstate;
    int rowstoskip;

    /* Memory */
    MemoryContext tmpCtx;
} DiskAnnBuildState;

typedef struct DiskAnnPageOpaqueData {
    BlockNumber nextblkno;
    uint8 pageType;
    uint16 unused;
    uint16 pageId; /* for identification of DiskAnn indexes */
} DiskAnnPageOpaqueData;
typedef DiskAnnPageOpaqueData* DiskAnnPageOpaque;

typedef struct DiskAnnMetaPageData {
    uint32 magicNumber;
    uint32 version;
    uint32 nodeSize;
    uint32 itemSize;
    uint32 edgeSize;
    uint32 indexSize;
    uint32 pqTableSize;
    uint32 pqDisTableSize;
    BlockNumber insertPage;

    uint16 dimensions;
    uint16 nfrozen;
    uint16 pqM;
    uint16 pqKsub;
    uint16 pqcodeSize;
    uint16 pqTableNblk;
    uint16 pqDisTableNblk;
    bool enablePQ;
    BlockNumber frozenBlkno[FROZEN_POINT_SIZE];
} DiskAnnMetaPageData;
typedef DiskAnnMetaPageData* DiskAnnMetaPage;

typedef struct DiskAnnNodePageData {
    uint8 type;
    uint8 deleted;
    uint16 len;
    uint16 res;
    double sqrSum;
    ItemPointerData heaptids[DISKANN_HEAPTIDS];
    uint8 heaptidsLength;
    uint8 pqcode[FLEXIBLE_ARRAY_MEMBER];
} DiskAnnNodePageData;
typedef DiskAnnNodePageData* DiskAnnNodePage;

Datum diskannhandler(PG_FUNCTION_ARGS);
Datum diskannbuild(PG_FUNCTION_ARGS);
Datum diskannbuildempty(PG_FUNCTION_ARGS);
Datum diskanninsert(PG_FUNCTION_ARGS);
Datum diskannbulkdelete(PG_FUNCTION_ARGS);
Datum diskannvacuumcleanup(PG_FUNCTION_ARGS);
Datum diskanncostestimate(PG_FUNCTION_ARGS);
Datum diskannoptions(PG_FUNCTION_ARGS);
Datum diskannvalidate(PG_FUNCTION_ARGS);
Datum diskannbeginscan(PG_FUNCTION_ARGS);
Datum diskannrescan(PG_FUNCTION_ARGS);
Datum diskanngettuple(PG_FUNCTION_ARGS);
Datum diskannendscan(PG_FUNCTION_ARGS);

Datum DiskAnnNormValue(const DiskAnnTypeInfo* typeInfo, Oid collation, Datum value);
bool DiskAnnCheckNorm(FmgrInfo* procinfo, Oid collation, Datum value);
const DiskAnnTypeInfo* DiskAnnGetTypeInfo(Relation index);
FmgrInfo* DiskAnnOptionalProcInfo(Relation index, uint16 procnum);
void DiskAnnInitPage(Page page, Size pagesize);
Page DiskAnnInitRegisterPage(Relation index, Buffer buf);
void DiskAnnUpdateMetaPage(Relation index, BlockNumber blkno, ForkNumber forkNum);
void InsertFrozenPoint(Relation index, BlockNumber frozen);
float ComputeL2DistanceFast(const float* u, const double su, const float* v, const double sv, uint16_t dim);
void GetEdgeTuple(DiskAnnEdgePage tup, BlockNumber blkno, Relation idx, uint32 nodeSize, uint32 edgeSize);
int CmpNeighborInfo(const void* a, const void* b);
void DiskANNGetMetaPageInfo(Relation index, DiskAnnMetaPage meta);
int DiskAnnComputePQTable(VectorArray samples, PQParams* params);
int DiskAnnComputeVectorPQCode(float* vector, const PQParams* params, uint8* pqCode);
int DiskAnnGetPQDistanceTable(float* vector, const PQParams* params, float* pqDistanceTable);
int DiskAnnGetPQDistance(const uint8* basecode, const PQParams* params, const float* pqDistanceTable,
                         float* pqDistance);
bool DiskAnnEnablePQ(Relation index);
int DiskAnnGetPqM(Relation index);
int DiskAnnGetPqKsub(Relation index);
void DiskAnnGetPQInfoFromMetaPage(Relation index, uint16* pqTableNblk, uint32* pqTableSize, uint16* pqDisTableNblk,
                                  uint32* pqDisTableSize);
void DiskAnnFlushPQInfoInternal(Relation index, char* table, BlockNumber startBlkno, uint32 nblks, uint64 totalSize);
void DiskAnnFlushPQInfo(DiskAnnBuildState* buildstate);
PQParams* GetPQInfo(DiskAnnBuildState* buildstate);

IndexBuildResult* diskannbuild_internal(Relation heap, Relation index, IndexInfo* indexInfo);
void diskannbuildempty_internal(Relation index);
bool diskanninsert_internal(Relation index, Datum* values, const bool* isnull, ItemPointer heap_tid, Relation heap,
                            IndexUniqueCheck checkUnique);
IndexBulkDeleteResult* diskannbulkdelete_internal(IndexVacuumInfo* info, IndexBulkDeleteResult* stats,
                                                  IndexBulkDeleteCallback callback, void* callbackState);
IndexBulkDeleteResult* diskannvacuumcleanup_internal(IndexVacuumInfo* info, IndexBulkDeleteResult* stats);
IndexScanDesc diskannbeginscan_internal(Relation index, int nkeys, int norderbys);
void diskannrescan_internal(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys);
bool diskanngettuple_internal(IndexScanDesc scan, ScanDirection dir);
void diskannendscan_internal(IndexScanDesc scan);
#endif
