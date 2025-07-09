/*
 * Copyright (c) 2025 Huawei Technologies Co.,Ltd.
 *
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

#define DISKANN_FUNC_NUM  5

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
#define DISKANN_DEFAULT_INDEX_SIZE 16

#define OUTDEGREE 96
#define FROZEN_POINT_SIZE 1
#define DISKANN_HEAPTIDS 10
#define DISKANN_METAPAGE_BLKNO 0

#ifdef DISKANN_BENCH
#define DiskAnnBench(name, code)                                            \
    do {                                                                    \
        instr_time start;                                                   \
        instr_time duration;                                                \
        INSTR_TIME_SET_CURRENT(start);                                      \
        double result = (code);                                             \
        INSTR_TIME_SET_CURRENT(duration);                                   \
        INSTR_TIME_SUBTRACT(duration, start);                               \
        elog(INFO, "%s: %.3f ms, assign tuples num: %.0f", name, INSTR_TIME_GET_MILLISEC(duration), result); \
    } while (0)
#else
#define DiskAnnBench(name, code) (code)
#endif

#define DiskAnnPageGetMeta(page) ((DiskAnnMetaPageData*)PageGetContents(page))
#define DiskAnnPageGetNode(itup) ((DiskAnnNodePage)((char*)(itup) + IndexTupleSize(itup)))
#define DiskAnnPageGetOpaque(page) ((DiskAnnPageOpaque)PageGetSpecialPointer(page))

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
    int dimensions;
    int maxDegree;
    int maxAlpha;
    int indexSize;
} DiskAnnOptions;

struct DiskAnnGraphStore : public BaseObject {
    DiskAnnGraphStore(Relation index);
    ~DiskAnnGraphStore();

    void GetVector(BlockNumber blkno, float* vec, double* sqr_sum, ItemPointerData* hctid) const;
    Relation m_rel;
    uint32 m_nodeSize;
    uint32 m_edgeSize;
    uint32 m_itemSize;
    double m_dimension;
};

typedef struct DiskAnnBuildState {
    /* Info */
    Relation heap;
    Relation index;
    IndexInfo* indexInfo;
    const DiskAnnTypeInfo* typeInfo;

    /* Settings */
    uint16 dimensions;
    uint32 maxDegree;
    uint32 maxAlpha;
    uint32 indexSize;

    /* Statistics */
    double indtuples;
    double reltuples;

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
    uint16 dimensions;
    uint32 maxDegree;
    uint32 maxAlpha;
    uint32 nodeSize;
    uint32 itemSize;
    uint32 edgeSize;
    uint32 indexSize;
    uint16 nfrozen;
    BlockNumber insertPage;
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
} DiskAnnNodePageData;
typedef DiskAnnNodePageData* DiskAnnNodePage;

typedef struct DiskAnnEdgePageData {
    uint8 type;
    uint16 count;
    BlockNumber nexts[OUTDEGREE];
    float distance[OUTDEGREE];
} DiskAnnEdgePageData;
typedef DiskAnnEdgePageData* DiskAnnEdgePage;

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
void DiskANNGetMetaPageInfo(Relation index, DiskAnnMetaPage meta);

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
