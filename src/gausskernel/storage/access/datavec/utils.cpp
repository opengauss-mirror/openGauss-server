/*
 * Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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
 * utils.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/utils.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/builtins.h"
#include "access/datavec/utils.h"
#include "access/datavec/halfutils.h"
#include "access/datavec/halfvec.h"
#include "access/datavec/bitvec.h"
#include "access/datavec/vector.h"
#include "access/datavec/hnsw.h"
#include "utils/dynahash.h"

pq_func_t g_pq_func = {0};
uint32 g_mmapOff = 0;
uint32 g_mmap_relNode = 0;
uint32 g_mmap_dbNode = 0;
LWLock g_mmap_nodeLock;

Size VectorItemSize(int dimensions)
{
    return VECTOR_SIZE(dimensions);
}

Size HalfvecItemSize(int dimensions)
{
    return HALFVEC_SIZE(dimensions);
}

Size BitItemSize(int dimensions)
{
    return VARBITTOTALLEN(dimensions);
}

void VectorUpdateCenter(Pointer v, int dimensions, const float *x)
{
    Vector *vec = (Vector *)v;

    SET_VARSIZE(vec, VECTOR_SIZE(dimensions));
    vec->dim = dimensions;

    for (int k = 0; k < dimensions; k++) {
        vec->x[k] = x[k];
    }
}

void HalfvecUpdateCenter(Pointer v, int dimensions, const float *x)
{
    HalfVector *vec = (HalfVector *)v;

    SET_VARSIZE(vec, HALFVEC_SIZE(dimensions));
    vec->dim = dimensions;

    for (int k = 0; k < dimensions; k++) {
        vec->x[k] = Float4ToHalfUnchecked(x[k]);
    }
}

void BitUpdateCenter(Pointer v, int dimensions, const float *x)
{
    VarBit *vec = (VarBit *)v;
    unsigned char *nx = VARBITS(vec);

    SET_VARSIZE(vec, VARBITTOTALLEN(dimensions));
    VARBITLEN(vec) = dimensions;

    for (uint32 k = 0; k < VARBITBYTES(vec); k++) {
        nx[k] = 0;
    }

    for (int k = 0; k < dimensions; k++) {
        nx[k / 8] |= (x[k] > 0.5 ? 1 : 0) << (7 - (k % 8));
    }
}

void VectorSumCenter(Pointer v, float *x)
{
    Vector *vec = (Vector *)v;

    for (int k = 0; k < vec->dim; k++) {
        x[k] += vec->x[k];
    } 
}

void HalfvecSumCenter(Pointer v, float *x)
{
    HalfVector *vec = (HalfVector *)v;

    for (int k = 0; k < vec->dim; k++) {
        x[k] += HalfToFloat4(vec->x[k]);
    }
}

void BitSumCenter(Pointer v, float *x)
{
    VarBit *vec = (VarBit *)v;

    for (int k = 0; k < VARBITLEN(vec); k++) {
        x[k] += (float)(((VARBITS(vec)[k / 8]) >> (7 - (k % 8))) & 0x01);
    }
}

/*
 * Allocate a vector array
 */
VectorArray VectorArrayInit(int maxlen, int dimensions, Size itemsize)
{
    VectorArray res = (VectorArray)palloc(sizeof(VectorArrayData));

    /* Ensure items are aligned to prevent UB */
    itemsize = MAXALIGN(itemsize);

    res->length = 0;
    res->maxlen = maxlen;
    res->dim = dimensions;
    res->itemsize = itemsize;
    res->items = (char *)palloc0_huge(CurrentMemoryContext, maxlen * itemsize);
    return res;
}

/*
 * Free a vector array
 */
void VectorArrayFree(VectorArray arr)
{
    if (arr->items != NULL) {
        pfree(arr->items);
    }
    pfree(arr);
}

Size MmapShmemSize() 
{
    ereport(WARNING, (errmsg("MMap is on")));
    g_mmapOff = InitMmapOff();
    LWLockInitialize(&g_mmap_nodeLock, 0);
    return hash_estimate_size(MAX_MMAP_BACKENDS + NUM_MMAP_PARTITIONS, sizeof(MmapShmem));
}

void MmapShmemInit(void)
{
    if (g_instance.attr.attr_storage.enable_mmap) {
        HASHCTL info;
        info.keysize = sizeof(BufferTag);
        info.entrysize = sizeof(MmapShmem);
        info.hash = tag_hash;
        info.num_partitions = NUM_MMAP_PARTITIONS;
        t_thrd.storage_cxt.ShmemMmap = ShmemInitHash("Mmap Shared Buffer", MAX_MMAP_BACKENDS + NUM_MMAP_PARTITIONS, 
                                                    MAX_MMAP_BACKENDS + NUM_MMAP_PARTITIONS, &info, HASH_ELEM | HASH_FUNCTION | HASH_PARTITION);
    }
}

static MmapShmem* MMapLookup(BufferTag* tag, uint32 hashcode)
{
    return (MmapShmem *)buf_hash_operate<HASH_FIND>(t_thrd.storage_cxt.ShmemMmap, tag, hashcode, NULL);
}

static MmapShmem* MMapInsert(BufferTag *tag, uint32 hashcode)
{
    MmapShmem *result = NULL;
    PG_TRY();
    {
        bool found = false;
        result = (MmapShmem *)buf_hash_operate<HASH_ENTER>(t_thrd.storage_cxt.ShmemMmap, tag, hashcode, &found);

    }
    PG_CATCH();
    {
        ereport(LOG, (errmsg("MMapInsert error")));
    }
    PG_END_TRY();
    return result;
}

static void MMapDelete(BufferTag *tag, uint32 hashcode)
{
    MmapShmem *result = (MmapShmem *)buf_hash_operate<HASH_REMOVE>(t_thrd.storage_cxt.ShmemMmap, tag, hashcode, NULL);

    if (result == NULL) { /* shouldn't happen */
        ereport(LOG, (errcode(ERRCODE_DATA_CORRUPTED), (errmsg("MMapDelete buffer hash table corrupted."))));
    }
    return;
}

static bool CanUseMmap(Relation index)
{
    bool result = g_instance.attr.attr_storage.enable_mmap && u_sess->datavec_ctx.hnsw_use_mmap &&
                    g_mmapOff != 0 && (g_mmap_relNode == 0 || index->rd_node.relNode == g_mmap_relNode);
    return result;
}

bool IsRelnodeMmapLoad(Oid relNode)
{
    return g_instance.attr.attr_storage.enable_mmap && g_mmap_relNode == relNode;
}
bool IsDBnodeMmapLoad(Oid dbNode)
{
    return g_instance.attr.attr_storage.enable_mmap && g_mmap_dbNode == dbNode;
}

static BlockNumber GetMMapBlocks()
{
    return g_mmapOff / BLCKSZ;
}

static bool MmapBlock(MmapShmem* sMmap, BlockNumber block_num)
{
    if (block_num >= sMmap->maxBlock) {
        ereport(LOG, (errmsg("MmapBlock block[%d] overflow maxBlock[%d]", block_num, sMmap->maxBlock)));
        return false;
    }
    if (sMmap->mShmem[block_num].mptr != NULL) {
        return true;
    }
    BlockNumber block_page = block_num / sMmap->numPerPage;
    BlockNumber block_page_head = block_page * sMmap->numPerPage;
    BufferTag tag = sMmap->key;
    tag.blockNum = block_page_head;
    uint32 new_hash = BufTableHashCode(&tag);
    LWLock* new_partition_lock = BufMappingPartitionLock(new_hash);
    LWLockAcquire(new_partition_lock, LW_EXCLUSIVE);

    if (sMmap->mShmem[block_num].mptr != NULL) {
        LWLockRelease(new_partition_lock);
        return true;
    }

    off_t off = block_page_head * BLCKSZ;
    size_t filespace = sMmap->totalSize - off;
    size_t len = (filespace < sMmap->mmapPage) ? filespace : sMmap->mmapPage;
    void* headprt = mmap(NULL, len, PROT_READ, MAP_PRIVATE, sMmap->mfd, off);
    if (headprt == MAP_FAILED) {
        LWLockRelease(new_partition_lock);
        ereport(LOG, (errmsg("mmap block[%d] error[%d] msg[%s]", block_num, errno, strerror(errno))));
        return false;
    }
    off += BLCKSZ;
    for(int i = 1; i < sMmap->numPerPage; i++) {
        if (off < sMmap->totalSize) {
            BlockNumber tmp = block_page_head + i;
            sMmap->mShmem[tmp].mptr = headprt + BLCKSZ * i;
            sMmap->mShmem[tmp].blockNum = tmp;
            sMmap->mShmem[tmp].isMmap = false;
            off += BLCKSZ;
            continue;
        }
        break;
    }
    sMmap->mShmem[block_page_head].mptr = headprt;
    sMmap->mShmem[block_page_head].blockNum = block_page_head;
    sMmap->mShmem[block_page_head].isMmap = true;
    LWLockRelease(new_partition_lock);
    return true;
}
static bool MmapInitMetaBlock(MmapShmem* sMmap)
{
    void* metaprt = NULL;
    if (sMmap->mMate == NULL) {
        size_t len = (sMmap->mmapPage < sMmap->totalSize) ? sMmap->mmapPage : sMmap->mmapPage;
        metaprt = mmap(NULL, len, PROT_READ, MAP_PRIVATE, sMmap->mfd, 0);
        if (metaprt == MAP_FAILED) {
            ereport(LOG, (errmsg("mmap MetaBlock error[%d] msg[%s]", errno, strerror(errno))));
            return false;
        }
        Page page = (Page)metaprt;
        sMmap->isUStore = HnswPageGetOpaque(page)->pageType == HNSW_USTORE_PAGE_TYPE;
        if (sMmap->isUStore) {
            return false;
        }
        
    }
    off_t off = BLCKSZ;
    for(int i = 1; i < sMmap->numPerPage; i++) {
        if (off < sMmap->totalSize) {
            sMmap->mShmem[i].mptr = metaprt + off;
            sMmap->mShmem[i].blockNum = i;
            sMmap->mShmem[i].isMmap = false;
            off += BLCKSZ;
            continue;
        }
        break;
    }
    sMmap->mShmem[0].blockNum = HNSW_METAPAGE_BLKNO;
    sMmap->mShmem[0].isMmap = true;
    sMmap->mShmem[0].mptr = metaprt;
    sMmap->mMate = &(sMmap->mShmem[0]);
    return true;
}

static char* GetPathbackend(RelFileNode rnode, BlockNumber segno)
{
    char *path = NULL;
    char *fullpath = NULL;
    int nRet = 0;
    path = relpathbackend(rnode, InvalidBackendId,  MAIN_FORKNUM);

    if (segno > 0) {
        /* be sure we have enough space for the '.segno' */
        fullpath = (char *)palloc(strlen(path) + 12);
        nRet = snprintf_s(fullpath, strlen(path) + 12, strlen(path) + 11, "%s.%u", path, segno);
        securec_check_ss(nRet, "", "");
        pfree(path);
    } else {
        fullpath = path;
    }

    return fullpath;
}
static bool LoadMmapFile(MmapShmem* result, BufferTag* tag, bool isMate, BlockNumber maxBlock)
{
    // open file
    BlockNumber blockNum = isMate ? 0 : tag->blockNum;
    char *path = GetPathbackend(tag->rnode, blockNum);
    uint32 flags =  PG_BINARY | O_RDONLY;
    result->mfd = open(path, flags, 0600);
    if (result->mfd < 0) {
        ereport(LOG, (errmsg("mmap open file [%s] error[%d] msg[%s]", path, errno, strerror(errno))));
        return false;
    }
    pfree(path);
    // check len
    off_t len = lseek(result->mfd, 0L, SEEK_END);
    if (len < BLCKSZ || len > MMAP_FILE_MAX_SIZE || len < (maxBlock + 1) * BLCKSZ) {
        ereport(LOG, (errmsg("mmap lseek file [%s] len[%d] block[%d]", path, len, maxBlock)));
        close(result->mfd);
        return false;
    }
    // save valuse
    result->key = *tag;
    result->totalSize = len;
    result->numPerPage = GetMMapBlocks();
    result->mmapPage = g_mmapOff;
    result->maxBlock = len / BLCKSZ;
    result->next = NULL;
    if (isMate) {
        if (MmapInitMetaBlock(result) == false) {
            close(result->mfd);
            return false;
        }
    }
    result->isInit = true;
    ereport(LOG, (errmsg("New mmap file[%s] size[%d] maxBlock[%d]", path, len, result->maxBlock)));
    return true;
}

static bool RoadMainMmapPage(BufferTag* tag, BlockNumber block_num, char** page)
{
    // MainMmp only support block_num < 131072
    if (block_num >= RELSEG_SIZE || block_num < HNSW_METAPAGE_BLKNO) {
        ereport(LOG, (errmsg("RoadMainMmapPage block[%d]", block_num)));
        return false;
    }
    uint32 new_hash = BufTableHashCode(tag);
    MmapShmem* result = MMapLookup(tag, new_hash);
    while (result == NULL || result->mMate == NULL) {
        LWLock* new_partition_lock = BufMappingPartitionLock(new_hash);
        LWLockAcquire(new_partition_lock, LW_EXCLUSIVE);
        if ((result = MMapLookup(tag, new_hash)) != NULL) {
            LWLockRelease(new_partition_lock);
            continue;
        }
        LWLockAcquire(&g_mmap_nodeLock, LW_EXCLUSIVE);
        if (g_mmap_relNode != 0 && g_mmap_relNode != tag->rnode.relNode) {
            ereport(LOG, (errmsg("mmap error. MMap is loaded by node[%d:%d], cur node[%d:%d]",
                g_mmap_dbNode, g_mmap_relNode, tag->rnode.dbNode, tag->rnode.relNode)));
            LWLockRelease(&g_mmap_nodeLock);
            LWLockRelease(new_partition_lock);
            return false;
        }
        result = MMapInsert(tag, new_hash);
        if (result == NULL) {
            LWLockRelease(&g_mmap_nodeLock);
            LWLockRelease(new_partition_lock);
            return false;
        }
        if (LoadMmapFile(result, tag, true, block_num) == false) {
            MMapDelete(tag, new_hash);
            LWLockRelease(&g_mmap_nodeLock);
            LWLockRelease(new_partition_lock);
            return false;
        }
        if (g_mmap_relNode == 0) {
            g_mmap_relNode = tag->rnode.relNode;
            g_mmap_dbNode = tag->rnode.dbNode;
            ereport(LOG, (errmsg("New MMap is init by node[%d:%d]", g_mmap_dbNode, g_mmap_relNode)));
        }
        LWLockRelease(&g_mmap_nodeLock);
        LWLockRelease(new_partition_lock);
    }
    if (block_num == HNSW_METAPAGE_BLKNO) {
        *page = (char*)result->mMate->mptr;
        return *page != NULL;
    }
    if (MmapBlock(result, block_num)) {
        *page = (char*)result->mShmem[block_num].mptr;
    }
    
    return *page != NULL;
}

static bool RoadMmapPage(BufferTag* tag, BlockNumber block_num, char** page)
{
    uint32 new_hash = BufTableHashCode(tag);
    MmapShmem* result = MMapLookup(tag, new_hash);
    BlockNumber target_block = block_num % RELSEG_SIZE;
    while (result == NULL || result->isInit == false) {
        LWLock* new_partition_lock = BufMappingPartitionLock(new_hash);
        LWLockAcquire(new_partition_lock, LW_EXCLUSIVE);
        if ((result = MMapLookup(tag, new_hash)) != NULL) {
            LWLockRelease(new_partition_lock);
            continue;
        }
        result = MMapInsert(tag, new_hash);
        if (result == NULL) {
            LWLockRelease(new_partition_lock);
            return false;
        }
        if (LoadMmapFile(result, tag, false, target_block) == false) {
            MMapDelete(tag, new_hash);
            LWLockRelease(new_partition_lock);
            return false;
        }
        LWLockRelease(new_partition_lock);
    }
    if (MmapBlock(result, target_block)) {
        *page = (char*)result->mShmem[target_block].mptr;
    }
    
    return *page != NULL;

}
static void* GetMMapMetaPage(Relation index)
{
    BufferTag  new_tag;
    Page page = NULL;
    new_tag.rnode = index->rd_node;
    new_tag.forkNum = MAIN_FORKNUM;
    new_tag.blockNum = HNSW_METAPAGE_BLKNO;
    if (RoadMainMmapPage(&new_tag, HNSW_METAPAGE_BLKNO, (char**)&page)) { 
        return (void*)HnswPageGetMeta(page);
    }
    return NULL;
}
static void* GetMMapPage(Relation index, BlockNumber block_num)
{
    if (!CanUseMmap(index) || index->rd_backend != InvalidBackendId) {
        return NULL;
    }
    BufferTag  new_tag;
    Page page = NULL;
    new_tag.rnode = index->rd_node;
    new_tag.forkNum = MAIN_FORKNUM; 
    new_tag.blockNum = block_num / RELSEG_SIZE;
    if (block_num < RELSEG_SIZE) {
        if (RoadMainMmapPage(&new_tag, block_num, (char**)&page)) { 
            return (void*)page;
        } 
        return NULL;
    }
    if (RoadMmapPage(&new_tag, block_num, (char**)&page)) { 
        return (void*)page;
    }
    return NULL;

    
}

void InitParamsMetaPage(Relation index, PQParams* params, bool* enablePQ, bool trymmap)
{
    if (trymmap && index->rd_backend == InvalidBackendId && HnswGetEnableMMap(index) && CanUseMmap(index)) {
        HnswMetaPage metap = (HnswMetaPage)GetMMapMetaPage(index);
        if (metap != NULL) {
            *enablePQ = metap->enablePQ;
            params->pqM = metap->pqM;
            params->pqKsub = metap->pqKsub;
            return;
        }
    }
    Buffer buf = ReadBuffer(index, HNSW_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    Page page = BufferGetPage(buf);
    HnswMetaPage metap = HnswPageGetMeta(page);
    *enablePQ = metap->enablePQ;
    params->pqM = metap->pqM;
    params->pqKsub = metap->pqKsub;
    UnlockReleaseBuffer(buf);
    return;
}

void GetMMapMetaPageInfo(Relation index, int* m, void** entryPoint)
{
    if (index->rd_backend == InvalidBackendId && CanUseMmap(index)) {
        HnswMetaPage metap = (HnswMetaPage)GetMMapMetaPage(index);
        if (metap != NULL) {
            if (unlikely(metap->magicNumber != HNSW_MAGIC_NUMBER))
                elog(ERROR, "hnsw index is not valid");
            if (m != NULL)
                *m = metap->m;

            if (entryPoint != NULL) {
                if (BlockNumberIsValid(metap->entryBlkno)) {
                    *entryPoint = (void*)HnswInitElementFromBlock(metap->entryBlkno, metap->entryOffno);
                    HnswElement(*entryPoint)->level = metap->entryLevel;
                    HnswElement(*entryPoint)->fromMmap = true;
                } else {
                    *entryPoint = NULL;
                }
            }
            return;
        }
    }
    HnswGetMetaPageInfo(index, m, (HnswElement*)entryPoint);
    return;
}
bool MmapLoadElement(HnswElement element, float *distance, Datum *q, Relation index, FmgrInfo *procinfo, Oid collation,
                     bool loadVec, float *maxDistance, IndexScanDesc scan, bool enablePQ, PQSearchInfo *pqinfo)
{
    
    HnswElementTuple etup;
    uint8 *ePQCode;
    PQParams *params;
    Page page = (Page)GetMMapPage(index, element->blkno);
    if (page == NULL) {
        return HnswLoadElement(element, distance, q, index, procinfo,
                                         collation, loadVec, maxDistance, scan, enablePQ, pqinfo);
    }

    etup = (HnswElementTuple)PageGetItem(page, PageGetItemId(page, element->offno));

    Assert(HnswIsElementTuple(etup));

    /* Calculate distance */
    if (distance != NULL) {
        if (enablePQ && pqinfo->lc == 0) {
            ePQCode = LoadPQcode(etup);
            params = &pqinfo->params;
            if (pqinfo->pqMode == HNSW_PQMODE_SDC && *pqinfo->qPQCode == NULL) {
                *distance = 0;
            } else if (pqinfo->pqMode == HNSW_PQMODE_ADC && pqinfo->pqDistanceTable == NULL) {
                *distance = 0;
            } else {
                GetPQDistance(ePQCode, pqinfo->qPQCode, params, pqinfo->pqDistanceTable, distance);
            }
        } else {
            if (DatumGetPointer(*q) == NULL) {
                *distance = 0;
            } else {
                *distance = (float)DatumGetFloat8(FunctionCall2Coll(
                            procinfo, collation, *q, PointerGetDatum(&etup->data)));
            }
        }
    }

    /* Load element */
    if (distance == NULL || maxDistance == NULL || *distance < *maxDistance) {
        HnswLoadElementFromTuple(element, etup, true, loadVec);
        if (enablePQ) {
            params = &pqinfo->params;
            Vector *vd1 = &etup->data;
            Vector *vd2 = (Vector *)DatumGetPointer(*q);
            float exactDis;
            if (pqinfo->params.funcType == HNSW_PQ_DIS_IP) {
                exactDis = -VectorInnerProduct(params->dim, vd1->x, vd2->x);
            } else {
                exactDis = VectorL2SquaredDistance(params->dim, vd1->x, vd2->x);
            }
            *distance = exactDis;
        }
    }

    return true;
}


HnswCandidate *MMapEntryCandidate(char *base, HnswElement entryPoint, Datum q, Relation index, FmgrInfo *procinfo,
                                  Oid collation, bool loadVec, IndexScanDesc scan, bool enablePQ, PQSearchInfo *pqinfo)
{
    if (index == NULL || !entryPoint->fromMmap || !CanUseMmap(index)) {
        return HnswEntryCandidate(base, entryPoint, q, index, procinfo, collation, loadVec, scan, enablePQ, pqinfo);
    }
    HnswCandidate *hc = (HnswCandidate *)palloc(sizeof(HnswCandidate));

    HnswPtrStore(base, hc->element, entryPoint);

    MmapLoadElement(entryPoint, &hc->distance, &q, index, procinfo,
                                     collation, loadVec, NULL, scan, enablePQ, pqinfo);
    return hc;
}


void HnswLoadUnvisitedFromMmap(HnswElement element, HnswElement *unvisited, int *unvisitedLength,
                          VisitedHash *v, Relation index, int m, int lm, int lc)
{
    HnswNeighborTuple ntup;
    int start;
    ItemPointerData indextids[HNSW_MAX_M * 2];
    Page page = (Page)GetMMapPage(index, element->neighborPage);
    if (page == NULL) {
        HnswLoadUnvisitedFromDisk(element, unvisited, unvisitedLength, v, index, m, lm, lc);
        return;
    }

    ntup = (HnswNeighborTuple)PageGetItem(page, PageGetItemId(page, element->neighborOffno));
    start = (element->level - lc) * m;

    /* Copy to minimize lock time */
    errno_t rc = memcpy_s(&indextids, lm * sizeof(ItemPointerData), ntup->indextids + start, lm * sizeof(ItemPointerData));
    securec_check(rc, "\0", "\0");

    *unvisitedLength = 0;

    for (int i = 0; i < lm; i++) {
        ItemPointer indextid = &indextids[i];
        bool found;

        if (!ItemPointerIsValid(indextid)) {
            break;
        }

        tidhash_insert(v->tids, *indextid, &found);

        if (!found) {
            unvisited[(*unvisitedLength)++] = HnswInitElementFromBlock(ItemPointerGetBlockNumber(indextid),
                                                                       ItemPointerGetOffsetNumber(indextid));
        }
    }
}