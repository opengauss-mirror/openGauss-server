/*
* Copyright (c) 2022 Huawei Technologies Co.,Ltd.
*
* -------------------------------------------------------------------------
*/
#include "storage/cfs/cfs_buffers.h"
#include "storage/cfs/cfs_tools.h"
#include "storage/smgr/fd.h"
#include "storage/smgr/relfilenode.h"
#include "storage/cfs/cfs_repair.h"
#include "executor/executor.h"
#include "pgstat.h"

#define CB_CUSTOM_VALUE_TWO 2
#define CB_CUSTOM_VALUE_THREE1 3
#define CB_CUSTOM_VALUE_THREE2 3
#define CB_CUSTOM_VALUE_THREE3 3

pca_page_buff_ctx_t *g_pca_buf_ctx = NULL;

static inline uint32 pca_hashcode(CfsBufferKey *key)
{
    return g_pca_buf_ctx->hashtbl.hash((void *)key, sizeof(CfsBufferKey));
}

static inline uint32 pca_key_cmp(CfsBufferKey *key1, CfsBufferKey *key2)
{
    return (uint32)g_pca_buf_ctx->hashtbl.match((void *)key1, (void *)key2, sizeof(CfsBufferKey));
}

#define SIZE_K(n) ((Size)n * 1024)
#define SIZE_M(n) ((Size)n * SIZE_K(1024))
#define SIZE_G(n) ((Size)n * SIZE_M(1024))

pca_page_ctrl_t *pca_buf_find_from_bucket(pca_hash_bucket_t *bucket, CfsBufferKey *key)
{
    pca_page_ctrl_t *ctrl = NULL;

    uint32 ctrl_id = bucket->first_ctrl_id;
    while (ctrl_id != PCA_INVALID_ID) {
        ctrl = PCA_GET_CTRL_BY_ID(g_pca_buf_ctx, ctrl_id);
        if (pca_key_cmp(key, &ctrl->pca_key) == 0) {
            return ctrl;
        }
        ctrl_id = ctrl->bck_next;
    }

    return NULL;
}

void pca_buf_remove_from_bucket(pca_hash_bucket_t *bucket, pca_page_ctrl_t *item)
{
    pca_page_ctrl_t *prev = NULL;
    pca_page_ctrl_t *next = NULL;

    if (item->bck_prev != PCA_INVALID_ID) {
        prev = PCA_GET_CTRL_BY_ID(g_pca_buf_ctx, item->bck_prev);
        prev->bck_next = item->bck_next;
    } else {
        bucket->first_ctrl_id = item->bck_next;
    }

    if (item->bck_next != PCA_INVALID_ID) {
        next = PCA_GET_CTRL_BY_ID(g_pca_buf_ctx, item->bck_next);
        next->bck_prev = item->bck_prev;
    }

    item->bck_prev = PCA_INVALID_ID;
    item->bck_next = PCA_INVALID_ID;
    item->bck_id = PCA_INVALID_ID;

    item->pca_key = {0};  // reset the key

    bucket->ctrl_count--;
}

void pca_buf_add_into_bucket(pca_hash_bucket_t *bucket, pca_page_ctrl_t *item)
{
    if (bucket->ctrl_count == 0) {
        item->bck_id = bucket->bck_id;
        item->bck_prev = PCA_INVALID_ID;
        item->bck_next = PCA_INVALID_ID;

        bucket->first_ctrl_id = item->ctrl_id;
        bucket->ctrl_count++;
        return;
    }

    pca_page_ctrl_t *head = PCA_GET_CTRL_BY_ID(g_pca_buf_ctx, bucket->first_ctrl_id);

    item->bck_id = bucket->bck_id;
    item->bck_prev = PCA_INVALID_ID;
    item->bck_next = head->ctrl_id;

    head->bck_prev = item->ctrl_id;

    bucket->first_ctrl_id = item->ctrl_id;
    bucket->ctrl_count++;
}

void pca_lru_push_nolock(pca_lru_list_t *lru, pca_page_ctrl_t *item, ctrl_state_e state)
{
    pca_page_ctrl_t *temp = NULL;

    // insert into the head location.
    if (lru->count == 0) {
        item->lru_next = PCA_INVALID_ID;
        item->lru_prev = PCA_INVALID_ID;

        lru->first = item->ctrl_id;
        lru->last = item->ctrl_id;
        lru->count++;
        item->state = state;
        return ;
    }

    item->lru_next = lru->first;
    item->lru_prev = PCA_INVALID_ID;

    temp = PCA_GET_CTRL_BY_ID(g_pca_buf_ctx, lru->first);
    temp->lru_prev = item->ctrl_id;

    lru->first = item->ctrl_id;
    lru->count++;
    item->state = state;
}

void pca_lru_push(pca_lru_list_t *lru, pca_page_ctrl_t *item, ctrl_state_e state)
{
    // insert into the head location.
    (void)LWLockAcquire(lru->lock, LW_EXCLUSIVE);
    pca_lru_push_nolock(lru, item, state);
    LWLockRelease(lru->lock);
}

void pca_lru_remove(pca_lru_list_t *lru, pca_page_ctrl_t *item)
{
    // remove at any location
    pca_page_ctrl_t *prev = NULL;
    pca_page_ctrl_t *next = NULL;

    if (item->lru_prev != PCA_INVALID_ID) {
        prev = PCA_GET_CTRL_BY_ID(g_pca_buf_ctx, item->lru_prev);
        prev->lru_next = item->lru_next;
    } else {
        lru->first = item->lru_next;
    }

    if (item->lru_next != PCA_INVALID_ID) {
        next = PCA_GET_CTRL_BY_ID(g_pca_buf_ctx, item->lru_next);
        next->lru_prev = item->lru_prev;
    } else {
        lru->last = item->lru_prev;
    }

    item->lru_prev = PCA_INVALID_ID;
    item->lru_next = PCA_INVALID_ID;
    item->state = CTRL_STATE_ISOLATION;
    lru->count--;

    return ;
}

pca_page_ctrl_t *pca_lru_pop(pca_lru_list_t *lru)
{
    pca_page_ctrl_t *item = NULL;

    if (lru->count == 0) {
        return NULL;
    }

    // remove from the last location.
    (void)LWLockAcquire(lru->lock, LW_EXCLUSIVE);

    if (lru->count == 0) {
        LWLockRelease(lru->lock);
        return NULL;
    }

    item = PCA_GET_CTRL_BY_ID(g_pca_buf_ctx, lru->last);
    pca_lru_remove(lru, item);

    LWLockRelease(lru->lock);

    return item;
}

bool pca_buf_recycle_core_try(pca_page_ctrl_t *item)
{
    if ((pg_atomic_read_u32(&item->ref_num) > 0) || (pg_atomic_read_u32(&item->touch_nr) >= PCA_BUF_TCH_AGE)) {
        return false;
    }

    pca_hash_bucket_t *bucket = PCA_GET_BUCKET_BY_ID(g_pca_buf_ctx, item->bck_id);
    if (!LWLockConditionalAcquire(bucket->lock, LW_EXCLUSIVE)) {
        return false;  // If the lock is not available, return FALSE with no side-effects.
    }

    if ((pg_atomic_read_u32(&item->ref_num) > 0) || (pg_atomic_read_u32(&item->touch_nr) >= PCA_BUF_TCH_AGE)) {
        LWLockRelease(bucket->lock);
        return false;
    }

    pca_buf_remove_from_bucket(bucket, item);
    pg_atomic_write_u32(&item->touch_nr, 0);  // reset the hot

    LWLockRelease(bucket->lock);
    return true;
}

pca_page_ctrl_t *pca_buf_recycle_core(pca_lru_list_t *main_lru)
{
    uint32 step = 0;
    pca_page_ctrl_t *item = NULL;
    pca_page_ctrl_t *res = NULL;

    if (!LWLockConditionalAcquire(main_lru->lock, LW_EXCLUSIVE)) {
        return res;
    }

    g_pca_buf_ctx->stat.recycle_cnt++;

    uint32 curr_ctrl_id = main_lru->last;
    while (curr_ctrl_id != PCA_INVALID_ID) {
        if (step >= PCA_BUF_MAX_RECYLE_STEPS) {
            break;
        }

        item = PCA_GET_CTRL_BY_ID(g_pca_buf_ctx, curr_ctrl_id);
        if (pca_buf_recycle_core_try(item)) {
            pca_lru_remove(main_lru, item);
            res = item;
            break;
        }

        item->touch_nr /= CB_CUSTOM_VALUE_TWO;  // hot cold down for next step if the ctrl can not be recycled...

        step++;
        curr_ctrl_id = item->lru_prev;
    }

    LWLockRelease(main_lru->lock);
    return res;
}

pca_page_ctrl_t *pca_buf_recycle(uint32 rand_val)
{
    pca_page_ctrl_t *item = NULL;
    uint8 rand_start = rand_val % PCA_PART_LIST_NUM;

    while (1) {
        // part 1: try to reuse a ctrl from free list
        for (uint8 i = 0; i < PCA_PART_LIST_NUM; i++) {
            item = pca_lru_pop(&g_pca_buf_ctx->free_lru[(rand_start + i) % PCA_PART_LIST_NUM]);
            if (item != NULL) {
                return item;
            }
        }

        // part 2: try to recycle a ctrl from main list
        // try all main lists to recycle, randomly start from a list
        for (uint8 i = 0; i < PCA_PART_LIST_NUM; i++) {
            item = pca_buf_recycle_core(&g_pca_buf_ctx->main_lru[(rand_start + i) % PCA_PART_LIST_NUM]);
            if (item != NULL) {
                return item;
            }

            // try to find one in free list
            if (g_pca_buf_ctx->free_lru[i].count != 0) {
                break;
            }
        }

        // do some sleep or pause
    }
}

static inline bool pca_is_all_zero(char *pca_page)
{
    size_t *tmp = (size_t *)(void *)pca_page;
    for (int i = 0; i < (int)(BLCKSZ / sizeof(size_t)); i++) {
        if (tmp[i] != 0) {
            return false;
        }
    }

    return true;
}

void pca_buf_load_page(pca_page_ctrl_t *item, const ExtentLocation& location, CfsBufferKey *key)
{
    errno_t rc;

    rc = (errno_t)memcpy_sp(&item->pca_key, sizeof(CfsBufferKey), key, sizeof(CfsBufferKey));
    securec_check(rc, "", "");

    // load real page from disk by mmap
    int nbytes = FilePRead(location.fd, (char *)item->pca_page, BLCKSZ,
                           location.headerNum * BLCKSZ, (uint32)WAIT_EVENT_DATA_FILE_READ);
    if (nbytes != BLCKSZ) {
        item->load_status = CTRL_PAGE_LOADED_ERROR;
        ereport(DEBUG5, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("Failed to pca_buf_load_page %s, headerNum: %u.", FilePathName(location.fd), location.headerNum)));
        return;
    }

    if (RecoveryInProgress()) {
        if (pca_is_all_zero((char *)item->pca_page)) {
            // the pca page has not been inited. so we do the init again.
            item->pca_page->algorithm = location.algorithm;
            item->pca_page->chunk_size = location.chrunk_size;
        }
    }

    /* if in recovey process and the pca page has been inited, we need to check it */
    if ((CfsHeaderPagerCheckStatus)CheckAndRepairCompressAddress(item->pca_page, location.chrunk_size,
                                                                 location.algorithm, location) ==
        CFS_HEADER_CHECK_STATUS_ERROR) {
        item->load_status = CTRL_PAGE_LOADED_ERROR;
        return;
    }

    item->load_status = CTRL_PAGE_IS_LOADED;
    return;
}

pca_page_ctrl_t *pca_buf_read_page(const ExtentLocation& location, LWLockMode lockMode, PcaBufferReadMode readMode)
{
    errno_t rc;
    CfsBufferKey key = {{location.relFileNode.spcNode, location.relFileNode.dbNode, location.relFileNode.relNode,
                         location.relFileNode.bucketNode},
        location.extentNumber};
    uint32 hashcode = pca_hashcode(&key);

    pca_hash_bucket_t *bucket = PCA_GET_BUCKET_BY_HASH(g_pca_buf_ctx, hashcode);

    // 1: try to find from buckets
    (void)LWLockAcquire(bucket->lock, LW_EXCLUSIVE);
    pca_page_ctrl_t *ctrl = pca_buf_find_from_bucket(bucket, &key);
    if (ctrl != NULL) {
        (void)LWLockAcquire(ctrl->content_lock, lockMode);
        if (ctrl->load_status != CTRL_PAGE_IS_LOADED && readMode == PCA_BUF_NORMAL_READ) {
            pca_buf_load_page(ctrl, location, &key); // load real pca page.
        }
        (void)pg_atomic_fetch_add_u32(&ctrl->ref_num, 1);
        (void)pg_atomic_fetch_add_u32(&ctrl->touch_nr, PCA_BUF_TCH_AGE);
        LWLockRelease(bucket->lock);
        return ctrl;
    }
    LWLockRelease(bucket->lock);

    // 2: try to recycle from list
    pca_page_ctrl_t *item = pca_buf_recycle(location.extentNumber);

    // 3: find from the bucket again
    (void)LWLockAcquire(bucket->lock, LW_EXCLUSIVE);
    ctrl = pca_buf_find_from_bucket(bucket, &key);
    if (ctrl != NULL) {
        (void)LWLockAcquire(ctrl->content_lock, lockMode);
        if (ctrl->load_status != CTRL_PAGE_IS_LOADED && readMode == PCA_BUF_NORMAL_READ) {
            pca_buf_load_page(ctrl, location, &key); // load real pca page.
        }
        (void)pg_atomic_fetch_add_u32(&ctrl->ref_num, 1);
        (void)pg_atomic_fetch_add_u32(&ctrl->touch_nr, 1);
        LWLockRelease(bucket->lock);
        // add to free list
        pca_lru_push(&g_pca_buf_ctx->free_lru[item->ctrl_id % PCA_PART_LIST_NUM], item, CTRL_STATE_FREE);
        return ctrl;
    }

    // only one thread can be here.
    pg_atomic_write_u32(&item->ref_num, 1);
    pg_atomic_write_u32(&item->touch_nr, PCA_BUF_TCH_AGE);

    // clear data of page
    rc = memset_s((void*)item->pca_page, BLCKSZ, 0, BLCKSZ);
    securec_check(rc, "\0", "\0");

    item->load_status = CTRL_PAGE_IS_NO_LOAD;
    if (readMode == PCA_BUF_NORMAL_READ) {
        pca_buf_load_page(item, location, &key); // load real pca page.
    }

    pca_buf_add_into_bucket(bucket, item); // add into bucket

    (void)LWLockAcquire(item->content_lock, lockMode);
    LWLockRelease(bucket->lock);
    // add to main list
    pca_lru_push(&g_pca_buf_ctx->main_lru[item->ctrl_id % PCA_PART_LIST_NUM], item, CTRL_STATE_MAIN);

    return item;
}

void pca_buf_free_page(pca_page_ctrl_t *ctrl, const ExtentLocation& location, bool need_write)
{
    if (need_write) {
        // sync to disk
        int nbytes = FilePWrite(location.fd, (char *)ctrl->pca_page, BLCKSZ, location.headerNum * BLCKSZ,
                                (uint32)WAIT_EVENT_DATA_FILE_WRITE);
        if (nbytes != BLCKSZ) {
            // get the ctrl locked before, the thread is still keep the lock, release ctrl lock and decrease the ref_num
            (void)pg_atomic_fetch_sub_u32(&ctrl->ref_num, 1);
            ctrl->load_status = CTRL_PAGE_LOADED_ERROR;
            LWLockRelease(ctrl->content_lock);

            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                errmsg("Failed to pca_buf_free_page %s", FilePathName(location.fd))));
            return;
        }
        ctrl->load_status = CTRL_PAGE_IS_LOADED;
    }

    // get the ctrl locked before, the thread is still keep the lock, release ctrl lock and decrease the ref_num
    (void)pg_atomic_fetch_sub_u32(&ctrl->ref_num, 1);
    LWLockRelease(ctrl->content_lock);
}

Size pca_buffer_size()
{
    Size tmpsize = ((uint32)(g_instance.attr.attr_storage.NPcaBuffers)) *
                       (sizeof(pca_page_ctrl_t) + BLCKSZ + 3 * sizeof(pca_hash_bucket_t))
        + PG_CACHE_LINE_SIZE + sizeof(pca_page_buff_ctx_t);
    Size buffer_size = (tmpsize > SIZE_G(16) ? SIZE_G(16) : tmpsize) + PG_CACHE_LINE_SIZE;

    return buffer_size;
}

uint32 pca_lock_count()
{
    Size buffer_size = pca_buffer_size();
    uint32 ctrl_count = (uint32)((buffer_size - sizeof(pca_page_buff_ctx_t)) /
        (sizeof(pca_page_ctrl_t) + BLCKSZ + 3 * sizeof(pca_hash_bucket_t)));

    return (PCA_LRU_LIST_NUM + ctrl_count + ctrl_count * CB_CUSTOM_VALUE_THREE1);
}

void pca_buf_init_ctx()
{
    char *buf;
    errno_t rc;
    bool found = false;
    Size buffer_size = pca_buffer_size();

    // init the memoey
#ifdef __aarch64__
    t_thrd.storage_cxt.PcaBufferBlocks =
            (char *)CACHELINEALIGN(ShmemInitStruct("PCA Buffer Blocks", buffer_size, &found));
#else
    t_thrd.storage_cxt.PcaBufferBlocks = (char *)ShmemInitStruct("PCA Buffer Blocks", buffer_size, &found);
#endif

    if (found) {
        return;
    }

    // init ctx
    buf = t_thrd.storage_cxt.PcaBufferBlocks;
    rc = memset_s((void*)buf, sizeof(pca_page_buff_ctx_t), 0, sizeof(pca_page_buff_ctx_t));
    securec_check(rc, "\0", "\0");

    g_pca_buf_ctx = (pca_page_buff_ctx_t *)(void *)buf;
    g_pca_buf_ctx->max_count = (uint32)((buffer_size - sizeof(pca_page_buff_ctx_t)) /
        (sizeof(pca_page_ctrl_t) + BLCKSZ + CB_CUSTOM_VALUE_THREE2 * sizeof(pca_hash_bucket_t)));
    g_pca_buf_ctx->ctrl_buf = (pca_page_ctrl_t *)(void *)(buf + sizeof(pca_page_buff_ctx_t));
    g_pca_buf_ctx->page_buf =
        ((char *)(g_pca_buf_ctx->ctrl_buf)) + (Size)((Size)g_pca_buf_ctx->max_count * sizeof(pca_page_ctrl_t));
    g_pca_buf_ctx->hashtbl.buckets = (pca_hash_bucket_t *)(void *)(
        ((char *)(g_pca_buf_ctx->page_buf)) + (Size)((Size)g_pca_buf_ctx->max_count * BLCKSZ));

    for (uint32 i = 0; i < PCA_PART_LIST_NUM; i++) {
        g_pca_buf_ctx->main_lru[i].lock = LWLockAssign((int)LWTRANCHE_PCA_BUFFER_CONTENT);
        LWLockInitialize(g_pca_buf_ctx->main_lru[i].lock, (int)LWTRANCHE_PCA_BUFFER_CONTENT);
        g_pca_buf_ctx->free_lru[i].lock = LWLockAssign((int)LWTRANCHE_PCA_BUFFER_CONTENT);
        LWLockInitialize(g_pca_buf_ctx->free_lru[i].lock, (int)LWTRANCHE_PCA_BUFFER_CONTENT);
    }

    // init all ctrl
    for (uint32 i = 1; i <= g_pca_buf_ctx->max_count; i++) {
        pca_page_ctrl_t *ctrl = PCA_GET_CTRL_BY_ID(g_pca_buf_ctx, i);
        rc = memset_s((void*)ctrl, sizeof(pca_page_ctrl_t), 0, sizeof(pca_page_ctrl_t));
        securec_check(rc, "\0", "\0");

        ctrl->ctrl_id = i;
        ctrl->pca_page = (CfsExtentHeader *)(void *)(g_pca_buf_ctx->page_buf + (Size)((Size)(i - 1) * BLCKSZ));
        ctrl->content_lock = LWLockAssign((int)LWTRANCHE_PCA_BUFFER_CONTENT);
        LWLockInitialize(ctrl->content_lock, (int)LWTRANCHE_PCA_BUFFER_CONTENT);
        // add to free list
        pca_lru_push_nolock(&g_pca_buf_ctx->free_lru[ctrl->ctrl_id % PCA_PART_LIST_NUM], ctrl, CTRL_STATE_FREE);
    }

    // init all buckets
    g_pca_buf_ctx->hashtbl.hash = tag_hash;
    g_pca_buf_ctx->hashtbl.match = memcmp;
    g_pca_buf_ctx->hashtbl.bucket_num = CB_CUSTOM_VALUE_THREE3 * g_pca_buf_ctx->max_count;
    for (uint32 i = 1; i <= g_pca_buf_ctx->hashtbl.bucket_num; i++) {
        pca_hash_bucket_t *bucket = PCA_GET_BUCKET_BY_ID(g_pca_buf_ctx, i);
        rc = memset_s((void*)bucket, sizeof(pca_hash_bucket_t), 0, sizeof(pca_hash_bucket_t));
        securec_check(rc, "\0", "\0");

        bucket->bck_id = i;
        bucket->lock = LWLockAssign((int)LWTRANCHE_PCA_BUFFER_CONTENT);
        LWLockInitialize(bucket->lock, (int)LWTRANCHE_PCA_BUFFER_CONTENT);
    }

    g_pca_buf_ctx->stat.recycle_cnt = 0;
}

CfsHeaderPagerCheckStatus CheckHeaderOfCompressAddr(CfsExtentHeader* pcMap, uint16 chunk_size,
                                                    uint8 algorithm, const char* path)
{
    if (pcMap->chunk_size != chunk_size || pcMap->algorithm != algorithm) {
        if (u_sess->attr.attr_security.zero_damaged_pages) {
            ereport(WARNING,
                (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("invalid chunk_size %hu or algorithm %hhu in head of compress relation address file \"%s\", "
                           "and reinitialized it.",
                        pcMap->chunk_size,
                        pcMap->algorithm,
                        path)));

            pg_atomic_write_u32(&pcMap->nblocks, 0);
            pg_atomic_write_u32(&pcMap->allocated_chunks, 0);
            pcMap->chunk_size = chunk_size;
            pcMap->algorithm = algorithm;

            return CFS_HEADER_CHECK_STATUS_REPAIRED;
        } else {
            ereport(WARNING,
                (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("invalid chunk_size %hu or algorithm %hhu in head of compress relation address file \"%s\"",
                        pcMap->chunk_size,
                        pcMap->algorithm,
                        path)));

            pcMap->chunk_size = chunk_size;
            pcMap->algorithm = algorithm;
            return CFS_HEADER_CHECK_STATUS_REPAIRED;
        }
    }

    return CFS_HEADER_CHECK_STATUS_OK;
}

CfsHeaderPagerCheckStatus CheckAndRepairCompressAddress(CfsExtentHeader *pcMap, uint16 chunk_size, uint8 algorithm,
                                                        const ExtentLocation& location)
{
    errno_t rc;
    CfsHeaderPagerCheckStatus status = CFS_HEADER_CHECK_STATUS_OK;

    char path[MAX_PATH];
    rc = sprintf_s(path, MAX_PATH, "[RelFileNode:%u/%u/%u], extentNumber:%u, extentStart:%u, extentOffset:%u,"
                  "headerNum:%u, chunk_size:%hu",
        location.relFileNode.spcNode, location.relFileNode.dbNode, location.relFileNode.relNode,
        location.extentNumber, location.extentStart, location.extentOffset, location.headerNum, chunk_size);
    securec_check_ss(rc, "", "");

    /* check head of compress address file */
    status = (CfsHeaderPagerCheckStatus)CheckHeaderOfCompressAddr(pcMap, chunk_size, algorithm, path);

    uint32 nblocks = pg_atomic_read_u32(&pcMap->nblocks);
    uint32 allocated_chunks = pg_atomic_read_u32(&pcMap->allocated_chunks);
    uint16 *global_chunknos = (uint16 *)palloc0(CFS_MAX_LOGIC_CHRUNKS_NUMBER(chunk_size) * sizeof(uint16));

    uint16 max_blocknum = (uint16)-1;
    uint16 max_nonzero_blocknum = (uint16)-1;
    uint16 max_allocated_chunkno = (pc_chunk_number_t)0;

    /* check compress address of every pages */
    for (uint16 blocknum = 0; blocknum < CFS_LOGIC_BLOCKS_PER_EXTENT; ++blocknum) {
        CfsExtentAddress *pcAddr = GetExtentAddress(pcMap, blocknum);
        if (pcAddr->checksum != AddrChecksum32(pcAddr, pcAddr->allocated_chunks)) {
            pcAddr->allocated_chunks = pcAddr->nchunks = 0;
            for (int i = 0; i < BLCKSZ / chunk_size; ++i) {
                pcAddr->chunknos[i] = 0;
            }
            pcAddr->checksum = 0;
            ereport(WARNING, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("invalid checkum %u of block %hu in file \"%s\"",
                                                                      pcAddr->checksum, blocknum, path)));
            status = CFS_HEADER_CHECK_STATUS_REPAIRED;
        }
        /*
         * skip when found first zero filled block after nblocks
         * if(blocknum >= (BlockNumber)nblocks && pcAddr->allocated_chunks == 0)
         * break;
         */

        /* check allocated_chunks for one page */
        if (pcAddr->allocated_chunks > BLCKSZ / chunk_size) {
            if (u_sess->attr.attr_security.zero_damaged_pages) {
                rc = memset_s((void *)pcAddr, SizeOfExtentAddress(chunk_size), 0,
                              SizeOfExtentAddress(chunk_size));
                securec_check_c(rc, "\0", "\0");
                ereport(WARNING, (errcode(ERRCODE_DATA_CORRUPTED),
                                  errmsg("invalid allocated_chunks %hhu of block %hu in file \"%s\", and zero this block",
                                         pcAddr->allocated_chunks, blocknum, path)));
                status = CFS_HEADER_CHECK_STATUS_REPAIRED;
                continue;
            } else {
                pfree(global_chunknos);
                ereport(WARNING, (errcode(ERRCODE_DATA_CORRUPTED),
                                errmsg("invalid allocated_chunks %hhu of block %hu in file \"%s\"",
                                       pcAddr->allocated_chunks, blocknum, path)));
                return CFS_HEADER_CHECK_STATUS_ERROR;
            }
        }

        /* check chunknos for one page */
        for (int i = 0; i < pcAddr->allocated_chunks; ++i) {
            /* check for invalid chunkno */
            if (pcAddr->chunknos[i] == 0 || pcAddr->chunknos[i] > CFS_MAX_LOGIC_CHRUNKS_NUMBER(chunk_size)) {
                if (u_sess->attr.attr_security.zero_damaged_pages) {
                    rc = memset_s((void *)pcAddr, SizeOfExtentAddress(chunk_size), 0,
                                  SizeOfExtentAddress(chunk_size));
                    securec_check_c(rc, "\0", "\0");
                    ereport(WARNING, (errcode(ERRCODE_DATA_CORRUPTED),
                                      errmsg("invalid chunk number %hu of block %hhu in file \"%s\", and zero this block",
                                             pcAddr->chunknos[i], blocknum, path)));
                    status = CFS_HEADER_CHECK_STATUS_REPAIRED;
                    continue;
                } else {
                    pfree(global_chunknos);
                    ereport(WARNING, (errcode(ERRCODE_DATA_CORRUPTED),
                                    errmsg("invalid chunk number %hu of block %hhu in file \"%s\"", pcAddr->chunknos[i],
                                           blocknum, path)));
                    return CFS_HEADER_CHECK_STATUS_ERROR;
                }
            }

            /* check for duplicate chunkno */
            if (global_chunknos[pcAddr->chunknos[i] - 1] != 0) {
                if (u_sess->attr.attr_security.zero_damaged_pages) {
                    rc = memset_s((void *)pcAddr, SizeOfExtentAddress(chunk_size), 0,
                                  SizeOfExtentAddress(chunk_size));
                    securec_check_c(rc, "\0", "\0");
                    ereport(
                        WARNING,
                        (errcode(ERRCODE_DATA_CORRUPTED),
                         errmsg(
                             "chunk number %hu of block %hu duplicate with block %hu in file \"%s\", and zero this block",
                             pcAddr->chunknos[i], blocknum, global_chunknos[pcAddr->chunknos[i] - 1], path)));
                    status = CFS_HEADER_CHECK_STATUS_REPAIRED;
                    continue;
                } else {
                    ereport(WARNING,
                            (errcode(ERRCODE_DATA_CORRUPTED),
                             errmsg("chunk number %hu of block %hu duplicate with block %hu in file \"%s\"",
                                    pcAddr->chunknos[i], blocknum, global_chunknos[pcAddr->chunknos[i] - 1], path)));
                    pfree(global_chunknos);
                    return CFS_HEADER_CHECK_STATUS_ERROR;
                }
            }
        }

        /* clean chunknos beyond allocated_chunks for one page */
        for (int i = pcAddr->allocated_chunks; i < BLCKSZ / chunk_size; ++i) {
            if (pcAddr->chunknos[i] != 0) {
                pcAddr->chunknos[i] = 0;
                ereport(WARNING, (errcode(ERRCODE_DATA_CORRUPTED),
                                  errmsg("clear chunk number %hu beyond allocated_chunks %hhu of block %hu in file \"%s\"",
                                         pcAddr->chunknos[i], pcAddr->allocated_chunks, blocknum, path)));
                status = CFS_HEADER_CHECK_STATUS_REPAIRED;
            }
        }

        /* check nchunks for one page */
        if (pcAddr->nchunks > pcAddr->allocated_chunks) {
            if (u_sess->attr.attr_security.zero_damaged_pages) {
                rc = memset_s((void *)pcAddr, SizeOfExtentAddress(chunk_size), 0,
                              SizeOfExtentAddress(chunk_size));
                securec_check_c(rc, "\0", "\0");
                ereport(
                    WARNING,
                    (errcode(ERRCODE_DATA_CORRUPTED),
                     errmsg("nchunks %hu exceeds allocated_chunks %hhu of block %hu in file \"%s\", and zero this block",
                            pcAddr->nchunks, pcAddr->allocated_chunks, blocknum, path)));
                status = CFS_HEADER_CHECK_STATUS_REPAIRED;
                continue;
            } else {
                pfree(global_chunknos);
                ereport(WARNING, (errcode(ERRCODE_DATA_CORRUPTED),
                                errmsg("nchunks h%u exceeds allocated_chunks %hhu of block %hu in file \"%s\"",
                                       pcAddr->nchunks, pcAddr->allocated_chunks, blocknum, path)));
                return CFS_HEADER_CHECK_STATUS_ERROR;
            }
        }

        max_blocknum = blocknum;
        if (pcAddr->nchunks > 0) {
            max_nonzero_blocknum = blocknum;
        }

        for (int i = 0; i < pcAddr->allocated_chunks; ++i) {
            global_chunknos[pcAddr->chunknos[i] - 1] = blocknum + 1;
            if (pcAddr->chunknos[i] > max_allocated_chunkno) {
                max_allocated_chunkno = pcAddr->chunknos[i];
            }
        }
    }

    int unused_chunks = 0;
    /* check for holes in allocated chunks */
    for (uint16 i = 0; i < max_allocated_chunkno; i++) {
        if (global_chunknos[i] == 0) {
            unused_chunks++;
        }
    }

    if (unused_chunks > 0) {
        ereport(WARNING, (errcode(ERRCODE_DATA_CORRUPTED),
                          errmsg("there are %d chunks of total allocated chunks %hu can not be use in file \"%s\"",
                                 unused_chunks, max_allocated_chunkno, path),
                          errhint("You may need to run VACUMM FULL to optimize space allocation.")));
    }

    /* update nblocks in head of compressed file */
    if (nblocks < (uint16)(max_nonzero_blocknum + 1)) {
        pg_atomic_write_u32(&pcMap->nblocks, max_nonzero_blocknum + 1);

        ereport(WARNING, (errcode(ERRCODE_DATA_CORRUPTED),
                          errmsg("update nblocks head of compressed file \"%s\". old: %u, new: %hu", path, nblocks,
                                 max_nonzero_blocknum + 1)));
        status = CFS_HEADER_CHECK_STATUS_REPAIRED;
    }

    /* update allocated_chunks in head of compress file */
    if (allocated_chunks != max_allocated_chunkno) {
        pg_atomic_write_u32(&pcMap->allocated_chunks, max_allocated_chunkno);

        ereport(WARNING, (errcode(ERRCODE_DATA_CORRUPTED),
                          errmsg("update allocated_chunks in head of compressed file \"%s\". old: %u, new: %hu", path,
                                 allocated_chunks, max_allocated_chunkno)));
        status = CFS_HEADER_CHECK_STATUS_REPAIRED;
    }

    /* clean compress address after max_blocknum + 1 */
    for (uint16 blocknum = max_blocknum + 1; blocknum < CFS_LOGIC_BLOCKS_PER_EXTENT; blocknum++) {
        char buf[128];
        char *p = NULL;
        CfsExtentAddress *pcAddr = GetExtentAddress(pcMap, blocknum);

        /* skip zero block */
        if (pcAddr->allocated_chunks == 0 && pcAddr->nchunks == 0) {
            continue;
        }

        /* clean compress address and output content of the address */
        rc = memset_s(buf, sizeof(buf), 0, sizeof(buf));
        securec_check_c(rc, "\0", "\0");
        p = buf;

        for (int i = 0; i < pcAddr->allocated_chunks; i++) {
            if (pcAddr->chunknos[i]) {
                const char *formatStr = i == 0 ? "%u" : ",%u";
                rc = snprintf_s(p, (sizeof(buf) - (unsigned long)(p - buf)), 
                                (sizeof(buf) - (unsigned long)(p - buf)) - 1, formatStr, pcAddr->chunknos[i]);
                securec_check_ss(rc, "\0", "\0");
                p += strlen(p);
            }
        }

        rc = memset_s((void *)pcAddr, SizeOfExtentAddress(chunk_size), 0, SizeOfExtentAddress(chunk_size));
        securec_check_c(rc, "\0", "\0");
        ereport(WARNING, (errcode(ERRCODE_DATA_CORRUPTED),
                          errmsg("clean unused compress address of block %hu in file \"%s\", old "
                                 "allocated_chunks/nchunks/chunknos: %hhu/%hhu/{%s}",
                                 blocknum, path, pcAddr->allocated_chunks, pcAddr->nchunks, buf)));
        status = CFS_HEADER_CHECK_STATUS_REPAIRED;
    }

    pfree(global_chunknos);

    return status;
}
