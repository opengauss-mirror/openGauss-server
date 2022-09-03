#ifndef OPENGAUSS_CFS_BUFFERS_H
#define OPENGAUSS_CFS_BUFFERS_H

#include "storage/cfs/cfs.h"
#include "storage/cfs/cfs_converter.h"
#include "storage/cfs/cfs_repair.h"

struct CfsBufferKey {
    RelFileNodeV2 relFileNode;
    uint32 ExtentCount;
};

/*====================================*/
/*=========SINGLE CTRL=========*/
/*====================================*/

typedef enum en_ctrl_state {
    CTRL_STATE_ISOLATION = 0,  // The temporary free state when taken away from the main chain does not belong to any bucket.
    CTRL_STATE_MAIN = 1,       // On the main chain
    CTRL_STATE_FREE = 2,       // Idle link
} ctrl_state_e;

typedef enum en_ctrl_load_status {
    CTRL_PAGE_IS_NO_LOAD,   // not read from disk
    CTRL_PAGE_IS_LOADED,    // normal read from disk with no error
    CTRL_PAGE_LOADED_ERROR  // page on the disk is error
} ctrl_load_status_e;

#ifdef WIN32
typedef struct st_pca_page_ctrl
#else
typedef struct __attribute__((aligned(128))) st_pca_page_ctrl
#endif
{
    uint32 ctrl_id;        // Specifies the CTRL ID, which is also the subscript of the CTRL array. The subscript starts from 1. 0 is an invalid value
    ctrl_state_e state;    // On that chain or free. This is controlled by the chain lock.
    volatile uint32 lru_prev, lru_next;  // Chains before and after. This is controlled by the chain lock.

    volatile uint32 bck_prev, bck_next, bck_id;  // bck_id indicates the bucket to which a user belongs. This is controlled by the lock of the bucket.
    /* The popularity increases by a bit each time a normal access is accessed, decreases by a bit each time a recycle traverse is traversed,
       until it reaches the freezing point. This value can be fuzzy. Concurrency control is not required.
    */
    pg_atomic_uint32 touch_nr;
    /* Access count, that's accurate. Add ctrl lock + +, release lock --, this ++ must be preceded by lock, -- must be preceded by lock release. */
    pg_atomic_uint32 ref_num;

    /* The preceding variables are not controlled by the content_lock lock. The preceding variables are not modified by the CTRL lock holder. */

    LWLock *content_lock;       // This command is used to control the concurrency of the actual content on the PCA page.
    ctrl_load_status_e load_status;

    CfsBufferKey pca_key;       // for real pageid on disk
    CfsExtentHeader *pca_page;  // Authentic PCA Page
} pca_page_ctrl_t;

/*====================================*/
/*=========HASH BUCKETS=========*/
/*====================================*/

// hash key is pageid。
typedef struct st_pca_hash_bucket {
    // This lock must be added to the add, remove, and find commands of the ctrl
    LWLock *lock;
    uint32 bck_id;    // ID of the bucket, which is also the subscript of the bucket array. The subscript starts from 1. 0 is an invalid value.

    uint32 ctrl_count;     // total ctrl count in this bucket.
    uint32 first_ctrl_id;  // first pca_ctrl id in this bucket.
} pca_hash_bucket_t;

typedef struct st_pca_page_hashtbl {
    HashValueFunc hash;      /* hash function */
    HashCompareFunc match;   /* key comparison function */

    uint32 bucket_num;
    pca_hash_bucket_t *buckets; // for many buckets array, max count is [bucket_num * 3][FLEXIBLE_ARRAY_MEMBER]
} pca_page_hashtbl_t;

/*====================================*/
/*=========LRU LIST=========*/
/*====================================*/

typedef struct st_pca_lru_list {
    LWLock *lock;   // protect this two link, each ctrl_t must is on one of them.
    volatile uint32 count;
    volatile uint32 first;
    volatile uint32 last;
} pca_lru_list_t;

/*====================================*/
/*=========PCA BUFFER CONTEXT=========*/
/*====================================*/

/*
    buffer map is:
    --------------------------------------------------------------------------------------------------------------
    |                |ctrl_t * N ....         |     pca_page_t * N ....       |   hash_bucket_t * 3N ....     |
    --------------------------------------------------------------------------------------------------------------
    |                |                        |                               |
    ctx              ctrl_buf                 page_buf                        hashtbl->buckets

    each ctrl->pca is pointing a real page in page_buf area.
*/
typedef struct st_pca_stat_info {
    uint64 recycle_cnt;
} pca_stat_info_t;

#define PCA_PART_LIST_NUM (8)
#define PCA_LRU_LIST_NUM (PCA_PART_LIST_NUM * 2)

#ifdef WIN32
typedef struct st_pca_page_buff_ctx
#else
typedef struct __attribute__((aligned(128))) st_pca_page_buff_ctx
#endif
{
    pca_page_ctrl_t *ctrl_buf;      // ctrl start
    char *page_buf;                 // pca page start
    pca_page_hashtbl_t hashtbl;     // hash table, internal using

    uint32 max_count;               // Maximum page count

    /*
        These two chains are mainly maintained.
        For lru for used pca page, if the free link is empty, when recycling is enabled, the tail of 1/6 LRUs is added to the free link at a time to forcibly recycle the free link.
        Each ctrl on this chain, the main chain, belongs to a bucket
        Eliminate from the tail and add from the head.
    */
    pca_lru_list_t main_lru[PCA_PART_LIST_NUM];

    /*
        lru for unused pca page. During initialization, all CTRLs are set and placed on this link. When applying for CTRLs, all CTRLs are allocated from this link.
        All the ctrls on this chain, the free chain, do not belong to any bucket.
        When initialized, all the ctrls are stringed together, similar to the high water mark.
        Take it from the tail and add it from the tail.
    */
    pca_lru_list_t free_lru[PCA_PART_LIST_NUM];

    // statistic info
    pca_stat_info_t stat;
} pca_page_buff_ctx_t;

#define PCA_INVALID_ID (0)
#define PCA_BUF_TCH_AGE (3)           // consider buffer is hot if its touch_number >= BUF_TCH_AGE
#define PCA_BUF_MAX_RECYLE_STEPS (1024)


#define PCA_GET_CTRL_BY_ID(ctx, ctrl_id) ((pca_page_ctrl_t *)(&(ctx->ctrl_buf[(ctrl_id) - 1])))
#define PCA_GET_BUCKET_BY_ID(ctx, bck_id) ((pca_hash_bucket_t *)(&(ctx->hashtbl.buckets[(bck_id) - 1])))
#define PCA_GET_BUCKET_BY_HASH(ctx, hashcode) (PCA_GET_BUCKET_BY_ID(ctx, (((hashcode) % ctx->hashtbl.bucket_num) + 1)))

#define PCA_SET_NO_READ(ctrl) (ctrl->load_status = CTRL_PAGE_IS_NO_LOAD)

typedef enum PcaBufferReadMode {
    PCA_BUF_NORMAL_READ,
    PCA_BUF_NO_READ
} PcaBufferReadMode;

// for another file
extern pca_page_buff_ctx_t *g_pca_buf_ctx;

/* functions */
/* init the pca buffer */
void pca_buf_init(char *buf, uint32 size);

/* read pca buffer, and read data from disk if not exists in buffer */
pca_page_ctrl_t *pca_buf_read_page(const ExtentLocation& location, LWLockMode lockMode, PcaBufferReadMode readMode);

/* release pca buffer and write data into disk */
void pca_buf_free_page(pca_page_ctrl_t *ctrl, const ExtentLocation& location, bool need_write);

Size pca_buffer_size();
uint32 pca_lock_count();

CfsHeaderPagerCheckStatus CheckAndRepairCompressAddress(CfsExtentHeader *pcMap, uint16 chunk_size, uint8 algorithm,
                                                        const ExtentLocation& location);

#endif
