/*
 * page_compression.h
 *		internal declarations for page compression
 *
 * Copyright (c) 2020, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/storage/page_compression.h
 */

#ifndef PAGE_COMPRESSION_H
#define PAGE_COMPRESSION_H

#include <sys/mman.h>

#include "c.h"
#include "storage/buf/bufpage.h"
#include "datatype/timestamp.h"
#include "catalog/pg_class.h"
#include "utils/atomic.h"

/* The page compression feature relies on native atomic operation support.
 * On platforms that do not support native atomic operations, the members
 * of pg_atomic_uint32 contain semaphore objects, which will affect the
 * persistence of compressed page address files.
 */
#define SUPPORT_PAGE_COMPRESSION (sizeof(pg_atomic_uint32) == sizeof(uint32))

/* In order to avoid the inconsistency of address metadata data when the server
 * is down, it is necessary to prevent the address metadata of one data block
 * from crossing two storage device blocks. The block size of ordinary storage
 * devices is a multiple of 512, so 512 is used as the block size of the
 * compressed address file.
 */
#define COMPRESS_ADDR_BLCKSZ 512

/* COMPRESS_ALGORITHM_XXX must be the same as COMPRESS_TYPE_XXX */
#define COMPRESS_ALGORITHM_PGLZ 1
#define COMPRESS_ALGORITHM_ZSTD 2
#define COMPRESS_ALGORITHM_PGZSTD 3

constexpr uint32 COMPRESS_ADDRESS_FLUSH_CHUNKS = 5000;

#define SUPPORT_COMPRESSED(relKind, relam) \
    ((relKind) == RELKIND_RELATION ||      \
     (((relKind) == RELKIND_INDEX || (relKind == RELKIND_GLOBAL_INDEX)) && \
     ((relam) == BTREE_AM_OID || (relam) == UBTREE_AM_OID)))

#define REL_SUPPORT_COMPRESSED(relation) SUPPORT_COMPRESSED((relation)->rd_rel->relkind, (relation)->rd_rel->relam)

typedef uint32 pc_chunk_number_t;
const uint32 PAGE_COMPRESSION_VERSION = 92815;
/*
 * layout of files for Page Compress:
 *
 * 1. page compression address file(_pca)
 * - PageCompressHeader
 * - PageCompressAddr[]
 *
 * 2. page compression data file(_pcd)
 * - PageCompressData[]
 *
 */
typedef struct PageCompressHeader {
    pg_atomic_uint32 nblocks;                      /* number of total blocks in this segment */
    pg_atomic_uint32 allocated_chunks;             /* number of total allocated chunks in data area */
    uint16 chunk_size;                             /* size of each chunk, must be 1/2 1/4 or 1/8 of BLCKSZ */
    uint8 algorithm;                               /* compress algorithm, 1=pglz, 2=lz4 */
    pg_atomic_uint32 last_synced_nblocks;          /* last synced nblocks */
    pg_atomic_uint32 last_synced_allocated_chunks; /* last synced allocated_chunks */
    pg_atomic_uint32 sync;
    TimestampTz last_recovery_start_time;          /* postmaster start time of last recovery */
} PageCompressHeader;

typedef struct PageCompressAddr {
    uint32 checksum;
    volatile uint8 nchunks;          /* number of chunks for this block */
    volatile uint8 allocated_chunks; /* number of allocated chunks for this block */
    /* variable-length fields, 1 based chunk no array for this block, size of the array must be 2, 4 or 8 */
    pc_chunk_number_t chunknos[FLEXIBLE_ARRAY_MEMBER];
} PageCompressAddr;

struct ReadBlockChunksStruct {
    PageCompressHeader* header;  // header: pca file
    FILE* fp;              // fp: table fp
    int segmentNo;
    char* fileName;        // fileName: for error report
};

typedef struct PageCompressData {
    char page_header[SizeOfPageHeaderData]; /* page header */
    uint32 crc32;
    uint32 size : 16;                       /* size of compressed data */
    uint32 byte_convert : 1;
    uint32 diff_convert : 1;
    uint32 algorithm : 4;
    uint32 unused : 10;
    char data[FLEXIBLE_ARRAY_MEMBER];       /* compressed page, except for the page header */
} PageCompressData;

typedef struct HeapPageCompressData {
    char page_header[SizeOfHeapPageHeaderData]; /* page header */
    uint32 crc32;
    uint32 size : 16;                       /* size of compressed data */
    uint32 byte_convert : 1;
    uint32 diff_convert : 1;
    uint32 algorithm : 4;
    uint32 unused : 10;
    char data[FLEXIBLE_ARRAY_MEMBER];       /* compressed page, except for the page header */
} HeapPageCompressData;

const uint4 CHUNK_SIZE_LIST[4] = {BLCKSZ / 2, BLCKSZ / 4, BLCKSZ / 8, BLCKSZ / 16};
constexpr uint4 INDEX_OF_HALF_BLCKSZ = 0; 
constexpr uint4 INDEX_OF_QUARTER_BLCKSZ = 1; 
constexpr uint4 INDEX_OF_EIGHTH_BRICK_BLCKSZ = 2; 
constexpr uint4 INDEX_OF_SIXTEENTHS_BLCKSZ = 3; 
#define MAX_PREALLOC_CHUNKS 7
#define COMPRESS_STR "_compress"
#define COMPRESS_SUFFIX "%s" COMPRESS_STR


#define SIZE_OF_PAGE_COMPRESS_HEADER_DATA sizeof(PageCompressHeader)
#define SIZE_OF_PAGE_COMPRESS_ADDR_HEADER_DATA offsetof(PageCompressAddr, chunknos)

#define SIZE_OF_PAGE_COMPRESS_ADDR(chunk_size) \
    (SIZE_OF_PAGE_COMPRESS_ADDR_HEADER_DATA + sizeof(pc_chunk_number_t) * (BLCKSZ / (chunk_size)))

#define NUMBER_PAGE_COMPRESS_ADDR_PER_BLOCK(chunk_size) (COMPRESS_ADDR_BLCKSZ / SIZE_OF_PAGE_COMPRESS_ADDR(chunk_size))

#define OFFSET_OF_PAGE_COMPRESS_ADDR(chunk_size, blockno)                                           \
    (COMPRESS_ADDR_BLCKSZ * (1 + (blockno) / NUMBER_PAGE_COMPRESS_ADDR_PER_BLOCK(chunk_size)) + \
        SIZE_OF_PAGE_COMPRESS_ADDR(chunk_size) * ((blockno) % NUMBER_PAGE_COMPRESS_ADDR_PER_BLOCK(chunk_size)))

#define GET_PAGE_COMPRESS_ADDR(pcbuffer, chunk_size, blockno) \
    (PageCompressAddr*)((char*)(pcbuffer) + OFFSET_OF_PAGE_COMPRESS_ADDR((chunk_size), (blockno) % RELSEG_SIZE))

#define SIZE_OF_PAGE_COMPRESS_ADDR_FILE(chunk_size) OFFSET_OF_PAGE_COMPRESS_ADDR((chunk_size), RELSEG_SIZE)

#define OFFSET_OF_PAGE_COMPRESS_CHUNK(chunk_size, chunkno) ((chunk_size) * ((chunkno)-1))

/* Abnormal scenarios may cause holes in the space allocation of data files,
 * causing data file expansion. Usually the holes are not too big, so the definition
 * allows a maximum of 10,000 chunks for holes. If allocated_chunks exceeds this value,
 * VACUUM FULL needs to be executed to reclaim space.
 */
#define MAX_CHUNK_NUMBER(chunk_size) ((uint32)(RELSEG_SIZE * (BLCKSZ / (chunk_size)) + 10000))

constexpr unsigned CMP_BYTE_CONVERT_LEN = 1;
constexpr unsigned CMP_DIFF_CONVERT_LEN = 1;
constexpr unsigned CMP_PRE_CHUNK_LEN = 3;
constexpr unsigned CMP_LEVEL_SYMBOL_LEN = 1;
constexpr unsigned CMP_LEVEL_LEN = 5;
constexpr unsigned CMP_ALGORITHM_LEN = 3;
constexpr unsigned CMP_CHUNK_SIZE_LEN = 2;

constexpr unsigned CMP_BYTE_CONVERT_INDEX = 0;
constexpr unsigned CMP_DIFF_CONVERT_INDEX = 1;
constexpr unsigned CMP_PRE_CHUNK_INDEX = 2;
constexpr unsigned CMP_COMPRESS_LEVEL_SYMBOL = 3;
constexpr unsigned CMP_LEVEL_INDEX = 4;
constexpr unsigned CMP_ALGORITHM_INDEX = 5;
constexpr unsigned CMP_CHUNK_SIZE_INDEX = 6;

struct CmpBitStuct {
    unsigned int bitLen;
    unsigned int mask;
    unsigned int moveBit;
};

constexpr CmpBitStuct g_cmpBitStruct[] = {{CMP_BYTE_CONVERT_LEN, 0x01, 15},
    {CMP_DIFF_CONVERT_LEN, 0x01, 14},
    {CMP_PRE_CHUNK_LEN, 0x07, 11},
    {CMP_LEVEL_SYMBOL_LEN, 0x01, 10},
    {CMP_LEVEL_LEN, 0x1F, 5},
    {CMP_ALGORITHM_LEN, 0x07, 2},
    {CMP_CHUNK_SIZE_LEN, 0x03, 0}};
/* RelFileCompressOption: Row-oriented table compress option */
struct RelFileCompressOption {
    unsigned byteConvert : g_cmpBitStruct[CMP_BYTE_CONVERT_INDEX].bitLen,  /* need byte convert? */
        diffConvert : g_cmpBitStruct[CMP_DIFF_CONVERT_INDEX].bitLen,  /* need diff convert processed? */
        compressPreallocChunks : g_cmpBitStruct[CMP_PRE_CHUNK_INDEX]
                                     .bitLen, /* prealloced chunks to store compressed data */
        compressLevelSymbol : g_cmpBitStruct[CMP_COMPRESS_LEVEL_SYMBOL]
                                  .bitLen, /* compress level symbol, true for positive and false for negative */
        compressLevel : g_cmpBitStruct[CMP_LEVEL_INDEX].bitLen,          /* compress level */
        compressAlgorithm : g_cmpBitStruct[CMP_ALGORITHM_INDEX].bitLen,  /* compress algorithm */
        compressChunkSize : g_cmpBitStruct[CMP_CHUNK_SIZE_INDEX].bitLen; /* chunk size of compressed data */
};

inline void TransCompressOptions(const RelFileNode& node, RelFileCompressOption* opt)
{
    unsigned short compressOption = node.opt;
    opt->compressChunkSize = compressOption & g_cmpBitStruct[CMP_CHUNK_SIZE_INDEX].mask;
    compressOption = compressOption >> g_cmpBitStruct[CMP_CHUNK_SIZE_INDEX].bitLen;
    opt->compressAlgorithm = compressOption & g_cmpBitStruct[CMP_ALGORITHM_INDEX].mask;
    compressOption = compressOption >> g_cmpBitStruct[CMP_ALGORITHM_INDEX].bitLen;
    opt->compressLevel = compressOption & g_cmpBitStruct[CMP_LEVEL_INDEX].mask;
    compressOption = compressOption >> g_cmpBitStruct[CMP_LEVEL_INDEX].bitLen;
    opt->compressLevelSymbol = compressOption & g_cmpBitStruct[CMP_COMPRESS_LEVEL_SYMBOL].mask;
    compressOption = compressOption >> g_cmpBitStruct[CMP_COMPRESS_LEVEL_SYMBOL].bitLen;
    opt->compressPreallocChunks = compressOption & g_cmpBitStruct[CMP_PRE_CHUNK_INDEX].mask;
    compressOption = compressOption >> g_cmpBitStruct[CMP_PRE_CHUNK_INDEX].bitLen;
    opt->diffConvert = compressOption & g_cmpBitStruct[CMP_DIFF_CONVERT_INDEX].mask;
    compressOption = compressOption >> g_cmpBitStruct[CMP_DIFF_CONVERT_INDEX].bitLen;
    opt->byteConvert = compressOption & g_cmpBitStruct[CMP_BYTE_CONVERT_INDEX].mask;
    compressOption = compressOption >> g_cmpBitStruct[CMP_BYTE_CONVERT_INDEX].bitLen;
}

#define SET_COMPRESS_OPTION(node, byteConvert, diffConvert, preChunks, symbol, level, algorithm, chunkSize) \
    do {                                                                                                    \
        (node).opt = 0;                                                                                     \
        (node).opt = (node).opt << g_cmpBitStruct[CMP_BYTE_CONVERT_INDEX].bitLen;                           \
        (node).opt += (byteConvert)&g_cmpBitStruct[CMP_BYTE_CONVERT_INDEX].mask;                            \
        (node).opt = (node).opt << g_cmpBitStruct[CMP_DIFF_CONVERT_INDEX].bitLen;                           \
        (node).opt += (diffConvert)&g_cmpBitStruct[CMP_DIFF_CONVERT_INDEX].mask;                            \
        (node).opt = (node).opt << g_cmpBitStruct[CMP_PRE_CHUNK_INDEX].bitLen;                              \
        (node).opt += (preChunks)&g_cmpBitStruct[CMP_PRE_CHUNK_INDEX].mask;                                 \
        (node).opt = (node).opt << g_cmpBitStruct[CMP_COMPRESS_LEVEL_SYMBOL].bitLen;                        \
        (node).opt += (symbol)&g_cmpBitStruct[CMP_COMPRESS_LEVEL_SYMBOL].mask;                              \
        (node).opt = (node).opt << g_cmpBitStruct[CMP_LEVEL_INDEX].bitLen;                                  \
        (node).opt += (level)&g_cmpBitStruct[CMP_LEVEL_INDEX].mask;                                         \
        (node).opt = (node).opt << g_cmpBitStruct[CMP_ALGORITHM_INDEX].bitLen;                              \
        (node).opt += (algorithm)&g_cmpBitStruct[CMP_ALGORITHM_INDEX].mask;                                 \
        (node).opt = (node).opt << g_cmpBitStruct[CMP_CHUNK_SIZE_INDEX].bitLen;                             \
        (node).opt += (chunkSize)&g_cmpBitStruct[CMP_CHUNK_SIZE_INDEX].mask;                                \
    } while (0)

#define GET_ROW_COL_CONVERT(opt) \
            (((opt) >> g_cmpBitStruct[CMP_BYTE_CONVERT_INDEX].moveBit) & g_cmpBitStruct[CMP_BYTE_CONVERT_INDEX].mask)
#define GET_DIFF_CONVERT(opt) \
            (((opt) >> g_cmpBitStruct[CMP_DIFF_CONVERT_INDEX].moveBit) & g_cmpBitStruct[CMP_DIFF_CONVERT_INDEX].mask)
#define GET_COMPRESS_PRE_CHUNKS(opt) \
    (((opt) >> g_cmpBitStruct[CMP_PRE_CHUNK_INDEX].moveBit) & g_cmpBitStruct[CMP_PRE_CHUNK_INDEX].mask)
#define GET_COMPRESS_CHUNK_SIZE(opt) \
    (((opt) >> g_cmpBitStruct[CMP_CHUNK_SIZE_INDEX].moveBit) & g_cmpBitStruct[CMP_CHUNK_SIZE_INDEX].mask)

#define IS_COMPRESSED_MAINFORK(reln, forkNum) ((reln)->smgr_rnode.node.opt != 0 && (forkNum) == MAIN_FORKNUM)
#define IS_COMPRESSED_RNODE(rnode, forkNum) ((rnode).opt != 0 && (forkNum) == MAIN_FORKNUM)
/* Compress function */
template <uint8 pagetype>
extern int TemplateCompressPage(const char* src, char* dst, int dst_size, RelFileCompressOption option);

template <uint8 pagetype>
extern int TemplateDecompressPage(const char* src, char* dst);

int CompressPageBufferBound(const char* page, uint8 algorithm);

int CompressPage(const char* src, char* dst, int dst_size, RelFileCompressOption option);

int DecompressPage(const char* src, char* dst);

/**
 * format mainfork path name to compressed path
 * @param dst destination buffer
 * @param pathName uncompressed table name
 */
extern void CopyCompressedPath(char *dst, const char* pathName);

/**
 * @param pathName mainFork File path name
 * @param relFileNode physically access, for validation
 * @param forkNumber for validation
 * @return size of mainFork
 */
 
#define FILE_BLOCK_SIZE_512 (512)
extern int64 CalculateMainForkSize(char* pathName, RelFileNode* relFileNode, ForkNumber forkNumber);
extern int64 CalculateCompressMainForkSize(char* pathName, bool suppressedENOENT = false);

/**
 * check if fileName is end with pca or pcd
 * @param fileName fileName
 * @return filetype
 */
bool IsCompressedFile(const char *fileName, size_t fileNameLen);

int64 CalculateFileSize(char* pathName, bool suppressedENOENT = false);
int64 CalculateFilePhyRealSize(char* pathName, bool suppressedENOENT = false);

/**
 * convert chunk size to the index of CHUNK_SIZE_LIST
 * @param compressedChunkSize {BLCKSZ / 2, BLCKSZ / 4, BLCKSZ / 8, BLCKSZ / 16}
 * @param success success or not
 * @return index of CHUNK_SIZE_LIST
 */
extern uint1 ConvertChunkSize(uint32 compressedChunkSize, bool* success);



#endif /* PAGE_COMPRESSION_H */
