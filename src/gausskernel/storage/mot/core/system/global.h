/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * global.h
 *    MOT constants and other definitions.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/global.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MOT_GLOBAL_H
#define MOT_GLOBAL_H

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <cinttypes>
#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <map>
#include <string>
#include "libintl.h"

#include "utils/elog.h"
#include "securec.h"

namespace MOT {
// forward declarations
enum class IndexType : uint8_t;
enum class IndexClass : uint8_t;

template <IndexType T>
class Tree;
typedef uint64_t TransactionId;  // Transaction Identifier
class TxnAccess;

/**
 * @enum Return code.
 */
enum RC : uint32_t {
    /** @var Denotes success. */
    RC_OK = 0,

    /** @var Denotes failure. */
    RC_ERROR = 1,

    /** @var Denotes operation aborted. */
    RC_ABORT,

    /** @var Denotes operation aborted due to serialization error */
    RC_SERIALIZATION_FAILURE,

    RC_UNSUPPORTED_COL_TYPE,
    RC_UNSUPPORTED_COL_TYPE_ARR,
    RC_EXCEEDS_MAX_ROW_SIZE,

    RC_COL_NAME_EXCEEDS_MAX_SIZE,

    RC_COL_SIZE_INVALID,

    RC_TABLE_EXCEEDS_MAX_DECLARED_COLS,

    RC_INDEX_EXCEEDS_MAX_SIZE,

    RC_TABLE_EXCEEDS_MAX_INDEXES,

    RC_TXN_EXCEEDS_MAX_DDLS,

    /** @var Unique constraint violation */
    RC_UNIQUE_VIOLATION,

    RC_TABLE_NOT_FOUND,
    RC_INDEX_NOT_FOUND,
    RC_LOCAL_ROW_FOUND,
    RC_LOCAL_ROW_NOT_FOUND,
    RC_LOCAL_ROW_DELETED,
    RC_INSERT_ON_EXIST,
    RC_INDEX_RETRY_INSERT,
    RC_INDEX_DELETE,
    RC_LOCAL_ROW_NOT_VISIBLE,
    RC_MEMORY_ALLOCATION_ERROR,
    RC_ILLEGAL_ROW_STATE,
    RC_NULL_VIOLATION,
    RC_PANIC,

    /** @var operation currently n.a. */
    RC_NA,
    /** @var The maximum value for return codes. */
    RC_MAX_VALUE,  // This is always the highest value of the RC list
};

/**
 * @brief Converts return code to string value.
 * @param rc The return code.
 * @return The string representation of the return code.
 */
extern const char* RcToString(RC rc);

/**
 * @enum Transaction State.
 * reflects the envelope state
 */
enum TxnState : uint32_t {
    /** @var Transaction Started */
    TXN_START = 0,

    /** @var  Transaction Committed */
    TXN_COMMIT = 1,

    /** @var  Transaction Rolled back */
    TXN_ROLLBACK = 2,

    /** @var  Transaction Prepared */
    TXN_PREPARE = 3,

    /** @var  End Transaction */
    TXN_END_TRANSACTION = 4,
};

enum TxnCommitStatus : uint32_t {
    /** @var Transaction Commit In Progress */
    TXN_COMMIT_IN_PROGRESS = 0,

    /** @var Transaction Committed */
    TXN_COMMITED = 1,

    /** @var Transaction Aborted */
    TXN_ABORTED = 2,

    /** @var Invalid Transaction Commit Status */
    TXN_COMMIT_INVALID = 3
};

/**
 * @enum Transaction validation codes for OCC.
 */
enum class TxnValidation {
    /** @var Do not wait during validation. */
    TXN_VALIDATION_NO_WAIT,

    /** @var Wait during validation. */
    TXN_VALIDATION_WAITING,

    /** @var Invalid validation code. */
    TXN_VALIDATION_ERROR
};

/**
 * @enum Checkpoint phases as defined in CALC paper.
 */
enum CheckpointPhase : uint8_t {
    /** @var Checkpoint phase is unknown. */
    NONE = 0,

    /** @var Checkpoint was not yet triggered. */
    REST = 1,

    /** @var Checkpoint process started. */
    PREPARE = 2,

    /** @var Resolving which data belongs to virtual checkpoint. */
    RESOLVE = 3,

    /** @var Capturing checkpoint data to files. */
    CAPTURE = 4,

    /** @var All data was captured, need to start the cleanup phase. */
    COMPLETE = 5,
};

/**
 * @define Helper macro to assist compiler in generating faster code. Denotes
 * that a branching phrase is likely to succeed most of the times during execution.
 */
#define MOT_EXPECT_TRUE(expr) __builtin_expect((expr), true)

/**
 * @define Helper macro to assist compiler in generating faster code. Denotes
 * that a branching phrase is likely to fail most of the times during execution.
 */
#define MOT_EXPECT_FALSE(expr) __builtin_expect((expr), false)

/**
 * @enum Concurrency control row access codes.
 */
enum AccessType : uint8_t {
    /** @var Invalid row access code. */
    INV,

    /** @var Denotes read row access code. */
    RD,

    /** @var Denotes read for update row access code. */
    RD_FOR_UPDATE,

    /** @var Denotes write row access code. */
    WR,

    /** @var Denotes delete row access code. */
    DEL,

    /** @var Denotes insert row access code. */
    INS,

    /** @var Internal code for testing. */
    SCAN,

    /** @var Unused. */
    TEST,

    INC,

    DEC

};

#define BITMAP_BYTE_IX(x) ((x) >> 3)
#define BITMAP_GETLEN(x) (BITMAP_BYTE_IX(x) + 1)
#define BITMAP_SET(b, x) (b[BITMAP_BYTE_IX(x)] |= (1 << ((x) & 0x07)))
#define BITMAP_CLEAR(b, x) (b[BITMAP_BYTE_IX(x)] &= ~(1 << ((x) & 0x07)))
#define BITMAP_GET(b, x) (b[BITMAP_BYTE_IX(x)] & (1 << ((x) & 0x07)))

/************************************************/
// constants
/************************************************/
#define MAX_KEY_SIZE (256U)
#define MAX_NUM_THREADS (2048U)

#ifndef UINT64_MAX
#define UINT64_MAX (18446744073709551615UL)
#endif  // UINT64_MAX

#define INVALID_TRANSACTION_ID ((TransactionId)0)  // Equal to InvalidTransactionId in the envelope.

// masstree cleanup
#define MASSTREE_OBJECT_COUNT_PER_CLEANUP 8000  // max # of objects to free per rcu_quiesce() call
#define MASSTREE_CLEANUP_THRESHOLD 10000        // max # of object to reclaim before calling reclaiming memory

/* GC PARAMS */
/* Assumed size of a cache line. */
#define CACHE_LINE_SIZE 64
#define CL_SIZE CACHE_LINE_SIZE
#define SENTINEL_SIZE(x) ((x)->GetSentinelSizeFromPool())
#define KEY_SIZE_FROM_POOL(x) ((x)->getKeyPoolSize())
#define ROW_SIZE_FROM_POOL(t) ((t)->GetRowSizeFromPool())
#define ONE_MB 1048576

// prefetch instruction
inline void Prefetch(const void* ptr)
{
#ifdef NOPREFETCH
    (void)ptr;
#else
#if (defined(__x86_64__) || defined(__x86__))
    typedef struct {
        char x[CACHE_LINE_SIZE];
    } cacheline_t;
    asm volatile("prefetcht0 %0" : : "m"(*(const cacheline_t*)ptr));
#else
    __builtin_prefetch(ptr);
#endif
#endif
}

// Isolation Levels
#define SERIALIZABLE 4
#define SNAPSHOT 3
#define REPEATABLE_READ 2
#define READ_COMMITED 1

/* Storage Params */
#define MAX_NUM_INDEXES (10U)
#define MAX_KEY_COLUMNS (10U)
#define MAX_TUPLE_SIZE 16384  // in bytes

#define MAX_VARCHAR_LEN 1024

// Do not change this. Masstree assumes 15 for optimization purposes
#define BTREE_ORDER 15

/** @define Constant denoting indentation used for MOT printouts. */
#define PRINT_REPORT_INDENT 2
}  // namespace MOT

#endif  // MOT_GLOBAL_H
