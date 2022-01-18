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
 * ---------------------------------------------------------------------------------------
 * 
 * gstrace_infra_int.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/gstrace/gstrace_infra_int.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef TRACE_INFRA_INT_H_
#define TRACE_INFRA_INT_H_

#include <stdint.h>
#include "gstrace/gstrace_infra.h"
#include "gstrace/system_gstrace.h"
// -----------------------------------------------------------------------------
// Global #defines
// -----------------------------------------------------------------------------
#define SLOT_SIZE 64
#define MAX_TRC_RC_SZ 2048  // 2^11
#define MAX_TRC_SLOTS (MAX_TRC_RC_SZ / SLOT_SIZE)
#define MAX_PATH_LEN 1024
#define LENGTH_TRACE_CONFIG_HASH (64)

#define TRACE_VERSION (0x1)
#define SLOT_AREAD_HEADER_MAGIC_NO 0xBCD7BCD7
#define GS_TRC_CFG_MAGIC_N 0xABCDABCE

#define MIN_BUF_SIZE MAX_TRC_RC_SZ  // The buffer size when it's tracing to file
#define DFT_BUF_SIZE 1073741824     // Default trace buffer size.  1GB.  must be a power of 2

#define CACHE_LINE_SIZE 64

#define USECS_PER_SEC ((uint64_t)(1000000))

/* some mask related functions */
#define GS_TRC_COMP_MASK 0x0FF0000
#define GS_TRC_FUNC_MASK 0x000FFFF
#define GS_TRC_NUM_BITS 8
#define GS_TRC_COMP_MAX 256
#define GS_TRC_COMP_MASK_SZ (GS_TRC_COMP_MAX / GS_TRC_NUM_BITS)  // 32
#define GS_TRC_FUNC_MAX 65536
#define GS_TRC_FUNC_MASK_SZ (GS_TRC_FUNC_MAX / GS_TRC_NUM_BITS)  // 8192

#define TRACE_COMMON_ERROR (-1)

typedef enum trace_msg_code {
    TRACE_OK,
    TRACE_ALREADY_START,
    TRACE_ALREADY_STOP,
    TRACE_PARAMETER_ERR,
    TRACE_BUFFER_SIZE_ERR,
    TRACE_ATTACH_CFG_SHARE_MEMORY_ERR,
    TRACE_ATTACH_BUFFER_SHARE_MEMORY_ERR,
    TRACE_OPEN_SHARE_MEMORY_ERR,
    TRACE_TRUNCATE_ERR,
    TRACE_MMAP_ERR,
    TRACE_MUNMAP_ERR,
    TRACE_UNLINK_SHARE_MEMORY_ERR,
    TRACE_DISABLE_ERR,
    TRACE_OPEN_OUTPUT_FILE_ERR,
    TRACE_OPEN_INPUT_FILE_ERR,
    TRACE_WRITE_BUFFER_HEADER_ERR,
    TRACE_WRITE_CFG_HEADER_ERR,
    TRACE_WRITE_BUFFER_ERR,
    TRACE_READ_CFG_FROM_FILE_ERR,
    TRACE_BUFFER_SIZE_FROM_FILE_ERR,
    TRACE_MAGIC_FROM_FILE_ERR,
    TRACE_READ_INFRA_FROM_FILE_ERR,
    TRACE_NO_RECORDS_ERR,
    TRACE_READ_SLOT_HEADER_ERR,
    TRACE_NUM_SLOT_ERR,
    TRACE_SLOT_MAGIC_ERR,
    TRACE_READ_SLOT_DATA_ERR,
    TRACE_WRITE_FORMATTED_RECORD_ERR,
    TRACE_STATR_SLOT_ERR,
    TRACE_TAIL_OFFSET_ERR,
    TRACE_SEQ_ERR,
    TRACE_VERSION_ERR,
    TRACE_CONFIG_SIZE_ERR,
    TRACE_PROCESS_NOT_EXIST,
    TRACE_MSG_MAX,
} trace_msg_code;

typedef struct trace_msg {
    trace_msg_code msg_code;
    const char* msg_string;
} trace_msg_t;


#define securec_check(errno, charList, ...)                                         \
    do {                                                                            \
        if (errno != EOK) {                                                         \
            freeSecurityFuncSpace_c(charList, ##__VA_ARGS__);                       \
            printf("ERROR at %s : %d : The destination buffer or format is "        \
                   "a NULL pointer or the invalid parameter handle is invoked..\n", \
                __FILE__,                                                           \
                __LINE__);                                                          \
            exit(1);                                                                \
        }                                                                           \
    } while (0)

/* White list for trace Id */
typedef struct trace_mask {
    /* while component's traceId is active */
    uint8_t comp_bitmap[GS_TRC_COMP_MASK_SZ];

    /* while function's traceId is active */
    uint8_t func_bitmap[GS_TRC_FUNC_MASK_SZ];
} trace_mask;

typedef enum trace_status {
    TRACE_STATUS_END_STOP = 0,
    TRACE_STATUS_RECORDING,
    TRACE_STATUS_PREPARE_STOP,
    TRACE_STATUS_BEGIN_STOP
} trace_status;

typedef enum trace_type { TRACE_ENTRY, TRACE_EXIT, TRACE_DATA } trace_type;

/*
 * Trace Buffer Header
 * The writable buffer will start right after this struct.
 */
typedef struct trace_infra {
    /* the number of trace records since startup */
    volatile uint64_t g_Counter;

    /* the number of used slots since startup */
    volatile uint64_t g_slot_count;

    /* ensure the previous two field in an sepreate cacheline */
    char pad[CACHE_LINE_SIZE - 2 * sizeof(uint64_t)];

    /* buffer size + size of this struct */
    uint64_t total_size;

    /* the number of trace slots in writable region */
    uint64_t num_slots;

    /* Used to calculate slot index in an efficient manner */
    uint64_t slot_index_mask;

    /* ensure the previous three fields in an sepreate cacheline */
    char pad2[CACHE_LINE_SIZE - 3 * sizeof(uint64_t)];
} trace_infra;

typedef struct trace_config {
    /* a magic number for validation */
    uint32_t trc_cfg_magic_no;

    /* user's identifier, now it is kernel's listening port. */
    uint32_t key;

    /* mark which process are traced */
    pid_t pid;

    /* whether or not trace is activated */
    volatile bool bEnabled;

    /* record trace into disk file or memory mapping file */
    bool bTrcToFile;

    /* Trace buffer size */
    uint64_t size;

    /* additional options, such as traceId white list */
    uint64_t options;

    /* if bTrcToFile is true, records are write down this file */
    char filePath[MAX_PATH_LEN];

    volatile int status;
    volatile int status_counter;

    /* trace ids white list */
    trace_mask gs_trc_mask;
    /* gstrace version. default value is TRACE_VERSION.
     * if change struct trace_config or trace_infra, should add 1 on TRACE_VERSION */
    uint32_t version;
    /* hash sum value of trace config files in gstrace/config. sha256(sha256(access.in) + sha256(bootstrap.in)+..)
     * default value is HASH_TRACE_CONFIG which auto generate by trc_gen.py. */
    char hash_trace_config_file[LENGTH_TRACE_CONFIG_HASH];
} trace_config;

typedef struct trace_context {
    /* address of trace infra struct in shared mem */
    trace_infra* pTrcInfra;

    /* address of trace config buffer in shared mem */
    trace_config* pTrcCfg;
} trace_context;

// slot area header is 16 bytes
// slot aread tail is 8 bytes
// trace record ( payload of the first slot ) is 28 bytes
// that leaves us with 12 free bytes ( 64 - 52 ) in the first slot
// we can us them for rc or code path value
typedef struct trace_slot_head {
    uint64_t hdr_sequence;
    uint32_t hdr_magic_number;
    uint32_t num_slots_in_area;
} trace_slot_head;

typedef struct trace_slot_tail {
    uint64_t tail_sequence;
} trace_slot_tail;

// total size of trace record is 40 bytes
// and we have 16 bytes for slot header and 8 for tail
// that makes up 64 bytes which just fits in on slot!!
// ----------------------------------------------------
typedef struct trace_record {
    uint64_t timestamp;
    pid_t pid;
    pid_t tid;
    trace_type type;  // entry, exit or data
    uint32_t probe;   // probe point in function
    uint32_t rec_id;  // traceId that contains info on function and component etc.
    uint32_t rc;
    uint32_t un_used;  // the compiler will reserve 40 bytes for the whole structre any way,
                       // due to alignment, may as well use it explicitly
    uint32_t user_data_len;
} trace_record;

extern trace_msg_code gstrace_start(int pid, const char* mask, uint64_t bufferSize, const char* trcFile);
extern trace_msg_code gstrace_stop(int pid);
extern trace_msg_code gstrace_dump(int pid, const char* outPath);
extern trace_msg_code gstrace_config(int pid);
extern bool isNumeric(const char* str);

extern uint32_t getFunctionIdxByName(const char* funcName, uint32_t* comp);
extern uint32_t getCompIdxByName(const char* compName);
extern const char* getCompNameById(uint32_t trc_id);
extern const char* getTraceConfigHash(void);
extern const char* getTraceFunctionName(uint32_t trc_id);
extern const char* getTraceTypeString(trace_type type);

// Will round up i to the nearest multiple of
// X.  X must be a power of 2
inline uint32_t gsAlign(uint32_t i, uint32_t X)
{
    return ((i + (X - 1)) & ~(uint32_t)(X - 1));
}

extern FILE* trace_fopen(const char* file_name, const char* mode);
extern int trace_fclose(FILE* stream);
extern int trace_open_filedesc(const char* file_name, int oflag, int mode);
extern int trace_close_filedesc(int fd);

#endif /* TRACE_INFRA_INT_H_ */
