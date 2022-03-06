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
 *  gstrace_infra.cpp
 *
 *
 * IDENTIFICATION
 *        src/lib/gstrace/common/gstrace_infra.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <pthread.h>
#include <signal.h>
#include <sys/time.h>
#include <sys/syscall.h>
#include <sys/stat.h>
#include <errno.h>
#include "port.h"
#include "securec.h"
#include "securec_check.h"
#include "gstrace/funcs.comps.h"
#include "gstrace/gstrace_infra_int.h"

#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

/* judge whether a char is digital */
#define isDigital(_ch) (((_ch) >= '0') && ((_ch) <= '9'))

/* activate trace ids white list */
#define GS_TRC_CFG_MASK_OPTION (1 << 0)
#define GS_TRC_MASK_MAX_LEN 8192
#define GS_TRC_WAIT_IN_US 1000

#define TRC_BUF_SHARED_MEM_NAME "/gstrace_trace_buffer"
#define TRC_CFG_SHARED_MEM_NAME "/gstrace_trace_cfg"
#define TRC_SHARED_MEM_NAME_MAX 256

#define COMPONENT_IDX(traceId) (((traceId)&GS_TRC_COMP_MASK) >> GS_TRC_COMP_SHIFT)

#define FUNCTION_IDX(traceId) (((traceId)&GS_TRC_FUNC_MASK) >> GS_TRC_FUNC_SHIFT)

#define isTraceEnbled(pTrcCxt) ((pTrcCxt)->pTrcCfg != NULL && (pTrcCxt)->pTrcCfg->bEnabled)

#define FILL_TRACE_HADER(slotAddress, seq, numSlot)              \
    do {                                                         \
        /* Write the slot header */                              \
        trace_slot_head* pHdr = (trace_slot_head*)(slotAddress); \
                                                                 \
        pHdr->hdr_sequence = seq;                                \
        pHdr->hdr_magic_number = SLOT_AREAD_HEADER_MAGIC_NO;     \
        pHdr->num_slots_in_area = numSlot;                       \
    } while (0)

#define FILL_TRACE_RECORD(pBuf, _type, _probe, _rec_id, data_len)                   \
    do {                                                                            \
        trace_record* pRec = (trace_record*)(pBuf);                                 \
        struct timeval tp;                                                          \
        errno_t rc = memset_s(&tp, sizeof(tp), 0, sizeof(tp));                      \
        securec_check(rc, "\0", "\0");                                              \
                                                                                    \
        gettimeofday(&tp, NULL);                                                    \
        pRec->timestamp = tp.tv_sec * USECS_PER_SEC + tp.tv_usec;                   \
        pRec->pid = getpid();                                                       \
        pRec->tid = syscall(__NR_gettid);                                           \
        pRec->type = _type;                                                         \
        pRec->probe = _probe;                                                       \
        pRec->rec_id = _rec_id;                                                     \
                                                                                    \
        /* Include types and lengths in user_data_len */                            \
        pRec->user_data_len = (data_len) + sizeof(trace_data_fmt) + sizeof(size_t); \
    } while (0)

#define FILL_TRACE_PROBEDATA(pBuf, totalSize, fmt_type, data_len, pData)                  \
    do {                                                                                  \
        if ((totalSize) >= (int)(sizeof(trace_data_fmt) + sizeof(size_t) + (data_len))) { \
            int ret;                                                                      \
            char* pDataInBuf = (char*)(pBuf);                                             \
                                                                                          \
            ret = memcpy_s(pDataInBuf, totalSize, &(fmt_type), sizeof(trace_data_fmt));   \
            securec_check(ret, "\0", "\0");                                               \
            pDataInBuf += sizeof(trace_data_fmt);                                         \
            totalSize -= sizeof(trace_data_fmt);                                          \
                                                                                          \
            ret = memcpy_s(pDataInBuf, totalSize, &(data_len), sizeof(size_t));           \
            securec_check(ret, "\0", "\0");                                               \
            pDataInBuf += sizeof(size_t);                                                 \
            totalSize -= sizeof(size_t);                                                  \
                                                                                          \
            ret = memcpy_s(pDataInBuf, totalSize, pData, data_len);                       \
            securec_check(ret, "\0", "\0");                                               \
        }                                                                                 \
    } while (0)

static __thread int* gtCurTryCounter = NULL;

static trace_context* getTraceContext()
{
    static trace_context TRC_GLOBALS;

    return &TRC_GLOBALS;
}

static pthread_mutex_t* getTraceFileMutex()
{
    static pthread_mutex_t fileMutex;

    return &fileMutex;
}

// Rounds given size down to the nearest power of 2 (<= 2^n)
static uint64_t roundToNearestPowerOfTwo(uint64_t initialSize)
{
    uint64_t roundedSize = 0;

    if (initialSize > 0) {
        roundedSize = 1;  // 2^0
        while (initialSize > 1) {
            roundedSize <<= 1;
            initialSize >>= 1;
        }
    }
    return roundedSize;
}

static void addValToBitMap(uint8_t* pBitMap, uint32_t value)
{
    pBitMap[value / GS_TRC_NUM_BITS] |= (1 << (value % GS_TRC_NUM_BITS));
}

// Check if a given string is numeric
bool isNumeric(const char* str)
{
    size_t i = 0;

    while (str[i] != '\0') {
        if (0 == isDigital(str[i])) {
            return false;
        }
        i++;
    }
    return true;
}
static bool parseFuncMask(char* str_func, trace_mask* trc_mask)
{
    /*
     * The specified components need doing parse only when funcs option
     * is "ALL" or at least one func is given by its ID (not trace id)
     * way in which component information is lacking.
     */
    bool bComps = false;
    char* tok = NULL;

    /* handle "ALL", set all the bits! */
    if (0 == strcmp(str_func, "ALL")) {
        bComps = true;
        for (uint32_t v = 0; v < GS_TRC_FUNC_MAX; v++) {
            addValToBitMap(trc_mask->func_bitmap, v);
        }
    } else {
        char* outer_ptr = NULL;

        tok = strtok_s(str_func, ",", &outer_ptr);
        while (tok != NULL) {
            uint32_t func, comp;

            if (!isNumeric(tok)) {
                /* look up the id of func */
                func = getFunctionIdxByName(tok, &comp);

                if (comp > 0) {
                    addValToBitMap(trc_mask->comp_bitmap, comp);
                }
            } else {
                /* the nth function in specified compoents */
                func = atoi(tok);
                bComps = true;
            }

            if (func > 0 && func < GS_TRC_FUNC_MAX) {
                addValToBitMap(trc_mask->func_bitmap, func);
            }
            tok = strtok_s(NULL, ",", &outer_ptr);
        }
    }

    return bComps;
}

static void parseCompMask(char* str_comp, trace_mask* trc_mask)
{
    char* tok = NULL;

    /* handle "ALL", set all the bits! */
    if (0 == strcmp(str_comp, "ALL")) {
        for (uint32_t v = 0; v < GS_TRC_COMP_MAX; v++) {
            addValToBitMap(trc_mask->comp_bitmap, v);
        }
    } else {
        char* outer_ptr = NULL;

        tok = strtok_s(str_comp, ",", &outer_ptr);
        while (tok != NULL) {
            uint32_t comp;

            if (!isNumeric(tok)) {
                /* look up the id of comp */
                comp = getCompIdxByName(tok);
            } else {
                comp = atoi(tok);
            }

            if (comp > 0 && comp < GS_TRC_COMP_MAX) {
                addValToBitMap(trc_mask->comp_bitmap, comp);
            }
            tok = strtok_s(NULL, ",", &outer_ptr);
        }
    }
}

// Parse the mask string
// Output: st_mask, the bit maps
static void parseMask(const char* mask, trace_mask* trc_mask)
{
    char str_mask[GS_TRC_MASK_MAX_LEN + 1] = {'\0'};
    char* str_comp = NULL;
    char* str_func = NULL;
    char* outer_ptr = NULL;

    /* copy it since strtok will make changes. */
    int ret = snprintf_s(str_mask, GS_TRC_MASK_MAX_LEN + 1, GS_TRC_MASK_MAX_LEN, "%s", mask);
    securec_check_ss_c(ret, "\0", "\0");

    /*
     * tokenize the mask string and populate our bitmaps.
     *
     * mask is of form comp.func, for example, 1.7,12 means
     * trace func calls of component 1 and functions 7 and 12
     */
    str_comp = strtok_s(str_mask, ".", &outer_ptr);
    str_func = strtok_s(NULL, ".", &outer_ptr);
    if (str_func != NULL) {
        bool bComps = parseFuncMask(str_func, trc_mask);
        if (str_comp != NULL && bComps) {
            parseCompMask(str_comp, trc_mask);
        }
    }
}

static void getSharedMemName(char* name, size_t len_name, const char* prefix, int pid)
{
    int ret = -1;

    if (len_name >= 1) {
        ret = snprintf_s(name, len_name, (len_name - 1), "%s_%d", prefix, pid);
    }
    securec_check_ss_c(ret, "\0", "\0");
}

/*
 * Attach to either the trace config shared memory buffer or the trace
 * buffer based on the Option passed, and then map the memory into the
 * process address space.
 * This function will NOT create the shared memory if it does not exist.
 */
static trace_msg_code attachTraceSharedMemLow(void** pTrcMem, const char* sMemName, uint64_t size)
{
    int fd;
    if (size == 0) {
        return TRACE_BUFFER_SIZE_ERR;
    }
    fd = shm_open(sMemName, O_RDWR, S_IRWXU);
    if (fd == -1) {
        // Failed to attach to shared memory
        return TRACE_OPEN_SHARE_MEMORY_ERR;
    }

    *pTrcMem = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (*pTrcMem == MAP_FAILED) {
        // failed to map memory
        return TRACE_MMAP_ERR;
    }

    return TRACE_OK;
}

static trace_msg_code detachTraceSharedMemLow(trace_infra* pTrcMem)
{
    if (pTrcMem != NULL && munmap(pTrcMem, pTrcMem->total_size) == -1) {
        return TRACE_MUNMAP_ERR;
    }
    return TRACE_OK;
}

static trace_msg_code attachTraceBufferSharedMem(int key)
{
    trace_msg_code rc;
    void* ptr = NULL;
    char sBufMemName[TRC_SHARED_MEM_NAME_MAX] = {0};
    uint64_t bufferSize;
    trace_context* pTrcCxt = getTraceContext();

    bufferSize = pTrcCxt->pTrcCfg->size;
    getSharedMemName(sBufMemName, sizeof(sBufMemName), TRC_BUF_SHARED_MEM_NAME, key);
    rc = attachTraceSharedMemLow(&ptr, sBufMemName, sizeof(trace_infra) + bufferSize);
    if (rc == TRACE_OK) {
        pTrcCxt->pTrcInfra = (trace_infra*)ptr;
    }

    return rc;
}

static trace_msg_code attachTraceCfgSharedMem(int key)
{
    char sCfgMemName[TRC_SHARED_MEM_NAME_MAX] = {0};
    trace_context* pTrcCxt = getTraceContext();
    trace_msg_code rc;
    void* ptr = NULL;

    getSharedMemName(sCfgMemName, sizeof(sCfgMemName), TRC_CFG_SHARED_MEM_NAME, key);
    rc = attachTraceSharedMemLow(&ptr, sCfgMemName, sizeof(trace_config));
    if (rc == TRACE_OK) {
        pTrcCxt->pTrcCfg = (trace_config*)ptr;
    }

    return rc;
}

/* Attach to buffer shared memory if tracing has been enabled */
static void attachTraceBufferIfEnabled()
{
    trace_context* pTrcCxt = getTraceContext();

    if (pTrcCxt->pTrcInfra == NULL) {
        if (attachTraceBufferSharedMem(pTrcCxt->pTrcCfg->key) != TRACE_OK) {
            printf("Failed to attach to trace buffer shared mem.\n");
        }
    }
}

static void detachTraceBufferIfDisabled()
{
    trace_context* pTrcCxt = getTraceContext();

    (void)detachTraceSharedMemLow(pTrcCxt->pTrcInfra);
    pTrcCxt->pTrcInfra = NULL;
    pTrcCxt->pTrcCfg->status = TRACE_STATUS_END_STOP;
    pTrcCxt->pTrcCfg->bEnabled = false;
}

static bool checkProcess(int port)
{
    int ret;
    bool procexist = false;
    char checkcmd[MAX_PATH_LEN] = {0};
    char buf[MAX_PATH_LEN];
    FILE* fp = NULL;

    ret = snprintf_s(checkcmd, MAX_PATH_LEN, MAX_PATH_LEN - 1, "lsof -i:%d", port);
    securec_check_ss_c(ret, "\0", "\0");

    fp = popen(checkcmd, "r");
    if (fp == NULL) {
        printf("popen failed. could not query database process.\n");
        return procexist;
    }
    while (fgets(buf, sizeof(buf), fp) != NULL) {
        procexist = true;
        break;
    }
    pclose(fp);

    return procexist;
}

void gstrace_destory(int code, void *arg)
{
    trace_context* pTrcCxt = getTraceContext();

    if (pTrcCxt->pTrcCfg != NULL) {
        char sMemName[TRC_SHARED_MEM_NAME_MAX] = {0};
        int key = pTrcCxt->pTrcCfg->key;

        /* munmap trace buffer */
        pTrcCxt->pTrcCfg->status = TRACE_STATUS_BEGIN_STOP;
        while (pTrcCxt->pTrcCfg->status_counter != 0) {
            usleep(GS_TRC_WAIT_IN_US);
        }
        detachTraceBufferIfDisabled();

        /* Don't munmap trace config because may other threads are accessing it. */
        pTrcCxt->pTrcCfg->key = -1;
        pTrcCxt->pTrcCfg->pid = 0;

        /* unlink trace buffer file */
        getSharedMemName(sMemName, sizeof(sMemName), TRC_BUF_SHARED_MEM_NAME, key);
        (void)shm_unlink(sMemName);

        /* unlink trace config file */
        getSharedMemName(sMemName, sizeof(sMemName), TRC_CFG_SHARED_MEM_NAME, key);
        (void)shm_unlink(sMemName);
    }
}

static int try_reuse_configshm(const char* sCfgMemName, int identifer)
{
    int fdCfg = shm_open(sCfgMemName, O_RDWR | O_EXCL, S_IRWXU);
    if (fdCfg == -1) {
        return -1;
    }

    struct stat stat;
    if (fstat(fdCfg, &stat) == 0 && stat.st_size == sizeof(trace_config)) {
        trace_config* pTrcCfg = (trace_config*)mmap(
            NULL, sizeof(trace_config), PROT_READ | PROT_WRITE, MAP_SHARED, fdCfg, 0);

        /* (1) the same MAGIC; (2) the same port; (3) the process not exist. */
        if (pTrcCfg->trc_cfg_magic_no == GS_TRC_CFG_MAGIC_N &&
            pTrcCfg->key == (uint32_t)identifer && kill(pTrcCfg->pid, 0) != 0 && errno == ESRCH) {
            (void)munmap(pTrcCfg, sizeof(trace_config));
            return fdCfg;
        }
        (void)munmap(pTrcCfg, sizeof(trace_config));
    }

    close(fdCfg);
    return -1;
}

int gstrace_init(int key)
{
    trace_context* pTrcCxt = getTraceContext();

    if (pTrcCxt->pTrcCfg != NULL) {
        return TRACE_COMMON_ERROR;
    }

    char sCfgMemName[TRC_SHARED_MEM_NAME_MAX] = {0};
    void* pTrcCfgAddress = NULL;
    const char* hash_str = HASH_TRACE_CONFIG;

    getSharedMemName(sCfgMemName, sizeof(sCfgMemName), TRC_CFG_SHARED_MEM_NAME, key);

    /* initial shared mem open for trace config info */
    int fdCfg = shm_open(sCfgMemName, O_RDWR | O_CREAT | O_EXCL, S_IRWXU);
    if (fdCfg == -1 && errno == EEXIST) {
        fdCfg = try_reuse_configshm(sCfgMemName, key);
    }

    if (fdCfg == -1) {
        // Failed to initialize config shared memory buffer
        return TRACE_COMMON_ERROR;
    }

    if (ftruncate(fdCfg, sizeof(trace_config)) == -1) {
        close(fdCfg);
        // Failed to set size of config shared memory buffer
        return TRACE_COMMON_ERROR;
    }

    /* map the shared memory to our process address space */
    pTrcCfgAddress = mmap(NULL, sizeof(trace_config), PROT_READ | PROT_WRITE, MAP_SHARED, fdCfg, 0);
    close(fdCfg);

    if (pTrcCfgAddress == MAP_FAILED) {
        // failed to map memory for trace cfg
        return TRACE_COMMON_ERROR;
    }

    /* Anchor the memory address in our global so we can use it later. */
    pTrcCxt->pTrcCfg = (trace_config*)pTrcCfgAddress;
    int ret = memset_s(pTrcCxt->pTrcCfg, sizeof(trace_config), 0, sizeof(trace_config));
    securec_check(ret, "\0", "\0");
    pTrcCxt->pTrcCfg->trc_cfg_magic_no = GS_TRC_CFG_MAGIC_N;
    pTrcCxt->pTrcCfg->key = key;
    pTrcCxt->pTrcCfg->pid = getpid();
    pTrcCxt->pTrcCfg->bEnabled = false;
    pTrcCxt->pTrcCfg->version = TRACE_VERSION;
    ret = memcpy_s(
        pTrcCxt->pTrcCfg->hash_trace_config_file, LENGTH_TRACE_CONFIG_HASH, hash_str, LENGTH_TRACE_CONFIG_HASH);
    securec_check(ret, "\0", "\0");

    (void)on_exit(gstrace_destory, NULL);

    return TRACE_OK;
}

static trace_msg_code createAndAttachTraceBuffer(int key, uint64_t bufferSize)
{
    trace_context* pTrcCxt = getTraceContext();
    char sBufMemName[TRC_SHARED_MEM_NAME_MAX] = {0};
    int ret;

    getSharedMemName(sBufMemName, sizeof(sBufMemName), TRC_BUF_SHARED_MEM_NAME, key);
    int fdTrc = shm_open(sBufMemName, O_RDWR | O_CREAT, S_IRWXU);
    if (fdTrc == -1) {
        // Failed to initialize trace shared memory buffer
        return TRACE_OPEN_SHARE_MEMORY_ERR;
    }

    ret = ftruncate(fdTrc, bufferSize + sizeof(trace_infra));
    if (ret == -1) {
        close(fdTrc);
        // Failed to set size of trace shared memory buffer
        return TRACE_TRUNCATE_ERR;
    }

    void* pTrcBufAddress = mmap(NULL, bufferSize + sizeof(trace_infra), PROT_READ | PROT_WRITE, MAP_SHARED, fdTrc, 0);
    close(fdTrc);
    if (pTrcBufAddress == MAP_FAILED) {
        // "failed to map memory for trace buffer
        return TRACE_MMAP_ERR;
    }
    // Anchor our shared memory address in our global variable
    pTrcCxt->pTrcInfra = (trace_infra*)pTrcBufAddress;
    ret = memset_s(pTrcCxt->pTrcInfra, sizeof(trace_infra), 0, sizeof(trace_infra));
    securec_check(ret, "\0", "\0");
    pTrcCxt->pTrcInfra->g_Counter = 0;
    pTrcCxt->pTrcInfra->g_slot_count = 0;
    pTrcCxt->pTrcInfra->total_size = bufferSize + sizeof(trace_infra);
    pTrcCxt->pTrcInfra->num_slots = bufferSize / SLOT_SIZE;
    pTrcCxt->pTrcInfra->slot_index_mask = (bufferSize / SLOT_SIZE) - 1;

    return TRACE_OK;
}

/*
 * init shared memory buffers
 * This function will now create two
 * shared memory buffers, one for the trace
 * config and one for the trace infra.
 */
trace_msg_code gstrace_start(int key, const char* mask, uint64_t bufferSize, const char* trcFile)
{
    trace_msg_code ret;
    bool bTrcToFile = (trcFile != NULL);
    trace_context* pTrcCxt = getTraceContext();

    if (!bTrcToFile && bufferSize <= 0) {
        /* Buffer size of Shared memory should be greater than 0. */
        return TRACE_BUFFER_SIZE_ERR;
    }

    /* align with slot size, and the buffer is expected to fill one trace record at least. */
    bufferSize = gsAlign(((bufferSize > MIN_BUF_SIZE) ? bufferSize : MIN_BUF_SIZE), SLOT_SIZE);
    bufferSize = bTrcToFile ? MIN_BUF_SIZE : roundToNearestPowerOfTwo(bufferSize);

    if (attachTraceCfgSharedMem(key) != TRACE_OK) {
        /* Failed to attached to shared memory. */
        return TRACE_ATTACH_CFG_SHARE_MEMORY_ERR;
    }

    if (pTrcCxt->pTrcCfg->bEnabled) {
        return TRACE_ALREADY_START;
    }

    /* check if database process exist. */
    if (!checkProcess(key)) {
        return TRACE_PROCESS_NOT_EXIST;
    }

    /* set the mask if it's passed in */
    if (mask != NULL) {
        trace_mask st_mask = {0};

        parseMask(mask, &st_mask);
        errno_t rcs = memcpy_s(&pTrcCxt->pTrcCfg->gs_trc_mask, sizeof(trace_mask), &st_mask, sizeof(trace_mask));
        securec_check(rcs, "\0", "\0");
        pTrcCxt->pTrcCfg->options |= GS_TRC_CFG_MASK_OPTION;
    }

    /* create trace buffer */
    ret = createAndAttachTraceBuffer(key, bufferSize);
    if (ret == TRACE_OK) {
        pTrcCxt->pTrcCfg->size = bufferSize;
        pTrcCxt->pTrcCfg->bTrcToFile = bTrcToFile;
        if (bTrcToFile) {
            int rc = snprintf_s(pTrcCxt->pTrcCfg->filePath, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s", trcFile);
            securec_check_ss_c(rc, "\0", "\0");
            ret = TRACE_OK;
        }
        pTrcCxt->pTrcCfg->status = TRACE_STATUS_RECORDING;

        /* After setting up the info, indicate that the trace is enabled */
        pTrcCxt->pTrcCfg->bEnabled = true;
        printf("Shared memory buffer has been initialized.\n");
    }

    return ret;
}

trace_msg_code gstrace_stop(int key)
{
    char sBufMemName[TRC_SHARED_MEM_NAME_MAX] = {0};
    trace_context* pTrcCxt = getTraceContext();

    if (attachTraceCfgSharedMem(key) != TRACE_OK) {
        return TRACE_ATTACH_CFG_SHARE_MEMORY_ERR;
    }

    if (!pTrcCxt->pTrcCfg->bEnabled || pTrcCxt->pTrcCfg->status != TRACE_STATUS_RECORDING) {
        return TRACE_ALREADY_STOP;
    }

    pTrcCxt->pTrcCfg->status = TRACE_STATUS_PREPARE_STOP;
    getSharedMemName(sBufMemName, sizeof(sBufMemName), TRC_BUF_SHARED_MEM_NAME, key);

    while (1) {
        if (!pTrcCxt->pTrcCfg->bEnabled) {
            /* kernel has detached with shared buffer. */
            break;
        } else if (kill(pTrcCxt->pTrcCfg->pid, 0) != 0  && errno == ESRCH) {
            /*
             * case when kernel stopped in advance.
             * Attention: it is unsafty for concurrency among INIT/START/STOP operations now.
             */
            pTrcCxt->pTrcInfra = NULL;
            pTrcCxt->pTrcCfg->status = TRACE_STATUS_END_STOP;
            pTrcCxt->pTrcCfg->bEnabled = false;
            break;
        }
        usleep(GS_TRC_WAIT_IN_US);
    }

    if (shm_unlink(sBufMemName) == -1) {
        perror("shm_unlink");
        return TRACE_UNLINK_SHARE_MEMORY_ERR;
    }

    pTrcCxt->pTrcCfg->size = 0;
    pTrcCxt->pTrcCfg->options = 0;

    printf("Successfully deleted shared mem and cleared trace configuration.\n");
    return TRACE_OK;
}

static bool isTraceIdRequired(const uint32_t rec_id)
{
    trace_context* pTrcCxt = getTraceContext();
    trace_config* cfg = pTrcCxt->pTrcCfg;

    if (cfg->options & GS_TRC_CFG_MASK_OPTION) {
        uint32_t comp_idx, func_idx, nword, nbits;

        comp_idx = COMPONENT_IDX(rec_id);
        nword = comp_idx / GS_TRC_NUM_BITS;
        nbits = comp_idx % GS_TRC_NUM_BITS;
        if (cfg->gs_trc_mask.comp_bitmap[nword] & (1 << nbits)) {
            return true;
        }

        func_idx = FUNCTION_IDX(rec_id);
        nword = func_idx / GS_TRC_NUM_BITS;
        nbits = func_idx % GS_TRC_NUM_BITS;
        if (cfg->gs_trc_mask.func_bitmap[nword] & (1 << nbits)) {
            return true;
        }
        return false;
    }

    return true;
}

static uint32_t gsTrcCalcNeededNumOfSlots(size_t len)
{
    if (len > 0) {
        size_t tSize = len;

        tSize += sizeof(trace_slot_head) + sizeof(trace_slot_tail);
        return gsAlign(tSize, SLOT_SIZE) / SLOT_SIZE;
    } else {
        return 0;
    }
}

/*
 * if pData is a string, it must be null ermainted and the data_len
 * must include the null character.
 */
static void do_memory_trace(const trace_type type, const uint32_t probe, const uint32_t rec_id,
    const trace_data_fmt fmt_type, const char* pData, size_t data_len)
{
    uint64_t sequence, slotIndex;
    trace_context* pTrcCxt = getTraceContext();

    sequence = __sync_fetch_and_add(&pTrcCxt->pTrcInfra->g_Counter, 1);

    /* Calculate how many slots for data, and one slot for header slot */
    uint32_t numSlotsNeeded = (pData == NULL) ? 1 : 1 + gsTrcCalcNeededNumOfSlots(data_len);
    if (numSlotsNeeded == 1) {
        /* Sequence will be the atomic value just before the increment */
        sequence = __sync_fetch_and_add(&pTrcCxt->pTrcInfra->g_slot_count, 1);
        slotIndex = sequence & pTrcCxt->pTrcInfra->slot_index_mask;
    } else {
        /* if multiple slots is required, it must be contiguous */
        for (;;) {
            sequence = __sync_fetch_and_add(&pTrcCxt->pTrcInfra->g_slot_count, numSlotsNeeded);
            slotIndex = sequence & pTrcCxt->pTrcInfra->slot_index_mask;

            /*
             * if the start slot index and the number of slots does not exceed
             * the total number of slots in the buffer that means the buffer end
             * is not in the middle of slot area (slot area is contiguous).
             */
            if ((slotIndex + numSlotsNeeded) <= pTrcCxt->pTrcInfra->num_slots) {
                break;
            }
        }
    }

    /* Get address of the first assigned slot in buffer while slotIndex starts at 0. */
    void* slotAddress = (char*)pTrcCxt->pTrcInfra + sizeof(trace_infra) + slotIndex * SLOT_SIZE;

    /* Write the slot header */
    FILL_TRACE_HADER(slotAddress, sequence, numSlotsNeeded);

    /* advance to the record offset */
    char* pBuf = (char*)slotAddress + sizeof(trace_slot_head);

    /* Write our trace record information */
    FILL_TRACE_RECORD(pBuf, type, probe, rec_id, data_len);

    /* Write the trace data as a series of triples */
    /* each triple is: (fmtType, dataSize, data) */
    if (pData != NULL) {
        int totalSize = numSlotsNeeded * SLOT_SIZE - sizeof(trace_record) - sizeof(trace_slot_head);

        pBuf = (char*)slotAddress + sizeof(trace_slot_head) + sizeof(trace_record);
        FILL_TRACE_PROBEDATA(pBuf, totalSize, fmt_type, data_len, pData);
    }

    /* Now write slot tail to indicate changes are committed */
    pBuf = ((char*)slotAddress + (numSlotsNeeded * SLOT_SIZE)) - sizeof(trace_slot_tail);
    trace_slot_tail* pTail = (trace_slot_tail*)pBuf;

    /* put a full memory barrier to ensure no compiler or cpu reordering. */
    __sync_synchronize();
    pTail->tail_sequence = ~(sequence);
}

static void write_trace_file(const void* pSlot, off_t offset, int size)
{
    trace_context* pTrcCxt = getTraceContext();
    FILE* fp = NULL;

    if (pthread_mutex_lock(getTraceFileMutex()) != 0) {
        printf("Failed to lock the output file.\n");
        return;
    }

    /* write pSlot to file */
    if (access(pTrcCxt->pTrcCfg->filePath, F_OK) == -1) {
        /* file not existed */
        fp = trace_fopen(pTrcCxt->pTrcCfg->filePath, "a+");
        if (fp != NULL) {
            if ((fwrite(pTrcCxt->pTrcCfg, sizeof(trace_config), 1, fp) != 1) ||
                (fwrite(pTrcCxt->pTrcInfra, sizeof(trace_infra), 1, fp) != 1) || (fwrite(pSlot, size, 1, fp) != 1)) {
                printf("Failed to write trace file.\n");
            }
        }
    } else {
        /* file existed, append to it */
        fp = trace_fopen(pTrcCxt->pTrcCfg->filePath, "r+");
        if (fp != NULL) {
            if ((fseek(fp, 0, SEEK_SET) != 0) || (fwrite(pTrcCxt->pTrcCfg, sizeof(trace_config), 1, fp) != 1) ||
                (fseek(fp, sizeof(trace_config), SEEK_SET) != 0) ||
                (fwrite(pTrcCxt->pTrcInfra, sizeof(trace_infra), 1, fp) != 1) || (fseek(fp, offset, SEEK_SET) != 0) ||
                (fwrite(pSlot, size, 1, fp) != 1)) {
                printf("Failed to append write trace file.\n");
            }
        }
    }

    if (fp != NULL) {
        (void)fflush(fp);
        (void)trace_fclose(fp);
    }

    (void)pthread_mutex_unlock(getTraceFileMutex());
}

// Trace to file, the format of the output file is the same as the function gstrace_dump
static void do_file_trace(const trace_type type, const uint32_t probe, const uint32_t rec_id,
    const trace_data_fmt fmt_type, const char* pData, size_t data_len)
{
    uint64_t sequence;
    void* pSlot = NULL;
    uint32_t numSlotsNeeded;
    off_t offset;
    trace_context* pTrcCxt = getTraceContext();

    /* Calculate how many slots for data, and one slot for header slot */
    numSlotsNeeded = (pData == NULL) ? 1 : 1 + gsTrcCalcNeededNumOfSlots(data_len);

    /* Increase record count by 1 */
    __sync_fetch_and_add(&pTrcCxt->pTrcInfra->g_Counter, 1);

    /* Sequence will be the atomic value just before the increment */
    sequence = __sync_fetch_and_add(&pTrcCxt->pTrcInfra->g_slot_count, numSlotsNeeded);
    offset = sizeof(trace_config) + sizeof(trace_infra) + SLOT_SIZE * sequence;

    /*
     * This size can be explained as "file size" (equal to "buffer size" in
     * do_memory_trace) which is expected to be used to calculate the max
     * slot number in format function.
     */
    pTrcCxt->pTrcCfg->size = 2 * roundToNearestPowerOfTwo((sequence + numSlotsNeeded) * SLOT_SIZE);

    int totalSize = SLOT_SIZE * numSlotsNeeded;
    pSlot = (void*)malloc(totalSize);
    if (pSlot != NULL) {
        /* Write the slot header */
        FILL_TRACE_HADER(pSlot, sequence, numSlotsNeeded);

        /* advance to the record offset */
        char* pBuf = (char*)pSlot + sizeof(trace_slot_head);

        /* Write our trace record information */
        FILL_TRACE_RECORD(pBuf, type, probe, rec_id, data_len);

        /* Write the data as a series of triples */
        /* each triple is: (fmtType, dataSize, data) */
        if (pData != NULL) {
            int bufSize = totalSize - sizeof(trace_slot_head) - sizeof(trace_record);

            pBuf = (char*)pSlot + sizeof(trace_slot_head) + sizeof(trace_record);
            FILL_TRACE_PROBEDATA(pBuf, bufSize, fmt_type, data_len, pData);
        }

        /* Now write slot tail to indicate changes are committed */
        pBuf = ((char*)pSlot + (numSlotsNeeded * SLOT_SIZE)) - sizeof(trace_slot_tail);
        trace_slot_tail* pTail = (trace_slot_tail*)pBuf;
        pTail->tail_sequence = ~(sequence);

        write_trace_file(pSlot, offset, totalSize);
        free(pSlot);
    }
}

/* write trace record. This primarily considers concurrency with gstrace tool. */
static void gstrace_internal(const trace_type type, const uint32_t probe, const uint32_t rec_id,
    const trace_data_fmt fmt_type, const char* pData, size_t data_len)
{
    trace_context* pTrcCxt = getTraceContext();

    /*
     * (1) while it is going to destory trace config, do nothing.
     * (2) trace is not initialized or activated.
     * (3) traceId is not in whitelist.
     */
    if (unlikely(isTraceEnbled(pTrcCxt)) && isTraceIdRequired(rec_id)) {
        if (likely(pTrcCxt->pTrcCfg->status == TRACE_STATUS_RECORDING)) {
            __sync_fetch_and_add(&pTrcCxt->pTrcCfg->status_counter, 1);
            /*
             * After increasing reference counting, do work if trace status is
             * still RECORDING.
             */
            if (pTrcCxt->pTrcCfg->status == TRACE_STATUS_RECORDING) {
                /* attach to the shared buffer memory if trace has been enabled. */
                attachTraceBufferIfEnabled();

                if (pTrcCxt->pTrcInfra != NULL) {
                    /* write trace record into memory map file or disk file */
                    if (pTrcCxt->pTrcCfg->bTrcToFile) {
                        do_file_trace(type, probe, rec_id, fmt_type, pData, data_len);
                    } else {
                        do_memory_trace(type, probe, rec_id, fmt_type, pData, data_len);
                    }
                }
            }
            __sync_fetch_and_sub(&pTrcCxt->pTrcCfg->status_counter, 1);
        } else if (pTrcCxt->pTrcCfg->status == TRACE_STATUS_PREPARE_STOP) {
            /* Detach me from the trace buffer's shared memory */
            /* others are writing trace records, do nothing before finish. */
            if (pTrcCxt->pTrcCfg->status_counter != 0) {
            } else {
                /* the first writer, who have detected stop operation, does unmap. */
                if (__sync_bool_compare_and_swap(
                        &pTrcCxt->pTrcCfg->status, TRACE_STATUS_PREPARE_STOP, TRACE_STATUS_BEGIN_STOP)) {
                    detachTraceBufferIfDisabled();
                }
            }
        } else {
            /* do nothing during unmap process since trace is disable */
        }
    }
}

void gstrace_data(
    const uint32_t probe, const uint32_t rec_id, const trace_data_fmt fmt_type, const char* pData, size_t data_len)
{
    /* do nothing if data size is too big. One for head and tail, another for fmt_type and size. */
    if (data_len <= SLOT_SIZE * (MAX_TRC_SLOTS - 1 - 1)) {
        gstrace_internal(TRACE_DATA, probe, rec_id, fmt_type, pData, data_len);
    }
}

void gstrace_entry(const uint32_t rec_id)
{
    if (gtCurTryCounter != NULL) {
        (*gtCurTryCounter)++;
    }
    gstrace_internal(TRACE_ENTRY, 0, rec_id, TRC_DATA_FMT_NONE, NULL, 0);
}

void gstrace_exit(const uint32_t rec_id)
{
    gstrace_internal(TRACE_EXIT, 0, rec_id, TRC_DATA_FMT_NONE, NULL, 0);
    if (gtCurTryCounter != NULL) {
        (*gtCurTryCounter)--;
    }
}

int* gstrace_tryblock_entry(int* newTryCounter)
{
    int* oldTryCounter = gtCurTryCounter;
    gtCurTryCounter = newTryCounter;
    *gtCurTryCounter = 0;

    return oldTryCounter;
}

void gstrace_tryblock_exit(bool inCatch, int* oldTryCounter)
{
    if (inCatch && gtCurTryCounter != NULL && *gtCurTryCounter != 0) {
        /* To deal with mismatch between ENTRY and EXIT, such as longjump
         * code, try/catch and so on, an special trace id(GS_TRC_ID_TRY_CATCH)
         * is used here to record gtCurTryCounter which stands for the number
         * of missing EXITs. While parsing the dumped trace file, we can create
         * so many EXITs for all unmatched ENTRYs. */
        gstrace_internal(TRACE_EXIT, 0, GS_TRC_ID_TRY_CATCH, TRC_DATA_FMT_NONE, NULL, *gtCurTryCounter);
    }
    gtCurTryCounter = oldTryCounter;
}

static trace_msg_code dump_trace_context(int fd)
{
    trace_context* pTrcCxt = getTraceContext();
    size_t bytesWritten;

    // Write the trace config header first
    bytesWritten = write(fd, (void*)pTrcCxt->pTrcCfg, sizeof(trace_config));
    if (bytesWritten != sizeof(trace_config)) {
        return TRACE_WRITE_CFG_HEADER_ERR;
    }

    // Write the trace buffer header next
    bytesWritten = write(fd, (void*)pTrcCxt->pTrcInfra, sizeof(trace_infra));
    if (bytesWritten != sizeof(trace_infra)) {
        return TRACE_WRITE_BUFFER_HEADER_ERR;
    } else {
        return TRACE_OK;
    }
}

static trace_msg_code dump_trace_buffer(int fd, const char* outPath)
{
    trace_context* pTrcCxt = getTraceContext();
    size_t bytesToWrite;

    /* get the address of the beginning of trace data */
    char* pBuf = (char*)pTrcCxt->pTrcInfra + sizeof(trace_infra);
    /* Write the remainder of the buffer */
    bytesToWrite = pTrcCxt->pTrcInfra->total_size - sizeof(trace_infra);
    while (bytesToWrite > 0) {
        int64_t nbyte = write(fd, (void*)pBuf, bytesToWrite); 
        if (nbyte < 0) {
            break;
        }
        pBuf += nbyte;
        bytesToWrite -= nbyte; 
    }

    if (bytesToWrite != 0) {
        return TRACE_WRITE_BUFFER_ERR;
    } else {
        printf("Shared memory buffer has been dumped to file: %s.\n", outPath);
        return TRACE_OK;
    }
}

// This function will dump the trace
// buffer to a binary file
// Precondition:  process already attached to
// trace buffer shared memory.
//
// path will contain path to output file
// if outPath is null or of zero length
// this function will put the dump in
// "tmp/trace_dump"
// -------------------------------------------
trace_msg_code gstrace_dump(int key, const char* outPath)
{
    trace_context* pTrcCxt = getTraceContext();
    trace_msg_code ret;

    if (attachTraceCfgSharedMem(key) != TRACE_OK) {
        return TRACE_ATTACH_CFG_SHARE_MEMORY_ERR;
    }

    if (!pTrcCxt->pTrcCfg->bEnabled) {
        return TRACE_DISABLE_ERR;
    }

    if (attachTraceBufferSharedMem(key) != TRACE_OK) {
        return TRACE_ATTACH_BUFFER_SHARE_MEMORY_ERR;
    }

    // open output file
    int fd = trace_open_filedesc(outPath, O_RDWR | O_CREAT | O_TRUNC, S_IRWXU);
    if (fd == -1) {
        return TRACE_OPEN_OUTPUT_FILE_ERR;
    }

    ret = dump_trace_context(fd);
    if (ret == TRACE_OK) {
        ret = dump_trace_buffer(fd, outPath);
    }

    (void)trace_close_filedesc(fd);
    return ret;
}

static char* getStatusString(int status)
{
    switch (status) {
        case TRACE_STATUS_RECORDING:
            return "TRACING";
        case TRACE_STATUS_PREPARE_STOP:
            return "PREPARE STOP";
        case TRACE_STATUS_BEGIN_STOP:
            return "STOPPING";
        case TRACE_STATUS_END_STOP:
        default:
            break;
    }

    return "STOPPED";
}

trace_msg_code gstrace_config(int key)
{
    int rc;
    trace_context* pTrcCxt = getTraceContext();

    rc = attachTraceCfgSharedMem(key);
    if (rc != TRACE_OK) {
        return TRACE_ATTACH_CFG_SHARE_MEMORY_ERR;
    }

    if (pTrcCxt->pTrcCfg->bEnabled) {
        printf("gstrace is enabled\n");
        printf("Trace buffer size is %lu bytes\n", pTrcCxt->pTrcCfg->size);
        printf("Trace status is %s\n", getStatusString(pTrcCxt->pTrcCfg->status));
        printf("Trace status counter is %d\n", pTrcCxt->pTrcCfg->status_counter);
    } else {
        printf("gstrace is NOT enabled\n");
    }
    return TRACE_OK;
}

uint32_t getFunctionIdxByName(const char* funcName, uint32_t* comp)
{
    uint32_t nComps = sizeof(GS_TRC_COMP_NAMES_BY_COMP) / sizeof(comp_name);

    for (uint32_t i = 1; i < nComps; ++i) {
        for (uint32_t j = 1; j < GS_TRC_FUNC_NAMES_BY_COMP[i].func_num; ++j) {
            if (0 == strcmp(GS_TRC_FUNC_NAMES_BY_COMP[i].func_names[j], funcName)) {
                *comp = i;
                return j;
            }
        }
    }
    *comp = 0;
    return 0;
}

uint32_t getCompIdxByName(const char* compName)
{
    uint32_t nComps = sizeof(GS_TRC_COMP_NAMES_BY_COMP) / sizeof(comp_name);

    for (uint32_t i = 1; i < nComps; ++i) {
        if (0 == strcmp(GS_TRC_COMP_NAMES_BY_COMP[i].compName, compName)) {
            return i;
        }
    }
    return 0;
}

const char* getCompNameById(uint32_t trc_id)
{
    uint32_t nComps = sizeof(GS_TRC_COMP_NAMES_BY_COMP) / sizeof(comp_name);
    uint32_t comp_idx = COMPONENT_IDX(trc_id);

    for (uint32_t i = 1; i < nComps; ++i) {
        if (GS_TRC_COMP_NAMES_BY_COMP[i].comp == comp_idx) {
            return GS_TRC_COMP_NAMES_BY_COMP[i].compName;
        }
    }

    return "unknown";
}

const char* getTraceConfigHash(void)
{
    return HASH_TRACE_CONFIG;
}

const char* getTraceFunctionName(uint32_t trc_id)
{
    uint32_t comp_idx = COMPONENT_IDX(trc_id);
    uint32_t func_idx = FUNCTION_IDX(trc_id);

    return GS_TRC_FUNC_NAMES_BY_COMP[comp_idx].func_names[func_idx];
}

const char* getTraceTypeString(trace_type type)
{
    switch (type) {
        case TRACE_ENTRY:
            return "ENTRY";
        case TRACE_EXIT:
            return "EXIT";
        case TRACE_DATA:
            return "DATA";
        default:
            return "UNKNOWN TRACE TYPE";
    }
}

FILE* trace_fopen(const char* open_file, const char* mode)
{
    char file_name[MAX_PATH_LEN];
    FILE* fp = NULL;
    int ret = snprintf_s(file_name, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s", open_file);
    securec_check_ss_c(ret, "\0", "\0");

    canonicalize_path(file_name);

    fp = fopen(file_name, mode);
    if (chmod(file_name, S_IWUSR | S_IRUSR) != 0) {
        fclose(fp);
        return NULL;
    }

    return fp;
}

int trace_fclose(FILE* stream)
{
    return (stream != NULL) ? fclose(stream) : 0;
}

int trace_open_filedesc(const char* open_file, int oflag, int mode)
{
    char file_name[MAX_PATH_LEN];
    int ret = snprintf_s(file_name, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s", open_file);
    securec_check_ss_c(ret, "\0", "\0");

    canonicalize_path(file_name);
    return open(file_name, oflag, mode);
}

int trace_close_filedesc(int fd)
{
    return (fd != -1) ? close(fd) : 0;
}
