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
 * sys_numa_api.cpp
 *    System NUMA APIs.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/sys_numa_api.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "sys_numa_api.h"
#include "utilities.h"
#include "mot_configuration.h"
#include "global.h"
#include "string_buffer.h"

#include "libintl.h"
#include "postgres.h"
#include "knl/knl_thread.h"

#include <cstddef>
#include <cstring>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <cstdlib>
#include <unistd.h>
#include <cerrno>
#include <cstdarg>
#include <dirent.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/resource.h>

#ifndef PACKED
#define PACKED __attribute__((packed))
#endif

/* Flags for mbind */
#define MPOL_MF_STRICT (1U)        /* Verify existing pages in the mapping */
#define MPOL_MF_MOVE (1U << 1)     /* Move pages owned by this process to conform to mapping */
#define MPOL_MF_MOVE_ALL (1U << 2) /* Move every page to conform to mapping */

// some required utility macros
#define ROUND_UP(x, y) (((x) + (y) - 1) & ~((y) - 1))
#define CPU_BYTES(x) (ROUND_UP(x, sizeof(long)))
#define CPU_LONGS(x) (CPU_BYTES(x) / sizeof(long))

#define HOW_MANY(x, y) (((x) + ((y) - 1)) / (y))
#define BITS_PER_ULONG ((unsigned int)8 * sizeof(unsigned long))
#define BITS_PER_UINT ((unsigned int)8 * sizeof(unsigned int))
#define LONGS_PER_BITS(n) HOW_MANY(n, BITS_PER_ULONG)
#define BYTES_PER_BITS(x) ((x + 7) / 8)

// define maximum number of NUMA nodes
#if defined(__x86_64__) || defined(__i386__)
#define NUMA_NUM_NODES 128
#else
#define NUMA_NUM_NODES 2048
#endif

namespace MOT {
#define NUMA_FILE_PATH_LEN 64
#define STR_TO_NBITS(s) (strlen(s) * 32 / 9)
#define STR_PREFIX(s, pre) (strncmp(s, pre, strlen(pre)) == 0)

DECLARE_LOGGER(SysNumaApi, Memory)

typedef struct NodeMask_ST {
    unsigned long m_n[NUMA_NUM_NODES / (sizeof(unsigned long) * 8)];
} NodeMaskSt;

#define NODEMASK_COMPACT(mask, node) \
    (((NodeMaskSt*)mask)->m_n[node / (8 * sizeof(unsigned long))] |= (1UL << (node % (8 * sizeof(unsigned long)))))

typedef struct PACKED BitMask_ST {
    unsigned long m_size; /* number of bits in the map */
    unsigned long m_maskp[0];
} BitMaskSt;

#define BITMASK_NBYTES(bmp) (LONGS_PER_BITS((bmp)->m_size) * sizeof(unsigned long))

#define BITMASK_GETBIT(bmp, n)                                                                                   \
    (((unsigned int)(n) < (bmp)->m_size)                                                                         \
            ? (((bmp)->m_maskp[(unsigned int)(n) / BITS_PER_ULONG] >> ((unsigned int)(n) % BITS_PER_ULONG)) & 1) \
            : 0)

#define BITMASK_SETBIT(bmp, n)                                                                             \
    if ((unsigned int)(n) < (bmp)->m_size) {                                                               \
        (bmp)->m_maskp[(unsigned int)(n) / BITS_PER_ULONG] |= 1UL << ((unsigned int)(n) % BITS_PER_ULONG); \
    }

#define BITMASK_CLEARBIT(bmp, n)                                                                              \
    if ((unsigned int)(n) < (bmp)->m_size) {                                                                  \
        (bmp)->m_maskp[(unsigned int)(n) / BITS_PER_ULONG] &= ~(1UL << ((unsigned int)(n) % BITS_PER_ULONG)); \
    }

#define BITMASK_FREE(x) \
    if (x != nullptr) { \
        free(x);        \
        x = nullptr;    \
    }

#define BITMASK_ONSTACK(name, x)                                                 \
    size_t sz = sizeof(BitMaskSt) + (LONGS_PER_BITS(x) * sizeof(unsigned long)); \
    char _bmp_buf[sz];                                                           \
    BitMaskSt* name = (BitMaskSt*)_bmp_buf;                                      \
    bzero(name, sz);                                                             \
    name->m_size = x;

// Report errors/warnings
static void MotSysNumaReportError(const char* msg);
static void MotSysNumaReportWarn(const char* msg, ...);

// bitmask routines
static BitMaskSt* MotSysNumaBitmaskAlloc(unsigned int n);
static unsigned int MotSysNumaBitmaskWeight(const BitMaskSt* bmp);
static int MotSysNumaParseBitmap(char* line, BitMaskSt* mask);

// Forward declarations
static void MotSysNumaNodeCpuMaskCleanup();
static void MotSysNumaNodeCpuMaskInit();

// Helpers
static long long MotSysNumaNodeSize(int node);
static int MotSysNumaReadBitmask(char* s, BitMaskSt* bmp);
static void MotSysNumaSetNodemaskSize();
static void MotSysNumaSetConfiguredNodes();
static void MotSysNumaSetNumaMaxCpu();
static void MotSysNumaSetConfiguredCpus();
static void MotSysNumaSetTaskConstraints();

// we allow external linkage for this function for testing purposes
extern void MotSysNumaDumpMMapError(
    int errnum, void* address, size_t length, int prot, int flags, int fd, off_t offset);

// Static variables
static const char* const MASK_SIZE_FILE = "/proc/self/status";
static const char* const NODE_MASK_PREFIX = "Mems_allowed:\t";
static const int NODE_MASK_PREFIX_SIZE = strlen(NODE_MASK_PREFIX);
static const char* const NODE_CPU_MAP_FILE = "/sys/devices/system/node/node%d/cpumap";
#define MOTBindPolicy t_thrd.mot_cxt.bindPolicy
#define MOTMBindFlags t_thrd.mot_cxt.mbindFlags
static unsigned int g_nodeMaskSize = 0;
static int g_cpuMaskSize = 0;
static int g_maxConfNode = -1;
static int g_maxConfCpu = -1;
static int g_numProcNode = -1;
static int g_numProcCpu = -1;
static BitMaskSt* g_allNodesBm = nullptr;
static BitMaskSt* g_allCpusBm = nullptr;
static BitMaskSt* g_nodesBm = nullptr;
static BitMaskSt* g_memNodeBm = nullptr;
static BitMaskSt** g_nodeCpuBm = nullptr;

void MotSysNumaInit()
{
    MotSysNumaSetNodemaskSize();
    MotSysNumaSetConfiguredNodes();
    MotSysNumaSetNumaMaxCpu();
    MotSysNumaSetConfiguredCpus();
    MotSysNumaSetTaskConstraints();
    MotSysNumaNodeCpuMaskInit();
}

void MotSysNumaDestroy()
{
    BITMASK_FREE(g_allCpusBm);
    BITMASK_FREE(g_allNodesBm);
    BITMASK_FREE(g_memNodeBm);
    BITMASK_FREE(g_nodesBm);
    MotSysNumaNodeCpuMaskCleanup();
}

static void* MOTSysNumaMmap(void* address, size_t length, int prot, int flags, int fd, off_t offset)
{
    void* mem = mmap(address, length, prot, flags, fd, offset);
    if (mem == MAP_FAILED) {
        int errorCode = errno;
        MotSysNumaDumpMMapError(errorCode, address, length, prot, flags, fd, offset);
    }
    return mem;
}

void* MotSysNumaAllocInterleaved(size_t size)
{
    if (size == 0) {
        return nullptr;
    }

    void* mem = MOTSysNumaMmap(0, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, 0, 0);
    if (mem == MAP_FAILED) {
        mem = nullptr;
    } else {
        if (syscall(__NR_mbind,
                (intptr_t)mem,
                size,
                MPOL_INTERLEAVE,
                (intptr_t)g_allNodesBm->m_maskp,
                g_allNodesBm->m_size + 1,
                MOTMBindFlags) != 0) {
            MotSysNumaReportError("mbind");
            if (munmap(mem, size) != 0) {
                MotSysNumaReportError("munmap");
            }
            mem = nullptr;
        }
    }
    return mem;
}

void* MotSysNumaAllocOnNode(size_t size, int node)
{
    if (size == 0) {
        return nullptr;
    }

    void* mem = MOTSysNumaMmap(0, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, 0, 0);
    if (mem == MAP_FAILED) {
        mem = nullptr;
    } else {
        BITMASK_ONSTACK(bmp, g_nodeMaskSize);
        BITMASK_SETBIT(bmp, node);
        if (syscall(__NR_mbind,
                (intptr_t)mem,
                size,
                MOTBindPolicy,
                (intptr_t)bmp->m_maskp,
                bmp->m_size + 1,
                MOTMBindFlags) != 0) {
            MotSysNumaReportError("mbind");
            if (munmap(mem, size) != 0) {
                MotSysNumaReportError("munmap");
            }
            mem = nullptr;
        }
    }
    return mem;
}

void* MotSysNumaAllocLocal(size_t size)
{
    if (size == 0) {
        return nullptr;
    }

    void* mem = MOTSysNumaMmap(0, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, 0, 0);
    if (mem == MAP_FAILED) {
        mem = nullptr;
    } else {
        if (syscall(__NR_mbind, (intptr_t)mem, size, MPOL_PREFERRED, nullptr, 0, MOTMBindFlags) != 0) {
            MotSysNumaReportError("mbind");
            if (munmap(mem, size) != 0) {
                MotSysNumaReportError("munmap");
            }
            mem = nullptr;
        }
    }
    return mem;
}

static void* MotSysNumaMapAligned(size_t size, size_t align)
{
    if (size == 0 || align == 0) {
        return MAP_FAILED;
    }

    // try fast allocation first
    void* mem = MOTSysNumaMmap(0, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, 0, 0);
    if (mem == MAP_FAILED) {
        return mem;
    }

    if (((uint64_t)mem) % align != 0) {
        // take the slow route
        (void)munmap(mem, size);
        mem = MOTSysNumaMmap(0, size + align, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, 0, 0);
        if (mem == MAP_FAILED) {
            return mem;
        } else {
            uint64_t offset = ((uint64_t)mem) % align;
            if (offset == 0) {  // aligned this time so we only need to unmap suffix of align bytes
                (void)munmap(((char*)mem) + size, align);
            } else {
                size_t leadingUseless = align - offset;  // guaranteed to be non-negative
                (void)munmap(mem, leadingUseless);
                void* alignedMem = ((char*)mem) + leadingUseless;
                (void)munmap(((char*)alignedMem) + size, offset);
                mem = alignedMem;
            }
        }
    }

    return mem;
}

/* Alloc memory page interleaved on all nodes. */
void* MotSysNumaAllocAlignedInterleaved(size_t size, size_t align)
{
    if (size == 0) {
        return nullptr;
    }

    void* mem = MotSysNumaMapAligned(size, align);
    if (mem == MAP_FAILED) {
        mem = nullptr;
    } else {
        if (syscall(__NR_mbind,
                (intptr_t)mem,
                size,
                MPOL_INTERLEAVE,
                (intptr_t)g_allNodesBm->m_maskp,
                g_allNodesBm->m_size + 1,
                MOTMBindFlags) != 0) {
            MotSysNumaReportError("mbind");
            if (munmap(mem, size) != 0) {
                MotSysNumaReportError("munmap");
            }
            mem = nullptr;
        }
    }
    return mem;
}

/* Alloc memory located on node */
void* MotSysNumaAllocAlignedOnNode(size_t size, size_t align, int node)
{
    if (size == 0) {
        return nullptr;
    }

    void* mem = MotSysNumaMapAligned(size, align);
    if (mem == MAP_FAILED) {
        mem = nullptr;
    } else {
        BITMASK_ONSTACK(bmp, g_nodeMaskSize);
        BITMASK_SETBIT(bmp, node);
        if (syscall(__NR_mbind,
                (intptr_t)mem,
                size,
                MOTBindPolicy,
                (intptr_t)bmp->m_maskp,
                bmp->m_size + 1,
                MOTMBindFlags) != 0) {
            MotSysNumaReportError("mbind");
            if (munmap(mem, size) != 0) {
                MotSysNumaReportError("munmap");
            }
            mem = nullptr;
        }
    }
    return mem;
}

/* Alloc memory on local node */
void* MotSysNumaAllocAlignedLocal(size_t size, size_t align)
{
    if (size == 0) {
        return nullptr;
    }

    void* mem = MotSysNumaMapAligned(size, align);
    if (mem == MAP_FAILED) {
        mem = nullptr;
    } else {
        if (syscall(__NR_mbind, (intptr_t)mem, size, MPOL_PREFERRED, nullptr, 0, MOTMBindFlags) != 0) {
            MotSysNumaReportError("mbind");
            if (munmap(mem, size) != 0) {
                MotSysNumaReportError("munmap");
            }
            mem = nullptr;
        }
    }
    return mem;
}

void MotSysNumaFree(void* mem, size_t size)
{
    if (munmap(mem, size) != 0) {
        MotSysNumaReportError("munmap");
    }
}

int MotSysNumaAvailable()
{
    if (syscall(__NR_get_mempolicy, nullptr, nullptr, 0, 0, 0) < 0 && errno == ENOSYS)
        return -1;
    return 0;
}

int MotSysNumaConfiguredNodes()
{
    int memnodecount = 0;

    for (int i = 0; i <= g_maxConfNode; i++) {
        if (BITMASK_GETBIT(g_memNodeBm, i)) {
            memnodecount++;
        } else {
            MotSysNumaReportWarn("NUMA node %d has no memory", i);
            return 0;
        }
    }

    return memnodecount;
}

int MotSysNumaGetNode(int cpu)
{
    if (cpu > g_cpuMaskSize) {
        errno = EINVAL;
        return -1;
    }

    if (g_nodeCpuBm == nullptr) {
        errno = EINVAL;
        return -1;
    }

    for (int node = 0; node <= g_maxConfNode; node++) {
        if (g_nodeCpuBm[node] != nullptr && BITMASK_GETBIT(g_nodeCpuBm[node], cpu)) {
            return node;
        }
    }
    errno = EINVAL;
    return -1;
}

void MotSysNumaSetBindPolicy(int strict)
{
    MOTBindPolicy = (strict ? MPOL_BIND : MPOL_PREFERRED);
}

void MotSysNumaSetStrict(int flag)
{
    if (flag) {
        MOTMBindFlags |= MPOL_MF_STRICT;
    } else {
        MOTMBindFlags &= ~MPOL_MF_STRICT;
    }
}

void MotSysNumaSetPreferred(int node)
{
    BITMASK_ONSTACK(bmp, g_nodeMaskSize);
    if (node >= 0) {
        BITMASK_SETBIT(bmp, node);
        if (syscall(__NR_set_mempolicy, MPOL_PREFERRED, bmp->m_maskp, bmp->m_size + 1) < 0) {
            MotSysNumaReportError("set_mempolicy MPOL_PREFERRED");
        }
    } else {
        if (syscall(__NR_set_mempolicy, MPOL_DEFAULT, bmp->m_maskp, bmp->m_size + 1) < 0) {
            MotSysNumaReportError("set_mempolicy MPOL_DEFAULT");
        }
    }
}

bool MotSysNumaCpuAllowed(int cpu)
{
    if (cpu > g_cpuMaskSize) {
        errno = EINVAL;
        return false;
    }

    if (g_allCpusBm == nullptr) {
        errno = EINVAL;
        return false;
    }

    if (BITMASK_GETBIT(g_allCpusBm, cpu) == 1) {
        return true;
    }

    return false;
}

static int MotSysNumaParseBitmap(char* line, BitMaskSt* mask)
{
    int i, ncpus;
    char* p = strchr(line, '\n');
    if (p == nullptr) {
        return -1;
    }
    ncpus = mask->m_size;

    for (i = 0; p > line; i++) {
        char* oldp = p;
        char* endp = nullptr;
        if (*p == ',') {
            --p;
        }

        while (p > line && *p != ',') {
            --p;
        }

        if (p > line && sizeof(unsigned long) == 8) {
            oldp--;
            errno_t erc = memmove_s(p, (oldp - p) + 1, p + 1, (oldp - p) + 1);
            securec_check(erc, "\0", "\0");
            while (p > line && *p != ',') {
                --p;
            }
        }
        if (*p == ',') {
            p++;
        }

        if (i >= (int)CPU_LONGS(ncpus)) {
            return -1;
        }

        mask->m_maskp[i] = strtoul(p, &endp, 16);
        if (endp != oldp) {
            return -1;
        }

        p--;
    }
    return 0;
}

static void MotSysNumaSetConfiguredNodes()
{
    DIR* d = nullptr;
    struct dirent* de = nullptr;

    g_memNodeBm = MotSysNumaBitmaskAlloc(g_nodeMaskSize);
    g_nodesBm = MotSysNumaBitmaskAlloc(g_nodeMaskSize);

    d = opendir("/sys/devices/system/node");
    if (d == nullptr) {
        g_maxConfNode = 0;
    } else {
        while ((de = readdir(d)) != nullptr) {
            int node;
            if (strncmp(de->d_name, "node", 4)) {
                continue;
            }
            node = strtoul(de->d_name + 4, nullptr, 0);
            BITMASK_SETBIT(g_nodesBm, node);
            if (MotSysNumaNodeSize(node) > 0) {
                BITMASK_SETBIT(g_memNodeBm, node);
            }
            if (g_maxConfNode < node) {
                g_maxConfNode = node;
            }
        }
        (void)closedir(d);
    }
}

static void MotSysNumaSetTaskConstraints()
{
    int i;
    char* buffer = nullptr;
    size_t buflen = 0;
    FILE* f;

    g_allCpusBm = MotSysNumaBitmaskAlloc(g_cpuMaskSize);
    g_allNodesBm = MotSysNumaBitmaskAlloc(g_nodeMaskSize);

    if ((f = fopen(MASK_SIZE_FILE, "r")) == nullptr) {
        return;
    }

    while (getline(&buffer, &buflen, f) > 0) {
        MOT_ASSERT(buffer != nullptr);
        char* mask = strrchr(buffer, '\t') + 1;

        if (strncmp(buffer, "Cpus_allowed:", 13) == 0) {
            g_numProcCpu = MotSysNumaReadBitmask(mask, g_allCpusBm);
        }

        if (strncmp(buffer, "Mems_allowed:", 13) == 0) {
            g_numProcNode = MotSysNumaReadBitmask(mask, g_allNodesBm);
        }
    }
    (void)fclose(f);
    if (buffer != nullptr) {
        free(buffer);
    }

    if (g_numProcCpu <= 0) {
        for (i = 0; i <= g_maxConfCpu; i++) {
            BITMASK_SETBIT(g_allCpusBm, i);
        }
        g_numProcCpu = g_maxConfCpu + 1;
    }

    if (g_numProcCpu > g_maxConfCpu + 1) {
        g_numProcCpu = g_maxConfCpu + 1;
        for (i = g_maxConfCpu + 1; i < (int)g_allCpusBm->m_size; i++) {
            BITMASK_CLEARBIT(g_allCpusBm, i);
        }
    }

    if (g_numProcNode <= 0) {
        for (i = 0; i <= g_maxConfNode; i++) {
            BITMASK_SETBIT(g_allNodesBm, i);
        }
        g_numProcNode = g_maxConfNode + 1;
    }

    return;
}

static void MotSysNumaSetNumaMaxCpu()
{
    int len = 4096;
    int n;
    int olde = errno;

    do {
        BITMASK_ONSTACK(buffer, len);
        n = syscall(__NR_sched_getaffinity, 0, BITMASK_NBYTES(buffer), buffer->m_maskp);
        if (n < 0) {
            if (errno == EINVAL) {
                if (len >= 1024 * 1024) {
                    break;
                }
                len *= 2;
                continue;
            } else {
                MotSysNumaReportWarn(
                    "Unable to determine max cpu (sched_getaffinity: %s); guessing...", gs_strerror(errno));
                n = sizeof(cpu_set_t);
                break;
            }
        }
    } while (n < 0);
    errno = olde;
    g_cpuMaskSize = n * 8;
}

static void MotSysNumaSetConfiguredCpus()
{
    g_maxConfCpu = sysconf(_SC_NPROCESSORS_CONF) - 1;
    if (g_maxConfCpu == -1) {
        MotSysNumaReportError("sysconf(NPROCESSORS_CONF) failed");
    }
}

static BitMaskSt* MotSysNumaBitmaskAlloc(unsigned int n)
{
    if (n < 1) {
        errno = EINVAL;
        MotSysNumaReportError("request to allocate mask for invalid number");
        exit(1);
    }

    int size = sizeof(BitMaskSt) + (LONGS_PER_BITS(n) * sizeof(unsigned long));
    BitMaskSt* bmp = (BitMaskSt*)calloc(1, size);
    if (bmp == nullptr) {
        MotSysNumaReportError("Out of memory allocating bitmask");
        exit(1);
    }
    bmp->m_size = n;
    return bmp;
}

static unsigned int MotSysNumaBitmaskWeight(const BitMaskSt* bmp)
{
    unsigned int w = 0;
    for (unsigned int i = 0; i < bmp->m_size; i++) {
        if (BITMASK_GETBIT(bmp, i)) {
            w++;
        }
    }
    return w;
}

static int MotSysNumaReadBitmask(char* s, BitMaskSt* bmp)
{
    char* end = s;
    unsigned tmplen = (bmp->m_size + BITS_PER_UINT - 1) / BITS_PER_UINT;
    unsigned int tmp[tmplen];
    unsigned int* start = tmp;
    unsigned int n = 0;
    unsigned int m = 0;

    if (s == nullptr) {
        return 0;
    }

    unsigned int i = strtoul(s, &end, 16);

    while (!i && *end++ == ',') {
        i = strtoul(end, &end, 16);
    }

    if (!i) {
        return -1;
    }

    start[n++] = i;
    while (*end++ == ',') {
        i = strtoul(end, &end, 16);
        start[n++] = i;

        if (n > tmplen) {
            return -1;
        }
    }

    while (n) {
        unsigned int w;
        unsigned long x = 0;
        for (w = 0; n && w < BITS_PER_ULONG; w += BITS_PER_UINT) {
            x |= ((unsigned long)start[n-- - 1] << w);
        }

        bmp->m_maskp[m++] = x;
    }
    return MotSysNumaBitmaskWeight(bmp);
}

static void MotSysNumaNodeCpuMaskCleanup(void)
{
    if (g_nodeCpuBm != nullptr) {
        for (unsigned int i = 0; i < g_nodeMaskSize; i++) {
            BITMASK_FREE(g_nodeCpuBm[i]);
        }
        free(g_nodeCpuBm);
        g_nodeCpuBm = nullptr;
    }
}

static void MotSysNumaNodeCpuMaskInit(void)
{
    errno_t erc;
    char filename[NUMA_FILE_PATH_LEN] = {0};
    size_t len;

    if (g_nodeCpuBm != nullptr) {
        return;
    }

    g_nodeCpuBm = (BitMaskSt**)calloc(g_nodeMaskSize, sizeof(BitMaskSt*));

    for (unsigned int i = 0; i < g_nodeMaskSize; i++) {
        len = 0;
        FILE* f = nullptr;
        char* line = nullptr;

        erc = snprintf_s(filename, NUMA_FILE_PATH_LEN, NUMA_FILE_PATH_LEN - 1, NODE_CPU_MAP_FILE, i);
        securec_check_ss(erc, "\0", "\0");
        f = fopen(filename, "r");
        if (f == nullptr) {
            if (BITMASK_GETBIT(g_nodesBm, i)) {
                MotSysNumaReportWarn("Can't open file %s, error %s)", filename, gs_strerror(errno));
            }
            continue;
        }

        if (getdelim(&line, &len, '\n', f) < 1) {
            if (BITMASK_GETBIT(g_nodesBm, i)) {
                MotSysNumaReportWarn("Can't parse file %s, error %s)", filename, gs_strerror(errno));
            }
        }

        if (line != nullptr) {
            BitMaskSt* mask = MotSysNumaBitmaskAlloc(g_cpuMaskSize);
            if (MotSysNumaParseBitmap(line, mask) < 0) {
                MotSysNumaReportWarn("Cannot parse CPU map.");
                BITMASK_FREE(mask);
            } else {
                g_nodeCpuBm[i] = mask;
            }

            free(line);
        }

        if (f != nullptr) {
            (void)fclose(f);
        }
    }
}

static void MotSysNumaSetNodemaskSize()
{
    FILE* fp;
    char* line = nullptr;
    size_t sz = 0;

    if ((fp = fopen(MASK_SIZE_FILE, "r")) != nullptr) {
        while (getline(&line, &sz, fp) > 0) {
            if (STR_PREFIX(line, NODE_MASK_PREFIX)) {
                g_nodeMaskSize = STR_TO_NBITS(line + NODE_MASK_PREFIX_SIZE);
                break;
            }
        }
        if (line != nullptr)
            free(line);
        (void)fclose(fp);
    }

    if (g_nodeMaskSize == 0) {
        int memPolicy = 0;
        unsigned long tmp[4096];
        g_nodeMaskSize = 32;
        while ((syscall(__NR_get_mempolicy, &memPolicy, tmp, g_nodeMaskSize + 1, 0, 0) < 0) && (errno == EINVAL) &&
               (g_nodeMaskSize <= 4096 * 8)) {
            g_nodeMaskSize <<= 1;
        }
    }
}

static long long MotSysNumaNodeSize(int node)
{
    const int cmdSize = 256;
    char cmd[cmdSize];

    errno_t erc =
        snprintf_s(cmd, cmdSize, cmdSize - 1, "cat /sys/devices/system/node/node%d/meminfo | grep MemTotal:", node);
    securec_check_ss(erc, "\0", "\0");

    std::string res = ExecOsCommand(cmd);
    std::string::size_type pos = res.find("MemTotal:");
    if (pos == std::string::npos) {
        return -1;
    }

    char* end = nullptr;
    char* line = (char*)res.c_str() + pos + strlen("MemTotal:");
    long long size = (long long)strtoull(line, &end, 0);
    if (end == line) {
        return -1;
    }

    return size;
}

static void MotSysNumaReportError(const char* msg)
{
    const unsigned int bufSize = 128;
    char buffer[bufSize] = {0};
    int olde = errno;
    LogLevel logLevel = GetGlobalConfiguration().m_numaErrorsLogLevel;
    if (MOT_CHECK_LOG_LEVEL(logLevel)) {
        char* errorString = strerror_r(olde, buffer, bufSize);
        MOT_LOG(logLevel, "%s: %s", msg, errorString);
    }
    errno = olde;
}

static void MotSysNumaReportWarn(const char* msg, ...)
{
    LogLevel logLevel = GetGlobalConfiguration().m_numaWarningsLogLevel;
    if (MOT_CHECK_LOG_LEVEL(logLevel)) {
        va_list ap;
        va_start(ap, msg);
        MOT_LOG(logLevel, msg, ap);
        va_end(ap);
    }
}

static bool HasAvailableMemory()
{
    struct rlimit rlim;
    struct rusage ru;
    if (getrlimit(RLIMIT_AS, &rlim) < 0) {
        return true;  // even though this is an error, there is nothing much we can do
    }
    if (rlim.rlim_cur == RLIM_INFINITY) {
        return true;  // no limit on virtual memory
    }

    if (getrusage(RUSAGE_SELF, &ru) < 0) {
        return true;  // even though this is an error, there is nothing much we can do
    }

    if (ru.ru_maxrss < (long)rlim.rlim_cur) {
        return true;
    }

    return false;
}

static void EAccessToString(StringBuffer* stringBuffer, int prot, int flags, int fd)
{
    struct stat st;
    int openMode = -1;

    // Check if a file descriptor refers to a non-regular file
    // Or MAP_PRIVATE was requested, but fd is not open for reading
    // Or MAP_SHARED was requested and PROT_WRITE is set, but fd is not open in read/write (O_RDWR) mode.
    // Or PROT_WRITE is set, but the file is append-only.
    if (fstat(fd, &st) == 0 && !S_ISREG(st.st_mode)) {
        StringBufferAppend(stringBuffer, "File descriptor %d refers to non-regular file", fd);
        return;
    }

    // check if MAP_PRIVATE was requested, but fd is not open for reading.
#ifdef MAP_PRIVATE
    if (flags & MAP_PRIVATE) {
        openMode = fcntl(fd, F_GETFD, 0);
        if (openMode >= 0 && (openMode & O_ACCMODE) == O_WRONLY) {
            StringBufferAppend(
                stringBuffer, "MAP_PRIVATE was requested, but the file descriptor is not open for reading");
            return;
        }
    }
#endif

    // Check if MAP_SHARED was requested and PROT_WRITE is set, but fd is not open in read/write (O_RDWR) mode.
    // Or just PROT_WRITE is set, but the file is append-only.
#ifdef PROT_WRITE
    if (prot & PROT_WRITE) {
        openMode = fcntl(fd, F_GETFD, 0);
        if (openMode >= 0) {
#ifdef MAP_SHARED
            if (flags & MAP_SHARED) {
                if ((openMode & O_ACCMODE) != O_RDWR) {
                    StringBufferAppend(stringBuffer, "The file descriptor is not open for both reading and writing");
                    return;
                }
            }
#endif

            // check if PROT_WRITE is set, but the file is append-only
            if (openMode & O_APPEND) {
                StringBufferAppend(stringBuffer, "The file descriptor is open for append");
                return;
            }
        }
    }
#endif
    StringBufferAppend(stringBuffer, "Unknown access error");
}

static void EInvalToString(StringBuffer* stringBuffer, void* address, size_t length, int prot, int flags, off_t offset)
{
    if (length == 0) {
        StringBufferAppend(stringBuffer, "The mapped length is zero");
        return;
    }
    if ((flags & MAP_PRIVATE) && (flags & MAP_SHARED)) {
        StringBufferAppend(stringBuffer, "You must specify exactly one of MAP_PRIVATE or MAP_SHARED");
        return;
    }
    if (!(flags & MAP_PRIVATE) && !(flags & MAP_SHARED)) {
        StringBufferAppend(stringBuffer, "You must specify exactly one of MAP_PRIVATE or MAP_SHARED");
        return;
    }

    int pageSize = sysconf(_SC_PAGESIZE);
    if (pageSize > 0) {
        unsigned mask = (pageSize - 1);
        if ((uintptr_t)address & mask) {
            StringBufferAppend(stringBuffer, "Address %p is not aligned to page size %d", address, pageSize);
            return;
        }
        if ((unsigned long)length & mask) {
            StringBufferAppend(stringBuffer, "Mapped length %zu is not aligned to page size %d", length, pageSize);
            return;
        }
        if ((unsigned long)offset & mask) {
            StringBufferAppend(stringBuffer, "Offset %zu is not aligned to page size %d", offset, pageSize);
            return;
        }
    }
    StringBufferAppend(stringBuffer, "Unknown invalid value error");
}

static void ETxtBsyToString(StringBuffer* stringBuffer, int flags, int fd)
{
#ifdef MAP_DENYWRITE
    if (flags & MAP_DENYWRITE) {
        int openMode = fcntl(fd, F_GETFD, 0);
        if (openMode >= 0) {
            if (((openMode & O_ACCMODE) == O_WRONLY) || ((openMode & O_ACCMODE) == O_RDWR)) {
                StringBufferAppend(stringBuffer,
                    "The mapping flag MAP_DENYWRITE is incompatible with the open mode of the file descriptor");
                return;
            }
        }
    }
#endif
    StringBufferAppend(stringBuffer, "Unknown text file busy error");
}

static void MotSysNumaMMapErrorToString(
    StringBuffer* stringBuffer, int errorCode, void* address, size_t length, int prot, int flags, int fd, off_t offset)
{
    switch (errorCode) {
        case EACCES:
            EAccessToString(stringBuffer, prot, flags, fd);
            break;

        case EAGAIN:
#if defined(EWOULDBLOCK) && EAGAIN != EWOULDBLOCK
        case EWOULDBLOCK:
#endif
            StringBufferAppend(stringBuffer, "The file has been locked, or too much memory has been locked");
            break;

        case EBADF:
            StringBufferAppend(stringBuffer, "The file descriptor is invalid and MAP_ANONYMOUS was not set");
            break;

        case EINVAL:
            EInvalToString(stringBuffer, address, length, prot, flags, offset);
            break;

        case ENFILE:
            StringBufferAppend(stringBuffer, "The system limit on the total number of open files has been reached");
            break;

#ifdef ENOTSUP
        case ENOTSUP:
#endif
#ifdef EOPNOTSUP
        case EOPNOTSUP:
#endif
        case ENODEV:
            StringBufferAppend(
                stringBuffer, "The underlying file system of the specified file does not support memory mapping");
            break;

        case ENOMEM:
            // No memory is available, or the process's maximum number of mappings would have been exceeded.
            if (!HasAvailableMemory()) {
                StringBufferAppend(stringBuffer, "No memory is available");
            } else {
                StringBufferAppend(
                    stringBuffer, "The maximum number of mappings for the process would have been exceeded");
            }
            break;

        case EPERM:
#ifdef PROT_EXEC
            if (prot & PROT_EXEC) {
                StringBufferAppend(stringBuffer, "The underlying file system does not permit execution");
            }
#endif
            StringBufferAppend(stringBuffer, "Unknown permission error");
            break;

        case ETXTBSY:
            ETxtBsyToString(stringBuffer, flags, fd);
            break;

        default:
            StringBufferAppend(stringBuffer, "Unknown error code %d", errorCode);
            break;
    }
}

void MotSysNumaDumpMMapError(int errorCode, void* address, size_t length, int prot, int flags, int fd, off_t offset)
{
    // use on-stack buffer and avoid any memory allocations at this point
    const int bufSize = 1024;
    char errorBuffer[bufSize];
    StringBufferApplyFixed(
        [errorCode, address, length, prot, flags, fd, offset](StringBuffer* stringBuffer) {
            MotSysNumaMMapErrorToString(stringBuffer, errorCode, address, length, prot, flags, fd, offset);
            MOT_LOG_ERROR("mmap() system call error details: %s", stringBuffer->m_buffer);
            MOT_LOG_SYSTEM_ERROR_CODE(errorCode, mmap, "Failed to map memory segment of size %zu bytes", length);
        },
        errorBuffer,
        bufSize);
}
}  // namespace MOT
