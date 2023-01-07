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
 * bbox_elf_dump.h
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/bbox/bbox_elf_dump.h
 *
 * -------------------------------------------------------------------------
 */
#include <elf.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/poll.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#if (defined WITH_OPENEULER_OS) || (defined OPENEULER_MAJOR)
#include <linux/sysctl.h>
#else
#include <sys/sysctl.h>
#endif
#include <sys/ptrace.h>
#include <asm/ptrace.h>
#include <sys/time.h>
#include <sys/uio.h>
#include <sys/wait.h>
#include <stdarg.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stddef.h>

#include "bbox_elf_dump_base.h"
#include "bbox_syscall_support.h"
#include "bbox_print.h"
#include "bbox_threads.h"
#include "bbox_lib.h"

#ifndef __BBOX_ELF_DUMP_H__
#define __BBOX_ELF_DUMP_H__

#ifdef __cplusplus
#if __cplusplus
extern "C" {
#endif
#endif /* __cplusplus */

#if __WORDSIZE == 64
#define BBOX_ELF_CLASS ELFCLASS64
#define BBOX_EHDR Elf64_Ehdr
#define BBOX_PHDR Elf64_Phdr
#define BBOX_SHDR Elf64_Shdr
#define BBOX_NHDR Elf64_Nhdr
#define BBOX_AUXV_T Elf64_auxv_t
#else
#define BBOX_ELF_CLASS ELFCLASS32
#define BBOX_EHDR Elf32_Ehdr
#define BBOX_PHDR Elf32_Phdr
#define BBOX_SHDR Elf32_Shdr
#define BBOX_NHDR Elf32_Nhdr
#define BBOX_AUXV_T Elf32_auxv_t
#endif

#define RET_BBOX_VDSO_INVALID (1)

#define COMPRESSION_CMD "lz4 -%u > \"%s\""
#define COMPRESSION_CMD_WITH_FILENAME "lz4 -%u > core.%d.lz4"

#define BBOX_FALSE (0)
#define BBOX_TRUE (1)

/* write "core\0\0\0\0" into core file. */
#define BBOX_CORE_STRING "CORE\0\0\0\0"
#define BBOX_CORE_NAME_LENGTH (5)
#define BBOX_LINUX_STRING "LINUX\000\000"
#define BBOX_LINUX_NAME_LENGTH (8)
#define BBOX_CORE_STRING_LENGTH (8)

#define THREAD_SELF_MAPS_FILE "/proc/self/maps"
#define THREAD_SELF_EXE_FILE "/proc/self/exe"
#define THREAD_SELF_AUXV_FILE "/proc/self/auxv"
#define THREAD_SELF_STAT_FILE "/proc/self/stat"
#define THREAD_SELF_COMMAND_LINE_FILE "/proc/self/cmdline"

#define VDSO_NAME_STRING "[vdso]"
#define VVAR_NAME_STRING "[vvar]"
#define DEVICE_ZERO_NAME_STRING "/dev/zero"
#define DEVICE_PREFIX_LEN 5
#define DEVICE_AND_NODE_FIELD_NUM 2

#define BBOX_BUFF_SIZE (4096)
#define BBOX_CMD_LEN (256)
#define BBOX_STAT_ITEM_NUM (64)

#define PF_MASK 0x00000007
#define PF_ANONYMOUS 0x80000000
#define PF_DEVICE 0x40000000
#define PF_VDSO 0x20000000
#define PF_MAPPEDFILE 0x10000000
#define PF_VVAR 0x08000000

#define BBOX_SECTION_NUM 3
#define BBOX_SHSTR_INDEX 2
#define BBOX_ADDON_INFO "BBOX_ADDON_INFO"
#define BBOX_LOG "BBOX_LOG"
#define BBOX_STR_TAB ".shstrtab"
#define BBOX_ADDON_INFO_SIZE (128 * 1024)
#define BBOX_SH_STR_TAB_SIZE (1024)

#define BBOX_NO_INTR(fn) \
    do {                 \
    } while ((fn) < 0 && errno == EINTR)

#define BBOX_WRITE(fd, pData, size)                                             \
    do {                                                                        \
        if (BBOX_DoWrite(fd, pData, size) != size) {                            \
            bbox_print(PRINT_ERR, "BBOX_DoWrite failed, errno = %d.\n", errno); \
            return RET_ERR;                                                     \
        }                                                                       \
    } while (0)

#if defined(__x86_64__)
#define ELF_ARCH EM_X86_64
#elif defined(__i386__)
#define ELF_ARCH EM_386
#elif defined(__ARM_ARCH_5TE__) || (defined(__ARM_ARCH_7A__))
#define ELF_ARCH EM_ARM
#elif defined(__aarch64__)
#define ELF_ARCH EM_AARCH64
#endif

#ifndef AT_SYSINFO_EHDR
#define AT_SYSINFO_EHDR 33
#endif

#if (defined(__x86_64__))
/* member variable are named by register. */
struct amd64_regs {
#define BP rbp
#define SP rsp
#define IP rip
    uint64_t r15, r14, r13, r12;
    uint64_t rbp, rbx, r11, r10;
    uint64_t r9, r8, rax, rcx, rdx, rsi, rdi, orig_rax;
    uint64_t rip, cs;
    uint64_t eflags;
    uint64_t rsp, ss;
    uint64_t fs_base, gs_base;
    uint64_t ds, es, fs, gs;
};

/* 64 CPU */
#define CPURegs amd64_regs

typedef struct Frame {
    struct CPURegs uregs;
    int errno_;
    pid_t tid;
} Frame;

/* save call frame */
#define FRAME(fm)                                   \
    Frame fm;                                       \
    do {                                            \
        fm.errno_ = errno;                          \
        fm.tid = sys_gettid();                      \
        __asm__ volatile("push %%rbp\n"             \
                         "push %%rbx\n"             \
                         "mov  %%r15,0(%%rax)\n"    \
                         "mov  %%r14,8(%%rax)\n"    \
                         "mov  %%r12,24(%%rax)\n"   \
                         "mov  %%r13,16(%%rax)\n"   \
                         "mov  %%rbp,32(%%rax)\n"   \
                         "mov  %%rbx,40(%%rax)\n"   \
                         "mov  %%r11,48(%%rax)\n"   \
                         "mov  %%r9,64(%%rax)\n"    \
                         "mov  %%r10,56(%%rax)\n"   \
                         "mov  %%r8,72(%%rax)\n"    \
                         "mov  %%rax,80(%%rax)\n"   \
                         "mov  %%rdx,96(%%rax)\n"   \
                         "mov  %%rcx,88(%%rax)\n"   \
                         "mov  %%rsi,104(%%rax)\n"  \
                         "mov  %%rdi,112(%%rax)\n"  \
                         "mov  %%ds,%%rbx\n"        \
                         "mov  %%rbx,192(%%rax)\n"  \
                         "mov  %%fs,%%rbx\n"        \
                         "mov  %%rbx,184(%%rax)\n"  \
                         "mov  %%es,%%rbx\n"        \
                         "mov  %%rbx,200(%%rax)\n"  \
                         "mov  %%gs,%%rbx\n"        \
                         "mov  %%rbx,208(%%rax)\n"  \
                         "call 0f\n"                \
                         "0:pop %%rbx\n"            \
                         "add  $1f-0b,%%rbx\n"      \
                         "mov  %%rbx,0x80(%%rax)\n" \
                         "mov  %%cs,%%rbx\n"        \
                         "mov  %%rbx,136(%%rax)\n"  \
                         "pushf\n"                  \
                         "pop  %%rbx\n"             \
                         "mov  %%rbx,0x90(%%rax)\n" \
                         "mov  %%rsp,%%rbx\n"       \
                         "add  $16,%%ebx\n"         \
                         "mov  %%rbx,0x98(%%rax)\n" \
                         "mov  %%ss,%%rbx\n"        \
                         "mov  %%rbx,0xa0(%%rax)\n" \
                         "pop  %%rbx\n"             \
                         "pop  %%rbp\n"             \
                         "1:"                       \
                         :                          \
                         : "a"(&fm)                 \
                         : "memory");               \
    } while (0)

#define SET_FRAME(f, r)                  \
    do {                                 \
        errno = (f).errno_;              \
        (f).uregs.fs_base = (r).fs_base; \
        (f).uregs.gs_base = (r).gs_base; \
        (r) = (f).uregs;                 \
    } while (0)

#elif (defined(__i386__))
/* member variable are named by register. */
struct i386_regs {
#define BP ebp
#define SP esp
#define IP eip
    uint32_t ebx, ecx, edx, esi, edi, ebp, eax;
    uint16_t ds, __ds;
    uint16_t es, __es;
    uint16_t fs, __fs;
    uint16_t gs, __gs;
    uint32_t orig_eax, eip;
    uint16_t cs, __cs;
    uint32_t eflags;
    uint32_t esp;
    uint16_t ss, __ss;
};

#define CPURegs i386_regs

typedef struct Frame {
    struct CPURegs uregs;
    int errno_;
    pid_t tid;
} Frame;

/* save call frame */
#define FRAME(fm)                                   \
    Frame fm;                                       \
    do {                                            \
        fm.errno_ = errno;                          \
        fm.tid = sys_gettid();                      \
        __asm__ volatile("push %%ebp\n"             \
                         "push %%ebx\n"             \
                         "mov  %%ebx,0(%%eax)\n"    \
                         "mov  %%ecx,4(%%eax)\n"    \
                         "mov  %%edx,8(%%eax)\n"    \
                         "mov  %%edi,16(%%eax)\n"   \
                         "mov  %%esi,12(%%eax)\n"   \
                         "mov  %%eax,24(%%eax)\n"   \
                         "mov  %%ebp,20(%%eax)\n"   \
                         "mov  %%ds,%%ebx\n"        \
                         "mov  %%ebx,28(%%eax)\n"   \
                         "mov  %%es,%%ebx\n"        \
                         "mov  %%ebx,32(%%eax)\n"   \
                         "mov  %%gs,%%ebx\n"        \
                         "mov  %%ebx, 40(%%eax)\n"  \
                         "mov  %%fs,%%ebx\n"        \
                         "mov  %%ebx,36(%%eax)\n"   \
                         "call 0f\n"                \
                         "0:pop %%ebx\n"            \
                         "add  $1f-0b,%%ebx\n"      \
                         "mov  %%ebx,0x30(%%eax)\n" \
                         "mov  %%cs,%%ebx\n"        \
                         "mov  %%ebx,0x34(%%eax)\n" \
                         "pushf\n"                  \
                         "pop  %%ebx\n"             \
                         "mov  %%ebx,0x38(%%eax)\n" \
                         "mov  %%esp,%%ebx\n"       \
                         "add  $8,%%ebx\n"          \
                         "mov  %%ebx,0x3c(%%eax)\n" \
                         "mov  %%ss,%%ebx\n"        \
                         "mov  %%ebx,0x40(%%eax)\n" \
                         "pop  %%ebx\n"             \
                         "pop  %%ebp\n"             \
                         "1:"                       \
                         :                          \
                         : "a"(&fm)                 \
                         : "memory");               \
    } while (0)

#define SET_FRAME(f, r)     \
    do {                    \
        errno = (f).errno_; \
        (r) = (f).uregs;    \
    } while (0)

#elif defined(__ARM_ARCH_5TE__) || (defined(__ARM_ARCH_7A__))
struct arm_regs {
/* General purpose registers                 */
#define BP uregs[11] /* Frame pointer                             */
#define SP uregs[13] /* Stack pointer                             */
#define IP uregs[15] /* Program counter                           */
#define LR uregs[14] /* Link register                             */
    long uregs[18];
};

#define CPURegs arm_regs

typedef struct Frame {
    struct arm_regs arm;
    int errno_;
    pid_t tid;
} Frame;

#define FRAME(fm)                                                        \
    Frame fm;                                                            \
    do {                                                                 \
        long cpsr;                                                       \
        fm.errno_ = errno;                                               \
        fm.tid = sys_gettid();                                           \
        __asm__ volatile("stmia %0, {r0-r15}\n" /* All integer regs   */ \
                         :                                               \
                         : "r"(&fm.arm)                                  \
                         : "memory");                                    \
        fm.arm.uregs[16] = 0;                                            \
        __asm__ volatile("mrs %0, cpsr\n" /* Condition code reg */       \
                         : "=r"(cpsr));                                  \
        fm.arm.uregs[17] = cpsr;                                         \
    } while (0)

#define SET_FRAME(fm, reg)                               \
    do {                                                 \
        /* Don't override the FPU status register.    */ \
        /* Use the value obtained from ptrace(). This */ \
        /* works, because our code does not perform   */ \
        /* any FPU operations, itself.                */ \
        long fps = (fm).arm.uregs[16];                   \
        errno = (fm).errno_;                             \
        (reg) = (fm).arm;                                \
        (reg).uregs[16] = fps;                           \
    } while (0)
#elif defined(__aarch64__)

#define CPURegs user_pt_regs

typedef struct Frame {
    struct user_pt_regs arm;
    int errno_;
    pid_t tid;
} Frame;

#define FRAME(fm)                                             \
    Frame fm;                                                 \
    label_bbox:                                               \
    do {                                                      \
        __asm__ __volatile__("stp x0, x1, [%0, #16 * 0]\n"    \
                             "stp x2,x3, [%0, #16 * 1]\n"     \
                             "stp x4,x5, [%0, #16 * 2]\n"     \
                             "stp x6,x7, [%0, #16 * 3]\n"     \
                             "stp x8,x9, [%0, #16 * 4]\n"     \
                             "stp x10,x11, [%0, #16 * 5]\n"   \
                             "stp x12,x13, [%0, #16 * 6]\n"   \
                             "stp x14,x15, [%0, #16 * 7]\n"   \
                             "stp x16,x17, [%0, #16 * 8]\n"   \
                             "stp x18,x19, [%0, #16 * 9]\n"   \
                             "stp x20,x21, [%0, #16 * 10]\n"  \
                             "stp x22,x23, [%0, #16 * 11]\n"  \
                             "stp x24,x25, [%0, #16 * 12]\n"  \
                             "stp x26,x27, [%0, #16 * 13]\n"  \
                             "stp x28,x29, [%0, #16 * 14]\n"  \
                             "mov x21, sp\n"                  \
                             "stp x30, x21, [%0, #16 * 15]\n" \
                             :                                \
                             : "r"(&fm.arm)                   \
                             : "x21", "memory");              \
        fm.errno_ = errno;                                    \
        fm.tid = sys_gettid();                                \
        fm.arm.pc = (u64) && label_bbox;                      \
    } while (0)

#define SET_FRAME(fm, reg)                               \
    do {                                                 \
        /* Don't override the FPU status register.    */ \
        /* Use the value obtained from ptrace(). This */ \
        /* works, because our code does not perform   */ \
        /* any FPU operations, itself.                */ \
        errno = (fm).errno_;                             \
        (reg) = (fm).arm;                                \
    } while (0)
#else
#define CPURegs
#endif

struct BBOX_ELF_SIGINFO { /* Information about signal (unused)         */
    int32_t iSigno;       /* Signal number                             */
    int32_t iCode;        /* Extra code                                */
    int32_t iErrno;       /* Errno                                     */
};

struct BBOX_ELF_PRPSINFO { /* Information about process                 */
    unsigned char ucState; /* Numeric process state                     */
    char cSname;           /* Char for pr_state                         */
    unsigned char ucZomb;  /* Zombie                                    */
    signed char cNice;     /* Nice val                                  */
    unsigned long ulLlag;  /* Flags                                     */
#if (defined(__x86_64__)) || (defined(__aarch64__))
    uint32_t tUid; /* User ID                                   */
    uint32_t tGid; /* Group ID                                  */
#else
    uint16_t tUid; /* User ID                                   */
    uint16_t tGid; /* Group ID                                  */
#endif
    pid_t tpid;       /* Process ID                                */
    pid_t tPpid;      /* Parent's process ID                       */
    pid_t tPgrp;      /* Group ID                                  */
    pid_t tSid;       /* Session ID                                */
    char cFname[16];  /* Filename of executable                    */
    char cPsargs[80]; /* Initial part of arg list                  */
};

#if defined(__ARM_ARCH_5TE__) || (defined(__ARM_ARCH_7A__))
struct BBOX_FPXREGSET {
    /* No extended FPU registers on ARM */
};

struct BBOX_FP_REGSET {
    unsigned int _sign1 : 1;
    unsigned int _unused : 15;
    unsigned int _sign2 : 1;
    unsigned int _exponent : 14;
    unsigned int _j : 1;
    unsigned int _mantissa1 : 31;
    unsigned int _mantissa0 : 32;
};

struct BBOX_FPREGSET {
    /* FPU registers */
    struct BBOX_FP_REGSET _fpregs[8];
    unsigned int _fpsr : 32;
    unsigned int _fpcr : 32;
    unsigned char _ftype[8];
    unsigned int _init_flag;
};
#elif defined(__i386__)
/* member variable are named by register. */
struct BBOX_FPREGSET { /* FPU registers */
    uint32_t _cwd;
    uint32_t _swd;
    uint32_t _twd;
    uint32_t _fip;
    uint32_t _fcs;
    uint32_t _foo;
    uint32_t _fos;
    uint32_t _st_space[20]; /* 8*10 bytes for each FP-reg = 80 bytes */
};

/* member variable are named by register. */
struct BBOX_FPXREGSET { /* SSE registers */
    uint16_t _cwd;
    uint16_t _swd;
    uint16_t _twd;
    uint16_t _fop;
    uint32_t _fip;
    uint32_t _fcs;
    uint32_t _foo;
    uint32_t _fos;
    uint32_t _mxcsr;
    uint32_t _mxcsr_mask;
    uint32_t _st_space[32];  /*  8*16 bytes for each FP-reg  = 128 bytes  */
    uint32_t _xmm_space[64]; /* 16*16 bytes for each XMM-reg = 128 bytes  */
    uint32_t _padding[24];
};
#elif defined(__x86_64__)
struct BBOX_FPXREGSET { /* x86-64 stores FPU registers in SSE struct */
    /* nothing */
};

/* member variable are named by register. */
struct BBOX_FPREGSET { /* FPU registers */
    uint16_t _cwd;
    uint16_t _swd;
    uint16_t _twd;
    uint16_t _fop;
    uint32_t _fip;
    uint32_t _fcs;
    uint32_t _foo;
    uint32_t _fos;
    uint32_t _mxcsr;
    uint32_t _mxcsr_mask;
    uint32_t _st_space[32];  /*  8*16 bytes for each FP-reg  = 128 bytes */
    uint32_t _xmm_space[64]; /* 16*16 bytes for each XMM-reg = 128 bytes */
    uint32_t _padding[24];
};
#elif defined(__aarch64__)
struct BBOX_FPXREGSET { /* x86-64 stores FPU registers in SSE struct */
    /* nothing */
};

/* member variable are named by register. */
struct BBOX_FPREGSET { /* FPU registers */
    /* nothing */
};
#endif

struct BBOX_ELF_PRPSSTATUS {                        /* Information about thread; includes CPU reg */
    struct BBOX_ELF_SIGINFO stSigInfo;              /* Info associated with signal               */
    uint16_t tCurSig;                               /* Current signal                            */
    unsigned long ulSigPend;                        /* Set of pending signals                    */
    unsigned long ulSigHold;                        /* Set of held signals                       */
    pid_t tpid;                                     /* Process ID                                */
    pid_t tPpid;                                    /* Parent's process ID                       */
    pid_t tPgrp;                                    /* Group ID                                  */
    pid_t tSid;                                     /* Session ID                                */
    struct BBOX_ELF_TIMEVAL stUserTime;             /* User time                                 */
    struct BBOX_ELF_TIMEVAL stSystemTime;           /* System time                               */
    struct BBOX_ELF_TIMEVAL stCumulativeUserTime;   /* Cumulative user time                      */
    struct BBOX_ELF_TIMEVAL stCumulativeSystemTime; /* Cumulative system time                    */
    struct CPURegs stRegisters;                     /* CPU registers                             */
    uint32_t tFpvalid;                              /* True if math co-processor being used      */
};

struct BBOX_ELF_NOTE {
    char* pName;
    int iNoteType;
    unsigned int uiDataSize;
    void* pData;
};

struct BBOX_CORE_USER { /* Ptrace returns this data for thread state */
    struct CPURegs stRegisters; /* CPU registers                             */
    unsigned long ulFpvalid;    /* True if math co-processor being used      */
#if defined(__i386__) || defined(__x86_64__)
    struct BBOX_FPREGSET stFpregs; /* FPU registers                             */
#endif
    unsigned long ulTextSegmentSize;   /* Text segment size in pages                */
    unsigned long ulDateSegmentSize;   /* Data segment size in pages                */
    unsigned long ulStackSegmentSize;  /* Stack segment size in pages               */
    unsigned long ulStartCodeAddress;  /* Starting virtual address of text          */
    unsigned long ulStartStackAddress; /* Starting virtual address of stack area    */
    unsigned long ulSignal;            /* Signal that caused the core dump          */
    unsigned long ulReserved;          /* No longer used                            */
    struct CPURegs* pstRegs;           /* Used by gdb to help find the CPU registers */
#if defined(__i386__) || defined(__x86_64__)
    struct BBOX_FPREGSET* pstFpregs; /* Pointer to FPU registers                  */
#endif
    unsigned long ulCoreFileMagic; /* Magic for old A.OUT core files            */
    char acUserCommand[32];        /* User command that was responsible         */
    unsigned long ulDebugReg[8];
#if (defined(__i386__) || defined(__x86_64__))
    unsigned long ulErrorCode;    /* CPU error code or 0                       */
    unsigned long ulFaultAddress; /* CR3 or 0                                  */
#elif defined(__ARM_ARCH_5TE__) || (defined(__ARM_ARCH_7A__))
    struct BBOX_FPREGSET stFpregs;   /* FPU registers                             */
    struct BBOX_FPREGSET* pstFpregs; /* Pointer to FPU registers                  */
#endif
};

struct BBOX_ELF_NOTE_INFO {
    pid_t tMainPid;                                  /* main pid */
    int iCoreUserFlags;
    int iThreadNoteInfoNum;                          /* count of all process */
    int iAuxvNoteInfoNum;                            /* number of Auxv in core file */
    int iFpxRegistersFlag;                           /* whether the SSE register is in the SSE structure */
    int iExtraNoteNum;                               /* number of extra note */
    size_t uiNoteAlign;                              /* fill the size of 0 for alignment */
    struct BBOX_ELF_PRPSINFO stPrpsinfo;             /* store the discription information of process */
    struct BBOX_CORE_USER stCoreUser;                /* store user data */
    struct BBOX_THREAD_NOTE_INFO* pstThreadNoteInfo; /* store status information of process, include register information, run time, and so on. */
    struct BBOX_EXTRA_NOTE* pstExtraNoteInfo;        /* additional populating system information segments */
};

struct BBOX_THREAD_NOTE_INFO {
    struct BBOX_ELF_PRPSSTATUS stPrpsstatus; /* store porcess cpu register information. */
    struct BBOX_FPREGSET stFpRegisters;      /* store porcess fpu register information. */
    struct BBOX_FPXREGSET stFpxRegisters;    /* store porcess see register information. */
    pid_t tPid;                              /* store process id */
};

struct BBOX_VM_MAPS {
    size_t uiStartAddress; /* start address of segment in address space. */
    size_t uiEndAddress;   /* end address of segment in address space. */
    size_t uiOffset;       /* offset of segment in address space. */
    size_t uiWriteSize;    /* size of core file written by segment. */
    unsigned int iFlags;   /* jurisdiction flag of segment in address space. */
    int iIsRemoveFlags;    /* mark whether the segment is written to core file */
};

union BBOX_VM_VDSO {
    size_t uiVDSOAddress; /* load address of VDSO */
    BBOX_EHDR* pVDSOEhdr; /* address of VDSO header */
};

struct BBOX_WRITE_FDS {
    size_t uiMaxLength; /* the max length of core file */
    int iWriteFd;       /* fd of core file */
};

typedef struct BBOX_SECTION {
    unsigned int uiSectionType;                /* type */
    unsigned int uiSectionNameSize;            /* size of section name */
    unsigned int uiSectionDescSize;            /* size of section discription */
    char acSectionName[BBOX_SECTION_NAME_LEN]; /* name */
    char* pacSectionDesc;                      /* to store the array of section discription */
} BBOX_SECTION_STRU;

struct BBOX_ELF_SECTION {
    unsigned int uiSectionNum;
    BBOX_SECTION_STRU* pstSection;
};

/* extern interface of bbox module, for creating core file. */
extern s32 BBOX_DoDumpElfCore(
    BBOX_GetAllThreadDone pDone, void* pDoneHandle, s32 iNumThreads, pid_t* pPids, va_list ap);

#ifdef __cplusplus
#if __cplusplus
}
#endif
#endif /* __cplusplus */

#endif
