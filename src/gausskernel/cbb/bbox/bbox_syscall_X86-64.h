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
 * bbox_syscall_X86-64.h
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/bbox/bbox_syscall_X86-64.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __BBOX_SYSCALL_X86_64_H_
#define __BBOX_SYSCALL_X86_64_H_

#include <sys/syscall.h>
#include <sys/errno.h>

#ifndef NT_PRXFPREG
#define NT_PRXFPREG 0x46e62b7f
#endif

#define SYS_NAME(name) sys_##name
#define SYS_CALL_RET_TYPE long

#define ___syscall_clobber "r11", "rcx", "memory"

#define ___syscall_return(type, __res)                         \
    do {                                                       \
        if ((unsigned long)(__res) >= (unsigned long)(-127)) { \
            errno = -(__res);                                  \
            __res = -1;                                        \
        }                                                      \
        return (type)(__res);                                  \
    } while (0)

#ifndef __KERNEL_SYSCALLS__
#define ___syscall "syscall"
#endif

#define __syscall0(type, name)                                                               \
    type SYS_NAME(name)(void)                                                                \
    {                                                                                        \
        long ___res;                                                                         \
        __asm__ volatile(___syscall : "=a"(___res) : "0"(__NR_##name) : ___syscall_clobber); \
        ___syscall_return(type, ___res);                                                     \
    }

#define __syscall1(type, name, type1, arg1)                                                                     \
    type SYS_NAME(name)(type1 arg1)                                                                             \
    {                                                                                                           \
        long ___res;                                                                                            \
        __asm__ volatile(___syscall : "=a"(___res) : "0"(__NR_##name), "D"((long)(arg1)) : ___syscall_clobber); \
        ___syscall_return(type, ___res);                                                                        \
    }

#define __syscall2(type, name, type1, arg1, type2, arg2)                          \
    type SYS_NAME(name)(type1 arg1, type2 arg2)                                   \
    {                                                                             \
        long ___res;                                                              \
        __asm__ volatile(___syscall                                               \
                         : "=a"(___res)                                           \
                         : "0"(__NR_##name), "D"((long)(arg1)), "S"((long)(arg2)) \
                         : ___syscall_clobber);                                   \
        ___syscall_return(type, ___res);                                          \
    }

#define __syscall3(type, name, type1, arg1, type2, arg2, type3, arg3)                                \
    type SYS_NAME(name)(type1 arg1, type2 arg2, type3 arg3)                                          \
    {                                                                                                \
        long ___res;                                                                                 \
        __asm__ volatile(___syscall                                                                  \
                         : "=a"(___res)                                                              \
                         : "0"(__NR_##name), "D"((long)(arg1)), "S"((long)(arg2)), "d"((long)(arg3)) \
                         : ___syscall_clobber);                                                      \
        ___syscall_return(type, ___res);                                                             \
    }

#define __syscall4(type, name, type1, arg1, type2, arg2, type3, arg3, type4, arg4)                         \
    type SYS_NAME(name)(type1 arg1, type2 arg2, type3 arg3, type4 arg4)                                    \
    {                                                                                                      \
        long ___res;                                                                                       \
        __asm__ volatile(                                                                                  \
            "movq %5,%%r10 ;" ___syscall                                                                   \
            : "=a"(___res)                                                                                 \
            : "0"(__NR_##name), "D"((long)(arg1)), "S"((long)(arg2)), "d"((long)(arg3)), "g"((long)(arg4)) \
            : ___syscall_clobber, "r10");                                                                  \
        ___syscall_return(type, ___res);                                                                   \
    }

#define __syscall5(type, name, type1, arg1, type2, arg2, type3, arg3, type4, arg4, type5, arg5) \
    type SYS_NAME(name)(type1 arg1, type2 arg2, type3 arg3, type4 arg4, type5 arg5)             \
    {                                                                                           \
        long ___res;                                                                            \
        __asm__ volatile("movq %5,%%r10 ; movq %6,%%r8 ; " ___syscall                           \
                         : "=a"(___res)                                                         \
                         : "0"(__NR_##name),                                                    \
                         "D"((long)(arg1)),                                                     \
                         "S"((long)(arg2)),                                                     \
                         "d"((long)(arg3)),                                                     \
                         "g"((long)(arg4)),                                                     \
                         "g"((long)(arg5))                                                      \
                         : ___syscall_clobber, "r8", "r10");                                    \
        ___syscall_return(type, ___res);                                                        \
    }

#define __syscall6(type, name, type1, arg1, type2, arg2, type3, arg3, type4, arg4, type5, arg5, type6, arg6) \
    type SYS_NAME(name)(type1 arg1, type2 arg2, type3 arg3, type4 arg4, type5 arg5, type6 arg6)              \
    {                                                                                                        \
        long ___res;                                                                                         \
        __asm__ volatile("movq %5,%%r10 ; movq %6,%%r8 ; movq %7,%%r9 ; " ___syscall                         \
                         : "=a"(___res)                                                                      \
                         : "0"(__NR_##name),                                                                 \
                         "D"((long)(arg1)),                                                                  \
                         "S"((long)(arg2)),                                                                  \
                         "d"((long)(arg3)),                                                                  \
                         "g"((long)(arg4)),                                                                  \
                         "g"((long)(arg5)),                                                                  \
                         "g"((long)(arg6))                                                                   \
                         : ___syscall_clobber, "r8", "r10", "r9");                                           \
        ___syscall_return(type, ___res);                                                                     \
    }

#endif
