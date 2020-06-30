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
 * bbox_syscall_ARM.h
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/bbox/bbox_syscall_ARM.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef __BBOX_SYSCALL_ARM_H_
#define __BBOX_SYSCALL_ARM_H_

#include <sys/syscall.h>
#include <sys/errno.h>

#ifndef __syscall
#if defined(__thumb__) || defined(__ARM_EABI__)
#define __SYS_REG(sys_name) register long __sysreg __asm__("r6") = __NR_##sys_name;
#define __SYS_REG_LIST(regs...) [sysreg] "r"(__sysreg), ##regs
#define __syscall(sys_name) "swi\t0"
#define __syscall_safe(sys_name)                    \
    "push  {r7}\n"                                  \
    "mov   r7,%[sysreg]\n" __syscall(sys_name) "\n" \
                                               "pop   {r7}"
#else
#define __SYS_REG(sys_name)
#define __SYS_REG_LIST(regs...) regs
#define __syscall(sys_name) "swi\t" __sys1(__NR_##sys_name) ""
#define __syscall_safe(sys_name) __syscall(sys_name)
#endif
#endif

#define SYS_NAME(name) sys_##name
#define SYS_ERRNO errno

#define ___syscall_return(type, ___res)                          \
    do {                                                         \
        if ((unsigned long)(___res) >= (unsigned long)(-4095)) { \
            SYS_ERRNO = -(___res);                               \
            ___res = -1;                                         \
        }                                                        \
        return (type)(___res);                                   \
    } while (0)

#define SYS_REG(r, a) register long __r##r __asm__("r" #r) = (long)a

/* r0..r3 are scratch registers and not preserved across function
 * calls.  We need to first evaluate the first 4 syscall arguments
 * and store them on stack.  They must be loaded into r0..r3 after
 * all function calls to avoid r0..r3 being clobbered.
 */
#define SYS_SAVE_ARG(r, a) long __tmp##r = (long)a

#define SYS_LOAD_ARG(r) register long __r##r __asm__("r" #r) = __tmp##r

#define SYS_BODY(type, name, args...)                                                                    \
    register long __res_r0 __asm__("r0");                                                                \
    long ___res;                                                                                         \
    __SYS_REG(name)                                                                                      \
    __asm__ __volatile__(__syscall_safe(name) : "=r"(__res_r0) : __SYS_REG_LIST(args) : "lr", "memory"); \
    ___res = __res_r0;                                                                                   \
    ___syscall_return(type, ___res)

#define __syscall0(type, name) \
    type SYS_NAME(name)()      \
    {                          \
        SYS_BODY(type, name);  \
    }

#define __syscall1(type, name, type1, arg1)                \
    type SYS_NAME(name)(type1 arg1)                        \
    {                                                      \
        /* There is no need for using a volatile temp.  */ \
        SYS_REG(0, arg1);                                  \
        SYS_BODY(type, name, "r"(__r0));                   \
    }

#define __syscall2(type, name, type1, arg1, type2, arg2) \
    type SYS_NAME(name)(type1 arg1, type2 arg2)          \
    {                                                    \
        SYS_SAVE_ARG(0, arg1);                           \
        SYS_SAVE_ARG(1, arg2);                           \
        SYS_LOAD_ARG(0);                                 \
        SYS_LOAD_ARG(1);                                 \
        SYS_BODY(type, name, "r"(__r0), "r"(__r1));      \
    }

#define __syscall3(type, name, type1, arg1, type2, arg2, type3, arg3) \
    type SYS_NAME(name)(type1 arg1, type2 arg2, type3 arg3)           \
    {                                                                 \
        SYS_SAVE_ARG(0, arg1);                                        \
        SYS_SAVE_ARG(1, arg2);                                        \
        SYS_SAVE_ARG(2, arg3);                                        \
        SYS_LOAD_ARG(0);                                              \
        SYS_LOAD_ARG(1);                                              \
        SYS_LOAD_ARG(2);                                              \
        SYS_BODY(type, name, "r"(__r0), "r"(__r1), "r"(__r2));        \
    }

#define __syscall4(type, name, type1, arg1, type2, arg2, type3, arg3, type4, arg4) \
    type SYS_NAME(name)(type1 arg1, type2 arg2, type3 arg3, type4 arg4)            \
    {                                                                              \
        SYS_SAVE_ARG(0, arg1);                                                     \
        SYS_SAVE_ARG(1, arg2);                                                     \
        SYS_SAVE_ARG(2, arg3);                                                     \
        SYS_SAVE_ARG(3, arg4);                                                     \
        SYS_LOAD_ARG(0);                                                           \
        SYS_LOAD_ARG(1);                                                           \
        SYS_LOAD_ARG(2);                                                           \
        SYS_LOAD_ARG(3);                                                           \
        SYS_BODY(type, name, "r"(__r0), "r"(__r1), "r"(__r2), "r"(__r3));          \
    }

#define __syscall5(type, name, type1, arg1, type2, arg2, type3, arg3, type4, arg4, type5, arg5) \
    type SYS_NAME(name)(type1 arg1, type2 arg2, type3 arg3, type4 arg4, type5 arg5)             \
    {                                                                                           \
        SYS_SAVE_ARG(0, arg1);                                                                  \
        SYS_SAVE_ARG(1, arg2);                                                                  \
        SYS_SAVE_ARG(2, arg3);                                                                  \
        SYS_SAVE_ARG(3, arg4);                                                                  \
        SYS_REG(4, arg5);                                                                       \
        SYS_LOAD_ARG(0);                                                                        \
        SYS_LOAD_ARG(1);                                                                        \
        SYS_LOAD_ARG(2);                                                                        \
        SYS_LOAD_ARG(3);                                                                        \
        SYS_BODY(type, name, "r"(__r0), "r"(__r1), "r"(__r2), "r"(__r3), "r"(__r4));            \
    }

#define __syscall6(type, name, type1, arg1, type2, arg2, type3, arg3, type4, arg4, type5, arg5, type6, arg6) \
    type SYS_NAME(name)(type1 arg1, type2 arg2, type3 arg3, type4 arg4, type5 arg5, type6 arg6)              \
    {                                                                                                        \
        SYS_SAVE_ARG(0, arg1);                                                                               \
        SYS_SAVE_ARG(1, arg2);                                                                               \
        SYS_SAVE_ARG(2, arg3);                                                                               \
        SYS_SAVE_ARG(3, arg4);                                                                               \
        SYS_REG(4, arg5);                                                                                    \
        SYS_REG(5, arg6);                                                                                    \
        SYS_LOAD_ARG(0);                                                                                     \
        SYS_LOAD_ARG(1);                                                                                     \
        SYS_LOAD_ARG(2);                                                                                     \
        SYS_LOAD_ARG(3);                                                                                     \
        SYS_BODY(type, name, "r"(__r0), "r"(__r1), "r"(__r2), "r"(__r3), "r"(__r4), "r"(__r5));              \
    }

#endif
