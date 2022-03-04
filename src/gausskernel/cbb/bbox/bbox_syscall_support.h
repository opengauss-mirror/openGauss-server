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
 * bbox_syscall_support.h
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/bbox/bbox_syscall_support.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __BBOX_SYSCALL_SUPPORT_H__
#define __BBOX_SYSCALL_SUPPORT_H__

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <stdlib.h>
#include <errno.h>
#include <signal.h>
#include <stdarg.h>
#include <string.h>
#include <stdint.h>
#include <stddef.h>
#include <unistd.h>
#include <dirent.h> /* Defines DT_* constants */
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/ptrace.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <syscall.h>
#include <linux/unistd.h>
#include <endian.h>
#include <sched.h>
#include <elf.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <linux/types.h>

#include <sys/poll.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#ifndef WITH_OPENEULER_OS
#include <sys/sysctl.h>
#else
#include <linux/sysctl.h>
#endif
#include <sys/uio.h>
#include <sys/wait.h>

#include "bbox_lib.h"
#include "bbox_pclint.h"

#if (defined(__x86_64__))
#include "bbox_syscall_X86-64.h"
#elif (defined(__i386__))
#include "bbox_syscall_X86.h"
#elif defined(__ARM_ARCH_5TE__) || (defined(__ARM_ARCH_7A__))
#include "bbox_syscall_ARM.h"
#elif (defined(__aarch64__))
#include "bbox_syscall_ARM64.h"
#endif

#define SA_RESTORER 0x04000000
#define KERNEL_NSIG 64
#define EM_AARCH64 183

#if (defined(__aarch64__))
struct linux_dirent {
    unsigned long d_ino;
    long d_off;
    unsigned short d_reclen;
    unsigned char d_type;
    char d_name[];
};
#else
struct linux_dirent {
    long d_ino;
    off_t d_off;
    unsigned short d_reclen;
    char d_name[];
};
#endif

struct kernel_rlimit {
    unsigned long rlim_cur;
    unsigned long rlim_max;
};

struct kernel_sigset_t {
    unsigned long sig[(KERNEL_NSIG + 8 * sizeof(unsigned long) - 1) / (8 * sizeof(unsigned long))];
};

struct kernel_sigaction {
    union {
        void (*sa_handler_)(int);
        void (*sa_sigaction_)(int, siginfo_t*, void*);
    } handle;
    unsigned long sa_flags;
    void (*sa_restorer)(void);
    struct kernel_sigset_t sa_mask;
};

struct kernel_timezone {
    int tz_minuteswest;
    int tz_dsttime;
};

struct kernel_timeval {
    long tv_sec;
    long tv_usec;
};

struct kernel_timespec {
    __kernel_time_t tv_sec; /* seconds */
    long tv_nsec;           /* nanoseconds */
};

#if (defined(__x86_64__))
struct kernel_stat {
    unsigned long st_dev;
    unsigned long st_ino;
    unsigned long st_nlink;

    unsigned int st_mode;
    unsigned int st_uid;
    unsigned int st_gid;
    unsigned int __pad0;
    unsigned long st_rdev;
    long st_size;
    long st_blksize;
    long st_blocks; /* Number 512-byte blocks allocated. */

    unsigned long st_atime_;
    unsigned long st_atime_nsec;
    unsigned long st_mtime_;
    unsigned long st_mtime_nsec;
    unsigned long st_ctime_;
    unsigned long st_ctime_nsec;
    long __unused[3];
};
#elif (defined(__i386__) || defined(__ARM_ARCH_5TE__)) || (defined(__ARM_ARCH_7A__))
struct kernel_stat {
    unsigned long st_dev;
    unsigned long st_ino;
    unsigned short st_mode;
    unsigned short st_nlink;
    unsigned short st_uid;
    unsigned short st_gid;
    unsigned long st_rdev;
    unsigned long st_size;
    unsigned long st_blksize;
    unsigned long st_blocks;
    unsigned long st_atime_;
    unsigned long st_atime_nsec;
    unsigned long st_mtime_;
    unsigned long st_mtime_nsec;
    unsigned long st_ctime_;
    unsigned long st_ctime_nsec;
    unsigned long __unused4;
    unsigned long __unused5;
};
#elif (defined(__aarch64__))
struct kernel_stat {
    unsigned long st_dev;
    unsigned long st_ino;
    unsigned int st_mode;
    unsigned int st_nlink;
    unsigned int st_uid;
    unsigned int st_gid;
    unsigned long st_rdev;
    unsigned long __pad1;
    long st_size;
    int st_blksize;
    int __pad2;
    long st_blocks;
    unsigned long st_atime_;
    unsigned long st_atime_nsec_;
    unsigned long st_mtime_;
    unsigned long st_mtime_nsec_;
    unsigned long st_ctime_;
    unsigned long st_ctime_nsec_;
    unsigned int __unused4;
    unsigned int __unused5;
};
#endif

extern long SYS_NAME(gettid)(void);
extern long SYS_NAME(getpid)(void);
extern long SYS_NAME(getppid)(void);
extern long SYS_NAME(geteuid)(void);
extern long SYS_NAME(getegid)(void);
extern long SYS_NAME(getpgrp)(void);
extern long SYS_NAME(getsid)(pid_t pid);
extern long SYS_NAME(gettimeofday)(struct kernel_timeval* tv, struct kernel_timezone* tz);

extern long SYS_NAME(ptrace)(long request, long pid, void* addr, void* data);
extern long SYS_NAME(sigaltstack)(const stack_t* uss, const stack_t* uoss);
extern long SYS_NAME(getpriority)(int which, int who);
extern long SYS_NAME(getrlimit)(unsigned int resource, struct kernel_rlimit* rlim);

extern long SYS_NAME(open)(const char* filename, int flags, int mode);
extern long SYS_NAME(close)(int fd);
extern long SYS_NAME(read)(unsigned int fd, void* buf, size_t count);
extern long SYS_NAME(write)(unsigned int fd, const void* buf, size_t count);
extern long SYS_NAME(lseek)(unsigned int fd, off_t offset, unsigned int origin);
extern long SYS_NAME(fcntl)(unsigned int fd, unsigned int cmd, unsigned long arg);
extern long SYS_NAME(getdents)(unsigned int fd, struct linux_dirent* dirent, unsigned int count);
extern long SYS_NAME(readlink)(const char* path, char* buf, int bufsiz);
extern long SYS_NAME(stat)(char* filename, struct kernel_stat* statbuf);
extern long SYS_NAME(fstat)(int fd, struct kernel_stat* statbuf);
extern long SYS_NAME(mkdir)(const char* pathname, int mode);
extern long SYS_NAME(unlink)(const char* pathname);
extern long SYS_NAME(rename)(const char* oldname, const char* newname);
extern long SYS_NAME(chmod)(const char* filename, mode_t mode);

extern long SYS_NAME(pipe)(int* fildes);
extern long SYS_NAME(pipe2)(int* fildes, int flags);
extern long SYS_NAME(dup2)(unsigned int oldfd, unsigned int newfd);
extern long SYS_NAME(dup)(unsigned int fildes);

extern long SYS_NAME(nanosleep)(struct kernel_timespec* rqtp, struct kernel_timespec* rmtp);
extern long SYS_NAME(execve)(char* name, char** argv, char** envp);
extern long SYS_NAME(vfork)(void);
extern long SYS_NAME(fork)(void);
extern long SYS_NAME(wait4)(pid_t upid, int* stat_addr, int options, struct rusage* ru);
extern long SYS_NAME(kill)(pid_t pid, int sig);
extern long SYS_NAME(sched_yield)(void);

extern long SYS_NAME(exit)(int error_code);
extern long SYS_NAME(exit_group)(int error_code);
extern long SYS_NAME(prctl)(int option, unsigned long arg2, unsigned long arg3, unsigned long arg4, unsigned long arg5);
extern long SYS_NAME(rt_sigprocmask)(
    int how, struct kernel_sigset_t* set, struct kernel_sigset_t* oset, size_t sigsetsize);
extern long SYS_NAME(rt_sigaction)(
    int sig, const struct kernel_sigaction* act, struct kernel_sigaction* oact, size_t sigsetsize);

extern int SYS_NAME(sysconf)(int name);
extern int SYS_NAME(sigemptyset)(struct kernel_sigset_t* set);
extern int SYS_NAME(sigfillset)(struct kernel_sigset_t* set);
extern int SYS_NAME(sigaddset)(struct kernel_sigset_t* set, int _signum);
extern int SYS_NAME(sigdelset)(struct kernel_sigset_t* set, int _signum);
extern int SYS_NAME(sigismember)(struct kernel_sigset_t* set, int _signum);
extern long SYS_NAME(sigprocmask)(int how, struct kernel_sigset_t* set, struct kernel_sigset_t* oldset);

#if (defined(__x86_64__))
extern long SYS_NAME(socket)(int family, int type, int protocol);
extern long SYS_NAME(clone)(unsigned long clone_flags, unsigned long newsp, int* parent_tidptr, int* child_tidptr);
extern long SYS_NAME(waitpid)(pid_t pid, int* status, int options);
extern long SYS_NAME(signal)(int _signum, void (*handler)(int));
extern long SYS_NAME(_clone)(
    int (*fn)(void*), void* child_stack, int flags, void* arg, int* parent_tidptr, void* newtls, int* child_tidptr);

#elif (defined(__i386__))
extern long SYS_NAME(waitpid)(pid_t pid, int* stat_addr, int options);
extern long SYS_NAME(signal)(int sig, __sighandler_t handler);
extern long SYS_NAME(socketcall)(int call, va_list args);
extern long SYS_NAME(clone)(
    unsigned long clone_flags, unsigned long newsp, int* parent_tid, void* newtls, int* child_tid);

extern long SYS_NAME(_socketcall)(int op, ...);
extern long SYS_NAME(socket)(int domain, int type, int protocol);
extern long SYS_NAME(_clone)(
    int (*fn)(void*), void* child_stack, int flags, void* arg, int* parent_tidptr, void* newtls, int* child_tidptr);

#elif (defined(__ARM_ARCH_5TE__)) || (defined(__ARM_ARCH_7A__))
#ifndef __NR_getrlimit
#define __NR_getrlimit 163
#endif

extern long SYS_NAME(waitpid)(pid_t pid, int* stat_addr, int options);
extern long SYS_NAME(signal)(int sig, __sighandler_t handler);
extern long SYS_NAME(socket)(int domain, int type, int protocol);
extern long SYS_NAME(clone)(
    unsigned long clone_flags, unsigned long newsp, int* parent_tidptr, void* newtls, int* child_tid);
extern long SYS_NAME(_clone)(
    int (*fn)(void*), void* child_stack, int flags, void* arg, int* parent_tidptr, void* newtls, int* child_tidptr);
#elif (defined(__aarch64__))
extern long SYS_NAME(waitpid)(pid_t pid, int* status, int options);
extern long SYS_NAME(signal)(int _signum, void (*handler)(int));
extern long SYS_NAME(socket)(int domain, int type, int protocol);
extern long SYS_NAME(_clone)(
    int (*fn)(void*), void* child_stack, int flags, void* arg, int* parent_tidptr, void* newtls, int* child_tidptr);
#endif
#endif
