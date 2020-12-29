/* -------------------------------------------------------------------------
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2005-2008, Google Inc.
 *
 *
 * bbox_syscall_support.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/bbox/bbox_syscall_support.cpp
 *
 *
 */
#include "bbox_syscall_support.h"
#include "../../src/include/securec.h"
#include "../../src/include/securec_check.h"
__syscall0(long, gettid);
__syscall0(long, getpid);
__syscall0(long, getppid);
__syscall0(long, geteuid);
__syscall0(long, getegid);
__syscall1(long, getsid, pid_t, pid);
__syscall2(long, gettimeofday, struct kernel_timeval*, tv, struct kernel_timezone*, tz);
__syscall4(long, ptrace, long, request, long, pid, void*, addr, void*, data);
__syscall2(long, sigaltstack, const stack_t*, uss, const stack_t*, uoss);
__syscall2(long, getpriority, int, which, int, who);
__syscall2(long, getrlimit, unsigned int, resource, struct kernel_rlimit*, rlim);
__syscall1(long, close, int, fd);
__syscall3(long, read, unsigned int, fd, void*, buf, size_t, count);
__syscall3(long, write, unsigned int, fd, const void*, buf, size_t, count);
__syscall3(long, lseek, unsigned int, fd, off_t, offset, unsigned int, origin);
__syscall3(long, fcntl, unsigned int, fd, unsigned int, cmd, unsigned long, arg);
__syscall2(long, fstat, int, fd, struct kernel_stat*, statbuf);
__syscall1(long, dup, unsigned int, fildes);
__syscall2(long, nanosleep, struct kernel_timespec*, rqtp, struct kernel_timespec*, rmtp);
__syscall3(long, execve, char*, name, char**, argv, char**, envp);
__syscall4(long, wait4, pid_t, upid, int*, stat_addr, int, options, struct rusage*, ru);
__syscall2(long, kill, pid_t, pid, int, sig);
__syscall0(long, sched_yield);
__syscall1(long, exit, int, error_code);
__syscall1(long, exit_group, int, error_code);
__syscall5(
    long, prctl, int, option, unsigned long, arg2, unsigned long, arg3, unsigned long, arg4, unsigned long, arg5);
__syscall4(
    long, rt_sigprocmask, int, how, struct kernel_sigset_t*, set, struct kernel_sigset_t*, oset, size_t, sigsetsize);
__syscall4(long, rt_sigaction, int, sig, const struct kernel_sigaction*, act, struct kernel_sigaction*, oact, size_t,
    sigsetsize);

#if (defined(__aarch64__))
__syscall1(long, getpgid, long, pid);
__syscall4(long, openat, int, dfd, const char*, filename, int, flags, int, mode);
__syscall3(long, getdents64, unsigned int, fd, struct linux_dirent*, dirent, unsigned int, count);
__syscall4(long, readlinkat, int, dfd, const char*, path, char*, buf, int, bufsiz);
__syscall4(long, newfstatat, int, dfd, const char*, filename, struct kernel_stat*, statbuf, int, flag);
__syscall3(long, mkdirat, int, dfd, const char*, pathname, int, mode);
__syscall3(long, unlinkat, int, dfd, const char*, pathname, int, flag);
__syscall4(long, renameat, int, olddfd, const char*, oldname, int, newdfd, const char*, newname);
__syscall3(long, fchmodat, int, dfd, const char*, filename, mode_t, mode);
__syscall2(long, pipe2, int*, fildes, int, flags);
__syscall3(long, dup3, unsigned int, oldfd, unsigned int, newfd, int, flags);
__syscall5(
    long, clone, unsigned long, clone_flags, unsigned long, newsp, int*, parent_tid, void*, newtls, int*, child_tid);

long SYS_NAME(getpgrp)(void)
{
    return sys_getpgid(sys_getpid());
}

long SYS_NAME(open)(const char* filename, int flags, int mode)
{
    return sys_openat(AT_FDCWD, filename, flags, mode);
}

long SYS_NAME(getdents)(unsigned int fd, struct linux_dirent* dirent, unsigned int count)
{
    return sys_getdents64(fd, dirent, count);
}

long SYS_NAME(readlink)(const char* path, char* buf, int bufsiz)
{
    return sys_readlinkat(AT_FDCWD, path, buf, bufsiz);
}

long SYS_NAME(stat)(char* filename, struct kernel_stat* statbuf)
{
    return sys_newfstatat(AT_FDCWD, filename, statbuf, 0);
}

long SYS_NAME(mkdir)(const char* pathname, int mode)
{
    return sys_mkdirat(AT_FDCWD, pathname, mode);
}

long SYS_NAME(unlink)(const char* pathname)
{
    return sys_unlinkat(AT_FDCWD, pathname, 0);
}

long SYS_NAME(rename)(const char* oldname, const char* newname)
{
    return sys_renameat(AT_FDCWD, oldname, AT_FDCWD, newname);
}

long SYS_NAME(chmod)(const char* filename, mode_t mode)
{
    return sys_fchmodat(AT_FDCWD, filename, mode);
}

long SYS_NAME(pipe)(int* fildes)
{
    return sys_pipe2(fildes, 0);
}

long SYS_NAME(dup2)(unsigned int oldfd, unsigned int newfd)
{
    return sys_dup3(oldfd, newfd, 0);
}

long SYS_NAME(fork)(void)
{
    return sys_clone(SIGCHLD, 0, 0, NULL, NULL);
}

#else

__syscall0(long, getpgrp);
__syscall3(long, open, const char*, filename, int, flags, int, mode);
__syscall3(long, getdents, unsigned int, fd, struct linux_dirent*, dirent, unsigned int, count);
__syscall3(long, readlink, const char*, path, char*, buf, int, bufsiz);
__syscall2(long, stat, char*, filename, struct kernel_stat*, statbuf);
__syscall2(long, mkdir, const char*, pathname, int, mode);
__syscall1(long, unlink, const char*, pathname);
__syscall2(long, rename, const char*, oldname, const char*, newname);
__syscall2(long, chmod, const char*, filename, mode_t, mode);
__syscall1(long, pipe, int*, fildes);
__syscall2(long, dup2, unsigned int, oldfd, unsigned int, newfd);
__syscall0(long, vfork);
__syscall0(long, fork);

#endif

int SYS_NAME(sysconf)(int name)
{
    switch (name) {
        case _SC_PAGESIZE:
            return getpagesize();
        case _SC_OPEN_MAX: {
            struct kernel_rlimit limit = {0};
            if (sys_getrlimit(RLIMIT_NOFILE, &limit) >= 0) {
                return limit.rlim_cur;
            } else {
                /* Default maximum open files for per process */
                return 8192;
            }
        }
        default:
            errno = ENOSYS;
            return -1;
    }
}

int SYS_NAME(sigemptyset)(struct kernel_sigset_t* set)
{
    errno_t rc = memset_s(set->sig, sizeof(set->sig), 0, sizeof(set->sig));
    securec_check_c(rc, "\0", "\0");
    return 0;
}

int SYS_NAME(sigfillset)(struct kernel_sigset_t* set)
{
    errno_t rc = memset_s(set->sig, sizeof(set->sig), 0xFF, sizeof(set->sig));
    securec_check_c(rc, "\0", "\0");
    return 0;
}

int SYS_NAME(sigaddset)(struct kernel_sigset_t* set, int __signum)
{
    int signo = (int)(8 * sizeof(set->sig));

    if (__signum < 1 || __signum > signo) {
        errno = EINVAL;
        return -1;
    } else {
        set->sig[(__signum - 1) / (8 * sizeof(set->sig[0]))] |= 1UL << ((__signum - 1) % (8 * sizeof(set->sig[0])));
        return 0;
    }
}

int SYS_NAME(sigdelset)(struct kernel_sigset_t* set, int __signum)
{
    int signo = (int)(8 * sizeof(set->sig));

    if (__signum < 1 || __signum > signo) {
        errno = EINVAL;
        return -1;
    } else {
        set->sig[(__signum - 1) / (8 * sizeof(set->sig[0]))] &= ~(1UL << ((__signum - 1) % (8 * sizeof(set->sig[0]))));
        return 0;
    }
}

int SYS_NAME(sigismember)(struct kernel_sigset_t* set, int __signum)
{
    int signo = (int)(8 * sizeof(set->sig));

    if (__signum < 1 || __signum > signo) {
        errno = EINVAL;
        return -1;
    } else {
        return !!(set->sig[(__signum - 1) / (8 * sizeof(set->sig[0]))] &
                  (1UL << ((__signum - 1) % (8 * sizeof(set->sig[0])))));
    }
}

long SYS_NAME(sigprocmask)(int how, struct kernel_sigset_t* set, struct kernel_sigset_t* oldset)
{
    long ret = 0;

    ret = SYS_NAME(rt_sigprocmask)(how, set, oldset, (KERNEL_NSIG + 7) / 8);
    return ret;
}

#if defined(__x86_64__)
__syscall3(long, socket, int, family, int, type, int, protocol);
__syscall4(long, clone, unsigned long, clone_flags, unsigned long, newsp, int*, parent_tidptr, int*, child_tidptr);
long SYS_NAME(waitpid)(pid_t pid, int* status, int options)
{
    return SYS_NAME(wait4)(pid, status, options, 0);
}

long SYS_NAME(signal)(int __signum, void (*handler)(int))
{
    struct kernel_sigaction sa;

    errno_t rc = memset_s(&sa, sizeof(sa), 0, sizeof(sa));
    securec_check_c(rc, "\0", "\0");

    sys_sigfillset(&sa.sa_mask);
    sa.sa_flags |= SA_RESTORER | SA_RESTART;
    sa.handle.sa_handler_ = handler;

    return SYS_NAME(rt_sigaction)(__signum, &sa, NULL, (KERNEL_NSIG + 7) / 8);
}

#define CLONE_SYSCALL_X86_64 \
    "movq   %2,%%rax\n"      \
    "syscall\n"              \
    "testq  %%rax,%%rax\n"   \
    "jnz    1f\n"            \
    "xorq   %%rbp,%%rbp\n"   \
    "popq   %%rax\n"         \
    "popq   %%rdi\n"         \
    "call   *%%rax\n"        \
    "movq   %%rax,%%rdi\n"   \
    "movq   %3,%%rax\n"      \
    "syscall\n"

long SYS_NAME(_clone)(
    int (*fn)(void*), void* child_stack, int flags, void* arg, int* parent_tidptr, void* newtls, int* child_tidptr)
{
    long ___res;
    {
        register void* __tls __asm__("r8") = newtls;
        register int* __ctid __asm__("r10") = child_tidptr;

        __asm__ __volatile__( 

            /* example: if (fn == NULL) return -EINVAL; */
            "testq  %4,%4\n"
            "jz     1f\n"

            /* example: if (child_stack == NULL) return -EINVAL; */
            "testq  %5,%5\n"
            "jz     1f\n"

            "subq   $0x10,%5\n"

            /* Push "arg" and "fn" onto the stack that will be
             * used by the child.
             */
            "movq   %7,0x8(%5)\n"
            "movq   %4,0x0(%5)\n"

            /* example: %rax = syscall(%rax = __NR_clone,
             *                         %rdi = flags,
             *                         %rsi = child_stack,
             *                         %rdx = parent_tidptr,
             *                         %r8  = new_tls,
             *                         %r10 = child_tidptr)
             */
            CLONE_SYSCALL_X86_64

            /* Return to parent.
             */
            "1:\n"
            : "=a"(___res)
            : "0"(-EINVAL),
            "i"(__NR_clone),
            "i"(__NR_exit),
            "r"(fn),
            "S"(child_stack),
            "D"(flags),
            "r"(arg),
            "d"(parent_tidptr),
            "r"(__tls),
            "r"(__ctid)
            : "memory", "r11", "rcx");
    }
    ___syscall_return(int, ___res);
}

#elif (defined(__i386__))
__syscall3(long, waitpid, pid_t, pid, int*, stat_addr, int, options);
__syscall2(long, signal, int, sig, __sighandler_t, handler);
__syscall2(long, socketcall, int, call, va_list, args);
__syscall5(
    long, clone, unsigned long, clone_flags, unsigned long, newsp, int*, parent_tid, void*, newtls, int*, child_tid);

long do_syscall(int number, ...)
{
    register long result;
    __asm__ volatile("push %ebx; push %esi; push %edi");
    __asm__ volatile("mov 28(%%ebp),%%edi;"
                     "mov 24(%%ebp),%%esi;"
                     "mov 20(%%ebp),%%edx;"
                     "mov 16(%%ebp),%%ecx;"
                     "mov 12(%%ebp),%%ebx;"
                     "mov  8(%%ebp),%%eax;"
                     "int $0x80"
                     : "=a"(result));
    __asm__ volatile("pop %edi; pop %esi; pop %ebx");
    return result;
}

long SYS_NAME(_socketcall)(int op, ...)
{
    int ret;
    ret = 0;
    va_list ap;
    va_start(ap, op);
    ret = SYS_NAME(socketcall)(op, ap);
    va_end(ap);
    return ret;
}

long SYS_NAME(socket)(int domain, int type, int protocol)
{
    return SYS_NAME(_socketcall)(1, domain, type, protocol);
}

#define CLONE_SYSCALL_X86_32 \
    "movl   %8,%%esi\n"      \
    "movl   %5,%%eax\n"      \
    "movl   %7,%%edx\n"      \
    "movl   %9,%%edi\n"      \
    "pushl  %%ebx\n"         \
    "movl   %%eax,%%ebx\n"   \
    "movl   %2,%%eax\n"      \
    "int    $0x80\n"         \
    "popl   %%ebx\n"         \
    "test   %%eax,%%eax\n"   \
    "jnz    1f\n"            \
    "movl   $0x0,%%ebp\n"    \
    "call   *%%ebx\n"        \
    "movl   %%eax,%%ebx\n"   \
    "movl   $0x1,%%eax\n"    \
    "int    $0x80\n"

long SYS_NAME(_clone)(
    int (*fn)(void*), void* child_stack, int flags, void* arg, int* parent_tidptr, void* newtls, int* child_tidptr)
{
    long ___res;

    __asm__ __volatile__( 
        /* example: if (fn == NULL) return -EINVAL; */
        "movl   %3,%%ecx\n"
        "jecxz  1f\n"

        /* example: if (child_stack == NULL) return -EINVAL; */
        "movl   %4,%%ecx\n"
        "jecxz  1f\n"

        /* Set up alignment of the child stack:
         * example: child_stack = (child_stack & ~0xF) - 20;
         */
        "andl   $-16,%%ecx\n"
        "subl   $0x14,%%ecx\n"

        /* Push "arg" and "fn" onto the stack that will be
         * used by the child.
         */
        "movl   %6,%%eax\n"
        "movl   %%eax,4(%%ecx)\n"
        "movl   %3,%%eax\n"
        "movl   %%eax,(%%ecx)\n"

        /* example: %eax = syscall(%eax = __NR_clone,
         *                         %ebx = flags,
         *                         %ecx = child_stack,
         *                         %edx = parent_tidptr,
         *                         %esi = newtls,
         *                         %edi = child_tidptr)
         * Also, make sure that %ebx gets preserved as it is
         * used in PIC mode.
         */
        CLONE_SYSCALL_X86_32

        /* Return to parent.
         */
        "1:\n"
        : "=a"(___res)
        : "0"(-EINVAL),
        "i"(__NR_clone),
        "m"(fn),
        "m"(child_stack),
        "m"(flags),
        "m"(arg),
        "m"(parent_tidptr),
        "m"(newtls),
        "m"(child_tidptr)
        : "memory", "ecx", "edx", "esi", "edi");
    ___syscall_return(int, ___res);
}

#elif (defined(__ARM_ARCH_5TE__)) || (defined(__ARM_ARCH_7A__))
__syscall3(long, socket, int, d, int, t, int, p);
__syscall5(
    long, clone, unsigned long, clone_flags, unsigned long, newsp, int*, parent_tid, void*, newtls, int*, child_tid);

long SYS_NAME(waitpid)(pid_t pid, int* status, int options)
{
    return SYS_NAME(wait4)(pid, status, options, 0);
}

long SYS_NAME(signal)(int __signum, void (*handler)(int))
{
    struct kernel_sigaction _sa;
    struct kernel_sigaction old;

    errno_t rc = memset_s(&_sa, sizeof(_sa), 0, sizeof(_sa));
    securec_check_c(rc, "\0", "\0");
    sys_sigfillset(&_sa.sa_mask);
    _sa.sa_flags |= SA_RESTORER | SA_RESTART;
    _sa.handle.sa_handler_ = handler;

    return SYS_NAME(rt_sigaction)(__signum, &_sa, &old, (KERNEL_NSIG + 7) / 8);
}

long SYS_NAME(_clone)(
    int (*fn)(void*), void* child_stack, int flags, void* arg, int* parent_tidptr, void* newtls, int* child_tidptr)
{
    register long ___res __asm__("r5");

    {
        if (fn == NULL || child_stack == NULL) {
            ___res = -EINVAL;
            goto _clone_exit;
        }

        /* stash first 4 arguments on stack first because we can only load
         * them after all function calls.
         */
        int tmp_flags = flags;
        int* tmp_stack = (int*)child_stack;
        void* tmp_ptid = parent_tidptr;
        void* tmp_tls = newtls;

        register int* ___ctid __asm__("r4") = child_tidptr;

        /* Push "arg" and "fn" onto the stack that will be
         * used by the child.
         */
        *(--tmp_stack) = (int)arg;
        *(--tmp_stack) = (int)fn;

        /* We must load r0..r3 last after all possible function calls.  */
        register int ___flags __asm__("r0") = tmp_flags;
        register void* ___stack __asm__("r1") = tmp_stack;
        register void* ___ptid __asm__("r2") = tmp_ptid;
        register void* ___tls __asm__("r3") = tmp_tls;

        /* example: %r0 = syscall(%r0 = flags,
         *                        %r1 = child_stack,
         *                        %r2 = parent_tidptr,
         *                        %r3 = newtls,
         *                        %r4 = child_tidptr)
         */
        __SYS_REG(clone)
        __asm__ __volatile__(
            "push  {r7}\n"
            "mov   r7,%1\n" __syscall(clone) "\n"

                                             "movs  %0,r0\n"
                                             "bne   1f\n"

                                             "ldr   r0,[sp, #4]\n"
                                             "mov   lr,pc\n"
                                             "ldr   pc,[sp]\n"

                                             "mov   r7,%2\n" __syscall(exit) "\n"

                                                                             "1: pop {r7}\n"
            : "=r"(___res)
            : "r"(__sysreg), "i"(__NR_exit), "r"(___stack), "r"(___flags), "r"(___ptid), "r"(___tls), "r"(___ctid)
            : "cc", "lr", "memory");
    }

_clone_exit:
    ___syscall_return(int, ___res);
}

#elif (defined(__aarch64__))
__syscall3(long, socket, int, family, int, type, int, protocol);

long SYS_NAME(waitpid)(pid_t pid, int* status, int options)
{
    return SYS_NAME(wait4)(pid, status, options, 0);
}

long SYS_NAME(signal)(int __signum, void (*handler)(int))
{
    struct kernel_sigaction _sa;
    struct kernel_sigaction old;

    errno_t rc = memset_s(&_sa, sizeof(_sa), 0, sizeof(_sa));
    securec_check_c(rc, "\0", "\0");
    sys_sigfillset(&_sa.sa_mask);
    _sa.sa_flags |= SA_RESTORER | SA_RESTART;
    _sa.handle.sa_handler_ = handler;

    return SYS_NAME(rt_sigaction)(__signum, &_sa, &old, (KERNEL_NSIG + 7) / 8);
}

long SYS_NAME(_clone)(
    int (*fn)(void*), void* child_stack, int flags, void* arg, int* parent_tidptr, void* newtls, int* child_tidptr)
{
    register long __res_x0 __asm__("x0");
    long ___res;
    {
        register int (*__fn)(void*) __asm__("x0") = fn;
        register void* __stack __asm__("x1") = child_stack;
        register int __flags __asm__("x2") = flags;
        register void* __arg __asm__("x3") = arg;
        register int* __ptid __asm__("x4") = parent_tidptr;
        register void* __tls __asm__("x5") = newtls;
        register int* __ctid __asm__("x6") = child_tidptr;

        __asm__ __volatile__( 

            /* example: if (fn == NULL || child_stack == NULL) return -EINVAL; */
            "cbz     x0,1f\n"
            "cbz     x1,1f\n"

            /* Push "arg" and "fn" onto the stack that will be
             * used by the child.
             */
            "stp x0,x3, [x1, #-16]!\n"

            "mov x0,x2\n" /* flags  */
            "mov x2,x4\n" /* ptid  */
            "mov x3,x5\n" /* tls */
            "mov x4,x6\n" /* ctid */
            "mov x8,%9\n" /* clone */

            "svc 0x0\n"

            /* example: if (%r0 != 0) return %r0; */
            "cmp x0, #0\n"
            "bne 2f\n"

            /* In the child, now. Call "fn(arg)".
             */
            "ldp x1, x0, [sp], #16\n"
            "blr x1\n"

            /* example: Call _exit(%r0).
             */
            "mov x8, %10\n"
            "svc 0x0\n"
            "1:\n"
            "mov x8, %1\n"
            "2:\n"
            : "=r"(__res_x0)
            : "i"(-EINVAL),
            "r"(__fn),
            "r"(__stack),
            "r"(__flags),
            "r"(__arg),
            "r"(__ptid),
            "r"(__tls),
            "r"(__ctid),
            "i"(__NR_clone),
            "i"(__NR_exit)
            : "x30", "memory");
    }
    ___res = __res_x0;
    ___syscall_return(int, ___res);
}

#endif
