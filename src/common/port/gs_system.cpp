/* -------------------------------------------------------------------------
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1988, 1993 The Regents of the University of California.
 *
 *
 * gs_system.cpp
 *
 * IDENTIFICATION
 *    src/common/port/gs_system.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "c.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <signal.h>
#include <paths.h>
#include <errno.h>
#include "gs_threadlocal.h"
#include "securec.h"
#include "securec_check.h"

#ifndef USE_ASSERT_CHECKING
#define Assert(p)
#else
#define Assert(p) assert(p)
#endif

static THR_LOCAL pid_t childPid = 0;

int gs_system(const char* command)
{
    return system(command);
}

/*
 * @@GaussDB@@
 * Brief		: caculate the number of args and assign the args to argv
 * Description	:
 * Notes		:
 */
static int setArgs(char* str, char** argv)
{
    int count = 0;

    Assert(str != NULL);
    while (isspace(*str)) {
        str++;
    }
    while (*str) {
        if (argv != NULL) {
            argv[count] = str;
        }
        while (*str && !isspace(*str)) {
            str++;
        }

        if ((argv != NULL) && *str) {
            *str++ = '\0';
        }
        while (isspace(*str)) {
            str++;
        }
        count++;
    }
    return count;
}

/*
 * @@GaussDB@@
 * Brief		: parse string info args
 * Description	:
 * Notes		: the input str must be revisable
 */
static char** parseStringToArgs(char* str, int* argc)
{
    char** argv = NULL;
    int argn = 0;

    Assert(str != NULL && argc != NULL);
    *argc = 0;

    /* get the argc of the string */
    argn = setArgs(str, NULL);
    if (argn == 0) {
        return NULL;
    }

    /* get the argv of the string */
    argv = (char**)malloc((argn + 1) * sizeof(char*));
    if (argv == NULL) {
        fprintf(stderr, "memory malloc failed.\n");
        return NULL;
    }
    int rc = memset_s(argv, sizeof(char*) * (argn + 1), 0, sizeof(char*) * (argn + 1));
    securec_check_ss_c(rc, "\0", "\0");

    argn = setArgs(str, argv);
    *argc = argn;

    return argv;
}

/*
 * @@GaussDB@@
 * Brief		: a security implement of popen in gaussdb
 * Description	:
 * Notes		:
 */
FILE* popen_security(const char* command, char type)
{
    int pipefd[2];
    pid_t pid;
    char** argv = NULL;
    int argc = 0;
    char* cmd = NULL;

    if (type != 'r' && type != 'w') {
        return NULL;
    }
    if (pipe(pipefd) < 0) {  /* create pipe */
        return NULL;
    }
    switch (pid = fork()) /* create child process */
    {
        case -1: /* error */
            close(pipefd[0]);
            close(pipefd[1]);
            return NULL;
        case 0: /* in child process */
            if (type == 'r') {
                close(pipefd[0]);
                dup2(pipefd[1], fileno(stderr));
                close(pipefd[1]);
            } else {
                close(pipefd[1]);
                dup2(pipefd[0], fileno(stdin));
                close(pipefd[0]);
            }
            /* parse string info args */
            cmd = strdup(command);
            if (cmd == NULL) {
                _exit(1);
            }
            argv = parseStringToArgs(cmd, &argc);
            if (argv == NULL || argc == 0) {
                _exit(1);
            }
            (void)execvp(argv[0], argv);
            _exit(127);
        default: /* in parent process */
            break;
    }

    childPid = pid;
    if (type == 'r') {
        close(pipefd[1]);
        return fdopen(pipefd[0], "r");
    } else {
        close(pipefd[0]);
        return fdopen(pipefd[1], "w");
    }
}

/*
 * @@GaussDB@@
 * Brief		: a security implement of pclose in gaussdb
 * Description	:
 * Notes		:
 */
int pclose_security(FILE* fp)
{
    int stat;
    pid_t pid;

    Assert(fp != NULL);

    if (childPid == 0) {
        return -1; /* fp wasn't opened by popen() */
    }
    pid = childPid;
    childPid = 0;
    if (fclose(fp) == EOF) {
        return -1;
    }
    while (waitpid(pid, &stat, 0) < 0) {
        if (errno != EINTR) {
            return -1; /* error other than EINTR from waitpid() */
        }
    }
    return stat; /* return child's termination status */
}

/*
 * @@GaussDB@@
 * Brief		: a security implement of gs_popen
 * Description	:
 * Notes		:
 */
int gs_popen_security(const char* command)
{
    FILE* fp = NULL;
    char* buf = NULL;

    const int max_buf_size = 4096;

    int length = 0;
    int ret = 0;

    if (command == NULL) {
        return -1;
    }

    buf = (char*)malloc(4096);
    if (buf == NULL) {
        return -1;
    }
    int rc = memset_s(buf, 4096, 0, 4096);
    securec_check_ss_c(rc, "\0", "\0");

    fp = popen_security(command, 'r');
    if (fp == NULL) {
        fprintf(stderr, "fork pipe or memorty allocate error.");
        free(buf);
        buf = NULL;
        return -1;
    }

    if (fread(buf, 1, max_buf_size, fp)) {
        buf[max_buf_size - 1] = '\0';
        length = strlen(buf);
        buf[length - 1] = '\0';
        fprintf(stderr, "%s", buf);
    }

    ret = pclose_security(fp);

    free(buf);
    buf = NULL;
    return ret;
}

/*
 * @@GaussDB@@
 * Brief		: a security implement of system in gaussdb
 * Description	:
 * Notes		:
 */
int gs_system_security(const char* command)
{
    pid_t pid;
    int status = 0;
    struct sigaction ign, intact, quitact;
    sigset_t newsigblock, oldsigblock;
    char* cmd = NULL;
    char** argv = NULL;
    int argc = 0;

    if (command == NULL) {
        /* just checking... */
        return 1;
    }

    /*
     * Ignore SIGINT and SIGQUIT, block SIGCHLD. Remember to save existing
     * signal dispositions.
     */
    ign.sa_handler = SIG_IGN;
    (void)sigemptyset(&ign.sa_mask);
    ign.sa_flags = 0;
    (void)sigaction(SIGINT, &ign, &intact);
    (void)sigaction(SIGQUIT, &ign, &quitact);
    (void)sigemptyset(&newsigblock);
    (void)sigaddset(&newsigblock, SIGCHLD);
    (void)sigprocmask(SIG_BLOCK, &newsigblock, &oldsigblock);
    switch (pid = fork()) {
        case -1: /* error */
            break;
        case 0: /* child */
            /*
             * Restore original signal dispositions and exec the command.
             */
            (void)sigaction(SIGINT, &intact, NULL);
            (void)sigaction(SIGQUIT, &quitact, NULL);
            (void)sigprocmask(SIG_SETMASK, &oldsigblock, NULL);
            /* parse string info args */
            cmd = strdup(command);
            if (cmd == NULL) {
                _exit(1);
            }
            argv = parseStringToArgs(cmd, &argc);
            free(cmd);
            if (argv == NULL || argc == 0) {
                _exit(1);
            }
            (void)execvp(argv[0], argv);
            _exit(127);
        default: /* parent */
            do {
                pid = wait4(pid, &status, 0, (struct rusage*)0);
            } while (pid == -1 && errno == EINTR);
            break;
    }
    (void)sigaction(SIGINT, &intact, NULL);
    (void)sigaction(SIGQUIT, &quitact, NULL);
    (void)sigprocmask(SIG_SETMASK, &oldsigblock, NULL);

    return ((pid == -1) ? -1 : status);
}
