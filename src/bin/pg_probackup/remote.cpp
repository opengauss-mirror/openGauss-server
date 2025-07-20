/*-------------------------------------------------------------------------
 *
 * remote.c
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */

#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <signal.h>

#ifdef WIN32
#define __thread __declspec(thread)
#else
#include <pthread.h>
#endif

#include "pg_probackup.h"
#include "file.h"
#include "common/fe_memutils.h"

#define MAX_CMDLINE_LENGTH  4096
#define MAX_CMDLINE_OPTIONS 256
/* is not used now #define ERR_BUF_SIZE        4096 may be removed in the future*/
#define PIPE_SIZE           (64*1024)

static int split_options(int argc, char* argv[], int max_options, char* options)
{
    char* opt = options;
    char in_quote = '\0';
    while (true) {
        switch (*opt) {
          case '\'':
          case '\"':
            if (!in_quote) {
                in_quote = *opt++;
                continue;
            }
            if (*opt == in_quote && *++opt != in_quote) {
                in_quote = '\0';
                continue;
            }
            break;
          case '\0':
            if (opt != options) {
                argv[argc++] = options;
                if (argc >= max_options)
                    elog(ERROR, "Too much options");
            }
            return argc;
          case ' ':
            argv[argc++] = options;
            if (argc >= max_options)
                elog(ERROR, "Too much options");
            *opt++ = '\0';
            options = opt;
            continue;
          default:
            break;
        }
        opt += 1;
    }
    return argc;
}

static __thread int child_pid;




void wait_ssh(void)
{
/*
 * We need to wait termination of SSH process to eliminate zombies.
 * There is no waitpid() function at Windows but there are no zombie processes caused by lack of wait/waitpid.
 * So just disable waitpid for Windows.
 */
#ifndef WIN32
    int status;
    waitpid(child_pid, &status, 0);
    elog(LOG, "SSH process %d is terminated with status %d",  child_pid, status);
#endif
}

#ifdef WIN32
void launch_ssh(char* argv[])
{
    int infd = atoi(argv[2]);
    int outfd = atoi(argv[3]);

    SYS_CHECK(close(STDIN_FILENO));
    SYS_CHECK(close(STDOUT_FILENO));

    SYS_CHECK(dup2(infd, STDIN_FILENO));
    SYS_CHECK(dup2(outfd, STDOUT_FILENO));

    SYS_CHECK(execvp(argv[4], argv+4));
}
#endif

static bool needs_quotes(char const* path)
{
    return strchr(path, ' ') != NULL;
}

bool launch_agent(void)
{
    char cmd[MAX_CMDLINE_LENGTH];
    char* ssh_argv[MAX_CMDLINE_OPTIONS];
    char libenv[MAX_CMDLINE_LENGTH] = {0};
    int ssh_argc;
    int outfd[2];
    int infd[2];
    int errfd[2];
    int agent_version;
    errno_t rc = 0;

    ssh_argc = 0;
#ifdef WIN32
    ssh_argv[ssh_argc++] = PROGRAM_NAME_FULL;
    ssh_argv[ssh_argc++] = (char *)"ssh";
    ssh_argc += 2; /* reserve space for pipe descriptors */
#endif
    ssh_argv[ssh_argc++] = (char *)instance_config.remote.proto;
    if (instance_config.remote.port != NULL) {
        ssh_argv[ssh_argc++] = (char *)"-p";
        ssh_argv[ssh_argc++] = (char *)instance_config.remote.port;
    }
    if (instance_config.remote.user != NULL) {
        ssh_argv[ssh_argc++] = (char *)"-l";
        ssh_argv[ssh_argc++] = (char *)instance_config.remote.user;
    }
    if (instance_config.remote.ssh_config != NULL) {
        ssh_argv[ssh_argc++] = (char *)"-F";
        ssh_argv[ssh_argc++] = (char *)instance_config.remote.ssh_config;
    }
    if (instance_config.remote.ssh_options != NULL) {
        ssh_argc = split_options(ssh_argc, (char **)const_cast<char **>(ssh_argv), MAX_CMDLINE_OPTIONS, pg_strdup(instance_config.remote.ssh_options));
    }

    ssh_argv[ssh_argc++] = (char *)"-o";
    ssh_argv[ssh_argc++] = (char *)"PasswordAuthentication=no";

    ssh_argv[ssh_argc++] = (char *)"-o";
    ssh_argv[ssh_argc++] = (char *)"Compression=no";

    ssh_argv[ssh_argc++] = (char *)"-o";
    ssh_argv[ssh_argc++] = (char *)"ControlMaster=no";

    ssh_argv[ssh_argc++] = (char *)"-o";
    ssh_argv[ssh_argc++] = (char *)"LogLevel=error";

    ssh_argv[ssh_argc++] = (char *)instance_config.remote.host;
    ssh_argv[ssh_argc++] = (char *)cmd;
    ssh_argv[ssh_argc] = NULL;

    if (instance_config.remote.libpath)
    {
#ifdef WIN32
        rc = snprintf_s(libenv, sizeof(libenv), sizeof(libenv) - 1, "set LD_LIBRARY_PATH=%s &&",
                     instance_config.remote.libpath);
        securec_check_ss_c(rc, "\0", "\0");
#else
        rc = snprintf_s(libenv, sizeof(libenv), sizeof(libenv) - 1, "export LD_LIBRARY_PATH=%s &&",
                     instance_config.remote.libpath);
        securec_check_ss_c(rc, "\0", "\0");
#endif
    }

    if (instance_config.remote.path)
    {
        char const* probackup = PROGRAM_NAME_FULL;
        char* sep = (char *)strrchr(probackup, '/');
        if (sep != NULL) {
            probackup = sep + 1;
        }
#ifdef WIN32
        else {
            sep = strrchr(probackup, '\\');
            if (sep != NULL) {
                probackup = sep + 1;
            }
        }
        if (needs_quotes(instance_config.remote.path) || needs_quotes(PROGRAM_NAME_FULL))
        {
            rc = snprintf_s(cmd, sizeof(cmd), sizeof(cmd) - 1, "\"%s %s\\%s\" agent",
                    libenv, instance_config.remote.path, probackup);
            securec_check_ss_c(rc, "\0", "\0");
        }
        else
        {
            rc = snprintf_s(cmd, sizeof(cmd), sizeof(cmd) - 1, "%s %s\\%s agent",
                    libenv, instance_config.remote.path, probackup);
            securec_check_ss_c(rc, "\0", "\0");
        }
#else
        if (needs_quotes(instance_config.remote.path) || needs_quotes(PROGRAM_NAME_FULL))
        {
            rc = snprintf_s(cmd, sizeof(cmd), sizeof(cmd) - 1, "\"%s %s/%s\" agent",
                    libenv, instance_config.remote.path, probackup);
            securec_check_ss_c(rc, "\0", "\0");
        }
        else
        {
            rc = snprintf_s(cmd, sizeof(cmd), sizeof(cmd) - 1, "%s %s/%s agent",
                    libenv, instance_config.remote.path, probackup);
            securec_check_ss_c(rc, "\0", "\0");
        }
#endif
    } else {
        if (needs_quotes(PROGRAM_NAME_FULL))
        {
            rc = snprintf_s(cmd, sizeof(cmd), sizeof(cmd) - 1, "\"%s\" agent", PROGRAM_NAME_FULL);
            securec_check_ss_c(rc, "\0", "\0");
        }
        else
        {
            rc = snprintf_s(cmd, sizeof(cmd), sizeof(cmd) - 1, "%s agent", PROGRAM_NAME_FULL);
            securec_check_ss_c(rc, "\0", "\0");
        }
    }

#ifdef WIN32
    SYS_CHECK(_pipe(infd, PIPE_SIZE, _O_BINARY)) ;
    SYS_CHECK(_pipe(outfd, PIPE_SIZE, _O_BINARY));
    ssh_argv[2] = (const char *)format_text("%d", outfd[0]);
    ssh_argv[3] = (const char *)format_text("%d", infd[1]);
    {
        intptr_t pid = _spawnvp(_P_NOWAIT, ssh_argv[0], ssh_argv);
        if (pid < 0)
            return false;
        child_pid = GetProcessId((HANDLE)pid);
#else
    SYS_CHECK(pipe(infd));
    SYS_CHECK(pipe(outfd));
    SYS_CHECK(pipe(errfd));

    SYS_CHECK(child_pid = fork());

    if (child_pid == 0) { /* child */
        SYS_CHECK(close(STDIN_FILENO));
        SYS_CHECK(close(STDOUT_FILENO));
        SYS_CHECK(close(STDERR_FILENO));

        SYS_CHECK(dup2(outfd[0], STDIN_FILENO));
        SYS_CHECK(dup2(infd[1],  STDOUT_FILENO));
        SYS_CHECK(dup2(errfd[1], STDERR_FILENO));

        SYS_CHECK(close(infd[0]));
        SYS_CHECK(close(infd[1]));
        SYS_CHECK(close(outfd[0]));
        SYS_CHECK(close(outfd[1]));
        SYS_CHECK(close(errfd[0]));
        SYS_CHECK(close(errfd[1]));

        if (execvp(ssh_argv[0], ssh_argv) < 0) {
            return false;
        }
    } else {
#endif
        elog(LOG, "Start SSH client process, pid %d", child_pid);
        SYS_CHECK(close(infd[1]));  /* These are being used by the child */
        SYS_CHECK(close(outfd[0]));
        SYS_CHECK(close(errfd[1]));
        

        fio_redirect(infd[0], outfd[1], errfd[0]); /* write to stdout */
    }

    /* Make sure that remote agent has the same version
     * TODO: we must also check PG version and fork edition
     */
    agent_version = fio_get_agent_version();
    if (agent_version != AGENT_PROTOCOL_VERSION)
    {
        char agent_version_str[1024];
        errno_t rc = sprintf_s(agent_version_str, 1024, "%d.%d.%d",
                agent_version / 10000,
                (agent_version / 100) % 100,
                agent_version % 100);
        securec_check_ss_c(rc, "\0", "\0");

        elog(ERROR, "Remote agent version %s does not match local program version %s",
            agent_version_str, PROGRAM_VERSION);
    }

    return true;
}
