/* --------------------------------------------------------------------
 * ps_status.c
 *
 * Routines to support changing the ps display of openGauss backends
 * to contain some useful information. Mechanism differs wildly across
 * platforms.
 *
 * src/backend/utils/misc/ps_status.c
 *
 * Copyright (c) 2000-2012, PostgreSQL Global Development Group
 * various details abducted from various places
 * --------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include <unistd.h>
#ifdef HAVE_SYS_PSTAT_H
#include <sys/pstat.h> /* for HP-UX */
#endif
#ifdef HAVE_PS_STRINGS
#include <machine/vmparam.h> /* for old BSD */
#include <sys/exec.h>
#endif
#if defined(__darwin__)
#include <crt_externs.h>
#endif

#include "libpq/libpq.h"
#include "miscadmin.h"
#include "storage/proc.h"
#include "utils/ps_status.h"

extern char** environ;

/*
 * Alternative ways of updating ps display:
 *
 * PS_USE_SETPROCTITLE
 *	   use the function setproctitle(const char *, ...)
 *	   (newer BSD systems)
 * PS_USE_PSTAT
 *	   use the pstat(PSTAT_SETCMD, )
 *	   (HPUX)
 * PS_USE_PS_STRINGS
 *	   assign PS_STRINGS->ps_argvstr = "string"
 *	   (some BSD systems)
 * PS_USE_CHANGE_ARGV
 *	   assign argv[0] = "string"
 *	   (some other BSD systems)
 * PS_USE_CLOBBER_ARGV
 *	   write over the argv and environment area
 *	   (Linux and most SysV-like systems)
 * PS_USE_WIN32
 *	   push the string out as the name of a Windows event
 * PS_USE_NONE
 *	   don't update ps display
 *	   (This is the default, as it is safest.)
 */
#if defined(HAVE_SETPROCTITLE)
#define PS_USE_SETPROCTITLE
#elif defined(HAVE_PSTAT) && defined(PSTAT_SETCMD)
#define PS_USE_PSTAT
#elif defined(HAVE_PS_STRINGS)
#define PS_USE_PS_STRINGS
#elif (defined(BSD) || defined(__hurd__)) && !defined(__darwin__)
#define PS_USE_CHANGE_ARGV
#elif defined(__linux__) || defined(_AIX) || defined(__sgi) || (defined(sun) && !defined(BSD)) || defined(ultrix) || \
    defined(__ksr__) || defined(__osf__) || defined(__svr5__) || defined(__darwin__)
#define PS_USE_CLOBBER_ARGV
#elif defined(WIN32)
#define PS_USE_WIN32
#else
#define PS_USE_NONE
#endif

/* Different systems want the buffer padded differently */
#if defined(_AIX) || defined(__linux__) || defined(__darwin__)
#define PS_PADDING '\0'
#else
#define PS_PADDING ' '
#endif

/*
 * Call this early in startup to save the original argc/argv values.
 * If needed, we make a copy of the original argv[] array to preserve it
 * from being clobbered by subsequent ps_display actions.
 *
 * (The original argv[] will not be overwritten by this routine, but may be
 * overwritten during init_ps_display.	Also, the physical location of the
 * environment strings may be moved, so this should be called before any code
 * that might try to hang onto a getenv() result.)
 */
char** save_ps_display_args(int argc, char** argv)
{
    u_sess->ps_cxt.save_argc = argc;
    u_sess->ps_cxt.save_argv = argv;

#if defined(PS_USE_CLOBBER_ARGV)

    /*
     * If we're going to overwrite the argv area, count the available space.
     * Also move the environment to make additional room.
     */
    {
        char* end_of_area = NULL;
        char** new_environ;
        int i;

        /*
         * check for contiguous argv strings
         */
        for (i = 0; i < argc; i++) {
            if (i == 0 || end_of_area + 1 == argv[i])
                end_of_area = argv[i] + strlen(argv[i]);
        }

        if (end_of_area == NULL) /* probably can't happen? */
        {
            u_sess->ps_cxt.ps_buffer = NULL;
            u_sess->ps_cxt.ps_buffer_size = 0;
            return argv;
        }

        /*
         * check for contiguous environ strings following argv
         */
        for (i = 0; environ[i] != NULL; i++) {
            if (end_of_area + 1 == environ[i])
                end_of_area = environ[i] + strlen(environ[i]);
        }

        u_sess->ps_cxt.ps_buffer = argv[0];
        u_sess->ps_cxt.last_status_len = u_sess->ps_cxt.ps_buffer_size = end_of_area - argv[0];

        /*
         * move the environment out of the way
         */
        new_environ = (char**)MemoryContextAlloc(
            THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), (i + 1) * sizeof(char*));
        if (NULL == new_environ) {
            ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                    errmsg("out of memory"),
                    errdetail("Failed on request of size %lu.", (unsigned long)(size_t)((i + 1) * sizeof(char*)))));
            return NULL;
        }

        for (i = 0; environ[i] != NULL; i++) {
            new_environ[i] =
                MemoryContextStrdup(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), environ[i]);
        }
        new_environ[i] = NULL;
        environ = new_environ;
    }
#endif /* PS_USE_CLOBBER_ARGV */

#if defined(PS_USE_CHANGE_ARGV) || defined(PS_USE_CLOBBER_ARGV)

    /*
     * If we're going to change the original argv[] then make a copy for
     * argument parsing purposes.
     *
     * (NB: do NOT think to remove the copying of argv[], even though
     * postmaster.c finishes looking at argv[] long before we ever consider
     * changing the ps display.  On some platforms, getopt() keeps pointers
     * into the argv array, and will get horribly confused when it is
     * re-called to analyze a subprocess' argument string if the argv storage
     * has been clobbered meanwhile.  Other platforms have other dependencies
     * on argv[].
     */
    {
        char** new_argv;
        int i;

        new_argv = (char**)MemoryContextAlloc(
            THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), (argc + 1) * sizeof(char*));

        if (NULL == new_argv) {
            ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                    errmsg("out of memory"),
                    errdetail("Failed on request of size %lu.", (unsigned long)(size_t)((argc + 1) * sizeof(char*)))));
            return NULL;
        }

        for (i = 0; i < argc; i++) {
            new_argv[i] = MemoryContextStrdup(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), argv[i]);
        }
        new_argv[argc] = NULL;

#if defined(__darwin__)

        /*
         * Darwin (and perhaps other NeXT-derived platforms?) has a static
         * copy of the argv pointer, which we may fix like so:
         */
        *_NSGetArgv() = new_argv;
#endif

        argv = new_argv;
    }
#endif /* PS_USE_CHANGE_ARGV or PS_USE_CLOBBER_ARGV */

    return argv;
}

/*
 * Call this once during subprocess startup to set the identification
 * values.	At this point, the original argv[] array may be overwritten.
 */
void init_ps_display(const char* username, const char* dbname, const char* host_info, const char* initial_str)
{
    Assert(username);
    Assert(dbname);
    Assert(host_info);

    if (t_thrd.proc) {
        int rcs = snprintf_s(t_thrd.proc->myProgName,
            sizeof(t_thrd.proc->myProgName),
            sizeof(t_thrd.proc->myProgName) - 1,
            "%s",
            t_thrd.proc_cxt.MyProgName);
        securec_check_ss(rcs, "\0", "\0");
        t_thrd.proc->myStartTime = t_thrd.proc_cxt.MyStartTime;
    }

#ifndef PS_USE_NONE
    /* no ps display for stand-alone backend */
    if (!IsUnderPostmaster)
        return;

    /* no ps display if you didn't call save_ps_display_args() */
    if (!u_sess->ps_cxt.save_argv)
        return;

#ifdef PS_USE_CLOBBER_ARGV
    /* If ps_buffer is a pointer, it might still be null */
    if (!u_sess->ps_cxt.ps_buffer)
        return;
#endif

        /*
         * Overwrite argv[] to point at appropriate space, if needed
         */

#ifdef PS_USE_CHANGE_ARGV
    u_sess->ps_cxt.save_argv[0] = u_sess->ps_cxt.ps_buffer;
    u_sess->ps_cxt.save_argv[1] = NULL;
#endif /* PS_USE_CHANGE_ARGV */

#ifdef PS_USE_CLOBBER_ARGV
    {
        int i;

        /* make extra argv slots point at end_of_area (a NUL) */
        for (i = 1; i < u_sess->ps_cxt.save_argc; i++)
            u_sess->ps_cxt.save_argv[i] = u_sess->ps_cxt.ps_buffer + u_sess->ps_cxt.ps_buffer_size;
    }
#endif /* PS_USE_CLOBBER_ARGV */

    /*
     * Make fixed prefix of ps display.
     */

#ifdef PS_USE_SETPROCTITLE

    /*
     * apparently setproctitle() already adds a `progname:' prefix to the ps
     * line
     */
    int rc = snprintf_s(u_sess->ps_cxt.ps_buffer,
        u_sess->ps_cxt.ps_buffer_size,
        u_sess->ps_cxt.ps_buffer_size - 1,
        "%s %s %s ",
        username,
        dbname,
        host_info);
    securec_check_ss(rc, "\0", "\0");
#else
    int rc = snprintf_s(u_sess->ps_cxt.ps_buffer,
        u_sess->ps_cxt.ps_buffer_size,
        u_sess->ps_cxt.ps_buffer_size - 1,
        "postgres: %s %s %s ",
        username,
        dbname,
        host_info);
    securec_check_ss(rc, "\0", "\0");
#endif

    u_sess->ps_cxt.ps_buffer_cur_len = u_sess->ps_cxt.ps_buffer_fixed_size = strlen(u_sess->ps_cxt.ps_buffer);

    set_ps_display(initial_str, true);
#endif /* not PS_USE_NONE */
}

/*
 * Call this to update the ps status display to a fixed prefix plus an
 * indication of what you're currently doing passed in the argument.
 */
void set_ps_display(const char* activity, bool force)
{
#ifndef PS_USE_NONE
    /* u_sess->attr.attr_common.update_process_title=off disables updates, unless force = true */
    if (!force && !u_sess->attr.attr_common.update_process_title)
        return;

    /* no ps display for stand-alone backend */
    if (!IsUnderPostmaster)
        return;

#ifdef PS_USE_CLOBBER_ARGV
    /* If ps_buffer is a pointer, it might still be null */
    if (!u_sess->ps_cxt.ps_buffer)
        return;
#endif

    /* Update ps_buffer to contain both fixed part and activity */
    strlcpy(u_sess->ps_cxt.ps_buffer + u_sess->ps_cxt.ps_buffer_fixed_size,
        activity,
        u_sess->ps_cxt.ps_buffer_size - u_sess->ps_cxt.ps_buffer_fixed_size);
    u_sess->ps_cxt.ps_buffer_cur_len = strlen(u_sess->ps_cxt.ps_buffer);

    /* Transmit new setting to kernel, if necessary */

#ifdef PS_USE_SETPROCTITLE
    setproctitle("%s", u_sess->ps_cxt.ps_buffer);
#endif

#ifdef PS_USE_PSTAT
    {
        union pstun pst;

        pst.pst_command = u_sess->ps_cxt.ps_buffer;
        pstat(PSTAT_SETCMD, pst, u_sess->ps_cxt.ps_buffer_cur_len, 0, 0);
    }
#endif /* PS_USE_PSTAT */

#ifdef PS_USE_PS_STRINGS
    PS_STRINGS->ps_nargvstr = 1;
    PS_STRINGS->ps_argvstr = u_sess->ps_cxt.ps_buffer;
#endif /* PS_USE_PS_STRINGS */

#ifdef PS_USE_CLOBBER_ARGV
    /* pad unused memory; need only clobber remainder of old status string */
    if (u_sess->ps_cxt.last_status_len > u_sess->ps_cxt.ps_buffer_cur_len) {
        int rcs = memset_s(u_sess->ps_cxt.ps_buffer + u_sess->ps_cxt.ps_buffer_cur_len,
            u_sess->ps_cxt.last_status_len - u_sess->ps_cxt.ps_buffer_cur_len,
            PS_PADDING,
            u_sess->ps_cxt.last_status_len - u_sess->ps_cxt.ps_buffer_cur_len);
        securec_check_ss(rcs, "\0", "\0");
    }
    u_sess->ps_cxt.last_status_len = u_sess->ps_cxt.ps_buffer_cur_len;
#endif /* PS_USE_CLOBBER_ARGV */

#ifdef PS_USE_WIN32
    {
        /*
         * Win32 does not support showing any changed arguments. To make it at
         * all possible to track which backend is doing what, we create a
         * named object that can be viewed with for example Process Explorer.
         */
        static HANDLE ident_handle = INVALID_HANDLE_VALUE;
        char name[PS_BUFFER_SIZE + 32];

        if (ident_handle != INVALID_HANDLE_VALUE)
            CloseHandle(ident_handle);

        int rc = sprintf_s(name,
            sizeof(name),
            sizeof(name) - 1,
            "pgident(%lu): %s",
            t_thrd.proc_cxt.MyProcPid,
            u_sess->ps_cxt.ps_buffer);
        securec_check_ss(rc, "\0", "\0");
        ident_handle = CreateEvent(NULL, TRUE, FALSE, name);
    }
#endif /* PS_USE_WIN32 */
#endif /* not PS_USE_NONE */
}

/*
 * Returns what's currently in the ps display, in case someone needs
 * it.	Note that only the activity part is returned.  On some platforms
 * the string will not be null-terminated, so return the effective
 * length into *displen.
 */
const char* get_ps_display(int* displen)
{
#ifdef PS_USE_CLOBBER_ARGV
    /* If ps_buffer is a pointer, it might still be null */
    if (!u_sess->ps_cxt.ps_buffer) {
        *displen = 0;
        return "";
    }
#endif

    *displen = (int)(u_sess->ps_cxt.ps_buffer_cur_len - u_sess->ps_cxt.ps_buffer_fixed_size);

    return u_sess->ps_cxt.ps_buffer + u_sess->ps_cxt.ps_buffer_fixed_size;
}
