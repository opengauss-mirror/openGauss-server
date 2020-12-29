/* -------------------------------------------------------------------------
 *
 * pqsignal.cpp
 *	  reliable BSD-style signal(2) routine stolen from RWW who stole it
 *	  from Stevens...
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/backend/libpq/pqsignal.cpp
 *
 * NOTES
 *		This shouldn't be in libpq, but the monitor and some other
 *		things need it...
 *
 *	A NOTE ABOUT SIGNAL HANDLING ACROSS THE VARIOUS PLATFORMS.
 *
 *	pg_config.h defines the macro HAVE_POSIX_SIGNALS for some platforms and
 *	not for others.  This file and pqsignal.h use that macro to decide
 *	how to handle signalling.
 *
 *	signal(2) handling - this is here because it affects some of
 *	the frontend commands as well as the backend processes.
 *
 *	Ultrix and SunOS provide BSD signal(2) semantics by default.
 *
 *	SVID2 and POSIX signal(2) semantics differ from BSD signal(2)
 *	semantics.	We can use the POSIX sigaction(2) on systems that
 *	allow us to request restartable signals (SA_RESTART).
 *
 *	Some systems don't allow restartable signals at all unless we
 *	link to a special BSD library.
 *
 *	We devoutly hope that there aren't any systems that provide
 *	neither POSIX signals nor BSD signals.	The alternative
 *	is to do signal-handler reinstallation, which doesn't work well
 *	at all.
 * ------------------------------------------------------------------------ */

#include "postgres.h"
#include "knl/knl_variable.h"

#include <signal.h>

#include "libpq/pqsignal.h"

/*
 * Initialize BlockSig, UnBlockSig, and StartupBlockSig.
 *
 * BlockSig is the set of signals to block when we are trying to block
 * signals.  This includes all signals we normally expect to get, but NOT
 * signals that should never be turned off.
 *
 * StartupBlockSig is the set of signals to block during startup packet
 * collection; it's essentially BlockSig minus SIGTERM, SIGQUIT, SIGALRM.
 *
 * UnBlockSig is the set of signals to block when we don't want to block
 * signals (is this ever nonzero??)
 */
void pqinitmask(void)
{
#ifdef HAVE_SIGPROCMASK

    sigemptyset(&t_thrd.libpq_cxt.UnBlockSig);

    /* First set all signals, then clear some. */
    sigfillset(&t_thrd.libpq_cxt.BlockSig);
    sigfillset(&t_thrd.libpq_cxt.StartupBlockSig);

    /*
     * Unmark those signals that should never be blocked. Some of these signal
     * names don't exist on all platforms.  Most do, but might as well ifdef
     * them all for consistency...
     */
#ifdef SIGTRAP
    sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGTRAP);
    sigdelset(&t_thrd.libpq_cxt.StartupBlockSig, SIGTRAP);
#endif
#ifdef SIGABRT
    sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGABRT);
    sigdelset(&t_thrd.libpq_cxt.StartupBlockSig, SIGABRT);
#endif
#ifdef SIGILL
    sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGILL);
    sigdelset(&t_thrd.libpq_cxt.StartupBlockSig, SIGILL);
#endif
#ifdef SIGFPE
    sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGFPE);
    sigdelset(&t_thrd.libpq_cxt.StartupBlockSig, SIGFPE);
#endif
#ifdef SIGSEGV
    sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGSEGV);
    sigdelset(&t_thrd.libpq_cxt.StartupBlockSig, SIGSEGV);
#endif
#ifdef SIGBUS
    sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGBUS);
    sigdelset(&t_thrd.libpq_cxt.StartupBlockSig, SIGBUS);
#endif
#ifdef SIGSYS
    sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGSYS);
    sigdelset(&t_thrd.libpq_cxt.StartupBlockSig, SIGSYS);
#endif
#ifdef SIGCONT
    sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGCONT);
    sigdelset(&t_thrd.libpq_cxt.StartupBlockSig, SIGCONT);
#endif

/* Signals unique to startup */
#ifdef SIGQUIT
    sigdelset(&t_thrd.libpq_cxt.StartupBlockSig, SIGQUIT);
#endif
#ifdef SIGTERM
    sigdelset(&t_thrd.libpq_cxt.StartupBlockSig, SIGTERM);
#endif
#ifdef SIGALRM
    sigdelset(&t_thrd.libpq_cxt.StartupBlockSig, SIGALRM);
#endif
#else
    /* Set the signals we want. */
    t_thrd.libpq_cxt.UnBlockSig = 0;
    t_thrd.libpq_cxt.BlockSig = sigmask(SIGQUIT) | sigmask(SIGTERM) | sigmask(SIGALRM) |
                                /* common signals between two */
                                sigmask(SIGHUP) | sigmask(SIGINT) | sigmask(SIGUSR1) | sigmask(SIGUSR2) |
                                sigmask(SIGCHLD) | sigmask(SIGWINCH) | sigmask(SIGFPE);
    t_thrd.libpq_cxt.StartupBlockSig = sigmask(SIGHUP) | sigmask(SIGINT) | sigmask(SIGUSR1) | sigmask(SIGUSR2) |
                                       sigmask(SIGCHLD) | sigmask(SIGWINCH) | sigmask(SIGFPE);
#endif
}

/* Win32 signal handling is in backend/port/win32/signal.c */
#ifndef WIN32

/*
 * Set up a signal handler
 */
pqsigfunc pqsignal(int signo, pqsigfunc func)
{
#if !defined(HAVE_POSIX_SIGNALS)
    return signal(signo, func);
#else
    struct sigaction act, oact;

    act.sa_handler = func;
    sigemptyset(&act.sa_mask);
    act.sa_flags = 0;
    if (signo != SIGALRM) {
        act.sa_flags |= SA_RESTART;
    }
#ifdef SA_NOCLDSTOP
    if (signo == SIGCHLD) {
        act.sa_flags |= SA_NOCLDSTOP;
    }
#endif
    if (sigaction(signo, &act, &oact) < 0) {
        return SIG_ERR;
    }
    return oact.sa_handler;
#endif /* !HAVE_POSIX_SIGNALS */
}

int install_signal(int signo, sa_sigaction_t func)
{
    struct sigaction sa;
    sa.sa_flags = SA_SIGINFO;
    sa.sa_sigaction = func;
    (void)sigemptyset(&sa.sa_mask);

    return sigaction(signo, &sa, NULL);
}
#endif /* WIN32 */
