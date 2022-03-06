/* -------------------------------------------------------------------------
 *
 * pqsignal.c
 *	  reliable BSD-style signal(2) routine stolen from RWW who stole it
 *	  from Stevens...
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/libpq/pqsignal.c,v 1.44 2008/01/01 19:45:49 momjian Exp $
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
 *	the frontend commands as well as the backend server.
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
#include <signal.h>
#include "cm/cm_c.h"
#include "cm/pqsignal.h"

sigset_t unblock_sig, block_sig;

/*
 * Initialize BlockSig, UnBlockSig, and AuthBlockSig.
 *
 * BlockSig is the set of signals to block when we are trying to block
 * signals.  This includes all signals we normally expect to get, but NOT
 * signals that should never be turned off.
 *
 * AuthBlockSig is the set of signals to block during authentication;
 * it's essentially BlockSig minus SIGTERM, SIGQUIT, SIGALRM.
 *
 * UnBlockSig is the set of signals to block when we don't want to block
 * signals (is this ever nonzero??)
 */
void init_signal_mask(void)
{
#ifdef HAVE_SIGPROCMASK

    sigemptyset(&unblock_sig);
    /* First set all signals, then clear some. */
    sigfillset(&block_sig);

    /*
     * Unmark those signals that should never be blocked. Some of these signal
     * names don't exist on all platforms.  Most do, but might as well ifdef
     * them all for consistency...
     */
#ifdef SIGTRAP
    (void)sigdelset(&block_sig, SIGTRAP);
#endif
#ifdef SIGABRT
    (void)sigdelset(&block_sig, SIGABRT);
#endif
#ifdef SIGILL
    (void)sigdelset(&block_sig, SIGILL);
#endif
#ifdef SIGFPE
    (void)sigdelset(&block_sig, SIGFPE);
#endif
#ifdef SIGSEGV
    (void)sigdelset(&block_sig, SIGSEGV);
#endif
#ifdef SIGBUS
    (void)sigdelset(&block_sig, SIGBUS);
#endif
#ifdef SIGSYS
    (void)sigdelset(&block_sig, SIGSYS);
#endif
#ifdef SIGCONT
    (void)sigdelset(&block_sig, SIGCONT);
#endif
#ifdef SIGQUIT
    (void)sigdelset(&block_sig, SIGQUIT);
#endif
#ifdef SIGTERM
    (void)sigdelset(&block_sig, SIGTERM);
#endif
#ifdef SIGALRM
    (void)sigdelset(&block_sig, SIGALRM);
#endif
#ifdef SIGCHLD
    (void)sigdelset(&block_sig, SIGCHLD);
#endif
#ifdef SIGINT
    (void)sigdelset(&block_sig, SIGINT);
#endif
#ifdef SIGUSR1
    (void)sigdelset(&block_sig, SIGUSR1);
#endif
#ifdef SIGUSR2
    (void)sigdelset(&block_sig, SIGUSR2);
#endif
#ifdef SIGHUP
    (void)sigdelset(&block_sig, SIGHUP);
#endif

#else
    /* Set the signals we want. */
    block_sig = sigmask(SIGQUIT) | sigmask(SIGTERM) | sigmask(SIGALRM) |
                /* common signals between two */
                sigmask(SIGHUP) | sigmask(SIGINT) | sigmask(SIGUSR1) | sigmask(SIGUSR2) | sigmask(SIGWINCH) |
                sigmask(SIGFPE);
#endif
}

/* Win32 signal handling is in backend/port/win32/signal.c */
#ifndef WIN32
sigfunc setup_signal_handle(int signo, sigfunc func)
{
#if !defined(HAVE_POSIX_SIGNALS)
    return signal(signo, func);
#else
    struct sigaction act, oact;

    act.sa_handler = func;
    sigemptyset(&act.sa_mask);
    act.sa_flags = SA_ONSTACK;

    if (signo != SIGALRM)
        act.sa_flags |= SA_RESTART;

#ifdef SA_NOCLDSTOP

    if (signo == SIGCHLD)
        act.sa_flags |= SA_NOCLDSTOP;

#endif

    if (sigaction(signo, &act, &oact) < 0)
        return SIG_ERR;

    return oact.sa_handler;
#endif /* !HAVE_POSIX_SIGNALS */
}

#endif /* WIN32 */
