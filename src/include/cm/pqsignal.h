/* ---------------------------------------------------------------------------------------
 * 
 * pqsignal.h
 *        prototypes for the reliable BSD-style signal(2) routine.
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * 
 * IDENTIFICATION
 *        src/include/cm/pqsignal.h
 *
 * NOTES
 *	  This shouldn't be in libpq, but the monitor and some other
 *	  things need it...
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef PQSIGNAL_H
#define PQSIGNAL_H

#include <signal.h>

extern sigset_t unblock_sig, block_sig;

typedef void (*sigfunc)(int);

extern void init_signal_mask(void);

#ifndef WIN32
extern sigfunc setup_signal_handle(int signo, sigfunc func);
#endif /* WIN32 */

#endif /* PQSIGNAL_H */
