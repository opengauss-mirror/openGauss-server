/*-------------------------------------------------------------------------
 *
 * globals.c
 *	  global variable declarations
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/init/globals.c
 *
 * NOTES
 *	  Globals used all over the place should be declared here and not
 *	  in other modules.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/xact.h"
#include "catalog/objectaccess.h"
#include "libpq/pqcomm.h"
#include "miscadmin.h"
#include "storage/backendid.h"
#include "utils/formatting.h"
#include "utils/atomic.h"

THR_LOCAL ProtocolVersion FrontendProtocol = PG_PROTOCOL_LATEST;

THR_LOCAL volatile bool InterruptPending = false;

THR_LOCAL char my_exec_path[MAXPGPATH]; /* full path to my executable */

/*
 * Hook on object accesses.  This is intended as infrastructure for security
 * and logging plugins.
 */
THR_LOCAL object_access_hook_type object_access_hook = NULL;

/*
 * IsPostmasterEnvironment is true in a postmaster process and any postmaster
 * child process; it is false in a standalone process (bootstrap or
 * standalone backend).  IsUnderPostmaster is true in postmaster child
 * processes.  Note that "child process" includes all children, not only
 * regular backends.  These should be set correctly as early as possible
 * in the execution of a process, so that error handling will do the right
 * things if an error should occur during process initialization.
 *
 * These are initialized for the bootstrap/standalone case.
 */
THR_LOCAL bool IsUnderPostmaster = false;

volatile ThreadId PostmasterPid = 0;
bool IsPostmasterEnvironment = false;
bool open_join_children = true;
bool will_shutdown = false;

/* hard-wired binary version number */
const uint32 GRAND_VERSION_NUM = 92072;

/* This variable indicates wheather the instance is in progress of upgrade as a whole */
uint32 volatile WorkingGrandVersionNum = GRAND_VERSION_NUM;

bool InplaceUpgradePrecommit = false;

#ifdef PGXC
bool useLocalXid = false;
#endif

/*
 *     EarlyBindingTLSVariables
 *         Bind static variables to another static TLS variable's address.
 *
 *	   This is needed because of the inability of the compiler: compiler
 *	   complains if you intialize a static TLS variable as another TLS
 *	   variable's address. So we do it for compiler in the earilest stage
 *	   of thread starting once.
 */
void EarlyBindingTLSVariables(void)
{
    static THR_LOCAL bool fDone = false;

    /* This shall be done only once per thread */
    if (!fDone) {
        /* Init number formatting cache */
        Init_NUM_cache();
        /* Init transaction state */
        InitCurrentTransactionState();
    }

    fDone = true;
}
