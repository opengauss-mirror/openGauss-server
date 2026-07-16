/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <pg_config.h>
#include <access/xact.h>
#include <access/heapam.h>
#include "../src/compat-msvc-enter.h"
#include <postmaster/bgworker.h>
#include <commands/extension.h>
#include <commands/user.h>
#include <miscadmin.h>
#include <parser/analyze.h>
#include <storage/ipc.h>
#include "../src/compat-msvc-exit.h"
#include <utils/guc.h>
#include <utils/inval.h>
#include <nodes/print.h>
#include <commands/dbcommands.h>
#include <commands/defrem.h>
#include "parallel/parallel.h"

#include "extension_utils.cpp"
#include "export.h"
#include "compat.h"
#include "extension_constants.h"

#include "loader/loader.h"
#include "loader/bgw_counter.h"
#include "loader/bgw_interface.h"
#include "loader/bgw_launcher.h"
#include "loader/bgw_message_queue.h"
#include "loader/lwlocks.h"


static char soversion[MAX_VERSION_LEN];
static post_parse_analyze_hook_type extension_post_parse_analyze_hook = NULL;

#define POST_LOAD_INIT_FN "ts_post_load_init"
#if PG96
#ifdef WIN32
#define CalledInParallelWorker() false
#else
#define CalledInParallelWorker() false
#endif /* WIN32 */
#else
#define CalledInParallelWorker()                                                                   \
	(MyBgworkerEntry != NULL && (MyBgworkerEntry->bgw_flags & BGWORKER_CLASS_PARALLEL) != 0)
#endif /* PG96 */



static void inline do_load()
{
	char *version = extension_version();
	char soname[MAX_SO_NAME_LEN];
	post_parse_analyze_hook_type old_hook;

	StrNCpy(soversion, version, MAX_VERSION_LEN);

	/*
	 * An inval_relcache callback can be called after previous checks of
	 * loaded had found it to be false. But the inval_relcache callback may
	 * load the extension setting it to true. Thus it needs to be rechecked
	 * here again by the outer call after inval_relcache completes. This is
	 * double-check locking, in effect.
	 */
	if (loaded)
		return;

	snprintf(soname, MAX_SO_NAME_LEN, "%s-%s", EXTENSION_SO, version);

	/*
	 * Set to true whether or not the load succeeds to prevent reloading if
	 * failure happened after partial load.
	 */
	loaded = true;

	/*
	 * In a parallel worker, we're not responsible for loading libraries, it's
	 * handled by the parallel worker infrastructure which restores the
	 * library state.
	 */
	if (CalledInParallelWorker())
		return;

	/*
	 * Set the config option to let versions 0.9.0 and 0.9.1 know that the
	 * loader was preloaded, newer versions use rendezvous variables instead.
	 */
	if (strcmp(version, "0.9.0") == 0 || strcmp(version, "0.9.1") == 0)
		SetConfigOption("timescaledb.loader_present", "on", PGC_USERSET, PGC_S_SESSION);

	/*
	 * we need to capture the loaded extension's post analyze hook, giving it
	 * a NULL as previous
	 */
	old_hook = post_parse_analyze_hook;
	post_parse_analyze_hook = NULL;

	/*
	 * We want to call the post_parse_analyze_hook from the versioned
	 * extension after we've loaded the versioned so. When the file is loaded
	 * it sets post_parse_analyze_hook, which we capture and store in
	 * extension_post_parse_analyze_hook to call at the end _PG_init
	 */
	PG_TRY();
	{	
		CFunInfo temp_for_tsdb = load_external_function(soname, POST_LOAD_INIT_FN, false, NULL);
		PGFunction ts_post_load_init = temp_for_tsdb.user_fn;

		if (ts_post_load_init != NULL)
			DirectFunctionCall1(ts_post_load_init, CharGetDatum(0));
	}
	PG_CATCH();
	{
		extension_post_parse_analyze_hook = post_parse_analyze_hook;
		post_parse_analyze_hook = old_hook;
		PG_RE_THROW();
	}
	PG_END_TRY();

	extension_post_parse_analyze_hook = post_parse_analyze_hook;
	post_parse_analyze_hook = old_hook;
}

static void inline extension_check()
{
	if (!loaded)
	{
		enum ExtensionState state = extension_current_state();

		switch (state)
		{
			case EXTENSION_STATE_TRANSITIONING:

				/*
				 * Always load as soon as the extension is transitioning. This
				 * is necessary so that the extension load before any CREATE
				 * FUNCTION calls. Otherwise, the CREATE FUNCTION calls will
				 * load the .so without capturing the post_parse_analyze_hook.
				 */
			case EXTENSION_STATE_CREATED:
				do_load();
				return;
			case EXTENSION_STATE_UNKNOWN:
			case EXTENSION_STATE_NOT_INSTALLED:
				return;
		}
	}
}

extern void
ts_loader_extension_check(void)
{
	extension_check();
}

extern bool
ts_loader_extension_exists(void)
{
	return extension_exists();
}

extern char *
ts_loader_extension_version(void)
{
	return extension_version();
}