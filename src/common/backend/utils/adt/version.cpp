/* -------------------------------------------------------------------------
 *
 * version.c
 *	 Returns the openGauss version string
 *
 * Copyright (c) 1998-2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *
 * src/backend/utils/adt/version.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "utils/builtins.h"

Datum pgsql_version(PG_FUNCTION_ARGS)
{
    PG_RETURN_TEXT_P(cstring_to_text(PG_VERSION_STR));
}

Datum opengauss_version(PG_FUNCTION_ARGS)
{
    PG_RETURN_TEXT_P(cstring_to_text(OPENGAUSS_VERSION_NUM_STR)); 
}

Datum gs_deployment(PG_FUNCTION_ARGS)
{
#if (defined(ENABLE_MULTIPLE_NODES))
    PG_RETURN_TEXT_P(cstring_to_text("Distribute"));
#elif (defined(ENABLE_PRIVATEGAUSS))
    PG_RETURN_TEXT_P(cstring_to_text("BusinessCentralized"));
#else
    PG_RETURN_TEXT_P(cstring_to_text("OpenSourceCentralized"));
#endif
}

