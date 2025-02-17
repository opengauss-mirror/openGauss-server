
#ifndef __GMS_ASSERT__
#define __GMS_ASSERT__

#include "postgres.h"

extern "C" Datum noop(PG_FUNCTION_ARGS);
extern "C" Datum enquote_literal(PG_FUNCTION_ARGS);
extern "C" Datum simple_sql_name(PG_FUNCTION_ARGS);
extern "C" Datum enquote_name(PG_FUNCTION_ARGS);
extern "C" Datum qualified_sql_name(PG_FUNCTION_ARGS);
extern "C" Datum schema_name(PG_FUNCTION_ARGS);
extern "C" Datum sql_object_name(PG_FUNCTION_ARGS);

#endif // __GMS_ASSERT__