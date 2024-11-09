#ifndef __GMS_INADDR__
#define __GMS_INADDR__

#include "postgres.h"

extern "C" Datum gms_inaddr_get_host_address(PG_FUNCTION_ARGS);
extern "C" Datum gms_inaddr_get_host_name(PG_FUNCTION_ARGS);

#endif // __GMS_INADDR__
