#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include <regex.h>
#include <time.h>
PG_FUNCTION_INFO_V1(is_weekend);

Datum
is_weekend(PG_FUNCTION_ARGS)
{
    DateADT input_date = PG_GETARG_DATEADT(0);
    struct tm tm;
    time_t timestamp;

    timestamp = (time_t)((input_date * 86400L));
    localtime_r(&timestamp, &tm);
    
    if (tm.tm_wday == 6 || tm.tm_wday == 0) {
        PG_RETURN_BOOL(true);
    } else {
        PG_RETURN_BOOL(false);
    }
}

