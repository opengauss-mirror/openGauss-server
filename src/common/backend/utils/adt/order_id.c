#include "postgres.h"
#include "fmgr.h"
#include "utils/date.h"
#include <time.h>
PG_FUNCTION_INFO_V1(generate_order_id);

Datum
generate_order_id(PG_FUNCTION_ARGS)
{
    char result[20];
    struct timeval tv;
    gettimeofday(&tv, NULL);
    
    snprintf(result, sizeof(result), "%ld%04d", tv.tv_sec, rand() % 10000);
    
    PG_RETURN_TEXT_P(cstring_to_text(result));
}
