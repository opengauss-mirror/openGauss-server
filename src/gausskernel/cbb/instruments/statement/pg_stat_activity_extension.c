#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "pgstat.h"
#include "miscadmin.h"
#include <time.h>

PG_MODULE_MAGIC;

// SQL 执行时长统计函数
PG_FUNCTION_INFO_V1(get_query_execution_time);

Datum
get_query_execution_time(PG_FUNCTION_ARGS)
{
    int pid = PG_GETARG_INT32(0);  // 获取 SQL 进程 ID
    TimestampTz start_time;
    TimestampTz now = GetCurrentTimestamp();

    // 获取当前进程的 SQL 开始时间
    start_time = pgstat_fetch_stat_beentry(pid)->query_start;

    if (start_time == 0)
        PG_RETURN_NULL();

    double exec_duration = ((double)(now - start_time)) / 1000;  // 转换为毫秒

    PG_RETURN_FLOAT8(exec_duration);
}

