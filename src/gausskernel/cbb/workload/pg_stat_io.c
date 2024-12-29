#include "postgres.h"
#include "fmgr.h"
#include <unistd.h>

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(pg_stat_disk_io);

Datum
pg_stat_disk_io(PG_FUNCTION_ARGS)
{
    int64 read_bytes = syscall(333);  // 假设系统调用 333 是获取磁盘读取
    int64 write_bytes = syscall(334); // 假设系统调用 334 是获取磁盘写入

    Datum values[2];
    values[0] = Int64GetDatum(read_bytes / 1024);  // KB
    values[1] = Int64GetDatum(write_bytes / 1024); // KB

    PG_RETURN_DATUM(values);
}
