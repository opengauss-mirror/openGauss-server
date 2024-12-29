#include "postgres.h"
#include "access/hash.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(hash_insert_opt);

Datum
hash_insert_opt(PG_FUNCTION_ARGS)
{
    Relation rel = (Relation) PG_GETARG_POINTER(0);
    Datum value = PG_GETARG_DATUM(1);
    bool isnull = PG_ARGISNULL(1);

    if (isnull)
        PG_RETURN_NULL();

    // 简单的哈希函数优化
    uint32 hash_value = DatumGetUInt32(hash_any((unsigned char *) &value, sizeof(Datum)));
    elog(LOG, "Inserting into hash bucket %d", hash_value % 1024);

    PG_RETURN_DATUM(value);
}
