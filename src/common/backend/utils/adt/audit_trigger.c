#include "postgres.h"
#include "fmgr.h"
#include "executor/spi.h"
#include "commands/trigger.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(record_table_changes);

Datum
record_table_changes(PG_FUNCTION_ARGS)
{
    TriggerData *trigdata = (TriggerData *) fcinfo->context;
    HeapTuple rettuple = NULL;
    TupleDesc tupdesc;
    Datum values[5];
    bool nulls[5] = {false};

    if (!CALLED_AS_TRIGGER(fcinfo))
        ereport(ERROR, (errmsg("Trigger not called")));

    // 获取表信息
    tupdesc = trigdata->tg_relation->rd_att;

    // 判断操作类型
    if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
    {
        values[0] = CStringGetTextDatum(trigdata->tg_relation->rd_rel->relname.data);
        values[1] = CStringGetTextDatum("INSERT");
        values[2] = CStringGetTextDatum(GetUserNameFromId(GetUserId(), false));
        values[3] = TimestampTzGetDatum(GetCurrentTimestamp());
        values[4] = CStringGetTextDatum(TextDatumGetCString(DirectFunctionCall1(row_to_json, PointerGetDatum(trigdata->tg_trigtuple))));
    }

    SPI_connect();
    SPI_execute("INSERT INTO audit_log VALUES ($1, $2, $3, $4, $5)", true, 5, values, nulls);
    SPI_finish();

    PG_RETURN_NULL();
}
