#include "miscadmin.h"
#include "shark.h"
#include "commands/sequence.h"
#include "utils/builtins.h"

#define DBCC_RESULT_MAX_LENGTH 256

extern void get_last_value_and_max_value(text* txt, int64* last_value, int64* current_max_value);
extern int64 get_and_reset_last_value(text* txt, int64 new_value, bool need_reseed);

extern "C" Datum dbcc_check_ident_no_reseed(PG_FUNCTION_ARGS);
extern "C" Datum dbcc_check_ident_reseed(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(dbcc_check_ident_no_reseed);
PG_FUNCTION_INFO_V1(dbcc_check_ident_reseed);

Datum dbcc_check_ident_no_reseed(PG_FUNCTION_ARGS)
{
    int64 last_value = 0;
    int64 current_max_value = 0;
    text* txt = PG_GETARG_TEXT_P(0);
    char result[DBCC_RESULT_MAX_LENGTH] = {0};
    errno_t rc = EOK;
    bool withmsg = true;

    if (!fcinfo->argnull[1]) {
        withmsg = !PG_GETARG_BOOL(1);
    }

    get_last_value_and_max_value(txt, &last_value, &current_max_value);

    if (!withmsg) {
        PG_RETURN_NULL();
    }

    rc = snprintf_s(result, DBCC_RESULT_MAX_LENGTH, DBCC_RESULT_MAX_LENGTH - 1,
                    "Checking identity information: current identity value '%lld', current column value '%lld'.",
                    last_value, current_max_value);
    securec_check_ss(rc, "\0", "\0");

    ereport(NOTICE, (errmsg("\"%s\"", result)));

    PG_RETURN_TEXT_P(cstring_to_text(result));
}



Datum dbcc_check_ident_reseed(PG_FUNCTION_ARGS)
{
    int64 last_value = 0;
    int64 new_seed = 0;
    errno_t rc = EOK;
    char result[DBCC_RESULT_MAX_LENGTH];
    bool withmsg = true;

    if (fcinfo->argnull[0]) {
        ereport(ERROR, (errmsg("table name cannot be null.")));
    }

    text* txt = PG_GETARG_TEXT_P(0);
    bool need_reseed = !fcinfo->argnull[1];
    if (need_reseed) {
        new_seed = PG_GETARG_INT64(1);
    }

    if(!fcinfo->argnull[2]) {
        withmsg = !PG_GETARG_BOOL(2);
    }

    last_value = get_and_reset_last_value(txt, new_seed, need_reseed);

    if (!withmsg) {
        PG_RETURN_NULL();
    }

    rc = snprintf_s(result, DBCC_RESULT_MAX_LENGTH, DBCC_RESULT_MAX_LENGTH - 1,
                    "Checking identity information: current identity value '%lld'.", last_value);
    securec_check_ss(rc, "\0", "\0");

    ereport(NOTICE, (errmsg("\"%s\"", result)));

    PG_RETURN_TEXT_P(cstring_to_text(result));
}

