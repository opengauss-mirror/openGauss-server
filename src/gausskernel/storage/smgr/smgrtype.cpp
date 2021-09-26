/* -------------------------------------------------------------------------
 *
 * smgrtype.cpp
 *    storage manager type
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/smgr/smgrtype.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "storage/smgr/smgr.h"

typedef struct st_smgrid {
    const char *smgr_name;
} smgrid;

/*
 *  g_storage_managers[] -- List of defined storage managers.
 */
static const smgrid g_storage_managers[] = {{"magnetic disk"}};

static const int STORAGE_MANAGERS_LENGTH = lengthof(g_storage_managers);

Datum smgrin(PG_FUNCTION_ARGS)
{
    char *s = PG_GETARG_CSTRING(0);
    int16 i;

    for (i = 0; i < STORAGE_MANAGERS_LENGTH; i++) {
        if (strcmp(s, g_storage_managers[i].smgr_name) == 0) {
            PG_RETURN_INT16(i);
        }
    }
    ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("unrecognized storage manager name \"%s\"", s)));
    PG_RETURN_INT16(0);
}

Datum smgrout(PG_FUNCTION_ARGS)
{
    int16 i = PG_GETARG_INT16(0);
    char *s = NULL;

    if (i >= STORAGE_MANAGERS_LENGTH || i < 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("invalid storage manager ID: %d", i)));
    }

    s = pstrdup(g_storage_managers[i].smgr_name);
    PG_RETURN_CSTRING(s);
}

Datum smgreq(PG_FUNCTION_ARGS)
{
    int16 a = PG_GETARG_INT16(0);
    int16 b = PG_GETARG_INT16(1);

    PG_RETURN_BOOL(a == b);
}

Datum smgrne(PG_FUNCTION_ARGS)
{
    int16 a = PG_GETARG_INT16(0);
    int16 b = PG_GETARG_INT16(1);

    PG_RETURN_BOOL(a != b);
}
