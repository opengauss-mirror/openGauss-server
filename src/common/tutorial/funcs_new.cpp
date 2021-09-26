/* src/common/tutorial/funcs_new.cpp */

/******************************************************************************
  These are user-defined functions that can be bound to a openGauss backend
  and called by openGauss to execute SQL functions of the same name.

  The calling format for these functions is defined by the CREATE FUNCTION
  SQL statement that binds them to the backend.

  NOTE: this file shows examples of "new style" function call conventions.
  See funcs.c for examples of "old style".
*****************************************************************************/

#include "postgres.h"
#include "knl/knl_variable.h" /* general openGauss declarations */

#include "executor/executor.h" /* for GetAttributeByName() */
#include "utils/geo_decls.h"   /* for point type */

PG_MODULE_MAGIC;

/* These prototypes just prevent possible warnings from gcc. */

Datum add_one(PG_FUNCTION_ARGS);
Datum add_one_float8(PG_FUNCTION_ARGS);
Datum makepoint(PG_FUNCTION_ARGS);
Datum copytext(PG_FUNCTION_ARGS);
Datum concat_text(PG_FUNCTION_ARGS);
Datum c_overpaid(PG_FUNCTION_ARGS);

/* By Value */
PG_FUNCTION_INFO_V1(add_one);

Datum add_one(PG_FUNCTION_ARGS)
{
    int32 arg = PG_GETARG_INT32(0);

    PG_RETURN_INT32(arg + 1);
}

/* By Reference, Fixed Length */
PG_FUNCTION_INFO_V1(add_one_float8);

Datum add_one_float8(PG_FUNCTION_ARGS)
{
    /* The macros for FLOAT8 hide its pass-by-reference nature */
    float8 arg = PG_GETARG_FLOAT8(0);

    PG_RETURN_FLOAT8(arg + 1.0);
}

PG_FUNCTION_INFO_V1(makepoint);

Datum makepoint(PG_FUNCTION_ARGS)
{
    Point* pointx = PG_GETARG_POINT_P(0);
    Point* pointy = PG_GETARG_POINT_P(1);
    Point* new_point = (Point*)palloc(sizeof(Point));

    new_point->x = pointx->x;
    new_point->y = pointy->y;

    PG_RETURN_POINT_P(new_point);
}

/* By Reference, Variable Length */
PG_FUNCTION_INFO_V1(copytext);

Datum copytext(PG_FUNCTION_ARGS)
{
    text* t = PG_GETARG_TEXT_P(0);

    /*
     * VARSIZE is the total size of the struct in bytes.
     */
    text* new_t = (text*)palloc(VARSIZE(t));

    SET_VARSIZE(new_t, VARSIZE(t));

    /*
     * VARDATA is a pointer to the data region of the struct.
     */
    errno_t ret = memcpy_s((void*)VARDATA(new_t),
        VARSIZE(t) - VARHDRSZ   /* destination */
        (void*) VARDATA(t),     /* source */
        VARSIZE(t) - VARHDRSZ); /* how many bytes */
    securec_check(ret, "\0", "\0");
    PG_RETURN_TEXT_P(new_t);
}

PG_FUNCTION_INFO_V1(concat_text);

Datum concat_text(PG_FUNCTION_ARGS)
{
    text* arg1 = PG_GETARG_TEXT_P(0);
    text* arg2 = PG_GETARG_TEXT_P(1);
    int32 arg1_size = VARSIZE(arg1) - VARHDRSZ;
    int32 arg2_size = VARSIZE(arg2) - VARHDRSZ;
    int32 new_text_size = arg1_size + arg2_size + VARHDRSZ;
    text* new_text = (text*)palloc(new_text_size);
    errno_t ret = 0;
    SET_VARSIZE(new_text, new_text_size);
    ret = memcpy_s(VARDATA(new_text), new_text_size - VARHDRSZ, VARDATA(arg1), arg1_size);
    securec_check(ret, "\0", "\0");
    ret = memcpy_s(VARDATA(new_text) + arg1_size, arg2_size, VARDATA(arg2), arg2_size);
    securec_check(ret, "\0", "\0");
    PG_RETURN_TEXT_P(new_text);
}

/* Composite types */
PG_FUNCTION_INFO_V1(c_overpaid);

Datum c_overpaid(PG_FUNCTION_ARGS)
{
    HeapTupleHeader t = PG_GETARG_HEAPTUPLEHEADER(0);
    int32 limit = PG_GETARG_INT32(1);
    bool isnull = false;
    int32 salary;

    salary = DatumGetInt32(GetAttributeByName(t, "salary", &isnull));
    if (isnull) {
        PG_RETURN_BOOL(false);
    }

    /*
     * Alternatively, we might prefer to do PG_RETURN_NULL() for null salary
     */
    PG_RETURN_BOOL(salary > limit);
}
