/* -------------------------------------------------------------------------
 *
 * spiDbesql.cpp
 * 				Server Programming Interface
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * 	  src/gausskernel/runtime/executor/spiDbesql.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/hash.h"
#include "access/printtup.h"
#include "access/sysattr.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/heap.h"
#include "catalog/pg_type.h"
#include "commands/prepare.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "executor/spiDbesql.h"
#include "executor/spi_priv.h"
#include "miscadmin.h"
#include "parser/parser.h"
#include "pgxc/pgxc.h"
#include "tcop/pquery.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/dynahash.h"
#include "utils/globalplancore.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "utils/elog.h"
#include "commands/sqladvisor.h"
#include "funcapi.h"

void SetDescribeArray(ArrayType** resDescribe, int index, const char* colName, int colType,
    int colMaxLen, bool colNullOk)
{
    TupleDesc tupDesc;
    TupleDesc blessTupdesc;
    const int describeColumn = 11;
    bool typbyVal = false;
    Datum values[describeColumn];
    bool nulls[describeColumn];
    HeapTuple tuple;
    Datum resultTup;

    /* build tupdesc for result tuples. */
    tupDesc = CreateTemplateTupleDesc(describeColumn, false);
    TupleDescInitEntry(tupDesc, (AttrNumber) 1, "col_type", INT4OID, -1, 0);
    TupleDescInitEntry(tupDesc, (AttrNumber) 2, "col_max_len", INT4OID, -1, 0);
    TupleDescInitEntry(tupDesc, (AttrNumber) 3, "col_name", VARCHAROID, -1, 0);
    TupleDescInitEntry(tupDesc, (AttrNumber) 4, "col_name_len", INT4OID, -1, 0);
    TupleDescInitEntry(tupDesc, (AttrNumber) 5, "col_schema_name", VARCHAROID, -1, 0);
    TupleDescInitEntry(tupDesc, (AttrNumber) 6, "col_schema_name_len", INT4OID, -1, 0);
    TupleDescInitEntry(tupDesc, (AttrNumber) 7, "col_precision", INT4OID, -1, 0);
    TupleDescInitEntry(tupDesc, (AttrNumber) 8, "col_scale", INT4OID, -1, 0);
    TupleDescInitEntry(tupDesc, (AttrNumber) 9, "col_charsetid", INT4OID, -1, 0);
    TupleDescInitEntry(tupDesc, (AttrNumber) 10, "col_charsetform", INT4OID, -1, 0);
    TupleDescInitEntry(tupDesc, (AttrNumber) 11, "col_null_ok", BOOLOID, -1, 0);
    blessTupdesc = BlessTupleDesc(tupDesc);

    errno_t rc = 0;
    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    /* get new values */
    values[0] = Int32GetDatum(colType);
    values[1] = Int32GetDatum(colMaxLen);
    values[2] = CStringGetTextDatum(colName);
    values[3] = Int32GetDatum(strlen(colName));
    values[4] = CStringGetTextDatum("");
    values[5] = Int32GetDatum(0);
    values[6] = Int32GetDatum(0);
    values[7] = Int32GetDatum(0);
    values[8] = Int32GetDatum(0);
    values[9] = Int32GetDatum(0);
    values[10] = BoolGetDatum(!colNullOk);

    /* insert new tuple */
    tuple = heap_form_tuple(blessTupdesc, values, nulls);
    resultTup = HeapTupleGetDatum(tuple);
    /* insert array */
    *resDescribe = array_set(*resDescribe, 1, &index, resultTup, false,  -1, -1, typbyVal, 'd');
}

void GetColumnDescribe(SPIPlanPtr plan, ArrayType** resDescribe, MemoryContext memctx)
{
    /* get columns describe */
    ListCell *cell = NULL;
    ListCell *columnCell = NULL;
    List *columnList = NULL;
    int columnsNum = 0;
    int rc;

    foreach (cell, plan->stmt_list) {
        if (cell) {
            columnList = ((Query*)lfirst(cell))->targetList;
            foreach (columnCell, columnList){
                if (columnCell) {
                    columnsNum += 1;
                    /* get SPIDescColumns from column_cell */
                    SPIDescColumns *SPIDescColumn = (SPIDescColumns *)palloc(sizeof(SPIDescColumns));
                    TargetEntry *descColumns = (TargetEntry*)columnCell->data.ptr_value;
                    SPIDescColumn->resno = descColumns->resno;
                    SPIDescColumn->resorigtbl = descColumns->resorigtbl;
                    SPIDescColumn->resname = (char *)palloc(NAMEDATALEN);
                    if (!descColumns->resname) {
                        continue;
                    }
                    rc = strcpy_s(SPIDescColumn->resname, NAMEDATALEN, (char*)descColumns->resname);
                    securec_check(rc, "", "");

                    /* get tuple */
                    HeapTuple attTuple;
                    int attNum;
                    Form_pg_attribute attForm;
                    attTuple = SearchSysCache2(ATTNAME, ObjectIdGetDatum(SPIDescColumn->resorigtbl),
                                               PointerGetDatum(SPIDescColumn->resname));
                    if (!HeapTupleIsValid(attTuple)) {
                        continue;
                    }
                    attForm = ((Form_pg_attribute)GETSTRUCT(attTuple));
                    attNum = attForm->attnum;
                    if (attNum <= 0) {
                        ReleaseSysCache(attTuple);
                        ereport(ERROR, (errmodule(MOD_OPT), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                             errmsg("cannot rename system column \"%s\"", SPIDescColumn->resname),
                             errdetail("N/A"), errcause("invalid"),
                             erraction("invalid")));
                    }
                    /* set tuple result to array */
                    
                    MemoryContext oldcontext = MemoryContextSwitchTo(memctx);
                    SetDescribeArray(resDescribe, columnsNum, attForm->attname.data, attForm->atttypid,
                                     attForm->attlen, attForm->attnotnull);
                    
                    MemoryContextSwitchTo(oldcontext);
                    ReleaseSysCache(attTuple);
                }
            }
        }
    }
}
void SpiGetColumnFromPlan(const char *src, ArrayType** resDescribe, MemoryContext memctx,
    ParserSetupHook parserSetup, void *parserSetupArg)
{
    _SPI_plan plan;
    if (src == NULL) {
        SPI_result = SPI_ERROR_ARGUMENT;
        return;
    }
    SPI_result = _SPI_begin_call(true);
    if (SPI_result < 0) {
        return;
    }
    errno_t errorno = memset_s(&plan, sizeof(_SPI_plan), '\0', sizeof(_SPI_plan));
    securec_check(errorno, "\0", "\0");
    plan.magic = _SPI_PLAN_MAGIC;
    plan.nargs = 0;
    plan.argtypes = NULL;
    plan.stmt_list = NIL;
    plan.spi_key = INVALID_SPI_KEY;
    plan.id = (uint32)-1;
    plan.parserSetup = parserSetup;
    plan.parserSetupArg = parserSetupArg;
    _SPI_prepare_plan(src, &plan);
    GetColumnDescribe(&plan, resDescribe, memctx);
    _SPI_end_call(true);
}

void SpiDescribeColumnsCallback(CommandDest dest, const char *src, ArrayType** resDescribe,
    MemoryContext memctx, ParserSetupHook parserSetup, void *parserSetupArg)
{
    bool connected = false;

    PG_TRY();
    {
        if (SPI_OK_CONNECT != SPI_connect(dest, NULL, NULL)) {
            ereport(ERROR, (errmodule(MOD_OPT), errcode(ERRCODE_SPI_CONNECTION_FAILURE),
                errmsg("Unable to connect to execute internal query, current level: %d, connected level: %d",
                       u_sess->SPI_cxt._curid, u_sess->SPI_cxt._connected),
                errdetail("N/A"), errcause("invalid"), erraction("invalid")));
        }
        connected = true;
        /* Do the query. */
        SpiGetColumnFromPlan(src, resDescribe, memctx,parserSetup, parserSetupArg);
        
        connected = false;
        (void)SPI_finish();
    }
    /* Clean up in case of error. */
    PG_CATCH();
    {
        if (connected) {
            SPI_finish();
        }

        /* Carry on with error handling. */
        PG_RE_THROW();
    }
    PG_END_TRY();
}

int SPI_execute_with_args_bind(const char *src, int nargs, Oid *argtypes, Datum *Values, const char *Nulls,
    bool read_only, long tcount, Cursor_Data *cursor_data, ParserSetupHook parserSetup, void *parserSetupArg)
{
    _SPI_plan plan;

    if (src == NULL || nargs < 0 || tcount < 0) {
        return SPI_ERROR_ARGUMENT;
    }

    if (nargs > 0 && (argtypes == NULL || Values == NULL)) {
        return SPI_ERROR_PARAM;
    }

    int res = _SPI_begin_call(true);
    if (res < 0) {
        return res;
    }
    errno_t errorno = memset_s(&plan, sizeof(_SPI_plan), '\0', sizeof(_SPI_plan));
    securec_check(errorno, "\0", "\0");
    plan.magic = _SPI_PLAN_MAGIC;
    plan.cursor_options = 0;
    plan.nargs = 0;
    plan.argtypes = NULL;
    plan.parserSetup = parserSetup;
    plan.parserSetupArg = parserSetupArg;

    ParamListInfo param_list_info = _SPI_convert_params(nargs, argtypes, Values, Nulls, cursor_data);

    _SPI_prepare_oneshot_plan(src, &plan);

    res = _SPI_execute_plan(&plan, param_list_info, InvalidSnapshot, InvalidSnapshot, read_only, true, tcount);
#ifdef ENABLE_MULTIPLE_NODES
    if (checkAdivsorState() && checkSPIPlan(&plan)) {
        collectDynWithArgs(src, param_list_info, plan.cursor_options);
    }
#endif
    _SPI_end_call(true);
    return res;
}

void spi_exec_bind_with_callback(CommandDest dest, const char *src, bool read_only, long tcount, bool direct_call,
    void (*callbackFn)(void *), void *clientData, int nargs, Oid *argtypes, Datum *Values,
    ParserSetupHook parserSetup, void *parserSetupArg, const char *nulls)
{
    bool connected = false;
    int ret = 0;

    PG_TRY();
    {
        if (SPI_OK_CONNECT != SPI_connect_ext(dest, callbackFn, clientData, SPI_OPT_NONATOMIC, InvalidOid)) {
            ereport(ERROR, (errcode(ERRCODE_SPI_CONNECTION_FAILURE),
                errmsg("Unable to connect to execute internal query, current level: %d, connected level: %d",
                       u_sess->SPI_cxt._curid, u_sess->SPI_cxt._connected)));
        }
        connected = true;

        elog(DEBUG1, "Executing SQL: %s", src);

        /* Do the query. */
        ret = SPI_execute_with_args_bind(src, nargs, argtypes, Values, nulls,
                                         read_only, tcount, NULL, parserSetup, parserSetupArg);
        Assert(ret > 0);

        if (direct_call && callbackFn != NULL) {
            callbackFn(clientData);
        }

        connected = false;
        (void)SPI_finish();
    }
    /* Clean up in case of error. */
    PG_CATCH();
    {
        if (connected) {
            SPI_finish();
        }

        /* Carry on with error handling. */
        PG_RE_THROW();
    }
    PG_END_TRY();
}
