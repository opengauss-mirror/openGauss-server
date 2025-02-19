#include "miscadmin.h"
#include "src/pltsql/pltsql.h"
#include "src/backend_parser/scanner.h"
#include "commands/extension.h"
#include "shark.h"

PG_MODULE_MAGIC;

static bool global_hook_inited = false;
static uint32 shark_index;

extern List* tsql_raw_parser(const char* str, List** query_string_locationlist);
extern void assign_tablecmds_hook(void);

void _PG_init(void)
{}

void init_session_vars(void)
{
    if (!DB_IS_CMPT(D_FORMAT)) {
        return;
    }
    if (!global_hook_inited) {
        g_instance.raw_parser_hook[DB_CMPT_D] = (void*)tsql_raw_parser;
        global_hook_inited = true;
    }
    u_sess->hook_cxt.coreYYlexHook = (void*)pgtsql_core_yylex;
    u_sess->hook_cxt.plsqlCompileHook = (void*)pltsql_compile;
    u_sess->hook_cxt.checkVaildUserHook = (void*)check_vaild_username;
    u_sess->hook_cxt.fetchStatusHook = (void*)fetch_cursor_end_hook;
    u_sess->hook_cxt.rowcountHook = (void*)rowcount_hook;

    RepallocSessionVarsArrayIfNecessary();
    SharkContext *cxt = (SharkContext*) MemoryContextAlloc(u_sess->self_mem_cxt, sizeof(sharkContext));
    u_sess->attr.attr_common.extension_session_vars_array[shark_index] = cxt;
    cxt->dialect_sql = false;
    cxt->rowcount = 0;
    cxt->fetch_status = FETCH_STATUS_SUCCESS;

    assign_tablecmds_hook();
}

SharkContext* GetSessionContext()
{
    if (u_sess->attr.attr_common.extension_session_vars_array[shark_index] == NULL) {
        init_session_vars();
    }
    return (SharkContext*) u_sess->attr.attr_common.extension_session_vars_array[shark_index];
}

void set_extension_index(uint32 index)
{
    shark_index = index;
}

void _PG_fini(void)
{}

void fetch_cursor_end_hook(int fetch_status)
{
    SharkContext *cxt = GetSessionContext();
    switch(fetch_status) {
        case FETCH_STATUS_SUCCESS:
        case FETCH_STATUS_FAIL:
        case FETCH_STATUS_NOT_EXIST:
        case FETCH_STATUS_NOT_FETCH:
            cxt->fetch_status = fetch_status;
            break;
        default:
            cxt->fetch_status = FETCH_STATUS_FAIL;
            break;
    }
}

void rowcount_hook(int64 rowcount)
{
    SharkContext *cxt = GetSessionContext();
    cxt->rowcount = rowcount;
}

PG_FUNCTION_INFO_V1(fetch_status);
Datum fetch_status(PG_FUNCTION_ARGS)
{
    SharkContext *cxt = GetSessionContext();
    PG_RETURN_INT32(cxt->fetch_status);
}

PG_FUNCTION_INFO_V1(rowcount);
Datum rowcount(PG_FUNCTION_ARGS)
{
    SharkContext *cxt = GetSessionContext();
    PG_RETURN_INT32(cxt->rowcount);
}

PG_FUNCTION_INFO_V1(rowcount_big);
Datum rowcount_big(PG_FUNCTION_ARGS)
{
    SharkContext *cxt = GetSessionContext();
    PG_RETURN_INT64(cxt->rowcount);
}
