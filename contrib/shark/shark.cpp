#include "miscadmin.h"
#include "src/pltsql/pltsql.h"
#include "src/backend_parser/scanner.h"
#include "commands/extension.h"
#include "shark.h"

PG_MODULE_MAGIC;

static bool global_hook_inited = false;
static uint32 shark_index;

extern List* tsql_raw_parser(const char* str, List** query_string_locationlist);

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

    RepallocSessionVarsArrayIfNecessary();
    SharkContext *cxt = (SharkContext*) MemoryContextAlloc(u_sess->self_mem_cxt, sizeof(sharkContext));
    u_sess->attr.attr_common.extension_session_vars_array[shark_index] = cxt;
    cxt->dialect_sql = false;
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