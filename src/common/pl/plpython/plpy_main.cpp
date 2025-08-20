/*
 * PL/Python main entry points
 *
 * src/common/pl/plpython/plpy_main.c
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/trigger.h"
#include "executor/spi.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "auditfuncs.h"
#include "pgaudit.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"

#include "plpython.h"

#include "plpy_main.h"

#include "plpy_elog.h"
#include "plpy_exec.h"
#include "plpy_plpymodule.h"
#include "plpy_procedure.h"

/* exported functions */
#if PY_MAJOR_VERSION >= 3
/* Use separate names to avoid clash in pg_pltemplate */
#define plpython_validator plpython3_validator
#define plpython_call_handler plpython3_call_handler
#define plpython_inline_handler plpython3_inline_handler
#endif

extern "C" void _PG_init(void);
extern "C" Datum plpython_validator(PG_FUNCTION_ARGS);
extern "C" Datum plpython_call_handler(PG_FUNCTION_ARGS);
extern "C" Datum plpython_inline_handler(PG_FUNCTION_ARGS);

#if PY_MAJOR_VERSION < 3
/* Define aliases plpython2_call_handler etc */
extern "C" Datum plpython2_validator(PG_FUNCTION_ARGS);
extern "C" Datum plpython2_call_handler(PG_FUNCTION_ARGS);
extern "C" Datum plpython2_inline_handler(PG_FUNCTION_ARGS);
#endif

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(plpython_validator);
PG_FUNCTION_INFO_V1(plpython_call_handler);
PG_FUNCTION_INFO_V1(plpython_inline_handler);

#if PY_MAJOR_VERSION < 3
PG_FUNCTION_INFO_V1(plpython2_validator);
PG_FUNCTION_INFO_V1(plpython2_call_handler);
PG_FUNCTION_INFO_V1(plpython2_inline_handler);
#endif

static bool PLy_procedure_is_trigger(Form_pg_proc procStruct);
static void plpython_error_callback(void* arg);
static void plpython_inline_error_callback(void* arg);
static void PLy_init_interp(void);

static PLyExecutionContext* PLy_push_execution_context(void);
static void PLy_pop_execution_context(void);
static void AuditPlpythonFunction(Oid funcoid, const char* funcname, AuditResult result);
static void InitPlySessionCtx(void);
static void release_PlySessionCtx(PlySessionCtx* ctx);
static void SetupPythonEnvs(void);
static void InitPlyGlobalsCtx(void);

PlyGlobalsCtx* g_ply_ctx;
/* this doesn't need to be global; use PLy_current_execution_context() */

/*
 * there are problems such as memory leaks, deadlocks, crashes in 'plpy',
 * so turn it off by default.
 */
const bool ENABLE_PLPY = false;
const int PYTHON_MAJOR_VERSION = 3;
const int PYTHON_MINOR_VERSION = 7;

/*
 * Perform one-time setup of PL/Python, after checking for a conflict
 * with other versions of Python.
 */
static void PLyInitialize(void)
{
    /* initialize python interpreter, only executed once in the process */
    if (!plpython_state->is_init) {
        MemoryContext old;

        plpython_state->release_PlySessionCtx_callback = release_PlySessionCtx;
        Assert(PY_MAJOR_VERSION >= PYTHON_MAJOR_VERSION);

        SetupPythonEnvs();

        InitPlyGlobalsCtx();

        Assert(g_ply_ctx);

        old = MemoryContextSwitchTo(g_ply_ctx->ply_mctx);
        PyImport_AppendInittab("plpy", PyInit_plpy);

        Py_Initialize();
        PyImport_ImportModule("plpy");
        PLy_init_interp();
        PLy_init_plpy();
        if (PyErr_Occurred())
            PLy_elog(FATAL, "untrapped error in initialization");

        MemoryContextSwitchTo(old);

        plpython_state->is_init = true;
    } else {
        g_ply_ctx = plpython_state->PlyGlobalsCtx;
    }

    /* initialize session */
    if (!u_sess->plpython_ctx) {
        pg_bindtextdomain(PG_TEXTDOMAIN("plpython"));
        InitPlySessionCtx();
        Assert(u_sess->attr.attr_common.g_PlySessionCtx);
        u_sess->plpython_ctx = u_sess->attr.attr_common.g_PlySessionCtx;
    } else {
        u_sess->attr.attr_common.g_PlySessionCtx = (PlySessionCtx*)u_sess->plpython_ctx;
    }
}

void _PG_init(void)
{
    /* Only support Python 3.7 */
    if (PY_MAJOR_VERSION != PYTHON_MAJOR_VERSION || PY_MINOR_VERSION != PYTHON_MINOR_VERSION) {
        ereport(ERROR, (errmsg("Python version is not 3.7, check if PYTHONHOME is valid.")));
    }
}

static void SetupPythonEnvs(void)
{
    /*
     * openGauss requires PYTHONHOME to be set (reason unknown, crashes otherwise)
     * Therefore during 'make install' we install the corresponding PYTHONHOME
     * under PGHOME/python directory. Set PYTHONHOME before initializing Python.
     */
    char pyhome[MAXPGPATH];
    size_t size;

    GetPythonhomePath(my_exec_path, pyhome);

    size = mbstowcs(NULL, pyhome, strlen(pyhome));
    if (size < 0) {
        PLy_elog(FATAL, "untrapped error in initialization");
    }

    wchar_t w_pythonhome[size];
    size = mbstowcs(w_pythonhome, pyhome, strlen(pyhome));
    if (size < 0) {
        PLy_elog(FATAL, "untrapped error in initialization");
    }
    w_pythonhome[size] = 0;

    Py_SetPythonHome(w_pythonhome);
}

static void InitPlyGlobalsCtx(void)
{
    /* Initialized only once and accessible by all processes, hence using a global variable is straightforward */
    static PlyGlobalsCtx plySessionCtx;
    /*
     * Reserved an extra connection, so free list is never NULL,
     * simplifies boundary condition handling
     */
    int maxConnections = g_instance.attr.attr_network.MaxConnections + 1;
    memset_s(&plySessionCtx, sizeof(plySessionCtx), 0, sizeof(plySessionCtx));

    g_ply_ctx = &plySessionCtx;
    g_ply_ctx->ply_mctx = AllocSetContextCreate(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DEFAULT),
        "plpy global context",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        SHARED_CONTEXT);
    g_ply_ctx->numberFreeContexts = maxConnections;
    g_ply_ctx->numberContexts = maxConnections;
    g_ply_ctx->sessionContexts = (PlySessionCtx*)MemoryContextAllocZero(g_ply_ctx->ply_mctx,
            maxConnections * sizeof(PlySessionCtx));

    for (int i = 0; i < maxConnections; i++) {
        PlySessionCtx* ctx = &g_ply_ctx->sessionContexts[i];
        ctx->ply_ctx_id = i;
        if (i == maxConnections - 1) {
            ctx->next_free_context = NULL; /* the last one */
        } else {
            ctx->next_free_context = &g_ply_ctx->sessionContexts[i + 1];
        }
    }

    g_ply_ctx->free_session_head = &g_ply_ctx->sessionContexts[0];
    g_ply_ctx->free_session_tail = &g_ply_ctx->sessionContexts[maxConnections - 1];

    plpython_state->PlyGlobalsCtx = g_ply_ctx;
}

static void InitPlySessionCtx()
{
    if (g_ply_ctx->numberFreeContexts <= 1) {
        PLy_elog(FATAL, "untrapped error in initialization");
    }
    Assert(g_ply_ctx->free_session_head);

    u_sess->attr.attr_common.g_PlySessionCtx = g_ply_ctx->free_session_head;
    g_ply_ctx->free_session_head = u_sess->attr.attr_common.g_PlySessionCtx->next_free_context;
    g_ply_ctx->numberFreeContexts--;
    u_sess->attr.attr_common.g_PlySessionCtx->next_free_context = NULL;
    Assert(g_ply_ctx->free_session_head);

    Assert(!u_sess->attr.attr_common.g_PlySessionCtx->session_mctx);
    char contextName[128];
    snprintf(contextName, sizeof(contextName),  "PL/Python session context %d", u_sess->attr.attr_common.g_PlySessionCtx->ply_ctx_id);
    u_sess->attr.attr_common.g_PlySessionCtx->session_mctx = AllocSetContextCreate(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DEFAULT),
                                                                contextName,
                                                                ALLOCSET_DEFAULT_MINSIZE,
                                                                ALLOCSET_DEFAULT_INITSIZE,
                                                                ALLOCSET_DEFAULT_MAXSIZE);

    snprintf(contextName, sizeof(contextName),  "PL/Python temp context %d", u_sess->attr.attr_common.g_PlySessionCtx->ply_ctx_id);
    u_sess->attr.attr_common.g_PlySessionCtx->session_tmp_mctx = AllocSetContextCreate(u_sess->attr.attr_common.g_PlySessionCtx->session_mctx,
                                                                contextName,
                                                                ALLOCSET_DEFAULT_MINSIZE,
                                                                ALLOCSET_DEFAULT_INITSIZE,
                                                                ALLOCSET_DEFAULT_MAXSIZE);

    u_sess->attr.attr_common.g_PlySessionCtx->explicit_subtransactions = NULL;
    u_sess->attr.attr_common.g_PlySessionCtx->PLy_execution_contexts = NULL;
    u_sess->attr.attr_common.g_PlySessionCtx->PLy_session_gd = PyDict_New();
    if (!u_sess->attr.attr_common.g_PlySessionCtx->PLy_session_gd)
        PLy_elog(ERROR, "could not create globals");

    Assert(!u_sess->attr.attr_common.g_PlySessionCtx->PLy_procedure_cache);
    init_procedure_caches();
}

static void release_PlySessionCtx(PlySessionCtx* ctx)
{
    if (ctx->PLy_procedure_cache) {
        HASH_SEQ_STATUS scan;
        PLyProcedureEntry* entry = NULL;
        PLyProcedure* proc = NULL;
        hash_seq_init(&scan, ctx->PLy_procedure_cache);
        while ((entry = (PLyProcedureEntry *)hash_seq_search(&scan))) {
            proc = entry->proc;
            PLy_procedure_delete(proc);
        }

        hash_destroy(ctx->PLy_procedure_cache);
        ctx->PLy_procedure_cache = NULL;
    }
    if (ctx->PLy_session_gd) {
        Py_DECREF(ctx->PLy_session_gd);
        ctx->PLy_session_gd = NULL;
    }

    if (ctx->session_mctx) {
        MemoryContextDelete(ctx->session_mctx);
        ctx->session_tmp_mctx = NULL;
        ctx->session_mctx = NULL;
    }

    ctx->explicit_subtransactions = NULL;
    ctx->PLy_execution_contexts = NULL;
    Assert(ctx->next_free_context == NULL);
    Assert(g_ply_ctx->free_session_tail);
    Assert(g_ply_ctx->numberFreeContexts > 0);
    g_ply_ctx->free_session_tail->next_free_context = ctx;
    g_ply_ctx->free_session_tail = ctx;

    g_ply_ctx->numberFreeContexts++;
}

/*
 * This should only be called once from _PG_init. Initialize the Python
 * interpreter and global data.
 */
void PLy_init_interp(void)
{
    PyObject* mainmod = NULL;

    mainmod = PyImport_AddModule("__main__");
    if (mainmod == NULL || PyErr_Occurred()) {
        PLy_elog(ERROR, "could not import \"__main__\" module");
    }
    Py_INCREF(mainmod);

    g_ply_ctx->PLy_interp_globals = PyModule_GetDict(mainmod);
    Py_DECREF(mainmod);
    if (g_ply_ctx->PLy_interp_globals == NULL || PyErr_Occurred()) {
        PLy_elog(ERROR, "could not initialize globals");
    }
}

Datum plpython_validator(PG_FUNCTION_ARGS)
{
    Oid funcoid = PG_GETARG_OID(0);
    HeapTuple tuple;
    Form_pg_proc procStruct;
    bool is_trigger = false;

    if (!CheckFunctionValidatorAccess(fcinfo->flinfo->fn_oid, funcoid)) {
        PG_RETURN_VOID();
    }

    if (!u_sess->attr.attr_sql.check_function_bodies) {
        PG_RETURN_VOID();
    }

    PG_TRY();
    {
        PlPyGilAcquire();
        PLyInitialize();

        /* Get the new function's pg_proc entry */
        tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcoid));
        if (!HeapTupleIsValid(tuple)) {
            elog(ERROR, "cache lookup failed for function %u", funcoid);
        }
        procStruct = (Form_pg_proc)GETSTRUCT(tuple);

        is_trigger = PLy_procedure_is_trigger(procStruct);

        ReleaseSysCache(tuple);

        /* We can't validate triggers against any particular table ... */
        PLy_procedure_get(funcoid, InvalidOid, is_trigger);
    }
    PG_CATCH();
    {
        PG_RE_THROW();
    }
    PG_END_TRY();

    PG_RETURN_VOID();
}

#if PY_MAJOR_VERSION < 3
Datum plpython2_validator(PG_FUNCTION_ARGS)
{
    return plpython_validator(fcinfo);
}
#endif /* PY_MAJOR_VERSION < 3 */

Datum plpython_call_handler(PG_FUNCTION_ARGS)
{
    Datum retval;
    PLyExecutionContext* exec_ctx = NULL;
    ErrorContextCallback plerrcontext;

    PG_TRY();
    {
        PlPyGilAcquire();
        PLyInitialize();

        /* Note: SPI_finish() happens in plpy_exec.cpp, which is dubious design */
        if (SPI_connect() != SPI_OK_CONNECT) {
            elog(ERROR, "SPI_connect failed");
        }

        /*
         * Push execution context onto stack.  It is important that this get
         * popped again, so avoid putting anything that could throw error between
         * here and the PG_TRY.
         */
        exec_ctx = PLy_push_execution_context();

        /*
         * Setup error traceback support for ereport()
         */
        plerrcontext.callback = plpython_error_callback;
        plerrcontext.previous = t_thrd.log_cxt.error_context_stack;
        t_thrd.log_cxt.error_context_stack = &plerrcontext;
    }
    PG_CATCH();
    {
        PG_RE_THROW();
    }
    PG_END_TRY();

    Oid funcoid = fcinfo->flinfo->fn_oid;
    PLyProcedure* proc = NULL;

    PG_TRY();
    {
        if (CALLED_AS_TRIGGER(fcinfo)) {
            Relation tgrel = ((TriggerData*)fcinfo->context)->tg_relation;
            HeapTuple trv;

            proc = PLy_procedure_get(funcoid, RelationGetRelid(tgrel), true);
            exec_ctx->curr_proc = proc;
            trv = PLy_exec_trigger(fcinfo, proc);
            retval = PointerGetDatum(trv);
        } else {
            proc = PLy_procedure_get(funcoid, InvalidOid, false);
            exec_ctx->curr_proc = proc;
            retval = PLy_exec_function(fcinfo, proc);

            if (AUDIT_EXEC_ENABLED) {
                AuditPlpythonFunction(funcoid, proc->proname, AUDIT_OK);
            }
        }
    }
    PG_CATCH();
    {
        PLy_pop_execution_context();
        PyErr_Clear();
        PG_RE_THROW();
    }
    PG_END_TRY();

    PG_TRY();
    {
        /* Pop the error context stack */
        t_thrd.log_cxt.error_context_stack = plerrcontext.previous;
        /* ... and then the execution context */
        PLy_pop_execution_context();
    }
    PG_CATCH();
    {
        PG_RE_THROW();
    }
    PG_END_TRY();

    return retval;
}

#if PY_MAJOR_VERSION < 3
Datum plpython2_call_handler(PG_FUNCTION_ARGS)
{
    return plpython_call_handler(fcinfo);
}
#endif /* PY_MAJOR_VERSION < 3 */

Datum plpython_inline_handler(PG_FUNCTION_ARGS)
{
    InlineCodeBlock* codeblock = (InlineCodeBlock*)DatumGetPointer(PG_GETARG_DATUM(0));
    FunctionCallInfoData fake_fcinfo;
    FmgrInfo flinfo;
    PLyProcedure proc;
    PLyExecutionContext* exec_ctx = NULL;
    ErrorContextCallback plerrcontext;
    errno_t rc = EOK;

    PG_TRY();
    {
        PlPyGilAcquire();
        PLyInitialize();

        /* Note: SPI_finish() happens in plpy_exec.c, which is dubious design */
        if (SPI_connect() != SPI_OK_CONNECT) {
            elog(ERROR, "SPI_connect failed");
        }

        rc = memset_s(&fake_fcinfo, sizeof(fake_fcinfo), 0, sizeof(fake_fcinfo));
        securec_check(rc, "\0", "\0");
        rc = memset_s(&flinfo, sizeof(flinfo), 0, sizeof(flinfo));
        securec_check(rc, "\0", "\0");

        fake_fcinfo.flinfo = &flinfo;
        flinfo.fn_oid = InvalidOid;
        flinfo.fn_mcxt = CurrentMemoryContext;

        rc = memset_s(&proc, sizeof(PLyProcedure), 0, sizeof(PLyProcedure));
        securec_check(rc, "\0", "\0");

        proc.mcxt = AllocSetContextCreate(u_sess->attr.attr_common.g_PlySessionCtx->session_mctx,
            "__plpython_inline_block",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE);

        proc.pyname = MemoryContextStrdup(proc.mcxt, "__plpython_inline_block");
        proc.result.out.d.typoid = VOIDOID;

        /*
         * Push execution context onto stack.  It is important that this get
         * popped again, so avoid putting anything that could throw error between
         * here and the PG_TRY.  (plpython_inline_error_callback doesn't currently
         * need the stack entry, but for consistency with plpython_call_handler we
         * do it in this order.)
         */
        exec_ctx = PLy_push_execution_context();

        /*
         * Setup error traceback support for ereport()
         */
        plerrcontext.callback = plpython_inline_error_callback;
        plerrcontext.previous = t_thrd.log_cxt.error_context_stack;
        t_thrd.log_cxt.error_context_stack = &plerrcontext;
    }
    PG_CATCH();
    {
        PG_RE_THROW();
    }
    PG_END_TRY();

    PG_TRY();
    {
        PLy_procedure_compile(&proc, codeblock->source_text);
        exec_ctx->curr_proc = &proc;
        PLy_exec_function(&fake_fcinfo, &proc);

        if (AUDIT_EXEC_ENABLED) {
            AuditPlpythonFunction(InvalidOid, proc.pyname, AUDIT_OK);
        }
    }
    PG_CATCH();
    {
        if (AUDIT_EXEC_ENABLED) {
            AuditPlpythonFunction(InvalidOid, proc.pyname, AUDIT_FAILED);
        }
        PLy_pop_execution_context();
        PLy_procedure_delete(&proc);
        PyErr_Clear();
        PG_RE_THROW();
    }
    PG_END_TRY();

    PG_TRY();
    {
        /* Pop the error context stack */
        t_thrd.log_cxt.error_context_stack = plerrcontext.previous;
        /* ... and then the execution context */
        PLy_pop_execution_context();

        /* Now clean up the transient procedure we made */
        PLy_procedure_delete(&proc);
    }
    PG_CATCH();
    {
        PG_RE_THROW();
    }
    PG_END_TRY();

    PG_RETURN_VOID();
}

#if PY_MAJOR_VERSION < 3
Datum plpython2_inline_handler(PG_FUNCTION_ARGS)
{
    return plpython_inline_handler(fcinfo);
}
#endif /* PY_MAJOR_VERSION < 3 */

static bool PLy_procedure_is_trigger(Form_pg_proc procStruct)
{
    return (procStruct->prorettype == TRIGGEROID || (procStruct->prorettype == OPAQUEOID && procStruct->pronargs == 0));
}

static void plpython_error_callback(void* arg)
{
    PLyExecutionContext* exec_ctx = PLy_current_execution_context();

    if (AUDIT_EXEC_ENABLED) {
        AuditPlpythonFunction(InvalidOid, PLy_procedure_name(exec_ctx->curr_proc), AUDIT_FAILED);
    }

    if (exec_ctx->curr_proc != NULL) {
        errcontext("PL/Python function \"%s\"", PLy_procedure_name(exec_ctx->curr_proc));
    }
}

static void plpython_inline_error_callback(void* arg)
{
    if (AUDIT_EXEC_ENABLED) {
        AuditPlpythonFunction(InvalidOid, "__plpython_inline_block", AUDIT_FAILED);
    }

    errcontext("PL/Python anonymous code block");
}

PLyExecutionContext* PLy_current_execution_context(void)
{
    if (u_sess->attr.attr_common.g_PlySessionCtx->PLy_execution_contexts == NULL) {
        elog(ERROR, "no Python function is currently executing");
    }

    return u_sess->attr.attr_common.g_PlySessionCtx->PLy_execution_contexts;
}

MemoryContext PLy_get_scratch_context(PLyExecutionContext *context)
{
    /*
     * A scratch context might never be needed in a given plpython procedure,
     * so allocate it on first request.
     */
    if (context->scratch_ctx == NULL)
        context->scratch_ctx =
            AllocSetContextCreate(u_sess->attr.attr_common.g_PlySessionCtx->session_mctx,
                                "PL/Python scratch context",
                                ALLOCSET_DEFAULT_MINSIZE,
                                ALLOCSET_DEFAULT_INITSIZE,
                                ALLOCSET_DEFAULT_MAXSIZE);
    return context->scratch_ctx;
}

static PLyExecutionContext* PLy_push_execution_context(void)
{
    PLyExecutionContext *context;

    context = (PLyExecutionContext *)MemoryContextAlloc(
        u_sess->attr.attr_common.g_PlySessionCtx->session_mctx, sizeof(PLyExecutionContext));

    context->curr_proc = NULL;
    context->scratch_ctx = NULL;
    context->next = u_sess->attr.attr_common.g_PlySessionCtx->PLy_execution_contexts;
    u_sess->attr.attr_common.g_PlySessionCtx->PLy_execution_contexts = context;
    return context;
}

static void PLy_pop_execution_context(void)
{
    PLyExecutionContext* context = u_sess->attr.attr_common.g_PlySessionCtx->PLy_execution_contexts;

    if (context == NULL) {
        elog(ERROR, "no Python function is currently executing");
    }

    u_sess->attr.attr_common.g_PlySessionCtx->PLy_execution_contexts = context->next;

    if (context->scratch_ctx) {
        MemoryContextDelete(context->scratch_ctx);
    }
}

static void AuditPlpythonFunction(Oid funcoid, const char* funcname, AuditResult result)
{
    char details[PGAUDIT_MAXLENGTH];
    errno_t rc = EOK;

    if (result == AUDIT_OK) {
        if (funcoid == InvalidOid) {
            // for AnonymousBlock
            rc = snprintf_s(details, PGAUDIT_MAXLENGTH, PGAUDIT_MAXLENGTH - 1,
                "Execute Plpython anonymous code block(oid = %u). ", funcoid);
        } else {
            // for normal function
            rc = snprintf_s(details, PGAUDIT_MAXLENGTH, PGAUDIT_MAXLENGTH - 1,
                "Execute PLpython function(oid = %u). ", funcoid);
        }
    } else {
        // for abnormal function
        rc = snprintf_s(details, PGAUDIT_MAXLENGTH, PGAUDIT_MAXLENGTH - 1,
            "Execute PLpython function(%s). ", funcname);
    }

    securec_check_ss(rc, "", "");
    audit_report(AUDIT_FUNCTION_EXEC, result, funcname, details);
}
