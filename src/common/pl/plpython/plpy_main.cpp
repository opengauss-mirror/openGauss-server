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

const int PGAUDIT_MAXLENGTH = 1024;

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

static const int plpython_python_version = PY_MAJOR_VERSION;

/* this doesn't need to be global; use PLy_current_execution_context() */
static THR_LOCAL PLyExecutionContext* PLy_execution_contexts = NULL;
THR_LOCAL plpy_t_context_struct g_plpy_t_context = {0};

pthread_mutex_t g_plyLocaleMutex = PTHREAD_MUTEX_INITIALIZER;

void PG_init(void)
{
    /* Be sure we do initialization only once (should be redundant now) */
    const int** version_ptr;

    if (g_plpy_t_context.inited) {
        return;
    }

    /* Be sure we don't run Python 2 and 3 in the same session (might crash) */
    version_ptr = (const int**)find_rendezvous_variable("plpython_python_version");
    if (!(*version_ptr)) {
        *version_ptr = &plpython_python_version;
    } else {
        if (**version_ptr != plpython_python_version) {
            ereport(FATAL,
                (errmsg("Python major version mismatch in session"),
                    errdetail("This session has previously used Python major version %d, and it is now attempting to "
                              "use Python major version %d.",
                        **version_ptr,
                        plpython_python_version),
                    errhint("Start a new session to use a different Python major version.")));
        }
    }

    pg_bindtextdomain(TEXTDOMAIN);

#if PY_MAJOR_VERSION >= 3
    PyImport_AppendInittab("plpy", PyInit_plpy);
#endif

#if PY_MAJOR_VERSION < 3
    if (!PyEval_ThreadsInitialized()) {
        PyEval_InitThreads();
    }
#endif

    Py_Initialize();
#if PY_MAJOR_VERSION >= 3
    PyImport_ImportModule("plpy");
#endif

#if PY_MAJOR_VERSION >= 3
    if (!PyEval_ThreadsInitialized()) {
        PyEval_InitThreads();
        PyEval_SaveThread();
    }
#endif

    PLy_init_interp();
    PLy_init_plpy();
    if (PyErr_Occurred()) {
        PLy_elog(FATAL, "untrapped error in initialization");
    }

    init_procedure_caches();

    g_plpy_t_context.explicit_subtransactions = NIL;

    PLy_execution_contexts = NULL;

    g_plpy_t_context.inited = true;
}

void _PG_init(void)
{
    PG_init();
}

/*
 * This should only be called once from _PG_init. Initialize the Python
 * interpreter and global data.
 */
void PLy_init_interp(void)
{
    static PyObject* PLy_interp_safe_globals = NULL;
    PyObject* mainmod = NULL;

    mainmod = PyImport_AddModule("__main__");
    if (mainmod == NULL || PyErr_Occurred()) {
        PLy_elog(ERROR, "could not import \"__main__\" module");
    }
    Py_INCREF(mainmod);
    g_plpy_t_context.PLy_interp_globals = PyModule_GetDict(mainmod);
    PLy_interp_safe_globals = PyDict_New();
    if (PLy_interp_safe_globals == NULL) {
        PLy_elog(ERROR, "could not create globals");
    }
    PyDict_SetItemString(g_plpy_t_context.PLy_interp_globals, "GD", PLy_interp_safe_globals);
    Py_DECREF(mainmod);
    if (g_plpy_t_context.PLy_interp_globals == NULL || PyErr_Occurred()) {
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

    AutoMutexLock plpythonLock(&g_plyLocaleMutex);
    if (g_plpy_t_context.Ply_LockLevel == 0) {
        plpythonLock.lock();
    }

    PyLock pyLock(&(g_plpy_t_context.Ply_LockLevel));

    PG_TRY();
    {
        PG_init();

        /* Get the new function's pg_proc entry */
        tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcoid));
        if (!HeapTupleIsValid(tuple)) {
            elog(ERROR, "cache lookup failed for function %u", funcoid);
        }
        procStruct = (Form_pg_proc)GETSTRUCT(tuple);

        is_trigger = PLy_procedure_is_trigger(procStruct);
        if (is_trigger) {
            elog(ERROR, "PL/Python does not support trigger");
        }

        ReleaseSysCache(tuple);

        /* We can't validate triggers against any particular table ... */
        PLy_procedure_get(funcoid, InvalidOid, is_trigger);
    }
    PG_CATCH();
    {
        pyLock.Reset();
        plpythonLock.unLock();
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
    AutoMutexLock plpythonLock(&g_plyLocaleMutex);

    if (g_plpy_t_context.Ply_LockLevel == 0) {
        plpythonLock.lock();
    }

    Datum retval;
    PLyExecutionContext* exec_ctx = NULL;
    ErrorContextCallback plerrcontext;

    PyLock pyLock(&(g_plpy_t_context.Ply_LockLevel));

    PG_TRY();
    {
        PG_init();

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
        pyLock.Reset();
        plpythonLock.unLock();
        PG_RE_THROW();
    }
    PG_END_TRY();

    Oid funcoid = fcinfo->flinfo->fn_oid;
    PLyProcedure* proc = NULL;

    PG_TRY();
    {
        if (CALLED_AS_TRIGGER(fcinfo)) {
            elog(ERROR, "PL/Python does not support trigger");
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
        if (AUDIT_EXEC_ENABLED) {
            AuditPlpythonFunction(funcoid, proc->proname, AUDIT_FAILED);
        }
        PLy_pop_execution_context();
        PyErr_Clear();
        pyLock.Reset();
        plpythonLock.unLock();
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
        pyLock.Reset();
        plpythonLock.unLock();
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
    AutoMutexLock plpythonLock(&g_plyLocaleMutex);
    if (g_plpy_t_context.Ply_LockLevel == 0) {
        plpythonLock.lock();
    }
    PyLock pyLock(&(g_plpy_t_context.Ply_LockLevel));

    InlineCodeBlock* codeblock = (InlineCodeBlock*)DatumGetPointer(PG_GETARG_DATUM(0));
    FunctionCallInfoData fake_fcinfo;
    FmgrInfo flinfo;
    PLyProcedure proc;
    PLyExecutionContext* exec_ctx = NULL;
    ErrorContextCallback plerrcontext;
    errno_t rc = EOK;

    PG_TRY();
    {
        PG_init();

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

        proc.pyname = PLy_strdup("__plpython_inline_block");
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
        plpythonLock.unLock();
        pyLock.Reset();
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
        plpythonLock.unLock();
        pyLock.Reset();
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
        plpythonLock.unLock();
        pyLock.Reset();
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
    if (PLy_execution_contexts == NULL) {
        elog(ERROR, "no Python function is currently executing");
    }

    return PLy_execution_contexts;
}

static PLyExecutionContext* PLy_push_execution_context(void)
{
    PLyExecutionContext* context = (PLyExecutionContext*)PLy_malloc(sizeof(PLyExecutionContext));

    context->curr_proc = NULL;
    context->scratch_ctx = AllocSetContextCreate(u_sess->top_transaction_mem_cxt,
        "PL/Python scratch context",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    context->next = PLy_execution_contexts;
    PLy_execution_contexts = context;
    return context;
}

static void PLy_pop_execution_context(void)
{
    PLyExecutionContext* context = PLy_execution_contexts;

    if (context == NULL) {
        elog(ERROR, "no Python function is currently executing");
    }

    PLy_execution_contexts = context->next;

    MemoryContextDelete(context->scratch_ctx);
    PLy_free(context);
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
