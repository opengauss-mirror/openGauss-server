/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * 
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * plpy_subxactobject.cpp
 *
 * IDENTIFICATION
 *    src\common\pl\plpython\plpy_subxactobject.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/xact.h"
#include "executor/spi.h"

#include "plpython.h"

#include "plpy_subxactobject.h"

#include "plpy_elog.h"

static void PLy_subtransaction_dealloc(PyObject* subxact);
static PyObject* PLy_subtransaction_enter(PyObject* self, PyObject* unused);
static PyObject* PLy_subtransaction_exit(PyObject* self, PyObject* args);

static char PLy_subtransaction_doc[] = {"PostgreSQL subtransaction context manager"};

static PyMethodDef PLy_subtransaction_methods[] = {{"__enter__", PLy_subtransaction_enter, METH_VARARGS, NULL},
    {"__exit__", PLy_subtransaction_exit, METH_VARARGS, NULL},
    /* user-friendly names for Python <2.6 */
    {"enter", PLy_subtransaction_enter, METH_VARARGS, NULL},
    {"exit", PLy_subtransaction_exit, METH_VARARGS, NULL},
    {NULL, NULL, 0, NULL}};

static PyTypeObject PLy_SubtransactionType = {
    PyVarObject_HEAD_INIT(NULL, 0) "PLySubtransaction", /* tp_name */
    sizeof(PLySubtransactionObject),                    /* tp_size */
    0,                                                  /* tp_itemsize */

    /*
     * methods
     */
    PLy_subtransaction_dealloc,               /* tp_dealloc */
    0,                                        /* tp_print */
    0,                                        /* tp_getattr */
    0,                                        /* tp_setattr */
    0,                                        /* tp_compare */
    0,                                        /* tp_repr */
    0,                                        /* tp_as_number */
    0,                                        /* tp_as_sequence */
    0,                                        /* tp_as_mapping */
    0,                                        /* tp_hash */
    0,                                        /* tp_call */
    0,                                        /* tp_str */
    0,                                        /* tp_getattro */
    0,                                        /* tp_setattro */
    0,                                        /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /* tp_flags */
    PLy_subtransaction_doc,                   /* tp_doc */
    0,                                        /* tp_traverse */
    0,                                        /* tp_clear */
    0,                                        /* tp_richcompare */
    0,                                        /* tp_weaklistoffset */
    0,                                        /* tp_iter */
    0,                                        /* tp_iternext */
    PLy_subtransaction_methods,               /* tp_tpmethods */
};

void PLy_subtransaction_init_type(void)
{
    if (PyType_Ready(&PLy_SubtransactionType) < 0) {
        elog(ERROR, "could not initialize PLy_SubtransactionType");
    }
}

/* s = plpy.subtransaction() */
PyObject* PLy_subtransaction_new(PyObject* self, PyObject* unused)
{
    PLySubtransactionObject* ob = NULL;

    ob = PyObject_New(PLySubtransactionObject, &PLy_SubtransactionType);

    if (ob == NULL) {
        return NULL;
    }

    ob->started = false;
    ob->exited = false;

    return (PyObject*)ob;
}

/* Python requires a dealloc function to be defined */
static void PLy_subtransaction_dealloc(PyObject* subxact)
{}

/*
 * subxact.__enter__() or subxact.enter()
 *
 * Start an explicit subtransaction.  SPI calls within an explicit
 * subtransaction will not start another one, so you can atomically
 * execute many SPI calls and still get a controllable exception if
 * one of them fails.
 */
static PyObject* PLy_subtransaction_enter(PyObject* self, PyObject* unused)
{
    PLySubtransactionData* subxactdata = NULL;
    MemoryContext oldcontext;
    PLySubtransactionObject* subxact = (PLySubtransactionObject*)self;

    if (subxact->started) {
        PLy_exception_set(PyExc_ValueError, "this subtransaction has already been entered");
        return NULL;
    }

    if (subxact->exited) {
        PLy_exception_set(PyExc_ValueError, "this subtransaction has already been exited");
        return NULL;
    }

    subxact->started = true;
    oldcontext = CurrentMemoryContext;

    subxactdata = (PLySubtransactionData*)PLy_malloc(sizeof(*subxactdata));
    subxactdata->oldcontext = oldcontext;
    subxactdata->oldowner = t_thrd.utils_cxt.CurrentResourceOwner;

#ifdef ENABLE_MULTIPLE_NODES
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        pgxc_node_remote_savepoint("Savepoint s1", EXEC_ON_ALL_NODES, true, true);
    }
#endif

    BeginInternalSubTransaction(NULL);
    /* Do not want to leave the previous memory context */
    MemoryContextSwitchTo(oldcontext);

    g_plpy_t_context.explicit_subtransactions = lcons(subxactdata, g_plpy_t_context.explicit_subtransactions);

    Py_INCREF(self);
    return self;
}

/*
 * subxact.__exit__(exc_type, exc, tb) or subxact.exit(exc_type, exc, tb)
 *
 * Exit an explicit subtransaction. exc_type is an exception type, exc
 * is the exception object, tb is the traceback.  If exc_type is None,
 * commit the subtransactiony, if not abort it.
 *
 * The method signature is chosen to allow subtransaction objects to
 * be used as context managers as described in
 * <http://www.python.org/dev/peps/pep-0343/>.
 */
static PyObject* PLy_subtransaction_exit(PyObject* self, PyObject* args)
{
    PyObject* type = NULL;
    PyObject* value = NULL;
    PyObject* traceback = NULL;
    PLySubtransactionData* subxactdata = NULL;
    PLySubtransactionObject* subxact = (PLySubtransactionObject*)self;

    if (!PyArg_ParseTuple(args, "OOO", &type, &value, &traceback)) {
        return NULL;
    }

    if (!subxact->started) {
        PLy_exception_set(PyExc_ValueError, "this subtransaction has not been entered");
        return NULL;
    }

    if (subxact->exited) {
        PLy_exception_set(PyExc_ValueError, "this subtransaction has already been exited");
        return NULL;
    }

    if (g_plpy_t_context.explicit_subtransactions == NIL) {
        PLy_exception_set(PyExc_ValueError, "there is no subtransaction to exit from");
        return NULL;
    }

    subxact->exited = true;

    if (type != Py_None) {
        /* Abort the inner transaction */
        RollbackAndReleaseCurrentSubTransaction();

#ifdef ENABLE_MULTIPLE_NODES
        if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
            pgxc_node_remote_savepoint("rollback to s1", EXEC_ON_ALL_NODES, false, false);

            pgxc_node_remote_savepoint("release s1", EXEC_ON_ALL_NODES, true, false);
        }
#endif

    } else {
        ReleaseCurrentSubTransaction();

#ifdef ENABLE_MULTIPLE_NODES
        if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
            pgxc_node_remote_savepoint("release s1", EXEC_ON_ALL_NODES, true, false);
        }
#endif
    }

    subxactdata = (PLySubtransactionData*)linitial(g_plpy_t_context.explicit_subtransactions);
    g_plpy_t_context.explicit_subtransactions = list_delete_first(g_plpy_t_context.explicit_subtransactions);

    MemoryContextSwitchTo(subxactdata->oldcontext);
    t_thrd.utils_cxt.CurrentResourceOwner = subxactdata->oldowner;
    PLy_free(subxactdata);

    /*
     * AtEOSubXact_SPI() should not have popped any SPI context, but just in
     * case it did, make sure we remain connected.
     */
    SPI_restore_connection();

    Py_INCREF(Py_None);
    return Py_None;
}
