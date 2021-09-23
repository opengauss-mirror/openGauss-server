/*
 * transforming Datums to Python objects and vice versa
 *
 * src/common/pl/plpython/plpy_typeio.cpp
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/transam.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "parser/parse_type.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

#include "plpython.h"

#include "plpy_typeio.h"

#include "plpy_elog.h"
#include "plpy_main.h"

/* I/O function caching */
static void PLy_input_datum_func2(PLyDatumToOb* arg, Oid typeOid, HeapTuple typeTup);
static void PLy_output_datum_func2(PLyObToDatum* arg, HeapTuple typeTup);

/* conversion from Datums to Python objects */
static PyObject* PLyBool_FromBool(PLyDatumToOb* arg, Datum d);
static PyObject* PLyFloat_FromFloat4(PLyDatumToOb* arg, Datum d);
static PyObject* PLyFloat_FromFloat8(PLyDatumToOb* arg, Datum d);
static PyObject* PLyFloat_FromNumeric(PLyDatumToOb* arg, Datum d);
static PyObject* PLyInt_FromInt16(PLyDatumToOb* arg, Datum d);
static PyObject* PLyInt_FromInt32(PLyDatumToOb* arg, Datum d);
static PyObject* PLyLong_FromInt64(PLyDatumToOb* arg, Datum d);
static PyObject* PLyLong_FromOid(PLyDatumToOb* arg, Datum d);
static PyObject* PLyBytes_FromBytea(PLyDatumToOb* arg, Datum d);
static PyObject* PLyString_FromDatum(PLyDatumToOb* arg, Datum d);
static PyObject* PLyList_FromArray(PLyDatumToOb* arg, Datum d);
static PyObject* PLyList_FromArray_recurse(
    PLyDatumToOb* elm, int* dims, int ndim, int dim, char** dataptr_p, bits8** bitmap_p, int* bitmask_p);

/* conversion from Python objects to Datums */
static Datum PLyObject_ToBool(PLyObToDatum* arg, int32 typmod, PyObject* plrv);
static Datum PLyObject_ToBytea(PLyObToDatum* arg, int32 typmod, PyObject* plrv);
static Datum PLyObject_ToComposite(PLyObToDatum* arg, int32 typmod, PyObject* plrv);
static Datum PLyObject_ToDatum(PLyObToDatum* arg, int32 typmod, PyObject* plrv);
static Datum PLySequence_ToArray(PLyObToDatum* arg, int32 typmod, PyObject* plrv);

/* conversion from Python objects to composite Datums (used by triggers and SRFs) */
static Datum PLyString_ToComposite(PLyTypeInfo* info, TupleDesc desc, PyObject* string);
static Datum PLyMapping_ToComposite(PLyTypeInfo* info, TupleDesc desc, PyObject* mapping);
static Datum PLySequence_ToComposite(PLyTypeInfo* info, TupleDesc desc, PyObject* sequence);
static Datum PLyGenericObject_ToComposite(PLyTypeInfo* info, TupleDesc desc, PyObject* object);

/* make allocations in the TopMemoryContext */
static void perm_fmgr_info(Oid functionId, FmgrInfo* finfo);

void PLy_typeinfo_init(PLyTypeInfo* arg)
{
    arg->is_rowtype = -1;
    arg->in.r.natts = arg->out.r.natts = 0;
    arg->in.r.atts = NULL;
    arg->out.r.atts = NULL;
    arg->typ_relid = InvalidOid;
    arg->typrel_xmin = InvalidTransactionId;
    ItemPointerSetInvalid(&arg->typrel_tid);
}

void PLy_typeinfo_dealloc(PLyTypeInfo* arg)
{
    if (arg->is_rowtype == 1) {
        int i;

        for (i = 0; i < arg->in.r.natts; i++) {
            if (arg->in.r.atts[i].elm != NULL) {
                PLy_free(arg->in.r.atts[i].elm);
            }
        }
        if (arg->in.r.atts) {
            PLy_free(arg->in.r.atts);
        }
        for (i = 0; i < arg->out.r.natts; i++) {
            if (arg->out.r.atts[i].elm != NULL) {
                PLy_free(arg->out.r.atts[i].elm);
            }
        }
        if (arg->out.r.atts) {
            PLy_free(arg->out.r.atts);
        }
    }
}

/*
 * Conversion functions.  Remember output from Python is input to
 * openGauss, and vice versa.
 */
void PLy_input_datum_func(PLyTypeInfo* arg, Oid typeOid, HeapTuple typeTup)
{
    if (arg->is_rowtype > 0) {
        elog(ERROR, "PLyTypeInfo struct is initialized for Tuple");
    }
    arg->is_rowtype = 0;
    PLy_input_datum_func2(&(arg->in.d), typeOid, typeTup);
}

void PLy_output_datum_func(PLyTypeInfo* arg, HeapTuple typeTup)
{
    if (arg->is_rowtype > 0) {
        elog(ERROR, "PLyTypeInfo struct is initialized for a Tuple");
    }
    arg->is_rowtype = 0;
    PLy_output_datum_func2(&(arg->out.d), typeTup);
}

void PLy_input_tuple_funcs(PLyTypeInfo* arg, TupleDesc desc)
{
    int i;

    if (arg->is_rowtype == 0) {
        elog(ERROR, "PLyTypeInfo struct is initialized for a Datum");
    }
    arg->is_rowtype = 1;

    if (arg->in.r.natts != desc->natts) {
        if (arg->in.r.atts) {
            PLy_free(arg->in.r.atts);
        }
        arg->in.r.natts = desc->natts;
        arg->in.r.atts = (PLyDatumToOb*)PLy_malloc0(desc->natts * sizeof(PLyDatumToOb));
    }

    /* Can this be an unnamed tuple? If not, then an Assert would be enough */
    if (desc->tdtypmod != -1) {
        elog(ERROR, "received unnamed record type as input");
    }

    Assert(OidIsValid(desc->tdtypeid));

    /*
     * RECORDOID means we got called to create input functions for a tuple
     * fetched by plpy.execute or for an anonymous record type
     */
    if (desc->tdtypeid != RECORDOID) {
        HeapTuple relTup;

        /* Get the pg_class tuple corresponding to the type of the input */
        arg->typ_relid = typeidTypeRelid(desc->tdtypeid);
        relTup = SearchSysCache1(RELOID, ObjectIdGetDatum(arg->typ_relid));
        if (!HeapTupleIsValid(relTup)) {
            elog(ERROR, "cache lookup failed for relation %u", arg->typ_relid);
        }

        /* Remember XMIN and TID for later validation if cache is still OK */
        arg->typrel_xmin = HeapTupleGetRawXmin(relTup);
        arg->typrel_tid = relTup->t_self;

        ReleaseSysCache(relTup);
    }

    for (i = 0; i < desc->natts; i++) {
        HeapTuple typeTup;

        if (desc->attrs[i]->attisdropped) {
            continue;
        }

        if (arg->in.r.atts[i].typoid == desc->attrs[i]->atttypid) {
            continue; /* already set up this entry */
        }

        typeTup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(desc->attrs[i]->atttypid));
        if (!HeapTupleIsValid(typeTup)) {
            elog(ERROR, "cache lookup failed for type %u", desc->attrs[i]->atttypid);
        }

        PLy_input_datum_func2(&(arg->in.r.atts[i]), desc->attrs[i]->atttypid, typeTup);

        ReleaseSysCache(typeTup);
    }
}

void PLy_output_tuple_funcs(PLyTypeInfo* arg, TupleDesc desc)
{
    int i;

    if (arg->is_rowtype == 0) {
        elog(ERROR, "PLyTypeInfo struct is initialized for a Datum");
    }
    arg->is_rowtype = 1;

    if (arg->out.r.natts != desc->natts) {
        if (arg->out.r.atts) {
            PLy_free(arg->out.r.atts);
        }
        arg->out.r.natts = desc->natts;
        arg->out.r.atts = (PLyObToDatum*)PLy_malloc0(desc->natts * sizeof(PLyObToDatum));
    }

    Assert(OidIsValid(desc->tdtypeid));

    /*
     * RECORDOID means we got called to create output functions for an
     * anonymous record type
     */
    if (desc->tdtypeid != RECORDOID) {
        HeapTuple relTup;

        /* Get the pg_class tuple corresponding to the type of the output */
        arg->typ_relid = typeidTypeRelid(desc->tdtypeid);
        relTup = SearchSysCache1(RELOID, ObjectIdGetDatum(arg->typ_relid));
        if (!HeapTupleIsValid(relTup)) {
            elog(ERROR, "cache lookup failed for relation %u", arg->typ_relid);
        }

        /* Remember XMIN and TID for later validation if cache is still OK */
        arg->typrel_xmin = HeapTupleGetRawXmin(relTup);
        arg->typrel_tid = relTup->t_self;

        ReleaseSysCache(relTup);
    }

    for (i = 0; i < desc->natts; i++) {
        HeapTuple typeTup;

        if (desc->attrs[i]->attisdropped) {
            continue;
        }

        if (arg->out.r.atts[i].typoid == desc->attrs[i]->atttypid) {
            continue; /* already set up this entry */
        }

        typeTup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(desc->attrs[i]->atttypid));
        if (!HeapTupleIsValid(typeTup)) {
            elog(ERROR, "cache lookup failed for type %u", desc->attrs[i]->atttypid);
        }

        PLy_output_datum_func2(&(arg->out.r.atts[i]), typeTup);

        ReleaseSysCache(typeTup);
    }
}

void PLy_output_record_funcs(PLyTypeInfo* arg, TupleDesc desc)
{
    /*
     * If the output record functions are already set, we just have to check
     * if the record descriptor has not changed
     */
    if ((arg->is_rowtype == 1) && (arg->out.d.typmod != -1) && (arg->out.d.typmod == desc->tdtypmod)) {
        return;
    }

    /* bless the record to make it known to the typcache lookup code */
    BlessTupleDesc(desc);
    /* save the freshly generated typmod */
    arg->out.d.typmod = desc->tdtypmod;
    /* proceed with normal I/O function caching */
    PLy_output_tuple_funcs(arg, desc);

    /*
     * it should change is_rowtype to 1, so we won't go through this again
     * unless the output record description changes
     */
    Assert(arg->is_rowtype == 1);
}

/*
 * Transform a tuple into a Python dict object.
 */
PyObject *PLyDict_FromTuple(PLyTypeInfo *info, HeapTuple tuple, TupleDesc desc, bool include_generated)
{
    PyObject* volatile dict = NULL;
    PLyExecutionContext* exec_ctx = PLy_current_execution_context();
    MemoryContext oldcontext = CurrentMemoryContext;
    int i;

    if (info->is_rowtype != 1) {
        elog(ERROR, "PLyTypeInfo structure describes a datum");
    }

    dict = PyDict_New();
    if (dict == NULL) {
        PLy_elog(ERROR, "could not create new dictionary");
    }

    PG_TRY();
    {
        /*
         * Do the work in the scratch context to avoid leaking memory from the
         * datatype output function calls.
         */
        MemoryContextSwitchTo(exec_ctx->scratch_ctx);
        for (i = 0; i < info->in.r.natts; i++) {
            char* key = NULL;
            Datum vattr;
            bool is_null = false;
            PyObject* value = NULL;

            if (desc->attrs[i]->attisdropped) {
                continue;
            }

            if (ISGENERATEDCOL(desc, i)) {
                /* don't include unless requested */
                if (!include_generated)
                    continue;
            }

            key = NameStr(desc->attrs[i]->attname);
            vattr = heap_getattr(tuple, (i + 1), desc, &is_null);

            if (is_null || info->in.r.atts[i].func == NULL) {
                PyDict_SetItemString(dict, key, Py_None);
            } else {
                value = (info->in.r.atts[i].func)(&info->in.r.atts[i], vattr);
                PyDict_SetItemString(dict, key, value);
                Py_DECREF(value);
            }
        }
        MemoryContextSwitchTo(oldcontext);
        MemoryContextReset(exec_ctx->scratch_ctx);
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(oldcontext);
        Py_DECREF(dict);
        PG_RE_THROW();
    }
    PG_END_TRY();

    return dict;
}

/*
 *	Convert a Python object to a composite Datum, using all supported
 *	conversion methods: composite as a string, as a sequence, as a mapping or
 *	as an object that has __getattr__ support.
 */
Datum PLyObject_ToCompositeDatum(PLyTypeInfo* info, TupleDesc desc, PyObject* plrv)
{
    Datum datum;

    if (PyString_Check(plrv) || PyUnicode_Check(plrv)) {
        datum = PLyString_ToComposite(info, desc, plrv);
    } else if (PySequence_Check(plrv)) {
        /* composite type as sequence (tuple, list etc) */
        datum = PLySequence_ToComposite(info, desc, plrv);
    } else if (PyMapping_Check(plrv)) {
        /* composite type as mapping (currently only dict) */
        datum = PLyMapping_ToComposite(info, desc, plrv);
    } else {
        /* returned as smth, must provide method __getattr__(name) */
        datum = PLyGenericObject_ToComposite(info, desc, plrv);
    }

    return datum;
}

static void PLy_output_datum_func2(PLyObToDatum* arg, HeapTuple typeTup)
{
    Form_pg_type typeStruct = (Form_pg_type)GETSTRUCT(typeTup);
    Oid element_type;

    perm_fmgr_info(typeStruct->typinput, &arg->typfunc);
    arg->typoid = HeapTupleGetOid(typeTup);
    arg->typmod = -1;
    arg->typioparam = getTypeIOParam(typeTup);
    arg->typbyval = typeStruct->typbyval;

    element_type = get_element_type(arg->typoid);

    /*
     * Select a conversion function to convert Python objects to openGauss
     * datums.	Most data types can go through the generic function.
     */
    switch (getBaseType(element_type ? element_type : arg->typoid)) {
        case BOOLOID:
            arg->func = PLyObject_ToBool;
            break;
        case BYTEAOID:
            arg->func = PLyObject_ToBytea;
            break;
        default:
            arg->func = PLyObject_ToDatum;
            break;
    }

    /* Composite types need their own input routine, though */
    if (typeStruct->typtype == TYPTYPE_COMPOSITE) {
        arg->func = PLyObject_ToComposite;
    }

    if (element_type) {
        char dummy_delim;
        Oid funcid;

        if (type_is_rowtype(element_type)) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("PL/Python functions cannot return type %s", format_type_be(arg->typoid)),
                    errdetail("PL/Python does not support conversion to arrays of row types.")));
        }

        arg->elm = (PLyObToDatum*)PLy_malloc0(sizeof(*arg->elm));
        arg->elm->func = arg->func;
        arg->func = PLySequence_ToArray;

        arg->elm->typoid = element_type;
        arg->elm->typmod = -1;
        get_type_io_data(element_type,
            IOFunc_input,
            &arg->elm->typlen,
            &arg->elm->typbyval,
            &arg->elm->typalign,
            &dummy_delim,
            &arg->elm->typioparam,
            &funcid);
        perm_fmgr_info(funcid, &arg->elm->typfunc);
    }
}

static void PLy_input_datum_func2(PLyDatumToOb* arg, Oid typeOid, HeapTuple typeTup)
{
    Form_pg_type typeStruct = (Form_pg_type)GETSTRUCT(typeTup);
    Oid element_type = get_element_type(typeOid);

    /* Get the type's conversion information */
    perm_fmgr_info(typeStruct->typoutput, &arg->typfunc);
    arg->typoid = HeapTupleGetOid(typeTup);
    arg->typmod = -1;
    arg->typioparam = getTypeIOParam(typeTup);
    arg->typbyval = typeStruct->typbyval;
    arg->typlen = typeStruct->typlen;
    arg->typalign = typeStruct->typalign;

    /* Determine which kind of Python object we will convert to */
    switch (getBaseType(element_type ? element_type : typeOid)) {
        case BOOLOID:
            arg->func = PLyBool_FromBool;
            break;
        case FLOAT4OID:
            arg->func = PLyFloat_FromFloat4;
            break;
        case FLOAT8OID:
            arg->func = PLyFloat_FromFloat8;
            break;
        case NUMERICOID:
            arg->func = PLyFloat_FromNumeric;
            break;
        case INT2OID:
            arg->func = PLyInt_FromInt16;
            break;
        case INT4OID:
            arg->func = PLyInt_FromInt32;
            break;
        case INT8OID:
            arg->func = PLyLong_FromInt64;
            break;
        case OIDOID:
            arg->func = PLyLong_FromOid;
            break;
        case BYTEAOID:
            arg->func = PLyBytes_FromBytea;
            break;
        default:
            arg->func = PLyString_FromDatum;
            break;
    }

    if (element_type) {
        char dummy_delim;
        Oid funcid;

        arg->elm = (PLyDatumToOb*)PLy_malloc0(sizeof(*arg->elm));
        arg->elm->func = arg->func;
        arg->func = PLyList_FromArray;
        arg->elm->typoid = element_type;
        arg->elm->typmod = -1;
        get_type_io_data(element_type,
            IOFunc_output,
            &arg->elm->typlen,
            &arg->elm->typbyval,
            &arg->elm->typalign,
            &dummy_delim,
            &arg->elm->typioparam,
            &funcid);
        perm_fmgr_info(funcid, &arg->elm->typfunc);
    }
}

static PyObject* PLyBool_FromBool(PLyDatumToOb* arg, Datum d)
{
    /*
     * We would like to use Py_RETURN_TRUE and Py_RETURN_FALSE here for
     * generating SQL from trigger functions, but those are only supported in
     * Python >= 2.3, and we support older versions.
     * http://docs.python.org/api/boolObjects.html
     */
    if (DatumGetBool(d)) {
        return PyBool_FromLong(1);
    }
    return PyBool_FromLong(0);
}

static PyObject* PLyFloat_FromFloat4(PLyDatumToOb* arg, Datum d)
{
    return PyFloat_FromDouble(DatumGetFloat4(d));
}

static PyObject* PLyFloat_FromFloat8(PLyDatumToOb* arg, Datum d)
{
    return PyFloat_FromDouble(DatumGetFloat8(d));
}

static PyObject* PLyFloat_FromNumeric(PLyDatumToOb* arg, Datum d)
{
    /*
     * Numeric is cast to a PyFloat: This results in a loss of precision Would
     * it be better to cast to PyString?
     */
    Datum f = DirectFunctionCall1(numeric_float8, d);
    double x = DatumGetFloat8(f);

    return PyFloat_FromDouble(x);
}

static PyObject* PLyInt_FromInt16(PLyDatumToOb* arg, Datum d)
{
    return PyInt_FromLong(DatumGetInt16(d));
}

static PyObject* PLyInt_FromInt32(PLyDatumToOb* arg, Datum d)
{
    return PyInt_FromLong(DatumGetInt32(d));
}

static PyObject* PLyLong_FromInt64(PLyDatumToOb* arg, Datum d)
{
    /* on 32 bit platforms "long" may be too small */
    if (sizeof(int64) > sizeof(long)) {
        return PyLong_FromLongLong(DatumGetInt64(d));
    } else {
        return PyLong_FromLong(DatumGetInt64(d));
    }
}

static PyObject* PLyLong_FromOid(PLyDatumToOb* arg, Datum d)
{
    return PyLong_FromUnsignedLong(DatumGetObjectId(d));
}

static PyObject* PLyBytes_FromBytea(PLyDatumToOb* arg, Datum d)
{
    text* txt = DatumGetByteaP(d);
    char* str = VARDATA(txt);
    size_t size = VARSIZE(txt) - VARHDRSZ;

    return PyBytes_FromStringAndSize(str, size);
}

static PyObject* PLyString_FromDatum(PLyDatumToOb* arg, Datum d)
{
    char* x = OutputFunctionCall(&arg->typfunc, d);
    PyObject* r = PyString_FromString(x);

    pfree(x);
    return r;
}

static PyObject* PLyList_FromArray(PLyDatumToOb* arg, Datum d)
{
    ArrayType* array = DatumGetArrayTypeP(d);
    PLyDatumToOb* elm = arg->elm;
    int ndim = 0;
    int* dims = NULL;
    char* dataptr = NULL;
    bits8* bitmap = NULL;
    int bitmask = 0;

    if (ARR_NDIM(array) == 0) {
        return PyList_New(0);
    }

    /* Array dimensions and left bounds */
    ndim = ARR_NDIM(array);
    dims = ARR_DIMS(array);
    Assert(ndim < MAXDIM);

    /*
     * We iterate the SQL array in the physical order it's stored in the
     * datum. For example, for a 3-dimensional array the order of iteration
     * would be the following: [0,0,0] elements through [0,0,k], then [0,1,0]
     * through [0,1,k] till [0,m,k], then [1,0,0] through [1,0,k] till
     * [1,m,k], and so on.
     *
     * In Python, there are no multi-dimensional lists as such, but they are
     * represented as a list of lists. So a 3-d array of [n,m,k] elements is a
     * list of n m-element arrays, each element of which is k-element array.
     * PLyList_FromArray_recurse() builds the Python list for a single
     * dimension, and recurses for the next inner dimension.
     */
    dataptr = ARR_DATA_PTR(array);
    bitmap = ARR_NULLBITMAP(array);
    bitmask = 1;

    return PLyList_FromArray_recurse(elm, dims, ndim, 0, &dataptr, &bitmap, &bitmask);
}

static PyObject* PLyList_FromArray_recurse(
    PLyDatumToOb* elm, int* dims, int ndim, int dim, char** dataptr_p, bits8** bitmap_p, int* bitmask_p)
{
    int i;
    PyObject* list;

    list = PyList_New(dims[dim]);
    if (!list) {
        return NULL;
    }

    if (dim < ndim - 1) {
        /* Outer dimension. Recurse for each inner slice. */
        for (i = 0; i < dims[dim]; i++) {
            PyObject* sublist;

            sublist = PLyList_FromArray_recurse(elm, dims, ndim, dim + 1, dataptr_p, bitmap_p, bitmask_p);
            PyList_SET_ITEM(list, i, sublist);
        }
    } else {
        /*
         * Innermost dimension. Fill the list with the values from the array
         * for this slice.
         */
        char* dataptr = *dataptr_p;
        bits8* bitmap = *bitmap_p;
        int bitmask = *bitmask_p;

        for (i = 0; i < dims[dim]; i++) {
            /* checking for NULL */
            if (bitmap && (*bitmap & bitmask) == 0) {
                Py_INCREF(Py_None);
                PyList_SET_ITEM(list, i, Py_None);
            } else {
                Datum itemvalue;

                itemvalue = fetch_att(dataptr, elm->typbyval, elm->typlen);
                PyList_SET_ITEM(list, i, elm->func(elm, itemvalue));
                dataptr = att_addlength_pointer(dataptr, elm->typlen, dataptr);
                dataptr = (char*)att_align_nominal(dataptr, elm->typalign);
            }

            /* advance bitmap pointer if any */
            if (bitmap) {
                bitmask <<= 1;
                /* (1<<8) */
                if (bitmask == 0x100) {
                    bitmap++;
                    bitmask = 1;
                }
            }
        }

        *dataptr_p = dataptr;
        *bitmap_p = bitmap;
        *bitmask_p = bitmask;
    }

    return list;
}

/*
 * Convert a Python object to a openGauss bool datum.	This can't go
 * through the generic conversion function, because Python attaches a
 * Boolean value to everything, more things than the openGauss bool
 * type can parse.
 */
static Datum PLyObject_ToBool(PLyObToDatum* arg, int32 typmod, PyObject* plrv)
{
    Datum rv;

    Assert(plrv != Py_None);
    rv = BoolGetDatum(PyObject_IsTrue(plrv));

    if (get_typtype(arg->typoid) == TYPTYPE_DOMAIN) {
        domain_check(rv, false, arg->typoid, &arg->typfunc.fn_extra, arg->typfunc.fn_mcxt);
    }

    return rv;
}

/*
 * Convert a Python object to a openGauss bytea datum.  This doesn't
 * go through the generic conversion function to circumvent problems
 * with embedded nulls.  And it's faster this way.
 */
static Datum PLyObject_ToBytea(PLyObToDatum* arg, int32 typmod, PyObject* plrv)
{
    PyObject* volatile plrv_so = NULL;
    Datum rv;

    Assert(plrv != Py_None);

    plrv_so = PyObject_Bytes(plrv);
    if (plrv_so == NULL) {
        PLy_elog(ERROR, "could not create bytes representation of Python object");
    }

    PG_TRY();
    {
        char* plrv_sc = PyBytes_AsString(plrv_so);
        size_t len = PyBytes_Size(plrv_so);
        size_t size = len + VARHDRSZ;
        bytea* result = (bytea*)palloc(size);

        SET_VARSIZE(result, size);
        errno_t rc;
        rc = memcpy_s(VARDATA(result), size, plrv_sc, len);
        securec_check(rc, "\0", "\0");
        rv = PointerGetDatum(result);
    }
    PG_CATCH();
    {
        Py_XDECREF(plrv_so);
        PG_RE_THROW();
    }
    PG_END_TRY();

    Py_XDECREF(plrv_so);

    if (get_typtype(arg->typoid) == TYPTYPE_DOMAIN) {
        domain_check(rv, false, arg->typoid, &arg->typfunc.fn_extra, arg->typfunc.fn_mcxt);
    }

    return rv;
}

/*
 * Convert a Python object to a composite type. First look up the type's
 * description, then route the Python object through the conversion function
 * for obtaining openGauss tuples.
 */
static Datum PLyObject_ToComposite(PLyObToDatum* arg, int32 typmod, PyObject* plrv)
{
    Datum rv;
    PLyTypeInfo info;
    TupleDesc desc;

    if (typmod != -1) {
        elog(ERROR, "received unnamed record type as input");
    }

    /* Create a dummy PLyTypeInfo */
    errno_t rc = memset_s(&info, sizeof(PLyTypeInfo), 0, sizeof(PLyTypeInfo));
    securec_check(rc, "\0", "\0");

    PLy_typeinfo_init(&info);
    /* Mark it as needing output routines lookup */
    info.is_rowtype = 2;

    desc = lookup_rowtype_tupdesc(arg->typoid, arg->typmod);

    /*
     * This will set up the dummy PLyTypeInfo's output conversion routines,
     * since we left is_rowtype as 2. A future optimisation could be caching
     * that info instead of looking it up every time a tuple is returned from
     * the function.
     */
    rv = PLyObject_ToCompositeDatum(&info, desc, plrv);

    PLy_typeinfo_dealloc(&info);

    return rv;
}

/*
 * Generic conversion function: Convert PyObject to cstring and
 * cstring into openGauss type.
 */
static Datum PLyObject_ToDatum(PLyObToDatum* arg, int32 typmod, PyObject* plrv)
{
    PyObject* volatile plrv_bo = NULL;
    Datum rv;

    Assert(plrv != Py_None);

    if (PyUnicode_Check(plrv)) {
        plrv_bo = PLyUnicode_Bytes(plrv);
    } else {
#if PY_MAJOR_VERSION >= 3
        PyObject* s = PyObject_Str(plrv);

        plrv_bo = PLyUnicode_Bytes(s);
        Py_XDECREF(s);
#else
        plrv_bo = PyObject_Str(plrv);
#endif
    }
    if (plrv_bo == NULL) {
        PLy_elog(ERROR, "could not create string representation of Python object");
    }

    PG_TRY();
    {
        char* plrv_sc = PyBytes_AsString(plrv_bo);
        size_t plen = PyBytes_Size(plrv_bo);
        size_t slen = strlen(plrv_sc);
        if (slen < plen) {
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("could not convert Python object into cstring: Python string representation appears to "
                           "contain null bytes")));
        } else if (slen > plen) {
            elog(ERROR, "could not convert Python object into cstring: Python string longer than reported length");
        }
        pg_verifymbstr(plrv_sc, slen, false);
        rv = InputFunctionCall(&arg->typfunc, plrv_sc, arg->typioparam, typmod);
    }
    PG_CATCH();
    {
        Py_XDECREF(plrv_bo);
        PG_RE_THROW();
    }
    PG_END_TRY();

    Py_XDECREF(plrv_bo);

    return rv;
}

static Datum PLySequence_ToArray(PLyObToDatum* arg, int32 typmod, PyObject* plrv)
{
    ArrayType* array = NULL;
    Datum rv;
    int i;
    Datum* elems = NULL;
    bool* nulls = NULL;
    int len;
    int lbs;

    Assert(plrv != Py_None);

    if (!PySequence_Check(plrv)) {
        PLy_elog(ERROR, "return value of function with array return type is not a Python sequence");
    }

    len = PySequence_Length(plrv);
    elems = (Datum*)palloc(sizeof(*elems) * len);
    nulls = (bool*)palloc(sizeof(*nulls) * len);

    for (i = 0; i < len; i++) {
        PyObject* obj = PySequence_GetItem(plrv, i);

        if (obj == Py_None) {
            nulls[i] = true;
        } else {
            nulls[i] = false;

            /*
             * We don't support arrays of row types yet, so the first argument
             * can be NULL.
             */
            elems[i] = arg->elm->func(arg->elm, -1, obj);
        }
        Py_XDECREF(obj);
    }

    lbs = 1;
    array = construct_md_array(elems,
        nulls,
        1,
        &len,
        &lbs,
        get_element_type(arg->typoid),
        arg->elm->typlen,
        arg->elm->typbyval,
        arg->elm->typalign);
    /*
     * If the result type is a domain of array, the resulting array must be
     * checked.
     */
    rv = PointerGetDatum(array);
    if (get_typtype(arg->typoid) == TYPTYPE_DOMAIN) {
        domain_check(rv, false, arg->typoid, &arg->typfunc.fn_extra, arg->typfunc.fn_mcxt);
    }
    return rv;
}

static Datum PLyString_ToComposite(PLyTypeInfo* info, TupleDesc desc, PyObject* string)
{
    HeapTuple typeTup;

    typeTup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(desc->tdtypeid));
    if (!HeapTupleIsValid(typeTup)) {
        elog(ERROR, "cache lookup failed for type %u", desc->tdtypeid);
    }

    PLy_output_datum_func2(&info->out.d, typeTup);

    ReleaseSysCache(typeTup);
    ReleaseTupleDesc(desc);

    return PLyObject_ToDatum(&info->out.d, info->out.d.typmod, string);
}

static Datum PLyMapping_ToComposite(PLyTypeInfo* info, TupleDesc desc, PyObject* mapping)
{
    HeapTuple tuple;
    Datum* values = NULL;
    bool* nulls = NULL;
    volatile int i;

    Assert(PyMapping_Check(mapping));

    if (info->is_rowtype == 2) {
        PLy_output_tuple_funcs(info, desc);
    }
    Assert(info->is_rowtype == 1);

    /* Build tuple */
    values = (Datum*)palloc(sizeof(Datum) * desc->natts);
    nulls = (bool*)palloc(sizeof(bool) * desc->natts);
    for (i = 0; i < desc->natts; ++i) {
        char* key = NULL;
        PyObject* volatile value = NULL;
        PLyObToDatum* att = NULL;

        if (desc->attrs[i]->attisdropped) {
            values[i] = (Datum)0;
            nulls[i] = true;
            continue;
        }

        key = NameStr(desc->attrs[i]->attname);
        value = NULL;
        att = &info->out.r.atts[i];
        PG_TRY();
        {
            value = PyMapping_GetItemString(mapping, key);
            if (value == Py_None) {
                values[i] = (Datum)NULL;
                nulls[i] = true;
            } else if (value != NULL) {
                values[i] = (att->func)(att, -1, value);
                nulls[i] = false;
            } else
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_COLUMN),
                        errmsg("key \"%s\" not found in mapping", key),
                        errhint("To return null in a column, "
                                "add the value None to the mapping with the key named after the column.")));

            Py_XDECREF(value);
            value = NULL;
        }
        PG_CATCH();
        {
            Py_XDECREF(value);
            PG_RE_THROW();
        }
        PG_END_TRY();
    }

    tuple = heap_form_tuple(desc, values, nulls);
    ReleaseTupleDesc(desc);
    pfree(values);
    pfree(nulls);

    return HeapTupleGetDatum(tuple);
}

static Datum PLySequence_ToComposite(PLyTypeInfo* info, TupleDesc desc, PyObject* sequence)
{
    HeapTuple tuple;
    Datum* values = NULL;
    bool* nulls = NULL;
    volatile int idx;
    volatile int i;

    Assert(PySequence_Check(sequence));

    /*
     * Check that sequence length is exactly same as PG tuple's. We actually
     * can ignore exceeding items or assume missing ones as null but to avoid
     * plpython developer's errors we are strict here
     */
    idx = 0;
    for (i = 0; i < desc->natts; i++) {
        if (!desc->attrs[i]->attisdropped) {
            idx++;
        }
    }
    if (PySequence_Length(sequence) != idx) {
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("length of returned sequence did not match number of columns in row")));
    }

    if (info->is_rowtype == 2) {
        PLy_output_tuple_funcs(info, desc);
    }
    Assert(info->is_rowtype == 1);

    /* Build tuple */
    values = (Datum*)palloc(sizeof(Datum) * desc->natts);
    nulls = (bool*)palloc(sizeof(bool) * desc->natts);
    idx = 0;
    for (i = 0; i < desc->natts; ++i) {
        PyObject* volatile value = NULL;
        PLyObToDatum* att = NULL;

        if (desc->attrs[i]->attisdropped) {
            values[i] = (Datum)0;
            nulls[i] = true;
            continue;
        }

        value = NULL;
        att = &info->out.r.atts[i];
        PG_TRY();
        {
            value = PySequence_GetItem(sequence, idx);
            Assert(value);
            if (value == Py_None) {
                values[i] = (Datum)NULL;
                nulls[i] = true;
            } else if (value != NULL) {
                values[i] = (att->func)(att, -1, value);
                nulls[i] = false;
            }

            Py_XDECREF(value);
            value = NULL;
        }
        PG_CATCH();
        {
            Py_XDECREF(value);
            PG_RE_THROW();
        }
        PG_END_TRY();

        idx++;
    }

    tuple = heap_form_tuple(desc, values, nulls);
    ReleaseTupleDesc(desc);
    pfree(values);
    pfree(nulls);

    return HeapTupleGetDatum(tuple);
}

static Datum PLyGenericObject_ToComposite(PLyTypeInfo* info, TupleDesc desc, PyObject* object)
{
    HeapTuple tuple;
    Datum* values = NULL;
    bool* nulls = NULL;
    volatile int i;

    if (info->is_rowtype == 2) {
        PLy_output_tuple_funcs(info, desc);
    }
    Assert(info->is_rowtype == 1);

    /* Build tuple */
    values = (Datum*)palloc(sizeof(Datum) * desc->natts);
    nulls = (bool*)palloc(sizeof(bool) * desc->natts);
    for (i = 0; i < desc->natts; ++i) {
        char* key = NULL;
        PyObject* volatile value = NULL;
        PLyObToDatum* att = NULL;

        if (desc->attrs[i]->attisdropped) {
            values[i] = (Datum)0;
            nulls[i] = true;
            continue;
        }

        key = NameStr(desc->attrs[i]->attname);
        value = NULL;
        att = &info->out.r.atts[i];
        PG_TRY();
        {
            value = PyObject_GetAttrString(object, key);
            if (value == Py_None) {
                values[i] = (Datum)NULL;
                nulls[i] = true;
            } else if (value != NULL) {
                values[i] = (att->func)(att, -1, value);
                nulls[i] = false;
            } else
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_COLUMN),
                        errmsg("attribute \"%s\" does not exist in Python object", key),
                        errhint("To return null in a column, "
                                "let the returned object have an attribute named "
                                "after column with value None.")));

            Py_XDECREF(value);
            value = NULL;
        }
        PG_CATCH();
        {
            Py_XDECREF(value);
            PG_RE_THROW();
        }
        PG_END_TRY();
    }

    tuple = heap_form_tuple(desc, values, nulls);
    ReleaseTupleDesc(desc);
    pfree(values);
    pfree(nulls);

    return HeapTupleGetDatum(tuple);
}

/*
 * This routine is a crock, and so is everyplace that calls it.  The problem
 * is that the cached form of plpython functions/queries is allocated permanently
 * (mostly via malloc()) and never released until backend exit.  Subsidiary
 * data structures such as fmgr info records therefore must live forever
 * as well.  A better implementation would store all this stuff in a per-
 * function memory context that could be reclaimed at need.  In the meantime,
 * fmgr_info_cxt must be called specifying
 * THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB) so that whatever
 * it might allocate, and whatever the eventual function might allocate using
 * fn_mcxt, will live forever too.
 */
static void perm_fmgr_info(Oid functionId, FmgrInfo* finfo)
{
    fmgr_info_cxt(functionId, finfo, THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB));
}
