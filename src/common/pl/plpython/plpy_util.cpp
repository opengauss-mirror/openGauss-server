/*
 * utility functions
 *
 * src/common/pl/plpython/plpy_util.c
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "mb/pg_wchar.h"
#include "utils/memutils.h"
#include "utils/palloc.h"

#include "plpython.h"

#include "plpy_util.h"

#include "plpy_elog.h"

void* PLy_malloc(size_t bytes)
{
    /* We need our allocations to be long-lived, so use t_thrd.top_mem_cxt */
    return MemoryContextAlloc(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), bytes);
}

void* PLy_malloc0(size_t bytes)
{
    void* ptr = PLy_malloc(bytes);

    if (bytes > 0) {
        errno_t rc = memset_s(ptr, bytes, 0, bytes);
        securec_check(rc, "\0", "\0");
    }
    return ptr;
}

char* PLy_strdup(const char* str)
{
    char* result = NULL;
    size_t len;
    errno_t rc = EOK;

    len = strlen(str) + 1;
    result = (char*)PLy_malloc(len);
    rc = memcpy_s(result, len, str, len);
    securec_check(rc, "\0", "\0");

    return result;
}

/* define this away */
void PLy_free(void* ptr)
{
    pfree(ptr);
}

/*
 * Convert a Python unicode object to a Python string/bytes object in
 * openGauss server encoding.	Reference ownership is passed to the
 * caller.
 */
PyObject* PLyUnicode_Bytes(PyObject* unicode)
{
    PyObject* bytes = NULL;
    PyObject* rv = NULL;
    char* utf8string = NULL;
    char* encoded = NULL;

    /* First encode the Python unicode object with UTF-8. */
    bytes = PyUnicode_AsUTF8String(unicode);
    if (bytes == NULL) {
        PLy_elog(ERROR, "could not convert Python Unicode object to bytes");
    }

    utf8string = PyBytes_AsString(bytes);
    if (utf8string == NULL) {
        Py_DECREF(bytes);
        PLy_elog(ERROR, "could not extract bytes from encoded string");
    }

    /*
     * Then convert to server encoding if necessary.
     *
     * PyUnicode_AsEncodedString could be used to encode the object directly
     * in the server encoding, but Python doesn't support all the encodings
     * that openGauss does (EUC_TW and MULE_INTERNAL). UTF-8 is used as an
     * intermediary in PLyUnicode_FromString as well.
     */
    if (GetDatabaseEncoding() != PG_UTF8) {
        PG_TRY();
        {
            encoded = (char*)pg_do_encoding_conversion(
                (unsigned char*)utf8string, strlen(utf8string), PG_UTF8, GetDatabaseEncoding());
        }
        PG_CATCH();
        {
            Py_DECREF(bytes);
            PG_RE_THROW();
        }
        PG_END_TRY();
    } else
        encoded = utf8string;

    /* finally, build a bytes object in the server encoding */
    rv = PyBytes_FromStringAndSize(encoded, strlen(encoded));

    /* if pg_do_encoding_conversion allocated memory, free it now */
    if (utf8string != encoded) {
        pfree(encoded);
    }

    Py_DECREF(bytes);
    return rv;
}

/*
 * Convert a Python unicode object to a C string in openGauss server
 * encoding.  No Python object reference is passed out of this
 * function.  The result is palloc'ed.
 *
 * Note that this function is disguised as PyString_AsString() when
 * using Python 3.	That function retuns a pointer into the internal
 * memory of the argument, which isn't exactly the interface of this
 * function.  But in either case you get a rather short-lived
 * reference that you ought to better leave alone.
 */
char* PLyUnicode_AsString(PyObject* unicode)
{
    PyObject* o = PLyUnicode_Bytes(unicode);
    char* rv = pstrdup(PyBytes_AsString(o));

    Py_XDECREF(o);
    return rv;
}

#if PY_MAJOR_VERSION >= 3
/*
 * Convert a C string in the openGauss server encoding to a Python
 * unicode object.	Reference ownership is passed to the caller.
 */
PyObject* PLyUnicode_FromString(const char* s)
{
    char* utf8string = NULL;
    PyObject* o = NULL;

    utf8string = (char*)pg_do_encoding_conversion((unsigned char*)s, strlen(s), GetDatabaseEncoding(), PG_UTF8);

    o = PyUnicode_FromString(utf8string);

    if (utf8string != s) {
        pfree(utf8string);
    }

    return o;
}

#endif /* PY_MAJOR_VERSION >= 3 */
