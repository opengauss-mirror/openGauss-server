/* -------------------------------------------------------------------------
 *
 * readfuncs.cpp
 *	  Reader functions for openGauss tree nodes.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *	  src/common/backend/nodes/readfuncs.cpp
 *
 * NOTES
 *	  Path and Plan nodes do not have any readfuncs support, because we
 *	  never have occasion to read them in.	(There was once code here that
 *	  claimed to read them, but it was broken as well as unused.)  We
 *	  never read executor state trees, either.
 *
 *	  Parse location fields are written out by outfuncs.c, but only for
 *	  possible debugging use.  When reading a location field, we discard
 *	  the stored value and set the location field to -1 (ie, "unknown").
 *	  This is because nodes coming from a stored rule should not be thought
 *	  to have a known location in the current query's text.
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <math.h>

#include "miscadmin.h"
#include "bulkload/dist_fdw.h"
#include "catalog/gs_opt_model.h"
#include "nodes/parsenodes.h"
#include "foreign/fdwapi.h"
#include "nodes/plannodes.h"
#include "optimizer/dataskew.h"
#include "optimizer/planner.h"
#include "nodes/parsenodes.h"
#include "nodes/readfuncs.h"
#include "parser/parse_func.h"
#include "parser/parse_hint.h"
#ifdef PGXC
#include "access/htup.h"
#include "catalog/dfsstore_ctlg.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_synonym.h"
#include "catalog/pg_type.h"
#include "access/attnum.h"
#include "pgxc/groupmgr.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgFdwRemote.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "catalog/pg_enum.h"
#include "access/transam.h"
#endif
#include "executor/node/nodeExtensible.h"
#include "storage/lmgr.h"
#include "optimizer/streamplan.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "tcop/utility.h"
#include "utils/builtins.h"

THR_LOCAL bool skip_read_extern_fields = false;

#define IS_DATANODE_BUT_NOT_SINGLENODE (IS_PGXC_DATANODE && !IS_SINGLE_NODE)
/*
 * Macros to simplify reading of different kinds of fields.  Use these
 * wherever possible to reduce the chance for silly typos.	Note that these
 * hard-wire conventions about the names of the local variables in a Read
 * routine.
 */

/* Macros for declaring appropriate local variables */

/* try to see if a field exist
 * IF_EXIST(fieldname)
 * {
 *    ... do actual read
 * }
 */
#define IF_EXIST(fieldname)                              \
    if ((NULL != (token = pg_strtok(&length, false))) && \
        (0 == strncmp(token, ":" CppAsString(fieldname), strlen(":" CppAsString(fieldname)))))

/* A few guys need only local_node */
#define READ_LOCALS_NO_FIELDS(nodeTypeName) nodeTypeName* local_node = makeNode(nodeTypeName)

#define READ_LOCALS_NULL(nodeTypeName) \
    if (local_node == NULL)            \
        local_node = makeNode(nodeTypeName);

/* And a few guys need only the pg_strtok support fields */
#define READ_TEMP_LOCALS() \
    char* token = NULL;    \
    int length;

#define LIST_LENGTH(fldname) \
    int size;                \
    size = list_length(local_node->fldname);

#define DEFINE_UINT64_ARRAY(arrayLen) uint64* local_node = (uint64*)palloc0(sizeof(uint64) * (arrayLen))

#define DEFINE_UINT16_ARRAY(arrayLen) uint16* local_node = (uint16*)palloc0(sizeof(uint16) * (arrayLen))

#define READ_UINT64_ARRAY_FILED(arrayLen) \
    DEFINE_UINT64_ARRAY(arrayLen);        \
    READ_TEMP_LOCALS()

#define READ_UINT16_ARRAY_FILED(arrayLen) \
    DEFINE_UINT16_ARRAY(arrayLen);        \
    READ_TEMP_LOCALS()

/* ... but most need both */
#define READ_LOCALS(nodeTypeName)        \
    READ_LOCALS_NO_FIELDS(nodeTypeName); \
    READ_TEMP_LOCALS()

/* Read an integer field (anything written as ":fldname %d") */
#define READ_INT_FIELD(fldname)                           \
    do {                                                  \
        token = pg_strtok(&length); /* skip :fldname */   \
        token = pg_strtok(&length); /* get field value */ \
        local_node->fldname = atoi(token);                \
    } while (0)

#define READ_INT_FIELD_DIRECT(fldname)                \
    token = pg_strtok(&length); /* get field value */ \
    local_node->fldname = atoi(token)

/* Read an integer field (anything written as ":fldname %ld") */
#define READ_LONG_FIELD(fldname)                          \
    do {                                                  \
        token = pg_strtok(&length); /* skip :fldname */   \
        token = pg_strtok(&length); /* get field value */ \
        local_node->fldname = atol(token);                \
    } while (0)

/* Read an 64bit unsigned integer field (anything written as ":fldname %lu") */
#define READ_UINT64_FIELD(fldname)                        \
    do {                                                  \
        token = pg_strtok(&length); /* skip :fldname */   \
        token = pg_strtok(&length); /* get field value */ \
        local_node->fldname = strtoul(token, NULL, 0);    \
    } while (0)

/* Read an unsigned long integer field (anything written as ":fldname %UINT64") */
#define READ_ULONG_FIELD(fldname)                         \
    do {                                                  \
        token = pg_strtok(&length); /* skip :fldname */   \
        token = pg_strtok(&length); /* get field value */ \
        local_node->fldname = strtoul(token, NULL, 0);    \
    } while (0)

#define READ_POINTER_FIELD(fldname, datatype)                     \
    do {                                                          \
        token = pg_strtok(&length); /* skip :fldname */           \
        token = pg_strtok(&length); /* get field value */         \
        local_node->fldname = (datatype*)strtoul(token, NULL, 0); \
    } while (0)

#define READ_INT_ARRAY(fldname, size)                                       \
    do {                                                                    \
        local_node->fldname = (int*)palloc(local_node->size * sizeof(int)); \
        token = pg_strtok(&length); /* skip :fldname */                     \
        for (int i = 0; i < local_node->size; i++) {                        \
            token = pg_strtok(&length); /* get field value */               \
            local_node->fldname[i] = atoi(token);                           \
        }                                                                   \
    } while (0)

#define READ_DOUBLE_ARRAY(fldname, size)                                          \
    do {                                                                          \
        local_node->fldname = (double*)palloc(local_node->size * sizeof(double)); \
        token = pg_strtok(&length); /* skip :fldname */                           \
        for (int i = 0; i < local_node->size; i++) {                              \
            token = pg_strtok(&length); /* get field value */                     \
            local_node->fldname[i] = atof(token);                                 \
        }                                                                         \
    } while (0)

#define READ_INT_ARRAY_LEN(fldname)                             \
    do {                                                        \
        local_node->fldname = (int*)palloc(size * sizeof(int)); \
        token = pg_strtok(&length); /* skip :fldname */         \
        for (int i = 0; i < size; i++) {                        \
            token = pg_strtok(&length); /* get field value */   \
            local_node->fldname[i] = atoi(token);               \
        }                                                       \
    } while (0)

#define READ_UINT2_ARRAY_LEN(fldname)                               \
    do {                                                            \
        local_node->fldname = (uint2*)palloc(size * sizeof(uint2)); \
        token = pg_strtok(&length); /* skip :fldname */             \
        for (int i = 0; i < size; i++) {                            \
            token = pg_strtok(&length); /* get field value */       \
            local_node->fldname[i] = (uint2)atoi(token);            \
        }                                                           \
    } while (0)

#define READ_ATTR_ARRAY(fldname, size)                                                    \
    do {                                                                                  \
        local_node->fldname = (AttrNumber*)palloc(local_node->size * sizeof(AttrNumber)); \
        token = pg_strtok(&length); /* skip :fldname */                                   \
        for (int i = 0; i < local_node->size; i++) {                                      \
            token = pg_strtok(&length); /* get field value */                             \
            local_node->fldname[i] = atoi(token);                                         \
        }                                                                                 \
    } while (0)

/* Read an unsigned integer field (anything written as ":fldname %u") */
#define READ_UINT_FIELD(fldname)                          \
    do {                                                  \
        token = pg_strtok(&length); /* skip :fldname */   \
        token = pg_strtok(&length); /* get field value */ \
        local_node->fldname = atoui(token);               \
    } while (0)

/* Read an OID field (don't hard-wire assumption that OID is same as uint) */
#define READ_OID_FIELD(fldname)                           \
    do {                                                  \
        token = pg_strtok(&length); /* skip :fldname */   \
        token = pg_strtok(&length); /* get field value */ \
        local_node->fldname = atooid(token);              \
    } while (0)

#define READ_OID_ARRAY(fldname, size)                                       \
    do {                                                                    \
        local_node->fldname = (Oid*)palloc(local_node->size * sizeof(Oid)); \
        token = pg_strtok(&length); /* skip :fldname */                     \
        for (int i = 0; i < local_node->size; i++) {                        \
            token = pg_strtok(&length); /* get field value */               \
            local_node->fldname[i] = atooid(token);                         \
        }                                                                   \
    } while (0)

/*
 * Note that this macro is used to convert collation to oid
 * by collation_name
 */
#define READ_OID_ARRAY_BYCONVERT(fldname, size)                                           \
    do {                                                                                  \
        for (int i = 0; i < local_node->size; i++) {                                      \
            if (local_node->fldname[i] >= FirstBootstrapObjectId) {                       \
                IF_EXIST(collname)                                                        \
                {                                                                         \
                    token = pg_strtok(&length); /* skip :fldname */                       \
                    token = pg_strtok(&length);                                           \
                    char* _collname = nullable_string(token, length);                     \
                    Assert(_collname != NULL);                                            \
                    if (!IS_PGXC_COORDINATOR) {                                           \
                        List* _collnameList = list_make1(makeString(_collname));          \
                        local_node->fldname[i] = get_collation_oid(_collnameList, false); \
                        list_free_ext(_collnameList);                                     \
                    }                                                                     \
                    if (NULL != _collname)                                                \
                        pfree_ext(_collname);                                             \
                }                                                                         \
            }                                                                             \
        }                                                                                 \
    } while (0);

#define READ_OID_ARRAY_LEN(fldname)                             \
    do {                                                        \
        local_node->fldname = (Oid*)palloc(size * sizeof(Oid)); \
        token = pg_strtok(&length); /* skip :fldname */         \
        for (int i = 0; i < size; i++) {                        \
            token = pg_strtok(&length); /* get field value */   \
            local_node->fldname[i] = atooid(token);             \
        }                                                       \
    } while (0);

/* Read a char field (ie, one ascii character) */
#define READ_CHAR_FIELD(fldname)                               \
    do {                                                       \
        token = pg_strtok(&length); /* skip :fldname */        \
        token = pg_strtok(&length); /* get field value */      \
        if (token == NULL) {                                   \
            ereport(ERROR,                                     \
                (errmodule(MOD_OPT),                           \
                    errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),   \
                    errmsg("token should not return NULL."))); \
        }                                                      \
        local_node->fldname = token[0];                        \
    } while (0);

/* Read an enumerated-type field that was written as an integer code */
#define READ_ENUM_FIELD(fldname, enumtype)                \
    do {                                                  \
        token = pg_strtok(&length); /* skip :fldname */   \
        token = pg_strtok(&length); /* get field value */ \
        local_node->fldname = (enumtype)atoi(token);      \
    } while (0);

/* Read a float field */
#define READ_FLOAT_FIELD(fldname)                         \
    do {                                                  \
        token = pg_strtok(&length); /* skip :fldname */   \
        token = pg_strtok(&length); /* get field value */ \
        local_node->fldname = atof(token);                \
    } while (0)

/* Read a boolean field */
#define READ_BOOL_FIELD(fldname)                               \
    do {                                                       \
        token = pg_strtok(&length); /* skip :fldname */        \
        token = pg_strtok(&length); /* get field value */      \
        if (token == NULL) {                                   \
            ereport(ERROR,                                     \
                (errmodule(MOD_OPT),                           \
                    errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),   \
                    errmsg("token should not return NULL."))); \
        }                                                      \
        local_node->fldname = strtobool(token);                \
    } while (0)

#define READ_BOOL_FIELD_DIRECT(fldname)                                                                              \
    token = pg_strtok(&length); /* get field value */                                                                \
    if (token == NULL) {                                                                                             \
        ereport(ERROR,                                                                                               \
            (errmodule(MOD_OPT), errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("token should not return NULL."))); \
    }                                                                                                                \
    local_node->fldname = strtobool(token)

#define READ_BOOL_ARRAY(fldname, size)                                        \
    do {                                                                      \
        local_node->fldname = (bool*)palloc(local_node->size * sizeof(bool)); \
        token = pg_strtok(&length); /* skip :fldname */                       \
        for (int i = 0; i < local_node->size; i++) {                          \
            token = pg_strtok(&length); /* get field value */                 \
            if (token == NULL) {                                              \
                ereport(ERROR,                                                \
                    (errmodule(MOD_OPT),                                      \
                        errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),              \
                        errmsg("token should not return NULL.")));            \
            }                                                                 \
            local_node->fldname[i] = strtobool(token);                        \
        }                                                                     \
    } while (0)

#define READ_BOOL_ARRAY_LEN(fldname)                               \
    do {                                                           \
        local_node->fldname = (bool*)palloc(size * sizeof(bool));  \
        token = pg_strtok(&length); /* skip :fldname */            \
        for (int i = 0; i < size; i++) {                           \
            token = pg_strtok(&length); /* get field value */      \
            if (token == NULL) {                                   \
                ereport(ERROR,                                     \
                    (errmodule(MOD_OPT),                           \
                        errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),   \
                        errmsg("token should not return NULL."))); \
            }                                                      \
            local_node->fldname[i] = strtobool(token);             \
        }                                                          \
    } while (0);

/* Read a character-string field */
#define READ_STRING_FIELD(fldname)                            \
    do {                                                      \
        token = pg_strtok(&length); /* skip :fldname */       \
        token = pg_strtok(&length); /* get field value */     \
        local_node->fldname = nullable_string(token, length); \
    } while (0)

#define READCOPY_STRING_FIELD_DIRECT(fldname)                                                                \
    do {                                                                                                     \
        token = pg_strtok(&length); /* get field value */                                                    \
        if (length >= 0) {                                                                                   \
            int reterrno = strncpy_s(local_node->fldname, length + 1, (debackslash(token, length)), length); \
            securec_check(reterrno, "\0", "\0");                                                             \
            local_node->fldname[length] = '\0';                                                              \
        }                                                                                                    \
    } while (0)

/* Read a parse location field (and throw away the value, per notes above) */
#define READ_LOCATION_FIELD(fldname)                                    \
    do {                                                                \
        token = pg_strtok(&length);        /* skip :fldname */          \
        token = pg_strtok(&length);        /* get field value */        \
        local_node->fldname = atoi(token); /* set field to "unknown" */ \
    } while (0)

/* Read a Node field */
#define READ_NODE_FIELD(fldname)                                                               \
    do {                                                                                       \
        token = pg_strtok(&length); /* skip :fldname */                                        \
        void* ptr = nodeRead(NULL, 0);                                                         \
        errno_t reterrno = memcpy_s(&local_node->fldname, sizeof(void*), &ptr, sizeof(void*)); \
        securec_check(reterrno, "\0", "\0");                                                   \
    } while (0)

/* Read a bitmapset field */
#define READ_BITMAPSET_FIELD(fldname)               \
    token = pg_strtok(&length); /* skip :fldname */ \
    local_node->fldname = _readBitmapset()

#define READ_TYPEINFO_FIELD(fldname)                                                                              \
    do {                                                                                                          \
        if (local_node->fldname >= FirstBootstrapObjectId) {                                                      \
            IF_EXIST(exprtypename)                                                                                \
            {                                                                                                     \
                char* exprtypename = NULL;                                                                        \
                char* exprtypenamespace = NULL;                                                                   \
                token = pg_strtok(&length);                                                                       \
                token = pg_strtok(&length);                                                                       \
                exprtypename = nullable_string(token, length);                                                    \
                token = pg_strtok(&length);                                                                       \
                token = pg_strtok(&length);                                                                       \
                exprtypenamespace = nullable_string(token, length);                                               \
                /* No need to reset field on CN or singlenode, keep pg_strtok() for forward compatibility */      \
                if (IS_DATANODE_BUT_NOT_SINGLENODE) {                                                             \
                    local_node->fldname = get_typeoid(get_namespace_oid(exprtypenamespace, false), exprtypename); \
                }                                                                                                 \
                pfree_ext(exprtypename);                                                                          \
                pfree_ext(exprtypenamespace);                                                                     \
            }                                                                                                     \
        }                                                                                                         \
    } while (0)

#define READ_TYPEINFO(typePtr)                                                                \
    do {                                                                                      \
        /* fetch next typid that write in typeinfo list */                                    \
        IF_EXIST(exprtypename)                                                                \
        {                                                                                     \
            char* exprtypename = NULL;                                                        \
            char* exprtypenamespace = NULL;                                                   \
            token = pg_strtok(&length);                                                       \
            token = pg_strtok(&length);                                                       \
            exprtypename = nullable_string(token, length);                                    \
            token = pg_strtok(&length);                                                       \
            token = pg_strtok(&length);                                                       \
            exprtypenamespace = nullable_string(token, length);                               \
            typePtr = get_typeoid(get_namespace_oid(exprtypenamespace, false), exprtypename); \
            pfree_ext(exprtypename);                                                          \
            pfree_ext(exprtypenamespace);                                                     \
        }                                                                                     \
    } while (0)

#define READ_TYPEINFO_LIST(fldname)                                       \
    do {                                                                  \
        ListCell* cell = NULL;                                            \
        foreach (cell, local_node->fldname) {                             \
            Oid typid = lfirst_oid(cell);                                 \
            if (typid < FirstBootstrapObjectId) /* skip bootstrap type */ \
                continue;                                                 \
            /* fetch next typid that write in typeinfo list */            \
            READ_TYPEINFO(lfirst_oid(cell));                              \
        }                                                                 \
    } while (0)

#define READ_TYPINFO_ARRAY(fldname, size)                                 \
    do {                                                                  \
        for (int i = 0; i < local_node->size; i++) {                      \
            Oid typid = local_node->fldname[i];                           \
            if (typid < FirstBootstrapObjectId) /* skip bootstrap type */ \
                continue;                                                 \
            /* fetch next typid that write in typeinfo list */            \
            READ_TYPEINFO(local_node->fldname[i]);                        \
        }                                                                 \
    } while (0);

/* read full-text search configuratio's oid from its name */
#define READ_CFGINFO_FIELD(fldname1, fldname2)                                                                        \
    do {                                                                                                              \
        if (REGCONFIGOID == local_node->fldname1) {                                                                   \
            char *cfgname, *cfgnamespace;                                                                             \
            token = pg_strtok(&length);                                                                               \
            token = pg_strtok(&length);                                                                               \
            cfgname = nullable_string(token, length);                                                                 \
            token = pg_strtok(&length);                                                                               \
            token = pg_strtok(&length);                                                                               \
            cfgnamespace = nullable_string(token, length);                                                            \
            local_node->fldname2 =                                                                                    \
                DirectFunctionCall1(regconfigin, CStringGetDatum(quote_qualified_identifier(cfgnamespace, cfgname))); \
            pfree_ext(cfgname);                                                                                       \
            pfree_ext(cfgnamespace);                                                                                  \
        }                                                                                                             \
    } while (0)

#define READ_FUNCINFO_FIELD(fldname)                                                                        \
    do {                                                                                                    \
        if (local_node->fldname >= FirstBootstrapObjectId) {                                                \
            IF_EXIST(funcname)                                                                              \
            {                                                                                               \
                char *funcname, *funcnamespace;                                                             \
                token = pg_strtok(&length);                                                                 \
                token = pg_strtok(&length);                                                                 \
                funcname = nullable_string(token, length);                                                  \
                token = pg_strtok(&length);                                                                 \
                token = pg_strtok(&length);                                                                 \
                funcnamespace = nullable_string(token, length);                                             \
                bool notfound = false;                                                                      \
                if (IS_DATANODE_BUT_NOT_SINGLENODE && !skip_read_extern_fields) {                           \
                    Oid funcoid = InvalidOid;                                                               \
                    do {                                                                                    \
                        Oid nspid = get_namespace_oid(funcnamespace, true);                                 \
                        if (!OidIsValid(nspid)) {                                                           \
                            notfound = true;                                                                \
                            break;                                                                          \
                        }                                                                                   \
                        funcoid = get_func_oid(funcname, nspid, (Expr*)local_node);                         \
                    } while (0);                                                                            \
                    if (notfound || !OidIsValid(funcoid)) {                                                 \
                        ereport(ERROR,                                                                      \
                            (errmodule(MOD_OPT), errcode(ERRCODE_UNDEFINED_OBJECT),                         \
                                errmsg("Cannot identify function %s.%s while deserializing field.",         \
                                    funcname, funcnamespace),                                               \
                                errdetail("Function with oid %u or its namespace may be renamed",           \
                                    local_node->fldname),                                                   \
                                errhint("Please rebuild column defalt expression, views etc. that are"      \
                                    " related to this renamed object."),                                    \
                                errcause("Object renamed after recorded as nodetree."),                     \
                                erraction("Rebuild relevant object.")));                                    \
                    }                                                                                       \
                    local_node->fldname = funcoid;                                                          \
                }                                                                                           \
                pfree_ext(funcname);                                                                        \
                pfree_ext(funcnamespace);                                                                   \
            }                                                                                               \
        }                                                                                                   \
    } while (0)

#define READ_OPINFO_FIELD(fldname)                                                              \
    do {                                                                                        \
        if (local_node->fldname >= FirstNormalObjectId) {                                       \
            char* opname;                                                                       \
            char* opnamespace;                                                                  \
            char* oprleftname;                                                                  \
            char* oprrightname;                                                                 \
            Oid namespaceId;                                                                    \
            Oid oprleft;                                                                        \
            Oid oprright;                                                                       \
            token = pg_strtok(&length);                                                         \
            token = pg_strtok(&length);                                                         \
            opname = nullable_string(token, length);                                            \
            token = pg_strtok(&length);                                                         \
            token = pg_strtok(&length);                                                         \
            opnamespace = nullable_string(token, length);                                       \
            token = pg_strtok(&length);                                                         \
            token = pg_strtok(&length);                                                         \
            oprleftname = nullable_string(token, length);                                       \
            token = pg_strtok(&length);                                                         \
            token = pg_strtok(&length);                                                         \
            oprrightname = nullable_string(token, length);                                      \
            if (IS_DATANODE_BUT_NOT_SINGLENODE) {                                               \
                namespaceId = get_namespace_oid(opnamespace, false);                            \
                oprleft = get_typeoid(namespaceId, oprleftname);                                \
                oprright = oprleft;                                                             \
                local_node->fldname = get_operator_oid(opname, namespaceId, oprleft, oprright); \
            }                                                                                   \
            pfree_ext(opname);                                                                  \
            pfree_ext(opnamespace);                                                             \
            pfree_ext(oprleftname);                                                             \
        }                                                                                       \
    } while (0)

#define READ_OPERATOROID_ARRAY(fldname, size)                                                          \
    do {                                                                                               \
        local_node->fldname = (Oid*)palloc(local_node->size * sizeof(Oid));                            \
        token = pg_strtok(&length);                                                                    \
        token = pg_strtok(&length);                                                                    \
        token = pg_strtok(&length);                                                                    \
        for (int i = 0; i < local_node->size; i++) {                                                   \
            token = pg_strtok(&length);                                                                \
            token = pg_strtok(&length);                                                                \
            token = pg_strtok(&length);                                                                \
            local_node->fldname[i] = atooid(token);                                                    \
            if (local_node->fldname[i] >= FirstNormalObjectId) {                                       \
                char* opname;                                                                          \
                char* opnamespace;                                                                     \
                char* oprleftname;                                                                     \
                char* oprrightname;                                                                    \
                Oid namespaceId;                                                                       \
                Oid oprleft;                                                                           \
                Oid oprright;                                                                          \
                token = pg_strtok(&length);                                                            \
                token = pg_strtok(&length);                                                            \
                opname = nullable_string(token, length);                                               \
                token = pg_strtok(&length);                                                            \
                token = pg_strtok(&length);                                                            \
                opnamespace = nullable_string(token, length);                                          \
                token = pg_strtok(&length);                                                            \
                token = pg_strtok(&length);                                                            \
                oprleftname = nullable_string(token, length);                                          \
                token = pg_strtok(&length);                                                            \
                token = pg_strtok(&length);                                                            \
                oprrightname = nullable_string(token, length);                                         \
                if (IS_DATANODE_BUT_NOT_SINGLENODE) {                                                  \
                    namespaceId = get_namespace_oid(opnamespace, false);                               \
                    oprleft = get_typeoid(namespaceId, oprleftname);                                   \
                    oprright = oprleft;                                                                \
                    local_node->fldname[i] = get_operator_oid(opname, namespaceId, oprleft, oprright); \
                }                                                                                      \
                pfree_ext(opname);                                                                     \
                pfree_ext(opnamespace);                                                                \
                pfree_ext(oprleftname);                                                                \
            }                                                                                          \
            token = pg_strtok(&length);                                                                \
            token = pg_strtok(&length);                                                                \
        }                                                                                              \
    } while (0);

#define READ_RELINFO_FIELD(fldname)                                                   \
    do {                                                                              \
        if (local_node->fldname >= FirstBootstrapObjectId) {                          \
            IF_EXIST(relname)                                                         \
            {                                                                         \
                char *relname, *relnamespace;                                         \
                token = pg_strtok(&length);                                           \
                token = pg_strtok(&length);                                           \
                relname = nullable_string(token, length);                             \
                token = pg_strtok(&length);                                           \
                token = pg_strtok(&length);                                           \
                relnamespace = nullable_string(token, length);                        \
                local_node->fldname = get_valid_relname_relid(relnamespace, relname); \
                pfree_ext(relname);                                                   \
                pfree_ext(relnamespace);                                              \
            }                                                                         \
        }                                                                             \
    } while (0)

#define READ_SYNINFO_FILED(fldname)                                                                      \
    do {                                                                                                 \
        IF_EXIST(synname)                                                                                \
        {                                                                                                \
            char* synName = NULL;                                                                        \
            char* synSchema = NULL;                                                                      \
            token = pg_strtok(&length);                                                                  \
            token = pg_strtok(&length);                                                                  \
            synName = nullable_string(token, length);                                                    \
            token = pg_strtok(&length);                                                                  \
            token = pg_strtok(&length);                                                                  \
            synSchema = nullable_string(token, length);                                                  \
            Assert(synName != NULL && synSchema != NULL);                                                \
            if (!IS_SINGLE_NODE && !IS_PGXC_COORDINATOR && !isRestoreMode) {                             \
                local_node->fldname = GetSynonymOid(synName, get_namespace_oid(synSchema, false), true); \
            }                                                                                            \
            pfree_ext(synName);                                                                          \
            pfree_ext(synSchema);                                                                        \
        }                                                                                                \
    } while (0)

/* Routine exit. This if branch just keep compiler silent. */
#define READ_DONE()      \
    if (token != NULL) { \
        token = NULL;    \
    }                    \
    return local_node

#define READ_END() return local_node

/*
 * NOTE: use atoi() to read values written with %d, or atoui() to read
 * values written with %u in outfuncs.c.  An exception is OID values,
 * for which use atooid().	(As of 7.1, outfuncs.c writes OIDs as %u,
 * but this will probably change in the future.)
 */
#define atoui(x) ((unsigned int)strtoul((x), NULL, 10))

#define atooid(x) ((Oid)strtoul((x), NULL, 10))

#define strtobool(x) ((*(x) == 't') ? true : false)

#define nullable_string(token, length) ((length) == 0 ? NULL : debackslash(token, length))

/*
 * function for _readOpExpr, _readDistinctExpr and _readNullIfExpr.
 */
#define READ_EXPR_FIELD()                                                           \
    do {                                                                            \
        READ_OID_FIELD(opno);                                                       \
        READ_OPINFO_FIELD(opno);                                                    \
        READ_OID_FIELD(opfuncid);                                                   \
        READ_FUNCINFO_FIELD(opfuncid);                                              \
                                                                                    \
        /* */                                                                       \
        /* The opfuncid is stored in the textual format primarily for debugging*/   \
        /* and documentation reasons.  We want to always read it as zero to force*/ \
        /* it to be re-looked-up in the pg_operator entry.	This ensures that*/     \
        /* stored rules don't have hidden dependencies on operators' functions.*/   \
        /* (We don't currently support an ALTER OPERATOR command, but might*/       \
        /* someday.)*/                                                              \
        /* */                                                                       \
        /* Until PteroDB supports ALTER OPERATOR let's comment this to make*/       \
        /* Plan shipping work*/                                                     \
        READ_OID_FIELD(opresulttype);                                               \
        READ_BOOL_FIELD(opretset);                                                  \
        READ_OID_FIELD(opcollid);                                                   \
        READ_OID_FIELD(inputcollid);                                                \
        READ_NODE_FIELD(args);                                                      \
        READ_LOCATION_FIELD(location);                                              \
                                                                                    \
        READ_TYPEINFO_FIELD(opresulttype);                                          \
                                                                                    \
        READ_DONE();                                                                \
    } while (0)

/*
 * function for _readStream and _readVecStream.
 */
#define READ_STREAM_FIELD()                                 \
    do {                                                    \
        READ_TEMP_LOCALS();                                 \
                                                            \
        /* Read Plan */                                     \
        _readScan(&local_node->scan);                       \
                                                            \
        READ_ENUM_FIELD(type, StreamType);                  \
                                                            \
        READ_STRING_FIELD(plan_statement);                  \
        READ_NODE_FIELD(consumer_nodes);                    \
        READ_NODE_FIELD(distribute_keys);                   \
        READ_BOOL_FIELD(is_sorted);                         \
        READ_NODE_FIELD(sort);                              \
        READ_BOOL_FIELD(is_dummy);                          \
        READ_INT_FIELD(smpDesc.consumerDop);                \
        READ_INT_FIELD(smpDesc.producerDop);                \
        READ_ENUM_FIELD(smpDesc.distriType, SmpStreamType); \
        READ_NODE_FIELD(skew_list);                         \
        READ_INT_FIELD(stream_level);                       \
        READ_NODE_FIELD(origin_consumer_nodes);             \
        READ_BOOL_FIELD(is_recursive_local);                \
                                                            \
        READ_DONE();                                        \
    } while (0)

/*
 * function for _readRemoteQuery and _readVecRemoteQuery.
 */
#define READ_REMOTEQUERY_FIELD()                                                                  \
    do {                                                                                          \
        /* Read Scan */                                                                           \
        _readScan(&local_node->scan);                                                             \
                                                                                                  \
        READ_ENUM_FIELD(exec_direct_type, ExecDirectType);                                        \
        READ_STRING_FIELD(sql_statement);                                                         \
        READ_NODE_FIELD(exec_nodes);                                                              \
        READ_ENUM_FIELD(combine_type, CombineType);                                               \
                                                                                                  \
        READ_BOOL_FIELD(read_only);                                                               \
        READ_BOOL_FIELD(force_autocommit);                                                        \
        READ_STRING_FIELD(statement);                                                             \
        READ_STRING_FIELD(cursor);                                                                \
        READ_INT_FIELD(rq_num_params);                                                            \
        /* fix a bug, local_node->rq_num_params will be 0 in plan router and scan gather node. */ \
        if (local_node->rq_num_params > 0) {                                                      \
            READ_OID_ARRAY(rq_param_types, rq_num_params);                                        \
        }                                                                                         \
        READ_BOOL_FIELD(rq_params_internal);                                                      \
                                                                                                  \
        READ_ENUM_FIELD(exec_type, RemoteQueryExecType);                                          \
        READ_BOOL_FIELD(is_temp);                                                                 \
                                                                                                  \
        READ_BOOL_FIELD(rq_finalise_aggs);                                                        \
        READ_BOOL_FIELD(rq_sortgroup_colno);                                                      \
        READ_NODE_FIELD(remote_query);                                                            \
        READ_NODE_FIELD(base_tlist);                                                              \
        READ_NODE_FIELD(coord_var_tlist);                                                         \
        READ_NODE_FIELD(query_var_tlist);                                                         \
        READ_BOOL_FIELD(has_row_marks);                                                           \
        READ_BOOL_FIELD(rq_save_command_id);                                                      \
        READ_BOOL_FIELD(is_simple);                                                               \
        READ_BOOL_FIELD(mergesort_required);                                                      \
        READ_BOOL_FIELD(spool_no_data);                                                           \
        READ_BOOL_FIELD(poll_multi_channel);                                                      \
        READ_INT_FIELD(num_stream);                                                               \
        READ_INT_FIELD(num_gather);                                                               \
        READ_NODE_FIELD(sort);                                                                    \
        READ_NODE_FIELD(rte_ref);                                                                 \
        READ_ENUM_FIELD(position, RemoteQueryType);                                               \
                                                                                                  \
        READ_TYPINFO_ARRAY(rq_param_types, rq_num_params);                                        \
        IF_EXIST(is_remote_function_query)                                                        \
        {                                                                                         \
            READ_BOOL_FIELD(is_remote_function_query);                                            \
        }                                                                                         \
        IF_EXIST(isCustomPlan)                                                        \
        {                                                                                         \
            READ_BOOL_FIELD(isCustomPlan);                                            \
        }                                                                                         \
        IF_EXIST(isFQS)                                                        \
        {                                                                                         \
            READ_BOOL_FIELD(isFQS);                                            \
        }                                                                                         \
        IF_EXIST(relationOids)                                                                           \
        {                                                                                         \
            READ_NODE_FIELD(relationOids);                                                        \
        }                                                                                         \
        READ_DONE();                                                                              \
    } while (0)

/*
 * function for _readHashJoin and _readVecHashJoin.
 */
#define READ_HASHJOIN_FIELD()                 \
    do {                                      \
        READ_TEMP_LOCALS();                   \
                                              \
        /* Read Join */                       \
        _readJoin(&local_node->join);         \
                                              \
        READ_NODE_FIELD(hashclauses);         \
        READ_BOOL_FIELD(streamBothSides);     \
        READ_BOOL_FIELD(transferFilterFlag);  \
        READ_BOOL_FIELD(rebuildHashTable);    \
        READ_BOOL_FIELD(isSonicHash);         \
        read_mem_info(&local_node->mem_info); \
                                              \
        READ_DONE();                          \
    } while (0)

/*
 * function for _readMergeJoin and _readVecMergeJoin.
 */
#define READ_MERGEJOIN()                      \
    do {                                      \
        READ_TEMP_LOCALS();                   \
                                              \
        /* Read Join */                       \
        _readJoin(&local_node->join);         \
                                              \
        READ_NODE_FIELD(mergeclauses);        \
        LIST_LENGTH(mergeclauses);            \
        READ_OID_ARRAY_LEN(mergeFamilies);    \
        READ_OID_ARRAY_LEN(mergeCollations);  \
        READ_INT_ARRAY_LEN(mergeStrategies);  \
        READ_BOOL_ARRAY_LEN(mergeNullsFirst); \
                                              \
        READ_DONE();                          \
    } while (0)

/*
 * function for _readForeignScan and _readVecForeignScan.
 */
#define READ_FOREIGNSCAN()               \
    do {                                 \
        READ_OID_FIELD(scan_relid);      \
        READ_NODE_FIELD(fdw_exprs);      \
        READ_NODE_FIELD(fdw_private);    \
        READ_BOOL_FIELD(fsSystemCol);    \
        READ_BOOL_FIELD(needSaveError);  \
        READ_NODE_FIELD(errCache);       \
        READ_NODE_FIELD(prunningResult); \
        READ_NODE_FIELD(rel);            \
        READ_NODE_FIELD(options);        \
                                         \
        READ_LONG_FIELD(objectNum);      \
        READ_INT_FIELD(bfNum);           \
    } while (0)

static Datum readDatum(bool typbyval);
static Scan* _readScan(Scan* local_node);
extern bool StreamTopConsumerAmI();
static void read_mem_info(OpMemInfo* local_node);
static void _readCursorData(Cursor_Data* local_node);

/*
 * _readBitmapset
 */
static Bitmapset* _readBitmapset(void)
{
    Bitmapset* result = NULL;

    READ_TEMP_LOCALS();

    token = pg_strtok(&length);
    if (token == NULL) {
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("incomplete Bitmapset structure")));
    }
    if (length != 1 || token[0] != '(') {
        ereport(ERROR,
            (errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE), errmsg("unrecognized token: \"%.*s\"", length, token)));
    }
    token = pg_strtok(&length);
    if (token == NULL) {
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("incomplete Bitmapset structure")));
    }
    if (length != 1 || token[0] != 'b') {
        ereport(ERROR,
            (errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE), errmsg("unrecognized token: \"%.*s\"", length, token)));
    }
    
    for (;;) {
        int val;
        char* endptr = NULL;

        token = pg_strtok(&length);
        if (token == NULL) {
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("unterminated Bitmapset structure")));
        }
        if (length == 1 && token[0] == ')') {
            break;
        }
        val = (int)strtol(token, &endptr, 10);
        if (endptr != token + length) {
            ereport(ERROR,
                (errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),
                    errmsg("unrecognized integer: \"%.*s\"", length, token)));
        }
        result = bms_add_member(result, val);
    }

    return result;
}

/*
 * ereport error information of _readUint64Array and _readUint16Array
 */
void check_token_complete(const char* token, const int length)
{
    if (token == NULL) {
        ereport(ERROR, (errcode(ERRCODE_ARRAY_ELEMENT_ERROR), errmsg("incomplete array structure")));
    }

    if (length != 1 || token[0] != '(') {
        ereport(ERROR,
            (errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE), errmsg("unrecognized token: \"%.*s\"", length, token)));
    }
}

/*
 * ereport error information of _readUint64Array and _readUint16Array
 */
void token_if_terminated(const char* token)
{
    if (token == NULL) {
        ereport(ERROR, (errcode(ERRCODE_ARRAY_ELEMENT_ERROR), errmsg("unterminated array structure")));
    }
}

/**
 * @Description: deserialize the uint64 string "(a int int ...)" to uint64 array.
 * @in arrayLen, the array length.
 * @return
 */
static uint64* _readUint64Array(int arrayLen)
{
    READ_UINT64_ARRAY_FILED(arrayLen);

    token = pg_strtok(&length);
    check_token_complete(token, length);

    token = pg_strtok(&length);
    if (token == NULL) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("incomplete Bitmapset structure")));
    }
    if (length != 1 || token[0] != 'a') {
        ereport(ERROR,
            (errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE), errmsg("unrecognized token: \"%.*s\"", length, token)));
    }
    
    for (int index = 0;;) {
        uint64 val;
        char* endptr = NULL;

        token = pg_strtok(&length);
        token_if_terminated(token);

        if (length == 1 && token[0] == ')') {
            break;
        }
        val = (uint64)strtoul(token, &endptr, 10);
        if (endptr != token + length) {
            ereport(ERROR,
                (errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),
                    errmsg("unrecognized integer: \"%.*s\"", length, token)));
        }
        Assert(index < arrayLen);
        local_node[index++] = val;
    }

    return local_node;
}

/**
 * @Description: deserialize the uint16 string "(a int int ...)" to uint16 array.
 * @in arrayLen, the array length.
 * @return
 */
static uint16* _readUint16Array(int arrayLen)
{
    READ_UINT16_ARRAY_FILED(arrayLen);

    token = pg_strtok(&length);
    check_token_complete(token, length);

    token = pg_strtok(&length);
    if (token == NULL) {
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("incomplete Bitmapset structure")));
    }
    if (length != 1 || token[0] != 'a') {
        ereport(ERROR,
            (errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE), errmsg("unrecognized token: \"%.*s\"", length, token)));
    }
    for (int index = 0;;) {
        uint16 val;
        char* endptr = NULL;

        token = pg_strtok(&length);
        token_if_terminated(token);

        if (length == 1 && token[0] == ')') {
            break;
        }
        val = (uint16)strtol(token, &endptr, 10);
        if (endptr != token + length) {
            ereport(ERROR,
                (errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),
                    errmsg("unrecognized integer: \"%.*s\"", length, token)));
        }
        Assert(index < arrayLen);
        local_node[index++] = val;
    }

    return local_node;
}

/*
 * _readDistribution
 *     read information for a Distribution
 *
 * @param (in) : void
 *
 * @return:
 *     Distribution *: the target Distribution pointer
 */
static Distribution* _readDistribution(void)
{
    Distribution* local_node = NewDistribution();
    READ_TEMP_LOCALS();

    READ_UINT_FIELD(group_oid);
    READ_BITMAPSET_FIELD(bms_data_nodeids);

    READ_DONE();
}

/*
 * @Description: Read string to hint struct.
 * @return: Hint struct.
 */
static Hint* _readBaseHint(Hint* local_node)
{
    READ_TEMP_LOCALS();

    READ_NODE_FIELD(relnames);
    READ_ENUM_FIELD(hint_keyword, HintKeyword);
    READ_ENUM_FIELD(state, HintStatus);

    READ_DONE();
}

/*
 * @Description: Read string to join hint struct.
 * @return: Join hint struct.
 */
static JoinMethodHint* _readJoinHint(void)
{
    READ_LOCALS(JoinMethodHint);

    _readBaseHint(&(local_node->base));
    READ_BOOL_FIELD(negative);
    READ_BITMAPSET_FIELD(joinrelids);
    READ_BITMAPSET_FIELD(inner_joinrelids);

    READ_DONE();
}

/*
 * @Description: Read string to predpush hint struct.
 * @return: Predpush hint struct.
 */
static PredpushHint* _readPredpushHint(void)
{
    READ_LOCALS(PredpushHint);

    _readBaseHint(&(local_node->base));
    READ_BOOL_FIELD(negative);
    READ_STRING_FIELD(dest_name);
    READ_INT_FIELD(dest_id);
    READ_BITMAPSET_FIELD(candidates);

    READ_DONE();
}

/*
 * @Description: Read string to predpush same level hint struct.
 * @return: Predpush same level hint struct.
 */
static PredpushSameLevelHint* _readPredpushSameLevelHint(void)
{
    READ_LOCALS(PredpushSameLevelHint);

    _readBaseHint(&(local_node->base));
    READ_BOOL_FIELD(negative);
    READ_STRING_FIELD(dest_name);
    READ_INT_FIELD(dest_id);
    READ_BITMAPSET_FIELD(candidates);

    READ_DONE();
}

/*
 * @Description: Read string to rewrite hint struct.
 * @return: rewrite hint struct.
 */
static RewriteHint* _readRewriteHint(void)
{
    READ_LOCALS(RewriteHint);

    _readBaseHint(&(local_node->base));
    READ_NODE_FIELD(param_names);
    READ_UINT_FIELD(param_bits);

    READ_DONE();
}

/*
 * @Description: Read string to gather hint struct.
 * @return: gather hint struct.
 */
static GatherHint* _readGatherHint(void)
{
    READ_LOCALS(GatherHint);
    _readBaseHint(&(local_node->base));
    READ_ENUM_FIELD(source, GatherSource);
    READ_DONE();
}

/*
 * @Description: Read string to no_gpc hint struct.
 * @return: no_gpc hint struct.
 */
static NoGPCHint* _readNoGPCHint(void)
{
    READ_LOCALS_NO_FIELDS(NoGPCHint);

    _readBaseHint(&(local_node->base));

    READ_END();
}

/*
 * @Description: Read string to no_expand hint struct.
 * @return: no_expand hint struct.
 */
static NoExpandHint* _readNoExpandHint(void)
{
    READ_LOCALS_NO_FIELDS(NoExpandHint);

    _readBaseHint(&(local_node->base));

    READ_END();
}

/*
 * @Description: Read string to set hint struct.
 * @return: set hint struct.
 */
static SetHint* _readSetHint(void)
{
    READ_LOCALS(SetHint);

    _readBaseHint(&(local_node->base));
    READ_STRING_FIELD(name);
    READ_STRING_FIELD(value);

    READ_DONE();
}

/*
 * @Description: Read string to rewrite plancache hint struct.
 * @return: plancache hint struct.
 */
static PlanCacheHint* _readPlanCacheHint(void)
{
    READ_LOCALS(PlanCacheHint);

    _readBaseHint(&(local_node->base));
    READ_BOOL_FIELD(chooseCustomPlan);

    READ_DONE();
}

/*
 * @Description: Read string to leading hint struct.
 * @return: Leading hint struct.
 */
static LeadingHint* _readLeadingHint(void)
{
    READ_LOCALS(LeadingHint);
    _readBaseHint(&(local_node->base));
    READ_BOOL_FIELD(join_order_hint);
    READ_DONE();
}

/*
 * @Description: Read string to rows hint struct.
 * @return: Rows hint struct.
 */
static RowsHint* _readRowsHint(void)
{
    READ_LOCALS(RowsHint);

    _readBaseHint(&(local_node->base));

    READ_BITMAPSET_FIELD(joinrelids);
    READ_STRING_FIELD(rows_str);
    READ_ENUM_FIELD(value_type, RowsValueType);
    READ_FLOAT_FIELD(rows);

    READ_DONE();
}

/*
 * @Description: Read string to stream hint struct.
 * @return: Stream hint struct.
 */
static StreamHint* _readStreamHint(void)
{
    READ_LOCALS(StreamHint);

    _readBaseHint(&(local_node->base));
    READ_BOOL_FIELD(negative);
    READ_BITMAPSET_FIELD(joinrelids);
    READ_ENUM_FIELD(stream_type, StreamType);

    READ_DONE();
}

/*
 * @Description: Read string to BlockName hint struct.
 * @return: BlockName hint struct.
 */
static BlockNameHint* _readBlockNameHint(void)
{
    READ_LOCALS_NO_FIELDS(BlockNameHint);

    _readBaseHint(&(local_node->base));

    READ_END();
}

/*
 * @Description: Read string to Scan hint struct.
 * @return: Scan hint struct.
 */
static ScanMethodHint* _readScanMethodHint(void)
{
    READ_LOCALS(ScanMethodHint);

    _readBaseHint(&(local_node->base));
    READ_BOOL_FIELD(negative);
    READ_BITMAPSET_FIELD(relid);
    READ_NODE_FIELD(indexlist);

    READ_END();
}

/*
 * @Description: Read string to pgfdw remote information.
 * @return: Scan hint struct.
 */
static PgFdwRemoteInfo* _readPgFdwRemoteInfo(void)
{
    READ_LOCALS(PgFdwRemoteInfo);

    READ_CHAR_FIELD(reltype);
    READ_INT_FIELD(datanodenum);
    READ_ULONG_FIELD(snapsize);

    token = pg_strtok(&length); /* skip :fldname */

    local_node->snapshot = (Snapshot)_readUint16Array(local_node->snapsize / 2);

    READ_DONE();
}

/*
 * @Description: Read string to Skew hint struct.
 * @return: Skew hint struct.
 */
static SkewHint* _readSkewHint(void)
{
    READ_LOCALS(SkewHint);

    _readBaseHint(&(local_node->base));
    READ_BITMAPSET_FIELD(relid);
    READ_NODE_FIELD(column_list);
    READ_NODE_FIELD(value_list);

    READ_END();
}

/*
 * @Description: Read string to Skew hint transform struct.
 * @return: Skew hint transform struct.
 */
static SkewHintTransf* _readSkewHintTransf(void)
{
    READ_LOCALS(SkewHintTransf);

    READ_NODE_FIELD(before);
    READ_NODE_FIELD(rel_info_list);
    READ_NODE_FIELD(column_info_list);
    READ_NODE_FIELD(value_info_list);

    READ_END();
}

static SkewRelInfo* _readSkewRelInfo(void)
{
    READ_LOCALS(SkewRelInfo);

    READ_STRING_FIELD(relation_name);
    READ_OID_FIELD(relation_oid);
    READ_NODE_FIELD(rte);
    READ_NODE_FIELD(parent_rte);

    READ_END();
}

static SkewColumnInfo* _readSkewColumnInfo(void)
{
    READ_LOCALS(SkewColumnInfo);

    READ_OID_FIELD(relation_Oid);
    READ_STRING_FIELD(column_name);
    READ_UINT_FIELD(attnum);
    READ_OID_FIELD(column_typid);
    READ_NODE_FIELD(expr);

    READ_TYPEINFO_FIELD(column_typid);
    READ_END();
}

static SkewValueInfo* _readSkewValueInfo(void)
{
    READ_LOCALS(SkewValueInfo);

    READ_BOOL_FIELD(support_redis);
    READ_NODE_FIELD(const_value);

    READ_END();
}

/*
 * @Description: Read string to hint state struct.
 * @return: Hint state struct
 */
static HintState* _readHintState()
{
    READ_LOCALS(HintState);

    READ_INT_FIELD(nall_hints);
    READ_NODE_FIELD(join_hint);
    READ_NODE_FIELD(leading_hint);
    READ_NODE_FIELD(row_hint);
    READ_NODE_FIELD(stream_hint);
    READ_NODE_FIELD(block_name_hint);
    READ_NODE_FIELD(scan_hint);
    READ_NODE_FIELD(skew_hint);
    IF_EXIST(predpush_hint) {
        READ_NODE_FIELD(predpush_hint);
    }
    IF_EXIST(rewrite_hint) {
        READ_NODE_FIELD(rewrite_hint);
    }
    IF_EXIST(gather_hint) {
        READ_NODE_FIELD(gather_hint);
    }
    IF_EXIST(no_expand_hint) {
        READ_NODE_FIELD(no_expand_hint);
    }
    IF_EXIST(set_hint) {
        READ_NODE_FIELD(set_hint);
    }
    IF_EXIST(cache_plan_hint) {
        READ_NODE_FIELD(cache_plan_hint);
    }
    IF_EXIST(no_gpc_hint) {
        READ_NODE_FIELD(no_gpc_hint);
    }
    IF_EXIST(predpush_same_level_hint) {
        READ_NODE_FIELD(predpush_same_level_hint);
    }
    READ_DONE();
}

/*
 * _readQuery
 */
static Query* _readQuery(void)
{
    READ_LOCALS(Query);

    READ_ENUM_FIELD(commandType, CmdType);
    READ_ENUM_FIELD(querySource, QuerySource);
    local_node->queryId = 0; /* not saved in output format */
    READ_BOOL_FIELD(canSetTag);
    READ_NODE_FIELD(utilityStmt);
    READ_INT_FIELD(resultRelation);
    READ_BOOL_FIELD(hasAggs);
    READ_BOOL_FIELD(hasWindowFuncs);
    READ_BOOL_FIELD(hasSubLinks);
    READ_BOOL_FIELD(hasDistinctOn);
    READ_BOOL_FIELD(hasRecursive);
    READ_BOOL_FIELD(hasModifyingCTE);
    READ_BOOL_FIELD(hasForUpdate);
    IF_EXIST(hasRowSecurity) {
        READ_BOOL_FIELD(hasRowSecurity);
    }
    IF_EXIST(hasSynonyms) {
        READ_BOOL_FIELD(hasSynonyms);
    }
    READ_NODE_FIELD(cteList);
    READ_NODE_FIELD(rtable);
    READ_NODE_FIELD(jointree);
    READ_NODE_FIELD(targetList);

    IF_EXIST(starStart) {
        READ_NODE_FIELD(starStart);
    }

    IF_EXIST(starEnd) {
        READ_NODE_FIELD(starEnd);
    }

    IF_EXIST(starOnly) {
        READ_NODE_FIELD(starOnly);
    }

    READ_NODE_FIELD(returningList);
    READ_NODE_FIELD(groupClause);
    READ_NODE_FIELD(groupingSets);
    READ_NODE_FIELD(havingQual);
    READ_NODE_FIELD(windowClause);
    READ_NODE_FIELD(distinctClause);
    READ_NODE_FIELD(sortClause);
    READ_NODE_FIELD(limitOffset);
    READ_NODE_FIELD(limitCount);
    READ_NODE_FIELD(rowMarks);
    READ_NODE_FIELD(setOperations);
    READ_NODE_FIELD(constraintDeps);

    IF_EXIST(hintState) {
        READ_NODE_FIELD(hintState);
    }

#ifdef PGXC
    IF_EXIST(sql_statement) {
        READ_STRING_FIELD(sql_statement);
    }
    IF_EXIST(is_local) {
        READ_BOOL_FIELD(is_local);
    }
    IF_EXIST(has_to_save_cmd_id) {
        READ_BOOL_FIELD(has_to_save_cmd_id);
    }
    IF_EXIST(vec_output) {
        READ_BOOL_FIELD(vec_output);
    }
    IF_EXIST(isTruncationCastAdded) { /* in order to deal history data when user execute data recovery */
        (void*)pg_strtok(&length);    /* skip :fldname */
        token = pg_strtok(&length);   /* get field value */
        local_node->tdTruncCastStatus = (TdTruncCastStatus)(strtobool(token) ? 1 : 2);
    }
    IF_EXIST(tdTruncCastStatus) {
        READ_ENUM_FIELD(tdTruncCastStatus, TdTruncCastStatus);
    }
    IF_EXIST(equalVars) {
        READ_NODE_FIELD(equalVars);
    }
#endif

    IF_EXIST(mergeTarget_relation) {
        READ_INT_FIELD(mergeTarget_relation);
    }
    IF_EXIST(mergeSourceTargetList) {
        READ_NODE_FIELD(mergeSourceTargetList);
    }
    IF_EXIST(mergeActionList) {
        READ_NODE_FIELD(mergeActionList);
    }
    IF_EXIST(upsertQuery) {
        READ_NODE_FIELD(upsertQuery);
    }
    IF_EXIST(upsertClause) {
        READ_NODE_FIELD(upsertClause);
    }
    IF_EXIST(isRowTriggerShippable) {
        READ_BOOL_FIELD(isRowTriggerShippable);
    }
    IF_EXIST(use_star_targets) {
        READ_BOOL_FIELD(use_star_targets);
    }
    IF_EXIST(is_from_full_join_rewrite) {
        READ_BOOL_FIELD(use_star_targets);
    }
    IF_EXIST(can_push)
    {
        READ_BOOL_FIELD(can_push);
    }

    IF_EXIST(unique_check)
    {
        READ_BOOL_FIELD(unique_check);
    }

    READ_DONE();
}

/*
 * _readNotifyStmt
 */
static NotifyStmt* _readNotifyStmt(void)
{
    READ_LOCALS(NotifyStmt);

    READ_STRING_FIELD(conditionname);
    READ_STRING_FIELD(payload);

    READ_DONE();
}

/*
 * _readDeclareCursorStmt
 */
static DeclareCursorStmt* _readDeclareCursorStmt(void)
{
    READ_LOCALS(DeclareCursorStmt);

    READ_STRING_FIELD(portalname);
    READ_INT_FIELD(options);
    READ_NODE_FIELD(query);

    READ_DONE();
}

static CopyStmt* _readCopyStmt(void)
{
    READ_LOCALS(CopyStmt);
    READ_NODE_FIELD(relation);

    READ_DONE();
}

static AlterTableStmt* _readAlterTableStmt(void)
{
    READ_LOCALS(AlterTableStmt);
    READ_NODE_FIELD(relation);

    READ_DONE();
}

static PLDebug_variable* _readPLDebug_variable(void)
{
    READ_LOCALS(PLDebug_variable);
    READ_STRING_FIELD(name);
    READ_STRING_FIELD(var_type);
    READ_STRING_FIELD(value);
    READ_STRING_FIELD(pkgname);
    READ_BOOL_FIELD(isconst);

    READ_DONE();
}

static PLDebug_breakPoint* _readPLDebug_breakPoint(void)
{
    READ_LOCALS(PLDebug_breakPoint);
    READ_INT_FIELD(bpIndex);
    READ_OID_FIELD(funcoid);
    READ_INT_FIELD(lineno);
    READ_STRING_FIELD(query);
    READ_BOOL_FIELD(active);

    READ_DONE();
}

static PLDebug_frame* _readPLDebug_frame(void)
{
    READ_LOCALS(PLDebug_frame);
    READ_INT_FIELD(frameno);
    READ_STRING_FIELD(funcname);
    READ_INT_FIELD(lineno);
    READ_STRING_FIELD(query);
    READ_INT_FIELD(funcoid);

    READ_DONE();
}

/*
 * _readSortGroupClause
 */
static SortGroupClause* _readSortGroupClause(void)
{
    READ_LOCALS(SortGroupClause);

    READ_UINT_FIELD(tleSortGroupRef);
    READ_OID_FIELD(eqop);
    READ_OPINFO_FIELD(eqop);
    READ_OID_FIELD(sortop);
    READ_OPINFO_FIELD(sortop);
    READ_BOOL_FIELD(nulls_first);
    READ_BOOL_FIELD(hashable);
    READ_BOOL_FIELD(groupSet);

    READ_DONE();
}

/*
 * _readGroupingSet
 */
static GroupingSet* _readGroupingSet(void)
{
    READ_LOCALS(GroupingSet);

    READ_ENUM_FIELD(kind, GroupingSetKind);
    READ_NODE_FIELD(content);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readWindowClause
 */
static WindowClause* _readWindowClause(void)
{
    READ_LOCALS(WindowClause);

    READ_STRING_FIELD(name);
    READ_STRING_FIELD(refname);
    READ_NODE_FIELD(partitionClause);
    READ_NODE_FIELD(orderClause);
    READ_INT_FIELD(frameOptions);
    READ_NODE_FIELD(startOffset);
    READ_NODE_FIELD(endOffset);
    READ_UINT_FIELD(winref);
    READ_BOOL_FIELD(copiedOrder);

    READ_DONE();
}

/*
 * _readRowMarkClause
 */
static RowMarkClause* _readRowMarkClause(void)
{
    READ_LOCALS(RowMarkClause);

    READ_UINT_FIELD(rti);
    READ_BOOL_FIELD(forUpdate);
    READ_BOOL_FIELD(noWait);
    IF_EXIST(waitSec) {
        READ_INT_FIELD(waitSec);
    }

    READ_BOOL_FIELD(pushedDown);
    IF_EXIST(strength) {
        READ_ENUM_FIELD(strength, LockClauseStrength);
    }

    READ_DONE();
}

/*
 * _readCommonTableExpr
 */
static CommonTableExpr* _readCommonTableExpr(void)
{
    READ_LOCALS(CommonTableExpr);

    READ_STRING_FIELD(ctename);
    READ_NODE_FIELD(aliascolnames);
    IF_EXIST(ctematerialized) {
        READ_ENUM_FIELD(ctematerialized, CTEMaterialize);
    }
    READ_NODE_FIELD(ctequery);
    READ_LOCATION_FIELD(location);
    READ_BOOL_FIELD(cterecursive);
    READ_INT_FIELD(cterefcount);
    READ_NODE_FIELD(ctecolnames);
    READ_NODE_FIELD(ctecoltypes);
    READ_NODE_FIELD(ctecoltypmods);
    READ_NODE_FIELD(ctecolcollations);
    READ_TYPEINFO_LIST(ctecoltypes);

    /*
     * Set default value for locator_type, and we will set its real value
     * in SS_process_ctes later.
     */
    local_node->locator_type = LOCATOR_TYPE_NONE;
    IF_EXIST(self_reference) {
        READ_BOOL_FIELD(self_reference);
    }
    IF_EXIST(swoptions) {
        READ_NODE_FIELD(swoptions);
    }
    IF_EXIST(referenced_by_subquery) {
        READ_BOOL_FIELD(referenced_by_subquery);
    }
    READ_DONE();
}

/*
 * _readStartWithTargetRelInfo
 */
static StartWithTargetRelInfo* _readStartWithTargetRelInfo(void)
{
    READ_LOCALS(StartWithTargetRelInfo);

    READ_STRING_FIELD(relname);
    READ_STRING_FIELD(aliasname);
    READ_STRING_FIELD(ctename);
    READ_NODE_FIELD(columns);
    READ_NODE_FIELD(tblstmt);

    READ_ENUM_FIELD(rtekind, RTEKind);
    READ_NODE_FIELD(rte);
    READ_NODE_FIELD(rtr);

    READ_DONE();
}

/*
 * _readSetOperationStmt
 */
static SetOperationStmt* _readSetOperationStmt(void)
{
    READ_LOCALS(SetOperationStmt);

    READ_ENUM_FIELD(op, SetOperation);
    READ_BOOL_FIELD(all);
    READ_NODE_FIELD(larg);
    READ_NODE_FIELD(rarg);
    READ_NODE_FIELD(colTypes);
    READ_NODE_FIELD(colTypmods);
    READ_NODE_FIELD(colCollations);
    READ_NODE_FIELD(groupClauses);

    READ_TYPEINFO_LIST(colTypes);
    READ_DONE();
}

/*
 * Stuff from primnodes.h.
 */

static Alias* _readAlias(void)
{
    READ_LOCALS(Alias);

    READ_STRING_FIELD(aliasname);
    READ_NODE_FIELD(colnames);

    READ_DONE();
}

static RangeVar* _readRangeVar(void)
{
    READ_LOCALS(RangeVar);

    local_node->catalogname = NULL; /* not currently saved in output
                                     * format */

    READ_STRING_FIELD(schemaname);
    READ_STRING_FIELD(relname);
    READ_STRING_FIELD(partitionname);
    IF_EXIST(subpartitionname) {
        READ_STRING_FIELD(subpartitionname);
    }
    READ_ENUM_FIELD(inhOpt, InhOption);
    READ_CHAR_FIELD(relpersistence);
    READ_NODE_FIELD(alias);
    READ_LOCATION_FIELD(location);
    READ_BOOL_FIELD(ispartition);
    IF_EXIST(issubpartition) {
        READ_BOOL_FIELD(issubpartition);
    }
    READ_NODE_FIELD(partitionKeyValuesList);

    IF_EXIST(isbucket){
        READ_BOOL_FIELD(isbucket);
    }

    IF_EXIST(buckets) {
        READ_NODE_FIELD(buckets);
    }

    IF_EXIST(withVerExpr) {
        READ_BOOL_FIELD(withVerExpr);
    }

    READ_DONE();
}

static IntoClause* _readIntoClause(void)
{
    READ_LOCALS(IntoClause);

    READ_NODE_FIELD(rel);
    READ_NODE_FIELD(colNames);
    READ_NODE_FIELD(options);
    READ_ENUM_FIELD(onCommit, OnCommitAction);
    READ_ENUM_FIELD(row_compress, RelCompressType);
    READ_STRING_FIELD(tableSpaceName);
    READ_BOOL_FIELD(skipData);
    READ_CHAR_FIELD(relkind);

    READ_DONE();
}

/*
 * _readVar
 */
static Var* _readVar(void)
{
    READ_LOCALS(Var);

    READ_UINT_FIELD(varno);
    READ_INT_FIELD(varattno);
    READ_OID_FIELD(vartype);
    READ_INT_FIELD(vartypmod);
    READ_OID_FIELD(varcollid);
    READ_UINT_FIELD(varlevelsup);
    READ_UINT_FIELD(varnoold);
    READ_INT_FIELD(varoattno);
    READ_LOCATION_FIELD(location);

    /*
     * recompute the vartype since inconsistent OID
     * among coordinators and data nodes
     */
    READ_TYPEINFO_FIELD(vartype);
    READ_DONE();
}

/*
 * _readRownum
 */
static Rownum* _readRownum(void)
{
    READ_LOCALS(Rownum);

    READ_OID_FIELD(rownumcollid);
    READ_LOCATION_FIELD(location);
    READ_DONE();
}

/*
 * _readConst
 */
static Const* _readConst(void)
{
    READ_LOCALS(Const);

    READ_OID_FIELD(consttype);
    READ_INT_FIELD(consttypmod);
    READ_OID_FIELD(constcollid);
    READ_INT_FIELD(constlen);
    READ_BOOL_FIELD(constbyval);
    READ_BOOL_FIELD(constisnull);
    READ_BOOL_FIELD(ismaxvalue);
    READ_LOCATION_FIELD(location);

    /*
     * WARNING:  it have been modified it too times, for forward compatibility
     * , we have to blame_outConst and make sure that all historical versions
     * can be decode correctly
     */

    /*
     * try to decode data type oid for new version, in new version, data type
     * name string is moved to the front of "constvalue"
     */
    READ_TYPEINFO_FIELD(consttype);

    IF_EXIST(constvalue) {
        token = pg_strtok(&length); /* skip string "constvalue" */
        if (local_node->ismaxvalue || local_node->constisnull) {
            /* skip "<MAXVALUE>" / "<>" */
            token = pg_strtok(&length);
        } else IF_EXIST(udftypevalue) {
                Oid typinput;
                Oid typioparam;
                Oid typid = local_node->consttype;
                int32 typmode = local_node->consttypmod;
                char* valueStr = NULL;

                token = pg_strtok(&length); /* skip string "udftypevalue" */
                token = pg_strtok(&length); /* read value string */
                valueStr = nullable_string(token, length);

                /* translate data string to Datum */
                getTypeInputInfo(typid, &typinput, &typioparam);
                local_node->constvalue = OidInputFunctionCall(typinput, valueStr, typioparam, typmode);
        } else {
            /*
             * 1. compatible with previous versions if oid >= FirstBootstrapObjectId
             * 2. for internel type with oid < FirstBootstrapObjectId
             */
            local_node->constvalue = readDatum(local_node->constbyval);
        }
    }

    /* read conttype again to compatible with previous versions */
    READ_TYPEINFO_FIELD(consttype);
    READ_CFGINFO_FIELD(consttype, constvalue);

    IF_EXIST(cursor_data) {
        token = pg_strtok(&length); /* skip white space */
        _readCursorData(&local_node->cursor_data);
    }

    READ_DONE();
}

/*
 * _readParam
 */
static Param* _readParam(void)
{
    READ_LOCALS(Param);

    READ_ENUM_FIELD(paramkind, ParamKind);
    READ_INT_FIELD(paramid);
    READ_OID_FIELD(paramtype);
    READ_INT_FIELD(paramtypmod);
    READ_OID_FIELD(paramcollid);
    READ_LOCATION_FIELD(location);

    READ_TYPEINFO_FIELD(paramtype);
    IF_EXIST(tableOfIndexType)
    {
        READ_OID_FIELD(tableOfIndexType);
    }
    IF_EXIST(recordVarTypOid)
    {
        READ_OID_FIELD(recordVarTypOid);
    }
    READ_DONE();
}

/*
 * _readAggref
 */
static Aggref* _readAggref(void)
{
    READ_LOCALS(Aggref);
    READ_OID_FIELD(aggfnoid);
#ifdef ENABLE_MULTIPLE_NODES
    token = pg_strtok(&length, false); /* name: pronamespace */
    if (token != NULL && (0 == memcmp(token, ":pronamespace", strlen(":pronamespace")))) {
        // shipping out for dn, oid will be different on dn, so proname and pronamespace string will be must.
        char* proname = NULL;
        char* pronamespace = NULL;
        char* rettypenamespace = NULL;
        char* rettypename = NULL;
        Oid namespace_oid = 0;
        Oid type_namespace_oid = 0;
        Oid rettype = 0;
        int nargs = 0;
        Oid* argtypes = NULL;

        token = pg_strtok(&length); /* skip name: pronamespace */
        token = pg_strtok(&length); /* get pronamespace value */
        pronamespace = nullable_string(token, length);
        namespace_oid = get_namespace_oid(pronamespace, false);
        token = pg_strtok(&length); /* skip name: proname */
        token = pg_strtok(&length); /* get proname value */
        proname = nullable_string(token, length);

        token = pg_strtok(&length); /* skip name: rettypenamespace */
        token = pg_strtok(&length); /* get rettypenamespace value */
        rettypenamespace = nullable_string(token, length);
        type_namespace_oid = get_namespace_oid(rettypenamespace, false);

        token = pg_strtok(&length); /* skip name: rettypename */
        token = pg_strtok(&length); /* get rettypename value */
        rettypename = nullable_string(token, length);

        rettype = get_typeoid(type_namespace_oid, rettypename);

        token = pg_strtok(&length); /* skip name: pronargs */
        token = pg_strtok(&length); /* get pronargs value */
        nargs = atoi(token);

        argtypes = (Oid*)palloc(nargs * sizeof(Oid));
        token = pg_strtok(&length); /* skip name: proargs */
        for (int i = 0; i < nargs; i++) {
            char* argtypenamespace = NULL;
            char* argtypename = NULL;
            Oid arg_type_namespace_oid = 0;
            Oid argtype = 0;
            token = pg_strtok(&length); /* get argtypenamespace value */
            argtypenamespace = nullable_string(token, length);
            arg_type_namespace_oid = get_namespace_oid(argtypenamespace, false);

            token = pg_strtok(&length); /* get argtypename value */
            argtypename = nullable_string(token, length);

            argtype = get_typeoid(arg_type_namespace_oid, argtypename);
            argtypes[i] = argtype;
        }
        local_node->aggfnoid = get_func_oid_ext(proname, namespace_oid, rettype, nargs, argtypes);
    }
#endif /* ENABLE_MULTIPLE_NODES */
    READ_OID_FIELD(aggtype);
#ifdef PGXC
    READ_OID_FIELD(aggtrantype);
    READ_BOOL_FIELD(agghas_collectfn);
    READ_INT_FIELD(aggstage);
#endif /* PGXC */
    READ_OID_FIELD(aggcollid);
    READ_OID_FIELD(inputcollid);
    IF_EXIST(aggdirectargs)
    {
        READ_NODE_FIELD(aggdirectargs);
    }
    READ_NODE_FIELD(args);
    READ_NODE_FIELD(aggorder);
    READ_NODE_FIELD(aggdistinct);
    READ_BOOL_FIELD(aggstar);
    IF_EXIST(aggkind)
    {
        READ_CHAR_FIELD(aggkind);
    }
    IF_EXIST(aggvariadic)
    {
        READ_BOOL_FIELD(aggvariadic);
    }
    READ_UINT_FIELD(agglevelsup);
    READ_LOCATION_FIELD(location);
    READ_TYPEINFO_FIELD(aggtype);
    READ_TYPEINFO_FIELD(aggtrantype);
    READ_DONE();
}

/*
 * _readGroupingFunc
 */
static GroupingFunc* _readGroupingFunc(void)
{
    READ_LOCALS(GroupingFunc);

    READ_NODE_FIELD(args);
    READ_NODE_FIELD(refs);
    READ_NODE_FIELD(cols);
    READ_UINT_FIELD(agglevelsup);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

static GroupingId* _readGroupingId(void)
{
    READ_LOCALS_NO_FIELDS(GroupingId);

    READ_END();
}

/*
 * _readWindowFunc
 */
static WindowFunc* _readWindowFunc(void)
{
    READ_LOCALS(WindowFunc);

    READ_OID_FIELD(winfnoid);
    READ_OID_FIELD(wintype);
    READ_OID_FIELD(wincollid);
    READ_OID_FIELD(inputcollid);
    READ_NODE_FIELD(args);
    READ_UINT_FIELD(winref);
    READ_BOOL_FIELD(winstar);
    READ_BOOL_FIELD(winagg);
    READ_LOCATION_FIELD(location);

    READ_TYPEINFO_FIELD(wintype);
    READ_FUNCINFO_FIELD(winfnoid);

    READ_DONE();
}

/*
 * _readArrayRef
 */
static ArrayRef* _readArrayRef(void)
{
    READ_LOCALS(ArrayRef);

    READ_OID_FIELD(refarraytype);
    READ_OID_FIELD(refelemtype);
    READ_INT_FIELD(reftypmod);
    READ_OID_FIELD(refcollid);
    READ_NODE_FIELD(refupperindexpr);
    READ_NODE_FIELD(reflowerindexpr);
    READ_NODE_FIELD(refexpr);
    READ_NODE_FIELD(refassgnexpr);

    READ_TYPEINFO_FIELD(refarraytype);
    READ_TYPEINFO_FIELD(refelemtype);
    READ_DONE();
}

/*
 * _readFuncExpr
 */
static FuncExpr* _readFuncExpr(void)
{
    READ_LOCALS(FuncExpr);

    READ_OID_FIELD(funcid);
    READ_OID_FIELD(funcresulttype);
    IF_EXIST(funcresulttype_orig) {
        READ_INT_FIELD(funcresulttype_orig);
    }
    READ_BOOL_FIELD(funcretset);
    READ_ENUM_FIELD(funcformat, CoercionForm);
    READ_OID_FIELD(funccollid);
    READ_OID_FIELD(inputcollid);
    READ_NODE_FIELD(args);
    token = pg_strtok(&length);
    char* fieldName = nullable_string(token, length);

    if (fieldName != NULL && pg_strcasecmp(fieldName, ":seqName") == 0 && local_node->funcid == NEXTVALFUNCOID) {
        char* seqName = NULL;
        char* seqNamespace = NULL;
        token = pg_strtok(&length);
        seqName = nullable_string(token, length);

        if (seqName == NULL) {
            ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("NULL seqName for nextval()")));
        }

        token = pg_strtok(&length);
        token = pg_strtok(&length);
        seqNamespace = nullable_string(token, length);

        if (seqNamespace == NULL) {
            Assert(false);
            ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("NULL seqNamespace for nextval()")));
        }

        if (IS_DATANODE_BUT_NOT_SINGLENODE && !skip_read_extern_fields) {

            Oid seqid = get_valid_relname_relid(seqNamespace, seqName, true);
            Const* firstArg = (Const*)linitial(local_node->args);
            if (OidIsValid(seqid)) {
                if (firstArg != NULL) {
                    firstArg->constvalue = ObjectIdGetDatum(seqid);
                }
            } else {
                ereport(ERROR, (errmodule(MOD_OPT), errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("Cannot identify sequence %s.%s while deserializing field.", seqNamespace, seqName),
                    errdetail("Sequence with oid %u or its namespace may be renamed",
                        DatumGetObjectId(firstArg->constvalue)),
                    errhint("Please rebuild column defalt expression, views etc. that are related to this sequence"),
                    errcause("Object renamed after recorded as nodetree."), erraction("Rebuild relevant object.")));
            }
        }
        pfree_ext(seqName);
        pfree_ext(seqNamespace);
        READ_LOCATION_FIELD(location);
    } else {
        token = pg_strtok(&length);
        local_node->location = atoi(token);
    }

    IF_EXIST(refSynOid) {
        READ_OID_FIELD(refSynOid);
    }

    READ_TYPEINFO_FIELD(funcresulttype);
    READ_FUNCINFO_FIELD(funcid);
    /*
     * Same reason as above, get synOid for distribution plan.
     */
    READ_SYNINFO_FILED(refSynOid);

    READ_DONE();
}

/*
 * _readNamedArgExpr
 */
static NamedArgExpr* _readNamedArgExpr(void)
{
    READ_LOCALS(NamedArgExpr);

    READ_NODE_FIELD(arg);
    READ_STRING_FIELD(name);
    READ_INT_FIELD(argnumber);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readOpExpr
 */
static OpExpr* _readOpExpr(void)
{
    READ_LOCALS(OpExpr);

    READ_EXPR_FIELD();
}

/*
 * _readDistinctExpr
 */
static DistinctExpr* _readDistinctExpr(void)
{
    READ_LOCALS(DistinctExpr);

    READ_EXPR_FIELD();
}

/*
 * _readNullIfExpr
 */
static NullIfExpr* _readNullIfExpr(void)
{
    READ_LOCALS(NullIfExpr);

    READ_EXPR_FIELD();
}

/*
 * _readScalarArrayOpExpr
 */
static ScalarArrayOpExpr* _readScalarArrayOpExpr(void)
{
    READ_LOCALS(ScalarArrayOpExpr);

    READ_OID_FIELD(opno);
    READ_OPINFO_FIELD(opno);
    READ_OID_FIELD(opfuncid);
    READ_FUNCINFO_FIELD(opfuncid);

    /*
     * The opfuncid is stored in the textual format primarily for debugging
     * and documentation reasons.  We want to always read it as zero to force
     * it to be re-looked-up in the pg_operator entry.	This ensures that
     * stored rules don't have hidden dependencies on operators' functions.
     * (We don't currently support an ALTER OPERATOR command, but might
     * someday.)
     */

    // Until PteroDB supports ALTER OPERATOR let's comment this to make
    // Plan shipping work
    READ_BOOL_FIELD(useOr);
    READ_OID_FIELD(inputcollid);
    READ_NODE_FIELD(args);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readBoolExpr
 */
static BoolExpr* _readBoolExpr(void)
{
    READ_LOCALS(BoolExpr);

    /* do-it-yourself enum representation */
    token = pg_strtok(&length); /* skip :boolop */
    token = pg_strtok(&length); /* get field value */
    if (strncmp(token, "and", 3) == 0)
        local_node->boolop = AND_EXPR;
    else if (strncmp(token, "or", 2) == 0)
        local_node->boolop = OR_EXPR;
    else if (strncmp(token, "not", 3) == 0)
        local_node->boolop = NOT_EXPR;
    else
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unrecognized boolop \"%.*s\"", length, token)));

    READ_NODE_FIELD(args);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readSubLink
 */
static SubLink* _readSubLink(void)
{
    READ_LOCALS(SubLink);

    READ_ENUM_FIELD(subLinkType, SubLinkType);
    READ_NODE_FIELD(testexpr);
    READ_NODE_FIELD(operName);
    READ_NODE_FIELD(subselect);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readMergeStmt
 */
static MergeStmt* _readMergeStmt(void)
{
    READ_LOCALS(MergeStmt);

    READ_NODE_FIELD(relation);
    READ_NODE_FIELD(source_relation);
    READ_NODE_FIELD(join_condition);
    READ_NODE_FIELD(mergeWhenClauses);
    READ_BOOL_FIELD(is_insert_update);
    READ_NODE_FIELD(insert_stmt);
    IF_EXIST(hintState) {
        READ_NODE_FIELD(hintState);
    };

    READ_DONE();
}

/*
 * _readSubPlan is not needed since it doesn't appear in stored rules.
 */

/*
 * _readFieldSelect
 */
static FieldSelect* _readFieldSelect(void)
{
    READ_LOCALS(FieldSelect);

    READ_NODE_FIELD(arg);
    READ_INT_FIELD(fieldnum);
    READ_OID_FIELD(resulttype);
    READ_INT_FIELD(resulttypmod);
    READ_OID_FIELD(resultcollid);

    READ_TYPEINFO_FIELD(resulttype);

    READ_DONE();
}

/*
 * _readFieldStore
 */
static FieldStore* _readFieldStore(void)
{
    READ_LOCALS(FieldStore);

    READ_NODE_FIELD(arg);
    READ_NODE_FIELD(newvals);
    READ_NODE_FIELD(fieldnums);
    READ_OID_FIELD(resulttype);

    READ_TYPEINFO_FIELD(resulttype);

    READ_DONE();
}

/*
 * _readRelabelType
 */
static RelabelType* _readRelabelType(void)
{
    READ_LOCALS(RelabelType);

    READ_NODE_FIELD(arg);
    READ_OID_FIELD(resulttype);
    READ_INT_FIELD(resulttypmod);
    READ_OID_FIELD(resultcollid);
    READ_ENUM_FIELD(relabelformat, CoercionForm);
    READ_LOCATION_FIELD(location);

    READ_TYPEINFO_FIELD(resulttype);

    READ_DONE();
}

/*
 * _readCoerceViaIO
 */
static CoerceViaIO* _readCoerceViaIO(void)
{
    READ_LOCALS(CoerceViaIO);

    READ_NODE_FIELD(arg);
    READ_OID_FIELD(resulttype);
    READ_OID_FIELD(resultcollid);
    READ_ENUM_FIELD(coerceformat, CoercionForm);
    READ_LOCATION_FIELD(location);

    READ_TYPEINFO_FIELD(resulttype);

    READ_DONE();
}

/*
 * _readArrayCoerceExpr
 */
static ArrayCoerceExpr* _readArrayCoerceExpr(void)
{
    READ_LOCALS(ArrayCoerceExpr);

    READ_NODE_FIELD(arg);
    READ_OID_FIELD(elemfuncid);
    READ_OID_FIELD(resulttype);
    READ_INT_FIELD(resulttypmod);
    READ_OID_FIELD(resultcollid);
    READ_BOOL_FIELD(isExplicit);
    READ_ENUM_FIELD(coerceformat, CoercionForm);
    READ_LOCATION_FIELD(location);

    READ_TYPEINFO_FIELD(resulttype);
    READ_FUNCINFO_FIELD(elemfuncid);

    READ_DONE();
}

/*
 * _readConvertRowtypeExpr
 */
static ConvertRowtypeExpr* _readConvertRowtypeExpr(void)
{
    READ_LOCALS(ConvertRowtypeExpr);

    READ_NODE_FIELD(arg);
    READ_OID_FIELD(resulttype);
    READ_ENUM_FIELD(convertformat, CoercionForm);
    READ_LOCATION_FIELD(location);

    READ_TYPEINFO_FIELD(resulttype);

    READ_DONE();
}

/*
 * _readCollateExpr
 */
static CollateExpr* _readCollateExpr(void)
{
    READ_LOCALS(CollateExpr);

    READ_NODE_FIELD(arg);
    READ_OID_FIELD(collOid);
    READ_LOCATION_FIELD(location);

    // Note that this section must be modified if _outCollateExpr changes
    if (local_node->collOid >= FirstBootstrapObjectId) {
        IF_EXIST(collname) {
            token = pg_strtok(&length);
            token = pg_strtok(&length);
            char* collname = nullable_string(token, length);
            Assert(collname != NULL);
            if (!IS_PGXC_COORDINATOR) {
                List* collnameList = list_make1(makeString(collname));
                local_node->collOid = get_collation_oid(collnameList, false);
                list_free_ext(collnameList);
            }
            if (NULL != collname)
                pfree_ext(collname);
        }
    }

    READ_DONE();
}

/*
 * _readCaseExpr
 */
static CaseExpr* _readCaseExpr(void)
{
    READ_LOCALS(CaseExpr);

    READ_OID_FIELD(casetype);
    READ_OID_FIELD(casecollid);
    READ_NODE_FIELD(arg);
    READ_NODE_FIELD(args);
    READ_NODE_FIELD(defresult);
    READ_LOCATION_FIELD(location);

    READ_TYPEINFO_FIELD(casetype);

    READ_DONE();
}

/*
 * _readCaseWhen
 */
static CaseWhen* _readCaseWhen(void)
{
    READ_LOCALS(CaseWhen);

    READ_NODE_FIELD(expr);
    READ_NODE_FIELD(result);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/*
 * _readCaseTestExpr
 */
static CaseTestExpr* _readCaseTestExpr(void)
{
    READ_LOCALS(CaseTestExpr);

    READ_OID_FIELD(typeId);
    READ_INT_FIELD(typeMod);
    READ_OID_FIELD(collation);

    READ_TYPEINFO_FIELD(typeId);

    READ_DONE();
}

/*
 * _readArrayExpr
 */
static ArrayExpr* _readArrayExpr(void)
{
    READ_LOCALS(ArrayExpr);

    READ_OID_FIELD(array_typeid);
    READ_OID_FIELD(array_collid);
    READ_OID_FIELD(element_typeid);
    READ_NODE_FIELD(elements);
    READ_BOOL_FIELD(multidims);
    READ_LOCATION_FIELD(location);

    READ_TYPEINFO_FIELD(array_typeid);
    READ_TYPEINFO_FIELD(element_typeid);

    READ_DONE();
}

/*
 * _readRowExpr
 */
static RowExpr* _readRowExpr(void)
{
    READ_LOCALS(RowExpr);

    READ_NODE_FIELD(args);
    READ_OID_FIELD(row_typeid);
    READ_ENUM_FIELD(row_format, CoercionForm);
    READ_NODE_FIELD(colnames);
    READ_LOCATION_FIELD(location);

    READ_TYPEINFO_FIELD(row_typeid);

    READ_DONE();
}

/*
 * _readRowCompareExpr
 */
static RowCompareExpr* _readRowCompareExpr(void)
{
    READ_LOCALS(RowCompareExpr);

    READ_ENUM_FIELD(rctype, RowCompareType);
    READ_NODE_FIELD(opnos);
    READ_NODE_FIELD(opfamilies);
    READ_NODE_FIELD(inputcollids);
    READ_NODE_FIELD(largs);
    READ_NODE_FIELD(rargs);

    READ_DONE();
}

/*
 * _readCoalesceExpr
 */
static CoalesceExpr* _readCoalesceExpr(void)
{
    READ_LOCALS(CoalesceExpr);

    READ_OID_FIELD(coalescetype);
    READ_OID_FIELD(coalescecollid);
    READ_NODE_FIELD(args);
    READ_LOCATION_FIELD(location);
    READ_BOOL_FIELD(isnvl);

    READ_TYPEINFO_FIELD(coalescetype);

    READ_DONE();
}

/*
 * _readMinMaxExpr
 */
static MinMaxExpr* _readMinMaxExpr(void)
{
    READ_LOCALS(MinMaxExpr);

    READ_OID_FIELD(minmaxtype);
    READ_OID_FIELD(minmaxcollid);
    READ_OID_FIELD(inputcollid);
    READ_ENUM_FIELD(op, MinMaxOp);
    READ_NODE_FIELD(args);
    READ_LOCATION_FIELD(location);

    READ_TYPEINFO_FIELD(minmaxtype);

    READ_DONE();
}

/*
 * _readXmlExpr
 */
static XmlExpr* _readXmlExpr(void)
{
    READ_LOCALS(XmlExpr);

    READ_ENUM_FIELD(op, XmlExprOp);
    READ_STRING_FIELD(name);
    READ_NODE_FIELD(named_args);
    READ_NODE_FIELD(arg_names);
    READ_NODE_FIELD(args);
    READ_ENUM_FIELD(xmloption, XmlOptionType);
    READ_OID_FIELD(type);
    READ_INT_FIELD(typmod);
    READ_LOCATION_FIELD(location);

    READ_TYPEINFO_FIELD(type);
    READ_DONE();
}

/*
 * _readNullTest
 */
static NullTest* _readNullTest(void)
{
    READ_LOCALS(NullTest);

    READ_NODE_FIELD(arg);
    READ_ENUM_FIELD(nulltesttype, NullTestType);
    READ_BOOL_FIELD(argisrow);

    READ_DONE();
}

/*
 * _readHashFilter
 */
static HashFilter* _readHashFilter(void)
{
    READ_LOCALS(HashFilter);

    READ_NODE_FIELD(arg);
    READ_NODE_FIELD(typeOids);
    READ_NODE_FIELD(nodeList);

    READ_TYPEINFO_LIST(typeOids);

    READ_DONE();
}

/*
 * _readBooleanTest
 */
static BooleanTest* _readBooleanTest(void)
{
    READ_LOCALS(BooleanTest);

    READ_NODE_FIELD(arg);
    READ_ENUM_FIELD(booltesttype, BoolTestType);

    READ_DONE();
}

/*
 * _readCoerceToDomain
 */
static CoerceToDomain* _readCoerceToDomain(void)
{
    READ_LOCALS(CoerceToDomain);

    READ_NODE_FIELD(arg);
    READ_OID_FIELD(resulttype);
    READ_INT_FIELD(resulttypmod);
    READ_OID_FIELD(resultcollid);
    READ_ENUM_FIELD(coercionformat, CoercionForm);
    READ_LOCATION_FIELD(location);

    READ_TYPEINFO_FIELD(resulttype);

    READ_DONE();
}

/*
 * _readCoerceToDomainValue
 */
static CoerceToDomainValue* _readCoerceToDomainValue(void)
{
    READ_LOCALS(CoerceToDomainValue);

    READ_OID_FIELD(typeId);
    READ_INT_FIELD(typeMod);
    READ_OID_FIELD(collation);
    READ_LOCATION_FIELD(location);

    READ_TYPEINFO_FIELD(typeId);

    READ_DONE();
}

/*
 * _readSetToDefault
 */
static SetToDefault* _readSetToDefault(void)
{
    READ_LOCALS(SetToDefault);

    READ_OID_FIELD(typeId);
    READ_INT_FIELD(typeMod);
    READ_OID_FIELD(collation);
    READ_LOCATION_FIELD(location);

    READ_TYPEINFO_FIELD(typeId);

    READ_DONE();
}

/*
 * _readCurrentOfExpr
 */
static CurrentOfExpr* _readCurrentOfExpr(void)
{
    READ_LOCALS(CurrentOfExpr);

    READ_UINT_FIELD(cvarno);
    READ_STRING_FIELD(cursor_name);
    READ_INT_FIELD(cursor_param);

    READ_DONE();
}

/*
 * _readTargetEntry
 */
static TargetEntry* _readTargetEntry(void)
{
    READ_LOCALS(TargetEntry);

    READ_NODE_FIELD(expr);
    READ_INT_FIELD(resno);
    READ_STRING_FIELD(resname);
    READ_UINT_FIELD(ressortgroupref);
    READ_OID_FIELD(resorigtbl);
    READ_INT_FIELD(resorigcol);
    READ_BOOL_FIELD(resjunk);

    READ_DONE();
}

/*
 * _readPseudoTargetEntry
 */
static PseudoTargetEntry* _readPseudoTargetEntry(void)
{
    READ_LOCALS(PseudoTargetEntry);

    READ_NODE_FIELD(tle);
    READ_NODE_FIELD(srctle);

    READ_DONE();
}

/*
 * _readRangeTblRef
 */
static RangeTblRef* _readRangeTblRef(void)
{
    READ_LOCALS(RangeTblRef);

    READ_INT_FIELD(rtindex);

    READ_DONE();
}

/*
 * _readJoinExpr
 */
static JoinExpr* _readJoinExpr(void)
{
    READ_LOCALS(JoinExpr);

    READ_ENUM_FIELD(jointype, JoinType);
    READ_BOOL_FIELD(isNatural);
    READ_NODE_FIELD(larg);
    READ_NODE_FIELD(rarg);
    READ_NODE_FIELD(usingClause);
    READ_NODE_FIELD(quals);
    READ_NODE_FIELD(alias);
    READ_INT_FIELD(rtindex);

    READ_DONE();
}

/*
 * _readFromExpr
 */
static FromExpr* _readFromExpr(void)
{
    READ_LOCALS(FromExpr);

    READ_NODE_FIELD(fromlist);
    READ_NODE_FIELD(quals);

    READ_DONE();
}

/*
 * _readMergeAction
 */
static MergeAction* _readMergeAction(void)
{
    READ_LOCALS(MergeAction);

    READ_BOOL_FIELD(matched);
    READ_NODE_FIELD(qual);
    READ_ENUM_FIELD(commandType, CmdType);
    READ_NODE_FIELD(targetList);
    READ_NODE_FIELD(pulluped_targetList);
    READ_DONE();
}

/*
 * Stuff from parsenodes.h.
 */

/*
 * _readRangeTblEntry
 */
static RangeTblEntry* _readRangeTblEntry(void)
{
    READ_LOCALS(RangeTblEntry);

    /* put alias + eref first to make dump more legible */
    READ_NODE_FIELD(alias);
    READ_NODE_FIELD(eref);
    READ_ENUM_FIELD(rtekind, RTEKind);
#ifdef PGXC
    READ_STRING_FIELD(relname);
    READ_NODE_FIELD(partAttrNum);
#endif

    token = pg_strtok(&length, false);
    if (token != NULL && (0 == memcmp(token, ":mainRelName", strlen(":mainRelName")))) {
        READ_STRING_FIELD(mainRelName);
    }

    token = pg_strtok(&length, false);
    if (token != NULL && (0 == memcmp(token, ":mainRelNameSpace", strlen(":mainRelNameSpace")))) {
        READ_STRING_FIELD(mainRelNameSpace);
    }

    switch (local_node->rtekind) {
        case RTE_RELATION:
            READ_OID_FIELD(relid);
            READ_CHAR_FIELD(relkind);
            READ_BOOL_FIELD(isResultRel);
            token = pg_strtok(&length, false);
            if (token != NULL && (0 == memcmp(token, ":tablesample", strlen(":tablesample")))) {
                READ_NODE_FIELD(tablesample);
            }

            IF_EXIST(timecapsule) {
                READ_NODE_FIELD(timecapsule);
            }

            READ_OID_FIELD(partitionOid);
            READ_BOOL_FIELD(isContainPartition);
            IF_EXIST(subpartitionOid) {
                READ_OID_FIELD(subpartitionOid);
            }
            IF_EXIST(isContainSubPartition) {
                READ_BOOL_FIELD(isContainSubPartition);
            }

            IF_EXIST(refSynOid)
            {
                READ_OID_FIELD(refSynOid);
            }

            READ_BOOL_FIELD(ispartrel);

            token = pg_strtok(&length, false);
            if (token != NULL && (0 == memcmp(token, ":ignoreResetRelid", strlen(":ignoreResetRelid")))) {
                READ_BOOL_FIELD(ignoreResetRelid);
            }

            READ_NODE_FIELD(pname);
            READ_NODE_FIELD(partid_list);
            READ_NODE_FIELD(plist);
            /*
             * Note: The Oid shipped(in plan) is invalid here
             * We need to get the Oid on this node.
             */
            if (local_node->relid >= FirstBootstrapObjectId && !local_node->ignoreResetRelid) {
                char* relname = NULL;
                char* relnamespace = NULL;
                Oid deltaRelId = InvalidOid;
                if (local_node->mainRelName != NULL) {
                    char* mainRelName = pstrdup(local_node->mainRelName);
                    char* mainRelNameSpace = pstrdup(local_node->mainRelNameSpace);

                    Oid relid = get_valid_relname_relid(mainRelNameSpace, mainRelName);

                    Relation mianRel = relation_open(relid, AccessShareLock);

                    deltaRelId = RelationGetDeltaRelId(mianRel);

                    relation_close(mianRel, AccessShareLock);
                    pfree_ext(mainRelName);
                    pfree_ext(mainRelNameSpace);

                } else {
                    IF_EXIST(relname) {
                        token = pg_strtok(&length);
                        token = pg_strtok(&length);
                        relname = nullable_string(token, length);
                        if (relname == NULL) {
                            Assert(false);
                            ereport(ERROR,
                                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                                    errmsg("NULL relname for RTE %u found", local_node->relid)));
                        }
                    }
                    IF_EXIST(relnamespace) {
                        token = pg_strtok(&length);
                        token = pg_strtok(&length);
                        relnamespace = nullable_string(token, length);
                        if (relnamespace == NULL) {
                            Assert(false);
                            ereport(ERROR,
                                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                                    errmsg("NULL relnamespace for RTE %u found", local_node->relid)));
                        }
                    }
                    /*
                     * Same reason as above, get synOid for distribution plan.
                     */
                    READ_SYNINFO_FILED(refSynOid);
                }

                /*
                 * 1. As get oid here is for distributed plan, we don't need get oid in restoremode which just for
                 * single node execution.
                 * 2. Get oid through relnamespace and relname is not reliable as this messages stored in nodes will not
                 * update properly, like nodes messages in pg_rewrite for views.
                 */
#ifdef ENABLE_MULTIPLE_NODES                 
                if (!IS_PGXC_COORDINATOR && !isRestoreMode) {
                    Oid relid = InvalidOid;
                    if (!OidIsValid(deltaRelId) && relname != NULL && relnamespace != NULL) {
                        relid = get_valid_relname_relid(relnamespace, relname);

                        pfree_ext(relname);
                        pfree_ext(relnamespace);
                    } else {
                        relid = deltaRelId;
                    }

                    if (OidIsValid(relid)) {
                        local_node->relid = relid;
                    }
                }
#endif                
            }

            break;
        case RTE_SUBQUERY:
            READ_NODE_FIELD(subquery);
            READ_BOOL_FIELD(security_barrier);
            break;
        case RTE_JOIN:
            READ_ENUM_FIELD(jointype, JoinType);
            READ_NODE_FIELD(joinaliasvars);
            break;
        case RTE_FUNCTION:
            READ_NODE_FIELD(funcexpr);
            READ_NODE_FIELD(funccoltypes);
            READ_NODE_FIELD(funccoltypmods);
            READ_NODE_FIELD(funccolcollations);

            READ_TYPEINFO_LIST(funccoltypes);
            break;
        case RTE_VALUES:
            READ_NODE_FIELD(values_lists);
            READ_NODE_FIELD(values_collations);
            break;
        case RTE_CTE:
            READ_STRING_FIELD(ctename);
            READ_UINT_FIELD(ctelevelsup);
            READ_BOOL_FIELD(self_reference);
            IF_EXIST(cterecursive) {
                READ_BOOL_FIELD(cterecursive);
            }
            READ_NODE_FIELD(ctecoltypes);
            READ_NODE_FIELD(ctecoltypmods);
            READ_NODE_FIELD(ctecolcollations);

            IF_EXIST(swConverted) {
                READ_BOOL_FIELD(swConverted);
            }
            IF_EXIST(origin_index) {
                READ_NODE_FIELD(origin_index);
            }
            IF_EXIST(swAborted) {
                READ_BOOL_FIELD(swAborted);
            }
            IF_EXIST(swSubExist) {
                READ_BOOL_FIELD(swSubExist);
            }

            READ_TYPEINFO_LIST(ctecoltypes);
            break;
#ifdef PGXC
        case RTE_REMOTE_DUMMY:
            /* Nothing to do */
            break;
#endif /* PGXC */
        case RTE_RESULT:
            /* no extra fields */
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized RTE kind: %d", (int)local_node->rtekind)));
            break;
    }

    IF_EXIST(lateral)
    {
        READ_BOOL_FIELD(lateral);
    }
    READ_BOOL_FIELD(inh);
    READ_BOOL_FIELD(inFromCl);
    READ_UINT_FIELD(requiredPerms);
    READ_OID_FIELD(checkAsUser);
    READ_BITMAPSET_FIELD(selectedCols);
    READ_BITMAPSET_FIELD(modifiedCols);

    IF_EXIST(insertedCols) {
        READ_BITMAPSET_FIELD(insertedCols);
    }

    IF_EXIST(updatedCols) {
        READ_BITMAPSET_FIELD(updatedCols);
    }

    READ_ENUM_FIELD(orientation, RelOrientation);

    IF_EXIST(securityQuals) {
        READ_NODE_FIELD(securityQuals);
    }

    IF_EXIST(subquery_pull_up) {
        READ_BOOL_FIELD(subquery_pull_up);
    }

    IF_EXIST(correlated_with_recursive_cte) {
        READ_BOOL_FIELD(correlated_with_recursive_cte);
    }

    IF_EXIST(relhasbucket) {
        READ_BOOL_FIELD(relhasbucket);
    }

    IF_EXIST(isbucket) {
        READ_BOOL_FIELD(isbucket);
    }

    IF_EXIST(bucketmapsize) {
        READ_INT_FIELD(bucketmapsize);
    }

    IF_EXIST(buckets) {
        READ_NODE_FIELD(buckets);
    }

    IF_EXIST(isexcluded){
        READ_BOOL_FIELD(isexcluded);
    }

    IF_EXIST(sublink_pull_up) {
        READ_BOOL_FIELD(sublink_pull_up);
    }

    IF_EXIST(is_ustore) {
        READ_BOOL_FIELD(is_ustore);
    }

    IF_EXIST(extraUpdatedCols) {
        READ_BITMAPSET_FIELD(extraUpdatedCols);
    }

    READ_DONE();
}

// TO-DO
//
static Plan* _readPlan(Plan* local_node)
{
    READ_LOCALS_NULL(Plan);
    READ_TEMP_LOCALS();

    READ_INT_FIELD(plan_node_id);
    READ_INT_FIELD(parent_node_id);
    READ_ENUM_FIELD(exec_type, RemoteQueryExecType);
    READ_FLOAT_FIELD(startup_cost);
    READ_FLOAT_FIELD(total_cost);
    READ_FLOAT_FIELD(plan_rows);
    READ_FLOAT_FIELD(multiple);
    READ_INT_FIELD(plan_width);
    READ_NODE_FIELD(targetlist);
    READ_NODE_FIELD(qual);
    READ_NODE_FIELD(lefttree);
    READ_NODE_FIELD(righttree);
    READ_BOOL_FIELD(ispwj);
    READ_INT_FIELD(paramno);
    IF_EXIST(subparamno) {
        READ_INT_FIELD(subparamno);
    }
    READ_NODE_FIELD(initPlan);
    READ_NODE_FIELD(distributed_keys);
    READ_NODE_FIELD(exec_nodes);
    READ_BITMAPSET_FIELD(extParam);
    READ_BITMAPSET_FIELD(allParam);
    READ_BOOL_FIELD(vec_output);
    READ_BOOL_FIELD(hasUniqueResults);
    READ_BOOL_FIELD(isDeltaTable);
    READ_INT_FIELD(operatorMemKB[0]);
    READ_INT_FIELD(operatorMemKB[1]);
    READ_INT_FIELD(operatorMaxMem);
    READ_BOOL_FIELD(parallel_enabled);
    READ_BOOL_FIELD(hasHashFilter);
    READ_NODE_FIELD(var_list);
    READ_NODE_FIELD(filterIndexList);
    READ_INT_FIELD(dop);
    READ_INT_FIELD(recursive_union_plan_nodeid);
    READ_BOOL_FIELD(recursive_union_controller);
    READ_INT_FIELD(control_plan_nodeid);
    READ_BOOL_FIELD(is_sync_plannode);
    if (t_thrd.proc->workingVersionNum >= ML_OPT_MODEL_VERSION_NUM) {
        READ_FLOAT_FIELD(pred_rows);
        READ_FLOAT_FIELD(pred_startup_time);
        READ_FLOAT_FIELD(pred_total_time);
        READ_LONG_FIELD(pred_max_memory);
    }
    READ_DONE();
}

static SubPlan* _readSubPlan(SubPlan* local_node)
{
    READ_LOCALS_NULL(SubPlan);
    READ_TEMP_LOCALS();

    READ_ENUM_FIELD(subLinkType, SubLinkType);
    READ_NODE_FIELD(testexpr);
    READ_NODE_FIELD(paramIds);
    READ_INT_FIELD(plan_id);
    READ_STRING_FIELD(plan_name);
    READ_OID_FIELD(firstColType);
    READ_INT_FIELD(firstColTypmod);
    READ_OID_FIELD(firstColCollation);
    READ_BOOL_FIELD(useHashTable);
    READ_BOOL_FIELD(unknownEqFalse);
    READ_NODE_FIELD(setParam);
    READ_NODE_FIELD(parParam);
    READ_NODE_FIELD(args);
    READ_FLOAT_FIELD(startup_cost);
    READ_FLOAT_FIELD(per_call_cost);

    READ_TYPEINFO_FIELD(firstColType);
    READ_DONE();
}

static LockRows* _readLockRows(LockRows* local_node)
{
    READ_LOCALS_NULL(LockRows);
    READ_TEMP_LOCALS();

    // Read Plan
    _readPlan(&local_node->plan);

    READ_NODE_FIELD(rowMarks);
    READ_INT_FIELD(epqParam);

    READ_DONE();
}

static Limit* _readLimit(Limit* local_node)
{
    READ_LOCALS_NULL(Limit);
    READ_TEMP_LOCALS();

    // Read Plan
    _readPlan(&local_node->plan);

    READ_NODE_FIELD(limitOffset);
    READ_NODE_FIELD(limitCount);

    READ_DONE();
}

static Agg* _readAgg(Agg* local_node)
{
    READ_LOCALS_NULL(Agg);
    READ_TEMP_LOCALS();

    // Read Plan
    _readPlan(&local_node->plan);

    READ_ENUM_FIELD(aggstrategy, AggStrategy);
    READ_INT_FIELD(numCols);
    READ_ATTR_ARRAY(grpColIdx, numCols);
    READ_OPERATOROID_ARRAY(grpOperators, numCols);

    READ_LONG_FIELD(numGroups);
    READ_NODE_FIELD(groupingSets);
    READ_NODE_FIELD(chain);
    READ_BOOL_FIELD(is_final);
    READ_BOOL_FIELD(single_node);
    READ_BITMAPSET_FIELD(aggParams);
    read_mem_info(&local_node->mem_info);
    READ_BOOL_FIELD(is_sonichash);
    READ_BOOL_FIELD(is_dummy);
    READ_UINT_FIELD(skew_optimize);

    IF_EXIST(unique_check) {
        READ_BOOL_FIELD(unique_check);
    }

    READ_DONE();
}

static WindowAgg* _readWindowAgg(WindowAgg* local_node)
{
    READ_LOCALS_NULL(WindowAgg);
    READ_TEMP_LOCALS();

    _readPlan(&local_node->plan);
    READ_UINT_FIELD(winref);

    READ_INT_FIELD(partNumCols);
    READ_ATTR_ARRAY(partColIdx, partNumCols);
    READ_OPERATOROID_ARRAY(partOperators, partNumCols);
    READ_INT_FIELD(ordNumCols);
    READ_ATTR_ARRAY(ordColIdx, ordNumCols);
    READ_OPERATOROID_ARRAY(ordOperators, ordNumCols);

    READ_INT_FIELD(frameOptions);
    READ_NODE_FIELD(startOffset);
    READ_NODE_FIELD(endOffset);
    read_mem_info(&local_node->mem_info);

    READ_DONE();
}

static Stream* _readStream(Stream* local_node)
{
    READ_LOCALS_NULL(Stream);

    READ_STREAM_FIELD();
}

static BitmapOr* _readBitmapOr(BitmapOr* local_node)
{
    READ_LOCALS_NULL(BitmapOr);
    READ_TEMP_LOCALS();

    _readPlan(&local_node->plan);
    READ_NODE_FIELD(bitmapplans);
    IF_EXIST(is_ustore) {
        READ_BOOL_FIELD(is_ustore);
    }

    READ_DONE();
}

static BitmapAnd* _readBitmapAnd(BitmapAnd* local_node)
{
    READ_LOCALS_NULL(BitmapAnd);
    READ_TEMP_LOCALS();

    _readPlan(&local_node->plan);
    READ_NODE_FIELD(bitmapplans);
    IF_EXIST(is_ustore) {
        READ_BOOL_FIELD(is_ustore);
    }

    READ_DONE();
}

static CStoreIndexOr* _readCStoreIndexOr(CStoreIndexOr* local_node)
{
    READ_LOCALS_NULL(CStoreIndexOr);
    READ_TEMP_LOCALS();

    _readPlan(&local_node->plan);
    READ_NODE_FIELD(bitmapplans);

    READ_DONE();
}

static CStoreIndexAnd* _readCStoreIndexAnd(CStoreIndexAnd* local_node)
{
    READ_LOCALS_NULL(CStoreIndexAnd);
    READ_TEMP_LOCALS();

    _readPlan(&local_node->plan);
    READ_NODE_FIELD(bitmapplans);

    READ_DONE();
}

static PruningResult* _readPruningResult(PruningResult* local_node)
{
    const int num = 92267;
    READ_LOCALS_NULL(PruningResult);
    READ_TEMP_LOCALS();

    READ_ENUM_FIELD(state, PruningResultState);
    /* skip boundary info */
    READ_BITMAPSET_FIELD(bm_rangeSelectedPartitions);
    READ_INT_FIELD(intervalOffset);
    READ_BITMAPSET_FIELD(intervalSelectedPartitions);
    READ_NODE_FIELD(ls_rangeSelectedPartitions);
    IF_EXIST(ls_selectedSubPartitions) {
        READ_NODE_FIELD(ls_selectedSubPartitions);
    }
    if (t_thrd.proc->workingVersionNum >= num) {
        READ_NODE_FIELD(expr);
    }
    IF_EXIST(isPbeSinlePartition) {
        READ_BOOL_FIELD(isPbeSinlePartition);
    }

    READ_DONE();
}

static SubPartitionPruningResult* _readSubPartitionPruningResult(SubPartitionPruningResult* local_node)
{
    READ_LOCALS_NULL(SubPartitionPruningResult);
    READ_TEMP_LOCALS();

    /* skip boundary info */
    READ_INT_FIELD(partSeq);
    READ_BITMAPSET_FIELD(bm_selectedSubPartitions);
    READ_NODE_FIELD(ls_selectedSubPartitions);

    READ_DONE();
}

static BucketInfo* _readBucketInfo(BucketInfo* local_node)
{
    READ_LOCALS_NULL(BucketInfo);
    READ_TEMP_LOCALS();

    READ_NODE_FIELD(buckets);

    READ_DONE();
}

static Scan* _readScan(Scan* local_node)
{
    READ_LOCALS_NULL(Scan);
    READ_TEMP_LOCALS();

    // Read Plan
    _readPlan(&local_node->plan);

    // When deseriliazing on a different node, should we map this too?
    // Remember OIDs are different on different nodes
    READ_UINT_FIELD(scanrelid);
    READ_BOOL_FIELD(isPartTbl);
    READ_INT_FIELD(itrs);
    READ_NODE_FIELD(pruningInfo);

    IF_EXIST(bucketInfo) {
        READ_NODE_FIELD(bucketInfo);
    }

    READ_ENUM_FIELD(partScanDirection, ScanDirection);
    READ_BOOL_FIELD(scan_qual_optimized);
    READ_BOOL_FIELD(predicate_pushdown_optimized);

    token = pg_strtok(&length, false);
    if (0 == memcmp(token, ":tablesample", strlen(":tablesample"))) {
        READ_NODE_FIELD(tablesample);
    }
    read_mem_info(&local_node->mem_info);
    IF_EXIST(scanBatchMode) {
        READ_BOOL_FIELD(scanBatchMode);
    }
    READ_DONE();
}

static PartIterator* _readPartIterator(PartIterator* local_node)
{
    READ_LOCALS_NULL(PartIterator);
    READ_TEMP_LOCALS();

    _readPlan(&local_node->plan);

    READ_ENUM_FIELD(partType, PartitionType);
    READ_INT_FIELD(itrs);
    READ_ENUM_FIELD(direction, ScanDirection);
    READ_NODE_FIELD(param);

    READ_DONE();
}

static PartIteratorParam* _readPartIteratorParam(PartIteratorParam* local_node)
{
    READ_LOCALS_NULL(PartIteratorParam);
    READ_TEMP_LOCALS();

    READ_INT_FIELD(paramno);
    IF_EXIST(subPartParamno) {
        READ_INT_FIELD(subPartParamno);
    }

    READ_DONE();
}

static CteScan* _readCteScan(CteScan* local_node)
{
    READ_LOCALS_NULL(CteScan);
    READ_TEMP_LOCALS();

    _readScan(&local_node->scan);

    READ_INT_FIELD(ctePlanId);
    READ_INT_FIELD(cteParam);

    IF_EXIST(cteRef) {
        READ_NODE_FIELD(cteRef);
    }
    IF_EXIST(internalEntryList) {
        READ_NODE_FIELD(internalEntryList);
    }

    READ_DONE();
}

static ValuesScan* _readValuesScan(ValuesScan* local_node)
{
    READ_LOCALS_NULL(ValuesScan);
    READ_TEMP_LOCALS();

    _readScan(&local_node->scan);

    READ_NODE_FIELD(values_lists);

    READ_DONE();
}

static BitmapHeapScan* _readBitmapHeapScan(BitmapHeapScan* local_node)
{
    READ_LOCALS_NULL(BitmapHeapScan);
    READ_TEMP_LOCALS();

    // Read Scan
    _readScan(&local_node->scan);

    READ_NODE_FIELD(bitmapqualorig);

    READ_DONE();
}

static CStoreIndexHeapScan* _readCStoreIndexHeapScan(CStoreIndexHeapScan* local_node)
{
    READ_LOCALS_NULL(CStoreIndexHeapScan);
    READ_TEMP_LOCALS();

    // Read Scan
    _readScan(&local_node->scan);

    READ_NODE_FIELD(bitmapqualorig);

    READ_DONE();
}

static TidScan* _readTidScan(TidScan* local_node)
{
    READ_LOCALS_NULL(TidScan);
    READ_TEMP_LOCALS();

    _readScan(&local_node->scan);
    READ_NODE_FIELD(tidquals);

    READ_DONE();
}

static IndexOnlyScan* _readIndexOnlyScan(IndexOnlyScan* local_node)
{
    READ_LOCALS_NULL(IndexOnlyScan);
    READ_TEMP_LOCALS();

    _readScan(&local_node->scan);

    READ_OID_FIELD(indexid);
    if (local_node->indexid >= FirstBootstrapObjectId) {
        IF_EXIST(indexname) {
            char *relname, *relnamespace;
            token = pg_strtok(&length);
            token = pg_strtok(&length);
            relname = nullable_string(token, length);
            token = pg_strtok(&length);
            token = pg_strtok(&length);
            relnamespace = nullable_string(token, length);
            if (!IS_PGXC_COORDINATOR)
                local_node->indexid = get_valid_relname_relid(relnamespace, relname);

            pfree_ext(relname);
            pfree_ext(relnamespace);
        }
    }
    READ_NODE_FIELD(indexqual);
    READ_NODE_FIELD(indexorderby);
    READ_NODE_FIELD(indextlist);
    READ_ENUM_FIELD(indexorderdir, ScanDirection);

    READ_DONE();
}

static BitmapIndexScan* _readBitmapIndexScan(BitmapIndexScan* local_node)
{
    READ_LOCALS_NULL(BitmapIndexScan);
    READ_TEMP_LOCALS();

    // Read Scan
    _readScan(&local_node->scan);

    READ_OID_FIELD(indexid);
    READ_NODE_FIELD(indexqual);
    READ_NODE_FIELD(indexqualorig);
#ifdef STREAMPLAN
    // Note: The Oid shipped(in plan) is invalid here
    // We need to get the Oid on this node
    if (local_node->indexid >= FirstBootstrapObjectId) {
        IF_EXIST(indexname) {
            char *indexname, *indexnamespace;

            token = pg_strtok(&length);
            token = pg_strtok(&length);
            indexname = nullable_string(token, length);
            token = pg_strtok(&length);
            token = pg_strtok(&length);
            indexnamespace = nullable_string(token, length);
            if (!IS_PGXC_COORDINATOR)
                local_node->indexid = get_valid_relname_relid(indexnamespace, indexname);

            pfree_ext(indexname);
            pfree_ext(indexnamespace);
        }
    }
#endif  // STREAMPLAN
    IF_EXIST(is_ustore) {
         READ_BOOL_FIELD(is_ustore);
     }
    READ_DONE();
}

static CStoreIndexCtidScan* _readCStoreIndexCtidScan(CStoreIndexCtidScan* local_node)
{
    READ_LOCALS_NULL(CStoreIndexCtidScan);
    READ_TEMP_LOCALS();

    // Read Scan
    _readScan(&local_node->scan);

    READ_OID_FIELD(indexid);
    READ_NODE_FIELD(indexqual);
    READ_NODE_FIELD(cstorequal);
    READ_NODE_FIELD(indexqualorig);
#ifdef STREAMPLAN
    // Note: The Oid shipped(in plan) is invalid here
    // We need to get the Oid on this node
    if (local_node->indexid >= FirstBootstrapObjectId) {
        IF_EXIST(indexname) {
            char *indexname, *indexnamespace;

            token = pg_strtok(&length);
            token = pg_strtok(&length);
            indexname = nullable_string(token, length);
            token = pg_strtok(&length);
            token = pg_strtok(&length);
            indexnamespace = nullable_string(token, length);
            if (!IS_PGXC_COORDINATOR) {
                local_node->indexid = get_valid_relname_relid(indexnamespace, indexname);
            }
            pfree_ext(indexname);
            pfree_ext(indexnamespace);
        }
    }
#endif  // STREAMPLAN
    READ_NODE_FIELD(indextlist);
    READ_DONE();
}

static SubqueryScan* _readSubqueryScan(SubqueryScan* local_node)
{
    READ_LOCALS_NULL(SubqueryScan);
    READ_TEMP_LOCALS();

    // Read Scan
    _readScan(&local_node->scan);

    READ_NODE_FIELD(subplan);

    READ_DONE();
}

static IndexScan* _readIndexScan(IndexScan* local_node)
{
    READ_LOCALS_NULL(IndexScan);
    READ_TEMP_LOCALS();

    // Read Scan
    _readScan(&local_node->scan);

    READ_OID_FIELD(indexid);
#ifdef STREAMPLAN
    // Note: The Oid shipped(in plan) is invalid here
    // We need to get the Oid on this node
    if (local_node->indexid >= FirstBootstrapObjectId) {
        IF_EXIST(indexname) {
            char *indexname, *indexnamespace;

            token = pg_strtok(&length);
            token = pg_strtok(&length);
            indexname = nullable_string(token, length);
            token = pg_strtok(&length);
            token = pg_strtok(&length);
            indexnamespace = nullable_string(token, length);
            if (!IS_PGXC_COORDINATOR)
                local_node->indexid = get_valid_relname_relid(indexnamespace, indexname);

            pfree_ext(indexname);
            pfree_ext(indexnamespace);
        }
    }
#endif  // STREAMPLAN

    READ_NODE_FIELD(indexqual);
    READ_NODE_FIELD(indexqualorig);
    READ_NODE_FIELD(indexorderby);
    READ_NODE_FIELD(indexorderbyorig);
    READ_ENUM_FIELD(indexorderdir, ScanDirection);
    IF_EXIST(is_ustore) {
        READ_BOOL_FIELD(is_ustore);
    }
    READ_DONE();
}

static CStoreIndexScan* _readCStoreIndexScan(CStoreIndexScan* local_node)
{
    READ_LOCALS_NULL(CStoreIndexScan);
    READ_TEMP_LOCALS();

    // Read Scan
    _readScan(&local_node->scan);

    READ_OID_FIELD(indexid);
#ifdef STREAMPLAN
    // Note: The Oid shipped(in plan) is invalid here
    // We need to get the Oid on this node
    if (local_node->indexid >= FirstBootstrapObjectId) {
        IF_EXIST(indexname) {
            char *indexname, *indexnamespace;

            token = pg_strtok(&length);
            token = pg_strtok(&length);
            indexname = nullable_string(token, length);
            token = pg_strtok(&length);
            token = pg_strtok(&length);
            indexnamespace = nullable_string(token, length);
            if (!IS_PGXC_COORDINATOR)
                local_node->indexid = get_valid_relname_relid(indexnamespace, indexname);

            pfree_ext(indexname);
            pfree_ext(indexnamespace);
        }
    }
#endif  // STREAMPLAN

    READ_NODE_FIELD(indexqual);
    READ_NODE_FIELD(indexqualorig);
    READ_NODE_FIELD(indexorderby);
    READ_NODE_FIELD(indexorderbyorig);
    READ_ENUM_FIELD(indexorderdir, ScanDirection);
    READ_NODE_FIELD(baserelcstorequal);
    READ_NODE_FIELD(cstorequal);
    READ_NODE_FIELD(indextlist);
    READ_ENUM_FIELD(relStoreLocation, RelstoreType);
    READ_BOOL_FIELD(indexonly);

    READ_DONE();
}

static DfsIndexScan* _readDfsIndexScan(DfsIndexScan* local_node)
{
    READ_LOCALS_NULL(DfsIndexScan);
    READ_TEMP_LOCALS();

    // Read Scan
    _readScan(&local_node->scan);

    READ_OID_FIELD(indexid);
#ifdef STREAMPLAN
    // Note: The Oid shipped(in plan) is invalid here
    // We need to get the Oid on this node
    if (local_node->indexid >= FirstBootstrapObjectId) {
        IF_EXIST(indexname) {
            char *indexname, *indexnamespace;

            token = pg_strtok(&length);
            token = pg_strtok(&length);
            indexname = nullable_string(token, length);
            token = pg_strtok(&length);
            token = pg_strtok(&length);
            indexnamespace = nullable_string(token, length);
            if (!IS_PGXC_COORDINATOR)
                local_node->indexid = get_valid_relname_relid(indexnamespace, indexname);

            pfree_ext(indexname);
            pfree_ext(indexnamespace);
        }
    }
#endif  // STREAMPLAN

    READ_NODE_FIELD(indextlist);
    READ_NODE_FIELD(indexqual);
    READ_NODE_FIELD(indexqualorig);
    READ_NODE_FIELD(indexorderby);
    READ_NODE_FIELD(indexorderbyorig);
    READ_ENUM_FIELD(indexorderdir, ScanDirection);
    READ_ENUM_FIELD(relStoreLocation, RelstoreType);
    READ_NODE_FIELD(cstorequal);
    READ_NODE_FIELD(indexScantlist);
    READ_NODE_FIELD(dfsScan);
    READ_BOOL_FIELD(indexonly);
    READ_DONE();
}

static Sort* _readSort(Sort* local_node)
{
    READ_LOCALS_NULL(Sort);
    READ_TEMP_LOCALS();

    // Read Plan
    _readPlan(&local_node->plan);

    READ_INT_FIELD(numCols);
    READ_ATTR_ARRAY(sortColIdx, numCols);
    READ_OPERATOROID_ARRAY(sortOperators, numCols);
    READ_OID_ARRAY(collations, numCols);

    // Conver collname to colloid when node->collOid is user-defined,
    // because DN and CN use different OID though the objection name is the same
    // Note that this function must be changed if _outSort change
    //
    READ_OID_ARRAY_BYCONVERT(collations, numCols);

    READ_BOOL_ARRAY(nullsFirst, numCols);
    read_mem_info(&local_node->mem_info);

    READ_DONE();
}

static Unique* _readUnique(Unique* local_node)
{
    READ_LOCALS_NULL(Unique);
    READ_TEMP_LOCALS();

    // Read Plan
    _readPlan(&local_node->plan);

    READ_INT_FIELD(numCols);
    READ_ATTR_ARRAY(uniqColIdx, numCols);
    READ_OPERATOROID_ARRAY(uniqOperators, numCols);
    READ_DONE();
}

static SimpleSort* _readSimpleSort(SimpleSort* local_node)
{
    READ_LOCALS_NULL(SimpleSort);
    READ_TEMP_LOCALS();

    READ_INT_FIELD(numCols);
    READ_ATTR_ARRAY(sortColIdx, numCols);
    READ_OPERATOROID_ARRAY(sortOperators, numCols);
    READ_OID_ARRAY(sortCollations, numCols);

    /*
     * Conver collname to colloid when node->collOid is user-defined,
     * because DN and CN use different OID though the objection name is the same
     * Note that this function must be changed if _outSimpleSort change
     */
    READ_OID_ARRAY_BYCONVERT(sortCollations, numCols);

    READ_BOOL_ARRAY(nullsFirst, numCols);

    READ_BOOL_FIELD(sortToStore);

    READ_DONE();
}

static RemoteQuery* _readRemoteQuery(void)
{
    READ_LOCALS(RemoteQuery);

    READ_REMOTEQUERY_FIELD();
}

static SliceBoundary* _readSliceBoundary(void)
{
    READ_LOCALS(SliceBoundary);
    READ_INT_FIELD(nodeIdx);
    READ_INT_FIELD(len);

    
    token = pg_strtok(&length); /* skip ":boundary" */
    token = pg_strtok(&length); /* skip "(" */
    for (int i = 0; i < local_node->len; i++) {
        void *ptr = nodeRead(NULL, 0);
        errno_t err = memcpy_s(&local_node->boundary[i], sizeof(void *), &ptr, sizeof(void *));
        securec_check(err, "\0", "\0");
    }
    token = pg_strtok(&length); /* skip ")" */

    READ_DONE();
}

static ExecBoundary* _readExecBoundary(void)
{
    READ_LOCALS(ExecBoundary);
    READ_CHAR_FIELD(locatorType);
    READ_INT_FIELD(count);

    token = pg_strtok(&length); /* skip ":eles" */
    token = pg_strtok(&length); /* skip "(" */
    if (local_node->count > 0) {
        local_node->eles = (SliceBoundary**)palloc0(sizeof(SliceBoundary*) * local_node->count);
        for (int i = 0; i < local_node->count; i++) {
            void *ptr = nodeRead(NULL, 0);
            errno_t err = memcpy_s(&local_node->eles[i], sizeof(void *), &ptr, sizeof(void *));
            securec_check(err, "\0", "\0");
        }
    }
    token = pg_strtok(&length); /* skip ")" */

    READ_DONE();
}

/*
 * _readExecNodes
 *     read information for a ExecNodes
 *
 * @param (in) : void
 *
 * @return:
 *     ExecNodes *: the target ExecNodes pointer
 */
static ExecNodes* _readExecNodes(void)
{
    READ_LOCALS(ExecNodes);

    READ_NODE_FIELD(primarynodelist);
    READ_NODE_FIELD(nodeList);
    Distribution* distribution = _readDistribution();
    ng_set_distribution(&local_node->distribution, distribution);
    READ_CHAR_FIELD(baselocatortype);
    READ_NODE_FIELD(en_expr);
    READ_OID_FIELD(en_relid);

    IF_EXIST(boundaries) {
        READ_NODE_FIELD(boundaries);
    }

    READ_ENUM_FIELD(accesstype, RelationAccessType);
    READ_NODE_FIELD(en_dist_vars);
    READ_INT_FIELD(bucketmapIdx);
    READ_BOOL_FIELD(nodelist_is_nil);
    READ_NODE_FIELD(original_nodeList);
    READ_NODE_FIELD(dynamic_en_expr);

    if (t_thrd.proc->workingVersionNum >= 92106) {
        IF_EXIST(bucketid) {
            READ_INT_FIELD(bucketid);
        }
        IF_EXIST(bucketexpr) {
            READ_NODE_FIELD(bucketexpr);
        }
        IF_EXIST(bucketrelid) {
            READ_OID_FIELD(bucketrelid);
        }
        /*
         *  Note: The Oid shipped(in plan) is invalid here
         *  We need to get the Oid on this node.
         */
        if (local_node->bucketrelid >= FirstBootstrapObjectId) {
            char* relname = NULL;
            char* relnamespace = NULL;

            IF_EXIST(relname) {
                token = pg_strtok(&length);
                token = pg_strtok(&length);
                relname = nullable_string(token, length);
                if (relname == NULL) {
                    ereport(ERROR,
                        (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                            errmsg("NULL relname for RTE %u found", local_node->bucketrelid)));
                }
            }
            IF_EXIST(relnamespace) {
                token = pg_strtok(&length);
                token = pg_strtok(&length);
                relnamespace = nullable_string(token, length);
                if (relnamespace == NULL) {
                    ereport(ERROR,
                        (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                            errmsg("NULL relnamespace for RTE %u found", local_node->bucketrelid)));
                }
            }
#ifdef ENABLE_MULTIPLE_NODES
            if (!IS_PGXC_COORDINATOR && !isRestoreMode) {
                Oid bucketrelid = InvalidOid;
                if (relname != NULL && relnamespace != NULL) {
                    bucketrelid = get_valid_relname_relid(relnamespace, relname);
                    pfree_ext(relname);
                    pfree_ext(relnamespace);
                }

                if (OidIsValid(bucketrelid)) {
                    local_node->bucketrelid = bucketrelid;
                }
            }
#endif
        }
    }
    READ_DONE();
}

static ModifyTable* _readModifyTable(ModifyTable* local_node)
{
    READ_LOCALS_NULL(ModifyTable);
    READ_TEMP_LOCALS();

    // Read Plan
    _readPlan(&local_node->plan);

    READ_ENUM_FIELD(operation, CmdType);
    READ_BOOL_FIELD(canSetTag);
    READ_NODE_FIELD(resultRelations);
    READ_INT_FIELD(resultRelIndex);
    READ_NODE_FIELD(plans);
    READ_NODE_FIELD(returningLists);
    READ_NODE_FIELD(fdwPrivLists);
    READ_NODE_FIELD(rowMarks);
    READ_INT_FIELD(epqParam);
    READ_BOOL_FIELD(partKeyUpdated);
#ifdef PGXC
    READ_NODE_FIELD(remote_plans);
    READ_NODE_FIELD(remote_insert_plans);
    READ_NODE_FIELD(remote_update_plans);
    READ_NODE_FIELD(remote_delete_plans);
#endif
    READ_BOOL_FIELD(is_dist_insertselect);
    READ_NODE_FIELD(cacheEnt);

    IF_EXIST(mergeTargetRelation) {
        READ_INT_FIELD(mergeTargetRelation);
    }

    IF_EXIST(mergeSourceTargetList) {
        READ_NODE_FIELD(mergeSourceTargetList);
    }

    IF_EXIST(mergeActionList) {
        READ_NODE_FIELD(mergeActionList);
    }

    IF_EXIST(upsertAction) {
        READ_ENUM_FIELD(upsertAction, UpsertAction);
    }

    IF_EXIST(updateTlist) {
        READ_NODE_FIELD(updateTlist);
    }

    IF_EXIST(exclRelTlist) {
        READ_NODE_FIELD(exclRelTlist);
    }

    IF_EXIST(exclRelRTIndex) {
        READ_INT_FIELD(exclRelRTIndex);
    }

    IF_EXIST(upsertWhere) {
        READ_NODE_FIELD(upsertWhere);
    }

    READ_DONE();
}

static UpsertExpr* _readUpsertExpr(void)
{
    READ_LOCALS(UpsertExpr);

    READ_ENUM_FIELD(upsertAction, UpsertAction);
    READ_NODE_FIELD(updateTlist);
    READ_NODE_FIELD(exclRelTlist);
    READ_INT_FIELD(exclRelIndex);
    IF_EXIST(upsertWhere) {
        READ_NODE_FIELD(upsertWhere);
    }

    READ_DONE();
}

static UpsertClause* _readUpsertClause(void)
{
    READ_LOCALS(UpsertClause);

    READ_NODE_FIELD(targetList);
    READ_INT_FIELD(location);
    IF_EXIST(whereClause) {
        READ_NODE_FIELD(whereClause);
    }
    READ_DONE();
}

/*
 * _readMergeWhenClause
 */
static MergeWhenClause* _readMergeWhenClause(void)
{
    READ_LOCALS(MergeWhenClause);

    READ_BOOL_FIELD(matched);
    READ_ENUM_FIELD(commandType, CmdType);
    READ_NODE_FIELD(condition);
    READ_NODE_FIELD(targetList);
    READ_NODE_FIELD(cols);
    READ_NODE_FIELD(values);

    READ_DONE();
}

static BaseResult* _readResult(BaseResult* local_node)
{
    READ_LOCALS_NULL(BaseResult);
    READ_TEMP_LOCALS();

    // Read Plan
    _readPlan(&local_node->plan);
    READ_NODE_FIELD(resconstantqual);

    READ_DONE();
}

static Material* _readMaterial(Material* local_node)
{
    READ_LOCALS_NULL(Material);
    READ_TEMP_LOCALS();

    // Read Plan
    _readPlan(&local_node->plan);
    READ_BOOL_FIELD(materialize_all);
    read_mem_info(&local_node->mem_info);

    READ_DONE();
}

static Append* _readAppend(Append* local_node)
{
    READ_LOCALS_NULL(Append);
    READ_TEMP_LOCALS();

    // Read Plan
    _readPlan(&local_node->plan);
    READ_NODE_FIELD(appendplans);

    READ_DONE();
}

static VecAppend* _readVecAppend(VecAppend* local_node)
{
    READ_LOCALS_NULL(VecAppend);
    READ_TEMP_LOCALS();

    // Read Plan
    _readPlan(&local_node->plan);
    READ_NODE_FIELD(appendplans);

    READ_DONE();
}

static MergeAppend* _readMergeAppend(MergeAppend* local_node)
{
    READ_LOCALS_NULL(MergeAppend);
    READ_TEMP_LOCALS();

    _readPlan(&local_node->plan);
    READ_NODE_FIELD(mergeplans);
    READ_INT_FIELD(numCols);
    READ_ATTR_ARRAY(sortColIdx, numCols);
    READ_OPERATOROID_ARRAY(sortOperators, numCols);
    READ_OID_ARRAY(collations, numCols);

    // Conver collname to colloid when node->collOid is user-defined,
    // because DN and CN use different OID though the objection name is the same
    // Note that this function must be changed if _outSort change
    //
    READ_OID_ARRAY_BYCONVERT(collations, numCols);

    READ_BOOL_ARRAY(nullsFirst, numCols);

    READ_DONE();
}

static Group* _readGroup(Group* local_node)
{
    READ_LOCALS_NULL(Group);
    READ_TEMP_LOCALS();

    _readPlan(&local_node->plan);

    READ_INT_FIELD(numCols);
    READ_ATTR_ARRAY(grpColIdx, numCols);
    READ_OPERATOROID_ARRAY(grpOperators, numCols);

    READ_DONE();
}

static Join* _readJoin(Join* local_node)
{
    READ_LOCALS_NULL(Join);
    READ_TEMP_LOCALS();

    // Read Plan
    _readPlan(&local_node->plan);

    READ_ENUM_FIELD(jointype, JoinType);
    READ_NODE_FIELD(joinqual);
    READ_BOOL_FIELD(optimizable);
    READ_NODE_FIELD(nulleqqual);
    READ_UINT_FIELD(skewoptimize);

    READ_DONE();
}

static Hash* _readHash(Hash* local_node)
{
    READ_LOCALS_NULL(Hash);
    READ_TEMP_LOCALS();

    // Read Plan
    _readPlan(&local_node->plan);

    READ_INT_FIELD(skewColumn);
    READ_BOOL_FIELD(skewInherit);
    READ_OID_FIELD(skewColType);
    READ_INT_FIELD(skewColTypmod);

    // for tmp, remove the line and fix the issue
    local_node->skewTable = InvalidOid;

    READ_TYPEINFO_FIELD(skewColType);
    READ_DONE();
}

static HashJoin* _readHashJoin(HashJoin* local_node)
{
    READ_LOCALS_NULL(HashJoin);
    READ_HASHJOIN_FIELD();
}

static MergeJoin* _readMergeJoin(MergeJoin* local_node)
{
    READ_LOCALS_NULL(MergeJoin);
    READ_MERGEJOIN();
}

static VecMergeJoin* _readVecMergeJoin(VecMergeJoin* local_node)
{
    READ_LOCALS_NULL(VecMergeJoin);
    READ_MERGEJOIN();
}

static NestLoop* _readNestLoop(void)
{
    READ_LOCALS(NestLoop);

    // Read Join
    _readJoin(&local_node->join);

    READ_NODE_FIELD(nestParams);
    READ_BOOL_FIELD(materialAll);
    READ_DONE();
}

static VecNestLoop* _readVecNestLoop(void)
{
    READ_LOCALS(VecNestLoop);

    // Read Join
    _readJoin(&local_node->join);

    READ_NODE_FIELD(nestParams);
    READ_BOOL_FIELD(materialAll);
    READ_DONE();
}

static VecMaterial* _readVecMaterial(void)
{
    READ_LOCALS(VecMaterial);

    // Read Plan
    _readPlan(&local_node->plan);
    READ_BOOL_FIELD(materialize_all);
    read_mem_info(&local_node->mem_info);

    READ_DONE();
}

static VecRemoteQuery* _readVecRemoteQuery(void)
{
    READ_LOCALS(VecRemoteQuery);

    READ_REMOTEQUERY_FIELD();
}

static PlannedStmt* _readPlannedStmt(void)
{
    READ_LOCALS(PlannedStmt);

    READ_ENUM_FIELD(commandType, CmdType);
    READ_UINT64_FIELD(queryId);
    READ_BOOL_FIELD(hasReturning);
    READ_BOOL_FIELD(hasModifyingCTE);
    READ_BOOL_FIELD(canSetTag);
    READ_BOOL_FIELD(transientPlan);
    IF_EXIST(dependsOnRole) {
        READ_BOOL_FIELD(dependsOnRole);
    }
    READ_NODE_FIELD(planTree);
    READ_NODE_FIELD(rtable);
    READ_NODE_FIELD(resultRelations);
    READ_NODE_FIELD(utilityStmt);
    READ_NODE_FIELD(subplans);
    READ_BITMAPSET_FIELD(rewindPlanIDs);
    READ_NODE_FIELD(rowMarks);
    READ_NODE_FIELD(relationOids);
    READ_NODE_FIELD(invalItems);
    READ_INT_FIELD(nParamExec);
    READ_INT_FIELD(num_streams);
    READ_INT_FIELD(max_push_sql_num);
    IF_EXIST(gather_count) {
        READ_INT_FIELD(gather_count);
    }
    READ_INT_FIELD(num_nodes);

    if (t_thrd.proc->workingVersionNum < 92097 || local_node->num_streams > 0) {
	    local_node->nodesDefinition = (NodeDefinition*)palloc0(sizeof(NodeDefinition) * local_node->num_nodes);
	    for (int i = 0; i < local_node->num_nodes; i++) {
	        READ_OID_FIELD(nodesDefinition[i].nodeoid);
	        READCOPY_STRING_FIELD_DIRECT(nodesDefinition[i].nodename.data);
	        READCOPY_STRING_FIELD_DIRECT(nodesDefinition[i].nodehost.data);
	        READ_INT_FIELD_DIRECT(nodesDefinition[i].nodeport);
	        READ_INT_FIELD_DIRECT(nodesDefinition[i].nodectlport);
	        READ_INT_FIELD_DIRECT(nodesDefinition[i].nodesctpport);
	        READCOPY_STRING_FIELD_DIRECT(nodesDefinition[i].nodehost1.data);
	        READ_INT_FIELD_DIRECT(nodesDefinition[i].nodeport1);
	        READ_INT_FIELD_DIRECT(nodesDefinition[i].nodectlport1);
	        READ_INT_FIELD_DIRECT(nodesDefinition[i].nodesctpport1);
	        READ_BOOL_FIELD_DIRECT(nodesDefinition[i].nodeisprimary);
	        READ_BOOL_FIELD_DIRECT(nodesDefinition[i].nodeispreferred);
	    }
	}

    READ_INT_FIELD(instrument_option);
    READ_INT_FIELD(num_plannodes);
    READ_INT_FIELD(query_mem[0]);
    READ_INT_FIELD(query_mem[1]);
    READ_INT_FIELD(assigned_query_mem[0]);
    READ_INT_FIELD(assigned_query_mem[1]);

    READ_INT_FIELD(num_bucketmaps);
    int size;
    for (int j = 0; j < local_node->num_bucketmaps; j++) {
        local_node->bucketCnt[j] = BUCKETDATALEN;
        IF_EXIST(bucketCnt) {
            READ_INT_FIELD(bucketCnt[j]);
        }
        size = local_node->bucketCnt[j];
        READ_UINT2_ARRAY_LEN(bucketMap[j]);
    }

    READ_STRING_FIELD(query_string);
    READ_NODE_FIELD(subplan_ids);
    READ_NODE_FIELD(initPlan);
    /* data redistribution for DFS table. */
    READ_UINT_FIELD(dataDestRelIndex);
    READ_INT_FIELD(MaxBloomFilterNum);
    READ_INT_FIELD(query_dop);
    READ_BOOL_FIELD(in_compute_pool);
    READ_BOOL_FIELD(has_obsrel);

    READ_INT_FIELD(ng_num);
    local_node->ng_queryMem = NULL;
    if (local_node->ng_num > 0) {
        local_node->ng_queryMem = (NodeGroupQueryMem*)palloc0(local_node->ng_num * sizeof(NodeGroupQueryMem));
        for (int i = 0; i < local_node->ng_num; i++) {
            READCOPY_STRING_FIELD_DIRECT(ng_queryMem[i].nodegroup);
            READ_INT_FIELD_DIRECT(ng_queryMem[i].query_mem[0]);
            READ_INT_FIELD_DIRECT(ng_queryMem[i].query_mem[1]);
        }
    }
    READ_BOOL_FIELD(isRowTriggerShippable);
    READ_BOOL_FIELD(is_stream_plan);

    READ_DONE();
}

static NestLoopParam* _readNestLoopParam(void)
{
    READ_LOCALS(NestLoopParam);

    READ_INT_FIELD(paramno);
    READ_NODE_FIELD(paramval);

    READ_DONE();
}

static PlanRowMark* _readPlanRowMark(void)
{
    READ_LOCALS(PlanRowMark);

    READ_UINT_FIELD(rti);
    READ_UINT_FIELD(prti);
    READ_UINT_FIELD(rowmarkId);
    READ_ENUM_FIELD(markType, RowMarkType);
    READ_BOOL_FIELD(noWait);
    IF_EXIST(waitSec) {
        READ_INT_FIELD(waitSec);
    }

    READ_BOOL_FIELD(isParent);
    READ_INT_FIELD(numAttrs);
    READ_BITMAPSET_FIELD(bms_nodeids);

    READ_DONE();
}
static Scan* _readSeqScan(void)
{
    READ_LOCALS_NO_FIELDS(SeqScan);

    _readScan(local_node);

    READ_END();
}

static SetOp* _readSetOp(SetOp* local_node)
{
    READ_LOCALS_NULL(SetOp);
    READ_TEMP_LOCALS();

    // Read Plan
    _readPlan(&local_node->plan);

    READ_ENUM_FIELD(cmd, SetOpCmd);
    READ_ENUM_FIELD(strategy, SetOpStrategy);
    READ_INT_FIELD(numCols);
    READ_ATTR_ARRAY(dupColIdx, numCols);
    READ_OPERATOROID_ARRAY(dupOperators, numCols);
    READ_INT_FIELD(flagColIdx);
    READ_INT_FIELD(firstFlag);
    READ_LONG_FIELD(numGroups);

    READ_DONE();
}

static FunctionScan* _readFunctionScan(FunctionScan* local_node)
{
    READ_LOCALS_NULL(FunctionScan);
    READ_TEMP_LOCALS();

    // Read Plan
    _readScan(&local_node->scan);

    READ_NODE_FIELD(funcexpr);
    READ_NODE_FIELD(funccolnames);
    READ_NODE_FIELD(funccoltypes);
    READ_NODE_FIELD(funccoltypmods);
    READ_NODE_FIELD(funccolcollations);

    READ_TYPEINFO_LIST(funccoltypes);

    READ_DONE();
}

static ForeignScan* _readForeignScan(ForeignScan* local_node)
{
    READ_LOCALS_NULL(ForeignScan);
    READ_TEMP_LOCALS();

    // Read Plan
    _readScan(&local_node->scan);

    READ_FOREIGNSCAN();

    if (local_node->bfNum > 0) {
        local_node->bloomFilterSet = (BloomFilterSet**)palloc0(sizeof(BloomFilterSet*) * local_node->bfNum);

        for (int cnt = 0; cnt < local_node->bfNum; cnt++) {
            READ_NODE_FIELD(bloomFilterSet[cnt]);
        }
    }

    READ_BOOL_FIELD(in_compute_pool);
    READ_DONE();
}

static ExtensiblePlan* _readExtensiblePlan(ExtensiblePlan* local_node)
{
    READ_LOCALS_NULL(ExtensiblePlan);
    READ_TEMP_LOCALS();
    char* extensible_name;
    ExtensiblePlanMethods* methods;

    // Read Plan
    _readScan(&local_node->scan);

    READ_UINT_FIELD(flags);
    READ_NODE_FIELD(extensible_plans);
    READ_NODE_FIELD(extensible_exprs);
    READ_NODE_FIELD(extensible_private);
    READ_NODE_FIELD(extensible_plan_tlist);
    READ_BITMAPSET_FIELD(extensible_relids);

    /* Lookup ExtensiblePlanMethods by ExtensibleName */
    token = pg_strtok(&length); /* skip methods: */
    token = pg_strtok(&length); /* ExtensibleName */
    extensible_name = nullable_string(token, length);
    methods = GetExtensiblePlanMethods(extensible_name, false);
    local_node->methods = methods;
    READ_DONE();
}

static ForeignPartState* _readForeignPartState(ForeignPartState* local_node)
{
    READ_LOCALS_NULL(ForeignPartState);
    READ_TEMP_LOCALS();

    READ_NODE_FIELD(partitionKey);

    READ_DONE();
}

static RecursiveUnion* _readRecursiveUnion(RecursiveUnion* local_node)
{
    READ_LOCALS_NULL(RecursiveUnion);
    READ_TEMP_LOCALS();

    // Read Plan
    _readPlan(&local_node->plan);

    READ_INT_FIELD(wtParam);
    READ_INT_FIELD(numCols);
    READ_ATTR_ARRAY(dupColIdx, numCols);
    READ_OPERATOROID_ARRAY(dupOperators, numCols);
    READ_LONG_FIELD(numGroups);
    READ_BOOL_FIELD(has_inner_stream);
    READ_BOOL_FIELD(has_outer_stream);
    READ_BOOL_FIELD(is_used);
    READ_BOOL_FIELD(is_correlated);
    IF_EXIST(internalEntryList) {
        READ_NODE_FIELD(internalEntryList);
    }

    READ_DONE();
}

static StartWithOp* _readStartWithOp(StartWithOp* local_node)
{
    READ_LOCALS_NULL(StartWithOp);

    READ_TEMP_LOCALS();

    // Read Plan
    _readPlan(&local_node->plan);

    READ_NODE_FIELD(cteplan);
    READ_NODE_FIELD(ruplan);

    READ_NODE_FIELD(keyEntryList);
    READ_NODE_FIELD(colEntryList);
    READ_NODE_FIELD(internalEntryList);
    READ_NODE_FIELD(fullEntryList);

    READ_NODE_FIELD(swoptions);

    READ_DONE();
}

static StartWithOptions* _readStartWithOptions(StartWithOptions* local_node)
{
    READ_LOCALS_NULL(StartWithOptions);

    READ_TEMP_LOCALS();

    READ_NODE_FIELD(siblings_orderby_clause);
    READ_NODE_FIELD(prior_key_index);
    READ_ENUM_FIELD(connect_by_type, StartWithConnectByType);
    READ_NODE_FIELD(connect_by_level_quals);
    READ_NODE_FIELD(connect_by_other_quals);
    READ_BOOL_FIELD(nocycle);

    READ_DONE();
}

static WorkTableScan* _readWorkTableScan(WorkTableScan* local_node)
{
    READ_LOCALS_NULL(WorkTableScan);
    READ_TEMP_LOCALS();

    // Read Plan
    _readScan(&local_node->scan);

    READ_INT_FIELD(wtParam);
    IF_EXIST(forStartWith) {
        READ_BOOL_FIELD(forStartWith);
    }

    READ_DONE();
}

static PlanInvalItem* _readPlanInvalItem(PlanInvalItem* local_node)
{
    READ_LOCALS_NULL(PlanInvalItem);
    READ_TEMP_LOCALS();

    READ_INT_FIELD(cacheId);
    READ_UINT_FIELD(hashValue);

    READ_DONE();
}

static DistFdwDataNodeTask* _readDistFdwDataNodeTask(DistFdwDataNodeTask* local_node)
{
    READ_LOCALS_NULL(DistFdwDataNodeTask);
    READ_TEMP_LOCALS();

    READ_STRING_FIELD(dnName);
    READ_NODE_FIELD(task);

    READ_DONE();
}

static DistFdwFileSegment* _readDistFdwFileSegment(DistFdwFileSegment* local_node)
{
    READ_LOCALS_NULL(DistFdwFileSegment);
    READ_TEMP_LOCALS();

    READ_STRING_FIELD(filename);
    READ_LONG_FIELD(begin);
    READ_LONG_FIELD(end);
    READ_LONG_FIELD(ObjectSize);

    READ_DONE();
}

static SplitInfo* _readSplitInfo(SplitInfo* local_node)
{
    READ_LOCALS_NULL(SplitInfo);
    READ_TEMP_LOCALS();

    READ_STRING_FIELD(filePath);
    READ_STRING_FIELD(fileName);
    READ_NODE_FIELD(partContentList);
    READ_LONG_FIELD(ObjectSize);
    READ_STRING_FIELD(eTag);
    READ_INT_FIELD(prefixSlashNum);

    READ_DONE();
}

static SplitMap* _readSplitMap(SplitMap* local_node)
{
    READ_LOCALS_NULL(SplitMap);
    READ_TEMP_LOCALS();

    READ_INT_FIELD(nodeId);
    READ_CHAR_FIELD(locatorType);
    READ_LONG_FIELD(totalSize);
    READ_INT_FIELD(fileNums);
    READ_STRING_FIELD(downDiskFilePath);
    READ_NODE_FIELD(lengths);
    READ_NODE_FIELD(splits);

    READ_DONE();
}

/* @hdfs */
static DfsPrivateItem* _readDfsPrivateItem(DfsPrivateItem* local_node)
{
    READ_LOCALS_NULL(DfsPrivateItem);
    READ_TEMP_LOCALS();

    READ_NODE_FIELD(columnList);
    READ_NODE_FIELD(targetList);
    READ_NODE_FIELD(restrictColList);
    READ_NODE_FIELD(partList);
    READ_NODE_FIELD(opExpressionList);
    READ_NODE_FIELD(dnTask);
    READ_NODE_FIELD(hdfsQual);
    READ_INT_FIELD(colNum);
    READ_DOUBLE_ARRAY(selectivity, colNum);
    READ_DONE();
}

/*
 * Description: Read tableSampleClause string to tableSampleClause struct.
 *
 * Parameters: void
 *
 * Return: TableSampleClause*
 */
static TableSampleClause* _readTableSampleClause(void)
{
    READ_LOCALS(TableSampleClause);

    READ_ENUM_FIELD(sampleType, TableSampleType);
    READ_NODE_FIELD(args);
    READ_NODE_FIELD(repeatable);

    READ_DONE();
}

/*
 * Description: Read TimeCapsuleClause string to TimeCapsuleClause struct.
 *
 * Parameters: void
 *
 * Return: TimeCapsuleClause*
 */
static TimeCapsuleClause* ReadTimeCapsuleClause(void)
{
    READ_LOCALS(TimeCapsuleClause);

    READ_ENUM_FIELD(tvtype, TvVersionType);
    READ_NODE_FIELD(tvver);

    READ_DONE();
}

static DefElem* _readDefElem(DefElem* local_node)
{
    READ_LOCALS_NULL(DefElem);
    READ_TEMP_LOCALS();

    READ_STRING_FIELD(defnamespace);
    READ_STRING_FIELD(defname);
    READ_NODE_FIELD(arg);
    READ_ENUM_FIELD(defaction, DefElemAction);

    IF_EXIST(begin_location) {
        READ_INT_FIELD(begin_location);
    }

    IF_EXIST(end_location) {
        READ_INT_FIELD(end_location);
    }

    READ_DONE();
}

static ErrorCacheEntry* _readErrorCacheEntry(ErrorCacheEntry* local_node)
{
    READ_LOCALS_NULL(ErrorCacheEntry);
    READ_TEMP_LOCALS();

    READ_NODE_FIELD(rte);
    READ_STRING_FIELD(filename);
    READ_DONE();
}

static VecToRow* _readVecToRow(void)
{
    READ_LOCALS_NO_FIELDS(VecToRow);

    // Read Plan
    _readPlan(&local_node->plan);

    READ_END();
}

static RowToVec* _readRowToVec(void)
{
    READ_LOCALS_NO_FIELDS(RowToVec);

    // Read Plan
    _readPlan(&local_node->plan);

    READ_END();
}

static VecSort* _readVecSort(VecSort* local_node)
{
    READ_LOCALS_NULL(VecSort);
    READ_TEMP_LOCALS();

    // Read Plan
    _readPlan(&local_node->plan);

    READ_INT_FIELD(numCols);
    READ_ATTR_ARRAY(sortColIdx, numCols);
    READ_OID_ARRAY(sortOperators, numCols);
    READ_OID_ARRAY(collations, numCols);
    READ_BOOL_ARRAY(nullsFirst, numCols);
    read_mem_info(&local_node->mem_info);

    READ_DONE();
}

static VecResult* _readVecResult(VecResult* local_node)
{
    READ_LOCALS_NULL(VecResult);
    READ_TEMP_LOCALS();

    _readPlan(&local_node->plan);
    READ_NODE_FIELD(resconstantqual);

    READ_DONE();
}

static CStoreScan* _readCStoreScan(CStoreScan* local_node)
{
    READ_LOCALS_NULL(CStoreScan);
    READ_TEMP_LOCALS();

    // Read Scan
    _readScan((Scan*)local_node);

    READ_NODE_FIELD(cstorequal);
    READ_NODE_FIELD(minMaxInfo);
    READ_ENUM_FIELD(relStoreLocation, RelstoreType);
    READ_BOOL_FIELD(is_replica_table);

    READ_DONE();
}

static DfsScan* _readDfsScan(DfsScan* local_node)
{
    READ_LOCALS_NULL(DfsScan);
    READ_TEMP_LOCALS();

    // Read Scan
    _readScan((Scan*)local_node);

    READ_ENUM_FIELD(relStoreLocation, RelstoreType);
    READ_NODE_FIELD(privateData);
    READ_STRING_FIELD(storeFormat);

    READ_DONE();
}

#ifdef ENABLE_MULTIPLE_NODES
static TsStoreScan* _readTsStoreScan(TsStoreScan *local_node)
{
    READ_LOCALS_NULL(TsStoreScan);
    READ_TEMP_LOCALS();
    // Read Scan
    _readScan((Scan *)local_node);

    READ_NODE_FIELD(tsstorequal);
    READ_NODE_FIELD(minMaxInfo);
    READ_ENUM_FIELD(relStoreLocation, RelstoreType);
    READ_BOOL_FIELD(is_replica_table);
    READ_INT_FIELD(sort_by_time_colidx);
    READ_INT_FIELD(limit);
    READ_BOOL_FIELD(is_simple_scan);
    READ_BOOL_FIELD(has_sort);
    READ_INT_FIELD(series_func_calls);
    READ_INT_FIELD(top_key_func_arg);
    READ_DONE();
}
#endif   /* ENABLE_MULTIPLE_NODES */

static VecSubqueryScan* _readVecSubqueryScan(VecSubqueryScan* local_node)
{
    READ_LOCALS_NULL(VecSubqueryScan);
    READ_TEMP_LOCALS();

    // Read Scan
    _readScan(&local_node->scan);

    READ_NODE_FIELD(subplan);

    READ_DONE();
}

static VecModifyTable* _readVecModifyTable(VecModifyTable* local_node)
{
    READ_LOCALS_NULL(VecModifyTable);
    READ_TEMP_LOCALS();
    _readPlan(&local_node->plan);
    READ_ENUM_FIELD(operation, CmdType);
    READ_BOOL_FIELD(canSetTag);
    READ_NODE_FIELD(resultRelations);
    READ_INT_FIELD(resultRelIndex);
    READ_NODE_FIELD(plans);
    READ_NODE_FIELD(returningLists);
    READ_NODE_FIELD(rowMarks);
    READ_INT_FIELD(epqParam);
    READ_BOOL_FIELD(partKeyUpdated);
#ifdef PGXC
    READ_NODE_FIELD(remote_plans);
    READ_NODE_FIELD(remote_insert_plans);
    READ_NODE_FIELD(remote_update_plans);
    READ_NODE_FIELD(remote_delete_plans);
#endif
    READ_NODE_FIELD(cacheEnt);

    IF_EXIST(mergeTargetRelation) {
        READ_INT_FIELD(mergeTargetRelation);
    }

    IF_EXIST(mergeSourceTargetList) {
        READ_NODE_FIELD(mergeSourceTargetList);
    }

    IF_EXIST(mergeActionList) {
        READ_NODE_FIELD(mergeActionList);
    }
    read_mem_info(&local_node->mem_info);

    READ_DONE();
}
static VecAgg* _readVecAgg(VecAgg* local_node)
{
    READ_LOCALS_NULL(VecAgg);
    READ_TEMP_LOCALS();

    // Read Plan
    _readPlan(&local_node->plan);

    READ_ENUM_FIELD(aggstrategy, AggStrategy);
    READ_INT_FIELD(numCols);
    READ_ATTR_ARRAY(grpColIdx, numCols);
    READ_OID_ARRAY(grpOperators, numCols);
    READ_LONG_FIELD(numGroups);
    READ_NODE_FIELD(groupingSets);
    READ_NODE_FIELD(chain);
    READ_BOOL_FIELD(is_final);
    READ_BOOL_FIELD(single_node);
    READ_BITMAPSET_FIELD(aggParams);
    read_mem_info(&local_node->mem_info);
    READ_BOOL_FIELD(is_sonichash);
    READ_UINT_FIELD(skew_optimize);

    IF_EXIST(unique_check) {
        READ_BOOL_FIELD(unique_check);
    }

    READ_DONE();
}

static VecLimit* _readVecLimit(VecLimit* local_node)
{
    READ_LOCALS_NULL(VecLimit);
    READ_TEMP_LOCALS();

    // Read Plan
    _readPlan(&local_node->plan);

    READ_NODE_FIELD(limitOffset);
    READ_NODE_FIELD(limitCount);

    READ_DONE();
}

static VecHashJoin* _readVecHashJoin(VecHashJoin* local_node)
{
    READ_LOCALS_NULL(VecHashJoin);
    READ_HASHJOIN_FIELD();
}

static VecSetOp* _readVecSetOp(VecSetOp* local_node)
{
    READ_LOCALS_NULL(VecSetOp);
    READ_TEMP_LOCALS();

    // Read Plan
    _readPlan(&local_node->plan);

    READ_ENUM_FIELD(cmd, SetOpCmd);
    READ_ENUM_FIELD(strategy, SetOpStrategy);
    READ_INT_FIELD(numCols);
    READ_ATTR_ARRAY(dupColIdx, numCols);
    READ_OID_ARRAY(dupOperators, numCols);
    READ_INT_FIELD(flagColIdx);
    READ_INT_FIELD(firstFlag);
    READ_LONG_FIELD(numGroups);

    READ_DONE();
}

static VecGroup* _readVecGroup(VecGroup* local_node)
{
    READ_LOCALS_NULL(VecGroup);
    READ_TEMP_LOCALS();

    _readPlan(&local_node->plan);

    READ_INT_FIELD(numCols);
    READ_ATTR_ARRAY(grpColIdx, numCols);
    READ_OID_ARRAY(grpOperators, numCols);

    READ_DONE();
}

static VecUnique* _readVecUnique(VecUnique* local_node)
{
    READ_LOCALS_NULL(VecUnique);
    READ_TEMP_LOCALS();

    // Read Plan
    _readPlan(&local_node->plan);

    READ_INT_FIELD(numCols);
    READ_ATTR_ARRAY(uniqColIdx, numCols);
    READ_OID_ARRAY(uniqOperators, numCols);

    READ_DONE();
}

static VecPartIterator* _readVecPartIterator(VecPartIterator* local_node)
{
    READ_LOCALS_NULL(VecPartIterator);
    READ_TEMP_LOCALS();

    _readPlan(&local_node->plan);

    READ_ENUM_FIELD(partType, PartitionType);
    READ_INT_FIELD(itrs);
    READ_ENUM_FIELD(direction, ScanDirection);
    READ_NODE_FIELD(param);

    READ_DONE();
}

static VecForeignScan* _readVecForeignScan(VecForeignScan* local_node)
{
    READ_LOCALS_NULL(VecForeignScan);
    READ_TEMP_LOCALS();
    _readScan(&local_node->scan);

    READ_FOREIGNSCAN();

    if (local_node->bfNum > 0) {
        local_node->bloomFilterSet = (BloomFilterSet**)palloc0(sizeof(BloomFilterSet*) * local_node->bfNum);

        for (int cnt = 0; cnt < local_node->bfNum; cnt++) {
            READ_NODE_FIELD(bloomFilterSet[cnt]);
        }
    }

    READ_BOOL_FIELD(in_compute_pool);
    READ_DONE();
}

static VecStream* _readVecStream(VecStream* local_node)
{
    READ_LOCALS_NULL(VecStream);

    READ_STREAM_FIELD();
}

static HDFSTableAnalyze* _readHDFSTableAnalyze(HDFSTableAnalyze* local_node)
{
    READ_LOCALS_NULL(HDFSTableAnalyze);
    READ_TEMP_LOCALS();

    READ_NODE_FIELD(DnWorkFlow);
    READ_INT_FIELD(DnCnt);
    READ_BOOL_FIELD(isHdfsStore);
    token = pg_strtok(&length); /* skip :fldname */
    for (int i = 0; i < ANALYZE_MODE_MAX_NUM - 1; i++) {
        token = pg_strtok(&length); /* get field value */
        local_node->sampleRate[i] = atof(token);
    }
    READ_UINT_FIELD(orgCnNodeNo);
    READ_BOOL_FIELD(isHdfsForeignTbl);
    READ_BOOL_FIELD(sampleTableRequired);
    READ_NODE_FIELD(tmpSampleTblNameList);
    READ_ENUM_FIELD(disttype, DistributionType);
    READ_INT_FIELD(memUsage.work_mem);
    READ_INT_FIELD(memUsage.max_mem);
    READ_DONE();
}

static VecWindowAgg* _readVecWindowAgg(VecWindowAgg* local_node)
{
    READ_LOCALS_NULL(VecWindowAgg);
    READ_TEMP_LOCALS();

    _readPlan(&local_node->plan);
    READ_UINT_FIELD(winref);

    READ_INT_FIELD(partNumCols);
    READ_ATTR_ARRAY(partColIdx, partNumCols);
    READ_OID_ARRAY(partOperators, partNumCols);
    READ_INT_FIELD(ordNumCols);
    READ_ATTR_ARRAY(ordColIdx, ordNumCols);
    READ_OID_ARRAY(ordOperators, ordNumCols);

    READ_INT_FIELD(frameOptions);
    READ_NODE_FIELD(startOffset);
    READ_NODE_FIELD(endOffset);
    read_mem_info(&local_node->mem_info);

    READ_DONE();
}

static PlaceHolderVar* _readPlaceHolderVar()
{
    READ_LOCALS(PlaceHolderVar);

    READ_NODE_FIELD(phexpr);
    READ_BITMAPSET_FIELD(phrels);
    READ_UINT_FIELD(phid);
    READ_UINT_FIELD(phlevelsup);

    READ_DONE();
}

static AttrMetaData* _readAttrMetaData(AttrMetaData* local_node)
{
    READ_LOCALS_NULL(AttrMetaData);
    READ_TEMP_LOCALS();

    READ_STRING_FIELD(attname);
    READ_OID_FIELD(atttypid);
    READ_OID_FIELD(attlen);
    READ_OID_FIELD(attnum);
    READ_OID_FIELD(atttypmod);
    READ_BOOL_FIELD(attbyval);
    READ_CHAR_FIELD(attstorage);
    READ_CHAR_FIELD(attalign);
    READ_BOOL_FIELD(attnotnull);
    READ_BOOL_FIELD(atthasdef);
    READ_BOOL_FIELD(attisdropped);
    READ_BOOL_FIELD(attislocal);
    READ_INT_FIELD(attkvtype);
    READ_INT_FIELD(attcmprmode);
    READ_INT_FIELD(attinhcount);
    READ_OID_FIELD(attcollation);

    READ_TYPEINFO_FIELD(atttypid);
    READ_DONE();
}

static RelationMetaData* _readRelationMetaData(RelationMetaData* local_node)
{
    READ_LOCALS_NULL(RelationMetaData);
    READ_TEMP_LOCALS();

    READ_OID_FIELD(rd_id);

    READ_OID_FIELD(spcNode);
    READ_OID_FIELD(dbNode);
    READ_OID_FIELD(relNode);

    IF_EXIST(bucketNode) {
        READ_INT_FIELD(bucketNode);
    }

    READ_STRING_FIELD(relname);
    READ_CHAR_FIELD(relkind);
    READ_CHAR_FIELD(parttype);

    READ_INT_FIELD(natts);
    READ_NODE_FIELD(attrs);

    READ_DONE();
}

static ForeignOptions* _readForeignOptions(ForeignOptions* local_node)
{
    READ_LOCALS_NULL(ForeignOptions);
    READ_TEMP_LOCALS();

    READ_ENUM_FIELD(stype, ServerTypeOption);
    READ_NODE_FIELD(fOptions);

    READ_DONE();
}

static BloomFilterSet* _readBloomFilterSet(BloomFilterSet* local_node)
{
    READ_LOCALS_NULL(BloomFilterSet);
    READ_TEMP_LOCALS();
    READ_ULONG_FIELD(length);
    local_node->data = _readUint64Array(local_node->length);
    READ_ULONG_FIELD(numBits);
    READ_ULONG_FIELD(numHashFunctions);
    READ_ULONG_FIELD(numValues);
    READ_ULONG_FIELD(maxNumValues);
    READ_ULONG_FIELD(startupEntries);

    elog(DEBUG1, "_readBloomFilterSet:: data");
    StringInfo str = makeStringInfo();
    appendStringInfo(str, "\n");
    for (uint64 ii = 0; ii < local_node->length; ii++) {
        if (local_node->data[ii] != 0) {
            appendStringInfo(str, "not zore value in position %lu , value %lu,\n ", ii, local_node->data[ii]);
        }
    }
    elog(DEBUG1, "%s", str->data);
    resetStringInfo(str);
    elog(DEBUG1, "_readBloomFilterSet:: valuePositions");
    appendStringInfo(str, "\n");
    if (local_node->startupEntries > 0) {
        local_node->valuePositions = (ValueBit*)palloc0(local_node->startupEntries * sizeof(ValueBit));
        appendStringInfo(str, "\n");
        for (uint64 cell = 0; cell < local_node->startupEntries; cell++) {
            uint16* array = _readUint16Array(MAX_HASH_FUNCTIONS);
            for (int idx = 0; idx < MAX_HASH_FUNCTIONS; idx++) {
                local_node->valuePositions[cell].position[idx] = array[idx];

                appendStringInfo(str, "%d, ", local_node->valuePositions[cell].position[idx]);
            }
            appendStringInfo(str, "\n");
        }
        elog(DEBUG1, "%s", str->data);
    }

    READ_ULONG_FIELD(minIntValue);
    READ_FLOAT_FIELD(minFloatValue);
    READ_STRING_FIELD(minStringValue);
    READ_ULONG_FIELD(maxIntValue);
    READ_FLOAT_FIELD(maxFloatValue);
    READ_STRING_FIELD(maxStringValue);

    READ_BOOL_FIELD(addMinMax);
    READ_BOOL_FIELD(hasMM);
    READ_ENUM_FIELD(bfType, BloomFilterType);
    READ_OID_FIELD(dataType);
    READ_INT_FIELD(typeMod);
    READ_OID_FIELD(collation);

    READ_TYPEINFO_FIELD(dataType);
    READ_DONE();
}

/**
 * @Description: deserialize the PurgeStmt struct.
 * @in str, deserialized string.
 * @return PurgeStmt struct.
 */
static PurgeStmt* ReadPurgeStmt()
{
    READ_LOCALS(PurgeStmt);
    READ_ENUM_FIELD(purtype, PurgeType);
    READ_NODE_FIELD(purobj);

    READ_DONE();
}

/**
 * @Description: deserialize the TimeCapsuleStmt struct.
 * @in str, deserialized string.
 * @return TimeCapsuleStmt struct.
 */
static TimeCapsuleStmt* ReadTimeCapsuleStmt()
{
    READ_LOCALS(TimeCapsuleStmt);
    READ_ENUM_FIELD(tcaptype, TimeCapsuleType);
    READ_NODE_FIELD(relation);
    READ_STRING_FIELD(new_relname);

    READ_NODE_FIELD(tvver);
    READ_ENUM_FIELD(tvtype, TvVersionType);

    READ_DONE();
}

/**
 * @Description: deserialize the CommentStmt struct.
 * @in str, deserialized string.
 * @return CommentStmt struct.
 */
static CommentStmt* _readCommentStmt()
{
    READ_LOCALS(CommentStmt);
    READ_ENUM_FIELD(objtype, ObjectType);
    READ_NODE_FIELD(objname);
    READ_NODE_FIELD(objargs);
    READ_STRING_FIELD(comment);

    READ_DONE();
}

/**
 * @Description: deserialize the TableLikeCtx struct.
 * @in str, deserialized string.
 * @return TableLikeCtx struct.
 */
static TableLikeCtx* _readTableLikeCtx()
{
    READ_LOCALS(TableLikeCtx);

    READ_UINT_FIELD(options);
    READ_BOOL_FIELD(temp_table);
    READ_BOOL_FIELD(hasoids);
    READ_NODE_FIELD(columns);
    READ_NODE_FIELD(ckconstraints);
    READ_NODE_FIELD(comments);
    READ_NODE_FIELD(cluster_keys);
    READ_NODE_FIELD(partition);
    READ_NODE_FIELD(inh_indexes);
    READ_NODE_FIELD(reloptions);

    READ_DONE();
}

/**
 * @Description: deserialize the ColumnDef struct.
 * @in str, deserialized string.
 * @return ColumnDef struct.
 */
static ColumnDef* _readColumnDef()
{
    READ_LOCALS(ColumnDef);

    READ_STRING_FIELD(colname);
    READ_NODE_FIELD(typname);
    IF_EXIST(kvtype) {
        READ_INT_FIELD(kvtype);
    }
    READ_INT_FIELD(inhcount);
    READ_BOOL_FIELD(is_local);
    READ_BOOL_FIELD(is_not_null);
    READ_BOOL_FIELD(is_from_type);
    READ_BOOL_FIELD(is_serial);
    READ_CHAR_FIELD(storage);
    READ_ENUM_FIELD(cmprs_mode, int8);
    READ_NODE_FIELD(raw_default);
    READ_NODE_FIELD(cooked_default);
    READ_NODE_FIELD(collClause);
    READ_OID_FIELD(collOid);
    READ_NODE_FIELD(constraints);
    READ_NODE_FIELD(fdwoptions);
    READ_NODE_FIELD(clientLogicColumnRef);
    if (local_node->storage == '0') {
        local_node->storage = 0;
    }

    IF_EXIST(generatedCol) {
        READ_CHAR_FIELD(generatedCol);
    }

    READ_DONE();
}

/**
 * @Description: deserialize the ColumnRef struct.
 * @in str, deserialized string.
 * @return ColumnRef struct.
 */
static ColumnRef* _readColumnRef()
{
    READ_LOCALS(ColumnRef);

    READ_NODE_FIELD(fields);
    IF_EXIST(prior) {
        READ_BOOL_FIELD(prior);
    }
    IF_EXIST(indnum) {
        READ_INT_FIELD(indnum);
    }
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/**
 * @Description: deserialize the TypeName struct.
 * @in str, deserialized string.
 * @return TypeName struct.
 */
static TypeName* _readTypeName()
{
    READ_LOCALS(TypeName);

    READ_NODE_FIELD(names);
    READ_OID_FIELD(typeOid);
    READ_BOOL_FIELD(setof);
    READ_BOOL_FIELD(pct_type);
    READ_NODE_FIELD(typmods);
    READ_INT_FIELD(typemod);
    READ_NODE_FIELD(arrayBounds);
    READ_LOCATION_FIELD(location);
    IF_EXIST(pct_rowtype)
    {
        READ_BOOL_FIELD(pct_rowtype);
    }
    IF_EXIST(end_location)
    {
        READ_LOCATION_FIELD(end_location);
    }

    READ_TYPEINFO_FIELD(typeOid);
    READ_DONE();
}

/**
 * @Description: deserialize the IndexElem struct.
 * @in str, deserialized string.
 * @return IndexElem struct.
 */
static IndexElem* _readIndexElem()
{
    READ_LOCALS(IndexElem);

    READ_STRING_FIELD(name);
    READ_NODE_FIELD(expr);
    READ_STRING_FIELD(indexcolname);
    READ_NODE_FIELD(collation);
    READ_NODE_FIELD(opclass);
    READ_ENUM_FIELD(ordering, SortByDir);
    READ_ENUM_FIELD(nulls_ordering, SortByNulls);

    READ_DONE();
}

/**
 * @Description: deserialize the IndexStmt struct.
 * @in str, deserialized string.
 * @return IndexStmt struct.
 */
static IndexStmt* _readIndexStmt()
{
    READ_LOCALS(IndexStmt);

    READ_STRING_FIELD(schemaname);
    READ_STRING_FIELD(idxname);
    READ_NODE_FIELD(relation);
    READ_STRING_FIELD(accessMethod);
    READ_STRING_FIELD(tableSpace);
    READ_NODE_FIELD(indexParams);
    if (t_thrd.proc->workingVersionNum >= SUPPORT_GPI_VERSION_NUM) {
        READ_NODE_FIELD(indexIncludingParams);
        READ_BOOL_FIELD(isGlobal);
    }
    READ_NODE_FIELD(options);
    READ_NODE_FIELD(whereClause);
    READ_NODE_FIELD(excludeOpNames);
    READ_STRING_FIELD(idxcomment);
    READ_OID_FIELD(indexOid);
    READ_OID_FIELD(oldNode);
    READ_NODE_FIELD(partClause);
    READ_BOOL_FIELD(isPartitioned);
    READ_BOOL_FIELD(unique);
    READ_BOOL_FIELD(primary);
    READ_BOOL_FIELD(isconstraint);
    READ_BOOL_FIELD(deferrable);
    READ_BOOL_FIELD(initdeferred);
    READ_BOOL_FIELD(concurrent);
    READ_NODE_FIELD(inforConstraint);

    READ_DONE();
}

/**
 * @Description: deserialize the Constraint struct.
 * @in str, deserialized string.
 * @return Constraint struct.
 */
static Constraint* _readConstraint()
{
    READ_LOCALS(Constraint);

    READ_STRING_FIELD(conname);
    READ_BOOL_FIELD(deferrable);
    READ_BOOL_FIELD(initdeferred);
    READ_LOCATION_FIELD(location);

    token = pg_strtok(&length); /* skip :fldname */
    token = pg_strtok(&length); /* get field value */

#define MATCH_TYPE(tokname) (length == (sizeof(tokname) - 1) && memcmp(token, tokname, sizeof(tokname) - 1) == 0)

    if (MATCH_TYPE("NULL")) {
        local_node->contype = CONSTR_NULL;
    } else if (MATCH_TYPE("NOT_NULL")) {
        local_node->contype = CONSTR_NOTNULL;
    } else if (MATCH_TYPE("DEFAULT")) {
        local_node->contype = CONSTR_DEFAULT;
        READ_NODE_FIELD(raw_expr);
        READ_STRING_FIELD(cooked_expr);
    } else if (MATCH_TYPE("CHECK")) {
        local_node->contype = CONSTR_CHECK;
        READ_BOOL_FIELD(is_no_inherit);
        READ_NODE_FIELD(raw_expr);
        READ_STRING_FIELD(cooked_expr);
    } else if (MATCH_TYPE("PRIMARY_KEY")) {
        local_node->contype = CONSTR_PRIMARY;
        READ_NODE_FIELD(keys);
        if (t_thrd.proc->workingVersionNum >= SUPPORT_GPI_VERSION_NUM) {
            READ_NODE_FIELD(including);
        }
        READ_NODE_FIELD(options);
        READ_STRING_FIELD(indexname);
        READ_STRING_FIELD(indexspace);
    } else if (MATCH_TYPE("UNIQUE")) {
        local_node->contype = CONSTR_UNIQUE;
        READ_NODE_FIELD(keys);
        if (t_thrd.proc->workingVersionNum >= SUPPORT_GPI_VERSION_NUM) {
            READ_NODE_FIELD(including);
        }
        READ_NODE_FIELD(options);
        READ_STRING_FIELD(indexname);
        READ_STRING_FIELD(indexspace);
    } else if (MATCH_TYPE("EXCLUSION")) {
        local_node->contype = CONSTR_EXCLUSION;
        READ_NODE_FIELD(exclusions);
        if (t_thrd.proc->workingVersionNum >= SUPPORT_GPI_VERSION_NUM) {
            READ_NODE_FIELD(including);
        }
        READ_NODE_FIELD(options);
        READ_STRING_FIELD(indexname);
        READ_STRING_FIELD(indexspace);
        READ_STRING_FIELD(access_method);
        READ_NODE_FIELD(where_clause);
    } else if (MATCH_TYPE("FOREIGN_KEY")) {
        local_node->contype = CONSTR_FOREIGN;
        READ_NODE_FIELD(pktable);
        READ_NODE_FIELD(fk_attrs);
        READ_NODE_FIELD(pk_attrs);
        READ_CHAR_FIELD(fk_matchtype);
        READ_CHAR_FIELD(fk_upd_action);
        READ_CHAR_FIELD(fk_del_action);
        READ_NODE_FIELD(old_conpfeqop);
        READ_NODE_FIELD(old_pktable_oid);
        READ_BOOL_FIELD(skip_validation);
        READ_BOOL_FIELD(initially_valid);
    } else if (MATCH_TYPE("CLUSTER")) {
        local_node->contype = CONSTR_CLUSTER;
        READ_NODE_FIELD(keys);
    } else if (MATCH_TYPE("ATTR_DEFERRABLE")) {
        local_node->contype = CONSTR_ATTR_DEFERRABLE;
    } else if (MATCH_TYPE("ATTR_NOT_DEFERRABLE")) {
        local_node->contype = CONSTR_ATTR_NOT_DEFERRABLE;
    } else if (MATCH_TYPE("ATTR_DEFERRED")) {
        local_node->contype = CONSTR_ATTR_DEFERRED;
    } else if (MATCH_TYPE("ATTR_IMMEDIATE")) {
        local_node->contype = CONSTR_ATTR_IMMEDIATE;
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("_readConstraint(): badly contype \"%s\"...", token)));
    }

    READ_DONE();
}

static ClientLogicColumnParam* _readColumnParam ()
{
    READ_LOCALS(ClientLogicColumnParam);
    READ_ENUM_FIELD(key, ClientLogicColumnProperty);
    READ_STRING_FIELD(value);
    READ_UINT_FIELD(len);
    READ_LOCATION_FIELD(location);
    READ_DONE();
}

static ClientLogicGlobalParam* _readGlobalParam ()
{
    READ_LOCALS(ClientLogicGlobalParam);
    READ_ENUM_FIELD(key, ClientLogicGlobalProperty);
    READ_STRING_FIELD(value);
    READ_UINT_FIELD(len);
    READ_LOCATION_FIELD(location);
    READ_DONE();
}

static CreateClientLogicGlobal* _readGlobalSetting ()
{
    READ_LOCALS(CreateClientLogicGlobal);
    READ_NODE_FIELD(global_key_name);
    READ_NODE_FIELD(global_setting_params);
    READ_DONE();
}

static CreateClientLogicColumn* _readColumnSetting ()
{
    READ_LOCALS(CreateClientLogicColumn);
    READ_NODE_FIELD(column_key_name);
    READ_NODE_FIELD(column_setting_params);
    READ_DONE();
}

/**
 * @Description: deserialize the ClientLogicColumnRef struct.
 * @in str, deserialized string.
 * @return clientLogicColumnRef struct.
 */
static ClientLogicColumnRef* _readClientLogicColumnRef()
{
    READ_LOCALS(ClientLogicColumnRef);

    READ_NODE_FIELD(column_key_name);
    READ_NODE_FIELD(orig_typname);
    READ_NODE_FIELD(dest_typname);
    READ_LOCATION_FIELD(location);

    READ_DONE();
}

/**
 * @Description: deserialize the RangePartitionDefState struct.
 * @in str, deserialized string.
 * @return RangePartitionDefState struct.
 */
static RangePartitionDefState* _readRangePartitionDefState()
{
    READ_LOCALS(RangePartitionDefState);

    READ_STRING_FIELD(partitionName);
    READ_NODE_FIELD(boundary);
    READ_STRING_FIELD(tablespacename);

    READ_DONE();
}

static ListPartitionDefState* _readListPartitionDefState()
{
    READ_LOCALS(ListPartitionDefState);

    READ_STRING_FIELD(partitionName);
    READ_NODE_FIELD(boundary);
    READ_STRING_FIELD(tablespacename);

    READ_DONE();
}

static HashPartitionDefState* _readHashPartitionDefState()
{
    READ_LOCALS(HashPartitionDefState);

    READ_STRING_FIELD(partitionName);
    READ_NODE_FIELD(boundary);
    READ_STRING_FIELD(tablespacename);

    READ_DONE();
}

/**
 * @Description: deserialize the IntervalPartitionDefState struct.
 * @in str, deserialized string.
 * @return IntervalPartitionDefState struct.
 */
static IntervalPartitionDefState* _readIntervalPartitionDefState()
{
    READ_LOCALS(IntervalPartitionDefState);

    READ_NODE_FIELD(partInterval);
    READ_NODE_FIELD(intervalTablespaces);

    READ_DONE();
}

/**
 * @Description: deserialize the PartitionState struct.
 * @in str, deserialized string.
 * @return PartitionState struct.
 */
static PartitionState* _readPartitionState()
{
    READ_LOCALS(PartitionState);

    READ_CHAR_FIELD(partitionStrategy);
    READ_NODE_FIELD(intervalPartDef);
    READ_NODE_FIELD(partitionKey);
    READ_NODE_FIELD(partitionList);
    READ_ENUM_FIELD(rowMovement, RowMovementValue);
    READ_NODE_FIELD(subPartitionState);
    READ_NODE_FIELD(partitionNameList);

    if (local_node->partitionStrategy == '0') {
        local_node->partitionStrategy = 0;
    }

    READ_DONE();
}

/**
 * @Description: deserialize the RangePartitionindexDefState struct.
 * @in str, deserialized string.
 * @return RangePartitionindexDefState struct.
 */
static RangePartitionindexDefState* _readRangePartitionindexDefState()
{
    READ_LOCALS(RangePartitionindexDefState);

    READ_STRING_FIELD(name);
    READ_STRING_FIELD(tablespace);
    READ_NODE_FIELD(sublist);

    READ_DONE();
}

/**
 * @Description: deserialize the RangePartitionStartEndDefState struct.
 * @in str, deserialized string.
 * @return RangePartitionStartEndDefState struct.
 */
static RangePartitionStartEndDefState* _readRangePartitionStartEndDefState()
{
    READ_LOCALS(RangePartitionStartEndDefState);

    READ_STRING_FIELD(partitionName);
    READ_NODE_FIELD(startValue);
    READ_NODE_FIELD(endValue);
    READ_NODE_FIELD(everyValue);
    READ_STRING_FIELD(tableSpaceName);

    READ_DONE();
}

/**
 * @Description: deserialize the SplitPartitionState struct.
 * @in str, deserialized string.
 * @return SplitPartitionState struct.
 */
static SplitPartitionState* _readSplitPartitionState()
{
    READ_LOCALS(SplitPartitionState);

    IF_EXIST(splitType) {
        READ_ENUM_FIELD(splitType, SplitPartitionType);
    }
    READ_STRING_FIELD(src_partition_name);
    READ_NODE_FIELD(partition_for_values);
    READ_NODE_FIELD(split_point);
    READ_NODE_FIELD(dest_partition_define_list);
    IF_EXIST(newListSubPartitionBoundry) {
        READ_NODE_FIELD(newListSubPartitionBoundry);
    }

    READ_DONE();
}

/**
 * @Description: deserialize the AddPartitionState struct.
 * @in str, deserialized string.
 * @return AddPartitionState struct.
 */
static AddPartitionState* _readAddPartitionState()
{
    READ_LOCALS(AddPartitionState);

    READ_NODE_FIELD(partitionList);
    READ_BOOL_FIELD(isStartEnd);

    READ_DONE();
}

static AddSubPartitionState* _readAddSubPartitionState()
{
    READ_LOCALS(AddSubPartitionState);

    READ_NODE_FIELD(subPartitionList);
    READ_STRING_FIELD(partitionName);

    READ_DONE();
}

static QualSkewInfo* _readQualSkewInfo()
{
    READ_LOCALS_NO_FIELDS(QualSkewInfo);
    READ_TEMP_LOCALS();

    READ_ENUM_FIELD(skew_stream_type, SkewStreamType);
    READ_NODE_FIELD(skew_quals);
    READ_FLOAT_FIELD(qual_cost.startup);
    READ_FLOAT_FIELD(qual_cost.per_tuple);
    READ_FLOAT_FIELD(broadcast_ratio);

    READ_DONE();
}

static TdigestData* _readTdigestData()
{

    READ_TEMP_LOCALS();
    token = pg_strtok(&length);
    token = pg_strtok(&length);
    double compression = atof(token);
    int compressNum = 6;
    int compressAdd = 10;
    TdigestData* local_node = makeNodeWithSize(TdigestData, sizeof(TdigestData) +
        (((compressNum * (int)compression) + compressAdd) * sizeof(CentroidPoint)));
    local_node->compression = compression;

    READ_INT_FIELD(cap);
    READ_INT_FIELD(merged_nodes);
    READ_INT_FIELD(unmerged_nodes);
    READ_FLOAT_FIELD(merged_count);
    READ_FLOAT_FIELD(unmerged_count);
    READ_FLOAT_FIELD(valuetoc);

    for (int i = 0; i < (local_node->merged_nodes + local_node->unmerged_nodes); i++) {
        READ_FLOAT_FIELD(nodes[i].mean);
        token = pg_strtok(&length);
        local_node->nodes[i].count = atol(token);
    }
    READ_DONE();
}

/*
 * parseNodeString
 *
 * Given a character string representing a node tree, parseNodeString creates
 * the internal node structure.
 *
 * The string to be read must already have been loaded into pg_strtok().
 */
Node* parseNodeString(void)
{
    void* return_value = NULL;

    READ_TEMP_LOCALS();

    /* Guard against stack overflow due to overly complex expressions */
    check_stack_depth();

    token = pg_strtok(&length);

#define MATCH(tokname, namelen) ((length == (namelen)) && (memcmp(token, (tokname), (namelen)) == 0))

    if (MATCH("QUERY", 5)) {
        return_value = _readQuery();
    } else if (MATCH("SORTGROUPCLAUSE", 15)) {
        return_value = _readSortGroupClause();
    } else if (MATCH("GROUPINGSET", 11)) {
        return_value = _readGroupingSet();
    } else if (MATCH("WINDOWCLAUSE", 12)) {
        return_value = _readWindowClause();
    } else if (MATCH("ROWMARKCLAUSE", 13)) {
        return_value = _readRowMarkClause();
    } else if (MATCH("COMMONTABLEEXPR", 15)) {
        return_value = _readCommonTableExpr();
    } else if (MATCH("STARTWITHTARGETRELINFO", 22)) {
        return_value = _readStartWithTargetRelInfo();
    } else if (MATCH("SETOPERATIONSTMT", 16)) {
        return_value = _readSetOperationStmt();
    } else if (MATCH("ALIAS", 5)) {
        return_value = _readAlias();
    } else if (MATCH("RANGEVAR", 8)) {
        return_value = _readRangeVar();
    } else if (MATCH("INTOCLAUSE", 10)) {
        return_value = _readIntoClause();
    } else if (MATCH("VAR", 3)) {
        return_value = _readVar();
    } else if (MATCH("CONST", 5)) {
        return_value = _readConst();
    } else if (MATCH("PARAM", 5)) {
        return_value = _readParam();
    } else if (MATCH("AGGREF", 6)) {
        return_value = _readAggref();
    } else if (MATCH("GROUPINGFUNC", 12)) {
        return_value = _readGroupingFunc();
    } else if (MATCH("GROUPINGID", 10)) {
        return_value = _readGroupingId();
    } else if (MATCH("WINDOWFUNC", 10)) {
        return_value = _readWindowFunc();
    } else if (MATCH("ARRAYREF", 8)) {
        return_value = _readArrayRef();
    } else if (MATCH("FUNCEXPR", 8)) {
        return_value = _readFuncExpr();
    } else if (MATCH("NAMEDARGEXPR", 12)) {
        return_value = _readNamedArgExpr();
    } else if (MATCH("OPEXPR", 6)) {
        return_value = _readOpExpr();
    } else if (MATCH("DISTINCTEXPR", 12)) {
        return_value = _readDistinctExpr();
    } else if (MATCH("NULLIFEXPR", 10)) {
        return_value = _readNullIfExpr();
    } else if (MATCH("SCALARARRAYOPEXPR", 17)) {
        return_value = _readScalarArrayOpExpr();
    } else if (MATCH("BOOLEXPR", 8)) {
        return_value = _readBoolExpr();
    } else if (MATCH("SUBLINK", 7)) {
        return_value = _readSubLink();
    } else if (MATCH("FIELDSELECT", 11)) {
        return_value = _readFieldSelect();
    } else if (MATCH("FIELDSTORE", 10)) {
        return_value = _readFieldStore();
    } else if (MATCH("RELABELTYPE", 11)) {
        return_value = _readRelabelType();
    } else if (MATCH("COERCEVIAIO", 11)) {
        return_value = _readCoerceViaIO();
    } else if (MATCH("ARRAYCOERCEEXPR", 15)) {
        return_value = _readArrayCoerceExpr();
    } else if (MATCH("CONVERTROWTYPEEXPR", 18)) {
        return_value = _readConvertRowtypeExpr();
    } else if (MATCH("COLLATE", 7)) {
        return_value = _readCollateExpr();
    } else if (MATCH("CASE", 4)) {
        return_value = _readCaseExpr();
    } else if (MATCH("WHEN", 4)) {
        return_value = _readCaseWhen();
    } else if (MATCH("CASETESTEXPR", 12)) {
        return_value = _readCaseTestExpr();
    } else if (MATCH("ARRAY", 5)) {
        return_value = _readArrayExpr();
    } else if (MATCH("ROW", 3)) {
        return_value = _readRowExpr();
    } else if (MATCH("ROWCOMPARE", 10)) {
        return_value = _readRowCompareExpr();
    } else if (MATCH("COALESCE", 8)) {
        return_value = _readCoalesceExpr();
    } else if (MATCH("MINMAX", 6)) {
        return_value = _readMinMaxExpr();
    } else if (MATCH("XMLEXPR", 7)) {
        return_value = _readXmlExpr();
    } else if (MATCH("NULLTEST", 8)) {
        return_value = _readNullTest();
    } else if (MATCH("HASHFILTER", 10)) {
        return_value = _readHashFilter();
    } else if (MATCH("BOOLEANTEST", 11)) {
        return_value = _readBooleanTest();
    } else if (MATCH("COERCETODOMAIN", 14)) {
        return_value = _readCoerceToDomain();
    } else if (MATCH("COERCETODOMAINVALUE", 19)) {
        return_value = _readCoerceToDomainValue();
    } else if (MATCH("SETTODEFAULT", 12)) {
        return_value = _readSetToDefault();
    } else if (MATCH("CURRENTOFEXPR", 13)) {
        return_value = _readCurrentOfExpr();
    } else if (MATCH("TARGETENTRY", 11)) {
        return_value = _readTargetEntry();
    } else if (MATCH("PSEUDOTARGETENTRY", 17)) {
        return_value = _readPseudoTargetEntry();
    } else if (MATCH("STARTWITHOPTIONS", 16)) {
        return_value = _readStartWithOptions(NULL);
    } else if (MATCH("STARTWITHOP", 11)) {
        return_value = _readStartWithOp(NULL);
    } else if (MATCH("RANGETBLREF", 11)) {
        return_value = _readRangeTblRef();
    } else if (MATCH("JOINEXPR", 8)) {
        return_value = _readJoinExpr();
    } else if (MATCH("FROMEXPR", 8)) {
        return_value = _readFromExpr();
    } else if (MATCH("MERGEACTION", 11)) {
        return_value = _readMergeAction();
    } else if (MATCH("MERGEINTO", 9)) {
        return_value = _readMergeStmt();
    } else if (MATCH("RTE", 3)) {
        return_value = _readRangeTblEntry();
    } else if (MATCH("TABLESAMPLECLAUSE", 17)) {
        return_value = _readTableSampleClause();
    } else if (MATCH("TIMECAPSULECLAUSE", 17)) {
        return_value = ReadTimeCapsuleClause();
    } else if (MATCH("NOTIFY", 6)) {
        return_value = _readNotifyStmt();
    } else if (MATCH("DECLARECURSOR", 13)) {
        return_value = _readDeclareCursorStmt();
    } else if (MATCH("NESTLOOP", 8)) {
        return_value = _readNestLoop();
    } else if (MATCH("SEQSCAN", 7)) {
        return_value = _readSeqScan();
    } else if (MATCH("BITMAPHEAPSCAN", 14)) {
        return_value = _readBitmapHeapScan(NULL);
    } else if (MATCH("BITMAPINDEXSCAN", 15)) {
        return_value = _readBitmapIndexScan(NULL);
    } else if (MATCH("STREAM", 6)) {
        return_value = _readStream(NULL);
    } else if (MATCH("LIMIT", 5)) {
        return_value = _readLimit(NULL);
    } else if (MATCH("AGG", 3)) {
        return_value = _readAgg(NULL);
    } else if (MATCH("SCAN", 4)) {
        return_value = _readScan(NULL);
    } else if (MATCH("BUCKETINFO", 10)) {
        return_value = _readBucketInfo(NULL);
    } else if (MATCH("SUBQUERYSCAN", 12)) {
        return_value = _readSubqueryScan(NULL);
    } else if (MATCH("INDEXSCAN", 9)) {
        return_value = _readIndexScan(NULL);
    } else if (MATCH("JOIN", 4)) {
        return_value = _readJoin(NULL);
    } else if (MATCH("HASH", 4)) {
        return_value = _readHash(NULL);
    } else if (MATCH("HASHJOIN", 8)) {
        return_value = _readHashJoin(NULL);
    } else if (MATCH("MERGEJOIN", 9)) {
        return_value = _readMergeJoin(NULL);
    } else if (MATCH("REMOTEQUERY", 11)) {
        return_value = _readRemoteQuery();
    } else if (MATCH("EXEC_NODES", 10)) {
        return_value = _readExecNodes();
    } else if (MATCH("SLICEBOUNDARY", 13)) {
        return_value = _readSliceBoundary();
    } else if (MATCH("EXECBOUNDARY", 12)) {
        return_value = _readExecBoundary();
    } else if (MATCH("PLAN", 4)) {
        return_value = _readPlan(NULL);
    } else if (MATCH("MATERIAL", 8)) {
        return_value = _readMaterial(NULL);
    } else if (MATCH("APPEND", 6)) {
        return_value = _readAppend(NULL);
    } else if (MATCH("MERGEAPPEND", 11)) {
        return_value = _readMergeAppend(NULL);
    } else if (MATCH("SUBPLAN", 7)) {
        return_value = _readSubPlan(NULL);
    } else if (MATCH("SIMPLESORT", 10)) {
        return_value = _readSimpleSort(NULL);
    } else if (MATCH("SORT", 4)) {
        return_value = _readSort(NULL);
    } else if (MATCH("UNIQUE", 6)) {
        return_value = _readUnique(NULL);
    } else if (MATCH("PLANNEDSTMT", 11)) {
        return_value = _readPlannedStmt();
    } else if (MATCH("SETOP", 5)) {
        return_value = _readSetOp(NULL);
    } else if (MATCH("GROUP", 5)) {
        return_value = _readGroup(NULL);
    } else if (MATCH("LOCKROWS", 8)) {
        return_value = _readLockRows(NULL);
    } else if (MATCH("CTESCAN", 7)) {
        return_value = _readCteScan(NULL);
    } else if (MATCH("WINDOWAGG", 9)) {
        return_value = _readWindowAgg(NULL);
    } else if (MATCH("MODIFYTABLE", 11)) {
        return_value = _readModifyTable(NULL);
    } else if (MATCH("MERGEWHENCLAUSE", 15)) {
        return_value = _readMergeWhenClause();
    } else if (MATCH("RESULT", 6)) {
        return_value = _readResult(NULL);
    } else if (MATCH("VALUESSCAN", 10)) {
        return_value = _readValuesScan(NULL);
    } else if (MATCH("FUNCTIONSCAN", 12)) {
        return_value = _readFunctionScan(NULL);
    } else if (MATCH("RECURSIVEUNION", 14)) {
        return_value = _readRecursiveUnion(NULL);
    } else if (MATCH("WORKTABLESCAN", 13)) {
        return_value = _readWorkTableScan(NULL);
    } else if (MATCH("PLANINVALITEM", 13)) {
        return_value = _readPlanInvalItem(NULL);
    } else if (MATCH("BITMAPOR", 8)) {
        return_value = _readBitmapOr(NULL);
    } else if (MATCH("PLANROWMARK", 11)) {
        return_value = _readPlanRowMark();
    } else if (MATCH("WINDOWAGG", 9)) {
        return_value = _readWindowAgg(NULL);
    } else if (MATCH("FOREIGNSCAN", 11)) {
        return_value = _readForeignScan(NULL);
    } else if (MATCH("EXTENSIBLEPLAN", 14)) {
        return_value = _readExtensiblePlan(NULL);
    } else if (MATCH("FOREIGNPARTSTATE", 16)) {
        return_value = _readForeignPartState(NULL);
    } else if (MATCH("BITMAPAND", 9)) {
        return_value = _readBitmapAnd(NULL);
    } else if (MATCH("INDEXONLYSCAN", 13)) {
        return_value = _readIndexOnlyScan(NULL);
    } else if (MATCH("DFSINDEXSCAN", 12)) {
        return_value = _readDfsIndexScan(NULL);
    } else if (MATCH("CSTOREINDEXSCAN", 15)) {
        return_value = _readCStoreIndexScan(NULL);
    } else if (MATCH("CSTOREINDEXCTIDSCAN", 19)) {
        return_value = _readCStoreIndexCtidScan(NULL);
    } else if (MATCH("CSTOREINDEXHEAPSCAN", 19)) {
        return_value = _readCStoreIndexHeapScan(NULL);
    } else if (MATCH("CSTOREINDEXAND", 14)) {
        return_value = _readCStoreIndexAnd(NULL);
    } else if (MATCH("CSTOREINDEXOR", 13)) {
        return_value = _readCStoreIndexOr(NULL);
    } else if (MATCH("PRUNINGRESULT", 13)) {
        return_value = _readPruningResult(NULL);
    } else if (MATCH("SUBPARTITIONPRUNINGRESULT", 25)) {
        return_value = _readSubPartitionPruningResult(NULL);
    } else if (MATCH("PARTITERATOR", 12)) {
        return_value = _readPartIterator(NULL);
    } else if (MATCH("PARTITERATORPARAM", 17)) {
        return_value = _readPartIteratorParam(NULL);
    } else if (MATCH("NESTLOOPPARAM", 13)) {
        return_value = _readNestLoopParam();
    } else if (MATCH("DISTFDWDATANODETASK", 19)) {
        return_value = _readDistFdwDataNodeTask(NULL);
    } else if (MATCH("DISTFDWFILESEGMENT", 18)) {
        return_value = _readDistFdwFileSegment(NULL);
    } else if (MATCH("SPLITINFO", 9)) {
        return_value = _readSplitInfo(NULL);
    } else if (MATCH("SPLITMAP", 8)) {
        return_value = _readSplitMap(NULL);
    } else if (MATCH("DFSPRIVATEITEM", 14)) {
        return_value = _readDfsPrivateItem(NULL);
    } else if (MATCH("DEFELEM", 7)) {
        return_value = _readDefElem(NULL);
    } else if (MATCH("TIDSCAN", 7)) {
        return_value = _readTidScan(NULL);
    } else if (MATCH("ERRORCACHEENTRY", 15)) {
        return_value = _readErrorCacheEntry(NULL);
    } else if (MATCH("ROWTOVEC", 8)) {
        return_value = _readRowToVec();
    } else if (MATCH("VECTOROW", 8)) {
        return_value = _readVecToRow();
    } else if (MATCH("VECSORT", 7)) {
        return_value = _readVecSort(NULL);
    } else if (MATCH("VECRESULT", 9)) {
        return_value = _readVecResult(NULL);
    } else if (MATCH("CSTORESCAN", 10)) {
        return_value = _readCStoreScan(NULL);
    } else if (MATCH("DFSSCAN", 7)) {
        return_value = _readDfsScan(NULL);
#ifdef ENABLE_MULTIPLE_NODES
    } else if (MATCH("TSSTORESCAN",11)) {
        return_value = _readTsStoreScan(NULL);
#endif
    } else if (MATCH("VECSUBQUERYSCAN", 15)) {
        return_value = _readVecSubqueryScan(NULL);
    } else if (MATCH("VECHASHJOIN", 11)) {
        return_value = _readVecHashJoin(NULL);
    } else if (MATCH("VECAGG", 6)) {
        return_value = _readVecAgg(NULL);
    } else if (MATCH("VECPARTITERATOR", 15)) {
        return_value = _readVecPartIterator(NULL);
    } else if (MATCH("VECAPPEND", 9)) {
        return_value = _readVecAppend(NULL);
    } else if (MATCH("VECSETOP", 8)) {
        return_value = _readVecSetOp(NULL);
    } else if (MATCH("VECFOREIGNSCAN", 14)) {
        return_value = _readVecForeignScan(NULL);
    } else if (MATCH("VECMODIFYTABLE", 14)) {
        return_value = _readVecModifyTable(NULL);
    } else if (MATCH("VECSTREAM", 9)) {
        return_value = _readVecStream(NULL);
    } else if (MATCH("VECLIMIT", 8)) {
        return_value = _readVecLimit(NULL);
    } else if (MATCH("VECGROUP", 8)) {
        return_value = _readVecGroup(NULL);
    } else if (MATCH("VECUNIQUE", 9)) {
        return_value = _readVecUnique(NULL);
    } else if ((MATCH("VECNESTLOOP", 11))) {
        return_value = _readVecNestLoop();
    } else if ((MATCH("VECMATERIAL", 11))) {
        return_value = _readVecMaterial();
    } else if ((MATCH("VECREMOTEQUERY", 14))) {
        return_value = _readVecRemoteQuery();
    } else if (MATCH("VECMERGEJOIN", 12)) {
        return_value = _readVecMergeJoin(NULL);
    } else if (MATCH("VECWINDOWAGG", 12)) {
        return_value = _readVecWindowAgg(NULL);
    } else if (MATCH("HDFSTABLEANALYZE", 16)) {
        return_value = _readHDFSTableAnalyze(NULL);
    } else if (MATCH("PLACEHOLDERVAR", 14)) {
        return_value = _readPlaceHolderVar();
    } else if (MATCH("ATTRMETADATA", 12)) {
        return_value = _readAttrMetaData(NULL);
    } else if (MATCH("RELATIONMETADATA", 16)) {
        return_value = _readRelationMetaData(NULL);
    } else if (MATCH("FOREIGNOPTIONS", 14)) {
        return_value = _readForeignOptions(NULL);
    } else if (MATCH("BLOOMFILTERSET", 14)) {
        return_value = _readBloomFilterSet(NULL);
    } else if (MATCH("HINTSTATE", 9)) {
        return_value = _readHintState();
    } else if (MATCH("JOINHINT", 8)) {
        return_value = _readJoinHint();
    } else if (MATCH("LEADINGHINT", 11)) {
        return_value = _readLeadingHint();
    } else if (MATCH("ROWSHINT", 8)) {
        return_value = _readRowsHint();
    } else if (MATCH("STREAMHINT", 10)) {
        return_value = _readStreamHint();
    } else if (MATCH("BLOCKNAMEHINT", 13)) {
        return_value = _readBlockNameHint();
    } else if (MATCH("SCANHINT", 8)) {
        return_value = _readScanMethodHint();
    } else if (MATCH("PGFDWREMOTEINFO", 15)) {
        return_value = _readPgFdwRemoteInfo();
    } else if (MATCH("SKEWHINT", 8)) {
        return_value = _readSkewHint();
    } else if (MATCH("SKEWHINTTRANSF", 14)) {
        return_value = _readSkewHintTransf();
    } else if (MATCH("SKEWRELINFO", 11)) {
        return_value = _readSkewRelInfo();
    } else if (MATCH("SKEWCOLUMNINFO", 14)) {
        return_value = _readSkewColumnInfo();
    } else if (MATCH("SKEWVALUEINFO", 13)) {
        return_value = _readSkewValueInfo();
    } else if (MATCH("QUALSKEWINFO", 12)) {
        return_value = _readQualSkewInfo();
    } else if (MATCH("PURGESTMT", 9)) {
        return_value = ReadPurgeStmt();
    } else if (MATCH("TIMECAPSULESTMT", 15)) {
        return_value = ReadTimeCapsuleStmt();
    } else if (MATCH("COMMENTSTMT", 11)) {
        return_value = _readCommentStmt();
    } else if (MATCH("TABLELIKECTX", 12)) {
        return_value = _readTableLikeCtx();
    } else if (MATCH("COLUMNDEF", 9)) {
        return_value = _readColumnDef();
    } else if (MATCH("COLUMNREF", 9)) {
        return_value = _readColumnRef();
    } else if (MATCH("TYPENAME", 8)) {
        return_value = _readTypeName();
    } else if (MATCH("INDEXELEM", 9)) {
        return_value = _readIndexElem();
    } else if (MATCH("INDEXSTMT", 9)) {
        return_value = _readIndexStmt();
    } else if (MATCH("CONSTRAINT", 10)) {
        return_value = _readConstraint();
    } else if (MATCH("PARTITIONSTATE", 14)) {
        return_value = _readPartitionState();
    } else if (MATCH("RANGEPARTITIONDEFSTATE", 22)) {
        return_value = _readRangePartitionDefState();
    } else if (MATCH("LISTPARTITIONDEFSTATE", 21)) {
        return_value = _readListPartitionDefState();
    } else if (MATCH("HASHPARTITIONDEFSTATE", 21)) {
        return_value = _readHashPartitionDefState();
    } else if (MATCH("INTERVALPARTITIONDEFSTATE", 25)) {
        return_value = _readIntervalPartitionDefState();
    } else if (MATCH("RANGEPARTITIONINDEXDEFSTATE", 27)) {
        return_value = _readRangePartitionindexDefState();
    } else if (MATCH("RANGEPARTITIONSTARTENDDEFSTATE", 30)) {
        return_value = _readRangePartitionStartEndDefState();
    } else if (MATCH("SPLITPARTITIONSTATE", 19)) {
        return_value = _readSplitPartitionState();
    } else if (MATCH("ADDPARTITIONSTATE", 17)) {
        return_value = _readAddPartitionState();
    } else if (MATCH("ADDSUBPARTITIONSTATE", 20)) {
        return_value = _readAddSubPartitionState();
    } else if (MATCH("CLIENTLOGICCOLUMNREF", 20)) {
        return_value = _readClientLogicColumnRef();
    } else if (MATCH("GLOBALPARAM", 11)) {
        return_value = _readGlobalParam();
    } else if (MATCH("COLUMNPARAM", 11)) {
        return_value = _readColumnParam();
    } else if (MATCH("GLOBALSETTING", 13)) {
        return_value = _readGlobalSetting();
    } else if (MATCH("COLUMNSETTING", 13)) {
        return_value = _readColumnSetting();
    } else if (MATCH("UPSERTEXPR", 10)) {
        return_value = _readUpsertExpr();
    } else if (MATCH("UPSERTCLAUSE", 12)) {
        return_value = _readUpsertClause();
    } else if (MATCH("PREDPUSHHINT", 12)) {
        return_value = _readPredpushHint();
    } else if (MATCH("PREDPUSHSAMELEVELHINT", 21)) {
        return_value = _readPredpushSameLevelHint();
    } else if (MATCH("REWRITEHINT", 11)) {
        return_value = _readRewriteHint();
    } else if (MATCH("GATHERHINT", 10)) {
        return_value = _readGatherHint();
    } else if (MATCH("NOEXPANDHINT", 12)) {
        return_value = _readNoExpandHint();
    } else if (MATCH("SETHINT", 7)) {
        return_value = _readSetHint();
    } else if (MATCH("PLANCACHEHINT", 13)) {
        return_value = _readPlanCacheHint();
    } else if (MATCH("NOGPCHINT", 9)) {
        return_value = _readNoGPCHint();
    } else if (MATCH("ROWNUM", 6)) {
        return_value = _readRownum();
    } else if (MATCH("COPY", 4)) {
        return_value = _readCopyStmt();
    } else if (MATCH("ALTERTABLE", 10)) {
        return_value = _readAlterTableStmt();
    } else if (MATCH("PLDEBUG_VARIABLE", 16)) {
        return_value = _readPLDebug_variable(); 
    } else if (MATCH("PLDEBUG_BREAKPOINT", 18)) {
        return_value = _readPLDebug_breakPoint(); 
    } else if (MATCH("PLDEBUG_FRAME", 13)) {
        return_value = _readPLDebug_frame();
    } else if (MATCH("TdigestData", 11)) {
        return_value = _readTdigestData();
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("parseNodeString(): badly formatted node string \"%s\"...", token)));
        return_value = NULL; /* keep compiler quiet */
    }

    return (Node*)return_value;
}

/*
 * readDatum
 *
 * Given a string representation of a constant, recreate the appropriate
 * Datum.  The string representation embeds length info, but not byValue,
 * so we must be told that.
 */
static Datum readDatum(bool typbyval)
{
    Size length, i;
    int tokenLength;
    char* token = NULL;
    Datum res;
    char* s = NULL;

    /*
     * read the actual length of the value
     */
    token = pg_strtok(&tokenLength);
    length = atoui(token);

    token = pg_strtok(&tokenLength); /* read the '[' */
    if (token == NULL || token[0] != '[') {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("expected \"[\" to start datum, but got \"%s\"; length = %lu",
                    token ? (const char*)token : "[NULL]",
                    (unsigned long)length)));
    }

    if (typbyval) {
        if (length > (Size)sizeof(Datum))
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("byval datum but length = %lu", (unsigned long)length)));

        res = (Datum)0;
        s = (char*)(&res);
        for (i = 0; i < (Size)sizeof(Datum); i++) {
            token = pg_strtok(&tokenLength);
            s[i] = (char)atoi(token);
        }
    } else if (length <= 0) {
        res = (Datum)NULL;
    } else {
        s = (char*)palloc(length);
        for (i = 0; i < length; i++) {
            token = pg_strtok(&tokenLength);
            s[i] = (char)atoi(token);
        }
        res = PointerGetDatum(s);
    }

    token = pg_strtok(&tokenLength); /* read the ']' */
    if (token == NULL || token[0] != ']') {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("expected \"]\" to end datum, but got \"%s\"; length = %lu",
                    token ? (const char*)token : "[NULL]",
                    (unsigned long)length)));
    }

    return res;
}

/*
 * read_mem_info
 *
 * Given a string representation of OpMemInfo structure, recreate the structure
 */
static void read_mem_info(OpMemInfo* local_node)
{
    READ_TEMP_LOCALS();

    READ_FLOAT_FIELD(opMem);
    READ_FLOAT_FIELD(minMem);
    READ_FLOAT_FIELD(maxMem);
    READ_FLOAT_FIELD(regressCost);
}

/*
 * _readCursorData
 *
 * Given a string representation of Cursor_Data structure, recreate the structure
 */
static void _readCursorData(Cursor_Data* local_node)
{
    READ_TEMP_LOCALS();

    READ_INT_FIELD(row_count);
    READ_INT_FIELD(cur_dno);
    READ_BOOL_FIELD(is_open);
    READ_BOOL_FIELD(found);
    READ_BOOL_FIELD(not_found);
    READ_BOOL_FIELD(null_open);
    READ_BOOL_FIELD(null_fetch);
}

/*
 * stringToNode_skip_extern_fields
 *
 * The object's oid is different between different nodes, so we send the object name
 * and namespace to remote node, and re-parse the object name to local OID.
 * But sometimes no need to do this.
 * For example:
 * 	create table t (a int default func());
 * We send the SQL statement to other node, and the func LOCAL OID will be recorded
 * in pg_attrdef's adbin, so no need to do re-parse the object name to LOCAL OID.
 */
void* stringToNode_skip_extern_fields(char* str)
{
    void* retval = NULL;

    PG_TRY();
    {
        skip_read_extern_fields = true;

        retval = stringToNode(str);

        skip_read_extern_fields = false;
    }

    PG_CATCH();
    {
        skip_read_extern_fields = false;
        PG_RE_THROW();
    }

    PG_END_TRY();

    return retval;
}
