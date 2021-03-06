/* -------------------------------------------------------------------------
 *
 * pg_proc.cpp
 *	  routines to support manipulation of the pg_proc relation
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/backend/catalog/pg_proc.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/transam.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_language.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_object.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_proc_fn.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/user.h"
#include "commands/trigger.h"
#include "executor/functions.h"
#include "funcapi.h"
#include "gs_policy/gs_policy_masking.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_type.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgrtab.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#ifdef PGXC
#include "pgxc/execRemote.h"
#include "pgxc/pgxc.h"
#include "catalog/pg_class.h"
#endif
#include "executor/spi.h"

#ifndef WIN32_ONLY_COMPILER
#include "dynloader.h"
#else
#include "port/dynloader/win32.h"
#endif
#include "catalog/pg_authid.h"
#include "catalog/pgxc_node.h"
#include "access/heapam.h"
#include "postmaster/postmaster.h"
#include "commands/dbcommands.h"
#include "storage/lmgr.h"

#define TEMPSEPARATOR '@'
#define SEPARATOR '#'

#define SENDTOOTHERNODE 1
#define SENDTOBACKUP 2

typedef enum CFunType { NormalType = 0, DumpType } CFunType;

/*
 * If "Create function ... LANGUAGE SQL" include agg function, agg->aggtype
 * is the final aggtype. While for "Select agg()", agg->aggtype should be agg->aggtrantype.
 * Here we use Parse_sql_language to distinguish these two cases.
 */
Datum fmgr_internal_validator(PG_FUNCTION_ARGS);
Datum fmgr_c_validator(PG_FUNCTION_ARGS);
Datum fmgr_sql_validator(PG_FUNCTION_ARGS);

typedef struct {
    char* proname;
    char* prosrc;
} parse_error_callback_arg;

static void sql_function_parse_error_callback(void* arg);
static int match_prosrc_to_query(const char* prosrc, const char* queryText, int cursorpos);
static bool match_prosrc_to_literal(const char* prosrc, const char* literal, int cursorpos, int* newcursorpos);
static bool pgxc_query_contains_view(List* queries);

static void check_library_path(char* absolutePath, CFunType function_type);

static char* get_temp_library(bool absolute_path);
static char* get_final_library_path(const char* final_file_name);

static void send_library_other_node(char* absolutePath);

static void copyLibraryToSpecialName(char* absolutePath, const char* final_file_name,
    const char* libPath, CFunType function_type);
static void send_library_to_Backup(char* sourcePath);

static char* getCFunProbin(const char* probin, Oid procNamespace, Oid proowner,
    const char* filename, char** final_file_name);

static void checkFunctionConflicts(HeapTuple oldtup, const char* procedureName, Oid proowner, Oid returnType,
    Datum allParameterTypes, Datum parameterModes, Datum parameterNames, bool returnsSet, bool replace, bool isOraStyle,
    bool isAgg, bool isWindowFunc);
static bool user_define_func_check(Oid languageId, const char* probin, char** absolutePath, CFunType* function_type);
static const char* get_file_name(const char* filePath, CFunType function_type);

/*
 * @Description: Check character c if is special.
 * @in c: character.
 * @return: True or false.
 */
static bool check_special_character(char c)
{
    switch (c) {
        case ' ':
        case '|':
        case ';':
        case '&':
        case '$':
        case '<':
        case '>':
        case '`':
        case '\\':
        case '\'':
        case '\"':
        case '{':
        case '}':
        case '(':
        case ')':
        case '[':
        case ']':
        case '~':
        case '*':
        case '?':
        case '!':
            return false;
        default:
            break;
    }

    return true;
}

/*
 * @Description: Check this absolute path valid, and also check if we have read
 *               permission on it
 * @in absolutePath: Library file absolute path.
 * @in function_type: Mark if is dump function.
 */
static void check_library_path(char* absolutePath, CFunType function_type)
{
    if (!file_exists(absolutePath)) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("File \"%s\" does not exist.", absolutePath)));
    }

    /* Check if library file has read permission */
    if (-1 == access(absolutePath, R_OK)) {
        ereport(ERROR,
            (errcode_for_file_access(), errmsg("Library File \"%s\" does not have READ permission.", absolutePath)));
    }

    int len = strlen(absolutePath);

    /* Can not include whitespace in the path else can lead to invalid system operate. */
    for (int i = 0; i < len; i++) {
        if (!check_special_character(absolutePath[i]) || isspace(absolutePath[i])) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("Library path can not include character %c.", absolutePath[i])));
        }
    }

    const char* filename = get_file_name(absolutePath, function_type);

    len = strlen(filename);

    /* File name can not include '#' which will  be used as separator of namespace oid and filename. */
    for (int i = 0; i < len; i++) {
        if (filename[i] == SEPARATOR) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("Library file name can not include character \"%c\".", SEPARATOR)));
        }
    }
}

/*
 * @Description: Get temp library path/filename.
 * @return: Temp path.
 */
static char* get_temp_library(bool absolute_path)
{
    StringInfoData temp_file_strinfo;
    initStringInfo(&temp_file_strinfo);
    bool isExecCN = (IS_PGXC_COORDINATOR && !IsConnFromCoord());
    if (absolute_path) {
        appendStringInfo(&temp_file_strinfo,
            "%s/pg_plugin/%ld%lu",
            t_thrd.proc_cxt.pkglib_path,
            GetCurrentTransactionStartTimestamp(),
            (GTM_MODE) ? (GetCurrentTransactionId()) :
                          (isExecCN ? GetCurrentTransactionId() : t_thrd.xact_cxt.cn_xid));
    } else {
        appendStringInfo(&temp_file_strinfo,
            "$libdir/pg_plugin/%ld%lu",
            GetCurrentTransactionStartTimestamp(),
            (GTM_MODE) ? (GetCurrentTransactionId()) :
                          (isExecCN ? GetCurrentTransactionId() : t_thrd.xact_cxt.cn_xid));
    }

    return temp_file_strinfo.data;
}

/*
 * @Description: Get transfer file path.
 * @return: This path.
 */
char* get_transfer_path()
{
    StringInfoData strinfo;
    initStringInfo(&strinfo);

    appendStringInfo(&strinfo, "%s/../../bin/transfer.py", t_thrd.proc_cxt.pkglib_path);

    return strinfo.data;
}

/*
 * @Description: Copy library file to target file.
 * @sourceFile: Source file.
 * @targetFile: Target file.
 */
bool copy_library_file(const char* sourceFile, const char* targetFile)
{
#define BUF_SIZE (8 * BLCKSZ)
#define UNIT_SIZE 1

    char* buffer = NULL;
    FILE* srcFd = NULL;
    FILE* tarFp = NULL;

    srcFd = fopen(sourceFile, "rb");
    if (srcFd == NULL) {
        return false;
    }

    buffer = (char*)palloc0(BUF_SIZE * UNIT_SIZE);

    tarFp = fopen(targetFile, "wb");
    if (tarFp == NULL) {
        fclose(srcFd);
        pfree_ext(buffer);
        return false;
    }

    size_t nbytes = fread(buffer, UNIT_SIZE, BUF_SIZE, srcFd);
    while (nbytes != 0) {
        if (fwrite(buffer, UNIT_SIZE, nbytes, tarFp) != (size_t)nbytes) {
            fclose(srcFd);
            fclose(tarFp);
            pfree_ext(buffer);
            return false;
        }

        nbytes = fread(buffer, UNIT_SIZE, BUF_SIZE, srcFd);
    }

    if (fclose(srcFd)) {
        fclose(tarFp);
        pfree_ext(buffer);
        return false;
    }

    if (fclose(tarFp)) {
        pfree_ext(buffer);
        return false;
    }

    pfree_ext(buffer);
    return true;
}

/*
 * @Description: Send this library to other node.
 * @in absolutePath: Source absolute path.
 */
static void send_library_other_node(char* absolutePath)
{
    char* temp_library_name = get_temp_library(true);
    check_backend_env(temp_library_name);

    char* transfer_path = get_transfer_path();

    bool copy_result = true;

    /* transer file is not exixts, that be not clusters. */
    if (IS_SINGLE_NODE || !file_exists(transfer_path)) {
        /* single node mode don't need transfer.py. */
        if (!IS_SINGLE_NODE)
            ereport(LOG, (errcode_for_file_access(), errmsg("File transfer.py does not exist.")));

        copy_result = copy_library_file(absolutePath, temp_library_name);
        if (!copy_result) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("Copy file \"%s\" failed: %m", absolutePath)));
        }
        pfree_ext(temp_library_name);
        pfree_ext(transfer_path);
        return;
    }

    StringInfoData strinfo;
    initStringInfo(&strinfo);

    appendStringInfo(&strinfo, "python %s %d %s %s", transfer_path, SENDTOOTHERNODE, absolutePath, temp_library_name);

    int rc = system(strinfo.data);

    if (rc != 0) {
        ereport(ERROR,
            (errcode(ERRCODE_SYSTEM_ERROR), errmsg("Send library to all node fail: %m, command %s", strinfo.data)));
    }

    pfree_ext(temp_library_name);
    pfree_ext(transfer_path);
    pfree_ext(strinfo.data);
}

/*
 * @Description: Send library file to backup.
 * @in sourcePath: Source file path.
 */
static void send_library_to_Backup(char* sourcePath)
{
    char* transfer_path = get_transfer_path();

    StringInfoData strinfo;
    initStringInfo(&strinfo);

    appendStringInfo(&strinfo,
        "python %s %d %s %s",
        transfer_path,
        SENDTOBACKUP,
        sourcePath,
        g_instance.attr.attr_common.PGXCNodeName);

    int rc = system(strinfo.data);

    if (rc != 0) {
        ereport(ERROR,
            (errcode(ERRCODE_SYSTEM_ERROR), errmsg("Send library to backup fail: %m, command %s", strinfo.data)));
    }

    pfree_ext(transfer_path);
    pfree_ext(strinfo.data);
}

/*
 * @Description: Get final library path.
 * @in final_file_name: Final file name.
 */
static char* get_final_library_path(const char* final_file_name)
{
    StringInfoData tar_strinfo;
    initStringInfo(&tar_strinfo);

    appendStringInfo(&tar_strinfo, "%s/pg_plugin/%s", t_thrd.proc_cxt.pkglib_path, final_file_name);

    return tar_strinfo.data;
}

/*
 * @Decsription: Send library to other machine from coordinator.
 * @in absolutePath: Source path.
 * @in fun_name: Function name.
 * @in filename: File name.
 * @in function_type: If is dump function.
 */
static void copyLibraryToSpecialName(char* absolutePath, const char* final_file_name,
    const char* libPath, CFunType function_type)
{
    char* srcPath = NULL;
    char* targetPath = NULL;
    char* temp_path = NULL;

    bool copy_result = true;

    temp_path = get_temp_library(false);
    srcPath = expand_dynamic_library_name(temp_path);
    /* Source file do not exist in pg_plugin. */
    if (!file_exists(srcPath)) {
        /* Upgrading, library file always exists, we use original library. */
        if (DumpType == function_type && file_exists(absolutePath)) {
            srcPath = absolutePath;
        } else {
            ereport(ERROR,
                (errcode_for_file_access(),
                    errmsg("Library file \"%s\" does not exist.", srcPath),
                    errdetail("Source library file may be rollback-deleted or copy fail failed, please retry"),
                    errhint("Try Re-Submit CREATE FUNCTION")));
        }
    }

    /* This library need be deleted when commit or abort. */
    InsertIntoPendingLibraryDelete(temp_path, true);
    InsertIntoPendingLibraryDelete(temp_path, false);

    /* Get final library path. */
    targetPath = get_final_library_path(final_file_name);

    /* If this library file exist, we need close this file handle. */
    if (file_exists(targetPath)) {
        ereport(ERROR,
            (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                errmsg("abort transaction due to concurrent create function.")));
    }

    /* Copy library file to pkglib_path/pg_plugin. */
    copy_result = copy_library_file(srcPath, targetPath);
    if (!copy_result) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("Copy file \"%s\" failed: %m", absolutePath)));
    }

    /* This library file need be deleted when rollback. */
    InsertIntoPendingLibraryDelete(libPath, false);

    /* Send library file to Standby node. */
    if (t_thrd.postmaster_cxt.ReplConnArray[1] != NULL) {
        send_library_to_Backup(targetPath);
    }

    pfree_ext(temp_path);
    pfree_ext(targetPath);
}

/*
 * @Description: Get filename.
 * @in: File path.
 * @in function_type: If is dump function.
 * @return: File name.
 */
static const char* get_file_name(const char* filePath, CFunType function_type)
{
    const char* ret = NULL;

    if (NormalType == function_type) {
        ret = last_dir_separator(filePath);
    } else {
        Assert(DumpType == function_type);

        const char* p = NULL;
        for (p = filePath; *p; p++) {
            if (*p == SEPARATOR) {
                ret = p;
            }
        }
    }

    if (ret == NULL) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Invalid library path.")));
    }

    /* Skip '\', get file-name. */
    return ret + 1;
}

/*
 * @Description: Get Probin which is library file path.
 * @out libPath: Library file path.
 * @in sourceFileName: Source library path.
 * @in fun_name: Function name.
 * @in filename: File name.
 * @in final_file_name: Final file name.
 * @return: Finial library path which will be keep to system table.
 */
static char* getCFunProbin(const char* probin, Oid procNamespace, Oid proowner,
    const char* filename, char** final_file_name)
{
    if (strlen(filename) >= MAXPGPATH - 1) {
        ereport(ERROR, (errcode(ERRCODE_NAME_TOO_LONG), errmsg("The name of dynamic library is too long")));
    }

    StringInfoData strinfo;
    initStringInfo(&strinfo);

    appendStringInfo(&strinfo,
        "%s%c%u%u%u%ld" XID_FMT "%c%s",
        g_instance.attr.attr_common.PGXCNodeName,
        SEPARATOR,
        u_sess->proc_cxt.MyDatabaseId,
        proowner,
        procNamespace,
        GetCurrentTimestamp(),
        GetCurrentTransactionId(),
        SEPARATOR,
        filename);

    *final_file_name = pstrdup(strinfo.data);

    resetStringInfo(&strinfo);

    appendStringInfo(&strinfo, "$libdir/pg_plugin/%s", *final_file_name);

    if (strinfo.len >= MAXPGPATH - 1) {
        ereport(ERROR, (errcode(ERRCODE_NAME_TOO_LONG), errmsg("The name of dynamic library is too long")));
    }

    return strinfo.data;
}

/*
 * @Description: check new function conflicts old functions or not
 * @in procedureName - function name
 * @in allParameterTypes - Param types including out parameters
 * @in parameterTypes - Param types only in parameters
 * @in procNamespace - function's namespace oid
 * @in package - is a package function or not
 * @return - new function conflicts old functions or not
 */
static bool checkPackageFunctionConflicts(
    const char* procedureName, Datum allParameterTypes, oidvector* parameterTypes, Oid procNamespace, bool package)
{
    int inpara_count;
    int allpara_count = 0;
    ArrayType* arr = NULL;
    bool result = false;
    oidvector* allpara_type = NULL;
    oidvector* inpara_type = NULL;
    Oid* p_argtypes = NULL;
    HeapTuple proctup = NULL;
    errno_t rc = EOK;
    if (allParameterTypes != PointerGetDatum(NULL)) {
        arr = DatumGetArrayTypeP(allParameterTypes);
        allpara_count = ARR_DIMS(arr)[0];
        if (ARR_NDIM(arr) != 1 || allpara_count < 0 || ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != OIDOID) {
            ereport(ERROR, (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR), errmsg("proallargtypes is not a 1-D Oid array")));
        }

        p_argtypes = (Oid*)palloc(allpara_count * sizeof(Oid));
        rc = memcpy_s(p_argtypes, allpara_count * sizeof(Oid), ARR_DATA_PTR(arr), allpara_count * sizeof(Oid));
        securec_check(rc, "\0", "\0");
        allpara_type = buildoidvector(p_argtypes, allpara_count);
        pfree_ext(p_argtypes);
    }

    inpara_count = parameterTypes->dim1;
    inpara_type = parameterTypes;

    /* search the function */
    /* Search syscache by name only */
    CatCList* catlist = NULL;
    catlist = SearchSysCacheList1(PROCNAMEARGSNSP, CStringGetDatum(procedureName));

    for (int i = 0; i < catlist->n_members; i++) {
        proctup = &catlist->members[i]->tuple;
        Oid* argtypes = NULL;
        Datum proallargtypes;
        bool isNull = false;
        int allnumargs = 0;
        Form_pg_proc pform = NULL;
        oidvector* proc_allpara_type = NULL;
        oidvector* proc_para_type = NULL;
        bool result1 = false;
        bool result2 = false;
        bool result3 = false;
        if (HeapTupleIsValid(proctup)) {
            pform = (Form_pg_proc)GETSTRUCT(proctup);
            /* compare function's namespace */
            if (pform->pronamespace != procNamespace)
                continue;

            Datum propackage = SysCacheGetAttr(PROCOID, proctup, Anum_pg_proc_package, &isNull);
            bool ispackage = false;
            if (!isNull)
                ispackage = DatumGetBool(propackage);
            /* only check package function */
            if (ispackage != package) {
                ReleaseCatCacheList(catlist);
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Do not allow package function overload not package function.")));
            } else if (!package)
                break;

            /* First discover the total number of parameters and get their types */
            proallargtypes = SysCacheGetAttr(PROCOID, proctup, Anum_pg_proc_proallargtypes, &isNull);

            if (!isNull) {
                arr = DatumGetArrayTypeP(proallargtypes); /* ensure not toasted */
                allnumargs = ARR_DIMS(arr)[0];
                if (ARR_NDIM(arr) != 1 || allnumargs < 0 || ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != OIDOID) {
                    ReleaseCatCacheList(catlist);
                    ereport(ERROR,
                        (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR), errmsg("proallargtypes is not a 1-D Oid array")));
                }

                Assert(allnumargs >= pform->pronargs);
                argtypes = (Oid*)palloc(allnumargs * sizeof(Oid));
                rc = memcpy_s(argtypes, allnumargs * sizeof(Oid), ARR_DATA_PTR(arr), allnumargs * sizeof(Oid));
                securec_check(rc, "\0", "\0");
                proc_allpara_type = buildoidvector(argtypes, allnumargs);
                pfree_ext(argtypes);
            }

            /* No need to compare param type if  param count is not same */
            if (pform->pronargs != allpara_count && pform->pronargs != inpara_count && allnumargs != allpara_count &&
                allnumargs != inpara_count) {
                if (proc_allpara_type != NULL) {
                    pfree_ext(proc_allpara_type);
                }
                continue;
            }

            proc_para_type = &pform->proargtypes;

            /* old function in param type compare new function all param type */
            if (allpara_type != NULL) {
                result1 = DatumGetBool(
                    DirectFunctionCall2(oidvectoreq, PointerGetDatum(proc_para_type), PointerGetDatum(allpara_type)));
            }

            if (proc_allpara_type != NULL) {
                /* old function all param type compare new function in param type */
                result2 = DatumGetBool(
                    DirectFunctionCall2(oidvectoreq, PointerGetDatum(proc_allpara_type), PointerGetDatum(inpara_type)));

                if (allpara_type != NULL) {
                    /* old function all param type compare new function all param type */
                    result3 = DatumGetBool(DirectFunctionCall2(
                        oidvectoreq, PointerGetDatum(allpara_type), PointerGetDatum(proc_allpara_type)));
                }
            }

            result = result1 || result2 || result3;
            if (proc_allpara_type != NULL) {
                pfree_ext(proc_allpara_type);
            }

            if (!result)
                continue;
            else
                break;
        }
    }

    if (allpara_type != NULL) {
        pfree_ext(allpara_type);
    }

    ReleaseCatCacheList(catlist);
    return result;
}

/*
 * @Description: Check old and new function if conflicts.
 * @in oldproc: Old function proc.
 * @in procedureName: Procedure name.
 * @in proowner: Procedure owner.
 * @in returnType: New function return type.
 * @in allParameterTypes: Param types.
 * @in parameterModes: Param modes, mark is in or out parameter.
 * @in parameterNames: Parameter Names.
 * @in returnsSet: Return type if is set.
 * @in replace: Is replace.
 * @in isOraStyle: Is A db style.
 * @in isAgg: Is agg function.
 * @in isWindowFunc: Is windows function.
 */
static void checkFunctionConflicts(HeapTuple oldtup, const char* procedureName, Oid proowner, Oid returnType,
    Datum allParameterTypes, Datum parameterModes, Datum parameterNames, bool returnsSet, bool replace, bool isOraStyle,
    bool isAgg, bool isWindowFunc)
{
    Datum proargnames;
    bool isnull = false;

    if (!replace) {
        ereport(ERROR,
            (errcode(ERRCODE_DUPLICATE_FUNCTION),
                errmsg("function \"%s\" already exists with same argument types", procedureName)));
    }

    if (!pg_proc_ownercheck(HeapTupleGetOid(oldtup), proowner)) {
        aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_PROC, procedureName);
    }

    Form_pg_proc oldproc = (Form_pg_proc)GETSTRUCT(oldtup);

    /* if the function is a builtin function, its oid is less than 10000.
     * we can't allow replace the builtin functions
     */
    if (IsSystemObjOid(HeapTupleGetOid(oldtup)) && u_sess->attr.attr_common.IsInplaceUpgrade == false) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                errmsg("function \"%s\" is a builtin function,it can not be changed", procedureName)));
    }
    /* if the function is a masking function, we can't allow to replace it. */
    if (IsMaskingFunctionOid(HeapTupleGetOid(oldtup)) && !u_sess->attr.attr_common.IsInplaceUpgrade) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                errmsg("function \"%s\" is a masking function,it can not be changed", procedureName)));
    }
    /* A db donot check function return type when replace */
    if (!isOraStyle) {
        /*
         * Not okay to change the return type of the existing proc, since
         * existing rules, views, etc may depend on the return type.
         */
        if (returnType != oldproc->prorettype || returnsSet != oldproc->proretset) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                    errmsg("cannot change return type of existing function"),
                    errhint("Use DROP FUNCTION first.")));
        }

        /*
         * If it returns RECORD, check for possible change of record type
         * implied by OUT parameters
         */
        if (returnType == RECORDOID) {
            TupleDesc olddesc;
            TupleDesc newdesc;

            olddesc = build_function_result_tupdesc_t(oldtup);
            newdesc = build_function_result_tupdesc_d(allParameterTypes, parameterModes, parameterNames);
            if (olddesc == NULL && newdesc == NULL)
                /* ok, both are runtime-defined RECORDs */;
            else if (olddesc == NULL || newdesc == NULL || !equalTupleDescs(olddesc, newdesc)) {
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                        errmsg("cannot change return type of existing function"),
                        errdetail("Row type defined by OUT parameters is different."),
                        errhint("Use DROP FUNCTION first.")));
            }
        }

        /*
         * If there were any named input parameters, check to make sure the
         * names have not been changed, as this could break existing calls. We
         * allow adding names to formerly unnamed parameters, though.
         */
        proargnames = SysCacheGetAttr(PROCNAMEARGSNSP, oldtup, Anum_pg_proc_proargnames, &isnull);
        if (!isnull) {
            Datum proargmodes;
            char** old_arg_names;
            char** new_arg_names;
            int n_old_arg_names;
            int n_new_arg_names;
            int j;

            proargmodes = SysCacheGetAttr(PROCNAMEARGSNSP, oldtup, Anum_pg_proc_proargmodes, &isnull);
            if (isnull) {
                proargmodes = PointerGetDatum(NULL);
            }

            n_old_arg_names = get_func_input_arg_names(proargnames, proargmodes, &old_arg_names);
            n_new_arg_names = get_func_input_arg_names(parameterNames, parameterModes, &new_arg_names);
            for (j = 0; j < n_old_arg_names; j++) {
                if (old_arg_names[j] == NULL)
                    continue;

                if (j >= n_new_arg_names || new_arg_names[j] == NULL ||
                    strcmp(old_arg_names[j], new_arg_names[j]) != 0) {
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                            errmsg("cannot change name of input parameter \"%s\"", old_arg_names[j]),
                            errhint("Use DROP FUNCTION first.")));
                }
            }
        }
    }

    /* Can't change aggregate or window-function status, either */
    if (oldproc->proisagg != isAgg) {
        if (oldproc->proisagg) {
            ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("function \"%s\" is an aggregate function", procedureName)));
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("function \"%s\" is not an aggregate function", procedureName)));
        }
    }

    if (oldproc->proiswindow != isWindowFunc) {
        if (oldproc->proiswindow) {
            ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("function \"%s\" is a window function", procedureName)));
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("function \"%s\" is not a window function", procedureName)));
        }
    }
}

/*
 * @Description: c function file's owner must be super user.
 * @in probin: Library path.
 */
static void cfunction_check_user(const char* probin)
{
    struct stat statbuf;
    if (lstat(probin, &statbuf) != 0) {
        ereport(ERROR, (errcode_for_file_access(),
            errmsg("could not stat file \"%s\": %m", probin),
            errdetail("the dynamic library for C function should be placed in $libdir/pg_plugin")));
    }

    /* Get the user name of C function .so file. */
    passwd* filepw = getpwuid(statbuf.st_uid);
    if (filepw == NULL || filepw->pw_name == NULL) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("can not get current user name for the C function file.")));
    }

    /* Get the user name of current process. */
    passwd* syspw = getpwuid(getuid());
    if (syspw == NULL || syspw->pw_name == NULL) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("can not get user name for current process.")));
    }

    if (strncmp(syspw->pw_name, filepw->pw_name, NAMEDATALEN) != 0) {
        ereport(
            ERROR, (errcode_for_file_access(), errmsg("the owner of file \"%s\" must be %s.", probin, syspw->pw_name)));
    }
}

/*
 * Detect if a given path contain a substring "../".
 */
inline void IsLeavingDefaultPath(const char* path)
{
    if (strstr(path, "../") != NULL) {
        ereport(ERROR, 
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Path \"%s\" is not admitted. Don't use \"../\" "
                "when enable_default_cfunc_libpath is on.", path)));
    }
}

/*
 * @Description: Check user-define function valid.
 * @in languageObjectId: Language oid.
 * @in probin: Library path.
 * @out absolutePath: Library absolute path.
 * @out function_type: If is dump fun.
 * @return: Return true if be user-defined c function.
 */
static bool user_define_func_check(Oid languageId, const char* probin, char** absolutePath, CFunType* function_type)
{
    char* library_path = NULL;
    char* new_path = (char *)probin;
    bool user_define_fun = false;
    CFunType fun_type = NormalType;
    bool check_user = false;

    if (languageId == ClanguageId) {
        if (strncmp(probin, PORC_PLUGIN_LIB_PATH, strlen(PORC_PLUGIN_LIB_PATH)) == 0) {
            /* Probin can be start with "$libdir/pg_plugin/" when upgrading. */
            user_define_fun = true;
            fun_type = DumpType;
        } else if (strncmp(probin, PORC_SRC_LIB_PATH, strlen(PORC_SRC_LIB_PATH)) == 0) {
            user_define_fun = true;
            check_user = true;
        } else if (strncmp(probin, PROC_LIB_PATH, strlen(PROC_LIB_PATH)) == 0) {
            /* During the initdb and upgrade, we need to use .so file in $libdir to create extension. */
            if (superuser()) {
                check_user = true;
            }
        } else {
            if (g_instance.attr.attr_sql.enable_default_cfunc_libpath) {
                /* Check if probin is a legal path. We need to detect "../" in probin since the existence of "../"
                 * may cause the target path leaves the default path. */
                IsLeavingDefaultPath(probin);
                /* Add plugin path. */
                int len = strlen(PORC_SRC_LIB_PATH) + strlen(probin) + 1;
                new_path = (char*)palloc0(len);
                errno_t rc = strcpy_s(new_path, len, PORC_SRC_LIB_PATH);
                securec_check(rc, "\0", "\0");
                rc = strcat_s(new_path, len, probin);
                securec_check(rc, "\0", "\0");
                user_define_fun = true;
                check_user = true;
            } else {
                /* C language function defined by users if probin is absolute path. */
                if (is_absolute_path(probin)) {
                    user_define_fun = true;
                }
                check_user = true;
            }
        }

        if (user_define_fun) {
            library_path = expand_dynamic_library_name(new_path);

            if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                /* Check path which input by user input valid.*/
                check_library_path(library_path, fun_type);

                if (check_user == true) {
                    cfunction_check_user(library_path);
                }
            }
        }

        *absolutePath = library_path;
        *function_type = fun_type;
    }

    return user_define_fun;
}

/* ----------------------------------------------------------------
 *		ProcedureCreate
 *
 * Note: allParameterTypes, parameterModes, parameterNames, and proconfig
 * are either arrays of the proper types or NULL.  We declare them Datum,
 * not "ArrayType *", to avoid importing array.h into pg_proc_fn.h.
 * ----------------------------------------------------------------
 */
Oid ProcedureCreate(const char* procedureName, Oid procNamespace, bool isOraStyle, bool replace, bool returnsSet,
    Oid returnType, Oid proowner, Oid languageObjectId, Oid languageValidator, const char* prosrc, const char* probin,
    bool isAgg, bool isWindowFunc, bool security_definer, bool isLeakProof, bool isStrict, char volatility,
    oidvector* parameterTypes, Datum allParameterTypes, Datum parameterModes, Datum parameterNames,
    List* parameterDefaults, Datum proconfig, float4 procost, float4 prorows, int2vector* prodefaultargpos, bool fenced,
    bool shippable, bool package, bool proIsProcedure)
{
    Oid retval;
    int parameterCount;
    int allParamCount;
    Oid* allParams = NULL;
    char* paramModes = NULL;
    bool genericInParam = false;
    bool genericOutParam = false;
    bool anyrangeInParam = false;
    bool anyrangeOutParam = false;
    bool internalInParam = false;
    bool internalOutParam = false;
    Oid variadicType = InvalidOid;
    Acl* proacl = NULL;
    Relation rel;
    HeapTuple tup;
    HeapTuple oldtup;
    bool nulls[Natts_pg_proc];
    Datum values[Natts_pg_proc];
    bool replaces[Natts_pg_proc];
    Oid relid;
    NameData procname;
    TupleDesc tupDesc;
    bool is_update = false;
    ObjectAddress myself, referenced;
    int i;
    bool user_defined_c_fun = false;
    CFunType function_type = NormalType;
    char* absolutePath = NULL;
    const char* filename = NULL;
    char* libPath = NULL;
    char* final_file_name = NULL;

    /* sanity checks */
    Assert(PointerIsValid(prosrc));

    parameterCount = parameterTypes->dim1;
    if (parameterCount < 0 || parameterCount > FUNC_MAX_ARGS)
        ereport(ERROR,
            (errcode(ERRCODE_TOO_MANY_ARGUMENTS),
                errmsg_plural("functions cannot have more than %d argument",
                    "functions cannot have more than %d arguments",
                    FUNC_MAX_ARGS,
                    FUNC_MAX_ARGS)));
    /* note: the above is correct, we do NOT count output arguments */
    /* Deconstruct array inputs */
    if (allParameterTypes != PointerGetDatum(NULL)) {
        /*
         * We expect the array to be a 1-D OID array; verify that. We don't
         * need to use deconstruct_array() since the array data is just going
         * to look like a C array of OID values.
         */
        ArrayType* allParamArray = (ArrayType*)DatumGetPointer(allParameterTypes);

        allParamCount = ARR_DIMS(allParamArray)[0];
        if (ARR_NDIM(allParamArray) != 1 || allParamCount <= 0 || ARR_HASNULL(allParamArray) ||
            ARR_ELEMTYPE(allParamArray) != OIDOID)
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("allParameterTypes is not a 1-D Oid array")));
        allParams = (Oid*)ARR_DATA_PTR(allParamArray);
        Assert(allParamCount >= parameterCount);
        /* we assume caller got the contents right */
    } else {
        allParamCount = parameterCount;
        allParams = parameterTypes->values;
    }

    if (parameterModes != PointerGetDatum(NULL)) {
        /*
         * We expect the array to be a 1-D CHAR array; verify that. We don't
         * need to use deconstruct_array() since the array data is just going
         * to look like a C array of char values.
         */
        ArrayType* modesArray = (ArrayType*)DatumGetPointer(parameterModes);

        if (ARR_NDIM(modesArray) != 1 || ARR_DIMS(modesArray)[0] != allParamCount || ARR_HASNULL(modesArray) ||
            ARR_ELEMTYPE(modesArray) != CHAROID)
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("parameterModes is not a 1-D char array")));
        paramModes = (char*)ARR_DATA_PTR(modesArray);
    }

    /*
     * Detect whether we have polymorphic or INTERNAL arguments.  The first
     * loop checks input arguments, the second output arguments.
     */
    for (i = 0; i < parameterCount; i++) {
        switch (parameterTypes->values[i]) {
            case ANYARRAYOID:
            case ANYELEMENTOID:
            case ANYNONARRAYOID:
            case ANYENUMOID:
                genericInParam = true;
                break;
            case ANYRANGEOID:
                genericInParam = true;
                anyrangeInParam = true;
                break;
            case INTERNALOID:
                internalInParam = true;
                break;
            default:
                break;
        }
    }

    if (allParameterTypes != PointerGetDatum(NULL)) {
        for (i = 0; i < allParamCount; i++) {
            if (paramModes == NULL || paramModes[i] == PROARGMODE_IN || paramModes[i] == PROARGMODE_VARIADIC)
                continue; /* ignore input-only params */

            switch (allParams[i]) {
                case ANYARRAYOID:
                case ANYELEMENTOID:
                case ANYNONARRAYOID:
                case ANYENUMOID:
                    genericOutParam = true;
                    break;
                case ANYRANGEOID:
                    genericOutParam = true;
                    anyrangeOutParam = true;
                    break;
                case INTERNALOID:
                    internalOutParam = true;
                    break;
                default:
                    break;
            }
        }
    }

    /*
     * Do not allow polymorphic return type unless at least one input argument
     * is polymorphic.	ANYRANGE return type is even stricter: must have an
     * ANYRANGE input (since we can't deduce the specific range type from
     * ANYELEMENT).  Also, do not allow return type INTERNAL unless at least
     * one input argument is INTERNAL.
     *
     * But when we are in inplace-upgrade, we can create function with polymorphic return type
     */
    if ((IsPolymorphicType(returnType) || genericOutParam) && !u_sess->attr.attr_common.IsInplaceUpgrade &&
        !genericInParam)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                errmsg("cannot determine result data type"),
                errdetail("A function returning a polymorphic type must have at least one polymorphic argument.")));

    if ((returnType == ANYRANGEOID || anyrangeOutParam) && !anyrangeInParam)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                errmsg("cannot determine result data type"),
                errdetail("A function returning ANYRANGE must have at least one ANYRANGE argument.")));

    if ((returnType == INTERNALOID || internalOutParam) && !internalInParam)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                errmsg("unsafe use of pseudo-type \"internal\""),
                errdetail("A function returning \"internal\" must have at least one \"internal\" argument.")));

    /*
     * don't allow functions of complex types that have the same name as
     * existing attributes of the type
     */
    if (parameterCount == 1 && OidIsValid(parameterTypes->values[0]) &&
        (relid = typeidTypeRelid(parameterTypes->values[0])) != InvalidOid &&
        get_attnum(relid, procedureName) != InvalidAttrNumber)
        ereport(ERROR,
            (errcode(ERRCODE_DUPLICATE_COLUMN),
                errmsg("\"%s\" is already an attribute of type %s",
                    procedureName,
                    format_type_be(parameterTypes->values[0]))));

    if (paramModes != NULL) {
        /*
         * Only the last input parameter can be variadic; if it is, save its
         * element type.  Errors here are just elog since caller should have
         * checked this already.
         */
        for (i = 0; i < allParamCount; i++) {
            switch (paramModes[i]) {
                case PROARGMODE_IN:
                case PROARGMODE_INOUT:
                    if (OidIsValid(variadicType))
                        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("variadic parameter must be last")));
                    break;
                case PROARGMODE_OUT:
                    /* okay */
                    break;
                case PROARGMODE_TABLE:
                    if (package) {
                        ereport(ERROR,
                            (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                                errmsg("package function does not support table parameter.")));
                    }
                    break;
                case PROARGMODE_VARIADIC: {
                    if (package) {
                        ereport(ERROR,
                            (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                                errmsg("package function does not support variadic parameter.")));
                    }
                    if (OidIsValid(variadicType))
                        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("variadic parameter must be last")));
                    switch (allParams[i]) {
                        case ANYOID:
                            variadicType = ANYOID;
                            break;
                        case ANYARRAYOID:
                            variadicType = ANYELEMENTOID;
                            break;
                        default:
                            variadicType = get_element_type(allParams[i]);
                            if (!OidIsValid(variadicType))
                                ereport(ERROR,
                                    (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("variadic parameter is not an array")));
                            break;
                    }
                    break;
                }
                default:
                    ereport(ERROR,
                        (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmsg("invalid parameter mode '%c'", paramModes[i])));
                    break;
            }
        }
    }

    /*
     * All seems OK; prepare the data to be inserted into pg_proc.
     */
    for (i = 0; i < Natts_pg_proc; ++i) {
        nulls[i] = false;
        values[i] = (Datum)0;
        replaces[i] = true;
    }

    (void)namestrcpy(&procname, procedureName);
    values[Anum_pg_proc_proname - 1] = NameGetDatum(&procname);
    values[Anum_pg_proc_pronamespace - 1] = ObjectIdGetDatum(procNamespace);
    values[Anum_pg_proc_proowner - 1] = ObjectIdGetDatum(proowner);
    values[Anum_pg_proc_prolang - 1] = ObjectIdGetDatum(languageObjectId);
    values[Anum_pg_proc_procost - 1] = Float4GetDatum(procost);
    values[Anum_pg_proc_prorows - 1] = Float4GetDatum(prorows);
    values[Anum_pg_proc_provariadic - 1] = ObjectIdGetDatum(variadicType);
    values[Anum_pg_proc_protransform - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_proc_proisagg - 1] = BoolGetDatum(isAgg);
    values[Anum_pg_proc_proiswindow - 1] = BoolGetDatum(isWindowFunc);
    values[Anum_pg_proc_prosecdef - 1] = BoolGetDatum(security_definer);
    values[Anum_pg_proc_proleakproof - 1] = BoolGetDatum(isLeakProof);
    values[Anum_pg_proc_proisstrict - 1] = BoolGetDatum(isStrict);
    values[Anum_pg_proc_proretset - 1] = BoolGetDatum(returnsSet);
    values[Anum_pg_proc_provolatile - 1] = CharGetDatum(volatility);
    values[Anum_pg_proc_pronargs - 1] = UInt16GetDatum(parameterCount);
    values[Anum_pg_proc_pronargdefaults - 1] = UInt16GetDatum(list_length(parameterDefaults));
    values[Anum_pg_proc_prorettype - 1] = ObjectIdGetDatum(returnType);
    values[Anum_pg_proc_proargtypes - 1] = PointerGetDatum(parameterTypes);
    if (allParameterTypes != PointerGetDatum(NULL))
        values[Anum_pg_proc_proallargtypes - 1] = allParameterTypes;
    else
        nulls[Anum_pg_proc_proallargtypes - 1] = true;
    if (parameterModes != PointerGetDatum(NULL))
        values[Anum_pg_proc_proargmodes - 1] = parameterModes;
    else
        nulls[Anum_pg_proc_proargmodes - 1] = true;
    if (parameterNames != PointerGetDatum(NULL))
        values[Anum_pg_proc_proargnames - 1] = parameterNames;
    else
        nulls[Anum_pg_proc_proargnames - 1] = true;
    if (parameterDefaults != NIL) {
        values[Anum_pg_proc_proargdefaults - 1] = CStringGetTextDatum(nodeToString(parameterDefaults));
        values[Anum_pg_proc_prodefaultargpos - 1] = PointerGetDatum(prodefaultargpos);
    } else {
        nulls[Anum_pg_proc_proargdefaults - 1] = true;
        nulls[Anum_pg_proc_prodefaultargpos - 1] = true;
    }

    values[Anum_pg_proc_prosrc - 1] = CStringGetTextDatum(prosrc);
    values[Anum_pg_proc_fenced - 1] = BoolGetDatum(fenced);
    values[Anum_pg_proc_shippable - 1] = BoolGetDatum(shippable);
    values[Anum_pg_proc_package - 1] = BoolGetDatum(package);
    values[Anum_pg_proc_prokind - 1] = CharGetDatum(proIsProcedure ? PROKIND_PROCEDURE : PROKIND_FUNCTION);

    if (probin != NULL) {
        /* Check user defined function. */
        user_defined_c_fun = user_define_func_check(languageObjectId, probin, &absolutePath, &function_type);
        if (user_defined_c_fun) {
            filename = get_file_name(absolutePath, function_type);
            check_backend_env(pstrdup(filename));
            /* Check dynamic lib file first */
            check_external_function(absolutePath, filename, prosrc);
            libPath = getCFunProbin(probin, procNamespace, proowner, filename, &final_file_name);

            values[Anum_pg_proc_probin - 1] = CStringGetTextDatum(libPath);
        } else {
            values[Anum_pg_proc_probin - 1] = CStringGetTextDatum(probin);
        }
    } else {
        nulls[Anum_pg_proc_probin - 1] = true;
    }

    if (proconfig != PointerGetDatum(NULL))
        values[Anum_pg_proc_proconfig - 1] = proconfig;
    else
        nulls[Anum_pg_proc_proconfig - 1] = true;
    /* proacl will be determined later */
    rel = heap_open(ProcedureRelationId, RowExclusiveLock);
    tupDesc = RelationGetDescr(rel);

    /* A db do not overload a function by arguments.*/
    if (isOraStyle && !package) {
        List* name = list_make2(makeString(get_namespace_name(procNamespace)), makeString(pstrdup(procedureName)));

        FuncCandidateList listfunc = FuncnameGetCandidates(name, -1, NULL, false, false, true);
        if (listfunc) {
            if (listfunc->next)
                ereport(ERROR,
                    (errcode(ERRCODE_DUPLICATE_FUNCTION),
                        errmsg("more than one function \"%s\" already exist, "
                               "please drop function first",
                            procedureName)));

            oldtup = SearchSysCache1(PROCOID, ObjectIdGetDatum(listfunc->oid));
        } else {
            oldtup = NULL;
        }
    } else {
        /* Check for pre-existing definition */
        oldtup = SearchSysCache3(PROCNAMEARGSNSP,
            PointerGetDatum(procedureName),
            PointerGetDatum(parameterTypes),
            ObjectIdGetDatum(procNamespace));
    }

    if (HeapTupleIsValid(oldtup)) {
        /* There is one; okay to replace it? */
        Form_pg_proc oldproc = (Form_pg_proc)GETSTRUCT(oldtup);

        checkFunctionConflicts(oldtup,
            procedureName,
            proowner,
            returnType,
            allParameterTypes,
            parameterModes,
            parameterNames,
            returnsSet,
            replace,
            isOraStyle,
            isAgg,
            isWindowFunc);

        bool isNull = false;
        Datum ispackage = SysCacheGetAttr(PROCOID, oldtup, Anum_pg_proc_package, &isNull);
        if (!isNull && DatumGetBool(ispackage) != package) {
            ReleaseSysCache(oldtup);
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Do not allow package function replace not package function.")));
        }

        /* For replace use-define C function, Here we need delete it's library when commit. */
        if (oldproc->prolang == ClanguageId) {
            if (PrepareCFunctionLibrary(oldtup)) {
                Oid functionId = HeapTupleGetOid(oldtup);
                /*
                 * User-define c function need close library handle and remove library file,
                 * so user can not use this function when removing. Here need add ExclusiveLock.
                 */
                LockDatabaseObject(ProcedureRelationId, functionId, 0, AccessExclusiveLock);
            }
        }

        /*
         * Do not change existing ownership or permissions, either.  Note
         * dependency-update code below has to agree with this decision.
         */
        replaces[Anum_pg_proc_proowner - 1] = false;
        replaces[Anum_pg_proc_proacl - 1] = false;

        /* Okay, do it... */
        tup = heap_modify_tuple(oldtup, tupDesc, values, nulls, replaces);
        simple_heap_update(rel, &tup->t_self, tup);

        ReleaseSysCache(oldtup);
        is_update = true;
    } else {
        /* checking for package function */
        bool conflicts =
            checkPackageFunctionConflicts(procedureName, allParameterTypes, parameterTypes, procNamespace, package);
        if (conflicts) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Package function does not support function overload which has the same type argument.")));
        }
        /* Creating a new procedure */
        /* First, get default permissions and set up proacl */
        proacl = get_user_default_acl(ACL_OBJECT_FUNCTION, proowner, procNamespace);
        if (proacl != NULL)
            values[Anum_pg_proc_proacl - 1] = PointerGetDatum(proacl);
        else
            nulls[Anum_pg_proc_proacl - 1] = true;

        tup = heap_form_tuple(tupDesc, values, nulls);

        /* Use inplace-upgrade override for pg_proc.oid, if supplied. */
        if (u_sess->attr.attr_common.IsInplaceUpgrade && OidIsValid(u_sess->upg_cxt.Inplace_upgrade_next_pg_proc_oid)) {
            HeapTupleSetOid(tup, u_sess->upg_cxt.Inplace_upgrade_next_pg_proc_oid);
            u_sess->upg_cxt.Inplace_upgrade_next_pg_proc_oid = InvalidOid;
        }

        (void)simple_heap_insert(rel, tup);
        is_update = false;
    }

    /* Need to update indexes for either the insert or update case */
    CatalogUpdateIndexes(rel, tup);

    retval = HeapTupleGetOid(tup);

    /*
     * Create dependencies for the new function.  If we are updating an
     * existing function, first delete any existing pg_depend entries.
     * (However, since we are not changing ownership or permissions, the
     * shared dependencies do *not* need to change, and we leave them alone.)
     */
    if (is_update) {
        (void)deleteDependencyRecordsFor(ProcedureRelationId, retval, true);

        /* the 'shared dependencies' also change when update. */
        deleteSharedDependencyRecordsFor(ProcedureRelationId, retval, 0);

        /* send invalid message for for relation holding replaced function as trigger */
        InvalidRelcacheForTriggerFunction(retval, ((Form_pg_proc)GETSTRUCT(tup))->prorettype);
    }

    myself.classId = ProcedureRelationId;
    myself.objectId = retval;
    myself.objectSubId = 0;

    if (u_sess->attr.attr_common.IsInplaceUpgrade && myself.objectId < FirstBootstrapObjectId && !is_update)
        recordPinnedDependency(&myself);
    else {
        /* dependency on namespace */
        referenced.classId = NamespaceRelationId;
        referenced.objectId = procNamespace;
        referenced.objectSubId = 0;
        recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

        /* dependency on implementation language */
        referenced.classId = LanguageRelationId;
        referenced.objectId = languageObjectId;
        referenced.objectSubId = 0;
        recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

        /* dependency on return type */
        referenced.classId = TypeRelationId;
        referenced.objectId = returnType;
        referenced.objectSubId = 0;
        recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

        /* dependency on parameter types */
        for (i = 0; i < allParamCount; i++) {
            referenced.classId = TypeRelationId;
            referenced.objectId = allParams[i];
            referenced.objectSubId = 0;
            recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
        }

        /* dependency on parameter default expressions */
        if (parameterDefaults != NULL)
            recordDependencyOnExpr(&myself, (Node*)parameterDefaults, NIL, DEPENDENCY_NORMAL);

        /*
         * dependency on owner
         * the 'shared dependencies' also change when update
         */
        recordDependencyOnOwner(ProcedureRelationId, retval, proowner, libPath);

        /* dependency on any roles mentioned in ACL */
        if (!is_update && proacl != NULL) {
            int nnewmembers;
            Oid* newmembers = NULL;

            nnewmembers = aclmembers(proacl, &newmembers);
            updateAclDependencies(ProcedureRelationId, retval, 0, proowner, 0, NULL, nnewmembers, newmembers);
        }

        /* dependency on extension */
        recordDependencyOnCurrentExtension(&myself, is_update);
    }

    heap_freetuple_ext(tup);

    /* Post creation hook for new function */
    InvokeObjectAccessHook(OAT_POST_CREATE, ProcedureRelationId, retval, 0, NULL);

    /* Recode the procedure create time. */
    if (OidIsValid(retval)) {
        if (!is_update) {
            CreatePgObject(retval, OBJECT_TYPE_PROC, proowner, true, true);
        } else {
            UpdatePgObjectMtime(retval, OBJECT_TYPE_PROC);
        }
    }

    heap_close(rel, RowExclusiveLock);

    /*
     * To user-defined C_function, need rename library filename to special name,
     * Because exist concurrent to the same library, so need lock.
     */
    AutoMutexLock libraryLock(&dlerror_lock);

    if (user_defined_c_fun) {
        libraryLock.lock();

        if ((IS_PGXC_COORDINATOR && !IsConnFromCoord()) || IS_SINGLE_NODE) {
            /* Send library file to all node's $libdir/pg_plugin/ catalogue. */
            send_library_other_node(absolutePath);
        }

        /*
         * Copy file, we need rename this file, we will add nodename, dbOid, userOid and
         * namespaceOid before filename.
         */
        copyLibraryToSpecialName(absolutePath, final_file_name, libPath, function_type);
    }

    /* Verify function body */
    if (OidIsValid(languageValidator)) {
        ArrayType* set_items = NULL;
        int save_nestlevel;

        /* Advance command counter so new tuple can be seen by validator */
        CommandCounterIncrement();

        /* Set per-function configuration parameters */
        set_items = (ArrayType*)DatumGetPointer(proconfig);
        if (set_items != NULL) { /* Need a new GUC nesting level */
            save_nestlevel = NewGUCNestLevel();
            ProcessGUCArray(set_items, (superuser() ? PGC_SUSET : PGC_USERSET), PGC_S_SESSION, GUC_ACTION_SAVE);
        } else
            save_nestlevel = 0; /* keep compiler quiet */

        OidFunctionCall1(languageValidator, ObjectIdGetDatum(retval));

        if (set_items != NULL)
            AtEOXact_GUC(true, save_nestlevel);
    }
    libraryLock.unLock();

    pfree_ext(final_file_name);
    return retval;
}

/*
 * Validator for internal functions
 *
 * Check that the given internal function name (the "prosrc" value) is
 * a known builtin function.
 */
Datum fmgr_internal_validator(PG_FUNCTION_ARGS)
{
    Oid funcoid = PG_GETARG_OID(0);
    HeapTuple tuple = NULL;
    bool isnull = false;
    Datum tmp;
    char* prosrc = NULL;

    if (!CheckFunctionValidatorAccess(fcinfo->flinfo->fn_oid, funcoid))
        PG_RETURN_VOID();

    /*
     * We do not honor check_function_bodies since it's unlikely the function
     * name will be found later if it isn't there now.
     */
    tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcoid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", funcoid)));

    tmp = SysCacheGetAttr(PROCOID, tuple, Anum_pg_proc_prosrc, &isnull);
    if (isnull)
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("null prosrc")));

    prosrc = TextDatumGetCString(tmp);
    /*
     * During inplace upgrade, built-in functions to be introduced are still
     * absent in the old fmgrtab.c.
     */
    if (fmgr_internal_function(prosrc) == InvalidOid && !u_sess->attr.attr_common.IsInplaceUpgrade)
        ereport(
            ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION), errmsg("there is no built-in function named \"%s\"", prosrc)));

    ReleaseSysCache(tuple);

    PG_RETURN_VOID();
}

/*
 * Validator for C language functions
 *
 * Make sure that the library file exists, is loadable, and contains
 * the specified link symbol. Also check for a valid function
 * information record.
 */
Datum fmgr_c_validator(PG_FUNCTION_ARGS)
{
    Oid funcoid = PG_GETARG_OID(0);
    HeapTuple tuple = NULL;
    bool isnull = false;
    Datum tmp;
    char* prosrc = NULL;
    char* probin = NULL;

    if (!CheckFunctionValidatorAccess(fcinfo->flinfo->fn_oid, funcoid))
        PG_RETURN_VOID();

    /*
     * It'd be most consistent to skip the check if !check_function_bodies,
     * but the purpose of that switch is to be helpful for pg_dump loading,
     * and for pg_dump loading it's much better if we *do* check.
     */
    tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcoid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", funcoid)));

    tmp = SysCacheGetAttr(PROCOID, tuple, Anum_pg_proc_prosrc, &isnull);
    if (isnull)
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("null prosrc for C function %u", funcoid)));
    prosrc = TextDatumGetCString(tmp);

    tmp = SysCacheGetAttr(PROCOID, tuple, Anum_pg_proc_probin, &isnull);
    if (isnull)
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("null probin for C function %u", funcoid)));
    probin = TextDatumGetCString(tmp);
    if (strcmp(probin, "$libdir/plpgsql") && strcmp(probin, "$libdir/dist_fdw") && strcmp(probin, "$libdir/file_fdw") &&
#ifdef ENABLE_MOT
        strcmp(probin, "$libdir/mot_fdw") &&
#endif
        strcmp(probin, "$libdir/log_fdw") && strcmp(probin, "$libdir/hdfs_fdw") &&
        strcmp(probin, "$libdir/postgres_fdw")) {
        (void)load_external_function(probin, prosrc, true, true);
    }

    ReleaseSysCache(tuple);

    PG_RETURN_VOID();
}

/*
 * Validator for SQL language functions
 *
 * Parse it here in order to be sure that it contains no syntax errors.
 */
Datum fmgr_sql_validator(PG_FUNCTION_ARGS)
{
    Oid funcoid = PG_GETARG_OID(0);
    HeapTuple tuple = NULL;
    Form_pg_proc proc;
    List* raw_parsetree_list = NIL;
    List* querytree_list = NIL;
    ListCell* lc = NULL;
    bool isnull = false;
    Datum tmp;
    char* prosrc = NULL;
    parse_error_callback_arg callback_arg;
    ErrorContextCallback sqlerrcontext;
    bool haspolyarg = false;
    int i;

    if (!CheckFunctionValidatorAccess(fcinfo->flinfo->fn_oid, funcoid))
        PG_RETURN_VOID();

    tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcoid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", funcoid)));
    proc = (Form_pg_proc)GETSTRUCT(tuple);
    /* Disallow pseudotype result */
    /* except for RECORD, VOID, or polymorphic */
    if (get_typtype(proc->prorettype) == TYPTYPE_PSEUDO && proc->prorettype != RECORDOID &&
        proc->prorettype != VOIDOID && !IsPolymorphicType(proc->prorettype))
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                errmsg("SQL functions cannot return type %s", format_type_be(proc->prorettype))));

    /* Disallow pseudotypes in arguments */
    /* except for polymorphic */
    haspolyarg = false;
    for (i = 0; i < proc->pronargs; i++) {
        if (get_typtype(proc->proargtypes.values[i]) == TYPTYPE_PSEUDO) {
            if (IsPolymorphicType(proc->proargtypes.values[i]))
                haspolyarg = true;
            else
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                        errmsg("SQL functions cannot have arguments of type %s",
                            format_type_be(proc->proargtypes.values[i]))));
        }
    }

    /* Postpone body checks if !u_sess->attr.attr_sql.check_function_bodies */
    if (u_sess->attr.attr_sql.check_function_bodies) {
        tmp = SysCacheGetAttr(PROCOID, tuple, Anum_pg_proc_prosrc, &isnull);
        if (isnull)
            ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("null prosrc")));

        prosrc = TextDatumGetCString(tmp);

        /*
         * Setup error traceback support for ereport().
         */
        callback_arg.proname = NameStr(proc->proname);
        callback_arg.prosrc = prosrc;

        sqlerrcontext.callback = sql_function_parse_error_callback;
        sqlerrcontext.arg = (void*)&callback_arg;
        sqlerrcontext.previous = t_thrd.log_cxt.error_context_stack;
        t_thrd.log_cxt.error_context_stack = &sqlerrcontext;

        /*
         * We can't do full prechecking of the function definition if there
         * are any polymorphic input types, because actual datatypes of
         * expression results will be unresolvable.  The check will be done at
         * runtime instead.
         *
         * We can run the text through the raw parser though; this will at
         * least catch silly syntactic errors.
         */
        raw_parsetree_list = pg_parse_query(prosrc);

        if (!haspolyarg) {
            /*
             * OK to do full precheck: analyze and rewrite the queries, then
             * verify the result type.
             */
            SQLFunctionParseInfoPtr pinfo;

            /* But first, set up parameter information */
            pinfo = prepare_sql_fn_parse_info(tuple, NULL, InvalidOid);

            querytree_list = NIL;
            foreach (lc, raw_parsetree_list) {
                Node* parsetree = (Node*)lfirst(lc);
                List* querytree_sublist = NIL;

#ifdef PGXC
                /* Block CTAS in SQL functions */
                if (IsA(parsetree, CreateTableAsStmt))
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("In XC, SQL functions cannot contain utility statements")));
#endif
                u_sess->catalog_cxt.Parse_sql_language = true;
                querytree_sublist =
                    pg_analyze_and_rewrite_params(parsetree, prosrc, (ParserSetupHook)sql_fn_parser_setup, pinfo);
                u_sess->catalog_cxt.Parse_sql_language = false;

#ifdef PGXC
                /* Check if the list of queries contains temporary objects */
                if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                    if (pgxc_query_contains_utility(querytree_sublist))
                        ereport(ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("In XC, SQL functions cannot contain utility statements")));

                    if (pgxc_query_contains_view(querytree_sublist))
                        ereport(
                            ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("In XC, SQL functions cannot contain view")));

                    if (pgxc_query_contains_temp_tables(querytree_sublist))
                        ExecSetTempObjectIncluded();
                }
#endif

                querytree_list = list_concat(querytree_list, querytree_sublist);
            }

            (void)check_sql_fn_retval(funcoid, proc->prorettype, querytree_list, NULL, NULL);
        }

        t_thrd.log_cxt.error_context_stack = sqlerrcontext.previous;
    }

    ReleaseSysCache(tuple);

    PG_RETURN_VOID();
}

/*
 * Error context callback for handling errors in SQL function definitions
 */
static void sql_function_parse_error_callback(void* arg)
{
    parse_error_callback_arg* callback_arg = (parse_error_callback_arg*)arg;

    /* See if it's a syntax error; if so, transpose to CREATE FUNCTION */
    if (!function_parse_error_transpose(callback_arg->prosrc)) {
        /* If it's not a syntax error, push info onto context stack */
        errcontext("SQL function \"%s\"", callback_arg->proname);
    }
}

/*
 * Adjust a syntax error occurring inside the function body of a CREATE
 * FUNCTION or DO command.	This can be used by any function validator or
 * anonymous-block handler, not only for SQL-language functions.
 * It is assumed that the syntax error position is initially relative to the
 * function body string (as passed in).  If possible, we adjust the position
 * to reference the original command text; if we can't manage that, we set
 * up an "internal query" syntax error instead.
 *
 * Returns true if a syntax error was processed, false if not.
 */
bool function_parse_error_transpose(const char* prosrc)
{
    int origerrposition;
    int newerrposition;
    const char* queryText = NULL;

    /*
     * Nothing to do unless we are dealing with a syntax error that has a
     * cursor position.
     *
     * Some PLs may prefer to report the error position as an internal error
     * to begin with, so check that too.
     */
    origerrposition = geterrposition();
    if (origerrposition <= 0) {
        origerrposition = getinternalerrposition();
        if (origerrposition <= 0) {
            return false;
        }
    }

    /* We can get the original query text from the active portal (hack...) */
    Assert(ActivePortal && ActivePortal->status == PORTAL_ACTIVE);
    queryText = ActivePortal->sourceText;

    /* Try to locate the prosrc in the original text */
    newerrposition = match_prosrc_to_query(prosrc, queryText, origerrposition);
    if (newerrposition > 0) {
        /* Successful, so fix error position to reference original query */
        errposition(newerrposition);
        /* Get rid of any report of the error as an "internal query" */
        internalerrposition(0);
        internalerrquery(NULL);
    } else {
        /*
         * If unsuccessful, convert the position to an internal position
         * marker and give the function text as the internal query.
         */
        errposition(0);
        internalerrposition(origerrposition);
        internalerrquery(prosrc);
    }

    return true;
}

/*
 * Try to locate the string literal containing the function body in the
 * given text of the CREATE FUNCTION or DO command.  If successful, return
 * the character (not byte) index within the command corresponding to the
 * given character index within the literal.  If not successful, return 0.
 */
static int match_prosrc_to_query(const char* prosrc, const char* queryText, int cursorpos)
{
    /*
     * Rather than fully parsing the original command, we just scan the
     * command looking for $prosrc$ or 'prosrc'.  This could be fooled (though
     * not in any very probable scenarios), so fail if we find more than one
     * match.
     */
    int prosrclen = strlen(prosrc);
    int querylen = strlen(queryText);
    int matchpos = 0;
    int curpos;
    int newcursorpos;

    for (curpos = 0; curpos < querylen - prosrclen; curpos++) {
        if (queryText[curpos] == '$' && strncmp(prosrc, &queryText[curpos + 1], prosrclen) == 0 &&
            queryText[curpos + 1 + prosrclen] == '$') {
            /*
             * Found a $foo$ match.  Since there are no embedded quoting
             * characters in a dollar-quoted literal, we don't have to do any
             * fancy arithmetic; just offset by the starting position.
             */
            if (matchpos) {
                return 0; /* multiple matches, fail */
            }
            matchpos = pg_mbstrlen_with_len(queryText, curpos + 1) + cursorpos;
        } else if (queryText[curpos] == '\'' &&
                   match_prosrc_to_literal(prosrc, &queryText[curpos + 1], cursorpos, &newcursorpos)) {
            /*
             * Found a 'foo' match.  match_prosrc_to_literal() has adjusted
             * for any quotes or backslashes embedded in the literal.
             */
            if (matchpos) {
                return 0; /* multiple matches, fail */
            }
            matchpos = pg_mbstrlen_with_len(queryText, curpos + 1) + newcursorpos;
        }
    }

    return matchpos;
}

/*
 * Try to match the given source text to a single-quoted literal.
 * If successful, adjust newcursorpos to correspond to the character
 * (not byte) index corresponding to cursorpos in the source text.
 *
 * At entry, literal points just past a ' character.  We must check for the
 * trailing quote.
 */
static bool match_prosrc_to_literal(const char* prosrc, const char* literal, int cursorpos, int* newcursorpos)
{
    int newcp = cursorpos;
    int chlen;

    /*
     * This implementation handles backslashes and doubled quotes in the
     * string literal.	It does not handle the SQL syntax for literals
     * continued across line boundaries.
     *
     * We do the comparison a character at a time, not a byte at a time, so
     * that we can do the correct cursorpos math.
     */
    while (*prosrc) {
        cursorpos--; /* characters left before cursor */

        /*
         * Check for backslashes and doubled quotes in the literal; adjust
         * newcp when one is found before the cursor.
         */
        if (*literal == '\\') {
            literal++;
            if (cursorpos > 0) {
                newcp++;
            }
        } else if (*literal == '\'') {
            if (literal[1] != '\'') {
                goto fail;
            }
            literal++;
            if (cursorpos > 0) {
                newcp++;
            }
        }
        chlen = pg_mblen(prosrc);
        if (strncmp(prosrc, literal, chlen) != 0) {
            goto fail;
        }
        prosrc += chlen;
        literal += chlen;
    }

    if (*literal == '\'' && literal[1] != '\'') {
        /* success */
        *newcursorpos = newcp;
        return true;
    }

fail:
    /* Must set *newcursorpos to suppress compiler warning */
    *newcursorpos = newcp;
    return false;
}

static bool pgxc_query_contains_view(List* queries)
{
    ListCell* elt = NULL;

    foreach (elt, queries) {
        ListCell* lc = NULL;
        Query* query = (Query*)lfirst(elt);

        if (query == NULL)
            continue;
        if (!query->rtable)
            continue;
        foreach (lc, query->rtable) {
            RangeTblEntry* rte = (RangeTblEntry*)lfirst(lc);

            if (rte->relkind == RELKIND_VIEW || rte->relkind == RELKIND_CONTQUERY)
                return true;
        }
    }

    return false;
}

/*
 * @Description: Close file handle and delete from file_list.
 * @in library_path: File absolute path.
 */
void delete_file_handle(const char* library_path)
{
    DynamicFileList* file_scanner = NULL;
    DynamicFileList* pre_file_scanner = file_list;

    char* fullname = expand_dynamic_library_name(library_path);
    for (file_scanner = file_list; file_scanner != NULL; file_scanner = file_scanner->next) {
        if (strncmp(fullname, file_scanner->filename, strlen(fullname) + 1) == 0) {
            if (file_list == file_tail) {
                file_list = file_tail = NULL;
            } else if (file_scanner == file_list) {
                file_list = file_list->next;
            } else if (file_scanner == file_tail) {
                pre_file_scanner->next = NULL;
                file_tail = pre_file_scanner;
            } else {
                pre_file_scanner->next = file_scanner->next;
                file_scanner->next = NULL;
            }
            clear_external_function_hash(file_scanner->handle);
            pg_dlclose(file_scanner->handle);
            pfree_ext(file_scanner);
            break;
        } else {
            pre_file_scanner = file_scanner;
        }
    }
}

/*
 * @Description: Check user-defined file path.
 * @in absolutePath: user-defined file path.
 */
void check_file_path(char* absolutePath)
{
    int len = strlen(absolutePath);

    /*
     * Can not include whitespace in the path
     * else can lead to invalid system operate.
     */
    for (int i = 0; i < len; i++) {
        if (!check_special_character(absolutePath[i]) || isspace(absolutePath[i])) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("File path can not include character '%c'", absolutePath[i])));
        }
    }
}
