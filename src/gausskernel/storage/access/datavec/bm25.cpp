/*
* Copyright (c) 2025 Huawei Technologies Co.,Ltd.
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
 * bm25.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/bm25.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "access/multi_redo_api.h"
#include "access/reloptions.h"
#include "miscadmin.h"
#include "storage/smgr/fd.h"
#include "utils/rel.h"
#include "access/datavec/bm25.h"

#include <ctype.h>
#include <sys/stat.h>
#include <unistd.h>
#include <limits.h>
#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

/* Jieba dictionary files relative to dict_path. The user dictionary may be empty. */
struct Bm25DictFileSpec {
    const char* name;
    bool requireNonEmpty;
};

static const Bm25DictFileSpec BM25_DICT_FILES[] = {
    {"jieba.dict.utf8", true},
    {"hmm_model.utf8", true},
    {"user.dict.utf8", false},
    {"idf.utf8", true},
    {"stop_words.utf8", true}
};
#define BM25_DICT_FILE_COUNT (sizeof(BM25_DICT_FILES) / sizeof(BM25_DICT_FILES[0]))

const char* const DEFAULT_TOKENIZER_CACHE_KEY = "DEFAULT";

char* Bm25GetDefaultDictBasePath(void)
{
    char* gausshome = gs_getenv_r("GAUSSHOME");
    if (gausshome == NULL || gausshome[0] == '\0') {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("GAUSSHOME is not set, cannot determine default BM25 dict_path")));
    }
    if (!is_absolute_path(gausshome)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("GAUSSHOME must be an absolute path to determine default BM25 dict_path")));
    }

    char resolvedGausshome[PATH_MAX + 1] = {0};
    const char* baseDir = gausshome;
    if (realpath(gausshome, resolvedGausshome) != NULL) {
        baseDir = resolvedGausshome;
    }

    char basePath[MAXPGPATH] = {0};
    int ret = snprintf_s(basePath, sizeof(basePath), sizeof(basePath) - 1, "%s/lib/jieba_dict", baseDir);
    if (ret < 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("failed to form default BM25 dict_path from GAUSSHOME")));
    }

    char resolved[PATH_MAX + 1] = {0};
    if (realpath(basePath, resolved) != NULL) {
        if (strlen(resolved) >= MAXPGPATH) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("default BM25 dict_path exceeds maximum length")));
        }
        return pstrdup(resolved);
    }
    return pstrdup(basePath);
}

static bool Bm25DictFileHasContent(const char* filepath, const char* fileName)
{
    unsigned char buffer[8192];
    FILE* file = AllocateFile(filepath, PG_BINARY_R);
    if (file == NULL) {
        ereport(ERROR,
            (errcode_for_file_access(), errmsg("cannot open dict file \"%s\": %m", fileName)));
    }

    size_t bytesRead;
    bool hasContent = false;
    while ((bytesRead = fread(buffer, 1, sizeof(buffer), file)) > 0) {
        for (size_t i = 0; i < bytesRead; i++) {
            if (!isspace(buffer[i])) {
                hasContent = true;
                break;
            }
        }
        if (hasContent) {
            break;
        }
    }

    bool readFailed = ferror(file) != 0;
    int closeResult = FreeFile(file);
    if (readFailed || closeResult != 0) {
        ereport(ERROR,
            (errcode_for_file_access(), errmsg("cannot read dict file \"%s\": %m", fileName)));
    }
    return hasContent;
}

/* Validate dict_path input (non-empty, length, absolute, no danger chars, no symlink). On error, ereport. */
static void Bm25ValidateDictPathInput(const char* dictPath)
{
    if (dictPath == NULL || dictPath[0] == '\0') {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("dict_path cannot be empty")));
    }
    if (strlen(dictPath) >= MAXPGPATH) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("dict_path exceeds maximum length")));
    }
    if (dictPath[0] != '/') {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("dict_path must be an absolute path")));
    }
    check_backend_env(dictPath);
#ifdef HAVE_SYMLINK
    {
        struct stat st;
        if (lstat(dictPath, &st) == 0 && S_ISLNK(st.st_mode)) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("dict_path cannot be a symbolic link")));
        }
    }
#endif
}

/* Resolve GAUSSHOME and check resolved dict path is under it. Returns length of gausshome prefix. */
static size_t Bm25GetGausshomePrefixLen(const char* resolved)
{
    char* gausshome = gs_getenv_r("GAUSSHOME");
    if (gausshome == NULL || gausshome[0] == '\0') {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("GAUSSHOME is not set, cannot validate dict_path")));
    }
    char gausshomeResolved[PATH_MAX + 1];
    if (realpath(gausshome, gausshomeResolved) == NULL) {
        ereport(ERROR, (errcode_for_file_access(),
            errmsg("cannot resolve GAUSSHOME \"%s\": %m", gausshome)));
    }
    size_t gausshomeLen = strlen(gausshomeResolved);
    if (strncmp(resolved, gausshomeResolved, gausshomeLen) != 0 ||
        (resolved[gausshomeLen] != '\0' && resolved[gausshomeLen] != '/')) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("dict_path must be under GAUSSHOME directory")));
    }
    return gausshomeLen;
}

/*
 * Validate dictionary file under resolved dict_path:
 *  - path can be lstat'ed and is not a symlink
 *  - file is readable
 *  - file is regular and non-empty when required
 *  - realpath still stays under resolved dict_path
 */
static void Bm25ValidateDictFile(const char* resolvedDir, const char* fileName, bool requireNonEmpty)
{
    char filepath[MAXPGPATH];
    char fileResolved[PATH_MAX + 1];
    struct stat st;
    size_t resolvedDirLen = strlen(resolvedDir);

    int ret = snprintf_s(filepath, sizeof(filepath), sizeof(filepath) - 1, "%s/%s", resolvedDir, fileName);
    if (ret < 0) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("dict_path too long for file \"%s\"", fileName)));
    }

    if (lstat(filepath, &st) != 0) {
        ereport(ERROR,
            (errcode_for_file_access(), errmsg("cannot access dict file \"%s\": %m", fileName)));
    }
    if (S_ISLNK(st.st_mode)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("dict file \"%s\" cannot be a symbolic link", fileName)));
    }
    if (access(filepath, R_OK) != 0) {
        ereport(ERROR,
            (errcode_for_file_access(), errmsg("dict file \"%s\" is not readable", fileName)));
    }
    if (!S_ISREG(st.st_mode)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("dict file \"%s\" is not a regular file", fileName)));
    }
    if (requireNonEmpty && !Bm25DictFileHasContent(filepath, fileName)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("dict file \"%s\" is empty or contains only whitespace", fileName)));
    }
    if (realpath(filepath, fileResolved) == NULL) {
        ereport(ERROR, (errcode_for_file_access(),
            errmsg("cannot resolve dict file \"%s\": %m", fileName)));
    }
    if (strncmp(fileResolved, resolvedDir, resolvedDirLen) != 0 ||
        (fileResolved[resolvedDirLen] != '\0' && fileResolved[resolvedDirLen] != '/')) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("dict file \"%s\" must remain under dict_path", fileName)));
    }
}

/*
 * Validate dict_path and return resolved (canonical) path.
 * On error, ereport(ERROR) and never return.
 * Caller must pfree the result.
 */
char* Bm25ValidateDictPath(const char* dictPath)
{
    char resolved[PATH_MAX + 1];
    size_t i;

    Bm25ValidateDictPathInput(dictPath);
    if (realpath(dictPath, resolved) == NULL) {
        ereport(ERROR, (errcode_for_file_access(),
            errmsg("cannot resolve dict_path \"%s\": %m", dictPath)));
    }
    (void)Bm25GetGausshomePrefixLen(resolved);

    for (i = 0; i < BM25_DICT_FILE_COUNT; i++) {
        Bm25ValidateDictFile(resolved, BM25_DICT_FILES[i].name, BM25_DICT_FILES[i].requireNonEmpty);
    }
    return pstrdup(resolved);
}

/*
 * Estimate the cost of an index scan
 */
static void bm25costestimate_internal(PlannerInfo *root, IndexPath *path, double loop_count, Cost *indexStartupCost,
                                      Cost *indexTotalCost, Selectivity *indexSelectivity, double *indexCorrelation)
{
    if (IsExtremeRedo()) {
        elog(ERROR, "bm25 index do not support extreme rto, please disable extreme rto and re-create bm25 index.");
    }
    /* Never use index without order */
    if (path->indexorderbys == NULL) {
        *indexStartupCost = DBL_MAX;
        *indexTotalCost = DBL_MAX;
        *indexSelectivity = 0;
        *indexCorrelation = 0;
        return;
    }

    *indexStartupCost = 0;
    *indexTotalCost = 0;
    *indexSelectivity = 1;
    *indexCorrelation = 0;
    return;
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(bm25build);
Datum bm25build(PG_FUNCTION_ARGS)
{
    if (IsExtremeRedo()) {
        elog(ERROR, "bm25 index do not support extreme rto.");
    }
    Relation heap = (Relation)PG_GETARG_POINTER(0);
    Relation index = (Relation)PG_GETARG_POINTER(1);
    IndexInfo *indexinfo = (IndexInfo *)PG_GETARG_POINTER(2);
    IndexBuildResult *result = bm25build_internal(heap, index, indexinfo);

    PG_RETURN_POINTER(result);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(bm25buildempty);
Datum bm25buildempty(PG_FUNCTION_ARGS)
{
    if (IsExtremeRedo()) {
        elog(ERROR, "bm25 index do not support extreme rto.");
    }
    Relation index = (Relation)PG_GETARG_POINTER(0);
    bm25buildempty_internal(index);

    PG_RETURN_VOID();
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(bm25beginscan);
Datum bm25beginscan(PG_FUNCTION_ARGS)
{
    Relation rel = (Relation)PG_GETARG_POINTER(0);
    int nkeys = PG_GETARG_INT32(1);
    int norderbys = PG_GETARG_INT32(2);
    IndexScanDesc scan = bm25beginscan_internal(rel, nkeys, norderbys);

    PG_RETURN_POINTER(scan);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(bm25rescan);
Datum bm25rescan(PG_FUNCTION_ARGS)
{
    IndexScanDesc scan = (IndexScanDesc)PG_GETARG_POINTER(0);
    ScanKey scankey = (ScanKey)PG_GETARG_POINTER(1);
    int nkeys = PG_GETARG_INT32(2);
    ScanKey orderbys = (ScanKey)PG_GETARG_POINTER(3);
    int norderbys = PG_GETARG_INT32(4);
    bm25rescan_internal(scan, scankey, nkeys, orderbys, norderbys);

    PG_RETURN_VOID();
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(bm25gettuple);
Datum bm25gettuple(PG_FUNCTION_ARGS)
{
    IndexScanDesc scan = (IndexScanDesc)PG_GETARG_POINTER(0);
    ScanDirection direction = (ScanDirection)PG_GETARG_INT32(1);

    if (NULL == scan)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid arguments for function bm25gettuple")));

    bool result = bm25gettuple_internal(scan, direction);

    PG_RETURN_BOOL(result);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(bm25endscan);
Datum bm25endscan(PG_FUNCTION_ARGS)
{
    IndexScanDesc scan = (IndexScanDesc)PG_GETARG_POINTER(0);
    bm25endscan_internal(scan);

    PG_RETURN_VOID();
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(bm25costestimate);
Datum bm25costestimate(PG_FUNCTION_ARGS)
{
    PlannerInfo *root = (PlannerInfo *)PG_GETARG_POINTER(0);
    IndexPath *path = (IndexPath *)PG_GETARG_POINTER(1);
    double loopcount = static_cast<double>(PG_GETARG_FLOAT8(2));
    Cost *startupcost = (Cost *)PG_GETARG_POINTER(3);
    Cost *totalcost = (Cost *)PG_GETARG_POINTER(4);
    Selectivity *selectivity = (Selectivity *)PG_GETARG_POINTER(5);
    double *correlation = reinterpret_cast<double *>(PG_GETARG_POINTER(6));
    bm25costestimate_internal(root, path, loopcount, startupcost, totalcost, selectivity, correlation);

    PG_RETURN_VOID();
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(bm25insert);
Datum bm25insert(PG_FUNCTION_ARGS)
{
    if (IsExtremeRedo()) {
        elog(ERROR, "bm25 index do not support extreme rto.");
    }
    Relation rel = (Relation)PG_GETARG_POINTER(0);
    Datum *values = (Datum *)PG_GETARG_POINTER(1);
    bool *isnull = reinterpret_cast<bool *>(PG_GETARG_POINTER(2));
    ItemPointer ht_ctid = (ItemPointer)PG_GETARG_POINTER(3);
    if (isnull[0]) {
        PG_RETURN_BOOL(false);
    }

    bool result = bm25insert_internal(rel, values, ht_ctid);

    PG_RETURN_BOOL(result);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(bm25options);
Datum bm25options(PG_FUNCTION_ARGS)
{
    Datum reloptions = PG_GETARG_DATUM(0);
    bool validate = PG_GETARG_BOOL(1);

    static const relopt_parse_elt tab[] = {
        {"dict_path", RELOPT_TYPE_STRING, offsetof(Bm25Options, dictPath)},
    };

    relopt_value *options;
    int numoptions;
    Bm25Options *rdopts;

    options = parseRelOptions(reloptions, validate, RELOPT_KIND_BM25, &numoptions);
    rdopts = (Bm25Options *)allocateReloptStruct(sizeof(Bm25Options), options, numoptions);
    fillRelOptions((void *)rdopts, sizeof(Bm25Options), options, numoptions, validate, tab, lengthof(tab));

    if (rdopts != NULL) {
        PG_RETURN_BYTEA_P(rdopts);
    }

    PG_RETURN_NULL();
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(bm25bulkdelete);
Datum bm25bulkdelete(PG_FUNCTION_ARGS)
{
    if (IsExtremeRedo()) {
        elog(ERROR, "bm25 index do not support extreme rto.");
    }
    IndexVacuumInfo *info = (IndexVacuumInfo *)PG_GETARG_POINTER(0);
    IndexBulkDeleteResult *volatile stats = (IndexBulkDeleteResult *)PG_GETARG_POINTER(1);
    IndexBulkDeleteCallback callback = (IndexBulkDeleteCallback)PG_GETARG_POINTER(2);
    void *callbackState = static_cast<void *>(PG_GETARG_POINTER(3));
    stats = bm25bulkdelete_internal(info, stats, callback, callbackState);

    PG_RETURN_POINTER(stats);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(bm25vacuumcleanup);
Datum bm25vacuumcleanup(PG_FUNCTION_ARGS)
{
    if (IsExtremeRedo()) {
        elog(ERROR, "bm25 index do not support extreme rto.");
    }
    IndexVacuumInfo *info = (IndexVacuumInfo *)PG_GETARG_POINTER(0);
    IndexBulkDeleteResult *stats = (IndexBulkDeleteResult *)PG_GETARG_POINTER(1);
    stats = bm25vacuumcleanup_internal(info, stats);
    PG_RETURN_POINTER(stats);
}
