/* -------------------------------------------------------------------------
 *
 * dist_fdw.cpp
 *		  foreign-data wrapper for server-side flat files.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Copyright (c) 2010-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * IDENTIFICATION
 *		  src/gausskernel/storage/bulkload/dist_fdw.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <arpa/inet.h>
#include <fnmatch.h>

#include "access/reloptions.h"
#include "access/sysattr.h"
#include "access/obs/obs_am.h"
#include "access/tableam.h"
#include "catalog/namespace.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_trigger.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "commands/vacuum.h"
#include "commands/tablecmds.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "nodes/nodes.h"
#include "vecexecutor/vecnodes.h"
#include "bulkload/foreignroutine.h"
#include "bulkload/vecforeignroutine.h"
#include "bulkload/dist_fdw.h"
#include "bulkload/utils.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "mb/pg_wchar.h"
#include "optimizer/var.h"
#include "optimizer/pgxcship.h"
#include "rewrite/rewriteHandler.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "nodes/relation.h"
#include "parser/parsetree.h"
#include "utils/plog.h"

/*
 * Describes the valid options for objects that use this wrapper.
 */
struct DistFdwOption {
    const char *optname;
    Oid optcontext; /* Oid of catalog in which option may appear */
};

static const char *const strTextFormat = "text";
static const char *const strCsvFormat = "csv";
static const char *const strFixedFormat = "fixed";

// Bulkload available options
const char *optLocation = "location";
const char *optFormat = "format";
const char *optHeader = "header";
const char *optDelimiter = "delimiter";
const char *optQutote = "quote";
const char *optEscape = "escape";
const char *optNull = "null";
const char *optEncoding = "encoding";
const char *optFillMissFields = "fill_missing_fields";
const char *optRejectLimit = "reject_limit";
const char *optMode = "mode";
const char *optForceNotNull = "force_not_null";
const char *optWriteOnly = "write_only";
const char *optWithoutEscaping = "noescaping";
const char *optErrorRel = "error_table";
const char *optFormatter = "formatter";
const char *optEol = "eol";
const char *optFix = "fix";
const char *optFileHeader = "fileheader";
const char *optOutputFilePrefix = "out_filename_prefix";
const char *optOutputFixAlignment = "out_fix_alignment";
const char *optLogRemote = "log_remote";
const char *optSessionKey = "session_key";
const char *optTaskList = "task_list";
const char *optIgnoreExtraData = "ignore_extra_data";

/* OBS specific options */
const char *optChunkSize = "chunksize";
const char *optEncrypt = "encrypt";
const char *optAccessKey = "access_key";
const char *optSecretAccessKey = "secret_access_key";

/*
 * bulkload compatible illegal chars option
 */
const char *optCompatibleIllegalChars = "compatible_illegal_chars";
/*
 * bulkload datetime format options
 */
const char *optDateFormat = "date_format";
const char *optTimeFormat = "time_format";
const char *optTimestampFormat = "timestamp_format";
const char *optSmalldatetimeFormat = "smalldatetime_format";

const char *LOCAL_PREFIX = "file://";
const char *GSFS_PREFIX = "gsfs://";
/* GSFSS_PREFIX protocol is a secure version of the GSFS_PREFIX protocol */
const char *GSFSS_PREFIX = "gsfss://";
const char *ROACH_PREFIX = "roach://";
const char *GSOBS_PREFIX = "gsobs://";

const int LOCAL_PREFIX_LEN = strlen(LOCAL_PREFIX);
const int GSFS_PREFIX_LEN = strlen(GSFS_PREFIX);
const int GSFSS_PREFIX_LEN = strlen(GSFSS_PREFIX);
const int ROACH_PREFIX_LEN = strlen(ROACH_PREFIX);
const int GSOBS_PREFIX_LEN = strlen(GSOBS_PREFIX);

/*
 * Valid options for file_fdw.
 * These options are based on the options for COPY FROM command.
 *
 * Note: If you are adding new option for user mapping, you need to modify
 * distGetOptions(), which currently doesn't bother to look at user mappings.
 */
static struct DistFdwOption loader_valid_options[] = {
    /* File options */
    { optLocation, ForeignTableRelationId },
    { OPTION_NAME_REGION, ForeignTableRelationId },

    /* Format options */
    /* oids option is not supported */
    { optFormat, ForeignTableRelationId },
    { optHeader, ForeignTableRelationId },
    { optDelimiter, ForeignTableRelationId },
    { optQutote, ForeignTableRelationId },
    { optEscape, ForeignTableRelationId },
    { optNull, ForeignTableRelationId },
    { optEncoding, ForeignTableRelationId },
    { optFillMissFields, ForeignTableRelationId },
    { optMode, ForeignTableRelationId },
    { optWithoutEscaping, ForeignTableRelationId },
    { optForceNotNull, AttributeRelationId },
    { optEol, ForeignTableRelationId },
    { optFix, ForeignTableRelationId },
    { optFileHeader, ForeignTableRelationId },
    { optOutputFilePrefix, ForeignTableRelationId },
    { optOutputFixAlignment, ForeignTableRelationId },
    { optRejectLimit, ForeignTableRelationId },
    { optIgnoreExtraData, ForeignTableRelationId },

    /* OBS only options */
    { optChunkSize, ForeignTableRelationId },
    { optEncrypt, ForeignTableRelationId },
    { optAccessKey, ForeignTableRelationId },
    { optSecretAccessKey, ForeignTableRelationId },

    /*
     * bulkload compatible illegal chars option
     */
    { optCompatibleIllegalChars, ForeignTableRelationId },
    /*
     * bulkload datetime format options
     */
    { optDateFormat, ForeignTableRelationId },
    { optTimeFormat, ForeignTableRelationId },
    { optTimestampFormat, ForeignTableRelationId },
    { optSmalldatetimeFormat, ForeignTableRelationId },
    /* Sentinel */
    { NULL, InvalidOid }
};

PG_FUNCTION_INFO_V1(dist_fdw_handler);
PG_FUNCTION_INFO_V1(dist_fdw_validator);

#define BULKLOAD_FILE_MAX_SIZE_TO_DIVIDE (64 * 1024 * 1024)
#define BULKLOAD_FILE_SIZE_OF_DIVIDE (16 * 1024 * 1024)
#define BULKLOAD_RAW_BUFFER_SIZE 65536

#ifdef ENABLE_UT
#define static
#endif

IdGen gt_sessionId = {0, 0, false};

static bool distAnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc *func, BlockNumber *totalpages,
                                    void *additionalData = NULL, bool estimate_table_rownum = false);

static int distAcquireSampleRows(Relation relation, int logLevel, HeapTuple *sampleRows, int targetRowCount,
                                 double *totalRowCount, double *totalDeadCount, void *additionalData,
                                 bool estimate_table_rownum);

/*
 * FDW insert callback routinues
 */
static List *distExportPlan(PlannerInfo *root, ModifyTable *plan, Index resultRelation, int subplan_index);
static void distExportBegin(ModifyTableState *mtstate, ResultRelInfo *rinfo, List *fdw_private, int subplan_index,
                            int eflags);
static TupleTableSlot *distExportExec(EState *estate, ResultRelInfo *resultRelInfo, TupleTableSlot *slot,
                                      TupleTableSlot *planSlot);
static void distExportEnd(EState *estate, ResultRelInfo *resultRelInfo);
static int distExportRelUpdatable(Relation rel);

/*
 * Helper functions
 */
static bool is_valid_option(const char *option, Oid context);

static void CheckAndGetUserExportDir(const char *userExportDir);
static void CreateDirIfNecessary(const char *mydir);
static void InitExportEnvirnment(const char *userExportDir);
static void distValidateTableDef(Node *Obj);

extern List *CopyGetAttnums(TupleDesc tupDesc, Relation rel, List *attnamelist);

extern void bulkloadFuncFactory(CopyState cstate);

extern void UntransformFormatterOption(DefElem *def);
extern void SetFixedAlignment(TupleDesc tupDesc, Relation rel, FixFormatter *formatter, const char *char_alignment);
extern void VerifyEncoding(int encoding);
extern void GetDistImportOptions(Oid relOid, DistImportPlanState *planstate, ForeignOptions *fOptions = NULL);

#ifndef ENABLE_LITE_MODE
static void assignOBSTaskToDataNode(List *urllist, List **totalTask, List *dnNames, DistImportPlanState *planstate,
                                    int64 *fileNum = NULL);
#endif
static void assignTaskToDataNodeInSharedMode(List *urllist, List **totalTask, List *dnNames);
static void assignTaskToDataNodeInNormalMode(List *urllist, List **totalTask, List *dnNames, int dop);

extern void decryptOBSForeignTableOption(List **options);

List *getOBSFileList(List *urllist, bool encrypt, const char *access_key, const char *secret_access_key,
                     bool isAnalyze);
#ifndef ENABLE_LITE_MODE
/*
 * In OBS parallel data loading case, we may have # of datanodes not
 * equal to # of objects, as one object can only be assign to one
 * datandoes, so we create or fetch pre-created DistFdwDataNodeTask
 * handlers basing on num_processed
 */
static void assignOBSFileToDataNode(List *urllist, List **totalTask, List *dnNames);
#endif

/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
Datum dist_fdw_handler(PG_FUNCTION_ARGS)
{
    FdwRoutine *fdwroutine = makeNode(FdwRoutine);

    fdwroutine->GetForeignRelSize = distImportGetRelSize;
    fdwroutine->GetForeignPaths = distImportGetPaths;
    fdwroutine->GetForeignPlan = distImportGetPlan;
    fdwroutine->ExplainForeignScan = distImportExplain;
    fdwroutine->BeginForeignScan = distImportBegin;
    fdwroutine->IterateForeignScan = distExecImport;
    fdwroutine->ReScanForeignScan = distReImport;
    fdwroutine->EndForeignScan = distImportEnd;

    /* Functions for updating foreign tables */
    fdwroutine->AddForeignUpdateTargets = NULL;
    fdwroutine->PlanForeignModify = distExportPlan;
    fdwroutine->BeginForeignModify = distExportBegin;
    fdwroutine->ExecForeignInsert = distExportExec;
    fdwroutine->ExecForeignUpdate = NULL;
    fdwroutine->ExecForeignDelete = NULL;
    fdwroutine->EndForeignModify = distExportEnd;
    fdwroutine->IsForeignRelUpdatable = distExportRelUpdatable;

    fdwroutine->ExplainForeignModify = NULL;

    fdwroutine->AnalyzeForeignTable = distAnalyzeForeignTable;
    fdwroutine->AcquireSampleRows = distAcquireSampleRows;

    fdwroutine->VecIterateForeignScan = distExecVecImport;

    fdwroutine->ValidateTableDef = distValidateTableDef;

    /* @hdfs
     * PartitionTblProcess is only used for hdfs_fdw now, so set null here.
     */
    fdwroutine->PartitionTblProcess = NULL;
    fdwroutine->BuildRuntimePredicate = NULL;
    PG_RETURN_POINTER(fdwroutine);
}

/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses file_fdw.
 *
 * Raise an ERROR if the option or its value is considered invalid.
 */
Datum dist_fdw_validator(PG_FUNCTION_ARGS)
{
    List *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
    Oid catalog = PG_GETARG_OID(1);
    DistImportPlanState planstate;
    DistImportExecutionState execState;
    errno_t rc = EOK;

    rc = memset_s(&planstate, sizeof(planstate), 0, sizeof(planstate));
    securec_check(rc, "\0", "\0");
    rc = memset_s(&execState, sizeof(execState), 0, sizeof(execState));
    securec_check(rc, "\0", "\0");
    ProcessDistImportOptions(&planstate, options_list, true, catalog != ForeignTableRelationId);
    ProcessCopyOptions((CopyState) & execState, !planstate.writeOnly, planstate.options);
    VerifyEncoding(planstate.fileEncoding);
    if (execState.fileformat != FORMAT_TEXT && IS_SHARED_MODE(planstate.mode))
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("SHARED mode can only be used with TEXT format")));
    PG_RETURN_VOID();
}

ErrorCacheEntry *GetForeignErrCacheEntry(Oid relid, uint32 distSessionKey)
{
    ErrorCacheEntry *entry = NULL;
    DefElem *def = GetForeignTableOptionByName(relid, optErrorRel);

    if (def != NULL) {
        RangeTblEntry *rte = NULL;
        char *relname = strVal(def->arg);
        Oid errorOid = get_relname_relid(relname, get_rel_namespace(relid));

        entry = makeNode(ErrorCacheEntry);
        if (errorOid == InvalidOid)
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("can't find error record table \"%s\"", relname)));
        entry->filename = generate_unique_cache_name_prefix(errorOid, distSessionKey);

        rte = (RangeTblEntry *)makeNode(RangeTblEntry);
        rte->rtekind = RTE_RELATION;
        rte->relid = errorOid;
        rte->relkind = get_rel_relkind(errorOid);
        rte->requiredPerms = ACL_INSERT;
        entry->rte = rte;
    }
    return entry;
}

/*
 * Check if the provided option is one of the valid options.
 * context is the Oid of the catalog holding the object the option is for.
 */
static bool is_valid_option(const char *option, Oid context)
{
    struct DistFdwOption *opt = NULL;

    for (opt = loader_valid_options; opt->optname; opt++) {
        if (context == opt->optcontext && strcmp(opt->optname, option) == 0)
            return true;
    }
    return false;
}

/*
 * Fetch the options for a file_fdw foreign table.
 *
 * We have to separate out "filename" from the other options because
 * it must not appear in the options list passed to the core COPY code.
 */
void distGetOptions(Oid foreigntableid, char **locaionts, List **other_options)
{
    ForeignTable *table = NULL;
    ForeignServer *server = NULL;
    ForeignDataWrapper *wrapper = NULL;
    List *options = NIL;
    ListCell *lc = NULL;
    ListCell *prev = NULL;

    /*
     * Extract options from FDW objects.  We ignore user mappings because
     * file_fdw doesn't have any options that can be specified there.
     *
     * (XXX Actually, given the current contents of valid_options[], there's
     * no point in examining anything except the foreign table's own options.
     * Simplify?)
     */
    table = GetForeignTable(foreigntableid);
    server = GetForeignServer(table->serverid);
    wrapper = GetForeignDataWrapper(server->fdwid);

    options = NIL;
    options = list_concat(options, wrapper->options);
    options = list_concat(options, server->options);
    options = list_concat(options, table->options);

    options = adaptOBSURL(options);

    *locaionts = NULL;
    prev = NULL;
    foreach (lc, options) {
        DefElem *def = (DefElem *)lfirst(lc);

        if (strcmp(def->defname, optLocation) == 0) {
            *locaionts = defGetString(def);
            options = list_delete_cell(options, lc, prev);
            break;
        }
        prev = lc;
    }

    if (*locaionts == NULL)
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("LOCATION is required for the foreign tables")));
    *other_options = options;
}

/*
 * @hdfs
 * In order to match AnalyzeForeignTable function input parameter change
 * we add the parameter (void* additionalData). This parameter may not be
 * used by fileAnalyzeForeignTable function.
 */
static bool distAnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc *func, BlockNumber *totalPageCount,
                                    void *additionalData, bool estimate_table_rownum)
{
    if (isWriteOnlyFt(RelationGetRelid(relation))) {
        return false;
    }
    if (!IS_OBS_CSV_TXT_FOREIGN_TABLE(RelationGetRelid(relation))) {
        return false;
    }

    if (additionalData == NULL)
        return true;

    (*totalPageCount) = getPageCountForFt(additionalData);

    return true;
}

#ifndef ENABLE_LITE_MODE
/**
 * @Description: Scheduler file for dist obs foreign table.
 * @in foreignTableId, the given foreign table Oid.
 * @return return the dn task.
 */
List *CNSchedulingForDistOBSFt(Oid foreignTableId)
{
    char *url = HdfsGetOptionValue(foreignTableId, optLocation);
    char *regionCode = HdfsGetOptionValue(foreignTableId, OPTION_NAME_REGION);
    char *newUrl = url;
    errno_t rc = EOK;

    Assert(url != NULL);
    if (url != NULL && pg_strncasecmp(url, OBS_PREFIX, OBS_PREfIX_LEN) == 0) {
        /* the regionCode may be NULL, we will get the default region. */
        newUrl = rebuildAllLocationOptions(regionCode, url);
    }

    List *urlList = DeserializeLocations(newUrl);
    bool encrypt = false;
    DefElem *encryptStr = HdfsGetOptionDefElem(foreignTableId, OPTION_NAME_SERVER_ENCRYPT);
    char *ak = HdfsGetOptionValue(foreignTableId, OPTION_NAME_SERVER_AK);
    char *sak = HdfsGetOptionValue(foreignTableId, OPTION_NAME_SERVER_SAK);

    if (encryptStr != NULL) {
        encrypt = defGetBoolean(encryptStr);
    }

    List *obsFileList = getOBSFileList(urlList, encrypt, ak, sak, true);
    List *totalTask = NIL;
    List *nodeList = NIL;
    RelationLocInfo *rlc = GetRelationLocInfo(foreignTableId);
    if (rlc != NULL) {
        nodeList = rlc->nodeList;
    }

    /* get all data node names */
    List *dnNames = !nodeList ? PgxcNodeGetAllDataNodeNames() : PgxcNodeGetDataNodeNames(nodeList);

    /* assign obs file to each data node */
    assignOBSFileToDataNode(obsFileList, &totalTask, dnNames);

    pfree(rlc);
    if (sak != NULL) {
        rc = memset_s(sak, strlen(sak), 0, strlen(sak));
        securec_check(rc, "\0", "\0");
        pfree(sak);
    }
    return totalTask;
}
#endif

/**
 * @Description: Build the related scanState information.
 * @in relation, the relation.
 * @in splitinfo, the current datanode file list.
 * @out
 * @out
 * @return
 */
ForeignScanState *buildRelatedStateInfo(Relation relation, DistFdwFileSegment *splitinfo)
{
    ForeignScanState *scanState = NULL;

    TupleTableSlot *scanTupleSlot = NULL;
    List *HDFSNodeWorkList = NIL;
    List *fileWorkList = NIL;
    DistFdwFileSegment *fileInfo = NULL;
    TupleDesc tupleDescriptor = RelationGetDescr(relation);
    int columnCount = tupleDescriptor->natts;

    Datum *columnValues = (Datum *)palloc0(columnCount * sizeof(Datum));
    bool *columnNulls = (bool *)palloc0(columnCount * sizeof(bool));

    fileInfo = makeNode(DistFdwFileSegment);
    fileInfo->filename = splitinfo->filename;

    DistFdwDataNodeTask *fileSplitTask = NULL;
    /* Put file information into SplitMap struct */
    fileSplitTask = makeNode(DistFdwDataNodeTask);
    fileWorkList = lappend(fileWorkList, (void *)fileInfo);
    fileSplitTask->dnName = g_instance.attr.attr_common.PGXCNodeName;
    fileSplitTask->task = fileWorkList;

    HDFSNodeWorkList = lappend(HDFSNodeWorkList, fileSplitTask);

    /* setup foreign scan plan node */
    ForeignScan *foreignScan = NULL;
    foreignScan = makeNode(ForeignScan);
    foreignScan->fdw_private = lappend(foreignScan->fdw_private,
                                       makeDefElem(pstrdup(optTaskList), (Node *)HDFSNodeWorkList));
    ;

    /* setup tuple slot */
    scanTupleSlot = MakeTupleTableSlot(true, tupleDescriptor->td_tam_ops);
    scanTupleSlot->tts_tupleDescriptor = tupleDescriptor;
    scanTupleSlot->tts_values = columnValues;
    scanTupleSlot->tts_isnull = columnNulls;

    /* setup scan state */
    scanState = makeNode(ForeignScanState);
    scanState->ss.ss_currentRelation = relation;
    scanState->ss.ps.plan = (Plan *)foreignScan;
    scanState->ss.ss_ScanTupleSlot = scanTupleSlot;

    scanState->scanMcxt = AllocSetContextCreate(CurrentMemoryContext, "analyze for Foreign Scan",
                                                ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE,
                                                ALLOCSET_DEFAULT_MAXSIZE);

    distImportBegin(scanState, 0);

    return scanState;
}

/*
 * Brief: Get a random sample of rows from the dist obs foreign table. Checked rows
 * are returned in the caller allocated sampleRows array, which must have at least
 * target row count entries. The practical counts of rows checked is returned as
 * the function result. We also count the number of rows in the collection and
 * return it in row count entries. Always set dead row count to zero.
 *
 * Pay attention to be that there would have same differences betwen the order of the returned
 * list of rows and their actual order in the Orc file. Thence, related estimates
 * derived later could be inaccurate,
 * but that's OK. Currently don't use related estimates (the planner only
 * concern correlation for index scans).
 *
 * input param @relation: relation we want to sample;
 * input param @logLevel: log level;
 * input param @sampleRows: store samples;
 * input param @targetRowCount:count of tuples we want to sample;
 * input param @totalRowCount:actual count of tuples we want sampled;
 * input param @deadRows: is used as a input parameter to pass data using for analyzing operation;
 * input param @additionalData:we use this parameter to pass data.
 */
static int distAcquireSampleRows(Relation relation, int logLevel, HeapTuple *sampleRows, int targetRowCount,
                                 double *totalRowCount, double *deadRows, void *additionalData, bool estimate_table_rownum)
{
    /* We report "analyze" nothing if additionalData is null.  */
    if (additionalData == NULL) {
        ereport(logLevel, (errmodule(MOD_HDFS), errmsg("\"%s\": scanned %.0f tuples and sampled %d tuples.",
                                                       RelationGetRelationName(relation), 0.0, 0)));
        return 0;
    }
    int sampleRowCount = 0;
    double rowCount = 0.0;
    double rowCountToSkip = -1; /* -1 means not set yet */
    double selectionState = 0;
    MemoryContext oldContext = NULL;
    MemoryContext tupleContext = NULL;

    List *fileList = NIL;
    ForeignScanState *scanState = NULL;
    BlockSamplerData bs;
    TupleDesc tupleDescriptor = RelationGetDescr(relation);
    int columnCount = tupleDescriptor->natts;
    double liveRows = 0; /* tuple count we scanned */

    /* get filename list */
    DistFdwDataNodeTask *fileMap = (DistFdwDataNodeTask *)additionalData;
    fileList = fileMap->task;
    unsigned int totalFileNumbers = (unsigned int)list_length(fileList);

    /* Init blocksampler, this function will help us decide which file to be sampled. */
    BlockSampler_Init(&bs, totalFileNumbers, targetRowCount);

    while (BlockSampler_HasMore(&bs)) { /* If still have file to be sampled, we will be into while loop */
        /* Get File No. */
        unsigned int targFile = BlockSampler_Next(&bs);
        TupleTableSlot *scanTupleSlot = NULL;
        /* Put file information into SplitInfo struct */
        DistFdwFileSegment *splitinfo = (DistFdwFileSegment *)list_nth(fileList, (int)targFile);

        scanState = buildRelatedStateInfo(relation, splitinfo);

        /*
         * Use per-tuple memory context to prevent leak of memory used to read and
         * parse rows from the file using ReadLineFromFile and FillTupleSlot.
         */
        tupleContext = AllocSetContextCreate(CurrentMemoryContext, "TEX/CSV OBS temporary context",
                                             ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE,
                                             ALLOCSET_DEFAULT_MAXSIZE);

        /* prepare for sampling rows */
        selectionState = anl_init_selection_state(targetRowCount);

        for (;;) {
            /* check for user-requested abort or sleep */
            vacuum_delay_point();

            tupleDescriptor = scanState->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
            scanTupleSlot = scanState->ss.ss_ScanTupleSlot;
            Datum *columnValues = scanTupleSlot->tts_values;
            bool *columnNulls = scanTupleSlot->tts_isnull;

            /* clean columnValues and columnNulls */
            errno_t rc = memset_s(columnValues, (Size)columnCount * sizeof(Datum), 0, columnCount * sizeof(Datum));
            securec_check(rc, "\0", "\0");
            rc = memset_s(columnNulls, (Size)columnCount * sizeof(bool), true, columnCount * sizeof(bool));
            securec_check(rc, "\0", "\0");

            /* read the next record */
            /* need change to batch read */
            MemoryContextReset(tupleContext);
            oldContext = MemoryContextSwitchTo(tupleContext);
            (void)distExecImport(scanState);
            (void)MemoryContextSwitchTo(oldContext);

            /* if there are no more records to read, break */
            if (TTS_EMPTY(scanTupleSlot)) {
                break;
            }

            ++liveRows;

            /*
             * The first targetRowCount sample rows are simply copied into the
             * reservoir. Then we start replacing tuples in the sample until we
             * reach the end of the relation.
             */
            if (sampleRowCount < targetRowCount) {
                sampleRows[sampleRowCount++] = (HeapTuple)tableam_tops_form_tuple(tupleDescriptor, columnValues, columnNulls);
            } else {
                /*
                 * If we need to compute a new S value, we must use the "not yet
                 * incremented" value of rowCount as t.
                 */
                if (rowCountToSkip < 0) {
                    rowCountToSkip = anl_get_next_S(rowCount, targetRowCount, &selectionState);
                }

                if (rowCountToSkip <= 0) {
                    /*
                     * Found a suitable tuple, so save it, replacing one old tuple
                     * at random.
                     */
                    int rowIndex = (int)(targetRowCount * anl_random_fract());
                    Assert(rowIndex >= 0);
                    Assert(rowIndex < targetRowCount);

                    heap_freetuple(sampleRows[rowIndex]);
                    sampleRows[rowIndex] = (HeapTuple)tableam_tops_form_tuple(tupleDescriptor, columnValues, columnNulls);
                }
                rowCountToSkip -= 1;
            }
            rowCount += 1;
        }

        /* clean up */
        MemoryContextDelete(tupleContext);
        pfree(scanTupleSlot->tts_values);
        pfree(scanTupleSlot->tts_isnull);
        pfree_ext(scanTupleSlot->tts_lobPointers);
        ForeignScan *fscan = (ForeignScan *)scanState->ss.ps.plan;
        List *workList = (List *)(((DefElem *)linitial(fscan->fdw_private))->arg);
        list_free(workList);
        list_free(fscan->fdw_private);
        distImportEnd(scanState);

        if (estimate_table_rownum == true)
            break;
    }

    if (estimate_table_rownum == true) {
        (*totalRowCount) = totalFileNumbers * liveRows;
        return sampleRowCount;
    }

    /* free list */
    list_free(fileList);

    /* emit some interesting relation info */
    ereport(logLevel, (errmodule(MOD_HDFS), errmsg("\"%s\": scanned %.0f tuples and sampled %d tuples",
                                                   RelationGetRelationName(relation), rowCount, sampleRowCount)));

    /* @hdfs estimate totalRowCount */
    if (bs.m > 0) {
        *totalRowCount = floor((liveRows / bs.m) * totalFileNumbers + 0.5);
    } else {
        *totalRowCount = 0.0;
    }

    (*deadRows) = 0; /* @hdfs dead rows is no means to foreign table */
    return sampleRowCount;
}

bool check_selective_binary_conversion(RelOptInfo *baserel, Oid foreigntableid, List **columns)
{
    ForeignTable *table = NULL;
    ListCell *lc = NULL;
    Relation rel;
    TupleDesc tupleDesc;
    AttrNumber attnum;
    Bitmapset *attrs_used = NULL;
    bool has_wholerow = false;
    int numattrs;
    int i;

    *columns = NIL; /* default result */

    /*
     * Check format of the file.  If binary format, this is irrelevant.
     */
    table = GetForeignTable(foreigntableid);
    foreach (lc, table->options) {
        DefElem *def = (DefElem *)lfirst(lc);

        if (strcmp(def->defname, optFormat) == 0) {
            char *format = defGetString(def);

            if (strcmp(format, "binary") == 0)
                return false;
            break;
        }
    }

    /* Collect all the attributes needed for joins or final output. */
    pull_varattnos((Node *)baserel->reltarget->exprs, baserel->relid, &attrs_used);

    /* Add all the attributes used by restriction clauses. */
    foreach (lc, baserel->baserestrictinfo) {
        RestrictInfo *rinfo = (RestrictInfo *)lfirst(lc);

        pull_varattnos((Node *)rinfo->clause, baserel->relid, &attrs_used);
    }

    /* Convert attribute numbers to column names. */
    rel = heap_open(foreigntableid, AccessShareLock);
    tupleDesc = RelationGetDescr(rel);

    while ((attnum = (AttrNumber)bms_first_member(attrs_used)) >= 0) {
        /* Adjust for system attributes. */
        attnum += FirstLowInvalidHeapAttributeNumber;

        if (attnum == 0) {
            has_wholerow = true;
            break;
        }

        /* Ignore system attributes. */
        if (attnum < 0)
            continue;

        /* Get user attributes. */
        if (attnum > 0) {
            Form_pg_attribute attr = &tupleDesc->attrs[attnum - 1];
            char *attname = NameStr(attr->attname);

            /* Skip dropped attributes (probably shouldn't see any here). */
            if (attr->attisdropped)
                continue;
            *columns = lappend(*columns, makeString(pstrdup(attname)));
        }
    }

    /* Count non-dropped user attributes while we have the tupdesc. */
    numattrs = 0;
    for (i = 0; i < tupleDesc->natts; i++) {
        Form_pg_attribute attr = &tupleDesc->attrs[i];

        if (attr->attisdropped)
            continue;
        numattrs++;
    }

    heap_close(rel, AccessShareLock);

    /* If there's a whole-row reference, fail: we need all the columns. */
    if (has_wholerow) {
        *columns = NIL;
        return false;
    }

    /* If all the user attributes are needed, fail. */
    if (numattrs == list_length(*columns)) {
        *columns = NIL;
        return false;
    }

    return true;
}

/*
 * read raw buffer
 */
static int getRawBuffer(FILE *file, char *buf, long pos, long len, char *filename)
{
    int bytesread = 0;
    if (fseek(file, pos, SEEK_SET)) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("unable to fseek file \"%s\"", filename)));
    }

    bytesread = (int)fread(buf, 1, len, file);
    if (ferror(file)) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not read from file: %m")));
    }

    Assert(bytesread == len);
    return bytesread;
}

/*
 * search char from begin to end
 */
static long searchForward(FILE *file, char *buf, long begin, long end, char *fileName, char searchChar)
{
    long readStartPos = end;
    long rawLen = BULKLOAD_RAW_BUFFER_SIZE;
    long retPos = 0;

    do {
        /* move  forward 64K */
        readStartPos -= BULKLOAD_RAW_BUFFER_SIZE;
        rawLen = BULKLOAD_RAW_BUFFER_SIZE;
        if (readStartPos < begin) {
            rawLen = BULKLOAD_RAW_BUFFER_SIZE - (begin - readStartPos);
            readStartPos = begin;
        }

        int bytesread = getRawBuffer(file, buf, readStartPos, rawLen, fileName);
        int pos = bytesread - 1;
        /* search '\n' */
        while (pos >= 0 && buf[pos] != searchChar) {
            pos--;
        }

        if (pos != -1) {
            /* find it */
            retPos = readStartPos + pos;
            break;
        }
    } while (readStartPos > begin);

    return retPos;
}

/*
 * Divide file into segments
 */
void divideFileSegment(char *fileName, long fileSize, List **segmentlist)
{
    Assert(fileName && segmentlist && fileSize >= 0);
    Assert(BULKLOAD_FILE_SIZE_OF_DIVIDE >= BULKLOAD_RAW_BUFFER_SIZE);
    Assert(BULKLOAD_FILE_MAX_SIZE_TO_DIVIDE >= BULKLOAD_FILE_SIZE_OF_DIVIDE);

    DistFdwFileSegment *segment = NULL;

    if (fileSize > BULKLOAD_FILE_MAX_SIZE_TO_DIVIDE) {
        /* >64MB need split */
        char *buf = (char *)palloc0(BULKLOAD_RAW_BUFFER_SIZE);
        FILE *file = AllocateFile(fileName, "r");
        if (file == NULL) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("unable to open file \"%s\"", fileName)));
        }

        /* region of search */
        long searchBegin = 0;
        long searchEnd = BULKLOAD_FILE_SIZE_OF_DIVIDE;

        long segmentBegin = 0;
        long pos = 0;

        do {
            /* search '\n' */
            pos = searchForward(file, buf, searchBegin, searchEnd, fileName, '\n');
            if (pos != 0) {
                /* find '\n' */
                Assert(pos >= searchBegin && pos <= searchEnd);

                /* make segment node  */
                segment = makeNode(DistFdwFileSegment);
                segment->filename = pstrdup(fileName);
                segment->begin = segmentBegin;
                segment->end = pos + 1;
                *segmentlist = lappend(*segmentlist, segment);

                /* next sengment begin */
                segmentBegin = pos + 1;

                /* move next search area */
                searchBegin = segmentBegin;
                if (searchBegin >= fileSize) {
                    /* reach end of the file */
                    break;
                }
                searchEnd = searchBegin + BULKLOAD_FILE_SIZE_OF_DIVIDE;
            } else {
                /* can not find, move next search area */
                searchBegin = searchEnd;
                if (searchBegin >= fileSize) {
                    /* reach end of the file, add the last segment */
                    segment = makeNode(DistFdwFileSegment);
                    segment->filename = pstrdup(fileName);
                    segment->begin = segmentBegin;
                    segment->end = fileSize;
                    *segmentlist = lappend(*segmentlist, segment);
                    break;
                }
                searchEnd = searchBegin + BULKLOAD_FILE_SIZE_OF_DIVIDE;
            }

            if (searchEnd >= fileSize) {
                /* the last segment */
                segment = makeNode(DistFdwFileSegment);
                segment->filename = pstrdup(fileName);
                segment->begin = segmentBegin;
                segment->end = fileSize;
                *segmentlist = lappend(*segmentlist, segment);
                break;
            }
        } while (searchBegin < fileSize);

        pfree(buf);
        /* close file */
        (void)FreeFile(file);
    } else {
        /* whole file in one segment */
        segment = makeNode(DistFdwFileSegment);
        segment->filename = pstrdup(fileName);
        segment->begin = 0;
        segment->end = fileSize;
        *segmentlist = lappend(*segmentlist, segment);
    }

    Assert(segment != NULL);
    Assert(segment->end == fileSize);
}

/*
 * Through each directory to find the matching files, and segment it if the file large than 64MB
 */
List *getFileSegmentList(List *urllist)
{
    char *path = NULL;
    char *pattern = NULL;
    char *url = NULL;
    char *filename = NULL;
    DIR *dir = NULL;
    ListCell *lc = NULL;
    List *segmentlist = NIL;
    long filesize = 0;

    foreach (lc, urllist) {
        url = strVal(lfirst(lc));
        /* get path and pattern */
        getPathAndPattern(url, &path, &pattern);

        /* for each file */
        dir = AllocateDir(path);
        if (dir == NULL) {
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("unable to open URL \"%s\"", path)));
        }

        while ((filename = scan_dir(dir, path, pattern, &filesize)) != NULL) {
            /* segment the file */
            divideFileSegment(filename, filesize, &segmentlist);
        }

        FreeDir(dir);
    }

    return segmentlist;
}

/*
 * assign file segments to data node
 */
List *assignFileSegmentList(List *segmentlist, List *dnNames)
{
    ListCell *lc = NULL;
    List *totalTask = NIL;
    int segmentlistLen = list_length(segmentlist);
    int datanodeNums = list_length(dnNames);
    int avgTaskLen;

    /* fix codemars */
    if (datanodeNums == 0)
        return NIL;
    avgTaskLen = segmentlistLen / datanodeNums;

    /* if file segment list length less than data node number */
    avgTaskLen = avgTaskLen == 0 ? 1 : avgTaskLen;

    /* split file segment list to each data note */
    ListCell *head = list_head(segmentlist);
    int i = 0;
    int dnAssigned = 0;
    for (lc = list_head(segmentlist); lc != NULL;) {
        /* count element in avgTaskLen */
        i++;
        if (i != avgTaskLen) {
            lc = lnext(lc);
            continue;
        } else {
            /* reset counter */
            i = 0;
        }

        /* split list */
        List *dnSegmentList = makeNode(List);
        dnSegmentList->head = head;

        if (dnAssigned != datanodeNums - 1) {
            /* assign dnSegmentList in average segment number */
            dnSegmentList->tail = lc;
            dnSegmentList->length = avgTaskLen;
            segmentlistLen -= avgTaskLen;
        } else {
            /* the last data node assign remaining segments */
            dnSegmentList->tail = list_tail(segmentlist);
            dnSegmentList->length = segmentlistLen;
        }

        lc = lnext(lc);
        head = lc;
        lnext(dnSegmentList->tail) = NULL;

        /* add data node task */
        DistFdwDataNodeTask *dnTask = makeNode(DistFdwDataNodeTask);
        dnTask->dnName = (char *)list_nth(dnNames, dnAssigned);
        dnTask->task = dnSegmentList;
        totalTask = lappend(totalTask, dnTask);
        dnAssigned++;

        if (dnAssigned == datanodeNums)
            break;
    }

    return totalTask;
}

#ifndef ENABLE_LITE_MODE
/*
 * @Description: get all matched files in obs for each url
 * @IN urllist: obs url list
 * @IN encrypt: is using https for obs
 * @IN access_key: access key for obs
 * @IN secret_access_key: secret access key for obs
 * @Return:  all matched files in obs for each url
 */
List *getOBSFileList(List *urllist, bool encrypt, const char *access_key, const char *secret_access_key, bool isAnalyze)
{
    Assert(urllist);

    List *obs_file_list = NIL;
    ListCell *lc = NULL;
    char *url = NULL;

    PROFILING_OBS_START();
    pgstat_report_waitevent(WAIT_EVENT_OBS_LIST);
    foreach (lc, urllist) {
        url = strVal(lfirst(lc));

        // list files in bucket
        List *flattened_locations = NIL;

        flattened_locations = list_bucket_objects_analyze(url, encrypt, access_key, secret_access_key);
        if (flattened_locations != NIL) {
            obs_file_list = list_concat(obs_file_list, flattened_locations);
        }
    }
    pgstat_report_waitevent(WAIT_EVENT_END);
    PROFILING_OBS_END_LIST(list_length(obs_file_list));

    return obs_file_list;
}

/*
 * In OBS parallel data loading case, we may have # of datanodes not
 * equal to # of objects, as one object can only be assign to one
 * datandoes, so we create or fetch pre-created DistFdwDataNodeTask
 * handlers basing on num_processed
 */
static void assignOBSFileToDataNode(List *urllist, List **totalTask, List *dnNames)
{
    ListCell *urllc = list_head(urllist);
    int dn_num = list_length(dnNames);
    int num_processed = 0;
    int ntasks = 0;
    DistFdwDataNodeTask *task = NULL;

    // alloacte DistFdwDataNodeTask slot
    if (dn_num <= list_length(urllist)) {
        ntasks = dn_num;
    } else {
        ntasks = list_length(urllist);
    }

    for (int i = 0; i < ntasks; i++) {
        task = makeNode(DistFdwDataNodeTask);
        task->dnName = pstrdup((char *)list_nth(dnNames, (i % dn_num)));
        *totalTask = lappend(*totalTask, task);
    }

    foreach (urllc, urllist) {
        DistFdwFileSegment *segment = makeNode(DistFdwFileSegment);

        /*
         * In OBS parallel data loading case, we may have # of datanodes not
         * equal to # of objects, as one object can only be assign to one
         * datandoes, so we create or fetch pre-created DistFdwDataNodeTask
         * handlers basing on num_processed.
         *
         * Note: We strongly recommend that divide data into multiple objects(files)
         * to take advantage of parallel processing.
         */
        task = (DistFdwDataNodeTask *)list_nth(*totalTask, (num_processed % ntasks));
        task->task = lappend(task->task, segment);

        void *data = lfirst(urllc);
        if (IsA(data, SplitInfo)) {
            /* analyze dist obs foreign table. */
            SplitInfo *splitInfo = (SplitInfo *)data;
            segment->filename = pstrdup(splitInfo->filePath);
            segment->ObjectSize = splitInfo->ObjectSize;
        } else {
            /* */
            segment->filename = pstrdup(strVal(data));
        }

        num_processed++;
        ereport(DEBUG1,
                (errmodule(MOD_OBS), errmsg("Assign object %s to datanode:%s", segment->filename, task->dnName)));
    }
}

/*
 * @Description: assign task to each data node for obs
 * @IN urllist: URL location list
 * @OUT totalTask:pointer to total task list
 * @IN dnNames: list of datanodes names
 * @IN planstate: plan state
 */
static void assignOBSTaskToDataNode(List *urllist, List **totalTask, List *dnNames, DistImportPlanState *planstate,
                                    int64 *fileNum)
{
    // get obs access options
    ObsCopyOptions obs_copy_options;
    getOBSOptions(&obs_copy_options, planstate->options);

    List *obs_file_list = NIL;

    if (!planstate->writeOnly) {
        // import, get file list from obs
        obs_file_list = getOBSFileList(urllist, obs_copy_options.encrypt, obs_copy_options.access_key,
                                       obs_copy_options.secret_access_key, false);
        if (fileNum != NULL) {
            *fileNum = list_length(obs_file_list);
        }
        if (obs_file_list == NIL)
            return;
    } else {
        // export,  does not need get file list from obs, still using url list
        obs_file_list = urllist;
    }

    // assign obs file to each data node
    assignOBSFileToDataNode(obs_file_list, totalTask, dnNames);

    pfree(obs_file_list);
}
#endif

/*
 * @Description: assign task to each data node in shared mode
 * @IN urllist: URL location list
 * @IN totalTask: pointer to total task list
 * @IN dnNames: list of datanodes names
 * @Return:
 * @See also:
 */
static void assignTaskToDataNodeInSharedMode(List *urllist, List **totalTask, List *dnNames)
{
    /* generate file segment list */
    List *segmentlist = getFileSegmentList(urllist);
    if (segmentlist == NIL) {
        /* Can not find matched file */
        return;
    }

    /* assign file segment to data node */
    *totalTask = assignFileSegmentList(segmentlist, dnNames);

    /* release only the List struct not all the List element */
    pfree(segmentlist);
}

/*
 * @Description: assign task to each data node
 * @IN urllist: URL location list
 * @IN import: true if import, false if export
 * @IN totalTask: pointer to total task list
 * @IN dnNames: list of datanodes names
 * @Return:
 * @See also:
 */
static void assignTaskToDataNodeInNormalMode(List *urllist, List **totalTask, List *dnNames, int dop)
{
    /*
     * We can customize degree of parallel (DOP) for importing data
     * in normal mode. but for exporting data, we set its DOP to 1
     * only in hard codes.
     */
    if (list_length(urllist) <= list_length(dnNames)) {
        /*
         * Limitation: each datanode is assigned urls which are the same
         * Please see function 'getDataNodeTask'
         */
        ListCell *dnlc = NULL;
        Assert(IS_PGXC_COORDINATOR);
        for (int i = 0; i < dop; ++i) {
            ListCell *urllc = list_head(urllist);
            foreach (dnlc, dnNames) {
                DistFdwFileSegment *segment = makeNode(DistFdwFileSegment);
                DistFdwDataNodeTask *task = makeNode(DistFdwDataNodeTask);
                task->dnName = pstrdup((char *)lfirst(dnlc));
                task->task = lappend(task->task, segment);
                segment->filename = pstrdup(strVal(lfirst(urllc)));

                urllc = lnext(urllc);
                if (urllc == NULL)
                    urllc = list_head(urllist);

                *totalTask = lappend(*totalTask, task);
            }
        }
    } else {
        Assert(IS_PGXC_COORDINATOR);
        if (dop > 1) {
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("if the number of gds is greater than dn , "
                                                               "query dop should not be greater than one.")));
        } else {
            ereport(WARNING, (errcode(ERRCODE_WARNING),
                              errmsg("suggest that the number of GDS is not greater than the number of datanode")));
        }

        /*
         * url number > DOP * data node number,
         * every url assigned at least one data node
         */
        ListCell *urllc = NULL;
        List *segmentlist = NIL;
        /* change list */
        foreach (urllc, urllist) {
            DistFdwFileSegment *segment = makeNode(DistFdwFileSegment);
            segment->filename = pstrdup(strVal(lfirst(urllc)));
            segmentlist = lappend(segmentlist, segment);
        }
        /* assign */
        *totalTask = assignFileSegmentList(segmentlist, dnNames);
        /* release only the List struct not all the List element */
        pfree(segmentlist);
    }
}

/*
 * @Description: assign task to each data node
 * @IN import: true if import, false if export
 * @IN mode: foreign table mode option
 * @IN nodeList: datanodes list
 * @IN urllist: URL location list
 * @IN planstate: plan state
 * @Return:
 * @See also:
 */
List *assignTaskToDataNode(List *urllist, ImportMode mode, List *nodeList, int dop, DistImportPlanState *planstate,
                           int64 *fileNum = NULL)
{
    if (urllist == NIL) {
        return NIL;
    }

    /* get all data node names */
    List *dnNames = !nodeList ? PgxcNodeGetAllDataNodeNames() : PgxcNodeGetDataNodeNames(nodeList);
    List *totalTask = NIL;

    /* check it is obs source */
    const char *first_url = strVal(lfirst(list_head(urllist)));
    if (is_obs_protocol(first_url)) {
#ifndef ENABLE_LITE_MODE
        assignOBSTaskToDataNode(urllist, &totalTask, dnNames, planstate, fileNum);
        list_free(dnNames);
        return totalTask;
#else
        FEATURE_ON_LITE_MODE_NOT_SUPPORTED();
#endif
    }

    if (IS_SHARED_MODE(mode)) {
        assignTaskToDataNodeInSharedMode(urllist, &totalTask, dnNames);
    } else if (IS_NORMAL_MODE(mode)) {
        assignTaskToDataNodeInNormalMode(urllist, &totalTask, dnNames, dop);
    }

    /* release the List and ListCell struct */
    list_free(dnNames);

    return totalTask;
}

bool DoAcceptOneError(DistImportExecutionState *festate)
{
    bool do_accept = false;
    if (festate->needSaveError && (festate->rejectLimit == REJECT_UNLIMITED || festate->rejectLimit > 0)) {
        if (festate->rejectLimit > 0) {
            festate->rejectLimit--;
        }
        do_accept = true;
    }
    return do_accept;
}

bool DoAcceptOneError(CopyState cstate)
{
    bool do_accept = false;
    if ((cstate->log_errors || cstate->logErrorsData) &&
        (cstate->reject_limit == REJECT_UNLIMITED || cstate->reject_limit > 0)) {
        if (cstate->reject_limit > 0) {
            cstate->reject_limit--;
        }
        do_accept = true;
    }
    return do_accept;
}

static void DistBegin(CopyState cstate, bool isImport, Relation rel, Node *raw_query, const char *queryString,
                      List *attnamelist, List *options)
{
    TupleDesc tupDesc;
    int num_phys_attrs;
    MemoryContext oldcontext;
    List *attnums = NIL;
    ListCell *cur = NULL;

    /*
     * We allocate everything used by a cstate in a new memory context. This
     * avoids memory leaks during repeated use of COPY in a query.
     */
    cstate->copycontext = AllocSetContextCreate(CurrentMemoryContext, "COPY", ALLOCSET_DEFAULT_MINSIZE,
                                                ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    oldcontext = MemoryContextSwitchTo(cstate->copycontext);

    /* Extract options from the statement node tree */
    ProcessCopyOptions(cstate, isImport, options);

    /* Process the source/target relation or query */
    Assert(!raw_query);

    cstate->rel = rel;

    tupDesc = RelationGetDescr(cstate->rel);

    /* Don't allow COPY w/ OIDs to or from a table without them */
    if (cstate->oids && !cstate->rel->rd_rel->relhasoids)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
                        errmsg("table \"%s\" does not have OIDs", RelationGetRelationName(cstate->rel))));

    if (IS_FIXED(cstate))
        SetFixedAlignment(tupDesc, rel, (FixFormatter *)(cstate->formatter), cstate->out_fix_alignment);

    /* Generate or convert list of attributes to process */
    cstate->attnumlist = CopyGetAttnums(tupDesc, cstate->rel, attnamelist);

    num_phys_attrs = tupDesc->natts;

    /* Convert FORCE QUOTE name list to per-column flags, check validity */
    cstate->force_quote_flags = (bool *)palloc0(num_phys_attrs * sizeof(bool));
    if (cstate->force_quote_all) {
        int i;

        for (i = 0; i < num_phys_attrs; i++)
            cstate->force_quote_flags[i] = true;
    } else if (cstate->force_quote) {
        attnums = CopyGetAttnums(tupDesc, cstate->rel, cstate->force_quote);

        foreach (cur, attnums) {
            int attnum = lfirst_int(cur);
            if (!list_member_int(cstate->attnumlist, attnum))
                ereport(ERROR, (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                                errmsg("FORCE QUOTE column \"%s\" not referenced by COPY",
                                       NameStr(tupDesc->attrs[attnum - 1].attname))));
            cstate->force_quote_flags[attnum - 1] = true;
        }
    }

    /* Convert FORCE NOT NULL name list to per-column flags, check validity */
    cstate->force_notnull_flags = (bool *)palloc0(num_phys_attrs * sizeof(bool));
    if (cstate->force_notnull) {
        attnums = CopyGetAttnums(tupDesc, cstate->rel, cstate->force_notnull);

        foreach (cur, attnums) {
            int attnum = lfirst_int(cur);
            if (!list_member_int(cstate->attnumlist, attnum))
                ereport(ERROR, (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                                errmsg("FORCE NOT NULL column \"%s\" not referenced by COPY",
                                       NameStr(tupDesc->attrs[attnum - 1].attname))));
            cstate->force_notnull_flags[attnum - 1] = true;
        }
    }

    /*
     * Check fill_missing_fields conflict
     * */
    if (cstate->fill_missing_fields) {
        /* find last valid column */
        int i = num_phys_attrs - 1;
        for (; i >= 0; i--) {
            if (!tupDesc->attrs[i].attisdropped)
                break;
        }

        if (cstate->force_notnull_flags[i])
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR), errmsg("fill_missing_fields can't be set while \"%s\" is NOT NULL",
                                                           NameStr(tupDesc->attrs[i].attname))));
    }

    /* Use client encoding when ENCODING option is not specified. */
    if (cstate->file_encoding < 0)
        cstate->file_encoding = pg_get_client_encoding();

    /*
     * Set up encoding conversion info.  Even if the file and server encodings
     * are the same, we must apply pg_any_to_server() to validate data in
     * multibyte encodings.
     */
    cstate->need_transcoding = (cstate->file_encoding != GetDatabaseEncoding() ||
                                pg_database_encoding_max_length() > 1);

    /* See Multibyte encoding comment above */
    cstate->encoding_embeds_ascii = PG_ENCODING_IS_CLIENT_ONLY(cstate->file_encoding);

    cstate->copy_dest = COPY_FILE; /* default */

    (void)MemoryContextSwitchTo(oldcontext);
}

void InitDistImport(DistImportExecutionState *importstate, Relation rel, const char *filename, List *attnamelist,
                    List *options, List *totalTask)
{
    TupleDesc tupDesc;
    FormData_pg_attribute *attr = NULL;
    AttrNumber num_phys_attrs, num_defaults;
    FmgrInfo *in_functions = NULL;
    Oid *typioparams = NULL;
    bool *accept_empty_str = NULL;
    int attnum;
    Oid in_func_oid;
    int *defmap = NULL;
    ExprState **defexprs;
    MemoryContext oldcontext;
    bool volatile_defexprs = false;
    FmgrInfo *in_convert_funcs = NULL;
    int *attr_encodings = NULL;

    DistBegin((CopyState)importstate, true, rel, NULL, NULL, attnamelist, options);
    oldcontext = MemoryContextSwitchTo(importstate->copycontext);

    /* Initialize state variables */
    importstate->fe_eof = false;
    if (importstate->eol_type != EOL_UD)
        importstate->eol_type = EOL_UNKNOWN;
    importstate->cur_relname = RelationGetRelationName(importstate->rel);
    importstate->cur_lineno = 0;
    importstate->cur_attname = NULL;
    importstate->cur_attval = NULL;
    importstate->taskList = NIL;

    /* Set up variables to avoid per-attribute overhead. */
    initStringInfo(&importstate->attribute_buf);
    initStringInfo(&importstate->sequence_buf);
    initStringInfo(&importstate->line_buf);
    initStringInfo(&importstate->fieldBuf);
    importstate->line_buf_converted = false;
    importstate->raw_buf = (char *)palloc(RAW_BUF_SIZE + 1);
    importstate->raw_buf_index = importstate->raw_buf_len = 0;

    tupDesc = RelationGetDescr(importstate->rel);
    attr = tupDesc->attrs;
    num_phys_attrs = (AttrNumber)tupDesc->natts;
    num_defaults = 0;
    volatile_defexprs = false;

    /*
     * Pick up the required catalog information for each attribute in the
     * relation, including the input function, the element type (to pass to
     * the input function), and info about defaults and constraints. (Which
     * input function we use depends on text/binary format choice.)
     */
    in_functions = (FmgrInfo *)palloc(num_phys_attrs * sizeof(FmgrInfo));
    typioparams = (Oid *)palloc(num_phys_attrs * sizeof(Oid));
    accept_empty_str = (bool *)palloc(num_phys_attrs * sizeof(bool));
    defmap = (int *)palloc(num_phys_attrs * sizeof(int));
    defexprs = (ExprState **)palloc(num_phys_attrs * sizeof(ExprState *));
    attr_encodings = (int*)palloc(num_phys_attrs * sizeof(int));
    in_convert_funcs = (FmgrInfo*)palloc(num_phys_attrs * sizeof(FmgrInfo));

#ifdef PGXC
    /* We don't currently allow COPY with non-shippable ROW triggers */
    if (RelationGetLocInfo(importstate->rel) &&
        (pgxc_find_nonshippable_row_trig(importstate->rel, TRIGGER_TYPE_INSERT, TRIGGER_TYPE_BEFORE, false) ||
         pgxc_find_nonshippable_row_trig(importstate->rel, TRIGGER_TYPE_INSERT, TRIGGER_TYPE_AFTER, false))) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Non-shippable ROW triggers not supported with COPY")));
    }

    /* Output functions are required to convert default values to output form */
    importstate->out_functions = (FmgrInfo *)palloc(num_phys_attrs * sizeof(FmgrInfo));
    importstate->out_convert_funcs = (FmgrInfo*)palloc(num_phys_attrs * sizeof(FmgrInfo));
#endif

    for (attnum = 1; attnum <= num_phys_attrs; attnum++) {
        /* We don't need info for dropped attributes */
        if (attr[attnum - 1].attisdropped)
            continue;

        accept_empty_str[attnum - 1] = IsTypeAcceptEmptyStr(attr[attnum - 1].atttypid);
        /* Fetch the input function and typioparam info */
        if (IS_BINARY(importstate))
            getTypeBinaryInputInfo(attr[attnum - 1].atttypid, &in_func_oid, &typioparams[attnum - 1]);
        else
            getTypeInputInfo(attr[attnum - 1].atttypid, &in_func_oid, &typioparams[attnum - 1]);
        fmgr_info(in_func_oid, &in_functions[attnum - 1]);
        attr_encodings[attnum - 1] = get_valid_charset_by_collation(attr[attnum - 1].attcollation);
        construct_conversion_fmgr_info(
            GetDatabaseEncoding(), attr_encodings[attnum - 1], (void*)&in_convert_funcs[attnum - 1]);
        /* Get default info if needed */
        if (!list_member_int(importstate->attnumlist, attnum)) {
            /* attribute is NOT to be copied from input */
            /* use default value if one exists */
            Node *defexpr = build_column_default(importstate->rel, attnum);

            if (defexpr != NULL) {
#ifdef PGXC
                if (IS_PGXC_COORDINATOR) {
                    /*
                     * If default expr is shippable to Datanode, don't include
                     * default values in the data row sent to the Datanode; let
                     * the Datanode insert the default values.
                     */
                    Expr *planned_defexpr = expression_planner((Expr *)defexpr);
                    if (!pgxc_is_expr_shippable(planned_defexpr, NULL)) {
                        Oid out_func_oid;
                        bool isvarlena = false;
                        /* Initialize expressions in copycontext. */
                        defexprs[num_defaults] = ExecInitExpr(planned_defexpr, NULL);
                        defmap[num_defaults] = attnum - 1;
                        num_defaults++;

                        /*
                         * Initialize output functions needed to convert default
                         * values into output form before appending to data row.
                         */
                        if (IS_BINARY(importstate))
                            getTypeBinaryOutputInfo(attr[attnum - 1].atttypid, &out_func_oid, &isvarlena);
                        else
                            getTypeOutputInfo(attr[attnum - 1].atttypid, &out_func_oid, &isvarlena);
                        fmgr_info(out_func_oid, &importstate->out_functions[attnum - 1]);
                        /* set conversion functions */
                        construct_conversion_fmgr_info(attr_encodings[attnum - 1], importstate->file_encoding,
                            (void*)&importstate->out_convert_funcs[attnum - 1]);
                        if (attr_encodings[attnum - 1] != importstate->file_encoding) {
                            importstate->need_transcoding = true;
                        }
                    }
                } else {
#endif /* PGXC */
                    /* Initialize expressions in copycontext. */
                    defexprs[num_defaults] = ExecInitExpr(expression_planner((Expr *)defexpr), NULL);
                    defmap[num_defaults] = attnum - 1;
                    num_defaults++;

                    if (!volatile_defexprs)
                        volatile_defexprs = contain_volatile_functions(defexpr);
#ifdef PGXC
                }
#endif
            }
        }
    }

    /* We keep those variables in cstate. */
    importstate->in_functions = in_functions;
    importstate->typioparams = typioparams;
    importstate->accept_empty_str = accept_empty_str;
    importstate->defmap = defmap;
    importstate->defexprs = defexprs;
    importstate->volatile_defexprs = volatile_defexprs;
    importstate->num_defaults = num_defaults;
    importstate->attr_encodings = attr_encodings;
    importstate->in_convert_funcs = in_convert_funcs;

    importstate->filename = pstrdup(filename);

    /* init bulkload */
    bulkloadFuncFactory((CopyState)importstate);
    importstate->bulkLoadFunc.initBulkLoad((CopyState)importstate, filename, totalTask);

    if (!IS_BINARY(importstate)) {
        /* must rely on user to tell us... */
        importstate->file_has_oids = importstate->oids;
    } else
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("unsupport BINARY format")));

    /* create workspace for CopyReadAttributes results */
    if (!IS_BINARY(importstate)) {
        cstate_fields_buffer_init(importstate);
    }

    (void)MemoryContextSwitchTo(oldcontext);
}

void EndDistImport(DistImportExecutionState *importstate)
{
    ListCell *lc = NULL;

    foreach (lc, importstate->elogger) {
        ImportErrorLogger *elogger = (ImportErrorLogger *)lfirst(lc);
        DELETE_EX(elogger);
    }
    /* end bulkload */
    importstate->bulkLoadFunc.endBulkLoad((CopyState)importstate);
    if (importstate->errLogRel)
        relation_close(importstate->errLogRel, AccessShareLock);
    MemoryContextDelete(importstate->copycontext);
    pfree(importstate);
}

static char *distExportNextFileName(const char *abspath, const char *relname, const char *suffix);
static void getTimestampStr(void);
static const char *FetchAndCheckFormat(DefElem *defel);
static void distExportSwitchSegment(CopyState cstate, Relation rel);

#ifndef WIN32
    static const char delimiter = '/';
#else
    static const char delimiter = '\\';
#endif

static const uint64 distExportMaxSegSize = (1 << 30);

/*
 * distExportRelUpdatable
 *		Determine whether a foreign table supports INSERT, UPDATE and/or DELETE.
 */
static int distExportRelUpdatable(Relation rel)
{
    // for dist foreign talbe, only checking table's permition is enough
    ForeignTable *table = GetForeignTable(RelationGetRelid(rel));
    if (table != NULL && table->write_only)
        return (1 << CMD_INSERT);
    else
        return 0;
}

/*
 * distExportPlan
 *		Plan an INSERT operation on a foreign table
 */
static List *distExportPlan(PlannerInfo *root, ModifyTable *plan, Index resultRelation, int subplan_index)
{
    CmdType operation = plan->operation;
    List *fdwPriv = NIL;
    DistImportPlanState *planstate = NULL;
    RangeTblEntry *rte = NULL;
    char *location = NULL;

    if (!IS_STREAM_PLAN)
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("Un-support feature")));

    if (CMD_UPDATE == operation || CMD_DELETE == operation)
        return NIL;

    Assert(CMD_INSERT == operation);
    rte = planner_rt_fetch(resultRelation, root);
    planstate = (DistImportPlanState *)palloc0(sizeof(DistImportPlanState));
    GetDistImportOptions(rte->relid, planstate);
    location = strVal(lfirst(list_head(planstate->source)));
    if (!is_local_location(location)) {
        uint32 distSessionKey;
        List *tasklist = NIL;

        // generate session key
        distSessionKey = generate_unique_id(&gt_sessionId);
        fdwPriv = lappend(fdwPriv, makeDefElem(pstrdup(optSessionKey), (Node *)makeInteger((long)distSessionKey)));

        // get task list
        tasklist = assignTaskToDataNode(planstate->source, MODE_NORMAL, ((Plan *)plan)->exec_nodes->nodeList, 1,
                                        planstate);
        fdwPriv = lappend(fdwPriv, makeDefElem(pstrdup(optTaskList), (Node *)tasklist));
    }
    return fdwPriv;
}

static void distExportBegin(ModifyTableState *mtstate, ResultRelInfo *rinfo, List *fdw_private, int subplan_index,
                            int eflags)
{
    if ((uint32)eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return;

    List *options = NIL;
    char *location = NULL;
    Relation rel = rinfo->ri_RelationDesc;
    bool isRemote = false;
    uint32 sessionKey = 0;
    List *tasklist = NIL;

    distGetOptions(RelationGetRelid(rel), &location, &options);
    isRemote = !is_local_location(location);

    // Add decrpyt function for obs access key and security access key in obs options
    decryptOBSForeignTableOption(&options);

    const char *suffix = strTextFormat;
    ListCell *lc = NULL;
    foreach (lc, options) {
        DefElem *defel = (DefElem *)lfirst(lc);
        if (strcasecmp(defel->defname, optFormat) == 0)
            suffix = FetchAndCheckFormat(defel);
        else if (strcasecmp(defel->defname, optFormatter) == 0)
            UntransformFormatterOption(defel);
    }

    foreach (lc, fdw_private) {
        DefElem *def = (DefElem *)lfirst(lc);
        if (strcasecmp(def->defname, optSessionKey) == 0) {
            if (IS_PGXC_COORDINATOR) {
                sessionKey = generate_unique_id(&gt_sessionId);
                def->arg = (Node *)makeInteger((long)sessionKey);
            } else {
                sessionKey = (uint32)intVal(def->arg);
            }
            ereport(DEBUG1, (errcode(ERRCODE_DEBUG), errmsg("Session id: %u", sessionKey)));
        }
        if (strcasecmp(def->defname, optTaskList) == 0)
            tasklist = (List *)def->arg;
    }

    List *locations = DeserializeLocations(location);
    Assert(list_length(locations) >= 1);

    // coordinator doesn't create any directory or file, and has no file handle.
    if (IS_STREAM_PLAN || isRemote) {
        char *filename = isRemote ? location : strVal(lfirst(list_head(locations)));
        rinfo->ri_FdwState = (void *)beginExport(rel, filename, options, isRemote, sessionKey, tasklist);
    } else if (!isRemote) {
        const char *userExportDir = strVal(lfirst(list_head(locations)));
        if (strncmp(userExportDir, LOCAL_PREFIX, LOCAL_PREFIX_LEN) == 0) {
            // remove the prefix string 'file://'
            userExportDir = userExportDir + LOCAL_PREFIX_LEN;
        }

        InitExportEnvirnment(userExportDir);

        // outfile isn't allocated in CopyState memory context, so free it by calling pfree();
        // later, when switching to new segment file, do the same thing again;
        char *relname = get_rel_name(RelationGetRelid(rel));
        char *outfile = distExportNextFileName(t_thrd.bulk_cxt.distExportDataDir, relname, suffix);
        rinfo->ri_FdwState = (void *)beginExport(rel, outfile, options, false, sessionKey, tasklist);
        ProcessFileHeader((CopyState)rinfo->ri_FdwState);
        pfree(outfile);
    }

#ifndef ENABLE_MULTIPLE_NODES
    CopyState cstate_for_verify = (CopyState)rinfo->ri_FdwState;
    if (cstate_for_verify->remoteExport && cstate_for_verify->copy_dest == COPY_GDS) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Un-supported feature"),
                        errdetail("Gauss Data Service(GDS) are not supported in single node mode.")));
    }
#endif
}

static TupleTableSlot *distExportExec(EState *estate, ResultRelInfo *resultRelInfo, TupleTableSlot *slot,
                                      TupleTableSlot *planSlot)
{
    CopyState cstate = (CopyState)resultRelInfo->ri_FdwState;
    Assert(cstate);

    // IF condition says that, each segment file size will be bigger than 1G, more or little;
    // it doesn't matter because the upmost limit for known OS filesystem is more than 1G;
    if (exportGetTotalSize(cstate) > distExportMaxSegSize) {
        exportFlushOutBuffer(cstate);
        distExportSwitchSegment(cstate, resultRelInfo->ri_RelationDesc);
        exportResetTotalSize(cstate);
    }

    execExport(cstate, slot);
    return slot;
}

static void distExportEnd(EState *estate, ResultRelInfo *resultRelInfo)
{
    if (resultRelInfo->ri_FdwState != NULL) {
        endExport((CopyState)resultRelInfo->ri_FdwState);
    }
}

static void distExportSwitchSegment(CopyState cstate, Relation rel)
{
    List *options = NULL;
    char *location = NULL;
    distGetOptions(RelationGetRelid(rel), &location, &options);

    const char *suffix = strTextFormat;
    ListCell *lc = NULL;
    foreach (lc, options) {
        DefElem *defel = (DefElem *)lfirst(lc);
        if (strncasecmp(defel->defname, optFormat, 6) == 0) {
            suffix = FetchAndCheckFormat(defel);
            break;
        }
    }

    // switch to next segment file to copy to
    char *relname = get_rel_name(RelationGetRelid(rel));
    char *outfile = distExportNextFileName(t_thrd.bulk_cxt.distExportDataDir, relname, suffix);
    exportAllocNewFile(cstate, outfile);
    pfree(outfile);
}

// File Name: table name + txid + datanode id + segment no + suffix
static char *distExportNextFileName(const char *abspath, const char *relname, const char *suffix)
{
    // in the same export transaction, segment no is the only var for filenmae.
    // (abspath, relname, distExportCurrXid) can identify the only export action.
    // (distExportTimestampStr, segno) can avoid the conflicts of same file name.
    char temp[MAX_PATH_LEN] = {0};
    errno_t rc = EOK;
    uint32 segno = t_thrd.bulk_cxt.distExportNextSegNo++;
    Assert(t_thrd.bulk_cxt.distExportCurrXid != 0);
    rc = snprintf_s(temp, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s%s_%lu_%s_%u.%s", abspath, relname,
                    t_thrd.bulk_cxt.distExportCurrXid, t_thrd.bulk_cxt.distExportTimestampStr, segno, suffix);
    securec_check_ss(rc, "\0", "\0");

    return pstrdup(temp);
}

static const char *FetchAndCheckFormat(DefElem *defel)
{
    char *format = defGetString(defel);
    if (0 == strncasecmp(format, strCsvFormat, 3))
        return strCsvFormat;

    if (0 == strncasecmp(format, strTextFormat, 4))
        return strTextFormat;

    if (0 == strncasecmp(format, strFixedFormat, 5))
        return strFixedFormat;

    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("only both text && csv formats are supported for foreign table")));
    return strTextFormat; /* make complier mute */
}

static void getTimestampStr(void)
{
    time_t currTime;
    struct tm result;
    currTime = time(NULL);
    localtime_r(&currTime, &result);

    size_t num = strftime(t_thrd.bulk_cxt.distExportTimestampStr, 15, "%Y%m%d%H%M%S", &result);
    if (num != 14) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("invalid timestamp string length")));
    }

    t_thrd.bulk_cxt.distExportTimestampStr[num] = '\0';
}

static void CheckAndGetUserExportDir(const char *userExportDir)
{
    // user must have created data directory, otherwise error reported.
    // the filepath user defined must be an existing absolute path.
    struct stat st;
    errno_t rc = EOK;
    if (stat(userExportDir, &st) != 0 && errno != EEXIST) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("%s doesn't exist, please create it first", userExportDir)));
    } else if (!S_ISDIR(st.st_mode)) {
        ereport(ERROR, (errcode_for_file_access(),
                        errmsg("%s exists and is a file, please remove it first and create directory", userExportDir)));
    }

    // until here the exporting directory is known, copy it into distExportDataDir.
    // make sure distExportDataDir ends with directory delimiter.
    unsigned int lenOfDirName = strlen(userExportDir);
    if ((delimiter == userExportDir[lenOfDirName - 1]) && (lenOfDirName < MAX_PATH_LEN)) {
        rc = strncpy_s(t_thrd.bulk_cxt.distExportDataDir, MAX_PATH_LEN, userExportDir, lenOfDirName);
        securec_check(rc, "\0", "\0");
        t_thrd.bulk_cxt.distExportDataDir[lenOfDirName] = '\0';
    } else if ((delimiter != userExportDir[lenOfDirName - 1]) && ((lenOfDirName + 1) < MAX_PATH_LEN)) {
        rc = strncpy_s(t_thrd.bulk_cxt.distExportDataDir, MAX_PATH_LEN, userExportDir, lenOfDirName);
        securec_check(rc, "\0", "\0");
        t_thrd.bulk_cxt.distExportDataDir[lenOfDirName] = delimiter;
        t_thrd.bulk_cxt.distExportDataDir[++lenOfDirName] = '\0';
    } else
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("location filepath is too long when importing data to foreign table")));
}

static void CreateDirIfNecessary(const char *mydir)
{
    struct stat st;
    if (0 == stat(mydir, &st)) {
        if (!S_ISDIR(st.st_mode))
            ereport(ERROR, (errcode_for_file_access(),
                            errmsg("%s exists and is a file, please remove it first and create directory", mydir)));

        // mydir exists, nothing to do
        return;
    }

    // maybe other threads have created this directory, so check the errno again
    if ((0 != mkdir(t_thrd.bulk_cxt.distExportDataDir, S_IRWXU)) && (errno != EEXIST))
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not create directory \"%s\": %m", mydir)));
}

static void InitExportEnvirnment(const char *userExportDir)
{
    // reset all environments to run exporting when start a new transaction
    TransactionId curXid = GetCurrentTransactionId();
    errno_t rc = EOK;
    int ret;
    if (t_thrd.bulk_cxt.distExportCurrXid != curXid) {
        // segment number alwarys increases by 1 within the same transaction,
        // even though there are one or more export actions in this transaction.
        t_thrd.bulk_cxt.distExportNextSegNo = 0;
        t_thrd.bulk_cxt.distExportCurrXid = curXid;
        getTimestampStr();

        CheckAndGetUserExportDir(userExportDir);
        int lenOfDirName = strlen(t_thrd.bulk_cxt.distExportDataDir);

        int topDirLen = 8;
        char topDir[topDirLen + 1];
        rc = strncpy_s(topDir, topDirLen + 1, t_thrd.bulk_cxt.distExportTimestampStr, topDirLen);
        securec_check(rc, "\0", "\0");
        topDir[topDirLen] = '\0';

        int lenOfNodeName = (int)strlen(g_instance.attr.attr_common.PGXCNodeName);
        if (lenOfDirName + topDirLen + lenOfNodeName + 2 < MAX_PATH_LEN) {
            // set the 1-level directory and create it if necessary.
            // it's named by local date, excluding time info (hours, minutes or seconds).
            rc = strcat_s(t_thrd.bulk_cxt.distExportDataDir, MAX_PATH_LEN, topDir);
            securec_check(rc, "\0", "\0");
            lenOfDirName += topDirLen;
            t_thrd.bulk_cxt.distExportDataDir[lenOfDirName++] = delimiter;
            t_thrd.bulk_cxt.distExportDataDir[lenOfDirName] = '\0';
            CreateDirIfNecessary(t_thrd.bulk_cxt.distExportDataDir);

            // set the 2-level directory and create it if necessary.
            // it's named by node name.
            ret = strcat_s(t_thrd.bulk_cxt.distExportDataDir, MAX_PATH_LEN, g_instance.attr.attr_common.PGXCNodeName);
            securec_check(ret, "\0", "\0");
            lenOfDirName += lenOfNodeName;
            t_thrd.bulk_cxt.distExportDataDir[lenOfDirName++] = delimiter;
            t_thrd.bulk_cxt.distExportDataDir[lenOfDirName] = '\0';
            CreateDirIfNecessary(t_thrd.bulk_cxt.distExportDataDir);
        } else
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("location filepath is too long when importing data to foreign table")));
    }
}

static void TransformFormatterOptions(CreateForeignTableStmt *stmt)
{
    ListCell *lc = NULL;
    StringInfo str = makeStringInfo();
    bool hasFormatter = false;

    foreach (lc, stmt->base.tableElts) {
        ColumnDef *coldef = (ColumnDef *)lfirst(lc);
        if (coldef->position) {
            Position *pos = coldef->position;

            if (lc != list_head(stmt->base.tableElts))
                appendStringInfoChar(str, '.');

            appendStringInfo(str, "%s(%d,%d)", pos->colname, pos->position, pos->fixedlen);
            hasFormatter = true;
        }
    }

    if (hasFormatter)
        stmt->options = lappend(stmt->options, makeDefElem(pstrdup(optFormatter), (Node *)makeString(str->data)));
}

/* @hdfs
 * brief: Validate table definition
 * input param @obj: A Obj including infomation to validate when alter tabel and create table.
 */
static void distValidateAlterTableStmt(Node* Obj)
{
    ListCell* lc = NULL;
    AlterTableStmt* stmt = (AlterTableStmt*)Obj;

    Oid relId = RangeVarGetRelid(stmt->relation, NoLock, true);
    bool obsTbl = IS_OBS_CSV_TXT_FOREIGN_TABLE(relId);

    foreach (lc, stmt->cmds) {
        AlterTableCmd* cmd = (AlterTableCmd*)lfirst(lc);
        if (obsTbl) {
            if (!DIST_OBS_SUPPORT_AT_CMD(cmd->subtype)) {
                ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("Un-support feature"),
                                errdetail("target table is a foreign table")));
            }
        } else {
            if (!FOREIGNTABLE_SUPPORT_AT_CMD(cmd->subtype)) {
                if (!(AT_AddIndex == cmd->subtype || AT_DropConstraint == cmd->subtype)) {
                    ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("Un-support feature"),
                                    errdetail("target table is a foreign table")));
                }
            }
        }

        /* error_table, write_only can not SET, ADD or DROP by ALTER FOREIGN TABLE OPTIONS */
        if (cmd->subtype == AT_GenericOptions) {
            List* defList = (List*)cmd->def;
            ListCell* deflc = NULL;
            foreach (deflc, defList) {
                DefElem* def = (DefElem*)lfirst(deflc);
                if (strcmp(def->defname, optErrorRel) == 0 || strcmp(def->defname, optWriteOnly) == 0) {
                    ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("Invalid option %s", def->defname)));
                }
            }
        }
    }
}

static void distValidateCreateForeignTableStmt(Node* Obj)
{
    CreateForeignTableStmt* stmt = (CreateForeignTableStmt*)Obj;
    ListCell* lc = NULL;
    List* options_list = stmt->options;
    Node* errLog = stmt->error_relation;
    Oid catalog = ForeignTableRelationId;
    DistributeBy* DisByOp = ((CreateStmt*)Obj)->distributeby;

    if (NULL != DisByOp && DISTTYPE_ROUNDROBIN != DisByOp->disttype) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Unsupport distribute type."),
                        errdetail("Supported option value is \"roundrobin\".")));
    }

    foreach (lc, options_list) {
        DefElem* def = (DefElem*)lfirst(lc);
        if (!is_valid_option(def->defname, catalog)) {
            const struct DistFdwOption* opt = NULL;
            StringInfoData buf;

            /*
             * Unknown option specified, complain about it. Provide a hint
             * with list of valid options for the object.
             */
            initStringInfo(&buf);
            for (opt = loader_valid_options; opt->optname; opt++) {
                if (catalog == opt->optcontext)
                    appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "", opt->optname);
            }

            ereport(ERROR,
                    (errcode(ERRCODE_FDW_INVALID_OPTION_NAME), errmsg("invalid option \"%s\"", def->defname),
                    buf.len > 0 ? errhint("Valid options in this context are: %s", buf.data)
                    : errhint("There are no valid options in this context.")));
        }
    }

    if (errLog != NULL) {
        if (IsA(errLog, DefElem))
            stmt->extOptions = lappend(stmt->extOptions, errLog);
        else {
            RangeVar *rv = (RangeVar *)errLog;
            stmt->extOptions = lappend(stmt->extOptions, makeDefElem(pstrdup(optErrorRel),
                                                                     (Node *)makeString(pstrdup(rv->relname))));
        }
    }

    if (stmt->write_only) {
        DefElem *writeOpt = makeDefElem(pstrdup(optWriteOnly),
                                        (Node *)makeString(pstrdup(stmt->write_only ? "true" : "false")));
        stmt->extOptions = lappend(stmt->extOptions, writeOpt);
    }

    if (stmt->part_state != NULL) {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("It is not allowed to create partition on this foreign table.")));
    }

    TransformFormatterOptions(stmt);
    stmt->options = list_concat(stmt->options, stmt->extOptions);
}

static void distValidateTableDef(Node* Obj)
{
    if (Obj == NULL)
        return;

    switch (nodeTag(Obj)) {
        case T_AlterTableStmt: {
            distValidateAlterTableStmt(Obj);
            break;
        }
        case T_CreateForeignTableStmt: {
            distValidateCreateForeignTableStmt(Obj);
            break;
        }
        default:
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("unrecognized node type: %d", (int)nodeTag(Obj))));
            break;
    }
}

extern char *TrimStr(const char *str);

bool is_obs_protocol(const char *locations)
{
    bool result = false;

    Assert(locations != NULL);
    char *trimed_locations = TrimStr(locations);

    if (trimed_locations == NULL) {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("Invalid URL \"%s\" in trimed LOCATION", trimed_locations)));
    }

    if (pg_strncasecmp(trimed_locations, GSOBS_PREFIX, GSOBS_PREFIX_LEN) == 0 ||
        pg_strncasecmp(trimed_locations, OBS_PREFIX, OBS_PREfIX_LEN) == 0) {
        result = true;
    }

    pfree(trimed_locations);

    return result;
}

/*
 * @dist_fdw
 * brief: Calculate the foreign table size.
 * input param @fileName: the file names of the foreign table;
 */
int64 GetForeignTableTotalSize(List *const fileName)
{
    int64 totalSize = 0;
    ListCell *fileCell = NULL;

    Assert(fileName != NULL);

    /* Iterate the fileName list to get each file size and add them one by one. */
    foreach (fileCell, fileName) {
        int64 size = 0;
        void *data = lfirst(fileCell);
        if (IsA(data, SplitInfo)) {
            SplitInfo *fileInfo = (SplitInfo *)data;
            size = fileInfo->ObjectSize;
        } else {
            /* for txt/csv format obs foreign table. */
            DistFdwFileSegment *fileSegment = (DistFdwFileSegment *)data;
            size = fileSegment->ObjectSize;
        }

        totalSize += size < 0 ? 0 : size;
    }
    return totalSize;
}

BlockNumber getPageCountForFt(void *additionalData)
{
    BlockNumber totalPageCount = 0;

    /*
     * Get table total size. The table may have many files. We add each file size together. File list in additionalData
     * comes from CN scheduler.
     */
    List *fileList = NIL;
    if (IsA(additionalData, SplitMap)) {
        SplitMap *splitMap = (SplitMap *)additionalData;
        fileList = splitMap->splits;
    } else {
        /* for dist obs foreign table. */
        DistFdwDataNodeTask *dnTask = (DistFdwDataNodeTask *)additionalData;
        fileList = dnTask->task;
    }
    double totalSize = GetForeignTableTotalSize(fileList);

    /*
     * description: BLSCKZ value may change
     */
    totalPageCount = (uint32)(totalSize + (BLCKSZ - 1)) / BLCKSZ;
    if (totalPageCount < 1) {
        totalPageCount = 1;
    }

    return totalPageCount;
}

