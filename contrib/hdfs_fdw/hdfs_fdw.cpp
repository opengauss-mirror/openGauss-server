#include "access/dfs/dfs_query.h"
#include "access/dfs/dfs_query_reader.h"
#include "access/dfs/dfs_wrapper.h"
#include "hdfs_fdw.h"
#include "scheduler.h"
#include "access/reloptions.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/catalog.h"
#include "catalog/heap.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "commands/tablecmds.h"
#include "foreign/dummyserver.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "foreign/regioninfo.h"
#include "gaussdb_version.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/cost.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/pathnode.h"
#include "optimizer/plancat.h"
#include "optimizer/planmain.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "pgxc/pgxc.h"
#include "tcop/utility.h"
#include "utils/int8.h"
#include "utils/partitionkey.h"
#include "utils/syscache.h"
#include "parser/parse_type.h"
#include "dfs_adaptor.h"
#include "gaussdb_version.h"
#include "c.h"
/* Array of options that are valid for dfs_fdw. */
static const HdfsValidOption ValidDFSOptionArray[] = {{OPTION_NAME_LOCATION, T_FOREIGN_TABLE_OBS_OPTION},
    {OPTION_NAME_FORMAT, T_FOREIGN_TABLE_COMMON_OPTION},
    {OPTION_NAME_HEADER, T_FOREIGN_TABLE_CSV_OPTION},
    {OPTION_NAME_DELIMITER, T_FOREIGN_TABLE_TEXT_OPTION | T_FOREIGN_TABLE_CSV_OPTION},
    {OPTION_NAME_QUOTE, T_FOREIGN_TABLE_CSV_OPTION},
    {OPTION_NAME_ESCAPE, T_FOREIGN_TABLE_CSV_OPTION},
    {OPTION_NAME_NULL, T_FOREIGN_TABLE_TEXT_OPTION | T_FOREIGN_TABLE_CSV_OPTION},
    {OPTION_NAME_NOESCAPING, T_FOREIGN_TABLE_TEXT_OPTION},
    {OPTION_NAME_ENCODING, T_FOREIGN_TABLE_COMMON_OPTION},
    {OPTION_NAME_FILL_MISSING_FIELDS, T_FOREIGN_TABLE_TEXT_OPTION | T_FOREIGN_TABLE_CSV_OPTION},
    {OPTION_NAME_IGNORE_EXTRA_DATA, T_FOREIGN_TABLE_TEXT_OPTION | T_FOREIGN_TABLE_CSV_OPTION},
    {OPTION_NAME_DATE_FORMAT, T_FOREIGN_TABLE_TEXT_OPTION | T_FOREIGN_TABLE_CSV_OPTION},
    {OPTION_NAME_TIME_FORMAT, T_FOREIGN_TABLE_TEXT_OPTION | T_FOREIGN_TABLE_CSV_OPTION},
    {OPTION_NAME_TIMESTAMP_FORMAT, T_FOREIGN_TABLE_TEXT_OPTION | T_FOREIGN_TABLE_CSV_OPTION},
    {OPTION_NAME_SMALLDATETIME_FORMAT, T_FOREIGN_TABLE_TEXT_OPTION | T_FOREIGN_TABLE_CSV_OPTION},
    {OPTION_NAME_CHUNK_SIZE, T_FOREIGN_TABLE_TEXT_OPTION | T_FOREIGN_TABLE_CSV_OPTION},
    {OPTION_NAME_FILENAMES, T_FOREIGN_TABLE_HDFS_OPTION},
    {OPTION_NAME_FOLDERNAME, T_FOREIGN_TABLE_COMMON_OPTION},
    {OPTION_NAME_CHECK_ENCODING, T_FOREIGN_TABLE_COMMON_OPTION},
    {OPTION_NAME_TOTALROWS, T_FOREIGN_TABLE_OBS_OPTION},
    /* server options. */
    {OPTION_NAME_REGION, T_OBS_SERVER_OPTION},
    {OPTION_NAME_ADDRESS, T_SERVER_COMMON_OPTION},
    {OPTION_NAME_CFGPATH, T_HDFS_SERVER_OPTION},
    {OPTION_NAME_DBNAME, T_DUMMY_SERVER_OPTION},
    {OPTION_NAME_REMOTESERVERNAME, T_DUMMY_SERVER_OPTION},
    /* protocol of connecting remote server */
    {OPTION_NAME_SERVER_ENCRYPT, T_OBS_SERVER_OPTION},
    /* access key for authenticating remote OBS server */
    {OPTION_NAME_SERVER_AK, T_OBS_SERVER_OPTION},
    /* cecret access key for authenticating renmote OBS server */
    {OPTION_NAME_SERVER_SAK, T_OBS_SERVER_OPTION},
    /* user name for dummy server */
    {OPTION_NAME_USER_NAME, T_DUMMY_SERVER_OPTION},
    /* passwd for dummy server */
    {OPTION_NAME_PASSWD, T_DUMMY_SERVER_OPTION},
    {OPTION_NAME_SERVER_TYPE, T_SERVER_COMMON_OPTION}};

/* Array of options that are valid for server type. */
static const HdfsValidOption ValidServerTypeOptionArray[] = {
    {OBS_SERVER, T_SERVER_TYPE_OPTION}, {HDFS_SERVER, T_SERVER_TYPE_OPTION}, {DUMMY_SERVER, T_SERVER_TYPE_OPTION}};

#define DELIM_MAX_LEN (10)
#define INVALID_DELIM_CHAR ("\\.abcdefghijklmnopqrstuvwxyz0123456789")
#define NULL_STR_MAX_LEN (100)

#define ESTIMATION_ITEM "EstimationItem"

struct OptionsFound {
    DefElem* optionDef;
    bool found;

    OptionsFound() : optionDef(NULL), found(false)
    {}
};

/*
 * this struct keeps the related option. It is be used to check the
 * conflict options.
 */
struct TextCsvOptionsFound {
    OptionsFound header;
    OptionsFound delimiter;
    OptionsFound quotestr;
    OptionsFound escapestr;
    OptionsFound nullstr;
    OptionsFound noescaping;
    OptionsFound fillmissingfields;
    OptionsFound ignoreextradata;
    OptionsFound dateFormat;
    OptionsFound timeFormat;
    OptionsFound timestampFormat;
    OptionsFound smalldatetimeFormat;
    OptionsFound chunksize;

    TextCsvOptionsFound()
    {}
};

static const uint32 ValidDFSOptionCount = sizeof(ValidDFSOptionArray) / sizeof(ValidDFSOptionArray[0]);
static const uint32 ValidServerTypeOptionCount =
    sizeof(ValidServerTypeOptionArray) / sizeof(ValidServerTypeOptionArray[0]);

static void estimate_costs(PlannerInfo* root, RelOptInfo* baserel, HdfsFdwPlanState* fdw_private, Cost* startupCost,
    Cost* totalCost, int dop = 1);
static void estimate_size(PlannerInfo* root, RelOptInfo* baserel, HdfsFdwPlanState* fdw_private);
static void HdfsGetForeignRelSize(PlannerInfo* root, RelOptInfo* rel, Oid foreignTableId);
static void HdfsGetForeignPaths(PlannerInfo* root, RelOptInfo* rel, Oid foreignTableId);
static ForeignScan* HdfsGetForeignPlan(
    PlannerInfo* root, RelOptInfo* rel, Oid foreignTableId, ForeignPath* bestPath, List* targetList, List* scanClauses);
static void HdfsExplainForeignScan(ForeignScanState* scanState, ExplainState* explainState);
static void HdfsBeginForeignScan(ForeignScanState* scanState, int eflags);
static TupleTableSlot* HdfsIterateForeignScan(ForeignScanState* scanState);
static VectorBatch* HdfsIterateVecForeignScan(VecForeignScanState* node);
static void HdfsReScanForeignScan(ForeignScanState* scanState);
static void HdfsEndForeignScan(ForeignScanState* scanState);
static bool HdfsAnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc* acquireSampleRowsFunc,
    BlockNumber* totalPageCount, void* additionalData, bool estimate_table_rownum);
static int HdfsAcquireSampleRows(Relation relation, int logLevel, HeapTuple* sampleRows, int targetRowCount,
    double* totalRowCount, double* totalDeadCount, void* additionalData, bool estimate_table_rownum);
static void HdfsPartitionTblProcess(Node* obj, Oid relid, HDFS_PARTTBL_OPERATOR op);
static void HdfsValidateTableDef(Node* Obj);
static void HdfsBuildRuntimePredicate(ForeignScanState* node, void* value, int colIdx, HDFS_RUNTIME_PREDICATE type);
static int HdfsGetFdwType();
void checkObsPath(char* foldername, char* printName, const char* delimiter);

static Oid HdfsInsertForeignPartitionEntry(
    CreateForeignTableStmt* stmt, Oid relid, Relation pg_partition, int2vector* pkey);
static StringInfo HdfsGetOptionNames(uint32 checkOptionBit);
static void CheckCheckEncoding(DefElem* defel);
static void CheckFilenamesForPartitionFt(List* OptList);
extern bool CodeGenThreadObjectReady();
extern void checkOBSServerValidity(char* hostName, char* ak, char* sk, bool encrypt);
dfs::reader::Reader* getReader(
    Oid foreignTableId, dfs::reader::ReaderState* readerState, dfs::DFSConnector* conn, const char* format);

static bool CheckTextCsvOptionsFound(DefElem* optionDef, TextCsvOptionsFound* textOptionsFound, DFSFileType formatType);
static void CheckTextCsvOptions(
    const TextCsvOptionsFound* textOptionsFound, const char* checkEncodingLevel, DFSFileType formatType);
static bool IsHdfsFormat(List* OptList);
static DFSFileType getFormatByDefElem(DefElem* opt);
static DFSFileType getFormatByName(char* format);
static bool CheckExtOption(List* extOptList, const char* str);
static uint32 getOptionBitmapForFt(DFSFileType fileFormat, uint32 ftTypeBitmap);
static char* checkLocationOption(char* location);
static void checkSingleByteOption(const char* quoteStr, const char* optionName);
static void checkChunkSize(char* chunksizeStr);
static void checkIllegalChr(const char* optionValue, const char* optionName);

extern char* TrimStr(const char* str);

/* Declarations for dynamic loading */
PG_FUNCTION_INFO_V1(hdfs_fdw_handler);
PG_FUNCTION_INFO_V1(hdfs_fdw_validator);

/*
 * brief: foreign table callback functions.
 */
Datum hdfs_fdw_handler(PG_FUNCTION_ARGS)
{
    FdwRoutine* fdwRoutine = makeNode(FdwRoutine);

    fdwRoutine->AnalyzeForeignTable = HdfsAnalyzeForeignTable;
    fdwRoutine->AcquireSampleRows = HdfsAcquireSampleRows;
    fdwRoutine->BeginForeignScan = HdfsBeginForeignScan;
    fdwRoutine->BuildRuntimePredicate = HdfsBuildRuntimePredicate;
    fdwRoutine->EndForeignScan = HdfsEndForeignScan;
    fdwRoutine->ExplainForeignScan = HdfsExplainForeignScan;
    fdwRoutine->GetForeignPaths = HdfsGetForeignPaths;
    fdwRoutine->GetFdwType = HdfsGetFdwType;
    fdwRoutine->GetForeignPlan = HdfsGetForeignPlan;
    fdwRoutine->GetForeignRelSize = HdfsGetForeignRelSize;
    fdwRoutine->IterateForeignScan = HdfsIterateForeignScan;
    fdwRoutine->PartitionTblProcess = HdfsPartitionTblProcess;
    fdwRoutine->ReScanForeignScan = HdfsReScanForeignScan;
    fdwRoutine->VecIterateForeignScan = HdfsIterateVecForeignScan;
    fdwRoutine->ValidateTableDef = HdfsValidateTableDef;

    PG_RETURN_POINTER(fdwRoutine);
}

/**
 * @Description:
 * @in ServerOptionList: Find the server type from ServerOptionList, and then check the validity of
 * the server type. Currently, the "OBS" and the "HDFS" server type are supported for DFS server.
 * @in ServerOptionList: The server option list given by user.
 * @return Return T_OBS_SERVER if the server type is "OBS", otherwise return T_HDFS_SERVER.
 */
ServerTypeOption foundAndGetServerType(List* ServerOptionList)
{
    ServerTypeOption serverType = T_INVALID;
    ListCell* optionCell = NULL;
    bool serverTypeFound = false;

    foreach (optionCell, ServerOptionList) {
        DefElem* optionDef = (DefElem*)lfirst(optionCell);
        char* optionName = optionDef->defname;
        if (0 == pg_strcasecmp(optionName, OPTION_NAME_SERVER_TYPE)) {
            serverTypeFound = true;
            char* typeValue = defGetString(optionDef);
            if (0 == pg_strcasecmp(typeValue, OBS_SERVER)) {
                serverType = T_OBS_SERVER;
                break;
            } else if (0 == pg_strcasecmp(typeValue, HDFS_SERVER)) {
                serverType = T_HDFS_SERVER;
                break;
            } else if (0 == pg_strcasecmp(typeValue, DUMMY_SERVER)) {
                serverType = T_DUMMY_SERVER;
                break;
            } else {
                ereport(ERROR,
                    (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                        errmodule(MOD_HDFS),
                        errmsg("Invalid option \"%s\"", optionName),
                        errhint(
                            "Valid options in this context are: %s", HdfsGetOptionNames(SERVER_TYPE_OPTION)->data)));
            }
        }
    }

    if (!serverTypeFound) {
        ereport(ERROR,
            (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                errmodule(MOD_HDFS),
                errmsg("Need type option for the server."),
                errhint("Valid options in this context are: %s", HdfsGetOptionNames(SERVER_TYPE_OPTION)->data)));
    }

    return serverType;
}

/**
 * @Description: Check the option parameter validity. If the option is not included in Server option or foreign
 * table option, we must throw error.
 * @in OptionList: the checked option list.
 * @in whichOption: This parameter is used to confirm the option type.
 * @return None.
 */
void checkOptionNameValidity(List* OptionList, uint32 whichOption)
{
    const HdfsValidOption* validOption = NULL;
    uint32 count = 0;
    ListCell* optionCell = NULL;

    validOption = ValidDFSOptionArray;
    count = ValidDFSOptionCount;

    foreach (optionCell, OptionList) {
        DefElem* optionDef = (DefElem*)lfirst(optionCell);
        char* optionName = optionDef->defname;
        uint32 optionIndex = 0;
        bool optionValid = false;
        for (optionIndex = 0; optionIndex < count; optionIndex++) {
            const HdfsValidOption* option = &(validOption[optionIndex]);

            if (0 == pg_strcasecmp(option->optionName, optionName) && (option->optType & whichOption)) {
                optionValid = true;
                break;
            }
        }

        /*
         * If the given option name is considered invalid, we throw an error.
         */
        if (!optionValid) {
            ereport(ERROR,
                (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                    errmodule(MOD_HDFS),
                    errmsg("Invalid option \"%s\"", optionName),
                    errhint("Valid options in this context are: %s", HdfsGetOptionNames(whichOption)->data)));
        }
    }
}

/**
 * @Description: Check validity of foreign server option.
 * @in ServerOptionList: The given foreign server option list.
 * @return None.
 */
void ServerOptionCheckSet(List* ServerOptionList, ServerTypeOption serverType, 
                            bool& addressFound, bool& cfgPathFound, bool& akFound, bool& sakFound, bool& encrypt,
                            bool& userNameFound, bool& passWordFound, bool& regionFound, char*& hostName,
                            char*& ak, char*& sk, char*& regionStr)
{
    ListCell* optionCell = NULL;

    foreach (optionCell, ServerOptionList) {

        DefElem* optionDef = (DefElem*)lfirst(optionCell);
        char* optionName = optionDef->defname;
        if (0 == pg_strcasecmp(optionName, OPTION_NAME_ADDRESS)) {
            addressFound = true;
            if (T_HDFS_SERVER == serverType) {
                CheckGetServerIpAndPort(defGetString(optionDef), NULL, true, -1);
            } else {
                hostName = defGetString(optionDef);
            }

        } else if (0 == pg_strcasecmp(optionName, OPTION_NAME_CFGPATH)) {
            cfgPathFound = true;
            CheckFoldernameOrFilenamesOrCfgPtah(defGetString(optionDef), OPTION_NAME_CFGPATH);
        } else if (0 == pg_strcasecmp(optionName, OPTION_NAME_SERVER_ENCRYPT)) {
            encrypt = defGetBoolean(optionDef);
        } else if (0 == pg_strcasecmp(optionName, OPTION_NAME_SERVER_AK)) {
            ak = defGetString(optionDef);
            akFound = true;
        } else if (0 == pg_strcasecmp(optionName, OPTION_NAME_SERVER_SAK)) {
            sk = defGetString(optionDef);
            sakFound = true;
        } else if (0 == pg_strcasecmp(optionName, OPTION_NAME_USER_NAME)) {
            userNameFound = true;
        } else if (0 == pg_strcasecmp(optionName, OPTION_NAME_PASSWD)) {
            passWordFound = true;
        } else if (0 == pg_strcasecmp(optionName, OPTION_NAME_REGION)) {
            regionFound = true;
            regionStr = defGetString(optionDef);
        }
    }
}

void serverOptionValidator(List* ServerOptionList)
{
    bool addressFound = false;
    bool cfgPathFound = false;
    bool akFound = false;
    bool sakFound = false;
    bool encrypt = true;
    bool userNameFound = false;
    bool passWordFound = false;
    bool regionFound = false;
    char* hostName = NULL;
    char* ak = NULL;
    char* sk = NULL;
    char* regionStr = NULL;

    ServerTypeOption serverType = T_INVALID;

    /*
     * Firstly, get server type.
     */
    serverType = foundAndGetServerType(ServerOptionList);

    /*
     * Secondly, check validity of all option names.
     */
    switch (serverType) {
        case T_OBS_SERVER: {
#ifndef ENABLE_LITE_MODE
            checkOptionNameValidity(ServerOptionList, OBS_SERVER_OPTION);
            break;
#else
            FEATURE_ON_LITE_MODE_NOT_SUPPORTED();
#endif
        }
        case T_HDFS_SERVER: {
            FEATURE_NOT_PUBLIC_ERROR("HDFS is not yet supported.");
            if (is_feature_disabled(SQL_ON_HDFS) == true) {
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("HDFS Data Source is not supported.")));
            }

#ifndef ENABLE_MULTIPLE_NODES
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Un-supported feature"),
                    errdetail("HDFS data source or servers are no longer supported.")));
#endif

            checkOptionNameValidity(ServerOptionList, HDFS_SERVER_OPTION);
            break;
        }
        case T_DUMMY_SERVER: {
            checkOptionNameValidity(ServerOptionList, DUMMY_SERVER_OPTION);
            break;
        }
        default: {
            /*
             * Do not occur here.
             * If we support other server, we need to expend one case.
             */
            Assert(0);
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmodule(MOD_HDFS),
                    errmsg("Do not occur here when get server type %d.", serverType)));
        }
    }

    /*
     * Thirdly, check the validity of all option values.
     */
    ServerOptionCheckSet(ServerOptionList, serverType, addressFound, cfgPathFound, akFound, sakFound, 
                         encrypt, userNameFound, passWordFound, regionFound, hostName, ak, sk, regionStr);

#ifndef ENABLE_LITE_MODE
    if (T_OBS_SERVER == serverType) {
        if (addressFound && regionFound) {
            ereport(ERROR,
                (errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
                    errmodule(MOD_DFS),
                    errmsg("Do not allow to specify both region option and address option "
                           "for the foreign table at the same time.")));
        }

        if (!akFound) {
            ereport(ERROR,
                (errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
                    errmodule(MOD_HDFS),
                    errmsg("Need %s option for the OBS server.", OPTION_NAME_SERVER_AK)));
        }

        if (!sakFound) {
            ereport(ERROR,
                (errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
                    errmodule(MOD_HDFS),
                    errmsg("Need %s option for the OBS server.", OPTION_NAME_SERVER_SAK)));
        }

        /*
         * we will try to connect OBS server, if failed to connect, it will throw error.
         */
        if (hostName != NULL) {
            checkOBSServerValidity(hostName, ak, sk, encrypt);
        } else {
            /*
             * IF the address option has not been specified. we would using the region option if the region
             * option exist. otherwise, we will read defualt region value from the region map file.
             */
            char* URL = readDataFromJsonFile(regionStr);
            checkOBSServerValidity(URL, ak, sk, encrypt);
        }
    }
#endif

    if (T_HDFS_SERVER == serverType && !cfgPathFound) {
        ereport(ERROR,
            (errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
                errmodule(MOD_HDFS),
                errmsg("Need %s option for the HDFS server.", OPTION_NAME_CFGPATH)));
    }

    if (T_DUMMY_SERVER == serverType) {
        if (!addressFound) {
            ereport(ERROR,
                (errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
                    errmodule(MOD_OBS),
                    errmsg("Need %s option for the dummy server.", OPTION_NAME_ADDRESS)));
        }

        if (!userNameFound) {
            ereport(ERROR,
                (errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
                    errmodule(MOD_OBS),
                    errmsg("Need %s option for the dummy server.", OPTION_NAME_USER_NAME)));
        }
        if (!passWordFound) {
            ereport(ERROR,
                (errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
                    errmodule(MOD_OBS),
                    errmsg("Need %s option for the dummy server.", OPTION_NAME_PASSWD)));
        }
    }
}

/**
 * @Description: get the final optionBitmap according to judge the data format
 * and foreign table type.
 * @in fileFormat, the file format.
 * @in optBitmap, the foreign table type, OBS_FOREIGN_TABLE_OPTION or
 * HDFS_FOREIGN_TABLE_OPTION.
 * @return return the final foreign option bitmap.
 */
static uint32 getOptionBitmapForFt(DFSFileType fileFormat, uint32 ftTypeBitmap)
{
    uint32 optBitmap = ftTypeBitmap;

    switch (fileFormat) {
        case DFS_TEXT: {
            /* Delete the CSV format option bitmap. */
            optBitmap = optBitmap & (~T_FOREIGN_TABLE_CSV_OPTION);
            /*
             * If the ftTypeBitmap is the OBS_FOREIGN_TABLE_OPTION(not include
             * T_FOREIGN_TABLE_HDFS_ORC_OPTION), it is not necessary
             * to delete T_FOREIGN_TABLE_HDFS_ORC_OPTION option.
             * Only the HDFS_FOREIGN_TABLE_OPTION need to delete this option bitmap.
             */
            break;
        }
        case DFS_CSV: {
            optBitmap = optBitmap & (~T_FOREIGN_TABLE_TEXT_OPTION);
            break;
        }
        case DFS_ORC: {
            optBitmap = optBitmap & (~T_FOREIGN_TABLE_TEXT_OPTION);
            optBitmap = optBitmap & (~T_FOREIGN_TABLE_CSV_OPTION);
            break;
        }
        case DFS_PARQUET: {
            optBitmap = optBitmap & (~T_FOREIGN_TABLE_TEXT_OPTION);
            optBitmap = optBitmap & (~T_FOREIGN_TABLE_CSV_OPTION);
            break;
        }
        case DFS_CARBONDATA: {
            optBitmap = optBitmap & (~T_FOREIGN_TABLE_TEXT_OPTION);
            optBitmap = optBitmap & (~T_FOREIGN_TABLE_CSV_OPTION);
            break;
        }
        default: {
            /* never occur */
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmodule(MOD_DFS),
                    errmsg("Supported formats for the foreign table are: orc, parquet, carbondata, text, csv.")));
            Assert(0);
        }
    }

    return optBitmap;
}

/**
 * @Description: Check validity of foreign table option.
 * @in tabelOptionList: The given foreign table option list.
 * @return None.
 */
static DFSFileType CheckOptionFormat(List* tabelOptionList)
{
    ListCell* optionCell = NULL;
    bool formatFound = false;
    DFSFileType formatType = DFS_INVALID;

    foreach (optionCell, tabelOptionList) {
        DefElem* optionDef = (DefElem*)lfirst(optionCell);
        char* optionName = optionDef->defname;

        if (0 == pg_strcasecmp(optionName, OPTION_NAME_FORMAT)) {
            formatFound = true;
            char* format = defGetString(optionDef);
            formatType = getFormatByName(format);
        }
    }

    if (!formatFound) {
        ereport(ERROR,
            (errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
                errmodule(MOD_DFS),
                errmsg("Need format option for the foreign table.")));
    }

    return formatType;
}

static void CheckOptionSet(List* tabelOptionList, uint32 whichOption, DFSFileType formatType,
                        bool& filenameFound, bool& foldernameFound, bool& encodingFound, char*& checkEncodingLevel,
                        bool& totalrowsFound, bool& locationFound, bool& textOptionsFound, 
                        TextCsvOptionsFound& textOptionsFoundDetail)
{
    ListCell* optionCell = NULL;
    foreach (optionCell, tabelOptionList) {

        DefElem* optionDef = (DefElem*)lfirst(optionCell);
        char* optionName = optionDef->defname;

        if (0 == pg_strcasecmp(optionName, OPTION_NAME_FILENAMES)) {
            filenameFound = true;
            CheckFoldernameOrFilenamesOrCfgPtah(defGetString(optionDef), OPTION_NAME_FILENAMES);
        } else if (0 == pg_strcasecmp(optionName, OPTION_NAME_FOLDERNAME)) {
            foldernameFound = true;
            if (whichOption == HDFS_FOREIGN_TABLE_OPTION) {
                CheckFoldernameOrFilenamesOrCfgPtah(defGetString(optionDef), OPTION_NAME_FOLDERNAME);
            } else {
                checkObsPath(defGetString(optionDef), OPTION_NAME_FOLDERNAME, ",");
            }
        } else if (0 == pg_strcasecmp(optionName, OPTION_NAME_ENCODING)) {
            encodingFound = true;
            checkEncoding(optionDef);
        } else if (0 == pg_strcasecmp(optionName, OPTION_NAME_CHECK_ENCODING)) {
            CheckCheckEncoding(optionDef);
            checkEncodingLevel = defGetString(optionDef);
        } else if (0 == pg_strcasecmp(optionName, OPTION_NAME_TOTALROWS)) {
            totalrowsFound = true;
        } else if (0 == pg_strcasecmp(optionName, OPTION_NAME_LOCATION)) {
            strVal(optionDef->arg) = checkLocationOption(defGetString(optionDef));
            locationFound = true;
        } else if (CheckTextCsvOptionsFound(optionDef, &textOptionsFoundDetail, formatType)) {
            textOptionsFound = true;
        }
    }
}

void foreignTableOptionValidator(List* tabelOptionList, uint32 whichOption)
{
    bool filenameFound = false;
    bool foldernameFound = false;
    bool encodingFound = false;
    bool totalrowsFound = false;
    bool locationFound = false;

    DFSFileType formatType = DFS_INVALID;
    char* checkEncodingLevel = NULL;

    /* text parser options */
    TextCsvOptionsFound textOptionsFoundDetail;
    bool textOptionsFound = false;

    /* option bit for option name validity */
    uint32 name_validity_option = whichOption;

#ifndef ENABLE_MULTIPLE_NODES
    if (whichOption == HDFS_FOREIGN_TABLE_OPTION) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Un-supported feature"),
                errdetail("HDFS foreign tables are no longer supported.")));
    }
#endif

    /* check format */
    formatType = CheckOptionFormat(tabelOptionList);

    name_validity_option = getOptionBitmapForFt(formatType, whichOption);

    /*
     * Firstly, check validity of all option names.
     */
    checkOptionNameValidity(tabelOptionList, name_validity_option);

    /*
     * Secondly, check the validity of all option values.
     */
    CheckOptionSet(tabelOptionList, whichOption, formatType, filenameFound, foldernameFound, encodingFound, 
                    checkEncodingLevel, totalrowsFound, locationFound, textOptionsFound, textOptionsFoundDetail);

    if (textOptionsFound) {
        CheckTextCsvOptions(&textOptionsFoundDetail, checkEncodingLevel, formatType);
    }

    if (whichOption == HDFS_FOREIGN_TABLE_OPTION) {
        if ((filenameFound && foldernameFound) || (!filenameFound && !foldernameFound)) {
            ereport(ERROR,
                (errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
                    errmodule(MOD_HDFS),
                    errmsg("It is not allowed to specify both filenames and foldername for the foreign table, "
                           "need either filenames or foldername.")));
        }
    } else {

        if ((foldernameFound && locationFound) || (!foldernameFound && !locationFound)) {
            ereport(ERROR,
                (errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
                    errmodule(MOD_DFS),
                    errmsg("Either location option or foldername option is specified for the foreign table.")));
        }

        if (!encodingFound) {
            ereport(ERROR,
                (errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
                    errmodule(MOD_HDFS),
                    errmsg("Need %s option for the foreign table.", OPTION_NAME_ENCODING)));
        }

        if (!totalrowsFound) {
            ereport(WARNING,
                (errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
                    errmodule(MOD_HDFS),
                    errmsg("It is not specified %s option for the foreign table.", OPTION_NAME_TOTALROWS)));
        }
    }
}

/*
 * brief: Validates options given to one of the following commands:
 * foreign data wrapper, server, user mapping, or foreign table.
 * Currently, we deal with server and foreign table options.
 */
Datum hdfs_fdw_validator(PG_FUNCTION_ARGS)
{
    Datum optionArray = PG_GETARG_DATUM(0);
    Oid optionContextId = PG_GETARG_OID(1);
    List* optionList = untransformRelOptions(optionArray);

    /*
     * currently, Only the OBS/HDFS foreign, OBS/HDFS server will entrance
     * this function.
     */
    if (ForeignServerRelationId == optionContextId) {
        serverOptionValidator(optionList);
    } else if (ForeignTableRelationId == optionContextId) {
        Assert(list_length(optionList) >= 1);
        DefElem* defElem = (DefElem*)list_nth(optionList, list_length(optionList) - 1);
        char* typeValue = defGetString(defElem);
        List* tempList = list_copy(optionList);
        List* checkedList = list_delete(tempList, defElem);
        if (0 == pg_strcasecmp(typeValue, HDFS)) {
            FEATURE_NOT_PUBLIC_ERROR("HDFS is not yet supported.");
            foreignTableOptionValidator(checkedList, HDFS_FOREIGN_TABLE_OPTION);
        } else if (0 == pg_strcasecmp(typeValue, OBS)) {
            foreignTableOptionValidator(checkedList, OBS_FOREIGN_TABLE_OPTION);
        }
        list_free(checkedList);
        checkedList = NIL;
    }

    if (NIL != optionList) {
        list_free_deep(optionList);
        optionList = NIL;
    }
    /*
     * When create Foreign Data wrapper, optionContextId is ForeignDataWrapperRelationId.
     * In this case, we do nothing now.
     */

    PG_RETURN_VOID();
}

/*
 * brief: Estimate foreign table size
 * input param @root: plan information struct pointer;
 * input param @baserel: RelOptInfo struct pointer;
 * input param @fdw_private: HdfsFdwExecState information struct pointer;
 */
static void estimate_size(PlannerInfo* root, RelOptInfo* baserel, HdfsFdwPlanState* fdw_private)
{
    double rowSelectivity = 0.0;
    double outputRowCount = 0.0;
    char* filename = NULL;

    BlockNumber pageCountEstimate = (BlockNumber)baserel->pages;
    filename = fdw_private->hdfsFdwOptions->filename;

    /*Calculates tuplesCount */
    if (pageCountEstimate > 0 || (pageCountEstimate == 0 && baserel->tuples > 0)) {
        /*
         * We have number of pages and number of tuples from pg_class (from a
         * previous Analyze), so compute a tuples-per-page estimate and scale
         * that by the current file size.
         */

        fdw_private->tuplesCount = clamp_row_est(baserel->tuples);
    } else {
        /*
         * Otherwise we have to fake it. We back into this estimate using the
         * planner's idea of baserelation width, which may be inaccurate. For better
         * estimates, users need to run Analyze.
         */
        struct stat statBuffer;
        int tupleWidth = 0;

        int statResult = stat(filename, &statBuffer);
        if (statResult < 0) {
            /* file may not be there at plan time, so use a default estimate */
            statBuffer.st_size = 10 * BLCKSZ;
        }

        tupleWidth = MAXALIGN((unsigned int)baserel->width) + MAXALIGN(sizeof(HeapTupleHeaderData));
        fdw_private->tuplesCount = clamp_row_est((double)statBuffer.st_size / (double)tupleWidth);
        baserel->tuples = fdw_private->tuplesCount;
    }

    rowSelectivity = clauselist_selectivity(root, baserel->baserestrictinfo, 0, JOIN_INNER, NULL);
    outputRowCount = clamp_row_est(fdw_private->tuplesCount * rowSelectivity);
    baserel->rows = outputRowCount;
}

/*
 * brief: Estimate foreign table cost
 * input param @root: plan information struct pointer;
 * input param @baserel: RelOptInfo struct pointer;
 * input param @fdw_private: HdfsFdwExecState information struct pointer;
 * input param @startupCost: the startup cost of relation;
 * input param @totalCost: the total cost of relation;
 * input param @dop: parallel degree;
 */
static void estimate_costs(
    PlannerInfo* root, RelOptInfo* baserel, HdfsFdwPlanState* fdw_private, Cost* startupCost, Cost* totalCost, int dop)
{
    BlockNumber pageCount = 0;
    double tupleParseCost = 0.0;
    double tupleFilterCost = 0.0;
    double cpuCostPerTuple = 0.0;
    double executionCost = 0.0;
    char* filename = NULL;
    struct stat statBuffer;

    filename = fdw_private->hdfsFdwOptions->filename;

    /*Calculates the number of pages in a file.*/
    /* if file doesn't exist at plan time, use default estimate for its size */
    int statResult = stat(filename, &statBuffer);
    if (statResult < 0) {
        statBuffer.st_size = 10 * BLCKSZ;
    }

    pageCount = (statBuffer.st_size + (BLCKSZ - 1)) / BLCKSZ;
    if (pageCount < 1) {
        pageCount = 1;
    }

    /*
     * We always estimate costs as cost_seqscan(), thus assuming that I/O
     * costs are equivalent to a regular table file of the same size. However,
     * we take per-tuple CPU costs as 10x of a seqscan to account for the
     * cost of parsing records.
     */
    tupleParseCost = u_sess->attr.attr_sql.cpu_tuple_cost * HDFS_TUPLE_COST_MULTIPLIER;
    tupleFilterCost = baserel->baserestrictcost.per_tuple;
    cpuCostPerTuple = tupleParseCost + tupleFilterCost;
    executionCost = (u_sess->attr.attr_sql.seq_page_cost * pageCount) + (cpuCostPerTuple * fdw_private->tuplesCount);

    dop = SET_DOP(dop);
    *startupCost = baserel->baserestrictcost.startup;
    *totalCost = *startupCost + executionCost / dop + u_sess->opt_cxt.smp_thread_cost * (dop - 1);
}

/*
 * brief: Estimates HDFS foreign table size , which is assigned
 *        to baserel->rows for row count.
 * input param @root: plan information struct pointer;
 * input param @rel: relation information struct pointer;
 * input param @foreignTableId: Oid of the foreign table.
 */
static void HdfsGetForeignRelSize(PlannerInfo* root, RelOptInfo* rel, Oid foreignTableId)
{

    HdfsFdwPlanState* fdw_private = NULL;

    /*
     * Fetch options.  We need options at this point, but we might as
     * well get everything and not need to re-fetch it later in planning.
     */
    fdw_private = (HdfsFdwPlanState*)palloc0(sizeof(HdfsFdwExecState));

    fdw_private->hdfsFdwOptions = HdfsGetOptions(foreignTableId);
    rel->fdw_private = fdw_private;

    /* Estimate relation size */
    estimate_size(root, rel, fdw_private);
}

/*
 * brief: Generate all interesting paths for a the given HDFS relation.
 *        There is only one interesting path here.
 * input param @root: plan information struct pointer;
 * input param @rel: relation information struct pointer;
 * input param @foreignTableId: Oid of the foreign table.
 */
static void HdfsGetForeignPaths(PlannerInfo* root, RelOptInfo* rel, Oid foreignTableId)
{

    Path* foreignScanPath = NULL;
    HdfsFdwPlanState* fdw_private = NULL;
    Cost startupCost = 0.0;
    Cost totalCost = 0.0;
    fdw_private = (HdfsFdwPlanState*)rel->fdw_private;

    estimate_costs(root, rel, fdw_private, &startupCost, &totalCost);

    /* create a foreign path node and add it as the only possible path */
    foreignScanPath = (Path*)create_foreignscan_path(root,
        rel,
        startupCost,
        totalCost,
        NIL,  /* no known ordering */
        NULL, /* not parameterized */
        NIL); /* no fdw_private */

    add_path(root, rel, foreignScanPath);

    /* Add a parallel foreignscan path based on cost. */
    if (u_sess->opt_cxt.query_dop > 1) {
        estimate_costs(root, rel, fdw_private, &startupCost, &totalCost, u_sess->opt_cxt.query_dop);
        foreignScanPath = (Path*)create_foreignscan_path(
            root, rel, startupCost, totalCost, NIL, NULL, NIL, u_sess->opt_cxt.query_dop);
        if (foreignScanPath->dop > 1)
            add_path(root, rel, foreignScanPath);
    }
}

/*
 * brief: Makes a access HDFS plan in order to scann the foreign table.
 *        Query column list is also needed here to map columns later.
 * input param @root: plan information struct pointer;
 * input param @rel: relation information struct pointer;
 * input param @foreignTableId:Oid of foreign table;
 * input param @bestPath: the best path of the foreign scan;
 * input param @targetList: the column list which is required in query.
 * input param @scanClauses: clauses of the foreign scan.
 */
static ForeignScan* HdfsGetForeignPlan(
    PlannerInfo* root, RelOptInfo* rel, Oid foreignTableId, ForeignPath* bestPath, List* targetList, List* scanClauses)
{
    ForeignScan* foreignScan = NULL;
    List* columnList = NULL;
    List* restrictColumnList = NIL;
    List* foreignPrivateList = NIL;
    List* dnTask = NIL;
    List* hdfsQual = NIL;
    List* prunningResult = NIL;
    List* partList = NIL;
    List* hdfsQualColumn = NIL;
    double* selectivity = NULL;
    int columnCount = 0;
    bool isPartTabl = false;
    List* dfsReadTList = NIL;
    Relation relation = heap_open(foreignTableId, AccessShareLock);
    int64 fileNum = 0;

    char* format = HdfsGetOptionValue(foreignTableId, OPTION_NAME_FORMAT);

    if (RelationIsPartitioned(relation)) {
        isPartTabl = true;
    }
    heap_close(relation, NoLock);

    /*
     * We will put all the scanClauses into the plan node's qual list for the executor
     * to check, because we have no native ability to evaluate restriction clauses.
     */
    scanClauses = extract_actual_clauses(scanClauses, false);

    /*
     * As an optimization, we only add columns that are present in the query to the
     * column mapping hash. we get the column list, restrictColumnList
     * here and put it into foreign scan node's private list.
     */
    restrictColumnList = GetRestrictColumns(scanClauses, rel->max_attr);

    /* Build dfs reader tlist, can contain no columns */
    dfsReadTList = build_dfs_reader_tlist(targetList, NIL);

    columnList = MergeList(dfsReadTList, restrictColumnList, rel->max_attr);

    dnTask = CNScheduling(foreignTableId,
        rel->relid,
        restrictColumnList,
        scanClauses,
        prunningResult,
        partList,
        bestPath->path.locator_type,
        false,
        columnList,
        rel->max_attr,
        &fileNum);
    /*
     * We parse quals which is needed and can be pushed down to orc, parquet and carbondata
     * reader from scanClauses for each column.
     */
    if (u_sess->attr.attr_sql.enable_hdfs_predicate_pushdown &&
        (0 == pg_strcasecmp(format, DFS_FORMAT_PARQUET) || 0 == pg_strcasecmp(format, DFS_FORMAT_CARBONDATA) ||
            0 == pg_strcasecmp(format, DFS_FORMAT_CARBONDATA))) {
        columnCount = rel->max_attr;
        hdfsQualColumn = fix_pushdown_qual(&hdfsQual, &scanClauses, partList);
        selectivity = (double*)palloc0(sizeof(double) * columnCount);
        CalculateWeightByColumns(root, hdfsQualColumn, hdfsQual, selectivity, columnCount);
    }

    /*
     * wrapper DfsPrivateItem using DefElem struct.
     */
    DfsPrivateItem* item = MakeDfsPrivateItem(columnList,
        dfsReadTList,
        restrictColumnList,
        scanClauses,
        dnTask,
        hdfsQual,
        selectivity,
        columnCount,
        partList);
    foreignPrivateList = list_make1(makeDefElem(DFS_PRIVATE_ITEM, (Node*)item));

    /* create the foreign scan node */
    foreignScan = make_foreignscan(targetList,
        scanClauses,
        rel->relid,
        NIL, /* no expressions to evaluate */
        foreignPrivateList);

    foreignScan->objectNum = fileNum;

    /*
     * @hdfs
     * Judge whether using the infomational constraint on scan_qual and hdfsqual.
     */
    if (u_sess->attr.attr_sql.enable_constraint_optimization) {
        ListCell* l = NULL;
        foreignScan->scan.scan_qual_optimized = useInformationalConstraint(root, scanClauses, NULL);

        foreignScan->scan.predicate_pushdown_optimized = useInformationalConstraint(root, hdfsQual, NULL);
        /*
         * Mark the foreign scan whether has unique results on one of its output columns.
         */
        foreach (l, foreignScan->scan.plan.targetlist) {
            TargetEntry* tle = (TargetEntry*)lfirst(l);

            if (IsA(tle->expr, Var)) {
                Var* var = (Var*)tle->expr;
                RangeTblEntry* rtable = planner_rt_fetch(var->varno, root);

                if (RTE_RELATION == rtable->rtekind && findConstraintByVar(var, rtable->relid, UNIQUE_CONSTRAINT)) {
                    foreignScan->scan.plan.hasUniqueResults = true;
                    break;
                }
            }
        }
    }

    ((Plan*)foreignScan)->vec_output = true;

    if (isPartTabl) {
        ((Scan*)foreignScan)->isPartTbl = true;
        ((ForeignScan*)foreignScan)->prunningResult = prunningResult;
    } else {
        ((Scan*)foreignScan)->isPartTbl = false;
    }

    return foreignScan;
}

/*
 * brief: Builds additional details for the explain statment.
 * input param @scanState: execution state for specific hdfs foreign scan;
 * input param @explainState: explain state for specific hdfs foreign scan.
 */
static void HdfsExplainForeignScan(ForeignScanState* scanState, ExplainState* explainState)
{
    Oid foreignTableId = RelationGetRelid(scanState->ss.ss_currentRelation);
    HdfsFdwOptions* hdfsFdwOptions = HdfsGetOptions(foreignTableId);
    List* prunningResult = ((ForeignScan*)scanState->ss.ps.plan)->prunningResult;

    ServerTypeOption srvType = getServerType(foreignTableId);
    if (t_thrd.explain_cxt.explain_perf_mode == EXPLAIN_NORMAL) {
        if (T_OBS_SERVER == srvType) {
            ExplainPropertyText("Server Type", OBS, explainState);
        } else {
            ExplainPropertyText("Server Type", HDFS, explainState);
        }
    }

    const char* fileFormant = "File";
    char* format = HdfsGetOptionValue(foreignTableId, OPTION_NAME_FORMAT);
    if (format != NULL) {
        if (0 == pg_strcasecmp(format, DFS_FORMAT_ORC)) {
            fileFormant = "Orc File";
        } else if (0 == pg_strcasecmp(format, DFS_FORMAT_PARQUET)) {
            fileFormant = "Parquet File";
        } else if (0 == pg_strcasecmp(format, DFS_FORMAT_CARBONDATA)) {
            fileFormant = "Carbondata File";
        } else if (0 == pg_strcasecmp(format, DFS_FORMAT_TEXT)) {
            fileFormant = "Text File";
        } else if (0 == pg_strcasecmp(format, DFS_FORMAT_CSV)) {
            fileFormant = "Csv File";
        }
    }

    if (NULL != hdfsFdwOptions->filename)
        ExplainPropertyText(fileFormant, hdfsFdwOptions->filename, explainState);
    else if (NULL != hdfsFdwOptions->foldername)
        ExplainPropertyText(fileFormant, hdfsFdwOptions->foldername, explainState);
    else if (NULL != hdfsFdwOptions->location)
        ExplainPropertyText(fileFormant, hdfsFdwOptions->location, explainState);

    if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && explainState->planinfo->m_detailInfo) {
        if (T_OBS_SERVER == srvType) {
            explainState->planinfo->m_detailInfo->set_plan_name<true, true>();
            appendStringInfo(explainState->planinfo->m_detailInfo->info_str, "%s: %s\n", "Server Type", OBS);
        } else {
            explainState->planinfo->m_detailInfo->set_plan_name<true, true>();
            appendStringInfo(explainState->planinfo->m_detailInfo->info_str, "%s: %s\n", "Server Type", HDFS);
        }
    }

    /* Explain verbose the info of partition prunning result. */
    if (((Scan*)scanState->ss.ps.plan)->isPartTbl && NULL != prunningResult) {
        ListCell* lc = NULL;
        List* stringList = NIL;
        StringInfo detailStr = NULL;
        bool first = true;

        if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && explainState->planinfo->m_detailInfo) {
            detailStr = explainState->planinfo->m_detailInfo->info_str;
            explainState->planinfo->m_detailInfo->set_plan_name<true, true>();
            appendStringInfoSpaces(detailStr, explainState->indent * 2);
            appendStringInfo(detailStr, "%s: ", "Partition pruning");
        }

        foreach (lc, prunningResult) {
            Value* v = (Value*)lfirst(lc);
            stringList = lappend(stringList, v->val.str);
            if (detailStr) {
                if (!first)
                    appendStringInfoString(detailStr, ", ");
                appendStringInfoString(detailStr, v->val.str);
                first = false;
            }
        }

        if (detailStr) {
            appendStringInfoChar(detailStr, '\n');
        }

        (void)ExplainPropertyList("Partition pruning", stringList, explainState);
        list_free_deep(stringList);
    }

    /* show estimation details whether push down the plan to the compute pool.*/
    if (u_sess->attr.attr_sql.acceleration_with_compute_pool && u_sess->attr.attr_sql.show_acce_estimate_detail &&
        explainState->verbose &&
        (t_thrd.explain_cxt.explain_perf_mode == EXPLAIN_NORMAL ||
            t_thrd.explain_cxt.explain_perf_mode == EXPLAIN_PRETTY) &&
        T_OBS_SERVER == srvType) {
        ListCell* lc = NULL;
        Value* val = NULL;

        ForeignScan* fsscan = (ForeignScan*)scanState->ss.ps.plan;

        val = NULL;
        char* detail = NULL;
        foreach (lc, fsscan->fdw_private) {
            DefElem* def = (DefElem*)lfirst(lc);
            if (0 == pg_strcasecmp(def->defname, ESTIMATION_ITEM)) {
                val = (Value*)def->arg;
                detail = val->val.str;
                break;
            }
        }

        if (detail && t_thrd.explain_cxt.explain_perf_mode == EXPLAIN_NORMAL)
            ExplainPropertyText("Estimation Details", detail, explainState);

        if (detail && t_thrd.explain_cxt.explain_perf_mode == EXPLAIN_PRETTY && explainState->planinfo->m_detailInfo) {
            explainState->planinfo->m_detailInfo->set_plan_name<true, true>();
            appendStringInfo(explainState->planinfo->m_detailInfo->info_str, "%s: %s\n", "Estimation Details", detail);
        }
    }
}

/*
 * brief: Initialize the member in the struct HdfsFdwExecState.
 * input param @scanState: execution state for specific hdfs foreign scan;
 * input param @eflags: is a bitwise OR of flag bits described in executor.h
 */
static void HdfsBeginForeignScan(ForeignScanState* scanState, int eflags)
{
    HdfsFdwExecState* execState = NULL;
    uint32 i = 0;

    ForeignScan* foreignScan = (ForeignScan*)scanState->ss.ps.plan;
    if (NULL == foreignScan) {
        ereport(ERROR,
            (errcode(ERRCODE_FDW_INVALID_USE_OF_NULL_POINTER), errmodule(MOD_HDFS), errmsg("foreignScan is NULL")));
    }

    List* foreignPrivateList = (List*)foreignScan->fdw_private;
    if (0 == list_length(foreignPrivateList)) {
        ereport(ERROR,
            (errcode(ERRCODE_FDW_INVALID_LIST_LENGTH), errmodule(MOD_HDFS), errmsg("foreignPrivateList is NULL")));
    }

    void* options = NULL;
    if (foreignScan->rel && true == foreignScan->in_compute_pool) {
        Assert(T_OBS_SERVER == foreignScan->options->stype || T_HDFS_SERVER == foreignScan->options->stype);

        if (T_OBS_SERVER == foreignScan->options->stype)
            options = setObsSrvOptions(foreignScan->options);
        else
            options = setHdfsSrvOptions(foreignScan->options);
    }

    Oid foreignTableId = RelationGetRelid(scanState->ss.ss_currentRelation);
    DfsPrivateItem* item = (DfsPrivateItem*)((DefElem*)linitial(foreignPrivateList))->arg;

    /* Here we need to adjust the plan qual to avoid double filtering for ORC/Parquet/Carbondata. */
    char* format = NULL;
    if (options != NULL && foreignScan->rel && true == foreignScan->in_compute_pool)
        format = getFTOptionValue(foreignScan->options->fOptions, OPTION_NAME_FORMAT);
    else
        format = HdfsGetOptionValue(foreignTableId, OPTION_NAME_FORMAT);

    if (format && (0 == pg_strcasecmp(format, DFS_FORMAT_ORC) || 0 == pg_strcasecmp(format, DFS_FORMAT_PARQUET) ||
                      0 == pg_strcasecmp(format, DFS_FORMAT_CARBONDATA)))
        list_delete_list(&foreignScan->scan.plan.qual, item->hdfsQual);
    if (format != NULL) {
        if (0 == pg_strcasecmp(format, DFS_FORMAT_ORC)) {
            u_sess->contrib_cxt.file_format = DFS_ORC;
        } else if (0 == pg_strcasecmp(format, DFS_FORMAT_PARQUET)) {
            u_sess->contrib_cxt.file_format = DFS_PARQUET;
        } else if (0 == pg_strcasecmp(format, DFS_FORMAT_CSV)) {
            u_sess->contrib_cxt.file_format = DFS_CSV;
        } else if (0 == pg_strcasecmp(format, DFS_FORMAT_TEXT)) {
            u_sess->contrib_cxt.file_format = DFS_TEXT;
        } else if (0 == pg_strcasecmp(format, DFS_FORMAT_CARBONDATA)) {
            u_sess->contrib_cxt.file_format = DFS_CARBONDATA;
        } else {
            u_sess->contrib_cxt.file_format = DFS_INVALID;
        }
    } else {
        u_sess->contrib_cxt.file_format = DFS_INVALID;
    }

    /* if Explain with no Analyze or execute on CN, do nothing */
    if (IS_PGXC_COORDINATOR || ((unsigned int)eflags & EXEC_FLAG_EXPLAIN_ONLY)) {
        return;
    }

    /*
     * block the initialization of HDFS ForeignScan node in DWS DN.
     *
     * "foreignScan->rel" means the ForeignScan node will really run on
     * DN of the compute pool.
     *
     * "foreignScan->in_compute_pool" will be set to true in
     * do_query_for_planrouter() of PLANROUTER node.
     *
     * Currently, "IS_PGXC_DATANODE == true and false == foreignScan->in_compute_pool"
     * means that we are still on DWS DN, and "foreignScan->rel == true" means
     * that the ForeignScan of HDFS foreign table should NOT run on DWS DN.
     */

    dfs::reader::ReaderState* readerState = (dfs::reader::ReaderState*)palloc0(sizeof(dfs::reader::ReaderState));
    readerState->persistCtx = AllocSetContextCreate(CurrentMemoryContext,
        "dfs reader context",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    /* Build the connector to DFS. The controller will be transfered into dfs reader. */
    dfs::DFSConnector* conn = NULL;
    if (NULL == foreignScan->rel) {
        conn = dfs::createConnector(readerState->persistCtx, foreignTableId);
    } else {
        if (false == foreignScan->in_compute_pool) {
            conn = dfs::createConnector(readerState->persistCtx, foreignTableId);
        } else {
            /* we are in the comptue pool if foreignScan->rel is not NULL.
             * metadata is in foreignScan->rel, NOT in pg's catalog.
             */
            Assert(T_OBS_SERVER == foreignScan->options->stype || T_HDFS_SERVER == foreignScan->options->stype);

            if (T_OBS_SERVER == foreignScan->options->stype)
                conn = dfs::createConnector(readerState->persistCtx, T_OBS_SERVER, options);
            else
                conn = dfs::createConnector(readerState->persistCtx, T_HDFS_SERVER, options);
        }
    }

    if (NULL == conn) {
        ereport(ERROR,
            (errcode(ERRCODE_CONNECTION_DOES_NOT_EXIST), errmodule(MOD_HDFS), errmsg("can not connect to hdfs.")));
    }

    /* Assign the splits of current data node. */
    AssignSplits(item->dnTask, readerState, conn);
    if (0 == list_length(readerState->splitList)) {
        delete(conn);
        conn = NULL;
        MemoryContextDelete(readerState->persistCtx);
        pfree(readerState);
        readerState = NULL;
        return;
    }
    FillReaderState(readerState, &scanState->ss, item);

    u_sess->contrib_cxt.file_number = list_length(readerState->splitList);

    execState = (HdfsFdwExecState*)palloc0(sizeof(HdfsFdwExecState));
    execState->readerState = readerState;
    execState->fileReader = getReader(foreignTableId, readerState, conn, format);
    Assert(execState->fileReader);
    execState->readerState->fileReader = execState->fileReader;

    if (u_sess->attr.attr_sql.enable_bloom_filter && list_length(item->hdfsQual) > 0 &&
        foreignScan->not_use_bloomfilter == false) {
        for (i = 0; i < readerState->relAttrNum; i++) {
            filter::BloomFilter* blf = readerState->bloomFilters[i];
            if (NULL != blf) {
                execState->fileReader->addBloomFilter(blf, i, false);
            }
        }
    }

    INSTR_TIME_SET_CURRENT(readerState->obsScanTime);

    scanState->fdw_state = (void*)execState;
}

/*
 * brief: Obtains the next record from the Orc file. The record is tranformed to tuple,
 *        which is made into the ScanTupleSlot as a virtual tuple.
 * input param @scanState: execution state for specific hdfs foreign scan.
 */
static TupleTableSlot* HdfsIterateForeignScan(ForeignScanState* scanState)
{
    HdfsFdwExecState* execState = (HdfsFdwExecState*)scanState->fdw_state;
    // Some datanode may not get any work assigned (more datanodes than table splits), simply return NULL
    if (NULL == execState)
        return NULL;

    if (!execState->readerState->scanstate->runTimePredicatesReady) {
        BuildRunTimePredicates(execState->readerState);
        execState->fileReader->dynamicAssignSplits();
    }

    TupleTableSlot* tupleSlot = scanState->ss.ss_ScanTupleSlot;

    (void)ExecClearTuple(tupleSlot);

    execState->fileReader->nextTuple(tupleSlot);

    /*
     * @hdfs
     * Optimize foreign scan by using informational constraint.
     */
    if (((ForeignScan*)scanState->ss.ps.plan)->scan.predicate_pushdown_optimized && false == tupleSlot->tts_isempty) {
        /*
         * If we find a suitable tuple, set is_scan_end value is true.
         * It means that we do not find suitable tuple in the next iteration,
         * the iteration is over.
         */
        scanState->ss.is_scan_end = true;
    }
    return tupleSlot;
}

static VectorBatch* HdfsIterateVecForeignScan(VecForeignScanState* node)
{
    HdfsFdwExecState* execState = (HdfsFdwExecState*)node->fdw_state;
    MemoryContext context = node->m_scanCxt;
    MemoryContext oldContext = NULL;
    VectorBatch* batch = node->m_pScanBatch;

    /*
     * execState->filename is NULL or is '\0' will no longer be the condition
     * to decide if we can return immediately, execState == NULL is the only
     * condition. Because filename will move its start position when parse the file list.
     */
    if (NULL == execState) {
        node->m_done = true;
        return NULL;
    }

    if (!execState->readerState->scanstate->runTimePredicatesReady) {
        // Dynamic predicate loading
        BuildRunTimePredicates(execState->readerState);
        execState->fileReader->dynamicAssignSplits();
    }

    MemoryContextReset(context);
    oldContext = MemoryContextSwitchTo(context);
    execState->fileReader->nextBatch(batch);

    if (((ForeignScan*)(node->ss.ps.plan))->scan.predicate_pushdown_optimized && !BatchIsNull(batch)) {
        batch->m_rows = 1;
        batch->FixRowCount();
        node->ss.is_scan_end = true;
    }
    (void)MemoryContextSwitchTo(oldContext);

    return batch;
}

/*
 * brief: Rescans the foreign table.
 * input param @scanState: execution state for specific hdfs foreign scan.
 */
static void HdfsReScanForeignScan(ForeignScanState* scanState)
{
    HdfsFdwExecState* execState = (HdfsFdwExecState*)scanState->fdw_state;
    ForeignScan* foreignScan = (ForeignScan*)scanState->ss.ps.plan;
    ;
    List* foreignPrivateList = (List*)foreignScan->fdw_private;

    if (IS_PGXC_COORDINATOR) {
        return;  // return for now as the scheduling is done already.
    }

    if ((NULL == execState || NULL == execState->fileReader) || 0 == list_length(foreignPrivateList)) {
        return;
    }

    MemoryContextReset(execState->readerState->rescanCtx);
    AutoContextSwitch newContext(execState->readerState->rescanCtx);
    execState->readerState->scanstate->runTimePredicatesReady = false;

    DfsPrivateItem* item = (DfsPrivateItem*)((DefElem*)linitial(foreignPrivateList))->arg;
    AssignSplits(item->dnTask, execState->readerState, NULL);
    execState->readerState->currentSplit = NULL;
}

/*
 * brief: Ends the foreign scanning, and cleans up the acquired
 *        environmental resources.
 * input param @scanState: execution state for specific hdfs foreign scan.
 */
static void HdfsEndForeignScan(ForeignScanState* scanState)
{
#define INVALID_PERCENTAGE_THRESHOLD (0.3)
    HdfsFdwExecState* executionState = (HdfsFdwExecState*)scanState->fdw_state;

    if (NULL == executionState) {
        return;
    }

    dfs::reader::ReaderState* readerState = executionState->readerState;
    if (NULL == readerState) {
        return;
    }

    /* print one log which shows how many incompatible rows have been ignored */
    if (readerState->incompatibleCount > 0) {
        double ratio = (double)readerState->incompatibleCount / readerState->dealWithCount;
        ereport(LOG,
            (errmodule(MOD_HDFS),
                errmsg("%d(%.2f%%) elements are incompatible and replaced by \"?\" automatically for foreign table %s.",
                    readerState->incompatibleCount,
                    ratio * 100,
                    RelationGetRelationName(readerState->scanstate->ss_currentRelation))));
        if (ratio > INVALID_PERCENTAGE_THRESHOLD) {
            ereport(WARNING,
                (errmodule(MOD_HDFS),
                    errmsg("More than 30%% elements are incompatible and replaced by \"?\" "
                           "automatically for foreign table %s, please check the encoding setting"
                           " or clean the original data.",
                        RelationGetRelationName(readerState->scanstate->ss_currentRelation))));
        }
    }

#ifdef ENABLE_LLVM_COMPILE
    /*
     * LLVM optimization information should be shown. We check the query
     * uses LLVM optimization or not.
     */
    if (scanState->ss.ps.instrument && u_sess->attr.attr_sql.enable_codegen && CodeGenThreadObjectReady() &&
        IsA(scanState, VecForeignScanState)) {
        VecForeignScanState* node = (VecForeignScanState*)scanState;
        List** hdfsScanPredicateArr = readerState->hdfsScanPredicateArr;

        /*
         * If hdfsScanPredicateArr is NIL, Just now, LLVM optimization is not used,
         * it is not necessary to check hdfs pridicate.
         */
        if (NULL != hdfsScanPredicateArr) {
            List* partList = readerState->partList;
            uint32* colNoMap = readerState->colNoMapArr;

            VectorBatch* batch = node->m_pScanBatch;

            for (uint32 i = 0; i < readerState->relAttrNum; i++) {
                uint32 orcIdx = 0;
                ListCell* cell = NULL;

                if (NULL == partList || !list_member_int(partList, i + 1)) {
                    if (NULL == colNoMap) {
                        orcIdx = i;
                    } else {
                        orcIdx = colNoMap[i] - 1;
                    }

                    foreach (cell, hdfsScanPredicateArr[orcIdx]) {
                        ScalarVector arr;

                        arr = batch->m_arr[i];
                        switch (arr.m_desc.typeId) {
                            case INT2OID:
                            case INT4OID:
                            case INT8OID: {
                                HdfsScanPredicate<Int64Wrapper, int64>* predicate =
                                    (HdfsScanPredicate<Int64Wrapper, int64>*)lfirst(cell);
                                if (NULL != predicate->m_jittedFunc) {
                                    node->ss.ps.instrument->isLlvmOpt = true;
                                }
                                break;
                            }
                            case FLOAT4OID:
                            case FLOAT8OID: {
                                HdfsScanPredicate<Float8Wrapper, float8>* predicate =
                                    (HdfsScanPredicate<Float8Wrapper, float8>*)lfirst(cell);
                                if (NULL != predicate->m_jittedFunc) {
                                    node->ss.ps.instrument->isLlvmOpt = true;
                                }
                                break;
                            }
                            default: {
                                /* do nothing here*/
                                break;
                            }
                        }

                        if (true == node->ss.ps.instrument->isLlvmOpt) {
                            break;
                        }
                    }
                }
            }
        }
    }
#endif

    /* clears all file related memory */
    if (NULL != executionState->fileReader) {
        RemoveDfsReadHandler(executionState->fileReader);
        executionState->fileReader->end();
        DELETE_EX(executionState->fileReader);
    }

    Instrumentation* instr = scanState->ss.ps.instrument;
    if (NULL != instr) {
        instr->dfsType = readerState->dfsType;
        instr->dynamicPrunFiles = readerState->dynamicPrunFiles;
        instr->staticPruneFiles = readerState->staticPruneFiles;
        instr->bloomFilterRows = readerState->bloomFilterRows;
        instr->minmaxFilterRows = readerState->minmaxFilterRows;
        instr->bloomFilterBlocks = readerState->bloomFilterBlocks;
        instr->minmaxCheckFiles = readerState->minmaxCheckFiles;
        instr->minmaxFilterFiles = readerState->minmaxFilterFiles;
        instr->minmaxCheckStripe = readerState->minmaxCheckStripe;
        instr->minmaxFilterStripe = readerState->minmaxFilterStripe;
        instr->minmaxCheckStride = readerState->minmaxCheckStride;
        instr->minmaxFilterStride = readerState->minmaxFilterStride;

        instr->orcMetaCacheBlockCount = readerState->orcMetaCacheBlockCount;
        instr->orcMetaCacheBlockSize = readerState->orcMetaCacheBlockSize;
        instr->orcMetaLoadBlockCount = readerState->orcMetaLoadBlockCount;
        instr->orcMetaLoadBlockSize = readerState->orcMetaLoadBlockSize;
        instr->orcDataCacheBlockCount = readerState->orcDataCacheBlockCount;
        instr->orcDataCacheBlockSize = readerState->orcDataCacheBlockSize;
        instr->orcDataLoadBlockCount = readerState->orcDataLoadBlockCount;
        instr->orcDataLoadBlockSize = readerState->orcDataLoadBlockSize;

        instr->localBlock = readerState->localBlock;
        instr->remoteBlock = readerState->remoteBlock;

        instr->nnCalls = readerState->nnCalls;
        instr->dnCalls = readerState->dnCalls;
    }

    ForeignScan* foreignScan = (ForeignScan*)scanState->ss.ps.plan;

    if (u_sess->instr_cxt.obs_instr && IS_PGXC_DATANODE && foreignScan->in_compute_pool &&
        T_OBS_SERVER == foreignScan->options->stype) {
        Relation rel = scanState->ss.ss_currentRelation;

        int fnumber;
        if (DFS_ORC == u_sess->contrib_cxt.file_format || DFS_PARQUET == u_sess->contrib_cxt.file_format ||
            DFS_CARBONDATA == u_sess->contrib_cxt.file_format) {
            fnumber = readerState->minmaxCheckFiles;
        } else if (DFS_TEXT == u_sess->contrib_cxt.file_format || DFS_CSV == u_sess->contrib_cxt.file_format) {
            fnumber = u_sess->contrib_cxt.file_number;
        } else
            fnumber = 0;

        u_sess->instr_cxt.obs_instr->save(RelationGetRelationName(rel),
            fnumber,
            readerState->orcDataLoadBlockSize,
            elapsed_time(&(readerState->obsScanTime)),
            u_sess->contrib_cxt.file_format);
    }

    MemoryContextDelete(readerState->persistCtx);
    MemoryContextDelete(readerState->rescanCtx);
    pfree(readerState);
    readerState = NULL;
    pfree(executionState);
    executionState = NULL;
}

/*
 * brief: Sets the all page counts and the function pointer used to get a
 * random sample of rows from the HDFS data files. We use parameter func to pass data.
 * input param @relation: relation information struct;
 * input param @func: is used as a input parameter to transfer work flow information;
 * input param @totalPageCount: total page count of the relation;
 * input param @additionalData: file list from CN scheduler.
 * Notice: The parameter func is not used. we get AcquireSampleRowsFunc by AcquireSampleRows FDWHander.
 */
static bool HdfsAnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc* func, BlockNumber* totalPageCount,
    void* additionalData, bool estimate_table_rownum)
{
    if (NULL == additionalData)
        return true;

    (*totalPageCount) = getPageCountForFt(additionalData);

    return true;
}

/*
 * @hdfs
 * Brief: Get a random sample of rows from the HDFS foreign table. Checked rows
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
int HdfsAcquireSampleRows(Relation relation, int logLevel, HeapTuple* sampleRows, int targetRowCount,
    double* totalRowCount, double* deadRows, void* additionalData, bool estimate_table_rownum)
{
    /* We report "analyze" nothing if additionalData is null.  */
    if (NULL == additionalData) {
        ereport(logLevel,
            (errmodule(MOD_HDFS),
                errmsg(
                    "\"%s\": scanned %.0f tuples and sampled %d tuples.", RelationGetRelationName(relation), 0.0, 0)));
        return 0;
    }
    int sampleRowCount = 0;
    double rowCount = 0.0;
    double rowCountToSkip = -1; /* -1 means not set yet */
    double selectionState = 0;
    MemoryContext oldContext = NULL;
    MemoryContext tupleContext = NULL;
    List* columnList = NIL;
    List* opExpressionList = NIL;
    List* fileList = NIL;
    ForeignScanState* scanState = NULL;
    int eflags = 0;
    BlockSamplerData bs;
    SplitMap* fileSplitMap = NULL;
    SplitInfo* fileInfo = NULL;
    ForeignScan* foreignScan = NULL;
    TupleDesc tupleDescriptor = RelationGetDescr(relation);
    int columnCount = tupleDescriptor->natts;
    double liveRows = 0; /* tuple count we scanned */

    /* get filename list */
    SplitMap* fileMap = (SplitMap*)additionalData;
    fileList = fileMap->splits;

    columnList = CreateColList((Form_pg_attribute*)tupleDescriptor->attrs, columnCount);
    unsigned int totalFileNumbers = list_length(fileList);

    /* description: change file -> stride: Jason's advice */
    /* Init blocksampler, this function will help us decide which file to be sampled. */
    BlockSampler_Init(&bs, totalFileNumbers, targetRowCount);

    while (BlockSampler_HasMore(&bs)) /* If still have file to be sampled, we will be into while loop */
    {
        TupleTableSlot* scanTupleSlot = NULL;
        List* HDFSNodeWorkList = NIL;
        List* foreignPrivateList = NIL;
        List* fileWorkList = NIL;

        /* Get File No. */
        unsigned int targFile = BlockSampler_Next(&bs);

        Datum* columnValues = (Datum*)palloc0(columnCount * sizeof(Datum));
        bool* columnNulls = (bool*)palloc0(columnCount * sizeof(bool));

        /*
         * create list of columns of the relation
         * columnList changed in HdfsBeginForeignScan
         */
        list_free(columnList);
        columnList = CreateColList((Form_pg_attribute*)tupleDescriptor->attrs, columnCount);

        /* Put file information into SplitInfo struct */
        SplitInfo* splitinfo = (SplitInfo*)list_nth(fileList, targFile);

        fileInfo = makeNode(SplitInfo);
        fileInfo->filePath = splitinfo->filePath;
        fileInfo->partContentList = splitinfo->partContentList;
        fileInfo->ObjectSize = splitinfo->ObjectSize;
        fileInfo->eTag = splitinfo->eTag;
        fileInfo->prefixSlashNum = splitinfo->prefixSlashNum;

        /* Put file information into SplitMap struct */
        fileSplitMap = makeNode(SplitMap);
        fileWorkList = lappend(fileWorkList, (void*)fileInfo);
        fileSplitMap->nodeId = u_sess->pgxc_cxt.PGXCNodeId;
        fileSplitMap->locatorType = LOCATOR_TYPE_NONE;
        fileSplitMap->splits = fileWorkList;

        HDFSNodeWorkList = lappend(HDFSNodeWorkList, fileSplitMap);

        DfsPrivateItem* item = MakeDfsPrivateItem(columnList,
            columnList,
            NIL,
            opExpressionList,
            HDFSNodeWorkList,
            NIL,
            NULL,
            0,
            GetPartitionList(RelationGetRelid(relation)));
        foreignPrivateList = list_make1(makeDefElem("DfsPrivateItem", (Node*)item));

        /* setup foreign scan plan node */
        foreignScan = makeNode(ForeignScan);
        foreignScan->fdw_private = foreignPrivateList;

        /* Judge relation is partition table or not */
        Scan* tmpScan = (Scan*)foreignScan;
        if (NIL != fileInfo->partContentList)
            tmpScan->isPartTbl = true;

        /* setup tuple slot */
        scanTupleSlot = MakeTupleTableSlot();
        scanTupleSlot->tts_tupleDescriptor = tupleDescriptor;
        scanTupleSlot->tts_values = columnValues;
        scanTupleSlot->tts_isnull = columnNulls;

        /* setup scan state */
        scanState = makeNode(ForeignScanState);
        scanState->ss.ss_currentRelation = relation;
        scanState->ss.ps.plan = (Plan*)foreignScan;
        scanState->ss.ss_ScanTupleSlot = scanTupleSlot;

        HdfsBeginForeignScan(scanState, eflags);

        /*
         * Use per-tuple memory context to prevent leak of memory used to read and
         * parse rows from the file using ReadLineFromFile and FillTupleSlot.
         */
        tupleContext = AllocSetContextCreate(CurrentMemoryContext,
            "hdfs_fdw temporary context",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE);

        /* prepare for sampling rows */
        selectionState = anl_init_selection_state(targetRowCount);

        for (;;) {
            /* check for user-requested abort or sleep */
            vacuum_delay_point();

            /* clean columnValues and columnNulls */
            errno_t rc = memset_s(columnValues, columnCount * sizeof(Datum), 0, columnCount * sizeof(Datum));
            securec_check(rc, "\0", "\0");
            rc = memset_s(columnNulls, columnCount * sizeof(bool), true, columnCount * sizeof(bool));
            securec_check(rc, "\0", "\0");

            /* read the next record */
            /* description: change to batch read */
            MemoryContextReset(tupleContext);
            oldContext = MemoryContextSwitchTo(tupleContext);
            (void)HdfsIterateForeignScan(scanState);
            (void)MemoryContextSwitchTo(oldContext);

            /* if there are no more records to read, break */
            if (scanTupleSlot->tts_isempty) {
                break;
            }

            ++liveRows;

            /*
             * The first targetRowCount sample rows are simply copied into the
             * reservoir. Then we start replacing tuples in the sample until we
             * reach the end of the relation.
             */
            if (sampleRowCount < targetRowCount) {
                sampleRows[sampleRowCount++] = heap_form_tuple(tupleDescriptor, columnValues, columnNulls);
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
                    sampleRows[rowIndex] = heap_form_tuple(tupleDescriptor, columnValues, columnNulls);
                }
                rowCountToSkip -= 1;
            }
            rowCount += 1;
        }

        /* clean up */
        MemoryContextDelete(tupleContext);
        pfree_ext(columnValues);
        pfree_ext(columnNulls);
        list_free(HDFSNodeWorkList);
        list_free(foreignPrivateList);
        HDFSNodeWorkList = NIL;
        foreignPrivateList = NIL;
        HdfsEndForeignScan(scanState);

        if (estimate_table_rownum == true)
            break;
    }

    if (estimate_table_rownum == true) {

        (*totalRowCount) = totalFileNumbers * liveRows;
        return sampleRowCount;
    }

    /* free list */
    list_free(fileList);
    list_free(columnList);
    fileList = NIL;
    columnList = NIL;

    /* emit some interesting relation info */
    ereport(logLevel,
        (errmodule(MOD_HDFS),
            errmsg("\"%s\": scanned %.0f tuples and sampled %d tuples",
                RelationGetRelationName(relation),
                rowCount,
                sampleRowCount)));

    /* @hdfs estimate totalRowCount */
    if (bs.m > 0) {
        *totalRowCount = floor((liveRows / bs.m) * totalFileNumbers + 0.5);
    } else {
        *totalRowCount = 0.0;
    }

    (*deadRows) = 0; /* @hdfs dead rows is no means to foreign table */
    return sampleRowCount;
}

/*
 * brief: The handler to process the situation when create a new partition hdfs foreign table or drop one.
 * input param @obj: an object including infomation to validate when create table;
 * input param @relid: Oid of the relation;
 * input param @op: hdfs partition table operator.
 */
static void HdfsPartitionTblProcess(Node* obj, Oid relid, HDFS_PARTTBL_OPERATOR op)
{
    HeapTuple partTuple = NULL;
    HeapTuple classTuple = NULL;
    Form_pg_class pgClass = NULL;
    Relation partitionRel = NULL;
    Relation classRel = NULL;
    List* partKeyList = NIL;
    TupleDesc desciption = NULL;
    int2vector* pkey = NULL;

    /* When the obj is null, process the partition table's dropping: delete the related tuple in pg_partition. */
    if (HDFS_DROP_PARTITIONED_FOREIGNTBL == op) {
        classRel = relation_open(relid, AccessShareLock);
        if (PARTTYPE_PARTITIONED_RELATION == classRel->rd_rel->parttype) {
            partTuple = searchPgPartitionByParentIdCopy(PART_OBJ_TYPE_PARTED_TABLE, relid);
            if (NULL != partTuple) {
                partitionRel = heap_open(PartitionRelationId, RowExclusiveLock);
                simple_heap_delete(partitionRel, &partTuple->t_self);

                heap_freetuple(partTuple);
                partTuple = NULL;
                heap_close(partitionRel, RowExclusiveLock);
            }
        }
        relation_close(classRel, AccessShareLock);
    }
    /* When the node tag of obj is T_CreateForeignTableStmt, we modify the parttype in pg_class and insert a new tuple
       in pg_partition. */
    else if (HDFS_CREATE_PARTITIONED_FOREIGNTBL == op) {
        CreateForeignTableStmt* stmt = (CreateForeignTableStmt*)obj;
        List* pos = NIL;
        if (NULL != stmt->part_state) {
            /* update the parttype info in the pg_class */
            partKeyList = stmt->part_state->partitionKey;

            classRel = heap_open(RelationRelationId, RowExclusiveLock);
            classTuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
            if (!HeapTupleIsValid(classTuple))
                ereport(ERROR,
                    (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmodule(MOD_HDFS),
                        errmsg("cache lookup failed for relid %u", relid)));
            pgClass = (Form_pg_class)GETSTRUCT(classTuple);
            pgClass->parttype = 'p';
            heap_inplace_update(classRel, classTuple);
            heap_freetuple(classTuple);
            classTuple = NULL;
            heap_close(classRel, RowExclusiveLock);

            /* Insert into pg_partition a new tuple with the info of partition columnlist. */
            partitionRel = heap_open(PartitionRelationId, RowExclusiveLock);
            desciption = BuildDescForRelation(stmt->base.tableElts, (Node*)makeString(ORIENTATION_ROW));

            /* get partitionkey's position */
            pos = GetPartitionkeyPos(partKeyList, stmt->base.tableElts);

            /* check partitionkey's datatype */
            CheckValuePartitionKeyType(desciption->attrs, pos);

            list_free(pos);
            pos = NIL;

            pkey = buildPartitionKey(partKeyList, desciption);
            (void)HdfsInsertForeignPartitionEntry(stmt, relid, partitionRel, pkey);
            heap_close(partitionRel, RowExclusiveLock);
        }
    }
}

/*@hdfs
 *brief: Validate hdfs foreign table definition
 *input param @obj: An Obj including infomation to validate when alter table and create table.
 */
static void HdfsValidateTableDef(Node* Obj)
{
    DFSFileType format_type = DFS_INVALID;

    if (NULL == Obj)
        return;

    switch (nodeTag(Obj)) {
        case T_AlterTableStmt: {
            AlterTableStmt* stmt = (AlterTableStmt*)Obj;
            List* cmds = stmt->cmds;
            ListCell* lcmd = NULL;
            Oid relid = InvalidOid;
            LOCKMODE lockmode_getrelid = AccessShareLock;

            foreach (lcmd, cmds) {
                AlterTableCmd* cmd = (AlterTableCmd*)lfirst(lcmd);

                if (AT_AddColumn == cmd->subtype || AT_AlterColumnType == cmd->subtype) {
                    ColumnDef* colDef = (ColumnDef*)cmd->def;
                    if (stmt->relation) {
                        relid = RangeVarGetRelid(stmt->relation, lockmode_getrelid, true);
                        if (OidIsValid(relid)) {
                            char* format = HdfsGetOptionValue(relid, OPTION_NAME_FORMAT);
                            format_type = getFormatByName(format);
                        }
                    }
                    if (AT_AddColumn == cmd->subtype) {
                        DFSCheckDataType(colDef->typname, colDef->colname, format_type);
                    } else {
                        DFSCheckDataType(colDef->typname, cmd->name, format_type);
                    }
                }

                /* @hdfs
                 * currently, alter foreign table syntax support alter type is following type:
                 * AT_AddColumn, AT_DropNotNull, AT_SetNotNull, AT_DropColumn, AT_AlterColumnType,
                 * AT_AlterColumnGenericOptions, AT_GenericOptions,AT_ChangeOwner.
                 * So, Check altering type for foreign table, if cmd->subtype is not above types for
                 * foreign table, error msg is performed
                 */
                if (cmd->subtype != AT_DropNotNull && cmd->subtype != AT_DropConstraint &&
                    cmd->subtype != AT_AddIndex && cmd->subtype != AT_SetNotNull &&
                    cmd->subtype != AT_AlterColumnType && cmd->subtype != AT_AlterColumnGenericOptions &&
                    cmd->subtype != AT_GenericOptions && cmd->subtype != AT_ChangeOwner &&
                    cmd->subtype != AT_AddNodeList && cmd->subtype != AT_DeleteNodeList &&
                    cmd->subtype != AT_SubCluster && cmd->subtype != AT_SetStatistics) {
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmodule(MOD_HDFS),
                            errmsg("Un-support feature"),
                            errdetail("target table is a foreign table")));
                }
            }
            break;
        }
        case T_CreateForeignTableStmt: {
            DistributeBy* DisByOp = ((CreateStmt*)Obj)->distributeby;

            /* Start a node by isRestoreMode when adding a node, do not regist distributeby
             * info for pgxc_class. So distributeby clause is not needed.
             */
            if (!isRestoreMode) {
                if (NULL == DisByOp) {
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmodule(MOD_HDFS),
                            errmsg("Need DISTRIBUTE BY clause for the foreign table.")));
                }
                if (isSpecifiedSrvTypeFromSrvName(((CreateForeignTableStmt*)Obj)->servername, HDFS)) {
                    if (DISTTYPE_ROUNDROBIN != DisByOp->disttype && DISTTYPE_REPLICATION != DisByOp->disttype) {
                        ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmodule(MOD_HDFS),
                                errmsg("Unsupport distribute type."),
                                errdetail("Supported option values are \"roundrobin\" and \"replication\".")));
                    }
                } else {
                    if (DISTTYPE_REPLICATION == DisByOp->disttype || DISTTYPE_HASH == DisByOp->disttype) {
                        ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("Only support ROUNDROBIN distribution type for this foreign table.")));
                    }

                    if (((CreateForeignTableStmt*)Obj)->part_state) {
                        /* as for obs foreign table, unsupport parition table for text, csv, carbondata format. */
                        List* options = ((CreateForeignTableStmt*)Obj)->options;
                        char* fileType = getFTOptionValue(options, OPTION_NAME_FORMAT);
                        /* if the fileType is NULL, the foreignTableOptionValidator will throw error.*/
                        if (NULL != fileType && (0 == pg_strcasecmp(fileType, DFS_FORMAT_TEXT) ||
                                                    0 == pg_strcasecmp(fileType, DFS_FORMAT_CSV) ||
                                                    0 == pg_strcasecmp(fileType, DFS_FORMAT_CARBONDATA))) {
                            ereport(ERROR,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmsg("The obs partition foreign table is not supported on text, csv, carbondata "
                                           "format.")));
                        }
                    }
                }
            }

            /*if the filenames option exists for a HDFS partition foreign table, the error will be meet */
            if (((CreateForeignTableStmt*)Obj)->part_state) {
                CheckFilenamesForPartitionFt(((CreateForeignTableStmt*)Obj)->options);
            }

            /*if the error_relation exists for a HDFS foreign table ,the warning reminds that it is not support*/
            if (((CreateForeignTableStmt*)Obj)->error_relation != NULL &&
                IsA(((CreateForeignTableStmt*)Obj)->error_relation, RangeVar)) {
                ereport(WARNING, (errmsg("The error_relation of the foreign table is not support.")));
            }

            /*Check data type */
            if (IsHdfsFormat(((CreateForeignTableStmt*)Obj)->options)) {
                ListCell* cell = NULL;
                List* tableElts = ((CreateStmt*)Obj)->tableElts;
                List* OptList = ((CreateForeignTableStmt*)Obj)->options;
                DefElem* Opt = NULL;

                foreach (cell, OptList) {
                    Opt = (DefElem*)lfirst(cell);
                    format_type = getFormatByDefElem(Opt);
                    if (DFS_INVALID != format_type) {
                        break;
                    }
                }

                foreach (cell, tableElts) {
                    TypeName* typName = NULL;
                    ColumnDef* colDef = (ColumnDef*)lfirst(cell);

                    if (NULL == colDef || NULL == colDef->typname) {
                        break;
                    }

                    typName = colDef->typname;
                    DFSCheckDataType(typName, colDef->colname, format_type);
                }
            }

            /* check write only */
            if (((CreateForeignTableStmt*)Obj)->write_only) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_HDFS), errmsg("Unsupport write only.")));
            }

            /* check optLogRemote and optRejectLimit */
            if (CheckExtOption(((CreateForeignTableStmt*)Obj)->extOptions, optLogRemote)) {
                ereport(WARNING, (errmsg("The REMOTE LOG of hdfs foreign table is not support.")));
            }

            if (CheckExtOption(((CreateForeignTableStmt*)Obj)->extOptions, optRejectLimit)) {
                ereport(WARNING, (errmsg("The PER NODE REJECT LIMIT of hdfs foreign table is not support.")));
            }

            break;
        }
        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmodule(MOD_HDFS),
                    errmsg("unrecognized node type: %d", (int)nodeTag(Obj))));
    }
}

/*
 * brief: The api to push down the run time predicate to hdfs foreign scan.
 * input param @node: Foreign scan state to update with the run time predicate.
 * input param @value: The run time predicate structure point.
 * input param @colIdx: The index of the column on which the predicate effects.
 * input param @type: The type of the run time predicate.
 */
static void HdfsBuildRuntimePredicate(ForeignScanState* node, void* value, int colIdx, HDFS_RUNTIME_PREDICATE type)
{
    HdfsFdwExecState* execState = (HdfsFdwExecState*)node->fdw_state;
    if (HDFS_BLOOM_FILTER == type) {
        /* Add bloom filter into fileReader for stride skip and file skip. */
        if (NULL != execState && NULL != execState->fileReader) {
            execState->fileReader->addBloomFilter((filter::BloomFilter*)value, colIdx, true);
        }
    }
}

/*
 * @hdfs
 * brief: Get the name string of orc file.
 */
static int HdfsGetFdwType()
{
    return HDFS_ORC;
}

/*
 * brief: Insert a tuple with hdfs partition foreign table info into pg_partition, we store the partition column
 * sequence in pg_partition_partkey.
 * input param @stmt: store information when create foreign table;
 * input param @relid: Oid of the relation;
 * input param @pg_partition:relation information of foreign table;
 * input param @pkey:partition key.
 */
static Oid HdfsInsertForeignPartitionEntry(
    CreateForeignTableStmt* stmt, Oid relid, Relation pg_partition, int2vector* pkey)
{
    Datum values[Natts_pg_partition];
    bool nulls[Natts_pg_partition];
    HeapTuple tup = NULL;
    Oid retOid;
    NameData relnamedata;
    errno_t rc = EOK;

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "\0", "\0");
    if (-1 == namestrcpy(&relnamedata, stmt->base.relation->relname)) {
        ereport(ERROR,
            (errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
                errmodule(MOD_HDFS),
                errmsg("The name of the relation is invalid.")));
    }

    values[Anum_pg_partition_relname - 1] = NameGetDatum(&relnamedata);
    values[Anum_pg_partition_parttype - 1] = CharGetDatum(PART_OBJ_TYPE_PARTED_TABLE);
    values[Anum_pg_partition_parentid - 1] = ObjectIdGetDatum(relid);
    values[Anum_pg_partition_rangenum - 1] = UInt32GetDatum(list_length(stmt->part_state->partitionKey));
    values[Anum_pg_partition_intervalnum - 1] = UInt32GetDatum(0);
    values[Anum_pg_partition_partstrategy - 1] = CharGetDatum(PART_STRATEGY_VALUE);
    values[Anum_pg_partition_relfilenode - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_partition_reltablespace - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_partition_relpages - 1] = Float8GetDatum(InvalidOid);
    values[Anum_pg_partition_reltuples - 1] = Float8GetDatum(InvalidOid);
    values[Anum_pg_partition_relallvisible - 1] = UInt32GetDatum(InvalidOid);
    ;
    values[Anum_pg_partition_reltoastrelid - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_partition_reltoastidxid - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_partition_indextblid - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_partition_deltarelid - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_partition_reldeltaidx - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_partition_relcudescrelid - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_partition_relcudescidx - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_partition_relfrozenxid - 1] = ShortTransactionIdGetDatum(InvalidOid);

    /*partition key*/
    if (pkey != NULL) {
        values[Anum_pg_partition_partkey - 1] = PointerGetDatum(pkey);
    } else {
        nulls[Anum_pg_partition_partkey - 1] = true;
    }

    nulls[Anum_pg_partition_intablespace - 1] = true;
    nulls[Anum_pg_partition_intspnum - 1] = true;

    /*interval*/
    nulls[Anum_pg_partition_interval - 1] = true;

    /*maxvalue*/
    nulls[Anum_pg_partition_boundaries - 1] = true;

    /*transit*/
    nulls[Anum_pg_partition_transit - 1] = true;

    /*reloptions*/
    nulls[Anum_pg_partition_reloptions - 1] = true;

    values[Anum_pg_partition_relfrozenxid64 - 1] = TransactionIdGetDatum(InvalidOid);
    /*form a tuple using values and null array, and insert it*/
    tup = heap_form_tuple(RelationGetDescr(pg_partition), values, nulls);
    HeapTupleSetOid(tup, GetNewOid(pg_partition));
    retOid = simple_heap_insert(pg_partition, tup);
    CatalogUpdateIndexes(pg_partition, tup);

    heap_freetuple(tup);
    tup = NULL;
    return retOid;
}

/**
 * @Description: Fetchs all valid option names that are belong to the current
 * context, and append a comma separated string.
 * @in checkOptionBit: the current context.
 * @return return the string of option names.
 */
static StringInfo HdfsGetOptionNames(uint32 checkOptionBit)
{
    bool flag = false;  // is first option appended
    HdfsValidOption option;
    const HdfsValidOption* validOption = NULL;
    StringInfo names = makeStringInfo();
    uint32 count = 0;
    uint32 i = 0;

    if (checkOptionBit & DFS_OPTION_ARRAY) {
        count = ValidDFSOptionCount;
        validOption = ValidDFSOptionArray;
    } else if (checkOptionBit & SERVER_TYPE_OPTION_ARRAY) {
        count = ValidServerTypeOptionCount;
        validOption = ValidServerTypeOptionArray;
    } else {
        Assert(0);
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmodule(MOD_HDFS),
                errmsg("Do not occur here when get option names.")));
    }

    for (i = 0; i < count; i++) {
        option = validOption[i];
        if (checkOptionBit & option.optType) {
            if (flag) {
                appendStringInfoString(names, ", ");
            }

            appendStringInfoString(names, option.optionName);
            flag = true;
        }
    }

    return names;
}

static void checkSingleByteOption(const char* quoteStr, const char* optionName)
{
    if (1 != strlen(quoteStr)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmodule(MOD_DFS),
                errmsg("%s option must be a single one-byte character.", optionName)));
    }
}

static void checkChunkSize(char* chunksizeStr)
{
    int chunksize = 0;

    /* Error-out invalid numeric value */
    if (!parse_int(chunksizeStr, &chunksize, 0, NULL)) {
        ereport(ERROR,
            (errcode(ERRCODE_FDW_INVALID_STRING_FORMAT),
                errmsg("Invalid 'chunksize' option value '%s', only numeric value can be set", chunksizeStr)));
    }

    /* Error-out unallowed rage */
    if (chunksize < 8 || chunksize > 512) {
        ereport(ERROR,
            (errcode(ERRCODE_FDW_INVALID_STRING_FORMAT),
                errmsg("Invalid 'chunksize' option value '%s' for OBS Read-Only table, valid range [8, 512] in MB",
                    chunksizeStr)));
    }
}

/**
 * @Description: Check the location option for the obs foreign table.
 * the each location must be start from "obs;//" prefix. the lcations are
 * separated by character '|'. If the location dose not satisfy the location
 * rule, we will throw an error.
 * @in location, the location to be checked.
 * @return none.
 */
static char* checkLocationOption(char* location)
{
    int len = location ? strlen(location) : 0;
    char* str = NULL;
    char* token = NULL;
    char* saved = NULL;

    StringInfo retStr = makeStringInfo();

    if (0 == len) {
        ereport(ERROR,
            (errcode(ERRCODE_FDW_ERROR), errmodule(MOD_DFS), errmsg("Invalid location value for the foreign table.")));
    }

    token = strtok_r(location, "|", &saved);
    while (token != NULL) {
        str = TrimStr(token);
        if (NULL == str || 0 != pg_strncasecmp(str, OBS_PREFIX, OBS_PREfIX_LEN)) {
            if (NULL != str)
                pfree_ext(str);

            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmodule(MOD_DFS),
                    errmsg("Invalid location value for the foreign table."),
                    errdetail("The location option need start from %s string.", OBS_PREFIX)));
        }

        checkObsPath(str + OBS_PREfIX_LEN - 1, OPTION_NAME_LOCATION, "|");
        appendStringInfo(retStr, "%s", str);
        pfree_ext(str);
        token = strtok_r(NULL, "|", &saved);
        if (token != NULL) {
            appendStringInfo(retStr, "|");
        }
    }

    return retStr->data;
}

/*
 *brief: Check checkencoding option.
 *input param @Defel: the option information struct pointer
 */
static void CheckCheckEncoding(DefElem* defel)
{
    /*
     * Currently, the orc format is supported for hdfs foreign table, but
     * the parquet and csv formats will be supported in the future.
     */
    char* checkEncodingLevel = defGetString(defel);
    if (0 == pg_strcasecmp(checkEncodingLevel, "high") || 0 == pg_strcasecmp(checkEncodingLevel, "low"))
        return;

    ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmodule(MOD_HDFS),
            errmsg("Only high/low is supported for the checkencoding option.")));
}

/* @hdfs
 * brief: Check whether or not exist filenames option for the HDFS partition foreign table.
 *        The HDFS partition foreign table dose not support filenames option.
 * input param @OptList: The foreign table option list.
 */
static void CheckFilenamesForPartitionFt(List* OptList)
{
    ListCell* cell = NULL;
    foreach (cell, OptList) {
        DefElem* Opt = (DefElem*)lfirst(cell);

        if (0 == pg_strcasecmp(Opt->defname, OPTION_NAME_FILENAMES)) {
            ereport(ERROR,
                (errcode(ERRCODE_FDW_ERROR),
                    errmodule(MOD_HDFS),
                    errmsg("The filenames option is not supported for a partitioned foreign table.")));
        }
    }
}

/*
 * @Description: check format is orc
 * @IN OptList: The foreign table option list.
 * @Return: true if orc format, false for other
 * @See also:
 */
static bool IsHdfsFormat(List* OptList)
{
    ListCell* cell = NULL;
    foreach (cell, OptList) {
        DefElem* Opt = (DefElem*)lfirst(cell);

        if (0 == pg_strcasecmp(Opt->defname, OPTION_NAME_FORMAT)) {
            char* format = defGetString(Opt);
            if (0 == pg_strcasecmp(format, DFS_FORMAT_ORC) || 0 == pg_strcasecmp(format, DFS_FORMAT_PARQUET) ||
                0 == pg_strcasecmp(format, DFS_FORMAT_CARBONDATA)) {
                return true;
            }
        }
    }

    return false;
}

/*
 *brief: Get format option.
 *input param @DefElem: the option information struct pointer
 */
static DFSFileType getFormatByDefElem(DefElem* opt)
{
    char* format = NULL;

    if (0 == pg_strcasecmp(opt->defname, OPTION_NAME_FORMAT)) {
        format = defGetString(opt);
        return getFormatByName(format);
    }
    return DFS_INVALID;
}

/*
 *brief: Get format option.
 *input param @format: the format(ORC/TEXT/CSV/Parquet/Carbondata)
 */
static DFSFileType getFormatByName(char* format)
{
    /*
     * Currently, the orc format is supported for hdfs foreign table, but
     * the parquet and csv formats will be supported in the future.
     */
    DFSFileType format_type = DFS_INVALID;

    if (format != NULL) {
        if (0 == pg_strcasecmp(format, DFS_FORMAT_ORC)) {
            format_type = DFS_ORC;
        } else if (0 == pg_strcasecmp(format, DFS_FORMAT_TEXT)) {
            format_type = DFS_TEXT;
        } else if (0 == pg_strcasecmp(format, DFS_FORMAT_CSV)) {
            format_type = DFS_CSV;
        } else if (0 == pg_strcasecmp(format, DFS_FORMAT_PARQUET)) {
            format_type = DFS_PARQUET;
        } else if (0 == pg_strcasecmp(format, DFS_FORMAT_CARBONDATA)) {
            format_type = DFS_CARBONDATA;
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmodule(MOD_DFS),
                    errmsg("Invalid option \"%s\"", format),
                    errhint("Valid options in this context are: orc, parquet, carbondata, text, csv")));
        }
    }

    return format_type;
}

/*
 * @Description: check log_remote and reject_limit of extOption
 * @IN OptList: The foreign table option list.
 * @Return: true if log_remote or reject_limit, false for other
 * @See also:
 */
static bool CheckExtOption(List* extOptList, const char* str)
{
    bool CheckExtOption = false;
    ListCell* cell = NULL;
    foreach (cell, extOptList) {
        DefElem* Opt = (DefElem*)lfirst(cell);

        if (0 == pg_strcasecmp(Opt->defname, str)) {
            CheckExtOption = true;
            break;
        }
    }

    return CheckExtOption;
}

/*
 * @Description: check delimiter value
 * @IN defel: delimiter value
 * @See also:
 */
static void CheckDelimiter(DefElem* defel, DFSFileType formatType)
{
    char* delimiter = defGetString(defel);

    /* delimiter strings should be no more than 10 bytes. */
    if (strlen(delimiter) > DELIM_MAX_LEN) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmodule(MOD_HDFS),
                errmsg("delimiter must be less than or equal to %d bytes", DELIM_MAX_LEN)));
    }

    /* Disallow end-of-line characters */
    if (strchr(delimiter, '\r') != NULL || strchr(delimiter, '\n') != NULL)

    {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmodule(MOD_HDFS),
                errmsg("delimiter cannot be \'\\r\' or \'\\n\'")));
    }

    if (DFS_TEXT == formatType && strpbrk(delimiter, INVALID_DELIM_CHAR) != NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmodule(MOD_HDFS),
                errmsg("delimiter \"%s\" cannot contain any characters in\"%s\"", delimiter, INVALID_DELIM_CHAR)));
    }
}

/*
 * @Description: check null str value
 * @IN defel: null str value
 * @See also:
 */
static void CheckNull(DefElem* defel)
{
    char* null_str = defGetString(defel);

    if (strlen(null_str) > NULL_STR_MAX_LEN) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmodule(MOD_HDFS), errmsg("null value string is too long")));
    }

    /* Disallow end-of-line characters */
    if (strchr(null_str, '\r') != NULL || strchr(null_str, '\n') != NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmodule(MOD_HDFS),
                errmsg("null representation cannot use \'\\r\' or \'\\n\'")));
    }
}

/*
 * @Description: check single text options
 * @IN optionDef: option value
 * @IN/OUT textOptionsFound: text option found detail
 * @Return: ture if found any of text options, false not have any text options found
 * @Notice:  the TextCsvOptionsFound->xxx.optionDef will equal to optionDef
 */
static bool CheckTextCsvOptionsFound(DefElem* optionDef, TextCsvOptionsFound* textOptionsFound, DFSFileType formatType)
{
    char* optionName = optionDef->defname;
    bool found = false;

    if (0 == pg_strcasecmp(optionName, OPTION_NAME_DELIMITER)) {
        textOptionsFound->delimiter.found = true;
        textOptionsFound->delimiter.optionDef = optionDef;
        CheckDelimiter(optionDef, formatType);
        found = true;
    } else if (0 == pg_strcasecmp(optionName, OPTION_NAME_NULL)) {
        textOptionsFound->nullstr.found = true;
        textOptionsFound->nullstr.optionDef = optionDef;
        CheckNull(optionDef);
        found = true;
    } else if (0 == pg_strcasecmp(optionName, OPTION_NAME_HEADER)) {
        textOptionsFound->header.found = true;
        textOptionsFound->header.optionDef = optionDef;
        (void)defGetBoolean(optionDef);
        found = true;
    } else if (0 == pg_strcasecmp(optionName, OPTION_NAME_NOESCAPING)) {
        textOptionsFound->noescaping.found = true;
        textOptionsFound->noescaping.optionDef = optionDef;
        (void)defGetBoolean(optionDef);
        found = true;
    } else if (0 == pg_strcasecmp(optionName, OPTION_NAME_FILL_MISSING_FIELDS)) {
        textOptionsFound->fillmissingfields.found = true;
        textOptionsFound->fillmissingfields.optionDef = optionDef;
        (void)defGetBoolean(optionDef);
        found = true;
    } else if (0 == pg_strcasecmp(optionName, OPTION_NAME_IGNORE_EXTRA_DATA)) {
        textOptionsFound->ignoreextradata.found = true;
        textOptionsFound->ignoreextradata.optionDef = optionDef;
        (void)defGetBoolean(optionDef);
        found = true;
    } else if (0 == pg_strcasecmp(optionName, OPTION_NAME_DATE_FORMAT)) {
        textOptionsFound->dateFormat.found = true;
        textOptionsFound->dateFormat.optionDef = optionDef;
        check_datetime_format(defGetString(optionDef));
        found = true;
    } else if (0 == pg_strcasecmp(optionName, OPTION_NAME_TIME_FORMAT)) {
        textOptionsFound->timeFormat.found = true;
        textOptionsFound->timeFormat.optionDef = optionDef;
        check_datetime_format(defGetString(optionDef));
        found = true;
    } else if (0 == pg_strcasecmp(optionName, OPTION_NAME_TIMESTAMP_FORMAT)) {
        textOptionsFound->timestampFormat.found = true;
        textOptionsFound->timestampFormat.optionDef = optionDef;
        check_datetime_format(defGetString(optionDef));
        found = true;
    } else if (0 == pg_strcasecmp(optionName, OPTION_NAME_SMALLDATETIME_FORMAT)) {
        textOptionsFound->smalldatetimeFormat.found = true;
        textOptionsFound->smalldatetimeFormat.optionDef = optionDef;
        check_datetime_format(defGetString(optionDef));
        found = true;
    } else if (0 == pg_strcasecmp(optionName, OPTION_NAME_QUOTE)) {
        textOptionsFound->quotestr.found = true;
        textOptionsFound->quotestr.optionDef = optionDef;

        checkSingleByteOption(defGetString(optionDef), OPTION_NAME_QUOTE);
        found = true;
    } else if (0 == pg_strcasecmp(optionName, OPTION_NAME_ESCAPE)) {
        textOptionsFound->escapestr.found = true;
        textOptionsFound->escapestr.optionDef = optionDef;
        checkSingleByteOption(defGetString(optionDef), OPTION_NAME_ESCAPE);
        found = true;
    } else if (0 == pg_strcasecmp(optionName, OPTION_NAME_CHUNK_SIZE)) {
        checkChunkSize(defGetString(optionDef));
        found = true;
    }

    return found;
}

/*
 * @Description:  check text\csv options
 * @IN textOptionsFound: text\csv options
 * @IN checkEncodingLevel: check encoding level
 * @See also:
 */
static void CheckCsvOptions(const TextCsvOptionsFound* textOptionsFound, const char* checkEncodingLevel, 
                            const char* delimiter, const char* nullstr, DFSFileType formatType, bool compatible_illegal_chars)
{
    const char* quoteStr = NULL;
    const char* escapeStr = NULL;
    quoteStr = textOptionsFound->quotestr.found ? defGetString(textOptionsFound->quotestr.optionDef) : "\"";
    
    if (NULL != strchr(delimiter, *quoteStr)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmodule(MOD_DFS),
                errmsg("delimiter cannot contain quote character")));
    }
    
    if (NULL != strstr(nullstr, quoteStr)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmodule(MOD_DFS),
                errmsg("quote character must not appear in the NULL specification")));
    }
    
    escapeStr = textOptionsFound->escapestr.found ? defGetString(textOptionsFound->escapestr.optionDef) : "\"";
    if (textOptionsFound->quotestr.found && textOptionsFound->escapestr.found && quoteStr[0] == escapeStr[0]) {
        escapeStr = "\0";
    }
    
    if (compatible_illegal_chars) {
        checkIllegalChr(quoteStr, OPTION_NAME_QUOTE);
        checkIllegalChr(escapeStr, OPTION_NAME_ESCAPE);
    }
}

static void CheckTextCsvOptions(
    const TextCsvOptionsFound* textOptionsFound, const char* checkEncodingLevel, DFSFileType formatType)
{
    DefElem* optionDef = textOptionsFound->delimiter.optionDef;
    char* delimiter = NULL;

    if (NULL != optionDef) {
        delimiter = defGetString(optionDef);
    } else if (DFS_TEXT == formatType) {
        delimiter = "\t";
    } else {
        delimiter = ",";
    }

    optionDef = textOptionsFound->nullstr.optionDef;
    char* nullstr = NULL;

    if (NULL != optionDef) {
        nullstr = defGetString(optionDef);
    } else if (DFS_TEXT == formatType) {
        nullstr = "\\N";
    } else {
        nullstr = "";
    }

    size_t delimiter_len = strlen(delimiter);
    size_t nullstr_len = strlen(nullstr);

    /* Don't allow the delimiter to appear in the null string. */
    if ((delimiter_len >= 1 && nullstr_len >= 1) &&
        (strstr(nullstr, delimiter) != NULL || strstr(delimiter, nullstr) != NULL)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("delimiter must not appear in the NULL specification")));
    }

    bool compatible_illegal_chars = false;
    if (checkEncodingLevel && (0 == pg_strcasecmp(checkEncodingLevel, "low"))) {
        compatible_illegal_chars = true;
    }
    /* confusion when  compatible illegal chars */
    if (compatible_illegal_chars) {
        /*  check nullstr */
        if (1 == nullstr_len) {
            checkIllegalChr(nullstr, OPTION_NAME_NULL);
        }

        /*  check delimiter 	*/
        if (1 == delimiter_len) {
            checkIllegalChr(delimiter, OPTION_NAME_DELIMITER);
        }
    }

    if (DFS_CSV == formatType) {
        CheckCsvOptions(textOptionsFound, checkEncodingLevel, delimiter, nullstr, formatType, compatible_illegal_chars);
    }
}

static void checkIllegalChr(const char* optionValue, const char* optionName)
{

    if (' ' == optionValue[0] || '?' == optionValue[0]) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmodule(MOD_DFS),
                errmsg("illegal chars conversion may confuse %s 0x%x", optionName, optionValue[0])));
    }
}

/*
 * @Description: create reader
 * @IN/OUT foreignTableId: foreign table id
 * @IN/OUT readerState: reader state
 * @IN/OUT conn: dfs connector
 * @Return: reader ptr
 * @See also:
 */
dfs::reader::Reader* getReader(
    Oid foreignTableId, dfs::reader::ReaderState* readerState, dfs::DFSConnector* conn, const char* format)
{
    Assert(NULL != format);
    if (!pg_strcasecmp(format, DFS_FORMAT_ORC)) {
        return dfs::reader::createOrcReader(readerState, conn, true);
    } else if (!pg_strcasecmp(format, DFS_FORMAT_PARQUET)) {
        return dfs::reader::createParquetReader(readerState, conn, true);
    } else if (!pg_strcasecmp(format, DFS_FORMAT_TEXT)) {
        return dfs::reader::createTextReader(readerState, conn, true);
#ifdef ENABLE_MULTIPLE_NODES
    } else if (!pg_strcasecmp(format, DFS_FORMAT_CARBONDATA)) {
        return dfs::reader::createCarbondataReader(readerState, conn, true);
#endif
    } else {
        return dfs::reader::createCsvReader(readerState, conn, true);
    }
}
