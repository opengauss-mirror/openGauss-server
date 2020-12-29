/* -------------------------------------------------------------------------
 *
 * foreign.h
 *	  support for foreign-data wrappers, servers and user mappings.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 * src/include/foreign/foreign.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef FOREIGN_H
#define FOREIGN_H

#include "access/obs/obs_am.h"
#include "commands/defrem.h"
#include "nodes/parsenodes.h"

#ifndef OBS_SERVER
#define OBS_SERVER "obs"
#endif
#ifndef HDFS_SERVER
#define HDFS_SERVER "hdfs"
#endif

#define OBS_BUCKET_URL_FORMAT_FLAG ".obs."
#define OBS_PREFIX "obs://"
#define OBS_PREfIX_LEN strlen(OBS_PREFIX)

/* Defines for valid option names. */
/* The followings are foreign table options. */
#define OPTION_NAME_LOCATION "location"
#define OPTION_NAME_FORMAT "format"
#define OPTION_NAME_HEADER "header"
#define OPTION_NAME_DELIMITER "delimiter"
#define OPTION_NAME_QUOTE "quote"
#define OPTION_NAME_ESCAPE "escape"
#define OPTION_NAME_NULL "null"
#define OPTION_NAME_NOESCAPING "noescaping"
#define OPTION_NAME_ENCODING "encoding"
#define OPTION_NAME_FILL_MISSING_FIELDS "fill_missing_fields"
#define OPTION_NAME_IGNORE_EXTRA_DATA "ignore_extra_data"
#define OPTION_NAME_DATE_FORMAT "date_format"
#define OPTION_NAME_TIME_FORMAT "time_format"
#define OPTION_NAME_TIMESTAMP_FORMAT "timestamp_format"
#define OPTION_NAME_SMALLDATETIME_FORMAT "smalldatetime_format"
#define OPTION_NAME_CHUNK_SIZE "chunksize"

#define OPTION_NAME_FILENAMES "filenames"
#define OPTION_NAME_FOLDERNAME "foldername"
#define OPTION_NAME_CHECK_ENCODING "checkencoding"
#define OPTION_NAME_TOTALROWS "totalrows"

/* The followings are foreign server options. */
#define OPTION_NAME_REGION "region"
#define OPTION_NAME_ADDRESS "address"
#define OPTION_NAME_CFGPATH "hdfscfgpath"
#define OPTION_NAME_SERVER_ENCRYPT "encrypt"
#define OPTION_NAME_SERVER_AK "access_key"
#define OPTION_NAME_SERVER_SAK "secret_access_key"
#define OPTION_NAME_SERVER_TYPE "type"
#define OPTION_NAME_DBNAME "dbname"
#define OPTION_NAME_REMOTESERVERNAME "remoteservername"
#define OPTION_NAME_OBSKEY "obskey"

/* The followings are foreign table option values */
#define MAX_TOTALROWS LONG_MAX
#define MIN_TOTALROWS 1

/* Helper for obtaining username for user mapping */
#define MappingUserName(userid) (OidIsValid(userid) ? GetUserNameFromId(userid) : "public")

#define DFS_FORMAT_TEXT "text"
#define DFS_FORMAT_CSV "csv"
#define DFS_FORMAT_ORC "orc"
#define DFS_FORMAT_PARQUET "parquet"
#define DFS_FORMAT_CARBONDATA "carbondata"

typedef enum DFSFileType {
    DFS_INVALID = -1,
    DFS_ORC = 0,
    DFS_PARQUET = 1,
    DFS_TEXT = 2,
    DFS_CSV = 3,
    DFS_CARBONDATA = 4
} DFSFileType;

/*
 * Generic option types for validation.
 * NB! Thes are treated as flags, so use only powers of two here.
 */
typedef enum {
    ServerOpt = 1,      /* options applicable to SERVER */
    UserMappingOpt = 2, /* options for USER MAPPING */
    FdwOpt = 4          /* options for FOREIGN DATA WRAPPER */
} GenericOptionFlags;

typedef struct ForeignDataWrapper {
    Oid fdwid;        /* FDW Oid */
    Oid owner;        /* FDW owner user Oid */
    char* fdwname;    /* Name of the FDW */
    Oid fdwhandler;   /* Oid of handler function, or 0 */
    Oid fdwvalidator; /* Oid of validator function, or 0 */
    List* options;    /* fdwoptions as DefElem list */
} ForeignDataWrapper;

typedef struct ForeignServer {
    Oid serverid;        /* server Oid */
    Oid fdwid;           /* foreign-data wrapper */
    Oid owner;           /* server owner user Oid */
    char* servername;    /* name of the server */
    char* servertype;    /* server type, optional */
    char* serverversion; /* server version, optional */
    List* options;       /* srvoptions as DefElem list */
} ForeignServer;

typedef struct UserMapping {
    Oid userid;    /* local user Oid */
    Oid serverid;  /* server Oid */
    List* options; /* useoptions as DefElem list */
} UserMapping;

typedef struct ForeignTable {
    Oid relid;       /* relation Oid */
    Oid serverid;    /* server Oid */
    List* options;   /* ftoptions as DefElem list */
    bool write_only; /* is relation updatable ? */
} ForeignTable;

/*
 *HdfsServerAddress keeps foreign server ip and port
 */
typedef struct HdfsServerAddress {
    char* HdfsIp;
    char* HdfsPort;
} HdfsServerAddress;

/* Define the server type enum in order to realize some function interface. */
typedef enum ServerTypeOption {
    T_INVALID = 0,
    T_OBS_SERVER,
    T_HDFS_SERVER,
    T_MOT_SERVER,
    T_DUMMY_SERVER,
    T_TXT_CSV_OBS_SERVER, /* mark the txt/csv foramt OBS foreign server. the fdw
                        is dist_fdw. */
    T_PGFDW_SERVER
} ServerTypeOption;

/*
 * This struct store the foreign table options for computing pool.
 */
typedef struct ForeignOptions {
    NodeTag type;

    ServerTypeOption stype;

    /* it store the all foreign options.  */
    List* fOptions;
} ForeignOptions;

/*
 * HdfsFdwOptions holds the option values to be used when reading and parsing
 * the orc file. To resolve these values, we first check foreign table's
 * options, and if not present, we then fall back to the default values
 * specified above.
 */
typedef struct HdfsFdwOptions {
    char* foldername;
    char* filename;
    char* address;
    int port;
    char* location;
} HdfsFdwOptions;

typedef struct HdfsOptions {
    char* servername;
    char* format;
} HdfsOptions;

extern int find_Nth(const char* str, unsigned N, const char* find);

extern ForeignServer* GetForeignServer(Oid serverid);
extern ForeignServer* GetForeignServerByName(const char* name, bool missing_ok);
extern UserMapping* GetUserMapping(Oid userid, Oid serverid);
extern ForeignDataWrapper* GetForeignDataWrapper(Oid fdwid);
extern ForeignDataWrapper* GetForeignDataWrapperByName(const char* name, bool missing_ok);
extern ForeignTable* GetForeignTable(Oid relid);

extern List* GetForeignColumnOptions(Oid relid, AttrNumber attnum);

extern Oid get_foreign_data_wrapper_oid(const char* fdwname, bool missing_ok);
extern Oid get_foreign_server_oid(const char* servername, bool missing_ok);

extern DefElem* GetForeignTableOptionByName(Oid reloid, const char* optname);
extern bool IsSpecifiedFDW(const char* ServerName, const char* SepcifiedType);
extern char* getServerOptionValue(Oid srvOid, const char* optionName);

// foreign table option values
DefElem* HdfsGetOptionDefElem(Oid foreignTableId, const char* optionName);

extern char* getFTOptionValue(List* option_list, const char* option_name);
extern DefElemAction getFTAlterAction(List* option_list, const char* option_name);

extern DefElem* getFTOptionDefElemFromList(List* optionList, const char* optionName);

extern double convertFTOptionValue(const char* s);
extern char* GetForeignServerName(Oid serverid);
extern ColumnDef* makeColumnDef(const char* colname, char* coltype);

/**
 * @Description: Jude whether type of the foreign table equal to the specified type or not.
 * @in relId: The foreign table Oid.
 * @in SepcifiedType: The given type of foreign table.
 * @return If the relation type equal to the given type of foreign table, return true,
 *         otherwise return false.
 */
extern bool IsSpecifiedFDWFromRelid(Oid relId, const char* SepcifiedType);

/**
 * @Description: Jude whether type of the foreign table support SELECT/INSERT/UPDATE/DELETE/COPY
 * @in relId: The foreign table Oid.
 * @return Rreturn true if the foreign table support those DML.
 */
extern bool CheckSupportedFDWType(Oid relId);

/**
 * @Description: Get the all options for the OBS foreign table.
 * we store the option into ObsOptions.
 * @in foreignTable, the foreign table oid.
 * @return return all options.
 */
extern ObsOptions* getObsOptions(Oid foreignTableId);

/*
 * brief: Creates and returns the option values to be used.
 * input param @foreignTableId: Oid of the foreign tableId.
 */
HdfsFdwOptions* HdfsGetOptions(Oid foreignTableId);

bool isSpecifiedSrvTypeFromRelId(Oid relId, const char* SepcifiedType);
bool isSpecifiedSrvTypeFromSrvName(const char* srvName, const char* SepcifiedType);

/**
 * @Description: Currently, two OBS location format support in database.
 * one format is "gsobs:://obsdomain/bucket/prefix", another is "gsobs:://bucket.obsdomain/prefix".
 * we adjust second format to the first format, we only deal with the first format in parse phase.
 * @in optionsList, if the list include location option, we will adjust format.
 * @return return new optionsList.
 */
List* regularizeObsLocationInfo(List* optionsList);

char* rebuildAllLocationOptions(char* regionCode, char* location);

/**
 * @Description: If using the region option, we must to reset location option
 * with region string, eg. obs://bucket/prefix ->obs://URL/prefix/bucket.
 * @in option List tobe given.
 * @return return the modified option.
 */
List* adaptOBSURL(List* list);

/**
 * @Description: Currently, two OBS location format support in database.
 * one format is "gsobs:://obsdomain/bucket/prefix", another is "gsobs:://bucket.obsdomain/prefix".
 * we adjust second format to the first format, we only deal with the first format in parse phase.
 * @in sourceStr, if the list include location option, we will adjust format.
 * @return return new location string.
 */
char* adjustObsLocationInfoOrder(char* sourceStr);

/*
 * brief:  Get the value of the given option name by traversing
 *         foreign table and foreign server options. Return the
 *         value of the given option, if found. Otherwise, return NULL;
 * input param @foreignTableId: Oid of the foreign tableId;
 * input param @optionName: name of the option.
 */
char* HdfsGetOptionValue(Oid foreignTableId, const char* optionName);
ServerTypeOption getServerType(Oid foreignTableId);
List* getFdwOptions(Oid foreignTableId);
int getSpecialCharCnt(const char* path, const char specialChar);
ObsOptions* setObsSrvOptions(ForeignOptions* fOptions);
HdfsOptions* setHdfsSrvOptions(ForeignOptions* fOptions);

bool isWriteOnlyFt(Oid relid);

#define isObsOrHdfsTableFormTblOid(relId) \
    (isSpecifiedSrvTypeFromRelId(relId, HDFS) || isSpecifiedSrvTypeFromRelId(relId, OBS))

#define isMOTFromTblOid(relId) \
    (IsSpecifiedFDWFromRelid(relId, MOT_FDW))

#define isObsOrHdfsTableFormSrvName(srvName) \
    (isSpecifiedSrvTypeFromSrvName(srvName, HDFS) || isSpecifiedSrvTypeFromSrvName(srvName, OBS))

#define isMOTTableFromSrvName(srvName) \
    (IsSpecifiedFDW(srvName, MOT_FDW))

#define isPostgresFDWFromSrvName(srvName) \
    (IsSpecifiedFDW(srvName, POSTGRES_FDW))

#define isMysqlFDWFromTblOid(relId) \
    (IsSpecifiedFDWFromRelid(relId, MYSQL_FDW))

#define isOracleFDWFromTblOid(relId) \
    (IsSpecifiedFDWFromRelid(relId, ORACLE_FDW))

#define isPostgresFDWFromTblOid(relId) \
    (IsSpecifiedFDWFromRelid(relId, POSTGRES_FDW))

#define IS_OBS_CSV_TXT_FOREIGN_TABLE(relId) \
    (IsSpecifiedFDWFromRelid(relId, DIST_FDW) && (is_obs_protocol(HdfsGetOptionValue(relId, optLocation))))

#define CAN_BUILD_INFORMATIONAL_CONSTRAINT_BY_RELID(relId) \
    (isObsOrHdfsTableFormTblOid(relId) || IS_OBS_CSV_TXT_FOREIGN_TABLE(relId))

#define CAN_BUILD_INFORMATIONAL_CONSTRAINT_BY_STMT(stmt)      \
    (isObsOrHdfsTableFormSrvName(stmt->servername) ||         \
        (NULL == getFTOptionValue(stmt->options, optLocation) \
                ? false                                       \
                : is_obs_protocol(getFTOptionValue(stmt->options, optLocation))))

#define IS_LOGFDW_FOREIGN_TABLE(relId) (IsSpecifiedFDWFromRelid(relId, LOG_FDW))

#define IS_POSTGRESFDW_FOREIGN_TABLE(relId) (IsSpecifiedFDWFromRelid(relId, GC_FDW))

#define ENCRYPT_STR_PREFIX "encryptstr"
#define MIN_ENCRYPTED_PASSWORD_LENGTH 54

#define isEncryptedPassword(passwd)                                          \
    (strncmp(passwd, ENCRYPT_STR_PREFIX, strlen(ENCRYPT_STR_PREFIX)) == 0 && \
        strlen(passwd) >= MIN_ENCRYPTED_PASSWORD_LENGTH)

#endif /* FOREIGN_H */
