/* -------------------------------------------------------------------------
 *
 * pg_dump.c
 *	  pg_dump is a utility for dumping out an openGauss database
 *	  into a script file.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *	pg_dump will read the system catalogs in a database and dump out a
 *	script that reproduces the schema in terms of SQL that is understood
 *	by openGauss
 *
 *	Note that pg_dump runs in a transaction-snapshot mode transaction,
 *	so it sees a consistent snapshot of the database including system
 *	catalogs. However, it relies in part on various specialized backend
 *	functions like pg_get_indexdef(), and those things tend to run on
 *	SnapshotNow time, ie they look at the currently committed state.  So
 *	it is possible to get 'cache lookup failed' error if someone
 *	performs DDL changes while a dump is happening. The window for this
 *	sort of thing is from the acquisition of the transaction snapshot to
 *	getSchemaData() (when pg_dump acquires AccessShareLock on every
 *	table it intends to dump). It isn't very large, but it can happen.
 *
 *	http://archives.postgresql.org/pgsql-bugs/2010-02/msg00187.php
 *
 * IDENTIFICATION
 *	  src/bin/pg_dump/pg_dump.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres_fe.h"
#include <string>

#ifdef WIN32_PG_DUMP
#undef PGDLLIMPORT
#define PGDLLIMPORT
#endif

#include <sys/time.h>
#ifdef ENABLE_NLS
#include <locale.h>
#endif
#ifdef HAVE_TERMIOS_H
#include <termios.h>
#endif

#include "getopt_long.h"

#include "access/attnum.h"
#include "access/sysattr.h"
#include "catalog/pg_am.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_cast.h"
#include "catalog/pg_class.h"
#include "catalog/pg_database.h"
#include "catalog/pg_default_acl.h"
#include "catalog/pg_event_trigger.h"
#include "catalog/pg_largeobject.h"
#include "catalog/pg_largeobject_metadata.h"
#include "catalog/pg_partition.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_rlspolicy.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_publication.h"
#include "catalog/pg_type.h"
#include "catalog/pg_attrdef.h"
#include "catalog/pg_partition.h"
#include "libpq/libpq-fs.h"
#include "libpq/libpq-int.h"
#include "catalog/pgxc_node.h"
#include "pg_backup_archiver.h"
#include "pg_backup_db.h"
#include "dumpmem.h"
#include "dumputils.h"
#include "postgres.h"
#include "knl/knl_variable.h"
#include "common/fe_memutils.h"
#include "openssl/rand.h"
#include "miscadmin.h"
#include "bin/elog.h"
#ifdef HAVE_CE
#include "client_logic_cache/types_to_oid.h"
#include "client_logic_processor/values_processor.h"
#include "client_logic_common/client_logic_utils.h"
#include "client_logic_cache/types_to_oid.h"
#include "client_logic_hooks/global_hook_executor.h"
#include "client_logic_hooks/hooks_manager.h"
#endif
#ifndef WIN32_PG_DUMP
#include "access/transam.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "catalog/pg_constraint.h"

#else

typedef enum {
    REL_CMPRS_NOT_SUPPORT = 0,
    REL_CMPRS_PAGE_PLAIN,
    REL_CMPRS_FIELDS_EXTRACT,
    REL_CMPRS_MAX_TYPE
} RelCompressType;

#define FirstBootstrapObjectId 10000
#define FirstNormalObjectId 16384

#define ConstraintRelationId 2606

#endif

typedef unsigned char GS_UCHAR;
#if defined(__LP64__) || defined(__64BIT__)
typedef unsigned int GS_UINT32;
#else
typedef unsigned long GS_UINT32;
#endif

#ifdef WIN32
#define uint32_t u_long
typedef unsigned int uint32;
typedef unsigned __int64 uint64;
typedef __int64 int64;
typedef int socklen_t;
typedef unsigned short uint16_t;

#define strcasecmp _stricmp
#define strncasecmp _strnicmp
#define strtoll _strtoi64
#define strdup _strdup

#endif

#ifdef GAUSS_SFT_TEST
#include "gauss_sft.h"
#endif
#define PROG_NAME "gs_dump"


#define MAX_FORMATTER_SIZE 255
#define PARTTYPE_VALUE_PARTITIONED_RELATION 'v'
#define CSTORE_NAMESPACE 100
#define OUTPUT_OBJECT_NUM 10000
#define atoxid(x) ((TransactionId)strtoul((x), NULL, 10))
#define MAX_KEY 32
#define MAX_CLIENT_KEYS_PARAM_SIZE 256
#define MAX_CLIENT_ENCRYPTION_KEYS_SIZE 1024
#define PATCH_LEN 50
#define TARGET_V1 (strcasecmp("v1", optarg) == 0 ? true : false)
#define TARGET_V5 (strcasecmp("v5", optarg) == 0 ? true : false)
#define if_exists (targetV1 || targetV5) ? "IF EXISTS " : ""
#define if_cascade (targetV1 || targetV5) ? " CASCADE" : ""
#define INTERVAL_UNITE_OFFSET 3

#define USING_STR_OFFSET 7
#define UNIQUE_OFFSET 7
#define PRIMARY_KEY_OFFSET 11
const int MAX_CMK_STORE_SIZE = 64;
#define  BEGIN_P_STR      " BEGIN_B_PROC " /* used in dolphin type proc body*/
#define  BEGIN_P_LEN      14
#define  BEGIN_N_STR      "    BEGIN     " /* BEGIN_P_STR to same length*/
/* used for progress report */
int g_curStep = 0;
int g_totalObjNums = 0;
int g_dumpObjNums = 0;
const char *g_progressDetails[] = {
    "reading schemas",
    "reading user-defined tables",
    "reading extensions",
    "reading user-defined functions",
    "reading user-defined types",
    "reading procedural languages",
    "reading user-defined aggregate functions",
    "reading user-defined operators",
    "reading user-defined operator classes",
    "reading user-defined operator families",
    "reading user-defined text search parsers",
    "reading user-defined text search templates",
    "reading user-defined text search dictionaries",
    "reading user-defined text search configurations",
    "reading user-defined foreign-data wrappers",
    "reading user-defined foreign servers",
    "reading default privileges",
    "reading user-defined collations",
    "reading user-defined conversions",
    "reading type casts",
    "reading table inheritance information",
    "finding extension members",
    "finding inheritance relationships",
    "reading column info for interesting tables",
    "flagging inherited columns in subtables",
    "reading indexes",
    "reading constraints",
    "reading constraints about foregin table",
    "reading triggers",
    "reading events",
    "reading rewrite rules",
    "reading row level security policies",
    "reading user-defined packages",
    "reading publications",
    "reading publication membership",
    "reading subscriptions",
    "reading event triggers"
};
int g_totalSteps = sizeof(g_progressDetails) / sizeof(g_progressDetails[0]);
static volatile bool g_progressFlagScan = false;
static volatile bool g_progressFlagDump = false;
static pthread_cond_t g_cond = PTHREAD_COND_INITIALIZER;
static pthread_cond_t g_condDump = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t g_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Database Security: Data importing/dumping support AES128. */
#include "utils/aes.h"
#include "pgtime.h"

#ifdef ENABLE_UT
#define static
#endif

#define FREE_PTR(ptr)      \
    if (ptr != NULL) {     \
        free(ptr);         \
        ptr = NULL;        \
    }

extern char* optarg;
extern int optind;
extern int opterr;

inline bool IsCStoreInternalObj(DumpableObject* obj)
{
    return (((obj) != NULL) && ((obj)->nmspace != NULL) && CSTORE_NAMESPACE == (obj)->nmspace->dobj.catId.oid);
}

typedef struct {
    const char* descr; /* comment for an object */
    Oid classoid;      /* object class (catalog OID) */
    Oid objoid;        /* object OID */
    int objsubid;      /* subobject (table column #) */
} CommentItem;

typedef struct {
    const char* provider; /* label provider of this security label */
    const char* label;    /* security label for an object */
    Oid classoid;         /* object class (catalog OID) */
    Oid objoid;           /* object OID */
    int objsubid;         /* subobject (table column #) */
} SecLabelItem;

typedef struct {
    Oid oid;
    char* name;
} TablespaceInfo;

/* global decls */
bool g_verbose; /* User wants verbose narration of our
                 * activities. */

/* various user-settable parameters */
static int compressLevel = -1;
static bool outputBlobs = false;
static int outputClean = 0;
static int outputCreateDB = 0;
static const char* format = NULL;
static const char* filename = NULL;
static bool oids = false;
static int outputNoOwner = 0;
static char* outputSuperuser = NULL;
static const char* username = NULL;
static char* pchPasswd = NULL;
static const char* dumpencoding = NULL;
static const char* pghost = NULL;
static const char* pgport = NULL;
static int exclude_with = 0;
static char* use_role = NULL;
static char* rolepasswd = NULL;
static enum trivalue prompt_password = TRI_DEFAULT;
bool schemaOnly;
bool dataOnly;
bool dont_overwritefile = false;
bool include_extensions = false;
bool check_filepath = false;
int dumpSections; /* bitmask of chosen sections */
bool aclsSkip;
bool targetV1;
bool targetV5;
const char* lockWaitTimeout;

static char connected_node_type = 'N'; /* 'N' -- NONE
                                          'C' -- Coordinator
                                          'D' -- Datanode
                                        */
char* all_data_nodename_list = NULL;
const uint32 CHARACTER_SET_VERSION_NUM = 92844;
const uint32 USTORE_UPGRADE_VERSION = 92368;
const uint32 PACKAGE_ENHANCEMENT = 92444;
const uint32 SUBSCRIPTION_VERSION = 92580;
const uint32 SUBSCRIPTION_BINARY_VERSION_NUM = 92656;
const uint32 B_DUMP_TRIGGER_VERSION_NUM = 92843;
const uint32 EVENT_VERSION = 92844;
const uint32 EVENT_TRIGGER_VERSION_NUM = 92845;
const uint32 RB_OBJECT_VERSION_NUM = 92831;
const uint32 PUBLICATION_DDL_VERSION_NUM = 92921;

#ifdef DUMPSYSLOG
char* syslogpath = NULL;
#endif

/* Database Security: Data importing/dumping support AES128. */
const char* encrypt_mode = NULL;
const char* encrypt_key = NULL;
extern const char* encrypt_salt;

/* subquery used to convert user ID (eg, datdba) to user name */
static const char* username_subquery;

/* obsolete as of 7.3: */
static Oid g_last_builtin_oid; /* value of the last builtin oid */

/*
 * Object inclusion/exclusion lists
 *
 * The string lists record the patterns given by command-line switches,
 * which we then convert to lists of OIDs of matching objects.
 */
static SimpleStringList schema_include_patterns = {NULL, NULL};
static SimpleOidList schema_include_oids = {NULL, NULL};
static SimpleStringList schema_exclude_patterns = {NULL, NULL};
static SimpleOidList schema_exclude_oids = {NULL, NULL};

static SimpleStringList table_include_patterns = {NULL, NULL};
static SimpleOidList table_include_oids = {NULL, NULL};
/* depend object storage list */
static SimpleOidList depend_obj_oids = {NULL, NULL};
/* object self and depend object storage list */
static SimpleOidList all_obj_oids = {NULL, NULL};
static SimpleStringList table_exclude_patterns = {NULL, NULL};
static SimpleOidList table_exclude_oids = {NULL, NULL};
static SimpleStringList tabledata_exclude_patterns = {NULL, NULL};
static SimpleOidList tabledata_exclude_oids = {NULL, NULL};
static SimpleStringList exclude_guc = {NULL, NULL};

/* default, if no "inclusion" switches appear, is to dump everything */
static bool include_everything = true;

char g_opaque_type[10]; /* name for the opaque type */

/* placeholders for the delimiters for comments */
char g_comment_start[10];
char g_comment_end[10];

static const CatalogId nilCatalogId = {0, 0};

/* flags for various command-line long options */
#ifdef DUMPSYSLOG
int dump_syslog = 0;
#endif

static int binary_upgrade = 0;
static int disable_dollar_quoting = 0;
static int dump_inserts = 0;
static int column_inserts = 0;
static int no_security_labels = 0;
static int no_unlogged_table_data = 0;
static int serializable_deferrable = 0;
static int non_Lock_Table = 0;
static int include_alter_table = 0;
static int exclude_self = 0;
static int include_depend_objs = 0;
static bool is_encrypt = false;
static bool could_encrypt = false;
static int exclude_function = 0;
static bool is_pipeline = false;
static int no_subscriptions = 0;
static int no_publications = 0;
#if defined(USE_ASSERT_CHECKING) || defined(FASTCHECK)
static bool disable_progress = false;
#endif

/* Used to count the number of -t input */
int gTableCount = 0;
/* Individual object backup tag */
bool isSingleTableDump = false;

#ifdef PGXC
static int include_nodes = 0;
#endif

static int g_tablespaceNum = -1;
static TablespaceInfo* g_tablespaces = nullptr;

#define disconnect_and_exit(conn, code)                                        \
    do {                                                                       \
        if ((conn) != NULL) {                                                  \
            char* pqpss = PQpass(conn);                                        \
            if (pqpss != NULL) {                                               \
                errno_t rc = memset_s(pqpss, strlen(pqpss), 0, strlen(pqpss)); \
                securec_check_c(rc, "\0", "\0");                               \
            }                                                                  \
            PQfinish(conn);                                                    \
        }                                                                      \
        exit_nicely(code);                                                     \
    } while (0)

void help(const char* progname);
static void setup_connection(Archive* AH);
static void dumpsyslog(Archive* fout);
static void getopt_dump(int argc, char** argv, struct option options[], int* result);
static void validatedumpoptions(void);
static void validatedumpformats(void);
static ArchiveFormat parseArchiveFormat(ArchiveMode* mode);
static void expand_schema_name_patterns(Archive* fout, SimpleStringList* patterns, SimpleOidList* oids, bool isinclude);
static void expand_table_name_patterns(Archive* fout, SimpleStringList* patterns, SimpleOidList* oids, bool isinclude);
static void exclude_error_tables(Archive* fout, SimpleOidList* oids);
static void ExcludeMatRelTables(Archive* fout, SimpleOidList* oids);

static NamespaceInfo* findNamespace(Archive* fout, Oid nsoid, Oid objoid);
static void dumpTableData(Archive* fout, TableDataInfo* tdinfo);
static void guessConstraintInheritance(TableInfo* tblinfo, int numTables);
static void dumpComment(Archive* fout, const char* target, const char* nmspace, const char* owner, CatalogId catalogId,
    int subid, DumpId dumpId);
static int findComments(Archive* fout, Oid classoid, Oid objoid, CommentItem** items);
static int collectComments(Archive* fout, CommentItem** items);
static void dumpSecLabel(Archive* fout, const char* target, const char* nmspace, const char* owner, CatalogId catalogId,
    int subid, DumpId dumpId);
static int findSecLabels(Archive* fout, Oid classoid, Oid objoid, SecLabelItem** items);
static int collectSecLabels(Archive* fout, SecLabelItem** items);
static void dumpDumpableObject(Archive* fout, DumpableObject* dobj);
static void dumpNamespace(Archive* fout, NamespaceInfo* nspinfo);
static void dumpDirectory(Archive* fout);
static void dumpExtension(Archive* fout, ExtensionInfo* extinfo);
static void dumpTableofType(Archive* fout, TypeInfo* tyinfo);
static void dumpType(Archive* fout, TypeInfo* tyinfo);
static void dumpBaseType(Archive* fout, TypeInfo* tyinfo);
static void dumpEnumType(Archive* fout, TypeInfo* tyinfo);
static void dumpRangeType(Archive* fout, TypeInfo* tyinfo);
static void dumpDomain(Archive* fout, TypeInfo* tyinfo);
static void dumpCompositeType(Archive* fout, TypeInfo* tyinfo);
static void dumpCompositeTypeColComments(Archive* fout, TypeInfo* tyinfo);
static void dumpShellType(Archive* fout, ShellTypeInfo* stinfo);
static void dumpProcLang(Archive* fout, ProcLangInfo* plang);
static void dumpFunc(Archive* fout, FuncInfo* finfo);
static void DumpPkgSpec(Archive* fout, PkgInfo* pinfo);
static void DumpPkgBody(Archive* fout, PkgInfo* pinfo);
static void dumpCast(Archive* fout, CastInfo* cast);
static void dumpOpr(Archive* fout, OprInfo* oprinfo);
static void dumpAccessMethod(Archive *fout, const AccessMethodInfo *aminfo);
static void dumpOpclass(Archive* fout, OpclassInfo* opcinfo);
static void dumpOpfamily(Archive* fout, OpfamilyInfo* opfinfo);
static void dumpCollation(Archive* fout, CollInfo* convinfo);
static void dumpConversion(Archive* fout, ConvInfo* convinfo);
static void dumpRule(Archive* fout, RuleInfo* rinfo);
static void dumpEvent(Archive *fout, EventInfo *einfo);
static void dumpRlsPolicy(Archive* fout, RlsPolicyInfo* policyinfo);
static void dumpAgg(Archive* fout, AggInfo* agginfo);
static void dumpTrigger(Archive* fout, TriggerInfo* tginfo);
static void dumpTable(Archive* fout, TableInfo* tbinfo);
static void dumpTableSchema(Archive* fout, TableInfo* tbinfo);
static void dumpAttrDef(Archive* fout, AttrDefInfo* adinfo);
static void dumpSequence(Archive* fout, TableInfo* tbinfo, bool large);
static void dumpSequenceData(Archive* fout, TableDataInfo* tdinfo, bool large);
static void dumpIndex(Archive* fout, IndxInfo* indxinfo);
static void dumpConstraint(Archive* fout, ConstraintInfo* coninfo);
static void dumpConstraintForForeignTbl(Archive* fout, ConstraintInfo* coninfo);
static void dumpTableConstraintComment(Archive* fout, ConstraintInfo* coninfo);
static void dumpTSParser(Archive* fout, TSParserInfo* prsinfo);
static void dumpTSDictionary(Archive* fout, TSDictInfo* dictinfo);
static void dumpTSTemplate(Archive* fout, TSTemplateInfo* tmplinfo);
static void dumpTSConfig(Archive* fout, TSConfigInfo* cfginfo);
static void dumpForeignDataWrapper(Archive* fout, FdwInfo* fdwinfo);
static void dumpForeignServer(Archive* fout, ForeignServerInfo* srvinfo);
static void dumpUserMappings(
    Archive* fout, const char* servername, const char* nmspace, const char* owner, CatalogId catalogId, DumpId dumpId);
static void dumpDefaultACL(Archive* fout, DefaultACLInfo* daclinfo);
static void dumpPublication(Archive *fout, const PublicationInfo *pubinfo);
static void dumpPublicationTable(Archive *fout, const PublicationRelInfo *pubrinfo);
static void dumpSubscription(Archive *fout, const SubscriptionInfo *subinfo);

static void dumpACL(Archive* fout, CatalogId objCatId, DumpId objDumpId, const char* type, const char* name,
    const char* subname, const char* tag, const char* nspname, const char* owner, const char* acls);
static void dumpSynonym(Archive* fout);
static void getDependencies(Archive* fout);
static void BuildArchiveDependencies(Archive* fout);
static void findDumpableDependencies(
    ArchiveHandle* AH, DumpableObject* dobj, DumpId** dependencies, int* nDeps, int* allocDeps);

static DumpableObject* createBoundaryObjects(void);
static void addBoundaryDependencies(DumpableObject** dobjs, int numObjs, DumpableObject* boundaryObjs);

static void getDomainConstraints(Archive* fout, TypeInfo* tyinfo);
static void getTableData(TableInfo* tblinfo, int numTables);
static void makeTableDataInfo(TableInfo* tbinfo, bool oids);
static void getTableDataFKConstraints(void);
static char* format_function_arguments(FuncInfo* finfo, char* funcargs);
static char* format_function_arguments_old(Archive* fout, FuncInfo* finfo, int nallargs, const char** allargtypes,
    const char** argmodes, const char** argnames);
static char* format_function_signature(Archive* fout, FuncInfo* finfo, bool honor_quotes);
static const char* convertRegProcReference(Archive* fout, const char* proc);
static const char* convertOperatorReference(Archive* fout, const char* opr);
static const char* convertTSFunction(Archive* fout, Oid funcOid);
static Oid findLastBuiltinOid_V71(Archive* fout, const char*);
static Oid findLastBuiltinOid_V70(Archive* fout);
static void selectSourceSchema(Archive* fout, const char* schemaName);
static char* getFormattedTypeName(Archive* fout, Oid oid, OidOptions opts);
static char* myFormatType(const char* typname, int32 typmod);
static const char* fmtQualifiedId(Archive* fout, const char* schema, const char* id);
static void getBlobs(Archive* fout);
static void dumpBlob(Archive* fout, BlobInfo* binfo);
static int dumpBlobs(Archive* fout, void* arg);
static void dumpDatabase(Archive* AH);
static void getGrantAnyPrivilegeQuery(Archive* fout, PQExpBuffer grantAnyPrivilegeSql,
    Oid roleid, const char* adminOption);
static void dumpAnyPrivilege(Archive* fout);
#ifdef HAVE_CE
static void dumpClientGlobalKeys(Archive* fout, const Oid nspoid, const char *nspname);
static void dumpColumnEncryptionKeys(Archive* fout, const Oid nspoid, const char *nspname);
#endif
static void dumpEncoding(Archive* AH);
static void dumpStdStrings(Archive* AH);
static void binary_upgrade_set_type_oids_by_type_oid(
    Archive* Afout, PQExpBuffer upgrade_buffer, Oid pg_type_oid, bool error_table);
static bool binary_upgrade_set_type_oids_by_rel_oid(
    Archive* fout, PQExpBuffer upgrade_buffer, Oid pg_rel_oid, bool error_tbl);
static void binary_upgrade_set_pg_class_oids(
    Archive* fout, PQExpBuffer upgrade_buffer, Oid pg_class_oid, bool is_index, bool error_tbl);
static void binary_upgrade_extension_member(PQExpBuffer upgrade_buffer, DumpableObject* dobj, const char* objlabel);
static void binary_upgrade_set_pg_partition_oids(
    Archive* fout, PQExpBuffer upgrade_buffer, TableInfo* tbinfo, IndxInfo* idxinfo, bool is_index);
static void setTableInfoAttNumAndNames(Archive* fout, TableInfo* tblInfo);
static const char* getAttrName(int attrnum, TableInfo* tblInfo);
static const char* fmtCopyColumnList(const TableInfo* ti);
static PGresult* ExecuteSqlQueryForSingleRow(Archive* fout, const char* query);
static PQExpBuffer createTablePartition(Archive* fout, TableInfo* tbinfo);
static void GenerateSubPartitionBy(PQExpBuffer result, Archive *fout, TableInfo *tbinfo);
static void GenerateSubPartitionDetail(PQExpBuffer result, Archive *fout, TableInfo *tbinfo, Oid partOid,
    int subpartkeynum, Oid *subpartkeycols);
static bool isTypeString(const TableInfo* tbinfo, unsigned int colum);
static void find_current_connection_node_type(Archive* fout);
static char* changeTableName(const char* tableName);
static void appendOidList(SimpleOidList* oidList, Oid val);
static bool isIndependentRole(PGconn* conn, char* rolname);
static bool isSuperRole(PGconn* conn, char* rolname);
static bool isExecUserSuperRole(Archive* fout);
static bool isExecUserNotObjectOwner(Archive* fout, const char* objRoleName);
static bool isTsStoreTable(const TableInfo *tbinfo);
static bool IsIncrementalMatview(Archive* fout, Oid matviewOid);
static bool IsPackageObject(Archive* fout, Oid classid, Oid objid);
static Oid* GetMatviewOid(Archive* fout, Oid tableoid, int *nMatviews);
static void DumpMatviewSchema(
    Archive* fout, TableInfo* tbinfo, PQExpBuffer query, PQExpBuffer q, PQExpBuffer delq, PQExpBuffer labelq);
static void DumpDistKeys(PQExpBuffer resultBuf, const TableInfo* tbinfo);
static PQExpBuffer DumpRangeDistribution(Archive* fout, const TableInfo* tbinfo);
static PQExpBuffer DumpListDistribution(Archive* fout, const TableInfo* tbinfo);
static void DumpHashDistribution(PQExpBuffer resultBuf, const TableInfo* tbinfo);
static void get_password_pipeline();
static void get_role_password();
static void get_encrypt_key();
static void dumpEventTrigger(Archive *fout, EventTriggerInfo *evtinfo);
static void dumpUniquePrimaryDef(PQExpBuffer buf, ConstraintInfo* coninfo, IndxInfo* indxinfo, bool isBcompatibility);
static void dumpTableAutoIncrement(Archive* fout, PQExpBuffer sqlbuf, TableInfo* tbinfo);
static bool IsPlainFormat();
static bool needIgnoreSequence(TableInfo* tbinfo);
static void *ProgressReportDump(void *arg);
static void *ProgressReportScanDatabase(void *arg);
inline bool isDB4AIschema(const NamespaceInfo *nspinfo);
#ifdef DUMPSYSLOG
static void ReceiveSyslog(PGconn* conn, const char* current_path);
#endif

#ifdef GSDUMP_LLT
bool lltRunning = true;
void stopLLT()
{
    lltRunning = false;
}
#endif

static void free_dump();

int main(int argc, char** argv)
{
    TableInfo* tblinfo = NULL;
    int numTables;
    DumpableObject** dobjs = NULL;
    int numObjs;
    DumpableObject* boundaryObjs = NULL;
    int i;
    int plainText = 0;
    int my_version;
    int optindex;
    RestoreOptions* ropt = NULL;
    ArchiveFormat archiveFormat = archUnknown;
    ArchiveMode archiveMode;
    Archive* fout = NULL; /* the script file */

    /* Database Security: Data importing/dumping support AES128. */
    struct timeval aes_start_time;
    struct timeval aes_end_time;
    pg_time_t total_time = 0;

    static int disable_triggers = 0;
    static int outputNoTablespaces = 0;
    static int use_setsessauth = 0;
    errno_t rc = 0;
    int nRet = 0;
    char *errorMessages = NULL;

    static struct option long_options[] = {{"data-only", no_argument, NULL, 'a'},
        {"blobs", no_argument, NULL, 'b'},
        {"clean", no_argument, NULL, 'c'},
        {"create", no_argument, NULL, 'C'},
        {"file", required_argument, NULL, 'f'},
        {"format", required_argument, NULL, 'F'},
        {"host", required_argument, NULL, 'h'},
        {"oids", no_argument, NULL, 'o'},
        {"no-owner", no_argument, NULL, 'O'},
        {"port", required_argument, NULL, 'p'},
        {"target", required_argument, NULL, 'q'},
        {"schema", required_argument, NULL, 'n'},
        {"exclude-schema", required_argument, NULL, 'N'},
        {"schema-only", no_argument, NULL, 's'},
        {"sysadmin", required_argument, NULL, 'S'},
        {"table", required_argument, NULL, 't'},
        {"exclude-table", required_argument, NULL, 'T'},
        {"no-password", no_argument, NULL, 'w'},
        {"password", required_argument, NULL, 'W'},
        {"username", required_argument, NULL, 'U'},
        {"verbose", no_argument, NULL, 'v'},
        {"no-privileges", no_argument, NULL, 'x'},
        {"no-acl", no_argument, NULL, 'x'},
        {"compress", required_argument, NULL, 'Z'},
        {"encoding", required_argument, NULL, 'E'},
        {"exclude-guc", required_argument, NULL, 'g'},
        {"help", no_argument, NULL, '?'},
        {"version", no_argument, NULL, 'V'},

        /*
         * the following options don't have an equivalent short option letter
         */
        {"attribute-inserts", no_argument, &column_inserts, 1},
        {"binary-upgrade", no_argument, &binary_upgrade, 1},
        {"non-lock-table", no_argument, &non_Lock_Table, 1},
        {"column-inserts", no_argument, &column_inserts, 1},
        {"disable-dollar-quoting", no_argument, &disable_dollar_quoting, 1},
        {"disable-triggers", no_argument, &disable_triggers, 1},
        {"exclude-table-data", required_argument, NULL, 4},
        {"inserts", no_argument, &dump_inserts, 1},
        {"lock-wait-timeout", required_argument, NULL, 2},
        {"no-tablespaces", no_argument, &outputNoTablespaces, 1},
        {"quote-all-identifiers", no_argument, &quote_all_identifiers, 1},
        {"role", required_argument, NULL, 3},
        {"section", required_argument, NULL, 5},
        {"serializable-deferrable", no_argument, &serializable_deferrable, 1},
        {"use-set-session-authorization", no_argument, &use_setsessauth, 1},
        {"no-security-labels", no_argument, &no_security_labels, 1},
#if !defined(ENABLE_MULTIPLE_NODES)
        {"no-publications", no_argument, &no_publications, 1},
#endif
        {"no-unlogged-table-data", no_argument, &no_unlogged_table_data, 1},
#if !defined(ENABLE_MULTIPLE_NODES)
        {"no-subscriptions", no_argument, &no_subscriptions, 1},
#endif
        {"include-alter-table", no_argument, &include_alter_table, 1},
        {"exclude-self", no_argument, &exclude_self, 1},
        {"include-depend-objs", no_argument, &include_depend_objs, 1},
        {"exclude-with", no_argument, &exclude_with, 1},
        {"exclude-function", no_argument, &exclude_function, 1},
#ifdef DUMPSYSLOG
        {"syslog", no_argument, &dump_syslog, 1},
#endif
        /* Database Security: Data importing/dumping support AES128. */
        {"with-encryption", required_argument, NULL, 6},
        {"with-key", required_argument, NULL, 7},
        {"rolepassword", required_argument, NULL, 9},
#ifdef DUMPSYSLOG
        {"syslogpath", required_argument, NULL, 10},
#endif
#ifdef ENABLE_MULTIPLE_NODES
        {"include-nodes", no_argument, &include_nodes, 1},
#endif
        {"dont-overwrite-file", no_argument, NULL, 11},
        {"with-salt", required_argument, NULL, 12},
        {"binary-upgrade-usermap", required_argument, NULL, 13},
        {"include-extensions", no_argument, NULL, 14},
        {"include-table-file", required_argument, NULL, 15},
        {"exclude-table-file", required_argument, NULL, 16},
        {"pipeline", no_argument, NULL, 17},
#if defined(USE_ASSERT_CHECKING) || defined(FASTCHECK)
        {"disable-progress", no_argument, NULL, 18},
#endif
        {NULL, 0, NULL, 0}};

    set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("gs_dump"));

#ifdef GSDUMP_LLT
    while (lltRunning) {
        sleep(3);
    }
    return 0;
#endif

    g_verbose = false;
    format = gs_strdup("p");
    rc = strncpy_s(g_comment_start, sizeof(g_comment_start) / sizeof(char), "-- ", strlen("-- "));
    securec_check_c(rc, "\0", "\0");

    g_comment_start[strlen("-- ")] = '\0';
    g_comment_end[0] = '\0';
    rc = strncpy_s(g_opaque_type, sizeof(g_opaque_type) / sizeof(char), "opaque", strlen("opaque"));
    securec_check_c(rc, "\0", "\0");
    g_opaque_type[strlen("opaque")] = '\0';

    dataOnly = schemaOnly = false;
    dumpSections = DUMP_UNSECTIONED;
    lockWaitTimeout = NULL;

    progname = get_progname("gs_dump");

    /* Set default options based on progname */
    if (strcmp(progname, "pg_backup") == 0) {
        GS_FREE(format);
        format = gs_strdup("c");
    }

    /* Database Security: Data importing/dumping support AES128. */
    gettimeofday(&aes_start_time, NULL);
    if (argc > 1) {
        if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0) {
            help(progname);
            exit_nicely(0);
        }
        if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0) {
            puts("gs_dump " DEF_GS_VERSION);
            exit_nicely(0);
        }
    }

    /* parse the dumpall options */
    getopt_dump(argc, argv, long_options, &optindex);

    if (is_pipeline) {
        get_password_pipeline();
    }

    // log output redirect
    init_log((char*)PROG_NAME);

    validatedumpoptions();

    /* Identify archive format to emit */
    archiveFormat = parseArchiveFormat(&archiveMode);
    /* archiveFormat specific setup */
    if (archiveFormat == archNull) {
        plainText = 1;
    }

    if (!could_encrypt && is_encrypt) {
        exit_horribly(NULL, "Encrypt mode is only supported for plain text, "
            "You should assign -F p or -F plain.\n");
    }

    /* Custom and directory formats are compressed by default, others not */
    if (compressLevel == -1) {
        if (archiveFormat == archCustom || archiveFormat == archDirectory)
            compressLevel = Z_DEFAULT_COMPRESSION;
        else
            compressLevel = 0;
    } else if (archiveFormat == archNull) {
        exit_horribly(NULL, "Compress mode is not supported for plain text.\n");
    }
    
    /*
	 * If emitting an archive format, we always want to emit a DATABASE item,
	 * in case --create is specified at pg_restore time.
     */
    if (!plainText) {
        outputCreateDB = 1;
    }
    
    // Overwrite  the file if file already exists and overwrite option is specified
    if ((NULL != filename) && (archDirectory != archiveFormat) && (true == dont_overwritefile) &&
        (true == fileExists(filename))) {
        write_msg(NULL,
            "Dump File specified already exists.\n"
            "Perform dump again by: "
            "\n1) Specifying a different file "
            "\n2) Removing the specified file "
            "\n3) Not specifying dont-overwrite-file option\n");

        /* Clear password related memory to avoid leaks when exit. */
        if (pchPasswd != NULL) {
            rc = memset_s(pchPasswd, strlen(pchPasswd), 0, strlen(pchPasswd));
            securec_check_c(rc, "\0", "\0");
        }
        if (rolepasswd != NULL) {
            rc = memset_s(rolepasswd, strlen(rolepasswd), 0, strlen(rolepasswd));
            securec_check_c(rc, "\0", "\0");
        }

        exit_nicely(0);
    }

    /* Open the output file */
    fout = CreateArchive(filename, archiveFormat, compressLevel, archiveMode);

    /* Register the cleanup hook */
    on_exit_close_archive(fout);

    if (NULL != filename) {
        /* Take lock on the file itself on non-directory format and create a
         * lock file on the directory and take lock on that file
         */
        if (archDirectory != archiveFormat) {
            catalog_lock(filename);
            on_exit_nicely(catalog_unlock, NULL);
        } else {
            char* lockFile = NULL;
            int lockFileLen = strlen(filename) + strlen(DIR_SEPARATOR) + strlen(DIR_LOCK_FILE) + 1;
            lockFile = (char*)pg_malloc(lockFileLen);
            if (lockFile == NULL) {
                exit_horribly(NULL, "out of memory.\n");
            }
            nRet = snprintf_s(lockFile, lockFileLen, lockFileLen - 1, "%s/%s", filename, DIR_LOCK_FILE);
            securec_check_ss_c(nRet, lockFile, "\0");
            catalog_lock(lockFile);
            on_exit_nicely(remove_lock_file, lockFile);
            free(lockFile);
            lockFile = NULL;
            on_exit_nicely(catalog_unlock, NULL);
        }
    }

    if (fout == NULL)
        exit_horribly(NULL, "could not open output file \"%s\" for writing\n", filename);

    /* Let the archiver know how noisy to be */
    fout->verbose = g_verbose;

    if ((encrypt_mode != NULL) && (encrypt_key == NULL)) {
        get_encrypt_key();
    }
    /* Database Security: Data importing/dumping support AES128. */
    check_encrypt_parameters(fout, encrypt_mode, encrypt_key);

    if (true == fout->encryptfile) {
        if (NULL == encrypt_salt) {
            GS_UINT32 retval = 0;
            GS_UCHAR init_rand[RANDOM_LEN + 1] = {0};

            /* get a random values as salt for encrypt */
            retval = RAND_priv_bytes(init_rand, RANDOM_LEN);
            if (retval != 1) {
                exit_horribly(NULL, "Generate random key failed\n");
            }

            rc = memset_s(fout->rand, (RANDOM_LEN + 1), 0, RANDOM_LEN + 1);
            securec_check_c(rc, "\0", "\0");
            rc = memcpy_s((GS_UCHAR*)fout->rand, RANDOM_LEN, init_rand, RANDOM_LEN);
            securec_check_c(rc, "\0", "\0");
        } else {
            rc = memset_s(fout->rand, (RANDOM_LEN + 1), 0, RANDOM_LEN + 1);
            securec_check_c(rc, "\0", "\0");
            rc = memcpy_s((GS_UCHAR*)fout->rand, RANDOM_LEN, encrypt_salt, RANDOM_LEN);
            securec_check_c(rc, "\0", "\0");
        }
    }

    my_version = parse_version(PG_VERSION);
    if (my_version < 0)
        exit_horribly(NULL, "could not parse version string \"%s\"\n", PG_VERSION);

    /*
     * We allow the server to be back to 7.0, and up to any minor release of
     * our own major version.  (See also version check in pg_dumpall.c.)
     */
    fout->minRemoteVersion = 90201;
    fout->maxRemoteVersion = (my_version / 100) * 100 + 99;

    ((ArchiveHandle*)fout)->savedPassword = pchPasswd;

    dumpsyslog(fout);

    /*
     * Open the database using the Archiver, so it knows about it. Errors mean
     * death.
     */
    errorMessages = ConnectDatabase(fout, dbname, pghost, pgport, username, prompt_password, false);
    if (errorMessages != NULL) {
        (void)remove(filename);
        GS_FREE(filename);
        exit_horribly(NULL, "connection to database \"%s\" failed: %s ",
            ((dbname != NULL) ? dbname : ""), errorMessages);
    }

#ifndef ENABLE_MULTIPLE_NODES
    /*
     * During gs_dump, PQfnumber() is matched according to the lowercase column name.
     * However, when uppercase_attribute_name is on, the column names in the result set
     * will be converted to uppercase. So we need to turn off it temporarily. We don't
     * need to turn it on cause this connection is for gs_dump only, will not affect others.
     */
    if (!SetUppercaseAttributeNameToOff(((ArchiveHandle*)fout)->connection)) {
        (void)remove(filename);
        GS_FREE(filename);
        exit_horribly(NULL, "%s set uppercase_attribute_name to off failed.\n", progname);
    }
#endif

    fout->workingVersionNum = GetVersionNumFromServer(fout);

    if (CheckIfStandby(fout)) {
        (void)remove(filename);
        exit_horribly(NULL, "%s is not supported on standby or cascade standby\n", progname);
    }
    
    if (schemaOnly) {
        ExecuteSqlStatement(fout, "set enable_hashjoin=off");
        ExecuteSqlStatement(fout, "set enable_mergejoin=off");
        ExecuteSqlStatement(fout, "set enable_indexscan=true");
        ExecuteSqlStatement(fout, "set enable_nestloop=true");
    }
    /* Turn off log collection parameters to improve execution performance */
    ExecuteSqlStatement(fout, "set resource_track_level='none'");
    find_current_connection_node_type(fout);
    // Get the database which will be dumped
    if (NULL == dbname) {
        ArchiveHandle* AH = (ArchiveHandle*)fout;
        dbname = gs_strdup(PQdb(AH->connection));
    }
    if (NULL == instport) {
        ArchiveHandle* AH = (ArchiveHandle*)fout;
        instport = gs_strdup(PQport(AH->connection));
    }

    if ((use_role != NULL) && (rolepasswd == NULL)) {
        get_role_password();
    }

    setup_connection(fout);

    if (!isExecUserSuperRole(fout)) {
        if (use_role != NULL) {
            write_msg(NULL,
                "Notice: options --role is not super or sysadmin role, can only back up objects belonging to user "
                "%s.\n",
                use_role);
        } else {
            write_msg(NULL,
                "Notice: options -U is not super or sysadmin role, can only back up objects belonging to user %s.\n",
                PQuser(((ArchiveHandle*)fout)->connection));
        }
    }

    /*
     * Disable security label support if server version < v9.1.x (prevents
     * access to nonexistent pg_seclabel catalog)
     */
    if (fout->remoteVersion < 90100)
        no_security_labels = 1;

    /*
     * When running against 9.0 or later, check if we are in recovery mode,
     * which means we are on a hot standby.
     */
    if (fout->remoteVersion >= 90000) {
        PGresult* res = ExecuteSqlQueryForSingleRow(fout, (char*)"SELECT pg_catalog.pg_is_in_recovery()");
        if (strcmp(PQgetvalue(res, 0, 0), "t") == 0) {
            /*
             * On hot standby slaves, never try to dump unlogged table data,
             * since it will just throw an error.
             */
            no_unlogged_table_data = true;
        }
        PQclear(res);
    }

    /*
     * Start transaction-snapshot mode transaction to dump consistent data.
     */
    ExecuteSqlStatement(fout, "START TRANSACTION");
    if (fout->remoteVersion >= 90100) {
        if (serializable_deferrable)
            ExecuteSqlStatement(fout,
                "SET TRANSACTION ISOLATION LEVEL "
                "SERIALIZABLE, READ ONLY, DEFERRABLE");
        else
            ExecuteSqlStatement(fout,
                "SET TRANSACTION ISOLATION LEVEL "
                "REPEATABLE READ");
    } else
        ExecuteSqlStatement(fout, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");

    /* Select the appropriate subquery to convert user IDs to names */
    if (fout->remoteVersion >= 80100)
        username_subquery = "SELECT rolname FROM pg_catalog.pg_roles WHERE oid =";
    else if (fout->remoteVersion >= 70300)
        username_subquery = "SELECT usename FROM pg_catalog.pg_user WHERE usesysid =";
    else
        username_subquery = "SELECT usename FROM pg_user WHERE usesysid =";

    /* Find the last built-in OID, if needed */
    if (fout->remoteVersion < 70300) {
        if (fout->remoteVersion >= 70100)
            g_last_builtin_oid = findLastBuiltinOid_V71(fout, PQdb(GetConnection(fout)));
        else
            g_last_builtin_oid = findLastBuiltinOid_V70(fout);
        if (g_verbose)
            write_msg(NULL, "last built-in OID is %u\n", g_last_builtin_oid);
    }

    /* Expand schema selection patterns into OID lists */
    if (schema_include_patterns.head != NULL) {
        expand_schema_name_patterns(fout, &schema_include_patterns, &schema_include_oids, false);
        if (schema_include_oids.head == NULL)
            exit_horribly(NULL, "No matching schemas were found\n");
    }
    expand_schema_name_patterns(fout, &schema_exclude_patterns, &schema_exclude_oids, false);
    /* non-matching exclusion patterns aren't an error */

    /* Expand table selection patterns into OID lists */
    if (table_include_patterns.head != NULL) {
        expand_table_name_patterns(fout, &table_include_patterns, &table_include_oids, false);
        if (table_include_oids.head == NULL)
            exit_horribly(NULL, "No matching tables were found\n");
    }
    expand_table_name_patterns(fout, &table_exclude_patterns, &table_exclude_oids, false);

    expand_table_name_patterns(fout, &tabledata_exclude_patterns, &tabledata_exclude_oids, false);

    /* We don't dump map/log tables */
    ExcludeMatRelTables(fout, &table_exclude_oids);

    if (schemaOnly && binary_upgrade)
        exclude_error_tables(fout, &table_exclude_oids);

    /* non-matching exclusion patterns aren't an error */

    /*
     * Dumping blobs is now default unless we saw an inclusion switch or -s
     * ... but even if we did see one of these, -b turns it back on.
     */
    if (include_everything && !schemaOnly)
        outputBlobs = true;

    /*
     * Now scan the database and create DumpableObject structs for all the
     * objects we intend to dump.
     */
    write_msg(NULL, "Begin scanning database. \n");
    pthread_t progressThread;
    pthread_create(&progressThread, NULL, ProgressReportScanDatabase, NULL);

    tblinfo = getSchemaData(fout, &numTables);

    g_progressFlagScan = true;
    pthread_mutex_lock(&g_mutex);
    pthread_cond_signal(&g_cond);
    pthread_mutex_unlock(&g_mutex);
    pthread_join(progressThread, NULL);

    write_msg(NULL, "Finish scanning database. \n");

    if (fout->remoteVersion < 80400)
        guessConstraintInheritance(tblinfo, numTables);

    if (!schemaOnly) {
        getTableData(tblinfo, numTables);
        if (dataOnly)
            getTableDataFKConstraints();
    }

    if (outputBlobs)
        getBlobs(fout);

    /*
     * Collect dependency data to assist in ordering the objects.
     */
    getDependencies(fout);

    /* Lastly, create dummy objects to represent the section boundaries */
    boundaryObjs = createBoundaryObjects();

    /* Get pointers to all the known DumpableObjects */
    getDumpableObjects(&dobjs, &numObjs);

    /*
     * Add dummy dependencies to enforce the dump section ordering.
     */
    addBoundaryDependencies(dobjs, numObjs, boundaryObjs);

    /*
     * Sort the objects into a safe dump order (no forward references).
     *
     * In 7.3 or later, we can rely on dependency information to help us
     * determine a safe order, so the initial sort is mostly for cosmetic
     * purposes: we sort by name to ensure that logically identical schemas
     * will dump identically.  Before 7.3 we don't have dependencies and we
     * use OID ordering as an (unreliable) guide to creation order.
     */
    if (fout->remoteVersion >= 70300)
        sortDumpableObjectsByTypeName(dobjs, numObjs);
    else
        sortDumpableObjectsByTypeOid(dobjs, numObjs);

    sortDumpableObjects(dobjs, numObjs, boundaryObjs[0].dumpId, boundaryObjs[1].dumpId);

    /*
     * Create archive TOC entries for all the objects to be dumped, in a safe
     * order.
     */

    /* First the special ENCODING and STDSTRINGS entries. */
    dumpEncoding(fout);
    dumpStdStrings(fout);

    /* The database item is always next, unless we don't want it at all */
    if (include_everything && !dataOnly) {
        dumpDatabase(fout);
        dumpDirectory(fout);
    }
    ArchiveHandle* archiveHandle = (ArchiveHandle*)fout;
    const char* sqlCmd = "select * from pg_class where relname = 'gs_db_privilege';";
    if (isExistsSQLResult(archiveHandle->connection, sqlCmd)) {
        dumpAnyPrivilege(fout);
    }
    g_totalObjNums = numObjs;
    write_msg(NULL, "Start dumping objects \n");
    pthread_t progressThreadDumpProgress;
    pthread_create(&progressThreadDumpProgress, NULL, ProgressReportDump, NULL);

    /* Now the rearrangeable objects. */
    for (i = 0; i < numObjs; i++) {
        g_dumpObjNums++;
        dumpDumpableObject(fout, dobjs[i]);
    }
    g_progressFlagDump = true;
    pthread_mutex_lock(&g_mutex);
    pthread_cond_signal(&g_condDump);
    pthread_mutex_unlock(&g_mutex);
    pthread_join(progressThreadDumpProgress, NULL);
    write_msg(NULL, "Finish dumping objects \n");

    /*
     * Synonym now only support for table/view/function/procedure. When dump all object, it will be dumped.
     */
    if (include_everything && !dataOnly) {
        dumpSynonym(fout);
    }

    /*
     * Set up options info to ensure we dump what we want.
     */
    ropt = NewRestoreOptions();
    ropt->filename = filename;
    ropt->dropSchema = outputClean;
    ropt->dataOnly = dataOnly;
    ropt->schemaOnly = schemaOnly;
    ropt->dumpSections = dumpSections;
    ropt->aclsSkip = aclsSkip;
    ropt->superuser = outputSuperuser;
    ropt->targetV1 = targetV1;
    ropt->targetV5 = targetV5;
    ropt->createDB = outputCreateDB;
    ropt->noOwner = outputNoOwner;
    ropt->noTablespace = outputNoTablespaces;
    ropt->disable_triggers = disable_triggers;
    ropt->use_setsessauth = use_setsessauth;
    ropt->no_subscriptions = no_subscriptions;
    ropt->no_publications = no_publications;

    if (compressLevel == -1)
        ropt->compression = 0;
    else
        ropt->compression = compressLevel;

    ropt->suppressDumpWarnings = true; /* We've already shown them */

    SetArchiveRestoreOptions(fout, ropt);

    /*
     * The archive's TOC entries are now marked as to which ones will
     * actually be output, so we can set up their dependency lists properly.
     * This isn't necessary for plain-text output, though.
     */
    if (!plainText)
        BuildArchiveDependencies(fout);

    /*
     * And finally we can do the actual output.
     *
     * Note: for non-plain-text output formats, the output file is written
     * inside CloseArchive().  This is, um, bizarre; but not worth changing
     * right now.
     */
    if (plainText)
        RestoreArchive(fout);

    /* Clear password related memory to avoid leaks when core. */
    if (((ArchiveHandle*)fout)->savedPassword != NULL) {
        rc = memset_s(((ArchiveHandle*)fout)->savedPassword,
            strlen(((ArchiveHandle*)fout)->savedPassword),
            0,
            strlen(((ArchiveHandle*)fout)->savedPassword));
        securec_check_c(rc, "\0", "\0");
    }
    if (NULL != rolepasswd) {
        rc = memset_s(rolepasswd, strlen(rolepasswd), 0, strlen(rolepasswd));
        securec_check_c(rc, "\0", "\0");
    }
    if (pchPasswd != NULL) {
        rc = memset_s(pchPasswd, strlen(pchPasswd), 0, strlen(pchPasswd));
        securec_check_c(rc, "\0", "\0");
        free(pchPasswd);
        pchPasswd = NULL;
    }

    free_dump();

    free((void*)format);
    format = NULL;
    CloseArchive(fout);

    /* After the object is exported, the transaction is ended */
    ExecuteSqlStatement(fout, "COMMIT");

    /*free the memory allocated for gs_restore options */
    free(ropt);
    ropt = NULL;

    encryptArchive(fout, archiveFormat);
    write_msg(NULL, "dump database %s successfully\n", dbname);

    /* Database Security: Data importing/dumping support AES128. */
    gettimeofday(&aes_end_time, NULL);
    total_time =
        1000 * (aes_end_time.tv_sec - aes_start_time.tv_sec) + (aes_end_time.tv_usec - aes_start_time.tv_usec) / 1000;
    write_msg(NULL, "total time: %lld  ms\n", (long long int)total_time);

    exit_nicely(0);
}

static void get_password_pipeline()
{
    const int pass_max_len = 1024;
    char* pass_buf = NULL;
    errno_t rc = EOK;

    if (isatty(fileno(stdin))) {
        exit_horribly(NULL, "Terminal is not allowed to use --pipeline\n");
    }

    pass_buf = (char*)pg_malloc(pass_max_len);
    rc = memset_s(pass_buf, pass_max_len, 0, pass_max_len);
    securec_check_c(rc, "\0", "\0");

    if (NULL != fgets(pass_buf, pass_max_len, stdin)) {
        prompt_password = TRI_YES;
        pass_buf[strlen(pass_buf) - 1] = '\0';
        GS_FREE(pchPasswd);
        pchPasswd = gs_strdup(pass_buf);
    }

    rc = memset_s(pass_buf, pass_max_len, 0, pass_max_len);
    securec_check_c(rc, "\0", "\0");
    free(pass_buf);
    pass_buf = NULL;
}

static void get_role_password() {
    GS_FREE(rolepasswd);
    rolepasswd = simple_prompt("Role Password: ", 100, false);
    if (rolepasswd == NULL) {
        exit_horribly(NULL, "out of memory\n");
    }
}

static void get_encrypt_key()
{
    GS_FREE(encrypt_key);
    encrypt_key = simple_prompt("Encrypt Key: ", MAX_PASSWDLEN, false);
    if (encrypt_key == NULL) {
        exit_horribly(NULL, "out of memory\n");
    }
}

static void free_dump()
{
    GS_FREE(dumpencoding);
    GS_FREE(pghost);
    GS_FREE(pgport);
    GS_FREE(outputSuperuser);
    GS_FREE(username);
    GS_FREE(lockWaitTimeout);
    GS_FREE(use_role);
    GS_FREE(encrypt_mode);
    GS_FREE(encrypt_key);
    GS_FREE(rolepasswd);
    GS_FREE(encrypt_salt);
    GS_FREE(gdatcompatibility);
}

/*
 * make sure the table list type is correct.
 * it like this:
 *    schemaName.tableName or tableName
 */
bool checkTableListType(const char* lineValue)
{
    char delims[] = ".";
    char* tmp = NULL;
    char* ptr = NULL;
    char* outer_ptr = NULL;
    int count = 0;

    tmp = gs_strdup(lineValue);
    ptr = strtok_r(tmp, delims, &outer_ptr);
    while (NULL != ptr) {
        count += 1;
        ptr = strtok_r(NULL, delims, &outer_ptr);
    }
    free(tmp);
    tmp = NULL;

    if (1 == count || 2 == count)
        return true;
    else
        return false;
}

/*
 * Skip the newline character
 */
char* skipNewLineCharacter(const char* lineValue)
{
    char* result = NULL;
    unsigned int i = 0;
    int rc = 0;
    unsigned int len = strlen(lineValue);
    if (len + 1 >= INT_MAX) {
        write_stderr(_("lineValue is too long(%u).\n"), len);
        return NULL;
    }

    result = (char*)pg_malloc((len + 1) * sizeof(char));
    rc = memset_s(result, (len + 1), 0, (len + 1));
    securec_check_c(rc, "\0", "\0");
    for (i = 0; i < len; i++) {
        if ('\n' != lineValue[i])
            result[i] = lineValue[i];
    }
    return result;
}

/*
 * parse the table list, get the include/exclude table list
 */
void parseTableList(char* fileName, SimpleStringList* stringList)
{
    FILE* fp = NULL;
    int c = 0;
    int maxlength = 1;
    int linelen = 0;
    int nlines = 0;
    char** result = NULL;
    char* buffer = NULL;
    int i = 0;
    char* tmp = NULL;

    /* open the table list file */
    if ((fp = fopen(fileName, "r")) == NULL) {
        write_stderr(_("%s: could not open file \"%s\" for reading: %s\n"), progname, fileName, strerror(errno));
        exit_nicely(1);
    }

    /* pass over the file twice - the first time to size the result */
    while ((c = fgetc(fp)) != EOF) {
        linelen++;
        if (c == '\n') {
            nlines++;
            if (linelen > maxlength)
                maxlength = linelen;
            linelen = 0;
        }
    }

    /* handle last line without a terminating newline (yuck) */
    if (linelen)
        nlines++;
    if (linelen > maxlength)
        maxlength = linelen;

    /* set up the result and the line buffer */
    result = (char**)pg_malloc((nlines + 1) * sizeof(char*));
    buffer = (char*)pg_malloc(maxlength + 1);
    /* now reprocess the file and store the lines */
    rewind(fp);
    nlines = 0;
    while (fgets(buffer, maxlength + 1, fp) != NULL)
        result[nlines++] = gs_strdup(buffer);
    free(buffer);
    buffer = NULL;
    result[nlines] = NULL;

    /*
     * parse the line value into table_include_patterns
     */
    for (i = 0; i < nlines + 1; i++) {
        if (NULL != result[i] && '\0' != result[i][0] && '\n' != result[i][0] && checkTableListType(result[i])) {
            /* Skip the newline character */
            tmp = skipNewLineCharacter(result[i]);
            if (tmp == NULL) {
                continue;
            }
            simple_string_list_append(stringList, tmp);
            free(tmp);
            tmp = NULL;
        }
    }

    /* free */
    for (i = 0; i < nlines + 1 && NULL != result[i]; i++) {
        free(result[i]);
        result[i] = NULL;
    }
    free(result);
    result = NULL;
    fclose(fp);
}

static void compress_level_invalid(bool if_compress)
{
    if (if_compress) {
        write_stderr(_("%s: options -Z/--compress should be set between 0 and 9\n"), progname);
        write_stderr(_("Try \"%s --help\" for more information.\n"), progname);
        exit_nicely(1);
    }
}

/* parse the dump options */
void getopt_dump(int argc, char** argv, struct option options[], int* result)
{
    int c = 0;
    char* filepath = NULL;
    char* listFilePath = NULL;
    char* listFileName = NULL;
    bool if_compress = false;

        /* check if a required_argument option has a void argument */
    char optstring[] = "abcCE:f:F:g:h:n:N:oOp:q:RsS:t:T:U:vwW:xZ:";
    for (int i = 0; i < argc; i++) {
        char *optstr = argv[i];
        int is_only_shortbar;
        if (strlen(optstr) == 1) {
            is_only_shortbar = optstr[0] == '-' ? 1 : 0;
        } else {
            is_only_shortbar = 0;
        }
        if (is_only_shortbar) {
            fprintf(stderr, _("%s: The option '-' is not a valid option.\n"), progname);
            exit(1);
        }

        char *oli = strchr(optstring, optstr[1]);
        int is_shortopt_with_space;
        if (oli != NULL && strlen(optstr) >= 1 && strlen(oli) >= 2) {
            is_shortopt_with_space =
                optstr[0] == '-' && oli != NULL && oli[1] == ':' && oli[2] != ':' && optstr[2] == '\0';
        } else {
            is_shortopt_with_space = 0;
        }
        if (is_shortopt_with_space) {
            if (i == argc - 1) {
                fprintf(stderr, _("%s: The option '-%c' requires a parameter.\n"), progname, optstr[1]);
                exit(1);
            }
            
            char *next_optstr = argv[i + 1];
            char *next_oli = strchr(optstring, next_optstr[1]);
            int is_arg_optionform = next_optstr[0] == '-' && next_oli != NULL;
            if (is_arg_optionform) {
                fprintf(stderr, _("%s: The option '-%c' requires a parameter.\n"), progname, optstr[1]);
                exit(1);
            }
        }
    }

    while ((c = getopt_long(argc, argv, "abcCE:f:F:g:h:n:N:oOp:q:RsS:t:T:U:vwW:xZ:", options, result)) != -1) {
        switch (c) {
            case 'a': /* Dump data only */
                dataOnly = true;
                break;

            case 'b': /* Dump blobs */
                outputBlobs = true;
                break;

            case 'c': /* clean (i.e., drop) schema prior to create */
                outputClean = 1;
                break;

            case 'C': /* Create DB */
                outputCreateDB = 1;
                break;

            case 'E': /* Dump encoding */
                GS_FREE(dumpencoding);
                dumpencoding = gs_strdup(optarg);
                break;

            case 'F':
                GS_FREE(format);
                format = gs_strdup(optarg);
                break;

            case 'f':
                GS_FREE(filepath);
                filepath = gs_strdup(optarg);
                check_filepath = true;
                filename = make_absolute_path(filepath);
                free(filepath);
                filepath = NULL;
                break;

            case 'h': /* server host */
                GS_FREE(pghost);
                pghost = gs_strdup(optarg);
                break;

            case 'g': /* exclude guc parameter */
                simple_string_list_append(&exclude_guc, optarg);
                break;
            case 'n': /* include schema(s) */
                simple_string_list_append(&schema_include_patterns, optarg);
                include_everything = false;
                break;

            case 'N': /* exclude schema(s) */
                simple_string_list_append(&schema_exclude_patterns, optarg);
                break;

            case 'o': /* Dump oids */
                oids = true;
                break;

            case 'O': /* Don't reconnect to match owner */
                outputNoOwner = 1;
                break;

            case 'p': /* server port */
                GS_FREE(pgport);
                pgport = gs_strdup(optarg);
                break;
				
            case 'q':
                targetV1 = TARGET_V1;
                targetV5 = TARGET_V5;
                break;

            case 's': /* dump schema only */
                schemaOnly = true;
                break;

            case 'S': /* Username for superuser in plain text output */
                GS_FREE(outputSuperuser);
                outputSuperuser = gs_strdup(optarg);
                break;

            case 't': /* include table(s) */
                simple_string_list_append(&table_include_patterns, optarg);
                gTableCount += 1;
                include_everything = false;
                break;

            case 'T': /* exclude table(s) */
                simple_string_list_append(&table_exclude_patterns, optarg);
                break;

            case 'U':
                GS_FREE(username);
                username = gs_strdup(optarg);
                break;

            case 'v': /* verbose */
                g_verbose = true;
                break;

            case 'w':
                prompt_password = TRI_NO;
                break;

            case 'W':
                prompt_password = TRI_YES;
                GS_FREE(pchPasswd);
                pchPasswd = gs_strdup(optarg);
                replace_password(argc, argv, "-W");
                replace_password(argc, argv, "--password");
                break;

            case 'x': /* skip ACL dump */
                aclsSkip = true;
                break;

            case 'Z': /* Compression Level */
                compressLevel = atoi(optarg);
                if_compress = compressLevel > 9 || compressLevel < 0;
                compress_level_invalid(if_compress);
                break;

            case 0:
                /* This covers the long options. */
                break;

            case 2: /* lock-wait-timeout */
                GS_FREE(lockWaitTimeout);
                lockWaitTimeout = gs_strdup(optarg);
                break;

            case 3: /* SET ROLE */
                GS_FREE(use_role);
                use_role = gs_strdup(optarg);
                break;

            case 4: /* exclude table(s) data */
                simple_string_list_append(&tabledata_exclude_patterns, optarg);
                break;

            case 5: /* section */
                set_dump_section(optarg, &dumpSections);
                break;
            /* Database Security: Data importing/dumping support AES128. */
            case 6: /* AES mode , only AES128 is available */
                GS_FREE(encrypt_mode);
                encrypt_mode = gs_strdup(optarg);
                is_encrypt = true;
                break;
            case 7: /* AES encryption key */
                GS_FREE(encrypt_key);
                encrypt_key = gs_strdup(optarg);
                replace_password(argc, argv, "--with-key");
                is_encrypt = true;
                break;

            case 9: /*ROLE PASSWD*/
                GS_FREE(rolepasswd);
                rolepasswd = gs_strdup(optarg);
                replace_password(argc, argv, "--rolepassword");
                break;

#ifdef DUMPSYSLOG
            case 10:
                GS_FREE(syslogpath);
                syslogpath = gs_strdup(optarg);
                break;
#endif

            case 11:
                dont_overwritefile = true;
                break;

            case 12:
                GS_FREE(encrypt_salt);
                encrypt_salt = gs_strdup(optarg);
                break;

            case 13: {
                char* temp = NULL;
                GS_FREE(binary_upgrade_oldowner);
                binary_upgrade_oldowner = gs_strdup(optarg);
                if (NULL == (temp = strchr(binary_upgrade_oldowner, '='))) {
                    write_stderr(_("%s: Invalid binary upgrade usermap\n"), progname);
                    write_stderr(_("Try \"%s --help\" for more information.\n"), progname);
                    exit_nicely(1);
                } else {
                    temp[0] = '\0';
                    binary_upgrade_newowner = temp + 1;
                }
                break;
            }

            case 14:
                include_extensions = true;
                break;

            case 15: /* include table(s) file */
                /*
                 * Firstly, we make sure the optarg is a file.
                 * Second,  we parse the file , write the value into table_include_patterns
                 */
                GS_FREE(listFilePath);
                GS_FREE(listFileName);
                listFilePath = gs_strdup(optarg);
                listFileName = make_absolute_path(listFilePath);
                if (true == fileExists(listFileName)) {
                    parseTableList(listFileName, &table_include_patterns);
                } else {
                    write_stderr(_("%s: Invalid include-table-file\n"), progname);
                    write_stderr(_("Try \"%s --help\" for more information.\n"), progname);
                    GS_FREE(listFilePath);
                    GS_FREE(listFileName);
                    exit_nicely(1);
                }
                include_everything = false;
                GS_FREE(listFilePath);
                GS_FREE(listFileName);
                break;

            case 16: /* exclude table(s) file */
                GS_FREE(listFilePath);
                GS_FREE(listFileName);
                listFilePath = gs_strdup(optarg);
                listFileName = make_absolute_path(listFilePath);
                if (true == fileExists(listFileName)) {
                    parseTableList(optarg, &table_exclude_patterns);
                } else {
                    write_stderr(_("%s: Invalid exclude-table-file\n"), progname);
                    write_stderr(_("Try \"%s --help\" for more information.\n"), progname);
                    GS_FREE(listFilePath);
                    GS_FREE(listFileName);
                    exit_nicely(1);
                }
                GS_FREE(listFilePath);
                GS_FREE(listFileName);
                break;
            case 17:
                is_pipeline = true;
                break;
#if defined(USE_ASSERT_CHECKING) || defined(FASTCHECK)
            case 18:
                disable_progress = true;
                break;
#endif
            default:
                write_stderr(_("Try \"%s --help\" for more information.\n"), progname);
                exit_nicely(1);
        }
    }

    /* Get database name from command line */
    if (optind < argc)
        dbname = argv[optind++];

    /* Complain if any arguments remain */
    if (optind < argc) {
        write_stderr(_("%s: too many command-line arguments (first is \"%s\")\n"), progname, argv[optind]);
        write_stderr(_("Try \"%s --help\" for more information.\n"), progname);
        exit_nicely(1);
    }
}

void validatedumpoptions()
{
    /* --column-inserts implies --inserts */
    if (column_inserts) {
        dump_inserts = 1;
    }

    if (dataOnly && schemaOnly)
        exit_horribly(NULL, "options -s/--schema-only and -a/--data-only cannot be used together\n");

    if (dataOnly && outputClean)
        exit_horribly(NULL, "options -c/--clean and -a/--data-only cannot be used together\n");

    if (dump_inserts && oids) {
        write_msg(NULL, "options --inserts/--column-inserts and -o/--oids cannot be used together\n");
        write_msg(NULL, "(The INSERT command cannot set OIDs.)\n");
        exit_nicely(1);
    }

    if ((NULL != binary_upgrade_oldowner || NULL != binary_upgrade_newowner) && !binary_upgrade) {
        write_stderr(_("%s: options --binary-upgrade-usermap should be used with --binary-upgrade option\n"), progname);
        write_stderr(_("Try \"%s --help\" for more information.\n"), progname);
        exit_nicely(1);
    }

    if (!include_everything && exclude_self && !include_depend_objs) {
        write_stderr(_("%s: options --exclude-self should be used with --include-depend-objs option.\n"), progname);
        exit_nicely(1);
    }

    if (!include_everything && exclude_self && outputClean) {
        write_stderr(_("%s: options --exclude-self should not be used with -c option.\n"), progname);
        exit_nicely(1);
    }

    if (gTableCount > 100) {
        write_stderr(_("%s: The number of options -t must less than or equal to 100.\n"), progname);
        exit_nicely(1);
    }

    validatedumpformats();
}

/*
 * validatedumpformats
 *
 * this function is a utility function for check the -F value
 * options -F/--format argument must be used with -f/--file together
 * when the -F/--format value is 'c' , 'd', 't'
 *
 */
void validatedumpformats()
{
    if ((pg_strcasecmp(format, "custom") == 0 || pg_strcasecmp(format, "c") == 0) ||
        (pg_strcasecmp(format, "directory") == 0 || pg_strcasecmp(format, "d") == 0) ||
        (pg_strcasecmp(format, "tar") == 0 || pg_strcasecmp(format, "t") == 0)) {
        if (check_filepath == false) {
            write_stderr(_("options -F/--format argument '%c' must be used with -f/--file together\n"), *format);
            write_stderr(_("Try \"%s --help\" for more information.\n"), progname);
            exit_nicely(1);
        }
    }
}

#ifdef ENABLE_UT
void uttest_validatedumpformats(const char* fmt, bool checkFilePath)
{
    progname = "gs_dump";
    format = strdup(fmt);
    check_filepath = checkFilePath;
    validatedumpformats();
}
#endif

void dumpsyslog(Archive* fout)
{
#ifdef DUMPSYSLOG
    PGconn* log_conn = NULL;
    PGresult* log_res = NULL;

    /* whether the logpath is accessable */
    if (1 == dump_syslog) {
        if (NULL == syslogpath) {
            syslogpath = ".";
        } else {
            /* create new directory if not present */
            if (-1 == access(syslogpath, R_OK | W_OK)) {
                if (mkdir(syslogpath, S_IRWXU) < 0) {
                    exit_horribly(NULL, "could not create the specified log path: %s\n", syslogpath);
                    return 0;
                }
            }
        }
        if (-1 == access(syslogpath, R_OK | W_OK)) {
            exit_horribly(NULL, "could not access the specified log path: %s\n", syslogpath);
            return 0;
        }
        log_conn = get_dumplog_conn(dbname, pghost, pgport, username, ((ArchiveHandle*)fout)->savedPassword);

        log_res = PQexec(log_conn, "DUMP_SYSLOG");
        if (PQresultStatus(log_res) != PGRES_COPY_BOTH) {
            PQclear(log_res);
            PQfinish(log_conn);
            exit_horribly(NULL, "get dumpsyslog connect failed, could not send dump_syslog command to server\n");
            return 0;
        }
        PQclear(log_res);
        ReceiveSyslog(log_conn, syslogpath);
        (void)fprintf(stderr, _("Dump syslog finished\n"));
        exit_nicely(0);
    }
#endif
}

void help(const char* pchProgname)
{
    printf(_("%s dumps a database as a text file or to other formats.\n\n"), pchProgname);
    printf(_("Usage:\n"));
    printf(_("  %s [OPTION]... [DBNAME]\n"), pchProgname);

    printf(_("\nGeneral options:\n"));
    printf(_("  -f, --file=FILENAME                         output file or directory name\n"));
    printf(_("  -F, --format=c|d|t|p                        output file format (custom, directory, tar,\n"
             "                                              plain text (default))\n"));
    printf(_("  -v, --verbose                               verbose mode\n"));
    printf(_("  -V, --version                               output version information, then exit\n"));
    printf(_("  -Z, --compress=0-9                          compression level for compressed formats\n"));
    printf(_("  --lock-wait-timeout=TIMEOUT                 fail after waiting TIMEOUT for a table lock\n"));
    printf(_("  -?, --help                                  show this help, then exit\n"));

    printf(_("\nOptions controlling the output content:\n"));
    printf(_("  -a, --data-only                             dump only the data, not the schema\n"));
    printf(_("  -b, --blobs                                 include large objects in dump\n"));
    printf(_("  -c, --clean                                 clean (drop) database objects before recreating\n"));
    printf(_("  -C, --create                                include commands to create database in dump\n"));
    printf(_("  -E, --encoding=ENCODING                     dump the data in encoding ENCODING\n"));
    printf(_("  -g, --exclude-guc=GUC_PARAM                 do NOT dump the GUC_PARAM set\n"));
    printf(_("  -n, --schema=SCHEMA                         dump the named schema(s) only\n"));
    printf(_("  -N, --exclude-schema=SCHEMA                 do NOT dump the named schema(s)\n"));
    printf(_("  -o, --oids                                  include OIDs in dump\n"));
    printf(_("  -O, --no-owner                              skip restoration of object ownership in\n"
             "                                              plain-text format\n"));
    printf(_("  -s, --schema-only                           dump only the schema, no data\n"));
    printf(_("  -q, --target=VERSION                        dump data format can compatible Gaussdb version "
             "(v1 or ..)\n"));
    printf(_("  -S, --sysadmin=NAME                         system admin user name to use in plain-text format\n"));
    printf(_("  -t, --table=TABLE                           dump the named table(s) only\n"));
    printf(_("  -T, --exclude-table=TABLE                   do NOT dump the named table(s)\n"));
    printf(_("  --include-table-file=FileName               dump the named table(s) only\n"));
    printf(_("  --exclude-table-file=FileName               do NOT dump the named table(s)\n"));
    printf(_("  --pipeline                                  use pipeline to pass the password,\n"
             "                                              forbidden to use in terminal\n"));
    printf(_("  -x, --no-privileges/--no-acl                do not dump privileges (grant/revoke)\n"));
    printf(_("  --column-inserts/--attribute-inserts        dump data as INSERT commands with column names\n"));
    printf(_("  --disable-dollar-quoting                    disable dollar quoting, use SQL standard quoting\n"));
    printf(_("  --disable-triggers                          disable triggers during data-only restore\n"));
    printf(_("  --exclude-table-data=TABLE                  do NOT dump data for the named table(s)\n"));
    printf(_("  --exclude-with                              do NOT dump WITH() of table(s)\n"));
    printf(_("  --inserts                                   dump data as INSERT commands, rather than COPY\n"));
#if !defined(ENABLE_MULTIPLE_NODES)
    printf(_("  --no-publications                           do not dump publications\n"));
#endif
    printf(_("  --no-security-labels                        do not dump security label assignments\n"));
#if !defined(ENABLE_MULTIPLE_NODES)
    printf(_("  --no-subscriptions                          do not dump subscriptions\n"));
#endif
    printf(_("  --no-tablespaces                            do not dump tablespace assignments\n"));
    printf(_("  --no-unlogged-table-data                    do not dump unlogged table data\n"));
    printf(_("  --include-alter-table                       dump the table delete column\n"));
    printf(_("  --quote-all-identifiers                     quote all identifiers, even if not key words\n"));
    printf(_("  --section=SECTION                           dump named section (pre-data, data, or post-data)\n"));
    printf(_("  --serializable-deferrable                   wait until the dump can run without anomalies\n"));
    printf(_("  --dont-overwrite-file                       do not overwrite the existing file in case of plain, tar "
             "and custom format\n"));
    printf(_("  --use-set-session-authorization\n"
             "                                              use SET SESSION AUTHORIZATION commands instead of\n"
             "                                              ALTER OWNER commands to set ownership\n"));
    printf(_("  --exclude-function                          do not dump function and procedure\n"));    
    /* Database Security: Data importing/dumping support AES128. */
    printf(_("  --with-encryption=AES128                    dump data is encrypted using AES128\n"));
    printf(_("  --with-key=KEY                              AES128 encryption key, must be 16 bytes in length\n"));
    printf(_("  --with-salt=RANDVALUES                      used by gs_dumpall, pass rand value array\n"));
#ifdef ENABLE_MULTIPLE_NODES
    printf(_("  --include-nodes                             include TO NODE/GROUP clause in the dumped CREATE TABLE "
             "and CREATE FOREIGN TABLE commands.\n"));
#endif
    printf(_("  --include-extensions                        include extensions in dump\n"));

    printf(_("  --binary-upgrade                            for use by upgrade utilities only\n"));
    printf(_(
        "  --binary-upgrade-usermap=\"USER1=USER2\"      to be used only by upgrade utility for mapping usernames\n"));
    printf(_("  --non-lock-table                            for use by OM tools utilities only\n"));
    printf(_("  --include-depend-objs                       dump the object which depends on the input object\n"));
    printf(_("  --exclude-self                              do not dump the input object\n"));

    printf(_("\nConnection options:\n"));
    printf(_("  -h, --host=HOSTNAME                         database server host or socket directory\n"));
    printf(_("  -p, --port=PORT                             database server port number\n"));
    printf(_("  -U, --username=NAME                         connect as specified database user\n"));
    printf(_("  -w, --no-password                           never prompt for password\n"));
    printf(_("  -W, --password=PASSWORD                     the password of specified database user\n"));
    printf(_("  --role=ROLENAME                             do SET ROLE before dump\n"));
    printf(_("  --rolepassword=ROLEPASSWORD                 the password for role\n"));

    printf(_("\nIf no database name is supplied, then the PGDATABASE environment\n"
             "variable value is used.\n\n"));
}

static void setup_connection(Archive* AH)
{
    PGconn* conn = GetConnection(AH);
    const char* std_strings = NULL;
    PGresult* res = NULL;
    ArchiveHandle* AHX = (ArchiveHandle*)AH;
    errno_t rc = 0;

    /* Set the client encoding if requested */
    if (dumpencoding != NULL) {
        if (PQsetClientEncoding(conn, dumpencoding) < 0) {
            /* Clear password related memory to avoid leaks when exit. */
            if (pchPasswd != NULL) {
                rc = memset_s(pchPasswd, strlen(pchPasswd), 0, strlen(pchPasswd));
                securec_check_c(rc, "\0", "\0");
            }
            if (rolepasswd != NULL) {
                rc = memset_s(rolepasswd, strlen(rolepasswd), 0, strlen(rolepasswd));
                securec_check_c(rc, "\0", "\0");
            }
            char* pqpass = PQpass(conn);
            if (pqpass != NULL) {
                rc = memset_s(pqpass, strlen(pqpass), 0, strlen(pqpass));
                securec_check_c(rc, "\0", "\0");
            }
            exit_horribly(NULL, "invalid client encoding \"%s\" specified\n", dumpencoding);
        }
    }

    /*
     * Get the active encoding and the standard_conforming_strings setting, so
     * we know how to escape strings.
     */
    AH->encoding = PQclientEncoding(conn);

    std_strings = PQparameterStatus(conn, "standard_conforming_strings");
    AH->std_strings = ((std_strings != NULL) && strcmp(std_strings, "on") == 0);

    /* Set the role if requested */
    if (NULL != use_role && AH->remoteVersion >= 80100) {
        PQExpBuffer query = createPQExpBuffer();
        char* retrolepasswd = PQescapeLiteral(conn, rolepasswd, strlen(rolepasswd));

        if (retrolepasswd == NULL) {
            /* Clear password related memory to avoid leaks when exit. */
            if (pchPasswd != NULL) {
                rc = memset_s(pchPasswd, strlen(pchPasswd), 0, strlen(pchPasswd));
                securec_check_c(rc, "\0", "\0");
            }
            if (rolepasswd != NULL) {
                rc = memset_s(rolepasswd, strlen(rolepasswd), 0, strlen(rolepasswd));
                securec_check_c(rc, "\0", "\0");
            }
            char* pqpass = PQpass(conn);
            if (pqpass != NULL) {
                rc = memset_s(pqpass, strlen(pqpass), 0, strlen(pqpass));
                securec_check_c(rc, "\0", "\0");
            }

            write_msg(NULL, "Failed to escapes a string for an SQL command: %s\n", PQerrorMessage(conn));
            exit_nicely(1);
        }
        appendPQExpBuffer(query, "SET ROLE %s PASSWORD %s", fmtId(use_role), retrolepasswd);
        rc = memset_s(retrolepasswd, strlen(retrolepasswd), 0, strlen(retrolepasswd));
        securec_check_c(rc, "\0", "\0");
        PQfreemem(retrolepasswd);

        res = PQexec(AHX->connection, query->data);
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            fprintf(stderr, _("%s: query failed: %s"), progname, PQerrorMessage(AHX->connection));
            write_msg(NULL, "query failed: %s", PQerrorMessage(AHX->connection));

            /* Clear password related memory to avoid leaks when exit. */
            if (pchPasswd != NULL) {
                rc = memset_s(pchPasswd, strlen(pchPasswd), 0, strlen(pchPasswd));
                securec_check_c(rc, "\0", "\0");
            }
            if (rolepasswd != NULL) {
                rc = memset_s(rolepasswd, strlen(rolepasswd), 0, strlen(rolepasswd));
                securec_check_c(rc, "\0", "\0");
            }
            char* pqpass = PQpass(conn);
            if (pqpass != NULL) {
                rc = memset_s(pqpass, strlen(pqpass), 0, strlen(pqpass));
                securec_check_c(rc, "\0", "\0");
            }
            if (query->data != NULL) {
                rc = memset_s(query->data, strlen(query->data), 0, strlen(query->data));
                securec_check_c(rc, "\0", "\0");
            }

            PQclear(res);
            destroyPQExpBuffer(query);
            exit_nicely(1);
        }

        PQclear(res);
        /* Clear password related memory to avoid leaks when core. */
        if (query->data != NULL) {
            rc = memset_s(query->data, strlen(query->data), 0, strlen(query->data));
            securec_check_c(rc, "\0", "\0");
        }

        destroyPQExpBuffer(query);
    }

    /* Set the datestyle to ISO to ensure the dump's portability */
    ExecuteSqlStatement(AH, "SET DATESTYLE = ISO");

    /* Likewise, avoid using sql_standard intervalstyle */
    if (AH->remoteVersion >= 80400)
        ExecuteSqlStatement(AH, "SET INTERVALSTYLE = POSTGRES");

    /*
     * If supported, set extra_float_digits so that we can dump float data
     * exactly (given correctly implemented float I/O code, anyway)
     */
    if (AH->remoteVersion >= 90000)
        ExecuteSqlStatement(AH, "SET extra_float_digits TO 3");
    else if (AH->remoteVersion >= 70400)
        ExecuteSqlStatement(AH, "SET extra_float_digits TO 2");

    /*
     * If synchronized scanning is supported, disable it, to prevent
     * unpredictable changes in row ordering across a dump and reload.
     */
    if (AH->remoteVersion >= 80300)
        ExecuteSqlStatement(AH, "SET synchronize_seqscans TO off");

    /*
     * Disable timeouts if supported.
     */
    if (AH->remoteVersion >= 70300)
        ExecuteSqlStatement(AH, "SET statement_timeout = 0");

    /*
     * Quote all identifiers, if requested.
     */
    if (quote_all_identifiers && AH->remoteVersion >= 90100)
        ExecuteSqlStatement(AH, "SET quote_all_identifiers = true");
}

static ArchiveFormat parseArchiveFormat(ArchiveMode* mode)
{
    ArchiveFormat archiveFormat = archUnknown;

    *mode = archModeWrite;

    if (pg_strcasecmp(format, "a") == 0 || pg_strcasecmp(format, "append") == 0) {
        /* This is used by pg_dumpall, and is not documented */
        archiveFormat = archNull;
        *mode = archModeAppend;
        could_encrypt = true;
    } else if (pg_strcasecmp(format, "c") == 0) {
        archiveFormat = archCustom;
    } else if (pg_strcasecmp(format, "custom") == 0) {
        archiveFormat = archCustom;
    } else if (pg_strcasecmp(format, "d") == 0) {
        archiveFormat = archDirectory;
    } else if (pg_strcasecmp(format, "directory") == 0) {
        archiveFormat = archDirectory;
    } else if (pg_strcasecmp(format, "p") == 0) {
        archiveFormat = archNull;
        could_encrypt = true;
    } else if (pg_strcasecmp(format, "plain") == 0) {
        archiveFormat = archNull;
        could_encrypt = true;
    } else if (pg_strcasecmp(format, "t") == 0) {
        archiveFormat = archTar;
    } else if (pg_strcasecmp(format, "tar") == 0) {
        archiveFormat = archTar;
    }
    else {
        exit_horribly(NULL, "invalid output format \"%s\" specified\n", format);
    }
    return archiveFormat;
}

/*
 * Find the OIDs of all schemas matching the given list of patterns,
 * and append them to the given OID list.
 */
static void expand_schema_name_patterns(
    Archive* fout, SimpleStringList* patterns, SimpleOidList* oidlists, bool isinclude)
{
    PQExpBuffer query;
    PGresult* res = NULL;
    SimpleStringListCell* cell = NULL;
    int i = 0;

    if (patterns->head == NULL) {
        return; /* nothing to do */
    }

    if (fout->remoteVersion < 70300) {
        exit_horribly(NULL, "server version must be at least 7.3 to use schema selection switches\n");
    }

    /*
     * The loop below runs multiple SELECTs might sometimes result in
     * duplicate entries in the OID list, but we don't care.
     */
    query = createPQExpBuffer();
    for (cell = patterns->head; cell != NULL; cell = cell->next) {
        appendPQExpBuffer(query, "SELECT oid FROM pg_catalog.pg_namespace n\n");

        processSQLNamePattern(GetConnection(fout), query, cell->val, false, false, NULL, "n.nspname", NULL, NULL);

        res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
        if (PQntuples(res) == 0) {
            write_msg(NULL, "no matching schemas were found for pattern \"%s\"\n", cell->val);
        }

        for (i = 0; i < PQntuples(res); i++) {
            simple_oid_list_append(oidlists, atooid(PQgetvalue(res, i, 0)));
        }

        PQclear(res);
        resetPQExpBuffer(query);
    }

    destroyPQExpBuffer(query);
}

/*
 * Find the OIDs of all tables matching the given list of patterns,
 * and append them to the given OID list.
 */
static void expand_table_name_patterns(
    Archive* fout, SimpleStringList* patterns, SimpleOidList* oidlists, bool isinclude)
{
    PQExpBuffer query;
    PGresult* res = NULL;
    SimpleStringListCell* cell = NULL;
    int i = 0;

    if (patterns->head == NULL) {
        return; /* nothing to do */
    }

    /*
     * this might sometimes result in duplicate entries in the OID list, but
     * we don't care.
     */
    query = createPQExpBuffer();
    for (cell = patterns->head; cell != NULL; cell = cell->next) {
        appendPQExpBuffer(query,
            "SELECT c.oid"
            "\nFROM pg_catalog.pg_class c"
            "\n     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace"
            "\nWHERE c.relkind in ('%c', '%c', '%c', '%c', '%c','%c', '%c', '%c')\n",
            RELKIND_RELATION,
            RELKIND_SEQUENCE,
            RELKIND_LARGE_SEQUENCE,
            RELKIND_VIEW,
            RELKIND_MATVIEW,
            RELKIND_CONTQUERY,
            RELKIND_FOREIGN_TABLE,
            RELKIND_STREAM);

        processSQLNamePattern(GetConnection(fout),
            query,
            cell->val,
            true,
            false,
            "n.nspname",
            "c.relname",
            NULL,
            "pg_catalog.pg_table_is_visible(c.oid)");

        res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
        if (PQntuples(res) == 0) {
            write_msg(NULL, "no matching tables were found for pattern \"%s\"\n", cell->val);
        }

        for (i = 0; i < PQntuples(res); i++) {
            simple_oid_list_append(oidlists, atooid(PQgetvalue(res, i, 0)));
        }

        PQclear(res);
        resetPQExpBuffer(query);
    }

    destroyPQExpBuffer(query);
}

/*
 * We don't dump matview related tables, such as mlog/map tables
 */
static void ExcludeMatRelTables(Archive* fout, SimpleOidList* oidlists)
{
    PQExpBuffer query;
    PGresult* res = NULL;
    int i;

    query = createPQExpBuffer();
    appendPQExpBuffer(query,
        "SELECT c.oid"
        "   FROM pg_catalog.pg_class c"
        "   WHERE c.relkind = 'r'"
        "   AND (c.relname like 'matviewmap\\_%%' OR c.relname like 'mlog\\_%%'\n)");

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    for (i = 0; i < PQntuples(res); i++) {
        simple_oid_list_append(oidlists, atooid(PQgetvalue(res, i, 0)));
    }

    PQclear(res);
    destroyPQExpBuffer(query);
}

static void exclude_error_tables(Archive* fout, SimpleOidList* oidlists)
{
    PQExpBuffer query;
    PGresult* res = NULL;
    int i;

    query = createPQExpBuffer();

    appendPQExpBuffer(query,
        "WITH ft_err_tabs as "
        "    ( SELECT fs.srvname, ft.ftrelid,"
        " ( SELECT option_value"
        " FROM pg_catalog.pg_options_to_table(ft.ftoptions) "
        "    WHERE option_name = 'error_table' ) AS error_table"
        " FROM pg_catalog.pg_foreign_table ft "
        " INNER JOIN pg_catalog.pg_foreign_server fs "
        " ON fs.oid = ft.ftserver"
        "   ) \n"
        "SELECT et.oid as error_table_id"
        "   FROM ft_err_tabs fe"
        "   INNER JOIN pg_catalog.pg_class ft"
        "     ON ft.oid	= fe.ftrelid"
        "   INNER JOIN pg_catalog.pg_class et"
        "     ON et.relnamespace	= ft.relnamespace"
        "   AND et.relname = fe.error_table;");

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    for (i = 0; i < PQntuples(res); i++) {
        simple_oid_list_append(oidlists, atooid(PQgetvalue(res, i, 0)));
    }

    PQclear(res);
    destroyPQExpBuffer(query);
}

/*
 * selectDumpableNamespace: policy-setting subroutine
 *		Mark a nmspace as to be dumped or not
 */
static void selectDumpableNamespace(NamespaceInfo* nsinfo)
{
    /*
     * If specific tables are being dumped, do not dump any complete
     * namespaces. If specific namespaces are being dumped, dump just those
     * namespaces. Otherwise, dump all non-system namespaces.
     */
    if (table_include_oids.head != NULL)
        nsinfo->dobj.dump = false;
    else if (schema_include_oids.head != NULL)
        nsinfo->dobj.dump = simple_oid_list_member(&schema_include_oids, nsinfo->dobj.catId.oid);
    else if (strncmp(nsinfo->dobj.name, "pg_", 3) == 0 || strncmp(nsinfo->dobj.name, "dbe_", 4) == 0 || 
             strcmp(nsinfo->dobj.name, "pkg_util") == 0 || strcmp(nsinfo->dobj.name, "sys") == 0 ||
             strcmp(nsinfo->dobj.name, "cstore") == 0 || strcmp(nsinfo->dobj.name, "snapshot") == 0 ||
             strcmp(nsinfo->dobj.name, "information_schema") == 0 || strcmp(nsinfo->dobj.name, "pkg_service") == 0 ||
             strcmp(nsinfo->dobj.name, "blockchain") == 0 || strcmp(nsinfo->dobj.name, "sqladvisor") == 0 ||
             strcmp(nsinfo->dobj.name, "coverage") == 0
#ifdef ENABLE_MULTIPLE_NODES
             || strcmp(nsinfo->dobj.name, "db4ai") == 0
#endif
    )
        nsinfo->dobj.dump = false;
    else
        nsinfo->dobj.dump = true;

    /*
     * In any case, a nmspace can be excluded by an exclusion switch
     */
    if (nsinfo->dobj.dump && simple_oid_list_member(&schema_exclude_oids, nsinfo->dobj.catId.oid))
        nsinfo->dobj.dump = false;
}

/*
 * get the specified object dependent object and object self
 * then append them to the oid list.
 */
static void getAllDumpObjectOidList(Archive* fout, SimpleOidList* tableIncludeOid, SimpleOidList* dependOidList)
{
    SimpleOidListCell* cell = NULL;

    /* it means that we do not use -f and  --include-table-file options.*/
    if (NULL == tableIncludeOid->head && NULL == dependOidList->head) {
        return;
    }

    for (cell = tableIncludeOid->head; cell != NULL; cell = cell->next) {
        appendOidList(&all_obj_oids, cell->val);
    }

    for (cell = dependOidList->head; cell != NULL; cell = cell->next) {
        appendOidList(&all_obj_oids, cell->val);
    }
}
/*
 * selectDumpableTable: policy-setting subroutine
 *		Mark a table as to be dumped or not
 */
static void selectDumpableTable(Archive* fout, TableInfo* tbinfo)
{
#define INTERNAL_MASK "internal_mask"
    /*
     * If specific tables are being dumped, dump just those tables; else, dump
     * according to the parent nmspace's dump flag.
     */
    if (table_include_oids.head != NULL) {
        /* only dump depend object */
        if (exclude_self && include_depend_objs) {
            tbinfo->dobj.dump = simple_oid_list_member(&depend_obj_oids, tbinfo->dobj.catId.oid);
        }
        /* only dump object self */
        else if (!exclude_self && !include_depend_objs) {
            tbinfo->dobj.dump = simple_oid_list_member(&table_include_oids, tbinfo->dobj.catId.oid);
        }
        /* dump object self and depend object*/
        else if (!exclude_self && include_depend_objs) {
            tbinfo->dobj.dump = simple_oid_list_member(&all_obj_oids, tbinfo->dobj.catId.oid);
        }
    } else {
        tbinfo->dobj.dump = tbinfo->dobj.nmspace->dobj.dump;
    }
    /*
     * In any case, a table can be excluded by an exclusion switch
     */
    if (tbinfo->dobj.dump && simple_oid_list_member(&table_exclude_oids, tbinfo->dobj.catId.oid))
        tbinfo->dobj.dump = false;

    /*
     * skipping column store tables
     */
    if (CSTORE_NAMESPACE == tbinfo->dobj.nmspace->dobj.catId.oid)
        tbinfo->dobj.dump = false;

    /*
     * skipping error tables
     */
    if (NULL != strstr(tbinfo->reloptions, INTERNAL_MASK)) {
        tbinfo->dobj.dump = false;
    }

    if (isExecUserNotObjectOwner(fout, tbinfo->rolname)) {
        tbinfo->dobj.dump = false;
    }
    
    /*
     * skipping recycle bin object
     */
    if (tbinfo->dobj.dump && GetVersionNum(fout) >= USTORE_UPGRADE_VERSION &&
        IsRbObject(fout, tbinfo->dobj.catId.tableoid, tbinfo->dobj.catId.oid, tbinfo->dobj.name)) {
        tbinfo->dobj.dump = false;
        return;
    }
}


/*
 * selectDumpableType: policy-setting subroutine
 *		Mark a type as to be dumped or not
 *
 * If it's a table's rowtype or an autogenerated array type, we also apply a
 * special type code to facilitate sorting into the desired order.	(We don't
 * want to consider those to be ordinary types because that would bring tables
 * up into the datatype part of the dump order.)  We still set the object's
 * dump flag; that's not going to cause the dummy type to be dumped, but we
 * need it so that casts involving such types will be dumped correctly -- see
 * dumpCast.  This means the flag should be set the same as for the underlying
 * object (the table or base type).
 */
static void selectDumpableType(Archive* fout, TypeInfo* tyinfo)
{
    /* skip complex types, except for standalone composite types */
    if (OidIsValid(tyinfo->typrelid) && tyinfo->typrelkind != RELKIND_COMPOSITE_TYPE) {
        TableInfo* tytable = findTableByOid(tyinfo->typrelid);

        tyinfo->dobj.objType = DO_DUMMY_TYPE;
        if (tytable != NULL)
            tyinfo->dobj.dump = tytable->dobj.dump;
        else
            tyinfo->dobj.dump = false;
        return;
    }

    /* skip auto-generated array types */
    if (tyinfo->isArray) {
        tyinfo->dobj.objType = DO_DUMMY_TYPE;

        /*
         * Fall through to set the dump flag; we assume that the subsequent
         * rules will do the same thing as they would for the array's base
         * type.  (We cannot reliably look up the base type here, since
         * getTypes may not have processed it yet.)
         */
    }

    /* dump only types in dumpable namespaces */
    if (!tyinfo->dobj.nmspace->dobj.dump)
        tyinfo->dobj.dump = false;

    /* skip undefined placeholder types */
    else if (!tyinfo->isDefined)
        tyinfo->dobj.dump = false;
    else if (isDB4AIschema(tyinfo->dobj.nmspace))
        tyinfo->dobj.dump = false;
    else
        tyinfo->dobj.dump = true;

    if (tyinfo->dobj.dump && GetVersionNum(fout) >= USTORE_UPGRADE_VERSION &&
        IsRbObject(fout, tyinfo->dobj.catId.tableoid, tyinfo->dobj.catId.oid, tyinfo->dobj.name)) {
        tyinfo->dobj.dump = false;
        return;
    }

    /*
     * Do not dump type defined by package.
     */
    if (tyinfo->dobj.dump && GetVersionNum(fout) >= PACKAGE_ENHANCEMENT &&
        IsPackageObject(fout, tyinfo->dobj.catId.tableoid, tyinfo->dobj.catId.oid)) {
        tyinfo->dobj.dump = false;
        return;
    }
}

/*
 * selectDumpableDefaultACL: policy-setting subroutine
 *		Mark a default ACL as to be dumped or not
 *
 * For per-schema default ACLs, dump if the schema is to be dumped.
 * Otherwise dump if we are dumping "everything".  Note that dataOnly
 * and aclsSkip are checked separately.
 */
static void selectDumpableDefaultACL(DefaultACLInfo* dinfo)
{
    if (dinfo->dobj.nmspace != NULL) {
        if (isDB4AIschema(dinfo->dobj.nmspace))
            dinfo->dobj.dump = false;
        else
            dinfo->dobj.dump = dinfo->dobj.nmspace->dobj.dump;
    } else
        dinfo->dobj.dump = include_everything;
}

/*
 * selectDumpableAccessMethod: policy-setting subroutine
 *      Mark an access method as to be dumped or not
 *
 * Access methods do not belong to any particular namespace.  To identify
 * built-in access methods, we must resort to checking whether the
 * method's OID is in the range reserved for initdb.
 */
static void selectDumpableAccessMethod(AccessMethodInfo *method)
{
    if (method->dobj.catId.oid <= (Oid) FirstNormalObjectId)
        method->dobj.dump = false;
    else
        method->dobj.dump = include_everything;
}

/*
 * selectDumpableExtension: policy-setting subroutine
 *		Mark an extension as to be dumped or not
 *
 * Normally, we dump all extensions, or none of them if include_everything
 * is false (i.e., a --schema or --table switch was given).  However, in
 * binary-upgrade mode it's necessary to skip built-in extensions, since we
 * assume those will already be installed in the target database.  We identify
 * such extensions by their having OIDs in the range reserved for initdb.
 */
static void selectDumpableExtension(ExtensionInfo* extinfo)
{
    if (binary_upgrade && extinfo->dobj.catId.oid < (Oid)FirstNormalObjectId)
        extinfo->dobj.dump = false;
    else
        extinfo->dobj.dump = include_everything;
}

/*
 * selectDumpablePublicationTable: policy-setting subroutine
 *		Mark a publication table as to be dumped or not
 *
 * Publication tables have schemas, but those are ignored in decision making,
 * because publications are only dumped when we are dumping everything.
 */
static void selectDumpablePublicationTable(DumpableObject *dobj)
{
    dobj->dump = include_everything;
}

/*
 * selectDumpableObject: policy-setting subroutine
 *		Mark a generic dumpable object as to be dumped or not
 *
 * Use this only for object types without a special-case routine above.
 */
static void selectDumpableObject(DumpableObject* dobj, Archive* fout = NULL)
{
    /*
     * Default policy is to dump if parent nmspace is dumpable, or always
     * for non-nmspace-associated items.
     */
    if (NULL != dobj->nmspace)
        dobj->dump = dobj->nmspace->dobj.dump;
    else
        dobj->dump = true;

    if (dobj->dump && GetVersionNum(fout) >= USTORE_UPGRADE_VERSION &&
        IsRbObject(fout, dobj->catId.tableoid, dobj->catId.oid, dobj->name)) {
        dobj->dump = false;
        return;
    }
}

/*
 * selectDumpableFuncs: policy-setting subroutine
 * 		Mark a function as to be dumped or not
 *
 * Normally, we dump all extensions, or none of them if dump_include_everything
 * is false. However, DB4AI functions is created in gs_init, no need dump
 */
static void selectDumpableFuncs(FuncInfo *fcinfo, Archive *fout = NULL)
{
    if (isDB4AIschema(fcinfo->dobj.nmspace)) {
        fcinfo->dobj.dump = false;
    } else {
        selectDumpableObject(&(fcinfo->dobj), fout);
    }
}

/*
 * 	Dump a table's contents for loading using the COPY command
 * 	- this routine is called by the Archiver when it wants the table
 * 	  to be dumped.
 */

static int dumpTableData_copy(Archive* fout, void* dcontext)
{
    TableDataInfo* tdinfo = (TableDataInfo*)dcontext;
    TableInfo* tbinfo = tdinfo->tdtable;
    const char* classname = tbinfo->dobj.name;
    const bool hasoids = tbinfo->hasoids;
    const bool boids = tdinfo->oids;
    PQExpBuffer q = createPQExpBuffer();
    PGconn* conn = GetConnection(fout);
    PGresult* res = NULL;
    int ret = 0;
    char* copybuf = NULL;
    const char* column_list = NULL;

    if (g_verbose)
        write_msg(NULL,
            "dumping contents of table \"%s\"\n",
            fmtQualifiedId(fout, tbinfo->dobj.nmspace->dobj.name, tbinfo->dobj.name));

    if (isDB4AIschema(tbinfo->dobj.nmspace) && !isExecUserSuperRole(fout)) {
        write_msg(NULL, "WARNING: schema db4ai not dumped because current user is not a superuser\n");
        destroyPQExpBuffer(q);
        return 1;
    }

    /*
     * Make sure we are in proper schema.  We will qualify the table name
     * below anyway (in case its name conflicts with a pg_catalog table); but
     * this ensures reproducible results in case the table contains regproc,
     * regclass, etc columns.
     */
    selectSourceSchema(fout, tbinfo->dobj.nmspace->dobj.name);

    /*
     * If possible, specify the column list explicitly so that we have no
     * possibility of retrieving data in the wrong column order.  (The default
     * column ordering of COPY will not be what we want in certain corner
     * cases involving ADD COLUMN and inheritance.)
     */
    if (fout->remoteVersion >= 70300)
        column_list = fmtCopyColumnList(tbinfo);
    else
        column_list = ""; /* can't select columns in COPY */

    if (boids && hasoids) {
        appendPQExpBuffer(q,
            "COPY %s %s WITH OIDS TO stdout;",
            fmtQualifiedId(fout, tbinfo->dobj.nmspace->dobj.name, classname),
            column_list);
    } else if (NULL != tdinfo->filtercond || tdinfo->tdtable->isMOT) {
        /* Note: this syntax is only supported in 8.2 and up */
        appendPQExpBufferStr(q, "COPY (SELECT ");
        /* klugery to get rid of parens in column list */
        if (strlen(column_list) > 2) {
            appendPQExpBufferStr(q, column_list + 1);
            q->data[q->len - 1] = ' ';
        } else {
            appendPQExpBufferStr(q, "* ");
        }
        if (tdinfo->filtercond) {
            appendPQExpBuffer(q,
                "FROM %s %s) TO stdout;",
                fmtQualifiedId(fout, tbinfo->dobj.nmspace->dobj.name, classname),
                tdinfo->filtercond);
        } else {
            appendPQExpBuffer(q,
                "FROM %s) TO stdout;",
                fmtQualifiedId(fout, tbinfo->dobj.nmspace->dobj.name, classname));
        }
    } else {
        appendPQExpBuffer(
            q, "COPY %s %s TO stdout;", fmtQualifiedId(fout, tbinfo->dobj.nmspace->dobj.name, classname), column_list);
    }
    res = ExecuteSqlQuery(fout, q->data, PGRES_COPY_OUT);
    PQclear(res);

    for (;;) {
        ret = PQgetCopyData(conn, &copybuf, 0);

        if (ret < 0)
            break; /* done or error */

        if (NULL != copybuf) {
            size_t writeBytes = 0;
            writeBytes = WriteData(fout, copybuf, ret);
            if (writeBytes != (size_t)ret) {
                write_msg(NULL, "could not write to output file: %s\n", strerror(errno));
                exit_nicely(1);
            }

            PQfreemem(copybuf);
        }

        /* ----------
         * THROTTLE:
         *
         * There was considerable discussion in late July, 2000 regarding
         * slowing down pg_dump when backing up large tables. Users with both
         * slow & fast (multi-processor) machines experienced performance
         * degradation when doing a backup.
         *
         * Initial attempts based on sleeping for a number of ms for each ms
         * of work were deemed too complex, then a simple 'sleep in each loop'
         * implementation was suggested. The latter failed because the loop
         * was too tight. Finally, the following was implemented:
         *
         * If throttle is non-zero, then
         *		See how long since the last sleep.
         *		Work out how long to sleep (based on ratio).
         *		If sleep is more than 100ms, then
         *			sleep
         *			reset timer
         *		EndIf
         * EndIf
         *
         * where the throttle value was the number of ms to sleep per ms of
         * work. The calculation was done in each loop.
         *
         * Most of the hard work is done in the backend, and this solution
         * still did not work particularly well: on slow machines, the ratio
         * was 50:1, and on medium paced machines, 1:1, and on fast
         * multi-processor machines, it had little or no effect, for reasons
         * that were unclear.
         *
         * Further discussion ensued, and the proposal was dropped.
         *
         * For those people who want this feature, it can be implemented using
         * gettimeofday in each loop, calculating the time since last sleep,
         * multiplying that by the sleep ratio, then if the result is more
         * than a preset 'minimum sleep time' (say 100ms), call the 'select'
         * function to sleep for a subsecond period ie.
         *
         * select(0, NULL, NULL, NULL, &tvi);
         *
         * This will return after the interval specified in the structure tvi.
         * Finally, call gettimeofday again to save the 'last sleep time'.
         * ----------
         */
    }

    archprintf(fout, "\\.\n;\n\n");

    if (ret == -2) {
        /* copy data transfer failed */
        write_msg(NULL, "Dumping the contents of table \"%s\" failed: PQgetCopyData() failed.\n", classname);
        write_msg(NULL, "Error message from server: %s", PQerrorMessage(conn));
        write_msg(NULL, "The command was: %s\n", q->data);
        exit_nicely(1);
    }

    /* Check command status and return to normal libpq state */
    res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        write_msg(NULL, "Dumping the contents of table \"%s\" failed: PQgetResult() failed.\n", classname);
        write_msg(NULL, "Error message from server: %s", PQerrorMessage(conn));
        write_msg(NULL, "The command was: %s\n", q->data);
        exit_nicely(1);
    }
    PQclear(res);

    destroyPQExpBuffer(q);
    return 1;
}

#ifdef ENABLE_UT
int uttest_dumpTableData_copy(Archive* fout, void* dcontext)
{
    return dumpTableData_copy(fout, dcontext);
}
#endif

/*
 * Dump table data using INSERT commands.
 *
 * Caution: when we restore from an archive file direct to database, the
 * INSERT commands emitted by this function have to be parsed by
 * pg_backup_db.c's ExecuteInsertCommands(), which will not handle comments,
 * E'' strings, or dollar-quoted strings.  So don't emit anything like that.
 */
static int dumpTableData_insert(Archive* fout, void* dcontext)
{
    TableDataInfo* tdinfo = (TableDataInfo*)dcontext;
    TableInfo* tbinfo = tdinfo->tdtable;
    const char* classname = tbinfo->dobj.name;
    char* pfname = NULL;
    PQExpBuffer q = createPQExpBuffer();
    PGresult* res = NULL;
    int tuple;
    int nfields;
    int field;

    if (isDB4AIschema(tbinfo->dobj.nmspace) && !isExecUserSuperRole(fout)) {
        write_msg(NULL, "WARNING: schema db4ai not dumped because current user is not a superuser\n");
        destroyPQExpBuffer(q);
        return 1;
    }
    /*
     * Make sure we are in proper schema.  We will qualify the table name
     * below anyway (in case its name conflicts with a pg_catalog table); but
     * this ensures reproducible results in case the table contains regproc,
     * regclass, etc columns.
     */
    selectSourceSchema(fout, tbinfo->dobj.nmspace->dobj.name);

    if (fout->remoteVersion >= 70100) {
        /*syntax changed from CURSOR declaration */
        appendPQExpBuffer(q,
            "CURSOR _pg_dump_cursor FOR "
            "SELECT * FROM ONLY (%s)",
            fmtQualifiedId(fout, tbinfo->dobj.nmspace->dobj.name, classname));
    } else {
        appendPQExpBuffer(q,
            "DECLARE _pg_dump_cursor CURSOR FOR "
            "SELECT * FROM %s",
            fmtQualifiedId(fout, tbinfo->dobj.nmspace->dobj.name, classname));
    }
    if (NULL != tdinfo->filtercond)
        appendPQExpBuffer(q, " %s", tdinfo->filtercond);

    ExecuteSqlStatement(fout, q->data);

    while (1) {
        res = ExecuteSqlQuery(fout, "FETCH 100 FROM _pg_dump_cursor", PGRES_TUPLES_OK);
        nfields = PQnfields(res);
        for (tuple = 0; tuple < PQntuples(res); tuple++) {
            archprintf(fout, "INSERT INTO %s ", fmtId(classname));
            if (nfields == 0) {
                /* corner case for zero-column table */
                archprintf(fout, "DEFAULT VALUES;\n");
                continue;
            }
            if (column_inserts) {
                resetPQExpBuffer(q);
                appendPQExpBuffer(q, "(");
                for (field = 0; field < nfields; field++) {
                    if (field > 0)
                        appendPQExpBuffer(q, ", ");
                    pfname = PQfname(res, field);
                    if (NULL == pfname) {
                        continue;
                    }
                    appendPQExpBufferStr(q, fmtId(pfname));
                }
                appendPQExpBuffer(q, ") ");
                archputs(q->data, fout);
            }
            archprintf(fout, "VALUES (");
            for (field = 0; field < nfields; field++) {
                if (field > 0)
                    archprintf(fout, ", ");
                if (tbinfo->attrdefs[field] &&
                    tbinfo->attrdefs[field]->generatedCol) {
                    (void)archputs("DEFAULT", fout);
                    continue;
                }
                if (PQgetisnull(res, tuple, field)) {
                    archprintf(fout, "NULL");
                    continue;
                }

                /* XXX This code is partially duplicated in ruleutils.c */
                switch (PQftype(res, field)) {
                    case INT2OID:
                    case INT4OID:
                    case INT8OID:
                    case OIDOID:
                    case FLOAT4OID:
                    case FLOAT8OID:
                    case NUMERICOID: {
                        /*
                         * These types are printed without quotes unless
                         * they contain values that aren't accepted by the
                         * scanner unquoted (e.g., 'NaN').	Note that
                         * strtod() and friends might accept NaN, so we
                         * can't use that to test.
                         *
                         * In reality we only need to defend against
                         * infinity and NaN, so we need not get too crazy
                         * about pattern matching here.
                         */
                        const char* s = PQgetvalue(res, tuple, field);

                        if (strspn(s, "0123456789 +-eE.") == strlen(s))
                            archprintf(fout, "%s", s);
                        else
                            archprintf(fout, "'%s'", s);
                    } break;

                    case BITOID:
                    case VARBITOID:
                        archprintf(fout, "B'%s'", PQgetvalue(res, tuple, field));
                        break;

                    case BOOLOID:
                        if (strcmp(PQgetvalue(res, tuple, field), "t") == 0)
                            archprintf(fout, "true");
                        else
                            archprintf(fout, "false");
                        break;

                    default:
                        /* All other types are printed as string literals. */
                        resetPQExpBuffer(q);
                        appendStringLiteralAH(q, PQgetvalue(res, tuple, field), fout);
                        archputs(q->data, fout);
                        break;
                }
            }
            archprintf(fout, ");\n");
        }

        if (PQntuples(res) <= 0) {
            PQclear(res);
            break;
        }
        PQclear(res);
    }

    archprintf(fout, "\n\n");

    ExecuteSqlStatement(fout, "CLOSE _pg_dump_cursor");

    destroyPQExpBuffer(q);
    return 1;
}

/*
 * dumpTableData -
 *	  dump the contents of a single table
 *
 * Actually, this just makes an ArchiveEntry for the table contents.
 */
static void dumpTableData(Archive* fout, TableDataInfo* tdinfo)
{
    TableInfo* tbinfo = tdinfo->tdtable;
    PQExpBuffer copyBuf = createPQExpBuffer();
    DataDumperPtr dumpFn = NULL;
    char* copyStmt = NULL;

    if (!dump_inserts) {
        /* Dump/restore using COPY */
        dumpFn = dumpTableData_copy;
        /* must use 2 steps here 'cause fmtId is nonreentrant */
        appendPQExpBuffer(copyBuf, "COPY %s.%s ", tbinfo->dobj.nmspace->dobj.name, fmtId(tbinfo->dobj.name));
        appendPQExpBuffer(copyBuf,
            "%s %sFROM stdin;\n",
            fmtCopyColumnList(tbinfo),
            (tdinfo->oids && tbinfo->hasoids) ? "WITH OIDS " : "");
        copyStmt = copyBuf->data;
    } else {
        /* Restore using INSERT */
        dumpFn = dumpTableData_insert;
        copyStmt = NULL;
    }

    /*
     * Note: although the TableDataInfo is a full DumpableObject, we treat its
     * dependency on its table as "special" and pass it to ArchiveEntry now.
     * See comments for BuildArchiveDependencies.
     */
    ArchiveEntry(fout,
        tdinfo->dobj.catId,
        tdinfo->dobj.dumpId,
        tbinfo->dobj.name,
        tbinfo->dobj.nmspace->dobj.name,
        NULL,
        tbinfo->rolname,
        false,
        "TABLE DATA",
        SECTION_DATA,
        "",
        "",
        copyStmt,
        &(tbinfo->dobj.dumpId),
        1,
        dumpFn,
        tdinfo);

    destroyPQExpBuffer(copyBuf);
}

/*
 * getTableData -
 *	  set up dumpable objects representing the contents of tables
 */
static void getTableData(TableInfo* tblinfo, int numTables)
{
    int i;

    for (i = 0; i < numTables; i++) {
        if (tblinfo[i].dobj.dump)
            makeTableDataInfo(&(tblinfo[i]), oids);
    }
}

/*
 * Make a dumpable object for the data of this specific table
 *
 * Note: we make a TableDataInfo if and only if we are going to dump the
 * table data; the "dump" flag in such objects isn't used.
 */
static void makeTableDataInfo(TableInfo* tbinfo, bool boids)
{
    TableDataInfo* tdinfo = NULL;

    /*
     * Nothing to do if we already decided to dump the table.  This will
     * happen for "config" tables.
     */
    if (tbinfo->dataObj != NULL)
        return;

    /* Skip VIEWs (no data to dump) */
    if (tbinfo->relkind == RELKIND_VIEW || tbinfo->relkind == RELKIND_CONTQUERY)
        return;

    /* Skip Materialized Views' data */
    if (tbinfo->relkind == RELKIND_MATVIEW)
        return;

    /* Skip FOREIGN TABLEs except MOT type or STREAMs (no data to dump) */
    if ((tbinfo->relkind == RELKIND_FOREIGN_TABLE && !tbinfo->isMOT) || tbinfo->relkind == RELKIND_STREAM)
        return;

    /* Don't dump data in unlogged tables, if so requested */
    if (tbinfo->relpersistence == RELPERSISTENCE_UNLOGGED && no_unlogged_table_data)
        return;

    /* Don't dump data in global temp table/sequence */
    if (tbinfo->relpersistence == RELPERSISTENCE_GLOBAL_TEMP)
        return;

    /* Check that the data is not explicitly excluded */
    if (simple_oid_list_member(&tabledata_exclude_oids, tbinfo->dobj.catId.oid))
        return;

    /* OK, let's dump it */
    tdinfo = (TableDataInfo*)pg_malloc(sizeof(TableDataInfo));

    tdinfo->dobj.objType = DO_TABLE_DATA;

    /*
     * Note: use tableoid 0 so that this object won't be mistaken for
     * something that pg_depend entries apply to.
     */
    tdinfo->dobj.catId.tableoid = 0;
    tdinfo->dobj.catId.oid = tbinfo->dobj.catId.oid;
    AssignDumpId(&tdinfo->dobj);
    tdinfo->dobj.name = tbinfo->dobj.name;
    tdinfo->dobj.nmspace = tbinfo->dobj.nmspace;
    tdinfo->tdtable = tbinfo;
    tdinfo->oids = boids;
    tdinfo->filtercond = NULL; /* might get set later */
    addObjectDependency(&tdinfo->dobj, tbinfo->dobj.dumpId);

    tbinfo->dataObj = tdinfo;
}

/*
 * getTableDataFKConstraints -
 *	  add dump-order dependencies reflecting foreign key constraints
 *
 * This code is executed only in a data-only dump --- in schema+data dumps
 * we handle foreign key issues by not creating the FK constraints until
 * after the data is loaded.  In a data-only dump, however, we want to
 * order the table data objects in such a way that a table's referenced
 * tables are restored first.  (In the presence of circular references or
 * self-references this may be impossible; we'll detect and complain about
 * that during the dependency sorting step.)
 */
static void getTableDataFKConstraints(void)
{
    DumpableObject** dobjs;
    int numObjs;
    int i;

    /* Search through all the dumpable objects for FK constraints */
    getDumpableObjects(&dobjs, &numObjs);
    for (i = 0; i < numObjs; i++) {
        if (dobjs[i]->objType == DO_FK_CONSTRAINT) {
            ConstraintInfo* cinfo = (ConstraintInfo*)dobjs[i];
            TableInfo* ftable = NULL;

            /* Not interesting unless both tables are to be dumped */
            if (cinfo->contable == NULL || cinfo->contable->dataObj == NULL)
                continue;
            ftable = findTableByOid(cinfo->confrelid);
            if (ftable == NULL || ftable->dataObj == NULL)
                continue;

            /*
             * Okay, make referencing table's TABLE_DATA object depend on the
             * referenced table's TABLE_DATA object.
             */
            addObjectDependency(&cinfo->contable->dataObj->dobj, ftable->dataObj->dobj.dumpId);
        }
    }
    free(dobjs);
    dobjs = NULL;
}

/*
 * guessConstraintInheritance:
 *	In pre-8.4 databases, we can't tell for certain which constraints
 *	are inherited.	We assume a CHECK constraint is inherited if its name
 *	matches the name of any constraint in the parent.  Originally this code
 *	tried to compare the expression texts, but that can fail for various
 *	reasons --- for example, if the parent and child tables are in different
 *	schemas, reverse-listing of function calls may produce different text
 *	(schema-qualified or not) depending on search path.
 *
 *	In 8.4 and up we can rely on the conislocal field to decide which
 *	constraints must be dumped; much safer.
 *
 *	This function assumes all conislocal flags were initialized to TRUE.
 *	It clears the flag on anything that seems to be inherited.
 */
static void guessConstraintInheritance(TableInfo* tblinfo, int numTables)
{
    int i = 0;
    int j = 0;
    int k = 0;

    for (i = 0; i < numTables; i++) {
        TableInfo* tbinfo = &(tblinfo[i]);
        int numParents;
        TableInfo** parents;
        TableInfo* parent = NULL;

        /* Sequences and views never have parents */
        if (tbinfo->relkind == RELKIND_SEQUENCE || tbinfo->relkind == RELKIND_VIEW || 
            tbinfo->relkind == RELKIND_CONTQUERY || tbinfo->relkind == RELKIND_LARGE_SEQUENCE)
            continue;

        /* Don't bother computing anything for non-target tables, either */
        if (!tbinfo->dobj.dump)
            continue;

        numParents = tbinfo->numParents;
        parents = tbinfo->parents;

        if (numParents == 0)
            continue; /* nothing to see here, move along */

        /* scan for inherited CHECK constraints */
        for (j = 0; j < tbinfo->ncheck; j++) {
            ConstraintInfo* constr = NULL;

            constr = &(tbinfo->checkexprs[j]);

            for (k = 0; k < numParents; k++) {
                int l;

                parent = parents[k];
                for (l = 0; l < parent->ncheck; l++) {
                    ConstraintInfo* pconstr = &(parent->checkexprs[l]);

                    if (strcmp(pconstr->dobj.name, constr->dobj.name) == 0) {
                        constr->conislocal = false;
                        break;
                    }
                }
                if (!constr->conislocal)
                    break;
            }
        }
    }
}

/*
 * getDbAlterConfigCommand:
 */

char* getDbAlterConfigCommand(
    PGconn* conn, const char* arrayitem, const char* type, const char* name, const char* type2, const char* name2)
{
    char* pos = NULL;
    char* mine = NULL;
    PQExpBuffer buf = NULL;
    char* result = NULL;

    mine = gs_strdup(arrayitem);
    pos = strchr(mine, '=');
    if (pos == NULL) {
        free(mine);
        mine = NULL;
        return NULL;
    }

    buf = createPQExpBuffer();
    *pos = 0;
    appendPQExpBuffer(buf, "ALTER %s %s ", type, fmtId(name));
    if (type2 != NULL && name2 != NULL) {
        appendPQExpBuffer(buf, "IN %s %s ", type2, fmtId(name2));
    }
    appendPQExpBuffer(buf, "SET %s TO ", fmtId(mine));

    /*
     * Some GUC variable names are 'LIST' type and hence must not be quoted.
     */
    if (pg_strcasecmp(mine, "u_sess->time_cxt.DateStyle") == 0 || pg_strcasecmp(mine, "search_path") == 0) {
        appendPQExpBuffer(buf, "%s", pos + 1);
    } else {
        appendStringLiteralConn(buf, pos + 1, conn);
    }
    appendPQExpBuffer(buf, ";\n");

    result = gs_strdup(buf->data);

    destroyPQExpBuffer(buf);
    free(mine);
    mine = NULL;

    return result;
}

/*
 * dumpDatabase:
 *	dump the database definition
 */
static void dumpDatabase(Archive* fout)
{
    PQExpBuffer dbQry = createPQExpBuffer();
    PQExpBuffer delQry = createPQExpBuffer();
    PQExpBuffer creaQry = createPQExpBuffer();
    PGconn* conn = GetConnection(fout);
    PGresult* res = NULL;
    int i_tableoid = 0;
    int i_oid = 0;
    int i_dba = 0;
    int i_encoding = 0;
    int i_collate = 0;
    int i_ctype = 0;
    int i_frozenxid = 0;
    int i_frozenxid64 = 0;
    int i_tablespace = 0;
    int i_connlimit = 0;
    int i_datcompatibility = 0;
    CatalogId dbCatId;
    DumpId dbDumpId;
    const char* datname = NULL;
    const char* dba = NULL;
    const char* encoding = NULL;
    const char* collate = NULL;
    const char* ctype = NULL;
    const char* tablespace = NULL;
    const char* datcompatibility = NULL;
    uint32 frozenxid = 0;
    uint64 frozenxid64 = 0;
    int dataConnLimit = 0;
    bool isHasDatcompatibility = true;
    ArchiveHandle* AH = (ArchiveHandle*)fout;
    bool isHasDatfrozenxid64 = false;

    ddr_Assert(fout != NULL);
    datname = PQdb(conn);
    if (NULL == datname) {
        destroyPQExpBuffer(dbQry);
        destroyPQExpBuffer(delQry);
        destroyPQExpBuffer(creaQry);
        return;
    }

    if (g_verbose)
        write_msg(NULL, "saving database definition\n");

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, "pg_catalog");

    isHasDatcompatibility = is_column_exists(AH->connection, DatabaseRelationId, "datcompatibility");
    isHasDatfrozenxid64 = is_column_exists(AH->connection, DatabaseRelationId, "datfrozenxid64");
    /* Get the database owner and parameters from pg_database */
    if (fout->remoteVersion >= 80400) {
        if (isHasDatcompatibility) {
            appendPQExpBuffer(dbQry,
                "SELECT tableoid, oid, "
                "(%s datdba) AS dba, "
                "pg_catalog.pg_encoding_to_char(encoding) AS encoding, "
                "datcollate, datctype, datfrozenxid, %s, datconnlimit, "
                "(SELECT spcname FROM pg_tablespace t WHERE t.oid = dattablespace) AS tablespace, "
                "pg_catalog.shobj_description(oid, 'pg_database') AS description, "
                "datcompatibility "
                "FROM pg_database "
                "WHERE datname = ",
                username_subquery,
                isHasDatfrozenxid64 ? "datfrozenxid64" : "0 AS datfrozenxid64");
        } else {
            appendPQExpBuffer(dbQry,
                "SELECT tableoid, oid, "
                "(%s datdba) AS dba, "
                "pg_catalog.pg_encoding_to_char(encoding) AS encoding, "
                "datcollate, datctype, datfrozenxid, %s, datconnlimit, "
                "(SELECT spcname FROM pg_tablespace t WHERE t.oid = dattablespace) AS tablespace, "
                "pg_catalog.shobj_description(oid, 'pg_database') AS description "
                "FROM pg_database "
                "WHERE datname = ",
                username_subquery,
                isHasDatfrozenxid64 ? "datfrozenxid64" : "0 AS datfrozenxid64");
        }
        appendStringLiteralAH(dbQry, datname, fout);
    } else if (fout->remoteVersion >= 80200) {
        appendPQExpBuffer(dbQry,
            "SELECT tableoid, oid, "
            "(%s datdba) AS dba, "
            "pg_catalog.pg_encoding_to_char(encoding) AS encoding, "
            "NULL AS datcollate, NULL AS datctype, datfrozenxid"
            "(SELECT spcname FROM pg_tablespace t WHERE t.oid = dattablespace) AS tablespace, "
            "pg_catalog.shobj_description(oid, 'pg_database') AS description "

            "FROM pg_database "
            "WHERE datname = ",
            username_subquery);
        appendStringLiteralAH(dbQry, datname, fout);
    } else if (fout->remoteVersion >= 80000) {
        appendPQExpBuffer(dbQry,
            "SELECT tableoid, oid, "
            "(%s datdba) AS dba, "
            "pg_catalog.pg_encoding_to_char(encoding) AS encoding, "
            "NULL AS datcollate, NULL AS datctype, datfrozenxid"
            "(SELECT spcname FROM pg_tablespace t WHERE t.oid = dattablespace) AS tablespace "
            "FROM pg_database "
            "WHERE datname = ",
            username_subquery);
        appendStringLiteralAH(dbQry, datname, fout);
    } else if (fout->remoteVersion >= 70100) {
        appendPQExpBuffer(dbQry,
            "SELECT tableoid, oid, "
            "(%s datdba) AS dba, "
            "pg_catalog.pg_encoding_to_char(encoding) AS encoding, "
            "NULL AS datcollate, NULL AS datctype, "
            "0 AS datfrozenxid, "
            "NULL AS tablespace "
            "FROM pg_database "
            "WHERE datname = ",
            username_subquery);
        appendStringLiteralAH(dbQry, datname, fout);
    } else {
        appendPQExpBuffer(dbQry,
            "SELECT "
            "(SELECT oid FROM pg_class WHERE relname = 'pg_database') AS tableoid, "
            "oid, "
            "(%s datdba) AS dba, "
            "pg_catalog.pg_encoding_to_char(encoding) AS encoding, "
            "NULL AS datcollate, NULL AS datctype, "
            "0 AS datfrozenxid, "
            "NULL AS tablespace "
            "FROM pg_database "
            "WHERE datname = ",
            username_subquery);
        appendStringLiteralAH(dbQry, datname, fout);
    }

    res = ExecuteSqlQueryForSingleRow(fout, dbQry->data);

    i_tableoid = PQfnumber(res, "tableoid");
    i_oid = PQfnumber(res, "oid");
    i_dba = PQfnumber(res, "dba");
    i_encoding = PQfnumber(res, "encoding");
    i_collate = PQfnumber(res, "datcollate");
    i_ctype = PQfnumber(res, "datctype");
    i_frozenxid = PQfnumber(res, "datfrozenxid");
    i_frozenxid64 = PQfnumber(res, "datfrozenxid64");
    i_tablespace = PQfnumber(res, "tablespace");
    i_connlimit = PQfnumber(res, "datconnlimit");

    dbCatId.tableoid = atooid(PQgetvalue(res, 0, i_tableoid));
    dbCatId.oid = atooid(PQgetvalue(res, 0, i_oid));
    dba = PQgetvalue(res, 0, i_dba);
    encoding = PQgetvalue(res, 0, i_encoding);
    collate = PQgetvalue(res, 0, i_collate);
    ctype = PQgetvalue(res, 0, i_ctype);
    frozenxid = atooid(PQgetvalue(res, 0, i_frozenxid));
    frozenxid64 = atoxid(PQgetvalue(res, 0, i_frozenxid64));
    tablespace = PQgetvalue(res, 0, i_tablespace);
    dataConnLimit = atoi(PQgetvalue(res, 0, i_connlimit));

    if (isHasDatcompatibility) {
        i_datcompatibility = PQfnumber(res, "datcompatibility");
        datcompatibility = PQgetvalue(res, 0, i_datcompatibility);
        gdatcompatibility = gs_strdup(datcompatibility);
    }

    appendPQExpBuffer(creaQry, "CREATE DATABASE %s WITH TEMPLATE = template0", fmtId(datname));
    if (strlen(encoding) > 0) {
        appendPQExpBuffer(creaQry, " ENCODING = ");
        appendStringLiteralAH(creaQry, encoding, fout);
    }
    if (strlen(collate) > 0) {
        appendPQExpBuffer(creaQry, " LC_COLLATE = ");
        appendStringLiteralAH(creaQry, collate, fout);
    }
    if (strlen(ctype) > 0) {
        appendPQExpBuffer(creaQry, " LC_CTYPE = ");
        appendStringLiteralAH(creaQry, ctype, fout);
    }
    if (strlen(tablespace) > 0 && strcmp(tablespace, "pg_default") != 0)
        appendPQExpBuffer(creaQry, " TABLESPACE = %s", fmtId(tablespace));

    if (dataConnLimit > -1) {
        appendPQExpBuffer(creaQry, " CONNECTION LIMIT = %d", dataConnLimit);
    }

    if (isHasDatcompatibility && strlen(datcompatibility) > 0) {
        appendPQExpBuffer(creaQry, " DBCOMPATIBILITY = ");
        appendStringLiteralAH(creaQry, datcompatibility, fout);
    }

    appendPQExpBuffer(creaQry, ";\n");

    if (binary_upgrade) {
        appendPQExpBuffer(
            creaQry, "\n-- For binary upgrade, set datfrozenxid %s.\n", isHasDatfrozenxid64 ? ", datfrozenxid64" : " ");
        if (isHasDatfrozenxid64)
            appendPQExpBuffer(creaQry,
                "UPDATE pg_catalog.pg_database\n"
                "SET datfrozenxid = '%u'\n"
                "SET datfrozenxid64 = '%lu'\n"
                "WHERE    datname = ",
                frozenxid,
                frozenxid64);
        else
            appendPQExpBuffer(creaQry,
                "UPDATE pg_catalog.pg_database\n"
                "SET datfrozenxid = '%u'\n"
                "WHERE    datname = ",
                frozenxid);
        appendStringLiteralAH(creaQry, datname, fout);
        appendPQExpBuffer(creaQry, ";\n");
    }

    appendPQExpBuffer(delQry, "DROP DATABASE IF EXISTS %s;\n", fmtId(datname));

    /* Dump database-specific configuration*/
    if (fout->remoteVersion >= 70300) {
        int count = 1;
        for (;;) {
            PGresult* tmpRes = NULL;
            char* result = NULL;
            PQExpBuffer configQry = createPQExpBuffer();

            if (fout->remoteVersion >= 90000) {
                appendPQExpBuffer(configQry,
                    "SELECT setconfig[%d] FROM pg_db_role_setting WHERE "
                    "setrole = 0 AND setdatabase = (SELECT oid FROM pg_database WHERE datname = ",
                    count);
            } else {
                appendPQExpBuffer(configQry, "SELECT datconfig[%d] FROM pg_database WHERE datname = ", count);
            }
            appendStringLiteralConn(configQry, datname, conn);

            if (fout->remoteVersion >= 90000) {
                appendPQExpBuffer(configQry, ")");
            }
            appendPQExpBuffer(configQry, ";");

            tmpRes = ExecuteSqlQuery(fout, configQry->data, PGRES_TUPLES_OK);
            destroyPQExpBuffer(configQry);

            if (PQntuples(tmpRes) == 1 && !PQgetisnull(tmpRes, 0, 0)) {
                result = getDbAlterConfigCommand(conn, PQgetvalue(tmpRes, 0, 0), "DATABASE", datname, NULL, NULL);
                if (NULL != result) {
                    appendPQExpBuffer(creaQry, "%s", result);
                    appendPQExpBuffer(creaQry, "\n");
                    free(result);
                    result = NULL;
                }
                PQclear(tmpRes);
                count++;
            } else {
                PQclear(tmpRes);
                break;
            }
        }
    }

    dbDumpId = createDumpId();

    ArchiveEntry(fout,
        dbCatId,          /* catalog ID */
        dbDumpId,         /* dump ID */
        datname,          /* Name */
        NULL,             /* Namespace */
        NULL,             /* Tablespace */
        dba,              /* Owner */
        false,            /* with oids */
        "DATABASE",       /* Desc */
        SECTION_PRE_DATA, /* Section */
        creaQry->data,    /* Create */
        delQry->data,     /* Del */
        NULL,             /* Copy */
        NULL,             /* Deps */
        0,                /* # Deps */
        NULL,             /* Dumper */
        NULL);            /* Dumper Arg */

    /*
     * pg_largeobject and pg_largeobject_metadata come from the old system
     * intact, so set their relfrozenxids.
     */
    if (binary_upgrade) {
        PGresult* lo_res = NULL;
        PQExpBuffer loFrozenQry = createPQExpBuffer();
        PQExpBuffer loOutQry = createPQExpBuffer();
        int i_relfrozenxid;
        int i_relfrozenxid64;
        bool isHasRelfrozenxid64 = false;

        /*
         * pg_largeobject
         */
        isHasRelfrozenxid64 = is_column_exists(AH->connection, RelationRelationId, "relfrozenxid64");

        appendPQExpBuffer(loFrozenQry,
            "SELECT relfrozenxid, %s\n"
            "FROM pg_catalog.pg_class\n"
            "WHERE oid = %u;\n",
            isHasRelfrozenxid64 ? "relfrozenxid64" : "0 AS relfrozenxid64",
            LargeObjectRelationId);

        lo_res = ExecuteSqlQueryForSingleRow(fout, loFrozenQry->data);

        i_relfrozenxid = PQfnumber(lo_res, "relfrozenxid");
        i_relfrozenxid64 = PQfnumber(lo_res, "relfrozenxid64");

        appendPQExpBuffer(loOutQry,
            "\n-- For binary upgrade, set pg_largeobject.relfrozenxid, %s\n",
            isHasRelfrozenxid64 ? "pg_largeobject.relfrozenxid64" : " ");
        if (isHasRelfrozenxid64)
            appendPQExpBuffer(loOutQry,
                "UPDATE pg_catalog.pg_class\n"
                "SET relfrozenxid = '%u'\n"
                "SET relfrozenxid64 = '%lu'\n"
                "WHERE oid = %u;\n",
                atoi(PQgetvalue(lo_res, 0, i_relfrozenxid)),
                atoxid(PQgetvalue(lo_res, 0, i_relfrozenxid64)),
                LargeObjectRelationId);
        else
            appendPQExpBuffer(loOutQry,
                "UPDATE pg_catalog.pg_class\n"
                "SET relfrozenxid = '%u'\n"
                "WHERE oid = %u;\n",
                atoi(PQgetvalue(lo_res, 0, i_relfrozenxid)),
                LargeObjectRelationId);

        ArchiveEntry(fout,
            nilCatalogId,
            createDumpId(),
            "pg_largeobject",
            NULL,
            NULL,
            "",
            false,
            "pg_largeobject",
            SECTION_PRE_DATA,
            loOutQry->data,
            "",
            NULL,
            NULL,
            0,
            NULL,
            NULL);

        PQclear(lo_res);

        /*
         * pg_largeobject_metadata
         */
        if (fout->remoteVersion >= 90000) {
            resetPQExpBuffer(loFrozenQry);
            resetPQExpBuffer(loOutQry);

            appendPQExpBuffer(loFrozenQry,
                "SELECT relfrozenxid, %s\n"
                "FROM pg_catalog.pg_class\n"
                "WHERE oid = %u;\n",
                isHasRelfrozenxid64 ? "relfrozenxid64" : "0 AS relfrozenxid64",
                LargeObjectMetadataRelationId);

            lo_res = ExecuteSqlQueryForSingleRow(fout, loFrozenQry->data);

            i_relfrozenxid = PQfnumber(lo_res, "relfrozenxid");
            i_relfrozenxid64 = PQfnumber(lo_res, "relfrozenxid64");

            appendPQExpBuffer(loOutQry,
                "\n-- For binary upgrade, set pg_largeobject_metadata.relfrozenxid64 %s\n",
                isHasRelfrozenxid64 ? ", pg_largeobject_metadata.relfrozenxid64" : "");

            if (isHasRelfrozenxid64)
                appendPQExpBuffer(loOutQry,
                    "UPDATE pg_catalog.pg_class\n"
                    "SET relfrozenxid = '%u'\n"
                    "SET relfrozenxid64 = '%lu'\n"
                    "WHERE oid = %u;\n",
                    atoi(PQgetvalue(lo_res, 0, i_relfrozenxid)),
                    atoxid(PQgetvalue(lo_res, 0, i_relfrozenxid64)),
                    LargeObjectMetadataRelationId);
            else
                appendPQExpBuffer(loOutQry,
                    "UPDATE pg_catalog.pg_class\n"
                    "SET relfrozenxid = '%u'\n"
                    "WHERE oid = %u;\n",
                    atoi(PQgetvalue(lo_res, 0, i_relfrozenxid)),
                    LargeObjectMetadataRelationId);
            ArchiveEntry(fout,
                nilCatalogId,
                createDumpId(),
                "pg_largeobject_metadata",
                NULL,
                NULL,
                "",
                false,
                "pg_largeobject_metadata",
                SECTION_PRE_DATA,
                loOutQry->data,
                "",
                NULL,
                NULL,
                0,
                NULL,
                NULL);

            PQclear(lo_res);
        }

        destroyPQExpBuffer(loFrozenQry);
        destroyPQExpBuffer(loOutQry);
    }

    /* Dump DB comment if any */
    if (fout->remoteVersion >= 80200) {
        /*
         * 8.2 keeps comments on shared objects in a shared table, so we
         * cannot use the dumpComment used for other database objects.
         */
        char* comment = PQgetvalue(res, 0, PQfnumber(res, "description"));

        if ((comment != NULL) && strlen(comment)) {
            resetPQExpBuffer(dbQry);

            /*
             * Generates warning when loaded into a differently-named
             * database.
             */
            appendPQExpBuffer(dbQry, "COMMENT ON DATABASE %s IS ", fmtId(datname));
            appendStringLiteralAH(dbQry, comment, fout);
            appendPQExpBuffer(dbQry, ";\n");

            ArchiveEntry(fout,
                dbCatId,
                createDumpId(),
                datname,
                NULL,
                NULL,
                dba,
                false,
                "COMMENT",
                SECTION_NONE,
                dbQry->data,
                "",
                NULL,
                &dbDumpId,
                1,
                NULL,
                NULL);
        }
    } else {
        resetPQExpBuffer(dbQry);
        appendPQExpBuffer(dbQry, "DATABASE %s", fmtId(datname));
        dumpComment(fout, dbQry->data, NULL, "", dbCatId, 0, dbDumpId);
    }

    /* Dump shared security label. */
    if (!no_security_labels && fout->remoteVersion >= 90200) {
        PGresult* shres = NULL;
        PQExpBuffer seclabelQry = createPQExpBuffer();

        buildShSecLabelQuery(conn, "pg_database", dbCatId.oid, seclabelQry);
        shres = ExecuteSqlQuery(fout, seclabelQry->data, PGRES_TUPLES_OK);
        resetPQExpBuffer(seclabelQry);
        emitShSecLabels(conn, shres, seclabelQry, "DATABASE", datname);
        if (strlen(seclabelQry->data))
            ArchiveEntry(fout,
                dbCatId,
                createDumpId(),
                datname,
                NULL,
                NULL,
                dba,
                false,
                "SECURITY LABEL",
                SECTION_NONE,
                seclabelQry->data,
                "",
                NULL,
                &dbDumpId,
                1,
                NULL,
                NULL);
        destroyPQExpBuffer(seclabelQry);
        PQclear(shres);
    }

    PQclear(res);

    destroyPQExpBuffer(dbQry);
    destroyPQExpBuffer(delQry);
    destroyPQExpBuffer(creaQry);
}

static void getGrantAnyPrivilegeQuery(Archive* fout, PQExpBuffer grantAnyPrivilegeSql,
    Oid roleid, const char* adminOption)
{
    PGresult* anyRes = NULL;
    PQExpBuffer gsDbPrivilegeQry = createPQExpBuffer();
    if (gsDbPrivilegeQry == NULL) {
        return;
    }
    appendPQExpBuffer(gsDbPrivilegeQry,
        "SELECT pg_roles.rolname, gs_db_privilege.privilege_type from gs_db_privilege "
        "left join pg_roles on gs_db_privilege.roleid = pg_roles.oid "
        "where gs_db_privilege.roleid = %u and gs_db_privilege.admin_option = '%s';", roleid, adminOption);
    anyRes = ExecuteSqlQuery(fout, gsDbPrivilegeQry->data, PGRES_TUPLES_OK);
    int tupleNum = PQntuples(anyRes);
    if (tupleNum <= 0) {
        PQclear(anyRes);
        destroyPQExpBuffer(gsDbPrivilegeQry);
        return;
    }
    appendPQExpBuffer(grantAnyPrivilegeSql, "GRANT ");
    char* roleName = PQgetvalue(anyRes, 0, 0);
    for (int j = 0; j < tupleNum; j++) {
        char* privilegeType = PQgetvalue(anyRes, j, 1);
        if (j == tupleNum - 1) {
            appendPQExpBuffer(grantAnyPrivilegeSql, "%s ", privilegeType);
        } else {
            appendPQExpBuffer(grantAnyPrivilegeSql, "%s, ", privilegeType);
        }
    }
    appendPQExpBuffer(grantAnyPrivilegeSql, "TO %s", roleName);
    if (*adminOption == 't') {
        appendPQExpBuffer(grantAnyPrivilegeSql, " WITH ADMIN OPTION");
    }
    appendPQExpBuffer(grantAnyPrivilegeSql, ";\n");
    PQclear(anyRes);
    destroyPQExpBuffer(gsDbPrivilegeQry);
}
/*
 * Dump privilege of database level.
 *
 */
static void dumpAnyPrivilege(Archive *fout)
{
    PGresult* res = NULL;
    Oid roleid = 0;
    PQExpBuffer selectGsDbPrivilegeQry = createPQExpBuffer();
    if (selectGsDbPrivilegeQry == NULL) {
        return;
    }
    PQExpBuffer grantAnyPrivilegeSql = createPQExpBuffer();
    if (grantAnyPrivilegeSql == NULL) {
        destroyPQExpBuffer(selectGsDbPrivilegeQry);
        return;
    }
    appendPQExpBuffer(selectGsDbPrivilegeQry, "SELECT distinct roleid from gs_db_privilege;");
    res = ExecuteSqlQuery(fout, selectGsDbPrivilegeQry->data, PGRES_TUPLES_OK);
    int roleidNum = PQfnumber(res, "roleid");
    for (int i = 0; i < PQntuples(res); i++) {
        roleid = atooid(PQgetvalue(res, i, roleidNum));
        getGrantAnyPrivilegeQuery(fout, grantAnyPrivilegeSql, roleid, "f");
        getGrantAnyPrivilegeQuery(fout, grantAnyPrivilegeSql, roleid, "t");
    }
    ArchiveEntry(fout,
        nilCatalogId,                /* catalog ID */
        createDumpId(),              /* dump ID */
        "AnyPrivilege",              /* Name */
        NULL,                        /* Namespace */
        NULL,                        /* Tablespace */
        "",                          /* Owner */
        false,                       /* with oids */
        "AnyPrivilege",              /* Desc */
        SECTION_PRE_DATA,            /* Section */
        grantAnyPrivilegeSql->data,  /* Create */
        "",                          /* Del */
        NULL,                        /* Copy */
        NULL,                        /* Deps */
        0,                           /* # Deps */
        NULL,                        /* Dumper */
        NULL);                       /* Dumper Arg */
    PQclear(res);
    destroyPQExpBuffer(selectGsDbPrivilegeQry);
    destroyPQExpBuffer(grantAnyPrivilegeSql);
}

#ifdef HAVE_CE
static void dumpClientGlobalKeys(Archive *fout, const Oid nspoid, const char *nspname) 
{
    int j = 0;
    PGresult *cmk_res = NULL;
    PGresult *cmk_args_res = NULL;
    int i_global_key_id = 0;
    int i_global_key_name = 0;
    int ntups = 0;
    int cmk_args_ntups = 0;
    Oid global_key_id = 0;
    const char *global_key_name = NULL;
    PQExpBuffer delQry = createPQExpBuffer();
    PQExpBuffer selectClientGlobalKeysQry = createPQExpBuffer();
    PQExpBuffer selectClientGlobalKeysArgsQry = createPQExpBuffer();
    PQExpBuffer createClientGlobalKeysQry = createPQExpBuffer();

    appendPQExpBuffer(selectClientGlobalKeysQry,
        "SELECT oid,* from gs_client_global_keys where key_namespace = %u;", nspoid);
    cmk_res = ExecuteSqlQuery(fout, selectClientGlobalKeysQry->data, PGRES_TUPLES_OK);
    ntups = PQntuples(cmk_res);
    i_global_key_id = PQfnumber(cmk_res, "oid");
    i_global_key_name = PQfnumber(cmk_res, "global_key_name");

    for (j = 0; j < ntups; j++) {
        int k = 0;
        char *key = NULL;
        char *value = NULL;
        unsigned char *key_store = (unsigned char *)pg_malloc(MAX_CLIENT_KEYS_PARAM_SIZE);
        unsigned char *key_path = (unsigned char *)pg_malloc(MAX_CLIENT_KEYS_PARAM_SIZE);
        unsigned char *algorithm = (unsigned char *)pg_malloc(MAX_CLIENT_KEYS_PARAM_SIZE);
        error_t rc = memset_s(key_store, MAX_CLIENT_KEYS_PARAM_SIZE, 0, MAX_CLIENT_KEYS_PARAM_SIZE);
        securec_check_c(rc, "", "");
        rc = memset_s(key_path, MAX_CLIENT_KEYS_PARAM_SIZE, 0, MAX_CLIENT_KEYS_PARAM_SIZE);
        securec_check_c(rc, "", "");
        rc = memset_s(algorithm, MAX_CLIENT_KEYS_PARAM_SIZE, 0, MAX_CLIENT_KEYS_PARAM_SIZE);
        securec_check_c(rc, "", "");
        resetPQExpBuffer(selectClientGlobalKeysArgsQry);
        resetPQExpBuffer(createClientGlobalKeysQry);
        resetPQExpBuffer(delQry);

        global_key_id = atooid(PQgetvalue(cmk_res, j, i_global_key_id));
        global_key_name = PQgetvalue(cmk_res, j, i_global_key_name);
        appendPQExpBuffer(selectClientGlobalKeysArgsQry,
            "SELECT * from gs_client_global_keys_args where global_key_id = '%u';", global_key_id);
        cmk_args_res = ExecuteSqlQuery(fout, selectClientGlobalKeysArgsQry->data, PGRES_TUPLES_OK);
        cmk_args_ntups = PQntuples(cmk_args_res);
        int i_key = PQfnumber(cmk_args_res, "key");
        int i_value = PQfnumber(cmk_args_res, "value");
        for (k = 0; k < cmk_args_ntups; k++) {
            key = PQgetvalue(cmk_args_res, k, i_key);
            value = PQgetvalue(cmk_args_res, k, i_value);
            unsigned char *unescaped_data = NULL;
            size_t unescapedDataSize = 0;
            unescaped_data = PQunescapeBytea((unsigned char *)value, &unescapedDataSize);
            if (strcmp(key, "KEY_STORE") == 0) {
                rc = memcpy_s(key_store, MAX_CLIENT_KEYS_PARAM_SIZE, unescaped_data, unescapedDataSize);
                securec_check_c(rc, "", "");
            } else if (strcmp(key, "KEY_PATH") == 0) {
                rc = memcpy_s(key_path, MAX_CLIENT_KEYS_PARAM_SIZE, unescaped_data, unescapedDataSize);
                securec_check_c(rc, "", "");
            } else if (strcmp(key, "ALGORITHM") == 0) {
                rc = memcpy_s(algorithm, MAX_CLIENT_KEYS_PARAM_SIZE, unescaped_data, unescapedDataSize);
                securec_check_c(rc, "", "");
            }
            PQfreemem(unescaped_data);
        } 
        appendPQExpBuffer(createClientGlobalKeysQry,
            "CREATE CLIENT MASTER KEY %s.%s ", nspname, fmtId(global_key_name));
        appendPQExpBuffer(createClientGlobalKeysQry, 
            "WITH ( KEY_STORE = %s , KEY_PATH = \"%s\", ALGORITHM = %s );", key_store, key_path, algorithm);
        appendPQExpBuffer(delQry, "DROP CLIENT MASTER KEY IF EXISTS %s.%s;\n", nspname, fmtId(global_key_name));

        ArchiveEntry(fout,
            nilCatalogId,
            createDumpId(),
            "GLOBALKEY",
            NULL,
            NULL,
            "",
            false,
            "GLOBALKEY",
            SECTION_PRE_DATA,
            createClientGlobalKeysQry->data,
            delQry->data,
            NULL,
            NULL,
            0,
            NULL,
            NULL);

        if (key_store != NULL) {
            free(key_store);
            key_store = NULL;
        }
        if (key_path != NULL) {
            free(key_path);
            key_path = NULL;
        }
        if (algorithm != NULL) {
            free(algorithm);
            algorithm = NULL;
        }
    }

    PQclear(cmk_res);
    PQclear(cmk_args_res);
    destroyPQExpBuffer(delQry);
    destroyPQExpBuffer(selectClientGlobalKeysQry);
    destroyPQExpBuffer(selectClientGlobalKeysArgsQry);
    destroyPQExpBuffer(createClientGlobalKeysQry);
}

static void dumpColumnEncryptionKeys(Archive *fout, const Oid nspoid, const char *nspname)
{
    int cek_index = 0;
    PQExpBuffer delQry = createPQExpBuffer();
    PQExpBuffer selectClientGlobalKeysArgsQry = createPQExpBuffer();
    PQExpBuffer selectColumnKeysQry = createPQExpBuffer();
    PQExpBuffer selectColumnKeysArgsQry = createPQExpBuffer();
    PQExpBuffer createColumnEncryptionKeysQry = createPQExpBuffer();

    PGresult *cek_res = NULL;
    PGresult *cek_args_res = NULL;
    PGresult *cmk_args_res = NULL;

    int i_column_key_id = 0;
    int i_column_key_name = 0;
    int i_cek_global_key_name = 0;
    int i_cek_global_key_id = 0;

    Oid column_key_id = 0;
    const char *column_key_name = NULL;
    const char *cek_global_key_name = NULL;
    Oid cek_global_key_id = 0;

    int cek_ntups = 0;
    int cek_args_ntups = 0;
    int cmk_args_ntups = 0;
    appendPQExpBuffer(selectColumnKeysQry,
        " SELECT a.oid, a.column_key_name, b.global_key_name, a.global_key_id "
        " from gs_column_keys a inner join gs_client_global_keys b "
        " on a.global_key_id = b.oid where a.key_namespace = %u;", nspoid);

    cek_res = ExecuteSqlQuery(fout, selectColumnKeysQry->data, PGRES_TUPLES_OK);
    cek_ntups = PQntuples(cek_res);

    i_column_key_id = PQfnumber(cek_res, "oid");
    i_column_key_name = PQfnumber(cek_res, "column_key_name");
    i_cek_global_key_name = PQfnumber(cek_res, "global_key_name");
    i_cek_global_key_id =  PQfnumber(cek_res, "global_key_id");
    
    for (cek_index = 0; cek_index < cek_ntups; cek_index++) {
        char *key = NULL;
        char *value = NULL;
        char *encrypted_value = (char *)pg_malloc(MAX_CLIENT_ENCRYPTION_KEYS_SIZE);
        unsigned char *algorithm = (unsigned char *)pg_malloc(MAX_CLIENT_KEYS_PARAM_SIZE);
        error_t rc = EOK;
        int k = 0;

        rc = memset_s(encrypted_value, MAX_CLIENT_ENCRYPTION_KEYS_SIZE, 0, MAX_CLIENT_ENCRYPTION_KEYS_SIZE);
        securec_check_c(rc, "", "");
        rc = memset_s(algorithm, MAX_CLIENT_KEYS_PARAM_SIZE, 0, MAX_CLIENT_KEYS_PARAM_SIZE);
        securec_check_c(rc, "", "");
        resetPQExpBuffer(selectColumnKeysArgsQry);
        resetPQExpBuffer(selectClientGlobalKeysArgsQry);
        resetPQExpBuffer(createColumnEncryptionKeysQry);
        resetPQExpBuffer(delQry);

        column_key_id = atooid(PQgetvalue(cek_res, cek_index, i_column_key_id));
        column_key_name = PQgetvalue(cek_res, cek_index, i_column_key_name);
        cek_global_key_name = PQgetvalue(cek_res, cek_index, i_cek_global_key_name);
        cek_global_key_id = atooid(PQgetvalue(cek_res, cek_index, i_cek_global_key_id));
        appendPQExpBuffer(selectColumnKeysArgsQry,
            "SELECT * from gs_column_keys_args where column_key_id = '%u';", column_key_id);
        cek_args_res = ExecuteSqlQuery(fout, selectColumnKeysArgsQry->data, PGRES_TUPLES_OK);
        cek_args_ntups = PQntuples(cek_args_res);
        int i_key = PQfnumber(cek_args_res, "key");
        int i_value = PQfnumber(cek_args_res, "value");
        appendPQExpBuffer(selectClientGlobalKeysArgsQry,
            "SELECT * from gs_client_global_keys_args where global_key_id = '%u';", cek_global_key_id);
        cmk_args_res = ExecuteSqlQuery(fout, selectClientGlobalKeysArgsQry->data, PGRES_TUPLES_OK);
        cmk_args_ntups = PQntuples(cmk_args_res);
        int i_cmk_key = PQfnumber(cmk_args_res, "key");
        int i_cmk_value = PQfnumber(cmk_args_res, "value");

        char key_store_str[MAX_CMK_STORE_SIZE] = {0};
        char key_path_str[MAX_CLIENT_KEYS_PARAM_SIZE] = {0};
        char key_algorithm_str[MAX_CLIENT_KEYS_PARAM_SIZE] = {0};

        for (k = 0; k < cmk_args_ntups; k++) {
            key = PQgetvalue(cmk_args_res, k, i_cmk_key);
            value = PQgetvalue(cmk_args_res, k, i_cmk_value);
            unsigned char *unescaped_data = NULL;
            size_t unescapedDataSize = 0;

            unescaped_data = PQunescapeBytea((unsigned char *)value, &unescapedDataSize);
            if (strcasecmp(key, "key_store") == 0) {
                rc = memcpy_s(key_store_str, MAX_CMK_STORE_SIZE, unescaped_data, unescapedDataSize);
            } else if (strcmp(key, "KEY_PATH") == 0) {
                rc = memcpy_s(key_path_str, MAX_CLIENT_KEYS_PARAM_SIZE, unescaped_data, unescapedDataSize);
            } else if (strcmp(key, "ALGORITHM") == 0) {
                rc = memcpy_s(key_algorithm_str, MAX_CLIENT_KEYS_PARAM_SIZE, unescaped_data, unescapedDataSize);
            }
            securec_check_c(rc, "", "");
            PQfreemem(unescaped_data);
        } 

        unsigned char *deprocessed_cek = NULL;
        size_t deprocessed_cek_len = 0;

        for (k = 0; k < cek_args_ntups; k++) {
            key = PQgetvalue(cek_args_res, k, i_key);
            value = PQgetvalue(cek_args_res, k, i_value);
            unsigned char *unescaped_data = NULL;
            size_t unescapedDataSize = 0;
            unescaped_data = PQunescapeBytea((unsigned char *)value, &unescapedDataSize);
            if (strcmp(key, "ENCRYPTED_VALUE") == 0) {
                rc = memcpy_s(encrypted_value, MAX_CLIENT_ENCRYPTION_KEYS_SIZE, unescaped_data, unescapedDataSize);
                securec_check_c(rc, "", "");

                if (!HooksManager::GlobalSettings::deprocess_column_setting(
                    (const unsigned char *)encrypted_value, 
                    unescapedDataSize,
                    key_store_str,
                    key_path_str,
                    key_algorithm_str,
                    &deprocessed_cek,
                    &deprocessed_cek_len)) {
                    exit_nicely(1);
                }
                deprocessed_cek = (unsigned char *)pg_realloc(deprocessed_cek, MAX_CLIENT_ENCRYPTION_KEYS_SIZE);
                char ch = '\'';
                for (size_t decrypted_key_index = 0; deprocessed_cek[decrypted_key_index] != '\0'; 
                    decrypted_key_index++) {
                    if (deprocessed_cek[decrypted_key_index] == ch) {
                        rc = memmove_s(deprocessed_cek +  decrypted_key_index + 1,
                            deprocessed_cek_len + 1 - decrypted_key_index + 1,
                            deprocessed_cek + decrypted_key_index,
                            deprocessed_cek_len + 1 - decrypted_key_index);
                        securec_check_c(rc, "\0", "\0");
                        decrypted_key_index++;
                    }
                }
            } else if (strcmp(key, "ALGORITHM") == 0) {
                rc = memcpy_s(algorithm, MAX_CLIENT_KEYS_PARAM_SIZE, unescaped_data, unescapedDataSize);
                securec_check_c(rc, "", "");
            }
            PQfreemem(unescaped_data);
        }
        appendPQExpBuffer(createColumnEncryptionKeysQry,
            "CREATE COLUMN ENCRYPTION KEY %s.%s ", nspname, column_key_name);
        appendPQExpBuffer(createColumnEncryptionKeysQry, 
            "WITH VALUES (CLIENT_MASTER_KEY = %s.%s, encrypted_value='%s', ALGORITHM = %s );\n",
            nspname, cek_global_key_name, deprocessed_cek, algorithm);
        appendPQExpBuffer(delQry, "DROP COLUMN ENCRYPTION KEY IF EXISTS %s.%s;\n", nspname, fmtId(column_key_name));
        ArchiveEntry(fout,
            nilCatalogId,
            createDumpId(),
            "COLUMNKEY",
            NULL,
            NULL,
            "",
            false,
            "COLUMNKEY",
            SECTION_PRE_DATA,
            createColumnEncryptionKeysQry->data,
            delQry->data,
            NULL,
            NULL,
            0,
            NULL,
            NULL);
        rc = memset_s(deprocessed_cek, MAX_CLIENT_ENCRYPTION_KEYS_SIZE, 0, MAX_CLIENT_ENCRYPTION_KEYS_SIZE);
        securec_check_c(rc, "", "");
        FREE_PTR(deprocessed_cek);
        FREE_PTR(encrypted_value);
        FREE_PTR(algorithm);
    }

    PQclear(cek_res);
    PQclear(cek_args_res);
    PQclear(cmk_args_res);
    destroyPQExpBuffer(delQry);
    destroyPQExpBuffer(selectColumnKeysQry);
    destroyPQExpBuffer(selectColumnKeysArgsQry);
    destroyPQExpBuffer(selectClientGlobalKeysArgsQry);
    destroyPQExpBuffer(createColumnEncryptionKeysQry);
}
#endif
/*
 * dumpEncoding: put the correct encoding into the archive
 */
static void dumpEncoding(Archive* AH)
{
    ddr_Assert(AH != NULL);
    const char* encname = pg_encoding_to_char(AH->encoding);
    if (NULL == encname) {
        return;
    }
    PQExpBuffer qry = createPQExpBuffer();

    if (g_verbose)
        write_msg(NULL, "saving encoding = %s\n", encname);

    appendPQExpBuffer(qry, "SET client_encoding = ");
    appendStringLiteralAH(qry, encname, AH);
    appendPQExpBuffer(qry, ";\n");

    ArchiveEntry(AH,
        nilCatalogId,
        createDumpId(),
        "ENCODING",
        NULL,
        NULL,
        "",
        false,
        "ENCODING",
        SECTION_PRE_DATA,
        qry->data,
        "",
        NULL,
        NULL,
        0,
        NULL,
        NULL);

    destroyPQExpBuffer(qry);
}

/*
 * dumpStdStrings: put the correct escape string behavior into the archive
 */
static void dumpStdStrings(Archive* AH)
{
    ddr_Assert(AH != NULL);
    const char* stdstrings = AH->std_strings ? "on" : "off";
    PQExpBuffer qry = createPQExpBuffer();

    if (g_verbose)
        write_msg(NULL, "saving standard_conforming_strings = %s\n", stdstrings);

    appendPQExpBuffer(qry, "SET standard_conforming_strings = '%s';\n", stdstrings);

    ArchiveEntry(AH,
        nilCatalogId,
        createDumpId(),
        "STDSTRINGS",
        NULL,
        NULL,
        "",
        false,
        "STDSTRINGS",
        SECTION_PRE_DATA,
        qry->data,
        "",
        NULL,
        NULL,
        0,
        NULL,
        NULL);

    destroyPQExpBuffer(qry);
}

/*
 * getBlobs:
 *	Collect schema-level data about large objects
 */
static void getBlobs(Archive* fout)
{
    PQExpBuffer blobQry = createPQExpBuffer();
    BlobInfo* binfo = NULL;
    DumpableObject* bdata = NULL;
    PGresult* res = NULL;
    int ntups = 0;
    int i = 0;

    ddr_Assert(fout != NULL);
    /* Verbose message */
    if (g_verbose)
        write_msg(NULL, "reading large objects\n");

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, "pg_catalog");

    /* Fetch BLOB OIDs, and owner/ACL data if >= 9.0 */
    if (fout->remoteVersion >= 90000)
        appendPQExpBuffer(blobQry,
            "SELECT oid, (%s lomowner) AS rolname, lomacl"
            " FROM pg_largeobject_metadata",
            username_subquery);
    else if (fout->remoteVersion >= 70100)
        appendPQExpBuffer(blobQry,
            "SELECT DISTINCT loid, NULL::oid, NULL::oid"
            " FROM pg_largeobject");
    else
        appendPQExpBuffer(blobQry,
            "SELECT oid, NULL::oid, NULL::oid"
            " FROM pg_class WHERE relkind = 'l'");

    res = ExecuteSqlQuery(fout, blobQry->data, PGRES_TUPLES_OK);

    ntups = PQntuples(res);
    if (ntups > 0) {
        /*
         * Each large object has its own BLOB archive entry.
         */
        binfo = (BlobInfo*)pg_malloc(ntups * sizeof(BlobInfo));

        for (i = 0; i < ntups; i++) {
            binfo[i].dobj.objType = DO_BLOB;
            binfo[i].dobj.catId.tableoid = LargeObjectRelationId;
            binfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, 0));
            AssignDumpId(&binfo[i].dobj);

            binfo[i].dobj.name = gs_strdup(PQgetvalue(res, i, 0));
            if (!PQgetisnull(res, i, 1))
                binfo[i].rolname = gs_strdup(PQgetvalue(res, i, 1));
            else
                binfo[i].rolname = "";
            if (!PQgetisnull(res, i, 2))
                binfo[i].blobacl = gs_strdup(PQgetvalue(res, i, 2));
            else
                binfo[i].blobacl = NULL;
        }

        /*
         * If we have any large objects, a "BLOBS" archive entry is needed.
         * This is just a placeholder for sorting; it carries no data now.
         */
        bdata = (DumpableObject*)pg_malloc(sizeof(DumpableObject));
        bdata->objType = DO_BLOB_DATA;
        bdata->catId = nilCatalogId;
        AssignDumpId(bdata);
        bdata->name = gs_strdup("BLOBS");
    }

    PQclear(res);
    destroyPQExpBuffer(blobQry);
}

/*
 * dumpBlob
 *
 * dump the definition (metadata) of the given large object
 */
static void dumpBlob(Archive* fout, BlobInfo* binfo)
{
    if (isExecUserNotObjectOwner(fout, binfo->rolname))
        return;

    PQExpBuffer cquery = createPQExpBuffer();
    PQExpBuffer dquery = createPQExpBuffer();

    appendPQExpBuffer(cquery, "SELECT pg_catalog.lo_create('%s');\n", binfo->dobj.name);

    appendPQExpBuffer(dquery, "SELECT pg_catalog.lo_unlink('%s');\n", binfo->dobj.name);

    ArchiveEntry(fout,
        binfo->dobj.catId,
        binfo->dobj.dumpId,
        binfo->dobj.name,
        NULL,
        NULL,
        binfo->rolname,
        false,
        "BLOB",
        SECTION_PRE_DATA,
        cquery->data,
        dquery->data,
        NULL,
        NULL,
        0,
        NULL,
        NULL);

    /* set up tag for comment and/or ACL */
    resetPQExpBuffer(cquery);
    appendPQExpBuffer(cquery, "LARGE OBJECT %s", binfo->dobj.name);

    /* Dump comment if any */
    dumpComment(fout, cquery->data, NULL, binfo->rolname, binfo->dobj.catId, 0, binfo->dobj.dumpId);

    /* Dump security label if any */
    dumpSecLabel(fout, cquery->data, NULL, binfo->rolname, binfo->dobj.catId, 0, binfo->dobj.dumpId);

    /* Dump ACL if any */
    if (NULL != binfo->blobacl)
        dumpACL(fout,
            binfo->dobj.catId,
            binfo->dobj.dumpId,
            "LARGE OBJECT",
            binfo->dobj.name,
            NULL,
            cquery->data,
            NULL,
            binfo->rolname,
            binfo->blobacl);

    destroyPQExpBuffer(cquery);
    destroyPQExpBuffer(dquery);
}

/*
 * dumpBlobs:
 *	dump the data contents of all large objects
 */
static int dumpBlobs(Archive* fout, void* arg)
{
    const char* blobQry = NULL;
    const char* blobFetchQry = NULL;
    PGconn* conn = GetConnection(fout);
    PGresult* res = NULL;
    char* buf = NULL;
    int ntups = 0;
    int i = 0;
    int cnt = 0;
    int ret = 0;
    errno_t rc = 0;

    if (g_verbose)
        write_msg(NULL, "saving large objects\n");

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, "pg_catalog");

    /*
     * Currently, we re-fetch all BLOB OIDs using a cursor.  Consider scanning
     * the already-in-memory dumpable objects instead...
     */
    /*syntax changed from CURSOR declaration */
    if (fout->remoteVersion >= 90000)
        blobQry = "CURSOR bloboid FOR SELECT oid FROM pg_largeobject_metadata";
    else if (fout->remoteVersion >= 70100)
        blobQry = "DECLARE bloboid CURSOR FOR SELECT DISTINCT loid FROM pg_largeobject";
    else
        blobQry = "DECLARE bloboid CURSOR FOR SELECT oid FROM pg_class WHERE relkind = 'l'";

    ExecuteSqlStatement(fout, blobQry);

    /* Command to fetch from cursor */
    blobFetchQry = "FETCH 1000 IN bloboid";

    buf = (char*)pg_malloc(LOBBUFSIZE + 1);
    rc = memset_s(buf, (LOBBUFSIZE + 1), 0, LOBBUFSIZE + 1);
    securec_check_c(rc, buf, "\0");
    do {
        /* Do a fetch */
        res = ExecuteSqlQuery(fout, blobFetchQry, PGRES_TUPLES_OK);

        /* Process the tuples, if any */
        ntups = PQntuples(res);
        for (i = 0; i < ntups; i++) {
            Oid blobOid = 0;
            int loFd = 0;

            blobOid = atooid(PQgetvalue(res, i, 0));
            /* Open the BLOB */
            loFd = lo_open(conn, blobOid, INV_READ);
            if (loFd == -1) {
                free(buf);
                buf = NULL;
                exit_horribly(NULL, "could not open large object %u: %s", blobOid, PQerrorMessage(conn));
            }

            StartBlob(fout, blobOid);

            /* Now read it in chunks, sending data to archive */
            do {
                cnt = lo_read(conn, loFd, buf, LOBBUFSIZE);
                if (cnt < 0) {
                    free(buf);
                    buf = NULL;
                    exit_horribly(NULL, "error reading large object %u: %s", blobOid, PQerrorMessage(conn));
                }

                ret = WriteData(fout, buf, cnt);
                if (ret != cnt) {
                    free(buf);
                    buf = NULL;
                    write_msg(NULL, "could not write to output file: %s\n", strerror(errno));
                    exit_nicely(1);
                }

            } while (cnt > 0);

            lo_close(conn, loFd);
            EndBlob(fout, blobOid);
        }

        PQclear(res);
    } while (ntups > 0);

    free(buf);
    buf = NULL;
    return 1;
}

/*
 * getPublications
 * 	  get information about publications
 */
void getPublications(Archive *fout)
{
    PQExpBuffer query;
    PGresult *res;
    PublicationInfo *pubinfo;
    int i_tableoid;
    int i_oid;
    int i_pubname;
    int i_rolname;
    int i_puballtables;
    int i_pubinsert;
    int i_pubupdate;
    int i_pubdelete;
    int i_pubddl = 0;
    int i, ntups;

    if (no_publications || GetVersionNum(fout) < SUBSCRIPTION_VERSION) {
        return;
    }

    query = createPQExpBuffer();

    resetPQExpBuffer(query);

    /* Get the publications. */
    if (GetVersionNum(fout) >= PUBLICATION_DDL_VERSION_NUM) {
        appendPQExpBuffer(query,
            "SELECT p.tableoid, p.oid, p.pubname, "
            "(%s p.pubowner) AS rolname, "
            "p.puballtables, p.pubinsert, p.pubupdate, p.pubdelete, p.pubddl "
            "FROM pg_catalog.pg_publication p",
            username_subquery);
    } else {
        appendPQExpBuffer(query,
            "SELECT p.tableoid, p.oid, p.pubname, "
            "(%s p.pubowner) AS rolname, "
            "p.puballtables, p.pubinsert, p.pubupdate, p.pubdelete "
            "FROM pg_catalog.pg_publication p",
            username_subquery);
    }

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    ntups = PQntuples(res);
    if (ntups == 0) {
        PQclear(res);
        destroyPQExpBuffer(query);
        return;
    }

    i_tableoid = PQfnumber(res, "tableoid");
    i_oid = PQfnumber(res, "oid");
    i_pubname = PQfnumber(res, "pubname");
    i_rolname = PQfnumber(res, "rolname");
    i_puballtables = PQfnumber(res, "puballtables");
    i_pubinsert = PQfnumber(res, "pubinsert");
    i_pubupdate = PQfnumber(res, "pubupdate");
    i_pubdelete = PQfnumber(res, "pubdelete");
    if (GetVersionNum(fout) >= PUBLICATION_DDL_VERSION_NUM) {
        i_pubddl = PQfnumber(res, "pubddl");
    }

    pubinfo = (PublicationInfo *)pg_malloc(ntups * sizeof(PublicationInfo));

    for (i = 0; i < ntups; i++) {
        pubinfo[i].dobj.objType = DO_PUBLICATION;
        pubinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        pubinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&pubinfo[i].dobj);
        pubinfo[i].dobj.name = gs_strdup(PQgetvalue(res, i, i_pubname));
        pubinfo[i].rolname = gs_strdup(PQgetvalue(res, i, i_rolname));
        pubinfo[i].puballtables = (strcmp(PQgetvalue(res, i, i_puballtables), "t") == 0);
        pubinfo[i].pubinsert = (strcmp(PQgetvalue(res, i, i_pubinsert), "t") == 0);
        pubinfo[i].pubupdate = (strcmp(PQgetvalue(res, i, i_pubupdate), "t") == 0);
        pubinfo[i].pubdelete = (strcmp(PQgetvalue(res, i, i_pubdelete), "t") == 0);
        if (GetVersionNum(fout) >= PUBLICATION_DDL_VERSION_NUM) {
            pubinfo[i].pubddl = atol(PQgetvalue(res, i, i_pubddl));
        }

        if (strlen(pubinfo[i].rolname) == 0) {
            write_msg(NULL, "WARNING: owner of publication \"%s\" appears to be invalid\n", pubinfo[i].dobj.name);
        }

        /* Decide whether we want to dump it */
        selectDumpableObject(&(pubinfo[i].dobj), fout);
    }
    PQclear(res);

    destroyPQExpBuffer(query);
}

/*
 * dumpPublication
 * 	  dump the definition of the given publication
 */
static void dumpPublication(Archive *fout, const PublicationInfo *pubinfo)
{
    PQExpBuffer delq;
    PQExpBuffer query;
    PQExpBuffer labelq;
    bool first = true;

    if (dataOnly || !pubinfo->dobj.dump) {
        return;
    }

    delq = createPQExpBuffer();
    query = createPQExpBuffer();
    labelq = createPQExpBuffer();

    appendPQExpBuffer(delq, "DROP PUBLICATION %s;\n", fmtId(pubinfo->dobj.name));

    appendPQExpBuffer(query, "CREATE PUBLICATION %s", fmtId(pubinfo->dobj.name));

    appendPQExpBuffer(labelq, "PUBLICATION %s", fmtId(pubinfo->dobj.name));

    if (pubinfo->puballtables)
        appendPQExpBufferStr(query, " FOR ALL TABLES");

    appendPQExpBufferStr(query, " WITH (publish = '");
    if (pubinfo->pubinsert) {
        appendPQExpBufferStr(query, "insert");
        first = false;
    }

    if (pubinfo->pubupdate) {
        if (!first) {
            appendPQExpBufferStr(query, ", ");
        }
        appendPQExpBufferStr(query, "update");
        first = false;
    }

    if (pubinfo->pubdelete) {
        if (!first) {
            appendPQExpBufferStr(query, ", ");
        }
        appendPQExpBufferStr(query, "delete");
        first = false;
    }

    if (GetVersionNum(fout) >= PUBLICATION_DDL_VERSION_NUM && pubinfo->pubddl != 0) {
        if (!first) {
            appendPQExpBufferStr(query, "',");
        }

        if (pubinfo->pubddl == PUBDDL_ALL) {
            appendPQExpBufferStr(query, "ddl = 'all");
        } else {
            appendPQExpBufferStr(query, "ddl = ");

            if (ENABLE_PUBDDL_TYPE(pubinfo->pubddl, PUBDDL_TABLE)) {
                appendPQExpBuffer(query, "'table");
            }
        }
        first = false;
    }

    appendPQExpBufferStr(query, "');\n");

    ArchiveEntry(fout, pubinfo->dobj.catId, pubinfo->dobj.dumpId, pubinfo->dobj.name, NULL, NULL, pubinfo->rolname,
        false, "PUBLICATION", SECTION_POST_DATA, query->data, delq->data, NULL, NULL, 0, NULL, NULL);

    dumpComment(fout, labelq->data, NULL, pubinfo->rolname, pubinfo->dobj.catId, 0, pubinfo->dobj.dumpId);
    dumpSecLabel(fout, labelq->data, NULL, pubinfo->rolname, pubinfo->dobj.catId, 0, pubinfo->dobj.dumpId);

    destroyPQExpBuffer(delq);
    destroyPQExpBuffer(query);
    destroyPQExpBuffer(labelq);
}

/*
 * getPublicationTables
 * 	  get information about publication membership for dumpable tables.
 */
void getPublicationTables(Archive *fout, TableInfo tblinfo[], int numTables)
{
    PQExpBuffer query;
    PGresult *res;
    PublicationRelInfo *pubrinfo;
    int i_tableoid;
    int i_oid;
    int i_pubname;
    int i, j, ntups;

    if (no_publications || GetVersionNum(fout) < SUBSCRIPTION_VERSION) {
        return;
    }

    query = createPQExpBuffer();

    for (i = 0; i < numTables; i++) {
        TableInfo *tbinfo = &tblinfo[i];

        /* Only plain tables can be aded to publications. */
        if (tbinfo->relkind != RELKIND_RELATION) {
            continue;
        }

        if (g_verbose) {
            write_msg(NULL, "reading publication membership for table \"%s.%s\"\n", tbinfo->dobj.nmspace->dobj.name,
                tbinfo->dobj.name);
        }

        resetPQExpBuffer(query);

        /* Get the publication memebership for the table. */
        appendPQExpBuffer(query,
            "SELECT pr.tableoid, pr.oid, p.pubname "
            "FROM pg_catalog.pg_publication_rel pr,"
            "     pg_catalog.pg_publication p "
            "WHERE pr.prrelid = '%u'"
            "  AND p.oid = pr.prpubid",
            tbinfo->dobj.catId.oid);
        res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

        ntups = PQntuples(res);
        if (ntups == 0) {
            /*
             * Table is not member of any publications. Clean up and return.
             */
            PQclear(res);
            continue;
        }

        i_tableoid = PQfnumber(res, "tableoid");
        i_oid = PQfnumber(res, "oid");
        i_pubname = PQfnumber(res, "pubname");

        pubrinfo = (PublicationRelInfo *)pg_malloc(ntups * sizeof(PublicationRelInfo));
        for (j = 0; j < ntups; j++) {
            pubrinfo[j].dobj.objType = DO_PUBLICATION_REL;
            pubrinfo[j].dobj.catId.tableoid = atooid(PQgetvalue(res, j, i_tableoid));
            pubrinfo[j].dobj.catId.oid = atooid(PQgetvalue(res, j, i_oid));
            AssignDumpId(&pubrinfo[j].dobj);
            pubrinfo[j].dobj.nmspace = tbinfo->dobj.nmspace;
            pubrinfo[j].dobj.name = tbinfo->dobj.name;
            pubrinfo[j].pubname = gs_strdup(PQgetvalue(res, j, i_pubname));
            pubrinfo[j].pubtable = tbinfo;

            /* Decide whether we want to dump it */
            selectDumpablePublicationTable(&(pubrinfo[j].dobj));
        }
        PQclear(res);
    }
    destroyPQExpBuffer(query);
}

/*
 * dumpPublicationTable
 * 	  dump the definition of the given publication table mapping
 */
static void dumpPublicationTable(Archive *fout, const PublicationRelInfo *pubrinfo)
{
    TableInfo *tbinfo = pubrinfo->pubtable;
    PQExpBuffer query;
    PQExpBuffer tag;

    if (dataOnly || !pubrinfo->dobj.dump) {
        return;
    }

    tag = createPQExpBuffer();
    appendPQExpBuffer(tag, "%s %s", pubrinfo->pubname, tbinfo->dobj.name);

    query = createPQExpBuffer();
    appendPQExpBuffer(query, "ALTER PUBLICATION %s ADD TABLE ONLY", fmtId(pubrinfo->pubname));
    appendPQExpBuffer(query, " %s;", fmtId(tbinfo->dobj.name));

    /*
     * There is no point in creating drop query as drop query as the drop
     * is done by table drop.
     */
    ArchiveEntry(fout, pubrinfo->dobj.catId, pubrinfo->dobj.dumpId, tag->data, tbinfo->dobj.nmspace->dobj.name, NULL,
        "", false, "PUBLICATION TABLE", SECTION_POST_DATA, query->data, "", NULL, NULL, 0, NULL, NULL);

    destroyPQExpBuffer(tag);
    destroyPQExpBuffer(query);
}

/*
 * getSubscriptions
 * 	  get information about subscriptions
 */
void getSubscriptions(Archive *fout)
{
    PQExpBuffer query;
    PGresult *res;
    SubscriptionInfo *subinfo;
    int i_tableoid;
    int i_oid;
    int i_subname;
    int i_rolname;
    int i_subconninfo;
    int i_subslotname;
    int i_subsynccommit;
    int i_subpublications;
    int i_subbinary;
    int i_submatchddlowner;
    int i;
    int ntups;

    if (no_subscriptions || GetVersionNum(fout) < SUBSCRIPTION_VERSION) {
        return;
    }

    if (!isExecUserSuperRole(fout)) {
        write_msg(NULL, "WARNING: subscriptions not dumped because current user is not a superuser\n");
        return;
    }

    query = createPQExpBuffer();

    resetPQExpBuffer(query);

    /* Get the subscriptions in current database. */
    appendPQExpBuffer(query, "SELECT s.tableoid, s.oid, s.subname,"
        "(%s s.subowner) AS rolname, s.subconninfo, s.subslotname, "
        "s.subsynccommit, s.subpublications \n", username_subquery);

    if (GetVersionNum(fout) >= SUBSCRIPTION_BINARY_VERSION_NUM) {
        appendPQExpBuffer(query, ", s.subbinary\n");
    } else {
        appendPQExpBuffer(query, ", false AS subbinary\n");
    }

    if (GetVersionNum(fout) >= PUBLICATION_DDL_VERSION_NUM) {
        appendPQExpBuffer(query, ", s.submatchddlowner\n");
    } else {
        appendPQExpBuffer(query, ", true AS submatchddlowner\n");
    }

    appendPQExpBuffer(query, "FROM pg_catalog.pg_subscription s "
        "WHERE s.subdbid = (SELECT oid FROM pg_catalog.pg_database"
        "                   WHERE datname = current_database())");

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    ntups = PQntuples(res);
    if (ntups == 0) {
        PQclear(res);
        destroyPQExpBuffer(query);
        return;
    }

    i_tableoid = PQfnumber(res, "tableoid");
    i_oid = PQfnumber(res, "oid");
    i_subname = PQfnumber(res, "subname");
    i_rolname = PQfnumber(res, "rolname");
    i_subconninfo = PQfnumber(res, "subconninfo");
    i_subslotname = PQfnumber(res, "subslotname");
    i_subsynccommit = PQfnumber(res, "subsynccommit");
    i_subpublications = PQfnumber(res, "subpublications");
    i_subbinary = PQfnumber(res, "subbinary");
    i_submatchddlowner = PQfnumber(res, "submatchddlowner");

    subinfo = (SubscriptionInfo *)pg_malloc(ntups * sizeof(SubscriptionInfo));

    for (i = 0; i < ntups; i++) {
        subinfo[i].dobj.objType = DO_SUBSCRIPTION;
        subinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        subinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&subinfo[i].dobj);
        subinfo[i].dobj.name = gs_strdup(PQgetvalue(res, i, i_subname));
        subinfo[i].rolname = gs_strdup(PQgetvalue(res, i, i_rolname));
        subinfo[i].subconninfo = gs_strdup(PQgetvalue(res, i, i_subconninfo));
        if (PQgetisnull(res, i, i_subslotname)) {
            subinfo[i].subslotname = NULL;
        } else {
            subinfo[i].subslotname = gs_strdup(PQgetvalue(res, i, i_subslotname));
        }
        subinfo[i].subsynccommit = gs_strdup(PQgetvalue(res, i, i_subsynccommit));
        subinfo[i].subpublications = gs_strdup(PQgetvalue(res, i, i_subpublications));
        subinfo[i].subbinary = gs_strdup(PQgetvalue(res, i, i_subbinary));
        subinfo[i].submatchddlowner = gs_strdup(PQgetvalue(res, i, i_submatchddlowner));

        if (strlen(subinfo[i].rolname) == 0) {
            write_msg(NULL, "WARNING: owner of subscription \"%s\" appears to be invalid\n", subinfo[i].dobj.name);
        }

        /* Decide whether we want to dump it */
        selectDumpableObject(&(subinfo[i].dobj), fout);
    }
    PQclear(res);

    destroyPQExpBuffer(query);
}

/*
 * dumpSubscription
 * 	  dump the definition of the given subscription
 */
static void dumpSubscription(Archive *fout, const SubscriptionInfo *subinfo)
{
    PQExpBuffer delq;
    PQExpBuffer query;
    PQExpBuffer labelq;
    PQExpBuffer publications;
    char **pubnames = NULL;
    int npubnames = 0;
    int i;

    if (!subinfo->dobj.dump || dataOnly) {
        return;
    }

    delq = createPQExpBuffer();
    query = createPQExpBuffer();
    labelq = createPQExpBuffer();

    appendPQExpBuffer(delq, "DROP SUBSCRIPTION %s;\n", fmtId(subinfo->dobj.name));

    appendPQExpBuffer(query, "CREATE SUBSCRIPTION %s CONNECTION ", fmtId(subinfo->dobj.name));
    appendStringLiteralAH(query, subinfo->subconninfo, fout);

    /* Build list of quoted publications and append them to query. */
    if (!parsePGArray(subinfo->subpublications, &pubnames, &npubnames)) {
        write_msg(NULL, "WARNING: could not parse subpublications array\n");
        if (pubnames) {
            free(pubnames);
        }
        pubnames = NULL;
        npubnames = 0;
    }

    publications = createPQExpBuffer();
    for (i = 0; i < npubnames; i++) {
        if (i > 0) {
            appendPQExpBufferStr(publications, ", ");
        }

        appendPQExpBufferStr(publications, fmtId(pubnames[i]));
    }

    appendPQExpBuffer(query, " PUBLICATION %s WITH (enabled = false, slot_name = ", publications->data);
    if (subinfo->subslotname) {
        appendStringLiteralAH(query, subinfo->subslotname, fout);
    } else {
        appendPQExpBufferStr(query, "NONE");
    }

    if (strcmp(subinfo->subbinary, "t") == 0) {
        appendPQExpBuffer(query, ", binary = true");
    }

    if (strcmp(subinfo->submatchddlowner, "f") == 0) {
        appendPQExpBuffer(query, ", matchddlowner = false");
    }

    if (strcmp(subinfo->subsynccommit, "off") != 0) {
        appendPQExpBuffer(query, ", synchronous_commit = %s", fmtId(subinfo->subsynccommit));
    }

    appendPQExpBufferStr(query, ");\n");
    appendPQExpBuffer(labelq, "SUBSCRIPTION %s", fmtId(subinfo->dobj.name));
    ArchiveEntry(fout, subinfo->dobj.catId, subinfo->dobj.dumpId, subinfo->dobj.name, NULL, NULL, subinfo->rolname,
        false, "SUBSCRIPTION", SECTION_POST_DATA, query->data, delq->data, NULL, NULL, 0, NULL, NULL);

    dumpComment(fout, labelq->data, NULL, subinfo->rolname, subinfo->dobj.catId, 0, subinfo->dobj.dumpId);
    dumpSecLabel(fout, labelq->data, NULL, subinfo->rolname, subinfo->dobj.catId, 0, subinfo->dobj.dumpId);

    destroyPQExpBuffer(publications);
    if (pubnames) {
        free(pubnames);
    }

    destroyPQExpBuffer(delq);
    destroyPQExpBuffer(query);
    destroyPQExpBuffer(labelq);
}

static void binary_upgrade_set_type_oids_by_type_oid(
    Archive* fout, PQExpBuffer upgrade_buffer, Oid pg_type_oid, bool error_tbl)
{
    PQExpBuffer upgrade_query = createPQExpBuffer();
    PGresult* upgrade_res = NULL;
    Oid pg_type_array_oid;
    const char* errfunc_name = error_tbl ? "_etbl" : "";

    appendPQExpBuffer(upgrade_buffer, "\n-- For binary upgrade, must preserve pg_type oid\n");
    appendPQExpBuffer(upgrade_buffer,
        "SELECT binary_upgrade.set_next%s_pg_type_oid('%u'::pg_catalog.oid);\n\n",
        errfunc_name,
        pg_type_oid);

    /* we only support old >= 8.3 for binary upgrades */
    appendPQExpBuffer(upgrade_query,
        "SELECT typarray "
        "FROM pg_catalog.pg_type "
        "WHERE pg_type.oid = '%u'::pg_catalog.oid;",
        pg_type_oid);

    upgrade_res = ExecuteSqlQueryForSingleRow(fout, upgrade_query->data);

    pg_type_array_oid = atooid(PQgetvalue(upgrade_res, 0, PQfnumber(upgrade_res, "typarray")));

    if (OidIsValid(pg_type_array_oid)) {
        appendPQExpBuffer(upgrade_buffer, "\n-- For binary upgrade, must preserve pg_type array oid\n");
        appendPQExpBuffer(upgrade_buffer,
            "SELECT binary_upgrade.set_next%s_array_pg_type_oid('%u'::pg_catalog.oid);\n\n",
            errfunc_name,
            pg_type_array_oid);
    }

    PQclear(upgrade_res);
    destroyPQExpBuffer(upgrade_query);
}

static bool binary_upgrade_set_type_oids_by_rel_oid(
    Archive* fout, PQExpBuffer upgrade_buffer, Oid pg_rel_oid, bool error_tbl)
{
    PQExpBuffer upgrade_query = createPQExpBuffer();
    PGresult* upgrade_res = NULL;
    Oid pg_type_oid;
    bool toast_set = false;
    const char* errfunc_name = error_tbl ? "_etbl" : "";

    /* we only support old >= 8.3 for binary upgrades */
    appendPQExpBuffer(upgrade_query,
        "SELECT c.reltype AS crel, t.reltype AS trel "
        "FROM pg_catalog.pg_class c "
        "LEFT JOIN pg_catalog.pg_class t ON "
        "  (c.reltoastrelid = t.oid) "
        "WHERE c.oid = '%u'::pg_catalog.oid;",
        pg_rel_oid);

    upgrade_res = ExecuteSqlQueryForSingleRow(fout, upgrade_query->data);

    pg_type_oid = atooid(PQgetvalue(upgrade_res, 0, PQfnumber(upgrade_res, "crel")));

    binary_upgrade_set_type_oids_by_type_oid(fout, upgrade_buffer, pg_type_oid, error_tbl);

    if (!PQgetisnull(upgrade_res, 0, PQfnumber(upgrade_res, "trel"))) {
        /* Toast tables do not have pg_type array rows */
        Oid pg_type_toast_oid = atooid(PQgetvalue(upgrade_res, 0, PQfnumber(upgrade_res, "trel")));

        appendPQExpBuffer(upgrade_buffer, "\n-- For binary upgrade, must preserve pg_type toast oid\n");
        appendPQExpBuffer(upgrade_buffer,
            "SELECT binary_upgrade.set_next%s_toast_pg_type_oid('%u'::pg_catalog.oid);\n\n",
            errfunc_name,
            pg_type_toast_oid);

        toast_set = true;
    }

    PQclear(upgrade_res);
    destroyPQExpBuffer(upgrade_query);

    return toast_set;
}

static void binary_upgrade_set_pg_class_oids(
    Archive* fout, PQExpBuffer upgrade_buffer, Oid pg_class_oid, bool is_index, bool error_tbl)
{
    PQExpBuffer upgrade_query = createPQExpBuffer();
    PGresult* upgrade_res = NULL;
    Oid pg_class_reltoastrelid;
    Oid pg_class_reltoastidxid;
    const char* errfunc_name = error_tbl ? "_etbl" : "";
    Oid pg_class_relidfs;
    Oid pg_class_reltoastrelidfs;
    Oid pg_class_reltoastidxidfs;

    appendPQExpBuffer(upgrade_query,
        "SELECT c.relfilenode, c.reltoastrelid, "
        "   t.relfilenode as reltoastrelidfs, t.reltoastidxid, "
        "   ti.relfilenode reltoastidxidfs "
        "FROM pg_catalog.pg_class c "
        "     LEFT JOIN pg_catalog.pg_class t "
        "  ON ( t.oid = c.reltoastrelid) "
        "     LEFT JOIN pg_class  ti "
        "  ON (ti.oid = t.reltoastidxid) "
        "WHERE c.oid = '%u'::pg_catalog.oid;",
        pg_class_oid);

    upgrade_res = ExecuteSqlQueryForSingleRow(fout, upgrade_query->data);

    pg_class_reltoastrelid = atooid(PQgetvalue(upgrade_res, 0, PQfnumber(upgrade_res, "reltoastrelid")));
    pg_class_reltoastidxid = atooid(PQgetvalue(upgrade_res, 0, PQfnumber(upgrade_res, "reltoastidxid")));

    pg_class_relidfs = atooid(PQgetvalue(upgrade_res, 0, PQfnumber(upgrade_res, "relfilenode")));
    pg_class_reltoastrelidfs = atooid(PQgetvalue(upgrade_res, 0, PQfnumber(upgrade_res, "reltoastrelidfs")));
    pg_class_reltoastidxidfs = atooid(PQgetvalue(upgrade_res, 0, PQfnumber(upgrade_res, "reltoastidxidfs")));

    appendPQExpBuffer(upgrade_buffer, "\n-- For binary upgrade, must preserve pg_class oids\n");

    if (!is_index) {
        appendPQExpBuffer(upgrade_buffer,
            "SELECT binary_upgrade.set_next%s_heap_pg_class_oid('%u'::pg_catalog.oid);\n",
            errfunc_name,
            pg_class_oid);
        appendPQExpBuffer(upgrade_buffer,
            "SELECT binary_upgrade.set_next%s_heap_pg_class_rfoid('%u'::pg_catalog.oid);\n",
            errfunc_name,
            pg_class_relidfs);
        /* only tables have toast tables, not indexes */
        if (OidIsValid(pg_class_reltoastrelid)) {
            /*
             * One complexity is that the table definition might not require
             * the creation of a TOAST table, and the TOAST table might have
             * been created long after table creation, when the table was
             * loaded with wide data.  By setting the TOAST oid we force
             * creation of the TOAST heap and TOAST index by the backend so we
             * can cleanly copy the files during binary upgrade.
             */

            appendPQExpBuffer(upgrade_buffer,
                "SELECT binary_upgrade.set_next%s_toast_pg_class_oid('%u'::pg_catalog.oid);\n",
                errfunc_name,
                pg_class_reltoastrelid);
            appendPQExpBuffer(upgrade_buffer,
                "SELECT binary_upgrade.set_next%s_toast_pg_class_rfoid('%u'::pg_catalog.oid);\n",
                errfunc_name,
                pg_class_reltoastrelidfs);

            /* every toast table has an index */
            appendPQExpBuffer(upgrade_buffer,
                "SELECT binary_upgrade.set_next%s_index_pg_class_oid('%u'::pg_catalog.oid);\n",
                errfunc_name,
                pg_class_reltoastidxid);
            appendPQExpBuffer(upgrade_buffer,
                "SELECT binary_upgrade.set_next%s_index_pg_class_rfoid('%u'::pg_catalog.oid);\n",
                errfunc_name,
                pg_class_reltoastidxidfs);
        }
    } else {
        appendPQExpBuffer(upgrade_buffer,
            "SELECT binary_upgrade.set_next%s_index_pg_class_oid('%u'::pg_catalog.oid);\n",
            errfunc_name,
            pg_class_oid);
        appendPQExpBuffer(upgrade_buffer,
            "SELECT binary_upgrade.set_next%s_index_pg_class_rfoid('%u'::pg_catalog.oid);\n",
            errfunc_name,
            pg_class_relidfs);
    }

    appendPQExpBuffer(upgrade_buffer, "\n");

    PQclear(upgrade_res);
    destroyPQExpBuffer(upgrade_query);
}

static void binary_upgrade_set_error_table_oids(Archive* fout, PQExpBuffer upgrade_buffer, TableInfo* tbinfo)
{
    PQExpBuffer upgrade_query;
    PGresult* res = NULL;
    Oid error_table_oid = 0;

    upgrade_query = createPQExpBuffer();

    appendPQExpBuffer(upgrade_query,
        "SELECT c.oid as error_table_id"
        "  FROM pg_catalog.pg_class c "
        "  WHERE c.relnamespace = '%u' "
        "    AND c.relname = ( SELECT ( SELECT option_value "
        "        FROM pg_catalog.pg_options_to_table(ft.ftoptions) "
        "        WHERE option_name = 'error_table' ) "
        "      FROM pg_catalog.pg_foreign_table ft "
        "      WHERE ft.ftrelid = '%u' );",
        tbinfo->dobj.nmspace->dobj.catId.oid,
        tbinfo->dobj.catId.oid);

    res = ExecuteSqlQuery(fout, upgrade_query->data, PGRES_TUPLES_OK);
    if (PQntuples(res) == 0) {
        PQclear(res);
        destroyPQExpBuffer(upgrade_query);
        return;
    }

    error_table_oid = atooid(PQgetvalue(res, 0, 0));

    binary_upgrade_set_type_oids_by_rel_oid(fout, upgrade_buffer, error_table_oid, true);

    binary_upgrade_set_pg_class_oids(fout, upgrade_buffer, error_table_oid, false, true);

    PQclear(res);
    destroyPQExpBuffer(upgrade_query);
}

/*
 * If the DumpableObject is a member of an extension, add a suitable
 * ALTER EXTENSION ADD command to the creation commands in upgrade_buffer.
 */
static void binary_upgrade_extension_member(PQExpBuffer upgrade_buffer, DumpableObject* dobj, const char* objlabel)
{
    DumpableObject* extobj = NULL;
    int i;

    if (!dobj->ext_member || (false == include_extensions))
        return;

    /*
     * Find the parent extension.  We could avoid this search if we wanted to
     * add a link field to DumpableObject, but the space costs of that would
     * be considerable.  We assume that member objects could only have a
     * direct dependency on their own extension, not any others.
     */
    for (i = 0; i < dobj->nDeps; i++) {
        extobj = findObjectByDumpId(dobj->dependencies[i]);
        if (NULL != extobj && extobj->objType == DO_EXTENSION)
            break;
        extobj = NULL;
    }
    if (extobj == NULL)
        exit_horribly(NULL, "could not find parent extension for %s\n", objlabel);

    appendPQExpBuffer(upgrade_buffer, "\n-- For binary upgrade, handle extension membership the hard way\n");
    appendPQExpBuffer(upgrade_buffer, "ALTER EXTENSION %s ADD %s;\n", fmtId(extobj->name), objlabel);
}

/*
 * getNamespaces:
 *	  read all namespaces in the system catalogs and return them in the
 * NamespaceInfo* structure
 *
 *	numNamespaces is set to the number of namespaces read in
 */
NamespaceInfo* getNamespaces(Archive* fout, int* numNamespaces)
{
    PGresult* res = NULL;
    int ntups;
    int i;
    PQExpBuffer query;
    NamespaceInfo* nsinfo = NULL;
    int i_tableoid;
    int i_oid;
    int i_nspname;
    int i_rolname;
    int i_nspacl;
    int i_nspblockchain;
    int i_collation;

    /*
     * Before 7.3, there are no real namespaces; create two dummy entries, one
     * for user stuff and one for system stuff.
     */
    if (fout->remoteVersion < 70300) {
        nsinfo = (NamespaceInfo*)pg_malloc(2 * sizeof(NamespaceInfo));

        nsinfo[0].dobj.objType = DO_NAMESPACE;
        nsinfo[0].dobj.catId.tableoid = 0;
        nsinfo[0].dobj.catId.oid = 0;
        AssignDumpId(&nsinfo[0].dobj);
        nsinfo[0].dobj.name = gs_strdup("public");
        nsinfo[0].rolname = gs_strdup("");
        nsinfo[0].nspacl = gs_strdup("");
        nsinfo[0].hasBlockchain = false;
        nsinfo[0].collate = 0;

        selectDumpableNamespace(&nsinfo[0]);

        nsinfo[1].dobj.objType = DO_NAMESPACE;
        nsinfo[1].dobj.catId.tableoid = 0;
        nsinfo[1].dobj.catId.oid = 1;
        AssignDumpId(&nsinfo[1].dobj);
        nsinfo[1].dobj.name = gs_strdup("pg_catalog");
        nsinfo[1].rolname = gs_strdup("");
        nsinfo[1].nspacl = gs_strdup("");
        nsinfo[1].hasBlockchain = false;
        nsinfo[1].collate = 0;

        selectDumpableNamespace(&nsinfo[1]);

        *numNamespaces = 2;

        return nsinfo;
    }

    query = createPQExpBuffer();

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, "pg_catalog");

    /*
     * we fetch all namespaces including system ones, so that every object we
     * read in can be linked to a containing nmspace.
     */
    appendPQExpBuffer(query,
        "SELECT tableoid, oid, nspname, "
        "(%s nspowner) AS rolname, "
        "nspacl, nspblockchain, ", username_subquery);
    if (GetVersionNum(fout) >= CHARACTER_SET_VERSION_NUM) {
        appendPQExpBuffer(query, "nspcollation FROM pg_namespace");
    } else {
        appendPQExpBuffer(query, "NULL AS nspcollation FROM pg_namespace");
    }

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    ntups = PQntuples(res);

    nsinfo = (NamespaceInfo*)pg_malloc(ntups * sizeof(NamespaceInfo));

    i_tableoid = PQfnumber(res, "tableoid");
    i_oid = PQfnumber(res, "oid");
    i_nspname = PQfnumber(res, "nspname");
    i_rolname = PQfnumber(res, "rolname");
    i_nspacl = PQfnumber(res, "nspacl");
    i_nspblockchain = PQfnumber(res, "nspblockchain");
    i_collation = PQfnumber(res, "nspcollation");

    for (i = 0; i < ntups; i++) {
        nsinfo[i].dobj.objType = DO_NAMESPACE;
        nsinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        nsinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&nsinfo[i].dobj);
        nsinfo[i].dobj.name = gs_strdup(PQgetvalue(res, i, i_nspname));
        nsinfo[i].rolname = gs_strdup(PQgetvalue(res, i, i_rolname));
        nsinfo[i].nspacl = gs_strdup(PQgetvalue(res, i, i_nspacl));
        nsinfo[i].hasBlockchain = (strcmp(PQgetvalue(res, i, i_nspblockchain), "t") == 0) ? true : false;
        if (PQgetisnull(res, i, i_collation)) {
            nsinfo[i].collate = 0;
        } else {
            nsinfo[i].collate = atooid(PQgetvalue(res, i, i_collation));
        }

        /* Decide whether to dump this nmspace */
        selectDumpableNamespace(&nsinfo[i]);
#ifndef ENABLE_MULTIPLE_NODES
        if (unlikely(isDB4AIschema(&nsinfo[i]) && nsinfo[i].dobj.dump)) {
            selectSourceSchema(fout, "db4ai");
            resetPQExpBuffer(query);
            appendPQExpBuffer(query, "SELECT id FROM snapshot");
            PGresult *res_ = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
            if (PQntuples(res_) != 0 and outputClean == 0) {
                exit_horribly(NULL, "Using options -c/--clean to dump db4ai schema. Or clean this schema.\n");
            }
            nsinfo[i].dobj.dump = (PQntuples(res_) != 0 and outputClean == 1);
            PQclear(res_);
        }
#endif
    }

    PQclear(res);
    destroyPQExpBuffer(query);

    *numNamespaces = ntups;

    return nsinfo;
}

/*
 * findNamespace:
 *		given a nmspace OID and an object OID, look up the info read by
 *		getNamespaces
 *
 * NB: for pre-7.3 source database, we use object OID to guess whether it's
 * a system object or not.	In 7.3 and later there is no guessing, and we
 * don't use objoid at all.
 */
static NamespaceInfo* findNamespace(Archive* fout, Oid nsoid, Oid objoid)
{
    NamespaceInfo* nsinfo = NULL;

    if (fout->remoteVersion >= 70300) {
        nsinfo = findNamespaceByOid(nsoid);
    } else {
        /* This code depends on the dummy objects set up by getNamespaces. */
        Oid i;

        if (objoid > g_last_builtin_oid)
            i = 0; /* user object */
        else
            i = 1; /* system object */
        nsinfo = findNamespaceByOid(i);
    }

    if (nsinfo == NULL)
        exit_horribly(NULL, "schema with OID %u does not exist\n", nsoid);

    return nsinfo;
}

/*
 * getExtensions:
 *	  read all extensions in the system catalogs and return them in the
 * ExtensionInfo* structure
 *
 *	numExtensions is set to the number of extensions read in
 */
ExtensionInfo* getExtensions(Archive* fout, int* numExtensions)
{
    PGresult* res = NULL;
    int ntups;
    int i;
    PQExpBuffer query;
    ExtensionInfo* extinfo = NULL;
    int i_tableoid;
    int i_oid;
    int i_extname;
    int i_nspname;
    int i_extrelocatable;
    int i_extversion;
    int i_extconfig;
    int i_extcondition;
    int i_extrole;

    /*
     * Before 9.1, there are no extensions.
     */
    if (fout->remoteVersion < 90100) {
        *numExtensions = 0;
        return NULL;
    }

    query = createPQExpBuffer();

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, "pg_catalog");

    appendPQExpBuffer(query,
        "SELECT x.tableoid, x.oid, "
        "(%s x.extowner) AS rolname, "
        "x.extname, n.nspname, x.extrelocatable, x.extversion, x.extconfig, x.extcondition "
        "FROM pg_extension x "
        "JOIN pg_namespace n ON n.oid = x.extnamespace",
        username_subquery);

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    ntups = PQntuples(res);

    extinfo = (ExtensionInfo*)pg_malloc(ntups * sizeof(ExtensionInfo));

    i_tableoid = PQfnumber(res, "tableoid");
    i_oid = PQfnumber(res, "oid");
    i_extname = PQfnumber(res, "extname");
    i_nspname = PQfnumber(res, "nspname");
    i_extrelocatable = PQfnumber(res, "extrelocatable");
    i_extversion = PQfnumber(res, "extversion");
    i_extconfig = PQfnumber(res, "extconfig");
    i_extcondition = PQfnumber(res, "extcondition");
    i_extrole = PQfnumber(res, "rolname");

    for (i = 0; i < ntups; i++) {
        extinfo[i].dobj.objType = DO_EXTENSION;
        extinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        extinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&extinfo[i].dobj);
        extinfo[i].dobj.name = gs_strdup(PQgetvalue(res, i, i_extname));
        extinfo[i].nmspace = gs_strdup(PQgetvalue(res, i, i_nspname));
        extinfo[i].relocatable = *(PQgetvalue(res, i, i_extrelocatable)) == 't';
        extinfo[i].extversion = gs_strdup(PQgetvalue(res, i, i_extversion));
        extinfo[i].extconfig = gs_strdup(PQgetvalue(res, i, i_extconfig));
        extinfo[i].extcondition = gs_strdup(PQgetvalue(res, i, i_extcondition));
        extinfo[i].extrole = gs_strdup(PQgetvalue(res, i, i_extrole));

        /* Decide whether we want to dump it */
        selectDumpableExtension(&(extinfo[i]));
    }

    PQclear(res);
    destroyPQExpBuffer(query);

    *numExtensions = ntups;

    return extinfo;
}

/*
 * getTypes:
 *	  read all types in the system catalogs and return them in the
 * TypeInfo* structure
 *
 *	numTypes is set to the number of types read in
 *
 * NB: this must run after getFuncs() because we assume we can do
 * findFuncByOid().
 */
TypeInfo* getTypes(Archive* fout, int* numTypes)
{
    PGresult* res = NULL;
    int ntups;
    int i;
    PQExpBuffer query = createPQExpBuffer();
    TypeInfo* tyinfo = NULL;
    ShellTypeInfo* stinfo = NULL;
    int i_tableoid;
    int i_oid;
    int i_typname;
    int i_typnamespace;
    int i_typacl;
    int i_rolname;
    int i_typinput;
    int i_typoutput;
    int i_typelem;
    int i_typrelid;
    int i_typrelkind;
    int i_typtype;
    int i_typisdefined;
    int i_isarray;

    /*
     * we include even the built-in types because those may be used as array
     * elements by user-defined types
     *
     * we filter out the built-in types when we dump out the types
     *
     * same approach for undefined (shell) types and array types
     *
     * Note: as of 8.3 we can reliably detect whether a type is an
     * auto-generated array type by checking the element type's typarray.
     * (Before that the test is capable of generating false positives.) We
     * still check for name beginning with '_', though, so as to avoid the
     * cost of the subselect probe for all standard types.	This would have to
     * be revisited if the backend ever allows renaming of array types.
     */

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, "pg_catalog");
    if (GetVersionNum(fout) >= 92838 && findDBCompatibility(fout, PQdb(GetConnection(fout))) &&
        hasSpecificExtension(fout, "dolphin")) {
        appendPQExpBuffer(query,
            "SELECT tableoid, oid, typname, "
            "typnamespace, typacl, "
            "(%s typowner) AS rolname, "
            "typinput::oid AS typinput, "
            "typoutput::oid AS typoutput, typelem, typrelid, "
            "CASE WHEN typrelid = 0 THEN ' '::`char` "
            "ELSE (SELECT relkind FROM pg_class WHERE oid = typrelid) END AS typrelkind, "
            "typtype, typisdefined, "
            "typname[0] = '_' AND typelem != 0 AND "
            "(SELECT typarray FROM pg_type te WHERE oid = pg_type.typelem) = oid AS isarray "
            "FROM pg_type",
            username_subquery);
    } else if (fout->remoteVersion >= 90200) {
        appendPQExpBuffer(query,
            "SELECT tableoid, oid, typname, "
            "typnamespace, typacl, "
            "(%s typowner) AS rolname, "
            "typinput::oid AS typinput, "
            "typoutput::oid AS typoutput, typelem, typrelid, "
            "CASE WHEN typrelid = 0 THEN ' '::\"char\" "
            "ELSE (SELECT relkind FROM pg_class WHERE oid = typrelid) END AS typrelkind, "
            "typtype, typisdefined, "
            "typname[0] = '_' AND typelem != 0 AND "
            "(SELECT typarray FROM pg_type te WHERE oid = pg_type.typelem) = oid AS isarray "
            "FROM pg_type",
            username_subquery);
    } else if (fout->remoteVersion >= 80300) {
        appendPQExpBuffer(query,
            "SELECT tableoid, oid, typname, "
            "typnamespace, '{=U}' AS typacl, "
            "(%s typowner) AS rolname, "
            "typinput::oid AS typinput, "
            "typoutput::oid AS typoutput, typelem, typrelid, "
            "CASE WHEN typrelid = 0 THEN ' '::\"char\" "
            "ELSE (SELECT relkind FROM pg_class WHERE oid = typrelid) END AS typrelkind, "
            "typtype, typisdefined, "
            "typname[0] = '_' AND typelem != 0 AND "
            "(SELECT typarray FROM pg_type te WHERE oid = pg_type.typelem) = oid AS isarray "
            "FROM pg_type",
            username_subquery);
    } else if (fout->remoteVersion >= 70300) {
        appendPQExpBuffer(query,
            "SELECT tableoid, oid, typname, "
            "typnamespace, '{=U}' AS typacl, "
            "(%s typowner) AS rolname, "
            "typinput::oid AS typinput, "
            "typoutput::oid AS typoutput, typelem, typrelid, "
            "CASE WHEN typrelid = 0 THEN ' '::\"char\" "
            "ELSE (SELECT relkind FROM pg_class WHERE oid = typrelid) END AS typrelkind, "
            "typtype, typisdefined, "
            "typname[0] = '_' AND typelem != 0 AS isarray "
            "FROM pg_type",
            username_subquery);
    } else if (fout->remoteVersion >= 70100) {
        appendPQExpBuffer(query,
            "SELECT tableoid, oid, typname, "
            "0::oid AS typnamespace, '{=U}' AS typacl, "
            "(%s typowner) AS rolname, "
            "typinput::oid AS typinput, "
            "typoutput::oid AS typoutput, typelem, typrelid, "
            "CASE WHEN typrelid = 0 THEN ' '::\"char\" "
            "ELSE (SELECT relkind FROM pg_class WHERE oid = typrelid) END AS typrelkind, "
            "typtype, typisdefined, "
            "typname[0] = '_' AND typelem != 0 AS isarray "
            "FROM pg_type",
            username_subquery);
    } else {
        appendPQExpBuffer(query,
            "SELECT "
            "(SELECT oid FROM pg_class WHERE relname = 'pg_type') AS tableoid, "
            "oid, typname, "
            "0::oid AS typnamespace, '{=U}' AS typacl, "
            "(%s typowner) AS rolname, "
            "typinput::oid AS typinput, "
            "typoutput::oid AS typoutput, typelem, typrelid, "
            "CASE WHEN typrelid = 0 THEN ' '::\"char\" "
            "ELSE (SELECT relkind FROM pg_class WHERE oid = typrelid) END AS typrelkind, "
            "typtype, typisdefined, "
            "typname[0] = '_' AND typelem != 0 AS isarray "
            "FROM pg_type",
            username_subquery);
    }

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    ntups = PQntuples(res);

    tyinfo = (TypeInfo*)pg_malloc(ntups * sizeof(TypeInfo));

    i_tableoid = PQfnumber(res, "tableoid");
    i_oid = PQfnumber(res, "oid");
    i_typname = PQfnumber(res, "typname");
    i_typnamespace = PQfnumber(res, "typnamespace");
    i_typacl = PQfnumber(res, "typacl");
    i_rolname = PQfnumber(res, "rolname");
    i_typinput = PQfnumber(res, "typinput");
    i_typoutput = PQfnumber(res, "typoutput");
    i_typelem = PQfnumber(res, "typelem");
    i_typrelid = PQfnumber(res, "typrelid");
    i_typrelkind = PQfnumber(res, "typrelkind");
    i_typtype = PQfnumber(res, "typtype");
    i_typisdefined = PQfnumber(res, "typisdefined");
    i_isarray = PQfnumber(res, "isarray");

    for (i = 0; i < ntups; i++) {
        if (NULL == PQgetvalue(res, i, i_tableoid) ||
            NULL == PQgetvalue(res, i, i_oid) ||
            NULL == PQgetvalue(res, i, i_typname) ||
            NULL == PQgetvalue(res, i, i_typnamespace) ||
            NULL == PQgetvalue(res, i, i_rolname) ||
            NULL == PQgetvalue(res, i, i_typacl) ||
            NULL == PQgetvalue(res, i, i_typelem) ||
            NULL == PQgetvalue(res, i, i_typrelkind) ||
            NULL == PQgetvalue(res, i, i_typtype)) {
            exit_horribly(NULL, "PQgetvalue cannot be NULL.\n");
        }
        tyinfo[i].dobj.objType = DO_TYPE;
        tyinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        tyinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&tyinfo[i].dobj);
        tyinfo[i].dobj.name = gs_strdup(PQgetvalue(res, i, i_typname));
        tyinfo[i].dobj.nmspace =
            findNamespace(fout, atooid(PQgetvalue(res, i, i_typnamespace)), tyinfo[i].dobj.catId.oid);
        tyinfo[i].rolname = gs_strdup(PQgetvalue(res, i, i_rolname));
        tyinfo[i].typacl = gs_strdup(PQgetvalue(res, i, i_typacl));
        tyinfo[i].typelem = atooid(PQgetvalue(res, i, i_typelem));
        tyinfo[i].typrelid = atooid(PQgetvalue(res, i, i_typrelid));
        tyinfo[i].typrelkind = *PQgetvalue(res, i, i_typrelkind);
        tyinfo[i].typtype = *PQgetvalue(res, i, i_typtype);
        tyinfo[i].shellType = NULL;

        if (strcmp(PQgetvalue(res, i, i_typisdefined), "t") == 0)
            tyinfo[i].isDefined = true;
        else
            tyinfo[i].isDefined = false;

        if (strcmp(PQgetvalue(res, i, i_isarray), "t") == 0)
            tyinfo[i].isArray = true;
        else
            tyinfo[i].isArray = false;

        /* Decide whether we want to dump it */
        selectDumpableType(fout, &tyinfo[i]);

        /*
         * If it's a domain, fetch info about its constraints, if any
         */
        tyinfo[i].nDomChecks = 0;
        tyinfo[i].domChecks = NULL;
        if (tyinfo[i].dobj.dump && tyinfo[i].typtype == TYPTYPE_DOMAIN)
            getDomainConstraints(fout, &(tyinfo[i]));

        /*
         * If it's a base type, make a DumpableObject representing a shell
         * definition of the type.	We will need to dump that ahead of the I/O
         * functions for the type.	Similarly, range types need a shell
         * definition in case they have a canonicalize function.
         *
         * Note: the shell type doesn't have a catId.  You might think it
         * should copy the base type's catId, but then it might capture the
         * pg_depend entries for the type, which we don't want.
         */
        if (tyinfo[i].dobj.dump && (tyinfo[i].typtype == TYPTYPE_BASE || tyinfo[i].typtype == TYPTYPE_RANGE)) {
            stinfo = (ShellTypeInfo*)pg_malloc(sizeof(ShellTypeInfo));
            stinfo->dobj.objType = DO_SHELL_TYPE;
            stinfo->dobj.catId = nilCatalogId;
            AssignDumpId(&stinfo->dobj);
            stinfo->dobj.name = gs_strdup(tyinfo[i].dobj.name);
            stinfo->dobj.nmspace = tyinfo[i].dobj.nmspace;
            stinfo->baseType = &(tyinfo[i]);
            tyinfo[i].shellType = stinfo;

            /*
             * Initially mark the shell type as not to be dumped.  We'll only
             * dump it if the I/O or canonicalize functions need to be dumped;
             * this is taken care of while sorting dependencies.
             */
            stinfo->dobj.dump = false;

            /*
             * However, if dumping from pre-7.3, there will be no dependency
             * info so we have to fake it here.  We only need to worry about
             * typinput and typoutput since the other functions only exist
             * post-7.3.
             */
            if (fout->remoteVersion < 70300) {
                Oid typinput;
                Oid typoutput;
                FuncInfo* funcInfo = NULL;

                typinput = atooid(PQgetvalue(res, i, i_typinput));
                typoutput = atooid(PQgetvalue(res, i, i_typoutput));

                funcInfo = findFuncByOid(typinput);
                if (NULL != funcInfo && funcInfo->dobj.dump) {
                    /* base type depends on function */
                    addObjectDependency(&tyinfo[i].dobj, funcInfo->dobj.dumpId);
                    /* function depends on shell type */
                    addObjectDependency(&funcInfo->dobj, stinfo->dobj.dumpId);
                    /* mark shell type as to be dumped */
                    stinfo->dobj.dump = true;
                }

                funcInfo = findFuncByOid(typoutput);
                if (NULL != funcInfo && funcInfo->dobj.dump) {
                    /* base type depends on function */
                    addObjectDependency(&tyinfo[i].dobj, funcInfo->dobj.dumpId);
                    /* function depends on shell type */
                    addObjectDependency(&funcInfo->dobj, stinfo->dobj.dumpId);
                    /* mark shell type as to be dumped */
                    stinfo->dobj.dump = true;
                }
            }
        }
    }

    *numTypes = ntups;

    PQclear(res);

    destroyPQExpBuffer(query);

    return tyinfo;
}

/*
 * getOperators:
 *	  read all operators in the system catalogs and return them in the
 * OprInfo* structure
 *
 *	numOprs is set to the number of operators read in
 */
OprInfo* getOperators(Archive* fout, int* numOprs)
{
    PGresult* res = NULL;
    int ntups = 0;
    int i = 0;
    PQExpBuffer query = createPQExpBuffer();
    OprInfo* oprinfo = NULL;
    int i_tableoid = 0;
    int i_oid = 0;
    int i_oprname = 0;
    int i_oprnamespace = 0;
    int i_rolname = 0;
    int i_oprkind = 0;
    int i_oprcode = 0;

    /*
     * find all operators, including builtin operators; we filter out
     * system-defined operators at dump-out time.
     */

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, "pg_catalog");

    if (fout->remoteVersion >= 70300) {
        appendPQExpBuffer(query,
            "SELECT tableoid, oid, oprname, "
            "oprnamespace, "
            "(%s oprowner) AS rolname, "
            "oprkind, "
            "oprcode::oid AS oprcode "
            "FROM pg_operator",
            username_subquery);
    } else if (fout->remoteVersion >= 70100) {
        appendPQExpBuffer(query,
            "SELECT tableoid, oid, oprname, "
            "0::oid AS oprnamespace, "
            "(%s oprowner) AS rolname, "
            "oprkind, "
            "oprcode::oid AS oprcode "
            "FROM pg_operator",
            username_subquery);
    } else {
        appendPQExpBuffer(query,
            "SELECT "
            "(SELECT oid FROM pg_class WHERE relname = 'pg_operator') AS tableoid, "
            "oid, oprname, "
            "0::oid AS oprnamespace, "
            "(%s oprowner) AS rolname, "
            "oprkind, "
            "oprcode::oid AS oprcode "
            "FROM pg_operator",
            username_subquery);
    }

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);
    *numOprs = ntups;

    oprinfo = (OprInfo*)pg_malloc(ntups * sizeof(OprInfo));

    i_tableoid = PQfnumber(res, "tableoid");
    i_oid = PQfnumber(res, "oid");
    i_oprname = PQfnumber(res, "oprname");
    i_oprnamespace = PQfnumber(res, "oprnamespace");
    i_rolname = PQfnumber(res, "rolname");
    i_oprkind = PQfnumber(res, "oprkind");
    i_oprcode = PQfnumber(res, "oprcode");

    for (i = 0; i < ntups; i++) {
        oprinfo[i].dobj.objType = DO_OPERATOR;
        oprinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        oprinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&oprinfo[i].dobj);
        oprinfo[i].dobj.name = gs_strdup(PQgetvalue(res, i, i_oprname));
        oprinfo[i].dobj.nmspace =
            findNamespace(fout, atooid(PQgetvalue(res, i, i_oprnamespace)), oprinfo[i].dobj.catId.oid);
        oprinfo[i].rolname = gs_strdup(PQgetvalue(res, i, i_rolname));
        oprinfo[i].oprkind = (PQgetvalue(res, i, i_oprkind))[0];
        oprinfo[i].oprcode = atooid(PQgetvalue(res, i, i_oprcode));

        /* Decide whether we want to dump it */
        selectDumpableObject(&(oprinfo[i].dobj), fout);
    }

    PQclear(res);

    destroyPQExpBuffer(query);

    return oprinfo;
}

/*
 * getCollations:
 *	  read all collations in the system catalogs and return them in the
 * CollInfo* structure
 *
 *	numCollations is set to the number of collations read in
 */
CollInfo* getCollations(Archive* fout, int* numCollations)
{
    PGresult* res = NULL;
    int ntups;
    int i;
    PQExpBuffer query;
    CollInfo* collinfo = NULL;
    int i_tableoid;
    int i_oid;
    int i_collname;
    int i_collnamespace;
    int i_rolname;

    /* Collations didn't exist pre-9.1 */
    if (fout->remoteVersion < 90100) {
        *numCollations = 0;
        return NULL;
    }

    query = createPQExpBuffer();

    /*
     * find all collations, including builtin collations; we filter out
     * system-defined collations at dump-out time.
     */

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, "pg_catalog");

    appendPQExpBuffer(query,
        "SELECT tableoid, oid, collname, "
        "collnamespace, "
        "(%s collowner) AS rolname "
        "FROM pg_collation",
        username_subquery);

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    ntups = PQntuples(res);
    *numCollations = ntups;

    collinfo = (CollInfo*)pg_malloc(ntups * sizeof(CollInfo));

    i_tableoid = PQfnumber(res, "tableoid");
    i_oid = PQfnumber(res, "oid");
    i_collname = PQfnumber(res, "collname");
    i_collnamespace = PQfnumber(res, "collnamespace");
    i_rolname = PQfnumber(res, "rolname");

    for (i = 0; i < ntups; i++) {
        collinfo[i].dobj.objType = DO_COLLATION;
        collinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        collinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&collinfo[i].dobj);
        collinfo[i].dobj.name = gs_strdup(PQgetvalue(res, i, i_collname));
        collinfo[i].dobj.nmspace =
            findNamespace(fout, atooid(PQgetvalue(res, i, i_collnamespace)), collinfo[i].dobj.catId.oid);
        collinfo[i].rolname = gs_strdup(PQgetvalue(res, i, i_rolname));

        /* Decide whether we want to dump it */
        selectDumpableObject(&(collinfo[i].dobj), fout);
    }

    PQclear(res);

    destroyPQExpBuffer(query);

    return collinfo;
}

/*
 * getConversions:
 *	  read all conversions in the system catalogs and return them in the
 * ConvInfo* structure
 *
 *	numConversions is set to the number of conversions read in
 */
ConvInfo* getConversions(Archive* fout, int* numConversions)
{
    PGresult* res = NULL;
    int ntups = 0;
    int i = 0;
    PQExpBuffer query = createPQExpBuffer();
    ConvInfo* convinfo = NULL;
    int i_tableoid = 0;
    int i_oid = 0;
    int i_conname = 0;
    int i_connamespace = 0;
    int i_rolname = 0;

    /* Conversions didn't exist pre-7.3 */
    if (fout->remoteVersion < 70300) {
        *numConversions = 0;
        destroyPQExpBuffer(query);
        return NULL;
    }

    /*
     * find all conversions, including builtin conversions; we filter out
     * system-defined conversions at dump-out time.
     */

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, "pg_catalog");

    appendPQExpBuffer(query,
        "SELECT tableoid, oid, conname, "
        "connamespace, "
        "(%s conowner) AS rolname "
        "FROM pg_conversion",
        username_subquery);

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    ntups = PQntuples(res);
    *numConversions = ntups;

    convinfo = (ConvInfo*)pg_malloc(ntups * sizeof(ConvInfo));

    i_tableoid = PQfnumber(res, "tableoid");
    i_oid = PQfnumber(res, "oid");
    i_conname = PQfnumber(res, "conname");
    i_connamespace = PQfnumber(res, "connamespace");
    i_rolname = PQfnumber(res, "rolname");

    for (i = 0; i < ntups; i++) {
        convinfo[i].dobj.objType = DO_CONVERSION;
        convinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        convinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&convinfo[i].dobj);
        convinfo[i].dobj.name = gs_strdup(PQgetvalue(res, i, i_conname));
        convinfo[i].dobj.nmspace =
            findNamespace(fout, atooid(PQgetvalue(res, i, i_connamespace)), convinfo[i].dobj.catId.oid);
        convinfo[i].rolname = gs_strdup(PQgetvalue(res, i, i_rolname));

        /* Decide whether we want to dump it */
        selectDumpableObject(&(convinfo[i].dobj), fout);
    }

    PQclear(res);

    destroyPQExpBuffer(query);

    return convinfo;
}

bool IsRbObject(Archive* fout, Oid classid, Oid objid, const char* objname)
{
    PGresult* res = NULL;
    bool isRecycleObj = false;
    int colNum = 0;
    int tupNum = 0;
    char* recycleObject = NULL;
    char* f = "f";

    if (GetVersionNum(fout) >= RB_OBJECT_VERSION_NUM) {
        PQExpBuffer query = createPQExpBuffer();
        /* Make sure we are in proper schema */
        selectSourceSchema(fout, "pg_catalog");
        appendPQExpBuffer(query,
            "SELECT pg_catalog.gs_is_recycle_obj(%u, %u, NULL)",
            classid,
            objid);
        res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

        colNum = PQfnumber(res, "gs_is_recycle_obj");
        recycleObject = gs_strdup(PQgetvalue(res, tupNum, colNum));
        if (strcmp(recycleObject, f) == 0) {
            isRecycleObj = false;
        } else {
            isRecycleObj = true;
        }
        GS_FREE(recycleObject);
        PQclear(res);
        destroyPQExpBuffer(query);
        return isRecycleObj;
    } else {
        return false;
    }
    
}

uint32 GetVersionNum(Archive *fout)
{
    if (unlikely(fout->workingVersionNum == 0)) {
        fout->workingVersionNum = GetVersionNumFromServer(fout);
    }
    return fout->workingVersionNum;
}

uint32 GetVersionNumFromServer(Archive *fout)
{
    PGresult* res = NULL;
    PQExpBuffer query = createPQExpBuffer();
    uint32 versionNum = 0;
    int colNum = 0;
    int tupNum = 0;

    appendPQExpBuffer(query, "SELECT pg_catalog.working_version_num()");
    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    colNum = PQfnumber(res, "working_version_num");
    versionNum = atooid(PQgetvalue(res, tupNum, colNum));

    PQclear(res);
    destroyPQExpBuffer(query);

    return versionNum;
}

/*
 * getAccessMethods:
 *    read all user-defined access methods in the system catalogs and return
 *    them in the AccessMethodInfo* structure
 *
 *  numAccessMethods is set to the number of access methods read in
 */
AccessMethodInfo* getAccessMethods(Archive *fout, int *numAccessMethods)
{
    PGresult   *res;
    int         ntups;
    int         i;
    PQExpBuffer query;
    AccessMethodInfo *aminfo;
    int         i_tableoid;
    int         i_oid;
    int         i_amname;
    int         i_amhandler;

    query = createPQExpBuffer();

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, "pg_catalog");

    /* Select all access methods from pg_am table */
    appendPQExpBufferStr(query, "SELECT tableoid, oid, amname, "
        "amhandler::pg_catalog.regproc AS amhandler "
        "FROM pg_am where amhandler != 0");

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    ntups = PQntuples(res);
    *numAccessMethods = ntups;

    if (ntups == 0) {
        PQclear(res);
        destroyPQExpBuffer(query);
        return NULL;
    }

    aminfo = (AccessMethodInfo *) pg_malloc(ntups * sizeof(AccessMethodInfo));

    i_tableoid = PQfnumber(res, "tableoid");
    i_oid = PQfnumber(res, "oid");
    i_amname = PQfnumber(res, "amname");
    i_amhandler = PQfnumber(res, "amhandler");

    for (i = 0; i < ntups; i++) {
        aminfo[i].dobj.objType = DO_ACCESS_METHOD;
        aminfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        aminfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&aminfo[i].dobj);
        aminfo[i].dobj.name = gs_strdup(PQgetvalue(res, i, i_amname));
        aminfo[i].dobj.nmspace = NULL;
        aminfo[i].amhandler = gs_strdup(PQgetvalue(res, i, i_amhandler));

        /* Decide whether we want to dump it */
        selectDumpableAccessMethod(&(aminfo[i]));
    }

    PQclear(res);

    destroyPQExpBuffer(query);

    return aminfo;
}

/*
 * getOpclasses:
 *	  read all opclasses in the system catalogs and return them in the
 * OpclassInfo* structure
 *
 *	numOpclasses is set to the number of opclasses read in
 */
OpclassInfo* getOpclasses(Archive* fout, int* numOpclasses)
{
    PGresult* res = NULL;
    int ntups;
    int i;
    PQExpBuffer query = createPQExpBuffer();
    OpclassInfo* opcinfo = NULL;
    int i_tableoid;
    int i_oid;
    int i_opcname;
    int i_opcnamespace;
    int i_rolname;

    /*
     * find all opclasses, including builtin opclasses; we filter out
     * system-defined opclasses at dump-out time.
     */

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, "pg_catalog");

    if (fout->remoteVersion >= 70300) {
        appendPQExpBuffer(query,
            "SELECT tableoid, oid, opcname, "
            "opcnamespace, "
            "(%s opcowner) AS rolname "
            "FROM pg_opclass",
            username_subquery);
    } else if (fout->remoteVersion >= 70100) {
        appendPQExpBuffer(query,
            "SELECT tableoid, oid, opcname, "
            "0::oid AS opcnamespace, "
            "''::name AS rolname "
            "FROM pg_opclass");
    } else {
        appendPQExpBuffer(query,
            "SELECT "
            "(SELECT oid FROM pg_class WHERE relname = 'pg_opclass') AS tableoid, "
            "oid, opcname, "
            "0::oid AS opcnamespace, "
            "''::name AS rolname "
            "FROM pg_opclass");
    }

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    ntups = PQntuples(res);
    *numOpclasses = ntups;

    opcinfo = (OpclassInfo*)pg_malloc(ntups * sizeof(OpclassInfo));

    i_tableoid = PQfnumber(res, "tableoid");
    i_oid = PQfnumber(res, "oid");
    i_opcname = PQfnumber(res, "opcname");
    i_opcnamespace = PQfnumber(res, "opcnamespace");
    i_rolname = PQfnumber(res, "rolname");

    for (i = 0; i < ntups; i++) {
        opcinfo[i].dobj.objType = DO_OPCLASS;
        opcinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        opcinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&opcinfo[i].dobj);
        opcinfo[i].dobj.name = gs_strdup(PQgetvalue(res, i, i_opcname));
        opcinfo[i].dobj.nmspace =
            findNamespace(fout, atooid(PQgetvalue(res, i, i_opcnamespace)), opcinfo[i].dobj.catId.oid);
        opcinfo[i].rolname = gs_strdup(PQgetvalue(res, i, i_rolname));

        /* Decide whether we want to dump it */
        selectDumpableObject(&(opcinfo[i].dobj), fout);
    }

    PQclear(res);

    destroyPQExpBuffer(query);

    return opcinfo;
}

/*
 * getOpfamilies:
 *	  read all opfamilies in the system catalogs and return them in the
 * OpfamilyInfo* structure
 *
 *	numOpfamilies is set to the number of opfamilies read in
 */
OpfamilyInfo* getOpfamilies(Archive* fout, int* numOpfamilies)
{
    PGresult* res = NULL;
    int ntups;
    int i;
    PQExpBuffer query;
    OpfamilyInfo* opfinfo = NULL;
    int i_tableoid;
    int i_oid;
    int i_opfname;
    int i_opfnamespace;
    int i_rolname;

    /* Before 8.3, there is no separate concept of opfamilies */
    if (fout->remoteVersion < 80300) {
        *numOpfamilies = 0;
        return NULL;
    }

    query = createPQExpBuffer();

    /*
     * find all opfamilies, including builtin opfamilies; we filter out
     * system-defined opfamilies at dump-out time.
     */

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, "pg_catalog");

    appendPQExpBuffer(query,
        "SELECT tableoid, oid, opfname, "
        "opfnamespace, "
        "(%s opfowner) AS rolname "
        "FROM pg_opfamily",
        username_subquery);

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    ntups = PQntuples(res);
    *numOpfamilies = ntups;

    opfinfo = (OpfamilyInfo*)pg_malloc(ntups * sizeof(OpfamilyInfo));

    i_tableoid = PQfnumber(res, "tableoid");
    i_oid = PQfnumber(res, "oid");
    i_opfname = PQfnumber(res, "opfname");
    i_opfnamespace = PQfnumber(res, "opfnamespace");
    i_rolname = PQfnumber(res, "rolname");

    for (i = 0; i < ntups; i++) {
        opfinfo[i].dobj.objType = DO_OPFAMILY;
        opfinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        opfinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&opfinfo[i].dobj);
        opfinfo[i].dobj.name = gs_strdup(PQgetvalue(res, i, i_opfname));
        opfinfo[i].dobj.nmspace =
            findNamespace(fout, atooid(PQgetvalue(res, i, i_opfnamespace)), opfinfo[i].dobj.catId.oid);
        opfinfo[i].rolname = gs_strdup(PQgetvalue(res, i, i_rolname));

        /* Decide whether we want to dump it */
        selectDumpableObject(&(opfinfo[i].dobj), fout);
    }

    PQclear(res);

    destroyPQExpBuffer(query);

    return opfinfo;
}

/*
 * getAggregates:
 *	  read all the user-defined aggregates in the system catalogs and
 * return them in the AggInfo* structure
 *
 * numAggs is set to the number of aggregates read in
 */
AggInfo* getAggregates(Archive* fout, int* numAggs)
{
    PGresult* res = NULL;
    int ntups;
    int i;
    PQExpBuffer query = createPQExpBuffer();
    AggInfo* agginfo = NULL;
    char* endptr = NULL;
    int i_tableoid;
    int i_oid;
    int i_aggname;
    int i_aggnamespace;
    int i_pronargs;
    int i_proargtypes;
    int i_rolname;
    int i_aggacl;

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, "pg_catalog");

    /*
     * Find all user-defined aggregates.  See comment in getFuncs() for the
     * rationale behind the filtering logic.
     */

    if (fout->remoteVersion >= 80200) {
        appendPQExpBuffer(query,
            "SELECT tableoid, oid, proname AS aggname, "
            "pronamespace AS aggnamespace, "
            "pronargs, CASE WHEN pronargs <= %d THEN proargtypes else proargtypesext end as proargtypes, "
            "(%s proowner) AS rolname, "
            "proacl AS aggacl "
            "FROM pg_proc p "
            "WHERE proisagg AND ("
            "pronamespace != "
            "(SELECT oid FROM pg_namespace "
            "WHERE nspname = 'pg_catalog')",
            FUNC_MAX_ARGS_INROW,
            username_subquery);
        if (binary_upgrade && fout->remoteVersion >= 90100)
            appendPQExpBuffer(query,
                " OR EXISTS(SELECT 1 FROM pg_depend WHERE "
                "classid = 'pg_proc'::regclass AND "
                "objid = p.oid AND "
                "refclassid = 'pg_extension'::regclass AND "
                "deptype = 'e')");
        appendPQExpBuffer(query, ")");
    } else if (fout->remoteVersion >= 70300) {
        appendPQExpBuffer(query,
            "SELECT tableoid, oid, proname AS aggname, "
            "pronamespace AS aggnamespace, "
            "CASE WHEN proargtypes[0] = 'pg_catalog.\"any\"'::pg_catalog.regtype THEN 0 ELSE 1 END AS pronargs, "
            "proargtypes, "
            "(%s proowner) AS rolname, "
            "proacl AS aggacl "
            "FROM pg_proc "
            "WHERE proisagg "
            "AND pronamespace != "
            "(SELECT oid FROM pg_namespace WHERE nspname = 'pg_catalog')",
            username_subquery);
    } else if (fout->remoteVersion >= 70100) {
        appendPQExpBuffer(query,
            "SELECT tableoid, oid, aggname, "
            "0::oid AS aggnamespace, "
            "CASE WHEN aggbasetype = 0 THEN 0 ELSE 1 END AS pronargs, "
            "aggbasetype AS proargtypes, "
            "(%s aggowner) AS rolname, "
            "'{=X}' AS aggacl "
            "FROM pg_aggregate "
            "where oid > '%u'::oid",
            username_subquery,
            g_last_builtin_oid);
    } else {
        appendPQExpBuffer(query,
            "SELECT "
            "(SELECT oid FROM pg_class WHERE relname = 'pg_aggregate') AS tableoid, "
            "oid, aggname, "
            "0::oid AS aggnamespace, "
            "CASE WHEN aggbasetype = 0 THEN 0 ELSE 1 END AS pronargs, "
            "aggbasetype AS proargtypes, "
            "(%s aggowner) AS rolname, "
            "'{=X}' AS aggacl "
            "FROM pg_aggregate "
            "where oid > '%u'::oid",
            username_subquery,
            g_last_builtin_oid);
    }

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    ntups = PQntuples(res);
    *numAggs = ntups;

    agginfo = (AggInfo*)pg_malloc(ntups * sizeof(AggInfo));

    i_tableoid = PQfnumber(res, "tableoid");
    i_oid = PQfnumber(res, "oid");
    i_aggname = PQfnumber(res, "aggname");
    i_aggnamespace = PQfnumber(res, "aggnamespace");
    i_pronargs = PQfnumber(res, "pronargs");
    i_proargtypes = PQfnumber(res, "proargtypes");
    i_rolname = PQfnumber(res, "rolname");
    i_aggacl = PQfnumber(res, "aggacl");

    for (i = 0; i < ntups; i++) {
        agginfo[i].aggfn.dobj.objType = DO_AGG;
        agginfo[i].aggfn.dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        agginfo[i].aggfn.dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&agginfo[i].aggfn.dobj);
        agginfo[i].aggfn.dobj.name = gs_strdup(PQgetvalue(res, i, i_aggname));
        agginfo[i].aggfn.dobj.nmspace =
            findNamespace(fout, atooid(PQgetvalue(res, i, i_aggnamespace)), agginfo[i].aggfn.dobj.catId.oid);
        agginfo[i].aggfn.rolname = gs_strdup(PQgetvalue(res, i, i_rolname));
        agginfo[i].aggfn.lang = InvalidOid;       /* not currently interesting */
        agginfo[i].aggfn.prorettype = InvalidOid; /* not saved */
        agginfo[i].aggfn.proacl = gs_strdup(PQgetvalue(res, i, i_aggacl));
        agginfo[i].aggfn.nargs = (int)strtol(PQgetvalue(res, i, i_pronargs), &endptr, 10);
        if (agginfo[i].aggfn.nargs == 0)
            agginfo[i].aggfn.argtypes = NULL;
        else {
            if (agginfo[i].aggfn.nargs > INT_MAX / (int)sizeof(Oid)) {
                exit_horribly(NULL, "the number is overflow.\n");
            }
            agginfo[i].aggfn.argtypes = (Oid*)pg_malloc(agginfo[i].aggfn.nargs * sizeof(Oid));
            if (fout->remoteVersion >= 70300)
                parseOidArray(PQgetvalue(res, i, i_proargtypes), agginfo[i].aggfn.argtypes, agginfo[i].aggfn.nargs);
            else
                /* it's just aggbasetype */
                agginfo[i].aggfn.argtypes[0] = atooid(PQgetvalue(res, i, i_proargtypes));
        }

        /* Decide whether we want to dump it */
        selectDumpableObject(&(agginfo[i].aggfn.dobj), fout);
    }

    PQclear(res);

    destroyPQExpBuffer(query);

    /* free 1 bytes of memory that is allocated above by pg_malloc, if no tuples exist */
    if (0 == ntups) {
        free(agginfo);
        agginfo = NULL;
    }

    return agginfo;
}

/*
 * getFuncs:
 *	  read all the user-defined functions in the system catalogs and
 * return them in the FuncInfo* structure
 *
 * numFuncs is set to the number of functions read in
 */
FuncInfo* getFuncs(Archive* fout, int* numFuncs)
{
    PGresult* res = NULL;
    char* endptr = NULL;
    int ntups;
    int i;
    PQExpBuffer query = createPQExpBuffer();
    FuncInfo* finfo = NULL;
    int i_tableoid;
    int i_oid;
    int i_proname;
    int i_pronamespace;
    int i_rolname;
    int i_prolang;
    int i_pronargs;
    int i_proargtypes;
    int i_prorettype;
    int i_proacl;

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, "pg_catalog");

    /*
     * Find all user-defined functions.  Normally we can exclude functions in
     * pg_catalog, which is worth doing since there are several thousand of
     * 'em.  However, there are some extensions that create functions in
     * pg_catalog.	In normal dumps we can still ignore those --- but in
     * binary-upgrade mode, we must dump the member objects of the extension,
     * so be sure to fetch any such functions.
     *
     * Also, in 9.2 and up, exclude functions that are internally dependent on
     * something else, since presumably those will be created as a result of
     * creating the something else.  This currently only acts to suppress
     * constructor functions for range types.  Note that this is OK only
     * because the constructors don't have any dependencies the range type
     * doesn't have; otherwise we might not get creation ordering correct.
     * Also, if the functions are used by casts then we need to gather the
     * information about them, though they won't be dumped if they are built-in.
     */

    if (fout->remoteVersion >= 70300) {
        /* enable_hashjoin in this sql */
        appendPQExpBuffer(query,
            "SELECT /*+ set(enable_hashjoin on) */ tableoid, oid, proname, prolang, "
            "pronargs, CASE WHEN pronargs <= %d THEN proargtypes else proargtypesext end as proargtypes, "
            "prorettype, proacl, "
            "pronamespace, "
            "(%s proowner) AS rolname "
            "FROM pg_proc p "
            "WHERE NOT proisagg AND ("
            "pronamespace != "
            "(SELECT oid FROM pg_namespace "
            "WHERE nspname = 'pg_catalog')",
            FUNC_MAX_ARGS_INROW,
            username_subquery);
        if (fout->remoteVersion >= 90200)
            appendPQExpBuffer(query,
                "\n  AND NOT EXISTS (SELECT 1 FROM pg_depend "
                "WHERE classid = 'pg_proc'::regclass AND "
                "objid = p.oid AND deptype = 'i') "
                "\n  OR EXISTS (SELECT 1 FROM pg_cast"
                "\n  WHERE pg_cast.oid > '%u'::oid "
                "\n  AND p.oid = pg_cast.castfunc)",
                g_last_builtin_oid);
        if (binary_upgrade && fout->remoteVersion >= 90100)
            appendPQExpBuffer(query,
                "\n  OR EXISTS(SELECT 1 FROM pg_depend WHERE "
                "classid = 'pg_proc'::regclass AND "
                "objid = p.oid AND "
                "refclassid = 'pg_extension'::regclass AND "
                "deptype = 'e')");
        appendPQExpBuffer(query, ")");
    } else if (fout->remoteVersion >= 70100) {
        appendPQExpBuffer(query,
            "SELECT tableoid, oid, proname, prolang, "
            "pronargs, CASE WHEN pronargs <= %d THEN proargtypes else proargtypesext end as proargtypes, prorettype, "
            "'{=X}' AS proacl, "
            "0::oid AS pronamespace, "
            "(%s proowner) AS rolname "
            "FROM pg_proc "
            "WHERE pg_proc.oid > '%u'::oid",
            FUNC_MAX_ARGS_INROW,
            username_subquery,
            g_last_builtin_oid);
    } else {
        appendPQExpBuffer(query,
            "SELECT "
            "(SELECT oid FROM pg_class "
            " WHERE relname = 'pg_proc') AS tableoid, "
            "oid, proname, prolang, "
            "pronargs, CASE WHEN pronargs <= %d THEN proargtypes else proargtypesext end as proargtypes, prorettype, "
            "'{=X}' AS proacl, "
            "0::oid AS pronamespace, "
            "(%s proowner) AS rolname "
            "FROM pg_proc "
            "where pg_proc.oid > '%u'::oid",
            FUNC_MAX_ARGS_INROW,
            username_subquery,
            g_last_builtin_oid);
    }

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    ntups = PQntuples(res);

    *numFuncs = ntups;

    finfo = (FuncInfo*)pg_calloc(ntups, sizeof(FuncInfo));

    i_tableoid = PQfnumber(res, "tableoid");
    i_oid = PQfnumber(res, "oid");
    i_proname = PQfnumber(res, "proname");
    i_pronamespace = PQfnumber(res, "pronamespace");
    i_rolname = PQfnumber(res, "rolname");
    i_prolang = PQfnumber(res, "prolang");
    i_pronargs = PQfnumber(res, "pronargs");
    i_proargtypes = PQfnumber(res, "proargtypes");
    i_prorettype = PQfnumber(res, "prorettype");
    i_proacl = PQfnumber(res, "proacl");

    for (i = 0; i < ntups; i++) {
        finfo[i].dobj.objType = DO_FUNC;
        finfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        finfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&finfo[i].dobj);
        finfo[i].dobj.name = gs_strdup(PQgetvalue(res, i, i_proname));
        finfo[i].dobj.nmspace =
            findNamespace(fout, atooid(PQgetvalue(res, i, i_pronamespace)), finfo[i].dobj.catId.oid);
        finfo[i].rolname = gs_strdup(PQgetvalue(res, i, i_rolname));
        finfo[i].lang = atooid(PQgetvalue(res, i, i_prolang));
        finfo[i].prorettype = atooid(PQgetvalue(res, i, i_prorettype));
        finfo[i].proacl = gs_strdup(PQgetvalue(res, i, i_proacl));
        finfo[i].nargs = (int)strtol(PQgetvalue(res, i, i_pronargs), &endptr, 10);
        if (finfo[i].nargs > INT_MAX / (int)sizeof(Oid)) {
            exit_horribly(NULL, "the number is overflow.\n");
        }
        if (finfo[i].nargs == 0)
            finfo[i].argtypes = NULL;
        else {
            finfo[i].argtypes = (Oid*)pg_malloc(finfo[i].nargs * sizeof(Oid));
            parseOidArray(PQgetvalue(res, i, i_proargtypes), finfo[i].argtypes, finfo[i].nargs);
        }

        /* Decide whether we want to dump it */
        selectDumpableFuncs(&(finfo[i]), fout);
    }

    PQclear(res);

    destroyPQExpBuffer(query);

    return finfo;
}

PkgInfo* getPackages(Archive* fout, int* numPackages)
{
    PGresult* res = NULL;
    int ntups;
    int i;
    PQExpBuffer query = createPQExpBuffer();
    PkgInfo* pInfo = NULL;
    int iTableOid;
    int iOid;
    int iPkgName;
    int iPkgNameSpace;
    int iRolName;
    int iPkgAcl;

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, "pg_catalog");
    appendPQExpBuffer(query,
        "SELECT tableoid, oid, pkgname, pkgnamespace,pkgacl,"
        "(%s pkgowner) AS rolname "
        "FROM gs_package WHERE "
        "pkgnamespace != "
        "(SELECT oid FROM pg_namespace "
        "WHERE nspname = 'pg_catalog')",
        username_subquery);
    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    ntups = PQntuples(res);

    *numPackages = ntups;

    if (ntups == 0) {
        PQclear(res);
        destroyPQExpBuffer(query);
        return NULL;
    }
    pInfo = (PkgInfo*)pg_calloc(ntups, sizeof(PkgInfo));

    iTableOid = PQfnumber(res, "tableoid");
    iOid = PQfnumber(res, "oid");
    iPkgName = PQfnumber(res, "pkgname");
    iPkgNameSpace = PQfnumber(res, "pkgnamespace");
    iRolName = PQfnumber(res, "rolname");
    iPkgAcl = PQfnumber(res, "pkgacl");

    for (i = 0; i < ntups; i++) {
        pInfo[i].dobj.objType = DO_PACKAGE;
        pInfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, iTableOid));
        pInfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, iOid));
        AssignDumpId(&pInfo[i].dobj);
        pInfo[i].dobj.name = gs_strdup(PQgetvalue(res, i, iPkgName));
        pInfo[i].dobj.nmspace =
            findNamespace(fout, atooid(PQgetvalue(res, i, iPkgNameSpace)), pInfo[i].dobj.catId.oid);
        pInfo[i].rolname = gs_strdup(PQgetvalue(res, i, iRolName));
        pInfo[i].pkgacl = gs_strdup(PQgetvalue(res, i, iPkgAcl));
        /* Decide whether we want to dump it */
        selectDumpableObject(&(pInfo[i].dobj), fout);
    }

    PQclear(res);

    destroyPQExpBuffer(query);

    return pInfo;
}

/*
 Add oid to incoming list
 */
static void appendOidList(SimpleOidList* oidList, Oid val)
{
    if (val != 0) {
        simple_oid_list_append(oidList, val);
    }
}

/*
 * Get the input object relation kind. Now we only support table/foreign table/sequence/view.
 */
static char getRelationKind(Archive* fout, Oid val)
{
    PQExpBuffer query;
    PGresult* res = NULL;
    int i_relkind = 0;
    char relkind;

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, "pg_catalog");

    query = createPQExpBuffer();
    appendPQExpBuffer(query,
        " SELECT relkind FROM pg_catalog.pg_class "
        " WHERE oid = %u ",
        val);
    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    if (PQntuples(res) == 0) {
        write_msg(NULL, "Failed to find the object type for \"%u\"\n", val);
        destroyPQExpBuffer(query);
        exit_nicely(1);
    }

    i_relkind = PQfnumber(res, "relkind");
    relkind = *(PQgetvalue(res, 0, i_relkind));
    PQclear(res);
    destroyPQExpBuffer(query);

    return relkind;
}

/*
 * Finding its dependent object oid through the oidlist
 */
static SimpleOidList getDependObjectOid(Archive* fout, SimpleOidList* inputList)
{
    PQExpBuffer query;
    PGresult* res = NULL;
    char relkind;
    SimpleOidListCell* cell = NULL;
    SimpleOidList depOidList = {NULL, NULL};
    int ntups = 0;
    int i = 0;

    if (NULL == inputList->head)
        return depOidList;

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, "pg_catalog");

    for (cell = inputList->head; cell != NULL; cell = cell->next) {
        /* get object relation type */
        relkind = getRelationKind(fout, cell->val);

        /* make the depend SQL command by the object relkind and Oid */
        query = createPQExpBuffer();
        switch (relkind) {
            case RELKIND_RELATION:
                /*
                1. table -> table (foreign key constraint and inherit table do not need to dump)
                2. table -> index (it will be dumpped automaticly)
                3. table -> view
                */
                appendPQExpBuffer(query,
                    " SELECT c.ev_class AS oid "
                    " FROM pg_depend a, pg_depend b, pg_class pc, pg_rewrite c "
                    " WHERE pc.oid = %u AND "
                    " a.refclassid = 'pg_class'::regclass AND "
                    " a.classid = 'pg_rewrite'::regclass AND "
                    " a.objid = b.objid AND "
                    " a.classid = b.classid AND "
                    " a.refclassid = b.refclassid AND "
                    " a.refobjid <> b.refobjid AND "
                    " pc.oid = a.refobjid AND "
                    " c.oid = b.objid "
                    " GROUP BY c.ev_class, pc.oid; ",
                    cell->val);
                break;

            case RELKIND_SEQUENCE:
            case RELKIND_LARGE_SEQUENCE:
                /*
                1. sequence -> table/partition table
                2. sequence -> foreign table(default values on foreign tables are not supported)
                */
                appendPQExpBuffer(query,
                    " SELECT attr.adrelid AS oid "
                    " FROM pg_depend a, pg_depend b, pg_class pc, pg_attrdef attr "
                    " WHERE pc.oid = %u AND "
                    " a.refclassid = 'pg_class'::regclass AND "
                    " a.classid = 'pg_attrdef'::regclass AND "
                    " a.objid = b.objid AND "
                    " a.classid = b.classid AND "
                    " a.refclassid = b.refclassid AND "
                    " a.refobjid <> b.refobjid AND "
                    " pc.oid = a.refobjid AND "
                    " attr.oid = b.objid "
                    " GROUP BY attr.adrelid, pc.oid;",
                    cell->val);
                break;

            case RELKIND_CONTQUERY:
            case RELKIND_VIEW:
            case RELKIND_MATVIEW:
                /*
                1. view -> view
                */
                appendPQExpBuffer(query,
                    " SELECT c.ev_class AS oid "
                    " FROM pg_depend a, pg_depend b, pg_class pc, pg_rewrite c "
                    " WHERE pc.oid = %u AND "
                    " a.refclassid = 'pg_class'::regclass AND "
                    " a.classid = 'pg_rewrite'::regclass AND "
                    " a.objid = b.objid AND "
                    " a.classid = b.classid AND "
                    " a.refclassid = b.refclassid AND "
                    " a.refobjid <> b.refobjid AND "
                    " pc.oid = a.refobjid AND "
                    " c.oid = b.objid AND "
                    " c.ev_class != pc.oid "
                    " GROUP BY c.ev_class, pc.oid;",
                    cell->val);
                break;

            case RELKIND_FOREIGN_TABLE:
            case RELKIND_STREAM:
                /*
                1. foreign table -> view
                */
                appendPQExpBuffer(query,
                    " SELECT c.ev_class AS oid "
                    " FROM pg_depend a, pg_depend b, pg_class pc, pg_rewrite c "
                    " WHERE pc.oid = %u AND "
                    " a.refclassid = 'pg_class'::regclass AND "
                    " a.classid = 'pg_rewrite'::regclass AND "
                    " a.objid = b.objid AND "
                    " a.classid = b.classid AND "
                    " a.refclassid = b.refclassid AND "
                    " a.refobjid <> b.refobjid AND "
                    " pc.oid = a.refobjid AND "
                    " c.oid = b.objid "
                    " GROUP BY c.ev_class, pc.oid;",
                    cell->val);
                break;

            default:
                /*
                 * RELKIND_COMPOSITE_TYPE and other relkind is not support.
                 */
                destroyPQExpBuffer(query);
                write_msg(NULL, "unexpected section code %c\n", relkind);
                exit_nicely(1);
        }

        res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
        ntups = PQntuples(res);
        if (0 != ntups) {
            /*
             * put the depend oids into depOidList
             */
            for (i = 0; i < ntups; i++) {
                if (PQfnumber(res, "oid") < 0) {
                    write_msg(NULL, "PQfnumber cannot be negative(%d).\n", PQfnumber(res, "oid"));
                    continue;
                }
                appendOidList(&depOidList, atooid(PQgetvalue(res, i, PQfnumber(res, "oid"))));
            }
        }
        PQclear(res);
        destroyPQExpBuffer(query);
    }

    return depOidList;
}

static void free_SimpleOidList(SimpleOidList* list)
{
    SimpleOidListCell* cell = NULL;

    while ((list != NULL) && NULL != list->head) {
        cell = list->head;
        list->head = list->head->next;
        free(cell);
        cell = NULL;
    }
}

/*
 * get the specified object dependent object,
 * and append them to the given OID list.
 */
static void getAllDependObjectOidList(Archive* fout, SimpleOidList* oidlists, SimpleOidList* dependOidList)
{
    SimpleOidListCell* cell = NULL;
    SimpleOidList tmpList = {NULL, NULL};

    /* it means that we do not use -f and  --include-table-file options.*/
    if (NULL == oidlists->head)
        return;

    tmpList = getDependObjectOid(fout, oidlists);
    while (NULL != tmpList.head) {
        SimpleOidList tmpDepList = {NULL, NULL};
        for (cell = tmpList.head; cell != NULL; cell = cell->next) {
            appendOidList(dependOidList, cell->val);
            appendOidList(&tmpDepList, cell->val);
        }
        free_SimpleOidList(&tmpList);
        tmpList = getDependObjectOid(fout, &tmpDepList);
        free_SimpleOidList(&tmpDepList);
    }
}

static bool isIndependentRole(PGconn* conn, char* rolname)
{
    PQExpBuffer query = NULL;
    bool isExistsRolkind = false;
    bool isExists = false;

    /* check whether rolkind exists in pg_roles */
    isExistsRolkind = is_column_exists(conn, AuthIdRelationId, "rolkind");
    if (isExistsRolkind) {
        query = createPQExpBuffer();
        appendPQExpBuffer(query, "SELECT rolkind FROM pg_catalog.pg_roles WHERE rolname='%s' AND rolkind='i'", rolname);
        isExists = isExistsSQLResult(conn, query->data);
        destroyPQExpBuffer(query);
        return isExists;
    } else {
        return false;
    }
}

/*
 * Determine if the user is super or sysadmin
 */
static bool isSuperRole(PGconn* conn, char* rolname)
{
    PQExpBuffer query = createPQExpBuffer();
    bool isExists = false;

    appendPQExpBuffer(query,
        "SELECT rolname FROM pg_catalog.pg_roles WHERE rolname='%s' AND "
        "(rolsuper='t' OR rolsystemadmin = 't')",
        rolname);
    isExists = isExistsSQLResult(conn, query->data);
    destroyPQExpBuffer(query);

    return isExists;
}

/*
 * Determine if the execute role/--role is superRole
 */
static bool isExecUserSuperRole(Archive* fout)
{
    ArchiveHandle* AH = (ArchiveHandle*)fout;
    char* executeUser = NULL;

    /* get execute user */
    executeUser = PQuser(AH->connection);

    if ((use_role != NULL) && isSuperRole(AH->connection, use_role)) {
        return true;
    } else if (isSuperRole(AH->connection, executeUser)) {
        return true;
    } else {
        return false;
    }
}

/*
 * When the executing user or --role is not the owner of the object and it is not super/sysadmin
 * it means that there is no permission to backup the object
 */
static bool isExecUserNotObjectOwner(Archive* fout, const char* objRoleName)
{
    ArchiveHandle* AH = (ArchiveHandle*)fout;
    char* executeUser = NULL;
    size_t maxLen1 = 0;
    size_t maxLen2 = 0;

    if (NULL == objRoleName)
        return false;

    if (NULL != use_role) {
        maxLen1 = (strlen(use_role) > strlen(objRoleName)) ? strlen(use_role) : strlen(objRoleName);
        /* if role is not the owner of object and it is not super/sysadmin
         * it means that there is no permission to backup the object
         */
        if (0 != strncmp(use_role, objRoleName, maxLen1) && !isSuperRole(AH->connection, use_role)) {
            return true;
        }
    } else {
        /* get execute user */
        executeUser = PQuser(AH->connection);
        /* get len */
        maxLen2 = (strlen(executeUser) > strlen(objRoleName)) ? strlen(executeUser) : strlen(objRoleName);
        if (0 != strncmp(executeUser, objRoleName, maxLen2) && !isSuperRole(AH->connection, executeUser)) {
            return true;
        }
    }

    return false;
}
/*
 * getTables
 *    read all the user-defined tables (no indexes, no catalogs)
 * in the system catalogs return them in the TableInfo* structure
 *
 * numTables is set to the number of tables read in
 */
TableInfo* getTables(Archive* fout, int* numTables)
{
    PGresult* res = NULL;
    int ntups = 0;
    int i = 0;
    PQExpBuffer query;
    TableInfo* tblinfo = NULL;
    int i_reltableoid = 0;
    int i_reloid = 0;
    int i_relname = 0;
    int i_relnamespace = 0;
    int i_relkind = 0;
    int i_relacl = 0;
    int i_rolname = 0;
    int i_relchecks = 0;
    int i_relhastriggers = 0;
    int i_relhasindex = 0;
    int i_relhasrules = 0;
    int i_relhasoids = 0;
    int i_relfrozenxid = 0, i_relfrozenxid64 = 0;
    int i_toastoid = 0;
    int i_toastfrozenxid = 0, i_toastfrozenxid64 = 0;
    int i_relpersistence = 0;
    int i_relbucket = 0;
    int i_owning_tab = 0;
    int i_owning_col = 0;
    int i_relreplident = 0;
#ifdef PGXC
    int i_pgxclocatortype = 0;
    int i_pgxcattnum = 0;
    int i_pgxc_node_names = 0;
#endif
    int i_reltablespace = 0;
    int i_reloptions = 0;
    int i_checkoption = 0;
    int i_toastreloptions = 0;
    int i_reloftype = 0;
    int i_parttype = 0;
    int i_relrowmovement = 0;
    int i_relhsblockchain = 0;
    int i_viewsecurity = 0;
    int1 i_relcmprs = 0;
    SimpleOidListCell* cell = NULL;
    int count = 0;
    bool isTableDumpped = false;
    ArchiveHandle* AH = (ArchiveHandle*)fout;
    bool isHasRelfrozenxid64 = false;
    bool isHasRelbucket = false;
    /* This SQL must using enable_hashjoin */
    ExecuteSqlStatement(fout, "set enable_hashjoin=on");
    /*
     * if table_include_oids.head is null, it means that we do not use -t/--include-table-file options.
     */
    if (NULL != table_include_oids.head) {
        /*
         get the oid list of specified object dependent object
        */
        if (include_depend_objs) {
            getAllDependObjectOidList(fout, &table_include_oids, &depend_obj_oids);
        }

        /* get the oid list of specified object */
        if (!exclude_self && include_depend_objs) {
            getAllDumpObjectOidList(fout, &table_include_oids, &depend_obj_oids);
        }

        /* Only supports backups one object*/
        for (cell = table_include_oids.head; cell != NULL; cell = cell->next) {
            count += 1;
        }

        if (0 == (count - 1) && !exclude_self && !include_depend_objs) {
            isSingleTableDump = true;
        }
    }

    /*
     * Find all the tables and table-like objects.
     *
     * We include system catalogs, so that we can work if a user table is
     * defined to inherit from a system catalog (pretty weird, but...)
     *
     * We ignore relations that are not ordinary tables, sequences, views,
     * composite types, or foreign tables.
     *
     * Composite-type table entries won't be dumped as such, but we have to
     * make a DumpableObject for them so that we can track dependencies of the
     * composite type (pg_depend entries for columns of the composite type
     * link to the pg_class entry not the pg_type entry).
     *
     * Note: in this phase we should collect only a minimal amount of
     * information about each table, basically just enough to decide if it is
     * interesting. We must fetch all tables in this phase because otherwise
     * we cannot correctly identify inherited columns, owned sequences, etc.
     */

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, "pg_catalog");
    query = createPQExpBuffer();
    isHasRelfrozenxid64 = is_column_exists(AH->connection, RelationRelationId, "relfrozenxid64");
    isHasRelbucket = is_column_exists(AH->connection, RelationRelationId, "relbucket");
    if (fout->remoteVersion >= 90100) {
        /*
         * Left join to pick up dependency info linking sequences to their
         * owning column, if any (note this dependency is AUTO as of 8.2)
         */
        /* add  "c.parttype, c.relrowmovement, " */
        if (isSingleTableDump) {
            appendPQExpBuffer(query,
                "SELECT c.tableoid, c.oid, c.relname, "
                "c.relacl, c.relkind, c.relnamespace, "
                "(%s c.relowner) AS rolname, "
                "c.relchecks, c.relhastriggers, "
                "c.relhasindex, c.relhasrules, c.relhasoids, "
                "c.relfrozenxid, %s, tc.oid AS toid, "
                "tc.relfrozenxid AS tfrozenxid, "
                "%s, "
                "c.relpersistence, "
                "%s, ",
                username_subquery,
                isHasRelfrozenxid64 ? "c.relfrozenxid64" : "0 AS relfrozenxid64",
                isHasRelfrozenxid64 ? "tc.relfrozenxid64 AS tfrozenxid64" : "0 AS tfrozenxid64",
                isHasRelbucket? "c.relbucket" : "-1 AS relbucket");

            if (true == is_column_exists(AH->connection, RelationRelationId, "relreplident")) {
                appendPQExpBuffer(query, "c.relreplident, ");
            } else {
                appendPQExpBuffer(query, "'d' AS relreplident, ");
            }

            appendPQExpBuffer(query,
                "c.parttype, c.relrowmovement, c.relcmprs, "
                "CASE WHEN c.reloftype <> 0 THEN c.reloftype::pg_catalog.regtype ELSE NULL::Oid END AS reloftype, "
                "d.refobjid AS owning_tab, "
                "d.refobjsubid AS owning_col, "
                "(SELECT spcname FROM pg_tablespace t WHERE t.oid = c.reltablespace) AS reltablespace, "
                "(SELECT nspblockchain FROM pg_namespace n WHERE n.oid = c.relnamespace) AS relhsblockchain, "
#ifdef PGXC
                "(SELECT pclocatortype from pgxc_class v where v.pcrelid = c.oid) AS pgxclocatortype,"
                "(SELECT pcattnum from pgxc_class v where v.pcrelid = c.oid) AS pgxcattnum,"
                "(SELECT pg_catalog.string_agg(node_name,',') AS pgxc_node_names from pgxc_node n where n.oid "
                "in (select pg_catalog.unnest(nodeoids) from pgxc_class v where v.pcrelid=c.oid) ) , "
#endif
                "pg_catalog.array_to_string(pg_catalog.array_remove(pg_catalog.array_remove(pg_catalog.array_remove"
                    "(pg_catalog.array_remove(c.reloptions, 'view_sql_security=definer'), 'view_sql_security=invoker'), "
                        "'check_option=local'), 'check_option=cascaded'), ', ') AS reloptions, "
                "CASE WHEN 'check_option=local' = ANY (c.reloptions) THEN 'LOCAL'::text "
                    "WHEN 'check_option=cascaded' = ANY (c.reloptions) THEN 'CASCADED'::text ELSE NULL END AS checkoption, "
                "CASE WHEN 'view_sql_security=definer' = ANY (c.reloptions) THEN 'DEFINER'::text "  
                     "WHEN 'view_sql_security=invoker' = ANY (c.reloptions) THEN 'INVOKER'::text "
                        "ELSE NULL END AS viewsecurity, "     
                "pg_catalog.array_to_string(array(SELECT 'toast.' || "
                "x FROM pg_catalog.unnest(tc.reloptions) x), ', ') AS toast_reloptions "
                "FROM pg_class c "
                "LEFT JOIN pg_depend d ON "
                "(c.relkind in ('%c','%c') AND "
                "d.classid = c.tableoid AND d.objid = c.oid AND "
                "d.objsubid = 0 AND "
                "d.refclassid = c.tableoid AND d.deptype = 'a') "
                "LEFT JOIN pg_class tc ON (c.reltoastrelid = tc.oid) "
                "WHERE c.oid = %u "
                "ORDER BY c.oid",
                RELKIND_SEQUENCE,
                RELKIND_LARGE_SEQUENCE,
                table_include_oids.head->val);
        } else {
            appendPQExpBuffer(query,
                "SELECT c.tableoid, c.oid, c.relname, "
                "c.relacl, c.relkind, c.relnamespace, "
                "(%s c.relowner) AS rolname, "
                "c.relchecks, c.relhastriggers, "
                "c.relhasindex, c.relhasrules, c.relhasoids, "
                "c.relfrozenxid, %s, tc.oid AS toid, "
                "tc.relfrozenxid AS tfrozenxid, "
                "%s, "
                "c.relpersistence, "
                "%s, ",
                username_subquery,
                isHasRelfrozenxid64 ? "c.relfrozenxid64" : "0 AS c.relfrozenxid64",
                isHasRelfrozenxid64 ? "tc.relfrozenxid64 AS tfrozenxid64" : "0 AS tfrozenxid64",
                isHasRelbucket ? "c.relbucket" : "-1 AS relbucket");

            if (true == is_column_exists(AH->connection, RelationRelationId, "relreplident")) {
                appendPQExpBuffer(query, "c.relreplident, ");
            } else {
                appendPQExpBuffer(query, "'d' AS relreplident, ");
            }
            appendPQExpBuffer(query,
                "c.parttype, c.relrowmovement, c.relcmprs, "
                "CASE WHEN c.reloftype <> 0 THEN c.reloftype::pg_catalog.regtype ELSE NULL::Oid END AS reloftype, "
                "d.refobjid AS owning_tab, "
                "d.refobjsubid AS owning_col, "
                "(SELECT spcname FROM pg_tablespace t WHERE t.oid = c.reltablespace) AS reltablespace, "
                "(SELECT nspblockchain FROM pg_namespace n WHERE n.oid = c.relnamespace) AS relhsblockchain, "
#ifdef PGXC
                "(SELECT pclocatortype from pgxc_class v where v.pcrelid = c.oid) AS pgxclocatortype,"
                "(SELECT pcattnum from pgxc_class v where v.pcrelid = c.oid) AS pgxcattnum,"
                "(SELECT pg_catalog.string_agg(node_name,',') AS pgxc_node_names from pgxc_node n where n.oid "
                "in (select pg_catalog.unnest(nodeoids) from pgxc_class v where v.pcrelid=c.oid) ) , "
#endif
                "pg_catalog.array_to_string(pg_catalog.array_remove(pg_catalog.array_remove(pg_catalog.array_remove"
                    "(pg_catalog.array_remove(c.reloptions, 'view_sql_security=definer'), 'view_sql_security=invoker'), "
                        "'check_option=local'), 'check_option=cascaded'), ', ') AS reloptions, "
                "CASE WHEN 'check_option=local' = ANY (c.reloptions) THEN 'LOCAL'::text "
                    "WHEN 'check_option=cascaded' = ANY (c.reloptions) THEN 'CASCADED'::text ELSE NULL END AS checkoption, "
                "CASE WHEN 'view_sql_security=definer' = ANY (c.reloptions) THEN 'DEFINER'::text "  
                     "WHEN 'view_sql_security=invoker' = ANY (c.reloptions) THEN 'INVOKER'::text "
                        "ELSE NULL END AS viewsecurity, "  
                "pg_catalog.array_to_string(array(SELECT 'toast.' || "
                "x FROM pg_catalog.unnest(tc.reloptions) x), ', ') AS toast_reloptions "
                "FROM pg_class c "
                "LEFT JOIN pg_depend d ON "
                "(c.relkind in ('%c', '%c') AND "
                "d.classid = c.tableoid AND d.objid = c.oid AND "
                "d.objsubid = 0 AND "
                "d.refclassid = c.tableoid AND d.deptype = 'a') "
                "LEFT JOIN pg_class tc ON (c.reltoastrelid = tc.oid) "
                "WHERE c.relkind in ('%c', '%c', '%c', '%c', '%c', '%c', '%c', '%c', '%c') AND c.relnamespace != %d "
                "ORDER BY c.oid",
                RELKIND_SEQUENCE,
                RELKIND_LARGE_SEQUENCE,
                RELKIND_RELATION,
                RELKIND_SEQUENCE,
                RELKIND_LARGE_SEQUENCE,
                RELKIND_VIEW,
                RELKIND_MATVIEW,
                RELKIND_CONTQUERY,
                RELKIND_COMPOSITE_TYPE,
                RELKIND_FOREIGN_TABLE,
                RELKIND_STREAM,
                CSTORE_NAMESPACE);
        }
    } else if (fout->remoteVersion >= 90000) {
        /*
         * Left join to pick up dependency info linking sequences to their
         * owning column, if any (note this dependency is AUTO as of 8.2)
         */
        appendPQExpBuffer(query,
            "SELECT c.tableoid, c.oid, c.relname, "
            "c.relacl, c.relkind, c.relnamespace, "
            "(%s c.relowner) AS rolname, "
            "c.relchecks, c.relhastriggers, "
            "c.relhasindex, c.relhasrules, c.relhasoids, "
            "c.relfrozenxid, tc.oid AS toid, "
            "tc.relfrozenxid AS tfrozenxid, "
            "'p' AS relpersistence, "
            "CASE WHEN c.reloftype <> 0 THEN c.reloftype::pg_catalog.regtype ELSE NULL::Oid END AS reloftype, "
            "d.refobjid AS owning_tab, "
            "d.refobjsubid AS owning_col, "
            "(SELECT spcname FROM pg_tablespace t WHERE t.oid = c.reltablespace) AS reltablespace, "
            "pg_catalog.array_to_string(c.reloptions, ', ') AS reloptions, "
            "pg_catalog.array_to_string(array(SELECT 'toast.' || "
            "x FROM pg_catalog.unnest(tc.reloptions) x), ', ') AS toast_reloptions "
            "FROM pg_class c "
            "LEFT JOIN pg_depend d ON "
            "(c.relkind in ('%c', '%c') AND "
            "d.classid = c.tableoid AND d.objid = c.oid AND "
            "d.objsubid = 0 AND "
            "d.refclassid = c.tableoid AND d.deptype = 'a') "
            "LEFT JOIN pg_class tc ON (c.reltoastrelid = tc.oid) "
            "WHERE c.relkind in ('%c', '%c', '%c', '%c', '%c') "
            "ORDER BY c.oid",
            username_subquery,
            RELKIND_SEQUENCE,
            RELKIND_LARGE_SEQUENCE,
            RELKIND_RELATION,
            RELKIND_SEQUENCE,
            RELKIND_LARGE_SEQUENCE,
            RELKIND_VIEW,
            RELKIND_COMPOSITE_TYPE);
    } else if (fout->remoteVersion >= 80400) {
        /*
         * Left join to pick up dependency info linking sequences to their
         * owning column, if any (note this dependency is AUTO as of 8.2)
         * PGXC is based on PostgreSQL version 8.4, it is not necessary to
         * to modify the other SQL queries.
         */
        appendPQExpBuffer(query,
            "SELECT c.tableoid, c.oid, c.relname, "
            "c.relacl, c.relkind, c.relnamespace, "
            "(%s c.relowner) AS rolname, "
            "c.relchecks, c.relhastriggers, "
            "c.relhasindex, c.relhasrules, c.relhasoids, "
            "c.relfrozenxid, tc.oid AS toid, "
            "tc.relfrozenxid AS tfrozenxid, "
            "'p' AS relpersistence, "
            "NULL AS reloftype, "
            "d.refobjid AS owning_tab, "
            "d.refobjsubid AS owning_col, "
            "(SELECT spcname FROM pg_tablespace t WHERE t.oid = c.reltablespace) AS reltablespace, "
            "pg_catalog.array_to_string(c.reloptions, ', ') AS reloptions, "
            "pg_catalog.array_to_string(array(SELECT 'toast.' || "
            "x FROM pg_catalog.unnest(tc.reloptions) x), ', ') AS toast_reloptions "
            "FROM pg_class c "
            "LEFT JOIN pg_depend d ON "
            "(c.relkind in ('%c', '%c') AND "
            "d.classid = c.tableoid AND d.objid = c.oid AND "
            "d.objsubid = 0 AND "
            "d.refclassid = c.tableoid AND d.deptype = 'a') "
            "LEFT JOIN pg_class tc ON (c.reltoastrelid = tc.oid) "
            "WHERE c.relkind in ('%c', '%c', '%c', '%c', '%c') "
            "ORDER BY c.oid",
            username_subquery,
            RELKIND_SEQUENCE,
            RELKIND_LARGE_SEQUENCE,
            RELKIND_RELATION,
            RELKIND_SEQUENCE,
            RELKIND_LARGE_SEQUENCE,
            RELKIND_VIEW,
            RELKIND_COMPOSITE_TYPE);
    } else if (fout->remoteVersion >= 80200) {
        /*
         * Left join to pick up dependency info linking sequences to their
         * owning column, if any (note this dependency is AUTO as of 8.2)
         */
        appendPQExpBuffer(query,
            "SELECT c.tableoid, c.oid, c.relname, "
            "c.relacl, c.relkind, c.relnamespace, "
            "(%s c.relowner) AS rolname, "
            "c.relchecks, (c.reltriggers <> 0) AS relhastriggers, "
            "c.relhasindex, c.relhasrules, c.relhasoids, "
            "c.relfrozenxid, tc.oid AS toid, "
            "tc.relfrozenxid AS tfrozenxid, "
            "'p' AS relpersistence, "
            "NULL AS reloftype, "
            "d.refobjid AS owning_tab, "
            "d.refobjsubid AS owning_col, "
            "(SELECT spcname FROM pg_tablespace t WHERE t.oid = c.reltablespace) AS reltablespace, "
            "pg_catalog.array_to_string(c.reloptions, ', ') AS reloptions, "
            "NULL AS toast_reloptions "
            "FROM pg_class c "
            "LEFT JOIN pg_depend d ON "
            "(c.relkind in ('%c', '%c') AND "
            "d.classid = c.tableoid AND d.objid = c.oid AND "
            "d.objsubid = 0 AND "
            "d.refclassid = c.tableoid AND d.deptype = 'a') "
            "LEFT JOIN pg_class tc ON (c.reltoastrelid = tc.oid) "
            "WHERE c.relkind in ('%c', '%c', '%c', '%c', '%c') "
            "ORDER BY c.oid",
            username_subquery,
            RELKIND_SEQUENCE,
            RELKIND_LARGE_SEQUENCE,
            RELKIND_RELATION,
            RELKIND_SEQUENCE,
            RELKIND_LARGE_SEQUENCE,
            RELKIND_VIEW,
            RELKIND_COMPOSITE_TYPE);
    } else if (fout->remoteVersion >= 80000) {
        /*
         * Left join to pick up dependency info linking sequences to their
         * owning column, if any
         */
        appendPQExpBuffer(query,
            "SELECT c.tableoid, c.oid, relname, "
            "relacl, relkind, relnamespace, "
            "(%s relowner) AS rolname, "
            "relchecks, (reltriggers <> 0) AS relhastriggers, "
            "relhasindex, relhasrules, relhasoids, "
            "0 AS relfrozenxid, "
            "0 AS toid, "
            "0 AS tfrozenxid, "
            "'p' AS relpersistence, "
            "NULL AS reloftype, "
            "d.refobjid AS owning_tab, "
            "d.refobjsubid AS owning_col, "
            "(SELECT spcname FROM pg_tablespace t WHERE t.oid = c.reltablespace) AS reltablespace, "
            "NULL AS reloptions, "
            "NULL AS toast_reloptions "
            "FROM pg_class c "
            "LEFT JOIN pg_depend d ON "
            "(c.relkind = ('%c', '%c') AND "
            "d.classid = c.tableoid AND d.objid = c.oid AND "
            "d.objsubid = 0 AND "
            "d.refclassid = c.tableoid AND d.deptype = 'i') "
            "WHERE relkind in ('%c', '%c', '%c', '%c', '%c') "
            "ORDER BY c.oid",
            username_subquery,
            RELKIND_SEQUENCE,
            RELKIND_LARGE_SEQUENCE,
            RELKIND_RELATION,
            RELKIND_SEQUENCE,
            RELKIND_LARGE_SEQUENCE,
            RELKIND_VIEW,
            RELKIND_COMPOSITE_TYPE);
    } else if (fout->remoteVersion >= 70300) {
        /*
         * Left join to pick up dependency info linking sequences to their
         * owning column, if any
         */
        appendPQExpBuffer(query,
            "SELECT c.tableoid, c.oid, relname, "
            "relacl, relkind, relnamespace, "
            "(%s relowner) AS rolname, "
            "relchecks, (reltriggers <> 0) AS relhastriggers, "
            "relhasindex, relhasrules, relhasoids, "
            "0 AS relfrozenxid, "
            "0 AS toid, "
            "0 AS tfrozenxid, "
            "'p' AS relpersistence, "
            "NULL AS reloftype, "
            "d.refobjid AS owning_tab, "
            "d.refobjsubid AS owning_col, "
            "NULL AS reltablespace, "
            "NULL AS reloptions, "
            "NULL AS toast_reloptions "
            "FROM pg_class c "
            "LEFT JOIN pg_depend d ON "
            "(c.relkind in ('%c', '%c') AND "
            "d.classid = c.tableoid AND d.objid = c.oid AND "
            "d.objsubid = 0 AND "
            "d.refclassid = c.tableoid AND d.deptype = 'i') "
            "WHERE relkind IN ('%c', '%c', '%c', '%c', '%c') "
            "ORDER BY c.oid",
            username_subquery,
            RELKIND_SEQUENCE,
            RELKIND_LARGE_SEQUENCE,
            RELKIND_RELATION,
            RELKIND_SEQUENCE,
            RELKIND_LARGE_SEQUENCE,
            RELKIND_VIEW,
            RELKIND_COMPOSITE_TYPE);
    } else if (fout->remoteVersion >= 70200) {
        appendPQExpBuffer(query,
            "SELECT tableoid, oid, relname, relacl, relkind, "
            "0::oid AS relnamespace, "
            "(%s relowner) AS rolname, "
            "relchecks, (reltriggers <> 0) AS relhastriggers, "
            "relhasindex, relhasrules, relhasoids, "
            "0 AS relfrozenxid, "
            "0 AS toid, "
            "0 AS tfrozenxid, "
            "'p' AS relpersistence, "
            "NULL AS reloftype, "
            "NULL::oid AS owning_tab, "
            "NULL::int4 AS owning_col, "
            "NULL AS reltablespace, "
            "NULL AS reloptions, "
            "NULL AS toast_reloptions "
            "FROM pg_class "
            "WHERE relkind IN ('%c', '%c', '%c', '%c') "
            "ORDER BY oid",
            username_subquery,
            RELKIND_RELATION,
            RELKIND_SEQUENCE,
            RELKIND_LARGE_SEQUENCE,
            RELKIND_VIEW);
    } else if (fout->remoteVersion >= 70100) {
        /* all tables have oids in 7.1 */
        appendPQExpBuffer(query,
            "SELECT tableoid, oid, relname, relacl, relkind, "
            "0::oid AS relnamespace, "
            "(%s relowner) AS rolname, "
            "relchecks, (reltriggers <> 0) AS relhastriggers, "
            "relhasindex, relhasrules, "
            "'t'::bool AS relhasoids, "
            "0 AS relfrozenxid, "
            "0 AS toid, "
            "0 AS tfrozenxid, "
            "'p' AS relpersistence, "
            "NULL AS reloftype, "
            "NULL::oid AS owning_tab, "
            "NULL::int4 AS owning_col, "
            "NULL AS reltablespace, "
            "NULL AS reloptions, "
            "NULL AS toast_reloptions "
            "FROM pg_class "
            "WHERE relkind IN ('%c', '%c', '%c', '%c') "
            "ORDER BY oid",
            username_subquery,
            RELKIND_RELATION,
            RELKIND_SEQUENCE,
            RELKIND_LARGE_SEQUENCE,
            RELKIND_VIEW);
    } else {
        /*
         * Before 7.1, view relkind was not set to 'v', so we must check if we
         * have a view by looking for a rule in pg_rewrite.
         */
        appendPQExpBuffer(query,
            "SELECT "
            "(SELECT oid FROM pg_class WHERE relname = 'pg_class') AS tableoid, "
            "oid, relname, relacl, "
            "CASE WHEN relhasrules and relkind = 'r' "
            "  and EXISTS(SELECT rulename FROM pg_rewrite r WHERE "
            "             r.ev_class = c.oid AND r.ev_type = '1') "
            "THEN '%c'::\"char\" "
            "ELSE relkind END AS relkind,"
            "0::oid AS relnamespace, "
            "(%s relowner) AS rolname, "
            "relchecks, (reltriggers <> 0) AS relhastriggers, "
            "relhasindex, relhasrules, "
            "'t'::bool AS relhasoids, "
            "0 as relfrozenxid, "
            "0 AS toid, "
            "0 AS tfrozenxid, "
            "'p' AS relpersistence, "
            "NULL AS reloftype, "
            "NULL::oid AS owning_tab, "
            "NULL::int4 AS owning_col, "
            "NULL AS reltablespace, "
            "NULL AS reloptions, "
            "NULL AS toast_reloptions "
            "FROM pg_class c "
            "WHERE relkind IN ('%c', '%c', '%c') "
            "ORDER BY oid",
            RELKIND_VIEW,
            username_subquery,
            RELKIND_RELATION,
            RELKIND_SEQUENCE,
            RELKIND_LARGE_SEQUENCE);
    }

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);
    *numTables = ntups;

    /*
     * Extract data from result and lock dumpable tables.  We do the locking
     * before anything else, to minimize the window wherein a table could
     * disappear under us.
     *
     * Note that we have to save info about all tables here, even when dumping
     * only one, because we don't yet know which tables might be inheritance
     * ancestors of the target table.
     */
    tblinfo = (TableInfo*)pg_calloc(ntups, sizeof(TableInfo));

    i_reltableoid = PQfnumber(res, "tableoid");
    i_reloid = PQfnumber(res, "oid");
    i_relname = PQfnumber(res, "relname");
    i_relnamespace = PQfnumber(res, "relnamespace");
    i_relacl = PQfnumber(res, "relacl");
    i_relkind = PQfnumber(res, "relkind");
    i_rolname = PQfnumber(res, "rolname");
    i_relchecks = PQfnumber(res, "relchecks");
    i_relhastriggers = PQfnumber(res, "relhastriggers");
    i_relhasindex = PQfnumber(res, "relhasindex");
    i_relhasrules = PQfnumber(res, "relhasrules");
    i_relhasoids = PQfnumber(res, "relhasoids");
    i_relfrozenxid = PQfnumber(res, "relfrozenxid");
    i_relfrozenxid64 = PQfnumber(res, "relfrozenxid64");
    i_toastoid = PQfnumber(res, "toid");
    i_toastfrozenxid = PQfnumber(res, "tfrozenxid");
    i_toastfrozenxid64 = PQfnumber(res, "tfrozenxid64");
    i_relpersistence = PQfnumber(res, "relpersistence");
    i_relreplident = PQfnumber(res, "relreplident");
    i_relbucket = PQfnumber(res, "relbucket");
    i_parttype = PQfnumber(res, "parttype");
    i_relrowmovement = PQfnumber(res, "relrowmovement");
    i_relcmprs = PQfnumber(res, "relcmprs");
    i_owning_tab = PQfnumber(res, "owning_tab");
    i_owning_col = PQfnumber(res, "owning_col");
    i_relhsblockchain = PQfnumber(res, "relhsblockchain");
#ifdef PGXC
    i_pgxclocatortype = PQfnumber(res, "pgxclocatortype");
    i_pgxcattnum = PQfnumber(res, "pgxcattnum");
    i_pgxc_node_names = PQfnumber(res, "pgxc_node_names");
#endif
    i_reltablespace = PQfnumber(res, "reltablespace");
    i_reloptions = PQfnumber(res, "reloptions");
    i_checkoption = PQfnumber(res, "checkoption");
    i_viewsecurity = PQfnumber(res, "viewsecurity"); 
    i_toastreloptions = PQfnumber(res, "toast_reloptions");
    i_reloftype = PQfnumber(res, "reloftype");

    if ((lockWaitTimeout != NULL) && fout->remoteVersion >= 70300) {
        /*
         * Arrange to fail instead of waiting forever for a table lock.
         *
         * NB: this coding assumes that the only queries issued within the
         * following loop are LOCK TABLEs; else the timeout may be undesirably
         * applied to other things too.
         */
        resetPQExpBuffer(query);
        appendPQExpBuffer(query, "SET statement_timeout = ");
        appendStringLiteralConn(query, lockWaitTimeout, GetConnection(fout));
        ExecuteSqlStatement(fout, query->data);
    }

    for (i = 0; i < ntups; i++) {
        tblinfo[i].dobj.objType = DO_TABLE;
        tblinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_reltableoid));
        tblinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_reloid));
        AssignDumpId(&tblinfo[i].dobj);
        tblinfo[i].dobj.name = gs_strdup(PQgetvalue(res, i, i_relname));
        tblinfo[i].dobj.nmspace =
            findNamespace(fout, atooid(PQgetvalue(res, i, i_relnamespace)), tblinfo[i].dobj.catId.oid);
        tblinfo[i].rolname = gs_strdup(PQgetvalue(res, i, i_rolname));
        tblinfo[i].relacl = gs_strdup(PQgetvalue(res, i, i_relacl));
        tblinfo[i].relkind = *(PQgetvalue(res, i, i_relkind));
        tblinfo[i].relpersistence = *(PQgetvalue(res, i, i_relpersistence));
        tblinfo[i].parttype = *(PQgetvalue(res, i, i_parttype));
        tblinfo[i].relrowmovement = (strcmp(PQgetvalue(res, i, i_relrowmovement), "t") == 0);
        tblinfo[i].relcmprs = atoi(PQgetvalue(res, i, i_relcmprs));

        tblinfo[i].hasindex = (strcmp(PQgetvalue(res, i, i_relhasindex), "t") == 0);
        tblinfo[i].hasrules = (strcmp(PQgetvalue(res, i, i_relhasrules), "t") == 0);
        tblinfo[i].hastriggers = (strcmp(PQgetvalue(res, i, i_relhastriggers), "t") == 0);
        tblinfo[i].hasoids = (strcmp(PQgetvalue(res, i, i_relhasoids), "t") == 0);
        tblinfo[i].isblockchain = (strcmp(PQgetvalue(res, i, i_relhsblockchain), "t") == 0);
        tblinfo[i].isMOT = false;
        tblinfo[i].relreplident = *(PQgetvalue(res, i, i_relreplident));
        tblinfo[i].frozenxid = atooid(PQgetvalue(res, i, i_relfrozenxid));
        tblinfo[i].frozenxid64 = atoxid(PQgetvalue(res, i, i_relfrozenxid64));
        tblinfo[i].toast_oid = atooid(PQgetvalue(res, i, i_toastoid));
        tblinfo[i].toast_frozenxid = atooid(PQgetvalue(res, i, i_toastfrozenxid));
        tblinfo[i].toast_frozenxid64 = atoxid(PQgetvalue(res, i, i_toastfrozenxid64));
        if (PQgetisnull(res, i, i_reloftype))
            tblinfo[i].reloftype = NULL;
        else
            tblinfo[i].reloftype = gs_strdup(PQgetvalue(res, i, i_reloftype));
        if (PQgetisnull(res, i, i_relbucket))
            tblinfo[i].relbucket = -1;
        else
            tblinfo[i].relbucket = atooid(PQgetvalue(res, i, i_relbucket));
        tblinfo[i].ncheck = atoi(PQgetvalue(res, i, i_relchecks));
        if (PQgetisnull(res, i, i_owning_tab)) {
            tblinfo[i].owning_tab = InvalidOid;
            tblinfo[i].owning_col = 0;
        } else {
            tblinfo[i].owning_tab = atooid(PQgetvalue(res, i, i_owning_tab));
            tblinfo[i].owning_col = atoi(PQgetvalue(res, i, i_owning_col));
        }
        if (tblinfo[i].relkind == RELKIND_MATVIEW) {
            tblinfo[i].isIncremental = IsIncrementalMatview(fout, tblinfo[i].dobj.catId.oid);
        } else {
            tblinfo[i].isIncremental = false;
        }
        tblinfo[i].autoinc_attnum = 0;
        tblinfo[i].autoincconstraint = 0;
        tblinfo[i].autoincindex = 0;
#ifdef PGXC
        /* Not all the tables have pgxc locator Data */
        if (PQgetisnull(res, i, i_pgxclocatortype)) {
            tblinfo[i].pgxclocatortype = 'E';
            tblinfo[i].pgxcattnum = 0;
        } else {
            tblinfo[i].pgxclocatortype = *(PQgetvalue(res, i, i_pgxclocatortype));
            tblinfo[i].pgxcattnum = gs_strdup(PQgetvalue(res, i, i_pgxcattnum));
        }
        tblinfo[i].pgxc_node_names = gs_strdup(PQgetvalue(res, i, i_pgxc_node_names));
#endif
        tblinfo[i].reltablespace = gs_strdup(PQgetvalue(res, i, i_reltablespace));
        {
            char* tmp_str = NULL;
            char* p = NULL;
            char* saveptr1 = NULL;
            const char* split = ", ";
            int j = 0, k = 0;
            static const char* redisOpts[] = {
                "append_mode", "append_mode_internal", "end_ctid_internal", "rel_cn_oid", "start_ctid_internal"};
            static const char* timeseries_opt[] = {"ttl", "period"};
            static const char* tde_opt[] = {"dek_cipher", "cmk_id"};
            PQExpBuffer reloptions = createPQExpBuffer();
            tmp_str = gs_strdup(PQgetvalue(res, i, i_reloptions));

            p = strtok_r(tmp_str, split, &saveptr1);
            while (p != NULL) {
                bool is_redis_options = false;
                bool is_timeseries = false;
                bool is_tde_options = false;

                for (j = 0; j < (int)lengthof(redisOpts); j++) {
                    if (pg_strncasecmp(p, redisOpts[j], strlen(redisOpts[j])) == 0) {
                        is_redis_options = true;
                        break;
                    }
                }

                for (j = 0; j < (int)lengthof(timeseries_opt); j++) {
                    if (pg_strncasecmp(p, timeseries_opt[j], strlen(timeseries_opt[j])) == 0) {
                        write_stderr("%s: gs_dump dump timeseries options.\n", p);
                        is_timeseries = true;
                    }
                }

                for (j = 0; j < (int)lengthof(tde_opt); j++) {
                    if (pg_strncasecmp(p, tde_opt[j], strlen(tde_opt[j])) == 0) {
                        is_tde_options = true;
                    }
                }

                if (is_timeseries) {
                    char* q = NULL;
                    char* savaptr2 = NULL;
                    q = strtok_r(p, "=", &savaptr2);
                    if (k > 0) {
                        appendPQExpBuffer(reloptions, ", %s", p);
                    } else {
                        appendPQExpBufferStr(reloptions, q);
                        k++;
                    }
                    appendPQExpBuffer(reloptions, "='%s ",savaptr2);                    
                    q = NULL;
                    q = strtok_r(NULL, ",", &saveptr1);
                    appendPQExpBuffer(reloptions, "%s'", q);
                    p = strtok_r(NULL, split, &saveptr1);
                    continue;
                }

                if (is_tde_options) {
                    char* q = NULL;
                    char* savaptr2 = NULL;
                    q = strtok_r(p, "=", &savaptr2);
                    if (k > 0)
                        appendPQExpBuffer(reloptions, ", %s", q);
                    else {
                        appendPQExpBufferStr(reloptions, q);
                        k++;
                    }
                    appendPQExpBuffer(reloptions, "='%s'",savaptr2); /* add '' for dek_cipher and cmk_id options */
                    p = strtok_r(NULL, split, &saveptr1);
                    continue;
                }

                if (!is_redis_options) {
                    if (k > 0)
                        appendPQExpBuffer(reloptions, ", %s", p);
                    else {
                        appendPQExpBufferStr(reloptions, p);
                        k++;
                    }
                }
                p = strtok_r(NULL, split, &saveptr1);
            }

            if (reloptions->len > 0)
                tblinfo[i].reloptions = gs_strdup(reloptions->data);
            else
                tblinfo[i].reloptions = gs_strdup(PQgetvalue(res, i, i_reloptions));
            destroyPQExpBuffer(reloptions);
            free(tmp_str);
            tmp_str = NULL;
        }
        if (i_checkoption == -1 || PQgetisnull(res, i, i_checkoption))
            tblinfo[i].checkoption = NULL;
        else
            tblinfo[i].checkoption = gs_strdup(PQgetvalue(res, i, i_checkoption));
        tblinfo[i].toast_reloptions = gs_strdup(PQgetvalue(res, i, i_toastreloptions));

        /* b_format database views' sql security options */
        if (i_viewsecurity == -1 || PQgetisnull(res, i, i_viewsecurity))
            tblinfo[i].viewsecurity = NULL;
        else
            tblinfo[i].viewsecurity = gs_strdup(PQgetvalue(res, i, i_viewsecurity));
        /* other fields were zeroed above */

#ifdef ENABLE_MOT
        /* check if table is MOT */
        if (RELKIND_FOREIGN_TABLE == tblinfo[i].relkind) {
            int i_fdwname = -1;
            char* fdwnamestr = NULL;
            PQExpBuffer queryFdw = createPQExpBuffer();
            char* tableName = changeTableName(tblinfo[i].dobj.name);
            resetPQExpBuffer(queryFdw);
            appendPQExpBuffer(queryFdw,
                "SELECT fdw.fdwname FROM "
                "pg_foreign_data_wrapper fdw, pg_foreign_server fs, pg_foreign_table ft, pg_class c "
                "WHERE fdw.oid = fs.srvfdw AND fs.oid = ft.ftserver AND "
                "c.oid = ft.ftrelid AND c.relname = \'%s\' AND c.relnamespace = %u;",
                tableName,
                tblinfo[i].dobj.nmspace->dobj.catId.oid);
            free(tableName);
            tableName = NULL;
            PGresult* resFdw = ExecuteSqlQueryForSingleRow(fout, queryFdw->data);

            i_fdwname = PQfnumber(resFdw, "fdwname");
            fdwnamestr = gs_strdup(PQgetvalue(resFdw, 0, i_fdwname));
            if (0 == strncasecmp(fdwnamestr, MOT_FDW, NAMEDATALEN)) {
                tblinfo[i].isMOT = true;
            }
            free(fdwnamestr);
            fdwnamestr = NULL;
            PQclear(resFdw);
            destroyPQExpBuffer(queryFdw);
        }
#endif

        /*
         * Decide whether we want to dump this table.
         */
        if (tblinfo[i].relkind == RELKIND_COMPOSITE_TYPE)
            tblinfo[i].dobj.dump = false;
        else
            selectDumpableTable(fout, &tblinfo[i]);
        tblinfo[i].interesting = tblinfo[i].dobj.dump;

        /*
         * in Upgrade scenario no need to take a lock on table as
         * no operation on database (specially DDLs)
         */
        if (!binary_upgrade && !non_Lock_Table) {
            if (tblinfo[i].relkind == RELKIND_RELATION &&
                strncmp(tblinfo[i].dobj.name, "BIN$", 4) == 0 &&
                IsRbObject(fout, tblinfo[i].dobj.catId.tableoid, tblinfo[i].dobj.catId.oid, tblinfo[i].dobj.name)) {
                    tblinfo[i].dobj.dump = false;
                    continue;
                }
            /*
             * Read-lock target tables to make sure they aren't DROPPED or altered
             * in schema before we get around to dumping them.
             *
             * Note that we don't explicitly lock parents of the target tables; we
             * assume our lock on the child is enough to prevent schema
             * alterations to parent tables.
             *
             * NOTE: it'd be kinda nice to lock other relations too, not only
             * plain tables, but the backend doesn't presently allow that.
             */
            if (tblinfo[i].dobj.dump && tblinfo[i].relkind == RELKIND_RELATION) {
                resetPQExpBuffer(query);
                appendPQExpBuffer(query,
                    "LOCK TABLE %s IN ACCESS SHARE MODE",
                    fmtQualifiedId(fout, tblinfo[i].dobj.nmspace->dobj.name, tblinfo[i].dobj.name));
                ExecuteSqlStatement(fout, query->data);
            }
        }
    }

    /*
     * When do gs_dump using option --role and -t/--include-table-file. Because permission reason(backup other user's
     * object), maybe there is no tables found
     */
    for (i = 0; i < ntups; i++) {
        if (tblinfo[i].dobj.dump) {
            isTableDumpped = true;
            break;
        }
    }
    /* if using --include-depend-objs/--exclude-self, it will be skipped. Because maybe there is no depend object*/
    if (NULL != table_include_oids.head && !include_depend_objs && !isTableDumpped) {
        PQclear(res);
        destroyPQExpBuffer(query);
        exit_horribly(NULL, "No matching tables were found\n");
    }

    if (NULL != lockWaitTimeout && fout->remoteVersion >= 70300) {
        ExecuteSqlStatement(fout, "SET statement_timeout = 0");
    }
    /* other SQL should use enable_hashjoin false*/
    ExecuteSqlStatement(fout, "set enable_hashjoin=false");

    PQclear(res);
    destroyPQExpBuffer(query);
    return tblinfo;
}

/*
 * Get ivm from gs_matview
 */
static bool IsIncrementalMatview(Archive* fout, Oid matviewOid)
{
    PQExpBuffer query;
    PGresult* res = NULL;
    int ntups;
    char ivm;

    query = createPQExpBuffer();
    appendPQExpBuffer(query, "SELECT ivm FROM gs_matview WHERE matviewid = %u", matviewOid);
    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);
    if (ntups != 1) {
        exit_horribly(NULL,
            "gs_matview has incorrect attribute for type %u, make sure it's a matview", matviewOid);
    }
    ivm = *PQgetvalue(res, 0, 0);

    destroyPQExpBuffer(query);
    PQclear(res);

    return ivm == 't';
}

/*
 * Get matview oid from gs_matview_dependency
 */
static Oid* GetMatviewOid(Archive* fout, Oid tableoid, int *nMatviews)
{
    PQExpBuffer query;
    PGresult* res = NULL;

    query = createPQExpBuffer();
    appendPQExpBuffer(query, "SELECT matviewid FROM gs_matview_dependency WHERE relid = '%u'::oid", tableoid);
    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    *nMatviews = PQntuples(res);
    Oid *matviewoid = NULL;

    if (*nMatviews > 0) {
        matviewoid = (Oid *)pg_malloc(*nMatviews * sizeof(Oid));
        for (int i = 0; i < *nMatviews; i++) {
            matviewoid[i] = atooid(PQgetvalue(res, i, 0));
        }
    }
    destroyPQExpBuffer(query);
    PQclear(res);

    return matviewoid;
}

/*
 * getOwnedSeqs
 *	  identify owned sequences and mark them as dumpable if owning table is
 *
 * We used to do this in getTables(), but it's better to do it after the
 * index used by findTableByOid() has been set up.
 */
void getOwnedSeqs(Archive* fout, TableInfo tblinfo[], int numTables)
{
    int i;

    /*
     * Force sequences that are "owned" by table columns to be dumped whenever
     * their owning table is being dumped.
     */
    for (i = 0; i < numTables; i++) {
        TableInfo* seqinfo = &tblinfo[i];
        TableInfo* owning_tab = NULL;

        if (!OidIsValid(seqinfo->owning_tab))
            continue; /* not an owned sequence */
        if (seqinfo->dobj.dump)
            continue; /* no need to search */
        owning_tab = findTableByOid(seqinfo->owning_tab);
        if (NULL != owning_tab && owning_tab->dobj.dump) {
            seqinfo->interesting = true;
            seqinfo->dobj.dump = true;
        }
    }
}

/*
 * getInherits
 *	  read all the inheritance information
 * from the system catalogs return them in the InhInfo* structure
 *
 * numInherits is set to the number of pairs read in
 * the execute user is independent user, but the grammer is not support, so skip it
 */
InhInfo* getInherits(Archive* fout, int* numInherits)
{
    PGresult* res = NULL;
    int ntups;
    int i;
    PQExpBuffer query = createPQExpBuffer();
    InhInfo* inhinfo = NULL;

    int i_inhrelid;
    int i_inhparent;

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, "pg_catalog");

    /* find all the inheritance information */

    appendPQExpBuffer(query, "SELECT inhrelid, inhparent FROM pg_inherits");

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    ntups = PQntuples(res);

    *numInherits = ntups;

    inhinfo = (InhInfo*)pg_malloc(ntups * sizeof(InhInfo));
    if (inhinfo == NULL) {
        exit_horribly(NULL, "dump is failed to allocate memory!\n");
    }
    i_inhrelid = PQfnumber(res, "inhrelid");
    i_inhparent = PQfnumber(res, "inhparent");

    for (i = 0; i < ntups; i++) {
        inhinfo[i].inhrelid = atooid(PQgetvalue(res, i, i_inhrelid));
        inhinfo[i].inhparent = atooid(PQgetvalue(res, i, i_inhparent));
    }

    PQclear(res);

    destroyPQExpBuffer(query);

    return inhinfo;
}

static void SeparateNonAutoIncrementIndex(TableInfo* tbinfo, ConstraintInfo* constrinfo, IndxInfo* indxinfo)
{
    /* UNIQUE or PRIMARY KEY on auto_increment column should be dump in CREATE TABLE */
    if (constrinfo->contype != 'x' &&
        tbinfo->autoinc_attnum > 0 &&
        tbinfo->autoincconstraint == 0 &&
        (Oid)tbinfo->autoinc_attnum == indxinfo->indkeys[0]) {
        constrinfo->separate = false;
        indxinfo->indexconstraint = constrinfo->dobj.dumpId;
        tbinfo->autoincconstraint = constrinfo->dobj.dumpId;
    } else {
        constrinfo->separate = true;
        indxinfo->indexconstraint = constrinfo->dobj.dumpId;
        /* If pre-7.3 DB, better make sure table comes first */
        addObjectDependency(&constrinfo->dobj, tbinfo->dobj.dumpId);
    }
}

/*
 * getIndexes
 *	  get information about every index on a dumpable table
 *
 * Note: index data is not returned directly to the caller, but it
 * does get entered into the DumpableObject tables.
 */
void getIndexes(Archive* fout, TableInfo tblinfo[], int numTables)
{
    int i = 0;
    int j = 0;
    PQExpBuffer query = createPQExpBuffer();
    PGresult* res = NULL;
    IndxInfo* indxinfo = NULL;
    ConstraintInfo* constrinfo = NULL;
    int i_tableoid = 0;
    int i_oid = 0;
    int i_indexname = 0;
    int i_indexdef = 0;
    int i_indnkeyatts = 0;
    int i_indnatts = 0;
    int i_indnkeys = 0;
    int i_indkey = 0;
    int i_indisclustered = 0;
    int i_indisusable = 0;
    int i_contype = 0;
    int i_conname = 0;
    int i_condeferrable = 0;
    int i_condeferred = 0;
    int i_contableoid = 0;
    int i_conoid = 0;
    int i_condef = 0;
    int i_tablespace = 0;
    int i_options = 0;
    int ntups = 0;
    int i_indisreplident = 0;
    int i_indisvisible = 0;
    ArchiveHandle* AH = (ArchiveHandle*)fout;

    for (i = 0; i < numTables; i++) {
        TableInfo* tbinfo = &tblinfo[i];
        int32 contrants_processed = 0;

        /* Only plain tables, matview and mot have indexes */
        if ((tbinfo->relkind != RELKIND_RELATION && tbinfo->relkind != RELKIND_MATVIEW && !tbinfo->isMOT) ||
            !tbinfo->hasindex)
            continue;

        /* Ignore indexes of tables not to be dumped */
        if (!tbinfo->dobj.dump)
            continue;

        if (g_verbose)
            write_msg(NULL,
                "reading indexes for table \"%s\"\n",
                fmtQualifiedId(fout, tbinfo->dobj.nmspace->dobj.name, tbinfo->dobj.name));

        /* Make sure we are in proper schema so indexdef is right */
        selectSourceSchema(fout, tbinfo->dobj.nmspace->dobj.name);

        /*
         * The point of the messy-looking outer join is to find a constraint
         * that is related by an internal dependency link to the index. If we
         * find one, create a CONSTRAINT entry linked to the INDEX entry.  We
         * assume an index won't have more than one internal dependency.
         *
         * As of 9.0 we don't need to look at pg_depend but can check for a
         * match to pg_constraint.conindid.  The check on conrelid is
         * redundant but useful because that column is indexed while conindid
         * is not.
         */
        resetPQExpBuffer(query);
        if (fout->remoteVersion >= 90000) {
            /*
             * the test on indisready is necessary in 9.2, and harmless in
             * earlier/later versions
             */
            appendPQExpBuffer(query,
                "SELECT t.tableoid, t.oid, "
                "t.relname AS indexname, "
                "pg_catalog.pg_get_indexdef(i.indexrelid, %s) AS indexdef, "
                "i.indnkeyatts AS indnkeyatts, "
                "i.indnatts AS indnatts, "
                "t.relnatts AS indnkeys, "
                "i.indkey, i.indisclustered, i.indisusable, ", schemaOnly ? "true" : "false");
            if (true == is_column_exists(AH->connection, IndexRelationId, "indisreplident")) {
                appendPQExpBuffer(query, "i.indisreplident, ");
            } else {
                appendPQExpBuffer(query, "false AS indisreplident, ");
            }
            if (is_column_exists(AH->connection, IndexRelationId, "indisvisible")) {
                appendPQExpBuffer(query, "i.indisvisible, ");
            } else {
                appendPQExpBuffer(query, "true AS indisvisible, ");
            }
            appendPQExpBuffer(query,
                "c.contype, c.conname, "
                "c.condeferrable, c.condeferred, "
                "c.tableoid AS contableoid, "
                "c.oid AS conoid, "
                "pg_catalog.pg_get_constraintdef(c.oid, false) AS condef, "
                "(SELECT spcname FROM pg_catalog.pg_tablespace s WHERE s.oid = t.reltablespace) AS tablespace, "
                "pg_catalog.array_to_string(t.reloptions, ', ') AS options "
                "FROM pg_catalog.pg_index i "
                "JOIN pg_catalog.pg_class t ON (t.oid = i.indexrelid) "
                "LEFT JOIN pg_catalog.pg_constraint c "
                "ON (i.indrelid = c.conrelid AND "
                "i.indexrelid = c.conindid AND "
                "c.contype IN ('p','u','x')) "
                "WHERE i.indrelid = '%u'::pg_catalog.oid "
                "AND i.indisvalid AND i.indisready "
                "ORDER BY indexname",
                tbinfo->dobj.catId.oid);
        } else if (fout->remoteVersion >= 80200) {
            appendPQExpBuffer(query,
                "SELECT t.tableoid, t.oid, "
                "t.relname AS indexname, "
                "pg_catalog.pg_get_indexdef(i.indexrelid) AS indexdef, "
                "t.relnatts AS indnkeys, "
                "i.indkey, i.indisclustered, i.indisusable, "
                "c.contype, c.conname, "
                "c.condeferrable, c.condeferred, "
                "c.tableoid AS contableoid, "
                "c.oid AS conoid, "
                "null AS condef, "
                "(SELECT spcname FROM pg_catalog.pg_tablespace s WHERE s.oid = t.reltablespace) AS tablespace, "
                "pg_catalog.array_to_string(t.reloptions, ', ') AS options "
                "FROM pg_catalog.pg_index i "
                "JOIN pg_catalog.pg_class t ON (t.oid = i.indexrelid) "
                "LEFT JOIN pg_catalog.pg_depend d "
                "ON (d.classid = t.tableoid "
                "AND d.objid = t.oid "
                "AND d.deptype = 'i') "
                "LEFT JOIN pg_catalog.pg_constraint c "
                "ON (d.refclassid = c.tableoid "
                "AND d.refobjid = c.oid) "
                "WHERE i.indrelid = '%u'::pg_catalog.oid "
                "AND i.indisvalid "
                "ORDER BY indexname",
                tbinfo->dobj.catId.oid);
        } else if (fout->remoteVersion >= 80000) {
            appendPQExpBuffer(query,
                "SELECT t.tableoid, t.oid, "
                "t.relname AS indexname, "
                "pg_catalog.pg_get_indexdef(i.indexrelid) AS indexdef, "
                "t.relnatts AS indnkeys, "
                "i.indkey, i.indisclustered, i.indisusable, "
                "c.contype, c.conname, "
                "c.condeferrable, c.condeferred, "
                "c.tableoid AS contableoid, "
                "c.oid AS conoid, "
                "null AS condef, "
                "(SELECT spcname FROM pg_catalog.pg_tablespace s WHERE s.oid = t.reltablespace) AS tablespace, "
                "null AS options "
                "FROM pg_catalog.pg_index i "
                "JOIN pg_catalog.pg_class t ON (t.oid = i.indexrelid) "
                "LEFT JOIN pg_catalog.pg_depend d "
                "ON (d.classid = t.tableoid "
                "AND d.objid = t.oid "
                "AND d.deptype = 'i') "
                "LEFT JOIN pg_catalog.pg_constraint c "
                "ON (d.refclassid = c.tableoid "
                "AND d.refobjid = c.oid) "
                "WHERE i.indrelid = '%u'::pg_catalog.oid "
                "ORDER BY indexname",
                tbinfo->dobj.catId.oid);
        } else if (fout->remoteVersion >= 70300) {
            appendPQExpBuffer(query,
                "SELECT t.tableoid, t.oid, "
                "t.relname AS indexname, "
                "pg_catalog.pg_get_indexdef(i.indexrelid) AS indexdef, "
                "t.relnatts AS indnkeys, "
                "i.indkey, i.indisclustered, i.indisusable, "
                "c.contype, c.conname, "
                "c.condeferrable, c.condeferred, "
                "c.tableoid AS contableoid, "
                "c.oid AS conoid, "
                "null AS condef, "
                "NULL AS tablespace, "
                "null AS options "
                "FROM pg_catalog.pg_index i "
                "JOIN pg_catalog.pg_class t ON (t.oid = i.indexrelid) "
                "LEFT JOIN pg_catalog.pg_depend d "
                "ON (d.classid = t.tableoid "
                "AND d.objid = t.oid "
                "AND d.deptype = 'i') "
                "LEFT JOIN pg_catalog.pg_constraint c "
                "ON (d.refclassid = c.tableoid "
                "AND d.refobjid = c.oid) "
                "WHERE i.indrelid = '%u'::pg_catalog.oid "
                "ORDER BY indexname",
                tbinfo->dobj.catId.oid);
        } else if (fout->remoteVersion >= 70100) {
            appendPQExpBuffer(query,
                "SELECT t.tableoid, t.oid, "
                "t.relname AS indexname, "
                "pg_catalog.pg_get_indexdef(i.indexrelid) AS indexdef, "
                "t.relnatts AS indnkeys, "
                "i.indkey, false AS indisclustered, i.indisusable, "
                "CASE WHEN i.indisprimary THEN 'p'::char "
                "ELSE '0'::char END AS contype, "
                "t.relname AS conname, "
                "false AS condeferrable, "
                "false AS condeferred, "
                "0::oid AS contableoid, "
                "t.oid AS conoid, "
                "null AS condef, "
                "NULL AS tablespace, "
                "null AS options "
                "FROM pg_index i, pg_class t "
                "WHERE t.oid = i.indexrelid "
                "AND i.indrelid = '%u'::oid "
                "ORDER BY indexname",
                tbinfo->dobj.catId.oid);
        } else {
            appendPQExpBuffer(query,
                "SELECT "
                "(SELECT oid FROM pg_class WHERE relname = 'pg_class') AS tableoid, "
                "t.oid, "
                "t.relname AS indexname, "
                "pg_catalog.pg_get_indexdef(i.indexrelid) AS indexdef, "
                "t.relnatts AS indnkeys, "
                "i.indkey, false AS indisclustered, i.indisusable, "
                "CASE WHEN i.indisprimary THEN 'p'::char "
                "ELSE '0'::char END AS contype, "
                "t.relname AS conname, "
                "false AS condeferrable, "
                "false AS condeferred, "
                "0::oid AS contableoid, "
                "t.oid AS conoid, "
                "null AS condef, "
                "NULL AS tablespace, "
                "null AS options "
                "FROM pg_index i, pg_class t "
                "WHERE t.oid = i.indexrelid "
                "AND i.indrelid = '%u'::oid "
                "ORDER BY indexname",
                tbinfo->dobj.catId.oid);
        }

        res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

        ntups = PQntuples(res);
        if (0 == ntups) {
            PQclear(res);
            continue;
        }
        i_tableoid = PQfnumber(res, "tableoid");
        i_oid = PQfnumber(res, "oid");
        i_indexname = PQfnumber(res, "indexname");
        i_indexdef = PQfnumber(res, "indexdef");
        i_indnkeyatts = PQfnumber(res, "indnkeyatts");
        i_indnatts = PQfnumber(res, "indnatts");
        i_indnkeys = PQfnumber(res, "indnkeys");
        i_indkey = PQfnumber(res, "indkey");
        i_indisclustered = PQfnumber(res, "indisclustered");
        i_indisvisible = PQfnumber(res, "indisvisible");
        i_indisusable = PQfnumber(res, "indisusable");
        i_indisreplident = PQfnumber(res, "indisreplident");
        i_contype = PQfnumber(res, "contype");
        i_conname = PQfnumber(res, "conname");
        i_condeferrable = PQfnumber(res, "condeferrable");
        i_condeferred = PQfnumber(res, "condeferred");
        i_contableoid = PQfnumber(res, "contableoid");
        i_conoid = PQfnumber(res, "conoid");
        i_condef = PQfnumber(res, "condef");
        i_tablespace = PQfnumber(res, "tablespace");
        i_options = PQfnumber(res, "options");

        indxinfo = (IndxInfo*)pg_malloc(ntups * sizeof(IndxInfo));
        constrinfo = (ConstraintInfo*)pg_malloc(ntups * sizeof(ConstraintInfo));

        for (j = 0; j < ntups; j++) {
            char contype;
            contype = *(PQgetvalue(res, j, i_contype));

            indxinfo[j].dobj.objType = DO_INDEX;
            indxinfo[j].dobj.catId.tableoid = atooid(PQgetvalue(res, j, i_tableoid));
            indxinfo[j].dobj.catId.oid = atooid(PQgetvalue(res, j, i_oid));
            AssignDumpId(&indxinfo[j].dobj);
            indxinfo[j].dobj.name = gs_strdup(PQgetvalue(res, j, i_indexname));
            indxinfo[j].dobj.nmspace = tbinfo->dobj.nmspace;
            indxinfo[j].indextable = tbinfo;
            indxinfo[j].indexdef = gs_strdup(PQgetvalue(res, j, i_indexdef));
            indxinfo[j].indnkeyattrs = atoi(PQgetvalue(res, j, i_indnkeyatts));
            indxinfo[j].indnattrs = atoi(PQgetvalue(res, j, i_indnatts));
            indxinfo[j].indnkeys = atoi(PQgetvalue(res, j, i_indnkeys));
            indxinfo[j].tablespace = gs_strdup(PQgetvalue(res, j, i_tablespace));
            indxinfo[j].options = gs_strdup(PQgetvalue(res, j, i_options));

            /*
             * In pre-7.4 releases, indkeys may contain more entries than
             * indnkeys says (since indnkeys will be 1 for a functional
             * index).	We don't actually care about this case since we don't
             * examine indkeys except for indexes associated with PRIMARY and
             * UNIQUE constraints, which are never functional indexes. But we
             * have to allocate enough space to keep parseOidArray from
             * complaining.
             */
            indxinfo[j].indkeys = (Oid*)pg_malloc(INDEX_MAX_KEYS * sizeof(Oid));
            parseOidArray(PQgetvalue(res, j, i_indkey), indxinfo[j].indkeys, INDEX_MAX_KEYS);
            indxinfo[j].indisclustered = (PQgetvalue(res, j, i_indisclustered)[0] == 't');
            indxinfo[j].indisusable = (PQgetvalue(res, j, i_indisusable)[0] == 't');
            indxinfo[j].indisreplident = (PQgetvalue(res, j, i_indisreplident)[0] == 't');
            indxinfo[j].indisvisible = (PQgetvalue(res, j, i_indisvisible)[0] == 't');

            if (contype == 'p' || contype == 'u' || contype == 'x') {
                /*
                 * If we found a constraint matching the index, create an
                 * entry for it.
                 *
                 * In a pre-7.3 database, we take this path iff the index was
                 * marked indisprimary.
                 */
                contrants_processed++;
                constrinfo[j].dobj.objType = tbinfo->isMOT ? DO_FTBL_CONSTRAINT : DO_CONSTRAINT;
                constrinfo[j].dobj.catId.tableoid = atooid(PQgetvalue(res, j, i_contableoid));
                constrinfo[j].dobj.catId.oid = atooid(PQgetvalue(res, j, i_conoid));
                AssignDumpId(&constrinfo[j].dobj);
                constrinfo[j].dobj.name = gs_strdup(PQgetvalue(res, j, i_conname));
                constrinfo[j].dobj.nmspace = tbinfo->dobj.nmspace;
                constrinfo[j].contable = tbinfo;
                constrinfo[j].condomain = NULL;
                constrinfo[j].contype = contype;
                constrinfo[j].condef = gs_strdup(PQgetvalue(res, j, i_condef));
                constrinfo[j].confrelid = InvalidOid;
                constrinfo[j].conindex = indxinfo[j].dobj.dumpId;
                constrinfo[j].condeferrable = *(PQgetvalue(res, j, i_condeferrable)) == 't';
                constrinfo[j].condeferred = *(PQgetvalue(res, j, i_condeferred)) == 't';
                constrinfo[j].conislocal = true;
                SeparateNonAutoIncrementIndex(tbinfo, &constrinfo[j], &indxinfo[j]);
            } else {
                /* Plain secondary index */
                indxinfo[j].indexconstraint = 0;
                if (tbinfo->autoinc_attnum > 0 &&
                    tbinfo->autoincconstraint == 0 &&
                    tbinfo->autoincindex == 0 &&
                    (Oid)tbinfo->autoinc_attnum == indxinfo[j].indkeys[0]) {
                    tbinfo->autoincindex = indxinfo[j].dobj.dumpId;
                }
            }
        }
        if (0 == contrants_processed) {
            GS_FREE(constrinfo);
        }
        PQclear(res);
    }

    destroyPQExpBuffer(query);
}

static void getConstraintsOnForeignTableInternal(TableInfo* tbinfo, Archive* fout)
{
    if (RELKIND_FOREIGN_TABLE == tbinfo->relkind || RELKIND_STREAM == tbinfo->relkind) {
        int j;
        int i_fdwname = -1;
        char* fdwnamestr = NULL;
        bool isHDFSFTbl = false;
        PQExpBuffer query = createPQExpBuffer();
        char* tableName = changeTableName(tbinfo->dobj.name);
        PGresult* res = NULL;
        ArchiveHandle* AH = (ArchiveHandle*)fout;

        resetPQExpBuffer(query);
        appendPQExpBuffer(query,
            "SELECT fdw.fdwname FROM "
            "pg_foreign_data_wrapper fdw, pg_foreign_server fs, pg_foreign_table ft, pg_class c "
            "WHERE fdw.oid = fs.srvfdw AND fs.oid = ft.ftserver AND "
            "c.oid = ft.ftrelid AND c.relname = \'%s\' AND c.relnamespace = %u;",
            tableName,
            tbinfo->dobj.nmspace->dobj.catId.oid);
        free(tableName);
        tableName = NULL;
        res = ExecuteSqlQueryForSingleRow(fout, query->data);

        i_fdwname = PQfnumber(res, "fdwname");
        fdwnamestr = gs_strdup(PQgetvalue(res, 0, i_fdwname));
        if (0 == strncasecmp(fdwnamestr, HDFS_FDW, NAMEDATALEN) ||
            0 == strncasecmp(fdwnamestr, DFS_FDW, NAMEDATALEN) ||
            0 == strncasecmp(fdwnamestr, DIST_FDW, NAMEDATALEN) ||
            0 == strncasecmp(fdwnamestr, "log_fdw", NAMEDATALEN) ||
            0 == strncasecmp(fdwnamestr, GC_FDW, NAMEDATALEN)) {
            isHDFSFTbl = true;
        }
        free(fdwnamestr);
        fdwnamestr = NULL;
        PQclear(res);

        if (isHDFSFTbl) {
            int ntups;
            int i_conname;
            int i_conkey;
            int i_consoft;
            int i_conopt;
            int i_contype;
            int i_contableoid;
            int i_conoid;

            if (g_verbose)
                write_msg(NULL,
                    "reading constraints for foreign table \"%s\"\n",
                    fmtQualifiedId(fout, tbinfo->dobj.nmspace->dobj.name, tbinfo->dobj.name));
            /*
             * select table schema to ensure constraint expr is qualified if
             * needed
             */
            selectSourceSchema(fout, tbinfo->dobj.nmspace->dobj.name);

            resetPQExpBuffer(query);
            appendPQExpBuffer(query,
                "SELECT tableoid, oid, conname, "
                "pg_catalog.unnest(conkey) as conkey, ");
            if (true == is_column_exists(AH->connection, ConstraintRelationId, "consoft")) {
                appendPQExpBuffer(query, "consoft, ");
            } else {
                appendPQExpBuffer(query, "false as consoft, ");
            }

            if (true == is_column_exists(AH->connection, ConstraintRelationId, "conopt")) {
                appendPQExpBuffer(query, "conopt, ");
            } else {
                appendPQExpBuffer(query, "false as conopt, ");
            }

            appendPQExpBuffer(query,
                "contype "
                "FROM pg_catalog.pg_constraint "
                "WHERE conrelid = '%u'::pg_catalog.oid ",
                tbinfo->dobj.catId.oid);
            res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
            ntups = PQntuples(res);
            if (0 == ntups) {
                PQclear(res);
                destroyPQExpBuffer(query);
                return;
            }

            i_conname = PQfnumber(res, "conname");
            i_conkey = PQfnumber(res, "conkey");
            i_consoft = PQfnumber(res, "consoft");
            i_conopt = PQfnumber(res, "conopt");
            i_contype = PQfnumber(res, "contype");

            i_contableoid = PQfnumber(res, "tableoid");
            i_conoid = PQfnumber(res, "oid");

            ConstraintInfo* constrinfo = (ConstraintInfo*)pg_malloc(ntups * sizeof(ConstraintInfo));

            for (j = 0; j < ntups; j++) {
                constrinfo[j].dobj.objType = DO_FTBL_CONSTRAINT;
                constrinfo[j].dobj.catId.tableoid = atooid(PQgetvalue(res, j, i_contableoid));
                constrinfo[j].dobj.catId.oid = atooid(PQgetvalue(res, j, i_conoid));
                AssignDumpId(&constrinfo[j].dobj);

                constrinfo[j].dobj.name = gs_strdup(PQgetvalue(res, j, i_conname));
                constrinfo[j].dobj.nmspace = tbinfo->dobj.nmspace;
                constrinfo[j].contable = tbinfo;
                constrinfo[j].conkey = atooid(PQgetvalue(res, j, i_conkey));

                if ('f' == *PQgetvalue(res, j, i_consoft)) {
                    constrinfo[j].consoft = false;
                    constrinfo[j].conopt = false;
                } else {
                    constrinfo[j].consoft = true;
                    if ('f' == *PQgetvalue(res, j, i_conopt)) {
                        constrinfo[j].conopt = false;
                    } else {
                        constrinfo[j].conopt = true;
                    }
                }

                constrinfo[j].contype = *(PQgetvalue(res, j, i_contype));
                constrinfo[j].condomain = NULL;
                constrinfo[j].conindex = 0;
                constrinfo[j].condeferrable = false;
                constrinfo[j].condeferred = false;
                constrinfo[j].conislocal = true;
                constrinfo[j].separate = true;
            }

            PQclear(res);
        }
        destroyPQExpBuffer(query);
    }
}

/*
 * @hdfs
 * Brief        : Build ConstraintInfo struct for informational constraint info in order to
 *                 dump right foreign table.
 * Description  :Build ConstraintInfo struct for informational constraint info in order to
 *                 dump right foreign table.
 * Input        : fout, a Archive struct
 *                tblinfo, a Tableinfo pointor
 *                numTables, the all tables number
 * Output       : none
 * Return Value : none
 * Notes        : none
 */
void getConstraintsOnForeignTable(Archive* fout, TableInfo tblinfo[], int numTables)
{
    int i;

    ddr_Assert(NULL != fout && NULL != tblinfo);

    /* pg_constraint was created in 7.3, so nothing to do if older */
    if (fout->remoteVersion < 70300) {
        return;
    }

    for (i = 0; i < numTables; i++) {
        getConstraintsOnForeignTableInternal(&tblinfo[i], fout);
    }
}

/*
 * getConstraints
 *
 * Get info about constraints on dumpable tables.
 *
 * Currently handles foreign keys only.
 * Unique and primary key constraints are handled with indexes,
 * while check constraints are processed in getTableAttrs().
 */
void getConstraints(Archive* fout, TableInfo tblinfo[], int numTables)
{
    int i = 0;
    int j = 0;
    ConstraintInfo* constrinfo = NULL;
    PQExpBuffer query;
    PGresult* res = NULL;
    int i_contableoid = 0;
    int i_conoid = 0;
    int i_conname = 0;
    int i_confrelid = 0;
    int i_condef = 0;
    int ntups = 0;

    /* pg_constraint was created in 7.3, so nothing to do if older */
    if (fout->remoteVersion < 70300)
        return;

    query = createPQExpBuffer();

    for (i = 0; i < numTables; i++) {
        TableInfo* tbinfo = &tblinfo[i];

        if (!tbinfo->hastriggers || !tbinfo->dobj.dump)
            continue;

        if (g_verbose)
            write_msg(NULL,
                "reading foreign key constraints for table \"%s\"\n",
                fmtQualifiedId(fout, tbinfo->dobj.nmspace->dobj.name, tbinfo->dobj.name));

        /*
         * select table schema to ensure constraint expr is qualified if
         * needed
         */
        selectSourceSchema(fout, tbinfo->dobj.nmspace->dobj.name);

        resetPQExpBuffer(query);
        appendPQExpBuffer(query,
            "SELECT tableoid, oid, conname, confrelid, "
            "pg_catalog.pg_get_constraintdef(oid) AS condef "
            "FROM pg_catalog.pg_constraint "
            "WHERE conrelid = '%u'::pg_catalog.oid "
            "AND contype = 'f'",
            tbinfo->dobj.catId.oid);
        res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
        ntups = PQntuples(res);
        if (0 == ntups) {
            PQclear(res);
            continue;
        }

        i_contableoid = PQfnumber(res, "tableoid");
        i_conoid = PQfnumber(res, "oid");
        i_conname = PQfnumber(res, "conname");
        i_confrelid = PQfnumber(res, "confrelid");
        i_condef = PQfnumber(res, "condef");

        constrinfo = (ConstraintInfo*)pg_malloc(ntups * sizeof(ConstraintInfo));

        for (j = 0; j < ntups; j++) {
            constrinfo[j].dobj.objType = DO_FK_CONSTRAINT;
            constrinfo[j].dobj.catId.tableoid = atooid(PQgetvalue(res, j, i_contableoid));
            constrinfo[j].dobj.catId.oid = atooid(PQgetvalue(res, j, i_conoid));
            AssignDumpId(&constrinfo[j].dobj);
            constrinfo[j].dobj.name = gs_strdup(PQgetvalue(res, j, i_conname));
            constrinfo[j].dobj.nmspace = tbinfo->dobj.nmspace;
            constrinfo[j].contable = tbinfo;
            constrinfo[j].condomain = NULL;
            constrinfo[j].contype = 'f';
            constrinfo[j].condef = gs_strdup(PQgetvalue(res, j, i_condef));
            constrinfo[j].confrelid = atooid(PQgetvalue(res, j, i_confrelid));
            constrinfo[j].conindex = 0;
            constrinfo[j].condeferrable = false;
            constrinfo[j].condeferred = false;
            constrinfo[j].conislocal = true;
            constrinfo[j].separate = true;
        }

        PQclear(res);
    }

    destroyPQExpBuffer(query);
}

/*
 * getDomainConstraints
 *
 * Get info about constraints on a domain.
 */
static void getDomainConstraints(Archive* fout, TypeInfo* tyinfo)
{
    int i = 0;
    ConstraintInfo* constrinfo = NULL;
    PQExpBuffer query;
    PGresult* res = NULL;
    int i_tableoid = 0;
    int i_oid = 0;
    int i_conname = 0;
    int i_consrc = 0;
    int ntups = 0;

    /* pg_constraint was created in 7.3, so nothing to do if older */
    if (fout->remoteVersion < 70300)
        return;

    /*
     * select appropriate schema to ensure names in constraint are properly
     * qualified
     */
    selectSourceSchema(fout, tyinfo->dobj.nmspace->dobj.name);

    query = createPQExpBuffer();

    if (fout->remoteVersion >= 90100)
        appendPQExpBuffer(query,
            "SELECT tableoid, oid, conname, "
            "pg_catalog.pg_get_constraintdef(oid) AS consrc, "
            "convalidated "
            "FROM pg_catalog.pg_constraint "
            "WHERE contypid = '%u'::pg_catalog.oid "
            "ORDER BY conname",
            tyinfo->dobj.catId.oid);

    else if (fout->remoteVersion >= 70400)
        appendPQExpBuffer(query,
            "SELECT tableoid, oid, conname, "
            "pg_catalog.pg_get_constraintdef(oid) AS consrc, "
            "true as convalidated "
            "FROM pg_catalog.pg_constraint "
            "WHERE contypid = '%u'::pg_catalog.oid "
            "ORDER BY conname",
            tyinfo->dobj.catId.oid);
    else
        appendPQExpBuffer(query,
            "SELECT tableoid, oid, conname, "
            "'CHECK (' || consrc || ')' AS consrc, "
            "true as convalidated "
            "FROM pg_catalog.pg_constraint "
            "WHERE contypid = '%u'::pg_catalog.oid "
            "ORDER BY conname",
            tyinfo->dobj.catId.oid);

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    ntups = PQntuples(res);

    i_tableoid = PQfnumber(res, "tableoid");
    i_oid = PQfnumber(res, "oid");
    i_conname = PQfnumber(res, "conname");
    i_consrc = PQfnumber(res, "consrc");

    constrinfo = (ConstraintInfo*)pg_malloc(ntups * sizeof(ConstraintInfo));

    tyinfo->nDomChecks = ntups;
    tyinfo->domChecks = constrinfo;

    for (i = 0; i < ntups; i++) {
        bool validated = PQgetvalue(res, i, 4)[0] == 't';

        constrinfo[i].dobj.objType = DO_CONSTRAINT;
        constrinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        constrinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&constrinfo[i].dobj);
        constrinfo[i].dobj.name = gs_strdup(PQgetvalue(res, i, i_conname));
        constrinfo[i].dobj.nmspace = tyinfo->dobj.nmspace;
        constrinfo[i].contable = NULL;
        constrinfo[i].condomain = tyinfo;
        constrinfo[i].contype = 'c';
        constrinfo[i].condef = gs_strdup(PQgetvalue(res, i, i_consrc));
        constrinfo[i].confrelid = InvalidOid;
        constrinfo[i].conindex = 0;
        constrinfo[i].condeferrable = false;
        constrinfo[i].condeferred = false;
        constrinfo[i].conislocal = true;

        constrinfo[i].separate = !validated;

        /*
         * Make the domain depend on the constraint, ensuring it won't be
         * output till any constraint dependencies are OK.	If the constraint
         * has not been validated, it's going to be dumped after the domain
         * anyway, so this doesn't matter.
         */
        if (validated)
            addObjectDependency(&tyinfo->dobj, constrinfo[i].dobj.dumpId);
    }

    PQclear(res);

    destroyPQExpBuffer(query);
}

/*
 * getRules
 *	  get basic information about every rule in the system
 *
 * numRules is set to the number of rules read in
 */
RuleInfo* getRules(Archive* fout, int* numRules)
{
    PGresult* res = NULL;
    int ntups;
    int i;
    PQExpBuffer query;
    RuleInfo* ruleinfo = NULL;
    int i_tableoid;
    int i_oid;
    int i_rulename;
    int i_ruletable;
    int i_ev_type;
    int i_is_instead;
    int i_ev_enabled;
    ArchiveHandle* AH = (ArchiveHandle*)fout;
    char* execUser = NULL;

    /*
     * No need to recheck object dependencies when backing up single objects
     */
    if (isSingleTableDump) {
        return NULL;
    }

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, "pg_catalog");

    query = createPQExpBuffer();
    if (fout->remoteVersion >= 80300) {
        /*
         * When executing user/--role is not super/sysadmin, we only need to obtain it's rules
         * Because only the objects(sequence/table/foreign table/view) of the user are backed up
         */
        if (use_role != NULL) {
            if (isSuperRole(AH->connection, use_role)) {
                appendPQExpBuffer(query,
                    "SELECT "
                    "tableoid, oid, rulename, "
                    "ev_class AS ruletable, ev_type, is_instead, "
                    "ev_enabled "
                    "FROM pg_rewrite "
                    "ORDER BY oid");
            } else {
                appendPQExpBuffer(query,
                    "SELECT re.tableoid, re.oid, re.rulename, re.ev_class AS ruletable, "
                    "re.ev_type, re.is_instead, re.ev_enabled "
                    "FROM pg_rewrite re, pg_class pc, pg_roles rol "
                    "WHERE rol.rolname = '%s' AND rol.oid = pc.relowner AND re.ev_class = pc.oid "
                    "ORDER BY re.oid;",
                    use_role);
            }
        } else {
            execUser = PQuser(AH->connection);
            if (isSuperRole(AH->connection, execUser)) {
                appendPQExpBuffer(query,
                    "SELECT "
                    "tableoid, oid, rulename, "
                    "ev_class AS ruletable, ev_type, is_instead, "
                    "ev_enabled "
                    "FROM pg_rewrite "
                    "ORDER BY oid");
            } else {
                appendPQExpBuffer(query,
                    "SELECT re.tableoid, re.oid, re.rulename, re.ev_class AS ruletable, "
                    "re.ev_type, re.is_instead, re.ev_enabled "
                    "FROM pg_rewrite re, pg_class pc, pg_roles rol "
                    "WHERE rol.rolname = '%s' AND rol.oid = pc.relowner AND re.ev_class = pc.oid "
                    "ORDER BY re.oid;",
                    execUser);
            }
        }
    } else if (fout->remoteVersion >= 70100) {
        appendPQExpBuffer(query,
            "SELECT "
            "tableoid, oid, rulename, "
            "ev_class AS ruletable, ev_type, is_instead, "
            "'O'::char AS ev_enabled "
            "FROM pg_rewrite "
            "ORDER BY oid");
    } else {
        appendPQExpBuffer(query,
            "SELECT "
            "(SELECT oid FROM pg_class WHERE relname = 'pg_rewrite') AS tableoid, "
            "oid, rulename, "
            "ev_class AS ruletable, ev_type, is_instead, "
            "'O'::char AS ev_enabled "
            "FROM pg_rewrite "
            "ORDER BY oid");
    }

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);
    *numRules = ntups;
    /* Skip without association map */
    if (0 == ntups) {
        PQclear(res);
        destroyPQExpBuffer(query);
        return NULL;
    }
    ruleinfo = (RuleInfo*)pg_malloc(ntups * sizeof(RuleInfo));
    i_tableoid = PQfnumber(res, "tableoid");
    i_oid = PQfnumber(res, "oid");
    i_rulename = PQfnumber(res, "rulename");
    i_ruletable = PQfnumber(res, "ruletable");
    i_ev_type = PQfnumber(res, "ev_type");
    i_is_instead = PQfnumber(res, "is_instead");
    i_ev_enabled = PQfnumber(res, "ev_enabled");

    for (i = 0; i < ntups; i++) {
        Oid ruletableoid;

        ruleinfo[i].dobj.objType = DO_RULE;
        ruleinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        ruleinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&ruleinfo[i].dobj);
        ruleinfo[i].dobj.name = gs_strdup(PQgetvalue(res, i, i_rulename));
        ruletableoid = atooid(PQgetvalue(res, i, i_ruletable));
        ruleinfo[i].ruletable = findTableByOid(ruletableoid);
        if (ruleinfo[i].ruletable == NULL)
            exit_horribly(NULL,
                "failed sanity check, parent table OID %u of pg_rewrite entry OID %u not found\n",
                ruletableoid,
                ruleinfo[i].dobj.catId.oid);
        ruleinfo[i].dobj.nmspace = ruleinfo[i].ruletable->dobj.nmspace;
        ruleinfo[i].dobj.dump = ruleinfo[i].ruletable->dobj.dump;
        ruleinfo[i].ev_type = *(PQgetvalue(res, i, i_ev_type));
        ruleinfo[i].is_instead = *(PQgetvalue(res, i, i_is_instead)) == 't';
        ruleinfo[i].ev_enabled = *(PQgetvalue(res, i, i_ev_enabled));
        if (ruleinfo[i].ruletable != NULL) {
            /*
             * If the table is a view, force its ON SELECT rule to be sorted
             * before the view itself --- this ensures that any dependencies
             * for the rule affect the table's positioning. Other rules are
             * forced to appear after their table.
             * Also applied for matview.
             */
            bool flag = (ruleinfo[i].ruletable->relkind == RELKIND_VIEW ||
                        ruleinfo[i].ruletable->relkind == RELKIND_MATVIEW ||
                        ruleinfo[i].ruletable->relkind == RELKIND_CONTQUERY)
                        && ruleinfo[i].ev_type == '1' && ruleinfo[i].is_instead;
            if (flag) {
                addObjectDependency(&ruleinfo[i].ruletable->dobj, ruleinfo[i].dobj.dumpId);
                /* We'll merge the rule into CREATE VIEW, if possible */
                ruleinfo[i].separate = false;
            } else {
                addObjectDependency(&ruleinfo[i].dobj, ruleinfo[i].ruletable->dobj.dumpId);
                ruleinfo[i].separate = true;
            }
        } else
            ruleinfo[i].separate = true;

        /*
         * If we're forced to break a dependency loop by dumping a view as a
         * table and separate _RETURN rule, we'll move the view's reloptions
         * to the rule.  (This is necessary because tables and views have
         * different valid reloptions, so we can't apply the options until the
         * backend knows it's a view.)  Otherwise the rule's reloptions stay
         * NULL.
         */
        ruleinfo[i].reloptions = NULL;
    }

    PQclear(res);

    destroyPQExpBuffer(query);

    return ruleinfo;
}

/*
 * getRlsPolicies
 *     get row level security policies on a dumpable table
 *
 * @param (in) Archive: Archive information for dump.
 * @param (in) tblinfo: Table information array.
 * @param (in) numTables: Table nums in array tblinfo.
 * @return: void
 */
void getRlsPolicies(Archive* fout, TableInfo tblinfo[], int numTables)
{
    /* if dataonly, skip get row level security policies */
    if (dataOnly)
        return;

    ArchiveHandle* AH = (ArchiveHandle*)fout;
    /* check whether exist catalog pg_rlspolicy by column(pg_rlspolicy.polname) */
    if (false == is_column_exists(AH->connection, RlsPolicyRelationId, "polname"))
        return;

    /* analysis all row level security policies */
    RlsPolicyInfo* policyinfo = NULL;
    PQExpBuffer query = createPQExpBuffer();
    PGresult* res = NULL;
    TableInfo* tbinfo = NULL;
    int i = 0;

    /* check all tables */
    for (i = 0; i < numTables; i++) {
        tbinfo = &tblinfo[i];
        if ((tbinfo->dobj.dump == false) || (tbinfo->relkind != RELKIND_RELATION) ||
            (tbinfo->dobj.catId.oid < FirstNormalObjectId))
            continue;

        /* create a dumpable object for row level security policy */
        if (g_verbose)
            write_msg(NULL,
                "reading policies for table \"%s\"\n",
                fmtQualifiedId(fout, tbinfo->dobj.nmspace->dobj.name, tbinfo->dobj.name));

        resetPQExpBuffer(query);

        /* get the row level security policy from pg_rlspolicy(skip policies for system catalog) */
        appendPQExpBuffer(query,
            "SELECT rlspolicy.polrelid, rlspolicy.oid, rlspolicy.polname, "
            "rlspolicy.polcmd, rlspolicy.polpermissive, "
            "CASE WHEN rlspolicy.polroles = '{0}' THEN 'public' ELSE "
            "pg_catalog.array_to_string(ARRAY(SELECT pg_roles.rolname FROM pg_roles "
            "WHERE pg_roles.oid = ANY(rlspolicy.polroles) ORDER BY pg_roles.rolname), ', ') END AS polroles, "
            "pg_catalog.pg_get_expr(rlspolicy.polqual, rlspolicy.polrelid) AS polqual "
            "FROM pg_catalog.pg_rlspolicy rlspolicy WHERE rlspolicy.polrelid = %u;",
            (Oid)tbinfo->dobj.catId.oid);
        res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
        int ntups = PQntuples(res);
        if (0 == ntups) {
            PQclear(res);
            continue;
        }
        policyinfo = (RlsPolicyInfo*)pg_malloc(ntups * sizeof(RlsPolicyInfo));
        int i_tableoid = PQfnumber(res, "polrelid");
        int i_oid = PQfnumber(res, "oid");
        int i_polname = PQfnumber(res, "polname");
        int i_polcmd = PQfnumber(res, "polcmd");
        int i_polpermissive = PQfnumber(res, "polpermissive");
        int i_polroles = PQfnumber(res, "polroles");
        int i_polqual = PQfnumber(res, "polqual");
        int j = 0;
        for (j = 0; j < ntups; j++) {
            policyinfo[j].dobj.objType = DO_RLSPOLICY;
            policyinfo[j].dobj.catId.tableoid = atooid(PQgetvalue(res, j, i_tableoid));
            policyinfo[j].dobj.catId.oid = atooid(PQgetvalue(res, j, i_oid));
            AssignDumpId(&policyinfo[j].dobj);
            policyinfo[j].dobj.nmspace = tbinfo->dobj.nmspace;
            policyinfo[j].dobj.name = gs_strdup(tbinfo->dobj.name);
            policyinfo[j].dobj.dump = tbinfo->dobj.dump;
            policyinfo[j].policytable = tbinfo;
            policyinfo[j].polname = gs_strdup(PQgetvalue(res, j, i_polname));
            policyinfo[j].polcmd = *(PQgetvalue(res, j, i_polcmd));
            policyinfo[j].polpermissive = *(PQgetvalue(res, j, i_polpermissive)) == 't';
            policyinfo[j].polroles = gs_strdup(PQgetvalue(res, j, i_polroles));
            policyinfo[j].polqual = gs_strdup(PQgetvalue(res, j, i_polqual));
        }
        PQclear(res);
    }
    destroyPQExpBuffer(query);
    return;
}

/*
 * getTriggers
 *	  get information about every trigger on a dumpable table
 *
 * Note: trigger data is not returned directly to the caller, but it
 * does get entered into the DumpableObject tables.
 * the execute user is independent user, but the grammer is not support, so skip it
 */
void getTriggers(Archive* fout, TableInfo tblinfo[], int numTables)
{
    int i = 0;
    int j = 0;
    PQExpBuffer query = createPQExpBuffer();
    PGresult* res = NULL;
    TriggerInfo* tginfo = NULL;
    int i_tableoid = 0;
    int i_oid = 0;
    int i_tgname = 0;
    int i_tgfname = 0;
    int i_tgtype = 0;
    int i_tgnargs = 0;
    int i_tgargs = 0;
    int i_tgisconstraint = 0;
    int i_tgconstrname = 0;
    int i_tgconstrrelid = 0;
    int i_tgconstrrelname = 0;
    int i_tgenabled = 0;
    int i_tgdeferrable = 0;
    int i_tginitdeferred = 0;
    int i_tgdef = 0;
    int i_tgdb = 0;
    int ntups = 0;

    for (i = 0; i < numTables; i++) {
        TableInfo* tbinfo = &tblinfo[i];

        if (!tbinfo->hastriggers || !tbinfo->dobj.dump)
            continue;

        if (g_verbose)
            write_msg(NULL,
                "reading triggers for table \"%s\"\n",
                fmtQualifiedId(fout, tbinfo->dobj.nmspace->dobj.name, tbinfo->dobj.name));

        /*
         * select table schema to ensure regproc name is qualified if needed
         */
        selectSourceSchema(fout, tbinfo->dobj.nmspace->dobj.name);

        resetPQExpBuffer(query);
        if (fout->remoteVersion >= 90000) {
            /*
             * NB: think not to use pretty=true in pg_get_triggerdef.  It
             * could result in non-forward-compatible dumps of WHEN clauses
             * due to under-parenthesization.
             */
            if (GetVersionNum(fout) >= B_DUMP_TRIGGER_VERSION_NUM) {
                appendPQExpBuffer(query,
                    "SELECT tgname, tgfbody, "
                    "tgfoid::pg_catalog.regproc AS tgfname, "
                    "pg_catalog.pg_get_triggerdef(oid, false) AS tgdef, "
                    "tgenabled, tableoid, oid "
                    "FROM pg_catalog.pg_trigger t "
                    "WHERE tgrelid = '%u'::pg_catalog.oid "
                    "AND NOT tgisinternal",
                    tbinfo->dobj.catId.oid);
            } else {
                appendPQExpBuffer(query,
                    "SELECT tgname, "
                    "tgfoid::pg_catalog.regproc AS tgfname, "
                    "pg_catalog.pg_get_triggerdef(oid, false) AS tgdef, "
                    "tgenabled, tableoid, oid "
                    "FROM pg_catalog.pg_trigger t "
                    "WHERE tgrelid = '%u'::pg_catalog.oid "
                    "AND NOT tgisinternal",
                    tbinfo->dobj.catId.oid);
            }
        } else if (fout->remoteVersion >= 80300) {
            /*
             * We ignore triggers that are tied to a foreign-key constraint
             */
            appendPQExpBuffer(query,
                "SELECT tgname, "
                "tgfoid::pg_catalog.regproc AS tgfname, "
                "tgtype, tgnargs, tgargs, tgenabled, "
                "tgisconstraint, tgconstrname, tgdeferrable, "
                "tgconstrrelid, tginitdeferred, tableoid, oid, "
                "tgconstrrelid::pg_catalog.regclass AS tgconstrrelname "
                "FROM pg_catalog.pg_trigger t "
                "WHERE tgrelid = '%u'::pg_catalog.oid "
                "AND tgconstraint = 0",
                tbinfo->dobj.catId.oid);
        } else if (fout->remoteVersion >= 70300) {
            /*
             * We ignore triggers that are tied to a foreign-key constraint,
             * but in these versions we have to grovel through pg_constraint
             * to find out
             */
            appendPQExpBuffer(query,
                "SELECT tgname, "
                "tgfoid::pg_catalog.regproc AS tgfname, "
                "tgtype, tgnargs, tgargs, tgenabled, "
                "tgisconstraint, tgconstrname, tgdeferrable, "
                "tgconstrrelid, tginitdeferred, tableoid, oid, "
                "tgconstrrelid::pg_catalog.regclass AS tgconstrrelname "
                "FROM pg_catalog.pg_trigger t "
                "WHERE tgrelid = '%u'::pg_catalog.oid "
                "AND (NOT tgisconstraint "
                " OR NOT EXISTS"
                "  (SELECT 1 FROM pg_catalog.pg_depend d "
                "   JOIN pg_catalog.pg_constraint c ON (d.refclassid = c.tableoid AND d.refobjid = c.oid) "
                "   WHERE d.classid = t.tableoid AND d.objid = t.oid AND d.deptype = 'i' AND c.contype = 'f'))",
                tbinfo->dobj.catId.oid);
        } else if (fout->remoteVersion >= 70100) {
            appendPQExpBuffer(query,
                "SELECT tgname, tgfoid::regproc AS tgfname, "
                "tgtype, tgnargs, tgargs, tgenabled, "
                "tgisconstraint, tgconstrname, tgdeferrable, "
                "tgconstrrelid, tginitdeferred, tableoid, oid, "
                "(SELECT relname FROM pg_class WHERE oid = tgconstrrelid) "
                "     AS tgconstrrelname "
                "FROM pg_trigger "
                "WHERE tgrelid = '%u'::oid",
                tbinfo->dobj.catId.oid);
        } else {
            appendPQExpBuffer(query,
                "SELECT tgname, tgfoid::regproc AS tgfname, "
                "tgtype, tgnargs, tgargs, tgenabled, "
                "tgisconstraint, tgconstrname, tgdeferrable, "
                "tgconstrrelid, tginitdeferred, "
                "(SELECT oid FROM pg_class WHERE relname = 'pg_trigger') AS tableoid, "
                "oid, "
                "(SELECT relname FROM pg_class WHERE oid = tgconstrrelid) "
                "     AS tgconstrrelname "
                "FROM pg_trigger "
                "WHERE tgrelid = '%u'::oid",
                tbinfo->dobj.catId.oid);
        }
        res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

        ntups = PQntuples(res);
        if (0 == ntups) {
            PQclear(res);
            continue;
        }

        i_tableoid = PQfnumber(res, "tableoid");
        i_oid = PQfnumber(res, "oid");
        i_tgname = PQfnumber(res, "tgname");
        i_tgfname = PQfnumber(res, "tgfname");
        i_tgtype = PQfnumber(res, "tgtype");
        i_tgnargs = PQfnumber(res, "tgnargs");
        i_tgargs = PQfnumber(res, "tgargs");
        i_tgisconstraint = PQfnumber(res, "tgisconstraint");
        i_tgconstrname = PQfnumber(res, "tgconstrname");
        i_tgconstrrelid = PQfnumber(res, "tgconstrrelid");
        i_tgconstrrelname = PQfnumber(res, "tgconstrrelname");
        i_tgenabled = PQfnumber(res, "tgenabled");
        i_tgdeferrable = PQfnumber(res, "tgdeferrable");
        i_tginitdeferred = PQfnumber(res, "tginitdeferred");
        i_tgdef = PQfnumber(res, "tgdef");
        i_tgdb = PQfnumber(res, "tgfbody");

        tginfo = (TriggerInfo*)pg_malloc(ntups * sizeof(TriggerInfo));

        for (j = 0; j < ntups; j++) {
            tginfo[j].dobj.objType = DO_TRIGGER;
            tginfo[j].dobj.catId.tableoid = atooid(PQgetvalue(res, j, i_tableoid));
            tginfo[j].dobj.catId.oid = atooid(PQgetvalue(res, j, i_oid));
            AssignDumpId(&tginfo[j].dobj);
            tginfo[j].dobj.name = gs_strdup(PQgetvalue(res, j, i_tgname));
            tginfo[j].dobj.nmspace = tbinfo->dobj.nmspace;
            tginfo[j].tgtable = tbinfo;
            tginfo[j].tgenabled = *(PQgetvalue(res, j, i_tgenabled));
            tginfo[j].tgfname = gs_strdup(PQgetvalue(res, j, i_tgfname));
            if (!PQgetisnull(res, j, i_tgdb)) {
                char *body =  PQgetvalue(res, j, i_tgdb);
                tginfo[j].tgdb = true;
                if (pg_strncasecmp(body, BEGIN_P_STR, BEGIN_P_LEN) == 0 ) {
                    tginfo[j].tgbodybstyle = true;
                } else {
                    tginfo[j].tgbodybstyle = false;
                }
            } else {
                tginfo[j].tgdb = false;
                tginfo[j].tgbodybstyle = false;
            }
            if (i_tgdef >= 0) {
                tginfo[j].tgdef = gs_strdup(PQgetvalue(res, j, i_tgdef));

                /* remaining fields are not valid if we have tgdef */
                tginfo[j].tgtype = 0;
                tginfo[j].tgnargs = 0;
                tginfo[j].tgargs = NULL;
                tginfo[j].tgisconstraint = false;
                tginfo[j].tgdeferrable = false;
                tginfo[j].tginitdeferred = false;
                tginfo[j].tgconstrname = NULL;
                tginfo[j].tgconstrrelid = InvalidOid;
                tginfo[j].tgconstrrelname = NULL;
            } else {
                tginfo[j].tgdef = NULL;

                tginfo[j].tgtype = atoi(PQgetvalue(res, j, i_tgtype));
                tginfo[j].tgnargs = atoi(PQgetvalue(res, j, i_tgnargs));
                tginfo[j].tgargs = gs_strdup(PQgetvalue(res, j, i_tgargs));
                tginfo[j].tgisconstraint = *(PQgetvalue(res, j, i_tgisconstraint)) == 't';
                tginfo[j].tgdeferrable = *(PQgetvalue(res, j, i_tgdeferrable)) == 't';
                tginfo[j].tginitdeferred = *(PQgetvalue(res, j, i_tginitdeferred)) == 't';

                if (tginfo[j].tgisconstraint) {
                    tginfo[j].tgconstrname = gs_strdup(PQgetvalue(res, j, i_tgconstrname));
                    tginfo[j].tgconstrrelid = atooid(PQgetvalue(res, j, i_tgconstrrelid));
                    if (OidIsValid(tginfo[j].tgconstrrelid)) {
                        if (PQgetisnull(res, j, i_tgconstrrelname))
                            exit_horribly(NULL,
                                "query produced null referenced table name for foreign key trigger \"%s\" on table "
                                "\"%s\" (OID of table: %u)\n",
                                tginfo[j].dobj.name,
                                tbinfo->dobj.name,
                                tginfo[j].tgconstrrelid);
                        tginfo[j].tgconstrrelname = gs_strdup(PQgetvalue(res, j, i_tgconstrrelname));
                    } else
                        tginfo[j].tgconstrrelname = NULL;
                } else {
                    tginfo[j].tgconstrname = NULL;
                    tginfo[j].tgconstrrelid = InvalidOid;
                    tginfo[j].tgconstrrelname = NULL;
                }
            }
        }

        PQclear(res);
    }

    destroyPQExpBuffer(query);
}

EventInfo* getEvents(Archive *fout, int *numEvents)
{
    PGresult *res = NULL;
    PGresult *attres = NULL;
    PGresult *procres = NULL;
    int ntups;
    int attntups;
    int i;
    int j;
    PQExpBuffer query;
    EventInfo *evinfo = NULL;
    int i_oid;
    int i_jobid;
    int i_definer;
    int i_evname;
    int i_starttime;
    int i_endtime;
    int i_intervaltime;
    int i_evstatus;
    int i_evbody;
    int i_nspname;
    char* database_name = PQdb(GetConnection(fout));
    bool is_bcompatibility = findDBCompatibility(fout, PQdb(GetConnection(fout)));

    if (GetVersionNum(fout) < EVENT_VERSION) {
        return NULL;
    }
    /* Make sure we are in proper schema */
    selectSourceSchema(fout, "pg_catalog");
    query = createPQExpBuffer();
    if (is_bcompatibility) {
        appendPQExpBuffer(
            query,
            "SELECT pg_job.oid,  job_id, log_user, job_name, pg_job.nspname, pg_namespace.oid, dbname, start_date, "
            "end_date, interval, enable "
            "FROM pg_job LEFT join pg_namespace on pg_namespace.nspname = pg_job.nspname where dbname=\'%s\'",
            database_name);
        res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
        
        ntups = PQntuples(res);
        if (ntups == 0) {
            PQclear(res);
            destroyPQExpBuffer(query);
            return NULL;
        }
        *numEvents = ntups;
        evinfo = (EventInfo *)pg_malloc(ntups * sizeof(EventInfo));
        i_definer = PQfnumber(res, "log_user");
        i_evname = PQfnumber(res, "job_name");
        i_starttime = PQfnumber(res, "start_date");
        i_endtime = PQfnumber(res, "end_date");
        i_intervaltime = PQfnumber(res, "interval");
        i_evstatus = PQfnumber(res, "enable");
        i_oid = PQfnumber(res, "oid");
        i_nspname = PQfnumber(res, "nspname");
        i_jobid = PQfnumber(res, "job_id");
        for (i = 0; i < ntups; i++) {
            evinfo[i].dobj.objType = DO_EVENT;
            evinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
            AssignDumpId(&evinfo[i].dobj);
            evinfo[i].dobj.name = gs_strdup(PQgetvalue(res, i, i_evname));
            evinfo[i].dobj.nmspace =
                findNamespace(fout, atooid(PQgetvalue(res, i, 5)), evinfo[i].dobj.catId.oid);
            evinfo[i].evdefiner = gs_strdup(PQgetvalue(res, i, i_definer));
            evinfo[i].evname = gs_strdup(PQgetvalue(res, i, i_evname));
            evinfo[i].starttime = gs_strdup(PQgetvalue(res, i, i_starttime));
            evinfo[i].endtime = gs_strdup(PQgetvalue(res, i, i_endtime));
            evinfo[i].intervaltime = gs_strdup(PQgetvalue(res, i, i_intervaltime));
            evinfo[i].evstatus = (PQgetvalue(res, i, i_evstatus)[0] == 't');
            evinfo[i].nspname = gs_strdup(PQgetvalue(res, i, i_nspname));
            evinfo[i].comment = NULL;
            Oid ev_oid = atooid(PQgetvalue(res, i, i_jobid));
            PQExpBuffer attquery = createPQExpBuffer();
            appendPQExpBuffer(attquery, "SELECT "
            "* "
            "FROM gs_job_attribute where job_name='%s'", evinfo[i].evname);
            attres = ExecuteSqlQuery(fout, attquery->data, PGRES_TUPLES_OK);
            attntups = PQntuples(attres);
            int i_attributename;
            int i_attributevalue;
            for (j = 0; j < attntups; j++) {
                i_attributename = PQfnumber(attres, "attribute_name");
                i_attributevalue = PQfnumber(attres, "attribute_value");
                if (strcmp(PQgetvalue(attres, j, i_attributename), "auto_drop") == 0) {
                    evinfo[i].autodrop = (PQgetvalue(attres, j, i_attributevalue)[0] == 't');
                } else if (strcmp(PQgetvalue(attres, j, i_attributename), "comments") == 0) {
                    evinfo[i].comment = gs_strdup(PQgetvalue(attres, j, i_attributevalue));
                }
            }
            PQExpBuffer procquery = createPQExpBuffer();
            appendPQExpBuffer(procquery, "SELECT "
            "what "
            "FROM pg_job_proc where job_id=%u", ev_oid);
            procres = ExecuteSqlQuery(fout, procquery->data, PGRES_TUPLES_OK);
            i_evbody = PQfnumber(procres, "what");
            evinfo[i].evbody = gs_strdup(PQgetvalue(procres, 0, i_evbody));
            PQclear(attres);
            PQclear(procres);
            destroyPQExpBuffer(attquery);
            destroyPQExpBuffer(procquery);
            selectDumpableObject(&(evinfo[i].dobj), fout);
        }
    }
    
    PQclear(res);

    destroyPQExpBuffer(query);
    return evinfo;
}

/*
 * getProcLangs
 *	  get basic information about every procedural language in the system
 *
 * numProcLangs is set to the number of langs read in
 *
 * NB: this must run after getFuncs() because we assume we can do
 * findFuncByOid().
 */
ProcLangInfo* getProcLangs(Archive* fout, int* numProcLangs)
{
    PGresult* res = NULL;
    int ntups = 0;
    int i = 0;
    PQExpBuffer query = createPQExpBuffer();
    ProcLangInfo* planginfo = NULL;
    int i_tableoid = 0;
    int i_oid = 0;
    int i_lanname = 0;
    int i_lanpltrusted = 0;
    int i_lanplcallfoid = 0;
    int i_laninline = 0;
    int i_lanvalidator = 0;
    int i_lanacl = 0;
    int i_lanowner = 0;

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, "pg_catalog");

    if (fout->remoteVersion >= 90000) {
        /* pg_language has a laninline column */
        appendPQExpBuffer(query,
            "SELECT tableoid, oid, "
            "lanname, lanpltrusted, lanplcallfoid, "
            "laninline, lanvalidator,  lanacl, "
            "(%s lanowner) AS rolename "
            "FROM pg_language "
            "WHERE lanispl "
            "ORDER BY oid",
            username_subquery);
    } else if (fout->remoteVersion >= 80300) {
        /* pg_language has a lanowner column */
        appendPQExpBuffer(query,
            "SELECT tableoid, oid, "
            "lanname, lanpltrusted, lanplcallfoid, "
            "lanvalidator,  lanacl, "
            "(%s lanowner) AS rolename "
            "FROM pg_language "
            "WHERE lanispl "
            "ORDER BY oid",
            username_subquery);
    } else if (fout->remoteVersion >= 80100) {
        /* Languages are owned by the bootstrap superuser, OID 10 */
        appendPQExpBuffer(query,
            "SELECT tableoid, oid, *, "
            "(%s '10') AS rolename "
            "FROM pg_language "
            "WHERE lanispl "
            "ORDER BY oid",
            username_subquery);
    } else if (fout->remoteVersion >= 70400) {
        /* Languages are owned by the bootstrap superuser, sysid 1 */
        appendPQExpBuffer(query,
            "SELECT tableoid, oid, *, "
            "(%s '1') AS rolename "
            "FROM pg_language "
            "WHERE lanispl "
            "ORDER BY oid",
            username_subquery);
    } else if (fout->remoteVersion >= 70100) {
        /* No clear notion of an owner at all before 7.4 ... */
        appendPQExpBuffer(query,
            "SELECT tableoid, oid, * FROM pg_language "
            "WHERE lanispl "
            "ORDER BY oid");
    } else {
        appendPQExpBuffer(query,
            "SELECT "
            "(SELECT oid FROM pg_class WHERE relname = 'pg_language') AS tableoid, "
            "oid, * FROM pg_language "
            "WHERE lanispl "
            "ORDER BY oid");
    }

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    ntups = PQntuples(res);

    *numProcLangs = ntups;

    planginfo = (ProcLangInfo*)pg_malloc(ntups * sizeof(ProcLangInfo));

    i_tableoid = PQfnumber(res, "tableoid");
    i_oid = PQfnumber(res, "oid");
    i_lanname = PQfnumber(res, "lanname");
    i_lanpltrusted = PQfnumber(res, "lanpltrusted");
    i_lanplcallfoid = PQfnumber(res, "lanplcallfoid");
    /* these may fail and return -1: */
    i_laninline = PQfnumber(res, "laninline");
    i_lanvalidator = PQfnumber(res, "lanvalidator");
    i_lanacl = PQfnumber(res, "lanacl");
    i_lanowner = PQfnumber(res, "rolename");

    for (i = 0; i < ntups; i++) {
        planginfo[i].dobj.objType = DO_PROCLANG;
        planginfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        planginfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&planginfo[i].dobj);

        planginfo[i].dobj.name = gs_strdup(PQgetvalue(res, i, i_lanname));
        planginfo[i].lanpltrusted = *(PQgetvalue(res, i, i_lanpltrusted)) == 't';
        planginfo[i].lanplcallfoid = atooid(PQgetvalue(res, i, i_lanplcallfoid));
        if (i_laninline >= 0)
            planginfo[i].laninline = atooid(PQgetvalue(res, i, i_laninline));
        else
            planginfo[i].laninline = InvalidOid;
        if (i_lanvalidator >= 0)
            planginfo[i].lanvalidator = atooid(PQgetvalue(res, i, i_lanvalidator));
        else
            planginfo[i].lanvalidator = InvalidOid;
        if (i_lanacl >= 0)
            planginfo[i].lanacl = gs_strdup(PQgetvalue(res, i, i_lanacl));
        else
            planginfo[i].lanacl = gs_strdup("{=U}");
        if (i_lanowner >= 0)
            planginfo[i].rolename = gs_strdup(PQgetvalue(res, i, i_lanowner));
        else
            planginfo[i].rolename = gs_strdup("");

        if (fout->remoteVersion < 70300) {
            /*
             * We need to make a dependency to ensure the function will be
             * dumped first.  (In 7.3 and later the regular dependency
             * mechanism will handle this for us.)
             */
            FuncInfo* funcInfo = findFuncByOid(planginfo[i].lanplcallfoid);

            if (NULL != funcInfo)
                addObjectDependency(&planginfo[i].dobj, funcInfo->dobj.dumpId);
        }
    }

    PQclear(res);

    destroyPQExpBuffer(query);

    return planginfo;
}

/*
 * getCasts
 *	  get basic information about every cast in the system
 *
 * numCasts is set to the number of casts read in
 */
CastInfo* getCasts(Archive* fout, int* numCasts)
{
    PGresult* res = NULL;
    int ntups;
    int i;
    PQExpBuffer query = createPQExpBuffer();
    CastInfo* castinfo = NULL;
    int i_tableoid;
    int i_oid;
    int i_castsource;
    int i_casttarget;
    int i_castfunc;
    int i_castcontext;
    int i_castmethod;

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, "pg_catalog");

    if (fout->remoteVersion >= 80400) {
        appendPQExpBuffer(query,
            "SELECT tableoid, oid, "
            "castsource, casttarget, castfunc, castcontext, "
            "castmethod "
            "FROM pg_cast ORDER BY 3,4");
    } else if (fout->remoteVersion >= 70300) {
        appendPQExpBuffer(query,
            "SELECT tableoid, oid, "
            "castsource, casttarget, castfunc, castcontext, "
            "CASE WHEN castfunc = 0 THEN 'b' ELSE 'f' END AS castmethod "
            "FROM pg_cast ORDER BY 3,4");
    } else {
        appendPQExpBuffer(query,
            "SELECT 0 AS tableoid, p.oid, "
            "t1.oid AS castsource, t2.oid AS casttarget, "
            "p.oid AS castfunc, 'e' AS castcontext, "
            "'f' AS castmethod "
            "FROM pg_type t1, pg_type t2, pg_proc p "
            "WHERE p.pronargs = 1 AND "
            "p.proargtypes[0] = t1.oid AND "
            "p.prorettype = t2.oid AND p.proname = t2.typname "
            "ORDER BY 3,4");
    }

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    ntups = PQntuples(res);

    *numCasts = ntups;

    castinfo = (CastInfo*)pg_malloc(ntups * sizeof(CastInfo));

    i_tableoid = PQfnumber(res, "tableoid");
    i_oid = PQfnumber(res, "oid");
    i_castsource = PQfnumber(res, "castsource");
    i_casttarget = PQfnumber(res, "casttarget");
    i_castfunc = PQfnumber(res, "castfunc");
    i_castcontext = PQfnumber(res, "castcontext");
    i_castmethod = PQfnumber(res, "castmethod");

    for (i = 0; i < ntups; i++) {
        PQExpBufferData namebuf;
        TypeInfo* sTypeInfo = NULL;
        TypeInfo* tTypeInfo = NULL;

        castinfo[i].dobj.objType = DO_CAST;
        castinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        castinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&castinfo[i].dobj);
        castinfo[i].castsource = atooid(PQgetvalue(res, i, i_castsource));
        castinfo[i].casttarget = atooid(PQgetvalue(res, i, i_casttarget));
        castinfo[i].castfunc = atooid(PQgetvalue(res, i, i_castfunc));
        castinfo[i].castcontext = *(PQgetvalue(res, i, i_castcontext));
        castinfo[i].castmethod = *(PQgetvalue(res, i, i_castmethod));

        /*
         * Try to name cast as concatenation of typnames.  This is only used
         * for purposes of sorting.  If we fail to find either type, the name
         * will be an empty string.
         */
        initPQExpBuffer(&namebuf);
        sTypeInfo = findTypeByOid(castinfo[i].castsource);
        tTypeInfo = findTypeByOid(castinfo[i].casttarget);
        if (NULL != sTypeInfo && NULL != tTypeInfo)
            appendPQExpBuffer(&namebuf, "%s %s", sTypeInfo->dobj.name, tTypeInfo->dobj.name);
        castinfo[i].dobj.name = namebuf.data;

        if (OidIsValid(castinfo[i].castfunc)) {
            /*
             * We need to make a dependency to ensure the function will be
             * dumped first.  (In 7.3 and later the regular dependency
             * mechanism will handle this for us.)
             */
            FuncInfo* funcInfo = NULL;

            funcInfo = findFuncByOid(castinfo[i].castfunc);
            if (funcInfo != NULL)
                addObjectDependency(&castinfo[i].dobj, funcInfo->dobj.dumpId);
        }
    }

    PQclear(res);

    destroyPQExpBuffer(query);

    return castinfo;
}

/*
 * getTableAttrs -
 *	  for each interesting table, read info about its attributes
 *	  (names, types, default values, CHECK constraints, etc)
 *
 * This is implemented in a very inefficient way right now, looping
 * through the tblinfo and doing a join per table to find the attrs and their
 * types.  However, because we want type names and so forth to be named
 * relative to the schema of each table, we couldn't do it in just one
 * query.  (Maybe one query per schema?)
 *
 *	modifies tblinfo
 */
void getTableAttrs(Archive* fout, TableInfo* tblinfo, int numTables)
{
    int i = 0;
    int j = 0;
    PQExpBuffer q = createPQExpBuffer();
    PQExpBuffer ce_sql = createPQExpBuffer();
    int i_attnum = 0;
    int i_attname = 0;
    int i_atttypname = 0;
    int i_atttypmod = 0;
    int i_attstattarget = 0;
    int i_attstorage = 0;
    int i_typstorage = 0;
    int i_attnotnull = 0;
    int i_atthasdef = 0;
    int i_attisdropped = 0;
    int i_attlen = 0;
    int i_attalign = 0;
    int i_attislocal = 0;
    int i_attoptions = 0;
    int i_attcollation = 0;
    int i_attfdwoptions = 0;
    int i_typid = 0;
    int i_attkvtype = 0;

    int i_column_name = 0;
    int i_column_key_name = 0;
    int i_encryption_type = 0;
    int i_client_encryption_original_type = 0;

    PGresult *res = NULL;
    PGresult *ce_res = NULL;
    int ntups = 0;
    int ce_ntups = 0;
    bool hasdefaults = false;

    for (i = 0; i < numTables; i++) {
        TableInfo* tbinfo = &tblinfo[i];

        /* Don't bother to collect info for sequences */
        if (RELKIND_IS_SEQUENCE(tbinfo->relkind))
            continue;

        /* Don't bother with uninteresting tables, either */
        if (!tbinfo->interesting)
            continue;

        /*
         * Make sure we are in proper schema for this table; this allows
         * correct retrieval of formatted type names and default exprs
         */
        selectSourceSchema(fout, tbinfo->dobj.nmspace->dobj.name);

        /* find all the user attributes and their types */
        /*
         * we must read the attribute names in attribute number order! because
         * we will use the attnum to index into the attnames array later.  We
         * actually ask to order by "attrelid, attnum" because (at least up to
         * 7.3) the planner is not smart enough to realize it needn't re-sort
         * the output of an indexscan on pg_attribute_relid_attnum_index.
         */
        if (g_verbose)
            write_msg(NULL,
                "finding the columns and types of table \"%s\"\n",
                fmtQualifiedId(fout, tbinfo->dobj.nmspace->dobj.name, tbinfo->dobj.name));

        resetPQExpBuffer(q);
        resetPQExpBuffer(ce_sql);
        if (fout->remoteVersion >= 90200) {
            /*
             * attfdwoptions is new in 9.2.
             */
            appendPQExpBuffer(q,
                "SELECT a.attnum, a.attname, a.atttypmod, "
                "a.attstattarget, a.attstorage, t.typstorage, "
                "a.attnotnull, a.atthasdef, a.attisdropped, "
                "a.attlen, a.attalign, a.attislocal, a.attkvtype, t.oid AS typid, "
                "CASE ");
            if (TabExists(fout, "pg_catalog", "pg_set")) {
                appendPQExpBuffer(q,
                    "WHEN t.typtype = 's' THEN concat('set(', (select pg_catalog.string_agg(concat('''', setlabel, ''''), ',' order by setsortorder) from pg_catalog.pg_set group by settypid having settypid = t.oid), ')') ");

            }
            appendPQExpBuffer(q,
                "WHEN t.typtype = 'e' THEN concat('enum(', (select pg_catalog.string_agg(concat('''', enumlabel, ''''), ',' order by enumsortorder) from pg_catalog.pg_enum group by enumtypid having enumtypid = t.oid), ')') ELSE pg_catalog.format_type(t.oid,a.atttypmod) END "
                "AS atttypname, "
                "pg_catalog.array_to_string(a.attoptions, ', ') AS attoptions, "
                "CASE WHEN a.attcollation <> t.typcollation "
                "THEN a.attcollation ELSE 0::Oid END AS attcollation, "
                "pg_catalog.array_to_string(ARRAY("
                "SELECT pg_catalog.quote_ident(option_name) || "
                "' ' || pg_catalog.quote_literal(option_value) "
                "FROM pg_catalog.pg_options_to_table(attfdwoptions) "
                "ORDER BY option_name"
                "), E',\n    ') AS attfdwoptions "
                "FROM pg_catalog.pg_attribute a LEFT JOIN pg_catalog.pg_type t "
                "ON a.atttypid = t.oid "
                "WHERE a.attrelid = '%u'::pg_catalog.oid "
                "AND a.attnum > 0::pg_catalog.int2 "
                "ORDER BY a.attrelid, a.attnum",
                tbinfo->dobj.catId.oid);
            
            appendPQExpBuffer(ce_sql,
                "SELECT b.column_name, a.column_key_name, b.encryption_type, "
                "pg_catalog.format_type(c.atttypmod, b.data_type_original_mod) AS client_encryption_original_type from "
                "gs_encrypted_columns b inner join pg_attribute c on b.column_name = c.attname and "
                "c.attrelid = b.rel_id and b.rel_id = '%u' inner join gs_column_keys a on a.oid = b.column_key_id",
                tbinfo->dobj.catId.oid);
        } else if (fout->remoteVersion >= 90100) {
            /*
             * attcollation is new in 9.1.	Since we only want to dump COLLATE
             * clauses for attributes whose collation is different from their
             * type's default, we use a CASE here to suppress uninteresting
             * attcollations cheaply.
             */
            appendPQExpBuffer(q,
                "SELECT a.attnum, a.attname, a.atttypmod, "
                "a.attstattarget, a.attstorage, t.typstorage, "
                "a.attnotnull, a.atthasdef, a.attisdropped, "
                "a.attlen, a.attalign, a.attislocal, "
                "pg_catalog.format_type(t.oid,a.atttypmod) AS atttypname, "
                "pg_catalog.array_to_string(a.attoptions, ', ') AS attoptions, "
                "CASE WHEN a.attcollation <> t.typcollation "
                "THEN a.attcollation ELSE 0::Oid END AS attcollation, "
                "NULL AS attfdwoptions "
                "FROM pg_catalog.pg_attribute a LEFT JOIN pg_catalog.pg_type t "
                "ON a.atttypid = t.oid "
                "WHERE a.attrelid = '%u'::pg_catalog.oid "
                "AND a.attnum > 0::pg_catalog.int2 "
                "ORDER BY a.attrelid, a.attnum",
                tbinfo->dobj.catId.oid);
        } else if (fout->remoteVersion >= 90000) {
            /* attoptions is new in 9.0 */
            appendPQExpBuffer(q,
                "SELECT a.attnum, a.attname, a.atttypmod, "
                "a.attstattarget, a.attstorage, t.typstorage, "
                "a.attnotnull, a.atthasdef, a.attisdropped, "
                "a.attlen, a.attalign, a.attislocal, "
                "pg_catalog.format_type(t.oid,a.atttypmod) AS atttypname, "
                "pg_catalog.array_to_string(a.attoptions, ', ') AS attoptions, "
                "0 AS attcollation, "
                "NULL AS attfdwoptions "
                "FROM pg_catalog.pg_attribute a LEFT JOIN pg_catalog.pg_type t "
                "ON a.atttypid = t.oid "
                "WHERE a.attrelid = '%u'::pg_catalog.oid "
                "AND a.attnum > 0::pg_catalog.int2 "
                "ORDER BY a.attrelid, a.attnum",
                tbinfo->dobj.catId.oid);
        } else if (fout->remoteVersion >= 70300) {
            /* need left join here to not fail on dropped columns ... */
            appendPQExpBuffer(q,
                "SELECT a.attnum, a.attname, a.atttypmod, "
                "a.attstattarget, a.attstorage, t.typstorage, "
                "a.attnotnull, a.atthasdef, a.attisdropped, "
                "a.attlen, a.attalign, a.attislocal, "
                "pg_catalog.format_type(t.oid,a.atttypmod) AS atttypname, "
                "'' AS attoptions, 0 AS attcollation, "
                "NULL AS attfdwoptions "
                "FROM pg_catalog.pg_attribute a LEFT JOIN pg_catalog.pg_type t "
                "ON a.atttypid = t.oid "
                "WHERE a.attrelid = '%u'::pg_catalog.oid "
                "AND a.attnum > 0::pg_catalog.int2 "
                "ORDER BY a.attrelid, a.attnum",
                tbinfo->dobj.catId.oid);
        } else if (fout->remoteVersion >= 70100) {
            /*
             * attstattarget doesn't exist in 7.1.  It does exist in 7.2, but
             * we don't dump it because we can't tell whether it's been
             * explicitly set or was just a default.
             *
             * attislocal doesn't exist before 7.3, either; in older databases
             * we assume it's TRUE, else we'd fail to dump non-inherited atts.
             */
            appendPQExpBuffer(q,
                "SELECT a.attnum, a.attname, a.atttypmod, "
                "-1 AS attstattarget, a.attstorage, "
                "t.typstorage, a.attnotnull, a.atthasdef, "
                "false AS attisdropped, a.attlen, "
                "a.attalign, true AS attislocal, "
                "pg_catalog.format_type(t.oid,a.atttypmod) AS atttypname, "
                "'' AS attoptions, 0 AS attcollation, "
                "NULL AS attfdwoptions "
                "FROM pg_attribute a LEFT JOIN pg_type t "
                "ON a.atttypid = t.oid "
                "WHERE a.attrelid = '%u'::oid "
                "AND a.attnum > 0::int2 "
                "ORDER BY a.attrelid, a.attnum",
                tbinfo->dobj.catId.oid);
        } else {
            /* format_type not available before 7.1 */
            appendPQExpBuffer(q,
                "SELECT attnum, attname, atttypmod, "
                "-1 AS attstattarget, "
                "attstorage, attstorage AS typstorage, "
                "attnotnull, atthasdef, false AS attisdropped, "
                "attlen, attalign, "
                "true AS attislocal, "
                "(SELECT typname FROM pg_type WHERE oid = atttypid) AS atttypname, "
                "'' AS attoptions, 0 AS attcollation, "
                "NULL AS attfdwoptions "
                "FROM pg_attribute a "
                "WHERE attrelid = '%u'::oid "
                "AND attnum > 0::int2 "
                "ORDER BY attrelid, attnum",
                tbinfo->dobj.catId.oid);
        }

        res = ExecuteSqlQuery(fout, q->data, PGRES_TUPLES_OK);
        ce_res = ExecuteSqlQuery(fout, ce_sql->data, PGRES_TUPLES_OK);
        ntups = PQntuples(res);
        ce_ntups = PQntuples(ce_res);

        i_attnum = PQfnumber(res, "attnum");
        i_attname = PQfnumber(res, "attname");
        i_atttypname = PQfnumber(res, "atttypname");
        i_typid = PQfnumber(res, "typid");
        i_atttypmod = PQfnumber(res, "atttypmod");
        i_attstattarget = PQfnumber(res, "attstattarget");
        i_attstorage = PQfnumber(res, "attstorage");
        i_typstorage = PQfnumber(res, "typstorage");
        i_attnotnull = PQfnumber(res, "attnotnull");
        i_atthasdef = PQfnumber(res, "atthasdef");
        i_attisdropped = PQfnumber(res, "attisdropped");
        i_attlen = PQfnumber(res, "attlen");
        i_attalign = PQfnumber(res, "attalign");
        i_attislocal = PQfnumber(res, "attislocal");
        i_attoptions = PQfnumber(res, "attoptions");
        i_attcollation = PQfnumber(res, "attcollation");
        i_attfdwoptions = PQfnumber(res, "attfdwoptions");
        i_attkvtype = PQfnumber(res, "attkvtype");

        i_column_name =  PQfnumber(ce_res, "column_name");
        i_column_key_name =  PQfnumber(ce_res, "column_key_name");
        i_encryption_type = PQfnumber(ce_res, "encryption_type");
        i_client_encryption_original_type = PQfnumber(ce_res, "client_encryption_original_type");

        tbinfo->numatts = ntups;
        tbinfo->attnames = (char**)pg_malloc(ntups * sizeof(char*));
        tbinfo->atttypnames = (char**)pg_malloc(ntups * sizeof(char*));

        tbinfo->column_key_names = (char**)pg_malloc(ntups * sizeof(char*));
        tbinfo->encryption_type = (int*)pg_malloc(ntups * sizeof(int));

        tbinfo->typid = (int*)pg_malloc(ntups * sizeof(int));
        tbinfo->atttypmod = (int*)pg_malloc(ntups * sizeof(int));
        tbinfo->attstattarget = (int*)pg_malloc(ntups * sizeof(int));
        tbinfo->attstorage = (char*)pg_malloc(ntups * sizeof(char));
        tbinfo->typstorage = (char*)pg_malloc(ntups * sizeof(char));
        tbinfo->attisdropped = (bool*)pg_malloc(ntups * sizeof(bool));
        tbinfo->attisblockchainhash = (bool*)pg_malloc(ntups * sizeof(bool));
        tbinfo->attlen = (int*)pg_malloc(ntups * sizeof(int));
        tbinfo->attalign = (char*)pg_malloc(ntups * sizeof(char));
        tbinfo->attislocal = (bool*)pg_malloc(ntups * sizeof(bool));
        tbinfo->attoptions = (char**)pg_malloc(ntups * sizeof(char*));
        tbinfo->attcollation = (Oid*)pg_malloc(ntups * sizeof(Oid));
        tbinfo->attfdwoptions = (char**)pg_malloc(ntups * sizeof(char*));
        tbinfo->notnull = (bool*)pg_malloc(ntups * sizeof(bool));
        tbinfo->inhNotNull = (bool*)pg_malloc(ntups * sizeof(bool));
        tbinfo->attrdefs = (AttrDefInfo**)pg_malloc(ntups * sizeof(AttrDefInfo*));
        tbinfo->attkvtype = (int*)pg_malloc(ntups * sizeof(int));
        hasdefaults = false;

        for (j = 0; j < ntups; j++) {
            if (j + 1 != atoi(PQgetvalue(res, j, i_attnum)))
                exit_horribly(NULL, "invalid column numbering in table \"%s\"\n", tbinfo->dobj.name);
            tbinfo->attnames[j] = gs_strdup(PQgetvalue(res, j, i_attname));
            tbinfo->atttypnames[j] = gs_strdup(PQgetvalue(res, j, i_atttypname));
            tbinfo->column_key_names[j]  = NULL;
            tbinfo->encryption_type[j] = 0;
            for (int k = 0; k < ce_ntups; k++) {
                char *temp_column_name = PQgetvalue(ce_res, k, i_column_name);
                if (temp_column_name != NULL && strcmp(temp_column_name, tbinfo->attnames[j]) == 0) {
                    tbinfo->column_key_names[j] = gs_strdup(PQgetvalue(ce_res, k, i_column_key_name));
                    tbinfo->encryption_type[j] = atoi(PQgetvalue(ce_res, k, i_encryption_type));
                    if (tbinfo->atttypnames[j] != NULL) {
                        free(tbinfo->atttypnames[j]);
                        tbinfo->atttypnames[j] = NULL;
                    }
                    tbinfo->atttypnames[j] = gs_strdup(PQgetvalue(ce_res, k, i_client_encryption_original_type));
                    break;
                }
            }

            tbinfo->typid[j] = atoi(PQgetvalue(res, j, i_typid));
            tbinfo->atttypmod[j] = atoi(PQgetvalue(res, j, i_atttypmod));
            tbinfo->attstattarget[j] = atoi(PQgetvalue(res, j, i_attstattarget));
            tbinfo->attstorage[j] = *(PQgetvalue(res, j, i_attstorage));
            tbinfo->typstorage[j] = *(PQgetvalue(res, j, i_typstorage));
            tbinfo->attisdropped[j] = (PQgetvalue(res, j, i_attisdropped)[0] == 't');
            tbinfo->attisblockchainhash[j] = tbinfo->isblockchain ? (strcmp(tbinfo->attnames[j], "hash") == 0) : false;
            tbinfo->attlen[j] = atoi(PQgetvalue(res, j, i_attlen));
            tbinfo->attalign[j] = *(PQgetvalue(res, j, i_attalign));
            tbinfo->attislocal[j] = (PQgetvalue(res, j, i_attislocal)[0] == 't');
            tbinfo->notnull[j] = (PQgetvalue(res, j, i_attnotnull)[0] == 't');
            tbinfo->attoptions[j] = gs_strdup(PQgetvalue(res, j, i_attoptions));
            tbinfo->attcollation[j] = atooid(PQgetvalue(res, j, i_attcollation));
            tbinfo->attfdwoptions[j] = gs_strdup(PQgetvalue(res, j, i_attfdwoptions));
            tbinfo->attkvtype[j] = atoi(PQgetvalue(res, j, i_attkvtype));
            tbinfo->attrdefs[j] = NULL; /* fix below */
            if (PQgetvalue(res, j, i_atthasdef)[0] == 't')
                hasdefaults = true;
            /* these flags will be set in flagInhAttrs() */
            tbinfo->inhNotNull[j] = false;
        }

        PQclear(res);
        PQclear(ce_res);

        /*
         * Get info about column defaults
         */
        if (hasdefaults) {
            AttrDefInfo* attrdefs = NULL;
            int numDefaults;
            ArchiveHandle* AH = (ArchiveHandle*)fout;
            bool hasGenColFeature = is_column_exists(AH->connection, AttrDefaultRelationId, "adgencol");
            bool hasOnUpdateFeature = is_column_exists(AH->connection, AttrDefaultRelationId, "adbin_on_update");

            if (g_verbose)
                write_msg(NULL,
                    "finding default expressions of table \"%s\"\n",
                    fmtQualifiedId(fout, tbinfo->dobj.nmspace->dobj.name, tbinfo->dobj.name));

            resetPQExpBuffer(q);
            if (hasGenColFeature && !hasOnUpdateFeature) {
                appendPQExpBuffer(q,
                    "SELECT tableoid, oid, adnum, "
                    "pg_catalog.pg_get_expr(adbin, adrelid) AS adsrc "
                    ", adgencol AS generatedCol "
                    "FROM pg_catalog.pg_attrdef "
                    "WHERE adrelid = '%u'::pg_catalog.oid",
                    tbinfo->dobj.catId.oid);
            } else if (hasGenColFeature && hasOnUpdateFeature) {
                appendPQExpBuffer(q,
                    "SELECT tableoid, oid, adnum, "
                    "pg_catalog.pg_get_expr(adbin, adrelid) AS adsrc "
                    ", adgencol AS generatedCol, "
                    "pg_catalog.pg_get_expr(adbin_on_update, adrelid) AS adsrc_on_update "
                    "FROM pg_catalog.pg_attrdef "
                    "WHERE adrelid = '%u'::pg_catalog.oid",
                    tbinfo->dobj.catId.oid);
            } else if (fout->remoteVersion >= 70300) {
                appendPQExpBuffer(q,
                    "SELECT tableoid, oid, adnum, "
                    "pg_catalog.pg_get_expr(adbin, adrelid) AS adsrc "
                    ", '' AS generatedCol "
                    "FROM pg_catalog.pg_attrdef "
                    "WHERE adrelid = '%u'::pg_catalog.oid",
                    tbinfo->dobj.catId.oid);
            } else if (fout->remoteVersion >= 70200) {
                /* 7.2 did not have OIDs in pg_attrdef */
                appendPQExpBuffer(q,
                    "SELECT tableoid, 0 AS oid, adnum, "
                    "pg_catalog.pg_get_expr(adbin, adrelid) AS adsrc "
                    ", '' AS generatedCol "
                    "FROM pg_attrdef "
                    "WHERE adrelid = '%u'::oid",
                    tbinfo->dobj.catId.oid);
            } else if (fout->remoteVersion >= 70100) {
                /* no pg_get_expr, so must rely on adsrc */
                appendPQExpBuffer(q,
                    "SELECT tableoid, oid, adnum, adsrc "
                    ", '' AS generatedCol "
                    "FROM pg_attrdef "
                    "WHERE adrelid = '%u'::oid",
                    tbinfo->dobj.catId.oid);
            } else {
                /* no pg_get_expr, no tableoid either */
                appendPQExpBuffer(q,
                    "SELECT "
                    "(SELECT oid FROM pg_class WHERE relname = 'pg_attrdef') AS tableoid, "
                    "oid, adnum, adsrc "
                    ", '' AS generatedCol "
                    "FROM pg_attrdef "
                    "WHERE adrelid = '%u'::oid",
                    tbinfo->dobj.catId.oid);
            }
            res = ExecuteSqlQuery(fout, q->data, PGRES_TUPLES_OK);

            numDefaults = PQntuples(res);
            attrdefs = (AttrDefInfo*)pg_malloc(numDefaults * sizeof(AttrDefInfo));

            /* We should dump AttrDef before Matview, so we get matview oid here first */
            Oid tableoid = tbinfo->dobj.catId.oid;
            int nMatviews = 0;
            Oid *matviewoid = GetMatviewOid(fout, tableoid, &nMatviews);

            for (j = 0; j < numDefaults; j++) {
                int adnum;

                adnum = atoi(PQgetvalue(res, j, 2));

                if (adnum <= 0 || adnum > ntups)
                    exit_horribly(NULL, "invalid adnum value %d for table \"%s\"\n", adnum, tbinfo->dobj.name);

                /*
                 * dropped columns shouldn't have defaults, but just in case,
                 * ignore 'em
                 */
                if (tbinfo->attisdropped[adnum - 1])
                    continue;

                attrdefs[j].dobj.objType = DO_ATTRDEF;
                attrdefs[j].dobj.catId.tableoid = atooid(PQgetvalue(res, j, 0));
                attrdefs[j].dobj.catId.oid = atooid(PQgetvalue(res, j, 1));
                AssignDumpId(&attrdefs[j].dobj);
                attrdefs[j].adtable = tbinfo;
                attrdefs[j].adnum = adnum;
                attrdefs[j].generatedCol = *(PQgetvalue(res, j, 4));
                attrdefs[j].adef_expr = gs_strdup(PQgetvalue(res, j, 3));
                if (tbinfo->autoinc_attnum == 0) {
                    tbinfo->autoinc_attnum = (strcmp(attrdefs[j].adef_expr, "AUTO_INCREMENT") == 0) ? adnum : 0;
                }
                if (hasOnUpdateFeature) {
                    attrdefs[j].adupd_expr = gs_strdup(PQgetvalue(res, j, 5));
                }
                attrdefs[j].dobj.name = gs_strdup(tbinfo->dobj.name);
                attrdefs[j].dobj.nmspace = tbinfo->dobj.nmspace;

                attrdefs[j].dobj.dump = tbinfo->dobj.dump;

                /* After dump matview, we can not change the basetable column definition, so if we dump attrdef,
                 * we add a dependency to the matview, if the basetable has some */
                if (nMatviews > 0) {
                    for (int k = 0; k < nMatviews; k++) {
                        addObjectDependency((DumpableObject *)findTableByOid(matviewoid[k]), attrdefs[j].dobj.dumpId);
                    }
                }

                /*
                 * Defaults on a VIEW must always be dumped as separate ALTER
                 * TABLE commands.	Defaults on regular tables are dumped as
                 * part of the CREATE TABLE if possible, which it won't be if
                 * the column is not going to be emitted explicitly.
                 */
                if (tbinfo->relkind == RELKIND_VIEW || tbinfo->relkind == RELKIND_MATVIEW ||
                    tbinfo->relkind == RELKIND_CONTQUERY) {
                    attrdefs[j].separate = true;
                    /* needed in case pre-7.3 DB: */
                    addObjectDependency(&attrdefs[j].dobj, tbinfo->dobj.dumpId);
                } else if (!shouldPrintColumn(tbinfo, adnum - 1)) {
                    /* column will be suppressed, print default separately */
                    attrdefs[j].separate = true;
                    /* needed in case pre-7.3 DB: */
                    addObjectDependency(&attrdefs[j].dobj, tbinfo->dobj.dumpId);
                } else {
                    attrdefs[j].separate = false;

                    /*
                     * Mark the default as needing to appear before the table,
                     * so that any dependencies it has must be emitted before
                     * the CREATE TABLE.  If this is not possible, we'll
                     * change to "separate" mode while sorting dependencies.
                     */
                    if (tbinfo->autoinc_attnum <= 0) {
                        addObjectDependency(&tbinfo->dobj, attrdefs[j].dobj.dumpId);
                    }
                }

                tbinfo->attrdefs[adnum - 1] = &attrdefs[j];
            }

            FREE_PTR(matviewoid);
            PQclear(res);
        }

        /*
         * Get info about table CHECK constraints
         */
        if (tbinfo->ncheck > 0) {
            ConstraintInfo* constrs = NULL;
            int numConstrs;

            if (g_verbose)
                write_msg(NULL,
                    "finding check constraints for table \"%s\"\n",
                    fmtQualifiedId(fout, tbinfo->dobj.nmspace->dobj.name, tbinfo->dobj.name));

            resetPQExpBuffer(q);
            if (fout->remoteVersion >= 90200) {
                /*
                 * convalidated is new in 9.2 (actually, it is there in 9.1,
                 * but it wasn't ever false for check constraints until 9.2).
                 */
                appendPQExpBuffer(q,
                    "SELECT tableoid, oid, conname, "
                    "pg_catalog.pg_get_constraintdef(oid) AS consrc, "
                    "conislocal, convalidated "
                    "FROM pg_catalog.pg_constraint "
                    "WHERE conrelid = '%u'::pg_catalog.oid "
                    "   AND contype = 'c' "
                    "ORDER BY conname",
                    tbinfo->dobj.catId.oid);
            } else if (fout->remoteVersion >= 80400) {
                /* conislocal is new in 8.4 */
                appendPQExpBuffer(q,
                    "SELECT tableoid, oid, conname, "
                    "pg_catalog.pg_get_constraintdef(oid) AS consrc, "
                    "conislocal, true AS convalidated "
                    "FROM pg_catalog.pg_constraint "
                    "WHERE conrelid = '%u'::pg_catalog.oid "
                    "   AND contype = 'c' "
                    "ORDER BY conname",
                    tbinfo->dobj.catId.oid);
            } else if (fout->remoteVersion >= 70400) {
                appendPQExpBuffer(q,
                    "SELECT tableoid, oid, conname, "
                    "pg_catalog.pg_get_constraintdef(oid) AS consrc, "
                    "true AS conislocal, true AS convalidated "
                    "FROM pg_catalog.pg_constraint "
                    "WHERE conrelid = '%u'::pg_catalog.oid "
                    "   AND contype = 'c' "
                    "ORDER BY conname",
                    tbinfo->dobj.catId.oid);
            } else if (fout->remoteVersion >= 70300) {
                /* no pg_get_constraintdef, must use consrc */
                appendPQExpBuffer(q,
                    "SELECT tableoid, oid, conname, "
                    "'CHECK (' || consrc || ')' AS consrc, "
                    "true AS conislocal, true AS convalidated "
                    "FROM pg_catalog.pg_constraint "
                    "WHERE conrelid = '%u'::pg_catalog.oid "
                    "   AND contype = 'c' "
                    "ORDER BY conname",
                    tbinfo->dobj.catId.oid);
            } else if (fout->remoteVersion >= 70200) {
                /* 7.2 did not have OIDs in pg_relcheck */
                appendPQExpBuffer(q,
                    "SELECT tableoid, 0 AS oid, "
                    "rcname AS conname, "
                    "'CHECK (' || rcsrc || ')' AS consrc, "
                    "true AS conislocal, true AS convalidated "
                    "FROM pg_relcheck "
                    "WHERE rcrelid = '%u'::oid "
                    "ORDER BY rcname",
                    tbinfo->dobj.catId.oid);
            } else if (fout->remoteVersion >= 70100) {
                appendPQExpBuffer(q,
                    "SELECT tableoid, oid, "
                    "rcname AS conname, "
                    "'CHECK (' || rcsrc || ')' AS consrc, "
                    "true AS conislocal, true AS convalidated "
                    "FROM pg_relcheck "
                    "WHERE rcrelid = '%u'::oid "
                    "ORDER BY rcname",
                    tbinfo->dobj.catId.oid);
            } else {
                /* no tableoid in 7.0 */
                appendPQExpBuffer(q,
                    "SELECT "
                    "(SELECT oid FROM pg_class WHERE relname = 'pg_relcheck') AS tableoid, "
                    "oid, rcname AS conname, "
                    "'CHECK (' || rcsrc || ')' AS consrc, "
                    "true AS conislocal, true AS convalidated "
                    "FROM pg_relcheck "
                    "WHERE rcrelid = '%u'::oid "
                    "ORDER BY rcname",
                    tbinfo->dobj.catId.oid);
            }
            res = ExecuteSqlQuery(fout, q->data, PGRES_TUPLES_OK);

            numConstrs = PQntuples(res);
            if (numConstrs != tbinfo->ncheck) {
                write_msg(NULL,
                    ngettext("expected %d check constraint on table \"%s\" but found %d\n",
                        "expected %d check constraints on table \"%s\" but found %d\n",
                        tbinfo->ncheck),
                    tbinfo->ncheck,
                    tbinfo->dobj.name,
                    numConstrs);
                write_msg(NULL, "(The system catalogs might be corrupted.)\n");
                exit_nicely(1);
            }

            constrs = (ConstraintInfo*)pg_malloc(numConstrs * sizeof(ConstraintInfo));
            tbinfo->checkexprs = constrs;

            for (j = 0; j < numConstrs; j++) {
                bool validated = PQgetvalue(res, j, 5)[0] == 't';

                constrs[j].dobj.objType = DO_CONSTRAINT;
                constrs[j].dobj.catId.tableoid = atooid(PQgetvalue(res, j, 0));
                constrs[j].dobj.catId.oid = atooid(PQgetvalue(res, j, 1));
                AssignDumpId(&constrs[j].dobj);
                constrs[j].dobj.name = gs_strdup(PQgetvalue(res, j, 2));
                constrs[j].dobj.nmspace = tbinfo->dobj.nmspace;
                constrs[j].contable = tbinfo;
                constrs[j].condomain = NULL;
                constrs[j].contype = 'c';
                constrs[j].condef = gs_strdup(PQgetvalue(res, j, 3));
                constrs[j].confrelid = InvalidOid;
                constrs[j].conindex = 0;
                constrs[j].condeferrable = false;
                constrs[j].condeferred = false;
                constrs[j].conislocal = (PQgetvalue(res, j, 4)[0] == 't');

                /*
                 * An unvalidated constraint needs to be dumped separately, so
                 * that potentially-violating existing data is loaded before
                 * the constraint.
                 */
                constrs[j].separate = !validated;

                constrs[j].dobj.dump = tbinfo->dobj.dump;

                /*
                 * Mark the constraint as needing to appear before the table
                 * --- this is so that any other dependencies of the
                 * constraint will be emitted before we try to create the
                 * table.  If the constraint is to be dumped separately, it
                 * will be dumped after data is loaded anyway, so don't do it.
                 * (There's an automatic dependency in the opposite direction
                 * anyway, so don't need to add one manually here.)
                 */
                if (!constrs[j].separate)
                    addObjectDependency(&tbinfo->dobj, constrs[j].dobj.dumpId);

                /*
                 * If the constraint is inherited, this will be detected later
                 * (in pre-8.4 databases).	We also detect later if the
                 * constraint must be split out from the table definition.
                 */
            }
            PQclear(res);
        }
    }

    destroyPQExpBuffer(q);
    destroyPQExpBuffer(ce_sql);
}

/*
 * Test whether a column should be printed as part of table's CREATE TABLE.
 * Column number is zero-based.
 *
 * Normally this is always true, but it's false for dropped columns, as well
 * as those that were inherited without any local definition.  (If we print
 * such a column it will mistakenly get pg_attribute.attislocal set to true.)
 * However, in binary_upgrade mode, we must print all such columns anyway and
 * fix the attislocal/attisdropped state later, so as to keep control of the
 * physical column order.
 *
 * This function exists because there are scattered nonobvious places that
 * must be kept in sync with this decision.
 */
bool shouldPrintColumn(TableInfo* tbinfo, int colno)
{
    if (binary_upgrade || include_alter_table)
        return true;
    /* this means current column is hidden in the tstable */
    if (isTsStoreTable(tbinfo) && tbinfo->attkvtype[colno] == 4)
        return false;
    bool attislocal = tbinfo->attislocal[colno];
    bool attisdropped = tbinfo->attisdropped[colno];
    bool attisblockchainhash = tbinfo->attisblockchainhash[colno];
    return (attislocal && !attisdropped && !attisblockchainhash);
}

/*
 * getTSParsers:
 *	  read all text search parsers in the system catalogs and return them
 *	  in the TSParserInfo* structure
 *
 *	numTSParsers is set to the number of parsers read in
 */
TSParserInfo* getTSParsers(Archive* fout, int* numTSParsers)
{
    PGresult* res = NULL;
    int ntups;
    int i;
    PQExpBuffer query;
    TSParserInfo* prsinfo = NULL;
    int i_tableoid;
    int i_oid;
    int i_prsname;
    int i_prsnamespace;
    int i_prsstart;
    int i_prstoken;
    int i_prsend;
    int i_prsheadline;
    int i_prslextype;

    /* Before 8.3, there is no built-in text search support */
    if (fout->remoteVersion < 80300) {
        *numTSParsers = 0;
        return NULL;
    }

    query = createPQExpBuffer();

    /*
     * find all text search objects, including builtin ones; we filter out
     * system-defined objects at dump-out time.
     */

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, "pg_catalog");

    appendPQExpBuffer(query,
        "SELECT tableoid, oid, prsname, prsnamespace, "
        "prsstart::oid, prstoken::oid, "
        "prsend::oid, prsheadline::oid, prslextype::oid "
        "FROM pg_ts_parser");

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    ntups = PQntuples(res);
    *numTSParsers = ntups;

    prsinfo = (TSParserInfo*)pg_malloc(ntups * sizeof(TSParserInfo));

    i_tableoid = PQfnumber(res, "tableoid");
    i_oid = PQfnumber(res, "oid");
    i_prsname = PQfnumber(res, "prsname");
    i_prsnamespace = PQfnumber(res, "prsnamespace");
    i_prsstart = PQfnumber(res, "prsstart");
    i_prstoken = PQfnumber(res, "prstoken");
    i_prsend = PQfnumber(res, "prsend");
    i_prsheadline = PQfnumber(res, "prsheadline");
    i_prslextype = PQfnumber(res, "prslextype");

    for (i = 0; i < ntups; i++) {
        prsinfo[i].dobj.objType = DO_TSPARSER;
        prsinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        prsinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&prsinfo[i].dobj);
        prsinfo[i].dobj.name = gs_strdup(PQgetvalue(res, i, i_prsname));
        prsinfo[i].dobj.nmspace =
            findNamespace(fout, atooid(PQgetvalue(res, i, i_prsnamespace)), prsinfo[i].dobj.catId.oid);
        prsinfo[i].prsstart = atooid(PQgetvalue(res, i, i_prsstart));
        prsinfo[i].prstoken = atooid(PQgetvalue(res, i, i_prstoken));
        prsinfo[i].prsend = atooid(PQgetvalue(res, i, i_prsend));
        prsinfo[i].prsheadline = atooid(PQgetvalue(res, i, i_prsheadline));
        prsinfo[i].prslextype = atooid(PQgetvalue(res, i, i_prslextype));

        /* Decide whether we want to dump it */
        selectDumpableObject(&(prsinfo[i].dobj), fout);
    }

    PQclear(res);

    destroyPQExpBuffer(query);

    return prsinfo;
}

/*
 * getTSDictionaries:
 *	  read all text search dictionaries in the system catalogs and return them
 *	  in the TSDictInfo* structure
 *
 *	numTSDicts is set to the number of dictionaries read in
 */
TSDictInfo* getTSDictionaries(Archive* fout, int* numTSDicts)
{
    PGresult* res = NULL;
    int ntups = 0;
    int i = 0;
    PQExpBuffer query;
    TSDictInfo* dictinfo = NULL;
    int i_tableoid = 0;
    int i_oid = 0;
    int i_dictname = 0;
    int i_dictnamespace = 0;
    int i_rolname = 0;
    int i_dicttemplate = 0;
    int i_dictinitoption = 0;

    /* Before 8.3, there is no built-in text search support */
    if (fout->remoteVersion < 80300) {
        *numTSDicts = 0;
        return NULL;
    }

    query = createPQExpBuffer();

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, "pg_catalog");

    appendPQExpBuffer(query,
        "SELECT tableoid, oid, dictname, "
        "dictnamespace, (%s dictowner) AS rolname, "
        "dicttemplate, dictinitoption "
        "FROM pg_ts_dict",
        username_subquery);

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    ntups = PQntuples(res);
    *numTSDicts = ntups;

    dictinfo = (TSDictInfo*)pg_malloc(ntups * sizeof(TSDictInfo));

    i_tableoid = PQfnumber(res, "tableoid");
    i_oid = PQfnumber(res, "oid");
    i_dictname = PQfnumber(res, "dictname");
    i_dictnamespace = PQfnumber(res, "dictnamespace");
    i_rolname = PQfnumber(res, "rolname");
    i_dictinitoption = PQfnumber(res, "dictinitoption");
    i_dicttemplate = PQfnumber(res, "dicttemplate");

    for (i = 0; i < ntups; i++) {
        dictinfo[i].dobj.objType = DO_TSDICT;
        dictinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        dictinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&dictinfo[i].dobj);
        dictinfo[i].dobj.name = gs_strdup(PQgetvalue(res, i, i_dictname));
        dictinfo[i].dobj.nmspace =
            findNamespace(fout, atooid(PQgetvalue(res, i, i_dictnamespace)), dictinfo[i].dobj.catId.oid);
        dictinfo[i].rolname = gs_strdup(PQgetvalue(res, i, i_rolname));
        dictinfo[i].dicttemplate = atooid(PQgetvalue(res, i, i_dicttemplate));
        if (PQgetisnull(res, i, i_dictinitoption))
            dictinfo[i].dictinitoption = NULL;
        else
            dictinfo[i].dictinitoption = gs_strdup(PQgetvalue(res, i, i_dictinitoption));

        /* Decide whether we want to dump it */
        selectDumpableObject(&(dictinfo[i].dobj), fout);
    }

    PQclear(res);

    destroyPQExpBuffer(query);

    return dictinfo;
}

/*
 * getTSTemplates:
 *	  read all text search templates in the system catalogs and return them
 *	  in the TSTemplateInfo* structure
 *
 *	numTSTemplates is set to the number of templates read in
 */
TSTemplateInfo* getTSTemplates(Archive* fout, int* numTSTemplates)
{
    PGresult* res = NULL;
    int ntups = 0;
    int i = 0;
    PQExpBuffer query;
    TSTemplateInfo* tmplinfo = NULL;
    int i_tableoid = 0;
    int i_oid = 0;
    int i_tmplname = 0;
    int i_tmplnamespace = 0;
    int i_tmplinit = 0;
    int i_tmpllexize = 0;

    /* Before 8.3, there is no built-in text search support */
    if (fout->remoteVersion < 80300) {
        *numTSTemplates = 0;
        return NULL;
    }

    query = createPQExpBuffer();

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, "pg_catalog");

    appendPQExpBuffer(query,
        "SELECT tableoid, oid, tmplname, "
        "tmplnamespace, tmplinit::oid, tmpllexize::oid "
        "FROM pg_ts_template");

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    ntups = PQntuples(res);
    *numTSTemplates = ntups;

    tmplinfo = (TSTemplateInfo*)pg_malloc(ntups * sizeof(TSTemplateInfo));

    i_tableoid = PQfnumber(res, "tableoid");
    i_oid = PQfnumber(res, "oid");
    i_tmplname = PQfnumber(res, "tmplname");
    i_tmplnamespace = PQfnumber(res, "tmplnamespace");
    i_tmplinit = PQfnumber(res, "tmplinit");
    i_tmpllexize = PQfnumber(res, "tmpllexize");

    for (i = 0; i < ntups; i++) {
        tmplinfo[i].dobj.objType = DO_TSTEMPLATE;
        tmplinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        tmplinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&tmplinfo[i].dobj);
        tmplinfo[i].dobj.name = gs_strdup(PQgetvalue(res, i, i_tmplname));
        tmplinfo[i].dobj.nmspace =
            findNamespace(fout, atooid(PQgetvalue(res, i, i_tmplnamespace)), tmplinfo[i].dobj.catId.oid);
        tmplinfo[i].tmplinit = atooid(PQgetvalue(res, i, i_tmplinit));
        tmplinfo[i].tmpllexize = atooid(PQgetvalue(res, i, i_tmpllexize));

        /* Decide whether we want to dump it */
        selectDumpableObject(&(tmplinfo[i].dobj), fout);
    }

    PQclear(res);

    destroyPQExpBuffer(query);

    return tmplinfo;
}

/*
 * getTSConfigurations:
 *	  read all text search configurations in the system catalogs and return
 *	  them in the TSConfigInfo* structure
 *
 *	numTSConfigs is set to the number of configurations read in
 */
TSConfigInfo* getTSConfigurations(Archive* fout, int* numTSConfigs)
{
    PGresult* res = NULL;
    int ntups = 0;
    int i = 0;
    PQExpBuffer query;
    TSConfigInfo* cfginfo = NULL;
    int i_tableoid = 0;
    int i_oid = 0;
    int i_cfgname = 0;
    int i_cfgnamespace = 0;
    int i_rolname = 0;
    int i_cfgparser = 0;

    /* Before 8.3, there is no built-in text search support */
    if (fout->remoteVersion < 80300) {
        *numTSConfigs = 0;
        return NULL;
    }

    query = createPQExpBuffer();

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, "pg_catalog");

    appendPQExpBuffer(query,
        "SELECT tableoid, oid, cfgname, "
        "cfgnamespace, (%s cfgowner) AS rolname, cfgparser "
        "FROM pg_ts_config",
        username_subquery);

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    ntups = PQntuples(res);
    *numTSConfigs = ntups;

    cfginfo = (TSConfigInfo*)pg_malloc(ntups * sizeof(TSConfigInfo));

    i_tableoid = PQfnumber(res, "tableoid");
    i_oid = PQfnumber(res, "oid");
    i_cfgname = PQfnumber(res, "cfgname");
    i_cfgnamespace = PQfnumber(res, "cfgnamespace");
    i_rolname = PQfnumber(res, "rolname");
    i_cfgparser = PQfnumber(res, "cfgparser");

    for (i = 0; i < ntups; i++) {
        cfginfo[i].dobj.objType = DO_TSCONFIG;
        cfginfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        cfginfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&cfginfo[i].dobj);
        cfginfo[i].dobj.name = gs_strdup(PQgetvalue(res, i, i_cfgname));
        cfginfo[i].dobj.nmspace =
            findNamespace(fout, atooid(PQgetvalue(res, i, i_cfgnamespace)), cfginfo[i].dobj.catId.oid);
        cfginfo[i].rolname = gs_strdup(PQgetvalue(res, i, i_rolname));
        cfginfo[i].cfgparser = atooid(PQgetvalue(res, i, i_cfgparser));

        /* Decide whether we want to dump it */
        selectDumpableObject(&(cfginfo[i].dobj), fout);
    }

    PQclear(res);

    destroyPQExpBuffer(query);

    return cfginfo;
}

/*
 * getForeignDataWrappers:
 *	  read all foreign-data wrappers in the system catalogs and return
 *	  them in the FdwInfo* structure
 *
 *	numForeignDataWrappers is set to the number of fdws read in
 */
FdwInfo* getForeignDataWrappers(Archive* fout, int* numForeignDataWrappers)
{
    PGresult* res = NULL;
    int ntups;
    int i;
    PQExpBuffer query = createPQExpBuffer();
    FdwInfo* fdwinfo = NULL;
    int i_tableoid;
    int i_oid;
    int i_fdwname;
    int i_rolname;
    int i_fdwhandler;
    int i_fdwvalidator;
    int i_fdwacl;
    int i_fdwoptions;

    /* Before 8.4, there are no foreign-data wrappers */
    if (fout->remoteVersion < 80400) {
        *numForeignDataWrappers = 0;
        destroyPQExpBuffer(query);
        return NULL;
    }

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, "pg_catalog");

    if (fout->remoteVersion >= 90100) {
        appendPQExpBuffer(query,
            "SELECT tableoid, oid, fdwname, "
            "(%s fdwowner) AS rolname, "
            "fdwhandler::pg_catalog.regproc, "
            "fdwvalidator::pg_catalog.regproc, fdwacl, "
            "pg_catalog.array_to_string(ARRAY("
            "SELECT pg_catalog.quote_ident(option_name) || ' ' || "
            "pg_catalog.quote_literal(option_value) "
            "FROM pg_catalog.pg_options_to_table(fdwoptions) "
            "ORDER BY option_name"
            "), E',\n    ') AS fdwoptions "
            "FROM pg_foreign_data_wrapper",
            username_subquery);
    } else {
        appendPQExpBuffer(query,
            "SELECT tableoid, oid, fdwname, "
            "(%s fdwowner) AS rolname, "
            "'-' AS fdwhandler, "
            "fdwvalidator::pg_catalog.regproc, fdwacl, "
            "pg_catalog.array_to_string(ARRAY("
            "SELECT pg_catalog.quote_ident(option_name) || ' ' || "
            "pg_catalog.quote_literal(option_value) "
            "FROM pg_catalog.pg_options_to_table(fdwoptions) "
            "ORDER BY option_name"
            "), E',\n    ') AS fdwoptions "
            "FROM pg_foreign_data_wrapper",
            username_subquery);
    }

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    ntups = PQntuples(res);
    *numForeignDataWrappers = ntups;

    fdwinfo = (FdwInfo*)pg_malloc(ntups * sizeof(FdwInfo));

    i_tableoid = PQfnumber(res, "tableoid");
    i_oid = PQfnumber(res, "oid");
    i_fdwname = PQfnumber(res, "fdwname");
    i_rolname = PQfnumber(res, "rolname");
    i_fdwhandler = PQfnumber(res, "fdwhandler");
    i_fdwvalidator = PQfnumber(res, "fdwvalidator");
    i_fdwacl = PQfnumber(res, "fdwacl");
    i_fdwoptions = PQfnumber(res, "fdwoptions");

    for (i = 0; i < ntups; i++) {
        fdwinfo[i].dobj.objType = DO_FDW;
        fdwinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        fdwinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&fdwinfo[i].dobj);
        fdwinfo[i].dobj.name = gs_strdup(PQgetvalue(res, i, i_fdwname));
        fdwinfo[i].dobj.nmspace = NULL;
        fdwinfo[i].rolname = gs_strdup(PQgetvalue(res, i, i_rolname));
        fdwinfo[i].fdwhandler = gs_strdup(PQgetvalue(res, i, i_fdwhandler));
        fdwinfo[i].fdwvalidator = gs_strdup(PQgetvalue(res, i, i_fdwvalidator));
        fdwinfo[i].fdwoptions = gs_strdup(PQgetvalue(res, i, i_fdwoptions));
        fdwinfo[i].fdwacl = gs_strdup(PQgetvalue(res, i, i_fdwacl));

        /* Decide whether we want to dump it */
        selectDumpableObject(&(fdwinfo[i].dobj), fout);
    }

    PQclear(res);

    destroyPQExpBuffer(query);

    return fdwinfo;
}

/*
 * getForeignServers:
 *	  read all foreign servers in the system catalogs and return
 *	  them in the ForeignServerInfo * structure
 *
 *	numForeignServers is set to the number of servers read in
 */
ForeignServerInfo* getForeignServers(Archive* fout, int* numForeignServers)
{
    PGresult* res = NULL;
    int ntups = 0;
    int i = 0;
    PQExpBuffer query = createPQExpBuffer();
    ForeignServerInfo* srvinfo = NULL;
    int i_tableoid = 0;
    int i_oid = 0;
    int i_srvname = 0;
    int i_rolname = 0;
    int i_srvfdw = 0;
    int i_srvtype = 0;
    int i_srvversion = 0;
    int i_srvacl = 0;
    int i_srvoptions = 0;

    /* Before 8.4, there are no foreign servers */
    if (fout->remoteVersion < 80400) {
        *numForeignServers = 0;
        destroyPQExpBuffer(query);
        return NULL;
    }

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, "pg_catalog");

    appendPQExpBuffer(query,
        "SELECT tableoid, oid, srvname, "
        "(%s srvowner) AS rolname, "
        "srvfdw, srvtype, srvversion, srvacl,"
        "pg_catalog.array_to_string(ARRAY("
        "SELECT pg_catalog.quote_ident(option_name) || ' ' || "
        "pg_catalog.quote_literal(option_value) "
        "FROM pg_catalog.pg_options_to_table(srvoptions) "
        "ORDER BY option_name"
        "), E',\n    ') AS srvoptions "
        "FROM pg_foreign_server",
        username_subquery);

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    ntups = PQntuples(res);
    *numForeignServers = ntups;

    srvinfo = (ForeignServerInfo*)pg_malloc(ntups * sizeof(ForeignServerInfo));

    i_tableoid = PQfnumber(res, "tableoid");
    i_oid = PQfnumber(res, "oid");
    i_srvname = PQfnumber(res, "srvname");
    i_rolname = PQfnumber(res, "rolname");
    i_srvfdw = PQfnumber(res, "srvfdw");
    i_srvtype = PQfnumber(res, "srvtype");
    i_srvversion = PQfnumber(res, "srvversion");
    i_srvacl = PQfnumber(res, "srvacl");
    i_srvoptions = PQfnumber(res, "srvoptions");

    for (i = 0; i < ntups; i++) {
        srvinfo[i].dobj.objType = DO_FOREIGN_SERVER;
        srvinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        srvinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&srvinfo[i].dobj);
        srvinfo[i].dobj.name = gs_strdup(PQgetvalue(res, i, i_srvname));
        srvinfo[i].dobj.nmspace = NULL;
        srvinfo[i].rolname = gs_strdup(PQgetvalue(res, i, i_rolname));
        srvinfo[i].srvfdw = atooid(PQgetvalue(res, i, i_srvfdw));
        srvinfo[i].srvtype = gs_strdup(PQgetvalue(res, i, i_srvtype));
        srvinfo[i].srvversion = gs_strdup(PQgetvalue(res, i, i_srvversion));
        srvinfo[i].srvoptions = gs_strdup(PQgetvalue(res, i, i_srvoptions));
        srvinfo[i].srvacl = gs_strdup(PQgetvalue(res, i, i_srvacl));

        /* Decide whether we want to dump it */
        selectDumpableObject(&(srvinfo[i].dobj), fout);
    }

    PQclear(res);

    destroyPQExpBuffer(query);

    return srvinfo;
}

/*
 * getDefaultACLs:
 *	  read all default ACL information in the system catalogs and return
 *	  them in the DefaultACLInfo structure
 *
 *	numDefaultACLs is set to the number of ACLs read in
 */
DefaultACLInfo* getDefaultACLs(Archive* fout, int* numDefaultACLs)
{
    DefaultACLInfo* daclinfo = NULL;
    PQExpBuffer query;
    PGresult* res = NULL;
    int i_oid;
    int i_tableoid;
    int i_defaclrole;
    int i_defaclnamespace;
    int i_defaclobjtype;
    int i_defaclacl;
    int i, ntups;

    if (fout->remoteVersion < 90000) {
        *numDefaultACLs = 0;
        return NULL;
    }

    query = createPQExpBuffer();

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, "pg_catalog");

    appendPQExpBuffer(query,
        "SELECT oid, tableoid, "
        "(%s defaclrole) AS defaclrole, "
        "defaclnamespace, "
        "defaclobjtype, "
        "defaclacl "
        "FROM pg_default_acl",
        username_subquery);

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);
    *numDefaultACLs = ntups;

    daclinfo = (DefaultACLInfo*)pg_malloc(ntups * sizeof(DefaultACLInfo));

    i_oid = PQfnumber(res, "oid");
    i_tableoid = PQfnumber(res, "tableoid");
    i_defaclrole = PQfnumber(res, "defaclrole");
    i_defaclnamespace = PQfnumber(res, "defaclnamespace");
    i_defaclobjtype = PQfnumber(res, "defaclobjtype");
    i_defaclacl = PQfnumber(res, "defaclacl");

    for (i = 0; i < ntups; i++) {
        Oid nspid = atooid(PQgetvalue(res, i, i_defaclnamespace));

        daclinfo[i].dobj.objType = DO_DEFAULT_ACL;
        daclinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        daclinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&daclinfo[i].dobj);
        /* cheesy ... is it worth coming up with a better object name? */
        daclinfo[i].dobj.name = gs_strdup(PQgetvalue(res, i, i_defaclobjtype));

        if (nspid != InvalidOid)
            daclinfo[i].dobj.nmspace = findNamespace(fout, nspid, daclinfo[i].dobj.catId.oid);
        else
            daclinfo[i].dobj.nmspace = NULL;

        daclinfo[i].defaclrole = gs_strdup(PQgetvalue(res, i, i_defaclrole));
        daclinfo[i].defaclobjtype = *(PQgetvalue(res, i, i_defaclobjtype));
        daclinfo[i].defaclacl = gs_strdup(PQgetvalue(res, i, i_defaclacl));

        /* Decide whether we want to dump it */
        selectDumpableDefaultACL(&(daclinfo[i]));
    }

    PQclear(res);

    destroyPQExpBuffer(query);

    /* free 1 bytes of memory that is allocated above by pg_malloc, if no tuples exist */
    if (0 == ntups) {
        free(daclinfo);
        daclinfo = NULL;
    }

    return daclinfo;
}

/*
 * dumpComment --
 *
 * This routine is used to dump any comments associated with the
 * object handed to this routine. The routine takes a constant character
 * string for the target part of the comment-creation command, plus
 * the nmspace and owner of the object (for labeling the ArchiveEntry),
 * plus catalog ID and subid which are the lookup key for pg_description,
 * plus the dump ID for the object (for setting a dependency).
 * If a matching pg_description entry is found, it is dumped.
 *
 * Note: although this routine takes a dumpId for dependency purposes,
 * that purpose is just to mark the dependency in the emitted dump file
 * for possible future use by pg_restore.  We do NOT use it for determining
 * ordering of the comment in the dump file, because this routine is called
 * after dependency sorting occurs.  This routine should be called just after
 * calling ArchiveEntry() for the specified object.
 */
static void dumpComment(Archive* fout, const char* target, const char* nmspace, const char* owner, CatalogId catalogId,
    int subid, DumpId dumpId)
{
    CommentItem* comments = NULL;
    int ncomments;

    /* Comments are schema not data ... except blob comments are data */
    if (strncmp(target, "LARGE OBJECT ", 13) != 0) {
        if (dataOnly)
            return;
    } else {
        if (schemaOnly)
            return;
    }

    /* Search for comments associated with catalogId, using table */
    ncomments = findComments(fout, catalogId.tableoid, catalogId.oid, &comments);

    /* Is there one matching the subid? */
    while (ncomments > 0) {
        if (comments->objsubid == subid)
            break;
        comments++;
        ncomments--;
    }

    /* If a comment exists, build COMMENT ON statement */
    if (ncomments > 0) {
        PQExpBuffer query = createPQExpBuffer();

        appendPQExpBuffer(query, "COMMENT ON %s IS ", target);
        appendStringLiteralAH(query, comments->descr, fout);
        appendPQExpBuffer(query, ";\n");

        /*
         * We mark comments as SECTION_NONE because they really belong in the
         * same section as their parent, whether that is pre-data or
         * post-data.
         */
        ArchiveEntry(fout,
            nilCatalogId,
            createDumpId(),
            target,
            nmspace,
            NULL,
            owner,
            false,
            "COMMENT",
            SECTION_NONE,
            query->data,
            "",
            NULL,
            &(dumpId),
            1,
            NULL,
            NULL);

        destroyPQExpBuffer(query);
    }
}

/*
 * dumpTableComment --
 *
 * As above, but dump comments for both the specified table (or view)
 * and its columns.
 */
static void dumpTableComment(Archive* fout, TableInfo* tbinfo, const char* reltypename)
{
    CommentItem* comments = NULL;
    int ncomments;
    PQExpBuffer query;
    PQExpBuffer target;

    /* Comments are SCHEMA not data */
    if (dataOnly)
        return;

    /* Search for comments associated with relation, using table */
    ncomments = findComments(fout, tbinfo->dobj.catId.tableoid, tbinfo->dobj.catId.oid, &comments);

    /* If comments exist, build COMMENT ON statements */
    if (ncomments <= 0)
        return;

    query = createPQExpBuffer();
    target = createPQExpBuffer();

    while (ncomments > 0) {
        const char* descr = comments->descr;
        int objsubid = comments->objsubid;

        if (objsubid == 0) {
            resetPQExpBuffer(target);
            appendPQExpBuffer(target, "%s %s", reltypename, fmtId(tbinfo->dobj.name));

            resetPQExpBuffer(query);
            appendPQExpBuffer(query, "COMMENT ON %s IS ", target->data);
            appendStringLiteralAH(query, descr, fout);
            appendPQExpBuffer(query, ";\n");

            ArchiveEntry(fout,
                nilCatalogId,
                createDumpId(),
                target->data,
                tbinfo->dobj.nmspace->dobj.name,
                NULL,
                tbinfo->rolname,
                false,
                "COMMENT",
                SECTION_NONE,
                query->data,
                "",
                NULL,
                &(tbinfo->dobj.dumpId),
                1,
                NULL,
                NULL);
        } else if (objsubid > 0 && objsubid <= tbinfo->numatts) {
            resetPQExpBuffer(target);
            appendPQExpBuffer(target, "COLUMN %s.", fmtId(tbinfo->dobj.name));
            appendPQExpBuffer(target, "%s", fmtId(tbinfo->attnames[objsubid - 1]));

            resetPQExpBuffer(query);
            appendPQExpBuffer(query, "COMMENT ON %s IS ", target->data);
            appendStringLiteralAH(query, descr, fout);
            appendPQExpBuffer(query, ";\n");

            ArchiveEntry(fout,
                nilCatalogId,
                createDumpId(),
                target->data,
                tbinfo->dobj.nmspace->dobj.name,
                NULL,
                tbinfo->rolname,
                false,
                "COMMENT",
                SECTION_NONE,
                query->data,
                "",
                NULL,
                &(tbinfo->dobj.dumpId),
                1,
                NULL,
                NULL);
        }

        comments++;
        ncomments--;
    }

    destroyPQExpBuffer(query);
    destroyPQExpBuffer(target);
}

/*
 * findComments --
 *
 * Find the comment(s), if any, associated with the given object.  All the
 * objsubid values associated with the given classoid/objoid are found with
 * one search.
 */
static int findComments(Archive* fout, Oid classoid, Oid objoid, CommentItem** items)
{
    /* static storage for table of comments */
    static CommentItem* comments = NULL;
    static int ncomments = -1;

    CommentItem* cmiddle = NULL;
    CommentItem* clow = NULL;
    CommentItem* chigh = NULL;
    int nmatch;

    /* Get comments if we didn't already */
    if (ncomments < 0)
        ncomments = collectComments(fout, &comments);

    /*
     * Pre-7.2, pg_description does not contain classoid, so collectComments
     * just stores a zero.	If there's a collision on object OID, well, you
     * get duplicate comments.
     */
    if (fout->remoteVersion < 70200)
        classoid = 0;

    /*
     * Do binary search to find some item matching the object.
     */
    clow = &comments[0];
    chigh = &comments[ncomments - 1];
    while (clow <= chigh) {
        cmiddle = clow + (chigh - clow) / 2;

        if (classoid < cmiddle->classoid)
            chigh = cmiddle - 1;
        else if (classoid > cmiddle->classoid)
            clow = cmiddle + 1;
        else if (objoid < cmiddle->objoid)
            chigh = cmiddle - 1;
        else if (objoid > cmiddle->objoid)
            clow = cmiddle + 1;
        else
            break; /* found a match */
    }

    if (clow > chigh) /* no matches */
    {
        *items = NULL;
        return 0;
    }

    /*
     * Now determine how many items match the object.  The search loop
     * invariant still holds: only items between low and high inclusive could
     * match.
     */
    nmatch = 1;
    while (cmiddle > clow) {
        if (classoid != cmiddle[-1].classoid || objoid != cmiddle[-1].objoid)
            break;
        cmiddle--;
        nmatch++;
    }

    *items = cmiddle;

    cmiddle += nmatch;
    while (cmiddle <= chigh) {
        if (classoid != cmiddle->classoid || objoid != cmiddle->objoid)
            break;
        cmiddle++;
        nmatch++;
    }

    return nmatch;
}

/*
 * collectComments --
 *
 * Construct a table of all comments available for database objects.
 * We used to do per-object queries for the comments, but it's much faster
 * to pull them all over at once, and on most databases the memory cost
 * isn't high.
 *
 * The table is sorted by classoid/objid/objsubid for speed in lookup.
 */
static int collectComments(Archive* fout, CommentItem** items)
{
    PGresult* res = NULL;
    PQExpBuffer query;
    int i_description = 0;
    int i_classoid = 0;
    int i_objoid = 0;
    int i_objsubid = 0;
    int ntups = 0;
    int i = 0;
    CommentItem* comments = NULL;

    /*
     * Note we do NOT change source schema here; preserve the caller's
     * setting, instead.
     */

    query = createPQExpBuffer();

    if (fout->remoteVersion >= 70300) {
        appendPQExpBuffer(query,
            "SELECT description, classoid, objoid, objsubid "
            "FROM pg_catalog.pg_description "
            "ORDER BY classoid, objoid, objsubid");
    } else if (fout->remoteVersion >= 70200) {
        appendPQExpBuffer(query,
            "SELECT description, classoid, objoid, objsubid "
            "FROM pg_description "
            "ORDER BY classoid, objoid, objsubid");
    } else {
        /* Note: this will fail to find attribute comments in pre-7.2... */
        appendPQExpBuffer(query,
            "SELECT description, 0 AS classoid, objoid, 0 AS objsubid "
            "FROM pg_description "
            "ORDER BY objoid");
    }

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    /* Construct lookup table containing OIDs in numeric form */

    i_description = PQfnumber(res, "description");
    i_classoid = PQfnumber(res, "classoid");
    i_objoid = PQfnumber(res, "objoid");
    i_objsubid = PQfnumber(res, "objsubid");

    ntups = PQntuples(res);

    comments = (CommentItem*)pg_malloc(ntups * sizeof(CommentItem));

    for (i = 0; i < ntups; i++) {

        comments[i].descr = gs_strdup(PQgetvalue(res, i, i_description));
        comments[i].classoid = atooid(PQgetvalue(res, i, i_classoid));
        comments[i].objoid = atooid(PQgetvalue(res, i, i_objoid));
        comments[i].objsubid = atoi(PQgetvalue(res, i, i_objsubid));
    }

    /* Do NOT free the PGresult since we are keeping pointers into it */
    destroyPQExpBuffer(query);

    *items = comments;
    PQclear(res);
    return ntups;
}

static void check_dump_func(Archive* fout, DumpableObject* dobj)
{
    if (!exclude_function) {
        dumpFunc(fout, (FuncInfo*)dobj);
    }
}

/*
 * dumpDumpableObject
 *
 * This routine and its subsidiaries are responsible for creating
 * ArchiveEntries (TOC objects) for each object to be dumped.
 */
static void dumpDumpableObject(Archive* fout, DumpableObject* dobj)
{
    switch (dobj->objType) {
        case DO_NAMESPACE:
            dumpNamespace(fout, (NamespaceInfo*)dobj);
            break;
        case DO_EXTENSION:
            if (true == include_extensions) {
                dumpExtension(fout, (ExtensionInfo*)dobj);
            }
            break;
        case DO_TYPE:
            dumpType(fout, (TypeInfo*)dobj);
            break;
        case DO_SHELL_TYPE:
            dumpShellType(fout, (ShellTypeInfo*)dobj);
            break;
        case DO_FUNC:
            check_dump_func(fout, dobj);
            break;
        case DO_PACKAGE:
            DumpPkgSpec(fout, (PkgInfo*)dobj);
            DumpPkgBody(fout, (PkgInfo*)dobj);
            break;
        case DO_AGG:
            dumpAgg(fout, (AggInfo*)dobj);
            break;
        case DO_OPERATOR:
            dumpOpr(fout, (OprInfo*)dobj);
            break;
        case DO_ACCESS_METHOD:
            dumpAccessMethod(fout, (const AccessMethodInfo *) dobj);
            break;
        case DO_OPCLASS:
            dumpOpclass(fout, (OpclassInfo*)dobj);
            break;
        case DO_OPFAMILY:
            dumpOpfamily(fout, (OpfamilyInfo*)dobj);
            break;
        case DO_COLLATION:
            dumpCollation(fout, (CollInfo*)dobj);
            break;
        case DO_CONVERSION:
            dumpConversion(fout, (ConvInfo*)dobj);
            break;
        case DO_TABLE:
            dumpTable(fout, (TableInfo*)dobj);
            break;
        case DO_ATTRDEF:
            dumpAttrDef(fout, (AttrDefInfo*)dobj);
            break;
        case DO_INDEX:
            dumpIndex(fout, (IndxInfo*)dobj);
            break;
        case DO_RULE:
            dumpRule(fout, (RuleInfo*)dobj);
            break;
        case DO_TRIGGER: {
            /* -t condition will not dump trigger without trigger function. */
            if (gTableCount == 0)
                dumpTrigger(fout, (TriggerInfo*)dobj);
            break;
        }
        case DO_EVENT: {
            dumpEvent(fout, (EventInfo *)dobj);
            break;
        }
        case DO_CONSTRAINT:
            dumpConstraint(fout, (ConstraintInfo*)dobj);
            break;
        case DO_FTBL_CONSTRAINT:
            dumpConstraintForForeignTbl(fout, (ConstraintInfo*)dobj);
            break;
        case DO_FK_CONSTRAINT:
            dumpConstraint(fout, (ConstraintInfo*)dobj);
            break;
        case DO_PROCLANG:
            dumpProcLang(fout, (ProcLangInfo*)dobj);
            break;
        case DO_CAST:
            dumpCast(fout, (CastInfo*)dobj);
            break;
        case DO_TABLE_DATA: {
            ArchiveHandle* AH = (ArchiveHandle*)fout;
            TableDataInfo* tdinfo = (TableDataInfo*)dobj;
            TableInfo* tbinfo = tdinfo->tdtable;
            char* executeUser = NULL;
            bool isExistsRolkind = false;

            isExistsRolkind = is_column_exists(AH->connection, AuthIdRelationId, "rolkind");
            executeUser = PQuser(AH->connection);
            /*
             * If the table's owner is independent role, only need the owner to backup it.
             * else, print warning message, and skip the object.
             */
            if (isExistsRolkind && isIndependentRole(AH->connection, tbinfo->rolname)) {
                size_t maxLen1 = 0;
                size_t maxLen2 = 0;

                if (use_role != NULL) {
                    /* if role is not the owner of object,
                     * it means that there is no permission to dump
                     */
                    maxLen1 = strlen(use_role) > strlen(tbinfo->rolname) ? strlen(use_role) : strlen(tbinfo->rolname);
                    if (0 != strncmp(use_role, tbinfo->rolname, maxLen1)) {
                        write_msg(NULL,
                            "WARNING: permission denied for object \"%s\" when dump the object data, only user %s can "
                            "do it\n",
                            fmtQualifiedId(fout, tbinfo->dobj.nmspace->dobj.name, tbinfo->dobj.name),
                            tbinfo->rolname);
                        break;
                    }
                } else {
                    maxLen2 =
                        strlen(executeUser) > strlen(tbinfo->rolname) ? strlen(executeUser) : strlen(tbinfo->rolname);
                    if (0 != strncmp(executeUser, tbinfo->rolname, maxLen2)) {
                        write_msg(NULL,
                            "WARNING: permission denied for object \"%s\" when dump the object data, only user %s can "
                            "do it\n",
                            fmtQualifiedId(fout, tbinfo->dobj.nmspace->dobj.name, tbinfo->dobj.name),
                            tbinfo->rolname);
                        break;
                    }
                }
            }

            if (RELKIND_IS_SEQUENCE(tbinfo->relkind))
                dumpSequenceData(fout, (TableDataInfo*)dobj, tbinfo->relkind == RELKIND_LARGE_SEQUENCE);
            else
                dumpTableData(fout, (TableDataInfo*)dobj);
            break;
        }
        case DO_TSPARSER:
            dumpTSParser(fout, (TSParserInfo*)dobj);
            break;
        case DO_TSDICT:
            dumpTSDictionary(fout, (TSDictInfo*)dobj);
            break;
        case DO_TSTEMPLATE:
            dumpTSTemplate(fout, (TSTemplateInfo*)dobj);
            break;
        case DO_TSCONFIG:
            dumpTSConfig(fout, (TSConfigInfo*)dobj);
            break;
        case DO_EVENT_TRIGGER:
            dumpEventTrigger(fout, (EventTriggerInfo *) dobj);
            break;
        case DO_FDW:
            dumpForeignDataWrapper(fout, (FdwInfo*)dobj);
            break;
        case DO_FOREIGN_SERVER:
            dumpForeignServer(fout, (ForeignServerInfo*)dobj);
            break;
        case DO_DEFAULT_ACL:
            dumpDefaultACL(fout, (DefaultACLInfo*)dobj);
            break;
        case DO_BLOB:
            dumpBlob(fout, (BlobInfo*)dobj);
            break;
        case DO_BLOB_DATA:
            ArchiveEntry(fout,
                dobj->catId,
                dobj->dumpId,
                dobj->name,
                NULL,
                NULL,
                "",
                false,
                "BLOBS",
                SECTION_DATA,
                "",
                "",
                NULL,
                NULL,
                0,
                dumpBlobs,
                NULL);
            break;
        case DO_RLSPOLICY:
            dumpRlsPolicy(fout, (RlsPolicyInfo*)dobj);
            break;
        case DO_PUBLICATION:
            dumpPublication(fout, (PublicationInfo *)dobj);
            break;
        case DO_PUBLICATION_REL:
            dumpPublicationTable(fout, (PublicationRelInfo *)dobj);
            break;
        case DO_SUBSCRIPTION:
            dumpSubscription(fout, (SubscriptionInfo *)dobj);
            break;
        case DO_PRE_DATA_BOUNDARY:
        case DO_POST_DATA_BOUNDARY:
            /* never dumped, nothing to do */
        case DO_DUMMY_TYPE:
            /* table rowtypes and array types are never dumped separately */
            break;
        default:
            break;
    }
}

/*
 * dumpNamespace
 *	  writes out to fout the queries to recreate a user-defined nmspace
 */
static void dumpNamespace(Archive* fout, NamespaceInfo* nspinfo)
{
    PQExpBuffer q;
    PQExpBuffer delq;
    PQExpBuffer labelq;
    char* qnspname = NULL;

    /* Skip if not to be dumped */
    if (!nspinfo->dobj.dump || dataOnly)
        return;

    /* don't dump dummy nmspace from pre-7.3 source */
    if (strlen(nspinfo->dobj.name) == 0)
        return;

    if (isDB4AIschema(nspinfo)) {
        return;
    }
    if (isExecUserNotObjectOwner(fout, nspinfo->rolname))
        return;

    q = createPQExpBuffer();
    delq = createPQExpBuffer();
    labelq = createPQExpBuffer();

    qnspname = gs_strdup(fmtId(nspinfo->dobj.name));
    if (strcasecmp(qnspname, "public") != 0) {
        appendPQExpBuffer(delq, "DROP SCHEMA IF EXISTS %s%s;\n", qnspname, if_cascade);

        if (nspinfo->hasBlockchain) {
            appendPQExpBuffer(q, "CREATE SCHEMA %s WITH BLOCKCHAIN;\n", qnspname);
        } else {
            appendPQExpBuffer(q, "CREATE SCHEMA %s;\n", qnspname);
        }
        if (OidIsValid(nspinfo->collate)) {
            CollInfo *coll = NULL;
            coll = findCollationByOid(nspinfo->collate);
            if (coll != NULL) {
                appendPQExpBuffer(q, "ALTER SCHEMA %s COLLATE = %s;\n", qnspname, fmtId(coll->dobj.name));
            }
        }
    }

    appendPQExpBuffer(labelq, "SCHEMA %s", qnspname);

    if (binary_upgrade)
        binary_upgrade_extension_member(q, &nspinfo->dobj, labelq->data);

    ArchiveEntry(fout,
        nspinfo->dobj.catId,
        nspinfo->dobj.dumpId,
        nspinfo->dobj.name,
        NULL,
        NULL,
        nspinfo->rolname,
        false,
        "SCHEMA",
        SECTION_PRE_DATA,
        q->data,
        delq->data,
        NULL,
        NULL,
        0,
        NULL,
        NULL);

    /* Dump Schema Comments and Security Labels */
    if (strcasecmp(qnspname, "public") != 0) {
        dumpComment(fout, labelq->data, NULL, nspinfo->rolname, nspinfo->dobj.catId, 0, nspinfo->dobj.dumpId);
    }
    dumpSecLabel(fout, labelq->data, NULL, nspinfo->rolname, nspinfo->dobj.catId, 0, nspinfo->dobj.dumpId);

    dumpACL(fout,
        nspinfo->dobj.catId,
        nspinfo->dobj.dumpId,
        "SCHEMA",
        qnspname,
        NULL,
        nspinfo->dobj.name,
        NULL,
        nspinfo->rolname,
        nspinfo->nspacl);
#ifdef HAVE_CE
    dumpClientGlobalKeys(fout, nspinfo->dobj.catId.oid, nspinfo->dobj.name);
    dumpColumnEncryptionKeys(fout, nspinfo->dobj.catId.oid, nspinfo->dobj.name);
#endif
    free(qnspname);
    qnspname = NULL;

    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
    destroyPQExpBuffer(labelq);
}

/*
 * dumpDirectory
 * 	dump directory definition from pg_directory if pg_directory exists
 */
static void dumpDirectory(Archive* fout)
{
    PQExpBuffer q;
    PQExpBuffer delq;
    PQExpBuffer query;
    PGresult* res = NULL;
    int ntups = 0;
    int i = 0;
    int i_tableoid = 0;
    int i_oid = 0;
    int i_dirname = 0;
    int i_rolname = 0;
    int i_dirpath = 0;
    int i_diracl = 0;
    char* dirname = NULL;
    char* rolname = NULL;
    char* dirpath = NULL;
    char* diracl = NULL;

    if (!isExecUserSuperRole(fout)) {
        write_msg(NULL, "WARNING: directory not dumped because current user is not a superuser\n");
        return;
    }

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, "pg_catalog");

    /* check system table pg_directory, makesure it is exists */
    query = createPQExpBuffer();
    appendPQExpBuffer(query,
        "SELECT relname "
        "FROM pg_class c, pg_namespace n "
        "where c.relname='pg_directory' and n.nspname='pg_catalog'");
    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);
    if (ntups < 1) {
        PQclear(res);
        destroyPQExpBuffer(query);
        return;
    }

    /* get info about pg_directory */
    PQclear(res);
    resetPQExpBuffer(query);
    appendPQExpBuffer(query,
        "SELECT d.tableoid, d.oid, d.dirname, a.rolname, d.dirpath, d.diracl "
        "FROM pg_directory d, pg_authid a "
        "WHERE d.owner = a.oid ");
    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);
    if (ntups < 1) {
        PQclear(res);
        destroyPQExpBuffer(query);
        return;
    }

    /* Construct lookup table containing OIDs in numeric form */
    i_tableoid = PQfnumber(res, "tableoid");
    i_oid = PQfnumber(res, "oid");
    i_dirname = PQfnumber(res, "dirname");
    i_rolname = PQfnumber(res, "rolname");
    i_dirpath = PQfnumber(res, "dirpath");
    i_diracl = PQfnumber(res, "diracl");

    for (i = 0; i < ntups; i++) {
        CatalogId objId;
        objId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        objId.oid = atooid(PQgetvalue(res, i, i_oid));

        dirname = gs_strdup(PQgetvalue(res, i, i_dirname));
        rolname = gs_strdup(PQgetvalue(res, i, i_rolname));
        dirpath = gs_strdup(PQgetvalue(res, i, i_dirpath));
        diracl = gs_strdup(PQgetvalue(res, i, i_diracl));

        q = createPQExpBuffer();
        delq = createPQExpBuffer();

        appendPQExpBuffer(delq, "DROP DIRECTORY IF EXISTS %s;\n", fmtId(dirname));
        appendPQExpBuffer(q, "CREATE DIRECTORY %s AS '%s';", fmtId(dirname), dirpath);

        ArchiveEntry(fout,
            objId,            /* catalog ID */
            createDumpId(),   /* dump ID */
            dirname,          /* Name */
            NULL,             /* Namespace */
            NULL,             /* Tablespace */
            rolname,          /* Owner */
            false,            /* with oids */
            "DIRECTORY",      /* Desc */
            SECTION_PRE_DATA, /* Section */
            q->data,          /* Create */
            delq->data,       /* Del */
            NULL,             /* Copy */
            NULL,             /* Deps */
            0,                /* #Dep */
            NULL,             /* Dumper */
            NULL);            /* Dumper arg */

        /*
         * Comments and Security Labels for DIRECTORY are not supported, so skip them.
         * Only dump the ACL info
         */
        dumpACL(fout, objId, createDumpId(), "DIRECTORY", fmtId(dirname), NULL, dirname, NULL, rolname, diracl);

        destroyPQExpBuffer(q);
        destroyPQExpBuffer(delq);

        GS_FREE(dirname);
        GS_FREE(rolname);
        GS_FREE(dirpath);
        GS_FREE(diracl);
    }

    PQclear(res);
    destroyPQExpBuffer(query);
    return;
}

/*
 * dumpExtension
 *	  writes out to fout the queries to recreate an extension
 */
static void dumpExtension(Archive* fout, ExtensionInfo* extinfo)
{
    PQExpBuffer q;
    PQExpBuffer delq;
    PQExpBuffer labelq;
    char* qextname = NULL;

    /* Skip if not to be dumped */
    if (!extinfo->dobj.dump || dataOnly)
        return;

    if (isExecUserNotObjectOwner(fout, extinfo->extrole))
        return;

    q = createPQExpBuffer();
    delq = createPQExpBuffer();
    labelq = createPQExpBuffer();

    qextname = gs_strdup(fmtId(extinfo->dobj.name));

    appendPQExpBuffer(delq, "DROP EXTENSION %s;\n", qextname);

    if (!binary_upgrade) {
        /*
         * In a regular dump, we use IF NOT EXISTS so that there isn't a
         * problem if the extension already exists in the target database;
         * this is essential for installed-by-default extensions such as
         * plpgsql.
         *
         * In binary-upgrade mode, that doesn't work well, so instead we skip
         * built-in extensions based on their OIDs; see
         * selectDumpableExtension.
         */
        appendPQExpBuffer(q, "CREATE EXTENSION IF NOT EXISTS %s WITH SCHEMA %s;\n", qextname, fmtId(extinfo->nmspace));
    } else {
        int i = 0;
        int n = 0;
        DumpableObject* extobj = NULL;

        appendPQExpBuffer(q, "-- For binary upgrade, create an empty extension and insert objects into it\n");

        /*
         *	We unconditionally create the extension, so we must drop it if it
         *	exists.  This could happen if the user deleted 'plpgsql' and then
         *	readded it, causing its oid to be greater than FirstNormalObjectId.
         *	The FirstNormalObjectId test was kept to avoid repeatedly dropping
         *	and recreating extensions like 'plpgsql'.
         */
        appendPQExpBuffer(q, "DROP EXTENSION IF EXISTS %s;\n", qextname);

        appendPQExpBuffer(q, "SELECT binary_upgrade.create_empty_extension(");
        appendStringLiteralAH(q, extinfo->dobj.name, fout);
        appendPQExpBuffer(q, ", ");
        appendStringLiteralAH(q, extinfo->nmspace, fout);
        appendPQExpBuffer(q, ", ");
        appendPQExpBuffer(q, "%s, ", extinfo->relocatable ? "true" : "false");
        appendStringLiteralAH(q, extinfo->extversion, fout);
        appendPQExpBuffer(q, ", ");

        /*
         * Note that we're pushing extconfig (an OID array) back into
         * pg_extension exactly as-is.	This is OK because pg_class OIDs are
         * preserved in binary upgrade.
         */
        if (strlen(extinfo->extconfig) > 2)
            appendStringLiteralAH(q, extinfo->extconfig, fout);
        else
            appendPQExpBuffer(q, "NULL");
        appendPQExpBuffer(q, ", ");
        if (strlen(extinfo->extcondition) > 2)
            appendStringLiteralAH(q, extinfo->extcondition, fout);
        else
            appendPQExpBuffer(q, "NULL");
        appendPQExpBuffer(q, ", ");
        appendPQExpBuffer(q, "ARRAY[");
        n = 0;
        for (i = 0; i < extinfo->dobj.nDeps; i++) {
            extobj = findObjectByDumpId(extinfo->dobj.dependencies[i]);
            if (NULL != extobj && extobj->objType == DO_EXTENSION) {
                if (n++ > 0)
                    appendPQExpBuffer(q, ",");
                appendStringLiteralAH(q, extobj->name, fout);
            }
        }
        appendPQExpBuffer(q, "]::pg_catalog.text[]");
        appendPQExpBuffer(q, ");\n");
    }

    appendPQExpBuffer(labelq, "EXTENSION %s", qextname);

    ArchiveEntry(fout,
        extinfo->dobj.catId,
        extinfo->dobj.dumpId,
        extinfo->dobj.name,
        NULL,
        NULL,
        "",
        false,
        "EXTENSION",
        SECTION_PRE_DATA,
        q->data,
        delq->data,
        NULL,
        NULL,
        0,
        NULL,
        NULL);

    /* Dump Extension Comments and Security Labels */
    dumpComment(fout, labelq->data, NULL, "", extinfo->dobj.catId, 0, extinfo->dobj.dumpId);
    dumpSecLabel(fout, labelq->data, NULL, "", extinfo->dobj.catId, 0, extinfo->dobj.dumpId);

    free(qextname);
    qextname = NULL;

    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
    destroyPQExpBuffer(labelq);
}

/*
 * dumpType
 *	  writes out to fout the queries to recreate a user-defined type
 */
static void dumpType(Archive* fout, TypeInfo* tyinfo)
{
    /* Skip if not to be dumped */
    if (!tyinfo->dobj.dump || dataOnly)
        return;

    if (isExecUserNotObjectOwner(fout, tyinfo->rolname))
        return;

    /* Dump out in proper style */
    if (tyinfo->typtype == TYPTYPE_BASE)
        dumpBaseType(fout, tyinfo);
    else if (tyinfo->typtype == TYPTYPE_DOMAIN)
        dumpDomain(fout, tyinfo);
    else if (tyinfo->typtype == TYPTYPE_COMPOSITE)
        dumpCompositeType(fout, tyinfo);
    else if (tyinfo->typtype == TYPTYPE_ENUM) {
        if (findDBCompatibility(fout, PQdb(GetConnection(fout))) && hasSpecificExtension(fout, "dolphin")) {
            return;
        }
        dumpEnumType(fout, tyinfo);
    }
    else if (tyinfo->typtype == TYPTYPE_RANGE)
        dumpRangeType(fout, tyinfo);
    else if (tyinfo->typtype == TYPTYPE_TABLEOF)
        dumpTableofType(fout, tyinfo);
    else if (tyinfo->typtype == TYPTYPE_SET)
        /* set type is created with table, can not be created alone */
        return;
    else
        write_msg(NULL, "WARNING: typtype of data type \"%s\" appears to be invalid\n", tyinfo->dobj.name);
}

/*
 * dumpTableofType
 *       writes out to fout the queries to recreate a user-defined tableof type
 */
static void dumpTableofType(Archive* fout, TypeInfo* tyinfo)
{
    PQExpBuffer q = createPQExpBuffer();
    PQExpBuffer delq = createPQExpBuffer();
    PQExpBuffer labelq = createPQExpBuffer();
    PQExpBuffer query = createPQExpBuffer();
    PGresult* res = NULL;
    int num = 0;
    int i = 0;
    char* qtypname = NULL;
    char* label = NULL;
    int field_num = -1;

    /* Set proper schema search path */
    selectSourceSchema(fout, "pg_catalog");
    appendPQExpBuffer(query,
                      "SELECT typname "
                      "FROM pg_catalog.pg_type join "
                      "(select typelem from pg_catalog.pg_type "
                      "WHERE oid = '%u') t "
                      "on oid = t.typelem",
                      tyinfo->typelem);
    
    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    num = PQntuples(res);

    qtypname = gs_strdup(fmtId(tyinfo->dobj.name));
    /*
     * DROP must be fully qualified in case same name appears in pg_catalog.
     * CASCADE shouldn't be required here as for normal types since the I/O
     * functions are generic and do not get dropped.
     */
    appendPQExpBuffer(delq, "DROP TYPE %s%s.", if_exists, fmtId(tyinfo->dobj.nmspace->dobj.name));
    appendPQExpBuffer(delq, "%s%s;\n", qtypname, if_cascade);

    appendPQExpBuffer(q, "CREATE TYPE %s AS TABLE OF ", qtypname);

    if (num != 1) {
        PQclear(res);
        destroyPQExpBuffer(q);
        destroyPQExpBuffer(delq);
        destroyPQExpBuffer(labelq);
        destroyPQExpBuffer(query);
        free(qtypname);
        return;
    }
    field_num = PQfnumber(res, "typname");
    label = PQgetvalue(res, i, field_num);
    appendPQExpBuffer(q, "%s;\n", fmtId(label));

    appendPQExpBuffer(labelq, "TYPE %s", qtypname);

    ArchiveEntry(fout,
        tyinfo->dobj.catId,
        tyinfo->dobj.dumpId,
        tyinfo->dobj.name,
        tyinfo->dobj.nmspace->dobj.name,
        NULL,
        tyinfo->rolname,
        false,
        "TYPE",
        SECTION_PRE_DATA,
        q->data,
        delq->data,
        NULL,
        NULL,
        0,
        NULL,
        NULL);

    /* Dump Type Comments and Security Labels */
    dumpComment(fout,
        labelq->data,
        tyinfo->dobj.nmspace->dobj.name,
        tyinfo->rolname,
        tyinfo->dobj.catId,
        0,
        tyinfo->dobj.dumpId);
    dumpSecLabel(fout,
        labelq->data,
        tyinfo->dobj.nmspace->dobj.name,
        tyinfo->rolname,
        tyinfo->dobj.catId,
        0,
        tyinfo->dobj.dumpId);

    dumpACL(fout,
        tyinfo->dobj.catId,
        tyinfo->dobj.dumpId,
        "TYPE",
        qtypname,
        NULL,
        tyinfo->dobj.name,
        tyinfo->dobj.nmspace->dobj.name,
        tyinfo->rolname,
        tyinfo->typacl);

    PQclear(res);
    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
    destroyPQExpBuffer(labelq);
    destroyPQExpBuffer(query);
    free(qtypname);
    qtypname = NULL;
}

/*
 * dumpEnumType
 *	  writes out to fout the queries to recreate a user-defined enum type
 */
static void dumpEnumType(Archive* fout, TypeInfo* tyinfo)
{
    PQExpBuffer q = createPQExpBuffer();
    PQExpBuffer delq = createPQExpBuffer();
    PQExpBuffer labelq = createPQExpBuffer();
    PQExpBuffer query = createPQExpBuffer();
    PGresult* res = NULL;
    int num = 0;
    int i = 0;
    Oid enum_oid = 0;
    char* qtypname = NULL;
    char* label = NULL;
    int field_num = -1;

    /* Set proper schema search path */
    selectSourceSchema(fout, "pg_catalog");

    if (fout->remoteVersion >= 90100)
        appendPQExpBuffer(query,
            "SELECT oid, enumlabel "
            "FROM pg_catalog.pg_enum "
            "WHERE enumtypid = '%u'"
            "ORDER BY enumsortorder",
            tyinfo->dobj.catId.oid);
    else
        appendPQExpBuffer(query,
            "SELECT oid, enumlabel "
            "FROM pg_catalog.pg_enum "
            "WHERE enumtypid = '%u'"
            "ORDER BY oid",
            tyinfo->dobj.catId.oid);

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    num = PQntuples(res);

    qtypname = gs_strdup(fmtId(tyinfo->dobj.name));

    /*
     * DROP must be fully qualified in case same name appears in pg_catalog.
     * CASCADE shouldn't be required here as for normal types since the I/O
     * functions are generic and do not get dropped.
     */
    appendPQExpBuffer(delq, "DROP TYPE %s%s.", if_exists, fmtId(tyinfo->dobj.nmspace->dobj.name));
    appendPQExpBuffer(delq, "%s%s;\n", qtypname, if_cascade);

    if (binary_upgrade)
        binary_upgrade_set_type_oids_by_type_oid(fout, q, tyinfo->dobj.catId.oid, false);

    appendPQExpBuffer(q, "CREATE TYPE %s AS ENUM (", qtypname);

    if (!binary_upgrade) {
        /* Labels with server-assigned oids */
        for (i = 0; i < num; i++) {
            field_num = PQfnumber(res, "enumlabel");
            label = PQgetvalue(res, i, field_num);
            if (i > 0)
                appendPQExpBuffer(q, ",");
            appendPQExpBuffer(q, "\n    ");
            appendStringLiteralAH(q, label, fout);
        }
    }

    appendPQExpBuffer(q, "\n);\n");

    if (binary_upgrade) {
        /* Labels with dump-assigned (preserved) oids */
        for (i = 0; i < num; i++) {
            enum_oid = atooid(PQgetvalue(res, i, PQfnumber(res, "oid")));
            label = PQgetvalue(res, i, PQfnumber(res, "enumlabel"));

            if (i == 0)
                appendPQExpBuffer(q, "\n-- For binary upgrade, must preserve pg_enum oids\n");
            appendPQExpBuffer(q, "SELECT binary_upgrade.set_next_pg_enum_oid('%u'::pg_catalog.oid);\n", enum_oid);
            appendPQExpBuffer(q, "ALTER TYPE %s.", fmtId(tyinfo->dobj.nmspace->dobj.name));
            appendPQExpBuffer(q, "%s ADD VALUE ", qtypname);
            appendStringLiteralAH(q, label, fout);
            appendPQExpBuffer(q, ";\n\n");
        }
    }

    appendPQExpBuffer(labelq, "TYPE %s", qtypname);

    if (binary_upgrade)
        binary_upgrade_extension_member(q, &tyinfo->dobj, labelq->data);

    ArchiveEntry(fout,
        tyinfo->dobj.catId,
        tyinfo->dobj.dumpId,
        tyinfo->dobj.name,
        tyinfo->dobj.nmspace->dobj.name,
        NULL,
        tyinfo->rolname,
        false,
        "TYPE",
        SECTION_PRE_DATA,
        q->data,
        delq->data,
        NULL,
        NULL,
        0,
        NULL,
        NULL);

    /* Dump Type Comments and Security Labels */
    dumpComment(fout,
        labelq->data,
        tyinfo->dobj.nmspace->dobj.name,
        tyinfo->rolname,
        tyinfo->dobj.catId,
        0,
        tyinfo->dobj.dumpId);
    dumpSecLabel(fout,
        labelq->data,
        tyinfo->dobj.nmspace->dobj.name,
        tyinfo->rolname,
        tyinfo->dobj.catId,
        0,
        tyinfo->dobj.dumpId);

    dumpACL(fout,
        tyinfo->dobj.catId,
        tyinfo->dobj.dumpId,
        "TYPE",
        qtypname,
        NULL,
        tyinfo->dobj.name,
        tyinfo->dobj.nmspace->dobj.name,
        tyinfo->rolname,
        tyinfo->typacl);

    PQclear(res);
    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
    destroyPQExpBuffer(labelq);
    destroyPQExpBuffer(query);
    free(qtypname);
    qtypname = NULL;
}

/*
 * dumpRangeType
 *	  writes out to fout the queries to recreate a user-defined range type
 */
static void dumpRangeType(Archive* fout, TypeInfo* tyinfo)
{
    PQExpBuffer q = createPQExpBuffer();
    PQExpBuffer delq = createPQExpBuffer();
    PQExpBuffer labelq = createPQExpBuffer();
    PQExpBuffer query = createPQExpBuffer();
    PGresult* res = NULL;
    Oid collationOid;
    char* qtypname = NULL;
    char* procname = NULL;

    /*
     * select appropriate schema to ensure names in CREATE are properly
     * qualified
     */
    selectSourceSchema(fout, tyinfo->dobj.nmspace->dobj.name);

    appendPQExpBuffer(query,
        "SELECT pg_catalog.format_type(rngsubtype, NULL) AS rngsubtype, "
        "opc.opcname AS opcname, "
        "(SELECT nspname FROM pg_catalog.pg_namespace nsp "
        "  WHERE nsp.oid = opc.opcnamespace) AS opcnsp, "
        "opc.opcdefault, "
        "CASE WHEN rngcollation = st.typcollation THEN 0 "
        "     ELSE rngcollation END AS collation, "
        "rngcanonical, rngsubdiff "
        "FROM pg_catalog.pg_range r, pg_catalog.pg_type st, "
        "     pg_catalog.pg_opclass opc "
        "WHERE st.oid = rngsubtype AND opc.oid = rngsubopc AND "
        "rngtypid = '%u'",
        tyinfo->dobj.catId.oid);

    res = ExecuteSqlQueryForSingleRow(fout, query->data);

    qtypname = gs_strdup(fmtId(tyinfo->dobj.name));

    /*
     * DROP must be fully qualified in case same name appears in pg_catalog.
     * CASCADE shouldn't be required here as for normal types since the I/O
     * functions are generic and do not get dropped.
     */
    appendPQExpBuffer(delq, "DROP TYPE %s%s.", if_exists, fmtId(tyinfo->dobj.nmspace->dobj.name));
    appendPQExpBuffer(delq, "%s%s;\n", qtypname, if_cascade);

    if (binary_upgrade)
        binary_upgrade_set_type_oids_by_type_oid(fout, q, tyinfo->dobj.catId.oid, false);

    appendPQExpBuffer(q, "CREATE TYPE %s AS RANGE (", qtypname);

    appendPQExpBuffer(q, "\n    subtype = %s", PQgetvalue(res, 0, PQfnumber(res, "rngsubtype")));

    /* print subtype_opclass only if not default for subtype */
    if (PQgetvalue(res, 0, PQfnumber(res, "opcdefault"))[0] != 't') {
        char* opcname = PQgetvalue(res, 0, PQfnumber(res, "opcname"));
        char* nspname = PQgetvalue(res, 0, PQfnumber(res, "opcnsp"));

        /* always schema-qualify, don't try to be smart */
        appendPQExpBuffer(q, ",\n    subtype_opclass = %s.", fmtId(nspname));
        appendPQExpBuffer(q, "%s", fmtId(opcname));
    }

    collationOid = atooid(PQgetvalue(res, 0, PQfnumber(res, "collation")));
    if (OidIsValid(collationOid)) {
        CollInfo* coll = findCollationByOid(collationOid);

        if (coll != NULL) {
            /* always schema-qualify, don't try to be smart */
            appendPQExpBuffer(q, ",\n    collation = %s.", fmtId(coll->dobj.nmspace->dobj.name));
            appendPQExpBuffer(q, "%s", fmtId(coll->dobj.name));
        }
    }

    procname = PQgetvalue(res, 0, PQfnumber(res, "rngcanonical"));
    if (strcmp(procname, "-") != 0)
        appendPQExpBuffer(q, ",\n    canonical = %s", procname);

    procname = PQgetvalue(res, 0, PQfnumber(res, "rngsubdiff"));
    if (strcmp(procname, "-") != 0)
        appendPQExpBuffer(q, ",\n    subtype_diff = %s", procname);

    appendPQExpBuffer(q, "\n);\n");

    appendPQExpBuffer(labelq, "TYPE %s", qtypname);

    if (binary_upgrade)
        binary_upgrade_extension_member(q, &tyinfo->dobj, labelq->data);

    ArchiveEntry(fout,
        tyinfo->dobj.catId,
        tyinfo->dobj.dumpId,
        tyinfo->dobj.name,
        tyinfo->dobj.nmspace->dobj.name,
        NULL,
        tyinfo->rolname,
        false,
        "TYPE",
        SECTION_PRE_DATA,
        q->data,
        delq->data,
        NULL,
        NULL,
        0,
        NULL,
        NULL);

    /* Dump Type Comments and Security Labels */
    dumpComment(fout,
        labelq->data,
        tyinfo->dobj.nmspace->dobj.name,
        tyinfo->rolname,
        tyinfo->dobj.catId,
        0,
        tyinfo->dobj.dumpId);
    dumpSecLabel(fout,
        labelq->data,
        tyinfo->dobj.nmspace->dobj.name,
        tyinfo->rolname,
        tyinfo->dobj.catId,
        0,
        tyinfo->dobj.dumpId);

    dumpACL(fout,
        tyinfo->dobj.catId,
        tyinfo->dobj.dumpId,
        "TYPE",
        qtypname,
        NULL,
        tyinfo->dobj.name,
        tyinfo->dobj.nmspace->dobj.name,
        tyinfo->rolname,
        tyinfo->typacl);

    free(qtypname);
    qtypname = NULL;
    PQclear(res);
    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
    destroyPQExpBuffer(labelq);
    destroyPQExpBuffer(query);
}

/*
 * dumpBaseType
 *	  writes out to fout the queries to recreate a user-defined base type
 */
static void dumpBaseType(Archive* fout, TypeInfo* tyinfo)
{
    PQExpBuffer q = createPQExpBuffer();
    PQExpBuffer delq = createPQExpBuffer();
    PQExpBuffer labelq = createPQExpBuffer();
    PQExpBuffer query = createPQExpBuffer();
    PGresult* res = NULL;
    char* qtypname = NULL;
    char* typlen = NULL;
    char* typinput = NULL;
    char* typoutput = NULL;
    char* typreceive = NULL;
    char* typsend = NULL;
    char* typmodin = NULL;
    char* typmodout = NULL;
    char* typanalyze = NULL;
    Oid typreceiveoid = 0;
    Oid typsendoid = 0;
    Oid typmodinoid = 0;
    Oid typmodoutoid = 0;
    Oid typanalyzeoid = 0;
    char* typcategory = NULL;
    char* typispreferred = NULL;
    char* typdelim = NULL;
    char* typbyval = NULL;
    char* typalign = NULL;
    char* typstorage = NULL;
    char* typcollatable = NULL;
    char* typdefault = NULL;
    bool typdefault_is_literal = false;

    /* Set proper schema search path so regproc references list correctly */
    selectSourceSchema(fout, tyinfo->dobj.nmspace->dobj.name);

    /* Fetch type-specific details */
    if (fout->remoteVersion >= 90100) {
        appendPQExpBuffer(query,
            "SELECT typlen, "
            "typinput, typoutput, typreceive, typsend, "
            "typmodin, typmodout, typanalyze, "
            "typreceive::pg_catalog.oid AS typreceiveoid, "
            "typsend::pg_catalog.oid AS typsendoid, "
            "typmodin::pg_catalog.oid AS typmodinoid, "
            "typmodout::pg_catalog.oid AS typmodoutoid, "
            "typanalyze::pg_catalog.oid AS typanalyzeoid, "
            "typcategory, typispreferred, "
            "typdelim, typbyval, typalign, typstorage, "
            "(typcollation <> 0) AS typcollatable, "
            "pg_catalog.pg_get_expr(typdefaultbin, 0) AS typdefaultbin, typdefault "
            "FROM pg_catalog.pg_type "
            "WHERE oid = '%u'::pg_catalog.oid",
            tyinfo->dobj.catId.oid);
    } else if (fout->remoteVersion >= 80400) {
        appendPQExpBuffer(query,
            "SELECT typlen, "
            "typinput, typoutput, typreceive, typsend, "
            "typmodin, typmodout, typanalyze, "
            "typreceive::pg_catalog.oid AS typreceiveoid, "
            "typsend::pg_catalog.oid AS typsendoid, "
            "typmodin::pg_catalog.oid AS typmodinoid, "
            "typmodout::pg_catalog.oid AS typmodoutoid, "
            "typanalyze::pg_catalog.oid AS typanalyzeoid, "
            "typcategory, typispreferred, "
            "typdelim, typbyval, typalign, typstorage, "
            "false AS typcollatable, "
            "pg_catalog.pg_get_expr(typdefaultbin, 0) AS typdefaultbin, typdefault "
            "FROM pg_catalog.pg_type "
            "WHERE oid = '%u'::pg_catalog.oid",
            tyinfo->dobj.catId.oid);
    } else if (fout->remoteVersion >= 80300) {
        /* Before 8.4, pg_get_expr does not allow 0 for its second arg */
        appendPQExpBuffer(query,
            "SELECT typlen, "
            "typinput, typoutput, typreceive, typsend, "
            "typmodin, typmodout, typanalyze, "
            "typreceive::pg_catalog.oid AS typreceiveoid, "
            "typsend::pg_catalog.oid AS typsendoid, "
            "typmodin::pg_catalog.oid AS typmodinoid, "
            "typmodout::pg_catalog.oid AS typmodoutoid, "
            "typanalyze::pg_catalog.oid AS typanalyzeoid, "
            "'U' AS typcategory, false AS typispreferred, "
            "typdelim, typbyval, typalign, typstorage, "
            "false AS typcollatable, "
            "pg_catalog.pg_get_expr(typdefaultbin, 'pg_catalog.pg_type'::pg_catalog.regclass) AS typdefaultbin, "
            "typdefault "
            "FROM pg_catalog.pg_type "
            "WHERE oid = '%u'::pg_catalog.oid",
            tyinfo->dobj.catId.oid);
    } else if (fout->remoteVersion >= 80000) {
        appendPQExpBuffer(query,
            "SELECT typlen, "
            "typinput, typoutput, typreceive, typsend, "
            "'-' AS typmodin, '-' AS typmodout, "
            "typanalyze, "
            "typreceive::pg_catalog.oid AS typreceiveoid, "
            "typsend::pg_catalog.oid AS typsendoid, "
            "0 AS typmodinoid, 0 AS typmodoutoid, "
            "typanalyze::pg_catalog.oid AS typanalyzeoid, "
            "'U' AS typcategory, false AS typispreferred, "
            "typdelim, typbyval, typalign, typstorage, "
            "false AS typcollatable, "
            "pg_catalog.pg_get_expr(typdefaultbin, 'pg_catalog.pg_type'::pg_catalog.regclass) AS typdefaultbin, "
            "typdefault "
            "FROM pg_catalog.pg_type "
            "WHERE oid = '%u'::pg_catalog.oid",
            tyinfo->dobj.catId.oid);
    } else if (fout->remoteVersion >= 70400) {
        appendPQExpBuffer(query,
            "SELECT typlen, "
            "typinput, typoutput, typreceive, typsend, "
            "'-' AS typmodin, '-' AS typmodout, "
            "'-' AS typanalyze, "
            "typreceive::pg_catalog.oid AS typreceiveoid, "
            "typsend::pg_catalog.oid AS typsendoid, "
            "0 AS typmodinoid, 0 AS typmodoutoid, "
            "0 AS typanalyzeoid, "
            "'U' AS typcategory, false AS typispreferred, "
            "typdelim, typbyval, typalign, typstorage, "
            "false AS typcollatable, "
            "pg_catalog.pg_get_expr(typdefaultbin, 'pg_catalog.pg_type'::pg_catalog.regclass) AS typdefaultbin, "
            "typdefault "
            "FROM pg_catalog.pg_type "
            "WHERE oid = '%u'::pg_catalog.oid",
            tyinfo->dobj.catId.oid);
    } else if (fout->remoteVersion >= 70300) {
        appendPQExpBuffer(query,
            "SELECT typlen, "
            "typinput, typoutput, "
            "'-' AS typreceive, '-' AS typsend, "
            "'-' AS typmodin, '-' AS typmodout, "
            "'-' AS typanalyze, "
            "0 AS typreceiveoid, 0 AS typsendoid, "
            "0 AS typmodinoid, 0 AS typmodoutoid, "
            "0 AS typanalyzeoid, "
            "'U' AS typcategory, false AS typispreferred, "
            "typdelim, typbyval, typalign, typstorage, "
            "false AS typcollatable, "
            "pg_catalog.pg_get_expr(typdefaultbin, 'pg_catalog.pg_type'::pg_catalog.regclass) AS typdefaultbin, "
            "typdefault "
            "FROM pg_catalog.pg_type "
            "WHERE oid = '%u'::pg_catalog.oid",
            tyinfo->dobj.catId.oid);
    } else if (fout->remoteVersion >= 70200) {
        /*
         * Note: although pre-7.3 catalogs contain typreceive and typsend,
         * ignore them because they are not right.
         */
        appendPQExpBuffer(query,
            "SELECT typlen, "
            "typinput, typoutput, "
            "'-' AS typreceive, '-' AS typsend, "
            "'-' AS typmodin, '-' AS typmodout, "
            "'-' AS typanalyze, "
            "0 AS typreceiveoid, 0 AS typsendoid, "
            "0 AS typmodinoid, 0 AS typmodoutoid, "
            "0 AS typanalyzeoid, "
            "'U' AS typcategory, false AS typispreferred, "
            "typdelim, typbyval, typalign, typstorage, "
            "false AS typcollatable, "
            "NULL AS typdefaultbin, typdefault "
            "FROM pg_type "
            "WHERE oid = '%u'::oid",
            tyinfo->dobj.catId.oid);
    } else if (fout->remoteVersion >= 70100) {
        /*
         * Ignore pre-7.2 typdefault; the field exists but has an unusable
         * representation.
         */
        appendPQExpBuffer(query,
            "SELECT typlen, "
            "typinput, typoutput, "
            "'-' AS typreceive, '-' AS typsend, "
            "'-' AS typmodin, '-' AS typmodout, "
            "'-' AS typanalyze, "
            "0 AS typreceiveoid, 0 AS typsendoid, "
            "0 AS typmodinoid, 0 AS typmodoutoid, "
            "0 AS typanalyzeoid, "
            "'U' AS typcategory, false AS typispreferred, "
            "typdelim, typbyval, typalign, typstorage, "
            "false AS typcollatable, "
            "NULL AS typdefaultbin, NULL AS typdefault "
            "FROM pg_type "
            "WHERE oid = '%u'::oid",
            tyinfo->dobj.catId.oid);
    } else {
        appendPQExpBuffer(query,
            "SELECT typlen, "
            "typinput, typoutput, "
            "'-' AS typreceive, '-' AS typsend, "
            "'-' AS typmodin, '-' AS typmodout, "
            "'-' AS typanalyze, "
            "0 AS typreceiveoid, 0 AS typsendoid, "
            "0 AS typmodinoid, 0 AS typmodoutoid, "
            "0 AS typanalyzeoid, "
            "'U' AS typcategory, false AS typispreferred, "
            "typdelim, typbyval, typalign, "
            "'p'::char AS typstorage, "
            "false AS typcollatable, "
            "NULL AS typdefaultbin, NULL AS typdefault "
            "FROM pg_type "
            "WHERE oid = '%u'::oid",
            tyinfo->dobj.catId.oid);
    }

    res = ExecuteSqlQueryForSingleRow(fout, query->data);

    typlen = PQgetvalue(res, 0, PQfnumber(res, "typlen"));
    typinput = PQgetvalue(res, 0, PQfnumber(res, "typinput"));
    typoutput = PQgetvalue(res, 0, PQfnumber(res, "typoutput"));
    typreceive = PQgetvalue(res, 0, PQfnumber(res, "typreceive"));
    typsend = PQgetvalue(res, 0, PQfnumber(res, "typsend"));
    typmodin = PQgetvalue(res, 0, PQfnumber(res, "typmodin"));
    typmodout = PQgetvalue(res, 0, PQfnumber(res, "typmodout"));
    typanalyze = PQgetvalue(res, 0, PQfnumber(res, "typanalyze"));
    typreceiveoid = atooid(PQgetvalue(res, 0, PQfnumber(res, "typreceiveoid")));
    typsendoid = atooid(PQgetvalue(res, 0, PQfnumber(res, "typsendoid")));
    typmodinoid = atooid(PQgetvalue(res, 0, PQfnumber(res, "typmodinoid")));
    typmodoutoid = atooid(PQgetvalue(res, 0, PQfnumber(res, "typmodoutoid")));
    typanalyzeoid = atooid(PQgetvalue(res, 0, PQfnumber(res, "typanalyzeoid")));
    typcategory = PQgetvalue(res, 0, PQfnumber(res, "typcategory"));
    typispreferred = PQgetvalue(res, 0, PQfnumber(res, "typispreferred"));
    typdelim = PQgetvalue(res, 0, PQfnumber(res, "typdelim"));
    typbyval = PQgetvalue(res, 0, PQfnumber(res, "typbyval"));
    typalign = PQgetvalue(res, 0, PQfnumber(res, "typalign"));
    typstorage = PQgetvalue(res, 0, PQfnumber(res, "typstorage"));
    typcollatable = PQgetvalue(res, 0, PQfnumber(res, "typcollatable"));
    if (!PQgetisnull(res, 0, PQfnumber(res, "typdefaultbin")))
        typdefault = PQgetvalue(res, 0, PQfnumber(res, "typdefaultbin"));
    else if (!PQgetisnull(res, 0, PQfnumber(res, "typdefault"))) {
        typdefault = PQgetvalue(res, 0, PQfnumber(res, "typdefault"));
        typdefault_is_literal = true; /* it needs quotes */
    } else
        typdefault = NULL;

    qtypname = gs_strdup(fmtId(tyinfo->dobj.name));

    /*
     * DROP must be fully qualified in case same name appears in pg_catalog.
     * The reason we include CASCADE is that the circular dependency between
     * the type and its I/O functions makes it impossible to drop the type any
     * other way.
     */
    appendPQExpBuffer(delq, "DROP TYPE %s%s.", if_exists, fmtId(tyinfo->dobj.nmspace->dobj.name));
    appendPQExpBuffer(delq, "%s CASCADE;\n", qtypname);

    /* We might already have a shell type, but setting pg_type_oid is harmless */
    if (binary_upgrade)
        binary_upgrade_set_type_oids_by_type_oid(fout, q, tyinfo->dobj.catId.oid, false);

    appendPQExpBuffer(q,
        "CREATE TYPE %s (\n"
        "    INTERNALLENGTH = %s",
        qtypname,
        (strcmp(typlen, "-1") == 0) ? "variable" : typlen);

    if (fout->remoteVersion >= 70300) {
        /* regproc result is correctly quoted as of 7.3 */
        appendPQExpBuffer(q, ",\n    INPUT = %s", typinput);
        appendPQExpBuffer(q, ",\n    OUTPUT = %s", typoutput);
        if (OidIsValid(typreceiveoid))
            appendPQExpBuffer(q, ",\n    RECEIVE = %s", typreceive);
        if (OidIsValid(typsendoid))
            appendPQExpBuffer(q, ",\n    SEND = %s", typsend);
        if (OidIsValid(typmodinoid))
            appendPQExpBuffer(q, ",\n    TYPMOD_IN = %s", typmodin);
        if (OidIsValid(typmodoutoid))
            appendPQExpBuffer(q, ",\n    TYPMOD_OUT = %s", typmodout);
        if (OidIsValid(typanalyzeoid))
            appendPQExpBuffer(q, ",\n    ANALYZE = %s", typanalyze);
    } else {
        /* regproc delivers an unquoted name before 7.3 */
        /* cannot combine these because fmtId uses static result area */
        appendPQExpBuffer(q, ",\n    INPUT = %s", fmtId(typinput));
        appendPQExpBuffer(q, ",\n    OUTPUT = %s", fmtId(typoutput));
        /* receive/send/typmodin/typmodout/analyze need not be printed */
    }

    if (strcmp(typcollatable, "t") == 0)
        appendPQExpBuffer(q, ",\n    COLLATABLE = true");

    if (typdefault != NULL) {
        appendPQExpBuffer(q, ",\n    DEFAULT = ");
        if (typdefault_is_literal)
            appendStringLiteralAH(q, typdefault, fout);
        else
            appendPQExpBufferStr(q, typdefault);
    }

    if (OidIsValid(tyinfo->typelem)) {
        char* elemType = NULL;

        /* reselect schema in case changed by function dump */
        selectSourceSchema(fout, tyinfo->dobj.nmspace->dobj.name);
        elemType = getFormattedTypeName(fout, tyinfo->typelem, zeroAsOpaque);
        appendPQExpBuffer(q, ",\n    ELEMENT = %s", elemType);
        free(elemType);
        elemType = NULL;
    }

    if (strcmp(typcategory, "U") != 0) {
        appendPQExpBuffer(q, ",\n    CATEGORY = ");
        appendStringLiteralAH(q, typcategory, fout);
    }

    if (strcmp(typispreferred, "t") == 0)
        appendPQExpBuffer(q, ",\n    PREFERRED = true");

    if ((typdelim != NULL) && strcmp(typdelim, ",") != 0) {
        appendPQExpBuffer(q, ",\n    DELIMITER = ");
        appendStringLiteralAH(q, typdelim, fout);
    }

    if (strcmp(typalign, "c") == 0)
        appendPQExpBuffer(q, ",\n    ALIGNMENT = char");
    else if (strcmp(typalign, "s") == 0)
        appendPQExpBuffer(q, ",\n    ALIGNMENT = int2");
    else if (strcmp(typalign, "i") == 0)
        appendPQExpBuffer(q, ",\n    ALIGNMENT = int4");
    else if (strcmp(typalign, "d") == 0)
        appendPQExpBuffer(q, ",\n    ALIGNMENT = double");

    if (strcmp(typstorage, "p") == 0)
        appendPQExpBuffer(q, ",\n    STORAGE = plain");
    else if (strcmp(typstorage, "e") == 0)
        appendPQExpBuffer(q, ",\n    STORAGE = external");
    else if (strcmp(typstorage, "x") == 0)
        appendPQExpBuffer(q, ",\n    STORAGE = extended");
    else if (strcmp(typstorage, "m") == 0)
        appendPQExpBuffer(q, ",\n    STORAGE = main");

    if (strcmp(typbyval, "t") == 0)
        appendPQExpBuffer(q, ",\n    PASSEDBYVALUE");

    appendPQExpBuffer(q, "\n);\n");

    appendPQExpBuffer(labelq, "TYPE %s", qtypname);

    if (binary_upgrade)
        binary_upgrade_extension_member(q, &tyinfo->dobj, labelq->data);

    ArchiveEntry(fout,
        tyinfo->dobj.catId,
        tyinfo->dobj.dumpId,
        tyinfo->dobj.name,
        tyinfo->dobj.nmspace->dobj.name,
        NULL,
        tyinfo->rolname,
        false,
        "TYPE",
        SECTION_PRE_DATA,
        q->data,
        delq->data,
        NULL,
        NULL,
        0,
        NULL,
        NULL);

    /* Dump Type Comments and Security Labels */
    dumpComment(fout,
        labelq->data,
        tyinfo->dobj.nmspace->dobj.name,
        tyinfo->rolname,
        tyinfo->dobj.catId,
        0,
        tyinfo->dobj.dumpId);
    dumpSecLabel(fout,
        labelq->data,
        tyinfo->dobj.nmspace->dobj.name,
        tyinfo->rolname,
        tyinfo->dobj.catId,
        0,
        tyinfo->dobj.dumpId);

    dumpACL(fout,
        tyinfo->dobj.catId,
        tyinfo->dobj.dumpId,
        "TYPE",
        qtypname,
        NULL,
        tyinfo->dobj.name,
        tyinfo->dobj.nmspace->dobj.name,
        tyinfo->rolname,
        tyinfo->typacl);

    PQclear(res);
    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
    destroyPQExpBuffer(labelq);
    destroyPQExpBuffer(query);
    free(qtypname);
    qtypname = NULL;
}

/*
 * dumpDomain
 *	  writes out to fout the queries to recreate a user-defined domain
 */
static void dumpDomain(Archive* fout, TypeInfo* tyinfo)
{
    PQExpBuffer q = createPQExpBuffer();
    PQExpBuffer delq = createPQExpBuffer();
    PQExpBuffer labelq = createPQExpBuffer();
    PQExpBuffer query = createPQExpBuffer();
    PGresult* res = NULL;
    int i = 0;
    char* qtypname = NULL;
    char* typnotnull = NULL;
    char* typdefn = NULL;
    char* typdefault = NULL;
    Oid typcollation;
    bool typdefault_is_literal = false;

    /* Set proper schema search path so type references list correctly */
    selectSourceSchema(fout, tyinfo->dobj.nmspace->dobj.name);

    /* Fetch domain specific details */
    if (fout->remoteVersion >= 90100) {
        /* typcollation is new in 9.1 */
        appendPQExpBuffer(query,
            "SELECT t.typnotnull, "
            "pg_catalog.format_type(t.typbasetype, t.typtypmod) AS typdefn, "
            "pg_catalog.pg_get_expr(t.typdefaultbin, 'pg_catalog.pg_type'::pg_catalog.regclass) AS typdefaultbin, "
            "t.typdefault, "
            "CASE WHEN t.typcollation <> u.typcollation "
            "THEN t.typcollation ELSE 0 END AS typcollation "
            "FROM pg_catalog.pg_type t "
            "LEFT JOIN pg_catalog.pg_type u ON (t.typbasetype = u.oid) "
            "WHERE t.oid = '%u'::pg_catalog.oid",
            tyinfo->dobj.catId.oid);
    } else {
        /* We assume here that remoteVersion must be at least 70300 */
        appendPQExpBuffer(query,
            "SELECT typnotnull, "
            "pg_catalog.format_type(typbasetype, typtypmod) AS typdefn, "
            "pg_catalog.pg_get_expr(typdefaultbin, 'pg_catalog.pg_type'::pg_catalog.regclass) AS typdefaultbin, "
            "typdefault, 0 AS typcollation "
            "FROM pg_catalog.pg_type "
            "WHERE oid = '%u'::pg_catalog.oid",
            tyinfo->dobj.catId.oid);
    }

    res = ExecuteSqlQueryForSingleRow(fout, query->data);

    typnotnull = PQgetvalue(res, 0, PQfnumber(res, "typnotnull"));
    typdefn = PQgetvalue(res, 0, PQfnumber(res, "typdefn"));
    if (!PQgetisnull(res, 0, PQfnumber(res, "typdefaultbin")))
        typdefault = PQgetvalue(res, 0, PQfnumber(res, "typdefaultbin"));
    else if (!PQgetisnull(res, 0, PQfnumber(res, "typdefault"))) {
        typdefault = PQgetvalue(res, 0, PQfnumber(res, "typdefault"));
        typdefault_is_literal = true; /* it needs quotes */
    } else
        typdefault = NULL;
    typcollation = atooid(PQgetvalue(res, 0, PQfnumber(res, "typcollation")));

    if (binary_upgrade)
        binary_upgrade_set_type_oids_by_type_oid(fout, q, tyinfo->dobj.catId.oid, false);

    qtypname = gs_strdup(fmtId(tyinfo->dobj.name));

    appendPQExpBuffer(q, "CREATE DOMAIN %s AS %s", qtypname, typdefn);

    /* Print collation only if different from base type's collation */
    if (OidIsValid(typcollation)) {
        CollInfo* coll = NULL;

        coll = findCollationByOid(typcollation);
        if (coll != NULL) {
            /* always schema-qualify, don't try to be smart */
            appendPQExpBuffer(q, " COLLATE %s.", fmtId(coll->dobj.nmspace->dobj.name));
            appendPQExpBuffer(q, "%s", fmtId(coll->dobj.name));
        }
    }

    if (typnotnull[0] == 't')
        appendPQExpBuffer(q, " NOT NULL");

    if (typdefault != NULL) {
        appendPQExpBuffer(q, " DEFAULT ");
        if (typdefault_is_literal)
            appendStringLiteralAH(q, typdefault, fout);
        else
            appendPQExpBufferStr(q, typdefault);
    }

    PQclear(res);

    /*
     * Add any CHECK constraints for the domain
     */
    for (i = 0; i < tyinfo->nDomChecks; i++) {
        ConstraintInfo* domcheck = &(tyinfo->domChecks[i]);

        if (!domcheck->separate)
            appendPQExpBuffer(q, "\n\tCONSTRAINT %s %s", fmtId(domcheck->dobj.name), domcheck->condef);
    }

    appendPQExpBuffer(q, ";\n");

    /*
     * DROP must be fully qualified in case same name appears in pg_catalog
     */
    appendPQExpBuffer(delq, "DROP DOMAIN %s.", fmtId(tyinfo->dobj.nmspace->dobj.name));
    appendPQExpBuffer(delq, "%s;\n", qtypname);

    appendPQExpBuffer(labelq, "DOMAIN %s", qtypname);

    if (binary_upgrade)
        binary_upgrade_extension_member(q, &tyinfo->dobj, labelq->data);

    ArchiveEntry(fout,
        tyinfo->dobj.catId,
        tyinfo->dobj.dumpId,
        tyinfo->dobj.name,
        tyinfo->dobj.nmspace->dobj.name,
        NULL,
        tyinfo->rolname,
        false,
        "DOMAIN",
        SECTION_PRE_DATA,
        q->data,
        delq->data,
        NULL,
        NULL,
        0,
        NULL,
        NULL);

    /* Dump Domain Comments and Security Labels */
    dumpComment(fout,
        labelq->data,
        tyinfo->dobj.nmspace->dobj.name,
        tyinfo->rolname,
        tyinfo->dobj.catId,
        0,
        tyinfo->dobj.dumpId);
    dumpSecLabel(fout,
        labelq->data,
        tyinfo->dobj.nmspace->dobj.name,
        tyinfo->rolname,
        tyinfo->dobj.catId,
        0,
        tyinfo->dobj.dumpId);

    dumpACL(fout,
        tyinfo->dobj.catId,
        tyinfo->dobj.dumpId,
        "TYPE",
        qtypname,
        NULL,
        tyinfo->dobj.name,
        tyinfo->dobj.nmspace->dobj.name,
        tyinfo->rolname,
        tyinfo->typacl);

    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
    destroyPQExpBuffer(labelq);
    destroyPQExpBuffer(query);
    free(qtypname);
    qtypname = NULL;
}

/*
 * dumpCompositeType
 *	  writes out to fout the queries to recreate a user-defined stand-alone
 *	  composite type
 */
static void dumpCompositeType(Archive* fout, TypeInfo* tyinfo)
{
    PQExpBuffer q = createPQExpBuffer();
    PQExpBuffer dropped = createPQExpBuffer();
    PQExpBuffer delq = createPQExpBuffer();
    PQExpBuffer labelq = createPQExpBuffer();
    PQExpBuffer query = createPQExpBuffer();
    PGresult* res = NULL;
    char* qtypname = NULL;
    int ntups = 0;
    int i_attname = 0;
    int i_atttypdefn = 0;
    int i_attlen = 0;
    int i_attalign = 0;
    int i_attisdropped = 0;
    int i_attcollation = 0;
    int i_typrelid = 0;
    int i = 0;
    int actual_atts = 0;

    /* Set proper schema search path so type references list correctly */
    selectSourceSchema(fout, tyinfo->dobj.nmspace->dobj.name);

    /* Fetch type specific details */
    if (fout->remoteVersion >= 90100) {
        /*
         * attcollation is new in 9.1.	Since we only want to dump COLLATE
         * clauses for attributes whose collation is different from their
         * type's default, we use a CASE here to suppress uninteresting
         * attcollations cheaply.  atttypid will be 0 for dropped columns;
         * collation does not matter for those.
         */
        appendPQExpBuffer(query,
            "SELECT a.attname, "
            "pg_catalog.format_type(a.atttypid, a.atttypmod) AS atttypdefn, "
            "a.attlen, a.attalign, a.attisdropped, "
            "CASE WHEN a.attcollation <> at.typcollation "
            "THEN a.attcollation ELSE 0::Oid END AS attcollation, "
            "ct.typrelid "
            "FROM pg_catalog.pg_type ct "
            "JOIN pg_catalog.pg_attribute a ON a.attrelid = ct.typrelid "
            "LEFT JOIN pg_catalog.pg_type at ON at.oid = a.atttypid "
            "WHERE ct.oid = '%u'::pg_catalog.oid "
            "ORDER BY a.attnum ",
            tyinfo->dobj.catId.oid);
    } else {
        /*
         * We assume here that remoteVersion must be at least 70300.  Since
         * ALTER TYPE could not drop columns until 9.1, attisdropped should
         * always be false.
         */
        appendPQExpBuffer(query,
            "SELECT a.attname, "
            "pg_catalog.format_type(a.atttypid, a.atttypmod) AS atttypdefn, "
            "a.attlen, a.attalign, a.attisdropped, "
            "0 AS attcollation, "
            "ct.typrelid "
            "FROM pg_catalog.pg_type ct, pg_catalog.pg_attribute a "
            "WHERE ct.oid = '%u'::pg_catalog.oid "
            "AND a.attrelid = ct.typrelid "
            "ORDER BY a.attnum ",
            tyinfo->dobj.catId.oid);
    }

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    ntups = PQntuples(res);

    i_attname = PQfnumber(res, "attname");
    i_atttypdefn = PQfnumber(res, "atttypdefn");
    i_attlen = PQfnumber(res, "attlen");
    i_attalign = PQfnumber(res, "attalign");
    i_attisdropped = PQfnumber(res, "attisdropped");
    i_attcollation = PQfnumber(res, "attcollation");
    i_typrelid = PQfnumber(res, "typrelid");

    if (binary_upgrade) {
        Oid typrelid = atooid(PQgetvalue(res, 0, i_typrelid));

        binary_upgrade_set_type_oids_by_type_oid(fout, q, tyinfo->dobj.catId.oid, false);
        binary_upgrade_set_pg_class_oids(fout, q, typrelid, false, false);
    }

    qtypname = gs_strdup(fmtId(tyinfo->dobj.name));

    appendPQExpBuffer(q, "CREATE TYPE %s AS (", qtypname);

    actual_atts = 0;
    for (i = 0; i < ntups; i++) {
        char* attname = NULL;
        char* atttypdefn = NULL;
        char* attlen = NULL;
        char* attalign = NULL;
        bool attisdropped = false;
        Oid attcollation;

        attname = PQgetvalue(res, i, i_attname);
        atttypdefn = PQgetvalue(res, i, i_atttypdefn);
        attlen = PQgetvalue(res, i, i_attlen);
        attalign = PQgetvalue(res, i, i_attalign);
        attisdropped = (PQgetvalue(res, i, i_attisdropped)[0] == 't');
        attcollation = atooid(PQgetvalue(res, i, i_attcollation));

        if (attisdropped && !binary_upgrade)
            continue;

        /* Format properly if not first attr */
        if (actual_atts++ > 0)
            appendPQExpBuffer(q, ",");
        appendPQExpBuffer(q, "\n\t");

        if (!attisdropped) {
            appendPQExpBuffer(q, "%s %s", fmtId(attname), atttypdefn);

            /* Add collation if not default for the column type */
            if (OidIsValid(attcollation)) {
                CollInfo* coll = NULL;

                coll = findCollationByOid(attcollation);
                if (NULL != coll) {
                    /* always schema-qualify, don't try to be smart */
                    appendPQExpBuffer(q, " COLLATE %s.", fmtId(coll->dobj.nmspace->dobj.name));
                    appendPQExpBuffer(q, "%s", fmtId(coll->dobj.name));
                }
            }
        } else {
            /*
             * This is a dropped attribute and we're in binary_upgrade mode.
             * Insert a placeholder for it in the CREATE TYPE command, and set
             * length and alignment with direct UPDATE to the catalogs
             * afterwards. See similar code in dumpTableSchema().
             */
            appendPQExpBuffer(q, "%s INTEGER /* dummy */", fmtId(attname));

            /* stash separately for insertion after the CREATE TYPE */
            appendPQExpBuffer(dropped, "\n-- For binary upgrade, recreate dropped column.\n");
            appendPQExpBuffer(dropped,
                "UPDATE pg_catalog.pg_attribute\n"
                "SET attlen = %s, "
                "attalign = '%s', attbyval = false\n"
                "WHERE attname = ",
                attlen,
                attalign);
            appendStringLiteralAH(dropped, attname, fout);
            appendPQExpBuffer(dropped, "\n  AND attrelid = ");
            appendStringLiteralAH(dropped, qtypname, fout);
            appendPQExpBuffer(dropped, "::pg_catalog.regclass;\n");

            appendPQExpBuffer(dropped, "ALTER TYPE %s ", qtypname);
            appendPQExpBuffer(dropped, "DROP ATTRIBUTE %s;\n", fmtId(attname));
        }
    }
    appendPQExpBuffer(q, "\n);\n");
    appendPQExpBufferStr(q, dropped->data);

    /*
     * DROP must be fully qualified in case same name appears in pg_catalog
     */
    appendPQExpBuffer(delq, "DROP TYPE %s%s.", if_exists, fmtId(tyinfo->dobj.nmspace->dobj.name));
    appendPQExpBuffer(delq, "%s%s;\n", qtypname, if_cascade);

    appendPQExpBuffer(labelq, "TYPE %s", qtypname);

    if (binary_upgrade)
        binary_upgrade_extension_member(q, &tyinfo->dobj, labelq->data);

    ArchiveEntry(fout,
        tyinfo->dobj.catId,
        tyinfo->dobj.dumpId,
        tyinfo->dobj.name,
        tyinfo->dobj.nmspace->dobj.name,
        NULL,
        tyinfo->rolname,
        false,
        "TYPE",
        SECTION_PRE_DATA,
        q->data,
        delq->data,
        NULL,
        NULL,
        0,
        NULL,
        NULL);

    /* Dump Type Comments and Security Labels */
    dumpComment(fout,
        labelq->data,
        tyinfo->dobj.nmspace->dobj.name,
        tyinfo->rolname,
        tyinfo->dobj.catId,
        0,
        tyinfo->dobj.dumpId);
    dumpSecLabel(fout,
        labelq->data,
        tyinfo->dobj.nmspace->dobj.name,
        tyinfo->rolname,
        tyinfo->dobj.catId,
        0,
        tyinfo->dobj.dumpId);

    dumpACL(fout,
        tyinfo->dobj.catId,
        tyinfo->dobj.dumpId,
        "TYPE",
        qtypname,
        NULL,
        tyinfo->dobj.name,
        tyinfo->dobj.nmspace->dobj.name,
        tyinfo->rolname,
        tyinfo->typacl);

    PQclear(res);
    destroyPQExpBuffer(q);
    destroyPQExpBuffer(dropped);
    destroyPQExpBuffer(delq);
    destroyPQExpBuffer(labelq);
    destroyPQExpBuffer(query);
    GS_FREE(qtypname);

    /* Dump any per-column comments */
    dumpCompositeTypeColComments(fout, tyinfo);
}

/*
 * dumpCompositeTypeColComments
 *	  writes out to fout the queries to recreate comments on the columns of
 *	  a user-defined stand-alone composite type
 */
static void dumpCompositeTypeColComments(Archive* fout, TypeInfo* tyinfo)
{
    CommentItem* comments = NULL;
    int ncomments;
    PGresult* res = NULL;
    PQExpBuffer query;
    PQExpBuffer target;
    Oid pgClassOid;
    int i;
    int ntups;
    int i_attname;
    int i_attnum;

    query = createPQExpBuffer();

    /* We assume here that remoteVersion must be at least 70300 */
    appendPQExpBuffer(query,
        "SELECT c.tableoid, a.attname, a.attnum "
        "FROM pg_catalog.pg_class c, pg_catalog.pg_attribute a "
        "WHERE c.oid = '%u' AND c.oid = a.attrelid "
        "  AND NOT a.attisdropped "
        "ORDER BY a.attnum ",
        tyinfo->typrelid);

    /* Fetch column attnames */
    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    ntups = PQntuples(res);
    if (ntups < 1) {
        PQclear(res);
        destroyPQExpBuffer(query);
        return;
    }

    pgClassOid = atooid(PQgetvalue(res, 0, PQfnumber(res, "tableoid")));

    /* Search for comments associated with type's pg_class OID */
    ncomments = findComments(fout, pgClassOid, tyinfo->typrelid, &comments);

    /* If no comments exist, we're done */
    if (ncomments <= 0) {
        PQclear(res);
        destroyPQExpBuffer(query);
        return;
    }

    /* Build COMMENT ON statements */
    target = createPQExpBuffer();

    i_attnum = PQfnumber(res, "attnum");
    i_attname = PQfnumber(res, "attname");
    while (ncomments > 0) {
        const char* attname = NULL;

        attname = NULL;
        for (i = 0; i < ntups; i++) {
            if (atoi(PQgetvalue(res, i, i_attnum)) == comments->objsubid) {
                attname = PQgetvalue(res, i, i_attname);
                break;
            }
        }
        if (attname != NULL) /* just in case we don't find it */
        {
            const char* descr = comments->descr;

            resetPQExpBuffer(target);
            appendPQExpBuffer(target, "COLUMN %s.", fmtId(tyinfo->dobj.name));
            appendPQExpBuffer(target, "%s", fmtId(attname));

            resetPQExpBuffer(query);
            appendPQExpBuffer(query, "COMMENT ON %s IS ", target->data);
            appendStringLiteralAH(query, descr, fout);
            appendPQExpBuffer(query, ";\n");

            ArchiveEntry(fout,
                nilCatalogId,
                createDumpId(),
                target->data,
                tyinfo->dobj.nmspace->dobj.name,
                NULL,
                tyinfo->rolname,
                false,
                "COMMENT",
                SECTION_NONE,
                query->data,
                "",
                NULL,
                &(tyinfo->dobj.dumpId),
                1,
                NULL,
                NULL);
        }

        comments++;
        ncomments--;
    }

    PQclear(res);
    destroyPQExpBuffer(query);
    destroyPQExpBuffer(target);
}

/*
 * dumpShellType
 *	  writes out to fout the queries to create a shell type
 *
 * We dump a shell definition in advance of the I/O functions for the type.
 */
static void dumpShellType(Archive* fout, ShellTypeInfo* stinfo)
{
    PQExpBuffer q;

    /* Skip if not to be dumped */
    if (!stinfo->dobj.dump || dataOnly)
        return;

    if (isExecUserNotObjectOwner(fout, stinfo->baseType->rolname))
        return;

    q = createPQExpBuffer();

    /*
     * Note the lack of a DROP command for the shell type; any required DROP
     * is driven off the base type entry, instead.	This interacts with
     * _printTocEntry()'s use of the presence of a DROP command to decide
     * whether an entry needs an ALTER OWNER command.  We don't want to alter
     * the shell type's owner immediately on creation; that should happen only
     * after it's filled in, otherwise the backend complains.
     */

    if (binary_upgrade)
        binary_upgrade_set_type_oids_by_type_oid(fout, q, stinfo->baseType->dobj.catId.oid, false);

    appendPQExpBuffer(q, "CREATE TYPE %s;\n", fmtId(stinfo->dobj.name));

    ArchiveEntry(fout,
        stinfo->dobj.catId,
        stinfo->dobj.dumpId,
        stinfo->dobj.name,
        stinfo->dobj.nmspace->dobj.name,
        NULL,
        stinfo->baseType->rolname,
        false,
        "SHELL TYPE",
        SECTION_PRE_DATA,
        q->data,
        "",
        NULL,
        NULL,
        0,
        NULL,
        NULL);

    destroyPQExpBuffer(q);
}

/*
 * Determine whether we want to dump definitions for procedural languages.
 * Since the languages themselves don't have schemas, we can't rely on
 * the normal schema-based selection mechanism.  We choose to dump them
 * whenever neither --schema nor --table was given.  (Before 8.1, we used
 * the dump flag of the PL's call handler function, but in 8.1 this will
 * probably always be false since call handlers are created in pg_catalog.)
 *
 * For some backwards compatibility with the older behavior, we forcibly
 * dump a PL if its handler function (and validator if any) are in a
 * dumpable nmspace.	That case is not checked here.
 *
 * Also, if the PL belongs to an extension, we do not use this heuristic.
 * That case isn't checked here either.
 */
static bool shouldDumpProcLangs(void)
{
    if (!include_everything)
        return false;
    /* And they're schema not data */
    if (dataOnly)
        return false;
    return true;
}

/*
 * dumpProcLang
 *		  writes out to fout the queries to recreate a user-defined
 *		  procedural language
 */
static void dumpProcLang(Archive* fout, ProcLangInfo* plang)
{
    PQExpBuffer defqry;
    PQExpBuffer delqry;
    PQExpBuffer labelq;
    bool useParams = false;
    char* qlanname = NULL;
    char* lanschema = NULL;
    FuncInfo* funcInfo = NULL;
    FuncInfo* inlineInfo = NULL;
    FuncInfo* validatorInfo = NULL;

    /* Skip if not to be dumped */
    if (!plang->dobj.dump || dataOnly)
        return;

    if (isExecUserNotObjectOwner(fout, plang->rolename))
        return;
    /*
     * Try to find the support function(s).  It is not an error if we don't
     * find them --- if the functions are in the pg_catalog schema, as is
     * standard in 8.1 and up, then we won't have loaded them. (In this case
     * we will emit a parameterless CREATE LANGUAGE command, which will
     * require PL template knowledge in the backend to reload.)
     */

    funcInfo = findFuncByOid(plang->lanplcallfoid);
    if (funcInfo != NULL && !funcInfo->dobj.dump)
        funcInfo = NULL; /* treat not-dumped same as not-found */

    if (OidIsValid(plang->laninline)) {
        inlineInfo = findFuncByOid(plang->laninline);
        if (inlineInfo != NULL && !inlineInfo->dobj.dump)
            inlineInfo = NULL;
    }

    if (OidIsValid(plang->lanvalidator)) {
        validatorInfo = findFuncByOid(plang->lanvalidator);
        if (validatorInfo != NULL && !validatorInfo->dobj.dump)
            validatorInfo = NULL;
    }

    /*
     * If the functions are dumpable then emit a traditional CREATE LANGUAGE
     * with parameters.  Otherwise, dump only if shouldDumpProcLangs() says to
     * dump it.
     *
     * However, for a language that belongs to an extension, we must not use
     * the shouldDumpProcLangs heuristic, but just dump the language iff we're
     * told to (via dobj.dump).  Generally the support functions will belong
     * to the same extension and so have the same dump flags ... if they
     * don't, this might not work terribly nicely.
     */
    useParams = (funcInfo != NULL && (inlineInfo != NULL || !OidIsValid(plang->laninline)) &&
                 (validatorInfo != NULL || !OidIsValid(plang->lanvalidator)));

    if (!plang->dobj.ext_member) {
        if (!useParams && !shouldDumpProcLangs())
            return;
    }

    defqry = createPQExpBuffer();
    delqry = createPQExpBuffer();
    labelq = createPQExpBuffer();

    qlanname = gs_strdup(fmtId(plang->dobj.name));

    /*
     * If dumping a HANDLER clause, treat the language as being in the handler
     * function's schema; this avoids cluttering the HANDLER clause. Otherwise
     * it doesn't really have a schema.
     */
    if (useParams)
        lanschema = funcInfo->dobj.nmspace->dobj.name;
    else
        lanschema = NULL;

    appendPQExpBuffer(delqry, "DROP PROCEDURAL LANGUAGE %s;\n", qlanname);

    if (useParams) {
        appendPQExpBuffer(defqry, "CREATE %sPROCEDURAL LANGUAGE %s", plang->lanpltrusted ? "TRUSTED " : "", qlanname);
        appendPQExpBuffer(defqry, " HANDLER %s", fmtId(funcInfo->dobj.name));
        if (OidIsValid(plang->laninline)) {
            appendPQExpBuffer(defqry, " INLINE ");
            /* Cope with possibility that inline is in different schema */
            if ((inlineInfo != NULL) && (funcInfo != NULL) && (inlineInfo->dobj.nmspace != funcInfo->dobj.nmspace)) {
                appendPQExpBuffer(defqry, "%s.", fmtId(inlineInfo->dobj.nmspace->dobj.name));
            }

            if (inlineInfo != NULL) {
                appendPQExpBuffer(defqry, "%s", fmtId(inlineInfo->dobj.name));
            }
        }

        if ((validatorInfo != NULL) && (funcInfo != NULL) && (OidIsValid(plang->lanvalidator))) {
            appendPQExpBuffer(defqry, " VALIDATOR ");

            /* Cope with possibility that validator is in different schema */
            if (validatorInfo->dobj.nmspace != funcInfo->dobj.nmspace)
                appendPQExpBuffer(defqry, "%s.", fmtId(validatorInfo->dobj.nmspace->dobj.name));
            appendPQExpBuffer(defqry, "%s", fmtId(validatorInfo->dobj.name));
        }
    } else {
        /*
         * If not dumping parameters, then use CREATE OR REPLACE so that the
         * command will not fail if the language is preinstalled in the target
         * database.  We restrict the use of REPLACE to this case so as to
         * eliminate the risk of replacing a language with incompatible
         * parameter settings: this command will only succeed at all if there
         * is a pg_pltemplate entry, and if there is one, the existing entry
         * must match it too.
         */
        appendPQExpBuffer(defqry, "CREATE OR REPLACE PROCEDURAL LANGUAGE %s", qlanname);
    }
    appendPQExpBuffer(defqry, ";\n");

    appendPQExpBuffer(labelq, "LANGUAGE %s", qlanname);

    if (binary_upgrade)
        binary_upgrade_extension_member(defqry, &plang->dobj, labelq->data);

    ArchiveEntry(fout,
        plang->dobj.catId,
        plang->dobj.dumpId,
        plang->dobj.name,
        lanschema,
        NULL,
        plang->rolename,
        false,
        "PROCEDURAL LANGUAGE",
        SECTION_PRE_DATA,
        defqry->data,
        delqry->data,
        NULL,
        NULL,
        0,
        NULL,
        NULL);

    /* Dump Proc Lang Comments and Security Labels */
    dumpComment(fout, labelq->data, NULL, "", plang->dobj.catId, 0, plang->dobj.dumpId);
    dumpSecLabel(fout, labelq->data, NULL, "", plang->dobj.catId, 0, plang->dobj.dumpId);

    if (plang->lanpltrusted)
        dumpACL(fout,
            plang->dobj.catId,
            plang->dobj.dumpId,
            "LANGUAGE",
            qlanname,
            NULL,
            plang->dobj.name,
            lanschema,
            plang->rolename,
            plang->lanacl);

    free(qlanname);
    qlanname = NULL;

    destroyPQExpBuffer(defqry);
    destroyPQExpBuffer(delqry);
    destroyPQExpBuffer(labelq);
}

/*
 * format_function_arguments: generate function name and argument list
 *
 * This is used when we can rely on pg_get_function_arguments to format
 * the argument list.
 */
static char* format_function_arguments(FuncInfo* finfo, char* funcargs)
{
    PQExpBufferData fn;

    initPQExpBuffer(&fn);
    appendPQExpBuffer(&fn, "%s(%s)", fmtId(finfo->dobj.name), funcargs);
    return fn.data;
}

/*
 * format_function_arguments_old: generate function name and argument list
 *
 * The argument type names are qualified if needed.  The function name
 * is never qualified.
 *
 * This is used only with pre-8.4 servers, so we aren't expecting to see
 * VARIADIC or TABLE arguments, nor are there any defaults for arguments.
 *
 * Any or all of allargtypes, argmodes, argnames may be NULL.
 */
static char* format_function_arguments_old(Archive* fout, FuncInfo* finfo, int nallargs, const char** allargtypes,
    const char** argmodes, const char** argnames)
{
    PQExpBufferData fn;
    int j = 0;

    initPQExpBuffer(&fn);
    appendPQExpBuffer(&fn, "%s(", fmtId(finfo->dobj.name));
    for (j = 0; j < nallargs; j++) {
        Oid typid = 0;
        char* typname = NULL;
        const char* argmode = NULL;
        const char* argname = NULL;

        typid = allargtypes != NULL ? atooid(allargtypes[j]) : finfo->argtypes[j];
        typname = getFormattedTypeName(fout, typid, zeroAsOpaque);

        if (NULL != argmodes) {
            switch (argmodes[j][0]) {
                case PROARGMODE_IN:
                    argmode = "";
                    break;
                case PROARGMODE_OUT:
                    argmode = "OUT ";
                    break;
                case PROARGMODE_INOUT:
                    argmode = "INOUT ";
                    break;
                default:
                    write_msg(NULL, "WARNING: bogus value in proargmodes array\n");
                    argmode = "";
                    break;
            }
        } else
            argmode = "";

        argname = argnames != NULL ? argnames[j] : (char*)NULL;
        if (NULL != argname && argname[0] == '\0')
            argname = NULL;

        appendPQExpBuffer(&fn,
            "%s%s%s%s%s",
            (j > 0) ? ", " : "",
            argmode,
            argname != NULL ? fmtId(argname) : "",
            argname != NULL ? " " : "",
            typname);
        free(typname);
        typname = NULL;
    }
    appendPQExpBuffer(&fn, ")");
    return fn.data;
}

/*
 * format_function_signature: generate function name and argument list
 *
 * This is like format_function_arguments_old except that only a minimal
 * list of input argument types is generated; this is sufficient to
 * reference the function, but not to define it.
 *
 * If honor_quotes is false then the function name is never quoted.
 * This is appropriate for use in TOC tags, but not in SQL commands.
 */
static char* format_function_signature(Archive* fout, FuncInfo* finfo, bool honor_quotes)
{
    PQExpBufferData fn;
    int j = 0;

    initPQExpBuffer(&fn);
    if (honor_quotes)
        appendPQExpBuffer(&fn, "%s(", fmtId(finfo->dobj.name));
    else
        appendPQExpBuffer(&fn, "%s(", finfo->dobj.name);
    for (j = 0; j < finfo->nargs; j++) {
        char* typname = NULL;

        typname = getFormattedTypeName(fout, finfo->argtypes[j], zeroAsOpaque);

        appendPQExpBuffer(&fn, "%s%s", (j > 0) ? ", " : "", typname);
        free(typname);
        typname = NULL;
    }
    appendPQExpBuffer(&fn, ")");
    return fn.data;
}

/*
 * dumpFunc:
 *	  dump out one function
 */
static void dumpFunc(Archive* fout, FuncInfo* finfo)
{
    PQExpBuffer query;
    PQExpBuffer definerquery;
    PQExpBuffer q;
    PQExpBuffer delqry;
    PQExpBuffer labelq;
    PQExpBuffer asPart;
    PQExpBuffer headWithDefault;
    PQExpBuffer headWithoutDefault;
    PGresult* res = NULL;
    PGresult* defres = NULL;
    char* funcsig = NULL;     /* identity signature */
    char* funcfullsig = NULL; /* full signature */
    char* funcsig_tag = NULL;
    char* proretset = NULL;
    char* prosrc = NULL;
    char* proargsrc = NULL;
    char* probin = NULL;
    char* funcargs = NULL;
    char* funciargs = NULL;
    char* funcresult = NULL;
    char* proallargtypes = NULL;
    char* proargmodes = NULL;
    char* proargnames = NULL;
    char* proiswindow = NULL;
    char* provolatile = NULL;
    char* proisstrict = NULL;
    char* prosecdef = NULL;
    char* proleakproof = NULL;
    char* proconfig = NULL;
    char* procost = NULL;
    char* prorows = NULL;
    char* lanname = NULL;
    char* selfloop = NULL;
    char* fencedmode = NULL; /*Fenced UDF add*/
    char* proKind = NULL;
    char* proshippable = NULL;
    char* propackage = NULL;
    char* rettypename = NULL;
    char* propackageid = NULL;
    char* definer = NULL;
    int nallargs = 0;
    char** allargtypes = NULL;
    char** argmodes = NULL;
    char** argnames = NULL;
    char** configitems = NULL;
    int nconfigitems = 0;
    int i = 0;
    bool isHasFencedmode = false;
    bool isHasProshippable = false;
    bool isHasPropackage = false;
    bool hasProKindAttr = false;
    bool isProcedure = false;
    bool hasProargsrc = false;
    bool isNullProargsrc = false;
    bool addDelimiter = false;
    bool isNullSelfloop = false;
    const char *funcKind;
    ArchiveHandle* AH = (ArchiveHandle*)fout;

    /* Skip if not to be dumped */
    if (!finfo->dobj.dump || dataOnly)
        return;

    if (isExecUserNotObjectOwner(fout, finfo->rolname))
        return;

    query = createPQExpBuffer();
    q = createPQExpBuffer();
    delqry = createPQExpBuffer();
    labelq = createPQExpBuffer();
    asPart = createPQExpBuffer();
    definerquery = createPQExpBuffer();
    headWithDefault = createPQExpBuffer();
    headWithoutDefault = createPQExpBuffer();

    /* Set proper schema search path so type references list correctly */
    selectSourceSchema(fout, finfo->dobj.nmspace->dobj.name);

    isHasFencedmode = is_column_exists(AH->connection, ProcedureRelationId, "fencedmode");
    isHasProshippable = is_column_exists(AH->connection, ProcedureRelationId, "proshippable");
    isHasPropackage = is_column_exists(AH->connection, ProcedureRelationId, "propackage");
    hasProKindAttr = is_column_exists(AH->connection, ProcedureRelationId, "prokind");
    hasProargsrc = is_column_exists(AH->connection, ProcedureRelationId, "proargsrc");

    /*
     * proleakproof was added at v9.2
     */
    appendPQExpBuffer(query,
        "SELECT proretset, prosrc, probin, "
        "pg_catalog.pg_get_function_arguments(oid) AS funcargs, "
        "pg_catalog.pg_get_function_identity_arguments(oid) AS funciargs, "
        "pg_catalog.pg_get_function_result(oid) AS funcresult, "
        "proiswindow, provolatile, proisstrict, prosecdef, "
        "proleakproof, proconfig, procost, prorows, propackageid, proowner,"
        "%s, "
        "%s, "
        "%s, "
        "%s, "
        "(SELECT lanname FROM pg_catalog.pg_language WHERE oid = prolang) AS lanname, "
        "%s, "
        "(SELECT 1 FROM pg_depend WHERE objid = oid AND objid = refobjid AND refclassid = 1255 LIMIT 1) AS selfloop "
        "FROM pg_catalog.pg_proc "
        "WHERE oid = '%u'::pg_catalog.oid",
        isHasFencedmode ? "fencedmode" : "NULL AS fencedmode",
        isHasProshippable ? "proshippable" : "NULL AS proshippable",
        isHasPropackage ? "propackage" : "NULL AS propackage",
        hasProKindAttr?"prokind":"'f' as prokind",
        hasProargsrc ? "proargsrc" : "NULL AS proargsrc",
        finfo->dobj.catId.oid);

    res = ExecuteSqlQueryForSingleRow(fout, query->data);
    proretset = PQgetvalue(res, 0, PQfnumber(res, "proretset"));
    prosrc = PQgetvalue(res, 0, PQfnumber(res, "prosrc"));
    proargsrc = PQgetvalue(res, 0, PQfnumber(res, "proargsrc"));
    probin = PQgetvalue(res, 0, PQfnumber(res, "probin"));
    funcargs = PQgetvalue(res, 0, PQfnumber(res, "funcargs"));
    funciargs = PQgetvalue(res, 0, PQfnumber(res, "funciargs"));
    funcresult = PQgetvalue(res, 0, PQfnumber(res, "funcresult"));
    proallargtypes = proargmodes = proargnames = NULL;
    proiswindow = PQgetvalue(res, 0, PQfnumber(res, "proiswindow"));
    provolatile = PQgetvalue(res, 0, PQfnumber(res, "provolatile"));
    proisstrict = PQgetvalue(res, 0, PQfnumber(res, "proisstrict"));
    prosecdef = PQgetvalue(res, 0, PQfnumber(res, "prosecdef"));
    proleakproof = PQgetvalue(res, 0, PQfnumber(res, "proleakproof"));
    proconfig = PQgetvalue(res, 0, PQfnumber(res, "proconfig"));
    procost = PQgetvalue(res, 0, PQfnumber(res, "procost"));
    prorows = PQgetvalue(res, 0, PQfnumber(res, "prorows"));
    selfloop = PQgetvalue(res, 0, PQfnumber(res, "selfloop"));
    lanname = PQgetvalue(res, 0, PQfnumber(res, "lanname"));
    fencedmode = PQgetvalue(res, 0, PQfnumber(res, "fencedmode"));
    proshippable = PQgetvalue(res, 0, PQfnumber(res, "proshippable"));
    propackage = PQgetvalue(res, 0, PQfnumber(res, "propackage"));
    propackageid = PQgetvalue(res, 0, PQfnumber(res, "propackageid"));

    if ((gdatcompatibility != NULL) && strcmp(gdatcompatibility, B_FORMAT) == 0) {
        /* get definer user name */
        appendPQExpBuffer(definerquery, "SELECT  rolname from  pg_authid where oid=%s", PQgetvalue(res, 0, PQfnumber(res, "proowner")));
        defres = ExecuteSqlQueryForSingleRow(fout, definerquery->data);
        definer =  PQgetvalue(defres, 0, PQfnumber(defres, "rolname"));
    }

    /* get is defined using delimiter with mysql format */
    if (pg_strncasecmp(prosrc, BEGIN_P_STR, BEGIN_P_LEN) == 0 ) {
        addDelimiter = true;
        errno_t rc = memcpy_s((char*)prosrc, strlen(prosrc), BEGIN_N_STR, BEGIN_P_LEN);
        securec_check_c(rc, "\0", "\0");
    }

    if (propackageid != NULL) {
        if (strcmp(propackageid, "0") != 0) {
            PQclear(res);
            destroyPQExpBuffer(query);
            destroyPQExpBuffer(q);
            destroyPQExpBuffer(delqry);
            destroyPQExpBuffer(labelq);
            destroyPQExpBuffer(asPart);
            destroyPQExpBuffer(definerquery);
            destroyPQExpBuffer(headWithDefault);
            destroyPQExpBuffer(headWithoutDefault);
            return;
        }
    }

    if (hasProKindAttr) {
        proKind = PQgetvalue(res, 0, PQfnumber(res, "prokind"));
        if (proKind != NULL) {
            isProcedure = proKind[0] == PROKIND_PROCEDURE;
        }
    }

    isNullProargsrc = (proargsrc == NULL || proargsrc[0] == '\0');

    /*
     * See backend/commands/functioncmds.c for details of how the 'AS' clause
     * is used.  In 8.4 and up, an unused probin is NULL (here ""); previous
     * versions would set it to "-".  There are no known cases in which prosrc
     * is unused, so the tests below for "-" are probably useless.
     */
    if (probin[0] != '\0' && strcmp(probin, "-") != 0) {
        if (!addDelimiter)
            appendPQExpBuffer(asPart, "AS ");
        appendStringLiteralAH(asPart, probin, fout);
        if (strcmp(prosrc, "-") != 0) {
            appendPQExpBuffer(asPart, ", ");

            /*
             * where we have bin, use dollar quoting if allowed and src
             * contains quote or backslash; else use regular quoting.
             */
            /* procedure follows Oracle style, without any quoting */
            if (isProcedure) {
                appendPQExpBuffer(asPart, "%s", prosrc);
            } else if (disable_dollar_quoting || (strchr(prosrc, '\'') == NULL && strchr(prosrc, '\\') == NULL)) {
                appendStringLiteralAH(asPart, prosrc, fout);
            } else {
                if (addDelimiter)
                    appendPQExpBuffer(asPart, "%s", prosrc);
                else
                    appendStringLiteralDQ(asPart, prosrc, NULL);
            }
        }
    } else {
        if (strcmp(prosrc, "-") != 0) {
            if (!addDelimiter)
                appendPQExpBuffer(asPart, "AS ");

            /* procedure follows Oracle style, without any quoting */
            if (isProcedure || !isNullProargsrc) {
                appendPQExpBuffer(asPart, "%s", prosrc);
            } else if (disable_dollar_quoting) {
                appendStringLiteralAH(asPart, prosrc, fout);
            } else {
                if (addDelimiter)
                    appendPQExpBuffer(asPart, "%s", prosrc);
                else
                    appendStringLiteralDQ(asPart, prosrc, NULL);
            }
        }
    }

    nallargs = finfo->nargs; /* unless we learn different from allargs */

    if ((proallargtypes != NULL) && *proallargtypes) {
        int nitems = 0;

        if (!parsePGArray(proallargtypes, &allargtypes, &nitems) || nitems < finfo->nargs) {
            write_msg(NULL, "WARNING: could not parse proallargtypes array\n");
            GS_FREE(allargtypes);
        } else
            nallargs = nitems;
    }

    if ((proargmodes != NULL) && *proargmodes) {
        int nitems = 0;

        if (!parsePGArray(proargmodes, &argmodes, &nitems) || nitems != nallargs) {
            write_msg(NULL, "WARNING: could not parse proargmodes array\n");
            GS_FREE(argmodes);
        }
    }

    if ((proargnames != NULL) && *proargnames) {
        int nitems = 0;

        if (!parsePGArray(proargnames, &argnames, &nitems) || nitems != nallargs) {
            write_msg(NULL, "WARNING: could not parse proargnames array\n");
            GS_FREE(argnames);
        }
    }

    if ((proconfig != NULL) && *proconfig) {
        if (!parsePGArray(proconfig, &configitems, &nconfigitems)) {
            write_msg(NULL, "WARNING: could not parse proconfig array\n");
            GS_FREE(configitems);
            nconfigitems = 0;
        }
    }

    if (NULL != funcargs) {
        /* 8.4 or later; we rely on server-side code for most of the work */
        funcfullsig = format_function_arguments(finfo, isNullProargsrc ? funcargs : proargsrc);
        funcsig = format_function_arguments(finfo, funciargs);
    } else {
        /* pre-8.4, do it ourselves */
        funcsig = format_function_arguments_old(
            fout, finfo, nallargs, (const char**)allargtypes, (const char**)argmodes, (const char**)argnames);
        funcfullsig = funcsig;
    }

    funcsig_tag = format_function_signature(fout, finfo, false);

    if (isProcedure) {
        funcKind = "PROCEDURE";
    } else {
        funcKind = "FUNCTION";
    }

    /*
     * DROP must be fully qualified in case same name appears in pg_catalog
     */
    appendPQExpBuffer(delqry, "DROP %s IF EXISTS %s.%s%s;\n", funcKind,
                      fmtId(finfo->dobj.nmspace->dobj.name), funcsig, if_cascade);

    isNullSelfloop = (selfloop == NULL || selfloop[0] == '\0');

    if (!isNullSelfloop) {
        if (addDelimiter && IsPlainFormat()) {
            appendPQExpBuffer(headWithoutDefault, "delimiter //\n");
        }

        if ((gdatcompatibility != NULL) && strcmp(gdatcompatibility, B_FORMAT) == 0) {
            appendPQExpBuffer(headWithoutDefault, "CREATE DEFINER = \"%s\" %s %s ", definer, funcKind, funcsig);
        } else {
            appendPQExpBuffer(headWithoutDefault, "CREATE %s %s ", funcKind, funcsig);
        }
    }

    if (addDelimiter && IsPlainFormat()) {
        appendPQExpBuffer(headWithDefault, "delimiter //\n");
    }

    if ((gdatcompatibility != NULL) && strcmp(gdatcompatibility, B_FORMAT) == 0) {
        if (isNullSelfloop)
            appendPQExpBuffer(headWithDefault, "CREATE DEFINER = \"%s\" %s %s ", definer, funcKind, funcfullsig);
        else
            appendPQExpBuffer(headWithDefault, "CREATE OR REPLACE %s %s ", funcKind, funcfullsig);
        PQclear(defres);
    } else {
        if (isNullSelfloop)
            appendPQExpBuffer(headWithDefault, "CREATE %s %s ", funcKind, funcfullsig);
        else
            appendPQExpBuffer(headWithDefault, "CREATE OR REPLACE %s %s ", funcKind, funcfullsig);
    }
    
    
    if (isProcedure) {
        /* For procedure, no return type, do nothing */
    } else if (funcresult != NULL) {
        if (!isNullProargsrc && !addDelimiter) {
            appendPQExpBuffer(q, "RETURN %s", funcresult);
        } else {
            appendPQExpBuffer(q, "RETURNS %s", funcresult);
        }
    } else {
        rettypename = getFormattedTypeName(fout, finfo->prorettype, zeroAsOpaque);
        appendPQExpBuffer(q, "RETURNS %s%s", (proretset[0] == 't') ? "SETOF " : "", rettypename);
        GS_FREE(rettypename);
    }

    /* No need to specify language for procedure */
    if ((!isProcedure) && isNullProargsrc) {
        appendPQExpBuffer(q, "\n    LANGUAGE %s", fmtId(lanname));
    }

    if (proiswindow[0] == 't')
        appendPQExpBuffer(q, " WINDOW");

    if (provolatile[0] != PROVOLATILE_VOLATILE) {
        if (provolatile[0] == PROVOLATILE_IMMUTABLE)
            appendPQExpBuffer(q, " IMMUTABLE");
        else if (provolatile[0] == PROVOLATILE_STABLE)
            appendPQExpBuffer(q, " STABLE");
        else if (provolatile[0] != PROVOLATILE_VOLATILE)
            exit_horribly(NULL, "unrecognized provolatile value for function \"%s\"\n", finfo->dobj.name);
    }

    if (isHasProshippable && (isProcedure || !addDelimiter)) {
        if (proshippable[0] == 't') {
            appendPQExpBuffer(q, " SHIPPABLE");
        } else {
            appendPQExpBuffer(q, " NOT SHIPPABLE");
        }
    }

    if (isHasPropackage && (propackage[0] == 't')) {
        appendPQExpBuffer(q, " PACKAGE");
    }

    if (proisstrict[0] == 't')
        appendPQExpBuffer(q, " STRICT");

    /* only language c can support FENCED/NOT FENCED */
    if (((int)strlen(lanname) - 1) == 0 && strcmp(lanname, "c") == 0) {
        if (isHasFencedmode && fencedmode[0] == 't')
            appendPQExpBuffer(q, " FENCED");
        else
            appendPQExpBuffer(q, " NOT FENCED");
    }

    if (prosecdef[0] == 't')
        appendPQExpBuffer(q, " SECURITY DEFINER");

    if (proleakproof[0] == 't')
        appendPQExpBuffer(q, " LEAKPROOF");

    /*
     * COST and ROWS are emitted only if present and not default, so as not to
     * break backwards-compatibility of the dump without need.	Keep this code
     * in sync with the defaults in functioncmds.c.
     */
    if (strcmp(procost, "0") != 0) {
        if (strcmp(lanname, "internal") == 0 || strcmp(lanname, "c") == 0) {
            /* default cost is 1 */
            if (strcmp(procost, "1") != 0)
                appendPQExpBuffer(q, " COST %s", procost);
        } else {
            /* default cost is 100 */
            if (strcmp(procost, "100") != 0)
                appendPQExpBuffer(q, " COST %s", procost);
        }
    }
    if (proretset[0] == 't' && strcmp(prorows, "0") != 0 && strcmp(prorows, "1000") != 0)
        appendPQExpBuffer(q, " ROWS %s", prorows);

    for (i = 0; i < nconfigitems; i++) {
        /* we feel free to scribble on configitems[] here */
        char* configitem = configitems[i];
        char* pos = NULL;

        pos = strchr(configitem, '=');
        if (pos == NULL)
            continue;
        *pos++ = '\0';
        appendPQExpBuffer(q, "\n    SET %s TO ", fmtId(configitem));

        /*
         * Some GUC variable names are 'LIST' type and hence must not be
         * quoted.
         */
        if (pg_strcasecmp(configitem, "u_sess->time_cxt.DateStyle") == 0 ||
            pg_strcasecmp(configitem, "search_path") == 0)
            appendPQExpBuffer(q, "%s", pos);
        else
            appendStringLiteralAH(q, pos, fout);
    }

    /* add slash at the end for a procedure */
    if (!fout->encryptfile && (pg_strcasecmp(format, "plain") == 0 || 
        pg_strcasecmp(format, "p") == 0 || pg_strcasecmp(format, "a") == 0
        || pg_strcasecmp(format, "append") == 0)) {
        if (addDelimiter && IsPlainFormat()) {
            appendPQExpBuffer(q, "\n %s;\n%s", asPart->data, "//\n");
        } else {
            appendPQExpBuffer(q, "\n %s;\n%s", asPart->data, (isProcedure || (!isNullProargsrc)) ? "/\n" : "");
        }
    } else {
         if (addDelimiter && IsPlainFormat()) {
            appendPQExpBuffer(q, "\n %s;\n%s", asPart->data, "//\n");
        } else {
            appendPQExpBuffer(q, "\n %s;\n%s", asPart->data, (isProcedure || (!isNullProargsrc)) ? "\n" : "");
        }
    }

    /*
     * since COMMENT ON PROCEDURE and ALTER EXTENSION ADD PROCEDURE are not valid in grammar
     * COMMENT ON/ALTER EXTENSION ADD, we should use FUNCTION instead.
     */
    appendPQExpBuffer(labelq, "%s %s\n", "FUNCTION", funcsig);

    if (binary_upgrade)
        binary_upgrade_extension_member(q, &finfo->dobj, labelq->data);

    if (addDelimiter && IsPlainFormat()) {
        appendPQExpBuffer(q, "delimiter ;\n");
    }

    if (isNullSelfloop) {
        appendPQExpBuffer(headWithDefault, "%s", q->data);
    } else {
        appendPQExpBuffer(headWithoutDefault, "%s\n%s%s", q->data, headWithDefault->data, q->data);
    }

    ArchiveEntry(fout,
        finfo->dobj.catId,
        finfo->dobj.dumpId,
        funcsig_tag,
        finfo->dobj.nmspace->dobj.name,
        NULL,
        finfo->rolname,
        false,
        funcKind,
        SECTION_PRE_DATA,
        isNullSelfloop ? headWithDefault->data : headWithoutDefault->data,
        delqry->data,
        NULL,
        NULL,
        0,
        NULL,
        NULL);

    /* Dump Function Comments and Security Labels */
    dumpComment(
        fout, labelq->data, finfo->dobj.nmspace->dobj.name, finfo->rolname, finfo->dobj.catId, 0, finfo->dobj.dumpId);
    dumpSecLabel(
        fout, labelq->data, finfo->dobj.nmspace->dobj.name, finfo->rolname, finfo->dobj.catId, 0, finfo->dobj.dumpId);

    dumpACL(fout,
        finfo->dobj.catId,
        finfo->dobj.dumpId,
        funcKind,
        funcsig,
        NULL,
        funcsig_tag,
        finfo->dobj.nmspace->dobj.name,
        finfo->rolname,
        finfo->proacl);

    PQclear(res);

    destroyPQExpBuffer(query);
    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delqry);
    destroyPQExpBuffer(labelq);
    destroyPQExpBuffer(asPart);
    destroyPQExpBuffer(definerquery);
    destroyPQExpBuffer(headWithDefault);
    destroyPQExpBuffer(headWithoutDefault);

    GS_FREE(funcsig);
    GS_FREE(funcfullsig);
    GS_FREE(funcsig_tag);
    GS_FREE(allargtypes);
    GS_FREE(argmodes);
    GS_FREE(argnames);
    GS_FREE(configitems);
}

static void DumpPkgSpec(Archive* fout, PkgInfo* pinfo)
{
    PQExpBuffer query;
    PQExpBuffer q;
    PQExpBuffer delQuery;
    PQExpBuffer labelQuery;
    PGresult* res = NULL;
    char* pkgSpecSrc = NULL;
    const char* package_str = "PACKAGE";
    const int PACKAGE_DECLARE_LEN = 18;
    /* Skip if not to be dumped */
    if (!pinfo->dobj.dump || dataOnly) {
        return;
    }

    if (isExecUserNotObjectOwner(fout, pinfo->rolname)) {
        return;
    }

    query = createPQExpBuffer();
    q = createPQExpBuffer();
    delQuery = createPQExpBuffer();
    labelQuery = createPQExpBuffer();

    /* Set proper schema search path so type references list correctly */
    selectSourceSchema(fout, pinfo->dobj.nmspace->dobj.name);

    appendPQExpBuffer(query,
        "SELECT pkgspecsrc "
        "FROM pg_catalog.gs_package "
        "WHERE oid = '%u'::pg_catalog.oid",
        pinfo->dobj.catId.oid);

    res = ExecuteSqlQueryForSingleRow(fout, query->data);
    pkgSpecSrc = PQgetvalue(res, 0, PQfnumber(res, "pkgspecsrc"));
    pkgSpecSrc += PACKAGE_DECLARE_LEN;
    /*
     * DROP must be fully qualified in case same name appears in pg_catalog
     */
    appendPQExpBuffer(delQuery, "DROP PACKAGE IF EXISTS %s.%s;\n", 
        fmtId(pinfo->dobj.nmspace->dobj.name), 
        pinfo->dobj.name);

    appendPQExpBuffer(q, "CREATE PACKAGE %s.%s IS", fmtId(pinfo->dobj.nmspace->dobj.name), pinfo->dobj.name);

    /* add slash at the end for a procedure */
    if (!fout->encryptfile && (pg_strcasecmp(format, "plain") == 0 || 
        pg_strcasecmp(format, "p") == 0 || pg_strcasecmp(format, "a") == 0
        || pg_strcasecmp(format, "append") == 0))
        appendPQExpBuffer(q, "\n %s %s;\n/", pkgSpecSrc, pinfo->dobj.name);
    else
        appendPQExpBuffer(q, "\n %s %s;\n", pkgSpecSrc, pinfo->dobj.name);

    appendPQExpBuffer(labelQuery, "PACKAGE %s\n", pinfo->dobj.name);


    ArchiveEntry(fout,
        pinfo->dobj.catId,
        pinfo->dobj.dumpId,
        pinfo->dobj.name,
        pinfo->dobj.nmspace->dobj.name,
        NULL,
        pinfo->rolname,
        false,
        package_str,
        SECTION_PRE_DATA,
        q->data,
        delQuery->data,
        NULL,
        NULL,
        0,
        NULL,
        NULL);

    dumpACL(fout,
        pinfo->dobj.catId,
        pinfo->dobj.dumpId,
        package_str,
        pinfo->dobj.name,
        NULL,
        pinfo->dobj.name,
        pinfo->dobj.nmspace->dobj.name,
        pinfo->rolname,
        pinfo->pkgacl);

    PQclear(res);

    destroyPQExpBuffer(query);
    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delQuery);
    destroyPQExpBuffer(labelQuery);

}

static void DumpPkgBody(Archive* fout, PkgInfo* pinfo)
{
    PQExpBuffer query;
    PQExpBuffer q;
    PQExpBuffer delQuery;
    PQExpBuffer labelQuery;
    PGresult* res = NULL;
    const int PACKAGE_DECLARE_LEN = 18;
    char* pkgBodySrc = NULL;
    const char* package_str = "PACKAGE BODY";
    /* Skip if not to be dumped */
    if (!pinfo->dobj.dump || dataOnly)
        return;

    if (isExecUserNotObjectOwner(fout, pinfo->rolname)) {
        return;
    }

    query = createPQExpBuffer();
    q = createPQExpBuffer();
    delQuery = createPQExpBuffer();
    labelQuery = createPQExpBuffer();

    /* Set proper schema search path so type references list correctly */
    selectSourceSchema(fout, pinfo->dobj.nmspace->dobj.name);

    /*
     * proleakproof was added at v9.2
     */

    appendPQExpBuffer(query,
        "SELECT pkgbodydeclsrc "
        "FROM pg_catalog.gs_package "
        "WHERE oid = '%u'::pg_catalog.oid",
        pinfo->dobj.catId.oid);  

    res = ExecuteSqlQueryForSingleRow(fout, query->data);
    pkgBodySrc = PQgetvalue(res, 0, PQfnumber(res, "pkgbodydeclsrc"));
    if (pkgBodySrc == NULL || strlen(pkgBodySrc) < PACKAGE_DECLARE_LEN) {
        PQclear(res);
        destroyPQExpBuffer(query);
        destroyPQExpBuffer(q);
        destroyPQExpBuffer(delQuery);
        destroyPQExpBuffer(labelQuery);
        return;
    }
    pkgBodySrc += PACKAGE_DECLARE_LEN;
    /*
     * DROP must be fully qualified in case same name appears in pg_catalog
     */
    appendPQExpBuffer(delQuery, "DROP PACKAGE BODY IF EXISTS %s.%s;\n", 
        fmtId(pinfo->dobj.nmspace->dobj.name), pinfo->dobj.name);
        
    appendPQExpBuffer(q, "CREATE PACKAGE BODY %s.%s IS", fmtId(pinfo->dobj.nmspace->dobj.name), pinfo->dobj.name);

    /* add slash at the end for a procedure */
    if (!fout->encryptfile && (pg_strcasecmp(format, "plain") == 0 || 
        pg_strcasecmp(format, "p") == 0 || pg_strcasecmp(format, "a") == 0
        || pg_strcasecmp(format, "append") == 0)) {
        appendPQExpBuffer(q, "\n %s %s;\n/", pkgBodySrc, pinfo->dobj.name);
    }
    else {
        appendPQExpBuffer(q, "\n %s %s;\n", pkgBodySrc, pinfo->dobj.name);
    }

    appendPQExpBuffer(labelQuery, "PACKAGE BODY %s\n", pinfo->dobj.name);
    


    ArchiveEntry(fout,
        pinfo->dobj.catId,
        pinfo->dobj.dumpId,
        pinfo->dobj.name,
        pinfo->dobj.nmspace->dobj.name,
        NULL,
        pinfo->rolname,
        false,
        package_str,
        SECTION_PRE_DATA,
        q->data,
        delQuery->data,
        NULL,
        NULL,
        0,
        NULL,
        NULL);

    PQclear(res);

    destroyPQExpBuffer(query);
    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delQuery);
    destroyPQExpBuffer(labelQuery);

}

/*
 * Dump a user-defined cast
 */
static void dumpCast(Archive* fout, CastInfo* cast)
{
    PQExpBuffer defqry;
    PQExpBuffer delqry;
    PQExpBuffer labelq;
    FuncInfo* funcInfo = NULL;
    char* sourcename = NULL;
    char* destname = NULL;

    /* Skip if not to be dumped */
    if (!cast->dobj.dump || dataOnly)
        return;

    /* Cannot dump if we don't have the cast function's info */
    if (OidIsValid(cast->castfunc)) {
        funcInfo = findFuncByOid(cast->castfunc);
        if (funcInfo == NULL)
            exit_horribly(NULL, "unable to find function definition for OID %u", cast->castfunc);
    }

    /*
     * As per discussion we dump casts if one or more of the underlying
     * objects (the conversion function and the two data types) are not
     * builtin AND if all of the non-builtin objects are included in the dump.
     * Builtin meaning, the nmspace name does not start with "pg_".
     *
     * However, for a cast that belongs to an extension, we must not use this
     * heuristic, but just dump the cast iff we're told to (via dobj.dump).
     */
    if (!cast->dobj.ext_member) {
        TypeInfo* sourceInfo = findTypeByOid(cast->castsource);
        TypeInfo* targetInfo = findTypeByOid(cast->casttarget);

        if (sourceInfo == NULL || targetInfo == NULL)
            return;

        /*
         * Skip this cast if all objects are from pg_
         */
        if ((funcInfo == NULL || strncmp(funcInfo->dobj.nmspace->dobj.name, "pg_", 3) == 0) &&
            strncmp(sourceInfo->dobj.nmspace->dobj.name, "pg_", 3) == 0 &&
            strncmp(targetInfo->dobj.nmspace->dobj.name, "pg_", 3) == 0)
            return;

        /*
         * Skip cast if function isn't from pg_ and is not to be dumped.
         */
        if (funcInfo != NULL && strncmp(funcInfo->dobj.nmspace->dobj.name, "pg_", 3) != 0 && !funcInfo->dobj.dump)
            return;

        /*
         * Same for the source type
         */
        if (strncmp(sourceInfo->dobj.nmspace->dobj.name, "pg_", 3) != 0 && !sourceInfo->dobj.dump)
            return;

        /*
         * and the target type.
         */
        if (strncmp(targetInfo->dobj.nmspace->dobj.name, "pg_", 3) != 0 && !targetInfo->dobj.dump)
            return;
    }

    /* Make sure we are in proper schema (needed for getFormattedTypeName) */
    selectSourceSchema(fout, "pg_catalog");

    defqry = createPQExpBuffer();
    delqry = createPQExpBuffer();
    labelq = createPQExpBuffer();

    sourcename = getFormattedTypeName(fout, cast->castsource, zeroAsNone);
    destname = getFormattedTypeName(fout, cast->casttarget, zeroAsNone);
    appendPQExpBuffer(delqry, "DROP CAST (%s AS %s);\n", sourcename, destname);
    appendPQExpBuffer(defqry, "CREATE CAST (%s AS %s) ", sourcename, destname);

    switch (cast->castmethod) {
        case COERCION_METHOD_BINARY:
            appendPQExpBuffer(defqry, "WITHOUT FUNCTION");
            break;
        case COERCION_METHOD_INOUT:
            appendPQExpBuffer(defqry, "WITH INOUT");
            break;
        case COERCION_METHOD_FUNCTION:
            if (funcInfo != NULL) {
                char* fsig = format_function_signature(fout, funcInfo, true);

                /*
                 * Always qualify the function name, in case it is not in
                 * pg_catalog schema (format_function_signature won't qualify
                 * it).
                 */
                appendPQExpBuffer(defqry, "WITH FUNCTION %s.%s", fmtId(funcInfo->dobj.nmspace->dobj.name), fsig);
                free(fsig);
                fsig = NULL;
            } else
                write_msg(NULL, "WARNING: bogus value in pg_cast.castfunc or pg_cast.castmethod field\n");
            break;
        default:
            write_msg(NULL, "WARNING: bogus value in pg_cast.castmethod field\n");
            break;
    }

    if (cast->castcontext == 'a')
        appendPQExpBuffer(defqry, " AS ASSIGNMENT");
    else if (cast->castcontext == 'i')
        appendPQExpBuffer(defqry, " AS IMPLICIT");
    appendPQExpBuffer(defqry, ";\n");

    appendPQExpBuffer(labelq, "CAST (%s AS %s)", sourcename, destname);

    if (binary_upgrade)
        binary_upgrade_extension_member(defqry, &cast->dobj, labelq->data);

    ArchiveEntry(fout,
        cast->dobj.catId,
        cast->dobj.dumpId,
        labelq->data,
        "pg_catalog",
        NULL,
        "",
        false,
        "CAST",
        SECTION_PRE_DATA,
        defqry->data,
        delqry->data,
        NULL,
        NULL,
        0,
        NULL,
        NULL);

    /* Dump Cast Comments */
    dumpComment(fout, labelq->data, NULL, "", cast->dobj.catId, 0, cast->dobj.dumpId);

    destroyPQExpBuffer(defqry);
    destroyPQExpBuffer(delqry);
    destroyPQExpBuffer(labelq);
    free(sourcename);
    sourcename = NULL;
    free(destname);
    destname = NULL;
}

/*
 * dumpOpr
 *	  write out a single operator definition
 */
static void dumpOpr(Archive* fout, OprInfo* oprinfo)
{
    PQExpBuffer query;
    PQExpBuffer q;
    PQExpBuffer delq;
    PQExpBuffer labelq;
    PQExpBuffer oprid;
    PQExpBuffer details;
    const char* name = NULL;
    PGresult* res = NULL;
    int i_oprkind = 0;
    int i_oprcode = 0;
    int i_oprleft = 0;
    int i_oprright = 0;
    int i_oprcom = 0;
    int i_oprnegate = 0;
    int i_oprrest = 0;
    int i_oprjoin = 0;
    int i_oprcanmerge = 0;
    int i_oprcanhash = 0;
    char* oprkind = NULL;
    char* oprcode = NULL;
    char* oprleft = NULL;
    char* oprright = NULL;
    char* oprcom = NULL;
    char* oprnegate = NULL;
    char* oprrest = NULL;
    char* oprjoin = NULL;
    char* oprcanmerge = NULL;
    char* oprcanhash = NULL;

    /* Skip if not to be dumped */
    if (!oprinfo->dobj.dump || dataOnly)
        return;

    if (isExecUserNotObjectOwner(fout, oprinfo->rolname))
        return;
    /*
     * some operators are invalid because they were the result of user
     * defining operators before commutators exist
     */
    if (!OidIsValid(oprinfo->oprcode))
        return;

    query = createPQExpBuffer();
    q = createPQExpBuffer();
    delq = createPQExpBuffer();
    labelq = createPQExpBuffer();
    oprid = createPQExpBuffer();
    details = createPQExpBuffer();

    /* Make sure we are in proper schema so regoperator works correctly */
    selectSourceSchema(fout, oprinfo->dobj.nmspace->dobj.name);

    if (fout->remoteVersion >= 80300) {
        appendPQExpBuffer(query,
            "SELECT oprkind, "
            "oprcode::pg_catalog.regprocedure, "
            "oprleft::pg_catalog.regtype, "
            "oprright::pg_catalog.regtype, "
            "oprcom::pg_catalog.regoperator, "
            "oprnegate::pg_catalog.regoperator, "
            "oprrest::pg_catalog.regprocedure, "
            "oprjoin::pg_catalog.regprocedure, "
            "oprcanmerge, oprcanhash "
            "FROM pg_catalog.pg_operator "
            "WHERE oid = '%u'::pg_catalog.oid",
            oprinfo->dobj.catId.oid);
    } else if (fout->remoteVersion >= 70300) {
        appendPQExpBuffer(query,
            "SELECT oprkind, "
            "oprcode::pg_catalog.regprocedure, "
            "oprleft::pg_catalog.regtype, "
            "oprright::pg_catalog.regtype, "
            "oprcom::pg_catalog.regoperator, "
            "oprnegate::pg_catalog.regoperator, "
            "oprrest::pg_catalog.regprocedure, "
            "oprjoin::pg_catalog.regprocedure, "
            "(oprlsortop != 0) AS oprcanmerge, "
            "oprcanhash "
            "FROM pg_catalog.pg_operator "
            "WHERE oid = '%u'::pg_catalog.oid",
            oprinfo->dobj.catId.oid);
    } else if (fout->remoteVersion >= 70100) {
        appendPQExpBuffer(query,
            "SELECT oprkind, oprcode, "
            "CASE WHEN oprleft = 0 THEN '-' "
            "ELSE pg_catalog.format_type(oprleft, NULL) END AS oprleft, "
            "CASE WHEN oprright = 0 THEN '-' "
            "ELSE pg_catalog.format_type(oprright, NULL) END AS oprright, "
            "oprcom, oprnegate, oprrest, oprjoin, "
            "(oprlsortop != 0) AS oprcanmerge, "
            "oprcanhash "
            "FROM pg_operator "
            "WHERE oid = '%u'::oid",
            oprinfo->dobj.catId.oid);
    } else {
        appendPQExpBuffer(query,
            "SELECT oprkind, oprcode, "
            "CASE WHEN oprleft = 0 THEN '-'::name "
            "ELSE (SELECT typname FROM pg_type WHERE oid = oprleft) END AS oprleft, "
            "CASE WHEN oprright = 0 THEN '-'::name "
            "ELSE (SELECT typname FROM pg_type WHERE oid = oprright) END AS oprright, "
            "oprcom, oprnegate, oprrest, oprjoin, "
            "(oprlsortop != 0) AS oprcanmerge, "
            "oprcanhash "
            "FROM pg_operator "
            "WHERE oid = '%u'::oid",
            oprinfo->dobj.catId.oid);
    }

    res = ExecuteSqlQueryForSingleRow(fout, query->data);

    i_oprkind = PQfnumber(res, "oprkind");
    i_oprcode = PQfnumber(res, "oprcode");
    i_oprleft = PQfnumber(res, "oprleft");
    i_oprright = PQfnumber(res, "oprright");
    i_oprcom = PQfnumber(res, "oprcom");
    i_oprnegate = PQfnumber(res, "oprnegate");
    i_oprrest = PQfnumber(res, "oprrest");
    i_oprjoin = PQfnumber(res, "oprjoin");
    i_oprcanmerge = PQfnumber(res, "oprcanmerge");
    i_oprcanhash = PQfnumber(res, "oprcanhash");

    oprkind = PQgetvalue(res, 0, i_oprkind);
    oprcode = PQgetvalue(res, 0, i_oprcode);
    oprleft = PQgetvalue(res, 0, i_oprleft);
    oprright = PQgetvalue(res, 0, i_oprright);
    oprcom = PQgetvalue(res, 0, i_oprcom);
    oprnegate = PQgetvalue(res, 0, i_oprnegate);
    oprrest = PQgetvalue(res, 0, i_oprrest);
    oprjoin = PQgetvalue(res, 0, i_oprjoin);
    oprcanmerge = PQgetvalue(res, 0, i_oprcanmerge);
    oprcanhash = PQgetvalue(res, 0, i_oprcanhash);

    name = convertRegProcReference(fout, oprcode);
    appendPQExpBuffer(details, "    PROCEDURE = %s", name);
    appendPQExpBuffer(oprid, "%s (", oprinfo->dobj.name);
    GS_FREE(name);

    /*
     * right unary means there's a left arg and left unary means there's a
     * right arg
     */
    if (strcmp(oprkind, "r") == 0 || strcmp(oprkind, "b") == 0) {
        if (fout->remoteVersion >= 70100)
            name = oprleft;
        else
            name = fmtId(oprleft);
        appendPQExpBuffer(details, ",\n    LEFTARG = %s", name);
        appendPQExpBuffer(oprid, "%s", name);
    } else
        appendPQExpBuffer(oprid, "NONE");

    if (strcmp(oprkind, "l") == 0 || strcmp(oprkind, "b") == 0) {
        if (fout->remoteVersion >= 70100)
            name = oprright;
        else
            name = fmtId(oprright);
        appendPQExpBuffer(details, ",\n    RIGHTARG = %s", name);
        appendPQExpBuffer(oprid, ", %s)", name);
    } else
        appendPQExpBuffer(oprid, ", NONE)");

    name = convertOperatorReference(fout, oprcom);
    if (NULL != name)
        appendPQExpBuffer(details, ",\n    COMMUTATOR = %s", name);

    GS_FREE(name);

    name = convertOperatorReference(fout, oprnegate);
    if (NULL != name)
        appendPQExpBuffer(details, ",\n    NEGATOR = %s", name);

    GS_FREE(name);

    if (strcmp(oprcanmerge, "t") == 0)
        appendPQExpBuffer(details, ",\n    MERGES");

    if (strcmp(oprcanhash, "t") == 0)
        appendPQExpBuffer(details, ",\n    HASHES");

    name = convertRegProcReference(fout, oprrest);
    if (NULL != name)
        appendPQExpBuffer(details, ",\n    RESTRICT = %s", name);

    GS_FREE(name);

    name = convertRegProcReference(fout, oprjoin);
    if (name != NULL)
        appendPQExpBuffer(details, ",\n    JOIN = %s", name);

    free((void*)name);
    name = NULL;

    /*
     * DROP must be fully qualified in case same name appears in pg_catalog
     */
    appendPQExpBuffer(delq, "DROP OPERATOR %s.%s;\n", fmtId(oprinfo->dobj.nmspace->dobj.name), oprid->data);

    appendPQExpBuffer(q, "CREATE OPERATOR %s (\n%s\n);\n", oprinfo->dobj.name, details->data);

    appendPQExpBuffer(labelq, "OPERATOR %s", oprid->data);

    if (binary_upgrade)
        binary_upgrade_extension_member(q, &oprinfo->dobj, labelq->data);

    ArchiveEntry(fout,
        oprinfo->dobj.catId,
        oprinfo->dobj.dumpId,
        oprinfo->dobj.name,
        oprinfo->dobj.nmspace->dobj.name,
        NULL,
        oprinfo->rolname,
        false,
        "OPERATOR",
        SECTION_PRE_DATA,
        q->data,
        delq->data,
        NULL,
        NULL,
        0,
        NULL,
        NULL);

    /* Dump Operator Comments */
    dumpComment(fout,
        labelq->data,
        oprinfo->dobj.nmspace->dobj.name,
        oprinfo->rolname,
        oprinfo->dobj.catId,
        0,
        oprinfo->dobj.dumpId);

    PQclear(res);

    destroyPQExpBuffer(query);
    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
    destroyPQExpBuffer(labelq);
    destroyPQExpBuffer(oprid);
    destroyPQExpBuffer(details);
}

/*
 * Convert a function reference obtained from pg_operator
 *
 * Returns what to print, or NULL if function references is InvalidOid
 *
 * In 7.3 the input is a REGPROCEDURE display; we have to strip the
 * argument-types part.  In prior versions, the input is a REGPROC display.
 */
static const char* convertRegProcReference(Archive* fout, const char* proc)
{
    ddr_Assert(fout != NULL);

    /* In all cases "-" means a null reference */
    if (strcmp(proc, "-") == 0)
        return NULL;

    if (fout->remoteVersion >= 70300) {
        char* name = NULL;
        char* paren = NULL;
        bool inquote = false;

        name = gs_strdup(proc);
        /* find non-double-quoted left paren */
        inquote = false;
        for (paren = name; *paren; paren++) {
            if (*paren == '(' && !inquote) {
                *paren = '\0';
                break;
            }
            if (*paren == '"')
                inquote = !inquote;
        }
        return name;
    }

    /* REGPROC before 7.3 does not quote its result */
    return fmtId(proc);
}

/*
 * Convert an operator cross-reference obtained from pg_operator
 *
 * Returns what to print, or NULL to print nothing
 *
 * In 7.3 and up the input is a REGOPERATOR display; we have to strip the
 * argument-types part, and add OPERATOR() decoration if the name is
 * schema-qualified.  In older versions, the input is just a numeric OID,
 * which we search our operator list for.
 */
static const char* convertOperatorReference(Archive* fout, const char* opr)
{
    OprInfo* oprInfo = NULL;
    int nRet = 0;

    /* In all cases "0" means a null reference */
    if (strcmp(opr, "0") == 0)
        return NULL;

    if (fout->remoteVersion >= 70300) {
        char* name = NULL;
        char* oname = NULL;
        char* ptr = NULL;
        bool inquote = false;
        bool sawdot = false;
        size_t onamelen = 0;

        name = gs_strdup(opr);
        /* find non-double-quoted left paren, and check for non-quoted dot */
        for (ptr = name; *ptr; ptr++) {
            if (*ptr == '"')
                inquote = !inquote;
            else if (*ptr == '.' && !inquote)
                sawdot = true;
            else if (*ptr == '(' && !inquote) {
                *ptr = '\0';
                break;
            }
        }
        /* If not schema-qualified, don't need to add OPERATOR() */
        if (!sawdot)
            return name;

        onamelen = strlen(name) + 11;
        oname = (char*)pg_malloc(onamelen);
        nRet = snprintf_s(oname, onamelen, onamelen - 1, "OPERATOR(%s)", name);
        securec_check_ss_c(nRet, oname, "\0");
        free(name);
        name = NULL;
        return oname;
    }

    oprInfo = findOprByOid(atooid(opr));
    if (oprInfo == NULL) {
        write_msg(NULL, "WARNING: could not find operator with OID %s\n", opr);
        return NULL;
    }
    return oprInfo->dobj.name;
}

/*
 * Convert a function OID obtained from pg_ts_parser or pg_ts_template
 *
 * It is sufficient to use REGPROC rather than REGPROCEDURE, since the
 * argument lists of these functions are predetermined.  Note that the
 * caller should ensure we are in the proper schema, because the results
 * are search path dependent!
 */
static const char* convertTSFunction(Archive* fout, Oid funcOid)
{
    char* result = NULL;
    char query[128] = {0};
    PGresult* res = NULL;
    int nRet = 0;

    nRet = snprintf_s(query,
        sizeof(query) / sizeof(char),
        sizeof(query) / sizeof(char) - 1,
        "SELECT '%u'::pg_catalog.regproc",
        funcOid);
    securec_check_ss_c(nRet, "\0", "\0");
    res = ExecuteSqlQueryForSingleRow(fout, query);

    result = gs_strdup(PQgetvalue(res, 0, 0));

    PQclear(res);

    return result;
}

/*
 * dumpAccessMethod
 *    write out a single access method definition
 */
static void dumpAccessMethod(Archive *fout, const AccessMethodInfo *aminfo)
{
    PQExpBuffer q;
    PQExpBuffer delq;
    PQExpBuffer labelq;
    char       *qamname;

    /* Skip if not to be dumped */
    if (!aminfo->dobj.dump)
        return;

    q = createPQExpBuffer();
    delq = createPQExpBuffer();
    labelq = createPQExpBuffer();
    qamname = gs_strdup(fmtId(aminfo->dobj.name));

    appendPQExpBuffer(q, "CREATE ACCESS METHOD %s ", qamname);
    appendPQExpBuffer(q, "HANDLER %s;\n", aminfo->amhandler);
    appendPQExpBuffer(delq, "DROP ACCESS METHOD %s;\n", qamname);
    appendPQExpBuffer(labelq, "ACCESS METHOD %s;\n", qamname);

    ArchiveEntry(fout, aminfo->dobj.catId, aminfo->dobj.dumpId, aminfo->dobj.name, NULL, NULL, "",
        false, "ACCESS METHOD", SECTION_PRE_DATA, q->data, delq->data, NULL, NULL, 0, NULL, NULL);

    /* Dump Access Method Comments */
    dumpComment(fout, labelq->data, NULL, "", aminfo->dobj.catId, 0, aminfo->dobj.dumpId);

    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
    destroyPQExpBuffer(labelq);
    free(qamname);
}

/*
 * dumpOpclass
 *	  write out a single operator class definition
 */
static void dumpOpclass(Archive* fout, OpclassInfo* opcinfo)
{
    PQExpBuffer query;
    PQExpBuffer q;
    PQExpBuffer delq;
    PQExpBuffer labelq;
    PGresult* res = NULL;
    int ntups = 0;
    int i_opcintype = 0;
    int i_opckeytype = 0;
    int i_opcdefault = 0;
    int i_opcfamily = 0;
    int i_opcfamilyname = 0;
    int i_opcfamilynsp = 0;
    int i_amname = 0;
    int i_amopstrategy = 0;
    int i_amopreqcheck = 0;
    int i_amopopr = 0;
    int i_sortfamily = 0;
    int i_sortfamilynsp = 0;
    int i_amprocnum = 0;
    int i_amproc = 0;
    int i_amproclefttype = 0;
    int i_amprocrighttype = 0;
    char* opcintype = NULL;
    char* opckeytype = NULL;
    char* opcdefault = NULL;
    char* opcfamily = NULL;
    char* opcfamilyname = NULL;
    char* opcfamilynsp = NULL;
    char* amname = NULL;
    char* amopstrategy = NULL;
    char* amopreqcheck = NULL;
    char* amopopr = NULL;
    char* sortfamily = NULL;
    char* sortfamilynsp = NULL;
    char* amprocnum = NULL;
    char* amproc = NULL;
    char* amproclefttype = NULL;
    char* amprocrighttype = NULL;
    bool needComma = false;
    int i = 0;

    /* Skip if not to be dumped */
    if (!opcinfo->dobj.dump || dataOnly)
        return;

    if (isExecUserNotObjectOwner(fout, opcinfo->rolname))
        return;

    /*
     * XXX currently we do not implement dumping of operator classes from
     * pre-7.3 databases.  This could be done but it seems not worth the
     * trouble.
     */
    if (fout->remoteVersion < 70300)
        return;

    query = createPQExpBuffer();
    q = createPQExpBuffer();
    delq = createPQExpBuffer();
    labelq = createPQExpBuffer();

    /* Make sure we are in proper schema so regoperator works correctly */
    selectSourceSchema(fout, opcinfo->dobj.nmspace->dobj.name);

    /* Get additional fields from the pg_opclass row */
    if (fout->remoteVersion >= 80300) {
        appendPQExpBuffer(query,
            "SELECT opcintype::pg_catalog.regtype, "
            "opckeytype::pg_catalog.regtype, "
            "opcdefault, opcfamily, "
            "opfname AS opcfamilyname, "
            "nspname AS opcfamilynsp, "
            "(SELECT amname FROM pg_catalog.pg_am WHERE oid = opcmethod) AS amname "
            "FROM pg_catalog.pg_opclass c "
            "LEFT JOIN pg_catalog.pg_opfamily f ON f.oid = opcfamily "
            "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = opfnamespace "
            "WHERE c.oid = '%u'::pg_catalog.oid",
            opcinfo->dobj.catId.oid);
    } else {
        appendPQExpBuffer(query,
            "SELECT opcintype::pg_catalog.regtype, "
            "opckeytype::pg_catalog.regtype, "
            "opcdefault, NULL AS opcfamily, "
            "NULL AS opcfamilyname, "
            "NULL AS opcfamilynsp, "
            "(SELECT amname FROM pg_catalog.pg_am WHERE oid = opcamid) AS amname "
            "FROM pg_catalog.pg_opclass "
            "WHERE oid = '%u'::pg_catalog.oid",
            opcinfo->dobj.catId.oid);
    }

    res = ExecuteSqlQueryForSingleRow(fout, query->data);

    i_opcintype = PQfnumber(res, "opcintype");
    i_opckeytype = PQfnumber(res, "opckeytype");
    i_opcdefault = PQfnumber(res, "opcdefault");
    i_opcfamily = PQfnumber(res, "opcfamily");
    i_opcfamilyname = PQfnumber(res, "opcfamilyname");
    i_opcfamilynsp = PQfnumber(res, "opcfamilynsp");
    i_amname = PQfnumber(res, "amname");

    opcintype = PQgetvalue(res, 0, i_opcintype);
    opckeytype = PQgetvalue(res, 0, i_opckeytype);
    opcdefault = PQgetvalue(res, 0, i_opcdefault);
    /* opcfamily will still be needed after we PQclear res */
    opcfamily = gs_strdup(PQgetvalue(res, 0, i_opcfamily));
    opcfamilyname = PQgetvalue(res, 0, i_opcfamilyname);
    opcfamilynsp = PQgetvalue(res, 0, i_opcfamilynsp);
    /* amname will still be needed after we PQclear res */
    amname = gs_strdup(PQgetvalue(res, 0, i_amname));

    /*
     * DROP must be fully qualified in case same name appears in pg_catalog
     */
    appendPQExpBuffer(delq, "DROP OPERATOR CLASS %s", fmtId(opcinfo->dobj.nmspace->dobj.name));
    appendPQExpBuffer(delq, ".%s", fmtId(opcinfo->dobj.name));
    appendPQExpBuffer(delq, " USING %s;\n", fmtId(amname));

    /* Build the fixed portion of the CREATE command */
    appendPQExpBuffer(q, "CREATE OPERATOR CLASS %s\n    ", fmtId(opcinfo->dobj.name));
    if (strcmp(opcdefault, "t") == 0)
        appendPQExpBuffer(q, "DEFAULT ");
    appendPQExpBuffer(q, "FOR TYPE %s USING %s", opcintype, fmtId(amname));
    if (strlen(opcfamilyname) > 0 && (strcmp(opcfamilyname, opcinfo->dobj.name) != 0 ||
                                         strcmp(opcfamilynsp, opcinfo->dobj.nmspace->dobj.name) != 0)) {
        appendPQExpBuffer(q, " FAMILY ");
        if (strcmp(opcfamilynsp, opcinfo->dobj.nmspace->dobj.name) != 0)
            appendPQExpBuffer(q, "%s.", fmtId(opcfamilynsp));
        appendPQExpBuffer(q, "%s", fmtId(opcfamilyname));
    }
    appendPQExpBuffer(q, " AS\n    ");

    needComma = false;

    if (strcmp(opckeytype, "-") != 0) {
        appendPQExpBuffer(q, "STORAGE %s", opckeytype);
        needComma = true;
    }

    PQclear(res);

    /*
     * Now fetch and print the OPERATOR entries (pg_amop rows).
     *
     * Print only those opfamily members that are tied to the opclass by
     * pg_depend entries.
     *
     * XXX RECHECK is gone as of 8.4, but we'll still print it if dumping an
     * older server's opclass in which it is used.  This is to avoid
     * hard-to-detect breakage if a newer pg_dump is used to dump from an
     * older server and then reload into that old version.	This can go away
     * once 8.3 is so old as to not be of interest to anyone.
     */
    resetPQExpBuffer(query);

    if (fout->remoteVersion >= 90100) {
        appendPQExpBuffer(query,
            "SELECT amopstrategy, false AS amopreqcheck, "
            "amopopr::pg_catalog.regoperator, "
            "opfname AS sortfamily, "
            "nspname AS sortfamilynsp "
            "FROM pg_catalog.pg_amop ao JOIN pg_catalog.pg_depend ON "
            "(classid = 'pg_catalog.pg_amop'::pg_catalog.regclass AND objid = ao.oid) "
            "LEFT JOIN pg_catalog.pg_opfamily f ON f.oid = amopsortfamily "
            "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = opfnamespace "
            "WHERE refclassid = 'pg_catalog.pg_opclass'::pg_catalog.regclass "
            "AND refobjid = '%u'::pg_catalog.oid "
            "AND amopfamily = '%s'::pg_catalog.oid "
            "ORDER BY amopstrategy",
            opcinfo->dobj.catId.oid,
            opcfamily);
    } else if (fout->remoteVersion >= 80400) {
        appendPQExpBuffer(query,
            "SELECT amopstrategy, false AS amopreqcheck, "
            "amopopr::pg_catalog.regoperator, "
            "NULL AS sortfamily, "
            "NULL AS sortfamilynsp "
            "FROM pg_catalog.pg_amop ao, pg_catalog.pg_depend "
            "WHERE refclassid = 'pg_catalog.pg_opclass'::pg_catalog.regclass "
            "AND refobjid = '%u'::pg_catalog.oid "
            "AND classid = 'pg_catalog.pg_amop'::pg_catalog.regclass "
            "AND objid = ao.oid "
            "ORDER BY amopstrategy",
            opcinfo->dobj.catId.oid);
    } else if (fout->remoteVersion >= 80300) {
        appendPQExpBuffer(query,
            "SELECT amopstrategy, amopreqcheck, "
            "amopopr::pg_catalog.regoperator, "
            "NULL AS sortfamily, "
            "NULL AS sortfamilynsp "
            "FROM pg_catalog.pg_amop ao, pg_catalog.pg_depend "
            "WHERE refclassid = 'pg_catalog.pg_opclass'::pg_catalog.regclass "
            "AND refobjid = '%u'::pg_catalog.oid "
            "AND classid = 'pg_catalog.pg_amop'::pg_catalog.regclass "
            "AND objid = ao.oid "
            "ORDER BY amopstrategy",
            opcinfo->dobj.catId.oid);
    } else {
        /*
         * Here, we print all entries since there are no opfamilies and hence
         * no loose operators to worry about.
         */
        appendPQExpBuffer(query,
            "SELECT amopstrategy, amopreqcheck, "
            "amopopr::pg_catalog.regoperator, "
            "NULL AS sortfamily, "
            "NULL AS sortfamilynsp "
            "FROM pg_catalog.pg_amop "
            "WHERE amopclaid = '%u'::pg_catalog.oid "
            "ORDER BY amopstrategy",
            opcinfo->dobj.catId.oid);
    }

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    ntups = PQntuples(res);

    i_amopstrategy = PQfnumber(res, "amopstrategy");
    i_amopreqcheck = PQfnumber(res, "amopreqcheck");
    i_amopopr = PQfnumber(res, "amopopr");
    i_sortfamily = PQfnumber(res, "sortfamily");
    i_sortfamilynsp = PQfnumber(res, "sortfamilynsp");

    for (i = 0; i < ntups; i++) {
        amopstrategy = PQgetvalue(res, i, i_amopstrategy);
        amopreqcheck = PQgetvalue(res, i, i_amopreqcheck);
        amopopr = PQgetvalue(res, i, i_amopopr);
        sortfamily = PQgetvalue(res, i, i_sortfamily);
        sortfamilynsp = PQgetvalue(res, i, i_sortfamilynsp);

        if (needComma)
            appendPQExpBuffer(q, " ,\n    ");

        appendPQExpBuffer(q, "OPERATOR %s %s", amopstrategy, amopopr);

        if (strlen(sortfamily) > 0) {
            appendPQExpBuffer(q, " FOR ORDER BY ");
            if (strcmp(sortfamilynsp, opcinfo->dobj.nmspace->dobj.name) != 0)
                appendPQExpBuffer(q, "%s.", fmtId(sortfamilynsp));
            appendPQExpBuffer(q, "%s", fmtId(sortfamily));
        }

        if (strcmp(amopreqcheck, "t") == 0)
            appendPQExpBuffer(q, " RECHECK");

        needComma = true;
    }

    PQclear(res);

    /*
     * Now fetch and print the FUNCTION entries (pg_amproc rows).
     *
     * Print only those opfamily members that are tied to the opclass by
     * pg_depend entries.
     *
     * We print the amproclefttype/amprocrighttype even though in most cases
     * the backend could deduce the right values, because of the corner case
     * of a btree sort support function for a cross-type comparison.  That's
     * only allowed in 9.2 and later, but for simplicity print them in all
     * versions that have the columns.
     */
    resetPQExpBuffer(query);

    if (fout->remoteVersion >= 80300) {
        appendPQExpBuffer(query,
            "SELECT amprocnum, "
            "amproc::pg_catalog.regprocedure, "
            "amproclefttype::pg_catalog.regtype, "
            "amprocrighttype::pg_catalog.regtype "
            "FROM pg_catalog.pg_amproc ap, pg_catalog.pg_depend "
            "WHERE refclassid = 'pg_catalog.pg_opclass'::pg_catalog.regclass "
            "AND refobjid = '%u'::pg_catalog.oid "
            "AND classid = 'pg_catalog.pg_amproc'::pg_catalog.regclass "
            "AND objid = ap.oid "
            "ORDER BY amprocnum",
            opcinfo->dobj.catId.oid);
    } else {
        appendPQExpBuffer(query,
            "SELECT amprocnum, "
            "amproc::pg_catalog.regprocedure, "
            "'' AS amproclefttype, "
            "'' AS amprocrighttype "
            "FROM pg_catalog.pg_amproc "
            "WHERE amopclaid = '%u'::pg_catalog.oid "
            "ORDER BY amprocnum",
            opcinfo->dobj.catId.oid);
    }

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    ntups = PQntuples(res);

    i_amprocnum = PQfnumber(res, "amprocnum");
    i_amproc = PQfnumber(res, "amproc");
    i_amproclefttype = PQfnumber(res, "amproclefttype");
    i_amprocrighttype = PQfnumber(res, "amprocrighttype");

    for (i = 0; i < ntups; i++) {
        amprocnum = PQgetvalue(res, i, i_amprocnum);
        amproc = PQgetvalue(res, i, i_amproc);
        amproclefttype = PQgetvalue(res, i, i_amproclefttype);
        amprocrighttype = PQgetvalue(res, i, i_amprocrighttype);

        if (needComma)
            appendPQExpBuffer(q, " ,\n    ");

        appendPQExpBuffer(q, "FUNCTION %s", amprocnum);

        if (*amproclefttype && *amprocrighttype)
            appendPQExpBuffer(q, " (%s, %s)", amproclefttype, amprocrighttype);

        appendPQExpBuffer(q, " %s", amproc);

        needComma = true;
    }

    PQclear(res);

    appendPQExpBuffer(q, ";\n");

    appendPQExpBuffer(labelq, "OPERATOR CLASS %s", fmtId(opcinfo->dobj.name));
    appendPQExpBuffer(labelq, " USING %s", fmtId(amname));

    if (binary_upgrade)
        binary_upgrade_extension_member(q, &opcinfo->dobj, labelq->data);

    ArchiveEntry(fout,
        opcinfo->dobj.catId,
        opcinfo->dobj.dumpId,
        opcinfo->dobj.name,
        opcinfo->dobj.nmspace->dobj.name,
        NULL,
        opcinfo->rolname,
        false,
        "OPERATOR CLASS",
        SECTION_PRE_DATA,
        q->data,
        delq->data,
        NULL,
        NULL,
        0,
        NULL,
        NULL);

    /* Dump Operator Class Comments */
    dumpComment(fout, labelq->data, NULL, opcinfo->rolname, opcinfo->dobj.catId, 0, opcinfo->dobj.dumpId);

    free(amname);
    amname = NULL;
    destroyPQExpBuffer(query);
    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
    destroyPQExpBuffer(labelq);
    free(opcfamily);
    opcfamily = NULL;
}

/*
 * dumpOpfamily
 *	  write out a single operator family definition
 *
 * Note: this also dumps any "loose" operator members that aren't bound to a
 * specific opclass within the opfamily.
 */
static void dumpOpfamily(Archive* fout, OpfamilyInfo* opfinfo)
{
    PQExpBuffer query;
    PQExpBuffer q;
    PQExpBuffer delq;
    PQExpBuffer labelq;
    PGresult* res = NULL;
    PGresult* res_ops = NULL;
    PGresult* res_procs = NULL;
    int ntups = 0;
    int i_amname = 0;
    int i_amopstrategy = 0;
    int i_amopreqcheck = 0;
    int i_amopopr = 0;
    int i_sortfamily = 0;
    int i_sortfamilynsp = 0;
    int i_amprocnum = 0;
    int i_amproc = 0;
    int i_amproclefttype = 0;
    int i_amprocrighttype = 0;
    char* amname = NULL;
    char* amopstrategy = NULL;
    char* amopreqcheck = NULL;
    char* amopopr = NULL;
    char* sortfamily = NULL;
    char* sortfamilynsp = NULL;
    char* amprocnum = NULL;
    char* amproc = NULL;
    char* amproclefttype = NULL;
    char* amprocrighttype = NULL;
    bool needComma = false;
    int i = 0;

    /* Skip if not to be dumped */
    if (!opfinfo->dobj.dump || dataOnly)
        return;

    if (isExecUserNotObjectOwner(fout, opfinfo->rolname))
        return;

    /*
     * We want to dump the opfamily only if (1) it contains "loose" operators
     * or functions, or (2) it contains an opclass with a different name or
     * owner.  Otherwise it's sufficient to let it be created during creation
     * of the contained opclass, and not dumping it improves portability of
     * the dump.  Since we have to fetch the loose operators/funcs anyway, do
     * that first.
     */

    query = createPQExpBuffer();
    q = createPQExpBuffer();
    delq = createPQExpBuffer();
    labelq = createPQExpBuffer();

    /* Make sure we are in proper schema so regoperator works correctly */
    selectSourceSchema(fout, opfinfo->dobj.nmspace->dobj.name);

    /*
     * Fetch only those opfamily members that are tied directly to the
     * opfamily by pg_depend entries.
     *
     * XXX RECHECK is gone as of 8.4, but we'll still print it if dumping an
     * older server's opclass in which it is used.  This is to avoid
     * hard-to-detect breakage if a newer pg_dump is used to dump from an
     * older server and then reload into that old version.	This can go away
     * once 8.3 is so old as to not be of interest to anyone.
     */
    if (fout->remoteVersion >= 90100) {
        appendPQExpBuffer(query,
            "SELECT amopstrategy, false AS amopreqcheck, "
            "amopopr::pg_catalog.regoperator, "
            "opfname AS sortfamily, "
            "nspname AS sortfamilynsp "
            "FROM pg_catalog.pg_amop ao JOIN pg_catalog.pg_depend ON "
            "(classid = 'pg_catalog.pg_amop'::pg_catalog.regclass AND objid = ao.oid) "
            "LEFT JOIN pg_catalog.pg_opfamily f ON f.oid = amopsortfamily "
            "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = opfnamespace "
            "WHERE refclassid = 'pg_catalog.pg_opfamily'::pg_catalog.regclass "
            "AND refobjid = '%u'::pg_catalog.oid "
            "AND amopfamily = '%u'::pg_catalog.oid "
            "ORDER BY amopstrategy",
            opfinfo->dobj.catId.oid,
            opfinfo->dobj.catId.oid);
    } else if (fout->remoteVersion >= 80400) {
        appendPQExpBuffer(query,
            "SELECT amopstrategy, false AS amopreqcheck, "
            "amopopr::pg_catalog.regoperator, "
            "NULL AS sortfamily, "
            "NULL AS sortfamilynsp "
            "FROM pg_catalog.pg_amop ao, pg_catalog.pg_depend "
            "WHERE refclassid = 'pg_catalog.pg_opfamily'::pg_catalog.regclass "
            "AND refobjid = '%u'::pg_catalog.oid "
            "AND classid = 'pg_catalog.pg_amop'::pg_catalog.regclass "
            "AND objid = ao.oid "
            "ORDER BY amopstrategy",
            opfinfo->dobj.catId.oid);
    } else {
        appendPQExpBuffer(query,
            "SELECT amopstrategy, amopreqcheck, "
            "amopopr::pg_catalog.regoperator, "
            "NULL AS sortfamily, "
            "NULL AS sortfamilynsp "
            "FROM pg_catalog.pg_amop ao, pg_catalog.pg_depend "
            "WHERE refclassid = 'pg_catalog.pg_opfamily'::pg_catalog.regclass "
            "AND refobjid = '%u'::pg_catalog.oid "
            "AND classid = 'pg_catalog.pg_amop'::pg_catalog.regclass "
            "AND objid = ao.oid "
            "ORDER BY amopstrategy",
            opfinfo->dobj.catId.oid);
    }

    res_ops = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    resetPQExpBuffer(query);

    appendPQExpBuffer(query,
        "SELECT amprocnum, "
        "amproc::pg_catalog.regprocedure, "
        "amproclefttype::pg_catalog.regtype, "
        "amprocrighttype::pg_catalog.regtype "
        "FROM pg_catalog.pg_amproc ap, pg_catalog.pg_depend "
        "WHERE refclassid = 'pg_catalog.pg_opfamily'::pg_catalog.regclass "
        "AND refobjid = '%u'::pg_catalog.oid "
        "AND classid = 'pg_catalog.pg_amproc'::pg_catalog.regclass "
        "AND objid = ap.oid "
        "ORDER BY amprocnum",
        opfinfo->dobj.catId.oid);

    res_procs = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    if (PQntuples(res_ops) == 0 && PQntuples(res_procs) == 0) {
        /* No loose members, so check contained opclasses */
        resetPQExpBuffer(query);

        appendPQExpBuffer(query,
            "SELECT 1 "
            "FROM pg_catalog.pg_opclass c, pg_catalog.pg_opfamily f, pg_catalog.pg_depend "
            "WHERE f.oid = '%u'::pg_catalog.oid "
            "AND refclassid = 'pg_catalog.pg_opfamily'::pg_catalog.regclass "
            "AND refobjid = f.oid "
            "AND classid = 'pg_catalog.pg_opclass'::pg_catalog.regclass "
            "AND objid = c.oid "
            "AND (opcname != opfname OR opcnamespace != opfnamespace OR opcowner != opfowner) "
            "LIMIT 1",
            opfinfo->dobj.catId.oid);

        res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

        if (PQntuples(res) == 0) {
            /* no need to dump it, so bail out */
            PQclear(res);
            PQclear(res_ops);
            PQclear(res_procs);
            destroyPQExpBuffer(query);
            destroyPQExpBuffer(q);
            destroyPQExpBuffer(delq);
            destroyPQExpBuffer(labelq);
            return;
        }

        PQclear(res);
    }

    /* Get additional fields from the pg_opfamily row */
    resetPQExpBuffer(query);

    appendPQExpBuffer(query,
        "SELECT "
        "(SELECT amname FROM pg_catalog.pg_am WHERE oid = opfmethod) AS amname "
        "FROM pg_catalog.pg_opfamily "
        "WHERE oid = '%u'::pg_catalog.oid",
        opfinfo->dobj.catId.oid);

    res = ExecuteSqlQueryForSingleRow(fout, query->data);

    i_amname = PQfnumber(res, "amname");

    /* amname will still be needed after we PQclear res */
    amname = gs_strdup(PQgetvalue(res, 0, i_amname));

    /*
     * DROP must be fully qualified in case same name appears in pg_catalog
     */
    appendPQExpBuffer(delq, "DROP OPERATOR FAMILY %s", fmtId(opfinfo->dobj.nmspace->dobj.name));
    appendPQExpBuffer(delq, ".%s", fmtId(opfinfo->dobj.name));
    appendPQExpBuffer(delq, " USING %s;\n", fmtId(amname));

    /* Build the fixed portion of the CREATE command */
    appendPQExpBuffer(q, "CREATE OPERATOR FAMILY %s", fmtId(opfinfo->dobj.name));
    appendPQExpBuffer(q, " USING %s;\n", fmtId(amname));

    PQclear(res);

    /* Do we need an ALTER to add loose members? */
    if (PQntuples(res_ops) > 0 || PQntuples(res_procs) > 0) {
        appendPQExpBuffer(q, "ALTER OPERATOR FAMILY %s", fmtId(opfinfo->dobj.name));
        appendPQExpBuffer(q, " USING %s ADD\n    ", fmtId(amname));

        needComma = false;

        /*
         * Now fetch and print the OPERATOR entries (pg_amop rows).
         */
        ntups = PQntuples(res_ops);

        i_amopstrategy = PQfnumber(res_ops, "amopstrategy");
        i_amopreqcheck = PQfnumber(res_ops, "amopreqcheck");
        i_amopopr = PQfnumber(res_ops, "amopopr");
        i_sortfamily = PQfnumber(res_ops, "sortfamily");
        i_sortfamilynsp = PQfnumber(res_ops, "sortfamilynsp");

        for (i = 0; i < ntups; i++) {
            amopstrategy = PQgetvalue(res_ops, i, i_amopstrategy);
            amopreqcheck = PQgetvalue(res_ops, i, i_amopreqcheck);
            amopopr = PQgetvalue(res_ops, i, i_amopopr);
            sortfamily = PQgetvalue(res_ops, i, i_sortfamily);
            sortfamilynsp = PQgetvalue(res_ops, i, i_sortfamilynsp);

            if (needComma)
                appendPQExpBuffer(q, " ,\n    ");

            appendPQExpBuffer(q, "OPERATOR %s %s", amopstrategy, amopopr);

            if (strlen(sortfamily) > 0) {
                appendPQExpBuffer(q, " FOR ORDER BY ");
                if (strcmp(sortfamilynsp, opfinfo->dobj.nmspace->dobj.name) != 0)
                    appendPQExpBuffer(q, "%s.", fmtId(sortfamilynsp));
                appendPQExpBuffer(q, "%s", fmtId(sortfamily));
            }

            if (strcmp(amopreqcheck, "t") == 0)
                appendPQExpBuffer(q, " RECHECK");

            needComma = true;
        }

        /*
         * Now fetch and print the FUNCTION entries (pg_amproc rows).
         */
        ntups = PQntuples(res_procs);

        i_amprocnum = PQfnumber(res_procs, "amprocnum");
        i_amproc = PQfnumber(res_procs, "amproc");
        i_amproclefttype = PQfnumber(res_procs, "amproclefttype");
        i_amprocrighttype = PQfnumber(res_procs, "amprocrighttype");

        for (i = 0; i < ntups; i++) {
            amprocnum = PQgetvalue(res_procs, i, i_amprocnum);
            amproc = PQgetvalue(res_procs, i, i_amproc);
            amproclefttype = PQgetvalue(res_procs, i, i_amproclefttype);
            amprocrighttype = PQgetvalue(res_procs, i, i_amprocrighttype);

            if (needComma)
                appendPQExpBuffer(q, " ,\n    ");

            appendPQExpBuffer(q, "FUNCTION %s (%s, %s) %s", amprocnum, amproclefttype, amprocrighttype, amproc);

            needComma = true;
        }

        appendPQExpBuffer(q, ";\n");
    }

    appendPQExpBuffer(labelq, "OPERATOR FAMILY %s", fmtId(opfinfo->dobj.name));
    appendPQExpBuffer(labelq, " USING %s", fmtId(amname));

    if (binary_upgrade)
        binary_upgrade_extension_member(q, &opfinfo->dobj, labelq->data);

    ArchiveEntry(fout,
        opfinfo->dobj.catId,
        opfinfo->dobj.dumpId,
        opfinfo->dobj.name,
        opfinfo->dobj.nmspace->dobj.name,
        NULL,
        opfinfo->rolname,
        false,
        "OPERATOR FAMILY",
        SECTION_PRE_DATA,
        q->data,
        delq->data,
        NULL,
        NULL,
        0,
        NULL,
        NULL);

    /* Dump Operator Family Comments */
    dumpComment(fout, labelq->data, NULL, opfinfo->rolname, opfinfo->dobj.catId, 0, opfinfo->dobj.dumpId);

    free(amname);
    amname = NULL;
    PQclear(res_ops);
    PQclear(res_procs);
    destroyPQExpBuffer(query);
    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
    destroyPQExpBuffer(labelq);
}

/*
 * dumpCollation
 *	  write out a single collation definition
 */
static void dumpCollation(Archive* fout, CollInfo* collinfo)
{
    PQExpBuffer query;
    PQExpBuffer q;
    PQExpBuffer delq;
    PQExpBuffer labelq;
    PGresult* res = NULL;
    int i_collcollate;
    int i_collctype;
    const char* collcollate = NULL;
    const char* collctype = NULL;

    /* Skip if not to be dumped */
    if (!collinfo->dobj.dump || dataOnly)
        return;

    if (isExecUserNotObjectOwner(fout, collinfo->rolname))
        return;

    query = createPQExpBuffer();
    q = createPQExpBuffer();
    delq = createPQExpBuffer();
    labelq = createPQExpBuffer();

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, collinfo->dobj.nmspace->dobj.name);

    /* Get conversion-specific details */
    appendPQExpBuffer(query,
        "SELECT "
        "collcollate, "
        "collctype "
        "FROM pg_catalog.pg_collation c "
        "WHERE c.oid = '%u'::pg_catalog.oid",
        collinfo->dobj.catId.oid);

    res = ExecuteSqlQueryForSingleRow(fout, query->data);

    i_collcollate = PQfnumber(res, "collcollate");
    i_collctype = PQfnumber(res, "collctype");

    collcollate = PQgetvalue(res, 0, i_collcollate);
    collctype = PQgetvalue(res, 0, i_collctype);

    /*
     * DROP must be fully qualified in case same name appears in pg_catalog
     */
    appendPQExpBuffer(delq, "DROP COLLATION %s", fmtId(collinfo->dobj.nmspace->dobj.name));
    appendPQExpBuffer(delq, ".%s;\n", fmtId(collinfo->dobj.name));

    appendPQExpBuffer(q, "CREATE COLLATION %s (lc_collate = ", fmtId(collinfo->dobj.name));
    appendStringLiteralAH(q, collcollate, fout);
    appendPQExpBuffer(q, ", lc_ctype = ");
    appendStringLiteralAH(q, collctype, fout);
    appendPQExpBuffer(q, ");\n");

    appendPQExpBuffer(labelq, "COLLATION %s", fmtId(collinfo->dobj.name));

    if (binary_upgrade)
        binary_upgrade_extension_member(q, &collinfo->dobj, labelq->data);

    ArchiveEntry(fout,
        collinfo->dobj.catId,
        collinfo->dobj.dumpId,
        collinfo->dobj.name,
        collinfo->dobj.nmspace->dobj.name,
        NULL,
        collinfo->rolname,
        false,
        "COLLATION",
        SECTION_PRE_DATA,
        q->data,
        delq->data,
        NULL,
        NULL,
        0,
        NULL,
        NULL);

    /* Dump Collation Comments */
    dumpComment(fout,
        labelq->data,
        collinfo->dobj.nmspace->dobj.name,
        collinfo->rolname,
        collinfo->dobj.catId,
        0,
        collinfo->dobj.dumpId);

    PQclear(res);

    destroyPQExpBuffer(query);
    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
    destroyPQExpBuffer(labelq);
}

/*
 * dumpConversion
 *	  write out a single conversion definition
 */
static void dumpConversion(Archive* fout, ConvInfo* convinfo)
{
    PQExpBuffer query;
    PQExpBuffer q;
    PQExpBuffer delq;
    PQExpBuffer labelq;
    PGresult* res = NULL;
    int i_conforencoding = 0;
    int i_contoencoding = 0;
    int i_conproc = 0;
    int i_condefault = 0;
    const char* conforencoding = NULL;
    const char* contoencoding = NULL;
    const char* conproc = NULL;
    bool condefault = false;

    /* Skip if not to be dumped */
    if (!convinfo->dobj.dump || dataOnly)
        return;

    if (isExecUserNotObjectOwner(fout, convinfo->rolname))
        return;

    query = createPQExpBuffer();
    q = createPQExpBuffer();
    delq = createPQExpBuffer();
    labelq = createPQExpBuffer();

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, convinfo->dobj.nmspace->dobj.name);

    /* Get conversion-specific details */
    appendPQExpBuffer(query,
        "SELECT "
        "pg_catalog.pg_encoding_to_char(conforencoding) AS conforencoding, "
        "pg_catalog.pg_encoding_to_char(contoencoding) AS contoencoding, "
        "conproc, condefault "
        "FROM pg_catalog.pg_conversion c "
        "WHERE c.oid = '%u'::pg_catalog.oid",
        convinfo->dobj.catId.oid);

    res = ExecuteSqlQueryForSingleRow(fout, query->data);

    i_conforencoding = PQfnumber(res, "conforencoding");
    i_contoencoding = PQfnumber(res, "contoencoding");
    i_conproc = PQfnumber(res, "conproc");
    i_condefault = PQfnumber(res, "condefault");

    conforencoding = PQgetvalue(res, 0, i_conforencoding);
    contoencoding = PQgetvalue(res, 0, i_contoencoding);
    conproc = PQgetvalue(res, 0, i_conproc);
    condefault = (PQgetvalue(res, 0, i_condefault)[0] == 't');

    /*
     * DROP must be fully qualified in case same name appears in pg_catalog
     */
    appendPQExpBuffer(delq, "DROP CONVERSION %s", fmtId(convinfo->dobj.nmspace->dobj.name));
    appendPQExpBuffer(delq, ".%s;\n", fmtId(convinfo->dobj.name));

    appendPQExpBuffer(q, "CREATE %sCONVERSION %s FOR ", (condefault) ? "DEFAULT " : "", fmtId(convinfo->dobj.name));
    appendStringLiteralAH(q, conforencoding, fout);
    appendPQExpBuffer(q, " TO ");
    appendStringLiteralAH(q, contoencoding, fout);
    /* regproc is automatically quoted in 7.3 and above */
    appendPQExpBuffer(q, " FROM %s;\n", conproc);

    appendPQExpBuffer(labelq, "CONVERSION %s", fmtId(convinfo->dobj.name));

    if (binary_upgrade)
        binary_upgrade_extension_member(q, &convinfo->dobj, labelq->data);

    ArchiveEntry(fout,
        convinfo->dobj.catId,
        convinfo->dobj.dumpId,
        convinfo->dobj.name,
        convinfo->dobj.nmspace->dobj.name,
        NULL,
        convinfo->rolname,
        false,
        "CONVERSION",
        SECTION_PRE_DATA,
        q->data,
        delq->data,
        NULL,
        NULL,
        0,
        NULL,
        NULL);

    /* Dump Conversion Comments */
    dumpComment(fout,
        labelq->data,
        convinfo->dobj.nmspace->dobj.name,
        convinfo->rolname,
        convinfo->dobj.catId,
        0,
        convinfo->dobj.dumpId);

    PQclear(res);

    destroyPQExpBuffer(query);
    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
    destroyPQExpBuffer(labelq);
}

/*
 * format_aggregate_signature: generate aggregate name and argument list
 *
 * The argument type names are qualified if needed.  The aggregate name
 * is never qualified.
 */
static char* format_aggregate_signature(AggInfo* agginfo, Archive* fout, bool honor_quotes)
{
    PQExpBufferData buf;
    int j;

    initPQExpBuffer(&buf);
    if (honor_quotes)
        appendPQExpBuffer(&buf, "%s", fmtId(agginfo->aggfn.dobj.name));
    else
        appendPQExpBuffer(&buf, "%s", agginfo->aggfn.dobj.name);

    if (agginfo->aggfn.nargs == 0)
        appendPQExpBuffer(&buf, "(*)");
    else {
        appendPQExpBuffer(&buf, "(");
        for (j = 0; j < agginfo->aggfn.nargs; j++) {
            char* typname = NULL;

            typname = getFormattedTypeName(fout, agginfo->aggfn.argtypes[j], zeroAsOpaque);

            appendPQExpBuffer(&buf, "%s%s", (j > 0) ? ", " : "", typname);
            free(typname);
            typname = NULL;
        }
        appendPQExpBuffer(&buf, ")");
    }
    return buf.data;
}

/*
 * dumpAgg
 *	  write out a single aggregate definition
 */
static void dumpAgg(Archive* fout, AggInfo* agginfo)
{
    PQExpBuffer query;
    PQExpBuffer q;
    PQExpBuffer delq;
    PQExpBuffer labelq;
    PQExpBuffer details;
    char* aggsig = NULL;
    char* aggsig_tag = NULL;
    PGresult* res = NULL;
    int i_aggtransfn = 0;
    int i_aggfinalfn = 0;
    int i_aggsortop = 0;
    int i_aggtranstype = 0;
    int i_agginitval = 0;
    int i_convertok = 0;
    const char* aggtransfn = NULL;
    const char* aggfinalfn = NULL;
    const char* aggsortop = NULL;
    const char* aggtranstype = NULL;
    const char* agginitval = NULL;
    bool convertok = false;

    /* Skip if not to be dumped */
    if (!agginfo->aggfn.dobj.dump || dataOnly)
        return;

    if (isExecUserNotObjectOwner(fout, agginfo->aggfn.rolname))
        return;

    query = createPQExpBuffer();
    q = createPQExpBuffer();
    delq = createPQExpBuffer();
    labelq = createPQExpBuffer();
    details = createPQExpBuffer();

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, agginfo->aggfn.dobj.nmspace->dobj.name);

    /* Get aggregate-specific details */
    if (fout->remoteVersion >= 80100) {
        appendPQExpBuffer(query,
            "SELECT aggtransfn, "
            "aggfinalfn, aggtranstype::pg_catalog.regtype, "
            "aggsortop::pg_catalog.regoperator, "
            "agginitval, "
            "'t'::boolean AS convertok "
            "FROM pg_catalog.pg_aggregate a, pg_catalog.pg_proc p "
            "WHERE a.aggfnoid = p.oid "
            "AND p.oid = '%u'::pg_catalog.oid",
            agginfo->aggfn.dobj.catId.oid);
    } else if (fout->remoteVersion >= 70300) {
        appendPQExpBuffer(query,
            "SELECT aggtransfn, "
            "aggfinalfn, aggtranstype::pg_catalog.regtype, "
            "0 AS aggsortop, "
            "agginitval, "
            "'t'::boolean AS convertok "
            "FROM pg_catalog.pg_aggregate a, pg_catalog.pg_proc p "
            "WHERE a.aggfnoid = p.oid "
            "AND p.oid = '%u'::pg_catalog.oid",
            agginfo->aggfn.dobj.catId.oid);
    } else if (fout->remoteVersion >= 70100) {
        appendPQExpBuffer(query,
            "SELECT aggtransfn, aggfinalfn, "
            "pg_catalog.format_type(aggtranstype, NULL) AS aggtranstype, "
            "0 AS aggsortop, "
            "agginitval, "
            "'t'::boolean AS convertok "
            "FROM pg_aggregate "
            "WHERE oid = '%u'::oid",
            agginfo->aggfn.dobj.catId.oid);
    } else {
        appendPQExpBuffer(query,
            "SELECT aggtransfn1 AS aggtransfn, "
            "aggfinalfn, "
            "(SELECT typname FROM pg_type WHERE oid = aggtranstype1) AS aggtranstype, "
            "0 AS aggsortop, "
            "agginitval1 AS agginitval, "
            "(aggtransfn2 = 0 and aggtranstype2 = 0 and agginitval2 is null) AS convertok "
            "FROM pg_aggregate "
            "WHERE oid = '%u'::oid",
            agginfo->aggfn.dobj.catId.oid);
    }

    res = ExecuteSqlQueryForSingleRow(fout, query->data);

    i_aggtransfn = PQfnumber(res, "aggtransfn");
    i_aggfinalfn = PQfnumber(res, "aggfinalfn");
    i_aggsortop = PQfnumber(res, "aggsortop");
    i_aggtranstype = PQfnumber(res, "aggtranstype");
    i_agginitval = PQfnumber(res, "agginitval");
    i_convertok = PQfnumber(res, "convertok");

    aggtransfn = PQgetvalue(res, 0, i_aggtransfn);
    aggfinalfn = PQgetvalue(res, 0, i_aggfinalfn);
    aggsortop = PQgetvalue(res, 0, i_aggsortop);
    aggtranstype = PQgetvalue(res, 0, i_aggtranstype);
    agginitval = PQgetvalue(res, 0, i_agginitval);
    convertok = (PQgetvalue(res, 0, i_convertok)[0] == 't');

    aggsig = format_aggregate_signature(agginfo, fout, true);
    aggsig_tag = format_aggregate_signature(agginfo, fout, false);

    if (!convertok) {
        write_msg(NULL,
            "WARNING: aggregate function %s could not be dumped correctly for this database version; ignored\n",
            aggsig);

        free(aggsig);
        aggsig = NULL;
        free(aggsig_tag);
        aggsig_tag = NULL;

        PQclear(res);
        free((void*)aggsortop);
        aggsortop = NULL;
        destroyPQExpBuffer(query);
        destroyPQExpBuffer(q);
        destroyPQExpBuffer(delq);
        destroyPQExpBuffer(labelq);
        destroyPQExpBuffer(details);
        return;
    }

    if (fout->remoteVersion >= 70300) {
        /* If using 7.3's regproc or regtype, data is already quoted */
        appendPQExpBuffer(details, "    SFUNC = %s,\n    STYPE = %s", aggtransfn, aggtranstype);
    } else if (fout->remoteVersion >= 70100) {
        /* format_type quotes, regproc does not */
        appendPQExpBuffer(details, "    SFUNC = %s,\n    STYPE = %s", fmtId(aggtransfn), aggtranstype);
    } else {
        /* need quotes all around */
        appendPQExpBuffer(details, "    SFUNC = %s,\n", fmtId(aggtransfn));
        appendPQExpBuffer(details, "    STYPE = %s", fmtId(aggtranstype));
    }

    if (!PQgetisnull(res, 0, i_agginitval)) {
        appendPQExpBuffer(details, ",\n    INITCOND = ");
        appendStringLiteralAH(details, agginitval, fout);
    }

    if (strcmp(aggfinalfn, "-") != 0) {
        appendPQExpBuffer(details, ",\n    FINALFUNC = %s", aggfinalfn);
    }

    aggsortop = convertOperatorReference(fout, aggsortop);
    if (NULL != aggsortop) {
        appendPQExpBuffer(details, ",\n    SORTOP = %s", aggsortop);
    }

    /*
     * DROP must be fully qualified in case same name appears in pg_catalog
     */
    appendPQExpBuffer(delq, "DROP AGGREGATE %s.%s;\n", fmtId(agginfo->aggfn.dobj.nmspace->dobj.name), aggsig);

    appendPQExpBuffer(q, "CREATE AGGREGATE %s (\n%s\n);\n", aggsig, details->data);

    appendPQExpBuffer(labelq, "AGGREGATE %s", aggsig);

    if (binary_upgrade)
        binary_upgrade_extension_member(q, &agginfo->aggfn.dobj, labelq->data);

    ArchiveEntry(fout,
        agginfo->aggfn.dobj.catId,
        agginfo->aggfn.dobj.dumpId,
        aggsig_tag,
        agginfo->aggfn.dobj.nmspace->dobj.name,
        NULL,
        agginfo->aggfn.rolname,
        false,
        "AGGREGATE",
        SECTION_PRE_DATA,
        q->data,
        delq->data,
        NULL,
        NULL,
        0,
        NULL,
        NULL);

    /* Dump Aggregate Comments */
    dumpComment(fout,
        labelq->data,
        agginfo->aggfn.dobj.nmspace->dobj.name,
        agginfo->aggfn.rolname,
        agginfo->aggfn.dobj.catId,
        0,
        agginfo->aggfn.dobj.dumpId);
    dumpSecLabel(fout,
        labelq->data,
        agginfo->aggfn.dobj.nmspace->dobj.name,
        agginfo->aggfn.rolname,
        agginfo->aggfn.dobj.catId,
        0,
        agginfo->aggfn.dobj.dumpId);

    /*
     * Since there is no GRANT ON AGGREGATE syntax, we have to make the ACL
     * command look like a function's GRANT; in particular this affects the
     * syntax for zero-argument aggregates.
     */
    free(aggsig);
    aggsig = NULL;
    free(aggsig_tag);
    aggsig_tag = NULL;

    aggsig = format_function_signature(fout, &agginfo->aggfn, true);
    aggsig_tag = format_function_signature(fout, &agginfo->aggfn, false);

    dumpACL(fout,
        agginfo->aggfn.dobj.catId,
        agginfo->aggfn.dobj.dumpId,
        "FUNCTION",
        aggsig,
        NULL,
        aggsig_tag,
        agginfo->aggfn.dobj.nmspace->dobj.name,
        agginfo->aggfn.rolname,
        agginfo->aggfn.proacl);

    free(aggsig);
    aggsig = NULL;
    free(aggsig_tag);
    aggsig_tag = NULL;

    PQclear(res);
    free((void*)aggsortop);
    aggsortop = NULL;
    destroyPQExpBuffer(query);
    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
    destroyPQExpBuffer(labelq);
    destroyPQExpBuffer(details);
}

/*
 * dumpTSParser
 *	  write out a single text search parser
 */
static void dumpTSParser(Archive* fout, TSParserInfo* prsinfo)
{
    PQExpBuffer q;
    PQExpBuffer delq;
    PQExpBuffer labelq;
    const char* name = NULL;

    /* Skip if not to be dumped */
    if (!prsinfo->dobj.dump || dataOnly)
        return;

    q = createPQExpBuffer();
    delq = createPQExpBuffer();
    labelq = createPQExpBuffer();

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, prsinfo->dobj.nmspace->dobj.name);

    appendPQExpBuffer(q, "CREATE TEXT SEARCH PARSER %s (\n", fmtId(prsinfo->dobj.name));

    name = convertTSFunction(fout, prsinfo->prsstart);
    appendPQExpBuffer(q, "    START = %s,\n", name);
    GS_FREE(name);

    name = convertTSFunction(fout, prsinfo->prstoken);
    appendPQExpBuffer(q, "    GETTOKEN = %s,\n", name);
    GS_FREE(name);

    name = convertTSFunction(fout, prsinfo->prsend);
    appendPQExpBuffer(q, "    END = %s,\n", name);
    GS_FREE(name);

    if (prsinfo->prsheadline != InvalidOid) {
        name = convertTSFunction(fout, prsinfo->prsheadline);
        appendPQExpBuffer(q, "    HEADLINE = %s,\n", name);
        GS_FREE(name);
    }

    name = convertTSFunction(fout, prsinfo->prslextype);
    appendPQExpBuffer(q, "    LEXTYPES = %s );\n", name);
    free((void*)name);
    name = NULL;

    /*
     * DROP must be fully qualified in case same name appears in pg_catalog
     */
    appendPQExpBuffer(delq, "DROP TEXT SEARCH PARSER %s", fmtId(prsinfo->dobj.nmspace->dobj.name));
    appendPQExpBuffer(delq, ".%s;\n", fmtId(prsinfo->dobj.name));

    appendPQExpBuffer(labelq, "TEXT SEARCH PARSER %s", fmtId(prsinfo->dobj.name));

    if (binary_upgrade)
        binary_upgrade_extension_member(q, &prsinfo->dobj, labelq->data);

    ArchiveEntry(fout,
        prsinfo->dobj.catId,
        prsinfo->dobj.dumpId,
        prsinfo->dobj.name,
        prsinfo->dobj.nmspace->dobj.name,
        NULL,
        "",
        false,
        "TEXT SEARCH PARSER",
        SECTION_PRE_DATA,
        q->data,
        delq->data,
        NULL,
        NULL,
        0,
        NULL,
        NULL);

    /* Dump Parser Comments */
    dumpComment(fout, labelq->data, NULL, "", prsinfo->dobj.catId, 0, prsinfo->dobj.dumpId);

    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
    destroyPQExpBuffer(labelq);
}

/*
 * dumpTSDictionary
 *	  write out a single text search dictionary
 */
static void dumpTSDictionary(Archive* fout, TSDictInfo* dictinfo)
{
    PQExpBuffer q;
    PQExpBuffer delq;
    PQExpBuffer labelq;
    PQExpBuffer query;
    PGresult* res = NULL;
    char* nspname = NULL;
    char* tmplname = NULL;

    /* Skip if not to be dumped */
    if (!dictinfo->dobj.dump || dataOnly)
        return;

    if (isExecUserNotObjectOwner(fout, dictinfo->rolname))
        return;

    q = createPQExpBuffer();
    delq = createPQExpBuffer();
    labelq = createPQExpBuffer();
    query = createPQExpBuffer();

    /* Fetch name and nmspace of the dictionary's template */
    selectSourceSchema(fout, "pg_catalog");
    appendPQExpBuffer(query,
        "SELECT nspname, tmplname "
        "FROM pg_ts_template p, pg_namespace n "
        "WHERE p.oid = '%u' AND n.oid = tmplnamespace",
        dictinfo->dicttemplate);
    res = ExecuteSqlQueryForSingleRow(fout, query->data);
    nspname = PQgetvalue(res, 0, 0);
    tmplname = PQgetvalue(res, 0, 1);

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, dictinfo->dobj.nmspace->dobj.name);

    appendPQExpBuffer(q, "CREATE TEXT SEARCH DICTIONARY %s (\n", fmtId(dictinfo->dobj.name));

    appendPQExpBuffer(q, "    TEMPLATE = ");
    if (strcmp(nspname, dictinfo->dobj.nmspace->dobj.name) != 0)
        appendPQExpBuffer(q, "%s.", fmtId(nspname));
    appendPQExpBuffer(q, "%s", fmtId(tmplname));

    PQclear(res);

    /* the dictinitoption can be dumped straight into the command */
    if (NULL != (dictinfo->dictinitoption))
        appendPQExpBuffer(q, ",\n    %s", dictinfo->dictinitoption);

    appendPQExpBuffer(q, " );\n");

    /*
     * DROP must be fully qualified in case same name appears in pg_catalog
     */
    appendPQExpBuffer(delq, "DROP TEXT SEARCH DICTIONARY %s", fmtId(dictinfo->dobj.nmspace->dobj.name));
    appendPQExpBuffer(delq, ".%s;\n", fmtId(dictinfo->dobj.name));

    appendPQExpBuffer(labelq, "TEXT SEARCH DICTIONARY %s", fmtId(dictinfo->dobj.name));

    if (binary_upgrade)
        binary_upgrade_extension_member(q, &dictinfo->dobj, labelq->data);

    ArchiveEntry(fout,
        dictinfo->dobj.catId,
        dictinfo->dobj.dumpId,
        dictinfo->dobj.name,
        dictinfo->dobj.nmspace->dobj.name,
        NULL,
        dictinfo->rolname,
        false,
        "TEXT SEARCH DICTIONARY",
        SECTION_PRE_DATA,
        q->data,
        delq->data,
        NULL,
        NULL,
        0,
        NULL,
        NULL);

    /* Dump Dictionary Comments */
    dumpComment(fout, labelq->data, NULL, dictinfo->rolname, dictinfo->dobj.catId, 0, dictinfo->dobj.dumpId);

    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
    destroyPQExpBuffer(labelq);
    destroyPQExpBuffer(query);
}

/*
 * dumpTSTemplate
 *	  write out a single text search template
 */
static void dumpTSTemplate(Archive* fout, TSTemplateInfo* tmplinfo)
{
    PQExpBuffer q;
    PQExpBuffer delq;
    PQExpBuffer labelq;
    const char* name = NULL;

    /* Skip if not to be dumped */
    if (!tmplinfo->dobj.dump || dataOnly)
        return;

    q = createPQExpBuffer();
    delq = createPQExpBuffer();
    labelq = createPQExpBuffer();

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, tmplinfo->dobj.nmspace->dobj.name);

    appendPQExpBuffer(q, "CREATE TEXT SEARCH TEMPLATE %s (\n", fmtId(tmplinfo->dobj.name));

    if (tmplinfo->tmplinit != InvalidOid) {
        name = convertTSFunction(fout, tmplinfo->tmplinit);
        appendPQExpBuffer(q, "    INIT = %s,\n", name);
        GS_FREE(name);
    }

    name = convertTSFunction(fout, tmplinfo->tmpllexize);
    appendPQExpBuffer(q, "    LEXIZE = %s );\n", name);
    free((void*)name);
    name = NULL;

    /*
     * DROP must be fully qualified in case same name appears in pg_catalog
     */
    appendPQExpBuffer(delq, "DROP TEXT SEARCH TEMPLATE %s", fmtId(tmplinfo->dobj.nmspace->dobj.name));
    appendPQExpBuffer(delq, ".%s;\n", fmtId(tmplinfo->dobj.name));

    appendPQExpBuffer(labelq, "TEXT SEARCH TEMPLATE %s", fmtId(tmplinfo->dobj.name));

    if (binary_upgrade)
        binary_upgrade_extension_member(q, &tmplinfo->dobj, labelq->data);

    ArchiveEntry(fout,
        tmplinfo->dobj.catId,
        tmplinfo->dobj.dumpId,
        tmplinfo->dobj.name,
        tmplinfo->dobj.nmspace->dobj.name,
        NULL,
        "",
        false,
        "TEXT SEARCH TEMPLATE",
        SECTION_PRE_DATA,
        q->data,
        delq->data,
        NULL,
        NULL,
        0,
        NULL,
        NULL);

    /* Dump Template Comments */
    dumpComment(fout, labelq->data, NULL, "", tmplinfo->dobj.catId, 0, tmplinfo->dobj.dumpId);

    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
    destroyPQExpBuffer(labelq);
}

/*
 * dumpTSConfig
 *	  write out a single text search configuration
 */
static void dumpTSConfig(Archive* fout, TSConfigInfo* cfginfo)
{
    PQExpBuffer q;
    PQExpBuffer delq;
    PQExpBuffer labelq;
    PQExpBuffer query;
    PGresult* res = NULL;
    char* nspname = NULL;
    char* prsname = NULL;
    char* cfoptions = NULL;
    int ntups = 0;
    int i = 0;
    int i_tokenname = 0;
    int i_dictname = 0;

    char* temp = NULL;

    /* Skip if not to be dumped */
    if (!cfginfo->dobj.dump || dataOnly)
        return;

    if (isExecUserNotObjectOwner(fout, cfginfo->rolname))
        return;

    q = createPQExpBuffer();
    delq = createPQExpBuffer();
    labelq = createPQExpBuffer();
    query = createPQExpBuffer();

    /* Fetch name and nmspace of the config's parser */
    selectSourceSchema(fout, "pg_catalog");
    appendPQExpBuffer(query,
        "SELECT nspname, prsname "
        "FROM pg_ts_parser p, pg_namespace n "
        "WHERE p.oid = '%u' AND n.oid = prsnamespace",
        cfginfo->cfgparser);
    res = ExecuteSqlQueryForSingleRow(fout, query->data);
    nspname = PQgetvalue(res, 0, 0);
    prsname = PQgetvalue(res, 0, 1);
    temp = gs_strdup(prsname);

    appendPQExpBuffer(q, "CREATE TEXT SEARCH CONFIGURATION %s (\n", fmtId(cfginfo->dobj.name));

    appendPQExpBuffer(q, "    PARSER = ");
    if (strcmp(nspname, cfginfo->dobj.nmspace->dobj.name) != 0)
        appendPQExpBuffer(q, "%s.", fmtId(nspname));
    appendPQExpBuffer(q, "%s )", fmtId(prsname));
    PQclear(res);

    resetPQExpBuffer(query);
    appendPQExpBuffer(query,
        "SELECT attname FROM pg_attribute "
        "WHERE attrelid IN (SELECT oid FROM "
        "pg_class WHERE relname = 'pg_ts_config') "
        "AND attname = 'cfoptions'");
    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);
    if (0 == ntups) /* old version, no cfoptions */
        appendPQExpBuffer(q, ";\n");
    else /* read text search configuration options */
    {
        PQclear(res);
        resetPQExpBuffer(query);
        appendPQExpBuffer(query,
            "SELECT pg_catalog.array_to_string(cfoptions, ', ') "
            "FROM pg_ts_config WHERE oid = %u ",
            cfginfo->dobj.catId.oid);
        res = ExecuteSqlQueryForSingleRow(fout, query->data);
        cfoptions = PQgetvalue(res, 0, 0);
        /* Dump text search configuration options if any */
        if (NULL != cfoptions && cfoptions[0]) {
            if (strcmp(temp, "pound") == 0) {
                uint16 offset = strlen(cfoptions) - 1;
                appendPQExpBuffer(q, "\nWITH (\n    split_flag='%s' );\n", (cfoptions + offset));
            } else
                appendPQExpBuffer(q, "\nWITH (\n    %s );\n", cfoptions);
        }

        else
            appendPQExpBuffer(q, ";\n");
    }

    free(temp);
    temp = NULL;

    PQclear(res);

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, cfginfo->dobj.nmspace->dobj.name);
    resetPQExpBuffer(query);
    appendPQExpBuffer(query,
        "SELECT \n"
        "  ( SELECT alias FROM pg_catalog.ts_token_type('%u'::pg_catalog.oid) AS t \n"
        "    WHERE t.tokid = m.maptokentype ) AS tokenname, \n"
        "  m.mapdict::pg_catalog.regdictionary AS dictname \n"
        "FROM pg_catalog.pg_ts_config_map AS m \n"
        "WHERE m.mapcfg = '%u' \n"
        "ORDER BY m.mapcfg, m.maptokentype, m.mapseqno",
        cfginfo->cfgparser,
        cfginfo->dobj.catId.oid);

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);

    i_tokenname = PQfnumber(res, "tokenname");
    i_dictname = PQfnumber(res, "dictname");

    for (i = 0; i < ntups; i++) {
        char* tokenname = PQgetvalue(res, i, i_tokenname);
        char* dictname = PQgetvalue(res, i, i_dictname);

        if (i == 0 || strcmp(tokenname, PQgetvalue(res, i - 1, i_tokenname)) != 0) {
            /* starting a new token type, so start a new command */
            if (i > 0)
                appendPQExpBuffer(q, ";\n");
            appendPQExpBuffer(
                q, "\n--\n--ELEMENT\n--\nALTER TEXT SEARCH CONFIGURATION %s\n", fmtId(cfginfo->dobj.name));
            /* tokenname needs quoting, dictname does NOT */
            appendPQExpBuffer(q, "    ADD MAPPING FOR %s WITH %s", fmtId(tokenname), dictname);
        } else
            appendPQExpBuffer(q, ", %s", dictname);
    }

    if (ntups > 0)
        appendPQExpBuffer(q, ";\n");

    PQclear(res);

    /*
     * DROP must be fully qualified in case same name appears in pg_catalog
     */
    appendPQExpBuffer(delq, "DROP TEXT SEARCH CONFIGURATION IF EXISTS %s", fmtId(cfginfo->dobj.nmspace->dobj.name));
    appendPQExpBuffer(delq, ".%s;\n", fmtId(cfginfo->dobj.name));

    appendPQExpBuffer(labelq, "TEXT SEARCH CONFIGURATION %s", fmtId(cfginfo->dobj.name));

    if (binary_upgrade)
        binary_upgrade_extension_member(q, &cfginfo->dobj, labelq->data);

    ArchiveEntry(fout,
        cfginfo->dobj.catId,
        cfginfo->dobj.dumpId,
        cfginfo->dobj.name,
        cfginfo->dobj.nmspace->dobj.name,
        NULL,
        cfginfo->rolname,
        false,
        "TEXT SEARCH CONFIGURATION",
        SECTION_PRE_DATA,
        q->data,
        delq->data,
        NULL,
        NULL,
        0,
        NULL,
        NULL);

    /* Dump Configuration Comments */
    dumpComment(fout, labelq->data, NULL, cfginfo->rolname, cfginfo->dobj.catId, 0, cfginfo->dobj.dumpId);

    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
    destroyPQExpBuffer(labelq);
    destroyPQExpBuffer(query);
}

/*
 * dumpForeignDataWrapper
 *	  write out a single foreign-data wrapper definition
 */
static void dumpForeignDataWrapper(Archive* fout, FdwInfo* fdwinfo)
{
    PQExpBuffer q;
    PQExpBuffer delq;
    PQExpBuffer labelq;
    char* qfdwname = NULL;

    /* Skip if not to be dumped */
    if (!fdwinfo->dobj.dump || dataOnly)
        return;

    if (isExecUserNotObjectOwner(fout, fdwinfo->rolname))
        return;
    /*
     * FDWs that belong to an extension are dumped based on their "dump"
     * field. Otherwise omit them if we are only dumping some specific object.
     */
    if (!fdwinfo->dobj.ext_member)
        if (!include_everything)
            return;

    q = createPQExpBuffer();
    delq = createPQExpBuffer();
    labelq = createPQExpBuffer();

    qfdwname = gs_strdup(fmtId(fdwinfo->dobj.name));

    appendPQExpBuffer(q, "CREATE FOREIGN DATA WRAPPER %s", qfdwname);

    if (strcmp(fdwinfo->fdwhandler, "-") != 0)
        appendPQExpBuffer(q, " HANDLER %s", fdwinfo->fdwhandler);

    if (strcmp(fdwinfo->fdwvalidator, "-") != 0)
        appendPQExpBuffer(q, " VALIDATOR %s", fdwinfo->fdwvalidator);

    if (strlen(fdwinfo->fdwoptions) > 0)
        appendPQExpBuffer(q, " OPTIONS (\n    %s\n)", fdwinfo->fdwoptions);

    appendPQExpBuffer(q, ";\n");

    appendPQExpBuffer(delq, "DROP FOREIGN DATA WRAPPER %s;\n", qfdwname);

    appendPQExpBuffer(labelq, "FOREIGN DATA WRAPPER %s", qfdwname);

    if (binary_upgrade)
        binary_upgrade_extension_member(q, &fdwinfo->dobj, labelq->data);

    ArchiveEntry(fout,
        fdwinfo->dobj.catId,
        fdwinfo->dobj.dumpId,
        fdwinfo->dobj.name,
        NULL,
        NULL,
        fdwinfo->rolname,
        false,
        "FOREIGN DATA WRAPPER",
        SECTION_PRE_DATA,
        q->data,
        delq->data,
        NULL,
        NULL,
        0,
        NULL,
        NULL);

    /* Handle the ACL */
    dumpACL(fout,
        fdwinfo->dobj.catId,
        fdwinfo->dobj.dumpId,
        "FOREIGN DATA WRAPPER",
        qfdwname,
        NULL,
        fdwinfo->dobj.name,
        NULL,
        fdwinfo->rolname,
        fdwinfo->fdwacl);

    /* Dump Foreign Data Wrapper Comments */
    dumpComment(fout, labelq->data, NULL, fdwinfo->rolname, fdwinfo->dobj.catId, 0, fdwinfo->dobj.dumpId);

    free(qfdwname);
    qfdwname = NULL;

    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
    destroyPQExpBuffer(labelq);
}

static inline bool IsDefaultForeignServer(const char* serverName)
{
    /*
     * gsmpp_server, gsmpp_errorinfo_server, mot_server are created by default at the time of initdb.
     */
    if ((0 == strncmp(serverName, "gsmpp_server", strlen("gsmpp_server") + 1)) ||
        (0 == strncmp(serverName, "\"gsmpp_server\"", strlen("\"gsmpp_server\"") + 1)) ||
        (0 == strncmp(serverName, "gsmpp_errorinfo_server", strlen("gsmpp_errorinfo_server") + 1)) ||
        (0 == strncmp(serverName, "\"gsmpp_errorinfo_server\"", strlen("\"gsmpp_errorinfo_server\"") + 1)) ||
        (0 == strncmp(serverName, "mot_server", strlen("mot_server") + 1)) ||
        (0 == strncmp(serverName, "\"mot_server\"", strlen("\"mot_server\"") + 1))) {
        return true;
    }
    return false;
}

/*
 * dumpForeignServer
 *	  write out a foreign server definition
 */
static void dumpForeignServer(Archive* fout, ForeignServerInfo* srvinfo)
{
    PQExpBuffer q;
    PQExpBuffer delq;
    PQExpBuffer labelq;
    PQExpBuffer query;
    PGresult* res = NULL;
    char* qsrvname = NULL;
    char* fdwname = NULL;
    bool IsHdfsFdw = false;

    /* Skip if not to be dumped */
    if (!srvinfo->dobj.dump || dataOnly || !include_everything)
        return;

    if (isExecUserNotObjectOwner(fout, srvinfo->rolname))
        return;

    /*
     * gsmpp_server, gsmpp_errorinfo_server, mot_server are created by default at the time of initdb,
     * so skip it.
     */
    qsrvname = gs_strdup(fmtId(srvinfo->dobj.name));
    if (IsDefaultForeignServer(qsrvname)) {
        free(qsrvname);
        qsrvname = NULL;
        return;
    }

    q = createPQExpBuffer();
    delq = createPQExpBuffer();
    labelq = createPQExpBuffer();
    query = createPQExpBuffer();

    /* look up the foreign-data wrapper */
    selectSourceSchema(fout, "pg_catalog");
    appendPQExpBuffer(query,
        "SELECT fdwname "
        "FROM pg_foreign_data_wrapper w "
        "WHERE w.oid = '%u'",
        srvinfo->srvfdw);
    res = ExecuteSqlQueryForSingleRow(fout, query->data);
    fdwname = PQgetvalue(res, 0, 0);

    if (0 == pg_strcasecmp(fdwname, "hdfs_fdw")) {
        IsHdfsFdw = true;
    }

    appendPQExpBuffer(q, "CREATE SERVER %s", qsrvname);
    if ((srvinfo->srvtype != NULL) && strlen(srvinfo->srvtype) > 0) {
        appendPQExpBuffer(q, " TYPE ");
        appendStringLiteralAH(q, srvinfo->srvtype, fout);
    }
    if ((srvinfo->srvversion != NULL) && strlen(srvinfo->srvversion) > 0) {
        appendPQExpBuffer(q, " VERSION ");
        appendStringLiteralAH(q, srvinfo->srvversion, fout);
    }

    appendPQExpBuffer(q, " FOREIGN DATA WRAPPER ");
    appendPQExpBuffer(q, "%s", fmtId(fdwname));

    if ((srvinfo->srvoptions != NULL) && strlen(srvinfo->srvoptions) > 0) {
        appendPQExpBuffer(q, " OPTIONS (\n    %s", srvinfo->srvoptions);
        if (IsHdfsFdw && NULL == strstr(srvinfo->srvoptions, "type")) {
            appendPQExpBuffer(q, ", type \'hdfs\'");
        }
        appendPQExpBuffer(q, " \n)");
    }

    appendPQExpBuffer(q, ";\n");

    appendPQExpBuffer(delq, "DROP SERVER IF EXISTS %s;\n", qsrvname);

    appendPQExpBuffer(labelq, "SERVER %s", qsrvname);

    if (binary_upgrade)
        binary_upgrade_extension_member(q, &srvinfo->dobj, labelq->data);

    ArchiveEntry(fout,
        srvinfo->dobj.catId,
        srvinfo->dobj.dumpId,
        srvinfo->dobj.name,
        NULL,
        NULL,
        srvinfo->rolname,
        false,
        "SERVER",
        SECTION_PRE_DATA,
        q->data,
        delq->data,
        NULL,
        NULL,
        0,
        NULL,
        NULL);

    /* Handle the ACL */
    dumpACL(fout,
        srvinfo->dobj.catId,
        srvinfo->dobj.dumpId,
        "FOREIGN SERVER",
        qsrvname,
        NULL,
        srvinfo->dobj.name,
        NULL,
        srvinfo->rolname,
        srvinfo->srvacl);

    /* Dump user mappings */
    dumpUserMappings(fout, srvinfo->dobj.name, NULL, srvinfo->rolname, srvinfo->dobj.catId, srvinfo->dobj.dumpId);

    /* Dump Foreign Server Comments */
    dumpComment(fout, labelq->data, NULL, srvinfo->rolname, srvinfo->dobj.catId, 0, srvinfo->dobj.dumpId);

    free(qsrvname);
    qsrvname = NULL;

    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
    destroyPQExpBuffer(labelq);
    destroyPQExpBuffer(query);
    if (res == NULL) {
        return;
    }
    GS_FREE(res->tuples);
    GS_FREE(res->paramDescs);
    GS_FREE(res->events);
    GS_FREE(res->errMsg);
    GS_FREE(res->errFields);
    GS_FREE(res->curBlock);
    GS_FREE(res);
}

/*
 * dumpUserMappings
 *
 * This routine is used to dump any user mappings associated with the
 * server handed to this routine. Should be called after ArchiveEntry()
 * for the server.
 */
static void dumpUserMappings(
    Archive* fout, const char* servername, const char* nmspace, const char* owner, CatalogId catalogId, DumpId dumpId)
{
    PQExpBuffer q;
    PQExpBuffer delq;
    PQExpBuffer query;
    PQExpBuffer tag;
    PGresult* res = NULL;
    int ntups;
    int i_usename;
    int i_umoptions;
    int i;

    q = createPQExpBuffer();
    tag = createPQExpBuffer();
    delq = createPQExpBuffer();
    query = createPQExpBuffer();

    /*
     * We read from the publicly accessible view pg_user_mappings, so as not
     * to fail if run by a non-superuser.  Note that the view will show
     * umoptions as null if the user hasn't got privileges for the associated
     * server; this means that pg_dump will dump such a mapping, but with no
     * OPTIONS clause.	A possible alternative is to skip such mappings
     * altogether, but it's not clear that that's an improvement.
     */
    selectSourceSchema(fout, "pg_catalog");

    appendPQExpBuffer(query,
        "SELECT usename, "
        "pg_catalog.array_to_string(ARRAY("
        "SELECT pg_catalog.quote_ident(option_name) || ' ' || "
        "pg_catalog.quote_literal(option_value) "
        "FROM pg_catalog.pg_options_to_table(umoptions) "
        "ORDER BY option_name"
        "), E',\n    ') AS umoptions "
        "FROM pg_user_mappings "
        "WHERE srvid = '%u' "
        "ORDER BY usename",
        catalogId.oid);

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    ntups = PQntuples(res);
    i_usename = PQfnumber(res, "usename");
    i_umoptions = PQfnumber(res, "umoptions");

    for (i = 0; i < ntups; i++) {
        char* usename = NULL;
        char* umoptions = NULL;

        usename = PQgetvalue(res, i, i_usename);
        umoptions = PQgetvalue(res, i, i_umoptions);

        resetPQExpBuffer(q);
        appendPQExpBuffer(q, "CREATE USER MAPPING FOR %s", fmtId(usename));
        appendPQExpBuffer(q, " SERVER %s", fmtId(servername));

        if ((umoptions != NULL) && strlen(umoptions) > 0)
            appendPQExpBuffer(q, " OPTIONS (\n    %s\n)", umoptions);

        appendPQExpBuffer(q, ";\n");

        resetPQExpBuffer(delq);
        appendPQExpBuffer(delq, "DROP USER MAPPING FOR %s", fmtId(usename));
        appendPQExpBuffer(delq, " SERVER %s;\n", fmtId(servername));

        resetPQExpBuffer(tag);
        appendPQExpBuffer(tag, "USER MAPPING %s SERVER %s", usename, servername);

        ArchiveEntry(fout,
            nilCatalogId,
            createDumpId(),
            tag->data,
            nmspace,
            NULL,
            owner,
            false,
            "USER MAPPING",
            SECTION_PRE_DATA,
            q->data,
            delq->data,
            NULL,
            &dumpId,
            1,
            NULL,
            NULL);
    }

    PQclear(res);

    destroyPQExpBuffer(query);
    destroyPQExpBuffer(delq);
    destroyPQExpBuffer(q);
    destroyPQExpBuffer(tag);
}

/*
 * Write out default privileges information
 */
static void dumpDefaultACL(Archive* fout, DefaultACLInfo* daclinfo)
{
    PQExpBuffer q;
    PQExpBuffer tag;
    PQExpBuffer res;
    const char* type = NULL;
    ArchiveHandle* AH = (ArchiveHandle*)fout;
    bool result = false;
    /* the execute role */
    bool execRole = false;
    /* input--role */
    bool inputRole = false;
    /* acl role */
    bool aclRole = false;

    /* Skip if not to be dumped */
    if (!daclinfo->dobj.dump || dataOnly || aclsSkip)
        return;

    if (isExecUserNotObjectOwner(fout, daclinfo->defaclrole))
        return;

    switch (daclinfo->defaclobjtype) {
        case DEFACLOBJ_RELATION:
            type = "TABLES";
            break;
        case DEFACLOBJ_SEQUENCE:
            type = "SEQUENCES";
            break;
        case DEFACLOBJ_LARGE_SEQUENCE:
            type = "LARGE SEQUENCES";
            break;
        case DEFACLOBJ_FUNCTION:
            type = "FUNCTIONS";
            break;
        case DEFACLOBJ_TYPE:
            type = "TYPES";
            break;
        case DEFACLOBJ_GLOBAL_SETTING:
        case DEFACLOBJ_COLUMN_SETTING:
            return;
        default:
            /* shouldn't get here */
            exit_horribly(NULL, "unrecognized object type in default privileges: %d\n", (int)daclinfo->defaclobjtype);
            type = ""; /* keep compiler quiet */
            break;
    }

    q = createPQExpBuffer();
    res = createPQExpBuffer();
    tag = createPQExpBuffer();

    appendPQExpBuffer(tag, "DEFAULT PRIVILEGES FOR %s", type);

    /* build the actual command(s) for this tuple */
    result = buildDefaultACLCommands(type,
        daclinfo->dobj.nmspace != NULL ? daclinfo->dobj.nmspace->dobj.name : NULL,
        daclinfo->defaclacl,
        daclinfo->defaclrole,
        fout->remoteVersion,
        q,
        AH->connection);

    /* check the execute role , --role and the acl user, whether it is supper role */
    execRole = isExecUserSuperRole(fout);
    aclRole = isSuperRole(AH->connection, daclinfo->defaclrole);

    /*
     * If --role is not null, it means that the real execute role is --role
     * else, the real execute role is execRole
     */
    if (NULL != use_role) {
        inputRole = isSuperRole(AH->connection, use_role);
        if (inputRole && !aclRole) {
            (void)appendPQExpBuffer(res,
                "ALTER USER %s SYSADMIN;\n"
                "%s"
                "ALTER USER %s NOSYSADMIN;\n",
                fmtId(daclinfo->defaclrole),
                q->data,
                fmtId(daclinfo->defaclrole));
        } else {
            (void)appendPQExpBuffer(res, "%s", q->data);
        }
    } else {

        if (execRole && !aclRole) {
            (void)appendPQExpBuffer(res,
                "ALTER USER %s SYSADMIN;\n"
                "%s"
                "ALTER USER %s NOSYSADMIN;\n",
                fmtId(daclinfo->defaclrole),
                q->data,
                fmtId(daclinfo->defaclrole));
        } else {
            (void)appendPQExpBuffer(res, "%s", q->data);
        }
    }

    if (!result) {
        exit_horribly(NULL, "could not parse default ACL list (%s)\n", daclinfo->defaclacl);
    }

    ArchiveEntry(fout,
        daclinfo->dobj.catId,
        daclinfo->dobj.dumpId,
        tag->data,
        daclinfo->dobj.nmspace != NULL ? daclinfo->dobj.nmspace->dobj.name : NULL,
        NULL,
        daclinfo->defaclrole,
        false,
        "DEFAULT ACL",
        SECTION_POST_DATA,
        res->data,
        "",
        NULL,
        NULL,
        0,
        NULL,
        NULL);

    destroyPQExpBuffer(tag);
    destroyPQExpBuffer(q);
    destroyPQExpBuffer(res);
}

/* ----------
 * Write out grant/revoke information
 *
 * 'objCatId' is the catalog ID of the underlying object.
 * 'objDumpId' is the dump ID of the underlying object.
 * 'type' must be one of
 *		TABLE, SEQUENCE, FUNCTION, LANGUAGE, SCHEMA, DATABASE, TABLESPACE,
 *		FOREIGN DATA WRAPPER, SERVER, or LARGE OBJECT.
 * 'name' is the formatted name of the object.	Must be quoted etc. already.
 * 'subname' is the formatted name of the sub-object, if any.  Must be quoted.
 * 'tag' is the tag for the archive entry (typ. unquoted name of object).
 * 'nspname' is the nmspace the object is in (NULL if none).
 * 'owner' is the owner, NULL if there is no owner (for languages).
 * 'acls' is the string read out of the fooacl system catalog field;
 *		it will be parsed here.
 * ----------
 */
static void dumpACL(Archive* fout, CatalogId objCatId, DumpId objDumpId, const char* type, const char* name,
    const char* subname, const char* tag, const char* nspname, const char* owner, const char* acls)
{
    PQExpBuffer sql;
    ArchiveHandle* AH = (ArchiveHandle*)fout;
    /* Do nothing if ACL dump is not enabled */
    if (aclsSkip)
        return;

    /* --data-only skips ACLs *except* BLOB ACLs */
    if (dataOnly && strcmp(type, "LARGE OBJECT") != 0)
        return;

    sql = createPQExpBuffer();

    if (!buildACLCommands(name, subname, type, acls, owner, "", fout->remoteVersion, sql, AH->connection))
        exit_horribly(NULL, "could not parse ACL list (%s) for object \"%s\" (%s)\n", acls, name, type);

    if (sql->len > 0)
        ArchiveEntry(fout,
            nilCatalogId,
            createDumpId(),
            tag,
            nspname,
            NULL,
            owner != NULL ? owner : "",
            false,
            "ACL",
            SECTION_NONE,
            sql->data,
            "",
            NULL,
            &(objDumpId),
            1,
            NULL,
            NULL);

    destroyPQExpBuffer(sql);
}

/*
 * dumpSecLabel
 *
 * This routine is used to dump any security labels associated with the
 * object handed to this routine. The routine takes a constant character
 * string for the target part of the security-label command, plus
 * the nmspace and owner of the object (for labeling the ArchiveEntry),
 * plus catalog ID and subid which are the lookup key for pg_seclabel,
 * plus the dump ID for the object (for setting a dependency).
 * If a matching pg_seclabel entry is found, it is dumped.
 *
 * Note: although this routine takes a dumpId for dependency purposes,
 * that purpose is just to mark the dependency in the emitted dump file
 * for possible future use by pg_restore.  We do NOT use it for determining
 * ordering of the label in the dump file, because this routine is called
 * after dependency sorting occurs.  This routine should be called just after
 * calling ArchiveEntry() for the specified object.
 */
static void dumpSecLabel(Archive* fout, const char* target, const char* nmspace, const char* owner, CatalogId catalogId,
    int subid, DumpId dumpId)
{
    SecLabelItem* labels = NULL;
    int nlabels;
    int i;
    PQExpBuffer query;

    /* do nothing, if --no-security-labels is supplied */
    if (no_security_labels)
        return;

    /* Comments are schema not data ... except blob comments are data */
    if (strncmp(target, "LARGE OBJECT ", 13) != 0) {
        if (dataOnly)
            return;
    } else {
        if (schemaOnly)
            return;
    }

    /* Search for security labels associated with catalogId, using table */
    nlabels = findSecLabels(fout, catalogId.tableoid, catalogId.oid, &labels);

    query = createPQExpBuffer();

    for (i = 0; i < nlabels; i++) {
        /*
         * Ignore label entries for which the subid doesn't match.
         */
        if (labels[i].objsubid != subid)
            continue;

        appendPQExpBuffer(query, "SECURITY LABEL FOR %s ON %s IS ", fmtId(labels[i].provider), target);
        appendStringLiteralAH(query, labels[i].label, fout);
        appendPQExpBuffer(query, ";\n");
    }

    if (query->len > 0) {
        ArchiveEntry(fout,
            nilCatalogId,
            createDumpId(),
            target,
            nmspace,
            NULL,
            owner,
            false,
            "SECURITY LABEL",
            SECTION_NONE,
            query->data,
            "",
            NULL,
            &(dumpId),
            1,
            NULL,
            NULL);
    }
    destroyPQExpBuffer(query);
}

/*
 * dumpTableSecLabel
 *
 * As above, but dump security label for both the specified table (or view)
 * and its columns.
 */
static void dumpTableSecLabel(Archive* fout, TableInfo* tbinfo, const char* reltypename)
{
    SecLabelItem* labels = NULL;
    int nlabels;
    int i;
    PQExpBuffer query;
    PQExpBuffer target;

    /* do nothing, if --no-security-labels is supplied */
    if (no_security_labels)
        return;

    /* SecLabel are SCHEMA not data */
    if (dataOnly)
        return;

    /* Search for comments associated with relation, using table */
    nlabels = findSecLabels(fout, tbinfo->dobj.catId.tableoid, tbinfo->dobj.catId.oid, &labels);

    /* If security labels exist, build SECURITY LABEL statements */
    if (nlabels <= 0)
        return;

    query = createPQExpBuffer();
    target = createPQExpBuffer();

    for (i = 0; i < nlabels; i++) {
        const char* colname = NULL;
        const char* provider = labels[i].provider;
        const char* label = labels[i].label;
        int objsubid = labels[i].objsubid;

        resetPQExpBuffer(target);
        if (objsubid == 0) {
            appendPQExpBuffer(target, "%s %s", reltypename, fmtId(tbinfo->dobj.name));
        } else {
            colname = getAttrName(objsubid, tbinfo);
            /* first fmtId result must be consumed before calling it again */
            appendPQExpBuffer(target, "COLUMN %s", fmtId(tbinfo->dobj.name));
            appendPQExpBuffer(target, ".%s", fmtId(colname));
        }
        appendPQExpBuffer(query, "SECURITY LABEL FOR %s ON %s IS ", fmtId(provider), target->data);
        appendStringLiteralAH(query, label, fout);
        appendPQExpBuffer(query, ";\n");
    }
    if (query->len > 0) {
        resetPQExpBuffer(target);
        appendPQExpBuffer(target, "%s %s", reltypename, fmtId(tbinfo->dobj.name));
        ArchiveEntry(fout,
            nilCatalogId,
            createDumpId(),
            target->data,
            tbinfo->dobj.nmspace->dobj.name,
            NULL,
            tbinfo->rolname,
            false,
            "SECURITY LABEL",
            SECTION_NONE,
            query->data,
            "",
            NULL,
            &(tbinfo->dobj.dumpId),
            1,
            NULL,
            NULL);
    }
    destroyPQExpBuffer(query);
    destroyPQExpBuffer(target);
}

/*
 * findSecLabels
 *
 * Find the security label(s), if any, associated with the given object.
 * All the objsubid values associated with the given classoid/objoid are
 * found with one search.
 */
static int findSecLabels(Archive* fout, Oid classoid, Oid objoid, SecLabelItem** items)
{
    /* static storage for table of security labels */
    static SecLabelItem* labels = NULL;
    static int nlabels = -1;

    SecLabelItem* smiddle = NULL;
    SecLabelItem* slow = NULL;
    SecLabelItem* shigh = NULL;
    int nmatch;

    /* Get security labels if we didn't already */
    if (nlabels < 0)
        nlabels = collectSecLabels(fout, &labels);

    if (nlabels <= 0) /* no labels, so no match is possible */
    {
        *items = NULL;
        return 0;
    }

    /*
     * Do binary search to find some item matching the object.
     */
    slow = &labels[0];
    shigh = &labels[nlabels - 1];
    while (slow <= shigh) {
        smiddle = slow + (shigh - slow) / 2;

        if (classoid < smiddle->classoid)
            shigh = smiddle - 1;
        else if (classoid > smiddle->classoid)
            slow = smiddle + 1;
        else if (objoid < smiddle->objoid)
            shigh = smiddle - 1;
        else if (objoid > smiddle->objoid)
            slow = smiddle + 1;
        else
            break; /* found a match */
    }

    if (slow > shigh) /* no matches */
    {
        *items = NULL;
        return 0;
    }

    /*
     * Now determine how many items match the object.  The search loop
     * invariant still holds: only items between low and high inclusive could
     * match.
     */
    nmatch = 1;
    while (smiddle > slow) {
        if (classoid != smiddle[-1].classoid || objoid != smiddle[-1].objoid)
            break;
        smiddle--;
        nmatch++;
    }

    *items = smiddle;

    smiddle += nmatch;
    while (smiddle <= shigh) {
        if (classoid != smiddle->classoid || objoid != smiddle->objoid)
            break;
        smiddle++;
        nmatch++;
    }

    return nmatch;
}

/*
 * collectSecLabels
 *
 * Construct a table of all security labels available for database objects.
 * It's much faster to pull them all at once.
 *
 * The table is sorted by classoid/objid/objsubid for speed in lookup.
 */
static int collectSecLabels(Archive* fout, SecLabelItem** items)
{
    PGresult* res = NULL;
    PQExpBuffer query;
    int i_label;
    int i_provider;
    int i_classoid;
    int i_objoid;
    int i_objsubid;
    int ntups;
    int i;
    SecLabelItem* labels = NULL;

    query = createPQExpBuffer();

    appendPQExpBuffer(query,
        "SELECT label, provider, classoid, objoid, objsubid "
        "FROM pg_catalog.pg_seclabel "
        "ORDER BY classoid, objoid, objsubid");

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    /* Construct lookup table containing OIDs in numeric form */
    i_label = PQfnumber(res, "label");
    i_provider = PQfnumber(res, "provider");
    i_classoid = PQfnumber(res, "classoid");
    i_objoid = PQfnumber(res, "objoid");
    i_objsubid = PQfnumber(res, "objsubid");

    ntups = PQntuples(res);

    labels = (SecLabelItem*)pg_malloc(ntups * sizeof(SecLabelItem));

    for (i = 0; i < ntups; i++) {
        labels[i].label = PQgetvalue(res, i, i_label);
        labels[i].provider = PQgetvalue(res, i, i_provider);
        labels[i].classoid = atooid(PQgetvalue(res, i, i_classoid));
        labels[i].objoid = atooid(PQgetvalue(res, i, i_objoid));
        labels[i].objsubid = atoi(PQgetvalue(res, i, i_objsubid));
    }

    /* Do NOT free the PGresult since we are keeping pointers into it */
    destroyPQExpBuffer(query);

    *items = labels;
    PQclear(res);
    return ntups;
}

/*
 * dumpTable
 *	  write out to fout the declarations (not data) of a user-defined table
 */
static void dumpTable(Archive* fout, TableInfo* tbinfo)
{
    if (tbinfo->dobj.dump && !dataOnly) {
        char* namecopy = NULL;

        if (RELKIND_IS_SEQUENCE(tbinfo->relkind))
            dumpSequence(fout, tbinfo, tbinfo->relkind == RELKIND_LARGE_SEQUENCE);
        else
            dumpTableSchema(fout, tbinfo);

        /* Handle the ACL here */
        namecopy = gs_strdup(fmtId(tbinfo->dobj.name));
        const char* kind = RELKIND_IS_SEQUENCE(tbinfo->relkind) ?
            (tbinfo->relkind == RELKIND_SEQUENCE ? "SEQUENCE" : "LARGE SEQUENCE") :
            "TABLE";
        dumpACL(fout,
            tbinfo->dobj.catId,
            tbinfo->dobj.dumpId,
            kind,
            namecopy,
            NULL,
            tbinfo->dobj.name,
            tbinfo->dobj.nmspace->dobj.name,
            tbinfo->rolname,
            tbinfo->relacl);

        /*
         * Handle column ACLs, if any.	Note: we pull these with a separate
         * query rather than trying to fetch them during getTableAttrs, so
         * that we won't miss ACLs on system columns.
         */
        if (fout->remoteVersion >= 80400) {
            PQExpBuffer query = createPQExpBuffer();
            PGresult* res = NULL;
            int i = 0;

            appendPQExpBuffer(query,
                "SELECT attname, attacl FROM pg_catalog.pg_attribute "
                "WHERE attrelid = '%u' AND NOT attisdropped AND attacl IS NOT NULL "
                "ORDER BY attnum",
                tbinfo->dobj.catId.oid);
            res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

            for (i = 0; i < PQntuples(res); i++) {
                char* attname = PQgetvalue(res, i, 0);
                char* attacl = PQgetvalue(res, i, 1);
                char* attnamecopy = NULL;
                char* acltag = NULL;
                size_t acltaglen = 0;
                int nRet = 0;

                attnamecopy = gs_strdup(fmtId(attname));
                acltaglen = strlen(tbinfo->dobj.name) + strlen(attname) + 2;
                acltag = (char*)pg_malloc(acltaglen);
                nRet = snprintf_s(acltag, acltaglen, acltaglen - 1, "%s.%s", tbinfo->dobj.name, attname);
                securec_check_ss_c(nRet, acltag, "\0");
                /* Column's GRANT type is always TABLE */
                dumpACL(fout,
                    tbinfo->dobj.catId,
                    tbinfo->dobj.dumpId,
                    "TABLE",
                    namecopy,
                    attnamecopy,
                    acltag,
                    tbinfo->dobj.nmspace->dobj.name,
                    tbinfo->rolname,
                    attacl);
                free(attnamecopy);
                attnamecopy = NULL;
                free(acltag);
                acltag = NULL;
            }
            PQclear(res);
            destroyPQExpBuffer(query);
        }

        free(namecopy);
        namecopy = NULL;
    }
}

static void bupgrade_set_pg_partition_cstore_oids(
    Archive* fout, PQExpBuffer upgrade_buffer, TableInfo* tbinfo, IndxInfo* idxinfo, bool is_index)
{
    PQExpBuffer upgrade_query = createPQExpBuffer();
    PGresult* upgrade_res = NULL;
    PGresult* upgrade_res1 = NULL;
    int32 partkeynum;
    Oid* partkeycols = NULL;
    int32 i = 0;
    int32 ntups = 0;

    int32 i_psort_relid = 0;
    int32 i_psort_reltype = 0;
    int32 i_psort_array_type = 0;

    int32 i_reldeltarelid = 0;
    int32 i_delta_reltype = 0;
    int32 i_delta_array_type = 0;
    int32 i_relcudescrelid = 0;
    int32 i_cudesc_reltype = 0;
    int32 i_cudesc_array_type = 0;
    int32 i_relcudescidx = 0;
    int32 i_cudesc_toast_relid = 0;
    int32 i_cudesc_toast_reltype = 0;
    int32 i_cudesc_toast_index = 0;
    int32 i_delta_toast_relid = 0;
    int32 i_delta_toast_reltype = 0;
    int32 i_delta_toast_index = 0;

    int32 i_reldeltarelidfs = 0;
    int32 i_relcudescrelidfs = 0;
    int32 i_relcudescidxfs = 0;
    int32 i_cudesc_toast_relidfs = 0;
    int32 i_cudesc_toast_indexfs = 0;
    int32 i_delta_toast_relidfs = 0;
    int32 i_delta_toast_indexfs = 0;
    int32 i_psort_relidfs = 0;

    appendPQExpBuffer(upgrade_query,
        "SELECT oid, pg_catalog.array_length(partkey, 1) AS partkeynum, partkey "
        "FROM pg_partition WHERE parentid = '%u' AND parttype = '%c'",
        tbinfo->dobj.catId.oid,
        PART_OBJ_TYPE_PARTED_TABLE);

    upgrade_res = ExecuteSqlQueryForSingleRow(fout, upgrade_query->data);

    partkeynum = atooid(PQgetvalue(upgrade_res, 0, PQfnumber(upgrade_res, "partkeynum")));
    partkeycols = (Oid*)pg_malloc(partkeynum * sizeof(Oid));
    parseOidArray(PQgetvalue(upgrade_res, 0, PQfnumber(upgrade_res, "partkey")), partkeycols, partkeynum);
    PQclear(upgrade_res);
    resetPQExpBuffer(upgrade_query);

    if (!is_index) {
        appendPQExpBuffer(upgrade_query,
            " SELECT p.reldeltarelid, d.relfilenode as reldeltarelidfs, d.reltype AS delta_reltype, "
            "	dt.typarray AS delta_array_type, "
            "	d.reldeltaidx, COALESCE(dif.relfilenode, 0::Oid) as reldeltaidxfs,"
            "	p.relcudescrelid, cu.relfilenode as relcudescrelidfs,"
            "	cu.reltype AS cudesc_reltype, "
            "	cut.typarray AS cudesc_array_type, "
            "	cu.relcudescidx, COALESCE(cif.relfilenode, 0::Oid) as relcudescidxfs,"
            "	cu.reltoastrelid as cudesc_toast_relid, COALESCE(ct.relfilenode, 0::Oid) as cudesc_toast_relidfs,"
            "	ct.reltype as cudesc_toast_reltype, "
            "	ct.reltoastidxid as cudesc_toast_index, COALESCE(cti.relfilenode, 0::Oid) as cudesc_toast_indexfs,"
            "	COALESCE(dut.oid, 0::Oid) as delta_toast_relid, COALESCE(dut.relfilenode, 0::Oid) as "
            "delta_toast_relidfs,"
            "	COALESCE(dut.reltype, 0::Oid) as delta_toast_reltype, "
            "	COALESCE(dut.reltoastidxid, 0::Oid) as delta_toast_index,"
            "	COALESCE(dti.relfilenode, 0::Oid) as delta_toast_indexfs"
            "   FROM pg_partition p "
            "	INNER JOIN pg_class d ON (d.oid 	= p.reldeltarelid)"
            "	INNER JOIN pg_class cu ON (cu.oid 	= p.relcudescrelid)"
            "	LEFT JOIN pg_class dif ON (dif.oid 	= d.reldeltaidx)"
            "	LEFT JOIN pg_class dut ON (dut.oid 	= d.reltoastrelid)"
            "	LEFT JOIN pg_class cif ON (cif.oid 	= cu.relcudescidx )"
            "	LEFT JOIN pg_class ct ON (ct.oid	= cu.reltoastrelid )"
            "	LEFT JOIN pg_class cti ON (cti.oid 	= ct.reltoastidxid )"
            "	LEFT JOIN pg_type dt ON ( dt.oid	= d.reltype )"
            "	LEFT JOIN pg_type cut ON ( cut.oid 	= cu.reltype )"
            "	LEFT JOIN pg_class dti ON (dti.oid 	= dut.reltoastidxid )	   "
            "  WHERE p.parentid 	= %u "
            "    AND p.parttype 	= '%c' "
            "    AND p.partstrategy 	= '%c' "
            "ORDER BY ",
            tbinfo->dobj.catId.oid,
            PART_OBJ_TYPE_TABLE_PARTITION,
            PART_STRATEGY_RANGE);
    } else {
        appendPQExpBuffer(upgrade_query,
            "SELECT i.relcudescrelid as psort_relid, ps.relfilenode as psort_relidfs, ps.reltype AS psort_reltype,"
            "       dps.typarray AS psort_array_type, ps.reldeltarelid, d.relfilenode as reldeltarelidfs, d.reltype AS "
            "delta_reltype,"
            "       dt.typarray AS delta_array_type, d.reldeltaidx, coalesce(dfn.relfilenode, 0::Oid) AS reldeltaidxfs,"
            "       ps.relcudescrelid, cu.relfilenode as relcudescrelidfs, cu.reltype AS cudesc_reltype,"
            "       cut.typarray AS cudesc_array_type, cu.relcudescidx, cuips.relfilenode AS relcudescidxfs,"
            "       cu.reltoastrelid as cudesc_toast_relid, ct.relfilenode AS cudesc_toast_relidfs,"
            "       ct.reltype as cudesc_toast_reltype, ct.reltoastidxid as cudesc_toast_index,"
            "       coalesce(ctps.relfilenode, 0::Oid) AS cudesc_toast_indexfs,"
            "       coalesce(dut.oid, 0::Oid) as delta_toast_relid, coalesce(dut.relfilenode, 0::Oid) AS "
            "delta_toast_relidfs,"
            "       coalesce(dut.reltype, 0::Oid) as delta_toast_reltype, "
            "       coalesce(dut.reltoastidxid, 0::Oid) as delta_toast_index, coalesce(dutps.relfilenode, 0::Oid) as "
            "delta_toast_indexfs"
            "  FROM pg_class i "
            "       INNER JOIN pg_class ps"
            "    ON ps.oid   = i.relcudescrelid"
            "       INNER JOIN pg_class d"
            "    ON d.oid	= ps.reldeltarelid"
            "       LEFT JOIN pg_class dfn"
            "    ON dfn.oid	= d.reldeltaidx"
            "       LEFT JOIN pg_class dut "
            "    ON dut.oid = d.reltoastrelid"
            "       INNER JOIN pg_class cu"
            "    ON cu.oid	= ps.relcudescrelid"
            "       INNER JOIN pg_class ct"
            "    ON ct.oid	= cu.reltoastrelid"
            "       INNER JOIN pg_type dps"
            "    ON dps.oid	= ps.reltype   "
            "       INNER JOIN pg_type dt"
            "    ON dt.oid	= d.reltype"
            "       INNER JOIN pg_type cut"
            "    ON cut.oid	= cu.reltype"
            "       LEFT JOIN pg_class cuips"
            "    ON cuips.oid = cu.relcudescidx "
            "       LEFT JOIN pg_class ctps"
            "    ON ctps.oid = ct.reltoastidxid"
            "       LEFT JOIN pg_class dutps"
            "    ON dutps.oid = dut.reltoastidxid"
            "  WHERE i.oid  =  %u ",
            idxinfo->dobj.catId.oid);

        upgrade_res1 = ExecuteSqlQuery(fout, upgrade_query->data, PGRES_TUPLES_OK);
        ntups = PQntuples(upgrade_res1);
        if (ntups == 0) {
            PQclear(upgrade_res1);
            free(partkeycols);
            partkeycols = NULL;
            destroyPQExpBuffer(upgrade_query);
            return;
        }

        resetPQExpBuffer(upgrade_query);
        appendPQExpBuffer(upgrade_query,
            "SELECT pi.relcudescrelid as psort_relid, ps.relfilenode as psort_relidfs, ps.reltype AS psort_reltype,"
            "        dps.typarray AS psort_array_type, ps.reldeltarelid, d.relfilenode as reldeltarelidfs, d.reltype "
            "AS delta_reltype,"
            "        dt.typarray AS delta_array_type, d.reldeltaidx, coalesce(dfn.relfilenode, 0::Oid) AS "
            "reldeltaidxfs, "
            "    ps.relcudescrelid, cu.relfilenode as relcudescrelidfs, cu.reltype AS cudesc_reltype, "
            "    cut.typarray AS cudesc_array_type, cu.relcudescidx, cuips.relfilenode AS relcudescidxfs, "
            "    cu.reltoastrelid as cudesc_toast_relid, ct.relfilenode AS cudesc_toast_relidfs,   "
            "    ct.reltype as cudesc_toast_reltype, ct.reltoastidxid as cudesc_toast_index,"
            "        coalesce(ctps.relfilenode, 0::Oid) AS cudesc_toast_indexfs,"
            "        coalesce(dut.oid, 0::Oid) as delta_toast_relid, coalesce(dut.relfilenode, 0::Oid) AS "
            "delta_toast_relidfs, "
            "    coalesce(dut.reltype, 0::Oid) as delta_toast_reltype, "
            "        coalesce(dut.reltoastidxid, 0::Oid) as delta_toast_index, coalesce(dutps.relfilenode, 0::Oid) as "
            "delta_toast_indexfs "
            " FROM pg_partition p"
            "      INNER JOIN pg_partition pi"
            "   ON p.oid = pi.indextblid"
            "      INNER JOIN pg_class ps"
            "   ON ps.oid = pi.relcudescrelid"
            "      INNER JOIN pg_class d "
            "   ON d.oid	= ps.reldeltarelid"
            "       LEFT JOIN pg_class dfn"
            "    ON dfn.oid	= d.reldeltaidx"
            "      LEFT JOIN pg_class dut "
            "   ON dut.oid = d.reltoastrelid"
            "      INNER JOIN pg_class cu"
            "   ON cu.oid	= ps.relcudescrelid"
            "      INNER JOIN pg_class ct"
            "   ON ct.oid	= cu.reltoastrelid"
            "      INNER JOIN pg_type dps"
            "   ON dps.oid	= ps.reltype"
            "      INNER JOIN pg_type dt"
            "   ON dt.oid	= d.reltype"
            "      INNER JOIN pg_type cut"
            "   ON cut.oid	= cu.reltype"
            "       LEFT JOIN pg_class cuips"
            "    ON cuips.oid = cu.relcudescidx "
            "       LEFT JOIN pg_class ctps"
            "    ON ctps.oid = ct.reltoastidxid"
            "       LEFT JOIN pg_class dutps"
            "    ON dutps.oid = dut.reltoastidxid"
            " WHERE p.parentid = %u "
            "  AND p.parttype = '%c' "
            "  AND p.partstrategy = '%c' "
            "  AND pi.parentid = %u "
            "  AND pi.parttype = '%c' "
            " ORDER BY ",
            tbinfo->dobj.catId.oid,
            PART_OBJ_TYPE_TABLE_PARTITION,
            PART_STRATEGY_RANGE,
            idxinfo->dobj.catId.oid,
            PART_OBJ_TYPE_INDEX_PARTITION);
    }

    for (i = 1; i <= partkeynum; i++) {
        if (i == partkeynum)
            appendPQExpBuffer(
                upgrade_query, "p.boundaries[%d]::%s ASC NULLS LAST", i, tbinfo->atttypnames[partkeycols[i - 1] - 1]);
        else
            appendPQExpBuffer(upgrade_query, "p.boundaries[%d]::%s NULLS LAST, ", i, tbinfo->atttypnames[partkeycols[i - 1] - 1]);
    }

    upgrade_res = ExecuteSqlQuery(fout, upgrade_query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(upgrade_res);
    if (ntups == 0) {
        PQclear(upgrade_res);
        PQclear(upgrade_res1);
        free(partkeycols);
        partkeycols = NULL;
        destroyPQExpBuffer(upgrade_query);
        return;
    }

    if (is_index) {
        i_psort_relid = PQfnumber(upgrade_res, "psort_relid");
        i_psort_relidfs = PQfnumber(upgrade_res, "psort_relidfs");
        i_psort_reltype = PQfnumber(upgrade_res, "psort_reltype");
        i_psort_array_type = PQfnumber(upgrade_res, "psort_array_type");
    }

    i_reldeltarelid = PQfnumber(upgrade_res, "reldeltarelid");
    i_delta_reltype = PQfnumber(upgrade_res, "delta_reltype");
    i_delta_array_type = PQfnumber(upgrade_res, "delta_array_type");
    i_relcudescrelid = PQfnumber(upgrade_res, "relcudescrelid");
    i_cudesc_reltype = PQfnumber(upgrade_res, "cudesc_reltype");
    i_cudesc_array_type = PQfnumber(upgrade_res, "cudesc_array_type");
    i_relcudescidx = PQfnumber(upgrade_res, "relcudescidx");
    i_cudesc_toast_relid = PQfnumber(upgrade_res, "cudesc_toast_relid");
    i_cudesc_toast_reltype = PQfnumber(upgrade_res, "cudesc_toast_reltype");
    i_cudesc_toast_index = PQfnumber(upgrade_res, "cudesc_toast_index");

    i_delta_toast_relid = PQfnumber(upgrade_res, "delta_toast_relid");
    i_delta_toast_reltype = PQfnumber(upgrade_res, "delta_toast_reltype");
    i_delta_toast_index = PQfnumber(upgrade_res, "delta_toast_index");

    i_reldeltarelidfs = PQfnumber(upgrade_res, "reldeltarelidfs");
    i_relcudescrelidfs = PQfnumber(upgrade_res, "relcudescrelidfs");
    i_relcudescidxfs = PQfnumber(upgrade_res, "relcudescidxfs");
    i_cudesc_toast_relidfs = PQfnumber(upgrade_res, "cudesc_toast_relidfs");
    i_cudesc_toast_indexfs = PQfnumber(upgrade_res, "cudesc_toast_indexfs");
    i_delta_toast_relidfs = PQfnumber(upgrade_res, "delta_toast_relidfs");
    i_delta_toast_indexfs = PQfnumber(upgrade_res, "delta_toast_indexfs");

    if (is_index) {
        appendPQExpBuffer(upgrade_buffer, "SELECT binary_upgrade.set_next_cstore_psort_oid('{");

        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res1, 0, i_psort_relid));

        for (i = 0; i < ntups - 1; i++) {
            appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res, i, i_psort_relid));
        }

        appendPQExpBuffer(upgrade_buffer, "%s }');\n", PQgetvalue(upgrade_res, i, i_psort_relid));

        appendPQExpBuffer(upgrade_buffer, "SELECT binary_upgrade.set_next_cstore_psort_rfoid('{");

        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res1, 0, i_psort_relidfs));

        for (i = 0; i < ntups - 1; i++) {
            appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res, i, i_psort_relidfs));
        }

        appendPQExpBuffer(upgrade_buffer, "%s }');\n", PQgetvalue(upgrade_res, i, i_psort_relidfs));

        appendPQExpBuffer(upgrade_buffer, "SELECT binary_upgrade.set_next_cstore_psort_typoid('{");

        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res1, 0, i_psort_reltype));

        for (i = 0; i < ntups - 1; i++) {
            appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res, i, i_psort_reltype));
        }

        appendPQExpBuffer(upgrade_buffer, "%s }');\n", PQgetvalue(upgrade_res, i, i_psort_reltype));

        appendPQExpBuffer(upgrade_buffer, "SELECT binary_upgrade.set_next_cstore_psort_atypoid('{");

        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res1, 0, i_psort_array_type));

        for (i = 0; i < ntups - 1; i++) {
            appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res, i, i_psort_array_type));
        }

        appendPQExpBuffer(upgrade_buffer, "%s }');\n", PQgetvalue(upgrade_res, i, i_psort_array_type));
    }

    /*
     * Restructure the partition definition by checking the information in
     * pg_partition. Notes: the top boundary could be MAXVALUE, so we should
     * check before restructuring.
     */
    appendPQExpBuffer(upgrade_buffer, "SELECT binary_upgrade.set_next_cstore_delta_oid('{");
    if (is_index) {
        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res1, 0, i_reldeltarelid));
    }

    for (i = 0; i < ntups - 1; i++) {
        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res, i, i_reldeltarelid));
    }

    appendPQExpBuffer(upgrade_buffer, "%s }');\n", PQgetvalue(upgrade_res, i, i_reldeltarelid));

    appendPQExpBuffer(upgrade_buffer, "SELECT binary_upgrade.set_next_cstore_delta_rfoid('{");
    if (is_index) {
        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res1, 0, i_reldeltarelidfs));
    }

    for (i = 0; i < ntups - 1; i++) {
        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res, i, i_reldeltarelidfs));
    }

    appendPQExpBuffer(upgrade_buffer, "%s }');\n", PQgetvalue(upgrade_res, i, i_reldeltarelidfs));

    /*delta rel typeid */
    appendPQExpBuffer(upgrade_buffer, "SELECT binary_upgrade.set_next_cstore_delta_typoid('{");
    if (is_index) {
        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res1, 0, i_delta_reltype));
    }

    for (i = 0; i < ntups - 1; i++) {
        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res, i, i_delta_reltype));
    }

    appendPQExpBuffer(upgrade_buffer, "%s }');\n", PQgetvalue(upgrade_res, i, i_delta_reltype));

    /*delta rel array typeid */
    appendPQExpBuffer(upgrade_buffer, "SELECT binary_upgrade.set_next_cstore_delta_atypoid('{");
    if (is_index) {
        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res1, 0, i_delta_array_type));
    }

    for (i = 0; i < ntups - 1; i++) {
        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res, i, i_delta_array_type));
    }

    appendPQExpBuffer(upgrade_buffer, "%s }');\n", PQgetvalue(upgrade_res, i, i_delta_array_type));

    /*cudesc rel */
    appendPQExpBuffer(upgrade_buffer, "SELECT binary_upgrade.set_next_cstore_cudesc_oid('{");
    if (is_index) {
        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res1, 0, i_relcudescrelid));
    }

    for (i = 0; i < ntups - 1; i++) {
        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res, i, i_relcudescrelid));
    }

    appendPQExpBuffer(upgrade_buffer, "%s }');\n", PQgetvalue(upgrade_res, i, i_relcudescrelid));

    appendPQExpBuffer(upgrade_buffer, "SELECT binary_upgrade.set_next_cstore_cudesc_rfoid('{");
    if (is_index) {
        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res1, 0, i_relcudescrelidfs));
    }

    for (i = 0; i < ntups - 1; i++) {
        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res, i, i_relcudescrelidfs));
    }

    appendPQExpBuffer(upgrade_buffer, "%s }');\n", PQgetvalue(upgrade_res, i, i_relcudescrelidfs));

    /*cudesc rel type oid */
    appendPQExpBuffer(upgrade_buffer, "SELECT binary_upgrade.set_next_cstore_cudesc_typoid('{");
    if (is_index) {
        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res1, 0, i_cudesc_reltype));
    }

    for (i = 0; i < ntups - 1; i++) {
        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res, i, i_cudesc_reltype));
    }

    appendPQExpBuffer(upgrade_buffer, "%s }');\n", PQgetvalue(upgrade_res, i, i_cudesc_reltype));

    /*cudesc rel array type oid */
    appendPQExpBuffer(upgrade_buffer, "SELECT binary_upgrade.set_next_cstore_cudesc_atypoid('{");
    if (is_index) {
        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res1, 0, i_cudesc_array_type));
    }

    for (i = 0; i < ntups - 1; i++) {
        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res, i, i_cudesc_array_type));
    }

    appendPQExpBuffer(upgrade_buffer, "%s }');\n", PQgetvalue(upgrade_res, i, i_cudesc_array_type));

    /*cudes idx */
    appendPQExpBuffer(upgrade_buffer, "SELECT binary_upgrade.set_next_cstore_cudesc_idx_oid('{");
    if (is_index) {
        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res1, 0, i_relcudescidx));
    }

    for (i = 0; i < ntups - 1; i++) {
        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res, i, i_relcudescidx));
    }

    appendPQExpBuffer(upgrade_buffer, "%s }');\n", PQgetvalue(upgrade_res, i, i_relcudescidx));

    appendPQExpBuffer(upgrade_buffer, "SELECT binary_upgrade.set_next_cstore_cudesc_idx_rfoid('{");
    if (is_index) {
        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res1, 0, i_relcudescidxfs));
    }

    for (i = 0; i < ntups - 1; i++) {
        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res, i, i_relcudescidxfs));
    }

    appendPQExpBuffer(upgrade_buffer, "%s }');\n", PQgetvalue(upgrade_res, i, i_relcudescidxfs));

    /*cudes toast rel oid */
    appendPQExpBuffer(upgrade_buffer, "SELECT binary_upgrade.set_next_cstore_cudesc_toast_oid('{");
    if (is_index) {
        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res1, 0, i_cudesc_toast_relid));
    }

    for (i = 0; i < ntups - 1; i++) {
        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res, i, i_cudesc_toast_relid));
    }

    appendPQExpBuffer(upgrade_buffer, "%s }');\n", PQgetvalue(upgrade_res, i, i_cudesc_toast_relid));

    appendPQExpBuffer(upgrade_buffer, "SELECT binary_upgrade.set_next_cstore_cudesc_toast_rfoid('{");
    if (is_index) {
        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res1, 0, i_cudesc_toast_relidfs));
    }

    for (i = 0; i < ntups - 1; i++) {
        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res, i, i_cudesc_toast_relidfs));
    }

    appendPQExpBuffer(upgrade_buffer, "%s }');\n", PQgetvalue(upgrade_res, i, i_cudesc_toast_relidfs));

    /*cudes toast rel type */
    appendPQExpBuffer(upgrade_buffer, "SELECT binary_upgrade.set_next_cstore_cudesc_toast_typoid('{");
    if (is_index) {
        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res1, 0, i_cudesc_toast_reltype));
    }

    for (i = 0; i < ntups - 1; i++) {
        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res, i, i_cudesc_toast_reltype));
    }

    appendPQExpBuffer(upgrade_buffer, "%s }');\n", PQgetvalue(upgrade_res, i, i_cudesc_toast_reltype));

    /*cudesc toast_index */
    appendPQExpBuffer(upgrade_buffer, "SELECT binary_upgrade.set_next_cstore_cudesc_toast_idx_oid('{");
    if (is_index) {
        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res1, 0, i_cudesc_toast_index));
    }

    for (i = 0; i < ntups - 1; i++) {
        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res, i, i_cudesc_toast_index));
    }

    appendPQExpBuffer(upgrade_buffer, "%s }');\n", PQgetvalue(upgrade_res, i, i_cudesc_toast_index));

    appendPQExpBuffer(upgrade_buffer, "SELECT binary_upgrade.set_next_cstore_cudesc_toast_idx_rfoid('{");
    if (is_index) {
        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res1, 0, i_cudesc_toast_indexfs));
    }

    for (i = 0; i < ntups - 1; i++) {
        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res, i, i_cudesc_toast_indexfs));
    }

    appendPQExpBuffer(upgrade_buffer, "%s }');\n", PQgetvalue(upgrade_res, i, i_cudesc_toast_indexfs));

    /*delta toast rel oid */
    appendPQExpBuffer(upgrade_buffer, "SELECT binary_upgrade.set_next_cstore_delta_toast_oid('{");
    if (is_index) {
        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res1, 0, i_delta_toast_relid));
    }

    for (i = 0; i < ntups - 1; i++) {
        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res, i, i_delta_toast_relid));
    }

    appendPQExpBuffer(upgrade_buffer, "%s }');\n", PQgetvalue(upgrade_res, i, i_delta_toast_relid));

    appendPQExpBuffer(upgrade_buffer, "SELECT binary_upgrade.set_next_cstore_delta_toast_rfoid('{");
    if (is_index) {
        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res1, 0, i_delta_toast_relidfs));
    }

    for (i = 0; i < ntups - 1; i++) {
        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res, i, i_delta_toast_relidfs));
    }

    appendPQExpBuffer(upgrade_buffer, "%s }');\n", PQgetvalue(upgrade_res, i, i_delta_toast_relidfs));

    /*delta toast rel type */
    appendPQExpBuffer(upgrade_buffer, "SELECT binary_upgrade.set_next_cstore_delta_toast_typoid('{");
    if (is_index) {
        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res1, 0, i_delta_toast_reltype));
    }

    for (i = 0; i < ntups - 1; i++) {
        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res, i, i_delta_toast_reltype));
    }

    appendPQExpBuffer(upgrade_buffer, "%s }');\n", PQgetvalue(upgrade_res, i, i_delta_toast_reltype));

    /*delta toast_index */
    appendPQExpBuffer(upgrade_buffer, "SELECT binary_upgrade.set_next_cstore_delta_toast_idx_oid('{");
    if (is_index) {
        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res1, 0, i_delta_toast_index));
    }

    for (i = 0; i < ntups - 1; i++) {
        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res, i, i_delta_toast_index));
    }

    appendPQExpBuffer(upgrade_buffer, "%s }');\n", PQgetvalue(upgrade_res, i, i_delta_toast_index));

    appendPQExpBuffer(upgrade_buffer, "SELECT binary_upgrade.set_next_cstore_delta_toast_idx_rfoid('{");
    if (is_index) {
        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res1, 0, i_delta_toast_indexfs));
    }

    for (i = 0; i < ntups - 1; i++) {
        appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res, i, i_delta_toast_indexfs));
    }

    appendPQExpBuffer(upgrade_buffer, "%s }');\n", PQgetvalue(upgrade_res, i, i_delta_toast_indexfs));

    if (NULL != upgrade_res1) {
        PQclear(upgrade_res1);
    }
    if (NULL != partkeycols) {
        free(partkeycols);
        partkeycols = NULL;
    }
    PQclear(upgrade_res);
    destroyPQExpBuffer(upgrade_query);
}

static void bupgrade_set_cstore_oids(
    Archive* fout, PQExpBuffer upgrade_buffer, TableInfo* tbinfo, IndxInfo* idxinfo, bool is_index)
{
    PQExpBuffer upgrade_query = createPQExpBuffer();
    PGresult* upgrade_res = NULL;

    int32 i_psort_relid = 0;
    int32 i_psort_reltype = 0;
    int32 i_psort_array_type = 0;

    int32 i_reldeltarelid = 0;
    int32 i_delta_reltype = 0;
    int32 i_delta_array_type = 0;
    int32 i_reldeltaidx = 0;
    int32 i_relcudescrelid = 0;
    int32 i_cudesc_reltype = 0;
    int32 i_cudesc_array_type = 0;
    int32 i_relcudescidx = 0;
    int32 i_cudesc_toast_relid = 0;
    int32 i_cudesc_toast_reltype = 0;
    int32 i_cudesc_toast_index = 0;
    int32 i_delta_toast_relid = 0;
    int32 i_delta_toast_reltype = 0;
    int32 i_delta_toast_index = 0;

    int32 i_reldeltarelidfs = 0;
    int32 i_relcudescrelidfs = 0;
    int32 i_relcudescidxfs = 0;
    int32 i_cudesc_toast_relidfs = 0;
    int32 i_cudesc_toast_indexfs = 0;
    int32 i_delta_toast_relidfs = 0;
    int32 i_delta_toast_indexfs = 0;
    int32 i_psort_relidfs = 0;
    int32 i_reldeltaidxfs = 0;

    if (!is_index) {
        appendPQExpBuffer(upgrade_query,
            "SELECT c.oid relid, c.reldeltarelid, d.relfilenode as reldeltarelidfs, d.reltype AS delta_reltype, "
            "	 dt.typarray AS delta_array_type, d.reldeltaidx, coalesce(dips.relfilenode, 0::Oid) AS reldeltaidxfs,"
            "	 c.relcudescrelid, cu.relfilenode AS relcudescrelidfs, "
            "	 cu.reltype AS cudesc_reltype, cut.typarray AS cudesc_array_type, "
            "	 cu.relcudescidx, coalesce(cuips.relfilenode, 0::Oid) AS relcudescidxfs,  cu.reltoastrelid as "
            "cudesc_toast_relid, "
            "	 coalesce(ct.relfilenode, 0::Oid) AS cudesc_toast_relidfs, ct.reltype as cudesc_toast_reltype,"
            "	 ct.reltoastidxid as cudesc_toast_index, coalesce(ctps.relfilenode, 0::Oid) as cudesc_toast_indexfs,"
            "	 coalesce(dut.oid, 0::Oid) as delta_toast_relid, coalesce(dut.relfilenode, 0::Oid) as "
            "delta_toast_relidfs,"
            "	 coalesce(dut.reltype, 0::Oid) as delta_toast_reltype, "
            "	 coalesce(dut.reltoastidxid, 0::Oid) as delta_toast_index, coalesce(dutps.relfilenode, 0::Oid) as "
            "delta_toast_indexfs"
            "	FROM pg_class c inner join pg_class d "
            "	     on d.oid = c.reldeltarelid "
            "	 LEFT JOIN pg_class dut "
            "	     on dut.oid = d.reltoastrelid "
            "	 INNER JOIN pg_class cu "
            "	     on cu.oid = c.relcudescrelid "
            "	 INNER JOIN pg_class ct "
            "	     on ct.oid = cu.reltoastrelid "
            "	 INNER JOIN pg_type dt "
            "	     on dt.oid = d.reltype "
            "	 INNER JOIN pg_type cut "
            "	     on cut.oid = cu.reltype "
            "	 LEFT JOIN pg_class dips"
            "	     on dips.oid = d.reldeltaidx"
            "	 LEFT JOIN pg_class cuips"
            "	     on cuips.oid = cu.relcudescidx"
            "	 LEFT JOIN pg_class ctps"
            "	     on ctps.oid = ct.reltoastidxid"
            "	 LEFT JOIN pg_class dutps"
            "	     on dutps.oid = dut.reltoastidxid"
            "	WHERE c.oid = %u ;",
            tbinfo->dobj.catId.oid);
    } else {
        appendPQExpBuffer(upgrade_query,
            "SELECT i.relcudescrelid as psort_relid, ps.relfilenode as psort_relidfs, ps.reltype AS psort_reltype,"
            "	   dps.typarray AS psort_array_type, ps.reldeltarelid, d.relfilenode as reldeltarelidfs, d.reltype AS "
            "delta_reltype,"
            "	   dt.typarray AS delta_array_type, d.reldeltaidx, COALESCE(dfn.relfilenode, 0::Oid) AS reldeltaidxfs, "
            "	   ps.relcudescrelid, cu.relfilenode as relcudescrelidfs, cu.reltype AS cudesc_reltype,"
            "	   cut.typarray AS cudesc_array_type, cu.relcudescidx, cuips.relfilenode AS relcudescidxfs,"
            "	   cu.reltoastrelid as cudesc_toast_relid, ct.relfilenode AS cudesc_toast_relidfs,"
            "	   ct.reltype as cudesc_toast_reltype, ct.reltoastidxid as cudesc_toast_index, "
            "       COALESCE(ctps.relfilenode, 0::Oid) AS cudesc_toast_indexfs,"
            "	   COALESCE(dut.oid, 0::Oid) as delta_toast_relid, COALESCE(dut.relfilenode, 0::Oid) AS "
            "delta_toast_relidfs,"
            "	   COALESCE(dut.reltype, 0::Oid) as delta_toast_reltype, "
            "	   COALESCE(dut.reltoastidxid, 0::Oid) as delta_toast_index, COALESCE(dutps.relfilenode, 0::Oid) as "
            "delta_toast_indexfs"
            "  FROM pg_class i "
            "	   INNER JOIN pg_class ps "
            "	ON ps.oid = i.relcudescrelid"
            "	   INNER JOIN pg_class d "
            "	ON d.oid	= ps.reldeltarelid"
            "       LEFT JOIN pg_class dfn"
            "    ON dfn.oid	= d.reldeltaidx"
            "	   LEFT JOIN pg_class dut "
            "	ON dut.oid = d.reltoastrelid "
            "	   INNER JOIN pg_class cu "
            "	ON cu.oid	= ps.relcudescrelid"
            "	   INNER JOIN pg_class ct "
            "	ON ct.oid	= cu.reltoastrelid"
            "	   INNER JOIN pg_type dps "
            "	ON dps.oid	= ps.reltype"
            "	   INNER JOIN pg_type dt "
            "	ON dt.oid	= d.reltype"
            "	   INNER JOIN pg_type cut"
            "	ON cut.oid	= cu.reltype"
            "       LEFT JOIN pg_class cuips"
            "    ON cuips.oid = cu.relcudescidx "
            "       LEFT JOIN pg_class ctps"
            "    ON ctps.oid = ct.reltoastidxid"
            "       LEFT JOIN pg_class dutps"
            "    ON dutps.oid = dut.reltoastidxid"
            " WHERE i.oid  =  %u ;",
            idxinfo->dobj.catId.oid);
    }

    upgrade_res = ExecuteSqlQueryForSingleRow(fout, upgrade_query->data);

    if (is_index) {
        i_psort_relid = atooid(PQgetvalue(upgrade_res, 0, PQfnumber(upgrade_res, "psort_relid")));
        i_psort_reltype = atooid(PQgetvalue(upgrade_res, 0, PQfnumber(upgrade_res, "psort_reltype")));
        i_psort_array_type = atooid(PQgetvalue(upgrade_res, 0, PQfnumber(upgrade_res, "psort_array_type")));
        i_psort_relidfs = atooid(PQgetvalue(upgrade_res, 0, PQfnumber(upgrade_res, "psort_relidfs")));
    }

    i_reldeltarelid = atooid(PQgetvalue(upgrade_res, 0, PQfnumber(upgrade_res, "reldeltarelid")));
    i_delta_reltype = atooid(PQgetvalue(upgrade_res, 0, PQfnumber(upgrade_res, "delta_reltype")));
    i_delta_array_type = atooid(PQgetvalue(upgrade_res, 0, PQfnumber(upgrade_res, "delta_array_type")));
    i_reldeltaidx = atooid(PQgetvalue(upgrade_res, 0, PQfnumber(upgrade_res, "reldeltaidx")));
    i_relcudescrelid = atooid(PQgetvalue(upgrade_res, 0, PQfnumber(upgrade_res, "relcudescrelid")));
    i_cudesc_reltype = atooid(PQgetvalue(upgrade_res, 0, PQfnumber(upgrade_res, "cudesc_reltype")));
    i_cudesc_array_type = atooid(PQgetvalue(upgrade_res, 0, PQfnumber(upgrade_res, "cudesc_array_type")));
    i_relcudescidx = atooid(PQgetvalue(upgrade_res, 0, PQfnumber(upgrade_res, "relcudescidx")));
    i_cudesc_toast_relid = atooid(PQgetvalue(upgrade_res, 0, PQfnumber(upgrade_res, "cudesc_toast_relid")));
    i_cudesc_toast_reltype = atooid(PQgetvalue(upgrade_res, 0, PQfnumber(upgrade_res, "cudesc_toast_reltype")));
    i_cudesc_toast_index = atooid(PQgetvalue(upgrade_res, 0, PQfnumber(upgrade_res, "cudesc_toast_index")));
    i_delta_toast_relid = atooid(PQgetvalue(upgrade_res, 0, PQfnumber(upgrade_res, "delta_toast_relid")));
    i_delta_toast_reltype = atooid(PQgetvalue(upgrade_res, 0, PQfnumber(upgrade_res, "delta_toast_reltype")));
    i_delta_toast_index = atooid(PQgetvalue(upgrade_res, 0, PQfnumber(upgrade_res, "delta_toast_index")));

    i_reldeltarelidfs = atooid(PQgetvalue(upgrade_res, 0, PQfnumber(upgrade_res, "reldeltarelidfs")));
    i_reldeltaidxfs = atooid(PQgetvalue(upgrade_res, 0, PQfnumber(upgrade_res, "reldeltaidxfs")));
    i_relcudescrelidfs = atooid(PQgetvalue(upgrade_res, 0, PQfnumber(upgrade_res, "relcudescrelidfs")));
    i_relcudescidxfs = atooid(PQgetvalue(upgrade_res, 0, PQfnumber(upgrade_res, "relcudescidxfs")));
    i_cudesc_toast_relidfs = atooid(PQgetvalue(upgrade_res, 0, PQfnumber(upgrade_res, "cudesc_toast_relidfs")));
    i_cudesc_toast_indexfs = atooid(PQgetvalue(upgrade_res, 0, PQfnumber(upgrade_res, "cudesc_toast_indexfs")));
    i_delta_toast_relidfs = atooid(PQgetvalue(upgrade_res, 0, PQfnumber(upgrade_res, "delta_toast_relidfs")));
    i_delta_toast_indexfs = atooid(PQgetvalue(upgrade_res, 0, PQfnumber(upgrade_res, "delta_toast_indexfs")));

    if (is_index) {
        appendPQExpBuffer(upgrade_buffer,
            "SELECT binary_upgrade.set_next_cstore_psort_oid"
            "('{%d }');\n",
            i_psort_relid);
        appendPQExpBuffer(upgrade_buffer,
            "SELECT binary_upgrade.set_next_cstore_psort_rfoid"
            "('{%d }');\n",
            i_psort_relidfs);

        appendPQExpBuffer(upgrade_buffer,
            "SELECT binary_upgrade.set_next_cstore_psort_typoid"
            "('{%d }');\n",
            i_psort_reltype);

        appendPQExpBuffer(upgrade_buffer,
            "SELECT binary_upgrade.set_next_cstore_psort_atypoid"
            "('{%d }');\n",
            i_psort_array_type);
    }

    appendPQExpBuffer(upgrade_buffer,
        "SELECT binary_upgrade.set_next_cstore_delta_oid"
        "('{%d }');\n",
        i_reldeltarelid);
    appendPQExpBuffer(upgrade_buffer,
        "SELECT binary_upgrade.set_next_cstore_delta_rfoid"
        "('{%d }');\n",
        i_reldeltarelidfs);

    appendPQExpBuffer(upgrade_buffer,
        "SELECT binary_upgrade.set_next_cstore_delta_typoid"
        "('{%d }');\n",
        i_delta_reltype);

    appendPQExpBuffer(upgrade_buffer,
        "SELECT binary_upgrade.set_next_cstore_delta_atypoid"
        "('{%d }');\n",
        i_delta_array_type);

    if (InvalidOid != i_reldeltaidx) {
        appendPQExpBuffer(upgrade_buffer,
            "SELECT binary_upgrade.set_next_cstore_delta_idx_oid"
            "('{%d }');\n",
            i_reldeltaidx);
        appendPQExpBuffer(upgrade_buffer,
            "SELECT binary_upgrade.set_next_cstore_delta_idx_rfoid"
            "('{%d }');\n",
            i_reldeltaidxfs);
    }

    appendPQExpBuffer(upgrade_buffer,
        "SELECT binary_upgrade.set_next_cstore_cudesc_oid"
        "('{%d }');\n",
        i_relcudescrelid);
    appendPQExpBuffer(upgrade_buffer,
        "SELECT binary_upgrade.set_next_cstore_cudesc_rfoid"
        "('{%d }');\n",
        i_relcudescrelidfs);

    appendPQExpBuffer(upgrade_buffer,
        "SELECT binary_upgrade.set_next_cstore_cudesc_typoid"
        "('{%d }');\n",
        i_cudesc_reltype);

    appendPQExpBuffer(upgrade_buffer,
        "SELECT binary_upgrade.set_next_cstore_cudesc_atypoid"
        "('{%d }');\n",
        i_cudesc_array_type);

    appendPQExpBuffer(upgrade_buffer,
        "SELECT binary_upgrade.set_next_cstore_cudesc_idx_oid"
        "('{%d }');\n",
        i_relcudescidx);
    appendPQExpBuffer(upgrade_buffer,
        "SELECT binary_upgrade.set_next_cstore_cudesc_idx_rfoid"
        "('{%d }');\n",
        i_relcudescidxfs);

    appendPQExpBuffer(upgrade_buffer,
        "SELECT binary_upgrade.set_next_cstore_cudesc_toast_oid"
        "('{%d }');\n",
        i_cudesc_toast_relid);
    appendPQExpBuffer(upgrade_buffer,
        "SELECT binary_upgrade.set_next_cstore_cudesc_toast_rfoid"
        "('{%d }');\n",
        i_cudesc_toast_relidfs);

    appendPQExpBuffer(upgrade_buffer,
        "SELECT binary_upgrade.set_next_cstore_cudesc_toast_typoid"
        "('{%d }');\n",
        i_cudesc_toast_reltype);
    appendPQExpBuffer(upgrade_buffer,
        "SELECT binary_upgrade.set_next_cstore_cudesc_toast_idx_oid"
        "('{%d }');\n",
        i_cudesc_toast_index);
    appendPQExpBuffer(upgrade_buffer,
        "SELECT binary_upgrade.set_next_cstore_cudesc_toast_idx_rfoid"
        "('{%d }');\n",
        i_cudesc_toast_indexfs);

    appendPQExpBuffer(upgrade_buffer,
        "SELECT binary_upgrade.set_next_cstore_delta_toast_oid"
        "('{%d }');\n",
        i_delta_toast_relid);
    appendPQExpBuffer(upgrade_buffer,
        "SELECT binary_upgrade.set_next_cstore_delta_toast_rfoid"
        "('{%d }');\n",
        i_delta_toast_relidfs);

    appendPQExpBuffer(upgrade_buffer,
        "SELECT binary_upgrade.set_next_cstore_delta_toast_typoid"
        "('{%d }');\n",
        i_delta_toast_reltype);
    appendPQExpBuffer(upgrade_buffer,
        "SELECT binary_upgrade.set_next_cstore_delta_toast_idx_oid"
        "('{%d }');\n",
        i_delta_toast_index);

    appendPQExpBuffer(upgrade_buffer,
        "SELECT binary_upgrade.set_next_cstore_delta_toast_idx_rfoid"
        "('{%d }');\n",
        i_delta_toast_indexfs);
    PQclear(upgrade_res);
    destroyPQExpBuffer(upgrade_query);
}

static void binary_upgrade_set_pg_partition_oids(
    Archive* fout, PQExpBuffer upgrade_buffer, TableInfo* tbinfo, IndxInfo* idxinfo, bool is_index)
{
    PQExpBuffer upgrade_query = createPQExpBuffer();
    PGresult* upgrade_res = NULL;
    int32 partkeynum = 0;
    Oid* partkeycols = NULL;
    Oid partition_rel_oid = 0;
    int32 i = 0;
    int32 ntups = 0;

    appendPQExpBuffer(upgrade_query,
        "SELECT partstrategy, oid, "
        "pg_catalog.array_length(partkey, 1) AS partkeynum, partkey "
        "FROM pg_partition WHERE parentid = '%u' AND parttype = '%c'",
        tbinfo->dobj.catId.oid,
        PART_OBJ_TYPE_PARTED_TABLE);

    upgrade_res = ExecuteSqlQueryForSingleRow(fout, upgrade_query->data);

    partition_rel_oid = atooid(PQgetvalue(upgrade_res, 0, PQfnumber(upgrade_res, "oid")));
    partkeynum = atooid(PQgetvalue(upgrade_res, 0, PQfnumber(upgrade_res, "partkeynum")));
    partkeycols = (Oid*)pg_malloc(partkeynum * sizeof(Oid));
    parseOidArray(PQgetvalue(upgrade_res, 0, PQfnumber(upgrade_res, "partkey")), partkeycols, partkeynum);
    int partStrategyIdx = PQfnumber(upgrade_res, "partstrategy");
    char partStrategy = *PQgetvalue(upgrade_res, 0, partStrategyIdx);
    PQclear(upgrade_res);
    char newStrategy = partStrategy;
    if (partStrategy != PART_STRATEGY_LIST &&
        partStrategy != PART_STRATEGY_HASH) {
        newStrategy = PART_STRATEGY_RANGE;
    }

    resetPQExpBuffer(upgrade_query);

    if (!is_index) {
        int32 i_partitionoid;
        int32 i_partreltoastid;
        int32 i_partreltoasttype;
        int32 i_partreltoastindex;
        int32 i_partitionoidfs;
        int32 i_partreltoastidfs;
        int32 i_partreltoastindexfs;

        /* Updated master table, no matter the class parttype is 'v' or 'p' */
        appendPQExpBuffer(upgrade_buffer,
            "SELECT binary_upgrade.set_next_partrel_pg_partition_oid"
            "('%u'::pg_catalog.oid);\n",
            partition_rel_oid);

        /* The current version only supports HDFS value partition table,
         * and the partition table without oid number. So we do not need to
         * update its partition. partstrategy must be 'r'.
         */
        appendPQExpBuffer(upgrade_query,
            "SELECT p.oid AS partitionoid, p.relfilenode AS partitionoidfs,"
            " COALESCE(p.reltoastrelid, 0::Oid) AS partreltoastid, "
            " COALESCE(c.relfilenode, 0::Oid) AS partreltoastidfs,"
            " COALESCE(c.reltype, 0::Oid) AS partreltoasttype,"
            " COALESCE(c.reltoastidxid, 0::Oid) AS partreltoastindex, "
            " COALESCE(ci.relfilenode, 0::Oid) as partreltoastindexfs "
            " FROM pg_partition p LEFT JOIN pg_class c ON p.reltoastrelid = c.oid "
            "  LEFT JOIN pg_class ci ON ci.oid = c.reltoastidxid "
            " WHERE p.parentid = '%u' AND p.parttype = '%c' "
            " AND p.partstrategy = '%c' ORDER BY ",
            tbinfo->dobj.catId.oid,
            PART_OBJ_TYPE_TABLE_PARTITION,
            newStrategy);

        for (i = 1; i <= partkeynum; i++) {
            if (i == partkeynum)
                appendPQExpBuffer(
                    upgrade_query, "p.boundaries[%d]::%s ASC NULLS LAST", i, tbinfo->atttypnames[partkeycols[i - 1] - 1]);
            else
                appendPQExpBuffer(
                    upgrade_query, "p.boundaries[%d]::%s NULLS LAST, ", i, tbinfo->atttypnames[partkeycols[i - 1] - 1]);
        }

        upgrade_res = ExecuteSqlQuery(fout, upgrade_query->data, PGRES_TUPLES_OK);

        ntups = PQntuples(upgrade_res);
        if (ntups == 0) {
            PQclear(upgrade_res);
            free(partkeycols);
            partkeycols = NULL;
            destroyPQExpBuffer(upgrade_query);
            return;
        }

        i_partitionoid = PQfnumber(upgrade_res, "partitionoid");
        i_partreltoastid = PQfnumber(upgrade_res, "partreltoastid");
        i_partreltoasttype = PQfnumber(upgrade_res, "partreltoasttype");
        i_partreltoastindex = PQfnumber(upgrade_res, "partreltoastindex");

        i_partitionoidfs = PQfnumber(upgrade_res, "partitionoidfs");
        i_partreltoastidfs = PQfnumber(upgrade_res, "partreltoastidfs");
        i_partreltoastindexfs = PQfnumber(upgrade_res, "partreltoastindexfs");

        ntups = PQntuples(upgrade_res);

        if (ntups == 0) {
            PQclear(upgrade_res);
            free(partkeycols);
            partkeycols = NULL;
            destroyPQExpBuffer(upgrade_query);
            return;
        }

        /*
         * Restructure the partition definition by checking the information in
         * pg_partition. Notes: the top boundary could be MAXVALUE, so we should
         * check before restructuring.
         */
        appendPQExpBuffer(upgrade_buffer, "SELECT binary_upgrade.set_next_part_pg_partition_oids('{");

        for (i = 0; i < ntups - 1; i++) {
            appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res, i, i_partitionoid));
        }

        appendPQExpBuffer(upgrade_buffer, "%s }');\n", PQgetvalue(upgrade_res, i, i_partitionoid));

        appendPQExpBuffer(upgrade_buffer, "SELECT binary_upgrade.set_next_part_pg_partition_rfoids('{");

        for (i = 0; i < ntups - 1; i++) {
            appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res, i, i_partitionoidfs));
        }

        appendPQExpBuffer(upgrade_buffer, "%s }');\n", PQgetvalue(upgrade_res, i, i_partitionoidfs));

        if (OidIsValid(atoi(PQgetvalue(upgrade_res, 0, i_partreltoastid)))) {
            appendPQExpBuffer(upgrade_buffer, "SELECT binary_upgrade.setnext_part_toast_pg_class_oids('{");

            for (i = 0; i < ntups - 1; i++) {
                appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res, i, i_partreltoastid));
            }

            appendPQExpBuffer(upgrade_buffer, "%s }');\n", PQgetvalue(upgrade_res, i, i_partreltoastid));

            appendPQExpBuffer(upgrade_buffer, "SELECT binary_upgrade.setnext_part_toast_pg_class_rfoids('{");

            for (i = 0; i < ntups - 1; i++) {
                appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res, i, i_partreltoastidfs));
            }

            appendPQExpBuffer(upgrade_buffer, "%s }');\n", PQgetvalue(upgrade_res, i, i_partreltoastidfs));

            appendPQExpBuffer(upgrade_buffer, "SELECT binary_upgrade.set_next_part_toast_pg_type_oids('{");

            for (i = 0; i < ntups - 1; i++) {
                appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res, i, i_partreltoasttype));
            }

            appendPQExpBuffer(upgrade_buffer, "%s }');\n", PQgetvalue(upgrade_res, i, i_partreltoasttype));

            appendPQExpBuffer(upgrade_buffer, "SELECT binary_upgrade.setnext_part_index_pg_class_oids('{");

            for (i = 0; i < ntups - 1; i++) {
                appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res, i, i_partreltoastindex));
            }

            appendPQExpBuffer(upgrade_buffer, "%s }');\n", PQgetvalue(upgrade_res, i, i_partreltoastindex));

            appendPQExpBuffer(upgrade_buffer, "SELECT binary_upgrade.setnext_part_index_pg_class_rfoids('{");

            for (i = 0; i < ntups - 1; i++) {
                appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res, i, i_partreltoastindexfs));
            }

            appendPQExpBuffer(upgrade_buffer, "%s }');\n", PQgetvalue(upgrade_res, i, i_partreltoastindexfs));
        }

        PQclear(upgrade_res);
    } else {
        int32 i_partrelindexid;
        int32 i_partrelindexidfs;

        appendPQExpBuffer(upgrade_query,
            "SELECT pp.oid AS partitionoid, pi.oid AS partrelindexid, pi.relfilenode AS partrelindexidfs  "
            "FROM pg_partition pp, pg_partition pi WHERE pp.oid = pi.indextblid "
            "AND pp.parentid = '%u' AND pp.parttype = '%c' "
            "AND pp.partstrategy = '%c' AND pi.parentid = '%u' "
            "AND pi.parttype = '%c' ORDER BY ",
            tbinfo->dobj.catId.oid,
            PART_OBJ_TYPE_TABLE_PARTITION,
            newStrategy,
            idxinfo->dobj.catId.oid,
            PART_OBJ_TYPE_INDEX_PARTITION);

        for (i = 1; i <= partkeynum; i++) {
            if (i == partkeynum)
                appendPQExpBuffer(
                    upgrade_query, "pp.boundaries[%d]::%s ASC", i, tbinfo->atttypnames[partkeycols[i - 1] - 1]);
            else
                appendPQExpBuffer(
                    upgrade_query, "pp.boundaries[%d]::%s, ", i, tbinfo->atttypnames[partkeycols[i - 1] - 1]);
        }

        upgrade_res = ExecuteSqlQuery(fout, upgrade_query->data, PGRES_TUPLES_OK);
        ntups = PQntuples(upgrade_res);
        if (ntups == 0) {
            PQclear(upgrade_res);
            free(partkeycols);
            partkeycols = NULL;
            destroyPQExpBuffer(upgrade_query);
            return;
        }

        ntups = PQntuples(upgrade_res);
        if (ntups == 0) {
            PQclear(upgrade_res);
            free(partkeycols);
            partkeycols = NULL;
            destroyPQExpBuffer(upgrade_query);
            return;
        }

        i_partrelindexid = PQfnumber(upgrade_res, "partrelindexid");
        i_partrelindexidfs = PQfnumber(upgrade_res, "partrelindexidfs");

        appendPQExpBuffer(upgrade_buffer, "SELECT binary_upgrade.setnext_part_index_pg_class_oids('{");

        for (i = 0; i < ntups - 1; i++) {
            appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res, i, i_partrelindexid));
        }

        appendPQExpBuffer(upgrade_buffer, "%s }');\n", PQgetvalue(upgrade_res, i, i_partrelindexid));

        appendPQExpBuffer(upgrade_buffer, "SELECT binary_upgrade.setnext_part_index_pg_class_rfoids('{");

        for (i = 0; i < ntups - 1; i++) {
            appendPQExpBuffer(upgrade_buffer, "%s, ", PQgetvalue(upgrade_res, i, i_partrelindexidfs));
        }

        appendPQExpBuffer(upgrade_buffer, "%s }');\n", PQgetvalue(upgrade_res, i, i_partrelindexidfs));

        PQclear(upgrade_res);
    }

    free(partkeycols);
    partkeycols = NULL;
    destroyPQExpBuffer(upgrade_query);
}

static void LoadTablespaces(Archive* fout)
{
    PQExpBuffer query = createPQExpBuffer();
    appendPQExpBuffer(query, "select oid, spcname from pg_tablespace");
    PGresult* res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    g_tablespaceNum = PQntuples(res);
    Assert(g_tablespaceNum > 0);
    g_tablespaces = (TablespaceInfo*)pg_malloc(g_tablespaceNum * sizeof(TablespaceInfo));
    for (int i = 0; i < g_tablespaceNum; ++i) {
        g_tablespaces[i].oid = atooid(PQgetvalue(res, i, 0));
        g_tablespaces[i].name = gs_strdup(PQgetvalue(res, i, 1));
    }
    destroyPQExpBuffer(query);
    PQclear(res);
}

static const char* GetTablespaceNameByOid(Archive* fout, Oid oid)
{
    if (g_tablespaceNum == -1) {
        LoadTablespaces(fout);
    }

    for (int i = 0; i < g_tablespaceNum; ++i) {
        if (g_tablespaces[i].oid == oid) {
            return g_tablespaces[i].name;
        }
    }
    exit_horribly(NULL, "tablespace %u not exist\n", oid);
}

static void OutputIntervalPartitionDef(Archive* fout, const PGresult* res, PQExpBuffer outputBuf)
{
    int intervalIdx = PQfnumber(res, "interval");
    appendPQExpBuffer(outputBuf, "INTERVAL('%s')", PQgetvalue(res, 0, intervalIdx));

    int interTblSpcNumIdx = PQfnumber(res, "inttblspcnum");
    if (PQgetisnull(res, 0, interTblSpcNumIdx)) {
        return;
    }

    int interTblSpcNum = atoi(PQgetvalue(res, 0, interTblSpcNumIdx));
    if (interTblSpcNum <= 0) {
        return;
    }
    appendPQExpBuffer(outputBuf, " STORE IN(");

    int interTblSpcIdx = PQfnumber(res, "intervaltablespace");
    Oid* interTblSpcs = (Oid*)pg_malloc((size_t)interTblSpcNum * sizeof(Oid));
    parseOidArray(PQgetvalue(res, 0, interTblSpcIdx), interTblSpcs, interTblSpcNum);
    for (int i = 0; i < interTblSpcNum; ++i) {
        if (i > 0) {
            appendPQExpBuffer(outputBuf, ", ");
        }
        appendPQExpBuffer(outputBuf, "%s", GetTablespaceNameByOid(fout, interTblSpcs[i]));
    }
    appendPQExpBuffer(outputBuf, ")");
    free(interTblSpcs);
}

static bool PartkeyexprIsNull(Archive* fout, TableInfo* tbinfo, bool isSubPart)
{
    PQExpBuffer partkeyexpr = createPQExpBuffer();
    PGresult* partkeyexpr_res = NULL;
    int i_partkeyexpr;
    bool partkeyexprIsNull = true;
    ArchiveHandle* AH = (ArchiveHandle*)fout;
    if (!is_column_exists(AH->connection, PartitionRelationId, "partkeyexpr")) {
        return true;
    }

    if (!isSubPart)
        appendPQExpBuffer(partkeyexpr,
            "select partkeyexpr from pg_partition where (parttype = 'r') and (parentid in (select oid from pg_class where relname = \'%s\' and "
            "relnamespace = %u));",tbinfo->dobj.name, tbinfo->dobj.nmspace->dobj.catId.oid);
    else
        appendPQExpBuffer(partkeyexpr,
            "select distinct partkeyexpr from pg_partition where (parttype = 'p') and (parentid in (select oid from pg_class where relname = \'%s\' and "
            "relnamespace = %u));",tbinfo->dobj.name, tbinfo->dobj.nmspace->dobj.catId.oid);
    partkeyexpr_res = ExecuteSqlQueryForSingleRow(fout, partkeyexpr->data);
    i_partkeyexpr = PQfnumber(partkeyexpr_res, "partkeyexpr");
    char* partkeyexpr_buf = PQgetvalue(partkeyexpr_res, 0, i_partkeyexpr);
    if (partkeyexpr_buf && strcmp(partkeyexpr_buf, "") != 0)
        partkeyexprIsNull = false;
    PQclear(partkeyexpr_res);
    destroyPQExpBuffer(partkeyexpr);
    return partkeyexprIsNull;
}

static void GetPartkeyexprSrc(PQExpBuffer result, Archive* fout, TableInfo* tbinfo, bool isSubPart)
{
    PQExpBuffer tabledef = createPQExpBuffer();
    PGresult* tabledef_res = NULL;
    int i_tabledef;
    appendPQExpBuffer(tabledef,"select pg_get_tabledef(\'%s\');",tbinfo->dobj.name);
    tabledef_res = ExecuteSqlQueryForSingleRow(fout, tabledef->data);
    i_tabledef = PQfnumber(tabledef_res, "pg_get_tabledef");
    char* tabledef_buf = PQgetvalue(tabledef_res, 0, i_tabledef);
    std::string tabledef_str(tabledef_buf);
    size_t start;
    if (isSubPart) {
        start = tabledef_str.find("SUBPARTITION BY");
        start+=16;
    } else {
        start = tabledef_str.find("PARTITION BY");
        start+=13;
    }
    destroyPQExpBuffer(tabledef);
    PQclear(tabledef_res);
    if (start != std::string::npos) {
        std::string tmp = tabledef_str.substr(start);
        size_t pos = tmp.find("(");
        size_t i = 0;
        size_t j = pos+1;
        size_t icount = 0;
        size_t jcount = 0;
        i = tmp.find(")");
        while ((j = tmp.find("(", j)) != std::string::npos && j < i) {
            j++;
            jcount++;
        }
        while (i != std::string::npos && icount < jcount) {
            i++;
            i = tmp.find(")", i);
            icount++;
        }
        std::string res = tmp.substr(pos+1,i-pos-1);
        appendPQExpBuffer(result, "%s", res.c_str());
    }
}

static void GenerateSubPartitionBy(PQExpBuffer result, Archive *fout, TableInfo *tbinfo)
{
    int i;
    int i_partkey;
    int i_partkeynum;
    char partStrategy;
    int partkeynum;
    Oid *partkeycols = NULL;
    PQExpBuffer subPartStrategyQ = createPQExpBuffer();
    PGresult *res = NULL;

    bool partkeyexprIsNull = PartkeyexprIsNull(fout, tbinfo, true);
    /* get subpartitioned table's partstrategy */
    appendPQExpBuffer(subPartStrategyQ,
        "SELECT partstrategy "
        "FROM pg_partition a "
        "INNER JOIN (SELECT oid FROM pg_partition b WHERE b.parentid = '%u' AND b.parttype = '%c') c "
        "ON a.parentid = c.oid",
        tbinfo->dobj.catId.oid, PART_OBJ_TYPE_TABLE_PARTITION);
    res = ExecuteSqlQuery(fout, subPartStrategyQ->data, PGRES_TUPLES_OK);
    Assert(PQntuples(res) > 0);
    partStrategy = *PQgetvalue(res, 0, 0);
    PQclear(res);

    /* get subpartitioned table's partkey */
    resetPQExpBuffer(subPartStrategyQ);
    appendPQExpBuffer(subPartStrategyQ,
        "SELECT pg_catalog.array_length(partkey, 1) AS partkeynum, partkey "
        "FROM pg_partition WHERE parentid = '%u' AND parttype = '%c'",
        tbinfo->dobj.catId.oid, PART_OBJ_TYPE_TABLE_PARTITION);
    res = ExecuteSqlQuery(fout, subPartStrategyQ->data, PGRES_TUPLES_OK);
    Assert(PQntuples(res) > 0);
    i_partkeynum = PQfnumber(res, "partkeynum");
    i_partkey = PQfnumber(res, "partkey");
    partkeynum = atoi(PQgetvalue(res, 0, i_partkeynum));
    partkeycols = (Oid *)pg_malloc((size_t)partkeynum * sizeof(Oid));
    parseOidArray(PQgetvalue(res, 0, i_partkey), partkeycols, partkeynum);

    switch (partStrategy) {
        case PART_STRATEGY_LIST:
            appendPQExpBuffer(result, "SUBPARTITION BY LIST (");
            break;
        case PART_STRATEGY_HASH:
            appendPQExpBuffer(result, "SUBPARTITION BY HASH (");
            break;
        case PART_STRATEGY_RANGE:
            appendPQExpBuffer(result, "SUBPARTITION BY RANGE (");
            break;
        default:
            exit_horribly(NULL, "unsupported subpartition type %c\n", partStrategy);
    }

    /* assign subpartitioning columns */
    if (partkeyexprIsNull) {
        for (i = 0; i < partkeynum; i++) {
            if (i > 0)
                appendPQExpBuffer(result, ", ");
            if (partkeycols[i] < 1) {
                exit_horribly(NULL, "array index should not be smaller than zero: %d\n", partkeycols[i] - 1);
            }
            appendPQExpBuffer(result, "%s", fmtId(tbinfo->attnames[partkeycols[i] - 1]));
        }
    } else {
        GetPartkeyexprSrc(result, fout, tbinfo, true);
    }
    appendPQExpBuffer(result, ")\n");

    PQclear(res);
    destroyPQExpBuffer(subPartStrategyQ);
    free(partkeycols);
}

static void GenerateSubPartitionDetail(PQExpBuffer result, Archive *fout, TableInfo *tbinfo, Oid partOid,
    int subpartkeynum, Oid *subpartkeycols)
{
    int i, j;
    int i_partname;
    int *i_partboundary = NULL;
    int i_boundaries;
    int i_boundStr;
    int i_reltblspc;
    int i_subpartStrategy;
    char subpartStrategy;
    int ntups;

    appendPQExpBuffer(result, "\n    (");

    PQExpBuffer subPartDetailQ = createPQExpBuffer();
    PGresult *res = NULL;

    appendPQExpBuffer(subPartDetailQ, "SELECT p.relname AS partname, p.partstrategy AS partstrategy, ");
    for (i = 1; i <= subpartkeynum; i++) {
        appendPQExpBuffer(subPartDetailQ, "p.boundaries[%d] AS partboundary_%d, ", i, i);
    }
    appendPQExpBuffer(subPartDetailQ,
        "pg_catalog.array_to_string(p.boundaries, ',') as bound, "
        "pg_catalog.array_to_string(p.boundaries, ''',''') as boundstr, "
        "t.spcname AS reltblspc "
        "FROM pg_partition p LEFT JOIN pg_tablespace t "
        "ON p.reltablespace = t.oid "
        "WHERE p.parentid = '%u' AND p.parttype = '%c' "
        "ORDER BY ",
        partOid, PART_OBJ_TYPE_TABLE_SUB_PARTITION);
    for (i = 1; i <= subpartkeynum; i++) {
        if (i == subpartkeynum) {
            appendPQExpBuffer(subPartDetailQ, "p.boundaries[%d]::%s ASC NULLS LAST", i,
                tbinfo->atttypnames[subpartkeycols[i - 1] - 1]);
        } else {
            appendPQExpBuffer(subPartDetailQ, "p.boundaries[%d]::%s NULLS LAST, ", i,
                tbinfo->atttypnames[subpartkeycols[i - 1] - 1]);
        }
    }
    res = ExecuteSqlQuery(fout, subPartDetailQ->data, PGRES_TUPLES_OK);

    i_partname = PQfnumber(res, "partName");
    i_partboundary = (int *)pg_malloc(subpartkeynum * sizeof(int));
    for (i = 1; i <= subpartkeynum; i++) {
        char checkRowName[32] = {0};
        int nRet = 0;
        nRet = snprintf_s(checkRowName, sizeof(checkRowName) / sizeof(char), sizeof(checkRowName) / sizeof(char) - 1,
            "partBoundary_%d", i);
        securec_check_ss_c(nRet, "\0", "\0");
        i_partboundary[i - 1] = PQfnumber(res, checkRowName);
    }
    i_boundaries = PQfnumber(res, "bound");
    i_boundStr = PQfnumber(res, "boundstr");
    i_reltblspc = PQfnumber(res, "reltblspc");
    i_subpartStrategy = PQfnumber(res, "partstrategy");
    ntups = PQntuples(res);

    /*
     * Restructure the subpartition definition by checking the information in
     * pg_partition. Notes: the top boundary could be MAXVALUE, so we should
     * check before restructuring.
     */
    for (i = 0; i < ntups; i++) {
        char *pname = gs_strdup(PQgetvalue(res, i, i_partname));
        subpartStrategy = *PQgetvalue(res, i, i_subpartStrategy);

        if (i > 0) {
            appendPQExpBuffer(result, ",");
        }
        appendPQExpBuffer(result, "\n        ");
        if (subpartStrategy == PART_STRATEGY_LIST) {
            appendPQExpBuffer(result, "SUBPARTITION %s VALUES (", fmtId(pname));
        } else if (subpartStrategy == PART_STRATEGY_HASH) {
            appendPQExpBuffer(result, "SUBPARTITION %s ", fmtId(pname));
        } else { /* PART_STRATEGY_RANGE */
            appendPQExpBuffer(result, "SUBPARTITION %s VALUES LESS THAN (", fmtId(pname));
        }
        if (subpartStrategy == PART_STRATEGY_RANGE) {
            for (j = 0; j < subpartkeynum; j++) {
                char *pvalue = NULL;
                if (!PQgetisnull(res, i, i_partboundary[j])) {
                    pvalue = gs_strdup(PQgetvalue(res, i, i_partboundary[j]));
                }

                if (j > 0) {
                    appendPQExpBuffer(result, ",");
                }

                if (pvalue == NULL) {
                    appendPQExpBuffer(result, "MAXVALUE");
                    continue;
                } else if (isTypeString(tbinfo, subpartkeycols[j])) {
                    appendPQExpBuffer(result, "'%s'", pvalue);
                } else {
                    appendPQExpBuffer(result, "%s", pvalue);
                }

                free(pvalue);
                pvalue = NULL;
            }
            appendPQExpBuffer(result, ")");
        } else if (subpartStrategy == PART_STRATEGY_LIST) {
            char *boundaryValue = NULL;
            if (!PQgetisnull(res, i, i_boundaries)) {
                boundaryValue = gs_strdup(PQgetvalue(res, i, i_boundaries));
            }
            if (boundaryValue == NULL || strlen(boundaryValue) == 0) {
                appendPQExpBuffer(result, "DEFAULT");
            } else if (isTypeString(tbinfo, subpartkeycols[0])) {
                char *boundStr = gs_strdup(PQgetvalue(res, i, i_boundStr));
                appendPQExpBuffer(result, "'%s'", boundStr);
                free(boundStr);
            } else {
                appendPQExpBuffer(result, "%s", boundaryValue);
            }
            if (boundaryValue != NULL) {
                free(boundaryValue);
                boundaryValue = NULL;
            }

            appendPQExpBuffer(result, ")");
        }
        /*
         * Append subpartition tablespace.
         * Skip it, if subpartition tablespace is the same as partition tablespace.
         */
        if (!PQgetisnull(res, i, i_reltblspc)) {
            char *parttblspc = gs_strdup(PQgetvalue(res, i, i_reltblspc));
            if (strcmp(parttblspc, tbinfo->reltablespace) != 0) {
                appendPQExpBuffer(result, " TABLESPACE %s", fmtId(parttblspc));
            }
            free(parttblspc);
            parttblspc = NULL;
        }
        /* If the tablespace is null, the table uses the default tablespace of the database or schema. */

        free(pname);
        pname = NULL;
    }

    appendPQExpBuffer(result, "\n    )");

    PQclear(res);
    destroyPQExpBuffer(subPartDetailQ);
    free(i_partboundary);
}

/*
 * createTablePartition
 * Write the declaration of partitioned table.
 * This returns a new buffer which must be freed by the caller.
 */
static PQExpBuffer createTablePartition(Archive* fout, TableInfo* tbinfo)
{
    PQExpBuffer result = createPQExpBuffer();
    PQExpBuffer defq = createPQExpBuffer();
    PQExpBuffer partitionq = createPQExpBuffer();
    PQExpBuffer query = createPQExpBuffer();
    PGresult* res = NULL;
    bool isHDFSFTbl = false;
    int i_partkey;
    int i_partname;
    int* i_partboundary = NULL;
    int i_reltblspc;
    int i_partkeynum;
    char* parttblspc = NULL;
    Oid* partkeycols = NULL;
    int partkeynum;
    int ntups;
    int i;
    int j;
    int cnt;

    bool partkeyexprIsNull = PartkeyexprIsNull(fout, tbinfo, false);
    /* get partitioned table info */
    appendPQExpBuffer(defq,
        "SELECT partstrategy, interval[1], "
        "pg_catalog.array_length(partkey, 1) AS partkeynum, partkey, "
        "pg_catalog.array_length(intervaltablespace, 1) AS inttblspcnum, intervaltablespace "
        "FROM pg_partition WHERE parentid = '%u' AND parttype = '%c'",
        tbinfo->dobj.catId.oid,
        PART_OBJ_TYPE_PARTED_TABLE);
    res = ExecuteSqlQueryForSingleRow(fout, defq->data);

    i_partkeynum = PQfnumber(res, "partkeynum");
    i_partkey = PQfnumber(res, "partkey");
    partkeynum = atoi(PQgetvalue(res, 0, i_partkeynum));
    partkeycols = (Oid*)pg_malloc((size_t)partkeynum * sizeof(Oid));
    parseOidArray(PQgetvalue(res, 0, i_partkey), partkeycols, partkeynum);
    int partStrategyIdx = PQfnumber(res, "partstrategy");
    char partStrategy = *PQgetvalue(res, 0, partStrategyIdx);
    if (partStrategy != PART_STRATEGY_INTERVAL) {
        PQclear(res);
    }
    char newStrategy = partStrategy;
    if (partStrategy != PART_STRATEGY_LIST &&
        partStrategy != PART_STRATEGY_HASH) {
        newStrategy = PART_STRATEGY_RANGE;
    }
    if (RELKIND_FOREIGN_TABLE == tbinfo->relkind || RELKIND_STREAM == tbinfo->relkind) {
        int i_fdwname = -1;
        char* fdwnamestr = NULL;
        char* tableName = changeTableName(tbinfo->dobj.name);
        resetPQExpBuffer(query);
        appendPQExpBuffer(query,
            "select fdw.fdwname from pg_foreign_data_wrapper fdw ,pg_foreign_server fs,"
            "pg_foreign_table ft ,pg_class "
            "where fdw.oid = fs.srvfdw and fs.oid = ft.ftserver and pg_class.oid = "
            "ft.ftrelid and pg_class.relname = \'%s\' and pg_class.relnamespace = %u;",
            tableName,
            tbinfo->dobj.nmspace->dobj.catId.oid);
        free(tableName);
        tableName = NULL;

        res = ExecuteSqlQueryForSingleRow(fout, query->data);
        i_fdwname = PQfnumber(res, "fdwname");
        fdwnamestr = gs_strdup(PQgetvalue(res, 0, i_fdwname));
        if (0 == strncasecmp(fdwnamestr, "hdfs_fdw", NAMEDATALEN) ||
            0 == strncasecmp(fdwnamestr, "dfs_fdw", NAMEDATALEN)) {
            isHDFSFTbl = true;
            appendPQExpBuffer(result, "PARTITION BY (");
        }

        free(fdwnamestr);
        fdwnamestr = NULL;
    } else if (tbinfo->parttype == PARTTYPE_VALUE_PARTITIONED_RELATION) {
        /* put value partition table */
        appendPQExpBuffer(result, "PARTITION BY VALUES (");
    } else if (partStrategy == PART_STRATEGY_LIST) {
        /* restructure list partitioned table definition */
        appendPQExpBuffer(result, "PARTITION BY LIST (");
    } else if (partStrategy == PART_STRATEGY_HASH) {
        /* restructure hash partitioned table definition */
        appendPQExpBuffer(result, "PARTITION BY HASH (");
    } else {
        /* restructure range or interval partitioned table definition */
        appendPQExpBuffer(result, "PARTITION BY RANGE (");
    }

    /* assign partitioning columns */
    if (partkeyexprIsNull) {
        for (i = 0; i < partkeynum; i++) {
            if (i > 0)
                appendPQExpBuffer(result, ", ");
            if (partkeycols[i] < 1) {
                exit_horribly(NULL, "array index should not be smaller than zero: %d\n", partkeycols[i] - 1);
            }
            appendPQExpBuffer(result, "%s", fmtId(tbinfo->attnames[partkeycols[i] - 1]));
        }
    } else {
        GetPartkeyexprSrc(result, fout, tbinfo, false);
    }

    if (tbinfo->parttype == PARTTYPE_SUBPARTITIONED_RELATION) {
        appendPQExpBuffer(result, ") ");
        GenerateSubPartitionBy(result, fout, tbinfo);
    } else {
        appendPQExpBuffer(result, ")\n");
    }

    if (partStrategy == PART_STRATEGY_INTERVAL) {
        OutputIntervalPartitionDef(fout, res, result);
        appendPQExpBuffer(result, "\n");
        PQclear(res);
    }

    /* generate partition details */
    if (isHDFSFTbl) {
        appendPQExpBuffer(result, "AUTOMAPPED");
    } else if (tbinfo->parttype == PARTTYPE_VALUE_PARTITIONED_RELATION) {
        res = NULL;
    } else {
        appendPQExpBuffer(result, "(");

        i_partboundary = (int*)pg_malloc(partkeynum * sizeof(int));
        if (partStrategy != PART_STRATEGY_LIST || partkeynum == 1) {
            /* get table partitions info */
            appendPQExpBuffer(partitionq,
                "SELECT p.oid as oid, "
                "p.relname AS partname, "
                "pg_catalog.array_length(partkey, 1) AS subpartkeynum, "
                "partkey AS subpartkey, ");

            for (i = 1; i <= partkeynum; i++)
                appendPQExpBuffer(partitionq, "p.boundaries[%d] AS partboundary_%d, ", i, i);
            appendPQExpBuffer(partitionq,
                "pg_catalog.array_to_string(p.boundaries, ',') as bound, "
                "pg_catalog.array_to_string(p.boundaries, ''',''') as boundstr, "
                "t.spcname AS reltblspc "
                "FROM pg_partition p LEFT JOIN pg_tablespace t "
                "ON p.reltablespace = t.oid "
                "WHERE p.parentid = '%u' AND p.parttype = '%c' "
                "AND p.partstrategy = '%c' ORDER BY ",
                tbinfo->dobj.catId.oid,
                PART_OBJ_TYPE_TABLE_PARTITION,
                newStrategy);
            for (i = 1; i <= partkeynum; i++) {
                if (partkeyexprIsNull) {
                    if (i == partkeynum)
                        appendPQExpBuffer(
                            partitionq, "p.boundaries[%d]::%s ASC NULLS LAST", i, tbinfo->atttypnames[partkeycols[i - 1] - 1]);
                    else
                        appendPQExpBuffer(
                            partitionq, "p.boundaries[%d]::%s NULLS LAST, ", i, tbinfo->atttypnames[partkeycols[i - 1] - 1]);
                } else {
                    if (i == partkeynum)
                        appendPQExpBuffer(partitionq, "p.boundaries[%d] ASC NULLS LAST", i);
                    else
                        appendPQExpBuffer(partitionq, "p.boundaries[%d] NULLS LAST, ", i);
                }
            }
        } else {
            appendPQExpBuffer(partitionq,
                "SELECT /*+ hashjoin(p t) */ p.oid AS oid, "
                "p.relname AS partname, "
                "pg_catalog.array_length(partkey, 1) AS subpartkeynum, "
                "partkey AS subpartkey, ");
            for (i = 1; i <= partkeynum; i++) {
                appendPQExpBuffer(partitionq, "NULL AS partboundary_%d, ", i);
            }
            appendPQExpBuffer(partitionq,
                "p.bound_def AS bound, "
                "p.bound_def AS boundstr, "
                "t.spcname AS reltblspc FROM ( "
                "SELECT oid, relname, reltablespace, partkey, "
                "pg_catalog.string_agg(bound,',' ORDER BY bound_id) AS bound_def FROM( "
                "SELECT oid, relname, reltablespace, partkey, bound_id, '('||"
                "pg_catalog.array_to_string(pg_catalog.array_agg(key_value ORDER BY key_id),',','NULL')||')' AS bound "
                "FROM ( SELECT oid, relname, reltablespace, partkey, bound_id, key_id, ");
            cnt = 0;
            for (i = 0; i < partkeynum; i++) {
                if (!isTypeString(tbinfo, partkeycols[i])) {
                    continue;
                }
                if (cnt > 0) {
                    appendPQExpBuffer(partitionq, ",");
                } else {
                    appendPQExpBuffer(partitionq, "CASE WHEN key_id in (");
                }
                appendPQExpBuffer(partitionq, "%d", i + 1);
                cnt++;
            }
            if (cnt > 0) {
                appendPQExpBuffer(partitionq, ") THEN pg_catalog.quote_literal(key_value) ELSE key_value END AS ");
            }
            appendPQExpBuffer(partitionq,
                "key_value FROM ( "
                "SELECT oid, relname, reltablespace, partkey, bound_id, "
                "pg_catalog.generate_subscripts(keys_array, 1) AS key_id, "
                "pg_catalog.unnest(keys_array)::text AS key_value FROM ( "
                "SELECT oid, relname, reltablespace, partkey, bound_id,key_bounds::cstring[] AS keys_array FROM ( "
                "SELECT oid, relname, reltablespace, partkey, pg_catalog.unnest(boundaries) AS key_bounds, "
                "pg_catalog.generate_subscripts(boundaries, 1) AS bound_id FROM pg_partition "
                "WHERE parentid = %u AND parttype = '%c' AND partstrategy = '%c')))) "
                "GROUP BY oid, relname, reltablespace, partkey, bound_id) "
                "GROUP BY oid, relname, reltablespace, partkey "
                "UNION ALL SELECT oid, relname, reltablespace, partkey, 'DEFAULT' AS bound_def FROM pg_partition "
                "WHERE parentid = %u AND parttype = '%c' AND partstrategy = '%c' AND boundaries[1] IS NULL) p "
                "LEFT JOIN pg_tablespace t ON p.reltablespace = t.oid "
                "ORDER BY p.bound_def ASC NULLS LAST",
                tbinfo->dobj.catId.oid, PART_OBJ_TYPE_TABLE_PARTITION, PART_STRATEGY_LIST,
                tbinfo->dobj.catId.oid, PART_OBJ_TYPE_TABLE_PARTITION, PART_STRATEGY_LIST);
        }

        res = ExecuteSqlQuery(fout, partitionq->data, PGRES_TUPLES_OK);

        i_partname = PQfnumber(res, "partName");
        for (i = 1; i <= partkeynum; i++) {
            char checkRowName[32] = {0};
            int nRet = 0;
            nRet = snprintf_s(checkRowName,
                sizeof(checkRowName) / sizeof(char),
                sizeof(checkRowName) / sizeof(char) - 1,
                "partBoundary_%d",
                i);
            securec_check_ss_c(nRet, "\0", "\0");
            i_partboundary[i - 1] = PQfnumber(res, checkRowName);
        }
        int iBoundaries = PQfnumber(res, "bound");
        int iBoundStr = PQfnumber(res, "boundstr");
        int iPartoid = PQfnumber(res, "oid");
        int iSubpartkeynum = PQfnumber(res, "subpartkeynum");
        int iSubpartkey = PQfnumber(res, "subpartkey");
        i_reltblspc = PQfnumber(res, "reltblspc");
        ntups = PQntuples(res);

        /*
         * Restructure the partition definition by checking the information in
         * pg_partition. Notes: the top boundary could be MAXVALUE, so we should
         * check before restructuring.
         */
        for (i = 0; i < ntups; i++) {
            char* pname = gs_strdup(PQgetvalue(res, i, i_partname));

            if (i > 0)
                appendPQExpBuffer(result, ",");
            appendPQExpBuffer(result, "\n    ");
            if (partStrategy == PART_STRATEGY_LIST) {
                appendPQExpBuffer(result, "PARTITION %s VALUES (", fmtId(pname));
            } else if (partStrategy == PART_STRATEGY_HASH) {
                appendPQExpBuffer(result, "PARTITION %s ", fmtId(pname));
            } else {
                appendPQExpBuffer(result, "PARTITION %s VALUES LESS THAN (", fmtId(pname));
            }
            if (partStrategy != PART_STRATEGY_HASH &&
                partStrategy != PART_STRATEGY_LIST) {
                for (j = 0; j < partkeynum; j++) {
                    char* pvalue = NULL;
                    if (!PQgetisnull(res, i, i_partboundary[j]))
                        pvalue = gs_strdup(PQgetvalue(res, i, i_partboundary[j]));

                    if (j > 0)
                        appendPQExpBuffer(result, ",");

                    if (pvalue == NULL) {
                        appendPQExpBuffer(result, "MAXVALUE");
                        continue;
                    } else if (isTypeString(tbinfo, partkeycols[j]))
                        appendPQExpBuffer(result, "'%s'", pvalue);
                    else
                        appendPQExpBuffer(result, "%s", pvalue);

                    free(pvalue);
                    pvalue = NULL;
                }
                appendPQExpBuffer(result, ")");
            } else if (partStrategy == PART_STRATEGY_LIST) {
                char* boundaryValue = NULL;
                if (!PQgetisnull(res, i, iBoundaries)) {
                    boundaryValue = gs_strdup(PQgetvalue(res, i, iBoundaries));
                }
                if (boundaryValue == NULL || strlen(boundaryValue) == 0) {
                    appendPQExpBuffer(result, "DEFAULT");
                } else if (partkeynum == 1 && isTypeString(tbinfo, partkeycols[0])) {
                    char *boundStr = gs_strdup(PQgetvalue(res, i, iBoundStr));
                    appendPQExpBuffer(result, "'%s'", boundStr);
                    free(boundStr);
                } else {
                    appendPQExpBuffer(result, "%s", boundaryValue);
                }
                if (boundaryValue != NULL) {
                    free(boundaryValue);
                    boundaryValue = NULL;
                }

                appendPQExpBuffer(result, ")");
            }
            /*
             * Append partition tablespace.
             * Skip it, if partition tablespace is the same as partitioned table.
             */
            if (!PQgetisnull(res, i, i_reltblspc)) {
                parttblspc = gs_strdup(PQgetvalue(res, i, i_reltblspc));
                if (strcmp(parttblspc, tbinfo->reltablespace) != 0)
                    appendPQExpBuffer(result, " TABLESPACE %s", fmtId(parttblspc));
                free(parttblspc);
                parttblspc = NULL;
            }
            /* If the tablespace is null, the table uses the default tablespace of the database or schema. */
            
            free(pname);
            pname = NULL;

            if (tbinfo->parttype == PARTTYPE_SUBPARTITIONED_RELATION) {
                Oid partid = atooid(PQgetvalue(res, i, iPartoid));
                int subpartkeynum = atoi(PQgetvalue(res, 0, iSubpartkeynum));
                Oid* subpartkeycols = (Oid*)pg_malloc((size_t)subpartkeynum * sizeof(Oid));
                parseOidArray(PQgetvalue(res, 0, iSubpartkey), subpartkeycols, subpartkeynum);
                GenerateSubPartitionDetail(result, fout, tbinfo, partid, subpartkeynum, subpartkeycols);
                free(subpartkeycols);
            }
        }

        appendPQExpBuffer(result, "\n)");

        if (tbinfo->relrowmovement &&
            partStrategy != PART_STRATEGY_LIST &&
            partStrategy != PART_STRATEGY_HASH) {
            appendPQExpBuffer(result, "\n%s", "ENABLE ROW MOVEMENT");
        }
    }
    PQclear(res);
    destroyPQExpBuffer(defq);
    destroyPQExpBuffer(partitionq);
    destroyPQExpBuffer(query);
    free(partkeycols);
    partkeycols = NULL;
    free(i_partboundary);
    i_partboundary = NULL;

    return result;
}
/*
 * changeTableName
 *     change the table name, if the name including "'"
 */
static char* changeTableName(const char* tableName)
{
    char* oldTableName = gs_strdup(tableName);
    size_t mallocLen = strlen(tableName) * 2 + 1;
    char* newTableName = (char*)pg_malloc(mallocLen * sizeof(char));
    errno_t errorno = 0;
    int i = 0, j = 0;
    int len = (int)strlen(oldTableName);

    errorno = memset_s(newTableName, mallocLen, '\0', mallocLen);
    securec_check_c(errorno, "\0", "\0");

    for (i = 0, j = 0; i < len && j < int(mallocLen - 1); i++, j++) {
        if ('\'' == oldTableName[i]) {
            newTableName[j] = oldTableName[i];
            j++;
            newTableName[j] = '\'';
        } else {
            newTableName[j] = oldTableName[i];
        }
    }
    newTableName[j] = '\0';

    free(oldTableName);
    oldTableName = NULL;
    return newTableName;
}
/*
 * dumpViewSchema
 *	 write the declaration (not data) of one user-defined view
 */
static void dumpViewSchema(
    Archive* fout, TableInfo* tbinfo, PQExpBuffer query, PQExpBuffer q, PQExpBuffer delq, PQExpBuffer labelq)
{
    char* viewdef = NULL;
    char* schemainfo = NULL;
    PGresult* defres = NULL;
    PGresult* schemares = NULL;

    /* Beginning in 7.3, viewname is not unique; rely on OID */
    appendPQExpBuffer(
        query, "SELECT pg_catalog.pg_get_viewdef('%u'::pg_catalog.oid) AS viewdef", tbinfo->dobj.catId.oid);
    defres = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    if (PQntuples(defres) != 1) {
        if (PQntuples(defres) < 1)
            exit_horribly(NULL, "query to obtain definition of view %s returned no data\n", fmtId(tbinfo->dobj.name));
        else
            exit_horribly(NULL,
                "query to obtain definition of view %s returned more than one definition\n",
                fmtId(tbinfo->dobj.name));
    }

    viewdef = PQgetvalue(defres, 0, 0);

    if (strlen(viewdef) == 0) {
        exit_horribly(NULL, "definition of view %s appears to be empty (length zero)\n", fmtId(tbinfo->dobj.name));
    }

    /* Fetch views'schema info */
    resetPQExpBuffer(query);
    appendPQExpBuffer(query,
        "SELECT pg_catalog.string_agg(attname, ',') as schema FROM("
        "SELECT pg_catalog.quote_ident(attname) as attname FROM pg_catalog.pg_attribute "
        "WHERE attrelid = '%u' AND attnum > 0 AND NOT attisdropped ORDER BY attnum)",
        tbinfo->dobj.catId.oid);
    schemares = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    if (PQntuples(schemares) < 1)
        exit_horribly(NULL, "query to obtain schema info of view %s returned no data\n", fmtId(tbinfo->dobj.name));
    else {
        schemainfo = PQgetvalue(schemares, 0, 0);
        if (strlen(schemainfo) == 0)
            exit_horribly(NULL, "schema info of view %s appears to be empty (length zero)\n", fmtId(tbinfo->dobj.name));
    }

    /*
     * DROP must be fully qualified in case same name appears in
     * pg_catalog
     */
    appendPQExpBuffer(delq, "DROP VIEW IF EXISTS %s.", fmtId(tbinfo->dobj.nmspace->dobj.name));
    appendPQExpBuffer(delq, "%s%s;\n", fmtId(tbinfo->dobj.name), if_cascade);

    if (binary_upgrade)
        binary_upgrade_set_pg_class_oids(fout, q, tbinfo->dobj.catId.oid, false, false);

    /*
     * The DROP statement is automatically backed up when the backup format is -c/-t/-d.
     * So when use include_depend_objs and -p, we should also add the function.
     */
    if (include_depend_objs && !outputClean && IsPlainFormat()) {
        appendPQExpBuffer(q, "DROP VIEW IF EXISTS %s.", fmtId(tbinfo->dobj.nmspace->dobj.name));
        appendPQExpBuffer(q, "%s CASCADE;\n", fmtId(tbinfo->dobj.name));
    }

    appendPQExpBuffer(q, "CREATE ");
    if (tbinfo->viewsecurity != NULL)
        appendPQExpBuffer(q, "\n  SQL SECURITY %s \n   ", tbinfo->viewsecurity);
        
    appendPQExpBuffer(q, "VIEW %s(%s)", fmtId(tbinfo->dobj.name), schemainfo);
    if ((tbinfo->reloptions != NULL) && strlen(tbinfo->reloptions) > 0)
        appendPQExpBuffer(q, " WITH (%s)", tbinfo->reloptions);
    appendPQExpBuffer(q, " AS\n    ");

    Assert(viewdef[strlen(viewdef) - 1] == ';');
    appendBinaryPQExpBuffer(q, viewdef, strlen(viewdef) - 1);

    if (tbinfo->checkoption != NULL)
        appendPQExpBuffer(q, "\n  WITH %s CHECK OPTION", tbinfo->checkoption);
    appendPQExpBuffer(q, ";\n");

    appendPQExpBuffer(labelq, "VIEW %s", fmtId(tbinfo->dobj.name));

    PQclear(defres);
    PQclear(schemares);
}

bool isColumnStoreTable(const TableInfo *tbinfo)
{
    if ((NULL != tbinfo->reloptions) && ((NULL != strstr(tbinfo->reloptions, "orientation=column")) ||
        (NULL != strstr(tbinfo->reloptions, "orientation=orc")))) {
        return true;
    } else {
        return false;
    }
}

bool isTsStoreTable(const TableInfo *tbinfo) 
{
    if ((tbinfo->reloptions != NULL) && (strstr(tbinfo->reloptions, "orientation=timeseries") != NULL)) {
        return true;
    } else {
        return false;
    }
}

inline bool isDB4AIschema(const NamespaceInfo *nspinfo)
{
    return (strcmp(nspinfo->dobj.name, "db4ai") == 0);
}

static void dumpExtensionLinkedGrammer(Archive *fout, const TableInfo *tbinfo, PQExpBuffer q)
{
    /* Add the grammar extension linked to PGXC depending on data got from pgxc_class */
    if (tbinfo->pgxclocatortype == 'E') {
        return;
    }
    /* @hdfs
     * The HDFS foreign table dose not support the syntax
     * "create foreign table ... DISTRIBUTE BY ROUNDROBIN OPTIONS ..."
     */
    if (tbinfo->relkind != RELKIND_FOREIGN_TABLE && tbinfo->relkind != RELKIND_STREAM) {
        /* N: DISTRIBUTE BY ROUNDROBIN */
        if (tbinfo->pgxclocatortype == 'N') {
            appendPQExpBuffer(q, "\nDISTRIBUTE BY ROUNDROBIN");
        }
        /* R: DISTRIBUTE BY REPLICATED */
        else if (tbinfo->pgxclocatortype == 'R') {
            appendPQExpBuffer(q, "\nDISTRIBUTE BY REPLICATION");
        }
        /* H: DISTRIBUTE BY HASH  */
        else if (tbinfo->pgxclocatortype == 'H') {
            DumpHashDistribution(q, tbinfo);
        }
        /* G/L: DISTRIBUTE BY RANGE/LIST */
        else if (IsLocatorDistributedBySlice(tbinfo->pgxclocatortype)) {
            PQExpBuffer result;
            if (tbinfo->pgxclocatortype == 'G') {
                result = DumpRangeDistribution(fout, tbinfo);
            } else if (tbinfo->pgxclocatortype == 'L') {
                result = DumpListDistribution(fout, tbinfo);
            }
            if (result != NULL) {
                appendPQExpBuffer(q, "\n%s", result->data);
                destroyPQExpBuffer(result);
            }
        } else if (tbinfo->pgxclocatortype == 'M') {
            appendPQExpBuffer(q, "\nDISTRIBUTE BY MODULO ");
            DumpDistKeys(q, tbinfo);
        }
    }
}

static void dumpAddPartialGrammer(Archive *fout, const TableInfo *tbinfo, PQExpBuffer q, bool isColumnStoreTbl)
{
    /*
     * If it is column store table and pg version >= 90200,
     * support "ALTER TABLE TABLE_NAME ADD PARTIAL CLUSTER KEY(COLUMNNAME2, COLUMNNAME2 ...);"
     */
    if (isColumnStoreTbl && (fout->remoteVersion >= 90200)) {
        char* cluster_constraintdef = NULL;
        int tuples = 0;
        int i = 0;
        PQExpBuffer query = createPQExpBuffer();
        PGresult *res = NULL;
        /* get constraint_cluster by the table oid */
        appendPQExpBuffer(query,
            "SELECT r.conname, pg_catalog.pg_get_constraintdef(r.oid, true) "
            "FROM pg_catalog.pg_constraint r "
            "WHERE r.conrelid = '%u' AND r.contype = 's' "
            "ORDER BY 1;",
            tbinfo->dobj.catId.oid);
        res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
        tuples = PQntuples(res);
        if (tuples > 0) {
            for (i = 0; i < tuples; i++) {
                cluster_constraintdef = PQgetvalue(res, i, 1);
                appendPQExpBuffer(q, "ALTER TABLE %s ", fmtId(tbinfo->dobj.name));
                appendPQExpBuffer(q, "ADD %s;\n", cluster_constraintdef);
            }
        }
        PQclear(res);
        destroyPQExpBuffer(query);
    }
}

static int GetDistributeKeyNum(Archive* fout, Oid tableoid)
{
    PQExpBuffer bufq = createPQExpBuffer();
    PGresult* res = NULL;
    int num;

    appendPQExpBuffer(bufq,
        "SELECT pg_catalog.ARRAY_LENGTH(p.BOUNDARIES, 1) AS distkeynum FROM PGXC_SLICE p "
        "WHERE p.relid = '%u' AND p.type = 's' LIMIT 1", 
        tableoid);

    res = ExecuteSqlQuery(fout, bufq->data, PGRES_TUPLES_OK);
    num = (int)strtol(PQgetvalue(res, 0, 0), NULL, 10);

    PQclear(res);
    destroyPQExpBuffer(bufq);

    return num;
}

static void GetBoundaryIndex(const PGresult* res, int* boundaryIndex, int distKeyNum)
{
    int nRet;
    /*
     * list/range distribution key number can't exceed 4,
     * so 32 bytes is enough for "distBoundary_X" string.
     */
    char checkRowName[MAX_KEY];
    for (int i = 1; i <= distKeyNum; i++) {
        nRet = snprintf_s(checkRowName, sizeof(checkRowName), sizeof(checkRowName) - 1, "distBoundary_%d", i);
        securec_check_ss_c(nRet, "\0", "\0");
        boundaryIndex[i - 1] = PQfnumber(res, checkRowName);
    }
}

static void GetDistributeAttrNum(const char* attnumStr, int* attnums, int distKeyNum)
{
    int i;
    char* token = NULL;
    char* saveptr = NULL;
    char* originStr = gs_strdup(attnumStr);

    i = 0;
    token = strtok_r(originStr, " ", &saveptr);
    while (token) {
        if (i >= distKeyNum) {
            break;
        }

        attnums[i] = atoi(token);
        token = strtok_r(NULL, " ", &saveptr);
        i++;
    }

    GS_FREE(originStr);
}

static void DumpSingleSliceBoundary(PQExpBuffer resultBuf, PGresult* res, const TableInfo* tbinfo, int tupleNum,
    int* boundaryIndex, int* keyAttrs, int distKeyNum, bool parentheses)
{
    char* pvalue = NULL;

    if (parentheses) {
        appendPQExpBuffer(resultBuf, "(");
    }

    for (int j = 0; j < distKeyNum; j++) {
        pvalue = NULL;

        if (!PQgetisnull(res, tupleNum, boundaryIndex[j])) {
            pvalue = PQgetvalue(res, tupleNum, boundaryIndex[j]);
        }

        if (j > 0) {
            appendPQExpBuffer(resultBuf, ", ");
        }

        if (pvalue == NULL) {
            appendPQExpBuffer(resultBuf, "MAXVALUE");
        } else {
            if (isTypeString(tbinfo, keyAttrs[j])) {
                appendPQExpBuffer(resultBuf, "'%s'", pvalue);
            } else {
                appendPQExpBuffer(resultBuf, "%s", pvalue);
            }
        }
    }

    if (parentheses) {
        appendPQExpBuffer(resultBuf, ")");
    }

    return;
}

static void DumpSliceDataNodeName(PQExpBuffer resultBuf, const char* nodeName)
{
    /* Optional: DATANODE dn_name */
    if (nodeName != NULL) {
        appendPQExpBuffer(resultBuf, " DATANODE ");
        appendPQExpBuffer(resultBuf, "%s", nodeName);
    }
}

static PGresult* QueryRangeSliceDefs(Archive* fout, const TableInfo* tbinfo, int distKeyNum)
{
    int i;
    Oid tableoid;
    PGresult* res = NULL;
    PQExpBuffer tmpBuf = createPQExpBuffer();

    tableoid = tbinfo->dobj.catId.oid;

    appendPQExpBuffer(tmpBuf, "select ");
    for (i = 1; i <= distKeyNum; i++) {
        appendPQExpBuffer(tmpBuf, "p.boundaries[%d] AS distBoundary_%d, ", i, i);
    }
    appendPQExpBuffer(tmpBuf,
        "p.relname as slice, case when p.specified = 't' then q.node_name else null end as nodename "
        "FROM pgxc_slice p JOIN pgxc_node q ON p.nodeoid = q.oid "
        "WHERE p.relid = '%u' AND p.type = 's' "
        "ORDER BY p.sliceorder",
        tableoid);

    res = ExecuteSqlQuery(fout, tmpBuf->data, PGRES_TUPLES_OK);

    destroyPQExpBuffer(tmpBuf);

    return res;
}

static void DumpDistKeys(PQExpBuffer resultBuf, const TableInfo* tbinfo)
{
    char* p = NULL;
    char delims[] = " ";
    char* saveptr = NULL;
    p = strtok_r(tbinfo->pgxcattnum, delims, &saveptr);
    int i = 0;
    int hashkey = 0;

    appendPQExpBuffer(resultBuf, "(");

    while (p != NULL) {
        hashkey = atoi(p);
        if (i == 0) {
            appendPQExpBuffer(resultBuf, "%s", fmtId(tbinfo->attnames[hashkey - 1]));
            i++;
        } else {
            appendPQExpBuffer(resultBuf, ", %s", fmtId(tbinfo->attnames[hashkey - 1]));
        }
        p = strtok_r(NULL, delims, &saveptr);
    }
    appendPQExpBuffer(resultBuf, ")");
}

static PQExpBuffer DumpRangeDistribution(Archive* fout, const TableInfo* tbinfo)
{
    int i;
    int ntups;
    int sliceFieldNum;
    int nodeNameFieldNum;
    int distKeyNum;
    Oid tableoid;
    int* keyAttrs = NULL;
    int* boundaryIndex = NULL;
    char* pvalue = NULL;
    PGresult* res = NULL;
    PQExpBuffer resultBuf = createPQExpBuffer();
    PQExpBuffer tmpBuf = createPQExpBuffer();

    tableoid = tbinfo->dobj.catId.oid;
    distKeyNum = GetDistributeKeyNum(fout, tableoid);
    keyAttrs = (int*)pg_malloc(distKeyNum * sizeof(int));
    GetDistributeAttrNum(tbinfo->pgxcattnum, keyAttrs, distKeyNum);

    appendPQExpBuffer(resultBuf, "\nDISTRIBUTE BY RANGE ");
    DumpDistKeys(resultBuf, tbinfo);
    appendPQExpBuffer(resultBuf, "\n");

    /* Get Range Slice Definition */
    res = QueryRangeSliceDefs(fout, tbinfo, distKeyNum);

    /* SLICE slice_name VALUES LESS THAN (literal [,..]) [DATANODE dn_name] */
    boundaryIndex = (int*)pg_malloc(distKeyNum * sizeof(int));
    GetBoundaryIndex(res, boundaryIndex, distKeyNum);
    sliceFieldNum = PQfnumber(res, "slice");
    nodeNameFieldNum = PQfnumber(res, "nodename");

    ntups = PQntuples(res);
    appendPQExpBuffer(resultBuf, "(\n    ");
    for (i = 0; i < ntups; i++) {
        pvalue = PQgetvalue(res, i, sliceFieldNum);
        appendPQExpBuffer(resultBuf, "SLICE %s VALUES LESS THAN (", pvalue);
        DumpSingleSliceBoundary(resultBuf, res, tbinfo, i, boundaryIndex, keyAttrs, distKeyNum, false);
        appendPQExpBuffer(resultBuf, ")");

        if (!PQgetisnull(res, i, nodeNameFieldNum)) {
            pvalue = PQgetvalue(res, i, nodeNameFieldNum);
            DumpSliceDataNodeName(resultBuf, pvalue);
        }

        if (i < ntups - 1) {
            appendPQExpBuffer(resultBuf, ",\n    ");
        }
    }
    appendPQExpBuffer(resultBuf, "\n)");

    PQclear(res);
    destroyPQExpBuffer(tmpBuf);
    GS_FREE(boundaryIndex);
    GS_FREE(keyAttrs);

    return resultBuf;
}

static PGresult* QueryListSliceDefs(Archive* fout, const TableInfo* tbinfo, int distKeyNum)
{
    int i;
    Oid tableoid;
    PGresult* res = NULL;
    PQExpBuffer tmpBuf = createPQExpBuffer();

    tableoid = tbinfo->dobj.catId.oid;
    
    /* Get List Slice Definition */
    appendPQExpBuffer(tmpBuf, "select ");
    for (i = 1; i <= distKeyNum; i++) {
        appendPQExpBuffer(tmpBuf, "p.boundaries[%d] AS distBoundary_%d, ", i, i);
    }
    appendPQExpBuffer(tmpBuf,
        "p.relname as slice, p.sindex as sindex, "
        "case when p.specified = 't' then q.node_name else null end as nodename "
        "FROM pgxc_slice p JOIN pgxc_node q on p.nodeoid = q.oid "
        "WHERE p.relid = '%u' AND p.type = 's' "
        "ORDER BY p.sliceorder, p.sindex",
        tableoid);
    res = ExecuteSqlQuery(fout, tmpBuf->data, PGRES_TUPLES_OK);

    destroyPQExpBuffer(tmpBuf);

    return res;
}

static int GetListSlicesindex(PGresult* res, int tupleIndex, int sindexFieldNum)
{
    int result = 0;
    char* pvalue = NULL;

    pvalue = PQgetvalue(res, tupleIndex, sindexFieldNum);
    if (pvalue != NULL) {
        result = atoi(pvalue);
    }

    return result;
}

static PQExpBuffer DumpListDistribution(Archive* fout, const TableInfo* tbinfo)
{
    int i;
    int ntups;
    int sindex;
    int sindexFieldNum;
    int nodeNameFieldNum;
    int sliceNameFieldNum;
    int distKeyNum;
    Oid tableoid;
    bool parentheses = false;
    char* pvalue = NULL;
    char* nodeName = NULL;
    int* boundaryIndex = NULL;
    int* keyAttrs = NULL;
    PGresult* res = NULL;
    PQExpBuffer resultBuf = createPQExpBuffer();
    PQExpBuffer tmpBuf = createPQExpBuffer();

    tableoid = tbinfo->dobj.catId.oid;
    distKeyNum = GetDistributeKeyNum(fout, tableoid);
    keyAttrs = (int*)pg_malloc(distKeyNum * sizeof(int));
    GetDistributeAttrNum(tbinfo->pgxcattnum, keyAttrs, distKeyNum);
    
    /* distribute columns */
    appendPQExpBuffer(resultBuf, "DISTRIBUTE BY LIST ");
    DumpDistKeys(resultBuf, tbinfo);
    appendPQExpBuffer(resultBuf, "\n");

    /* Get List Slice Definition */
    res = QueryListSliceDefs(fout, tbinfo, distKeyNum);

    /* SLICE slice_name VALUES (...) [DATANODE dn_name] */
    nodeNameFieldNum = PQfnumber(res, "nodename");
    sliceNameFieldNum = PQfnumber(res, "slice");
    sindexFieldNum = PQfnumber(res, "sindex");
    parentheses = (distKeyNum > 1);
    boundaryIndex = (int*)pg_malloc(distKeyNum * sizeof(int));
    GetBoundaryIndex(res, boundaryIndex, distKeyNum);

    ntups = PQntuples(res);
    appendPQExpBuffer(resultBuf, "(\n");
    for (i = 0; i < ntups; i++) {
        sindex = GetListSlicesindex(res, i, sindexFieldNum);
        if (sindex == 0) {
            if (i != 0) {
                appendPQExpBuffer(resultBuf, ")");
                DumpSliceDataNodeName(resultBuf, nodeName);
                appendPQExpBuffer(resultBuf, ",\n");
            }

            /* SLICE slice_name */
            pvalue = PQgetvalue(res, i, sliceNameFieldNum);
            appendPQExpBuffer(resultBuf, "    SLICE ");
            appendPQExpBuffer(resultBuf, "%s", pvalue);
            appendPQExpBuffer(resultBuf, " VALUES (");

            GS_FREE(nodeName);
            if (!PQgetisnull(res, i, nodeNameFieldNum)) {
                pvalue = PQgetvalue(res, i, nodeNameFieldNum);
                nodeName = gs_strdup(pvalue);
            }
        } else {
            appendPQExpBuffer(resultBuf, ", ");
        }

        /* slice boundary */
        if (PQgetisnull(res, i, 0)) {
            appendPQExpBuffer(resultBuf, "DEFAULT");
        } else {
            DumpSingleSliceBoundary(resultBuf, res, tbinfo, i, boundaryIndex, keyAttrs, distKeyNum, parentheses);
        }
    }

    appendPQExpBuffer(resultBuf, ")");
    DumpSliceDataNodeName(resultBuf, nodeName);
    appendPQExpBuffer(resultBuf, "\n)");

    PQclear(res);
    destroyPQExpBuffer(tmpBuf);
    GS_FREE(boundaryIndex);
    GS_FREE(keyAttrs);
    GS_FREE(nodeName);

    return resultBuf;
}

static void DumpHashDistribution(PQExpBuffer resultBuf, const TableInfo* tbinfo)
{
    char* p = NULL;
    char delims[] = " ";
    char* saveptr = NULL;
    p = strtok_r(tbinfo->pgxcattnum, delims, &saveptr);
    int hashkey = 0;
    hashkey = atoi(p);
    if (tbinfo->attkvtype[hashkey - 1] == 4) {
        appendPQExpBuffer(resultBuf, "%s", "DISTRIBUTE BY hidetag");
        return;
    }
    appendPQExpBuffer(resultBuf, "\nDISTRIBUTE BY HASH ");
    DumpDistKeys(resultBuf, tbinfo);
}

/*
 * DumpMatviewSchema
 * Write the declaration (not data) of one user-defined materialized view
 */
static void DumpMatviewSchema(
    Archive* fout, TableInfo* tbinfo, PQExpBuffer query, PQExpBuffer q, PQExpBuffer delq, PQExpBuffer labelq)
{
    char* matviewdef = NULL;
    char* schemainfo = NULL;
    PGresult* defres = NULL;
    PGresult* schemares = NULL;

    /* Matviewname is not unique, rely on OID */
    appendPQExpBuffer(
        query, "SELECT pg_catalog.pg_get_viewdef('%u'::pg_catalog.oid) AS matviewdef", tbinfo->dobj.catId.oid);
    defres = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    if (PQntuples(defres) != 1) {
        if (PQntuples(defres) < 1) {
            exit_horribly(NULL,
                "query to obtain definition of materialized view %s returned no data\n", fmtId(tbinfo->dobj.name));
        } else {
            exit_horribly(NULL,
                "query to obtain definition of materialized view %s returned more than one definition\n",
                fmtId(tbinfo->dobj.name));
        }
    }

    matviewdef = PQgetvalue(defres, 0, 0);

    if (strlen(matviewdef) == 0) {
        exit_horribly(NULL,
            "definition of materialized view %s appears to be empty (length zero)\n", fmtId(tbinfo->dobj.name));
    }

    /* Fetch matviews'schema info */
    resetPQExpBuffer(query);
    appendPQExpBuffer(query,
        "SELECT pg_catalog.string_agg(attname, ',') as schema FROM("
        "SELECT pg_catalog.quote_ident(attname) as attname FROM pg_catalog.pg_attribute "
        "WHERE attrelid = '%u' AND attnum > 0 AND NOT attisdropped ORDER BY attnum)",
        tbinfo->dobj.catId.oid);
    schemares = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    if (PQntuples(schemares) < 1) {
        exit_horribly(NULL,
            "query to obtain schema info of materialized view %s returned no data\n", fmtId(tbinfo->dobj.name));
    } else {
        schemainfo = PQgetvalue(schemares, 0, 0);
        if (strlen(schemainfo) == 0) {
            exit_horribly(NULL,
                "schema info of materialized view %s appears to be empty (length zero)\n", fmtId(tbinfo->dobj.name));
        }
    }

    /*
     * DROP must be fully qualified in case same name appears in
     * pg_catalog
     */
    appendPQExpBuffer(delq, "DROP MATERIALIZED VIEW IF EXISTS %s.", fmtId(tbinfo->dobj.nmspace->dobj.name));
    appendPQExpBuffer(delq, "%s;\n", fmtId(tbinfo->dobj.name));

    if (binary_upgrade) {
        binary_upgrade_set_pg_class_oids(fout, q, tbinfo->dobj.catId.oid, false, false);
    }

    /*
     * The DROP statement is automatically backed up when the backup format is -c/-t/-d.
     * So when use include_depend_objs and -p, we should also add the function.
     */
    if (include_depend_objs && !outputClean && IsPlainFormat()) {
        appendPQExpBuffer(q, "DROP MATERIALIZED VIEW IF EXISTS %s.", fmtId(tbinfo->dobj.nmspace->dobj.name));
        appendPQExpBuffer(q, "%s CASCADE;\n", fmtId(tbinfo->dobj.name));
    }

    if (tbinfo->isIncremental) {
        appendPQExpBuffer(q, "CREATE INCREMENTAL MATERIALIZED VIEW %s(%s)", fmtId(tbinfo->dobj.name), schemainfo);
    } else {
        appendPQExpBuffer(q, "CREATE MATERIALIZED VIEW %s(%s)", fmtId(tbinfo->dobj.name), schemainfo);
    }
    if (!tbinfo->isIncremental && (tbinfo->reloptions != NULL) && strlen(tbinfo->reloptions) > 0) {
        appendPQExpBuffer(q, " WITH (%s)", tbinfo->reloptions);
    }
    appendPQExpBuffer(q, " AS\n    %s\n", matviewdef);

    appendPQExpBuffer(labelq, "MATERIALIZED VIEW %s", fmtId(tbinfo->dobj.name));

    PQclear(defres);
    PQclear(schemares);
}

/*
 * dumpTableSchema
 *	  write the declaration (not data) of one user-defined table or view
 */
static void dumpTableSchema(Archive* fout, TableInfo* tbinfo)
{
    PQExpBuffer query = createPQExpBuffer();
    PQExpBuffer q = createPQExpBuffer();
    PQExpBuffer delq = createPQExpBuffer();
    PQExpBuffer labelq = createPQExpBuffer();
    PGresult* res = NULL;
    int numParents = 0;
    TableInfo** parents;
    int actual_atts = 0; /* number of attrs in this CREATE statement */
    const char* reltypename = NULL;
    char* storage = NULL;
    char* srvname = NULL;
    char* ftoptions = NULL;
    char** ft_frmt_clmn = NULL; /* formatter columns       */
    char* formatter = NULL;
    int cnt_ft_frmt_clmns = 0; /* no of formatter columns */
    bool ft_write_only = false;
    int j = 0;
    int k = 0;
    int i = 0;
    char* name = NULL;
    bool isHDFSFTbl = false; /*HDFS foreign table*/
    bool isHDFSTbl = false;  /*HDFS table*/
    bool isGDSFtbl = false;  /*GDS foreign table*/
    bool isGCtbl = false;    /* gc_fdw foreign table */
    bool isColumnStoreTbl = false;
    bool isChangeCreateSQL = false;
    bool enableHashbucket = false;	
    int firstInitdefInd = 0;
    int attrNums = 0;
    bool to_node_dumped_foreign_tbl = false;
    bool isAddWith = true;
    bool isBcompatibility = false;

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, tbinfo->dobj.nmspace->dobj.name);

    if (binary_upgrade) {
        binary_upgrade_set_type_oids_by_rel_oid(fout, q, tbinfo->dobj.catId.oid, false);
    }

    /* Is it a table or a view? */
    if (tbinfo->relkind == RELKIND_VIEW || tbinfo->relkind == RELKIND_CONTQUERY) {
        reltypename = "VIEW";
        dumpViewSchema(fout, tbinfo, query, q, delq, labelq);
    } else if (tbinfo->relkind == RELKIND_MATVIEW) {
        reltypename = "MATERIALIZED VIEW";
        DumpMatviewSchema(fout, tbinfo, query, q, delq, labelq);
    } else {
        if (tbinfo->relkind == RELKIND_FOREIGN_TABLE || tbinfo->relkind == RELKIND_STREAM) {
            int i_srvname;
            int i_ftoptions;
            int i_ftwriteonly;
            char* ft_write_only_str = NULL;

            reltypename = "FOREIGN TABLE";

            /* retrieve name of foreign server and generic options */
            appendPQExpBuffer(query,
                "SELECT fs.srvname, "
                "pg_catalog.array_to_string(ARRAY("
                "SELECT pg_catalog.quote_ident(option_name) || "
                "' ' || pg_catalog.quote_literal(option_value) "
                "FROM pg_catalog.pg_options_to_table(ftoptions) "
                "ORDER BY option_name"
                "), E',\n    ') AS ftoptions, ft.ftwriteonly ftwriteonly "
                "FROM pg_catalog.pg_foreign_table ft "
                "JOIN pg_catalog.pg_foreign_server fs "
                "ON (fs.oid = ft.ftserver) "
                "WHERE ft.ftrelid = '%u'",
                tbinfo->dobj.catId.oid);
            res = ExecuteSqlQueryForSingleRow(fout, query->data);
            i_srvname = PQfnumber(res, "srvname");
            i_ftoptions = PQfnumber(res, "ftoptions");
            i_ftwriteonly = PQfnumber(res, "ftwriteonly");
            srvname = gs_strdup(PQgetvalue(res, 0, i_srvname));
            ftoptions = gs_strdup(PQgetvalue(res, 0, i_ftoptions));
            ft_write_only_str = PQgetvalue(res, 0, i_ftwriteonly);
            if (('t' == ft_write_only_str[0]) || ('1' == ft_write_only_str[0]) ||
                (('o' == ft_write_only_str[0]) && ('n' == ft_write_only_str[1]))) {
                ft_write_only = true;
            }

            PQclear(res);

            if (binary_upgrade && ((ftoptions != NULL) && ftoptions[0]) && (NULL != strstr(ftoptions, "error_table"))) {
                binary_upgrade_set_error_table_oids(fout, q, tbinfo);
            }

            if (((ftoptions != NULL) && ftoptions[0]) && (NULL != strstr(ftoptions, "formatter"))) {
                int i_formatter = 0;
                char* temp_iter = NULL;
                int iterf = 0;
                resetPQExpBuffer(query);
                appendPQExpBuffer(query,
                    "WITH ft_options AS "
                    "(SELECT (pg_catalog.pg_options_to_table(ft.ftoptions)).option_name, "
                    "(pg_catalog.pg_options_to_table(ft.ftoptions)).option_value "
                    "FROM pg_catalog.pg_foreign_table ft "
                    "WHERE ft.ftrelid = %u) "
                    "SELECT option_value FROM ft_options WHERE option_name = 'formatter'",
                    tbinfo->dobj.catId.oid);
                res = ExecuteSqlQueryForSingleRow(fout, query->data);
                i_formatter = PQfnumber(res, "option_value");
                formatter = gs_strdup(PQgetvalue(res, 0, i_formatter));
                PQclear(res);
                temp_iter = formatter;
                cnt_ft_frmt_clmns = 1;
                while (*temp_iter != '\0') {
                    if ('.' == *temp_iter)
                        cnt_ft_frmt_clmns++;
                    temp_iter++;
                }

                ft_frmt_clmn = (char**)pg_malloc(cnt_ft_frmt_clmns * sizeof(char*));
                iterf = 0;
                temp_iter = formatter;
                while (*temp_iter != '\0') {
                    ft_frmt_clmn[iterf] = temp_iter;
                    while (*temp_iter && '.' != *temp_iter)
                        temp_iter++;
                    if ('.' == *temp_iter) {
                        iterf++;
                        *temp_iter = '\0';
                        temp_iter++;
                    }
                }
            }
        } else {
            reltypename = "TABLE";
            srvname = NULL;
            ftoptions = NULL;
        }

        isColumnStoreTbl = isColumnStoreTable(tbinfo);

        if (NULL != tbinfo->reloptions && NULL != strstr(tbinfo->reloptions, "orientation=orc")) {
            isHDFSTbl = true;
        }

        if (NULL != tbinfo->reloptions && ((NULL != strstr(tbinfo->reloptions, "hashbucket=on")) ||
            (NULL != strstr(tbinfo->reloptions, "hashbucket=yes")) ||
            (NULL != strstr(tbinfo->reloptions, "hashbucket=1")) ||
            (NULL != strstr(tbinfo->reloptions, "hashbucket=true")) ||
            (NULL != strstr(tbinfo->reloptions, "bucketcnt=")))) {
            enableHashbucket = true;
        }

        numParents = tbinfo->numParents;
        parents = tbinfo->parents;

        /*
         * DROP must be fully qualified in case same name appears in
         * pg_catalog
         */
        appendPQExpBuffer(delq, "DROP %s IF EXISTS %s.", reltypename, fmtId(tbinfo->dobj.nmspace->dobj.name));
        appendPQExpBuffer(delq, "%s%s;\n", fmtId(tbinfo->dobj.name), if_cascade);

        /*
         * The DROP statement is automatically backed up when the backup format is -c/-t/-d.
         * So when use include_depend_objs and -p, we should also add the function.
         */
        if (include_depend_objs && !outputClean && IsPlainFormat()) {
            appendPQExpBuffer(q, "DROP %s IF EXISTS %s.", reltypename, fmtId(tbinfo->dobj.nmspace->dobj.name));
            appendPQExpBuffer(q, "%s CASCADE;\n", fmtId(tbinfo->dobj.name));
        }

        appendPQExpBuffer(labelq, "%s %s", reltypename, fmtId(tbinfo->dobj.name));

        if (binary_upgrade) {
            binary_upgrade_set_pg_class_oids(fout, q, tbinfo->dobj.catId.oid, false, false);
            if (tbinfo->parttype == PARTTYPE_PARTITIONED_RELATION ||
                tbinfo->parttype == PARTTYPE_SUBPARTITIONED_RELATION ||
                tbinfo->parttype == PARTTYPE_VALUE_PARTITIONED_RELATION ||
                tbinfo->parttype == PARTTYPE_SUBPARTITIONED_RELATION) {
                binary_upgrade_set_pg_partition_oids(fout, q, tbinfo, NULL, false);
            }

            if (isColumnStoreTbl) {
                /* cstore doesn't support subpartition now */
                if (tbinfo->parttype == PARTTYPE_PARTITIONED_RELATION) {
                    bupgrade_set_pg_partition_cstore_oids(fout, q, tbinfo, NULL, false);
                } else {
                    bupgrade_set_cstore_oids(fout, q, tbinfo, NULL, false);
                }
            }
        }

        if (include_alter_table && fout->remoteVersion >= 90200) {
            PQExpBuffer resultq = createPQExpBuffer();
            char* tableName = changeTableName(tbinfo->dobj.name);
            appendPQExpBuffer(resultq,
                "SELECT attr.attinitdefval, attr.attnum FROM pg_attribute attr "
                "WHERE attr.attrelid = (SELECT oid FROM pg_class "
                "WHERE relname = \'%s\' and relnamespace = %u) ORDER BY attr.attnum DESC;",
                tableName,
                tbinfo->dobj.nmspace->dobj.catId.oid);
            GS_FREE(tableName);

            /*
             * We can makesure PQntuples(res) > 0
             */
            res = ExecuteSqlQuery(fout, resultq->data, PGRES_TUPLES_OK);
            for (i = 0; i < PQntuples(res); i++) {
                char* attInitDefVal = PQgetvalue(res, i, 0);
                int attnum = atoi(PQgetvalue(res, i, 1));

                if (strlen(attInitDefVal) == 0) {
                    isChangeCreateSQL = false;
                } else {
                    firstInitdefInd = attnum - 1;
                    isChangeCreateSQL = true;
                    break;
                }
            }
            destroyPQExpBuffer(resultq);
            PQclear(res);
        }
        /*
         * When tbinfo->relbucket is 1, we are dumpping from CN, or from DN which is doing expansion.
         * If we are dumpping from CN, just ignore it.
         * No applications will dump data from DN which is doing expansion.
         * So, if tbinfo->relbucket is 1, just skip dumpping pg_hashbucket.
         */
        if (enableHashbucket == true && fout->getHashbucketInfo == false && tbinfo->relbucket != (Oid)1)
        {
            PQExpBuffer getHashbucketQuery = createPQExpBuffer();

            if (tbinfo->relbucket == (Oid)-1)
            {
                write_stderr("Wrong relbucket OID for table (%d),\n",tbinfo->dobj.catId.oid);
                exit_nicely(1);
            }
            appendPQExpBuffer(getHashbucketQuery, "select bucketmapsize, bucketvector from pg_hashbucket "
                    "where oid = '%u'",
                    tbinfo->relbucket);
            res = ExecuteSqlQuery(fout, getHashbucketQuery->data, PGRES_TUPLES_OK);
            int bucketMap = PQfnumber(res, "bucketmapsize");
            int bucketId = PQfnumber(res, "bucketvector");
            int ntups = PQntuples(res);
            if (ntups == 1) {
                appendPQExpBuffer(q, "SELECT pg_catalog.set_hashbucket_info('");
                appendPQExpBuffer(q, "%d ", atoi(PQgetvalue(res, 0, bucketMap)));
                appendPQExpBuffer(q, "%s", PQgetvalue(res, 0, bucketId));
                appendPQExpBuffer(q, "');\n");
            } else {
                write_stderr("wrong relbucket OID (%d) in pg_hashbucket for table(%d)" , tbinfo->relbucket, tbinfo->dobj.catId.oid);
                exit_nicely(1);
            }
            PQclear(res);
            destroyPQExpBuffer(getHashbucketQuery);
            fout->getHashbucketInfo = true;
        }
        if (tbinfo->parttype == PARTTYPE_PARTITIONED_RELATION || tbinfo->parttype == PARTTYPE_SUBPARTITIONED_RELATION) {
            appendPQExpBuffer(q, "CREATE %s %s", reltypename, fmtId(tbinfo->dobj.name));
        } else {
            const char *tableType = nullptr;
            if (tbinfo->relpersistence == RELPERSISTENCE_UNLOGGED) {
                tableType = "UNLOGGED ";
            } else if (tbinfo->relpersistence == RELPERSISTENCE_GLOBAL_TEMP) {
                tableType = "GLOBAL TEMPORARY ";
            } else {
                tableType = "";
            }
            appendPQExpBuffer(q,
                "CREATE %s%s %s",
                tableType,
                reltypename,
                fmtId(tbinfo->dobj.name));
        }

        /*
         * Attach to type, if reloftype; except in case of a binary upgrade,
         * we dump the table normally and attach it to the type afterward.
         */
        if ((tbinfo->reloftype != NULL) && !binary_upgrade) {
            Oid typeOid = atooid(tbinfo->reloftype);
            appendPQExpBuffer(q, " OF %s", getFormattedTypeName(fout, typeOid, zeroAsNone));
        }
        /*
         * No matter row table or colunm table, We can make suere that attrNums >= 1.
         */
        if (include_alter_table && isChangeCreateSQL) {
            attrNums = firstInitdefInd;
        } else {
            attrNums = tbinfo->numatts;
        }

        /* Dump the attributes */
        actual_atts = 0;
        isBcompatibility = findDBCompatibility(fout, PQdb(GetConnection(fout)));
        for (j = 0; j < attrNums; j++) {
            /*
             * Normally, dump if it's locally defined in this table, and not
             * dropped.  But for binary upgrade, we'll dump all the columns,
             * and then fix up the dropped and nonlocal cases below.
             */
            if (shouldPrintColumn(tbinfo, j)) {
                /*
                 * Default value --- suppress if to be printed separately.
                 */
                bool has_default = (tbinfo->attrdefs[j] != NULL && !tbinfo->attrdefs[j]->separate);
                bool has_encrypted_column = (tbinfo->column_key_names[j] != NULL && tbinfo->encryption_type[j] != 0);
                /*
                 * Not Null constraint --- suppress if inherited, except in
                 * binary-upgrade case where that won't work.
                 */
                bool has_notnull = (tbinfo->notnull[j] && (!tbinfo->inhNotNull[j] || binary_upgrade));

                /* Skip column if fully defined by reloftype */
                if ((tbinfo->reloftype != NULL) && !has_default && !has_notnull && !binary_upgrade)
                    continue;

                /* Format properly if not first attr */
                if (actual_atts == 0)
                    appendPQExpBuffer(q, " (");
                else
                    appendPQExpBuffer(q, ",");
                appendPQExpBuffer(q, "\n    ");
                actual_atts++;

                /* Attribute name */
                appendPQExpBuffer(q, "%s ", fmtId(tbinfo->attnames[j]));

                if (tbinfo->attisdropped[j]) {
                    /*
                     * ALTER TABLE DROP COLUMN clears pg_attribute.atttypid,
                     * so we will not have gotten a valid type name; insert
                     * INTEGER as a stopgap.  We'll clean things up later.
                     */
                    appendPQExpBuffer(q, "INTEGER /* dummy */");
                    /* Skip all the rest, too */
                    continue;
                }

                /* Attribute type */
                if ((tbinfo->reloftype != NULL) && !binary_upgrade) {
                    appendPQExpBuffer(q, "WITH OPTIONS");
                } else if (fout->remoteVersion >= 70100) {
                    if (isBcompatibility && hasSpecificExtension(fout, "dolphin") && strcmp(tbinfo->atttypnames[j], "numeric") == 0) {
                        free(tbinfo->atttypnames[j]);
                        tbinfo->atttypnames[j] = gs_strdup("number");
                    }
                    appendPQExpBuffer(q, "%s", tbinfo->atttypnames[j]);
                    if (has_encrypted_column) {
                        char *encryption_type = NULL;
                        appendPQExpBuffer(q, " encrypted with (column_encryption_key = %s, ",
                            tbinfo->column_key_names[j]);
                        if (tbinfo->encryption_type[j] == 2) {
                            encryption_type = "DETERMINISTIC";
                        } else if (tbinfo->encryption_type[j] == 1) {
                            encryption_type = "RANDOMIZED";
                        }
                        appendPQExpBuffer(q, "encryption_type = %s)", encryption_type);
                    }
                } else {
                    /* If no format_type, fake it */
                    name = myFormatType(tbinfo->atttypnames[j], tbinfo->atttypmod[j]);
                    appendPQExpBuffer(q, "%s", name);
                    GS_FREE(name);
                }
                if (tbinfo->attkvtype[j] != 0) {
                    if (tbinfo->attkvtype[j] == 1) {
                        appendPQExpBuffer(q, " %s", "TSTag");
                    } else if (tbinfo->attkvtype[j] == 2) {
                        appendPQExpBuffer(q, " %s", "TSField");
                    } else if (tbinfo->attkvtype[j] == 3) {
                        appendPQExpBuffer(q, " %s", "TSTime");
                    }
                }             

                /* Add collation if not default for the type */
                if (OidIsValid(tbinfo->attcollation[j])) {
                    CollInfo* coll = NULL;

                    coll = findCollationByOid(tbinfo->attcollation[j]);
                    if (NULL != coll) {
                        /* always schema-qualify, don't try to be smart */
                        appendPQExpBuffer(q, " COLLATE %s.", fmtId(coll->dobj.nmspace->dobj.name));
                        appendPQExpBuffer(q, "%s", fmtId(coll->dobj.name));
                    }
                }

                if (NULL != formatter) {
                    int iter = 0;
                    for (iter = 0; iter < cnt_ft_frmt_clmns; iter++) {
                        if ((0 == strncmp(tbinfo->attnames[j], ft_frmt_clmn[iter], strlen(tbinfo->attnames[j]))) &&
                            ('(' == ft_frmt_clmn[iter][strlen(tbinfo->attnames[j])])) {
                            appendPQExpBuffer(q, " position%s", &ft_frmt_clmn[iter][strlen(tbinfo->attnames[j])]);
                        }
                    }
                }

                if (has_default) {
                    char *default_value = tbinfo->attrdefs[j]->adef_expr;
                    char *onUpdate_value = NULL;
                    ArchiveHandle* AH = (ArchiveHandle*)fout;
                    bool hasOnUpdateFeature = is_column_exists(AH->connection, AttrDefaultRelationId, "adbin_on_update");
                    if (hasOnUpdateFeature) {
                        onUpdate_value = tbinfo->attrdefs[j]->adupd_expr;
                    }
#ifdef HAVE_CE
                    if (is_clientlogic_datatype(tbinfo->typid[j])) {
                        size_t plainTextSize = 0;
                        unsigned char *plaintext = NULL;
                        int originalTypeId = tbinfo->atttypmod[j];
                        ProcessStatus process_status = ADD_QUOTATION_MARK;
                        ValuesProcessor::deprocess_value(((ArchiveHandle *)fout)->connection,
                            (unsigned char *)default_value, strlen(default_value), originalTypeId, 0, &plaintext,
                            plainTextSize, process_status);
                        default_value = (char *)plaintext;
                    }
#endif
                    if (tbinfo->attrdefs[j]->generatedCol == ATTRIBUTE_GENERATED_STORED)
                        appendPQExpBuffer(q, " GENERATED ALWAYS AS (%s) STORED",
                                          default_value);
                    else if (j + 1 == tbinfo->autoinc_attnum)
                        appendPQExpBuffer(q, " %s", default_value);
                    else if (pg_strcasecmp(default_value, "") != 0)
                        appendPQExpBuffer(q, " DEFAULT %s", default_value);

                    if (hasOnUpdateFeature) {
                        if (pg_strcasecmp(onUpdate_value, "") != 0) {
                            if (pg_strcasecmp(onUpdate_value, "pg_systimestamp()") == 0) {
                                appendPQExpBuffer(q, " ON UPDATE CURRENT_TIMESTAMP");
                            } else if (pg_strcasecmp(onUpdate_value, "('now'::text)::time with time zone") == 0) {
                                appendPQExpBuffer(q, " ON UPDATE CURRENT_TIME");
                            } else if (pg_strcasecmp(onUpdate_value, "text_date('now'::text)") == 0) {
                                appendPQExpBuffer(q, " ON UPDATE CURRENT_DATE");
                            } else if (pg_strcasecmp(onUpdate_value, "('now'::text)::time without time zone") == 0) {
                                appendPQExpBuffer(q, " ON UPDATE LOCALTIME");
                            } else if (pg_strcasecmp(onUpdate_value, "('now'::text)::timestamp without time zone") == 0) {
                                appendPQExpBuffer(q, " ON UPDATE LOCALTIMESTAMP");
                            } else {
                                char* size_start = NULL;
                                char* size_end = NULL;
                                int number_size = 0;
                                size_start = strstr(onUpdate_value, "timestamp(");
                                if (size_start != NULL) {
                                    size_start = size_start + 10;
                                    size_end = strstr(size_start, ")");
                                    number_size = size_end - size_start;
                                    char num[number_size + 1] = {0};
                                    errno_t rc = strncpy_s(num, sizeof(num), size_start, number_size);
                                    securec_check_c(rc, "\0", "\0");
                                    appendPQExpBuffer(q, " ON UPDATE CURRENT_TIMESTAMP(%s)", num);
                                } else {
                                    size_start = strstr(onUpdate_value, "time(");
                                    if (size_start != NULL) {
                                        size_start = size_start + 5;
                                        size_end = strstr(size_start, ")");
                                        number_size = size_end - size_start;
                                        char num[number_size + 1] = {0};
                                        errno_t rc = strncpy_s(num, sizeof(num), size_start, number_size);
                                        securec_check_c(rc, "\0", "\0");
                                        appendPQExpBuffer(q, " ON UPDATE CURRENT_TIME(%s)", num);
                                    }
                                }
                            }
                        }
                    }
#ifdef HAVE_CE
                    if (is_clientlogic_datatype(tbinfo->typid[j])) {
                        libpq_free(default_value);
                    }
#endif
                }
                if (has_notnull)
                    appendPQExpBuffer(q, " NOT NULL");
            }
        }

        /*
         * 1) do not use --include-alter-table
         * 2) the SQL command not need to be modified
         */
        if (!include_alter_table || !isChangeCreateSQL) {
            /*
             * Add non-inherited CHECK constraints, if any.
             */
            for (j = 0; j < tbinfo->ncheck; j++) {
                ConstraintInfo* constr = &(tbinfo->checkexprs[j]);

                if (constr->separate || !constr->conislocal)
                    continue;

                if (actual_atts == 0)
                    appendPQExpBuffer(q, " (\n    ");
                else
                    appendPQExpBuffer(q, ",\n    ");

                appendPQExpBuffer(q, "CONSTRAINT %s ", fmtId(constr->dobj.name));
                appendPQExpBuffer(q, "%s", constr->condef);

                actual_atts++;
            }
        }
        /* Dump constraint needed by auto_increment */
        if (tbinfo->autoincconstraint != 0) {
            ddr_Assert(actual_atts != 0);
            ConstraintInfo* coninfo = (ConstraintInfo*)findObjectByDumpId(tbinfo->autoincconstraint);
            IndxInfo* indxinfo = (IndxInfo*)findObjectByDumpId(coninfo->conindex);
            appendPQExpBuffer(q, ",\n    ");
            appendPQExpBuffer(q, "CONSTRAINT %s ", fmtId(coninfo->dobj.name));
            dumpUniquePrimaryDef(q, coninfo, indxinfo, isBcompatibility);
            actual_atts++;
        } else if (tbinfo->autoincindex != 0) {
            ddr_Assert(actual_atts != 0);
            IndxInfo* indxinfo = (IndxInfo*)findObjectByDumpId(tbinfo->autoincindex);
            uint32 posoffset;
            const char* endpos = NULL;
            char* usingpos = strstr(indxinfo->indexdef, " USING ");
            if (usingpos != NULL) {
                endpos = strstr(usingpos, " TABLESPACE ");
                if (endpos != NULL) {
                    posoffset = (uint32)(endpos - usingpos);
                } else {
                    posoffset = (uint32)strlen(usingpos);
                }
                /* get "USING ... (keypart, ...)" from indexdef */
                char indexpartdef[posoffset + 1] = {0};
                errno_t rc = strncpy_s(indexpartdef, posoffset + 1, usingpos, posoffset);
                securec_check_c(rc, "\0", "\0");
                appendPQExpBuffer(q, ",\n    INDEX %s%s", fmtId(indxinfo->dobj.name), indexpartdef);
                actual_atts++;
            }
        }

        if (actual_atts) {
            appendPQExpBuffer(q, "\n)");
        } else if (!((tbinfo->reloftype != NULL) && !binary_upgrade)) {
            /*
             * We must have a parenthesized attribute list, even though empty,
             * when not using the OF TYPE syntax.
             */
            appendPQExpBuffer(q, " (\n)");
        }

        if (numParents > 0 && !binary_upgrade) {
            appendPQExpBuffer(q, "\nINHERITS (");
            for (k = 0; k < numParents; k++) {
                TableInfo* parentRel = parents[k];

                if (k > 0)
                    appendPQExpBuffer(q, ", ");
                if (parentRel->dobj.nmspace != tbinfo->dobj.nmspace)
                    appendPQExpBuffer(q, "%s.", fmtId(parentRel->dobj.nmspace->dobj.name));
                appendPQExpBuffer(q, "%s", fmtId(parentRel->dobj.name));
            }
            appendPQExpBuffer(q, ")");
        }

        if (tbinfo->autoinc_attnum > 0) {
            dumpTableAutoIncrement(fout, q, tbinfo);
        }

        if (tbinfo->relkind == RELKIND_FOREIGN_TABLE || tbinfo->relkind == RELKIND_STREAM)
            appendPQExpBuffer(q, "\nSERVER %s", fmtId(srvname));

        isAddWith = !exclude_with && (((tbinfo->reloptions != NULL) && strlen(tbinfo->reloptions) > 0) ||
            ((tbinfo->toast_reloptions != NULL) && strlen(tbinfo->toast_reloptions) > 0));
        if (isAddWith) {
            bool addcomma = false;

            appendPQExpBuffer(q, "\nWITH (");
            if ((tbinfo->reloptions != NULL) && strlen(tbinfo->reloptions) > 0) {
                addcomma = true;
                appendPQExpBuffer(q, "%s", tbinfo->reloptions);
            }
            if ((tbinfo->toast_reloptions != NULL) && strlen(tbinfo->toast_reloptions) > 0) {
                appendPQExpBuffer(q, "%s%s", addcomma ? ", " : "", tbinfo->toast_reloptions);
            }
            appendPQExpBuffer(q, ")");
        }

        if (REL_CMPRS_FIELDS_EXTRACT == tbinfo->relcmprs) {
            appendPQExpBuffer(q, "\nCOMPRESS");
        }

#ifdef PGXC
        /* Add the grammar extension linked to PGXC depending on data got from pgxc_class */
        dumpExtensionLinkedGrammer(fout, tbinfo, q);

        /*@hdfs
         * Judge whether the relation is a HDFS foreign table.
         */
        if (RELKIND_FOREIGN_TABLE == tbinfo->relkind || RELKIND_STREAM == tbinfo->relkind) {
            int i_fdwname = -1;
            char* fdwnamestr = NULL;
            char* tableName = changeTableName(srvname);

            resetPQExpBuffer(query);
            appendPQExpBuffer(query,
                "SELECT fdw.fdwname "
                "FROM pg_foreign_server fs, pg_foreign_data_wrapper fdw "
                "WHERE fs.srvname = \'%s\' AND fs.srvfdw = fdw.Oid;",
                tableName);
            GS_FREE(tableName);
            res = ExecuteSqlQueryForSingleRow(fout, query->data);
            i_fdwname = PQfnumber(res, "fdwname");
            fdwnamestr = gs_strdup(PQgetvalue(res, 0, i_fdwname));
            PQclear(res);

            if (strncasecmp(fdwnamestr, HDFS_FDW, NAMEDATALEN) == 0 ||
                strncasecmp(fdwnamestr, DFS_FDW, NAMEDATALEN) == 0 ||
                strncasecmp(fdwnamestr, "log_fdw", NAMEDATALEN) == 0) {
                isHDFSFTbl = true;
            } else if (strncasecmp(fdwnamestr, DIST_FDW, NAMEDATALEN) == 0) {
                isGDSFtbl = true;
            } else if (strncasecmp(fdwnamestr, GC_FDW, NAMEDATALEN) == 0) {
                isGCtbl = true;
            }
            /*
             * default is DIST_FDW
             */
            GS_FREE(fdwnamestr);
        }

        if (RELKIND_FOREIGN_TABLE != tbinfo->relkind && RELKIND_STREAM != tbinfo->relkind
            && tbinfo->pgxc_node_names != NULL &&
            tbinfo->pgxc_node_names[0] != '\0') {
            if (include_nodes) {
                if (isHDFSTbl) {
                    appendPQExpBuffer(q, "\nTO NODE (%s)", tbinfo->pgxc_node_names);
                } else {
                    /*Adapt multi-nodegroup, local table creation statement TO NODE modified to TO GROUP*/
                    int i_tb_node_group_names = 0;
                    char* tb_node_group_names = NULL;
                    resetPQExpBuffer(query);
                    appendPQExpBuffer(query,
                        "SELECT pgroup "
                        "FROM pg_catalog.pg_class, pg_catalog.pg_namespace, pg_catalog.pgxc_class, "
                        "pg_catalog.pgxc_group"
                        "  WHERE pgxc_class.pcrelid = pg_class.oid and "
                        "  pg_class.relnamespace = pg_namespace.oid and"
                        "  pgxc_class.pgroup = pgxc_group.group_name and "
                        "  pgxc_class.pcrelid = %u",
                        tbinfo->dobj.catId.oid);
                    res = ExecuteSqlQueryForSingleRow(fout, query->data);
                    i_tb_node_group_names = PQfnumber(res, "pgroup");
                    tb_node_group_names = PQgetvalue(res, 0, i_tb_node_group_names);
                    appendPQExpBuffer(q, "\nTO GROUP %s", fmtId(tb_node_group_names));
                    PQclear(res);
                }
            }
            to_node_dumped_foreign_tbl = true;
        }
#endif

        /* Dump partition if the table is partitioned.
         * A partitioned table is either range partitioned or interval
         * partitioned.(But for toast table, i have no idea about whether
         * it has partitioned porperties.
         */
        if (((tbinfo->parttype == PARTTYPE_PARTITIONED_RELATION ||
            tbinfo->parttype == PARTTYPE_SUBPARTITIONED_RELATION) &&
            tbinfo->relkind != RELKIND_FOREIGN_TABLE && tbinfo->relkind != RELKIND_STREAM) ||
            tbinfo->parttype == PARTTYPE_VALUE_PARTITIONED_RELATION) {
            PQExpBuffer result;
            result = createTablePartition(fout, tbinfo);
            if (result != NULL) {
                appendPQExpBuffer(q, "\n%s", result->data);
                destroyPQExpBuffer(result);
            }
        }

        /* Dump generic options if any */
        if (NULL != ftoptions && ftoptions[0]) {
            /*Special handling required*/
            if ((NULL != strstr(ftoptions, "error_table")) || (NULL != strstr(ftoptions, "formatter")) ||
                (NULL != strstr(ftoptions, "log_remote")) || (NULL != strstr(ftoptions, "reject_limit"))) {
                int i_error_table = 0;
                int i_log_remote = 0;
                int i_reject_limit = 0;
                int i_ftoptions = 0;

                char* error_table = NULL;
                char* log_remote = NULL;
                char* reject_limit = NULL;
                char* ftoptionsUpdated = NULL;

                resetPQExpBuffer(query);
                /* retrieve name of foreign server and generic options */
                appendPQExpBuffer(query,
                    "WITH ft_options AS "
                    "(SELECT (pg_catalog.pg_options_to_table(ft.ftoptions)).option_name, "
                    "(pg_catalog.pg_options_to_table(ft.ftoptions)).option_value "
                    "FROM pg_catalog.pg_foreign_table ft "
                    "WHERE ft.ftrelid = %u) "
                    "SELECT "
                    "( SELECT option_value FROM ft_options WHERE option_name = 'error_table' ) AS error_table,"
                    "( SELECT option_value FROM ft_options WHERE option_name = 'log_remote' ) AS log_remote,"
                    "( SELECT option_value FROM ft_options WHERE option_name = 'reject_limit' ) AS reject_limit,"
                    "pg_catalog.array_to_string(ARRAY( "
                    "SELECT pg_catalog.quote_ident(option_name) || ' ' || pg_catalog.quote_literal(option_value) "
                    "FROM ft_options WHERE option_name <> 'error_table' AND option_name <> 'log_remote' "
                    "AND option_name <> 'reject_limit' AND option_name <> 'formatter' ORDER BY option_name "
                    "), E',\n    ') AS ftoptions;",
                    tbinfo->dobj.catId.oid);
                res = ExecuteSqlQueryForSingleRow(fout, query->data);
                i_ftoptions = PQfnumber(res, "ftoptions");
                i_error_table = PQfnumber(res, "error_table");
                i_log_remote = PQfnumber(res, "log_remote");
                i_reject_limit = PQfnumber(res, "reject_limit");

                ftoptionsUpdated = gs_strdup(PQgetvalue(res, 0, i_ftoptions));

                if (!PQgetisnull(res, 0, i_error_table)) {
                    error_table = gs_strdup(PQgetvalue(res, 0, i_error_table));
                }

                if (!PQgetisnull(res, 0, i_log_remote)) {
                    log_remote = gs_strdup(PQgetvalue(res, 0, i_log_remote));
                }

                if (!PQgetisnull(res, 0, i_reject_limit)) {
                    reject_limit = gs_strdup(PQgetvalue(res, 0, i_reject_limit));
                }

                PQclear(res);

                appendPQExpBuffer(q, "\nOPTIONS (\n    %s\n)", ftoptionsUpdated);

                /* [ WRITE ONLY | READ ONLY ] */
                if (ft_write_only) {
                    appendPQExpBuffer(q, " WRITE ONLY");
                }

                /* [ WITH error_table_name | LOG INTO error_table_name | LOG REMOTE ]  */
                /* [ PER NODE REJECT LIMIT "'" m "'" ] */
                if (NULL != error_table) {
                    int i_errtbl_pgxc_node_names;
                    char* errtbl_pgxc_node_names = NULL;
                    char* tableName = changeTableName(error_table);

                    /* WITH error_table_name | LOG INTO error_table_name */
                    appendPQExpBuffer(q, " WITH %s", fmtId(error_table));

                    if (NULL != log_remote) {
                        appendPQExpBuffer(q, " REMOTE LOG '%s'", log_remote);
                    }

                    /* [ PER NODE REJECT LIMIT "'" m "'" ]  */
                    if (NULL != reject_limit) {
                        appendPQExpBuffer(q, " PER NODE REJECT LIMIT '%s'", reject_limit);
                    }
#ifdef PGXC
                    resetPQExpBuffer(query);
                    /* retrieve name of foreign server and generic options */
                    appendPQExpBuffer(query,
                        "SELECT pg_catalog.string_agg(node_name,',') AS pgxc_node_names "
                        "FROM pgxc_node n where n.oid in ("
                        "  SELECT pg_catalog.unnest(nodeoids) FROM pgxc_class v "
                        "  WHERE v.pcrelid=("
                        "     SELECT oid FROM pg_class "
                        "     WHERE relname=\'%s\' and relnamespace = %u))",
                        tableName,
                        tbinfo->dobj.nmspace->dobj.catId.oid);
                    GS_FREE(tableName);

                    res = ExecuteSqlQueryForSingleRow(fout, query->data);
                    i_errtbl_pgxc_node_names = PQfnumber(res, "pgxc_node_names");
                    errtbl_pgxc_node_names = PQgetvalue(res, 0, i_errtbl_pgxc_node_names);
                    if (include_nodes && !isGDSFtbl && errtbl_pgxc_node_names != NULL &&
                        errtbl_pgxc_node_names[0] != '\0') {
                        appendPQExpBuffer(q, "\nTO NODE (%s)", errtbl_pgxc_node_names);
                        to_node_dumped_foreign_tbl = true;
                    }
                    PQclear(res);
#endif
                } else {
                    if (NULL != log_remote) {
                        appendPQExpBuffer(q, " REMOTE LOG '%s'", log_remote);
                    }

                    /* [ PER NODE REJECT LIMIT "'" m "'" ]  */
                    if (NULL != reject_limit) {
                        appendPQExpBuffer(q, " PER NODE REJECT LIMIT '%s'", reject_limit);
                    }
                }

                GS_FREE(log_remote);
                GS_FREE(reject_limit);
                GS_FREE(error_table);
                GS_FREE(ftoptionsUpdated);
            } else {
                appendPQExpBuffer(q, "\nOPTIONS (\n    %s\n)", ftoptions);
                if (ft_write_only) {
                    appendPQExpBuffer(q, " WRITE ONLY");
                }
            }
        }

        /* @hdfs
         * The HDFS foreign table support the syntax
         * "CREATE FOREIGN TABLE ... OPTIONS ... DISTRIBUTE BY ROUNDROBIN"
         */
#ifdef PGXC
        if (RELKIND_FOREIGN_TABLE == tbinfo->relkind || RELKIND_STREAM == tbinfo->relkind) {
            /*
             * NOTICE: The distributeby clause is dumped when only a cordinator node
             * is oprerated by gs_dump. For datanode, it is not necessary to dump
             * distributeby clause.
             */
            if (isHDFSFTbl || isGCtbl) {
                switch (tbinfo->pgxclocatortype) {
                    case 'N':
                        appendPQExpBuffer(q, "\nDISTRIBUTE BY ROUNDROBIN");
                        break;
                    case 'R':
                        appendPQExpBuffer(q, "\nDISTRIBUTE BY REPLICATION");
                        break;
                    default:
                        write_msg(NULL,
                            "Failed to dump foreign table %s, because the distribution mode %c is not supported.\n",
                            fmtId(tbinfo->dobj.name),
                            tbinfo->pgxclocatortype);
                        break;
                }
            }

            if ((isHDFSFTbl || isGDSFtbl || isGCtbl) && include_nodes) {
                if (tbinfo->pgxc_node_names != NULL && tbinfo->pgxc_node_names[0] != '\0') {
                    appendPQExpBuffer(q, "\nTO NODE (%s)", tbinfo->pgxc_node_names);
                } else if ((to_node_dumped_foreign_tbl == false) && (connected_node_type == PGXC_NODE_COORDINATOR) &&
                           (NULL != all_data_nodename_list)) {
                    appendPQExpBuffer(q, "\nTO NODE (%s)", all_data_nodename_list);
                }
            }
            if (isHDFSFTbl && PARTTYPE_PARTITIONED_RELATION == tbinfo->parttype) {
                PQExpBuffer result = NULL;
                /* add partition defination */
                result = createTablePartition(fout, tbinfo);
                if (result != NULL) {
                    appendPQExpBuffer(q, "\n%s", result->data);
                    destroyPQExpBuffer(result);
                }
            }
        }
#endif

        appendPQExpBuffer(q, ";\n");

        if (include_alter_table && isChangeCreateSQL) {
            for (j = attrNums; j < tbinfo->numatts; j++) {
                if (shouldPrintColumn(tbinfo, j)) {
                    bool has_default = (tbinfo->attrdefs[j] != NULL && !tbinfo->attrdefs[j]->separate);
                    bool has_notnull = (tbinfo->notnull[j] && (!tbinfo->inhNotNull[j] || binary_upgrade));
                    bool has_encrypted_column = (tbinfo->column_key_names[j] != NULL && 
                        tbinfo->encryption_type[j] != 0);
                    bool has_nextval = false;

                    /* Skip column if fully defined by reloftype */
                    if ((tbinfo->reloftype != NULL) && !has_default && !has_notnull && !binary_upgrade)
                        continue;
                    /* split the create table SQL */
                    if (tbinfo->relkind == RELKIND_FOREIGN_TABLE) {
                        appendPQExpBuffer(q, "ALTER FOREIGN TABLE %s ADD COLUMN ", fmtId(tbinfo->dobj.name));
                    } else if (tbinfo->relkind == RELKIND_STREAM) {
                        appendPQExpBuffer(q, "ALTER STREAM %s ADD COLUMN ", fmtId(tbinfo->dobj.name));
                    } else {
                        appendPQExpBuffer(q, "ALTER TABLE %s ADD COLUMN ", fmtId(tbinfo->dobj.name));
                    }
                    /* Attribute name */
                    appendPQExpBuffer(q, "%s ", fmtId(tbinfo->attnames[j]));

                    if (tbinfo->attisdropped[j]) {
                        /*
                         * ALTER TABLE DROP COLUMN clears pg_attribute.atttypid,
                         * so we will not have gotten a valid type name; insert
                         * INTEGER as a stopgap.  We'll clean things up later.
                         */
                        appendPQExpBuffer(q, "INTEGER /* dummy */");
                        appendPQExpBuffer(q, ";\n");
                        /* Skip all the rest, too */
                        continue;
                    }

                    /* Attribute type */
                    if ((tbinfo->reloftype != NULL) && !binary_upgrade) {
                        appendPQExpBuffer(q, "WITH OPTIONS");
                    } else if (fout->remoteVersion >= 70100) {
                        appendPQExpBuffer(q, "%s", tbinfo->atttypnames[j]);
                        if (has_encrypted_column) {
                            char *encryption_type = NULL;
                            appendPQExpBuffer(q, " encrypted with (column_encryption_key = %s, ", 
                                tbinfo->column_key_names[j]);
                            if (tbinfo->encryption_type[j] == 2) {
                                encryption_type = "DETERMINISTIC";
                            } else if (tbinfo->encryption_type[j] == 1) {
                                encryption_type = "RANDOMIZED";
                            }
                            appendPQExpBuffer(q, "  encryption_type = %s)", encryption_type);
                        }

                    } else {
                        /* If no format_type, fake it */
                        name = myFormatType(tbinfo->atttypnames[j], tbinfo->atttypmod[j]);
                        appendPQExpBuffer(q, "%s", name);
                        GS_FREE(name);
                    }

                    /* Add collation if not default for the type */
                    if (OidIsValid(tbinfo->attcollation[j])) {
                        CollInfo* coll = NULL;
                        coll = findCollationByOid(tbinfo->attcollation[j]);
                        if (coll != NULL) {
                            /* always schema-qualify, don't try to be smart */
                            appendPQExpBuffer(q, " COLLATE %s.", fmtId(coll->dobj.nmspace->dobj.name));
                            appendPQExpBuffer(q, "%s", fmtId(coll->dobj.name));
                        }
                    }

                    if (formatter != NULL) {
                        int iter;
                        for (iter = 0; iter < cnt_ft_frmt_clmns; iter++) {
                            if ((0 == strncmp(tbinfo->attnames[j], ft_frmt_clmn[iter], strlen(tbinfo->attnames[j]))) &&
                                ('(' == ft_frmt_clmn[iter][strlen(tbinfo->attnames[j])])) {
                                appendPQExpBuffer(q, " position%s", &ft_frmt_clmn[iter][strlen(tbinfo->attnames[j])]);
                            }
                        }
                    }

                    if (has_default) {
                        if (strlen(tbinfo->attrdefs[j]->adef_expr) > strlen("nextval") &&
                            (strncmp(tbinfo->attrdefs[j]->adef_expr, "nextval", strlen("nextval")) == 0)) {
                            has_nextval = true;
                        } else {
                            appendPQExpBuffer(q, " DEFAULT %s", tbinfo->attrdefs[j]->adef_expr);
                        }
                    }
                    if (has_notnull)
                        appendPQExpBuffer(q, " NOT NULL");
                    appendPQExpBuffer(q, ";\n");

                    if (has_nextval) {
                        if (tbinfo->relkind == RELKIND_FOREIGN_TABLE) {
                            appendPQExpBuffer(q, "ALTER FOREIGN TABLE %s ALTER COLUMN ", fmtId(tbinfo->dobj.name));
                        } else if (tbinfo->relkind == RELKIND_STREAM) {
                            appendPQExpBuffer(q, "ALTER STREAM %s ALTER COLUMN ", fmtId(tbinfo->dobj.name));
                        } else {
                            appendPQExpBuffer(q, "ALTER TABLE %s ALTER COLUMN ", fmtId(tbinfo->dobj.name));
                        }
                        appendPQExpBuffer(
                            q, "%s SET DEFAULT %s", fmtId(tbinfo->attnames[j]), tbinfo->attrdefs[j]->adef_expr);
                        appendPQExpBuffer(q, ";\n");
                    }
                }
            }

            /*
             * Add non-inherited CHECK constraints, if any.
             */
            for (j = 0; j < tbinfo->ncheck; j++) {
                ConstraintInfo* constr = &(tbinfo->checkexprs[j]);

                if (constr->separate || !constr->conislocal)
                    continue;

                if (tbinfo->relkind == RELKIND_FOREIGN_TABLE) {
                    appendPQExpBuffer(q, "ALTER FOREIGN TABLE %s ", fmtId(tbinfo->dobj.name));
                } else if (tbinfo->relkind == RELKIND_STREAM) {
                    appendPQExpBuffer(q, "ALTER STREAM %s ", fmtId(tbinfo->dobj.name));
                } else {
                    appendPQExpBuffer(q, "ALTER TABLE %s ", fmtId(tbinfo->dobj.name));
                }
                appendPQExpBuffer(q, "ADD CONSTRAINT %s ", fmtId(constr->dobj.name));
                appendPQExpBuffer(q, "%s;\n", constr->condef);
            }
        }

        if (include_alter_table &&
            ((tbinfo->relkind == RELKIND_RELATION) || (tbinfo->relkind == RELKIND_FOREIGN_TABLE) 
            || (tbinfo->relkind == RELKIND_STREAM))) {
            for (j = 0; j < tbinfo->numatts; j++) {
                if (tbinfo->attisdropped[j]) {
                    appendPQExpBuffer(q, "\n-- Recreate dropped column information.\n");
                    appendPQExpBuffer(q,
                        "UPDATE pg_catalog.pg_attribute\n"
                        "SET attlen = %d, "
                        "attalign = '%c', attbyval = false\n"
                        "WHERE attname = ",
                        tbinfo->attlen[j],
                        tbinfo->attalign[j]);
                    appendStringLiteralAH(q, tbinfo->attnames[j], fout);
                    appendPQExpBuffer(q, "\n  AND attrelid = ");
                    appendStringLiteralAH(q, fmtId(tbinfo->dobj.name), fout);
                    appendPQExpBuffer(q, "::pg_catalog.regclass;\n");

                    if (tbinfo->relkind == RELKIND_RELATION) {
                        appendPQExpBuffer(q, "ALTER TABLE %s ", fmtId(tbinfo->dobj.name));
                    } else {
                        appendPQExpBuffer(q, "ALTER FOREIGN TABLE %s ", fmtId(tbinfo->dobj.name));
                    }

                    appendPQExpBuffer(q, "DROP COLUMN %s;\n", fmtId(tbinfo->attnames[j]));
                }
            }
        }

        /*
         * If it is column store table and pg version >= 90200,
         * support "ALTER TABLE TABLE_NAME ADD PARTIAL CLUSTER KEY(COLUMNNAME2, COLUMNNAME2 ...);"
         */
        dumpAddPartialGrammer(fout, tbinfo, q, isColumnStoreTbl);

        GS_FREE(ft_frmt_clmn);
        GS_FREE(formatter);
        /*
         * To create binary-compatible heap files, we have to ensure the same
         * physical column order, including dropped columns, as in the
         * original.  Therefore, we create dropped columns above and drop them
         * here, also updating their attlen/attalign values so that the
         * dropped column can be skipped properly.	(We do not bother with
         * restoring the original attbyval setting.)  Also, inheritance
         * relationships are set up by doing ALTER INHERIT rather than using
         * an INHERITS clause --- the latter would possibly mess up the column
         * order.  That also means we have to take care about setting
         * attislocal correctly, plus fix up any inherited CHECK constraints.
         * Analogously, we set up typed tables using ALTER TABLE / OF here.
         */
        if (binary_upgrade && 
            ((tbinfo->relkind == RELKIND_RELATION) || (tbinfo->relkind == RELKIND_FOREIGN_TABLE) ||
            (tbinfo->relkind == RELKIND_STREAM)) &&
            tbinfo->relpersistence != RELPERSISTENCE_GLOBAL_TEMP) {
            for (j = 0; j < tbinfo->numatts; j++) {
                if (tbinfo->attisdropped[j]) {
                    appendPQExpBuffer(q, "\n-- For binary upgrade, recreate dropped column.\n");
                    appendPQExpBuffer(q,
                        "UPDATE pg_catalog.pg_attribute\n"
                        "SET attlen = %d, "
                        "attalign = '%c', attbyval = false\n"
                        "WHERE attname = ",
                        tbinfo->attlen[j],
                        tbinfo->attalign[j]);
                    appendStringLiteralAH(q, tbinfo->attnames[j], fout);
                    appendPQExpBuffer(q, "\n  AND attrelid = ");
                    appendStringLiteralAH(q, fmtId(tbinfo->dobj.name), fout);
                    appendPQExpBuffer(q, "::pg_catalog.regclass;\n");

                    if (tbinfo->relkind == RELKIND_STREAM) {
                        appendPQExpBuffer(q, "ALTER %s %s ", "STREAM", fmtId(tbinfo->dobj.name));
                    } else {
                        appendPQExpBuffer(q,
                            "ALTER %s %s ",
                            (tbinfo->relkind == RELKIND_FOREIGN_TABLE) ? "FOREIGN TABLE" : "TABLE",
                            fmtId(tbinfo->dobj.name));
                    }
                    appendPQExpBuffer(q, "DROP COLUMN %s;\n", fmtId(tbinfo->attnames[j]));
                } else if (!(tbinfo->attislocal[j])) {
                    appendPQExpBuffer(q, "\n-- For binary upgrade, recreate inherited column.\n");
                    appendPQExpBuffer(q,
                        "UPDATE pg_catalog.pg_attribute\n"
                        "SET attislocal = false\n"
                        "WHERE attname = ");
                    appendStringLiteralAH(q, tbinfo->attnames[j], fout);
                    appendPQExpBuffer(q, "\n  AND attrelid = ");
                    appendStringLiteralAH(q, fmtId(tbinfo->dobj.name), fout);
                    appendPQExpBuffer(q, "::pg_catalog.regclass;\n");
                }
            }

            for (k = 0; k < tbinfo->ncheck; k++) {
                ConstraintInfo* constr = &(tbinfo->checkexprs[k]);

                if (constr->separate || constr->conislocal)
                    continue;

                appendPQExpBuffer(q, "\n-- For binary upgrade, set up inherited constraint.\n");
                if (tbinfo->relkind == RELKIND_STREAM) {
                    appendPQExpBuffer(q, "ALTER %s %s ", "STREAM", fmtId(tbinfo->dobj.name));
                } else {
                    appendPQExpBuffer(q,
                        "ALTER %s %s ",
                        (tbinfo->relkind == RELKIND_FOREIGN_TABLE) ? "FOREIGN TABLE" : "TABLE",
                        fmtId(tbinfo->dobj.name));
                }
                appendPQExpBuffer(q, " ADD CONSTRAINT %s ", fmtId(constr->dobj.name));
                appendPQExpBuffer(q, "%s;\n", constr->condef);
                appendPQExpBuffer(q,
                    "UPDATE pg_catalog.pg_constraint\n"
                    "SET conislocal = false\n"
                    "WHERE contype = 'c' AND conname = ");
                appendStringLiteralAH(q, constr->dobj.name, fout);
                appendPQExpBuffer(q, "\n  AND conrelid = ");
                appendStringLiteralAH(q, fmtId(tbinfo->dobj.name), fout);
                appendPQExpBuffer(q, "::pg_catalog.regclass;\n");
            }

            if (numParents > 0) {
                appendPQExpBuffer(q, "\n-- For binary upgrade, set up inheritance this way.\n");
                for (k = 0; k < numParents; k++) {
                    TableInfo* parentRel = parents[k];

                    appendPQExpBuffer(q, "ALTER TABLE %s INHERIT ", fmtId(tbinfo->dobj.name));
                    if (parentRel->dobj.nmspace != tbinfo->dobj.nmspace)
                        appendPQExpBuffer(q, "%s.", fmtId(parentRel->dobj.nmspace->dobj.name));
                    appendPQExpBuffer(q, "%s;\n", fmtId(parentRel->dobj.name));
                }
            }

            if (tbinfo->reloftype != NULL) {
                appendPQExpBuffer(q, "\n-- For binary upgrade, set up typed tables this way.\n");
                appendPQExpBuffer(q, "ALTER TABLE %s OF %s;\n", fmtId(tbinfo->dobj.name), tbinfo->reloftype);
            }

            ArchiveHandle* AH = (ArchiveHandle*)fout;
            bool isHasRelfrozenxid64 = is_column_exists(AH->connection, RelationRelationId, "relfrozenxid64");
            appendPQExpBuffer(q,
                "\n-- For binary upgrade, set heap's relfrozenxid %s\n",
                isHasRelfrozenxid64 ? ", relfrozenxid64" : "");

            if (isHasRelfrozenxid64)
                appendPQExpBuffer(q,
                    "UPDATE pg_catalog.pg_class\n"
                    "SET relfrozenxid = '%u'\n"
                    "SET relfrozenxid64 = '%lu'\n"
                    "WHERE oid = ",
                    tbinfo->frozenxid,
                    tbinfo->frozenxid64);
            else
                appendPQExpBuffer(q,
                    "UPDATE pg_catalog.pg_class\n"
                    "SET relfrozenxid = '%u'\n"
                    "WHERE oid = ",
                    tbinfo->frozenxid);
            appendStringLiteralAH(q, fmtId(tbinfo->dobj.name), fout);
            appendPQExpBuffer(q, "::pg_catalog.regclass;\n");

            if (tbinfo->toast_oid) {
                /* We preserve the toast oids, so we can use it during restore */
                appendPQExpBuffer(q,
                    "\n-- For binary upgrade, set toast's relfrozenxid, %s\n",
                    isHasRelfrozenxid64 ? "relfrozenxid64" : "");
                if (isHasRelfrozenxid64)
                    appendPQExpBuffer(q,
                        "UPDATE pg_catalog.pg_class\n"
                        "SET relfrozenxid = '%u'\n"
                        "SET relfrozenxid64 = '%lu'\n"
                        "WHERE oid = '%u';\n",
                        tbinfo->toast_frozenxid,
                        tbinfo->toast_frozenxid64,
                        tbinfo->toast_oid);
                else
                    appendPQExpBuffer(q,
                        "UPDATE pg_catalog.pg_class\n"
                        "SET relfrozenxid = '%u'\n"
                        "WHERE oid = '%u';\n",
                        tbinfo->toast_frozenxid,
                        tbinfo->toast_oid);
            }
        }

        /*
         * Dump additional per-column properties that we can't handle in the
         * main CREATE TABLE command.
         */
        for (j = 0; j < tbinfo->numatts; j++) {
            /* None of this applies to dropped columns */
            if (tbinfo->attisdropped[j])
                continue;

            /*
             * If we didn't dump the column definition explicitly above, and
             * it is NOT NULL and did not inherit that property from a parent,
             * we have to mark it separately.
             */
            if (!shouldPrintColumn(tbinfo, j) && (tbinfo->notnull[j]) && !(tbinfo->inhNotNull[j])) {
                if (tbinfo->relkind == RELKIND_STREAM) {
                    appendPQExpBuffer(q, "ALTER %s %s ", "STREAM", fmtId(tbinfo->dobj.name));
                } else {
                    appendPQExpBuffer(q,
                        "ALTER %s %s ",
                        (tbinfo->relkind == RELKIND_FOREIGN_TABLE) ? "FOREIGN TABLE" : "TABLE",
                        fmtId(tbinfo->dobj.name));
                }
                appendPQExpBuffer(q, "ALTER COLUMN %s SET NOT NULL;\n", fmtId(tbinfo->attnames[j]));
            }

            /*
             * Dump per-column statistics information. We only issue an ALTER
             * TABLE statement if the attstattarget entry for this column is
             * non-negative (i.e. it's not the default value)
             */
            if (tbinfo->attstattarget[j] >= 0) {
                if (tbinfo->relkind == RELKIND_STREAM) {
                    appendPQExpBuffer(q, "ALTER %s %s ", "STREAM", fmtId(tbinfo->dobj.name));
                } else {
                    appendPQExpBuffer(q,
                        "ALTER %s %s ",
                        (tbinfo->relkind == RELKIND_FOREIGN_TABLE) ? "FOREIGN TABLE" : "TABLE",
                        fmtId(tbinfo->dobj.name));
                }
                appendPQExpBuffer(q, "ALTER COLUMN %s ", fmtId(tbinfo->attnames[j]));
                appendPQExpBuffer(q, "SET STATISTICS %d;\n", tbinfo->attstattarget[j]);
            }

            if (tbinfo->attstattarget[j] < -1) {
                int percent_value;
                percent_value = (tbinfo->attstattarget[j] + 1) * (-1);
                if (tbinfo->relkind == RELKIND_STREAM) {
                    appendPQExpBuffer(q, "ALTER %s %s ", "STREAM", fmtId(tbinfo->dobj.name));
                } else {
                    appendPQExpBuffer(q,
                        "ALTER %s %s ",
                        (tbinfo->relkind == RELKIND_FOREIGN_TABLE) ? "FOREIGN TABLE" : "TABLE",
                        fmtId(tbinfo->dobj.name));
                }

                appendPQExpBuffer(q, "ALTER COLUMN %s ", fmtId(tbinfo->attnames[j]));
                appendPQExpBuffer(q, "SET STATISTICS PERCENT %d;\n", percent_value);
            }

            /*
             * Dump per-column storage information.  The statement is only
             * dumped if the storage has been changed from the type's default.
             */
            if (tbinfo->attstorage[j] != tbinfo->typstorage[j]) {
                switch (tbinfo->attstorage[j]) {
                    case 'p':
                        storage = gs_strdup("PLAIN");
                        break;
                    case 'e':
                        storage = gs_strdup("EXTERNAL");
                        break;
                    case 'm':
                        storage = gs_strdup("MAIN");
                        break;
                    case 'x':
                        storage = gs_strdup("EXTENDED");
                        break;
                    default:
                        storage = NULL;
                        break;
                }

                /*
                 * Only dump the statement if it's a storage type we recognize
                 */
                if (storage != NULL) {
                    appendPQExpBuffer(q, "ALTER TABLE %s ", fmtId(tbinfo->dobj.name));
                    appendPQExpBuffer(q, "ALTER COLUMN %s ", fmtId(tbinfo->attnames[j]));
                    appendPQExpBuffer(q, "SET STORAGE %s;\n", storage);
                    GS_FREE(storage);
                }
            }

            /*
             * Dump per-column attributes.
             */
            if ((tbinfo->attoptions[j] != NULL) && tbinfo->attoptions[j][0] != '\0') {
                if (tbinfo->relkind == RELKIND_STREAM) {
                    appendPQExpBuffer(q, "ALTER %s %s ", "STREAM", fmtId(tbinfo->dobj.name));
                } else {
                    appendPQExpBuffer(q,
                        "ALTER %s %s ",
                        (tbinfo->relkind == RELKIND_FOREIGN_TABLE) ? "FOREIGN TABLE" : "TABLE",
                        fmtId(tbinfo->dobj.name));
                }
                appendPQExpBuffer(q, "ALTER COLUMN %s ", fmtId(tbinfo->attnames[j]));
                appendPQExpBuffer(q, "SET (%s);\n", tbinfo->attoptions[j]);
            }

            /*
             * Dump per-column fdw options.
             */
            if (tbinfo->relkind == RELKIND_FOREIGN_TABLE && (tbinfo->attfdwoptions[j] != NULL) &&
                tbinfo->attfdwoptions[j][0] != '\0') {
                appendPQExpBuffer(q, "ALTER FOREIGN TABLE %s ", fmtId(tbinfo->dobj.name));
                appendPQExpBuffer(q, "ALTER COLUMN %s ", fmtId(tbinfo->attnames[j]));
                appendPQExpBuffer(q, "OPTIONS (\n    %s\n);\n", tbinfo->attfdwoptions[j]);
            }
            /*
             * Dump per-column fdw options for stream.
             */
            if (tbinfo->relkind == RELKIND_STREAM && (tbinfo->attfdwoptions[j] != NULL) &&
                tbinfo->attfdwoptions[j][0] != '\0') {
                appendPQExpBuffer(q, "ALTER STREAM %s ", fmtId(tbinfo->dobj.name));
                appendPQExpBuffer(q, "ALTER COLUMN %s ", fmtId(tbinfo->attnames[j]));
                appendPQExpBuffer(q, "OPTIONS (\n    %s\n);\n", tbinfo->attfdwoptions[j]);
            }
        }
    }
    /*
     * dump properties we only have ALTER TABLE syntax for
     */
    if ((tbinfo->relkind == RELKIND_RELATION) && tbinfo->relreplident != REPLICA_IDENTITY_DEFAULT) {
        if (tbinfo->relreplident == REPLICA_IDENTITY_INDEX) {
            /* nothing to do, will be set when the index is dumped */
        } else if (tbinfo->relreplident == REPLICA_IDENTITY_NOTHING) {
            appendPQExpBuffer(q, "\nALTER TABLE ONLY (%s) REPLICA IDENTITY NOTHING;\n", fmtId(tbinfo->dobj.name));
        } else if (tbinfo->relreplident == REPLICA_IDENTITY_FULL) {
            appendPQExpBuffer(q, "\nALTER TABLE ONLY (%s) REPLICA IDENTITY FULL;\n", fmtId(tbinfo->dobj.name));
        }
    }

    if (binary_upgrade) {
        binary_upgrade_extension_member(q, &tbinfo->dobj, labelq->data);
    }

    ArchiveEntry(fout,
        tbinfo->dobj.catId,
        tbinfo->dobj.dumpId,
        tbinfo->dobj.name,
        tbinfo->dobj.nmspace->dobj.name,
        (tbinfo->relkind == RELKIND_VIEW || tbinfo->relkind == RELKIND_MATVIEW ||
            tbinfo->relkind == RELKIND_CONTQUERY) ? NULL : tbinfo->reltablespace,
        tbinfo->rolname,
        (strcmp(reltypename, "TABLE") == 0) ? tbinfo->hasoids : false,
        reltypename,
        SECTION_PRE_DATA,
        q->data,
        delq->data,
        NULL,
        NULL,
        0,
        NULL,
        NULL);

    /* Dump Table Comments */
    dumpTableComment(fout, tbinfo, reltypename);

    /* Dump Table Security Labels */
    dumpTableSecLabel(fout, tbinfo, reltypename);

    /* Dump comments on inlined table constraints */
    for (j = 0; j < tbinfo->ncheck; j++) {
        ConstraintInfo* constr = &(tbinfo->checkexprs[j]);

        if (constr->separate || !constr->conislocal)
            continue;

        dumpTableConstraintComment(fout, constr);
    }

    destroyPQExpBuffer(query);
    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
    destroyPQExpBuffer(labelq);
    GS_FREE(srvname);
    GS_FREE(ftoptions);
}

/*
 * dumpAttrDef --- dump an attribute's default-value declaration
 */
static void dumpAttrDef(Archive* fout, AttrDefInfo* adinfo)
{
    TableInfo* tbinfo = adinfo->adtable;
    int adnum = adinfo->adnum;
    PQExpBuffer q;
    PQExpBuffer delq;

    /* Skip if table definition not to be dumped */
    if (!tbinfo->dobj.dump || dataOnly)
        return;

    /* Skip if not "separate"; it was dumped in the table's definition */
    if (!adinfo->separate)
        return;

    q = createPQExpBuffer();
    delq = createPQExpBuffer();

    appendPQExpBuffer(q, "ALTER TABLE %s ", fmtId(tbinfo->dobj.name));
    appendPQExpBuffer(q, "ALTER COLUMN %s SET DEFAULT %s;\n", fmtId(tbinfo->attnames[adnum - 1]), adinfo->adef_expr);

    /*
     * DROP must be fully qualified in case same name appears in pg_catalog
     */
    appendPQExpBuffer(delq, "ALTER TABLE %s%s.", if_exists, fmtId(tbinfo->dobj.nmspace->dobj.name));
    appendPQExpBuffer(delq, "%s ", fmtId(tbinfo->dobj.name));
    appendPQExpBuffer(delq, "ALTER COLUMN %s DROP DEFAULT;\n", fmtId(tbinfo->attnames[adnum - 1]));

    ArchiveEntry(fout,
        adinfo->dobj.catId,
        adinfo->dobj.dumpId,
        tbinfo->attnames[adnum - 1],
        tbinfo->dobj.nmspace->dobj.name,
        NULL,
        tbinfo->rolname,
        false,
        "DEFAULT",
        SECTION_PRE_DATA,
        q->data,
        delq->data,
        NULL,
        NULL,
        0,
        NULL,
        NULL);

    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
}

static void setTableInfoAttNumAndNames(Archive* fout, TableInfo* tblInfo)
{
    if (0 != tblInfo->numatts && NULL != tblInfo->attnames)
        return;
    if (fout->remoteVersion < 90200) {
        return;
    }

    PQExpBuffer resultq = createPQExpBuffer();
    errno_t errorno = 0;
    int i = 0;
    PGresult* res = NULL;
    char* tableName = changeTableName(tblInfo->dobj.name);

    /* get table attrnum */
    /* Make sure we are in proper schema */
    selectSourceSchema(fout, tblInfo->dobj.nmspace->dobj.name);
    resetPQExpBuffer(resultq);
    appendPQExpBuffer(resultq,
        "SELECT relnatts FROM pg_class "
        "WHERE relname = \'%s\' and relnamespace = %u;",
        tableName,
        tblInfo->dobj.nmspace->dobj.catId.oid);
    res = ExecuteSqlQueryForSingleRow(fout, resultq->data);
    tblInfo->numatts = atoi(PQgetvalue(res, 0, PQfnumber(res, "relnatts")));
    PQclear(res);

    resetPQExpBuffer(resultq);
    appendPQExpBuffer(resultq,
        "SELECT attr.attnum, attr.attname FROM pg_attribute attr "
        "WHERE attr.attrelid = (SELECT oid FROM pg_class "
        "WHERE relname = \'%s\' and relnamespace = %u) and attr.attnum > 0 "
        "ORDER BY attr.attnum ASC;",
        tableName,
        tblInfo->dobj.nmspace->dobj.catId.oid);
    res = ExecuteSqlQuery(fout, resultq->data, PGRES_TUPLES_OK);
    /* We can makesure PQntuples(res) > 0 */
    tblInfo->attnames = (char**)pg_malloc((size_t)tblInfo->numatts * sizeof(char*));
    errorno = memset_s(tblInfo->attnames, tblInfo->numatts, '\0', tblInfo->numatts);
    securec_check_c(errorno, "\0", "\0");
    for (i = 0; i < PQntuples(res); i++) {
        int attnum = atoi(PQgetvalue(res, i, 0));
        if (attnum < 1) {
            exit_horribly(NULL, "array index should not be smaller than zero: %d\n", attnum - 1);
        }
        tblInfo->attnames[attnum - 1] = gs_strdup(PQgetvalue(res, i, 1));
    }

    free(tableName);
    tableName = NULL;

    destroyPQExpBuffer(resultq);
    PQclear(res);
}

/*
 * getAttrName: extract the correct name for an attribute
 *
 * The array tblInfo->attnames[] only provides names of user attributes;
 * if a system attribute number is supplied, we have to fake it.
 * We also do a little bit of bounds checking for safety's sake.
 */
static const char* getAttrName(int attrnum, TableInfo* tblInfo)
{
    if (attrnum > 0 && attrnum <= tblInfo->numatts)
        return tblInfo->attnames[attrnum - 1];
    switch (attrnum) {
        case SelfItemPointerAttributeNumber:
            return "ctid";
        case ObjectIdAttributeNumber:
            return "oid";
        case MinTransactionIdAttributeNumber:
            return "xmin";
        case MinCommandIdAttributeNumber:
            return "cmin";
        case MaxTransactionIdAttributeNumber:
            return "xmax";
        case MaxCommandIdAttributeNumber:
            return "cmax";
        case TableOidAttributeNumber:
            return "tableoid";
#ifdef PGXC
        case XC_NodeIdAttributeNumber:
            return "xc_node_id";
        case BucketIdAttributeNumber:
            return "tablebucketid";
        case UidAttributeNumber:
            return "gs_tuple_uid";
#endif
#ifdef USE_SPQ
        case RootSelfItemPointerAttributeNumber:
            return "_root_ctid";
#endif
        default:
            break;
    }
    exit_horribly(NULL, "invalid column number %d for table \"%s\"\n", attrnum, tblInfo->dobj.name);
    return NULL; /* keep compiler quiet */
}

/*
 * dumpIndex
 *	  write out to fout a user-defined index
 */
static void dumpIndex(Archive* fout, IndxInfo* indxinfo)
{
    TableInfo* tbinfo = indxinfo->indextable;
    bool is_constraint = (indxinfo->indexconstraint != 0);
    PQExpBuffer q;
    PQExpBuffer delq;
    PQExpBuffer labelq;

    if (dataOnly)
        return;

    q = createPQExpBuffer();
    delq = createPQExpBuffer();
    labelq = createPQExpBuffer();

    appendPQExpBuffer(labelq, "INDEX %s", fmtId(indxinfo->dobj.name));

    /*
     * If there's an associated constraint, don't dump the index per se, but
     * do dump any comment for it.	(This is safe because dependency ordering
     * will have ensured the constraint is emitted first.)  Note that the
     * emitted comment has to be shown as depending on the constraint, not
     * the index, in such cases.
     */
    if (!is_constraint && (tbinfo->autoincconstraint > 0 || indxinfo->dobj.dumpId != tbinfo->autoincindex)) {
        if (binary_upgrade) {
            binary_upgrade_set_pg_class_oids(fout, q, indxinfo->dobj.catId.oid, true, false);

            if (tbinfo->parttype == PARTTYPE_PARTITIONED_RELATION ||
                tbinfo->parttype == PARTTYPE_SUBPARTITIONED_RELATION ||
                tbinfo->parttype == PARTTYPE_VALUE_PARTITIONED_RELATION) {
                binary_upgrade_set_pg_partition_oids(fout, q, tbinfo, indxinfo, true);
            }

            if ((NULL != tbinfo->reloptions) &&
                ((NULL != strstr(tbinfo->reloptions, "orientation=column")) ||
                    (NULL != strstr(tbinfo->reloptions, "orientation=orc"))) &&
                ((NULL != strstr(indxinfo->indexdef, "USING \"psort\"")) ||
                    (NULL != strstr(indxinfo->indexdef, "USING psort")))) {
                if (tbinfo->parttype == PARTTYPE_PARTITIONED_RELATION) {
                    bupgrade_set_pg_partition_cstore_oids(fout, q, tbinfo, indxinfo, true);
                } else {
                    bupgrade_set_cstore_oids(fout, q, tbinfo, indxinfo, true);
                }
            }
        }

        /* Plain secondary index */
        appendPQExpBuffer(q, "%s;\n", indxinfo->indexdef);

        /* If the index is clustered, we need to record that. */
        if (indxinfo->indisclustered) {
            appendPQExpBuffer(q, "\nALTER TABLE %s CLUSTER", fmtId(tbinfo->dobj.name));
            appendPQExpBuffer(q, " ON %s;\n", fmtId(indxinfo->dobj.name));
        }
        /* If the index is unusable, we need to record that. */
        if (!indxinfo->indisusable) {
            appendPQExpBuffer(q, "\nALTER INDEX %s UNUSABLE;\n", fmtId(indxinfo->dobj.name));
        }
        /* If the index is clustered, we need to record that. */
        if (indxinfo->indisreplident) {
            appendPQExpBuffer(q, "\nALTER TABLE ONLY (%s) REPLICA IDENTITY USING", fmtId(tbinfo->dobj.name));
            appendPQExpBuffer(q, " INDEX %s;\n", fmtId(indxinfo->dobj.name));
        }

        /*
         * DROP must be fully qualified in case same name appears in
         * pg_catalog
         */
        appendPQExpBuffer(delq, "DROP INDEX IF EXISTS %s.", fmtId(tbinfo->dobj.nmspace->dobj.name));
        appendPQExpBuffer(delq, "%s%s;\n", fmtId(indxinfo->dobj.name), if_cascade);

        ArchiveEntry(fout,
            indxinfo->dobj.catId,
            indxinfo->dobj.dumpId,
            indxinfo->dobj.name,
            tbinfo->dobj.nmspace->dobj.name,
            indxinfo->tablespace,
            tbinfo->rolname,
            false,
            "INDEX",
            SECTION_POST_DATA,
            q->data,
            delq->data,
            NULL,
            NULL,
            0,
            NULL,
            NULL);
    }

    /* Dump Index Comments */
    dumpComment(fout,
        labelq->data,
        tbinfo->dobj.nmspace->dobj.name,
        tbinfo->rolname,
        indxinfo->dobj.catId,
        0,
        is_constraint ? indxinfo->indexconstraint : indxinfo->dobj.dumpId);

    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
    destroyPQExpBuffer(labelq);
}

/*
 * @hdfs
 * Brief        : Dump the informational constraint info for foreign table.
 * Description  : Dump the informational constraint info for foreign table.
 * Input        : fout, a Archive struct.
 *                coninfo, a ConstraintInfo struct.
 * Output       : none.
 * Return Value : none.
 * Notes        : none.
 */
static void dumpConstraintForForeignTbl(Archive* fout, ConstraintInfo* coninfo)
{
    TableInfo* tbinfo = NULL;
    PQExpBuffer q;
    PQExpBuffer delq;

    ddr_Assert(NULL != fout && NULL != coninfo);

    tbinfo = coninfo->contable;
    /* Skip if not to be dumped */
    if (!coninfo->dobj.dump || dataOnly)
        return;

    if (tbinfo == NULL) {
        return;
    }

    q = createPQExpBuffer();
    delq = createPQExpBuffer();

    if (coninfo->contype == 'p' || coninfo->contype == 'u') {

        ConstraintInfo* constrinfo = NULL;

        constrinfo = (ConstraintInfo*)findObjectByDumpId(coninfo->dobj.dumpId);

        if (constrinfo == NULL)
            exit_horribly(NULL, "missing constraint \"%s\"\n", coninfo->dobj.name);

        /* foreign table constaint doesn't create any local index
           No need to preserve any information here incase of upgrade scenario.

        if (binary_upgrade)
            binary_upgrade_set_pg_class_oids(fout, q,
                                             constrinfo->dobj.catId.oid, true, false);
        */

        appendPQExpBuffer(q, "ALTER FOREIGN TABLE %s\n", fmtId(tbinfo->dobj.name));
        appendPQExpBuffer(q, "    ADD CONSTRAINT %s ", fmtId(coninfo->dobj.name));

        if (tbinfo->isMOT) {
            /* Index-related constraint */
            IndxInfo* indxinfo = (IndxInfo*)findObjectByDumpId(coninfo->conindex);
            if (indxinfo == NULL) {
                exit_horribly(NULL, "missing index for constraint \"%s\"\n", coninfo->dobj.name);
            }

            if (coninfo->condef != NULL) {
                /* pg_get_constraintdef should have provided everything */
                appendPQExpBuffer(q, "%s;\n", coninfo->condef);
            } else {
                appendPQExpBuffer(q, "%s (", (coninfo->contype == 'p') ? "PRIMARY KEY" : "UNIQUE");
                for (int k = 0; k < indxinfo->indnkeys; k++) {
                    int indkey = (int)indxinfo->indkeys[k];
                    const char* attname = NULL;

                    if (indkey == InvalidAttrNumber) {
                        break;
                    }
                    attname = getAttrName(indkey, tbinfo);
                    appendPQExpBuffer(q, "%s%s", (k == 0) ? "" : ", ", fmtId(attname));
                }

                appendPQExpBuffer(q, ")");

                if ((indxinfo->options != NULL) && strlen(indxinfo->options) > 0) {
                    appendPQExpBuffer(q, " WITH (%s)", indxinfo->options);
                }
                if (coninfo->condeferrable) {
                    appendPQExpBuffer(q, " DEFERRABLE");
                    if (coninfo->condeferred) {
                        appendPQExpBuffer(q, " INITIALLY DEFERRED");
                    }
                }

                appendPQExpBuffer(q, ";\n");
            }
        } else {
            appendPQExpBuffer(q, "%s (", (coninfo->contype == 'p') ? "PRIMARY KEY" : "UNIQUE");
            if (constrinfo->conkey > 0) {
                int key = (int)constrinfo->conkey;
                const char* attname = NULL;

                if (key == InvalidAttrNumber) {
                    destroyPQExpBuffer(q);
                    destroyPQExpBuffer(delq);
                    return;
                }
                setTableInfoAttNumAndNames(fout, tbinfo);
                attname = getAttrName(key, tbinfo);

                appendPQExpBuffer(q, "%s%s", "", fmtId(attname));
            }

            appendPQExpBuffer(q, ")");

            if (constrinfo->consoft) {
                appendPQExpBuffer(q, " NOT ENFORCED");
                if (constrinfo->conopt) {
                    appendPQExpBuffer(q, " ENABLE QUERY OPTIMIZATION");
                } else {
                    appendPQExpBuffer(q, " DISABLE QUERY OPTIMIZATION");
                }
            }

            appendPQExpBuffer(q, ";\n");
        }

        /*
         * DROP must be fully qualified in case same name appears in pg_catalog
         * In case of FDW MOT do not generate drop statements for CONSTRAINT
         */
        if (!tbinfo->isMOT) {
            appendPQExpBuffer(delq, "ALTER TABLE %s%s.", if_exists, fmtId(tbinfo->dobj.nmspace->dobj.name));
            appendPQExpBuffer(delq, "%s ", fmtId(tbinfo->dobj.name));
            appendPQExpBuffer(delq, "DROP CONSTRAINT %s%s%s;\n", if_exists, fmtId(coninfo->dobj.name), if_cascade);
        }
        ArchiveEntry(fout,
            coninfo->dobj.catId,
            coninfo->dobj.dumpId,
            coninfo->dobj.name,
            tbinfo->dobj.nmspace->dobj.name,
            NULL,  // shema
            tbinfo->rolname,
            false,
            "CONSTRAINT",
            SECTION_POST_DATA,
            q->data,
            delq->data,
            NULL,
            NULL,
            0,
            NULL,
            NULL);
    }

    /* Dump Constraint Comments --- only works for table constraints */
    if (NULL != tbinfo && coninfo->separate)
        dumpTableConstraintComment(fout, coninfo);

    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
}

/*
 * dumpConstraint
 *	  write out to fout a user-defined constraint
 */

static void dumpConstraint(Archive* fout, ConstraintInfo* coninfo)
{
    TableInfo* tbinfo = NULL;
    PQExpBuffer q;
    PQExpBuffer delq;
    bool isBcompatibility = false;

    ddr_Assert(NULL != fout && NULL != coninfo);

    /* Skip if not to be dumped */
    if (!coninfo->dobj.dump || dataOnly)
        return;

    tbinfo = coninfo->contable;
    if (tbinfo == NULL) {
       return;
    }
    setTableInfoAttNumAndNames(fout, tbinfo);

    q = createPQExpBuffer();
    delq = createPQExpBuffer();

    isBcompatibility = findDBCompatibility(fout, PQdb(GetConnection(fout)));

    if (coninfo->contype == 'p' || coninfo->contype == 'u' || coninfo->contype == 'x') {
        if (coninfo->separate) {
            /* Index-related constraint */
            IndxInfo* indxinfo = NULL;

            indxinfo = (IndxInfo*)findObjectByDumpId(coninfo->conindex);

            if (indxinfo == NULL)
                exit_horribly(NULL, "missing index for constraint \"%s\"\n", coninfo->dobj.name);

            if (binary_upgrade) {
                binary_upgrade_set_pg_class_oids(fout, q, indxinfo->dobj.catId.oid, true, false);

                if (tbinfo->parttype == PARTTYPE_PARTITIONED_RELATION ||
                    tbinfo->parttype == PARTTYPE_SUBPARTITIONED_RELATION ||
                    tbinfo->parttype == PARTTYPE_VALUE_PARTITIONED_RELATION) {
                    binary_upgrade_set_pg_partition_oids(fout, q, tbinfo, indxinfo, true);
                }

                if ((NULL != tbinfo->reloptions) &&
                    ((NULL != strstr(tbinfo->reloptions, "orientation=column")) ||
                        (NULL != strstr(tbinfo->reloptions, "orientation=orc"))) &&
                    ((NULL != strstr(indxinfo->indexdef, "USING \"psort\"")) ||
                        (NULL != strstr(indxinfo->indexdef, "USING psort")))) {
                    if (tbinfo->parttype == PARTTYPE_PARTITIONED_RELATION) {
                        bupgrade_set_pg_partition_cstore_oids(fout, q, tbinfo, indxinfo, true);
                    } else {
                        bupgrade_set_cstore_oids(fout, q, tbinfo, indxinfo, true);
                    }
                }
            }

            appendPQExpBuffer(q, "ALTER TABLE %s\n", fmtId(tbinfo->dobj.name));
            appendPQExpBuffer(q, "    ADD CONSTRAINT %s ", fmtId(coninfo->dobj.name));

            if (coninfo->contype == 'x') {
                /* pg_get_constraintdef should have provided everything */
                appendPQExpBuffer(q, "%s;\n", coninfo->condef);
            } else {
                dumpUniquePrimaryDef(q, coninfo, indxinfo, isBcompatibility);
                appendPQExpBuffer(q, ";\n");
            }

            /* If the index is clustered, we need to record that. */
            if (indxinfo->indisclustered) {
                appendPQExpBuffer(q, "\nALTER TABLE %s CLUSTER", fmtId(tbinfo->dobj.name));
                appendPQExpBuffer(q, " ON %s;\n", fmtId(indxinfo->dobj.name));
            }

            /* If the index is unusable, we need to record that. */
            if (!indxinfo->indisusable) {
                appendPQExpBuffer(q, "\nALTER INDEX %s UNUSABLE;\n", fmtId(indxinfo->dobj.name));
            }
            
            /*
            * DROP must be fully qualified in case same name appears in
            * pg_catalog
            */
            appendPQExpBuffer(delq, "ALTER TABLE %s%s.", if_exists, fmtId(tbinfo->dobj.nmspace->dobj.name));
            appendPQExpBuffer(delq, "%s ", fmtId(tbinfo->dobj.name));
            appendPQExpBuffer(delq, "DROP CONSTRAINT %s%s%s;\n", if_exists, fmtId(coninfo->dobj.name), if_cascade);

            ArchiveEntry(fout,
                coninfo->dobj.catId,
                coninfo->dobj.dumpId,
                coninfo->dobj.name,
                tbinfo->dobj.nmspace->dobj.name,
                indxinfo->tablespace,
                tbinfo->rolname,
                false,
                "CONSTRAINT",
                SECTION_POST_DATA,
                q->data,
                delq->data,
                NULL,
                NULL,
                0,
                NULL,
                NULL);
        }
    } else if (coninfo->contype == 'f') {
        /*
         * XXX Potentially wrap in a 'SET CONSTRAINTS OFF' block so that the
         * current table data is not processed
         */
        appendPQExpBuffer(q, "ALTER TABLE %s\n", fmtId(tbinfo->dobj.name));
        appendPQExpBuffer(q, "    ADD CONSTRAINT %s %s;\n", fmtId(coninfo->dobj.name), coninfo->condef);

        /*
         * DROP must be fully qualified in case same name appears in
         * pg_catalog
         */
        appendPQExpBuffer(delq, "ALTER TABLE %s%s.", if_exists, fmtId(tbinfo->dobj.nmspace->dobj.name));
        appendPQExpBuffer(delq, "%s ", fmtId(tbinfo->dobj.name));
        appendPQExpBuffer(delq, "DROP CONSTRAINT %s%s%s;\n", if_exists, fmtId(coninfo->dobj.name), if_cascade);

        ArchiveEntry(fout,
            coninfo->dobj.catId,
            coninfo->dobj.dumpId,
            coninfo->dobj.name,
            tbinfo->dobj.nmspace->dobj.name,
            NULL,
            tbinfo->rolname,
            false,
            "FK CONSTRAINT",
            SECTION_POST_DATA,
            q->data,
            delq->data,
            NULL,
            NULL,
            0,
            NULL,
            NULL);
    } else if (coninfo->contype == 'c' && (tbinfo != NULL)) {
        /* CHECK constraint on a table */

        /* Ignore if not to be dumped separately */
        if (coninfo->separate) {
            /* not ONLY since we want it to propagate to children */
            appendPQExpBuffer(q, "ALTER TABLE %s\n", fmtId(tbinfo->dobj.name));
            appendPQExpBuffer(q, "    ADD CONSTRAINT %s %s;\n", fmtId(coninfo->dobj.name), coninfo->condef);

            /*
             * DROP must be fully qualified in case same name appears in
             * pg_catalog
             */
            appendPQExpBuffer(delq, "ALTER TABLE %s%s.", if_exists, fmtId(tbinfo->dobj.nmspace->dobj.name));
            appendPQExpBuffer(delq, "%s ", fmtId(tbinfo->dobj.name));
            appendPQExpBuffer(delq, "DROP CONSTRAINT %s%s%s;\n", if_exists, fmtId(coninfo->dobj.name), if_cascade);

            ArchiveEntry(fout,
                coninfo->dobj.catId,
                coninfo->dobj.dumpId,
                coninfo->dobj.name,
                tbinfo->dobj.nmspace->dobj.name,
                NULL,
                tbinfo->rolname,
                false,
                "CHECK CONSTRAINT",
                SECTION_POST_DATA,
                q->data,
                delq->data,
                NULL,
                NULL,
                0,
                NULL,
                NULL);
        }
    } else if (coninfo->contype == 'c' && tbinfo == NULL) {
        /* CHECK constraint on a domain */
        TypeInfo* tyinfo = coninfo->condomain;

        /* Ignore if not to be dumped separately */
        if (coninfo->separate) {
            appendPQExpBuffer(q, "ALTER DOMAIN %s\n", fmtId(tyinfo->dobj.name));
            appendPQExpBuffer(q, "    ADD CONSTRAINT %s %s;\n", fmtId(coninfo->dobj.name), coninfo->condef);

            /*
             * DROP must be fully qualified in case same name appears in
             * pg_catalog
             */
            appendPQExpBuffer(delq, "ALTER DOMAIN %s.", fmtId(tyinfo->dobj.nmspace->dobj.name));
            appendPQExpBuffer(delq, "%s ", fmtId(tyinfo->dobj.name));
            appendPQExpBuffer(delq, "DROP CONSTRAINT %s;\n", fmtId(coninfo->dobj.name));

            ArchiveEntry(fout,
                coninfo->dobj.catId,
                coninfo->dobj.dumpId,
                coninfo->dobj.name,
                tyinfo->dobj.nmspace->dobj.name,
                NULL,
                tyinfo->rolname,
                false,
                "CHECK CONSTRAINT",
                SECTION_POST_DATA,
                q->data,
                delq->data,
                NULL,
                NULL,
                0,
                NULL,
                NULL);
        }
    } else {
        exit_horribly(NULL, "unrecognized constraint type: %c\n", coninfo->contype);
    }

    /* Dump Constraint Comments --- only works for table constraints */
    if (NULL != tbinfo && coninfo->separate)
        dumpTableConstraintComment(fout, coninfo);

    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
}

/*
 * dumpTableConstraintComment --- dump a constraint's comment if any
 *
 * This is split out because we need the function in two different places
 * depending on whether the constraint is dumped as part of CREATE TABLE
 * or as a separate ALTER command.
 */
static void dumpTableConstraintComment(Archive* fout, ConstraintInfo* coninfo)
{
    TableInfo* tbinfo = coninfo->contable;
    PQExpBuffer labelq = createPQExpBuffer();

    ddr_Assert(NULL != fout && NULL != coninfo);

    appendPQExpBuffer(labelq, "CONSTRAINT %s ", fmtId(coninfo->dobj.name));
    appendPQExpBuffer(labelq, "ON %s", fmtId(tbinfo->dobj.name));
    dumpComment(fout,
        labelq->data,
        tbinfo->dobj.nmspace->dobj.name,
        tbinfo->rolname,
        coninfo->dobj.catId,
        0,
        coninfo->separate ? coninfo->dobj.dumpId : tbinfo->dobj.dumpId);

    destroyPQExpBuffer(labelq);
}

/*
 * findLastBuiltInOid -
 * find the last built in oid
 *
 * For 7.1 and 7.2, we do this by retrieving datlastsysoid from the
 * pg_database entry for the current database
 */
static Oid findLastBuiltinOid_V71(Archive* fout, const char* databasename)
{
    PGresult* res = NULL;
    Oid last_oid = 0;
    PQExpBuffer query = createPQExpBuffer();

    resetPQExpBuffer(query);
    appendPQExpBuffer(query, "SELECT datlastsysoid from pg_database where datname = ");
    appendStringLiteralAH(query, databasename, fout);

    res = ExecuteSqlQueryForSingleRow(fout, query->data);
    last_oid = atooid(PQgetvalue(res, 0, PQfnumber(res, "datlastsysoid")));
    PQclear(res);
    destroyPQExpBuffer(query);
    return last_oid;
}

/*
 * findLastBuiltInOid -
 * find the last built in oid
 *
 * For 7.0, we do this by assuming that the last thing that initdb does is to
 * create the pg_indexes view.	This sucks in general, but seeing that 7.0.x
 * initdb won't be changing anymore, it'll do.
 */
static Oid findLastBuiltinOid_V70(Archive* fout)
{
    PGresult* res = NULL;
    unsigned int last_oid;

    res = ExecuteSqlQueryForSingleRow(fout, (char*)"SELECT oid FROM pg_class WHERE relname = 'pg_indexes'");
    last_oid = atooid(PQgetvalue(res, 0, PQfnumber(res, "oid")));
    PQclear(res);
    return last_oid;
}

/*
 * dumpSequence
 *	  write the declaration (not data) of one user-defined sequence
 */
static void dumpSequence(Archive* fout, TableInfo* tbinfo, bool large)
{
    PGresult* res = NULL;
    char* startv = NULL;
    char* incby = NULL;
    char* maxv = NULL;
    char* minv = NULL;
    char* cache = NULL;
    char bufm[100] = {0};
    char bufx[100] = {0};
    bool cycled = false;
    int nRet = 0;
    const char* optLarge = large ? "LARGE" : "";
    PQExpBuffer query = createPQExpBuffer();
    PQExpBuffer delqry = createPQExpBuffer();
    PQExpBuffer labelq = createPQExpBuffer();

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, tbinfo->dobj.nmspace->dobj.name);
    if (needIgnoreSequence(tbinfo)) {
        destroyPQExpBuffer(query);
        destroyPQExpBuffer(delqry);
        destroyPQExpBuffer(labelq);
        return;
    }
    nRet = snprintf_s(bufm, sizeof(bufm) / sizeof(char), sizeof(bufm) / sizeof(char) - 1, INT64_FORMAT, SEQ_MINVALUE);
    securec_check_ss_c(nRet, "\0", "\0");
    nRet = snprintf_s(bufx, sizeof(bufx) / sizeof(char), sizeof(bufx) / sizeof(char) - 1, INT64_FORMAT, SEQ_MAXVALUE);
    securec_check_ss_c(nRet, "\0", "\0");

    if (fout->remoteVersion >= 80400) {
        appendPQExpBuffer(query,
            "SELECT sequence_name, "
            "start_value, increment_by, "
            "CASE WHEN increment_by > 0 AND max_value = %s THEN NULL "
            "     WHEN increment_by < 0 AND max_value = -1 THEN NULL "
            "     ELSE max_value "
            "END AS max_value, "
            "CASE WHEN increment_by > 0 AND min_value = 1 THEN NULL "
            "     WHEN increment_by < 0 AND min_value = %s THEN NULL "
            "     ELSE min_value "
            "END AS min_value, "
            "cache_value, is_cycled FROM %s",
            bufx,
            bufm,
            fmtId(tbinfo->dobj.name));
    } else {
        appendPQExpBuffer(query,
            "SELECT sequence_name, "
            "0 AS start_value, increment_by, "
            "CASE WHEN increment_by > 0 AND max_value = %s THEN NULL "
            "     WHEN increment_by < 0 AND max_value = -1 THEN NULL "
            "     ELSE max_value "
            "END AS max_value, "
            "CASE WHEN increment_by > 0 AND min_value = 1 THEN NULL "
            "     WHEN increment_by < 0 AND min_value = %s THEN NULL "
            "     ELSE min_value "
            "END AS min_value, "
            "cache_value, is_cycled FROM %s",
            bufx,
            bufm,
            fmtId(tbinfo->dobj.name));
    }

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    if (PQntuples(res) != 1) {
        write_msg(NULL,
            ngettext("query to get data of sequence %s returned %d row (expected 1)\n",
                "query to get data of sequence %s returned %d rows (expected 1)\n",
                PQntuples(res)),
            fmtId(tbinfo->dobj.name),
            PQntuples(res));
        PQclear(res);
        destroyPQExpBuffer(query);
        destroyPQExpBuffer(delqry);
        destroyPQExpBuffer(labelq);
        exit_nicely(1);
    }

    /* Disable this check: it fails if sequence has been renamed */
#ifdef NOT_USED
    if (strcmp(PQgetvalue(res, 0, 0), tbinfo->dobj.name) != 0) {
        write_msg(NULL,
            "query to get data of sequence \"%s\" returned name \"%s\"\n",
            tbinfo->dobj.name,
            PQgetvalue(res, 0, 0));
        PQclear(res);
        destroyPQExpBuffer(query);
        destroyPQExpBuffer(delqry);
        destroyPQExpBuffer(labelq);
        exit_nicely(1);
    }
#endif

    startv = PQgetvalue(res, 0, 1);
    incby = PQgetvalue(res, 0, 2);
    if (!PQgetisnull(res, 0, 3))
        maxv = PQgetvalue(res, 0, 3);
    if (!PQgetisnull(res, 0, 4))
        minv = PQgetvalue(res, 0, 4);
    cache = PQgetvalue(res, 0, 5);
    cycled = (strcmp(PQgetvalue(res, 0, 6), "t") == 0);

    /*
     * DROP must be fully qualified in case same name appears in pg_catalog
     */
    appendPQExpBuffer(delqry, "DROP %s SEQUENCE IF EXISTS %s.", optLarge, fmtId(tbinfo->dobj.nmspace->dobj.name));
    appendPQExpBuffer(delqry, "%s%s;\n", fmtId(tbinfo->dobj.name), if_cascade);

    resetPQExpBuffer(query);

    if (binary_upgrade) {
        binary_upgrade_set_pg_class_oids(fout, query, tbinfo->dobj.catId.oid, false, false);
        binary_upgrade_set_type_oids_by_rel_oid(fout, query, tbinfo->dobj.catId.oid, false);
    }

    /*
     * The DROP statement is automatically backed up when the backup format is -c/-t/-d.
     * So when use include_depend_objs and -p, we should also add the function.
     */
    if (include_depend_objs && !outputClean && IsPlainFormat()) {
        appendPQExpBuffer(query, "DROP %s SEQUENCE IF EXISTS %s.", optLarge, fmtId(tbinfo->dobj.nmspace->dobj.name));
        appendPQExpBuffer(query, "%s CASCADE;\n", fmtId(tbinfo->dobj.name));
    }

    appendPQExpBuffer(query, "CREATE %s SEQUENCE %s\n", optLarge, fmtId(tbinfo->dobj.name));

    if (fout->remoteVersion >= 80400)
        appendPQExpBuffer(query, "    START WITH %s\n", startv);

    appendPQExpBuffer(query, "    INCREMENT BY %s\n", incby);

    if (NULL != minv)
        appendPQExpBuffer(query, "    MINVALUE %s\n", minv);
    else
        appendPQExpBuffer(query, "    NO MINVALUE\n");

    if (NULL != maxv)
        appendPQExpBuffer(query, "    MAXVALUE %s\n", maxv);
    else
        appendPQExpBuffer(query, "    NO MAXVALUE\n");

    appendPQExpBuffer(query, "    CACHE %s%s", cache, (cycled ? "\n    CYCLE" : ""));

    appendPQExpBuffer(query, ";\n");

    appendPQExpBuffer(labelq, "%s SEQUENCE %s", optLarge, fmtId(tbinfo->dobj.name));

    /* binary_upgrade:	no need to clear TOAST table oid */
    if (binary_upgrade)
        binary_upgrade_extension_member(query, &tbinfo->dobj, labelq->data);

    ArchiveEntry(fout,
        tbinfo->dobj.catId,
        tbinfo->dobj.dumpId,
        tbinfo->dobj.name,
        tbinfo->dobj.nmspace->dobj.name,
        NULL,
        tbinfo->rolname,
        false,
        large ? "LARGE SEQUENCE" : "SEQUENCE",
        SECTION_PRE_DATA,
        query->data,
        delqry->data,
        NULL,
        NULL,
        0,
        NULL,
        NULL);

    /*
     * If the sequence is owned by a table column, emit the ALTER for it as a
     * separate TOC entry immediately following the sequence's own entry.
     * It's OK to do this rather than using full sorting logic, because the
     * dependency that tells us it's owned will have forced the table to be
     * created first.  We can't just include the ALTER in the TOC entry
     * because it will fail if we haven't reassigned the sequence owner to
     * match the table's owner.
     *
     * We need not schema-qualify the table reference because both sequence
     * and table must be in the same schema.
     */
    if (OidIsValid(tbinfo->owning_tab)) {
        TableInfo* owning_tab = findTableByOid(tbinfo->owning_tab);

        if (NULL != owning_tab && owning_tab->dobj.dump) {
            resetPQExpBuffer(query);
            appendPQExpBuffer(query, "ALTER %s SEQUENCE %s", optLarge, fmtId(tbinfo->dobj.name));
            appendPQExpBuffer(query, " OWNED BY %s", fmtId(owning_tab->dobj.name));
            appendPQExpBuffer(query, ".%s;\n", fmtId(owning_tab->attnames[tbinfo->owning_col - 1]));
            ArchiveEntry(fout,
                nilCatalogId,
                createDumpId(),
                tbinfo->dobj.name,
                tbinfo->dobj.nmspace->dobj.name,
                NULL,
                tbinfo->rolname,
                false,
                large ? "SEQUENCE OWNED BY" : "LARGE SEQUENCE OWNED BY",
                SECTION_PRE_DATA,
                query->data,
                "",
                NULL,
                &(tbinfo->dobj.dumpId),
                1,
                NULL,
                NULL);
        }
    }

    /* Dump Sequence Comments and Security Labels */
    dumpComment(fout,
        labelq->data,
        tbinfo->dobj.nmspace->dobj.name,
        tbinfo->rolname,
        tbinfo->dobj.catId,
        0,
        tbinfo->dobj.dumpId);
    dumpSecLabel(fout,
        labelq->data,
        tbinfo->dobj.nmspace->dobj.name,
        tbinfo->rolname,
        tbinfo->dobj.catId,
        0,
        tbinfo->dobj.dumpId);

    PQclear(res);

    destroyPQExpBuffer(query);
    destroyPQExpBuffer(delqry);
    destroyPQExpBuffer(labelq);
}

/*
 * dumpSequenceData
 *	  write the data of one user-defined sequence
 */
static void dumpSequenceData(Archive* fout, TableDataInfo* tdinfo, bool large)
{
    TableInfo* tbinfo = tdinfo->tdtable;
    PGresult* res = NULL;
    char* last = NULL;
    char* start = NULL;
    char* increment = NULL;
    char* max = NULL;
    char* min = NULL;

    bool called = false;
    PQExpBuffer query = createPQExpBuffer();

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, tbinfo->dobj.nmspace->dobj.name);

    if (needIgnoreSequence(tbinfo)) {
        destroyPQExpBuffer(query);
        return;
    }
    appendPQExpBuffer(
        query, "SELECT start_value, increment_by, max_value, min_value, is_called"
#ifndef ENABLE_MULTIPLE_NODES
        ", last_value"
#endif
        " FROM %s", fmtId(tbinfo->dobj.name));

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    if (PQntuples(res) != 1) {
        write_msg(NULL,
            ngettext("query to get data of sequence %s returned %d row (expected 1)\n",
                "query to get data of sequence %s returned %d rows (expected 1)\n",
                PQntuples(res)),
            fmtId(tbinfo->dobj.name),
            PQntuples(res));
        PQclear(res);
        destroyPQExpBuffer(query);
        exit_nicely(1);
    }

    /*local last_value and isCalled are not reliable. but the following values are reliable */
    start = PQgetvalue(res, 0, 0);

    increment = PQgetvalue(res, 0, 1);

    max = PQgetvalue(res, 0, 2);

    min = PQgetvalue(res, 0, 3);

    called = (strcmp(PQgetvalue(res, 0, 4), "t") == 0);

#ifndef ENABLE_MULTIPLE_NODES
    last = PQgetvalue(res, 0, 5);
    if (isDB4AIschema(tbinfo->dobj.nmspace)) {
        last = atoi(last) > atoi(min) ? last : min;
    }
#else
    /*
     * In Postgres-XC it is possible that the current value of a
     * sequence cached on each node is different as several sessions
     * might use the sequence on different nodes. So what we do here
     * to get a consistent dump is to get the next value of sequence.
     * This insures that sequence value is unique as nextval is directly
     * obtained from GTM.
     * If the sequence reaches the maximum or minimum value already, we may get
     * ERROR when calling nextval. Currently we don't have an API to get the
     * CURRVAL on GTM. So we need more code to handle the error case.
     * After GTMCURRVAL is implemented, it's a much better approach.
     */

    /*create a savepoint for nextval call*/
    ExecuteSqlStatement(fout, "SAVEPOINT bfnextval");

    resetPQExpBuffer(query);
    appendPQExpBuffer(query, "SELECT pg_catalog.nextval(");
    appendStringLiteralAH(query, fmtId(tbinfo->dobj.name), fout);
    appendPQExpBuffer(query, ");\n");
    PQclear(res);

    ArchiveHandle* AH = (ArchiveHandle*)fout;
    res = PQexec(AH->connection, query->data);

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        /*Check if it fail because of reaching maximum value or not*/
        char* errMessage = PQerrorMessage(AH->connection);
        if (strstr(errMessage, "reached maximum value") != NULL) {
            last = max;
            called = true;
        } else if (strstr(errMessage, "reached minimum value") != NULL) {
            last = min;
            called = true;
        } else {
            write_msg(NULL, "failed to execute SELECT pg_catalog.nextval(%s).\n", fmtId(tbinfo->dobj.name));
            PQclear(res);
            destroyPQExpBuffer(query);
            exit_nicely(1);
        }
    } else {
        /*call nextvalue successfully*/
        if (PQntuples(res) != 1) {
            write_msg(NULL,
                ngettext("query to get nextval of sequence %s "
                         "returned %d rows (expected 1)\n",
                    "query to get nextval of sequence %s "
                    "returned %d rows (expected 1)\n",
                    PQntuples(res)),
                fmtId(tbinfo->dobj.name),
                PQntuples(res));
            PQclear(res);
            destroyPQExpBuffer(query);
            exit_nicely(1);
        }

        last = PQgetvalue(res, 0, 0);
    }

    ExecuteSqlStatement(fout, "ROLLBACK TO SAVEPOINT bfnextval");
    ExecuteSqlStatement(fout, "RELEASE bfnextval");
#endif

    resetPQExpBuffer(query);
    appendPQExpBuffer(query, "SELECT pg_catalog.setval(");
    appendStringLiteralAH(query, fmtId(tbinfo->dobj.name), fout);
    appendPQExpBuffer(query, ", %s, %s);\n", last, (called ? "true" : "false"));

    const char* sequenceSet = large ? "LARGE SEQUENCE SET" : "SEQUENCE SET";

    ArchiveEntry(fout,
        nilCatalogId,
        createDumpId(),
        tbinfo->dobj.name,
        tbinfo->dobj.nmspace->dobj.name,
        NULL,
        tbinfo->rolname,
        false,
        sequenceSet,
        SECTION_DATA,
        query->data,
        "",
        NULL,
        &(tbinfo->dobj.dumpId),
        1,
        NULL,
        NULL);

    PQclear(res);
    destroyPQExpBuffer(query);
}

static void dumpTrigger(Archive* fout, TriggerInfo* tginfo)
{
    TableInfo* tbinfo = tginfo->tgtable;
    PQExpBuffer query;
    PQExpBuffer delqry;
    PQExpBuffer labelq;
    char* tgargs = NULL;
    size_t lentgargs = 0;
    int findx = 0;

    if (dataOnly)
        return;

    query = createPQExpBuffer();
    delqry = createPQExpBuffer();
    labelq = createPQExpBuffer();

    /*
     * DROP must be fully qualified in case same name appears in pg_catalog
     */
    appendPQExpBuffer(delqry, "DROP TRIGGER %s ", fmtId(tginfo->dobj.name));
    appendPQExpBuffer(delqry, "ON %s.", fmtId(tbinfo->dobj.nmspace->dobj.name));
    appendPQExpBuffer(delqry, "%s;\n", fmtId(tbinfo->dobj.name));

    if (NULL != tginfo->tgdef) {
        if (tginfo->tgdb) {
            appendPQExpBuffer(query, "DROP FUNCTION %s ;\n", tginfo->tgfname);
            if (tginfo->tgbodybstyle && IsPlainFormat())
                appendPQExpBuffer(query, "delimiter //\n");
            appendPQExpBuffer(query, "%s", tginfo->tgdef);
            if (IsPlainFormat()) {
                if (tginfo->tgbodybstyle)
                    appendPQExpBuffer(query, "\n//\n");
                else
                    appendPQExpBuffer(query, "\n/\n");
            } else {
                appendPQExpBuffer(query, ";\n");
            }
        } else {
            appendPQExpBuffer(query, "%s;\n", tginfo->tgdef);
        }
        if (tginfo->tgbodybstyle && IsPlainFormat())
            appendPQExpBuffer(query, "delimiter ;\n");
    } else {
        if (tginfo->tgisconstraint) {
            appendPQExpBuffer(query, "CREATE CONSTRAINT TRIGGER ");
            appendPQExpBufferStr(query, fmtId(tginfo->tgconstrname));
        } else {
            appendPQExpBuffer(query, "CREATE TRIGGER ");
            appendPQExpBufferStr(query, fmtId(tginfo->dobj.name));
        }
        appendPQExpBuffer(query, "\n    ");

        /* Trigger type */
        if (TRIGGER_FOR_BEFORE(tginfo->tgtype))
            appendPQExpBuffer(query, "BEFORE");
        else if (TRIGGER_FOR_AFTER(tginfo->tgtype))
            appendPQExpBuffer(query, "AFTER");
        else if (TRIGGER_FOR_INSTEAD(tginfo->tgtype))
            appendPQExpBuffer(query, "INSTEAD OF");
        else {
            write_msg(NULL, "unexpected tgtype value: %d\n", tginfo->tgtype);
            exit_nicely(1);
        }

        findx = 0;
        if (TRIGGER_FOR_INSERT(tginfo->tgtype)) {
            appendPQExpBuffer(query, " INSERT");
            findx++;
        }
        if (TRIGGER_FOR_DELETE(tginfo->tgtype)) {
            if (findx > 0)
                appendPQExpBuffer(query, " OR DELETE");
            else
                appendPQExpBuffer(query, " DELETE");
            findx++;
        }
        if (TRIGGER_FOR_UPDATE(tginfo->tgtype)) {
            if (findx > 0)
                appendPQExpBuffer(query, " OR UPDATE");
            else
                appendPQExpBuffer(query, " UPDATE");
            findx++;
        }
        if (TRIGGER_FOR_TRUNCATE(tginfo->tgtype)) {
            if (findx > 0)
                appendPQExpBuffer(query, " OR TRUNCATE");
            else
                appendPQExpBuffer(query, " TRUNCATE");
            findx++;
        }
        appendPQExpBuffer(query, " ON %s\n", fmtId(tbinfo->dobj.name));

        if (tginfo->tgisconstraint) {
            if (OidIsValid(tginfo->tgconstrrelid)) {
                /* If we are using regclass, name is already quoted */
                if (fout->remoteVersion >= 70300)
                    appendPQExpBuffer(query, "    FROM %s\n    ", tginfo->tgconstrrelname);
                else
                    appendPQExpBuffer(query, "    FROM %s\n    ", fmtId(tginfo->tgconstrrelname));
            }
            if (!tginfo->tgdeferrable)
                appendPQExpBuffer(query, "NOT ");
            appendPQExpBuffer(query, "DEFERRABLE INITIALLY ");
            if (tginfo->tginitdeferred)
                appendPQExpBuffer(query, "DEFERRED\n");
            else
                appendPQExpBuffer(query, "IMMEDIATE\n");
        }

        if (TRIGGER_FOR_ROW(tginfo->tgtype))
            appendPQExpBuffer(query, "    FOR EACH ROW\n    ");
        else
            appendPQExpBuffer(query, "    FOR EACH STATEMENT\n    ");

        /* In 7.3, result of regproc is already quoted */
        if (fout->remoteVersion >= 70300)
            appendPQExpBuffer(query, "EXECUTE PROCEDURE %s(", tginfo->tgfname);
        else
            appendPQExpBuffer(query, "EXECUTE PROCEDURE %s(", fmtId(tginfo->tgfname));

        tgargs = (char*)PQunescapeBytea((unsigned char*)tginfo->tgargs, &lentgargs);
        if (NULL != tgargs) {
            const char* p = tgargs;
            for (findx = 0; findx < tginfo->tgnargs; findx++) {
                /* find the embedded null that terminates this trigger argument */
                size_t tlen = strlen(p);

                if (p + tlen >= tgargs + lentgargs) {
                    /* hm, not found before end of bytea value... */
                    write_msg(NULL,
                        "invalid argument string (%s) for trigger %s on table %s\n",
                        tginfo->tgargs,
                        fmtId(tginfo->dobj.name),
                        fmtId(tbinfo->dobj.name));
                    exit_nicely(1);
                }

                if (findx > 0)
                    appendPQExpBuffer(query, ", ");
                appendStringLiteralAH(query, p, fout);
                p += tlen + 1;
            }
            free(tgargs);
            tgargs = NULL;
            appendPQExpBuffer(query, ");\n");
        }
    }

    if (tginfo->tgenabled != 't' && tginfo->tgenabled != 'O') {
        appendPQExpBuffer(query, "\nALTER TABLE %s ", fmtId(tbinfo->dobj.name));
        switch (tginfo->tgenabled) {
            case 'D':
            case 'f':
                appendPQExpBuffer(query, "DISABLE");
                break;
            case 'A':
                appendPQExpBuffer(query, "ENABLE ALWAYS");
                break;
            case 'R':
                appendPQExpBuffer(query, "ENABLE REPLICA");
                break;
            default:
                appendPQExpBuffer(query, "ENABLE");
                break;
        }
        appendPQExpBuffer(query, " TRIGGER %s;\n", fmtId(tginfo->dobj.name));
    }

    appendPQExpBuffer(labelq, "TRIGGER %s ", fmtId(tginfo->dobj.name));
    appendPQExpBuffer(labelq, "ON %s", fmtId(tbinfo->dobj.name));

    ArchiveEntry(fout,
        tginfo->dobj.catId,
        tginfo->dobj.dumpId,
        tginfo->dobj.name,
        tbinfo->dobj.nmspace->dobj.name,
        NULL,
        tbinfo->rolname,
        false,
        "TRIGGER",
        SECTION_POST_DATA,
        query->data,
        delqry->data,
        NULL,
        NULL,
        0,
        NULL,
        NULL);

    dumpComment(fout,
        labelq->data,
        tbinfo->dobj.nmspace->dobj.name,
        tbinfo->rolname,
        tginfo->dobj.catId,
        0,
        tginfo->dobj.dumpId);

    destroyPQExpBuffer(query);
    destroyPQExpBuffer(delqry);
    destroyPQExpBuffer(labelq);
}

static bool hasExcludeGucParam(SimpleStringList *guc, const char *guc_param)
{
    SimpleStringListCell* cell = NULL;

    if (guc->head == NULL) {
        return false; /* nothing to do */
    }

    for (cell = guc->head; cell != NULL; cell = cell->next) {
        if (strcmp(cell->val, guc_param) == 0)
            return true;
    }

    return false;
}

/*
 * dumpRule
 *		Dump a rule
 */
static void dumpRule(Archive* fout, RuleInfo* rinfo)
{
    TableInfo* tbinfo = rinfo->ruletable;
    PQExpBuffer query;
    PQExpBuffer cmd;
    PQExpBuffer delcmd;
    PQExpBuffer labelq;
    PGresult* res = NULL;
    bool isaddreplace = false;
    bool isexguc = false;
    bool isdroprule = false;
    char *rule = NULL;
    char *data = NULL;

    /* Skip if not to be dumped */
    if (!rinfo->dobj.dump || dataOnly)
        return;

    /*
     * If it is an ON SELECT rule that is created implicitly by CREATE VIEW,
     * we do not want to dump it as a separate object.
     */
    if (!rinfo->separate)
        return;

    /*
     * Make sure we are in proper schema.
     */
    selectSourceSchema(fout, tbinfo->dobj.nmspace->dobj.name);

    query = createPQExpBuffer();
    cmd = createPQExpBuffer();
    delcmd = createPQExpBuffer();
    labelq = createPQExpBuffer();

    if (fout->remoteVersion >= 70300) {
        appendPQExpBuffer(
            query, "SELECT pg_catalog.pg_get_ruledef('%u'::pg_catalog.oid) AS definition", rinfo->dobj.catId.oid);
    } else {
        /* Rule name was unique before 7.3 ... */
        appendPQExpBuffer(query, "SELECT pg_catalog.pg_get_ruledef('%s') AS definition", rinfo->dobj.name);
    }

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    if (PQntuples(res) != 1) {
        write_msg(NULL,
            "query to get rule %s for table %s failed: wrong number of rows returned\n",
            fmtId(rinfo->dobj.name),
            fmtId(tbinfo->dobj.name));
        exit_nicely(1);
    }

    /*Enable the rule support function.*/
    isexguc = hasExcludeGucParam(&exclude_guc, "enable_cluster_resize");
    if (!isexguc) {
        printfPQExpBuffer(cmd, "SET enable_cluster_resize = on;\n");
    }

    rule = PQgetvalue(res, 0, 0);    
    isaddreplace = (targetV1 || targetV5) && rule;
    if (isaddreplace) {        
        char *p = NULL;
        int ret = 0;
        errno_t tnRet = 0;

        data = (char *)pg_malloc(strlen(rule) + PATCH_LEN);
        tnRet = memset_s(data, strlen(rule) + PATCH_LEN, 0, strlen(rule) + PATCH_LEN);
        securec_check_c(tnRet, "\0", "\0");
        p = strsep(&rule, " ");
        ret = strcat_s(data, strlen(rule) + PATCH_LEN, p);
        securec_check_c(ret, "\0", "\0");
        ret = strcat_s(data, strlen(rule) + PATCH_LEN, " OR REPLACE ");
        securec_check_c(ret, "\0", "\0");
        ret = strcat_s(data, strlen(rule) + PATCH_LEN, rule);
        securec_check_c(ret, "\0", "\0");
        rule = data;
    }
    appendPQExpBuffer(cmd, "%s\n", rule);
    if (data)
        free(data);

    /*
     * Add the command to alter the rules replication firing semantics if it
     * differs from the default.
     */
    if (rinfo->ev_enabled != 'O') {
        appendPQExpBuffer(cmd, "ALTER TABLE %s ", fmtId(tbinfo->dobj.name));
        switch (rinfo->ev_enabled) {
            case 'A':
                appendPQExpBuffer(cmd, "ENABLE ALWAYS RULE %s;\n", fmtId(rinfo->dobj.name));
                break;
            case 'R':
                appendPQExpBuffer(cmd, "ENABLE REPLICA RULE %s;\n", fmtId(rinfo->dobj.name));
                break;
            case 'D':
                appendPQExpBuffer(cmd, "DISABLE RULE %s;\n", fmtId(rinfo->dobj.name));
                break;
            default:
                break;
        }
    }

    /*
     * Apply view's reloptions when its ON SELECT rule is separate.
     */
    if ((rinfo->reloptions != NULL) && strlen(rinfo->reloptions) > 0) {
        appendPQExpBuffer(cmd, "ALTER VIEW %s SET (%s);\n", fmtId(tbinfo->dobj.name), rinfo->reloptions);
    }

    /*
     * DROP must be fully qualified in case same name appears in pg_catalog
     */
    isdroprule = targetV1 || targetV5;
    if (!isdroprule) {
        appendPQExpBuffer(delcmd, "DROP RULE %s ", fmtId(rinfo->dobj.name));
        appendPQExpBuffer(delcmd, "ON %s.", fmtId(tbinfo->dobj.nmspace->dobj.name));
        appendPQExpBuffer(delcmd, "%s;\n", fmtId(tbinfo->dobj.name));
    }

    appendPQExpBuffer(labelq, "RULE %s", fmtId(rinfo->dobj.name));
    appendPQExpBuffer(labelq, " ON %s", fmtId(tbinfo->dobj.name));

    ArchiveEntry(fout,
        rinfo->dobj.catId,
        rinfo->dobj.dumpId,
        rinfo->dobj.name,
        tbinfo->dobj.nmspace->dobj.name,
        NULL,
        tbinfo->rolname,
        false,
        "RULE",
        SECTION_POST_DATA,
        cmd->data,
        delcmd->data,
        NULL,
        NULL,
        0,
        NULL,
        NULL);

    /* Dump rule comments */
    dumpComment(
        fout, labelq->data, tbinfo->dobj.nmspace->dobj.name, tbinfo->rolname, rinfo->dobj.catId, 0, rinfo->dobj.dumpId);

    PQclear(res);

    destroyPQExpBuffer(query);
    destroyPQExpBuffer(cmd);
    destroyPQExpBuffer(delcmd);
    destroyPQExpBuffer(labelq);
}

static void dumpEvent(Archive *fout, EventInfo *einfo)
{
    PQExpBuffer query;
    PQExpBuffer delqry;

    if (dataOnly || !einfo->dobj.dump) {
        return;
    }

    /* Set proper schema search path so type references list correctly */
    selectSourceSchema(fout, einfo->dobj.nmspace->dobj.name);

    query = createPQExpBuffer();
    delqry = createPQExpBuffer();

    appendPQExpBuffer(delqry, "DROP EVENT %s ", fmtId(einfo->evname));
    appendPQExpBuffer(query, "CREATE ");
    if (einfo->evdefiner) {
        appendPQExpBuffer(query, "definer=%s ", einfo->evdefiner);
    }
    appendPQExpBuffer(query, "EVENT %s.", fmtId(einfo->nspname));
    appendPQExpBuffer(query, "%s ON SCHEDULE ", fmtId(einfo->evname));
    if (strcmp(einfo->intervaltime, "null") != 0) {
        int interval_len = 10;
        char* begin_pos = einfo->intervaltime + interval_len;
        char* end_pos = strstr(begin_pos, "\'");
        uint32 pos_offset;
        char* interval_unit;
        if (end_pos) {
            interval_unit = end_pos + INTERVAL_UNITE_OFFSET;
            pos_offset = (uint32)(end_pos - begin_pos);
        } else {
            return;
        }
        char interval_num[pos_offset + 1] = {0};
        errno_t rc = memcpy_s(interval_num, pos_offset + 1, begin_pos, pos_offset);
        securec_check_c(rc, "\0", "\0");
        
        bool is_exist_to_unite = (strstr(einfo->intervaltime, " to ") != NULL);
        if (is_exist_to_unite) {
            appendPQExpBuffer(query, "EVERY \'%s\' ", interval_num);
        } else {
            appendPQExpBuffer(query, "EVERY %s ", interval_num);
        }
        appendPQExpBuffer(query, "%s ", interval_unit);
        if (einfo->starttime) {
            appendPQExpBuffer(query, "STARTS \'%s\' ", einfo->starttime);
        }
        if (einfo->endtime) {
            appendPQExpBuffer(query, "ENDS \'%s\' ", einfo->endtime);
        }
    } else {
        appendPQExpBuffer(query, "AT \'%s\' ", einfo->starttime);
    }
    if (einfo->autodrop) {
        appendPQExpBuffer(query, "ON COMPLETION NOT PRESERVE ");
    } else {
        appendPQExpBuffer(query, "ON COMPLETION PRESERVE ");
    }
    if (einfo->evstatus) {
        appendPQExpBuffer(query, "ENABLE ");
    } else {
        appendPQExpBuffer(query, "DISABLE ");
    }
    if (einfo->comment) {
        appendPQExpBuffer(query, "COMMENT \'%s\' ", einfo->comment);
    }
    appendPQExpBuffer(query, "DO %s;", einfo->evbody);

    ArchiveEntry(fout,
        einfo->dobj.catId,
        einfo->dobj.dumpId,
        einfo->evname,
        einfo->dobj.nmspace->dobj.name,
        NULL,
        einfo->evdefiner,
        false,
        "EVENT",
        SECTION_POST_DATA,
        query->data,
        delqry->data,
        NULL,
        NULL,
        0,
        NULL,
        NULL);
    destroyPQExpBuffer(query);
    destroyPQExpBuffer(delqry);
}
/*
 * dumpRlsPolicy
 *	Dump row level security information for table
 */
static void dumpRlsPolicy(Archive* fout, RlsPolicyInfo* policyinfo)
{
    /* Skip if not to be dumped */
    if ((policyinfo->dobj.dump == false) || (dataOnly == true))
        return;

    TableInfo* tbinfo = policyinfo->policytable;
    PQExpBuffer query = createPQExpBuffer();
    const char* tablename = fmtQualifiedId(fout, tbinfo->dobj.nmspace->dobj.name, tbinfo->dobj.name);

    /* dump row level security policy */
    char* polcmd = NULL;
    if (policyinfo->polcmd == '*')
        polcmd = "ALL";
    else if (policyinfo->polcmd == 'r')
        polcmd = "SELECT";
    else if (policyinfo->polcmd == 'w')
        polcmd = "UPDATE";
    else if (policyinfo->polcmd == 'd')
        polcmd = "DELETE";
    else {
        write_msg(NULL, "unexpected policy command type: %c\n", policyinfo->polcmd);
        exit_nicely(1);
    }
    appendPQExpBuffer(query, "CREATE POLICY %s ", fmtId(policyinfo->polname));
    appendPQExpBuffer(query, "ON %s ", tablename);
    appendPQExpBuffer(query, "AS %s ", policyinfo->polpermissive ? "PERMISSIVE" : "RESTRICTIVE");
    appendPQExpBuffer(query, "FOR %s TO %s USING(%s)", polcmd, policyinfo->polroles, policyinfo->polqual);
    appendPQExpBuffer(query, ";\n");
    PQExpBuffer tag = createPQExpBuffer();
    appendPQExpBuffer(tag, "%s %s", policyinfo->dobj.name, policyinfo->polname);
    PQExpBuffer delcmd = createPQExpBuffer();
    appendPQExpBuffer(delcmd, "DROP POLICY %s ON %s;", policyinfo->polname, tablename);
    ArchiveEntry(fout,
        policyinfo->dobj.catId,
        policyinfo->dobj.dumpId,
        tag->data,
        tbinfo->dobj.nmspace->dobj.name,
        NULL,
        tbinfo->rolname,
        false,
        "ROW LEVEL SECURITY POLICY",
        SECTION_POST_DATA,
        query->data,
        "",
        NULL,
        NULL,
        0,
        NULL,
        NULL);
    destroyPQExpBuffer(query);
    destroyPQExpBuffer(tag);
    destroyPQExpBuffer(delcmd);
    return;
}

static void dumpSynonym(Archive* fout)
{
    PGresult* res = NULL;
    int ntups = 0;
    bool is_exists = false;
    ArchiveHandle* AH = (ArchiveHandle*)fout;
    int i = 0;
    int i_oid = 0;
    int i_tableoid = 0;
    int i_synname = 0;
    int i_nspname = 0;
    int i_rolname = 0;
    int i_synobjschema = 0;
    int i_synobjname = 0;

    PQExpBuffer query;
    PQExpBuffer q;
    PQExpBuffer delq;

    if (!isExecUserSuperRole(fout)) {
        write_msg(NULL, "WARNING: synonym not dumped because current user is not a superuser\n");
        return;
    }

    selectSourceSchema(fout, "pg_catalog");
    query = createPQExpBuffer();
    printfPQExpBuffer(query,
        "SELECT c.relname FROM pg_class c, pg_namespace n "
        "WHERE relname = 'pg_synonym' AND n.nspname='pg_catalog';");
    is_exists = isExistsSQLResult(AH->connection, query->data);
    if (!is_exists) {
        destroyPQExpBuffer(query);
        return;
    } else {
        resetPQExpBuffer(query);
    }

    /*
     * Only the super user can access pg_authid. Therefore, user verification is ignored.
     */
    appendPQExpBuffer(query,
        "SELECT s.oid, s.tableoid, s.synname, n.nspname, a.rolname, s.synobjschema, s.synobjname "
        "FROM pg_synonym s, pg_namespace n, pg_authid a "
        "WHERE n.oid = s.synnamespace AND s.synowner = a.oid;");

    /* Fetch column attnames */
    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);
    if (ntups < 1) {
        PQclear(res);
        destroyPQExpBuffer(query);
        return;
    }

    i_oid = PQfnumber(res, "oid");
    i_tableoid = PQfnumber(res, "tableoid");
    i_synname = PQfnumber(res, "synname");
    i_nspname = PQfnumber(res, "nspname");
    i_rolname = PQfnumber(res, "rolname");
    i_synobjschema = PQfnumber(res, "synobjschema");
    i_synobjname = PQfnumber(res, "synobjname");

    for (i = 0; i < ntups; i++) {
        char* synname = NULL;
        char* nspname = NULL;
        char* rolname = NULL;
        char* synobjschema = NULL;
        char* synobjname = NULL;
        CatalogId objId;

        objId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        objId.oid = atooid(PQgetvalue(res, i, i_oid));
        /*
        * Do not dump synonym defined by package.
        */
        if (GetVersionNum(fout) >= PACKAGE_ENHANCEMENT &&
            IsPackageObject(fout, objId.tableoid, objId.oid)) {
            continue;
        }
        synname = gs_strdup(PQgetvalue(res, i, i_synname));
        nspname = gs_strdup(PQgetvalue(res, i, i_nspname));
        rolname = gs_strdup(PQgetvalue(res, i, i_rolname));
        synobjschema = gs_strdup(PQgetvalue(res, i, i_synobjschema));
        synobjname = gs_strdup(PQgetvalue(res, i, i_synobjname));

        q = createPQExpBuffer();
        delq = createPQExpBuffer();

        appendPQExpBuffer(delq, "DROP SYNONYM IF EXISTS %s.", fmtId(nspname));
        appendPQExpBuffer(delq, "%s;\n", fmtId(synname));

        appendPQExpBuffer(q, "CREATE OR REPLACE SYNONYM %s.", fmtId(nspname));
        appendPQExpBuffer(q, "%s ", fmtId(synname));
        appendPQExpBuffer(q, "FOR %s.", fmtId(synobjschema));
        appendPQExpBuffer(q, "%s;", fmtId(synobjname));
        ArchiveEntry(fout,
            objId,            /* catalog ID */
            createDumpId(),   /* dump ID */
            synname,          /* Name */
            nspname,          /* Namespace */
            NULL,             /* Tablespace */
            rolname,          /* Owner */
            false,            /* with oids */
            "SYNONYM",        /* Desc */
            SECTION_PRE_DATA, /* Section */
            q->data,          /* Create */
            delq->data,       /* Del */
            NULL,             /* Copy */
            NULL,             /* Deps */
            0,                /* #Dep */
            NULL,             /* Dumper */
            NULL);            /* Dumper arg */

        /*
         * Comments, Security Labels and ACL info for SYNONYM are not supported, so skip them.
         */

        destroyPQExpBuffer(q);
        destroyPQExpBuffer(delq);

        GS_FREE(synname);
        GS_FREE(nspname);
        GS_FREE(rolname);
        GS_FREE(synobjschema);
        GS_FREE(synobjname);
    }

    destroyPQExpBuffer(query);
    PQclear(res);
}

/*
 * getExtensionMembership --- obtain extension membership data
 */
void getExtensionMembership(Archive* fout, ExtensionInfo extinfo[], int numExtensions)
{
    PQExpBuffer query;
    PGresult* res = NULL;
    int ntups = 0;
    int i;
    int i_classid = 0;
    int i_objid = 0;
    int i_refclassid = 0;
    int i_refobjid = 0;
    DumpableObject* dobj = NULL;
    DumpableObject* refdobj = NULL;

    /* Nothing to do if no extensions */
    if (numExtensions == 0)
        return;

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, "pg_catalog");

    query = createPQExpBuffer();

    /* refclassid constraint is redundant but may speed the search */
    appendPQExpBuffer(query,
        "SELECT "
        "classid, objid, refclassid, refobjid "
        "FROM pg_depend "
        "WHERE refclassid = 'pg_extension'::regclass "
        "AND deptype = 'e' "
        "ORDER BY 3,4");

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    ntups = PQntuples(res);

    i_classid = PQfnumber(res, "classid");
    i_objid = PQfnumber(res, "objid");
    i_refclassid = PQfnumber(res, "refclassid");
    i_refobjid = PQfnumber(res, "refobjid");

    /*
     * Since we ordered the SELECT by referenced ID, we can expect that
     * multiple entries for the same extension will appear together; this
     * saves on searches.
     */
    refdobj = NULL;

    for (i = 0; i < ntups; i++) {
        CatalogId objId;
        CatalogId refobjId;

        objId.tableoid = atooid(PQgetvalue(res, i, i_classid));
        objId.oid = atooid(PQgetvalue(res, i, i_objid));
        refobjId.tableoid = atooid(PQgetvalue(res, i, i_refclassid));
        refobjId.oid = atooid(PQgetvalue(res, i, i_refobjid));

        if (refdobj == NULL || refdobj->catId.tableoid != refobjId.tableoid || refdobj->catId.oid != refobjId.oid)
            refdobj = findObjectByCatalogId(refobjId);

        /*
         * Failure to find objects mentioned in pg_depend is not unexpected,
         * since for example we don't collect info about TOAST tables.
         */
        if (refdobj == NULL) {
#ifdef NOT_USED
            write_stderr("no referenced object %u %u\n", refobjId.tableoid, refobjId.oid);
#endif
            continue;
        }

        dobj = findObjectByCatalogId(objId);

        if (dobj == NULL) {
#ifdef NOT_USED
            write_stderr("no referencing object %u %u\n", objId.tableoid, objId.oid);
#endif
            continue;
        }

        /* Record dependency so that getDependencies needn't repeat this */
        addObjectDependency(dobj, refdobj->dumpId);

        dobj->ext_member = true;

        /*
         * Normally, mark the member object as not to be dumped.  But in
         * binary upgrades, we still dump the members individually, since the
         * idea is to exactly reproduce the database contents rather than
         * replace the extension contents with something different.
         */
        if (!binary_upgrade)
            dobj->dump = false;
        else
            dobj->dump = refdobj->dump;
    }

    PQclear(res);

    /*
     * Now identify extension configuration tables and create TableDataInfo
     * objects for them, ensuring their data will be dumped even though the
     * tables themselves won't be.
     *
     * Note that we create TableDataInfo objects even in schemaOnly mode, ie,
     * user data in a configuration table is treated like schema data. This
     * seems appropriate since system data in a config table would get
     * reloaded by CREATE EXTENSION.
     */
    for (i = 0; i < numExtensions; i++) {
        ExtensionInfo* curext = &(extinfo[i]);
        char* extconfig = curext->extconfig;
        char* extcondition = curext->extcondition;
        char** extconfigarray = NULL;
        char** extconditionarray = NULL;
        int nconfigitems;
        int nconditionitems;

        /* Tables of not-to-be-dumped extensions shouldn't be dumped */
        if (!curext->dobj.dump)
            continue;

        if (parsePGArray(extconfig, &extconfigarray, &nconfigitems) &&
            parsePGArray(extcondition, &extconditionarray, &nconditionitems) && nconfigitems == nconditionitems) {
            int j;

            for (j = 0; j < nconfigitems; j++) {
                TableInfo* configtbl = NULL;

                configtbl = findTableByOid(atooid(extconfigarray[j]));
                if (configtbl == NULL)
                    continue;

                /*
                 * Note: config tables are dumped without OIDs regardless of
                 * the --oids setting.	This is because row filtering
                 * conditions aren't compatible with dumping OIDs.
                 */
                makeTableDataInfo(configtbl, false);
                if (configtbl->dataObj != NULL) {
                    if (strlen(extconditionarray[j]) > 0)
                        configtbl->dataObj->filtercond = gs_strdup(extconditionarray[j]);
                }
            }
        }
        if (NULL != extconfigarray) {
            free(extconfigarray);
            extconfigarray = NULL;
        }   
        if (NULL != extconditionarray) {
            free(extconditionarray);
            extconditionarray = NULL;
        }
    }

    destroyPQExpBuffer(query);
}

/*
 * getDependencies --- obtain available dependency data
 */
static void getDependencies(Archive* fout)
{
    PQExpBuffer query;
    PGresult* res = NULL;
    int ntups = 0;
    int i = 0;
    int i_classid = 0;
    int i_objid = 0;
    int i_refclassid = 0;
    int i_refobjid = 0;
    int i_deptype = 0;
    DumpableObject* dobj = NULL;
    DumpableObject* refdobj = NULL;

    /* No dependency info available before 7.3 */
    if (fout->remoteVersion < 70300)
        return;

    if (g_verbose)
        write_msg(NULL, "reading dependency data\n");

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, "pg_catalog");

    query = createPQExpBuffer();

    /*
     * PIN dependencies aren't interesting, and EXTENSION dependencies were
     * already processed by getExtensionMembership.
     */
    appendPQExpBuffer(query,
        "SELECT "
        "classid, objid, refclassid, refobjid, deptype "
        "FROM pg_depend "
        "WHERE deptype != 'p' AND deptype != 'e' "
        "ORDER BY 1,2");

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    ntups = PQntuples(res);

    i_classid = PQfnumber(res, "classid");
    i_objid = PQfnumber(res, "objid");
    i_refclassid = PQfnumber(res, "refclassid");
    i_refobjid = PQfnumber(res, "refobjid");
    i_deptype = PQfnumber(res, "deptype");

    /*
     * Since we ordered the SELECT by referencing ID, we can expect that
     * multiple entries for the same object will appear together; this saves
     * on searches.
     */
    dobj = NULL;

    for (i = 0; i < ntups; i++) {
        CatalogId objId;
        CatalogId refobjId;
        char deptype;

        objId.tableoid = atooid(PQgetvalue(res, i, i_classid));
        objId.oid = atooid(PQgetvalue(res, i, i_objid));
        refobjId.tableoid = atooid(PQgetvalue(res, i, i_refclassid));
        refobjId.oid = atooid(PQgetvalue(res, i, i_refobjid));
        deptype = *(PQgetvalue(res, i, i_deptype));

        if (dobj == NULL || dobj->catId.tableoid != objId.tableoid || dobj->catId.oid != objId.oid)
            dobj = findObjectByCatalogId(objId);

        /*
         * Failure to find objects mentioned in pg_depend is not unexpected,
         * since for example we don't collect info about TOAST tables.
         */
        if (dobj == NULL) {
#ifdef NOT_USED
            write_stderr("no referencing object %u %u\n", objId.tableoid, objId.oid);
#endif
            continue;
        }

        refdobj = findObjectByCatalogId(refobjId);

        if (refdobj == NULL) {
#ifdef NOT_USED
            write_stderr("no referenced object %u %u\n", refobjId.tableoid, refobjId.oid);
#endif
            continue;
        }

        /*
         * Ordinarily, table rowtypes have implicit dependencies on their
         * tables.	However, for a composite type the implicit dependency goes
         * the other way in pg_depend; which is the right thing for DROP but
         * it doesn't produce the dependency ordering we need. So in that one
         * case, we reverse the direction of the dependency.
         */
        if (deptype == 'i' && dobj->objType == DO_TABLE && refdobj->objType == DO_TYPE)
            addObjectDependency(refdobj, dobj->dumpId);
        else
            /* normal case */
            addObjectDependency(dobj, refdobj->dumpId);
    }

    PQclear(res);

    destroyPQExpBuffer(query);
}

/*
 * createBoundaryObjects - create dummy DumpableObjects to represent
 * dump section boundaries.
 */
static DumpableObject* createBoundaryObjects(void)
{
    DumpableObject* dobjs = NULL;

    dobjs = (DumpableObject*)pg_malloc(2 * sizeof(DumpableObject));

    dobjs[0].objType = DO_PRE_DATA_BOUNDARY;
    dobjs[0].catId = nilCatalogId;
    AssignDumpId(dobjs + 0);
    dobjs[0].name = gs_strdup("PRE-DATA BOUNDARY");

    dobjs[1].objType = DO_POST_DATA_BOUNDARY;
    dobjs[1].catId = nilCatalogId;
    AssignDumpId(dobjs + 1);
    dobjs[1].name = gs_strdup("POST-DATA BOUNDARY");

    return dobjs;
}

/*
 * addBoundaryDependencies - add dependencies as needed to enforce the dump
 * section boundaries.
 */
static void addBoundaryDependencies(DumpableObject** dobjs, int numObjs, DumpableObject* boundaryObjs)
{
    DumpableObject* preDataBound = boundaryObjs + 0;
    DumpableObject* postDataBound = boundaryObjs + 1;
    int i;

    for (i = 0; i < numObjs; i++) {
        DumpableObject* dobj = dobjs[i];

        /*
         * We can ingore those objects which are in CSTORE_NAMESPACE becuase of internal objects
         */
        if (IsCStoreInternalObj(dobj))
            continue;

        /*
         * The classification of object types here must match the SECTION_xxx
         * values assigned during subsequent ArchiveEntry calls!
         */
        switch (dobj->objType) {
            case DO_NAMESPACE:
            case DO_EXTENSION:
            case DO_TYPE:
            case DO_SHELL_TYPE:
            case DO_FUNC:
            case DO_AGG:
            case DO_OPERATOR:
            case DO_ACCESS_METHOD:
            case DO_OPCLASS:
            case DO_OPFAMILY:
            case DO_COLLATION:
            case DO_CONVERSION:
            case DO_TABLE:
            case DO_ATTRDEF:
            case DO_PROCLANG:
            case DO_CAST:
            case DO_DUMMY_TYPE:
            case DO_TSPARSER:
            case DO_TSDICT:
            case DO_TSTEMPLATE:
            case DO_TSCONFIG:
            case DO_FDW:
            case DO_FOREIGN_SERVER:
            case DO_BLOB:
                /* Pre-data objects: must come before the pre-data boundary */
                addObjectDependency(preDataBound, dobj->dumpId);
                break;
            case DO_TABLE_DATA:
            case DO_BLOB_DATA:
            /*
             * This is more of a hack fix to break dependency loop due to untracked dependencies.
             * Consider move it back to pre-data catagory after dependencies are correctly handled.
             */
            case DO_PACKAGE:
                /* Data objects: must come between the boundaries */
                addObjectDependency(dobj, preDataBound->dumpId);
                addObjectDependency(postDataBound, dobj->dumpId);
                break;
            case DO_INDEX:
            case DO_TRIGGER:
            case DO_EVENT_TRIGGER:                
            case DO_DEFAULT_ACL:
            case DO_RLSPOLICY:
            case DO_PUBLICATION:
            case DO_PUBLICATION_REL:
            case DO_SUBSCRIPTION:
            case DO_EVENT:
                /* Post-data objects: must come after the post-data boundary */
                if (dobj->objType == DO_INDEX &&
                    ((IndxInfo*)dobj)->indextable && ((IndxInfo*)dobj)->indextable->isMOT) {
                    addObjectDependency(preDataBound, dobj->dumpId);
                } else {
                    addObjectDependency(dobj, postDataBound->dumpId);
                }
                break;
            case DO_RULE:
                /* Rules are post-data, but only if dumped separately */
                if (((RuleInfo*)dobj)->separate)
                    addObjectDependency(dobj, postDataBound->dumpId);
                break;
            case DO_CONSTRAINT:
            case DO_FK_CONSTRAINT:
            case DO_FTBL_CONSTRAINT:
                /* Constraints are post-data, but only if dumped separately */
                if (((ConstraintInfo*)dobj)->contable && ((ConstraintInfo*)dobj)->contable->isMOT) {
                    addObjectDependency(preDataBound, dobj->dumpId);
                } else if (((ConstraintInfo*)dobj)->separate) {
                    addObjectDependency(dobj, postDataBound->dumpId);
                }
                break;
            case DO_PRE_DATA_BOUNDARY:
                /* nothing to do */
                break;
            case DO_POST_DATA_BOUNDARY:
                /* must come after the pre-data boundary */
                addObjectDependency(dobj, preDataBound->dumpId);
                break;
        }
    }
}

/*
 * BuildArchiveDependencies - create dependency data for archive TOC entries
 *
 * The raw dependency data obtained by getDependencies() is not terribly
 * useful in an archive dump, because in many cases there are dependency
 * chains linking through objects that don't appear explicitly in the dump.
 * For example, a view will depend on its _RETURN rule while the _RETURN rule
 * will depend on other objects --- but the rule will not appear as a separate
 * object in the dump.  We need to adjust the view's dependencies to include
 * whatever the rule depends on that is included in the dump.
 *
 * Just to make things more complicated, there are also "special" dependencies
 * such as the dependency of a TABLE DATA item on its TABLE, which we must
 * not rearrange because pg_restore knows that TABLE DATA only depends on
 * its table.  In these cases we must leave the dependencies strictly as-is
 * even if they refer to not-to-be-dumped objects.
 *
 * To handle this, the convention is that "special" dependencies are created
 * during ArchiveEntry calls, and an archive TOC item that has any such
 * entries will not be touched here.  Otherwise, we recursively search the
 * DumpableObject data structures to build the correct dependencies for each
 * archive TOC item.
 */
static void BuildArchiveDependencies(Archive* fout)
{
    ArchiveHandle* AH = (ArchiveHandle*)fout;
    TocEntry* te = NULL;

    /* Scan all TOC entries in the archive */
    for (te = AH->toc->next; te != AH->toc; te = te->next) {
        DumpableObject* dobj = NULL;
        DumpId* dependencies = NULL;
        int nDeps;
        int allocDeps;

        /* No need to process entries that will not be dumped */
        if (te->reqs == 0)
            continue;
        /* Ignore entries that already have "special" dependencies */
        if (te->nDeps > 0)
            continue;
        /* Otherwise, look up the item's original DumpableObject, if any */
        dobj = findObjectByDumpId(te->dumpId);
        if (dobj == NULL)
            continue;
        /* No work if it has no dependencies */
        if (dobj->nDeps <= 0)
            continue;
        /* Set up work array */
        allocDeps = 64;
        dependencies = (DumpId*)pg_malloc(allocDeps * sizeof(DumpId));
        nDeps = 0;
        /* Recursively find all dumpable dependencies */
        findDumpableDependencies(AH, dobj, &dependencies, &nDeps, &allocDeps);
        /* And save 'em ... */
        if (nDeps > 0) {
            dependencies = (DumpId*)pg_realloc(dependencies, nDeps * sizeof(DumpId));
            te->dependencies = dependencies;
            te->nDeps = nDeps;
        } else {
            free(dependencies);
            dependencies = NULL;
        }
    }
}

/* Recursive search subroutine for BuildArchiveDependencies */
static void findDumpableDependencies(
    ArchiveHandle* AH, DumpableObject* dobj, DumpId** dependencies, int* nDeps, int* allocDeps)
{
    int i;

    /*
     * Ignore section boundary objects: if we search through them, we'll
     * report lots of bogus dependencies.
     */
    if (dobj->objType == DO_PRE_DATA_BOUNDARY || dobj->objType == DO_POST_DATA_BOUNDARY)
        return;

    for (i = 0; i < dobj->nDeps; i++) {
        DumpId depid = dobj->dependencies[i];

        if (TocIDRequired(AH, depid) != 0) {
            /* Object will be dumped, so just reference it as a dependency */
            if (*nDeps >= *allocDeps) {
                *allocDeps *= 2;
                *dependencies = (DumpId*)pg_realloc(*dependencies, *allocDeps * sizeof(DumpId));
            }
            (*dependencies)[*nDeps] = depid;
            (*nDeps)++;
        } else {
            /*
             * Object will not be dumped, so recursively consider its deps.
             * We rely on the assumption that sortDumpableObjects already
             * broke any dependency loops, else we might recurse infinitely.
             */
            DumpableObject* otherdobj = findObjectByDumpId(depid);

            if (otherdobj != NULL)
                findDumpableDependencies(AH, otherdobj, dependencies, nDeps, allocDeps);
        }
    }
}

/*
 * selectSourceSchema - make the specified schema the active search path
 * in the source database.
 *
 * NB: pg_catalog is explicitly searched after the specified schema;
 * so user names are only qualified if they are cross-schema references,
 * and system names are only qualified if they conflict with a user name
 * in the current schema.
 *
 * Whenever the selected schema is not pg_catalog, be careful to qualify
 * references to system catalogs and types in our emitted commands!
 */
static void selectSourceSchema(Archive* fout, const char* schemaName)
{
    static char* curSchemaName = NULL;
    PQExpBuffer query;

    /* Not relevant if fetching from pre-7.3 DB */
    if (fout->remoteVersion < 70300)
        return;
    /* Ignore null schema names */
    if (schemaName == NULL || *schemaName == '\0')
        return;
    /* Optimize away repeated selection of same schema */
    if ((curSchemaName != NULL) && strcmp(curSchemaName, schemaName) == 0)
        return;

    query = createPQExpBuffer();
    /*
     * It is invalid to set pg_temp or pg_catalog behind other schemas in search path explicitly.
     * The priority order is pg_temp, pg_catalog and other schemas.
     */
    appendPQExpBuffer(query, "SET search_path = %s", fmtId(schemaName));
    ExecuteSqlStatement(fout, query->data);

    destroyPQExpBuffer(query);
    if (curSchemaName != NULL) {
        free(curSchemaName);
        curSchemaName = NULL;
    }
    curSchemaName = gs_strdup(schemaName);
}

/*
 * getFormattedTypeName - retrieve a nicely-formatted type name for the
 * given type name.
 *
 * NB: in 7.3 and up the result may depend on the currently-selected
 * schema; this is why we don't try to cache the names.
 */
static char* getFormattedTypeName(Archive* fout, Oid oid, OidOptions opts)
{
    char* result = NULL;
    PQExpBuffer query;
    PGresult* res = NULL;

    if (oid == 0) {
        if ((opts & zeroAsOpaque) != 0)
            return gs_strdup(g_opaque_type);
        else if ((opts & zeroAsAny) != 0)
            return gs_strdup("'any'");
        else if ((opts & zeroAsStar) != 0)
            return gs_strdup("*");
        else if ((opts & zeroAsNone) != 0)
            return gs_strdup("NONE");
    }

    query = createPQExpBuffer();
    if (fout->remoteVersion >= 70300) {
        appendPQExpBuffer(query, "SELECT pg_catalog.format_type('%u'::pg_catalog.oid, NULL)", oid);
    } else if (fout->remoteVersion >= 70100) {
        appendPQExpBuffer(query, "SELECT pg_catalog.format_type('%u'::oid, NULL)", oid);
    } else {
        appendPQExpBuffer(query,
            "SELECT typname "
            "FROM pg_type "
            "WHERE oid = '%u'::oid",
            oid);
    }

    res = ExecuteSqlQueryForSingleRow(fout, query->data);

    if (fout->remoteVersion >= 70100) {
        /* already quoted */
        result = gs_strdup(PQgetvalue(res, 0, 0));
    } else {
        /* may need to quote it */
        result = gs_strdup(fmtId(PQgetvalue(res, 0, 0)));
    }

    PQclear(res);
    destroyPQExpBuffer(query);

    return result;
}

/*
 * myFormatType --- local implementation of format_type for use with 7.0.
 */
static char* myFormatType(const char* typname, int32 typmod)
{
    char* result = NULL;
    bool isarray = false;
    PQExpBuffer buf = createPQExpBuffer();

    /* Handle array types */
    if (typname[0] == '_') {
        isarray = true;
        typname++;
    }

    /* Show lengths on bpchar and varchar */
    if (strcmp(typname, "bpchar") == 0) {
        int len = (typmod - VARHDRSZ);

        appendPQExpBuffer(buf, "character");
        if (len > 1)
            appendPQExpBuffer(buf, "(%d)", typmod - VARHDRSZ);
    } else if (strcmp(typname, "varchar") == 0) {
        appendPQExpBuffer(buf, "character varying");
        if (typmod != -1)
            appendPQExpBuffer(buf, "(%d)", typmod - VARHDRSZ);
    } else if (strcmp(typname, "numeric") == 0) {
        appendPQExpBuffer(buf, "numeric");
        if (typmod != -1) {
            uint32 tmp_typmod;
            int precision;
            int scale;

            tmp_typmod = typmod - VARHDRSZ;
            precision = (tmp_typmod >> 16) & 0xffff;
            scale = tmp_typmod & 0xffff;
            appendPQExpBuffer(buf, "(%d,%d)", precision, scale);
        }
    }

    /*
     * char is an internal single-byte data type; Let's make sure we force it
     * through with quotes. - thomas 1998-12-13
     */
    else if (strcmp(typname, "char") == 0)
        appendPQExpBuffer(buf, "\"char\"");
    else
        appendPQExpBuffer(buf, "%s", fmtId(typname));

    /* Append array qualifier for array types */
    if (isarray)
        appendPQExpBuffer(buf, "[]");

    result = gs_strdup(buf->data);
    destroyPQExpBuffer(buf);

    return result;
}

/*
 * fmtQualifiedId - convert a qualified name to the proper format for
 * the source database.
 *
 * Like fmtId, use the result before calling again.
 */
static const char* fmtQualifiedId(Archive* fout, const char* schema, const char* id)
{
    static PQExpBuffer id_return = NULL;

    if (id_return != NULL) /* first time through? */
        resetPQExpBuffer(id_return);
    else
        id_return = createPQExpBuffer();

    /* Suppress schema name if fetching from pre-7.3 DB */
    if (fout->remoteVersion >= 70300 && (schema != NULL) && *schema) {
        appendPQExpBuffer(id_return, "%s.", fmtId(schema));
    }
    appendPQExpBuffer(id_return, "%s", fmtId(id));

    return id_return->data;
}

/*
 * Return a column list clause for the given relation.
 *
 * Special case: if there are no undropped columns in the relation, return
 * "", not an invalid "()" column list.
 */
static const char* fmtCopyColumnList(const TableInfo* ti)
{
    static PQExpBuffer q = NULL;
    int numatts = ti->numatts;
    char** attnames = ti->attnames;
    bool* attisdropped = ti->attisdropped;
    bool needComma = false;
    int i = 0;

    if (q != NULL) /* first time through? */
        resetPQExpBuffer(q);
    else
        q = createPQExpBuffer();

    appendPQExpBuffer(q, "(");

    for (i = 0; i < numatts; i++) {
        if (attisdropped[i]) {
            continue;
        }
        if (ti->attrdefs[i] != NULL && ti->attrdefs[i]->generatedCol) {
            continue;
        }
        if (needComma) {
            appendPQExpBuffer(q, ", ");
        }
        appendPQExpBuffer(q, "%s", fmtId(attnames[i]));
        needComma = true;
    }

    if (!needComma)
        return ""; /* no undropped columns */

    appendPQExpBuffer(q, ")");
    return q->data;
}

/*
 * Execute an SQL query and verify that we got exactly one row back.
 */
static PGresult* ExecuteSqlQueryForSingleRow(Archive* fout, const char* query)
{
    PGresult* res = NULL;
    int ntups;

    res = ExecuteSqlQuery(fout, query, PGRES_TUPLES_OK);

    /* Expecting a single result only */
    ntups = PQntuples(res);
    if (ntups != 1)
        exit_horribly(NULL,
            ngettext(
                "query returned %d row instead of one: %s\n", "query returned %d rows instead of one: %s\n", ntups),
            ntups,
            query);

    return res;
}

#ifdef DUMPSYSLOG
/*
 * @@GaussDB@@
 * Brief		: receive syslog from remote server
 * Description	:
 * Notes		:
 */
static void ReceiveSyslog(PGconn* conn, const char* current_path)
{
    PGresult* res = NULL;
    char filename[MAXPGPATH] = {0};
    int64 current_len_left = 0;
    int64 current_padding = 0;
    char* copybuf = NULL;
    FILE* file = NULL;
    int nRet = 0;

    if (NULL == conn) {
        write_stderr(_("%s: receive syslog failed because of internal error: %s"), progname, PQerrorMessage(conn));
        disconnect_and_exit(conn, 1);
    }

    if (NULL == current_path) {
        current_path = ".";
    }

    /*
     * Get the COPY data
     */
    res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_COPY_BOTH) {
        write_stderr(_("%s: could not get COPY data stream: %s"), progname, PQerrorMessage(conn));
        disconnect_and_exit(conn, 1);
    }

    while (true) {
        int r;
        if (copybuf != NULL) {
            PQfreemem(copybuf);
            copybuf = NULL;
        }
        r = PQgetCopyData(conn, &copybuf, 0);
        if (r == -1) {
            /*
             * End of chunk
             */
            if (file)
                fclose(file);
            break;
        } else if (r == -2) {
            write_stderr(_(stderr, _("\n")));
            write_stderr(_("%s: could not read COPY data: %s"), progname, PQerrorMessage(conn));
            PQclear(res);
            disconnect_and_exit(conn, 1);
        }

        if (NULL == file) {
            int filemode;

            /*
             * No current file, so this must be the header for a new file
             */
            if (r != MAXPGPATH * 2 + 512) {
                write_stderr(_("%s: invalid tar block header size: %d\n"), progname, r);
                PQclear(res);
                disconnect_and_exit(conn, 1);
            }

            if (sscanf_s(copybuf + 1048, "%20lo", &current_len_left) != 1) {
                write_stderr(_("%s: could not parse file size\n"), progname);
                disconnect_and_exit(conn, 1);
            }

            /* Set permissions on the file */
            if (sscanf_s(&copybuf[1024], "%07o ", &filemode) != 1) {
                write_stderr(_("%s: could not parse file mode\n"), progname);
                PQclear(res);
                disconnect_and_exit(conn, 1);
            }

            /*
             * All files are padded up to 512 bytes
             */
            current_padding = ((current_len_left + 511) & ~511) - current_len_left;

            /*
             * First part of header is zero terminated filename
             */
            nRet = snprintf_s(filename, sizeof(filename), sizeof(filename) - 1, "%s/%s", current_path, copybuf);
            securec_check_ss_c(nRet, "\0", "\0");
            filename[MAXPGPATH - 1] = '\0';

            file = fopen(filename, "wb");
            if (!file) {
                write_stderr(_("%s: could not create file \"%s\": %s\n"), progname, filename, gs_strerror(errno));
                PQclear(res);
                disconnect_and_exit(conn, 1);
            }

#ifndef WIN32
            if (chmod(filename, (mode_t)filemode))
                write_stderr(
                    _("%s: could not set permissions on file \"%s\": %s\n"), progname, filename, gs_strerror(errno));
#endif

            if (0 == current_len_left) {
                /*
                 * Done with this file, next one will be a new tar header
                 */
                if (file) {
                    fclose(file);
                    file = NULL;
                    continue;
                }
            }
        } else {
            /*
             * Continuing blocks in existing file
             */
            if (0 == current_len_left && r == current_padding) {
                fclose(file);
                file = NULL;
                continue;
            }

            if (fwrite(copybuf, r, 1, file) != 1) {
                write_stderr(_("%s: could not write to file \"%s\": %s\n"), progname, filename, gs_strerror(errno));
                PQclear(res);
                disconnect_and_exit(conn, 1);
            }

            current_len_left -= r;
            if (0 == current_len_left && 0 == current_padding) {
                fclose(file);
                file = NULL;
                continue;
            }
        }
    }

    if (file != NULL) {
        write_stderr(_("%s: COPY stream ended before last file was finished\n"), progname);
        PQclear(res);
        disconnect_and_exit(conn, 1);
    }

    if (copybuf != NULL)
        PQfreemem(copybuf);
    PQclear(res);
    PQfinish(conn);
}

#endif
/*
 * Check whether the specified column of the table is writing in string or not
 * by checking the limited typoid that partition supported. Notes: if the caller
 * is interval partition, please process the format of the column value.
 */
static bool isTypeString(const TableInfo* tbinfo, unsigned int colum)
{
    switch (tbinfo->typid[colum - 1]) {
        case INT2OID:
        case INT4OID:
        case INT8OID:
        case FLOAT4OID:
        case FLOAT8OID:
        case NUMERICOID:
            /* Here we ignore infinity and NaN */
            return false;
        default:
            /* All other types are regarded as string. */
            return true;
    }
}

static void find_current_connection_node_type(Archive* fout)
{
    PGresult* res = NULL;
    char* value = NULL;
    res = ExecuteSqlQuery(fout,
        "SELECT n.node_type "
        "FROM pg_catalog.pg_settings s, pg_catalog.pgxc_node n "
        "WHERE s.name = 'pgxc_node_name' "
        "AND n.node_name=s.setting",
        PGRES_TUPLES_OK);
    if (PQntuples(res) > 0) {
        value = PQgetvalue(res, 0, 0);
        if (value != NULL && (value[0] == PGXC_NODE_COORDINATOR || value[0] == PGXC_NODE_DATANODE)) {
            connected_node_type = value[0];
        }
    }

    PQclear(res);

    if (connected_node_type == PGXC_NODE_COORDINATOR) {
        res = ExecuteSqlQueryForSingleRow(fout,
            "SELECT pg_catalog.string_agg(n.node_name,',') AS pgxc_node_names "
            "FROM pg_catalog.pgxc_node n "
            "WHERE n.node_type = 'D'");
        if (!PQgetisnull(res, 0, 0)) {
            all_data_nodename_list = gs_strdup(PQgetvalue(res, 0, 0));
        }

        PQclear(res);
    }
}

static bool IsPackageObject(Archive* fout, Oid classid, Oid objid)
{
    PGresult* res = NULL;
    PQExpBuffer query = createPQExpBuffer();
    const int gs_package_oid = 7815;
    int colNum = 0;
    int tupNum = 0;
    int count;

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, "pg_catalog");

    appendPQExpBuffer(query,
        "SELECT pg_catalog.count(*) from pg_depend where classid = %d "
        "and objid = %d and refclassid = %d and deptype = 'a'",
        classid, objid, gs_package_oid);

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    colNum = PQfnumber(res, "count");
    count = atoi(PQgetvalue(res, tupNum, colNum));
    PQclear(res);
    destroyPQExpBuffer(query);
    return count > 0;
}

/*
 * getEventTriggers
 *   get information about event triggers
 */
EventTriggerInfo *
getEventTriggers(Archive *fout, int *numEventTriggers)
{
    /* Before 9.3, there are no event triggers */
    if (GetVersionNum(fout) < EVENT_TRIGGER_VERSION_NUM) {
        *numEventTriggers = 0;
        return NULL;
    }

    int i;
    PQExpBuffer query = createPQExpBuffer();
    PGresult   *res;
    EventTriggerInfo *evtinfo;
    int i_tableoid;
    int i_oid;
    int i_evtname;
    int i_evtevent;
    int i_evtowner;
    int i_evttags;
    int i_evtfname;
    int i_evtenabled;
    int ntups;
 
    /* Make sure we are in proper schema */
    selectSourceSchema(fout, "pg_catalog");
 
    appendPQExpBuffer(query,
                      "SELECT e.tableoid, e.oid, evtname, evtenabled, "
                      "evtevent, (%s evtowner) AS evtowner, "
                      "array_to_string(array("
                      "select quote_literal(x) "
                      " from unnest(evttags) as t(x)), ', ') as evttags, "
                      "e.evtfoid::regproc as evtfname "
                      "FROM pg_event_trigger e "
                      "ORDER BY e.oid",
                      username_subquery);
 
    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);
    *numEventTriggers = ntups;

    if (ntups == 0) {
        PQclear(res);
        destroyPQExpBuffer(query);
        return NULL;
    }

    evtinfo = (EventTriggerInfo *) pg_malloc(ntups * sizeof(EventTriggerInfo));
 
    i_tableoid = PQfnumber(res, "tableoid");
    i_oid = PQfnumber(res, "oid");
    i_evtname = PQfnumber(res, "evtname");
    i_evtevent = PQfnumber(res, "evtevent");
    i_evtowner = PQfnumber(res, "evtowner");
    i_evttags = PQfnumber(res, "evttags");
    i_evtfname = PQfnumber(res, "evtfname");
    i_evtenabled = PQfnumber(res, "evtenabled");
 
    for (i = 0; i < ntups; i++) {
        evtinfo[i].dobj.objType = DO_EVENT_TRIGGER;
        evtinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        evtinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&evtinfo[i].dobj);
        evtinfo[i].dobj.name = gs_strdup(PQgetvalue(res, i, i_evtname));
        evtinfo[i].evtname = gs_strdup(PQgetvalue(res, i, i_evtname));
        evtinfo[i].evtevent = gs_strdup(PQgetvalue(res, i, i_evtevent));
        evtinfo[i].evtowner = gs_strdup(PQgetvalue(res, i, i_evtowner));
        evtinfo[i].evttags = gs_strdup(PQgetvalue(res, i, i_evttags));
        evtinfo[i].evtfname = gs_strdup(PQgetvalue(res, i, i_evtfname));
        evtinfo[i].evtenabled = *(PQgetvalue(res, i, i_evtenabled));
    }
 
    PQclear(res);
    destroyPQExpBuffer(query);
    return evtinfo;
}

static bool eventtrigger_filter(EventTriggerInfo *evtinfo)
{
    static char *reserved_trigger_prefix[] = {PUB_EVENT_TRIG_PREFIX PUB_TRIG_DDL_CMD_END,
                                              PUB_EVENT_TRIG_PREFIX PUB_TRIG_DDL_CMD_START};
    static const size_t triggerPrefixLength = sizeof(reserved_trigger_prefix) / sizeof(reserved_trigger_prefix[0]);

    for (size_t i = 0; i < triggerPrefixLength; ++i) {
        if (!strncmp(evtinfo->dobj.name, reserved_trigger_prefix[i], strlen(reserved_trigger_prefix[i]))) {
            return false;
        }
    }
    return true;
}

static void dumpEventTrigger(Archive *fout, EventTriggerInfo *evtinfo)
{
    PQExpBuffer query;
    PQExpBuffer labelq;
 
    /* Skip if not to be dumped */
    if (!evtinfo->dobj.dump || dataOnly)
        return;

    if (!eventtrigger_filter(evtinfo))
        return;

    query = createPQExpBuffer();
    labelq = createPQExpBuffer();
 
    appendPQExpBuffer(query, "CREATE EVENT TRIGGER ");
    appendPQExpBufferStr(query, fmtId(evtinfo->dobj.name));
    appendPQExpBuffer(query, " ON ");
    appendPQExpBufferStr(query, fmtId(evtinfo->evtevent));
    appendPQExpBufferStr(query, " ");
 
    if (strcmp("", evtinfo->evttags) != 0) {
        appendPQExpBufferStr(query, "\n         WHEN TAG IN (");
        appendPQExpBufferStr(query, evtinfo->evttags);
        appendPQExpBufferStr(query, ") ");
    }
 
    appendPQExpBuffer(query, "\n   EXECUTE PROCEDURE ");
    appendPQExpBufferStr(query, evtinfo->evtfname);
    appendPQExpBuffer(query, "();\n");
 
    if (evtinfo->evtenabled != 'O') {
        appendPQExpBuffer(query, "\nALTER EVENT TRIGGER %s ",
                          fmtId(evtinfo->dobj.name));
        switch (evtinfo->evtenabled) {
            case 'D':
                appendPQExpBuffer(query, "DISABLE");
                break;
            case 'A':
                appendPQExpBuffer(query, "ENABLE ALWAYS");
                break;
            case 'R':
                appendPQExpBuffer(query, "ENABLE REPLICA");
                break;
            default:
                appendPQExpBuffer(query, "ENABLE");
                break;
        }
        appendPQExpBuffer(query, ";\n");
    }
    appendPQExpBuffer(labelq, "EVENT TRIGGER %s ",
                      fmtId(evtinfo->dobj.name));
 
    ArchiveEntry(fout, evtinfo->dobj.catId, evtinfo->dobj.dumpId,
                 evtinfo->dobj.name, NULL, NULL, evtinfo->evtowner, false,
                 "EVENT TRIGGER", SECTION_POST_DATA,
                 query->data, "", NULL, NULL, 0, NULL, NULL);
 
    dumpComment(fout, labelq->data, NULL, NULL,evtinfo->dobj.catId, 0, evtinfo->dobj.dumpId);
 
    destroyPQExpBuffer(query);
    destroyPQExpBuffer(labelq);
}

static void dumpUniquePrimaryDef(PQExpBuffer buf, ConstraintInfo* coninfo, IndxInfo* indxinfo,  bool isBcompatibility)
{
    appendPQExpBuffer(buf, "%s ", coninfo->contype == 'p' ? "PRIMARY KEY" : "UNIQUE");

    const char* usingpos = NULL;

    if (isBcompatibility) {
        usingpos = strstr(indxinfo->indexdef, " USING ");
        if (usingpos != NULL) {
            usingpos += USING_STR_OFFSET;

            const char* endpos = NULL;
            endpos = strstr(usingpos, " ");
            uint32 posoffset = 0;
            if (endpos != NULL) {
                posoffset = (uint32)(endpos - usingpos);
            }

            char idxopt[posoffset + 1] = {0};
            /* Write the storage mode in the condef. */
            errno_t rc = memcpy_s(idxopt, posoffset + 1, usingpos, posoffset);
            securec_check_c(rc, "\0", "\0");
            appendPQExpBuffer(buf, "USING %s", idxopt);
        }
    }

    appendPQExpBuffer(buf, "%s", coninfo->contype == 'p' ?
             coninfo->condef + PRIMARY_KEY_OFFSET : coninfo->condef + UNIQUE_OFFSET);

    if ((indxinfo->options != NULL) && strlen(indxinfo->options) > 0)
        appendPQExpBuffer(buf, " WITH (%s)", indxinfo->options);

    if (!indxinfo->indisvisible) {
        appendPQExpBuffer(buf, " INVISIBLE");
    }

    if (coninfo->condeferrable) {
        appendPQExpBuffer(buf, " DEFERRABLE");
        if (coninfo->condeferred)
            appendPQExpBuffer(buf, " INITIALLY DEFERRED");
    }
}
static void dumpTableAutoIncrement(Archive* fout, PQExpBuffer sqlbuf, TableInfo* tbinfo)
{
    PGresult* res = NULL;
    PQExpBuffer query = createPQExpBuffer();

    /* Make sure we are in proper schema */
    selectSourceSchema(fout, tbinfo->dobj.nmspace->dobj.name);
    /* Obtain the sequence name of the auto_increment column from the pg_attrdef. */
    appendPQExpBuffer(query, "select nspname, relname from "
                             "pg_class c left join pg_namespace n on c.relnamespace = n.oid "
                             "left join  pg_depend d on c.oid = d.objid "
                             "where classid = %u and deptype = '%c' and refobjid = %u and refobjsubid = %u "
                             "and c.relkind in ('%c', '%c')"
                      ,RelationRelationId, DEPENDENCY_AUTO, tbinfo->dobj.catId.oid, tbinfo->autoinc_attnum
                      ,RELKIND_SEQUENCE, RELKIND_LARGE_SEQUENCE);

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    if (PQntuples(res) != 1) {
        write_msg(NULL,
            ngettext("query to get auto_increment sequence name of %s returned %d row (expected 1)\n",
                "query to get auto_increment sequence name of %s returned %d row (expected 1)\n",
                PQntuples(res)), tbinfo->dobj.name, PQntuples(res));
        PQclear(res);
        destroyPQExpBuffer(query);
        return;
    }
    destroyPQExpBuffer(query);
    /* Set the sequence name. */
    tbinfo->autoinc_seqname = gs_strdup(PQgetvalue(res, 0, 1));
    if (tbinfo->relpersistence == RELPERSISTENCE_GLOBAL_TEMP ||
        (tbinfo->relpersistence == RELPERSISTENCE_UNLOGGED && no_unlogged_table_data)) {
        PQclear(res);
        return;
    }
    /* Get the sequence value. */
    query = createPQExpBuffer();
    appendPQExpBuffer(query,
        "SELECT CASE WHEN is_called AND last_value < max_value THEN last_value + 1 "
        "     WHEN is_called AND last_value >= max_value THEN max_value "
        "     ELSE last_value END AS last_value FROM %s",
        fmtQualifiedId(fout, PQgetvalue(res, 0, 0), PQgetvalue(res, 0, 1)));
    PQclear(res);

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    if (PQntuples(res) != 1) {
        write_msg(NULL,
            ngettext("query to get auto_increment sequence value of %s returned %d row (expected 1)\n",
                "query to get auto_increment sequence value of %s returned %d row (expected 1)\n",
                PQntuples(res)), tbinfo->dobj.name, PQntuples(res));
    } else { /* Print it as auto_increment. */
        appendPQExpBuffer(sqlbuf, " AUTO_INCREMENT = %s ", PQgetvalue(res, 0, 0));
    }

    PQclear(res);
    destroyPQExpBuffer(query);
}

static bool IsPlainFormat()
{
	return pg_strcasecmp(format, "plain") == 0 || pg_strcasecmp(format, "p") == 0;
}

static bool needIgnoreSequence(TableInfo* tbinfo)
{
    if (OidIsValid(tbinfo->owning_tab)) {
        TableInfo* owning_tab = findTableByOid(tbinfo->owning_tab);
        if (NULL != owning_tab &&
            owning_tab->dobj.dump &&
            owning_tab->autoinc_attnum > 0 &&
            owning_tab->autoinc_attnum == tbinfo->owning_col) {
            if (owning_tab->autoinc_seqname &&
                strcmp(owning_tab->autoinc_seqname, tbinfo->dobj.name) == 0) {
                return true;
            }
        }
    }
    return false;
}

/*
 * Print a progress report based on the global variables.
 * Execute this function in another thread and print the progress periodically.
 */
static void *ProgressReportDump(void *arg)
{
#if defined(USE_ASSERT_CHECKING) || defined(FASTCHECK)
    if (disable_progress) {
        return nullptr;
    }
#endif
    if (g_totalObjNums == 0) {
        return nullptr;
    }
    char progressBar[53];
    int percent;
    do {
        /* progress report */
        percent = (int)(100 * g_dumpObjNums / g_totalObjNums);
        GenerateProgressBar(percent, progressBar);
        fprintf(stdout, "Progress: %s %d%% (%d/%d, dumpObjNums/totalObjNums). dump objects \r",
            progressBar, percent, g_dumpObjNums, g_totalObjNums);
        pthread_mutex_lock(&g_mutex);
        timespec timeout;
        timeval now;
        gettimeofday(&now, nullptr);
        timeout.tv_sec = now.tv_sec + 1;
        timeout.tv_nsec = 0;
        int ret = pthread_cond_timedwait(&g_condDump, &g_mutex, &timeout);
        pthread_mutex_unlock(&g_mutex);
        if (ret == ETIMEDOUT) {
            continue;
        } else {
            break;
        }
    } while ((g_dumpObjNums < g_totalObjNums) && !g_progressFlagDump);
    percent = 100;
    GenerateProgressBar(percent, progressBar);
    fprintf(stdout, "Progress: %s %d%% (%d/%d, dumpObjNums/totalObjNums). dump objects \n",
            progressBar, percent, g_dumpObjNums, g_totalObjNums);
    return nullptr;
}

static void *ProgressReportScanDatabase(void *arg)
{
#if defined(USE_ASSERT_CHECKING) || defined(FASTCHECK)
    if (disable_progress) {
        return nullptr;
    }
#endif
    char progressBar[53];
    int percent;
    do {
        /* progress report */
        percent = (int)(g_curStep * 100 / g_totalSteps);
        GenerateProgressBar(percent, progressBar);
        fprintf(stdout, "Progress: %s %d%% (%d/%d, cur_step/total_step). %s \r",
            progressBar, percent, g_curStep, g_totalSteps, g_progressDetails[g_curStep]);
        pthread_mutex_lock(&g_mutex);
        timespec timeout;
        timeval now;
        gettimeofday(&now, nullptr);
        timeout.tv_sec = now.tv_sec + 1;
        timeout.tv_nsec = 0;
        int ret = pthread_cond_timedwait(&g_cond, &g_mutex, &timeout);
        pthread_mutex_unlock(&g_mutex);
        if (ret == ETIMEDOUT) {
            continue;
        } else {
            break;
        }
    } while ((g_curStep < g_totalSteps) && !g_progressFlagScan);
    percent = 100;
    GenerateProgressBar(percent, progressBar);
    fprintf(stdout, "Progress: %s %d%% (%d/%d, cur_step/total_step). finish scanning database                       \n",
            progressBar, percent, g_curStep, g_totalSteps);
    return nullptr;
}

bool FuncExists(Archive* fout, const char* funcNamespace, const char* funcName)
{
    char query[300];

    bool exist = false;
    ArchiveHandle* AH = (ArchiveHandle*)fout;
    sprintf(query, "SELECT * FROM pg_proc a LEFT JOIN pg_namespace b on a.pronamespace=b.oid WHERE a.proname='%s' and b.nspname='%s'", funcName, funcNamespace);
    
    exist = isExistsSQLResult(AH->connection, query);
    return exist;
}

bool TabExists(Archive* fout, const char* schemaName, const char* tabName)
{
    char query[300];
    bool exist = false;
    ArchiveHandle* AH = (ArchiveHandle*)fout;

    sprintf(query, "SELECT * FROM pg_tables  WHERE schemaname='%s' and tablename='%s'", schemaName, tabName);
    
    exist = isExistsSQLResult(AH->connection, query);
    return exist;
}
