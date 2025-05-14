#include <cstdlib>
#include "c.h"
#include "postgres.h"
#include "gs_threadlocal.h"
#include "parser/analyze.h"
#include "executor/executor.h"
#include "query_anomaly/query_anomaly_labels.h"

#define MAX_NODE_TYPE_COUNT ((int)T_CharsetClause + 1)
constexpr int LARGE_OPERATION_ROW_NUMBER = 1000;
constexpr int MAX_EREPORT_QUERY_LEN = 1000;

enum class StatementType {
    OTHER_STMT_TYPE,
    DDL_CHANGE_STMT_TYPE,
    PERMISSION_CHANGE_STMT_TYPE,
    FILE_OPERATION_STMT_TYPE,
    TRUNCATE_STMT_TYPE
};

static post_parse_analyze_hook_type pre_anomaly_detection_analyze_hook = NULL;
static ExecutorFinish_hook_type pre_anomaly_detection_executor_finish = NULL;

pthread_mutex_t g_command_type_per_node_type_lock = PTHREAD_MUTEX_INITIALIZER;
static bool g_command_type_per_node_type_initialized = false;
static StatementType g_command_type_per_node_type[MAX_NODE_TYPE_COUNT];

/*
 * @brief Checks if to implement query anomaly
 */
bool check_if_to_implement_anomaly()
{
    if (u_sess->proc_cxt.IsInnerMaintenanceTools) {
        return false;  // No need to handle internal kernel queries
    }
    if (u_sess->exec_cxt.nesting_level > 0) {
        return false; // No need to handle triggers and functions
    }
    return true;
}

/**
 * Identify node type as DCL (create) command
 *
 * @param value node type
 * @return true if DCL else false
 */
static bool is_node_type_ddl_create(int value)
{
    switch (value) {
        case T_CreateStmt:
        case T_CreateTableAsStmt:
        case T_CreateForeignTableStmt:
        case T_CreateSchemaStmt:
        case T_CreateTableSpaceStmt:
        case T_CreateEventStmt:
        case T_CreateEnumStmt:
        case T_CreateFunctionStmt:
        case T_CreatePackageStmt:
        case T_CreatePackageBodyStmt:
        case T_CreatedbStmt:
        case T_CreateTrigStmt:
        case T_CreateResourcePoolStmt:
        case T_CreateWorkloadGroupStmt:
        case T_CreateAppWorkloadGroupMappingStmt:
        case T_CreateForeignServerStmt:
        case T_CreateDataSourceStmt:
        case T_CreateDirectoryStmt:
        case T_CreateSynonymStmt:
        case T_CreateSeqStmt:
        case T_CreateModelStmt:
        case T_CreatePublicationStmt:
        case T_CreateSubscriptionStmt:
        case T_CreateFdwStmt:
        case T_ViewStmt:
            return true;
        default:
            return false;
    }
}

/**
 * Identify node type as DCL (alter) command
 *
 * @param value node type
 * @return true if DCL else false
 */
static bool is_node_type_ddl_alter(int value)
{
    switch (value) {
        case T_AlterTableStmt:
        case T_AlterTableSpaceOptionsStmt:
        case T_AlterEventStmt:
        case T_AlterEnumStmt:
        case T_AlterFunctionStmt:
        case T_AlterDatabaseStmt:
        case T_AlterDatabaseSetStmt:
        case T_AlterDefaultPrivilegesStmt:
        case T_AlterObjectSchemaStmt:
        case T_AlterResourcePoolStmt:
        case T_AlterGlobalConfigStmt:
        case T_AlterWorkloadGroupStmt:
        case T_AlterAppWorkloadGroupMappingStmt:
        case T_AlterForeignServerStmt:
        case T_AlterDataSourceStmt:
        case T_AlterTSDictionaryStmt:
        case T_AlterTSConfigurationStmt:
        case T_AlterSeqStmt:
        case T_AlterPublicationStmt:
        case T_AlterSubscriptionStmt:
        case T_AlterFdwStmt:
            return true;
        default:
            return false;
    }
}

/**
 * Identify node type as DCL (drop) command
 *
 * @param value node type
 * @return true if DCL else false
 */
static bool is_node_type_ddl_drop(int value)
{
    switch (value) {
        case T_DropOwnedStmt:
        case T_DropTableSpaceStmt:
        case T_DropEventStmt:
        case T_DropdbStmt:
        case T_DropStmt:
        case T_DropResourcePoolStmt:
        case T_DropGlobalConfigStmt:
        case T_DropWorkloadGroupStmt:
        case T_DropAppWorkloadGroupMappingStmt:
        case T_DropDirectoryStmt:
        case T_DropSubscriptionStmt:
            return true;
        default:
            return false;
    }
}

/**
 * Identify node type as DCL (other) command
 *
 * @param value node type
 * @return true if DCL else false
 */
static bool is_node_type_ddl_other(int value)
{
    switch (value) {
        case T_IndexStmt:
        case T_ViewStmt:
        case T_CompositeTypeStmt:
        case T_CompileStmt:
        case T_RenameStmt:
        case T_ReindexStmt:
        case T_DefineStmt:
        case T_PurgeStmt:
            return true;
        default:
            return false;
    }
}

/**
 * Identify node type as DDL command
 *
 * @param value node type
 * @return true if DDL else false
 */
static bool is_node_type_ddl(int value)
{
    return (is_node_type_ddl_create(value) || is_node_type_ddl_alter(value) || is_node_type_ddl_drop(value) ||
            is_node_type_ddl_other(value));
}
/**
 * Identify node type as file operation
 *
 * @param value node type
 * @return true if DDL else false
 */
static bool is_node_type_file_operation(int value)
{
    switch (value) {
        case T_CopyStmt:
        case T_CopyColExpr:
            return true;
        default:
            return false;
    }
}

/**
 * Identify node type as DCL command
 *
 * @param value node type
 * @return true if DCL else false
 */
static bool is_node_type_dcl(int value)
{
    switch (value) {
        case T_CreateUserMappingStmt:
        case T_AlterUserMappingStmt:
        case T_DropUserMappingStmt:
        case T_CreateRoleStmt:
        case T_AlterRoleStmt:
        case T_AlterRoleSetStmt:
        case T_DropRoleStmt:
        case T_ReassignOwnedStmt:
        case T_GrantStmt:
        case T_GrantRoleStmt:
        case T_GrantDbStmt:
        case T_SecLabelStmt:
        case T_AlterOwnerStmt:
        case T_CreateGroupStmt:
        case T_AlterGroupStmt:
        case T_DropGroupStmt:
        case T_CreateRlsPolicyStmt:
        case T_AlterRlsPolicyStmt:
        case T_CreateWeakPasswordDictionaryStmt:
        case T_DropWeakPasswordDictionaryStmt:
        case T_TimeCapsuleStmt:
            return true;
        default:
            return false;
    }
}

/**
 * Initialize g_command_type_per_node_type.
 */
void init_anomaly_detection_check_rules()
{
    if (likely(g_command_type_per_node_type_initialized)) {
        return;
    }
    pthread_mutex_lock(&g_command_type_per_node_type_lock);
    if (g_command_type_per_node_type_initialized) {
        pthread_mutex_unlock(&g_command_type_per_node_type_lock);
        return;
    }
    for (int index = 0; index < MAX_NODE_TYPE_COUNT; ++index) {
        if (is_node_type_ddl(index)) {
            g_command_type_per_node_type[index] = StatementType::DDL_CHANGE_STMT_TYPE;
        } else if (is_node_type_dcl(index)) {
            g_command_type_per_node_type[index] = StatementType::PERMISSION_CHANGE_STMT_TYPE;
        } else if (is_node_type_file_operation(index)) {
            g_command_type_per_node_type[index] = StatementType::FILE_OPERATION_STMT_TYPE;
        } else if (index == T_TruncateStmt) {
            g_command_type_per_node_type[index] = StatementType::TRUNCATE_STMT_TYPE;
        } else {
            g_command_type_per_node_type[index] = StatementType::OTHER_STMT_TYPE;
        }
    }
    g_command_type_per_node_type_initialized = true;
    pthread_mutex_unlock(&g_command_type_per_node_type_lock);
}

/**
 * Sends report about "risky query"
 *
 * @param msg_prefix prefix of report message
 * @param source_query original query
 * @param affected_rows number of rows affected by the query, or other operation led here.
 */
static void report_risky_query_error(const char* msg_prefix, const char* source_query, size_t affected_rows)
{
    if (strlen(source_query) <= MAX_EREPORT_QUERY_LEN) {
        ereport(LOG, (errmsg("%s. Affected rows: %lu. Query: %s", msg_prefix, affected_rows, source_query)));
    } else {
        char shortened_query[MAX_EREPORT_QUERY_LEN] = {0};
        errno_t is_ok = strncpy_s(shortened_query, sizeof(shortened_query), source_query, sizeof(shortened_query) - 1);
        securec_check(is_ok, "\0", "\0");
        ereport(LOG, (errmsg("%s. Affected rows: %lu. Query: %s...", msg_prefix, affected_rows, shortened_query)));
    }
}

/**
 * Sends report about "risky query"
 *
 * @param msg_prefix prefix of report message
 * @param source_query original query
 */
 static void report_risky_query_error(const char* msg_prefix, const char* source_query)
{
    char* mask_string = nullptr;
    MASK_PASSWORD_START(mask_string, source_query);
    if (strlen(mask_string) <= MAX_EREPORT_QUERY_LEN) {
        ereport(LOG, (errmsg("%s. Query: %s", msg_prefix, mask_string)));
    } else {
        char shortened_query[MAX_EREPORT_QUERY_LEN] = {0};
        errno_t is_ok = strncpy_s(shortened_query, sizeof(shortened_query), mask_string, sizeof(shortened_query) - 1);
        securec_check(is_ok, "\0", "\0");
        ereport(LOG, (errmsg("%s. Query: %s...", msg_prefix, shortened_query)));
    }
    MASK_PASSWORD_END(mask_string, source_query);
}


/*
 * Logs the number of affected rows at the end of query execution.
 * @param [in] queryDesc
 */
void anomaly_detection_executor_finish_hook(QueryDesc *query_desc)
{
    if (pre_anomaly_detection_executor_finish != NULL) {
        pre_anomaly_detection_executor_finish(query_desc);
    }
    if (!u_sess->attr.attr_security.enable_risky_query_detection) {
        return;
    }
    if (query_desc == NULL || query_desc->sourceText == NULL) {
        return;
    }
    if (!check_if_to_implement_anomaly()) {
        return;
    }
    CmdType operation = query_desc->operation;
    if (query_desc->estate != NULL) {
        if (query_desc->estate->es_processed > LARGE_OPERATION_ROW_NUMBER) {
            switch (operation) {
                case CMD_SELECT:
                    report_risky_query_error("Risky query large select", query_desc->sourceText,
                        query_desc->estate->es_processed);
                    break;
                case CMD_UPDATE:
                    report_risky_query_error("Risky query large update", query_desc->sourceText,
                        query_desc->estate->es_processed);
                    break;
                case CMD_DELETE:
                    report_risky_query_error("Risky query large delete", query_desc->sourceText,
                        query_desc->estate->es_processed);
                    break;
                default:  // should not do here anything
                    ;
            }
        }
    }
}

/*
 * @brief checks the statament where caluse and alert if needed
 * @param [in] pstate
 * @param [in] query
 */
static void check_where_clause(ParseState *pstate, Query *query)
{
    CmdType target_operation = query->commandType;
    if (target_operation == CMD_UPDATE) {
        if (query->jointree != NULL && query->jointree->quals == NULL) {
            report_risky_query_error("Risky query update with no 'where clause'", pstate->p_sourcetext);
        }
    } else if (target_operation == CMD_DELETE) {
        if (query->jointree != NULL && query->jointree->quals == NULL) {
            report_risky_query_error("Risky query delete with no 'where clause'", pstate->p_sourcetext);
        }
    }
}

/**
 * @brief check if the statement is a DDL, file operation, permission operation or truncate operation.
 * @param [in] pstate
 * @param [in] query
 */
static void check_utility_operation(ParseState *pstate, Query *query)
{
    Node *utility_statement = query->utilityStmt;
    if (utility_statement == NULL || utility_statement->type >= MAX_NODE_TYPE_COUNT) {
        return;
    }
    switch (g_command_type_per_node_type[utility_statement->type]) {
        case StatementType::DDL_CHANGE_STMT_TYPE:
            report_risky_query_error("Risky query schema change (DDL)", pstate->p_sourcetext);
            break;
        case StatementType::FILE_OPERATION_STMT_TYPE:
            report_risky_query_error("Risky query file operation", pstate->p_sourcetext);
            break;
        case StatementType::TRUNCATE_STMT_TYPE:
            report_risky_query_error("Risky query truncate operation", pstate->p_sourcetext);
            break;
        case StatementType::PERMISSION_CHANGE_STMT_TYPE:
            report_risky_query_error("Risky query permission change", pstate->p_sourcetext);
            break;
        default:
            break;
    }
}

/*
 * @brief checks if the statament access atable with resource lable and alert if needed
 * @param [in] pstate
 * @param [in] query
 */
static void check_resource_label(ParseState *pstate, Query *query)
{
    List *tables_oid_list = NULL;
    ListCell *lc;
    foreach (lc, query->rtable) {
        RangeTblEntry *rte = (RangeTblEntry *)lfirst(lc);
        if (rte->rtekind == RTE_RELATION) {
            tables_oid_list = lappend_oid(tables_oid_list, rte->relid);
        }
    }
    if (tables_oid_list != NULL && get_query_anomaly_labels(tables_oid_list)) {
        report_risky_query_error("Risky query access resource label", pstate->p_sourcetext);
    }
}

/*
 * @brief Anomaly detection post analyze hook
 * @param [in] pstate
 * @param [in] query
 */
static void anomaly_detection_analyze_hook(ParseState *pstate, Query *query)
{
    if (pre_anomaly_detection_analyze_hook != NULL) {
        pre_anomaly_detection_analyze_hook(pstate, query);
    }
    if (!u_sess->attr.attr_security.enable_risky_query_detection) {
        return;
    }
    if (!check_if_to_implement_anomaly()) {
        return;
    }
    if (query == NULL || pstate == NULL || pstate->p_sourcetext == NULL) {
        return;
    }
    load_query_anomaly_labels(false);
    check_utility_operation(pstate, query);
    check_resource_label(pstate, query);
    check_where_clause(pstate, query);
}

/*
 * @brief setup the hook
 */
void install_query_anomaly_hook()
{
    if (post_parse_analyze_hook != anomaly_detection_analyze_hook) {
        pre_anomaly_detection_analyze_hook = post_parse_analyze_hook;
        post_parse_analyze_hook = anomaly_detection_analyze_hook;
    }
    // setup ExecutorFinish_hook
    if (ExecutorFinish_hook != anomaly_detection_executor_finish_hook) {
        pre_anomaly_detection_executor_finish = ExecutorFinish_hook;
        ExecutorFinish_hook = anomaly_detection_executor_finish_hook;
    }
}

/*
 * @brief release server memory on shutdown
 */
void anomaly_detection_close()
{
    finish_query_anomaly_labels();
}
