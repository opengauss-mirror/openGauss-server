#include "postgres.h"
#include "fmgr/fmgr_comp.h"
#include "utils/elog.h"
#include "utils/builtins.h"
#include "utils/array.h"
#include "utils/partitionmap_gs.h"
#include "utils/postinit.h"
#include "knl/knl_instance.h"
#include "knl/knl_thread.h"
#include "nodes/pg_list.h"
#include "distributelayer/streamProducer.h"
#include "pthread.h"
#include "plugin_utils/datetime.h"
#include "plugin_utils/my_locale.h"
#include <unordered_set>
#include <string>
#include <iostream>
#include "cjson/cJSON.h"

#define DEBUG_MODE 0
#define NOT_USE_WHITE_LIST 1
#define USE_JSON_OUTPUT 1

typedef struct {
  char* parse_tree_json;
  char* err_msg;
  bool is_passed;
} OgQueryParseResult;

typedef struct parser_walker_context
{
    bool has_error;
    std::unordered_set<std::string>* typeWhitelist;
    std::unordered_set<std::string>* funcWhitelist;
    OgQueryParseResult* result;
} parser_walker_context;

typedef struct json_walker_context
{
    cJSON* root_obj;
    cJSON* cur_obj;
    cJSON* pre_obj;
    Node* parent_node;
} json_walker_context;

knl_instance_context g_instance;

static List* raw_parser_opengauss(const char* str);
extern "C" List* dolphin_raw_parser(const char* str, List** query_string_locationlist);
#if NOT_USE_WHITE_LIST
extern "C" OgQueryParseResult raw_parser_opengauss_dolphin(const char* str);
#else
extern "C" bool raw_parser_opengauss_dolphin(const char* str, const char* typelist, const char* funclist);
#endif

static bool isStmtNode(Node* node, char** stmtType)
{
    switch (nodeTag(node)) {
        case T_InsertStmt: {
            *stmtType = "insert";
        } break;
        case T_DeleteStmt: {
            *stmtType = "delete";
        } break;
        case T_UpdateStmt: {
            *stmtType = "update";
        } break;
        case T_MergeStmt: {
            *stmtType = "merge";
        } break;
        case T_SelectStmt: {
            *stmtType = "select";
	    } break;
        case T_CreateStmt: {
            *stmtType = "create";
	    } break;
	    case T_CompositeTypeStmt: {
            *stmtType = "create type";
	    } break;
	    case T_AlterTableStmt: {
            *stmtType = "alter table";
	    } break;
        case T_IndexStmt: {
            *stmtType = "create index";
        } break;
        case T_RenameStmt: {
            *stmtType = "rename";
        } break;
        case T_DropStmt: {
            *stmtType = "drop";
        } break;
        case T_TruncateStmt: {
            *stmtType = "truncate";
        } break;
        default:
            return false;
    }
    return true;
}

cJSON* get_or_create_field(cJSON* obj, char* filedname)
{
    cJSON* field = cJSON_GetObjectItem(obj, filedname);
    if (!field) {
        field = cJSON_AddArrayToObject(obj, filedname);
    }
    return field;
}

static cJSON* create_constraint_json(cJSON* cur_obj, Constraint* conNode)
{
    cJSON* con = cJSON_CreateObject();
    if (conNode->contype == CONSTR_PRIMARY) {
        cJSON_AddStringToObject(con, "contype", "PRIMARY_KEY");
    } else if (conNode->contype == CONSTR_UNIQUE) {
        cJSON_AddStringToObject(con, "contype", "UNIQUE_KEY");
    } else if (conNode->contype == CONSTR_CHECK) {
        cJSON_AddStringToObject(con, "contype", "CHECK");
    } else if (conNode->contype == CONSTR_NOTNULL) {
        cJSON_AddStringToObject(con, "contype", "NOTNULL");
    } else if (conNode->contype == CONSTR_DEFAULT) {
        cJSON_AddStringToObject(con, "contype", "DEFAULT");
    } else if (conNode->contype == CONSTR_FOREIGN) {
        cJSON_AddStringToObject(con, "contype", "FOREIGN_KEY");
    } else {
        cJSON_Delete(con);
        return nullptr;
    }
    cJSON* cons = get_or_create_field(cur_obj, "constraints");
    cJSON* keys = get_or_create_field(con, "keys");
    cJSON_AddItemToArray(cons, con);
    return keys;
}

static void add_typename_into_field(TypeName* typname, cJSON* field)
{
    List* names = typname->names;
    const int LENGTH_TWO = 2;
    if (names->length = LENGTH_TWO && strcmp(strVal(linitial(names)), "pg_catalog") == 0) {
        cJSON_AddStringToObject(field, "fieldType", strVal(lsecond(names)));
    } else {
        cJSON_AddStringToObject(field, "fieldType", NameListToString(names));
    }
}

extern bool create_json_walker(Node* node, void* walker_context)
{
    if (node == NULL) {
        return false;
    }

    json_walker_context* context = (json_walker_context*)walker_context;
    char* nodetype = NULL;
    cJSON* cur_obj = context->cur_obj;

    if (isStmtNode(node, &nodetype)) {
        cJSON* stmt = cJSON_CreateObject();
        cJSON_AddStringToObject(stmt, "stmtType", nodetype);
        cJSON_AddItemToObject(stmt, "stmts", cJSON_CreateArray());
        cJSON_AddItemToArray(cJSON_GetObjectItem(cur_obj, "stmts"), stmt);
        context->pre_obj = context->cur_obj;
        context->cur_obj = stmt;
        if (IsA(node, IndexStmt)) {
            cJSON* keys = get_or_create_field(stmt, "keys");
            ListCell* lc_key = NULL;
            foreach(lc_key, ((IndexStmt*)node)->indexParams) {
                IndexElem *elem = (IndexElem*)lfirst(lc_key);
                cJSON_AddItemToArray(keys, cJSON_CreateString(elem->name));
            }
            if (((IndexStmt*)node)->relation && ((IndexStmt*)node)->relation->relname) {
                cJSON_AddStringToObject(stmt, "relname", ((IndexStmt*)node)->relation->relname);
            }
        }
        if (IsA(node, RenameStmt)) {
            RenameStmt* rnstmt = (RenameStmt*)node;
            if (rnstmt->relation && rnstmt->relation->relname) {
                cJSON_AddStringToObject(stmt, "tablename", rnstmt->relation->relname);
            }
            if (rnstmt->newname) {
                cJSON_AddStringToObject(stmt, "newname", rnstmt->newname);
            }
            if (rnstmt->subname) {
                cJSON_AddStringToObject(stmt, "subname", rnstmt->subname);
            }
            if (rnstmt->newschema) {
                cJSON_AddStringToObject(stmt, "newschema", rnstmt->newschema);
            }
        }

        if (IsA(node, DropStmt)) {
            DropStmt* dstmt = (DropStmt*)node;
            if (dstmt->objects) {
                cJSON* objects = get_or_create_field(stmt, "objects");
                ListCell* lc_obj = NULL;
                foreach(lc_obj, dstmt->objects) {
                    cJSON* object = cJSON_CreateObject();
                    cJSON_AddStringToObject(object, "objectName", NameListToString((List*)lfirst(lc_obj)));
                    cJSON_AddItemToArray(objects, object);
                }
            }
        }
    }

    if (IsA(node, AlterTableCmd) && ((AlterTableCmd*)node)->name) {
        cJSON* fields = get_or_create_field(cur_obj, "fields");
        cJSON* field = cJSON_CreateObject();
        cJSON_AddStringToObject(field, "fieldName", ((AlterTableCmd*)node)->name);
        cJSON_AddItemToArray(fields, field);
    }

    if (IsA(node, ResTarget) && ((ResTarget*)node)->name && !((ResTarget*)node)->indirection) {
        cJSON* fields = get_or_create_field(cur_obj, "fields");
        cJSON* field = cJSON_CreateObject();
        cJSON_AddStringToObject(field, "fieldName", ((ResTarget*)node)->name);
        cJSON_AddItemToArray(fields, field);
    }

    if (IsA(node, ColumnRef)) {
        cJSON* fields = get_or_create_field(cur_obj, "fields");
        cJSON* field = cJSON_CreateObject();
        cJSON_AddStringToObject(field, "fieldName", NameListToString(((ColumnRef*)node)->fields));
        cJSON_AddItemToArray(fields, field);
        Node* parent_node = context->parent_node;
        if (parent_node && IsA(parent_node, TypeCast)) {
            add_typename_into_field(((TypeCast*)parent_node)->typname, field);
        }
    }

    if (IsA(node, ColumnDef)) {
        cJSON* fields = get_or_create_field(cur_obj, "fields");
        cJSON* field = cJSON_CreateObject();
        Node* parent_node = context->parent_node;
        if (parent_node && IsA(parent_node, AlterTableCmd) && ((AlterTableCmd*)parent_node)->subtype == AT_AlterColumnType) {
            cJSON_AddStringToObject(field, "fieldName", ((AlterTableCmd*)parent_node)->name);
        } else {
            cJSON_AddStringToObject(field, "fieldName", ((ColumnDef*)node)->colname);
        }
        add_typename_into_field(((ColumnDef*)node)->typname, field);
        cJSON_AddItemToArray(fields, field);
        // Constraint
        ListCell* lc_con = NULL;
        foreach(lc_con, ((ColumnDef*)node)->constraints) {
            Constraint *con = (Constraint*)lfirst(lc_con);
            cJSON* keys = create_constraint_json(cur_obj, con);
            if (keys) {
                cJSON_AddItemToArray(keys, cJSON_CreateString(((ColumnDef*)node)->colname));
            }
        }
    }

    if (IsA(node, RangeVar)) {
        cJSON* relations = get_or_create_field(cur_obj, "relations");
        cJSON* relation = cJSON_CreateObject();
        cJSON_AddStringToObject(relation, "relName", ((RangeVar*)node)->relname);
        if (((RangeVar*)node)->alias) {
            cJSON_AddStringToObject(relation, "alias", ((RangeVar*)node)->alias->aliasname);
        }
        cJSON_AddItemToArray(relations, relation);
    }

    if (IsA(node, A_Expr)) {
        cJSON* exprs = get_or_create_field(cur_obj, "exprs");
        cJSON* expr = cJSON_CreateObject();
        cJSON_AddStringToObject(expr, "exprName", NameListToString(((A_Expr*)node)->name));
        cJSON_AddItemToArray(exprs, expr);
    }

    if (IsA(node, Constraint)) {
        cJSON* keys = create_constraint_json(cur_obj, (Constraint*)node);
        ListCell* lc_key = NULL;
        foreach(lc_key, ((Constraint*)node)->keys) {
            IndexElem *elem = (IndexElem*)lfirst(lc_key);
            cJSON_AddItemToArray(keys, cJSON_CreateString(elem->name));
        }
    }

    if (IsA(node, FuncCall)) {
        cJSON* funcs = get_or_create_field(cur_obj, "funcs");
        cJSON* func = cJSON_CreateObject();
        cJSON_AddStringToObject(func, "funcName", NameListToString(((FuncCall*)node)->funcname));
        cJSON_AddItemToArray(funcs, func);
    }

    context->parent_node = node;
    return raw_expression_tree_walker(node, (bool (*)())create_json_walker, (void*)context);
}

static bool isInTypeWhitelist(TypeName* type, std::unordered_set<std::string>* typeWhitelist)
{
    List* names = type->names;
    if (list_length(names) != 1) {
        return true;
    }
    char* name = NameListToString(names);
    if (typeWhitelist->find(name) == typeWhitelist->end()) {
        return false;
    }
    return true;
}

static bool isInFuncWhitelist(FuncCall* func, std::unordered_set<std::string>* funcWhitelist)
{
    List* names = func->funcname;
    if (list_length(names) != 1) {
        return true;
    }
    char* name = NameListToString(names);
    if (funcWhitelist->find(name) == funcWhitelist->end()) {
        return false;
    }
    return true;
}

static bool check_parse_type_and_func_walker(Node* node, void* context)
{
    if (node == NULL) {
        return false;
    }

    if (IsA(node, TypeName) && !isInTypeWhitelist((TypeName*)node, ((parser_walker_context*)context)->typeWhitelist)) {
        printf("unsupport typeName \"%s\".\n", NameListToString(((TypeName*)node)->names));
        ((parser_walker_context*)context)->has_error = true;
        return false;
    }

    if (IsA(node, FuncCall) && !isInFuncWhitelist((FuncCall*)node, ((parser_walker_context*)context)->funcWhitelist)) {
        printf("unsupport funcCall \"%s\".\n", NameListToString(((FuncCall*)node)->funcname));
        ((parser_walker_context*)context)->has_error = true;
        return false;
    }

    return raw_expression_tree_walker(node, (bool (*)())check_parse_type_and_func_walker, (void*)context);
}

static List* raw_parser_opengauss(const char* str, parser_walker_context* context)
{    
    MemoryContextInit();
    knl_thread_init(WORKER);

    t_thrd.fake_session = create_session_context(t_thrd.top_mem_cxt, 0);
    t_thrd.fake_session->status = KNL_SESS_FAKE;
    u_sess = t_thrd.fake_session;

    struct PGPROC proc;
    t_thrd.proc = &proc;
    proc.workingVersionNum = 0;
    List* parsetree_list = NULL;
    char* json_tree = NULL;

    PG_TRY();
    {
        parsetree_list = dolphin_raw_parser(str, NULL);
    }
    PG_CATCH();
    {
        MemoryContext ecxt = MemoryContextSwitchTo(CurrentMemoryContext);
        ErrorData* edata = CopyErrorData();
        FlushErrorState();
        context->result->err_msg = strdup(edata->message);
#if DEBUG_MODE
        printf("Details: %s\n", edata->message);   
        printf("Syntax error detected in sql: %s\n", trim((char*)str));
#endif
        (void)MemoryContextSwitchTo(ecxt);
        context->has_error = true;
    }
    PG_END_TRY();

    json_walker_context json_context;
    json_context.root_obj = cJSON_CreateObject();
    json_context.cur_obj = json_context.root_obj;
    json_context.parent_node = NULL;
    cJSON_AddArrayToObject(json_context.root_obj, "stmts");
    ListCell* raw_parsetree_item = NULL;
    foreach (raw_parsetree_item, parsetree_list) {
        Node* parsetree = (Node*)lfirst(raw_parsetree_item);
        create_json_walker(parsetree, &json_context);
        json_context.cur_obj = json_context.root_obj;
    }
    json_tree = cJSON_Print(json_context.root_obj);

#if DEBUG_MODE
        printf("Details: %s\n\n", nodeToString(parsetree_list));
#endif

#if NOT_USE_WHITE_LIST
    return context->has_error ? nullptr : parsetree_list;
#else
    foreach (raw_parsetree_item, parsetree_list) {
        Node* parsetree = (Node*)lfirst(raw_parsetree_item);
        check_parse_type_and_func_walker(parsetree, context);
    }
    if (context->has_error && parsetree_list != nullptr) {
        printf("TypeName or FuncCall error detected in SQL: %s\n", trim((char*)str));
    }
    return context->has_error ? nullptr : parsetree_list;
#endif
}

#if NOT_USE_WHITE_LIST
OgQueryParseResult raw_parser_opengauss_dolphin(const char* str)
{
    OgQueryParseResult result;
    result.err_msg = nullptr;
    parser_walker_context context = {false, nullptr, nullptr, &result};
    List* parsetree_list = raw_parser_opengauss(str, &context);
    // output JSON
    json_walker_context json_context;
    json_context.root_obj = cJSON_CreateObject();
    cJSON_AddStringToObject(json_context.root_obj, "version", "7.0.0 RC1");
    if (parsetree_list) {
        json_context.cur_obj = json_context.root_obj;
        json_context.parent_node = NULL;
        cJSON_AddArrayToObject(json_context.root_obj, "stmts");
        ListCell* raw_parsetree_item = NULL;
        foreach (raw_parsetree_item, parsetree_list) {
            Node* parsetree = (Node*)lfirst(raw_parsetree_item);
            create_json_walker(parsetree, &json_context);
            json_context.cur_obj = json_context.root_obj;
        }
        result.is_passed = true;
    } else {
        result.is_passed = false;
    }
    result.parse_tree_json = cJSON_Print(json_context.root_obj);
#if DEBUG_MODE
        printf("Json: %s\n\n", result.parse_tree_json);
#endif
    return result;
}

#else
bool raw_parser_opengauss_dolphin(const char* str, const char* typelist_p, const char* funclist_p)
{
    std::unordered_set<std::string> typeWhitelist {
        "bool",  "bytea",  "char",  "name",  "int2vector",  "regproc",  "text",  "oid",  "tid",  "xid",  "xid32",  "cid",
        "oidvector",  "oidvector_extend",  "int2vector_extend",  "raw",  "_raw",  "blob",  "_blob",  "clob",  "_clob",
        "pg_type",  "pg_attribute",  "pg_proc",  "pg_class",  "gs_package",  "pg_object_type",  "json",  "xml",  "_xml",
        "_json",  "smgr",  "point",  "lseg",  "path",  "box",  "polygon",  "_line",  "float4",  "float8",  "abstime",
        "reltime",  "tinterval",  "unknown",  "circle",  "_circle",  "money",  "_money",  "macaddr",  "inet",  "cidr",
        "_bool",  "_bytea",  "_char",  "_name",  "_int2vector_extend",  "_int2",  "_int1",  "_int2vector",  "_int4",
        "_regproc",  "_text",  "_oid",  "_tid",  "_xid",  "_xid32",  "_cid",  "_oidvector",  "_bpchar",  "_varchar",  "_int8",
        "_point",  "_lseg",  "_path",  "_box",  "_float4",  "_float8",  "_abstime",  "_reltime",  "_tinterval",  "_polygon",
        "aclitem",  "_aclitem",  "_macaddr",  "_inet",  "_cidr",  "bpchar",  "varchar",  "nvarchar2",  "_nvarchar2",  "date",
        "time",  "timestamp",  "_timestamp",  "_date",  "_time",  "timestamptz",  "_timestamptz",  "interval",  "_interval",
        "_numeric",  "_int16",  "timetz",  "_timetz",  "bit",  "_bit",  "varbit",  "_varbit",  "numeric",  "refcursor",
        "_refcursor",  "regprocedure",  "regoper",  "regoperator",  "regclass",  "regtype",  "_regprocedure",  "_regoper",
        "_regoperator",  "_regclass",  "_regtype",  "uuid",  "_uuid",  "tsvector",  "gtsvector",  "tsquery",  "regconfig",
        "regdictionary",  "_tsvector",  "_gtsvector",  "_tsquery",  "_regconfig",  "_regdictionary",  "jsonb",  "_jsonb",
        "txid_snapshot",  "_txid_snapshot",  "int4range",  "_int4range",  "numrange",  "_numrange",  "tsrange",  "_tsrange",
        "tstzrange",  "_tstzrange",  "daterange",  "_daterange",  "int8range",  "_int8range",  "smalldatetime",
        "_smalldatetime",  "hll",  "hash16",  "hash32",  "_hash16",  "_hash32",  "undefined",  "vector",  "sparsevec",
        "_vector",  "_sparsevec",  "pg_partition",  "pg_attrdef",  "pg_constraint",  "pg_inherits",  "pg_index",
        "pg_operator",  "pg_opfamily",  "pg_opclass",  "pg_am",  "pg_amop",  "pg_amproc",  "pg_language",
        "pg_largeobject_metadata",  "pg_largeobject",  "pg_aggregate",  "pg_rewrite",  "pg_trigger",  "pg_event_trigger",
        "pg_description",  "pg_cast",  "pg_enum",  "pg_set",  "pg_namespace",  "pg_conversion",  "pg_depend",  "pg_database",
        "pg_db_role_setting",  "pg_tablespace",  "pg_pltemplate",  "pg_authid",  "pg_auth_members",  "pg_shdepend",
        "pg_shdescription",  "pg_ts_config",  "pg_ts_config_map",  "pg_ts_dict",  "pg_ts_parser",  "pg_ts_template",
        "pg_auth_history",  "pg_user_status",  "pg_extension",  "pg_obsscaninfo",  "pg_foreign_data_wrapper",
        "pg_foreign_server",  "pg_user_mapping",  "pgxc_class",  "pgxc_node",  "pgxc_group",  "pg_resource_pool",
        "pg_workload_group",  "pg_app_workloadgroup_mapping",  "pg_foreign_table",  "pg_rlspolicy",  "pg_default_acl",
        "pg_seclabel",  "pg_shseclabel",  "pg_collation",  "pg_range",  "gs_policy_label",  "gs_auditing_policy",
        "gs_auditing_policy_access",  "gs_auditing_policy_privileges",  "gs_auditing_policy_filters",  "gs_masking_policy",
        "gs_masking_policy_actions",  "gs_masking_policy_filters",  "gs_encrypted_columns",  "gs_column_keys",
        "gs_column_keys_args",  "gs_client_global_keys",  "gs_encrypted_proc",  "gs_client_global_keys_args",  "pg_job",
        "gs_asp",  "pg_job_proc",  "pg_extension_data_source",  "pg_object",  "pg_synonym",  "gs_obsscaninfo",  "pg_directory",
        "pg_hashbucket",  "gs_global_chain",  "gs_global_config",  "streaming_stream",  "streaming_cont_query",
        "streaming_reaper_status",  "gs_matview",  "gs_matview_dependency",  "gs_matview_log",  "pgxc_slice",  "gs_opt_model",
        "gs_recyclebin",  "gs_txn_snapshot",  "gs_model_warehouse",  "gs_dependencies",  "gs_dependencies_obj",
        "gs_job_argument",  "gs_job_attribute",  "gs_uid",  "gs_db_privilege",  "pg_replication_origin",  "pg_publication",
        "pg_publication_rel",  "pg_subscription",  "gs_sql_patch",  "pg_subscription_rel",  "pg_proc_ext",
        "pgxc_prepared_xacts",  "pg_roles",  "pg_shadow",  "pg_group",  "pg_user",  "pg_rules",  "gs_labels",
        "gs_auditing_access",  "gs_auditing_privilege",  "gs_auditing",  "gs_masking",  "pg_rlspolicies",  "pg_views",
        "pg_tables",  "gs_matviews",  "pg_indexes",  "pg_gtt_relstats",  "pg_gtt_attached_pids",  "pg_gtt_stats",  "pg_locks",
        "pg_cursors",  "pg_available_extensions",  "pg_available_extension_versions",  "pg_prepared_xacts",
        "pg_prepared_statements",  "pg_seclabels",  "pg_settings",  "pg_timezone_abbrevs",  "pg_timezone_names",
        "pg_control_group_config",  "pg_stat_all_tables",  "pg_stat_xact_all_tables",  "pg_stat_sys_tables",
        "pg_stat_xact_sys_tables",  "pg_stat_user_tables",  "pg_stat_xact_user_tables",  "pg_statio_all_tables",
        "pg_statio_sys_tables",  "pg_statio_user_tables",  "pg_stat_all_indexes",  "pg_stat_sys_indexes",
        "pg_stat_user_indexes",  "pg_statio_all_indexes",  "pg_statio_sys_indexes",  "pg_statio_user_indexes",
        "pg_statio_all_sequences",  "pg_statio_sys_sequences",  "pg_statio_user_sequences",  "pg_stat_activity",
        "pg_stat_activity_ng",  "pg_session_wlmstat",  "pg_wlm_statistics",  "gs_session_memory_statistics"
        "pg_session_iostat",  "gs_cluster_resource_info",  "gs_session_cpu_statistics",  "gs_wlm_session_statistics",
        "pg_total_user_resource_info_oid",  "pg_total_user_resource_info",  "gs_wlm_user_resource_history",
        "gs_wlm_instance_history",  "gs_wlm_session_query_info_all",  "gs_wlm_session_info_all",  "gs_wlm_session_info",
        "gs_wlm_session_history",  "gs_wlm_cgroup_info",  "gs_wlm_user_info",  "gs_wlm_resource_pool",
        "gs_wlm_rebuild_user_resource_pool",  "gs_wlm_workload_records",  "gs_os_run_info",  "gs_session_memory_context",
        "gs_thread_memory_context",  "gs_shared_memory_detail",  "gs_instance_time",  "gs_session_time",  "gs_session_memory",
        "gs_total_memory_detail",  "pg_total_memory_detail",  "gs_redo_stat",  "gs_session_stat",  "gs_file_stat",
        "gs_session_memory_detail",  "pg_stat_replication",  "pg_replication_slots",  "pg_stat_database",
        "pg_stat_database_conflicts",  "pg_stat_user_functions",  "pg_stat_xact_user_functions",  "pg_stat_bgwriter",
        "pg_user_mappings",  "exception",  "pg_thread_wait_status",  "pgxc_thread_wait_status",  "gs_sql_count",
        "pg_os_threads",  "pg_node_env",  "pg_comm_status",  "pg_comm_recv_stream",  "pg_comm_send_stream",
        "pg_running_xacts",  "pg_variable_info",  "pg_get_invalid_backends",  "pg_get_senders_catchup_time",
        "gs_stat_session_cu",  "pg_comm_delay",  "gs_comm_proxy_thread_status",  "gs_all_control_group_info",  "mpp_tables",
        "gs_wlm_operator_info",  "gs_wlm_operator_statistics",  "gs_wlm_operator_history",  "gs_wlm_plan_operator_info",
        "gs_wlm_plan_operator_history",  "gs_wlm_plan_encoding_table",  "gs_total_nodegroup_memory_detail",
        "gs_get_control_group_info",  "gs_wlm_ec_operator_statistics",  "gs_wlm_ec_operator_history",
        "gs_wlm_ec_operator_info",  "pg_tde_info",  "pg_stat_bad_block",  "plan_table_data",  "plan_table",
        "get_global_prepared_xacts",  "statement_history",  "sys_dummy",  "bulk_exception",  "_bulk_exception",
        "gs_db_privileges",  "gs_gsc_memory_detail",  "gs_lsc_memory_detail",  "pg_publication_tables",
        "pg_stat_subscription",  "pg_replication_origin_status",  "xmltype",  "anytype",  "anydata",  "anydataset",
        "statement_history",  "hstore",  "_hstore",  "dblink_pkey_results",  "_dblink_pkey_results",  "index_statistic",
        "_index_statistic",  "pg_type_nonstrict_basic_value",  "_pg_type_nonstrict_basic_value",  "_year",  "_uint1",
        "_uint2",  "_uint4",  "_uint8",  "binary",  "_binary",  "varbinary",  "_varbinary",  "tinyblob",  "_tinyblob",
        "mediumblob",  "_mediumblob",  "longblob",  "_longblob",  "statement_history",  "year",  "int1",  "uint1",  "int2",
        "uint2",  "int4",  "uint4",  "int8",  "uint8"
    };
    std::unordered_set<std::string> funcWhitelist {
        "abbrev",  "abort",  "abs",  "abstime",  "abstimeeq",  "abstimege",  "abstimegt",  "abstimein",  "abstimele",
        "abstimelt",  "abstimene",  "abstimeout",  "abstimerecv",  "abstimesend",  "abstime_text",
        "abstime_to_smalldatetime",  "accessbdouble",  "accessblob",  "accesschar",  "accessdate",  "accessnchar",
        "accessnumber",  "accessnvarchar2",  "accessraw",  "accesstimestamp",  "accesstimestamptz",  "accessvarchar",
        "accessvarchar2",  "aclcontains",  "acldefault",  "aclexplode",  "aclinsert",  "aclitemeq",  "aclitemin",
        "aclitemout",  "aclremove",  "acos",  "add_breakpoint",  "adddate",  "addinstance",  "add_months",  "addtime",
        "aes_decrypt",  "aes_encrypt",  "age",  "akeys",  "alldigitsmasking",  "analyze_partition",  "analyze_query",
        "analyze_tables",  "analyze_workload",  "any2boolean",  "any2interval",  "any_accum",  "anyarray_in",
        "anyarray_out",  "anyarray_recv",  "anyarray_send",  "anydata",  "anydataset",  "anyelement_in",
        "anyelement_out",  "anyenum_in",  "anyenum_out",  "anyenum_sum",  "any_in",  "anynonarray_in",
        "anynonarray_out",  "any_out",  "anyrange_in",  "anyrange_out",  "anyset_in",  "anyset_out",  "anyset_sum",
        "anytextcat",  "anytype",  "any_value",  "appendchildxml",  "archive_snapshot",  "area",  "areajoinsel",
        "areasel",  "array_agg",  "array_agg_finalfn",  "array_agg_transfn",  "array_append",  "array_cat",
        "arraycontained",  "arraycontains",  "arraycontjoinsel",  "arraycontsel",  "array_delete",  "array_deleteidx",
        "array_dims",  "array_eq",  "array_except",  "array_except_distinct",  "array_exists",  "array_extend",
        "array_extendnull",  "array_fill",  "array_ge",  "array_gt",  "array_in",  "array_indexby_delete",
        "array_indexby_length",  "array_integer_agg_add",  "array_integer_deleteidx",  "array_integer_exists",
        "array_integer_first",  "array_integer_last",  "array_integer_next",  "array_integer_prior",
        "array_integer_sum",  "array_intersect",  "array_intersect_distinct",  "array_larger",  "array_le",
        "array_length",  "array_lower",  "array_lt",  "array_ndims",  "array_ne",  "array_next",  "array_out",
        "arrayoverlap",  "array_prepend",  "array_prior",  "array_recv",  "array_remove",  "array_replace",
        "array_send",  "array_smaller",  "array_to_json",  "array_to_string",  "array_to_vector",  "array_trim",
        "array_typanalyze",  "array_union",  "array_union_distinct",  "array_upper",  "array_varchar_deleteidx",
        "array_varchar_exists",  "array_varchar_first",  "array_varchar_last",  "array_varchar_next",
        "array_varchar_prior",  "ascii",  "ascii_to_mic",  "ascii_to_utf8",  "asin",  "assign_table_type",
        "a_sysdate",  "atan",  "atan2",  "attach",  "avals",  "avg",  "backtrace",  "basicemailmasking",
        "b_between_and",  "b_db_date",  "b_db_last_day",  "b_db_statement_start_time",
        "b_db_statement_start_timestamp",  "b_db_sys_real_timestamp",  "begincreate",  "b_extract",  "big5_to_euc_tw",
        "big5_to_mic",  "big5_to_utf8",  "bigint_any_value",  "bigint_tid",  "bin",  "binary2boolean",  "binary_and",
        "binaryand",  "binary_boolean_eq",  "binary_boolean_ge",  "binary_boolean_gt",  "binary_boolean_le",
        "binary_boolean_lt",  "binary_boolean_ne",  "binary_cmp",  "binaryeq",  "binaryge",  "binarygt",  "binary_in",
        "binary_int1_eq",  "binary_int1_ne",  "binary_json",  "binary_larger",  "binaryle",  "binarylike",
        "binarylt",  "binaryne",  "binarynlike",  "binary_numeric_ge",  "binary_numeric_gt",  "binary_numeric_le",
        "binary_numeric_lt",  "binary_or_binary",  "binary_or_double",  "binary_or_uint8",  "binary_out",
        "binary_quantize",  "binary_recv",  "binary_send",  "binary_smaller",  "binary_text_eq",  "binarytextlike",
        "binarytextnlike",  "binary_time_ge",  "binary_time_gt",  "binary_time_le",  "binary_time_lt",
        "binary_timestamp",  "binary_timestamptz",  "binary_typmodin",  "binary_typmodout",  "binary_uint1_ge",
        "binary_uint1_gt",  "binary_uint1_le",  "binary_uint1_lt",  "binary_uint2_ge",  "binary_uint2_gt",
        "binary_uint2_le",  "binary_uint2_lt",  "binary_uint4_ge",  "binary_uint4_gt",  "binary_uint4_le",
        "binary_uint4_lt",  "binary_uint8_ge",  "binary_uint8_gt",  "binary_uint8_le",  "binary_uint8_lt",
        "binary_varbinary",  "binary_varbinary_eq",  "binary_varbinary_ge",  "binary_varbinary_gt",
        "binary_varbinary_le",  "binary_varbinary_lt",  "binary_varbinary_ne",  "binary_xor_binary",
        "binary_year_eq",  "binary_year_ge",  "binary_year_gt",  "binary_year_le",  "binary_year_lt",
        "binary_year_ne",  "bin_bit_concat",  "bin_bool_concat",  "bin_concat",  "bin_float4_concat",
        "bin_float8_concat",  "bin_int8_concat",  "bin_int_concat",  "bin_num_concat",  "bin_text_concat",
        "bin_to_num",  "bin_varbin_concat",  "bit",  "bit2float4",  "bit2float8",  "bit2numeric",  "bit_and",
        "bitand",  "bit_bin_concat",  "bit_bin_in",  "bit_blob",  "bit_bool",  "bit_bool_concat",  "bit_bool_ge",
        "bit_bool_gt",  "bit_bool_le",  "bit_bool_lt",  "bit_cast_date",  "bit_cast_datetime",  "bit_cast_int8",
        "bit_cast_time",  "bit_cast_timestamp",  "bitcat",  "bitcmp",  "bit_concat",  "bit_count",  "bit_date_ge",
        "bit_date_gt",  "bit_date_le",  "bit_date_lt",  "bit_enum",  "biteq",  "bitfromdate",  "bitfromdatetime",
        "bitfromfloat4",  "bitfromfloat8",  "bitfromint1",  "bitfromint2",  "bitfromnumeric",  "bitfromtime",
        "bitfromtimestamp",  "bitfromuint1",  "bitfromuint2",  "bitfromuint4",  "bitfromuint8",  "bitfromyear",
        "bitge",  "bitgt",  "bit_in",  "bit_json",  "bit_larger",  "bitle",  "bit_length",  "bitlike",
        "bit_longblob",  "bitlt",  "bitne",  "bitnlike",  "bitnot",  "bit_or",  "bitor",  "bitother2like",
        "bitother3like",  "bitother4like",  "bitother5like",  "bitother6like",  "bitother7like",  "bitother8like",
        "bitotherlike",  "bitothern2like",  "bitothern3like",  "bitothern4like",  "bitothern5like",  "bitothern6like",
        "bitothern7like",  "bitothern8like",  "bitothernlike",  "bit_out",  "bit_recv",  "bit_send",  "bitshiftleft",
        "bitshiftright",  "bit_smaller",  "bit_text_concat",  "bit_time_ge",  "bit_time_gt",  "bit_time_le",
        "bit_time_lt",  "bit_timestamp_ge",  "bit_timestamp_gt",  "bit_timestamp_le",  "bit_timestamp_lt",
        "bit_timestamptz_ge",  "bit_timestamptz_gt",  "bit_timestamptz_le",  "bit_timestamptz_lt",  "bittobinary",
        "bittoblob",  "bittochar",  "bittodate",  "bittodatetime",  "bittoint1",  "bittoint2",  "bittolongblob",
        "bittomediumblob",  "bittotext",  "bittotime",  "bittotimestamp",  "bittotinyblob",  "bittouint1",
        "bittouint2",  "bittouint4",  "bittouint8",  "bittovarbinary",  "bittovarchar",  "bittypmodin",
        "bittypmodout",  "bit_uint8_ge",  "bit_uint8_gt",  "bit_uint8_le",  "bit_uint8_lt",  "bit_varbin_concat",
        "bit_xor",  "bitxor",  "bit_xor_time",  "bit_year",  "bit_year_ge",  "bit_year_gt",  "bit_year_le",
        "bit_year_lt",  "bloband",  "blob_any_value",  "blob_boolean_eq",  "blob_boolean_ge",  "blob_boolean_gt",
        "blob_boolean_le",  "blob_boolean_lt",  "blob_boolean_ne",  "blob_cmp",  "blob_datetime_cmp",
        "blob_datetime_eq",  "blob_datetime_ge",  "blob_datetime_gt",  "blob_datetime_le",  "blob_datetime_lt",
        "blob_datetime_ne",  "blob_date_xor",  "blob_eq",  "blob_eq_text",  "blob_float8_cmp",  "blob_float8_eq",
        "blob_float8_ge",  "blob_float8_gt",  "blob_float8_le",  "blob_float8_lt",  "blob_float8_ne",  "blob_ge",
        "blob_ge_text",  "blob_gt",  "blob_gt_text",  "blob_int8_cmp",  "blob_int8_eq",  "blob_int8_ge",
        "blob_int8_gt",  "blob_int8_le",  "blob_int8_lt",  "blob_int8_ne",  "blob_json",  "blob_larger",  "blob_le",
        "blob_le_text",  "bloblike",  "blob_longblob_cmp",  "blob_longblob_eq",  "blob_longblob_ge",
        "blob_longblob_gt",  "blob_longblob_le",  "blob_longblob_lt",  "blob_longblob_ne",  "blob_lt",
        "blob_lt_text",  "blob_mediumblob_cmp",  "blob_mediumblob_eq",  "blob_mediumblob_ge",  "blob_mediumblob_gt",
        "blob_mediumblob_le",  "blob_mediumblob_lt",  "blob_mediumblob_ne",  "blob_ne",  "blob_ne_text",
        "blob_numeric_cmp",  "blob_numeric_eq",  "blob_numeric_ge",  "blob_numeric_gt",  "blob_numeric_le",
        "blob_numeric_lt",  "blob_numeric_ne",  "blob_smaller",  "blob_timestamp_cmp",  "blob_timestamp_eq",
        "blob_timestamp_ge",  "blob_timestamp_gt",  "blob_timestamp_le",  "blob_timestamp_lt",  "blob_timestamp_ne",
        "blob_timestamptz_xor",  "blob_tinyblob_cmp",  "blob_tinyblob_eq",  "blob_tinyblob_ge",  "blob_tinyblob_gt",
        "blob_tinyblob_le",  "blob_tinyblob_lt",  "blob_tinyblob_ne",  "blob_to_float8",  "blob_uint8_cmp",
        "blob_uint8_eq",  "blob_uint8_ge",  "blob_uint8_gt",  "blob_uint8_le",  "blob_uint8_lt",  "blob_uint8_ne",
        "blobxor",  "blob_xor_blob",  "blob_year_eq",  "blob_year_ge",  "blob_year_gt",  "blob_year_le",
        "blob_year_lt",  "blob_year_ne",  "b_mod",  "b_not_between_and",  "b_not_sym_between_and",  "bool",
        "bool_and",  "booland_statefunc",  "bool_bin_concat",  "bool_bit_concat",  "bool_bit_ge",  "bool_bit_gt",
        "bool_bit_le",  "bool_bit_lt",  "boolboollike",  "boolboolnlike",  "bool_cast_date",  "bool_cast_datetime",
        "bool_cast_time",  "bool_cast_timestamptz",  "bool_concat",  "bool_date_xor",  "boolean_binary_eq",
        "boolean_binary_ge",  "boolean_binary_gt",  "boolean_binary_le",  "boolean_binary_lt",  "boolean_binary_ne",
        "boolean_blob_eq",  "boolean_blob_ge",  "boolean_blob_gt",  "boolean_blob_le",  "boolean_blob_lt",
        "boolean_blob_ne",  "boolean_date",  "boolean_datetime",  "boolean_longblob_eq",  "boolean_longblob_ge",
        "boolean_longblob_gt",  "boolean_longblob_le",  "boolean_longblob_lt",  "boolean_longblob_ne",
        "boolean_mediumblob_eq",  "boolean_mediumblob_ge",  "boolean_mediumblob_gt",  "boolean_mediumblob_le",
        "boolean_mediumblob_lt",  "boolean_mediumblob_ne",  "boolean_time",  "boolean_timestamptz",
        "boolean_tinyblob_eq",  "boolean_tinyblob_ge",  "boolean_tinyblob_gt",  "boolean_tinyblob_le",
        "boolean_tinyblob_lt",  "boolean_tinyblob_ne",  "boolean_year",  "bool_enum",  "booleq",  "bool_float8_xor",
        "boolge",  "boolgt",  "boolin",  "bool_int1",  "bool_int2",  "bool_int8",  "bool_larger",  "boolle",
        "boollt",  "boolne",  "bool_or",  "boolor_statefunc",  "boolout",  "boolrecv",  "boolsend",  "bool_smaller",
        "bool_text",  "bool_text_concat",  "booltextlike",  "booltextnlike",  "bool_timestamptz_xor",  "booltobit",
        "booltofloat4",  "booltofloat8",  "bool_uint1",  "bool_uint2",  "bool_uint4",  "bool_uint8",  "boolum",
        "bool_varbin_concat",  "boolxor",  "bool_xor_uint1",  "bool_xor_uint2",  "bool_xor_uint4",  "bool_xor_uint8",
        "box",  "box_above",  "box_above_eq",  "box_add",  "box_below",  "box_below_eq",  "box_center",
        "box_contain",  "box_contained",  "box_contain_pt",  "box_distance",  "box_div",  "box_eq",  "box_ge",
        "box_gt",  "box_in",  "box_intersect",  "box_le",  "box_left",  "box_lt",  "box_mul",  "box_out",
        "box_overabove",  "box_overbelow",  "box_overlap",  "box_overleft",  "box_overright",  "box_recv",
        "box_right",  "box_same",  "box_send",  "box_sub",  "bpchar",  "bpcharbinarylike",  "bpcharbinarynlike",
        "bpchar_cast_int8",  "bpcharcmp",  "bpchar_date",  "bpchar_enum",  "bpchareq",  "bpchar_float4",
        "bpchar_float8",  "bpcharge",  "bpchargt",  "bpchariclike",  "bpcharicnlike",  "bpcharicregexeq",
        "bpcharicregexne",  "bpcharin",  "bpchar_int1",  "bpchar_int2",  "bpchar_int4",  "bpchar_int8",
        "bpchar_larger",  "bpcharle",  "bpcharlike",  "bpcharlt",  "bpcharne",  "bpcharnlike",  "bpchar_numeric",
        "bpcharout",  "bpchar_pattern_ge",  "bpchar_pattern_gt",  "bpchar_pattern_le",  "bpchar_pattern_lt",
        "bpcharrecv",  "bpcharregexeq",  "bpcharregexne",  "bpcharsend",  "bpchar_smaller",  "bpchar_sortsupport",
        "bpchar_text",  "bpchar_time",  "bpchar_timestamp",  "bpchar_timestamptz",  "bpchar_to_smalldatetime",
        "bpchartypmodin",  "bpchartypmodout",  "bpchar_uint1",  "bpchar_uint2",  "bpchar_uint4",  "bpchar_uint8",
        "b_plpgsql_call_handler",  "b_plpgsql_inline_handler",  "b_plpgsql_validator",  "broadcast",
        "b_sym_between_and",  "btabstimecmp",  "btarraycmp",  "btbeginscan",  "btboolcmp",  "btbpchar_pattern_cmp",
        "btbuild",  "btbuildempty",  "btbulkdelete",  "btcanreturn",  "btcharcmp",  "btcostestimate",  "btendscan",
        "btequalimage",  "btfloat48cmp",  "btfloat4cmp",  "btfloat4sortsupport",  "btfloat84cmp",  "btfloat8cmp",
        "btfloat8sortsupport",  "btgetbitmap",  "btgettuple",  "b_timestampdiff",  "btinsert",  "btint24cmp",
        "btint28cmp",  "btint2cmp",  "btint2setcmp",  "btint2sortsupport",  "btint42cmp",  "btint48cmp",  "btint4cmp",
        "btint4setcmp",  "btint4sortsupport",  "btint82cmp",  "btint84cmp",  "btint8cmp",  "btint8setcmp",
        "btint8sortsupport",  "btmarkpos",  "btmerge",  "btnamecmp",  "btnamesortsupport",  "btoidcmp",
        "btoidsortsupport",  "btoidvectorcmp",  "btoptions",  "btrecordcmp",  "btreltimecmp",  "btrescan",
        "btrestrpos",  "btrim",  "btsetcmp",  "btsetint2cmp",  "btsetint4cmp",  "btsetint8cmp",  "btsetsortsupport",
        "bttextcmp",  "bttext_pattern_cmp",  "bttextsortsupport",  "bttidcmp",  "bttintervalcmp",  "btvacuumcleanup",
        "btvarstrequalimage",  "bucketabstime",  "bucketbool",  "bucketbpchar",  "bucketbytea",  "bucketcash",
        "bucketchar",  "bucketdate",  "bucketfloat4",  "bucketfloat8",  "bucketint1",  "bucketint2",
        "bucketint2vector",  "bucketint4",  "bucketint8",  "bucketinterval",  "bucketname",  "bucketnumeric",
        "bucketnvarchar2",  "bucketoid",  "bucketoidvector",  "bucketraw",  "bucketreltime",  "bucketsmalldatetime",
        "buckettext",  "buckettime",  "buckettimestamp",  "buckettimestamptz",  "buckettimetz",  "bucketuuid",
        "bucketvarchar",  "bytea_any_value",  "byteacat",  "byteacmp",  "byteaeq",  "byteage",  "byteagt",  "byteain",
        "byteale",  "bytealike",  "bytealt",  "byteane",  "byteanlike",  "byteaout",  "bytearecv",  "byteasend",
        "bytea_sortsupport",  "bytea_string_agg_finalfn",  "bytea_string_agg_transfn",  "byteawithoutordercolin",
        "byteawithoutordercolout",  "byteawithoutordercolrecv",  "byteawithoutordercolsend",
        "byteawithoutorderwithequalcolcmp",  "byteawithoutorderwithequalcolcmpbyteal",
        "byteawithoutorderwithequalcolcmpbytear",  "byteawithoutorderwithequalcoleq",
        "byteawithoutorderwithequalcoleqbyteal",  "byteawithoutorderwithequalcoleqbytear",
        "byteawithoutorderwithequalcolin",  "byteawithoutorderwithequalcolne",
        "byteawithoutorderwithequalcolnebyteal",  "byteawithoutorderwithequalcolnebytear",
        "byteawithoutorderwithequalcolout",  "byteawithoutorderwithequalcolrecv",
        "byteawithoutorderwithequalcolsend",  "byteawithoutorderwithequalcoltypmodin",
        "byteawithoutorderwithequalcoltypmodout",  "calculate_coverage",  "calculate_quantile_of",
        "calculate_value_at",  "capture_view_to_json",  "cash_cmp",  "cash_div_cash",  "cash_div_flt4",
        "cash_div_flt8",  "cash_div_int1",  "cash_div_int2",  "cash_div_int4",  "cash_div_int8",  "cash_eq",
        "cash_ge",  "cash_gt",  "cash_in",  "cashlarger",  "cash_le",  "cash_lt",  "cash_mi",  "cash_mul_flt4",
        "cash_mul_flt8",  "cash_mul_int1",  "cash_mul_int2",  "cash_mul_int4",  "cash_mul_int8",  "cash_ne",
        "cash_out",  "cash_pl",  "cash_recv",  "cash_send",  "cashsmaller",  "cash_uint",  "cash_words",
        "cast_to_unsigned",  "cbrt",  "cbtreebuild",  "cbtreecanreturn",  "cbtreecostestimate",  "cbtreegetbitmap",
        "cbtreegettuple",  "cbtreeoptions",  "ceil",  "ceiling",  "center",  "cginbuild",  "cgingetbitmap",  "char",
        "chara",  "character_length",  "char_bool",  "char_cast_ui1",  "char_cast_ui2",  "char_cast_ui4",
        "char_cast_ui8",  "chareq",  "charge",  "chargt",  "charin",  "charle",  "char_length",  "charlt",  "charne",
        "charout",  "charrecv",  "charsend",  "char_year",  "check_engine_status",  "checksum",
        "checksumtext_agg_transfn",  "chr",  "cideq",  "cidin",  "cidout",  "cidr",  "cidrecv",  "cidr_in",
        "cidr_out",  "cidr_recv",  "cidr_send",  "cidsend",  "circle",  "circle_above",  "circle_add_pt",
        "circle_below",  "circle_center",  "circle_contain",  "circle_contained",  "circle_contain_pt",
        "circle_distance",  "circle_div_pt",  "circle_eq",  "circle_ge",  "circle_gt",  "circle_in",  "circle_le",
        "circle_left",  "circle_lt",  "circle_mul_pt",  "circle_ne",  "circle_out",  "circle_overabove",
        "circle_overbelow",  "circle_overlap",  "circle_overleft",  "circle_overright",  "circle_recv",
        "circle_right",  "circle_same",  "circle_send",  "circle_sub_pt",  "clean",  "clean_workload",
        "clear_all_invalid_locks",  "clock_timestamp",  "close_lb",  "close_ls",  "close_lseg",  "close_pb",
        "close_pl",  "close_ps",  "close_sb",  "close_sl",  "col_description",  "comm_check_connection_status",
        "comm_client_info",  "complex_array_in",  "compress",  "compress_address_details",  "compress_address_header",
        "compress_buffer_stat_info",  "compress_ratio_info",  "compress_statistic_info",  "concat",  "concat_ws",
        "connect_by_root",  "connection_id",  "continue",  "contjoinsel",  "contsel",  "conv",  "convert",
        "convertbdouble",  "convertblob",  "convertchar",  "convertdate",  "convert_datetime_double",
        "convert_datetime_uint8",  "convert_from",  "convertnchar",  "convertnumber",  "convertnvarchar2",
        "convertraw",  "convert_text_datetime",  "convert_text_timestamptz",  "converttimestamp",
        "converttimestamptz",  "convert_timestamptz_double",  "convert_timestamptz_uint8",  "convert_to",
        "convert_to_nocase",  "convert_tz",  "convertvarchar",  "convertvarchar2",  "copy_error_log_create",
        "copy_summary_create",  "corr",  "corr_k",  "corr_k_final_fn",  "corr_s",  "corr_s_final_fn",
        "corr_sk_trans_fn",  "corr_sk_trans_fn_no3",  "cos",  "cosine_distance",  "cot",  "count",  "covar_pop",
        "covar_samp",  "coverage_arrays",  "crc32",  "create_abort_sql_patch",  "create_hint_sql_patch",
        "create_snapshot",  "create_snapshot_internal",  "create_wdr_snapshot",
        "create_wlm_instance_statistics_info",  "create_wlm_operator_info",  "create_wlm_session_info",  "createxml",
        "creditcardmasking",  "cstore_tid_out",  "cstring_in",  "cstring_out",  "cstring_recv",  "cstring_send",
        "cume_dist",  "cume_dist_final",  "cupointer_bigint",  "curdate",  "current_database",  "current_query",
        "current_schema",  "current_schemas",  "current_setting",  "current_user",  "currtid",  "currtid2",
        "currval",  "cursor_to_xml",  "cursor_to_xmlschema",  "database",  "database_to_xml",
        "database_to_xml_and_xmlschema",  "database_to_xmlschema",  "datalength",  "date",  "date2float4",
        "date2float8",  "date2int1",  "date2int2",  "date2uint1",  "date2uint2",  "date2uint4",  "date2uint8",
        "date_add",  "date_add_interval",  "date_add_numeric",  "date_agg_finalfn",  "date_any_value",  "date_bit_ge",
        "date_bit_gt",  "date_bit_le",  "date_bit_lt",  "date_blob_xor",  "date_bool",  "date_bool_xor",
        "date_bpchar",  "date_cast",  "date_cast_datetime",  "date_cast_int8",  "date_cast_timestamptz",  "date_cmp",
        "date_cmp_timestamp",  "date_cmp_timestamptz",  "datediff",  "date_enum",  "date_eq",  "date_eq_timestamp",
        "date_eq_timestamptz",  "date_float8_xor",  "date_format",  "date_ge",  "date_ge_timestamp",
        "date_ge_timestamptz",  "date_gt",  "date_gt_timestamp",  "date_gt_timestamptz",  "date_in",  "date_int",
        "date_int8",  "date_int8_xor",  "date_json",  "date_larger",  "date_le",  "date_le_timestamp",
        "date_le_timestamptz",  "date_list_agg_noarg2_transfn",  "date_list_agg_transfn",  "date_lt",
        "date_lt_timestamp",  "date_lt_timestamptz",  "date_mi",  "date_mii",  "date_mi_interval",  "date_ne",
        "date_ne_timestamp",  "date_ne_timestamptz",  "date_numeric",  "date_out",  "date_part",  "date_pli",
        "date_pl_interval",  "daterange",  "daterange_canonical",  "daterange_subdiff",  "date_recv",  "date_send",
        "date_smaller",  "date_sortsupport",  "date_sub",  "date_sub_numeric",  "date_text",  "date_text_xor",
        "date_time",  "datetime_bigint",  "datetime_blob_cmp",  "datetime_blob_eq",  "datetime_blob_ge",
        "datetime_blob_gt",  "datetime_blob_le",  "datetime_blob_lt",  "datetime_blob_ne",  "datetime_div_numeric",
        "datetime_double_eq",  "datetime_double_ge",  "datetime_double_gt",  "datetime_double_le",
        "datetime_double_lt",  "datetime_double_ne",  "datetime_enum",  "datetime_float",  "datetime_float4",
        "datetime_int1",  "datetime_int2",  "datetime_int4",  "datetime_int8_xor",  "datetime_json",
        "datetime_longblob_cmp",  "datetime_longblob_eq",  "datetime_longblob_ge",  "datetime_longblob_gt",
        "datetime_longblob_le",  "datetime_longblob_lt",  "datetime_longblob_ne",  "datetime_mediumblob_cmp",
        "datetime_mediumblob_eq",  "datetime_mediumblob_ge",  "datetime_mediumblob_gt",  "datetime_mediumblob_le",
        "datetime_mediumblob_lt",  "datetime_mediumblob_ne",  "datetime_mi_float",  "datetime_mi_int4",
        "datetime_mul_int4",  "datetime_mul_numeric",  "datetime_pl",  "datetime_pl_float",  "datetime_pl_int4",
        "datetime_text_eq",  "datetime_text_ge",  "datetime_text_gt",  "datetime_text_le",  "datetime_text_lt",
        "datetime_text_ne",  "datetime_tinyblob_cmp",  "datetime_tinyblob_eq",  "datetime_tinyblob_ge",
        "datetime_tinyblob_gt",  "datetime_tinyblob_le",  "datetime_tinyblob_lt",  "datetime_tinyblob_ne",
        "datetimetz_pl",  "datetime_uint1",  "datetime_uint2",  "datetime_uint4",  "datetime_uint8",
        "datetime_uint8_eq",  "datetime_uint8_ge",  "datetime_uint8_gt",  "datetime_uint8_le",  "datetime_uint8_lt",
        "datetime_uint8_ne",  "date_time_xor",  "datetime_year",  "date_trunc",  "date_uint1_eq",  "date_uint1_ge",
        "date_uint1_gt",  "date_uint1_le",  "date_uint1_lt",  "date_uint1_ne",  "date_uint2_eq",  "date_uint2_ge",
        "date_uint2_gt",  "date_uint2_le",  "date_uint2_lt",  "date_uint2_ne",  "date_varchar",  "datexor",
        "date_xor_transfn",  "date_year",  "day",  "dayname",  "dayofmonth",  "dayofweek",  "dayofyear",
        "db4ai_predict_by_bool",  "db4ai_predict_by_float4",  "db4ai_predict_by_float8",
        "db4ai_predict_by_float8_array",  "db4ai_predict_by_int32",  "db4ai_predict_by_int64",
        "db4ai_predict_by_numeric",  "db4ai_predict_by_text",  "db_b_format",  "dblink",  "dblink_build_sql_delete",
        "dblink_build_sql_insert",  "dblink_build_sql_update",  "dblink_cancel_query",  "dblink_close",
        "dblink_connect",  "dblink_connect_u",  "dblink_current_query",  "dblink_disconnect",  "dblink_error_message",
        "dblink_exec",  "dblink_fetch",  "dblink_get_connections",  "dblink_get_drivername",  "dblink_get_notify",
        "dblink_get_pkey",  "dblink_get_result",  "dblink_is_busy",  "dblink_open",  "dblink_send_query",  "dcbrt",
        "decode",  "defined",  "definer_current_user",  "degrees",  "delete",  "delete_breakpoint",  "delta",
        "dense_rank",  "dense_rank_final",  "dexp",  "diagonal",  "diameter",  "disable_breakpoint",  "disable_conn",
        "disable_sql_patch",  "dispell_init",  "dispell_lexize",  "dist_cpoly",  "dist_fdw_handler",
        "dist_fdw_validator",  "dist_lb",  "dist_pb",  "dist_pc",  "dist_pl",  "dist_ppath",  "dist_ps",  "dist_sb",
        "dist_sl",  "div",  "dlog1",  "dlog10",  "dolphin_attname_eq",  "dolphin_bitnot",  "dolphin_boolnot",
        "dolphin_charnot",  "dolphin_datenot",  "dolphin_enumnot",  "dolphin_float48div",  "dolphin_float48mi",
        "dolphin_float48mul",  "dolphin_float48pl",  "dolphin_float4div",  "dolphin_float4mi",  "dolphin_float4mul",
        "dolphin_float4not",  "dolphin_float4pl",  "dolphin_float84div",  "dolphin_float84mi",  "dolphin_float84mul",
        "dolphin_float84pl",  "dolphin_float8not",  "dolphin_int1div",  "dolphin_int1_div_uint1",  "dolphin_int1mi",
        "dolphin_int1_mi_uint1",  "dolphin_int1mul",  "dolphin_int1_mul_uint1",  "dolphin_int1not",  "dolphin_int1pl",
        "dolphin_int1_pl_uint1",  "dolphin_int24div",  "dolphin_int24mi",  "dolphin_int24mul",  "dolphin_int24pl",
        "dolphin_int28div",  "dolphin_int28mi",  "dolphin_int28mul",  "dolphin_int28pl",  "dolphin_int2div",
        "dolphin_int2_div_uint2",  "dolphin_int2mi",  "dolphin_int2_mi_uint2",  "dolphin_int2mul",
        "dolphin_int2_mul_uint2",  "dolphin_int2not",  "dolphin_int2pl",  "dolphin_int2_pl_uint2",
        "dolphin_int42div",  "dolphin_int42mi",  "dolphin_int42mul",  "dolphin_int42pl",  "dolphin_int48div",
        "dolphin_int48mi",  "dolphin_int48mul",  "dolphin_int48pl",  "dolphin_int4div",  "dolphin_int4_div_uint4",
        "dolphin_int4mi",  "dolphin_int4_mi_uint4",  "dolphin_int4mul",  "dolphin_int4_mul_uint4",  "dolphin_int4not",
        "dolphin_int4pl",  "dolphin_int4_pl_uint4",  "dolphin_int82div",  "dolphin_int82mi",  "dolphin_int82mul",
        "dolphin_int82pl",  "dolphin_int84div",  "dolphin_int84mi",  "dolphin_int84mul",  "dolphin_int84pl",
        "dolphin_int8div",  "dolphin_int8_div_uint8",  "dolphin_int8mi",  "dolphin_int8_mi_uint8",  "dolphin_int8mul",
        "dolphin_int8_mul_uint8",  "dolphin_int8not",  "dolphin_int8pl",  "dolphin_int8_pl_uint8",  "dolphin_invoke",
        "dolphin_numericnot",  "dolphin_setnot",  "dolphin_textnot",  "dolphin_timenot",  "dolphin_timestampnot",
        "dolphin_timestamptznot",  "dolphin_types",  "dolphin_uint1div",  "dolphin_uint1_div_int1",
        "dolphin_uint1mi",  "dolphin_uint1_mi_int1",  "dolphin_uint1mul",  "dolphin_uint1_mul_int1",
        "dolphin_uint1not",  "dolphin_uint1pl",  "dolphin_uint1_pl_int1",  "dolphin_uint2div",
        "dolphin_uint2_div_int2",  "dolphin_uint2mi",  "dolphin_uint2_mi_int2",  "dolphin_uint2mul",
        "dolphin_uint2_mul_int2",  "dolphin_uint2not",  "dolphin_uint2pl",  "dolphin_uint2_pl_int2",
        "dolphin_uint4div",  "dolphin_uint4_div_int4",  "dolphin_uint4mi",  "dolphin_uint4_mi_int4",
        "dolphin_uint4mul",  "dolphin_uint4_mul_int4",  "dolphin_uint4not",  "dolphin_uint4pl",
        "dolphin_uint4_pl_int4",  "dolphin_uint8div",  "dolphin_uint8_div_int8",  "dolphin_uint8_mi_int8",
        "dolphin_uint8_mul_int8",  "dolphin_uint8not",  "dolphin_uint8_pl_int8",  "dolphin_varcharnot",
        "dolphin_varlenanot",  "dolphin_version",  "dolphin_yearnot",  "domain_in",  "domain_recv",
        "double_any_value",  "double_or_binary",  "dpow",  "drop_sql_patch",  "dround",  "dsimple_init",
        "dsimple_lexize",  "dsnowball_init",  "dsnowball_lexize",  "dsqrt",  "dss_io_stat",  "dsynonym_init",
        "dsynonym_lexize",  "dtrunc",  "dynamic_func_control",  "each",  "elem_contained_by_range",  "elt",
        "empty_blob",  "enable_breakpoint",  "enable_sql_patch",  "encode",  "encode_feature_perf_hist",
        "encode_plan_node",  "end_collect_workload",  "endcreate",  "enum_bit",  "enum_boolean",  "enum_cmp",
        "enum_date",  "enum_eq",  "enum_first",  "enum_float4",  "enum_float8",  "enum_ge",  "enum_gt",  "enum_in",
        "enum_int1",  "enum_int2",  "enum_int4",  "enum_int8",  "enum_json",  "enum_larger",  "enum_last",  "enum_le",
        "enum_lt",  "enum_ne",  "enum_numeric",  "enum_out",  "enum_range",  "enum_recv",  "enum_send",  "enum_set",
        "enum_smaller",  "enumtexteq",  "enumtextge",  "enumtextgt",  "enumtextle",  "enumtext_like",  "enumtextlt",
        "enumtextne",  "enumtext_nlike",  "enum_time",  "enum_timestamp",  "enum_timestamptz",  "enum_uint1",
        "enum_uint2",  "enum_uint4",  "enum_uint8",  "enum_varlena",  "enum_year",  "eqjoinsel",  "eqsel",
        "euc_cn_to_mic",  "euc_cn_to_utf8",  "euc_jis_2004_to_shift_jis_2004",  "euc_jis_2004_to_utf8",
        "euc_jp_to_mic",  "euc_jp_to_sjis",  "euc_jp_to_utf8",  "euc_kr_to_mic",  "euc_kr_to_utf8",  "euc_tw_to_big5",
        "euc_tw_to_mic",  "euc_tw_to_utf8",  "event_trigger_in",  "event_trigger_out",  "every",  "exec_hadoop_sql",
        "exec_on_extension",  "exist",  "exists_all",  "exists_any",  "existsnode",  "exp",  "export_set",
        "extract_internal",  "extractvalue",  "f4_cast_ui1",  "f4_cast_ui2",  "f4_cast_ui4",  "f4_cast_ui8",
        "f4toi1",  "f4toui1",  "f4toui2",  "f4toui4",  "f4toui8",  "f8_cast_ui1",  "f8_cast_ui2",  "f8_cast_ui4",
        "f8_cast_ui8",  "f8toi1",  "f8toui1",  "f8toui2",  "f8toui4",  "f8toui8",  "factorial",  "family",
        "fdw_handler_in",  "fdw_handler_out",  "fenced_udf_process",  "fetchval",  "field",  "file_fdw_handler",
        "file_fdw_validator",  "find_in_set",  "finish",  "first",  "first_transition",  "first_value",  "float4",
        "float48div",  "float48eq",  "float48ge",  "float48gt",  "float48le",  "float48lt",  "float48mi",
        "float48mul",  "float48ne",  "float48pl",  "float4abs",  "float4_accum",  "float4_b_format_date",
        "float4_b_format_datetime",  "float4_b_format_time",  "float4_b_format_timestamp",  "float4_bin_concat",
        "float4_bool",  "float4_bpchar",  "float4_cast_date",  "float4_cast_datetime",  "float4_cast_int8",
        "float4_cast_time",  "float4_cast_timestamptz",  "float4div",  "float4_enum",  "float4eq",  "float4ge",
        "float4gt",  "float4in",  "float4_json",  "float4larger",  "float4le",  "float4_list_agg_noarg2_transfn",
        "float4_list_agg_transfn",  "float4lt",  "float4mi",  "float4mul",  "float4ne",  "float4_nvarchar2",
        "float4out",  "float4pl",  "float4recv",  "float4send",  "float4smaller",  "float4_text",  "float4um",
        "float4up",  "float4_varbin_concat",  "float4_varchar",  "float4_year",  "float8",  "float84div",
        "float84eq",  "float84ge",  "float84gt",  "float84le",  "float84lt",  "float84mi",  "float84mul",
        "float84ne",  "float84pl",  "float8abs",  "float8_accum",  "float8_avg",  "float8_b_format_date",
        "float8_b_format_datetime",  "float8_b_format_time",  "float8_b_format_timestamp",  "float8_bin_concat",
        "float8_blob_cmp",  "float8_blob_eq",  "float8_blob_ge",  "float8_blob_gt",  "float8_blob_le",
        "float8_blob_lt",  "float8_blob_ne",  "float8_bool",  "float8_bool_xor",  "float8_bpchar",
        "float8_cast_date",  "float8_cast_datetime",  "float8_cast_int8",  "float8_cast_time",
        "float8_cast_timestamptz",  "float8_collect",  "float8_corr",  "float8_covar_pop",  "float8_covar_samp",
        "float8_date_xor",  "float8div",  "float8_enum",  "float8eq",  "float8ge",  "float8gt",  "float8in",
        "float8_interval",  "float8_json",  "float8larger",  "float8le",  "float8_list_agg_noarg2_transfn",
        "float8_list_agg_transfn",  "float8_longblob_cmp",  "float8_longblob_eq",  "float8_longblob_ge",
        "float8_longblob_gt",  "float8_longblob_le",  "float8_longblob_lt",  "float8_longblob_ne",  "float8lt",
        "float8_mediumblob_cmp",  "float8_mediumblob_eq",  "float8_mediumblob_ge",  "float8_mediumblob_gt",
        "float8_mediumblob_le",  "float8_mediumblob_lt",  "float8_mediumblob_ne",  "float8mi",  "float8mul",
        "float8ne",  "float8_nvarchar2",  "float8out",  "float8pl",  "float8recv",  "float8_regr_accum",
        "float8_regr_avgx",  "float8_regr_avgy",  "float8_regr_collect",  "float8_regr_intercept",  "float8_regr_r2",
        "float8_regr_slope",  "float8_regr_sxx",  "float8_regr_sxy",  "float8_regr_syy",  "float8send",
        "float8smaller",  "float8_stddev_pop",  "float8_stddev_samp",  "float8_sum",  "float8_text",
        "float8_timestamptz_xor",  "float8_timestamp_xor",  "float8_tinyblob_cmp",  "float8_tinyblob_eq",
        "float8_tinyblob_ge",  "float8_tinyblob_gt",  "float8_tinyblob_le",  "float8_tinyblob_lt",
        "float8_tinyblob_ne",  "float8_to_interval",  "float8um",  "float8up",  "float8_varbin_concat",
        "float8_varchar",  "float8_var_pop",  "float8_var_samp",  "float8_year",  "float_any_value",  "float_sum",
        "floor",  "flt4_mul_cash",  "flt8_mul_cash",  "fmgr_c_validator",  "fmgr_internal_validator",
        "fmgr_sql_validator",  "format",  "format_type",  "from_base64",  "from_days",  "from_unixtime",
        "fullemailmasking",  "gather_encoding_info",  "gb18030_2022_to_utf8",  "gb18030_to_utf8",  "gbk_to_utf8",
        "generate_procoverage_report",  "generate_series",  "generate_subscripts",  "generate_wdr_report",
        "get_all_locks",  "get_analyzed_result",  "getanydatasetexcept",  "getbdouble",  "get_bit",  "getblob",
        "getbucket",  "get_byte",  "getchar",  "get_client_info",  "getcount",  "get_current_ts_config",
        "getdatabaseencoding",  "getdate",  "get_db_source_datasize",  "getdistributekey",  "get_distribution_key",
        "get_dn_hist_relhash",  "get_format",  "get_global_bgwriter_stat",  "get_global_config_settings",
        "get_global_file_iostat",  "get_global_file_redo_iostat",  "get_global_full_sql_by_timestamp",
        "get_global_instance_time",  "get_global_locks",  "get_global_memory_node_detail",
        "get_global_operator_history",  "get_global_operator_history_table",  "get_global_operator_runtime",
        "get_global_os_runtime",  "get_global_os_threads",  "get_global_record_reset_time",  "get_global_rel_iostat",
        "get_global_replication_slots",  "get_global_replication_stat",  "get_global_session_memory",
        "get_global_session_memory_detail",  "get_global_session_stat",  "get_global_session_stat_activity",
        "get_global_session_time",  "get_global_shared_memory_detail",  "get_global_slow_sql_by_timestamp",
        "get_global_stat_all_indexes",  "get_global_stat_all_tables",  "get_global_stat_bad_block",
        "get_global_stat_database",  "get_global_stat_database_conflicts",  "get_global_statement_complex_history",
        "get_global_statement_complex_history_table",  "get_global_statement_complex_runtime",
        "get_global_statement_count",  "get_global_statio_all_indexes",  "get_global_statio_all_sequences",
        "get_global_statio_all_tables",  "get_global_statio_sys_indexes",  "get_global_statio_sys_sequences",
        "get_global_statio_sys_tables",  "get_global_statio_user_indexes",  "get_global_statio_user_sequences",
        "get_global_statio_user_tables",  "get_global_stat_sys_indexes",  "get_global_stat_sys_tables",
        "get_global_stat_user_functions",  "get_global_stat_user_indexes",  "get_global_stat_user_tables",
        "get_global_stat_xact_all_tables",  "get_global_stat_xact_sys_tables",  "get_global_stat_xact_user_functions",
        "get_global_stat_xact_user_tables",  "get_global_thread_wait_status",
        "get_global_transactions_prepared_xacts",  "get_global_transactions_running_xacts",
        "get_global_user_transaction",  "get_global_wait_events",  "get_global_workload_transaction",
        "get_gtm_lite_status",  "get_hostname",  "get_index_columns",  "getinfo",  "get_instr_rt_percentile",
        "get_instr_unique_sql",  "get_instr_user_login",  "get_instr_wait_event",  "get_instr_workload_info",
        "get_local_active_session",  "get_local_prepared_xact",  "get_local_rel_iostat",
        "get_local_toastname_and_toastindexname",  "get_local_toast_relation",  "get_lock",  "getnchar",
        "get_nodename",  "get_node_stat_reset_time",  "getnumber",  "getnvarchar2",  "get_paxos_replication_info",
        "getpgusername",  "get_prepared_pending_xid",  "getraw",  "get_remote_prepared_xacts",  "get_schema_oid",
        "get_stat_db_cu",  "get_statement_history",  "get_statement_responsetime_percentile",  "getstringval",
        "get_summary_stat_all_indexes",  "get_summary_stat_all_tables",  "get_summary_statement",
        "get_summary_statio_all_indexes",  "get_summary_statio_all_tables",  "get_summary_statio_sys_indexes",
        "get_summary_statio_sys_tables",  "get_summary_statio_user_indexes",  "get_summary_statio_user_tables",
        "get_summary_stat_sys_indexes",  "get_summary_stat_sys_tables",  "get_summary_stat_user_indexes",
        "get_summary_stat_user_tables",  "get_summary_stat_xact_all_tables",  "get_summary_stat_xact_sys_tables",
        "get_summary_stat_xact_user_tables",  "get_summary_transactions_prepared_xacts",
        "get_summary_transactions_running_xacts",  "get_summary_user_login",  "get_summary_workload_sql_count",
        "get_summary_workload_sql_elapse_time",  "gettimestamp",  "gettimestamptz",  "gettype",  "gettypename",
        "getvarchar",  "getvarchar2",  "get_wait_event_info",  "ginarrayconsistent",  "ginarrayextract",
        "ginarraytriconsistent",  "ginbeginscan",  "ginbuild",  "ginbuildempty",  "ginbulkdelete",
        "gin_clean_pending_list",  "gin_cmp_prefix",  "gin_cmp_tslexeme",  "gin_compare_jsonb",
        "gin_consistent_jsonb",  "gin_consistent_jsonb_hash",  "gincostestimate",  "ginendscan",  "gin_extract_jsonb",
        "gin_extract_jsonb_hash",  "gin_extract_jsonb_query",  "gin_extract_jsonb_query_hash",  "gin_extract_tsquery",
        "gin_extract_tsvector",  "gingetbitmap",  "gininsert",  "ginmarkpos",  "ginmerge",  "ginoptions",
        "ginqueryarrayextract",  "ginrescan",  "ginrestrpos",  "gin_triconsistent_jsonb",
        "gin_triconsistent_jsonb_hash",  "gin_tsquery_consistent",  "gin_tsquery_triconsistent",  "ginvacuumcleanup",
        "gistbeginscan",  "gist_box_compress",  "gist_box_consistent",  "gist_box_decompress",  "gist_box_penalty",
        "gist_box_picksplit",  "gist_box_same",  "gist_box_union",  "gistbuild",  "gistbuildempty",  "gistbulkdelete",
        "gist_circle_compress",  "gist_circle_consistent",  "gistcostestimate",  "gistendscan",  "gistgetbitmap",
        "gistgettuple",  "gistinsert",  "gistmarkpos",  "gistmerge",  "gistoptions",  "gist_point_compress",
        "gist_point_consistent",  "gist_point_distance",  "gist_poly_compress",  "gist_poly_consistent",
        "gistrescan",  "gistrestrpos",  "gistvacuumcleanup",  "global_clean_prepared_xacts",
        "global_comm_get_client_info",  "global_comm_get_recv_stream",  "global_comm_get_send_stream",
        "global_comm_get_status",  "global_slow_query_history",  "global_slow_query_info",
        "global_slow_query_info_bytime",  "global_space_shrink",  "global_stat_clean_hotkeys",
        "global_stat_get_hotkeys_info",  "global_threadpool_status",  "group_concat",  "group_concat_finalfn",
        "group_concat_transfn",  "gs_all_control_group_info",  "gs_all_nodegroup_control_group_info",
        "gs_block_dw_io",  "gs_catalog_attribute_records",  "gs_cgroup_map_ng_conf",  "gs_comm_proxy_thread_status",
        "gs_control_group_info",  "gs_create_log_tables",  "gs_current_xlog_insert_end_location",  "gs_decrypt",
        "gs_decrypt_aes128",  "gs_deployment",  "gs_download_obs_file",  "gs_encrypt",  "gs_encrypt_aes128",
        "gs_explain_model",  "gs_extend_library",  "gs_fault_inject",  "gs_get_active_archiving_standby",
        "gs_get_control_group_info",  "gs_get_global_barriers_status",  "gs_get_global_barrier_status",
        "gs_get_hadr_key_cn",  "gs_get_hba_conf",  "gs_get_history_memory_detail",  "gs_get_local_barrier_status",
        "gs_get_next_xid_csn",  "gs_get_nodegroup_tablecount",  "gs_get_obs_file_context",
        "gs_get_parallel_decode_status",  "gs_get_preparse_location",  "gs_get_recv_locations",
        "gs_get_schemadef_name",  "gs_get_session_memctx_detail",  "gs_get_shared_memctx_detail",
        "gs_get_standby_cluster_barrier_status",  "gs_get_thread_memctx_detail",  "gs_get_viewdef_name",
        "gs_get_viewdef_oid",  "gs_gsc_catalog_detail",  "gs_gsc_clean",  "gs_gsc_dbstat_info",
        "gs_gsc_table_detail",  "gs_hadr_do_switchover",  "gs_hadr_has_barrier_creator",  "gs_hadr_in_recovery",
        "gs_hadr_local_rto_and_rpo_stat",  "gs_hadr_remote_rto_and_rpo_stat",  "gs_hot_standby_space_info",
        "gs_index_advise",  "gs_index_recycle_queue",  "gs_index_verify",  "gs_interval",  "gs_io_wait_status",
        "gs_is_dw_io_blocked",  "gs_is_recycle_obj",  "gs_is_recycle_object",  "gs_lwlock_status",
        "gs_master_status",  "gs_parse_page_bypath",  "gs_password_deadline",  "gs_password_notifytime",
        "gs_paxos_stat_replication",  "gs_pitr_archive_slot_force_advance",  "gs_pitr_clean_history_global_barriers",
        "gs_pitr_get_warning_for_xlog_force_recycle",  "gs_query_standby_cluster_barrier_id_exist",
        "gs_read_block_from_remote",  "gs_read_file_from_remote",  "gs_read_file_size_from_remote",
        "gs_read_segment_block_from_remote",  "gs_repair_file",  "gs_repair_page",  "gs_respool_exception_info",
        "gs_roach_disable_delay_ddl_recycle",  "gs_roach_enable_delay_ddl_recycle",  "gs_roach_stop_backup",
        "gs_roach_switch_xlog",  "gs_session_memory_detail_tp",  "gs_set_obs_delete_location",
        "gs_set_obs_delete_location_with_slotname",  "gs_set_obs_file_context",
        "gs_set_standby_cluster_target_barrier_id",  "gs_space_shrink",  "gs_stack",  "gs_stat_activity_timeout",
        "gs_stat_clean_hotkeys",  "gs_stat_get_hotkeys_info",  "gs_stat_get_wlm_plan_operator_info",  "gs_stat_undo",
        "gs_stat_ustore",  "gs_stat_wal_entrytable",  "gs_stat_walreceiver",  "gs_stat_walrecvwriter",
        "gs_stat_walsender",  "gs_streaming_dr_get_switchover_barrier",  "gs_streaming_dr_in_switchover",
        "gs_streaming_dr_service_truncation_check",  "gs_switch_relfilenode",  "gs_total_nodegroup_memory_detail",
        "gs_txid_oldestxmin",  "gs_undo_dump_parsepage_mv",  "gs_undo_dump_record",  "gs_undo_dump_xid",
        "gs_undo_meta",  "gs_undo_meta_dump_slot",  "gs_undo_meta_dump_spaces",  "gs_undo_meta_dump_zone",
        "gs_undo_record",  "gs_undo_translot",  "gs_undo_translot_dump_slot",  "gs_undo_translot_dump_xid",
        "gs_upload_obs_file",  "gs_validate_ext_listen_ip",  "gs_verify_and_tryrepair_page",  "gs_verify_data_file",
        "gs_walwriter_flush_position",  "gs_walwriter_flush_stat",  "gs_wlm_get_all_user_resource_info",
        "gs_wlm_get_resource_pool_info",  "gs_wlm_get_session_info",  "gs_wlm_get_user_info",
        "gs_wlm_get_user_session_info",  "gs_wlm_get_workload_records",  "gs_wlm_node_clean",  "gs_wlm_node_recover",
        "gs_wlm_persistent_user_resource_info",  "gs_wlm_readjust_user_space",
        "gs_wlm_readjust_user_space_through_username",  "gs_wlm_readjust_user_space_with_reset_flag",
        "gs_wlm_rebuild_user_resource_pool",  "gs_wlm_session_respool",  "gs_wlm_switch_cgroup",
        "gs_wlm_user_resource_info",  "gs_write_term_log",  "gs_xlogdump_lsn",  "gs_xlogdump_parsepage_tablepath",
        "gs_xlogdump_tablepath",  "gs_xlogdump_xid",  "gs_xlog_keepers",  "gtsquery_compress",  "gtsquery_consistent",
        "gtsquery_decompress",  "gtsquery_penalty",  "gtsquery_picksplit",  "gtsquery_same",  "gtsquery_union",
        "gtsvector_compress",  "gtsvector_consistent",  "gtsvector_decompress",  "gtsvectorin",  "gtsvectorout",
        "gtsvector_penalty",  "gtsvector_picksplit",  "gtsvector_same",  "gtsvector_union",  "hamming_distance",
        "has_any_column_privilege",  "has_any_privilege",  "has_cek_privilege",  "has_cmk_privilege",
        "has_column_privilege",  "has_database_privilege",  "has_directory_privilege",
        "has_foreign_data_wrapper_privilege",  "has_function_privilege",  "hash16in",  "hash16out",  "hash32in",
        "hash32out",  "hash_aclitem",  "hash_array",  "hashbeginscan",  "hashbpchar",  "hashbuild",  "hashbuildempty",
        "hashbulkdelete",  "hashchar",  "hashcostestimate",  "hashendscan",  "hashenum",  "hashfloat4",  "hashfloat8",
        "hashgetbitmap",  "hashgettuple",  "hashinet",  "hashinsert",  "hashint1",  "hashint2",  "hashint2vector",
        "hashint4",  "hashint8",  "hashmacaddr",  "hashmarkpos",  "hashmerge",  "hashname",  "hash_numeric",
        "hashoid",  "hashoidvector",  "hashoptions",  "hash_range",  "hashrescan",  "hashrestrpos",  "hashsetint",
        "hashsettext",  "hashtext",  "hashuint1",  "hashuint2",  "hashuint4",  "hashuint8",  "hashvacuumcleanup",
        "hashvarlena",  "has_language_privilege",  "has_nodegroup_privilege",  "has_schema_privilege",
        "has_sequence_privilege",  "has_server_privilege",  "has_table_privilege",  "has_tablespace_privilege",
        "has_type_privilege",  "height",  "hex",  "hextoraw",  "hll",  "hll_add",  "hll_add_agg",  "hll_add_rev",
        "hll_add_trans0",  "hll_add_trans1",  "hll_add_trans2",  "hll_add_trans3",  "hll_add_trans4",
        "hll_cardinality",  "hll_duplicatecheck",  "hll_empty",  "hll_eq",  "hll_expthresh",  "hll_hash_any",
        "hll_hash_bigint",  "hll_hash_boolean",  "hll_hash_bytea",  "hll_hash_byteawithoutorderwithequalcol",
        "hll_hash_integer",  "hll_hash_smallint",  "hll_hash_text",  "hll_hashval",  "hll_hashval_eq",
        "hll_hashval_in",  "hll_hashval_int4",  "hll_hashval_ne",  "hll_hashval_out",  "hll_in",  "hll_log2explicit",
        "hll_log2m",  "hll_log2sparse",  "hll_ne",  "hll_out",  "hll_pack",  "hll_print",  "hll_recv",
        "hll_regwidth",  "hll_schema_version",  "hll_send",  "hll_sparseon",  "hll_trans_in",  "hll_trans_out",
        "hll_trans_recv",  "hll_trans_send",  "hll_type",  "hll_typmod_in",  "hll_typmod_out",  "hll_union",
        "hll_union_agg",  "hll_union_collect",  "hll_union_trans",  "hnswbeginscan",  "hnsw_bit_support",
        "hnswbuild",  "hnswbuildempty",  "hnswbulkdelete",  "hnswcostestimate",  "hnswdelete",  "hnswendscan",
        "hnswgettuple",  "hnswhandler",  "hnswinsert",  "hnswoptions",  "hnswrescan",  "hnsw_sparsevec_support",
        "hnswvacuumcleanup",  "hnswvalidate",  "host",  "hostmask",  "hour",  "hs_concat",  "hs_contained",
        "hs_contains",  "hstore",  "hstore_in",  "hstore_out",  "hstore_recv",  "hstore_send",  "hstore_to_array",
        "hstore_to_matrix",  "hstore_version_diag",  "hypopg_create_index",  "hypopg_display_index",
        "hypopg_drop_index",  "hypopg_estimate_size",  "hypopg_reset_index",  "i16toi1",  "i1_cast_ui1",
        "i1_cast_ui2",  "i1_cast_ui4",  "i1_cast_ui8",  "i1tof4",  "i1tof8",  "i1toi2",  "i1toi4",  "i1toi8",
        "i1toui1",  "i1toui2",  "i1toui4",  "i1toui8",  "i2_cast_ui1",  "i2_cast_ui2",  "i2_cast_ui4",  "i2_cast_ui8",
        "i2toi1",  "i2toui1",  "i2toui2",  "i2toui4",  "i2toui8",  "i4_cast_ui1",  "i4_cast_ui2",  "i4_cast_ui4",
        "i4_cast_ui8",  "i4toi1",  "i4toui1",  "i4toui2",  "i4toui4",  "i4toui8",  "i8_cast_ui1",  "i8_cast_ui2",
        "i8_cast_ui4",  "i8_cast_ui8",  "i8toi1",  "i8toui1",  "i8toui2",  "i8toui4",  "i8toui8",  "iclikejoinsel",
        "iclikesel",  "icnlikejoinsel",  "icnlikesel",  "icregexeqjoinsel",  "icregexeqsel",  "icregexnejoinsel",
        "icregexnesel",  "inet6_aton",  "inet6_ntoa",  "inetand",  "inet_aton",  "inet_client_addr",
        "inet_client_port",  "inet_in",  "inetmi",  "inetmi_int8",  "inetnot",  "inet_ntoa",  "inetor",  "inet_out",
        "inetpl",  "inet_recv",  "inet_send",  "inet_server_addr",  "inet_server_port",  "info_breakpoints",
        "info_code",  "info_locals",  "init",  "initcap",  "inner_product",  "insert",  "instr",  "int1",  "int12cmp",
        "int12eq",  "int12ge",  "int12gt",  "int12le",  "int12lt",  "int14cmp",  "int14eq",  "int14ge",  "int14gt",
        "int14le",  "int14lt",  "int16",  "int16_b_format_date",  "int16_b_format_datetime",  "int16_b_format_time",
        "int16_b_format_timestamp",  "int16_bool",  "int16_cast_date",  "int16_cast_datetime",  "int16_cast_time",
        "int16_cast_timestamptz",  "int16div",  "int16eq",  "int16ge",  "int16gt",  "int16in",  "int16le",  "int16lt",
        "int16mi",  "int16mul",  "int16ne",  "int16out",  "int16pl",  "int16recv",  "int16send",  "int16_u1",
        "int16_u2",  "int16_u4",  "int16_u8",  "int16_year",  "int18cmp",  "int18eq",  "int18ge",  "int18gt",
        "int18le",  "int18lt",  "int1abs",  "int1_accum",  "int1and",  "int1_and_uint1",  "int1_avg_accum",
        "int1_binary_eq",  "int1_binary_ne",  "int1_bool",  "int1_bpchar",  "int1cmp",  "int1div",  "int1_div_uint1",
        "int1_enum",  "int1eq",  "int1ge",  "int1gt",  "int1in",  "int1inc",  "int1_json",  "int1larger",  "int1le",
        "int1_list_agg_noarg2_transfn",  "int1_list_agg_transfn",  "int1lt",  "int1mi",  "int1_mi_uint1",  "int1mod",
        "int1mul",  "int1_mul_cash",  "int1_mul_uint1",  "int1ne",  "int1not",  "int1_numeric",  "int1_nvarchar2",
        "int1or",  "int1_or_uint1",  "int1out",  "int1pl",  "int1_pl_uint1",  "int1recv",  "int1send",  "int1shl",
        "int1shr",  "int1smaller",  "int1_text",  "int1_typmodin",  "int1_typmodout",  "int1_uint1_eq",
        "int1_uint1_ge",  "int1_uint1_gt",  "int1_uint1_le",  "int1_uint1_lt",  "int1_uint1_mod",  "int1_uint1_ne",
        "int1um",  "int1up",  "int1_varchar",  "int1xor",  "int1_xor_uint1",  "int2",  "int24div",  "int24eq",
        "int24ge",  "int24gt",  "int24le",  "int24lt",  "int24mi",  "int24mul",  "int24ne",  "int24pl",  "int28div",
        "int28eq",  "int28ge",  "int28gt",  "int28le",  "int28lt",  "int28mi",  "int28mul",  "int28ne",  "int28pl",
        "int2abs",  "int2_accum",  "int2and",  "int2_and_uint2",  "int2_avg_accum",  "int2_bool",  "int2_bpchar",
        "int2div",  "int2_div_uint2",  "int2_enum",  "int2eq",  "int2_eq_uint1",  "int2ge",  "int2gt",  "int2in",
        "int2_json",  "int2larger",  "int2le",  "int2_list_agg_noarg2_transfn",  "int2_list_agg_transfn",  "int2lt",
        "int2mi",  "int2_mi_uint2",  "int2mod",  "int2mul",  "int2_mul_cash",  "int2_mul_uint2",  "int2ne",
        "int2not",  "int2or",  "int2_or_uint2",  "int2out",  "int2pl",  "int2_pl_uint2",  "int2recv",  "int2send",
        "int2seteq",  "int2setge",  "int2setgt",  "int2setle",  "int2setlt",  "int2setne",  "int2shl",  "int2shr",
        "int2smaller",  "int2_sum",  "int2_text",  "int2_typmodin",  "int2_typmodout",  "int2_uint2_eq",
        "int2_uint2_ge",  "int2_uint2_gt",  "int2_uint2_le",  "int2_uint2_lt",  "int2_uint2_mod",  "int2_uint2_ne",
        "int2um",  "int2up",  "int2_varchar",  "int2vectoreq",  "int2vectorin",  "int2vectorin_extend",
        "int2vectorout",  "int2vectorout_extend",  "int2vectorrecv",  "int2vectorrecv_extend",  "int2vectorsend",
        "int2vectorsend_extend",  "int2xor",  "int2_xor_uint2",  "int32_b_format_date",  "int32_b_format_datetime",
        "int32_b_format_time",  "int32_b_format_timestamp",  "int32_cast_date",  "int32_cast_datetime",
        "int32_cast_time",  "int32_cast_timestamptz",  "int32_year",  "int4",  "int42div",  "int42eq",  "int42ge",
        "int42gt",  "int42le",  "int42lt",  "int42mi",  "int42mul",  "int42ne",  "int42pl",  "int48div",  "int48eq",
        "int48ge",  "int48gt",  "int48le",  "int48lt",  "int48mi",  "int48mul",  "int48ne",  "int48pl",  "int4abs",
        "int4_accum",  "int4and",  "int4_and_uint4",  "int4_avg_accum",  "int4_bpchar",  "int4div",  "int4_div_uint4",
        "int4_enum",  "int4eq",  "int4_eq_uint1",  "int4ge",  "int4gt",  "int4in",  "int4inc",  "int4_json",
        "int4larger",  "int4le",  "int4_list_agg_noarg2_transfn",  "int4_list_agg_transfn",  "int4lt",  "int4mi",
        "int4_mi_uint4",  "int4mod",  "int4mul",  "int4_mul_cash",  "int4_mul_uint4",  "int4ne",  "int4not",
        "int4or",  "int4_or_uint4",  "int4out",  "int4pl",  "int4_pl_uint4",  "int4range",  "int4range_canonical",
        "int4range_subdiff",  "int4recv",  "int4send",  "int4seteq",  "int4setge",  "int4setgt",  "int4setle",
        "int4setlt",  "int4setne",  "int4shl",  "int4shr",  "int4smaller",  "int4_sum",  "int4_text",
        "int4_typmodin",  "int4_typmodout",  "int4_uint4_eq",  "int4_uint4_ge",  "int4_uint4_gt",  "int4_uint4_le",
        "int4_uint4_lt",  "int4_uint4_mod",  "int4_uint4_ne",  "int4um",  "int4up",  "int4_varchar",  "int4xor",
        "int4_xor_uint4",  "int64_b_format_date",  "int64_b_format_datetime",  "int64_b_format_time",
        "int64_b_format_timestamp",  "int64_cast_date",  "int64_cast_datetime",  "int64_cast_time",
        "int64_cast_timestamptz",  "int64_year",  "int8",  "int82div",  "int82eq",  "int82ge",  "int82gt",  "int82le",
        "int82lt",  "int82mi",  "int82mul",  "int82ne",  "int82pl",  "int84div",  "int84eq",  "int84ge",  "int84gt",
        "int84le",  "int84lt",  "int84mi",  "int84mul",  "int84ne",  "int84pl",  "int8abs",  "int8_accum",  "int8and",
        "int8_and_nvarchar2",  "int8_and_uint8",  "int8_and_year",  "int8_avg",  "int8_avg_accum",
        "int8_avg_accum_numeric",  "int8_avg_collect",  "int8_b_format_date",  "int8_b_format_datetime",
        "int8_b_format_time",  "int8_b_format_timestamp",  "int8_bin_concat",  "int8_blob_cmp",  "int8_blob_eq",
        "int8_blob_ge",  "int8_blob_gt",  "int8_blob_le",  "int8_blob_lt",  "int8_blob_ne",  "int8_bool",
        "int8_bpchar",  "int8_cast_date",  "int8_cast_datetime",  "int8_cast_time",  "int8_cast_timestamptz",
        "int8_cmp_uint1",  "int8_cmp_uint2",  "int8_cmp_uint4",  "int8_cmp_uint8",  "int8_datetime_xor",
        "int8_date_xor",  "int8div",  "int8_div_uint8",  "int8_enum",  "int8eq",  "int8_eq_uint1",  "int8ge",
        "int8gt",  "int8in",  "int8inc",  "int8inc_any",  "int8inc_float8_float8",  "int8_json",  "int8larger",
        "int8le",  "int8_list_agg_noarg2_transfn",  "int8_list_agg_transfn",  "int8_longblob_cmp",
        "int8_longblob_eq",  "int8_longblob_ge",  "int8_longblob_gt",  "int8_longblob_le",  "int8_longblob_lt",
        "int8_longblob_ne",  "int8lt",  "int8_mediumblob_cmp",  "int8_mediumblob_eq",  "int8_mediumblob_ge",
        "int8_mediumblob_gt",  "int8_mediumblob_le",  "int8_mediumblob_lt",  "int8_mediumblob_ne",  "int8mi",
        "int8_mi_uint8",  "int8mod",  "int8mul",  "int8_mul_cash",  "int8_mul_uint8",  "int8ne",  "int8not",
        "int8or",  "int8_or_nvarchar2",  "int8_or_uint8",  "int8_or_year",  "int8out",  "int8pl",  "int8pl_inet",
        "int8_pl_uint8",  "int8range",  "int8range_canonical",  "int8range_subdiff",  "int8recv",  "int8send",
        "int8seteq",  "int8setge",  "int8setgt",  "int8setle",  "int8setlt",  "int8setne",  "int8shl",  "int8shr",
        "int8smaller",  "int8_sum",  "int8_sum_to_int8",  "int8_text",  "int8_timestamptz_xor",  "int8_timestamp_xor",
        "int8_time_xor",  "int8_tinyblob_cmp",  "int8_tinyblob_eq",  "int8_tinyblob_ge",  "int8_tinyblob_gt",
        "int8_tinyblob_le",  "int8_tinyblob_lt",  "int8_tinyblob_ne",  "int8_typmodin",  "int8_typmodout",
        "int8_uint2_eq",  "int8_uint4_eq",  "int8_uint8_eq",  "int8_uint8_ge",  "int8_uint8_gt",  "int8_uint8_le",
        "int8_uint8_lt",  "int8_uint8_mod",  "int8_uint8_ne",  "int8um",  "int8up",  "int8_varbin_concat",
        "int8_varchar",  "int8_xor",  "int8xor",  "int8_xor_nvarchar2",  "int8_xor_uint8",  "int8_xor_year",
        "int8_year",  "int8_year_mod",  "int_bin_concat",  "integer_pl_date",  "inter_lb",  "internal_in",
        "internal_out",  "inter_sb",  "inter_sl",  "interval",  "interval_accum",  "interval_avg",  "interval_cmp",
        "interval_collect",  "interval_div",  "interval_eq",  "interval_ge",  "interval_gt",  "interval_hash",
        "interval_in",  "interval_larger",  "interval_le",  "interval_list_agg_noarg2_transfn",
        "interval_list_agg_transfn",  "interval_lt",  "interval_mi",  "interval_mul",  "interval_ne",  "interval_out",
        "interval_pl",  "interval_pl_date",  "interval_pl_time",  "interval_pl_timestamp",  "interval_pl_timestamptz",
        "interval_pl_timetz",  "interval_pl_year",  "interval_recv",  "interval_send",  "interval_smaller",
        "interval_support",  "intervaltonum",  "intervaltypmodin",  "intervaltypmodout",  "interval_um",
        "intinterval",  "int_sum_ext",  "int_uint2_eq",  "int_varbin_concat",  "isclosed",  "isdefined",  "isempty",
        "isexists",  "isfinite",  "is_free_lock",  "ishorizontal",  "is_ipv4",  "is_ipv4_compat",  "is_ipv4_mapped",
        "is_ipv6",  "iso8859_1_to_utf8",  "iso8859_to_utf8",  "isopen",  "iso_to_koi8r",  "iso_to_mic",
        "iso_to_win1251",  "iso_to_win866",  "isparallel",  "isperp",  "isubmit_on_nodes",  "is_used_lock",
        "isvertical",  "ivfflatbeginscan",  "ivfflat_bit_support",  "ivfflatbuild",  "ivfflatbuildempty",
        "ivfflatbulkdelete",  "ivfflatcostestimate",  "ivfflatendscan",  "ivfflatgettuple",  "ivfflathandler",
        "ivfflatinsert",  "ivfflatoptions",  "ivfflatrescan",  "ivfflatvacuumcleanup",  "ivfflatvalidate",
        "jaccard_distance",  "job_cancel",  "job_finish",  "job_submit",  "job_update",  "johab_to_utf8",  "json_agg",
        "json_agg_finalfn",  "json_agg_transfn",  "json_append",  "json_array",  "json_arrayagg",
        "json_array_append",  "json_array_element",  "json_array_elements",  "json_array_elements_text",
        "json_array_element_text",  "json_array_insert",  "json_array_length",  "jsonb_array_element",
        "jsonb_array_elements",  "jsonb_array_elements_text",  "jsonb_array_element_text",  "jsonb_array_length",
        "jsonb_cmp",  "jsonb_contained",  "jsonb_contains",  "jsonb_delete",  "jsonb_each",  "jsonb_each_text",
        "jsonb_eq",  "jsonb_exists",  "jsonb_exists_all",  "jsonb_exists_any",  "jsonb_extract_path",
        "jsonb_extract_path_op",  "jsonb_extract_path_text",  "jsonb_extract_path_text_op",  "jsonb_ge",  "jsonb_gt",
        "jsonb_hash",  "jsonb_in",  "jsonb_insert",  "jsonb_le",  "jsonb_lt",  "jsonb_ne",  "jsonb_object_field",
        "jsonb_object_field_text",  "jsonb_object_keys",  "jsonb_out",  "jsonb_populate_record",
        "jsonb_populate_recordset",  "jsonb_recv",  "jsonb_send",  "jsonb_set",  "jsonb_typeof",  "json_build_array",
        "json_build_object",  "json_contains",  "json_contains_path",  "json_depth",  "json_each",  "json_each_text",
        "json_eq",  "json_exists",  "json_extract",  "json_extract_path",  "json_extract_path_op",
        "json_extract_path_text",  "json_extract_path_text_op",  "json_ge",  "json_gt",  "json_in",  "json_insert",
        "json_keys",  "json_larger",  "json_le",  "json_length",  "json_lt",  "json_merge",  "json_merge_patch",
        "json_merge_preserve",  "json_ne",  "json_object",  "json_object_agg",  "json_objectagg",
        "json_object_agg_finalfn",  "json_objectagg_finalfn",  "json_objectagg_mysql_transfn",
        "json_object_agg_transfn",  "json_object_field",  "json_object_field_text",  "json_object_keys",
        "json_object_mysql",  "json_object_noarg",  "json_out",  "json_populate_record",  "json_populate_recordset",
        "json_pretty",  "json_quote",  "json_recv",  "json_remove",  "json_replace",  "json_search",  "json_send",
        "json_set",  "json_smaller",  "json_storage_size",  "json_textcontains",  "json_to_bool",  "json_to_record",
        "json_to_recordset",  "json_type",  "json_typeof",  "json_unquote",  "json_uplus",  "json_valid",
        "justify_days",  "justify_hours",  "justify_interval",  "kill_snapshot",  "koi8r_to_iso",  "koi8r_to_mic",
        "koi8r_to_utf8",  "koi8r_to_win1251",  "koi8r_to_win866",  "koi8u_to_utf8",  "l1_distance",  "l2_distance",
        "l2_norm",  "l2_normalize",  "lag",  "language_handler_in",  "language_handler_out",
        "large_seq_rollback_ntree",  "large_seq_upgrade_ntree",  "last",  "last_day",  "last_insert_id",
        "last_transition",  "lastval",  "last_value",  "latin1_to_mic",  "latin2_to_mic",  "latin2_to_win1250",
        "latin3_to_mic",  "latin4_to_mic",  "lblob_xor_lblob",  "lcase",  "lead",  "ledger_gchain_archive",
        "ledger_gchain_check",  "ledger_gchain_repair",  "ledger_hist_archive",  "ledger_hist_check",
        "ledger_hist_repair",  "left",  "length",  "lengthb",  "like",  "like_escape",  "likejoinsel",  "likesel",
        "line",  "line_distance",  "line_eq",  "line_horizontal",  "line_in",  "line_interpt",  "line_intersect",
        "line_out",  "line_parallel",  "line_perp",  "line_recv",  "line_send",  "line_vertical",  "listagg",
        "list_agg_finalfn",  "list_agg_noarg2_transfn",  "list_agg_transfn",  "ln",  "local_bad_block_info",
        "local_bgwriter_stat",  "local_candidate_stat",  "local_ckpt_stat",  "local_clear_bad_block_info",
        "local_debug_server_info",  "local_double_write_stat",  "local_pagewriter_stat",  "local_recovery_status",
        "local_redo_stat",  "local_redo_time_count",  "local_rto_stat",  "local_segment_space_info",
        "local_single_flush_dw_stat",  "local_space_shrink",  "local_xlog_redo_statics",  "locate",
        "lock_cluster_ddl",  "locktag_decode",  "lo_close",  "lo_creat",  "lo_create",  "lo_export",  "lo_from_bytea",
        "log",  "log10",  "log2",  "lo_get",  "log_fdw_handler",  "log_fdw_validator",  "login_audit_messages",
        "login_audit_messages_pid",  "lo_import",  "lo_lseek",  "lo_lseek64",  "longblob_blob_cmp",
        "longblob_blob_eq",  "longblob_blob_ge",  "longblob_blob_gt",  "longblob_blob_le",  "longblob_blob_lt",
        "longblob_blob_ne",  "longblob_boolean_eq",  "longblob_boolean_ge",  "longblob_boolean_gt",
        "longblob_boolean_le",  "longblob_boolean_lt",  "longblob_boolean_ne",  "longblob_cmp",
        "longblob_datetime_cmp",  "longblob_datetime_eq",  "longblob_datetime_ge",  "longblob_datetime_gt",
        "longblob_datetime_le",  "longblob_datetime_lt",  "longblob_datetime_ne",  "longblob_eq",  "longblob_eq_text",
        "longblob_float8_cmp",  "longblob_float8_eq",  "longblob_float8_ge",  "longblob_float8_gt",
        "longblob_float8_le",  "longblob_float8_lt",  "longblob_float8_ne",  "longblob_ge",  "longblob_ge_text",
        "longblob_gt",  "longblob_gt_text",  "longblob_int8_cmp",  "longblob_int8_eq",  "longblob_int8_ge",
        "longblob_int8_gt",  "longblob_int8_le",  "longblob_int8_lt",  "longblob_int8_ne",  "longblob_json",
        "longblob_larger",  "longblob_le",  "longblob_le_text",  "longblob_lt",  "longblob_lt_text",
        "longblob_mediumblob_cmp",  "longblob_mediumblob_eq",  "longblob_mediumblob_ge",  "longblob_mediumblob_gt",
        "longblob_mediumblob_le",  "longblob_mediumblob_lt",  "longblob_mediumblob_ne",  "longblob_ne",
        "longblob_ne_text",  "longblob_numeric_cmp",  "longblob_numeric_eq",  "longblob_numeric_ge",
        "longblob_numeric_gt",  "longblob_numeric_le",  "longblob_numeric_lt",  "longblob_numeric_ne",
        "longblob_rawin",  "longblob_rawout",  "longblob_recv",  "longblob_send",  "longblob_smaller",
        "longblob_timestamp_cmp",  "longblob_timestamp_eq",  "longblob_timestamp_ge",  "longblob_timestamp_gt",
        "longblob_timestamp_le",  "longblob_timestamp_lt",  "longblob_timestamp_ne",  "longblob_tinyblob_cmp",
        "longblob_tinyblob_eq",  "longblob_tinyblob_ge",  "longblob_tinyblob_gt",  "longblob_tinyblob_le",
        "longblob_tinyblob_lt",  "longblob_tinyblob_ne",  "longblob_uint8_cmp",  "longblob_uint8_eq",
        "longblob_uint8_ge",  "longblob_uint8_gt",  "longblob_uint8_le",  "longblob_uint8_lt",  "longblob_uint8_ne",
        "longblob_year_eq",  "longblob_year_ge",  "longblob_year_gt",  "longblob_year_le",  "longblob_year_lt",
        "longblob_year_ne",  "lo_open",  "lo_put",  "loread",  "lo_tell",  "lo_tell64",  "lo_truncate",
        "lo_truncate64",  "lo_unlink",  "lower",  "lower_inc",  "lower_inf",  "lowrite",  "lpad",  "lseg",
        "lseg_center",  "lseg_distance",  "lseg_eq",  "lseg_ge",  "lseg_gt",  "lseg_horizontal",  "lseg_in",
        "lseg_interpt",  "lseg_intersect",  "lseg_le",  "lseg_length",  "lseg_lt",  "lseg_ne",  "lseg_out",
        "lseg_parallel",  "lseg_perp",  "lseg_recv",  "lseg_send",  "lseg_vertical",  "ltrim",  "macaddr_and",
        "macaddr_cmp",  "macaddr_eq",  "macaddr_ge",  "macaddr_gt",  "macaddr_in",  "macaddr_le",  "macaddr_lt",
        "macaddr_ne",  "macaddr_not",  "macaddr_or",  "macaddr_out",  "macaddr_recv",  "macaddr_send",  "makeaclitem",
        "makedate",  "make_set",  "maketime",  "manage_snapshot_internal",  "masklen",  "max",  "mblob_xor_mblob",
        "md5",  "median",  "median_float8_finalfn",  "median_interval_finalfn",  "median_transfn",
        "mediumblob_blob_cmp",  "mediumblob_blob_eq",  "mediumblob_blob_ge",  "mediumblob_blob_gt",
        "mediumblob_blob_le",  "mediumblob_blob_lt",  "mediumblob_blob_ne",  "mediumblob_boolean_eq",
        "mediumblob_boolean_ge",  "mediumblob_boolean_gt",  "mediumblob_boolean_le",  "mediumblob_boolean_lt",
        "mediumblob_boolean_ne",  "mediumblob_cmp",  "mediumblob_datetime_cmp",  "mediumblob_datetime_eq",
        "mediumblob_datetime_ge",  "mediumblob_datetime_gt",  "mediumblob_datetime_le",  "mediumblob_datetime_lt",
        "mediumblob_datetime_ne",  "mediumblob_eq",  "mediumblob_eq_text",  "mediumblob_float8_cmp",
        "mediumblob_float8_eq",  "mediumblob_float8_ge",  "mediumblob_float8_gt",  "mediumblob_float8_le",
        "mediumblob_float8_lt",  "mediumblob_float8_ne",  "mediumblob_ge",  "mediumblob_ge_text",  "mediumblob_gt",
        "mediumblob_gt_text",  "mediumblob_int8_cmp",  "mediumblob_int8_eq",  "mediumblob_int8_ge",
        "mediumblob_int8_gt",  "mediumblob_int8_le",  "mediumblob_int8_lt",  "mediumblob_int8_ne",  "mediumblob_json",
        "mediumblob_larger",  "mediumblob_le",  "mediumblob_le_text",  "mediumblob_longblob_cmp",
        "mediumblob_longblob_eq",  "mediumblob_longblob_ge",  "mediumblob_longblob_gt",  "mediumblob_longblob_le",
        "mediumblob_longblob_lt",  "mediumblob_longblob_ne",  "mediumblob_lt",  "mediumblob_lt_text",
        "mediumblob_ne",  "mediumblob_ne_text",  "mediumblob_numeric_cmp",  "mediumblob_numeric_eq",
        "mediumblob_numeric_ge",  "mediumblob_numeric_gt",  "mediumblob_numeric_le",  "mediumblob_numeric_lt",
        "mediumblob_numeric_ne",  "mediumblob_rawin",  "mediumblob_rawout",  "mediumblob_recv",  "mediumblob_send",
        "mediumblob_smaller",  "mediumblob_timestamp_cmp",  "mediumblob_timestamp_eq",  "mediumblob_timestamp_ge",
        "mediumblob_timestamp_gt",  "mediumblob_timestamp_le",  "mediumblob_timestamp_lt",  "mediumblob_timestamp_ne",
        "mediumblob_tinyblob_cmp",  "mediumblob_tinyblob_eq",  "mediumblob_tinyblob_ge",  "mediumblob_tinyblob_gt",
        "mediumblob_tinyblob_le",  "mediumblob_tinyblob_lt",  "mediumblob_tinyblob_ne",  "mediumblob_uint8_cmp",
        "mediumblob_uint8_eq",  "mediumblob_uint8_ge",  "mediumblob_uint8_gt",  "mediumblob_uint8_le",
        "mediumblob_uint8_lt",  "mediumblob_uint8_ne",  "mediumblob_year_eq",  "mediumblob_year_ge",
        "mediumblob_year_gt",  "mediumblob_year_le",  "mediumblob_year_lt",  "mediumblob_year_ne",  "microsecond",
        "mic_to_ascii",  "mic_to_big5",  "mic_to_euc_cn",  "mic_to_euc_jp",  "mic_to_euc_kr",  "mic_to_euc_tw",
        "mic_to_iso",  "mic_to_koi8r",  "mic_to_latin1",  "mic_to_latin2",  "mic_to_latin3",  "mic_to_latin4",
        "mic_to_sjis",  "mic_to_win1250",  "mic_to_win1251",  "mic_to_win866",  "mid",  "min",  "minute",
        "mktinterval",  "mod",  "mode",  "mode_final",  "model_train_opt",  "money",  "month",  "monthname",
        "mot_global_memory_detail",  "mot_jit_detail",  "mot_jit_profile",  "mot_local_memory_detail",
        "mot_session_memory_detail",  "mul_d_interval",  "multiply",  "name",  "namebinarylike",  "namebinarynlike",
        "nameeq",  "namege",  "namegt",  "nameiclike",  "nameicnlike",  "nameicregexeq",  "nameicregexne",  "namein",
        "namele",  "namelike",  "namelt",  "namene",  "namenlike",  "nameout",  "namerecv",  "nameregexeq",
        "nameregexne",  "namesend",  "negetive_time",  "neqjoinsel",  "neqsel",  "netmask",  "network",
        "network_cmp",  "network_eq",  "network_ge",  "network_gt",  "network_larger",  "network_le",  "network_lt",
        "network_ne",  "network_smaller",  "network_sub",  "network_subeq",  "network_sup",  "network_supeq",
        "new_time",  "next",  "next_day",  "nextval",  "ngram_end",  "ngram_lextype",  "ngram_nexttoken",
        "ngram_start",  "nlikejoinsel",  "nlikesel",  "nls_initcap",  "nlssort",  "node_oid_name",  "notlike",
        "not_regexp",  "now",  "npoints",  "nth_value",  "ntile",  "num_bin_concat",  "numeric",  "numeric_abs",
        "numeric_accum",  "numeric_accum_numeric",  "numeric_add",  "numeric_any_value",  "numeric_avg",
        "numeric_avg_accum",  "numeric_avg_accum_numeric",  "numeric_avg_collect",  "numeric_avg_numeric",
        "numeric_b_format_date",  "numeric_b_format_datetime",  "numeric_b_format_time",
        "numeric_b_format_timestamp",  "numeric_binary_ge",  "numeric_binary_gt",  "numeric_binary_le",
        "numeric_binary_lt",  "numeric_blob_cmp",  "numeric_blob_eq",  "numeric_blob_ge",  "numeric_blob_gt",
        "numeric_blob_le",  "numeric_blob_lt",  "numeric_blob_ne",  "numeric_bool",  "numeric_bpchar",
        "numeric_cast_date",  "numeric_cast_datetime",  "numeric_cast_int8",  "numeric_cast_time",
        "numeric_cast_timestamptz",  "numeric_cast_uint1",  "numeric_cast_uint2",  "numeric_cast_uint4",
        "numeric_cast_uint8",  "numeric_cmp",  "numeric_collect",  "numeric_div",  "numeric_div_trunc",
        "numeric_enum",  "numeric_eq",  "numeric_exp",  "numeric_fac",  "numeric_ge",  "numeric_gt",  "numeric_in",
        "numeric_inc",  "numeric_int1",  "numeric_json",  "numeric_larger",  "numeric_le",
        "numeric_list_agg_noarg2_transfn",  "numeric_list_agg_transfn",  "numeric_ln",  "numeric_log",
        "numeric_longblob_cmp",  "numeric_longblob_eq",  "numeric_longblob_ge",  "numeric_longblob_gt",
        "numeric_longblob_le",  "numeric_longblob_lt",  "numeric_longblob_ne",  "numeric_lt",
        "numeric_mediumblob_cmp",  "numeric_mediumblob_eq",  "numeric_mediumblob_ge",  "numeric_mediumblob_gt",
        "numeric_mediumblob_le",  "numeric_mediumblob_lt",  "numeric_mediumblob_ne",  "numeric_mod",  "numeric_mul",
        "numeric_ne",  "numeric_out",  "numeric_power",  "numeric_recv",  "numeric_send",  "numeric_smaller",
        "numeric_sortsupport",  "numeric_sqrt",  "numeric_stddev_pop",  "numeric_stddev_pop_numeric",
        "numeric_stddev_samp",  "numeric_stddev_samp_numeric",  "numeric_sub",  "numeric_sum",  "numeric_support",
        "numeric_text",  "numeric_tinyblob_cmp",  "numeric_tinyblob_eq",  "numeric_tinyblob_ge",
        "numeric_tinyblob_gt",  "numeric_tinyblob_le",  "numeric_tinyblob_lt",  "numeric_tinyblob_ne",
        "numerictypmodin",  "numerictypmodout",  "numeric_uint1",  "numeric_uint2",  "numeric_uint4",
        "numeric_uint8",  "numeric_uminus",  "numeric_uplus",  "numeric_varchar",  "numeric_var_pop",
        "numeric_var_pop_numeric",  "numeric_var_samp",  "numeric_var_samp_numeric",  "numeric_xor",  "numeric_year",
        "numnode",  "numrange",  "numrange_subdiff",  "numtoday",  "numtodsinterval",  "num_to_interval",
        "num_varbin_concat",  "nvarchar2",  "nvarchar2_and_int8",  "nvarchar2_and_uint4",  "nvarchar2_cast_int8",
        "nvarchar2_cast_ui1",  "nvarchar2_cast_ui2",  "nvarchar2_cast_ui4",  "nvarchar2_cast_ui8",  "nvarchar2_enum",
        "nvarchar2in",  "nvarchar2_mi_time",  "nvarchar2_or_int8",  "nvarchar2_or_uint4",  "nvarchar2out",
        "nvarchar2_pl_time",  "nvarchar2recv",  "nvarchar2send",  "nvarchar2typmodin",  "nvarchar2typmodout",
        "nvarchar2_xor_int8",  "obj_description",  "oct",  "octet_length",  "oid",  "oideq",  "oidge",  "oidgt",
        "oidin",  "oidlarger",  "oidle",  "oidlt",  "oidne",  "oidout",  "oidrecv",  "oidsend",  "oidsmaller",
        "oidvectoreq",  "oidvectorge",  "oidvectorgt",  "oidvectorin",  "oidvectorin_extend",  "oidvectorle",
        "oidvectorlt",  "oidvectorne",  "oidvectorout",  "oidvectorout_extend",  "oidvectorrecv",
        "oidvectorrecv_extend",  "oidvectorsend",  "oidvectorsend_extend",  "oidvectortypes",
        "ondemand_recovery_status",  "on_pb",  "on_pl",  "on_ppath",  "on_ps",  "on_sb",  "on_sl",  "opaque_in",
        "opaque_out",  "op_bit_bool_xor",  "op_bit_date_xor",  "op_bit_timestamptz_xor",  "op_bit_timestamp_xor",
        "op_bitxor",  "op_bit_xor_num",  "op_bit_xor_uint8",  "op_blob_add_intr",  "op_blob_float8_xor",
        "op_blob_int8_xor",  "op_blob_int_xor",  "op_blob_sub_intr",  "op_bool_bit_xor",  "op_bool_float8_xor",
        "op_bool_time_xor",  "op_boolxor",  "op_bool_xor_int1",  "op_bool_xor_int2",  "op_bool_xor_int4",
        "op_bool_xor_int8",  "op_bool_xor_uint1",  "op_bool_xor_uint2",  "op_bool_xor_uint4",  "op_bool_xor_uint8",
        "op_date_add_intr",  "op_date_bit_xor",  "op_date_float8_xor",  "op_date_int8_xor",  "op_date_sub_intr",
        "op_date_text_xor",  "op_date_time_xor",  "op_datexor",  "op_dpow",  "op_dttm_add_intr",  "op_dttm_sub_intr",
        "opengauss_version",  "op_enum_add_intr",  "op_enum_sub_intr",  "op_float8_blob_xor",  "op_float8_bool_xor",
        "op_float8_date_xor",  "op_float8_timestamptz_xor",  "op_float8_timestamp_xor",  "op_int1xor",
        "op_int1_xor_bool",  "op_int1_xor_uint1",  "op_int2xor",  "op_int2_xor_bool",  "op_int2_xor_uint2",
        "op_int4xor",  "op_int4_xor_bool",  "op_int4_xor_uint4",  "op_int8_blob_xor",  "op_int8_date_xor",
        "op_int8_timestamptz_xor",  "op_int8_timestamp_xor",  "op_int8_time_xor",  "op_int8xor",  "op_int8_xor_bool",
        "op_int8_xor_uint8",  "op_int_blob_xor",  "op_intr_add_blob",  "op_intr_add_date",  "op_intr_add_dttm",
        "op_intr_add_enum",  "op_intr_add_json",  "op_intr_add_lblob",  "op_intr_add_mblob",  "op_intr_add_num",
        "op_intr_add_set",  "op_intr_add_tblob",  "op_intr_add_text",  "op_intr_add_time",  "op_intr_add_tmsp",
        "op_json_add_intr",  "op_json_sub_intr",  "op_lblob_add_intr",  "op_lblob_sub_intr",  "op_mblob_add_intr",
        "op_mblob_sub_intr",  "op_num_add_intr",  "op_numeric_power",  "op_num_sub_intr",  "op_num_xor_bit",
        "op_set_add_intr",  "op_set_sub_intr",  "op_tblob_add_intr",  "op_tblob_sub_intr",  "op_text_add_intr",
        "op_text_date_xor",  "op_text_sub_intr",  "op_text_timestamptz_xor",  "op_text_timestamp_xor",
        "op_text_time_xor",  "op_textxor",  "op_time_add_intr",  "op_time_bool_xor",  "op_time_date_xor",
        "op_time_int8_xor",  "op_timestamp_bit_xor",  "op_timestamp_float8_xor",  "op_timestamp_int8_xor",
        "op_timestamp_text_xor",  "op_timestamptz_bit_xor",  "op_timestamptz_float8_xor",  "op_timestamptz_int8_xor",
        "op_timestamptz_text_xor",  "op_timestamptzxor",  "op_timestampxor",  "op_time_sub_intr",  "op_time_text_xor",
        "op_timexor",  "op_tmsp_add_intr",  "op_tmsp_sub_intr",  "op_uint1xor",  "op_uint1_xor_bool",
        "op_uint1_xor_int1",  "op_uint2xor",  "op_uint2_xor_bool",  "op_uint2_xor_int2",  "op_uint4xor",
        "op_uint4_xor_bool",  "op_uint4_xor_int4",  "op_uint8xor",  "op_uint8_xor_bit",  "op_uint8_xor_bool",
        "op_uint8_xor_int8",  "ord",  "ordered_set_transition",  "ordered_set_transition_multi",  "overlaps",
        "overlay",  "path",  "path_add",  "path_add_pt",  "path_center",  "path_contain_pt",  "path_distance",
        "path_div_pt",  "path_in",  "path_inter",  "path_length",  "path_mul_pt",  "path_n_eq",  "path_n_ge",
        "path_n_gt",  "path_n_le",  "path_n_lt",  "path_npoints",  "path_out",  "path_recv",  "path_send",
        "path_sub_pt",  "pclose",  "percentile_cont",  "percentile_cont_float8_final",
        "percentile_cont_interval_final",  "percentile_of_value",  "percent_rank",  "percent_rank_final",
        "period_add",  "period_diff",  "pg_advisory_lock",  "pg_advisory_lock_shared",  "pg_advisory_unlock",
        "pg_advisory_unlock_all",  "pg_advisory_unlock_shared",  "pg_advisory_xact_lock",
        "pg_advisory_xact_lock_shared",  "pg_autovac_coordinator",  "pg_autovac_status",  "pg_autovac_timeout",
        "pg_available_extensions",  "pg_available_extension_versions",  "pg_backend_pid",  "pg_buffercache_pages",
        "pg_cancel_backend",  "pg_cancel_invalid_query",  "pg_cancel_session",  "pg_cbm_force_track",
        "pg_cbm_get_changed_block",  "pg_cbm_get_merged_file",  "pg_cbm_recycle_file",  "pg_cbm_rotate_file",
        "pg_cbm_tracked_location",  "_pg_char_max_length",  "_pg_char_octet_length",  "pg_char_to_encoding",
        "pg_check_authid",  "pg_check_xidlimit",  "pg_clean_region_info",  "pg_client_encoding",  "pg_collation_for",
        "pg_collation_is_visible",  "pg_column_is_updatable",  "pg_column_size",  "pg_comm_delay",
        "pg_comm_recv_stream",  "pg_comm_send_stream",  "pg_comm_status",  "pg_conf_load_time",
        "pg_control_checkpoint",  "pg_control_group_config",  "pg_control_system",  "pg_conversion_is_visible",
        "pg_create_logical_replication_slot",  "pg_create_physical_replication_slot",
        "pg_create_physical_replication_slot_extern",  "pg_create_restore_point",  "pg_current_sessid",
        "pg_current_sessionid",  "pg_current_userid",  "pg_current_xlog_insert_location",  "pg_current_xlog_location",
        "pg_cursor",  "pg_database_size",  "_pg_datetime_precision",  "pg_ddl_command_in",  "pg_ddl_command_out",
        "pg_ddl_command_recv",  "pg_ddl_command_send",  "pg_delete_audit",  "pg_describe_object",
        "pg_disable_delay_ddl_recycle",  "pg_disable_delay_xlog_recycle",  "pg_drop_replication_slot",
        "pg_enable_delay_ddl_recycle",  "pg_enable_delay_xlog_recycle",  "pg_encoding_max_length",
        "pg_encoding_to_char",  "pg_event_trigger_ddl_commands",  "pg_event_trigger_dropped_objects",
        "pg_event_trigger_table_rewrite_oid",  "pg_event_trigger_table_rewrite_reason",  "_pg_expandarray",
        "pg_export_snapshot",  "pg_export_snapshot_and_csn",  "pg_extension_config_dump",
        "pg_extension_update_paths",  "pgfadvise",  "pgfadvise_dontneed",  "pgfadvise_loader",  "pgfadvise_normal",
        "pgfadvise_random",  "pgfadvise_sequential",  "pgfadvise_willneed",  "pg_filenode_relation",  "pgfincore",
        "pgfincore_drawer",  "pg_free_remain_segment",  "pg_function_is_visible",  "pg_get_constraintdef",
        "pg_get_delta_info",  "pg_get_expr",  "pg_get_flush_lsn",  "pg_get_function_arguments",  "pg_get_functiondef",
        "pg_get_function_identity_arguments",  "pg_get_function_result",  "pg_get_gtt_relstats",
        "pg_get_gtt_statistics",  "pg_get_indexdef",  "pg_get_keywords",  "pg_get_nonstrict_basic_value",
        "pg_get_object_address",  "pg_get_publication_tables",  "pg_get_replica_identity_index",
        "pg_get_replication_slot_name",  "pg_get_replication_slots",  "pg_get_ruledef",  "pg_get_running_xacts",
        "pg_get_serial_sequence",  "pg_get_sync_flush_lsn",  "pg_get_tabledef",  "pg_get_triggerdef",
        "pg_get_userbyid",  "pg_get_variable_info",  "pg_get_viewdef",  "pg_get_xidlimit",  "pg_gtt_attached_pid",
        "pg_has_role",  "pg_identify_object",  "pg_indexes_size",  "_pg_index_position",  "_pg_interval_type",
        "pg_is_in_recovery",  "pg_is_other_temp_schema",  "pg_is_xlog_replay_paused",  "_pg_keysequal",
        "pg_last_xact_replay_timestamp",  "pg_last_xlog_receive_location",  "pg_last_xlog_replay_location",
        "pg_listening_channels",  "pg_list_gtt_relfrozenxids",  "pg_lock_status",  "pg_log_comm_status",
        "pg_logical_get_area_changes",  "pg_logical_slot_get_binary_changes",  "pg_logical_slot_get_changes",
        "pg_logical_slot_peek_binary_changes",  "pg_logical_slot_peek_changes",  "pg_ls_dir",  "pg_ls_tmpdir",
        "pg_ls_waldir",  "pg_my_temp_schema",  "pg_node_tree_in",  "pg_node_tree_out",  "pg_node_tree_recv",
        "pg_node_tree_send",  "pg_notify",  "_pg_numeric_precision",  "_pg_numeric_precision_radix",
        "_pg_numeric_scale",  "pg_opclass_is_visible",  "pg_open_tables",  "pg_operator_is_visible",
        "pg_opfamily_is_visible",  "pg_options_to_table",  "pg_parse_clog",  "pg_partition_filenode",
        "pg_partition_filepath",  "pg_partition_indexes_size",  "pg_partition_size",  "pg_pool_ping",
        "pg_pool_validate",  "pg_postmaster_start_time",  "pg_prepared_statement",  "pg_prepared_xact",
        "pg_query_audit",  "pg_read_binary_file",  "pg_read_binary_file_blocks",  "pg_read_file",
        "pg_relation_compression_ratio",  "pg_relation_filenode",  "pg_relation_filepath",
        "pg_relation_is_updatable",  "pg_relation_size",  "pg_relation_with_compression",  "pg_reload_conf",
        "pg_replication_origin_advance",  "pg_replication_origin_create",  "pg_replication_origin_drop",
        "pg_replication_origin_oid",  "pg_replication_origin_progress",  "pg_replication_origin_session_is_setup",
        "pg_replication_origin_session_progress",  "pg_replication_origin_session_reset",
        "pg_replication_origin_session_setup",  "pg_replication_origin_xact_reset",
        "pg_replication_origin_xact_setup",  "pg_replication_slot_advance",  "pg_resume_bkp_flag",
        "pg_rotate_logfile",  "pg_sequence_last_value",  "pg_sequence_parameters",  "pg_shared_memctx_detail",
        "pg_shared_memory_detail",  "pg_show_all_settings",  "pg_show_replication_origin_status",  "pg_size_pretty",
        "pg_sleep",  "pg_start_backup",  "pg_stat_bad_block",  "pg_stat_bad_block_clear",  "pg_stat_clear_snapshot",
        "pg_stat_file",  "pg_stat_file_recursive",  "pg_stat_get_activity",  "pg_stat_get_activity_for_temptable",
        "pg_stat_get_activity_ng",  "pg_stat_get_activity_with_conninfo",  "pg_stat_get_analyze_count",
        "pg_stat_get_autoanalyze_count",  "pg_stat_get_autovacuum_count",  "pg_stat_get_backend_activity",
        "pg_stat_get_backend_activity_start",  "pg_stat_get_backend_client_addr",  "pg_stat_get_backend_client_port",
        "pg_stat_get_backend_dbid",  "pg_stat_get_backend_idset",  "pg_stat_get_backend_pid",
        "pg_stat_get_backend_start",  "pg_stat_get_backend_userid",  "pg_stat_get_backend_waiting",
        "pg_stat_get_backend_xact_start",  "pg_stat_get_bgwriter_buf_written_checkpoints",
        "pg_stat_get_bgwriter_buf_written_clean",  "pg_stat_get_bgwriter_maxwritten_clean",
        "pg_stat_get_bgwriter_requested_checkpoints",  "pg_stat_get_bgwriter_stat_reset_time",
        "pg_stat_get_bgwriter_timed_checkpoints",  "pg_stat_get_blocks_fetched",  "pg_stat_get_blocks_hit",
        "pg_stat_get_buf_alloc",  "pg_stat_get_buf_fsync_backend",  "pg_stat_get_buf_written_backend",
        "pg_stat_get_cgroup_info",  "pg_stat_get_checkpoint_sync_time",  "pg_stat_get_checkpoint_write_time",
        "pg_stat_get_cu_hdd_asyn",  "pg_stat_get_cu_hdd_sync",  "pg_stat_get_cu_mem_hit",  "pg_stat_get_data_senders",
        "pg_stat_get_db_blk_read_time",  "pg_stat_get_db_blk_write_time",  "pg_stat_get_db_blocks_fetched",
        "pg_stat_get_db_blocks_hit",  "pg_stat_get_db_conflict_all",  "pg_stat_get_db_conflict_bufferpin",
        "pg_stat_get_db_conflict_lock",  "pg_stat_get_db_conflict_snapshot",
        "pg_stat_get_db_conflict_startup_deadlock",  "pg_stat_get_db_conflict_tablespace",
        "pg_stat_get_db_cu_hdd_asyn",  "pg_stat_get_db_cu_hdd_sync",  "pg_stat_get_db_cu_mem_hit",
        "pg_stat_get_db_deadlocks",  "pg_stat_get_db_numbackends",  "pg_stat_get_db_stat_reset_time",
        "pg_stat_get_db_temp_bytes",  "pg_stat_get_db_temp_files",  "pg_stat_get_db_tuples_deleted",
        "pg_stat_get_db_tuples_fetched",  "pg_stat_get_db_tuples_inserted",  "pg_stat_get_db_tuples_returned",
        "pg_stat_get_db_tuples_updated",  "pg_stat_get_db_xact_commit",  "pg_stat_get_db_xact_rollback",
        "pg_stat_get_dead_tuples",  "pg_stat_get_env",  "pg_stat_get_file_stat",  "pg_stat_get_function_calls",
        "pg_stat_get_function_self_time",  "pg_stat_get_function_total_time",  "pg_stat_get_last_analyze_time",
        "pg_stat_get_last_autoanalyze_time",  "pg_stat_get_last_autovacuum_time",
        "pg_stat_get_last_data_changed_time",  "pg_stat_get_last_vacuum_time",  "pg_stat_get_live_tuples",
        "pg_stat_get_mem_mbytes_reserved",  "pg_stat_get_numscans",  "pg_stat_get_partition_dead_tuples",
        "pg_stat_get_partition_live_tuples",  "pg_stat_get_partition_tuples_changed",
        "pg_stat_get_partition_tuples_deleted",  "pg_stat_get_partition_tuples_hot_updated",
        "pg_stat_get_partition_tuples_inserted",  "pg_stat_get_partition_tuples_updated",
        "pg_stat_get_pooler_status",  "pg_stat_get_realtime_info_internal",  "pg_stat_get_redo_stat",
        "pg_stat_get_role_name",  "pg_stat_get_session_wlmstat",  "pg_stat_get_sql_count",  "pg_stat_get_status",
        "pg_stat_get_stream_replications",  "pg_stat_get_subscription",  "pg_stat_get_thread",
        "pg_stat_get_tuples_changed",  "pg_stat_get_tuples_deleted",  "pg_stat_get_tuples_fetched",
        "pg_stat_get_tuples_hot_updated",  "pg_stat_get_tuples_inserted",  "pg_stat_get_tuples_returned",
        "pg_stat_get_tuples_updated",  "pg_stat_get_vacuum_count",  "pg_stat_get_wal_receiver",
        "pg_stat_get_wal_senders",  "pg_stat_get_wlm_ec_operator_info",  "pg_stat_get_wlm_instance_info",
        "pg_stat_get_wlm_instance_info_with_cleanup",  "pg_stat_get_wlm_node_resource_info",
        "pg_stat_get_wlm_operator_info",  "pg_stat_get_wlm_realtime_ec_operator_info",
        "pg_stat_get_wlm_realtime_operator_info",  "pg_stat_get_wlm_realtime_session_info",
        "pg_stat_get_wlm_session_info",  "pg_stat_get_wlm_session_info_internal",
        "pg_stat_get_wlm_session_iostat_info",  "pg_stat_get_wlm_statistics",  "pg_stat_get_workload_struct_info",
        "pg_stat_get_xact_blocks_fetched",  "pg_stat_get_xact_blocks_hit",  "pg_stat_get_xact_function_calls",
        "pg_stat_get_xact_function_self_time",  "pg_stat_get_xact_function_total_time",  "pg_stat_get_xact_numscans",
        "pg_stat_get_xact_partition_tuples_deleted",  "pg_stat_get_xact_partition_tuples_hot_updated",
        "pg_stat_get_xact_partition_tuples_inserted",  "pg_stat_get_xact_partition_tuples_updated",
        "pg_stat_get_xact_tuples_deleted",  "pg_stat_get_xact_tuples_fetched",  "pg_stat_get_xact_tuples_hot_updated",
        "pg_stat_get_xact_tuples_inserted",  "pg_stat_get_xact_tuples_returned",  "pg_stat_get_xact_tuples_updated",
        "pg_stat_remain_segment_info",  "pg_stat_reset",  "pg_stat_reset_shared",
        "pg_stat_reset_single_function_counters",  "pg_stat_reset_single_table_counters",
        "pg_stat_segment_extent_usage",  "pg_stat_session_cu",  "pg_stat_set_last_data_changed_time",
        "pg_stop_backup",  "pg_switch_xlog",  "pg_sync_cstore_delta",  "pgsysconf",  "pgsysconf_pretty",
        "pg_systimestamp",  "pg_table_is_visible",  "pg_table_size",  "pg_tablespace_databases",
        "pg_tablespace_location",  "pg_tablespace_size",  "pg_tde_info",  "pg_terminate_active_session_socket",
        "pg_terminate_backend",  "pg_terminate_session",  "pg_test_err_contain_err",  "pg_timezone_abbrevs",
        "pg_timezone_names",  "pg_total_autovac_tuples",  "pg_total_relation_size",  "pg_trigger_depth",
        "_pg_truetypid",  "_pg_truetypmod",  "pg_try_advisory_lock",  "pg_try_advisory_lock_shared",
        "pg_try_advisory_xact_lock",  "pg_try_advisory_xact_lock_shared",  "pg_ts_config_is_visible",
        "pg_ts_dict_is_visible",  "pg_ts_parser_is_visible",  "pg_ts_template_is_visible",  "pg_type_is_visible",
        "pg_typeof",  "pg_user_iostat",  "pg_wlm_jump_queue",  "pgxc_disaster_read_clear",  "pgxc_disaster_read_init",
        "pgxc_disaster_read_set",  "pgxc_disaster_read_status",  "pgxc_get_csn",  "pgxc_get_stat_dirty_tables",
        "pgxc_get_thread_wait_status",  "pgxc_gtm_snapshot_status",  "pgxc_is_committed",  "pgxc_lock_for_backup",
        "pgxc_lock_for_sp_database",  "pgxc_lock_for_transfer",  "pgxc_log_comm_status",  "pgxc_max_datanode_size",
        "pgxc_node_str",  "pgxc_pool_check",  "pgxc_pool_connection_status",  "pgxc_pool_reload",
        "pgxc_prepared_xact",  "pgxc_snapshot_status",  "pgxc_stat_dirty_tables",  "pgxc_unlock_for_sp_database",
        "pgxc_unlock_for_transfer",  "pg_xlogfile_name",  "pg_xlogfile_name_offset",  "pg_xlog_location_diff",
        "pg_xlog_replay_pause",  "pg_xlog_replay_resume",  "pi",  "plainto_tsquery",  "plancache_clean",
        "plancache_status",  "plan_seed",  "plpgsql_call_handler",  "plpgsql_inline_handler",  "plpgsql_validator",
        "point",  "point_above",  "point_add",  "point_below",  "point_distance",  "point_div",  "point_eq",
        "point_horiz",  "point_in",  "point_left",  "point_mul",  "point_ne",  "point_out",  "point_recv",
        "point_right",  "point_send",  "point_sub",  "point_vert",  "poly_above",  "poly_below",  "poly_center",
        "poly_contain",  "poly_contained",  "poly_contain_pt",  "poly_distance",  "polygon",  "poly_in",  "poly_left",
        "poly_npoints",  "poly_out",  "poly_overabove",  "poly_overbelow",  "poly_overlap",  "poly_overleft",
        "poly_overright",  "poly_recv",  "poly_right",  "poly_same",  "poly_send",  "popen",  "position",
        "positionjoinsel",  "positionsel",  "postgresql_fdw_validator",  "pound_end",  "pound_lextype",
        "pound_nexttoken",  "pound_start",  "pow",  "power",  "prepare_snapshot",  "prepare_snapshot_internal",
        "prepare_statement_status",  "print_var",  "prsd_end",  "prsd_headline",  "prsd_lextype",  "prsd_nexttoken",
        "prsd_start",  "psortbuild",  "psortcanreturn",  "psortcostestimate",  "psortgetbitmap",  "psortgettuple",
        "psortoptions",  "pt_contained_circle",  "pt_contained_poly",  "pubddl_decode",
        "publication_deparse_ddl_command_end",  "publication_deparse_ddl_command_start",
        "publication_deparse_table_rewrite",  "publish_snapshot",  "purge_snapshot",  "purge_snapshot_internal",
        "pv_builtin_functions",  "pv_compute_pool_workload",  "pv_instance_time",  "pv_os_run_info",
        "pv_session_memctx_detail",  "pv_session_memory",  "pv_session_memory_detail",  "pv_session_stat",
        "pv_session_time",  "pv_thread_memory_detail",  "pv_total_memory_detail",  "quarter",  "query_all_drc_info",
        "query_imcstore_views",  "query_node_reform_info",  "query_node_reform_info_from_dms",
        "query_page_distribution_info",  "query_to_xml",  "query_to_xml_and_xmlschema",  "query_to_xmlschema",
        "querytree",  "quote",  "quote_ident",  "quote_literal",  "quote_nullable",  "radians",  "radius",
        "raise_application_error",  "rand",  "random",  "random_bytes",  "randommasking",  "range_adjacent",
        "range_after",  "range_before",  "range_cmp",  "range_contained_by",  "range_contains",
        "range_contains_elem",  "range_eq",  "range_ge",  "range_gist_compress",  "range_gist_consistent",
        "range_gist_decompress",  "range_gist_penalty",  "range_gist_picksplit",  "range_gist_same",
        "range_gist_union",  "range_gt",  "range_in",  "range_intersect",  "range_le",  "range_lt",  "range_minus",
        "range_ne",  "range_out",  "range_overlaps",  "range_overleft",  "range_overright",  "range_recv",
        "range_send",  "range_typanalyze",  "range_union",  "rank",  "rank_final",  "rawcat",  "rawcmp",  "raweq",
        "rawge",  "rawgt",  "rawin",  "rawle",  "rawlike",  "rawlt",  "rawne",  "rawnlike",  "rawout",  "rawrecv",
        "rawsend",  "rawtohex",  "read_disable_conn_file",  "realtime_build_log_ctrl_status",  "rebuild_partition",
        "record_eq",  "record_ge",  "record_gt",  "record_in",  "record_le",  "record_lt",  "record_ne",
        "record_out",  "record_recv",  "record_send",  "regclass",  "regclassin",  "regclassout",  "regclassrecv",
        "regclasssend",  "regconfigin",  "regconfigout",  "regconfigrecv",  "regconfigsend",  "regdictionaryin",
        "regdictionaryout",  "regdictionaryrecv",  "regdictionarysend",  "regexeqjoinsel",  "regexeqsel",
        "regex_like_m",  "regexnejoinsel",  "regexnesel",  "regexp",  "regexp_count",  "regexp_instr",  "regexp_like",
        "regexpmasking",  "regexp_matches",  "regexp_replace",  "regexp_split_to_array",  "regexp_split_to_table",
        "regexp_substr",  "regoperatorin",  "regoperatorout",  "regoperatorrecv",  "regoperatorsend",  "regoperin",
        "regoperout",  "regoperrecv",  "regopersend",  "regprocedurein",  "regprocedureout",  "regprocedurerecv",
        "regproceduresend",  "regprocin",  "regprocout",  "regprocrecv",  "regprocsend",  "regr_avgx",  "regr_avgy",
        "regr_count",  "regr_intercept",  "regr_r2",  "regr_slope",  "regr_sxx",  "regr_sxy",  "regr_syy",
        "regtypein",  "regtypeout",  "regtyperecv",  "regtypesend",  "release_all_locks",  "release_lock",  "reltime",
        "reltimeeq",  "reltimege",  "reltimegt",  "reltimein",  "reltimele",  "reltimelt",  "reltimene",
        "reltimeout",  "reltimerecv",  "reltimesend",  "reltime_text",  "remote_bgwriter_stat",
        "remote_candidate_stat",  "remote_ckpt_stat",  "remote_double_write_stat",  "remote_pagewriter_stat",
        "remote_recovery_status",  "remote_redo_stat",  "remote_rto_stat",  "remote_segment_space_info",
        "remote_single_flush_dw_stat",  "remove_partitioning",  "repeat",  "replace",  "report_application_error",
        "reset_unique_sql",  "reverse",  "RI_FKey_cascade_del",  "RI_FKey_cascade_upd",  "RI_FKey_check_ins",
        "RI_FKey_check_upd",  "RI_FKey_noaction_del",  "RI_FKey_noaction_upd",  "RI_FKey_restrict_del",
        "RI_FKey_restrict_upd",  "RI_FKey_setdefault_del",  "RI_FKey_setdefault_upd",  "RI_FKey_setnull_del",
        "RI_FKey_setnull_upd",  "right",  "rlike",  "round",  "row_count",  "row_number",  "row_to_json",  "rpad",
        "rtrim",  "run",  "sample_snapshot",  "scalargtjoinsel",  "scalargtsel",  "scalarltjoinsel",  "scalarltsel",
        "schema",  "schema_to_xml",  "schema_to_xml_and_xmlschema",  "schema_to_xmlschema",  "second",  "sec_to_time",
        "sessionid2pid",  "session_user",  "set",  "setanydatasetexcept",  "setbdouble",  "set_bit",  "setblob",
        "set_boolean",  "set_byte",  "set_caching_sha2_password",  "set_cast_int8",  "setchar",  "set_config",
        "set_cost_params",  "set_date",  "setdate",  "set_datetime",  "set_enum",  "seteq",  "setge",  "setgt",
        "set_hashbucket_info",  "set_in",  "setinfo",  "setint2eq",  "setint2ge",  "setint2gt",  "setint2le",
        "setint2lt",  "setint2ne",  "setint4eq",  "setint4ge",  "setint4gt",  "setint4le",  "setint4lt",  "setint4ne",
        "setint8eq",  "setint8ge",  "setint8gt",  "setint8le",  "setint8lt",  "setint8ne",  "set_json",  "setle",
        "setlt",  "set_masklen",  "set_native_password",  "setnchar",  "setne",  "setnumber",  "setnvarchar2",
        "set_out",  "setraw",  "set_recv",  "setseed",  "set_send",  "settexteq",  "settextge",  "settextgt",
        "settextle",  "settextlt",  "settextne",  "set_time",  "set_timestamp",  "settimestamp",  "settimestamptz",
        "settobit",  "settobpchar",  "settoint1",  "settonumber",  "settonvarchar2",  "settotext",  "settouint1",
        "settouint2",  "settouint4",  "settouint8",  "settovarchar",  "setval",  "set_var",  "setvarchar",
        "setvarchar2",  "setweight",  "set_weight_params",  "set_working_grand_version_num_manually",  "set_year",
        "sha",  "sha1",  "sha2",  "shell_in",  "shell_out",  "shift_jis_2004_to_euc_jis_2004",
        "shift_jis_2004_to_utf8",  "shobj_description",  "showallgucreset",  "show_any_privileges",
        "show_character_set",  "show_collation",  "show_function_status",  "show_object_grants",
        "show_role_privilege",  "show_sql_patch",  "show_status",  "show_triggers",  "shufflemasking",  "sign",
        "similar_escape",  "sin",  "sjis_to_euc_jp",  "sjis_to_mic",  "sjis_to_utf8",  "skeys",  "sleep",  "slice",
        "slice_array",  "slope",  "smalldatetime_cmp",  "smalldatetime_eq",  "smalldatetime_ge",  "smalldatetime_gt",
        "smalldatetime_hash",  "smalldatetime_in",  "smalldatetime_larger",  "smalldatetime_le",  "smalldatetime_lt",
        "smalldatetime_ne",  "smalldatetime_out",  "smalldatetime_recv",  "smalldatetime_send",
        "smalldatetime_smaller",  "smalldatetime_to_abstime",  "smalldatetime_to_time",  "smalldatetime_to_timestamp",
        "smalldatetime_to_timestamptz",  "smalldatetime_to_varchar2",  "smallint_sum_ext",  "smgreq",  "smgrin",
        "smgrne",  "smgrout",  "soundex",  "soundex_difference",  "space",  "sparsevec",  "sparsevec_cmp",
        "sparsevec_eq",  "sparsevec_ge",  "sparsevec_gt",  "sparsevec_in",  "sparsevec_l2_squared_distance",
        "sparsevec_le",  "sparsevec_lt",  "sparsevec_ne",  "sparsevec_negative_inner_product",  "sparsevec_out",
        "sparsevec_recv",  "sparsevec_send",  "sparsevec_to_vector",  "sparsevec_typmod_in",  "spgbeginscan",
        "spgbuild",  "spgbuildempty",  "spgbulkdelete",  "spgcanreturn",  "spgcostestimate",  "spgendscan",
        "spggetbitmap",  "spggettuple",  "spginsert",  "spg_kd_choose",  "spg_kd_config",  "spg_kd_inner_consistent",
        "spg_kd_picksplit",  "spgmarkpos",  "spgmerge",  "spgoptions",  "spg_quad_choose",  "spg_quad_config",
        "spg_quad_inner_consistent",  "spg_quad_leaf_consistent",  "spg_quad_picksplit",  "spgrescan",  "spgrestrpos",
        "spg_text_choose",  "spg_text_config",  "spg_text_inner_consistent",  "spg_text_leaf_consistent",
        "spg_text_picksplit",  "spgvacuumcleanup",  "split_part",  "sqrt",  "ss_buffer_ctrl",
        "ss_txnstatus_cache_stat",  "standby_statement_history",  "start_collect_workload",
        "statement_detail_decode",  "statement_timestamp",  "std",  "stddev",  "stddev_pop",  "stddev_samp",  "step",
        "strcmp",  "string_agg",  "string_agg_finalfn",  "string_agg_transfn",  "string_to_array",  "strip",
        "strpos",  "str_to_date",  "subdate",  "submit_on_nodes",  "substr",  "substrb",  "substring",
        "substring_index",  "substring_inner",  "subtime",  "subtype_in",  "subtype_recv",  "subvector",  "sum",
        "sum_ext",  "suppress_redundant_updates_trigger",  "svals",  "sys_connect_by_path",  "sysdate",
        "system_user",  "table_data_skewness",  "table_distribution",  "table_skewness",  "tablespace_oid_name",
        "table_to_xml",  "table_to_xml_and_xmlschema",  "table_to_xmlschema",  "tan",  "tconvert",  "tdigest_in",
        "tdigest_merge",  "tdigest_mergep",  "tdigest_merge_to_one",  "tdigest_out",  "test_ge_blob",
        "test_ge_longblob",  "test_ge_mediumblob",  "test_ge_tinyblob",  "test_gt_blob",  "test_gt_longblob",
        "test_gt_mediumblob",  "test_gt_tinyblob",  "text",  "text_and_uint8",  "textanycat",  "text_any_value",
        "text_binary_eq",  "textbinarylike",  "textbinarynlike",  "text_bin_concat",  "text_bit_concat",  "text_bool",
        "text_bool_concat",  "textboollike",  "textboolnlike",  "text_cast_int8",  "text_cast_uint1",
        "text_cast_uint2",  "text_cast_uint4",  "text_cast_uint8",  "textcat",  "text_date",  "text_date_explicit",
        "text_datetime_eq",  "text_datetime_ge",  "text_datetime_gt",  "text_datetime_le",  "text_datetime_lt",
        "text_datetime_ne",  "text_date_xor",  "text_enum",  "textenumeq",  "textenumge",  "textenumgt",
        "textenumle",  "textenum_like",  "textenumlt",  "textenumne",  "textenum_nlike",  "texteq",  "text_eq_blob",
        "text_eq_longblob",  "text_eq_mediumblob",  "text_eq_tinyblob",  "text_float4",  "text_float8",  "text_ge",
        "text_gt",  "texticlike",  "texticnlike",  "texticregexeq",  "texticregexne",  "textin",  "text_int1",
        "text_int2",  "text_int4",  "text_int8",  "text_interval",  "text_larger",  "text_le",  "text_le_blob",
        "text_le_longblob",  "text_le_mediumblob",  "textlen",  "text_le_tinyblob",  "textlike",  "text_lt",
        "text_lt_blob",  "text_lt_longblob",  "text_lt_mediumblob",  "text_lt_tinyblob",  "textne",  "text_ne_blob",
        "text_ne_longblob",  "text_ne_mediumblob",  "text_ne_tinyblob",  "textnlike",  "text_numeric",  "textout",
        "text_pattern_ge",  "text_pattern_gt",  "text_pattern_le",  "text_pattern_lt",  "textrecv",  "textregexeq",
        "textregexne",  "textsend",  "textseteq",  "textsetge",  "textsetgt",  "textsetle",  "textsetlt",
        "textsetne",  "text_smaller",  "text_sum",  "text_time_explicit",  "text_timestamp",  "text_timestamp_eq",
        "text_timestamp_ge",  "text_timestamp_gt",  "text_timestamp_le",  "text_timestamp_lt",  "text_timestamp_ne",
        "text_timestamptz_xor",  "text_timestamp_xor",  "text_time_xor",  "text_to_bit",  "text_to_blob",
        "text_to_longblob",  "text_to_mediumblob",  "text_to_tinyblob",  "text_varbinary_eq",  "text_varbinary_ge",
        "text_varbinary_gt",  "text_varbinary_le",  "textvarbinarylike",  "text_varbinary_lt",  "text_varbinary_ne",
        "text_varbin_concat",  "text_xor",  "textxor",  "text_year",  "thesaurus_init",  "thesaurus_lexize",
        "threadpool_status",  "tideq",  "tidge",  "tidgt",  "tidin",  "tidlarger",  "tidle",  "tidlt",  "tidne",
        "tidout",  "tidrecv",  "tidsend",  "tidsmaller",  "time",  "time_any_value",  "time_binary_ge",
        "time_binary_gt",  "time_binary_le",  "time_binary_lt",  "time_bit_ge",  "time_bit_gt",  "time_bit_le",
        "time_bit_lt",  "time_bool",  "time_bpchar",  "time_cast",  "time_cast_implicit",  "time_cast_int8",
        "time_cast_ui1",  "time_cast_ui2",  "time_cast_ui4",  "time_cast_ui8",  "time_cmp",  "time_date",
        "timedate_pl",  "time_datetime",  "time_date_xor",  "timediff",  "time_div_numeric",  "time_enum",  "time_eq",
        "time_eq_timestamp",  "time_eq_timestamptz",  "time_float",  "time_float4",  "time_format",  "time_ge",
        "time_ge_timestamp",  "time_ge_timestamptz",  "time_gt",  "time_gt_timestamp",  "time_gt_timestamptz",
        "time_hash",  "time_in",  "time_int1",  "time_int2",  "time_int8",  "time_int8_xor",  "time_integer",
        "time_json",  "time_larger",  "time_le",  "time_le_timestamp",  "time_le_timestamptz",  "time_lt",
        "time_lt_timestamp",  "time_lt_timestamptz",  "timemi",  "time_mi_float",  "time_mi_interval",
        "time_mi_time",  "time_mul_numeric",  "time_mysql",  "time_ne",  "time_ne_timestamp",  "time_ne_timestamptz",
        "timenow",  "time_numeric",  "timeofday",  "time_out",  "timepl",  "time_pl_float",  "time_pl_interval",
        "time_recv",  "time_send",  "time_smaller",  "timestamp",  "timestamp_add",  "timestamp_agg_finalfn",
        "timestamp_any_value",  "timestamp_bit_ge",  "timestamp_bit_gt",  "timestamp_bit_le",  "timestamp_bit_lt",
        "timestamp_blob_cmp",  "timestamp_blob_eq",  "timestamp_blob_ge",  "timestamp_blob_gt",  "timestamp_blob_le",
        "timestamp_blob_lt",  "timestamp_blob_ne",  "timestamp_bool",  "timestamp_bpchar",  "timestamp_cast",
        "timestamp_cast_int8",  "timestamp_cmp",  "timestamp_cmp_date",  "timestamp_cmp_timestamptz",
        "timestamp_diff",  "timestamp_double_eq",  "timestamp_double_ge",  "timestamp_double_gt",
        "timestamp_double_le",  "timestamp_double_lt",  "timestamp_double_ne",  "timestamp_enum",  "timestamp_eq",
        "timestamp_eq_date",  "timestamp_eq_time",  "timestamp_eq_timestamptz",  "timestamp_explicit",
        "timestamp_float8_xor",  "timestamp_ge",  "timestamp_ge_date",  "timestamp_ge_time",
        "timestamp_ge_timestamptz",  "timestamp_gt",  "timestamp_gt_date",  "timestamp_gt_time",
        "timestamp_gt_timestamptz",  "timestamp_hash",  "timestamp_in",  "timestamp_int8_xor",  "timestamp_json",
        "timestamp_larger",  "timestamp_le",  "timestamp_le_date",  "timestamp_le_time",  "timestamp_le_timestamptz",
        "timestamp_list_agg_noarg2_transfn",  "timestamp_list_agg_transfn",  "timestamp_longblob_cmp",
        "timestamp_longblob_eq",  "timestamp_longblob_ge",  "timestamp_longblob_gt",  "timestamp_longblob_le",
        "timestamp_longblob_lt",  "timestamp_longblob_ne",  "timestamp_lt",  "timestamp_lt_date",
        "timestamp_lt_time",  "timestamp_lt_timestamptz",  "timestamp_mediumblob_cmp",  "timestamp_mediumblob_eq",
        "timestamp_mediumblob_ge",  "timestamp_mediumblob_gt",  "timestamp_mediumblob_le",  "timestamp_mediumblob_lt",
        "timestamp_mediumblob_ne",  "timestamp_mi",  "timestamp_mi_int4",  "timestamp_mi_interval",
        "timestamp_mysql",  "timestamp_ne",  "timestamp_ne_date",  "timestamp_ne_time",  "timestamp_ne_timestamptz",
        "timestamp_numeric",  "timestamp_out",  "timestamp_pl_int4",  "timestamp_pl_interval",  "timestamp_recv",
        "timestamp_send",  "timestamp_smaller",  "timestamp_sortsupport",  "timestamp_support",  "timestamp_text",
        "timestamp_text_eq",  "timestamp_text_ge",  "timestamp_text_gt",  "timestamp_text_le",  "timestamp_text_lt",
        "timestamp_text_ne",  "timestamp_text_xor",  "timestamp_tinyblob_cmp",  "timestamp_tinyblob_eq",
        "timestamp_tinyblob_ge",  "timestamp_tinyblob_gt",  "timestamp_tinyblob_le",  "timestamp_tinyblob_lt",
        "timestamp_tinyblob_ne",  "timestamp_to_smalldatetime",  "timestamptypmodin",  "timestamptypmodout",
        "timestamptz",  "timestamptz_any_value",  "timestamptz_bit_ge",  "timestamptz_bit_gt",  "timestamptz_bit_le",
        "timestamptz_bit_lt",  "timestamptz_blob_xor",  "timestamptz_bool",  "timestamptz_bool_xor",
        "timestamptz_bpchar",  "timestamptz_cast",  "timestamptz_cast_int8",  "timestamptz_cmp",
        "timestamptz_cmp_date",  "timestamptz_cmp_timestamp",  "timestamptz_eq",  "timestamptz_eq_date",
        "timestamptz_eq_time",  "timestamptz_eq_timestamp",  "timestamptz_explicit",  "timestamptz_float4",
        "timestamptz_float8",  "timestamptz_float8_xor",  "timestamptz_ge",  "timestamptz_ge_date",
        "timestamptz_ge_time",  "timestamptz_ge_timestamp",  "timestamptz_gt",  "timestamptz_gt_date",
        "timestamptz_gt_time",  "timestamptz_gt_timestamp",  "timestamptz_in",  "timestamptz_int1",
        "timestamptz_int2",  "timestamptz_int4",  "timestamptz_int8",  "timestamptz_int8_xor",  "timestamptz_larger",
        "timestamptz_le",  "timestamptz_le_date",  "timestamptz_le_time",  "timestamptz_le_timestamp",
        "timestamptz_list_agg_noarg2_transfn",  "timestamptz_list_agg_transfn",  "timestamptz_lt",
        "timestamptz_lt_date",  "timestamptz_lt_time",  "timestamptz_lt_timestamp",  "timestamptz_mi",
        "timestamptz_mi_interval",  "timestamptz_ne",  "timestamptz_ne_date",  "timestamptz_ne_time",
        "timestamptz_ne_timestamp",  "timestamptz_numeric",  "timestamptz_out",  "timestamptz_pl_interval",
        "timestamptz_recv",  "timestamptz_send",  "timestamptz_smaller",  "timestamptz_text_xor",
        "timestamptz_time_xor",  "timestamptz_to_smalldatetime",  "timestamptztypmodin",  "timestamptztypmodout",
        "timestamptz_uint1",  "timestamptz_uint2",  "timestamptz_uint4",  "timestamptz_varchar",  "timestamptzxor",
        "timestamp_uint8",  "timestamp_uint8_eq",  "timestamp_uint8_ge",  "timestamp_uint8_gt",  "timestamp_uint8_le",
        "timestamp_uint8_lt",  "timestamp_uint8_ne",  "timestamp_varchar",  "timestampxor",  "timestamp_xor_transfn",
        "timestamp_year",  "timestampzone_text",  "time_support",  "time_text",  "time_text_xor",  "time_timestamp",
        "time_timestamptz_xor",  "time_to_sec",  "timetypmodin",  "timetypmodout",  "timetz",  "timetz_any_value",
        "timetz_cast_int8",  "timetz_cmp",  "timetzdate_pl",  "timetz_eq",  "timetz_float8",  "timetz_ge",
        "timetz_gt",  "timetz_hash",  "timetz_in",  "timetz_int8",  "timetz_larger",  "timetz_le",  "timetz_lt",
        "timetz_mi_interval",  "timetz_ne",  "timetz_numeric",  "timetz_out",  "timetz_pl_interval",  "timetz_recv",
        "timetz_send",  "timetz_smaller",  "timetz_text",  "timetztypmodin",  "timetztypmodout",
        "timetz_xor_transfn",  "time_uint1",  "time_uint1_eq",  "time_uint1_ge",  "time_uint1_gt",  "time_uint1_le",
        "time_uint1_lt",  "time_uint1_ne",  "time_uint2",  "time_uint2_eq",  "time_uint2_ge",  "time_uint2_gt",
        "time_uint2_le",  "time_uint2_lt",  "time_uint2_ne",  "time_uint4",  "time_uint8",  "time_varchar",
        "timexor",  "time_xor_bit",  "time_xor_transfn",  "time_year",  "timezone",  "timezone_extract",  "tinterval",
        "tintervalct",  "tintervalend",  "tintervaleq",  "tintervalge",  "tintervalgt",  "tintervalin",
        "tintervalle",  "tintervalleneq",  "tintervallenge",  "tintervallengt",  "tintervallenle",  "tintervallenlt",
        "tintervallenne",  "tintervallt",  "tintervalne",  "tintervalout",  "tintervalov",  "tintervalrecv",
        "tintervalrel",  "tintervalsame",  "tintervalsend",  "tintervalstart",  "tinyblob_blob_cmp",
        "tinyblob_blob_eq",  "tinyblob_blob_ge",  "tinyblob_blob_gt",  "tinyblob_blob_le",  "tinyblob_blob_lt",
        "tinyblob_blob_ne",  "tinyblob_boolean_eq",  "tinyblob_boolean_ge",  "tinyblob_boolean_gt",
        "tinyblob_boolean_le",  "tinyblob_boolean_lt",  "tinyblob_boolean_ne",  "tinyblob_cmp",
        "tinyblob_datetime_cmp",  "tinyblob_datetime_eq",  "tinyblob_datetime_ge",  "tinyblob_datetime_gt",
        "tinyblob_datetime_le",  "tinyblob_datetime_lt",  "tinyblob_datetime_ne",  "tinyblob_eq",  "tinyblob_eq_text",
        "tinyblob_float8_cmp",  "tinyblob_float8_eq",  "tinyblob_float8_ge",  "tinyblob_float8_gt",
        "tinyblob_float8_le",  "tinyblob_float8_lt",  "tinyblob_float8_ne",  "tinyblob_ge",  "tinyblob_ge_text",
        "tinyblob_gt",  "tinyblob_gt_text",  "tinyblob_int8_cmp",  "tinyblob_int8_eq",  "tinyblob_int8_ge",
        "tinyblob_int8_gt",  "tinyblob_int8_le",  "tinyblob_int8_lt",  "tinyblob_int8_ne",  "tinyblob_json",
        "tinyblob_larger",  "tinyblob_le",  "tinyblob_le_text",  "tinyblob_longblob_cmp",  "tinyblob_longblob_eq",
        "tinyblob_longblob_ge",  "tinyblob_longblob_gt",  "tinyblob_longblob_le",  "tinyblob_longblob_lt",
        "tinyblob_longblob_ne",  "tinyblob_lt",  "tinyblob_lt_text",  "tinyblob_mediumblob_cmp",
        "tinyblob_mediumblob_eq",  "tinyblob_mediumblob_ge",  "tinyblob_mediumblob_gt",  "tinyblob_mediumblob_le",
        "tinyblob_mediumblob_lt",  "tinyblob_mediumblob_ne",  "tinyblob_ne",  "tinyblob_ne_text",
        "tinyblob_numeric_cmp",  "tinyblob_numeric_eq",  "tinyblob_numeric_ge",  "tinyblob_numeric_gt",
        "tinyblob_numeric_le",  "tinyblob_numeric_lt",  "tinyblob_numeric_ne",  "tinyblob_rawin",  "tinyblob_rawout",
        "tinyblob_recv",  "tinyblob_send",  "tinyblob_smaller",  "tinyblob_timestamp_cmp",  "tinyblob_timestamp_eq",
        "tinyblob_timestamp_ge",  "tinyblob_timestamp_gt",  "tinyblob_timestamp_le",  "tinyblob_timestamp_lt",
        "tinyblob_timestamp_ne",  "tinyblob_uint8_cmp",  "tinyblob_uint8_eq",  "tinyblob_uint8_ge",
        "tinyblob_uint8_gt",  "tinyblob_uint8_le",  "tinyblob_uint8_lt",  "tinyblob_uint8_ne",
        "tinyblob_xor_tinyblob",  "tinyblob_year_eq",  "tinyblob_year_ge",  "tinyblob_year_gt",  "tinyblob_year_le",
        "tinyblob_year_lt",  "tinyblob_year_ne",  "tinyint_sum",  "to_ascii",  "to_base64",  "to_bigint",
        "to_binary",  "to_binary_float",  "to_char",  "to_clob",  "to_date",  "to_days",  "to_hex",  "to_integer",
        "to_interval",  "to_json",  "to_mediumblob",  "to_number",  "to_numeric",  "to_nvarchar2",  "to_seconds",
        "total_cpu",  "total_memory",  "to_text",  "to_timestamp",  "to_tinyblob",  "to_ts",  "to_tsquery",
        "to_tsvector",  "to_tsvector_for_batch",  "to_varbinary",  "to_varchar",  "to_varchar2",
        "track_memory_context",  "track_memory_context_detail",  "track_model_train_opt",  "transaction_timestamp",
        "translate",  "trigger_in",  "trigger_out",  "trunc",  "truncate",  "ts_debug",  "ts_headline",  "ts_lexize",
        "tsmatchjoinsel",  "ts_match_qv",  "tsmatchsel",  "ts_match_tq",  "ts_match_tt",  "ts_match_vq",  "ts_parse",
        "tsq_mcontained",  "tsq_mcontains",  "tsquery_and",  "tsquery_cmp",  "tsquery_eq",  "tsquery_ge",
        "tsquery_gt",  "tsqueryin",  "tsquery_le",  "tsquery_lt",  "tsquery_ne",  "tsquery_not",  "tsquery_or",
        "tsqueryout",  "tsqueryrecv",  "tsquerysend",  "tsrange",  "tsrange_subdiff",  "ts_rank",  "ts_rank_cd",
        "ts_rewrite",  "ts_stat",  "ts_token_type",  "ts_typanalyze",  "tstzrange",  "tstzrange_subdiff",
        "tsvector_cmp",  "tsvector_concat",  "tsvector_eq",  "tsvector_ge",  "tsvector_gt",  "tsvectorin",
        "tsvector_le",  "tsvector_lt",  "tsvector_ne",  "tsvectorout",  "tsvectorrecv",  "tsvectorsend",
        "tsvector_update_trigger",  "tsvector_update_trigger_column",  "turn_off",  "turn_on",  "txid_current",
        "txid_current_snapshot",  "txid_snapshot_in",  "txid_snapshot_out",  "txid_snapshot_recv",
        "txid_snapshot_send",  "txid_snapshot_xip",  "txid_snapshot_xmax",  "txid_snapshot_xmin",
        "txid_visible_in_snapshot",  "ubtbeginscan",  "ubtbuild",  "ubtbuildempty",  "ubtbulkdelete",  "ubtcanreturn",
        "ubtcostestimate",  "ubtendscan",  "ubtgetbitmap",  "ubtgettuple",  "ubtinsert",  "ubtmarkpos",  "ubtmerge",
        "ubtoptions",  "ubtrescan",  "ubtrestrpos",  "ubtvacuumcleanup",  "ucase",  "uhc_to_utf8",  "ui1tof4",
        "ui1tof8",  "ui1toi1",  "ui1toi2",  "ui1toi4",  "ui1toi8",  "ui1toui2",  "ui1toui4",  "ui1toui8",  "ui2tof4",
        "ui2tof8",  "ui2toi1",  "ui2toi2",  "ui2toi4",  "ui2toi8",  "ui2toui1",  "ui2toui4",  "ui2toui8",  "ui4tof4",
        "ui4tof8",  "ui4toi1",  "ui4toi2",  "ui4toi4",  "ui4toi8",  "ui4toui1",  "ui4toui2",  "ui4toui8",  "ui8tof4",
        "ui8tof8",  "ui8toi1",  "ui8toi2",  "ui8toi4",  "ui8toi8",  "ui8toui1",  "ui8toui2",  "ui8toui4",  "uint1",
        "uint12cmp",  "uint14cmp",  "uint_16",  "uint16_cast_date",  "uint16_cast_datetime",  "uint16_cast_time",
        "uint16_cast_timestamptz",  "uint18cmp",  "uint1abs",  "uint1_accum",  "uint1and",  "uint1_and_int1",
        "uint1_avg_accum",  "uint1_binary_ge",  "uint1_binary_gt",  "uint1_binary_le",  "uint1_binary_lt",
        "uint1_bool",  "uint1cmp",  "uint1_date_ne",  "uint1div",  "uint1_div_int1",  "uint1_enum",  "uint1eq",
        "uint1ge",  "uint1gt",  "uint1in",  "uint1_int1cmp",  "uint1_int1_eq",  "uint1_int1_ge",  "uint1_int1_gt",
        "uint1_int1_le",  "uint1_int1_lt",  "uint1_int1_mod",  "uint1_int1_ne",  "uint1_int2cmp",  "uint1_int2_eq",
        "uint1_int2_ge",  "uint1_int2_gt",  "uint1_int2_le",  "uint1_int2_lt",  "uint1_int4cmp",  "uint1_int4_eq",
        "uint1_int4_ge",  "uint1_int4_gt",  "uint1_int4_le",  "uint1_int4_lt",  "uint1_int8cmp",  "uint1_int8_eq",
        "uint1_int8_ge",  "uint1_int8_gt",  "uint1_int8_le",  "uint1_int8_lt",  "uint1_json",  "uint1larger",
        "uint1le",  "uint1_list_agg_noarg2_transfn",  "uint1_list_agg_transfn",  "uint1lt",  "uint1mi",
        "uint1_mi_int1",  "uint1mod",  "uint1mul",  "uint1_mul_int1",  "uint1ne",  "uint1not",  "uint1_numeric",
        "uint1or",  "uint1_or_int1",  "uint1out",  "uint1pl",  "uint1_pl_int1",  "uint1recv",  "uint1send",
        "uint1shl",  "uint1shr",  "uint1smaller",  "uint1_sortsupport",  "uint1_sum",  "uint1_time_ne",
        "uint1_uint2_eq",  "uint1_uint2_ge",  "uint1_uint2_gt",  "uint1_uint2_le",  "uint1_uint2_lt",
        "uint1_uint4_eq",  "uint1_uint4_ge",  "uint1_uint4_gt",  "uint1_uint4_le",  "uint1_uint4_lt",
        "uint1_uint8_eq",  "uint1_uint8_ge",  "uint1_uint8_gt",  "uint1_uint8_le",  "uint1_uint8_lt",  "uint1um",
        "uint1up",  "uint1xor",  "uint1_xor_bool",  "uint1_xor_int1",  "uint2",  "uint24cmp",  "uint28cmp",
        "uint2abs",  "uint2_accum",  "uint2and",  "uint2_and_int2",  "uint2_avg_accum",  "uint2_binary_ge",
        "uint2_binary_gt",  "uint2_binary_le",  "uint2_binary_lt",  "uint2_bool",  "uint2cmp",  "uint2_date_ne",
        "uint2div",  "uint2_div_int2",  "uint2_enum",  "uint2eq",  "uint2ge",  "uint2gt",  "uint2in",
        "uint2_int2cmp",  "uint2_int2_eq",  "uint2_int2_ge",  "uint2_int2_gt",  "uint2_int2_le",  "uint2_int2_lt",
        "uint2_int2_mod",  "uint2_int2_ne",  "uint2_int4cmp",  "uint2_int4_eq",  "uint2_int4_ge",  "uint2_int4_gt",
        "uint2_int4_le",  "uint2_int4_lt",  "uint2_int8cmp",  "uint2_int8_eq",  "uint2_int8_ge",  "uint2_int8_gt",
        "uint2_int8_le",  "uint2_int8_lt",  "uint2_json",  "uint2larger",  "uint2le",
        "uint2_list_agg_noarg2_transfn",  "uint2_list_agg_transfn",  "uint2lt",  "uint2mi",  "uint2_mi_int2",
        "uint2mod",  "uint2mul",  "uint2_mul_int2",  "uint2ne",  "uint2not",  "uint2_numeric",  "uint2or",
        "uint2_or_int2",  "uint2out",  "uint2pl",  "uint2_pl_int2",  "uint2recv",  "uint2send",  "uint2shl",
        "uint2shr",  "uint2smaller",  "uint2_sortsupport",  "uint2_sum",  "uint2_time_ne",  "uint2_uint4_eq",
        "uint2_uint4_ge",  "uint2_uint4_gt",  "uint2_uint4_le",  "uint2_uint4_lt",  "uint2_uint8_eq",
        "uint2_uint8_ge",  "uint2_uint8_gt",  "uint2_uint8_le",  "uint2_uint8_lt",  "uint2um",  "uint2up",
        "uint2xor",  "uint2_xor_bool",  "uint2_xor_int2",  "uint32_cast_date",  "uint32_cast_datetime",
        "uint32_cast_time",  "uint32_cast_timestamptz",  "uint4",  "uint48cmp",  "uint4abs",  "uint4_accum",
        "uint4and",  "uint4_and_int4",  "uint4_and_nvarchar2",  "uint4_and_year",  "uint4_avg_accum",
        "uint4_binary_ge",  "uint4_binary_gt",  "uint4_binary_le",  "uint4_binary_lt",  "uint4_bool",  "uint4cmp",
        "uint4div",  "uint4_div_int4",  "uint4_enum",  "uint4eq",  "uint4ge",  "uint4gt",  "uint4in",
        "uint4_int4cmp",  "uint4_int4_eq",  "uint4_int4_ge",  "uint4_int4_gt",  "uint4_int4_le",  "uint4_int4_lt",
        "uint4_int4_mod",  "uint4_int4_ne",  "uint4_int8cmp",  "uint4_int8_eq",  "uint4_int8_ge",  "uint4_int8_gt",
        "uint4_int8_le",  "uint4_int8_lt",  "uint4_json",  "uint4larger",  "uint4le",
        "uint4_list_agg_noarg2_transfn",  "uint4_list_agg_transfn",  "uint4lt",  "uint4mi",  "uint4_mi_int4",
        "uint4mod",  "uint4_mod_year",  "uint4mul",  "uint4_mul_int4",  "uint4ne",  "uint4not",  "uint4_numeric",
        "uint4or",  "uint4_or_int4",  "uint4_or_nvarchar2",  "uint4_or_year",  "uint4out",  "uint4pl",
        "uint4_pl_int4",  "uint4recv",  "uint4send",  "uint4shl",  "uint4shr",  "uint4smaller",  "uint4_sortsupport",
        "uint4_sum",  "uint4_uint8_eq",  "uint4_uint8_ge",  "uint4_uint8_gt",  "uint4_uint8_le",  "uint4_uint8_lt",
        "uint4um",  "uint4up",  "uint4xor",  "uint4_xor_bool",  "uint4_xor_int4",  "uint4_xor_year",  "uint4_year",
        "uint64_cast_date",  "uint64_cast_datetime",  "uint64_cast_time",  "uint64_cast_timestamptz",  "uint8",
        "uint8abs",  "uint8_accum",  "uint8and",  "uint8_and_int8",  "uint8_avg_accum",  "uint8_binary_ge",
        "uint8_binary_gt",  "uint8_binary_le",  "uint8_binary_lt",  "uint8_bit_ge",  "uint8_bit_gt",  "uint8_bit_le",
        "uint8_bit_lt",  "uint8_blob_cmp",  "uint8_blob_eq",  "uint8_blob_ge",  "uint8_blob_gt",  "uint8_blob_le",
        "uint8_blob_lt",  "uint8_blob_ne",  "uint8_bool",  "uint8_cast_date",  "uint8_cast_datetime",
        "uint8_cast_int8",  "uint8_cast_time",  "uint8_cast_timestamptz",  "uint8cmp",  "uint8div",  "uint8_div_int8",
        "uint8_enum",  "uint8eq",  "uint8ge",  "uint8gt",  "uint8in",  "uint8_int8cmp",  "uint8_int8_eq",
        "uint8_int8_ge",  "uint8_int8_gt",  "uint8_int8_le",  "uint8_int8_lt",  "uint8_int8_mod",  "uint8_int8_ne",
        "uint8_json",  "uint8larger",  "uint8le",  "uint8_list_agg_noarg2_transfn",  "uint8_list_agg_transfn",
        "uint8_longblob_cmp",  "uint8_longblob_eq",  "uint8_longblob_ge",  "uint8_longblob_gt",  "uint8_longblob_le",
        "uint8_longblob_lt",  "uint8_longblob_ne",  "uint8lt",  "uint8_mediumblob_cmp",  "uint8_mediumblob_eq",
        "uint8_mediumblob_ge",  "uint8_mediumblob_gt",  "uint8_mediumblob_le",  "uint8_mediumblob_lt",
        "uint8_mediumblob_ne",  "uint8mi",  "uint8_mi_int8",  "uint8mod",  "uint8mul",  "uint8_mul_int8",  "uint8ne",
        "uint8not",  "uint8_numeric",  "uint8or",  "uint8_or_binary",  "uint8_or_int8",  "uint8out",  "uint8pl",
        "uint8_pl_int8",  "uint8recv",  "uint8send",  "uint8shl",  "uint8shr",  "uint8smaller",  "uint8_sortsupport",
        "uint8_sum",  "uint8_tinyblob_cmp",  "uint8_tinyblob_eq",  "uint8_tinyblob_ge",  "uint8_tinyblob_gt",
        "uint8_tinyblob_le",  "uint8_tinyblob_lt",  "uint8_tinyblob_ne",  "uint8_uint4_eq",  "uint8um",  "uint8up",
        "uint8_xor",  "uint8xor",  "uint8_xor_bool",  "uint8_xor_int8",  "uint_any_value",  "uint_cash",
        "uncompress",  "uncompressed_length",  "undefinedin",  "undefinedout",  "undefinedrecv",  "undefinedsend",
        "unhex",  "unique_key_recheck",  "unix_timestamp",  "unknownin",  "unknownout",  "unknownrecv",
        "unknownsend",  "unlock_cluster_ddl",  "unnest",  "update_pgjob",  "upper",  "upper_inc",  "upper_inf",
        "user",  "utc_date_func",  "utc_time_func",  "utc_timestamp_func",  "utf8_to_ascii",  "utf8_to_big5",
        "utf8_to_euc_cn",  "utf8_to_euc_jis_2004",  "utf8_to_euc_jp",  "utf8_to_euc_kr",  "utf8_to_euc_tw",
        "utf8_to_gb18030",  "utf8_to_gb18030_2022",  "utf8_to_gbk",  "utf8_to_iso8859",  "utf8_to_iso8859_1",
        "utf8_to_johab",  "utf8_to_koi8r",  "utf8_to_koi8u",  "utf8_to_shift_jis_2004",  "utf8_to_sjis",
        "utf8_to_uhc",  "utf8_to_win",  "uuid",  "uuid_cmp",  "uuid_eq",  "uuid_ge",  "uuid_gt",  "uuid_hash",
        "uuid_in",  "uuid_le",  "uuid_lt",  "uuid_ne",  "uuid_out",  "uuid_recv",  "uuid_send",  "uuid_short",
        "value",  "value_of_percentile",  "varbinary2boolean",  "varbinaryand",  "varbinary_and_binary",
        "varbinary_and_blob",  "varbinary_and_longblob",  "varbinary_and_mediumblob",  "varbinary_and_tinyblob",
        "varbinary_binary_eq",  "varbinary_binary_ge",  "varbinary_binary_gt",  "varbinary_binary_le",
        "varbinary_binary_lt",  "varbinary_binary_ne",  "varbinary_cmp",  "varbinary_in",  "varbinary_json",
        "varbinary_larger",  "varbinarylike",  "varbinary_out",  "varbinary_recv",  "varbinary_send",
        "varbinary_smaller",  "varbinary_text_eq",  "varbinary_text_ge",  "varbinary_text_gt",  "varbinary_text_le",
        "varbinarytextlike",  "varbinary_text_lt",  "varbinary_text_ne",  "varbinary_typmodin",
        "varbinary_varbinary_eq",  "varbinary_varbinary_ge",  "varbinary_varbinary_gt",  "varbinary_varbinary_le",
        "varbinary_varbinary_lt",  "varbinary_varbinary_ne",  "varbinary_xor_varbinary",  "varbin_bin_concat",
        "varbin_bit_concat",  "varbin_bool_concat",  "varbin_concat",  "varbin_float4_concat",
        "varbin_float8_concat",  "varbin_int8_concat",  "varbin_int_concat",  "varbin_num_concat",
        "varbin_text_concat",  "varbit",  "varbit_bit_xor_finalfn",  "varbit_bit_xor_transfn",  "varbitcmp",
        "varbiteq",  "varbitge",  "varbitgt",  "varbit_in",  "varbit_larger",  "varbitle",  "varbitlt",  "varbitne",
        "varbit_out",  "varbit_recv",  "varbit_send",  "varbit_smaller",  "varbit_support",  "varbittypmodin",
        "varbittypmodout",  "varchar",  "varchar2_to_smlldatetime",  "varchar_bool",  "varchar_cast_int8",
        "varchar_cast_ui1",  "varchar_cast_ui2",  "varchar_cast_ui4",  "varchar_cast_ui8",  "varchar_date",
        "varchar_enum",  "varchar_float4",  "varchar_float8",  "varcharin",  "varchar_int1",  "varchar_int2",
        "varchar_int4",  "varchar_int8",  "varchar_json",  "varchar_larger",  "varchar_numeric",  "varcharout",
        "varcharrecv",  "varcharsend",  "varchar_smaller",  "varchar_support",  "varchar_time",  "varchar_timestamp",
        "varchar_timestamptz",  "varchartypmodin",  "varchartypmodout",  "varchar_uint1",  "varchar_uint2",
        "varchar_uint4",  "varchar_uint8",  "varchar_year",  "variance",  "varlena2bit",  "varlena2bpchar",
        "varlena2date",  "varlena2datetime",  "varlena2float4",  "varlena2float8",  "varlena2int1",  "varlena2int2",
        "varlena2int4",  "varlena2int8",  "varlena2numeric",  "varlena2text",  "varlena2time",  "varlena2timestamptz",
        "varlena2ui1",  "varlena2ui2",  "varlena2ui4",  "varlena2ui8",  "varlena2varchar",  "varlena2year",
        "varlena_cast_int1",  "varlena_cast_int2",  "varlena_cast_int4",  "varlena_cast_int8",  "varlena_cast_ui1",
        "varlena_cast_ui2",  "varlena_cast_ui4",  "varlena_cast_ui8",  "varlena_enum",  "varlena_json",
        "varlenatoset",  "var_pop",  "var_samp",  "vector",  "vector_accum",  "vector_add",  "vector_avg",
        "vector_cmp",  "vector_combine",  "vector_concat",  "vector_dims",  "vector_eq",  "vector_ge",  "vector_gt",
        "vector_in",  "vector_l2_squared_distance",  "vector_le",  "vector_lt",  "vector_mul",  "vector_ne",
        "vector_negative_inner_product",  "vector_norm",  "vector_out",  "vector_recv",  "vector_send",
        "vector_spherical_distance",  "vector_sub",  "vector_to_float4",  "vector_to_float8",  "vector_to_int4",
        "vector_to_numeric",  "vector_to_sparsevec",  "vector_typmod_in",  "version",  "void_in",  "void_out",
        "void_recv",  "void_send",  "wdr_xdb_query",  "week",  "weekday",  "weekofyear",  "weight_string",  "width",
        "width_bucket",  "win1250_to_latin2",  "win1250_to_mic",  "win1251_to_iso",  "win1251_to_koi8r",
        "win1251_to_mic",  "win1251_to_win866",  "win866_to_iso",  "win866_to_koi8r",  "win866_to_mic",
        "win866_to_win1251",  "win_to_utf8",  "working_version_num",  "xideq",  "xideq4",  "xideqint4",  "xideqint8",
        "xidin",  "xidin4",  "xidlt",  "xidlt4",  "xidout",  "xidout4",  "xidrecv",  "xidrecv4",  "xidsend",
        "xidsend4",  "xml",  "xmlagg",  "xmlcomment",  "xmlconcat2",  "xmlexists",  "xml_in",  "xml_is_well_formed",
        "xml_is_well_formed_content",  "xml_is_well_formed_document",  "xml_out",  "xml_recv",  "xml_send",
        "xmlsequence",  "xmltype",  "xmlvalidate",  "xor",  "xpath",  "xpath_exists",  "year",  "year_and_int8",
        "year_and_uint4",  "year_any_value",  "year_binary_eq",  "year_binary_ge",  "year_binary_gt",
        "year_binary_le",  "year_binary_lt",  "year_binary_ne",  "year_bit_ge",  "year_bit_gt",  "year_bit_le",
        "year_bit_lt",  "year_blob_eq",  "year_blob_ge",  "year_blob_gt",  "year_blob_le",  "year_blob_lt",
        "year_blob_ne",  "year_cast_int8",  "year_cmp",  "year_date",  "year_datetime",  "year_enum",  "year_eq",
        "year_float4",  "year_float8",  "year_ge",  "year_gt",  "year_in",  "year_int16",  "year_int64",  "year_int8",
        "year_integer",  "year_json",  "year_larger",  "year_le",  "year_longblob_eq",  "year_longblob_ge",
        "year_longblob_gt",  "year_longblob_le",  "year_longblob_lt",  "year_longblob_ne",  "year_lt",
        "year_mediumblob_eq",  "year_mediumblob_ge",  "year_mediumblob_gt",  "year_mediumblob_le",
        "year_mediumblob_lt",  "year_mediumblob_ne",  "year_mi",  "year_mi_interval",  "year_ne",  "year_numeric",
        "year_or_int8",  "year_or_uint4",  "year_out",  "year_pl_interval",  "year_recv",  "year_send",
        "year_smaller",  "year_sortsupport",  "year_sum",  "year_time",  "year_timestamp",  "year_tinyblob_eq",
        "year_tinyblob_ge",  "year_tinyblob_gt",  "year_tinyblob_le",  "year_tinyblob_lt",  "year_tinyblob_ne",
        "year_to_bool",  "yeartypmodin",  "yeartypmodout",  "year_uint1",  "year_uint2",  "year_uint4",  "year_uint8",
        "yearweek",  "year_xor_int8",  "year_xor_transfn",  "year_xor_uint4",  "zhprs_end",  "zhprs_getlexeme",
        "zhprs_lextype",  "zhprs_start"
    };
    parser_walker_context context;
    char *token = NULL;
    char *save_ptr = NULL;
    char *delims = " ";
    char* typelist = strdup(typelist_p);
    token = strtok_r(typelist, delims, &save_ptr);  
    while(token != NULL) {  
        typeWhitelist.insert(token);
        token = strtok_r(NULL, delims, &save_ptr);  
    }
    char* funclist = strdup(funclist_p);
    token = strtok_r(funclist, delims, &save_ptr);  
    while(token != NULL) {  
        funcWhitelist.insert(token);
        token = strtok_r(NULL, delims, &save_ptr);  
    }

    context.has_error = false;
    context.typeWhitelist = &typeWhitelist;
    context.funcWhitelist = &funcWhitelist;
    List* parsetree_list = raw_parser_opengauss(str, &context);
    free(typelist);
    free(funclist);
    return (parsetree_list != nullptr);
}
#endif


THR_LOCAL PGDLLIMPORT SnapshotData SnapshotNowData = {};
THR_LOCAL conn_mysql_infoP_t temp_Conn_Mysql_Info = nullptr;

void FlushErrorState(void)
{
    /*
     * Reset stack to empty.  The only case where it would be more than one
     * deep is if we serviced an error that interrupted construction of
     * another message.  We assume control escaped out of that message
     * construction and won't ever go back.
     */
    t_thrd.log_cxt.errordata_stack_depth = -1;
    t_thrd.log_cxt.recursion_depth = 0;
    /* Delete all data in ErrorContext */
    MemoryContextResetAndDeleteChildren(ErrorContext);
}

#define CHECK_STACK_DEPTH()                                               \
    do {                                                                  \
        if (t_thrd.log_cxt.errordata_stack_depth < 0) {                   \
            t_thrd.log_cxt.errordata_stack_depth = -1;                    \
            ereport(ERROR, (errmsg_internal("errstart was not called"))); \
        }                                                                 \
    } while (0)

ErrorData* CopyErrorData(void)
{
    ErrorData* edata = &t_thrd.log_cxt.errordata[t_thrd.log_cxt.errordata_stack_depth];
    ErrorData* newedata = NULL;

    CHECK_STACK_DEPTH();

    Assert(CurrentMemoryContext != ErrorContext);


    newedata = (ErrorData*)palloc(sizeof(ErrorData));
    errno_t rc = memcpy_s(newedata, sizeof(ErrorData), edata, sizeof(ErrorData));
    securec_check(rc, "\0", "\0");

    if (newedata->message)
        newedata->message = pstrdup(newedata->message);
    if (newedata->detail)
        newedata->detail = pstrdup(newedata->detail);
    if (newedata->detail_log)
        newedata->detail_log = pstrdup(newedata->detail_log);
    if (newedata->hint)
        newedata->hint = pstrdup(newedata->hint);
    if (newedata->context)
        newedata->context = pstrdup(newedata->context);
    if (newedata->internalquery)
        newedata->internalquery = pstrdup(newedata->internalquery);
    if (newedata->filename)
        newedata->filename = pstrdup(newedata->filename);
    if (newedata->funcname)
        newedata->funcname = pstrdup(newedata->funcname);
    if (newedata->backtrace_log)
        newedata->backtrace_log = pstrdup(newedata->backtrace_log);
    if (newedata->cause)
        newedata->cause = pstrdup(newedata->cause);
    if (newedata->action)
        newedata->action = pstrdup(newedata->action);
    if (edata->sqlstate)
        newedata->sqlstate = pstrdup(newedata->sqlstate);
    if (newedata->class_origin)
        newedata->class_origin = pstrdup(newedata->class_origin);
    if (newedata->subclass_origin)
        newedata->subclass_origin = pstrdup(newedata->subclass_origin);
    if (newedata->cons_catalog)
        newedata->cons_catalog = pstrdup(newedata->cons_catalog);
    if (newedata->cons_schema)
        newedata->cons_schema = pstrdup(newedata->cons_schema);
    if (newedata->cons_name)
        newedata->cons_name = pstrdup(newedata->cons_name);
    if (newedata->catalog_name)
        newedata->catalog_name = pstrdup(newedata->catalog_name);
    if (newedata->schema_name)
        newedata->schema_name = pstrdup(newedata->schema_name);
    if (newedata->table_name)
        newedata->table_name = pstrdup(newedata->table_name);
    if (newedata->column_name)
        newedata->column_name = pstrdup(newedata->column_name);
    if (newedata->cursor_name)
        newedata->cursor_name = pstrdup(newedata->cursor_name);
    if (newedata->mysql_errno)
        newedata->mysql_errno = pstrdup(newedata->mysql_errno);
    return newedata;
}


extern "C" Datum ui8toi4(FunctionCallInfo fcinfo);
Datum ui8toi4(FunctionCallInfo fcinfo)
{
    return 0;
}

int	ScanKeywordLookup(const char *text, const ScanKeywordList *keywords)
{
    return 0;
}

bool superuser(void)
{
    return false;
}

extern "C" Datum ui8toui4(FunctionCallInfo fcinfo);
Datum ui8toui4(FunctionCallInfo fcinfo)
{
    return 0;
}

SelectStmt *MakeShowTriggersQuery(List *args, Node *likeWhereOpt, bool isLikeExpr)
{
    return nullptr;
}

void LockRelationOid(Oid relid, LOCKMODE lockmode)
{

}

ForeignServer* GetForeignServerByName(const char* name, bool missing_ok)
{
    return nullptr;
}

void check_stack_depth(void)
{

}

Datum bi128_out(int128 data, int scale)
{
    return 0;
}

void DefineCustomBoolVariable(const char* name, const char* short_desc, const char* long_desc, bool* valueAddr,
    bool bootValue, GucContext context, int flags, GucBoolCheckHook check_hook, GucBoolAssignHook assign_hook,
    GucShowHook show_hook)
{

}

Relation RelationIdGetRelation(Oid relationId)
{
    return nullptr;
}

enum CmpType {CMP_STRING_TYPE, CMP_REAL_TYPE, CMP_INT_TYPE, CMP_DECIMAL_TYPE, CMP_UNKNOWN_TYPE};
CmpType map_oid_to_cmp_type(Oid oid, bool *unsigned_flag, bool *is_temporal_type)
{
    return CMP_UNKNOWN_TYPE;
}

void DefineCustomStringVariable(const char* name, const char* short_desc, const char* long_desc,
    char** valueAddr, const char* bootValue, GucContext context, int flags, GucStringCheckHook check_hook,
    GucStringAssignHook assign_hook, GucShowHook show_hook)
{

}

void DefineCustomEnumVariable(const char* name, const char* short_desc, const char* long_desc, int* valueAddr,
    int bootValue, const struct config_enum_entry* options, GucContext context, int flags, GucEnumCheckHook check_hook,
    GucEnumAssignHook assign_hook, GucShowHook show_hook)
{

}

void CopyCursorInfoData(Cursor_Data* target_data, Cursor_Data* source_data)
{

}

 uint32 pg_crc32c_hardware(uint32 crc, const void* data, Size len)
 {
    return 0;
 }

Oid PackageNameGetOid(const char* pkgname, Oid namespaceId)
{
    return 0;
}

void UnlockRelationOid(Oid relid, LOCKMODE lockmode)
{
    return;
}

void DefineCustomIntVariable(const char* name, const char* short_desc, const char* long_desc, int* valueAddr,
    int bootValue, int minValue, int maxValue, GucContext context, int flags, GucIntCheckHook check_hook,
    GucIntAssignHook assign_hook, GucShowHook show_hook)
{

}

void DefineCustomInt64Variable(const char* name, const char* short_desc, const char* long_desc, int64* valueAddr,
    int64 bootValue, int64 minValue, int64 maxValue, GucContext context, int flags, GucInt64CheckHook check_hook,
    GucInt64AssignHook assign_hook, GucShowHook show_hook)
{

}

void RepallocSessionVarsArrayIfNecessary()
{
    
}

AclResult pg_foreign_server_aclcheck(Oid srv_oid, Oid roleid, AclMode mode)
{
    return ACLCHECK_OK;
}

char* GetQualifiedSynonymName(Oid synOid, bool qualified)
{
    return nullptr;
}

void DefineCustomRealVariable(const char* name, const char* short_desc, const char* long_desc, double* valueAddr,
    double bootValue, double minValue, double maxValue, GucContext context, int flags, GucRealCheckHook check_hook,
    GucRealAssignHook assign_hook, GucShowHook show_hook)
{

}

uint64 pg_relation_table_size(Relation rel)
{

}

SelectStmt *MakeShowCollationQuery(List *args, Node *likeWhereOpt, bool isLikeExpr)
{
    return nullptr;
}

SelectStmt *MakeShowCharacterQuery(List *args, Node *likeWhereOpt, bool isLikeExpr)
{
    return nullptr;
}

SelectStmt *MakeShowFuncProQuery(List *args, Node *likeWhereOpt, bool isLikeExpr)
{
    return nullptr;
}

char* get_and_check_db_name(Oid dbid, bool is_ereport)
{
    return nullptr;
}

char* GetUserNameFromId(Oid roleid)
{
    return nullptr;
}

CmpType agg_cmp_type(CmpType a, CmpType b)
{
    return CMP_UNKNOWN_TYPE;
}

bool checkCompArgs(const char *cmptFmt)
{
    return false;
}

ColumnRef* makeColumnRef(char* relname, char* colname, int location)
{
    return nullptr;
}

bool resolve_units(char *unit_str, b_units *unit)
{
    return false;
}

MyLocale* MyLocaleSearch(char* target)
{
    return nullptr;
}

List* pg_parse_query(const char* query_string, List** query_string_locationlist,
                            List* (*parser_hook)(const char*, List**))
{
    return nullptr;
}

bool ExecCheckRTPerms(List* rangeTable, bool ereport_on_violation)
{
    return false;
}

Oid get_relname_relid(const char* relname, Oid relnamespace)
{
    return 0;
}

/* fmgr.h */

Datum DirectFunctionCall1Coll(PGFunction func, Oid collation, Datum arg1, bool can_ignore)
{
    Datum dummy = {0};
    return dummy;
}

Datum DirectFunctionCall3Coll(PGFunction func, Oid collation, Datum arg1, Datum arg2, Datum arg3, bool can_ignore)
{
    Datum dummy = {0};
    return dummy;
}

Datum FunctionCall5Coll(FmgrInfo* flinfo, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4, Datum arg5)
{
    Datum dummy = {0};
    return dummy;
}

Datum OidFunctionCall5Coll(
    Oid functionId, Oid collation, Datum arg1, Datum arg2, Datum arg3, Datum arg4, Datum arg5, bool can_ignore)
{
    Datum dummy = {0};
    return dummy;
}

/* gram.cpp */
bool isRestoreMode = false;
bool isSecurityMode = false;

Relation HeapOpenrvExtended(
    const RangeVar* relation, LOCKMODE lockmode, bool missing_ok, bool isSupportSynonym, StringInfo detailInfo)
{
    return nullptr;
}

void ReleaseSysCacheList(catclist *cl)
{
    return;
}

void ReleaseSysCache(HeapTuple tuple)
{
    return;
}

Oid get_am_oid(const char* amname, bool missing_ok)
{
    return 0;
}

/* executor.h */
syscalllock getpwuid_lock;
syscalllock env_lock;
syscalllock dlerror_lock;
syscalllock kerberos_conn_lock;
syscalllock read_cipher_lock;
syscalllock file_list_lock;

/* elog.cpp */
pthread_mutex_t bt_lock = PTHREAD_MUTEX_INITIALIZER;

void pgaudit_user_no_privileges(const char* object_name, const char* detailsinfo)
{
    return;
}

void pg_usleep(long microsec)
{
    return;
}

void proc_exit(int code)
{
    printf("FATAL: terminating process due to error.\n");
    exit(1);
}

/* partitionmap_gs.h */
void relation_close(Relation relation, LOCKMODE lockmode)
{
    return;
}

/* scan.inc */
int PthreadMutexLock(ResourceOwner owner, pthread_mutex_t* mutex, bool trace)
{
    return 0;
}

int ResourceOwnerForgetIfExistPthreadMutex(ResourceOwner owner, pthread_mutex_t* pMutex, bool trace)
{
    return 0;
}

List* CopyHotKeys(List* srcHotKeys)
{
    return nullptr;
}

/* elog.h */
bool module_logging_is_on(ModuleId module_id)
{
    return false;
}

/* syscache.h */
struct catclist* SearchSysCacheList(int cacheId, int nkeys, Datum key1, Datum key2, Datum key3, Datum key4)
{
    return nullptr;
}

Oid get_element_type(Oid typid)
{
    return typid;
}

/* copyfuncs.cpp */
PruningResult* copyPruningResult(PruningResult* srcPruningResult)
{
    return nullptr;
}

GlobalSysDBCache::GlobalSysDBCache()
{

}

StreamObj::StreamObj(MemoryContext context, StreamObjType type)
{

}

StreamProducer::StreamProducer(StreamKey key, PlannedStmt* pstmt, Stream* streamNode, MemoryContext context, int socketNum,
    StreamTransType type) : StreamObj(context, STREAM_PRODUCER)
{
    
}

void StreamProducer::reportError()
{

}

void StreamProducer::reportNotice()
{

}

ErrorData* StreamNodeGroup::getProducerEdata()
{
    return nullptr;
}

GlobalDBStatManager::GlobalDBStatManager()
{

}

struct varlena* pg_detoast_datum(struct varlena* datum)
{
    return nullptr;
}

bool IsStmtRetryAvaliable(int elevel, int sqlErrCode)
{
    return false;
}

/* s_loc.cpp */
int s_lock(volatile slock_t* lock, const char* file, int line)
{
    return 0;
}

/* superuser.cpp */
bool CheckExecDirectPrivilege(const char* query)
{
    return true;
}

int gs_close_all_stream_by_debug_id(uint64 query_id)
{
    return 0;
}

void InstanceTypeNameDependExtend(TypeDependExtend** dependExtend)
{
    return;
}

void releaseExplainTable()
{
    return;
}

void ThreadExitCXX(int code)
{
    pthread_exit((void*)(size_t)(code));
}

char* format_type_be(Oid type_oid)
{
    return nullptr;
}

THR_LOCAL bool assert_enabled = false;
THR_LOCAL int log_min_messages = WARNING;
THR_LOCAL int client_min_messages = NOTICE;
THR_LOCAL bool IsInitdb = false;
THR_LOCAL bool force_backtrace_messages = true;
THR_LOCAL MemoryContext SelfMemoryContext = NULL;

void AdjustThreadAffinity(void)
{

}

void CStoreMemAlloc::Init()
{

}

AlgorithmAPI *get_algorithm_api(AlgorithmML algorithm)
{
    return nullptr;
}

long gs_get_comm_used_memory(void)
{
    return 0;
}

UndoRecord::UndoRecord()
{

}
URecVector::URecVector()
{

}
void URecVector::Initialize(int capacity, bool isPrepared)
{

}
void URecVector::SetMemoryContext(MemoryContext mem_cxt)
{

}

bool URecVector::PushBack(UndoRecord* urec)
{
    return false;
}

Oid GetSysCacheOid(int cacheId, Datum key1, Datum key2, Datum key3, Datum key4)
{
    return 0;
}

PostgresInitializer::PostgresInitializer()
{

}

Size gs_get_comm_context_memory(void)
{
    return 0;
}

bool SearchSysCacheExists(int cacheId, Datum key1, Datum key2, Datum key3, Datum key4)
{
    return false;
}

int GetCurrentTransactionNestLevel(void)
{
    return 0;
}

bool IsTransactionState(void)
{
    return false;
}

HeapTuple SearchSysCache1(int cacheId, Datum key1)
{
    return nullptr;
}

char* OidOutputFunctionCall(Oid functionId, Datum val)
{
    return nullptr;
}

double get_local_rows(double global_rows, double multiple, bool replicate, unsigned int num_data_nodes)
{
    return 0;
}

AclResult pg_namespace_aclcheck(Oid nsp_oid, Oid roleid, AclMode mode, bool check_nodegroup)
{
    return ACLCHECK_OK;
}

ResourceOwner ResourceOwnerCreate(ResourceOwner parent, const char* name, MemoryContext memCxt)
{
    return nullptr;
}

char* get_cfgname(Oid cfgid)
{
    return nullptr;
}

Oid getUserId(void)
{
    return 0;
}

void GetSynonymAndSchemaName(Oid synOid, char **synName_p, char **synSchema_p)
{
    return;
}

Oid get_func_namespace(Oid funcid)
{
    return 0;
}

unsigned int ng_get_dest_num_data_nodes(Plan* plan)
{
    return 0;
}

void get_oper_name_namespace_oprs(Oid operid, char** oprname, char** nspname, Oid* oprleft, Oid* oprright)
{
    return;
}

unsigned int ng_get_dest_num_data_nodes(RelOptInfo* rel)
{
    return 0;
}

bool type_is_set(Oid typid)
{
    return false;
}

char* get_rel_name(Oid id){
    return nullptr;
}

char* get_typename(Oid id){
    return nullptr;
}

char* get_typenamespace(Oid id){
    return nullptr;
}

char* get_func_name(Oid id){
    return nullptr;
}

char* get_cfgnamespace(Oid id){
    return nullptr;
}

char* get_namespace_name(Oid id){
    return nullptr;
}

char* get_collation_name(Oid id){
    return nullptr;
}

Oid get_rel_namespace(Oid relid)
{
    return 0;
}

Oid FindDefaultConversion(Oid connamespace, int32 for_encoding, int32 to_encoding)
{
    return 0;
}

bool TimestampDifferenceExceeds(TimestampTz start_time,
									   TimestampTz stop_time,
									   int msec)
{
    return false;
}

TimestampTz GetCurrentTimestamp(void)
{
    return 0;
}

bool EnableGlobalSysCache()
{
    return false;
}

void getTypeOutputInfo(Oid type, Oid* typOutput, bool* typIsVarlena)
{
    return;
}

long pg_lrand48(unsigned short rand48_seed[3])
{
    return 0;
}

ThreadId gs_thread_self(void)
{
    return 0;
}

int ExecGetPlanNodeid(void)
{
    return 0;
}

int pthread_rwlock_unlock (pthread_rwlock_t *__rwlock)
{
    return 0;
}

int pthread_rwlock_init (pthread_rwlock_t *__restrict __rwlock,
				const pthread_rwlockattr_t *__restrict
				__attr)
{
    return 0;
}

int pthread_rwlock_rdlock (pthread_rwlock_t *__rwlock)
{
    return 0;
}

int pthread_rwlock_tryrdlock (pthread_rwlock_t *__rwlock)
{
    return 0;
}


int pthread_rwlock_wrlock (pthread_rwlock_t *__rwlock)
{
    return 0;
}

size_t mmap_threshold = (size_t)0xffffffff;
ThreadPoolControler* g_threadPoolControler = NULL;
CommSocketOption g_default_invalid_sock_opt = {
    .valid = false
};
CommEpollOption g_default_invalid_epoll_opt = {
    .valid = false
};
CommPollOption g_default_invalid_poll_opt = {
    .valid = false
};
int SysCacheSize = 0;
bool isSingleMode = false;
const TableAmRoutine* TableAmUstore = NULL;
const TableAmRoutine* TableAmHeap = NULL;
const char *optLocation = "location";
/* Globally available receiver for DestNone */
DestReceiver* None_Receiver = NULL;

Node* strip_implicit_coercions(Node* node)
{
    return nullptr;
}

void checkArrayTypeInsert(ParseState* pstate, Expr* expr)
{
    return;
}

const char *get_typename_by_id(Oid typeOid)
{
    if (typeOid == BYTEAWITHOUTORDERWITHEQUALCOLOID) {
        return "byteawithoutorderwithequal";
    } else if (typeOid == BYTEAWITHOUTORDERCOLOID) {
        return "byteawithoutorder";
    }
    return NULL;
}

HeapTuple SearchSysCacheCopy(int cacheId, Datum key1, Datum key2, Datum key3, Datum key4, int level)
{
    return nullptr;
}

TupleDesc lookup_rowtype_tupdesc_copy(Oid type_id, int32 typmod)
{
    return nullptr;
}

Datum OidInputFunctionCall(Oid functionId, char* str, Oid typioparam, int32 typmod, bool can_ignore)
{
    return 0;
}

Var* get_var_from_node(Node* node, bool (*func)(Oid))
{
    return nullptr;
}

int locate_agg_of_level(Node* node, int levelsup)
{
    return 0;
}

int locate_var_of_level(Node* node, int levelsup)
{
    return 0;
}

void TupleDescInitEntry(TupleDesc desc, AttrNumber attributeNumber, const char *attributeName, Oid oidtypeid,
                        int32 typmod, int attdim)
{
    return;
}

char* pg_findformat(const char* key, const char* source)
{
    return nullptr;
}

AclResult pg_nodegroup_aclcheck(Oid group_oid, Oid roleid, AclMode mode)
{
    ACLCHECK_OK;
}

void ProcessUtility(processutility_context* processutility_cxt,
    DestReceiver* dest,
#ifdef PGXC
    bool sent_to_remote,
#endif /* PGXC */
    char* completion_tag,
    ProcessUtilityContext context,
    bool isCTAS)
{
    return;
}

void check_matview_op_supported(CreateTableAsStmt *ctas)
{
    return;
}

void InsertErrorMessage(const char* message, int yyloc, bool isQueryString, int lines)
{
    return;
}

int GetProcedureLineNumberInPackage(const char* procedureStr, int loc)
{
    return 0;
}

const int GetCustomParserId()
{
    return 0;
}

OgRecordAutoController::OgRecordAutoController(TimeInfoType time_info_type)
{
    return;
}

int CompileWhich()
{
    return PLPGSQL_COMPILE_NULL;
}

OgRecordAutoController::~OgRecordAutoController()
{
    return;
}

int GetLineNumber(const char* procedureStr, int loc)
{
    return 0;
}

void InitSpiPrinttupDR(DestReceiver* dr)
{
    return;
}

int64 og_get_time_unique_id()
{
    return 0;
}

bool enable_idle_in_transaction_session_sig_alarm(int delayms)
{
    return false;
}

bool enable_session_sig_alarm(int delayms)
{
    return false;
}

bool IsTransactionOrTransactionBlock(void)
{
    return false;
}

bool IsAbortedTransactionBlockState(void)
{
    return false;
}

char get_rel_relkind(Oid relid)
{
    return 0;
}

void StatementRetryController::Reset(void)
{
    return;
}

Oid get_func_oid(const char* funcname, Oid funcnamespace, Expr* expr)
{
    return 0;
}

void BitvecInit(void)
{
    return;
}

char* getPartitionName(Oid partid, bool missing_ok)
{
    return nullptr;
}

Oid GetUserId(void)
{
    return 0;
}

int PthreadMutexUnlock(ResourceOwner owner, pthread_mutex_t* mutex, bool trace)
{
    return 0;
}

void pq_endcopyout(bool errorAbort)
{
    return;
}

bool func_oid_check_pass(Oid oid)
{
    return true;
}

char* trim(char* src)
{
    char* s = 0;
    char* e = 0;
    char* c = 0;

    for (c = src; (c != NULL) && *c; ++c) {
        if (isspace(*c)) {
            if (e == NULL) {
                e = c;
            }
        } else {
            if (s == NULL) {
                s = c;
            }
            e = 0;
        }
    }
    if (s == NULL) {
        s = src;
    }
    if (e != NULL) {
        *e = 0;
    }

    return s;
}

MemoryContext RackMemoryAllocator::AllocSetContextCreate(MemoryContext parent, const char* name, Size minContextSize,
    Size initBlockSize, Size maxBlockSize, Size maxSize, bool isShared, bool isSession)
{
    return nullptr;
}

pg_crc32c pg_comp_crc32c_sb8(pg_crc32c crc, const void* data, size_t len)
{
    return 0;
}

int main()
{

}
