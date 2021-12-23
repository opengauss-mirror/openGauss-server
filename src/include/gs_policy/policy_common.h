/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * policy_common.h
 *
 * IDENTIFICATION
 *    src/include/gs_policy/policy_common.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef _GS_POLICY_COMMON_H
#define _GS_POLICY_COMMON_H
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"

#include "postgres.h"
#include "gs_set.h"
#include "gs_map.h"
#include "gs_vector.h"
#include "pgaudit.h"

struct GsPolicyFQDN {
    GsPolicyFQDN():m_value_schema(0), m_value_object(0), is_function(false){}
    Oid m_value_schema; /* schema */
    Oid m_value_object; /* table */
    gs_stl::gs_string m_value_object_attrib; /* column */
    bool is_function;
};

struct GsPolicyLabel {
    bool operator == (const GsPolicyLabel &arg) const;
    bool operator < (const GsPolicyLabel& arg) const;
    int operator - (const GsPolicyLabel& arg) const;
    long long   m_id;
    gs_stl::gs_string m_name;
    gs_stl::gs_string m_type;
    gs_stl::gs_string m_data_value;
    GsPolicyFQDN m_data_value_fqdn;
    gs_stl::gs_string m_data_type;
};

struct GsPolicyLabelKey {
    gs_stl::gs_string m_name;
    gs_stl::gs_string m_type;
};

typedef gs_stl::gs_set<gs_stl::gs_string> policy_labelname_set;
typedef gs_stl::gs_set<Oid> policy_oid_set;

/* set of existing labels keyed by data type and data value */
typedef gs_stl::gs_set<GsPolicyLabel> policy_labels_set;
/* map of existing labels - key: label name, value: set of labels keyed by resource type & name. */
typedef gs_stl::gs_map<gs_stl::gs_string, policy_labels_set> policy_labels_map;
/* set of tables fqdn contained in label */
typedef gs_stl::gs_set<gs_stl::gs_string> policy_label_tables_set;

extern void create_policy_label(CreatePolicyLabelStmt *stmt);
extern void load_existing_labels(Relation rel, policy_labels_map *existing_labels);
extern void alter_policy_label(AlterPolicyLabelStmt *stmt);
extern void drop_policy_label(DropPolicyLabelStmt *stmt);
extern bool VerifyLabelHasColumn(RangeVar *rel, GsPolicyFQDN *fqdn,
    const gs_stl::gs_set<gs_stl::gs_string> *labels);

typedef bool(*ValidateBehaviourPtr)(const char *func_name, const char* func_parameters,
                                    bool* invalid_params, bool is_audit);
extern PGDLLIMPORT ValidateBehaviourPtr validate_masking_behaviour_hook;

typedef bool (*LoadLabelsPtr)(bool);
extern PGDLLIMPORT LoadLabelsPtr load_labels_hook;

typedef void (*RelationBuildPtr)(Relation);
extern PGDLLIMPORT RelationBuildPtr RelationBuildRlsPolicies_hook;

typedef void(*GetRlsPoliciesPtr)(const Query *query, const RangeTblEntry *rte,
                                 const Relation relation, List *&rlsQuals, List *&withCheckOptions,
                                 int rtIndex, bool &hasRowSecurity, bool &hasSubLink);
extern PGDLLIMPORT GetRlsPoliciesPtr get_rls_policies_hook;

typedef void (*ReseThreadValuesPtr)();
extern THR_LOCAL PGDLLIMPORT ReseThreadValuesPtr reset_policy_thr_hook;

typedef bool (*LoadPoliciesPtr)(bool);
extern PGDLLIMPORT LoadPoliciesPtr load_rls_policies_hook;
extern PGDLLIMPORT LoadPoliciesPtr load_audit_policies_hook;
extern PGDLLIMPORT LoadPoliciesPtr load_masking_policies_hook;
extern PGDLLIMPORT LoadPoliciesPtr load_security_policies_hook;

typedef void (*LightUnifiedAuditExecutorPtr)(const Query *query);
extern THR_LOCAL PGDLLIMPORT LightUnifiedAuditExecutorPtr light_unified_audit_executor_hook;

/* hooks for sqlbypass */
typedef void (*OpFusionUnifiedAuditExecutorPtr)(const PlannedStmt *plannedStmt);
extern PGDLLIMPORT OpFusionUnifiedAuditExecutorPtr opfusion_unified_audit_executor_hook;
typedef void (*OpFusionUnifiedAuditFlushLogsPtr)(AuditResult audit_result);
extern PGDLLIMPORT OpFusionUnifiedAuditFlushLogsPtr opfusion_unified_audit_flush_logs_hook;

typedef bool (*LoadPolicyAccessPtr)(bool);
extern PGDLLIMPORT LoadPolicyAccessPtr load_policy_access_hook;
extern PGDLLIMPORT LoadPolicyAccessPtr load_masking_policy_actions_hook;
extern PGDLLIMPORT LoadPolicyAccessPtr load_security_access_hook;

typedef bool (*LoadPolicyPrivilegesPtr)(bool);
extern PGDLLIMPORT LoadPolicyPrivilegesPtr load_policy_privileges_hook;
extern PGDLLIMPORT LoadPolicyPrivilegesPtr load_security_privileges_hook;

typedef bool (*LoadPolicyFilterPtr)(bool);
extern PGDLLIMPORT LoadPolicyFilterPtr load_policy_filter_hook;
extern PGDLLIMPORT LoadPolicyFilterPtr load_masking_policy_filter_hook;
extern PGDLLIMPORT LoadPolicyFilterPtr load_security_filter_hook;

typedef bool (*IsMaskingHasObjPtr)(bool, const gs_stl::gs_string);
extern PGDLLIMPORT IsMaskingHasObjPtr isMaskingHasObj_hook;

typedef bool (*CheckPolicyPrivilegesForLabelPtr)(const policy_labels_map*);
extern PGDLLIMPORT CheckPolicyPrivilegesForLabelPtr check_audit_policy_privileges_for_label_hook;

typedef bool (*CheckPolicyAccessForLabelPtr)(const policy_labels_map*);
extern PGDLLIMPORT CheckPolicyAccessForLabelPtr check_audit_policy_access_for_label_hook;

typedef bool (*CheckPolicyActionsForLabelPtr)(const policy_labels_map*);
extern PGDLLIMPORT CheckPolicyActionsForLabelPtr check_masking_policy_actions_for_label_hook;
extern PGDLLIMPORT CheckPolicyActionsForLabelPtr check_rls_policy_actions_for_label_hook;

typedef void (*LoginUserPtr)(const char* /* db name */, const char* /* user */, bool /* sucess */, bool /* login */);
extern THR_LOCAL PGDLLIMPORT LoginUserPtr user_login_hook;

typedef bool (*VerifyCopyCommandIsReparsed)(List*, const char*, gs_stl::gs_string&);
extern THR_LOCAL PGDLLIMPORT VerifyCopyCommandIsReparsed copy_need_to_be_reparse;

typedef bool (*IsRecompilePrepareStmtPtr)(const char *stmt_name);
extern PGDLLIMPORT IsRecompilePrepareStmtPtr recompile_prepare_stmt_hook;

typedef void (*IsInsidePrepareStmtPtr)(const char *stmt_name);
extern PGDLLIMPORT IsInsidePrepareStmtPtr prepare_stmt_state_hook;

typedef bool (*IsLabelExistPtr)(const char *label_name);
extern PGDLLIMPORT IsLabelExistPtr verify_label_hook;

typedef void (*InSideView)(bool);
extern PGDLLIMPORT InSideView query_from_view_hook;

typedef void (*GsSaveManagementEvent)(const char *message);
extern PGDLLIMPORT GsSaveManagementEvent gs_save_mng_event_hook;

typedef void (*GsSendManagementEvent)(AuditResult result_type);
extern PGDLLIMPORT GsSendManagementEvent gs_send_mng_event_hook;

typedef bool (*VerifyLabelsByPolicy)(const char* filter_tree, const policy_labelname_set *policy_labels,
                                     Oid policyOid, gs_stl::gs_string *polname);
extern PGDLLIMPORT VerifyLabelsByPolicy gs_verify_labels_by_policy_hook;

bool get_files_list(const char  *dir,std::vector<std::string>& files,const char *ext, int queue_size);
bool is_policy_enabled();
bool is_security_policy_relation(Oid table_oid);

#endif /* _GS_POLICY_COMMON_H */
